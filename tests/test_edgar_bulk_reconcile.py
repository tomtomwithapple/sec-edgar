"""Job `edgar_bulk`: the weekly bulk reconcile and prune (plan: How › SEC EDGAR › Bulk; C17; S27).

Sunday 03:00 ET (calendar `any`) the job re-downloads both bulk ZIPs through the free-disk guard,
re-parses only members whose CRC changed, then prunes the bulk blobs to the newest 2 per dataset.
The ZIPs are the fixture ZIPs of `test_edgar_bulk` (real member JSON); a "changed" member is
Apple's real companyfacts document with one fact's `frame` moved, as SEC does. No request leaves
the process.
"""

import io
import json
import zipfile
from dataclasses import replace
from datetime import UTC, date, datetime

import test_edgar_bulk as fixtures

from usfin import calendars, jobs, scheduler
from usfin.sources.edgar import bulk, companyfacts

# the bulk tests' fixtures and helpers
settings, cli_env, disk, sec = fixtures.settings, fixtures.cli_env, fixtures.disk, fixtures.sec
GIB, _query, _company, FIXTURES = fixtures.GIB, fixtures._query, fixtures._company, fixtures.FIXTURES
APPLE_FACTS = "CIK0000320193.json"


def _run(settings, when=datetime(2026, 10, 11, 7, 0, tzinfo=UTC)):
    reg = jobs.Registry()
    reg.register(bulk.JOB)
    return jobs.run_job("edgar_bulk", settings=settings, registry=reg, clock=lambda: when)


def _companyfacts_zip(frame: str | None = None) -> bytes:
    """companyfacts.zip with JPMorgan verbatim and Apple's first revenue fact's frame set."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for p in sorted((FIXTURES / "companyfacts").glob("*.json")):
            body = p.read_bytes()
            if p.name == APPLE_FACTS and frame is not None:
                doc = json.loads(body)
                doc["facts"]["us-gaap"]["SalesRevenueNet"]["units"]["USD"][0]["frame"] = frame
                body = json.dumps(doc).encode()
            zf.writestr(p.name, body)
    return buf.getvalue()


def test_the_job_downloads_both_zips_and_parses_every_member_the_first_time(
    settings, sec, disk
):
    _company(settings, 320193, "AAPL")
    result = _run(settings)
    assert result.status == "ok", result.error
    assert sec.requests == [
        "HEAD companyfacts.zip", "GET companyfacts.zip", "HEAD submissions.zip",
        "GET submissions.zip",
    ]  # fmt: skip
    assert result.rows_upserted == 13 + 11  # facts (Apple 9, JPMorgan 4) + Apple's filings
    assert _query(
        settings, "SELECT dataset, count(*) FROM edgar_bulk_member GROUP BY 1 ORDER BY 1"
    ) == [("bulk_companyfacts", 2), ("bulk_submissions", 2)]
    # the watermark per dataset is the New York date of the reconciled ZIP
    assert _query(settings, "SELECT key, value FROM watermark ORDER BY key") == [
        ("companyfacts", "2026-10-11"),
        ("submissions", "2026-10-11"),
    ]


def test_a_second_zip_with_one_changed_member_reparses_only_that_member(
    settings, sec, disk, monkeypatch
):
    _company(settings, 320193, "AAPL")
    assert _run(settings).status == "ok"
    members = "SELECT dataset || ':' || name, raw_id FROM edgar_bulk_member"
    first = dict(_query(settings, members))
    parsed: list[str] = []
    store = companyfacts.store_document

    def spy(con, doc, *, name, raw_id=None):
        parsed.append(name)
        return store(con, doc, name=name, raw_id=raw_id)

    monkeypatch.setattr(companyfacts, "store_document", spy)
    sec.bodies["companyfacts.zip"] = _companyfacts_zip(frame="CY2009")
    result = _run(settings, when=datetime(2026, 10, 18, 7, 0, tzinfo=UTC))
    assert result.status == "ok", result.error
    assert parsed == [APPLE_FACTS]  # JPMorgan's CRC did not change; submissions.zip did not
    assert result.rows_upserted == 1  # the moved frame
    now = dict(_query(settings, members))
    changed = {n for n in now if now[n] != first[n]}
    assert changed == {f"bulk_companyfacts:{APPLE_FACTS}"}
    assert _query(settings, "SELECT count(*) FROM xbrl_fact") == [(13,)]


def test_prune_keeps_the_newest_two_zips_per_dataset_after_the_job(settings, sec, disk):
    for week, frame in enumerate([None, "CY2009", "CY2010"]):
        sec.bodies["companyfacts.zip"] = _companyfacts_zip(frame)
        result = _run(settings, when=datetime(2026, 10, 11 + 7 * week, 7, 0, tzinfo=UTC))
        assert result.status == "ok", result.error
    rows = _query(
        settings,
        "SELECT dataset, blob_path, pruned_at IS NOT NULL FROM raw_response"
        " WHERE dataset LIKE 'bulk_%' ORDER BY dataset, id",
    )
    facts = [r for r in rows if r[0] == "bulk_companyfacts"]
    assert [r[2] for r in facts] == [True, False, False]  # the first week's ZIP pruned
    assert not (settings.raw_dir / facts[0][1]).exists()
    assert all((settings.raw_dir / r[1]).exists() for r in facts[1:])
    # submissions.zip never changed: three fetches of one blob count as one, nothing pruned
    subs = [r for r in rows if r[0] == "bulk_submissions"]
    assert len(subs) == 3 and len({r[1] for r in subs}) == 1 and not any(r[2] for r in subs)


def test_low_free_disk_skips_the_run_raises_one_disk_low_alert_and_downloads_nothing(
    settings, sec, disk
):
    disk.free = 10 * GIB  # the floor itself: no room for any ZIP
    result = _run(settings)
    assert result.status == "skipped"
    assert "disk low" in result.error and "floor 10.0 GiB" in result.error
    assert sec.requests == ["HEAD companyfacts.zip"]  # no GET, and nothing after the skip
    assert _query(settings, "SELECT status FROM job_run WHERE job = 'edgar_bulk'") == [
        ("skipped",)
    ]
    again = _run(settings, when=datetime(2026, 10, 11, 8, 0, tzinfo=UTC))
    assert again.status == "skipped"
    alerts = _query(settings, "SELECT job, rule, subject, raised_on FROM alert")
    assert alerts == [("edgar_bulk", "disk_low", "bulk_companyfacts", date(2026, 10, 11))]
    (detail,) = _query(settings, "SELECT detail FROM alert")[0]
    assert json.loads(detail)["floor"] == 10 * GIB
    assert _query(settings, "SELECT count(*) FROM raw_response WHERE dataset LIKE 'bulk_%'") == [
        (0,)
    ]
    assert _query(settings, "SELECT count(*) FROM watermark") == [(0,)]


def test_the_second_download_is_guarded_too(settings, sec, disk):
    # room for companyfacts.zip but, once it is stored, not for submissions.zip as well
    cf, subs = len(sec.bodies["companyfacts.zip"]), len(sec.bodies["submissions.zip"])
    # (raw, db) for the HEAD guard and again for the GET's Content-Length (S35), per ZIP
    disk.script = [10 * GIB + cf] * 4 + [10 * GIB + subs - 1] * 2
    result = _run(settings)
    assert result.status == "skipped"
    assert sec.requests == ["HEAD companyfacts.zip", "GET companyfacts.zip", "HEAD submissions.zip"]
    assert _query(settings, "SELECT subject FROM alert WHERE rule = 'disk_low'") == [
        ("bulk_submissions",)
    ]
    # the companyfacts members parsed before the skip stay committed
    assert _query(settings, "SELECT count(*) FROM xbrl_fact") == [(13,)]


def test_bulk_limit_caps_the_members_parsed_per_dataset(settings, sec, disk):
    _company(settings, 320193, "AAPL")
    result = _run(replace(settings, bulk_limit=1))
    assert result.status == "ok", result.error
    assert _query(
        settings, "SELECT dataset, count(*) FROM edgar_bulk_member GROUP BY 1 ORDER BY 1"
    ) == [("bulk_companyfacts", 1), ("bulk_submissions", 1)]


def test_bulk_limit_reads_usfin_bulk_limit(tmp_path, monkeypatch):
    from usfin.settings import load_settings

    monkeypatch.delenv("USFIN_BULK_LIMIT", raising=False)
    assert load_settings(env_file=tmp_path / "none").bulk_limit is None
    monkeypatch.setenv("USFIN_BULK_LIMIT", "200")
    assert load_settings(env_file=tmp_path / "none").bulk_limit == 200


def test_a_malformed_member_fails_its_dataset_and_the_rest_commit(settings, sec, disk):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        apple = FIXTURES / "companyfacts" / APPLE_FACTS
        zf.writestr(apple.name, apple.read_bytes())
        zf.writestr("CIK0000019617.json", b"{not json")
    sec.bodies["companyfacts.zip"] = buf.getvalue()
    result = _run(settings)
    assert result.status == "failed"
    assert "CIK0000019617.json" in result.key_errors["companyfacts"]
    assert "submissions" not in result.key_errors and "prune" not in result.key_errors
    assert _query(settings, "SELECT DISTINCT cik FROM xbrl_fact") == [(320193,)]


def test_edgar_bulk_is_scheduled_sunday_0300_any_day():
    entry = scheduler.load_schedule(fixtures.REPO_CONFIG)["edgar_bulk"]
    assert entry == scheduler.Entry("edgar_bulk", "0 3 * * sun", "any")
    cals = calendars.load_calendars(fixtures.REPO_CONFIG)
    after = datetime(2026, 10, 8, 12, 0, tzinfo=UTC)  # a Thursday
    fires = scheduler.next_fires(entry, cals, after, n=2)
    assert [f.strftime("%a %Y-%m-%d %H:%M") for f in fires] == [
        "Sun 2026-10-11 03:00",
        "Sun 2026-10-18 03:00",
    ]


def test_edgar_bulk_is_registered():
    assert "edgar_bulk" in jobs.load_registry().names()


def test_a_refused_head_sizes_by_a_ranged_get_and_the_job_runs_ok(settings, sec, disk):
    _company(settings, 320193, "AAPL")
    sec.head_status = 403  # as SEC answered a HEAD on Archives in S36
    result = _run(settings)
    assert result.status == "ok", result.error
    assert sec.requests == [
        "HEAD companyfacts.zip", "GET companyfacts.zip bytes=0-0", "GET companyfacts.zip",
        "HEAD submissions.zip", "GET submissions.zip bytes=0-0", "GET submissions.zip",
    ]  # fmt: skip
    assert not fixtures.http.SEC_BLOCK.blocked()


def test_a_streamed_zip_larger_than_announced_by_head_is_guarded_before_writing(
    settings, sec, disk
):
    size = len(sec.bodies["companyfacts.zip"])
    disk.free = 10 * GIB + size + GIB // 2
    sec.get_length = size + GIB
    result = _run(settings)
    assert result.status == "skipped"
    assert "disk low" in result.error
    assert sec.requests == ["HEAD companyfacts.zip", "GET companyfacts.zip"]
    assert _query(settings, "SELECT rule, subject FROM alert") == [
        ("disk_low", "bulk_companyfacts")
    ]
    assert _query(settings, "SELECT count(*) FROM raw_response WHERE dataset LIKE 'bulk_%'") == [
        (0,)
    ]
