"""EDGAR bulk backfill (`usfin edgar backfill`, module `usfin.sources.edgar.bulk`; C1, C17).

The fixture ZIPs are built in the test from the real member JSON recorded for S20/S21, byte for
byte, under their real member names: `tests/fixtures/edgar/submissions/` (Apple's
`CIK0000320193.json` and its page `CIK0000320193-submissions-001.json`, as submissions.zip names
them) and `tests/fixtures/edgar/companyfacts/` (Apple and JPMorgan `CIK##########.json`, as
companyfacts.zip names them). Nothing else is invented. No request leaves the process.
"""

import io
import shutil
import zipfile
from collections import namedtuple
from dataclasses import replace
from pathlib import Path

import httpx
import pytest
import respx
from typer.testing import CliRunner

from usfin import http, ratelimit, raw, store
from usfin.cli import app
from usfin.settings import load_settings
from usfin.sources.edgar import bulk

FIXTURES = Path(__file__).parent / "fixtures" / "edgar"
REPO_CONFIG = Path(__file__).resolve().parents[1] / "config"
UA = "usfin tests ops@example.test"
GIB = 2**30
Usage = namedtuple("Usage", "total used free")


def zip_of(folder: str, names: list[str] | None = None) -> bytes:
    buf = io.BytesIO()
    paths = sorted((FIXTURES / folder).glob("*.json"))
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for p in paths:
            if names is None or p.name in names:
                zf.writestr(p.name, p.read_bytes())
    return buf.getvalue()


class Disk:
    """`shutil.disk_usage` stand-in: `free` bytes, or the next of `script` per call."""

    def __init__(self, free: float):
        self.free = free
        self.script: list[float] = []
        self.paths: list[str] = []

    def __call__(self, path) -> Usage:
        self.paths.append(str(path))
        free = self.script.pop(0) if self.script else self.free
        return Usage(228 * GIB, 228 * GIB - int(free), int(free))


@pytest.fixture
def disk(monkeypatch):
    d = Disk(18 * GIB)
    monkeypatch.setattr(bulk, "disk_usage", d)
    return d


@pytest.fixture
def settings(tmp_path, monkeypatch):
    for key in ("USFIN_DB", "USFIN_RAW", "USFIN_CONFIG", "USFIN_LOCK_WAIT", "USFIN_MIN_FREE_GB"):
        monkeypatch.delenv(key, raising=False)
    config = tmp_path / "config"
    shutil.copytree(REPO_CONFIG, config)
    t = [0.0]
    monkeypatch.setattr(
        ratelimit,
        "LIMITERS",
        ratelimit.Limiters(clock=lambda: t[0], sleep=lambda s: t.__setitem__(0, t[0] + s)),
    )
    monkeypatch.setattr(http, "BACKOFF_BASE", 0.0)
    monkeypatch.setattr(http, "SEC_BLOCK", http.SecBlock())
    monkeypatch.setattr(raw, "LARGE_BODY_BYTES", 1000)  # the fixture ZIPs count as bulk bodies
    return replace(
        load_settings(env_file=tmp_path / "no.env"),
        db_path=tmp_path / "usfin.duckdb",
        raw_dir=tmp_path / "raw",
        config_dir=config,
        lock_wait=0,
        sec_user_agent=UA,
    )


@pytest.fixture
def cli_env(settings, monkeypatch):
    monkeypatch.setenv("USFIN_DB", str(settings.db_path))
    monkeypatch.setenv("USFIN_RAW", str(settings.raw_dir))
    monkeypatch.setenv("USFIN_CONFIG", str(settings.config_dir))
    monkeypatch.setenv("USFIN_LOCK_WAIT", "0")
    monkeypatch.setenv("SEC_USER_AGENT", UA)


class Sec:
    """Serves both bulk ZIPs (HEAD and GET); records every request as 'METHOD name', a ranged
    GET as 'GET name bytes=0-0'. As live on 2026-10-08 (S35): HEAD 200 with Content-Length, a
    `Range: bytes=0-0` GET 206 with `Content-Range: bytes 0-0/<size>`. `head_status` (Archives
    HEAD answered 403 to S36), `ranges` (False: the Range header is ignored, 200 full body),
    `range_status` and `get_length` (a GET announcing another Content-Length) vary that."""

    def __init__(self):
        self.bodies = {
            "submissions.zip": zip_of("submissions"),
            "companyfacts.zip": zip_of("companyfacts"),
        }
        self.content_length = True
        self.head_status = 200
        self.ranges = True
        self.range_status = 206
        self.get_length: int | None = None
        self.requests: list[str] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        name = request.url.path.rsplit("/", 1)[-1]
        rng = request.headers.get("Range")
        self.requests.append(f"{request.method} {name}" + (f" {rng}" if rng else ""))
        body = self.bodies[name]
        headers = {"Content-Type": "application/zip"}
        if request.method == "HEAD":
            if self.head_status != 200:
                return httpx.Response(self.head_status, headers={"Content-Type": "text/html"})
            if self.content_length:
                headers["Content-Length"] = str(len(body))
            return httpx.Response(200, headers=headers)
        if rng and self.ranges:
            if self.range_status != 206:
                return httpx.Response(self.range_status, headers={"Content-Type": "text/html"})
            headers["Content-Range"] = f"bytes 0-0/{len(body)}"
            return httpx.Response(206, headers=headers, content=body[:1])
        if self.get_length is not None:
            headers["Content-Length"] = str(self.get_length)
        return httpx.Response(200, headers=headers, content=body)


@pytest.fixture
def sec(settings):
    s = Sec()
    with respx.mock(assert_all_called=False) as router:
        router.route(url__regex=r"^https://www\.sec\.gov/Archives/edgar/daily-index/").mock(
            side_effect=s
        )
        yield s


def _query(settings, sql, params=()):
    with store.session(settings.db_path, wait=0) as con:
        return con.execute(sql, list(params)).fetchall()


def _backfill(*args):
    return CliRunner().invoke(app, ["edgar", "backfill", *args])


def test_urls_are_the_planned_bulk_zips():
    assert bulk.DATASETS["submissions"].url == (
        "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip"
    )
    assert bulk.DATASETS["companyfacts"].url == (
        "https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip"
    )
    assert bulk.DATASETS["submissions"].raw_dataset == "bulk_submissions"
    assert bulk.DATASETS["companyfacts"].raw_dataset == "bulk_companyfacts"
    assert set(raw.BULK_DATASETS) == {"bulk_submissions", "bulk_companyfacts"}


def test_companyfacts_zip_streams_to_raw_as_zip_and_members_parse_in_place(
    settings, sec, disk, cli_env, monkeypatch
):
    bodies = []
    save = raw.save

    def spy(con, **kw):
        bodies.append(type(kw["body"]))
        return save(con, **kw)

    monkeypatch.setattr(raw, "save", spy)
    out = _backfill("companyfacts")
    assert out.exit_code == 0, out.output
    assert sec.requests == ["HEAD companyfacts.zip", "GET companyfacts.zip"]
    # the ZIP body reached the raw store as a chunk stream, never as one bytes object
    assert bodies[-1] is not bytes
    (rid, path, size), = _query(
        settings,
        "SELECT id, blob_path, bytes FROM raw_response WHERE dataset = 'bulk_companyfacts'",
    )
    assert path.endswith(".zip") and size == len(sec.bodies["companyfacts.zip"])
    # the same per-CIK parser as job edgar_companyfacts: Apple 9 + JPMorgan 4 us-gaap/dei facts
    assert _query(settings, "SELECT cik, count(*) FROM xbrl_fact GROUP BY cik ORDER BY cik") == [
        (19617, 4),
        (320193, 9),
    ]
    zf = zipfile.ZipFile(io.BytesIO(sec.bodies["companyfacts.zip"]))
    assert _query(
        settings, "SELECT dataset, name, crc, raw_id FROM edgar_bulk_member ORDER BY name"
    ) == [("bulk_companyfacts", i.filename, i.CRC, rid) for i in zf.infolist()]
    assert "members 2, parsed 2, rows 13" in out.output
    # nothing was extracted next to the blob
    blobs = [p.name for p in settings.raw_dir.rglob("*") if p.is_file()]
    assert not [n for n in blobs if n.endswith(".json")] and len(blobs) == 2  # HEAD .gz, ZIP


def _company(settings, cik, ticker):
    with store.session(settings.db_path, wait=0) as con:
        con.execute(
            "INSERT INTO edgar_company VALUES (?, ?, 'x', ?, true, now()::TIMESTAMP)",
            [cik, ticker, ticker],
        )


def test_submissions_members_use_the_per_cik_parser_main_document_before_its_page(
    settings, sec, disk, cli_env
):
    _company(settings, 320193, "AAPL")
    out = _backfill("submissions")
    assert out.exit_code == 0, out.output
    # 7 recent filings + 4 on the page, every one with the company as party
    assert _query(settings, "SELECT count(*), count(DISTINCT cik) FROM edgar_filing") == [(11, 1)]
    assert _query(settings, "SELECT DISTINCT source FROM edgar_filing") == [("submissions",)]
    # the page member is linked to its listing, so the per-CIK job will not fetch it again
    (rid,) = _query(settings, "SELECT id FROM raw_response WHERE dataset = 'bulk_submissions'")[0]
    assert _query(settings, "SELECT name, raw_id FROM edgar_submissions_page") == [
        ("CIK0000320193-submissions-001.json", rid)
    ]


def test_limit_parses_that_many_members_and_crcs_make_the_next_run_resume(
    settings, sec, disk, cli_env
):
    out = _backfill("companyfacts", "--limit", "1")
    assert out.exit_code == 0, out.output
    assert _query(settings, "SELECT count(*) FROM edgar_bulk_member") == [(1,)]
    out = _backfill("companyfacts", "--reuse")
    assert out.exit_code == 0, out.output
    assert "parsed 1," in out.output
    assert _query(settings, "SELECT count(*) FROM edgar_bulk_member") == [(2,)]
    out = _backfill("companyfacts", "--reuse")
    assert "parsed 0," in out.output
    # --reuse parses the stored ZIP: one download in all
    assert sec.requests.count("GET companyfacts.zip") == 1
    assert _query(settings, "SELECT count(*) FROM xbrl_fact") == [(13,)]


def test_cik_picks_only_that_companys_members(settings, sec, disk, cli_env):
    out = _backfill("companyfacts", "--cik", "19617")
    assert out.exit_code == 0, out.output
    assert _query(settings, "SELECT DISTINCT cik FROM xbrl_fact") == [(19617,)]
    assert _query(settings, "SELECT name FROM edgar_bulk_member") == [("CIK0000019617.json",)]
    # a --cik pick is not a sample of the whole ZIP: no estimate row
    assert _query(settings, "SELECT count(*) FROM edgar_bulk_estimate") == [(0,)]


def test_limit_spreads_the_sample_over_the_zip(settings):
    names = [f"CIK{c:010d}.json" for c in range(1, 11)]
    infos = [zipfile.ZipInfo(n) for n in names]
    for i in infos:
        i.CRC = 1
    picked = bulk.select_members(infos, done={}, limit=3, ciks=None)
    assert [i.filename for i in picked] == [names[0], names[3], names[6]]
    assert len(bulk.select_members(infos, done={}, limit=None, ciks=None)) == 10


def test_a_member_name_neither_parser_knows_is_skipped_and_counted(settings, sec, disk, cli_env):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        apple = FIXTURES / "companyfacts" / "CIK0000320193.json"
        zf.writestr("CIK0000320193.json", apple.read_bytes())
        zf.writestr("readme.txt", b"x")
    sec.bodies["companyfacts.zip"] = buf.getvalue()
    out = _backfill("companyfacts")
    assert out.exit_code == 0, out.output
    assert "members 2, parsed 1, rows 9, skipped 1, empty 0" in out.output


def test_an_empty_member_is_recorded_with_its_crc_and_no_rows(settings, sec, disk, cli_env):
    # companyfacts.zip of 2026-10-08 holds 62 members that are exactly `{}` (2 bytes, no cik,
    # no facts); `fixtures/edgar/bulk/CIK0000022370.json` is one of them, byte for byte
    empty = FIXTURES / "bulk" / "CIK0000022370.json"
    assert empty.read_bytes() == b"{}"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(empty.name, empty.read_bytes())
        apple = FIXTURES / "companyfacts" / "CIK0000320193.json"
        zf.writestr(apple.name, apple.read_bytes())
    sec.bodies["companyfacts.zip"] = buf.getvalue()
    out = _backfill("companyfacts")
    assert out.exit_code == 0, out.output
    assert "members 2, parsed 2, rows 9, skipped 0, empty 1, errors 0" in out.output
    assert _query(settings, "SELECT name FROM edgar_bulk_member ORDER BY name") == [
        ("CIK0000022370.json",),
        ("CIK0000320193.json",),
    ]


def test_submissions_load_only_companies_in_edgar_company_with_their_pages(
    settings, sec, disk, cli_env
):
    # Decisions after S26 (2): only CIKs in edgar_company (active or not), pages included
    _company(settings, 19617, "JPM")
    out = _backfill("submissions")
    assert out.exit_code == 0, out.output
    assert "parsed 0," in out.output and "not in edgar_company 2" in out.output
    assert _query(settings, "SELECT count(*) FROM edgar_filing") == [(0,)]
    assert _query(settings, "SELECT count(*) FROM edgar_bulk_member") == [(0,)]
    _company(settings, 320193, "AAPL")
    with store.session(settings.db_path, wait=0) as con:
        con.execute("UPDATE edgar_company SET active = false WHERE cik = 320193")
    out = _backfill("submissions", "--reuse")
    assert out.exit_code == 0, out.output
    assert "parsed 2," in out.output and "not in edgar_company 0" in out.output
    assert _query(settings, "SELECT name FROM edgar_bulk_member ORDER BY name") == [
        ("CIK0000320193-submissions-001.json",),
        ("CIK0000320193.json",),
    ]
    assert _query(settings, "SELECT count(*) FROM edgar_filing") == [(11,)]


def test_companyfacts_are_not_filtered_by_edgar_company(settings, sec, disk, cli_env):
    out = _backfill("companyfacts")
    assert out.exit_code == 0, out.output
    assert "parsed 2," in out.output and "not in edgar_company" not in out.output
