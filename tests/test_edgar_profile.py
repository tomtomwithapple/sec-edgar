"""EDGAR company profile (S45: table `edgar_company_profile`, view `edgar_company_sic`,
`usfin edgar profiles`).

Apple's real submissions main document and page (tests/fixtures/edgar/submissions/, fetched
2026-10-08; see test_edgar_submissions) carry `sic` "3571", `sicDescription` "Electronic
Computers", `ownerOrg` "06 Technology". Other documents here are that one edited. No request.
"""

import copy
import io
import json
import shutil
import zipfile
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest
from typer.testing import CliRunner

from usfin import raw, store
from usfin.cli import app
from usfin.settings import load_settings
from usfin.sources.edgar import profile, submissions

FIXTURES = Path(__file__).parent / "fixtures" / "edgar" / "submissions"
REPO_CONFIG = Path(__file__).resolve().parents[1] / "config"
APPLE = 320193
MAIN_NAME = "CIK0000320193.json"
PAGE_NAME = "CIK0000320193-submissions-001.json"
T1 = datetime(2026, 10, 8, 14, 0, tzinfo=UTC).replace(tzinfo=None)  # naive UTC, as stored
T2 = datetime(2026, 10, 9, 14, 0, tzinfo=UTC).replace(tzinfo=None)


def _doc(name: str = MAIN_NAME) -> dict:
    return json.loads((FIXTURES / name).read_text("utf-8"))


@pytest.fixture
def settings(tmp_path, monkeypatch):
    for key in ("USFIN_DB", "USFIN_RAW", "USFIN_CONFIG", "USFIN_LOCK_WAIT", "USFIN_DEMO"):
        monkeypatch.delenv(key, raising=False)
    config = tmp_path / "config"
    shutil.copytree(REPO_CONFIG, config)
    return replace(
        load_settings(env_file=tmp_path / "no.env"),
        db_path=tmp_path / "usfin.duckdb",
        raw_dir=tmp_path / "raw",
        config_dir=config,
        lock_wait=0,
    )


@pytest.fixture
def con(settings):
    with store.session(settings.db_path, wait=0) as c:
        yield c


def _profile(con, cik=APPLE):
    return con.execute(
        "SELECT name, sic, sic_description, owner_org, entity_type, category, fiscal_year_end,"
        " state_of_incorporation, exchanges, seen_at, raw_id FROM edgar_company_profile"
        " WHERE cik = ?",
        [cik],
    ).fetchone()


# --- parsing and storing ------------------------------------------------------------------------


def test_a_main_document_stores_the_company_profile(con):
    submissions.store_document(con, _doc(), name=MAIN_NAME, fetched_at=T1)
    assert _profile(con) == (
        "Apple Inc.", 3571, "Electronic Computers", "06 Technology", "operating",
        "Large accelerated filer", "0926", "CA", ["Nasdaq"], T1, None,
    )  # fmt: skip


def test_a_page_stores_no_profile(con):
    submissions.store_document(con, _doc(PAGE_NAME), name=PAGE_NAME, fetched_at=T1)
    assert _profile(con) is None


def test_a_blank_sic_is_null_and_a_malformed_one_is_rejected(con):
    doc = _doc() | {"sic": "", "sicDescription": ""}
    submissions.store_document(con, doc, name=MAIN_NAME, fetched_at=T1)
    assert _profile(con)[1:3] == (None, None)
    for bad in ("35A1", "12345", "-1"):
        with pytest.raises(profile.ProfileError):
            profile.row(APPLE, _doc() | {"sic": bad}, seen_at=T1, raw_id=None)


def test_a_newer_document_replaces_the_profile_and_an_older_one_does_not(con):
    submissions.store_document(con, _doc(), name=MAIN_NAME, fetched_at=T2)
    older = _doc() | {"sic": "3572", "sicDescription": "Computer Storage Devices"}
    submissions.store_document(con, older, name=MAIN_NAME, fetched_at=T1)
    assert _profile(con)[1] == 3571
    assert profile.store(con, APPLE, older, seen_at=T2) == 1  # the same time replaces
    assert _profile(con)[1] == 3572
    assert profile.store(con, APPLE, _doc(), seen_at=None) == 0  # an undated one never does
    assert _profile(con)[1] == 3572


def test_a_bulk_member_takes_its_zip_fetch_time(con, settings):
    zip_id = raw.save(
        con, source="edgar", dataset="bulk_submissions", url="https://x.test/submissions.zip",
        body=b"zip", http_status=200, fetched_at=T1, raw_dir=settings.raw_dir,
    )  # fmt: skip
    submissions.store_document(con, _doc(), name=MAIN_NAME, raw_id=zip_id)
    assert _profile(con)[-2:] == (T1, zip_id)


# --- view ---------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "sic, major, division",
    [
        (3571, 35, "D"), (100, 1, "A"), (1311, 13, "B"), (1531, 15, "C"), (4911, 49, "E"),
        (5045, 50, "F"), (5331, 53, "G"), (6022, 60, "H"), (6770, 67, "H"), (7370, 73, "I"),
        (9311, 93, "J"), (9999, 99, "K"), (8888, 88, None), (8880, 88, None), (9995, 99, None),
        (None, None, None),
    ],
)  # fmt: skip
def test_the_view_gives_the_sic_major_group_and_division(con, sic, major, division):
    con.execute("INSERT INTO edgar_company_profile (cik, sic) VALUES (1, ?)", [sic])
    got = con.execute(
        "SELECT sic_major, sic_division, sic_division_name FROM edgar_company_sic WHERE cik = 1"
    ).fetchone()
    assert got[:2] == (major, division)
    assert (got[2] is None) == (division is None)


# --- backfill -----------------------------------------------------------------------------------


def _zip(docs: dict[str, dict]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, doc in docs.items():
            zf.writestr(name, json.dumps(doc))
    return buf.getvalue()


def _seed(settings):
    """edgar_company: Apple and CIK 2; the bulk ZIP (T1) has Apple, its page, CIK 2 and CIK 3
    (not a company); a per-CIK response for Apple (T2) has a newer SIC."""
    other = copy.deepcopy(_doc()) | {"cik": "0000000002", "name": "Two", "sic": "6022"}
    third = copy.deepcopy(_doc()) | {"cik": "0000000003", "name": "Three", "sic": "1311"}
    body = _zip({
        MAIN_NAME: _doc(), PAGE_NAME: _doc(PAGE_NAME),
        "CIK0000000002.json": other, "CIK0000000003.json": third,
    })  # fmt: skip
    newer = _doc() | {"sic": "3572"}
    with store.session(settings.db_path, wait=0) as con:
        con.execute(
            "INSERT INTO edgar_company VALUES"
            f" ({APPLE}, 'AAPL', 'Apple Inc.', 'AAPL', true, '2026-10-08'),"
            " (2, 'TWO', 'Two', 'TWO', true, '2026-10-08')"
        )
        zip_id = raw.save(
            con, source="edgar", dataset="bulk_submissions",
            url="https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip",
            body=body, http_status=200, fetched_at=T1, raw_dir=settings.raw_dir,
        )  # fmt: skip
        resp_id = raw.save(
            con, source="edgar", dataset="edgar_submissions",
            url=f"https://data.sec.gov/submissions/{MAIN_NAME}",
            body=json.dumps(newer).encode(), http_status=200, fetched_at=T2,
            raw_dir=settings.raw_dir,
        )  # fmt: skip
    return zip_id, resp_id


def test_backfill_reads_company_members_then_newer_per_cik_responses(settings):
    zip_id, resp_id = _seed(settings)
    with store.session(settings.db_path, wait=0) as con:
        done = profile.backfill(
            con, raw_dir=settings.raw_dir, zip_raw_id=zip_id, ciks={APPLE, 2},
            name_re=submissions.NAME,
        )  # fmt: skip
        assert (done.members, done.responses, done.written, done.errors) == (2, 1, 3, [])
        rows = con.execute(
            "SELECT cik, sic, seen_at, raw_id FROM edgar_company_profile ORDER BY cik"
        ).fetchall()
        assert rows == [(2, 6022, T1, zip_id), (APPLE, 3572, T2, resp_id)]
        assert con.execute("SELECT count(*) FROM edgar_filing").fetchone() == (0,)
        again = profile.backfill(
            con, raw_dir=settings.raw_dir, zip_raw_id=zip_id, ciks={APPLE, 2},
            name_re=submissions.NAME,
        )  # fmt: skip
        assert again.written == 2  # CIK 2 at the same time and Apple's T2 response; T1 loses
        assert con.execute(
            "SELECT cik, sic FROM edgar_company_profile ORDER BY cik"
        ).fetchall() == [(2, 6022), (APPLE, 3572)]


def test_cli_edgar_profiles_fills_the_table_from_the_stored_zip(settings, monkeypatch):
    _seed(settings)
    monkeypatch.setenv("USFIN_DB", str(settings.db_path))
    monkeypatch.setenv("USFIN_RAW", str(settings.raw_dir))
    monkeypatch.setenv("USFIN_CONFIG", str(settings.config_dir))
    monkeypatch.setenv("USFIN_LOCK_WAIT", "0")
    out = CliRunner().invoke(app, ["edgar", "profiles"])
    assert out.exit_code == 0, out.output
    assert "members 2, responses 1, written 3, errors 0; profiles 2, with SIC 2" in out.output
