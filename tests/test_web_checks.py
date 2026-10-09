"""Auction cross-check page (`/series/auctions/xcheck`, S38) and BLS release calendar
(`/series/releases`, S39).

Rows are written by the test into `auction_xcheck` and `bls_release` as the jobs store them; no
request leaves the process. The clock is 2026-10-08 21:00 UTC (17:00 ET), so "today" in New York
is 2026-10-08.
"""

import json

import pytest
from test_web_symbols import _client, _csv, settings  # noqa: F401

from usfin import store

CPI, CORE, PAYROLLS = "CUSR0000SA0", "CUSR0000SA0L1E", "CES0000000001"


def _xcheck(con):
    rows = [
        ("912797AA1", "2026-10-06", "matched", "Bill", None),
        ("912797AB9", "2026-10-07", "matched", "Bill", None),
        ("91282CAB9", "2026-08-12", "mismatch", "Note",
         {"high_yield": {"fiscal": "4.125", "treasurydirect": "4.130"}}),
        ("91282CAC7", "2026-10-07", "pending", "Note",
         {"bid_to_cover": {"fiscal": None, "treasurydirect": "2.51"}}),
        ("912810AA0", "2026-09-10", "td_only", "Bond", None),
    ]  # fmt: skip
    for cusip, d, result, typ, diff in rows:
        con.execute(
            "INSERT INTO auction_xcheck VALUES (?, ?, ?, ?, ?)",
            [cusip, d, result, typ, None if diff is None else json.dumps(diff)],
        )


def _releases(con):
    rows = [
        ("2026-09-11", "Consumer Price Index", "2026-09-11 12:30"),
        ("2026-10-03", "Employment Situation", "2026-10-03 12:30"),
        ("2026-10-08", "Consumer Price Index", "2026-10-08 12:30"),  # today: upcoming
        ("2026-10-15", "Producer Price Index", "2026-10-15 12:30"),
        ("2026-11-06", "Employment Situation", "2026-11-06 13:30"),  # EST: 08:30 ET
        ("2026-10-20", "Real Earnings", "2026-10-20 12:30"),  # no configured series
    ]
    for d, name, at in rows:
        con.execute("INSERT INTO bls_release VALUES (?, ?, ?, 1)", [d, name, at])


@pytest.fixture
def client(settings):  # noqa: F811
    with store.session(settings.db_path, wait=0) as con:
        _xcheck(con)
        _releases(con)
    s, c = _client(settings)
    yield c
    s.shutdown()


@pytest.fixture
def empty(settings):  # noqa: F811
    s, c = _client(settings)
    yield c
    s.shutdown()


# --- auction cross-check ---------------------------------------------------------------------


def test_xcheck_lists_pairs_not_matched_disagreement_first_with_fields(client):
    page = client.get("/series/auctions/xcheck")
    assert page.status_code == 200
    text = page.text
    assert "<script" not in text
    assert text.index("91282CAB9") < text.index("91282CAC7") < text.index("912810AA0")
    assert "912797AA1" not in text  # matched pairs are not listed by default
    assert "high_yield" in text and "4.125" in text and "4.130" in text
    rows = _csv(client.get("/series/auctions/xcheck?format=csv"))
    assert [r["result"] for r in rows] == ["mismatch", "pending", "td_only"]
    assert json.loads(rows[0]["diff"]) == {
        "high_yield": {"fiscal": "4.125", "treasurydirect": "4.130"}
    }
    assert rows[0]["fields"] == "high_yield"


def test_xcheck_summary_counts_each_result_per_type_and_in_total(client):
    rows = _csv(client.get("/series/auctions/xcheck?format=csv&table=summary"))
    by = {r["security_type"]: r for r in rows}
    assert [r["security_type"] for r in rows] == ["Bill", "Bond", "Note", "all"]
    assert by["Bill"]["matched"] == "2" and by["Note"]["mismatch"] == "1"
    assert by["all"]["pairs"] == "5" and by["all"]["td_only"] == "1"
    assert by["all"]["first"] == "2026-08-12" and by["all"]["last"] == "2026-10-07"


@pytest.mark.parametrize(
    ("result", "n"), [("all", 5), ("matched", 2), ("mismatch", 1), ("open", 3)]
)
def test_xcheck_result_filter(client, result, n):
    rows = _csv(client.get(f"/series/auctions/xcheck?format=csv&result={result}"))
    assert len(rows) == n


@pytest.mark.parametrize("query", ["result=nope", "table=nope"])
def test_xcheck_bad_parameters_are_400(client, query):
    assert client.get(f"/series/auctions/xcheck?{query}").status_code == 400


def test_xcheck_with_nothing_stored_says_which_job_to_run(empty):
    page = empty.get("/series/auctions/xcheck")
    assert page.status_code == 200 and "usfin run auction_xcheck" in page.text
    assert empty.get("/series/auctions/xcheck?format=csv").text.startswith("auction_date,")


def test_auctions_and_series_pages_link_to_the_new_pages(client):
    assert 'href="/series/auctions/xcheck"' in client.get("/series/auctions").text
    index = client.get("/series").text
    assert 'href="/series/releases"' in index and 'href="/series/auctions/xcheck"' in index


# --- BLS release calendar --------------------------------------------------------------------


def test_releases_default_to_upcoming_from_today_in_new_york_soonest_first(client):
    rows = _csv(client.get("/series/releases?format=csv"))
    assert [r["release_date"] for r in rows] == [
        "2026-10-08", "2026-10-15", "2026-10-20", "2026-11-06",
    ]  # fmt: skip
    assert rows[0]["series"] == f"{CPI} {CORE}"
    assert rows[2]["series"] == ""  # Real Earnings publishes no configured series


def test_releases_page_shows_new_york_time_and_links_each_series(client):
    page = client.get("/series/releases")
    assert page.status_code == 200 and "<script" not in page.text
    assert "Thu 08:30" in page.text  # 12:30 UTC in EDT
    assert "Fri 08:30" in page.text  # 2026-11-06 13:30 UTC in EST
    assert f'href="/series/bls/{CPI}"' in page.text and f'href="/series/bls/{PAYROLLS}"' in page.text


def test_past_releases_are_newest_first_and_a_release_filters(client):
    past = _csv(client.get("/series/releases?format=csv&when=past"))
    assert [r["release_date"] for r in past] == ["2026-10-03", "2026-09-11"]
    cpi = _csv(client.get("/series/releases?format=csv&when=all&release=Consumer+Price+Index"))
    assert [r["release_date"] for r in cpi] == ["2026-09-11", "2026-10-08"]


def test_release_summary_has_next_and_last_per_configured_release(client):
    rows = {r["release_name"]: r for r in _csv(client.get("/series/releases?format=csv&table=summary"))}
    assert "Real Earnings" not in rows
    assert rows["Consumer Price Index"]["next"] == "2026-10-08"
    assert rows["Consumer Price Index"]["last"] == "2026-09-11"
    assert rows["Employment Situation"]["next"] == "2026-11-06"
    assert PAYROLLS in rows["Employment Situation"]["series"]


@pytest.mark.parametrize(
    ("query", "status"), [("when=soon", 400), ("table=nope", 400), ("release=Nope", 404)]
)
def test_releases_bad_parameters(client, query, status):
    assert client.get(f"/series/releases?{query}").status_code == status


def test_releases_with_nothing_stored_says_which_job_to_run(empty):
    page = empty.get("/series/releases")
    assert page.status_code == 200 and "usfin run bls_calendar" in page.text
    assert empty.get("/series/releases?format=csv").text.startswith("release_date,")
