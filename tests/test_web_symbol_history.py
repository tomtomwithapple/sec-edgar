"""Symbol history page (`/symbols/history`, S40).

`symbol_history` rows are written by the test as S30's diff stores them: a baseline snapshot T0
per file opens every symbol (not shown as listings), then at T1 a listing (NEWCO), a delisting
(OLDX), a rename (ACME → Acme Holdings: close + open at the same instant) and an exchange move
(MOVE: N → Q, a delisting plus a listing); `symbol_diff` has the three snapshots, one gated.
"""

from datetime import UTC, datetime

import pytest
from test_web_symbols import _client, _csv, settings  # noqa: F401

from usfin import store

T0, T1, T2 = (datetime(2026, 10, d, 11, 15, tzinfo=UTC).replace(tzinfo=None) for d in (6, 7, 8))
NL, OL = "nasdaqlisted", "otherlisted"


def _row(con, symbol, exchange, start, end, name, etf=False, category=None, file=None):
    con.execute(
        "INSERT INTO symbol_history VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL)",
        [symbol, exchange, start, end, file or (NL if exchange == "Q" else OL), name, etf,
         category],
    )  # fmt: skip


@pytest.fixture
def client(settings):  # noqa: F811
    with store.session(settings.db_path, wait=0) as con:
        _row(con, "AAPL", "Q", T0, None, "Apple Inc. - Common Stock", category="Q")
        _row(con, "SPY", "P", T0, None, "SPDR S&P 500 ETF Trust", etf=True)
        _row(con, "OLDX", "Q", T0, T1, "Old Corp", category="G")
        _row(con, "ACME", "Q", T0, T1, "Acme Corp", category="S")
        _row(con, "ACME", "Q", T1, None, "Acme Holdings", category="S")
        _row(con, "MOVE", "N", T0, T1, "Mover Inc")
        _row(con, "MOVE", "Q", T1, None, "Mover Inc", category="G")
        _row(con, "NEWCO", "Q", T1, None, "NewCo Inc", category="G")
        for file, at, rows, prev, diffed, opened, closed in [
            (NL, T0, 5, None, True, 4, 0),
            (NL, T1, 5, T0, True, 3, 3),
            (OL, T2, 1, T0, False, 0, 0),
        ]:
            con.execute(
                "INSERT INTO symbol_diff VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [file, at, rows, prev, None if prev is None else 5, None if prev is None
                 else 0.0, diffed, opened, closed],
            )  # fmt: skip
    s, c = _client(settings)
    yield c
    s.shutdown()


def test_events_after_the_baseline_newest_first(client):
    rows = _csv(client.get("/symbols/history?format=csv"))
    got = [(r["event"], r["symbol"], r["exchange"]) for r in rows]
    assert sorted(got) == sorted(
        [
            ("changed", "ACME", "Q"),
            ("delisted", "MOVE", "N"),
            ("listed", "MOVE", "Q"),
            ("listed", "NEWCO", "Q"),
            ("delisted", "OLDX", "Q"),
        ]
    )  # AAPL and SPY are baseline only
    acme = next(r for r in rows if r["symbol"] == "ACME")
    assert acme["changed"] == "name" and acme["before"] == "name=Acme Corp"
    assert acme["name"] == "Acme Holdings" and acme["at"] == "2026-10-07 11:15:00Z"


@pytest.mark.parametrize(("event", "n"), [("listed", 2), ("delisted", 2), ("changed", 1)])
def test_event_filter(client, event, n):
    rows = _csv(client.get(f"/symbols/history?format=csv&event={event}"))
    assert len(rows) == n and {r["event"] for r in rows} == {event}


def test_a_symbol_shows_every_version_and_its_events(client):
    page = client.get("/symbols/history?symbol=move")  # any case
    assert page.status_code == 200 and "<script" not in page.text
    assert "MOVE: listing history" in page.text
    versions = _csv(client.get("/symbols/history?symbol=MOVE&format=csv&table=versions"))
    assert [(v["exchange"], v["valid_to"]) for v in versions] == [
        ("Q", ""), ("N", "2026-10-07 11:15:00Z"),
    ]  # fmt: skip
    events = _csv(client.get("/symbols/history?symbol=MOVE&format=csv"))
    assert {e["event"] for e in events} == {"listed", "delisted"}


def test_page_shows_baseline_counts_and_snapshots_with_the_gated_one(client):
    text = client.get("/symbols/history").text
    assert "Baseline" in text and "2026-10-06 11:15:00Z" in text
    assert "listed (2)" in text and "changed (1)" in text
    assert "gated" in text
    snaps = _csv(client.get("/symbols/history?format=csv&table=snapshots"))
    assert [s["snapshot_at"][:10] for s in snaps] == ["2026-10-08", "2026-10-07", "2026-10-06"]


def test_unmatched_page_links_to_history(client):
    text = client.get("/symbols/unmatched?match=all").text
    assert 'href="/symbols/history"' in text
    assert 'href="/symbols/history?symbol=AAPL"' in text


@pytest.mark.parametrize("query", ["event=nope", "table=nope", "table=versions"])
def test_bad_parameters_are_400(client, query):
    assert client.get(f"/symbols/history?{query}").status_code == 400


def test_nothing_stored_says_which_job_to_run(settings):  # noqa: F811
    s, c = _client(settings)
    try:
        page = c.get("/symbols/history")
        assert page.status_code == 200 and "usfin run symbols" in page.text
        assert c.get("/symbols/history?format=csv").text.startswith("at,event,")
    finally:
        s.shutdown()
