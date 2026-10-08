"""EDGAR company page (`/edgar/{cik|ticker}`) and the XBRL screen (`/screen`) (S33).

The company page shows the header (`edgar_company`), the company's filings newest first and
paged, key facts by the concept map (per concept the newest annual, quarterly and instant frame,
first listed tag wins, with the tag used), an annual chart of one concept, recent Form 4
transactions and Schedule 13D/G stakes on the company; each table has a CSV (`?format=csv&
table=`). `/screen` is S28's `screen` (one concept × one frame period from `xbrl_fact`) or, on
demand, SEC's frames API through `frames_cache`. Rows here are written by the test; the frames
response is S28's recorded fixture. No request leaves the process.
"""

import csv
import io
import re
import shutil
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
import respx
from fastapi.testclient import TestClient

from usfin import http, jobs, ratelimit, raw, scheduler, store
from usfin.settings import load_settings
from usfin.web import app as web
from usfin.web import edgar as web_edgar

REPO_CONFIG = Path(__file__).resolve().parents[1] / "config"
FRAME = (
    Path(__file__).parent / "fixtures" / "edgar" / "frames" / "us-gaap_Revenues_USD_CY2025Q2.json"
)
FRAME_URL = "https://data.sec.gov/api/xbrl/frames/us-gaap/Revenues/USD/CY2025Q2.json"
NOW = datetime(2026, 10, 8, 21, 0, tzinfo=UTC)
T0 = NOW.replace(tzinfo=None)
UA = "usfin tests ops@example.test"
APPLE, BRK = 320193, 1067983
RFCC = "RevenueFromContractWithCustomerExcludingAssessedTax"


@pytest.fixture
def settings(tmp_path, monkeypatch):
    for key in ("USFIN_DB", "USFIN_RAW", "USFIN_CONFIG", "USFIN_LOCK_WAIT", "USFIN_DEMO"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.delenv("SEC_USER_AGENT", raising=False)
    config = tmp_path / "config"
    shutil.copytree(REPO_CONFIG, config)
    (config / "schedule.yaml").write_text("jobs: {}\n", "utf-8")
    t = [0.0]
    monkeypatch.setattr(
        ratelimit,
        "LIMITERS",
        ratelimit.Limiters(clock=lambda: t[0], sleep=lambda s: t.__setitem__(0, t[0] + s)),
    )
    monkeypatch.setattr(http, "BACKOFF_BASE", 0.0)
    monkeypatch.setattr(http, "SEC_BLOCK", http.SecBlock())
    monkeypatch.setattr(raw, "_utcnow", lambda: T0)
    return replace(
        load_settings(env_file=tmp_path / "no.env"),
        db_path=tmp_path / "usfin.duckdb",
        raw_dir=tmp_path / "raw",
        config_dir=config,
        lock_wait=0,
        sec_user_agent=UA,
    )


def _fact(con, cik, tag, value, start, end, frame, *, filed, accn, unit="USD", form="10-K",
          taxonomy="us-gaap"):  # fmt: skip
    con.execute(
        "INSERT INTO xbrl_fact VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, ?)",
        [cik, taxonomy, tag, unit, accn, start, end, start == end, value, form, filed, frame],
    )


def _filing(con, accn, cik, form, filed, accepted=None, doc=None):
    con.execute(
        "INSERT INTO edgar_filing VALUES (?, ?, ?, ?, ?, NULL, ?, ?, 'submissions')",
        [accn, cik, form, filed, accepted, doc, form.endswith("/A")],
    )


@pytest.fixture
def db(settings):
    with store.session(settings.db_path, wait=0) as con:
        con.execute(
            "INSERT INTO edgar_company VALUES"
            " (320193, 'AAPL', 'Apple Inc.', 'AAPL', true, '2026-10-08'),"
            " (1067983, 'BRK-B', 'BERKSHIRE HATHAWAY INC', 'BRK.B', true, '2026-10-08'),"
            " (1067983, 'BRK-A', 'BERKSHIRE HATHAWAY INC', 'BRK.A', true, '2026-10-08')"
        )
        k25, q25 = "0000320193-25-000079", "0000320193-26-000020"
        # Apple revenue: SalesRevenueNet until 2016, the ASC 606 tag later; 2017-2022 not stored
        _fact(con, APPLE, "SalesRevenueNet", 215.639e9, "2015-09-27", "2016-09-24", "CY2016",
              filed="2016-10-26", accn="0001628280-16-020309")  # fmt: skip
        _fact(con, APPLE, RFCC, 383.285e9, "2022-09-25", "2023-09-30", "CY2023",
              filed="2025-10-31", accn=k25)  # fmt: skip
        _fact(con, APPLE, RFCC, 391.035e9, "2023-10-01", "2024-09-28", "CY2024",
              filed="2025-10-31", accn=k25)  # fmt: skip
        _fact(con, APPLE, RFCC, 416.161e9, "2024-09-29", "2025-09-27", "CY2025",
              filed="2025-10-31", accn=k25)  # fmt: skip
        # the quarter, repeated by a later 10-Q (the frame sits on the newest filing)
        _fact(con, APPLE, RFCC, 94.0e9, "2025-03-30", "2025-06-28", "CY2025Q2",
              filed="2025-08-01", accn="0000320193-25-000073", form="10-Q")  # fmt: skip
        _fact(con, APPLE, RFCC, 94.036e9, "2025-03-30", "2025-06-28", "CY2025Q2",
              filed="2026-07-31", accn=q25, form="10-Q")  # fmt: skip
        # a newer fact without a frame is not a frame period
        _fact(con, APPLE, RFCC, 1.0, "2026-03-29", "2026-06-27", None,
              filed="2026-07-31", accn=q25, form="10-Q")  # fmt: skip
        _fact(con, APPLE, "Revenues", 7.0, "2025-03-30", "2025-06-28", "CY2025Q2",
              filed="2026-07-31", accn=q25, unit="CAD", form="10-Q")  # fmt: skip
        _fact(con, APPLE, "Assets", 331.495e9, "2025-06-28", "2025-06-28", "CY2025Q2I",
              filed="2026-07-31", accn=q25, form="10-Q")  # fmt: skip
        _fact(con, APPLE, "Assets", 364.98e9, "2024-09-28", "2024-09-28", "CY2024Q3I",
              filed="2025-10-31", accn=k25)  # fmt: skip
        _fact(con, APPLE, "EarningsPerShareDiluted", 7.46, "2024-09-29", "2025-09-27", "CY2025",
              filed="2025-10-31", accn=k25, unit="USD/shares")  # fmt: skip
        # Berkshire reports both revenue tags for the quarter: the first listed (Revenues) wins
        brk = "0001193125-25-170000"
        _fact(con, BRK, "Revenues", 92.515e9, "2025-04-01", "2025-06-30", "CY2025Q2",
              filed="2025-08-02", accn=brk, form="10-Q")  # fmt: skip
        _fact(con, BRK, "NetIncomeLoss", 4.0e9, "2014-07-01", "2014-09-30", "CY2014Q3",
              filed="2014-11-03", accn="0001193125-14-390000", form="10-Q")  # fmt: skip
        _fact(con, BRK, RFCC, 10.0e9, "2025-04-01", "2025-06-30", "CY2025Q2",
              filed="2025-08-02", accn=brk, form="10-Q")  # fmt: skip

        _filing(con, k25, APPLE, "10-K", "2025-10-31", "2025-10-31 20:01:00", "aapl-20250927.htm")
        _filing(con, q25, APPLE, "10-Q", "2026-07-31", "2026-07-31 20:30:00", "aapl-20260627.htm")
        _filing(con, "0000320193-26-000019", APPLE, "8-K", "2026-07-30")
        _filing(con, brk, BRK, "10-Q", "2025-08-02")

        f4 = "0001127602-26-000111"
        _filing(con, f4, APPLE, "4", "2026-08-12")
        con.execute(
            "INSERT INTO insider_filing VALUES (?, '4', 320193, 'Apple Inc.', 'AAPL',"
            " '2026-08-10', NULL, 'ownership.xml', 1, 2)",
            [f4],
        )
        con.execute(
            "INSERT INTO insider_owner VALUES (?, 1, 1214156, 'COOK TIMOTHY D', true, true,"
            " false, false, 'Chief Executive Officer', NULL)",
            [f4],
        )
        con.execute(
            "INSERT INTO insider_txn (accession_no, line_no, issuer_cik, owner_cik, owner_name,"
            " is_derivative, is_holding, security_title, txn_date, txn_code, shares, price,"
            " acquired_disposed, shares_owned_after, direct_indirect) VALUES"
            " (?, 1, 320193, 1214156, 'COOK TIMOTHY D', false, false, 'Common Stock',"
            "  '2026-08-10', 'S', 108136, 229.98, 'D', 3280295, 'D'),"
            " (?, 2, 320193, 1214156, 'COOK TIMOTHY D', false, true, 'Common Stock in trust',"
            "  NULL, NULL, NULL, NULL, NULL, 77777, 'I')",
            [f4, f4],
        )
        sc = "0000102909-26-001234"
        con.execute(
            "INSERT INTO ownership_stake (accession_no, person_seq, filer_cik, filer_name,"
            " subject_cik, form, event_date, percent_of_class, shares, filed_at, person_type)"
            " VALUES (?, 1, NULL, 'The Vanguard Group', 320193, 'SCHEDULE 13G/A', '2026-06-30',"
            " 9.31, 1386700000, '2026-07-29', 'IA')",
            [sc],
        )
    return settings


@pytest.fixture
def client(db):
    s = scheduler.Scheduler(db, jobs.Registry(), clock=lambda: NOW)
    yield TestClient(web.create_app(s, manage=False, ui_wait=0.3))
    s.shutdown()


class Fake:
    """data.sec.gov frames: the fixture for Revenues USD CY2025Q2, 404 for any other frame."""

    def __init__(self):
        self.body = FRAME.read_text("utf-8")
        self.requests: list[str] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(str(request.url))
        if str(request.url) != FRAME_URL:
            return httpx.Response(404, text="<html>not found</html>")
        return httpx.Response(200, text=self.body, headers={"content-type": "application/json"})


@pytest.fixture
def fake(settings):
    f = Fake()
    with respx.mock(assert_all_called=False) as router:
        router.route().mock(side_effect=f)  # every request is seen, none leaves
        yield f


def _csv(response):
    assert response.status_code == 200, response.text
    assert response.headers["content-type"].startswith("text/csv")
    assert "attachment" in response.headers["content-disposition"]
    return list(csv.DictReader(io.StringIO(response.text)))


def _svgs(page):
    return re.findall(r"<svg\b.*?</svg>", page, flags=re.DOTALL)


# --- company page ---------------------------------------------------------------------------


def test_company_page_shows_header_filings_key_facts_chart_insiders_and_stakes(client):
    page = client.get(f"/edgar/{APPLE}")
    assert page.status_code == 200
    text = page.text
    assert "Apple Inc." in text and "AAPL" in text and "0000320193" in text
    assert 'href="/edgar"' in text and 'aria-current="page"' in text  # nav entry
    # filings, newest first, linked to SEC's filing index
    assert text.index("0000320193-26-000020") < text.index("0000320193-25-000079")
    assert "https://www.sec.gov/Archives/edgar/data/320193/000032019326000020/" in text
    # key facts with the tag used
    assert RFCC in text and "94,036,000,000" in text and "331,495,000,000" in text
    assert "/screen?concept=revenue&amp;period=CY2025Q2" in text
    # an annual revenue chart
    svgs = _svgs(text)
    assert len(svgs) == 1 and 'role="img"' in svgs[0] and "revenue" in svgs[0]
    # insider transactions (holdings are not transactions) and 13D/G stakes on the company
    assert "COOK TIMOTHY D" in text and "Chief Executive Officer" in text
    assert "Common Stock in trust" not in text
    assert "The Vanguard Group" in text and "9.31" in text
    assert "<script" not in text


def test_a_ticker_in_any_notation_resolves_to_its_cik(client):
    page = client.get("/edgar/aapl", follow_redirects=False)
    assert page.status_code in (302, 307)
    assert page.headers["location"] == f"/edgar/{APPLE}"
    for spelling in ("BRK.B", "brk-b", "BRK/B", "BRK-A"):
        page = client.get(f"/edgar/{spelling}")
        assert page.status_code == 200 and "BERKSHIRE HATHAWAY INC" in page.text, spelling
    csv_redirect = client.get("/edgar/AAPL?format=csv&table=facts", follow_redirects=False)
    assert csv_redirect.headers["location"] == f"/edgar/{APPLE}?format=csv&table=facts"
    assert client.get("/edgar/NOPE").status_code == 404
    assert client.get("/edgar/999999999").status_code == 404  # no company, no filing, no fact
    lookup = client.get("/edgar?q=aapl", follow_redirects=False)
    assert lookup.headers["location"] == "/edgar/aapl"


def test_key_facts_pick_the_newest_frame_per_kind_and_the_first_listed_tag(client):
    rows = _csv(client.get(f"/edgar/{APPLE}?format=csv&table=facts"))
    got = {(r["concept"], r["kind"]): r for r in rows}
    q = got[("revenue", "quarter")]
    assert (q["period"], q["tag"], q["value"]) == ("CY2025Q2", RFCC, "94036000000.0")
    assert q["filed"] == "2026-07-31"  # the frame's newest filing, not the 2025 10-Q
    a = got[("revenue", "annual")]
    assert (a["period"], a["value"]) == ("CY2025", "416161000000.0")
    assets = got[("assets", "instant")]
    assert (assets["period"], assets["value"]) == ("CY2025Q2I", "331495000000.0")
    assert got[("eps_diluted", "annual")]["unit"] == "USD/shares"
    assert ("net_income", "annual") not in got  # nothing reported, nothing shown
    brk = _csv(client.get(f"/edgar/{BRK}?format=csv&table=facts"))
    assert [(r["concept"], r["tag"], r["value"]) for r in brk] == [
        ("revenue", "Revenues", "92515000000.0"),
        ("net_income", "NetIncomeLoss", "4000000000.0"),
    ]
    # a newest frame years older than the company's other key facts is marked old
    page = client.get(f"/edgar/{BRK}").text
    row = re.search(r"<tr>\s*<td class=\"name\">net_income</td>.*?</tr>", page, re.DOTALL)
    assert row and ">old<" in row.group(0)
    assert ">old<" not in client.get(f"/edgar/{APPLE}").text


def test_annual_chart_has_a_gap_for_years_not_reported_and_can_switch_concept(client):
    svg = _svgs(client.get(f"/edgar/{APPLE}").text)[0]
    paths = re.findall(r'<path class="line" d="([^"]+)"', svg)
    assert len(paths) == 1 and paths[0].count("M") == 1  # 2023-2025 drawn
    assert svg.count('class="dot"') == 1  # 2016 alone before the 2017-2022 gap
    assert "6 missing" in svg
    eps = _svgs(client.get(f"/edgar/{APPLE}?chart=eps_diluted").text)
    assert "eps_diluted" in eps[0]
    # an instant concept is charted by quarter-end: 2024 Q3, (Q4), (2025 Q1), Q2
    assets = _svgs(client.get(f"/edgar/{APPLE}?chart=assets").text)[0]
    assert assets.count('class="dot"') == 2 and "2 missing" in assets
    assert "no values" in client.get(f"/edgar/{APPLE}?chart=net_income").text
    assert client.get(f"/edgar/{APPLE}?chart=nope").status_code == 400


def test_filings_are_paged_and_their_csv_has_every_filing(client, monkeypatch):
    monkeypatch.setattr(web_edgar, "FILINGS_PAGE", 2)

    def filings_section(url):
        page = client.get(url).text
        return page[page.index("<h2>Filings</h2>") : page.index("<h2>Insider")]

    first = filings_section(f"/edgar/{APPLE}")
    assert "0000320193-26-000020" in first and "0001127602-26-000111" in first
    assert "0000320193-25-000079" not in first
    assert f"/edgar/{APPLE}?page=2" in first
    second = filings_section(f"/edgar/{APPLE}?page=2")
    assert "0000320193-25-000079" in second and "0001127602-26-000111" not in second
    assert client.get(f"/edgar/{APPLE}?page=0").status_code == 400
    rows = _csv(client.get(f"/edgar/{APPLE}?format=csv"))  # the filings table by default
    assert [r["accession_no"] for r in rows] == [
        "0001127602-26-000111",
        "0000320193-26-000020",
        "0000320193-26-000019",
        "0000320193-25-000079",
    ]
    assert rows[1]["form"] == "10-Q" and rows[1]["accepted_at"] == "2026-07-31 20:30:00Z"


def test_insider_and_stake_csvs(client):
    txns = _csv(client.get(f"/edgar/{APPLE}?format=csv&table=insider"))
    assert [(r["owner_name"], r["txn_code"], r["shares"], r["price"]) for r in txns] == [
        ("COOK TIMOTHY D", "S", "108136.0", "229.98")
    ]
    stakes = _csv(client.get(f"/edgar/{APPLE}?format=csv&table=stakes"))
    assert [(r["filer_name"], r["percent_of_class"]) for r in stakes] == [
        ("The Vanguard Group", "9.31")
    ]
    assert client.get(f"/edgar/{APPLE}?format=csv&table=nope").status_code == 400


# --- screen ---------------------------------------------------------------------------------


def test_screen_without_a_query_offers_the_concepts(client):
    page = client.get("/screen")
    assert page.status_code == 200
    assert 'href="/screen"' in page.text
    for name in ("revenue", "assets", "eps_diluted", "shares_outstanding"):
        assert f'value="{name}"' in page.text


def test_screen_lists_companies_with_the_tag_used(client):
    page = client.get("/screen?concept=revenue&period=CY2025Q2")
    assert page.status_code == 200
    text = page.text
    assert text.index("Apple Inc.") < text.index("BERKSHIRE HATHAWAY INC")  # largest first
    assert f'href="/edgar/{APPLE}"' in text
    assert RFCC in text and "Revenues" in text
    rows = _csv(client.get("/screen?concept=revenue&period=CY2025Q2&format=csv"))
    assert [(r["cik"], r["tag"], r["value"]) for r in rows] == [
        (str(APPLE), RFCC, "94036000000.0"),
        (str(BRK), "Revenues", "92515000000.0"),
    ]
    assert rows[0]["filed"] == "2026-07-31"
    assets = _csv(client.get("/screen?concept=assets&period=CY2025Q2I&format=csv"))
    assert [r["value"] for r in assets] == ["331495000000.0"]


def test_screen_rejects_a_bad_period_or_concept(client):
    assert client.get("/screen?concept=revenue&period=2025Q2").status_code == 400
    assert client.get("/screen?concept=nope&period=CY2025Q2").status_code == 400


def test_screen_frames_on_demand_go_through_the_cache(client, db, fake):
    url = "/screen?concept=revenue&period=CY2025Q2&source=frames"
    page = client.get(url)
    assert page.status_code == 200, page.text
    assert fake.requests == [FRAME_URL]  # the concept's first tag
    assert "WALMART INC." in page.text and "177,402,000,000" in page.text
    assert 'href="/edgar/104169"' in page.text
    again = client.get(url + "&format=csv")
    assert fake.requests == [FRAME_URL]  # answered from frames_cache
    rows = _csv(again)
    assert rows[0]["cik"] == "104169" and rows[0]["value"] == "177402000000.0"
    with store.session(db.db_path, wait=0) as con:
        assert con.execute("SELECT key FROM frames_cache").fetchall() == [
            ("us-gaap/Revenues/USD/CY2025Q2",)
        ]
        assert con.execute("SELECT count(*) FROM xbrl_fact").fetchone()[0] == 14  # untouched
    missing = client.get(f"/screen?concept=revenue&period=CY2025Q2&source=frames&tag={RFCC}")
    assert missing.status_code == 404 and "404" in missing.text
    assert client.get(url + "&tag=NotInTheConcept").status_code == 400


def test_screen_frames_without_a_user_agent_send_nothing(db, fake):
    s = scheduler.Scheduler(replace(db, sec_user_agent=None), jobs.Registry(), clock=lambda: NOW)
    try:
        client = TestClient(web.create_app(s, manage=False, ui_wait=0.3))
        page = client.get("/screen?concept=revenue&period=CY2025Q2&source=frames")
    finally:
        s.shutdown()
    assert page.status_code == 503 and "SEC_USER_AGENT" in page.text
    assert fake.requests == []
