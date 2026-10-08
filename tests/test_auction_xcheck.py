"""Job `auction_xcheck`: Fiscal Data `fd_auctions` vs TreasuryDirect `tsy_security` (C15).

Rows are seeded straight into both tables with values as the two sources store them for real
auctions (data/usfin.duckdb on 2026-10-08): a note (high yield; TD's "5.3000" vs Fiscal Data's
"5.300000"), a bill (discount and investment rate, no yield), an FRN reopening (discount margin
only; the same CUSIP auctioned again a month later) and an announced bill with no results yet.
"""

import json
import shutil
from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from usfin import alerts, jobs, store
from usfin.settings import load_settings
from usfin.sources import auction_xcheck as xc
from usfin.sources import fiscal

REPO_CONFIG = Path(__file__).resolve().parents[1] / "config"
NOW = datetime(2026, 10, 8, 21, 30, tzinfo=UTC)  # 17:30 ET, after the 17:00 fiscal run
LATER_SAME_DAY = datetime(2026, 10, 8, 23, 0, tzinfo=UTC)
NEXT_DAY = datetime(2026, 10, 9, 21, 30, tzinfo=UTC)

# (cusip, auction_date, security_type, security_term, offering_amt,
#  high_yield, high_discount_rate, high_investment_rate, high_discount_margin)
AUCTIONS = [
    ("91282CRF0", "2026-10-07", "Note", "9-Year 10-Month", "39000000000", 5.3, None, None, None),
    ("912797WY9", "2026-10-07", "Bill", "17-Week", "75000000000", None, 4.105, 4.219, None),
    ("91282CRD5", "2026-08-26", "Note", "1-Year 11-Month", "28000000000", None, None, None, 0.055),
    ("91282CRD5", "2026-09-23", "Note", "1-Year 10-Month", "28000000000", None, None, None, 0.04),
    ("912797VW4", "2026-10-08", "Bill", "4-Week", "110000000000", None, None, None, None),
]

FD_DDL = """
CREATE TABLE fd_auctions (
    record_date DATE, cusip VARCHAR NOT NULL, security_type VARCHAR, security_term VARCHAR,
    auction_date DATE NOT NULL, offering_amt DECIMAL(24,4), high_yield DOUBLE,
    high_discnt_rate DOUBLE, high_investment_rate DOUBLE, high_discnt_margin DOUBLE,
    PRIMARY KEY (cusip, auction_date)
)"""


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


def _seed(settings, fd=AUCTIONS, td=AUCTIONS):
    with store.session(settings.db_path, wait=0) as con:
        con.execute(FD_DDL)
        for cusip, day, typ, term, amt, y, dr, ir, m in fd:
            con.execute(
                "INSERT INTO fd_auctions VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [cusip, typ, term, day, Decimal(amt), y, dr, ir, m],
            )
        for cusip, day, typ, term, amt, y, dr, ir, m in td:
            status = "announced" if (y, dr, ir, m) == (None,) * 4 else "auctioned"
            con.execute(
                "INSERT INTO tsy_security (cusip, auction_date, security_type, security_term,"
                " offering_amt, high_yield, high_discount_rate, high_investment_rate,"
                " high_discount_margin, status, payload) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '{}')",
                [cusip, day, typ, term, Decimal(amt), y, dr, ir, m, status],
            )


def _changed(rows, cusip, day, **values):
    out = []
    for row in rows:
        if row[0] == cusip and row[1] == day:
            fields = ["offering_amt", "high_yield", "high_discount_rate",
                      "high_investment_rate", "high_discount_margin"]
            row = list(row)
            for name, value in values.items():
                row[4 + fields.index(name)] = value
            row = tuple(row)
        out.append(row)
    return out


def _registry():
    reg = jobs.Registry()
    reg.register(xc.JOB)
    return reg


def _run(settings, clock=NOW):
    return jobs.run_job(
        "auction_xcheck", settings=settings, registry=_registry(), clock=lambda: clock
    )


def _alerts(settings):
    with store.session(settings.db_path, wait=0) as con:
        return [
            a for a in alerts.list_alerts(con, include_acked=True) if a["rule"] == "auction_mismatch"
        ]


def _results(settings):
    with store.session(settings.db_path, wait=0) as con:
        return dict(
            con.execute(
                "SELECT result, count(*) FROM auction_xcheck GROUP BY result"
            ).fetchall()
        )


# --- wiring ------------------------------------------------------------------------------------


def test_auction_xcheck_is_registered_and_chained_after_fiscal(monkeypatch):
    monkeypatch.delenv("USFIN_DEMO", raising=False)
    registry = jobs.load_registry()
    assert "auction_xcheck" in registry
    assert fiscal.JOB.then == ("auction_xcheck",)
    assert registry.get("auction_xcheck").parser_map(load_settings()) == {}


def test_fiscal_run_chains_auction_xcheck(settings):
    _seed(settings)
    reg = jobs.Registry()
    reg.register(jobs.Job("fiscal_stub", ("k",), lambda ctx, key: [], {}, then=("auction_xcheck",)))
    reg.register(xc.JOB)
    result = jobs.run_job("fiscal_stub", settings=settings, registry=reg, clock=lambda: NOW)
    assert [(c.job, c.status) for c in result.chained] == [("auction_xcheck", "ok")]
    assert _results(settings) == {"matched": 5}


# --- comparison rules --------------------------------------------------------------------------


def test_matching_rows_raise_no_alert(settings):
    _seed(settings)
    result = _run(settings)
    assert result.status == "ok"
    assert _alerts(settings) == []
    # the announced bill has no results on either side yet: its offering amount agrees
    assert _results(settings) == {"matched": 5}


def test_rates_written_at_different_precision_still_match():
    # TreasuryDirect "5.3000", Fiscal Data "5.300000"; float noise is not a disagreement
    assert xc.compare_pair({"high_yield": 5.3}, {"high_yield": float("5.300000")}) == {}
    assert xc.compare_pair({"high_yield": 0.1 + 0.2}, {"high_yield": 0.3}) == {}
    assert xc.compare_pair({"high_yield": 5.3}, {"high_yield": 5.301}) == {
        "high_yield": {"fiscal": 5.301, "treasurydirect": 5.3}
    }


def test_a_yield_difference_raises_exactly_one_alert_per_pair_and_day(settings):
    td = _changed(AUCTIONS, "91282CRF0", "2026-10-07", high_yield=5.31)
    _seed(settings, td=td)
    _run(settings)
    _run(settings, clock=LATER_SAME_DAY)
    found = _alerts(settings)
    assert len(found) == 1
    alert = found[0]
    assert (alert["job"], alert["subject"]) == ("auction_xcheck", "91282CRF0@2026-10-07")
    assert alert["raised_on"] == date(2026, 10, 8)
    assert alert["detail"]["fields"] == {"high_yield": {"fiscal": 5.3, "treasurydirect": 5.31}}
    assert alert["detail"]["security_type"] == "Note"
    # still disagreeing the next day: one more alert, for that day
    _run(settings, clock=NEXT_DAY)
    assert sorted(a["raised_on"] for a in _alerts(settings)) == [
        date(2026, 10, 8),
        date(2026, 10, 9),
    ]


def test_an_offering_difference_and_a_rate_difference_on_one_pair_are_one_alert(settings):
    fd = _changed(AUCTIONS, "912797WY9", "2026-10-07", offering_amt="80000000000",
                  high_discount_rate=4.110)
    _seed(settings, fd=fd)
    _run(settings)
    found = _alerts(settings)
    assert len(found) == 1
    assert found[0]["subject"] == "912797WY9@2026-10-07"
    assert found[0]["detail"]["fields"] == {
        "offering_amt": {"fiscal": "80000000000.0000", "treasurydirect": "75000000000.0000"},
        "high_discount_rate": {"fiscal": 4.11, "treasurydirect": 4.105},
    }
    assert _results(settings) == {"matched": 4, "mismatch": 1}


def test_reopenings_of_one_cusip_are_compared_by_auction_date(settings):
    # the FRN reopened on 09-23: its margin differs from the 08-26 original; only 09-23 disagrees
    td = _changed(AUCTIONS, "91282CRD5", "2026-09-23", high_discount_margin=0.045)
    _seed(settings, td=td)
    _run(settings)
    assert [a["subject"] for a in _alerts(settings)] == ["91282CRD5@2026-09-23"]


def test_a_result_on_one_side_only_is_pending_not_a_mismatch(settings):
    # TreasuryDirect (14:00) has today's bill result; Fiscal Data has not published it yet
    td = _changed(AUCTIONS, "912797VW4", "2026-10-08", high_discount_rate=3.9,
                  high_investment_rate=3.96)
    _seed(settings, td=td)
    _run(settings)
    assert _alerts(settings) == []
    assert _results(settings) == {"matched": 4, "pending": 1}


def test_rows_on_one_side_only_are_counted_not_alerted(settings):
    extra_td = ("912797WZ6", "2026-10-07", "Bill", "6-Week", "90000000000", None, 4.0, 4.1, None)
    old_td = ("912795AB0", "1999-12-28", "Bill", "13-Week", "8000000000", None, 5.2, 5.3, None)
    _seed(settings, fd=AUCTIONS[:-1], td=[*AUCTIONS, extra_td, old_td])
    _run(settings)
    assert _alerts(settings) == []
    # the 1999 bill predates Fiscal Data's first auction: outside the overlap, not counted
    assert _results(settings) == {"matched": 4, "td_only": 2}
    _seed_fd_only = ("912797XX0", "2026-10-09", "Bill", "8-Week", "100000000000",
                     None, None, None, None)
    with store.session(settings.db_path, wait=0) as con:
        con.execute(
            "INSERT INTO fd_auctions (cusip, security_type, security_term, auction_date,"
            " offering_amt) VALUES (?, ?, ?, ?, ?)",
            [_seed_fd_only[0], _seed_fd_only[2], _seed_fd_only[3], _seed_fd_only[1],
             Decimal(_seed_fd_only[4])],
        )
    _run(settings)
    assert _results(settings) == {"matched": 4, "td_only": 2, "fd_only": 1}
    assert _alerts(settings) == []


def test_second_run_changes_nothing_and_counts_zero_rows(settings):
    td = _changed(AUCTIONS, "91282CRF0", "2026-10-07", high_yield=5.31)
    _seed(settings, td=td)
    first = _run(settings)
    with store.session(settings.db_path, wait=0) as con:
        before = sorted(con.execute("SELECT * FROM auction_xcheck").fetchall(), key=repr)
    second = _run(settings, clock=LATER_SAME_DAY)
    with store.session(settings.db_path, wait=0) as con:
        after = sorted(con.execute("SELECT * FROM auction_xcheck").fetchall(), key=repr)
    assert first.rows_upserted == 5 and second.rows_upserted == 0
    assert before == after


def test_a_fixed_disagreement_updates_its_row(settings):
    td = _changed(AUCTIONS, "91282CRF0", "2026-10-07", high_yield=5.31)
    _seed(settings, td=td)
    _run(settings)
    with store.session(settings.db_path, wait=0) as con:
        con.execute("UPDATE tsy_security SET high_yield = 5.3 WHERE cusip = '91282CRF0'")
    result = _run(settings, clock=NEXT_DAY)
    assert result.rows_upserted == 1
    assert _results(settings) == {"matched": 5}
    assert len(_alerts(settings)) == 1  # only the first day's


def test_no_fd_auctions_table_yet_is_an_ok_run_with_nothing_compared(settings):
    with store.session(settings.db_path, wait=0):
        pass
    result = _run(settings)
    assert result.status == "ok" and result.rows_upserted == 0
    assert _results(settings) == {}


def test_reparse_has_nothing_to_replay(settings):
    _seed(settings)
    with store.session(settings.db_path, wait=0) as con:
        result = jobs.reparse(con, xc.JOB, settings=settings, now=NOW)
    assert result.raw_ids == [] and result.rows == 0


def test_detail_is_json_serializable():
    detail = xc.mismatch_detail(
        {"cusip": "X", "auction_date": date(2026, 1, 2), "security_type": "Bill",
         "security_term": "4-Week"},
        {"offering_amt": {"fiscal": Decimal("1.5"), "treasurydirect": Decimal("2.0000")}},
    )
    assert json.loads(json.dumps(detail))["fields"]["offering_amt"] == {
        "fiscal": "1.5", "treasurydirect": "2.0000"
    }
