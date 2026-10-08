from datetime import date
from pathlib import Path

import pytest
from typer.testing import CliRunner

from usfin import calendars
from usfin.cli import app
from usfin.settings import ConfigError

REPO_CONFIG = Path(__file__).resolve().parents[1] / "config"


@pytest.fixture
def cals():
    return calendars.load_calendars(REPO_CONFIG)


def _verdicts(cals, d):
    return {name: cals.is_day(name, d) for name in ("federal", "bond", "nyse")}


def test_columbus_day_federal_and_bond_closed_nyse_open(cals):
    d = date(2026, 10, 12)
    assert _verdicts(cals, d) == {"federal": False, "bond": False, "nyse": True}
    assert cals.is_federal_day(d) is False
    assert cals.is_bond_day(d) is False
    assert cals.is_nyse_day(d) is True


def test_good_friday_federal_open_bond_and_nyse_closed(cals):
    d = date(2026, 4, 3)
    assert _verdicts(cals, d) == {"federal": True, "bond": False, "nyse": False}


def test_veterans_day_federal_closed_nyse_open(cals):
    d = date(2026, 11, 11)
    assert cals.is_federal_day(d) is False
    assert cals.is_bond_day(d) is False
    assert cals.is_nyse_day(d) is True


def test_observed_holiday_closes_federal_day(cals):
    # Independence Day 2026 is a Saturday; observed Friday 2026-07-03.
    assert _verdicts(cals, date(2026, 7, 3)) == {"federal": False, "bond": False, "nyse": False}


def test_plain_weekday_is_open_everywhere(cals):
    assert _verdicts(cals, date(2026, 10, 8)) == {"federal": True, "bond": True, "nyse": True}


@pytest.mark.parametrize("d", [date(2026, 10, 10), date(2026, 10, 11)])
def test_weekend_is_closed_everywhere(cals, d):
    assert _verdicts(cals, d) == {"federal": False, "bond": False, "nyse": False}
    assert cals.is_day("any", d) is True


@pytest.mark.parametrize("year", [2024, 2025, 2026, 2027, 2030])
def test_good_friday_closes_bond_every_year(cals, year):
    good_fridays = {2024: date(2024, 3, 29), 2025: date(2025, 4, 18), 2026: date(2026, 4, 3),
                    2027: date(2027, 3, 26), 2030: date(2030, 4, 19)}
    d = good_fridays[year]
    assert cals.is_federal_day(d) is True
    assert cals.is_bond_day(d) is False


def test_unknown_calendar_is_rejected(cals):
    with pytest.raises(ValueError, match="calendar"):
        cals.is_day("lse", date(2026, 10, 8))


def test_overrides_add_a_closure_and_an_opening(tmp_path):
    (tmp_path / "calendars.yaml").write_text(
        "bond:\n"
        "  closed: [2026-10-08]\n"
        "nyse:\n"
        "  open: [2026-04-03]\n"
        "federal:\n"
        "  closed: ['2026-10-09']\n",
        "utf-8",
    )
    cals = calendars.load_calendars(tmp_path)
    assert cals.is_bond_day(date(2026, 10, 8)) is False  # SIFMA one-off closure
    assert cals.is_federal_day(date(2026, 10, 8)) is True  # other calendars untouched
    assert cals.is_nyse_day(date(2026, 4, 3)) is True  # one-off opening
    # bond is federal + Good Friday: a federal closure closes bond too
    assert cals.is_federal_day(date(2026, 10, 9)) is False
    assert cals.is_bond_day(date(2026, 10, 9)) is False


def test_bond_override_can_open_a_day_federal_closes(tmp_path):
    (tmp_path / "calendars.yaml").write_text("bond:\n  open: [2026-10-12]\n", "utf-8")
    cals = calendars.load_calendars(tmp_path)
    assert cals.is_bond_day(date(2026, 10, 12)) is True
    assert cals.is_federal_day(date(2026, 10, 12)) is False


def test_missing_or_empty_overrides_file_means_no_overrides(tmp_path):
    assert calendars.load_calendars(tmp_path).is_nyse_day(date(2026, 4, 3)) is False
    (tmp_path / "calendars.yaml").write_text("# nothing\n", "utf-8")
    assert calendars.load_calendars(tmp_path).is_nyse_day(date(2026, 4, 3)) is False


@pytest.mark.parametrize(
    "text",
    [
        "lse:\n  closed: [2026-10-08]\n",  # unknown calendar
        "nyse:\n  shut: [2026-10-08]\n",  # unknown key
        "nyse:\n  closed: [not-a-date]\n",
        "nyse:\n  closed: [2026-10-08]\n  open: [2026-10-08]\n",  # both
        "- 2026-10-08\n",  # not a mapping
    ],
)
def test_bad_overrides_are_config_errors(tmp_path, text):
    (tmp_path / "calendars.yaml").write_text(text, "utf-8")
    with pytest.raises(ConfigError):
        calendars.load_calendars(tmp_path)


def test_prev_and_next_day_skip_weekends_and_holidays(cals):
    # Fri 2026-10-09, (Sat, Sun, Mon Columbus Day), Tue 2026-10-13
    assert cals.next_day("federal", date(2026, 10, 9)) == date(2026, 10, 13)
    assert cals.prev_day("federal", date(2026, 10, 13)) == date(2026, 10, 9)
    assert cals.next_day("nyse", date(2026, 10, 9)) == date(2026, 10, 12)
    assert cals.prev_day("bond", date(2026, 4, 6)) == date(2026, 4, 2)  # skips Good Friday
    assert cals.prev_day("federal", date(2026, 4, 6)) == date(2026, 4, 3)
    # strictly before / after, even when d itself is a calendar day
    assert cals.next_day("federal", date(2026, 10, 8)) == date(2026, 10, 9)
    assert cals.prev_day("federal", date(2026, 10, 8)) == date(2026, 10, 7)
    assert cals.next_day("any", date(2026, 10, 10)) == date(2026, 10, 11)


def test_days_back_counts_calendar_days_before_d(cals):
    assert cals.days_back("federal", date(2026, 10, 13), 0) == date(2026, 10, 13)
    assert cals.days_back("federal", date(2026, 10, 13), 1) == date(2026, 10, 9)
    assert cals.days_back("federal", date(2026, 10, 13), 5) == date(2026, 10, 5)
    assert cals.days_back("nyse", date(2026, 10, 13), 5) == date(2026, 10, 6)
    with pytest.raises(ValueError):
        cals.days_back("federal", date(2026, 10, 13), -1)


def test_cli_calendar_prints_three_verdicts(monkeypatch):
    monkeypatch.setenv("USFIN_CONFIG", str(REPO_CONFIG))
    result = CliRunner().invoke(app, ["calendar", "2026-10-12"])
    assert result.exit_code == 0, result.output
    lines = result.output.strip().splitlines()
    assert lines[0].startswith("2026-10-12 Mon")
    assert "Columbus Day" in lines[0]
    assert lines[1:] == ["federal: closed", "bond: closed", "nyse: open"]


def test_cli_calendar_rejects_a_bad_date(monkeypatch):
    monkeypatch.setenv("USFIN_CONFIG", str(REPO_CONFIG))
    result = CliRunner().invoke(app, ["calendar", "2026-13-01"])
    assert result.exit_code != 0
