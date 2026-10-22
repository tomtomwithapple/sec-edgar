"""README and `.env.example` document Alpaca market data (plan alpaca-market-data: A12, S13).

The docs are read as text: the keys, the three jobs with their schedule from
`config/schedule.yaml`, the backfill commands with their options, and the pages. A key or job
added to the code without a README line fails here.
"""

import re
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[1]
README = (REPO / "README.md").read_text()
ENV_EXAMPLE = (REPO / ".env.example").read_text()
SCHEDULE = yaml.safe_load((REPO / "config" / "schedule.yaml").read_text())["jobs"]
ALPACA_KEYS = ("APCA_API_KEY_ID", "APCA_API_SECRET_KEY")
ALPACA_JOBS = ("alpaca_assets", "alpaca_corp_actions", "alpaca_bars")


def env_keys(text: str) -> list[str]:
    return re.findall(r"^([A-Z][A-Z0-9_]*)=", text, flags=re.MULTILINE)


def table_row(first_cell: str) -> str:
    """The README table row whose first cell is `first_cell` (in backticks or not)."""
    for line in README.splitlines():
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if line.lstrip().startswith("|") and cells[0].strip("`") == first_cell:
            return line
    raise AssertionError(f"no README table row for {first_cell}")


@pytest.mark.parametrize("key", ALPACA_KEYS)
def test_env_example_lists_alpaca_keys_empty(key):
    assert key in env_keys(ENV_EXAMPLE)
    assert re.search(rf"^{key}=$", ENV_EXAMPLE, flags=re.MULTILINE), "no value in the example"


def test_every_env_example_key_has_a_readme_row():
    missing = [k for k in env_keys(ENV_EXAMPLE) if k not in README]
    assert not missing
    for key in ALPACA_KEYS:
        row = table_row(key)
        assert "Alpaca" in row
        assert "ConfigError" in row or "refuse" in row  # what happens when it is unset


@pytest.mark.parametrize("job", ALPACA_JOBS)
def test_schedule_table_has_each_alpaca_job_as_configured(job):
    entry = SCHEDULE[job]
    minute, hour = entry["cron"].split()[:2]
    row = table_row(job)
    assert f"{int(hour):02d}:{int(minute):02d}" in row
    assert entry["calendar"] in row


@pytest.mark.parametrize(
    "text",
    [
        "usfin alpaca backfill corp-actions",
        "usfin alpaca backfill bars",
        "--symbols",
        "--from",
        "usfin run alpaca_assets",
        "usfin run alpaca_corp_actions",
        "usfin reparse alpaca_bars",
        "caffeinate",
    ],
)
def test_readme_documents_alpaca_commands(text):
    assert text in README


@pytest.mark.parametrize(
    "text",
    [
        "/prices?symbol=",
        "/prices/universe",
        "matched",
        "?format=csv",
        "alpaca_bar_as_of",
        "alpaca_bar_adjusted",
        "alpaca_security_lookup",
        "SIVB",
    ],
)
def test_readme_documents_alpaca_pages_and_views(text):
    assert text in README


def test_readme_links_the_alpaca_plan():
    assert "docs/plans/alpaca-market-data.md" in README
