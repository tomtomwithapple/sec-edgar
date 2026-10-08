"""Concept map and the XBRL cross-section screen (one concept × one frame period, from `xbrl_fact`).

`config/concepts.yaml` is `{version: int, concepts: {name: {taxonomy?, unit?, tags: [tag, …]}}}`
(taxonomy default `us-gaap`, unit default `USD`): companies tag the same idea differently (and
change tags over time: Apple's revenue is `SalesRevenueNet` to 2017, then
`RevenueFromContractWithCustomerExcludingAssessedTax`), so a concept lists candidate tags in
order of preference. Bump `version` when a list changes, since screens move with it.

`screen(con, concept, period)` picks, per company, the **first tag in the list** the company
reports for that period and returns which tag it used. A frame period (`CY2025Q2`) maps to
`xbrl_fact` rows by the **`frame` column**, not by period dates: rows with `taxonomy` and `unit`
of the concept, `tag` in its list and `frame = period` exactly. `frame` is SEC's own assignment
(the same one the frames API serves): a duration of about a quarter (46–136 days seen for
CY2025Q2) or a year (320–401 days for CY2025) is fitted to the nearest calendar period, and the
`I` suffix marks an instant period — which SEC also gives to short durations (5–39 days, 1 169
facts in CY2025Q2I), so `is_instant` is not consulted. SEC hangs a frame on one filing per
company and period, the latest (Apple's April–June 2025 revenue carries CY2025Q2 on the 10-Q of
2026-07-31, which repeats it, not the 10-Q of 2025-08-01), so the screen shows the latest
reported value with its `accn` and `filed`; a stale duplicate (a fact dropped from a later
document keeps its old frame) loses to the newest `filed`. This is a current-view screen: for a
point-in-time view, `frame` cannot be used (it moves), and `xbrl_as_of` with period dates is the
tool. On the full load (122.7 M facts) the screen is one scan of `frame`/`tag` without an index
(S28: 1.6–1.8 s on a process's first query, 0.37–0.49 s warm).
"""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import duckdb
import yaml

from usfin.settings import ConfigError
from usfin.sources.edgar.frames import check_period

TAXONOMIES = ("us-gaap", "dei")
_KEYS = {"taxonomy", "unit", "tags"}


@dataclass(frozen=True)
class Concept:
    name: str
    taxonomy: str
    unit: str
    tags: tuple[str, ...]


@dataclass(frozen=True)
class ConceptMap:
    version: int
    concepts: Mapping[str, Concept]

    def __getitem__(self, name: str) -> Concept:
        try:
            return self.concepts[name]
        except KeyError:
            known = ", ".join(sorted(self.concepts))
            raise ConfigError(f"unknown concept {name!r} (concepts.yaml has {known})") from None


def load_concepts(config_dir: Path | str) -> ConceptMap:
    """Read and check `<config_dir>/concepts.yaml`."""
    path = Path(config_dir) / "concepts.yaml"
    data = yaml.safe_load(path.read_text("utf-8")) if path.is_file() else None
    if not isinstance(data, Mapping):
        raise ConfigError(f"{path}: expected `version` and `concepts`")
    version = data.get("version")
    if not isinstance(version, int) or isinstance(version, bool) or version < 1:
        raise ConfigError(f"{path}: `version` must be a whole number >= 1, got {version!r}")
    raw_concepts = data.get("concepts")
    if not isinstance(raw_concepts, Mapping) or not raw_concepts:
        raise ConfigError(f"{path}: `concepts` must map names to {{tags: [...]}}")
    out = {}
    for name, spec in raw_concepts.items():
        where = f"{path}: concept {name!r}"
        if not isinstance(spec, Mapping):
            raise ConfigError(f"{where}: expected {{taxonomy?, unit?, tags}}")
        extra = set(spec) - _KEYS
        if extra:
            raise ConfigError(f"{where}: unknown keys {sorted(extra)}")
        taxonomy = spec.get("taxonomy", "us-gaap")
        if taxonomy not in TAXONOMIES:
            raise ConfigError(f"{where}: taxonomy {taxonomy!r} is not one of {TAXONOMIES}")
        tags = spec.get("tags")
        if not isinstance(tags, list) or not tags or not all(isinstance(t, str) for t in tags):
            raise ConfigError(f"{where}: `tags` must be a non-empty list of tag names")
        twice = sorted({t for t in tags if tags.count(t) > 1})
        if twice:
            raise ConfigError(f"{where}: tags listed twice: {twice}")
        unit = spec.get("unit", "USD")
        if not isinstance(unit, str) or not unit:
            raise ConfigError(f"{where}: `unit` must be a unit name such as USD")
        out[str(name)] = Concept(str(name), taxonomy, unit, tuple(tags))
    return ConceptMap(version, out)


@dataclass(frozen=True)
class ScreenRow:
    cik: int
    name: str | None
    """`edgar_company.title` when the CIK has a ticker (S19), else None."""
    tag: str
    value: float
    period_start: date
    period_end: date
    accn: str
    form: str
    filed: date


def screen(con: duckdb.DuckDBPyConnection, concept: Concept, period: str) -> list[ScreenRow]:
    """One row per company reporting `concept` for frame `period`, largest value first."""
    check_period(period)
    rows = con.execute(
        """
        WITH pref AS (
            SELECT tag, pos FROM (SELECT unnest(?) AS tag, generate_subscripts(?, 1) AS pos)
        ),
        hits AS (
            SELECT f.cik, f.tag, f.value, f.period_start, f.period_end, f.accn, f.form, f.filed
            FROM xbrl_fact f JOIN pref p ON f.tag = p.tag
            WHERE f.frame = ? AND f.taxonomy = ? AND f.unit = ?
            QUALIFY row_number() OVER (
                PARTITION BY f.cik ORDER BY p.pos, f.filed DESC, f.accn DESC) = 1
        ),
        names AS (SELECT cik, min(title) AS title FROM edgar_company GROUP BY cik)
        SELECT h.cik, n.title, h.tag, h.value, h.period_start, h.period_end, h.accn, h.form,
               h.filed
        FROM hits h LEFT JOIN names n USING (cik)
        ORDER BY h.value DESC, h.cik
        """,
        [list(concept.tags), list(concept.tags), period, concept.taxonomy, concept.unit],
    ).fetchall()
    return [ScreenRow(*r) for r in rows]
