"""`ticker_norm`: one spelling of a ticker for joining EDGAR to Nasdaq Trader (C10).

Uppercase and strip; `$` (Nasdaq's preferred marker) becomes `.P`; `.`, `-`, `/` and a space all
become `.`. So `BRK.B`, `BRK-B`, `BRK/B` → `BRK.B`; `ABR$D`, `ABR-PD` → `ABR.PD`. Then EDGAR's
unit, warrant and right suffixes take Nasdaq's ACT spelling (S31): `.UN` → `.U`, `.WT` → `.W`,
`.RI` → `.R` at the end, so `AAC-UN`, `AAC.U`, `AAC-U` → `AAC.U`, `AAC-WT` → `AAC.W`,
`AIIA-RI` → `AIIA.R` (company_tickers.json and otherlisted.txt, 2026-10-08: 131 Nasdaq Trader
symbols matched by it, no two EDGAR tickers or open symbols sharing a norm). Other suffixes are
kept (`NE-WTA` → `NE.WTA`, `UWMC-RW` → `UWMC.RW`). Used by `edgar_company` (S19) and the symbol
join (S31); the SQL macro `ticker_norm` (migration 019) is the same function and must stay so.
"""

_SEPARATORS = str.maketrans({"-": ".", "/": ".", " ": "."})
_EDGAR_SUFFIXES = {".UN": ".U", ".WT": ".W", ".RI": ".R"}
"""EDGAR's unit / warrant / right suffix → Nasdaq Trader's (ACT Symbol)."""


def ticker_norm(symbol: str) -> str:
    norm = symbol.strip().upper().replace("$", ".P").translate(_SEPARATORS)
    for edgar, nasdaq in _EDGAR_SUFFIXES.items():
        if norm.endswith(edgar):
            return norm[: -len(edgar)] + nasdaq
    return norm
