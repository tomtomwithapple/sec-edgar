"""`ticker_norm`: one spelling of a ticker for joining EDGAR to Nasdaq Trader (C10).

Uppercase and strip; `$` (Nasdaq's preferred marker) becomes `.P`; `.`, `-`, `/` and a space all
become `.`. So `BRK.B`, `BRK-B`, `BRK/B` → `BRK.B`; `ABR$D`, `ABR-PD` → `ABR.PD`; `AAC.U`,
`AAC-U` → `AAC.U`. Suffix letters are kept as written: EDGAR's units are `-UN` (`KCAC.UN`),
warrants `-WT`, rights `-RI` (company_tickers.json, 2026-10-08). Used by `edgar_company` (S19)
and the symbol join (S31).
"""

_SEPARATORS = str.maketrans({"-": ".", "/": ".", " ": "."})


def ticker_norm(symbol: str) -> str:
    return symbol.strip().upper().replace("$", ".P").translate(_SEPARATORS)
