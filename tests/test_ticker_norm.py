"""`ticker_norm`: one spelling for class, preferred and unit tickers across EDGAR and Nasdaq Trader
(C10; plan How > Nasdaq Trader). Shared by `edgar_company` (S19) and the symbol join (S31)."""

import pytest

from usfin.ticker import ticker_norm


@pytest.mark.parametrize(
    ("symbol", "norm"),
    [
        ("BRK.B", "BRK.B"),
        ("BRK-B", "BRK.B"),
        ("BRK/B", "BRK.B"),
        ("BRK B", "BRK.B"),
        ("ABR$D", "ABR.PD"),
        ("ABR-PD", "ABR.PD"),
        ("AAC.U", "AAC.U"),
        ("AAC-U", "AAC.U"),
        ("AAPL", "AAPL"),
        (" brk-b ", "BRK.B"),
    ],
)
def test_class_preferred_and_unit_spellings_normalize_alike(symbol, norm):
    assert ticker_norm(symbol) == norm


def test_edgar_suffixes_keep_their_letters():
    # EDGAR's company_tickers.json writes units -UN, warrants -WT, rights -RI (2026-10-08)
    assert ticker_norm("KCAC-UN") == "KCAC.UN"
    assert ticker_norm("SLND-WT") == "SLND.WT"
    assert ticker_norm("NONE.") == "NONE."
