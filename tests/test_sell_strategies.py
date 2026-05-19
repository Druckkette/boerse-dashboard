import pandas as pd

from sell_strategies import (
    Position,
    strategie_notbremse_verlust,
    strategie_21ma_bruch,
    strategie_atr_basiert,
    verkaufs_empfehlung_gesamt,
)


def make_df(closes):
    return pd.DataFrame({
        "open": closes,
        "high": [c * 1.01 for c in closes],
        "low": [c * 0.99 for c in closes],
        "close": closes,
        "volume": [1000] * len(closes),
    })


def test_notbremse_triggered():
    p = Position("T", 100, "2026-01-01", 10)
    d = make_df([100, 95])
    sig = strategie_notbremse_verlust(p, d, "Unsicher")
    assert sig[0]["tranche_pct"] == 100


def test_21ma_gestaffelt_day1():
    p = Position("T", 90, "2026-01-01", 10)
    closes = list(range(100, 122)) + [110]
    d = make_df(closes)
    sigs = strategie_21ma_bruch(p, d, "gestaffelt")
    assert any(s["name"].startswith("Erster Schluss") for s in sigs)


def test_aggregation_killer_to_100():
    p = Position("T", 100, "2026-01-01", 10, realisierte_tranchen=[25])
    d = make_df([100, 92])
    res = verkaufs_empfehlung_gesamt(p, d, d, None, None, "Bullisch", "Neutral", ["notbremse_verlust"])
    assert res["gesamt_tranche"] == 100
    assert res["jetzt_zu_verkaufen"] == 75


def test_atr_basiert_contains_expected_fields_and_refs():
    p = Position("T", 100, "2026-01-01", 10)
    closes = [100] * 30 + [106, 112, 119]
    d = make_df(closes)
    sigs = strategie_atr_basiert(p, d, ziel_atr_multiplikator=1)
    assert sigs, "Expected ATR-based signal(s)"
    assert all(s["buch_verweis"] == "Kap. 6.4 ATR-basiert" for s in sigs)
