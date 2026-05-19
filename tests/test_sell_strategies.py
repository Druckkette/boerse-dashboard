import pandas as pd

from sell_strategies import (
    Position,
    strategie_21ma_bruch,
    strategie_atr_basiert,
    strategie_notbremse_verlust,
    strategie_einfache_verluststufen,
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


def test_atr_basiert_triggers_all_core_signals():
    p = Position("T", 100, "2026-01-01", 10)
    # Letzter Close 130 => +30% Gewinn; ATR ~5%; EMA21 deutlich tiefer
    closes = [80] * 21 + [130]
    d = pd.DataFrame({
        "open": closes,
        "high": [c + 2.5 for c in closes],
        "low": [c - 2.5 for c in closes],
        "close": closes,
        "volume": [1000] * len(closes),
    })

    sigs = strategie_atr_basiert(p, d, ziel_atr_multiplikator=3)

    names = [s["name"] for s in sigs]
    assert any("ATR Gewinn erreicht" in n for n in names)
    assert any("ATR über 21-EMA" in n for n in names)


def test_atr_basiert_stop_signal_uses_1_5_atr():
    p = Position("T", 100, "2026-01-01", 10)
    # ATR ~2%, Stop ~97%, Schluss darunter => Stop-Signal
    closes = [100] * 21 + [96]
    d = pd.DataFrame({
        "open": closes,
        "high": [c + 1 for c in closes],
        "low": [c - 1 for c in closes],
        "close": closes,
        "volume": [1000] * len(closes),
    })

    sigs = strategie_atr_basiert(p, d, ziel_atr_multiplikator=3)

    stop = [s for s in sigs if s["name"] == "Stopp bei -1.5 ATR"]
    assert stop
    assert stop[0]["tranche_pct"] == 100
    assert stop[0]["trigger_typ"] == "intraday"


def test_atr_basiert_custom_ema_extension_thresholds():
    p = Position("T", 100, "2026-01-01", 10)
    closes = [100] * 21 + [106]
    d = pd.DataFrame({
        "open": closes,
        "high": [c + 1 for c in closes],
        "low": [c - 1 for c in closes],
        "close": closes,
        "volume": [1000] * len(closes),
    })

    # Mit Standard 3 ATR kein Überdehnungs-Signal, mit 2 ATR schon
    std = strategie_atr_basiert(p, d, ziel_atr_multiplikator=99)
    custom = strategie_atr_basiert(p, d, ziel_atr_multiplikator=99, ueberdehnung_atr_start=2, ueberdehnung_atr_stark=3)

    assert not any("ATR über 21-EMA" in s["name"] for s in std)
    assert any("ATR über 21-EMA" in s["name"] for s in custom)


def test_einfache_verluststufen_default_entspricht_kapitel():
    p = Position("T", 100, "2026-01-01", 10)
    d = make_df([100, 96])
    sigs = strategie_einfache_verluststufen(p, d)
    assert sigs
    assert sigs[0]["name"] == "Verlust ≥ 3%"
    assert sigs[0]["tranche_pct"] == 33


def test_einfache_verluststufen_custom_schwellen_werden_genutzt():
    p = Position("T", 100, "2026-01-01", 10)
    d = make_df([100, 95])
    sigs = strategie_einfache_verluststufen(p, d, verlust_stufe_1=2, verlust_stufe_2=4, verlust_stufe_3=6)
    assert sigs
    assert sigs[0]["name"] == "Verlust ≥ 4%"
    assert sigs[0]["naechste_marke"] == 94.0
