import pandas as pd

from sell_strategies import (
    Position,
    strategie_21ma_bruch,
    strategie_atr_basiert,
    strategie_einfach_halbe_position,
    strategie_misslungener_ausbruch_5stufen,
    strategie_notbremse_verlust,
    strategie_rueckkehr_pivot,
    strategie_ma_bruch_defensiv,
    strategie_rs_linie,
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


def test_einfach_halbe_position_custom_erste_gewinnmitnahme():
    p = Position("T", 100, "2026-01-01", 10)
    d = make_df([100, 118])
    sigs_default = strategie_einfach_halbe_position(p, d)
    sigs_custom = strategie_einfach_halbe_position(p, d, erste_haelfte_gewinn_pct=17.5)
    assert not sigs_default
    assert any(s["name"] == "Erste Hälfte bei 17.5%+" for s in sigs_custom)


def test_strategie21_skip_tag0_when_below_notbremse():
    p = Position("T", 100, "2026-01-01", 10, tief_tag_1=97, tief_tag_0=92)
    d = pd.DataFrame({
        "open": [101, 96],
        "high": [102, 97],
        "low": [99, 91],
        "close": [100, 91.5],
        "volume": [1000, 1000],
    })
    sigs = strategie_misslungener_ausbruch_5stufen(p, d)
    names = [s["name"] for s in sigs]
    assert "Intraday unter Tief Tag 0" not in names
    assert "Schluss unter Tief Tag 0" not in names


def test_strategie21_pivot_return_extension():
    p = Position("T", 100, "2026-01-01", 10, pivot=100, realisierte_tranchen=[33])
    idx = pd.date_range("2026-01-01", periods=6, freq="D")
    d = pd.DataFrame({
        "open": [101, 98, 101, 104, 102, 99],
        "high": [102, 100, 104, 105, 103, 100],
        "low": [99, 97, 100, 103, 100, 98],
        "close": [101, 99, 101, 104, 102, 100],
        "volume": [1000] * 6,
    }, index=idx)
    sigs = strategie_misslungener_ausbruch_5stufen(p, d)
    assert any(s["name"] == "Zweite Rückkehr zum Pivot" for s in sigs)


def test_rs_linie_uses_weekly_between_20_and_80_pct():
    p = Position("T", 100, "2026-01-01", 10)
    d = make_df([100 + i for i in range(60)] + [130])  # +30%
    spy = make_df([100] * len(d))
    w = make_df(([100] * 10) + ([140] * 15) + [90])
    w_spy = make_df([100] * len(w))
    sigs = strategie_rs_linie(p, d, spy, w, w_spy)
    assert any("(woche)" in s["name"] for s in sigs)


def test_rs_linie_uses_monthly_above_80_pct():
    p = Position("T", 100, "2026-01-01", 10)
    idx = pd.date_range("2024-01-31", periods=30, freq="ME")
    closes = ([300] * 29) + [190]
    d = pd.DataFrame({"open": closes, "high": closes, "low": closes, "close": closes, "volume": [1000] * 30}, index=idx)
    spy = pd.DataFrame({"open": [100] * 30, "high": [100] * 30, "low": [100] * 30, "close": [100] * 30, "volume": [1000] * 30}, index=idx)
    w = make_df([100] * 30)
    sigs = strategie_rs_linie(p, d, spy, w, w)
    assert any("(monat)" in s["name"] for s in sigs)


def test_ma_bruch_defensiv_adds_downward_ma200_confirmation_signal():
    p = Position("T", 100, "2026-01-01", 10)
    daily = pd.DataFrame({
        "open": [300 - i for i in range(240)],
        "high": [301 - i for i in range(240)],
        "low": [299 - i for i in range(240)],
        "close": [300 - i for i in range(240)],
        "volume": [1000] * 239 + [1700],
    })
    weekly = pd.DataFrame({
        "open": [120 - i for i in range(20)],
        "high": [121 - i for i in range(20)],
        "low": [119 - i for i in range(20)],
        "close": [120 - i for i in range(20)],
        "volume": [1000] * 20,
    })

    sigs = strategie_ma_bruch_defensiv(p, daily, weekly)

    assert any(s["name"] == "200-MA dreht abwärts" and s["tranche_pct"] == 0 for s in sigs)


def test_ma_bruch_defensiv_uses_detailed_chapter_references():
    p = Position("T", 100, "2026-01-01", 10)
    daily = pd.DataFrame({
        "open": [300 - i for i in range(240)],
        "high": [301 - i for i in range(240)],
        "low": [299 - i for i in range(240)],
        "close": [300 - i for i in range(240)],
        "volume": [1000] * 239 + [1700],
    })
    weekly = pd.DataFrame({
        "open": [120 - i for i in range(20)],
        "high": [121 - i for i in range(20)],
        "low": [119 - i for i in range(20)],
        "close": [120 - i for i in range(20)],
        "volume": [1000] * 20,
    })

    sigs = strategie_ma_bruch_defensiv(p, daily, weekly)
    refs = {s["buch_verweis"] for s in sigs}

    assert "Kap. 6.3 50-MA" in refs
    assert "Kap. 6.3 10-Wochen-Linie" in refs
    assert "Kap. 6.3 200-MA" in refs


def test_strategie14_rueckkehr_pivot_volume_reasoning_and_refs():
    p = Position("T", 100, "2026-01-01", 10, pivot=100, tief_tag_1=99, tief_tag_0=98)
    d = pd.DataFrame({
        "open": [102] * 50 + [98],
        "high": [103] * 50 + [99],
        "low": [101] * 50 + [97],
        "close": [102] * 41 + [99] * 9 + [98.5],
        "volume": [1000] * 50 + [1800],
    })
    sigs = [s for s in strategie_rueckkehr_pivot(p, d) if s["name"] == "Schluss unter Tief Ausbruchstag"]
    assert sigs
    assert sigs[0]["tranche_pct"] == 50
    assert sigs[0]["begruendung"] == "Mit erhöhtem Volumen"
    assert sigs[0]["buch_verweis"] == "Kap. 6.3 Rückkehr zum Ausbruchspunkt"


def test_strategie14_rueckkehr_pivot_zeitkomponente_10_tage():
    p = Position("T", 100, "2026-01-01", 10, pivot=100)
    d = pd.DataFrame({
        "open": [101] * 20,
        "high": [102] * 20,
        "low": [97] * 20,
        "close": [101] * 10 + [99] * 10,
        "volume": [1000] * 20,
    })
    sigs = strategie_rueckkehr_pivot(p, d)
    pvt = [s for s in sigs if "Tage unter Pivot" in s["name"]]
    assert pvt
    assert pvt[0]["tranche_pct"] == 50
    assert pvt[0]["begruendung"] == "Rückkehr über Ausbruchspunkt nicht gelungen"
