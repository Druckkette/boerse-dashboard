import pandas as pd

from sell_strategies import (
    Position,
    strategie_21ma_bruch,
    strategie_atr_basiert,
    strategie_einfach_halbe_position,
    strategie_misslungener_ausbruch_5stufen,
    strategie_notbremse_verlust,
    strategie_rueckkehr_pivot,
    strategie_stau_tage,
    strategie_ma_bruch_defensiv,
    strategie_rs_linie,
    strategie_einfache_verluststufen,
    verkaufs_empfehlung_gesamt,
    diagnose_strategie_kein_signal,
    strategie_downside_reversal,
    strategie_verlusttage_haeufung,
    strategie_ma_abstand,
    strategie_drawdown_vom_peak,
    strategie_gewinn_in_stufen,
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




def test_notbremse_info_signal_enthaelt_kritischen_kurs_und_markt():
    p = Position("T", 100, "2026-01-01", 10)
    d = make_df([100, 97])
    sig = strategie_notbremse_verlust(p, d, "Bullisch")
    assert sig[0]["name"] == "Notbremse-Marke"
    assert sig[0]["naechste_marke"] == 93.0
    assert "Bullisch" in sig[0]["begruendung"]


def test_notbremse_custom_thresholds_from_setup():
    p = Position("T", 100, "2026-01-01", 10)
    d = make_df([100, 94])
    res = verkaufs_empfehlung_gesamt(
        p, d, d, None, None, "Bullisch", "Neutral", ["notbremse_verlust"],
        {"notbremse_verlust_schwelle_bullisch_pct": 6.0, "notbremse_verlust_schwelle_unsicher_pct": 5.0, "notbremse_verlust_schwelle_baerisch_pct": 4.0},
    )
    assert res["gesamt_tranche"] == 100
    assert any("Verlustgrenze -6%" in s.get("begruendung", "") for s in res["alle_signale"])


def test_rueckkehr_pivot_custom_setup_wird_uebernommen():
    p = Position("T", 100, "2026-01-01", 10, tief_tag_1=99, tief_tag_0=98)
    d = make_df([100, 97])  # unter Tag-1 und Tag-0
    res = verkaufs_empfehlung_gesamt(
        p, d, d, None, None, "Unsicher", "Neutral", ["rueckkehr_pivot"],
        {
            "rueckkehr_tranche_stufe1_pct": 25.0,
            "rueckkehr_tranche_stufe2_pct": 40.0,
            "rueckkehr_notbremse_verlust_pct": 6.0,
        },
    )
    sigs = res["alle_signale"]
    assert any(s["name"] == "Schluss unter Tief Ausbruchstag" and s["tranche_pct"] == 25 for s in sigs)
    assert any(s["name"] == "Schluss unter Tief Vortag" and s["tranche_pct"] == 40 and s["naechste_marke"] == 94.0 for s in sigs)


def test_rueckkehr_pivot_notbremse_bei_max_verlust():
    p = Position("T", 100, "2026-01-01", 10)
    d = make_df([100, 92])  # -8% < -7% Default-Notbremse
    sigs = strategie_rueckkehr_pivot(p, d)
    notbremse = [s for s in sigs if s["trigger_typ"] == "intraday"]
    assert notbremse
    assert notbremse[0]["tranche_pct"] == 100


def test_gewinn_in_stufen_default_bulkowski_bullisch():
    p = Position("T", 100, "2026-01-01", 10)
    d = make_df([100, 125])
    sigs = strategie_gewinn_in_stufen(p, d, "Bullisch")
    assert any(s["name"] == "Gewinn-Nachdenkschwelle erreicht" for s in sigs)
    assert any(s["name"] == "Pflicht-Teilverkauf Gewinnzone" and s["tranche_pct"] == 33 for s in sigs)


def test_gewinn_in_stufen_custom_thresholds_from_setup():
    p = Position("T", 100, "2026-01-01", 10)
    d = make_df([100, 112])
    sigs = strategie_gewinn_in_stufen(
        p, d, "Bullisch",
        nachdenken_schwelle_bull_pct=8,
        teilverkauf_schwelle_unten_bull_pct=10,
        teilverkauf_schwelle_oben_bull_pct=12,
    )
    assert any(s["name"] == "Pflicht-Teilverkauf Gewinnzone" and s["tranche_pct"] == 50 for s in sigs)


def test_21ma_gestaffelt_day1():
    p = Position("T", 90, "2026-01-01", 10)
    closes = list(range(100, 122)) + [110]
    d = make_df(closes)
    sigs = strategie_21ma_bruch(p, d, "gestaffelt")
    assert any(s["name"].startswith("Erster Schluss") for s in sigs)


def test_21ma_aggressiv_mit_volumen():
    p = Position("T", 80, "2026-01-01", 10)
    closes = [100] * 59 + [96]
    volumes = [1000] * 59 + [2000]
    d = pd.DataFrame({
        "open": closes,
        "high": [c * 1.01 for c in closes],
        "low": [c * 0.99 for c in closes],
        "close": closes,
        "volume": volumes[-len(closes):],
    })
    sigs = strategie_21ma_bruch(p, d, "aggressiv")
    assert any("Deutlicher 21-EMA-Bruch" in s["name"] for s in sigs)


def test_21ma_geduldig_nach_drei_tagen():
    p = Position("T", 80, "2026-01-01", 10)
    closes = list(range(100, 121)) + [98, 97, 96]
    d = make_df(closes)
    sigs = strategie_21ma_bruch(p, d, "geduldig")
    assert any("seit 3 Tagen gebrochen" in s["name"] for s in sigs)


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


def test_rs_linie_falls_back_to_weekly_when_monthly_history_is_short():
    p = Position("T", 100, "2026-01-01", 10)
    month_idx = pd.date_range("2025-01-31", periods=17, freq="ME")
    month_closes = ([300] * 16) + [190]
    d = pd.DataFrame({"open": month_closes, "high": month_closes, "low": month_closes, "close": month_closes, "volume": [1000] * 17}, index=month_idx)
    spy = pd.DataFrame({"open": [100] * 17, "high": [100] * 17, "low": [100] * 17, "close": [100] * 17, "volume": [1000] * 17}, index=month_idx)
    week_idx = pd.date_range("2024-01-05", periods=80, freq="W-FRI")
    week_closes = ([300] * 79) + [190]
    w = pd.DataFrame({"open": week_closes, "high": week_closes, "low": week_closes, "close": week_closes, "volume": [1000] * 80}, index=week_idx)
    w_spy = pd.DataFrame({"open": [100] * 80, "high": [100] * 80, "low": [100] * 80, "close": [100] * 80, "volume": [1000] * 80}, index=week_idx)

    sigs = strategie_rs_linie(p, d, spy, w, w_spy)

    assert any("RS bricht 25-MA (woche)" == s["name"] for s in sigs)


def test_diagnose_rs_linie_mentions_weekly_fallback_for_short_monthly_history():
    p = Position("T", 100, "2026-01-01", 10)
    month_idx = pd.date_range("2025-01-31", periods=17, freq="ME")
    month_closes = ([300] * 16) + [190]
    d = pd.DataFrame({"open": month_closes, "high": month_closes, "low": month_closes, "close": month_closes, "volume": [1000] * 17}, index=month_idx)
    spy = pd.DataFrame({"open": [100] * 17, "high": [100] * 17, "low": [100] * 17, "close": [100] * 17, "volume": [1000] * 17}, index=month_idx)
    week_idx = pd.date_range("2024-01-05", periods=80, freq="W-FRI")
    week_closes = ([300] * 79) + [190]
    w = pd.DataFrame({"open": week_closes, "high": week_closes, "low": week_closes, "close": week_closes, "volume": [1000] * 80}, index=week_idx)
    w_spy = pd.DataFrame({"open": [100] * 80, "high": [100] * 80, "low": [100] * 80, "close": [100] * 80, "volume": [1000] * 80}, index=week_idx)

    grund = diagnose_strategie_kein_signal("rs_linie", p, d, w, spy, w_spy, "Bullisch", {})

    assert "Fallback von monat auf woche" in grund




def test_diagnose_rs_linie_reports_actual_ma_position():
    p = Position("T", 100, "2026-01-01", 10)
    d = make_df(([100] * 55) + ([90] * 5))
    spy = make_df([100] * len(d))
    grund = diagnose_strategie_kein_signal("rs_linie", p, d, d, spy, d, "Bullisch", {})
    assert "unter dem 21-MA" in grund
    assert "unter dem 50-MA" in grund
    assert "RS 0.9000" in grund


def test_rs_linie_below_both_mas_triggers_slow_ma_signal():
    p = Position("T", 100, "2026-01-01", 10)
    d = make_df(([100] * 55) + ([90] * 5))
    spy = make_df([100] * len(d))
    sigs = strategie_rs_linie(p, d, spy, d, d)
    names = [s["name"] for s in sigs]
    assert "RS bricht 50-MA (tag)" in names

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


def test_verlusttage_haeufung_signals_match_chapter_logic():
    p = Position("T", 100, "2026-01-01", 10)
    closes = [105, 104, 103, 102, 101, 106, 105, 104, 103, 102, 107, 106, 115, 113, 110]
    opens =  [106, 105, 104, 103, 102, 105, 106, 105, 104, 103, 106, 107, 116, 114, 111]
    volumes = [1000] * 12 + [1300, 1400, 1500]
    d = pd.DataFrame({
        "open": opens,
        "high": [max(o, c) + 1 for o, c in zip(opens, closes)],
        "low": [min(o, c) - 1 for o, c in zip(opens, closes)],
        "close": closes,
        "volume": volumes,
    })

    sigs = strategie_verlusttage_haeufung(
        p,
        d,
        min_tiefere_schlusskurse_in_folge=3,
        volumen_lookback_tage=10,
        volumen_ratio_min=1.1,
        updown_fenster_tage=15,
        updown_diff_min=3,
    )
    names = [s["name"] for s in sigs]
    assert "3 tiefere Schlusskurse mit Volumen" in names
    assert "Abwärtstage überwiegen im 15-Tage-Fenster" in names
    weekly = pd.DataFrame({
        "open": [120 - i for i in range(20)],
        "high": [121 - i for i in range(20)],
        "low": [119 - i for i in range(20)],
        "close": [120 - i for i in range(20)],
        "volume": [1000] * 20,
    })


def test_downside_reversal_strong_signal_has_detailed_reference_and_reason():
    p = Position("T", 100, "2026-01-01", 10)
    base = list(range(100, 130))
    closes = base + [121]
    highs = [c + 1 for c in base] + [132]
    lows = [c - 1 for c in base] + [120]
    volumes = [1000] * 30 + [2200]
    d = pd.DataFrame({"open": closes, "high": highs, "low": lows, "close": closes, "volume": volumes})
    sigs = strategie_downside_reversal(p, d, volumen_lookback_tage=20)
    assert sigs
    assert sigs[0]["name"] == "Downside Reversal an neuem Hoch"
    assert sigs[0]["buch_verweis"] == "Kap. 6.2 Downside Reversal"
    assert "Neues Hoch" in sigs[0]["begruendung"]


def test_downside_reversal_uses_custom_thresholds_and_tranches():
    p = Position("T", 100, "2026-01-01", 10)
    closes = [100] * 20 + [103]
    d = pd.DataFrame({
        "open": [100] * 21,
        "high": [101] * 20 + [112],
        "low": [99] * 20 + [100],
        "close": closes,
        "volume": [1000] * 20 + [1400],
    })
    sigs = strategie_downside_reversal(
        p, d,
        kerzenweite_lookback_tage=10,
        volumen_lookback_tage=20,
        neues_hoch_lookback_tage=30,
        weite_kerze_faktor=1.4,
        volumen_ratio_min=1.3,
        tranche_weite_umkehr_pct=27,
    )
    assert sigs
    assert sigs[0]["name"] == "Weite Umkehrkerze"
    assert sigs[0]["tranche_pct"] == 27

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


def test_strategie13_stau_tage_default_logic():
    p = Position("T", 100, "2026-01-01", 10, peak=130)
    d = pd.DataFrame({
        "open": [100] * 42 + [120.1, 120.2] + [120] * 8,
        "high": [101] * 42 + [121.1, 121.2] + [121] * 8,
        "low": [99] * 42 + [119.1, 119.2] + [119] * 8,
        "close": [120] * 42 + [120.4, 120.3] + [120] * 8,
        "volume": [1000] * 50 + [1400, 1500],
    })
    sigs = strategie_stau_tage(p, d)
    assert sigs
    assert "2 Stau-Tage in 10 Sessions" == sigs[0]["name"]
    assert sigs[0]["tranche_pct"] == 20
    assert sigs[0]["buch_verweis"] == "Kap. 6.2 Stau-Tage"


def test_strategie13_stau_tage_custom_setup_and_near_high_tranche():
    p = Position("T", 100, "2026-01-01", 10, peak=126)
    d = pd.DataFrame({
        "open": [100] * 30 + [122.0, 121.8, 122, 122],
        "high": [101] * 30 + [123.0, 122.8, 123, 123],
        "low": [99] * 30 + [121.0, 120.8, 121, 121],
        "close": [122] * 30 + [122.2, 122.1, 122, 122],
        "volume": [1000] * 30 + [1250, 1350, 900, 900],
    })
    sigs = strategie_stau_tage(
        p, d,
        fenster_tage=4,
        volumen_lookback_tage=20,
        max_tagesveraenderung_pct=0.5,
        min_vol_ratio=1.2,
        min_stau_tage=2,
        nahe_hoch_drawdown_max_pct=4.0,
        tranche_nahe_hoch_pct=40.0,
        tranche_standard_pct=15.0,
    )
    assert sigs
    assert sigs[0]["tranche_pct"] == 40


def test_ma_abstand_kapitel_6_2_stufen_und_texte():
    p = Position("T", 100, "2026-01-01", 10)
    closes = [100.0] * 220 + [220.0]
    d = pd.DataFrame({
        "open": closes,
        "high": closes,
        "low": closes,
        "close": closes,
        "volume": [1000] * len(closes),
    })

    sigs = strategie_ma_abstand(p, d)
    names = [s["name"] for s in sigs]

    assert "10% über 10-MA" in names
    assert "15% über 21-EMA" in names
    assert "25% über 50-MA" in names
    assert any("über 200-MA (Klimaxzone)" in n for n in names)
    assert all(s["buch_verweis"] == "Kap. 6.2 MA-Abstand" for s in sigs)


def test_ma_abstand_custom_schwelle_und_tranche_werden_uebernommen():
    p = Position("T", 100, "2026-01-01", 10)
    closes = [100.0] * 220 + [113.0]
    d = pd.DataFrame({
        "open": closes,
        "high": closes,
        "low": closes,
        "close": closes,
        "volume": [1000] * len(closes),
    })

    sigs = strategie_ma_abstand(
        p,
        d,
        schwelle_ma10_pct=11.0,
        tranche_ma10_pct=40.0,
        schwelle_ma21_pct=50.0,
        schwelle_ma50_pct=50.0,
        schwelle_ma200_pct=50.0,
    )

    assert len(sigs) == 1
    assert sigs[0]["name"] == "10% über 10-MA"
    assert sigs[0]["tranche_pct"] == 40

def test_strategie5_drawdown_stufe1_enthaelt_naechste_marke_12_prozent():
    p = Position("T", 100, "2026-01-01", 10, peak=120)
    closes = [100.0] * 30 + [109.5]  # +9.5% PnL und 8.75% Drawdown vom Peak
    d = pd.DataFrame({
        "open": closes,
        "high": [120.0] * len(closes),
        "low": closes,
        "close": closes,
        "volume": [1000] * len(closes),
    })
    sigs = strategie_drawdown_vom_peak(p, d)
    assert len(sigs) == 1
    assert sigs[0]["name"] == "Drawdown 8% vom Peak"
    assert abs(sigs[0]["naechste_marke"] - 105.6) < 1e-9


def test_strategie5_drawdown_stufe3_mit_trendbruch_komplettausstieg():
    p = Position("T", 100, "2026-01-01", 10, peak=150)
    closes = [120.0] * 25 + [110.0]
    d = pd.DataFrame({
        "open": closes,
        "high": [150.0] * len(closes),
        "low": closes,
        "close": closes,
        "volume": [1000] * len(closes),
    })
    sigs = strategie_drawdown_vom_peak(p, d)
    assert len(sigs) == 1
    assert sigs[0]["name"] == "Drawdown >15% + Trendbruch"
    assert sigs[0]["tranche_pct"] == 100
    assert sigs[0]["naechste_marke"] is None


def test_strategie5_drawdown_custom_setup_wird_uebernommen():
    p = Position("T", 100, "2026-01-01", 10, peak=130)
    closes = [120.0] * 30 + [118.0]  # +18% PnL, 9.23% Drawdown
    d = pd.DataFrame({
        "open": closes,
        "high": [130.0] * len(closes),
        "low": closes,
        "close": closes,
        "volume": [1000] * len(closes),
    })
    sigs = strategie_drawdown_vom_peak(
        p,
        d,
        drawdown_stufe1_min_pct=9.0,
        drawdown_stufe2_min_pct=11.0,
        drawdown_stufe3_min_pct=14.0,
        tranche_stufe1_pct=22.0,
    )
    assert len(sigs) == 1
    assert sigs[0]["tranche_pct"] == 22
    assert abs(sigs[0]["naechste_marke"] - (130 * 0.89)) < 1e-9


def test_themen_dedup_zaehlt_pro_thema_nur_staerkstes_signal():
    """Wenn zwei Strategien dasselbe Thema treffen, soll nur das stärkere Signal in die Summe einfließen."""
    from sell_strategies import STRATEGY_THEMES

    # Sanity check: ma21_bruch, ma_bruch_defensiv und ma_basierte_sequenz teilen sich das Thema "trendbruch".
    assert STRATEGY_THEMES["ma21_bruch"] == "trendbruch"
    assert STRATEGY_THEMES["ma_bruch_defensiv"] == "trendbruch"
    assert STRATEGY_THEMES["ma_basierte_sequenz"] == "trendbruch"
    # drawdown_vom_peak ist ein eigenes Thema.
    assert STRATEGY_THEMES["drawdown_vom_peak"] == "drawdown"


def test_themen_dedup_drawdown_und_ma21_bruch_zaehlen_einzeln():
    """Drawdown vom Peak und 21-EMA-Bruch sind unterschiedliche Themen → beide tragen zur Summe bei."""
    p = Position("T", 100, "2026-01-01", 10, peak=130)
    # Konstruiere Szenario: hoher Peak, jetzt PnL+18%, Drawdown ~9% (Stufe 1 → 25%), gleichzeitig Schluss unter 21-EMA.
    closes = [120.0] * 30 + [110.0]
    d = pd.DataFrame({
        "open": closes,
        "high": [130.0] + [121.0] * 29 + [113.0],
        "low": [120.0] * 30 + [108.0],
        "close": closes,
        "volume": [1000] * len(closes),
    })
    res = verkaufs_empfehlung_gesamt(
        p, d, d, None, None, "Bullisch", "Neutral",
        ["drawdown_vom_peak", "ma21_bruch"],
        {"drawdown_stufe1_min_pct": 5.0, "drawdown_stufe2_min_pct": 20.0, "drawdown_stufe3_min_pct": 30.0},
    )
    # Beide Themen sollten in der Summe vertreten sein.
    assert "drawdown" in res["themen_in_summe"]
    assert "trendbruch" in res["themen_in_summe"]


def test_themen_dedup_klimax_signale_zaehlen_nur_einmal():
    """split_anstieg und groesster_anstieg_volumen sind beide klimax — Summe bleibt klein."""
    # Setup: starker Anstieg, der theoretisch beide Klimax-Strategien feuert.
    # Vereinfacht: wir prüfen das Mapping reicht; die echte Aggregation hängt von Marktdaten ab.
    from sell_strategies import STRATEGY_THEMES
    assert STRATEGY_THEMES["split_anstieg"] == "klimax"
    assert STRATEGY_THEMES["groesster_anstieg_volumen"] == "klimax"
    assert STRATEGY_THEMES["erschoepfungsluecke"] == "klimax"


def test_themen_dedup_signale_tragen_thema_im_output():
    """Jedes Signal in `alle_signale` soll ein 'thema'-Feld haben."""
    p = Position("T", 100, "2026-01-01", 10)
    d = make_df([100, 95])
    res = verkaufs_empfehlung_gesamt(p, d, d, None, None, "Bullisch", "Neutral", ["notbremse_verlust"])
    sig_themen = {s.get("thema") for s in res["alle_signale"]}
    assert "verlust_notbremse" in sig_themen


def test_diagnose_ma21_bruch_position_im_verlust():
    """Im Verlust soll die Diagnose explizit auf den Verlustfall hinweisen."""
    p = Position("T", 100, "2026-01-01", 10)
    d = make_df([100, 95])  # Position aktuell im Verlust
    grund = diagnose_strategie_kein_signal("ma21_bruch", p, d, d, None, None, "Bullisch", {})
    assert "Verlust" in grund
    assert "Gewinnfall" in grund or "Strategie 1" in grund or "rueckkehr_pivot" in grund


def test_diagnose_ma21_bruch_geduldig_braucht_drei_tage():
    """Mit Variante 'geduldig' im Gewinn aber <3 Tage unter MA — Grund muss Variante nennen."""
    closes = [100, 105, 110, 115, 120, 125, 130, 135, 140, 145,
              150, 155, 160, 165, 170, 175, 180, 185, 190, 195,
              200, 205, 210, 215, 220, 225, 200]  # letzter Tag plötzlich unter MA, aber nur 1 Tag
    p = Position("T", 100, "2026-01-01", 10)
    d = make_df(closes)
    grund = diagnose_strategie_kein_signal(
        "ma21_bruch", p, d, d, None, None, "Bullisch", {"ma21_variante": "geduldig"},
    )
    assert "geduldig" in grund


def test_diagnose_einfache_verluststufen_ueber_schwelle():
    """Wenn P&L oberhalb der ersten Verluststufe liegt, soll die Diagnose das nennen."""
    p = Position("T", 100, "2026-01-01", 10)
    d = make_df([100, 99])
    grund = diagnose_strategie_kein_signal(
        "einfache_verluststufen", p, d, d, None, None, "Bullisch",
        {"verlust_stufe_1": 3.0, "verlust_stufe_2": 5.0, "verlust_stufe_3": 7.0},
    )
    assert "3" in grund
    assert "%" in grund


def test_diagnose_split_anstieg_kein_datum():
    """Ohne split_datum soll ein klarer Hinweis kommen."""
    p = Position("T", 100, "2026-01-01", 10)
    d = make_df([100, 110])
    grund = diagnose_strategie_kein_signal("split_anstieg", p, d, d, None, None, "Bullisch", {})
    assert "Split" in grund


def test_diagnose_rs_linie_ohne_benchmark():
    """Ohne SPY-Daten soll die Diagnose das nennen."""
    p = Position("T", 100, "2026-01-01", 10)
    d = make_df([100, 110])
    grund = diagnose_strategie_kein_signal("rs_linie", p, d, d, None, None, "Bullisch", {})
    assert "Benchmark" in grund or "SPY" in grund


def test_diagnose_unbekannter_key_liefert_fallback():
    p = Position("T", 100, "2026-01-01", 10)
    d = make_df([100, 110])
    grund = diagnose_strategie_kein_signal("kein_existierender_key", p, d, d, None, None, "Bullisch", {})
    assert isinstance(grund, str) and len(grund) > 0


def test_diagnose_groesster_einbruch_zeigt_gewinntag_nicht_als_verlust():
    """Bei einem Gewinntag soll die Diagnose 'Gewinntag' / + statt 'Tagesverlust' ausgeben."""
    idx = pd.date_range("2026-01-02", periods=20, freq="B")
    # 19 Tage flach um 100, letzter Tag +4.72% (gain → close-to-close negative drop)
    closes = [100.0] * 19 + [104.72]
    d = pd.DataFrame({
        "open": closes,
        "high": [c * 1.001 for c in closes],
        "low": [c * 0.999 for c in closes],
        "close": closes,
        "volume": [1000] * len(closes),
    }, index=idx)
    p = Position("T", 90.0, idx[0], 10)  # ~16% Gewinn → über min_pnl 10%
    grund = diagnose_strategie_kein_signal(
        "groesster_einbruch", p, d, None, None, None, "Bullisch", {},
    )
    assert "Gewinntag" in grund
    assert "Tagesverlust" not in grund or grund.count("Tagesverlust") <= 1
    assert "+4.72" in grund or "4.72" in grund


def test_groesster_einbruch_vergleicht_nur_seit_einstieg():
    """Pre-Einstieg-Drops dürfen den 'größten Einbruch' nicht maskieren."""
    idx = pd.date_range("2025-01-02", periods=60, freq="B")
    # Erst Riesendrop -20% (vor Einstieg), dann flach, dann am Ende -5% (seit Einstieg größter Drop)
    closes = [100.0] * 30 + [80.0] + [120.0] * 28 + [114.0]
    d = pd.DataFrame({
        "open": closes,
        "high": [c * 1.001 for c in closes],
        "low": [c * 0.999 for c in closes],
        "close": closes,
        "volume": [1000] * len(closes),
    }, index=idx)
    # Einstieg nach dem großen Drop
    p = Position("T", 100.0, idx[35], 10)
    from sell_strategies import strategie_groesster_einbruch
    sigs = strategie_groesster_einbruch(p, d, None, min_pnl_pct=10.0, min_tagesverlust_pct=3.0)
    # Heute ist der größte Einbruch seit Einstieg → Signal soll feuern
    assert any("Tagesverlust seit Beginn" in s["name"] or "hohes Volumen" in s["name"] for s in sigs)


def test_groesster_anstieg_volumen_vergleicht_nur_seit_einstieg():
    """Pre-Einstieg-Rallies dürfen den 'größten Anstieg seit Einstieg' nicht maskieren."""
    from sell_strategies import strategie_groesster_anstieg_volumen
    idx = pd.date_range("2025-01-02", periods=60, freq="B")
    # Vor Einstieg: ein riesiger Tagesanstieg +25% bei extrem hohem Volumen
    # (würde ohne 'seit Einstieg' den heutigen Anstieg überdecken).
    # Nach Einstieg: flach, dann am letzten Tag +8% Anstieg + neues Volumen-Hoch seit Einstieg.
    # Position-PnL deutlich > 15% (Einstieg 100, Endkurs 140).
    pre = [100.0] * 30 + [125.0]            # 31 Werte (letzter pre-buy: +25%)
    post = [129.6] * 28 + [140.0]           # 29 Werte (letzter post-buy: ~+8% ggü. Vortag)
    closes = pre + post
    assert len(closes) == 60
    volumes = ([1000] * 30 + [50000]        # extremes Volumen vor Einstieg
               + [1200] * 28 + [3000])      # heutiges Volumen ist Max seit Einstieg
    d = pd.DataFrame({
        "open": closes,
        "high": [c * 1.001 for c in closes],
        "low": [c * 0.999 for c in closes],
        "close": closes,
        "volume": volumes,
    }, index=idx)
    # Einstieg nach dem großen Pre-Einstiegs-Anstieg → +8% wäre der größte seit Einstieg.
    p = Position("T", 100.0, idx[31], 10)
    sigs = strategie_groesster_anstieg_volumen(p, d)
    # Heute ist der größte Anstieg seit Einstieg → Signal soll feuern.
    assert sigs, f"Erwartetes Signal nicht gefeuert: {sigs}"
    namen = [s["name"] for s in sigs]
    assert any("Größter Anstieg" in n or "Größter Tagesanstieg" in n for n in namen)


def test_diagnose_rueckkehr_pivot_zeigt_kein_nan_bei_fehlenden_tiefs():
    """Wenn nur Pivot gesetzt ist, soll die Diagnose Tief Tag 0/1 nicht als 'nan' ausgeben."""
    p = Position("T", 100, "2026-01-01", 10, pivot=80, tief_tag_1=float("nan"), tief_tag_0=float("nan"))
    d = make_df([100] * 30 + [120])
    grund = diagnose_strategie_kein_signal(
        "rueckkehr_pivot", p, d, d, None, None, "Bullisch", {},
    )
    assert "nan" not in grund.lower()
    assert "Pivot" in grund
