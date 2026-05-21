"""UI-independent rule engine for sell-decision recommendations."""

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any

import math

import pandas as pd

from sell_strategies import Position, verkaufs_empfehlung_gesamt


ALLOWED_RECOMMENDATION_LEVELS = [0, 25, 33, 50, 66, 75, 100]
BEARISH_MARKET_LABEL = "Bärisch"
DEFENSIVE_MODE = "Defensiv verkaufen: Verluste begrenzen"
STRENGTH_OFFENSIVE_MODE = "Stärke offensiv verkaufen: Gewinn in weiter laufender Aktie mitnehmen"
STRENGTH_DEFENSIVE_MODE = "Stärke defensiv verkaufen: Gewinn nach Rückzug sichern"

# All Hub strategies available to the Live-Monitor (full parity with Strategien-Hub UI).
# Patterns #11 (Distribution-Tage) and #15 (Volumen-Faktor) remain LM-native further below.
# Users select the active subset via the Setup-Panel's multiselect.
LM_HUB_STRATEGIES_ALL = [
    "notbremse_verlust",
    "drei_stufen_nach_kauf",
    "gewinn_in_stufen",
    "ma21_bruch",
    "drawdown_vom_peak",
    "ma_abstand",
    "verlusttage_haeufung",
    "groesster_anstieg_volumen",
    "split_anstieg",
    "erschoepfungsluecke",
    "downside_reversal",
    "stau_tage",
    "rueckkehr_pivot",
    "ma_bruch_defensiv",
    "drei_verlustwochen",
    "groesster_einbruch",
    "rs_linie",
    "ma_basierte_sequenz",
    "einfach_halbe_position",
    "misslungener_ausbruch_5stufen",
    "einfache_verluststufen",
    "atr_basiert",
]

# Default active subset — matches the original 13 LM patterns (1-10, 12-14 of the
# comparison table). Other strategies are available via the multiselect but opt-in
# to avoid signal collisions (e.g., einfach_halbe_position + gewinn_in_stufen both
# firing at pnl≥20).
LM_HUB_STRATEGIES_DEFAULT = [
    "notbremse_verlust",
    "drei_stufen_nach_kauf",
    "gewinn_in_stufen",
    "ma21_bruch",
    "drawdown_vom_peak",
    "ma_abstand",
    "verlusttage_haeufung",
    "split_anstieg",
    "downside_reversal",
    "stau_tage",
    "rueckkehr_pivot",
    "ma_bruch_defensiv",
    "drei_verlustwochen",
    "groesster_einbruch",
    "rs_linie",
]

# Backwards compatibility / default subset.
LM_HUB_STRATEGIES = LM_HUB_STRATEGIES_DEFAULT

# Hub default parameters that mirror the Strategien-Hub setup defaults (app.py:11506+).
# Overridable via metrics_payload["lm_setup"] or manual_data["sell_setup"].
LM_HUB_DEFAULTS = {
    "ma21_variante": "gestaffelt",
    "notbremse_verlust_schwelle_baerisch_pct": 4.0,
    "notbremse_verlust_schwelle_unsicher_pct": 5.0,
    "notbremse_verlust_schwelle_bullisch_pct": 7.0,
    "drei_stufen_max_gewinn_aktiv_pct": 8.0,
    "drei_stufen_tranche_stufe1_pct": 33.0,
    "drei_stufen_tranche_stufe2_pct": 33.0,
    "drei_stufen_notbremse_verlust_pct": 7.0,
    "gewinn_nachdenken_schwelle_bull_pct": 15.0,
    "gewinn_teilverkauf_unten_bull_pct": 20.0,
    "gewinn_teilverkauf_oben_bull_pct": 35.0,
    "gewinn_nachdenken_schwelle_bear_pct": 10.0,
    "gewinn_teilverkauf_unten_bear_pct": 10.0,
    "gewinn_teilverkauf_oben_bear_pct": 15.0,
    "drawdown_stufe1_min_pct": 8.0,
    "drawdown_stufe2_min_pct": 12.0,
    "drawdown_stufe3_min_pct": 15.0,
    "drawdown_tranche_stufe1_pct": 25.0,
    "drawdown_tranche_stufe2_pct": 33.0,
    "drawdown_tranche_stufe3_ohne_trendbruch_pct": 50.0,
    "drawdown_tranche_stufe3_mit_trendbruch_pct": 100.0,
    "ma_abstand_schwelle_ma10_pct": 10.0,
    "ma_abstand_schwelle_ma21_pct": 15.0,
    "ma_abstand_schwelle_ma50_pct": 25.0,
    "ma_abstand_schwelle_ma200_pct": 70.0,
    "ma_abstand_klimax_ma200_vollausstieg_pct": 100.0,
    "ma_abstand_tranche_ma10_pct": 25.0,
    "ma_abstand_tranche_ma21_pct": 33.0,
    "ma_abstand_tranche_ma50_pct": 33.0,
    "ma_abstand_tranche_ma200_basis_pct": 50.0,
    "verlusttage_min_tiefere_schlusskurse_in_folge": 3,
    "verlusttage_volumen_lookback_tage": 50,
    "verlusttage_volumen_ratio_min": 1.1,
    "verlusttage_tief_marker_lookback_tage": 5,
    "verlusttage_updown_fenster_tage": 15,
    "verlusttage_updown_diff_min": 3,
    "verlusttage_tranche_pct": 25.0,
    "split_signal_schwelle_pct": 25.0,
    "split_starke_schwelle_pct": 50.0,
    "split_datum": None,
    "downside_kerzenweite_lookback_tage": 10,
    "downside_volumen_lookback_tage": 50,
    "downside_neues_hoch_lookback_tage": 30,
    "downside_weite_kerze_faktor": 1.5,
    "downside_volumen_ratio_min": 1.2,
    "downside_schluss_unteres_drittel_faktor": 3.0,
    "downside_tranche_neues_hoch_pct": 33.0,
    "downside_tranche_weite_umkehr_pct": 20.0,
    "downside_tranche_warnstufe_pct": 15.0,
    "stau_fenster_tage": 10,
    "stau_volumen_lookback_tage": 50,
    "stau_max_tagesveraenderung_pct": 1.0,
    "stau_min_vol_ratio": 1.3,
    "stau_min_tage": 2,
    "stau_nahe_hoch_drawdown_max_pct": 5.0,
    "stau_tranche_nahe_hoch_pct": 33.0,
    "stau_tranche_standard_pct": 20.0,
    "groesster_einbruch_min_pnl_pct": 10.0,
    "groesster_einbruch_min_tagesverlust_pct": 3.0,
    "groesster_einbruch_tagesvol_ratio_schwelle": 1.5,
    "groesster_einbruch_wochenvol_ratio_schwelle": 1.3,
    "rs_pnl_tag_zu_woche": 20.0,
    "rs_pnl_woche_zu_monat": 80.0,
    # ATR-basiert (Strategie 23)
    "ziel_atr_multiplikator": 3.0,
    "ueberdehnung_atr_start": 3.0,
    "ueberdehnung_atr_stark": 4.0,
    # Einfache Verluststufen (Strategie 22)
    "verlust_stufe_1": 3.0,
    "verlust_stufe_2": 5.0,
    "verlust_stufe_3": 7.0,
    # Einfach halbe Position (Strategie 20)
    "erste_haelfte_gewinn_pct": 20.0,
    # MA-basierte Sequenz (Strategie 19)
    "ma_seq_gewinnzone_min_pct": 20.0,
    "ma_seq_gewinnzone_max_pct": 25.0,
    "ma_seq_gewinnzone_tranche_pct": 33.0,
    "ma_seq_ueber_ma10_pct": 10.0,
    "ma_seq_ueber_ma10_tranche_pct": 20.0,
    "ma_seq_unter_ma10_mindestgewinn_pct": 5.0,
    "ma_seq_unter_ma10_tranche_pct": 20.0,
    "ma_seq_pendel_tranche_pct": 25.0,
    "ma_seq_pendel_lookback_tage": 5,
    "ma_seq_pendel_wechsel_min": 3,
    "ma_seq_unter_ma21_mindestgewinn_pct": 5.0,
    "ma_seq_unter_ma21_tranche_pct": 25.0,
    "ma_seq_klarer_ma50_bruch_pct": 2.0,
    # Set of active strategies (subset of LM_HUB_STRATEGIES_ALL). Default: original 13 patterns.
    "active_strategies": list(LM_HUB_STRATEGIES_DEFAULT),
}
LOSS_LIMIT_STYLE = "Verlustbegrenzung"
STRENGTH_OFFENSIVE_STYLE = "Gewinn in Stärke mitnehmen"
STRENGTH_DEFENSIVE_STYLE = "Gewinn nach Rückzug sichern"

BOOK_REFERENCES = {
    "killer_loss_7": "Risikoregel: 7%-Notbremse",
    "killer_bear_loss_5": "Bärenmarkt-Regel: engere Verlustgrenze 3-5%",
    "killer_sma200_volume": "Trendbruch-Regel: 200-Tage-Linie mit hohem Volumen",
    "killer_breakout_failed_volume": "Ausbruchsregel: Ausbruch ohne Kraft / Tief Tag 1",
    "killer_three_loss_weeks_rising_volume": "Wochenchart-Regel: drei Verlustwochen mit steigendem Volumen",
    "killer_eight_weeks_under_10w": "Wochenchart-Regel: acht Wochenschlüsse unter 10-Wochen-Linie",
    "tranche_low_day_1_loss": "Ausbruchsrisiko: Schluss unter Tief Tag 1",
    "tranche_low_day_0_loss": "Ausbruchsrisiko: Schluss unter Tief Tag 0",
    "tranche_first_sma21_break_gain": "Trendregel: erster 21-Tage-Linienbruch im Gewinn",
    "tranche_three_days_under_sma21_gain": "Trendregel: drei Tage unter 21-Tage-Linie",
    "tranche_sma50_break_volume": "Trendregel: 50-Tage-Linienbruch mit erhöhtem Volumen",
    "tranche_profit_zone_20_25": "Gewinnmitnahme-Regel: 20-25%-Zone",
    "tranche_drawdown_8": "Trailing-Regel: Drawdown vom Hoch >= 8%",
    "tranche_drawdown_12": "Trailing-Regel: Drawdown vom Hoch >= 12%",
    "tranche_drawdown_15_full": "Trailing-Regel: Drawdown vom Hoch >= 15%",
    "tranche_extended_sma50": "Überdehnungsregel: >25% über 50-Tage-Linie",
    "tranche_extended_sma200": "Überdehnungsregel: >70% über 200-Tage-Linie",
    "tranche_rs_first_21_break": "Relative-Stärke-Regel: RS-Linie bricht 21-Tage-Linie",
    "tranche_rs_three_days_under_21": "Relative-Stärke-Regel: RS drei Tage unter 21-Tage-Linie",
    "tranche_rs_50_break": "Relative-Stärke-Regel: RS-Linie bricht 50-Tage-Linie",
    "tranche_worst_day_high_volume": "Volumenregel: größter Tagesverlust mit erhöhtem Volumen",
    "tranche_personality_changed": "Persönlichkeits-Check",
    "tranche_weak_industry_gain": "Branchengruppen-Regel: schwache Gruppe trotz Gewinn",
    "warning_stall_days": "Warnzeichen: Stau-Tage nahe Ausbruchspunkt",
    "warning_low_closes": "Warnzeichen: mehrere Schlusskurse nahe Tagestief",
    "warning_negative_divergence": "Warnzeichen: negative Divergenz zum Markt",
    "warning_weak_rebounds": "Warnzeichen: schwache Erholungsversuche",
    "warning_downside_reversal": "Warnzeichen: Downside Reversal nahe Hoch",
    "warning_lower_lows": "Warnzeichen: drei bis vier tiefere Tiefs ohne Rebound",
    "watch_under_sma21_1_2": "Watch: ein bis zwei Tage unter 21-Tage-Linie",
    "watch_drawdown_5_8": "Watch: Drawdown 5-8% vom Hoch",
    "watch_up_down_volume": "Watch: Up/Down-Volume-Ratio < 1,0",
    "watch_distribution_days": "Watch: vier oder mehr Distribution-Tage in 25 Sessions",
}

WARNING_CONTRIBUTIONS = {
    # Patterns that the Hub engine does NOT cover natively → stay as LM auto-warnings.
    "low_closes": ("warning_low_closes", 15, "Mehrere Schlusskurse nahe Tagestief"),
    "mehrere_schlusskurse_nahe_tagestief": ("warning_low_closes", 15, "Mehrere Schlusskurse nahe Tagestief"),
    "negative_market_divergence": ("warning_negative_divergence", 20, "Negative Divergenz zum Markt"),
    "negative_divergenz_zum_markt": ("warning_negative_divergence", 20, "Negative Divergenz zum Markt"),
    "weak_rebounds": ("warning_weak_rebounds", 15, "Schwache Erholungsversuche"),
    "schwache_erholungsversuche": ("warning_weak_rebounds", 15, "Schwache Erholungsversuche"),
    # Patterns that DUPLICATE Hub strategies are intentionally NOT mapped here anymore:
    #   - stall_days_near_breakout   → covered by Hub strategie_stau_tage
    #   - failed_breakout_high_volume → covered by Hub strategie_rueckkehr_pivot
    #   - lower_lows_no_rebound      → covered by Hub strategie_verlusttage_haeufung
    #   - downside_reversal_near_high → covered by Hub strategie_downside_reversal
    #   - worst_day_high_volume      → covered by Hub strategie_groesster_einbruch
    #   - three_loss_weeks_rising_volume → covered by Hub strategie_drei_verlustwochen
}


def _resolve_setup(metrics_payload: dict, manual_data: dict | None) -> dict:
    setup = dict(LM_HUB_DEFAULTS)
    payload_setup = (metrics_payload or {}).get("lm_setup") if isinstance(metrics_payload, dict) else None
    if isinstance(payload_setup, dict):
        setup.update(payload_setup)
    manual_setup = (manual_data or {}).get("sell_setup") if isinstance(manual_data, dict) else None
    if isinstance(manual_setup, dict):
        setup.update(manual_setup)
    return setup


def _slugify(value: str) -> str:
    text = _norm_key(value)
    return text or "hub_signal"


def _classify_hub_signal_mode(name: str, contribution: int, pnl: float, regime: str) -> tuple[str, str]:
    """Derive sell_mode/sell_style from signal name + pnl context."""
    lower = (name or "").lower()
    loss_keywords = ("notbremse", "verlust", "7%", "5%", "verluststufe", "stopp")
    profit_keywords = ("gewinn", "gewinnzone", "ma-abstand", "über 10-ma", "über 21-ma", "über 50-ma", "über 200-ma", "klimax", "anstieg", "ziel", "atr gewinn")
    if pnl <= 0 or any(token in lower for token in loss_keywords):
        return DEFENSIVE_MODE, LOSS_LIMIT_STYLE
    if any(token in lower for token in profit_keywords):
        return STRENGTH_OFFENSIVE_MODE, STRENGTH_OFFENSIVE_STYLE
    return STRENGTH_DEFENSIVE_MODE, STRENGTH_DEFENSIVE_STYLE


def _hub_signal_to_rule_signal(hub_signal: dict, pnl: float, regime: str, as_of_date: str) -> RuleSignal:
    name = str(hub_signal.get("name") or "Hub-Signal")
    contribution = int(hub_signal.get("tranche_pct") or 0)
    book_ref = str(hub_signal.get("buch_verweis") or "")
    reason = str(hub_signal.get("begruendung") or "")
    next_mark = hub_signal.get("naechste_marke")
    trigger_type = str(hub_signal.get("trigger_typ") or "")
    strategy_key = str(hub_signal.get("strategy_key") or "")
    note_parts: list[str] = []
    if reason:
        note_parts.append(reason)
    if next_mark is not None:
        try:
            mark_value = float(next_mark)
            note_parts.append(f"Nächste Marke: {mark_value:.2f}")
        except (TypeError, ValueError):
            pass
    if trigger_type and trigger_type not in {"schluss", "info"}:
        note_parts.append(f"Trigger: {trigger_type}")
    sell_mode, sell_style = _classify_hub_signal_mode(name, contribution, pnl, regime)
    signal_id = "hub_" + _slugify(name)
    rule_signal = RuleSignal(
        id=signal_id,
        label=name,
        contribution_percent=int(contribution),
        book_reference=book_ref,
        signal_date=str(as_of_date or ""),
        event_note=" · ".join(part for part in note_parts if part),
        sell_mode=sell_mode if contribution > 0 else "",
        sell_style=sell_style if contribution > 0 else "",
        strategy_key=strategy_key,
    )
    return rule_signal


def _build_position_from_payload(metrics_payload: dict, manual_data: dict, tranche_log: list[dict] | None) -> Position | None:
    metrics, ticker, buy_price, shares = _extract_inputs(metrics_payload or {})
    if not ticker or buy_price <= 0:
        return None
    pivot = _safe_float(_manual_value(manual_data, metrics_payload or {}, "pivot"))
    low_day_1 = _safe_float(_manual_value(manual_data, metrics_payload or {}, "low_day_1"))
    low_day_0 = _safe_float(_manual_value(manual_data, metrics_payload or {}, "low_day_0"))
    buy_date = (metrics_payload or {}).get("buy_date") or ""
    try:
        buy_ts = pd.Timestamp(buy_date).tz_localize(None) if buy_date else pd.Timestamp.now().normalize()
    except Exception:
        buy_ts = pd.Timestamp.now().normalize()
    ohlc = (metrics_payload or {}).get("ohlc_frames", {}) if isinstance(metrics_payload, dict) else {}
    peak = _safe_float((ohlc or {}).get("peak_since_buy")) if isinstance(ohlc, dict) else None
    realisierte = [
        _safe_float((entry or {}).get("tranche_percent"), 0.0) or 0.0
        for entry in (tranche_log or [])
        if isinstance(entry, dict) and str((entry or {}).get("ticker", ticker)).upper().strip() == ticker
    ]
    return Position(
        ticker=ticker,
        einstiegspreis=float(buy_price),
        einstiegsdatum=buy_ts,
        stueckzahl=float(shares or 0.0),
        pivot=pivot,
        tief_tag_1=low_day_1,
        tief_tag_0=low_day_0,
        peak=peak,
        realisierte_tranchen=realisierte,
    )


def _run_hub_engine(metrics_payload: dict, manual_data: dict, tranche_log: list[dict] | None) -> list[dict]:
    """Execute the Strategien-Hub engine on payload-bundled OHLC frames."""
    ohlc = (metrics_payload or {}).get("ohlc_frames", {}) if isinstance(metrics_payload, dict) else {}
    if not isinstance(ohlc, dict):
        return []
    daily = ohlc.get("daily_since_buy")
    weekly = ohlc.get("weekly_since_buy")
    bench_daily = ohlc.get("benchmark_daily")
    bench_weekly = ohlc.get("benchmark_weekly")
    if not isinstance(daily, pd.DataFrame) or daily.empty:
        return []
    if not isinstance(weekly, pd.DataFrame):
        weekly = pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
    position = _build_position_from_payload(metrics_payload, manual_data, tranche_log)
    if position is None:
        return []
    market_environment = str((manual_data or {}).get("market_environment") or "Unsicher") or "Unsicher"
    industry_group_status = str((manual_data or {}).get("industry_group_status") or "Neutral") or "Neutral"
    options = _resolve_setup(metrics_payload, manual_data)
    raw_active = options.get("active_strategies")
    if isinstance(raw_active, (list, tuple, set)) and raw_active:
        active_strategies = [k for k in raw_active if k in LM_HUB_STRATEGIES_ALL]
    else:
        active_strategies = list(LM_HUB_STRATEGIES_ALL)
    try:
        result = verkaufs_empfehlung_gesamt(
            position,
            daily,
            weekly if isinstance(weekly, pd.DataFrame) else pd.DataFrame(),
            bench_daily if isinstance(bench_daily, pd.DataFrame) else None,
            bench_weekly if isinstance(bench_weekly, pd.DataFrame) else None,
            market_environment,
            industry_group_status,
            active_strategies,
            options,
        )
    except Exception:
        return []
    signals = result.get("alle_signale", []) if isinstance(result, dict) else []
    return [sig for sig in signals if isinstance(sig, dict) and sig.get("aktuell_aktiv") and int(sig.get("tranche_pct") or 0) >= 0]


@dataclass(frozen=True)
class RuleSignal:
    id: str
    label: str
    contribution_percent: int = 0
    book_reference: str = ""
    signal_date: str = ""
    event_note: str = ""
    sell_mode: str = ""
    sell_style: str = ""
    strategy_key: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _safe_float(value, default=None):
    if value is None:
        return default
    try:
        if isinstance(value, float) and math.isnan(value):
            return default
    except Exception:
        pass
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    if math.isnan(parsed):
        return default
    return parsed


def _safe_bool(value) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "ja", "on", "x"}
    return bool(value)


def _norm_key(value: str) -> str:
    out = str(value or "").strip().lower()
    replacements = {"ä": "ae", "ö": "oe", "ü": "ue", "ß": "ss"}
    for old, new in replacements.items():
        out = out.replace(old, new)
    return "_".join(part for part in out.replace("-", " ").replace("/", " ").split() if part)


def _metric(metrics: dict, key: str, default=None):
    return _safe_float(metrics.get(key), default)


def _signal(
    signal_id: str,
    label: str,
    contribution: int = 0,
    *,
    signal_date: str = "",
    event_note: str = "",
    sell_mode: str = "",
    sell_style: str = "",
    strategy_key: str = "",
) -> RuleSignal:
    return RuleSignal(
        signal_id,
        label,
        int(contribution),
        BOOK_REFERENCES.get(signal_id, ""),
        str(signal_date or ""),
        str(event_note or ""),
        str(sell_mode or ""),
        str(sell_style or ""),
        str(strategy_key or ""),
    )


def _event_date(metrics: dict, *keys: str, fallback: str = "") -> str:
    for key in keys:
        value = metrics.get(key) if isinstance(metrics, dict) else None
        if value not in (None, ""):
            return str(value)
    return str(fallback or "")


def _event_note(*parts) -> str:
    return " · ".join(str(part) for part in parts if part not in (None, ""))


def _floor_allowed(value: float) -> int:
    value = max(0.0, min(100.0, float(value or 0.0)))
    return max(level for level in ALLOWED_RECOMMENDATION_LEVELS if level <= value)


def _next_allowed(value: int) -> int:
    for level in ALLOWED_RECOMMENDATION_LEVELS:
        if level > value:
            return level
    return 100


def _sum_already_sold(ticker: str, tranche_log: list[dict] | None) -> float:
    clean_ticker = str(ticker or "").upper().strip()
    total = 0.0
    for entry in tranche_log or []:
        if not isinstance(entry, dict):
            continue
        entry_ticker = str(entry.get("ticker") or clean_ticker).upper().strip()
        if clean_ticker and entry_ticker and entry_ticker != clean_ticker:
            continue
        pct = _safe_float(entry.get("tranche_percent"), 0.0) or 0.0
        if pct > 0:
            total += pct
    return max(0.0, min(100.0, total))


def _regime(pnl_pct: float | None, market_environment: str) -> str:
    pnl = _safe_float(pnl_pct, 0.0) or 0.0
    if pnl < 0:
        return "Defensiv"
    if str(market_environment) == BEARISH_MARKET_LABEL and pnl >= 10:
        return "Erste Gewinnmitnahme"
    if pnl < 15:
        return "Schutz"
    if pnl < 25:
        return "Erste Gewinnmitnahme"
    if pnl <= 80:
        return "Trailing"
    return "Großgewinner"


def _extract_inputs(metrics_payload: dict) -> tuple[dict, str, float, float]:
    if not isinstance(metrics_payload, dict):
        return {}, "", 0.0, 0.0
    metrics = metrics_payload.get("metrics") if isinstance(metrics_payload.get("metrics"), dict) else metrics_payload
    ticker = str(metrics_payload.get("ticker") or "").upper().strip()
    buy_price = _safe_float(metrics_payload.get("buy_price"), _safe_float(metrics.get("buy_price"), 0.0)) or 0.0
    shares = _safe_float(metrics_payload.get("shares"), _safe_float(metrics.get("shares"), 0.0)) or 0.0
    return metrics, ticker, buy_price, shares


def _active_checkbox_map(metrics_payload: dict, manual_data: dict, group: str) -> dict:
    combined = {}
    auto = (metrics_payload or {}).get("auto_checkboxes", {}) if isinstance(metrics_payload, dict) else {}
    if isinstance(auto, dict) and isinstance(auto.get(group), dict):
        combined.update({str(key): _safe_bool(value) for key, value in auto[group].items()})
    if isinstance(manual_data, dict) and isinstance(manual_data.get(group), dict):
        for key, value in manual_data[group].items():
            clean_key = str(key or "").strip()
            if clean_key and _safe_bool(value):
                combined[clean_key] = True
    return combined

def _manual_value(manual_data: dict, metrics_payload: dict, key: str):
    if isinstance(manual_data, dict) and manual_data.get(key) not in (None, ""):
        return manual_data.get(key)
    defaults = metrics_payload.get("manual_defaults", {}) if isinstance(metrics_payload, dict) else {}
    if isinstance(defaults, dict):
        return defaults.get(key)
    return None


def _build_stop_price(regime: str, metrics: dict, metrics_payload: dict, manual_data: dict, buy_price: float):
    current = _metric(metrics, "current_price")
    pivot = _safe_float(_manual_value(manual_data, metrics_payload, "pivot"))
    low_day_1 = _safe_float(_manual_value(manual_data, metrics_payload, "low_day_1"))
    sma21 = _metric(metrics, "sma21")
    sma50 = _metric(metrics, "sma50")
    weekly_sma10 = _metric(metrics, "weekly_sma10")

    def highest_valid(*values):
        valid = [_safe_float(v) for v in values if _safe_float(v) is not None and _safe_float(v) > 0]
        return max(valid) if valid else None

    if regime == "Defensiv":
        return highest_valid(buy_price * 0.93 if buy_price else None, low_day_1)
    if regime == "Schutz":
        candidates = [v for v in [pivot, sma21] if v is not None and v > 0]
        if current and candidates:
            below = [v for v in candidates if v <= current]
            return max(below) if below else max(candidates)
        return highest_valid(pivot, sma21)
    if regime == "Erste Gewinnmitnahme":
        return buy_price if buy_price > 0 else None
    if regime == "Trailing":
        return highest_valid(sma50 * 0.98 if sma50 else None, weekly_sma10)
    if regime == "Großgewinner":
        return weekly_sma10
    return None


def _build_trigger_prices(regime: str, metrics: dict, metrics_payload: dict, manual_data: dict, stop_price):
    low_day_1 = _safe_float(_manual_value(manual_data, metrics_payload, "low_day_1"))
    low_day_0 = _safe_float(_manual_value(manual_data, metrics_payload, "low_day_0"))
    sma21 = _metric(metrics, "sma21")
    sma50 = _metric(metrics, "sma50")
    weekly_sma10 = _metric(metrics, "weekly_sma10")
    if regime == "Defensiv":
        return low_day_1 or stop_price, low_day_0 or stop_price
    if regime == "Schutz":
        return sma21 or stop_price, low_day_0 or stop_price
    if regime == "Erste Gewinnmitnahme":
        return sma21 or stop_price, sma50 or stop_price
    if regime == "Trailing":
        return sma50 or weekly_sma10 or stop_price, weekly_sma10 or stop_price
    return weekly_sma10 or stop_price, weekly_sma10 or stop_price


def compute_sell_health_score(metrics_payload: dict, manual_data: dict | None = None) -> dict[str, Any]:
    """Compute the portfolio-ranking health score from sell metrics and manual data."""
    manual_data = manual_data or {}
    metrics, _ticker, _buy_price, _shares = _extract_inputs(metrics_payload or {})
    score = 50.0
    reasons: list[str] = []

    pnl = _metric(metrics, "pnl_pct")
    if pnl is not None:
        if pnl >= 20:
            score += 12; reasons.append("P&L >= 20%")
        elif pnl >= 10:
            score += 8; reasons.append("P&L 10-20%")
        elif pnl >= 0:
            score += 3; reasons.append("P&L 0-10%")
        elif pnl >= -3:
            score -= 5; reasons.append("P&L -3-0%")
        elif pnl >= -7:
            score -= 15; reasons.append("P&L -7--3%")
        else:
            score -= 30; reasons.append("P&L < -7%")

    current = _metric(metrics, "current_price")
    sma21 = _metric(metrics, "sma21")
    sma50 = _metric(metrics, "sma50")
    low_day_1 = _safe_float(_manual_value(manual_data, metrics_payload or {}, "low_day_1"))

    if current is not None and sma21 is not None:
        if current >= sma21:
            score += 8; reasons.append("Kurs >= 21-MA")
        else:
            score -= 12; reasons.append("Kurs < 21-MA")
    if current is not None and sma50 is not None:
        if current >= sma50:
            score += 6; reasons.append("Kurs >= 50-MA")
        elif current >= sma50 * 0.98:
            score -= 5; reasons.append("Kurs knapp unter 50-MA")
        else:
            score -= 20; reasons.append("Kurs < 50-MA -2%")
    if current is not None and low_day_1 is not None and current < low_day_1:
        score -= 15; reasons.append("Schluss unter Tief Tag 1")

    drawdown = abs(_metric(metrics, "drawdown_from_high_since_buy_pct", 0.0) or 0.0)
    if drawdown >= 15:
        score -= 15; reasons.append("Drawdown >= 15%")
    elif drawdown >= 12:
        score -= 10; reasons.append("Drawdown 12-15%")
    elif drawdown >= 8:
        score -= 5; reasons.append("Drawdown 8-12%")

    rs_line = _metric(metrics, "rs_line")
    rs_ma21 = _metric(metrics, "rs_ma21")
    rs_ma50 = _metric(metrics, "rs_ma50")
    days_under_rs21 = int(_metric(metrics, "days_under_rs_ma21", 0) or 0)
    if rs_line is not None and rs_ma21 is not None and rs_ma50 is not None:
        if rs_line >= rs_ma21 and rs_line >= rs_ma50 and days_under_rs21 == 0:
            rs_trend = "hoch"
            score += 10; reasons.append("RS hoch")
        elif rs_line < rs_ma21 or rs_line < rs_ma50 or days_under_rs21 >= 3:
            rs_trend = "runter"
            score -= 12; reasons.append("RS runter/unter MAs")
        else:
            rs_trend = "seitwärts"
    else:
        rs_trend = "seitwärts"

    dist_days = int(_metric(metrics, "distribution_days_25", 0) or 0)
    if dist_days >= 6:
        score -= 15; reasons.append("Distribution >= 6")
    elif dist_days >= 4:
        score -= 8; reasons.append("Distribution 4-5")
    elif dist_days >= 2:
        score -= 3; reasons.append("Distribution 2-3")

    score = max(0.0, min(100.0, score))
    if score >= 65:
        status = "Halten"
    elif score >= 40:
        status = "Beobachten"
    else:
        status = "Verkaufen"
    return {"health_score": round(score, 1), "status": status, "rs_trend": rs_trend, "reasons": reasons}


def evaluate_sell_decision(metrics_payload: dict, manual_data: dict | None = None, tranche_log: list[dict] | None = None) -> dict[str, Any]:
    """Evaluate sell-decision rules using the Strategien-Hub engine for patterns 1-10/12-14.

    Patterns #11 (Distribution-Tage) and #15 (Volumen-Faktor) remain in LM-native code
    below, together with all LM-only features (regime, stop price, health-score,
    personality check, industry-group penalty, watch signals).
    """
    manual_data = manual_data or {}
    metrics, ticker, buy_price, _shares = _extract_inputs(metrics_payload or {})
    if not ticker and isinstance(manual_data, dict):
        ticker = str(manual_data.get("ticker") or "").upper().strip()
    market_environment = str(manual_data.get("market_environment") or "Unsicher").strip() or "Unsicher"
    industry_group_status = str(manual_data.get("industry_group_status") or "Neutral").strip() or "Neutral"
    pnl = _metric(metrics, "pnl_pct", 0.0) or 0.0
    as_of_date = _event_date(metrics, "as_of_date", fallback=(metrics_payload or {}).get("as_of", ""))
    regime = _regime(pnl, market_environment)
    is_bearish = market_environment == BEARISH_MARKET_LABEL
    positive_pnl = pnl > 0
    negative_pnl = pnl < 0
    defensive_mode = DEFENSIVE_MODE
    strength_offensive_mode = STRENGTH_OFFENSIVE_MODE
    strength_defensive_mode = STRENGTH_DEFENSIVE_MODE

    killer_signals: list[RuleSignal] = []
    tranche_signals: list[RuleSignal] = []
    watch_signals: list[RuleSignal] = []

    # Patterns 1-10, 12-14: delegated to the Strategien-Hub engine.
    for hub_signal in _run_hub_engine(metrics_payload or {}, manual_data, tranche_log):
        contribution = int(hub_signal.get("tranche_pct") or 0)
        rule_signal = _hub_signal_to_rule_signal(hub_signal, pnl, regime, as_of_date)
        if contribution >= 100:
            killer_signals.append(rule_signal)
        elif contribution > 0:
            tranche_signals.append(rule_signal)
        else:
            # Hub info signals (tranche_pct == 0) become watch signals.
            watch_signals.append(rule_signal)

    already_sold = _sum_already_sold(ticker, tranche_log)

    # LM-only tranche signals (no Hub equivalent).
    if _safe_bool(manual_data.get("personality_changed")):
        tranche_signals.append(_signal(
            "tranche_personality_changed", "Persönlichkeits-Check angekreuzt", 25,
            signal_date=as_of_date,
            event_note="Manuelle Einschätzung im Live-Monitor",
            sell_mode=strength_defensive_mode if positive_pnl else defensive_mode,
            sell_style=STRENGTH_DEFENSIVE_STYLE if positive_pnl else LOSS_LIMIT_STYLE,
            strategy_key="lm_personality_check",
        ))
    if industry_group_status == "Schwach" and pnl > 10:
        tranche_signals.append(_signal(
            "tranche_weak_industry_gain", "Industriegruppe schwach und P&L > 10%", 33,
            signal_date=as_of_date,
            event_note=f"Industriegruppe: {industry_group_status} · P&L {pnl:.1f}%",
            sell_mode=strength_defensive_mode,
            sell_style=STRENGTH_DEFENSIVE_STYLE,
            strategy_key="lm_industry_group",
        ))

    for raw_key, active in _active_checkbox_map(metrics_payload or {}, manual_data, "warning_checkboxes").items():
        if not _safe_bool(active):
            continue
        mapped = WARNING_CONTRIBUTIONS.get(_norm_key(raw_key))
        if mapped:
            signal_id, contribution, label = mapped
            tranche_signals.append(_signal(
                signal_id, label, contribution,
                signal_date=as_of_date,
                event_note="Automatisch erkannt oder manuell aktiviertes Warnzeichen im Live-Monitor",
                sell_mode=strength_defensive_mode if positive_pnl else defensive_mode,
                sell_style=STRENGTH_DEFENSIVE_STYLE if positive_pnl else LOSS_LIMIT_STYLE,
                strategy_key="lm_auto_warning",
            ))

    # LM-native watch signals (incl. pattern #11 Distribution-Tage).
    days_under_sma21 = int(_metric(metrics, "days_under_sma21", 0) or 0)
    drawdown = abs(_metric(metrics, "drawdown_from_high_since_buy_pct", 0.0) or 0.0)
    if positive_pnl and 1 <= days_under_sma21 <= 2:
        watch_signals.append(_signal("watch_under_sma21_1_2", "Ein bis zwei Tage unter 21-MA bei positivem P&L", strategy_key="lm_watch"))
    if positive_pnl and 5 <= drawdown < 8:
        watch_signals.append(_signal("watch_drawdown_5_8", "Drawdown 5-8% vom Peak bei positivem P&L", strategy_key="lm_watch"))
    if (_metric(metrics, "up_down_volume_ratio_50") is not None) and (_metric(metrics, "up_down_volume_ratio_50") < 1.0):
        watch_signals.append(_signal("watch_up_down_volume", "Up/Down-Volume-Ratio < 1.0", strategy_key="lm_watch"))
    if (_metric(metrics, "distribution_days_25", 0) or 0) >= 4:
        watch_signals.append(_signal("watch_distribution_days", "Vier oder mehr Distribution-Tage in 25 Sessions", strategy_key="lm_watch"))

    if killer_signals:
        target_total = 100
    else:
        contribution_sum = sum(sig.contribution_percent for sig in tranche_signals)
        target_total = _floor_allowed(contribution_sum)
        if len(tranche_signals) >= 4:
            target_total = max(target_total, 75)
        if is_bearish and target_total < 100:
            target_total = _next_allowed(target_total)

    # Avoid recommending already executed tranches again. Killer signals target 100%,
    # but the immediate recommendation is only the still unsold remainder.
    sell_now_raw = max(0.0, min(100.0, target_total - already_sold))
    sell_now = _floor_allowed(sell_now_raw)
    recommendation_percent = int(sell_now)
    remaining_after_sale = max(0.0, 100.0 - already_sold - sell_now)

    if recommendation_percent >= 100 or (target_total == 100 and remaining_after_sale <= 0):
        label = "KOMPLETTVERKAUF"
    elif recommendation_percent > 0:
        label = "TEILVERKAUF"
    else:
        label = "HALTEN"

    stop_price = _build_stop_price(regime, metrics, metrics_payload or {}, manual_data, buy_price)
    next_tranche_trigger, full_exit = _build_trigger_prices(regime, metrics, metrics_payload or {}, manual_data, stop_price)
    add_again_condition = "Erst nach Rückeroberung der 21-MA/10-Wochen-Linie und nachlassendem Verkaufsvolumen wieder aufstocken."
    if regime in {"Defensiv", "Schutz"}:
        add_again_condition = "Nur bei Rückkehr über Pivot/21-MA und stabilem Marktumfeld erneut aufstocken."

    all_signals = [*killer_signals, *tranche_signals, *watch_signals]
    if killer_signals:
        explanation = f"{killer_signals[0].label}: kompletter Verkauf erforderlich."
    elif recommendation_percent > 0:
        explanation = f"{len(tranche_signals)} aktive Tranche-Signale ergeben Zielverkauf {target_total}%; bereits verkauft {already_sold:.0f}%; jetzt zusätzlich {recommendation_percent}% verkaufen."
    elif already_sold >= target_total and target_total > 0:
        explanation = "Die aktuell notwendige Verkaufsquote wurde bereits durch frühere Tranchen erreicht."
    elif watch_signals:
        explanation = f"Keine Verkaufstranche, aber {len(watch_signals)} Watch-Signal(e) beobachten."
    else:
        explanation = "Keine aktiven Verkaufsregeln. Position halten."

    book_references = {sig.id: sig.book_reference for sig in all_signals if sig.book_reference}
    tranche_styles = {sig.sell_style for sig in tranche_signals if sig.sell_style}
    if recommendation_percent <= 0:
        sell_mode_summary = "Keine neue Verkaufstranche"
        sell_style_summary = ""
    elif killer_signals or negative_pnl:
        sell_mode_summary = defensive_mode
        sell_style_summary = LOSS_LIMIT_STYLE
    elif STRENGTH_DEFENSIVE_STYLE in tranche_styles or drawdown >= 5:
        sell_mode_summary = strength_defensive_mode
        sell_style_summary = STRENGTH_DEFENSIVE_STYLE
    else:
        sell_mode_summary = strength_offensive_mode
        sell_style_summary = STRENGTH_OFFENSIVE_STYLE

    return {
        "recommendation_percent": int(recommendation_percent),
        "recommendation_label": label,
        "regime": regime,
        "killer_signals": [sig.to_dict() for sig in killer_signals],
        "tranche_signals": [sig.to_dict() for sig in tranche_signals],
        "watch_signals": [sig.to_dict() for sig in watch_signals],
        "stop_price": stop_price,
        "next_tranche_trigger_price": next_tranche_trigger,
        "full_exit_price": full_exit,
        "add_again_condition": add_again_condition,
        "explanation_short": explanation,
        "book_references": book_references,
        "target_total_sold_percent": int(target_total),
        "already_sold_percent": already_sold,
        "sell_now_percent": int(sell_now),
        "remaining_after_sale_percent": remaining_after_sale,
        "sell_mode": sell_mode_summary,
        "sell_style": sell_style_summary,
    }
