"""UI-independent rule engine for sell-decision recommendations."""

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any

import math


ALLOWED_RECOMMENDATION_LEVELS = [0, 25, 33, 50, 66, 75, 100]
BEARISH_MARKET_LABEL = "Bärisch"

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
    "stall_days_near_breakout": ("warning_stall_days", 25, "Stau-Tage nahe Ausbruchspunkt"),
    "stau_tage_nahe_ausbruchspunkt": ("warning_stall_days", 25, "Stau-Tage nahe Ausbruchspunkt"),
    "low_closes": ("warning_low_closes", 15, "Mehrere Schlusskurse nahe Tagestief"),
    "mehrere_schlusskurse_nahe_tagestief": ("warning_low_closes", 15, "Mehrere Schlusskurse nahe Tagestief"),
    "negative_market_divergence": ("warning_negative_divergence", 20, "Negative Divergenz zum Markt"),
    "negative_divergenz_zum_markt": ("warning_negative_divergence", 20, "Negative Divergenz zum Markt"),
    "weak_rebounds": ("warning_weak_rebounds", 15, "Schwache Erholungsversuche"),
    "schwache_erholungsversuche": ("warning_weak_rebounds", 15, "Schwache Erholungsversuche"),
    "downside_reversal_near_high": ("warning_downside_reversal", 25, "Downside Reversal nahe Hoch"),
    "downside_reversal_nahe_hoch": ("warning_downside_reversal", 25, "Downside Reversal nahe Hoch"),
    "lower_lows_no_rebound": ("warning_lower_lows", 25, "Drei bis vier tiefere Tiefs ohne Rebound"),
    "drei_bis_vier_tiefere_tiefs_ohne_rebound": ("warning_lower_lows", 25, "Drei bis vier tiefere Tiefs ohne Rebound"),
}


@dataclass(frozen=True)
class RuleSignal:
    id: str
    label: str
    contribution_percent: int = 0
    book_reference: str = ""

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


def _signal(signal_id: str, label: str, contribution: int = 0) -> RuleSignal:
    return RuleSignal(signal_id, label, int(contribution), BOOK_REFERENCES.get(signal_id, ""))


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


def evaluate_sell_decision(metrics_payload: dict, manual_data: dict | None = None, tranche_log: list[dict] | None = None) -> dict[str, Any]:
    """Evaluate the pure sell-decision rules without UI dependencies."""
    manual_data = manual_data or {}
    metrics, ticker, buy_price, _shares = _extract_inputs(metrics_payload or {})
    if not ticker and isinstance(manual_data, dict):
        ticker = str(manual_data.get("ticker") or "").upper().strip()
    market_environment = str(manual_data.get("market_environment") or "Unsicher").strip() or "Unsicher"
    industry_group_status = str(manual_data.get("industry_group_status") or "Neutral").strip() or "Neutral"
    pnl = _metric(metrics, "pnl_pct", 0.0) or 0.0
    current = _metric(metrics, "current_price")
    volume_ratio = _metric(metrics, "volume_ratio_50")
    regime = _regime(pnl, market_environment)
    is_bearish = market_environment == BEARISH_MARKET_LABEL
    positive_pnl = pnl > 0
    negative_pnl = pnl < 0

    low_day_1 = _safe_float(_manual_value(manual_data, metrics_payload or {}, "low_day_1"))
    low_day_0 = _safe_float(_manual_value(manual_data, metrics_payload or {}, "low_day_0"))
    sma21 = _metric(metrics, "sma21")
    sma50 = _metric(metrics, "sma50")
    sma200 = _metric(metrics, "sma200")
    rs_line = _metric(metrics, "rs_line")
    rs_ma50 = _metric(metrics, "rs_ma50")
    price_vs_sma50_pct = _metric(metrics, "price_vs_sma50_pct")
    price_vs_sma200_pct = _metric(metrics, "price_vs_sma200_pct")
    if price_vs_sma50_pct is None and current and sma50:
        price_vs_sma50_pct = (current / sma50 - 1) * 100
    if price_vs_sma200_pct is None and current and sma200:
        price_vs_sma200_pct = (current / sma200 - 1) * 100

    killer_signals: list[RuleSignal] = []
    tranche_signals: list[RuleSignal] = []
    watch_signals: list[RuleSignal] = []

    if pnl <= -7:
        killer_signals.append(_signal("killer_loss_7", "7%-Notbremse", 100))
    if is_bearish and pnl <= -5:
        killer_signals.append(_signal("killer_bear_loss_5", "Bärenmarkt-Notbremse", 100))
    if current is not None and sma200 and current < sma200 and volume_ratio is not None and volume_ratio > 1.5:
        killer_signals.append(_signal("killer_sma200_volume", "200-MA-Bruch mit Volumen", 100))
    if current is not None and low_day_1 and current < low_day_1 and volume_ratio is not None and volume_ratio > 1.2 and negative_pnl:
        killer_signals.append(_signal("killer_breakout_failed_volume", "Ausbruch ohne Kraft mit Volumen-Rückfall", 100))
    if _safe_bool(metrics.get("three_loss_weeks_rising_volume")) or (_metric(metrics, "consecutive_loss_weeks_rising_volume", 0) or 0) >= 3:
        killer_signals.append(_signal("killer_three_loss_weeks_rising_volume", "Drei Verlustwochen mit steigendem Wochenvolumen", 100))
    if (_metric(metrics, "weekly_closes_under_10w", 0) or 0) >= 8:
        killer_signals.append(_signal("killer_eight_weeks_under_10w", "Acht oder mehr Wochenschlüsse unter 10-Wochen-Linie", 100))

    if current is not None and low_day_1 and current < low_day_1 and negative_pnl:
        tranche_signals.append(_signal("tranche_low_day_1_loss", "Schluss unter Tief Tag 1 im Verlustfall", 33))
    if current is not None and low_day_0 and current < low_day_0 and negative_pnl:
        tranche_signals.append(_signal("tranche_low_day_0_loss", "Schluss unter Tief Tag 0 im Verlustfall", 33))
    days_under_sma21 = int(_metric(metrics, "days_under_sma21", 0) or 0)
    if positive_pnl and days_under_sma21 >= 1:
        tranche_signals.append(_signal("tranche_first_sma21_break_gain", "Erstmaliger Bruch der 21-MA im Gewinnfall", 25))
    if positive_pnl and days_under_sma21 >= 3:
        tranche_signals.append(_signal("tranche_three_days_under_sma21_gain", "Drei Tage in Folge unter 21-MA im Gewinnfall", 25))
    if current is not None and sma50 and current < sma50 * 0.98 and volume_ratio is not None and volume_ratio > 1.2:
        tranche_signals.append(_signal("tranche_sma50_break_volume", "Bruch der 50-MA mit mindestens 2% Abstand und erhöhtem Volumen", 50))

    already_sold = _sum_already_sold(ticker, tranche_log)
    no_tranche_realized = already_sold <= 0
    profit_zone_threshold = 10 if is_bearish else 20
    if pnl >= profit_zone_threshold and pnl <= 25 and no_tranche_realized:
        tranche_signals.append(_signal("tranche_profit_zone_20_25", "Erreichen der Gewinnmitnahme-Zone ohne realisierte Tranche", 33))

    drawdown = abs(_metric(metrics, "drawdown_from_high_since_buy_pct", 0.0) or 0.0)
    if positive_pnl and drawdown >= 15:
        tranche_signals.append(_signal("tranche_drawdown_15_full", "Drawdown vom Peak >= 15% bei positivem P&L", 100))
    else:
        if positive_pnl and drawdown >= 8:
            tranche_signals.append(_signal("tranche_drawdown_8", "Drawdown vom Peak >= 8% bei positivem P&L", 25))
        if positive_pnl and drawdown >= 12:
            tranche_signals.append(_signal("tranche_drawdown_12", "Drawdown vom Peak >= 12% bei positivem P&L", 25))

    if positive_pnl and price_vs_sma50_pct is not None and price_vs_sma50_pct > 25:
        tranche_signals.append(_signal("tranche_extended_sma50", "Kurs mehr als 25% über der 50-MA", 33))
    if positive_pnl and price_vs_sma200_pct is not None and price_vs_sma200_pct > 70:
        tranche_signals.append(_signal("tranche_extended_sma200", "Kurs mehr als 70% über der 200-MA", 33))

    days_under_rs21 = int(_metric(metrics, "days_under_rs_ma21", 0) or 0)
    if days_under_rs21 >= 1:
        tranche_signals.append(_signal("tranche_rs_first_21_break", "RS-Linie bricht erstmalig ihren 21-MA", 20))
    if days_under_rs21 >= 3:
        tranche_signals.append(_signal("tranche_rs_three_days_under_21", "RS-Linie schließt drei Tage in Folge unter 21-MA", 30))
    if rs_line is not None and rs_ma50 is not None and rs_line < rs_ma50:
        tranche_signals.append(_signal("tranche_rs_50_break", "RS-Linie bricht ihren 50-MA", 50))

    if positive_pnl and pnl > 15 and _safe_bool(metrics.get("worst_day_loss_high_volume")):
        tranche_signals.append(_signal("tranche_worst_day_high_volume", "Größter Tagesverlust seit Einstieg bei erhöhtem Volumen", 33))
    if _safe_bool(manual_data.get("personality_changed")):
        tranche_signals.append(_signal("tranche_personality_changed", "Persönlichkeits-Check angekreuzt", 25))
    if industry_group_status == "Schwach" and pnl > 10:
        tranche_signals.append(_signal("tranche_weak_industry_gain", "Industriegruppe schwach und P&L > 10%", 33))

    for raw_key, active in (manual_data.get("warning_checkboxes") or {}).items():
        if not _safe_bool(active):
            continue
        mapped = WARNING_CONTRIBUTIONS.get(_norm_key(raw_key))
        if mapped:
            signal_id, contribution, label = mapped
            tranche_signals.append(_signal(signal_id, label, contribution))

    if positive_pnl and 1 <= days_under_sma21 <= 2:
        watch_signals.append(_signal("watch_under_sma21_1_2", "Ein bis zwei Tage unter 21-MA bei positivem P&L"))
    if positive_pnl and 5 <= drawdown < 8:
        watch_signals.append(_signal("watch_drawdown_5_8", "Drawdown 5-8% vom Peak bei positivem P&L"))
    if (_metric(metrics, "up_down_volume_ratio_50") is not None) and (_metric(metrics, "up_down_volume_ratio_50") < 1.0):
        watch_signals.append(_signal("watch_up_down_volume", "Up/Down-Volume-Ratio < 1.0"))
    if (_metric(metrics, "distribution_days_25", 0) or 0) >= 4:
        watch_signals.append(_signal("watch_distribution_days", "Vier oder mehr Distribution-Tage in 25 Sessions"))

    if killer_signals:
        target_total = 100
    else:
        contribution_sum = sum(sig.contribution_percent for sig in tranche_signals)
        target_total = _floor_allowed(contribution_sum)
        if len(tranche_signals) >= 4:
            target_total = max(target_total, 75)
        if is_bearish and target_total < 100:
            target_total = _next_allowed(target_total)

    # Avoid recommending already executed tranches again. Killer signals mean sell all remaining shares.
    if killer_signals and already_sold < 100:
        sell_now = 100
        recommendation_percent = 100
        remaining_after_sale = 0.0
    else:
        sell_now_raw = max(0.0, target_total - already_sold)
        sell_now = _floor_allowed(sell_now_raw)
        recommendation_percent = sell_now
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
        explanation = f"{len(tranche_signals)} aktive Tranche-Signale ergeben Zielverkauf {target_total}%; jetzt {recommendation_percent}% verkaufen."
    elif watch_signals:
        explanation = f"Keine Verkaufstranche, aber {len(watch_signals)} Watch-Signal(e) beobachten."
    else:
        explanation = "Keine aktiven Verkaufsregeln. Position halten."

    book_references = {sig.id: sig.book_reference for sig in all_signals if sig.book_reference}
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
    }
