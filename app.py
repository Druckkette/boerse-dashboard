"""Main Streamlit app orchestrating modularized UI components and pages."""

import hashlib
import html
from decimal import Decimal, ROUND_HALF_UP
import hmac
import io
import json
import logging
import os
import re
import sqlite3
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from ftplib import FTP
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st
import yfinance as yf
from plotly.subplots import make_subplots

try:
    import psycopg2
    from psycopg2.extras import execute_values
except Exception:
    psycopg2 = None
    execute_values = None

from sell_decision_metrics import (
    build_sell_decision_metrics_payload,
    build_sell_decision_metrics_smoke_inputs,
)
import sell_decision_rules
from sell_decision_rules import (
    LM_HUB_DEFAULTS,
    LM_HUB_PROFILES,
    LM_HUB_PROFILE_DEFAULT,
    LM_HUB_STRATEGIES_ALL,
    LM_HUB_STRATEGIES_DEFAULT,
    LM_HUB_STRATEGIEN,
    LM_HUB_WARNUNGEN,
    LM_HUB_STRATEGIEN_DEFAULT,
    LM_HUB_WARNUNGEN_DEFAULT,
)
from sell_strategies import STRATEGIE_INFO
from sell_strategies import Position, verkaufs_empfehlung_gesamt, diagnose_strategie_kein_signal
from ui.charts import CHART_COLORS, apply_consistent_layout
from ui.tables import flow_column_config, performance_column_config, rating_overview_column_config
from ui.theme import APP_CSS, PAGE_CONFIG


def evaluate_sell_decision(metrics_payload: dict, manual_data: dict | None = None, tranche_log: list[dict] | None = None, recommendation_state: dict | None = None) -> dict:
    """Delegate sell decisions to the rules module without importing optional names directly."""
    return sell_decision_rules.evaluate_sell_decision(metrics_payload, manual_data, tranche_log, recommendation_state=recommendation_state)


def compute_sell_health_score(metrics_payload: dict, manual_data: dict | None = None) -> dict:
    """Return the engine health score, with a compatibility fallback for older deployed rules modules."""
    engine_fn = getattr(sell_decision_rules, "compute_sell_health_score", None)
    if callable(engine_fn):
        return engine_fn(metrics_payload, manual_data)
    return _compute_sell_health_score_fallback(metrics_payload, manual_data)


def _fallback_float(value, default=0.0):
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    if np.isnan(parsed):
        return default
    return parsed


def _compute_sell_health_score_fallback(metrics_payload: dict, manual_data: dict | None = None) -> dict:
    metrics = metrics_payload.get("metrics") if isinstance(metrics_payload, dict) and isinstance(metrics_payload.get("metrics"), dict) else (metrics_payload or {})
    score = 50.0
    reasons = []
    pnl = _fallback_float(metrics.get("pnl_pct"), 0.0)
    drawdown = abs(_fallback_float(metrics.get("drawdown_from_high_pct"), 0.0))
    days_under_21 = _fallback_float(metrics.get("days_under_ema21"), 0.0)
    rs_slope = _fallback_float(metrics.get("rs_slope_21"), 0.0)
    if pnl >= 20:
        score += 12
        reasons.append("Gewinnpuffer vorhanden")
    elif pnl <= -7:
        score -= 35
        reasons.append("7%-Verlustgrenze überschritten")
    elif pnl < 0:
        score -= 12
        reasons.append("Position im Verlust")
    if drawdown >= 15:
        score -= 22
        reasons.append("Drawdown vom Hoch kritisch")
    elif drawdown >= 8:
        score -= 12
        reasons.append("Drawdown vom Hoch erhöht")
    if days_under_21 >= 3:
        score -= 14
        reasons.append("Mehrere Tage unter 21-EMA")
    elif days_under_21 >= 1:
        score -= 6
        reasons.append("Kurz unter 21-EMA")
    if rs_slope > 0:
        score += 8
        rs_trend = "steigend"
    elif rs_slope < 0:
        score -= 8
        rs_trend = "fallend"
        reasons.append("Relative Stärke fällt")
    else:
        rs_trend = "seitwärts"
    score = max(0.0, min(100.0, score))
    if score >= 70:
        status = "Gesund"
    elif score >= 40:
        status = "Beobachten"
    else:
        status = "Verkaufskandidat"
    return {"health_score": round(score, 1), "status": status, "rs_trend": rs_trend, "reasons": reasons}


def configure_page() -> None:
    st.set_page_config(**PAGE_CONFIG)


WORKSPACE_FILE = "user_workspace.json"
WORKSPACE_SCOPE_DEFAULT = "default"
DEFAULT_FAVORITES = ["NVDA", "MSFT", "AAPL", "META", "AMZN", "PLTR", "LLY", "TSLA"]
RS_SOURCE_CSV_LATEST = "csv_latest"
RS_SOURCE_FRED_CSV = "csv_fred"
RS_SOURCE_COMPUTED = "computed"
RS_SOURCE_LABELS = {
    RS_SOURCE_CSV_LATEST: "Eigene CSV aus diesem Repo (Standard)",
    RS_SOURCE_FRED_CSV: "Fred RS-Rating CSV (rs-log)",
    RS_SOURCE_COMPUTED: "DB / Live-Berechnung",
}
RS_OUTPUT_DIR_NAME = "output"
RS_OUTPUT_FILE_NAME = "rs_stocks.csv"
INSTITUTIONAL_13F_OUTPUT_FILE_NAME = "institutional_13f_trends.json"
FRED_RS_CSV_URL = "https://raw.githubusercontent.com/Fred6725/rs-log/main/output/rs_stocks.csv"
METRIC_GLOSSARY = {
    "Dist.-Tage": "Verkaufstage mit höherem Volumen als am Vortag. Viele Distribution Days sprechen für institutionellen Abgabedruck.",
    "21-EMA": "Abstand zur 21-EMA in ATR. Je weiter der Index darüber liegt, desto eher ist er kurzfristig überdehnt.",
    "50-SMA": "Prozentualer Abstand zur 50-Tage-Linie. Sehr große positive Abstände können Überhitzung anzeigen.",
    "Drawdown": "Abstand zum 52-Wochen-Hoch. Große negative Werte zeigen eine laufende Korrektur oder Schwächephase.",
    "Closing Range": "Wo der Schluss im Tagesbereich liegt. Hohe Werte bedeuten einen starken Schluss nahe Tageshoch.",
    "ATR (21T)": "Durchschnittliche Schwankungsbreite der letzten 21 Tage in Prozent. Hilft bei Risiko und Positionsgröße.",
    "Beta": "Empfindlichkeit der Aktie gegenüber dem Gesamtmarkt. Werte über 1 bedeuten meist mehr Dynamik, aber auch mehr Schwankung.",
    "McClellan Osc.": "RANA-McClellan: EMA(19)−EMA(39) der ratio-adjustierten Net Advances (×1000). Zonen: 0 = Regimewechsel · ±30 = neutrales Rauschen · ±50 = ernstzunehmender Breiten-Impuls · ±80 = überdehnte Breite · ±125 = Extremzustand.",
    "NH/NL Ratio": "Verhältnis neuer 52-Wochen-Hochs zu neuen 52-Wochen-Tiefs. Über 1 zeigt breite Stärke.",
    "% > 50-SMA": "Anteil der Aktien oberhalb ihrer 50-Tage-Linie. Zeigt, wie breit kurzfristige Trends sind.",
    "% > 200-SMA": "Anteil der Aktien oberhalb ihrer 200-Tage-Linie. Zeigt die langfristige Marktverfassung.",
    "Deemer Ratio": "Advancing Volume geteilt durch Declining Volume. Werte über 1.97 gelten als seltener Breitenschub.",
    "RS-Linie": "Kurs der Aktie geteilt durch den S&P 500. Eine steigende RS-Linie zeigt Outperformance gegenüber dem Markt.",
    "RS-Rating": "Perzentil-Rang der relativen Stärke gegenüber dem geladenen Aktienuniversum. Hohe Werte stehen für Marktführerschaft.",
}

def inject_css() -> None:
    st.markdown(APP_CSS, unsafe_allow_html=True)


def inject_workspace_css():
    st.markdown("""
        <style>
        .ws-card { background: #ffffff; border: 0.5px solid rgba(0,0,0,0.12); border-radius: 12px; padding: 14px 16px; margin-bottom: 12px; }
        .ws-card-muted { background: #f5f4ef; border-radius: 8px; padding: 12px 14px; }
        .ws-label { font-size: 12px; font-weight: 500; color: #5f5e5a; letter-spacing: 0.4px; text-transform: uppercase; margin-bottom: 8px; }
        .ws-kpi { background: #f5f4ef; border-radius: 8px; padding: 12px 14px; }
        .ws-kpi-label { font-size: 12px; color: #5f5e5a; margin-bottom: 4px; }
        .ws-kpi-value { font-size: 22px; font-weight: 500; line-height: 1.2; }
        .ws-kpi-value.success { color: #0f6e56; }
        .ws-kpi-value.danger { color: #a32d2d; }
        .ws-pill { display: inline-flex; align-items: center; gap: 4px; font-size: 12px; font-weight: 500; padding: 4px 10px; border: 0.5px solid rgba(0,0,0,0.12); border-radius: 999px; font-family: 'SF Mono', Monaco, Consolas, monospace; margin: 0 4px 6px 0; }
        .ws-badge { display: inline-block; font-size: 11px; padding: 2px 8px; border-radius: 999px; font-weight: 500; }
        .ws-badge.ok { background: #e1f5ee; color: #0f6e56; }
        .ws-badge.stop { background: #fcebeb; color: #a32d2d; }
        .ws-hero { background: #e6f1fb; border-radius: 12px; padding: 18px 20px; margin-bottom: 12px; }
        .ws-hero-label { font-size: 11px; font-weight: 500; color: #0c447c; letter-spacing: 0.5px; margin-bottom: 6px; }
        .ws-hero-value { font-size: 44px; font-weight: 500; color: #0c447c; line-height: 1; }
        .ws-hero-sub { font-size: 12px; color: #185fa5; margin-top: 6px; }
        .ws-hint { background: #faeeda; color: #854f0b; border-radius: 8px; padding: 10px 12px; font-size: 12px; }
        .ws-positions-table tr.stop-row td { background: rgba(226, 75, 74, 0.03); border-left: 3px solid #a32d2d; }
        .ws-positions-table td { padding: 10px 14px; font-size: 13px; border-top: 0.5px solid rgba(0,0,0,0.08); font-variant-numeric: tabular-nums; }
        .ws-positions-table th { padding: 10px 14px; font-size: 12px; font-weight: 500; color: #5f5e5a; background: #f5f4ef; text-align: left; }
        .ws-mono { font-family: 'SF Mono', Monaco, Consolas, monospace; font-weight: 500; }
        .ws-num-pos { color: #0f6e56; font-weight: 500; }
        .ws-num-neg { color: #a32d2d; font-weight: 500; }
        .stButton > button[kind="primary"] { background: #e6f1fb; color: #0c447c; border: 0.5px solid #185fa5; font-weight: 500; }
        div[data-testid="column"] .stButton > button { width: 100%; font-size: 12px; padding: 4px 10px; min-height: 32px; }
        @media (prefers-color-scheme: dark) {
            .ws-card { background: #161616; border-color: rgba(255,255,255,0.16); }
            .ws-card-muted, .ws-kpi, .ws-positions-table th { background: #262520; }
            .ws-label, .ws-kpi-label { color: #b8b5aa; }
            .ws-pill { border-color: rgba(255,255,255,0.18); }
        }
        </style>
    """, unsafe_allow_html=True)

def _safe_json_load(path: Path, default):
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.debug("workspace load failed: %s", exc)
    return default

def _safe_json_dump(path: Path, payload) -> None:
    try:
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as exc:
        logger.debug("workspace save failed: %s", exc)

def _workspace_scope() -> str:
    try:
        scope = _safe_get_secret("private_area", "workspace_key", default="")
    except Exception:
        scope = ""
    return str(scope or WORKSPACE_SCOPE_DEFAULT).strip() or WORKSPACE_SCOPE_DEFAULT

def _workspace_meta_key(name: str) -> str:
    return f"workspace:{_workspace_scope()}:{name}"

def _workspace_backend_label() -> str:
    try:
        return _get_store_label(_get_price_store())
    except Exception:
        return "lokaler Speicher"

def _ensure_workspace_store_ready() -> None:
    if st.session_state.get("_workspace_store_ready"):
        return
    try:
        _init_price_cache_db(_get_price_store())
        st.session_state["_workspace_store_ready"] = True
    except Exception as exc:
        logger.debug("workspace store init failed: %s", exc)

def _default_portfolio_settings():
    return {
        "cash_balance": 0.0,
        "risk_per_position_pct": 1.0,
        "target_risk_contribution": 0.20,
        "max_depot_loss_low": 8.0,
        "max_depot_loss_high": 12.0,
        "curve_start_date": "",
        "rs_rating_source": RS_SOURCE_CSV_LATEST,
        "db_backend_preference": "sqlite",
        "neon_auto_update_preference": "on",
        "position_monitor_enabled": False,
        "position_monitor_threshold_atr": 1.5,
        "position_monitor_reference": "high_since_buy",
        "position_monitor_atr_period": 14,
        "position_monitor_lookback_days": 420,
        "position_monitor_interval_minutes": 5,
        "position_monitor_cooldown_hours": 18,
        "position_monitor_pushover_user_key": "",
        "position_monitor_pushover_app_token": "",
    }

POSITION_MONITOR_STATE_FIELD = "position_monitor_state"
POSITION_MONITOR_REFERENCE_LABELS = {
    "high_since_buy": "Vom Hoch seit Kauf",
    "entry": "Vom Einstand",
    "both": "Beides",
}

SELL_DECISION_MARKET_ENVIRONMENTS = {"Bullisch", "Unsicher", "Bärisch"}
SELL_DECISION_INDUSTRY_GROUP_STATUSES = {"Stark", "Neutral", "Schwach"}
SELL_DECISION_STATE_KEY = "sell_decision_state"
DEPOT_CURVE_CSV_IMPORT_KEY = "depot_curve_csv_import"


def _default_depot_curve_csv_import_state() -> dict:
    return {
        "records": [],
        "filename": "",
        "row_count": 0,
        "created_at": "",
        "updated_at": "",
        "last_import_summary": {},
        "isin_overrides": {},
    }


def _default_position_manual_sell_data(ticker: str = "") -> dict:
    return {
        "ticker": _normalize_single_ticker(ticker),
        "pivot": None,
        "low_day_1": None,
        "low_day_0": None,
        "market_environment": "Unsicher",
        "industry_group_status": "Neutral",
        "personality_changed": False,
        "strength_checkboxes": {},
        "warning_checkboxes": {},
        "sell_setup": {},
    }


def _default_sell_decision_state() -> dict:
    return {
        "positions_manual": {},
        "tranche_log": [],
        "closed_trades": [],
        "post_mortem_log": [],
        # Hysterese- und Snooze-Status pro Ticker (ticker → dict).
        "recommendation_state": {},
    }


def _normalize_recommendation_state_entry(entry) -> dict:
    if not isinstance(entry, dict):
        return {"last_seen_date": "", "last_pct": 0, "consecutive_days": 0, "snoozed_until": "", "snoozed_pct": 0}
    return {
        "last_seen_date": _normalize_date_string(entry.get("last_seen_date")),
        "last_pct": int(_safe_optional_float(entry.get("last_pct")) or 0),
        "consecutive_days": int(_safe_optional_float(entry.get("consecutive_days")) or 0),
        "snoozed_until": _normalize_date_string(entry.get("snoozed_until")),
        "snoozed_pct": int(_safe_optional_float(entry.get("snoozed_pct")) or 0),
    }


def _safe_optional_float(value):
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if np.isnan(parsed):
        return None
    return parsed


def _safe_bool(value) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "ja", "on"}
    return bool(value)


def _normalize_checkbox_map(raw) -> dict:
    if not isinstance(raw, dict):
        return {}
    normalized = {}
    for key, value in raw.items():
        clean_key = str(key or "").strip()
        if clean_key:
            normalized[clean_key] = _safe_bool(value)
    return normalized


def _normalize_position_manual_sell_data(ticker: str, data) -> dict:
    base = _default_position_manual_sell_data(ticker)
    if not isinstance(data, dict):
        return base
    raw_ticker = data.get("ticker") or ticker
    base["ticker"] = _normalize_single_ticker(raw_ticker)
    base["pivot"] = _safe_optional_float(data.get("pivot"))
    base["low_day_1"] = _safe_optional_float(data.get("low_day_1"))
    base["low_day_0"] = _safe_optional_float(data.get("low_day_0"))
    market_env = str(data.get("market_environment") or base["market_environment"]).strip()
    base["market_environment"] = market_env if market_env in SELL_DECISION_MARKET_ENVIRONMENTS else "Unsicher"
    group_status = str(data.get("industry_group_status") or base["industry_group_status"]).strip()
    base["industry_group_status"] = group_status if group_status in SELL_DECISION_INDUSTRY_GROUP_STATUSES else "Neutral"
    base["personality_changed"] = _safe_bool(data.get("personality_changed", False))
    base["strength_checkboxes"] = _normalize_checkbox_map(data.get("strength_checkboxes"))
    base["warning_checkboxes"] = _normalize_checkbox_map(data.get("warning_checkboxes"))
    setup_raw = data.get("sell_setup")
    if isinstance(setup_raw, dict):
        # Setup wird unverändert durchgereicht (Profile, Tranche-Parameter, Schwellen).
        # Nur Timestamp-Werte werden zu Strings normalisiert, damit das State serialisierbar bleibt.
        clean_setup = {}
        for key, value in setup_raw.items():
            if isinstance(value, pd.Timestamp):
                clean_setup[str(key)] = value.strftime("%Y-%m-%d")
            else:
                clean_setup[str(key)] = value
        base["sell_setup"] = clean_setup
    return base


def _normalize_sell_decision_state(raw) -> dict:
    state = _default_sell_decision_state()
    if not isinstance(raw, dict):
        return state

    manual_source = raw.get("positions_manual", {})
    if isinstance(manual_source, list):
        manual_source = {item.get("ticker", ""): item for item in manual_source if isinstance(item, dict)}
    if isinstance(manual_source, dict):
        for key, data in manual_source.items():
            ticker = _normalize_single_ticker((data or {}).get("ticker") if isinstance(data, dict) else key) or _normalize_single_ticker(key)
            if not ticker:
                continue
            normalized = _normalize_position_manual_sell_data(ticker, data)
            normalized["ticker"] = ticker
            state["positions_manual"][ticker] = normalized

    state["tranche_log"] = [_normalize_tranche_log_entry(entry) for entry in raw.get("tranche_log", []) if isinstance(entry, dict)]
    state["tranche_log"] = [entry for entry in state["tranche_log"] if entry.get("ticker")]
    state["closed_trades"] = [_normalize_closed_trade_entry(entry) for entry in raw.get("closed_trades", []) if isinstance(entry, dict)]
    state["closed_trades"] = [entry for entry in state["closed_trades"] if entry.get("ticker")]
    state["post_mortem_log"] = [_normalize_post_mortem_entry(entry) for entry in raw.get("post_mortem_log", []) if isinstance(entry, dict)]
    state["post_mortem_log"] = [entry for entry in state["post_mortem_log"] if entry.get("ticker")]

    rec_source = raw.get("recommendation_state", {})
    if isinstance(rec_source, dict):
        for ticker_key, entry in rec_source.items():
            ticker = _normalize_single_ticker(ticker_key)
            if not ticker:
                continue
            state["recommendation_state"][ticker] = _normalize_recommendation_state_entry(entry)
    return state


def _normalize_date_string(value) -> str:
    if value is None:
        return ""
    if isinstance(value, str) and not value.strip():
        return ""
    try:
        parsed = pd.Timestamp(value)
    except Exception:
        return str(value or "").strip()
    if pd.isna(parsed):
        return ""
    return parsed.strftime("%Y-%m-%d")


def _normalize_tranche_log_entry(entry: dict) -> dict:
    ticker = _normalize_single_ticker(entry.get("ticker", ""))
    return {
        "date": _normalize_date_string(entry.get("date")),
        "ticker": ticker,
        "tranche_percent": _safe_optional_float(entry.get("tranche_percent")),
        "price": _safe_optional_float(entry.get("price")),
        "shares_sold": _safe_optional_float(entry.get("shares_sold")),
        "trigger_signal": str(entry.get("trigger_signal") or "").strip(),
        "notes": str(entry.get("notes") or "").strip(),
    }


def _normalize_closed_trade_entry(entry: dict) -> dict:
    ticker = _normalize_single_ticker(entry.get("ticker", ""))
    buy_price = _safe_optional_float(entry.get("buy_price"))
    sell_price = _safe_optional_float(entry.get("sell_price"))
    realized = _safe_optional_float(entry.get("realized_pnl_percent"))
    if realized is None and buy_price and sell_price:
        realized = (float(sell_price) / float(buy_price) - 1) * 100
    return {
        "id": str(entry.get("id") or uuid.uuid4()).strip(),
        "source": str(entry.get("source") or "depot_sell_booking").strip(),
        "booked_at": _normalize_date_string(entry.get("booked_at") or entry.get("analysis_date")),
        "ticker": ticker,
        "buy_date": _normalize_date_string(entry.get("buy_date")),
        "buy_price": buy_price,
        "buy_currency": str(entry.get("buy_currency") or entry.get("currency") or "").strip().upper(),
        "sell_date": _normalize_date_string(entry.get("sell_date") or entry.get("date")),
        "sell_price": sell_price,
        "sell_currency": str(entry.get("sell_currency") or "").strip().upper(),
        "shares": _safe_optional_float(entry.get("shares") or entry.get("shares_sold")),
        "pivot": _safe_optional_float(entry.get("pivot")),
        "planned_stop": _safe_optional_float(entry.get("planned_stop") or entry.get("stop_price")),
        "realized_pnl_percent": realized,
        "notes": str(entry.get("notes") or "").strip(),
    }


def _normalize_post_mortem_entry(entry: dict) -> dict:
    ticker = _normalize_single_ticker(entry.get("ticker", ""))
    return {
        "analysis_date": _normalize_date_string(entry.get("analysis_date")),
        "ticker": ticker,
        "buy_date": _normalize_date_string(entry.get("buy_date")),
        "buy_price": _safe_optional_float(entry.get("buy_price")),
        "sell_date": _normalize_date_string(entry.get("sell_date")),
        "sell_price": _safe_optional_float(entry.get("sell_price")),
        "shares": _safe_optional_float(entry.get("shares")),
        "pivot": _safe_optional_float(entry.get("pivot")),
        "planned_stop": _safe_optional_float(entry.get("planned_stop")),
        "realized_pnl_percent": _safe_optional_float(entry.get("realized_pnl_percent")),
        "max_gain_percent": _safe_optional_float(entry.get("max_gain_percent")),
        "max_drawdown_percent": _safe_optional_float(entry.get("max_drawdown_percent")),
        "holding_days": int(_safe_optional_float(entry.get("holding_days")) or 0),
        "positive_points_count": int(_safe_optional_float(entry.get("positive_points_count")) or 0),
        "error_count": int(_safe_optional_float(entry.get("error_count")) or 0),
        "verdict_class": str(entry.get("verdict_class") or "").strip(),
        "checkboxes": _normalize_checkbox_map(entry.get("checkboxes")),
        "lessons_learned": entry.get("lessons_learned") if isinstance(entry.get("lessons_learned"), list) else str(entry.get("lessons_learned") or "").strip(),
        "trade_data": entry.get("trade_data", {}) if isinstance(entry.get("trade_data", {}), dict) else {},
    }

def _normalize_depot_curve_import_summary(raw) -> dict:
    if not isinstance(raw, dict):
        raw = {}
    out = {}
    for key in ("added", "updated", "unchanged", "kept_missing", "total_before", "total_uploaded", "total_after"):
        try:
            out[key] = int(raw.get(key, 0) or 0)
        except Exception:
            out[key] = 0
    return out


def _normalize_depot_curve_csv_import_state(raw) -> dict:
    state = _default_depot_curve_csv_import_state()
    if not isinstance(raw, dict):
        return state
    records = raw.get("records", [])
    if isinstance(records, list):
        state["records"] = [row for row in records if isinstance(row, dict)]
    state["filename"] = str(raw.get("filename", "") or "")
    try:
        state["row_count"] = int(raw.get("row_count", len(state["records"])) or 0)
    except Exception:
        state["row_count"] = len(state["records"])
    state["created_at"] = str(raw.get("created_at", "") or "")
    state["updated_at"] = str(raw.get("updated_at", "") or "")
    state["last_import_summary"] = _normalize_depot_curve_import_summary(raw.get("last_import_summary", {}))
    overrides = raw.get("isin_overrides", {})
    if isinstance(overrides, dict):
        state["isin_overrides"] = {
            str(k).upper().strip(): _normalize_single_ticker(str(v))
            for k, v in overrides.items()
            if str(k or "").strip() and _normalize_single_ticker(str(v))
        }
    return state


_DEPOT_CURVE_IMPORT_KEY_COLUMNS = (
    "datetime", "date", "type", "asset_class", "name", "symbol",
    "shares_num", "price_num", "amount_num", "fee_num", "tax_num", "description",
)
_DEPOT_CURVE_IMPORT_HASH_COLUMNS = (
    "datetime", "date", "account_type", "category", "type", "asset_class", "name",
    "symbol", "shares", "shares_num", "price", "price_num", "amount", "amount_num",
    "fee", "fee_num", "tax", "tax_num", "currency", "original_amount",
    "original_currency", "fx_rate", "description", "transaction_id",
)


def _depot_curve_compare_value(value) -> str:
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (int, float, np.integer, np.floating)):
        try:
            return f"{float(value):.12g}"
        except Exception:
            return str(value)
    return str(value or "").strip()


def _depot_curve_row_signature(row, columns: tuple[str, ...]) -> str:
    return "|".join(f"{col}={_depot_curve_compare_value(row.get(col, ''))}" for col in columns)


def _depot_curve_transaction_base_key(row) -> str:
    tx_id = _depot_curve_compare_value(row.get("transaction_id", "")).strip()
    if tx_id:
        return f"id:{tx_id}"
    signature = _depot_curve_row_signature(row, _DEPOT_CURVE_IMPORT_KEY_COLUMNS)
    return "sig:" + hashlib.sha256(signature.encode("utf-8")).hexdigest()


def _normalize_depot_curve_csv_frame(df: pd.DataFrame | None) -> pd.DataFrame:
    if df is None or len(df) == 0:
        return pd.DataFrame()
    out = df.copy()
    for col in ("date", "datetime", "event_ts"):
        if col in out.columns:
            out[col] = pd.to_datetime(out[col], errors="coerce", utc=True).dt.tz_convert(None)
    if "date" in out.columns:
        out["date"] = out["date"].dt.normalize()
    if "event_ts" not in out.columns:
        out["event_ts"] = out["datetime"] if "datetime" in out.columns else out.get("date")
    event_source = out.get("event_ts")
    if event_source is None:
        event_source = pd.Series(pd.NaT, index=out.index)
    out["event_ts"] = pd.to_datetime(event_source, errors="coerce", utc=True).dt.tz_convert(None)
    if "date" in out.columns:
        out["event_ts"] = out["event_ts"].fillna(out["date"])
    for col in ("shares", "price", "amount", "fee", "tax", "shares_num", "price_num", "amount_num", "fee_num", "tax_num"):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    for raw, parsed in (("shares", "shares_num"), ("price", "price_num"), ("amount", "amount_num"), ("fee", "fee_num"), ("tax", "tax_num")):
        if parsed not in out.columns:
            source = out[raw] if raw in out.columns else pd.Series(0.0, index=out.index)
            out[parsed] = pd.to_numeric(source, errors="coerce").fillna(0.0)
        else:
            out[parsed] = pd.to_numeric(out[parsed], errors="coerce").fillna(0.0)
    for col in ("type", "asset_class", "symbol"):
        if col in out.columns:
            out[col] = out[col].astype(str).str.upper().str.strip()
    return out.sort_values(["event_ts"], kind="stable").reset_index(drop=True)


def _depot_curve_csv_records_to_frame(records: list[dict]) -> pd.DataFrame:
    if not isinstance(records, list) or not records:
        return pd.DataFrame()
    return _normalize_depot_curve_csv_frame(pd.DataFrame(records))


def _depot_curve_csv_frame_to_records(df: pd.DataFrame | None) -> list[dict]:
    if df is None or len(df) == 0:
        return []
    clean = _normalize_depot_curve_csv_frame(df)
    return json.loads(clean.to_json(orient="records", date_format="iso", date_unit="us"))


def _depot_curve_keyed_rows(df: pd.DataFrame | None) -> dict[str, dict]:
    normalized = _normalize_depot_curve_csv_frame(df)
    keyed: dict[str, dict] = {}
    duplicate_counts: dict[str, int] = {}
    if normalized.empty:
        return keyed
    for _, row in normalized.iterrows():
        base_key = _depot_curve_transaction_base_key(row)
        duplicate_index = duplicate_counts.get(base_key, 0)
        duplicate_counts[base_key] = duplicate_index + 1
        key = base_key if duplicate_index == 0 else f"{base_key}#{duplicate_index + 1}"
        row_hash = hashlib.sha256(
            _depot_curve_row_signature(row, _DEPOT_CURVE_IMPORT_HASH_COLUMNS).encode("utf-8")
        ).hexdigest()
        keyed[key] = {"row": row.to_dict(), "hash": row_hash}
    return keyed


def _merge_depot_curve_csv_import(existing_df: pd.DataFrame | None, uploaded_df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    existing = _depot_curve_keyed_rows(existing_df)
    uploaded = _depot_curve_keyed_rows(uploaded_df)
    merged = {key: dict(item["row"]) for key, item in existing.items()}
    summary = {
        "added": 0,
        "updated": 0,
        "unchanged": 0,
        "kept_missing": 0,
        "total_before": len(existing),
        "total_uploaded": len(uploaded),
        "total_after": 0,
    }
    for key, item in uploaded.items():
        if key not in existing:
            summary["added"] += 1
        elif item["hash"] != existing[key]["hash"]:
            summary["updated"] += 1
        else:
            summary["unchanged"] += 1
        merged[key] = dict(item["row"])
    summary["kept_missing"] = len(set(existing) - set(uploaded))
    merged_df = _normalize_depot_curve_csv_frame(pd.DataFrame(list(merged.values()))) if merged else pd.DataFrame()
    summary["total_after"] = int(len(merged_df))
    return merged_df, summary


def _build_depot_curve_csv_import_state(
    df: pd.DataFrame | None,
    *,
    filename: str = "",
    previous: dict | None = None,
    summary: dict | None = None,
    isin_overrides: dict | None = None,
) -> dict:
    previous_state = _normalize_depot_curve_csv_import_state(previous or {})
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    records = _depot_curve_csv_frame_to_records(df)
    return {
        "records": records,
        "filename": str(filename or previous_state.get("filename", "") or ""),
        "row_count": len(records),
        "created_at": previous_state.get("created_at") or now,
        "updated_at": now,
        "last_import_summary": _normalize_depot_curve_import_summary(summary or previous_state.get("last_import_summary", {})),
        "isin_overrides": (
            _normalize_depot_curve_csv_import_state({"isin_overrides": isin_overrides}).get("isin_overrides", {})
            if isin_overrides is not None
            else previous_state.get("isin_overrides", {})
        ),
    }


def _set_depot_curve_csv_import_state(state: dict) -> None:
    normalized = _normalize_depot_curve_csv_import_state(state)
    st.session_state[DEPOT_CURVE_CSV_IMPORT_KEY] = normalized
    st.session_state["pf_depot_curve_tx_df"] = _depot_curve_csv_records_to_frame(normalized.get("records", []))
    st.session_state["pf_depot_curve_tx_filename"] = normalized.get("filename", "")
    st.session_state["pf_depot_curve_isin_overrides"] = normalized.get("isin_overrides", {})
    _sync_workspace()


def _clear_depot_curve_csv_import_state() -> None:
    st.session_state[DEPOT_CURVE_CSV_IMPORT_KEY] = _default_depot_curve_csv_import_state()
    st.session_state.pop("pf_depot_curve_tx_df", None)
    st.session_state.pop("pf_depot_curve_tx_filename", None)
    st.session_state.pop("pf_depot_curve_isin_overrides", None)
    _sync_workspace()


def _workspace_payload():
    settings = dict(_default_portfolio_settings())
    raw_settings = st.session_state.get("portfolio_settings", {})
    if isinstance(raw_settings, dict):
        settings.update(raw_settings)
    return {
        "watchlist": list(dict.fromkeys(st.session_state.get("watchlist", []))),
        "recent_tickers": list(dict.fromkeys(st.session_state.get("recent_tickers", [])))[:12],
        "positions": st.session_state.get("positions", []),
        "todos": _normalize_workspace_todos(st.session_state.get("todos", [])),
        "portfolio_history": st.session_state.get("portfolio_history", []),
        "portfolio_cash_flows": st.session_state.get("portfolio_cash_flows", []),
        "portfolio_settings": settings,
        DEPOT_CURVE_CSV_IMPORT_KEY: _normalize_depot_curve_csv_import_state(st.session_state.get(DEPOT_CURVE_CSV_IMPORT_KEY, {})),
        SELL_DECISION_STATE_KEY: _normalize_sell_decision_state(st.session_state.get(SELL_DECISION_STATE_KEY, {})),
    }

def _load_workspace_from_store():
    _ensure_workspace_store_ready()
    payload = {}
    store = _get_price_store()
    defaults = {
        "watchlist": [],
        "recent_tickers": [],
        "positions": [],
        "todos": [],
        "portfolio_history": [],
        "portfolio_cash_flows": [],
        "portfolio_settings": _default_portfolio_settings(),
        DEPOT_CURVE_CSV_IMPORT_KEY: _default_depot_curve_csv_import_state(),
        SELL_DECISION_STATE_KEY: _default_sell_decision_state(),
    }
    meta_keys = {field: _workspace_meta_key(field) for field in defaults}
    raw_values = _get_cache_metadata_many(store, list(meta_keys.values()))
    for field, default in defaults.items():
        raw = raw_values.get(meta_keys[field], None)
        if raw in (None, ""):
            payload[field] = default
            continue
        try:
            payload[field] = json.loads(raw)
        except Exception as exc:
            logger.debug("workspace field decode failed for %s: %s", field, exc)
            payload[field] = default
    if not isinstance(payload.get("portfolio_settings"), dict):
        payload["portfolio_settings"] = _default_portfolio_settings()
    else:
        merged = dict(_default_portfolio_settings())
        merged.update(payload.get("portfolio_settings", {}))
        payload["portfolio_settings"] = merged
    if not isinstance(payload.get("portfolio_history"), list):
        payload["portfolio_history"] = []
    if not isinstance(payload.get("portfolio_cash_flows"), list):
        payload["portfolio_cash_flows"] = []
    payload[DEPOT_CURVE_CSV_IMPORT_KEY] = _normalize_depot_curve_csv_import_state(payload.get(DEPOT_CURVE_CSV_IMPORT_KEY, {}))
    payload[SELL_DECISION_STATE_KEY] = _normalize_sell_decision_state(payload.get(SELL_DECISION_STATE_KEY, {}))
    return payload

def _sync_workspace() -> None:
    payload = _workspace_payload()
    _safe_json_dump(Path(WORKSPACE_FILE), payload)
    try:
        _ensure_workspace_store_ready()
        store = _get_price_store()
        values = {
            _workspace_meta_key("watchlist"): json.dumps(payload["watchlist"], ensure_ascii=False),
            _workspace_meta_key("recent_tickers"): json.dumps(payload["recent_tickers"], ensure_ascii=False),
            _workspace_meta_key("positions"): json.dumps(payload["positions"], ensure_ascii=False),
            _workspace_meta_key("todos"): json.dumps(payload["todos"], ensure_ascii=False),
            _workspace_meta_key("portfolio_history"): json.dumps(payload["portfolio_history"], ensure_ascii=False),
            _workspace_meta_key("portfolio_cash_flows"): json.dumps(payload["portfolio_cash_flows"], ensure_ascii=False),
            _workspace_meta_key("portfolio_settings"): json.dumps(payload["portfolio_settings"], ensure_ascii=False),
            _workspace_meta_key(DEPOT_CURVE_CSV_IMPORT_KEY): json.dumps(payload[DEPOT_CURVE_CSV_IMPORT_KEY], ensure_ascii=False),
            _workspace_meta_key(SELL_DECISION_STATE_KEY): json.dumps(payload[SELL_DECISION_STATE_KEY], ensure_ascii=False),
            _workspace_meta_key("updated_at"): datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        }
        _set_cache_metadata_many(store, values)
    except Exception as exc:
        logger.debug("workspace sync to store failed: %s", exc)

def _init_workspace_state():
    if st.session_state.get("_workspace_initialized"):
        return
    stored = {}
    try:
        stored = _load_workspace_from_store()
    except Exception as exc:
        logger.debug("workspace store load failed: %s", exc)
    workspace_fields = [
        "watchlist", "recent_tickers", "positions", "todos", "portfolio_history",
        "portfolio_cash_flows", DEPOT_CURVE_CSV_IMPORT_KEY, SELL_DECISION_STATE_KEY,
    ]
    if not any(stored.get(k) for k in workspace_fields):
        local_stored = _safe_json_load(Path(WORKSPACE_FILE), {})
        if isinstance(local_stored, dict) and local_stored:
            stored = local_stored
            try:
                st.session_state["watchlist"] = stored.get("watchlist", [])
                st.session_state["recent_tickers"] = stored.get("recent_tickers", [])
                st.session_state["positions"] = stored.get("positions", [])
                st.session_state["todos"] = _normalize_workspace_todos(stored.get("todos", []))
                st.session_state["portfolio_history"] = stored.get("portfolio_history", [])
                st.session_state["portfolio_cash_flows"] = stored.get("portfolio_cash_flows", [])
                st.session_state[DEPOT_CURVE_CSV_IMPORT_KEY] = _normalize_depot_curve_csv_import_state(stored.get(DEPOT_CURVE_CSV_IMPORT_KEY, {}))
                st.session_state[SELL_DECISION_STATE_KEY] = _normalize_sell_decision_state(stored.get(SELL_DECISION_STATE_KEY, {}))
                migrated_settings = dict(_default_portfolio_settings())
                if isinstance(stored.get("portfolio_settings"), dict):
                    migrated_settings.update(stored.get("portfolio_settings", {}))
                st.session_state["portfolio_settings"] = migrated_settings
                _sync_workspace()
            except Exception as exc:
                logger.debug("workspace migration failed: %s", exc)
    st.session_state["watchlist"] = stored.get("watchlist", []) if isinstance(stored, dict) else []
    st.session_state["recent_tickers"] = stored.get("recent_tickers", []) if isinstance(stored, dict) else []
    st.session_state["positions"] = stored.get("positions", []) if isinstance(stored, dict) else []
    st.session_state["todos"] = _normalize_workspace_todos(stored.get("todos", [])) if isinstance(stored, dict) else []
    st.session_state["portfolio_history"] = stored.get("portfolio_history", []) if isinstance(stored, dict) and isinstance(stored.get("portfolio_history", []), list) else []
    st.session_state["portfolio_cash_flows"] = stored.get("portfolio_cash_flows", []) if isinstance(stored, dict) and isinstance(stored.get("portfolio_cash_flows", []), list) else []
    st.session_state[DEPOT_CURVE_CSV_IMPORT_KEY] = _normalize_depot_curve_csv_import_state(stored.get(DEPOT_CURVE_CSV_IMPORT_KEY, {}) if isinstance(stored, dict) else {})
    depot_curve_import_state = st.session_state[DEPOT_CURVE_CSV_IMPORT_KEY]
    st.session_state["pf_depot_curve_tx_df"] = _depot_curve_csv_records_to_frame(depot_curve_import_state.get("records", []))
    st.session_state["pf_depot_curve_tx_filename"] = depot_curve_import_state.get("filename", "")
    st.session_state["pf_depot_curve_isin_overrides"] = depot_curve_import_state.get("isin_overrides", {})
    st.session_state[SELL_DECISION_STATE_KEY] = _normalize_sell_decision_state(stored.get(SELL_DECISION_STATE_KEY, {}) if isinstance(stored, dict) else {})
    base_settings = dict(_default_portfolio_settings())
    if isinstance(stored, dict) and isinstance(stored.get("portfolio_settings"), dict):
        base_settings.update(stored.get("portfolio_settings", {}))
    st.session_state["portfolio_settings"] = base_settings
    st.session_state.setdefault("pos_filter", "alle")
    st.session_state.setdefault("show_add_ticker", False)
    st.session_state.setdefault("show_add_todo", False)
    st.session_state["_workspace_initialized"] = True

def _sell_decision_state_copy(state: dict) -> dict:
    # JSON roundtrip keeps callers from mutating session state by reference.
    return json.loads(json.dumps(_normalize_sell_decision_state(state), ensure_ascii=False))


def load_sell_decision_state() -> dict:
    _init_workspace_state()
    state = _normalize_sell_decision_state(st.session_state.get(SELL_DECISION_STATE_KEY, {}))
    st.session_state[SELL_DECISION_STATE_KEY] = state
    return _sell_decision_state_copy(state)


def save_sell_decision_state(state: dict) -> dict:
    _init_workspace_state()
    normalized = _normalize_sell_decision_state(state)
    st.session_state[SELL_DECISION_STATE_KEY] = normalized
    _sync_workspace()
    return _sell_decision_state_copy(normalized)


def get_position_manual_sell_data(ticker: str) -> dict:
    norm_ticker = _normalize_single_ticker(ticker)
    state = load_sell_decision_state()
    stored = state.get("positions_manual", {}).get(norm_ticker, {}) if norm_ticker else {}
    return _normalize_position_manual_sell_data(norm_ticker, stored)


def save_position_manual_sell_data(ticker: str, data: dict) -> dict:
    norm_ticker = _normalize_single_ticker(ticker or (data or {}).get("ticker", ""))
    if not norm_ticker:
        return _default_position_manual_sell_data("")
    state = load_sell_decision_state()
    normalized = _normalize_position_manual_sell_data(norm_ticker, data)
    normalized["ticker"] = norm_ticker
    state.setdefault("positions_manual", {})[norm_ticker] = normalized
    save_sell_decision_state(state)
    return dict(normalized)


def load_tranche_log() -> list[dict]:
    state = load_sell_decision_state()
    return [dict(entry) for entry in state.get("tranche_log", [])]

def get_position_tranche_log(ticker: str) -> list[dict]:
    """Rückwärtskompatibler Helper für ältere Strategy-Hub-Aufrufe."""
    norm = _normalize_single_ticker(ticker)
    return [row for row in load_tranche_log() if _normalize_single_ticker(row.get("ticker", "")) == norm]


def append_tranche_log(entry: dict) -> dict:
    normalized = _normalize_tranche_log_entry(entry or {})
    if not normalized.get("ticker"):
        return normalized
    state = load_sell_decision_state()
    rows = list(state.get("tranche_log", []))
    rows.append(normalized)
    state["tranche_log"] = rows[-3000:]
    save_sell_decision_state(state)
    return dict(normalized)


def get_recommendation_state(ticker: str) -> dict:
    """Read the hysteresis/snooze state for one ticker. Returns an empty default if absent."""
    norm = _normalize_single_ticker(ticker)
    if not norm:
        return _normalize_recommendation_state_entry({})
    state = load_sell_decision_state()
    entry = (state.get("recommendation_state") or {}).get(norm) or {}
    return _normalize_recommendation_state_entry(entry)


def save_recommendation_state(ticker: str, entry: dict) -> dict:
    """Persist the hysteresis/snooze state for one ticker."""
    norm = _normalize_single_ticker(ticker)
    if not norm:
        return _normalize_recommendation_state_entry({})
    normalized = _normalize_recommendation_state_entry(entry)
    state = load_sell_decision_state()
    state.setdefault("recommendation_state", {})[norm] = normalized
    save_sell_decision_state(state)
    return dict(normalized)


def snooze_recommendation(ticker: str, snoozed_pct: int, days: int = 5) -> dict:
    """Mark the current recommendation as snoozed for the next `days` trading days."""
    norm = _normalize_single_ticker(ticker)
    if not norm:
        return _normalize_recommendation_state_entry({})
    current = get_recommendation_state(norm)
    snoozed_until = (datetime.now(timezone.utc).date() + timedelta(days=max(int(days), 1))).strftime("%Y-%m-%d")
    current["snoozed_until"] = snoozed_until
    current["snoozed_pct"] = int(max(0, min(100, int(snoozed_pct))))
    return save_recommendation_state(norm, current)


def clear_recommendation_snooze(ticker: str) -> dict:
    """Cancel an active snooze for the given ticker."""
    norm = _normalize_single_ticker(ticker)
    if not norm:
        return _normalize_recommendation_state_entry({})
    current = get_recommendation_state(norm)
    current["snoozed_until"] = ""
    current["snoozed_pct"] = 0
    return save_recommendation_state(norm, current)


def load_closed_trades() -> list[dict]:
    state = load_sell_decision_state()
    return [dict(entry) for entry in state.get("closed_trades", [])]


def append_closed_trade(entry: dict) -> dict:
    normalized = _normalize_closed_trade_entry(entry or {})
    if not normalized.get("ticker"):
        return normalized
    state = load_sell_decision_state()
    rows = list(state.get("closed_trades", []))
    rows.append(normalized)
    state["closed_trades"] = rows[-3000:]
    save_sell_decision_state(state)
    return dict(normalized)


def load_post_mortem_log() -> list[dict]:
    state = load_sell_decision_state()
    return [dict(entry) for entry in state.get("post_mortem_log", [])]


def append_post_mortem_result(entry: dict) -> dict:
    normalized = _normalize_post_mortem_entry(entry or {})
    if not normalized.get("ticker"):
        return normalized
    state = load_sell_decision_state()
    rows = list(state.get("post_mortem_log", []))
    rows.append(normalized)
    state["post_mortem_log"] = rows[-3000:]
    save_sell_decision_state(state)
    return dict(normalized)


def _get_private_password_hash() -> str:
    candidates = [
        _safe_get_secret("private_area", "password_sha256", default=""),
        _safe_get_secret("PRIVATE_AREA_PASSWORD_SHA256", default=""),
    ]
    for value in candidates:
        if value and str(value).strip():
            return str(value).strip().lower()
    plain = _safe_get_secret("private_area", "password", default="") or _safe_get_secret("PRIVATE_AREA_PASSWORD", default="")
    if plain and str(plain).strip():
        return hashlib.sha256(str(plain).encode("utf-8")).hexdigest()
    return ""

def _private_area_enabled() -> bool:
    return bool(_get_private_password_hash())

def _is_private_unlocked() -> bool:
    if not _private_area_enabled():
        return True
    return bool(st.session_state.get("private_area_authenticated", False))

def _unlock_private_area(password: str) -> bool:
    candidate = hashlib.sha256((password or "").encode("utf-8")).hexdigest()
    ok = bool(password) and hmac.compare_digest(candidate, _get_private_password_hash())
    st.session_state["private_area_authenticated"] = ok
    if ok:
        st.session_state["private_area_error"] = ""
    else:
        st.session_state["private_area_error"] = "Passwort nicht korrekt."
    return ok

def _lock_private_area() -> None:
    st.session_state["private_area_authenticated"] = False

def _render_private_gate(title: str = "🔐 Privater Bereich") -> bool:
    if _is_private_unlocked():
        return True
    st.markdown(f"### {title}")
    st.info("Dieser Bereich ist geschützt. Gib dein Passwort ein, um dein persönliches Depot, Watchlist und To-dos zu laden.")
    with st.form("private_area_login"):
        password = st.text_input("Passwort", type="password", key="private_area_password_input")
        submitted = st.form_submit_button("Entsperren", width="stretch")
    if submitted:
        if _unlock_private_area(password):
            st.success("Privater Bereich entsperrt.")
            st.rerun()
    err = st.session_state.get("private_area_error", "")
    if err:
        st.error(err)
    return False

def _add_recent_ticker(ticker: str) -> None:
    ticker = _normalize_single_ticker(ticker)
    if not ticker:
        return
    _init_workspace_state()
    recents = [ticker] + [t for t in st.session_state["recent_tickers"] if t != ticker]
    st.session_state["recent_tickers"] = recents[:12]
    _sync_workspace()

def _add_watchlist_ticker(ticker: str) -> None:
    ticker = _normalize_single_ticker(ticker)
    if not ticker or not _is_valid_ticker(ticker):
        return
    _init_workspace_state()
    cur = [t for t in st.session_state["watchlist"] if t != ticker]
    cur.insert(0, ticker)
    st.session_state["watchlist"] = cur[:25]
    _add_recent_ticker(ticker)
    _sync_workspace()

def _remove_watchlist_ticker(ticker: str) -> None:
    ticker = _normalize_single_ticker(ticker)
    _init_workspace_state()
    st.session_state["watchlist"] = [t for t in st.session_state["watchlist"] if t != ticker]
    _sync_workspace()


def _normalize_workspace_todos(raw) -> list[dict]:
    if raw is None:
        items = []
    elif isinstance(raw, str):
        items = [line.strip() for line in raw.splitlines() if line.strip()]
    elif isinstance(raw, list):
        items = raw
    else:
        items = []

    normalized = []
    for idx, item in enumerate(items):
        if isinstance(item, dict):
            text_value = str(item.get("text", "") or "").strip()
            done = bool(item.get("done", False))
            todo_id = str(item.get("id") or "").strip()
        else:
            text_value = str(item or "").strip()
            done = False
            todo_id = ""
        if not text_value:
            continue
        normalized.append({
            "id": todo_id or hashlib.sha1(f"{idx}:{text_value}".encode("utf-8")).hexdigest()[:12],
            "text": text_value,
            "done": done,
        })
    return normalized[:50]


def _set_workspace_todos(todos: list[dict]) -> None:
    st.session_state["todos"] = _normalize_workspace_todos(todos)
    _sync_workspace()


def _add_todo(text_value: str) -> None:
    text_value = str(text_value or "").strip()
    if not text_value:
        return
    todos = _normalize_workspace_todos(st.session_state.get("todos", []))
    todos.append({"id": uuid.uuid4().hex[:12], "text": text_value, "done": False})
    _set_workspace_todos(todos)


def _update_todo_status(todo_id: str, done: bool) -> None:
    todos = _normalize_workspace_todos(st.session_state.get("todos", []))
    for todo in todos:
        if todo.get("id") == todo_id:
            todo["done"] = bool(done)
            break
    _set_workspace_todos(todos)


def _workspace_last_save() -> datetime | None:
    """Letzter persistierter Workspace-Speicher-Zeitstempel oder ``None``,
    falls noch nie gespeichert. ``datetime.now()`` als Fallback würde dem
    Nutzer fälschlich „vor 0 Min." anzeigen, obwohl nichts gespeichert ist."""
    raw = _get_cache_metadata(_get_price_store(), _workspace_meta_key("updated_at"), "")
    parsed = pd.to_datetime(raw, utc=True, errors="coerce") if raw else pd.NaT
    if pd.notna(parsed):
        return parsed.to_pydatetime()
    return None


def _workspace_positions_df(positions: list[dict], settings: dict | None = None) -> pd.DataFrame:
    settings = settings or _get_portfolio_settings()
    snapshot_df, _summary = _build_portfolio_snapshot(
        positions,
        cash_balance=_safe_float(settings.get("cash_balance"), 0.0),
        target_risk_contribution=_safe_float(settings.get("target_risk_contribution"), 0.20),
    )
    rows = []
    if snapshot_df is not None and not snapshot_df.empty:
        for _, row in snapshot_df.iterrows():
            if bool(row.get("is_cash", False)):
                continue
            pnl_pct = _safe_float(row.get("pnl_pct"), np.nan)
            status = "Stop-Loss" if (not np.isnan(pnl_pct) and pnl_pct < -7) else "OK"
            buy_date = pd.to_datetime(row.get("buy_date"), errors="coerce")
            rows.append({
                "ticker": _normalize_single_ticker(row.get("ticker", "")),
                "stueck": _safe_float(row.get("shares"), 0.0),
                "einstand": _safe_float(row.get("entry"), 0.0),
                "kaufdatum": buy_date.to_pydatetime() if pd.notna(buy_date) else datetime.now(timezone.utc),
                "status": status,
                "pnl_pct": pnl_pct if not np.isnan(pnl_pct) else 0.0,
                "wert": _safe_float(row.get("current_value"), 0.0),
                "currency": str(row.get("currency", "EUR") or "EUR").upper(),
            })
    return pd.DataFrame(rows, columns=["ticker", "stueck", "einstand", "kaufdatum", "status", "pnl_pct", "wert", "currency"])


@st.cache_data(ttl=3600, show_spinner=False)
def _usd_eur_rate() -> float:
    """USD→EUR Wechselkurs aus Yahoo. Cached, da pro P&L-Berechnung
    ohne Cache ein Yahoo-Roundtrip ausgelöst würde."""
    try:
        fx = yf.Ticker("EURUSD=X").history(period="5d")
        if fx is not None and len(fx) > 0:
            eur_usd = float(fx["Close"].dropna().iloc[-1])
            if eur_usd > 0:
                return 1.0 / eur_usd
    except Exception as exc:
        logger.debug("USD/EUR rate lookup failed: %s", exc)
    return 0.9259


def _usd_to_eur(value: float) -> float:
    return float(value or 0.0) * _usd_eur_rate()


def _format_eur(value: float) -> str:
    return f"{float(value or 0):,.0f} €".replace(",", ".")

def _is_valid_ticker(ticker: str) -> bool:
    return bool(re.fullmatch(r"[A-Z0-9][A-Z0-9.-]{0,14}", _normalize_single_ticker(ticker)))


def _normalize_workspace_ticker_list(values, limit: int = 25) -> list[str]:
    if values is None:
        candidates = []
    elif isinstance(values, str):
        candidates = re.split(r"[,;\s]+", values)
    elif isinstance(values, dict):
        candidates = values.values()
    else:
        try:
            candidates = list(values)
        except TypeError:
            candidates = [values]

    out = []
    for value in candidates:
        ticker = _normalize_single_ticker(value)
        if ticker and _is_valid_ticker(ticker) and ticker not in out:
            out.append(ticker)
        if len(out) >= limit:
            break
    return out


def _upsert_position(position: dict) -> None:
    _init_workspace_state()
    positions = st.session_state["positions"]
    ticker = position.get("ticker")
    updated = False
    for idx, existing in enumerate(positions):
        if existing.get("ticker") == ticker:
            merged = dict(existing)
            merged.update(position)
            positions[idx] = merged
            updated = True
            break
    if not updated:
        positions.insert(0, position)
    st.session_state["positions"] = positions[:30]
    _add_recent_ticker(ticker)
    if ticker:
        _add_watchlist_ticker(ticker)
    _sync_workspace()

def _remove_position(ticker: str) -> None:
    _init_workspace_state()
    st.session_state["positions"] = [p for p in st.session_state["positions"] if p.get("ticker") != ticker]
    _sync_workspace()

def _remove_positions_bulk(tickers: list[str]) -> int:
    _init_workspace_state()
    normalized = {str(t or "").strip().upper() for t in (tickers or []) if str(t or "").strip()}
    if not normalized:
        return 0
    before = st.session_state.get("positions", [])
    remaining = [p for p in before if str(p.get("ticker", "")).strip().upper() not in normalized]
    removed = len(before) - len(remaining)
    if removed > 0:
        st.session_state["positions"] = remaining
        _sync_workspace()
    return removed

def _remove_all_positions() -> int:
    _init_workspace_state()
    before = st.session_state.get("positions", [])
    if not before:
        return 0
    st.session_state["positions"] = []
    _sync_workspace()
    return len(before)

@st.cache_data(ttl=900, show_spinner=False)
def search_symbol_candidates(query: str):
    query = (query or "").strip()
    if not query:
        return []
    url = "https://query1.finance.yahoo.com/v1/finance/search"
    params = {"q": query, "quotesCount": 8, "newsCount": 0}
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        res = requests.get(url, params=params, headers=headers, timeout=10)
        res.raise_for_status()
        payload = res.json() or {}
        out = []
        for item in payload.get("quotes", []):
            symbol = str(item.get("symbol", "")).upper().strip()
            if not symbol or "=" in symbol or "^" in symbol:
                continue
            qtype = str(item.get("quoteType", "")).upper()
            if qtype and qtype not in {"EQUITY", "ETF"}:
                continue
            name = item.get("shortname") or item.get("longname") or ""
            exch = item.get("exchange", "") or item.get("exchDisp", "")
            out.append({"symbol": symbol, "name": str(name), "exchange": str(exch), "type": qtype or "EQUITY"})
        if out:
            dedup = []
            seen = set()
            for row in out:
                if row["symbol"] in seen:
                    continue
                seen.add(row["symbol"])
                dedup.append(row)
            return dedup
    except Exception as exc:
        logger.debug("symbol search failed for %s: %s", query, exc)
    fallback = query.upper().replace(" ", "")
    return [{"symbol": fallback, "name": "", "exchange": "", "type": "MANUAL"}] if fallback else []

@st.cache_data(ttl=3600, show_spinner=False)
def _ticker_display_names(tickers: tuple[str, ...]) -> dict[str, str]:
    normalized = tuple(dict.fromkeys(_normalize_single_ticker(t) for t in tickers if _normalize_single_ticker(t)))
    names: dict[str, str] = {}
    for ticker in normalized:
        name = ""
        try:
            candidates = search_symbol_candidates(ticker)
            exact = next((row for row in candidates if _normalize_single_ticker(row.get("symbol", "")) == ticker), None)
            if exact:
                name = str(exact.get("name", "") or "").strip()
        except Exception:
            name = ""
        if not name:
            try:
                metrics = _portfolio_symbol_metrics(ticker)
                metric_name = str((metrics or {}).get("name", "") or "").strip()
                if metric_name and metric_name.upper() != ticker:
                    name = metric_name
            except Exception:
                name = ""
        names[ticker] = name or ticker
    return names

ISIN_TO_YAHOO: dict[str, str] = {
    "US0378331005": "AAPL",
    "US5949181045": "MSFT",
    "US67066G1040": "NVDA",
    "US02079K3059": "GOOGL",
    "US0231351067": "AMZN",
    "US30303M1027": "META",
    "US88160R1014": "TSLA",
    "US1912161007": "KO",
    "US8740391003": "TSM",
    "DE0007236101": "SIE.DE",
    "DE0007164600": "SAP.DE",
    "DE000BAY0017": "BAYN.DE",
    "DE0007030009": "RHM.DE",
    "DE000ENER6Y0": "ENR.DE",
    "DE0007664039": "VOW3.DE",
    "DE000HLAG475": "HLAG.DE",
    "DE000A0WMPJ6": "AIXA.DE",
    "DE000A0Z1JH9": "PSAN.DE",
    "DE000RENK730": "R3NK.DE",
    "DE000A2N4H07": "WEW.DE",
    "KYG7397A1067": "1337.HK",
    "US20717M1036": "CFLT",
    "US4878361082": "K",
}

_ISIN_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")
_YAHOO_GERMAN_SUFFIXES = (".DE", ".F", ".SG", ".DU", ".MU", ".BE", ".HM", ".HA")
_YAHOO_GERMAN_EXCHANGES = {
    "GER", "XETRA", "FRANKFURT", "STUTTGART", "DUSSELDORF",
    "DÜSSELDORF", "MUNICH", "BERLIN", "HAMBURG", "HANOVER",
}
_YAHOO_US_EXCHANGES = {
    "NMS", "NGM", "NCM", "NYQ", "NYSE", "NASDAQ", "NAS", "ASE",
    "AMEX", "PCX", "BATS", "PNK", "OTC",
}


_TR_KONTOAUSZUG_MONTHS = {
    "Jan.": 1, "Feb.": 2, "März": 3, "Mar.": 3, "Apr.": 4, "Mai": 5, "Juni": 6,
    "Juli": 7, "Aug.": 8, "Sept.": 9, "Sep.": 9, "Okt.": 10, "Nov.": 11, "Dez.": 12,
}
_TR_KONTOAUSZUG_TYPES = {
    "Überweisung", "Handel", "Ertrag", "Steuern", "Gebühren", "Zinsen", "Sparplan", "Karte",
}
_TR_KONTOAUSZUG_AMOUNT_RE = re.compile(r"^-?\d{1,3}(?:\.\d{3})*,\d{2}$")
_TR_KONTOAUSZUG_ISIN_RE = re.compile(r"\b([A-Z]{2}[A-Z0-9]{9}[0-9])\b")


def _tr_kontoauszug_amount_to_float(raw: str) -> float | None:
    s = str(raw or "").replace("€", "").strip()
    if not s:
        return None
    sign = -1.0 if s.startswith("-") else 1.0
    s = s.lstrip("-").lstrip("+")
    s = s.replace(".", "").replace(",", ".")
    try:
        return sign * float(s)
    except ValueError:
        return None


def _parse_tr_kontoauszug_pdf(file_or_bytes) -> pd.DataFrame:
    """Parse a Trade Republic Kontoauszug PDF.

    Returns a DataFrame with one row per cash booking, columns:
        date (Timestamp), type (str), description (str),
        isin (str, may be empty), event_kind (str: deposit/withdrawal/buy/sell/dividend/tax/other),
        inflow_eur (float|nan), outflow_eur (float|nan), balance_eur (float).

    Inflow/outflow are derived from the running SALDO delta, which is the
    most reliable column in the statement and avoids fragile x-position
    classification of the two amount columns.
    """
    try:
        import pdfplumber
    except Exception as exc:  # pragma: no cover - dependency hint
        raise RuntimeError("pdfplumber ist nicht installiert. Bitte requirements aktualisieren.") from exc

    if hasattr(file_or_bytes, "read"):
        raw = file_or_bytes.read()
        try:
            file_or_bytes.seek(0)
        except Exception:
            pass
    elif isinstance(file_or_bytes, (bytes, bytearray)):
        raw = bytes(file_or_bytes)
    else:
        raise ValueError("Erwartet PDF-Datei oder bytes.")

    raw_rows: list[dict] = []
    seq = 0

    with pdfplumber.open(io.BytesIO(raw)) as pdf:
        for page in pdf.pages:
            try:
                words = page.extract_words(use_text_flow=True) or []
            except Exception:
                words = []
            if not words:
                continue
            words.sort(key=lambda w: (round(float(w["top"]), 1), float(w["x0"])))
            lines: list[list[dict]] = []
            current: list[dict] = []
            current_top: float | None = None
            for w in words:
                top = float(w["top"])
                if current_top is None or abs(top - current_top) <= 3.0:
                    current.append(w)
                    current_top = top if current_top is None else (current_top + top) / 2.0
                else:
                    lines.append(current)
                    current = [w]
                    current_top = top
            if current:
                lines.append(current)

            active: dict | None = None
            for line_words in lines:
                line_words.sort(key=lambda w: float(w["x0"]))
                tokens = [w["text"] for w in line_words]
                if not tokens:
                    continue

                # Detect "DD MMM. YYYY" date at the start of a row.
                date = None
                date_end = None
                for i in range(len(tokens) - 2):
                    day_t, mon_t, yr_t = tokens[i], tokens[i + 1], tokens[i + 2]
                    if (
                        day_t.isdigit() and 1 <= int(day_t) <= 31
                        and mon_t in _TR_KONTOAUSZUG_MONTHS
                        and yr_t.isdigit() and len(yr_t) == 4
                    ):
                        try:
                            date = pd.Timestamp(int(yr_t), _TR_KONTOAUSZUG_MONTHS[mon_t], int(day_t))
                            date_end = i + 3
                            break
                        except Exception:
                            continue

                if date is not None and date_end is not None:
                    if active is not None:
                        raw_rows.append(active)
                    type_label = ""
                    type_idx: int | None = None
                    for j in range(date_end, min(date_end + 4, len(tokens))):
                        if tokens[j] in _TR_KONTOAUSZUG_TYPES:
                            type_label = tokens[j]
                            type_idx = j
                            break
                    amount_words = [w for w in line_words if _TR_KONTOAUSZUG_AMOUNT_RE.match(str(w["text"]).strip())]
                    amount_words.sort(key=lambda w: float(w["x0"]))
                    amount_ids = {id(w) for w in amount_words}
                    desc_start = (type_idx + 1) if type_idx is not None else date_end
                    desc_tokens = [
                        w["text"] for w in line_words[desc_start:]
                        if id(w) not in amount_ids and w["text"] not in {"€", "EUR"}
                    ]
                    amounts = [_tr_kontoauszug_amount_to_float(w["text"]) for w in amount_words]
                    seq += 1
                    active = {
                        "_seq": seq,
                        "date": date,
                        "type": type_label,
                        "description": " ".join(desc_tokens).strip(),
                        "amounts": [a for a in amounts if a is not None],
                    }
                else:
                    if active is None:
                        continue
                    joined = " ".join(tokens).strip()
                    # Skip footers, headers, page numbers etc.
                    if not joined:
                        continue
                    if "Seite" in tokens and "von" in tokens:
                        continue
                    if joined.startswith(("Erstellt am", "Trade Republic Bank", "Geschäftsführer",
                                          "Brunnenstraße", "10119 Berlin", "www.traderepublic",
                                          "Sitz der Gesellschaft", "AG Charlottenburg",
                                          "Umsatzsteuer", "Andreas Torner", "Gernot Mittendorfer",
                                          "Christian Hecker", "Thomas Pischke")):
                        continue
                    if "DATUM" in tokens and "TYP" in tokens:
                        continue
                    if joined.upper().startswith(("TRADE REPUBLIC", "KONTOÜBERSICHT", "UMSATZÜBERSICHT")):
                        continue
                    active["description"] = (active["description"] + " " + joined).strip()
            if active is not None:
                raw_rows.append(active)
                active = None

    if not raw_rows:
        return pd.DataFrame(columns=["date", "type", "description", "isin", "event_kind", "inflow_eur", "outflow_eur", "balance_eur"])

    cleaned: list[dict] = []
    for r in raw_rows:
        amounts = r.get("amounts") or []
        if not amounts:
            continue
        balance = float(amounts[-1])
        # Movement (if separate column existed); we re-derive from SALDO delta below.
        cleaned.append({
            "_seq": r["_seq"],
            "date": r["date"],
            "type": r["type"],
            "description": r["description"],
            "balance_eur": balance,
        })

    if not cleaned:
        return pd.DataFrame(columns=["date", "type", "description", "isin", "event_kind", "inflow_eur", "outflow_eur", "balance_eur"])

    df = pd.DataFrame(cleaned).sort_values("_seq").reset_index(drop=True)

    prev = 0.0
    inflows: list[float] = []
    outflows: list[float] = []
    for bal in df["balance_eur"].tolist():
        bal = float(bal)
        delta = bal - prev
        if delta > 1e-6:
            inflows.append(delta)
            outflows.append(float("nan"))
        elif delta < -1e-6:
            inflows.append(float("nan"))
            outflows.append(-delta)
        else:
            inflows.append(float("nan"))
            outflows.append(float("nan"))
        prev = bal
    df["inflow_eur"] = inflows
    df["outflow_eur"] = outflows

    def _classify(row) -> str:
        typ = str(row["type"] or "")
        desc = str(row["description"] or "")
        if typ == "Überweisung":
            if "PayOut" in desc or "Auszahlung" in desc:
                return "withdrawal"
            return "deposit"
        if typ == "Handel":
            if "Direktverkauf" in desc or "Verkauf" in desc:
                return "sell"
            if "Direktkauf" in desc or "Kauf" in desc:
                return "buy"
            return "trade"
        if typ == "Ertrag":
            return "dividend"
        if typ == "Steuern":
            return "tax"
        if typ == "Gebühren":
            return "fee"
        if typ == "Zinsen":
            return "interest"
        return "other"

    df["event_kind"] = df.apply(_classify, axis=1)

    # Only extract ISIN for security-related events; deposit/withdrawal descriptions
    # contain IBAN references that can look superficially like ISINs.
    security_kinds = {"buy", "sell", "dividend", "tax", "trade"}

    def _extract_isin(row) -> str:
        if row["event_kind"] not in security_kinds:
            return ""
        m = _TR_KONTOAUSZUG_ISIN_RE.search(str(row.get("description", "") or ""))
        return m.group(1) if m else ""

    df["isin"] = df.apply(_extract_isin, axis=1)
    return df[["date", "type", "description", "isin", "event_kind", "inflow_eur", "outflow_eur", "balance_eur"]]


def _build_cash_trajectory_from_pdf(pdf_df: pd.DataFrame, calendar: pd.DatetimeIndex) -> pd.Series:
    """Per-day cash balance series, forward-filled across calendar days.

    Uses the LAST balance per day from the Kontoauszug; days before the first
    booking inherit 0; days after the last booking keep the latest balance.
    """
    if pdf_df is None or pdf_df.empty or len(calendar) == 0:
        return pd.Series(0.0, index=calendar)
    df = pdf_df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.normalize()
    df = df.dropna(subset=["date"]).sort_values("date")
    if df.empty:
        return pd.Series(0.0, index=calendar)
    end_of_day = df.groupby("date")["balance_eur"].last()
    series = end_of_day.reindex(calendar, method="ffill").fillna(0.0)
    return series


def _build_cash_trajectory_from_csv(tx_df: pd.DataFrame, calendar: pd.DatetimeIndex) -> pd.Series:
    """Reconstruct the daily EUR cash balance from a TR transactions export.

    Each booking's signed EUR cash impact is ``amount + fee + tax``. The standard
    Trade Republic export already encodes deposits (CUSTOMER_INBOUND), withdrawals
    (CUSTOMER_OUTBOUND_REQUEST), tax optimisations, dividends, interest and trade
    fees this way — accumulating these values yields the exact end-of-period cash
    balance shown on the official Kontoauszug. No PDF needed.
    """
    if tx_df is None or tx_df.empty or len(calendar) == 0:
        return pd.Series(0.0, index=calendar)
    df = tx_df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.normalize()
    df = df.dropna(subset=["date"]).sort_values("date")
    if df.empty:
        return pd.Series(0.0, index=calendar)
    for col in ("amount_num", "fee_num", "tax_num"):
        if col not in df.columns:
            df[col] = pd.to_numeric(df.get(col.split("_")[0], 0), errors="coerce").fillna(0.0)
    df["cash_delta"] = df["amount_num"] + df["fee_num"] + df["tax_num"]
    daily = df.groupby("date")["cash_delta"].sum()
    cumulative = daily.cumsum()
    series = cumulative.reindex(calendar, method="ffill").fillna(0.0)
    return series


def _parse_transaction_export_csv(uploaded_file) -> pd.DataFrame:
    required = {"date", "type", "asset_class", "name", "symbol", "shares", "price", "currency"}
    try:
        df = pd.read_csv(uploaded_file)
    except Exception as exc:
        raise ValueError(f"CSV konnte nicht gelesen werden: {exc}") from exc
    if df.empty:
        raise ValueError("Die CSV-Datei ist leer.")
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"Fehlende Spalten: {', '.join(missing)}")
    df = df.copy()
    # Parse all timestamps consistently as UTC, then drop timezone information.
    # This avoids mixed-aware/naive Timestamp comparisons during sorting.
    df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True).dt.tz_convert(None)
    if "datetime" in df.columns:
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce", utc=True).dt.tz_convert(None)
    df["event_ts"] = df["datetime"] if "datetime" in df.columns else df["date"]
    df["event_ts"] = df["event_ts"].fillna(df["date"])
    df["shares_num"] = pd.to_numeric(df["shares"], errors="coerce").fillna(0.0)
    df["price_num"] = pd.to_numeric(df["price"], errors="coerce").fillna(0.0)
    # `amount`, `fee`, `tax` carry the signed EUR cash impact per booking and are
    # the basis for the cash-trajectory reconstruction. They are present in the
    # standard Trade Republic export but are optional for backwards compatibility.
    for col in ("amount", "fee", "tax"):
        if col in df.columns:
            df[f"{col}_num"] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
        else:
            df[f"{col}_num"] = 0.0
    df["type"] = df["type"].astype(str).str.upper().str.strip()
    df["asset_class"] = df["asset_class"].astype(str).str.upper().str.strip()
    df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()
    return df.sort_values("event_ts")


def _reconstruct_open_positions_from_transactions(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    states: dict[str, dict] = {}
    for row in df.to_dict("records"):
        isin = str(row.get("symbol", "") or "").strip().upper()
        if not isin:
            continue
        typ = str(row.get("type", "") or "").strip().upper()
        raw_shares = float(_safe_float(row.get("shares_num"), 0.0))
        shares = abs(raw_shares)
        price = float(_safe_float(row.get("price_num"), 0.0))
        state = states.setdefault(isin, {"shares": 0.0, "cost": 0.0, "first_buy_date": None, "name": row.get("name", ""), "asset_class": row.get("asset_class", ""), "currency": row.get("currency", "EUR")})
        state["name"] = row.get("name", state["name"])
        state["asset_class"] = row.get("asset_class", state["asset_class"])
        state["currency"] = row.get("currency", state["currency"])
        current_date = row.get("date")

        if typ in {"BUY", "TRANSFER_IN"}:
            if state["shares"] <= 0:
                state["first_buy_date"] = current_date
            state["shares"] += shares
            state["cost"] += shares * price
        elif typ == "SELL_CANCELLED":
            # Stornierter Verkauf: vorherigen SELL umkehren, NICHT wie BUY zum
            # Verkaufspreis verbuchen. Andernfalls würde die Cost-Basis verfälscht.
            # Pragmatischer Ansatz: Anteile mit aktuellem Durchschnittskurs zurück­buchen,
            # damit der bestehende cost/shares-Schnitt erhalten bleibt.
            current_avg = (state["cost"] / state["shares"]) if state["shares"] > 0 else float(price)
            if state["shares"] <= 0 and current_avg <= 0:
                current_avg = float(price)
            state["shares"] += shares
            state["cost"] += shares * current_avg
        elif typ in {"SELL", "TRANSFER_OUT"}:
            sell_shares = min(max(shares, 0.0), max(state["shares"], 0.0))
            avg = (state["cost"] / state["shares"]) if state["shares"] > 0 else 0.0
            state["shares"] -= sell_shares
            state["cost"] -= sell_shares * avg
        elif typ == "SPLIT":
            # TR encodes split bookings as the *post-split total holding*.
            # Treating this as additive delta inflates positions (often ~x3/x4).
            state["shares"] = max(shares, 0.0)
        elif typ in {"WARRANT_EXERCISE", "INSOLVENCY_PROCEEDINGS", "DELISTED", "EXPIRATION"}:
            red = min(max(shares, 0.0), max(state["shares"], 0.0))
            avg = (state["cost"] / state["shares"]) if state["shares"] > 0 else 0.0
            state["shares"] -= red
            state["cost"] -= red * avg
        elif typ == "DIVIDEND":
            continue
        if state["shares"] <= 1e-9:
            state["shares"] = 0.0
            state["cost"] = 0.0
            state["first_buy_date"] = None

    rows = []
    deriv_rows = []
    for isin, state in states.items():
        if state["shares"] <= 0:
            continue
        avg = state["cost"] / state["shares"] if state["shares"] > 0 else 0.0
        item = {
            "name": state["name"],
            "isin": isin,
            "shares": float(state["shares"]),
            "avg_buy_price": float(avg),
            "first_buy_date": pd.Timestamp(state["first_buy_date"]).date().isoformat() if state["first_buy_date"] is not None and not pd.isna(state["first_buy_date"]) else "",
            "currency": str(state["currency"] or "EUR").upper(),
            "asset_class": str(state["asset_class"] or "").upper(),
        }
        if item["asset_class"] in {"STOCK", "FUND"}:
            rows.append(item)
        else:
            deriv_rows.append(item)
    return pd.DataFrame(rows), pd.DataFrame(deriv_rows)


def _looks_like_isin(value: str) -> bool:
    return bool(_ISIN_RE.match(str(value or "").strip().upper()))


def _is_german_yahoo_candidate(candidate: dict) -> bool:
    symbol = str((candidate or {}).get("symbol", "") or "").strip().upper()
    exchange = str((candidate or {}).get("exchange", "") or "").strip().upper()
    dotted_symbol = symbol.replace("-", ".")
    return dotted_symbol.endswith(_YAHOO_GERMAN_SUFFIXES) or any(token in exchange for token in _YAHOO_GERMAN_EXCHANGES)


def _is_us_yahoo_candidate(candidate: dict) -> bool:
    symbol = str((candidate or {}).get("symbol", "") or "").strip().upper()
    exchange = str((candidate or {}).get("exchange", "") or "").strip().upper()
    if any(token in exchange for token in _YAHOO_US_EXCHANGES):
        return True
    # Unsuffixed Yahoo symbols are normally US listings. Foreign listings almost
    # always carry suffixes such as .MX/.TO/.VI and can have incompatible prices.
    return "." not in symbol and "-" not in symbol


def _rank_yahoo_candidate(candidate: dict, index: int, *, prefer_german: bool) -> tuple[int, int, str]:
    symbol = str((candidate or {}).get("symbol", "") or "").strip().upper()
    german_rank = 0 if _is_german_yahoo_candidate(candidate) else 1
    if prefer_german:
        return (german_rank, index, symbol)
    return (0, index, symbol)


def _select_yahoo_candidate(candidates: list[dict], isin: str, *, prefer_german: bool) -> str:
    cleaned = []
    isin = str(isin or "").strip().upper()
    for index, row in enumerate(candidates or []):
        symbol = str((row or {}).get("symbol", "") or "").strip().upper()
        if not symbol or "=" in symbol or "^" in symbol:
            continue
        if symbol == isin or _looks_like_isin(symbol):
            continue
        qtype = str((row or {}).get("type", "") or "").upper()
        if qtype and qtype not in {"EQUITY", "ETF"}:
            continue
        cleaned.append((index, row))
    if isin.startswith("US"):
        cleaned = [(index, row) for index, row in cleaned if _is_us_yahoo_candidate(row)]
    if not cleaned:
        return ""
    return _normalize_single_ticker(
        sorted(cleaned, key=lambda item: _rank_yahoo_candidate(item[1], item[0], prefer_german=prefer_german))[0][1].get("symbol", "")
    )


def _suggest_yahoo_ticker(isin: str, name: str, asset_class: str) -> str:
    isin = str(isin or "").upper().strip()
    mapped = _normalize_single_ticker(ISIN_TO_YAHOO.get(isin, ""))
    if mapped:
        return mapped
    if str(asset_class or "").upper() not in {"STOCK", "FUND"}:
        return ""

    # Yahoo's search endpoint can resolve many TR ISINs directly.
    # Prefer ISIN query over free-text names to avoid ambiguous matches. If that
    # fails, German listings are preferred for German securities and funds so TR
    # exports are more likely to stay in EUR.
    prefer_german = isin.startswith("DE") or str(asset_class or "").upper() == "FUND"
    for idx, query in enumerate((isin, str(name or "").strip())):
        if not query:
            continue
        try:
            candidates = search_symbol_candidates(query)
            ticker = _select_yahoo_candidate(
                candidates,
                isin,
                prefer_german=prefer_german if idx > 0 else isin.startswith("DE"),
            )
            if ticker:
                return ticker
        except Exception:
            continue
    return ""


def _render_transaction_importer() -> None:
    st.markdown("#### 📥 CSV-Import aus Transaktionsexport")
    st.warning("Bitte prüfe die vorgeschlagenen Yahoo-Ticker vor dem Import. Die CSV enthält ISINs, nicht automatisch Yahoo-Symbole.")
    uploaded_file = st.file_uploader("Transaktionsexport (CSV)", type=["csv"], key="pf_transaction_csv")
    if not uploaded_file:
        return
    try:
        tx_df = _parse_transaction_export_csv(uploaded_file)
        import_df, skipped_df = _reconstruct_open_positions_from_transactions(tx_df)
    except ValueError as exc:
        st.error(str(exc))
        return
    if import_df.empty and skipped_df.empty:
        st.info("Keine offenen Positionen gefunden.")
        return
    if not import_df.empty:
        import_df = import_df.copy()
        import_df["importieren"] = True
        import_df["ticker"] = import_df.apply(lambda r: _suggest_yahoo_ticker(r.get("isin", ""), r.get("name", ""), r.get("asset_class", "")), axis=1)
        preview = import_df[["importieren", "name", "isin", "ticker", "shares", "avg_buy_price", "first_buy_date", "currency", "asset_class"]]
        edited = st.data_editor(preview, width="stretch", hide_index=True, key="pf_import_preview")
        if st.button("Positionen importieren", key="pf_import_positions_btn", type="primary", width="stretch"):
            selected = edited[(edited["importieren"] == True) & edited["ticker"].astype(str).str.strip().ne("")]
            if selected.empty:
                st.warning("Keine importierbaren Zeilen ausgewählt.")
            else:
                imported_count = 0
                skipped_count = 0
                for row in selected.to_dict("records"):
                    currency = str(row.get("currency") or "EUR").upper()
                    avg_price = float(_safe_float(row.get("avg_buy_price"), 0.0))
                    position = {
                        "ticker": _normalize_single_ticker(row.get("ticker", "")),
                        "shares": float(_safe_float(row.get("shares"), 0.0)),
                        "buy_price": avg_price,
                        "buy_price_eur": avg_price if currency == "EUR" else None,
                        "buy_date": str(row.get("first_buy_date") or ""),
                        "currency": currency or "EUR",
                        "note": f"Import aus Transaktionsexport.csv / ISIN {row.get('isin', '')}",
                    }
                    if position["ticker"] and position["shares"] > 0:
                        _upsert_position(position)
                        imported_count += 1
                    else:
                        skipped_count += 1
                _sync_workspace()
                if imported_count:
                    suffix = f" ({skipped_count} übersprungen)" if skipped_count else ""
                    st.success(f"{imported_count} Position(en) importiert{suffix}.")
                else:
                    st.warning("Keine gültigen Positionen importiert (Ticker/Stückzahl prüfen).")
                st.rerun()
    if not skipped_df.empty:
        st.caption("Nicht automatisch importiert (z. B. Derivate/Optionsscheine):")
        st.dataframe(skipped_df, width="stretch", hide_index=True)

def _is_mobile_client() -> bool:
    try:
        headers = getattr(st.context, "headers", {}) or {}
        ua = str(headers.get("user-agent", "")).lower()
    except Exception:
        ua = ""
    return any(token in ua for token in ["iphone", "android", "mobile", "ipad"])

def _render_ticker_picker(
    key_prefix: str,
    label: str,
    placeholder: str = "NVDA oder Nvidia",
    show_quick: bool = True,
    action_label: str | None = None,
    action_key: str | None = None,
    action_help: str | None = None,
):
    _init_workspace_state()
    input_key = f"{key_prefix}_query_input"
    selected_key = f"{key_prefix}_query"
    pending_key = f"{key_prefix}_pending_ticker"
    pending_ticker = st.session_state.pop(pending_key, None)
    if pending_ticker:
        st.session_state[selected_key] = pending_ticker
        st.session_state[input_key] = pending_ticker
    elif input_key not in st.session_state:
        st.session_state[input_key] = st.session_state.get(selected_key, "")
    if action_label and action_key:
        input_col, action_col = st.columns([4, 1])
        with input_col:
            query = st.text_input(label, placeholder=placeholder, key=input_key)
        with action_col:
            st.markdown("<div style='height: 1.75rem'></div>", unsafe_allow_html=True)
            if st.button(action_label, width="stretch", key=action_key, type="secondary", help=action_help):
                st.session_state[f"{action_key}_requested"] = True
    else:
        query = st.text_input(label, placeholder=placeholder, key=input_key)
    query = (query or "").strip()
    st.session_state[selected_key] = query

    if show_quick:
        max_recent = 4 if _is_mobile_client() else 8
        recent_cols_count = 2 if _is_mobile_client() else 8
        recents = list(dict.fromkeys(st.session_state.get("recent_tickers", [])))[:max_recent]
        others: list[str] = []
        for source in [st.session_state.get("watchlist", []), DEFAULT_FAVORITES]:
            for ticker in source:
                if ticker not in recents and ticker not in others:
                    others.append(ticker)
        others = others[: max(0, 8 - len(recents))]

        if recents:
            st.markdown('<div class="card-label">🕐 Zuletzt geprüft</div>', unsafe_allow_html=True)
            cols = st.columns(min(recent_cols_count, len(recents)))
            for i, ticker in enumerate(recents):
                with cols[i % len(cols)]:
                    if st.button(ticker, key=f"{key_prefix}_quick_{ticker}", width="stretch", type="secondary"):
                        st.session_state[pending_key] = ticker
                        st.rerun()

        if others:
            cols = st.columns(min(4, len(others)))
            for i, ticker in enumerate(others):
                with cols[i % len(cols)]:
                    if st.button(ticker, key=f"{key_prefix}_quick_{ticker}", width="stretch"):
                        st.session_state[pending_key] = ticker
                        st.rerun()
    if not query:
        return ""
    candidates = search_symbol_candidates(query)
    labels = []
    for item in candidates:
        name = item.get("name", "")
        exch = item.get("exchange", "")
        suffix = f" — {name}" if name else ""
        if exch:
            suffix += f" · {exch}"
        labels.append(f"{item['symbol']}{suffix}")
    default_idx = 0
    for idx, item in enumerate(candidates):
        if item["symbol"].upper() == query.upper():
            default_idx = idx
            break
    if len(candidates) == 1:
        chosen = candidates[0]["symbol"].upper()
        st.caption(f"Treffer: {labels[0]}")
    else:
        idx = st.selectbox("Treffer", options=list(range(len(candidates))), index=default_idx, format_func=lambda i: labels[i], key=f"{key_prefix}_pick")
        chosen = candidates[idx]["symbol"].upper()
    if _is_private_unlocked():
        _add_recent_ticker(chosen)
    return chosen

def _format_market_date(ts) -> str:
    try:
        return pd.Timestamp(ts).strftime("%d.%m.%Y")
    except Exception:
        return "—"

def _elapsed_text(ts) -> str:
    try:
        delta = datetime.now() - pd.Timestamp(ts).to_pydatetime()
        hours = int(delta.total_seconds() // 3600)
        if hours < 1:
            minutes = max(1, int(delta.total_seconds() // 60))
            return f"vor {minutes} Min."
        if hours < 24:
            return f"vor {hours} Std."
        return f"vor {delta.days} Tg."
    except Exception:
        return "—"

def _format_data_freshness(selected: str, df: pd.DataFrame, vol_dashboard: pd.DataFrame | None = None) -> dict:
    latest_date = _format_market_date(df.index[-1]) if df is not None and len(df) else "—"
    vix_date = _format_market_date(vol_dashboard.index[-1]) if vol_dashboard is not None and len(vol_dashboard) else latest_date
    store = _get_price_store()
    meta = _get_cache_metadata_many(store, [
        "last_refresh_at", "cache_prices_last_write_at",
        "last_refresh_loaded_universe", "last_refresh_requested_universe",
    ])
    return {
        "index_name": selected,
        "index_date": latest_date,
        "vix_date": vix_date,
        "store_label": _get_store_label(store),
        "nyse_refresh": meta.get("last_refresh_at", "") or meta.get("cache_prices_last_write_at", ""),
        "coverage": meta.get("last_refresh_loaded_universe", ""),
        "requested": meta.get("last_refresh_requested_universe", ""),
    }

def _ampel_phase_label(phase: str) -> str:
    phase = str(phase or '').lower()
    return {
        'rot': 'ROT',
        'gelb': 'GELB — Startschuss',
        'gruen': 'GRÜN — Frühe Bestätigung',
        'aufwaertstrend': 'AUFWÄRTSTREND',
        'neutral': 'NEUTRAL',
    }.get(phase, phase.upper() if phase else '—')


def _ampel_reason_line(L) -> str:
    phase = str(L.get('Ampel_Phase', '')).lower()
    anchor = L.get('Anchor_Date')
    ss_low = L.get('Startschuss_Low', np.nan)
    floor = L.get('Floor_Mark', np.nan)
    if phase == 'gelb':
        if anchor and pd.notna(ss_low):
            return f'Trendwende-Ampel: GELB — Startschuss aktiv seit {anchor} · Startschuss-Tief {float(ss_low):,.2f}'
        return 'Trendwende-Ampel: GELB — Startschuss aktiv'
    if phase == 'gruen':
        if pd.notna(ss_low):
            return f'Trendwende-Ampel: GRÜN — Startschuss bestätigt · Absicherung über {float(ss_low):,.2f}'
        return 'Trendwende-Ampel: GRÜN — Startschuss bestätigt'
    if phase == 'aufwaertstrend':
        return 'Trendwende-Ampel: AUFWÄRTSTREND — MA-Ordnung bestätigt'
    if phase == 'rot':
        if pd.notna(floor):
            return f'Trendwende-Ampel: ROT — Korrektur aktiv · Floor-Marke {float(floor):,.2f}'
        return 'Trendwende-Ampel: ROT — Korrektur aktiv'
    return 'Trendwende-Ampel: NEUTRAL — kein aktiver Zyklus'


def _market_action_and_tone(phase: str, warning_count: int, breadth_mode: str, vol_regime: str):
    phase = str(phase or "").lower()
    breadth_mode = str(breadth_mode or "").lower()
    vol_regime = str(vol_regime or "").lower()
    if phase == "rot" or warning_count >= 3 or breadth_mode == "schutz" or vol_regime == "stress":
        if phase == "rot":
            msg = "Ampel rot. Risiko reduzieren, keine aggressiven Neueinstiege und bestehende Positionen kritisch prüfen."
        elif vol_regime == "stress":
            msg = "Volatilität im Stress-Regime. Defensive Haltung — kein Neukauf trotz Ampelphase."
        elif breadth_mode == "schutz":
            msg = "Marktbreite im Schutzmodus. Risiko reduzieren — keine aggressiven Neueinstiege."
        else:
            msg = f"{warning_count} Warnzeichen aktiv. Defensive Haltung — Risiko reduzieren trotz laufender Ampelphase."
        return "Defensiv", "bad", msg
    if phase == "gelb":
        if warning_count <= 2 and breadth_mode != "schutz" and vol_regime not in {"stress", "risk"}:
            return "Startschuss", "warn", "Startschuss aktiv. Erste Pilotpositionen sind erlaubt, aber nur selektiv und mit enger Risikokontrolle über das Startschuss-Tief."
        return "Startschuss", "warn", "Startschuss erkannt, aber Umfeld noch nicht frei. Nur kleine Testpositionen und keine Aggressivität."
    if phase == "gruen":
        if warning_count <= 2 and breadth_mode != "schutz":
            return "Frühe Bestätigung", "good", "Startschuss bestätigt. Gute Setups sind erlaubt und Risiko kann vorsichtig erhöht werden."
        return "Frühe Bestätigung", "warn", "Ampel grün, aber Umfeld gemischt. Nur selektiv aufstocken."
    if phase == "aufwaertstrend":
        return "Offensiv", "good", "MA-Ordnung bestätigt. Markt konstruktiv, führende Aktien beobachten und Risiko schrittweise erhöhen."
    if warning_count >= 2 or breadth_mode == "neutral" or vol_regime in {"risk", "vorsicht"}:
        return "Neutral", "warn", "Selektiv bleiben. Nur A-Setups und eher kleine Einstiege."
    return "Konstruktiv", "good", "Markt konstruktiv. Führende Aktien beobachten und Risiko schrittweise erhöhen."

def _build_market_reasons(L, warning_count: int, breadth_mode: str, vol_latest: pd.Series):
    reasons = []
    reasons.append(_ampel_reason_line(L))
    reasons.append(f"Aktive Warnzeichen: {warning_count}")
    p50 = L.get("Dist_50SMA_pct", np.nan)
    if not np.isnan(p50):
        reasons.append(f"Abstand zur 50-SMA: {p50:+.1f}%")
    if breadth_mode:
        reasons.append(f"Equal-Weight-Modus: {str(breadth_mode).capitalize()}")
    vix_regime = vol_latest.get("VIX_Regime", "")
    if vix_regime:
        reasons.append(f"VIX-Regime: {vix_regime}")
    return reasons[:4]


def _fmt_num(value, suffix: str = "") -> str:
    try:
        if pd.isna(value):
            return "—"
        return f"{float(value):,.2f}{suffix}"
    except Exception:
        return "—"


def _fmt_pct(value) -> str:
    try:
        if pd.isna(value):
            return "—"
        return f"{float(value):+.1f}%"
    except Exception:
        return "—"


def render_haltung_banner(haltung, warnzeichen, abstand_50sma, equal_weight):
    """Render the defensive/offensive recommendation as a product-style banner."""
    is_defensive = str(haltung or "").strip().lower() == "defensiv"
    bg = "#FAEEDA" if is_defensive else "#EAF3DE"
    icon_color = "#854F0B" if is_defensive else "#3B6D11"
    body_color = "#412402" if is_defensive else "#173404"
    icon = "⚠" if is_defensive else "↗"
    title = f"{'Defensiv' if is_defensive else 'Offensiv'} — {int(warnzeichen or 0)} Warnzeichen aktiv"
    distance = _fmt_pct(abstand_50sma)
    equal_weight_text = html.escape(str(equal_weight or "—"))
    if is_defensive:
        body = (
            f"Die Marktampel fordert Risikokontrolle, weil {int(warnzeichen or 0)} Warnzeichen aktiv sind und der Abstand zur 50-SMA bei {distance} liegt. "
            f"Der Equal-Weight-Modus steht auf {equal_weight_text}; neue Käufe bleiben selektiv und Positionsgrößen defensiv."
        )
    else:
        body = (
            f"Das Marktbild erlaubt eine offensivere Haltung, weil die Ampel konstruktiv ist und der Abstand zur 50-SMA bei {distance} liegt. "
            f"Der Equal-Weight-Modus steht auf {equal_weight_text}; führende Aktien können schrittweise höher gewichtet werden."
        )
    st.markdown(
        f"""
        <div class="haltung-banner" style="background:{bg}; color:{body_color};">
          <div class="haltung-banner__icon" style="color:{icon_color};">{icon}</div>
          <div>
            <div class="eyebrow" style="color:{icon_color};">Empfohlene Haltung</div>
            <div class="haltung-banner__title" style="color:{body_color};">{html.escape(title)}</div>
            <div class="haltung-banner__body" style="color:{body_color};">{body}</div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_dashboard_kpi_card(label, value, unit, trend_text, trend_color, trend_icon, footnote):
    """Render a fixed-height KPI card with custom HTML instead of st.metric."""
    value_html = str(value) if str(value).lstrip().startswith("<") else html.escape(str(value))
    unit_html = f'<span class="dash-kpi-card__unit">{html.escape(str(unit))}</span>' if unit else ""
    trend = html.escape(str(trend_text or "—"))
    icon = html.escape(str(trend_icon or ""))
    st.markdown(
        f"""
        <div class="dash-kpi-card">
          <div>
            <div class="eyebrow">{html.escape(str(label))}</div>
            <div class="dash-kpi-card__value">{value_html}{unit_html}</div>
            <div class="dash-kpi-card__trend" style="color:{trend_color};">{icon} {trend}</div>
          </div>
          <div class="dash-kpi-card__footnote">{html.escape(str(footnote or ""))}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _phase_to_active_index(active_phase: str) -> int:
    phase = str(active_phase or "").lower()
    if phase == "rot":
        return 0
    if phase == "gelb":
        return 1
    if phase in {"gruen", "aufwaertstrend"}:
        return 2
    return -1


def render_ampel(active_phase, ankertag, bodenmarke, startschuss_tief):
    """Render the Trendwende-Ampel as a compact horizontal phase stepper."""
    active_idx = _phase_to_active_index(active_phase)
    status_label = "Aufwärtstrend aktiv" if str(active_phase).lower() == "aufwaertstrend" else _ampel_phase_label(active_phase) or "Neutral"
    phases = [
        ("Rot", "Abwärts / Distribution", "Substanzielle Korrektur oder institutioneller Abgabedruck."),
        ("Gelb", "Bodenbildung", "Ankertag und Startschuss werden als frühe Stabilisierung geprüft."),
        ("Grün", "Aufwärtstrend bestätigt", "Startschuss hält, Trendstruktur und MA-Ordnung bestätigen den Zyklus."),
    ]
    dot_colors = ["#A32D2D", "#854F0B", "#639922"]
    phase_html = ""
    for i, (name, label, desc) in enumerate(phases):
        active_cls = " is-active" if i == active_idx else ""
        phase_html += (
            f'<div class="ampel-phase{active_cls}">'
            f'<div class="ampel-phase__top"><span class="dash-ampel-dot" style="background:{dot_colors[i]};"></span>'
            f'<span class="eyebrow">{html.escape(name)}</span></div>'
            f'<div class="ampel-phase__desc"><strong>{html.escape(label)}</strong><br>{html.escape(desc)}</div>'
            f'</div>'
        )
    anchor_txt = html.escape(str(ankertag)) if ankertag else "—"
    floor_txt = _fmt_num(bodenmarke)
    ss_txt = _fmt_num(startschuss_tief)
    st.markdown(
        f"""
        <div class="ampel-card">
          <div class="ampel-card__header">
            <div class="eyebrow">Trendwende-Ampel</div>
            <div class="ampel-pill">↗ {html.escape(status_label)}</div>
          </div>
          <div class="ampel-stepper">{phase_html}</div>
          <div class="ampel-details">
            <div class="ampel-detail">
              <div class="ampel-detail__icon">✓</div>
              <div>
                <div class="ampel-detail__title">MA-Ordnung bestätigt</div>
                <div class="ampel-detail__body ampel-detail__mono">21-EMA &gt; 50-SMA &gt; 200-SMA</div>
              </div>
            </div>
            <div class="ampel-detail">
              <div class="ampel-detail__icon">⚑</div>
              <div>
                <div class="ampel-detail__title">Letzter Startschuss</div>
                <div class="ampel-detail__body">Ankertag {anchor_txt} · Boden {floor_txt} · Startschuss-Tief {ss_txt}</div>
              </div>
            </div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

def _build_market_changes(df: pd.DataFrame, selected: str, wc: int, vol_dashboard: pd.DataFrame | None = None, breadth_label: str = ""):
    changes = []
    if df is None or len(df) < 2:
        return changes
    prev = df.iloc[-2]
    latest = df.iloc[-1]
    price_delta = latest.get("Pct_Change", np.nan)
    if not np.isnan(price_delta):
        arrow = "▲" if price_delta >= 0 else "▼"
        arrow_color = "#27500A" if price_delta >= 0 else "#A32D2D"
        dd_val = latest.get("Dist_52w_pct", np.nan)
        dd_txt = f"52W-Hoch: {dd_val:.1f}%" if not np.isnan(dd_val) else ""
        idx_date = pd.Timestamp(df.index[-1]).strftime("%d.%m.%Y") if len(df) > 0 else "—"
        idx_stand = f"Index Stand: {idx_date}"
        display_selected = "Nasdaq" if "Nasdaq" in selected else selected
        changes.append({"title": display_selected, "value": f"{price_delta:+.2f}%", "detail": f"Schlusskurs {latest['Close']:,.2f}", "detail2": idx_stand, "detail3": dd_txt, "arrow": arrow, "arrow_color": arrow_color})
    dist_prev = int(prev.get("Dist_Count_25", 0))
    dist_now = int(latest.get("Dist_Count_25", 0))
    delta = dist_now - dist_prev
    delta_txt = f"{delta:+d}" if delta else "unverändert"
    dist_quality = "✓ Gut" if dist_now < 4 else ("⚠ Häufung" if dist_now < 6 else "✗ Kritisch")
    dist_quality_color = "#27500A" if dist_now < 4 else ("#854F0B" if dist_now < 6 else "#A32D2D")
    changes.append({"title": "Distribution", "value": str(dist_now), "unit": "Tage", "detail": f"Gegenüber gestern: {delta_txt}", "quality": dist_quality, "quality_color": dist_quality_color})
    prev_phase = _ampel_phase_label(prev.get("Ampel_Phase", ""))
    phase = _ampel_phase_label(latest.get("Ampel_Phase", ""))
    if str(latest.get("Ampel_Phase", "")).lower() == "gelb":
        ss_low = latest.get("Startschuss_Low", np.nan)
        ss_detail = f"Startschuss-Tief {float(ss_low):,.2f}" if pd.notna(ss_low) else "Startschuss aktiv"
        detail = f"{ss_detail} · {wc} Warnzeichen"
    elif str(latest.get("Ampel_Phase", "")).lower() == "gruen":
        detail = f"Startschuss bestätigt · {wc} Warnzeichen"
    else:
        detail = f"Wechsel von {prev_phase or '—'} auf {phase or '—'}" if phase != prev_phase else f"Unverändert seit gestern · {wc} Warnzeichen"
    changes.append({"title": "Trendwende-Ampel", "value": phase or "—", "detail": detail})
    if vol_dashboard is not None and len(vol_dashboard) >= 2:
        vp = vol_dashboard.iloc[-2]
        vl = vol_dashboard.iloc[-1]
        regime_now = vl.get("VIX_Regime", "n/a")
        regime_prev = vp.get("VIX_Regime", "n/a")
        change_detail = f"Regime {regime_prev} → {regime_now}" if regime_now != regime_prev else f"Regime stabil: {regime_now}"
        vix_close = vl.get("VIX_Close", np.nan)
        val = f"VIX {vix_close:.1f}" if pd.notna(vix_close) else "VIX n/a"
        vix_date = pd.Timestamp(vol_dashboard.index[-1]).strftime("%d.%m.%Y")
        vix_stand = f"VIX Stand: {vix_date}" if pd.notna(vix_close) else ""
        changes.append({"title": "Volatilität", "value": val, "detail": change_detail, "detail2": vix_stand})
    if breadth_label:
        changes.append({"title": "Breite", "value": breadth_label, "detail": "Equal-Weight als Bestätigung des Indextrends"})
    return changes[:4]

def _render_change_cards(changes):
    if not changes:
        return
    cols = st.columns(4)
    for col, item in zip(cols, changes[:4]):
        with col:
            title = str(item.get("title", ""))
            value = str(item.get("value", "—"))
            trend_text = item.get("detail", "—")
            trend_icon = item.get("arrow") or "→"
            trend_color = item.get("arrow_color") or "#5F5E5A"
            footnote = item.get("detail2") or item.get("detail3") or ""
            unit = item.get("unit", "")

            if title == "Distribution":
                trend_color = item.get("quality_color", "#5F5E5A")
                trend_icon = "✓" if "Gut" in str(item.get("quality", "")) else "⚠"
                trend_text = item.get("quality") or trend_text
                footnote = item.get("detail", "25-Tage-Fenster")
            elif title == "Trendwende-Ampel":
                phase_value = html.escape(value or "—")
                value = f'<span class="dash-ampel-dot" style="background:#639922;margin-right:8px;vertical-align:middle;"></span>{phase_value}'
                trend_color = "#27500A"
                trend_icon = "↗"
                footnote = item.get("detail", "Aktuelle Ampelphase")
            elif title == "Volatilität":
                trend_color = "#5F5E5A"
                trend_icon = "→"
                footnote = item.get("detail2") or "VIX-Regime"
            elif title in {"S&P 500", "Nasdaq", "Russell 2000", "Nasdaq Composite"}:
                footnote = item.get("detail2") or item.get("detail3") or item.get("detail", "")

            render_dashboard_kpi_card(title, value, unit, trend_text, trend_color, trend_icon, footnote)

def _render_hero_card(mode: str, tone: str, reasons: list[str], action: str, freshness: dict):
    tone_cls = {"good": "hero-good", "warn": "hero-warn", "bad": "hero-bad"}.get(tone, "hero-warn")
    tone_color = {"good": "#22c55e", "warn": "#f59e0b", "bad": "#ef4444"}.get(tone, "#94a3b8")
    bullets = "".join(f"<li>{r}</li>" for r in reasons)
    st.markdown(
        f'<div class="summary-hero"><div style="font-size:1.65rem;font-weight:900;color:{tone_color};letter-spacing:.04em;margin:0 0 8px 0;">{mode}</div><ul style="margin:6px 0 0 1rem;padding:0;line-height:1.45;">{bullets}</ul><div class="hero-action {tone_cls}">Konsequenz: {action}</div></div>',
        unsafe_allow_html=True,
    )

def _render_dist_tile(label: str, value: str, indicator: str, tone: str, caption_text: str = "") -> None:
    """Render a MA-distance metric tile with tone-colored border (good=green, warn=yellow, bad=red)."""
    color = {"good": "#22c55e", "warn": "#f59e0b", "bad": "#ef4444"}.get(tone, "#64748b")
    st.markdown(
        f'<div style="border:1px solid {color}40;border-radius:8px;padding:10px 14px;background:{color}0d;margin-bottom:2px;">'
        f'<div style="font-size:.62rem;color:#64748b;text-transform:uppercase;letter-spacing:.07em;margin-bottom:6px;">{label}</div>'
        f'<div style="font-size:1.35rem;font-weight:700;color:#0d1626;">{value}</div>'
        f'<div style="font-size:.72rem;color:{color};font-weight:600;margin-top:4px;">{indicator}</div>'
        f'</div>',
        unsafe_allow_html=True,
    )
    if caption_text:
        st.caption(caption_text)


def _dist_tile_html(label: str, value: str, indicator: str, tone: str) -> str:
    color = {"good": "#16a34a", "warn": "#ca8a04", "bad": "#dc2626"}.get(tone, "#64748b")
    return (
        f'<div style="border:1px solid {color}40;border-radius:8px;padding:9px 10px;background:{color}0d;">'
        f'<div style="font-size:11px;color:#64748b;text-transform:uppercase;letter-spacing:.07em;margin-bottom:5px;font-weight:700;">{label}</div>'
        f'<div style="font-size:1.18rem;font-weight:800;color:#0d1626;line-height:1.15;">{value}</div>'
        f'<div style="font-size:.72rem;color:{color};font-weight:700;margin-top:4px;">{indicator}</div>'
        f'</div>'
    )


def _render_market_glossary(keys):
    items = []
    for key in keys:
        text = METRIC_GLOSSARY.get(key)
        if text:
            items.append(f"<strong>{key}</strong> — {text}")
    if items:
        st.markdown('<div class="kpi-explainer">' + "<br>".join(items) + "</div>", unsafe_allow_html=True)


def render_kpi_card(
    label: str,
    value: str,
    interpretation: str,
    tone: str,
    help_text: str | None = None,
    why_important: str | None = None,
    rule_note: str | None = None,
    glossary_key: str | None = None,
    compact: bool = False,
) -> None:
    tone_cls = {
        "good": "status-good",
        "warn": "status-warn",
        "bad": "status-bad",
        "neutral": "status-neutral",
    }.get(str(tone or "neutral").lower(), "status-neutral")
    tone_label = {
        "good": "konstruktiv",
        "warn": "beobachten",
        "bad": "kritisch",
        "neutral": "informativ",
    }.get(str(tone or "neutral").lower(), "informativ")
    glossary_text = METRIC_GLOSSARY.get(glossary_key or label)
    what_it_means = help_text or glossary_text or "Keine zusätzliche Einordnung verfügbar."
    safe = lambda v: html.escape(str(v), quote=True)
    why_html = f'<div class="kpi-copy"><strong>Warum wichtig?</strong> {safe(why_important)}</div>' if why_important else ""
    rule_html = f'<div class="kpi-copy"><strong>Regelbasierte Einordnung:</strong> {safe(rule_note)}</div>' if rule_note else ""
    if compact:
        compact_rule = f'<div class="kpi-copy">{safe(rule_note)}</div>' if rule_note else ""
        st.markdown(
            f'<article class="kpi-card">'
            f'<div class="kpi-header"><div class="kpi-label">{safe(label)}</div>'
            f'<span class="status-chip {tone_cls}">{safe(tone_label)}</span></div>'
            f'<div class="kpi-value">{safe(value)}</div>'
            f"{compact_rule}"
            f'<div class="kpi-interpretation">{safe(interpretation)}</div>'
            f'</article>',
            unsafe_allow_html=True,
        )
        return

    st.markdown(
        f'<article class="kpi-card">'
        f'<div class="kpi-header"><div class="kpi-label">{safe(label)}</div>'
        f'<span class="status-chip {tone_cls}">{safe(tone_label)}</span></div>'
        f'<div class="kpi-value">{safe(value)}</div>'
        f'<div class="kpi-interpretation">{safe(interpretation)}</div>'
        f'<div class="kpi-copy"><strong>Was bedeutet das?</strong> {safe(what_it_means)}</div>'
        f"{why_html}"
        f"{rule_html}"
        f'</article>',
        unsafe_allow_html=True,
    )

def _simple_position_health(position: dict):
    ticker = position.get("ticker", "")
    if not ticker:
        return None
    df, info, *_ = load_stock_full(ticker)
    if df is None or len(df) < 20:
        return {"ticker": ticker, "status": "Keine Daten", "pnl": np.nan, "detail": "Yahoo-Daten fehlen"}
    latest = df.iloc[-1]
    raw_price = float(latest["Close"])
    market_currency = str((info or {}).get("currency", "USD") or "USD").upper()
    price = raw_price if market_currency == "EUR" else _usd_to_eur(raw_price)
    buy_price = _position_entry_price(position)
    pnl = ((price / buy_price) - 1) * 100 if buy_price else np.nan
    sma50 = df["Close"].rolling(50).mean().iloc[-1]
    sma50 = sma50 if market_currency == "EUR" else _usd_to_eur(sma50)
    status = "OK"
    detail = f"Aktuell {price:,.2f} €"
    if not np.isnan(pnl) and pnl < -7:
        status = "Stop-Loss"
        detail = f"{pnl:.1f}% seit Kauf"
    elif not np.isnan(sma50) and price < sma50:
        status = "Unter 50-SMA"
        detail = f"{price:,.2f} € unter {sma50:,.2f} €"
    elif not np.isnan(pnl):
        detail = f"{pnl:+.1f}% seit Kauf"
    return {"ticker": ticker, "name": (info or {}).get("shortName", ticker), "status": status, "pnl": pnl, "detail": detail, "price": price}


def _safe_float(value, default=np.nan):
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _round_half_up_int(value) -> int:
    if value is None or pd.isna(value):
        return 0
    return int(Decimal(str(float(value))).quantize(Decimal("1"), rounding=ROUND_HALF_UP))

def _get_portfolio_settings() -> dict:
    _init_workspace_state()
    settings = dict(_default_portfolio_settings())
    raw = st.session_state.get("portfolio_settings", {})
    if isinstance(raw, dict):
        settings.update(raw)
    numeric_fields = {
        "cash_balance",
        "risk_per_position_pct",
        "target_risk_contribution",
        "max_depot_loss_low",
        "max_depot_loss_high",
        "position_monitor_threshold_atr",
        "position_monitor_atr_period",
        "position_monitor_lookback_days",
        "position_monitor_interval_minutes",
        "position_monitor_cooldown_hours",
    }
    for key in numeric_fields:
        fallback = _default_portfolio_settings().get(key, 0.0)
        try:
            settings[key] = float(settings.get(key, fallback))
        except Exception:
            settings[key] = fallback
    settings["curve_start_date"] = str(settings.get("curve_start_date", "") or "").strip()
    settings["position_monitor_enabled"] = _safe_bool(settings.get("position_monitor_enabled", False))
    ref = str(settings.get("position_monitor_reference", "high_since_buy") or "high_since_buy").strip().lower()
    settings["position_monitor_reference"] = ref if ref in POSITION_MONITOR_REFERENCE_LABELS else "high_since_buy"
    settings["position_monitor_pushover_user_key"] = str(settings.get("position_monitor_pushover_user_key", "") or "").strip()
    settings["position_monitor_pushover_app_token"] = str(settings.get("position_monitor_pushover_app_token", "") or "").strip()
    rs_source = str(settings.get("rs_rating_source", RS_SOURCE_CSV_LATEST) or RS_SOURCE_CSV_LATEST).strip().lower()
    if rs_source == RS_SOURCE_FRED_CSV:
        settings["rs_rating_source"] = RS_SOURCE_FRED_CSV
    elif rs_source == RS_SOURCE_COMPUTED:
        settings["rs_rating_source"] = RS_SOURCE_COMPUTED
    else:
        settings["rs_rating_source"] = RS_SOURCE_CSV_LATEST
    return settings


def _get_rs_rating_source_setting() -> str:
    settings = _get_portfolio_settings()
    source = str(settings.get("rs_rating_source", RS_SOURCE_CSV_LATEST) or RS_SOURCE_CSV_LATEST).strip().lower()
    if source == RS_SOURCE_FRED_CSV:
        return RS_SOURCE_FRED_CSV
    if source == RS_SOURCE_COMPUTED:
        return RS_SOURCE_COMPUTED
    return RS_SOURCE_CSV_LATEST

def _save_portfolio_settings(settings: dict) -> None:
    merged = dict(_default_portfolio_settings())
    current = st.session_state.get("portfolio_settings", {})
    if isinstance(current, dict):
        merged.update(current)
    if isinstance(settings, dict):
        merged.update(settings)
    st.session_state["portfolio_settings"] = merged
    _sync_workspace()


def _get_position_monitor_state(store=None) -> dict:
    try:
        store = store or _get_price_store()
        _init_price_cache_db(store)
        raw = _get_cache_metadata(store, _workspace_meta_key(POSITION_MONITOR_STATE_FIELD), "{}")
        payload = json.loads(raw) if raw else {}
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}

def _adjust_cash_balance(delta_usd: float) -> float:
    _init_workspace_state()
    settings = _get_portfolio_settings()
    current_cash = _safe_float(settings.get("cash_balance"), 0.0)
    new_cash = current_cash + float(delta_usd or 0.0)
    settings["cash_balance"] = max(new_cash, 0.0)
    st.session_state["portfolio_settings"] = settings
    _sync_workspace()
    return float(settings["cash_balance"])

def _append_cash_flow_entry(date_value, flow_type: str, amount: float, note: str = "") -> None:
    _init_workspace_state()
    try:
        date_str = pd.Timestamp(date_value).strftime("%Y-%m-%d")
    except Exception:
        return
    amt = _safe_float(amount, np.nan)
    if np.isnan(amt) or amt <= 0:
        return
    clean_type = "deposit" if str(flow_type).lower() == "deposit" else "withdrawal"
    rows = list(st.session_state.get("portfolio_cash_flows", []))
    rows.append({
        "date": date_str,
        "type": clean_type,
        "amount": float(amt),
        "note": str(note or ""),
    })
    rows = sorted(rows, key=lambda r: (str(r.get("date", "")), str(r.get("type", ""))))
    st.session_state["portfolio_cash_flows"] = rows[-3000:]
    _sync_workspace()

def _remove_cash_flow_entry(index_value: int) -> None:
    _init_workspace_state()
    rows = list(st.session_state.get("portfolio_cash_flows", []))
    if 0 <= index_value < len(rows):
        rows.pop(index_value)
    st.session_state["portfolio_cash_flows"] = rows
    _sync_workspace()

def _persist_curve_start_from_widget() -> None:
    _init_workspace_state()
    try:
        selected = pd.Timestamp(st.session_state.get("pf_auto_curve_start")).date()
    except Exception:
        return
    settings = _get_portfolio_settings()
    settings["curve_start_date"] = str(selected)
    st.session_state["portfolio_settings"] = settings
    _sync_workspace()

def _position_entry_price(position: dict) -> float:
    if position.get("buy_price_eur") is not None:
        return _safe_float(position.get("buy_price_eur"), np.nan)
    currency = str(position.get("currency", "EUR") or "EUR").upper()
    raw_price = _safe_float(position.get("buy_price"), np.nan)
    if currency == "EUR" and not np.isnan(raw_price):
        return raw_price
    usd_price = _safe_float(position.get("buy_price_usd"), raw_price)
    return _usd_to_eur(usd_price) if not np.isnan(usd_price) else np.nan

def _position_stop_pct(position: dict) -> float:
    val = _safe_float(position.get("stop_pct"), np.nan)
    return val if not np.isnan(val) and val > 0 else np.nan

def _position_stop_price(position: dict) -> float:
    manual = _safe_float(position.get("stop_price"), np.nan)
    if not np.isnan(manual) and manual > 0:
        currency = str(position.get("currency", "EUR") or "EUR").upper()
        return manual if currency == "EUR" else _usd_to_eur(manual)
    entry = _position_entry_price(position)
    stop_pct = _position_stop_pct(position)
    if entry > 0 and not np.isnan(stop_pct):
        return entry * (1 - stop_pct / 100)
    return np.nan

def _price_to_eur(price: float, currency: str, trade_date) -> tuple[float, float | None]:
    value = float(price or 0.0)
    if str(currency).upper() == "EUR":
        return value, None
    rate = None
    try:
        fx = yf.Ticker("EURUSD=X").history(start=pd.Timestamp(trade_date) - timedelta(days=5), end=pd.Timestamp(trade_date) + timedelta(days=3))
        if fx is not None and len(fx) > 0:
            eur_usd = float(fx["Close"].iloc[-1])
            rate = 1.0 / eur_usd if eur_usd > 0 else None
    except Exception:
        rate = None
    if rate is None:
        rate = _usd_eur_rate()
    return value * float(rate), float(rate)


_FX_TO_EUR_FALLBACK = {
    "USD": 0.9259,
    "CAD": 0.68,
    "HKD": 0.118,
    "GBP": 1.17,
    "JPY": 0.0061,
    "ILS": 0.25,
    "CHF": 1.05,
    "AUD": 0.61,
    "DKK": 0.134,
    "NOK": 0.087,
    "SEK": 0.09,
}
_MINOR_UNIT_CURRENCY_SCALE = {
    "GBp": ("GBP", 0.01),
    "GBX": ("GBP", 0.01),
    "ILA": ("ILS", 0.01),
}


def _infer_ticker_currency(ticker: str) -> str:
    symbol = str(ticker or "").upper().strip().replace(".", "-")
    if not symbol:
        return "USD"
    if symbol.endswith(("-DE", "-F", "-PA", "-AS", "-MI", "-MC", "-BE", "-BR")):
        return "EUR"
    if symbol.endswith("-TO"):
        return "CAD"
    if symbol.endswith("-HK"):
        return "HKD"
    if symbol.endswith("-L"):
        return "GBP"
    if symbol.endswith("-T"):
        return "JPY"
    if symbol.endswith("-TA") or symbol.endswith("-IL"):
        return "ILS"
    return "USD"


@st.cache_data(ttl=3600, show_spinner=False)
def _ticker_market_currency(ticker: str) -> str:
    symbol = str(ticker or "").upper().strip().replace(".", "-")
    # Most exchange suffixes are unambiguous. Avoid expensive Yahoo metadata
    # calls for every historical holding in the TR export.
    if not symbol.endswith("-L"):
        return _infer_ticker_currency(symbol)
    for variant in _symbol_variants(ticker):
        try:
            currency = str(getattr(yf.Ticker(variant).fast_info, "currency", "") or "").strip()
            if currency:
                return currency.upper() if currency.isupper() else currency
        except Exception:
            continue
    return _infer_ticker_currency(ticker)


@st.cache_data(ttl=3600, show_spinner=False)
def _fx_to_eur_series(currency: str, start_date, end_date) -> pd.Series:
    raw_currency = str(currency or "EUR").strip()
    currency_key = raw_currency.upper()
    major_currency, unit_scale = _MINOR_UNIT_CURRENCY_SCALE.get(raw_currency, (currency_key, 1.0))
    start_ts = pd.Timestamp(start_date).normalize()
    end_ts = pd.Timestamp(end_date).normalize()
    if major_currency == "EUR":
        return pd.Series(float(unit_scale), index=pd.DatetimeIndex([start_ts]))
    pair = f"EUR{major_currency}=X"
    fx_close = _fetch_close_history(pair, start_ts, end_ts)
    if fx_close is not None and len(fx_close):
        fx_close = pd.to_numeric(fx_close, errors="coerce").dropna()
        fx_close = fx_close[fx_close > 0]
        if len(fx_close):
            return (1.0 / fx_close) * float(unit_scale)
    fallback = _FX_TO_EUR_FALLBACK.get(major_currency, _usd_eur_rate())
    return pd.Series(float(fallback) * float(unit_scale), index=pd.DatetimeIndex([start_ts]))


def _convert_close_series_to_eur(close: pd.Series, ticker: str, calendar: pd.DatetimeIndex) -> pd.Series:
    if close is None or len(close) == 0:
        return pd.Series(dtype=float)
    currency = _ticker_market_currency(ticker)
    rates = _fx_to_eur_series(currency, calendar.min(), calendar.max())
    rates.index = pd.to_datetime(rates.index, errors="coerce").normalize()
    rates = rates[~rates.index.isna()].sort_index()
    aligned_rates = rates.reindex(calendar, method="ffill").ffill().bfill().fillna(1.0)
    close_series = pd.to_numeric(pd.Series(close).copy(), errors="coerce")
    close_series.index = pd.to_datetime(close_series.index, errors="coerce").normalize()
    close_series = close_series[~close_series.index.isna()].sort_index()
    aligned_close = close_series.reindex(calendar, method="ffill").ffill().bfill()
    return aligned_close * aligned_rates


def _build_trade_price_close_series(group: pd.DataFrame, calendar: pd.DatetimeIndex) -> pd.Series:
    if group is None or len(group) == 0 or len(calendar) == 0:
        return pd.Series(dtype=float)
    prices = group.copy()
    prices["date"] = pd.to_datetime(prices.get("date"), errors="coerce").dt.normalize()
    prices["price_num"] = pd.to_numeric(prices.get("price_num", prices.get("price")), errors="coerce")
    prices = prices.dropna(subset=["date"])
    prices = prices[prices["price_num"].notna() & (prices["price_num"] > 0)]
    if prices.empty:
        return pd.Series(dtype=float)
    daily = prices.groupby("date")["price_num"].last().sort_index()
    return daily.reindex(calendar, method="ffill").ffill().bfill().dropna()


def _normalize_single_ticker(value: str) -> str:
    if value is None:
        return ""
    try:
        if isinstance(value, float) and np.isnan(value):
            return ""
    except Exception:
        pass
    t = str(value).strip().upper()
    t = t.replace(".", "-").replace("/", "-").replace(" ", "")
    return t


def _symbol_variants(symbol: str) -> list[str]:
    base = _normalize_single_ticker(symbol)
    if not base:
        return []
    variants = [base]
    # Keep the app's canonical display/storage form dash-normalized, but try
    # Yahoo's exchange suffix syntax too: RHM.DE, AGI.TO, 1211.HK, GAW.L, ...
    dotted = base.replace("-", ".")
    dashed = base.replace(".", "-")
    for cand in (dotted, dashed):
        cand = str(cand or "").strip().upper()
        if cand and cand not in variants:
            variants.append(cand)
    try:
        lookup = _search_yahoo_symbol_candidates(base)
        for cand in lookup.get("candidates", [])[:6]:
            if "=" in str(cand) or "^" in str(cand):
                continue
            cand = _normalize_single_ticker(cand)
            if cand and cand not in variants:
                variants.append(cand)
    except Exception:
        pass
    return variants


def _coerce_ohlc_frame(frame: pd.DataFrame | None) -> pd.DataFrame:
    if frame is None or len(frame) == 0:
        return pd.DataFrame()
    df = frame.copy()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()
    for col in ["Open", "High", "Low", "Close", "Volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "Close" in df.columns:
        df = df.dropna(subset=["Close"])
    return df


@st.cache_data(ttl=900, show_spinner=False)
def _bulk_download_ohlc(symbols: tuple[str, ...], start_date, end_date) -> dict[str, pd.DataFrame]:
    canonical = [_normalize_single_ticker(s) for s in symbols if _normalize_single_ticker(s)]
    if not canonical:
        return {}
    start_ts = pd.Timestamp(start_date).normalize() - timedelta(days=7)
    end_ts = pd.Timestamp(end_date).normalize() + timedelta(days=3)

    variants_by_symbol = {sym: _symbol_variants(sym) for sym in canonical}
    requested = []
    for vars_ in variants_by_symbol.values():
        requested.extend(vars_)
    requested = list(dict.fromkeys(requested))
    if not requested:
        return {}

    raw_by_variant: dict[str, pd.DataFrame] = {}
    try:
        raw = yf.download(requested, start=start_ts, end=end_ts, progress=False, auto_adjust=True, group_by="ticker", threads=True)
    except Exception:
        raw = None

    if raw is not None and len(raw) > 0:
        if isinstance(raw.columns, pd.MultiIndex):
            for variant in requested:
                frame = None
                try:
                    frame = raw[variant]
                except Exception:
                    try:
                        frame = raw.xs(variant, axis=1, level=0)
                    except Exception:
                        frame = None
                frame = _coerce_ohlc_frame(frame)
                if not frame.empty:
                    raw_by_variant[variant] = frame
        else:
            frame = _coerce_ohlc_frame(raw)
            if not frame.empty:
                raw_by_variant[requested[0]] = frame

    for variant in requested:
        if variant in raw_by_variant:
            continue
        try:
            hist = yf.Ticker(variant).history(start=start_ts, end=end_ts, auto_adjust=True)
        except Exception:
            hist = None
        frame = _coerce_ohlc_frame(hist)
        if not frame.empty:
            raw_by_variant[variant] = frame

    resolved: dict[str, pd.DataFrame] = {}
    for sym, variants in variants_by_symbol.items():
        for variant in variants:
            frame = raw_by_variant.get(variant)
            if frame is not None and not frame.empty:
                resolved[sym] = frame
                break
    return resolved


@st.cache_data(ttl=900, show_spinner=False)
def _bulk_close_history_map(symbols: tuple[str, ...], start_date, end_date) -> dict[str, pd.Series]:
    start_ts = pd.Timestamp(start_date).normalize()
    end_ts = pd.Timestamp(end_date).normalize()
    frames = _bulk_download_ohlc(symbols, start_date, end_date)
    out: dict[str, pd.Series] = {}
    for sym, frame in frames.items():
        if frame is None or frame.empty or "Close" not in frame:
            continue
        close = pd.to_numeric(frame["Close"], errors="coerce").dropna()
        if close.empty:
            continue
        idx = pd.to_datetime(close.index, errors="coerce")
        # yfinance can return tz-aware DatetimeIndex (e.g. UTC). For date-only
        # filtering we compare normalized, tz-naive timestamps to avoid
        # "Invalid comparison" between tz-aware and tz-naive values.
        if getattr(idx, "tz", None) is not None:
            idx = idx.tz_localize(None)
        close.index = idx.normalize()
        close = close[~close.index.duplicated(keep="last")].sort_index()
        close = close[(close.index >= start_ts) & (close.index <= end_ts)]
        if close.empty:
            continue
        out[sym] = close
    return out


_SELL_METRICS_SCHEMA_VERSION = "2026-05-24-ema21"


@st.cache_data(ttl=900, show_spinner=False)
def load_sell_decision_metrics(ticker: str, buy_date, buy_price: float, shares: float, benchmark_ticker: str = "SPY", currency: str = "", cache_buster: int = 0, pivot_date=None) -> dict:
    """Load cached Yahoo data and build reusable metrics for the sell-decision area.

    Der Hash-Aufruf auf ``_SELL_METRICS_SCHEMA_VERSION`` ist gewollt: Wird der
    Schlüsselsatz im Metrics-Payload (z. B. ``sma21`` → ``ema21``) geändert,
    bewirkt das Bumpen der Version, dass Streamlit beim nächsten Aufruf den
    Cache verwirft und das UI wieder konsistente Werte erhält.
    """
    _ = (cache_buster, _SELL_METRICS_SCHEMA_VERSION)
    norm_ticker = _normalize_single_ticker(ticker)
    norm_benchmark = _normalize_single_ticker(benchmark_ticker or "SPY") or "SPY"
    if not norm_ticker:
        return {"ok": False, "error": "Ticker fehlt.", "ticker": "", "benchmark_ticker": norm_benchmark, "metrics": {}, "manual_defaults": {}, "as_of": ""}
    try:
        buy_ts = pd.Timestamp(buy_date).normalize()
    except Exception:
        return {"ok": False, "error": "Ungültiges Einstiegsdatum.", "ticker": norm_ticker, "benchmark_ticker": norm_benchmark, "metrics": {}, "manual_defaults": {}, "as_of": ""}
    pivot_ts = None
    if pivot_date not in (None, ""):
        try:
            pivot_ts = pd.Timestamp(pivot_date).normalize()
        except Exception:
            pivot_ts = None

    end = datetime.now(timezone.utc).replace(tzinfo=None)
    # 420 calendar days usually covers >=250 trading days; include pre-buy data for pivot defaults.
    earliest_ref = pivot_ts.to_pydatetime() if pivot_ts is not None else buy_ts.to_pydatetime()
    start = min(end - timedelta(days=420), earliest_ref - timedelta(days=60))
    frames = _bulk_download_ohlc((norm_ticker, norm_benchmark), start, end)
    price_frame = frames.get(norm_ticker, pd.DataFrame())
    benchmark_frame = frames.get(norm_benchmark, pd.DataFrame())

    market_currency = str(currency or "").upper().strip()
    if not market_currency:
        try:
            market_currency = str((_portfolio_symbol_metrics(norm_ticker) or {}).get("currency", "USD") or "USD").upper()
        except Exception:
            market_currency = "USD"

    pnl_abs_eur = None
    fx_rate_to_eur = None
    try:
        current_close = pd.to_numeric(price_frame.get("Close", pd.Series(dtype=float)), errors="coerce").dropna()
        if not current_close.empty:
            pnl_abs_market = (float(current_close.iloc[-1]) - float(buy_price or 0.0)) * float(shares or 0.0)
            if market_currency == "EUR":
                pnl_abs_eur = pnl_abs_market
                fx_rate_to_eur = 1.0
            else:
                pnl_abs_eur, fx_rate_to_eur = _price_to_eur(pnl_abs_market, market_currency, end.date())
    except Exception:
        pnl_abs_eur = None
        fx_rate_to_eur = None

    return build_sell_decision_metrics_payload(
        ticker=norm_ticker,
        buy_date=buy_ts,
        buy_price=buy_price,
        shares=shares,
        price_frame=price_frame,
        benchmark_frame=benchmark_frame,
        benchmark_ticker=norm_benchmark,
        currency=market_currency,
        pnl_abs_eur=pnl_abs_eur,
        fx_rate_to_eur=fx_rate_to_eur,
        pivot_date=pivot_ts,
    )


def debug_sell_decision_metrics_smoke_test() -> list[dict]:
    """Best-effort smoke check for three liquid tickers; intended for local debugging."""
    results = []
    for sample in build_sell_decision_metrics_smoke_inputs():
        results.append(load_sell_decision_metrics(**sample))
    return results


def _beta_from_close_series(close: pd.Series, benchmark_close: pd.Series, window: int = 120) -> float:
    try:
        if close is None or benchmark_close is None or len(close) < 20 or len(benchmark_close) < 20:
            return np.nan
        joined = pd.concat(
            [close.pct_change(fill_method=None).rename("asset"), benchmark_close.pct_change(fill_method=None).rename("bench")],
            axis=1,
            join="inner",
        ).dropna()
        if len(joined) < 20:
            return np.nan
        joined = joined.tail(window)
        bench_var = joined["bench"].var()
        if pd.isna(bench_var) or bench_var <= 0:
            return np.nan
        return float(joined["asset"].cov(joined["bench"]) / bench_var)
    except Exception:
        return np.nan

@st.cache_data(ttl=900, show_spinner=False)
def _fetch_close_history(symbol: str, start_date, end_date):
    symbol = _normalize_single_ticker(symbol)
    start_ts = pd.Timestamp(start_date).normalize()
    end_ts = pd.Timestamp(end_date).normalize()
    if not symbol or end_ts < start_ts:
        return pd.Series(dtype=float)
    try:
        batch = _bulk_close_history_map((symbol,), start_ts, end_ts)
        close = batch.get(symbol, pd.Series(dtype=float))
        if len(close):
            return close[(close.index >= start_ts) & (close.index <= end_ts)]
    except Exception:
        pass

    for variant in _symbol_variants(symbol):
        df = _dl(variant, start_ts - timedelta(days=7), end_ts + timedelta(days=3))
        if df is None or df.empty or "Close" not in df:
            try:
                hist = yf.Ticker(variant).history(start=start_ts - timedelta(days=7), end=end_ts + timedelta(days=3), auto_adjust=True)
            except Exception:
                hist = None
            df = _coerce_ohlc_frame(hist)
        else:
            df = _coerce_ohlc_frame(df)
        if df is None or df.empty or "Close" not in df:
            continue
        close = pd.to_numeric(df["Close"], errors="coerce").dropna()
        if close.empty:
            continue
        close.index = pd.to_datetime(close.index).normalize()
        close = close[~close.index.duplicated(keep="last")]
        close = close.sort_index()
        close = close[(close.index >= start_ts) & (close.index <= end_ts)]
        return close
    return pd.Series(dtype=float)

@st.cache_data(ttl=900, show_spinner=False)
def _portfolio_symbol_metrics(ticker: str) -> dict:
    original_ticker = str(ticker or "").upper().strip()
    ticker = _normalize_single_ticker(original_ticker)
    if not ticker:
        return {}

    def _fallback_payload(name=None, sector="", industry="", price=np.nan, atr_pct=np.nan, beta=np.nan, currency="USD"):
        return {
            "ticker": ticker,
            "name": name or ticker,
            "sector": sector or "",
            "industry": industry or "",
            "price": price,
            "currency": str(currency or "USD").upper(),
            "atr_pct": atr_pct,
            "beta": beta,
        }

    df, info, *_ = load_stock_full(ticker)
    if df is not None and len(df) >= 30:
        latest_price = _safe_float(df["Close"].iloc[-1], np.nan)
        atr_val = _atr(df, 21).iloc[-1] if len(df) >= 21 else np.nan
        atr_pct = (atr_val / latest_price * 100) if latest_price and not np.isnan(latest_price) and not np.isnan(atr_val) else np.nan
        beta = _safe_float((info or {}).get("beta"), np.nan)
        if np.isnan(beta):
            spx = _fetch_close_history("^GSPC", datetime.now() - timedelta(days=260), datetime.now())
            beta = _beta_from_close_series(pd.to_numeric(df["Close"], errors="coerce"), spx)
        return _fallback_payload(
            name=(info or {}).get("shortName", ticker),
            sector=(info or {}).get("sector", ""),
            industry=(info or {}).get("industry", ""),
            price=latest_price,
            currency=(info or {}).get("currency", "USD"),
            atr_pct=atr_pct,
            beta=beta,
        )

    series = _fetch_close_history(ticker, datetime.now() - timedelta(days=260), datetime.now())
    latest_price = _safe_float(series.iloc[-1], np.nan) if len(series) else np.nan
    atr_pct = np.nan
    if len(series) >= 22 and not np.isnan(latest_price) and latest_price > 0:
        atr_pct = float(series.pct_change(fill_method=None).abs().rolling(21).mean().iloc[-1] * 100 * 1.6)
    spx = _fetch_close_history("^GSPC", datetime.now() - timedelta(days=260), datetime.now())
    beta = _beta_from_close_series(series, spx)
    name = ticker
    try:
        fast = yf.Ticker(ticker).fast_info
        if hasattr(fast, "get") and np.isnan(latest_price):
            latest_price = _safe_float(fast.get("lastPrice", np.nan), np.nan)
    except Exception:
        pass
    try:
        info = yf.Ticker(ticker).info or {}
    except Exception:
        info = {}
    if info:
        name = info.get("shortName") or info.get("longName") or ticker
        if np.isnan(beta):
            beta = _safe_float(info.get("beta", np.nan), np.nan)
        return _fallback_payload(
            name=name,
            sector=info.get("sector", ""),
            industry=info.get("industry", ""),
            price=latest_price,
            currency=info.get("currency", "USD"),
            atr_pct=atr_pct,
            beta=beta,
        )
    return _fallback_payload(name=name, price=latest_price, atr_pct=atr_pct, beta=beta)


@st.cache_data(ttl=900, show_spinner=False)
def _bulk_portfolio_metrics(tickers: tuple[str, ...]) -> dict[str, dict]:
    normalized = tuple(dict.fromkeys(_normalize_single_ticker(t) for t in tickers if _normalize_single_ticker(t)))
    if not normalized:
        return {}
    end = datetime.now()
    start = end - timedelta(days=260)
    frames = _bulk_download_ohlc(normalized, start, end)
    spx_close = _fetch_close_history("^GSPC", start, end)
    out: dict[str, dict] = {}
    for ticker in normalized:
        frame = frames.get(ticker)
        if frame is None or frame.empty or "Close" not in frame:
            out[ticker] = _portfolio_symbol_metrics(ticker)
            continue
        latest_price = _safe_float(frame["Close"].iloc[-1], np.nan)
        atr_pct = np.nan
        if len(frame) >= 21 and {"High", "Low", "Close"}.issubset(frame.columns):
            atr_val = _atr(frame, 21).iloc[-1]
            if latest_price and not np.isnan(latest_price) and not np.isnan(atr_val):
                atr_pct = float(atr_val / latest_price * 100)
        if np.isnan(atr_pct) and len(frame) >= 22 and latest_price and not np.isnan(latest_price):
            atr_pct = float(pd.to_numeric(frame["Close"], errors="coerce").pct_change(fill_method=None).abs().rolling(21).mean().iloc[-1] * 100 * 1.6)
        beta = _beta_from_close_series(pd.to_numeric(frame["Close"], errors="coerce"), spx_close)
        out[ticker] = {
            "ticker": ticker,
            "name": ticker,
            "sector": "",
            "industry": "",
            "price": latest_price,
            "currency": "EUR" if ticker.endswith((".DE", ".PA", ".AS", ".MI", ".MC", ".F")) else "USD",
            "atr_pct": atr_pct,
            "beta": beta,
        }
        if np.isnan(beta) or np.isnan(latest_price):
            fallback = _portfolio_symbol_metrics(ticker)
            merged = dict(fallback or {})
            merged.update({k: v for k, v in out[ticker].items() if not (isinstance(v, float) and np.isnan(v))})
            out[ticker] = merged
    return out

@st.cache_data(ttl=3600, show_spinner=False)
def _sp500_atr_reference(lookback_days: int = 180) -> float:
    end = datetime.now()
    start = end - timedelta(days=lookback_days)
    df = _dl("^GSPC", start, end)
    if df is None or len(df) < 30:
        return np.nan
    latest_price = _safe_float(df["Close"].iloc[-1], np.nan)
    atr_val = _atr(df, 21).iloc[-1]
    return (atr_val / latest_price * 100) if latest_price and not np.isnan(latest_price) and not np.isnan(atr_val) else np.nan

def _portfolio_positions_only(positions: list[dict]) -> list[dict]:
    out = []
    for pos in positions or []:
        shares = _safe_float((pos or {}).get("shares"), 0.0)
        if shares > 0:
            out.append(pos)
    return out

def _build_portfolio_snapshot(positions: list[dict], cash_balance: float = 0.0, target_risk_contribution: float = 0.20) -> tuple[pd.DataFrame, dict]:
    tracked = _portfolio_positions_only(positions)
    tickers = tuple(_normalize_single_ticker((pos or {}).get("ticker", "")) for pos in tracked if _normalize_single_ticker((pos or {}).get("ticker", "")))
    metrics_map = _bulk_portfolio_metrics(tickers)

    preliminary_rows = []
    total_value = max(float(cash_balance or 0), 0.0)
    for pos in tracked:
        raw_ticker = str((pos or {}).get("ticker", "")).upper().strip()
        ticker = _normalize_single_ticker(raw_ticker)
        shares = _safe_float(pos.get("shares"), 0.0)
        if not ticker or shares <= 0:
            continue
        metrics = metrics_map.get(ticker) or _portfolio_symbol_metrics(ticker)
        raw_price = _safe_float(metrics.get("price"), np.nan)
        market_currency = str(metrics.get("currency", "USD") or "USD").upper()
        price = raw_price if market_currency == "EUR" else _usd_to_eur(raw_price)
        entry = _position_entry_price(pos)
        current_value = shares * price if not np.isnan(price) else np.nan
        if not np.isnan(current_value):
            total_value += current_value
        preliminary_rows.append({
            "ticker": ticker,
            "name": metrics.get("name", ticker),
            "sector": metrics.get("sector", ""),
            "industry": metrics.get("industry", ""),
            "shares": shares,
            "entry": entry,
            "current_price": price,
            "current_value": current_value,
            "buy_date": pos.get("buy_date", ""),
            "pivot_tag": pos.get("pivot_tag", ""),
            "currency": pos.get("currency", "EUR"),
            "market_currency": market_currency,
            "note": pos.get("note", ""),
            "stop_pct": _position_stop_pct(pos),
            "stop_price": _position_stop_price(pos),
            "atr_pct": _safe_float(metrics.get("atr_pct"), np.nan),
            "beta": _safe_float(metrics.get("beta"), np.nan),
            "is_cash": False,
        })

    total_value = total_value if total_value > 0 else np.nan
    spx_atr_pct = _sp500_atr_reference()
    rows = []
    for row in preliminary_rows:
        weight = (row["current_value"] / total_value) if total_value and not np.isnan(total_value) and not np.isnan(row["current_value"]) else np.nan
        pnl_pct = ((row["current_price"] / row["entry"]) - 1) * 100 if row["entry"] and row["entry"] > 0 and not np.isnan(row["current_price"]) else np.nan
        pnl_abs = (row["current_price"] - row["entry"]) * row["shares"] if row["entry"] and row["entry"] > 0 and not np.isnan(row["current_price"]) else np.nan
        score = np.nan
        if not np.isnan(row["beta"]) and not np.isnan(row["atr_pct"]) and not np.isnan(spx_atr_pct) and spx_atr_pct > 0:
            score = 0.60 * row["beta"] + 0.40 * (row["atr_pct"] / spx_atr_pct)
        risk_contribution = weight * score if not np.isnan(weight) and not np.isnan(score) else np.nan
        # Bei sehr niedrigem Score ergäbe sich rechnerisch ein max_weight > 1, was als
        # „mehr als das gesamte Depot" absurd ist; deshalb auf 1.0 (= 100 %) cappen.
        max_weight_raw = (target_risk_contribution / score) if not np.isnan(score) and score > 0 else np.nan
        max_weight = min(1.0, max_weight_raw) if not np.isnan(max_weight_raw) else np.nan
        max_position_value = total_value * max_weight if not np.isnan(total_value) and not np.isnan(max_weight) else np.nan
        stop_distance_pct = ((row["current_price"] / row["stop_price"]) - 1) * 100 if not np.isnan(row["current_price"]) and not np.isnan(row["stop_price"]) and row["stop_price"] > 0 else np.nan
        position_risk_abs = row["shares"] * max(row["current_price"] - row["stop_price"], 0) if not np.isnan(row["current_price"]) and not np.isnan(row["stop_price"]) else np.nan
        rows.append({
            **row,
            "weight": weight,
            "pnl_pct": pnl_pct,
            "pnl_abs": pnl_abs,
            "score": score,
            "risk_contribution": risk_contribution,
            "max_weight": max_weight,
            "max_position_value": max_position_value,
            "stop_distance_pct": stop_distance_pct,
            "position_risk_abs": position_risk_abs,
        })

    if cash_balance and float(cash_balance) > 0:
        cash_value = float(cash_balance)
        cash_weight = (cash_value / total_value) if total_value and not np.isnan(total_value) else np.nan
        rows.append({
            "ticker": "CASH",
            "name": "Cash",
            "sector": "",
            "industry": "",
            "shares": cash_value,
            "entry": 1.0,
            "current_price": 1.0,
            "current_value": cash_value,
            "buy_date": "",
            "pivot_tag": "",
            "currency": "EUR",
            "market_currency": "EUR",
            "note": "Freie Liquidität",
            "stop_pct": 0.0,
            "stop_price": 1.0,
            "atr_pct": 0.0,
            "beta": 0.0,
            "is_cash": True,
            "weight": cash_weight,
            "pnl_pct": 0.0,
            "pnl_abs": 0.0,
            "score": 0.0,
            "risk_contribution": 0.0,
            "max_weight": np.nan,
            "max_position_value": np.nan,
            "stop_distance_pct": 0.0,
            "position_risk_abs": 0.0,
        })

    df = pd.DataFrame(rows)
    if df.empty:
        summary = {
            "tracked_count": 0,
            "total_value": max(float(cash_balance or 0), 0.0),
            "invested_value": 0.0,
            "cash_balance": max(float(cash_balance or 0), 0.0),
            "cash_ratio": 1.0 if float(cash_balance or 0) > 0 else 0.0,
            "portfolio_atr_pct": np.nan,
            "beta_balancer": np.nan,
            "max_depot_loss_pct": np.nan,
            "spx_atr_pct": spx_atr_pct,
        }
        return df, summary

    invested_df = df[df["is_cash"] == False] if "is_cash" in df else df
    invested_value = float(invested_df["current_value"].sum(skipna=True)) if not invested_df.empty else 0.0
    cash_value = max(float(cash_balance or 0), 0.0)
    total_value = invested_value + cash_value
    portfolio_atr_pct = float((df["weight"] * df["atr_pct"]).sum(skipna=True))
    beta_balancer = float(df["risk_contribution"].sum(skipna=True))
    valid_risks = invested_df["position_risk_abs"].dropna() if not invested_df.empty else pd.Series(dtype=float)
    max_depot_loss_pct = (float(valid_risks.sum()) / total_value * 100) if total_value > 0 and len(valid_risks) else np.nan
    summary = {
        "tracked_count": int(len(invested_df)),
        "total_value": total_value,
        "invested_value": invested_value,
        "cash_balance": cash_value,
        "cash_ratio": (cash_value / total_value) if total_value > 0 else np.nan,
        "portfolio_atr_pct": portfolio_atr_pct,
        "beta_balancer": beta_balancer,
        "max_depot_loss_pct": max_depot_loss_pct,
        "spx_atr_pct": spx_atr_pct,
    }
    df = df.assign(_cash_sort=df["is_cash"].astype(int)).sort_values(["_cash_sort", "pnl_pct"], ascending=[True, False], na_position="last").drop(columns=["_cash_sort"])
    return df, summary

def _portfolio_health_cards(summary: dict, positions_df: pd.DataFrame, settings: dict) -> list[dict]:
    cards = []
    count = int(summary.get("tracked_count", 0))
    if count < 8:
        cards.append({
            "tone": "warn",
            "title": "Depotstruktur",
            "value": f"{count} Positionen",
            "status": "unter Korridor",
            "detail": "Kapitel 7.2 empfiehlt meist einen Korridor von 8 bis 12 Aktien.",
        })
    elif count > 12:
        cards.append({
            "tone": "warn",
            "title": "Depotstruktur",
            "value": f"{count} Positionen",
            "status": "über Korridor",
            "detail": "Der empfohlene Korridor liegt bei 8 bis 12 Aktien. Mehr Positionen machen das Depot schwerer steuerbar.",
        })
    else:
        cards.append({
            "tone": "good",
            "title": "Depotstruktur",
            "value": f"{count} Positionen",
            "status": "im Korridor",
            "detail": "Du liegst im empfohlenen Korridor von 8 bis 12 Aktien.",
        })

    max_loss = _safe_float(summary.get("max_depot_loss_pct"), np.nan)
    low = _safe_float(settings.get("max_depot_loss_low"), 8.0)
    high = _safe_float(settings.get("max_depot_loss_high"), 12.0)
    if not np.isnan(max_loss):
        if max_loss < low:
            cards.append({
                "tone": "neutral",
                "title": "Max. Depotverlust",
                "value": f"{max_loss:.1f}%",
                "status": "defensiv",
                "detail": f"Der Wert liegt unter deinem Zielkorridor von {low:.0f} bis {high:.0f}%.",
            })
        elif max_loss > high:
            cards.append({
                "tone": "bad",
                "title": "Max. Depotverlust",
                "value": f"{max_loss:.1f}%",
                "status": "über Ziel",
                "detail": f"Der modellierte Verlust liegt über deinem Zielkorridor von {low:.0f} bis {high:.0f}%. Prüfe Positionsgrößen und Stopps zuerst.",
            })
        else:
            cards.append({
                "tone": "good",
                "title": "Max. Depotverlust",
                "value": f"{max_loss:.1f}%",
                "status": "im Ziel",
                "detail": f"Der Wert liegt im Zielkorridor von {low:.0f} bis {high:.0f}%.",
            })

    atr_pct = _safe_float(summary.get("portfolio_atr_pct"), np.nan)
    if not np.isnan(atr_pct):
        if atr_pct > 4:
            cards.append({
                "tone": "bad",
                "title": "Portfolio ATR",
                "value": f"{atr_pct:.2f}%",
                "status": "sehr aggressiv",
                "detail": "Im Buch gilt ein Wert über 4% als sehr aggressiv. Das Depot reagiert stark auf Schwankungen.",
            })
        elif atr_pct >= 2.5:
            cards.append({
                "tone": "neutral",
                "title": "Portfolio ATR",
                "value": f"{atr_pct:.2f}%",
                "status": "offensiv",
                "detail": "Das ist ordentlich Bewegung im Depot, aber noch kontrollierbar.",
            })
        else:
            cards.append({
                "tone": "good",
                "title": "Portfolio ATR",
                "value": f"{atr_pct:.2f}%",
                "status": "ruhig",
                "detail": "Das Depot wirkt aktuell vergleichsweise ruhig.",
            })

    balancer = _safe_float(summary.get("beta_balancer"), np.nan)
    if not np.isnan(balancer):
        if balancer >= 2.5:
            cards.append({
                "tone": "bad",
                "title": "Beta-Balancer",
                "value": f"{balancer:.2f}",
                "status": "sehr dynamisch",
                "detail": "Das entspricht einem schwankungsintensiven Depot mit hoher Marktsensitivität.",
            })
        elif balancer >= 1.5:
            cards.append({
                "tone": "neutral",
                "title": "Beta-Balancer",
                "value": f"{balancer:.2f}",
                "status": "offensiv",
                "detail": "Das ist offensiv, aber noch steuerbar.",
            })
        else:
            cards.append({
                "tone": "good",
                "title": "Beta-Balancer",
                "value": f"{balancer:.2f}",
                "status": "ausgewogen",
                "detail": "Der Wert liegt näher am Marktrisiko des S&P 500.",
            })

    target_rc = _safe_float(settings.get("target_risk_contribution"), 0.20)
    if not positions_df.empty and "risk_contribution" in positions_df:
        over = positions_df[positions_df["risk_contribution"] > target_rc]
        if len(over):
            tickers = ", ".join(over["ticker"].head(4).tolist())
            more_count = max(len(over) - 4, 0)
            more_text = f" und {more_count} weitere" if more_count else ""
            cards.append({
                "tone": "warn",
                "title": "Positionsbudget",
                "value": f"{len(over)} über Ziel",
                "status": f"Ziel {target_rc:.2f}",
                "detail": f"Über dem Ziel-Risikobeitrag: {tickers}{more_text}. Priorisiere diese Positionen beim Rebalancing.",
            })
    return cards


def _render_portfolio_health_cards(summary: dict, positions_df: pd.DataFrame, settings: dict) -> None:
    cards = _portfolio_health_cards(summary, positions_df, settings)
    if not cards:
        return
    safe = lambda value: html.escape(str(value or ""), quote=True)
    card_html = []
    for card in cards:
        tone = safe(card.get("tone", "neutral"))
        card_html.append(
            f'<article class="portfolio-health-card portfolio-health-card--{tone}">'
            f'<div class="portfolio-health-card__top">'
            f'<div class="portfolio-health-card__label">{safe(card.get("title"))}</div>'
            f'<span class="portfolio-health-card__status">{safe(card.get("status"))}</span>'
            f'</div>'
            f'<div class="portfolio-health-card__value">{safe(card.get("value"))}</div>'
            f'<div class="portfolio-health-card__detail">{safe(card.get("detail"))}</div>'
            f'</article>'
        )
    st.markdown(
        '<section class="portfolio-health-section">'
        '<div class="section-header">'
        '<div class="section-eyebrow">Depot-Kompass</div>'
        '<p class="section-subtitle">Einordnung der wichtigsten Depot-Hinweise nach Struktur, Verlustbudget und Schwankungsrisiko.</p>'
        '</div>'
        '<div class="portfolio-health-grid">'
        + "".join(card_html)
        + '</div></section>',
        unsafe_allow_html=True,
    )

def _build_reconstructed_portfolio_curve(positions: list[dict], cash_balance: float, start_date, end_date=None, cash_flows=None) -> pd.DataFrame:
    tracked = _portfolio_positions_only(positions)
    start_ts = pd.Timestamp(start_date).normalize()
    end_ts = pd.Timestamp(end_date or datetime.now(timezone.utc).date()).normalize()
    if end_ts < start_ts:
        return pd.DataFrame()

    bench = _fetch_close_history("^GSPC", start_ts, end_ts)
    if len(bench):
        calendar = pd.DatetimeIndex(bench.index).normalize().unique().sort_values()
    else:
        calendar = pd.date_range(start_ts, end_ts, freq="B")
    if len(calendar) == 0:
        return pd.DataFrame()

    curve = pd.DataFrame({"date": pd.DatetimeIndex(calendar)})
    curve["depot_value"] = float(cash_balance or 0.0)

    ticker_tuple = tuple(dict.fromkeys(_normalize_single_ticker((pos or {}).get("ticker", "")) for pos in tracked if _normalize_single_ticker((pos or {}).get("ticker", ""))))
    close_map = _bulk_close_history_map(ticker_tuple, start_ts, end_ts) if ticker_tuple else {}

    for pos in tracked:
        ticker = _normalize_single_ticker(pos.get("ticker", ""))
        shares = _safe_float(pos.get("shares"), 0.0)
        if not ticker or shares <= 0:
            continue
        try:
            buy_ts = pd.Timestamp(pos.get("buy_date")).normalize() if pos.get("buy_date") else start_ts
        except Exception:
            buy_ts = start_ts
        entry_price = _position_entry_price(pos)
        entry_value = shares * entry_price if not np.isnan(entry_price) and entry_price > 0 else 0.0
        if buy_ts > start_ts and entry_value > 0:
            curve.loc[curve["date"] < buy_ts, "depot_value"] += entry_value
        active_start = max(start_ts, buy_ts)
        close = close_map.get(ticker)
        if close is None or len(close) == 0:
            close = _fetch_close_history(ticker, active_start, end_ts)
        mask = curve["date"] >= active_start
        if close is None or len(close) == 0:
            if entry_price > 0:
                fallback_idx = pd.DatetimeIndex(curve.loc[mask, "date"])
                close = pd.Series(entry_price, index=fallback_idx, dtype=float)
            else:
                continue
        aligned = close.reindex(pd.DatetimeIndex(curve["date"]), method="ffill")
        values = aligned.reindex(pd.DatetimeIndex(curve.loc[mask, "date"]), method="ffill").ffill().bfill().fillna(0.0).to_numpy()
        curve.loc[mask, "depot_value"] += values * shares

    flow_df = pd.DataFrame(cash_flows or [])
    if not flow_df.empty and "date" in flow_df:
        flow_df["date"] = pd.to_datetime(flow_df["date"], errors="coerce").dt.normalize()
        flow_df = flow_df.dropna(subset=["date"]).copy()
        flow_df["amount"] = pd.to_numeric(flow_df.get("amount", 0), errors="coerce").fillna(0.0)
        flow_df["type"] = flow_df.get("type", "").astype(str).str.lower()
        flow_df["deposit"] = np.where(flow_df["type"] == "deposit", flow_df["amount"], 0.0)
        flow_df["withdrawal"] = np.where(flow_df["type"] == "withdrawal", flow_df["amount"], 0.0)
        by_day = flow_df.groupby("date", as_index=False)[["deposit", "withdrawal"]].sum()
        flow_map = by_day.set_index("date")
        curve["deposit"] = curve["date"].dt.normalize().map(flow_map["deposit"]).fillna(0.0)
        curve["withdrawal"] = curve["date"].dt.normalize().map(flow_map["withdrawal"]).fillna(0.0)
        for row in by_day.itertuples(index=False):
            day = pd.Timestamp(row.date).normalize()
            curve.loc[curve["date"] < day, "depot_value"] += float(row.withdrawal) - float(row.deposit)
    else:
        curve["deposit"] = 0.0
        curve["withdrawal"] = 0.0

    curve["date"] = pd.to_datetime(curve["date"], errors="coerce")
    curve = curve.dropna(subset=["date"]).copy()
    curve = curve[curve["depot_value"].notna()].copy()
    if curve.empty:
        return pd.DataFrame()
    if (curve["depot_value"] <= 0).all():
        return pd.DataFrame()

    index_values = [100.0]
    for idx in range(1, len(curve)):
        prev_value = float(curve.iloc[idx - 1]["depot_value"])
        current_value = float(curve.iloc[idx]["depot_value"])
        net_flow = float(curve.iloc[idx]["deposit"]) - float(curve.iloc[idx]["withdrawal"])
        if prev_value <= 0:
            day_return = 0.0
        else:
            adjusted_profit = current_value - prev_value - net_flow
            day_return = adjusted_profit / prev_value
        index_values.append(index_values[-1] * (1 + day_return))
    curve["portfolio_index"] = index_values
    curve["portfolio_index_sma10"] = curve["portfolio_index"].rolling(10, min_periods=10).mean()
    curve["portfolio_index_sma21"] = curve["portfolio_index"].rolling(21, min_periods=21).mean()
    if len(bench):
        aligned = bench.reindex(curve["date"].dt.normalize(), method="ffill")
        curve["sp500_close"] = aligned.values
        valid = curve["sp500_close"].dropna()
        if len(valid):
            start_bench = float(valid.iloc[0])
            curve["sp500_index"] = curve["sp500_close"] / start_bench * 100
        else:
            curve["sp500_index"] = np.nan
    else:
        curve["sp500_index"] = np.nan
    return curve


def _resolve_isin_ticker_map(tx_df: pd.DataFrame, overrides: dict | None = None) -> tuple[dict[str, str], list[dict]]:
    """Map ISINs from a Trade Republic transactions frame to Yahoo tickers.

    Returns (isin → ticker) plus a per-ISIN diagnostics list with name/asset_class
    for the UI. Overrides take precedence over ISIN_TO_YAHOO and the Yahoo search.
    """
    overrides = {str(k).upper().strip(): _normalize_single_ticker(str(v)) for k, v in (overrides or {}).items() if str(v or "").strip()}
    if tx_df is None or tx_df.empty or "symbol" not in tx_df.columns:
        return {}, []
    df = tx_df.copy()
    df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()
    df = df[df["symbol"].ne("") & df["asset_class"].astype(str).str.upper().isin({"STOCK", "FUND"})]
    if df.empty:
        return {}, []
    ticker_for: dict[str, str] = {}
    diagnostics: list[dict] = []
    for isin, group in df.groupby("symbol"):
        name = str(group["name"].iloc[0] or "")
        asset_class = str(group["asset_class"].iloc[0] or "").upper()
        ticker = overrides.get(isin) or _suggest_yahoo_ticker(isin, name, asset_class)
        ticker = _normalize_single_ticker(ticker)
        if ticker:
            ticker_for[isin] = ticker
        diagnostics.append({
            "isin": isin,
            "name": name,
            "asset_class": asset_class,
            "ticker": ticker,
        })
    return ticker_for, diagnostics


def _build_curve_from_transactions(
    tx_df: pd.DataFrame,
    isin_to_ticker: dict[str, str] | None = None,
    end_date=None,
    start_date=None,
    cash_series: pd.Series | None = None,
) -> pd.DataFrame:
    """Build a daily equity curve from a Trade Republic transactions CSV.

    Data flow:
    * Per-ISIN share trajectories from signed `shares` values (TR encodes
      buy/sell direction in the sign; DIVIDEND rows carry shares for reporting
      only and are skipped).
    * Position value per day = Σ shares × Yahoo close converted to EUR. If
      Yahoo has no history for a resolved ticker (e.g. delisted closed
      holdings) or the instrument is a derivative without Yahoo history, the
      curve falls back to TR trade prices from the CSV.
    * Cash trajectory by default rebuilt from the CSV itself via
      ``amount + fee + tax`` cumulative — TR's export contains every cash
      event (deposits, withdrawals, tax optimisation, dividends, interest,
      trade fees), so the running sum reproduces the official Kontoauszug
      end balance to the cent. ``cash_series`` can override this (e.g. for
      tests).
    * ``start_date`` (optional) — the day from which the equity index is
      normalized to 100. Rows before this date are dropped from the output;
      share holdings and cash up to that day are still correctly carried
      forward via the running totals.
    """
    if tx_df is None or tx_df.empty:
        return pd.DataFrame()

    df = tx_df.copy()
    df["_row_order"] = np.arange(len(df))
    event_source = df.get("event_ts", df.get("datetime", df.get("date")))
    df["event_ts"] = pd.to_datetime(event_source, errors="coerce", utc=True).dt.tz_convert(None)
    df["date"] = pd.to_datetime(df.get("date"), errors="coerce", utc=True).dt.tz_convert(None).dt.normalize()
    df["event_ts"] = df["event_ts"].fillna(df["date"])
    df = df.dropna(subset=["date"]).sort_values(["event_ts", "_row_order"], kind="stable").reset_index(drop=True)
    df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()
    df["type"] = df["type"].astype(str).str.upper().str.strip()
    df["asset_class"] = df["asset_class"].astype(str).str.upper().str.strip()
    df["shares_num"] = pd.to_numeric(df.get("shares_num", df.get("shares")), errors="coerce").fillna(0.0)

    tx_start_ts = pd.Timestamp(df["date"].min()).normalize()
    end_ts = pd.Timestamp(end_date or datetime.now(timezone.utc).date()).normalize()
    if end_ts < tx_start_ts:
        return pd.DataFrame()

    bench = _fetch_close_history("^GSPC", tx_start_ts, end_ts)
    if len(bench):
        calendar = pd.DatetimeIndex(bench.index).normalize().unique().sort_values()
    else:
        calendar = pd.DatetimeIndex(pd.date_range(tx_start_ts, end_ts, freq="B"))
    if len(calendar) == 0:
        return pd.DataFrame()

    # ---- Positions side ----
    isin_to_ticker = {str(k).upper().strip(): _normalize_single_ticker(v) for k, v in (isin_to_ticker or {}).items() if _normalize_single_ticker(v)}
    df_pos = df[df["asset_class"].isin({"STOCK", "FUND", "DERIVATIVE"})]
    ticker_tuple = tuple(dict.fromkeys(t for t in isin_to_ticker.values() if t))
    close_map = _bulk_close_history_map(ticker_tuple, tx_start_ts, end_ts) if ticker_tuple else {}

    positions_value = pd.Series(0.0, index=calendar)
    resolved_isins: list[str] = []
    unresolved_isins: list[str] = []
    price_fallback_isins: list[str] = []
    open_price_fallback_isins: list[str] = []
    # Events that don't move share counts (cash-side or reporting only).
    NON_SHARE_EVENTS = {"DIVIDEND", "INTEREST_PAYMENT", "TAX_OPTIMIZATION", "SEC_ACCOUNT"}
    for isin, group in df_pos.groupby("symbol"):
        if not isin:
            continue
        ticker = isin_to_ticker.get(isin)
        close = close_map.get(ticker) if ticker else pd.Series(dtype=float)
        if ticker and (close is None or len(close) == 0):
            close = _fetch_close_history(ticker, tx_start_ts, end_ts)
        close_is_eur = False
        if close is None or len(close) == 0:
            close = _build_trade_price_close_series(group, calendar)
            if close is None or len(close) == 0:
                unresolved_isins.append(isin)
                continue
            close_is_eur = True
            price_fallback_isins.append(isin)
        shares_series = pd.Series(0.0, index=calendar)
        running = 0.0
        for row in group.sort_values(["event_ts", "_row_order"], kind="stable").itertuples():
            typ = str(getattr(row, "type", "") or "").upper()
            if typ in NON_SHARE_EVENTS:
                continue
            raw_shares = float(getattr(row, "shares_num", 0.0) or 0.0)
            if raw_shares == 0.0:
                continue
            # Handle both TR variants: some exports encode SELL as negative shares,
            # others as positive shares with direction coming from `type`.
            if typ in {"SELL", "TRANSFER_OUT", "WARRANT_EXERCISE", "INSOLVENCY_PROCEEDINGS", "DELISTED", "EXPIRATION"}:
                signed_shares = -abs(raw_shares)
            elif typ in {"BUY", "TRANSFER_IN", "SELL_CANCELLED"}:
                signed_shares = abs(raw_shares)
            else:
                signed_shares = raw_shares
            # TR exports SPLIT rows with the *post-split total holding* in
            # `shares` (not a buy/sell delta). Treating it as delta would
            # overstate holdings and inflate the reconstructed depot value.
            if typ == "SPLIT":
                running = max(abs(raw_shares), 0.0)
            else:
                running = max(running + signed_shares, 0.0)
            day = pd.Timestamp(row.date).normalize()
            shares_series.loc[shares_series.index >= day] = running
        aligned = close.reindex(calendar, method="ffill").ffill().bfill().fillna(0.0)
        if not close_is_eur:
            aligned = _convert_close_series_to_eur(aligned, ticker, calendar).fillna(0.0)
        if close_is_eur and len(shares_series) and float(shares_series.iloc[-1]) > 1e-9:
            open_price_fallback_isins.append(isin)
        positions_value = positions_value.add(shares_series.values * aligned.values, fill_value=0.0)
        resolved_isins.append(isin)

    # ---- Cash side ----
    if cash_series is not None and len(cash_series):
        cash_aligned = pd.Series(cash_series).copy()
        cash_aligned.index = pd.to_datetime(cash_aligned.index, errors="coerce").normalize()
        cash_aligned = cash_aligned[~cash_aligned.index.isna()]
        cash_values = cash_aligned.reindex(calendar, method="ffill").fillna(0.0).values
    else:
        cash_values = _build_cash_trajectory_from_csv(df, calendar).values

    curve = pd.DataFrame({
        "date": calendar,
        "positions_value": positions_value.values,
        "cash": cash_values,
    })
    curve["depot_value"] = curve["positions_value"] + curve["cash"]

    # ---- External cash flows (TWR-neutral) ----
    # Deposits, withdrawals, tax optimisations move the depot value without being
    # the result of investment performance — they must be removed from the equity
    # index so the curve only reflects P&L (price moves, dividends, interest,
    # realized gains/losses, fees). Dividends/interest stay P&L-positive and
    # therefore are NOT in this set.
    EXTERNAL_FLOW_TYPES = {
        "CUSTOMER_INBOUND", "CUSTOMER_OUTBOUND_REQUEST", "CUSTOMER_INPAYMENT",
        "TRANSFER_INBOUND", "TRANSFER_INSTANT_INBOUND", "GIFT",
        "TAX_OPTIMIZATION",
    }
    ext_mask = df["type"].isin(EXTERNAL_FLOW_TYPES)
    if ext_mask.any():
        ext = df[ext_mask].copy()
        for col in ("amount_num", "fee_num", "tax_num"):
            if col not in ext.columns:
                ext[col] = pd.to_numeric(ext.get(col.split("_")[0], 0), errors="coerce").fillna(0.0)
        ext["delta"] = ext["amount_num"] + ext["fee_num"] + ext["tax_num"]
        ext_daily = ext.groupby(ext["date"].dt.normalize())["delta"].sum()
        ext_series = ext_daily.reindex(calendar, fill_value=0.0)
    else:
        ext_series = pd.Series(0.0, index=calendar)
    curve["external_flow"] = ext_series.values

    # ---- Start-date filter & TWR normalization ----
    if start_date is not None:
        try:
            start_ts = pd.Timestamp(start_date).normalize()
        except Exception:
            start_ts = None
    else:
        start_ts = None
    if start_ts is not None:
        curve = curve[curve["date"] >= start_ts].reset_index(drop=True)
    else:
        # Default: start at first day with non-zero positions.
        first_invested = curve["positions_value"].gt(0).idxmax() if (curve["positions_value"] > 0).any() else None
        if first_invested is not None and first_invested > 0:
            curve = curve.loc[first_invested:].reset_index(drop=True)

    if curve.empty or float(curve["depot_value"].iloc[0]) <= 0:
        return pd.DataFrame()

    # Time-weighted return: each day's return is the depot change MINUS the
    # external flow that came in/out that day, divided by yesterday's depot value.
    # Compounded → index starts at 100 on the first day.
    index_values = [100.0]
    depot_vals = curve["depot_value"].tolist()
    ext_vals = curve["external_flow"].tolist()
    for i in range(1, len(curve)):
        prev = float(depot_vals[i - 1])
        curr = float(depot_vals[i])
        ext = float(ext_vals[i])
        if prev > 0:
            day_return = (curr - prev - ext) / prev
        else:
            day_return = 0.0
        index_values.append(index_values[-1] * (1.0 + day_return))
    curve["portfolio_index"] = index_values
    curve["portfolio_index_sma10"] = curve["portfolio_index"].rolling(10, min_periods=10).mean()
    curve["portfolio_index_sma21"] = curve["portfolio_index"].rolling(21, min_periods=21).mean()

    if len(bench):
        bench_aligned = bench.reindex(curve["date"], method="ffill")
        curve["sp500_close"] = bench_aligned.values
        valid = curve["sp500_close"].dropna()
        if len(valid):
            curve["sp500_index"] = curve["sp500_close"] / float(valid.iloc[0]) * 100
        else:
            curve["sp500_index"] = np.nan
    else:
        curve["sp500_index"] = np.nan

    curve.attrs["resolved_isins"] = resolved_isins
    curve.attrs["unresolved_isins"] = unresolved_isins
    curve.attrs["price_fallback_isins"] = price_fallback_isins
    curve.attrs["open_price_fallback_isins"] = open_price_fallback_isins
    return curve


def _evaluate_position_engine(
    position: dict,
    manual_map: dict | None = None,
    tranche_log: list[dict] | None = None,
    cache_buster: int = 0,
) -> dict:
    """Run the shared sell-decision engine for one saved position.

    Combines `load_sell_decision_metrics` + `compute_sell_health_score` +
    `evaluate_sell_decision` so Mein-Depot Verkaufskandidaten, Verkaufs-
    entscheidung Portfolio-Ranking and Live-Monitor produce identical
    Health-Score, Status and Tranche-% per position. Returns
    `{ok, error, ticker, metrics_payload, metrics, manual, health, result}`.
    """
    base = {
        "ok": False, "error": "", "ticker": "",
        "metrics_payload": {}, "metrics": {}, "manual": {},
        "health": {}, "result": {},
    }
    ticker = _normalize_single_ticker((position or {}).get("ticker", ""))
    base["ticker"] = ticker
    if not ticker:
        base["error"] = "Ticker fehlt."
        return base
    try:
        buy_date = pd.Timestamp((position or {}).get("buy_date")).date()
    except Exception:
        base["error"] = "Kaufdatum fehlt/ungültig."
        return base
    buy_price = _safe_float((position or {}).get("buy_price"), np.nan)
    if np.isnan(buy_price) or buy_price <= 0:
        base["error"] = "Einstandspreis fehlt/ungültig."
        return base
    shares = _safe_float((position or {}).get("shares"), 0.0)
    stored_currency = str((position or {}).get("currency", "EUR") or "EUR").upper()
    try:
        symbol_metrics = _portfolio_symbol_metrics(ticker) or {}
        market_currency = str(symbol_metrics.get("currency", stored_currency) or stored_currency).upper()
        metric_buy_price = _sell_monitor_buy_price_for_market(
            ticker, buy_price, stored_currency, buy_date, market_currency,
        )
        pivot_tag_raw = (position or {}).get("pivot_tag")
        pivot_tag_for_metrics = None
        if pivot_tag_raw:
            try:
                pivot_tag_for_metrics = pd.Timestamp(pivot_tag_raw).date()
            except Exception:
                pivot_tag_for_metrics = None
        metrics_payload = load_sell_decision_metrics(
            ticker, buy_date, metric_buy_price, shares,
            benchmark_ticker="SPY", currency=market_currency,
            cache_buster=cache_buster, pivot_date=pivot_tag_for_metrics,
        )
        if not metrics_payload.get("ok"):
            base["error"] = metrics_payload.get("error") or "Kennzahlen fehlen."
            return base
        manual = dict((manual_map or {}).get(ticker, {}) or {})
        result = evaluate_sell_decision(metrics_payload, manual, tranche_log or [])
        health = compute_sell_health_score(metrics_payload, manual)
        base.update({
            "ok": True,
            "metrics_payload": metrics_payload,
            "metrics": metrics_payload.get("metrics", {}) or {},
            "manual": manual,
            "health": health,
            "result": result,
        })
    except Exception as exc:
        logger.debug("Position engine evaluation failed for %s: %s", ticker, exc)
        base["error"] = "Auswertung fehlgeschlagen. Prüfe Ticker, Kaufdatum und Yahoo-Verfügbarkeit."
    return base


def _build_sell_candidates_table(snapshot_df: pd.DataFrame, positions: list[dict]) -> pd.DataFrame:
    """Depot-Verkaufskandidaten aus derselben Engine wie Verkaufsentscheidung."""
    if snapshot_df is None or snapshot_df.empty:
        return pd.DataFrame()
    invested = snapshot_df[snapshot_df.get("is_cash", False) == False].copy() if "is_cash" in snapshot_df else snapshot_df.copy()
    if invested.empty:
        return pd.DataFrame()

    pos_by_ticker = {
        _normalize_single_ticker((pos or {}).get("ticker", "")): pos
        for pos in (positions or [])
        if _normalize_single_ticker((pos or {}).get("ticker", ""))
    }
    state = load_sell_decision_state()
    manual_map = state.get("positions_manual", {}) if isinstance(state, dict) else {}
    tranche_log = load_tranche_log()
    cache_buster = int(st.session_state.get("sell_rank_cache_buster", 0) or 0)

    rows = []
    for _, raw in invested.iterrows():
        ticker = _normalize_single_ticker(raw.get("ticker", ""))
        if not ticker:
            continue
        position = pos_by_ticker.get(ticker, {})
        payload = _evaluate_position_engine(position, manual_map, tranche_log, cache_buster)
        if not payload.get("ok"):
            rows.append({
                "Ticker": ticker,
                "P&L %": _safe_float(raw.get("pnl_pct"), np.nan),
                "Health-Score": np.nan,
                "Empf. Tranche %": 0,
                "Status": "Fehler",
                "Begründung": payload.get("error") or "Auswertung fehlgeschlagen.",
                "_sort": (4, 0, 0.0),
            })
            continue
        health = payload.get("health") or {}
        result = payload.get("result") or {}
        metrics = payload.get("metrics") or {}
        tranche = int(result.get("sell_now_percent", result.get("recommendation_percent", 0)) or 0)
        status = str(health.get("status") or "—")
        status_rank = {"Verkaufen": 0, "Beobachten": 1, "Halten": 2}.get(status, 3)
        reasons = health.get("reasons") or []
        rows.append({
            "Ticker": ticker,
            "P&L %": _safe_float(metrics.get("pnl_pct"), np.nan),
            "Health-Score": _safe_float(health.get("health_score"), np.nan),
            "Empf. Tranche %": tranche,
            "Status": _sell_ranking_status_badge(status),
            "Begründung": " · ".join(reasons[:4]) if reasons else "Keine kritischen Signale.",
            "_sort": (status_rank, -tranche, _safe_float(health.get("health_score"), 100.0)),
        })

    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df = df.sort_values("_sort").drop(columns=["_sort"]).reset_index(drop=True)
    return df


def _depot_state() -> dict:
    """Einmalige Snapshot-Berechnung, die alle Depot-Tabs teilen."""
    _init_workspace_state()
    settings = _get_portfolio_settings()
    positions = st.session_state.get("positions", [])
    snapshot_df, summary = _build_portfolio_snapshot(
        positions,
        cash_balance=_safe_float(settings.get("cash_balance"), 0.0),
        target_risk_contribution=_safe_float(settings.get("target_risk_contribution"), 0.20),
    )
    return {
        "settings": settings,
        "positions": positions,
        "snapshot_df": snapshot_df,
        "summary": summary,
    }


def _render_depot_overview(state: dict | None = None) -> None:
    state = state or _depot_state()
    snapshot_df = state["snapshot_df"]
    summary = state["summary"]
    settings = state["settings"]

    total_value = _safe_float(summary.get("total_value"), 0.0)
    cash_balance = _safe_float(summary.get("cash_balance"), 0.0)
    invested_value = _safe_float(summary.get("invested_value"), 0.0)
    tracked_count = int(summary.get("tracked_count", 0) or 0)
    cash_ratio = _safe_float(summary.get("cash_ratio"), np.nan)
    pf_atr = _safe_float(summary.get("portfolio_atr_pct"), np.nan)
    beta_balancer = _safe_float(summary.get("beta_balancer"), np.nan)
    max_depot_loss = _safe_float(summary.get("max_depot_loss_pct"), np.nan)
    target_rc = _safe_float(settings.get("target_risk_contribution"), 0.20)
    loss_low = _safe_float(settings.get("max_depot_loss_low"), 8.0)
    loss_high = _safe_float(settings.get("max_depot_loss_high"), 12.0)

    total_pnl_abs = 0.0
    total_pnl_pct = np.nan
    if snapshot_df is not None and not snapshot_df.empty:
        invested = snapshot_df[snapshot_df.get("is_cash", False) == False] if "is_cash" in snapshot_df else snapshot_df
        total_pnl_abs = float(invested["pnl_abs"].sum(skipna=True)) if not invested.empty else 0.0
        cost_basis = float((invested["entry"] * invested["shares"]).sum(skipna=True)) if not invested.empty else 0.0
        if cost_basis > 0:
            total_pnl_pct = total_pnl_abs / cost_basis * 100

    pnl_tone = "#22c55e" if total_pnl_abs >= 0 else "#ef4444"
    pnl_sign = "+" if total_pnl_abs >= 0 else ""
    pnl_pct_text = f"{pnl_sign}{total_pnl_pct:.2f}%" if not np.isnan(total_pnl_pct) else "—"
    st.markdown(
        f"""
        <div style="background: linear-gradient(135deg,#f7f6f1 0%,#eeece4 100%); border-radius: 14px; padding: 18px 22px; margin-bottom: 14px;">
          <div style="display:flex; flex-wrap:wrap; gap: 24px; align-items:flex-end; justify-content:space-between;">
            <div>
              <div style="font-size:12px;color:#6b6a64;letter-spacing:.4px;text-transform:uppercase;">Depotwert</div>
              <div style="font-size:30px;font-weight:600;font-variant-numeric:tabular-nums;">{_format_eur(total_value)}</div>
              <div style="font-size:12px;color:#6b6a64;margin-top:2px;">
                {tracked_count} Position{"en" if tracked_count != 1 else ""} · Investiert {_format_eur(invested_value)} · Cash {_format_eur(cash_balance)}
              </div>
            </div>
            <div style="text-align:right;">
              <div style="font-size:12px;color:#6b6a64;letter-spacing:.4px;text-transform:uppercase;">Unrealisiert</div>
              <div style="font-size:24px;font-weight:600;color:{pnl_tone};font-variant-numeric:tabular-nums;">{pnl_sign}{_format_eur(total_pnl_abs)}</div>
              <div style="font-size:12px;color:#6b6a64;margin-top:2px;">{pnl_pct_text} ggü. Einstand</div>
            </div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    k1, k2, k3, k4 = st.columns(4)
    cash_ratio_text = f"{cash_ratio*100:.1f}%" if not np.isnan(cash_ratio) else "—"
    pf_atr_text = f"{pf_atr:.2f}%" if not np.isnan(pf_atr) else "—"
    beta_text = f"{beta_balancer:.2f}" if not np.isnan(beta_balancer) else "—"
    loss_text = f"{max_depot_loss:.2f}%" if not np.isnan(max_depot_loss) else "—"
    loss_target = f"Ziel {loss_low:.0f}–{loss_high:.0f}%"
    with k1:
        st.metric("Cashquote", cash_ratio_text)
    with k2:
        st.metric("Portfolio ATR", pf_atr_text, help="Gewichteter ATR-Mittelwert über alle Positionen.")
    with k3:
        st.metric("Beta-Balancer", beta_text, help="Summe der Risikobeiträge (Gewicht × (0,6 Beta + 0,4 ATR/Markt-ATR)).")
    with k4:
        st.metric("Max. Depotverlust", loss_text, help=f"Aggregierter Stop-Loss aller Positionen. Korridor laut Regler: {loss_target}.")

    with st.expander("Was bedeutet Ziel-Risikobeitrag 0,20?", expanded=False):
        st.markdown(
            f"""
Der Ziel-Risikobeitrag begrenzt, wie viel marktrisiko-gewichtetes Gewicht eine einzelne Position im Depot haben soll.
Die App berechnet je Position:

```
Risikobeitrag = Depotgewicht × Balancer-Score
Balancer-Score = 0,60 × Beta + 0,40 × (ATR% der Aktie / ATR% des S&P 500)
```

Ein Zielwert von **{target_rc:.2f}** bedeutet: Eine Position soll höchstens rund **{target_rc:.0%} Risiko-Punkte**
beitragen. Beispiel: Hat eine Aktie einen Balancer-Score von 1,5, ergibt sich ein Maximalgewicht von
**{target_rc:.2f} / 1,5 = {target_rc / 1.5:.1%}**. Höhere Beta- oder ATR-Werte senken also automatisch die sinnvolle Positionsgröße.
            """
        )

    _render_portfolio_health_cards(summary, snapshot_df, settings)

    if snapshot_df is None or snapshot_df.empty:
        st.info(
            "Noch keine Depotpositionen mit Stückzahl > 0 erfasst. "
            "Wechsle in den Tab „Positionen“, um eine Idee anzulegen."
        )
        return

    invested_df = snapshot_df[snapshot_df.get("is_cash", False) == False].copy() if "is_cash" in snapshot_df else snapshot_df.copy()
    if not invested_df.empty:
        st.markdown("#### 🥧 Allokation")
        alloc = invested_df[["ticker", "current_value"]].copy().rename(columns={"ticker": "Ticker", "current_value": "Wert"})
        if cash_balance > 0:
            alloc = pd.concat([alloc, pd.DataFrame([{"Ticker": "CASH", "Wert": cash_balance}])], ignore_index=True)
        alloc = alloc[alloc["Wert"] > 0]
        if not alloc.empty:
            fig = go.Figure(data=[go.Pie(
                labels=alloc["Ticker"], values=alloc["Wert"], hole=0.55, textinfo="label+percent",
            )])
            fig.update_layout(height=280, margin=dict(l=10, r=10, t=10, b=10), showlegend=False)
            st.plotly_chart(fig, width="stretch", key="depot_alloc_pie")

    st.markdown("#### 📋 Positionen (kompakt)")
    display_df = invested_df[[
        "ticker", "shares", "current_value", "weight", "pnl_pct", "stop_distance_pct", "risk_contribution",
    ]].copy()
    display_df["Gewicht %"] = display_df["weight"] * 100
    display_df = display_df.rename(columns={
        "ticker": "Ticker",
        "shares": "Stück",
        "current_value": "Wert EUR",
        "pnl_pct": "P&L %",
        "stop_distance_pct": "Abstand Stop %",
        "risk_contribution": "Risikobeitrag",
    })[["Ticker", "Stück", "Wert EUR", "Gewicht %", "P&L %", "Abstand Stop %", "Risikobeitrag"]]
    display_df = display_df.sort_values("Wert EUR", ascending=False).reset_index(drop=True)
    st.dataframe(
        display_df.round(2), width="stretch", hide_index=True,
        column_config={
            "Stück": st.column_config.NumberColumn("Stück", format="%.0f"),
            "Wert EUR": st.column_config.NumberColumn("Wert EUR", format="%.0f €"),
            "Gewicht %": st.column_config.ProgressColumn("Gewicht %", format="%.1f%%", min_value=0, max_value=100),
            "P&L %": st.column_config.NumberColumn("P&L %", format="%+.2f%%"),
            "Abstand Stop %": st.column_config.NumberColumn("Abstand Stop %", format="%+.2f%%"),
            "Risikobeitrag": st.column_config.NumberColumn("Risikobeitrag", format="%.3f"),
        },
    )
    st.caption("Vollständiges Risiko-Ranking inkl. Score/ATR/Beta findest du im Tab „Positionen“.")

    st.markdown("#### 🎯 Verkaufskandidaten · kombinierte Empfehlung")
    st.caption(
        "Health-Score 0–100 und Empfohlene Tranche % kommen aus derselben Engine wie "
        "Live-Monitor und Verkaufsentscheidung → Portfolio-Ranking. "
        "**≥ 65 → Halten · 40–64 → Beobachten · < 40 → Verkaufen.** "
        "Für Tranchen-Details, manuelle Overrides und Setup wechsle in den Tab „Verkaufs-Entscheidung“."
    )
    with st.spinner("Werte deine Positionen mit der Verkaufs-Engine aus …"):
        candidates = _build_sell_candidates_table(snapshot_df, state["positions"])
    if candidates.empty:
        st.info("Keine Positionen für die Bewertung verfügbar.")
        return
    st.dataframe(
        candidates,
        width="stretch",
        hide_index=True,
        column_config={
            "Ticker": st.column_config.TextColumn("Ticker", width="small"),
            "P&L %": st.column_config.NumberColumn("P&L %", format="%+.2f%%"),
            "Health-Score": st.column_config.ProgressColumn(
                "Health-Score", format="%.0f", min_value=0, max_value=100,
                help="Engine-Health-Score 0–100 aus P&L, MA-Lage (21/50), RS-Trend, Distribution-Tage und Drawdown.",
            ),
            "Empf. Tranche %": st.column_config.NumberColumn(
                "Empf. Tranche %", format="%d%%",
                help="Aktuell empfohlene Verkaufsmenge laut Hub-Regeln (0 = halten).",
            ),
            "Status": st.column_config.TextColumn("Status"),
            "Begründung": st.column_config.TextColumn("Begründung", width="large"),
        },
    )


def _render_depot_portfolio_regler(state: dict | None = None) -> None:
    state = state or _depot_state()
    settings = state["settings"]

    st.markdown("#### ⚙️ Einstellungen · Depot-Annahmen")
    st.markdown(
        """
Der Portfolio-Regler legt fest, **wie das Depot gemessen und Positionsgrößen berechnet werden**.
Die Werte fließen in den Snapshot, das Risiko-Ranking und den Stückzahl-Rechner ein:

- **Cash / freie Liquidität** — verfügbarer Cashbestand in EUR. Wird zum Depotwert addiert,
  bestimmt damit das Risikobudget und die Cashquote.
- **Max. Verlust je Idee %** — wie viel Prozent des Depots du pro Idee maximal riskieren willst.
  → `Risikobudget je Idee = Depotwert × Max. Verlust je Idee %`. Direkt verwendet im Rechner für
  die maximale Stückzahl.
- **Ziel Risikobeitrag** — obere Schranke für den marktrisiko-gewichteten Anteil einer Position
  (Beta-Balancer). Beispiel: 0,20 = eine Idee soll höchstens 0,20 Risiko-Punkte beitragen.
  → `Max. Gewicht = Ziel Risikobeitrag / Balancer-Score`.
- **Untergrenze / Obergrenze Max.-Depotverlust %** — Korridor für den aggregierten Stop-Loss
  aller Positionen. Wird in der Übersicht als KPI „Max. Depotverlust" angezeigt und löst
  Warnungen aus, wenn der Korridor verlassen wird.
        """
    )

    set_cols = st.columns(2)
    with set_cols[0]:
        cash_balance = st.number_input(
            "Cash / freie Liquidität (EUR)", min_value=0.0,
            value=float(settings.get("cash_balance", 0.0)), step=100.0,
            key="pf_cash_balance",
            help="Wird automatisch durch Käufe/Verkäufe und Cash-Flows angepasst.",
        )
        risk_per_position_pct = st.number_input(
            "Max. Verlust je Idee %", min_value=0.1, max_value=5.0,
            value=float(settings.get("risk_per_position_pct", 1.0)), step=0.1,
            key="pf_risk_pct",
            help="Buch-Empfehlung: meist 1 %. Höher = größere Positionen und höheres Idee-Risiko.",
        )
        target_risk_contribution = st.number_input(
            "Ziel Risikobeitrag (Balancer)", min_value=0.05, max_value=0.50,
            value=float(settings.get("target_risk_contribution", 0.20)), step=0.01,
            key="pf_target_rc",
            help="Niedriger = strengere Begrenzung auf hoch-Beta- und hoch-ATR-Aktien.",
        )
    with set_cols[1]:
        max_loss_low = st.number_input(
            "Untergrenze Max.-Depotverlust %", min_value=1.0, max_value=30.0,
            value=float(settings.get("max_depot_loss_low", 8.0)), step=0.5,
            key="pf_max_loss_low",
            help="Unter diesem Wert nutzt du das Risikobudget möglicherweise zu wenig aus.",
        )
        max_loss_high = st.number_input(
            "Obergrenze Max.-Depotverlust %", min_value=1.0, max_value=30.0,
            value=float(settings.get("max_depot_loss_high", 12.0)), step=0.5,
            key="pf_max_loss_high",
            help="Über diesem Wert wird das Gesamtrisiko zu aggressiv.",
        )

    if st.button("💾 Einstellungen speichern", width="stretch", key="pf_save_settings", type="primary"):
        _save_portfolio_settings({
            "cash_balance": float(cash_balance),
            "risk_per_position_pct": float(risk_per_position_pct),
            "target_risk_contribution": float(target_risk_contribution),
            "max_depot_loss_low": float(min(max_loss_low, max_loss_high)),
            "max_depot_loss_high": float(max(max_loss_low, max_loss_high)),
        })
        st.success("Einstellungen gespeichert. Snapshot und Rechner nutzen jetzt die neuen Werte.")
        st.rerun()


def _render_depot_positions_manager(state: dict | None = None) -> None:
    state = state or _depot_state()
    settings = state["settings"]
    all_positions = state["positions"]
    snapshot_df = state["snapshot_df"]
    tracked_positions = _portfolio_positions_only(all_positions)

    st.markdown("#### 📋 Gesamtübersicht")
    if snapshot_df is None or snapshot_df.empty:
        st.info("Noch keine Positionen. Lege unten eine neue Idee an.")
    else:
        invested_df = snapshot_df[snapshot_df.get("is_cash", False) == False].copy() if "is_cash" in snapshot_df else snapshot_df.copy()
        if not invested_df.empty:
            overview = invested_df[[
                "ticker", "name", "shares", "entry", "current_price", "current_value",
                "pnl_pct", "stop_pct", "stop_price", "stop_distance_pct",
                "atr_pct", "beta", "risk_contribution", "max_position_value", "pivot_tag",
            ]].copy().rename(columns={
                "ticker": "Ticker", "name": "Name", "shares": "Stück", "entry": "Einstand",
                "current_price": "Aktuell", "current_value": "Wert EUR",
                "pnl_pct": "P&L %", "stop_pct": "Stopp %", "stop_price": "Stoppkurs",
                "stop_distance_pct": "Abstand Stop %", "atr_pct": "ATR %",
                "beta": "Beta", "risk_contribution": "Risikobeitrag",
                "max_position_value": "Max. Wert", "pivot_tag": "Pivot-Tag",
            }).sort_values("Wert EUR", ascending=False).reset_index(drop=True)
            overview["Pivot-Tag"] = pd.to_datetime(overview["Pivot-Tag"], errors="coerce").dt.date
            depot_names = _ticker_display_names(tuple(overview["Ticker"].tolist()))
            overview["Name"] = overview.apply(
                lambda row: row["Name"] if str(row.get("Name", "") or "").strip() and str(row.get("Name", "")).upper() != str(row["Ticker"]).upper()
                else depot_names.get(str(row["Ticker"]), str(row["Ticker"])),
                axis=1,
            )
            overview.insert(0, "Auswählen", False)
            overview_rounded = overview.round(2)
            edited_overview = st.data_editor(
                overview_rounded,
                width="stretch",
                hide_index=True,
                key="pf_overview_editor",
                column_config={
                    "Auswählen": st.column_config.CheckboxColumn("Auswählen", help="Markiere Positionen für das Sammel-Löschen."),
                    "Ticker": st.column_config.TextColumn("Ticker", width="small"),
                    "Name": st.column_config.TextColumn("Name", width="medium"),
                    "Stück": st.column_config.NumberColumn("Stück", format="%.0f"),
                    "Einstand": st.column_config.NumberColumn("Einstand", format="%.2f €"),
                    "Aktuell": st.column_config.NumberColumn("Aktuell", format="%.2f €"),
                    "Wert EUR": st.column_config.NumberColumn("Wert EUR", format="%.0f €"),
                    "P&L %": st.column_config.NumberColumn("P&L %", format="%+.2f"),
                    "Stopp %": st.column_config.NumberColumn(
                        "Stopp %", format="%.1f", min_value=0.1, max_value=50.0, step=0.1,
                        help="Klicke in die Zelle, um den Stoppabstand direkt zu bearbeiten. Der Stoppkurs wird beim Speichern automatisch neu berechnet.",
                    ),
                    "Stoppkurs": st.column_config.NumberColumn("Stoppkurs", format="%.2f €"),
                    "Abstand Stop %": st.column_config.NumberColumn("Abstand Stop %", format="%+.2f"),
                    "ATR %": st.column_config.NumberColumn("ATR %", format="%.2f"),
                    "Beta": st.column_config.NumberColumn("Beta", format="%.2f"),
                    "Risikobeitrag": st.column_config.NumberColumn("Risikobeitrag", format="%.3f"),
                    "Max. Wert": st.column_config.NumberColumn("Max. Wert", format="%.0f €"),
                    "Pivot-Tag": st.column_config.DateColumn(
                        "Pivot-Tag", format="DD.MM.YYYY",
                        help="Klicke in die Zelle, um den Pivot-Tag direkt zu bearbeiten. Tag 1 des Ausbruchs. Fallback: Kauftag.",
                    ),
                },
                disabled=[
                    "Ticker", "Name", "Stück", "Einstand", "Aktuell", "Wert EUR", "P&L %",
                    "Stoppkurs", "Abstand Stop %", "ATR %", "Beta", "Risikobeitrag", "Max. Wert",
                ],
            )
            st.caption(
                "Tipp: Klicke in die Spalten „Stopp %“ oder „Pivot-Tag“, um Werte direkt zu bearbeiten — "
                "anschließend „Änderungen speichern“. Markiere die Checkboxen, um Positionen auszuwählen und gemeinsam zu löschen. "
                "Für einen Verkauf mit Erlös bitte „Verkauf buchen“ verwenden."
            )
            action_col_1, action_col_2, action_col_3 = st.columns(3)
            with action_col_1:
                if st.button("💾 Änderungen speichern", key="pf_overview_save_edits", width="stretch", type="primary"):
                    changes = 0
                    for _, row in edited_overview.iterrows():
                        ticker_raw = str(row.get("Ticker", "")).strip()
                        if not ticker_raw:
                            continue
                        existing = next(
                            (p for p in all_positions if str(p.get("ticker", "")).strip().upper() == ticker_raw.upper()),
                            None,
                        )
                        if not existing:
                            continue
                        update = {"ticker": existing["ticker"]}
                        changed = False
                        new_stop_pct_raw = row.get("Stopp %")
                        if new_stop_pct_raw is not None and pd.notna(new_stop_pct_raw):
                            new_stop_pct = float(new_stop_pct_raw)
                            # _safe_float gibt schon einen Default (0.0); zusätzliches
                            # ``or 0.0`` würde einen legitimen Stop-Pct von 0 als
                            # „unset" interpretieren und nie übernehmen lassen.
                            current_stop_pct = float(_safe_float(existing.get("stop_pct"), 0.0))
                            if abs(new_stop_pct - current_stop_pct) > 1e-6:
                                update["stop_pct"] = new_stop_pct
                                buy_price_existing = float(_safe_float(existing.get("buy_price"), 0.0))
                                if buy_price_existing > 0:
                                    update["stop_price"] = buy_price_existing * (1 - new_stop_pct / 100)
                                changed = True
                        new_pivot_raw = row.get("Pivot-Tag")
                        if new_pivot_raw is not None and pd.notna(new_pivot_raw):
                            new_pivot_ts = pd.to_datetime(new_pivot_raw, errors="coerce")
                            if pd.notna(new_pivot_ts):
                                new_pivot_str = new_pivot_ts.strftime("%Y-%m-%d")
                                existing_pivot_raw = existing.get("pivot_tag", "") or ""
                                existing_pivot_ts = pd.to_datetime(existing_pivot_raw, errors="coerce") if existing_pivot_raw else None
                                existing_pivot_str = existing_pivot_ts.strftime("%Y-%m-%d") if existing_pivot_ts is not None and pd.notna(existing_pivot_ts) else ""
                                if new_pivot_str != existing_pivot_str:
                                    update["pivot_tag"] = new_pivot_str
                                    changed = True
                        if changed:
                            _upsert_position(update)
                            changes += 1
                    if changes:
                        st.success(f"{changes} Position(en) aktualisiert.")
                        st.rerun()
                    else:
                        st.info("Keine Änderungen erkannt.")
            with action_col_2:
                if st.button("🗑️ Ausgewählte löschen", key="pf_overview_delete_selected", width="stretch"):
                    checked_rows = edited_overview[edited_overview["Auswählen"] == True] if not edited_overview.empty else edited_overview
                    selected_tickers = checked_rows["Ticker"].astype(str).tolist() if not checked_rows.empty else []
                    if not selected_tickers:
                        st.warning("Keine Positionen ausgewählt.")
                    else:
                        removed_count = _remove_positions_bulk(selected_tickers)
                        if removed_count > 0:
                            st.success(f"{removed_count} ausgewählte Position(en) gelöscht.")
                            st.rerun()
            with action_col_3:
                if st.button("🧹 Alles löschen", key="pf_overview_delete_all", width="stretch"):
                    removed_total = _remove_all_positions()
                    if removed_total > 0:
                        st.success(f"Gesamtes Depot gelöscht ({removed_total} Positionen).")
                        st.rerun()
                    st.info("Depot ist bereits leer.")

    st.markdown("---")
    with st.expander("➕ Manuelle Erfassung von Positionen", expanded=False):
        manual_tabs = st.tabs(["➕ Neue Position", "💸 Verkauf buchen", "📥 CSV-Import"])
        with manual_tabs[0]:
            st.markdown("##### Neue Position erfassen oder bestehende aktualisieren")
            pos_ticker = _render_ticker_picker("portfolio_depot", "Ticker oder Firmenname suchen", "NVDA oder Nvidia", show_quick=False)
            selected_pos = next((p for p in all_positions if p.get("ticker") == pos_ticker), None) if pos_ticker else None
            entry_default = float((selected_pos or {}).get("buy_price", 1.0) or 1.0)
            shares_default = float((selected_pos or {}).get("shares", 0.0) or 0.0)
            stop_pct_default = float((selected_pos or {}).get("stop_pct", 7.0) or 7.0)

            if selected_pos:
                st.info(f"Bestehende Position {pos_ticker} wird befüllt — Speichern überschreibt die Daten.")

            price_col, shares_col, stop_col = st.columns(3)
            with price_col:
                buy_price = st.number_input("Einstand", min_value=0.01, value=entry_default, step=0.01, key="pf_buy_price")
            with shares_col:
                shares = st.number_input("Stückzahl", min_value=0.0, value=shares_default, step=1.0, key="pf_shares",
                                         help="Tipp: Stückzahl im Tab „Rechner“ vorab kalkulieren.")
            with stop_col:
                stop_pct = st.number_input("Stoppabstand %", min_value=0.1, max_value=50.0, value=stop_pct_default, step=0.1, key="pf_stop_pct")
            date_col, pivot_col, curr_col, note_col = st.columns([1, 1, 0.8, 1.2])
            with date_col:
                try:
                    default_date = pd.Timestamp((selected_pos or {}).get("buy_date")).date() if (selected_pos or {}).get("buy_date") else datetime.now(timezone.utc).date()
                except Exception:
                    default_date = datetime.now(timezone.utc).date()
                buy_date = st.date_input("Kaufdatum", value=default_date, key="pf_buy_date")
            with pivot_col:
                try:
                    pivot_tag_default = pd.Timestamp((selected_pos or {}).get("pivot_tag")).date() if (selected_pos or {}).get("pivot_tag") else buy_date
                except Exception:
                    pivot_tag_default = buy_date
                pivot_tag = st.date_input(
                    "Pivot-Tag",
                    value=pivot_tag_default,
                    key="pf_pivot_tag",
                    help="Ausbruchstag (Tag 1). Wird im Live-Monitor als Referenz für Tag 1 und Tag 0 (Vortag) genutzt. Bleibt der Wert leer/identisch zum Kauftag, wird der Kauftag als Fallback verwendet.",
                )
            with curr_col:
                curr_default = (selected_pos or {}).get("currency", "EUR")
                currency = st.selectbox("Währung", ["EUR", "USD"], index=0 if curr_default != "USD" else 1, key="pf_currency")
            with note_col:
                note = st.text_input("Notiz", value=(selected_pos or {}).get("note", ""), key="pf_note")

            stop_price_preview = float(buy_price) * (1 - float(stop_pct) / 100)
            st.caption(f"Abgeleiteter Stoppkurs: {stop_price_preview:,.2f}")

            save_col, _spacer = st.columns([1, 3])
            with save_col:
                if st.button("💾 Speichern", width="stretch", key="pf_save_position", type="primary", disabled=not bool(pos_ticker)):
                    buy_price_eur, eur_rate = _price_to_eur(float(buy_price), currency, buy_date)
                    previous_shares = _safe_float((selected_pos or {}).get("shares"), 0.0) if selected_pos else 0.0
                    delta_shares = max(float(shares) - previous_shares, 0.0)
                    buy_delta_value = delta_shares * float(buy_price_eur)
                    _upsert_position({
                        "ticker": pos_ticker,
                        "buy_price": float(buy_price),
                        "buy_price_eur": float(buy_price_eur),
                        "buy_date": str(buy_date),
                        "pivot_tag": str(pivot_tag),
                        "currency": currency,
                        "shares": float(shares),
                        "stop_pct": float(stop_pct),
                        "stop_price": float(stop_price_preview),
                        "note": note,
                    })
                    if buy_delta_value > 0:
                        # Verfügbarkeit VOR der Cash-Korrektur prüfen — sonst zeigt
                        # die Warnung den bereits reduzierten Stand und feuert
                        # falsch (oder gar nicht).
                        available_cash_before = float(settings.get("cash_balance", 0.0))
                        new_cash = _adjust_cash_balance(-buy_delta_value)
                        if buy_delta_value > available_cash_before:
                            st.warning(f"Kaufvolumen ({buy_delta_value:,.2f} €) überstieg den verfügbaren Cashbestand. Cash wurde auf {new_cash:,.2f} € begrenzt.")
                    st.success(f"{pos_ticker} gespeichert.")
                    st.rerun()

        with manual_tabs[1]:
            st.markdown("##### Verkauf buchen (mit Cash-Gutschrift)")
            sell_options = [p.get("ticker", "") for p in tracked_positions if p.get("ticker")]
            sell_ticker = st.selectbox("Position verkaufen", options=[""] + sell_options, key="pf_sell_ticker")
            selected_sell = next((p for p in tracked_positions if p.get("ticker") == sell_ticker), None) if sell_ticker else None
            max_sell_shares = float(_safe_float((selected_sell or {}).get("shares"), 0.0))
            sell_c1, sell_c2 = st.columns(2)
            with sell_c1:
                sell_shares = st.number_input("Zu verkaufende Stück", min_value=0.0, max_value=max_sell_shares if max_sell_shares > 0 else 0.0, value=max_sell_shares if max_sell_shares > 0 else 0.0, step=1.0, key="pf_sell_shares")
            with sell_c2:
                sell_price = st.number_input("Verkaufspreis", min_value=0.0, value=float(_safe_float((selected_sell or {}).get("current_price"), 0.0)), step=0.01, key="pf_sell_price")
            sell_c3, sell_c4 = st.columns(2)
            with sell_c3:
                sell_currency = st.selectbox("Währung Verkauf", ["EUR", "USD"], index=0, key="pf_sell_currency")
            with sell_c4:
                sell_date = st.date_input("Verkaufsdatum", value=datetime.now(timezone.utc).date(), key="pf_sell_date")
            if st.button("Verkauf buchen", width="stretch", key="pf_sell_book", disabled=not bool(selected_sell) or sell_shares <= 0 or sell_price <= 0):
                if not selected_sell:
                    st.warning("Bitte zuerst eine Position wählen.")
                elif sell_shares > max_sell_shares:
                    st.error("Die Verkaufsmenge ist größer als die vorhandene Stückzahl.")
                else:
                    sell_price_eur, _ = _price_to_eur(float(sell_price), sell_currency, sell_date)
                    proceeds = float(sell_shares) * float(sell_price_eur)
                    sell_ticker_norm = _normalize_single_ticker(selected_sell.get("ticker", ""))
                    manual_sell_data = get_position_manual_sell_data(sell_ticker_norm) if sell_ticker_norm else {}
                    buy_price_native = _safe_optional_float(selected_sell.get("buy_price"))
                    sell_price_native = float(sell_price)
                    realized_pct = (sell_price_native / buy_price_native - 1) * 100 if buy_price_native and buy_price_native > 0 else None
                    append_closed_trade({
                        "source": "depot_sell_booking",
                        "booked_at": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                        "ticker": sell_ticker_norm,
                        "buy_date": selected_sell.get("buy_date"),
                        "buy_price": buy_price_native,
                        "buy_currency": selected_sell.get("currency"),
                        "sell_date": str(sell_date),
                        "sell_price": sell_price_native,
                        "sell_currency": sell_currency,
                        "shares": float(sell_shares),
                        "pivot": manual_sell_data.get("pivot"),
                        "planned_stop": selected_sell.get("stop_price"),
                        "realized_pnl_percent": realized_pct,
                        "notes": "Teilverkauf" if float(sell_shares) < max_sell_shares else "Vollverkauf",
                    })
                    remaining = max(max_sell_shares - float(sell_shares), 0.0)
                    if remaining <= 0:
                        _remove_position(selected_sell.get("ticker", ""))
                    else:
                        updated = dict(selected_sell)
                        updated["shares"] = remaining
                        _upsert_position(updated)
                    _adjust_cash_balance(proceeds)
                    st.success(f"Verkauf gebucht. Cash erhöht um {proceeds:,.2f} €. Der Trade wurde für das Post-Mortem vorgemerkt.")
                    st.rerun()

        with manual_tabs[2]:
            _render_transaction_importer()

    with st.expander("💵 Cash-Flows", expanded=False):
        st.caption("Ein- und Auszahlungen wirken zeitgewichtet auf den Depotindex.")
        flow_form_col, flow_log_col = st.columns([1.1, 1.2])
        with flow_form_col:
            flow_date = st.date_input("Cash-Flow Datum", value=datetime.now(timezone.utc).date(), key="pf_flow_date")
            flow_amount = st.number_input("Cash-Flow Betrag (EUR)", min_value=0.0, value=0.0, step=100.0, key="pf_flow_amount")
            flow_note = st.text_input("Cash-Flow Notiz", value="", key="pf_flow_note")
            flow_act1, flow_act2 = st.columns(2)
            with flow_act1:
                if st.button("Einzahlung buchen", width="stretch", key="pf_flow_deposit", disabled=flow_amount <= 0):
                    _append_cash_flow_entry(flow_date, "deposit", flow_amount, flow_note)
                    _adjust_cash_balance(float(flow_amount))
                    st.success("Einzahlung erfasst.")
                    st.rerun()
            with flow_act2:
                if st.button("Auszahlung buchen", width="stretch", key="pf_flow_withdrawal", disabled=flow_amount <= 0):
                    _append_cash_flow_entry(flow_date, "withdrawal", flow_amount, flow_note)
                    _adjust_cash_balance(-float(flow_amount))
                    st.success("Auszahlung erfasst.")
                    st.rerun()

        with flow_log_col:
            cash_flows = st.session_state.get("portfolio_cash_flows", []) if isinstance(st.session_state.get("portfolio_cash_flows", []), list) else []
            if cash_flows:
                flow_df = pd.DataFrame(cash_flows).copy()
                flow_df["__idx"] = flow_df.index
                flow_df["date"] = pd.to_datetime(flow_df["date"], errors="coerce")
                flow_df = flow_df.dropna(subset=["date"]).sort_values("date", ascending=False).reset_index(drop=True)
                flow_df["Typ"] = flow_df["type"].map({"deposit": "Einzahlung", "withdrawal": "Auszahlung"}).fillna("—")
                flow_df["Datum"] = flow_df["date"].dt.date
                flow_df["Betrag"] = pd.to_numeric(flow_df.get("amount", 0), errors="coerce").fillna(0.0)
                flow_df["Notiz"] = flow_df.get("note", "").astype(str)
                st.markdown("##### Letzte Bewegungen")
                st.dataframe(
                    flow_df[["Datum", "Typ", "Betrag", "Notiz"]].round(2),
                    width="stretch", hide_index=True, column_config=flow_column_config(),
                )
                delete_idx = st.selectbox(
                    "Cash-Flow löschen",
                    options=[""] + [f"{i}: {row['Datum']} · {row['Typ']} · {row['Betrag']:,.2f} €" for i, row in flow_df.iterrows()],
                    key="pf_flow_delete_sel",
                )
                if delete_idx and st.button("Cash-Flow löschen", width="stretch", key="pf_flow_delete_btn"):
                    idx = int(str(delete_idx).split(":", 1)[0])
                    row = flow_df.iloc[idx]
                    if str(row.get("Typ")) == "Einzahlung":
                        _adjust_cash_balance(-float(row.get("Betrag", 0.0)))
                    elif str(row.get("Typ")) == "Auszahlung":
                        _adjust_cash_balance(float(row.get("Betrag", 0.0)))
                    _remove_cash_flow_entry(int(row.get("__idx", -1)))
                    st.success("Cash-Flow gelöscht.")
                    st.rerun()
            else:
                st.info("Noch keine Cash-Flows erfasst.")


def _render_depot_curve_chart(curve: pd.DataFrame, *, key: str, include_flow_cols: bool) -> None:
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=curve["date"], y=curve["portfolio_index"], mode="lines", name="Depotindex"))
    fig.add_trace(go.Scatter(x=curve["date"], y=curve["portfolio_index_sma10"], mode="lines", name="10-Tage SMA", line=dict(width=1.6, dash="dot")))
    fig.add_trace(go.Scatter(x=curve["date"], y=curve["portfolio_index_sma21"], mode="lines", name="21-Tage SMA", line=dict(width=1.6, dash="dash")))
    if "sp500_index" in curve and curve["sp500_index"].notna().any():
        fig.add_trace(go.Scatter(x=curve["date"], y=curve["sp500_index"], mode="lines", name="S&P 500 Index"))
    apply_consistent_layout(fig, height=380, top_margin=20)
    fig.update_layout(
        margin=dict(l=10, r=10, t=20, b=10),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
        yaxis=dict(title="Index (Start = 100)", gridcolor=CHART_COLORS["grid"]),
        xaxis=dict(title="", gridcolor=CHART_COLORS["grid"]),
    )
    st.plotly_chart(fig, width="stretch", key=key)

    if include_flow_cols:
        cols = ["date", "depot_value", "deposit", "withdrawal", "portfolio_index", "portfolio_index_sma10", "portfolio_index_sma21", "sp500_index"]
        rename_map = {
            "date": "Datum", "depot_value": "Depotwert", "deposit": "Einzahlung", "withdrawal": "Auszahlung",
            "portfolio_index": "Depotindex", "portfolio_index_sma10": "SMA 10", "portfolio_index_sma21": "SMA 21", "sp500_index": "S&P 500",
        }
        column_config = {
            "Datum": st.column_config.TextColumn("Datum", width="small"),
            "Depotwert": st.column_config.NumberColumn("Depotwert", format="%.2f"),
            "Einzahlung": st.column_config.NumberColumn("Einzahlung", format="%.2f €"),
            "Auszahlung": st.column_config.NumberColumn("Auszahlung", format="%.2f €"),
            "Depotindex": st.column_config.NumberColumn("Depotindex", format="%.2f"),
        }
    else:
        cols = ["date", "depot_value", "positions_value", "cash", "external_flow", "portfolio_index", "portfolio_index_sma10", "portfolio_index_sma21", "sp500_index"]
        rename_map = {
            "date": "Datum", "depot_value": "Depotwert", "positions_value": "Positionen", "cash": "Cash",
            "external_flow": "Ein-/Auszahlung",
            "portfolio_index": "Depotindex", "portfolio_index_sma10": "SMA 10", "portfolio_index_sma21": "SMA 21", "sp500_index": "S&P 500",
        }
        column_config = {
            "Datum": st.column_config.TextColumn("Datum", width="small"),
            "Depotwert": st.column_config.NumberColumn("Depotwert", format="%.2f"),
            "Positionen": st.column_config.NumberColumn("Positionen", format="%.2f"),
            "Cash": st.column_config.NumberColumn("Cash", format="%.2f €"),
            "Ein-/Auszahlung": st.column_config.NumberColumn("Ein-/Auszahlung", format="%.2f €", help="TWR-neutralisierte externe Cash-Bewegung an diesem Tag (Überweisung/Steueroptimierung)."),
            "Depotindex": st.column_config.NumberColumn("Depotindex", format="%.2f"),
        }
    available = [c for c in cols if c in curve.columns]
    display = curve[available].copy()
    display["date"] = pd.to_datetime(display["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    display = display.rename(columns=rename_map).round(2)
    st.dataframe(display, width="stretch", hide_index=True, column_config=column_config)


@st.cache_data(ttl=3600, show_spinner=False, max_entries=4)
def _parse_tr_kontoauszug_pdf_cached(pdf_bytes: bytes) -> pd.DataFrame:
    return _parse_tr_kontoauszug_pdf(pdf_bytes)


def _render_csv_curve_section(settings: dict) -> bool:
    """Depotkurve aus dem Trade-Republic-Transaktionsexport (CSV).

    Stückzahlen, Trade-Preise und Cash-Bewegungen kommen alle aus der CSV
    (``amount + fee + tax`` rekonstruiert den Cash-Saldo aufs Cent zum offiziellen
    Kontoauszug). Yahoo Finance liefert die täglichen Schlusskurse.

    Returns True if a curve was rendered (so the fallback can skip).
    """
    st.markdown("##### 📥 Trade-Republic-Transaktionsexport (CSV)")
    st.caption(
        "Lade den TR-Transaktionsexport hoch — er enthält Stückzahlen, Trade-Preise und "
        "alle Cash-Bewegungen (Einzahlungen, Auszahlungen, Steueroptimierung, Dividenden, "
        "Zinsen, Gebühren). Daraus wird Tag für Tag der Depotwert rekonstruiert "
        "(Yahoo-Close × Stückzahl + Cash aus CSV)."
    )

    saved_import = _normalize_depot_curve_csv_import_state(st.session_state.get(DEPOT_CURVE_CSV_IMPORT_KEY, {}))
    existing_tx_df = _depot_curve_csv_records_to_frame(saved_import.get("records", []))

    uploaded_csv = st.file_uploader(
        "Trade-Republic-Transaktionsexport importieren / aktualisieren",
        type=["csv"], key="pf_depot_curve_csv_upload",
        help=(
            "Der Import wird dauerhaft für die Kurve gespeichert. Beim Aktualisieren werden "
            "neue oder geänderte Buchungen übernommen; ältere gespeicherte Buchungen werden "
            "nicht still gelöscht."
        ),
    )
    if uploaded_csv is not None:
        try:
            parsed_upload = _parse_transaction_export_csv(uploaded_csv)
            preview_df, preview_summary = _merge_depot_curve_csv_import(existing_tx_df, parsed_upload)
            st.caption(
                "CSV geprüft: "
                f"{preview_summary['added']} neu · {preview_summary['updated']} geändert · "
                f"{preview_summary['unchanged']} unverändert · "
                f"{preview_summary['kept_missing']} gespeicherte ältere Buchungen bleiben erhalten."
            )
            if st.button("CSV-Import speichern / Kurve aktualisieren", key="pf_depot_curve_csv_apply", type="primary", width="stretch"):
                next_state = _build_depot_curve_csv_import_state(
                    preview_df,
                    filename=uploaded_csv.name,
                    previous=saved_import,
                    summary=preview_summary,
                )
                _set_depot_curve_csv_import_state(next_state)
                st.success("CSV-Import gespeichert. Die Depotkurve wurde aktualisiert.")
                st.rerun()
        except ValueError as exc:
            st.error(str(exc))

    saved_import = _normalize_depot_curve_csv_import_state(st.session_state.get(DEPOT_CURVE_CSV_IMPORT_KEY, {}))
    tx_df = _depot_curve_csv_records_to_frame(saved_import.get("records", []))
    if not isinstance(tx_df, pd.DataFrame) or tx_df.empty:
        st.info("Noch keine gespeicherte Trade-Republic-CSV vorhanden — die Depotkurve unten fällt auf die manuell gepflegten Positionen zurück.")
        return False

    # Cash-Endsaldo aus CSV — gleicher Wert wie auf dem offiziellen TR-Kontoauszug.
    tx_amount = pd.to_numeric(tx_df.get("amount_num", tx_df.get("amount", 0)), errors="coerce").fillna(0.0)
    tx_fee = pd.to_numeric(tx_df.get("fee_num", tx_df.get("fee", 0)), errors="coerce").fillna(0.0)
    tx_tax = pd.to_numeric(tx_df.get("tax_num", tx_df.get("tax", 0)), errors="coerce").fillna(0.0)
    csv_end_cash = float((tx_amount + tx_fee + tx_tax).sum())
    csv_first_date = pd.to_datetime(tx_df["date"], errors="coerce").min()
    csv_last_date = pd.to_datetime(tx_df["date"], errors="coerce").max()

    filename = saved_import.get("filename", "")
    info_cols = st.columns([3, 1])
    with info_cols[0]:
        st.caption(
            f"Aktive Datei: **{filename or 'Transaktionen'}** · {len(tx_df):,} Buchungen · "
            f"{csv_first_date:%Y-%m-%d} → {csv_last_date:%Y-%m-%d} · "
            f"Cash-Endsaldo (rekonstruiert): **{_format_eur(csv_end_cash)}**"
        )
        if saved_import.get("updated_at"):
            last_summary = _normalize_depot_curve_import_summary(saved_import.get("last_import_summary", {}))
            st.caption(
                f"Gespeichert seit {saved_import.get('created_at') or 'unbekannt'} · "
                f"zuletzt aktualisiert {saved_import.get('updated_at')} · "
                f"letzter Import: +{last_summary['added']} / geändert {last_summary['updated']} / "
                f"unverändert {last_summary['unchanged']}."
            )
    with info_cols[1]:
        if st.button("Kurve löschen", key="pf_depot_curve_csv_reset", width="stretch"):
            _clear_depot_curve_csv_import_state()
            st.rerun()

    # Date controls.
    today = datetime.now(timezone.utc).date()
    tx_first = csv_first_date.date() if pd.notna(csv_first_date) else today
    tx_last = csv_last_date.date() if pd.notna(csv_last_date) else today

    saved_start = None
    try:
        raw_start = str(settings.get("curve_start_date", "") or "").strip()
        if raw_start:
            saved_start = pd.Timestamp(raw_start).date()
    except Exception:
        saved_start = None
    start_default = saved_start or tx_first
    # Clamp the default into the valid range.
    start_default = max(tx_first, min(start_default, today))

    ctrl_start, ctrl_end = st.columns([1, 1])
    with ctrl_start:
        start_date = st.date_input(
            "Startdatum (Index = 100)",
            value=start_default,
            min_value=tx_first,
            max_value=today,
            key="pf_depot_curve_start_date",
            help="Tag, ab dem die Equity-Kurve auf 100 normiert wird. Stückzahlen und Cash "
                 "bis zu diesem Datum werden korrekt aus den vorhergehenden Buchungen kumuliert.",
        )
    with ctrl_end:
        end_date = st.date_input(
            "Enddatum", value=today,
            min_value=start_date, max_value=today,
            key="pf_depot_curve_end_date",
            help="Letzter Tag der Kurve. Standard: heute.",
        )

    overrides = saved_import.get("isin_overrides") or st.session_state.get("pf_depot_curve_isin_overrides", {}) or {}
    ticker_map, diagnostics = _resolve_isin_ticker_map(tx_df, overrides=overrides)

    with st.expander("Ticker-Zuordnung prüfen / anpassen", expanded=False):
        if not diagnostics:
            st.info("Keine STOCK/FUND-Positionen in der CSV gefunden.")
        else:
            edit_rows = []
            for row in diagnostics:
                edit_rows.append({
                    "ISIN": row["isin"],
                    "Name": row["name"],
                    "Anlageklasse": row["asset_class"],
                    "Yahoo-Ticker": row["ticker"],
                })
            edit_df = pd.DataFrame(edit_rows)
            edited = st.data_editor(
                edit_df, width="stretch", hide_index=True, key="pf_depot_curve_isin_editor",
                column_config={
                    "ISIN": st.column_config.TextColumn("ISIN", disabled=True),
                    "Name": st.column_config.TextColumn("Name", disabled=True),
                    "Anlageklasse": st.column_config.TextColumn("Anlageklasse", disabled=True),
                    "Yahoo-Ticker": st.column_config.TextColumn("Yahoo-Ticker", help="Leer lassen, um diese ISIN aus der Kurve auszuschließen."),
                },
            )
            if st.button("Ticker-Zuordnung übernehmen", key="pf_depot_curve_isin_apply"):
                new_overrides = {}
                for _, row in edited.iterrows():
                    isin = str(row.get("ISIN", "")).upper().strip()
                    ticker = _normalize_single_ticker(str(row.get("Yahoo-Ticker", "")))
                    if isin and ticker:
                        new_overrides[isin] = ticker
                next_state = _build_depot_curve_csv_import_state(
                    tx_df,
                    filename=filename,
                    previous=saved_import,
                    isin_overrides=new_overrides,
                )
                _set_depot_curve_csv_import_state(next_state)
                st.success("Ticker-Zuordnung gespeichert. Kurve wird neu berechnet.")
                st.rerun()

    with st.spinner("Berechne Depotkurve aus Transaktionen …"):
        curve = _build_curve_from_transactions(
            tx_df,
            isin_to_ticker=ticker_map,
            start_date=start_date,
            end_date=end_date,
        )
    if curve.empty:
        st.warning(
            "Aus der CSV konnte keine Kurve gebildet werden. Prüfe die Ticker-Zuordnung und "
            "stelle sicher, dass Yahoo Finance Kursdaten für deine Positionen liefert."
        )
        return True

    unresolved = curve.attrs.get("unresolved_isins") or []
    if unresolved:
        st.warning(
            "Diese ISINs konnten nicht aufgelöst werden und sind aus der Kurve ausgenommen: "
            + ", ".join(sorted(unresolved))
        )
    fallback_isins = curve.attrs.get("price_fallback_isins") or []
    if fallback_isins:
        open_fallback_isins = curve.attrs.get("open_price_fallback_isins") or []
        historical_count = max(len(fallback_isins) - len(open_fallback_isins), 0)
        if open_fallback_isins:
            st.info(
                "Für diese offenen ISINs hat Yahoo keine Historie geliefert; die Kurve nutzt "
                "ersatzweise die Trade-Preise aus der CSV: "
                + ", ".join(sorted(open_fallback_isins))
            )
            if historical_count:
                st.caption(
                    f"Zusätzlich wurden {historical_count} geschlossene/historische ISINs "
                    "über Trade-Preise bewertet."
                )
        else:
            st.caption(
                f"Für {len(fallback_isins)} geschlossene/historische ISINs wurden bei fehlender "
                "Yahoo-Historie Trade-Preise aus der CSV verwendet."
            )

    _render_depot_curve_chart(curve, key="pf_curve_chart_csv", include_flow_cols=False)
    return True


def _render_depot_curve_only(state: dict | None = None) -> None:
    state = state or _depot_state()
    settings = state["settings"]
    all_positions = state["positions"]
    st.markdown("#### 📈 Depotkurve")
    st.caption(
        "Zeitgewichtete Equity-Kurve (TWR) gegen den S&P 500 mit 10- und 21-Tage-SMA. "
        "Datenquelle: Trade-Republic-Transaktionsexport (CSV) — Stückzahlen, Trade-Cash, "
        "Dividenden, Zinsen, Gebühren. Yahoo Finance liefert die täglichen Schlusskurse. "
        "Externe Ein-/Auszahlungen und Steueroptimierungen werden aus dem Index "
        "neutralisiert, damit die Kurve nur die Investment-Performance zeigt."
    )

    rendered_csv = _render_csv_curve_section(settings)

    st.markdown("---")
    st.markdown("##### 🧮 Fallback: Kurve aus gepflegten Positionen")
    st.caption(
        "Wenn keine TR-CSV vorliegt, kann die Kurve aus den manuell gepflegten Depot-Positionen "
        "rekonstruiert werden. Sie startet beim Klick auf „Kurve starten“ und nutzt den im "
        "Depot-Workspace hinterlegten Cash-Saldo plus erfasste Cash-Flows."
    )
    saved_curve_start = None
    try:
        raw_curve_start = str(settings.get("curve_start_date", "") or "").strip()
        if raw_curve_start:
            saved_curve_start = pd.Timestamp(raw_curve_start).date()
    except Exception:
        saved_curve_start = None

    if st.session_state.pop("pf_auto_curve_start_force_today", False):
        today = datetime.now(timezone.utc).date()
        current_settings = _get_portfolio_settings()
        current_settings["curve_start_date"] = str(today)
        st.session_state["portfolio_settings"] = current_settings
        _sync_workspace()
        st.rerun()

    auto_end = datetime.now(timezone.utc).date()
    auto_col1, auto_col2 = st.columns([1, 1.2])
    with auto_col1:
        if saved_curve_start is None:
            if st.button("Kurve starten", width="stretch", key="pf_auto_curve_start_today"):
                st.session_state["pf_auto_curve_start_force_today"] = True
                st.rerun()
        else:
            st.success(f"Kurve aktiv seit {saved_curve_start.strftime('%Y-%m-%d')}")
    with auto_col2:
        if saved_curve_start is not None:
            if st.button("Kurve neu starten (Heute)", width="stretch", key="pf_auto_curve_restart_today"):
                st.session_state["pf_auto_curve_start_force_today"] = True
                st.rerun()

    auto_start = saved_curve_start
    cash_flows = st.session_state.get("portfolio_cash_flows", []) if isinstance(st.session_state.get("portfolio_cash_flows", []), list) else []
    if auto_start is None:
        if not rendered_csv:
            st.info("Depotkurve ist noch nicht gestartet. Klicke auf „Kurve starten“, um den ersten Starttag festzulegen.")
        return
    with st.spinner("Berechne Depotkurve aus Positionen …"):
        auto_curve = _build_reconstructed_portfolio_curve(
            all_positions, float(settings.get("cash_balance", 0.0)), auto_start, auto_end, cash_flows=cash_flows,
        )
    if auto_curve.empty:
        st.info("Für die Depotkurve fehlen aktuell verwertbare Kursdaten oder Positionen mit Stückzahl.")
        return

    _render_depot_curve_chart(auto_curve, key="pf_curve_chart_auto", include_flow_cols=True)


def _render_stueckzahl_rechner_page() -> None:
    """Stückzahl- und Positionsgrößen-Rechner — unabhängiges Pre-Trade-Werkzeug."""
    st.markdown("#### 🧮 Stückzahl- und Positionsgrößen-Rechner")
    st.caption(
        "Eigenständiges Werkzeug nach Kapitel 7.2. Vorgaben aus dem Portfolio-Regler werden "
        "vorbelegt, lassen sich aber für Was-wäre-wenn-Szenarien überschreiben — gespeichert wird hier nichts."
    )

    _init_workspace_state()
    settings = _get_portfolio_settings()

    if _is_private_unlocked():
        positions = st.session_state.get("positions", [])
        _, summary = _build_portfolio_snapshot(
            positions,
            cash_balance=_safe_float(settings.get("cash_balance"), 0.0),
            target_risk_contribution=_safe_float(settings.get("target_risk_contribution"), 0.20),
        )
        depot_default = _safe_float(summary.get("total_value"), 0.0)
        spx_atr_pct = _safe_float(summary.get("spx_atr_pct"), np.nan)
    else:
        depot_default = float(settings.get("cash_balance", 0.0) or 0.0)
        spx_atr_pct = _sp500_atr_reference()

    with st.expander("📐 Berechnungsgrundlagen anzeigen", expanded=False):
        st.markdown(
            """
**Verlust-Budget-Logik (Hauptberechnung):**

```
Risikobudget je Idee     = Depotwert × (Max. Verlust je Idee % / 100)
Risiko pro Aktie         = Einstand   × (Stoppabstand %        / 100)
Maximale Stückzahl       = floor(Risikobudget / Risiko pro Aktie)
Maximaler Positionswert  = Maximale Stückzahl × Einstand
Abgeleiteter Stoppkurs   = Einstand × (1 − Stoppabstand % / 100)
```

Der Rechner ermittelt also, wie viele Aktien du **maximal** kaufen kannst, ohne im
schlimmsten Fall (Stopp ausgelöst) mehr als das Risikobudget zu verlieren.

**Beta-Balancer-Sicht (Marktrisiko-Begrenzung):**

```
Balancer-Score    = 0,60 × Beta_Aktie + 0,40 × (ATR%_Aktie / ATR%_S&P 500)
Max. Gewicht      = Ziel Risikobeitrag / Balancer-Score
Max. Wert via BB  = Depotwert × Max. Gewicht
Max. Stück via BB = floor(Max. Wert / aktueller Kurs)
```

Der **Ziel Risikobeitrag** ist die obere Schranke für `Gewicht × Balancer-Score`
pro Idee. Eine hoch-Beta- bzw. hoch-ATR-Aktie bekommt automatisch ein
kleineres Maximalgewicht. **Praxis:** Vergleiche beide Resultate
(Verlust-Budget vs. Beta-Balancer) — der **kleinere Wert gewinnt**.
            """
        )

    cfg1, cfg2, cfg3 = st.columns(3)
    with cfg1:
        depot_value = st.number_input(
            "Depotwert (modelliert) — EUR", min_value=0.0,
            value=float(depot_default), step=500.0, key="rechner_depot_value",
            help="Vorbelegt aus dem gespeicherten Depot (inkl. Cash). Frei überschreibbar.",
        )
    with cfg2:
        risk_pct = st.number_input(
            "Max. Verlust je Idee %", min_value=0.1, max_value=5.0,
            value=float(settings.get("risk_per_position_pct", 1.0)),
            step=0.1, key="rechner_risk_pct",
            help="Aus dem Portfolio-Regler vorbelegt. Buch-Empfehlung: 1 %.",
        )
    with cfg3:
        target_rc = st.number_input(
            "Ziel Risikobeitrag (Balancer)", min_value=0.05, max_value=0.50,
            value=float(settings.get("target_risk_contribution", 0.20)),
            step=0.01, key="rechner_target_rc",
            help="Nur für die Beta-Balancer-Sicht relevant.",
        )

    st.markdown("##### Idee")
    p1, p2, p3 = st.columns([1.2, 1.0, 1.0])
    with p1:
        ticker_input = _render_ticker_picker(
            "rechner", "Ticker oder Firmenname (optional)",
            "z. B. NVDA — für Beta-Balancer-Sicht",
            show_quick=False,
        )
    with p2:
        buy_price = st.number_input("Einstand (EUR)", min_value=0.01, value=1.00, step=0.01, key="rechner_buy_price")
    with p3:
        stop_pct = st.number_input("Stoppabstand %", min_value=0.1, max_value=50.0, value=7.0, step=0.5, key="rechner_stop_pct")

    stop_price = float(buy_price) * (1 - float(stop_pct) / 100)
    risk_budget = float(depot_value) * (float(risk_pct) / 100) if depot_value > 0 else np.nan
    risk_per_share = float(buy_price) * (float(stop_pct) / 100) if buy_price > 0 else np.nan
    max_shares = int(np.floor(risk_budget / risk_per_share)) if not np.isnan(risk_budget) and not np.isnan(risk_per_share) and risk_per_share > 0 else 0
    max_position_value = max_shares * float(buy_price) if max_shares > 0 else np.nan

    st.markdown("##### Ergebnis · Verlust-Budget")
    m1, m2, m3, m4, m5 = st.columns(5)
    with m1:
        st.metric("Maximale Stückzahl", f"{max_shares:,}" if max_shares else "—",
                  help="floor(Risikobudget / Risiko pro Aktie)")
    with m2:
        st.metric("Risikobudget / Idee", f"{risk_budget:,.0f} €" if not np.isnan(risk_budget) else "—",
                  help="Depotwert × (Max. Verlust je Idee % / 100)")
    with m3:
        st.metric("Risiko pro Aktie", f"{risk_per_share:,.2f} €" if not np.isnan(risk_per_share) else "—",
                  help="Einstand × (Stoppabstand % / 100)")
    with m4:
        st.metric("Maximaler Positionswert", f"{max_position_value:,.0f} €" if not np.isnan(max_position_value) else "—",
                  help="Maximale Stückzahl × Einstand")
    with m5:
        st.metric("Abgeleiteter Stoppkurs", f"{stop_price:,.2f} €",
                  help="Einstand × (1 − Stoppabstand % / 100)")

    if ticker_input:
        metrics = _portfolio_symbol_metrics(ticker_input)
        atr_pct = _safe_float(metrics.get("atr_pct"), np.nan)
        beta = _safe_float(metrics.get("beta"), np.nan)
        score = np.nan
        if not np.isnan(beta) and not np.isnan(atr_pct) and not np.isnan(spx_atr_pct) and spx_atr_pct > 0:
            score = 0.60 * beta + 0.40 * (atr_pct / spx_atr_pct)
        raw_market_price = _safe_float(metrics.get("price"), np.nan)
        market_currency = str(metrics.get("currency", "USD") or "USD").upper()
        market_price_eur = raw_market_price if market_currency == "EUR" else _usd_to_eur(raw_market_price)
        balancer_weight = (float(target_rc) / score) if not np.isnan(score) and score > 0 else np.nan
        balancer_value = float(depot_value) * balancer_weight if depot_value > 0 and not np.isnan(balancer_weight) else np.nan
        balancer_shares = int(np.floor(balancer_value / market_price_eur)) if not np.isnan(balancer_value) and market_price_eur > 0 else 0

        st.markdown("##### Ergebnis · Beta-Balancer")
        spx_label = f"S&P 500 ATR%: {spx_atr_pct:.2f}%" if not np.isnan(spx_atr_pct) else "S&P 500 ATR% n/a"
        st.caption(f"Markt-Referenz: {spx_label}")
        b1, b2, b3, b4, b5 = st.columns(5)
        with b1:
            st.metric("ATR % der Aktie", f"{atr_pct:.2f}%" if not np.isnan(atr_pct) else "—")
        with b2:
            st.metric("Beta", f"{beta:.2f}" if not np.isnan(beta) else "—")
        with b3:
            st.metric("Balancer-Score", f"{score:.2f}" if not np.isnan(score) else "—",
                      help="0,60 × Beta + 0,40 × (ATR%-Aktie / ATR%-S&P 500)")
        with b4:
            st.metric("Max. Gewicht", f"{balancer_weight*100:.1f}%" if not np.isnan(balancer_weight) else "—",
                      help="Ziel Risikobeitrag / Balancer-Score")
        with b5:
            st.metric("Max. Stück (BB)", f"{balancer_shares:,}" if balancer_shares else "—",
                      help="floor(Depotwert × Max. Gewicht / aktueller Kurs)")

        if balancer_shares and max_shares:
            smaller = min(int(balancer_shares), int(max_shares))
            source = "Verlust-Budget" if smaller == max_shares else "Beta-Balancer"
            st.info(f"**Empfehlung:** maximal **{smaller:,} Stück** (begrenzt durch {source}).")

    st.markdown(
        """
        <div style="background: #f5f4ef; border-radius: 8px; padding: 12px; margin-top: 16px; font-size: 13px;">
            ℹ️ Kapitel 7.2 empfiehlt, pro Idee meist nicht mehr als 1 % des Depots zu riskieren.
            Erfasste Käufe trägst du anschließend im Tab „Positionen" ein.
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_workspace_sidebar():
    with st.sidebar:
        st.markdown("### Watchlist")
        st.caption(f"Speicher: {_workspace_backend_label()} · Bereich: {_workspace_scope()}")
        if _private_area_enabled():
            state_label = "✓ entsperrt" if _is_private_unlocked() else "🔒 gesperrt"
            st.caption(f"Privater Bereich: {state_label}")
            if _is_private_unlocked():
                if st.button("🔒 Sperren", width="stretch", key="sidebar_lock_private"):
                    _lock_private_area()
                    st.rerun()
        if not _is_private_unlocked():
            st.markdown('<div class="workspace-note">Watchlist und Depot sind gesperrt.</div>', unsafe_allow_html=True)
            st.caption("→ Watchlist öffnen zum Entsperren")
            return
        _init_workspace_state()
        watchlist = st.session_state.get("watchlist", [])
        st.markdown("**Watchlist**")
        if watchlist:
            sidebar_names = _ticker_display_names(tuple(watchlist[:8]))
            pill_html = []
            for ticker in watchlist[:8]:
                name = sidebar_names.get(ticker, ticker)
                label = ticker if name == ticker else f"{ticker} · {name}"
                pill_html.append(f'<span class="pill">{html.escape(label)}</span>')
            st.markdown('<div class="pill-wrap">' + "".join(pill_html) + '</div>', unsafe_allow_html=True)
        else:
            st.markdown('<div class="workspace-note">Noch keine Ticker in der Watchlist.</div>', unsafe_allow_html=True)
        positions = st.session_state.get("positions", [])
        st.caption(f"{len(positions)} Positionen · {len(st.session_state.get('recent_tickers', []))} zuletzt genutzt")
        st.divider()
        st.caption("Seiten: Watchlist ⭐ · Einstellungen ⚙️")

# ===== From market_data.py =====
logger = logging.getLogger(__name__)

def get_sp500_tickers():
    """Try to load current S&P 500 tickers from Wikipedia, fall back to hardcoded list."""
    try:
        tables = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
        symbols = tables[0]["Symbol"].tolist()
        return [s.replace(".", "-") for s in symbols]
    except Exception:
        pass
    # Hardcoded fallback (503 tickers, as of early 2025)
    return [
        'AAPL','ABBV','ABT','ACN','ADBE','ADI','ADM','ADP','ADSK','AEE','AEP','AES','AFL','AIG','AIZ',
        'AJG','AKAM','ALB','ALGN','ALK','ALL','ALLE','AMAT','AMCR','AMD','AME','AMGN','AMP','AMT','AMZN',
        'ANET','ANSS','AON','AOS','APA','APD','APH','APTV','ARE','ATO','ATVI','AVB','AVGO','AVY','AWK',
        'AXP','AZO','BA','BAC','BAX','BBWI','BBY','BDX','BEN','BF-B','BIO','BK','BKNG','BKR','BLK',
        'BMY','BR','BRK-B','BRO','BSX','BWA','BX','BXP','C','CAG','CAH','CARR','CAT','CB','CBOE',
        'CBRE','CCI','CCL','CDNS','CDW','CE','CEG','CF','CFG','CHD','CHRW','CHTR','CI','CINF','CL',
        'CLX','CMA','CMCSA','CME','CMG','CMI','CMS','CNC','CNP','COF','COO','COP','COST','CPB','CPRT',
        'CPT','CRL','CRM','CSCO','CSGP','CSX','CTAS','CTLT','CTRA','CTSH','CTVA','CVS','CVX','CZR',
        'D','DAL','DD','DE','DFS','DG','DGX','DHI','DHR','DIS','DISH','DLR','DLTR','DOV','DOW',
        'DPZ','DRI','DTE','DUK','DVA','DVN','DXC','DXCM','EA','EBAY','ECL','ED','EFX','EIX','EL',
        'EMN','EMR','ENPH','EOG','EPAM','EQIX','EQR','EQT','ES','ESS','ETN','ETR','ETSY','EVRG','EW',
        'EXC','EXPD','EXPE','EXR','F','FANG','FAST','FBHS','FCX','FDS','FDX','FE','FFIV','FI','FICO',
        'FIS','FISV','FITB','FLT','FMC','FOX','FOXA','FRC','FRT','FTNT','FTV','GD','GE','GEHC','GEN',
        'GILD','GIS','GL','GLW','GM','GNRC','GOOG','GOOGL','GPC','GPN','GRMN','GS','GWW','HAL','HAS',
        'HBAN','HCA','HD','HOLX','HON','HPE','HPQ','HRL','HSIC','HST','HSY','HUM','HWM','IBM','ICE',
        'IDXX','IEX','IFF','ILMN','INCY','INTC','INTU','INVH','IP','IPG','IQV','IR','IRM','ISRG','IT',
        'ITW','IVZ','J','JBHT','JCI','JKHY','JNJ','JNPR','JPM','K','KDP','KEY','KEYS','KHC','KIM',
        'KLAC','KMB','KMI','KMX','KO','KR','L','LDOS','LEN','LH','LHX','LIN','LKQ','LLY','LMT',
        'LNC','LNT','LOW','LRCX','LUMN','LUV','LVS','LW','LYB','LYV','MA','MAA','MAR','MAS','MCD',
        'MCHP','MCK','MCO','MDLZ','MDT','MET','META','MGM','MHK','MKC','MKTX','MLM','MMC','MMM','MNST',
        'MO','MOH','MOS','MPC','MPWR','MRK','MRNA','MRO','MS','MSCI','MSFT','MSI','MTB','MTCH','MTD',
        'MU','NCLH','NDAQ','NDSN','NEE','NEM','NFLX','NI','NKE','NOC','NOW','NRG','NSC','NTAP','NTRS',
        'NUE','NVDA','NVR','NWL','NWS','NWSA','NXPI','O','ODFL','OGN','OKE','OMC','ON','ORCL','ORLY',
        'OTIS','OXY','PARA','PAYC','PAYX','PCAR','PCG','PEAK','PEG','PEP','PFE','PFG','PG','PGR','PH',
        'PHM','PKG','PKI','PLD','PM','PNC','PNR','PNW','PODD','POOL','PPG','PPL','PRU','PSA','PSX',
        'PTC','PVH','PWR','PXD','PYPL','QCOM','QRVO','RCL','RE','REG','REGN','RF','RHI','RJF','RL',
        'RMD','ROK','ROL','ROP','ROST','RSG','RTX','RVTY','SBAC','SBNY','SBUX','SCHW','SEE','SHW',
        'SIVB','SJM','SLB','SNA','SNPS','SO','SPG','SPGI','SRE','STE','STT','STX','STZ','SWK','SWKS',
        'SYF','SYK','SYY','T','TAP','TDG','TDY','TECH','TEL','TER','TFC','TFX','TGT','TJX','TMO',
        'TMUS','TPR','TRGP','TRMB','TROW','TRV','TSCO','TSLA','TSN','TT','TTWO','TXN','TXT','TYL',
        'UAL','UDR','UHS','ULTA','UNH','UNP','UPS','URI','USB','V','VFC','VICI','VLO','VMC','VRSK',
        'VRSN','VRTX','VTR','VTRS','VZ','WAB','WAT','WBA','WBD','WDC','WEC','WELL','WFC','WHR','WM',
        'WMB','WMT','WRB','WRK','WST','WTW','WY','WYNN','XEL','XOM','XRAY','XYL','YUM','ZBH','ZBRA',
        'ZION','ZTS',
    ]

def _normalize_ticker_list(values):
    tickers = []
    for raw in values:
        if pd.isna(raw):
            continue
        t = str(raw).strip().upper()
        t = t.replace(".", "-").replace("/", "-").replace(" ", "")
        if not t or t in {"USD", "CASH", "N/A", "-"}:
            continue
        if re.fullmatch(r"[A-Z0-9\-]+", t):
            tickers.append(t)
    return list(dict.fromkeys(tickers))

def _extract_ticker_column(df):
    preferred = (
        "ticker", "issuer ticker", "symbol", "symbols",
        "nasdaq symbol", "cqs symbol", "act symbol",
        "trading symbol", "stock symbol"
    )
    lowered = [(c, str(c).strip().lower()) for c in df.columns]
    for want in preferred:
        for c, name in lowered:
            if name == want:
                return c
    for c, name in lowered:
        compact = re.sub(r"[^a-z]", "", name)
        if compact in {"ticker", "issuerticker", "symbol", "nasdaqsymbol", "cqssymbol", "actsymbol", "tradingsymbol", "stocksymbol"}:
            return c
    for c, name in lowered:
        if "ticker" in name or "symbol" in name:
            return c
    return None

def _parse_ishares_holdings_csv(text):
    lines = text.splitlines()
    header_idx = next((i for i, line in enumerate(lines[:150]) if "Ticker" in line and "Name" in line), None)
    if header_idx is None:
        return []
    try:
        holdings = pd.read_csv(io.StringIO("\n".join(lines[header_idx:])))
    except Exception:
        return []
    ticker_col = _extract_ticker_column(holdings)
    if ticker_col is None:
        return []
    return _normalize_ticker_list(holdings[ticker_col])

def _parse_ishares_holdings_html(html):
    try:
        tables = pd.read_html(io.StringIO(html))
    except Exception:
        return []
    collected = []
    for table in tables:
        ticker_col = _extract_ticker_column(table)
        if ticker_col is not None:
            collected.extend(table[ticker_col].tolist())
    return _normalize_ticker_list(collected)

def _parse_nasdaq_otherlisted_text(text):
    try:
        df = pd.read_csv(io.StringIO(text), sep="|", dtype=str, engine="python")
    except Exception:
        return []
    if df is None or df.empty:
        return []
    df.columns = [str(c).strip() for c in df.columns]
    first_col = df.columns[0]
    df = df[~df[first_col].fillna("").str.startswith("File Creation Time", na=False)].copy()
    df = df.dropna(how="all")
    lower_cols = {str(c).strip().lower(): c for c in df.columns}

    exchange_col = lower_cols.get("exchange")
    etf_col = lower_cols.get("etf")
    test_col = lower_cols.get("test issue")
    name_col = lower_cols.get("security name")
    cqs_col = lower_cols.get("cqs symbol")
    nasdaq_col = lower_cols.get("nasdaq symbol")
    act_col = lower_cols.get("act symbol")

    if exchange_col is None:
        return []

    df[exchange_col] = df[exchange_col].fillna("").astype(str).str.strip().str.upper()
    df = df[df[exchange_col] == "N"]

    if etf_col is not None:
        df[etf_col] = df[etf_col].fillna("").astype(str).str.strip().str.upper()
        df = df[df[etf_col] != "Y"]

    if test_col is not None:
        df[test_col] = df[test_col].fillna("").astype(str).str.strip().str.upper()
        df = df[df[test_col] != "Y"]

    def _looks_like_equity_name(name):
        name = str(name or "").strip()
        if not name:
            return False
        low = f" {name.lower()} "
        reject_patterns = (
            r"\bpreferred\b",
            r"\bdepositary\b",
            r"\bwarrants?\b",
            r"\brights?\b",
            r"\bunits?\b",
            r"\bnotes?\b",
            r"\bbonds?\b",
            r"\bdebentures?\b",
            r"\betn\b",
            r"\betf\b",
            r"\bclosed\s+end\b",
            r"\bmutual\s+fund\b",
            r"\bbeneficial\s+interest\b",
            r"\btrust\s+units?\b",
        )
        return not any(re.search(p, low) for p in reject_patterns)

    collected = []
    for _, row in df.iterrows():
        name = row.get(name_col, "") if name_col is not None else ""
        if not _looks_like_equity_name(name):
            continue
        symbol = ""
        for col in (cqs_col, nasdaq_col, act_col):
            if col is None:
                continue
            candidate = str(row.get(col, "") or "").strip().upper()
            if candidate and candidate != "NAN":
                symbol = candidate
                break
        if symbol:
            collected.append(symbol)

    return _normalize_ticker_list(collected)


def _parse_nasdaq_listed_text(text):
    try:
        df = pd.read_csv(io.StringIO(text), sep="|", dtype=str, engine="python")
    except Exception:
        return []
    if df is None or df.empty:
        return []
    df.columns = [str(c).strip() for c in df.columns]
    first_col = df.columns[0]
    df = df[~df[first_col].fillna("").str.startswith("File Creation Time", na=False)].copy()
    df = df.dropna(how="all")
    lower_cols = {str(c).strip().lower(): c for c in df.columns}
    symbol_col = lower_cols.get("symbol")
    name_col = lower_cols.get("security name")
    etf_col = lower_cols.get("etf")
    test_col = lower_cols.get("test issue")
    nextshares_col = lower_cols.get("nextshares")
    if symbol_col is None:
        return []
    if etf_col is not None:
        df[etf_col] = df[etf_col].fillna("").astype(str).str.strip().str.upper()
        df = df[df[etf_col] != "Y"]
    if test_col is not None:
        df[test_col] = df[test_col].fillna("").astype(str).str.strip().str.upper()
        df = df[df[test_col] != "Y"]
    if nextshares_col is not None:
        df[nextshares_col] = df[nextshares_col].fillna("").astype(str).str.strip().str.upper()
        df = df[df[nextshares_col] != "Y"]

    def _looks_like_equity_name(name):
        low = f" {str(name or '').lower()} "
        reject_patterns = (
            r"\bpreferred\b",
            r"\bdepositary\b",
            r"\bwarrants?\b",
            r"\brights?\b",
            r"\bunits?\b",
            r"\bnotes?\b",
            r"\bbonds?\b",
            r"\bdebentures?\b",
            r"\betn\b",
            r"\betf\b",
            r"\bclosed\s+end\b",
            r"\bmutual\s+fund\b",
            r"\btrust\s+units?\b",
        )
        return low.strip() and not any(re.search(p, low) for p in reject_patterns)

    symbols = []
    for _, row in df.iterrows():
        name = row.get(name_col, "") if name_col is not None else ""
        if name_col is not None and not _looks_like_equity_name(name):
            continue
        symbol = str(row.get(symbol_col, "") or "").strip().upper()
        if symbol and symbol != "NAN":
            symbols.append(symbol)
    return _normalize_ticker_list(symbols)


def _get_nasdaq_stocks_from_nasdaq_trader_ftp():
    buf = io.BytesIO()
    ftp = None
    try:
        ftp = FTP("ftp.nasdaqtrader.com", timeout=30)
        ftp.login()
        ftp.cwd("SymbolDirectory")
        ftp.retrbinary("RETR nasdaqlisted.txt", buf.write)
        raw = buf.getvalue().decode("utf-8", errors="ignore")
        tickers = _parse_nasdaq_listed_text(raw)
        return tickers if len(tickers) >= 1500 else []
    except Exception:
        return []
    finally:
        try:
            if ftp is not None:
                ftp.quit()
        except Exception:
            pass


def _get_nasdaq_stocks_from_nasdaq_trader_http():
    urls = [
        "https://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt",
        "http://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt",
        "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt",
    ]
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "text/plain,*/*"}
    for url in urls:
        try:
            resp = requests.get(url, timeout=30, headers=headers)
            resp.raise_for_status()
            tickers = _parse_nasdaq_listed_text(resp.text)
            if len(tickers) >= 1500:
                return tickers
        except Exception:
            continue
    return []


def _get_nasdaq_stocks_from_github_fallback():
    urls = [
        "https://raw.githubusercontent.com/joemccann/stock-exchange-symbols/master/csv/nasdaq.csv",
    ]
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "text/plain,text/csv,*/*"}
    for url in urls:
        try:
            resp = requests.get(url, timeout=25, headers=headers)
            resp.raise_for_status()
            raw_text = resp.content.decode("utf-8", errors="ignore")
            tickers = []
            try:
                df = pd.read_csv(io.StringIO(raw_text))
                ticker_col = _extract_ticker_column(df)
                if ticker_col is not None:
                    tickers = _normalize_ticker_list(df[ticker_col])
            except Exception:
                tickers = []
            if len(tickers) < 1000:
                tickers = _normalize_ticker_list(re.split(r"[\s,;|]+", raw_text))
            if len(tickers) >= 1200:
                return tickers
        except Exception:
            continue
    return []

def _get_nyse_stocks_from_nasdaq_trader_ftp():
    buf = io.BytesIO()
    ftp = None
    try:
        ftp = FTP("ftp.nasdaqtrader.com", timeout=30)
        ftp.login()
        ftp.cwd("SymbolDirectory")
        ftp.retrbinary("RETR otherlisted.txt", buf.write)
        raw = buf.getvalue().decode("utf-8", errors="ignore")
        tickers = _parse_nasdaq_otherlisted_text(raw)
        return tickers if len(tickers) >= 1200 else []
    except Exception:
        return []
    finally:
        try:
            if ftp is not None:
                ftp.quit()
        except Exception:
            pass

def _get_nyse_stocks_from_nasdaq_trader_http():
    urls = [
        "https://ftp.nasdaqtrader.com/SymbolDirectory/otherlisted.txt",
        "http://ftp.nasdaqtrader.com/SymbolDirectory/otherlisted.txt",
        "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt",
    ]
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "text/plain,*/*"}
    for url in urls:
        try:
            resp = requests.get(url, timeout=30, headers=headers)
            resp.raise_for_status()
            tickers = _parse_nasdaq_otherlisted_text(resp.text)
            if len(tickers) >= 1200:
                return tickers
        except Exception:
            continue
    return []

def _get_nyse_stocks_from_github_fallback():
    urls = [
        "https://raw.githubusercontent.com/joemccann/stock-exchange-symbols/master/csv/nyse.csv",
    ]
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "text/plain,text/csv,*/*"}
    for url in urls:
        try:
            resp = requests.get(url, timeout=25, headers=headers)
            resp.raise_for_status()
            text = resp.content.decode("utf-8", errors="ignore")
            tickers = []
            try:
                df = pd.read_csv(io.StringIO(text))
                ticker_col = _extract_ticker_column(df)
                if ticker_col is not None:
                    tickers = _normalize_ticker_list(df[ticker_col])
            except Exception:
                tickers = []
            if len(tickers) < 500:
                tickers = _normalize_ticker_list(re.split(r"[\s,;|]+", text))
            if len(tickers) >= 1000:
                return tickers
        except Exception:
            continue
    return []

def get_nyse_stock_tickers():
    """Load a current NYSE stock universe from Nasdaq Trader otherlisted.txt with fallbacks."""
    best = []
    loaders = (
        _get_nyse_stocks_from_nasdaq_trader_ftp,
        _get_nyse_stocks_from_nasdaq_trader_http,
        _get_nyse_stocks_from_github_fallback,
    )
    for loader in loaders:
        tickers = loader()
        if len(tickers) > len(best):
            best = tickers
        if len(tickers) >= 1200:
            return tickers
    return best if len(best) >= 300 else []


def get_nasdaq_stock_tickers():
    """Load a current Nasdaq common-stock universe from Nasdaq Trader with fallbacks."""
    best = []
    loaders = (
        _get_nasdaq_stocks_from_nasdaq_trader_ftp,
        _get_nasdaq_stocks_from_nasdaq_trader_http,
        _get_nasdaq_stocks_from_github_fallback,
    )
    for loader in loaders:
        tickers = loader()
        if len(tickers) > len(best):
            best = tickers
        if len(tickers) >= 1500:
            return tickers
    return best if len(best) >= 500 else []


@st.cache_data(ttl=86400, show_spinner=False)
def get_app_stock_universe_tickers():
    """Combined internal stock universe for cache coverage, diagnostics and portfolio support."""
    nyse = _normalize_ticker_list(get_nyse_stock_tickers())
    nasdaq = _normalize_ticker_list(get_nasdaq_stock_tickers())

    # Guard against weak fallbacks that only return a handful of symbols and would
    # otherwise create the false impression that Nasdaq support is active.
    nyse_ok = len(nyse) >= 1000
    nasdaq_ok = len(nasdaq) >= 1000

    if nyse_ok and nasdaq_ok:
        combined = _normalize_ticker_list(nyse + nasdaq)
        if len(combined) >= 2500:
            return combined

    if nyse_ok:
        # At minimum keep the NYSE universe stable instead of mixing it with a broken
        # Nasdaq fallback that returned only a few dozen symbols.
        return nyse

    fallback = _normalize_ticker_list(SP500_TICKERS + nyse + nasdaq)
    return fallback


@st.cache_data(ttl=86400, show_spinner=False)
def get_app_stock_universe_breakdown():
    nyse = _normalize_ticker_list(get_nyse_stock_tickers())
    nasdaq = _normalize_ticker_list(get_nasdaq_stock_tickers())
    combined = _normalize_ticker_list(nyse + nasdaq)
    return {
        "nyse": len(nyse),
        "nasdaq": len(nasdaq),
        "combined": len(combined),
        "nasdaq_ok": len(nasdaq) >= 1000,
        "nyse_ok": len(nyse) >= 1000,
    }


@st.cache_data(ttl=86400, show_spinner=False)
def get_nyse_breadth_tickers():
    """Backward-compatible helper. Internally the supported universe now includes NYSE and Nasdaq common stocks."""
    return get_app_stock_universe_tickers()


@st.cache_data(ttl=86400, show_spinner=False)
def get_russell2000_tickers():
    """Backward-compatible wrapper kept for older code paths."""
    return get_nyse_breadth_tickers()

def _dl(symbol, start, end):
    try:
        df = yf.download(symbol, start=start, end=end, progress=False, auto_adjust=True)
        if df is None or len(df) == 0:
            return None
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df.index = pd.to_datetime(df.index)
        df = df.sort_index()
        for c in ["Open", "High", "Low", "Close", "Volume"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        return df.dropna(subset=["Close"])
    except Exception as exc:
        logger.warning("Download failed for %s: %s", symbol, exc)
        return None
@st.cache_data(ttl=900, show_spinner=False)
def load_market_data(lookback_days=400):
    end = datetime.now()
    start = end - timedelta(days=lookback_days)
    tickers = {
        "S&P 500": "^GSPC", "Nasdaq Composite": "^IXIC", "Russell 2000": "^RUT",
        "RSP (Equal-Weight S&P)": "RSP", "QQEW (Equal-Weight Nasdaq)": "QQEW",
        "VIX": "^VIX", "VIXY": "VIXY",
        "XLU (Utilities)": "XLU", "XLP (Consumer Staples)": "XLP",
        "XLK (Technology)": "XLK", "XLY (Consumer Discr.)": "XLY",
    }
    sym_to_name = {sym: name for name, sym in tickers.items()}
    symbols = list(sym_to_name.keys())
    data = {}

    try:
        raw = yf.download(
            symbols, start=start, end=end,
            progress=False, auto_adjust=True, group_by="ticker", threads=True,
        )
        if raw is not None and len(raw) > 0 and isinstance(raw.columns, pd.MultiIndex):
            for sym in symbols:
                frame = None
                try:
                    frame = raw[sym]
                except Exception:
                    try:
                        frame = raw.xs(sym, axis=1, level=0)
                    except Exception:
                        pass
                frame = _coerce_ohlc_frame(frame)
                if frame is not None and not frame.empty and len(frame) > 20:
                    data[sym_to_name[sym]] = frame
    except Exception:
        pass

    # Fallback for any symbols missing from the batch result
    for name, sym in tickers.items():
        if name not in data:
            df = _dl(sym, start, end)
            if df is not None and len(df) > 20:
                data[name] = df

    # Yahoo can intermittently return empty/failed results for ^GSPC.
    # Keep the S&P slot available by trying robust fallbacks.
    if "S&P 500" not in data:
        for sp_fallback in ["^SPX", "SPY"]:
            df = _dl(sp_fallback, start, end)
            if df is not None and len(df) > 20:
                data["S&P 500"] = df
                break
    return data

SECTOR_ETFS = {
    "XLK": "Technology",
    "XLV": "Health Care",
    "XLF": "Financials",
    "XLI": "Industrials",
    "XLP": "Consumer Staples",
    "XLY": "Consumer Discretionary",
    "XLE": "Energy",
    "XLB": "Materials",
    "XLC": "Communication Services",
    "XLU": "Utilities",
    "XLRE": "Real Estate",
}

@st.cache_data(ttl=900, show_spinner=False)
def load_sector_data(lookback_days=200):
    """Load daily close prices for all sector ETFs."""
    end = datetime.now(); start = end - timedelta(days=lookback_days)
    symbols = list(SECTOR_ETFS.keys())
    try:
        df = yf.download(symbols, start=start, end=end, progress=False, auto_adjust=True, threads=True)
        if df is None or len(df) == 0: return None
        if isinstance(df.columns, pd.MultiIndex):
            closes = df["Close"].copy()
        else:
            closes = df[["Close"]].copy()
        closes = closes.apply(pd.to_numeric, errors="coerce")
        closes.index = pd.to_datetime(closes.index)
        closes = closes.sort_index()
        return closes
    except Exception as e:
        return None

def _sector_period_returns(closes, mode="daily"):
    """Return daily changes or weekly average daily changes for sector ETFs."""
    if closes is None or len(closes) < 5: return None

    pct = closes.pct_change(fill_method=None) * 100
    pct = pct.dropna(how="all")
    if mode == "weekly":
        pct = pct.resample("W-FRI").mean().dropna(how="all")
    return pct


def build_sector_tables(closes, mode="daily", n_periods=15):
    """Build sector performance and rank tables for latest days or weekly averages."""
    if closes is None or len(closes) < 5: return None, None, None

    pct = _sector_period_returns(closes, mode=mode)
    if pct is None or len(pct) == 0: return None, None, None
    display_data = pct.tail(n_periods)

    if len(display_data) < 2: return None, None, None

    latest = display_data.iloc[-1].dropna()

    # Rename columns: ETF ticker → "Sektor (ETF)"
    col_rename = {etf: f"{name} ({etf})" for etf, name in SECTOR_ETFS.items()}

    # Build transposed tables: rows = sectors, columns = dates.
    # Newest trading day stays at the far left for quick mobile scanning.
    perf_table = display_data.T.copy()
    perf_table.index = [col_rename.get(idx, idx) for idx in perf_table.index]

    latest_col = perf_table.columns[-1]
    perf_table = perf_table.sort_values(by=latest_col, ascending=False)
    perf_table = perf_table.iloc[:, ::-1]
    if mode == "weekly":
        perf_table.columns = [d.strftime("KW%V %d.%m") for d in perf_table.columns]
    else:
        perf_table.columns = [d.strftime("%d.%m") for d in perf_table.columns]
    perf_table = perf_table.round(2)

    rank_data = display_data.rank(axis=1, ascending=False, method="min")
    rank_table = rank_data.T.copy()
    rank_table.index = [col_rename.get(idx, idx) for idx in rank_table.index]
    rank_table = rank_table.loc[perf_table.index]
    rank_table = rank_table.iloc[:, ::-1]
    rank_table.columns = perf_table.columns
    rank_table = rank_table.astype("Int64")

    # Also return the latest values for the ranking summary
    latest_ranked = latest.sort_values(ascending=False)
    latest_ranked.index = [col_rename.get(idx, idx) for idx in latest_ranked.index]

    return perf_table, rank_table, latest_ranked


def _read_secret_value(name: str) -> str:
    try:
        value = st.secrets.get(name, "")
    except Exception:
        value = ""
    if not value:
        value = os.environ.get(name, "")
    return value or ""


@st.cache_data(ttl=86400, show_spinner=False)
def _fetch_sec_ticker_cik_map():
    """Load SEC ticker→CIK mapping."""
    try:
        url = "https://www.sec.gov/files/company_tickers.json"
        headers = {"User-Agent": "boerse-dashboard/1.0 contact@example.com"}
        r = requests.get(url, headers=headers, timeout=15)
        if r.status_code != 200:
            return {}
        data = r.json()
        out = {}
        if isinstance(data, dict):
            for item in data.values():
                if not isinstance(item, dict):
                    continue
                tkr = str(item.get("ticker", "")).upper().strip()
                cik = item.get("cik_str")
                if tkr and cik is not None:
                    out[tkr] = str(int(cik)).zfill(10)
        return out
    except Exception:
        return {}


def _merge_quarterly_raw(primary, secondary):
    """Merge two qraw dictionaries, preferring primary values on date collisions."""
    if primary is None:
        return secondary
    if secondary is None:
        return primary
    out = dict(primary)
    for key in ["DilutedEPS", "TotalRevenue", "NetIncome", "StockholdersEquity"]:
        p = primary.get(key)
        s = secondary.get(key)
        if p is None and s is not None:
            out[key] = s.sort_index(ascending=False)
            continue
        if p is not None and s is not None:
            try:
                merged = pd.concat([p, s[~s.index.isin(p.index)]])
                out[key] = merged.sort_index(ascending=False)
            except Exception:
                out[key] = p
    return out


@st.cache_data(ttl=3600, show_spinner=False)
def _fetch_quarterly_sec_companyfacts(ticker):
    """Fetch quarterly EPS and Revenue from SEC companyfacts (US issuers only)."""
    try:
        ticker = str(ticker or "").upper().strip()
        if not ticker:
            return None, "Kein Ticker"
        cik_map = _fetch_sec_ticker_cik_map()
        cik = cik_map.get(ticker)
        if not cik:
            return None, "Nicht im SEC-Universum gefunden"

        headers = {"User-Agent": "boerse-dashboard/1.0 contact@example.com"}
        url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
        r = requests.get(url, headers=headers, timeout=15)
        if r.status_code != 200:
            return None, f"SEC HTTP {r.status_code}"
        data = r.json()

        facts = ((data or {}).get("facts") or {}).get("us-gaap") or {}
        eps_concepts = [
            "EarningsPerShareDiluted",
            "EarningsPerShareBasicAndDiluted",
            "IncomeLossFromContinuingOperationsPerDilutedShare",
        ]
        rev_concepts = [
            "Revenues",
            "RevenueFromContractWithCustomerExcludingAssessedTax",
            "RevenueFromContractWithCustomerIncludingAssessedTax",
            "SalesRevenueNet",
        ]

        def _extract_quarters(concepts, unit_keys, duration_filter=None):
            by_end = {}
            for concept in concepts:
                node = facts.get(concept) or {}
                units = node.get("units") or {}
                for unit_key in unit_keys:
                    entries = units.get(unit_key) or []
                    for item in entries:
                        form = str(item.get("form", ""))
                        fp = str(item.get("fp", ""))
                        if form not in ("10-Q", "10-K"):
                            continue
                        if fp not in ("Q1", "Q2", "Q3", "Q4"):
                            continue
                        end = item.get("end")
                        val = item.get("val")
                        if end is None or val is None:
                            continue
                        if duration_filter is not None:
                            start = item.get("start")
                            if start:
                                try:
                                    days = (pd.Timestamp(end) - pd.Timestamp(start)).days
                                    if not duration_filter(days):
                                        continue
                                except Exception:
                                    continue
                        try:
                            end_ts = pd.Timestamp(end)
                            numeric_val = float(val)
                        except Exception:
                            continue
                        filed = pd.Timestamp(item.get("filed")) if item.get("filed") else pd.Timestamp.min
                        prev = by_end.get(end_ts)
                        if prev is None or filed > prev[0]:
                            by_end[end_ts] = (filed, numeric_val)
            if not by_end:
                return None
            ser = pd.Series({k: v[1] for k, v in by_end.items()})
            return ser.sort_index(ascending=False)

        ni_concepts = [
            "NetIncomeLoss",
            "ProfitLoss",
            "NetIncomeLossAvailableToCommonStockholdersBasic",
        ]
        eq_concepts = [
            "StockholdersEquity",
            "StockholdersEquityAttributableToParent",
            "Equity",
            "LiabilitiesAndStockholdersEquity",
        ]

        def _extract_balance_sheet(concepts, unit_keys):
            """Extract point-in-time balance sheet values (10-K and 10-Q, any fp)."""
            by_end = {}
            for concept in concepts:
                node = facts.get(concept) or {}
                units = node.get("units") or {}
                for unit_key in unit_keys:
                    entries = units.get(unit_key) or []
                    for item in entries:
                        form = str(item.get("form", ""))
                        if form not in ("10-K", "10-Q"):
                            continue
                        end = item.get("end")
                        val = item.get("val")
                        if end is None or val is None:
                            continue
                        try:
                            end_ts = pd.Timestamp(end)
                            numeric_val = float(val)
                        except Exception:
                            continue
                        filed = pd.Timestamp(item.get("filed")) if item.get("filed") else pd.Timestamp.min
                        prev = by_end.get(end_ts)
                        if prev is None or filed > prev[0]:
                            by_end[end_ts] = (filed, numeric_val)
                if by_end:
                    break  # stop at first concept that has data
            if not by_end:
                return None
            ser = pd.Series({k: v[1] for k, v in by_end.items()})
            return ser.sort_index(ascending=False)

        quarter_duration = lambda d: 75 <= d <= 110  # noqa: E731 — kompakte Quartalsbreite
        annual_duration = lambda d: 350 <= d <= 380  # noqa: E731 — 10-K-Annual-Filings

        def _q4_from_annual(quarter_series, annual_series):
            """Q4 wird in 10-K-Filings als YTD/Annual gemeldet; rekonstruiere
            Q4 = Annual − (Q1 + Q2 + Q3) für jedes Jahr, in dem alle drei
            Vorquartale + ein Annual vorliegen."""
            if quarter_series is None or annual_series is None or annual_series.empty:
                return quarter_series
            updates = {}
            existing = set(quarter_series.index)
            quarter_by_year = quarter_series.groupby(quarter_series.index.year)
            for year, ann_value in annual_series.items():
                yr = pd.Timestamp(year).year
                if yr not in quarter_by_year.groups:
                    continue
                rows = quarter_by_year.get_group(yr)
                if len(rows) < 3:
                    continue
                # nur erstes drei Quartale heranziehen (Q1-Q3)
                first_three = rows.sort_index().head(3)
                q4_end = pd.Timestamp(year)
                if q4_end in existing:
                    continue
                try:
                    q4_val = float(ann_value) - float(first_three.sum())
                except (TypeError, ValueError):
                    continue
                updates[q4_end] = q4_val
            if not updates:
                return quarter_series
            combined = pd.concat([quarter_series, pd.Series(updates)])
            return combined.sort_index(ascending=False)

        # EPS-Concepts: ohne Duration-Filter würden YTD-Werte (Q3-YTD ~270 Tage)
        # fälschlich als Q3-Wert akzeptiert.
        eps_series = _extract_quarters(eps_concepts, ["USD/shares"], duration_filter=quarter_duration)
        eps_annual = _extract_quarters(eps_concepts, ["USD/shares"], duration_filter=annual_duration)
        rev_series = _extract_quarters(rev_concepts, ["USD"], duration_filter=quarter_duration)
        rev_annual = _extract_quarters(rev_concepts, ["USD"], duration_filter=annual_duration)
        ni_series = _extract_quarters(ni_concepts, ["USD"], duration_filter=quarter_duration)
        ni_annual = _extract_quarters(ni_concepts, ["USD"], duration_filter=annual_duration)
        # Q4 aus 10-K rekonstruieren (Annual − Q1 − Q2 − Q3).
        eps_series = _q4_from_annual(eps_series, eps_annual)
        rev_series = _q4_from_annual(rev_series, rev_annual)
        ni_series = _q4_from_annual(ni_series, ni_annual)
        eq_series = _extract_balance_sheet(eq_concepts, ["USD"])

        out = {}
        if eps_series is not None and len(eps_series) > 0:
            out["DilutedEPS"] = eps_series
        if rev_series is not None and len(rev_series) > 0:
            out["TotalRevenue"] = rev_series
        if ni_series is not None and len(ni_series) > 0:
            out["NetIncome"] = ni_series
        if eq_series is not None and len(eq_series) > 0:
            out["StockholdersEquity"] = eq_series
        if not out:
            return None, "Keine SEC-Quartalsdaten gefunden"
        return out, None
    except requests.exceptions.Timeout:
        return None, "SEC Timeout (>15s)"
    except requests.exceptions.ConnectionError as e:
        return None, f"SEC Verbindungsfehler: {str(e)[:60]}"
    except Exception as e:
        return None, f"SEC Fehler: {type(e).__name__}: {str(e)[:60]}"


@st.cache_data(ttl=3600, show_spinner=False)
def _fetch_quarterly_fmp(ticker, fmp_key):
    """Fetch quarterly EPS and Revenue from Financial Modeling Prep (up to 12 quarters)."""
    if not fmp_key:
        return None, "Kein API-Key"
    try:
        import json
        attempts = [
            (
                "stable",
                "https://financialmodelingprep.com/stable/income-statement",
                {"symbol": str(ticker or "").upper().strip(), "period": "quarter", "limit": 12, "apikey": fmp_key},
            ),
            (
                "legacy",
                f"https://financialmodelingprep.com/api/v3/income-statement/{ticker}",
                {"period": "quarter", "limit": 12, "apikey": fmp_key},
            ),
        ]
        errors = []

        for label, url, params in attempts:
            r = requests.get(url, params=params, timeout=15)
            if r.status_code == 429:
                errors.append(f"{label}: Rate Limited (429)")
                continue
            if r.status_code == 403:
                errors.append(f"{label}: Zugriff verweigert (403)")
                continue
            if r.status_code == 401:
                errors.append(f"{label}: API-Key ungültig (401)")
                continue
            if r.status_code != 200:
                errors.append(f"{label}: HTTP {r.status_code}")
                continue

            data = json.loads(r.text)
            if isinstance(data, dict) and "Error Message" in data:
                errors.append(f"{label}: {data['Error Message'][:120]}")
                continue
            if isinstance(data, str):
                if data.strip():
                    errors.append(f"{label}: {data[:120]}")
                else:
                    errors.append(f"{label}: Leere Antwort")
                continue
            if not data or not isinstance(data, list):
                errors.append(f"{label}: Leere Antwort (Typ: {type(data).__name__})")
                continue

            out = {}
            eps_vals = {}
            rev_vals = {}
            ni_vals = {}
            for item in data:
                date_str = item.get("date", "")
                if not date_str:
                    continue
                dt = pd.Timestamp(date_str)
                eps = item.get("epsDiluted", item.get("epsdiluted", item.get("eps")))
                rev = item.get("revenue")
                ni = item.get("netIncome", item.get("netincome"))
                if eps is not None:
                    eps_vals[dt] = float(eps)
                if rev is not None:
                    rev_vals[dt] = float(rev)
                if ni is not None:
                    ni_vals[dt] = float(ni)
            if eps_vals:
                out["DilutedEPS"] = pd.Series(eps_vals).sort_index(ascending=False)
            if rev_vals:
                out["TotalRevenue"] = pd.Series(rev_vals).sort_index(ascending=False)
            if ni_vals:
                out["NetIncome"] = pd.Series(ni_vals).sort_index(ascending=False)
            if out:
                endpoint_label = "FMP stable" if label == "stable" else "FMP legacy"
                # Fetch TTM ratios for ROE and profit margin (lightweight call)
                try:
                    ratios_urls = [
                        ("stable", f"https://financialmodelingprep.com/stable/ratios-ttm", {"symbol": str(ticker).upper(), "apikey": fmp_key}),
                        ("legacy", f"https://financialmodelingprep.com/api/v3/ratios-ttm/{ticker}", {"apikey": fmp_key}),
                    ]
                    for _rlabel, _rurl, _rparams in ratios_urls:
                        _rr = requests.get(_rurl, params=_rparams, timeout=10)
                        if _rr.status_code == 200:
                            _rdata = _rr.json()
                            _ritem = _rdata[0] if isinstance(_rdata, list) and _rdata else (_rdata if isinstance(_rdata, dict) else {})
                            roe_ttm = _ritem.get("returnOnEquityTTM")
                            pm_ttm = _ritem.get("netProfitMarginTTM")
                            if roe_ttm is not None:
                                out["_roe_ttm"] = float(roe_ttm)
                            if pm_ttm is not None:
                                out["_pm_ttm"] = float(pm_ttm)
                            break
                except Exception:
                    pass
                return out, endpoint_label
            errors.append(f"{label}: Keine verwertbaren Quartalsdaten")

        return None, " | ".join(errors) if errors else "Keine FMP-Quartalsdaten"
    except requests.exceptions.ConnectionError as e:
        return None, f"Verbindungsfehler: {str(e)[:60]}"
    except requests.exceptions.Timeout:
        return None, "Timeout (>15s)"
    except Exception as e:
        return None, f"Fehler: {type(e).__name__}: {str(e)[:60]}"


def _load_ticker_attr_value(ticker_obj, ticker_symbol, attr_name):
    try:
        return getattr(ticker_obj, attr_name)
    except Exception as exc:
        logger.debug("Ticker attribute %s failed for %s: %s", attr_name, ticker_symbol, exc)
        # yfinance exposes some payloads both as properties and getter methods.
        # Try method fallback so intermittent/property-specific failures don't
        # blank out core fundamental checks.
        method_fallbacks = {
            "info": "get_info",
            "institutional_holders": "get_institutional_holders",
        }
        method_name = method_fallbacks.get(attr_name)
        if method_name:
            try:
                method = getattr(ticker_obj, method_name, None)
                if callable(method):
                    return method()
            except Exception as method_exc:
                logger.debug("Ticker method %s failed for %s: %s", method_name, ticker_symbol, method_exc)
        # Network fallback for common crumb-related Yahoo failures.
        if attr_name in {"info", "institutional_holders"}:
            try:
                if attr_name == "info":
                    info = _fetch_yahoo_info_via_http(ticker_symbol)
                    if info:
                        return info
                if attr_name == "institutional_holders":
                    holders = _fetch_yahoo_institutional_holders_via_http(ticker_symbol)
                    if holders is not None and len(holders):
                        return holders
            except Exception as http_exc:
                logger.debug("HTTP fallback for %s failed for %s: %s", attr_name, ticker_symbol, http_exc)
        return None


def _fetch_yahoo_info_via_http(ticker_symbol: str) -> dict:
    symbol = str(ticker_symbol or "").strip().upper()
    if not symbol:
        return {}
    url = f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{symbol}"
    modules = ",".join([
        "price", "summaryProfile", "defaultKeyStatistics",
        "financialData", "majorHoldersBreakdown",
    ])
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(url, params={"modules": modules}, headers=headers, timeout=12)
    resp.raise_for_status()
    payload = (resp.json() or {}).get("quoteSummary", {}).get("result", [])
    if not payload:
        return {}
    block = payload[0]
    out: dict[str, object] = {}
    price = block.get("price", {}) if isinstance(block, dict) else {}
    stats = block.get("defaultKeyStatistics", {}) if isinstance(block, dict) else {}
    fin = block.get("financialData", {}) if isinstance(block, dict) else {}
    profile = block.get("summaryProfile", {}) if isinstance(block, dict) else {}
    holders_breakdown = block.get("majorHoldersBreakdown", {}) if isinstance(block, dict) else {}

    def _raw(d, key):
        v = d.get(key) if isinstance(d, dict) else None
        if isinstance(v, dict):
            return v.get("raw", v.get("fmt"))
        return v

    out["shortName"] = _raw(price, "shortName") or _raw(price, "longName")
    out["returnOnEquity"] = _raw(fin, "returnOnEquity")
    out["profitMargins"] = _raw(fin, "profitMargins")
    out["grossMargins"] = _raw(fin, "grossMargins")
    out["operatingMargins"] = _raw(fin, "operatingMargins")
    out["debtToEquity"] = _raw(fin, "debtToEquity")
    out["revenueGrowth"] = _raw(fin, "revenueGrowth")
    out["earningsGrowth"] = _raw(fin, "earningsGrowth")
    out["beta"] = _raw(stats, "beta")
    out["bookValue"] = _raw(stats, "bookValue")
    out["sharesOutstanding"] = _raw(stats, "sharesOutstanding") or _raw(price, "sharesOutstanding")
    out["sector"] = _raw(profile, "sector")
    out["industry"] = _raw(profile, "industry")
    out["heldPercentInstitutions"] = _raw(holders_breakdown, "institutionsPercentHeld")
    return {k: v for k, v in out.items() if v is not None}


def _fetch_yahoo_institutional_holders_via_http(ticker_symbol: str) -> pd.DataFrame | None:
    symbol = str(ticker_symbol or "").strip().upper()
    if not symbol:
        return None
    url = f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{symbol}"
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(url, params={"modules": "institutionOwnership"}, headers=headers, timeout=12)
    resp.raise_for_status()
    payload = (resp.json() or {}).get("quoteSummary", {}).get("result", [])
    if not payload:
        return None
    ownership = payload[0].get("institutionOwnership", {}) if isinstance(payload[0], dict) else {}
    rows = ownership.get("ownershipList", []) if isinstance(ownership, dict) else []
    if not rows:
        return None
    parsed = []
    for item in rows:
        org = item.get("organization")
        pct = item.get("pctHeld")
        pct_val = pct.get("raw") if isinstance(pct, dict) else pct
        val = item.get("value")
        val_num = val.get("raw") if isinstance(val, dict) else val
        parsed.append({"Holder": org, "pctHeld": pct_val, "Value": val_num})
    frame = pd.DataFrame(parsed)
    return frame if len(frame) else None


def _fetch_fmp_institutional_holders(ticker, fmp_key):
    """Fetch institutional holders from FMP as fallback when Yahoo Finance fails."""
    if not fmp_key:
        return None
    symbol = str(ticker or "").upper().strip()
    if not symbol:
        return None
    attempts = [
        f"https://financialmodelingprep.com/stable/institutional-ownership/list?symbol={symbol}&apikey={fmp_key}",
        f"https://financialmodelingprep.com/api/v3/institutional-holder/{symbol}?apikey={fmp_key}",
    ]
    for url in attempts:
        try:
            r = requests.get(url, timeout=10)
            if r.status_code != 200:
                continue
            data = r.json()
            items = data if isinstance(data, list) else (data.get("data", []) if isinstance(data, dict) else [])
            if not items:
                continue
            parsed = []
            for item in items:
                holder = item.get("holder") or item.get("institutionName") or item.get("investor")
                shares = item.get("shares") or item.get("totalShares")
                val = item.get("value") or item.get("totalValue")
                parsed.append({"Holder": holder, "Shares": shares, "Value": val})
            if parsed:
                return pd.DataFrame(parsed)
        except Exception:
            continue
    return None


def _load_ticker_earnings_dates(ticker_obj, ticker_symbol, limit=12):
    try:
        return ticker_obj.get_earnings_dates(limit=limit)
    except Exception as exc:
        logger.debug("earnings dates failed for %s: %s", ticker_symbol, exc)
        return None


def _load_stock_reference_data_parallel(ticker, fmp_key, ticker_obj=None):
    ticker_obj = ticker_obj or yf.Ticker(ticker)
    task_specs = {
        "info": (_load_ticker_attr_value, ticker_obj, ticker, "info"),
        "qi": (_load_ticker_attr_value, ticker_obj, ticker, "quarterly_income_stmt"),
        "ai": (_load_ticker_attr_value, ticker_obj, ticker, "income_stmt"),
        "ih": (_load_ticker_attr_value, ticker_obj, ticker, "institutional_holders"),
        "ed": (_load_ticker_earnings_dates, ticker_obj, ticker, 12),
        "sec": (_fetch_quarterly_sec_companyfacts, ticker),
    }
    if fmp_key:
        task_specs["fmp"] = (_fetch_quarterly_fmp, ticker, fmp_key)

    results = {
        "info": {},
        "qi": None,
        "ai": None,
        "ih": None,
        "qe": None,
        "ed": None,
        "fmp": (None, None),
        "sec": (None, None),
    }
    max_workers = max(1, min(6, len(task_specs)))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for key, spec in task_specs.items():
            fn, *args = spec
            futures[executor.submit(fn, *args)] = key
        for future in as_completed(futures):
            key = futures[future]
            try:
                results[key] = future.result()
            except Exception as exc:
                logger.debug("Parallel stock component %s failed for %s: %s", key, ticker, exc)
    return results


@st.cache_data(ttl=900, show_spinner=False)
def load_stock_full(ticker, lookback_days=500, cache_buster=0):
    """Load price history plus the most important fundamental datasets for one ticker."""
    end = datetime.now()
    start = end - timedelta(days=lookback_days)
    empty_result = (None, None, None, None, None, None, None, None, None)
    try:
        ticker_obj = yf.Ticker(ticker)
        try:
            df = ticker_obj.history(start=start, end=end, auto_adjust=True)
        except Exception:
            df = None

        if df is None or len(df) < 20:
            # Fallback: use the shared downloader with symbol variants/retry path
            fallback_map = _bulk_download_ohlc((str(ticker).upper(),), start, end)
            df = fallback_map.get(str(ticker).upper()) if isinstance(fallback_map, dict) else None
        if df is None or len(df) < 20:
            return empty_result

        df.index = pd.to_datetime(df.index)
        df = df.sort_index()
        for c in ["Open", "High", "Low", "Close", "Volume"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")

        fmp_key = _read_secret_value("FMP_API_KEY")
        components = _load_stock_reference_data_parallel(ticker, fmp_key, ticker_obj=ticker_obj)

        info = components.get("info") or {}
        qi = components.get("qi")
        ai = components.get("ai")
        ih = components.get("ih")
        qe = components.get("qe")
        ed = components.get("ed")

        # Some Yahoo endpoints fail intermittently when fetched in parallel.
        # Retry critical fields sequentially (including a fresh Ticker object)
        # so ROE/margins/institutional checks don't end up "Nicht verfügbar".
        needs_info = (not info) or (isinstance(info, dict) and not info.get("returnOnEquity"))
        _ih_empty = ih is None or (isinstance(ih, pd.DataFrame) and ih.empty)
        needs_ih = _ih_empty
        if needs_info or needs_ih:
            for attempt in range(2):
                source_obj = ticker_obj if attempt == 0 else yf.Ticker(ticker)
                if needs_info:
                    retry_info = _load_ticker_attr_value(source_obj, ticker, "info") or {}
                    if isinstance(retry_info, dict) and retry_info:
                        info = retry_info
                        needs_info = not info.get("returnOnEquity")
                if needs_ih:
                    retry_ih = _load_ticker_attr_value(source_obj, ticker, "institutional_holders")
                    _retry_ih_ok = (
                        retry_ih is not None
                        and not (isinstance(retry_ih, pd.DataFrame) and retry_ih.empty)
                    )
                    if _retry_ih_ok:
                        ih = retry_ih
                        needs_ih = False
                if not needs_info and not needs_ih:
                    break
        # Final FMP fallback for institutional holders (when Yahoo is completely blocked)
        if needs_ih and fmp_key:
            fmp_ih = _fetch_fmp_institutional_holders(ticker, fmp_key)
            if fmp_ih is not None and not fmp_ih.empty:
                ih = fmp_ih

        qraw = None
        fmp_err = None
        fmp_result = components.get("fmp")
        if isinstance(fmp_result, tuple):
            qraw, fmp_err = fmp_result
        elif fmp_result is not None:
            qraw = fmp_result

        # SEC fallback/augment (especially useful when Yahoo/FMP provides only few quarters)
        sec_raw, sec_err = components.get("sec") or (None, None)
        if sec_raw is not None:
            qraw = _merge_quarterly_raw(qraw, sec_raw)
            if qraw is not None:
                if fmp_err:
                    fmp_err = f"{fmp_err} | SEC ergänzt"
                else:
                    fmp_err = "SEC ergänzt"
        elif qraw is None and sec_err:
            fmp_err = sec_err if not fmp_err else f"{fmp_err} | {sec_err}"

        return df, info, qi, ai, ih, qe, ed, qraw, fmp_err
    except Exception as exc:
        logger.warning("load_stock_full failed for %s: %s", ticker, exc)
        return empty_result


@st.cache_data(ttl=3600, show_spinner=False)
def load_sp500_for_rs(lookback_days=400):
    end = datetime.now()
    start = end - timedelta(days=lookback_days)
    return _dl("^GSPC", start, end)


def _load_stock_analysis_context_parallel(ticker, rs_source_setting, cache_buster=0):
    stock_result = (None, None, None, None, None, None, None, None, None)
    spx_df = None
    rs_universe_scores = None
    max_workers = 3 if rs_source_setting == RS_SOURCE_COMPUTED else 2

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        stock_future = executor.submit(load_stock_full, ticker, 500, cache_buster)
        spx_future = executor.submit(load_sp500_for_rs)
        rs_future = (
            executor.submit(load_cached_universe_rs_scores)
            if rs_source_setting == RS_SOURCE_COMPUTED
            else None
        )

        try:
            stock_result = stock_future.result()
        except Exception as exc:
            logger.warning("Parallel stock load failed for %s: %s", ticker, exc)
        try:
            spx_df = spx_future.result()
        except Exception as exc:
            logger.warning("Parallel S&P load failed for %s: %s", ticker, exc)
        if rs_future is not None:
            try:
                rs_universe_scores = rs_future.result()
            except Exception as exc:
                logger.warning("Parallel RS universe load failed for %s: %s", ticker, exc)

    return (*stock_result, spx_df, rs_universe_scores)

# ===== From cache_store.py =====
try:
    import psycopg2
    from psycopg2.extras import execute_values
except Exception:
    psycopg2 = None
    execute_values = None


CACHE_DB_NAME = "market_data_cache.sqlite"

_db_initialized: set = set()

CACHE_UNIVERSE_NAME = "us_common_stocks_v3"
BREADTH_SNAPSHOT_KEY = f"{CACHE_UNIVERSE_NAME}_breadth_snapshot_v1"
BREADTH_SNAPSHOT_AT_KEY = f"{CACHE_UNIVERSE_NAME}_breadth_snapshot_generated_at"
BREADTH_SNAPSHOT_SOURCE_KEY = f"{CACHE_UNIVERSE_NAME}_breadth_snapshot_source"
RS_SNAPSHOT_KEY = f"{CACHE_UNIVERSE_NAME}_rs_scores_sp500_v1"
RS_SNAPSHOT_AT_KEY = f"{CACHE_UNIVERSE_NAME}_rs_scores_sp500_generated_at"
RS_SNAPSHOT_SOURCE_KEY = f"{CACHE_UNIVERSE_NAME}_rs_scores_sp500_source"

def _safe_get_secret(*path, default=None):
    try:
        cur = st.secrets
        for key in path:
            cur = cur[key]
        return cur
    except Exception:
        return default

def _get_neon_connection_url():
    candidates = [
        _safe_get_secret("connections", "neon", "url", default=""),
        _safe_get_secret("NEON_DATABASE_URL", default=""),
        os.environ.get("NEON_DATABASE_URL", ""),
        os.environ.get("DATABASE_URL", ""),
    ]
    for value in candidates:
        if value and str(value).strip():
            return str(value).strip()
    return ""


@st.cache_data(ttl=300, show_spinner=False)
def _can_connect_neon(dsn: str) -> bool:
    """Best-effort Neon health check to avoid hard-crashing the app on DB outages."""
    if not dsn or psycopg2 is None:
        return False
    conn = None
    try:
        conn = psycopg2.connect(dsn, connect_timeout=5)
        return True
    except Exception as exc:
        logger.warning("Neon connection check failed, fallback to SQLite cache: %s", exc)
        return False
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def _get_cache_db_path():
    base_dir = Path(__file__).resolve().parent if "__file__" in globals() else Path(os.getcwd())
    cache_dir = base_dir / ".cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return str(cache_dir / CACHE_DB_NAME)

def _get_price_store():
    settings = st.session_state.get("portfolio_settings", {})
    has_explicit_preference = isinstance(settings, dict) and "db_backend_preference" in settings
    preference = settings.get("db_backend_preference", "sqlite") if isinstance(settings, dict) else "sqlite"
    if preference not in {"sqlite", "neon"}:
        preference = "sqlite"

    neon_url = _get_neon_connection_url()
    should_try_neon = preference == "neon"
    # Backward-compatible bootstrap for existing workspaces that do not yet persist
    # db_backend_preference: keep former auto-neon behavior until a preference is saved.
    if not has_explicit_preference:
        should_try_neon = True

    if should_try_neon and neon_url and psycopg2 is not None and _can_connect_neon(neon_url):
        return {"backend": "neon", "dsn": neon_url, "label": "Neon Postgres"}
    return {"backend": "sqlite", "db_path": _get_cache_db_path(), "label": "lokaler SQLite-Cache"}

def _get_store_label(store):
    return store.get("label", store.get("backend", "Datenspeicher"))


def _is_neon_auto_update_enabled(store) -> bool:
    if not isinstance(store, dict) or store.get("backend") != "neon":
        return False
    raw = str(_get_cache_metadata(store, "neon_auto_update_enabled", "1") or "1").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _set_neon_auto_update_enabled(store, enabled: bool) -> None:
    if not isinstance(store, dict) or store.get("backend") != "neon":
        return
    _set_cache_metadata_many(store, {"neon_auto_update_enabled": "1" if enabled else "0"})

def _is_neon_conn(conn) -> bool:
    """Echte Backend-Erkennung: ``_get_cache_conn`` kann transparent von Neon
    auf SQLite zurückfallen. SQL-Branches müssen sich am tatsächlichen Conn-Typ
    orientieren, nicht ausschließlich an ``store["backend"]``."""
    return conn is not None and not isinstance(conn, sqlite3.Connection)


def _get_cache_conn(store):
    if store["backend"] == "neon":
        if psycopg2 is None:
            raise RuntimeError("psycopg2-binary ist nicht installiert. Bitte in requirements.txt ergänzen.")
        try:
            return psycopg2.connect(store["dsn"], connect_timeout=15)
        except Exception as exc:
            logger.warning("Neon-Verbindung fehlgeschlagen, nutze SQLite-Fallback: %s", exc)
            fallback = sqlite3.connect(_get_cache_db_path(), timeout=30)
            fallback.execute("PRAGMA journal_mode=WAL;")
            fallback.execute("PRAGMA synchronous=NORMAL;")
            return fallback
    conn = sqlite3.connect(store["db_path"], timeout=30)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn

def _init_price_cache_db(store):
    _init_key = f"{store.get('backend')}:{str(store.get('db_path', store.get('dsn', '')))[:40]}"
    if _init_key in _db_initialized:
        return
    conn = _get_cache_conn(store)
    # _get_cache_conn may silently fall back from Neon to SQLite when Neon is
    # temporarily unreachable. Only mark initialized when we actually ran DDL
    # against the intended backend — so the next call retries Neon once it's back.
    actual_backend = "sqlite" if isinstance(conn, sqlite3.Connection) else "neon"
    try:
        # Auch hier Backend anhand des tatsächlichen Conn-Typs wählen, damit
        # ein Neon→SQLite-Fallback nicht in den Neon-Pfad mit %s-Platzhaltern läuft.
        is_neon = actual_backend == "neon"
        cur = conn.cursor() if is_neon else conn
        if is_neon:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS prices (
                    symbol TEXT NOT NULL,
                    date DATE NOT NULL,
                    close DOUBLE PRECISION,
                    high DOUBLE PRECISION,
                    low DOUBLE PRECISION,
                    PRIMARY KEY (symbol, date)
                )
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_prices_date ON prices(date)")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS universe_members (
                    universe TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (universe, symbol)
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS app_metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS symbol_mappings (
                    universe TEXT NOT NULL,
                    source_symbol TEXT NOT NULL,
                    yahoo_symbol TEXT,
                    status TEXT,
                    note TEXT,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (universe, source_symbol)
                )
                """
            )
        else:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS prices (
                    symbol TEXT NOT NULL,
                    date TEXT NOT NULL,
                    close REAL,
                    high REAL,
                    low REAL,
                    PRIMARY KEY (symbol, date)
                )
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_prices_date ON prices(date)")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS universe_members (
                    universe TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (universe, symbol)
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS app_metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT,
                    updated_at TEXT NOT NULL
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS symbol_mappings (
                    universe TEXT NOT NULL,
                    source_symbol TEXT NOT NULL,
                    yahoo_symbol TEXT,
                    status TEXT,
                    note TEXT,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (universe, source_symbol)
                )
                """
            )

        if is_neon:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS refresh_jobs (
                    job_id TEXT PRIMARY KEY,
                    job_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    progress INTEGER NOT NULL DEFAULT 0,
                    current_step TEXT,
                    message TEXT,
                    requested_by TEXT,
                    trigger_mode TEXT,
                    trigger_status TEXT,
                    trigger_response TEXT,
                    payload_json TEXT,
                    result_json TEXT,
                    runner_source TEXT,
                    run_url TEXT,
                    requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    started_at TIMESTAMPTZ,
                    finished_at TIMESTAMPTZ,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_refresh_jobs_status_requested_at ON refresh_jobs(status, requested_at DESC)")
        else:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS refresh_jobs (
                    job_id TEXT PRIMARY KEY,
                    job_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    progress INTEGER NOT NULL DEFAULT 0,
                    current_step TEXT,
                    message TEXT,
                    requested_by TEXT,
                    trigger_mode TEXT,
                    trigger_status TEXT,
                    trigger_response TEXT,
                    payload_json TEXT,
                    result_json TEXT,
                    runner_source TEXT,
                    run_url TEXT,
                    requested_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    updated_at TEXT NOT NULL
                )
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_refresh_jobs_status_requested_at ON refresh_jobs(status, requested_at DESC)")
        if is_neon:
            cur.execute("ALTER TABLE prices ADD COLUMN IF NOT EXISTS high DOUBLE PRECISION")
            cur.execute("ALTER TABLE prices ADD COLUMN IF NOT EXISTS low DOUBLE PRECISION")
        else:
            existing_cols = {row[1] for row in conn.execute("PRAGMA table_info(prices)").fetchall()}
            if "high" not in existing_cols:
                conn.execute("ALTER TABLE prices ADD COLUMN high REAL")
            if "low" not in existing_cols:
                conn.execute("ALTER TABLE prices ADD COLUMN low REAL")
        conn.commit()
    finally:
        conn.close()
    if actual_backend == store.get("backend"):
        _db_initialized.add(_init_key)


def _set_cache_metadata(store, key, value, *, conn=None):
    own_conn = conn is None
    conn = conn or _get_cache_conn(store)
    try:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        # _get_cache_conn kann bei nicht erreichbarem Neon transparent auf SQLite
        # zurückfallen — Backend-Wahl daher anhand des tatsächlichen Conn-Typs,
        # nicht ausschließlich anhand store["backend"].
        use_neon = store.get("backend") == "neon" and not isinstance(conn, sqlite3.Connection)
        if use_neon:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO app_metadata(key, value, updated_at)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value, updated_at=NOW()
                    """,
                    (key, str(value)),
                )
        else:
            conn.execute(
                """
                INSERT INTO app_metadata(key, value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
                """,
                (key, str(value), now),
            )
        if own_conn:
            conn.commit()
    finally:
        if own_conn:
            conn.close()


def _set_cache_metadata_many(store, values):
    conn = _get_cache_conn(store)
    try:
        for key, value in values.items():
            _set_cache_metadata(store, key, value, conn=conn)
        conn.commit()
    finally:
        conn.close()


def _get_cache_metadata(store, key, default=None):
    conn = None
    try:
        conn = _get_cache_conn(store)
        is_sqlite = isinstance(conn, sqlite3.Connection)
        if store.get("backend") == "neon" and not is_sqlite:
            with conn.cursor() as cur:
                cur.execute("SELECT value FROM app_metadata WHERE key=%s", (key,))
                row = cur.fetchone()
                return row[0] if row else default
        row = conn.execute("SELECT value FROM app_metadata WHERE key=?", (key,)).fetchone()
        return row[0] if row else default
    except Exception as exc:
        logger.warning("Konnte Cache-Metadaten '%s' nicht lesen (%s), nutze Default.", key, exc)
        return default
    finally:
        if conn is not None:
            conn.close()


def _get_cache_metadata_many(store, keys):
    key_list = [str(k) for k in (keys or []) if str(k)]
    if not key_list:
        return {}
    conn = None
    try:
        conn = _get_cache_conn(store)
        is_sqlite = isinstance(conn, sqlite3.Connection)
        if store.get("backend") == "neon" and not is_sqlite:
            placeholders = ", ".join(["%s"] * len(key_list))
            sql = f"SELECT key, value FROM app_metadata WHERE key IN ({placeholders})"
            with conn.cursor() as cur:
                cur.execute(sql, tuple(key_list))
                rows = cur.fetchall() or []
        else:
            placeholders = ", ".join(["?"] * len(key_list))
            sql = f"SELECT key, value FROM app_metadata WHERE key IN ({placeholders})"
            rows = conn.execute(sql, tuple(key_list)).fetchall() or []
        return {row[0]: row[1] for row in rows}
    except Exception as exc:
        logger.warning("Konnte Cache-Metadaten-Liste nicht lesen (%s), nutze leere Antwort.", exc)
        return {}
    finally:
        if conn is not None:
            conn.close()


def _utc_now_str():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _safe_int(value, default=0):
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except Exception:
        return default


def _json_safe_metadata(value):
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, (pd.Timestamp, datetime)):
        return pd.Timestamp(value).strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(k): _json_safe_metadata(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe_metadata(v) for v in value]
    if pd.isna(value):
        return None
    return str(value)


def _serialize_breadth_snapshot(frame):
    if frame is None or frame.empty:
        return ""
    payload = {
        "frame": json.loads(frame.to_json(orient="split", date_format="iso")),
        "attrs": _json_safe_metadata(getattr(frame, "attrs", {}) or {}),
    }
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def _deserialize_breadth_snapshot(blob):
    if not blob:
        return None
    try:
        payload = json.loads(blob)
        frame_payload = payload.get("frame")
        if not frame_payload:
            return None
        frame = pd.read_json(io.StringIO(json.dumps(frame_payload)), orient="split")
        frame.index = pd.to_datetime(frame.index)
        frame = frame.sort_index()
        frame.attrs = payload.get("attrs", {}) or {}
        return frame if not frame.empty else None
    except Exception as exc:
        logger.warning("Konnte Breadth-Snapshot nicht laden (%s).", exc)
        return None


def _store_breadth_snapshot(store, breadth_frame, source="refresh"):
    now_str = _utc_now_str()
    payload = _serialize_breadth_snapshot(breadth_frame)
    values = {
        BREADTH_SNAPSHOT_KEY: payload,
        BREADTH_SNAPSHOT_AT_KEY: now_str,
        BREADTH_SNAPSHOT_SOURCE_KEY: source,
    }
    _set_cache_metadata_many(store, values)
    return now_str


def _serialize_series_snapshot(series):
    if series is None or len(series) == 0:
        return ""
    payload = {
        "series": json.loads(series.to_json(date_format="iso")),
        "name": getattr(series, "name", None),
    }
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def _deserialize_series_snapshot(blob):
    if not blob:
        return None
    try:
        payload = json.loads(blob)
        series_payload = payload.get("series")
        if not isinstance(series_payload, dict) or not series_payload:
            return None
        ser = pd.Series(series_payload, dtype=float)
        ser.index = ser.index.astype(str)
        ser.name = payload.get("name")
        ser = pd.to_numeric(ser, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
        return ser.sort_values(ascending=False) if not ser.empty else None
    except Exception as exc:
        logger.warning("Konnte RS-Snapshot nicht laden (%s).", exc)
        return None


def _store_relative_strength_snapshot(store, scores, source="refresh"):
    now_str = _utc_now_str()
    values = {
        RS_SNAPSHOT_KEY: _serialize_series_snapshot(scores),
        RS_SNAPSHOT_AT_KEY: now_str,
        RS_SNAPSHOT_SOURCE_KEY: source,
    }
    _set_cache_metadata_many(store, values)
    return now_str


def _get_rs_output_path() -> Path:
    base_dir = Path(__file__).resolve().parent if "__file__" in globals() else Path(os.getcwd())
    return base_dir / RS_OUTPUT_DIR_NAME / RS_OUTPUT_FILE_NAME


def _scores_to_rs_ratings(scores: pd.Series | None) -> pd.Series:
    if scores is None:
        return pd.Series(dtype=int)
    ser = pd.to_numeric(scores, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
    if ser.empty:
        return pd.Series(dtype=int)
    ratings = ser.rank(pct=True, method="average").mul(99).round().clip(lower=1, upper=99).astype(int)
    return ratings.sort_values(ascending=False)


def _parse_rs_ratings_frame(df: pd.DataFrame, *, source_name: str, file_name: str = "", path_name: str = "") -> dict:
    payload = {
        "ok": False,
        "source": source_name,
        "file": str(file_name or ""),
        "path": str(path_name or ""),
        "value_column": "",
        "score_column": "",
        "count": 0,
        "ratings": {},
        "scores": {},
        "as_of_date": "",
        "generated_at_utc": "",
        "error": "",
    }
    if df is None or df.empty:
        payload["error"] = "CSV ist leer."
        return payload

    normalized_cols = {str(c).strip().lower().replace(" ", "_"): c for c in df.columns}
    ticker_col = next((normalized_cols[k] for k in ["ticker", "symbol", "stock", "aktie"] if k in normalized_cols), None)
    rating_col = next(
        (
            normalized_cols[k]
            for k in [
                "percentile",
                "rs_rating",
                "relative_strength_rating",
                "rs_rank",
                "rsrank",
                "rating",
                "relative_strength",
                "relative_strength_value",
                "rs",
            ]
            if k in normalized_cols
        ),
        None,
    )
    score_col = next((normalized_cols[k] for k in ["score", "rs_score", "weighted_score"] if k in normalized_cols), None)
    as_of_col = next((normalized_cols[k] for k in ["as_of_date", "as_of", "date", "stand"] if k in normalized_cols), None)
    generated_at_col = next((normalized_cols[k] for k in ["generated_at_utc", "generated_at", "updated_at"] if k in normalized_cols), None)

    if ticker_col is None or rating_col is None:
        payload["error"] = "Ticker- oder RS-Spalte in CSV nicht gefunden."
        return payload

    rows = df[[ticker_col, rating_col] + ([score_col] if score_col else [])].copy()
    rows[ticker_col] = rows[ticker_col].astype(str).str.upper().str.strip()
    rows[rating_col] = pd.to_numeric(rows[rating_col], errors="coerce")
    if score_col:
        rows[score_col] = pd.to_numeric(rows[score_col], errors="coerce")
    rows = rows.dropna(subset=[ticker_col, rating_col])
    rows = rows[rows[ticker_col] != ""]
    rows[rating_col] = rows[rating_col].clip(lower=1, upper=99).round().astype(int)
    rows = rows.drop_duplicates(subset=[ticker_col], keep="last")

    ratings = {str(r[ticker_col]).strip().upper(): int(r[rating_col]) for _, r in rows.iterrows()}
    scores = {}
    if score_col:
        score_rows = rows.dropna(subset=[score_col])
        scores = {str(r[ticker_col]).strip().upper(): float(r[score_col]) for _, r in score_rows.iterrows()}

    if as_of_col and as_of_col in df.columns:
        as_of_values = df[as_of_col].dropna().astype(str).str.strip()
        if not as_of_values.empty:
            payload["as_of_date"] = str(as_of_values.iloc[-1])
    if generated_at_col and generated_at_col in df.columns:
        generated_values = df[generated_at_col].dropna().astype(str).str.strip()
        if not generated_values.empty:
            payload["generated_at_utc"] = str(generated_values.iloc[-1])

    payload.update(
        {
            "ok": bool(ratings),
            "value_column": str(rating_col),
            "score_column": str(score_col or ""),
            "count": int(len(ratings)),
            "ratings": ratings,
            "scores": scores,
            "error": "" if ratings else "CSV enthielt keine auswertbaren RS-Werte.",
        }
    )
    return payload


@st.cache_data(ttl=1800, show_spinner=False)
def load_external_rs_ratings_map():
    output_path = _get_rs_output_path()
    payload = {
        "ok": False,
        "source": "repo_csv",
        "file": output_path.name,
        "path": str(output_path),
        "value_column": "",
        "score_column": "",
        "count": 0,
        "ratings": {},
        "scores": {},
        "as_of_date": "",
        "generated_at_utc": "",
        "error": "",
    }
    if not output_path.exists():
        payload["error"] = f"RS-CSV nicht gefunden: {output_path.name}"
        return payload
    try:
        df = pd.read_csv(output_path)
        return _parse_rs_ratings_frame(df, source_name="repo_csv", file_name=output_path.name, path_name=str(output_path))
    except Exception as exc:
        logger.debug("load_external_rs_ratings_map failed: %s", exc)
        payload["error"] = f"RS-CSV konnte nicht geladen werden ({exc})."
        return payload


@st.cache_data(ttl=1800, show_spinner=False)
def load_fred_rs_ratings_map():
    payload = {
        "ok": False,
        "source": "fred_csv",
        "file": "rs_stocks.csv",
        "path": FRED_RS_CSV_URL,
        "value_column": "",
        "score_column": "",
        "count": 0,
        "ratings": {},
        "scores": {},
        "as_of_date": "",
        "generated_at_utc": "",
        "error": "",
    }
    try:
        response = requests.get(FRED_RS_CSV_URL, headers={"User-Agent": "boerse-dashboard"}, timeout=20)
        response.raise_for_status()
        df = pd.read_csv(io.StringIO(response.text))
        return _parse_rs_ratings_frame(
            df,
            source_name="fred_csv",
            file_name="rs_stocks.csv",
            path_name=FRED_RS_CSV_URL,
        )
    except Exception as exc:
        logger.debug("load_fred_rs_ratings_map failed: %s", exc)
        payload["error"] = f"Fred-RS-CSV konnte nicht geladen werden ({exc})."
        return payload


def _load_selected_rs_ratings_map(source: str | None = None):
    selected = str(source or _get_rs_rating_source_setting() or RS_SOURCE_CSV_LATEST).strip().lower()
    if selected == RS_SOURCE_COMPUTED:
        return {
            "ok": False,
            "source": RS_SOURCE_COMPUTED,
            "file": "",
            "path": "",
            "value_column": "",
            "score_column": "",
            "count": 0,
            "ratings": {},
            "scores": {},
            "as_of_date": "",
            "generated_at_utc": "",
            "error": "",
        }
    if selected == RS_SOURCE_FRED_CSV:
        return load_fred_rs_ratings_map()
    return load_external_rs_ratings_map()


def _apply_rs_source_override(ticker: str, rs_ctx: dict | None):
    source = _get_rs_rating_source_setting()
    if source == RS_SOURCE_COMPUTED:
        return rs_ctx, None

    external = _load_selected_rs_ratings_map(source)
    note = {
        "source": source,
        "source_label": RS_SOURCE_LABELS.get(source, source),
        "ok": bool(external.get("ok")),
        "file": str(external.get("file", "")),
        "path": str(external.get("path", "")),
        "value_column": str(external.get("value_column", "")),
        "score_column": str(external.get("score_column", "")),
        "count": int(external.get("count", 0) or 0),
        "as_of_date": str(external.get("as_of_date", "")),
        "generated_at_utc": str(external.get("generated_at_utc", "")),
        "error": str(external.get("error", "") or "").strip(),
        "matched": False,
    }

    base_ctx = dict(rs_ctx or {})
    ratings = external.get("ratings", {}) if isinstance(external, dict) else {}
    scores = external.get("scores", {}) if isinstance(external, dict) else {}
    value = ratings.get(str(ticker or "").strip().upper()) if isinstance(ratings, dict) else None
    score_value = scores.get(str(ticker or "").strip().upper()) if isinstance(scores, dict) else None
    if value is not None:
        base_ctx["rating"] = int(np.clip(value, 1, 99))
        if score_value is not None:
            base_ctx["score"] = float(score_value)
        base_ctx["method"] = "external_csv"
        note["matched"] = True
    return base_ctx, note


def _refresh_relative_strength_snapshot_from_closes(store, close_frame, *, source="refresh", lookback_days=400):
    if close_frame is None or close_frame.empty:
        return None
    spx_df = load_sp500_for_rs(lookback_days=lookback_days)
    if spx_df is None or "Close" not in spx_df or spx_df["Close"].dropna().empty:
        return None
    benchmark_close = _coerce_daily_series(spx_df["Close"])
    closes = close_frame.copy()
    closes.index = pd.to_datetime(closes.index, errors="coerce")
    closes = closes[~closes.index.isna()]
    closes = closes.sort_index().loc[:, ~closes.columns.duplicated()]
    closes = closes.apply(pd.to_numeric, errors="coerce")
    common = closes.index.intersection(benchmark_close.index)
    if len(common) < 120:
        return None
    closes = closes.reindex(common)
    benchmark_close = benchmark_close.reindex(common)
    ratio_frame = closes.div(benchmark_close, axis=0).replace([np.inf, -np.inf], np.nan)
    min_obs = min(220, max(120, len(ratio_frame) // 2))
    ratio_frame = ratio_frame.loc[:, ratio_frame.notna().sum() >= min_obs]
    scores = _weighted_rs_scores_for_frame(ratio_frame)
    if scores is None or scores.empty:
        return None
    _store_relative_strength_snapshot(store, scores, source=source)
    return scores


def export_relative_strength_csv_for_github(lookback_days=400, batch_size=220, retry_batch_size=48):
    end = datetime.now()
    start = end - timedelta(days=lookback_days)
    tickers = [
        _normalize_symbol(t)
        for t in get_app_stock_universe_tickers()
        if str(t or "").strip()
    ]
    tickers = list(dict.fromkeys(tickers))
    if not tickers:
        return {"ok": False, "error": "Kein Aktienuniversum für den RS-Export verfügbar."}

    close_frames = []
    loaded_symbols = set()
    batch_count = 0

    for batch in _chunked(tickers, batch_size):
        bundle = _download_ohlc_batch_fast(batch, start, end, threads=True, timeout=40)
        close_frame = bundle.get("close") if bundle else None
        batch_count += 1
        if close_frame is None or close_frame.empty:
            continue
        close_frame = close_frame.sort_index().apply(pd.to_numeric, errors="coerce")
        close_frame = close_frame.loc[:, ~close_frame.columns.duplicated()]
        close_frames.append(close_frame)
        loaded_symbols.update(str(col).strip().upper() for col in close_frame.columns)

    missing_after_first = [ticker for ticker in tickers if ticker not in loaded_symbols]
    retry_batches = 0
    if missing_after_first:
        for batch in _chunked(missing_after_first, retry_batch_size):
            bundle = _download_ohlc_batch_fast(batch, start, end, threads=False, timeout=35)
            close_frame = bundle.get("close") if bundle else None
            retry_batches += 1
            if close_frame is None or close_frame.empty:
                continue
            close_frame = close_frame.sort_index().apply(pd.to_numeric, errors="coerce")
            close_frame = close_frame.loc[:, ~close_frame.columns.duplicated()]
            close_frames.append(close_frame)
            loaded_symbols.update(str(col).strip().upper() for col in close_frame.columns)

    if not close_frames:
        return {"ok": False, "error": "Yahoo lieferte keine Kursdaten für den RS-Export.", "requested": len(tickers)}

    closes = pd.concat(close_frames, axis=1)
    closes.index = pd.to_datetime(closes.index, errors="coerce")
    closes = closes[~closes.index.isna()]
    closes = closes.sort_index()
    closes = closes.loc[:, ~closes.columns.duplicated(keep="last")]
    closes = closes.apply(pd.to_numeric, errors="coerce")

    benchmark = load_sp500_for_rs(lookback_days=lookback_days)
    if benchmark is None or "Close" not in benchmark or benchmark["Close"].dropna().empty:
        return {"ok": False, "error": "Benchmark ^GSPC konnte für den RS-Export nicht geladen werden."}

    benchmark_close = _coerce_daily_series(benchmark["Close"])
    common = closes.index.intersection(benchmark_close.index)
    if len(common) < 120:
        return {"ok": False, "error": "Zu wenige gemeinsame Handelstage für den RS-Export.", "common_days": int(len(common))}

    closes = closes.reindex(common)
    benchmark_close = benchmark_close.reindex(common)
    ratio_frame = closes.div(benchmark_close, axis=0).replace([np.inf, -np.inf], np.nan)
    min_obs = min(220, max(120, len(ratio_frame) // 2))
    ratio_frame = ratio_frame.loc[:, ratio_frame.notna().sum() >= min_obs]
    scores = _weighted_rs_scores_for_frame(ratio_frame)
    ratings = _scores_to_rs_ratings(scores)
    if ratings.empty:
        return {"ok": False, "error": "Es konnten keine RS-Ratings aus den Kursdaten berechnet werden."}

    as_of_date = pd.Timestamp(common[-1]).strftime("%Y-%m-%d")
    generated_at = _utc_now_str()
    export_df = pd.DataFrame(
        {
            "ticker": ratings.index.astype(str),
            "rating": ratings.astype(int).values,
            "score": pd.to_numeric(scores.reindex(ratings.index), errors="coerce").round(6).values,
            "as_of_date": as_of_date,
            "generated_at_utc": generated_at,
            "universe": CACHE_UNIVERSE_NAME,
            "source": "github_actions_yahoo",
        }
    ).sort_values(["rating", "score", "ticker"], ascending=[False, False, True], kind="mergesort")

    output_path = _get_rs_output_path()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    export_df.to_csv(output_path, index=False)

    # @st.cache_data(ttl=1800) auf load_external_rs_ratings_map() würde sonst
    # bis zu 30 Minuten lang die alten RS-Werte zurückgeben.
    try:
        load_external_rs_ratings_map.clear()
    except Exception:
        pass

    return {
        "ok": True,
        "requested": len(tickers),
        "loaded": int(len(loaded_symbols)),
        "coverage": float(len(loaded_symbols) / max(len(tickers), 1)),
        "score_count": int(len(ratings)),
        "batch_count": int(batch_count),
        "retry_batches": int(retry_batches),
        "output_file": str(output_path),
        "as_of_date": as_of_date,
        "generated_at_utc": generated_at,
        "message": f"RS-CSV erzeugt: {len(ratings)} Ratings, Stand {as_of_date}.",
    }


def _refresh_breadth_snapshot(store, tickers, start_date, end_date, *, source="refresh"):
    bundle = _prepare_component_bundle(_read_cached_price_bundle(store, tickers, start_date, end_date))
    close_frame = bundle.get("close") if bundle else None
    if close_frame is None or close_frame.empty:
        return None

    requested = len(tickers)
    loaded = int(close_frame.shape[1])
    coverage = loaded / max(requested, 1)
    bundle["attrs"] = {
        "requested_universe": requested,
        "loaded_universe": loaded,
        "coverage_ratio": coverage,
        "partial_universe": coverage < 0.75,
        "cache_used": True,
        "cache_only_run": True,
        "store_backend": store["backend"],
        "store_label": _get_store_label(store),
        "cache_member_count": _safe_int(_get_cache_metadata(store, f"{CACHE_UNIVERSE_NAME}_member_count", requested), requested),
        "cache_members_updated_at": _get_cache_metadata(store, f"{CACHE_UNIVERSE_NAME}_members_updated_at", ""),
        "cache_prices_last_write_at": _get_cache_metadata(store, "prices_last_write_at", ""),
        "last_refresh_at": _get_cache_metadata(store, "last_refresh_at", ""),
    }
    breadth = compute_breadth_from_components(bundle)
    if breadth is None or breadth.empty:
        return None
    generated_at = _store_breadth_snapshot(store, breadth, source=source)
    _refresh_relative_strength_snapshot_from_closes(store, close_frame, source=source)
    breadth.attrs["snapshot_generated_at"] = generated_at
    breadth.attrs["snapshot_source"] = source
    return breadth


def _github_actions_config():
    owner = str(_safe_get_secret("github_actions", "owner", default=os.environ.get("GITHUB_ACTIONS_OWNER", "")) or "").strip()
    repo = str(_safe_get_secret("github_actions", "repo", default=os.environ.get("GITHUB_ACTIONS_REPO", "")) or "").strip()
    repository = str(_safe_get_secret("github_actions", "repository", default=os.environ.get("GITHUB_ACTIONS_REPOSITORY", "")) or "").strip()
    if repository and "/" in repository and (not owner or not repo):
        owner, repo = repository.split("/", 1)
    if repo and "/" in repo and not owner:
        owner, repo = repo.split("/", 1)
    workflow = str(_safe_get_secret("github_actions", "workflow", default=os.environ.get("GITHUB_ACTIONS_WORKFLOW", "market-refresh.yml")) or "market-refresh.yml").strip()
    ref = str(_safe_get_secret("github_actions", "ref", default=os.environ.get("GITHUB_ACTIONS_REF", "main")) or "main").strip()
    token = str(_safe_get_secret("github_actions", "token", default=os.environ.get("GITHUB_ACTIONS_TOKEN", "")) or "").strip()
    if not token:
        token = str(_safe_get_secret("github_actions", "dispatch_token", default=os.environ.get("GITHUB_ACTIONS_DISPATCH_TOKEN", "")) or "").strip()
    api_url = f"https://api.github.com/repos/{owner}/{repo}/actions/workflows/{workflow}/dispatches" if owner and repo and workflow else ""
    actions_url = f"https://github.com/{owner}/{repo}/actions/workflows/{workflow}" if owner and repo and workflow else ""
    return {
        "owner": owner,
        "repo": repo,
        "workflow": workflow,
        "ref": ref,
        "token": token,
        "api_url": api_url,
        "actions_url": actions_url,
        "ready": bool(owner and repo and workflow and token),
    }


def _job_row_to_dict(columns, row):
    if not row:
        return None
    data = dict(zip(columns, row))
    for raw_key, parsed_key in (("payload_json", "payload"), ("result_json", "result")):
        raw = data.get(raw_key)
        if raw in (None, ""):
            data[parsed_key] = None
        else:
            try:
                data[parsed_key] = json.loads(raw)
            except Exception:
                data[parsed_key] = None
    return data


def _create_refresh_job(store, job_type, requested_by="streamlit", payload=None, trigger_mode="github_actions"):
    job_id = f"job-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}"
    now = _utc_now_str()
    payload_json = json.dumps(payload or {}, ensure_ascii=False)
    conn = _get_cache_conn(store)
    try:
        if store.get("backend") == "neon" and not isinstance(conn, sqlite3.Connection):
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO refresh_jobs(
                        job_id, job_type, status, progress, current_step, message,
                        requested_by, trigger_mode, trigger_status, payload_json,
                        requested_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                    """,
                    (job_id, job_type, "queued", 0, "Wartet auf GitHub Actions", "Job angelegt", requested_by, trigger_mode, "pending", payload_json),
                )
        else:
            conn.execute(
                """
                INSERT INTO refresh_jobs(
                    job_id, job_type, status, progress, current_step, message,
                    requested_by, trigger_mode, trigger_status, payload_json,
                    requested_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (job_id, job_type, "queued", 0, "Wartet auf GitHub Actions", "Job angelegt", requested_by, trigger_mode, "pending", payload_json, now, now),
            )
        conn.commit()
    finally:
        conn.close()
    return _get_refresh_job(store, job_id)


def _update_refresh_job(store, job_id, **fields):
    allowed = {
        "status", "progress", "current_step", "message", "requested_by", "trigger_mode", "trigger_status",
        "trigger_response", "payload_json", "result_json", "runner_source", "run_url", "requested_at",
        "started_at", "finished_at", "updated_at",
    }
    cleaned = {}
    for key, value in fields.items():
        if key not in allowed:
            continue
        if key in {"payload_json", "result_json"} and value is not None and not isinstance(value, str):
            value = json.dumps(value, ensure_ascii=False)
        cleaned[key] = value
    cleaned.setdefault("updated_at", _utc_now_str())
    if not cleaned:
        return _get_refresh_job(store, job_id)
    conn = _get_cache_conn(store)
    try:
        if store.get("backend") == "neon" and not isinstance(conn, sqlite3.Connection):
            assignments = []
            values = []
            for key, value in cleaned.items():
                if key == "updated_at":
                    assignments.append("updated_at = NOW()")
                else:
                    assignments.append(f"{key} = %s")
                    values.append(value)
            values.append(job_id)
            sql = f"UPDATE refresh_jobs SET {', '.join(assignments)} WHERE job_id = %s"
            with conn.cursor() as cur:
                cur.execute(sql, tuple(values))
        else:
            assignments = []
            values = []
            for key, value in cleaned.items():
                assignments.append(f"{key} = ?")
                values.append(value)
            values.append(job_id)
            sql = f"UPDATE refresh_jobs SET {', '.join(assignments)} WHERE job_id = ?"
            conn.execute(sql, tuple(values))
        conn.commit()
    finally:
        conn.close()
    return _get_refresh_job(store, job_id)


def _get_refresh_job(store, job_id):
    conn = _get_cache_conn(store)
    try:
        if _is_neon_conn(conn):
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM refresh_jobs WHERE job_id=%s", (job_id,))
                row = cur.fetchone()
                cols = [desc[0] for desc in cur.description] if cur.description else []
        else:
            cur = conn.execute("SELECT * FROM refresh_jobs WHERE job_id=?", (job_id,))
            row = cur.fetchone()
            cols = [desc[0] for desc in cur.description] if cur.description else []
        return _job_row_to_dict(cols, row)
    finally:
        conn.close()


def _list_recent_refresh_jobs(store, limit=8):
    conn = _get_cache_conn(store)
    try:
        if _is_neon_conn(conn):
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM refresh_jobs ORDER BY requested_at DESC LIMIT %s", (limit,))
                rows = cur.fetchall()
                cols = [desc[0] for desc in cur.description] if cur.description else []
        else:
            cur = conn.execute("SELECT * FROM refresh_jobs ORDER BY requested_at DESC LIMIT ?", (limit,))
            rows = cur.fetchall()
            cols = [desc[0] for desc in cur.description] if cur.description else []
        return [_job_row_to_dict(cols, row) for row in rows]
    finally:
        conn.close()


def _get_active_refresh_job(store):
    conn = _get_cache_conn(store)
    try:
        # Statuses dynamisch mit Platzhaltern bauen, damit das Hinzufügen eines
        # weiteren Status (z. B. "skipped") nicht zu einem schwer auffindbaren
        # Runtime-Fehler durch falsche Platzhalter-Anzahl führt.
        statuses = ("queued", "running")
        if _is_neon_conn(conn):
            placeholders = ", ".join(["%s"] * len(statuses))
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT * FROM refresh_jobs WHERE status IN ({placeholders}) ORDER BY requested_at DESC LIMIT 1",
                    statuses,
                )
                row = cur.fetchone()
                cols = [desc[0] for desc in cur.description] if cur.description else []
        else:
            placeholders = ", ".join(["?"] * len(statuses))
            cur = conn.execute(
                f"SELECT * FROM refresh_jobs WHERE status IN ({placeholders}) ORDER BY requested_at DESC LIMIT 1",
                statuses,
            )
            row = cur.fetchone()
            cols = [desc[0] for desc in cur.description] if cur.description else []
        return _job_row_to_dict(cols, row)
    finally:
        conn.close()


def _job_type_label(job_type):
    return {
        "refresh_universe": "Aktienuniversum aktualisieren",
        "rescue_missing": "Fehlende nachladen",
        "auto_remap": "Automatisch remappen",
        "export_rs_csv": "RS-CSV in GitHub erzeugen",
        "position_atr_monitor": "ATR-Positionsmonitor",
        "pushover_test": "Pushover-Test",
    }.get(str(job_type or ""), str(job_type or "Unbekannt"))


def _trigger_github_actions_workflow(job_id, job_type, extra_inputs=None):
    cfg = _github_actions_config()
    if not cfg.get("ready"):
        return {"ok": False, "error": "GitHub Actions ist nicht vollständig konfiguriert.", "config": cfg}

    extra_inputs = extra_inputs if isinstance(extra_inputs, dict) else {}
    store = _get_price_store()
    neon_auto_enabled = _is_neon_auto_update_enabled(store) if store.get("backend") == "neon" else True
    trigger_name = str(extra_inputs.get("trigger", "") or "").strip().lower()
    is_automatic_trigger = trigger_name.startswith("auto") or trigger_name.startswith("schedule")
    if store.get("backend") == "neon" and (not neon_auto_enabled) and is_automatic_trigger:
        return {"ok": False, "error": "Neon Auto-Update ist deaktiviert. Automatischer GitHub-Job wurde nicht gestartet.", "actions_url": cfg.get("actions_url", "")}

    inputs = {
        "job_id": str(job_id),
        "job_type": str(job_type),
        "neon_auto_update_enabled": "1" if neon_auto_enabled else "0",
    }
    for key, value in extra_inputs.items():
        inputs[str(key)] = "" if value is None else str(value)
    payload = {"ref": cfg["ref"], "inputs": inputs}
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {cfg['token']}",
    }
    try:
        response = requests.post(cfg["api_url"], headers=headers, json=payload, timeout=20)
        ok = response.status_code in (200, 201, 202, 204)
        body = (response.text or "").strip()
        return {
            "ok": ok,
            "status_code": response.status_code,
            "body": body[:1000],
            "actions_url": cfg.get("actions_url", ""),
        }
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}", "actions_url": cfg.get("actions_url", "")}


def _request_external_refresh_job(job_type, requested_by="streamlit", payload=None):
    store = _get_price_store()
    _init_price_cache_db(store)
    active = _get_active_refresh_job(store)
    if active:
        return {"ok": False, "error": f"Es läuft bereits ein Job: {_job_type_label(active.get('job_type'))}", "job": active}
    job = _create_refresh_job(store, job_type=job_type, requested_by=requested_by, payload=payload or {})
    dispatch = _trigger_github_actions_workflow(job["job_id"], job_type, extra_inputs={"requested_by": requested_by})
    if dispatch.get("ok"):
        job = _update_refresh_job(
            store,
            job["job_id"],
            trigger_status="sent",
            current_step="In GitHub-Warteschlange",
            message="Workflow ausgelöst. Die App aktualisiert den Status beim nächsten Laden.",
            run_url=dispatch.get("actions_url", ""),
        )
        return {"ok": True, "job": job, "dispatch": dispatch}
    job = _update_refresh_job(
        store,
        job["job_id"],
        status="failed",
        trigger_status="failed",
        current_step="Dispatch fehlgeschlagen",
        message=dispatch.get("error") or dispatch.get("body") or "Workflow konnte nicht ausgelöst werden.",
        run_url=dispatch.get("actions_url", ""),
        finished_at=_utc_now_str(),
    )
    return {"ok": False, "job": job, "dispatch": dispatch, "error": job.get("message")}


def _maybe_request_external_refresh_job(store, job_type="refresh_universe", *, reason="auto", payload=None, cooldown_minutes=30):
    if not isinstance(store, dict) or store.get("backend") == "sqlite":
        return {"triggered": False, "reason": "not_external"}

    active = _get_active_refresh_job(store)
    if active:
        return {"triggered": False, "reason": "already_running", "job": active}

    clean_reason = re.sub(r"[^a-zA-Z0-9_]+", "_", str(reason or "auto")).strip("_") or "auto"
    last_run_key = f"external_auto_refresh_last_{clean_reason}"
    now_utc = datetime.now(timezone.utc)
    last_run = st.session_state.get(last_run_key)
    if isinstance(last_run, datetime) and (now_utc - last_run) < timedelta(minutes=cooldown_minutes):
        return {"triggered": False, "reason": "cooldown"}

    st.session_state[last_run_key] = now_utc
    result = _request_external_refresh_job(
        job_type,
        requested_by=f"streamlit_{clean_reason}",
        payload=payload or {"trigger": clean_reason},
    )
    return {
        "triggered": True,
        "ok": bool(result.get("ok")),
        "result": result,
        "job": result.get("job"),
        "error": result.get("error"),
    }


def _job_status_badge(status):
    s = str(status or "").lower()
    if s == "done":
        return "✅ Fertig"
    if s == "failed":
        return "❌ Fehlgeschlagen"
    if s == "running":
        return "🏃 Läuft"
    if s == "queued":
        return "🕒 Warteschlange"
    return s or "—"

def _store_universe_members(store, universe, tickers):
    tickers = list(dict.fromkeys([str(t).strip().upper() for t in tickers if t]))
    if not tickers:
        return
    try:
        existing = _load_cached_universe_members(store, universe)
    except Exception:
        existing = []
    if existing and set(existing) == set(tickers):
        _set_cache_metadata(store, f"{universe}_member_count", len(tickers))
        return
    conn = _get_cache_conn(store)
    try:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        if _is_neon_conn(conn):
            with conn.cursor() as cur:
                cur.execute("DELETE FROM universe_members WHERE universe=%s", (universe,))
                rows = [(universe, t, now) for t in tickers]
                execute_values(
                    cur,
                    """
                    INSERT INTO universe_members(universe, symbol, updated_at)
                    VALUES %s
                    ON CONFLICT (universe, symbol) DO UPDATE SET updated_at=EXCLUDED.updated_at
                    """,
                    rows,
                    page_size=1000,
                )
        else:
            conn.execute("DELETE FROM universe_members WHERE universe=?", (universe,))
            conn.executemany(
                "INSERT OR REPLACE INTO universe_members(universe, symbol, updated_at) VALUES (?, ?, ?)",
                [(universe, t, now) for t in tickers],
            )
        conn.commit()
    finally:
        conn.close()
    _set_cache_metadata(store, f"{universe}_member_count", len(tickers))
    _set_cache_metadata(store, f"{universe}_members_updated_at", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"))

def _load_cached_universe_members(store, universe):
    conn = _get_cache_conn(store)
    try:
        if _is_neon_conn(conn):
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT symbol FROM universe_members WHERE universe=%s ORDER BY symbol",
                    (universe,),
                )
                rows = cur.fetchall()
                return [r[0] for r in rows]
        rows = conn.execute(
            "SELECT symbol FROM universe_members WHERE universe=? ORDER BY symbol",
            (universe,),
        ).fetchall()
        return [r[0] for r in rows]
    finally:
        conn.close()

def _normalize_symbol(symbol):
    return str(symbol).strip().upper().replace("/", "-").replace(".", "-")

def _upsert_symbol_mapping(store, universe, source_symbol, yahoo_symbol=None, status="unknown", note=""):
    source_symbol = _normalize_symbol(source_symbol)
    yahoo_symbol = str(yahoo_symbol).strip().upper() if yahoo_symbol else None
    note = str(note or "")
    conn = _get_cache_conn(store)
    try:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        if _is_neon_conn(conn):
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO symbol_mappings(universe, source_symbol, yahoo_symbol, status, note, updated_at)
                    VALUES (%s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (universe, source_symbol)
                    DO UPDATE SET yahoo_symbol=EXCLUDED.yahoo_symbol,
                                  status=EXCLUDED.status,
                                  note=EXCLUDED.note,
                                  updated_at=NOW()
                    """,
                    (universe, source_symbol, yahoo_symbol, status, note),
                )
        else:
            conn.execute(
                """
                INSERT INTO symbol_mappings(universe, source_symbol, yahoo_symbol, status, note, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(universe, source_symbol)
                DO UPDATE SET yahoo_symbol=excluded.yahoo_symbol,
                              status=excluded.status,
                              note=excluded.note,
                              updated_at=excluded.updated_at
                """,
                (universe, source_symbol, yahoo_symbol, status, note, now),
            )
        conn.commit()
    finally:
        conn.close()

def _load_symbol_mappings(store, universe, status_filter=("mapped",)):
    conn = _get_cache_conn(store)
    try:
        is_neon = _is_neon_conn(conn)
        params = [universe]
        status_sql = ""
        if status_filter:
            if is_neon:
                status_sql = " AND status = ANY(%s)"
                params.append(list(status_filter))
            else:
                placeholders = ",".join(["?"] * len(status_filter))
                status_sql = f" AND status IN ({placeholders})"
                params.extend(list(status_filter))
        query = (
            "SELECT source_symbol, yahoo_symbol FROM symbol_mappings "
            "WHERE universe=" + ("%s" if is_neon else "?") +
            " AND yahoo_symbol IS NOT NULL AND TRIM(yahoo_symbol) <> ''" + status_sql
        )
        if is_neon:
            with conn.cursor() as cur:
                cur.execute(query, tuple(params))
                rows = cur.fetchall()
        else:
            rows = conn.execute(query, tuple(params)).fetchall()
        return {_normalize_symbol(src): str(yahoo).strip().upper() for src, yahoo in rows if src and yahoo}
    finally:
        conn.close()

def _chunked(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]

def _extract_price_field_frame(df, requested_batch, field_name):
    if df is None or len(df) == 0:
        return None
    if isinstance(df.columns, pd.MultiIndex):
        if field_name not in df.columns.get_level_values(0):
            return None
        frame = df[field_name].copy()
    else:
        if field_name not in df.columns:
            return None
        frame = df[[field_name]].copy()
        if len(requested_batch) == 1:
            frame.columns = requested_batch
    frame = frame.apply(pd.to_numeric, errors="coerce")
    frame.index = pd.to_datetime(frame.index)
    frame = frame.sort_index()
    frame.columns = [str(c).strip().upper().replace(".", "-") for c in frame.columns]
    frame = frame.loc[:, ~frame.columns.duplicated()]
    frame = frame.dropna(axis=1, how="all")
    return frame if frame.shape[1] else None

def _extract_ohlc_bundle(df, requested_batch):
    return {
        "close": _extract_price_field_frame(df, requested_batch, "Close"),
        "high": _extract_price_field_frame(df, requested_batch, "High"),
        "low": _extract_price_field_frame(df, requested_batch, "Low"),
    }

def _download_ohlc_batch_fast(batch, start, end, threads=True, timeout=30):
    """Bulk download helper returning Close/High/Low frames for breadth storage."""
    batch = [str(b).strip().upper().replace('/', '-') for b in list(dict.fromkeys(batch)) if b]
    if not batch:
        return None
    try:
        df = yf.download(
            batch,
            start=start,
            end=end,
            progress=False,
            auto_adjust=True,
            threads=threads,
            group_by="column",
            timeout=timeout,
        )
        bundle = _extract_ohlc_bundle(df, batch)
        return bundle if any(v is not None and not v.empty for v in bundle.values()) else None
    except Exception:
        return None

def _download_close_batch_fast(batch, start, end, threads=True, timeout=30):
    bundle = _download_ohlc_batch_fast(batch, start, end, threads=threads, timeout=timeout)
    if not bundle:
        return None
    return bundle.get("close")

def _rename_ohlc_bundle(bundle, rename_map):
    if not bundle:
        return None
    out = {}
    for key, frame in bundle.items():
        if frame is None or frame.empty:
            out[key] = None
            continue
        renamed = frame.rename(columns=rename_map)
        renamed = renamed.loc[:, ~renamed.columns.duplicated()]
        renamed = renamed.dropna(axis=1, how="all")
        out[key] = renamed if renamed.shape[1] else None
    return out if any(v is not None and not v.empty for v in out.values()) else None

def _bundle_loaded_symbols(bundle):
    if not bundle:
        return []
    for key in ("close", "high", "low"):
        frame = bundle.get(key)
        if frame is not None and not frame.empty:
            return [str(c).strip().upper() for c in frame.columns]
    return []

def _download_ohlc_batch_mapped(symbols, symbol_map, start, end, threads=True, timeout=30):
    symbols = [_normalize_symbol(s) for s in list(dict.fromkeys(symbols)) if s]
    if not symbols:
        return None, []

    query_to_original = {}
    query_symbols = []
    collision_symbols = []
    for original in symbols:
        query_symbol = str(symbol_map.get(original, original)).strip().upper().replace('/', '-')
        query_norm = _normalize_symbol(query_symbol)
        if query_norm in query_to_original and query_to_original[query_norm] != original:
            collision_symbols.append(original)
            continue
        query_to_original[query_norm] = original
        query_symbols.append(query_symbol)

    bundle = _download_ohlc_batch_fast(query_symbols, start, end, threads=threads, timeout=timeout)
    if not bundle:
        return None, collision_symbols

    rename_map = {col: query_to_original.get(_normalize_symbol(col), col) for col in _bundle_loaded_symbols(bundle)}
    bundle = _rename_ohlc_bundle(bundle, rename_map)
    return bundle, collision_symbols

def _download_close_batch_mapped(symbols, symbol_map, start, end, threads=True, timeout=30):
    bundle, collision_symbols = _download_ohlc_batch_mapped(symbols, symbol_map, start, end, threads=threads, timeout=timeout)
    if not bundle:
        return None, collision_symbols
    closes = bundle.get("close")
    return (closes if closes is not None and not closes.empty else None), collision_symbols

def _download_single_ticker_ohlc(symbol, start, end, timeout=30):
    """Single-symbol fallback download for hard-to-fetch tickers."""
    symbol = str(symbol).strip().upper().replace('/', '-')
    if not symbol:
        return None
    try:
        df = yf.download(
            symbol,
            start=start,
            end=end,
            progress=False,
            auto_adjust=True,
            threads=False,
            group_by="column",
            timeout=timeout,
        )
        bundle = _extract_ohlc_bundle(df, [symbol])
        return bundle if any(v is not None and not v.empty for v in bundle.values()) else None
    except Exception:
        return None

def _download_single_ticker_close(symbol, start, end, timeout=30):
    bundle = _download_single_ticker_ohlc(symbol, start, end, timeout=timeout)
    if not bundle:
        return None
    return bundle.get("close")

def _download_single_source_symbol(source_symbol, symbol_map, start, end, timeout=30):
    source_symbol = _normalize_symbol(source_symbol)
    query_symbol = str(symbol_map.get(source_symbol, source_symbol)).strip().upper().replace('/', '-')
    bundle = _download_single_ticker_ohlc(query_symbol, start, end, timeout=timeout)
    if not bundle:
        return None
    rename_map = {}
    for symbol in _bundle_loaded_symbols(bundle):
        rename_map[symbol] = source_symbol
    bundle = _rename_ohlc_bundle(bundle, rename_map)
    return bundle

def _search_yahoo_symbol_candidates(symbol, timeout=15):
    """Query Yahoo's public search endpoint to see whether a symbol is known there at all."""
    symbol = str(symbol).strip().upper()
    if not symbol:
        return {"lookup_ok": False, "exact_match": False, "candidates": []}
    url = "https://query2.finance.yahoo.com/v1/finance/search"
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json,text/plain,*/*"}
    params = {"q": symbol, "quotesCount": 10, "newsCount": 0, "listsCount": 0}
    try:
        resp = requests.get(url, params=params, timeout=timeout, headers=headers)
        resp.raise_for_status()
        payload = resp.json()
        quotes = payload.get("quotes", []) or []
        candidates = []
        for q in quotes:
            cand = str(q.get("symbol", "")).strip().upper()
            if cand:
                candidates.append(cand)
        candidates = list(dict.fromkeys(candidates))
        variants = {symbol, symbol.replace("-", "."), symbol.replace(".", "-")}
        exact_match = any(c in variants for c in candidates)
        return {"lookup_ok": len(candidates) > 0, "exact_match": exact_match, "candidates": candidates[:8]}
    except Exception:
        return {"lookup_ok": False, "exact_match": False, "candidates": []}

def _probe_single_missing_symbol(symbol, start, end, timeout=25):
    """Diagnose whether Yahoo knows a symbol and whether history can actually be loaded."""
    symbol = str(symbol).strip().upper().replace("/", "-")
    variants = [symbol]
    dotted = symbol.replace("-", ".")
    if dotted != symbol:
        variants.append(dotted)
    dashed = symbol.replace(".", "-")
    if dashed not in variants:
        variants.append(dashed)

    lookup = _search_yahoo_symbol_candidates(symbol, timeout=max(10, timeout))
    history_variant = ""
    history_rows = 0

    for variant in variants:
        closes = _download_single_ticker_close(variant, start, end, timeout=timeout)
        if closes is not None and not closes.empty:
            history_variant = variant
            history_rows = int(len(closes))
            break

    history_ok = history_rows > 0
    if history_ok:
        status = "Historie vorhanden"
    elif lookup["lookup_ok"]:
        status = "Yahoo kennt Symbol, aber keine Historie"
    else:
        status = "Yahoo kennt Symbol nicht"

    return {
        "symbol": symbol,
        "lookup_ok": bool(lookup["lookup_ok"]),
        "lookup_exact": bool(lookup["exact_match"]),
        "lookup_candidates": ", ".join(lookup["candidates"][:5]),
        "history_ok": history_ok,
        "history_variant": history_variant,
        "history_rows": history_rows,
        "status": status,
    }

def _build_symbol_test_candidates(symbol, lookup_candidates=None, max_candidates=8):
    seen = set()
    out = []

    def add(val):
        val = str(val).strip().upper()
        if not val:
            return
        if val not in seen:
            seen.add(val)
            out.append(val)

    symbol = str(symbol).strip().upper().replace('/', '-')
    add(symbol)
    add(symbol.replace('-', '.'))
    add(symbol.replace('.', '-'))
    for cand in (lookup_candidates or []):
        add(cand)
        add(str(cand).replace('-', '.'))
        add(str(cand).replace('.', '-'))
        if len(out) >= max_candidates:
            break
    return out[:max_candidates]

def _discover_yahoo_mapping(source_symbol, start, end, timeout=25, max_candidates=8):
    source_symbol = _normalize_symbol(source_symbol)
    lookup = _search_yahoo_symbol_candidates(source_symbol, timeout=max(10, timeout))
    candidates = _build_symbol_test_candidates(source_symbol, lookup.get("candidates", []), max_candidates=max_candidates)
    for candidate in candidates:
        bundle = _download_single_ticker_ohlc(candidate, start, end, timeout=timeout)
        if bundle:
            rename_map = {sym: source_symbol for sym in _bundle_loaded_symbols(bundle)}
            bundle = _rename_ohlc_bundle(bundle, rename_map)
            close_frame = bundle.get("close") if bundle else None
            history_rows = int(len(close_frame)) if close_frame is not None and not close_frame.empty else 0
            return {
                "symbol": source_symbol,
                "success": True,
                "yahoo_symbol": candidate,
                "history_rows": history_rows,
                "lookup_ok": bool(lookup.get("lookup_ok")),
                "lookup_exact": bool(lookup.get("exact_match")),
                "lookup_candidates": ", ".join(lookup.get("candidates", [])[:5]),
                "status": "Gemappt und geladen",
                "bundle": bundle,
            }
    status = "Yahoo kennt Symbol, aber keine Historie" if lookup.get("lookup_ok") else "Yahoo kennt Symbol nicht"
    return {
        "symbol": source_symbol,
        "success": False,
        "yahoo_symbol": "",
        "history_rows": 0,
        "lookup_ok": bool(lookup.get("lookup_ok")),
        "lookup_exact": bool(lookup.get("exact_match")),
        "lookup_candidates": ", ".join(lookup.get("candidates", [])[:5]),
        "status": status,
        "bundle": None,
    }

def auto_remap_missing_nyse_yahoo(lookback_days=550, max_workers=8, max_candidates=8):
    """Automatically test Yahoo candidate symbols for still-missing NYSE members and persist working mappings."""
    end = datetime.now()
    start = end - timedelta(days=lookback_days)
    start_date = pd.Timestamp(start).strftime("%Y-%m-%d")
    end_date = pd.Timestamp(end).strftime("%Y-%m-%d")
    store = _get_price_store()
    _init_price_cache_db(store)

    tickers = _get_nyse_stock_tickers_with_cache(store)
    if not tickers:
        return {"ok": False, "error": "Keine aktuelle NYSE-Tickerliste verfügbar.", "store": _get_store_label(store)}

    tickers = list(dict.fromkeys([_normalize_symbol(t) for t in tickers if t]))
    _store_universe_members(store, CACHE_UNIVERSE_NAME, tickers)

    missing_before, _ = _get_missing_universe_tickers(store, tickers)
    if not missing_before:
        requested = len(tickers)
        loaded = requested
        return {
            "ok": True,
            "requested": requested,
            "loaded": loaded,
            "coverage": loaded / max(requested, 1),
            "missing_before": 0,
            "missing_after": 0,
            "new_symbols_loaded": 0,
            "rows_written": 0,
            "mapped_successes": 0,
            "attempted": 0,
            "store": _get_store_label(store),
            "backend": store["backend"],
            "message": "Es fehlen aktuell keine NYSE-Ticker mehr im Datenspeicher.",
            "results_df": pd.DataFrame(),
            "counts": {},
        }

    rows_written = 0
    mapped_successes = 0
    results = []

    saved_mappings = _load_symbol_mappings(store, CACHE_UNIVERSE_NAME, status_filter=("mapped",))
    if saved_mappings:
        reusable = [s for s in missing_before if s in saved_mappings]
        for batch in _chunked(reusable, 40):
            bundle, collision_symbols = _download_ohlc_batch_mapped(batch, saved_mappings, start, end, threads=False, timeout=45)
            close_frame = bundle.get("close") if bundle else None
            loaded_symbols = []
            if close_frame is not None and not close_frame.empty:
                rows_written += _write_price_bundle_to_cache(store, bundle)
                loaded_symbols = [str(c).strip().upper() for c in close_frame.columns]
            for symbol in loaded_symbols:
                mapped_successes += 1
                results.append({
                    "symbol": symbol,
                    "success": True,
                    "yahoo_symbol": saved_mappings.get(symbol, symbol),
                    "history_rows": int(close_frame[symbol].dropna().shape[0]) if close_frame is not None and symbol in close_frame.columns else 0,
                    "lookup_ok": True,
                    "lookup_exact": True,
                    "lookup_candidates": saved_mappings.get(symbol, symbol),
                    "status": "Gespeichertes Mapping erneut genutzt",
                })
            for symbol in batch:
                if symbol not in loaded_symbols and symbol not in collision_symbols:
                    pass

    missing_after_saved, _ = _get_missing_universe_tickers(store, tickers)
    to_discover = [s for s in missing_after_saved if s not in saved_mappings]

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_discover_yahoo_mapping, symbol, start, end, 30, max_candidates): symbol
            for symbol in to_discover
        }
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                item = future.result()
            except Exception:
                item = {
                    "symbol": symbol,
                    "success": False,
                    "yahoo_symbol": "",
                    "history_rows": 0,
                    "lookup_ok": False,
                    "lookup_exact": False,
                    "lookup_candidates": "",
                    "status": "Mappingfehler",
                    "bundle": None,
                }
            bundle = item.pop("bundle", None)
            close_frame = bundle.get("close") if bundle else None
            if item.get("success") and close_frame is not None and not close_frame.empty:
                rows_written += _write_price_bundle_to_cache(store, bundle)
                mapped_successes += 1
                _upsert_symbol_mapping(
                    store,
                    CACHE_UNIVERSE_NAME,
                    item["symbol"],
                    item.get("yahoo_symbol"),
                    status="mapped",
                    note=item.get("lookup_candidates", "")[:300],
                )
            else:
                _upsert_symbol_mapping(
                    store,
                    CACHE_UNIVERSE_NAME,
                    item["symbol"],
                    item.get("yahoo_symbol") or None,
                    status="no_history" if item.get("lookup_ok") else "not_found",
                    note=item.get("lookup_candidates", "")[:300],
                )
            results.append(item)

    missing_after, _ = _get_missing_universe_tickers(store, tickers)
    loaded = max(0, len(tickers) - len(missing_after))
    requested = len(tickers)
    coverage = loaded / max(requested, 1)
    new_symbols_loaded = len(missing_before) - len(missing_after)

    results_df = pd.DataFrame(results)
    if not results_df.empty:
        results_df = results_df.sort_values(["status", "symbol"]).reset_index(drop=True)
    counts = results_df["status"].value_counts().to_dict() if not results_df.empty else {}

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    _set_cache_metadata(store, "last_auto_remap_at", now_str)
    _set_cache_metadata(store, "last_auto_remap_missing_before", len(missing_before))
    _set_cache_metadata(store, "last_auto_remap_missing_after", len(missing_after))
    _set_cache_metadata(store, "last_auto_remap_rows_written", rows_written)
    _set_cache_metadata(store, "last_auto_remap_mapped", mapped_successes)
    _set_cache_metadata(store, "last_refresh_loaded_universe", loaded)
    _set_cache_metadata(store, "last_refresh_requested_universe", requested)
    if rows_written > 0 and loaded > 0:
        _refresh_breadth_snapshot(store, tickers, start_date, end_date, source="auto-remap")

    return {
        "ok": loaded > 0,
        "requested": requested,
        "loaded": loaded,
        "coverage": coverage,
        "missing_before": len(missing_before),
        "missing_after": len(missing_after),
        "new_symbols_loaded": new_symbols_loaded,
        "rows_written": rows_written,
        "mapped_successes": mapped_successes,
        "attempted": len(to_discover),
        "store": _get_store_label(store),
        "backend": store["backend"],
        "last_auto_remap_at": now_str,
        "results_df": results_df,
        "counts": counts,
    }

def diagnose_missing_nyse_yahoo(sample_size=80, lookback_days=550, max_workers=8):
    """Diagnose a sample of still-missing NYSE symbols against Yahoo."""
    end = datetime.now()
    start = end - timedelta(days=lookback_days)
    start_date = pd.Timestamp(start).strftime("%Y-%m-%d")
    end_date = pd.Timestamp(end).strftime("%Y-%m-%d")
    store = _get_price_store()
    _init_price_cache_db(store)

    tickers = _get_nyse_stock_tickers_with_cache(store)
    if not tickers:
        return {"ok": False, "error": "Keine aktuelle NYSE-Tickerliste verfügbar.", "store": _get_store_label(store)}

    tickers = list(dict.fromkeys([str(t).strip().upper().replace('.', '-').replace('/', '-') for t in tickers if t]))
    missing, _ = _get_missing_universe_tickers(store, tickers)
    loaded_count = len(tickers) - len(missing)

    if not missing:
        return {
            "ok": True,
            "store": _get_store_label(store),
            "requested": len(tickers),
            "loaded": loaded_count,
            "missing_total": 0,
            "sample_size": 0,
            "results_df": pd.DataFrame(),
            "counts": {},
            "message": "Es fehlen aktuell keine NYSE-Ticker mehr im Datenspeicher.",
        }

    sample_size = max(1, min(int(sample_size), len(missing)))
    sample = missing[:sample_size]

    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_probe_single_missing_symbol, symbol, start, end, 25): symbol for symbol in sample}
        for future in as_completed(futures):
            try:
                results.append(future.result())
            except Exception:
                symbol = futures[future]
                results.append({
                    "symbol": symbol,
                    "lookup_ok": False,
                    "lookup_exact": False,
                    "lookup_candidates": "",
                    "history_ok": False,
                    "history_variant": "",
                    "history_rows": 0,
                    "status": "Diagnosefehler",
                })

    df = pd.DataFrame(results)
    if not df.empty:
        df = df.sort_values(["status", "symbol"]).reset_index(drop=True)

    counts = df["status"].value_counts().to_dict() if not df.empty else {}
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    _set_cache_metadata(store, "last_yahoo_diag_at", now_str)
    _set_cache_metadata(store, "last_yahoo_diag_sample", sample_size)
    _set_cache_metadata(store, "last_yahoo_diag_missing_total", len(missing))
    _set_cache_metadata(store, "last_yahoo_diag_history_ok", counts.get("Historie vorhanden", 0))
    _set_cache_metadata(store, "last_yahoo_diag_lookup_no_history", counts.get("Yahoo kennt Symbol, aber keine Historie", 0))
    _set_cache_metadata(store, "last_yahoo_diag_unknown", counts.get("Yahoo kennt Symbol nicht", 0))
    _set_cache_metadata(store, "last_yahoo_diag_errors", counts.get("Diagnosefehler", 0))

    return {
        "ok": True,
        "store": _get_store_label(store),
        "requested": len(tickers),
        "loaded": loaded_count,
        "missing_total": len(missing),
        "sample_size": sample_size,
        "results_df": df,
        "counts": counts,
        "last_diag_at": now_str,
    }

def _get_missing_universe_tickers(store, universe_tickers):
    last_dates = _get_cached_last_dates(store, universe_tickers)
    missing = [t for t in universe_tickers if t not in last_dates]
    return missing, last_dates

def _bundle_to_long_records(bundle):
    if not bundle:
        return []
    merged = None
    field_order = [("close", "close"), ("high", "high"), ("low", "low")]
    for bundle_key, out_col in field_order:
        frame = bundle.get(bundle_key)
        if frame is None or frame.empty:
            continue
        long_df = frame.copy()
        long_df.index = pd.to_datetime(long_df.index)
        long_df = long_df.reset_index().rename(columns={long_df.index.name or "index": "date"})
        long_df["date"] = pd.to_datetime(long_df["date"]).dt.strftime("%Y-%m-%d")
        long_df = long_df.melt(id_vars="date", var_name="symbol", value_name=out_col).dropna(subset=[out_col])
        if long_df.empty:
            continue
        long_df["symbol"] = long_df["symbol"].astype(str).str.upper()
        if merged is None:
            merged = long_df
        else:
            merged = merged.merge(long_df, on=["symbol", "date"], how="outer")
    if merged is None or merged.empty:
        return []
    for col in ("close", "high", "low"):
        if col not in merged.columns:
            merged[col] = np.nan
    merged = merged[["symbol", "date", "close", "high", "low"]]
    return list(merged.itertuples(index=False, name=None))


def _write_price_bundle_to_cache(store, bundle):
    records = _bundle_to_long_records(bundle)
    if not records:
        return 0
    conn = _get_cache_conn(store)
    try:
        if _is_neon_conn(conn):
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO prices(symbol, date, close, high, low)
                    VALUES %s
                    ON CONFLICT (symbol, date) DO UPDATE SET
                        close=COALESCE(EXCLUDED.close, prices.close),
                        high=COALESCE(EXCLUDED.high, prices.high),
                        low=COALESCE(EXCLUDED.low, prices.low)
                    """,
                    records,
                    page_size=5000,
                )
        else:
            conn.executemany(
                """
                INSERT INTO prices(symbol, date, close, high, low)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(symbol, date) DO UPDATE SET
                    close=COALESCE(excluded.close, prices.close),
                    high=COALESCE(excluded.high, prices.high),
                    low=COALESCE(excluded.low, prices.low)
                """,
                records,
            )
        _set_cache_metadata(store, "prices_last_write_at", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"), conn=conn)
        conn.commit()
    finally:
        conn.close()
    return len(records)
def _write_closes_to_cache(store, closes):
    if closes is None or closes.empty:
        return 0
    return _write_price_bundle_to_cache(store, {"close": closes})

def _read_cached_price_bundle(store, tickers, start_date, end_date):
    tickers = list(dict.fromkeys([str(t).strip().upper() for t in tickers if t]))
    if not tickers:
        return None
    conn = _get_cache_conn(store)
    try:
        frames = []
        if _is_neon_conn(conn):
            with conn.cursor() as cur:
                for batch in _chunked(tickers, 700):
                    cur.execute(
                        """
                        SELECT date, symbol, close, high, low
                        FROM prices
                        WHERE symbol = ANY(%s)
                          AND date >= %s
                          AND date <= %s
                        """,
                        (batch, start_date, end_date),
                    )
                    rows = cur.fetchall()
                    if rows:
                        frames.append(pd.DataFrame(rows, columns=["date", "symbol", "close", "high", "low"]))
        else:
            for batch in _chunked(tickers, 700):
                placeholders = ",".join(["?"] * len(batch))
                query = f"""
                    SELECT date, symbol, close, high, low
                    FROM prices
                    WHERE symbol IN ({placeholders})
                      AND date >= ?
                      AND date <= ?
                """
                params = list(batch) + [start_date, end_date]
                frame = pd.read_sql_query(query, conn, params=params)
                if not frame.empty:
                    frames.append(frame)
        if not frames:
            return None
        raw = pd.concat(frames, ignore_index=True)
    finally:
        conn.close()
    if raw.empty:
        return None
    raw["date"] = pd.to_datetime(raw["date"])
    raw["symbol"] = raw["symbol"].astype(str).str.upper()

    out = {}
    for field in ("close", "high", "low"):
        pivot = raw.pivot(index="date", columns="symbol", values=field).sort_index()
        pivot = pivot.apply(pd.to_numeric, errors="coerce")
        pivot = pivot.loc[:, ~pivot.columns.duplicated()]
        pivot = pivot.dropna(axis=1, how="all")
        out[field] = pivot if pivot.shape[1] else None
    return out if any(v is not None and not v.empty for v in out.values()) else None

def _read_cached_closes(store, tickers, start_date, end_date):
    bundle = _read_cached_price_bundle(store, tickers, start_date, end_date)
    if not bundle:
        return None
    return bundle.get("close")

def _get_cached_last_dates(store, tickers):
    tickers = list(dict.fromkeys([str(t).strip().upper() for t in tickers if t]))
    if not tickers:
        return {}
    conn = _get_cache_conn(store)
    try:
        out = {}
        if _is_neon_conn(conn):
            with conn.cursor() as cur:
                for batch in _chunked(tickers, 700):
                    cur.execute(
                        """
                        SELECT symbol, MAX(date) AS last_date
                        FROM prices
                        WHERE symbol = ANY(%s)
                        GROUP BY symbol
                        """,
                        (batch,),
                    )
                    rows = cur.fetchall()
                    out.update({str(symbol).upper(): pd.to_datetime(last_date) for symbol, last_date in rows if last_date})
            return out
        for batch in _chunked(tickers, 700):
            placeholders = ",".join(["?"] * len(batch))
            query = f"SELECT symbol, MAX(date) AS last_date FROM prices WHERE symbol IN ({placeholders}) GROUP BY symbol"
            rows = conn.execute(query, batch).fetchall()
            out.update({str(symbol).upper(): pd.to_datetime(last_date) for symbol, last_date in rows if last_date})
        return out
    finally:
        conn.close()

def _get_cached_price_field_counts(store, tickers, start_date, end_date):
    tickers = list(dict.fromkeys([str(t).strip().upper() for t in tickers if t]))
    if not tickers:
        return {}
    conn = _get_cache_conn(store)
    try:
        out = {}
        if _is_neon_conn(conn):
            with conn.cursor() as cur:
                for batch in _chunked(tickers, 700):
                    cur.execute(
                        """
                        SELECT symbol,
                               COUNT(close) AS close_count,
                               COUNT(high) AS high_count,
                               COUNT(low) AS low_count
                        FROM prices
                        WHERE symbol = ANY(%s)
                          AND date >= %s
                          AND date <= %s
                        GROUP BY symbol
                        """,
                        (batch, start_date, end_date),
                    )
                    rows = cur.fetchall()
                    for symbol, close_count, high_count, low_count in rows:
                        out[str(symbol).upper()] = {
                            "close": int(close_count or 0),
                            "high": int(high_count or 0),
                            "low": int(low_count or 0),
                        }
            return out
        for batch in _chunked(tickers, 700):
            placeholders = ",".join(["?"] * len(batch))
            query = f"""
                SELECT symbol,
                       COUNT(close) AS close_count,
                       COUNT(high) AS high_count,
                       COUNT(low) AS low_count
                FROM prices
                WHERE symbol IN ({placeholders})
                  AND date >= ?
                  AND date <= ?
                GROUP BY symbol
            """
            rows = conn.execute(query, list(batch) + [start_date, end_date]).fetchall()
            for symbol, close_count, high_count, low_count in rows:
                out[str(symbol).upper()] = {
                    "close": int(close_count or 0),
                    "high": int(high_count or 0),
                    "low": int(low_count or 0),
                }
        return out
    finally:
        conn.close()


def _get_nyse_stock_tickers_with_cache(store):
    cached = _load_cached_universe_members(store, CACHE_UNIVERSE_NAME)
    fresh = get_nyse_breadth_tickers()
    # Prefer the larger fresh universe to self-heal stale cache entries after universe changes.
    if fresh and (not cached or len(fresh) > len(cached) + 100):
        _store_universe_members(store, CACHE_UNIVERSE_NAME, fresh)
        return fresh
    if cached:
        return cached
    if fresh:
        _store_universe_members(store, CACHE_UNIVERSE_NAME, fresh)
    return fresh

@st.cache_data(ttl=120, show_spinner=False)
def get_live_universe_store_status(store_backend: str, store_label: str, last_refresh_at: str = ""):
    store = _get_price_store()
    _init_price_cache_db(store)
    tickers = _get_nyse_stock_tickers_with_cache(store)
    if not tickers:
        return {"requested": 0, "loaded": 0, "coverage": 0.0, "mapped": 0, "not_found": 0, "no_history": 0}
    tickers = list(dict.fromkeys([_normalize_symbol(t) for t in tickers if t]))
    requested = len(tickers)
    meta = _get_cache_metadata_many(store, ["last_refresh_loaded_universe", "last_refresh_requested_universe"])
    loaded = _safe_int(meta.get("last_refresh_loaded_universe"), 0)
    requested_meta = _safe_int(meta.get("last_refresh_requested_universe"), requested)
    if loaded <= 0 or requested_meta != requested:
        last_dates = _get_cached_last_dates(store, tickers)
        loaded = len(last_dates)
        _set_cache_metadata_many(store, {
            "last_refresh_loaded_universe": loaded,
            "last_refresh_requested_universe": requested,
        })
    counts = {"mapped": 0, "not_found": 0, "no_history": 0}
    try:
        conn = _get_cache_conn(store)
        try:
            # _get_cache_conn kann auf SQLite zurückfallen (z. B. wenn Neon nicht
            # erreichbar ist); dann muss auch hier der SQLite-Pfad genutzt werden.
            backend = store.get("backend") if isinstance(store, dict) else None
            if backend == "neon" and not isinstance(conn, sqlite3.Connection):
                with conn.cursor() as cur:
                    cur.execute("SELECT status, COUNT(*) FROM symbol_mappings WHERE universe=%s GROUP BY status", (CACHE_UNIVERSE_NAME,))
                    rows = cur.fetchall()
            else:
                rows = conn.execute("SELECT status, COUNT(*) FROM symbol_mappings WHERE universe=? GROUP BY status", (CACHE_UNIVERSE_NAME,)).fetchall()
            for status, count in rows:
                if str(status) in counts:
                    counts[str(status)] = int(count or 0)
        finally:
            conn.close()
    except Exception:
        pass
    return {"requested": requested, "loaded": loaded, "coverage": loaded / max(requested, 1), **counts}
def _prepare_component_frame(frame):
    if frame is None or frame.empty:
        return None
    frame = frame.copy()
    frame.index = pd.to_datetime(frame.index)
    frame = frame.sort_index()
    frame = frame.apply(pd.to_numeric, errors="coerce")
    frame = frame.loc[:, ~frame.columns.duplicated()]
    thresh = max(120, int(len(frame) * 0.30))
    frame = frame.dropna(axis=1, thresh=thresh)
    frame = frame.dropna(axis=1, how="all")
    return frame if frame.shape[1] else None

def _prepare_component_bundle(bundle):
    if not bundle:
        return None
    prepared = {k: _prepare_component_frame(v) for k, v in bundle.items()}
    close_frame = prepared.get("close")
    high_frame = prepared.get("high")
    low_frame = prepared.get("low")
    if close_frame is None or close_frame.empty:
        return None

    shared = set(close_frame.columns)
    if high_frame is not None and not high_frame.empty:
        shared &= set(high_frame.columns)
    if low_frame is not None and not low_frame.empty:
        shared &= set(low_frame.columns)

    if not shared:
        return None

    shared = sorted(shared)
    prepared["close"] = close_frame[shared]
    prepared["high"] = high_frame[shared] if high_frame is not None and not high_frame.empty else None
    prepared["low"] = low_frame[shared] if low_frame is not None and not low_frame.empty else None
    return prepared

def refresh_nyse_price_store(lookback_days=550, history_batch_size=140, recent_batch_size=220, recent_refresh_days=15):
    """Initial fill or incremental refresh for the persistent price store.

    - symbols without history: fetch full lookback window
    - existing symbols: refresh only a recent rolling window and upsert rows
    """
    end = datetime.now()
    start = end - timedelta(days=lookback_days)
    start_date = pd.Timestamp(start).strftime("%Y-%m-%d")
    end_date = pd.Timestamp(end).strftime("%Y-%m-%d")
    store = _get_price_store()
    _init_price_cache_db(store)

    tickers = _get_nyse_stock_tickers_with_cache(store)
    if not tickers:
        return {"ok": False, "error": "Keine aktuelle NYSE-Tickerliste verfügbar.", "store": _get_store_label(store)}
    tickers = list(dict.fromkeys([str(t).strip().upper().replace('.', '-').replace('/', '-') for t in tickers if t]))
    _store_universe_members(store, CACHE_UNIVERSE_NAME, tickers)

    last_dates = _get_cached_last_dates(store, tickers)
    min_history_rows = 180
    recent_start = max(start, end - timedelta(days=recent_refresh_days))
    field_count_start = pd.Timestamp(max(start, end - timedelta(days=max(min_history_rows * 2, 240)))).strftime("%Y-%m-%d")
    stale_cutoff = pd.Timestamp(recent_start) - pd.Timedelta(days=5)
    field_check_targets = [
        t for t in tickers
        if t not in last_dates or pd.Timestamp(last_dates.get(t)) < stale_cutoff
    ]
    field_counts = _get_cached_price_field_counts(store, field_check_targets, field_count_start, end_date) if field_check_targets else {}
    needs_full_ohlc = [
        t for t in field_check_targets
        if t not in last_dates
        or field_counts.get(t, {}).get("close", 0) < min_history_rows
        or field_counts.get(t, {}).get("high", 0) < min_history_rows
        or field_counts.get(t, {}).get("low", 0) < min_history_rows
    ]
    missing_history = list(dict.fromkeys(needs_full_ohlc))
    recent_targets = [t for t in tickers if t not in set(missing_history)]

    rows_written = 0
    history_batches = 0
    recent_batches = 0

    if missing_history:
        for batch in _chunked(missing_history, history_batch_size):
            bundle = _download_ohlc_batch_fast(batch, start, end, threads=True, timeout=40)
            close_frame = bundle.get("close") if bundle else None
            if close_frame is not None and not close_frame.empty:
                rows_written += _write_price_bundle_to_cache(store, bundle)
            history_batches += 1

        refreshed_dates = _get_cached_last_dates(store, tickers)
        refreshed_symbol_set = set(refreshed_dates)
        still_missing = [t for t in tickers if t not in refreshed_symbol_set]
        if still_missing:
            for batch in _chunked(still_missing, max(40, history_batch_size // 2)):
                bundle = _download_ohlc_batch_fast(batch, start, end, threads=False, timeout=30)
                close_frame = bundle.get("close") if bundle else None
                if close_frame is not None and not close_frame.empty:
                    rows_written += _write_price_bundle_to_cache(store, bundle)
                history_batches += 1
            refreshed_dates = _get_cached_last_dates(store, tickers)
            refreshed_symbol_set = set(refreshed_dates)
        recent_targets = [t for t in tickers if t in refreshed_symbol_set]

    for batch in _chunked(recent_targets, recent_batch_size):
        bundle = _download_ohlc_batch_fast(batch, recent_start, end, threads=True, timeout=35)
        close_frame = bundle.get("close") if bundle else None
        if close_frame is not None and not close_frame.empty:
            rows_written += _write_price_bundle_to_cache(store, bundle)
        recent_batches += 1

    # Nach den Recent-Batches neu einlesen: erst dadurch sind Symbole erfasst,
    # die ausschließlich im inkrementellen Refresh in den Cache gelangt sind.
    final_last_dates = _get_cached_last_dates(store, tickers) if recent_targets else (
        refreshed_dates if missing_history else last_dates
    )
    loaded = len(final_last_dates)
    requested = len(tickers)
    coverage = loaded / max(requested, 1)

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    _set_cache_metadata_many(store, {"last_refresh_at": now_str, "last_refresh_rows_written": rows_written, "last_refresh_loaded_universe": loaded, "last_refresh_requested_universe": requested})
    if loaded > 0:
        _refresh_breadth_snapshot(store, tickers, start_date, end_date, source="refresh")

    return {
        "ok": loaded > 0,
        "requested": requested,
        "loaded": loaded,
        "coverage": coverage,
        "rows_written": rows_written,
        "history_batches": history_batches,
        "recent_batches": recent_batches,
        "recent_refresh_days": recent_refresh_days,
        "last_refresh_at": now_str,
        "store": _get_store_label(store),
        "backend": store["backend"],
    }

def _maybe_auto_refresh_sqlite_cache(store, reason="auto"):
    """Run a best-effort auto refresh when SQLite is active.

    Prevents stale local-only setups from waiting on external Neon/GitHub schedules.
    """
    if not isinstance(store, dict) or store.get("backend") != "sqlite":
        return {"triggered": False, "reason": "not_sqlite"}

    lock_key = f"sqlite_auto_refresh_running_{reason}"
    if st.session_state.get(lock_key):
        return {"triggered": False, "reason": "already_running"}

    last_run_key = f"sqlite_auto_refresh_last_{reason}"
    last_run = st.session_state.get(last_run_key)
    now_utc = datetime.now(timezone.utc)
    if isinstance(last_run, datetime) and (now_utc - last_run) < timedelta(minutes=30):
        return {"triggered": False, "reason": "cooldown"}

    st.session_state[lock_key] = True
    st.session_state[last_run_key] = now_utc
    try:
        result = refresh_nyse_price_store()
        return {"triggered": True, "ok": bool(result.get("ok")), "result": result}
    except Exception as exc:
        logger.warning("SQLite auto refresh failed (%s): %s", reason, exc)
        return {"triggered": True, "ok": False, "error": str(exc)}
    finally:
        st.session_state[lock_key] = False

def rescue_missing_nyse_price_store(lookback_days=550, rescue_batch_size=24, max_workers=8):
    """Try to backfill still-missing NYSE symbols without manual ticker input."""
    end = datetime.now()
    start = end - timedelta(days=lookback_days)
    start_date = pd.Timestamp(start).strftime("%Y-%m-%d")
    end_date = pd.Timestamp(end).strftime("%Y-%m-%d")
    store = _get_price_store()
    _init_price_cache_db(store)

    tickers = _get_nyse_stock_tickers_with_cache(store)
    if not tickers:
        return {"ok": False, "error": "Keine aktuelle NYSE-Tickerliste verfügbar.", "store": _get_store_label(store)}

    tickers = list(dict.fromkeys([str(t).strip().upper().replace('.', '-').replace('/', '-') for t in tickers if t]))
    _store_universe_members(store, CACHE_UNIVERSE_NAME, tickers)

    missing_before, _ = _get_missing_universe_tickers(store, tickers)
    if not missing_before:
        requested = len(tickers)
        loaded = requested
        return {
            "ok": True,
            "requested": requested,
            "loaded": loaded,
            "coverage": loaded / max(requested, 1),
            "missing_before": 0,
            "missing_after": 0,
            "new_symbols_loaded": 0,
            "rows_written": 0,
            "rescue_batches": 0,
            "single_attempts": 0,
            "single_successes": 0,
            "store": _get_store_label(store),
            "backend": store["backend"],
            "message": "Es fehlen aktuell keine NYSE-Ticker mehr im Datenspeicher.",
        }

    rows_written = 0
    rescue_batches = 0
    saved_mappings = _load_symbol_mappings(store, CACHE_UNIVERSE_NAME, status_filter=("mapped",))

    for batch in _chunked(missing_before, rescue_batch_size):
        bundle, collisions = _download_ohlc_batch_mapped(batch, saved_mappings, start, end, threads=False, timeout=45)
        close_frame = bundle.get("close") if bundle else None
        if close_frame is not None and not close_frame.empty:
            rows_written += _write_price_bundle_to_cache(store, bundle)
        retry_symbols = list(dict.fromkeys((collisions or []) + [s for s in batch if close_frame is None or s not in close_frame.columns]))
        for symbol in retry_symbols:
            single_bundle = _download_single_source_symbol(symbol, saved_mappings, start, end, timeout=45)
            single_close = single_bundle.get("close") if single_bundle else None
            if single_close is not None and not single_close.empty:
                rows_written += _write_price_bundle_to_cache(store, single_bundle)
        rescue_batches += 1

    missing_after_batch, _ = _get_missing_universe_tickers(store, tickers)
    single_attempts = len(missing_after_batch)
    single_successes = 0

    if missing_after_batch:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_download_single_source_symbol, symbol, saved_mappings, start, end, 45): symbol
                for symbol in missing_after_batch
            }
            for future in as_completed(futures):
                closes = None
                try:
                    closes = future.result()
                except Exception:
                    closes = None
                close_frame = closes.get("close") if closes else None
                if close_frame is not None and not close_frame.empty:
                    rows_written += _write_price_bundle_to_cache(store, closes)
                    single_successes += 1

    missing_after, _ = _get_missing_universe_tickers(store, tickers)
    loaded = max(0, len(tickers) - len(missing_after))
    requested = len(tickers)
    coverage = loaded / max(requested, 1)
    new_symbols_loaded = len(missing_before) - len(missing_after)

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    _set_cache_metadata_many(store, {"last_rescue_at": now_str, "last_rescue_rows_written": rows_written, "last_rescue_missing_before": len(missing_before), "last_rescue_missing_after": len(missing_after), "last_refresh_loaded_universe": loaded, "last_refresh_requested_universe": requested})
    if rows_written > 0 and loaded > 0:
        _refresh_breadth_snapshot(store, tickers, start_date, end_date, source="rescue")

    return {
        "ok": loaded > 0,
        "requested": requested,
        "loaded": loaded,
        "coverage": coverage,
        "missing_before": len(missing_before),
        "missing_after": len(missing_after),
        "new_symbols_loaded": new_symbols_loaded,
        "rows_written": rows_written,
        "rescue_batches": rescue_batches,
        "single_attempts": single_attempts,
        "single_successes": single_successes,
        "last_rescue_at": now_str,
        "store": _get_store_label(store),
        "backend": store["backend"],
    }

def load_nyse_breadth_data(lookback_days=550):
    """Read cached breadth snapshot from the persistent store without reloading the full universe on every page view."""
    end = datetime.now()
    start = end - timedelta(days=lookback_days)
    start_date = pd.Timestamp(start).strftime("%Y-%m-%d")
    end_date = pd.Timestamp(end).strftime("%Y-%m-%d")
    store = _get_price_store()
    _init_price_cache_db(store)

    tickers = _get_nyse_stock_tickers_with_cache(store)
    if not tickers:
        return None
    tickers = list(dict.fromkeys([str(t).strip().upper().replace('.', '-').replace('/', '-') for t in tickers if t]))

    requested = len(tickers)
    meta = _get_cache_metadata_many(
        store,
        [
            BREADTH_SNAPSHOT_KEY,
            BREADTH_SNAPSHOT_AT_KEY,
            "last_refresh_loaded_universe",
            "last_refresh_requested_universe",
        ],
    )
    snapshot = _deserialize_breadth_snapshot(meta.get(BREADTH_SNAPSHOT_KEY, ""))
    if snapshot is not None and not snapshot.empty:
        loaded = _safe_int(snapshot.attrs.get("loaded_universe", snapshot.attrs.get("breadth_universe_loaded")), snapshot.shape[1] if hasattr(snapshot, "shape") else 0)
        requested_snapshot = _safe_int(snapshot.attrs.get("requested_universe"), requested)
        min_required = max(350, int(requested * 0.18))
        if requested_snapshot == requested and loaded >= min_required:
            snapshot.attrs["snapshot_generated_at"] = meta.get(BREADTH_SNAPSHOT_AT_KEY, snapshot.attrs.get("snapshot_generated_at", ""))
            snapshot.attrs["snapshot_source"] = _get_cache_metadata(store, BREADTH_SNAPSHOT_SOURCE_KEY, "")
            return snapshot

    breadth = _refresh_breadth_snapshot(store, tickers, start_date, end_date, source="lazy-rebuild")
    if breadth is None or breadth.empty:
        return None
    loaded = _safe_int(breadth.attrs.get("loaded_universe", breadth.attrs.get("breadth_universe_loaded")), 0)
    min_required = max(350, int(requested * 0.18))
    if loaded > _safe_int(meta.get("last_refresh_loaded_universe"), 0):
        _set_cache_metadata_many(store, {"last_refresh_loaded_universe": loaded, "last_refresh_requested_universe": requested})
    return breadth if loaded >= min_required else None


@st.cache_data(ttl=3600, show_spinner=False)
def load_fed_funds_rate(fred_key):
    """Load Federal Funds Rate from FRED API."""
    if not fred_key:
        return None
    try:
        url = "https://api.stlouisfed.org/fred/series/observations"
        params = {
            "series_id": "DFEDTARU",
            "api_key": fred_key,
            "file_type": "json",
            "sort_order": "desc",
            "limit": 500,
            "observation_start": (datetime.now() - timedelta(days=800)).strftime("%Y-%m-%d"),
        }
        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code != 200:
            logger.warning("FRED request failed with status %s", resp.status_code)
            return None
        obs = resp.json().get("observations", [])
        if not obs:
            return None
        df = pd.DataFrame(obs)
        df["date"] = pd.to_datetime(df["date"])
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df = df.dropna(subset=["value"]).set_index("date").sort_index()
        return df[["value"]].rename(columns={"value": "FedRate"})
    except Exception as exc:
        logger.warning("FRED download failed: %s", exc)
        return None


# Backward-compatible wrappers for the old function names
def refresh_russell2000_price_store(*args, **kwargs):
    return refresh_nyse_price_store(*args, **kwargs)


def rescue_missing_russell2000_price_store(*args, **kwargs):
    return rescue_missing_nyse_price_store(*args, **kwargs)


def auto_remap_missing_russell2000_yahoo(*args, **kwargs):
    return auto_remap_missing_nyse_yahoo(*args, **kwargs)


def diagnose_missing_russell2000_yahoo(*args, **kwargs):
    return diagnose_missing_nyse_yahoo(*args, **kwargs)


def load_russell2000_breadth_data(*args, **kwargs):
    return load_nyse_breadth_data(*args, **kwargs)

# ===== From analytics.py =====
def _ema(s,p): return s.ewm(span=p,adjust=False).mean()

def _sma(s,p): return s.rolling(window=p,min_periods=p).mean()

def _atr(df,p=21):
    h,l,pc = df["High"],df["Low"],df["Close"].shift(1)
    return pd.concat([h-l,(h-pc).abs(),(l-pc).abs()],axis=1).max(axis=1).rolling(p,min_periods=p).mean()

def _consec(s):
    o=np.zeros(len(s),dtype=int)
    for i in range(len(s)):
        if s.iloc[i]: o[i]=o[i-1]+1 if i>0 else 1
    return pd.Series(o,index=s.index)

def add_indicators(df):
    df=df.copy()
    df["EMA21"]=_ema(df["Close"],21);df["SMA50"]=_sma(df["Close"],50);df["SMA200"]=_sma(df["Close"],200)
    df["SMA10"]=_sma(df["Close"],10)
    df["ATR21"]=_atr(df,21);df["ATR_pct"]=df["ATR21"]/df["Close"]*100
    df["Vol_SMA50"]=_sma(df["Volume"],50);df["Pct_Change"]=df["Close"].pct_change(fill_method=None)*100
    rng=df["High"]-df["Low"];df["Closing_Range"]=np.where(rng>0,(df["Close"]-df["Low"])/rng,0.5)
    df["Dist_21EMA"]=(df["Close"]-df["EMA21"])/df["ATR21"]
    df["Dist_50SMA_pct"]=(df["Close"]-df["SMA50"])/df["SMA50"]*100
    df["Dist_200SMA_pct"]=(df["Close"]-df["SMA200"])/df["SMA200"]*100
    df["Dist_10SMA_pct"]=(df["Close"]-df["SMA10"])/df["SMA10"]*100
    df["High_52w"]=df["High"].rolling(252,min_periods=1).max()
    df["Dist_52w_pct"]=(df["Close"]-df["High_52w"])/df["High_52w"]*100
    df["MA_Order"]=(df["EMA21"]>df["SMA50"])&(df["SMA50"]>df["SMA200"])
    df["Low_above_21"]=df["Low"]>df["EMA21"];df["Low_above_50"]=df["Low"]>df["SMA50"];df["Low_above_200"]=df["Low"]>df["SMA200"]
    df["Consec_Low_above_21"]=_consec(df["Low_above_21"]);df["Consec_Low_above_50"]=_consec(df["Low_above_50"]);df["Consec_Low_above_200"]=_consec(df["Low_above_200"])
    df["EMA21_held"]=df["Close"]>df["EMA21"];df["SMA50_held"]=df["Close"]>df["SMA50"];df["SMA200_held"]=df["Close"]>df["SMA200"]
    _prev_close=df["Close"].shift(1)
    df["Intraday_Reversal_Down"]=(df["Open"]>_prev_close)&(df["Close"]<df["Open"])
    df["Intraday_Reversal_Up"]=(df["Open"]<_prev_close)&(df["Close"]>df["Open"])
    df["Neg_Reversals_10d"]=df["Intraday_Reversal_Down"].rolling(10,min_periods=1).sum().astype(int)
    df["Pos_Reversals_10d"]=df["Intraday_Reversal_Up"].rolling(10,min_periods=1).sum().astype(int)
    df["Low_CR"]=df["Closing_Range"]<0.25;df["Low_CR_5d"]=df["Low_CR"].rolling(5,min_periods=1).sum().astype(int)
    # „rückläufiges Volumen": mehrheitlich negative Volumendifferenzen — Original
    # forderte .max()<0 (ALLE 5 Tage strikt fallend), was praktisch nie eintrat.
    df["Up_Vol_Declining"]=(df["Close"]>df["Close"].shift(5))&(df["Volume"].diff().rolling(5, min_periods=5).mean()<0)
    return df

def _count_active_distribution_days(distribution_mask, close, window_days=25, recovery_gain_pct=6.0):
    mask = distribution_mask.fillna(False).astype(bool).to_numpy()
    close_values = pd.to_numeric(close, errors="coerce").to_numpy(dtype=float)
    recovery_factor = 1.0 + max(float(recovery_gain_pct), 0.0) / 100.0
    counts = []
    for i in range(len(mask)):
        start = max(0, i - int(window_days) + 1)
        count = 0
        for j in range(start, i + 1):
            if not mask[j]:
                continue
            ref_close = close_values[j]
            recovered = False
            if np.isfinite(ref_close) and ref_close > 0 and recovery_factor > 1.0 and i > j:
                later = close_values[j + 1:i + 1]
                later = later[np.isfinite(later)]
                recovered = bool(len(later) and later.max() >= ref_close * recovery_factor)
            if not recovered:
                count += 1
        counts.append(count)
    return pd.Series(counts, index=distribution_mask.index, dtype="int64")

def detect_distribution_days(df):
    df=df.copy();pc=df["Close"].shift(1);pv=df["Volume"].shift(1)
    is_down=df["Close"]<pc;high_vol=(df["Volume"]>pv)|(df["Volume"]>df["Vol_SMA50"])
    df["Is_Distribution"]=is_down&high_vol
    df["Is_Stall"]=(~is_down)&(df["Pct_Change"]<0.5)&(df["Volume"]>=pv*0.95)&(df["Closing_Range"]<0.5)
    df["Dist_Count_25"]=_count_active_distribution_days(df["Is_Distribution"], df["Close"], 25, 6.0)
    return df

def compute_ampel(df):
    df=df.copy();n=len(df);phase="neutral"
    anchor_idx=None;floor_mark=None;startschuss_idx=None;startschuss_low=None;gruen_since=None;startschuss_bonus=None
    phases=["neutral"]*n;anchor_dates=[None]*n;floor_marks=[None]*n;startschuss_lows=[None]*n;startschuss_bonuses=[None]*n
    c_=df["Close"].values;o_=df["Open"].values;h_=df["High"].values;l_=df["Low"].values
    v_=df["Volume"].values;pct_=df["Pct_Change"].values;cr_=df["Closing_Range"].values
    dc_=df["Dist_Count_25"].values;s50_=df["SMA50"].values;s200_=df["SMA200"].values;e21_=df["EMA21"].values
    def _clear():
        nonlocal anchor_idx,floor_mark,startschuss_idx,startschuss_low,gruen_since,startschuss_bonus
        anchor_idx=None;floor_mark=None;startschuss_idx=None;startschuss_low=None;gruen_since=None;startschuss_bonus=None
    def _corr(i):
        lb=max(0,i-60);rh=np.nanmax(h_[lb:i+1]);dd=(c_[i]-rh)/rh*100 if rh>0 else 0
        return dd<-10 or (not np.isnan(s50_[i]) and c_[i]<s50_[i] and dc_[i]>=4)
    for i in range(1,n):
        pi=pct_[i] if not np.isnan(pct_[i]) else 0.0;cri=cr_[i] if not np.isnan(cr_[i]) else 0.5
        if phase in ("neutral","aufwaertstrend"):
            if _corr(i): phase="rot";_clear()
            elif phase=="aufwaertstrend" and not np.isnan(e21_[i]) and not np.isnan(s50_[i]) and e21_[i]<s50_[i]: phase="rot";_clear()
        elif phase=="rot":
            if anchor_idx is not None and i>anchor_idx and l_[i]<floor_mark: anchor_idx=None;floor_mark=None
            if anchor_idx is None:
                if pi>0.0 or (c_[i]>o_[i] and cri>=0.5): anchor_idx=i;floor_mark=min(l_[i],l_[i-1])
            if anchor_idx is not None and i>=anchor_idx+5:
                if pi>=1.0 and v_[i]>v_[i-1] and l_[i]>=floor_mark: phase="gelb";startschuss_idx=i;startschuss_low=l_[i];startschuss_bonus=not np.isnan(e21_[i]) and c_[i]>e21_[i]
        elif phase=="gelb":
            if startschuss_low is not None and c_[i]<startschuss_low: phase="rot";_clear()
            elif startschuss_idx is not None and i>startschuss_idx+2: phase="gruen";gruen_since=i
        elif phase=="gruen":
            if startschuss_low is not None and c_[i]<startschuss_low: phase="rot";_clear()
            elif not np.isnan(s200_[i]) and c_[i]>s200_[i] and not np.isnan(e21_[i]) and not np.isnan(s50_[i]) and e21_[i]>s50_[i] and (gruen_since and i-gruen_since>=10): phase="aufwaertstrend"
        phases[i]=phase
        if anchor_idx is not None: anchor_dates[i]=df.index[anchor_idx].strftime("%Y-%m-%d")
        if floor_mark is not None: floor_marks[i]=round(floor_mark,2)
        if startschuss_low is not None: startschuss_lows[i]=round(startschuss_low,2)
        if startschuss_bonus is not None: startschuss_bonuses[i]=startschuss_bonus
    df["Ampel_Phase"]=phases;df["Anchor_Date"]=anchor_dates;df["Floor_Mark"]=floor_marks;df["Startschuss_Low"]=startschuss_lows;df["Startschuss_Bonus"]=startschuss_bonuses
    return df

def compute_breadth_mode(df_ew):
    df_ew=df_ew.copy();df_ew["High_52w"]=df_ew["High"].rolling(252,min_periods=1).max()
    df_ew["Dist_52w_pct"]=(df_ew["Close"]-df_ew["High_52w"])/df_ew["High_52w"]*100
    df_ew["Raw_Mode"]=df_ew["Dist_52w_pct"].apply(lambda d:"schutz" if d<-8 else "wachsam" if d<-4 else "rueckenwind")
    modes=df_ew["Raw_Mode"].values;stable=[modes[0]];pending=None;pc=0
    for i in range(1,len(modes)):
        if modes[i]!=stable[-1]:
            if pending==modes[i]: pc+=1
            else: pending=modes[i];pc=1
            if pc>=3: stable.append(pending);pending=None;pc=0
            else: stable.append(stable[-1])
        else: stable.append(modes[i]);pending=None;pc=0
    df_ew["Breadth_Mode"]=stable;return df_ew

def _rolling_zscore(s, window=63):
    min_periods = max(20, window // 3)
    mean = s.rolling(window, min_periods=min_periods).mean()
    std = s.rolling(window, min_periods=min_periods).std().replace(0, np.nan)
    return (s - mean) / std


def _rolling_percentile(s, window=252):
    min_periods = max(60, window // 4)
    try:
        return s.rolling(window, min_periods=min_periods).rank(pct=True)
    except Exception:
        def _pct_rank(x):
            xs = pd.Series(x)
            return xs.rank(pct=True).iloc[-1]
        return s.rolling(window, min_periods=min_periods).apply(_pct_rank, raw=False)
def analyze_vix(dv):
    dv = dv.copy()
    dv["SMA10"] = _sma(dv["Close"], 10)
    dv["EMA10"] = _ema(dv["Close"], 10)
    dv["EMA21"] = _ema(dv["Close"], 21)
    dv["Ret_5d"] = dv["Close"].pct_change(5)
    dv["Ret_20d"] = dv["Close"].pct_change(20)
    dv["Z63"] = _rolling_zscore(dv["Close"], 63)
    dv["PctRank252"] = _rolling_percentile(dv["Close"], 252)
    dv["Pct_Above_SMA10"] = (dv["Close"] - dv["SMA10"]) / dv["SMA10"] * 100

    panic_rule = (dv["PctRank252"] >= 0.85) & (dv["Z63"] >= 1.5)
    fallback_panic = (dv["Close"] > 20) & (dv["Close"] > dv["EMA10"])
    calm_rule = (dv["PctRank252"] <= 0.25) & (dv["Z63"] <= -0.5)
    fallback_calm = (dv["Close"] < 16) & (dv["Close"] < dv["EMA10"])

    raw_panic = (panic_rule.fillna(False) | fallback_panic.fillna(False)).astype(bool)
    panic_clear = (~raw_panic).astype(int).rolling(2, min_periods=2).min().fillna(0).astype(bool)
    dv["Raw_Is_Panic"] = raw_panic
    dv["Is_Panic"] = (raw_panic | (raw_panic.shift(1).fillna(False) & ~panic_clear)).fillna(False)
    dv["Is_Calm"] = calm_rule.fillna(False) | fallback_calm.fillna(False)
    dv["VIX_Regime"] = np.select(
        [dv["Is_Panic"], dv["Is_Calm"]],
        ["Stress", "Ruhig"],
        default="Neutral",
    )
    return dv

def analyze_vixy(dx):
    dx = dx.copy()
    dx["EMA10"] = _ema(dx["Close"], 10)
    dx["EMA21"] = _ema(dx["Close"], 21)
    dx["EMA50"] = _ema(dx["Close"], 50)
    dx["Ret_5d"] = dx["Close"].pct_change(5)
    dx["Ret_20d"] = dx["Close"].pct_change(20)
    dx["Z63"] = _rolling_zscore(dx["Close"], 63)
    dx["PctRank252"] = _rolling_percentile(dx["Close"], 252)

    trend_up = (dx["Close"] > dx["EMA21"]) & (dx["EMA21"] > dx["EMA21"].shift(5))
    dx["Stress_Confirmation"] = (
        ((dx["Ret_5d"] > 0.08) & (dx["PctRank252"] > 0.70) & trend_up)
        | ((dx["Ret_5d"] > 0.05) & trend_up)
    ).fillna(False)
    dx["Carry_Decay"] = ((dx["Close"] < dx["EMA21"]) & (dx["Ret_20d"] < 0)).fillna(False)
    dx["VIXY_State"] = np.select(
        [dx["Stress_Confirmation"], dx["Carry_Decay"]],
        ["Bestätigt", "Abbau"],
        default="Gemischt",
    )
    return dx

def build_volatility_dashboard(spx_df, vix_df=None, vixy_df=None):
    out = pd.DataFrame(index=spx_df.index.copy())
    out["SPX_Close"] = spx_df["Close"]
    out["SPX_Ret_5d"] = spx_df["Close"].pct_change(5)

    def _safe_bool_col(frame, col):
        # DataFrame.get(col, False) gibt einen rohen ``False`` zurück, wenn die
        # Spalte fehlt — auf einem bool ist ``.fillna`` ein AttributeError.
        if col in frame.columns:
            return frame[col].fillna(False)
        return pd.Series(False, index=frame.index)

    if vix_df is not None and len(vix_df) > 0:
        v = vix_df.reindex(out.index).ffill()
        out["VIX_Close"] = v["Close"]
        out["VIX_Ret_5d"] = v.get("Ret_5d")
        out["VIX_PctRank252"] = v.get("PctRank252")
        out["VIX_Is_Panic"] = _safe_bool_col(v, "Is_Panic")
        out["VIX_Is_Calm"] = _safe_bool_col(v, "Is_Calm")
        out["VIX_Regime"] = v["VIX_Regime"] if "VIX_Regime" in v.columns else "Neutral"
    else:
        out["VIX_Close"] = np.nan
        out["VIX_Ret_5d"] = np.nan
        out["VIX_PctRank252"] = np.nan
        out["VIX_Is_Panic"] = False
        out["VIX_Is_Calm"] = False
        out["VIX_Regime"] = "n/a"

    if vixy_df is not None and len(vixy_df) > 0:
        x = vixy_df.reindex(out.index).ffill()
        out["VIXY_Close"] = x["Close"]
        out["VIXY_Ret_5d"] = x.get("Ret_5d")
        out["VIXY_Stress_Confirmation"] = _safe_bool_col(x, "Stress_Confirmation")
        out["VIXY_Carry_Decay"] = _safe_bool_col(x, "Carry_Decay")
        out["VIXY_State"] = x["VIXY_State"] if "VIXY_State" in x.columns else "Gemischt"
    else:
        out["VIXY_Close"] = np.nan
        out["VIXY_Ret_5d"] = np.nan
        out["VIXY_Stress_Confirmation"] = False
        out["VIXY_Carry_Decay"] = False
        out["VIXY_State"] = "n/a"

    fragile_rally_warnings = pd.DataFrame(
        {
            "vixy_stress": out["VIXY_Stress_Confirmation"],
            "vix_rising": out["VIX_Ret_5d"] > 0,
            "vixy_rising_elevated_vix": (out["VIXY_Ret_5d"] > 0.03) & (out["VIX_PctRank252"] > 0.55),
        },
        index=out.index,
    ).fillna(False)
    out["Fragile_Rally_Warnings"] = fragile_rally_warnings.sum(axis=1)
    out["Fragile_Rally"] = ((out["SPX_Ret_5d"] > 0) & (out["Fragile_Rally_Warnings"] >= 2)).fillna(False)

    conditions = [
        out["VIX_Is_Panic"] & out["VIXY_Stress_Confirmation"],
        out["VIX_Is_Panic"] & ~out["VIXY_Stress_Confirmation"],
        out["Fragile_Rally"],
        out["VIX_Is_Calm"] & out["VIXY_Carry_Decay"] & (out["SPX_Ret_5d"] > 0),
    ]
    labels = [
        "Risk Off bestätigt",
        "Kurzer Volatilitätsschock",
        "Fragile Rally",
        "Risk On / ruhig",
    ]
    out["Vol_Regime"] = np.select(conditions, labels, default="Neutral")
    return out

def summarize_volatility_state(vol_df):
    latest = vol_df.iloc[-1]

    vix_regime = latest.get("VIX_Regime", "n/a")
    if vix_regime == "Stress":
        vix_tone = "#ef4444"
        vix_detail = f"VIX {latest['VIX_Close']:.1f} · erhöht gegenüber der eigenen Historie"
    elif vix_regime == "Ruhig":
        vix_tone = "#22c55e"
        vix_detail = f"VIX {latest['VIX_Close']:.1f} · wenig Angst im Optionsmarkt"
    elif pd.notna(latest.get("VIX_Close")):
        vix_tone = "#f59e0b"
        vix_detail = f"VIX {latest['VIX_Close']:.1f} · keine Extremzone"
    else:
        vix_tone = "#64748b"
        vix_detail = "Keine VIX-Daten verfügbar"

    if latest.get("VIXY_Stress_Confirmation", False):
        vixy_label = "Bestätigt"
        vixy_tone = "#ef4444"
        vixy_detail = f"VIXY {latest['VIXY_Close']:.1f} · Futures-Stress wird getragen"
    elif latest.get("VIXY_Carry_Decay", False):
        vixy_label = "Kein Stress"
        vixy_tone = "#22c55e"
        vixy_detail = f"VIXY {latest['VIXY_Close']:.1f} · eher normales Carry-Umfeld"
    elif pd.notna(latest.get("VIXY_Close")):
        vixy_label = "Gemischt"
        vixy_tone = "#f59e0b"
        vixy_detail = f"VIXY {latest['VIXY_Close']:.1f} · keine klare Bestätigung"
    else:
        vixy_label = "n/a"
        vixy_tone = "#64748b"
        vixy_detail = "Keine VIXY-Daten verfügbar"

    vol_regime = latest.get("Vol_Regime", "Neutral")
    tone_map = {
        "Risk Off bestätigt": "#ef4444",
        "Kurzer Volatilitätsschock": "#f59e0b",
        "Fragile Rally": "#f59e0b",
        "Risk On / ruhig": "#22c55e",
        "Neutral": "#64748b",
    }
    vol_tone = tone_map.get(vol_regime, "#64748b")
    if vol_regime == "Risk Off bestätigt":
        vol_detail = "VIX und VIXY ziehen gleichzeitig an"
    elif vol_regime == "Kurzer Volatilitätsschock":
        vol_detail = "VIX springt an, Futures bestätigen aber nicht voll"
    elif vol_regime == "Fragile Rally":
        vol_detail = "Aktienmarkt steigt, Volatilität bleibt aber zu fest"
    elif vol_regime == "Risk On / ruhig":
        vol_detail = "Ruhiges Umfeld mit abbauendem VIXY"
    else:
        vol_detail = "Keine klare Volatilitätslage"

    fragile = bool(latest.get("Fragile_Rally", False))
    fragile_label = "Warnung" if fragile else "Keine"
    fragile_tone = "#f59e0b" if fragile else "#22c55e"
    if fragile:
        fragile_detail = "S&P 500 steigt, aber VIX oder VIXY bleiben zu stark"
    else:
        fragile_detail = "Keine belastbare Divergenz zwischen Rally und Volatilität"

    return {
        "VIX Regime": {"status": vix_regime, "detail": vix_detail, "tone": vix_tone},
        "VIXY Bestätigung": {"status": vixy_label, "detail": vixy_detail, "tone": vixy_tone},
        "Vol Regime": {"status": vol_regime, "detail": vol_detail, "tone": vol_tone},
        "Fragile Rally": {"status": fragile_label, "detail": fragile_detail, "tone": fragile_tone},
    }

def detect_intermarket_divergence(data):
    results = []
    for name in ["S&P 500", "Nasdaq Composite", "Russell 2000"]:
        if name not in data:
            continue
        df = data[name]
        if df is None or len(df) < 5:
            continue
        close_series = pd.to_numeric(df["Close"], errors="coerce")
        high_series = pd.to_numeric(df["High"], errors="coerce")
        prev_close = close_series.shift(1)
        current_close = float(close_series.iloc[-1])
        day_pct = ((current_close / float(prev_close.iloc[-1])) - 1) * 100 if pd.notna(prev_close.iloc[-1]) and float(prev_close.iloc[-1]) != 0 else np.nan
        prev_20d_high = high_series.shift(1).rolling(20, min_periods=10).max()
        ref_high = float(prev_20d_high.iloc[-1]) if len(prev_20d_high) and pd.notna(prev_20d_high.iloc[-1]) else np.nan
        dist_to_20d_high = ((current_close / ref_high) - 1) * 100 if pd.notna(ref_high) and ref_high != 0 else np.nan
        at_20d_high = bool(pd.notna(ref_high) and current_close >= ref_high * 0.998)
        results.append({
            "name": name,
            "at_20d_high": at_20d_high,
            "pct": round(float(dist_to_20d_high), 2) if pd.notna(dist_to_20d_high) else np.nan,
            "dist_to_20d_high_pct": round(float(dist_to_20d_high), 2) if pd.notna(dist_to_20d_high) else np.nan,
            "day_pct": round(float(day_pct), 2) if pd.notna(day_pct) else np.nan,
            "ref_high": round(float(ref_high), 2) if pd.notna(ref_high) else np.nan,
        })
    return results

def detect_sector_rotation(data):
    def _p(n,d=10):
        if n not in data: return None
        df=data[n];return (df["Close"].iloc[-1]/df["Close"].iloc[-d-1]-1)*100 if len(df)>d+1 else None
    d=[("XLU",_p("XLU (Utilities)")),("XLP",_p("XLP (Consumer Staples)"))];o=[("XLK",_p("XLK (Technology)")),("XLY",_p("XLY (Consumer Discr.)"))]
    dp=[p for _,p in d if p is not None];op=[p for _,p in o if p is not None]
    if not dp or not op: return None,None,None
    ad=np.mean(dp);ao=np.mean(op);return ad>ao,{"Defensiv":d,"Offensiv":o},ad-ao

def detect_failing_rally(df):
    if len(df)<30: return None,None
    r=df.tail(60);hi=r["Close"].idxmax();hv=r.loc[hi,"Close"];ah=r.loc[hi:];
    if len(ah)<5: return None,None
    li=ah["Close"].idxmin();lv=ah.loc[li,"Close"];drop=hv-lv
    if drop/hv<0.03: return None,None
    rec=(df["Close"].iloc[-1]-lv);return round(rec/drop*100,1),round(drop/hv*100,1)

def render_ampel_section(L, history_df=None):
    """Render the Trendwende-Ampel with the current market-cycle markers."""
    phase = L["Ampel_Phase"]
    anchor = L["Anchor_Date"]
    floor = L["Floor_Mark"]
    ss_low = L["Startschuss_Low"]

    # Fallback for display: use last known cycle markers if the latest bar has blanks.
    if history_df is not None and isinstance(history_df, pd.DataFrame):
        if not anchor and "Anchor_Date" in history_df:
            anchor_candidates = history_df["Anchor_Date"].dropna()
            if len(anchor_candidates):
                anchor = anchor_candidates.iloc[-1]
        if pd.isna(floor) and "Floor_Mark" in history_df:
            floor_candidates = pd.to_numeric(history_df["Floor_Mark"], errors="coerce").dropna()
            if len(floor_candidates):
                floor = float(floor_candidates.iloc[-1])
        if pd.isna(ss_low) and "Startschuss_Low" in history_df:
            ss_candidates = pd.to_numeric(history_df["Startschuss_Low"], errors="coerce").dropna()
            if len(ss_candidates):
                ss_low = float(ss_candidates.iloc[-1])

    render_ampel(phase, anchor, floor, ss_low)

def render_check(label,ok,detail="",warn=False):
    critical_fail = (not ok) and str(label).startswith("Warnzeichen")
    cls="check-warn" if warn else ("check-ok" if ok else ("check-fail check-fail-critical" if critical_fail else "check-fail"));icon="⚠" if warn else ("✓" if ok else "✗")
    st.markdown(f'<div class="check-item"><div class="check-icon {cls}">{icon}</div><div style="flex:1;"><div style="font-size:.85rem;color:#0d1626;">{label}</div><div style="font-size:.7rem;color:#64748b;">{detail}</div></div></div>',unsafe_allow_html=True)

def _check_label(check):
    return check[0]

def _check_ok(check):
    return bool(check[1])

def _check_detail(check):
    return check[2] if len(check) > 2 else ""

def _check_warn(check):
    return bool(check[3]) if len(check) > 3 else False

def render_breadth(mode,dist_pct):
    c={"rueckenwind":"#22c55e","wachsam":"#f59e0b","schutz":"#ef4444"}.get(mode,"#64748b")
    lbl,desc={"rueckenwind":("Rückenwind","≤4%. Breite Stärke."),"wachsam":("Wachsam","4–8%. Strenger auswählen."),"schutz":("Schutz",">8%. Kapitalschutz.")}.get(mode,("—",""))
    fp=min(100,abs(dist_pct)/12*100)
    st.markdown(f'<div class="info-card" style="background:{c}12;border-color:{c}30;"><div class="breadth-card__header"><div style="width:12px;height:12px;border-radius:50%;background:{c};"></div><span style="font-weight:700;color:{c};">Modus: {lbl}</span><span style="font-size:.75rem;color:#94a3b8;">{dist_pct:.1f}% vom 52W-Hoch</span></div><div style="font-size:.75rem;color:#94a3b8;margin-bottom:8px;">{desc}</div><div class="breadth-track"><div class="breadth-fill" style="width:{fp}%;"></div></div><div class="breadth-scale"><span style="color:#22c55e;">Rückenwind</span><span style="color:#f59e0b;">Wachsam</span><span style="color:#ef4444;">Schutz</span></div></div>',unsafe_allow_html=True)


def _render_deep_analysis_content(component_bundle, sd, data):
    close_frame = component_bundle.get("close") if isinstance(component_bundle, dict) else None
    if isinstance(component_bundle, pd.DataFrame) and {"Advancers", "Decliners", "AD_Line"}.issubset(component_bundle.columns):
        br = component_bundle.copy()
    else:
        if close_frame is None or len(close_frame) <= 50:
            st.warning("Zu wenige gespeicherte Kursdaten für die Tiefenanalyse des NYSE/Nasdaq-Aktienuniversums.")
            return None
        br = compute_breadth_from_components(component_bundle)
    if br is None or len(br) <= 20:
        st.warning("Keine gültigen Handelstage gefunden.")
        return None

    last_trading_date = br.index[-1].strftime("%d.%m.%Y")
    breadth_attrs = br.attrs
    requested = breadth_attrs.get("requested_universe")
    loaded = breadth_attrs.get("loaded_universe", breadth_attrs.get("breadth_universe_loaded", len(close_frame.columns) if close_frame is not None else 0))
    coverage = float(breadth_attrs.get("coverage_ratio", 0.0) or 0.0)
    ratio_txt = f" / {requested}" if requested else ""
    st.success(f"✓ {loaded} Titel aus dem NYSE/Nasdaq-Aktienuniversum geladen{ratio_txt}, {len(br)} Handelstage · Stand: {last_trading_date}")
    if requested and loaded < requested * 0.8:
        st.warning(f"Hinweis: Es wurden nicht alle Titel des NYSE/Nasdaq-Aktienuniversums geladen. Die Tiefenanalyse läuft trotzdem mit {loaded} erfolgreich geladenen Aktien ({coverage:.0%} Abdeckung des gefundenen Universums).")
    st.plotly_chart(plot_breadth_deep(br, sd), width="stretch", config={"displayModeBar": False})

    br_valid = br.dropna(subset=["McClellan", "New_Highs"], how="all")
    if len(br_valid) == 0:
        st.warning("Keine gültigen Handelstage gefunden.")
        return br

    bL = br_valid.iloc[-1]
    bL_date = br_valid.index[-1].strftime("%d.%m.%Y")
    intraday_note = " · NH/NL auf Tageshoch/-tief" if br.attrs.get("nhnl_uses_intraday") else " · NH/NL fallback auf Schlusskurs"
    loaded_universe = br.attrs.get("breadth_universe_loaded", loaded)
    st.markdown(f'<div class="info-card"><div class="card-label">Marktbreite-Kennzahlen — NYSE/Nasdaq ({loaded_universe} Aktien) · {bL_date}{intraday_note}</div>', unsafe_allow_html=True)

    kb1, kb2, kb3, kb4, kb5 = st.columns(5)
    mc = bL["McClellan"]; nhr = bL["NH_NL_Ratio"]; nh_val = int(bL["New_Highs"]) if not np.isnan(bL["New_Highs"]) else 0; nl_val = int(bL["New_Lows"]) if not np.isnan(bL["New_Lows"]) else 0
    p50 = bL["Pct_Above_50SMA"]; p200 = bL["Pct_Above_200SMA"]; dr = bL["Deemer_Ratio"]
    try:
        p50_value = float(p50)
    except (TypeError, ValueError):
        p50_value = np.nan
    p50_available = np.isfinite(p50_value)
    p50_text = f"{p50_value:.0f}%" if p50_available else "Nicht verfügbar"
    with kb1:
        _mc_lbl = ("Extrem ↑" if mc > 125 else "Überdehnt ↑" if mc > 80 else "Impuls ↑" if mc > 50 else "Konstruktiv" if mc > 0 else "Schwach" if mc > -50 else "Impuls ↓" if mc > -80 else "Überdehnt ↓" if mc > -125 else "Extrem ↓") if not np.isnan(mc) else ""
        st.metric("McClellan Osc.", f"{mc:.1f}" if not np.isnan(mc) else "—", _mc_lbl)
    with kb2:
        st.metric("NH/NL Ratio", f"{nhr:.2f}" if not np.isnan(nhr) else f"{nh_val}/{nl_val}", f"{nh_val} Hochs / {nl_val} Tiefs")
    with kb3:
        st.metric("% > 50-SMA", p50_text if p50_available else "—", "Überhitzt" if p50_available and p50_value > 70 else "Schwach" if p50_available and p50_value < 30 else "")
    with kb4:
        st.metric("% > 200-SMA", f"{p200:.0f}%" if not np.isnan(p200) else "—")
    with kb5:
        if not np.isnan(dr):
            if dr >= 1.97:
                dr_label, dr_delta = "🚀 Sehr gut", "Breakaway Momentum!"
            elif dr >= 1.50:
                dr_label, dr_delta = f"{dr:.2f}", "Gut — konstruktiv"
            elif dr >= 1.00:
                dr_label, dr_delta = f"{dr:.2f}", "Neutral"
            else:
                dr_label, dr_delta = f"{dr:.2f}", "Schlecht — schwache Breite"
        else:
            dr_label, dr_delta = "—", ""
        st.metric("Deemer Ratio", dr_label, dr_delta)

    with st.expander("Kennzahlen der Tiefenanalyse erklärt", expanded=False):
        _render_market_glossary(["McClellan Osc.", "NH/NL Ratio", "% > 50-SMA", "% > 200-SMA", "Deemer Ratio"])

    recent_thrust = br["Breadth_Thrust"].tail(20).any()
    if recent_thrust:
        st.success("🚀 Breitenschub (Deemer Ratio > 1.97) in den letzten 20 Tagen erkannt.")

    if "S&P 500" in data:
        spx = data["S&P 500"]
        spx_at_high = spx["Close"].iloc[-1] >= spx["High"].rolling(20).max().iloc[-2] * 0.998
        ad_at_high = br["AD_Line"].iloc[-1] >= br["AD_Line"].rolling(20).max().iloc[-2] * 0.998
        if spx_at_high and not ad_at_high:
            st.warning("⚠ Divergenz: S&P 500 nahe 20T-Hoch, aber A/D-Linie nicht — Marktbreite lässt nach")
        elif spx_at_high and ad_at_high:
            st.success("✓ S&P 500 und A/D-Linie bestätigen sich — breite Beteiligung")
        render_check("Keine Divergenz Index vs. A/D-Linie", not (spx_at_high and not ad_at_high), "A/D-Linie bestätigt" if ad_at_high else "Divergenz aktiv")
        p50_divergence = spx_at_high and p50_available and p50_value < 70
        if p50_divergence:
            st.warning("⚠ Divergenz: Index nahe 20T-Hoch, aber % über 50-SMA unter 70% — nachlassende Marktbreite")
        if p50_available:
            p50_divergence_detail = f"Divergenz: {p50_value:.0f}% < 70%" if p50_divergence else f"{p50_value:.0f}% ≥ 70% — OK"
            render_check("Keine % > 50-SMA Divergenz", not p50_divergence, p50_divergence_detail)
        else:
            render_check("Keine % > 50-SMA Divergenz", False, p50_text, warn=True)
    render_check("McClellan > 0", mc > 0, f"McClellan: {mc:.1f}")
    render_check("% über 50-SMA > 70%", p50_available and p50_value > 70, p50_text)
    render_check("NH/NL Ratio > 1", nhr > 1 if not np.isnan(nhr) else False, f"Ratio: {nhr:.1f}" if not np.isnan(nhr) else "—")
    if not np.isnan(dr):
        dr_status = "Sehr gut" if dr >= 1.97 else "Gut" if dr >= 1.50 else "Neutral" if dr >= 1.00 else "Schlecht"
        render_check("Deemer Ratio ≥ 1.97 (Breakaway)", dr >= 1.97, f"Ratio: {dr:.2f} · {dr_status}")
    else:
        render_check("Deemer Ratio", False, "Nicht verfügbar")
    st.markdown("</div>", unsafe_allow_html=True)
    return br

def _x(idx): return [d.strftime("%Y-%m-%d") for d in idx]

def _y(s): return [None if pd.isna(v) else round(float(v),2) for v in s]

def plot_price(df,sd=90):
    dv=df.tail(sd);x=_x(dv.index);fig=go.Figure()
    fig.add_trace(go.Scatter(x=x,y=_y(dv["Close"]),name="Kurs",line=dict(color="#e2e8f0",width=2)))
    fig.add_trace(go.Scatter(x=x,y=_y(dv["EMA21"]),name="21-EMA",line=dict(color="#06b6d4",width=1,dash="dot")))
    fig.add_trace(go.Scatter(x=x,y=_y(dv["SMA50"]),name="50-SMA",line=dict(color="#f97316",width=1,dash="dot")))
    fig.add_trace(go.Scatter(x=x,y=_y(dv["SMA200"]),name="200-SMA",line=dict(color="#a855f7",width=1,dash="dash")))
    fl=dv["Floor_Mark"].dropna()
    if len(fl)>0: fig.add_hline(y=float(fl.iloc[-1]),line_dash="dash",line_color="#ef4444",line_width=1,annotation_text="Bodenmarke",annotation_font_color="#ef4444")
    for col,nm,clr,sym,sz in [("Is_Distribution","Dist.","#ef4444","triangle-down",7),("Is_Stall","Stau","#f59e0b","diamond",6),("Intraday_Reversal_Down","Umkehr↓","#f97316","x",8),("Intraday_Reversal_Up","Umkehr↑","#22c55e","x",8)]:
        m=dv[dv[col]==True]
        if len(m)>0: fig.add_trace(go.Scatter(x=_x(m.index),y=_y(m["Close"] if "Stall" not in nm else m["High"]),name=nm,mode="markers",marker=dict(color=clr,size=sz,symbol=sym)))
    fig.update_layout(template="plotly_white",paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(248,250,252,0)",margin=dict(l=0,r=0,t=30,b=0),height=380,legend=dict(orientation="h",yanchor="top",y=1.12,font=dict(size=9,color="#64748b")),xaxis=dict(gridcolor="rgba(100,116,139,0.12)",tickfont=dict(size=9,color="#64748b")),yaxis=dict(gridcolor="rgba(100,116,139,0.12)",tickfont=dict(size=9,color="#64748b")),hovermode="x unified")
    return fig

def plot_volume(df,sd=90):
    dv=df.tail(sd);x=_x(dv.index);colors=["#22c55e" if p>=0 else "#ef4444" for p in dv["Pct_Change"].fillna(0)]
    fig=go.Figure();fig.add_trace(go.Bar(x=x,y=_y(dv["Volume"]),marker_color=colors,opacity=0.7));fig.add_trace(go.Scatter(x=x,y=_y(dv["Vol_SMA50"]),line=dict(color="#64748b",width=1,dash="dot")))
    fig.update_layout(template="plotly_white",paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(248,250,252,0)",margin=dict(l=0,r=0,t=10,b=0),height=120,showlegend=False,xaxis=dict(gridcolor="rgba(100,116,139,0.12)",showgrid=False,tickfont=dict(size=9,color="#64748b")),yaxis=dict(gridcolor="rgba(100,116,139,0.12)",tickfont=dict(size=9,color="#64748b"),tickformat=".2s"))
    return fig

def plot_price_with_volume(df, sd=90):
    dv = df.tail(sd).copy(); x = _x(dv.index)
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.04, row_heights=[0.74, 0.26])
    fig.add_trace(go.Scatter(x=x, y=_y(dv["Close"]), name="Kurs", line=dict(color="#1d4ed8", width=2)), row=1, col=1)
    fig.add_trace(go.Scatter(x=x, y=_y(dv["EMA21"]), name="21-EMA", line=dict(color="#06b6d4", width=1, dash="dot")), row=1, col=1)
    fig.add_trace(go.Scatter(x=x, y=_y(dv["SMA50"]), name="50-SMA", line=dict(color="#f97316", width=1, dash="dot")), row=1, col=1)
    fig.add_trace(go.Scatter(x=x, y=_y(dv["SMA200"]), name="200-SMA", line=dict(color="#a855f7", width=1, dash="dash")), row=1, col=1)
    fl = dv["Floor_Mark"].dropna()
    if len(fl) > 0:
        fig.add_hline(y=float(fl.iloc[-1]), line_dash="dash", line_color="#ef4444", line_width=1, annotation_text="Bodenmarke", annotation_font_color="#ef4444", row=1, col=1)
    for col, nm, clr, sym, sz in [("Is_Distribution", "Dist.", "#ef4444", "triangle-down", 7), ("Is_Stall", "Stau", "#f59e0b", "diamond", 6), ("Intraday_Reversal_Down", "Umkehr↓", "#f97316", "x", 8), ("Intraday_Reversal_Up", "Umkehr↑", "#22c55e", "x", 8)]:
        m = dv[dv[col] == True]
        if len(m) > 0:
            fig.add_trace(go.Scatter(x=_x(m.index), y=_y(m["Close"] if "Stall" not in nm else m["High"]), name=nm, mode="markers", marker=dict(color=clr, size=sz, symbol=sym)), row=1, col=1)
    vol_colors = ["#22c55e" if p >= 0 else "#ef4444" for p in dv["Pct_Change"].fillna(0)]
    fig.add_trace(go.Bar(x=x, y=_y(dv["Volume"]), marker_color=vol_colors, opacity=0.7, name="Volumen", showlegend=False), row=2, col=1)
    fig.add_trace(go.Scatter(x=x, y=_y(dv["Vol_SMA50"]), name="Vol 50-SMA", line=dict(color="#64748b", width=1, dash="dot"), showlegend=False), row=2, col=1)
    apply_consistent_layout(fig, height=500)
    fig.update_layout(yaxis2=dict(gridcolor=CHART_COLORS["grid"], tickfont=dict(size=9, color="#64748b"), tickformat=".2s"))
    fig.update_xaxes(showgrid=False, row=1, col=1)
    fig.update_xaxes(gridcolor="rgba(100,116,139,0.12)", tickfont=dict(size=9, color="#64748b"), row=2, col=1)
    return fig

def plot_vix(dv, sd=90, title="VIX", price_color="#ef4444"):
    d = dv.tail(sd)
    x = _x(d.index)
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=x, y=_y(d["Close"]), name=title, line=dict(color=price_color, width=1.6)))
    ma_col = "EMA10" if "EMA10" in d.columns else "SMA10" if "SMA10" in d.columns else None
    ma_name = "10-EMA" if ma_col == "EMA10" else "10-SMA"
    if ma_col is not None:
        fig.add_trace(go.Scatter(x=x, y=_y(d[ma_col]), name=ma_name, line=dict(color=CHART_COLORS["secondary"], width=1, dash="dot")))
    apply_consistent_layout(fig, height=180, top_margin=10)
    return fig

def plot_volatility_combo(vix_df, vixy_df, sd=90):
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    has_vix = vix_df is not None and len(vix_df)
    has_vixy = vixy_df is not None and len(vixy_df)
    if has_vix:
        d = vix_df.tail(sd)
        fig.add_trace(go.Scatter(x=_x(d.index), y=_y(d["Close"]), name="VIX", line=dict(color=CHART_COLORS["negative"], width=1.6)), secondary_y=False)
    if has_vixy:
        d = vixy_df.tail(sd)
        fig.add_trace(go.Scatter(x=_x(d.index), y=_y(d["Close"]), name="VIXY", line=dict(color=CHART_COLORS["warning"], width=1.6)), secondary_y=True)
    apply_consistent_layout(fig, height=240, top_margin=10)
    fig.update_yaxes(title_text="VIX", secondary_y=False)
    fig.update_yaxes(title_text="VIXY", secondary_y=True, gridcolor="rgba(0,0,0,0)", tickfont=dict(size=10, color=CHART_COLORS["muted"]))
    return fig


def render_signal_card(title, status, detail, tone="#64748b"):
    st.markdown(
        f'<div class="info-card" style="height:100%; border-left:4px solid {tone};">'
        f'<div class="card-label">{title}</div>'
        f'<div style="font-size:1.0rem;font-weight:700;color:{tone};margin-bottom:8px;">{status}</div>'
        f'<div style="font-size:.78rem;color:#94a3b8;line-height:1.45;">{detail}</div>'
        f'</div>',
        unsafe_allow_html=True,
    )

def plot_breadth_deep(br,sd=90):
    """5 subplots: A/D Line, McClellan, NH/NL, % above MAs, Deemer Ratio."""
    d=br.tail(sd);x=_x(d.index)
    fig=make_subplots(rows=5,cols=1,shared_xaxes=True,vertical_spacing=0.035,
        subplot_titles=("A/D-Linie (kumulativ)","McClellan Oscillator","Neue Hochs vs. Neue Tiefs","% über gleitenden Durchschnitten","Deemer Ratio (Breitenschub)"),
        row_heights=[0.2,0.2,0.2,0.2,0.2])
    # A/D Line
    fig.add_trace(go.Scatter(x=x,y=_y(d["AD_Line"]),name="A/D-Linie",line=dict(color=CHART_COLORS["primary"],width=1.5)),row=1,col=1)
    fig.add_trace(go.Scatter(x=x,y=_y(d["AD_Line_SMA21"]),name="21-SMA",line=dict(color="#64748b",width=1,dash="dot")),row=1,col=1)
    # McClellan
    mc_colors=[("#22c55e" if v>=0 else "#ef4444") for v in d["McClellan"].fillna(0)]
    fig.add_trace(go.Bar(x=x,y=_y(d["McClellan"]),name="McClellan",marker_color=mc_colors,opacity=0.8),row=2,col=1)
    for _lvl, _clr in [(30,"#475569"),(50,"#f59e0b"),(80,"#ef4444"),(125,"#7c3aed")]:
        fig.add_hline(y=_lvl,line_dash="dot",line_color=_clr,line_width=0.5,row=2,col=1)
        fig.add_hline(y=-_lvl,line_dash="dot",line_color=_clr,line_width=0.5,row=2,col=1)
    # NH / NL
    fig.add_trace(go.Bar(x=x,y=_y(d["New_Highs"]),name="Neue Hochs",marker_color="#22c55e",opacity=0.7),row=3,col=1)
    fig.add_trace(go.Bar(x=x,y=_y(-d["New_Lows"]),name="Neue Tiefs",marker_color="#ef4444",opacity=0.7),row=3,col=1)
    # % above MAs
    fig.add_trace(go.Scatter(x=x,y=_y(d["Pct_Above_50SMA"]),name="% > 50-SMA",line=dict(color=CHART_COLORS["warning"],width=1.5)),row=4,col=1)
    fig.add_trace(go.Scatter(x=x,y=_y(d["Pct_Above_200SMA"]),name="% > 200-SMA",line=dict(color=CHART_COLORS["secondary"],width=1.5)),row=4,col=1)
    fig.add_hline(y=70,line_dash="dot",line_color="#f59e0b",line_width=0.5,row=4,col=1)
    # Deemer Ratio
    fig.add_trace(go.Scatter(x=x,y=_y(d["Deemer_Ratio"]),name="Deemer Ratio",line=dict(color=CHART_COLORS["primary"],width=1.5)),row=5,col=1)
    fig.add_hline(y=1.97,line_dash="dash",line_color="#22c55e",line_width=1,annotation_text="1.97 (Thrust)",annotation_font_color="#22c55e",annotation_font_size=9,row=5,col=1)
    fig.add_hline(y=1.0,line_dash="dot",line_color="#64748b",line_width=0.5,row=5,col=1)

    apply_consistent_layout(fig, height=750, show_legend=False)
    for i in range(1,6): fig.update_xaxes(gridcolor="rgba(100,116,139,0.12)",tickfont=dict(size=8,color="#64748b"),row=i,col=1);fig.update_yaxes(gridcolor="rgba(100,116,139,0.12)",tickfont=dict(size=8,color="#64748b"),row=i,col=1)
    for ann in fig.layout.annotations: ann.font.size=10;ann.font.color="#64748b"
    return fig

def plot_fed_rate(fed_df,sd=200):
    d=fed_df.tail(sd);x=_x(d.index);fig=go.Figure()
    fig.add_trace(go.Scatter(x=x,y=_y(d["FedRate"]),name="Fed Funds Rate",line=dict(color="#f59e0b",width=2),fill="tozeroy",fillcolor="rgba(245,158,11,0.1)"))
    apply_consistent_layout(fig, height=150, top_margin=10, show_legend=False)
    fig.update_layout(yaxis=dict(gridcolor=CHART_COLORS["grid"], tickfont=dict(size=9, color="#64748b"), title="% p.a.", title_font=dict(size=9, color="#64748b")))
    return fig


def compute_breadth_from_components(components):
    """From component price frames, compute breadth indicators daily."""
    if components is None:
        return None

    if isinstance(components, pd.DataFrame) and {"Advancers", "Decliners", "AD_Line"}.issubset(components.columns):
        return components

    if isinstance(components, dict):
        closes = components.get("close")
        highs = components.get("high")
        lows = components.get("low")
        attrs = components.get("attrs", {})
    else:
        closes = components
        highs = None
        lows = None
        attrs = {}

    if closes is None or len(closes) < 50:
        return None

    common_cols = set(closes.columns)
    if highs is not None and not highs.empty:
        common_cols &= set(highs.columns)
    if lows is not None and not lows.empty:
        common_cols &= set(lows.columns)
    common_cols = sorted(common_cols)

    if common_cols:
        closes = closes[common_cols]
        if highs is not None and not highs.empty:
            highs = highs[common_cols]
        if lows is not None and not lows.empty:
            lows = lows[common_cols]

    pct = closes.pct_change(fill_method=None)
    results = pd.DataFrame(index=closes.index)

    results["Advancers"] = (pct > 0).sum(axis=1)
    results["Decliners"] = (pct < 0).sum(axis=1)
    results["Net_Advances"] = results["Advancers"] - results["Decliners"]
    results["AD_Ratio"] = results["Advancers"] / results["Decliners"].replace(0, np.nan)
    results["AD_Line"] = results["Net_Advances"].cumsum()

    # RANA (Ratio-Adjusted Net Advances): Net Advances / Total × 1000.
    # Normiert auf Universum (~500–1500 Aktien statt NYSE ~3000+), sodass
    # die Buch-Schwellen ±70 direkt anwendbar bleiben.
    breadth_base = (results["Advancers"] + results["Decliners"]).replace(0, np.nan)
    results["RANA"] = (results["Net_Advances"] / breadth_base) * 1000.0
    results["McC_19"] = results["RANA"].ewm(span=19, adjust=False).mean()
    results["McC_39"] = results["RANA"].ewm(span=39, adjust=False).mean()
    results["McClellan"] = results["McC_19"] - results["McC_39"]

    avail = len(closes)
    nh_window = min(252, avail - 2) if avail > 22 else 20

    high_source = highs if highs is not None and not highs.empty else closes
    low_source = lows if lows is not None and not lows.empty else closes
    prev_highs = high_source.shift(1)
    prev_lows = low_source.shift(1)
    high_ref = prev_highs.rolling(nh_window, min_periods=20).max()
    low_ref = prev_lows.rolling(nh_window, min_periods=20).min()

    results["New_Highs"] = (high_source > high_ref).sum(axis=1)
    results["New_Lows"] = (low_source < low_ref).sum(axis=1)
    results["Net_New_Highs"] = results["New_Highs"] - results["New_Lows"]
    results["NH_NL_Ratio"] = results["New_Highs"] / results["New_Lows"].replace(0, np.nan)
    total_hl = (results["New_Highs"] + results["New_Lows"]).replace(0, np.nan)
    results["High_Low_Pct"] = results["New_Highs"] / total_hl * 100.0

    sma50 = closes.rolling(50, min_periods=50).mean()
    sma200 = closes.rolling(200, min_periods=200).mean()
    # Nenner symmetrisch zum Zähler: nur Ticker zählen, für die sowohl Close als
    # auch SMA verfügbar sind. Sonst senkt ein NaN-Close den Anteil künstlich.
    valid50 = (closes.notna() & sma50.notna()).sum(axis=1).replace(0, np.nan)
    valid200 = (closes.notna() & sma200.notna()).sum(axis=1).replace(0, np.nan)
    results["Pct_Above_50SMA"] = (closes > sma50).sum(axis=1) / valid50 * 100
    results["Pct_Above_200SMA"] = (closes > sma200).sum(axis=1) / valid200 * 100

    adv_10 = results["Advancers"].rolling(10).sum()
    dec_10 = results["Decliners"].rolling(10).sum()
    results["Deemer_Ratio"] = adv_10 / dec_10.replace(0, np.nan)
    results["Breadth_Thrust"] = results["Deemer_Ratio"] > 1.97

    results["AD_Line_SMA21"] = results["AD_Line"].rolling(21, min_periods=5).mean()
    results["McClellan_SMA10"] = results["McClellan"].rolling(10, min_periods=3).mean()

    for k, v in attrs.items():
        results.attrs[k] = v
    results.attrs["breadth_universe_loaded"] = int(closes.shape[1])
    results.attrs["breadth_valid_for_50sma"] = int(sma50.notna().iloc[-1].sum()) if len(sma50) else 0
    results.attrs["breadth_valid_for_200sma"] = int(sma200.notna().iloc[-1].sum()) if len(sma200) else 0
    results.attrs["nhnl_uses_intraday"] = highs is not None and lows is not None and not highs.empty and not lows.empty
    return results

# ===== From stock_analysis.py =====
def _find_row(stmt, candidates):
    """Find a row in an income statement by trying multiple name variants."""
    if stmt is None or stmt.empty: return None
    for name in candidates:
        if name in stmt.index: return stmt.loc[name]
    return None

def _quarterly_yoy_growth(qi, field, qe=None, ed=None, qraw=None):
    """YoY growth for last 3 quarters. Each quarter vs SAME quarter previous year.
    Data sources (tried in order):
    1. qraw (direct Yahoo API, may have 8+ quarters for EPS and Revenue)
    2. earnings_dates (epsActual, up to 12 quarters, EPS only)
    3. quarterly_income_stmt (usually 5 quarters max)
    Returns list of (label, growth%, flag, cur, prev) newest first."""

    def _fmt_qlabel(idx):
        if hasattr(idx, 'month'):
            return f"{idx.year} Q{(idx.month - 1) // 3 + 1}"
        return str(idx)[:7]

    def _is_same_quarter_chain(items):
        if not items:
            return False
        qset = set()
        for lbl, *_ in items:
            m = re.search(r"Q([1-4])", str(lbl))
            if not m:
                return False
            qset.add(m.group(1))
        return len(qset) == 1

    def _append_growth(acc, lbl, cur, prev):
        """Append one growth tuple with the same edge-case handling everywhere."""
        if prev < 0 and cur > 0:
            acc.append((lbl, None, "turnaround", cur, prev))
        elif prev < 0 and cur <= 0:
            acc.append((lbl, None, "still_neg", cur, prev))
        elif prev > 0 and cur < 0:
            acc.append((lbl, None, "turned_neg", cur, prev))
        elif prev == 0:
            acc.append((lbl, None, "prev_zero", cur, prev))
        else:
            g = (cur / prev - 1) * 100
            acc.append((lbl, round(g, 1), None, cur, prev))

    def _extract_yoy(vals):
        """
        Compare SAME fiscal quarter across years in a chain:
        latest Q vs same Q last year, then last year vs two years ago, etc.
        Example: Q1'26 vs Q1'25, Q1'25 vs Q1'24, Q1'24 vs Q1'23.
        """
        if len(vals) < 2:
            return []
        vals = vals.dropna().sort_index(ascending=False)
        if len(vals) < 2:
            return []

        def _parse_year_quarter(idx):
            ts = pd.to_datetime(idx, errors="coerce")
            if not pd.isna(ts):
                return int(ts.year), int(ts.quarter)
            s = str(idx)
            m = re.search(r"(20\d{2}).*Q([1-4])", s, flags=re.IGNORECASE)
            if m:
                return int(m.group(1)), int(m.group(2))
            return None

        # Build unique (year, quarter) points, keep newest value per bucket.
        bucket = {}
        ordered_keys = []
        for idx, raw_val in vals.items():
            yq = _parse_year_quarter(idx)
            if yq is None:
                continue
            try:
                v = float(raw_val)
            except Exception:
                continue
            if np.isnan(v):
                continue
            if yq not in bucket:
                bucket[yq] = v
                ordered_keys.append(yq)

        if not ordered_keys:
            return []

        # Anker = jüngstes (Jahr, Quartal) absolut, nicht „erstes eingelesenes".
        # Sonst hängt der YoY-Vergleich von der Lese-Reihenfolge des Datenproviders ab.
        _, anchor_q = max(bucket.keys(), key=lambda yq: (yq[0], yq[1]))
        same_q_points = sorted([(y, bucket[(y, q)]) for (y, q) in bucket.keys() if q == anchor_q], key=lambda t: t[0], reverse=True)
        if len(same_q_points) < 2:
            return []

        results = []
        # Safety alias: older branches used `out` as accumulator name.
        # Keep alias to prevent NameError in mixed deployments/cached code paths.
        out = results
        for i in range(min(3, len(same_q_points) - 1)):
            cur_year, cur = same_q_points[i]
            prev_year, prev = same_q_points[i + 1]
            lbl = f"{cur_year} Q{anchor_q}"
            _append_growth(results, lbl, cur, prev)
        return results

    # ── 0. Direct Yahoo API (qraw) — best source, may have 8+ quarters ──
    if qraw is not None:
        raw_map = {"eps": "DilutedEPS", "revenue": "TotalRevenue"}
        raw_key = raw_map.get(field)
        if raw_key and raw_key in qraw:
            vals = qraw[raw_key]
            if len(vals) >= 5:
                res = _extract_yoy(vals)
                if _is_same_quarter_chain(res):
                    return res

    # ── 1. earnings_dates: epsActual for up to 12 quarters (EPS only) ──
    if field == "eps" and ed is not None and not ed.empty:
        eps_col = None
        for col_name in ["Reported EPS", "EPS Actual", "epsActual", "Earnings/Share"]:
            if col_name in ed.columns: eps_col = col_name; break
        if eps_col:
            eps_data = ed[[eps_col]].copy()
            eps_data = eps_data[eps_data[eps_col].notna()]
            eps_data = eps_data[~eps_data.index.duplicated(keep='first')]
            eps_data = eps_data.sort_index(ascending=False)
            if len(eps_data) >= 5:
                res = _extract_yoy(eps_data[eps_col])
                if _is_same_quarter_chain(res):
                    return res

    # ── 2. quarterly_income_stmt (try for revenue, usually 4-5 quarters) ──
    candidates = {
        "eps": ["Diluted EPS", "Basic EPS"],
        "revenue": ["Total Revenue", "Revenue", "Operating Revenue"],
    }
    row = _find_row(qi, candidates.get(field, [field]))
    if row is not None:
        vals = row.dropna().sort_index(ascending=False)
        if len(vals) >= 5:
            res = _extract_yoy(vals)
            if _is_same_quarter_chain(res):
                return res

    return []

def _annual_yoy_growth(ai, field):
    """YoY growth for last 3 years. Returns list of (year, growth%, flag, cur, prev)."""
    candidates = {
        "eps": ["Diluted EPS","Basic EPS"],
        "revenue": ["Total Revenue","Revenue","Operating Revenue"],
    }
    row = _find_row(ai, candidates.get(field, [field]))
    if row is None: return []
    vals = row.dropna().sort_index(ascending=False)
    if len(vals) < 2: return []
    results = []
    for i in range(min(3, len(vals) - 1)):
        cur = float(vals.iloc[i]); prev = float(vals.iloc[i + 1])
        if np.isnan(prev) or np.isnan(cur): continue
        yr = vals.index[i].strftime("%Y") if hasattr(vals.index[i], 'strftime') else str(vals.index[i])[:4]
        if prev < 0 and cur > 0:
            results.append((yr, None, "turnaround", cur, prev))
        elif prev < 0 and cur <= 0:
            results.append((yr, None, "still_neg", cur, prev))
        elif prev > 0 and cur < 0:
            results.append((yr, None, "turned_neg", cur, prev))
        elif prev == 0:
            results.append((yr, None, "prev_zero", cur, prev))
        else:
            g = (cur / prev - 1) * 100
            results.append((yr, round(g, 1), None, cur, prev))
    return results

def _fmt_growth_item(item):
    """Format a single growth item (label, growth, flag, cur, prev) for display."""
    lbl, g, flag, cur, prev = item
    if flag == "turnaround":
        return f"{lbl}: Turnaround ({prev:.2f}→{cur:.2f})"
    elif flag == "still_neg":
        return f"{lbl}: noch negativ ({prev:.2f}→{cur:.2f})"
    elif flag == "turned_neg":
        return f"{lbl}: ins Negative ({prev:.2f}→{cur:.2f})"
    elif flag == "prev_zero":
        return f"{lbl}: Vorjahr war 0"
    else:
        return f"{lbl}: {g:+.0f}%"

def _check_growth_ok(items, threshold=20):
    """Check if all normal growth items meet the threshold. Turnarounds count as OK."""
    for _, g, flag, _, _ in items:
        if flag == "turnaround": continue  # turnaround is acceptable
        if flag in ("still_neg", "turned_neg"): return False
        if g is not None and g < threshold: return False
    return True

def _sum_last_4q_eps(qi):
    """Sum of the last 4 quarterly EPS values."""
    row = _find_row(qi, ["Diluted EPS","Basic EPS"])
    if row is None: return None
    vals = row.dropna().sort_index(ascending=False)
    if len(vals) < 4: return None
    return round(float(vals.iloc[:4].sum()), 2)

def _calc_cmf(df, period=20):
    h = df["High"]; l = df["Low"]; c = df["Close"]; v = df["Volume"]
    rng = h - l
    mfm = np.where(rng > 0, ((c - l) - (h - c)) / rng, 0.0)
    mfv = mfm * v
    return pd.Series(mfv, index=df.index).rolling(period).sum() / v.rolling(period).sum()

def _cmf_rating(val):
    if np.isnan(val): return "—","Nicht verfügbar","#64748b"
    if val > 0.25: return "A","Starke Akkumulation","#22c55e"
    if val > 0.10: return "B","Moderate Akkumulation","#22c55e"
    if val > -0.10: return "C","Neutral","#f59e0b"
    if val > -0.25: return "D","Moderate Distribution","#ef4444"
    return "E","Starke Distribution","#ef4444"

def _coerce_daily_series(series):
    if series is None:
        return None
    try:
        s = pd.to_numeric(series, errors="coerce")
        if not isinstance(s, pd.Series):
            s = pd.Series(s)
        idx = pd.to_datetime(s.index, errors="coerce")
        if getattr(idx, "tz", None) is not None:
            idx = idx.tz_localize(None)
        idx = pd.DatetimeIndex(idx).normalize()
        s = pd.Series(s.to_numpy(), index=idx, name=getattr(series, "name", None))
        s = s[~s.index.isna()]
        s = s[~s.index.duplicated(keep="last")].sort_index().dropna()
        return s
    except Exception as exc:
        logger.debug("_coerce_daily_series failed: %s", exc)
        return None


def _build_relative_strength_line(stock_close, benchmark_close, normalize_to=100.0):
    if stock_close is None or benchmark_close is None:
        return None
    s = _coerce_daily_series(stock_close)
    b = _coerce_daily_series(benchmark_close)
    if s is None or b is None or s.empty or b.empty:
        return None
    common = s.index.intersection(b.index)
    if len(common) < 60:
        return None
    s_common = s.reindex(common)
    b_common = b.reindex(common)
    rs = (s_common / b_common).replace([np.inf, -np.inf], np.nan).dropna()
    if rs.empty:
        return None
    if normalize_to is not None:
        base = rs.iloc[0]
        if pd.notna(base) and base != 0:
            rs = rs / base * float(normalize_to)
    rs.name = "RS-Linie"
    return rs

@st.cache_data(ttl=1800, show_spinner=False)
def load_cached_universe_closes_for_rs(lookback_days=400):
    try:
        store = _get_price_store()
        _init_price_cache_db(store)
        tickers = _load_cached_universe_members(store, CACHE_UNIVERSE_NAME)
        if not tickers:
            tickers = get_app_stock_universe_tickers()
        tickers = [str(t).strip().upper() for t in tickers if t]
        if not tickers:
            return None
        end = pd.Timestamp.utcnow().normalize()
        start = end - pd.Timedelta(days=lookback_days)
        bundle = _read_cached_price_bundle(store, tickers, start.date().isoformat(), end.date().isoformat())
        closes = bundle.get("close") if bundle else None
        if closes is None or closes.empty:
            return None
        closes = closes.sort_index()
        closes = closes.loc[:, ~closes.columns.duplicated()].apply(pd.to_numeric, errors="coerce")
        min_obs = min(220, max(120, len(closes) // 2))
        valid_cols = [c for c in closes.columns if closes[c].notna().sum() >= min_obs]
        if valid_cols:
            closes = closes[valid_cols]
        return closes if not closes.empty else None
    except Exception as exc:
        logger.debug("load_cached_universe_closes_for_rs failed: %s", exc)
        return None


@st.cache_data(ttl=1800, show_spinner=False)
def load_cached_universe_rs_scores(lookback_days=400):
    try:
        store = _get_price_store()
        _init_price_cache_db(store)
        meta = _get_cache_metadata_many(store, [RS_SNAPSHOT_KEY, RS_SNAPSHOT_AT_KEY])
        scores = _deserialize_series_snapshot(meta.get(RS_SNAPSHOT_KEY, ""))
        if scores is not None and len(scores) >= 100:
            return scores

        tickers = _load_cached_universe_members(store, CACHE_UNIVERSE_NAME)
        if not tickers:
            tickers = get_app_stock_universe_tickers()
        tickers = [str(t).strip().upper() for t in tickers if t]
        if not tickers:
            return None

        end = pd.Timestamp.utcnow().normalize()
        start = end - pd.Timedelta(days=lookback_days)
        bundle = _read_cached_price_bundle(store, tickers, start.date().isoformat(), end.date().isoformat())
        closes = bundle.get("close") if bundle else None
        scores = _refresh_relative_strength_snapshot_from_closes(store, closes, source="lazy-rs-rebuild", lookback_days=lookback_days)
        return scores if scores is not None and len(scores) else None
    except Exception as exc:
        logger.debug("load_cached_universe_rs_scores failed: %s", exc)
        return None


def _weighted_rs_score(rs_line, windows=((63, 0.4), (126, 0.2), (189, 0.2), (252, 0.2))):
    if rs_line is None:
        return None
    series = pd.to_numeric(rs_line, errors="coerce").dropna()
    if len(series) < 80:
        return None
    score = 0.0
    weight_sum = 0.0
    last = series.iloc[-1]
    for lookback, weight in windows:
        if len(series) <= lookback:
            continue
        prev = series.iloc[-lookback - 1]
        if pd.isna(prev) or prev == 0:
            continue
        score += ((last / prev) - 1.0) * weight
        weight_sum += weight
    if weight_sum == 0:
        return None
    return score / weight_sum


def _weighted_rs_scores_for_frame(ratio_frame, windows=((63, 0.4), (126, 0.2), (189, 0.2), (252, 0.2))):
    if ratio_frame is None or ratio_frame.empty:
        return pd.Series(dtype=float)
    frame = ratio_frame.replace([np.inf, -np.inf], np.nan)
    last = frame.iloc[-1]
    score = pd.Series(0.0, index=frame.columns, dtype=float)
    weight_sum = pd.Series(0.0, index=frame.columns, dtype=float)
    for lookback, weight in windows:
        if len(frame) <= lookback:
            continue
        prev = frame.iloc[-lookback - 1]
        comp = (last / prev) - 1.0
        valid = prev.notna() & last.notna() & (prev != 0)
        score.loc[valid] = score.loc[valid] + comp.loc[valid] * weight
        weight_sum.loc[valid] = weight_sum.loc[valid] + weight
    out = score / weight_sum.replace(0, np.nan)
    return out.replace([np.inf, -np.inf], np.nan).dropna()


def _calc_rs_rating(stock_close, benchmark_close, universe_closes=None, universe_scores=None):
    raw_rs = _build_relative_strength_line(stock_close, benchmark_close, normalize_to=None)
    plot_rs = _build_relative_strength_line(stock_close, benchmark_close, normalize_to=100.0)
    payload = {
        "rating": None,
        "score": None,
        "method": "unavailable",
        "universe_size": 0,
        "rs_line": plot_rs,
        "rs_line_raw": raw_rs,
        "ema21": None,
        "sma50": None,
        "sma200": None,
        "above_21": None,
        "above_50": None,
        "above_200": None,
        "trend_5w": None,
        "trend_13w": None,
        "near_high_52w": None,
        "new_high_52w": None,
        "distance_to_high_pct": None,
        "excess_return_3m": None,
        "excess_return_6m": None,
        "excess_return_12m": None,
        "benchmark_close": _coerce_daily_series(benchmark_close),
    }
    if raw_rs is None or raw_rs.empty:
        return payload

    payload["score"] = _weighted_rs_score(raw_rs)
    rs_line = plot_rs
    ema21 = rs_line.ewm(span=21).mean() if rs_line is not None else None
    sma50 = rs_line.rolling(50).mean() if rs_line is not None else None
    sma200 = rs_line.rolling(200).mean() if rs_line is not None else None
    payload["ema21"] = ema21
    payload["sma50"] = sma50
    payload["sma200"] = sma200

    if rs_line is not None and len(rs_line) > 0:
        last_rs = rs_line.iloc[-1]
        if ema21 is not None and len(ema21) > 0 and pd.notna(ema21.iloc[-1]):
            payload["above_21"] = bool(last_rs > ema21.iloc[-1])
        if sma50 is not None and len(sma50) > 0 and pd.notna(sma50.iloc[-1]):
            payload["above_50"] = bool(last_rs > sma50.iloc[-1])
        if sma200 is not None and len(sma200) > 0 and pd.notna(sma200.iloc[-1]):
            payload["above_200"] = bool(last_rs > sma200.iloc[-1])
        if len(rs_line) > 25 and pd.notna(rs_line.iloc[-26]):
            payload["trend_5w"] = bool(last_rs > rs_line.iloc[-26])
        if len(rs_line) > 65 and pd.notna(rs_line.iloc[-66]):
            payload["trend_13w"] = bool(last_rs > rs_line.iloc[-66])
        if len(raw_rs) > 63 and pd.notna(raw_rs.iloc[-64]) and raw_rs.iloc[-64] != 0:
            payload["excess_return_3m"] = float((raw_rs.iloc[-1] / raw_rs.iloc[-64] - 1) * 100)
        if len(raw_rs) > 126 and pd.notna(raw_rs.iloc[-127]) and raw_rs.iloc[-127] != 0:
            payload["excess_return_6m"] = float((raw_rs.iloc[-1] / raw_rs.iloc[-127] - 1) * 100)
        if len(raw_rs) > 252 and pd.notna(raw_rs.iloc[-253]) and raw_rs.iloc[-253] != 0:
            payload["excess_return_12m"] = float((raw_rs.iloc[-1] / raw_rs.iloc[-253] - 1) * 100)
        high_52w = rs_line.rolling(252, min_periods=50).max().iloc[-1] if len(rs_line) >= 50 else np.nan
        if pd.notna(high_52w) and high_52w != 0:
            payload["distance_to_high_pct"] = float((last_rs / high_52w - 1) * 100)
            payload["near_high_52w"] = bool(last_rs >= high_52w * 0.97)
            payload["new_high_52w"] = bool(last_rs >= high_52w * 0.999)

    cached_scores = universe_scores if isinstance(universe_scores, pd.Series) else pd.Series(dtype=float)
    universe_scores = cached_scores.dropna() if isinstance(cached_scores, pd.Series) else pd.Series(dtype=float)
    if universe_scores.empty and universe_closes is not None and not universe_closes.empty:
        closes = universe_closes.copy()
        try:
            closes.index = pd.to_datetime(closes.index, errors="coerce")
            if getattr(closes.index, "tz", None) is not None:
                closes.index = closes.index.tz_localize(None)
            closes.index = pd.DatetimeIndex(closes.index).normalize()
            closes = closes[~closes.index.isna()]
            closes = closes.loc[~closes.index.duplicated(keep="last")]
            closes = closes.sort_index().apply(pd.to_numeric, errors="coerce")
        except Exception as exc:
            logger.debug("universe RS index normalization failed: %s", exc)
            closes = None
        bench = _coerce_daily_series(benchmark_close)
        if closes is not None and bench is not None and not closes.empty and not bench.empty:
            common = closes.index.intersection(bench.index)
            if len(common) >= 120:
                bench = bench.reindex(common)
                closes = closes.reindex(common)
                ratio_frame = closes.div(bench, axis=0).replace([np.inf, -np.inf], np.nan)
                min_obs = min(220, max(120, len(ratio_frame) // 2))
                ratio_frame = ratio_frame.loc[:, ratio_frame.notna().sum() >= min_obs]
                universe_scores = _weighted_rs_scores_for_frame(ratio_frame)

    payload["universe_size"] = int(len(universe_scores))
    score = payload["score"]
    if score is not None and len(universe_scores) >= 100:
        ranked = pd.concat([universe_scores, pd.Series({"__TARGET__": score})])
        pct_rank = float(ranked.rank(pct=True, method="average").loc["__TARGET__"])
        payload["rating"] = int(np.clip(round(pct_rank * 99), 1, 99))
        payload["method"] = "universe_percentile"
    elif score is not None:
        proxy = 50 + score * 500
        payload["rating"] = int(np.clip(round(proxy), 1, 99))
        payload["method"] = "weighted_proxy"

    return payload


def _atr_category(pct):
    if np.isnan(pct): return "—","#64748b"
    if pct <= 2.5: return "Ruhig","#22c55e"
    if pct <= 4.0: return "Lebhaft","#06b6d4"
    if pct <= 8.0: return "Stürmisch","#f59e0b"
    return "Explosiv","#ef4444"

@st.cache_data(ttl=3600, show_spinner=False)
def _load_institutional_13f_trends():
    path = Path(RS_OUTPUT_DIR_NAME) / INSTITUTIONAL_13F_OUTPUT_FILE_NAME
    try:
        if not path.exists():
            return {}
        payload = json.loads(path.read_text(encoding="utf-8"))
        tickers = payload.get("tickers", {}) if isinstance(payload, dict) else {}
        return tickers if isinstance(tickers, dict) else {}
    except Exception as exc:
        logger.debug("13F trend load failed: %s", exc)
        return {}


def _institutional_13f_trend_for(ticker):
    ticker = str(ticker or "").strip().upper()
    if not ticker:
        return None
    trend = _load_institutional_13f_trends().get(ticker)
    return trend if isinstance(trend, dict) else None


def _fmt_int_de(value):
    try:
        return f"{int(value):,}".replace(",", ".")
    except Exception:
        return "0"


def _fmt_delta_de(value):
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return ""
    try:
        number = int(value)
    except Exception:
        return ""
    return f"{number:+,}".replace(",", ".")


def _optional_int(value):
    if value is None or value == "":
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    try:
        return int(value)
    except Exception:
        return None


def _institutional_13f_check(record):
    large = _optional_int(record.get("large_holder_count")) or 0
    large_delta = _optional_int(record.get("large_holder_delta"))
    holder_count = _optional_int(record.get("holder_count")) or 0
    holder_delta = _optional_int(record.get("holder_count_delta"))
    period = record.get("period") or ""
    previous_period = record.get("previous_period") or ""
    trend = record.get("trend") or "neutral"

    negative = (large_delta is not None and large_delta < 0) or large < 5
    mixed = (not negative) and trend == "neutral"
    positive = not negative and not mixed
    parts = [f"Große Institutionen: {_fmt_int_de(large)}"]
    if large_delta is not None:
        parts[-1] += f" ({_fmt_delta_de(large_delta)} vs. Vorquartal)"
    holder_detail = f"alle 13F-Halter: {_fmt_int_de(holder_count)}"
    if holder_delta is not None:
        holder_detail += f" ({_fmt_delta_de(holder_delta)})"
    parts.append(holder_detail)
    if previous_period:
        parts.append(f"{previous_period} → {period}")
    elif period:
        parts.append(period)
    if trend in {"positive", "negative", "neutral", "new"}:
        parts.append({"positive": "Trend positiv", "negative": "Trend negativ", "neutral": "Trend stabil", "new": "neu in 13F"}[trend])
    return positive, " · ".join(parts), mixed


def evaluate_fundamentals(info, qi, ai, ih, qe=None, ed=None, qraw=None, fmp_err=None, ticker=None):
    checks = []
    def _g(k, d=None):
        if not info:
            return d
        aliases = {
            "returnOnEquity": ("returnOnEquity", "return_on_equity"),
            "heldPercentInstitutions": ("heldPercentInstitutions", "held_percent_institutions", "heldByInstitutions", "institutionPercentHeld"),
            "profitMargins": ("profitMargins", "profit_margins"),
        }
        for key in aliases.get(k, (k,)):
            v = info.get(key, None)
            if v is not None:
                return v
        return v if v is not None else d

    # ── Debug: show data availability ──
    src_info = []
    if qraw is not None:
        source_prefix = "FMP"
        note_hint = ""
        if fmp_err:
            note_text = str(fmp_err)
            if note_text in ("FMP stable", "FMP legacy"):
                source_prefix = "FMP"
            elif note_text == "SEC ergänzt":
                source_prefix = "SEC"
            elif "SEC ergänzt" in note_text and ("FMP stable" in note_text or "FMP legacy" in note_text):
                source_prefix = "Quartalsdaten"
                note_hint = note_text
            elif "SEC ergänzt" in note_text and "FMP" not in note_text:
                source_prefix = "SEC"
                note_hint = note_text
            elif "SEC ergänzt" in note_text and "FMP" in note_text:
                source_prefix = "Quartalsdaten"
                note_hint = note_text
            elif note_text.startswith("SEC "):
                source_prefix = "SEC"
            else:
                source_prefix = "Quartalsdaten"
                note_hint = note_text
        for key, series in qraw.items():
            src_info.append(f"{source_prefix} {key}: {len(series)}Q")
        if note_hint:
            src_info.append(f"Hinweis: {note_hint}")
    elif fmp_err:
        src_info.append(f"FMP: {fmp_err}")
    else:
        fmp_avail = False
        try: fmp_avail = bool(st.secrets["FMP_API_KEY"])
        except:
            try: fmp_avail = bool(st.secrets.get("FMP_API_KEY", ""))
            except: pass
        if not fmp_avail: src_info.append("FMP: kein API-Key in Secrets")
    if qi is not None and not qi.empty:
        eps_row = _find_row(qi, ["Diluted EPS","Basic EPS"])
        rev_row = _find_row(qi, ["Total Revenue","Revenue","Operating Revenue"])
        n_eps = len(eps_row.dropna()) if eps_row is not None else 0
        n_rev = len(rev_row.dropna()) if rev_row is not None else 0
        src_info.append(f"Yahoo: {n_eps}Q EPS, {n_rev}Q Rev")
    if ed is not None and not ed.empty:
        for col in ["Reported EPS","EPS Actual","epsActual"]:
            if col in ed.columns:
                n = ed[col].notna().sum()
                src_info.append(f"earnings_dates: {n}Q")
                break
    data_note = " · ".join(src_info) if src_info else "Keine Quartalsdaten"
    checks.append(("Datenquellen", bool(src_info), data_note))

    # ── EPS: quarterly YoY (3 quarters) ──
    epsg = _quarterly_yoy_growth(qi, "eps", qe=qe, ed=ed, qraw=qraw)
    if epsg:
        details = " → ".join(_fmt_growth_item(item) for item in epsg)
        all_ok = _check_growth_ok(epsg, threshold=20)
        checks.append((f"EPS ≥20% YoY ({len(epsg)}Q)", all_ok, details))
    else:
        eq = _g("earningsQuarterlyGrowth")
        if eq is not None:
            checks.append(("EPS ≥20% YoY (letztes Q)", eq*100 >= 20, f"{eq*100:+.1f}%"))
        else:
            checks.append(("EPS ≥20% YoY", False, "Keine Quartalsdaten"))

    # ── EPS acceleration ──
    if len(epsg) >= 2:
        # Extract only the normal growth rates (skip turnarounds etc. for acceleration check)
        normal_rates = [(lbl, g) for lbl, g, flag, _, _ in epsg if flag is None and g is not None]
        if len(normal_rates) >= 2:
            accel_count = sum(1 for i in range(len(normal_rates)-1) if normal_rates[i][1] > normal_rates[i+1][1])
            full_accel = accel_count == len(normal_rates) - 1
            details = " → ".join(f"{g:+.0f}%" for _, g in reversed(normal_rates))
            checks.append(("EPS-Beschleunigung", full_accel,
                           f"Verlauf: {details}" + (" (bei Beschl. ab 13% akzeptabel)" if full_accel and normal_rates[0][1] < 20 else "")))
        else:
            checks.append(("EPS-Beschleunigung", False, "Nicht verfügbar"))
    else:
        checks.append(("EPS-Beschleunigung", False, "Nicht verfügbar"))

    # ── EPS: annual 3-year ──
    epsg_ann = _annual_yoy_growth(ai, "eps")
    if epsg_ann and len(epsg_ann) >= 2:
        details = " · ".join(_fmt_growth_item(item) for item in epsg_ann)
        all_ok = _check_growth_ok(epsg_ann, threshold=20)
        checks.append((f"Jährl. EPS ≥20% ({len(epsg_ann)} Jahre)", all_ok, details))
    else:
        eg = _g("earningsGrowth")
        if eg is not None:
            checks.append(("Jährl. EPS-Wachstum ≥20%", eg*100 >= 20, f"{eg*100:+.1f}% (nur 1 Jahr verfügbar)"))
        else:
            checks.append(("Jährl. EPS-Wachstum ≥20%", False, "Nicht verfügbar"))

    # ── Sum of last 4 quarterly EPS > 0 ──
    eps_sum = _sum_last_4q_eps(qi)
    if eps_sum is not None:
        checks.append(("Summe letzte 4 Quartals-EPS > 0", eps_sum > 0, f"${eps_sum:.2f}"))
    else:
        te = _g("trailingEps")
        if te is not None:
            checks.append(("Trailing EPS > 0 (Proxy für 4Q-Summe)", te > 0, f"${te:.2f}"))
        else:
            checks.append(("Trailing EPS > 0 (Proxy für 4Q-Summe)", False, "Nicht verfügbar"))

    # ── Revenue: quarterly YoY (3 quarters) ──
    revg = _quarterly_yoy_growth(qi, "revenue", qe=qe, ed=ed, qraw=qraw)
    if revg:
        details = " → ".join(_fmt_growth_item(item) for item in revg)
        all_ok = _check_growth_ok(revg, threshold=20)
        checks.append((f"Umsatz ≥20% YoY ({len(revg)}Q)", all_ok, details))
    else:
        rg = _g("revenueGrowth")
        if rg is not None:
            checks.append(("Umsatz ≥20% YoY", rg*100 >= 20, f"{rg*100:+.1f}% (nur jährlich verfügbar)"))
        else:
            checks.append(("Umsatz ≥20% YoY", False, "Keine Quartalsdaten"))

    # ── Revenue acceleration (Bonus) ──
    if len(revg) >= 2:
        normal_rev = [(lbl, g) for lbl, g, flag, _, _ in revg if flag is None and g is not None]
        if len(normal_rev) >= 2:
            accel = normal_rev[0][1] > normal_rev[1][1]
            checks.append(("Umsatz-Beschleunigung (Bonus)", accel,
                           f"{normal_rev[1][1]:+.0f}% → {normal_rev[0][1]:+.0f}%"))
        else:
            checks.append(("Umsatz-Beschleunigung (Bonus)", False, "Nicht verfügbar"))
    else:
        checks.append(("Umsatz-Beschleunigung (Bonus)", False, "Nicht verfügbar"))

    # ── Revenue: annual 3-year ──
    revg_ann = _annual_yoy_growth(ai, "revenue")
    if revg_ann and len(revg_ann) >= 2:
        details = " · ".join(_fmt_growth_item(item) for item in revg_ann)
        all_ok = _check_growth_ok(revg_ann, threshold=20)
        checks.append((f"Jährl. Umsatz ≥20% ({len(revg_ann)}J)", all_ok, details))
    else:
        rg = _g("revenueGrowth")
        if rg is not None:
            checks.append(("Jährl. Umsatzwachstum ≥20%", rg*100 >= 20, f"{rg*100:+.1f}%"))
        else:
            checks.append(("Jährl. Umsatzwachstum ≥20%", False, "Nicht verfügbar"))

    # ── ROE ≥ 17% ──
    roe = _g("returnOnEquity")
    # Fallback 1: FMP ratios-TTM (already fetched alongside income statement)
    if roe is None and qraw is not None:
        roe = qraw.get("_roe_ttm")
    # Fallback 2: compute from annual income stmt + bookValue/sharesOutstanding
    if roe is None and ai is not None and not ai.empty and info:
        try:
            ni_row = _find_row(ai, ["Net Income", "NetIncome", "Net Income Common Stockholders"])
            ni_series = ni_row.dropna().sort_index(ascending=False) if ni_row is not None else None
            net_income = float(ni_series.iloc[0]) if ni_series is not None and len(ni_series) else np.nan
            book_value = info.get("bookValue")
            shares_out = info.get("sharesOutstanding")
            if book_value is not None and shares_out is not None:
                equity = float(book_value) * float(shares_out)
                if pd.notna(net_income) and equity > 0:
                    roe = net_income / equity
        except Exception:
            pass
    # Fallback 3: compute TTM from SEC NetIncome / StockholdersEquity (no API key needed)
    if roe is None and qraw is not None:
        try:
            ni_q = qraw.get("NetIncome")
            eq_q = qraw.get("StockholdersEquity")
            if ni_q is not None and eq_q is not None and len(ni_q) >= 1 and len(eq_q) >= 1:
                n = min(4, len(ni_q))
                ni_ttm = ni_q.head(n).sum()
                equity_val = float(eq_q.iloc[0])
                if equity_val > 0:
                    roe = ni_ttm / equity_val
        except Exception:
            pass
    if roe is not None:
        checks.append(("ROE ≥17%", roe*100 >= 17, f"{roe*100:.1f}%"))
    else:
        checks.append(("ROE ≥17%", False, "Nicht verfügbar"))

    # ── Institutional holders: 13F trend without fund names ──
    institutional_13f = _institutional_13f_trend_for(ticker)
    inst_pct = _g("heldPercentInstitutions")
    if institutional_13f:
        ok, detail, warn = _institutional_13f_check(institutional_13f)
        checks.append(("Institutionelle Unterstützung", ok, detail, warn))
    elif ih is not None and not ih.empty:
        n_holders = len(ih)
        total_pct = inst_pct * 100 if inst_pct else 0
        checks.append(("Institutionelle Unterstützung", n_holders >= 5,
                       f"{n_holders} Top-Institutionen · {total_pct:.1f}% inst. gehalten"))
    elif inst_pct is not None:
        checks.append(("Institutionelle Beteiligung", inst_pct * 100 > 20,
                       f"{inst_pct*100:.1f}% inst. gehalten (Detailliste nicht verfügbar)"))
    else:
        float_shares = _g("floatShares")
        shares_out = _g("sharesOutstanding")
        if float_shares is not None and shares_out is not None and float(shares_out) > 0:
            free_float = float(float_shares) / float(shares_out)
            checks.append(("Institutionelle Unterstützung", free_float < 0.9,
                           f"Proxy Free-Float: {free_float*100:.1f}% (Institutionen-Datenfeed fehlt)"))
        else:
            checks.append(("Institutionelle Unterstützung", False, "Keine belastbaren Institutionendaten im aktuellen Datenlauf"))

    # ── Profit margin ──
    pm = _g("profitMargins")
    # Fallback 1: FMP ratios-TTM
    if pm is None and qraw is not None:
        pm = qraw.get("_pm_ttm")
    # Fallback 2: compute TTM from quarterly net income / revenue (FMP or SEC)
    if pm is None and qraw is not None:
        try:
            ni_q = qraw.get("NetIncome")
            rev_q = qraw.get("TotalRevenue")
            if ni_q is not None and rev_q is not None and len(ni_q) >= 1 and len(rev_q) >= 1:
                n = min(4, len(ni_q), len(rev_q))
                ni_ttm = ni_q.head(n).sum()
                rev_ttm = rev_q.head(n).sum()
                if rev_ttm != 0:
                    pm = ni_ttm / rev_ttm
        except Exception:
            pass
    if pm is not None:
        checks.append(("Gewinnmarge positiv", pm > 0, f"{pm*100:.1f}%"))
    else:
        checks.append(("Gewinnmarge positiv", False, "Nicht verfügbar"))

    return checks

def evaluate_technicals(df, info, spx_df=None, rs_ctx=None, rs_universe_scores=None):
    checks = []; L = df.iloc[-1]; price = L["Close"]

    checks.append(("Preis ≥ $15", price >= 15, f"${price:,.2f}"))

    h52 = df["High"].rolling(252, min_periods=20).max().iloc[-1]
    if not np.isnan(h52):
        d = (price / h52 - 1) * 100
        checks.append(("Nahe am 52W-Hoch", d > -10, f"{d:+.1f}% vom Hoch (${h52:,.2f})"))
    else:
        checks.append(("Nahe am 52W-Hoch", False, "Nicht verfügbar"))

    avg_v = df["Volume"].tail(20).mean(); dol_v = avg_v * price / 1e6
    if pd.notna(dol_v):
        checks.append(("Dollar-Volumen ≥ $30 Mio.", dol_v >= 30, f"${dol_v:,.0f} Mio./Tag"))
    else:
        checks.append(("Dollar-Volumen ≥ $30 Mio.", False, "Nicht verfügbar"))

    pc = df["Close"].pct_change(fill_method=None)
    uv = df["Volume"].where(pc > 0).tail(50).sum(); dv = df["Volume"].where(pc < 0).tail(50).sum()
    if dv > 0:
        udv = uv / dv
        checks.append(("Up/Down Vol. Ratio ≥1.0", udv >= 1.0, f"{udv:.2f}" + (" (ideal ≥1.1)" if udv >= 1.1 else "")))
    else:
        checks.append(("Up/Down Vol. Ratio ≥1.0", False, "Nicht verfügbar"))

    rs_ctx = rs_ctx or _calc_rs_rating(
        df["Close"],
        spx_df["Close"] if spx_df is not None else None,
        universe_scores=rs_universe_scores,
    )
    rs = rs_ctx.get("rating") if isinstance(rs_ctx, dict) else None
    if isinstance(rs_ctx, dict):
        method = rs_ctx.get("method", "unavailable")
        method_note = (
            "Universums-Ranking" if method == "universe_percentile"
            else "CSV-Rating" if method == "external_csv"
            else "Fallback-Proxy" if method == "weighted_proxy"
            else ""
        )
        universe_note = f" · {rs_ctx.get('universe_size', 0)} Aktien" if rs_ctx.get("universe_size") else ""
        if rs is not None:
            lbl = "Elite" if rs >= 90 else "Stark" if rs >= 80 else "Meiden (<70)" if rs < 70 else "OK"
            checks.append(("RS-Bewertung ≥80", rs >= 80, f"RS: {rs} ({lbl})" + (f" · {method_note}{universe_note}" if method_note else "")))
            checks.append(("RS-Bewertung ≥90", rs >= 90, f"Aktuell {rs}"))
        else:
            checks.append(("RS-Bewertung ≥80", False, "Nicht verfügbar"))
            checks.append(("RS-Bewertung ≥90", False, "Nicht verfügbar"))

        rs_line = rs_ctx.get("rs_line")
        ema21_rs = rs_ctx.get("ema21")
        sma50_rs = rs_ctx.get("sma50")
        if rs_line is not None and len(rs_line) > 0:
            rs_now = rs_line.iloc[-1]
            if ema21_rs is not None and len(ema21_rs) > 0 and pd.notna(ema21_rs.iloc[-1]):
                checks.append(("RS-Linie über 21-EMA", bool(rs_ctx.get("above_21")), f"{rs_now:.2f} vs {ema21_rs.iloc[-1]:.2f}"))
            else:
                checks.append(("RS-Linie über 21-EMA", False, "Nicht verfügbar"))
            if sma50_rs is not None and len(sma50_rs) > 0 and pd.notna(sma50_rs.iloc[-1]):
                checks.append(("RS-Linie über 50-SMA", bool(rs_ctx.get("above_50")), f"{rs_now:.2f} vs {sma50_rs.iloc[-1]:.2f}"))
            else:
                checks.append(("RS-Linie über 50-SMA", False, "Nicht verfügbar"))
            if rs_ctx.get("trend_5w") is not None:
                ex3 = rs_ctx.get("excess_return_3m")
                detail = f"Excess 3M: {ex3:+.1f}%" if ex3 is not None else "letzte 5 Wochen"
                checks.append(("RS-Linie steigt über 5 Wochen", bool(rs_ctx.get("trend_5w")), detail))
            else:
                checks.append(("RS-Linie steigt über 5 Wochen", False, "Nicht verfügbar"))
            if rs_ctx.get("trend_13w") is not None:
                ex6 = rs_ctx.get("excess_return_6m")
                detail = f"Excess 6M: {ex6:+.1f}%" if ex6 is not None else "letzte 13 Wochen"
                checks.append(("RS-Linie steigt über 13 Wochen", bool(rs_ctx.get("trend_13w")), detail))
            else:
                checks.append(("RS-Linie steigt über 13 Wochen", False, "Nicht verfügbar"))
            if rs_ctx.get("distance_to_high_pct") is not None:
                dist = rs_ctx.get("distance_to_high_pct")
                detail = "Neues RS-Hoch" if rs_ctx.get("new_high_52w") else f"{dist:+.1f}% zum RS-Hoch"
                checks.append(("RS-Linie nahe 52W-Hoch", bool(rs_ctx.get("near_high_52w")), detail))
            else:
                checks.append(("RS-Linie nahe 52W-Hoch", False, "Nicht verfügbar"))
        else:
            checks.append(("RS-Linie über 21-EMA", False, "Nicht verfügbar"))
            checks.append(("RS-Linie über 50-SMA", False, "Nicht verfügbar"))
            checks.append(("RS-Linie steigt über 5 Wochen", False, "Nicht verfügbar"))
            checks.append(("RS-Linie steigt über 13 Wochen", False, "Nicht verfügbar"))
            checks.append(("RS-Linie nahe 52W-Hoch", False, "Nicht verfügbar"))
    elif spx_df is not None and len(df) >= 126 and len(spx_df) >= 126:
        sp = (df["Close"].iloc[-1] / df["Close"].iloc[-126] - 1) * 100
        mp = (spx_df["Close"].iloc[-1] / spx_df["Close"].iloc[-126] - 1) * 100
        checks.append(("Relative Stärke vs. S&P (6M)", sp > mp, f"Aktie: {sp:+.1f}% · S&P: {mp:+.1f}% · Diff: {sp-mp:+.1f}%"))
    else:
        checks.append(("RS-Bewertung ≥80", False, "Nicht verfügbar"))
        checks.append(("RS-Bewertung ≥90", False, "Nicht verfügbar"))
        checks.append(("RS-Linie über 21-EMA", False, "Nicht verfügbar"))
        checks.append(("RS-Linie über 50-SMA", False, "Nicht verfügbar"))
        checks.append(("RS-Linie steigt über 5 Wochen", False, "Nicht verfügbar"))
        checks.append(("RS-Linie steigt über 13 Wochen", False, "Nicht verfügbar"))
        checks.append(("RS-Linie nahe 52W-Hoch", False, "Nicht verfügbar"))

    cmf = _calc_cmf(df, 20); cmf_val = cmf.iloc[-1] if len(cmf) > 0 else np.nan
    rat, meaning, _ = _cmf_rating(cmf_val)
    if pd.notna(cmf_val):
        checks.append(("CMF Rating A oder B", rat in ("A","B"), f"CMF: {cmf_val:+.3f} → {rat} ({meaning})"))
    else:
        checks.append(("CMF Rating A oder B", False, "Nicht verfügbar"))

    e21 = df["Close"].ewm(span=21).mean().iloc[-1]
    # min_periods explizit: bei kurzer Historie liefert .rolling(200) sonst stets NaN
    # (Default min_periods == window) und der gesamte 200er-Block bleibt unbewertet.
    s10 = df["Close"].rolling(10, min_periods=10).mean().iloc[-1]
    s50 = df["Close"].rolling(50, min_periods=50).mean().iloc[-1]
    s200 = df["Close"].rolling(200, min_periods=200).mean().iloc[-1] if len(df) >= 200 else float("nan")

    for nm, mv in [("10-SMA", s10), ("21-EMA", e21), ("50-SMA", s50), ("200-SMA", s200)]:
        if not np.isnan(mv):
            checks.append((f"Kurs über {nm}", price > mv, f"{price:,.2f} vs {mv:,.2f}"))
        else:
            checks.append((f"Kurs über {nm}", False, "Nicht verfügbar"))

    if not any(np.isnan(x) for x in [e21, s50, s200]):
        checks.append(("MA-Ordnung (21>50>200)", e21 > s50 > s200, f"21:{e21:,.0f} · 50:{s50:,.0f} · 200:{s200:,.0f}"))
    else:
        checks.append(("MA-Ordnung (21>50>200)", False, "Nicht verfügbar"))

    for nm, mv, thresh in [("10-SMA", s10, 10.0), ("21-EMA", e21, 14.0), ("50-SMA", s50, 25.0), ("200-SMA", s200, 70.0)]:
        if not np.isnan(mv):
            dist = (price / mv - 1) * 100
            extended = dist > thresh or dist < -thresh
            checks.append((f"Abstand {nm} (<{thresh:.0f}%)", not extended,
                           f"{dist:+.1f}% ({'überdehnt' if dist > 0 else 'darunter'}, Schwelle: ±{thresh:.0f}%)"))
        else:
            checks.append((f"Abstand {nm} (<{thresh:.0f}%)", False, "Nicht verfügbar"))

    return checks, cmf_val, rs_ctx

def _technical_points_score(technical_checks, rs_rating, cmf_value):
    check_map = {label: bool(ok) for label, ok, _ in technical_checks}
    score = 0.0
    # Summe aller maximal erreichbaren Einzelpunkte: 5+5+5+10+15+10+10+5+10+5+15 = 95.
    # Mit dieser Bezugsgröße kann ein perfektes Setup tatsächlich 100/100 erreichen.
    max_score = 95.0

    score += 5 if check_map.get("Preis ≥ $15", False) else 0
    score += 5 if check_map.get("Nahe am 52W-Hoch", False) else 0
    score += 5 if check_map.get("Dollar-Volumen ≥ $30 Mio.", False) else 0
    score += 10 if check_map.get("Up/Down Vol. Ratio ≥1.0", False) else 0

    rs_val = _safe_float(rs_rating, np.nan)
    if pd.notna(rs_val) and rs_val >= 80:
        # Kontinuierliche, monotone Staffel: RS 80 → 1 Pkt, RS 90 → 8 Pkt, RS 100 → 15 Pkt.
        score += float(np.clip((rs_val - 80) * 0.7 + 1.0, 1.0, 15.0))

    score += 10 if check_map.get("RS-Linie über 21-EMA", False) else 0
    score += 10 if check_map.get("RS-Linie über 50-SMA", False) else 0
    score += 5 if check_map.get("RS-Linie steigt über 5 Wochen", False) else 0
    score += 10 if check_map.get("RS-Linie steigt über 13 Wochen", False) else 0
    score += 5 if check_map.get("RS-Linie nahe 52W-Hoch", False) else 0

    rat, _, _ = _cmf_rating(cmf_value)
    # A = Starke Akkumulation (bestes Rating), B = Moderate Akkumulation.
    if rat == "A":
        score += 15
    elif rat == "B":
        score += 10

    return round(float(np.clip(score / max_score * 100.0, 0, 100)), 1)


def _fundamental_checklist_score_100(
    fundamentals_checks,
    q_earnings_growth,
    q_revenue_growth,
    earnings_growth,
    revenue_growth,
    roe,
    profit_margin,
):
    # Jedes Kern-Kriterium wird über Präfix-Matches gegen das tatsächlich von
    # evaluate_fundamentals erzeugte Label abgeglichen. evaluate_fundamentals
    # hängt je nach Datenlage Suffixe wie "(2Q)", "(3 Jahre)", "(letztes Q)"
    # an oder benutzt Fallback-Labels (z.B. "Trailing EPS > 0 …").
    criteria_specs = [
        ("EPS ≥20% YoY",                ("EPS ≥20% YoY",)),
        ("EPS-Beschleunigung",          ("EPS-Beschleunigung",)),
        ("Jährl. EPS-Wachstum ≥20%",    ("Jährl. EPS ≥20%", "Jährl. EPS-Wachstum")),
        ("Summe 4Q EPS > 0",            ("Summe letzte 4 Quartals-EPS > 0", "Trailing EPS > 0")),
        ("Umsatz ≥20% YoY",             ("Umsatz ≥20% YoY",)),
        ("Umsatz-Beschleunigung",       ("Umsatz-Beschleunigung",)),
        ("Jährl. Umsatzwachstum ≥20%", ("Jährl. Umsatz ≥20%", "Jährl. Umsatzwachstum")),
        ("ROE ≥17%",                    ("ROE ≥17%",)),
        ("Gewinnmarge positiv",         ("Gewinnmarge positiv",)),
    ]
    check_pairs = [(_check_label(check), _check_ok(check)) for check in (fundamentals_checks or [])]

    def _criterion_met(prefixes):
        for label, ok in check_pairs:
            if any(label.startswith(prefix) for prefix in prefixes):
                return bool(ok)
        return False

    unit = 100.0 / len(criteria_specs)

    def _tiered_growth_points(value, minimum=0.20, stretch=0.60):
        v = _safe_float(value, np.nan)
        if pd.isna(v) or v < minimum:
            return 0.0
        return unit * min((v - minimum) / max(stretch - minimum, 1e-9), 1.0)

    score = 0.0
    # 1) EPS YoY (Q): mehr Wachstum = mehr Punkte
    score += _tiered_growth_points(q_earnings_growth, minimum=0.20, stretch=0.80)
    # 2) EPS-Beschleunigung
    score += unit if _criterion_met(criteria_specs[1][1]) else 0.0
    # 3) Jährl. EPS-Wachstum
    score += _tiered_growth_points(earnings_growth, minimum=0.20, stretch=0.60)
    # 4) Summe 4Q EPS > 0 (oder Trailing-EPS-Fallback)
    score += unit if _criterion_met(criteria_specs[3][1]) else 0.0
    # 5) Umsatz YoY (Q)
    score += _tiered_growth_points(q_revenue_growth, minimum=0.20, stretch=0.60)
    # 6) Umsatz-Beschleunigung
    score += unit if _criterion_met(criteria_specs[5][1]) else 0.0
    # 7) Jährl. Umsatz-Wachstum
    score += _tiered_growth_points(revenue_growth, minimum=0.20, stretch=0.50)
    # 8) ROE
    score += _tiered_growth_points(roe, minimum=0.17, stretch=0.35)
    # 9) Gewinnmarge
    score += _tiered_growth_points(profit_margin, minimum=0.00, stretch=0.25) if _safe_float(profit_margin, np.nan) > 0 else 0.0

    met = sum(1 for _, prefixes in criteria_specs if _criterion_met(prefixes))
    return round(float(np.clip(score, 0, 100)), 1), met, len(criteria_specs)


def _weekly_ohlc(df):
    if df is None or df.empty:
        return pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume"])
    weekly = pd.DataFrame({
        "Open": df["Open"].resample("W-FRI").first(),
        "High": df["High"].resample("W-FRI").max(),
        "Low": df["Low"].resample("W-FRI").min(),
        "Close": df["Close"].resample("W-FRI").last(),
        "Volume": df["Volume"].resample("W-FRI").sum() if "Volume" in df.columns else np.nan,
    })
    return weekly.dropna(subset=["Open", "High", "Low", "Close"])


def _find_local_pivots(series, kind="low", left=3, right=3, min_separation=5):
    s = pd.to_numeric(series, errors="coerce").dropna()
    if len(s) < left + right + 3:
        return []
    values = s.to_numpy(dtype=float)
    idx = s.index
    pivots = []
    last_i = None
    for i in range(left, len(s) - right):
        current = values[i]
        prev_vals = values[i - left:i]
        next_vals = values[i + 1:i + 1 + right]
        if np.isnan(current) or np.isnan(prev_vals).any() or np.isnan(next_vals).any():
            continue
        if kind == "low":
            is_pivot = current <= prev_vals.min() and current <= next_vals.min() and (current < prev_vals.min() or current < next_vals.min())
        else:
            is_pivot = current >= prev_vals.max() and current >= next_vals.max() and (current > prev_vals.max() or current > next_vals.max())
        if not is_pivot:
            continue
        if last_i is not None and i - last_i < min_separation:
            prev_idx, prev_val = pivots[-1]
            if (kind == "low" and current < prev_val) or (kind == "high" and current > prev_val):
                pivots[-1] = (idx[i], float(current))
                last_i = i
            continue
        pivots.append((idx[i], float(current)))
        last_i = i
    return pivots


def _window_extreme(series, center_label, kind="low", radius=5):
    s = pd.to_numeric(series, errors="coerce").dropna()
    if s.empty:
        return None
    try:
        loc = s.index.get_loc(center_label)
        if isinstance(loc, slice):
            loc = loc.start
        elif isinstance(loc, np.ndarray):
            loc = int(loc[0])
    except KeyError:
        arr = s.index.get_indexer([center_label], method="nearest")
        if len(arr) == 0 or arr[0] == -1:
            return None
        loc = int(arr[0])
    start = max(0, loc - radius)
    end = min(len(s), loc + radius + 1)
    window = s.iloc[start:end].dropna()
    if window.empty:
        return None
    return float(window.min() if kind == "low" else window.max())


def _detect_recent_engulfing(df, lookback=15):
    out = {"bullish": None, "bearish": None}
    if df is None or len(df) < 2:
        return out

    # Keep the original engulfing logic but classify candle color with a tiny
    # tolerance so near-flat candles are not mislabeled as green/red.
    color_eps_pct = 0.0005  # 0.05%

    def _candle_dir(row):
        o = float(row["Open"])
        c = float(row["Close"])
        up = o * (1.0 + color_eps_pct)
        down = o * (1.0 - color_eps_pct)
        if c > up:
            return 1
        if c < down:
            return -1
        return 0

    start = max(1, len(df) - lookback)
    for i in range(start, len(df)):
        prev = df.iloc[i - 1]
        cur = df.iloc[i]
        date = pd.Timestamp(df.index[i]).strftime("%d.%m.%Y")

        prev_dir = _candle_dir(prev)
        cur_dir = _candle_dir(cur)
        prev_red = prev_dir == -1
        prev_green = prev_dir == 1
        cur_green = cur_dir == 1
        cur_red = cur_dir == -1

        prev_body = abs(prev["Close"] - prev["Open"])
        cur_body = abs(cur["Close"] - cur["Open"])

        if prev_red and cur_green and cur["Open"] <= prev["Close"] and cur["Close"] >= prev["Open"] and cur_body >= prev_body * 0.9:
            out["bullish"] = (date, f"{date} · Körper umschließt den roten Vortag")
        if prev_green and cur_red and cur["Open"] >= prev["Close"] and cur["Close"] <= prev["Open"] and cur_body >= prev_body * 0.9:
            out["bearish"] = (date, f"{date} · Körper umschließt den grünen Vortag")
    return out


def _detect_recent_outside_day(df, lookback=15):
    out = {"bullish": None, "bearish": None}
    if df is None or len(df) < 2:
        return out
    start = max(1, len(df) - lookback)
    for i in range(start, len(df)):
        prev = df.iloc[i - 1]
        cur = df.iloc[i]
        rng = cur["High"] - cur["Low"]
        if not pd.notna(rng) or rng <= 0:
            continue
        if cur["High"] > prev["High"] and cur["Low"] < prev["Low"]:
            close_pos = (cur["Close"] - cur["Low"]) / rng
            date = pd.Timestamp(df.index[i]).strftime("%d.%m.%Y")
            if cur["Close"] > cur["Open"] and cur["Close"] >= prev["Close"] and close_pos >= 0.6:
                out["bullish"] = (date, f"{date} · Outside Day mit Schluss im oberen Bereich ({close_pos:.0%})")
            elif cur["Close"] < cur["Open"] and cur["Close"] <= prev["Close"] and close_pos <= 0.4:
                out["bearish"] = (date, f"{date} · Outside Day mit Schluss im unteren Bereich ({close_pos:.0%})")
    return out


def _detect_inside_week(df):
    weekly = _weekly_ohlc(df)
    if len(weekly) < 2:
        return None
    prev = weekly.iloc[-2]
    cur = weekly.iloc[-1]
    if cur["High"] <= prev["High"] and cur["Low"] >= prev["Low"]:
        width = ((cur["High"] - cur["Low"]) / cur["Close"] * 100) if cur["Close"] else np.nan
        date = pd.Timestamp(weekly.index[-1]).strftime("KW bis %d.%m.%Y")
        detail = f"{date} · Wochenrange {width:.1f}% innerhalb der Vorwoche" if pd.notna(width) else f"{date} · Range innerhalb der Vorwoche"
        return {"date": weekly.index[-1], "detail": detail}
    return None


def _detect_price_rs_divergence(
    price_series,
    benchmark_series,
    rs_series=None,
    lookback=126,
    recent_bars=35,
    pivot_span=3,
    min_market_move_pct=0.75,
    flat_tolerance_pct=1.0,
    outperformance_gap_pct=3.0,
):
    if price_series is None or benchmark_series is None:
        return {"positive": None, "negative": None}

    price = _coerce_daily_series(price_series)
    bench = _coerce_daily_series(benchmark_series)
    rs = _coerce_daily_series(rs_series) if rs_series is not None else _build_relative_strength_line(price, bench, normalize_to=None)
    if price is None or bench is None or rs is None or price.empty or bench.empty or rs.empty:
        return {"positive": None, "negative": None}

    common = price.index.intersection(bench.index).intersection(rs.index)
    if len(common) < 40:
        return {"positive": None, "negative": None}

    price = price.reindex(common).tail(lookback)
    bench = bench.reindex(price.index)
    rs = rs.reindex(price.index)
    valid = price.notna() & bench.notna() & rs.notna()
    price = price.loc[valid]
    bench = bench.loc[valid]
    rs = rs.loc[valid]
    if len(price) < 40:
        return {"positive": None, "negative": None}

    def _loc(index, label):
        try:
            loc = index.get_loc(label)
            if isinstance(loc, slice):
                return int(loc.start)
            if isinstance(loc, np.ndarray):
                return int(loc[0]) if len(loc) else None
            return int(loc)
        except Exception:
            return None

    def _strength_positive(stock_change_pct, market_change_pct, rs_change_pct):
        rel_gap = stock_change_pct - market_change_pct
        if stock_change_pct >= -flat_tolerance_pct and rs_change_pct > 0:
            return "stark", "Markt macht tieferes Tief, Aktie bestätigt es nicht"
        if rel_gap >= outperformance_gap_pct and rs_change_pct > 0:
            return "mittel", "Aktie fällt deutlich weniger als der Markt"
        if market_change_pct <= -min_market_move_pct and rel_gap > 0 and rs_change_pct > 0:
            return "schwach", "Aktie hält sich leicht besser als der Markt"
        return None, None

    def _strength_negative(stock_change_pct, market_change_pct, rs_change_pct):
        rel_gap = stock_change_pct - market_change_pct
        if stock_change_pct <= flat_tolerance_pct and rs_change_pct < 0:
            return "stark", "Markt macht höheres Hoch, Aktie bestätigt es nicht"
        if rel_gap <= -outperformance_gap_pct and rs_change_pct < 0:
            return "mittel", "Aktie steigt deutlich schwächer als der Markt"
        if market_change_pct >= min_market_move_pct and rel_gap < 0 and rs_change_pct < 0:
            return "schwach", "Aktie läuft dem Markt nur leicht hinterher"
        return None, None

    def _score(rel_gap, rs_change_pct, market_move_pct):
        return round(max(0.0, abs(rel_gap)) * 1.5 + max(0.0, abs(rs_change_pct)) * 2.0 + max(0.0, abs(market_move_pct)) * 0.5, 1)

    def _make_candidate(kind, d1, d2, pivot_label, stock_change_pct, market_change_pct, rs_change_pct, strength, summary):
        rel_gap = stock_change_pct - market_change_pct
        score = _score(rel_gap, rs_change_pct, market_change_pct)
        detail = (
            f"{pd.Timestamp(d1).strftime('%d.%m.')} → {pd.Timestamp(d2).strftime('%d.%m.%Y')}"
            f" · {pivot_label} · {strength.capitalize()} · {summary}"
        )
        return {
            "date": d2,
            "kind": kind,
            "strength": strength,
            "score": score,
            "detail": detail,
            "price_change_pct": float(stock_change_pct),
            "benchmark_change_pct": float(market_change_pct),
            "rs_change_pct": float(rs_change_pct),
            "excess_change_pct": float(rel_gap),
            "stock_1": float(price.loc[d1]),
            "stock_2": float(price.loc[d2]),
            "benchmark_1": float(bench.loc[d1]),
            "benchmark_2": float(bench.loc[d2]),
            "rs_1": float(rs.loc[d1]),
            "rs_2": float(rs.loc[d2]),
        }

    def _choose_better(existing, candidate):
        if candidate is None:
            return existing
        if existing is None:
            return candidate
        if candidate["date"] > existing["date"]:
            return candidate
        if candidate["date"] == existing["date"] and candidate.get("score", 0) > existing.get("score", 0):
            return candidate
        return existing

    positive_candidate = None
    negative_candidate = None

    low_pivots = _find_local_pivots(bench, kind="low", left=pivot_span, right=pivot_span, min_separation=7)
    high_pivots = _find_local_pivots(bench, kind="high", left=pivot_span, right=pivot_span, min_separation=7)

    for pivots, pivot_kind in ((low_pivots, "Markt-Tiefs"), (high_pivots, "Markt-Hochs")):
        if len(pivots) < 2:
            continue
        for i in range(1, len(pivots)):
            d1, _ = pivots[i - 1]
            d2, _ = pivots[i]
            loc2 = _loc(price.index, d2)
            if loc2 is None or loc2 < len(price) - recent_bars:
                continue
            p1, p2 = price.loc[d1], price.loc[d2]
            b1, b2 = bench.loc[d1], bench.loc[d2]
            r1, r2 = rs.loc[d1], rs.loc[d2]
            if any(pd.isna(x) for x in [p1, p2, b1, b2, r1, r2]) or p1 == 0 or b1 == 0 or r1 == 0:
                continue
            stock_change_pct = float((p2 / p1 - 1.0) * 100.0)
            market_change_pct = float((b2 / b1 - 1.0) * 100.0)
            rs_change_pct = float((r2 / r1 - 1.0) * 100.0)

            if pivot_kind == "Markt-Tiefs":
                if market_change_pct <= -min_market_move_pct and rs_change_pct > 0:
                    strength, summary = _strength_positive(stock_change_pct, market_change_pct, rs_change_pct)
                    if strength is not None:
                        candidate = _make_candidate("positive", d1, d2, pivot_kind, stock_change_pct, market_change_pct, rs_change_pct, strength, summary)
                        positive_candidate = _choose_better(positive_candidate, candidate)
            else:
                if market_change_pct >= min_market_move_pct and rs_change_pct < 0:
                    strength, summary = _strength_negative(stock_change_pct, market_change_pct, rs_change_pct)
                    if strength is not None:
                        candidate = _make_candidate("negative", d1, d2, pivot_kind, stock_change_pct, market_change_pct, rs_change_pct, strength, summary)
                        negative_candidate = _choose_better(negative_candidate, candidate)

    return {"positive": positive_candidate, "negative": negative_candidate}


def evaluate_chart_signs(df, rs_ctx=None):
    signs = {"positiv": [], "negativ": [], "neutral": []}
    if len(df) < 50: return signs
    c = df["Close"]; h = df["High"]; l = df["Low"]; o = df["Open"]; v = df["Volume"]
    pct = c.pct_change(fill_method=None); vol_avg = v.rolling(50).mean()
    ema21 = c.ewm(span=21).mean(); sma50 = c.rolling(50).mean(); sma200 = c.rolling(200).mean()
    rng = h - l; cr_s = pd.Series(np.where(rng > 0, (c - l) / rng, 0.5), index=df.index)
    engulf = _detect_recent_engulfing(df, lookback=15)
    outside = _detect_recent_outside_day(df, lookback=15)
    inside_week = _detect_inside_week(df)
    benchmark_close = rs_ctx.get("benchmark_close") if isinstance(rs_ctx, dict) else None
    rs_line_raw = rs_ctx.get("rs_line_raw") if isinstance(rs_ctx, dict) else None
    divergence = _detect_price_rs_divergence(c, benchmark_close, rs_series=rs_line_raw, lookback=126, recent_bars=35, pivot_span=3, min_market_move_pct=0.75, flat_tolerance_pct=1.0, outperformance_gap_pct=3.0) if benchmark_close is not None else {"positive": None, "negative": None}

    t20 = df.tail(20)
    uhv = ((t20["Close"] > t20["Close"].shift(1)) & (t20["Volume"] > vol_avg.tail(20))).sum()
    dhv = ((t20["Close"] < t20["Close"].shift(1)) & (t20["Volume"] > vol_avg.tail(20))).sum()
    if uhv > dhv: signs["positiv"].append(("Mehr Gewinn- als Verlusttage mit hohem Vol.", f"{uhv} vs {dhv} (20T)"))
    elif dhv > uhv: signs["negativ"].append(("Mehr Verlust- als Gewinntage mit hohem Vol.", f"{dhv} vs {uhv} (20T)"))

    a21 = (c.tail(10) > ema21.tail(10)).sum(); a50 = (c.tail(10) > sma50.tail(10)).sum()
    if a21 >= 8 and a50 >= 8: signs["positiv"].append(("Leben über den Durchschnitten", f"{a21}/10 über 21-EMA, {a50}/10 über 50-SMA"))
    elif a21 <= 2 or a50 <= 2: signs["negativ"].append(("Leben unter den Durchschnitten", f"{a21}/10 über 21-EMA, {a50}/10 über 50-SMA"))

    e21 = ema21.iloc[-1]; s50 = sma50.iloc[-1]; s200 = sma200.iloc[-1]
    if not any(np.isnan(x) for x in [e21, s50, s200]):
        if e21 > s50 > s200: signs["positiv"].append(("Durchschnitte in richtiger Ordnung", "21>50>200"))
        elif e21 < s50 < s200: signs["negativ"].append(("Durchschnitte in falscher Ordnung", "21<50<200"))
    if len(ema21) >= 10:
        eu = ema21.iloc[-1] > ema21.iloc[-10]; su = sma50.iloc[-1] > sma50.iloc[-10] if not np.isnan(sma50.iloc[-10]) else None
        if eu and su: signs["positiv"].append(("Nach oben zeigende Durchschnittslinien", ""))
        elif not eu and su is False: signs["negativ"].append(("Nach unten zeigende Durchschnittslinien", ""))

    gu = ((o.tail(20) > h.shift(1).tail(20)) & (v.tail(20) > vol_avg.tail(20))).sum()
    gd = ((o.tail(20) < l.shift(1).tail(20)) & (v.tail(20) > vol_avg.tail(20))).sum()
    if gu > 0: signs["positiv"].append(("Positive Kurslücken", f"{gu} in 20T"))
    if gd > 0: signs["negativ"].append(("Negative Kurslücken bei hohem Vol.", f"{gd} in 20T"))

    drops = pct.tail(20) < -0.005
    dl = (drops & (v.tail(20) < vol_avg.tail(20) * 0.8)).sum()
    dhh = (drops & (v.tail(20) > vol_avg.tail(20) * 1.2)).sum()
    if dl >= 3: signs["positiv"].append(("Preisrückgänge bei niedrigem Vol.", f"{dl} Tage"))
    if dhh >= 3: signs["negativ"].append(("Preisrückgänge bei hohem Vol.", f"{dhh} Tage"))
    rises = pct.tail(20) > 0.005
    rh = (rises & (v.tail(20) > vol_avg.tail(20) * 1.2)).sum()
    if rh >= 3: signs["positiv"].append(("Preissteigerungen bei hohem Vol.", f"{rh} in 20T"))

    stall = ((pct.tail(10) >= 0) & (pct.tail(10) < 0.005) & (v.tail(10) >= v.shift(1).tail(10) * 0.95) & (cr_s.tail(10) < 0.5)).sum()
    if stall >= 2: signs["negativ"].append(("Stau-Tage", f"{stall} in 10T"))

    ur = ((o.tail(10) < c.shift(1).tail(10)) & (c.tail(10) > o.tail(10)) & (cr_s.tail(10) > 0.7)).sum()
    dr2 = ((o.tail(10) > c.shift(1).tail(10)) & (c.tail(10) < o.tail(10)) & (cr_s.tail(10) < 0.3)).sum()
    if ur >= 2: signs["positiv"].append(("Upside Reversals", f"{ur} in 10T"))
    if dr2 >= 2: signs["negativ"].append(("Downside Reversals", f"{dr2} in 10T"))

    if isinstance(rs_ctx, dict):
        rs_line = rs_ctx.get("rs_line")
        sma50_rs = rs_ctx.get("sma50")
        rating = rs_ctx.get("rating")
        if rs_line is not None and len(rs_line) > 0:
            rs_now = rs_line.iloc[-1]
            if rs_ctx.get("trend_5w"):
                signs["positiv"].append(("RS-Linie steigt", "über 5 Wochen"))
            elif rs_ctx.get("trend_5w") is False:
                signs["negativ"].append(("RS-Linie fällt", "über 5 Wochen"))
            if rs_ctx.get("above_21") and rs_ctx.get("above_50"):
                signs["positiv"].append(("RS-Linie über ihren Durchschnitten", f"{rs_now:.2f} über 21-EMA und 50-SMA"))
            elif rs_ctx.get("above_50") is False and sma50_rs is not None and len(sma50_rs) > 0 and pd.notna(sma50_rs.iloc[-1]):
                signs["negativ"].append(("RS-Linie unter 50-SMA", f"{rs_now:.2f} vs {sma50_rs.iloc[-1]:.2f}"))
            if rs_ctx.get("new_high_52w"):
                signs["positiv"].append(("RS-Linie auf neuem 52W-Hoch", "Marktführerschaft bestätigt"))
            elif rs_ctx.get("near_high_52w"):
                dist = rs_ctx.get("distance_to_high_pct")
                signs["neutral"].append(("RS-Linie knapp unter Hoch", f"{dist:+.1f}% zum 52W-Hoch"))
            elif rs_ctx.get("distance_to_high_pct") is not None and rs_ctx.get("distance_to_high_pct") <= -10:
                signs["negativ"].append(("RS-Linie deutlich unter Hoch", f"{rs_ctx.get('distance_to_high_pct'):+.1f}%"))
        if rating is not None:
            if rating >= 90:
                signs["positiv"].append(("RS-Rating im Elite-Bereich", f"RS {rating}"))
            elif rating < 70:
                signs["negativ"].append(("Schwaches RS-Rating", f"RS {rating}"))

    if engulf.get("bullish"):
        signs["positiv"].append(("Bullish Engulfing", engulf["bullish"][1]))
    if engulf.get("bearish"):
        signs["negativ"].append(("Bearish Engulfing", engulf["bearish"][1]))
    if outside.get("bullish"):
        signs["positiv"].append(("Positiver Outside Day", outside["bullish"][1]))
    if outside.get("bearish"):
        signs["negativ"].append(("Negativer Outside Day", outside["bearish"][1]))
    if inside_week is not None:
        signs["neutral"].append(("Inside Week", inside_week["detail"]))
    if divergence.get("positive"):
        det = divergence["positive"]
        signs["positiv"].append(("Positive Divergenz", det["detail"] + f" · Aktie {det['price_change_pct']:+.1f}% vs Markt {det['benchmark_change_pct']:+.1f}% · RS {det['rs_change_pct']:+.1f}% · Relativ {det['excess_change_pct']:+.1f}% · Score {det['score']:.1f}"))
    if divergence.get("negative"):
        det = divergence["negative"]
        signs["negativ"].append(("Negative Divergenz", det["detail"] + f" · Aktie {det['price_change_pct']:+.1f}% vs Markt {det['benchmark_change_pct']:+.1f}% · RS {det['rs_change_pct']:+.1f}% · Relativ {det['excess_change_pct']:+.1f}% · Score {det['score']:.1f}"))

    avg_cr = cr_s.tail(5).mean()
    if avg_cr > 0.6: signs["positiv"].append(("Schlussposition obere 40%", f"Ø {avg_cr:.0%}"))
    elif avg_cr < 0.25: signs["negativ"].append(("Tiefe Schlussposition", f"Ø {avg_cr:.0%}"))

    if not np.isnan(s50):
        d50 = (c.iloc[-1] / s50 - 1) * 100
        if d50 > 15: signs["negativ"].append(("Großer Abstand zu Durchschnitten", f"{d50:+.1f}% zur 50-SMA"))

    wc = c.resample("W-FRI").last().dropna()
    if len(wc) >= 6 and (wc.pct_change(fill_method=None).tail(5) > 0).all():
        signs["positiv"].append(("5 positive Wochen in Folge", ""))

    if not np.isnan(s50) and abs(c.iloc[-1] / s50 - 1) < 0.01: signs["neutral"].append(("Rückkehr zur 50-Tage-Linie", ""))
    if h.iloc[-1] <= h.iloc[-2] and l.iloc[-1] >= l.iloc[-2]: signs["neutral"].append(("Inside Day", ""))
    r5 = (h.tail(5).max() - l.tail(5).min()) / c.iloc[-1] * 100
    if r5 < 3: signs["neutral"].append(("Enge Konsolidierung", f"5T-Range: {r5:.1f}%"))
    if not np.isnan(e21) and abs(l.iloc[-1] - e21) / e21 < 0.005: signs["neutral"].append(("Test der 21-EMA", ""))
    if not np.isnan(s50) and abs(l.iloc[-1] - s50) / s50 < 0.005: signs["neutral"].append(("Test der 50-SMA", ""))

    # ─── Buch Kap. 4.8 ergänzende Chart-Signs ───

    # POSITIV: Downside Reversal mit starkem Folgetag (Buch Kap. 4.8 positiv #9)
    # Sucht in den letzten 15 Tagen ein Downside Reversal (Open>Close des Vortags,
    # Close<Open, Schluss im unteren Drittel), gefolgt von einem starken Folgetag
    # (Schluss >+1.5% UND Schlusskurs in oberer Tageshälfte).
    try:
        look_dr = min(15, len(df) - 2)
        for i in range(2, look_dr + 1):
            idx_dr = -i
            idx_next = -i + 1
            o_dr, c_dr = o.iloc[idx_dr], c.iloc[idx_dr]
            o_prev, c_prev = o.iloc[idx_dr - 1], c.iloc[idx_dr - 1]
            h_dr, l_dr = h.iloc[idx_dr], l.iloc[idx_dr]
            rng_dr = h_dr - l_dr
            if rng_dr <= 0:
                continue
            cr_dr = (c_dr - l_dr) / rng_dr
            is_downside_rev = (o_dr > c_prev) and (c_dr < o_dr) and (cr_dr < 0.3)
            if not is_downside_rev:
                continue
            c_next, o_next, h_next, l_next = c.iloc[idx_next], o.iloc[idx_next], h.iloc[idx_next], l.iloc[idx_next]
            rng_next = h_next - l_next
            cr_next = (c_next - l_next) / rng_next if rng_next > 0 else 0.5
            ret_next = (c_next / c_dr - 1) * 100
            if ret_next >= 1.5 and cr_next >= 0.5:
                signs["positiv"].append(("Downside Reversal mit starkem Folgetag", f"Tag -{i}: {ret_next:+.1f}%"))
                break
    except Exception:
        pass

    # POSITIV: Shake-out (Buch Kap. 4.8 positiv #17)
    # Definition: Intraday-Tief unterbietet ein lokales Tief der letzten 20 Tage,
    # aber Schlusskurs liegt wieder ÜBER diesem Tief, Schluss in oberer Tageshälfte,
    # und Volumen erhöht (>= Durchschnitt).
    try:
        if len(df) >= 21:
            last_h = h.iloc[-1]
            last_l = l.iloc[-1]
            last_c = c.iloc[-1]
            last_v = v.iloc[-1]
            rng_last = last_h - last_l
            cr_last = (last_c - last_l) / rng_last if rng_last > 0 else 0.5
            prior_low = l.iloc[-21:-1].min()
            vol_avg_20 = v.iloc[-21:-1].mean()
            if (
                pd.notna(prior_low) and pd.notna(vol_avg_20) and vol_avg_20 > 0
                and last_l < prior_low
                and last_c > prior_low
                and cr_last >= 0.5
                and last_v >= vol_avg_20
            ):
                signs["positiv"].append(("Shake-out", f"Tief unter Vor-20T-Tief, Schluss wieder darüber"))
    except Exception:
        pass

    # NEGATIV: Beschleunigte Verluste (Buch Kap. 4.5)
    # Drei aufeinanderfolgende Verlusttage mit zunehmenden Verlusten,
    # mindestens der letzte ≤ -2%.
    try:
        if len(pct) >= 3:
            r1, r2, r3 = pct.iloc[-3] * 100, pct.iloc[-2] * 100, pct.iloc[-1] * 100
            if r1 < 0 and r2 < 0 and r3 < 0 and r3 < r2 < r1 and r3 <= -2.0:
                signs["negativ"].append(("Beschleunigte Verluste", f"{r1:+.1f}% → {r2:+.1f}% → {r3:+.1f}%"))
    except Exception:
        pass

    # NEUTRAL: Natürliche Reaktion (Buch Kap. 4.7)
    # Rücksetzer von 8-12% vom 20T-Hoch, intakter Trend (Kurs noch > 50-SMA).
    try:
        if len(c) >= 20 and not np.isnan(s50):
            high_20 = h.tail(20).max()
            drawdown_from_high = (c.iloc[-1] / high_20 - 1) * 100
            if -12.0 <= drawdown_from_high <= -8.0 and c.iloc[-1] > s50:
                signs["neutral"].append(("Natürliche Reaktion", f"{drawdown_from_high:+.1f}% vom 20T-Hoch"))
    except Exception:
        pass

    # NEUTRAL: 2,5-Tage-Korrektur (Buch Kap. 4.7)
    # Zwei rote Tage in Folge, gefolgt von einem Tag mit Schluss in oberer Tageshälfte
    # (auch bei leicht negativem Tagesergebnis).
    try:
        if len(c) >= 3:
            r_m2 = c.iloc[-3] - o.iloc[-3]
            r_m1 = c.iloc[-2] - o.iloc[-2]
            rng_last = h.iloc[-1] - l.iloc[-1]
            cr_last = (c.iloc[-1] - l.iloc[-1]) / rng_last if rng_last > 0 else 0.5
            if r_m2 < 0 and r_m1 < 0 and cr_last >= 0.5:
                signs["neutral"].append(("2,5-Tage-Korrektur", "2 rote Tage, Tag 3 Schluss obere Hälfte"))
    except Exception:
        pass

    return signs


def _chart_behavior_score_100(positive_count: int, negative_count: int) -> int:
    """Skaliert Chartverhalten auf 0–100 anhand Maximalsignalen und Pos/Neg-Verhältnis.
    Stand Buch Kap. 4.8: 19 positive Merkmale (inkl. Downside Reversal m. starkem Folgetag,
    Shake-out) und 18 negative Merkmale (inkl. beschleunigte Verluste)."""
    max_positive = 19
    max_negative = 18
    total_max_signals = max_positive + max_negative
    total_active = positive_count + negative_count

    # Anteil positiver Signale unter allen aktiven Pos/Neg-Signalen.
    ratio_component = (positive_count / total_active) if total_active > 0 else 0.5  # 0..1

    # Breiten-Komponente relativ zum maximal möglichen Signalumfang.
    net_component = ((positive_count - negative_count) + max_negative) / total_max_signals  # 0..1

    # Verhältnis hat den größeren Einfluss, Netto-Signalbreite stabilisiert.
    score = int(round((ratio_component * 0.65 + net_component * 0.35) * 100))
    return max(0, min(100, score))


def _dollar_volume_below_threshold(df: pd.DataFrame, price: float, threshold_mio: float = 30.0) -> bool:
    """Prüft, ob das durchschnittliche 20-Tage Dollar-Volumen unter der Schwelle liegt.
    Buch Kap. 3.5: Mindestliquidität 30 Mio. USD/Tag."""
    try:
        if df is None or df.empty or "Volume" not in df.columns:
            return False
        avg_v = pd.to_numeric(df["Volume"], errors="coerce").tail(20).mean()
        if pd.isna(avg_v) or pd.isna(price):
            return False
        dol_v_mio = (avg_v * float(price)) / 1e6
        return dol_v_mio < threshold_mio
    except Exception:
        return False


def build_stock_assessment(
    df: pd.DataFrame,
    info: dict | None,
    fundamentals_checks: list[tuple[str, bool, str]] | None,
    technical_checks: list[tuple[str, bool, str]] | None,
    chart_signs: dict | None,
    rs_ctx: dict | None = None,
    cmf_val: float | None = None,
) -> dict:
    """Erzeugt Status, Summary, Drivers und Warnings für die Aktienbewertung.

    drivers/warnings spiegeln 1:1 die Regeln, die in die 4 sichtbaren KPI-Scores
    einfließen (Technisch, Fundamental, Gleitende Durchschnitte, Chartverhalten),
    plus die Buch-Risikoschwellen (ATR, Beta, Drawdown, Distanzen). Pro KPI-Regel
    genau ein Treiber-Text (wenn erfüllt) ODER ein Warn-Text (wenn nicht erfüllt).
    Die Listen sind nach Wichtigkeit sortiert; das [:5]-Limit zeigt die jeweils
    schärfsten Signale zuerst.

    status / summary tragen nur die 5 strukturellen Sonderfälle, die kein
    KPI-Score abbildet: 'Nicht bewertbar' (zu wenig Daten), 'Mindestpreis nicht
    erreicht' (Kurs < 15 USD), 'Volumen nicht erreicht' (Ø Dollar-Vol < 30 Mio.),
    'Überdehnt' (Distanz zu MAs sprengt Buch-Schwellen), 'Unter 200 Tage' (Buch-
    Hard-Cap Kap. 4.3). Im Normalfall sind beide leer – der Tab entscheidet dann
    via overall_score.
    """
    if df is None or df.empty:
        return {
            "status": "Nicht bewertbar",
            "status_tone": "neutral",
            "summary": "Die Datenlage ist unvollständig, daher nur eingeschränkt bewertbar.",
            "drivers": [],
            "warnings": ["Keine Kursdaten verfügbar."],
        }

    info = info or {}
    close = pd.to_numeric(df["Close"], errors="coerce")
    price = float(close.iloc[-1]) if len(close) else np.nan
    sma10 = close.rolling(10).mean()
    ema21 = close.ewm(span=21).mean()
    sma50 = close.rolling(50).mean()
    sma200 = close.rolling(200).mean()
    atr_s = _atr(df, 21)
    atr_val = atr_s.iloc[-1] if len(atr_s) else np.nan
    atr_pct = (atr_val / price * 100) if pd.notna(atr_val) and pd.notna(price) and price else np.nan
    drawdown_52w = (price / close.rolling(252).max().iloc[-1] - 1) * 100 if len(close) >= 252 else np.nan

    above_10 = bool(pd.notna(sma10.iloc[-1]) and pd.notna(price) and price > sma10.iloc[-1])
    above_21 = bool(pd.notna(ema21.iloc[-1]) and pd.notna(price) and price > ema21.iloc[-1])
    above_50 = bool(pd.notna(sma50.iloc[-1]) and pd.notna(price) and price > sma50.iloc[-1])
    above_200 = bool(pd.notna(sma200.iloc[-1]) and pd.notna(price) and price > sma200.iloc[-1])
    ma_order = bool(
        pd.notna(ema21.iloc[-1]) and pd.notna(sma50.iloc[-1]) and pd.notna(sma200.iloc[-1])
        and ema21.iloc[-1] > sma50.iloc[-1] > sma200.iloc[-1]
    )
    dist_21 = (price / ema21.iloc[-1] - 1) * 100 if pd.notna(price) and pd.notna(ema21.iloc[-1]) and ema21.iloc[-1] else np.nan
    dist_50 = (price / sma50.iloc[-1] - 1) * 100 if pd.notna(price) and pd.notna(sma50.iloc[-1]) and sma50.iloc[-1] else np.nan
    dist_200 = (price / sma200.iloc[-1] - 1) * 100 if pd.notna(price) and pd.notna(sma200.iloc[-1]) and sma200.iloc[-1] else np.nan

    rs_rating = rs_ctx.get("rating") if isinstance(rs_ctx, dict) else np.nan
    rs_rating = float(rs_rating) if rs_rating is not None and pd.notna(rs_rating) else np.nan
    beta = info.get("beta")

    cmf_letter = None
    if cmf_val is not None and pd.notna(cmf_val):
        try:
            cmf_letter, _, _ = _cmf_rating(float(cmf_val))
        except Exception:
            cmf_letter = None

    tech_map = {label: bool(ok) for label, ok, _ in (technical_checks or [])}
    fund_map = {_check_label(c): _check_ok(c) for c in (fundamentals_checks or [])}

    def _fund_state(*prefixes):
        for label, ok in fund_map.items():
            if any(label.startswith(p) for p in prefixes):
                return bool(ok)
        return None  # Check nicht vorhanden

    signs_pos = len((chart_signs or {}).get("positiv", []))
    signs_neg = len((chart_signs or {}).get("negativ", []))

    drivers: list[str] = []
    warnings: list[str] = []

    # === DRIVERS — Spiegel zu den 4 KPI-Scores, sortiert nach Wichtigkeit ===
    # KPI „Gleitende Durchschnitte" (Buch Kap. 4.3)
    if above_50 and above_200:
        drivers.append("Kurs über 50- und 200-Tage-Linie – Trendstruktur intakt.")
    if ma_order:
        drivers.append("MA-Ordnung 21>50>200 sauber gestaffelt.")
    if above_21:
        drivers.append("Kurs über 21-EMA – kurzfristiger Trend intakt.")
    if above_10:
        drivers.append("Kurs über 10-SMA – sehr kurzfristiger Trend bestätigt.")

    # KPI „Technisch" – RS und Volumen (Buch Kap. 3.5)
    if pd.notna(rs_rating) and rs_rating >= 80:
        drivers.append(f"RS-Rating {int(rs_rating)} ≥ 80 – relative Stärke gegenüber dem Markt.")
    if tech_map.get("RS-Linie steigt über 13 Wochen"):
        drivers.append("RS-Linie steigt seit 13 Wochen – nachhaltige Outperformance.")
    if tech_map.get("RS-Linie steigt über 5 Wochen"):
        drivers.append("RS-Linie steigt seit 5 Wochen – frische Outperformance.")
    if tech_map.get("RS-Linie über 50-SMA"):
        drivers.append("RS-Linie über 50-SMA – mittelfristige RS-Stärke.")
    if tech_map.get("RS-Linie über 21-EMA"):
        drivers.append("RS-Linie über 21-EMA – kurzfristige RS-Stärke.")
    if tech_map.get("RS-Linie nahe 52W-Hoch"):
        drivers.append("RS-Linie nahe 52-Wochen-Hoch.")
    if tech_map.get("Nahe am 52W-Hoch"):
        drivers.append("Kurs nahe 52-Wochen-Hoch.")
    if tech_map.get("Up/Down Vol. Ratio ≥1.0"):
        drivers.append("Up/Down-Volume-Ratio ≥ 1 – Akkumulation überwiegt.")
    if cmf_letter == "A":
        drivers.append("Starke Akkumulation laut Chaikin Money Flow (Rating A).")
    elif cmf_letter == "B":
        drivers.append("Moderate Akkumulation laut Chaikin Money Flow (Rating B).")

    # KPI „Fundamental" – 9er-Checkliste (Buch Kap. 3.4)
    if _fund_state("ROE ≥17%") is True:
        drivers.append("ROE ≥ 17% – hohe Eigenkapitalrendite (Buch Kap. 3.4).")
    if _fund_state("Jährl. EPS ≥20%", "Jährl. EPS-Wachstum") is True:
        drivers.append("Jährliches EPS-Wachstum ≥ 20% – Buch-Schwelle erfüllt.")
    if _fund_state("EPS ≥20% YoY") is True:
        drivers.append("Quartals-EPS ≥ 20% YoY – Wachstumstempo intakt.")
    if _fund_state("EPS-Beschleunigung") is True:
        drivers.append("EPS-Wachstum beschleunigt sich.")
    if _fund_state("Jährl. Umsatz ≥20%", "Jährl. Umsatzwachstum") is True:
        drivers.append("Jährliches Umsatzwachstum ≥ 20%.")
    if _fund_state("Umsatz ≥20% YoY") is True:
        drivers.append("Quartals-Umsatzwachstum ≥ 20% YoY.")
    if _fund_state("Umsatz-Beschleunigung") is True:
        drivers.append("Umsatzwachstum beschleunigt sich.")
    if _fund_state("Gewinnmarge positiv") is True:
        drivers.append("Gewinnmarge positiv.")
    if _fund_state("Summe letzte 4 Quartals-EPS > 0", "Trailing EPS > 0") is True:
        drivers.append("Summe der letzten 4 Quartals-EPS positiv.")

    # KPI „Chartverhalten" – Saldo der Chartsignale
    if signs_pos >= signs_neg + 3:
        drivers.append(f"Chartverhalten konstruktiv – {signs_pos} positive vs. {signs_neg} negative Signale.")

    # === WARNINGS — Spiegel + Buch-Risikoschwellen, sortiert nach Schärfe ===
    # Risiko / Volatilität (Buch Kap. 2.2 / 4.5 / 3.6)
    if pd.notna(drawdown_52w) and drawdown_52w <= -20:
        warnings.append("Drawdown ≤ −20% vom 52-Wochen-Hoch – Bärenmarkt-Niveau (Buch Kap. 2.2).")
    if pd.notna(atr_pct) and atr_pct >= 6:
        warnings.append("ATR ≥ 6% – hohe Volatilität, erhöhtes kurzfristiges Risiko.")
    if pd.notna(beta) and beta > 1.6:
        warnings.append("Beta > 1.6 – überdurchschnittliche Marktsensitivität.")

    # MA-Struktur (Buch Kap. 4.3) – sub-200 ist Sonderstatus, hier nur die übrigen
    if above_200 and not above_50:
        warnings.append("Kurs unter 50-SMA – mittelfristiger Trend gestört.")
    if above_200 and not above_21:
        warnings.append("Kurs unter 21-EMA – kurzfristiger Trend gestört.")
    if above_200 and not ma_order:
        warnings.append("MA-Ordnung 21>50>200 nicht erfüllt – Trendstruktur unsauber.")

    # Kapitalfluss
    if cmf_letter == "E":
        warnings.append("Deutliche Distribution laut Chaikin Money Flow (Rating E).")
    elif cmf_letter == "D":
        warnings.append("Moderate Distribution laut Chaikin Money Flow (Rating D).")

    # Relative Schwäche (Spiegel zu RS-Drivern)
    if pd.notna(rs_rating) and rs_rating < 80:
        warnings.append(f"RS-Rating {int(rs_rating)} < 80 – keine relative Stärke.")
    if tech_map.get("RS-Linie steigt über 13 Wochen") is False:
        warnings.append("RS-Linie fällt im 13-Wochen-Trend.")
    if tech_map.get("RS-Linie steigt über 5 Wochen") is False:
        warnings.append("RS-Linie fällt im 5-Wochen-Trend.")
    if tech_map.get("RS-Linie über 50-SMA") is False:
        warnings.append("RS-Linie unter 50-SMA – mittelfristige RS-Schwäche.")
    if tech_map.get("RS-Linie über 21-EMA") is False:
        warnings.append("RS-Linie unter 21-EMA – kurzfristige RS-Schwäche.")
    if tech_map.get("Up/Down Vol. Ratio ≥1.0") is False:
        warnings.append("Up/Down-Volume-Ratio < 1 – Distribution überwiegt.")

    # Fundamentals (Spiegel zu Drivern)
    if _fund_state("ROE ≥17%") is False:
        warnings.append("ROE < 17% – Buch-Qualitätsschwelle nicht erfüllt.")
    if _fund_state("Jährl. EPS ≥20%", "Jährl. EPS-Wachstum") is False:
        warnings.append("Jährliches EPS-Wachstum < 20%.")
    if _fund_state("Jährl. Umsatz ≥20%", "Jährl. Umsatzwachstum") is False:
        warnings.append("Jährliches Umsatzwachstum < 20%.")
    if _fund_state("Gewinnmarge positiv") is False:
        warnings.append("Gewinnmarge nicht positiv.")

    # Chartverhalten (Spiegel zum Chart-Driver)
    if signs_neg >= signs_pos + 3:
        warnings.append(f"Chartverhalten schwach – {signs_neg} negative vs. {signs_pos} positive Signale.")

    # Aggregat-Hinweise + Datenbasis
    if fundamentals_checks:
        failed_fund = [_check_label(c) for c in fundamentals_checks if not _check_ok(c)]
        if len(failed_fund) >= 8:
            warnings.append("Viele fundamentale Prüfpunkte sind aktuell nicht erfüllt oder nicht verfügbar.")
    if technical_checks:
        failed_tech = [label for label, ok, _ in technical_checks if not ok]
        if len(failed_tech) >= 8:
            warnings.append("Mehrere technische Prüfpunkte sind aktuell nicht erfüllt.")

    has_fundamentals = bool(
        info
        and (
            info.get("returnOnEquity") is not None
            or info.get("earningsGrowth") is not None
            or info.get("revenueGrowth") is not None
            or info.get("quarterlyEarningsGrowth") is not None
            or info.get("quarterlyRevenueGrowth") is not None
        )
    )
    if not has_fundamentals:
        warnings.append("Fundamentaldaten fehlen – Einordnung ist chartbasiert.")

    # === Status-Kaskade: 5 strukturelle Sonderfälle ===
    status = ""
    tone = "neutral"
    summary = ""

    if len(df) < 120:
        status = "Nicht bewertbar"
        tone = "neutral"
        summary = "Die Datenlage ist unvollständig, daher nur eingeschränkt bewertbar."
    elif pd.notna(price) and price < 15:
        # Buch Kap. 3.5: Mindestpreis 15 USD
        status = "Mindestpreis nicht erreicht"
        tone = "neutral"
        summary = "Kurs unter 15 USD – Buch-Mindestpreis (Kap. 3.5) nicht erfüllt."
    elif _dollar_volume_below_threshold(df, price, threshold_mio=30):
        # Buch Kap. 3.5: Ø Dollar-Volumen ≥ 30 Mio. USD/Tag
        status = "Volumen nicht erreicht"
        tone = "neutral"
        summary = "Dollar-Volumen unter 30 Mio. USD/Tag – Liquiditätsschwelle (Kap. 3.5) nicht erfüllt."
    elif (
        (pd.notna(dist_50) and dist_50 >= 25)
        or (pd.notna(dist_21) and dist_21 >= 14)
        or (pd.notna(dist_200) and dist_200 >= 70)
    ):
        # Buch Kap. 4.5: Überdehnt sobald 21-EMA 14%, 50-SMA 25% oder 200-SMA 70% gerissen
        status = "Überdehnt"
        tone = "warn"
        summary = "Die Aktie ist deutlich von ihren gleitenden Durchschnitten entfernt."
    elif not above_200:
        # Buch Kap. 4.3: "Du kaufst grundsätzlich keine Titel unter ihrer 200-Tage-Linie."
        status = "Unter 200 Tage"
        tone = "bad"
        summary = "Kurs unter der 200-Tage-Linie – das Buch (Kap. 4.3) rät grundsätzlich vom Kauf ab, unabhängig vom Score."

    return {
        "status": status,
        "status_tone": tone,
        "summary": summary,
        "drivers": list(dict.fromkeys(drivers))[:5],
        "warnings": list(dict.fromkeys(warnings))[:5],
    }

# ===== From tabs.py =====

def _scale_series_0_100(series: pd.Series, invert: bool = False) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    valid = numeric.dropna()
    if valid.empty:
        return pd.Series([50.0] * len(series), index=series.index)
    v_min = float(valid.min())
    v_max = float(valid.max())
    if np.isclose(v_min, v_max):
        scaled = pd.Series([60.0] * len(series), index=series.index)
    else:
        scaled = (numeric - v_min) / (v_max - v_min) * 100.0
    if invert:
        scaled = 100.0 - scaled
    return scaled.clip(lower=0, upper=100).fillna(50.0)


def _compute_stock_compare_rows(tickers: list[str], rs_source_setting: str) -> pd.DataFrame:
    if not tickers:
        return pd.DataFrame()

    spx_df = load_sp500_for_rs()
    spx_close = spx_df["Close"] if spx_df is not None and "Close" in spx_df else None
    rs_universe_scores = load_cached_universe_rs_scores() if rs_source_setting == RS_SOURCE_COMPUTED else None
    rows = []

    def _load_one(symbol: str):
        df, info, qi, ai, ih, qe, ed, qraw, fmp_err = load_stock_full(symbol)
        return symbol, df, info, qi, ai, ih, qe, ed, qraw, fmp_err

    with ThreadPoolExecutor(max_workers=min(8, max(2, len(tickers)))) as executor:
        futures = {executor.submit(_load_one, symbol): symbol for symbol in tickers}
        for fut in as_completed(futures):
            symbol = futures[fut]
            try:
                ticker, df, info, qi, ai, ih, qe, ed, qraw, fmp_err = fut.result()
            except Exception:
                ticker, df, info, qi, ai, ih, qe, ed, qraw, fmp_err = symbol, None, {}, None, None, None, None, None, None, None
            if df is None or len(df) < 120:
                continue

            close = df["Close"]
            latest = float(close.iloc[-1])
            ret_21 = (close.iloc[-1] / close.iloc[-22] - 1) * 100 if len(close) > 22 else np.nan
            ret_63 = (close.iloc[-1] / close.iloc[-64] - 1) * 100 if len(close) > 64 else np.nan
            ret_126 = (close.iloc[-1] / close.iloc[-127] - 1) * 100 if len(close) > 127 else np.nan
            dd_252 = ((close.iloc[-1] / close.tail(252).max()) - 1) * 100 if len(close) >= 252 else ((close.iloc[-1] / close.max()) - 1) * 100
            atr_val = _atr(df, 21).iloc[-1] if len(df) > 25 else np.nan
            atr_pct = (atr_val / latest * 100) if pd.notna(atr_val) and latest else np.nan
            ema21 = close.ewm(span=21).mean().iloc[-1] if len(close) >= 21 else np.nan
            sma50 = close.rolling(50).mean().iloc[-1] if len(close) >= 50 else np.nan
            sma200 = close.rolling(200).mean().iloc[-1] if len(close) >= 200 else np.nan
            above_21 = bool(pd.notna(ema21) and latest > ema21)
            above_50 = bool(pd.notna(sma50) and latest > sma50)
            above_200 = bool(pd.notna(sma200) and latest > sma200)
            beta = info.get("beta") if isinstance(info, dict) else np.nan

            rs_ctx = _calc_rs_rating(close, spx_close, universe_scores=rs_universe_scores)
            rs_ctx, _ = _apply_rs_source_override(ticker, rs_ctx)
            rs_rating = rs_ctx.get("rating") if isinstance(rs_ctx, dict) else np.nan
            technical_checks, cmf_val, _ = evaluate_technicals(
                df,
                info,
                spx_df=spx_df,
                rs_ctx=rs_ctx,
                rs_universe_scores=rs_universe_scores,
            )
            fundamental_checks = evaluate_fundamentals(info, qi, ai, ih, qe=qe, ed=ed, qraw=qraw, fmp_err=fmp_err, ticker=ticker)
            chart_signs = evaluate_chart_signs(df, rs_ctx=rs_ctx)

            trend_check_names = {
                "Kurs über 10-SMA",
                "Kurs über 21-EMA",
                "Kurs über 50-SMA",
                "Kurs über 200-SMA",
                "MA-Ordnung (21>50>200)",
            }
            trend_checks = [(label, ok, detail) for label, ok, detail in technical_checks if label in trend_check_names]
            technical_core_checks = [(label, ok, detail) for label, ok, detail in technical_checks if label not in trend_check_names]

            fund_pos = sum(1 for check in fundamental_checks if _check_ok(check))
            fund_neu = sum(1 for check in fundamental_checks if _check_warn(check))
            fund_neg = sum(1 for check in fundamental_checks if not _check_ok(check) and not _check_warn(check))
            tech_pos = sum(1 for _, ok, _ in technical_core_checks if ok)
            tech_neg = sum(1 for _, ok, _ in technical_core_checks if not ok)
            tech_neu = 0
            trend_pos = sum(1 for _, ok, _ in trend_checks if ok)
            trend_neg = sum(1 for _, ok, _ in trend_checks if not ok)
            trend_neu = 0
            chart_pos = len(chart_signs.get("positiv", []))
            chart_neg = len(chart_signs.get("negativ", []))
            chart_neu = len(chart_signs.get("neutral", []))

            q_revenue_growth = info.get("quarterlyRevenueGrowth") if isinstance(info, dict) else None
            q_earnings_growth = info.get("quarterlyEarningsGrowth") if isinstance(info, dict) else None
            if q_revenue_growth is None:
                revenue_fallback = _quarterly_yoy_growth(qi, "revenue", qe=qe, ed=ed, qraw=qraw)
                if revenue_fallback and revenue_fallback[0][1] is not None:
                    q_revenue_growth = revenue_fallback[0][1] / 100
            if q_earnings_growth is None:
                eps_fallback = _quarterly_yoy_growth(qi, "eps", qe=qe, ed=ed, qraw=qraw)
                if eps_fallback and eps_fallback[0][1] is not None:
                    q_earnings_growth = eps_fallback[0][1] / 100

            technical_points_score = _technical_points_score(technical_checks, rs_rating, cmf_val)
            fundamental_score, fundamental_met, fundamental_total = _fundamental_checklist_score_100(
                fundamental_checks,
                q_earnings_growth,
                q_revenue_growth,
                info.get("earningsGrowth") if isinstance(info, dict) else None,
                info.get("revenueGrowth") if isinstance(info, dict) else None,
                info.get("returnOnEquity") if isinstance(info, dict) else None,
                info.get("profitMargins") if isinstance(info, dict) else None,
            )
            chart_score_100 = _chart_behavior_score_100(chart_pos, chart_neg)

            sma10 = close.rolling(10).mean().iloc[-1] if len(close) >= 10 else np.nan
            ma_score = (
                30.0 * float(above_200)
                + 24.0 * float(above_50)
                + 18.0 * float(above_21)
                + 8.0 * float(pd.notna(sma10) and latest > sma10)
                + 20.0 * float(pd.notna(ema21) and pd.notna(sma50) and pd.notna(sma200) and ema21 > sma50 > sma200)
            )
            overall_score = _round_half_up_int(np.mean([technical_points_score, fundamental_score, chart_score_100, ma_score]))
            technical_score_display = _round_half_up_int(technical_points_score)
            fundamental_score_display = _round_half_up_int(fundamental_score)
            ma_score_display = _round_half_up_int(ma_score)
            chart_score_display = _round_half_up_int(chart_score_100)

            rows.append({
                "Ticker": ticker,
                "Name": (info.get("shortName", ticker) if isinstance(info, dict) else ticker),
                "Preis": latest,
                "Perf 1M %": ret_21,
                "Perf 3M %": ret_63,
                "Perf 6M %": ret_126,
                "Drawdown %": dd_252,
                "ATR %": atr_pct,
                "Beta": beta,
                "RS-Rating": rs_rating,
                "Über 10-SMA": bool(pd.notna(sma10) and latest > sma10),
                "Über 21-EMA": above_21,
                "Über 50-SMA": above_50,
                "Über 200-SMA": above_200,
                "MA-Ordnung": bool(pd.notna(ema21) and pd.notna(sma50) and pd.notna(sma200) and ema21 > sma50 > sma200),
                "Score Gleitende Durchschnitte": ma_score_display,
                "Trend Positiv": trend_pos,
                "Trend Negativ": trend_neg,
                "Trend Neutral": trend_neu,
                "Fundamental Positiv": fund_pos,
                "Fundamental Negativ": fund_neg,
                "Fundamental Neutral": fund_neu,
                "Technisch Positiv": tech_pos,
                "Technisch Negativ": tech_neg,
                "Technisch Neutral": tech_neu,
                "Chart Positiv": chart_pos,
                "Chart Negativ": chart_neg,
                "Chart Neutral": chart_neu,
                "Score Fundamental": fundamental_score_display,
                "Fundamental Kriterien erfüllt": fundamental_met,
                "Fundamental Kriterien gesamt": fundamental_total,
                "Score Technisch": technical_score_display,
                "Score Chart": chart_score_display,
                "Gesamt-Score": overall_score,
            })

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df = df.sort_values(["Gesamt-Score", "Ticker"], ascending=[False, True]).reset_index(drop=True)
    df["Rang"] = np.arange(1, len(df) + 1)
    return df


def _render_stock_ranking_tables(compare_df: pd.DataFrame, key_prefix: str = "compare") -> None:
    score_cols = [
        "Rang", "Ticker", "Gesamt-Score", "Score Technisch", "Score Fundamental",
        "Score Gleitende Durchschnitte", "Score Chart",
    ]
    overview_cols = score_cols
    st.markdown("##### 1) Gesamtranking")
    st.caption("Das Ranking nutzt dieselben Teil-Scores wie der Einzelaktien-Check: Technisch, Fundamental, Gleitende Durchschnitte und Chartverhalten.")
    st.dataframe(compare_df[overview_cols].round(1), width="stretch", hide_index=True, column_config=rating_overview_column_config())

    st.markdown("##### 2) Kategorien")
    category_config = {
        "Gesamtscore": {
            "sort": "Gesamt-Score",
            "cols": score_cols,
        },
        "Technisch": {
            "sort": "Score Technisch",
            "cols": [
                "Rang", "Ticker", "Score Technisch", "Technisch Positiv", "Technisch Negativ",
                "Technisch Neutral", "RS-Rating",
            ],
        },
        "Fundamental": {
            "sort": "Score Fundamental",
            "cols": [
                "Rang", "Ticker", "Score Fundamental", "Fundamental Kriterien erfüllt",
                "Fundamental Kriterien gesamt", "Fundamental Positiv", "Fundamental Negativ",
                "Fundamental Neutral",
            ],
        },
        "Gleitende Durchschnitte": {
            "sort": "Score Gleitende Durchschnitte",
            "cols": [
                "Rang", "Ticker", "Score Gleitende Durchschnitte", "Über 200-SMA", "Über 50-SMA",
                "Über 21-EMA", "Über 10-SMA", "MA-Ordnung",
            ],
        },
        "Chartverhalten": {
            "sort": "Score Chart",
            "cols": ["Rang", "Ticker", "Score Chart", "Chart Positiv", "Chart Negativ", "Chart Neutral"],
        },
    }
    if st.session_state.get(f"{key_prefix}_selected_category") not in category_config:
        st.session_state[f"{key_prefix}_selected_category"] = "Gesamtscore"

    button_cols = st.columns(len(category_config))
    for idx, category in enumerate(category_config.keys()):
        if button_cols[idx].button(category, width="stretch", key=f"{key_prefix}_cat_{idx}"):
            st.session_state[f"{key_prefix}_selected_category"] = category
            st.rerun()

    selected = st.session_state.get(f"{key_prefix}_selected_category", "Gesamtscore")
    selected_config = category_config.get(selected, category_config["Gesamtscore"])
    sort_col = selected_config["sort"]
    detail_cols = selected_config["cols"]
    detail_df = compare_df.sort_values([sort_col, "Ticker"], ascending=[False, True]).reset_index(drop=True).copy()
    detail_df["Rang"] = np.arange(1, len(detail_df) + 1)

    st.markdown(f"##### 3) Detailvergleich · {selected}")
    st.dataframe(
        detail_df[detail_cols].round(2),
        width="stretch",
        hide_index=True,
        key=f"{key_prefix}_detail_{selected}",
    )

    with st.expander("Alle Kennzahlen im direkten Vergleich", expanded=False):
        raw_cols = [
            "Ticker", "Name", "Preis", "Perf 1M %", "Perf 3M %", "Perf 6M %", "Drawdown %", "ATR %", "Beta",
            "RS-Rating", "Über 10-SMA", "Über 21-EMA", "Über 50-SMA", "Über 200-SMA", "MA-Ordnung",
            "Trend Positiv", "Trend Negativ", "Trend Neutral",
            "Fundamental Positiv", "Fundamental Negativ", "Fundamental Neutral",
            "Fundamental Kriterien erfüllt", "Fundamental Kriterien gesamt",
            "Technisch Positiv", "Technisch Negativ", "Technisch Neutral",
            "Chart Positiv", "Chart Negativ", "Chart Neutral",
            "Score Fundamental", "Score Technisch", "Score Chart",
            "Score Gleitende Durchschnitte", "Gesamt-Score",
        ]
        st.dataframe(compare_df[raw_cols].round(2), width="stretch", hide_index=True)


def _render_stock_compare_section() -> None:
    st.markdown("#### 🧮 Aktienvergleich mit Ranking")
    st.caption("Zuerst Übersicht mit Gesamtranking, danach Kategorie-Rankings. Über die Kategorie-Buttons öffnest du die Detailtabelle für den direkten Vergleich.")

    watchlist = st.session_state.get("watchlist", [])
    base_defaults = [t for t in watchlist if isinstance(t, str) and t][:6] or DEFAULT_FAVORITES[:5]
    tickers = st.multiselect(
        "Ticker vergleichen",
        options=sorted(set((watchlist or []) + DEFAULT_FAVORITES)),
        default=base_defaults,
        help="Wähle mindestens 2 Aktien für den Vergleich.",
        key="compare_tickers_multi",
    )
    manual = st.text_input("Weitere Ticker (kommagetrennt)", value="", placeholder="z.B. AMD, AVGO, NFLX", key="compare_tickers_manual")
    extra = [x.strip().upper() for x in manual.split(",") if x.strip()]
    tickers = list(dict.fromkeys([t.upper() for t in tickers] + extra))[:12]

    if len(tickers) < 2:
        st.info("Bitte mindestens 2 Ticker auswählen, damit ein Vergleich möglich ist.")
        return

    rs_source_setting = _get_rs_rating_source_setting()
    with st.spinner("Berechne Ranking und Kategorien …"):
        compare_df = _compute_stock_compare_rows(tickers, rs_source_setting)
    if compare_df.empty:
        st.warning("Für die ausgewählten Ticker konnten nicht genug Kursdaten geladen werden.")
        return

    _render_stock_ranking_tables(compare_df, key_prefix="compare")


def _next_earnings_info(ed, ref_now=None) -> dict | None:
    """Sucht in earnings_dates (Yahoo) das nächste zukünftige Earnings-Datum.
    Gibt {'date': pd.Timestamp, 'calendar_days': int, 'trading_days': int} zurück
    oder None, wenn nichts gefunden wurde. Buch Kap. 5.1: Kein Einstieg kurz vor
    Quartalszahlen."""
    if ed is None or (hasattr(ed, "empty") and ed.empty):
        return None
    try:
        if ref_now is None:
            ref_now = pd.Timestamp.now(tz="UTC")
        else:
            ref_now = pd.Timestamp(ref_now)
            if ref_now.tzinfo is None:
                ref_now = ref_now.tz_localize("UTC")
        idx = ed.index
        try:
            if idx.tz is None:
                idx = idx.tz_localize("UTC")
            else:
                idx = idx.tz_convert("UTC")
        except Exception:
            return None
        future = idx[idx > ref_now]
        if len(future) == 0:
            return None
        next_dt = future.min()
        calendar_days = int((next_dt.normalize() - ref_now.normalize()).days)
        # Handelstage approximieren (Mo-Fr, ohne Feiertage)
        try:
            trading_days = int(
                len(pd.bdate_range(ref_now.tz_convert(None).normalize(), next_dt.tz_convert(None).normalize())) - 1
            )
            trading_days = max(0, trading_days)
        except Exception:
            trading_days = calendar_days
        return {"date": next_dt, "calendar_days": calendar_days, "trading_days": trading_days}
    except Exception:
        return None


def _tab_aktienbewertung():
    _init_workspace_state()
    st.markdown(
        '<div class="summary-hero"><div class="hero-title">Aktienbewertung</div>'
        '<div class="hero-subtitle">Regelbasierter Qualitäts-, Trend- und Risiko-Check für Einzelaktien.</div>'
        '<div class="mini-help">Keine Anlageberatung – nur eine systematische Einordnung.</div></div>',
        unsafe_allow_html=True,
    )
    st.markdown("#### 🔎 Ticker-Suche")
    st.caption("Starte mit einem Symbol und erhalte eine geführte, regelbasierte Einordnung statt einer reinen Datenansicht.")

    mode = st.segmented_control(
        "Modus",
        options=["Vergleich & Ranking", "Einzelaktien-Check"],
        default="Einzelaktien-Check",
        key="stock_eval_mode",
        label_visibility="collapsed",
    )
    if mode == "Vergleich & Ranking":
        _render_stock_compare_section()
        return

    ticker = _render_ticker_picker(
        "stock",
        "Ticker oder Firmenname suchen",
        "Ticker eingeben, z. B. NVDA, MSFT, PLTR",
        show_quick=True,
        action_label="Daten neu laden",
        action_key="refresh_stock_data",
        action_help="Lädt die Daten für den aktuell ausgewählten Ticker ohne Cache neu.",
    )
    if not ticker:
        return

    rs_source_setting = _get_rs_rating_source_setting()
    _cache_v_key = f"_stock_cache_v_{ticker}"
    refresh_requested = st.session_state.pop("refresh_stock_data_requested", False)
    if refresh_requested:
        st.session_state[_cache_v_key] = st.session_state.get(_cache_v_key, 0) + 1
    cache_buster = st.session_state.get(_cache_v_key, 0)
    with st.spinner(f"Lade {ticker} …"):
        (
            df,
            info,
            qi,
            ai,
            ih,
            qe,
            ed,
            qraw,
            fmp_err,
            spx_df,
            rs_universe_scores,
        ) = _load_stock_analysis_context_parallel(ticker, rs_source_setting, cache_buster)

    if df is None or len(df) < 20:
        st.error(f"Keine Daten für '{ticker}'.")
        return

    _add_recent_ticker(ticker)
    L = df.iloc[-1]
    name = info.get("shortName", ticker) if info else ticker
    price = float(L["Close"])
    prev = df["Close"].iloc[-2]
    chg = (price / prev - 1) * 100
    last_date = _format_market_date(df.index[-1])

    act1, act2 = st.columns([1, 2])
    private_ok = _is_private_unlocked()
    with act1:
        if private_ok and st.button("➕ Zur Watchlist", width="stretch", key="add_watch_stock", type="secondary"):
            _add_watchlist_ticker(ticker)
            st.success(f"{ticker} zur Watchlist hinzugefügt.")
    with act2:
        st.caption(f"Datenstand: {last_date} · Quelle Yahoo Finance")
        if not private_ok:
            st.caption("Watchlist-Speicherung ist gesperrt, bis du den privaten Bereich entsperrst.")

    # ─── Earnings-Vorwarnung (Buch Kap. 5.1) ───
    # "Prüfe immer den Terminkalender hinsichtlich nahender Quartalszahlen.
    #  Ein Einstieg kurz vor den Zahlen erhöht dein Risiko deutlich."
    next_earn = _next_earnings_info(ed)
    if next_earn is not None:
        td = next_earn["trading_days"]
        cd = next_earn["calendar_days"]
        try:
            date_str = next_earn["date"].strftime("%d.%m.%Y")
        except Exception:
            date_str = str(next_earn["date"])
        when_text = "heute" if td == 0 or cd == 0 else f"in {td} Handelstagen / {cd} Kalendertagen"
        if td <= 5:
            st.error(
                f"⚠️ Nächste Quartalszahlen am {date_str} ({when_text}). "
                f"Buch Kap. 5.1: Kein Einstieg kurz vor Quartalszahlen ohne Gewinnpolster."
            )
        elif td <= 14:
            st.warning(
                f"📅 Nächste Quartalszahlen am {date_str} ({when_text}). "
                f"Buch Kap. 5.1: Risiko beim Einstieg vor Quartalszahlen erhöht."
            )
        else:
            st.info(f"📅 Nächste Quartalszahlen am {date_str} ({when_text}).")

    atr_s = _atr(df, 21)
    atr_val = atr_s.iloc[-1] if len(atr_s) > 0 else np.nan
    atr_pct = (atr_val / price * 100) if not np.isnan(atr_val) else np.nan
    vol_ratio = float(L["Volume"] / df["Volume"].rolling(50).mean().iloc[-1]) if len(df) >= 50 and pd.notna(df["Volume"].rolling(50).mean().iloc[-1]) and df["Volume"].rolling(50).mean().iloc[-1] else np.nan
    rs_ctx = _calc_rs_rating(
        df["Close"],
        spx_df["Close"] if spx_df is not None else None,
        universe_scores=rs_universe_scores,
    )
    rs_ctx, rs_source_note = _apply_rs_source_override(ticker, rs_ctx)
    rs_hint = ""
    if isinstance(rs_ctx, dict):
        if rs_ctx.get("trend_5w") is True:
            rs_hint = "RS verbessert sich"
        elif rs_ctx.get("trend_5w") is False:
            rs_hint = "RS verliert etwas Tempo"

    rs_rating_val = rs_ctx.get("rating") if isinstance(rs_ctx, dict) else None
    rs_rating_detail = (
        "CSV-Rating" if isinstance(rs_ctx, dict) and rs_ctx.get("method") == "external_csv"
        else "Perzentil im Universum" if isinstance(rs_ctx, dict) and rs_ctx.get("method") == "universe_percentile"
        else "Gewichtete RS" if isinstance(rs_ctx, dict) and rs_ctx.get("method") == "weighted_proxy"
        else "Vergleich zum S&P 500"
    )
    # Schnellurteil
    _ema21 = df["Close"].ewm(span=21).mean()
    _sma50 = df["Close"].rolling(50).mean()
    _sma200 = df["Close"].rolling(200).mean()

    fundamentals_checks = evaluate_fundamentals(info, qi, ai, ih, qe, ed, qraw, fmp_err, ticker=ticker)
    technical_checks, cmf_val, _ = evaluate_technicals(df, info, spx_df, rs_ctx=rs_ctx, rs_universe_scores=rs_universe_scores)
    signs = evaluate_chart_signs(df, rs_ctx=rs_ctx)

    # Yahoo Finance sometimes omits quarterlyRevenueGrowth / quarterlyEarningsGrowth from
    # the info dict. Fall back to the most recent quarter computed from detailed income data.
    _q_rev_fb = info.get("quarterlyRevenueGrowth") if info else None
    _q_eps_fb = info.get("quarterlyEarningsGrowth") if info else None
    if _q_rev_fb is None:
        _revg_fb = _quarterly_yoy_growth(qi, "revenue", qe=qe, ed=ed, qraw=qraw)
        if _revg_fb and _revg_fb[0][1] is not None:
            _q_rev_fb = _revg_fb[0][1] / 100
    if _q_eps_fb is None:
        _epsg_fb = _quarterly_yoy_growth(qi, "eps", qe=qe, ed=ed, qraw=qraw)
        if _epsg_fb and _epsg_fb[0][1] is not None:
            _q_eps_fb = _epsg_fb[0][1] / 100
    _info_for_assessment = dict(info or {})
    if _q_rev_fb is not None:
        _info_for_assessment["quarterlyRevenueGrowth"] = _q_rev_fb
    if _q_eps_fb is not None:
        _info_for_assessment["quarterlyEarningsGrowth"] = _q_eps_fb

    assessment = build_stock_assessment(
        df=df,
        info=_info_for_assessment,
        fundamentals_checks=fundamentals_checks,
        technical_checks=technical_checks,
        chart_signs=signs,
        rs_ctx=rs_ctx,
        cmf_val=cmf_val,
    )

    def _pct_or_na(value):
        return f"{value*100:.1f}%" if value is not None and pd.notna(value) else "n/a"

    def _val_or_na(value, fmt="{:.2f}"):
        return fmt.format(value) if value is not None and pd.notna(value) else "n/a"

    roe = info.get("returnOnEquity") if info else None
    gross_margin = info.get("grossMargins") if info else None
    op_margin = info.get("operatingMargins") if info else None
    profit_margin = info.get("profitMargins") if info else None
    debt_to_equity = info.get("debtToEquity") if info else None
    revenue_growth = info.get("revenueGrowth") if info else None
    earnings_growth = info.get("earningsGrowth") if info else None
    q_revenue_growth = _q_rev_fb
    q_earnings_growth = _q_eps_fb
    drawdown_52w = (price / df["Close"].rolling(252).max().iloc[-1] - 1) * 100 if len(df) >= 252 else np.nan

    _sma10 = df["Close"].rolling(10).mean()
    ma_score_single = (
        30.0 * float(pd.notna(_sma200.iloc[-1]) and price > _sma200.iloc[-1])
        + 24.0 * float(pd.notna(_sma50.iloc[-1]) and price > _sma50.iloc[-1])
        + 18.0 * float(pd.notna(_ema21.iloc[-1]) and price > _ema21.iloc[-1])
        + 8.0 * float(pd.notna(_sma10.iloc[-1]) and price > _sma10.iloc[-1])
        + 20.0 * float(pd.notna(_ema21.iloc[-1]) and pd.notna(_sma50.iloc[-1]) and pd.notna(_sma200.iloc[-1]) and _ema21.iloc[-1] > _sma50.iloc[-1] > _sma200.iloc[-1])
    )
    technical_score_single = _technical_points_score(
        technical_checks,
        rs_ctx.get("rating") if isinstance(rs_ctx, dict) else None,
        cmf_val,
    )
    fundamental_score_single, fundamental_met, fundamental_total = _fundamental_checklist_score_100(
        fundamentals_checks,
        q_earnings_growth,
        q_revenue_growth,
        earnings_growth,
        revenue_growth,
        roe,
        profit_margin,
    )
    np_ = len(signs["positiv"])
    nn = len(signs["negativ"])
    nu = len(signs["neutral"])
    chart_score = np_ - nn
    chart_score_100 = _chart_behavior_score_100(np_, nn)
    overall_score = _round_half_up_int(np.mean([technical_score_single, fundamental_score_single, chart_score_100, ma_score_single]))
    technical_score_single_i = _round_half_up_int(technical_score_single)
    fundamental_score_single_i = _round_half_up_int(fundamental_score_single)
    ma_score_single_i = _round_half_up_int(ma_score_single)
    chart_score_100_i = _round_half_up_int(chart_score_100)

    # Verdict basiert auf dem neuen Gesamtscore; Sonderfälle aus dem Assessment bleiben erhalten
    if assessment["status"] in (
        "Nicht bewertbar",
        "Mindestpreis nicht erreicht",
        "Volumen nicht erreicht",
        "Überdehnt",
        "Unter 200 Tage",
    ):
        verdict_label = assessment["status"]
        verdict_cls = f"status-{assessment['status_tone']}"
        verdict_text = assessment["summary"]
    else:
        # Verdict-Text dynamisch aus den 4 sichtbaren KPI-Scores ableiten,
        # damit der Header zeigt, welcher Teilbereich gerade stark/dünn ist
        # (statt der vorherigen Boilerplate-Sätze).
        _kpi_scores = [
            ("Trend", ma_score_single_i),
            ("Technisch", technical_score_single_i),
            ("Fundamentals", fundamental_score_single_i),
            ("Chart", chart_score_100_i),
        ]
        _strong = [name for name, s in _kpi_scores if s >= 70]
        _weak = [name for name, s in _kpi_scores if s < 45]

        def _join_kpis(names):
            if len(names) <= 1:
                return names[0] if names else ""
            if len(names) == 2:
                return f"{names[0]} & {names[1]}"
            return ", ".join(names[:-1]) + f" & {names[-1]}"

        _parts = []
        if _strong:
            _parts.append(f"{_join_kpis(_strong)} stark")
        if _weak:
            _parts.append(f"{_join_kpis(_weak)} dünn")

        if _parts:
            verdict_text = " · ".join(_parts) + "."
        elif overall_score >= 55:
            verdict_text = "Alle vier Teilbereiche im soliden Mittelband."
        else:
            verdict_text = "Alle vier Teilbereiche schwach."

        if overall_score >= 75:
            verdict_label = "Attraktiv"
            verdict_cls = "status-good"
        elif overall_score >= 55:
            verdict_label = "Beobachten"
            verdict_cls = "status-warn"
        else:
            verdict_label = "Zu schwach"
            verdict_cls = "status-bad"

    _vol_str = f"{vol_ratio:.2f}x 50-T-Schnitt" if not np.isnan(vol_ratio) else "—"
    rs_quick = f"{int(rs_rating_val)}" if rs_rating_val is not None and pd.notna(rs_rating_val) else "n/a"
    st.markdown(
        f'<div class="info-card">'
        f'<div class="card-label">Schnellurteil</div>'
        f'<div style="display:flex;flex-wrap:wrap;justify-content:space-between;gap:10px;align-items:center;">'
        f'<div><div class="hero-title" style="font-size:1.05rem;">{name} ({ticker})</div>'
        f'<div class="mini-help">Letzter Schluss {last_date} · Schlusskurs ${price:,.2f} · {chg:+.2f}% · Volumen {_vol_str} · RS-Rating {rs_quick}</div></div>'
        f'<div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;">'
        f'<span class="pill">Gesamtscore: {overall_score}/100</span>'
        f'<div style="width:52px;height:52px;border-radius:50%;border:2px solid rgba(59,130,246,0.35);display:flex;flex-direction:column;align-items:center;justify-content:center;background:rgba(59,130,246,0.08);">'
        f'<div style="font-size:.58rem;color:#64748b;line-height:1;">RS</div><div style="font-size:1rem;font-weight:700;color:#1d4ed8;line-height:1.05;">{rs_quick}</div></div>'
        f'<span class="status-chip {verdict_cls}">{verdict_label}</span></div></div>'
        f'<div class="mini-help">{verdict_text}</div></div>',
        unsafe_allow_html=True,
    )

    # KPI-Cockpit mit Einordnung
    rng_hl = L["High"] - L["Low"]
    cr_today = (L["Close"] - L["Low"]) / rng_hl * 100 if rng_hl > 0 else 50
    beta = info.get("beta") if info else None
    cat_lbl, _ = _atr_category(atr_pct)
    dist_10 = (price / _sma10.iloc[-1] - 1) * 100 if pd.notna(_sma10.iloc[-1]) and _sma10.iloc[-1] else np.nan
    dist_21 = (price / _ema21.iloc[-1] - 1) * 100 if pd.notna(_ema21.iloc[-1]) and _ema21.iloc[-1] else np.nan
    dist_50 = (price / _sma50.iloc[-1] - 1) * 100 if pd.notna(_sma50.iloc[-1]) and _sma50.iloc[-1] else np.nan
    dist_200 = (price / _sma200.iloc[-1] - 1) * 100 if pd.notna(_sma200.iloc[-1]) and _sma200.iloc[-1] else np.nan
    # Verdict am angezeigten 0–100-Score ausrichten, damit Header und Karten-Tone
    # nicht widersprechen (sonst „85/100 — Gemischt").
    if chart_score_100 >= 75:
        chart_verdict, chart_color = "Starkes Chartbild", "#22c55e"
    elif chart_score_100 >= 55:
        chart_verdict, chart_color = "Leicht positiv", "#22c55e"
    elif chart_score_100 >= 45:
        chart_verdict, chart_color = "Gemischt", "#f59e0b"
    else:
        chart_verdict, chart_color = "Schwaches Chartbild", "#ef4444"

    # --- Gesamtscore + Technisch + Chartverhalten nebeneinander ---
    score_cols = st.columns(5)
    with score_cols[0]:
        render_kpi_card(
            label="Gesamtscore",
            value=f"{overall_score}/100",
            interpretation=f"{verdict_label} · {name} ({ticker})",
            tone="good" if overall_score >= 75 else "warn" if overall_score >= 55 else "bad",
            help_text=verdict_text,
            why_important="Der Gesamtscore ist der gleichgewichtete Durchschnitt aus Technisch, Fundamental, Gleitende Durchschnitte und Chartverhalten.",
            rule_note="≥75 Attraktiv, 55–74 Beobachten, <55 Zu schwach.",
        )
    with score_cols[1]:
        render_kpi_card(
            label="Score Technisch",
            value=f"{technical_score_single_i}/100",
            interpretation="Regelbasiert nach Preis, Volumen, RS-Struktur und CMF.",
            tone="good" if technical_score_single_i >= 70 else "warn" if technical_score_single_i >= 45 else "bad",
            help_text="Technischer Teilscore nach deiner Punktelogik (inkl. RS-Staffelung und CMF-Bewertung).",
            why_important="Zeigt die technische Qualität unabhängig von fundamentalen Teilaspekten.",
            rule_note="Punktesystem gemäß definierter Kriterien, anschließend auf 0–100 skaliert.",
        )
    with score_cols[2]:
        render_kpi_card(
            label="Fundamental",
            value=f"{fundamental_score_single_i}/100",
            interpretation=f"{fundamental_met}/{fundamental_total} Kriterien erfüllt",
            tone="good" if fundamental_score_single_i >= 70 else "warn" if fundamental_score_single_i >= 45 else "bad",
            help_text="Gleichgewichtete 9er-Checkliste: EPS/Umsatz-Wachstum, Beschleunigung, ROE und Gewinnmarge.",
            why_important="Zeigt, wie viele Kernkriterien der fundamentalen Checkliste aktuell erfüllt sind.",
            rule_note="Jedes der 9 Kriterien zählt gleich viel (1/9); Score = erfüllte Kriterien / 9 × 100.",
        )
    with score_cols[3]:
        render_kpi_card(
            label="Gleitende Durchschnitte",
            value=f"{ma_score_single_i}/100",
            interpretation=(
                f'200-SMA: {"ja" if pd.notna(_sma200.iloc[-1]) and price > _sma200.iloc[-1] else "nein"} · '
                f'50-SMA: {"ja" if pd.notna(_sma50.iloc[-1]) and price > _sma50.iloc[-1] else "nein"} · '
                f'21-EMA: {"ja" if pd.notna(_ema21.iloc[-1]) and price > _ema21.iloc[-1] else "nein"} · '
                f'10-SMA: {"ja" if pd.notna(_sma10.iloc[-1]) and price > _sma10.iloc[-1] else "nein"} · '
                f'Ordnung 21>50>200: {"ja" if pd.notna(_ema21.iloc[-1]) and pd.notna(_sma50.iloc[-1]) and pd.notna(_sma200.iloc[-1]) and _ema21.iloc[-1] > _sma50.iloc[-1] > _sma200.iloc[-1] else "nein"}'
            ),
            # Schwelle 70 statt 75: konsistent mit Verdict-Header („Trend stark" ab 70),
            # damit Karte nicht „warn" zeigt während Header bereits „stark" meldet.
            tone="good" if ma_score_single_i >= 70 else "warn" if ma_score_single_i >= 45 else "bad",
            help_text="Gewichteter MA-Teilscore mit Schwerpunkt auf 200-SMA, danach 50-SMA, 21-EMA, MA-Ordnung und 10-SMA.",
            why_important="Der Score zeigt auf einen Blick, ob die Trendstruktur über mehrere Zeithorizonte konstruktiv ausgerichtet ist.",
            rule_note="Gewichtung: 200-SMA 30, 50-SMA 24, 21-EMA 18, MA-Ordnung 20, 10-SMA 8 Punkte.",
        )
    with score_cols[4]:
        render_kpi_card(
            label="Chartverhalten",
            value=f"{chart_score_100_i}/100",
            interpretation=chart_verdict,
            tone="good" if chart_score_100 >= 55 else "warn" if chart_score_100 >= 45 else "bad",
            help_text=f"{np_} Positiv · {nn} Negativ · {nu} Neutral · Netto {chart_score:+d}",
            why_important="Verdichtet die Chartsignale zu einer schnellen Einordnung des aktuellen Chartbilds.",
            rule_note="0–100 aus Maximalsignalen (19 positive / 18 negative) und Pos/Neg-Verhältnis; die verbale Einordnung folgt dem Netto-Score.",
        )

    # --- Trennlinie: Zusatzindikatoren (kein Bestandteil des Gesamtscores) ---
    st.markdown(
        '<div style="margin:1.6rem 0 0.6rem;border-top:1px solid rgba(255,255,255,0.10);padding-top:0.9rem;">'
        '<span style="font-size:0.70rem;text-transform:uppercase;letter-spacing:0.08em;opacity:0.40;font-weight:600;">'
        'Weitere Marktindikatoren &mdash; fließen nicht in den Gesamtscore ein'
        '</span></div>',
        unsafe_allow_html=True,
    )

    # --- Zusatzindikatoren ---
    extra_row_1 = st.columns(3)
    with extra_row_1[0]:
        dist10_tone = "neutral" if pd.isna(dist_10) else "good" if abs(dist_10) < 10 else "warn" if abs(dist_10) < 16 else "bad"
        render_kpi_card(
            label="Abstand 10-SMA",
            value=f"{dist_10:+.1f}%" if pd.notna(dist_10) else "n/a",
            interpretation="nah am Kurzfristtrend" if pd.notna(dist_10) and abs(dist_10) < 10 else "moderat entfernt" if pd.notna(dist_10) and abs(dist_10) < 16 else "stark erweitert",
            tone=dist10_tone,
            why_important="Der 10-SMA-Abstand zeigt, wie stark der Kurs vom sehr kurzfristigen Trend abweicht.",
            rule_note="Unter 10% Abstand ist gut.",
            compact=True,
        )
    with extra_row_1[1]:
        dist21_tone = "neutral" if pd.isna(dist_21) else "good" if abs(dist_21) < 14 else "warn" if abs(dist_21) < 20 else "bad"
        render_kpi_card(
            label="Abstand 21-EMA",
            value=f"{dist_21:+.1f}%" if pd.notna(dist_21) else "n/a",
            interpretation="im grünen Bereich" if pd.notna(dist_21) and abs(dist_21) < 14 else "leicht überdehnt" if pd.notna(dist_21) and abs(dist_21) < 20 else "deutlich überdehnt",
            tone=dist21_tone,
            glossary_key="21-EMA",
            why_important="Je weiter der Kurs von der 21-EMA entfernt ist, desto höher ist oft das kurzfristige Rücksetzerrisiko.",
            rule_note="Unter 14% Abstand ist gut.",
            compact=True,
        )
    with extra_row_1[2]:
        dist50_tone = "neutral" if pd.isna(dist_50) else "good" if abs(dist_50) <= 6 else "warn" if abs(dist_50) <= 14 else "bad"
        render_kpi_card(
            label="Abstand 50-SMA",
            value=f"{dist_50:+.1f}%" if pd.notna(dist_50) else "n/a",
            interpretation="nahe der 50-SMA" if pd.notna(dist_50) and abs(dist_50) <= 6 else "moderat entfernt" if pd.notna(dist_50) and abs(dist_50) <= 14 else "deutlich erweitert",
            tone=dist50_tone,
            glossary_key="50-SMA",
            why_important="Größere Abstände zur 50-SMA erhöhen oft das Rücksetzer-Risiko im laufenden Trend.",
            rule_note="Bis etwa ±6% stabil, darüber beobachten.",
            compact=True,
        )

    extra_row_2 = st.columns(3)
    with extra_row_2[0]:
        dist200_tone = "neutral" if pd.isna(dist_200) else "good" if dist_200 >= 0 else "warn" if dist_200 >= -8 else "bad"
        render_kpi_card(
            label="Abstand 200-SMA",
            value=f"{dist_200:+.1f}%" if pd.notna(dist_200) else "n/a",
            interpretation="oberhalb Langfristtrend" if pd.notna(dist_200) and dist_200 >= 0 else "leicht unter Langfristtrend" if pd.notna(dist_200) and dist_200 >= -8 else "deutlich unter Langfristtrend",
            tone=dist200_tone,
            why_important="Die 200-SMA zeigt den langfristigen Trendzustand und wirkt als Strukturfilter.",
            rule_note="Über 200-SMA ist der langfristige Trend konstruktiver.",
            compact=True,
        )
    with extra_row_2[1]:
        atr_tone = "neutral" if pd.isna(atr_pct) else "good" if atr_pct <= 2.5 else "warn" if atr_pct <= 6.0 else "bad"
        render_kpi_card(
            label="ATR / Volatilität",
            value=f"{atr_pct:.1f}%" if pd.notna(atr_pct) else "n/a",
            interpretation=f"{cat_lbl or 'ohne Kategorie'}",
            tone=atr_tone,
            glossary_key="ATR (21T)",
            why_important="Volatilität beeinflusst Schwankungsrisiko und sinnvolle Positionsgrößen.",
            rule_note="Niedrige ATR stabiler, hohe ATR volatiler.",
            compact=True,
        )
    with extra_row_2[2]:
        dist52_tone = "neutral" if pd.isna(drawdown_52w) else "good" if drawdown_52w >= -10 else "warn" if drawdown_52w >= -20 else "bad"
        dist52_interp = (
            "über 52W-Hoch" if pd.notna(drawdown_52w) and drawdown_52w > 0
            else "am 52W-Hoch" if pd.notna(drawdown_52w) and abs(drawdown_52w) < 0.2
            else "nahe Hoch" if pd.notna(drawdown_52w) and drawdown_52w >= -10
            else "spürbar darunter" if pd.notna(drawdown_52w) and drawdown_52w >= -20
            else "deutlich unter Hoch"
        )
        render_kpi_card(
            label="Abstand 52W-Hoch",
            value=f"{drawdown_52w:+.1f}%" if pd.notna(drawdown_52w) else "n/a",
            interpretation=dist52_interp,
            tone=dist52_tone,
            glossary_key="Drawdown",
            why_important="Der Abstand zum 52-Wochen-Hoch zeigt relative Stärke oder laufende Korrektur.",
            rule_note="Positiv = über dem bisherigen 52W-Hoch.",
            compact=True,
        )

    bullet_cols = st.columns(2)
    with bullet_cols[0]:
        st.markdown('<div class="info-card"><div class="card-label">Positive Treiber</div>', unsafe_allow_html=True)
        if assessment.get("drivers"):
            for item in assessment["drivers"]:
                st.markdown(f"- {item}")
        else:
            st.markdown("- Keine klaren positiven Treiber aus den aktuell verfügbaren Daten.")
        st.markdown("</div>", unsafe_allow_html=True)
    with bullet_cols[1]:
        st.markdown('<div class="info-card"><div class="card-label">Warnsignale</div>', unsafe_allow_html=True)
        if assessment.get("warnings"):
            for item in assessment["warnings"]:
                st.markdown(f"- {item}")
        else:
            st.markdown("- Aktuell keine dominanten Warnsignale im Regelset.")
        st.markdown("</div>", unsafe_allow_html=True)

    # Chart
    _vol_sma50 = df["Volume"].rolling(50).mean()
    rs_line = rs_ctx.get("rs_line") if isinstance(rs_ctx, dict) else None
    rs_ema21 = rs_ctx.get("ema21") if isinstance(rs_ctx, dict) else None
    rs_sma50 = rs_ctx.get("sma50") if isinstance(rs_ctx, dict) else None

    fig_stock = make_subplots(rows=3, cols=1, shared_xaxes=True, vertical_spacing=0.03, row_heights=[0.62, 0.18, 0.20])
    x = df.index
    fig_stock.add_trace(go.Candlestick(
        x=x, open=df["Open"], high=df["High"], low=df["Low"], close=df["Close"],
        increasing_line_color="#22c55e", decreasing_line_color="#ef4444",
        increasing_fillcolor="#22c55e", decreasing_fillcolor="#ef4444",
        name="Kurs", line=dict(width=1)), row=1, col=1)
    fig_stock.add_trace(go.Scatter(x=x, y=_ema21, name="21-EMA", line=dict(color="#06b6d4", width=1.2, dash="dot")), row=1, col=1)
    fig_stock.add_trace(go.Scatter(x=x, y=_sma50, name="50-SMA", line=dict(color="#f97316", width=1.2, dash="dot")), row=1, col=1)
    fig_stock.add_trace(go.Scatter(x=x, y=_sma200, name="200-SMA", line=dict(color="#a855f7", width=1.2, dash="dash")), row=1, col=1)
    vol_colors = ["#22c55e" if df["Close"].iloc[i] >= df["Open"].iloc[i] else "#ef4444" for i in range(len(df))]
    fig_stock.add_trace(go.Bar(x=x, y=df["Volume"], marker_color=vol_colors, opacity=0.5, name="Volumen", showlegend=False), row=2, col=1)
    fig_stock.add_trace(go.Scatter(x=x, y=_vol_sma50, name="Vol 50-SMA", line=dict(color="#64748b", width=1, dash="dot"), showlegend=False), row=2, col=1)
    if rs_line is not None and not rs_line.empty:
        fig_stock.add_trace(go.Scatter(x=rs_line.index, y=rs_line, name="RS-Linie", line=dict(color="#facc15", width=1.6)), row=3, col=1)
        if rs_ema21 is not None and len(rs_ema21) > 0:
            fig_stock.add_trace(go.Scatter(x=rs_ema21.index, y=rs_ema21, name="RS 21-EMA", line=dict(color="#38bdf8", width=1.0, dash="dot")), row=3, col=1)
        if rs_sma50 is not None and len(rs_sma50) > 0:
            fig_stock.add_trace(go.Scatter(x=rs_sma50.index, y=rs_sma50, name="RS 50-SMA", line=dict(color="#fb923c", width=1.0, dash="dash")), row=3, col=1)
    six_months_ago = df.index[-1] - pd.Timedelta(days=180)
    fig_stock.update_layout(
        template="plotly_white", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(248,250,252,0)",
        height=560, margin=dict(l=10, r=10, t=30, b=10), xaxis_rangeslider_visible=False,
        xaxis=dict(range=[six_months_ago, df.index[-1]], gridcolor="rgba(100,116,139,0.12)"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5, font=dict(size=10, color="#64748b")),
        yaxis=dict(title="", gridcolor="rgba(100,116,139,0.12)"), yaxis2=dict(title="", gridcolor="rgba(100,116,139,0.12)"), yaxis3=dict(title="", gridcolor="rgba(100,116,139,0.12)"), xaxis2=dict(gridcolor="rgba(100,116,139,0.12)"), xaxis3=dict(gridcolor="rgba(100,116,139,0.12)"),
    )
    fig_stock.update_xaxes(showgrid=False)
    st.plotly_chart(fig_stock, width="stretch", key="stock_chart")

    with st.expander("Fundamentale Checkliste", expanded=False):
        st.markdown('<div class="info-card">', unsafe_allow_html=True)
        for check in fundamentals_checks:
            render_check(_check_label(check), _check_ok(check), _check_detail(check), warn=_check_warn(check))
        extras = max(0, len(fundamentals_checks) - fundamental_total)
        sc = "#22c55e" if fundamental_met >= 7 else "#f59e0b" if fundamental_met >= 4 else "#ef4444"
        extras_note = f" · {extras} Zusatzinfos (Datenquellen, Institutionelle)" if extras > 0 else ""
        st.markdown(
            f'<div style="text-align:center;padding:8px;color:{sc};">'
            f'{fundamental_met}/{fundamental_total} Score-Kriterien erfüllt{extras_note}'
            '</div>',
            unsafe_allow_html=True,
        )
        st.markdown("</div>", unsafe_allow_html=True)

    with st.expander("Technische Checkliste", expanded=False):
        st.markdown('<div class="info-card">', unsafe_allow_html=True)
        tok = sum(1 for _, ok, _ in technical_checks if ok)
        for label, ok, detail in technical_checks:
            render_check(label, ok, detail)
        sc = "#22c55e" if tok >= 10 else "#f59e0b" if tok >= 6 else "#ef4444"
        st.markdown(f'<div style="text-align:center;padding:8px;color:{sc};">{tok}/{len(technical_checks)} Kriterien erfüllt</div>', unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with st.expander("Chartverhalten", expanded=False):
        sc1, sc2, sc3 = st.columns(3)
        for col, key, label, color in [(sc1, "positiv", "✓ Positiv", "#22c55e"), (sc2, "negativ", "✗ Negativ", "#ef4444"), (sc3, "neutral", "○ Neutral", "#94a3b8")]:
            with col:
                st.markdown(f'<div class="info-card" style="border-color:{color}30;"><div class="card-label" style="color:{color};">{label}</div>', unsafe_allow_html=True)
                if signs[key]:
                    for nm, dt in signs[key]:
                        st.markdown(f'<div style="padding:4px 0;border-bottom:1px solid #e3e8f0;"><div style="font-size:.84rem;color:{color};">{nm}</div><div style="font-size:.72rem;color:#64748b;">{dt}</div></div>', unsafe_allow_html=True)
                else:
                    st.markdown('<div style="color:#4a5568;font-size:.85rem;">Keine Zeichen</div>', unsafe_allow_html=True)
                st.markdown("</div>", unsafe_allow_html=True)

        st.markdown(
            f'<div class="info-card"><div class="card-label">Gesamtbewertung</div><div style="font-size:1rem;font-weight:700;color:{chart_color};">{chart_verdict}</div><div class="mini-help">{np_} Positiv · {nn} Negativ · {nu} Neutral · Score {chart_score:+d}</div></div>',
            unsafe_allow_html=True,
        )


SELL_MONITOR_STRENGTH_SIGNALS = [
    ("upper_third_closes", "Schlusskurse überwiegend im oberen Drittel der Tageskerze"),
    ("green_days_70", "ungefähr 70% grüne Tage in den ersten ein bis zwei Wochen"),
    ("positive_volume", "anziehendes Volumen an positiven Tagen, geringeres Volumen an Rücksetzern"),
    ("pullback_rebound", "Pullback an der 21- oder 50-MA wurde mit sichtbarer Stärke abgeprallt"),
    ("rs_line_strong", "RS-Linie steigt und verläuft über ihren gleitenden Durchschnitten"),
    ("strong_industry_group", "Industriegruppe gehört zu den stärksten Gruppen"),
]

SELL_MONITOR_WARNING_SIGNALS = [
    # Warning patterns that the Hub engine does NOT cover natively — kept as LM auto-warn.
    ("low_closes", "mehrere Schlusskurse nahe Tagestief"),
    ("distribution_cluster", "Häufung von Distributions-Tagen"),
    ("negative_market_divergence", "negative Divergenz zum Gesamtmarkt"),
    ("weak_rebounds", "schwache Erholungsversuche"),
    ("weak_industry_group", "Industriegruppe schwächelt"),
    # Removed (now covered by Hub strategies, see sell_decision_rules.WARNING_CONTRIBUTIONS):
    #   failed_breakout_high_volume → strategie_rueckkehr_pivot
    #   lower_lows_no_rebound       → strategie_verlusttage_haeufung
    #   stall_days_near_breakout    → strategie_stau_tage
    #   worst_day_high_volume       → strategie_groesster_einbruch
    #   downside_reversal_near_high → strategie_downside_reversal
    #   three_loss_weeks_rising_volume → strategie_drei_verlustwochen
]


def _render_sell_monitor_setup_panel(ticker: str, manual_data: dict) -> dict:
    """Render LM strategy setup (full Hub-parity) and return the active parameter dict.

    Mirrors the per-strategy setup panel from the Strategien-Hub: multiselect for
    active strategies, per-strategy descriptions, and per-strategy parameter inputs.
    Values are kept in st.session_state so changes apply on the next rerun.
    The Speichern-button persists current values into manual_data["sell_setup"].
    """
    saved = dict((manual_data or {}).get("sell_setup", {}) or {})

    def _val(key: str, default):
        if key in saved and saved.get(key) is not None:
            return saved[key]
        return default

    active = dict(LM_HUB_DEFAULTS)

    with st.expander("⚙️ Strategie-Setup (Hub-Engine)", expanded=False):
        st.caption("Vollständiges Setup analog zum Strategien-Hub. Alle Parameter wirken auf die Hub-Berechnung im Live-Monitor. Patterns #11 Distribution-Tage und #15 Volumen-Faktor bleiben LM-nativ.")

        # --- Strategie-Profil: kuratierte Bündel statt freier Multiselect-Liste ---
        st.markdown("#### 🧭 Strategie-Profil")
        st.caption('Profile bündeln aufeinander abgestimmte Strategien. Bei „Frei (Expert)" bleibt der volle Multiselect aktiv.')
        profile_keys = list(LM_HUB_PROFILES.keys())
        profile_labels = [str(LM_HUB_PROFILES[k].get("label") or k) for k in profile_keys]
        saved_profile = str(_val("profile", LM_HUB_PROFILE_DEFAULT) or LM_HUB_PROFILE_DEFAULT).strip().lower()
        if saved_profile not in profile_keys:
            saved_profile = LM_HUB_PROFILE_DEFAULT
        profile_index = profile_keys.index(saved_profile)
        chosen_profile_label = st.selectbox(
            "Profil",
            profile_labels,
            index=profile_index,
            key=f"lm_setup_profile_{ticker}",
        )
        chosen_profile_key = profile_keys[profile_labels.index(chosen_profile_label)]
        active["profile"] = chosen_profile_key
        profile_info = LM_HUB_PROFILES.get(chosen_profile_key) or {}
        st.caption(f"ℹ️ {profile_info.get('beschreibung', '')}")

        is_free_profile = chosen_profile_key == "frei"

        # --- Verkaufsstrategien (feste Verkaufsregeln) ---
        st.markdown("#### 🎯 Verkaufsstrategien")
        st.caption('Feste Verkaufsregeln mit klarer Tranche-Logik. Aktive Signale erscheinen im Bereich „Tranche-Signale".')
        if is_free_profile:
            strat_default = list(_val("active_strategies", list(LM_HUB_STRATEGIEN_DEFAULT)) or LM_HUB_STRATEGIEN_DEFAULT)
            strat_default = [k for k in strat_default if k in LM_HUB_STRATEGIEN]
            if not strat_default:
                strat_default = list(LM_HUB_STRATEGIEN_DEFAULT)
            selected_strats = st.multiselect(
                "Aktive Strategien",
                LM_HUB_STRATEGIEN,
                default=strat_default,
                key=f"lm_setup_active_strats_{ticker}",
                help="Standardmäßig sind 12 Strategien aktiv. Opt-in: atr_basiert, ma_basierte_sequenz, einfach_halbe_position, einfache_verluststufen, misslungener_ausbruch_5stufen.",
            )
        else:
            profile_strats = [k for k in (profile_info.get("strategien") or []) if k in LM_HUB_STRATEGIEN]
            selected_strats = profile_strats
            strat_list_html = html.escape(', '.join(profile_strats) or '—')
            profile_label_html = html.escape(str(profile_info.get('label') or chosen_profile_key))
            st.markdown(
                f'<div class="mini-help">Im Profil <b>{profile_label_html}</b> aktive Strategien: {strat_list_html}. '
                f'Für eine freie Auswahl auf <i>Frei (Expert)</i> wechseln.</div>',
                unsafe_allow_html=True,
            )
        active["active_strategies"] = selected_strats

        with st.expander("ℹ️ Strategie-Erklärungen", expanded=False):
            for key in selected_strats:
                st.markdown(f"**{key}** – {STRATEGIE_INFO.get(key, 'Keine Beschreibung hinterlegt.')}")

        # --- Warnsignale (Verhaltens-/Distributionssignale) ---
        st.markdown("#### ⚠️ Warnsignale (Hub)")
        st.caption('Verhaltens- und Distributionsmuster als Frühwarnung. Aktive Signale erscheinen im separaten Bereich „Warnsignale" und tragen weiterhin zur Gesamt-Tranche bei.')
        if is_free_profile:
            warn_default = list(_val("active_warnings", list(LM_HUB_WARNUNGEN_DEFAULT)) or LM_HUB_WARNUNGEN_DEFAULT)
            warn_default = [k for k in warn_default if k in LM_HUB_WARNUNGEN]
            if not warn_default:
                warn_default = list(LM_HUB_WARNUNGEN_DEFAULT)
            selected_warns = st.multiselect(
                "Aktive Warnsignale",
                LM_HUB_WARNUNGEN,
                default=warn_default,
                key=f"lm_setup_active_warns_{ticker}",
                help="Standardmäßig sind 3 Warnsignale aktiv. Opt-in: groesster_anstieg_volumen (Klimax-Single-Day), erschoepfungsluecke (Gap-up Klimax).",
            )
        else:
            profile_warns = [k for k in (profile_info.get("warnungen") or []) if k in LM_HUB_WARNUNGEN]
            selected_warns = profile_warns
            st.markdown(f"<div class='mini-help'>Im Profil aktive Warnsignale: {', '.join(profile_warns) or '—'}.</div>", unsafe_allow_html=True)
        active["active_warnings"] = selected_warns

        with st.expander("ℹ️ Warnsignal-Erklärungen", expanded=False):
            for key in selected_warns:
                st.markdown(f"**{key}** – {STRATEGIE_INFO.get(key, 'Keine Beschreibung hinterlegt.')}")

        st.markdown("#### Parameter je Strategie/Warnsignal")

        selected = selected_strats + selected_warns

        for key in selected:
            with st.expander(f"Strategie: {key}", expanded=False):
                st.caption(STRATEGIE_INFO.get(key, "Keine Beschreibung hinterlegt."))

                if key == "notbremse_verlust":
                    c1, c2, c3 = st.columns(3)
                    active["notbremse_verlust_schwelle_baerisch_pct"] = c1.number_input("Notbremse Bärisch (%)", min_value=0.5, max_value=30.0, value=float(_val("notbremse_verlust_schwelle_baerisch_pct", 4.0)), step=0.5, key=f"lm_setup_notbremse_bear_{ticker}")
                    active["notbremse_verlust_schwelle_unsicher_pct"] = c2.number_input("Notbremse Unsicher (%)", min_value=0.5, max_value=30.0, value=float(_val("notbremse_verlust_schwelle_unsicher_pct", 5.0)), step=0.5, key=f"lm_setup_notbremse_uncertain_{ticker}")
                    active["notbremse_verlust_schwelle_bullisch_pct"] = c3.number_input("Notbremse Bullisch (%)", min_value=0.5, max_value=30.0, value=float(_val("notbremse_verlust_schwelle_bullisch_pct", 7.0)), step=0.5, key=f"lm_setup_notbremse_bull_{ticker}")

                elif key == "rueckkehr_pivot":
                    c1, c2 = st.columns(2)
                    active["rueckkehr_tranche_stufe1_pct"] = c1.number_input("Tranche Sicherheitslinie 1 (%)", min_value=1.0, max_value=100.0, value=float(_val("rueckkehr_tranche_stufe1_pct", 33.0)), step=1.0, key=f"lm_setup_rueckkehr_t1_{ticker}")
                    active["rueckkehr_tranche_stufe1_volumen_pct"] = c1.number_input("Tranche Sicherheitslinie 1 bei Volumen (%)", min_value=1.0, max_value=100.0, value=float(_val("rueckkehr_tranche_stufe1_volumen_pct", 50.0)), step=1.0, key=f"lm_setup_rueckkehr_t1_vol_{ticker}")
                    active["rueckkehr_volumen_schwelle"] = c1.number_input("Volumenquoten-Schwelle", min_value=0.5, max_value=10.0, value=float(_val("rueckkehr_volumen_schwelle", 1.5)), step=0.1, key=f"lm_setup_rueckkehr_vol_schwelle_{ticker}")
                    active["rueckkehr_notbremse_verlust_pct"] = c1.number_input("Notbremse Verlust (%)", min_value=1.0, max_value=30.0, value=float(_val("rueckkehr_notbremse_verlust_pct", 7.0)), step=0.5, key=f"lm_setup_rueckkehr_notbremse_{ticker}")
                    active["rueckkehr_tranche_stufe2_pct"] = c2.number_input("Tranche Sicherheitslinie 2 (%)", min_value=1.0, max_value=100.0, value=float(_val("rueckkehr_tranche_stufe2_pct", 33.0)), step=1.0, key=f"lm_setup_rueckkehr_t2_{ticker}")
                    active["rueckkehr_pivot_tage_schwelle"] = int(c2.number_input("Tage unter Pivot", min_value=1, max_value=60, value=int(_val("rueckkehr_pivot_tage_schwelle", 10)), step=1, key=f"lm_setup_rueckkehr_pivot_tage_{ticker}"))
                    active["rueckkehr_tranche_pivot_pct"] = c2.number_input("Tranche Zeitkomponente Pivot (%)", min_value=1.0, max_value=100.0, value=float(_val("rueckkehr_tranche_pivot_pct", 50.0)), step=1.0, key=f"lm_setup_rueckkehr_pivot_tranche_{ticker}")

                elif key == "gewinn_in_stufen":
                    c1, c2 = st.columns(2)
                    active["gewinn_nachdenken_schwelle_bull_pct"] = c1.number_input("Nachdenkschwelle Bull/Unsicher (%)", min_value=0.0, max_value=200.0, value=float(_val("gewinn_nachdenken_schwelle_bull_pct", 15.0)), step=0.5, key=f"lm_setup_gain_nd_bull_{ticker}")
                    active["gewinn_teilverkauf_unten_bull_pct"] = c1.number_input("Gewinnzone unten Bull/Unsicher (%)", min_value=0.0, max_value=200.0, value=float(_val("gewinn_teilverkauf_unten_bull_pct", 20.0)), step=0.5, key=f"lm_setup_gain_lo_bull_{ticker}")
                    active["gewinn_teilverkauf_oben_bull_pct"] = c1.number_input("Gewinnzone oben Bull/Unsicher (%)", min_value=0.0, max_value=300.0, value=float(_val("gewinn_teilverkauf_oben_bull_pct", 35.0)), step=0.5, key=f"lm_setup_gain_hi_bull_{ticker}")
                    active["gewinn_nachdenken_schwelle_bear_pct"] = c2.number_input("Nachdenkschwelle Bärisch (%)", min_value=0.0, max_value=200.0, value=float(_val("gewinn_nachdenken_schwelle_bear_pct", 10.0)), step=0.5, key=f"lm_setup_gain_nd_bear_{ticker}")
                    active["gewinn_teilverkauf_unten_bear_pct"] = c2.number_input("Gewinnzone unten Bärisch (%)", min_value=0.0, max_value=200.0, value=float(_val("gewinn_teilverkauf_unten_bear_pct", 10.0)), step=0.5, key=f"lm_setup_gain_lo_bear_{ticker}")
                    active["gewinn_teilverkauf_oben_bear_pct"] = c2.number_input("Gewinnzone oben Bärisch (%)", min_value=0.0, max_value=300.0, value=float(_val("gewinn_teilverkauf_oben_bear_pct", 15.0)), step=0.5, key=f"lm_setup_gain_hi_bear_{ticker}")

                elif key == "ma21_bruch":
                    variants = ["gestaffelt", "aggressiv", "geduldig"]
                    default_variant = str(_val("ma21_variante", "gestaffelt"))
                    if default_variant not in variants:
                        default_variant = "gestaffelt"
                    active["ma21_variante"] = st.selectbox("Variante 21-EMA-Bruch", variants, index=variants.index(default_variant), key=f"lm_setup_ma21_var_{ticker}", help="Einmalig festlegen, wie offensiv Strategie 4 den Bruch der 21-EMA behandelt.")

                elif key == "drawdown_vom_peak":
                    c1, c2, c3 = st.columns(3)
                    active["drawdown_stufe1_min_pct"] = c1.number_input("Stufe 1 ab (%)", min_value=1.0, max_value=50.0, value=float(_val("drawdown_stufe1_min_pct", 8.0)), step=0.5, key=f"lm_setup_dd_s1_{ticker}")
                    active["drawdown_stufe2_min_pct"] = c2.number_input("Stufe 2 ab (%)", min_value=1.0, max_value=80.0, value=float(_val("drawdown_stufe2_min_pct", 12.0)), step=0.5, key=f"lm_setup_dd_s2_{ticker}")
                    active["drawdown_stufe3_min_pct"] = c3.number_input("Stufe 3 ab (%)", min_value=1.0, max_value=100.0, value=float(_val("drawdown_stufe3_min_pct", 15.0)), step=0.5, key=f"lm_setup_dd_s3_{ticker}")
                    c4, c5, c6 = st.columns(3)
                    active["drawdown_tranche_stufe1_pct"] = c4.number_input("Tranche Stufe 1 (%)", min_value=1.0, max_value=100.0, value=float(_val("drawdown_tranche_stufe1_pct", 25.0)), step=1.0, key=f"lm_setup_dd_t1_{ticker}")
                    active["drawdown_tranche_stufe2_pct"] = c5.number_input("Tranche Stufe 2 (%)", min_value=1.0, max_value=100.0, value=float(_val("drawdown_tranche_stufe2_pct", 33.0)), step=1.0, key=f"lm_setup_dd_t2_{ticker}")
                    active["drawdown_tranche_stufe3_ohne_trendbruch_pct"] = c6.number_input("Tranche Stufe 3 ohne Trendbruch (%)", min_value=1.0, max_value=100.0, value=float(_val("drawdown_tranche_stufe3_ohne_trendbruch_pct", 50.0)), step=1.0, key=f"lm_setup_dd_t3a_{ticker}")
                    active["drawdown_tranche_stufe3_mit_trendbruch_pct"] = st.number_input("Tranche Stufe 3 mit Trendbruch (%)", min_value=1.0, max_value=100.0, value=float(_val("drawdown_tranche_stufe3_mit_trendbruch_pct", 100.0)), step=1.0, key=f"lm_setup_dd_t3b_{ticker}")

                elif key == "ma_abstand":
                    c1, c2, c3 = st.columns(3)
                    active["ma_abstand_schwelle_ma10_pct"] = c1.number_input("Schwelle 10-MA (%)", min_value=1.0, max_value=50.0, value=float(_val("ma_abstand_schwelle_ma10_pct", 10.0)), step=0.5, key=f"lm_setup_dist10_{ticker}")
                    active["ma_abstand_schwelle_ma21_pct"] = c2.number_input("Schwelle 21-EMA (%)", min_value=1.0, max_value=80.0, value=float(_val("ma_abstand_schwelle_ma21_pct", 15.0)), step=0.5, key=f"lm_setup_dist21_{ticker}")
                    active["ma_abstand_schwelle_ma50_pct"] = c3.number_input("Schwelle 50-MA (%)", min_value=1.0, max_value=120.0, value=float(_val("ma_abstand_schwelle_ma50_pct", 25.0)), step=0.5, key=f"lm_setup_dist50_{ticker}")
                    c4, c5 = st.columns(2)
                    active["ma_abstand_schwelle_ma200_pct"] = c4.number_input("Klimax 200-MA (%)", min_value=10.0, max_value=200.0, value=float(_val("ma_abstand_schwelle_ma200_pct", 70.0)), step=1.0, key=f"lm_setup_dist200_{ticker}")
                    active["ma_abstand_klimax_ma200_vollausstieg_pct"] = c5.number_input("Vollausstieg ab 200-MA (%)", min_value=20.0, max_value=300.0, value=float(_val("ma_abstand_klimax_ma200_vollausstieg_pct", 100.0)), step=1.0, key=f"lm_setup_dist200_full_{ticker}")
                    c6, c7, c8, c9 = st.columns(4)
                    active["ma_abstand_tranche_ma10_pct"] = c6.number_input("Tranche 10-MA (%)", min_value=1.0, max_value=100.0, value=float(_val("ma_abstand_tranche_ma10_pct", 25.0)), step=1.0, key=f"lm_setup_dist10_tr_{ticker}")
                    active["ma_abstand_tranche_ma21_pct"] = c7.number_input("Tranche 21-EMA (%)", min_value=1.0, max_value=100.0, value=float(_val("ma_abstand_tranche_ma21_pct", 33.0)), step=1.0, key=f"lm_setup_dist21_tr_{ticker}")
                    active["ma_abstand_tranche_ma50_pct"] = c8.number_input("Tranche 50-MA (%)", min_value=1.0, max_value=100.0, value=float(_val("ma_abstand_tranche_ma50_pct", 33.0)), step=1.0, key=f"lm_setup_dist50_tr_{ticker}")
                    active["ma_abstand_tranche_ma200_basis_pct"] = c9.number_input("Tranche 200-MA Basis (%)", min_value=1.0, max_value=100.0, value=float(_val("ma_abstand_tranche_ma200_basis_pct", 50.0)), step=1.0, key=f"lm_setup_dist200_tr_{ticker}")

                elif key == "verlusttage_haeufung":
                    c1, c2, c3 = st.columns(3)
                    active["verlusttage_min_tiefere_schlusskurse_in_folge"] = int(c1.number_input("Min. tiefere Schlusskurse in Folge", min_value=2, max_value=5, value=int(_val("verlusttage_min_tiefere_schlusskurse_in_folge", 3)), step=1, key=f"lm_setup_losscluster_seq_{ticker}"))
                    active["verlusttage_tief_marker_lookback_tage"] = int(c1.number_input("Tief-Marker Lookback", min_value=2, max_value=20, value=int(_val("verlusttage_tief_marker_lookback_tage", 5)), step=1, key=f"lm_setup_losscluster_low_{ticker}"))
                    active["verlusttage_volumen_lookback_tage"] = int(c2.number_input("Volumen-Lookback (Tage)", min_value=10, max_value=200, value=int(_val("verlusttage_volumen_lookback_tage", 50)), step=1, key=f"lm_setup_losscluster_vollb_{ticker}"))
                    active["verlusttage_volumen_ratio_min"] = c2.number_input("Min. Volumenquote (3T/LB)", min_value=0.8, max_value=3.0, value=float(_val("verlusttage_volumen_ratio_min", 1.1)), step=0.05, key=f"lm_setup_losscluster_volr_{ticker}")
                    active["verlusttage_updown_fenster_tage"] = int(c3.number_input("Up/Down-Fenster (Tage)", min_value=5, max_value=40, value=int(_val("verlusttage_updown_fenster_tage", 15)), step=1, key=f"lm_setup_losscluster_udw_{ticker}"))
                    active["verlusttage_updown_diff_min"] = int(c3.number_input("Min. Abwärts-Überhang", min_value=1, max_value=10, value=int(_val("verlusttage_updown_diff_min", 3)), step=1, key=f"lm_setup_losscluster_udd_{ticker}"))
                    active["verlusttage_tranche_pct"] = st.number_input("Tranche je Signal (%)", min_value=1.0, max_value=100.0, value=float(_val("verlusttage_tranche_pct", 25.0)), step=1.0, key=f"lm_setup_losscluster_tr_{ticker}")

                elif key == "split_anstieg":
                    c1, c2 = st.columns(2)
                    active["split_signal_schwelle_pct"] = c1.number_input("Signal-Schwelle (%)", min_value=5.0, max_value=100.0, value=float(_val("split_signal_schwelle_pct", 25.0)), step=0.5, key=f"lm_setup_split_sig_{ticker}")
                    active["split_starke_schwelle_pct"] = c2.number_input("Starke Tranche ab (%)", min_value=10.0, max_value=200.0, value=float(_val("split_starke_schwelle_pct", 50.0)), step=0.5, key=f"lm_setup_split_strong_{ticker}")
                    manual_split = st.date_input("Manuelles Split-Datum (optional)", value=None, key=f"lm_setup_split_date_{ticker}")
                    if manual_split:
                        active["split_datum"] = pd.Timestamp(manual_split)

                elif key == "downside_reversal":
                    c1, c2, c3 = st.columns(3)
                    active["downside_kerzenweite_lookback_tage"] = int(c1.number_input("Kerzenweiten-Lookback", min_value=5, max_value=60, value=int(_val("downside_kerzenweite_lookback_tage", 10)), step=1, key=f"lm_setup_downside_cl_{ticker}"))
                    active["downside_neues_hoch_lookback_tage"] = int(c1.number_input("Neues-Hoch-Lookback", min_value=10, max_value=120, value=int(_val("downside_neues_hoch_lookback_tage", 30)), step=1, key=f"lm_setup_downside_hl_{ticker}"))
                    active["downside_schluss_unteres_drittel_faktor"] = c1.number_input("Schluss im unteren 1/x", min_value=2.0, max_value=6.0, value=float(_val("downside_schluss_unteres_drittel_faktor", 3.0)), step=0.1, key=f"lm_setup_downside_lt_{ticker}")
                    active["downside_volumen_lookback_tage"] = int(c2.number_input("Volumen-Lookback", min_value=20, max_value=200, value=int(_val("downside_volumen_lookback_tage", 50)), step=1, key=f"lm_setup_downside_vlb_{ticker}"))
                    active["downside_volumen_ratio_min"] = c2.number_input("Min. Volumenquote", min_value=0.8, max_value=5.0, value=float(_val("downside_volumen_ratio_min", 1.2)), step=0.1, key=f"lm_setup_downside_vr_{ticker}")
                    active["downside_weite_kerze_faktor"] = c2.number_input("Weite-Kerze-Faktor", min_value=1.0, max_value=4.0, value=float(_val("downside_weite_kerze_faktor", 1.5)), step=0.1, key=f"lm_setup_downside_wf_{ticker}")
                    active["downside_tranche_neues_hoch_pct"] = c3.number_input("Tranche Variante 1 (%)", min_value=1.0, max_value=100.0, value=float(_val("downside_tranche_neues_hoch_pct", 33.0)), step=1.0, key=f"lm_setup_downside_t1_{ticker}")
                    active["downside_tranche_weite_umkehr_pct"] = c3.number_input("Tranche Variante 2 (%)", min_value=1.0, max_value=100.0, value=float(_val("downside_tranche_weite_umkehr_pct", 20.0)), step=1.0, key=f"lm_setup_downside_t2_{ticker}")
                    active["downside_tranche_warnstufe_pct"] = c3.number_input("Tranche Warnstufe (%)", min_value=1.0, max_value=100.0, value=float(_val("downside_tranche_warnstufe_pct", 15.0)), step=1.0, key=f"lm_setup_downside_t3_{ticker}")

                elif key == "stau_tage":
                    c1, c2, c3 = st.columns(3)
                    active["stau_fenster_tage"] = int(c1.number_input("Fenster (Sessions)", min_value=5, max_value=30, value=int(_val("stau_fenster_tage", 10)), step=1, key=f"lm_setup_stall_window_{ticker}"))
                    active["stau_max_tagesveraenderung_pct"] = c1.number_input("Max. Tagesveränderung (%)", min_value=0.1, max_value=5.0, value=float(_val("stau_max_tagesveraenderung_pct", 1.0)), step=0.1, key=f"lm_setup_stall_maxch_{ticker}")
                    active["stau_min_tage"] = int(c1.number_input("Min. Stau-Tage", min_value=1, max_value=10, value=int(_val("stau_min_tage", 2)), step=1, key=f"lm_setup_stall_min_days_{ticker}"))
                    active["stau_volumen_lookback_tage"] = int(c2.number_input("Volumen-Lookback (Tage)", min_value=10, max_value=200, value=int(_val("stau_volumen_lookback_tage", 50)), step=1, key=f"lm_setup_stall_vlb_{ticker}"))
                    active["stau_min_vol_ratio"] = c2.number_input("Min. Volumenfaktor", min_value=0.8, max_value=5.0, value=float(_val("stau_min_vol_ratio", 1.3)), step=0.1, key=f"lm_setup_stall_vol_{ticker}")
                    active["stau_nahe_hoch_drawdown_max_pct"] = c2.number_input("Nahe-Hoch Drawdown max (%)", min_value=0.5, max_value=20.0, value=float(_val("stau_nahe_hoch_drawdown_max_pct", 5.0)), step=0.5, key=f"lm_setup_stall_nh_{ticker}")
                    active["stau_tranche_nahe_hoch_pct"] = c3.number_input("Tranche nahe Hoch (%)", min_value=1.0, max_value=100.0, value=float(_val("stau_tranche_nahe_hoch_pct", 33.0)), step=1.0, key=f"lm_setup_stall_t1_{ticker}")
                    active["stau_tranche_standard_pct"] = c3.number_input("Standard-Tranche (%)", min_value=1.0, max_value=100.0, value=float(_val("stau_tranche_standard_pct", 20.0)), step=1.0, key=f"lm_setup_stall_t2_{ticker}")

                elif key == "groesster_einbruch":
                    c1, c2 = st.columns(2)
                    active["groesster_einbruch_min_pnl_pct"] = c1.number_input("Mindest-P&L vor Aktivierung (%)", min_value=0.0, max_value=300.0, value=float(_val("groesster_einbruch_min_pnl_pct", 10.0)), step=0.5, key=f"lm_setup_drop_minpnl_{ticker}")
                    active["groesster_einbruch_min_tagesverlust_pct"] = c1.number_input("Mindest-Tagesverlust (%)", min_value=0.5, max_value=30.0, value=float(_val("groesster_einbruch_min_tagesverlust_pct", 3.0)), step=0.1, key=f"lm_setup_drop_minday_{ticker}")
                    active["groesster_einbruch_tagesvol_ratio_schwelle"] = c2.number_input("Volumenfaktor Tagesregel", min_value=0.5, max_value=10.0, value=float(_val("groesster_einbruch_tagesvol_ratio_schwelle", 1.5)), step=0.1, key=f"lm_setup_drop_dayvol_{ticker}")
                    active["groesster_einbruch_wochenvol_ratio_schwelle"] = c2.number_input("Volumenfaktor Wochenregel", min_value=0.5, max_value=10.0, value=float(_val("groesster_einbruch_wochenvol_ratio_schwelle", 1.3)), step=0.1, key=f"lm_setup_drop_weekvol_{ticker}")

                elif key == "rs_linie":
                    c1, c2 = st.columns(2)
                    active["rs_pnl_tag_zu_woche"] = c1.number_input("P&L-Schwelle Tag → Woche (%)", min_value=0.0, max_value=200.0, value=float(_val("rs_pnl_tag_zu_woche", 20.0)), step=0.5, key=f"lm_setup_rs_dw_{ticker}")
                    active["rs_pnl_woche_zu_monat"] = c2.number_input("P&L-Schwelle Woche → Monat (%)", min_value=0.0, max_value=300.0, value=float(_val("rs_pnl_woche_zu_monat", 80.0)), step=0.5, key=f"lm_setup_rs_wm_{ticker}")

                elif key == "ma_basierte_sequenz":
                    c1, c2, c3 = st.columns(3)
                    active["ma_seq_gewinnzone_min_pct"] = c1.number_input("Gewinnzone min (%)", min_value=0.0, max_value=200.0, value=float(_val("ma_seq_gewinnzone_min_pct", 20.0)), step=0.5, key=f"lm_setup_seq_zmin_{ticker}")
                    active["ma_seq_gewinnzone_tranche_pct"] = c1.number_input("Tranche Punkt 1 (%)", min_value=1.0, max_value=100.0, value=float(_val("ma_seq_gewinnzone_tranche_pct", 33.0)), step=1.0, key=f"lm_setup_seq_t1_{ticker}")
                    active["ma_seq_unter_ma10_mindestgewinn_pct"] = c1.number_input("Min. P&L für Punkt 3 (%)", min_value=-50.0, max_value=200.0, value=float(_val("ma_seq_unter_ma10_mindestgewinn_pct", 5.0)), step=0.5, key=f"lm_setup_seq_p3p_{ticker}")
                    active["ma_seq_pendel_lookback_tage"] = int(c1.number_input("Pendel-Lookback (Tage)", min_value=2, max_value=20, value=int(_val("ma_seq_pendel_lookback_tage", 5)), step=1, key=f"lm_setup_seq_plb_{ticker}"))
                    active["ma_seq_gewinnzone_max_pct"] = c2.number_input("Gewinnzone max (%)", min_value=0.0, max_value=200.0, value=float(_val("ma_seq_gewinnzone_max_pct", 25.0)), step=0.5, key=f"lm_setup_seq_zmax_{ticker}")
                    active["ma_seq_ueber_ma10_pct"] = c2.number_input("Überdehnung über 10-MA (%)", min_value=0.5, max_value=100.0, value=float(_val("ma_seq_ueber_ma10_pct", 10.0)), step=0.5, key=f"lm_setup_seq_p2t_{ticker}")
                    active["ma_seq_unter_ma10_tranche_pct"] = c2.number_input("Tranche Punkt 3 normal (%)", min_value=1.0, max_value=100.0, value=float(_val("ma_seq_unter_ma10_tranche_pct", 20.0)), step=1.0, key=f"lm_setup_seq_t3n_{ticker}")
                    active["ma_seq_pendel_wechsel_min"] = int(c2.number_input("Min. Pendel-Wechsel", min_value=1, max_value=10, value=int(_val("ma_seq_pendel_wechsel_min", 3)), step=1, key=f"lm_setup_seq_pw_{ticker}"))
                    active["ma_seq_ueber_ma10_tranche_pct"] = c3.number_input("Tranche Punkt 2 (%)", min_value=1.0, max_value=100.0, value=float(_val("ma_seq_ueber_ma10_tranche_pct", 20.0)), step=1.0, key=f"lm_setup_seq_t2_{ticker}")
                    active["ma_seq_pendel_tranche_pct"] = c3.number_input("Tranche Punkt 3 pendelnd (%)", min_value=1.0, max_value=100.0, value=float(_val("ma_seq_pendel_tranche_pct", 25.0)), step=1.0, key=f"lm_setup_seq_t3p_{ticker}")
                    active["ma_seq_unter_ma21_tranche_pct"] = c3.number_input("Tranche Punkt 4 (%)", min_value=1.0, max_value=100.0, value=float(_val("ma_seq_unter_ma21_tranche_pct", 25.0)), step=1.0, key=f"lm_setup_seq_t4_{ticker}")
                    active["ma_seq_klarer_ma50_bruch_pct"] = c3.number_input("Klarer 50-MA-Bruch (%)", min_value=0.5, max_value=20.0, value=float(_val("ma_seq_klarer_ma50_bruch_pct", 2.0)), step=0.5, key=f"lm_setup_seq_p5_{ticker}")
                    active["ma_seq_unter_ma21_mindestgewinn_pct"] = st.number_input("Min. P&L für Punkt 4 (%)", min_value=-50.0, max_value=200.0, value=float(_val("ma_seq_unter_ma21_mindestgewinn_pct", 5.0)), step=0.5, key=f"lm_setup_seq_p4p_{ticker}")

                elif key == "einfach_halbe_position":
                    active["erste_haelfte_gewinn_pct"] = st.number_input("Gewinnmitnahme 1. Hälfte (%)", min_value=5.0, max_value=100.0, value=float(_val("erste_haelfte_gewinn_pct", 20.0)), step=0.5, key=f"lm_setup_half_{ticker}")

                elif key == "einfache_verluststufen":
                    c1, c2, c3 = st.columns(3)
                    active["verlust_stufe_1"] = c1.number_input("Verluststufe 1 (%)", min_value=0.5, max_value=30.0, value=float(_val("verlust_stufe_1", 3.0)), step=0.5, key=f"lm_setup_loss_s1_{ticker}")
                    active["verlust_stufe_2"] = c2.number_input("Verluststufe 2 (%)", min_value=0.5, max_value=30.0, value=float(_val("verlust_stufe_2", 5.0)), step=0.5, key=f"lm_setup_loss_s2_{ticker}")
                    active["verlust_stufe_3"] = c3.number_input("Verluststufe 3 (%)", min_value=0.5, max_value=30.0, value=float(_val("verlust_stufe_3", 7.0)), step=0.5, key=f"lm_setup_loss_s3_{ticker}")

                elif key == "atr_basiert":
                    c1, c2, c3 = st.columns(3)
                    active["ziel_atr_multiplikator"] = c1.number_input("ATR-Ziel (Multiplikator)", min_value=1.0, max_value=10.0, value=float(_val("ziel_atr_multiplikator", 3.0)), step=0.5, key=f"lm_setup_atr_target_{ticker}")
                    active["ueberdehnung_atr_start"] = c2.number_input("ATR über 21-EMA (Start)", min_value=1.0, max_value=10.0, value=float(_val("ueberdehnung_atr_start", 3.0)), step=0.5, key=f"lm_setup_atr_ext_start_{ticker}")
                    active["ueberdehnung_atr_stark"] = c3.number_input("ATR über 21-EMA (Stark)", min_value=1.0, max_value=10.0, value=float(_val("ueberdehnung_atr_stark", 4.0)), step=0.5, key=f"lm_setup_atr_ext_strong_{ticker}")

                else:
                    st.caption("Für diese Strategie sind aktuell keine zusätzlichen Parameter konfigurierbar.")

        if st.button("💾 Setup für diesen Ticker speichern", key=f"lm_setup_save_{ticker}", type="secondary"):
            new_manual = dict(get_position_manual_sell_data(ticker) or {})
            persistable = {k: v for k, v in active.items() if not isinstance(v, pd.Timestamp)}
            # Monotonie analog Strategien-Hub erzwingen, damit User keine
            # widersprüchlichen Schwellen persistieren können (z. B. „Stark"
            # < „Start" beim ATR oder „Stufe2" < „Stufe1" beim Drawdown).
            def _ensure_monotone(d, lower_key, upper_key):
                if lower_key in d and upper_key in d:
                    try:
                        d[upper_key] = float(max(float(d[upper_key]), float(d[lower_key])))
                    except (TypeError, ValueError):
                        pass
            _ensure_monotone(persistable, "ueberdehnung_atr_start", "ueberdehnung_atr_stark")
            _ensure_monotone(persistable, "verlust_stufe_1", "verlust_stufe_2")
            _ensure_monotone(persistable, "verlust_stufe_2", "verlust_stufe_3")
            _ensure_monotone(persistable, "drawdown_stufe1_min_pct", "drawdown_stufe2_min_pct")
            _ensure_monotone(persistable, "drawdown_stufe2_min_pct", "drawdown_stufe3_min_pct")
            _ensure_monotone(persistable, "drawdown_tranche_stufe3_ohne_trendbruch_pct", "drawdown_tranche_stufe3_mit_trendbruch_pct")
            _ensure_monotone(persistable, "gewinn_nachdenken_schwelle_bull_pct", "gewinn_teilverkauf_unten_bull_pct")
            _ensure_monotone(persistable, "gewinn_teilverkauf_unten_bull_pct", "gewinn_teilverkauf_oben_bull_pct")
            _ensure_monotone(persistable, "gewinn_nachdenken_schwelle_bear_pct", "gewinn_teilverkauf_unten_bear_pct")
            _ensure_monotone(persistable, "gewinn_teilverkauf_unten_bear_pct", "gewinn_teilverkauf_oben_bear_pct")
            _ensure_monotone(persistable, "ma_seq_unter_ma10_tranche_pct", "ma_seq_pendel_tranche_pct")
            _ensure_monotone(persistable, "ma_seq_gewinnzone_min_pct", "ma_seq_gewinnzone_max_pct")
            _ensure_monotone(persistable, "ma_abstand_schwelle_ma10_pct", "ma_abstand_schwelle_ma21_pct")
            _ensure_monotone(persistable, "ma_abstand_schwelle_ma21_pct", "ma_abstand_schwelle_ma50_pct")
            _ensure_monotone(persistable, "ma_abstand_schwelle_ma50_pct", "ma_abstand_schwelle_ma200_pct")
            _ensure_monotone(persistable, "ma_abstand_schwelle_ma200_pct", "ma_abstand_klimax_ma200_vollausstieg_pct")
            _ensure_monotone(persistable, "rs_pnl_tag_zu_woche", "rs_pnl_woche_zu_monat")
            _ensure_monotone(persistable, "split_signal_schwelle_pct", "split_starke_schwelle_pct")
            new_manual["sell_setup"] = persistable
            save_position_manual_sell_data(ticker, new_manual)
            st.success("Setup gespeichert.")
            st.rerun()

    return active


def _sell_monitor_auto_checkbox_data(metrics_payload: dict) -> tuple[dict, dict, dict]:
    auto = (metrics_payload or {}).get("auto_checkboxes", {}) if isinstance(metrics_payload, dict) else {}
    if not isinstance(auto, dict):
        return {}, {}, {}
    strength = auto.get("strength_checkboxes") if isinstance(auto.get("strength_checkboxes"), dict) else {}
    warning = auto.get("warning_checkboxes") if isinstance(auto.get("warning_checkboxes"), dict) else {}
    reasons = auto.get("reasons") if isinstance(auto.get("reasons"), dict) else {}
    return strength, warning, reasons


def _sell_monitor_fmt_money(value, currency: str = "") -> str:
    if value is None or pd.isna(value):
        return "—"
    symbol = "€" if str(currency).upper() == "EUR" else "$" if str(currency).upper() == "USD" else str(currency or "")
    return f"{float(value):,.2f} {symbol}".replace(",", ".")


def _sell_monitor_distance(price, reference) -> float | None:
    price = _safe_float(price, np.nan)
    reference = _safe_float(reference, np.nan)
    if np.isnan(price) or np.isnan(reference) or reference <= 0:
        return None
    return (price / reference - 1) * 100


def _sell_decision_ui_css() -> None:
    st.markdown("""
    <style>
      .sell-rec-hero { word-break: break-word; overflow-wrap: anywhere; }
      .sell-rec-hero__row { display:flex; justify-content:space-between; gap:18px; align-items:flex-end; flex-wrap:wrap; }
      .sell-rec-hero__label { font-size:2rem; font-weight:800; line-height:1; }
      .sell-rec-hero__pct { font-size:1.4rem; font-weight:800; text-align:right; }
      .sell-killer-alert { background:var(--bad-bg); border:1px solid var(--bad-border); border-left:5px solid var(--bad); border-radius:var(--radius-lg); padding:14px 16px; margin-bottom:12px; box-shadow:var(--shadow-card); }
      .sell-killer-alert__title { color:var(--bad); font-weight:800; font-size:.92rem; margin-bottom:4px; }
      .sell-killer-alert__body { color:#7f1d1d; font-size:.86rem; line-height:1.45; }
      .sell-signal-card { border-radius:12px; padding:10px 11px; margin:8px 0; overflow-wrap:anywhere; word-break:normal; }
      .sell-signal-card__title { font-weight:800; font-size:.86rem; line-height:1.35; }
      .sell-signal-card__meta { color:var(--muted); font-size:.74rem; line-height:1.45; margin-top:4px; }
      .sell-signal-card__reason { color:var(--text); font-size:.76rem; line-height:1.45; margin-top:5px; }
      .sell-mode-pill { display:inline-block; border-radius:999px; padding:3px 8px; font-size:.72rem; font-weight:800; background:#eef2ff; color:#3730a3; margin-top:5px; }
      .sell-diagnostic-table [data-testid="stDataFrame"] { font-size:.82rem; }
      @media (max-width: 640px) {
        .sell-rec-hero__row { align-items:flex-start; gap:10px; }
        .sell-rec-hero__label { font-size:1.55rem; width:100%; }
        .sell-rec-hero__pct { font-size:1.14rem; text-align:left; width:100%; }
        .sell-killer-alert { padding:12px 12px; }
        .sell-signal-card { padding:9px 10px; }
      }
    </style>
    """, unsafe_allow_html=True)


def _sell_signal_reason(signal: dict, group_title: str) -> str:
    label = str((signal or {}).get("label") or "Signal").strip()
    contribution = int((signal or {}).get("contribution_percent", 0) or 0)
    if group_title == "Killer-Signale":
        return f"Dieses Killer-Signal erzwingt eine klare Schutzentscheidung: {label}."
    if group_title == "Tranche-Signale" and contribution:
        return f"Dieses Signal erhöht die Ziel-Verkaufsquote um {contribution} Prozentpunkte."
    if group_title == "Watch-Signale":
        return f"Dieses Watch-Signal erhöht die Aufmerksamkeit, ohne sofort eine neue Tranche zu erzwingen."
    return f"Aktives Signal aus dem Regelwerk: {label}."


def _sell_format_metric_value(value) -> str:
    if value is None:
        return "—"
    try:
        if pd.isna(value):
            return "—"
    except Exception:
        pass
    if isinstance(value, (float, np.floating)):
        return f"{float(value):,.4f}".replace(",", ".")
    if isinstance(value, (int, np.integer)):
        return f"{int(value)}"
    return str(value)


def _sell_decision_user_error(error: str | None, ticker: str = "") -> str:
    text = str(error or "").strip()
    ticker_text = f" für {ticker}" if ticker else ""
    if not text:
        return f"Yahoo-Daten konnten{ticker_text} nicht berechnet werden."
    lower = text.lower()
    if "ticker fehlt" in lower or "ungültig" in lower:
        return f"Ungültiger Ticker oder ungültige Eingabe{ticker_text}. Prüfe Symbol, Kaufdatum, Einstand und Stückzahl."
    if "benchmark" in lower:
        return "Benchmark-Daten fehlen. Prüfe die Yahoo-Verfügbarkeit des Benchmark-Symbols SPY und lade die Daten neu."
    if "keine kursdaten" in lower or "schlusskurse" in lower or "historie" in lower:
        return f"Für {ticker or 'diesen Ticker'} fehlt eine verwertbare Yahoo-Historie. Prüfe Symbol und Kaufdatum."
    return text


def _sell_monitor_buy_price_for_market(ticker: str, buy_price: float, currency: str, buy_date, market_currency: str) -> float:
    raw = float(buy_price or 0.0)
    src = str(currency or "EUR").upper()
    dst = str(market_currency or src).upper()
    if src == dst:
        return raw
    if src == "USD" and dst == "EUR":
        return _price_to_eur(raw, "USD", buy_date)[0]
    if src == "EUR" and dst == "USD":
        try:
            fx = yf.Ticker("EURUSD=X").history(start=pd.Timestamp(buy_date) - timedelta(days=5), end=pd.Timestamp(buy_date) + timedelta(days=3))
            if fx is not None and len(fx) > 0:
                eur_usd = float(fx["Close"].dropna().iloc[-1])
                if eur_usd > 0:
                    return raw * eur_usd
        except Exception as exc:
            logger.debug("EUR/USD conversion for sell monitor failed: %s", exc)
        rate = _usd_eur_rate()
        return raw / rate if rate else raw
    return raw


@st.cache_data(ttl=900, show_spinner=False)
def load_sell_decision_chart_frame(ticker: str, buy_date, benchmark_ticker: str = "SPY", cache_buster: int = 0) -> pd.DataFrame:
    _ = cache_buster
    norm_ticker = _normalize_single_ticker(ticker)
    if not norm_ticker:
        return pd.DataFrame()
    end = datetime.now(timezone.utc).replace(tzinfo=None)
    try:
        buy_ts = pd.Timestamp(buy_date).normalize()
    except Exception:
        buy_ts = pd.Timestamp(end - timedelta(days=420)).normalize()
    start = min(end - timedelta(days=420), buy_ts.to_pydatetime() - timedelta(days=60))
    frames = _bulk_download_ohlc((norm_ticker,), start, end)
    return _coerce_ohlc_frame(frames.get(norm_ticker))


def _render_sell_monitor_recommendation(result: dict, metrics: dict, shares: float, market_currency: str, ticker: str = "") -> None:
    raw_label = result.get("recommendation_label", "HALTEN")
    display_label = result.get("display_label") or raw_label
    pending_status = str(result.get("pending_status") or "")
    pct = int(result.get("sell_now_percent", result.get("recommendation_percent", 0)) or 0)
    killer_signals = result.get("killer_signals", []) or []

    # Farb-/Ton-Wahl: Pending-Status überschreibt den Roh-Label-Ton.
    if pending_status == "snoozed":
        tone, bg, border = "#64748b", "#f1f5f9", "#cbd5e1"
    elif pending_status == "in_bestaetigung":
        tone, bg, border = "#ca8a04", "#fefce8", "#fde047"
    elif raw_label == "HALTEN":
        tone, bg, border = "#16a34a", "#f0fdf4", "#bbf7d0"
    elif raw_label == "TEILVERKAUF":
        tone, bg, border = "#d97706", "#fffbeb", "#fde68a"
    else:
        tone, bg, border = "#dc2626", "#fef2f2", "#fecaca"

    if killer_signals:
        first_killer = killer_signals[0] if isinstance(killer_signals[0], dict) else {}
        st.markdown(
            f"""
            <div class="sell-killer-alert">
              <div class="sell-killer-alert__title">🔴 Killer-Signal aktiv</div>
              <div class="sell-killer-alert__body">{html.escape(str(first_killer.get('label') or 'Killer-Signal'))} · {pct}% jetzt verkaufen · {html.escape(str(first_killer.get('book_reference') or 'Regelwerk'))}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    pending_hint_html = ""
    if pending_status == "in_bestaetigung":
        pending_hint_html = (
            '<div style="margin-top:6px;font-size:.78rem;color:#854d0e;font-weight:600;">⏳ Signal in Bestätigung — '
            'Empfehlung muss morgen erneut auftauchen, bevor sie scharf gestellt wird.</div>'
        )
    elif pending_status == "snoozed":
        snoozed_until = str((result.get("next_recommendation_state") or {}).get("snoozed_until") or "")
        until_text = f" bis {snoozed_until}" if snoozed_until else ""
        pending_hint_html = (
            f'<div style="margin-top:6px;font-size:.78rem;color:#475569;font-weight:600;">🔕 Stumm geschaltet{until_text}. '
            'Eine stärkere Empfehlung oder ein Killer-Signal hebt die Stummschaltung sofort auf.</div>'
        )

    st.markdown(
        f"""
        <div class="summary-hero sell-rec-hero" style="background:{bg};border-color:{border};border-left:5px solid {tone};margin-bottom:14px;">
          <div class="card-label" style="color:{tone};">Verkaufs-Empfehlung</div>
          <div class="sell-rec-hero__row">
            <div class="sell-rec-hero__label" style="color:{tone};">{html.escape(display_label)}</div>
            <div class="sell-rec-hero__pct" style="color:{tone};">Konkrete Tranche: {pct}% jetzt verkaufen</div>
          </div>
          <div style="margin-top:8px;color:#334155;font-size:.9rem;line-height:1.45;">{html.escape(str(result.get('explanation_short', '')))}</div>
          {pending_hint_html}
          <div class="sell-mode-pill">{html.escape(str(result.get('sell_mode') or ''))}{(' · ' + html.escape(str(result.get('sell_style')))) if result.get('sell_style') else ''}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Snooze-Aktionen rendern, sobald eine scharfe Empfehlung vorliegt oder eine bereits aktive Snooze laufen.
    if ticker and pct > 0 and not killer_signals:
        snooze_cols = st.columns([1, 1, 2])
        if pending_status == "snoozed":
            if snooze_cols[0].button("🔔 Stummschaltung aufheben", key=f"sell_snooze_clear_{ticker}"):
                clear_recommendation_snooze(ticker)
                st.rerun()
        else:
            if snooze_cols[0].button("🔕 5 Tage stumm schalten", key=f"sell_snooze_5d_{ticker}", help="Empfehlung bis maximal zur eingestellten Tranche für 5 Handelstage stumm schalten. Eskalation und Killer-Signale überschreiben die Stummschaltung."):
                snooze_recommendation(ticker, snoozed_pct=pct, days=5)
                st.rerun()
            if snooze_cols[1].button("🔕 10 Tage stumm", key=f"sell_snooze_10d_{ticker}"):
                snooze_recommendation(ticker, snoozed_pct=pct, days=10)
                st.rerun()

    already = float(result.get("already_sold_percent", 0.0) or 0.0)
    shares_to_sell = _sell_monitor_shares_for_percent(shares, already, pct)
    remaining_shares = max(float(shares or 0.0) - shares_to_sell, 0.0)
    current_price = _safe_float(metrics.get("current_price"), np.nan)
    tranche_value_market = shares_to_sell * current_price if not np.isnan(current_price) else np.nan
    tranche_value_eur = tranche_value_market if str(market_currency).upper() == "EUR" else _usd_to_eur(tranche_value_market)
    cols = st.columns(4)
    cols[0].metric("Jetzt verkaufen", f"{shares_to_sell:,.2f} Stk.".replace(",", "."))
    cols[1].metric("Tranche EUR", _format_eur(tranche_value_eur if not np.isnan(tranche_value_eur) else 0.0))
    cols[2].metric("Verbleiben", f"{remaining_shares:,.2f} Stk.".replace(",", "."))
    cols[3].metric("Rest nach Verkauf", f"{float(result.get('remaining_after_sale_percent', 0.0) or 0.0):.0f}%")

    action_cols = st.columns(3)
    action_cols[0].metric("Stopp-Marke Restposition", _sell_monitor_fmt_money(result.get("stop_price"), market_currency))
    action_cols[1].metric("Nächste Tranche", _sell_monitor_fmt_money(result.get("next_tranche_trigger_price"), market_currency))
    action_cols[2].metric("Vollausstieg-Marke", _sell_monitor_fmt_money(result.get("full_exit_price"), market_currency))
    add_again = str(result.get("add_again_condition") or "").strip()
    if add_again and add_again != "—":
        st.caption(f"**Wieder aufstocken:** {add_again}")

def _sell_monitor_primary_signal(result: dict) -> str:
    for group in ("killer_signals", "tranche_signals", "watch_signals"):
        for signal in result.get(group, []) or []:
            label = str((signal or {}).get("label") or "").strip()
            if label:
                return label
    return str(result.get("recommendation_label") or "Manuelle Umsetzung").strip()


def _sell_monitor_shares_for_percent(shares: float, already_sold_percent: float, sell_now_percent: float) -> float:
    current_shares = max(float(shares or 0.0), 0.0)
    pct = max(float(sell_now_percent or 0.0), 0.0)
    if pct <= 0 or current_shares <= 0:
        return 0.0
    remaining_before_pct = max(100.0 - max(float(already_sold_percent or 0.0), 0.0), 1.0)
    return min(current_shares, current_shares * pct / remaining_before_pct)


def _render_sell_monitor_tranche_log(ticker: str, result: dict, metrics: dict, shares: float, market_currency: str, tranche_log: list[dict]) -> None:
    ticker = _normalize_single_ticker(ticker)
    ticker_tranches = [entry for entry in tranche_log if _normalize_single_ticker(entry.get("ticker", "")) == ticker]
    already = float(result.get("already_sold_percent", 0.0) or 0.0)
    sell_now = int(result.get("sell_now_percent", result.get("recommendation_percent", 0)) or 0)
    target_total = int(result.get("target_total_sold_percent", 0) or 0)
    remaining_after = float(result.get("remaining_after_sale_percent", 0.0) or 0.0)

    st.markdown("#### Realisierte Tranchen")
    summary_cols = st.columns(4)
    summary_cols[0].metric("Bereits verkauft", f"{already:.0f}%")
    summary_cols[1].metric("Ziel gesamt", f"{target_total}%")
    summary_cols[2].metric("Jetzt zusätzlich", f"{sell_now}%")
    summary_cols[3].metric("Rest nach Umsetzung", f"{remaining_after:.0f}%")

    if ticker_tranches:
        display_rows = []
        for entry in sorted(ticker_tranches, key=lambda row: str(row.get("date") or ""), reverse=True):
            display_rows.append({
                "Datum": entry.get("date") or "—",
                "Tranche %": _safe_float(entry.get("tranche_percent"), 0.0),
                "Kurs": _safe_float(entry.get("price"), np.nan),
                "Stück": _safe_float(entry.get("shares_sold"), np.nan),
                "Signal": entry.get("trigger_signal") or "—",
                "Notiz": entry.get("notes") or "",
            })
        st.dataframe(
            pd.DataFrame(display_rows),
            width="stretch",
            hide_index=True,
            column_config={
                "Tranche %": st.column_config.NumberColumn("Tranche %", format="%.0f%%"),
                "Kurs": st.column_config.NumberColumn(f"Kurs ({market_currency})", format="%.2f"),
                "Stück": st.column_config.NumberColumn("Stück", format="%.2f"),
            },
        )
    else:
        st.info("Für diese Position sind noch keine realisierten Tranchen protokolliert.")

    if sell_now <= 0:
        if already >= target_total and target_total > 0:
            st.caption("Die aktuell notwendige Verkaufsquote wurde bereits durch frühere Tranchen erreicht.")
        return

    current_price = _safe_float(metrics.get("current_price"), 0.0) or 0.0
    default_shares = _sell_monitor_shares_for_percent(shares, already, sell_now)
    signal_options = []
    for group in ("killer_signals", "tranche_signals", "watch_signals"):
        for signal in result.get(group, []) or []:
            label = str((signal or {}).get("label") or "").strip()
            if label and label not in signal_options:
                signal_options.append(label)
    primary_signal = _sell_monitor_primary_signal(result)
    if primary_signal and primary_signal not in signal_options:
        signal_options.insert(0, primary_signal)
    if not signal_options:
        signal_options = ["Manuelle Umsetzung"]

    with st.form(f"sell_monitor_tranche_log_{ticker}"):
        st.markdown("##### Umsetzung protokollieren")
        c1, c2, c3, c4 = st.columns(4)
        sale_date = c1.date_input("Datum", value=datetime.now(timezone.utc).date(), key=f"tranche_date_{ticker}")
        tranche_percent = c2.number_input("Verkaufte Tranche %", min_value=0.0, max_value=100.0, value=float(sell_now), step=1.0, key=f"tranche_pct_{ticker}")
        sale_price = c3.number_input("Kurs", min_value=0.0, value=float(current_price), step=0.01, key=f"tranche_price_{ticker}")
        shares_sold = c4.number_input("Stückzahl", min_value=0.0, value=float(default_shares), step=1.0, key=f"tranche_shares_{ticker}")
        trigger_signal = st.selectbox("Auslösendes Signal", signal_options, index=0, key=f"tranche_signal_{ticker}")
        notes = st.text_input("Notiz optional", value="", key=f"tranche_note_{ticker}")
        submitted = st.form_submit_button("Tranche als verkauft protokollieren", type="primary", use_container_width=True)
        if submitted:
            if tranche_percent <= 0:
                st.warning("Bitte eine verkaufte Tranche größer 0% erfassen.")
            else:
                append_tranche_log({
                    "date": str(sale_date),
                    "ticker": ticker,
                    "tranche_percent": float(tranche_percent),
                    "price": float(sale_price) if sale_price > 0 else None,
                    "shares_sold": float(shares_sold) if shares_sold > 0 else None,
                    "trigger_signal": trigger_signal,
                    "notes": notes,
                })
                st.success("Tranche wurde im Verkaufs-Log gespeichert. Die Portfolio-Position wurde nicht automatisch reduziert.")
                st.rerun()


def _render_sell_monitor_signals(result: dict) -> None:
    def render_list(title: str, signals: list[dict], color: str, bg: str):
        st.markdown(f'<div class="info-card sell-signal-group" style="border-color:{color}33;"><div class="card-label" style="color:{color};">{html.escape(title)}</div>', unsafe_allow_html=True)
        if not signals:
            st.markdown('<div class="mini-help">Keine aktiven Signale</div>', unsafe_allow_html=True)
        for signal in signals:
            contrib = int(signal.get("contribution_percent", 0) or 0)
            contrib_text = f" · Tranche-Beitrag {contrib}%" if contrib else ""
            book_ref = str(signal.get("book_reference") or "Regelwerk").strip()
            reason = _sell_signal_reason(signal, title)
            signal_date = str(signal.get("signal_date") or "Aktueller Stand")
            event_note = str(signal.get("event_note") or "")
            sell_mode = str(signal.get("sell_mode") or "")
            sell_style = str(signal.get("sell_style") or "")
            mode_line = " · ".join(part for part in [sell_mode, sell_style] if part)
            strategy_key = str(signal.get("strategy_key") or "").strip()
            if strategy_key.startswith("lm_"):
                badge_label = "LM-nativ"
                badge_color = "#0f766e"
                badge_bg = "#ccfbf1"
            elif strategy_key:
                badge_label = f"Hub · {strategy_key}"
                badge_color = "#3730a3"
                badge_bg = "#e0e7ff"
            else:
                badge_label = ""
                badge_color = ""
                badge_bg = ""
            badge_html = f'<div class="sell-strategy-badge" style="display:inline-block;border-radius:999px;padding:3px 8px;font-size:.7rem;font-weight:800;background:{badge_bg};color:{badge_color};margin-top:5px;margin-right:6px;">{html.escape(badge_label)}</div>' if badge_label else ''
            st.markdown(
                f"""
                <div class="sell-signal-card" style="background:{bg};">
                  <div class="sell-signal-card__title" style="color:{color};">{html.escape(str(signal.get('label', 'Signal')))}{html.escape(contrib_text)}</div>
                  <div class="sell-signal-card__meta">Erster Auslöser: {html.escape(signal_date)} · Kapitelverweis: {html.escape(book_ref)}</div>
                  <div class="sell-signal-card__reason">Begründung: {html.escape(reason)}</div>
                  {f'<div class="sell-signal-card__reason">Datenpunkt: {html.escape(event_note)}</div>' if event_note else ''}
                  {badge_html}
                  {f'<div class="sell-mode-pill">Einordnung: {html.escape(mode_line)}</div>' if mode_line else ''}
                </div>
                """,
                unsafe_allow_html=True,
            )
        st.markdown('</div>', unsafe_allow_html=True)

    c1, c2 = st.columns(2)
    with c1:
        render_list("Killer-Signale", result.get("killer_signals", []), "#dc2626", "#fef2f2")
    with c2:
        render_list("Tranche-Signale (Strategien)", result.get("tranche_signals", []), "#d97706", "#fffbeb")
    c3, c4 = st.columns(2)
    with c3:
        render_list("Warnsignale (Hub)", result.get("warning_signals", []), "#9333ea", "#faf5ff")
    with c4:
        render_list("Watch-Signale", result.get("watch_signals", []), "#2563eb", "#eff6ff")

def _render_sell_monitor_diagnostics(ticker: str, metrics_payload: dict, manual_data: dict, tranche_log: list[dict], chart_df: pd.DataFrame, market_currency: str) -> None:
    with st.expander("🔎 Erweiterte Diagnose", expanded=False):
        metrics = metrics_payload.get("metrics", {}) if isinstance(metrics_payload, dict) else {}
        if metrics:
            rows = [{"Kennzahl": str(key).replace("_", " "), "Wert": _sell_format_metric_value(value)} for key, value in metrics.items()]
            st.markdown('<div class="sell-diagnostic-table">', unsafe_allow_html=True)
            st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)
            st.markdown('</div>', unsafe_allow_html=True)
        if chart_df is None or chart_df.empty:
            st.info("Für den Diagnose-Chart fehlen Kursdaten. Prüfe Ticker, Kaufdatum und Yahoo-Verfügbarkeit.")
            return
        df = chart_df.copy().tail(220)
        close = pd.to_numeric(df["Close"], errors="coerce")
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df.index, y=close, mode="lines", name="Kurs"))
        for window, name, color, kind in [(21, "21-EMA", "#2563eb", "ema"), (50, "50-MA", "#d97706", "sma"), (200, "200-MA", "#64748b", "sma")]:
            ma = close.ewm(span=window, adjust=False, min_periods=window).mean() if kind == "ema" else close.rolling(window, min_periods=window).mean()
            if ma.notna().any():
                fig.add_trace(go.Scatter(x=df.index, y=ma, mode="lines", name=f"{name} (gleitend)", line=dict(width=1.4, color=color)))
        for key, name, color in [("pivot", "Pivot", "#7c3aed"), ("low_day_1", "Tag-1-Tief", "#dc2626")]:
            val = _safe_float(manual_data.get(key), np.nan)
            if not np.isnan(val):
                fig.add_hline(y=val, line_dash="dot", line_color=color, annotation_text=name)
        ticker_tranches = [t for t in tranche_log if str(t.get("ticker", "")).upper() == str(ticker).upper()]
        for idx, entry in enumerate(ticker_tranches[:10]):
            try:
                fig.add_trace(go.Scatter(
                    x=[pd.Timestamp(entry.get("date"))], y=[float(entry.get("price"))], mode="markers",
                    name=f"Tranche {entry.get('tranche_percent', '')}%", marker=dict(size=9, symbol="triangle-down", color="#dc2626")
                ))
            except Exception:
                continue
        apply_consistent_layout(fig, height=320, top_margin=20)
        fig.update_layout(yaxis_title=market_currency, xaxis_title="", legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5))
        st.plotly_chart(fig, width="stretch", key=f"sell_monitor_diag_{ticker}")


def _render_sell_decision_live_monitor() -> None:
    _init_workspace_state()
    inject_workspace_css()
    positions = [p for p in st.session_state.get("positions", []) if _normalize_single_ticker((p or {}).get("ticker", ""))]
    if not positions:
        st.info("Noch keine offenen Portfolio-Positionen vorhanden. Lege zuerst im Depot oder im Nach-Kauf-Check eine Position an.")
        return

    ticker_options = list(dict.fromkeys(_normalize_single_ticker(p.get("ticker", "")) for p in positions if _normalize_single_ticker(p.get("ticker", ""))))
    head_cols = st.columns([3, 1])
    with head_cols[0]:
        selected_ticker = st.selectbox("Offene Position", ticker_options, key="sell_live_ticker", label_visibility="collapsed")
    with head_cols[1]:
        if st.button("🔄 Yahoo neu laden", key="sell_live_reload", width="stretch"):
            st.session_state["sell_live_cache_buster"] = st.session_state.get("sell_live_cache_buster", 0) + 1
            st.rerun()

    position = next((p for p in positions if _normalize_single_ticker(p.get("ticker", "")) == selected_ticker), {})
    shares = _safe_float(position.get("shares"), 0.0)
    buy_date_raw = position.get("buy_date") or datetime.now(timezone.utc).date()
    try:
        buy_date = pd.Timestamp(buy_date_raw).date()
    except Exception:
        st.error("Die gespeicherte Position hat kein gültiges Kaufdatum.")
        return
    pivot_tag_raw = position.get("pivot_tag")
    pivot_tag = None
    if pivot_tag_raw:
        try:
            pivot_tag = pd.Timestamp(pivot_tag_raw).date()
        except Exception:
            pivot_tag = None
    stored_currency = str(position.get("currency", "EUR") or "EUR").upper()
    stored_buy_price = _safe_float(position.get("buy_price"), np.nan)
    if np.isnan(stored_buy_price) or stored_buy_price <= 0:
        st.error("Die gespeicherte Position hat keinen gültigen Einstandspreis.")
        return

    if not _is_valid_ticker(selected_ticker):
        st.error("Der ausgewählte Ticker ist ungültig. Prüfe das Symbol in der offenen Portfolio-Position.")
        return

    with st.spinner(f"Lade Yahoo-Daten für den Live-Monitor von {selected_ticker} …"):
        try:
            symbol_metrics = _portfolio_symbol_metrics(selected_ticker) or {}
            market_currency = str(symbol_metrics.get("currency", stored_currency) or stored_currency).upper()
            metric_buy_price = _sell_monitor_buy_price_for_market(selected_ticker, stored_buy_price, stored_currency, buy_date, market_currency)
            metrics_payload = load_sell_decision_metrics(
                selected_ticker, buy_date, metric_buy_price, shares, benchmark_ticker="SPY", currency=market_currency,
                cache_buster=st.session_state.get("sell_live_cache_buster", 0),
                pivot_date=pivot_tag,
            )
            chart_df = load_sell_decision_chart_frame(selected_ticker, buy_date, cache_buster=st.session_state.get("sell_live_cache_buster", 0))
        except Exception as exc:
            logger.debug("Sell Live-Monitor Yahoo load failed for %s: %s", selected_ticker, exc)
            st.error("Yahoo-Daten konnten nicht geladen werden. Prüfe Ticker, Kaufdatum und Internetverbindung.")
            return
    if not metrics_payload.get("ok"):
        st.error(_sell_decision_user_error(metrics_payload.get("error"), selected_ticker))
        return

    manual_data = get_position_manual_sell_data(selected_ticker)
    defaults = metrics_payload.get("manual_defaults", {}) or {}
    metrics = metrics_payload.get("metrics", {}) or {}
    tranche_log = load_tranche_log()

    # Compact stammdaten line
    pivot_caption = pd.Timestamp(pivot_tag).strftime("%d.%m.%Y") if pivot_tag else "Kauftag (Fallback)"
    st.caption(
        f"**{selected_ticker}** · {shares:,.2f} Stk.".replace(",", ".")
        + f" · gekauft {pd.Timestamp(buy_date).strftime('%d.%m.%Y')} zu {_sell_monitor_fmt_money(stored_buy_price, stored_currency)}"
        + f" · Pivot-Tag: {pivot_caption}"
        + f" · Markt {market_currency}"
    )

    # Auto-only checkboxes — derive once for use in the form and read-only display.
    auto_strength, auto_warning, auto_reasons = _sell_monitor_auto_checkbox_data(metrics_payload)
    auto_strength = dict(auto_strength)
    auto_warning = dict(auto_warning)
    auto_reasons = dict(auto_reasons)
    industry_status_current = manual_data.get("industry_group_status", "Neutral")
    if industry_status_current not in SELL_DECISION_INDUSTRY_GROUP_STATUSES:
        industry_status_current = "Neutral"
    if industry_status_current == "Stark":
        auto_strength["strong_industry_group"] = True
        auto_reasons["strong_industry_group"] = "Industriegruppe im Formular als stark eingestuft"
    if industry_status_current == "Schwach":
        auto_warning["weak_industry_group"] = True
        auto_reasons["weak_industry_group"] = "Industriegruppe im Formular als schwach eingestuft"

    # Rules engine uses only auto-derived checkbox state; manual overrides are no longer accepted.
    manual_for_rules = dict(manual_data)
    manual_for_rules["strength_checkboxes"] = {k: bool(v) for k, v in auto_strength.items()}
    manual_for_rules["warning_checkboxes"] = {k: bool(v) for k, v in auto_warning.items()}
    # Clear deprecated manual price fields so manual_defaults from metrics drive the rules.
    for key in ("pivot", "low_day_1", "low_day_0"):
        manual_for_rules[key] = None

    # Strategy setup must be rendered before evaluate so live widget changes flow into the rules.
    active_setup = _render_sell_monitor_setup_panel(selected_ticker, manual_data)
    metrics_payload["lm_setup"] = active_setup

    prior_recommendation_state = get_recommendation_state(selected_ticker)
    result = evaluate_sell_decision(metrics_payload, manual_for_rules, tranche_log, recommendation_state=prior_recommendation_state)
    next_recommendation_state = result.get("next_recommendation_state") or {}
    if next_recommendation_state and next_recommendation_state != prior_recommendation_state:
        save_recommendation_state(selected_ticker, next_recommendation_state)

    # A. Empfehlung (Hero)
    _render_sell_monitor_recommendation(result, metrics, shares, market_currency, selected_ticker)

    # B. Live-Kennzahlen
    st.markdown("#### Live-Kennzahlen")
    current = _safe_float(metrics.get("current_price"), np.nan)
    rs_status = "—"
    rs_line = _safe_float(metrics.get("rs_line"), np.nan)
    rs21 = _safe_float(metrics.get("rs_ma21"), np.nan)
    rs50 = _safe_float(metrics.get("rs_ma50"), np.nan)
    if not np.isnan(rs_line) and not np.isnan(rs21) and not np.isnan(rs50):
        # Klar disjunkte Klassifikation: STARK = über beiden MAs, SCHWACH = unter beiden,
        # sonst NEUTRAL. Die Original-Bedingung („rs_line > rs21 > rs50") schloss
        # Konstellationen wie rs_line>rs21 aber rs21<rs50 fälschlich aus „Stark" aus.
        if rs_line > rs21 and rs_line > rs50:
            rs_status = "Stark"
        elif rs_line < rs21 and rs_line < rs50:
            rs_status = "Schwach"
        else:
            rs_status = "Neutral"
    kpis = [
        {"title": "P&L", "value": _fmt_pct(metrics.get("pnl_pct")), "detail": "seit Einstieg"},
        {"title": "Drawdown", "value": _fmt_pct(metrics.get("drawdown_from_high_since_buy_pct")), "detail": "vom Hoch nach Kauf"},
        {"title": "Distanz 21-EMA", "value": _fmt_pct(_sell_monitor_distance(current, metrics.get("ema21"))), "detail": _sell_monitor_fmt_money(metrics.get("ema21"), market_currency)},
        {"title": "Distanz 50-MA", "value": _fmt_pct(_sell_monitor_distance(current, metrics.get("sma50"))), "detail": _sell_monitor_fmt_money(metrics.get("sma50"), market_currency)},
    ]
    _render_change_cards(kpis)
    st.markdown(f'<div class="info-card"><div class="card-label">RS-Linien-Status</div><div style="font-size:1.1rem;font-weight:800;">{html.escape(rs_status)}</div><div class="mini-help">RS {rs_line:.4f} · 21-MA {_fmt_num(rs21)} · 50-MA {_fmt_num(rs50)}</div></div>' if not np.isnan(rs_line) else '<div class="info-card"><div class="card-label">RS-Linien-Status</div>—</div>', unsafe_allow_html=True)

    # C. Aktive Signale
    st.markdown("#### Aktive Signale")
    _render_sell_monitor_signals(result)

    # D. Automatisch erkannte Muster (read-only, nur aktive)
    active_strength = [(k, label) for (k, label) in SELL_MONITOR_STRENGTH_SIGNALS if bool(auto_strength.get(k, False))]
    active_warning = [(k, label) for (k, label) in SELL_MONITOR_WARNING_SIGNALS if bool(auto_warning.get(k, False))]
    if active_strength or active_warning:
        st.markdown("#### Automatisch erkannte Muster")
        cols = st.columns(2)
        with cols[0]:
            st.markdown('<div class="card-label" style="color:#16a34a;">Stärke-Signale</div>', unsafe_allow_html=True)
            if not active_strength:
                st.markdown('<div class="mini-help">Keine aktiven Stärke-Signale.</div>', unsafe_allow_html=True)
            for key, label in active_strength:
                reason = str(auto_reasons.get(key, "")).strip()
                reason_html = f'<div class="mini-help">{html.escape(reason)}</div>' if reason else ""
                st.markdown(
                    f'<div class="info-card" style="border-color:#bbf7d0;background:#f0fdf4;margin:6px 0;padding:8px 10px;">'
                    f'<div style="font-weight:700;color:#16a34a;">✓ {html.escape(label)}</div>{reason_html}</div>',
                    unsafe_allow_html=True,
                )
        with cols[1]:
            st.markdown('<div class="card-label" style="color:#dc2626;">Warnzeichen</div>', unsafe_allow_html=True)
            if not active_warning:
                st.markdown('<div class="mini-help">Keine aktiven Warnsignale.</div>', unsafe_allow_html=True)
            for key, label in active_warning:
                reason = str(auto_reasons.get(key, "")).strip()
                reason_html = f'<div class="mini-help">{html.escape(reason)}</div>' if reason else ""
                st.markdown(
                    f'<div class="info-card" style="border-color:#fecaca;background:#fef2f2;margin:6px 0;padding:8px 10px;">'
                    f'<div style="font-weight:700;color:#dc2626;">⚠ {html.escape(label)}</div>{reason_html}</div>',
                    unsafe_allow_html=True,
                )

    # E. Tranche-Log
    with st.expander("📒 Tranche-Log", expanded=bool([t for t in tranche_log if _normalize_single_ticker(t.get("ticker", "")) == selected_ticker])):
        _render_sell_monitor_tranche_log(selected_ticker, result, metrics, shares, market_currency, tranche_log)

    # F. Marktumfeld & Selbsteinschätzung (manuell)
    with st.expander("🧭 Marktumfeld & Selbsteinschätzung", expanded=False):
        st.caption("Felder, die nicht automatisch ableitbar sind und die Empfehlung beeinflussen.")
        with st.form(f"sell_monitor_manual_{selected_ticker}"):
            m1, m2, m3 = st.columns([1.1, 1.1, 1.4])
            market_environment = m1.selectbox(
                "Marktumfeld",
                ["Bullisch", "Unsicher", "Bärisch"],
                index=["Bullisch", "Unsicher", "Bärisch"].index(manual_data.get("market_environment", "Unsicher") if manual_data.get("market_environment", "Unsicher") in ["Bullisch", "Unsicher", "Bärisch"] else "Unsicher"),
                key=f"sell_env_{selected_ticker}",
            )
            industry_status = m2.selectbox(
                "Industriegruppe",
                ["Stark", "Neutral", "Schwach"],
                index=["Stark", "Neutral", "Schwach"].index(industry_status_current),
                key=f"sell_ind_{selected_ticker}",
            )
            personality_changed = m3.checkbox(
                "Persönlichkeits-Check: Aktie hat ihre Persönlichkeit geändert",
                value=bool(manual_data.get("personality_changed", False)),
                key=f"sell_personality_{selected_ticker}",
            )
            saved = st.form_submit_button("💾 Selbsteinschätzung speichern", type="primary", use_container_width=True)
            if saved:
                # Bestehende Daten (insb. sell_setup-Parameter aus dem LM-Hub)
                # erhalten — sonst überschreibt save_position_manual_sell_data
                # den kompletten Eintrag und löscht die zuvor gespeicherte
                # Strategie-Konfiguration.
                existing_manual = get_position_manual_sell_data(selected_ticker) or {}
                merged = dict(existing_manual)
                merged.update({
                    "ticker": selected_ticker,
                    "market_environment": market_environment,
                    "industry_group_status": industry_status,
                    "personality_changed": personality_changed,
                })
                save_position_manual_sell_data(selected_ticker, merged)
                st.success("Selbsteinschätzung gespeichert.")
                st.rerun()

    # G. Diagnose & Chart
    _render_sell_monitor_diagnostics(selected_ticker, metrics_payload, manual_for_rules, tranche_log, chart_df, market_currency)


def _sell_ranking_status_badge(status: str) -> str:
    return {"Halten": "🟢 Halten", "Beobachten": "🟡 Beobachten", "Verkaufen": "🔴 Verkaufen"}.get(str(status), str(status or "—"))


def _evaluate_sell_ranking_position(position: dict, name_map: dict, manual_map: dict, tranche_log: list[dict], cache_buster: int) -> dict:
    ticker = _normalize_single_ticker((position or {}).get("ticker", ""))
    base = {
        "Ticker": ticker,
        "Firma": name_map.get(ticker, ticker),
        "P&L %": np.nan,
        "Health-Score": np.nan,
        "Empfohlene Tranche %": 0,
        "Status": "Fehler",
        "Drawdown vom Peak %": np.nan,
        "21-EMA": "—",
        "50-MA": "—",
        "RS-Trend": "—",
        "Distribution-Tage": np.nan,
        "Haltedauer Tage": np.nan,
        "Fehler": "",
        "_sort_tranche": 0,
        "_sort_health": -1,
    }
    payload = _evaluate_position_engine(position, manual_map, tranche_log, cache_buster)
    if not payload.get("ok"):
        base["Fehler"] = payload.get("error") or "Auswertung fehlgeschlagen."
        return base
    metrics = payload.get("metrics") or {}
    health = payload.get("health") or {}
    result = payload.get("result") or {}
    current = _safe_float(metrics.get("current_price"), np.nan)
    ema21 = _safe_float(metrics.get("ema21"), np.nan)
    sma50 = _safe_float(metrics.get("sma50"), np.nan)
    try:
        buy_date = pd.Timestamp((position or {}).get("buy_date")).date()
        held_days = max((datetime.now(timezone.utc).date() - buy_date).days, 0)
    except Exception:
        held_days = 0
    tranche = int(result.get("sell_now_percent", result.get("recommendation_percent", 0)) or 0)
    base.update({
        "P&L %": _safe_float(metrics.get("pnl_pct"), np.nan),
        "Health-Score": _safe_float(health.get("health_score"), np.nan),
        "Empfohlene Tranche %": tranche,
        "Status": _sell_ranking_status_badge(health.get("status")),
        "Drawdown vom Peak %": _safe_float(metrics.get("drawdown_from_high_since_buy_pct"), np.nan),
        "21-EMA": "darüber" if not np.isnan(current) and not np.isnan(ema21) and current >= ema21 else "darunter" if not np.isnan(current) and not np.isnan(ema21) else "—",
        "50-MA": "darüber" if not np.isnan(current) and not np.isnan(sma50) and current >= sma50 else "darunter" if not np.isnan(current) and not np.isnan(sma50) else "—",
        "RS-Trend": health.get("rs_trend", "seitwärts"),
        "Distribution-Tage": int(_safe_float(metrics.get("distribution_days_25"), 0) or 0),
        "Haltedauer Tage": held_days,
        "_sort_tranche": tranche,
        "_sort_health": _safe_float(health.get("health_score"), -1),
    })
    return base


def _sell_ranking_action_text(rows: list[dict]) -> str:
    valid = [r for r in rows if not r.get("Fehler")]
    if not valid:
        return "Für die aktuellen Positionen konnten noch keine belastbaren Ranking-Daten berechnet werden."
    sell_candidates = [r for r in valid if float(r.get("Empfohlene Tranche %") or 0) >= 50 or "Verkaufen" in str(r.get("Status", ""))]
    healthy = [r for r in valid if "Halten" in str(r.get("Status", ""))]
    watch = [r for r in valid if "Beobachten" in str(r.get("Status", ""))]
    if sell_candidates and healthy:
        return "Kapital aus den schwächsten Titeln freisetzen und in führende Positionen nur bei sauberen Setups umschichten."
    if sell_candidates and not healthy:
        return "Es gibt Verkaufs-Kandidaten, aber keine gesunden Halter. Cash ist auch eine Position — erst Stabilität abwarten."
    if len(healthy) == len(valid):
        return "Das Portfolio wirkt in Ordnung. Die Score-Reihenfolge zeigt eine mögliche Aufstockungsreihenfolge bei sauberen Setups."
    if len(watch) >= max(1, len(valid) / 2):
        return "Überwiegend Beobachten: Stopps enger ziehen und vorerst nicht aufstocken."
    return "Portfolio gemischt: zuerst die niedrigsten Health-Scores prüfen und Positionsgrößen defensiv halten."


def _render_sell_decision_portfolio_ranking() -> None:
    _init_workspace_state()
    positions = [p for p in st.session_state.get("positions", []) if _normalize_single_ticker((p or {}).get("ticker", ""))]
    st.markdown("#### 🏁 Portfolio-Ranking")
    st.caption("Bewertet alle offenen Positionen mit derselben Kennzahlen- und Regel-Engine wie der Live-Monitor.")
    if not positions:
        st.info("Noch keine offenen Portfolio-Positionen vorhanden. Lege zuerst im Depot oder im Nach-Kauf-Check eine Position an.")
        return

    col_btn, col_meta = st.columns([1, 2])
    with col_btn:
        run_clicked = st.button("Alle Positionen auswerten", key="sell_rank_run", type="primary", use_container_width=True)
    with col_meta:
        st.caption(f"{len(positions)} offene Position(en) · gespeicherte Live-Monitor-Daten werden berücksichtigt")
    if run_clicked:
        st.session_state["sell_rank_cache_buster"] = st.session_state.get("sell_rank_cache_buster", 0) + 1
        tickers = tuple(dict.fromkeys(_normalize_single_ticker(p.get("ticker", "")) for p in positions if _normalize_single_ticker(p.get("ticker", ""))))
        name_map = _ticker_display_names(tickers)
        state = load_sell_decision_state()
        manual_map = state.get("positions_manual", {}) if isinstance(state, dict) else {}
        tranche_log = load_tranche_log()
        rows = []
        progress = st.progress(0, text="Starte Auswertung …")
        for idx, position in enumerate(positions, start=1):
            ticker = _normalize_single_ticker(position.get("ticker", ""))
            progress.progress((idx - 1) / max(len(positions), 1), text=f"Bewerte {ticker or idx} …")
            rows.append(_evaluate_sell_ranking_position(position, name_map, manual_map, tranche_log, st.session_state.get("sell_rank_cache_buster", 0)))
        progress.progress(1.0, text="Auswertung abgeschlossen")
        st.session_state["sell_rank_rows"] = rows
        st.session_state["sell_rank_at"] = datetime.now(timezone.utc).strftime("%d.%m.%Y · %H:%M UTC")

    rows = st.session_state.get("sell_rank_rows", [])
    if not rows:
        st.info("Klicke auf „Alle Positionen auswerten“, um das Ranking zu berechnen.")
        return

    df = pd.DataFrame(rows).sort_values(["_sort_tranche", "_sort_health"], ascending=[False, True]).drop(columns=["_sort_tranche", "_sort_health"], errors="ignore")
    display_cols = ["Ticker", "Firma", "P&L %", "Health-Score", "Empfohlene Tranche %", "Status", "Drawdown vom Peak %", "21-EMA", "50-MA", "RS-Trend", "Distribution-Tage", "Haltedauer Tage"]
    if _is_mobile_client():
        display_cols = ["Ticker", "P&L %", "Health-Score", "Empfohlene Tranche %", "Status", "21-EMA", "50-MA"]
    st.dataframe(
        df[display_cols],
        width="stretch",
        hide_index=True,
        column_config={
            "Ticker": st.column_config.TextColumn("Ticker", help="Firmenname steht in der Spalte Firma, sofern zuverlässig verfügbar."),
            "Firma": st.column_config.TextColumn("Firma", width="medium"),
            "P&L %": st.column_config.NumberColumn("P&L %", format="%+.2f%%"),
            "Health-Score": st.column_config.ProgressColumn("Health-Score", format="%.0f", min_value=0, max_value=100),
            "Empfohlene Tranche %": st.column_config.NumberColumn("Empf. Tranche %", format="%d%%"),
            "Drawdown vom Peak %": st.column_config.NumberColumn("Drawdown Peak %", format="%+.2f%%"),
            "Distribution-Tage": st.column_config.NumberColumn("Dist.-Tage", format="%d"),
            "Haltedauer Tage": st.column_config.NumberColumn("Haltedauer", format="%d"),
        },
    )
    if st.session_state.get("sell_rank_at"):
        st.caption(f"Letzte Auswertung: {st.session_state['sell_rank_at']}")
    st.markdown(f'<div class="info-card"><div class="card-label">Aktionsempfehlung</div>{html.escape(_sell_ranking_action_text(rows))}</div>', unsafe_allow_html=True)
    errors = [r for r in rows if r.get("Fehler")]
    if errors:
        with st.expander(f"⚠️ Fehlermeldungen ({len(errors)})", expanded=False):
            err_df = pd.DataFrame([{"Ticker": r.get("Ticker"), "Fehler": r.get("Fehler")} for r in errors])
            st.dataframe(err_df, width="stretch", hide_index=True)


POST_MORTEM_CHECKBOXES = [
    ("entry_more_than_5_above_pivot", "Einstieg lag mehr als 5% über dem Pivot"),
    ("stop_hit_executed", "Stopp wurde erreicht und sauber ausgeführt"),
    ("stop_moved_down", "Stopp wurde während des Trades nach unten verschoben"),
    ("breakeven_after_20", "Stopp wurde nach 20% Gewinn auf Break-even nachgezogen"),
    ("partial_profit_20_25", "Teilgewinn bei 20–25% wurde mitgenommen"),
    ("emotional_sell", "Verkauf war emotional getrieben"),
    ("hope_hold", "Aktie wurde unter Hoffnung auf Erholung gehalten"),
    ("rule_based_sell", "Verkauf erfolgte auf Basis einer klaren Regel aus dem Regelwerk"),
    ("ignored_market_warnings", "Gesamtmarkt hatte Warnsignale, die ignoriert wurden"),
]


@st.cache_data(ttl=900, show_spinner=False)
def load_post_mortem_trade_metrics(ticker: str, buy_date, sell_date, buy_price: float, sell_price: float, cache_buster: int = 0) -> dict:
    _ = cache_buster
    ticker = _normalize_single_ticker(ticker)
    if not ticker:
        return {"ok": False, "error": "Ticker fehlt."}
    try:
        buy_ts = pd.Timestamp(buy_date).normalize()
        sell_ts = pd.Timestamp(sell_date).normalize()
    except Exception:
        return {"ok": False, "error": "Kauf- oder Verkaufsdatum ist ungültig."}
    if sell_ts < buy_ts:
        return {"ok": False, "error": "Verkaufsdatum liegt vor Kaufdatum."}
    entry = _safe_float(buy_price, np.nan)
    exit_ = _safe_float(sell_price, np.nan)
    if np.isnan(entry) or entry <= 0 or np.isnan(exit_) or exit_ <= 0:
        return {"ok": False, "error": "Kauf- oder Verkaufspreis ist ungültig."}
    frames = _bulk_download_ohlc((ticker,), buy_ts - timedelta(days=10), sell_ts + timedelta(days=3))
    df = _coerce_ohlc_frame(frames.get(ticker))
    realized = (exit_ / entry - 1) * 100
    holding_days = max((sell_ts.date() - buy_ts.date()).days, 0)
    if df.empty:
        return {
            "ok": False, "error": "Yahoo-Daten für Haltedauer fehlen; Basiswerte aus Eingaben berechnet.",
            "realized_pnl_percent": realized, "max_gain_percent": None, "max_drawdown_percent": None,
            "holding_days": holding_days, "high_price": None, "low_price": None, "drawdown_from_peak_percent": None,
        }
    held = df[(df.index >= buy_ts) & (df.index <= sell_ts)].copy()
    if held.empty:
        return {"ok": False, "error": "Keine Yahoo-Kurse im Kauf-/Verkaufsfenster gefunden.", "realized_pnl_percent": realized, "holding_days": holding_days}
    high_price = _safe_float(held["High"].max(), np.nan)
    low_price = _safe_float(held["Low"].min(), np.nan)
    max_gain = (high_price / entry - 1) * 100 if not np.isnan(high_price) else None
    drawdown_from_peak = (exit_ / high_price - 1) * 100 if not np.isnan(high_price) and high_price > 0 else None
    closes = pd.to_numeric(held["Close"], errors="coerce").dropna()
    max_dd = None
    if not closes.empty:
        running_peak = closes.cummax()
        dd = (closes / running_peak - 1) * 100
        max_dd = abs(float(dd.min())) if len(dd) else None
    return {
        "ok": True, "error": "", "realized_pnl_percent": realized,
        "max_gain_percent": max_gain, "max_drawdown_percent": max_dd,
        "holding_days": holding_days, "high_price": None if np.isnan(high_price) else high_price,
        "low_price": None if np.isnan(low_price) else low_price,
        "drawdown_from_peak_percent": drawdown_from_peak,
    }


def _analyze_post_mortem(trade: dict, checkboxes: dict, metrics: dict) -> dict:
    pnl = _safe_float(metrics.get("realized_pnl_percent"), _safe_float(trade.get("realized_pnl_percent"), 0.0)) or 0.0
    max_gain = _safe_float(metrics.get("max_gain_percent"), 0.0) or 0.0
    max_dd = _safe_float(metrics.get("max_drawdown_percent"), 0.0) or 0.0
    peak_giveback = abs(_safe_float(metrics.get("drawdown_from_peak_percent"), 0.0) or 0.0)
    buy_price = _safe_float(trade.get("buy_price"), np.nan)
    pivot = _safe_float(trade.get("pivot"), np.nan)
    good: list[str] = []
    errors: list[str] = []
    lessons: list[str] = []

    has_pivot = not np.isnan(buy_price) and not np.isnan(pivot) and pivot > 0
    extended_buy = (has_pivot and buy_price > pivot * 1.05) or _safe_bool(checkboxes.get("entry_more_than_5_above_pivot"))
    if pnl > -7:
        good.append("Verlust unter 7% gehalten")
    else:
        errors.append("7%-Grenze überschritten")
        lessons.append("Verlustbegrenzung erzwingen, Kap. 6.1")
    if 20 <= pnl <= 25:
        good.append("Gewinn in der Zielzone 20–25% realisiert")
    if pnl > 0 and peak_giveback < 5:
        good.append("Nahe am Hoch verkauft, weniger als 5% Drawdown vom Peak")
    if extended_buy:
        errors.append("Extended Buy")
        lessons.append("Extended Buy: künftig maximal 5% über Pivot kaufen, Kap. 4.2")
    elif has_pivot:
        good.append("Einstieg sauber am Pivot, weniger als 5% drüber")
    if _safe_bool(checkboxes.get("stop_hit_executed")):
        good.append("Stopp diszipliniert ausgeführt")
    if _safe_bool(checkboxes.get("breakeven_after_20")):
        good.append("Break-even-Stopp gesetzt")
    if _safe_bool(checkboxes.get("partial_profit_20_25")):
        good.append("Teilgewinn mitgenommen")
    if _safe_bool(checkboxes.get("rule_based_sell")):
        good.append("Regelbasierter Verkauf")

    if peak_giveback > 15:
        errors.append("Gewinne zurückgegeben, mehr als 15% vom Peak")
        lessons.append("Gewinne zurückgegeben: Stopp- und Teilverkaufsregeln beachten, Kap. 6.2 / 7.1")
    if _safe_bool(checkboxes.get("stop_moved_down")):
        errors.append("Stopp nach unten verschoben")
        lessons.append("Stopp nach unten verschoben: Regelbruch klar markieren, Kap. 7.1")
    if max_gain >= 20 and not _safe_bool(checkboxes.get("breakeven_after_20")):
        errors.append("Trotz Höchstgewinn kein Break-even-Stopp gesetzt")
        lessons.append("Nach 20% Gewinn Restposition mindestens auf Break-even absichern, Kap. 6.2 / 7.1")
    if _safe_bool(checkboxes.get("emotional_sell")):
        errors.append("Emotionaler Verkauf")
        lessons.append("Emotionaler Verkauf: Verkauf nur nach Regelwerk, Kap. 1.3 / 8.2")
    if _safe_bool(checkboxes.get("hope_hold")):
        errors.append("Hoffnungs-Halten")
        lessons.append("Hoffnungs-Halten: Hoffnung durch definierte Exit-Regeln ersetzen, Kap. 5.3 / 6.1")
    if _safe_bool(checkboxes.get("ignored_market_warnings")):
        errors.append("Marktwarnsignale ignoriert")
        lessons.append("Marktwarnsignale ignoriert: Gesamtmarktprüfung verpflichtend machen")
    if pnl <= -15:
        lessons.append("Größerer Verlust: Verlust-Mathematik beachten — je tiefer der Verlust, desto größer der notwendige Aufholgewinn, Kap. 7.1")

    lessons = list(dict.fromkeys(lessons))
    if pnl > 0 and not errors:
        verdict_class = "Disziplinierter, regelgetreuer Gewinntrade"
        verdict_tone = "good"
    elif pnl > 0:
        verdict_class = "Gewinn mit Regelbrüchen"
        verdict_tone = "warn"
        lessons.insert(0, "Nicht reproduzierbar: Gewinn trotz Fehlern nicht als sauberes Muster werten.")
    elif not errors:
        verdict_class = "Guter Verlust"
        verdict_tone = "good"
    else:
        verdict_class = "Verlust mit Regelverstößen"
        verdict_tone = "bad"
    return {"good": good, "errors": errors, "lessons": lessons, "verdict_class": verdict_class, "verdict_tone": verdict_tone}


def _render_post_mortem_list(title: str, items: list[str], color: str) -> None:
    st.markdown(f'<div class="info-card"><div class="card-label" style="color:{color};">{html.escape(title)}</div>', unsafe_allow_html=True)
    if not items:
        st.markdown('<div style="color:#64748b;font-size:.85rem;">Keine Punkte erkannt.</div>', unsafe_allow_html=True)
    for item in items:
        st.markdown(f'<div style="padding:6px 0;border-bottom:1px solid #e3e8f0;font-size:.86rem;">{html.escape(str(item))}</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)


def _post_mortem_default_date(value):
    try:
        parsed = pd.Timestamp(value) if value else pd.Timestamp(datetime.now(timezone.utc).date())
        if pd.isna(parsed):
            return datetime.now(timezone.utc).date()
        return parsed.date()
    except Exception:
        return datetime.now(timezone.utc).date()


def _render_sell_decision_post_mortem() -> None:
    st.markdown("#### 🧾 Post-Mortem")
    st.caption("Analysiere abgeschlossene Trades, Regelverstöße und konkrete Lessons Learned.")

    closed_trades = load_closed_trades()
    saved = load_post_mortem_log()
    tranche_sources = []
    for entry in load_tranche_log():
        if not isinstance(entry, dict) or not entry.get("ticker"):
            continue
        tranche_sources.append({
            "ticker": entry.get("ticker"),
            "sell_date": entry.get("date"),
            "sell_price": entry.get("price"),
            "shares": entry.get("shares_sold"),
            "source": "tranche_log",
            "source_note": f"Realisierte Tranche {entry.get('tranche_percent', '—')}%",
        })

    sources = []
    for row in closed_trades:
        pnl = row.get("realized_pnl_percent")
        pnl_part = f" · {float(pnl):+.1f}%" if pnl is not None else ""
        note_part = f" · {row.get('notes')}" if row.get("notes") else ""
        sources.append((f"Abgeschlossener Trade · {row.get('ticker', '—')} · {row.get('sell_date', '—')}{pnl_part}{note_part}", row))
    for row in tranche_sources:
        sources.append((f"Tranche · {row.get('ticker', '—')} · {row.get('sell_date', '—')} · {row.get('source_note', '')}", row))
    for row in saved:
        sources.append((f"Gespeicherte Analyse · {row.get('ticker', '—')} · {row.get('sell_date', '—')} · {float(row.get('realized_pnl_percent') or 0):+.1f}%", row))

    if closed_trades:
        st.success(f"{len(closed_trades)} abgeschlossene Trade(s) aus dem Depot-Verkaufsbuch gefunden.")
    else:
        st.info("Im bestehenden Depot-Code werden erst ab jetzt gebuchte Verkäufe als abgeschlossene Trades gespeichert. Nutze bis dahin das manuelle Formular als Fallback.")

    if sources:
        labels = [label for label, _row in sources]
        selected_saved = st.selectbox("Abgeschlossene Trades / realisierte Tranchen", ["Manuell neu erfassen"] + labels, key="pm_saved_select")
    else:
        labels = []
        selected_saved = "Manuell neu erfassen"

    source = {}
    if sources and selected_saved != "Manuell neu erfassen":
        source = dict(sources[labels.index(selected_saved)][1])

    with st.form("post_mortem_form"):
        c1, c2, c3 = st.columns(3)
        ticker = c1.text_input("Ticker", value=source.get("ticker", ""), placeholder="z.B. NVDA")
        buy_date = c2.date_input("Kaufdatum", value=_post_mortem_default_date(source.get("buy_date")))
        sell_date = c3.date_input("Verkaufsdatum", value=_post_mortem_default_date(source.get("sell_date")))
        p1, p2, p3, p4 = st.columns(4)
        buy_price = p1.number_input("Kaufpreis", min_value=0.0, value=float(source.get("buy_price") or 0.0), step=0.01)
        sell_price = p2.number_input("Verkaufspreis", min_value=0.0, value=float(source.get("sell_price") or 0.0), step=0.01)
        shares = p3.number_input("Stückzahl optional", min_value=0.0, value=float(source.get("shares") or 0.0), step=1.0)
        pivot = p4.number_input("Pivot optional", min_value=0.0, value=float(source.get("pivot") or 0.0), step=0.01)
        planned_stop = st.number_input("Geplanter Stopp optional", min_value=0.0, value=float(source.get("planned_stop") or 0.0), step=0.01)
        stored_checks = source.get("checkboxes", {}) if isinstance(source.get("checkboxes", {}), dict) else {}
        st.markdown("##### Selbstauskunft")
        cols = st.columns(1 if _is_mobile_client() else 3)
        checks = {}
        for idx, (key, label) in enumerate(POST_MORTEM_CHECKBOXES):
            with cols[idx % len(cols)]:
                checks[key] = st.checkbox(label, value=bool(stored_checks.get(key, False)), key=f"pm_{key}")
        submitted = st.form_submit_button("Post-Mortem berechnen", type="primary", use_container_width=True)

    if not submitted and not source:
        return
    ticker = _normalize_single_ticker(ticker)
    if not ticker or buy_price <= 0 or sell_price <= 0:
        st.warning("Bitte Ticker, Kaufpreis und Verkaufspreis ausfüllen.")
        return
    with st.spinner(f"Lade Yahoo-Daten für {ticker} …"):
        metrics = load_post_mortem_trade_metrics(ticker, buy_date, sell_date, buy_price, sell_price, cache_buster=st.session_state.get("pm_cache_buster", 0))
    if not metrics.get("ok"):
        st.warning(metrics.get("error") or "Yahoo-Daten konnten nicht geladen werden.")
    else:
        st.caption(
            f"Yahoo-Haltedauerdaten geladen · Hoch: {metrics.get('high_price') or '—'} · "
            f"Tief: {metrics.get('low_price') or '—'}"
        )
    trade = {"ticker": ticker, "buy_date": str(buy_date), "buy_price": buy_price, "sell_date": str(sell_date), "sell_price": sell_price, "shares": shares or None, "pivot": pivot or None, "planned_stop": planned_stop or None}
    analysis = _analyze_post_mortem(trade, checks, metrics)
    # Saubere 2x2- bzw. 1x4-Aufteilung: auf Mobile zwei Zeilen à zwei Spalten,
    # statt die Metriken via Modulo-Index doppelt in Spalte 0/1 zu rendern.
    if _is_mobile_client():
        row1 = st.columns(2)
        row1[0].metric("Realisierter G/V", _fmt_pct(metrics.get("realized_pnl_percent")))
        row1[1].metric("Max. Gewinn", _fmt_pct(metrics.get("max_gain_percent")))
        row2 = st.columns(2)
        row2[0].metric("Max. Drawdown", _fmt_pct(-abs(metrics.get("max_drawdown_percent") or 0)))
        row2[1].metric("Haltedauer", f"{int(metrics.get('holding_days') or 0)} Tage")
    else:
        metric_cols = st.columns(4)
        metric_cols[0].metric("Realisierter G/V", _fmt_pct(metrics.get("realized_pnl_percent")))
        metric_cols[1].metric("Max. Gewinn", _fmt_pct(metrics.get("max_gain_percent")))
        metric_cols[2].metric("Max. Drawdown", _fmt_pct(-abs(metrics.get("max_drawdown_percent") or 0)))
        metric_cols[3].metric("Haltedauer", f"{int(metrics.get('holding_days') or 0)} Tage")

    if _is_mobile_client():
        _render_post_mortem_list("Was gut lief", analysis["good"], "#16a34a")
        _render_post_mortem_list("Regelverletzungen und Fehler", analysis["errors"], "#dc2626")
        _render_post_mortem_list("Lessons Learned", analysis["lessons"], "#2563eb")
    else:
        a, b, c = st.columns(3)
        with a:
            _render_post_mortem_list("Was gut lief", analysis["good"], "#16a34a")
        with b:
            _render_post_mortem_list("Regelverletzungen und Fehler", analysis["errors"], "#dc2626")
        with c:
            _render_post_mortem_list("Lessons Learned", analysis["lessons"], "#2563eb")

    tone = {"good": ("#16a34a", "#f0fdf4", "#bbf7d0"), "warn": ("#d97706", "#fffbeb", "#fde68a"), "bad": ("#dc2626", "#fef2f2", "#fecaca")}[analysis["verdict_tone"]]
    st.markdown(f'<div class="summary-hero" style="background:{tone[1]};border-color:{tone[2]};border-left:5px solid {tone[0]};"><div class="card-label" style="color:{tone[0]};">Trade-Verdict</div><div style="font-size:1.7rem;font-weight:800;color:{tone[0]};">{html.escape(analysis["verdict_class"])}</div></div>', unsafe_allow_html=True)
    if st.button("💾 Post-Mortem speichern", type="primary", use_container_width=True, key="pm_save"):
        entry = {
            "analysis_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"), **trade,
            "realized_pnl_percent": metrics.get("realized_pnl_percent"),
            "max_gain_percent": metrics.get("max_gain_percent"),
            "max_drawdown_percent": metrics.get("max_drawdown_percent"),
            "holding_days": metrics.get("holding_days"),
            "positive_points_count": len(analysis["good"]),
            "error_count": len(analysis["errors"]),
            "verdict_class": analysis["verdict_class"],
            "checkboxes": checks,
            "lessons_learned": analysis["lessons"],
            "trade_data": {**metrics, "high_price": metrics.get("high_price"), "low_price": metrics.get("low_price")},
        }
        append_post_mortem_result(entry)
        st.success("Post-Mortem dauerhaft gespeichert.")
        st.rerun()




def _render_sell_strategy_hub() -> None:
    st.markdown("#### 🧠 Strategien-Hub (Börse ohne Bauchgefühl)")
    positions = [p for p in st.session_state.get("positions", []) if float(_safe_float(p.get("shares"),0))>0]
    if not positions:
        st.info("Keine offenen Positionen im Depot.")
        return

    # ═══════════════════════════════════════════════════════════
    # 1. TICKEREINGABE (zuerst)
    # ═══════════════════════════════════════════════════════════
    t = st.selectbox("📌 Position auswählen", [p.get("ticker","") for p in positions], key="strat_hub_ticker")
    pos = next((x for x in positions if x.get("ticker")==t), None)
    if not pos:
        return
    buy_date = pd.Timestamp(pos.get("buy_date") or datetime.now(timezone.utc).date()).tz_localize(None)
    with st.spinner(f"Lade Kursdaten für {t} …"):
        df, info, *_ = load_stock_full(t)
    if df is None or len(df)<30:
        st.warning("Zu wenig Kursdaten.")
        return
    daily_all = pd.DataFrame({"open":df["Open"],"high":df["High"],"low":df["Low"],"close":df["Close"],"volume":df["Volume"]}).dropna()
    daily_all.index = pd.to_datetime(daily_all.index).tz_localize(None)
    daily = daily_all[daily_all.index >= buy_date]
    # Wochen-OHLC aus der vollständigen Historie, damit Strategien mit Mehrwochen-Bedarf
    # (drei_verlustwochen, ma_bruch_defensiv 10-Wochen-Linie, größter Einbruch Wochenregel)
    # genug Kerzen bekommen — auch bei sehr frisch gekauften Positionen.
    weekly_all = daily_all.resample("W-FRI").agg({"open":"first","high":"max","low":"min","close":"last","volume":"sum"}).dropna()
    # Buy-Date-gefilterte Wochen-/Tagesreihe nur noch für Peak-Berechnung (Drawdown vom Peak seit Einstieg).
    weekly = daily.resample("W-FRI").agg({"open":"first","high":"max","low":"min","close":"last","volume":"sum"}).dropna()
    # SPY/Benchmark für RS-Linie laden — sonst meldet die Strategie immer „Keine Benchmark-Daten".
    daten_spy = None
    wochen_daten_spy = None
    try:
        df_spy, *_ = load_stock_full("SPY")
        if df_spy is not None and len(df_spy) > 0:
            daten_spy = pd.DataFrame({"open":df_spy["Open"],"high":df_spy["High"],"low":df_spy["Low"],"close":df_spy["Close"],"volume":df_spy["Volume"]}).dropna()
            daten_spy.index = pd.to_datetime(daten_spy.index).tz_localize(None)
            wochen_daten_spy = daten_spy.resample("W-FRI").agg({"open":"first","high":"max","low":"min","close":"last","volume":"sum"}).dropna()
    except Exception:
        pass
    man = get_position_manual_sell_data(t)
    manual_pivot = _safe_float(man.get("pivot"), np.nan)
    pivot_tag_ts = pd.to_datetime(pos.get("pivot_tag"), errors="coerce")
    pivot_tag_ts = pivot_tag_ts.tz_localize(None) if pd.notna(pivot_tag_ts) and getattr(pivot_tag_ts, "tzinfo", None) else pivot_tag_ts
    pivot_from_tag = np.nan
    if pd.notna(pivot_tag_ts):
        row = daily_all.loc[daily_all.index == pivot_tag_ts]
        if not row.empty:
            pivot_from_tag = _safe_float(row["low"].iloc[-1], np.nan)
    entry_row = daily_all.loc[daily_all.index >= buy_date].head(1)
    pivot_from_entry_low = _safe_float(entry_row["low"].iloc[0], np.nan) if not entry_row.empty else np.nan
    effective_pivot = manual_pivot if not np.isnan(manual_pivot) and manual_pivot > 0 else (pivot_from_tag if not np.isnan(pivot_from_tag) and pivot_from_tag > 0 else pivot_from_entry_low)
    # Tag 1 = Kauftag (oder Pivot-Tag, falls gesetzt). Tag 0 = der Handelstag davor.
    # Fallen aus Kursdaten als Auto-Wert, falls keine manuelle Eingabe vorliegt.
    ref_ts_tief = pivot_tag_ts if pd.notna(pivot_tag_ts) else buy_date
    auto_tief_tag_1 = np.nan
    auto_tief_tag_0 = np.nan
    if not daily_all.empty:
        after_ref = daily_all.loc[daily_all.index >= ref_ts_tief]
        before_ref = daily_all.loc[daily_all.index < ref_ts_tief]
        if not after_ref.empty:
            auto_tief_tag_1 = _safe_float(after_ref["low"].iloc[0], np.nan)
        if not before_ref.empty:
            auto_tief_tag_0 = _safe_float(before_ref["low"].iloc[-1], np.nan)
    manual_low_day_1 = _safe_float(man.get("low_day_1"), np.nan)
    manual_low_day_0 = _safe_float(man.get("low_day_0"), np.nan)
    effective_tief_tag_1 = manual_low_day_1 if not np.isnan(manual_low_day_1) and manual_low_day_1 > 0 else auto_tief_tag_1
    effective_tief_tag_0 = manual_low_day_0 if not np.isnan(manual_low_day_0) and manual_low_day_0 > 0 else auto_tief_tag_0
    tief_tag_1_value = None if np.isnan(effective_tief_tag_1) else float(effective_tief_tag_1)
    tief_tag_0_value = None if np.isnan(effective_tief_tag_0) else float(effective_tief_tag_0)
    # Peak-Fallback: bei leerem daily-Frame (z. B. Kaufdatum in Zukunft / kein Kurs)
    # liefert .max() NaN. Position.peak=NaN bricht später Drawdown-Berechnungen — daher None.
    if daily is not None and not daily.empty and "high" in daily.columns:
        _peak_value = float(daily["high"].max())
        if np.isnan(_peak_value):
            _peak_value = None
    else:
        _peak_value = None
    p = Position(ticker=t,einstiegspreis=float(_safe_float(pos.get("buy_price"),0) or 0.0),einstiegsdatum=buy_date,stueckzahl=float(_safe_float(pos.get("shares"),0) or 0.0),pivot=_safe_float(effective_pivot),tief_tag_1=tief_tag_1_value,tief_tag_0=tief_tag_0_value,peak=_peak_value,realisierte_tranchen=[float(x.get("tranche_percent",0) or 0) for x in get_position_tranche_log(t)])
    alle = ["notbremse_verlust","gewinn_in_stufen","ma21_bruch","drawdown_vom_peak","ma_abstand","verlusttage_haeufung","groesster_anstieg_volumen","split_anstieg","erschoepfungsluecke","downside_reversal","stau_tage","rueckkehr_pivot","ma_bruch_defensiv","drei_verlustwochen","groesster_einbruch","rs_linie","ma_basierte_sequenz","einfach_halbe_position","misslungener_ausbruch_5stufen","einfache_verluststufen","atr_basiert"]
    strategie_info = {
        "notbremse_verlust": "Strategie 2 (Kap. 6.1): Marktabhängige Notbremse nach Verlusthöhe, die immer parallel zu allen anderen Regeln aktiv ist. Sobald die positionsbezogene P&L die Schwelle erreicht oder unterschreitet, wird ein Intraday-Vollausstieg (100%) ausgelöst. Standard-Schwellen: Bärisch 4%, Unsicher 5%, Bullisch 7%. Zusätzlich wird unterhalb der Schwelle eine konkrete Notbremse-Marke als kritischer Kurs angezeigt.",
        "gewinn_in_stufen": "Strategie 3 (Kap. 6.2): Gewinnmitnahme in Stufen mit Nachdenkschwelle und Pflicht-Teilverkauf. Standard: Bullisch/Unsicher 15% Hinweis, dann 20–35% Teilverkauf (33% bis 50% Tranche). Bärisch: 10% Hinweis, dann 10–15% Teilverkauf. Alle Schwellen sind im Setup konfigurierbar.",
        "ma21_bruch": "Verkaufssignale bei Bruch der 21-EMA (aggressiv/gestaffelt/geduldig).",
        "drawdown_vom_peak": "Reduktion nach Rückgang vom Zwischenhoch, abgestuft nach Drawdown-Tiefe.",
        "ma_abstand": "Überdehnungen relativ zu 10-MA / 21-EMA / 50-MA / 200-MA als Gewinnmitnahme-Signal.",
        "verlusttage_haeufung": "Strategie 7 (Kap. 6.2): Erkennt Distribution über eine Häufung schwacher Tage. Signal 1: drei tiefere Schlusskurse in Folge mit erhöhter 3-Tage-Volumenquote gegen den Referenz-Lookback. Signal 2: im Up/Down-Fenster überwiegen Abwärtstage gegenüber Aufwärtstagen deutlich (Mindestdifferenz). Beide Trigger erzeugen standardmäßig eine 25%-Tranche mit Stop-/Marker-Logik auf lokale Tiefs.",
        "groesster_anstieg_volumen": "Klimax-/Spätphasen-Signal: größter Tagesanstieg mit extremem Volumen.",
        "split_anstieg": "Strategie 10 (Kap. 6.2): Warnt vor möglichem Gipfel, wenn die Aktie innerhalb der ersten 1-2 Wochen nach Aktiensplit stark steigt. Trigger nur, wenn ein Split-Datum bekannt ist. Ab +25% seit Split wird ein aktives Signal erzeugt (33% Tranche), ab +50% erhöht auf 50%. Referenz-/Stoppmarke ist der Schlusskurs am Split-Tag. Split-Datum wird bevorzugt automatisch via Yahoo Finance gesucht; falls dort kein verwertbarer Split in den letzten 14 Tagen gefunden wird, kann das Datum manuell gesetzt werden.",
        "erschoepfungsluecke": "Gap-up nach langem Lauf mit hohem Volumen als Erschöpfungssignal.",
        "downside_reversal": "Strategie 12 (Kap. 6.2): Downside Reversal für Gewinnerpositionen. Variante 1 (stark): neues 30-Tage-Hoch, Schluss im unteren Tagesdrittel und Volumenquote ≥ 1.2 erzeugt 33%-Signal („Downside Reversal an neuem Hoch“). Variante 2 (mittel): weite Umkehrkerze (Tagesspanne ≥ 1.5× 10-Tage-Schnitt), Schluss im unteren Drittel und Volumenquote ≥ 1.2 erzeugt 20%-Signal. Variante 3 (Warnstufe): weite Kerze mit Schluss unter Spannenmitte erzeugt 15%-Signal. Nächste Marke ist jeweils das Tageshoch der Umkehrkerze.",
        "stau_tage": "Strategie 13 (Kap. 6.2): Sucht in einem Fenster (Standard 10 Sessions) nach Stau-Tagen mit kaum Fortschritt (|Tagesveränderung| < 1%) bei überdurchschnittlichem Volumen (≥1.3× gegen 50-Tage-Schnitt). Ab mindestens 2 Stau-Tagen entsteht ein aktives Verkaufssignal. Die Tranche ist kontextabhängig: nahe Hoch (Drawdown < 5%) defensiver mit 33%, sonst 20%. Als nächste Marke wird das tiefste Tagestief der erkannten Stau-Tage gesetzt (Stopp-Logik).",
        "rueckkehr_pivot": "Strategie 14 (Kap. 6.3): Rückkehr zum Ausbruchspunkt. Sicherheitslinie 1 ist ein Schlusskurs unter Tief Tag 1 (33%; bei Volumenquote ≥1.5 auf 50% erhöht). Sicherheitslinie 2 ist ein Schlusskurs unter Tief Tag 0 (weitere 33%). Bleibt die Aktie 10 Handelstage in Folge unter dem Pivot, folgt ein 50%-Signal wegen ausbleibender Rückeroberung des Ausbruchspunkt. Alternativ Notbremse bei max. Verlust (Standard -7%) als Intraday-Vollausstieg. Pivot-Quelle: zuerst Wert aus dem Bereich ‚Meine Positionen‘; falls dort kein Pivot hinterlegt ist, wird der Kauftag als Fallback genutzt.",
        "ma_bruch_defensiv": "Strategie 15 (Kap. 6.3): Defensiver Exit-Prozess bei Trendbruch. Klarer 50-MA-Bruch (mind. max(2%, ATR%) unter MA und Volumenanstieg) triggert 50%, sonst nach 3 Schlusskursen unter 50-MA 33%. Nach 8 Wochen unter der 10-Wochen-Linie folgt ein Vollsignal (100%). Unter 200-MA werden 75% reduziert bzw. 100% bei hohem Volumen; dreht die 200-MA zusätzlich nach unten, wird ein bestätigendes Info-Signal ausgegeben.",
        "drei_verlustwochen": "Strategie 16 (Kap. 6.3): Triggert bei drei Verlustwochen in Folge mit jeweils tieferem Wochenschluss als in der Vorwoche und gleichzeitig steigendem Wochenvolumen (Woche 2 > Woche 1, Woche 3 > Woche 2). Vollsignal (100%) nur, wenn alle drei Wochen klare Abwärtswochen sind (Close < Open) – das spricht für ein sauberes Verteilungsmuster und eine komplette Reduktion. Vorwarnstufe (33%) falls nur die Sequenz aus fallenden Wochenschlüssen + steigendem Volumen erfüllt ist; dann Stopps enger nachziehen.",
        "groesster_einbruch": "Strategie 17 (Kap. 6.3): Reagiert auf den größten Einbruch seit Einstieg nach bereits gelaufener Position. Tagesregel: wenn der heutige Verlust der größte seit Einstieg ist und über einer Mindestschwelle liegt, wird defensiv reduziert (33%) oder bei deutlich erhöhtem Volumen stärker (50%). Wochenregel: wenn die aktuelle Verlustwoche die größte seit Einstieg ist und gleichzeitig das Wochenvolumen überdurchschnittlich hoch ist, folgt eine starke Reduktion (66%). Ziel: späte Rally-Phasen mit möglicher Verteilung früh absichern.",
        "rs_linie": "3-Stufen-Strategie auf Basis relativer Stärke gegen Benchmark (z. B. SPY).",
        "ma_basierte_sequenz": "Geschlossene MA-Verkaufssequenz von Gewinnzone bis klarem 50-MA-Bruch.",
        "einfach_halbe_position": "Einfache 50/50-Logik: erste Hälfte sichern, Rest über BE/erneute Stärke managen.",
        "misslungener_ausbruch_5stufen": "Fehl-Ausbruch detailliert in 5 Stufen inkl. Intraday-/Gap-Logik.",
        "einfache_verluststufen": "Minimalregel: gestaffelt bei -3% / -5% / -7% reduzieren.",
        "atr_basiert": "Adaptive Verkaufsregel: statt fester Prozentwerte verwendet sie die typische Schwankungsbreite (ATR) der Aktie. Eignet sich besonders für volatile Aktien, die mit festen Prozent-Schwellen zu früh ausgestoppt würden. Regeln: Teilverkauf (33%) ab ATR-Ziel-Gewinn, Vollausstieg bei Schluss ≤ Einstieg minus 1.5 ATR, sowie Überdehnungs-Teilverkauf über 21-EMA ab x ATR (Basis) bzw. y ATR (stark, 50%).",
    }
    # ═══════════════════════════════════════════════════════════
    # Parameter aus session_state lesen (oder Defaults verwenden).
    # Die zugehörigen Widgets stehen im Setup-Bereich am Ende.
    # ═══════════════════════════════════════════════════════════
    def _ss(key, default):
        return st.session_state.get(key, default)

    aktive = _ss(f"strat_hub_multi_{t}", list(alle))
    aktive = [k for k in aktive if k in alle] or list(alle)

    markt_default = man.get("market_environment", "Unsicher")
    if markt_default not in ("Bullisch","Unsicher","Bärisch"):
        markt_default = "Unsicher"
    markt = _ss(f"strat_hub_mkt_{t}", markt_default)

    ma21_variante = _ss(f"strat_hub_ma21_variante_{t}", "gestaffelt")
    ziel_atr = float(_ss(f"strat_hub_atr_mult_{t}", 3.0))
    atr_ueberdehnung_start = float(_ss(f"strat_hub_atr_ext_start_{t}", 3.0))
    atr_ueberdehnung_stark = float(_ss(f"strat_hub_atr_ext_strong_{t}", 4.0))
    drawdown_stufe1_min_pct = float(_ss(f"strat_hub_dd_stage1_{t}", 8.0))
    drawdown_stufe2_min_pct = float(_ss(f"strat_hub_dd_stage2_{t}", 12.0))
    drawdown_stufe3_min_pct = float(_ss(f"strat_hub_dd_stage3_{t}", 15.0))
    drawdown_tranche_stufe1_pct = float(_ss(f"strat_hub_dd_tranche1_{t}", 25.0))
    drawdown_tranche_stufe2_pct = float(_ss(f"strat_hub_dd_tranche2_{t}", 33.0))
    drawdown_tranche_stufe3_ohne_pct = float(_ss(f"strat_hub_dd_tranche3a_{t}", 50.0))
    drawdown_tranche_stufe3_mit_pct = float(_ss(f"strat_hub_dd_tranche3b_{t}", 100.0))
    verlust_stufe_1 = float(_ss(f"strat_hub_loss_stage1_{t}", 3.0))
    verlust_stufe_2 = float(_ss(f"strat_hub_loss_stage2_{t}", 5.0))
    verlust_stufe_3 = float(_ss(f"strat_hub_loss_stage3_{t}", 7.0))
    notbremse_verlust_schwelle_baerisch_pct = float(_ss(f"strat_hub_notbremse_bear_{t}", 4.0))
    notbremse_verlust_schwelle_unsicher_pct = float(_ss(f"strat_hub_notbremse_uncertain_{t}", 5.0))
    notbremse_verlust_schwelle_bullisch_pct = float(_ss(f"strat_hub_notbremse_bull_{t}", 7.0))
    erste_haelfte_gewinn_pct = float(_ss(f"strat_hub_first_half_profit_{t}", 20.0))
    gewinn_nachdenken_schwelle_bull_pct = float(_ss(f"strat_hub_gainstep_think_bull_{t}", 15.0))
    gewinn_teilverkauf_unten_bull_pct = float(_ss(f"strat_hub_gainstep_lo_bull_{t}", 20.0))
    gewinn_teilverkauf_oben_bull_pct = float(_ss(f"strat_hub_gainstep_hi_bull_{t}", 35.0))
    gewinn_nachdenken_schwelle_bear_pct = float(_ss(f"strat_hub_gainstep_think_bear_{t}", 10.0))
    gewinn_teilverkauf_unten_bear_pct = float(_ss(f"strat_hub_gainstep_lo_bear_{t}", 10.0))
    gewinn_teilverkauf_oben_bear_pct = float(_ss(f"strat_hub_gainstep_hi_bear_{t}", 15.0))
    ma_seq_gewinnzone_min_pct = float(_ss(f"strat_hub_ma_seq_gain_min_{t}", 20.0))
    ma_seq_gewinnzone_max_pct = float(_ss(f"strat_hub_ma_seq_gain_max_{t}", 25.0))
    ma_seq_gewinnzone_tranche_pct = float(_ss(f"strat_hub_ma_seq_gain_tranche_{t}", 33.0))
    ma_seq_ueber_ma10_pct = float(_ss(f"strat_hub_ma_seq_over10_pct_{t}", 10.0))
    ma_seq_ueber_ma10_tranche_pct = float(_ss(f"strat_hub_ma_seq_over10_tranche_{t}", 20.0))
    ma_seq_unter_ma10_mindestgewinn_pct = float(_ss(f"strat_hub_ma_seq_under10_minpnl_{t}", 5.0))
    ma_seq_unter_ma10_tranche_pct = float(_ss(f"strat_hub_ma_seq_under10_tranche_{t}", 20.0))
    ma_seq_pendel_tranche_pct = float(_ss(f"strat_hub_ma_seq_pendel_tranche_{t}", 25.0))
    ma_seq_pendel_lookback_tage = int(_ss(f"strat_hub_ma_seq_pendel_lb_{t}", 5))
    ma_seq_pendel_wechsel_min = int(_ss(f"strat_hub_ma_seq_pendel_switch_{t}", 3))
    ma_seq_unter_ma21_mindestgewinn_pct = float(_ss(f"strat_hub_ma_seq_under21_minpnl_{t}", 5.0))
    ma_seq_unter_ma21_tranche_pct = float(_ss(f"strat_hub_ma_seq_under21_tranche_{t}", 25.0))
    ma_seq_klarer_ma50_bruch_pct = float(_ss(f"strat_hub_ma_seq_under50_clear_{t}", 2.0))
    rs_pnl_tag_zu_woche = float(_ss(f"strat_hub_rs_pnl_day_week_{t}", 20.0))
    rs_pnl_woche_zu_monat = float(_ss(f"strat_hub_rs_pnl_week_month_{t}", 80.0))
    groesster_einbruch_min_pnl_pct = float(_ss(f"strat_hub_worst_drop_min_pnl_{t}", 10.0))
    groesster_einbruch_min_tagesverlust_pct = float(_ss(f"strat_hub_worst_drop_day_loss_{t}", 3.0))
    groesster_einbruch_tagesvol_ratio_schwelle = float(_ss(f"strat_hub_worst_drop_day_vol_ratio_{t}", 1.5))
    groesster_einbruch_wochenvol_ratio_schwelle = float(_ss(f"strat_hub_worst_drop_week_vol_ratio_{t}", 1.3))
    stau_fenster_tage = int(_ss(f"strat_hub_stall_window_{t}", 10))
    stau_volumen_lookback_tage = int(_ss(f"strat_hub_stall_vol_lookback_{t}", 50))
    stau_max_tagesveraenderung_pct = float(_ss(f"strat_hub_stall_max_day_change_{t}", 1.0))
    stau_min_vol_ratio = float(_ss(f"strat_hub_stall_min_vol_ratio_{t}", 1.3))
    stau_min_tage = int(_ss(f"strat_hub_stall_min_days_{t}", 2))
    stau_nahe_hoch_drawdown_max_pct = float(_ss(f"strat_hub_stall_near_high_dd_{t}", 5.0))
    stau_tranche_nahe_hoch_pct = float(_ss(f"strat_hub_stall_tranche_near_high_{t}", 33.0))
    stau_tranche_standard_pct = float(_ss(f"strat_hub_stall_tranche_standard_{t}", 20.0))
    verlusttage_min_tiefere_schlusskurse_in_folge = int(_ss(f"strat_hub_losscluster_min_seq_{t}", 3))
    verlusttage_volumen_lookback_tage = int(_ss(f"strat_hub_losscluster_vol_lb_{t}", 50))
    verlusttage_volumen_ratio_min = float(_ss(f"strat_hub_losscluster_vol_ratio_{t}", 1.1))
    verlusttage_tief_marker_lookback_tage = int(_ss(f"strat_hub_losscluster_low_marker_lb_{t}", 5))
    verlusttage_updown_fenster_tage = int(_ss(f"strat_hub_losscluster_ud_window_{t}", 15))
    verlusttage_updown_diff_min = int(_ss(f"strat_hub_losscluster_ud_diff_{t}", 3))
    verlusttage_tranche_pct = float(_ss(f"strat_hub_losscluster_tranche_{t}", 25.0))
    downside_kerzenweite_lookback_tage = int(_ss(f"strat_hub_downside_candle_lb_{t}", 10))
    downside_volumen_lookback_tage = int(_ss(f"strat_hub_downside_vol_lb_{t}", 50))
    downside_neues_hoch_lookback_tage = int(_ss(f"strat_hub_downside_high_lb_{t}", 30))
    downside_weite_kerze_faktor = float(_ss(f"strat_hub_downside_wide_factor_{t}", 1.5))
    downside_volumen_ratio_min = float(_ss(f"strat_hub_downside_vol_ratio_{t}", 1.2))
    downside_schluss_unteres_drittel_faktor = float(_ss(f"strat_hub_downside_lower_third_factor_{t}", 3.0))
    downside_tranche_neues_hoch_pct = float(_ss(f"strat_hub_downside_tranche_high_{t}", 33.0))
    downside_tranche_weite_umkehr_pct = float(_ss(f"strat_hub_downside_tranche_wide_{t}", 20.0))
    downside_tranche_warnstufe_pct = float(_ss(f"strat_hub_downside_tranche_warn_{t}", 15.0))
    split_lookback_tage = int(_ss(f"strat_hub_split_lookback_{t}", 30))
    split_auto_tagefenster = int(_ss(f"strat_hub_split_age_window_{t}", 14))
    split_signal_schwelle_pct = float(_ss(f"strat_hub_split_signal_threshold_{t}", 25.0))
    split_starke_schwelle_pct = float(_ss(f"strat_hub_split_strong_threshold_{t}", 50.0))
    ma_abstand_schwelle_ma10_pct = float(_ss(f"strat_hub_ma_dist_th10_{t}", 10.0))
    ma_abstand_schwelle_ma21_pct = float(_ss(f"strat_hub_ma_dist_th21_{t}", 15.0))
    ma_abstand_schwelle_ma50_pct = float(_ss(f"strat_hub_ma_dist_th50_{t}", 25.0))
    ma_abstand_schwelle_ma200_pct = float(_ss(f"strat_hub_ma_dist_th200_{t}", 70.0))
    ma_abstand_klimax_ma200_vollausstieg_pct = float(_ss(f"strat_hub_ma_dist_th200_full_{t}", 100.0))
    ma_abstand_tranche_ma10_pct = float(_ss(f"strat_hub_ma_dist_tr10_{t}", 25.0))
    ma_abstand_tranche_ma21_pct = float(_ss(f"strat_hub_ma_dist_tr21_{t}", 33.0))
    ma_abstand_tranche_ma50_pct = float(_ss(f"strat_hub_ma_dist_tr50_{t}", 33.0))
    ma_abstand_tranche_ma200_basis_pct = float(_ss(f"strat_hub_ma_dist_tr200_{t}", 50.0))
    rueckkehr_tranche_stufe1_pct = float(_ss(f"strat_hub_rueckkehr_tranche_1_{t}", 33.0))
    rueckkehr_tranche_stufe1_volumen_pct = float(_ss(f"strat_hub_rueckkehr_tranche_1_vol_{t}", 50.0))
    rueckkehr_volumen_schwelle = float(_ss(f"strat_hub_rueckkehr_vol_schwelle_{t}", 1.5))
    rueckkehr_tranche_stufe2_pct = float(_ss(f"strat_hub_rueckkehr_tranche_2_{t}", 33.0))
    rueckkehr_pivot_tage_schwelle = int(_ss(f"strat_hub_rueckkehr_pivot_tage_{t}", 10))
    rueckkehr_tranche_pivot_pct = float(_ss(f"strat_hub_rueckkehr_pivot_tranche_{t}", 50.0))
    rueckkehr_notbremse_verlust_pct = float(_ss(f"strat_hub_rueckkehr_notbremse_{t}", 7.0))

    # ── Split-Datum: Yahoo-Auto-Lookup + Fallback auf manuelle Eingabe ──
    split_auto_datum = None
    split_auto_msg = None
    if "split_anstieg" in aktive:
        try:
            splits = yf.Ticker(t).splits
            if isinstance(splits, pd.Series) and not splits.empty:
                splits_idx = pd.to_datetime(splits.index).tz_localize(None)
                cutoff = pd.Timestamp.now(tz="UTC").tz_localize(None) - pd.Timedelta(days=int(split_lookback_tage))
                recent_idx = splits_idx[splits_idx >= cutoff]
                if len(recent_idx) > 0:
                    candidate = recent_idx.max()
                    age_days = int((pd.Timestamp.now(tz="UTC").tz_localize(None) - candidate).days)
                    if age_days <= int(split_auto_tagefenster):
                        split_auto_datum = candidate
                        split_auto_msg = ("success", f"Yahoo-Split erkannt: {candidate.date().isoformat()} ({age_days} Tage alt).")
                    else:
                        split_auto_msg = ("info", f"Yahoo-Split gefunden ({candidate.date().isoformat()}), aber älter als {split_auto_tagefenster} Tage.")
                else:
                    split_auto_msg = ("info", "Yahoo lieferte keine Splits im gewählten Lookback.")
            else:
                split_auto_msg = ("info", "Yahoo lieferte keine Split-Historie für diesen Ticker.")
        except Exception as exc:
            split_auto_msg = ("warning", f"Yahoo-Splitabfrage fehlgeschlagen: {exc}")
    manual_split = _ss(f"strat_hub_split_manual_date_{t}", None)
    split_datum = split_auto_datum if split_auto_datum is not None else (pd.Timestamp(manual_split) if manual_split else None)

    # ═══════════════════════════════════════════════════════════
    # Ergebnis berechnen
    # ═══════════════════════════════════════════════════════════
    strategie_optionen = {
            "ma21_variante": ma21_variante,
            "ziel_atr_multiplikator": float(ziel_atr),
            "ueberdehnung_atr_start": float(atr_ueberdehnung_start),
            "ueberdehnung_atr_stark": float(max(atr_ueberdehnung_stark, atr_ueberdehnung_start)),
            "verlust_stufe_1": float(verlust_stufe_1),
            "verlust_stufe_2": float(max(verlust_stufe_2, verlust_stufe_1)),
            "verlust_stufe_3": float(max(verlust_stufe_3, max(verlust_stufe_2, verlust_stufe_1))),
            "notbremse_verlust_schwelle_baerisch_pct": float(notbremse_verlust_schwelle_baerisch_pct),
            "notbremse_verlust_schwelle_unsicher_pct": float(notbremse_verlust_schwelle_unsicher_pct),
            "notbremse_verlust_schwelle_bullisch_pct": float(notbremse_verlust_schwelle_bullisch_pct),
            "rueckkehr_tranche_stufe1_pct": float(rueckkehr_tranche_stufe1_pct),
            "rueckkehr_tranche_stufe1_volumen_pct": float(rueckkehr_tranche_stufe1_volumen_pct),
            "rueckkehr_volumen_schwelle": float(rueckkehr_volumen_schwelle),
            "rueckkehr_tranche_stufe2_pct": float(rueckkehr_tranche_stufe2_pct),
            "rueckkehr_pivot_tage_schwelle": int(rueckkehr_pivot_tage_schwelle),
            "rueckkehr_tranche_pivot_pct": float(rueckkehr_tranche_pivot_pct),
            "rueckkehr_notbremse_verlust_pct": float(rueckkehr_notbremse_verlust_pct),
            "gewinn_nachdenken_schwelle_bull_pct": float(gewinn_nachdenken_schwelle_bull_pct),
            "gewinn_teilverkauf_unten_bull_pct": float(max(gewinn_teilverkauf_unten_bull_pct, gewinn_nachdenken_schwelle_bull_pct)),
            "gewinn_teilverkauf_oben_bull_pct": float(max(gewinn_teilverkauf_oben_bull_pct, gewinn_teilverkauf_unten_bull_pct, gewinn_nachdenken_schwelle_bull_pct)),
            "gewinn_nachdenken_schwelle_bear_pct": float(gewinn_nachdenken_schwelle_bear_pct),
            "gewinn_teilverkauf_unten_bear_pct": float(max(gewinn_teilverkauf_unten_bear_pct, gewinn_nachdenken_schwelle_bear_pct)),
            "gewinn_teilverkauf_oben_bear_pct": float(max(gewinn_teilverkauf_oben_bear_pct, gewinn_teilverkauf_unten_bear_pct, gewinn_nachdenken_schwelle_bear_pct)),
            "erste_haelfte_gewinn_pct": float(erste_haelfte_gewinn_pct),
            "ma_seq_gewinnzone_min_pct": float(ma_seq_gewinnzone_min_pct),
            "ma_seq_gewinnzone_max_pct": float(max(ma_seq_gewinnzone_max_pct, ma_seq_gewinnzone_min_pct)),
            "ma_seq_gewinnzone_tranche_pct": float(ma_seq_gewinnzone_tranche_pct),
            "ma_seq_ueber_ma10_pct": float(ma_seq_ueber_ma10_pct),
            "ma_seq_ueber_ma10_tranche_pct": float(ma_seq_ueber_ma10_tranche_pct),
            "ma_seq_unter_ma10_mindestgewinn_pct": float(ma_seq_unter_ma10_mindestgewinn_pct),
            "ma_seq_unter_ma10_tranche_pct": float(ma_seq_unter_ma10_tranche_pct),
            "ma_seq_pendel_tranche_pct": float(max(ma_seq_pendel_tranche_pct, ma_seq_unter_ma10_tranche_pct)),
            "ma_seq_pendel_lookback_tage": int(ma_seq_pendel_lookback_tage),
            "ma_seq_pendel_wechsel_min": int(ma_seq_pendel_wechsel_min),
            "ma_seq_unter_ma21_mindestgewinn_pct": float(ma_seq_unter_ma21_mindestgewinn_pct),
            "ma_seq_unter_ma21_tranche_pct": float(ma_seq_unter_ma21_tranche_pct),
            "ma_seq_klarer_ma50_bruch_pct": float(ma_seq_klarer_ma50_bruch_pct),
            "rs_pnl_tag_zu_woche": float(rs_pnl_tag_zu_woche),
            "rs_pnl_woche_zu_monat": float(max(rs_pnl_woche_zu_monat, rs_pnl_tag_zu_woche)),
            "groesster_einbruch_min_pnl_pct": float(groesster_einbruch_min_pnl_pct),
            "groesster_einbruch_min_tagesverlust_pct": float(groesster_einbruch_min_tagesverlust_pct),
            "groesster_einbruch_tagesvol_ratio_schwelle": float(groesster_einbruch_tagesvol_ratio_schwelle),
            "groesster_einbruch_wochenvol_ratio_schwelle": float(groesster_einbruch_wochenvol_ratio_schwelle),
            "stau_fenster_tage": int(stau_fenster_tage),
            "stau_volumen_lookback_tage": int(stau_volumen_lookback_tage),
            "stau_max_tagesveraenderung_pct": float(stau_max_tagesveraenderung_pct),
            "stau_min_vol_ratio": float(stau_min_vol_ratio),
            "stau_min_tage": int(stau_min_tage),
            "stau_nahe_hoch_drawdown_max_pct": float(stau_nahe_hoch_drawdown_max_pct),
            "stau_tranche_nahe_hoch_pct": float(stau_tranche_nahe_hoch_pct),
            "stau_tranche_standard_pct": float(stau_tranche_standard_pct),
            "verlusttage_min_tiefere_schlusskurse_in_folge": int(verlusttage_min_tiefere_schlusskurse_in_folge),
            "verlusttage_volumen_lookback_tage": int(verlusttage_volumen_lookback_tage),
            "verlusttage_volumen_ratio_min": float(verlusttage_volumen_ratio_min),
            "verlusttage_tief_marker_lookback_tage": int(verlusttage_tief_marker_lookback_tage),
            "verlusttage_updown_fenster_tage": int(verlusttage_updown_fenster_tage),
            "verlusttage_updown_diff_min": int(verlusttage_updown_diff_min),
            "verlusttage_tranche_pct": float(verlusttage_tranche_pct),
            "downside_kerzenweite_lookback_tage": int(downside_kerzenweite_lookback_tage),
            "downside_volumen_lookback_tage": int(downside_volumen_lookback_tage),
            "downside_neues_hoch_lookback_tage": int(downside_neues_hoch_lookback_tage),
            "downside_weite_kerze_faktor": float(downside_weite_kerze_faktor),
            "downside_volumen_ratio_min": float(downside_volumen_ratio_min),
            "downside_schluss_unteres_drittel_faktor": float(downside_schluss_unteres_drittel_faktor),
            "downside_tranche_neues_hoch_pct": float(downside_tranche_neues_hoch_pct),
            "downside_tranche_weite_umkehr_pct": float(downside_tranche_weite_umkehr_pct),
            "downside_tranche_warnstufe_pct": float(downside_tranche_warnstufe_pct),
            "split_datum": split_datum,
            "split_signal_schwelle_pct": float(split_signal_schwelle_pct),
            "split_starke_schwelle_pct": float(max(split_starke_schwelle_pct, split_signal_schwelle_pct)),
            "ma_abstand_schwelle_ma10_pct": float(ma_abstand_schwelle_ma10_pct),
            "ma_abstand_schwelle_ma21_pct": float(max(ma_abstand_schwelle_ma21_pct, ma_abstand_schwelle_ma10_pct)),
            "ma_abstand_schwelle_ma50_pct": float(max(ma_abstand_schwelle_ma50_pct, ma_abstand_schwelle_ma21_pct)),
            "ma_abstand_schwelle_ma200_pct": float(max(ma_abstand_schwelle_ma200_pct, ma_abstand_schwelle_ma50_pct)),
            "ma_abstand_klimax_ma200_vollausstieg_pct": float(max(ma_abstand_klimax_ma200_vollausstieg_pct, ma_abstand_schwelle_ma200_pct)),
            "ma_abstand_tranche_ma10_pct": float(ma_abstand_tranche_ma10_pct),
            "ma_abstand_tranche_ma21_pct": float(ma_abstand_tranche_ma21_pct),
            "ma_abstand_tranche_ma50_pct": float(ma_abstand_tranche_ma50_pct),
            "ma_abstand_tranche_ma200_basis_pct": float(ma_abstand_tranche_ma200_basis_pct),
            # Strategie 5 (drawdown_vom_peak): Schwellen werden monoton durchgereicht.
            "drawdown_stufe1_min_pct": float(drawdown_stufe1_min_pct),
            "drawdown_stufe2_min_pct": float(max(drawdown_stufe2_min_pct, drawdown_stufe1_min_pct)),
            "drawdown_stufe3_min_pct": float(max(drawdown_stufe3_min_pct, max(drawdown_stufe2_min_pct, drawdown_stufe1_min_pct))),
            "tranche_stufe1_pct": float(drawdown_tranche_stufe1_pct),
            "tranche_stufe2_pct": float(drawdown_tranche_stufe2_pct),
            "tranche_stufe3_ohne_trendbruch_pct": float(drawdown_tranche_stufe3_ohne_pct),
            "tranche_stufe3_mit_trendbruch_pct": float(max(drawdown_tranche_stufe3_mit_pct, drawdown_tranche_stufe3_ohne_pct)),
    }
    res = verkaufs_empfehlung_gesamt(
        p,
        daily_all,
        weekly_all,
        daten_spy,
        wochen_daten_spy,
        markt,
        man.get("industry_group_status","Neutral"),
        aktive,
        strategie_optionen,
    )

    # ═══════════════════════════════════════════════════════════
    # 2. GESAMTÜBERSICHT
    # ═══════════════════════════════════════════════════════════
    st.markdown("### 📊 Gesamtübersicht")
    mc1, mc2, mc3 = st.columns(3)
    with mc1:
        st.metric("Gesamt-Tranche", f"{res['gesamt_tranche']}%")
    with mc2:
        st.metric("Jetzt zu verkaufen", f"{res['jetzt_zu_verkaufen']}%")
    with mc3:
        st.metric("Bereits realisiert", f"{res['bereits_realisiert']}%")
    if res.get("haupt_grund"):
        st.caption(f"**Hauptgrund:** {res['haupt_grund']}")

    # Pro-Strategie-Signale gruppieren für die Detail-Abschnitte
    signals_by_key: dict[str, list[dict]] = {}
    for s in res.get("alle_signale", []):
        k = str(s.get("strategy_key") or "")
        signals_by_key.setdefault(k, []).append(s)

    def _format_trigger_label(trigger_typ: str) -> str:
        labels = {
            "schluss": "Schlusskurs",
            "intraday": "Intraday",
            "info": "Info",
        }
        return labels.get(str(trigger_typ or "").lower(), str(trigger_typ or ""))

    def _render_signal_card(sig: dict) -> None:
        name = str(sig.get("name") or "Signal")
        tranche = sig.get("tranche_pct", 0)
        aktiv = bool(sig.get("aktuell_aktiv"))
        trigger = _format_trigger_label(str(sig.get("trigger_typ") or ""))
        marke = sig.get("naechste_marke")
        ref = str(sig.get("buch_verweis") or "")
        grund = str(sig.get("begruendung") or "")
        headline = f"**{name}** · Tranche **{int(tranche)}%**"
        if trigger:
            headline += f" · _{trigger}_"
        if aktiv and int(tranche) >= 100:
            st.error(f"🚨 {headline}")
        elif aktiv and int(tranche) > 0:
            st.warning(f"⚠️ {headline}")
        elif aktiv:
            st.info(f"ℹ️ {headline}")
        else:
            st.markdown(f"✅ {headline} — _kein aktives Signal_")
        meta_bits = []
        if marke not in (None, "", 0):
            try:
                meta_bits.append(f"Nächste Marke: **{float(marke):.2f}**")
            except Exception:
                meta_bits.append(f"Nächste Marke: **{marke}**")
        if ref:
            meta_bits.append(ref)
        if meta_bits:
            st.caption(" · ".join(meta_bits))
        if grund:
            st.caption(grund)

    # ═══════════════════════════════════════════════════════════
    # 3. STRATEGIEN IM DETAIL (Erklärung + aktuelles Ergebnis je Strategie)
    # ═══════════════════════════════════════════════════════════
    st.markdown("### 🎯 Strategien im Detail")
    st.caption(f"Pro Strategie wird zuerst die Erklärung gezeigt, danach das aktuelle Ergebnis für **{t}**.")

    for key in aktive:
        sigs = signals_by_key.get(key, [])
        active_count = sum(1 for s in sigs if s.get("aktuell_aktiv"))
        max_tranche = max((int(s.get("tranche_pct") or 0) for s in sigs if s.get("aktuell_aktiv")), default=0)
        if max_tranche >= 100:
            status_icon = "🚨"
        elif active_count > 0:
            status_icon = "⚠️"
        elif sigs:
            status_icon = "✅"
        else:
            status_icon = "•"
        header = f"{status_icon} {key}"
        if active_count > 0:
            header += f" — {active_count} aktiv · max. {max_tranche}%"
        with st.expander(header, expanded=False):
            st.markdown(f"**📖 Erklärung:** {strategie_info.get(key, 'Keine Beschreibung hinterlegt.')}")
            st.markdown("---")
            st.markdown(f"**📈 Aktuelles Ergebnis für `{t}`:**")
            if not sigs:
                try:
                    grund = diagnose_strategie_kein_signal(
                        key, p, daily_all, weekly_all, daten_spy, wochen_daten_spy, markt, strategie_optionen,
                    )
                except Exception as exc:
                    grund = f"Diagnose fehlgeschlagen: {exc}"
                st.info(f"Kein aktives Signal — {grund}")
            else:
                for s in sigs:
                    _render_signal_card(s)

    # ═══════════════════════════════════════════════════════════
    # 4. SETUP (am Ende)
    # ═══════════════════════════════════════════════════════════
    st.markdown("---")
    with st.expander("⚙️ Setup – Strategien & Parameter", expanded=False):
        st.caption("Hier wählst du das Marktumfeld, aktive Strategien und passt Parameter pro Strategie an. Änderungen wirken sofort auf die Abschnitte oben.")
        sc1, sc2 = st.columns([1, 3])
        with sc1:
            markt = st.selectbox(
                "Marktumfeld",
                ["Bullisch", "Unsicher", "Bärisch"],
                index=["Bullisch", "Unsicher", "Bärisch"].index(markt),
                key=f"strat_hub_mkt_{t}",
            )
        with sc2:
            aktive = st.multiselect(
                "Aktive Strategien",
                alle,
                default=aktive,
                key=f"strat_hub_multi_{t}",
                help="Wähle hier, welche Strategien in den Detail-Abschnitten oben ausgewertet werden.",
            )
        st.info("🔎 Hinweis: Prüfe die Aufwärtstrendlinie regelmäßig manuell im Chart (z. B. per eingezeichneter Trendlinie), da Strategie 8 nicht mehr automatisch ausgewertet wird.")

        st.markdown("#### Parameter je Strategie")
        for key in aktive:
            with st.expander(f"Strategie: {key}", expanded=False):
                st.caption(strategie_info.get(key, "Keine Beschreibung hinterlegt."))
                if key == "atr_basiert":
                    c1, c2, c3 = st.columns(3)
                    with c1:
                        st.number_input("ATR-Ziel (Multiplikator)", min_value=1.0, max_value=10.0, value=3.0, step=0.5, key=f"strat_hub_atr_mult_{t}", help="Teilverkauf ab x ATR Gewinn.")
                    with c2:
                        st.number_input("ATR über 21-EMA (Start)", min_value=1.0, max_value=10.0, value=3.0, step=0.5, key=f"strat_hub_atr_ext_start_{t}", help="Ab x ATR über 21-EMA wird 33% verkauft.")
                    with c3:
                        st.number_input("ATR über 21-EMA (Stark)", min_value=1.0, max_value=10.0, value=4.0, step=0.5, key=f"strat_hub_atr_ext_strong_{t}", help="Ab y ATR über 21-EMA wird 50% verkauft.")
                elif key == "notbremse_verlust":
                    st.markdown(
                        """
**Strategie 2 – Notbremse nach Verlusthöhe (Kap. 6.1):**
- **Immer aktiv** und unabhängig von allen anderen Strategien.
- Bei Erreichen/Unterschreiten der Verlustschwelle folgt **Vollausstieg (100%) intraday**.
- Die Schwelle ist je Marktumfeld frei definierbar (Bärisch/Unsicher/Bullisch).
- Falls noch nicht ausgelöst, zeigt das Infosignal den **kritischen Kurs** der Notbremse.
                        """
                    )
                    c1, c2, c3 = st.columns(3)
                    with c1:
                        notbremse_verlust_schwelle_baerisch_pct = st.number_input("Notbremse Bärisch (%)", min_value=0.5, max_value=30.0, value=4.0, step=0.5, key=f"strat_hub_notbremse_bear_{t}")
                    with c2:
                        notbremse_verlust_schwelle_unsicher_pct = st.number_input("Notbremse Unsicher (%)", min_value=0.5, max_value=30.0, value=5.0, step=0.5, key=f"strat_hub_notbremse_uncertain_{t}")
                    with c3:
                        notbremse_verlust_schwelle_bullisch_pct = st.number_input("Notbremse Bullisch (%)", min_value=0.5, max_value=30.0, value=7.0, step=0.5, key=f"strat_hub_notbremse_bull_{t}")
                elif key == "gewinn_in_stufen":
                    st.markdown(
                        """
**Strategie 3 – Verkauf bei festgelegtem prozentualem Gewinn (Kap. 6.2):**
- **Nachdenkschwelle**: ab dieser P&L-Marke wird ein Info-Signal erzeugt (noch kein Verkauf).
- **Pflicht-Teilverkauf**: ab Untergrenze der Gewinnzone wird ein aktives Signal erzeugt:
      - unterhalb der oberen Gewinnzonen-Grenze: **33%** Tranche
      - ab oberer Grenze: **50%** Tranche
- **Bärenmarkt-Modus**: eigene, engere Schwellen möglich.
                        """
                    )
                    c1, c2 = st.columns(2)
                    with c1:
                        gewinn_nachdenken_schwelle_bull_pct = st.number_input("Nachdenkschwelle Bullisch/Unsicher (%)", min_value=0.0, max_value=200.0, value=15.0, step=0.5, key=f"strat_hub_gainstep_think_bull_{t}")
                        gewinn_teilverkauf_unten_bull_pct = st.number_input("Gewinnzone unten Bullisch/Unsicher (%)", min_value=0.0, max_value=200.0, value=20.0, step=0.5, key=f"strat_hub_gainstep_lo_bull_{t}")
                        gewinn_teilverkauf_oben_bull_pct = st.number_input("Gewinnzone oben Bullisch/Unsicher (%)", min_value=0.0, max_value=300.0, value=35.0, step=0.5, key=f"strat_hub_gainstep_hi_bull_{t}")
                    with c2:
                        gewinn_nachdenken_schwelle_bear_pct = st.number_input("Nachdenkschwelle Bärisch (%)", min_value=0.0, max_value=200.0, value=10.0, step=0.5, key=f"strat_hub_gainstep_think_bear_{t}")
                        gewinn_teilverkauf_unten_bear_pct = st.number_input("Gewinnzone unten Bärisch (%)", min_value=0.0, max_value=200.0, value=10.0, step=0.5, key=f"strat_hub_gainstep_lo_bear_{t}")
                        gewinn_teilverkauf_oben_bear_pct = st.number_input("Gewinnzone oben Bärisch (%)", min_value=0.0, max_value=300.0, value=15.0, step=0.5, key=f"strat_hub_gainstep_hi_bear_{t}")
                elif key == "drawdown_vom_peak":
                    st.markdown(
                        """
**Strategie 5 – Drawdown vom Peak (Kap. 6.2):**
- **Stufe 1**: Erstes Sicherungssignal bei moderatem Rücksetzer vom Peak.
- **Stufe 2**: Deutliche Reduktion bei fortgeschrittenem Drawdown.
- **Stufe 3**: Harte Reduktion; bei zusätzlichem Trendbruch (Schluss unter 21-EMA) vollständiger Ausstieg.

Die Schwellen werden monoton durchgereicht: `Stufe2 ≥ Stufe1`, `Stufe3 ≥ Stufe2`.
                        """
                    )
                    c1, c2, c3 = st.columns(3)
                    with c1:
                        st.number_input("Stufe 1 ab (%)", min_value=1.0, max_value=50.0, value=8.0, step=0.5, key=f"strat_hub_dd_stage1_{t}", help="Drawdown vom Peak, ab dem die erste Tranche ausgelöst wird.")
                        st.number_input("Tranche Stufe 1 (%)", min_value=1.0, max_value=100.0, value=25.0, step=1.0, key=f"strat_hub_dd_tranche1_{t}")
                    with c2:
                        st.number_input("Stufe 2 ab (%)", min_value=1.0, max_value=80.0, value=12.0, step=0.5, key=f"strat_hub_dd_stage2_{t}", help="Drawdown, ab dem die zweite Tranche zwingend ausgelöst wird.")
                        st.number_input("Tranche Stufe 2 (%)", min_value=1.0, max_value=100.0, value=33.0, step=1.0, key=f"strat_hub_dd_tranche2_{t}")
                    with c3:
                        st.number_input("Stufe 3 ab (%)", min_value=1.0, max_value=100.0, value=15.0, step=0.5, key=f"strat_hub_dd_stage3_{t}", help="Drawdown, ab dem die harte Reduktion greift.")
                        st.number_input("Tranche Stufe 3 ohne Trendbruch (%)", min_value=1.0, max_value=100.0, value=50.0, step=1.0, key=f"strat_hub_dd_tranche3a_{t}")
                        st.number_input("Tranche Stufe 3 mit Trendbruch (%)", min_value=1.0, max_value=100.0, value=100.0, step=1.0, key=f"strat_hub_dd_tranche3b_{t}", help="Bei zusätzlichem Schluss unter 21-EMA – Komplettausstieg.")
                elif key == "rueckkehr_pivot":
                    st.markdown(
                        """
**Strategie 14 – Rückkehr zum Ausbruchspunkt (Kap. 6.3):**
- **Sicherheitslinie 1:** Schluss unter Tief Tag 1 → Teilverkauf (bei erhöhtem Volumen größere Tranche).
- **Sicherheitslinie 2:** Schluss unter Tief Tag 0 → weiterer Teilverkauf.
- **Zeitkomponente:** Bleibt die Aktie X Handelstage in Folge unter dem Pivot → Signal wegen ausbleibender Rückeroberung.
- **Notbremse:** Bei Erreichen der Verlustschwelle Restposition sofort intraday schließen.
                        """
                    )
                    c1, c2 = st.columns(2)
                    with c1:
                        rueckkehr_tranche_stufe1_pct = st.number_input(
                            "Tranche Sicherheitslinie 1 (%)", min_value=1.0, max_value=100.0, value=33.0, step=1.0,
                            key=f"strat_hub_rueckkehr_tranche_1_{t}",
                            help="Standard-Tranche bei Schluss unter Tief Tag 1.",
                        )
                        rueckkehr_tranche_stufe1_volumen_pct = st.number_input(
                            "Tranche Sicherheitslinie 1 bei Volumen (%)", min_value=1.0, max_value=100.0, value=50.0, step=1.0,
                            key=f"strat_hub_rueckkehr_tranche_1_vol_{t}",
                            help="Erhöhte Tranche, wenn die Volumenquote die Schwelle erreicht.",
                        )
                        rueckkehr_volumen_schwelle = st.number_input(
                            "Volumenquoten-Schwelle", min_value=0.5, max_value=10.0, value=1.5, step=0.1,
                            key=f"strat_hub_rueckkehr_vol_schwelle_{t}",
                            help="Ab dieser Volumenquote (heutiges Volumen / 50-Tage-Durchschnitt) wird Tranche 1 erhöht.",
                        )
                        rueckkehr_notbremse_verlust_pct = st.number_input(
                            "Notbremse Verlust (%)", min_value=1.0, max_value=30.0, value=7.0, step=0.5,
                            key=f"strat_hub_rueckkehr_notbremse_{t}",
                            help="Bei diesem Verlust (P&L) wird die Restposition sofort intraday geschlossen.",
                        )
                    with c2:
                        rueckkehr_tranche_stufe2_pct = st.number_input(
                            "Tranche Sicherheitslinie 2 (%)", min_value=1.0, max_value=100.0, value=33.0, step=1.0,
                            key=f"strat_hub_rueckkehr_tranche_2_{t}",
                            help="Tranche bei Schluss unter Tief Tag 0.",
                        )
                        rueckkehr_pivot_tage_schwelle = int(st.number_input(
                            "Tage unter Pivot", min_value=1, max_value=60, value=10, step=1,
                            key=f"strat_hub_rueckkehr_pivot_tage_{t}",
                            help="Anzahl Handelstage in Folge unter dem Pivot, ab der die Zeitkomponente auslöst.",
                        ))
                        rueckkehr_tranche_pivot_pct = st.number_input(
                            "Tranche Zeitkomponente Pivot (%)", min_value=1.0, max_value=100.0, value=50.0, step=1.0,
                            key=f"strat_hub_rueckkehr_pivot_tranche_{t}",
                            help="Tranche, wenn die Aktie X Handelstage in Folge unter dem Pivot bleibt.",
                        )
                elif key == "einfache_verluststufen":
                    c1, c2, c3 = st.columns(3)
                    with c1:
                        verlust_stufe_1 = st.number_input("Verluststufe 1 (%)", min_value=0.5, max_value=30.0, value=3.0, step=0.5, key=f"strat_hub_loss_stage1_{t}", help="Bei diesem Verlust wird die erste Tranche (33%) verkauft.")
                    with c2:
                        verlust_stufe_2 = st.number_input("Verluststufe 2 (%)", min_value=0.5, max_value=30.0, value=5.0, step=0.5, key=f"strat_hub_loss_stage2_{t}", help="Bei diesem Verlust folgt die zweite Tranche (33%).")
                    with c3:
                        verlust_stufe_3 = st.number_input("Verluststufe 3 (%)", min_value=0.5, max_value=30.0, value=7.0, step=0.5, key=f"strat_hub_loss_stage3_{t}", help="Bei diesem Verlust wird die Restposition sofort geschlossen.")
                elif key == "einfach_halbe_position":
                    erste_haelfte_gewinn_pct = st.number_input("Gewinnmitnahme 1. Hälfte (%)", min_value=5.0, max_value=100.0, value=20.0, step=0.5, key=f"strat_hub_first_half_profit_{t}", help="Ab diesem Gewinn wird die erste Hälfte (50%) verkauft.")
                elif key == "ma_basierte_sequenz":
                    st.markdown(
                        """
**Sequenzlogik (Kap. 6.4):**
- **Punkt 1:** Teilverkauf in der Gewinnzone (Standard: 20–25%).
- **Punkt 2:** Zusatz-Tranche bei Überdehnung über 10-MA (Standard: +10%).
- **Punkt 3:** Verkauf bei Schluss unter 10-MA; bei **Pendelverhalten** um die 10-MA höhere Tranche.
- **Punkt 4:** Weitere Reduktion bei Schluss unter 21-EMA.
- **Punkt 5 (final):** Vollausstieg bei klarem Bruch der 50-MA.
                        """
                    )
                    c1, c2, c3 = st.columns(3)
                    with c1:
                        ma_seq_gewinnzone_min_pct = st.number_input("Gewinnzone min (%)", min_value=0.0, max_value=200.0, value=20.0, step=0.5, key=f"strat_hub_ma_seq_gain_min_{t}", help="Untere Grenze für Punkt 1.")
                        ma_seq_gewinnzone_tranche_pct = st.number_input("Tranche Punkt 1 (%)", min_value=1.0, max_value=100.0, value=33.0, step=1.0, key=f"strat_hub_ma_seq_gain_tranche_{t}", help="Verkaufsgröße in der Gewinnzone.")
                        ma_seq_unter_ma10_mindestgewinn_pct = st.number_input("Min. P&L für Punkt 3 (%)", min_value=-50.0, max_value=200.0, value=5.0, step=0.5, key=f"strat_hub_ma_seq_under10_minpnl_{t}", help="Punkt 3 erst ab diesem Mindestgewinn.")
                        ma_seq_pendel_lookback_tage = int(st.number_input("Pendel-Lookback (Tage)", min_value=2, max_value=20, value=5, step=1, key=f"strat_hub_ma_seq_pendel_lb_{t}", help="Anzahl Tage für den Pendel-Check um die 10-MA."))
                    with c2:
                        ma_seq_gewinnzone_max_pct = st.number_input("Gewinnzone max (%)", min_value=0.0, max_value=200.0, value=25.0, step=0.5, key=f"strat_hub_ma_seq_gain_max_{t}", help="Obere Grenze für Punkt 1.")
                        ma_seq_ueber_ma10_pct = st.number_input("Überdehnung über 10-MA (%)", min_value=0.5, max_value=100.0, value=10.0, step=0.5, key=f"strat_hub_ma_seq_over10_pct_{t}", help="Schwelle für Punkt 2.")
                        ma_seq_unter_ma10_tranche_pct = st.number_input("Tranche Punkt 3 normal (%)", min_value=1.0, max_value=100.0, value=20.0, step=1.0, key=f"strat_hub_ma_seq_under10_tranche_{t}", help="Standard-Tranche bei Schluss unter 10-MA.")
                        ma_seq_pendel_wechsel_min = int(st.number_input("Min. Pendel-Wechsel", min_value=1, max_value=10, value=3, step=1, key=f"strat_hub_ma_seq_pendel_switch_{t}", help="Ab so vielen Seitenwechseln gilt das Verhalten als pendelnd."))
                    with c3:
                        ma_seq_ueber_ma10_tranche_pct = st.number_input("Tranche Punkt 2 (%)", min_value=1.0, max_value=100.0, value=20.0, step=1.0, key=f"strat_hub_ma_seq_over10_tranche_{t}", help="Verkaufsgröße bei Überdehnung über 10-MA.")
                        ma_seq_pendel_tranche_pct = st.number_input("Tranche Punkt 3 pendelnd (%)", min_value=1.0, max_value=100.0, value=25.0, step=1.0, key=f"strat_hub_ma_seq_pendel_tranche_{t}", help="Erhöhte Tranche, wenn Punkt 3 pendelnd erkannt wird.")
                        ma_seq_unter_ma21_tranche_pct = st.number_input("Tranche Punkt 4 (%)", min_value=1.0, max_value=100.0, value=25.0, step=1.0, key=f"strat_hub_ma_seq_under21_tranche_{t}", help="Verkaufsgröße bei Schluss unter 21-EMA.")
                        ma_seq_klarer_ma50_bruch_pct = st.number_input("Klarer 50-MA-Bruch (%)", min_value=0.5, max_value=20.0, value=2.0, step=0.5, key=f"strat_hub_ma_seq_under50_clear_{t}", help="Mindestabstand unter 50-MA für Punkt 5.")
                    ma_seq_unter_ma21_mindestgewinn_pct = st.number_input("Min. P&L für Punkt 4 (%)", min_value=-50.0, max_value=200.0, value=5.0, step=0.5, key=f"strat_hub_ma_seq_under21_minpnl_{t}", help="Punkt 4 erst ab diesem Mindestgewinn.")
                elif key == "rs_linie":
                    st.markdown(
                        """
**RS-Linien-3-Stufen-Strategie (Kap. 6.4):**
- **Stufe 1 (20%)**: RS-Linie bricht den schnellen MA erstmalig.
- **Stufe 2 (30%)**: RS bleibt 3 Perioden in Folge unter dem schnellen MA.
- **Stufe 3 (50%)**: RS bricht den langsamen MA (Restverkauf).

**Automatischer Zeithorizont:**
- unter Schwelle 1: **Tag** (21/50-MA)
- zwischen Schwelle 1 und 2: **Woche** (10/25-MA)
- ab Schwelle 2: **Monat** (12/24-MA)
                        """
                    )
                    c1, c2 = st.columns(2)
                    with c1:
                        rs_pnl_tag_zu_woche = st.number_input(
                            "PnL-Schwelle Tag → Woche (%)", min_value=0.0, max_value=200.0, value=20.0, step=0.5,
                            key=f"strat_hub_rs_pnl_day_week_{t}",
                            help="Ab diesem Gewinn wechselt Strategie 18 von Tages- auf Wochensignale.",
                        )
                    with c2:
                        rs_pnl_woche_zu_monat = st.number_input(
                            "PnL-Schwelle Woche → Monat (%)", min_value=0.0, max_value=300.0, value=80.0, step=0.5,
                            key=f"strat_hub_rs_pnl_week_month_{t}",
                            help="Ab diesem Gewinn wechselt Strategie 18 von Wochen- auf Monatssignale.",
                        )
                elif key == "ma21_bruch":
                    st.markdown(
                        """
**Strategie 4 – 21-EMA-Bruch (Kap. 6.2):**
- **Aggressiv:** klarer Bruch (mind. 2% unter 21-EMA) mit erhöhtem Volumen (≥1.2) triggert 33%.
      Zusatzregel: am zweiten Tag unter 21-EMA und Tagesverlust ≤ -7% wird sofort 50% reduziert.
- **Gestaffelt:** 3-stufiges Vorgehen (Tag 1/2/3 jeweils 25% nach Regelwerk).
- **Geduldig:** erst nach bestätigtem Bruch (mind. 3 Tage unter 21-EMA) 33%.

Die Variante wird **einmalig pro Position** gespeichert und beim nächsten Öffnen des Strategien-Hubs wiederverwendet.
                        """
                    )
                    st.selectbox(
                        "Variante 21-EMA-Bruch",
                        ["gestaffelt", "aggressiv", "geduldig"],
                        index=["gestaffelt", "aggressiv", "geduldig"].index(ma21_variante),
                        key=f"strat_hub_ma21_variante_{t}",
                        help="Einmalig festlegen, wie offensiv Strategie 4 den Bruch der 21-EMA behandelt.",
                    )
                elif key == "groesster_einbruch":
                    st.markdown(
                        """
**Strategie 17 – Größter Tages-/Wocheneinbruch seit Beginn (Kap. 6.3):**
- Nur aktiv, wenn die Position bereits mindestens die definierte **Mindest-P&L-Schwelle** erreicht hat.
- **Tagesregel**: Aktueller Tagesverlust ist der größte seit Einstieg und über der Mindest-Verlustschwelle.
      - Bei normalem Volumen: **33%** Tranche (defensive Reduktion).
      - Bei hohem Volumen (Volumenfaktor ≥ Schwelle): **50%** Tranche (stärkeres Warnsignal).
- **Wochenregel**: Aktuelle Verlustwoche ist die größte seit Einstieg und Wochenvolumen liegt über dem 12-Wochen-Durchschnitt (Faktor ≥ Schwelle).
      - Signal: **66%** Tranche.

Die Strategie versucht späte Trendphasen zu schützen, wenn erstmals ungewöhnlich große Abgaben auftreten.
                        """
                    )
                    c1, c2 = st.columns(2)
                    with c1:
                        groesster_einbruch_min_pnl_pct = st.number_input(
                            "Mindest-P&L vor Aktivierung (%)", min_value=0.0, max_value=300.0, value=10.0, step=0.5,
                            key=f"strat_hub_worst_drop_min_pnl_{t}",
                            help="Strategie 17 wird erst geprüft, wenn die Position mindestens diesen Gewinn erreicht hat.",
                        )
                        groesster_einbruch_tagesvol_ratio_schwelle = st.number_input(
                            "Volumenfaktor Tagesregel", min_value=0.5, max_value=10.0, value=1.5, step=0.1,
                            key=f"strat_hub_worst_drop_day_vol_ratio_{t}",
                            help="Ab diesem Faktor (heutiges Volumen / 50-Tage-Durchschnitt) gilt die Tageswarnung als volumenbestätigt (50%-Tranche).",
                        )
                    with c2:
                        groesster_einbruch_min_tagesverlust_pct = st.number_input(
                            "Mindest-Tagesverlust (%)", min_value=0.5, max_value=30.0, value=3.0, step=0.1,
                            key=f"strat_hub_worst_drop_day_loss_{t}",
                            help="Nur wenn der größte Tagesverlust zugleich über dieser Schwelle liegt, wird ein Signal ausgelöst.",
                        )
                        groesster_einbruch_wochenvol_ratio_schwelle = st.number_input(
                            "Volumenfaktor Wochenregel", min_value=0.5, max_value=10.0, value=1.3, step=0.1,
                            key=f"strat_hub_worst_drop_week_vol_ratio_{t}",
                            help="Ab diesem Faktor (aktuelles Wochenvolumen / 12-Wochen-Durchschnitt) gilt die Wochenwarnung als bestätigt.",
                        )
                elif key == "stau_tage":
                    st.markdown(
                        """
**Strategie 13 – Stau-Tage (Kap. 6.2):**
- Prüft die letzten **N Sessions** auf verdeckte Distribution.
- Ein Tag gilt als Stau-Tag, wenn:
      - **kaum Fortschritt**: \|Close-Open\| in % < Schwelle
      - **hohes Volumen**: Tagesvolumen / Referenzvolumen ≥ Schwelle
- Ab **mindestens X Stau-Tagen** im Fenster wird ein Signal aktiv.
- Die Tranche ist höher, wenn die Aktie noch **nahe am Hoch** notiert (geringer Drawdown).
- Als Stopp-Marke wird das **Tief des schwächsten Stau-Tags** genutzt.
                        """
                    )
                    c1, c2, c3 = st.columns(3)
                    with c1:
                        stau_fenster_tage = int(st.number_input("Fenster (Sessions)", min_value=5, max_value=30, value=10, step=1, key=f"strat_hub_stall_window_{t}"))
                        stau_max_tagesveraenderung_pct = st.number_input("Max. Tagesveränderung (%)", min_value=0.1, max_value=5.0, value=1.0, step=0.1, key=f"strat_hub_stall_max_day_change_{t}")
                        stau_min_tage = int(st.number_input("Min. Stau-Tage", min_value=1, max_value=10, value=2, step=1, key=f"strat_hub_stall_min_days_{t}"))
                    with c2:
                        stau_volumen_lookback_tage = int(st.number_input("Volumen-Lookback (Tage)", min_value=10, max_value=200, value=50, step=1, key=f"strat_hub_stall_vol_lookback_{t}"))
                        stau_min_vol_ratio = st.number_input("Min. Volumenfaktor", min_value=0.8, max_value=5.0, value=1.3, step=0.1, key=f"strat_hub_stall_min_vol_ratio_{t}")
                        stau_nahe_hoch_drawdown_max_pct = st.number_input("Nahe-Hoch Drawdown max (%)", min_value=0.5, max_value=20.0, value=5.0, step=0.5, key=f"strat_hub_stall_near_high_dd_{t}")
                    with c3:
                        stau_tranche_nahe_hoch_pct = st.number_input("Tranche nahe Hoch (%)", min_value=1.0, max_value=100.0, value=33.0, step=1.0, key=f"strat_hub_stall_tranche_near_high_{t}")
                        stau_tranche_standard_pct = st.number_input("Standard-Tranche (%)", min_value=1.0, max_value=100.0, value=20.0, step=1.0, key=f"strat_hub_stall_tranche_standard_{t}")
                elif key == "verlusttage_haeufung":
                    st.markdown(
                        """
**Strategie 7 – Häufung Verlusttage / Verhältnis Auf-/Abwärtstage (Kap. 6.2):**
- Nur aktiv bei Gewinnerpositionen (P&L > 0).
- **Signal A (Sequenz):** Mehrere tiefere Schlusskurse in Folge (Standard 3) + erhöhte Volumenquote (Ø letzte 3 Tage / Ø Lookback-Volumen).
- **Signal B (Persönlichkeitswechsel):** Im Up/Down-Fenster überwiegen Abwärtstage gegenüber Aufwärtstagen mit Mindestdifferenz.
- Beide Signale verwenden standardmäßig **25%** Tranche und setzen die nächste Marke auf lokale Tiefs.
                        """
                    )
                    c1, c2, c3 = st.columns(3)
                    with c1:
                        verlusttage_min_tiefere_schlusskurse_in_folge = int(st.number_input("Min. tiefere Schlusskurse in Folge", min_value=2, max_value=5, value=3, step=1, key=f"strat_hub_losscluster_min_seq_{t}"))
                        verlusttage_tief_marker_lookback_tage = int(st.number_input("Tief-Marker Lookback", min_value=2, max_value=20, value=5, step=1, key=f"strat_hub_losscluster_low_marker_lb_{t}"))
                    with c2:
                        verlusttage_volumen_lookback_tage = int(st.number_input("Volumen-Lookback (Tage)", min_value=10, max_value=200, value=50, step=1, key=f"strat_hub_losscluster_vol_lb_{t}"))
                        verlusttage_volumen_ratio_min = st.number_input("Min. Volumenquote (3T / LB)", min_value=0.8, max_value=3.0, value=1.1, step=0.05, key=f"strat_hub_losscluster_vol_ratio_{t}")
                    with c3:
                        verlusttage_updown_fenster_tage = int(st.number_input("Up/Down-Fenster (Tage)", min_value=5, max_value=40, value=15, step=1, key=f"strat_hub_losscluster_ud_window_{t}"))
                        verlusttage_updown_diff_min = int(st.number_input("Min. Abwärts-Überhang (Tage)", min_value=1, max_value=10, value=3, step=1, key=f"strat_hub_losscluster_ud_diff_{t}"))
                        verlusttage_tranche_pct = st.number_input("Tranche je Signal (%)", min_value=1.0, max_value=100.0, value=25.0, step=1.0, key=f"strat_hub_losscluster_tranche_{t}")
                elif key == "ma_abstand":
                    st.markdown(
                        """
**Strategie 6 – Abstand zu gleitenden Durchschnitten (Kap. 6.2):**
- Nur aktiv, wenn die Position im Gewinn ist (**P&L > 0**).
- Es werden vier Überdehnungen gemessen: Abstand zum **10-MA / 21-EMA / 50-MA / 200-MA** in Prozent.
- Signalstufen (Standard):
      - **10-MA ab 10%** → kurzfristig überhitzt, **25%** Tranche
      - **21-EMA ab 15%** → klare Überdehnung, **33%** Tranche
      - **50-MA ab 25%** → Spätphasen-Signal, **33%** Tranche
      - **200-MA ab 70%** → Klimaxzone, **50%** Tranche
      - **200-MA ab 100%** → Extrem-Klimax, **100%** Tranche
- Nächste Marke: jeweiliger MA (bei 200-MA-Klimax als engere Kontrolllinie die 50-MA).
                        """
                    )
                    c1, c2, c3 = st.columns(3)
                    with c1:
                        ma_abstand_schwelle_ma10_pct = st.number_input("Schwelle 10-MA (%)", min_value=1.0, max_value=50.0, value=10.0, step=0.5, key=f"strat_hub_ma_dist_th10_{t}")
                        ma_abstand_schwelle_ma21_pct = st.number_input("Schwelle 21-EMA (%)", min_value=1.0, max_value=80.0, value=15.0, step=0.5, key=f"strat_hub_ma_dist_th21_{t}")
                        ma_abstand_schwelle_ma50_pct = st.number_input("Schwelle 50-MA (%)", min_value=1.0, max_value=120.0, value=25.0, step=0.5, key=f"strat_hub_ma_dist_th50_{t}")
                    with c2:
                        ma_abstand_schwelle_ma200_pct = st.number_input("Klimax-Schwelle 200-MA (%)", min_value=10.0, max_value=200.0, value=70.0, step=1.0, key=f"strat_hub_ma_dist_th200_{t}")
                        ma_abstand_klimax_ma200_vollausstieg_pct = st.number_input("Vollausstieg ab 200-MA (%)", min_value=20.0, max_value=300.0, value=100.0, step=1.0, key=f"strat_hub_ma_dist_th200_full_{t}")
                    with c3:
                        ma_abstand_tranche_ma10_pct = st.number_input("Tranche 10-MA (%)", min_value=1.0, max_value=100.0, value=25.0, step=1.0, key=f"strat_hub_ma_dist_tr10_{t}")
                        ma_abstand_tranche_ma21_pct = st.number_input("Tranche 21-EMA (%)", min_value=1.0, max_value=100.0, value=33.0, step=1.0, key=f"strat_hub_ma_dist_tr21_{t}")
                        ma_abstand_tranche_ma50_pct = st.number_input("Tranche 50-MA (%)", min_value=1.0, max_value=100.0, value=33.0, step=1.0, key=f"strat_hub_ma_dist_tr50_{t}")
                        ma_abstand_tranche_ma200_basis_pct = st.number_input("Tranche 200-MA Basis (%)", min_value=1.0, max_value=100.0, value=50.0, step=1.0, key=f"strat_hub_ma_dist_tr200_{t}")
                elif key == "downside_reversal":
                    st.markdown(
                        """
**Strategie 12 – Downside Reversal (Kap. 6.2):**
- Nur aktiv, wenn die Position im Gewinn ist.
- **Variante 1 (stark):** neues Hoch + Schluss im unteren Tagesdrittel + erhöhtes Volumen.
- **Variante 2 (mittel):** weite Umkehrkerze + Schluss im unteren Drittel + erhöhtes Volumen.
- **Variante 3 (Warnstufe):** weite Kerze + Schluss unter Spannenmitte.
- Stopp-/Entwarnungsmarke ist das Hoch der Signalkerze.
                        """
                    )
                    c1, c2, c3 = st.columns(3)
                    with c1:
                        downside_kerzenweite_lookback_tage = int(st.number_input("Kerzenweiten-Lookback", min_value=5, max_value=60, value=10, step=1, key=f"strat_hub_downside_candle_lb_{t}"))
                        downside_neues_hoch_lookback_tage = int(st.number_input("Neues-Hoch-Lookback", min_value=10, max_value=120, value=30, step=1, key=f"strat_hub_downside_high_lb_{t}"))
                        downside_schluss_unteres_drittel_faktor = st.number_input("Schluss im unteren 1/x", min_value=2.0, max_value=6.0, value=3.0, step=0.1, key=f"strat_hub_downside_lower_third_factor_{t}")
                    with c2:
                        downside_volumen_lookback_tage = int(st.number_input("Volumen-Lookback", min_value=20, max_value=200, value=50, step=1, key=f"strat_hub_downside_vol_lb_{t}"))
                        downside_volumen_ratio_min = st.number_input("Min. Volumenquote", min_value=0.8, max_value=5.0, value=1.2, step=0.1, key=f"strat_hub_downside_vol_ratio_{t}")
                        downside_weite_kerze_faktor = st.number_input("Weite-Kerze-Faktor", min_value=1.0, max_value=4.0, value=1.5, step=0.1, key=f"strat_hub_downside_wide_factor_{t}")
                    with c3:
                        downside_tranche_neues_hoch_pct = st.number_input("Tranche Variante 1 (%)", min_value=1.0, max_value=100.0, value=33.0, step=1.0, key=f"strat_hub_downside_tranche_high_{t}")
                        downside_tranche_weite_umkehr_pct = st.number_input("Tranche Variante 2 (%)", min_value=1.0, max_value=100.0, value=20.0, step=1.0, key=f"strat_hub_downside_tranche_wide_{t}")
                        downside_tranche_warnstufe_pct = st.number_input("Tranche Warnstufe (%)", min_value=1.0, max_value=100.0, value=15.0, step=1.0, key=f"strat_hub_downside_tranche_warn_{t}")
                elif key == "split_anstieg":
                    st.markdown(
                        """
**Strategie 10 – Preisanstieg nach Split (Kap. 6.2):**
- Die Regel ist nur kurz nach einem Split relevant (**erste 14 Tage**).
- Gemessen wird der prozentuale Anstieg vom **Schlusskurs am Split-Tag** zum aktuellen Schluss.
- Signalstufen:
      - ab **+25%**: Teilverkauf (Standard **33%**)
      - ab **+50%**: stärkere Reduktion (Standard **50%**)
- Datumsquelle:
      1. bevorzugt automatische Yahoo-Split-Historie,
      2. falls nicht verfügbar/ungeeignet: manuelle Datumseingabe.
                        """
                    )
                    c1, c2 = st.columns(2)
                    with c1:
                        st.number_input("Yahoo Split-Lookback (Tage)", min_value=14, max_value=365, value=30, step=1, key=f"strat_hub_split_lookback_{t}", help="So weit zurück wird nach dem letzten Split-Ereignis in Yahoo gesucht.")
                        st.number_input("Max. Tage seit Split", min_value=7, max_value=30, value=14, step=1, key=f"strat_hub_split_age_window_{t}", help="Nur Splits innerhalb dieses Fensters werden für Strategie 10 automatisch berücksichtigt.")
                    with c2:
                        st.number_input("Signal-Schwelle (%)", min_value=5.0, max_value=100.0, value=25.0, step=0.5, key=f"strat_hub_split_signal_threshold_{t}", help="Ab diesem Kursanstieg seit Split wird ein Signal ausgelöst.")
                        st.number_input("Starke Tranche ab (%)", min_value=10.0, max_value=200.0, value=50.0, step=0.5, key=f"strat_hub_split_strong_threshold_{t}", help="Ab diesem Anstieg wird die stärkere Tranche verwendet.")
                    if split_auto_msg is not None:
                        level, msg = split_auto_msg
                        if level == "success":
                            st.success(msg)
                        elif level == "warning":
                            st.warning(msg)
                        else:
                            st.info(msg)
                    manual_default = split_auto_datum.date() if split_auto_datum is not None else None
                    st.date_input(
                        "Manuelles Split-Datum (Fallback)",
                        value=manual_default,
                        key=f"strat_hub_split_manual_date_{t}",
                        help="Falls Yahoo keinen passenden Split liefert, hier Split-Datum manuell setzen.",
                    )
                    if split_datum is not None:
                        st.caption(f"Aktives Split-Datum für Strategie 10: {pd.Timestamp(split_datum).date().isoformat()}")
                else:
                    st.caption("Für diese Strategie sind aktuell keine zusätzlichen Parameter verfügbar.")


def _tab_verkaufsentscheidung():
    if not _render_private_gate("🔐 Verkaufs-Entscheidung"):
        return
    inject_workspace_css()
    _sell_decision_ui_css()
    st.markdown("### 🧭 Verkaufs-Entscheidung")
    st.caption("Regelbasierter Verkaufsbereich für Live-Monitor, Portfolio-Ranking und spätere Post-Mortems.")
    tabs = st.tabs(["📡 Live-Monitor", "🏁 Portfolio-Ranking", "🧠 Strategien", "🧾 Post-Mortem"])
    with tabs[0]:
        _render_sell_decision_live_monitor()
    with tabs[1]:
        _render_sell_decision_portfolio_ranking()
    with tabs[2]:
        _render_sell_strategy_hub()
    with tabs[3]:
        _render_sell_decision_post_mortem()


# ═══════════════════════════════════════════════════════
# TAB 4: NACH DEM KAUF (Book Ch.5.2 + 5.3)
# ═══════════════════════════════════════════════════════

def _tab_nach_kauf():
    _init_workspace_state()
    st.markdown("### 🎯 Nach dem Kauf")
    st.caption(
        "Schneller Gesundheits-Check kurz nach dem Einstieg — nutzt dieselbe Engine "
        "wie Live-Monitor und Verkaufsentscheidung → Portfolio-Ranking. Gib Ticker, "
        "Kaufpreis und Kaufdatum ein; gespeicherte Positionen werden vorausgefüllt."
    )

    private_ok = _is_private_unlocked()
    saved_positions = st.session_state.get("positions", []) if private_ok else []
    if not private_ok:
        st.info("Dein persönliches Depot ist gesperrt. Du kannst diesen Tab manuell nutzen oder den privaten Bereich entsperren, um gespeicherte Positionen zu laden und zu speichern.")
    ticker = _render_ticker_picker("nachkauf_ticker", "Ticker oder Firmenname suchen", "NVDA oder Nvidia", show_quick=False)
    if not ticker:
        return

    saved = next((p for p in saved_positions if p.get("ticker") == ticker), None)
    symbol_metrics = _portfolio_symbol_metrics(ticker) or {}
    market_currency = str(symbol_metrics.get("currency", "USD") or "USD").upper()
    latest_price = _safe_float(symbol_metrics.get("price"), np.nan)

    bc1, bc2, bc3, bc4 = st.columns(4)
    with bc1:
        currency_default = "EUR"
        if saved and saved.get("currency") == "USD":
            currency_default = "USD"
        currency = st.selectbox("Währung", ["EUR", "USD"], index=0 if currency_default == "EUR" else 1, key="nk_curr")
    with bc2:
        if not np.isnan(latest_price) and latest_price > 0:
            if currency == "EUR" and market_currency != "EUR":
                fallback_price = _usd_to_eur(latest_price)
            else:
                fallback_price = latest_price
        else:
            fallback_price = 100.0
        default_buy = round(float(saved.get("buy_price", fallback_price * 0.95)) if saved else float(fallback_price * 0.95), 2)
        label = "Kaufkurs (€)" if currency == "EUR" else "Kaufkurs ($)"
        buy_price_input = st.number_input(label, min_value=0.01, value=default_buy, step=0.01, key="nk_price")
    with bc3:
        if saved and saved.get("buy_date"):
            try:
                saved_date = pd.Timestamp(saved["buy_date"]).date()
            except Exception:
                saved_date = datetime.now(timezone.utc).date()
        else:
            saved_date = datetime.now(timezone.utc).date()
        buy_date = st.date_input("Kaufdatum", value=saved_date, key="nk_date")
    with bc4:
        note_default = saved.get("note", "") if saved else ""
        note = st.text_input("Notiz", value=note_default, key="nk_note")

    if st.button("💾 Position speichern / aktualisieren", width="stretch", disabled=not private_ok):
        buy_price_eur, _ = _price_to_eur(float(buy_price_input), currency, buy_date)
        _upsert_position({
            "ticker": ticker,
            "buy_price": float(buy_price_input),
            "buy_price_eur": float(buy_price_eur),
            "buy_date": str(buy_date),
            "currency": currency,
            "note": note,
        })
        st.success(f"Position {ticker} gespeichert.")
    if not private_ok:
        st.caption("Speichern ist gesperrt, bis dein privater Bereich entsperrt ist.")

    if not (buy_price_input and buy_price_input > 0 and buy_date):
        return

    metric_buy_price = _sell_monitor_buy_price_for_market(
        ticker, float(buy_price_input), currency, buy_date, market_currency,
    )
    with st.spinner(f"Lade {ticker} und werte mit der Verkaufs-Engine aus …"):
        try:
            metrics_payload = load_sell_decision_metrics(
                ticker, buy_date, metric_buy_price, 1.0,
                benchmark_ticker="SPY", currency=market_currency,
                cache_buster=int(st.session_state.get("nk_cache_buster", 0) or 0),
            )
        except Exception as exc:
            logger.debug("Nach-Kauf engine load failed for %s: %s", ticker, exc)
            st.error("Yahoo-Daten konnten nicht geladen werden. Prüfe Ticker, Kaufdatum und Internetverbindung.")
            return
    if not metrics_payload.get("ok"):
        st.error(_sell_decision_user_error(metrics_payload.get("error"), ticker))
        return

    metrics = metrics_payload.get("metrics", {}) or {}
    health = compute_sell_health_score(metrics_payload, {})
    auto_strength, auto_warning, auto_reasons = _sell_monitor_auto_checkbox_data(metrics_payload)

    current_price = _safe_float(metrics.get("current_price"), np.nan)
    pnl_pct = _safe_float(metrics.get("pnl_pct"), np.nan)
    days_held = max((datetime.now(timezone.utc).date() - buy_date).days, 0)
    money_symbol = "€" if market_currency == "EUR" else "$" if market_currency == "USD" else market_currency

    status = str(health.get("status") or "—")
    status_tone = {"Halten": "hero-good", "Beobachten": "hero-warn", "Verkaufen": "hero-bad"}.get(status, "hero-warn")
    status_text = {
        "Halten": "🟢 Gesundes Verhalten — halten",
        "Beobachten": "🟡 Beobachten — gemischte Signale",
        "Verkaufen": "🔴 Risiko — Position prüfen / Tranche erwägen",
    }.get(status, status)
    eur_note = ""
    if currency == "EUR" and market_currency != "EUR" and metric_buy_price:
        eur_note = f' · €{buy_price_input:,.2f} → {money_symbol}{metric_buy_price:,.2f}'
    current_text = f"{money_symbol}{current_price:,.2f}" if not np.isnan(current_price) else "—"
    pnl_text = f"{'+' if pnl_pct >= 0 else ''}{pnl_pct:.1f}%" if not np.isnan(pnl_pct) else "—"

    st.markdown(
        f'<div class="summary-hero">'
        f'<div class="hero-title">{html.escape(ticker)} seit Kauf</div>'
        f'<div class="hero-subtitle">Kauf: {money_symbol}{metric_buy_price:,.2f} am {buy_date.strftime("%d.%m.%Y")}{eur_note}</div>'
        f'<div class="hero-action {status_tone}">Aktuell {current_text} · {pnl_text} · {days_held} Tage · {status_text}</div>'
        f'</div>',
        unsafe_allow_html=True,
    )

    kpi_cards = [
        {"title": "P&L", "value": _fmt_pct(metrics.get("pnl_pct")), "detail": f"{days_held} Tage seit Kauf"},
        {"title": "Drawdown", "value": _fmt_pct(metrics.get("drawdown_from_high_since_buy_pct")), "detail": "vom Hoch nach Kauf"},
        {"title": "Distanz 21-EMA", "value": _fmt_pct(_sell_monitor_distance(current_price, metrics.get("ema21"))), "detail": _sell_monitor_fmt_money(metrics.get("ema21"), market_currency)},
        {"title": "Distanz 50-MA", "value": _fmt_pct(_sell_monitor_distance(current_price, metrics.get("sma50"))), "detail": _sell_monitor_fmt_money(metrics.get("sma50"), market_currency)},
    ]
    _render_change_cards(kpi_cards)

    pos_signs: list[tuple[str, str]] = []
    for key, label in SELL_MONITOR_STRENGTH_SIGNALS:
        if bool(auto_strength.get(key, False)):
            pos_signs.append((label, str(auto_reasons.get(key, "")).strip()))
    neg_signs: list[tuple[str, str]] = []
    if not np.isnan(pnl_pct) and pnl_pct < -7:
        neg_signs.append(("⚠ Stop-Loss erreicht (>7% Verlust)", f"P&L {pnl_pct:+.1f}% — sofort prüfen"))
    for key, label in SELL_MONITOR_WARNING_SIGNALS:
        if bool(auto_warning.get(key, False)):
            neg_signs.append((label, str(auto_reasons.get(key, "")).strip()))

    pc1, pc2 = st.columns(2)
    with pc1:
        st.markdown('<div class="info-card" style="border-color:#22c55e30;"><div class="card-label" style="color:#22c55e;">Positive Zeichen</div>', unsafe_allow_html=True)
        if pos_signs:
            for nm, dt in pos_signs:
                detail_html = f'<div style="font-size:.72rem;color:#64748b;">{html.escape(dt)}</div>' if dt else ""
                st.markdown(f'<div style="padding:4px 0;border-bottom:1px solid #e3e8f0;"><div style="font-size:.84rem;color:#22c55e;">{html.escape(nm)}</div>{detail_html}</div>', unsafe_allow_html=True)
        else:
            st.markdown('<div style="color:#4a5568;font-size:.85rem;">Keine positiven Zeichen</div>', unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with pc2:
        st.markdown('<div class="info-card" style="border-color:#ef444430;"><div class="card-label" style="color:#ef4444;">Warnzeichen</div>', unsafe_allow_html=True)
        if neg_signs:
            for nm, dt in neg_signs:
                detail_html = f'<div style="font-size:.72rem;color:#64748b;">{html.escape(dt)}</div>' if dt else ""
                st.markdown(f'<div style="padding:4px 0;border-bottom:1px solid #e3e8f0;"><div style="font-size:.84rem;color:#ef4444;">{html.escape(nm)}</div>{detail_html}</div>', unsafe_allow_html=True)
        else:
            st.markdown('<div style="color:#4a5568;font-size:.85rem;">Keine Warnzeichen</div>', unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)

    score = _safe_float(health.get("health_score"), np.nan)
    rs_trend = str(health.get("rs_trend") or "—")
    verdict_color = {"Halten": "#22c55e", "Beobachten": "#f59e0b", "Verkaufen": "#ef4444"}.get(status, "#64748b")
    score_text = f"{score:.0f}/100" if not np.isnan(score) else "—"
    st.markdown(
        f'<div class="info-card"><div class="card-label">Nach-Kauf-Bewertung (Engine-Health-Score)</div>'
        f'<div style="font-size:1rem;font-weight:700;color:{verdict_color};">{html.escape(status_text)}</div>'
        f'<div class="mini-help">Health-Score {score_text} · RS-Trend {html.escape(rs_trend)} · {len(pos_signs)} positive · {len(neg_signs)} warnende Signale</div>'
        f'</div>',
        unsafe_allow_html=True,
    )
    st.caption("Für Tranchen-Empfehlung, manuelle Overrides und Setup wechsle in „Verkaufs-Entscheidung → Live-Monitor“.")


def _tab_sektoranalyse():
    """Tab 2: Sector performance ranking table."""
    st.markdown("### 🏭 Sektoranalyse — Performance-Ranking")
    st.caption("S&P 500 Sektor-ETFs nach Tagesperformance oder wöchentlichem Durchschnitt. Bester Sektor steht oben.")

    with st.spinner("Lade Sektor-Daten …"):
        sector_closes = load_sector_data()

    if sector_closes is None or len(sector_closes) < 5:
        st.error("Sektor-Daten konnten nicht geladen werden.")
        return

    table_periods = 3 if _is_mobile_client() else 15
    chart_periods = 15
    vc1, vc2 = st.columns([1, 1])
    with vc1:
        view_mode = st.radio("Zeitraum", ["Tagesansicht", "Wochenansicht"], index=0, horizontal=True, label_visibility="collapsed")
    is_weekly = view_mode == "Wochenansicht"
    mode_key = "weekly" if is_weekly else "daily"
    period_word = "Wochen" if is_weekly else "Handelstage"
    value_label = "% Wochenschnitt" if is_weekly else "% Tagesgewinn"
    with vc2:
        table_view = st.radio(
            "Tabellenwert",
            [value_label, "Platz Ranking"],
            index=1,
            horizontal=True,
            label_visibility="collapsed",
        )

    perf_table, rank_table, latest_ranked = build_sector_tables(sector_closes, mode=mode_key, n_periods=table_periods)

    if perf_table is None or rank_table is None or latest_ranked is None:
        st.warning("Nicht genug Daten für die Auswertung.")
        return

    # Determine the last trading date from the data
    last_date = sector_closes.index[-1].strftime("%d.%m.%Y")
    # UTC, da sector_closes-Index ebenfalls tz-naive UTC ist; sonst meldet ein
    # Server in einer Zeitzone vor UTC fälschlich „letzter Handelstag" obwohl
    # gleicher Tag.
    today = datetime.now(timezone.utc).strftime("%d.%m.%Y")
    date_note = f"Stand: {last_date}" + ("" if last_date == today else " (letzter Handelstag)")
    st.caption(date_note)

    # ── TOP 3 / BOTTOM 3 summary ──
    top3 = latest_ranked.head(3)
    bot3 = latest_ranked.tail(3)

    tc1, tc2 = st.columns(2)
    with tc1:
        st.markdown('<div class="card-label">🏆 TOP 3 SEKTOREN</div>', unsafe_allow_html=True)
        for i, (name, val) in enumerate(top3.items()):
            medal = ["🥇", "🥈", "🥉"][i]
            c = "#22c55e" if val > 0 else "#ef4444"
            st.markdown(f'<div style="display:flex;justify-content:space-between;padding:4px 0;border-bottom:1px solid #e3e8f0;">'
                        f'<span style="font-size:.85rem;color:#0d1626;">{medal} {name}</span>'
                        f'<span style="font-size:.85rem;font-weight:700;color:{c};">{val:+.2f}%</span></div>',
                        unsafe_allow_html=True)
    with tc2:
        st.markdown('<div class="card-label">📉 BOTTOM 3 SEKTOREN</div>', unsafe_allow_html=True)
        for name, val in bot3.items():
            c = "#22c55e" if val > 0 else "#ef4444"
            st.markdown(f'<div style="display:flex;justify-content:space-between;padding:4px 0;border-bottom:1px solid #e3e8f0;">'
                        f'<span style="font-size:.85rem;color:#0d1626;">{name}</span>'
                        f'<span style="font-size:.85rem;font-weight:700;color:{c};">{val:+.2f}%</span></div>',
                        unsafe_allow_html=True)

    st.markdown("")

    # ── FULL TABLE with color coding ──
    st.markdown(
        f'<div class="card-label">PERFORMANCE-TABELLE ({table_view.upper()} · letzte {table_periods} {period_word} · jüngste Periode links)</div>',
        unsafe_allow_html=True,
    )

    # Style the table: green for positive, red for negative
    def _color_cell(val):
        if pd.isna(val): return ""
        if val > 1.5: return "background-color: #22c55e30; color: #22c55e; font-weight: 600"
        if val > 0: return "background-color: #22c55e15; color: #22c55e"
        if val < -1.5: return "background-color: #ef444430; color: #ef4444; font-weight: 600"
        if val < 0: return "background-color: #ef444415; color: #ef4444"
        return "color: #94a3b8"

    def _rank_cell(val):
        if pd.isna(val): return ""
        if val <= 3: return "background-color: #22c55e20; color: #15803d; font-weight: 700"
        if val >= 9: return "background-color: #ef444420; color: #b91c1c; font-weight: 700"
        return "color: #475569"

    table = perf_table if table_view == value_label else rank_table
    if table_view == value_label:
        styled = table.style.map(_color_cell).format("{:+.2f}%", na_rep="—")
    else:
        styled = table.style.map(_rank_cell).format("#{:.0f}", na_rep="—")

    st.dataframe(styled, width="stretch", height=min(500, 40 + len(table) * 38))

    # ── RANKING HISTORY ──
    st.markdown("")
    chart_label = "WOCHENSCHNITT" if is_weekly else "TAGESRANKING"
    st.markdown(f'<div class="card-label">RANKING-VERLAUF ({chart_label} · Platz 1 = bester Sektor)</div>', unsafe_allow_html=True)

    pct_all = _sector_period_returns(sector_closes, mode=mode_key)
    pct_all = pct_all.dropna(how="all").tail(chart_periods)
    rank_all = pct_all.rank(axis=1, ascending=False, method="min")

    col_rename = {etf: f"{name} ({etf})" for etf, name in SECTOR_ETFS.items()}
    rank_all.columns = [col_rename.get(c, c) for c in rank_all.columns]
    sector_options = [col for col in latest_ranked.index if col in rank_all.columns]
    default_chart_sectors = [sector for sector in latest_ranked.head(3).index if sector in sector_options]

    fig = go.Figure()
    colors_cycle = ["#06b6d4", "#ef4444", "#22c55e", "#f59e0b", "#a855f7", "#f97316",
                    "#3b82f6", "#ec4899", "#84cc16", "#64748b", "#14b8a6"]
    for i, col in enumerate(sector_options):
        fig.add_trace(go.Scatter(
            x=_x(rank_all.index), y=_y(rank_all[col]),
            name=col.split(" (")[0],  # short name
            line=dict(color=colors_cycle[i % len(colors_cycle)], width=2.2),
            mode="lines+markers", marker=dict(size=4),
            visible=True if col in default_chart_sectors else "legendonly",
        ))
    fig.update_layout(
        template="plotly_white", paper_bgcolor=CHART_COLORS["bg"], plot_bgcolor="rgba(248,250,252,0)",
        margin=dict(l=0, r=0, t=10, b=34), height=380,
        yaxis=dict(autorange="reversed", gridcolor="rgba(100,116,139,0.12)", tickfont=dict(size=9, color="#64748b"),
                   title="Rang", title_font=dict(size=9, color="#64748b"), dtick=1),
        xaxis=dict(gridcolor="rgba(100,116,139,0.12)", tickfont=dict(size=9, color="#64748b")),
        legend=dict(orientation="h", yanchor="top", y=-0.15, xanchor="left", x=0,
                    font=dict(size=8, color="#64748b")),
        hovermode="x unified",
    )
    st.plotly_chart(fig, width="stretch", config={"displayModeBar": False})


def _render_market_dashboard_header(available):
    """Render dashboard header and return selected data key plus chart window."""
    label_to_key = {
        "S&P 500": "S&P 500",
        "Nasdaq": "Nasdaq Composite",
        "Russell 2000": "Russell 2000",
    }
    display_options = [label for label, key in label_to_key.items() if key in available]
    if not display_options:
        display_options = list(available)
        label_to_key = {key: key for key in available}

    left, right = st.columns([1.4, 1], vertical_alignment="center")
    with left:
        st.markdown(
            '<div class="eyebrow">Regelbasiertes Markt-Dashboard</div>'
            '<div class="h-title">Börse ohne Bauchgefühl</div>',
            unsafe_allow_html=True,
        )
    with right:
        control_cols = st.columns([1.35, 1])
        with control_cols[0]:
            try:
                selected_label = st.segmented_control(
                    "Index",
                    display_options,
                    default=display_options[0],
                    label_visibility="collapsed",
                    key="market_index_segmented",
                )
            except AttributeError:
                selected_label = st.radio(
                    "Index",
                    display_options,
                    horizontal=True,
                    label_visibility="collapsed",
                    key="market_index_radio",
                )
        with control_cols[1]:
            period_options = {"30 Tage": 30, "90 Tage": 90, "180 Tage": 180, "1 Jahr": 252}
            period_label = st.selectbox(
                "Zeitraum",
                list(period_options.keys()),
                index=1,
                label_visibility="collapsed",
                key="market_period_select",
            )
    st.markdown('<div class="dashboard-header-line"></div>', unsafe_allow_html=True)
    return label_to_key.get(selected_label, selected_label), period_options[period_label]

def _parse_refresh_date(value):
    if not value:
        return None
    parsed = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(parsed):
        return None
    return parsed.date()


def _deep_analysis_cache_state(breadth_last, benchmark_last, refresh_date=None) -> str:
    try:
        breadth_date = pd.Timestamp(breadth_last).date()
        benchmark_date = pd.Timestamp(benchmark_last).date()
    except Exception:
        return "unknown"
    if breadth_date >= benchmark_date:
        return "current"
    if refresh_date is not None:
        try:
            refresh_dt = pd.Timestamp(refresh_date).date()
        except Exception:
            refresh_dt = None
        if refresh_dt is not None and refresh_dt >= benchmark_date:
            return "last_available"
    return "stale"


def _tab_marktanalyse(compact: bool = False):
    """Marktanalyse mit optional kompakter Dashboard-Ansicht."""
    _init_workspace_state()
    with st.spinner("Lade Marktdaten …"):
        data = load_market_data()
    if not data:
        st.error("Keine Marktdaten.")
        return

    # Last-resort UI guard: keep S&P selectable even if cached market load missed it.
    if "S&P 500" not in data:
        try:
            end = datetime.now()
            start = end - timedelta(days=400)
            for sym in ["^GSPC", "^SPX", "SPY"]:
                sp_df = _dl(sym, start, end)
                if sp_df is not None and len(sp_df) > 20:
                    data["S&P 500"] = sp_df
                    break
        except Exception as exc:
            logger.warning("S&P fallback in _tab_marktanalyse failed: %s", exc)

    available = [i for i in ["S&P 500", "Nasdaq Composite", "Russell 2000"] if i in data]
    if not available:
        st.error("Keine Index-Daten.")
        return

    selected, sd = _render_market_dashboard_header(available)

    df = add_indicators(data[selected].copy())
    df = detect_distribution_days(df)
    df = compute_ampel(df)
    benchmark_df = data["S&P 500"] if "S&P 500" in data else data[selected]
    vix_df = analyze_vix(data["VIX"].copy()) if "VIX" in data else None
    vixy_df = analyze_vixy(data["VIXY"].copy()) if "VIXY" in data else None
    vol_dashboard = build_volatility_dashboard(benchmark_df, vix_df, vixy_df)
    vol_summary = summarize_volatility_state(vol_dashboard)
    vol_latest = vol_dashboard.iloc[-1] if vol_dashboard is not None and len(vol_dashboard) else pd.Series(dtype=float)
    L = df.iloc[-1]
    pct = L["Pct_Change"] if not np.isnan(L["Pct_Change"]) else 0.0

    rot, sd2, sp = detect_sector_rotation(data)
    div_r = detect_intermarket_divergence(data)
    ep = "RSP (Equal-Weight S&P)" if "S&P" in selected else "QQEW (Equal-Weight Nasdaq)"
    es = "QQEW (Equal-Weight Nasdaq)" if "S&P" in selected else "RSP (Equal-Weight S&P)"
    breadth_primary = compute_breadth_mode(data[ep].copy()) if ep in data else None
    breadth_secondary = compute_breadth_mode(data[es].copy()) if es in data else None
    breadth_label = breadth_primary.iloc[-1]["Breadth_Mode"].capitalize() if breadth_primary is not None and len(breadth_primary) else "—"

    # Warning items
    warning_items = []
    wc = 0
    nr = int(L.get("Neg_Reversals_10d", 0)); w = nr >= 3
    pr = int(L.get("Pos_Reversals_10d", 0))
    warning_items.append(("Bärische Intraday-Umkehrungen (10T)", nr < 3, f"{nr} neg. / {pr} pos. Umkehrungen", w)); wc += int(w)
    lc = int(L.get("Low_CR_5d", 0)); w = lc >= 3
    warning_items.append(("Closing Range Häufung (5T)", lc < 3, f"{lc}/5 Tage Schluss im unteren 25%", w)); wc += int(w)
    st10 = int(df["Is_Stall"].tail(10).sum()); w = st10 >= 3
    warning_items.append(("Stau-Tage (10T)", st10 < 3, f"{st10} Stau-Tage", w)); wc += int(w)
    dc = int(L["Dist_Count_25"]); w = dc >= 4
    warning_items.append(("Distributionstage (25T)", dc < 4, f"{dc} Dist.-Tage (Schwelle: 4)", w)); wc += int(w)
    d21 = L["Dist_21EMA"]
    d50 = L["Dist_50SMA_pct"]; t50 = 7.0 if "Nasdaq" in selected else 5.0
    if not np.isnan(d50):
        w = d50 > t50 or d50 < 0
        warning_items.append(("50-SMA Abstand", not w, f"{d50:+.1f}% ({'über' if d50 > 0 else 'unter'} 50-SMA, Schwelle: {t50:.0f}%)", w)); wc += int(w)
    if not np.isnan(d21):
        w_under = d21 < 0
        w_over = d21 > 3.0
        warning_items.append(("Kurs unter 21-EMA", not w_under, f"{d21:.1f} ATR unter 21-EMA" if w_under else f"{d21:.1f} ATR über 21-EMA", w_under)); wc += int(w_under)
        warning_items.append(("Überdehnt über 21-EMA (>3 ATR)", not w_over, f"{d21:.1f} ATR über 21-EMA", w_over)); wc += int(w_over)
    u200 = not np.isnan(L["SMA200"]) and L["Close"] < L["SMA200"]
    u50 = not np.isnan(L["SMA50"]) and L["Close"] < L["SMA50"]
    warning_items.append(("Kurs über 200-SMA", not u200, "Unter 200-SMA" if u200 else "OK", u200)); wc += int(u200)
    warning_items.append(("Kurs über 50-SMA", not u50, "Unter 50-SMA" if u50 else "OK", u50)); wc += int(u50)
    vd = bool(L.get("Up_Vol_Declining", False))
    warning_items.append(("Volumen an Aufwärtstagen", not vd, "Abnehmendes Vol." if vd else "OK", vd)); wc += int(vd)
    if div_r:
        ah = [r for r in div_r if r["at_20d_high"]]; nh = [r for r in div_r if not r["at_20d_high"]]; hd = len(ah) > 0 and len(nh) > 0
        warning_items.append(("Intermarket-Konvergenz", not hd, " · ".join(f"{r['name']}: {r.get('dist_to_20d_high_pct', r.get('pct', np.nan)):+.1f}% zum 20T-Hoch" if pd.notna(r.get('dist_to_20d_high_pct', r.get('pct', np.nan))) else f"{r['name']}: n/a" for r in div_r), hd)); wc += int(hd)
    if rot is not None:
        warning_items.append(("Keine Sektorrotation in Defensive", not rot, f"Spread: {sp:+.1f}%", bool(rot))); wc += int(bool(rot))
    rp, drp = detect_failing_rally(df)
    if rp is not None and drp and drp > 5:
        w = rp < 50
        warning_items.append(("Erholungsquote ≥50%", not w, f"Rückgang {drp:.1f}%, Erholung {rp:.0f}%", w)); wc += int(w)

    mode, tone, action = _market_action_and_tone(L.get("Ampel_Phase", ""), wc, breadth_label, str(vol_latest.get("VIX_Regime", "")))
    reasons = _build_market_reasons(L, wc, breadth_label, vol_latest)
    freshness = _format_data_freshness(selected, df, vol_dashboard)
    changes = _build_market_changes(df, selected, wc, vol_dashboard, breadth_label)
    render_haltung_banner(mode, wc, L.get("Dist_50SMA_pct", np.nan), breadth_label)
    _render_change_cards(changes)

    # Trendwende-Ampel wieder als zentrales Element sichtbar machen
    render_ampel_section(L, sd)

    # MA-Abstand Kacheln (10-SMA, 21-EMA, 50-SMA, 200-SMA)
    d10 = L.get("Dist_10SMA_pct", np.nan)
    d200 = L.get("Dist_200SMA_pct", np.nan)
    s10_val = L.get("SMA10", np.nan)
    if np.isnan(d10) and not np.isnan(s10_val) and s10_val > 0:
        d10 = (L["Close"] - s10_val) / s10_val * 100
    if np.isnan(d200) and not np.isnan(L["SMA200"]) and L["SMA200"] > 0:
        d200 = (L["Close"] - L["SMA200"]) / L["SMA200"] * 100

    if np.isnan(d21):
        d21_tone, d21_lbl = "neutral", "—"
    elif d21 < 0:
        d21_tone, d21_lbl = "bad", "✗ Darunter"
    elif d21 > 3.0:
        d21_tone, d21_lbl = "warn", "⚠ Überdehnt"
    else:
        d21_tone, d21_lbl = "good", "✓ OK"
    if np.isnan(d10):
        d10_tone, d10_lbl = "neutral", "—"
    elif d10 < 0:
        d10_tone, d10_lbl = "bad", "✗ Darunter"
    else:
        d10_tone, d10_lbl = "good", "✓ OK"
    if np.isnan(d50):
        d50_tone, d50_lbl = "neutral", "—"
    elif d50 < 0:
        d50_tone, d50_lbl = "bad", "✗ Darunter"
    elif d50 > t50:
        d50_tone, d50_lbl = "warn", "⚠ Überdehnt"
    else:
        d50_tone, d50_lbl = "good", "✓ OK"
    if np.isnan(d200):
        d200_tone, d200_lbl = "neutral", "—"
    elif d200 < 0:
        d200_tone, d200_lbl = "bad", "✗ Darunter"
    else:
        d200_tone, d200_lbl = "good", "✓ OK"
    st.markdown(
        '<div class="mobile-ma-grid">'
        + _dist_tile_html("21-EMA", f"{d21:.1f} ATR" if not np.isnan(d21) else "—", d21_lbl, d21_tone)
        + _dist_tile_html("10-SMA", f"{d10:+.1f}%" if not np.isnan(d10) else "—", d10_lbl, d10_tone)
        + _dist_tile_html("50-SMA", f"{d50:+.1f}%" if not np.isnan(d50) else "—", d50_lbl, d50_tone)
        + _dist_tile_html("200-SMA", f"{d200:+.1f}%" if not np.isnan(d200) else "—", d200_lbl, d200_tone)
        + '</div>',
        unsafe_allow_html=True,
    )

    st.plotly_chart(plot_price_with_volume(df, sd), width="stretch", config={"displayModeBar": False})

    if not compact:
      with st.expander("Frühwarnzeichen und Warnzeichen", expanded=True):
        st.markdown('<div class="section-divider">WARNLAGE</div>', unsafe_allow_html=True)
        for label, ok, detail, warn in warning_items:
            render_check(label, ok, detail, warn=warn)
        if wc == 0:
            st.markdown('<div style="text-align:center;padding:8px;color:#22c55e;">✓ Keine aktiven Warnzeichen</div>', unsafe_allow_html=True)
        elif wc <= 2:
            st.markdown(f'<div style="text-align:center;padding:8px;color:#f59e0b;">⚠ {wc} Warnzeichen</div>', unsafe_allow_html=True)
        else:
            st.markdown(f'<div style="text-align:center;padding:8px;color:#ef4444;">⚠ {wc} Warnzeichen — Risiko reduzieren</div>', unsafe_allow_html=True)


    if not compact:
      with st.expander("Trendcheck, Ordnung und Sektorrotation", expanded=False):
        cl, cr_ = st.columns(2)
        with cl:
            st.markdown('<div class="section-divider">TRENDPRÜFUNG</div>', unsafe_allow_html=True)
            _c = L["Close"]; _l = L["Low"]; _e = L["EMA21"]; _s5 = L["SMA50"]; _s2 = L["SMA200"]
            eo = not np.isnan(_e); so = not np.isnan(_s5); s2o = not np.isnan(_s2)
            for nm, mv, ok, hk, ck in [("21-EMA", _e, eo, "EMA21_held", "Consec_Low_above_21"), ("50-SMA", _s5, so, "SMA50_held", "Consec_Low_above_50"), ("200-SMA", _s2, s2o, "SMA200_held", "Consec_Low_above_200")]:
                render_check(f"Schluss über {nm}", ok and _c > mv, f"{_c:,.0f} vs {mv:,.0f}" if ok else "")
                render_check(f"Tief über {nm}", ok and _l > mv, f"{_l:,.0f} vs {mv:,.0f}" if ok else "")
                render_check(f"{nm} gehalten", bool(L.get(hk, False)), "Schlusskurs darüber" if bool(L.get(hk, False)) else "Darunter")
                cc = int(L.get(ck, 0))
                render_check(f"3T Tief>{nm}", cc >= 3, f"{cc} Tage")

            st.markdown('<div class="section-divider">ORDNUNG</div>', unsafe_allow_html=True)
            render_check("21-EMA > 50-SMA", eo and so and _e > _s5, f"{_e:,.0f} vs {_s5:,.0f}" if eo and so else "")
            render_check("21-EMA > 200-SMA", eo and s2o and _e > _s2, f"{_e:,.0f} vs {_s2:,.0f}" if eo and s2o else "")
            render_check("50-SMA > 200-SMA", so and s2o and _s5 > _s2, f"{_s5:,.0f} vs {_s2:,.0f}" if so and s2o else "")

        with cr_:
            if rot is not None and sd2:
                st.markdown('<div class="section-divider">SEKTORROTATION (10T)</div>', unsafe_allow_html=True)
                sector_html = '<div class="sector-grid">'
                for group, items in sd2.items():
                    color = "#dc2626" if group == "Defensiv" else "#16a34a"
                    sector_html += f'<div><div class="sector-heading">{group}</div>'
                    for name, perf in items:
                        if perf is not None:
                            sector_html += f'<div class="sector-badge" style="color:{color};"><span class="sector-dot" style="background:{color};"></span><span>{name} {perf:+.1f}%</span></div>'
                    sector_html += '</div>'
                sector_html += '</div>'
                st.markdown(sector_html, unsafe_allow_html=True)
            if div_r:
                st.markdown('<div class="section-divider">INTERMARKET-BILD</div>', unsafe_allow_html=True)
                with st.expander("ℹ️ Erklärung", expanded=False):
                    st.markdown("Tagesveränderung und Abstand zum vorherigen 20-Tage-Hoch zeigen, ob wichtige Indizes gemeinsam Stärke bestätigen.")
                for r in div_r:
                    dist = r.get("dist_to_20d_high_pct", r.get("pct", np.nan))
                    day = r.get("day_pct", np.nan)
                    tone_c = "#16a34a" if pd.notna(day) and day >= 0 else "#dc2626"
                    dist_c = "#16a34a" if pd.notna(dist) and dist >= 0 else "#dc2626"
                    day_txt = f"{day:+.1f}% Tag" if pd.notna(day) else "n/a"
                    dist_txt = f"{dist:+.1f}% zum 20T-Hoch" if pd.notna(dist) else "n/a"
                    st.markdown(f'<div style="padding:4px 0;display:flex;justify-content:space-between;gap:8px;font-size:14px;"><span style="color:{tone_c};font-weight:700;">{r["name"]}</span><span class="mini-help">{day_txt} · <span style="color:{dist_c};">{dist_txt}</span></span></div>', unsafe_allow_html=True)

    if not compact:
      with st.expander("Marktbreite und Volatilität", expanded=True):
        st.markdown('<div class="card-label">Marktbreite — Equal-Weight</div>', unsafe_allow_html=True)
        bc1, bc2 = st.columns(2)
        for col, etf, dfe in [(bc1, ep, breadth_primary), (bc2, es, breadth_secondary)]:
            with col:
                if dfe is not None and len(dfe):
                    render_breadth(dfe.iloc[-1]["Breadth_Mode"], float(dfe.iloc[-1]["Dist_52w_pct"]))
                    st.caption(etf)
                else:
                    st.info(f"{etf} n/a")

        st.markdown('<div class="section-divider">VOLATILITÄTS-REGIME</div>', unsafe_allow_html=True)
        vol_card = vol_summary.get("Vol Regime", {"status": "Neutral", "detail": "", "tone": "#ca8a04"})
        fragile_active = bool(vol_latest.get("Fragile_Rally", False))
        vol_status = f'{vol_card["status"]} ⚠️' if fragile_active else vol_card["status"]
        vol_tone = "#ca8a04" if fragile_active else vol_card["tone"]
        st.markdown(
            f'<div class="info-card" style="border-left:4px solid {vol_tone};">'
            f'<div class="card-label">VOLATILITÄTS-REGIME</div>'
            f'<div style="font-size:1.1rem;font-weight:800;color:{vol_tone};margin-bottom:6px;">{vol_status}</div>'
            f'<div class="vol-subline">VIX {vol_latest.get("VIX_Close", np.nan):.1f} · {vol_latest.get("VIX_Regime", "n/a")}</div>'
            f'<div class="vol-subline">VIXY {vol_latest.get("VIXY_Close", np.nan):.1f} · {vol_summary.get("VIXY Bestätigung", {}).get("status", "n/a")}</div>'
            f'</div>',
            unsafe_allow_html=True,
        )

        if vix_df is not None or vixy_df is not None:
            st.plotly_chart(plot_volatility_combo(vix_df, vixy_df, sd), width="stretch", config={"displayModeBar": False})
        else:
            st.info("Keine VIX-/VIXY-Daten verfügbar")

        if pd.notna(vol_latest.get("VIX_Close")) or pd.notna(vol_latest.get("VIXY_Close")):
            st.caption(
                f"VIX {vol_latest.get('VIX_Close', np.nan):.1f} · "
                f"VIXY {vol_latest.get('VIXY_Close', np.nan):.1f} · "
                f"S&P 500 5T {vol_latest.get('SPX_Ret_5d', np.nan) * 100:+.1f}%"
            )

    if not compact:
      with st.expander("Tägliche Checkliste", expanded=False):
        st.markdown('<div class="info-card"><div class="card-label">Tägliche Checkliste</div>', unsafe_allow_html=True)
        ddv = float(L["Dist_52w_pct"]) if not np.isnan(L["Dist_52w_pct"]) else 0
        no_correction = ddv > -8
        render_check("Kein substanzieller Drawdown (> -8%)", no_correction, f"Drawdown: {ddv:.1f}%" + (" — Korrektur läuft, Ampel aktiv" if not no_correction else " — Markt im Normalbereich"))
        render_check("Stabilisierung?", L["Ampel_Phase"] not in ("rot",) or L["Anchor_Date"] is not None, f"Ankertag: {L['Anchor_Date']}" if L["Anchor_Date"] else "Kein Zyklus" if L["Ampel_Phase"] in ("neutral", "aufwaertstrend") else "Noch keine")
        render_check("Startschuss (≥Gelb)?", L["Ampel_Phase"] in ("gelb", "gruen", "aufwaertstrend"), f"Phase: {_ampel_phase_label(L.get('Ampel_Phase', ''))}")
        if breadth_primary is not None and len(breadth_primary):
            render_check("Marktbreite?", breadth_primary.iloc[-1]["Breadth_Mode"] != "schutz", f"Modus: {breadth_primary.iloc[-1]['Breadth_Mode'].capitalize()}")
        render_check("VIX Regime nicht Stress?", vol_latest.get("VIX_Regime", "Neutral") != "Stress", f"Regime: {vol_latest.get('VIX_Regime', 'n/a')}")
        render_check("Warnzeichen ≤2?", wc <= 2, f"{wc} aktiv")
        st.markdown("</div>", unsafe_allow_html=True)

    if not compact:
        st.markdown("### 🔎 Tiefenanalyse")
        load_clicked = st.button("🔍 Tiefenanalyse laden", type="primary", key="load_deep_analysis_btn")
        if load_clicked:
            st.session_state["show_deep_analysis"] = True
        if not st.session_state.get("show_deep_analysis", False):
            st.caption("Die Tiefenanalyse wird nur bei Bedarf geladen, damit die Startansicht schnell bleibt.")
        else:
            store = _get_price_store()
            if store.get("backend") == "sqlite":
                auto_refresh = _maybe_auto_refresh_sqlite_cache(store, reason="deep_analysis")
                if auto_refresh.get("triggered"):
                    if auto_refresh.get("ok"):
                        st.caption("SQLite-Auto-Refresh ausgeführt, lade aktuelle Tiefenanalyse …")
                    else:
                        st.warning("SQLite-Auto-Refresh fehlgeschlagen. Es werden die zuletzt verfügbaren Daten verwendet.")
            with st.spinner("Lese Tiefenanalyse-Daten aus dem persistenten Datenspeicher …"):
                component_bundle = load_nyse_breadth_data()
            benchmark_last = pd.Timestamp(data["S&P 500"].index[-1]).date() if "S&P 500" in data and len(data["S&P 500"]) else pd.Timestamp(df.index[-1]).date()
            refresh_at_raw = _get_cache_metadata(store, "last_refresh_at", "")
            refresh_date = _parse_refresh_date(refresh_at_raw)
            if component_bundle is None:
                benchmark_str = benchmark_last.strftime("%d.%m.%Y")
                active_job = _get_active_refresh_job(store)
                if active_job:
                    st.warning(
                        f"Die Kursdaten werden aktualisiert (benötigter Stand: {benchmark_str}). "
                        "Bitte komm in ca. 10 Minuten erneut auf die Seite."
                    )
                else:
                    auto_job = _maybe_request_external_refresh_job(
                        store,
                        reason="deep_analysis",
                        payload={"trigger": "deep_analysis", "required_date": benchmark_last.isoformat(), "cache_state": "missing"},
                    )
                    if auto_job.get("triggered") and auto_job.get("ok"):
                        st.warning(
                            f"Für die Tiefenanalyse fehlen aktuell Kursdaten (benötigter Stand: {benchmark_str}). "
                            "Die Aktualisierung wurde gestartet. Bitte komm in ca. 10 Minuten erneut auf die Seite."
                        )
                    elif auto_job.get("triggered"):
                        st.warning(
                            f"Für die Tiefenanalyse fehlen aktuell Kursdaten (benötigter Stand: {benchmark_str}). "
                            f"Die Aktualisierung konnte nicht gestartet werden: {auto_job.get('error') or 'Unbekannter Fehler'}"
                        )
                    else:
                        neon_auto_text = "Die automatische Aktualisierung läuft Mo–Fr um 22:30 Uhr Berliner Zeit über GitHub Actions."
                        if store.get("backend") == "neon" and not _is_neon_auto_update_enabled(store):
                            neon_auto_text = "Die automatische Neon-Aktualisierung ist aktuell deaktiviert."
                        st.info(
                            f"Für die Tiefenanalyse fehlen aktuell Kursdaten (benötigter Stand: {benchmark_str}). "
                            f"{neon_auto_text} "
                            "Prüfe den Worker-Status in den App-Logs und starte bei Bedarf den Aktualisierungs-Workflow manuell."
                        )
            else:
                br = _render_deep_analysis_content(component_bundle, sd, data)
                if br is not None and len(br):
                    breadth_last = pd.Timestamp(br.index[-1]).date()
                    cache_state = _deep_analysis_cache_state(breadth_last, benchmark_last, refresh_date)
                    if cache_state == "stale":
                        benchmark_str = benchmark_last.strftime("%d.%m.%Y")
                        breadth_str = breadth_last.strftime("%d.%m.%Y")
                        active_job = _get_active_refresh_job(store)
                        if active_job:
                            st.warning(
                                f"Kurse sind veraltet (Cache: {breadth_str}, benötigt: {benchmark_str}). "
                                "Die Aktualisierung läuft bereits. Bitte komm in ca. 10 Minuten erneut auf die Seite."
                            )
                        else:
                            auto_job = _maybe_request_external_refresh_job(
                                store,
                                reason="deep_analysis",
                                payload={
                                    "trigger": "deep_analysis",
                                    "required_date": benchmark_last.isoformat(),
                                    "cache_date": breadth_last.isoformat(),
                                    "last_refresh_at": refresh_at_raw,
                                    "cache_state": cache_state,
                                },
                            )
                            if auto_job.get("triggered") and auto_job.get("ok"):
                                st.warning(
                                    f"Kurse sind veraltet (Cache: {breadth_str}, benötigt: {benchmark_str}). "
                                    "Die Aktualisierung wurde gestartet. Bitte komm in ca. 10 Minuten erneut auf die Seite."
                                )
                            elif auto_job.get("triggered"):
                                st.warning(
                                    f"Kurse sind veraltet (Cache: {breadth_str}, benötigt: {benchmark_str}). "
                                    f"Die Aktualisierung konnte nicht gestartet werden: {auto_job.get('error') or 'Unbekannter Fehler'}"
                                )
                            else:
                                neon_auto_text = "Die automatische Aktualisierung läuft Mo–Fr um 22:30 Uhr Berliner Zeit über GitHub Actions."
                                if store.get("backend") == "neon" and not _is_neon_auto_update_enabled(store):
                                    neon_auto_text = "Die automatische Neon-Aktualisierung ist aktuell deaktiviert."
                                st.info(
                                    f"Kurse sind veraltet (Cache: {breadth_str}, benötigt: {benchmark_str}). "
                                    f"{neon_auto_text} "
                                    "Prüfe den Worker-Status in den App-Logs und starte bei Bedarf den Aktualisierungs-Workflow manuell."
                                )
                    elif cache_state == "last_available":
                        st.info(
                            f"Refresh wurde am {refresh_at_raw} UTC abgeschlossen. "
                            f"Die Tiefenanalyse zeigt aktuell den letzten verfügbaren Handelstag ({breadth_last.strftime('%d.%m.%Y')})."
                        )

    st.caption(f"Börse ohne Bauchgefühl · v3.2 · Stand: {L.name.strftime('%d.%m.%Y')}")


def _tab_dashboard():
    """Marktampel: Index-Auswahl, Hero-Card und Ampellogik."""
    _tab_marktanalyse(compact=True)


# ===== Main entry point =====


def _render_arbeitsbereich() -> None:
    _init_workspace_state()
    watchlist = _normalize_workspace_ticker_list(st.session_state.get("watchlist", []), limit=25)
    last_save = _workspace_last_save()

    col_h1, col_h2 = st.columns([3, 1])
    with col_h1:
        st.markdown("#### ⭐ Watchlist")
        save_info = (
            f"Letzte Speicherung {last_save.strftime('%d.%m.%Y · %H:%M UTC')}."
            if last_save is not None
            else "Noch nicht gespeichert."
        )
        st.caption(
            save_info + " Die Tabelle nutzt dieselbe Logik wie „Aktienbewertung → Vergleich & Ranking“."
        )
    with col_h2:
        if st.button("🔒 Sperren", key="ws_lock", use_container_width=True):
            _lock_private_area()
            st.rerun()

    st.markdown("<div style='height: 8px'></div>", unsafe_allow_html=True)
    add_col, manage_col = st.columns([1, 2])
    with add_col:
        with st.container(border=True):
            st.markdown(f'<div class="ws-label">Watchlist · {len(watchlist)}</div>', unsafe_allow_html=True)
            new_ticker = st.text_input("Ticker hinzufügen", key="watchlist_new_ticker", placeholder="z.B. AMD")
            if st.button("Hinzufügen", key="watchlist_add_ticker", use_container_width=True, disabled=not bool(new_ticker.strip())):
                _add_watchlist_ticker(new_ticker.upper())
                st.rerun()
    with manage_col:
        if watchlist:
            name_map = _ticker_display_names(tuple(watchlist))
            st.markdown('<div class="ws-label">Verwalten</div>', unsafe_allow_html=True)
            head_del, head_ticker, head_name = st.columns([0.12, 0.24, 0.64])
            with head_ticker:
                st.caption("Ticker")
            with head_name:
                st.caption("Name")
            for ticker in watchlist:
                del_col, ticker_col, name_col = st.columns([0.12, 0.24, 0.64])
                with del_col:
                    if st.button("🗑", key=f"watchlist_delete_{ticker}", help=f"{ticker} aus der Watchlist entfernen"):
                        _remove_watchlist_ticker(ticker)
                        st.success(f"{ticker} aus der Watchlist entfernt.")
                        st.rerun()
                with ticker_col:
                    st.markdown(f'<span class="ws-mono">{html.escape(ticker)}</span>', unsafe_allow_html=True)
                with name_col:
                    st.markdown(html.escape(name_map.get(ticker, ticker)), unsafe_allow_html=True)
        else:
            st.info("Noch keine Ticker in der Watchlist.")

    if not watchlist:
        return

    rs_source_setting = _get_rs_rating_source_setting()
    with st.spinner("Berechne Watchlist-Ranking …"):
        compare_df = _compute_stock_compare_rows(watchlist, rs_source_setting)
    if compare_df.empty:
        st.warning("Für deine Watchlist konnten nicht genug Kursdaten geladen werden.")
        return

    _render_stock_ranking_tables(compare_df, key_prefix="watchlist")


def _tab_mein_depot():
    """Mein Depot: alle depot-bezogenen Funktionen inkl. Rechner und Watchlist."""
    if not _render_private_gate("🔐 Mein Depot"):
        return
    inject_workspace_css()
    state = _depot_state()
    st.markdown("### 💼 Mein Depot")
    st.caption(
        "Nach Kapitel 7.2 · Korridor 8–12 Aktien · Positionsgröße über max. Verlust · "
        "Verkaufskandidaten, Nach-Kauf-Check und Verkaufsentscheidung nutzen dieselbe Engine."
    )
    tabs = st.tabs([
        "📋 Übersicht",
        "✏️ Positionen",
        "📈 Depotkurve",
        "🎯 Nach-Kauf-Check",
        "🧭 Verkaufs-Entscheidung",
        "🧮 Rechner",
        "⭐ Watchlist",
    ])
    with tabs[0]:
        _render_depot_overview(state)
    with tabs[1]:
        _render_depot_positions_manager(state)
    with tabs[2]:
        _render_depot_curve_only(state)
    with tabs[3]:
        _tab_nach_kauf()
    with tabs[4]:
        _tab_verkaufsentscheidung()
    with tabs[5]:
        _render_stueckzahl_rechner_page()
    with tabs[6]:
        _init_workspace_state()
        st.caption(
            f"Persistent über {_workspace_backend_label()} · Watchlist: {_workspace_scope()}"
        )
        _render_arbeitsbereich()


def _render_technical_setup_area():
    st.markdown("### ⚙️ Technisches Setup")
    st.caption("Privater Wartungsbereich für Datenbankaktualisierung, Worker-Status und Diagnose.")
    store = _get_price_store()
    settings_preview = _get_portfolio_settings()
    pref_auto = settings_preview.get("neon_auto_update_preference", "on")
    pref_enabled = str(pref_auto).strip().lower() == "on"
    runtime_enabled = _is_neon_auto_update_enabled(store) if store.get("backend") == "neon" else pref_enabled
    if runtime_enabled:
        st.caption("Automatische Neon-Aktualisierung: Montag bis Freitag um 22:30 Uhr (Europe/Berlin). Manueller Start bleibt verfügbar.")
    else:
        st.caption("Automatische Neon-Aktualisierung ist deaktiviert. Manueller Start bleibt verfügbar.")
    st.caption(f"Persistenter Datenspeicher: {_get_store_label(store)}")
    if store["backend"] != "neon":
        st.warning("Neon ist aktuell nicht konfiguriert. Die App nutzt daher nur den lokalen SQLite-Cache. Für Streamlit Cloud ist Neon meist stabiler.")

    current_refresh_at = _get_cache_metadata(store, "last_refresh_at", "")
    live_status = get_live_universe_store_status(store["backend"], _get_store_label(store), str(current_refresh_at or ""))
    live_cols = st.columns(5)
    live_cols[0].metric("Universe live", int(live_status.get("requested", 0)))
    live_cols[1].metric("Im Cache", int(live_status.get("loaded", 0)))
    live_cols[2].metric("Abdeckung", f"{float(live_status.get('coverage', 0.0) or 0.0):.0%}")
    live_cols[3].metric("Mappings", int(live_status.get("mapped", 0)))
    live_cols[4].metric("Ungeklärt", int(live_status.get("not_found", 0)) + int(live_status.get("no_history", 0)))

    active_job = _get_active_refresh_job(store)
    latest_jobs = _list_recent_refresh_jobs(store, limit=6)
    latest_job = latest_jobs[0] if latest_jobs else None
    if latest_job:
        with st.container(border=True):
            st.markdown(f"**Letzter Job:** {_job_type_label(latest_job.get('job_type'))} · **Status:** {_job_status_badge(latest_job.get('status'))}")
            st.caption(f"Job-ID: {latest_job.get('job_id', '—')} · Schritt: {latest_job.get('current_step') or '—'}")
            if st.button("Status neu laden", use_container_width=True, key="tech_status_reload"):
                st.rerun()

    settings = _get_portfolio_settings()
    saved_neon_pref = settings.get("neon_auto_update_preference", "on")
    if saved_neon_pref not in {"on", "off"}:
        saved_neon_pref = "on"
    neon_auto_enabled = _is_neon_auto_update_enabled(store) if store.get("backend") == "neon" else (saved_neon_pref == "on")
    auto_cols = st.columns([1, 1.6])
    with auto_cols[0]:
        neon_auto_choice = st.selectbox(
            "Neon Auto-Update",
            options=["on", "off"],
            index=0 if neon_auto_enabled else 1,
            format_func=lambda value: "Aktiviert" if value == "on" else "Deaktiviert",
            key="tech_neon_auto_update_select",
        )
    with auto_cols[1]:
        if store.get("backend") == "neon":
            runtime_flag = "Aktiviert" if _is_neon_auto_update_enabled(store) else "Deaktiviert"
            st.caption(f"Aktiver Rhythmus: Montag–Freitag um 22:30 Uhr (Europe/Berlin) via GitHub Actions · Laufzeitstatus: {runtime_flag}.")
        else:
            st.caption("Neon ist nicht aktiv. Du kannst die Auto-Update-Präferenz trotzdem schon speichern.")
        if st.button("Auto-Update speichern", key="tech_neon_auto_update_save"):
            settings["neon_auto_update_preference"] = neon_auto_choice
            _save_portfolio_settings(settings)
            if store.get("backend") == "neon":
                _set_neon_auto_update_enabled(store, neon_auto_choice == "on")
            st.rerun()

    st.markdown("#### 🔔 Positionsmonitor · ATR-Push")
    st.caption(
        "GitHub Actions prüft die offenen Positionen aus der gespeicherten TR-CSV "
        "und sendet bei ATR-Verlusten eine Pushover-Nachricht an dein iPhone."
    )
    monitor_settings = _get_portfolio_settings()
    monitor_state = _get_position_monitor_state(store)
    last_summary = monitor_state.get("last_summary", {}) if isinstance(monitor_state.get("last_summary"), dict) else {}
    last_pushover_test_at = str(monitor_state.get("last_pushover_test_at", "") or "")
    last_pushover_test_result = monitor_state.get("last_pushover_test_result", [])
    if last_summary:
        checked = int(last_summary.get("checked", 0) or 0)
        alerts = int(last_summary.get("alerts", 0) or 0)
        sent = int(last_summary.get("sent", 0) or 0)
        finished_at = str(monitor_state.get("last_finished_at", "") or "")
        reason = str(last_summary.get("reason", "") or "")
        skip_text = f" · übersprungen: {reason}" if last_summary.get("skipped") and reason else ""
        st.caption(f"Letzter Lauf: {finished_at or 'unbekannt'} · geprüft {checked} · Alarme {alerts} · gesendet {sent}{skip_text}.")
        alert_details = last_summary.get("alerts_detail", [])
        if isinstance(alert_details, list) and alert_details:
            alert_df = pd.DataFrame(alert_details)
            alert_cols = [col for col in ("ticker", "drop_atr", "close", "atr", "reference_label", "trade_date") if col in alert_df.columns]
            if alert_cols:
                st.dataframe(alert_df[alert_cols], width="stretch", hide_index=True)
    if last_pushover_test_at:
        test_ok = False
        if isinstance(last_pushover_test_result, list) and last_pushover_test_result:
            test_ok = all(bool(item.get("ok")) for item in last_pushover_test_result if isinstance(item, dict))
        test_label = "erfolgreich" if test_ok else "fehlgeschlagen"
        st.caption(f"Letzter Pushover-Test: {last_pushover_test_at} · {test_label}.")

    monitor_cols = st.columns([1, 1, 1, 1])
    with monitor_cols[0]:
        monitor_enabled = st.toggle(
            "Monitor aktiv",
            value=bool(monitor_settings.get("position_monitor_enabled", False)),
            key="tech_position_monitor_enabled",
        )
    with monitor_cols[1]:
        monitor_threshold = st.number_input(
            "ATR-Schwelle",
            min_value=0.1,
            max_value=10.0,
            value=float(monitor_settings.get("position_monitor_threshold_atr", 1.5)),
            step=0.1,
            key="tech_position_monitor_threshold",
            help="Push wird ausgelöst, wenn der Verlust mindestens diese Anzahl ATR erreicht.",
        )
    with monitor_cols[2]:
        reference_options = list(POSITION_MONITOR_REFERENCE_LABELS.keys())
        current_reference = str(monitor_settings.get("position_monitor_reference", "high_since_buy"))
        monitor_reference = st.selectbox(
            "Referenz",
            options=reference_options,
            index=reference_options.index(current_reference) if current_reference in reference_options else 0,
            format_func=lambda key: POSITION_MONITOR_REFERENCE_LABELS.get(key, key),
            key="tech_position_monitor_reference",
        )
    with monitor_cols[3]:
        monitor_cooldown = st.number_input(
            "Cooldown Stunden",
            min_value=0.0,
            max_value=168.0,
            value=float(monitor_settings.get("position_monitor_cooldown_hours", 18)),
            step=1.0,
            key="tech_position_monitor_cooldown",
        )

    monitor_more_cols = st.columns([1, 1, 1, 2])
    with monitor_more_cols[0]:
        monitor_atr_period = st.number_input(
            "ATR-Periode",
            min_value=2,
            max_value=50,
            value=int(float(monitor_settings.get("position_monitor_atr_period", 14))),
            step=1,
            key="tech_position_monitor_atr_period",
        )
    with monitor_more_cols[1]:
        monitor_lookback = st.number_input(
            "Lookback Tage",
            min_value=60,
            max_value=1000,
            value=int(float(monitor_settings.get("position_monitor_lookback_days", 420))),
            step=30,
            key="tech_position_monitor_lookback",
        )
    with monitor_more_cols[2]:
        monitor_interval = st.number_input(
            "Intervall Minuten",
            min_value=5,
            max_value=1440,
            value=int(float(monitor_settings.get("position_monitor_interval_minutes", 5))),
            step=5,
            key="tech_position_monitor_interval",
            help="Die GitHub Action startet alle 5 Minuten. Dieser Wert steuert, wie oft wirklich geprüft wird.",
        )
    with monitor_more_cols[3]:
        monitor_user_key = st.text_area(
            "Pushover Benutzerschlüssel",
            value=str(monitor_settings.get("position_monitor_pushover_user_key", "") or ""),
            key="tech_position_monitor_pushover_user_key",
            height=70,
            help="Einen oder mehrere User Keys, getrennt durch Zeilenumbruch oder Komma. Alternativ GitHub Secret PUSHOVER_USER_KEY.",
        )
        monitor_app_token = st.text_input(
            "Pushover App Token",
            value=str(monitor_settings.get("position_monitor_pushover_app_token", "") or ""),
            key="tech_position_monitor_pushover_app_token",
            type="password",
            help="API Token deiner Pushover-App. Alternativ GitHub Secret PUSHOVER_APP_TOKEN.",
        )

    save_monitor_col, test_push_col, trigger_monitor_col = st.columns([1, 1, 1])
    with save_monitor_col:
        if st.button("Positionsmonitor speichern", key="tech_position_monitor_save", use_container_width=True):
            next_settings = _get_portfolio_settings()
            next_settings.update({
                "position_monitor_enabled": bool(monitor_enabled),
                "position_monitor_threshold_atr": float(monitor_threshold),
                "position_monitor_reference": str(monitor_reference),
                "position_monitor_atr_period": int(monitor_atr_period),
                "position_monitor_lookback_days": int(monitor_lookback),
                "position_monitor_interval_minutes": int(monitor_interval),
                "position_monitor_cooldown_hours": float(monitor_cooldown),
                "position_monitor_pushover_user_key": str(monitor_user_key or "").strip(),
                "position_monitor_pushover_app_token": str(monitor_app_token or "").strip(),
            })
            _save_portfolio_settings(next_settings)
            st.success("Positionsmonitor gespeichert.")
            st.rerun()
    with test_push_col:
        if st.button("Pushover-Test senden", key="tech_position_monitor_pushover_test", use_container_width=True, disabled=bool(active_job)):
            result = _request_external_refresh_job(
                "pushover_test",
                requested_by="streamlit_pushover_test",
                payload={"trigger": "streamlit_pushover_test"},
            )
            if result.get("ok"):
                st.success(f"✓ Pushover-Test angelegt: {result['job']['job_id']}.")
            else:
                st.error(result.get("error") or "Der Pushover-Test konnte nicht gestartet werden.")
    with trigger_monitor_col:
        if st.button("ATR-Monitor jetzt prüfen", key="tech_position_monitor_trigger", use_container_width=True, disabled=bool(active_job)):
            result = _request_external_refresh_job(
                "position_atr_monitor",
                requested_by="streamlit_position_monitor",
                payload={"trigger": "streamlit_position_monitor"},
            )
            if result.get("ok"):
                st.success(f"✓ ATR-Monitor angelegt: {result['job']['job_id']}.")
            else:
                st.error(result.get("error") or "Der ATR-Monitor konnte nicht gestartet werden.")
    st.caption(
        "GitHub Secrets für Pushover: PUSHOVER_USER_KEY und PUSHOVER_APP_TOKEN. "
        "Den App Token erstellst du bei Pushover über eine eigene Application/API Token."
    )

    current_rs_source = _get_rs_rating_source_setting()
    rs_source_options = list(RS_SOURCE_LABELS.keys())
    rs_source_choice = st.selectbox(
        "RS-Rating Quelle",
        options=rs_source_options,
        index=rs_source_options.index(current_rs_source) if current_rs_source in rs_source_options else 0,
        format_func=lambda key: RS_SOURCE_LABELS.get(key, key),
        key="tech_rs_source_select",
        help="Du kannst zwischen deiner eigenen Repo-CSV, Freds RS-CSV und der DB-/Live-Berechnung umschalten. Die Auswahl wird dauerhaft gespeichert.",
    )
    if st.button("RS-Quelle speichern", use_container_width=False, key="tech_rs_source_save"):
        settings["rs_rating_source"] = rs_source_choice if rs_source_choice in RS_SOURCE_LABELS else RS_SOURCE_CSV_LATEST
        _save_portfolio_settings(settings)
        st.success("RS-Quelle gespeichert. Die Auswahl bleibt auch nach Neustart erhalten.")

    backend_options = ["sqlite", "neon"]
    backend_labels = {
        "sqlite": "SQLite (Standard)",
        "neon": "Neon Postgres",
    }
    current_backend = settings.get("db_backend_preference", "sqlite")
    if current_backend not in backend_options:
        current_backend = "sqlite"
    backend_choice = st.selectbox(
        "Datenbank-Backend",
        options=backend_options,
        index=backend_options.index(current_backend),
        format_func=lambda key: backend_labels.get(key, key),
        key="tech_db_backend_select",
        help="SQLite ist der Standard. Neon wird nur genutzt, wenn konfiguriert und erreichbar.",
    )
    if st.button("Backend speichern", use_container_width=False, key="tech_db_backend_save"):
        settings["db_backend_preference"] = backend_choice if backend_choice in backend_options else "sqlite"
        _save_portfolio_settings(settings)
        st.rerun()

    rs_csv_info = _load_selected_rs_ratings_map(rs_source_choice)
    if rs_csv_info.get("ok"):
        csv_caption_parts = [
            f"Quelle: {RS_SOURCE_LABELS.get(rs_source_choice, rs_source_choice)}",
            f"CSV: {rs_csv_info.get('file')}",
            f"{int(rs_csv_info.get('count', 0) or 0)} Ratings",
        ]
        if rs_csv_info.get("as_of_date"):
            csv_caption_parts.append(f"Stand {rs_csv_info.get('as_of_date')}")
        if rs_csv_info.get("generated_at_utc"):
            csv_caption_parts.append(f"Erzeugt {rs_csv_info.get('generated_at_utc')} UTC")
        st.caption(" · ".join(csv_caption_parts))
    elif rs_source_choice != RS_SOURCE_COMPUTED:
        st.warning(rs_csv_info.get("error") or "Die gewählte RS-CSV ist noch nicht verfügbar. Die App fällt bis dahin auf die interne Berechnung zurück.")
    else:
        st.caption("Die DB-Variante nutzt den internen Snapshot bzw. die Live-Berechnung aus dem Datenspeicher.")

    btn_refresh, btn_rescue, btn_remap, btn_export, btn_diag = st.columns(5)
    with btn_refresh:
        refresh_clicked = st.button("Aktienuniversum aktualisieren", use_container_width=True, disabled=bool(active_job), key="tech_refresh_universe")
    with btn_rescue:
        rescue_clicked = st.button("Fehlende nachladen", use_container_width=True, disabled=bool(active_job), key="tech_rescue_missing")
    with btn_remap:
        remap_clicked = st.button("Automatisch remappen", use_container_width=True, disabled=bool(active_job), key="tech_auto_remap")
    with btn_export:
        export_clicked = st.button("RS-CSV erzeugen", use_container_width=True, disabled=bool(active_job), key="tech_export_rs_csv")
    with btn_diag:
        diagnose_clicked = st.button("Yahoo-Diagnose", use_container_width=True, key="tech_yahoo_diag")

    if refresh_clicked:
        result = _request_external_refresh_job("refresh_universe", payload={"trigger": "streamlit_private_tech"})
        if result.get("ok"):
            st.success(f"✓ Refresh-Job angelegt: {result['job']['job_id']}.")
        else:
            st.error(result.get("error") or "Der Refresh-Job konnte nicht gestartet werden.")
    if rescue_clicked:
        result = _request_external_refresh_job("rescue_missing", payload={"trigger": "streamlit_private_tech"})
        if result.get("ok"):
            st.success(f"✓ Rescue-Job angelegt: {result['job']['job_id']}.")
        else:
            st.error(result.get("error") or "Der Rescue-Job konnte nicht gestartet werden.")
    if remap_clicked:
        result = _request_external_refresh_job("auto_remap", payload={"trigger": "streamlit_private_tech"})
        if result.get("ok"):
            st.success(f"✓ Auto-Remap-Job angelegt: {result['job']['job_id']}.")
        else:
            st.error(result.get("error") or "Der Auto-Remap-Job konnte nicht gestartet werden.")
    if export_clicked:
        result = _request_external_refresh_job("export_rs_csv", payload={"trigger": "streamlit_private_tech", "rs_source": "github_csv"})
        if result.get("ok"):
            st.success(f"✓ RS-CSV-Job angelegt: {result['job']['job_id']}.")
        else:
            st.error(result.get("error") or "Der RS-CSV-Job konnte nicht gestartet werden.")
    if diagnose_clicked:
        with st.spinner("Teste eine Stichprobe der noch fehlenden Ticker im NYSE/Nasdaq-Aktienuniversum direkt gegen Yahoo …"):
            diag_stats = diagnose_missing_nyse_yahoo()
            st.cache_data.clear()
        if diag_stats.get("ok"):
            st.success(diag_stats.get("message", "Yahoo-Diagnose abgeschlossen."))
            results_df = diag_stats.get("results_df")
            if results_df is not None and not results_df.empty:
                st.dataframe(results_df, use_container_width=True, hide_index=True)
        else:
            st.error(diag_stats.get("error", "Die Yahoo-Diagnose ist fehlgeschlagen."))


def _tab_einstellungen():
    """Technisches Setup: Datenbankpflege, Worker-Status, RS-Quelle, Diagnose."""
    if not _render_private_gate("🔐 Einstellungen"):
        return
    _render_technical_setup_area()


def _render_topbar() -> None:
    st.markdown(
        """
        <div class="app-topbar">
          <div class="app-topbar__brand">
            <p class="app-topbar__eyebrow">Regelbasiertes Markt-Dashboard</p>
            <h1 class="app-topbar__title">Börse ohne Bauchgefühl</h1>
            <div class="app-topbar__subtitle">Kompakte Desktop-Ansicht für Ampel, Breite und Depot-Risiko</div>
          </div>
          <div class="app-topbar__meta"><span class="app-topbar__meta-dot"></span>Desktop optimiert</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def main():
    configure_page()
    inject_css()
    _render_workspace_sidebar()
    pages = [
        st.Page(_tab_dashboard, title="Marktampel", icon="🚦", url_path="marktampel", default=True),
        st.Page(_tab_marktanalyse, title="Marktanalyse", icon="📈", url_path="analyse"),
        st.Page(_tab_sektoranalyse, title="Sektoren", icon="🏭", url_path="sektoren"),
        st.Page(_tab_aktienbewertung, title="Aktienbewertung", icon="📋", url_path="aktie"),
        st.Page(_tab_mein_depot, title="Mein Depot", icon="💼", url_path="depot"),
        st.Page(_tab_einstellungen, title="Einstellungen", icon="⚙️", url_path="einstellungen"),
    ]
    navigation = st.navigation(pages, position="top")
    navigation.run()


if __name__ == "__main__":
    main()
