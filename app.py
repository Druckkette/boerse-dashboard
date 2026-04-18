"""
Single-file Streamlit app.
Merged back into one app.py while keeping the refactor improvements.
"""

import hashlib
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

# ===== From config.py =====
PAGE_CONFIG = {
    "page_title": "Börse ohne Bauchgefühl",
    "page_icon": "🚦",
    "layout": "wide",
    "initial_sidebar_state": "collapsed",
}

APP_CSS = """<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@500;700&display=swap');
:root{
  --bg:#0b1220;
  --panel:#111827;
  --panel-2:#0f172a;
  --panel-3:#0d1117;
  --border:#1e293b;
  --border-2:#253347;
  --muted:#94a3b8;
  --text:#e5eefb;
  --accent:#06b6d4;
  --accent-2:#3b82f6;
  --good:#22c55e;
  --warn:#f59e0b;
  --bad:#ef4444;
  --radius:14px;
  --radius-sm:10px;
}
html, body, [class*="css"] {font-family:'Inter',system-ui,sans-serif;}
.stApp{background-color:var(--bg);color:var(--text);font-family:'Inter',system-ui,sans-serif}
.main .block-container{padding-top:1.2rem;max-width:1240px}
h1,h2,h3{font-family:'Inter',system-ui,sans-serif!important;letter-spacing:-0.02em}
h1{font-size:1.75rem!important;font-weight:800!important;background:linear-gradient(135deg,#22d3ee,#3b82f6);-webkit-background-clip:text;-webkit-text-fill-color:transparent;letter-spacing:-.04em}
h2{font-size:1.2rem!important}
h3{font-size:1.0rem!important}
p, li, label, .stMarkdown, .stCaption {font-family:'Inter',system-ui,sans-serif!important}
code, pre, .card-label, [data-testid="stMetricLabel"], [data-testid="stMetricValue"]{font-family:'JetBrains Mono',monospace!important}

/* ── Metric cards ── */
[data-testid="stMetric"]{
  background:var(--panel);
  border:1px solid var(--border);
  border-radius:var(--radius);
  padding:14px 16px;
  box-shadow:0 2px 12px rgba(0,0,0,.25),0 0 0 1px rgba(255,255,255,.015) inset;
  transition:box-shadow .2s,border-color .2s;
}
[data-testid="stMetric"]:hover{
  box-shadow:0 4px 20px rgba(0,0,0,.38),0 0 0 1px rgba(255,255,255,.03) inset;
  border-color:var(--border-2);
}
[data-testid="stMetricLabel"]{color:#7c8aa0!important;font-size:.72rem!important;text-transform:uppercase;letter-spacing:.08em}
[data-testid="stMetricValue"]{color:var(--text)!important;font-size:1.32rem!important;font-weight:700!important}

/* ── Navigation tabs ── */
.stTabs [data-baseweb="tab-list"]{gap:6px;background:transparent;flex-wrap:wrap}
.stTabs [data-baseweb="tab"]{
  background:var(--panel);border:1px solid var(--border);border-radius:var(--radius-sm);
  color:var(--muted);padding:8px 14px;font-size:.86rem;
  transition:background .15s,color .15s,border-color .15s,box-shadow .15s;
}
.stTabs [aria-selected="true"]{
  background:#06b6d418!important;border-color:#0891b2!important;color:#67e8f9!important;
  box-shadow:0 0 14px rgba(6,182,212,.2)!important;
}

/* ── Cards ── */
.summary-hero,.change-card,.info-card,.workspace-card{
  background:var(--panel);
  border:1px solid var(--border);
  border-radius:var(--radius);
  padding:16px 18px;
  box-shadow:0 4px 20px rgba(0,0,0,.28);
}
.summary-hero{
  padding:20px 22px;
  background:linear-gradient(135deg,rgba(6,182,212,.11),rgba(59,130,246,.08));
  border:1px solid rgba(6,182,212,.22);
  border-left:4px solid var(--accent);
  box-shadow:0 4px 32px rgba(6,182,212,.07),0 4px 20px rgba(0,0,0,.28);
}
.ampel-box{border-radius:var(--radius);padding:16px 20px;display:flex;align-items:center;gap:16px}
.ampel-dot{width:48px;height:48px;border-radius:50%;flex-shrink:0}

/* ── Check items ── */
.check-item{display:flex;align-items:flex-start;gap:10px;padding:9px 0;border-bottom:1px solid var(--border)}
.check-item:last-child{border-bottom:none}
.check-icon{width:22px;height:22px;border-radius:50%;flex-shrink:0;display:flex;align-items:center;justify-content:center;font-size:12px;font-weight:700}
.check-ok  {background:#22c55e20;border:1.5px solid #22c55e50;color:var(--good);box-shadow:0 0 6px #22c55e18}
.check-fail{background:#ef444420;border:1.5px solid #ef444450;color:var(--bad); box-shadow:0 0 6px #ef444418}
.check-warn{background:#f59e0b20;border:1.5px solid #f59e0b50;color:var(--warn);box-shadow:0 0 6px #f59e0b18}

/* ── Cards inner layout ── */
.info-card,.workspace-card{margin-bottom:12px}
.card-label{font-size:.7rem;color:#7c8aa0;text-transform:uppercase;letter-spacing:.08em;margin-bottom:8px}
.mini-help{font-size:.76rem;color:#7c8aa0;line-height:1.45;margin-top:6px}
.hero-title{font-size:1.3rem;font-weight:800;color:var(--text);margin-bottom:4px;letter-spacing:-.02em}
.hero-subtitle{font-size:.85rem;color:var(--muted);margin-bottom:14px}
.hero-action{font-size:.93rem;font-weight:700;padding:10px 14px;border-radius:var(--radius-sm);margin-top:12px;letter-spacing:.01em}
.hero-good{background:#22c55e18;color:#86efac;border:1px solid #22c55e40}
.hero-warn{background:#f59e0b18;color:#fcd34d;border:1px solid #f59e0b40}
.hero-bad {background:#ef444418;color:#fca5a5;border:1px solid #ef444440}

/* ── Change cards ── */
.change-card{
  padding:14px 16px;
  border-left:3px solid var(--accent);
  transition:transform .15s,box-shadow .15s;
}
.change-card:hover{transform:translateY(-2px);box-shadow:0 8px 28px rgba(0,0,0,.42)}
.change-title{font-size:.72rem;color:#7c8aa0;text-transform:uppercase;letter-spacing:.08em;margin-bottom:6px}
.change-value{font-size:1rem;font-weight:700;color:var(--text)}
.change-detail{font-size:.8rem;color:var(--muted);margin-top:4px;line-height:1.35}

/* ── Custom KPI cards (colored metrics) ── */
.kpi-card{
  background:var(--panel);
  border:1px solid var(--border);
  border-radius:var(--radius);
  padding:14px 16px;
  box-shadow:0 2px 12px rgba(0,0,0,.25);
  transition:box-shadow .2s,border-color .2s,transform .15s;
  height:100%;
}
.kpi-card:hover{transform:translateY(-1px);box-shadow:0 6px 22px rgba(0,0,0,.38);border-color:var(--border-2)}
.kpi-card.kpi-warn{background:rgba(245,158,11,.07);border-color:rgba(245,158,11,.28);box-shadow:0 2px 16px rgba(245,158,11,.06)}
.kpi-card.kpi-bad {background:rgba(239,68,68,.07); border-color:rgba(239,68,68,.28); box-shadow:0 2px 16px rgba(239,68,68,.06)}
.kpi-card.kpi-good{background:rgba(34,197,94,.07);  border-color:rgba(34,197,94,.28);  box-shadow:0 2px 16px rgba(34,197,94,.06)}
.kpi-label  {font-size:.68rem;color:#7c8aa0;text-transform:uppercase;letter-spacing:.09em;font-family:'JetBrains Mono',monospace;margin-bottom:6px}
.kpi-value  {font-size:1.28rem;font-weight:700;color:var(--text);font-family:'JetBrains Mono',monospace;letter-spacing:-.01em;line-height:1.2}
.kpi-delta  {font-size:.78rem;margin-top:5px;font-weight:600}
.kpi-caption{font-size:.71rem;color:#64748b;margin-top:6px;line-height:1.35}

/* ── Section headers ── */
.section-header{
  display:flex;align-items:center;gap:10px;
  padding:4px 0 10px;
  border-bottom:1px solid var(--border);
  margin-bottom:18px;margin-top:2px;
}
.section-header-icon {font-size:1.05rem}
.section-header-title{font-size:.95rem;font-weight:700;color:var(--text);letter-spacing:-.01em}
.section-header-sub  {font-size:.76rem;color:var(--muted);margin-left:auto}

/* ── KPI explainer ── */
.kpi-explainer{background:rgba(15,23,42,.85);border:1px solid var(--border);border-radius:var(--radius-sm);padding:10px 12px;font-size:.8rem;color:var(--muted)}

/* ── Pills ── */
.pill-wrap{display:flex;flex-wrap:wrap;gap:6px}
.pill{
  display:inline-flex;align-items:center;
  padding:4px 10px;border-radius:999px;
  background:#0f172a;border:1px solid var(--border);
  color:var(--text);font-size:.8rem;
  transition:background .15s,border-color .15s;
}
.pill:hover{background:#1e293b;border-color:var(--border-2)}
.workspace-note{font-size:.82rem;color:var(--muted);line-height:1.45}

/* ── Breadth track ── */
.breadth-track{height:10px;border-radius:5px;background:var(--border);position:relative;overflow:hidden;margin:8px 0}
.breadth-fill{position:absolute;left:0;top:0;bottom:0;border-radius:5px;background:linear-gradient(90deg,#22c55e,#f59e0b,#ef4444);transition:width .5s}

/* ── Sidebar ── */
.sidebar-status-badge{
  display:flex;align-items:center;gap:8px;
  padding:8px 12px;border-radius:var(--radius-sm);
  background:var(--panel-3);border:1px solid var(--border);
  margin-bottom:8px;
}
.sidebar-dot{width:10px;height:10px;border-radius:50%;flex-shrink:0}

/* ── DataFrames ── */
.stDataFrame{border-radius:var(--radius)!important;overflow:hidden}
.stDataFrame thead tr th{
  background:#0d1117!important;font-size:.73rem!important;
  text-transform:uppercase;letter-spacing:.05em;color:#7c8aa0!important;
}
.stDataFrame tbody tr:hover td{background:rgba(30,41,59,.4)!important}

/* ── Divider ── */
hr{border:none;border-top:1px solid var(--border);margin:1rem 0}
</style>"""

def configure_page() -> None:
    st.set_page_config(**PAGE_CONFIG)


WORKSPACE_FILE = "user_workspace.json"
WORKSPACE_SCOPE_DEFAULT = "default"
DEFAULT_FAVORITES = ["NVDA", "MSFT", "AAPL", "META", "AMZN", "PLTR", "LLY", "TSLA"]
METRIC_GLOSSARY = {
    "Dist.-Tage": "Verkaufstage mit höherem Volumen als am Vortag. Viele Distribution Days sprechen für institutionellen Abgabedruck.",
    "21-EMA": "Abstand zur 21-EMA in ATR. Je weiter der Index darüber liegt, desto eher ist er kurzfristig überdehnt.",
    "50-SMA": "Prozentualer Abstand zur 50-Tage-Linie. Sehr große positive Abstände können Überhitzung anzeigen.",
    "Drawdown": "Abstand zum 52-Wochen-Hoch. Große negative Werte zeigen eine laufende Korrektur oder Schwächephase.",
    "Closing Range": "Wo der Schluss im Tagesbereich liegt. Hohe Werte bedeuten einen starken Schluss nahe Tageshoch.",
    "ATR (21T)": "Durchschnittliche Schwankungsbreite der letzten 21 Tage in Prozent. Hilft bei Risiko und Positionsgröße.",
    "DRR (Ø21T)": "Average Daily Range der letzten 21 Tage. Zeigt, wie nervös oder ruhig eine Aktie handelt.",
    "Beta": "Empfindlichkeit der Aktie gegenüber dem Gesamtmarkt. Werte über 1 bedeuten meist mehr Dynamik, aber auch mehr Schwankung.",
    "McClellan Osc.": "Kurzfristiger Breadth-Oszillator auf Basis Advancers minus Decliners. Über 0 ist meist konstruktiv.",
    "NH/NL Ratio": "Verhältnis neuer 52-Wochen-Hochs zu neuen 52-Wochen-Tiefs. Über 1 zeigt breite Stärke.",
    "% > 50-SMA": "Anteil der Aktien oberhalb ihrer 50-Tage-Linie. Zeigt, wie breit kurzfristige Trends sind.",
    "% > 200-SMA": "Anteil der Aktien oberhalb ihrer 200-Tage-Linie. Zeigt die langfristige Marktverfassung.",
    "Deemer Ratio": "Advancing Volume geteilt durch Declining Volume. Werte über 1.97 gelten als seltener Breitenschub.",
    "RS-Linie": "Kurs der Aktie geteilt durch den S&P 500. Eine steigende RS-Linie zeigt Outperformance gegenüber dem Markt.",
    "RS-Rating": "Perzentil-Rang der relativen Stärke gegenüber dem geladenen Aktienuniversum. Hohe Werte stehen für Marktführerschaft.",
}

def inject_css() -> None:
    st.markdown(APP_CSS, unsafe_allow_html=True)

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
        "rs_rating_source": "computed",
    }

def _workspace_payload():
    settings = dict(_default_portfolio_settings())
    raw_settings = st.session_state.get("portfolio_settings", {})
    if isinstance(raw_settings, dict):
        settings.update(raw_settings)
    return {
        "watchlist": list(dict.fromkeys(st.session_state.get("watchlist", []))),
        "recent_tickers": list(dict.fromkeys(st.session_state.get("recent_tickers", [])))[:12],
        "positions": st.session_state.get("positions", []),
        "todos": st.session_state.get("todos", ""),
        "portfolio_history": st.session_state.get("portfolio_history", []),
        "portfolio_cash_flows": st.session_state.get("portfolio_cash_flows", []),
        "portfolio_settings": settings,
    }

def _load_workspace_from_store():
    _ensure_workspace_store_ready()
    payload = {}
    store = _get_price_store()
    defaults = {
        "watchlist": [],
        "recent_tickers": [],
        "positions": [],
        "todos": "",
        "portfolio_history": [],
        "portfolio_cash_flows": [],
        "portfolio_settings": _default_portfolio_settings(),
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
    if not any(stored.get(k) for k in ["watchlist", "recent_tickers", "positions", "todos", "portfolio_history", "portfolio_cash_flows", "portfolio_settings"]):
        local_stored = _safe_json_load(Path(WORKSPACE_FILE), {})
        if isinstance(local_stored, dict) and local_stored:
            stored = local_stored
            try:
                st.session_state["watchlist"] = stored.get("watchlist", [])
                st.session_state["recent_tickers"] = stored.get("recent_tickers", [])
                st.session_state["positions"] = stored.get("positions", [])
                st.session_state["todos"] = stored.get("todos", "")
                st.session_state["portfolio_history"] = stored.get("portfolio_history", [])
                st.session_state["portfolio_cash_flows"] = stored.get("portfolio_cash_flows", [])
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
    st.session_state["todos"] = stored.get("todos", "") if isinstance(stored, dict) else ""
    st.session_state["portfolio_history"] = stored.get("portfolio_history", []) if isinstance(stored, dict) and isinstance(stored.get("portfolio_history", []), list) else []
    st.session_state["portfolio_cash_flows"] = stored.get("portfolio_cash_flows", []) if isinstance(stored, dict) and isinstance(stored.get("portfolio_cash_flows", []), list) else []
    base_settings = dict(_default_portfolio_settings())
    if isinstance(stored, dict) and isinstance(stored.get("portfolio_settings"), dict):
        base_settings.update(stored.get("portfolio_settings", {}))
    st.session_state["portfolio_settings"] = base_settings
    st.session_state["_workspace_initialized"] = True

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
        submitted = st.form_submit_button("Entsperren", use_container_width=True)
    if submitted:
        if _unlock_private_area(password):
            st.success("Privater Bereich entsperrt.")
            st.rerun()
    err = st.session_state.get("private_area_error", "")
    if err:
        st.error(err)
    return False

def _add_recent_ticker(ticker: str) -> None:
    if not ticker:
        return
    _init_workspace_state()
    recents = [ticker] + [t for t in st.session_state["recent_tickers"] if t != ticker]
    st.session_state["recent_tickers"] = recents[:12]
    _sync_workspace()

def _add_watchlist_ticker(ticker: str) -> None:
    if not ticker:
        return
    _init_workspace_state()
    cur = [t for t in st.session_state["watchlist"] if t != ticker]
    cur.insert(0, ticker)
    st.session_state["watchlist"] = cur[:25]
    _add_recent_ticker(ticker)
    _sync_workspace()

def _remove_watchlist_ticker(ticker: str) -> None:
    _init_workspace_state()
    st.session_state["watchlist"] = [t for t in st.session_state["watchlist"] if t != ticker]
    _sync_workspace()

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

def _render_ticker_picker(key_prefix: str, label: str, placeholder: str = "NVDA oder Nvidia", show_quick: bool = True):
    _init_workspace_state()
    if show_quick:
        quick = []
        for source in [st.session_state.get("recent_tickers", []), st.session_state.get("watchlist", []), DEFAULT_FAVORITES]:
            for ticker in source:
                if ticker not in quick:
                    quick.append(ticker)
        quick = quick[:8]
        if quick:
            cols = st.columns(min(4, len(quick)))
            for i, ticker in enumerate(quick):
                with cols[i % len(cols)]:
                    if st.button(ticker, key=f"{key_prefix}_quick_{ticker}", use_container_width=True):
                        st.session_state[f"{key_prefix}_query"] = ticker
    query = st.text_input(label, value=st.session_state.get(f"{key_prefix}_query", ""), placeholder=placeholder, key=f"{key_prefix}_query")
    query = (query or "").strip()
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
    return {
        "index_name": selected,
        "index_date": latest_date,
        "vix_date": vix_date,
        "store_label": _get_store_label(store),
        "nyse_refresh": _get_cache_metadata(store, "last_refresh_at", "") or _get_cache_metadata(store, "cache_prices_last_write_at", ""),
        "coverage": _get_cache_metadata(store, "last_refresh_loaded_universe", ""),
        "requested": _get_cache_metadata(store, "last_refresh_requested_universe", ""),
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
    ss_date = L.get('Startschuss_Date')
    ss_low = L.get('Startschuss_Low', np.nan)
    floor = L.get('Floor_Mark', np.nan)
    if phase == 'gelb':
        if anchor and pd.notna(ss_low):
            return f'Trendwende-Ampel: GELB — Startschuss seit {ss_date or anchor} · Startschuss-Tief {float(ss_low):,.2f}'
        return 'Trendwende-Ampel: GELB — Startschuss aktiv'
    if phase == 'gruen':
        if pd.notna(ss_low):
            return f'Trendwende-Ampel: GRÜN — Startschuss vom {ss_date or "—"} bestätigt · Absicherung über {float(ss_low):,.2f}'
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
    if phase == "rot":
        return "Defensiv", "bad", "Ampel rot. Risiko reduzieren, keine aggressiven Neueinstiege und bestehende Positionen kritisch prüfen."
    if phase not in {"gruen", "aufwaertstrend"} and (warning_count >= 4 or breadth_mode == "schutz" or vol_regime == "stress"):
        return "Defensiv", "bad", "Umfeld defensiv. Risiko reduzieren, nur selektiv agieren und bestehende Positionen enger managen."
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


def _is_ma_order_ok(ema21, sma50, sma200, tol: float = 1e-8) -> bool:
    vals = [ema21, sma50, sma200]
    if any(pd.isna(v) for v in vals):
        return False
    return (float(ema21) >= float(sma50) - tol) and (float(sma50) >= float(sma200) - tol)

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

def _build_market_changes(df: pd.DataFrame, selected: str, wc: int, vol_dashboard: pd.DataFrame | None = None, breadth_label: str = ""):
    changes = []
    if df is None or len(df) < 2:
        return changes
    prev = df.iloc[-2]
    latest = df.iloc[-1]
    price_delta = latest.get("Pct_Change", np.nan)
    if not np.isnan(price_delta):
        changes.append({"title": "Heute", "value": f"{selected}: {price_delta:+.2f}%", "detail": f"Schlusskurs {latest['Close']:,.2f}"})
    dist_prev = int(prev.get("Dist_Count_25", 0))
    dist_now = int(latest.get("Dist_Count_25", 0))
    delta = dist_now - dist_prev
    delta_txt = f"{delta:+d}" if delta else "unverändert"
    changes.append({"title": "Distribution", "value": f"{dist_now} aktive Dist.-Tage", "detail": f"Gegenüber gestern: {delta_txt}"})
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
        val = f"VIX {vl.get('VIX_Close', np.nan):.1f}" if pd.notna(vl.get("VIX_Close", np.nan)) else "VIX n/a"
        changes.append({"title": "Volatilität", "value": val, "detail": change_detail})
    if breadth_label:
        changes.append({"title": "Breite", "value": breadth_label, "detail": "Equal-Weight als Bestätigung des Indextrends"})
    return changes[:4]

def _render_change_cards(changes):
    if not changes:
        return
    cols = st.columns(len(changes))
    for col, item in zip(cols, changes):
        with col:
            st.markdown(
                f'<div class="change-card"><div class="change-title">{item["title"]}</div><div class="change-value">{item["value"]}</div><div class="change-detail">{item["detail"]}</div></div>',
                unsafe_allow_html=True,
            )

def _render_hero_card(mode: str, tone: str, reasons: list[str], action: str, freshness: dict):
    tone_cls = {"good": "hero-good", "warn": "hero-warn", "bad": "hero-bad"}.get(tone, "hero-warn")
    bullets = "".join(f"<li>{r}</li>" for r in reasons)
    nyse_txt = ""
    if freshness.get("coverage") and freshness.get("requested"):
        nyse_txt = f" · NYSE/Nasdaq-Cache {freshness['coverage']}/{freshness['requested']}"
    refresh_txt = ""
    if freshness.get("nyse_refresh"):
        refresh_txt = f" · Tiefenanalyse aktualisiert {_elapsed_text(freshness['nyse_refresh'])}"
    st.markdown(
        f'<div class="summary-hero"><div class="hero-title">Marktmodus: {mode}</div><div class="hero-subtitle">Stand Index {freshness.get("index_date","—")} · VIX {freshness.get("vix_date","—")} · Speicher {freshness.get("store_label","—")}{nyse_txt}{refresh_txt}</div><ul style="margin:0 0 0 1rem;padding:0;line-height:1.5;">{bullets}</ul><div class="hero-action {tone_cls}">Konsequenz: {action}</div></div>',
        unsafe_allow_html=True,
    )

def _render_market_glossary(keys):
    items = []
    for key in keys:
        text = METRIC_GLOSSARY.get(key)
        if text:
            items.append(f"<strong>{key}</strong> — {text}")
    if items:
        st.markdown('<div class="kpi-explainer">' + "<br>".join(items) + "</div>", unsafe_allow_html=True)


def _render_kpi_card(label: str, value: str, delta: str = "", tone: str = "neutral", caption: str = "") -> None:
    tone_cls = {"good": " kpi-good", "warn": " kpi-warn", "bad": " kpi-bad"}.get(tone, "")
    delta_color = {"good": "var(--good)", "warn": "var(--warn)", "bad": "var(--bad)"}.get(tone, "var(--muted)")
    delta_html = f'<div class="kpi-delta" style="color:{delta_color};">{delta}</div>' if delta else ""
    caption_html = f'<div class="kpi-caption">{caption}</div>' if caption else ""
    st.markdown(
        f'<div class="kpi-card{tone_cls}">'
        f'<div class="kpi-label">{label}</div>'
        f'<div class="kpi-value">{value}</div>'
        f'{delta_html}'
        f'{caption_html}'
        f'</div>',
        unsafe_allow_html=True,
    )


def _render_section_header(icon: str, title: str, subtitle: str = "") -> None:
    sub_html = f'<span class="section-header-sub">{subtitle}</span>' if subtitle else ""
    st.markdown(
        f'<div class="section-header">'
        f'<span class="section-header-icon">{icon}</span>'
        f'<span class="section-header-title">{title}</span>'
        f'{sub_html}'
        f'</div>',
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
    price = float(latest["Close"])
    buy_price = float(position.get("buy_price_usd") or position.get("buy_price") or 0)
    pnl = ((price / buy_price) - 1) * 100 if buy_price else np.nan
    sma50 = df["Close"].rolling(50).mean().iloc[-1]
    status = "OK"
    detail = f"Aktuell ${price:,.2f}"
    if not np.isnan(pnl) and pnl < -7:
        status = "Stop-Loss"
        detail = f"{pnl:.1f}% seit Kauf"
    elif not np.isnan(sma50) and price < sma50:
        status = "Unter 50-SMA"
        detail = f"${price:,.2f} unter ${sma50:,.2f}"
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
    }
    for key in numeric_fields:
        fallback = _default_portfolio_settings().get(key, 0.0)
        try:
            settings[key] = float(settings.get(key, fallback))
        except Exception:
            settings[key] = fallback
    settings["curve_start_date"] = str(settings.get("curve_start_date", "") or "").strip()
    rs_source = str(settings.get("rs_rating_source", "computed") or "computed").strip().lower()
    settings["rs_rating_source"] = "csv_latest" if rs_source == "csv_latest" else "computed"
    return settings


def _get_rs_rating_source_setting() -> str:
    settings = _get_portfolio_settings()
    source = str(settings.get("rs_rating_source", "computed") or "computed").strip().lower()
    return "csv_latest" if source == "csv_latest" else "computed"

def _save_portfolio_settings(settings: dict) -> None:
    merged = dict(_default_portfolio_settings())
    if isinstance(settings, dict):
        merged.update(settings)
    st.session_state["portfolio_settings"] = merged
    _sync_workspace()

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
    return _safe_float(position.get("buy_price_usd") or position.get("buy_price"), np.nan)

def _position_stop_pct(position: dict) -> float:
    val = _safe_float(position.get("stop_pct"), np.nan)
    return val if not np.isnan(val) and val > 0 else np.nan

def _position_stop_price(position: dict) -> float:
    manual = _safe_float(position.get("stop_price"), np.nan)
    if not np.isnan(manual) and manual > 0:
        return manual
    entry = _position_entry_price(position)
    stop_pct = _position_stop_pct(position)
    if entry > 0 and not np.isnan(stop_pct):
        return entry * (1 - stop_pct / 100)
    return np.nan

def _price_to_usd(price: float, currency: str, trade_date) -> tuple[float, float | None]:
    value = float(price or 0.0)
    if str(currency).upper() != "EUR":
        return value, None
    rate = None
    try:
        fx = yf.Ticker("EURUSD=X").history(start=pd.Timestamp(trade_date) - timedelta(days=5), end=pd.Timestamp(trade_date) + timedelta(days=3))
        if fx is not None and len(fx) > 0:
            rate = float(fx["Close"].iloc[-1])
    except Exception:
        rate = None
    if rate is None:
        rate = 1.08
    return value * float(rate), float(rate)

def _normalize_single_ticker(value: str) -> str:
    t = str(value or "").strip().upper()
    t = t.replace(".", "-").replace("/", "-").replace(" ", "")
    return t


def _symbol_variants(symbol: str) -> list[str]:
    base = _normalize_single_ticker(symbol)
    if not base:
        return []
    variants = [base]
    dotted = _normalize_single_ticker(base.replace("-", "."))
    dashed = _normalize_single_ticker(base.replace(".", "-"))
    for cand in (dotted, dashed):
        if cand and cand not in variants:
            variants.append(cand)
    try:
        lookup = _search_yahoo_symbol_candidates(base)
        for cand in lookup.get("candidates", [])[:6]:
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
        close.index = pd.to_datetime(close.index).normalize()
        close = close[~close.index.duplicated(keep="last")].sort_index()
        close = close[(close.index >= start_ts) & (close.index <= end_ts)]
        if close.empty:
            continue
        out[sym] = close
    return out


def _beta_from_close_series(close: pd.Series, benchmark_close: pd.Series, window: int = 120) -> float:
    try:
        if close is None or benchmark_close is None or len(close) < 20 or len(benchmark_close) < 20:
            return np.nan
        joined = pd.concat(
            [close.pct_change().rename("asset"), benchmark_close.pct_change().rename("bench")],
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

    def _fallback_payload(name=None, sector="", industry="", price=np.nan, atr_pct=np.nan, beta=np.nan):
        return {
            "ticker": ticker,
            "name": name or ticker,
            "sector": sector or "",
            "industry": industry or "",
            "price": price,
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
            atr_pct=atr_pct,
            beta=beta,
        )

    series = _fetch_close_history(ticker, datetime.now() - timedelta(days=260), datetime.now())
    latest_price = _safe_float(series.iloc[-1], np.nan) if len(series) else np.nan
    atr_pct = np.nan
    if len(series) >= 22 and not np.isnan(latest_price) and latest_price > 0:
        atr_pct = float(series.pct_change().abs().rolling(21).mean().iloc[-1] * 100 * 1.6)
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
            atr_pct = float(pd.to_numeric(frame["Close"], errors="coerce").pct_change().abs().rolling(21).mean().iloc[-1] * 100 * 1.6)
        beta = _beta_from_close_series(pd.to_numeric(frame["Close"], errors="coerce"), spx_close)
        out[ticker] = {
            "ticker": ticker,
            "name": ticker,
            "sector": "",
            "industry": "",
            "price": latest_price,
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
        price = _safe_float(metrics.get("price"), np.nan)
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
            "currency": pos.get("currency", "USD"),
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
        max_weight = (target_risk_contribution / score) if not np.isnan(score) and score > 0 else np.nan
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
            "currency": "USD",
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

def _portfolio_health_messages(summary: dict, positions_df: pd.DataFrame, settings: dict) -> list[tuple[str, str]]:
    messages = []
    count = int(summary.get("tracked_count", 0))
    if count < 8:
        messages.append(("warning", f"Du hältst aktuell {count} echte Depotpositionen. Kapitel 7.2 empfiehlt meist einen Korridor von 8 bis 12 Aktien."))
    elif count > 12:
        messages.append(("warning", f"Du hältst aktuell {count} Positionen. Das liegt über dem empfohlenen Korridor von 8 bis 12 Aktien und macht das Depot schwerer steuerbar."))
    else:
        messages.append(("success", f"Mit {count} Positionen liegst du im empfohlenen Korridor von 8 bis 12 Aktien."))

    max_loss = _safe_float(summary.get("max_depot_loss_pct"), np.nan)
    low = _safe_float(settings.get("max_depot_loss_low"), 8.0)
    high = _safe_float(settings.get("max_depot_loss_high"), 12.0)
    if not np.isnan(max_loss):
        if max_loss < low:
            messages.append(("info", f"Der modellierte maximale Depotverlust liegt bei {max_loss:.1f}%. Das ist defensiver als dein Zielkorridor von {low:.0f} bis {high:.0f}%."))
        elif max_loss > high:
            messages.append(("warning", f"Der modellierte maximale Depotverlust liegt bei {max_loss:.1f}% und damit über deinem Zielkorridor von {low:.0f} bis {high:.0f}%."))
        else:
            messages.append(("success", f"Der modellierte maximale Depotverlust liegt mit {max_loss:.1f}% im Zielkorridor von {low:.0f} bis {high:.0f}%."))

    atr_pct = _safe_float(summary.get("portfolio_atr_pct"), np.nan)
    if not np.isnan(atr_pct):
        if atr_pct > 4:
            messages.append(("warning", f"Deine Portfolio ATR liegt bei {atr_pct:.2f}%. Im Buch gilt ein Wert über 4% als sehr aggressiv."))
        elif atr_pct >= 2.5:
            messages.append(("info", f"Deine Portfolio ATR liegt bei {atr_pct:.2f}%. Das ist ordentlich Bewegung im Depot, aber noch kontrollierbar."))
        else:
            messages.append(("success", f"Deine Portfolio ATR liegt bei {atr_pct:.2f}%. Das wirkt aktuell vergleichsweise ruhig."))

    balancer = _safe_float(summary.get("beta_balancer"), np.nan)
    if not np.isnan(balancer):
        if balancer >= 2.5:
            messages.append(("warning", f"Dein Beta Balancer liegt bei {balancer:.2f}. Das entspricht einem sehr dynamischen, schwankungsintensiven Depot."))
        elif balancer >= 1.5:
            messages.append(("info", f"Dein Beta Balancer liegt bei {balancer:.2f}. Das ist offensiv, aber noch steuerbar."))
        else:
            messages.append(("success", f"Dein Beta Balancer liegt bei {balancer:.2f}. Das ist näher am Marktrisiko des S&P 500."))

    target_rc = _safe_float(settings.get("target_risk_contribution"), 0.20)
    if not positions_df.empty and "risk_contribution" in positions_df:
        over = positions_df[positions_df["risk_contribution"] > target_rc]
        if len(over):
            tickers = ", ".join(over["ticker"].head(4).tolist())
            messages.append(("warning", f"Diese Positionen liegen über dem Ziel-Risikobeitrag von {target_rc:.2f}: {tickers}."))
    return messages

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
    curve["portfolio_index_sma10"] = curve["portfolio_index"].rolling(10, min_periods=1).mean()
    curve["portfolio_index_sma21"] = curve["portfolio_index"].rolling(21, min_periods=1).mean()
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

def _render_portfolio_72_area():
    _init_workspace_state()
    settings = _get_portfolio_settings()
    all_positions = st.session_state.get("positions", [])
    snapshot_df, summary = _build_portfolio_snapshot(
        all_positions,
        cash_balance=_safe_float(settings.get("cash_balance"), 0.0),
        target_risk_contribution=_safe_float(settings.get("target_risk_contribution"), 0.20),
    )
    tracked_positions = _portfolio_positions_only(all_positions)
    current_total_value = _safe_float(summary.get("total_value"), 0.0)

    st.markdown("### 💼 Depot nach Kapitel 7.2")
    st.caption("Umgesetzt sind der Korridor von 8 bis 12 Aktien, die Positionsgröße über den maximalen Verlust, die Depotkurve mit Geldflüssen, die Einzelperformance, das ATR gewichtete Portfolio, der Beta Balancer und der maximale Depotverlust.")

    set_cols = st.columns(5)
    with set_cols[0]:
        cash_balance = st.number_input("Cash / freie Liquidität", min_value=0.0, value=float(settings.get("cash_balance", 0.0)), step=100.0, key="pf_cash_balance")
    with set_cols[1]:
        risk_per_position_pct = st.number_input("Max. Verlust je Idee %", min_value=0.1, max_value=5.0, value=float(settings.get("risk_per_position_pct", 1.0)), step=0.1, key="pf_risk_pct")
    with set_cols[2]:
        target_risk_contribution = st.number_input("Ziel Risikobeitrag", min_value=0.05, max_value=0.50, value=float(settings.get("target_risk_contribution", 0.20)), step=0.01, key="pf_target_rc")
    with set_cols[3]:
        max_loss_low = st.number_input("Untergrenze Max.-Depotverlust %", min_value=1.0, max_value=30.0, value=float(settings.get("max_depot_loss_low", 8.0)), step=0.5, key="pf_max_loss_low")
    with set_cols[4]:
        max_loss_high = st.number_input("Obergrenze Max.-Depotverlust %", min_value=1.0, max_value=30.0, value=float(settings.get("max_depot_loss_high", 12.0)), step=0.5, key="pf_max_loss_high")
    if st.button("Portfolio-Regler speichern", use_container_width=True, key="pf_save_settings"):
        _save_portfolio_settings({
            "cash_balance": float(cash_balance),
            "risk_per_position_pct": float(risk_per_position_pct),
            "target_risk_contribution": float(target_risk_contribution),
            "max_depot_loss_low": float(min(max_loss_low, max_loss_high)),
            "max_depot_loss_high": float(max(max_loss_low, max_loss_high)),
        })
        st.success("Portfolio-Regler gespeichert.")
        st.rerun()

    edit_col, calc_col = st.columns([1.15, 1.0])
    with edit_col:
        st.markdown("#### Position erfassen oder aktualisieren")
        pos_ticker = _render_ticker_picker("portfolio_depot", "Ticker oder Firmenname suchen", "NVDA oder Nvidia", show_quick=False)
        selected_pos = next((p for p in all_positions if p.get("ticker") == pos_ticker), None) if pos_ticker else None
        entry_default = float((selected_pos or {}).get("buy_price", 1.0) or 1.0)
        shares_default = float((selected_pos or {}).get("shares", 0.0) or 0.0)
        stop_pct_default = float((selected_pos or {}).get("stop_pct", 7.0) or 7.0)
        price_col, shares_col, stop_col = st.columns(3)
        with price_col:
            buy_price = st.number_input("Einstand", min_value=0.01, value=entry_default, step=0.01, key="pf_buy_price")
        with shares_col:
            shares = st.number_input("Stückzahl", min_value=0.0, value=shares_default, step=1.0, key="pf_shares")
        with stop_col:
            stop_pct = st.number_input("Stoppabstand %", min_value=0.1, max_value=50.0, value=stop_pct_default, step=0.1, key="pf_stop_pct")
        date_col, curr_col, note_col = st.columns([1, 0.8, 1.4])
        with date_col:
            try:
                default_date = pd.Timestamp((selected_pos or {}).get("buy_date")).date() if (selected_pos or {}).get("buy_date") else datetime.now(timezone.utc).date()
            except Exception:
                default_date = datetime.now(timezone.utc).date()
            buy_date = st.date_input("Kaufdatum", value=default_date, key="pf_buy_date")
        with curr_col:
            curr_default = (selected_pos or {}).get("currency", "USD")
            currency = st.selectbox("Währung", ["USD", "EUR"], index=0 if curr_default == "USD" else 1, key="pf_currency")
        with note_col:
            note = st.text_input("Notiz", value=(selected_pos or {}).get("note", ""), key="pf_note")

        stop_price_preview = float(buy_price) * (1 - float(stop_pct) / 100)
        st.caption(f"Abgeleiteter Stoppkurs: {stop_price_preview:,.2f}")

        if st.button("Depotposition speichern", use_container_width=True, key="pf_save_position", disabled=not bool(pos_ticker)):
            buy_price_usd, eur_usd_rate = _price_to_usd(float(buy_price), currency, buy_date)
            previous_shares = _safe_float((selected_pos or {}).get("shares"), 0.0) if selected_pos else 0.0
            delta_shares = max(float(shares) - previous_shares, 0.0)
            buy_delta_value = delta_shares * float(buy_price_usd)
            _upsert_position({
                "ticker": pos_ticker,
                "buy_price": float(buy_price),
                "buy_price_usd": float(buy_price_usd),
                "buy_date": str(buy_date),
                "currency": currency,
                "shares": float(shares),
                "stop_pct": float(stop_pct),
                "stop_price": float(stop_price_preview),
                "note": note,
            })
            if buy_delta_value > 0:
                new_cash = _adjust_cash_balance(-buy_delta_value)
                if buy_delta_value > float(settings.get("cash_balance", 0.0)):
                    st.warning(f"Kaufvolumen ({buy_delta_value:,.2f} USD) überstieg den verfügbaren Cashbestand. Cash wurde auf {new_cash:,.2f} USD begrenzt.")
            st.success(f"{pos_ticker} gespeichert.")
            st.rerun()

    with calc_col:
        st.markdown("#### Positionsgrößen-Rechner")
        st.caption("Kapitel 7.2 empfiehlt, pro Idee meist nicht mehr als 1% des Depots zu riskieren.")
        calculator_entry = _safe_float(st.session_state.get("pf_buy_price"), np.nan)
        calculator_stop_pct = _safe_float(st.session_state.get("pf_stop_pct"), np.nan)
        risk_budget = current_total_value * (float(risk_per_position_pct) / 100) if current_total_value > 0 else np.nan
        risk_per_share = calculator_entry * (calculator_stop_pct / 100) if not np.isnan(calculator_entry) and not np.isnan(calculator_stop_pct) else np.nan
        max_shares = int(np.floor(risk_budget / risk_per_share)) if not np.isnan(risk_budget) and not np.isnan(risk_per_share) and risk_per_share > 0 else 0
        max_position_value = max_shares * calculator_entry if max_shares > 0 and not np.isnan(calculator_entry) else np.nan
        st.metric("Modelliertes Depot", f"{current_total_value:,.0f}" if current_total_value > 0 else "—")
        st.metric("Risikobudget je Idee", f"{risk_budget:,.0f}" if not np.isnan(risk_budget) else "—")
        st.metric("Risiko pro Aktie", f"{risk_per_share:,.2f}" if not np.isnan(risk_per_share) else "—")
        st.metric("Maximale Stückzahl", f"{max_shares:,}" if max_shares else "—")
        st.metric("Maximale Positionsgröße", f"{max_position_value:,.0f}" if not np.isnan(max_position_value) else "—")

        if pos_ticker:
            metrics = _portfolio_symbol_metrics(pos_ticker)
            score = np.nan
            if not np.isnan(_safe_float(metrics.get("beta"), np.nan)) and not np.isnan(_safe_float(metrics.get("atr_pct"), np.nan)) and not np.isnan(_safe_float(summary.get("spx_atr_pct"), np.nan)) and _safe_float(summary.get("spx_atr_pct"), np.nan) > 0:
                score = 0.60 * _safe_float(metrics.get("beta"), np.nan) + 0.40 * (_safe_float(metrics.get("atr_pct"), np.nan) / _safe_float(summary.get("spx_atr_pct"), np.nan))
            balancer_weight = (float(target_risk_contribution) / score) if not np.isnan(score) and score > 0 else np.nan
            balancer_value = current_total_value * balancer_weight if current_total_value > 0 and not np.isnan(balancer_weight) else np.nan
            balancer_shares = int(np.floor(balancer_value / _safe_float(metrics.get("price"), np.nan))) if not np.isnan(balancer_value) and _safe_float(metrics.get("price"), np.nan) > 0 else 0
            st.markdown("---")
            st.metric("ATR % der Aktie", f"{_safe_float(metrics.get('atr_pct'), np.nan):.2f}%" if not np.isnan(_safe_float(metrics.get("atr_pct"), np.nan)) else "—")
            st.metric("Beta", f"{_safe_float(metrics.get('beta'), np.nan):.2f}" if not np.isnan(_safe_float(metrics.get("beta"), np.nan)) else "—")
            st.metric("Beta-Balancer-Score", f"{score:.2f}" if not np.isnan(score) else "—")
            st.metric("Max. Gewicht via Balancer", f"{balancer_weight*100:.1f}%" if not np.isnan(balancer_weight) else "—")
            st.metric("Max. Stück via Balancer", f"{balancer_shares:,}" if balancer_shares else "—")

    st.markdown("#### Transaktionen & Cash-Management")
    txn_col_sell, txn_col_flow = st.columns([1.2, 1.0])
    with txn_col_sell:
        st.caption("Verkäufe buchen: ganz oder teilweise. Verkaufserlöse werden automatisch dem Cash gutgeschrieben.")
        sell_options = [p.get("ticker", "") for p in tracked_positions if p.get("ticker")]
        sell_ticker = st.selectbox("Position verkaufen", options=[""] + sell_options, key="pf_sell_ticker")
        selected_sell = next((p for p in tracked_positions if p.get("ticker") == sell_ticker), None) if sell_ticker else None
        max_sell_shares = float(_safe_float((selected_sell or {}).get("shares"), 0.0))
        sell_col1, sell_col2, sell_col3 = st.columns(3)
        with sell_col1:
            sell_shares = st.number_input("Zu verkaufende Stück", min_value=0.0, max_value=max_sell_shares if max_sell_shares > 0 else 0.0, value=max_sell_shares if max_sell_shares > 0 else 0.0, step=1.0, key="pf_sell_shares")
        with sell_col2:
            sell_price = st.number_input("Verkaufspreis", min_value=0.0, value=float(_safe_float((selected_sell or {}).get("current_price"), 0.0)), step=0.01, key="pf_sell_price")
        with sell_col3:
            sell_currency = st.selectbox("Währung Verkauf", ["USD", "EUR"], index=0, key="pf_sell_currency")
        sell_date = st.date_input("Verkaufsdatum", value=datetime.now(timezone.utc).date(), key="pf_sell_date")
        if st.button("Verkauf buchen", use_container_width=True, key="pf_sell_book", disabled=not bool(selected_sell) or sell_shares <= 0 or sell_price <= 0):
            if not selected_sell:
                st.warning("Bitte zuerst eine Position wählen.")
            elif sell_shares > max_sell_shares:
                st.error("Die Verkaufsmenge ist größer als die vorhandene Stückzahl.")
            else:
                sell_price_usd, _ = _price_to_usd(float(sell_price), sell_currency, sell_date)
                proceeds = float(sell_shares) * float(sell_price_usd)
                remaining = max(max_sell_shares - float(sell_shares), 0.0)
                if remaining <= 0:
                    _remove_position(selected_sell.get("ticker", ""))
                else:
                    updated = dict(selected_sell)
                    updated["shares"] = remaining
                    _upsert_position(updated)
                _adjust_cash_balance(proceeds)
                st.success(f"Verkauf gebucht. Cash erhöht um {proceeds:,.2f} USD.")
                st.rerun()

    with txn_col_flow:
        st.caption("Externe Cash-Flows (Ein-/Auszahlungen) beeinflussen den Depotindex zeitgewichtet.")
        flow_date = st.date_input("Cash-Flow Datum", value=datetime.now(timezone.utc).date(), key="pf_flow_date")
        flow_amount = st.number_input("Cash-Flow Betrag", min_value=0.0, value=0.0, step=100.0, key="pf_flow_amount")
        flow_note = st.text_input("Cash-Flow Notiz", value="", key="pf_flow_note")
        flow_act1, flow_act2 = st.columns(2)
        with flow_act1:
            if st.button("Einzahlung buchen", use_container_width=True, key="pf_flow_deposit", disabled=flow_amount <= 0):
                _append_cash_flow_entry(flow_date, "deposit", flow_amount, flow_note)
                _adjust_cash_balance(float(flow_amount))
                st.success("Einzahlung erfasst.")
                st.rerun()
        with flow_act2:
            if st.button("Auszahlung buchen", use_container_width=True, key="pf_flow_withdrawal", disabled=flow_amount <= 0):
                _append_cash_flow_entry(flow_date, "withdrawal", flow_amount, flow_note)
                _adjust_cash_balance(-float(flow_amount))
                st.success("Auszahlung erfasst.")
                st.rerun()

    metric_cols = st.columns(6)
    metric_map = [
        ("Aktive Depotpositionen", f"{int(summary.get('tracked_count', 0))}"),
        ("Depotwert", f"{summary.get('total_value', 0):,.0f}" if summary.get("total_value", 0) else "—"),
        ("Cashquote", f"{summary.get('cash_ratio', 0)*100:.1f}%" if not np.isnan(_safe_float(summary.get("cash_ratio"), np.nan)) else "—"),
        ("Portfolio ATR", f"{summary.get('portfolio_atr_pct', np.nan):.2f}%" if not np.isnan(_safe_float(summary.get("portfolio_atr_pct"), np.nan)) else "—"),
        ("Beta Balancer", f"{summary.get('beta_balancer', np.nan):.2f}" if not np.isnan(_safe_float(summary.get("beta_balancer"), np.nan)) else "—"),
        ("Max. Depotverlust", f"{summary.get('max_depot_loss_pct', np.nan):.2f}%" if not np.isnan(_safe_float(summary.get("max_depot_loss_pct"), np.nan)) else "—"),
    ]
    for col, (label, value) in zip(metric_cols, metric_map):
        with col:
            st.metric(label, value)

    for level, msg in _portfolio_health_messages(summary, snapshot_df, {
        "target_risk_contribution": float(target_risk_contribution),
        "max_depot_loss_low": float(max_loss_low),
        "max_depot_loss_high": float(max_loss_high),
    }):
        getattr(st, level)(msg)

    if not snapshot_df.empty:
        display_df = snapshot_df.copy()
        display_df["Gewicht %"] = display_df["weight"] * 100
        display_df["P&L %"] = display_df["pnl_pct"]
        display_df["P&L $"] = display_df["pnl_abs"]
        display_df["ATR %"] = display_df["atr_pct"]
        display_df["Score"] = display_df["score"]
        display_df["Risikobeitrag"] = display_df["risk_contribution"]
        display_df["Max. Gewicht %"] = display_df["max_weight"] * 100
        display_df["Max. Wert"] = display_df["max_position_value"]
        display_df["Abstand Stop %"] = display_df["stop_distance_pct"]
        display_df["Positionsrisiko"] = display_df["position_risk_abs"]
        display_df = display_df[[
            "ticker", "shares", "entry", "current_price", "current_value", "Gewicht %", "stop_pct", "stop_price",
            "Abstand Stop %", "P&L %", "P&L $", "ATR %", "beta", "Score", "Risikobeitrag", "Max. Gewicht %", "Max. Wert", "Positionsrisiko", "note"
        ]].rename(columns={
            "ticker": "Ticker",
            "shares": "Stück",
            "entry": "Einstand",
            "current_price": "Aktuell",
            "current_value": "Wert",
            "stop_pct": "Stopp %",
            "stop_price": "Stoppkurs",
            "beta": "Beta",
            "note": "Notiz",
        })
        st.markdown("#### Einzelperformance und Risiko-Ranking")
        st.dataframe(display_df.round(2), use_container_width=True, hide_index=True)

        worst_source = snapshot_df[snapshot_df["is_cash"] == False] if "is_cash" in snapshot_df else snapshot_df
        worst = worst_source.sort_values("pnl_pct", ascending=True, na_position="last").head(3)[["ticker", "pnl_pct", "risk_contribution"]].copy()
        if len(worst):
            worst["pnl_pct"] = worst["pnl_pct"].round(2)
            worst["risk_contribution"] = worst["risk_contribution"].round(3)
            st.markdown("#### Verkaufskandidaten nach relativer Schwäche")
            st.dataframe(worst.rename(columns={"ticker": "Ticker", "pnl_pct": "P&L %", "risk_contribution": "Risikobeitrag"}), use_container_width=True, hide_index=True)
    else:
        st.info("Für das Depotcockpit werden nur Positionen mit Stückzahl größer 0 berücksichtigt.")

    st.markdown("#### Depotkurve")
    st.caption("Die Kurve startet erst mit Klick auf „Kurve starten“. Danach läuft sie dauerhaft ab diesem Starttag weiter.")
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
            if st.button("Kurve starten", use_container_width=True, key="pf_auto_curve_start_today"):
                st.session_state["pf_auto_curve_start_force_today"] = True
                st.rerun()
        else:
            st.success(f"Kurve aktiv seit {saved_curve_start.strftime('%Y-%m-%d')}")
    with auto_col2:
        if saved_curve_start is not None:
            if st.button("Kurve neu starten (Heute)", use_container_width=True, key="pf_auto_curve_restart_today"):
                st.session_state["pf_auto_curve_start_force_today"] = True
                st.rerun()

    auto_start = saved_curve_start
    cash_flows = st.session_state.get("portfolio_cash_flows", []) if isinstance(st.session_state.get("portfolio_cash_flows", []), list) else []

    auto_curve = pd.DataFrame()
    if auto_start is not None:
        auto_curve = _build_reconstructed_portfolio_curve(all_positions, float(settings.get("cash_balance", 0.0)), auto_start, auto_end, cash_flows=cash_flows)
    if auto_start is None:
        st.info("Depotkurve ist noch nicht gestartet. Klicke auf „Kurve starten“, um den ersten Starttag festzulegen.")
    elif not auto_curve.empty:
        fig_auto = go.Figure()
        fig_auto.add_trace(go.Scatter(x=auto_curve["date"], y=auto_curve["portfolio_index"], mode="lines", name="Depotindex"))
        fig_auto.add_trace(go.Scatter(x=auto_curve["date"], y=auto_curve["portfolio_index_sma10"], mode="lines", name="10-Tage SMA", line=dict(width=1.6, dash="dot")))
        fig_auto.add_trace(go.Scatter(x=auto_curve["date"], y=auto_curve["portfolio_index_sma21"], mode="lines", name="21-Tage SMA", line=dict(width=1.6, dash="dash")))
        if "sp500_index" in auto_curve and auto_curve["sp500_index"].notna().any():
            fig_auto.add_trace(go.Scatter(x=auto_curve["date"], y=auto_curve["sp500_index"], mode="lines", name="S&P 500 Index"))
        fig_auto.update_layout(
            template="plotly_dark",
            paper_bgcolor="#0f172a",
            plot_bgcolor="#0f172a",
            height=380,
            margin=dict(l=10, r=10, t=20, b=10),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
            yaxis=dict(title="Index (Start = 100)", gridcolor="#1e293b"),
            xaxis=dict(title="", gridcolor="#1e293b"),
        )
        st.plotly_chart(fig_auto, use_container_width=True, key="pf_curve_chart_auto")
        auto_display = auto_curve[["date", "depot_value", "deposit", "withdrawal", "portfolio_index", "portfolio_index_sma10", "portfolio_index_sma21", "sp500_index"]].copy()
        auto_display["date"] = auto_display["date"].dt.strftime("%Y-%m-%d")
        auto_display = auto_display.rename(columns={
            "date": "Datum",
            "depot_value": "Depotwert",
            "deposit": "Einzahlung",
            "withdrawal": "Auszahlung",
            "portfolio_index": "Depotindex",
            "portfolio_index_sma10": "SMA 10",
            "portfolio_index_sma21": "SMA 21",
            "sp500_index": "S&P 500",
        })
        st.dataframe(auto_display.round(2), use_container_width=True, hide_index=True)
    else:
        st.info("Für die Depotkurve fehlen aktuell verwertbare Kursdaten oder Positionen mit Stückzahl.")

    if cash_flows:
        flow_df = pd.DataFrame(cash_flows).copy()
        flow_df["__idx"] = flow_df.index
        flow_df["date"] = pd.to_datetime(flow_df["date"], errors="coerce")
        flow_df = flow_df.dropna(subset=["date"]).sort_values("date", ascending=False).reset_index(drop=True)
        flow_df["Typ"] = flow_df["type"].map({"deposit": "Einzahlung", "withdrawal": "Auszahlung"}).fillna("—")
        flow_df["Datum"] = flow_df["date"].dt.strftime("%Y-%m-%d")
        flow_df["Betrag"] = pd.to_numeric(flow_df.get("amount", 0), errors="coerce").fillna(0.0)
        flow_df["Notiz"] = flow_df.get("note", "").astype(str)
        st.markdown("##### Erfasste Ein- und Auszahlungen")
        st.dataframe(flow_df[["Datum", "Typ", "Betrag", "Notiz"]].round(2), use_container_width=True, hide_index=True)
        delete_idx = st.selectbox("Cash-Flow löschen", options=[""] + [f"{i}: {row['Datum']} · {row['Typ']} · {row['Betrag']:,.2f}" for i, row in flow_df.iterrows()], key="pf_flow_delete_sel")
        if delete_idx and st.button("Cash-Flow löschen", use_container_width=True, key="pf_flow_delete_btn"):
            idx = int(str(delete_idx).split(":", 1)[0])
            row = flow_df.iloc[idx]
            if str(row.get("Typ")) == "Einzahlung":
                _adjust_cash_balance(-float(row.get("Betrag", 0.0)))
            elif str(row.get("Typ")) == "Auszahlung":
                _adjust_cash_balance(float(row.get("Betrag", 0.0)))
            _remove_cash_flow_entry(int(row.get("__idx", -1)))
            st.success("Cash-Flow gelöscht.")
            st.rerun()


def _render_workspace_sidebar():
    with st.sidebar:
        st.markdown(
            '<div style="font-size:1rem;font-weight:800;color:#e5eefb;letter-spacing:-.03em;margin-bottom:10px;">'
            '🚦 Arbeitsbereich</div>',
            unsafe_allow_html=True,
        )

        # Market status badge (populated after first market tab load)
        ampel_phase = st.session_state.get("_sidebar_ampel_phase", "")
        ampel_colors = {
            "rot": "#ef4444", "gelb": "#f59e0b", "gruen": "#22c55e",
            "aufwaertstrend": "#3b82f6", "neutral": "#64748b",
        }
        ampel_labels = {
            "rot": "ROT — Abwarten", "gelb": "GELB — Startschuss",
            "gruen": "GRÜN — Bestätigung", "aufwaertstrend": "Aufwärtstrend ↑",
            "neutral": "Neutral",
        }
        if ampel_phase and ampel_phase in ampel_colors:
            color = ampel_colors[ampel_phase]
            label = ampel_labels[ampel_phase]
            st.markdown(
                f'<div class="sidebar-status-badge">'
                f'<div class="sidebar-dot" style="background:{color};box-shadow:0 0 8px {color}80;"></div>'
                f'<span style="font-size:.8rem;font-weight:700;color:{color};">{label}</span>'
                f'</div>',
                unsafe_allow_html=True,
            )

        st.markdown(
            f'<div style="font-size:.7rem;color:#4a5568;margin-bottom:10px;">'
            f'Speicher: {_workspace_backend_label()}</div>',
            unsafe_allow_html=True,
        )

        if _private_area_enabled():
            unlocked = _is_private_unlocked()
            dot_color = "#22c55e" if unlocked else "#4a5568"
            state_label = "entsperrt" if unlocked else "gesperrt"
            st.markdown(
                f'<div style="display:flex;align-items:center;gap:6px;margin-bottom:10px;">'
                f'<div style="width:7px;height:7px;border-radius:50%;background:{dot_color};"></div>'
                f'<span style="font-size:.75rem;color:#64748b;">Privater Bereich {state_label}</span>'
                f'</div>',
                unsafe_allow_html=True,
            )
            if unlocked:
                if st.button("🔒 Sperren", use_container_width=True, key="sidebar_lock_private"):
                    _lock_private_area()
                    st.rerun()
        if not _is_private_unlocked():
            st.markdown(
                '<div style="font-size:.8rem;color:#4a5568;padding:8px 0;line-height:1.45;">'
                'Watchlist und Depot sind gesperrt.</div>',
                unsafe_allow_html=True,
            )
            return

        _init_workspace_state()
        st.markdown('<div class="card-label" style="margin-top:4px;">Watchlist</div>', unsafe_allow_html=True)
        watchlist = st.session_state.get("watchlist", [])
        if watchlist:
            st.markdown(
                '<div class="pill-wrap">' + "".join(f'<span class="pill">{t}</span>' for t in watchlist[:8]) + '</div>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown('<div class="workspace-note">Noch keine Watchlist.</div>', unsafe_allow_html=True)

        positions = st.session_state.get("positions", [])
        recents = st.session_state.get("recent_tickers", [])
        st.markdown(
            f'<div style="font-size:.73rem;color:#4a5568;margin-top:10px;">'
            f'{len(positions)} Positionen · {len(recents)} zuletzt genutzt</div>',
            unsafe_allow_html=True,
        )

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


def _parse_nasdaq_traded_text(text):
    """Parse Nasdaq Trader nasdaqtraded.txt and keep only common stocks."""
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
    if symbol_col is None:
        return []

    etf_col = lower_cols.get("etf")
    test_col = lower_cols.get("test issue")
    nextshares_col = lower_cols.get("nextshares")
    name_col = lower_cols.get("security name")

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
            r"\bbeneficial\s+interest\b",
            r"\badr\b",
            r"\badrs\b",
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


def _get_common_stocks_from_nasdaq_traded_http():
    """Primary universe source: nasdaqtraded.txt (exclude ETFs + test issues)."""
    urls = [
        "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqtraded.txt",
        "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt",
        "https://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqtraded.txt",
        "http://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqtraded.txt",
    ]
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "text/plain,*/*"}
    best = []
    for url in urls:
        try:
            resp = requests.get(url, timeout=30, headers=headers)
            resp.raise_for_status()
            tickers = _parse_nasdaq_traded_text(resp.text)
            if len(tickers) > len(best):
                best = tickers
            if len(tickers) >= 2500:
                return tickers
        except Exception:
            continue
    return best if len(best) >= 1000 else []


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
    traded = _normalize_ticker_list(_get_common_stocks_from_nasdaq_traded_http())
    if len(traded) >= 2500:
        return traded

    # Fallback to previous approach when nasdaqtraded.txt is unavailable.
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
    end = datetime.now(); start = end - timedelta(days=lookback_days)
    tickers = {
        "S&P 500":"^GSPC","Nasdaq Composite":"^IXIC","Russell 2000":"^RUT",
        "RSP (Equal-Weight S&P)":"RSP","QQEW (Equal-Weight Nasdaq)":"QQEW",
        "VIX":"^VIX","VIXY":"VIXY",
        "XLU (Utilities)":"XLU","XLP (Consumer Staples)":"XLP",
        "XLK (Technology)":"XLK","XLY (Consumer Discr.)":"XLY",
    }
    data = {}
    for name, sym in tickers.items():
        df = _dl(sym, start, end)
        if df is not None and len(df) > 20: data[name] = df

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

def build_sector_table(closes, mode="daily", n_periods=20):
    """Build the ranked sector performance table.
    mode: 'daily' = each column is a trading day
          'weekly' = each column is a trading week (Friday close)
    Returns a styled DataFrame ready for display.
    """
    if closes is None or len(closes) < 5: return None, None

    if mode == "weekly":
        # Resample to weekly (Friday close)
        weekly = closes.resample("W-FRI").last().dropna(how="all")
        pct = weekly.pct_change() * 100
        pct = pct.dropna(how="all")
        display_data = pct.tail(n_periods + 1)  # +1 because we show the latest + history
    else:
        pct = closes.pct_change() * 100
        pct = pct.dropna(how="all")
        display_data = pct.tail(n_periods + 1)

    if len(display_data) < 2: return None, None

    # Latest period for ranking
    latest = display_data.iloc[-1].dropna()

    # Rename columns: ETF ticker → "Sektor (ETF)"
    col_rename = {etf: f"{name} ({etf})" for etf, name in SECTOR_ETFS.items()}

    # Build the transposed table: rows = sectors, columns = dates
    result = display_data.T.copy()
    result.index = [col_rename.get(idx, idx) for idx in result.index]

    # Format date columns
    if mode == "weekly":
        result.columns = [d.strftime("KW%V %d.%m") for d in result.columns]
    else:
        result.columns = [d.strftime("%d.%m") for d in result.columns]

    # Sort rows by the latest column (best performer first)
    last_col = result.columns[-1]
    result = result.sort_values(by=last_col, ascending=False)

    # Round to 2 decimals
    result = result.round(2)

    # Also return the latest values for the ranking summary
    latest_ranked = latest.sort_values(ascending=False)
    latest_ranked.index = [col_rename.get(idx, idx) for idx in latest_ranked.index]

    return result, latest_ranked


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
    for key in ["DilutedEPS", "TotalRevenue"]:
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

        eps_series = _extract_quarters(eps_concepts, ["USD/shares"])
        rev_series = _extract_quarters(rev_concepts, ["USD"], duration_filter=lambda d: 75 <= d <= 110)

        out = {}
        if eps_series is not None and len(eps_series) > 0:
            out["DilutedEPS"] = eps_series
        if rev_series is not None and len(rev_series) > 0:
            out["TotalRevenue"] = rev_series
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
    if not fmp_key: return None, "Kein API-Key"
    try:
        import json
        url = f"https://financialmodelingprep.com/api/v3/income-statement/{ticker}"
        params = {"period": "quarter", "limit": 12, "apikey": fmp_key}
        r = requests.get(url, params=params, timeout=15)
        if r.status_code == 429: return None, f"Rate Limited (429)"
        if r.status_code == 403: return None, f"Zugriff verweigert (403) — Domain evtl. geblockt"
        if r.status_code == 401: return None, f"API-Key ungültig (401)"
        if r.status_code != 200: return None, f"HTTP {r.status_code}"
        data = json.loads(r.text)
        if isinstance(data, dict) and "Error Message" in data:
            return None, f"FMP Fehler: {data['Error Message'][:80]}"
        if not data or not isinstance(data, list): return None, f"Leere Antwort (Typ: {type(data).__name__})"
        out = {}; eps_vals = {}; rev_vals = {}
        for item in data:
            date_str = item.get("date", "")
            if not date_str: continue
            dt = pd.Timestamp(date_str)
            eps = item.get("epsdiluted", item.get("epsDiluted"))
            rev = item.get("revenue")
            if eps is not None: eps_vals[dt] = float(eps)
            if rev is not None: rev_vals[dt] = float(rev)
        if eps_vals: out["DilutedEPS"] = pd.Series(eps_vals).sort_index(ascending=False)
        if rev_vals: out["TotalRevenue"] = pd.Series(rev_vals).sort_index(ascending=False)
        return (out if out else None), None
    except requests.exceptions.ConnectionError as e:
        return None, f"Verbindungsfehler: {str(e)[:60]}"
    except requests.exceptions.Timeout:
        return None, "Timeout (>15s)"
    except Exception as e:
        return None, f"Fehler: {type(e).__name__}: {str(e)[:60]}"


def _load_stock_full_core(ticker, lookback_days=500):
    """Load price history plus the most important fundamental datasets for one ticker."""
    end = datetime.now()
    start = end - timedelta(days=lookback_days)
    empty_result = (None, None, None, None, None, None, None, None, None)
    try:
        t = yf.Ticker(ticker)
        df = t.history(start=start, end=end, auto_adjust=True)
        if df is None or len(df) < 20:
            return empty_result

        df.index = pd.to_datetime(df.index)
        df = df.sort_index()
        for c in ["Open", "High", "Low", "Close", "Volume"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")

        def _safe_attr(attr_name):
            try:
                return getattr(t, attr_name)
            except Exception as exc:
                logger.debug("Ticker attribute %s failed for %s: %s", attr_name, ticker, exc)
                return None

        def _merge_info(base: dict, extra: dict | None) -> dict:
            out = dict(base or {})
            if isinstance(extra, dict):
                for k, v in extra.items():
                    if k not in out or out.get(k) in (None, "", "N/A"):
                        out[k] = v
            return out

        def _fetch_yahoo_quote_summary_fallback(symbol: str) -> dict:
            try:
                url = f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{symbol}"
                params = {"modules": "assetProfile,financialData"}
                headers = {"User-Agent": "Mozilla/5.0"}
                resp = requests.get(url, params=params, headers=headers, timeout=12)
                resp.raise_for_status()
                data = resp.json() or {}
                result = ((data.get("quoteSummary", {}) or {}).get("result", []) or [])
                if not result:
                    return {}
                node = result[0] or {}
                asset = node.get("assetProfile", {}) or {}
                fin = node.get("financialData", {}) or {}

                def _extract_num(val):
                    if isinstance(val, dict):
                        raw = val.get("raw")
                        return raw if raw is not None else val.get("fmt")
                    return val

                out = {}
                if asset.get("sector"):
                    out["sector"] = asset.get("sector")
                if asset.get("industry"):
                    out["industry"] = asset.get("industry")
                roe_val = _extract_num(fin.get("returnOnEquity"))
                if roe_val is not None:
                    out["returnOnEquity"] = roe_val
                beta_val = _extract_num(fin.get("beta"))
                if beta_val is not None:
                    out["beta"] = beta_val
                return out
            except Exception as exc:
                logger.debug("quoteSummary fallback failed for %s: %s", symbol, exc)
                return {}

        info = _safe_attr("info") or {}
        if not isinstance(info, dict):
            info = {}

        fast_info = _safe_attr("fast_info")
        fast_beta = None
        if fast_info is not None:
            try:
                fast_beta = fast_info.get("beta")
            except Exception:
                fast_beta = None
        if info.get("beta") in (None, "") and fast_beta is not None:
            info["beta"] = fast_beta

        needs_profile = any(info.get(k) in (None, "", "N/A") for k in ["sector", "industry", "returnOnEquity", "beta"])
        if needs_profile:
            fallback_info = _fetch_yahoo_quote_summary_fallback(ticker)
            info = _merge_info(info, fallback_info)

        qi = _safe_attr("quarterly_income_stmt")
        ai = _safe_attr("income_stmt")
        ih = _safe_attr("institutional_holders")
        qe = _safe_attr("quarterly_earnings")
        try:
            ed = t.get_earnings_dates(limit=12)
        except Exception as exc:
            logger.debug("earnings dates failed for %s: %s", ticker, exc)
            ed = None

        fmp_key = _read_secret_value("FMP_API_KEY")
        qraw = None
        fmp_err = None
        qraw_sources = []
        if fmp_key:
            result = _fetch_quarterly_fmp(ticker, fmp_key)
            if isinstance(result, tuple):
                qraw, fmp_err = result
            else:
                qraw = result
            if qraw is not None:
                qraw_sources.append("FMP")

        # SEC fallback/augment (especially useful when Yahoo/FMP provides only few quarters)
        sec_raw, sec_err = _fetch_quarterly_sec_companyfacts(ticker)
        if sec_raw is not None:
            qraw = _merge_quarterly_raw(qraw, sec_raw)
            qraw_sources.append("SEC")
            if fmp_err and qraw is not None:
                fmp_err = f"{fmp_err} | SEC ergänzt"
        elif qraw is None and sec_err:
            fmp_err = sec_err if not fmp_err else f"{fmp_err} | {sec_err}"

        if qraw is not None:
            src = "+".join(dict.fromkeys(qraw_sources)) if qraw_sources else "Direktdaten"
            qraw["_source"] = src

        return df, info, qi, ai, ih, qe, ed, qraw, fmp_err
    except Exception as exc:
        logger.warning("load_stock_full failed for %s: %s", ticker, exc)
        return empty_result


@st.cache_data(ttl=900, show_spinner=False)
def load_stock_full(ticker, lookback_days=500):
    return _load_stock_full_core(ticker, lookback_days=lookback_days)


def _load_sp500_for_rs_core(lookback_days=400):
    end = datetime.now()
    start = end - timedelta(days=lookback_days)
    for sym in ("^GSPC", "^SPX", "SPY"):
        df = _dl(sym, start, end)
        if df is not None and len(df) >= 120:
            return df
    return None


@st.cache_data(ttl=3600, show_spinner=False)
def load_sp500_for_rs(lookback_days=400):
    return _load_sp500_for_rs_core(lookback_days=lookback_days)

# ===== From cache_store.py =====
try:
    import psycopg2
    from psycopg2.extras import execute_values
except Exception:
    psycopg2 = None
    execute_values = None


logger = logging.getLogger(__name__)

CACHE_DB_NAME = "market_data_cache.sqlite"

CACHE_UNIVERSE_NAME = "us_common_stocks_v3"

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
    neon_url = _get_neon_connection_url()
    if neon_url and psycopg2 is not None and _can_connect_neon(neon_url):
        return {"backend": "neon", "dsn": neon_url, "label": "Neon Postgres"}
    return {"backend": "sqlite", "db_path": _get_cache_db_path(), "label": "lokaler SQLite-Cache"}

def _get_store_label(store):
    return store.get("label", store.get("backend", "Datenspeicher"))

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
    conn = _get_cache_conn(store)
    try:
        cur = conn.cursor() if store["backend"] == "neon" else conn
        if store["backend"] == "neon":
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

        if store["backend"] == "neon":
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
        if store["backend"] == "neon":
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


def _set_cache_metadata(store, key, value, *, conn=None):
    own_conn = conn is None
    conn = conn or _get_cache_conn(store)
    try:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        if store["backend"] == "neon":
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
        if store["backend"] == "neon" and not is_sqlite:
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
        if store["backend"] == "neon" and not is_sqlite:
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
        if store["backend"] == "neon":
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
        if store["backend"] == "neon":
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
        if store["backend"] == "neon":
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
        if store["backend"] == "neon":
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
        statuses = ("queued", "running")
        if store["backend"] == "neon":
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM refresh_jobs WHERE status IN (%s, %s) ORDER BY requested_at DESC LIMIT 1",
                    statuses,
                )
                row = cur.fetchone()
                cols = [desc[0] for desc in cur.description] if cur.description else []
        else:
            cur = conn.execute(
                "SELECT * FROM refresh_jobs WHERE status IN (?, ?) ORDER BY requested_at DESC LIMIT 1",
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
    }.get(str(job_type or ""), str(job_type or "Unbekannt"))


def _trigger_github_actions_workflow(job_id, job_type, extra_inputs=None):
    cfg = _github_actions_config()
    if not cfg.get("ready"):
        return {"ok": False, "error": "GitHub Actions ist nicht vollständig konfiguriert.", "config": cfg}
    inputs = {"job_id": str(job_id), "job_type": str(job_type)}
    if extra_inputs:
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
    conn = _get_cache_conn(store)
    try:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        if store["backend"] == "neon":
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
        if store["backend"] == "neon":
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
        if store["backend"] == "neon":
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
        params = [universe]
        status_sql = ""
        if status_filter:
            if store["backend"] == "neon":
                status_sql = " AND status = ANY(%s)"
                params.append(list(status_filter))
            else:
                placeholders = ",".join(["?"] * len(status_filter))
                status_sql = f" AND status IN ({placeholders})"
                params.extend(list(status_filter))
        query = (
            "SELECT source_symbol, yahoo_symbol FROM symbol_mappings "
            "WHERE universe=" + ("%s" if store["backend"] == "neon" else "?") +
            " AND yahoo_symbol IS NOT NULL AND TRIM(yahoo_symbol) <> ''" + status_sql
        )
        if store["backend"] == "neon":
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
        price_bundle = _prepare_component_bundle(_read_cached_price_bundle(store, tickers, start_date, end_date))
        close_frame = price_bundle.get("close") if price_bundle else None
        loaded = int(close_frame.shape[1]) if close_frame is not None else len(tickers)
        requested = len(tickers)
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
    price_bundle = _prepare_component_bundle(_read_cached_price_bundle(store, tickers, start_date, end_date))
    close_frame = price_bundle.get("close") if price_bundle else None
    loaded = int(close_frame.shape[1]) if close_frame is not None else max(0, len(tickers) - len(missing_after))
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
        if store["backend"] == "neon":
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
        if store["backend"] == "neon":
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
        if store["backend"] == "neon":
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
        if store["backend"] == "neon":
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
    last_dates = _get_cached_last_dates(store, tickers)
    requested = len(tickers)
    loaded = len(last_dates)
    counts = {"mapped": 0, "not_found": 0, "no_history": 0}
    try:
        conn = _open_store_connection(store)
        try:
            if store["backend"] == "neon":
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
    field_counts = _get_cached_price_field_counts(store, tickers, start_date, end_date)
    min_history_rows = 180
    needs_full_ohlc = [
        t for t in tickers
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
        recent_targets = [t for t in tickers if t in refreshed_symbol_set]

    recent_start = max(start, end - timedelta(days=recent_refresh_days))
    for batch in _chunked(recent_targets, recent_batch_size):
        bundle = _download_ohlc_batch_fast(batch, recent_start, end, threads=True, timeout=35)
        close_frame = bundle.get("close") if bundle else None
        if close_frame is not None and not close_frame.empty:
            rows_written += _write_price_bundle_to_cache(store, bundle)
        recent_batches += 1

    price_bundle = _prepare_component_bundle(_read_cached_price_bundle(store, tickers, start_date, end_date))
    close_frame = price_bundle.get("close") if price_bundle else None
    loaded = int(close_frame.shape[1]) if close_frame is not None else 0
    requested = len(tickers)
    coverage = loaded / max(requested, 1)

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    _set_cache_metadata_many(store, {"last_refresh_at": now_str, "last_refresh_rows_written": rows_written, "last_refresh_loaded_universe": loaded, "last_refresh_requested_universe": requested})

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
        price_bundle = _prepare_component_bundle(_read_cached_price_bundle(store, tickers, start_date, end_date))
        close_frame = price_bundle.get("close") if price_bundle else None
        loaded = int(close_frame.shape[1]) if close_frame is not None else len(tickers)
        requested = len(tickers)
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
    price_bundle = _prepare_component_bundle(_read_cached_price_bundle(store, tickers, start_date, end_date))
    close_frame = price_bundle.get("close") if price_bundle else None
    loaded = int(close_frame.shape[1]) if close_frame is not None else max(0, len(tickers) - len(missing_after))
    requested = len(tickers)
    coverage = loaded / max(requested, 1)
    new_symbols_loaded = len(missing_before) - len(missing_after)

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    _set_cache_metadata_many(store, {"last_rescue_at": now_str, "last_rescue_rows_written": rows_written, "last_rescue_missing_before": len(missing_before), "last_rescue_missing_after": len(missing_after), "last_refresh_loaded_universe": loaded, "last_refresh_requested_universe": requested})

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

@st.cache_data(ttl=900, show_spinner=False)
def load_nyse_breadth_data(lookback_days=550):
    """Read breadth data bundle from the persistent store without triggering a large network refresh."""
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

    bundle = _prepare_component_bundle(_read_cached_price_bundle(store, tickers, start_date, end_date))
    close_frame = bundle.get("close") if bundle else None
    if close_frame is None or close_frame.empty:
        return None

    requested = len(tickers)
    loaded = int(close_frame.shape[1])
    coverage = loaded / max(requested, 1)
    try:
        stored_loaded = int(_get_cache_metadata(store, "last_refresh_loaded_universe", 0) or 0)
    except Exception:
        stored_loaded = 0
    try:
        stored_requested = int(_get_cache_metadata(store, "last_refresh_requested_universe", 0) or 0)
    except Exception:
        stored_requested = 0
    if loaded > stored_loaded or requested != stored_requested:
        _set_cache_metadata_many(store, {"last_refresh_loaded_universe": loaded, "last_refresh_requested_universe": requested})

    attrs = {
        "requested_universe": requested,
        "loaded_universe": loaded,
        "coverage_ratio": coverage,
        "partial_universe": coverage < 0.75,
        "cache_used": True,
        "cache_only_run": True,
        "store_backend": store["backend"],
        "store_label": _get_store_label(store),
        "cache_member_count": int(_get_cache_metadata(store, f"{CACHE_UNIVERSE_NAME}_member_count", requested) or requested),
        "cache_members_updated_at": _get_cache_metadata(store, f"{CACHE_UNIVERSE_NAME}_members_updated_at", ""),
        "cache_prices_last_write_at": _get_cache_metadata(store, "prices_last_write_at", ""),
        "last_refresh_at": _get_cache_metadata(store, "last_refresh_at", ""),
    }
    bundle["attrs"] = attrs

    min_required = max(350, int(requested * 0.18))
    return bundle if loaded >= min_required else None


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
    df["ATR21"]=_atr(df,21);df["ATR_pct"]=df["ATR21"]/df["Close"]*100
    df["Vol_SMA50"]=_sma(df["Volume"],50);df["Pct_Change"]=df["Close"].pct_change()*100
    rng=df["High"]-df["Low"];df["Closing_Range"]=np.where(rng>0,(df["Close"]-df["Low"])/rng,0.5)
    df["Dist_21EMA"]=(df["Close"]-df["EMA21"])/df["ATR21"]
    df["Dist_50SMA_pct"]=(df["Close"]-df["SMA50"])/df["SMA50"]*100
    df["High_52w"]=df["High"].rolling(252,min_periods=1).max()
    df["Dist_52w_pct"]=(df["Close"]-df["High_52w"])/df["High_52w"]*100
    df["MA_Order"]=(df["EMA21"]>df["SMA50"])&(df["SMA50"]>df["SMA200"])
    df["Low_above_21"]=df["Low"]>df["EMA21"];df["Low_above_50"]=df["Low"]>df["SMA50"];df["Low_above_200"]=df["Low"]>df["SMA200"]
    df["Consec_Low_above_21"]=_consec(df["Low_above_21"]);df["Consec_Low_above_50"]=_consec(df["Low_above_50"]);df["Consec_Low_above_200"]=_consec(df["Low_above_200"])
    df["EMA21_held"]=df["Close"]>df["EMA21"];df["SMA50_held"]=df["Close"]>df["SMA50"];df["SMA200_held"]=df["Close"]>df["SMA200"]
    df["Intraday_Reversal_Down"]=(df["Open"]>df["Close"].shift(1))&(df["Close"]<df["Open"])&(df["Closing_Range"]<0.35)
    df["Neg_Reversals_10d"]=df["Intraday_Reversal_Down"].rolling(10,min_periods=1).sum().astype(int)
    df["Low_CR"]=df["Closing_Range"]<0.25;df["Low_CR_5d"]=df["Low_CR"].rolling(5,min_periods=1).sum().astype(int)
    is_up=df["Close"]>df["Close"].shift(1);up_vol=df["Volume"].where(is_up)
    df["Up_Vol_SMA5"]=up_vol.ffill().rolling(5,min_periods=2).mean()
    df["Up_Vol_Declining"]=df["Up_Vol_SMA5"]<df["Up_Vol_SMA5"].shift(5)
    return df

def detect_distribution_days(df):
    df=df.copy();pc=df["Close"].shift(1);pv=df["Volume"].shift(1)
    is_down=df["Close"]<pc;high_vol=(df["Volume"]>pv)|(df["Volume"]>df["Vol_SMA50"])
    df["Is_Distribution"]=is_down&high_vol
    df["Is_Stall"]=(~is_down)&(df["Pct_Change"]<0.5)&(df["Volume"]>=pv*0.95)&(df["Closing_Range"]<0.5)
    df["Dist_Count_25"]=df["Is_Distribution"].rolling(25,min_periods=1).sum().astype(int)
    return df

def compute_ampel(df):
    df=df.copy();n=len(df);phase="neutral"
    anchor_idx=None;floor_mark=None;startschuss_idx=None;startschuss_low=None;gruen_since=None
    phases=["neutral"]*n;anchor_dates=[None]*n;floor_marks=[None]*n;startschuss_lows=[None]*n;startschuss_dates=[None]*n
    c_=df["Close"].values;o_=df["Open"].values;h_=df["High"].values;l_=df["Low"].values
    v_=df["Volume"].values;pct_=df["Pct_Change"].values;cr_=df["Closing_Range"].values
    dc_=df["Dist_Count_25"].values;s50_=df["SMA50"].values;s200_=df["SMA200"].values;e21_=df["EMA21"].values
    def _clear():
        nonlocal anchor_idx,floor_mark,startschuss_idx,startschuss_low,gruen_since
        anchor_idx=None;floor_mark=None;startschuss_idx=None;startschuss_low=None;gruen_since=None
    def _corr(i):
        lb=max(0,i-60);rh=np.nanmax(h_[lb:i+1]);dd=(c_[i]-rh)/rh*100 if rh>0 else 0
        return dd<-8 or (not np.isnan(s50_[i]) and c_[i]<s50_[i] and dc_[i]>=4)
    for i in range(1,n):
        pi=pct_[i] if not np.isnan(pct_[i]) else 0.0;cri=cr_[i] if not np.isnan(cr_[i]) else 0.5
        if phase in ("neutral","aufwaertstrend"):
            if _corr(i): phase="rot";_clear()
            elif phase=="aufwaertstrend" and not _is_ma_order_ok(e21_[i], s50_[i], s200_[i]): phase="rot";_clear()
        elif phase=="rot":
            if anchor_idx is not None and i>anchor_idx and l_[i]<floor_mark: anchor_idx=None;floor_mark=None
            if anchor_idx is None:
                if pi>0.0 or (c_[i]>o_[i] and cri>=0.5): anchor_idx=i;floor_mark=min(l_[i],l_[i-1])
            if anchor_idx is not None and i>=anchor_idx+5:
                if pi>=1.0 and v_[i]>v_[i-1] and l_[i]>=floor_mark: phase="gelb";startschuss_idx=i;startschuss_low=l_[i]
        elif phase=="gelb":
            if startschuss_low is not None and c_[i]<startschuss_low: phase="rot";_clear()
            elif startschuss_idx is not None and i>startschuss_idx+2: phase="gruen";gruen_since=i
        elif phase=="gruen":
            if startschuss_low is not None and c_[i]<startschuss_low: phase="rot";_clear()
            elif not np.isnan(s200_[i]) and c_[i]>s200_[i] and _is_ma_order_ok(e21_[i], s50_[i], s200_[i]) and (gruen_since and i-gruen_since>=10): phase="aufwaertstrend";_clear()
        phases[i]=phase
        if anchor_idx is not None: anchor_dates[i]=df.index[anchor_idx].strftime("%Y-%m-%d")
        if floor_mark is not None: floor_marks[i]=round(floor_mark,2)
        if startschuss_low is not None: startschuss_lows[i]=round(startschuss_low,2)
        if startschuss_idx is not None: startschuss_dates[i]=df.index[startschuss_idx].strftime("%Y-%m-%d")
    df["Ampel_Phase"]=phases;df["Anchor_Date"]=anchor_dates;df["Floor_Mark"]=floor_marks;df["Startschuss_Low"]=startschuss_lows;df["Startschuss_Date"]=startschuss_dates
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

    dv["Is_Panic"] = panic_rule.fillna(False) | fallback_panic.fillna(False)
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

    if vix_df is not None and len(vix_df) > 0:
        v = vix_df.reindex(out.index).ffill()
        out["VIX_Close"] = v["Close"]
        out["VIX_Ret_5d"] = v.get("Ret_5d")
        out["VIX_PctRank252"] = v.get("PctRank252")
        out["VIX_Is_Panic"] = v.get("Is_Panic", False).fillna(False)
        out["VIX_Is_Calm"] = v.get("Is_Calm", False).fillna(False)
        out["VIX_Regime"] = v.get("VIX_Regime", "Neutral")
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
        out["VIXY_Stress_Confirmation"] = x.get("Stress_Confirmation", False).fillna(False)
        out["VIXY_Carry_Decay"] = x.get("Carry_Decay", False).fillna(False)
        out["VIXY_State"] = x.get("VIXY_State", "Gemischt")
    else:
        out["VIXY_Close"] = np.nan
        out["VIXY_Ret_5d"] = np.nan
        out["VIXY_Stress_Confirmation"] = False
        out["VIXY_Carry_Decay"] = False
        out["VIXY_State"] = "n/a"

    out["Fragile_Rally"] = (
        (out["SPX_Ret_5d"] > 0)
        & (
            out["VIXY_Stress_Confirmation"]
            | (out["VIX_Ret_5d"] > 0)
            | ((out["VIXY_Ret_5d"] > 0.03) & (out["VIX_PctRank252"] > 0.55))
        )
    ).fillna(False)

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

def render_ampel_section(L):
    """Render the full Trendwende-Ampel section with clickable inline rule explanations."""
    phase = L["Ampel_Phase"]
    anchor = L["Anchor_Date"]
    floor = L["Floor_Mark"]
    ss_low = L["Startschuss_Low"]
    ss_date = L.get("Startschuss_Date", None)

    phase_info = {
        "rot": {
            "active": 0, "label": "ROT — Abwarten",
            "reason": "Substanzielle Korrektur läuft." + (
                f" Ankertag: {anchor}. Bodenmarke: {floor:.0f}." if anchor and floor else
                " Warte auf Ankertag (erster positiver Schluss)."
            ),
            "action": "Nicht kaufen. Beobachte den Markt auf Stabilisierung.",
        },
        "gelb": {
            "active": 1, "label": "GELB — Startschuss",
            "reason": f"Startschuss erkannt! Ankertag: {anchor}. Startschuss-Tag: {ss_date or '—'}. Validierungslinie (Startschuss-Tief): {ss_low:.0f}." if anchor and ss_low else "Startschuss aktiv.",
            "action": "Erste Position(en) eröffnen (10–30% Kapital). Nur mit klarem Setup.",
        },
        "gruen": {
            "active": 2, "label": "GRÜN — Bestätigung",
            "reason": f"Startschuss hält seit {ss_date or '—'}. Kurs über Startschuss-Tief ({ss_low:.0f})." if ss_low else "Startschuss bestätigt.",
            "action": "Frühe Bestätigungsphase. Vorsichtig Exponierung aufbauen.",
        },
        "aufwaertstrend": {
            "active": 2, "label": "AUFWÄRTSTREND ↑",
            "reason": "MA-Ordnung bestätigt (21-EMA > 50-SMA > 200-SMA). Ampel-Zyklus abgeschlossen.",
            "action": "Offensiv handeln. Gießkannenmodus: viele kleine Positionen, beste Läufer aufstocken.",
        },
        "neutral": {
            "active": -1, "label": "NEUTRAL",
            "reason": "Keine substanzielle Korrektur erkannt. Trendwende-Ampel ist nicht aktiv.",
            "action": "Normale Marktbeobachtung. Ampel greift erst bei Drawdown > 8%.",
        },
    }
    info = phase_info.get(phase, phase_info["neutral"])

    colors_off = ["#3b1111", "#3b2d11", "#112b11"]
    colors_on = ["#ef4444", "#f59e0b", "#22c55e"]
    labels = ["ROT", "GELB", "GRÜN"]
    glow_on = ["0 0 20px #ef444480, 0 0 40px #ef444440", "0 0 20px #f59e0b80, 0 0 40px #f59e0b40", "0 0 20px #22c55e80, 0 0 40px #22c55e40"]
    phase_rules = {
        "rot": "ROT wird aktiv, wenn eine substanzielle Korrektur erkannt wird. Aktuell heißt das im Code: Drawdown von mehr als 8% vom jüngsten Hoch oder Schlusskurs unter der 50-SMA bei mindestens 4 Distribution Days im 25-Tage-Fenster.",
        "gelb": "GELB wird aktiv, wenn nach einem Ankertag frühestens ab Tag 5 ein Startschuss auftritt: mindestens +1,0% zum Vortag, Volumen über Vortag und kein Unterschreiten der Bodenmarke intraday.",
        "gruen": "GRÜN wird aktiv, wenn der Startschuss hält und nach GELB mehr als 2 weitere Handelstage vergehen, ohne dass das Startschuss-Tief per Schlusskurs gebrochen wird.",
        "aufwaertstrend": "AUFWÄRTSTREND wird aktiv, wenn die grüne Phase mindestens 10 Tage Bestand hatte, der Index über der 200-SMA liegt und die MA-Ordnung 21-EMA > 50-SMA > 200-SMA bestätigt ist.",
    }

    light_keys = ["rot", "gelb", "gruen"]
    lights_html = ""
    for i, key in enumerate(light_keys):
        is_active = i == info["active"]
        if phase == "aufwaertstrend" and i == 2:
            bg = "#3b82f6"
            glow = "0 0 20px #3b82f680, 0 0 40px #3b82f640"
            is_active = True
        else:
            bg = colors_on[i] if is_active else colors_off[i]
            glow = glow_on[i] if is_active else "none"
        border = f"2px solid {colors_on[i]}40" if is_active else "2px solid #1e293b"
        lbl_c = "#e2e8f0" if is_active else "#4a5568"
        fw = "700" if is_active else "400"
        phase_for_light = key if not (phase == "aufwaertstrend" and key == "gruen") else "aufwaertstrend"
        rule_text = phase_rules.get(phase_for_light, phase_rules.get(key, ""))
        title = "GRÜN / AUFWÄRTSTREND" if phase == "aufwaertstrend" and key == "gruen" else labels[i]
        lights_html += (
            f'<details style="min-width:88px;max-width:120px;">'
            f'<summary style="list-style:none;cursor:pointer;display:flex;flex-direction:column;align-items:center;gap:4px;outline:none;">'
            f'<div style="width:42px;height:42px;border-radius:50%;background:{bg};box-shadow:{glow};border:{border};"></div>'
            f'<div style="font-size:.6rem;color:{lbl_c};font-weight:{fw};letter-spacing:.05em;">{labels[i]}</div>'
            f'<div style="font-size:.55rem;color:#64748b;">Tippen für Regel</div>'
            f'</summary>'
            f'<div style="margin-top:6px;padding:8px;border:1px solid #1e293b;border-radius:8px;background:#0b1220;">'
            f'<div style="font-size:.62rem;color:#94a3b8;text-transform:uppercase;letter-spacing:.06em;">{title}</div>'
            f'<div style="font-size:.68rem;color:#e2e8f0;line-height:1.45;margin-top:4px;">{rule_text}</div>'
            f'</div>'
            f'</details>'
        )

    if phase in ("gelb", "gruen") and ss_low and anchor:
        startschuss_html = (
            f'<div style="display:flex;align-items:center;gap:8px;margin-top:10px;padding:8px 12px;background:#f59e0b12;border:1px solid #f59e0b30;border-radius:8px;">'
            f'<span style="font-size:1.4rem;">🔫</span>'
            f'<div><div style="font-size:.8rem;font-weight:700;color:#f59e0b;">Startschuss aktiv</div><div style="font-size:.7rem;color:#94a3b8;">Startschuss-Tag: {ss_date or "—"} · Startschuss-Tief: {ss_low:,.2f} · Ankertag: {anchor}</div></div></div>'
        )
    else:
        if phase == "rot" and anchor:
            ss_detail = f"Ankertag: {anchor} · Warte auf Tag ≥5 mit ≥1% Gewinn + Vol. > Vortag"
        elif phase == "rot":
            ss_detail = "Warte auf Ankertag, dann frühestens am 5. Tag möglich"
        else:
            ss_detail = "Kein aktiver Ampel-Zyklus"
        startschuss_html = (
            f'<div style="display:flex;align-items:center;gap:8px;margin-top:10px;padding:8px 12px;background:#1e293b40;border:1px solid #1e293b;border-radius:8px;opacity:0.5;">'
            f'<span style="font-size:1.4rem;filter:grayscale(1);">🔫</span>'
            f'<div><div style="font-size:.8rem;font-weight:700;color:#64748b;text-decoration:line-through;">Startschuss</div><div style="font-size:.7rem;color:#4a5568;">{ss_detail}</div></div></div>'
        )

    active_color = {"rot":"#ef4444","gelb":"#f59e0b","gruen":"#22c55e","aufwaertstrend":"#3b82f6","neutral":"#64748b"}.get(phase,"#64748b")
    html = (
        '<div class="info-card" style="padding:20px;">'
        '<div class="card-label">TRENDWENDE-AMPEL</div>'
        '<div style="display:flex;gap:20px;align-items:flex-start;flex-wrap:wrap;">'
        '<div style="display:flex;flex-direction:column;align-items:center;gap:6px;background:#0d1117;padding:16px 20px;border-radius:12px;border:1px solid #1e293b;">'
        f'<div style="display:flex;gap:12px;align-items:flex-start;flex-wrap:wrap;justify-content:center;">{lights_html}</div>'
        '</div>'
        '<div style="flex:1;min-width:220px;">'
        f'<div style="font-size:1.1rem;font-weight:800;color:{active_color};letter-spacing:.04em;margin-bottom:6px;">{info["label"]}</div>'
        f'<div style="font-size:.8rem;color:#e2e8f0;line-height:1.5;margin-bottom:6px;">{info["reason"]}</div>'
        f'<div style="font-size:.75rem;color:#94a3b8;line-height:1.4;padding:6px 10px;background:{active_color}10;border-left:3px solid {active_color};border-radius:0 6px 6px 0;">→ {info["action"]}</div>'
        f'{startschuss_html}'
        '</div></div></div>'
    )
    st.markdown(html, unsafe_allow_html=True)

    _e = L["EMA21"]; _s5 = L["SMA50"]; _s2 = L["SMA200"]
    eo = not np.isnan(_e); so = not np.isnan(_s5); s2o = not np.isnan(_s2)
    _mao = _is_ma_order_ok(_e, _s5, _s2)
    details = {
        "Ankertag": anchor if anchor else "— (kein aktiver Zyklus)" if phase in ("neutral", "aufwaertstrend") else "Warte auf Ankertag",
        "Startschuss-Tag": ss_date if ss_date else "—",
        "Bodenmarke": f"{floor:,.2f}" if floor else "—",
        "Startschuss-Tief": f"{ss_low:,.2f}" if ss_low else "—",
        "MA-Ordnung (21>50>200)": "Korrekt ✓" if _mao else "Gestört ✗",
    }
    cols = st.columns(5)
    for i, (k, v) in enumerate(details.items()):
        with cols[i]:
            st.markdown(f'<div style="background:#0d1117;border:1px solid #1e293b;border-radius:8px;padding:8px 12px;text-align:center;"><div style="font-size:.6rem;color:#64748b;text-transform:uppercase;letter-spacing:.08em;">{k}</div><div style="font-size:.85rem;color:#e2e8f0;font-weight:600;margin-top:4px;">{v}</div></div>', unsafe_allow_html=True)

def render_check(label,ok,detail="",warn=False):
    cls="check-warn" if warn else ("check-ok" if ok else "check-fail");icon="⚠" if warn else ("✓" if ok else "✗")
    st.markdown(f'<div class="check-item"><div class="check-icon {cls}">{icon}</div><div style="flex:1;"><div style="font-size:.85rem;color:#e2e8f0;">{label}</div><div style="font-size:.7rem;color:#64748b;">{detail}</div></div></div>',unsafe_allow_html=True)

def render_breadth(mode,dist_pct):
    c={"rueckenwind":"#22c55e","wachsam":"#f59e0b","schutz":"#ef4444"}.get(mode,"#64748b")
    lbl,desc={"rueckenwind":("Rückenwind","≤4%. Breite Stärke."),"wachsam":("Wachsam","4–8%. Strenger auswählen."),"schutz":("Schutz",">8%. Kapitalschutz.")}.get(mode,("—",""))
    fp=min(100,abs(dist_pct)/12*100)
    st.markdown(f'<div class="info-card" style="background:{c}12;border-color:{c}30;"><div style="display:flex;align-items:center;gap:10px;margin-bottom:8px;"><div style="width:12px;height:12px;border-radius:50%;background:{c};"></div><span style="font-weight:700;color:{c};">Modus: {lbl}</span><span style="font-size:.75rem;color:#94a3b8;">{dist_pct:.1f}% vom 52W-Hoch</span></div><div style="font-size:.75rem;color:#94a3b8;margin-bottom:8px;">{desc}</div><div class="breadth-track"><div class="breadth-fill" style="width:{fp}%;"></div></div><div style="display:flex;justify-content:space-between;font-size:.65rem;color:#64748b;"><span style="color:#22c55e;">Rückenwind</span><span style="color:#f59e0b;">Wachsam</span><span style="color:#ef4444;">Schutz</span></div></div>',unsafe_allow_html=True)


def _render_deep_analysis_content(component_bundle, sd, data):
    close_frame = component_bundle.get("close") if isinstance(component_bundle, dict) else component_bundle
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
    loaded = breadth_attrs.get("loaded_universe", len(close_frame.columns))
    coverage = float(breadth_attrs.get("coverage_ratio", 0.0) or 0.0)
    ratio_txt = f" / {requested}" if requested else ""
    st.success(f"✓ {loaded} Titel aus dem NYSE/Nasdaq-Aktienuniversum geladen{ratio_txt}, {len(br)} Handelstage · Stand: {last_trading_date}")
    if requested and loaded < requested * 0.8:
        st.warning(f"Hinweis: Es wurden nicht alle Titel des NYSE/Nasdaq-Aktienuniversums geladen. Die Tiefenanalyse läuft trotzdem mit {loaded} erfolgreich geladenen Aktien ({coverage:.0%} Abdeckung des gefundenen Universums).")
    st.plotly_chart(plot_breadth_deep(br, sd), use_container_width=True, config={"displayModeBar": False})

    br_valid = br.dropna(subset=["McClellan", "New_Highs"], how="all")
    if len(br_valid) == 0:
        st.warning("Keine gültigen Handelstage gefunden.")
        return br

    bL = br_valid.iloc[-1]
    bL_date = br_valid.index[-1].strftime("%d.%m.%Y")
    intraday_note = " · NH/NL auf Tageshoch/-tief" if br.attrs.get("nhnl_uses_intraday") else " · NH/NL fallback auf Schlusskurs"
    st.markdown(f'<div class="info-card"><div class="card-label">Marktbreite-Kennzahlen — NYSE/Nasdaq ({br.attrs.get("breadth_universe_loaded", len(close_frame.columns))} Aktien) · {bL_date}{intraday_note}</div>', unsafe_allow_html=True)

    kb1, kb2, kb3, kb4, kb5 = st.columns(5)
    mc = bL["McClellan"]; nhr = bL["NH_NL_Ratio"]; nh_val = int(bL["New_Highs"]) if not np.isnan(bL["New_Highs"]) else 0; nl_val = int(bL["New_Lows"]) if not np.isnan(bL["New_Lows"]) else 0
    p50 = bL["Pct_Above_50SMA"]; p200 = bL["Pct_Above_200SMA"]; dr = bL["Deemer_Ratio"]
    with kb1:
        st.metric("McClellan Osc.", f"{mc:.1f}" if not np.isnan(mc) else "—", "Überkauft" if mc > 70 else "Überverkauft" if mc < -70 else "Neutral" if not np.isnan(mc) else "")
    with kb2:
        st.metric("NH/NL Ratio", f"{nhr:.2f}" if not np.isnan(nhr) else f"{nh_val}/{nl_val}", f"{nh_val} Hochs / {nl_val} Tiefs")
    with kb3:
        st.metric("% > 50-SMA", f"{p50:.0f}%" if not np.isnan(p50) else "—", "Überhitzt" if p50 > 70 else "Schwach" if p50 < 30 else "")
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
    render_check("McClellan > 0", mc > 0, f"McClellan: {mc:.1f}")
    render_check("% über 50-SMA > 50%", p50 > 50, f"{p50:.0f}%")
    render_check("NH/NL Ratio > 1", nhr > 1 if not np.isnan(nhr) else False, f"Ratio: {nhr:.1f}" if not np.isnan(nhr) else "—")
    if not np.isnan(dr):
        dr_status = "Sehr gut" if dr >= 1.97 else "Gut" if dr >= 1.50 else "Neutral" if dr >= 1.00 else "Schlecht"
        render_check("Deemer Ratio", dr >= 1.50, f"Ratio: {dr:.2f} · {dr_status}")
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
    for col,nm,clr,sym,sz in [("Is_Distribution","Dist.","#ef4444","triangle-down",7),("Is_Stall","Stau","#f59e0b","diamond",6),("Intraday_Reversal_Down","Umkehr↓","#f97316","x",8)]:
        m=dv[dv[col]==True]
        if len(m)>0: fig.add_trace(go.Scatter(x=_x(m.index),y=_y(m["Close"] if "Stall" not in nm else m["High"]),name=nm,mode="markers",marker=dict(color=clr,size=sz,symbol=sym)))
    fig.update_layout(template="plotly_dark",paper_bgcolor="#111827",plot_bgcolor="#111827",margin=dict(l=0,r=0,t=30,b=0),height=380,legend=dict(orientation="h",yanchor="top",y=1.12,font=dict(size=9,color="#94a3b8")),xaxis=dict(gridcolor="#1e293b",tickfont=dict(size=9,color="#64748b")),yaxis=dict(gridcolor="#1e293b",tickfont=dict(size=9,color="#64748b")),hovermode="x unified")
    return fig

def plot_volume(df,sd=90):
    dv=df.tail(sd);x=_x(dv.index);colors=["#22c55e" if p>=0 else "#ef4444" for p in dv["Pct_Change"].fillna(0)]
    fig=go.Figure();fig.add_trace(go.Bar(x=x,y=_y(dv["Volume"]),marker_color=colors,opacity=0.7));fig.add_trace(go.Scatter(x=x,y=_y(dv["Vol_SMA50"]),line=dict(color="#64748b",width=1,dash="dot")))
    fig.update_layout(template="plotly_dark",paper_bgcolor="#111827",plot_bgcolor="#111827",margin=dict(l=0,r=0,t=10,b=0),height=120,showlegend=False,xaxis=dict(gridcolor="#1e293b",showgrid=False,tickfont=dict(size=9,color="#64748b")),yaxis=dict(gridcolor="#1e293b",tickfont=dict(size=9,color="#64748b"),tickformat=".2s"))
    return fig

def plot_price_with_volume(df, sd=90):
    dv = df.tail(sd).copy(); x = _x(dv.index)
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.04, row_heights=[0.74, 0.26])
    fig.add_trace(go.Scatter(x=x, y=_y(dv["Close"]), name="Kurs", line=dict(color="#e2e8f0", width=2)), row=1, col=1)
    fig.add_trace(go.Scatter(x=x, y=_y(dv["EMA21"]), name="21-EMA", line=dict(color="#06b6d4", width=1, dash="dot")), row=1, col=1)
    fig.add_trace(go.Scatter(x=x, y=_y(dv["SMA50"]), name="50-SMA", line=dict(color="#f97316", width=1, dash="dot")), row=1, col=1)
    fig.add_trace(go.Scatter(x=x, y=_y(dv["SMA200"]), name="200-SMA", line=dict(color="#a855f7", width=1, dash="dash")), row=1, col=1)
    fl = dv["Floor_Mark"].dropna()
    if len(fl) > 0:
        fig.add_hline(y=float(fl.iloc[-1]), line_dash="dash", line_color="#ef4444", line_width=1, annotation_text="Bodenmarke", annotation_font_color="#ef4444", row=1, col=1)
    for col, nm, clr, sym, sz in [("Is_Distribution", "Dist.", "#ef4444", "triangle-down", 7), ("Is_Stall", "Stau", "#f59e0b", "diamond", 6), ("Intraday_Reversal_Down", "Umkehr↓", "#f97316", "x", 8)]:
        m = dv[dv[col] == True]
        if len(m) > 0:
            fig.add_trace(go.Scatter(x=_x(m.index), y=_y(m["Close"] if "Stall" not in nm else m["High"]), name=nm, mode="markers", marker=dict(color=clr, size=sz, symbol=sym)), row=1, col=1)
    vol_colors = ["#22c55e" if p >= 0 else "#ef4444" for p in dv["Pct_Change"].fillna(0)]
    fig.add_trace(go.Bar(x=x, y=_y(dv["Volume"]), marker_color=vol_colors, opacity=0.7, name="Volumen", showlegend=False), row=2, col=1)
    fig.add_trace(go.Scatter(x=x, y=_y(dv["Vol_SMA50"]), name="Vol 50-SMA", line=dict(color="#64748b", width=1, dash="dot"), showlegend=False), row=2, col=1)
    fig.update_layout(template="plotly_dark", paper_bgcolor="#111827", plot_bgcolor="#111827", margin=dict(l=0, r=0, t=30, b=0), height=500, legend=dict(orientation="h", yanchor="top", y=1.10, font=dict(size=9, color="#94a3b8")), xaxis=dict(gridcolor="#1e293b", tickfont=dict(size=9, color="#64748b")), yaxis=dict(gridcolor="#1e293b", tickfont=dict(size=9, color="#64748b")), yaxis2=dict(gridcolor="#1e293b", tickfont=dict(size=9, color="#64748b"), tickformat=".2s"), hovermode="x unified")
    fig.update_xaxes(showgrid=False, row=1, col=1)
    fig.update_xaxes(gridcolor="#1e293b", tickfont=dict(size=9, color="#64748b"), row=2, col=1)
    return fig

def plot_vix(dv, sd=90, title="VIX", price_color="#ef4444"):
    d = dv.tail(sd)
    x = _x(d.index)
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=x, y=_y(d["Close"]), name=title, line=dict(color=price_color, width=1.6)))
    ma_col = "EMA10" if "EMA10" in d.columns else "SMA10" if "SMA10" in d.columns else None
    ma_name = "10-EMA" if ma_col == "EMA10" else "10-SMA"
    if ma_col is not None:
        fig.add_trace(go.Scatter(x=x, y=_y(d[ma_col]), name=ma_name, line=dict(color="#3b82f6", width=1, dash="dot")))
    fig.update_layout(template="plotly_dark", paper_bgcolor="#111827", plot_bgcolor="#111827", margin=dict(l=0, r=0, t=10, b=0), height=180, legend=dict(orientation="h", yanchor="top", y=1.15, font=dict(size=9, color="#94a3b8")), xaxis=dict(gridcolor="#1e293b", tickfont=dict(size=9, color="#64748b")), yaxis=dict(gridcolor="#1e293b", tickfont=dict(size=9, color="#64748b")))
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
    fig.add_trace(go.Scatter(x=x,y=_y(d["AD_Line"]),name="A/D-Linie",line=dict(color="#06b6d4",width=1.5)),row=1,col=1)
    fig.add_trace(go.Scatter(x=x,y=_y(d["AD_Line_SMA21"]),name="21-SMA",line=dict(color="#64748b",width=1,dash="dot")),row=1,col=1)
    # McClellan
    mc_colors=[("#22c55e" if v>=0 else "#ef4444") for v in d["McClellan"].fillna(0)]
    fig.add_trace(go.Bar(x=x,y=_y(d["McClellan"]),name="McClellan",marker_color=mc_colors,opacity=0.8),row=2,col=1)
    fig.add_hline(y=70,line_dash="dot",line_color="#f59e0b",line_width=0.5,row=2,col=1)
    fig.add_hline(y=-70,line_dash="dot",line_color="#f59e0b",line_width=0.5,row=2,col=1)
    # NH / NL
    fig.add_trace(go.Bar(x=x,y=_y(d["New_Highs"]),name="Neue Hochs",marker_color="#22c55e",opacity=0.7),row=3,col=1)
    fig.add_trace(go.Bar(x=x,y=_y(-d["New_Lows"]),name="Neue Tiefs",marker_color="#ef4444",opacity=0.7),row=3,col=1)
    # % above MAs
    fig.add_trace(go.Scatter(x=x,y=_y(d["Pct_Above_50SMA"]),name="% > 50-SMA",line=dict(color="#f97316",width=1.5)),row=4,col=1)
    fig.add_trace(go.Scatter(x=x,y=_y(d["Pct_Above_200SMA"]),name="% > 200-SMA",line=dict(color="#a855f7",width=1.5)),row=4,col=1)
    fig.add_hline(y=70,line_dash="dot",line_color="#f59e0b",line_width=0.5,row=4,col=1)
    # Deemer Ratio
    fig.add_trace(go.Scatter(x=x,y=_y(d["Deemer_Ratio"]),name="Deemer Ratio",line=dict(color="#06b6d4",width=1.5)),row=5,col=1)
    fig.add_hline(y=1.97,line_dash="dash",line_color="#22c55e",line_width=1,annotation_text="1.97 (Thrust)",annotation_font_color="#22c55e",annotation_font_size=9,row=5,col=1)
    fig.add_hline(y=1.0,line_dash="dot",line_color="#64748b",line_width=0.5,row=5,col=1)

    fig.update_layout(template="plotly_dark",paper_bgcolor="#111827",plot_bgcolor="#111827",margin=dict(l=0,r=0,t=30,b=0),height=750,showlegend=False)
    for i in range(1,6): fig.update_xaxes(gridcolor="#1e293b",tickfont=dict(size=8,color="#64748b"),row=i,col=1);fig.update_yaxes(gridcolor="#1e293b",tickfont=dict(size=8,color="#64748b"),row=i,col=1)
    for ann in fig.layout.annotations: ann.font.size=10;ann.font.color="#94a3b8"
    return fig

def plot_fed_rate(fed_df,sd=200):
    d=fed_df.tail(sd);x=_x(d.index);fig=go.Figure()
    fig.add_trace(go.Scatter(x=x,y=_y(d["FedRate"]),name="Fed Funds Rate",line=dict(color="#f59e0b",width=2),fill="tozeroy",fillcolor="rgba(245,158,11,0.1)"))
    fig.update_layout(template="plotly_dark",paper_bgcolor="#111827",plot_bgcolor="#111827",margin=dict(l=0,r=0,t=10,b=0),height=150,showlegend=False,xaxis=dict(gridcolor="#1e293b",tickfont=dict(size=9,color="#64748b")),yaxis=dict(gridcolor="#1e293b",tickfont=dict(size=9,color="#64748b"),title="% p.a.",title_font=dict(size=9,color="#64748b")))
    return fig


def compute_breadth_from_components(components):
    """From component price frames, compute breadth indicators daily."""
    if components is None:
        return None

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

    pct = closes.pct_change()
    results = pd.DataFrame(index=closes.index)

    results["Advancers"] = (pct > 0).sum(axis=1)
    results["Decliners"] = (pct < 0).sum(axis=1)
    results["Net_Advances"] = results["Advancers"] - results["Decliners"]
    results["AD_Ratio"] = results["Advancers"] / results["Decliners"].replace(0, np.nan)
    results["AD_Line"] = results["Net_Advances"].cumsum()

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
    valid50 = sma50.notna().sum(axis=1).replace(0, np.nan)
    valid200 = sma200.notna().sum(axis=1).replace(0, np.nan)
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
    4. quarterly_earnings (deprecated, sometimes still works)
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

    def _expected_anchor_quarter():
        """Return the quarter that should normally be compared YoY right now.

        We use the *last completed* calendar quarter as default anchor.
        Example: in Q1 we still anchor on Q4 (Q1 reports are often not fully out yet).
        """
        now = pd.Timestamp.utcnow()
        return ((int(now.quarter) - 2) % 4) + 1

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

        quarter_to_years = {}
        for y, q in bucket.keys():
            quarter_to_years.setdefault(q, set()).add(y)
        latest_year = max(y for y, _ in bucket.keys())

        expected_q = _expected_anchor_quarter()
        quarter_priority = [((expected_q - i - 1) % 4) + 1 for i in range(4)]
        anchor_q = None
        for q in quarter_priority:
            years_for_q = quarter_to_years.get(q, set())
            if len(years_for_q) >= 2 and latest_year in years_for_q:
                anchor_q = q
                break
        if anchor_q is None:
            anchor_q = ordered_keys[0][1]

        same_q_points = sorted(
            [(y, bucket[(y, q)]) for (y, q) in bucket.keys() if q == anchor_q],
            key=lambda t: t[0],
            reverse=True,
        )
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

    # ── 1. quarterly_income_stmt (try for revenue, usually 4-5 quarters) ──
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

    # ── 2. quarterly_earnings (deprecated fallback) ──
    if qe is not None and not qe.empty:
        col_map = {"eps": "Earnings", "revenue": "Revenue"}
        col = col_map.get(field)
        if col and col in qe.columns:
            vals = qe[col].dropna().sort_index(ascending=False)
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

def _fmt_quarter_value_pairs(items, metric_label="EPS"):
    """Format explicit quarterly values for transparency."""
    if not items:
        return ""
    pairs = []
    for lbl, _, _, cur, prev in items:
        try:
            pairs.append(f"{lbl}: {metric_label} {cur:.2f} vs {prev:.2f}")
        except Exception:
            continue
    return " | ".join(pairs)

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


@st.cache_data(ttl=3600, show_spinner=False)
def load_external_rs_ratings_map():
    api_url = "https://api.github.com/repos/Fred6725/rs-log/contents/output"
    headers = {"Accept": "application/vnd.github+json", "User-Agent": "boerse-dashboard"}
    payload = {"ok": False, "source": "external_csv", "file": "", "files": [], "value_column": "", "count": 0, "ratings": {}, "error": ""}
    try:
        resp = requests.get(api_url, headers=headers, timeout=20)
        resp.raise_for_status()
        rows = resp.json()
        if not isinstance(rows, list):
            payload["error"] = "Ungültige GitHub-API-Antwort."
            return payload
        csv_candidates = [r for r in rows if isinstance(r, dict) and str(r.get("name", "")).lower().endswith(".csv") and r.get("type") == "file"]
        if not csv_candidates:
            payload["error"] = "Keine CSV im output-Ordner gefunden."
            return payload

        preferred = next((r for r in csv_candidates if str(r.get("name", "")).lower() == "rs_stocks.csv"), None)
        chosen = preferred if preferred is not None else sorted(csv_candidates, key=lambda r: str(r.get("name", "")), reverse=True)[0]
        download_url = chosen.get("download_url")
        if not download_url:
            payload["error"] = "Kein download_url für CSV vorhanden."
            return payload
        csv_resp = requests.get(str(download_url), headers={"User-Agent": "boerse-dashboard"}, timeout=20)
        csv_resp.raise_for_status()
        df = pd.read_csv(io.StringIO(csv_resp.text))
        if df is None or df.empty:
            payload["error"] = "CSV ist leer."
            return payload
        normalized_cols = {str(c).strip().lower().replace(" ", "_"): c for c in df.columns}
        ticker_col = next((normalized_cols[k] for k in ["ticker", "symbol", "stock", "aktie"] if k in normalized_cols), None)
        rs_col = next(
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
        if ticker_col is None or rs_col is None:
            payload["error"] = "Ticker- oder RS-Spalte in CSV nicht gefunden."
            return payload

        rows = df[[ticker_col, rs_col]].copy()
        rows[ticker_col] = rows[ticker_col].astype(str).str.upper().str.strip()
        rows[rs_col] = pd.to_numeric(rows[rs_col], errors="coerce")
        rows = rows.dropna(subset=[ticker_col, rs_col])
        rows = rows[rows[ticker_col] != ""]
        rows[rs_col] = rows[rs_col].clip(lower=1, upper=99).round().astype(int)
        rows = rows.drop_duplicates(subset=[ticker_col], keep="last")
        ratings = {str(r[ticker_col]).strip().upper(): int(r[rs_col]) for _, r in rows.iterrows()}
        payload.update({
            "ok": bool(ratings),
            "file": str(chosen.get("name", "")),
            "files": [str(chosen.get("name", ""))],
            "value_column": str(rs_col),
            "count": int(len(ratings)),
            "ratings": ratings,
            "error": "" if ratings else "CSV enthielt keine auswertbaren RS-Werte.",
        })
        return payload
    except Exception as exc:
        logger.debug("load_external_rs_ratings_map failed: %s", exc)
        payload["error"] = f"RS-CSV konnte nicht geladen werden ({exc})."
        return payload


def _apply_rs_source_override(ticker: str, rs_ctx: dict | None):
    source = _get_rs_rating_source_setting()
    if source != "csv_latest":
        return rs_ctx, None
    external = load_external_rs_ratings_map()
    note = {
        "source": "csv_latest",
        "ok": bool(external.get("ok")),
        "file": str(external.get("file", "")),
        "files": list(external.get("files", []) or []),
        "value_column": str(external.get("value_column", "")),
        "count": int(external.get("count", 0) or 0),
        "error": str(external.get("error", "") or "").strip(),
        "matched": False,
    }
    base_ctx = dict(rs_ctx or {})
    ratings = external.get("ratings", {}) if isinstance(external, dict) else {}
    value = ratings.get(str(ticker or "").strip().upper()) if isinstance(ratings, dict) else None
    if value is not None:
        base_ctx["rating"] = int(np.clip(value, 1, 99))
        base_ctx["method"] = "external_csv"
        note["matched"] = True
    return base_ctx, note


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


def _calc_rs_rating(stock_close, benchmark_close, universe_closes=None):
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

    universe_scores = pd.Series(dtype=float)
    if universe_closes is not None and not universe_closes.empty:
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

def evaluate_fundamentals(info, qi, ai, ih, qe=None, ed=None, qraw=None, fmp_err=None):
    checks = []
    def _g(k, d=None):
        v = info.get(k, d) if info else d
        return v if v is not None else d

    # ── Debug: show data availability ──
    src_info = []
    if qraw is not None:
        raw_source = str(qraw.get("_source", "Direktdaten")).strip()
        for key, series in qraw.items():
            if not isinstance(series, pd.Series):
                continue
            src_info.append(f"{raw_source} {key}: {len(series)}Q")
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
        value_trace = _fmt_quarter_value_pairs(epsg, metric_label="EPS")
        if value_trace:
            details = f"{details} · Werte: {value_trace}"
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

    # ── Sum of last 4 quarterly EPS > 0 ──
    eps_sum = _sum_last_4q_eps(qi)
    if eps_sum is not None:
        checks.append(("Summe letzte 4 Quartals-EPS > 0", eps_sum > 0, f"${eps_sum:.2f}"))
    else:
        te = _g("trailingEps")
        if te is not None:
            checks.append(("Trailing EPS > 0 (Proxy für 4Q-Summe)", te > 0, f"${te:.2f}"))

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

    # ── ROE ≥ 17% ──
    roe = _g("returnOnEquity")
    if roe is not None:
        checks.append(("ROE ≥17%", roe*100 >= 17, f"{roe*100:.1f}%"))
    else:
        checks.append(("ROE ≥17%", False, "Nicht verfügbar"))

    # ── Institutional holders: count + top holders list ──
    inst_pct = _g("heldPercentInstitutions")
    if ih is not None and not ih.empty:
        n_holders = len(ih)
        total_pct = inst_pct * 100 if inst_pct else 0
        top3 = ", ".join(ih["Holder"].head(3).tolist()) if "Holder" in ih.columns else ""
        checks.append(("Institutionelle Unterstützung", n_holders >= 5,
                       f"{n_holders} Top-Institutionen · {total_pct:.0f}% inst. gehalten" + (f" · Top: {top3}" if top3 else "")))
    elif inst_pct is not None:
        checks.append(("Institutionelle Beteiligung", inst_pct * 100 > 20,
                       f"{inst_pct*100:.0f}% inst. gehalten (Detailliste nicht verfügbar)"))
    else:
        checks.append(("Institutionelle Unterstützung", False, "Nicht verfügbar"))

    # ── Profit margin ──
    pm = _g("profitMargins")
    if pm is not None:
        checks.append(("Gewinnmarge positiv", pm > 0, f"{pm*100:.1f}%"))

    return checks

def evaluate_technicals(df, info, spx_df=None, rs_ctx=None, rs_universe=None):
    checks = []; L = df.iloc[-1]; price = L["Close"]

    checks.append(("Preis ≥ $15", price >= 15, f"${price:,.2f}"))

    h52 = df["High"].rolling(252, min_periods=20).max().iloc[-1]
    if not np.isnan(h52):
        d = (price / h52 - 1) * 100
        checks.append(("Nahe am 52W-Hoch", d > -10, f"{d:+.1f}% vom Hoch (${h52:,.2f})"))

    avg_v = df["Volume"].tail(20).mean(); dol_v = avg_v * price / 1e6
    checks.append(("Dollar-Volumen ≥ $30 Mio.", dol_v >= 30, f"${dol_v:,.0f} Mio./Tag"))

    pc = df["Close"].pct_change()
    uv = df["Volume"].where(pc > 0).tail(50).sum(); dv = df["Volume"].where(pc < 0).tail(50).sum()
    if dv > 0:
        udv = uv / dv
        checks.append(("Up/Down Vol. Ratio ≥1.0", udv >= 1.0, f"{udv:.2f}" + (" (ideal ≥1.1)" if udv >= 1.1 else "")))

    rs_ctx = rs_ctx or _calc_rs_rating(df["Close"], spx_df["Close"] if spx_df is not None else None, universe_closes=rs_universe)
    rs = rs_ctx.get("rating") if isinstance(rs_ctx, dict) else None
    if isinstance(rs_ctx, dict):
        method = rs_ctx.get("method", "unavailable")
        method_note = (
            "CSV-Import"
            if method == "external_csv"
            else "Universums-Ranking"
            if method == "universe_percentile"
            else "Fallback-Proxy"
            if method == "weighted_proxy"
            else ""
        )
        universe_note = f" · {rs_ctx.get('universe_size', 0)} Aktien" if rs_ctx.get("universe_size") else ""
        if rs is not None:
            lbl = "Elite" if rs >= 90 else "Stark" if rs >= 80 else "Meiden (<70)" if rs < 70 else "OK"
            checks.append(("RS-Bewertung ≥80", rs >= 80, f"RS: {rs} ({lbl})" + (f" · {method_note}{universe_note}" if method_note else "")))
            checks.append(("RS-Bewertung ≥90", rs >= 90, f"Aktuell {rs}"))

        rs_line = rs_ctx.get("rs_line")
        ema21_rs = rs_ctx.get("ema21")
        sma50_rs = rs_ctx.get("sma50")
        if rs_line is not None and len(rs_line) > 0:
            rs_now = rs_line.iloc[-1]
            if ema21_rs is not None and len(ema21_rs) > 0 and pd.notna(ema21_rs.iloc[-1]):
                checks.append(("RS-Linie über 21-EMA", bool(rs_ctx.get("above_21")), f"{rs_now:.2f} vs {ema21_rs.iloc[-1]:.2f}"))
            if sma50_rs is not None and len(sma50_rs) > 0 and pd.notna(sma50_rs.iloc[-1]):
                checks.append(("RS-Linie über 50-SMA", bool(rs_ctx.get("above_50")), f"{rs_now:.2f} vs {sma50_rs.iloc[-1]:.2f}"))
            if rs_ctx.get("trend_5w") is not None:
                ex3 = rs_ctx.get("excess_return_3m")
                detail = f"Excess 3M: {ex3:+.1f}%" if ex3 is not None else "letzte 5 Wochen"
                checks.append(("RS-Linie steigt über 5 Wochen", bool(rs_ctx.get("trend_5w")), detail))
            if rs_ctx.get("trend_13w") is not None:
                ex6 = rs_ctx.get("excess_return_6m")
                detail = f"Excess 6M: {ex6:+.1f}%" if ex6 is not None else "letzte 13 Wochen"
                checks.append(("RS-Linie steigt über 13 Wochen", bool(rs_ctx.get("trend_13w")), detail))
            if rs_ctx.get("distance_to_high_pct") is not None:
                dist = rs_ctx.get("distance_to_high_pct")
                detail = "Neues RS-Hoch" if rs_ctx.get("new_high_52w") else f"{dist:+.1f}% zum RS-Hoch"
                checks.append(("RS-Linie nahe 52W-Hoch", bool(rs_ctx.get("near_high_52w")), detail))
    elif spx_df is not None and len(df) >= 126 and len(spx_df) >= 126:
        sp = (df["Close"].iloc[-1] / df["Close"].iloc[-126] - 1) * 100
        mp = (spx_df["Close"].iloc[-1] / spx_df["Close"].iloc[-126] - 1) * 100
        checks.append(("Relative Stärke vs. S&P (6M)", sp > mp, f"Aktie: {sp:+.1f}% · S&P: {mp:+.1f}% · Diff: {sp-mp:+.1f}%"))

    cmf = _calc_cmf(df, 20); cmf_val = cmf.iloc[-1] if len(cmf) > 0 else np.nan
    rat, meaning, _ = _cmf_rating(cmf_val)
    checks.append(("CMF Rating A oder B", rat in ("A","B"), f"CMF: {cmf_val:+.3f} → {rat} ({meaning})"))

    e21 = df["Close"].ewm(span=21).mean().iloc[-1]
    s10 = df["Close"].rolling(10).mean().iloc[-1]
    s50 = df["Close"].rolling(50).mean().iloc[-1]
    s200 = df["Close"].rolling(200).mean().iloc[-1]

    for nm, mv in [("21-EMA", e21), ("50-SMA", s50), ("200-SMA", s200)]:
        if not np.isnan(mv): checks.append((f"Kurs über {nm}", price > mv, f"{price:,.2f} vs {mv:,.2f}"))

    if not any(np.isnan(x) for x in [e21, s50, s200]):
        checks.append(("MA-Ordnung (21>50>200)", e21 > s50 > s200, f"21:{e21:,.0f} · 50:{s50:,.0f} · 200:{s200:,.0f}"))

    for nm, mv, thresh in [("10-SMA", s10, 10.0), ("21-EMA", e21, 14.0), ("50-SMA", s50, 25.0), ("200-SMA", s200, 70.0)]:
        if not np.isnan(mv):
            dist = (price / mv - 1) * 100
            extended = dist > thresh or dist < -thresh
            checks.append((f"Abstand {nm} (<{thresh:.0f}%)", not extended,
                           f"{dist:+.1f}% ({'überdehnt' if dist > 0 else 'darunter'}, Schwelle: ±{thresh:.0f}%)"))

    return checks, cmf_val, rs_ctx


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
    pct = c.pct_change(); vol_avg = v.rolling(50).mean()
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
    if len(wc) >= 6 and (wc.pct_change().tail(5) > 0).all():
        signs["positiv"].append(("5 positive Wochen in Folge", ""))

    if not np.isnan(s50) and abs(c.iloc[-1] / s50 - 1) < 0.01: signs["neutral"].append(("Rückkehr zur 50-Tage-Linie", ""))
    if h.iloc[-1] <= h.iloc[-2] and l.iloc[-1] >= l.iloc[-2]: signs["neutral"].append(("Inside Day", ""))
    r5 = (h.tail(5).max() - l.tail(5).min()) / c.iloc[-1] * 100
    if r5 < 3: signs["neutral"].append(("Enge Konsolidierung", f"5T-Range: {r5:.1f}%"))
    if not np.isnan(e21) and abs(l.iloc[-1] - e21) / e21 < 0.005: signs["neutral"].append(("Test der 21-EMA", ""))
    if not np.isnan(s50) and abs(l.iloc[-1] - s50) / s50 < 0.005: signs["neutral"].append(("Test der 50-SMA", ""))
    return signs

# ===== From tabs.py =====

def _tab_aktienbewertung():
    _init_workspace_state()
    _render_section_header("📋", "Aktienbewertung", "Fundamentals · Technik · Chartverhalten")

    ticker = _render_ticker_picker("stock", "Ticker oder Firmenname suchen", "NVDA oder Nvidia", show_quick=False)
    if not ticker:
        return

    with st.spinner(f"Lade {ticker} …"):
        df, info, qi, ai, ih, qe, ed, qraw, fmp_err = load_stock_full(ticker)
        spx_df = load_sp500_for_rs()
        rs_universe = load_cached_universe_closes_for_rs()

    if df is None or len(df) < 20:
        try:
            load_stock_full.clear()
        except Exception:
            pass
        with st.spinner(f"Lade {ticker} erneut (Live-Fallback) …"):
            df, info, qi, ai, ih, qe, ed, qraw, fmp_err = _load_stock_full_core(ticker)

    if spx_df is None or len(spx_df) < 120:
        try:
            load_sp500_for_rs.clear()
        except Exception:
            pass
        spx_df = _load_sp500_for_rs_core()

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

    act1, act2, act3 = st.columns([1, 1, 2])
    private_ok = _is_private_unlocked()
    with act1:
        if st.button("➕ Zur Watchlist", use_container_width=True, key="add_watch_stock", disabled=not private_ok):
            _add_watchlist_ticker(ticker)
            st.success(f"{ticker} zur Watchlist hinzugefügt.")
    with act2:
        if st.button("💼 Als Position merken", use_container_width=True, key="add_pos_stock", disabled=not private_ok):
            _upsert_position({
                "ticker": ticker,
                "buy_price": round(price, 2),
                "buy_price_usd": round(price, 2),
                "buy_date": last_date,
                "currency": "USD",
                "note": "",
            })
            st.success(f"{ticker} als Position vorgemerkt.")
    with act3:
        market_date = pd.Timestamp(df.index[-1]).date()
        today_local = datetime.now().date()
        freshness_note = "" if market_date >= today_local else " (letzter Handelstag)"
        st.caption(f"Datenstand: {last_date}{freshness_note} · Quelle Yahoo Finance")
        if not private_ok:
            st.caption("Watchlist und Depot-Speicherung sind gesperrt, bis du den privaten Bereich entsperrst.")

    st.markdown(
        f'<div class="summary-hero"><div class="hero-title">{name} ({ticker})</div>'
        f'<div class="hero-subtitle">Letzter Schluss {last_date}</div>'
        f'<div class="hero-action {"hero-good" if chg >= 0 else "hero-bad"}">Aktuell ${price:,.2f} · {chg:+.2f}% zum Vortag</div></div>',
        unsafe_allow_html=True,
    )

    # Today / recent changes
    atr_s = _atr(df, 21)
    atr_val = atr_s.iloc[-1] if len(atr_s) > 0 else np.nan
    atr_pct = (atr_val / price * 100) if not np.isnan(atr_val) else np.nan
    vol_ratio = float(L["Volume"] / df["Volume"].rolling(50).mean().iloc[-1]) if len(df) >= 50 and pd.notna(df["Volume"].rolling(50).mean().iloc[-1]) and df["Volume"].rolling(50).mean().iloc[-1] else np.nan
    rs_ctx = _calc_rs_rating(df["Close"], spx_df["Close"] if spx_df is not None else None, universe_closes=rs_universe)
    rs_ctx, rs_source_note = _apply_rs_source_override(ticker, rs_ctx)
    rs_hint = ""
    if isinstance(rs_ctx, dict):
        if rs_ctx.get("trend_5w") is True:
            rs_hint = "RS verbessert sich"
        elif rs_ctx.get("trend_5w") is False:
            rs_hint = "RS verliert etwas Tempo"

    rs_rating_val = rs_ctx.get("rating") if isinstance(rs_ctx, dict) else None
    rs_rating_detail = (
        "CSV-Import (rs-log/output)" if isinstance(rs_ctx, dict) and rs_ctx.get("method") == "external_csv"
        else "Perzentil im Universum" if isinstance(rs_ctx, dict) and rs_ctx.get("method") == "universe_percentile"
        else "Gewichtete RS" if isinstance(rs_ctx, dict) and rs_ctx.get("method") == "weighted_proxy"
        else "Vergleich zum S&P 500"
    )
    change_cards = [
        {"title": "Heute", "value": f"{chg:+.2f}%", "detail": f"Schlusskurs ${price:,.2f}"},
        {"title": "Volumen", "value": f"{vol_ratio:.2f}x 50-T-Schnitt" if not np.isnan(vol_ratio) else "—", "detail": "Werte >1 zeigen mehr Aktivität"},
        {"title": "Volatilität", "value": f"{atr_pct:.1f}%" if not np.isnan(atr_pct) else "—", "detail": "ATR als Risikomaß"},
    ]
    if rs_rating_val is not None:
        change_cards.append({"title": "RS-Rating", "value": f"{rs_rating_val}", "detail": rs_rating_detail})
    elif rs_hint:
        change_cards.append({"title": "Rel. Stärke", "value": rs_hint, "detail": "Vergleich zum S&P 500"})
    _render_change_cards(change_cards[:4])

    # Key metrics arranged in two rows for better readability
    rng_hl = L["High"] - L["Low"]
    cr_today = (L["Close"] - L["Low"]) / rng_hl * 100 if rng_hl > 0 else 50
    drr = ((df["High"] - df["Low"]) / df["Close"] * 100).tail(21).mean()
    beta = info.get("beta") if info else None
    cat_lbl, _ = _atr_category(atr_pct)

    top_metrics = st.columns(3)
    with top_metrics[0]:
        st.metric("Sektor", (info.get("sector", "—") if info else "—")[:22])
        st.caption("Geschäftsfeld der Aktie")
    with top_metrics[1]:
        st.metric("Branche", (info.get("industry", "—") if info else "—")[:28])
        st.caption("Feinere Untergruppe innerhalb des Sektors")
    with top_metrics[2]:
        st.metric("Closing Range", f"{cr_today:.0f}%")
        st.caption("Wie stark der Schluss im Tagesbereich lag")

    low_metrics = st.columns(4)
    with low_metrics[0]:
        st.metric("ATR (21T)", f"{atr_pct:.1f}%" if not np.isnan(atr_pct) else "—", cat_lbl)
    with low_metrics[1]:
        st.metric("DRR (Ø21T)", f"{drr:.2f}%")
    with low_metrics[2]:
        rs_metric_delta = "Elite" if rs_rating_val is not None and rs_rating_val >= 90 else "Stark" if rs_rating_val is not None and rs_rating_val >= 80 else ""
        st.metric("RS-Rating", f"{rs_rating_val}" if rs_rating_val is not None else "—", rs_metric_delta)
    with low_metrics[3]:
        st.metric("Beta", f"{beta:.2f}" if beta else "—", ">1.3 dynamisch" if beta and beta > 1.3 else "")

    if isinstance(rs_ctx, dict) and rs_ctx.get("distance_to_high_pct") is not None:
        st.caption(f"RS-Linie aktuell {rs_ctx.get('distance_to_high_pct'):+.1f}% von ihrem 52-Wochen-Hoch entfernt.")
    if isinstance(rs_source_note, dict) and rs_source_note.get("source") == "csv_latest":
        if rs_source_note.get("matched"):
            file_name = rs_source_note.get("file") or "rs_stocks*.csv"
            value_col = rs_source_note.get("value_column") or "Percentile"
            st.caption(f"RS-Rating kommt aus externer CSV ({file_name}) · Spalte: {value_col}.")
        elif rs_source_note.get("ok"):
            file_name = rs_source_note.get("file") or "rs_stocks*.csv"
            value_col = rs_source_note.get("value_column") or "Percentile"
            st.caption(f"CSV-Modus aktiv ({file_name}, Spalte: {value_col}), für diesen Ticker gab es keinen Eintrag. Fallback bleibt die bisherige Berechnung.")
        elif rs_source_note.get("error"):
            st.caption(f"CSV-Modus aktiv, Laden fehlgeschlagen: {rs_source_note.get('error')}")

    with st.expander("Kennzahlen kurz erklärt", expanded=False):
        _render_market_glossary(["Closing Range", "ATR (21T)", "DRR (Ø21T)", "Beta", "RS-Linie", "RS-Rating"])

    # Chart
    _ema21 = df["Close"].ewm(span=21).mean()
    _sma50 = df["Close"].rolling(50).mean()
    _sma200 = df["Close"].rolling(200).mean()
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
        template="plotly_dark", paper_bgcolor="#0f172a", plot_bgcolor="#0f172a",
        height=560, margin=dict(l=10, r=10, t=30, b=10), xaxis_rangeslider_visible=False,
        xaxis=dict(range=[six_months_ago, df.index[-1]], gridcolor="#1e293b"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5, font=dict(size=10)),
        yaxis=dict(title="", gridcolor="#1e293b"), yaxis2=dict(title="", gridcolor="#1e293b"), yaxis3=dict(title="", gridcolor="#1e293b"), xaxis2=dict(gridcolor="#1e293b"), xaxis3=dict(gridcolor="#1e293b"),
    )
    fig_stock.update_xaxes(showgrid=False)
    st.plotly_chart(fig_stock, use_container_width=True, key="stock_chart")

    col_f, col_t = st.columns(2)
    with col_f:
        st.markdown('<div class="info-card"><div class="card-label">Fundamentale Checkliste</div>', unsafe_allow_html=True)
        fc = evaluate_fundamentals(info, qi, ai, ih, qe, ed, qraw, fmp_err)
        fok = sum(1 for _, ok, _ in fc if ok)
        for label, ok, detail in fc:
            render_check(label, ok, detail)
        sc = "#22c55e" if fok >= 7 else "#f59e0b" if fok >= 4 else "#ef4444"
        st.markdown(f'<div style="text-align:center;padding:8px;color:{sc};">{fok}/{len(fc)} Kriterien erfüllt</div>', unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with col_t:
        st.markdown('<div class="info-card"><div class="card-label">Technische Checkliste</div>', unsafe_allow_html=True)
        tc, cmf_val, rs_val = evaluate_technicals(df, info, spx_df, rs_ctx=rs_ctx, rs_universe=rs_universe)
        tok = sum(1 for _, ok, _ in tc if ok)
        for label, ok, detail in tc:
            render_check(label, ok, detail)
        sc = "#22c55e" if tok >= 10 else "#f59e0b" if tok >= 6 else "#ef4444"
        st.markdown(f'<div style="text-align:center;padding:8px;color:{sc};">{tok}/{len(tc)} Kriterien erfüllt</div>', unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)

    signs = evaluate_chart_signs(df, rs_ctx=rs_ctx)
    st.markdown('<div class="card-label">Chartverhalten</div>', unsafe_allow_html=True)
    sc1, sc2, sc3 = st.columns(3)
    for col, key, label, color in [(sc1, "positiv", "✓ Positiv", "#22c55e"), (sc2, "negativ", "✗ Negativ", "#ef4444"), (sc3, "neutral", "○ Neutral", "#94a3b8")]:
        with col:
            st.markdown(f'<div class="info-card" style="border-color:{color}30;"><div class="card-label" style="color:{color};">{label}</div>', unsafe_allow_html=True)
            if signs[key]:
                for nm, dt in signs[key]:
                    st.markdown(f'<div style="padding:4px 0;border-bottom:1px solid #1e293b;"><div style="font-size:.84rem;color:{color};">{nm}</div><div style="font-size:.72rem;color:#64748b;">{dt}</div></div>', unsafe_allow_html=True)
            else:
                st.markdown('<div style="color:#4a5568;font-size:.85rem;">Keine Zeichen</div>', unsafe_allow_html=True)
            st.markdown("</div>", unsafe_allow_html=True)

    np_ = len(signs["positiv"]); nn = len(signs["negativ"]); nu = len(signs["neutral"])
    score = np_ - nn
    if score >= 3:
        verd, vc = "Starkes Chartbild", "#22c55e"
    elif score >= 1:
        verd, vc = "Leicht positiv", "#22c55e"
    elif score >= -1:
        verd, vc = "Gemischt", "#f59e0b"
    else:
        verd, vc = "Schwaches Chartbild", "#ef4444"
    st.markdown(
        f'<div class="info-card"><div class="card-label">Gesamtbewertung</div><div style="font-size:1rem;font-weight:700;color:{vc};">{verd}</div><div class="mini-help">{np_} Positiv · {nn} Negativ · {nu} Neutral · Score {score:+d}</div></div>',
        unsafe_allow_html=True,
    )


# ═══════════════════════════════════════════════════════
# TAB 4: NACH DEM KAUF (Book Ch.5.2 + 5.3)
# ═══════════════════════════════════════════════════════

def _tab_nach_kauf():
    _init_workspace_state()
    _render_section_header("🎯", "Nach dem Kauf", "Positions-Monitoring · P&L · Verhalten seit Kauf")

    private_ok = _is_private_unlocked()
    saved_positions = st.session_state.get("positions", []) if private_ok else []
    if not private_ok:
        st.info("Dein persönliches Depot ist gesperrt. Du kannst diesen Tab manuell nutzen oder den privaten Bereich entsperren, um gespeicherte Positionen zu laden und zu speichern.")
    st.caption("Keine Vorauswahl mehr. Gib den Ticker direkt ein und die Felder werden nur dann mit gespeicherten Daten befüllt, wenn die Position bereits existiert.")
    ticker = _render_ticker_picker("nachkauf_ticker", "Ticker oder Firmenname suchen", "NVDA oder Nvidia", show_quick=False)
    if not ticker:
        return

    with st.spinner(f"Lade {ticker} …"):
        df, info, *_ = load_stock_full(ticker)

    if df is None or len(df) < 30:
        st.error("Keine Kursdaten für diesen Ticker.")
        return

    price = float(df["Close"].iloc[-1])
    saved = next((p for p in saved_positions if p.get("ticker") == ticker), None)

    bc1, bc2, bc3, bc4 = st.columns(4)
    with bc1:
        currency_default = "USD"
        if saved and saved.get("currency") == "EUR":
            currency_default = "EUR"
        currency = st.selectbox("Währung", ["USD", "EUR"], index=0 if currency_default == "USD" else 1, key="nk_curr")
    with bc2:
        default_buy = round(float(saved.get("buy_price", price * 0.95)) if saved else float(price * 0.95), 2)
        label = "Kaufkurs ($)" if currency == "USD" else "Kaufkurs (€)"
        buy_price_input = st.number_input(label, min_value=0.01, value=default_buy, step=0.01, key="nk_price")
    with bc3:
        if saved and saved.get("buy_date"):
            try:
                saved_date = pd.Timestamp(saved["buy_date"]).date()
            except Exception:
                saved_date = df.index[-1].date()
        else:
            saved_date = df.index[-1].date()
        buy_date = st.date_input("Kaufdatum", value=saved_date, key="nk_date")
    with bc4:
        note_default = saved.get("note", "") if saved else ""
        note = st.text_input("Notiz", value=note_default, key="nk_note")

    if st.button("💾 Position speichern / aktualisieren", use_container_width=True, disabled=not private_ok):
        buy_price_usd = float(buy_price_input)
        eur_usd_rate = None
        if currency == "EUR":
            try:
                fx = yf.Ticker("EURUSD=X").history(start=pd.Timestamp(buy_date) - timedelta(days=5), end=pd.Timestamp(buy_date) + timedelta(days=3))
                if fx is not None and len(fx) > 0:
                    eur_usd_rate = float(fx["Close"].iloc[-1])
                    buy_price_usd = buy_price_input * eur_usd_rate
            except Exception:
                eur_usd_rate = None
            if eur_usd_rate is None:
                eur_usd_rate = 1.08
                buy_price_usd = buy_price_input * eur_usd_rate
        _upsert_position({
            "ticker": ticker,
            "buy_price": float(buy_price_input),
            "buy_price_usd": float(buy_price_usd),
            "buy_date": str(buy_date),
            "currency": currency,
            "note": note,
        })
        st.success(f"Position {ticker} gespeichert.")
    if not private_ok:
        st.caption("Speichern ist gesperrt, bis dein privater Bereich entsperrt ist.")

    if not (buy_price_input and buy_price_input > 0 and buy_date):
        return

    buy_price = buy_price_input
    eur_usd_rate = None
    if currency == "EUR":
        try:
            fx = yf.Ticker("EURUSD=X").history(start=pd.Timestamp(buy_date) - timedelta(days=5), end=pd.Timestamp(buy_date) + timedelta(days=3))
            if fx is not None and len(fx) > 0:
                eur_usd_rate = float(fx["Close"].iloc[-1])
                buy_price = buy_price_input * eur_usd_rate
        except Exception:
            pass
        if eur_usd_rate is None:
            eur_usd_rate = 1.08
            buy_price = buy_price_input * eur_usd_rate

    buy_ts = pd.Timestamp(buy_date)
    if df.index.tz is not None:
        buy_ts = buy_ts.tz_localize(df.index.tz)
    mask = df.index >= buy_ts
    if not mask.any():
        st.warning("Kaufdatum liegt nach den verfügbaren Daten.")
        return

    df_since = df.loc[mask]
    days_held = len(df_since)
    window = min(20, days_held)
    df_window = df_since.tail(window)
    pnl_pct = (price / buy_price - 1) * 100
    eur_note = f' · €{buy_price_input:,.2f} × {eur_usd_rate:.4f} = ${buy_price:,.2f}' if currency == "EUR" and eur_usd_rate else ""

    if pnl_pct >= 3:
        verdict_banner = ("Gesundes Verhalten", "hero-good")
    elif pnl_pct >= -2:
        verdict_banner = ("Gemischt, aber noch unkritisch", "hero-warn")
    else:
        verdict_banner = ("Schwächer als gewünscht", "hero-bad")

    st.markdown(
        f'<div class="summary-hero"><div class="hero-title">{ticker} seit Kauf</div><div class="hero-subtitle">Kauf: ${buy_price:,.2f} am {buy_date.strftime("%d.%m.%Y")}{eur_note}</div><div class="hero-action {verdict_banner[1]}">Aktuell ${price:,.2f} · {"+" if pnl_pct>=0 else ""}{pnl_pct:.1f}% · {days_held} Handelstage</div></div>',
        unsafe_allow_html=True,
    )

    c = df["Close"]; h = df["High"]; l = df["Low"]; v = df["Volume"]
    pct_chg = c.pct_change()
    vol_avg = v.rolling(50).mean()
    ema21 = c.ewm(span=21).mean(); sma50 = c.rolling(50).mean()
    rng_d = h - l
    cr_series = pd.Series(np.where(rng_d > 0, (c - l) / rng_d, 0.5), index=df.index)

    w_pct = pct_chg.loc[df_window.index]
    w_vol = v.loc[df_window.index]
    w_cr = cr_series.loc[df_window.index]
    w_vol_avg = vol_avg.loc[df_window.index]

    pos_signs = []; neg_signs = []
    if pnl_pct > 0:
        pos_signs.append(("Unmittelbare Stärke nach Kauf", f"+{pnl_pct:.1f}% Gewinn seit Einstieg ({days_held}T)"))
    elif pnl_pct < -3:
        neg_signs.append(("Kein Gewinn nach Kauf", f"{pnl_pct:.1f}% Verlust seit Einstieg ({days_held}T)"))

    avg_cr = w_cr.mean()
    if avg_cr > 0.6:
        pos_signs.append(("Schlüsse im oberen Bereich", f"Ø CR: {avg_cr:.0%} ({window}T)"))
    elif avg_cr < 0.35:
        neg_signs.append(("Schlüsse nahe Tagestief", f"Ø CR: {avg_cr:.0%} ({window}T)"))

    green_days = int((w_pct > 0).sum()); red_days = int((w_pct < 0).sum())
    green_ratio = green_days / window if window > 0 else 0
    if green_ratio >= 0.7:
        pos_signs.append(("Überwiegend grüne Tage", f"{green_days}/{window} im Plus ({green_ratio:.0%})"))
    elif green_ratio <= 0.4:
        neg_signs.append(("Überwiegend rote Tage", f"{red_days}/{window} im Minus ({1-green_ratio:.0%})"))

    if price > buy_price * 0.97:
        pos_signs.append(("Kurs über Kaufniveau", f"${price:,.2f} > ${buy_price:,.2f}"))
    else:
        neg_signs.append(("Schlusskurs unter Kaufniveau", f"${price:,.2f} < ${buy_price:,.2f}"))

    e21_val = ema21.iloc[-1]; s50_val = sma50.iloc[-1]
    if not np.isnan(e21_val) and price > e21_val:
        pos_signs.append(("Kurs über 21-EMA", f"${price:,.2f} > ${e21_val:,.2f}"))
    elif not np.isnan(e21_val):
        neg_signs.append(("Bruch der 21-EMA", f"${price:,.2f} < ${e21_val:,.2f}"))
    if not np.isnan(s50_val) and price > s50_val:
        pos_signs.append(("Kurs über 50-SMA", f"${price:,.2f} > ${s50_val:,.2f}"))
    elif not np.isnan(s50_val):
        neg_signs.append(("Bruch der 50-SMA", f"${price:,.2f} < ${s50_val:,.2f}"))

    if len(df_since) >= 3:
        rs_buy = c.loc[df_since.index[0]]
        rs_change = (price / rs_buy - 1) * 100 if rs_buy > 0 else 0
        if rs_change > 2:
            pos_signs.append(("Steigende Stärke seit Kauf", f"+{rs_change:.1f}% seit Kaufdatum"))
        elif rs_change < -3:
            neg_signs.append(("Schwäche seit Kauf", f"{rs_change:.1f}% seit Kaufdatum"))

    up_vol_w = v.loc[df_window.index].where(pct_chg.loc[df_window.index] > 0).mean()
    dn_vol_w = v.loc[df_window.index].where(pct_chg.loc[df_window.index] < 0).mean()
    if up_vol_w and dn_vol_w and not np.isnan(up_vol_w) and not np.isnan(dn_vol_w) and dn_vol_w > 0:
        ratio = up_vol_w / dn_vol_w
        if ratio > 1.2:
            pos_signs.append(("Akkumulationsmuster", f"Up/Down-Vol: {ratio:.2f} ({window}T)"))
        elif ratio < 0.8:
            neg_signs.append(("Verschlechterung Up/Down-Volume", f"Ratio: {ratio:.2f} ({window}T)"))

    stall_w = ((w_pct >= 0) & (w_pct < 0.005) & (w_vol > w_vol_avg * 0.95)).sum() if len(w_vol_avg.dropna()) > 0 else 0
    if stall_w >= 2:
        neg_signs.append(("Stau-Tage nahe Ausbruchspunkt", f"{stall_w} in {window}T"))

    c_window = c.loc[df_window.index]
    recent_high = c_window.iloc[-1] >= c_window.max() * 0.998
    weak_vol = w_vol.mean() < vol_avg.iloc[-1] * 0.8 if not np.isnan(vol_avg.iloc[-1]) else False
    if recent_high and weak_vol:
        neg_signs.append(("Neue Hochs bei magerem Volumen", "Nachlassende Kaufbereitschaft"))

    dist_w = int(((w_pct < 0) & (w_vol > v.shift(1).loc[df_window.index])).sum())
    if dist_w >= 3:
        neg_signs.append(("Häufung von Verkaufstagen", f"{dist_w} Dist.-Tage in {window}T"))

    if len(w_pct.dropna()) >= 3:
        worst_day = w_pct.min()
        if worst_day < -0.02:
            worst_idx = w_pct.idxmin()
            days_since_w = len(c.loc[worst_idx:]) - 1
            recovery = (c.iloc[-1] / c.loc[worst_idx] - 1) * 100
            if days_since_w >= 3 and recovery < abs(worst_day * 100) * 0.5:
                neg_signs.append(("Schwache Erholungsversuche", f"Schlimmster Tag: {worst_day*100:.1f}% · Erholung nach {days_since_w}T: {recovery:+.1f}%"))

    if pnl_pct < -7:
        neg_signs.append(("⚠ Stop-Loss: >7% Verlust", f"{pnl_pct:.1f}% — Sofort verkaufen"))

    summary_cards = [
        {"title": "P&L", "value": f"{pnl_pct:+.1f}%", "detail": f"{days_held} Handelstage seit Kauf"},
        {"title": "21-EMA", "value": "darüber" if not np.isnan(e21_val) and price > e21_val else "darunter", "detail": f"${e21_val:,.2f}" if not np.isnan(e21_val) else "—"},
        {"title": "50-SMA", "value": "darüber" if not np.isnan(s50_val) and price > s50_val else "darunter", "detail": f"${s50_val:,.2f}" if not np.isnan(s50_val) else "—"},
        {"title": "Warnzeichen", "value": str(len(neg_signs)), "detail": "Je mehr Warnzeichen, desto kleiner der Handlungsspielraum"},
    ]
    _render_change_cards(summary_cards)

    pc1, pc2 = st.columns(2)
    with pc1:
        st.markdown('<div class="info-card" style="border-color:#22c55e30;"><div class="card-label" style="color:#22c55e;">Positive Zeichen</div>', unsafe_allow_html=True)
        if pos_signs:
            for nm, dt in pos_signs:
                st.markdown(f'<div style="padding:4px 0;border-bottom:1px solid #1e293b;"><div style="font-size:.84rem;color:#22c55e;">{nm}</div><div style="font-size:.72rem;color:#64748b;">{dt}</div></div>', unsafe_allow_html=True)
        else:
            st.markdown('<div style="color:#4a5568;font-size:.85rem;">Keine positiven Zeichen</div>', unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with pc2:
        st.markdown('<div class="info-card" style="border-color:#ef444430;"><div class="card-label" style="color:#ef4444;">Warnzeichen</div>', unsafe_allow_html=True)
        if neg_signs:
            for nm, dt in neg_signs:
                st.markdown(f'<div style="padding:4px 0;border-bottom:1px solid #1e293b;"><div style="font-size:.84rem;color:#ef4444;">{nm}</div><div style="font-size:.72rem;color:#64748b;">{dt}</div></div>', unsafe_allow_html=True)
        else:
            st.markdown('<div style="color:#4a5568;font-size:.85rem;">Keine Warnzeichen</div>', unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)

    np2 = len(pos_signs); nn2 = len(neg_signs); score2 = np2 - nn2
    if nn2 == 0 and np2 >= 3:
        verdict2, vc2 = "Gesundes Verhalten — Position halten", "#22c55e"
    elif score2 >= 2:
        verdict2, vc2 = "Überwiegend positiv — beobachten", "#22c55e"
    elif score2 >= 0:
        verdict2, vc2 = "Gemischt — erhöhte Aufmerksamkeit", "#f59e0b"
    elif pnl_pct < -7:
        verdict2, vc2 = "Stop-Loss erreicht — Position schließen", "#ef4444"
    else:
        verdict2, vc2 = "Überwiegend negativ — Position überprüfen", "#ef4444"

    st.markdown(f'<div class="info-card"><div class="card-label">Nach-Kauf-Bewertung</div><div style="font-size:1rem;font-weight:700;color:{vc2};">{verdict2}</div><div class="mini-help">{np2} Positiv · {nn2} Negativ · Score {score2:+d}</div></div>', unsafe_allow_html=True)


def _tab_sektoranalyse():
    """Tab 2: Sector performance ranking table."""
    _render_section_header("🏭", "Sektoranalyse — Performance-Ranking", "S&P 500 ETFs · Bester Sektor oben")

    with st.spinner("Lade Sektor-Daten …"):
        sector_closes = load_sector_data()

    if sector_closes is None or len(sector_closes) < 5:
        st.error("Sektor-Daten konnten nicht geladen werden.")
        return

    # View mode selector
    sc1, sc2 = st.columns([2, 1])
    with sc1:
        mode = st.radio("Ansicht", ["Tagesansicht", "Wochenansicht"], horizontal=True, label_visibility="collapsed")
    with sc2:
        n_periods = st.selectbox("Historie", [10, 20, 30], index=1, format_func=lambda x: f"{x} {'Tage' if mode == 'Tagesansicht' else 'Wochen'}")

    is_daily = mode == "Tagesansicht"
    table, latest_ranked = build_sector_table(sector_closes, mode="daily" if is_daily else "weekly", n_periods=n_periods)

    if table is None or latest_ranked is None:
        st.warning("Nicht genug Daten für die Auswertung.")
        return

    # Determine the last trading date from the data
    last_date = sector_closes.index[-1].strftime("%d.%m.%Y")
    today = datetime.now().strftime("%d.%m.%Y")
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
            st.markdown(f'<div style="display:flex;justify-content:space-between;padding:4px 0;border-bottom:1px solid #1e293b;">'
                        f'<span style="font-size:.85rem;color:#e2e8f0;">{medal} {name}</span>'
                        f'<span style="font-size:.85rem;font-weight:700;color:{c};">{val:+.2f}%</span></div>',
                        unsafe_allow_html=True)
    with tc2:
        st.markdown('<div class="card-label">📉 BOTTOM 3 SEKTOREN</div>', unsafe_allow_html=True)
        for name, val in bot3.items():
            c = "#22c55e" if val > 0 else "#ef4444"
            st.markdown(f'<div style="display:flex;justify-content:space-between;padding:4px 0;border-bottom:1px solid #1e293b;">'
                        f'<span style="font-size:.85rem;color:#e2e8f0;">{name}</span>'
                        f'<span style="font-size:.85rem;font-weight:700;color:{c};">{val:+.2f}%</span></div>',
                        unsafe_allow_html=True)

    st.markdown("")

    # ── FULL TABLE with color coding ──
    st.markdown(f'<div class="card-label">PERFORMANCE-TABELLE ({mode.upper()} · letzte {n_periods} {"Tage" if is_daily else "Wochen"} · sortiert nach jüngstem {"Tag" if is_daily else "Woche"})</div>', unsafe_allow_html=True)

    # Style the table: green for positive, red for negative
    def _color_cell(val):
        if pd.isna(val): return ""
        if val > 1.5: return "background-color: #22c55e30; color: #22c55e; font-weight: 600"
        if val > 0: return "background-color: #22c55e15; color: #22c55e"
        if val < -1.5: return "background-color: #ef444430; color: #ef4444; font-weight: 600"
        if val < 0: return "background-color: #ef444415; color: #ef4444"
        return "color: #94a3b8"

    styled = table.style.map(_color_cell).format("{:+.2f}%", na_rep="—")

    st.dataframe(styled, use_container_width=True, height=min(500, 40 + len(table) * 38))

    # ── RANKING POSITION HISTORY ──
    st.markdown("")
    st.markdown(f'<div class="card-label">RANKING-VERLAUF (Position 1 = bester Sektor)</div>', unsafe_allow_html=True)

    # Build ranking history
    if is_daily:
        pct_all = sector_closes.pct_change() * 100
    else:
        pct_all = sector_closes.resample("W-FRI").last().pct_change() * 100
    pct_all = pct_all.dropna(how="all").tail(n_periods)

    # For each period, rank the sectors (1 = best)
    rankings = pct_all.rank(axis=1, ascending=False, method="min").astype(int)

    # Rename columns
    col_rename = {etf: f"{name} ({etf})" for etf, name in SECTOR_ETFS.items()}
    rankings.columns = [col_rename.get(c, c) for c in rankings.columns]

    # Plot ranking lines
    fig = go.Figure()
    colors_cycle = ["#06b6d4", "#ef4444", "#22c55e", "#f59e0b", "#a855f7", "#f97316",
                    "#3b82f6", "#ec4899", "#84cc16", "#64748b", "#14b8a6"]
    for i, col in enumerate(rankings.columns):
        fig.add_trace(go.Scatter(
            x=_x(rankings.index), y=_y(rankings[col]),
            name=col.split(" (")[0],  # short name
            line=dict(color=colors_cycle[i % len(colors_cycle)], width=1.5),
            mode="lines+markers", marker=dict(size=4),
        ))
    fig.update_layout(
        template="plotly_dark", paper_bgcolor="#111827", plot_bgcolor="#111827",
        margin=dict(l=0, r=0, t=10, b=0), height=350,
        yaxis=dict(autorange="reversed", gridcolor="#1e293b", tickfont=dict(size=9, color="#64748b"),
                   title="Rang", title_font=dict(size=9, color="#64748b"), dtick=1),
        xaxis=dict(gridcolor="#1e293b", tickfont=dict(size=9, color="#64748b")),
        legend=dict(orientation="h", yanchor="top", y=-0.15, xanchor="left", x=0,
                    font=dict(size=8, color="#94a3b8")),
        hovermode="x unified",
    )
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})



def _tab_marktanalyse():
    """Tab 1: Markt-Dashboard mit klarer Führung, kompaktem Startblock und Technik im Expander."""
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

    _render_section_header("📊", "Marktanalyse", "Index · Indikatoren · Marktbreite · Volatilität")

    c1, c2 = st.columns([3, 1])
    with c1:
        selected = st.radio("Index", available, horizontal=True, label_visibility="collapsed")
    with c2:
        sd = st.selectbox("Zeitraum", [60, 90, 130, 200], index=1, format_func=lambda x: f"{x} Tage")

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
    warning_items.append(("Intraday-Umkehrungen (10T)", nr < 3, f"{nr} negative Umkehrungen", w)); wc += int(w)
    lc = int(L.get("Low_CR_5d", 0)); w = lc >= 3
    warning_items.append(("Closing Range Häufung (5T)", lc < 3, f"{lc}/5 Tage Schluss im unteren 25%", w)); wc += int(w)
    st10 = int(df["Is_Stall"].tail(10).sum()); w = st10 >= 3
    warning_items.append(("Stau-Tage (10T)", st10 < 3, f"{st10} Stau-Tage", w)); wc += int(w)
    d21 = L["Dist_21EMA"]
    d50 = L["Dist_50SMA_pct"]; t50 = 7.0 if "Nasdaq" in selected else 5.0
    if not np.isnan(d50):
        w = d50 > t50 or d50 < 0
        warning_items.append(("50-SMA Abstand", not w, f"{d50:+.1f}% ({'über' if d50 > 0 else 'unter'} 50-SMA, Schwelle: {t50:.0f}%)", w)); wc += int(w)
    if not np.isnan(d21):
        w = d21 > 3.0 or d21 < 0
        warning_items.append(("21-EMA Abstand", not w, f"{d21:.1f} ATR ({'über' if d21 > 0 else 'unter'} 21-EMA, Schwelle: 3.0 ATR)", w)); wc += int(w)
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

    # Store ampel phase for sidebar badge
    st.session_state["_sidebar_ampel_phase"] = L.get("Ampel_Phase", "neutral")

    # Hero → Ampel (most important signal) → Change cards
    _render_hero_card(mode, tone, reasons, action, freshness)
    render_ampel_section(L)
    _render_change_cards(changes)

    # KPI cards — single row, color-coded by warning state
    dd = L["Dist_52w_pct"]
    dc = int(L["Dist_Count_25"])
    kpi_cols = st.columns(5)
    with kpi_cols[0]:
        idx_tone = "bad" if pct < -1.0 else "good" if pct > 0.5 else "neutral"
        _render_kpi_card(selected, f"{L['Close']:,.2f}", f"{pct:+.2f}%", idx_tone, "Tagesveränderung")
    with kpi_cols[1]:
        dc_tone = "bad" if dc >= 4 else "warn" if dc >= 2 else "neutral"
        _render_kpi_card("Dist.-Tage", str(dc), "⚠ Häufung" if dc >= 4 else "OK", dc_tone, "Abgabedruck 25T")
    with kpi_cols[2]:
        ema_tone = "warn" if not np.isnan(d21) and (d21 > 3.0 or d21 < 0) else "neutral"
        _render_kpi_card("21-EMA", f"{d21:.1f} ATR" if not np.isnan(d21) else "—", "", ema_tone, "Kurzfrist-Überdehnung")
    with kpi_cols[3]:
        sma_tone = "warn" if not np.isnan(d50) and (d50 > t50 or d50 < 0) else "neutral"
        _render_kpi_card("50-SMA", f"{d50:+.1f}%" if not np.isnan(d50) else "—", f"⚠ >{t50:.0f}%" if not np.isnan(d50) and d50 > t50 else "", sma_tone, "Mittelfrist-Überdehnung")
    with kpi_cols[4]:
        dd_tone = "bad" if not np.isnan(dd) and dd < -15 else "warn" if not np.isnan(dd) and dd < -5 else "neutral"
        _render_kpi_card("Drawdown", f"{dd:.1f}%" if not np.isnan(dd) else "—", "", dd_tone, "Abstand 52W-Hoch")

    with st.expander("Kennzahlen kurz erklärt", expanded=False):
        _render_market_glossary(["Dist.-Tage", "21-EMA", "50-SMA", "Drawdown"])

    st.plotly_chart(plot_price_with_volume(df, sd), use_container_width=True, config={"displayModeBar": False})

    with st.expander("Frühwarnzeichen und Warnzeichen", expanded=True):
        st.markdown('<div class="info-card"><div class="card-label">Warnlage</div>', unsafe_allow_html=True)
        for label, ok, detail, warn in warning_items:
            render_check(label, ok, detail, warn=warn)
        if wc == 0:
            st.markdown('<div style="text-align:center;padding:8px;color:#22c55e;">✓ Keine aktiven Warnzeichen</div>', unsafe_allow_html=True)
        elif wc <= 2:
            st.markdown(f'<div style="text-align:center;padding:8px;color:#f59e0b;">⚠ {wc} Warnzeichen</div>', unsafe_allow_html=True)
        else:
            st.markdown(f'<div style="text-align:center;padding:8px;color:#ef4444;">⚠ {wc} Warnzeichen — Risiko reduzieren</div>', unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with st.expander("Trendcheck, Ordnung und Sektorrotation", expanded=False):
        cl, cr_ = st.columns(2)
        with cl:
            st.markdown('<div class="info-card"><div class="card-label">Trendprüfung</div>', unsafe_allow_html=True)
            _c = L["Close"]; _l = L["Low"]; _e = L["EMA21"]; _s5 = L["SMA50"]; _s2 = L["SMA200"]
            eo = not np.isnan(_e); so = not np.isnan(_s5); s2o = not np.isnan(_s2)
            for nm, mv, ok, hk, ck in [("21-EMA", _e, eo, "EMA21_held", "Consec_Low_above_21"), ("50-SMA", _s5, so, "SMA50_held", "Consec_Low_above_50"), ("200-SMA", _s2, s2o, "SMA200_held", "Consec_Low_above_200")]:
                render_check(f"Schluss über {nm}", ok and _c > mv, f"{_c:,.0f} vs {mv:,.0f}" if ok else "")
                render_check(f"Tief über {nm}", ok and _l > mv, f"{_l:,.0f} vs {mv:,.0f}" if ok else "")
                render_check(f"{nm} gehalten", bool(L.get(hk, False)), "Schlusskurs darüber" if bool(L.get(hk, False)) else "Darunter")
                cc = int(L.get(ck, 0))
                render_check(f"3T Tief>{nm}", cc >= 3, f"{cc} Tage")
            st.markdown("</div>", unsafe_allow_html=True)

            st.markdown('<div class="info-card"><div class="card-label">Ordnung</div>', unsafe_allow_html=True)
            render_check("21-EMA > 50-SMA", eo and so and _e > _s5, f"{_e:,.0f} vs {_s5:,.0f}" if eo and so else "")
            render_check("21-EMA > 200-SMA", eo and s2o and _e > _s2, f"{_e:,.0f} vs {_s2:,.0f}" if eo and s2o else "")
            render_check("50-SMA > 200-SMA", so and s2o and _s5 > _s2, f"{_s5:,.0f} vs {_s2:,.0f}" if so and s2o else "")
            st.markdown("</div>", unsafe_allow_html=True)

        with cr_:
            if rot is not None and sd2:
                st.markdown('<div class="info-card"><div class="card-label">Sektorrotation (10T)</div>', unsafe_allow_html=True)
                for group, items in sd2.items():
                    st.markdown(f"**{group}:**")
                    for name, perf in items:
                        if perf is not None:
                            c = "#22c55e" if perf > 0 else "#ef4444"
                            st.markdown(f'<span style="color:{c};font-size:.85rem;">{name}: {perf:+.1f}%</span>', unsafe_allow_html=True)
                st.markdown("</div>", unsafe_allow_html=True)
            if div_r:
                st.markdown('<div class="info-card"><div class="card-label">Intermarket-Bild</div><div style="font-size:.72rem;color:#94a3b8;margin-bottom:8px;">Anzeige je Index: Tagesveränderung und Abstand zum vorherigen 20-Tage-Hoch. Negative Werte rechts bedeuten nicht zwingend einen negativen Tag, sondern Abstand zum Hoch.</div>', unsafe_allow_html=True)
                for r in div_r:
                    dist = r.get("dist_to_20d_high_pct", r.get("pct", np.nan))
                    day = r.get("day_pct", np.nan)
                    tone_c = "#22c55e" if pd.notna(day) and day >= 0 else "#ef4444"
                    dist_c = "#22c55e" if pd.notna(dist) and dist >= 0 else "#ef4444"
                    day_txt = f"{day:+.1f}% Tag" if pd.notna(day) else "n/a"
                    dist_txt = f"{dist:+.1f}% zum 20T-Hoch" if pd.notna(dist) else "n/a"
                    st.markdown(f'<div style="padding:4px 0;display:flex;justify-content:space-between;gap:12px;"><span style="color:{tone_c};font-weight:600;">{r["name"]}</span><span class="mini-help">{day_txt} · <span style="color:{dist_c};">{dist_txt}</span></span></div>', unsafe_allow_html=True)
                st.markdown("</div>", unsafe_allow_html=True)

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

        st.markdown('<div class="card-label">Volatilität und Stimmung</div>', unsafe_allow_html=True)
        sc1, sc2, sc3, sc4 = st.columns(4)
        for col, title in zip([sc1, sc2, sc3, sc4], ["VIX Regime", "VIXY Bestätigung", "Vol Regime", "Fragile Rally"]):
            with col:
                card = vol_summary[title]
                render_signal_card(title, card["status"], card["detail"], card["tone"])

        vc1, vc2 = st.columns(2)
        with vc1:
            if vix_df is not None:
                st.plotly_chart(plot_vix(vix_df, sd, title="VIX", price_color="#ef4444"), use_container_width=True, config={"displayModeBar": False})
            else:
                st.info("Keine VIX-Daten verfügbar")
        with vc2:
            if vixy_df is not None:
                st.plotly_chart(plot_vix(vixy_df, sd, title="VIXY", price_color="#f59e0b"), use_container_width=True, config={"displayModeBar": False})
            else:
                st.info("Keine VIXY-Daten verfügbar")

        if pd.notna(vol_latest.get("VIX_Close")) or pd.notna(vol_latest.get("VIXY_Close")):
            st.caption(
                f"VIX {vol_latest.get('VIX_Close', np.nan):.1f} · "
                f"VIXY {vol_latest.get('VIXY_Close', np.nan):.1f} · "
                f"S&P 500 5T {vol_latest.get('SPX_Ret_5d', np.nan) * 100:+.1f}%"
            )

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

    st.markdown("### 🔎 Tiefenanalyse")
    load_clicked = st.button("Tiefenanalyse laden", type="primary", key="load_deep_analysis_btn")
    if load_clicked:
        st.session_state["show_deep_analysis"] = True
    if not st.session_state.get("show_deep_analysis", False):
        st.caption("Die Tiefenanalyse wird nur bei Bedarf geladen, damit die Startansicht schnell bleibt.")
    else:
        with st.spinner("Lese Tiefenanalyse-Daten aus dem persistenten Datenspeicher …"):
            component_bundle = load_nyse_breadth_data()
        store = _get_price_store()
        benchmark_last = pd.Timestamp(data["S&P 500"].index[-1]).date() if "S&P 500" in data and len(data["S&P 500"]) else pd.Timestamp(df.index[-1]).date()
        refresh_at_raw = _get_cache_metadata(store, "last_refresh_at", "")
        refresh_date = None
        if refresh_at_raw:
            parsed = pd.to_datetime(refresh_at_raw, errors="coerce")
            if pd.notna(parsed):
                refresh_date = parsed.date()
        if component_bundle is None:
            benchmark_str = benchmark_last.strftime("%d.%m.%Y")
            active_job = _get_active_refresh_job(store)
            if active_job:
                st.warning(
                    f"Die Kursdaten werden aktualisiert (benötigter Stand: {benchmark_str}). "
                    "Bitte komm in ca. 10 Minuten erneut auf die Seite."
                )
            else:
                st.info(
                    f"Für die Tiefenanalyse fehlen aktuell Kursdaten (benötigter Stand: {benchmark_str}). "
                    "Die Aktualisierung läuft zeitgesteuert über GitHub Actions. "
                    "Im Bereich „Technisches Setup“ siehst du den letzten Job-Status und kannst bei Bedarf manuell starten."
                )
        else:
            br = _render_deep_analysis_content(component_bundle, sd, data)
            if br is not None and len(br):
                breadth_last = pd.Timestamp(br.index[-1]).date()
                refresh_matches_cache = refresh_date is not None and refresh_date >= breadth_last
                if breadth_last < benchmark_last and not refresh_matches_cache:
                    benchmark_str = benchmark_last.strftime("%d.%m.%Y")
                    breadth_str = breadth_last.strftime("%d.%m.%Y")
                    active_job = _get_active_refresh_job(store)
                    if active_job:
                        st.warning(
                            f"Kurse sind veraltet (Cache: {breadth_str}, benötigt: {benchmark_str}). "
                            "Die Aktualisierung läuft bereits. Bitte komm in ca. 10 Minuten erneut auf die Seite."
                        )
                    else:
                        st.info(
                            f"Kurse sind veraltet (Cache: {breadth_str}, benötigt: {benchmark_str}). "
                            "Die Aktualisierung läuft zeitgesteuert über GitHub Actions. "
                            "Im Bereich „Technisches Setup“ siehst du den letzten Job-Status und kannst bei Bedarf manuell starten."
                        )
                elif breadth_last < benchmark_last and refresh_matches_cache:
                    st.info(
                        f"Refresh wurde am {refresh_at_raw} UTC abgeschlossen. "
                        f"Die Tiefenanalyse zeigt aktuell den letzten verfügbaren Handelstag ({breadth_last.strftime('%d.%m.%Y')})."
                    )

    st.caption(f"Börse ohne Bauchgefühl · v3.2 · Stand: {L.name.strftime('%d.%m.%Y')}")


# ===== Main entry point =====




def _render_technical_setup_area():
    st.markdown("### ⚙️ Technisches Setup")
    st.caption("Privater Wartungsbereich für Datenbankaktualisierung, Worker-Status und Diagnose.")
    store = _get_price_store()
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

    btn_refresh, btn_rescue, btn_remap, btn_diag = st.columns(4)
    with btn_refresh:
        refresh_clicked = st.button("Aktienuniversum aktualisieren", use_container_width=True, disabled=bool(active_job), key="tech_refresh_universe")
    with btn_rescue:
        rescue_clicked = st.button("Fehlende nachladen", use_container_width=True, disabled=bool(active_job), key="tech_rescue_missing")
    with btn_remap:
        remap_clicked = st.button("Automatisch remappen", use_container_width=True, disabled=bool(active_job), key="tech_auto_remap")
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


def _tab_mein_bereich():
    if not _render_private_gate("🔐 Mein Bereich"):
        return
    _init_workspace_state()

    _render_section_header("🔐", "Mein Bereich", f"Speicher: {_workspace_backend_label()} · Workspace: {_workspace_scope()}")

    top_left, top_right = st.columns([1.4, 0.8])
    with top_left:
        updated_at = _get_cache_metadata(_get_price_store(), _workspace_meta_key("updated_at"), "")
        if updated_at:
            st.caption(f"Letzte Speicherung: {updated_at} UTC")
    with top_right:
        if _private_area_enabled() and st.button("🔒 Bereich sperren", use_container_width=True, key="private_lock_btn"):
            _lock_private_area()
            st.rerun()

    settings = _get_portfolio_settings()
    rs_source_labels = {
        "computed": "Berechnet (bisher)",
        "csv_latest": "Aus aktueller CSV (rs-log/output)",
    }
    inverse_rs_labels = {v: k for k, v in rs_source_labels.items()}
    current_rs_source = settings.get("rs_rating_source", "computed")
    selected_rs_label = st.selectbox(
        "RS-Rating Quelle",
        options=list(inverse_rs_labels.keys()),
        index=0 if current_rs_source == "computed" else 1,
        key="mein_bereich_rs_source",
        help="Steuert, ob das RS-Rating intern berechnet wird oder aus der neuesten CSV-Datei aus rs-log/output kommt.",
    )
    if st.button("Einstellungen speichern", use_container_width=False, key="mein_bereich_save_settings"):
        settings["rs_rating_source"] = inverse_rs_labels.get(selected_rs_label, "computed")
        _save_portfolio_settings(settings)
        st.success("Einstellungen gespeichert.")

    area_view = st.segmented_control(
        "Bereich",
        options=["📝 Arbeitsbereich", "💼 Depot 7.2", "⚙️ Technisches Setup"],
        default="📝 Arbeitsbereich",
        key="mein_bereich_view",
        label_visibility="collapsed",
    )

    if area_view == "📝 Arbeitsbereich":
        left, right = st.columns([1.0, 1.0])

        with left:
            st.markdown('<div class="workspace-card"><div class="card-label">Watchlist</div>', unsafe_allow_html=True)
            watchlist = st.session_state.get("watchlist", [])
            if watchlist:
                st.markdown('<div class="pill-wrap">' + "".join(f'<span class="pill">{t}</span>' for t in watchlist) + '</div>', unsafe_allow_html=True)
            else:
                st.markdown('<div class="workspace-note">Noch keine Ticker in der Watchlist.</div>', unsafe_allow_html=True)
            add_watch = st.text_input("Ticker zur Watchlist hinzufügen", value="", placeholder="NVDA", key="watch_add_input").upper().strip()
            col_add, col_remove = st.columns(2)
            with col_add:
                if st.button("Hinzufügen", use_container_width=True, key="watch_add_btn") and add_watch:
                    _add_watchlist_ticker(add_watch)
                    st.rerun()
            with col_remove:
                if watchlist:
                    rem = st.selectbox("Entfernen", options=watchlist, key="watch_remove_sel")
                    if st.button("Entfernen", use_container_width=True, key="watch_remove_btn"):
                        _remove_watchlist_ticker(rem)
                        st.rerun()
            st.markdown("</div>", unsafe_allow_html=True)

            st.markdown('<div class="workspace-card"><div class="card-label">Heutige To-dos</div>', unsafe_allow_html=True)
            todos = st.text_area("Notizen", value=st.session_state.get("todos", ""), height=220, key="todos_area", label_visibility="collapsed", placeholder="Zum Beispiel\nNVDA nach Earnings prüfen\nWatchlist nach Breakouts filtern")
            if st.button("To-dos speichern", use_container_width=True, key="save_todos"):
                st.session_state["todos"] = todos
                _sync_workspace()
                st.success("To-dos gespeichert.")
            st.markdown('<div class="workspace-note">Ideal für Tagesplan, offene Fragen und Beobachtungsliste.</div>', unsafe_allow_html=True)
            st.markdown("</div>", unsafe_allow_html=True)

        with right:
            st.markdown('<div class="workspace-card"><div class="card-label">Schnellzugriff</div>', unsafe_allow_html=True)
            recents = st.session_state.get("recent_tickers", [])
            if recents:
                st.markdown('<div class="pill-wrap">' + "".join(f'<span class="pill">{t}</span>' for t in recents[:12]) + '</div>', unsafe_allow_html=True)
            else:
                st.markdown('<div class="workspace-note">Noch keine zuletzt genutzten Ticker.</div>', unsafe_allow_html=True)
            st.markdown('<div class="workspace-note">Nutze diese Liste als tägliches Cockpit für deine wichtigsten Namen.</div>', unsafe_allow_html=True)
            st.markdown("</div>", unsafe_allow_html=True)

            st.markdown('<div class="workspace-card"><div class="card-label">Gespeicherte Positionen</div>', unsafe_allow_html=True)
            positions = st.session_state.get("positions", [])
            if positions:
                rows = []
                for pos in positions[:25]:
                    health = _simple_position_health(pos)
                    rows.append({
                        "Ticker": pos.get("ticker", ""),
                        "Stück": _safe_float(pos.get("shares"), 0.0),
                        "Kaufdatum": pos.get("buy_date", ""),
                        "Einstand": pos.get("buy_price", np.nan),
                        "Währung": pos.get("currency", "USD"),
                        "Status": health.get("status", "") if health else "",
                        "P&L %": round(float(health["pnl"]), 2) if health and health.get("pnl") is not None and not np.isnan(health.get("pnl")) else np.nan,
                        "Notiz": pos.get("note", ""),
                    })
                st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
                remove_pos = st.selectbox("Position entfernen", options=[""] + [p.get("ticker", "") for p in positions], key="pos_remove_sel")
                if remove_pos and st.button("Position löschen", use_container_width=True, key="pos_remove_btn"):
                    _remove_position(remove_pos)
                    st.rerun()
            else:
                st.markdown('<div class="workspace-note">Noch keine Positionen gespeichert. Reale Depotpositionen mit Stückzahl pflegst du im Tab „Depot 7.2“.</div>', unsafe_allow_html=True)
            st.markdown("</div>", unsafe_allow_html=True)

    elif area_view == "💼 Depot 7.2":
        _render_portfolio_72_area()

    elif area_view == "⚙️ Technisches Setup":
        _render_technical_setup_area()


def main():
    configure_page()
    inject_css()
    _render_workspace_sidebar()
    st.title("BÖRSE OHNE BAUCHGEFÜHL")

    main_view = st.segmented_control(
        "Navigation",
        options=["📊 Marktanalyse", "🏭 Sektoranalyse", "📋 Aktienbewertung", "🎯 Nach dem Kauf", "🔐 Mein Bereich"],
        default="📊 Marktanalyse",
        key="main_view",
        label_visibility="collapsed",
    )

    if main_view == "📊 Marktanalyse":
        _tab_marktanalyse()
    elif main_view == "🏭 Sektoranalyse":
        _tab_sektoranalyse()
    elif main_view == "📋 Aktienbewertung":
        _tab_aktienbewertung()
    elif main_view == "🎯 Nach dem Kauf":
        _tab_nach_kauf()
    elif main_view == "🔐 Mein Bereich":
        _tab_mein_bereich()

if __name__ == "__main__":
    main()
