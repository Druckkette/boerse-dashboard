"""Scheduled ATR monitor for open depot positions.

The monitor is intentionally UI-independent so it can run from GitHub Actions.
It reads the persisted Streamlit workspace, reconstructs open positions from the
saved Trade Republic CSV where available, evaluates ATR loss thresholds and sends
Pushover alerts.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import requests
import yfinance as yf

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import app


MONITOR_STATE_FIELD = "position_monitor_state"
REFERENCE_HIGH_SINCE_BUY = "high_since_buy"
REFERENCE_ENTRY = "entry"
REFERENCE_BOTH = "both"
REFERENCE_LABELS = {
    REFERENCE_HIGH_SINCE_BUY: "vom Hoch seit Kauf",
    REFERENCE_ENTRY: "vom Einstand",
    REFERENCE_BOTH: "vom Hoch seit Kauf oder Einstand",
}


@dataclass
class PositionCandidate:
    ticker: str
    name: str
    shares: float
    entry_price: float | None = None
    buy_date: str = ""
    isin: str = ""
    source: str = "workspace"


@dataclass
class MonitorConfig:
    enabled: bool
    threshold_atr: float
    reference: str
    atr_period: int
    lookback_days: int
    interval_minutes: float
    cooldown_hours: float
    pushover_user_keys: list[str]
    pushover_app_token: str
    dry_run: bool = False

    @classmethod
    def from_settings(cls, settings: dict[str, Any], *, dry_run: bool = False) -> "MonitorConfig":
        user_keys = _split_tokens(settings.get("position_monitor_pushover_user_key"))
        if not user_keys:
            user_keys = _split_tokens(os.environ.get("PUSHOVER_USER_KEY", ""))
        app_token = str(settings.get("position_monitor_pushover_app_token") or "").strip()
        if not app_token:
            app_token = os.environ.get("PUSHOVER_APP_TOKEN", "").strip()

        return cls(
            enabled=_truthy(settings.get("position_monitor_enabled", False)),
            threshold_atr=_coerce_float(settings.get("position_monitor_threshold_atr"), 1.5, minimum=0.1),
            reference=_normalize_reference(settings.get("position_monitor_reference", REFERENCE_HIGH_SINCE_BUY)),
            atr_period=int(_coerce_float(settings.get("position_monitor_atr_period"), 14, minimum=2)),
            lookback_days=int(_coerce_float(settings.get("position_monitor_lookback_days"), 420, minimum=60)),
            interval_minutes=_coerce_float(settings.get("position_monitor_interval_minutes"), 5, minimum=5),
            cooldown_hours=_coerce_float(settings.get("position_monitor_cooldown_hours"), 18, minimum=0),
            pushover_user_keys=user_keys,
            pushover_app_token=app_token,
            dry_run=dry_run,
        )


@dataclass
class ATRAlert:
    ticker: str
    name: str
    reference_label: str
    close: float
    atr: float
    drop_atr: float
    drop_abs: float
    reference_price: float
    threshold_atr: float
    trade_date: str
    source: str
    isin: str = ""

    @property
    def title(self) -> str:
        return f"ATR-Alarm: {self.ticker}"

    @property
    def body(self) -> str:
        return (
            f"{self.ticker}: {self.drop_atr:.1f} ATR {self.reference_label}. "
            f"Schluss {self.close:.2f}, Referenz {self.reference_price:.2f}, ATR {self.atr:.2f}."
        )


def _truthy(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "ja", "on", "aktiviert"}
    return bool(value)


def _split_tokens(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        raw_items = [str(item or "") for item in value]
    else:
        raw = str(value or "")
        raw_items = raw.replace(",", "\n").replace(";", "\n").splitlines()

    tokens: list[str] = []
    seen: set[str] = set()
    for item in raw_items:
        token = "".join(str(item or "").strip().split())
        if not token or token in seen:
            continue
        seen.add(token)
        tokens.append(token)
    return tokens


def _coerce_float(value: Any, default: float, *, minimum: float | None = None) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        parsed = default
    if math.isnan(parsed) or math.isinf(parsed):
        parsed = default
    if minimum is not None:
        parsed = max(parsed, minimum)
    return parsed


def _normalize_reference(value: Any) -> str:
    raw = str(value or "").strip().lower()
    aliases = {
        "hoch seit kauf": REFERENCE_HIGH_SINCE_BUY,
        "high": REFERENCE_HIGH_SINCE_BUY,
        "trailing": REFERENCE_HIGH_SINCE_BUY,
        "entry": REFERENCE_ENTRY,
        "einstand": REFERENCE_ENTRY,
        "buy": REFERENCE_ENTRY,
        "beides": REFERENCE_BOTH,
        "both": REFERENCE_BOTH,
    }
    if raw in {REFERENCE_HIGH_SINCE_BUY, REFERENCE_ENTRY, REFERENCE_BOTH}:
        return raw
    return aliases.get(raw, REFERENCE_HIGH_SINCE_BUY)


def _safe_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(parsed) or math.isinf(parsed):
        return None
    return parsed


def _safe_date(value: Any) -> pd.Timestamp | None:
    if value in (None, ""):
        return None
    try:
        parsed = pd.Timestamp(value)
    except Exception:
        return None
    if pd.isna(parsed):
        return None
    if parsed.tzinfo is not None:
        parsed = parsed.tz_convert(None)
    return parsed.normalize()


def _state_key() -> str:
    return app._workspace_meta_key(MONITOR_STATE_FIELD)


def load_monitor_state(store: dict[str, Any]) -> dict[str, Any]:
    raw = app._get_cache_metadata(store, _state_key(), "{}")
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def save_monitor_state(store: dict[str, Any], state: dict[str, Any]) -> None:
    state = state if isinstance(state, dict) else {}
    app._set_cache_metadata(store, _state_key(), json.dumps(state, ensure_ascii=False, separators=(",", ":")))


def load_workspace() -> tuple[dict[str, Any], dict[str, Any]]:
    store = app._get_price_store()
    app._init_price_cache_db(store)
    workspace = app._load_workspace_from_store()
    return workspace if isinstance(workspace, dict) else {}, store


def load_positions_from_workspace(workspace: dict[str, Any]) -> tuple[list[PositionCandidate], list[dict[str, Any]]]:
    csv_positions, diagnostics = _positions_from_saved_csv(workspace)
    if csv_positions:
        return csv_positions, diagnostics
    return _positions_from_manual_workspace(workspace), diagnostics


def _positions_from_saved_csv(workspace: dict[str, Any]) -> tuple[list[PositionCandidate], list[dict[str, Any]]]:
    csv_state = app._normalize_depot_curve_csv_import_state(workspace.get(app.DEPOT_CURVE_CSV_IMPORT_KEY, {}))
    tx_df = app._depot_curve_csv_records_to_frame(csv_state.get("records", []))
    if tx_df is None or tx_df.empty:
        return [], []

    overrides = csv_state.get("isin_overrides", {}) or {}
    ticker_map, diagnostics = app._resolve_isin_ticker_map(tx_df, overrides=overrides)
    open_df, derivative_df = app._reconstruct_open_positions_from_transactions(tx_df)
    if open_df is None or open_df.empty:
        return [], diagnostics

    positions: list[PositionCandidate] = []
    for _, row in open_df.iterrows():
        isin = str(row.get("isin", "") or "").upper().strip()
        ticker = app._normalize_single_ticker(ticker_map.get(isin, ""))
        shares = _safe_float(row.get("shares")) or 0.0
        if not ticker or shares <= 0:
            continue
        positions.append(
            PositionCandidate(
                ticker=ticker,
                name=str(row.get("name", "") or ticker),
                shares=shares,
                entry_price=_safe_float(row.get("avg_buy_price")),
                buy_date=str(row.get("first_buy_date", "") or ""),
                isin=isin,
                source="trade_republic_csv",
            )
        )

    if derivative_df is not None and not derivative_df.empty:
        diagnostics.append({"warning": f"{len(derivative_df)} Derivate ohne Yahoo-Ticker wurden uebersprungen."})
    return positions, diagnostics


def _positions_from_manual_workspace(workspace: dict[str, Any]) -> list[PositionCandidate]:
    positions: list[PositionCandidate] = []
    for raw in workspace.get("positions", []) or []:
        if not isinstance(raw, dict):
            continue
        ticker = app._normalize_single_ticker(raw.get("ticker", ""))
        shares = _safe_float(raw.get("shares")) or 0.0
        if not ticker or shares <= 0:
            continue
        entry = (
            _safe_float(raw.get("buy_price"))
            or _safe_float(raw.get("entry"))
            or _safe_float(raw.get("avg_buy_price"))
        )
        positions.append(
            PositionCandidate(
                ticker=ticker,
                name=str(raw.get("name", "") or ticker),
                shares=shares,
                entry_price=entry,
                buy_date=str(raw.get("buy_date") or raw.get("pivot_tag") or ""),
                source="manual_workspace",
            )
        )
    return positions


def fetch_ohlc(ticker: str, *, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    raw = yf.download(
        ticker,
        start=start.to_pydatetime(),
        end=(end + pd.Timedelta(days=1)).to_pydatetime(),
        auto_adjust=True,
        progress=False,
        threads=False,
    )
    if raw is None or raw.empty:
        return pd.DataFrame()
    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = raw.columns.get_level_values(0)
    frame = raw.copy()
    frame.index = pd.to_datetime(frame.index, errors="coerce").tz_localize(None).normalize()
    frame = frame[~frame.index.isna()]
    frame = frame[~frame.index.duplicated(keep="last")].sort_index()
    for column in ("Open", "High", "Low", "Close", "Volume"):
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame.dropna(subset=["High", "Low", "Close"], how="any")


def atr_series(frame: pd.DataFrame, period: int) -> pd.Series:
    if frame is None or frame.empty or not {"High", "Low", "Close"}.issubset(frame.columns):
        return pd.Series(dtype=float)
    high = pd.to_numeric(frame["High"], errors="coerce")
    low = pd.to_numeric(frame["Low"], errors="coerce")
    close = pd.to_numeric(frame["Close"], errors="coerce")
    prev_close = close.shift(1)
    true_range = pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return true_range.rolling(int(max(period, 2)), min_periods=int(max(period, 2))).mean()


def evaluate_position(position: PositionCandidate, frame: pd.DataFrame, config: MonitorConfig) -> ATRAlert | None:
    if frame is None or frame.empty or len(frame) < config.atr_period + 2:
        return None

    frame = frame.sort_index().dropna(subset=["High", "Low", "Close"], how="any")
    if frame.empty:
        return None

    atr_values = atr_series(frame, config.atr_period).dropna()
    if atr_values.empty:
        return None

    close = _safe_float(frame["Close"].iloc[-1])
    atr = _safe_float(atr_values.iloc[-1])
    if close is None or atr is None or atr <= 0:
        return None

    buy_ts = _safe_date(position.buy_date)
    since_buy = frame[frame.index >= buy_ts] if buy_ts is not None else frame
    if since_buy.empty:
        since_buy = frame

    candidates: list[tuple[str, float, float]] = []
    if config.reference in {REFERENCE_HIGH_SINCE_BUY, REFERENCE_BOTH}:
        high_since_buy = _safe_float(since_buy["High"].max())
        if high_since_buy is not None:
            candidates.append((REFERENCE_LABELS[REFERENCE_HIGH_SINCE_BUY], high_since_buy, high_since_buy - close))

    if config.reference in {REFERENCE_ENTRY, REFERENCE_BOTH} and position.entry_price:
        candidates.append((REFERENCE_LABELS[REFERENCE_ENTRY], float(position.entry_price), float(position.entry_price) - close))

    if not candidates:
        return None

    reference_label, reference_price, drop_abs = max(candidates, key=lambda item: item[2] / atr)
    drop_atr = drop_abs / atr
    if drop_atr < config.threshold_atr:
        return None

    last_date = pd.Timestamp(frame.index[-1]).strftime("%Y-%m-%d")
    return ATRAlert(
        ticker=position.ticker,
        name=position.name,
        reference_label=reference_label,
        close=float(close),
        atr=float(atr),
        drop_atr=float(drop_atr),
        drop_abs=float(drop_abs),
        reference_price=float(reference_price),
        threshold_atr=float(config.threshold_atr),
        trade_date=last_date,
        source=position.source,
        isin=position.isin,
    )


def should_alert(alert: ATRAlert, state: dict[str, Any], config: MonitorConfig, now: datetime) -> bool:
    alerts_state = state.setdefault("alerts", {})
    prior = alerts_state.get(alert.ticker, {}) if isinstance(alerts_state, dict) else {}
    raw_last = prior.get("last_alerted_at") if isinstance(prior, dict) else ""
    if not raw_last or config.cooldown_hours <= 0:
        return True
    try:
        last = pd.Timestamp(raw_last).to_pydatetime()
        if last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)
    except Exception:
        return True
    return now - last >= timedelta(hours=config.cooldown_hours)


def should_run_for_interval(state: dict[str, Any], config: MonitorConfig, now: datetime) -> bool:
    raw_last = str(state.get("last_evaluated_at") or "").strip()
    if not raw_last:
        return True
    try:
        last = pd.Timestamp(raw_last).to_pydatetime()
        if last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)
    except Exception:
        return True
    return now - last >= timedelta(minutes=max(config.interval_minutes, 5))


def mark_alerted(alert: ATRAlert, state: dict[str, Any], now: datetime) -> None:
    alerts_state = state.setdefault("alerts", {})
    alerts_state[alert.ticker] = {
        "last_alerted_at": now.astimezone(timezone.utc).isoformat(),
        "last_trade_date": alert.trade_date,
        "last_drop_atr": round(alert.drop_atr, 4),
        "last_close": round(alert.close, 4),
        "reference_label": alert.reference_label,
    }


def send_pushover_alerts(
    alerts: list[ATRAlert],
    user_keys: list[str],
    app_token: str,
    *,
    dry_run: bool = False,
) -> list[dict[str, Any]]:
    if not alerts:
        return []
    if dry_run:
        return [{"ok": True, "dry_run": True, "ticker": alert.ticker, "users": len(user_keys)} for alert in alerts]
    if not user_keys:
        raise RuntimeError("Pushover User Key fehlt. Trage ihn in Streamlit oder PUSHOVER_USER_KEY ein.")
    if not app_token:
        raise RuntimeError("Pushover App Token fehlt. Erstelle eine Pushover-App und setze PUSHOVER_APP_TOKEN.")

    results: list[dict[str, Any]] = []
    for alert in alerts:
        for user_key in user_keys:
            response = requests.post(
                "https://api.pushover.net/1/messages.json",
                data={
                    "token": app_token,
                    "user": user_key,
                    "title": alert.title,
                    "message": alert.body,
                    "priority": 0,
                    "sound": "pushover",
                    "timestamp": int(datetime.now(timezone.utc).timestamp()),
                },
                timeout=20,
            )
            body = response.text[:500]
            ok = False
            try:
                payload = response.json()
                ok = response.status_code == 200 and int(payload.get("status", 0)) == 1
            except Exception:
                ok = response.status_code == 200
            results.append(
                {
                    "ok": ok,
                    "ticker": alert.ticker,
                    "status_code": response.status_code,
                    "reason": body,
                }
            )
    return results


def run_monitor(*, dry_run: bool = False, force: bool = False) -> dict[str, Any]:
    workspace, store = load_workspace()
    settings = workspace.get("portfolio_settings", {}) if isinstance(workspace.get("portfolio_settings"), dict) else {}
    config = MonitorConfig.from_settings(settings, dry_run=dry_run)
    now = datetime.now(timezone.utc)
    state = load_monitor_state(store)
    state["last_started_at"] = now.isoformat()

    if not config.enabled:
        state["last_finished_at"] = datetime.now(timezone.utc).isoformat()
        state["last_summary"] = {"ok": True, "skipped": True, "reason": "disabled"}
        save_monitor_state(store, state)
        return {"ok": True, "skipped": True, "reason": "position_monitor_disabled"}

    if not force and not should_run_for_interval(state, config, now):
        state["last_finished_at"] = datetime.now(timezone.utc).isoformat()
        state["last_summary"] = {
            "ok": True,
            "skipped": True,
            "reason": "interval_wait",
            "interval_minutes": config.interval_minutes,
            "last_evaluated_at": state.get("last_evaluated_at", ""),
        }
        save_monitor_state(store, state)
        return state["last_summary"]

    positions, diagnostics = load_positions_from_workspace(workspace)
    state["last_evaluated_at"] = now.isoformat()
    end = pd.Timestamp(now.date()).normalize()
    alerts: list[ATRAlert] = []
    skipped: list[dict[str, Any]] = []

    for position in positions:
        buy_ts = _safe_date(position.buy_date)
        start = end - pd.Timedelta(days=config.lookback_days)
        if buy_ts is not None:
            start = min(start, buy_ts - pd.Timedelta(days=max(config.atr_period * 3, 45)))
        frame = fetch_ohlc(position.ticker, start=start, end=end)
        alert = evaluate_position(position, frame, config)
        if alert is None:
            skipped.append({"ticker": position.ticker, "reason": "no_alert_or_no_data"})
            continue
        if should_alert(alert, state, config, now):
            alerts.append(alert)
        else:
            skipped.append({"ticker": position.ticker, "reason": "cooldown", "drop_atr": round(alert.drop_atr, 3)})

    send_results = send_pushover_alerts(
        alerts,
        config.pushover_user_keys,
        config.pushover_app_token,
        dry_run=config.dry_run,
    ) if alerts else []
    sent_tickers = {row["ticker"] for row in send_results if row.get("ok")}
    for alert in alerts:
        if config.dry_run or alert.ticker in sent_tickers:
            mark_alerted(alert, state, now)

    finished = datetime.now(timezone.utc)
    summary = {
        "ok": True,
        "checked": len(positions),
        "alerts": len(alerts),
        "sent": len(sent_tickers) if not config.dry_run else len(alerts),
        "dry_run": config.dry_run,
        "threshold_atr": config.threshold_atr,
        "interval_minutes": config.interval_minutes,
        "reference": config.reference,
        "diagnostics": diagnostics[:12],
        "alerts_detail": [asdict(alert) for alert in alerts],
        "send_results": send_results,
        "skipped": skipped[:25],
    }
    state["last_finished_at"] = finished.isoformat()
    state["last_summary"] = summary
    save_monitor_state(store, state)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Check open positions for ATR loss and send Pushover alerts.")
    parser.add_argument("--dry-run", action="store_true", help="Evaluate alerts without sending Pushover messages.")
    parser.add_argument("--force", action="store_true", help="Ignore the configured monitor interval.")
    args = parser.parse_args()
    print(json.dumps(run_monitor(dry_run=args.dry_run, force=args.force), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
