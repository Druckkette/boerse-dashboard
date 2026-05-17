"""Reusable calculations for sell-decision metrics.

This module is intentionally data-provider agnostic. Callers pass already loaded
Yahoo/yfinance OHLC frames so Streamlit caching stays in the app layer.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import numpy as np
import pandas as pd


REQUIRED_OHLC_COLUMNS = {"High", "Low", "Close", "Volume"}


def _safe_float(value, default=None):
    if value is None:
        return default
    try:
        if pd.isna(value):
            return default
    except Exception:
        pass
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    if np.isnan(out):
        return default
    return out


def _iso_date(value) -> str:
    if value is None:
        return ""
    try:
        parsed = pd.Timestamp(value)
    except Exception:
        return str(value or "").strip()
    if pd.isna(parsed):
        return ""
    return parsed.strftime("%Y-%m-%d")


def _clean_ohlc_frame(frame: pd.DataFrame | None) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame()
    df = frame.copy()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df.index = pd.to_datetime(df.index).tz_localize(None).normalize()
    df = df.sort_index()
    for col in ["Open", "High", "Low", "Close", "Volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "Close" in df.columns:
        df = df.dropna(subset=["Close"])
    return df[~df.index.duplicated(keep="last")]


def _sma(series: pd.Series, window: int) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").rolling(window, min_periods=window).mean()


def _atr(frame: pd.DataFrame, window: int = 14) -> pd.Series:
    high = pd.to_numeric(frame["High"], errors="coerce")
    low = pd.to_numeric(frame["Low"], errors="coerce")
    prev_close = pd.to_numeric(frame["Close"], errors="coerce").shift(1)
    true_range = pd.concat([high - low, (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
    return true_range.rolling(window, min_periods=window).mean()


def _trailing_true_count(mask: pd.Series) -> int:
    if mask is None or mask.empty:
        return 0
    count = 0
    for value in mask.fillna(False).iloc[::-1]:
        if bool(value):
            count += 1
        else:
            break
    return int(count)


def _last_float(series: pd.Series | None):
    if series is None or series.empty:
        return None
    valid = pd.to_numeric(series, errors="coerce").dropna()
    if valid.empty:
        return None
    return float(valid.iloc[-1])


def _series_float_at_or_after(series: pd.Series, date_value):
    if series is None or series.empty:
        return None
    ts = pd.Timestamp(date_value).tz_localize(None).normalize()
    subset = series[series.index >= ts]
    if subset.empty:
        return None
    return _safe_float(subset.iloc[0])


def _previous_series_float(series: pd.Series, date_value):
    if series is None or series.empty:
        return None
    ts = pd.Timestamp(date_value).tz_localize(None).normalize()
    subset = series[series.index < ts]
    if subset.empty:
        return None
    return _safe_float(subset.iloc[-1])


def _last_index_date(series: pd.Series | pd.DataFrame | None) -> str:
    if series is None or len(series) == 0:
        return ""
    try:
        return pd.Timestamp(series.index[-1]).strftime("%Y-%m-%d")
    except Exception:
        return ""


def _error(message: str, ticker: str = "", benchmark_ticker: str = "SPY") -> dict[str, Any]:
    return {
        "ok": False,
        "error": message,
        "ticker": str(ticker or "").upper(),
        "benchmark_ticker": str(benchmark_ticker or "SPY").upper(),
        "metrics": {},
        "manual_defaults": {},
        "as_of": "",
    }


def build_sell_decision_metrics_payload(
    *,
    ticker: str,
    buy_date,
    buy_price: float,
    shares: float,
    price_frame: pd.DataFrame,
    benchmark_frame: pd.DataFrame,
    benchmark_ticker: str = "SPY",
    currency: str = "USD",
    pnl_abs_eur=None,
    fx_rate_to_eur=None,
) -> dict[str, Any]:
    """Build all reusable sell-decision metrics from OHLC inputs."""
    clean_ticker = str(ticker or "").upper().strip()
    clean_benchmark = str(benchmark_ticker or "SPY").upper().strip()
    if not clean_ticker:
        return _error("Ticker fehlt.", clean_ticker, clean_benchmark)
    try:
        buy_ts = pd.Timestamp(buy_date).tz_localize(None).normalize()
    except Exception:
        return _error("Ungültiges Einstiegsdatum.", clean_ticker, clean_benchmark)
    entry_price = _safe_float(buy_price)
    share_count = _safe_float(shares, 0.0)
    if entry_price is None or entry_price <= 0:
        return _error("Ungültiger Einstiegskurs.", clean_ticker, clean_benchmark)
    if share_count is None or share_count < 0:
        return _error("Ungültige Stückzahl.", clean_ticker, clean_benchmark)

    df = _clean_ohlc_frame(price_frame)
    bench = _clean_ohlc_frame(benchmark_frame)
    if df.empty:
        return _error(f"Keine Kursdaten für {clean_ticker} verfügbar.", clean_ticker, clean_benchmark)
    if bench.empty:
        return _error(f"Keine Benchmark-Daten für {clean_benchmark} verfügbar.", clean_ticker, clean_benchmark)
    missing_cols = REQUIRED_OHLC_COLUMNS - set(df.columns)
    if missing_cols:
        return _error(f"Kursdaten für {clean_ticker} ohne benötigte Spalten: {', '.join(sorted(missing_cols))}.", clean_ticker, clean_benchmark)
    if "Close" not in bench.columns:
        return _error(f"Benchmark-Daten für {clean_benchmark} enthalten keinen Schlusskurs.", clean_ticker, clean_benchmark)

    close = pd.to_numeric(df["Close"], errors="coerce")
    high = pd.to_numeric(df["High"], errors="coerce")
    low = pd.to_numeric(df["Low"], errors="coerce")
    volume = pd.to_numeric(df["Volume"], errors="coerce")
    current_price = _last_float(close)
    if current_price is None:
        return _error(f"Keine verwertbaren Schlusskurse für {clean_ticker} verfügbar.", clean_ticker, clean_benchmark)

    sma10 = _sma(close, 10)
    sma21 = _sma(close, 21)
    sma50 = _sma(close, 50)
    sma200 = _sma(close, 200)
    atr14 = _atr(df, 14)
    vol_sma50 = _sma(volume, 50)

    weekly = df.resample("W-FRI").agg({"Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum"}).dropna(subset=["Close"])
    weekly_close = pd.to_numeric(weekly["Close"], errors="coerce") if not weekly.empty else pd.Series(dtype=float)
    weekly_sma10 = _sma(weekly_close, 10)
    weekly_sma21 = _sma(weekly_close, 21)
    weekly_loss = weekly_close < weekly_close.shift(1)
    weekly_volume = pd.to_numeric(weekly.get("Volume", pd.Series(dtype=float)), errors="coerce") if not weekly.empty else pd.Series(dtype=float)
    weekly_rising_volume = weekly_volume > weekly_volume.shift(1)
    consecutive_loss_weeks_rising_volume = _trailing_true_count(weekly_loss & weekly_rising_volume)

    vol_ratio = None
    latest_vol_avg = _last_float(vol_sma50)
    latest_volume = _last_float(volume)
    if latest_vol_avg and latest_vol_avg > 0 and latest_volume is not None:
        vol_ratio = latest_volume / latest_vol_avg

    pct_change = close.pct_change()
    distribution_mask = (close < close.shift(1)) & (volume >= vol_sma50 * 1.2)
    distribution_days_25 = int(distribution_mask.tail(25).sum()) if len(distribution_mask) else 0

    last_50 = df.tail(50).copy()
    up_volume = float(last_50.loc[pd.to_numeric(last_50["Close"], errors="coerce").pct_change() > 0, "Volume"].sum(skipna=True)) if not last_50.empty else 0.0
    down_volume = float(last_50.loc[pd.to_numeric(last_50["Close"], errors="coerce").pct_change() < 0, "Volume"].sum(skipna=True)) if not last_50.empty else 0.0
    up_down_volume_ratio_50 = (up_volume / down_volume) if down_volume > 0 else None

    bench_close = pd.to_numeric(bench["Close"], errors="coerce").dropna()
    joined = pd.concat([close.rename("asset"), bench_close.rename("benchmark")], axis=1, join="inner").dropna()
    if joined.empty:
        return _error(f"Keine überlappenden Kursdaten für {clean_ticker} und {clean_benchmark}.", clean_ticker, clean_benchmark)
    rs_line = joined["asset"] / joined["benchmark"]
    rs_ma21 = _sma(rs_line, 21)
    rs_ma50 = _sma(rs_line, 50)
    weekly_rs = rs_line.resample("W-FRI").last().dropna()
    weekly_rs_ma10 = _sma(weekly_rs, 10)
    weekly_rs_ma25 = _sma(weekly_rs, 25)
    days_under_rs_ma21 = _trailing_true_count(rs_line < rs_ma21)
    days_under_rs_ma50 = _trailing_true_count(rs_line < rs_ma50)

    after_buy = df[df.index >= buy_ts]
    high_since_buy = _safe_float(pd.to_numeric(after_buy["High"], errors="coerce").max()) if not after_buy.empty else None
    drawdown_from_high_since_buy_pct = ((current_price / high_since_buy) - 1) * 100 if high_since_buy and high_since_buy > 0 else None
    pnl_pct = ((current_price / entry_price) - 1) * 100
    pnl_abs = (current_price - entry_price) * float(share_count or 0.0)

    under_sma21_days = _trailing_true_count(close < sma21)
    under_sma50_days = _trailing_true_count(close < sma50)
    weekly_under_10w_count = _trailing_true_count(weekly_close < weekly_sma10)

    since_entry = df[df.index >= buy_ts].copy()
    worst_day_loss_pct = None
    worst_day_date = ""
    worst_day_high_volume = None
    if not since_entry.empty:
        since_entry_pct = pd.to_numeric(since_entry["Close"], errors="coerce").pct_change() * 100
        # Include the first held day relative to prior close if available in full history.
        full_pct = pct_change.reindex(since_entry.index)
        if full_pct.dropna().size:
            worst_idx = full_pct.idxmin()
            worst_day_loss_pct = _safe_float(full_pct.loc[worst_idx] * 100)
            worst_day_date = pd.Timestamp(worst_idx).strftime("%Y-%m-%d")
            avg_vol_at_worst = _safe_float(vol_sma50.reindex(since_entry.index).loc[worst_idx])
            vol_at_worst = _safe_float(volume.reindex(since_entry.index).loc[worst_idx])
            worst_day_high_volume = bool(avg_vol_at_worst and vol_at_worst is not None and vol_at_worst >= avg_vol_at_worst * 1.2)

    pre_buy = df[df.index < buy_ts].tail(30)
    pivot_default = _safe_float(pd.to_numeric(pre_buy["High"], errors="coerce").max()) if not pre_buy.empty else None
    low_day_1_default = _series_float_at_or_after(low, buy_ts)
    low_day_0_default = _previous_series_float(low, buy_ts)

    metrics = {
        "current_price": current_price,
        "pnl_pct": pnl_pct,
        "pnl_abs": pnl_abs,
        "pnl_abs_currency": str(currency or "USD").upper(),
        "pnl_abs_eur": _safe_float(pnl_abs_eur),
        "fx_rate_to_eur": _safe_float(fx_rate_to_eur),
        "sma10": _last_float(sma10),
        "sma21": _last_float(sma21),
        "sma50": _last_float(sma50),
        "sma200": _last_float(sma200),
        "price_vs_sma50_pct": ((current_price / _last_float(sma50) - 1) * 100) if _last_float(sma50) else None,
        "price_vs_sma200_pct": ((current_price / _last_float(sma200) - 1) * 100) if _last_float(sma200) else None,
        "weekly_sma10": _last_float(weekly_sma10),
        "weekly_sma21": _last_float(weekly_sma21),
        "atr14": _last_float(atr14),
        "volume_ratio_50": vol_ratio,
        "distribution_days_25": distribution_days_25,
        "up_down_volume_ratio_50": up_down_volume_ratio_50,
        "rs_line": _last_float(rs_line),
        "rs_ma21": _last_float(rs_ma21),
        "rs_ma50": _last_float(rs_ma50),
        "weekly_rs_ma10": _last_float(weekly_rs_ma10),
        "weekly_rs_ma25": _last_float(weekly_rs_ma25),
        "days_under_rs_ma21": days_under_rs_ma21,
        "days_under_rs_ma50": days_under_rs_ma50,
        "consecutive_loss_weeks_rising_volume": consecutive_loss_weeks_rising_volume,
        "three_loss_weeks_rising_volume": consecutive_loss_weeks_rising_volume >= 3,
        "high_since_buy": high_since_buy,
        "drawdown_from_high_since_buy_pct": drawdown_from_high_since_buy_pct,
        "days_under_sma21": under_sma21_days,
        "days_under_sma50": under_sma50_days,
        "weekly_closes_under_10w": weekly_under_10w_count,
        "worst_day_loss_pct_since_buy": worst_day_loss_pct,
        "worst_day_loss_date": worst_day_date,
        "worst_day_loss_high_volume": worst_day_high_volume,
    }
    manual_defaults = {
        "pivot": pivot_default,
        "low_day_1": low_day_1_default,
        "low_day_0": low_day_0_default,
    }
    return {
        "ok": True,
        "error": "",
        "ticker": clean_ticker,
        "benchmark_ticker": clean_benchmark,
        "buy_date": _iso_date(buy_ts),
        "buy_price": entry_price,
        "shares": float(share_count or 0.0),
        "currency": str(currency or "USD").upper(),
        "as_of": _last_index_date(df),
        "benchmark_as_of": _last_index_date(bench),
        "metrics": metrics,
        "manual_defaults": manual_defaults,
    }


def build_sell_decision_metrics_smoke_inputs() -> list[dict[str, Any]]:
    """Three small example configurations for app-level smoke checks."""
    today = pd.Timestamp(datetime.now(timezone.utc).date())
    buy_date = (today - pd.DateOffset(years=1)).date()
    return [
        {"ticker": "AAPL", "buy_date": buy_date, "buy_price": 150.0, "shares": 1.0},
        {"ticker": "MSFT", "buy_date": buy_date, "buy_price": 300.0, "shares": 1.0},
        {"ticker": "SPY", "buy_date": buy_date, "buy_price": 400.0, "shares": 1.0},
    ]
