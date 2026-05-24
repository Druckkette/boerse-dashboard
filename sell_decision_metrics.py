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


def _ema(series: pd.Series, window: int) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").ewm(span=window, adjust=False, min_periods=window).mean()


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


def _trailing_true_start_date(mask: pd.Series) -> str:
    count = _trailing_true_count(mask)
    if count <= 0 or mask is None or mask.empty:
        return ""
    try:
        return pd.Timestamp(mask.index[-count]).strftime("%Y-%m-%d")
    except Exception:
        return ""


def _first_true_date(mask: pd.Series) -> str:
    if mask is None or mask.empty:
        return ""
    valid = mask.fillna(False)
    if not valid.any():
        return ""
    try:
        return pd.Timestamp(valid[valid].index[0]).strftime("%Y-%m-%d")
    except Exception:
        return ""


def _index_date_of_max(series: pd.Series | None) -> str:
    if series is None or series.empty:
        return ""
    valid = pd.to_numeric(series, errors="coerce").dropna()
    if valid.empty:
        return ""
    try:
        return pd.Timestamp(valid.idxmax()).strftime("%Y-%m-%d")
    except Exception:
        return ""

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


def _avg_volume_on_mask(volume: pd.Series, mask: pd.Series) -> float | None:
    if volume is None or volume.empty or mask is None or mask.empty:
        return None
    aligned_volume = pd.to_numeric(volume.reindex(mask.index), errors="coerce")
    selected = aligned_volume[mask.fillna(False)].dropna()
    if selected.empty:
        return None
    return float(selected.mean())


def _recent_true_count(mask: pd.Series | None, window: int) -> int:
    if mask is None or mask.empty:
        return 0
    return int(mask.fillna(False).tail(window).sum())


def _trailing_lower_low_days(low: pd.Series) -> int:
    transitions = _trailing_true_count(pd.to_numeric(low, errors="coerce") < pd.to_numeric(low, errors="coerce").shift(1))
    return transitions + 1 if transitions > 0 else 0


def _max_pct_gain(series: pd.Series | None) -> float | None:
    if series is None or series.empty:
        return None
    pct = pd.to_numeric(series, errors="coerce").pct_change().dropna() * 100
    if pct.empty:
        return None
    return float(pct.max())

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
        "auto_checkboxes": {"strength_checkboxes": {}, "warning_checkboxes": {}, "reasons": {}},
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
    pivot_date=None,
) -> dict[str, Any]:
    """Build all reusable sell-decision metrics from OHLC inputs.

    pivot_date (optional): Reference day for pivot/Tag-1/Tag-0. Pivot-Tag IS Tag 1,
    Tag 0 is the previous trading day. Falls back to buy_date if not provided.
    """
    clean_ticker = str(ticker or "").upper().strip()
    clean_benchmark = str(benchmark_ticker or "SPY").upper().strip()
    if not clean_ticker:
        return _error("Ticker fehlt.", clean_ticker, clean_benchmark)
    try:
        buy_ts = pd.Timestamp(buy_date).tz_localize(None).normalize()
    except Exception:
        return _error("Ungültiges Einstiegsdatum.", clean_ticker, clean_benchmark)
    pivot_ts = None
    if pivot_date not in (None, ""):
        try:
            pivot_ts = pd.Timestamp(pivot_date).tz_localize(None).normalize()
        except Exception:
            pivot_ts = None
    reference_ts = pivot_ts if pivot_ts is not None else buy_ts
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
    ema21 = _ema(close, 21)
    sma50 = _sma(close, 50)
    sma200 = _sma(close, 200)
    atr14 = _atr(df, 14)
    vol_sma50 = _sma(volume, 50)

    weekly = df.resample("W-FRI").agg({"Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum"}).dropna(subset=["Close"])
    weekly_close = pd.to_numeric(weekly["Close"], errors="coerce") if not weekly.empty else pd.Series(dtype=float)
    weekly_sma10 = _sma(weekly_close, 10)
    weekly_ema21 = _ema(weekly_close, 21)
    weekly_loss = weekly_close < weekly_close.shift(1)
    weekly_open = pd.to_numeric(weekly.get("Open", pd.Series(dtype=float)), errors="coerce") if not weekly.empty else pd.Series(dtype=float)
    weekly_red = weekly_close < weekly_open
    weekly_volume = pd.to_numeric(weekly.get("Volume", pd.Series(dtype=float)), errors="coerce") if not weekly.empty else pd.Series(dtype=float)
    weekly_rising_volume = weekly_volume > weekly_volume.shift(1)
    three_loss_weeks_mask = weekly_loss & weekly_rising_volume & weekly_red
    consecutive_loss_weeks_rising_volume = _trailing_true_count(three_loss_weeks_mask)

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
    rs_ema21 = _ema(rs_line, 21)
    rs_ma50 = _sma(rs_line, 50)
    weekly_rs = rs_line.resample("W-FRI").last().dropna()
    weekly_rs_ma10 = _sma(weekly_rs, 10)
    weekly_rs_ma25 = _sma(weekly_rs, 25)
    days_under_rs_ema21 = _trailing_true_count(rs_line < rs_ema21)
    days_under_rs_ma50 = _trailing_true_count(rs_line < rs_ma50)

    after_buy = df[df.index >= buy_ts]
    high_since_buy = _safe_float(pd.to_numeric(after_buy["High"], errors="coerce").max()) if not after_buy.empty else None
    drawdown_from_high_since_buy_pct = ((current_price / high_since_buy) - 1) * 100 if high_since_buy and high_since_buy > 0 else None
    pnl_pct = ((current_price / entry_price) - 1) * 100
    pnl_abs = (current_price - entry_price) * float(share_count or 0.0)

    under_ema21_days = _trailing_true_count(close < ema21)
    under_sma50_days = _trailing_true_count(close < sma50)
    weekly_under_10w_mask = weekly_close < weekly_sma10
    weekly_under_10w_count = _trailing_true_count(weekly_under_10w_mask)
    under_weekly_sma10_start_date = _trailing_true_start_date(weekly_under_10w_mask)
    three_loss_weeks_rising_volume_start_date = _trailing_true_start_date(three_loss_weeks_mask)

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

    pre_buy = df[df.index < reference_ts].tail(30)
    pivot_default = _safe_float(pd.to_numeric(pre_buy["High"], errors="coerce").max()) if not pre_buy.empty else None
    low_day_1_default = _series_float_at_or_after(low, reference_ts)
    low_day_0_default = _previous_series_float(low, reference_ts)
    low_day_1_date = _first_true_date(low.index.to_series().ge(reference_ts)) if low_day_1_default is not None else ""
    low_day_0_date = _last_index_date(low[low.index < reference_ts]) if low_day_0_default is not None else ""
    as_of_date = _last_index_date(df)
    high_since_buy_date = _index_date_of_max(pd.to_numeric(after_buy.get("High", pd.Series(dtype=float)), errors="coerce")) if not after_buy.empty else ""
    under_ema21_start_date = _trailing_true_start_date(close < ema21)
    under_sma50_start_date = _trailing_true_start_date(close < sma50)
    under_rs_ema21_start_date = _trailing_true_start_date(rs_line < rs_ema21)
    under_rs_ma50_start_date = _trailing_true_start_date(rs_line < rs_ma50)
    close_since_buy = close[close.index >= buy_ts]
    high_since_buy_series = high[high.index >= buy_ts]
    running_high_since_buy = high_since_buy_series.cummax() if not high_since_buy_series.empty else pd.Series(dtype=float)
    drawdown_since_buy = (close_since_buy / running_high_since_buy.reindex(close_since_buy.index) - 1) * 100 if not close_since_buy.empty else pd.Series(dtype=float)
    first_drawdown_8_date = _first_true_date(drawdown_since_buy <= -8)
    first_drawdown_12_date = _first_true_date(drawdown_since_buy <= -12)
    first_drawdown_15_date = _first_true_date(drawdown_since_buy <= -15)
    pnl_since_buy = ((close_since_buy / entry_price) - 1) * 100 if entry_price else pd.Series(dtype=float)
    first_loss_5_date = _first_true_date(pnl_since_buy <= -5)
    first_loss_7_date = _first_true_date(pnl_since_buy <= -7)
    first_low_day_1_loss_date = _first_true_date((close_since_buy < low_day_1_default) & (pnl_since_buy < 0)) if low_day_1_default else ""
    first_low_day_0_loss_date = _first_true_date((close_since_buy < low_day_0_default) & (pnl_since_buy < 0)) if low_day_0_default else ""
    sma50_since_buy = sma50.reindex(close_since_buy.index)
    sma200_since_buy = sma200.reindex(close_since_buy.index)
    sma50_extension_mask = (close_since_buy / sma50_since_buy - 1) * 100 > 25
    sma200_extension_mask = (close_since_buy / sma200_since_buy - 1) * 100 > 70
    volume_since_buy = volume.reindex(close_since_buy.index)
    vol_sma50_since_buy = vol_sma50.reindex(close_since_buy.index)
    sma50_volume_break_mask = (close_since_buy < sma50_since_buy * 0.98) & (volume_since_buy > vol_sma50_since_buy * 1.2)
    sma200_since_buy = sma200.reindex(close_since_buy.index)
    sma200_volume_break_mask = (close_since_buy < sma200_since_buy) & (volume_since_buy > vol_sma50_since_buy * 1.5)
    first_sma50_volume_break_date = _first_true_date(sma50_volume_break_mask)
    first_sma200_volume_break_date = _first_true_date(sma200_volume_break_mask)
    first_10_pct_gain_date = _first_true_date(pnl_since_buy >= 10)
    first_20_pct_gain_date = _first_true_date(pnl_since_buy >= 20)
    first_sma50_extension_date = _first_true_date(sma50_extension_mask)
    first_sma200_extension_date = _first_true_date(sma200_extension_mask)
    current_sma50_extension_start_date = _trailing_true_start_date(sma50_extension_mask)
    current_sma200_extension_start_date = _trailing_true_start_date(sma200_extension_mask)

    day_range = (high - low).replace(0, np.nan)
    closing_range = ((close - low) / day_range).clip(lower=0, upper=1)
    first_sessions = df[df.index >= buy_ts].head(10)
    first_green_ratio = None
    if len(first_sessions) >= 5:
        first_close = pd.to_numeric(first_sessions["Close"], errors="coerce")
        first_green_ratio = float((first_close > first_close.shift(1)).sum() / max(1, len(first_close) - 1))

    recent_20 = df.tail(20)
    recent_pct_20 = pd.to_numeric(recent_20.get("Close", pd.Series(dtype=float)), errors="coerce").pct_change() if not recent_20.empty else pd.Series(dtype=float)
    recent_volume_20 = pd.to_numeric(recent_20.get("Volume", pd.Series(dtype=float)), errors="coerce") if not recent_20.empty else pd.Series(dtype=float)
    recent_up_avg_volume = _avg_volume_on_mask(recent_volume_20, recent_pct_20 > 0)
    recent_down_avg_volume = _avg_volume_on_mask(recent_volume_20, recent_pct_20 < 0)
    positive_volume_ratio_20 = (recent_up_avg_volume / recent_down_avg_volume) if recent_down_avg_volume and recent_down_avg_volume > 0 and recent_up_avg_volume is not None else None

    recent_close_10 = close.tail(10)
    recent_volume_10 = volume.tail(10)
    recent_cr_10 = closing_range.tail(10)
    pivot_near_mask = pd.Series(False, index=recent_close_10.index)
    if pivot_default and pivot_default > 0 and not recent_close_10.empty:
        pivot_distance = (recent_close_10 / pivot_default - 1) * 100
        pivot_near_mask = pivot_distance.between(-3, 5, inclusive="both")
    tight_close_mask = recent_close_10.pct_change().fillna(0).abs() <= 0.018
    stall_mask = pivot_near_mask & tight_close_mask & (recent_volume_10 >= recent_volume_10.shift(1) * 0.95) & (recent_cr_10 < 0.55)

    low_close_count_5 = _recent_true_count(closing_range <= 0.25, 5)
    low_close_count_10 = _recent_true_count(closing_range <= 0.25, 10)
    upper_third_close_count_5 = _recent_true_count(closing_range >= (2 / 3), 5)
    upper_third_close_count_10 = _recent_true_count(closing_range >= (2 / 3), 10)
    lower_lows_count_4 = _trailing_true_count(low < low.shift(1))
    lower_low_days = _trailing_lower_low_days(low)
    lower_low_window = close.tail(max(lower_low_days, 1))
    lower_low_max_rebound_pct = _max_pct_gain(lower_low_window)
    lower_lows_no_rebound = bool(lower_low_days >= 3 and (lower_low_max_rebound_pct is None or lower_low_max_rebound_pct <= 2.5))

    recent_ema21 = ema21.reindex(close.index)
    recent_sma50 = sma50.reindex(close.index)
    pullback_rebound_21 = (low <= recent_ema21 * 1.015) & (close > recent_ema21) & (close > close.shift(1))
    pullback_rebound_50 = (low <= recent_sma50 * 1.015) & (close > recent_sma50) & (close > close.shift(1))
    pullback_rebound_recent = bool(((pullback_rebound_21 | pullback_rebound_50).tail(8)).fillna(False).any())

    asset_return_10 = (close.iloc[-1] / close.iloc[-11] - 1) if len(close.dropna()) >= 11 else None
    benchmark_return_10 = (bench_close.iloc[-1] / bench_close.iloc[-11] - 1) if len(bench_close.dropna()) >= 11 else None
    asset_return_20 = (close.iloc[-1] / close.iloc[-21] - 1) if len(close.dropna()) >= 21 else None
    benchmark_return_20 = (bench_close.iloc[-1] / bench_close.iloc[-21] - 1) if len(bench_close.dropna()) >= 21 else None
    rs_latest = _last_float(rs_line)
    rs21_latest = _last_float(rs_ema21)
    rs50_latest = _last_float(rs_ma50)
    rs_line_rising_5 = bool(len(rs_line.dropna()) >= 6 and rs_line.dropna().iloc[-1] > rs_line.dropna().iloc[-6])
    negative_market_divergence = bool(
        rs_latest is not None
        and rs21_latest is not None
        and rs_latest < rs21_latest
        and (
            (asset_return_10 is not None and benchmark_return_10 is not None and asset_return_10 < benchmark_return_10 - 0.03)
            or (asset_return_20 is not None and benchmark_return_20 is not None and asset_return_20 < benchmark_return_20 - 0.05)
        )
    )

    recent_high_10 = high.tail(10)
    recent_drawdown_10 = ((recent_close_10 / recent_high_10.cummax()) - 1) * 100 if not recent_close_10.empty else pd.Series(dtype=float)
    max_rebound_10 = _max_pct_gain(recent_close_10)
    weak_recent_rebound = bool(
        len(recent_close_10.dropna()) >= 5
        and (max_rebound_10 is None or max_rebound_10 < 3.0)
        and (
            (not recent_drawdown_10.dropna().empty and recent_drawdown_10.min() <= -5)
            or lower_lows_no_rebound
            or (_last_float(ema21) is not None and current_price < (_last_float(ema21) or current_price))
        )
    )

    downside_reversal_mask = (
        (high >= (high_since_buy or 0) * 0.95)
        & (close < close.shift(1))
        & (high > high.shift(1))
        & (closing_range <= 0.5)
    ) if high_since_buy and high_since_buy > 0 else pd.Series(False, index=close.index)
    downside_reversal_near_high = bool(downside_reversal_mask.tail(5).fillna(False).any())
    failed_breakout_high_volume = bool(low_day_1_default and current_price < low_day_1_default and vol_ratio is not None and vol_ratio > 1.2)

    auto_strength_checkboxes = {
        "upper_third_closes": upper_third_close_count_5 >= 3 or upper_third_close_count_10 >= 6,
        "green_days_70": bool(first_green_ratio is not None and first_green_ratio >= 0.70),
        "positive_volume": bool(
            (positive_volume_ratio_20 is not None and positive_volume_ratio_20 >= 1.10)
            or (up_down_volume_ratio_50 is not None and up_down_volume_ratio_50 >= 1.10)
        ),
        "pullback_rebound": pullback_rebound_recent,
        "rs_line_strong": bool(rs_latest is not None and rs21_latest is not None and rs50_latest is not None and rs_latest > rs21_latest > rs50_latest and rs_line_rising_5),
    }
    auto_warning_checkboxes = {
        "failed_breakout_high_volume": failed_breakout_high_volume,
        "lower_lows_no_rebound": lower_lows_no_rebound,
        "stall_days_near_breakout": int(stall_mask.sum()) >= 2,
        "low_closes": low_close_count_5 >= 3 or low_close_count_10 >= 5,
        "distribution_cluster": distribution_days_25 >= 4,
        "negative_market_divergence": negative_market_divergence,
        "weak_rebounds": weak_recent_rebound,
        "worst_day_high_volume": bool(worst_day_high_volume),
        "downside_reversal_near_high": downside_reversal_near_high,
        "three_loss_weeks_rising_volume": consecutive_loss_weeks_rising_volume >= 3,
    }
    auto_checkbox_reasons = {
        "upper_third_closes": f"{upper_third_close_count_5}/5 bzw. {upper_third_close_count_10}/10 Schlusskurse im oberen Kerzendrittel",
        "green_days_70": f"{(first_green_ratio or 0) * 100:.0f}% grüne Tage in den ersten {len(first_sessions)} Sessions seit Kauf" if first_green_ratio is not None else "Zu wenige Sessions seit Kauf",
        "positive_volume": f"Up-/Down-Volumenfaktor 20T {positive_volume_ratio_20:.2f}" if positive_volume_ratio_20 is not None else f"Up-/Down-Volumenfaktor 50T {_safe_float(up_down_volume_ratio_50, 0.0):.2f}",
        "pullback_rebound": "Rebound an 21-EMA/50-SMA in den letzten 8 Sessions" if pullback_rebound_recent else "Kein frischer Rebound an 21-EMA/50-SMA",
        "rs_line_strong": "RS-Linie steigt und liegt über 21-EMA und 50-Tage-Durchschnitt" if auto_strength_checkboxes["rs_line_strong"] else "RS-Linie steigt nicht über beiden Durchschnitten",
        "failed_breakout_high_volume": f"Kurs unter Tief Tag 1 bei Volumenfaktor {vol_ratio:.2f}" if failed_breakout_high_volume and vol_ratio is not None else "Nicht unter Tief Tag 1 mit erhöhtem Volumen",
        "lower_lows_no_rebound": f"{lower_low_days} tiefere Tagestiefs in Folge; stärkster Rebound {(_safe_float(lower_low_max_rebound_pct, 0.0)):.1f}%" if lower_lows_no_rebound else f"{lower_low_days} tiefere Tagestiefs in Folge; Rebound noch nicht schwach genug",
        "stall_days_near_breakout": f"{int(stall_mask.sum())} Stau-Tage nahe Pivot in den letzten 10 Sessions",
        "low_closes": f"{low_close_count_5}/5 bzw. {low_close_count_10}/10 Schlusskurse im unteren Kerzenviertel",
        "distribution_cluster": f"{distribution_days_25} Distribution-Tage in 25 Sessions",
        "negative_market_divergence": "10/20T-Rendite klar schwächer als Benchmark und RS unter 21-EMA" if negative_market_divergence else "Keine klare negative Divergenz gegen Benchmark",
        "weak_rebounds": f"Stärkster Rebound 10T {(_safe_float(max_rebound_10, 0.0)):.1f}% bei technischer Schwäche" if weak_recent_rebound else "Rebounds aktuell nicht schwach genug",
        "worst_day_high_volume": f"Größter Tagesverlust seit Kauf am {worst_day_date} mit erhöhtem Volumen" if worst_day_high_volume else "Größter Tagesverlust nicht mit erhöhtem Volumen",
        "downside_reversal_near_high": "Downside Reversal nahe Hoch in den letzten 5 Sessions" if downside_reversal_near_high else "Kein Downside Reversal nahe Hoch",
        "three_loss_weeks_rising_volume": f"{consecutive_loss_weeks_rising_volume} klare Verlustwochen (Close < Open) mit steigendem Volumen in Folge",
    }
    auto_checkboxes = {
        "strength_checkboxes": auto_strength_checkboxes,
        "warning_checkboxes": auto_warning_checkboxes,
        "reasons": auto_checkbox_reasons,
    }

    metrics = {
        "current_price": current_price,
        "pnl_pct": pnl_pct,
        "pnl_abs": pnl_abs,
        "pnl_abs_currency": str(currency or "USD").upper(),
        "pnl_abs_eur": _safe_float(pnl_abs_eur),
        "fx_rate_to_eur": _safe_float(fx_rate_to_eur),
        "sma10": _last_float(sma10),
        "ema21": _last_float(ema21),
        "sma50": _last_float(sma50),
        "sma200": _last_float(sma200),
        "price_vs_sma50_pct": ((current_price / _last_float(sma50) - 1) * 100) if _last_float(sma50) else None,
        "price_vs_sma200_pct": ((current_price / _last_float(sma200) - 1) * 100) if _last_float(sma200) else None,
        "weekly_sma10": _last_float(weekly_sma10),
        "weekly_ema21": _last_float(weekly_ema21),
        "atr14": _last_float(atr14),
        "volume_ratio_50": vol_ratio,
        "distribution_days_25": distribution_days_25,
        "up_down_volume_ratio_50": up_down_volume_ratio_50,
        "rs_line": _last_float(rs_line),
        "rs_ema21": _last_float(rs_ema21),
        "rs_ma50": _last_float(rs_ma50),
        "weekly_rs_ma10": _last_float(weekly_rs_ma10),
        "weekly_rs_ma25": _last_float(weekly_rs_ma25),
        "days_under_rs_ema21": days_under_rs_ema21,
        "days_under_rs_ma50": days_under_rs_ma50,
        "consecutive_loss_weeks_rising_volume": consecutive_loss_weeks_rising_volume,
        "three_loss_weeks_rising_volume": consecutive_loss_weeks_rising_volume >= 3,
        "high_since_buy": high_since_buy,
        "drawdown_from_high_since_buy_pct": drawdown_from_high_since_buy_pct,
        "days_under_ema21": under_ema21_days,
        "days_under_sma50": under_sma50_days,
        "weekly_closes_under_10w": weekly_under_10w_count,
        "under_weekly_sma10_start_date": under_weekly_sma10_start_date,
        "three_loss_weeks_rising_volume_start_date": three_loss_weeks_rising_volume_start_date,
        "as_of_date": as_of_date,
        "high_since_buy_date": high_since_buy_date,
        "low_day_1_date": low_day_1_date,
        "low_day_0_date": low_day_0_date,
        "under_ema21_start_date": under_ema21_start_date,
        "under_sma50_start_date": under_sma50_start_date,
        "under_rs_ema21_start_date": under_rs_ema21_start_date,
        "under_rs_ma50_start_date": under_rs_ma50_start_date,
        "first_10_pct_gain_date": first_10_pct_gain_date,
        "first_20_pct_gain_date": first_20_pct_gain_date,
        "first_loss_5_date": first_loss_5_date,
        "first_loss_7_date": first_loss_7_date,
        "first_low_day_1_loss_date": first_low_day_1_loss_date,
        "first_low_day_0_loss_date": first_low_day_0_loss_date,
        "first_sma50_volume_break_date": first_sma50_volume_break_date,
        "first_sma200_volume_break_date": first_sma200_volume_break_date,
        "first_drawdown_8_date": first_drawdown_8_date,
        "first_drawdown_12_date": first_drawdown_12_date,
        "first_drawdown_15_date": first_drawdown_15_date,
        "first_sma50_extension_date": first_sma50_extension_date,
        "first_sma200_extension_date": first_sma200_extension_date,
        "current_sma50_extension_start_date": current_sma50_extension_start_date,
        "current_sma200_extension_start_date": current_sma200_extension_start_date,
        "worst_day_loss_pct_since_buy": worst_day_loss_pct,
        "worst_day_loss_date": worst_day_date,
        "worst_day_loss_high_volume": worst_day_high_volume,
        "low_close_count_5": low_close_count_5,
        "low_close_count_10": low_close_count_10,
        "upper_third_close_count_5": upper_third_close_count_5,
        "upper_third_close_count_10": upper_third_close_count_10,
        "lower_lows_count": lower_lows_count_4,
        "lower_low_days": lower_low_days,
        "lower_low_max_rebound_pct": lower_low_max_rebound_pct,
        "positive_volume_ratio_20": positive_volume_ratio_20,
        "first_10_sessions_green_ratio": first_green_ratio,
        "max_rebound_10_pct": max_rebound_10,
    }
    manual_defaults = {
        "pivot": pivot_default,
        "low_day_1": low_day_1_default,
        "low_day_0": low_day_0_default,
    }

    # OHLC frames in lowercase column layout for the Hub-style sell engine.
    # Daily uses since-buy slice (matches Strategien-Hub semantics); benchmark stays full
    # so weekly resampling for the RS strategy has enough history.
    def _lowercase_ohlc(frame: pd.DataFrame) -> pd.DataFrame:
        if frame is None or frame.empty:
            return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
        cols = {}
        for src, dst in (("Open", "open"), ("High", "high"), ("Low", "low"), ("Close", "close"), ("Volume", "volume")):
            cols[dst] = pd.to_numeric(frame.get(src, pd.Series(dtype=float)), errors="coerce")
        out = pd.DataFrame(cols).dropna(subset=["close"])
        out.index = pd.to_datetime(out.index).tz_localize(None) if not out.empty else out.index
        return out

    daily_full = _lowercase_ohlc(df)
    daily_since_buy = daily_full[daily_full.index >= buy_ts] if not daily_full.empty else daily_full
    weekly_since_buy = (
        daily_since_buy.resample("W-FRI").agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}).dropna()
        if not daily_since_buy.empty else pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
    )
    bench_daily = _lowercase_ohlc(bench)
    bench_weekly = (
        bench_daily.resample("W-FRI").agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}).dropna()
        if not bench_daily.empty else pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
    )

    ohlc_frames = {
        "daily_since_buy": daily_since_buy,
        "weekly_since_buy": weekly_since_buy,
        "benchmark_daily": bench_daily,
        "benchmark_weekly": bench_weekly,
        "peak_since_buy": float(high_since_buy) if high_since_buy and high_since_buy > 0 else None,
    }

    return {
        "ok": True,
        "error": "",
        "ticker": clean_ticker,
        "benchmark_ticker": clean_benchmark,
        "buy_date": _iso_date(buy_ts),
        "pivot_date": _iso_date(pivot_ts) if pivot_ts is not None else "",
        "buy_price": entry_price,
        "shares": float(share_count or 0.0),
        "currency": str(currency or "USD").upper(),
        "as_of": as_of_date,
        "benchmark_as_of": _last_index_date(bench),
        "metrics": metrics,
        "manual_defaults": manual_defaults,
        "auto_checkboxes": auto_checkboxes,
        "ohlc_frames": ohlc_frames,
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
