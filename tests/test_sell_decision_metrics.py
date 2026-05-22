import unittest

import pandas as pd

from sell_decision_metrics import build_sell_decision_metrics_payload


def _base_frames(periods=80):
    dates = pd.date_range("2026-01-01", periods=periods, freq="B")
    close = pd.Series(range(periods), index=dates, dtype=float) + 100
    high = close + 2
    low = close - 2
    volume = pd.Series(1_000_000, index=dates, dtype=float)
    price_frame = pd.DataFrame({
        "Open": close - 0.5,
        "High": high,
        "Low": low,
        "Close": close,
        "Volume": volume,
    })
    benchmark_frame = price_frame.copy()
    benchmark_frame["Close"] = pd.Series(range(periods), index=dates, dtype=float) + 200
    return dates, price_frame, benchmark_frame


class SellDecisionMetricsTest(unittest.TestCase):
    def test_low_closes_auto_warning_is_detected(self):
        dates, price_frame, benchmark_frame = _base_frames()
        for idx in range(-5, 0):
            close = price_frame["Close"].iloc[idx]
            price_frame.iloc[idx, price_frame.columns.get_loc("Low")] = close - 0.2
            price_frame.iloc[idx, price_frame.columns.get_loc("High")] = close + 3.8

        result = build_sell_decision_metrics_payload(
            ticker="TST",
            buy_date=dates[10],
            buy_price=float(price_frame["Close"].iloc[10]),
            shares=1,
            price_frame=price_frame,
            benchmark_frame=benchmark_frame,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["metrics"]["low_close_count_5"], 5)
        self.assertTrue(result["auto_checkboxes"]["warning_checkboxes"]["low_closes"])
        self.assertIn("unteren Kerzenviertel", result["auto_checkboxes"]["reasons"]["low_closes"])

    def test_lower_lows_without_rebound_auto_warning_is_detected(self):
        dates, price_frame, benchmark_frame = _base_frames()
        price_frame.loc[dates[-3:], "Low"] = [180.0, 178.0, 176.0]
        price_frame.loc[dates[-3:], "Close"] = [181.0, 179.5, 178.8]
        price_frame.loc[dates[-3:], "High"] = [183.0, 181.0, 180.0]

        result = build_sell_decision_metrics_payload(
            ticker="TST",
            buy_date=dates[10],
            buy_price=float(price_frame["Close"].iloc[10]),
            shares=1,
            price_frame=price_frame,
            benchmark_frame=benchmark_frame,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["metrics"]["lower_low_days"], 3)
        self.assertTrue(result["auto_checkboxes"]["warning_checkboxes"]["lower_lows_no_rebound"])
        self.assertIn("tiefere Tagestiefs", result["auto_checkboxes"]["reasons"]["lower_lows_no_rebound"])

    def test_pivot_date_overrides_buy_date_for_pivot_defaults(self):
        dates, price_frame, benchmark_frame = _base_frames()
        # buy_date later than pivot_date; ensure low_day_1 follows pivot_date not buy_date.
        buy_idx = 30
        pivot_idx = 20
        # Mark the day at pivot_idx and pivot_idx-1 with distinctive Lows to verify selection.
        price_frame.iloc[pivot_idx, price_frame.columns.get_loc("Low")] = 42.0
        price_frame.iloc[pivot_idx - 1, price_frame.columns.get_loc("Low")] = 41.0

        result = build_sell_decision_metrics_payload(
            ticker="TST",
            buy_date=dates[buy_idx],
            buy_price=float(price_frame["Close"].iloc[buy_idx]),
            shares=1,
            price_frame=price_frame,
            benchmark_frame=benchmark_frame,
            pivot_date=dates[pivot_idx],
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["manual_defaults"]["low_day_1"], 42.0)
        self.assertEqual(result["manual_defaults"]["low_day_0"], 41.0)
        # pivot_default = highest High in the 30 sessions BEFORE pivot_date.
        pre_pivot_high_max = float(price_frame["High"].iloc[:pivot_idx].tail(30).max())
        self.assertEqual(result["manual_defaults"]["pivot"], pre_pivot_high_max)
        self.assertEqual(result["pivot_date"], dates[pivot_idx].strftime("%Y-%m-%d"))

    def test_pivot_date_omitted_falls_back_to_buy_date(self):
        dates, price_frame, benchmark_frame = _base_frames()
        buy_idx = 30
        price_frame.iloc[buy_idx, price_frame.columns.get_loc("Low")] = 77.0
        price_frame.iloc[buy_idx - 1, price_frame.columns.get_loc("Low")] = 76.0

        result = build_sell_decision_metrics_payload(
            ticker="TST",
            buy_date=dates[buy_idx],
            buy_price=float(price_frame["Close"].iloc[buy_idx]),
            shares=1,
            price_frame=price_frame,
            benchmark_frame=benchmark_frame,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["manual_defaults"]["low_day_1"], 77.0)
        self.assertEqual(result["manual_defaults"]["low_day_0"], 76.0)
        self.assertEqual(result["pivot_date"], "")


if __name__ == "__main__":
    unittest.main()
