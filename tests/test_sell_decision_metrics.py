import unittest

import pandas as pd

from sell_decision_metrics import build_sell_decision_metrics_payload


class SellDecisionMetricsTest(unittest.TestCase):
    def test_low_closes_auto_warning_is_detected(self):
        dates = pd.date_range("2026-01-01", periods=80, freq="B")
        close = pd.Series(range(80), index=dates, dtype=float) + 100
        high = close + 2
        low = close - 2
        for idx in range(-5, 0):
            low.iloc[idx] = close.iloc[idx] - 0.2
            high.iloc[idx] = close.iloc[idx] + 3.8
        volume = pd.Series(1_000_000, index=dates, dtype=float)
        price_frame = pd.DataFrame({
            "Open": close - 0.5,
            "High": high,
            "Low": low,
            "Close": close,
            "Volume": volume,
        })
        benchmark_frame = price_frame.copy()
        benchmark_frame["Close"] = pd.Series(range(80), index=dates, dtype=float) + 200

        result = build_sell_decision_metrics_payload(
            ticker="TST",
            buy_date=dates[10],
            buy_price=float(close.iloc[10]),
            shares=1,
            price_frame=price_frame,
            benchmark_frame=benchmark_frame,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["metrics"]["low_close_count_5"], 5)
        self.assertTrue(result["auto_checkboxes"]["warning_checkboxes"]["low_closes"])
        self.assertIn("unteren Kerzenviertel", result["auto_checkboxes"]["reasons"]["low_closes"])


if __name__ == "__main__":
    unittest.main()
