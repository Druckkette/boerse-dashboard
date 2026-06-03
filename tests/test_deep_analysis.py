import unittest
from datetime import date

import numpy as np
import pandas as pd

import app


class _Context:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeStreamlit:
    def __init__(self):
        self.calls = []

    def columns(self, count):
        return [_Context() for _ in range(count)]

    def expander(self, *args, **kwargs):
        return _Context()

    def markdown(self, *args, **kwargs):
        self.calls.append(("markdown", args, kwargs))

    def metric(self, *args, **kwargs):
        self.calls.append(("metric", args, kwargs))

    def plotly_chart(self, *args, **kwargs):
        self.calls.append(("plotly_chart", args, kwargs))

    def success(self, *args, **kwargs):
        self.calls.append(("success", args, kwargs))

    def warning(self, *args, **kwargs):
        self.calls.append(("warning", args, kwargs))


class DeepAnalysisTest(unittest.TestCase):
    def test_old_refresh_after_old_breadth_date_is_stale(self):
        self.assertEqual(
            app._deep_analysis_cache_state(
                breadth_last=date(2026, 5, 1),
                benchmark_last=date(2026, 6, 2),
                refresh_date=date(2026, 5, 3),
            ),
            "stale",
        )

    def test_recent_refresh_after_benchmark_means_last_available(self):
        self.assertEqual(
            app._deep_analysis_cache_state(
                breadth_last=date(2026, 5, 1),
                benchmark_last=date(2026, 6, 2),
                refresh_date=date(2026, 6, 2),
            ),
            "last_available",
        )

    def test_matching_breadth_and_benchmark_is_current(self):
        self.assertEqual(
            app._deep_analysis_cache_state(
                breadth_last=date(2026, 6, 2),
                benchmark_last=date(2026, 6, 2),
                refresh_date=date(2026, 5, 3),
            ),
            "current",
        )

    def test_missing_pct_above_50sma_renders_as_unavailable(self):
        idx = pd.date_range("2026-04-01", periods=30, freq="B")
        ad_line = pd.Series(np.arange(30), index=idx, dtype=float)
        breadth = pd.DataFrame(
            {
                "Advancers": 100,
                "Decliners": 80,
                "AD_Line": ad_line,
                "AD_Line_SMA21": ad_line.rolling(21, min_periods=1).mean(),
                "McClellan": 1.0,
                "New_Highs": 10,
                "New_Lows": 2,
                "NH_NL_Ratio": 5.0,
                "Pct_Above_50SMA": 55.0,
                "Pct_Above_200SMA": 45.0,
                "Deemer_Ratio": 1.2,
                "Breadth_Thrust": False,
            },
            index=idx,
        )
        breadth.iloc[-1, breadth.columns.get_loc("Pct_Above_50SMA")] = np.nan
        breadth.attrs["breadth_universe_loaded"] = 180

        spx = pd.DataFrame(
            {
                "Close": np.linspace(100, 130, 30),
                "High": np.linspace(101, 131, 30),
            },
            index=idx,
        )

        fake_st = _FakeStreamlit()
        original_st = app.st
        app.st = fake_st
        try:
            result = app._render_deep_analysis_content(breadth, 30, {"S&P 500": spx})
        finally:
            app.st = original_st

        self.assertIsNotNone(result)
        self.assertTrue(
            any(call[0] == "markdown" and "Nicht verf\u00fcgbar" in str(call[1]) for call in fake_st.calls)
        )


if __name__ == "__main__":
    unittest.main()
