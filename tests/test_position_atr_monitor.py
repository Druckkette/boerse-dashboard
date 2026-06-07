import unittest
from datetime import datetime, timedelta, timezone

import pandas as pd

from scripts.position_atr_monitor import (
    REFERENCE_BOTH,
    REFERENCE_ENTRY,
    REFERENCE_HIGH_SINCE_BUY,
    MonitorConfig,
    PositionCandidate,
    _split_tokens,
    evaluate_position,
    should_alert,
    should_run_for_interval,
)


def _sample_frame() -> pd.DataFrame:
    dates = pd.bdate_range("2026-01-01", periods=26)
    closes = [100.0] * 18 + [106.0, 110.0, 112.0, 111.0, 108.0, 105.0, 102.0, 100.0]
    return pd.DataFrame(
        {
            "High": [close + 1.0 for close in closes],
            "Low": [close - 1.0 for close in closes],
            "Close": closes,
        },
        index=dates,
    )


class PositionATRMonitorTest(unittest.TestCase):
    def test_high_since_buy_reference_triggers_alert(self):
        config = MonitorConfig(
            enabled=True,
            threshold_atr=2.0,
            reference=REFERENCE_HIGH_SINCE_BUY,
            atr_period=14,
            lookback_days=420,
            interval_minutes=5,
            cooldown_hours=18,
            pushover_user_keys=["abc"],
            pushover_app_token="app-token",
        )
        position = PositionCandidate(ticker="TEST", name="Test", shares=10, buy_date="2026-01-01")

        alert = evaluate_position(position, _sample_frame(), config)

        self.assertIsNotNone(alert)
        self.assertEqual(alert.ticker, "TEST")
        self.assertGreaterEqual(alert.drop_atr, 2.0)
        self.assertIn("Hoch", alert.reference_label)

    def test_entry_reference_uses_entry_price(self):
        config = MonitorConfig(
            enabled=True,
            threshold_atr=1.0,
            reference=REFERENCE_ENTRY,
            atr_period=14,
            lookback_days=420,
            interval_minutes=5,
            cooldown_hours=18,
            pushover_user_keys=["abc"],
            pushover_app_token="app-token",
        )
        position = PositionCandidate(ticker="TEST", name="Test", shares=10, entry_price=110.0)

        alert = evaluate_position(position, _sample_frame(), config)

        self.assertIsNotNone(alert)
        self.assertIn("Einstand", alert.reference_label)
        self.assertAlmostEqual(alert.reference_price, 110.0)

    def test_both_reference_picks_stronger_drop(self):
        config = MonitorConfig(
            enabled=True,
            threshold_atr=1.0,
            reference=REFERENCE_BOTH,
            atr_period=14,
            lookback_days=420,
            interval_minutes=5,
            cooldown_hours=18,
            pushover_user_keys=["abc"],
            pushover_app_token="app-token",
        )
        position = PositionCandidate(ticker="TEST", name="Test", shares=10, entry_price=101.0, buy_date="2026-01-01")

        alert = evaluate_position(position, _sample_frame(), config)

        self.assertIsNotNone(alert)
        self.assertIn("Hoch", alert.reference_label)

    def test_cooldown_blocks_recent_duplicate_alert(self):
        config = MonitorConfig(
            enabled=True,
            threshold_atr=1.0,
            reference=REFERENCE_HIGH_SINCE_BUY,
            atr_period=14,
            lookback_days=420,
            interval_minutes=5,
            cooldown_hours=18,
            pushover_user_keys=["abc"],
            pushover_app_token="app-token",
        )
        position = PositionCandidate(ticker="TEST", name="Test", shares=10, buy_date="2026-01-01")
        alert = evaluate_position(position, _sample_frame(), config)
        now = datetime(2026, 2, 15, tzinfo=timezone.utc)
        state = {"alerts": {"TEST": {"last_alerted_at": (now - timedelta(hours=2)).isoformat()}}}

        self.assertFalse(should_alert(alert, state, config, now))

        state["alerts"]["TEST"]["last_alerted_at"] = (now - timedelta(hours=24)).isoformat()
        self.assertTrue(should_alert(alert, state, config, now))

    def test_split_tokens_accepts_common_separators(self):
        self.assertEqual(_split_tokens(" aaa,bbb\nccc ; aaa "), ["aaa", "bbb", "ccc"])

    def test_configured_interval_blocks_too_frequent_scheduled_checks(self):
        config = MonitorConfig(
            enabled=True,
            threshold_atr=1.0,
            reference=REFERENCE_HIGH_SINCE_BUY,
            atr_period=14,
            lookback_days=420,
            interval_minutes=15,
            cooldown_hours=18,
            pushover_user_keys=["abc"],
            pushover_app_token="app-token",
        )
        now = datetime(2026, 2, 15, tzinfo=timezone.utc)
        state = {"last_evaluated_at": (now - timedelta(minutes=10)).isoformat()}

        self.assertFalse(should_run_for_interval(state, config, now))

        state["last_evaluated_at"] = (now - timedelta(minutes=16)).isoformat()
        self.assertTrue(should_run_for_interval(state, config, now))


if __name__ == "__main__":
    unittest.main()
