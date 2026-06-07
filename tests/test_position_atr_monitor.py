import unittest
from datetime import datetime, timedelta, timezone

import pandas as pd

from scripts.position_atr_monitor import (
    REFERENCE_BOTH,
    REFERENCE_ENTRY,
    REFERENCE_HIGH_SINCE_BUY,
    REFERENCE_PREVIOUS_CLOSE,
    MonitorConfig,
    PositionCandidate,
    _split_tokens,
    evaluate_position,
    positions_from_payload,
    send_pushover_test,
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

    def test_previous_close_reference_triggers_only_on_loss(self):
        config = MonitorConfig(
            enabled=True,
            threshold_atr=0.5,
            reference=REFERENCE_PREVIOUS_CLOSE,
            atr_period=14,
            lookback_days=420,
            interval_minutes=5,
            cooldown_hours=18,
            pushover_user_keys=["abc"],
            pushover_app_token="app-token",
        )
        position = PositionCandidate(ticker="TEST", name="Example AG", shares=10, buy_date="2026-01-01")

        alert = evaluate_position(position, _sample_frame(), config)

        self.assertIsNotNone(alert)
        self.assertIn("Vortagesschluss", alert.reference_label)
        self.assertAlmostEqual(alert.reference_price, 102.0)
        self.assertGreaterEqual(alert.drop_atr, 0.5)
        self.assertIn("Example AG (TEST) ist gefallen", alert.body)

    def test_previous_close_reference_ignores_intraday_gain(self):
        config = MonitorConfig(
            enabled=True,
            threshold_atr=0.5,
            reference=REFERENCE_PREVIOUS_CLOSE,
            atr_period=14,
            lookback_days=420,
            interval_minutes=5,
            cooldown_hours=18,
            pushover_user_keys=["abc"],
            pushover_app_token="app-token",
        )
        frame = _sample_frame().copy()
        frame.iloc[-1, frame.columns.get_loc("High")] = 105.0
        frame.iloc[-1, frame.columns.get_loc("Low")] = 103.0
        frame.iloc[-1, frame.columns.get_loc("Close")] = 104.0
        position = PositionCandidate(ticker="TEST", name="Example AG", shares=10, buy_date="2026-01-01")

        alert = evaluate_position(position, frame, config)

        self.assertIsNone(alert)

    def test_previous_close_reference_respects_atr_threshold(self):
        config = MonitorConfig(
            enabled=True,
            threshold_atr=5.0,
            reference=REFERENCE_PREVIOUS_CLOSE,
            atr_period=14,
            lookback_days=420,
            interval_minutes=5,
            cooldown_hours=18,
            pushover_user_keys=["abc"],
            pushover_app_token="app-token",
        )
        position = PositionCandidate(ticker="TEST", name="Example AG", shares=10, buy_date="2026-01-01")

        alert = evaluate_position(position, _sample_frame(), config)

        self.assertIsNone(alert)

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

    def test_positions_from_payload_builds_candidates(self):
        positions = positions_from_payload([
            {
                "ticker": " test ",
                "name": "Example AG",
                "shares": "3.5",
                "entry_price": "42.1",
                "buy_date": "2026-01-02",
                "isin": "de000test01",
                "source": "streamlit_current_depot",
            },
            {"ticker": "", "shares": 10},
        ])

        self.assertEqual(len(positions), 1)
        self.assertEqual(positions[0].ticker, "TEST")
        self.assertEqual(positions[0].shares, 3.5)
        self.assertEqual(positions[0].isin, "DE000TEST01")

    def test_pushover_test_dry_run_is_safe(self):
        result = send_pushover_test(dry_run=True)

        self.assertTrue(result["ok"])
        self.assertTrue(result["dry_run"])

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
