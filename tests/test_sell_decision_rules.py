"""Tests for the Hub-backed sell-decision rule engine.

The historical LM-native rule blocks for patterns 1-10, 12-14 have been replaced by
calls into the Strategien-Hub engine (sell_strategies.verkaufs_empfehlung_gesamt).
These tests verify:
  * Hub signals propagate into killer/tranche/watch buckets correctly
  * Pattern #11 Distribution-Tage and pattern #15 Volumen-Faktor logic remain LM-native
  * LM-only behaviours (personality check, weak-industry penalty, watch signals,
    regime classification, stop/trigger pricing, health score) still work
  * Aggregation (Bärisch-Aufstockung, already-sold floor, allowed levels) is unchanged
"""

import unittest

import numpy as np
import pandas as pd

from sell_decision_rules import compute_sell_health_score, evaluate_sell_decision


def _build_ohlc_frames(*, periods: int = 200, current_close: float = 110.0, buy_offset_days: int = 80, declining: bool = False, vol_spike_last: float | None = None):
    """Construct synthetic OHLC frames + payload for evaluate_sell_decision."""
    dates = pd.date_range(end=pd.Timestamp("2026-05-20"), periods=periods, freq="B")
    if declining:
        closes = np.linspace(current_close + 20, current_close, periods)
    else:
        closes = np.linspace(current_close - 30, current_close, periods)
    df = pd.DataFrame(
        {
            "open": closes - 0.3,
            "high": closes + 0.6,
            "low": closes - 0.6,
            "close": closes,
            "volume": [1_000_000.0] * periods,
        },
        index=dates,
    )
    if vol_spike_last is not None:
        df.loc[df.index[-1], "volume"] = 1_000_000.0 * vol_spike_last
    weekly = df.resample("W-FRI").agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}).dropna()

    bench_closes = np.linspace(400, 420, periods)
    bench = pd.DataFrame(
        {
            "open": bench_closes,
            "high": bench_closes + 1,
            "low": bench_closes - 1,
            "close": bench_closes,
            "volume": [10_000_000.0] * periods,
        },
        index=dates,
    )
    bench_weekly = bench.resample("W-FRI").agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}).dropna()

    buy_date = dates[periods - buy_offset_days]
    buy_price = float(df.loc[buy_date, "close"])
    return df, weekly, bench, bench_weekly, buy_date, buy_price


def _payload(
    *,
    pnl_pct: float | None = None,
    current_price: float | None = None,
    buy_price: float = 100.0,
    metrics_extra: dict | None = None,
    manual_defaults: dict | None = None,
    auto_checkboxes: dict | None = None,
    include_ohlc: bool = True,
    declining: bool = False,
    periods: int = 200,
    buy_offset_days: int = 80,
    setup: dict | None = None,
):
    """Build a payload with both metrics and (optionally) OHLC frames."""
    cur = current_price if current_price is not None else (buy_price * (1 + (pnl_pct or 0.0) / 100))
    metrics = {"current_price": cur}
    if pnl_pct is not None:
        metrics["pnl_pct"] = pnl_pct
    metrics["as_of_date"] = "2026-05-20"
    if metrics_extra:
        metrics.update(metrics_extra)

    payload = {
        "ticker": "TEST",
        "buy_price": buy_price,
        "shares": 10.0,
        "buy_date": "2026-01-15",
        "as_of": "2026-05-20",
        "metrics": metrics,
        "manual_defaults": manual_defaults or {},
    }
    if auto_checkboxes is not None:
        payload["auto_checkboxes"] = auto_checkboxes
    if setup is not None:
        payload["lm_setup"] = setup

    if include_ohlc:
        df, weekly, bench, bench_weekly, _, _ = _build_ohlc_frames(
            periods=periods,
            current_close=cur,
            buy_offset_days=buy_offset_days,
            declining=declining,
        )
        payload["ohlc_frames"] = {
            "daily_since_buy": df,
            "weekly_since_buy": weekly,
            "benchmark_daily": bench,
            "benchmark_weekly": bench_weekly,
            "peak_since_buy": float(df["high"].max()),
        }
    return payload


class HubBackedKillerSignalsTest(unittest.TestCase):
    def test_seven_percent_loss_triggers_killer_full_exit(self):
        """Hub strategie_drei_stufen_nach_kauf emits 7%-Notbremse at pnl <= -7."""
        result = evaluate_sell_decision(
            _payload(pnl_pct=-8.0, buy_price=100.0, current_price=92.0, declining=True),
        )
        self.assertEqual(result["recommendation_percent"], 100)
        self.assertEqual(result["recommendation_label"], "KOMPLETTVERKAUF")
        self.assertTrue(result["killer_signals"])

    def test_bearish_environment_lowers_notbremse_threshold_to_5(self):
        """Hub strategie_notbremse_verlust uses 5% threshold for Bärisch."""
        result = evaluate_sell_decision(
            _payload(pnl_pct=-6.0, buy_price=100.0, current_price=94.0, declining=True),
            manual_data={"market_environment": "Bärisch"},
        )
        self.assertEqual(result["recommendation_percent"], 100)
        self.assertTrue(any("Notbremse" in s["label"] for s in result["killer_signals"]))


class HubBackedTrancheSignalsTest(unittest.TestCase):
    def test_profit_zone_emits_at_least_one_tranche(self):
        """Hub strategie_gewinn_in_stufen triggers Pflicht-Teilverkauf at pnl >= 20 (bull)."""
        result = evaluate_sell_decision(_payload(pnl_pct=22.0, buy_price=100.0, current_price=122.0))
        self.assertEqual(result["recommendation_label"], "TEILVERKAUF")
        self.assertGreater(result["recommendation_percent"], 0)
        self.assertTrue(any("Gewinnzone" in s["label"] or "Pflicht-Teilverkauf" in s["label"] for s in result["tranche_signals"]))

    def test_bearish_aggregation_raises_target_to_next_level(self):
        """LM aggregation: in a bearish market, target is raised to next allowed level."""
        result = evaluate_sell_decision(
            _payload(pnl_pct=12.0, buy_price=100.0, current_price=112.0),
            manual_data={"market_environment": "Bärisch"},
        )
        # Bearish forces target above the floored signal sum.
        self.assertIn(result["recommendation_percent"], {25, 33, 50, 66, 75, 100})

    def test_already_sold_floors_recommendation_to_remainder(self):
        """Already-sold tranches are subtracted from the new recommendation."""
        result = evaluate_sell_decision(
            _payload(pnl_pct=22.0, buy_price=100.0, current_price=122.0),
            tranche_log=[{"ticker": "TEST", "tranche_percent": 33}],
        )
        self.assertEqual(result["already_sold_percent"], 33)
        self.assertLessEqual(result["sell_now_percent"], result["target_total_sold_percent"])

    def test_killer_signal_only_recommends_unsold_remainder(self):
        """A killer signal yields target 100, but already-sold reduces sell_now."""
        result = evaluate_sell_decision(
            _payload(pnl_pct=-8.0, buy_price=100.0, current_price=92.0, declining=True),
            tranche_log=[{"ticker": "TEST", "tranche_percent": 75}],
        )
        self.assertEqual(result["already_sold_percent"], 75)
        self.assertEqual(result["target_total_sold_percent"], 100)
        self.assertEqual(result["sell_now_percent"], 25)
        self.assertEqual(result["remaining_after_sale_percent"], 0)

    def test_recommendation_levels_are_in_allowed_set(self):
        """sell_now_percent must be from {0,25,33,50,66,75,100}."""
        result = evaluate_sell_decision(_payload(pnl_pct=22.0, buy_price=100.0, current_price=122.0))
        self.assertIn(result["sell_now_percent"], {0, 25, 33, 50, 66, 75, 100})


class LmNativeFeaturesTest(unittest.TestCase):
    """Patterns #11 (Distribution-Tage), #15 (Volumen-Faktor) and LM-only features."""

    def test_distribution_days_become_watch_signal(self):
        """Pattern #11 stays in LM: 4+ distribution days raise a watch signal."""
        result = evaluate_sell_decision(_payload(
            pnl_pct=12.0,
            buy_price=100.0,
            current_price=112.0,
            metrics_extra={"distribution_days_25": 5},
        ))
        watch_labels = [s["label"] for s in result["watch_signals"]]
        self.assertTrue(any("Distribution" in label for label in watch_labels))

    def test_personality_check_adds_lm_only_tranche(self):
        """Personality-check is LM-only and contributes 25%."""
        result = evaluate_sell_decision(
            _payload(pnl_pct=12.0, buy_price=100.0, current_price=112.0),
            manual_data={"personality_changed": True},
        )
        self.assertTrue(any(s["id"] == "tranche_personality_changed" for s in result["tranche_signals"]))

    def test_weak_industry_group_with_gain_adds_lm_tranche(self):
        result = evaluate_sell_decision(
            _payload(pnl_pct=15.0, buy_price=100.0, current_price=115.0),
            manual_data={"industry_group_status": "Schwach"},
        )
        self.assertTrue(any(s["id"] == "tranche_weak_industry_gain" for s in result["tranche_signals"]))

    def test_low_closes_warning_remains_in_lm(self):
        """LM-native warning #11-adjacent: low_closes still contributes via WARNING_CONTRIBUTIONS."""
        result = evaluate_sell_decision({
            "ticker": "TEST",
            "buy_price": 100.0,
            "shares": 10.0,
            "metrics": {"current_price": 112.0, "pnl_pct": 12.0, "as_of_date": "2026-05-20"},
            "auto_checkboxes": {
                "warning_checkboxes": {"low_closes": True},
                "strength_checkboxes": {},
                "reasons": {"low_closes": "3 der letzten 5 Schlusskurse im unteren Kerzenviertel"},
            },
        })
        self.assertTrue(any(s["id"] == "warning_low_closes" for s in result["tranche_signals"]))

    def test_hub_duplicated_warnings_no_longer_double_fire(self):
        """Warnings that doppeln mit Hub-Strategien werden nicht mehr als LM-Tranche eingezogen."""
        result = evaluate_sell_decision({
            "ticker": "TEST",
            "buy_price": 100.0,
            "shares": 10.0,
            "metrics": {"current_price": 112.0, "pnl_pct": 12.0, "as_of_date": "2026-05-20"},
            "auto_checkboxes": {
                "warning_checkboxes": {
                    "stall_days_near_breakout": True,
                    "lower_lows_no_rebound": True,
                    "downside_reversal_near_high": True,
                    "three_loss_weeks_rising_volume": True,
                    "failed_breakout_high_volume": True,
                    "worst_day_high_volume": True,
                },
                "strength_checkboxes": {},
                "reasons": {},
            },
        })
        for blocked_id in ("warning_stall_days", "warning_lower_lows", "warning_downside_reversal"):
            self.assertFalse(
                any(s["id"] == blocked_id for s in result["tranche_signals"]),
                f"{blocked_id} should be Hub-only now",
            )


class RegimeAndPricingTest(unittest.TestCase):
    """LM-specific regime/stop/trigger logic should still work."""

    def test_regime_classification_keeps_lm_semantics(self):
        result = evaluate_sell_decision(_payload(pnl_pct=85.0, buy_price=100.0, current_price=185.0))
        self.assertEqual(result["regime"], "Großgewinner")

    def test_defensive_regime_for_loss(self):
        result = evaluate_sell_decision(_payload(pnl_pct=-3.0, buy_price=100.0, current_price=97.0, declining=True))
        self.assertEqual(result["regime"], "Defensiv")

    def test_recommendation_includes_stop_and_trigger_prices(self):
        result = evaluate_sell_decision(_payload(pnl_pct=12.0, buy_price=100.0, current_price=112.0))
        self.assertIn("stop_price", result)
        self.assertIn("next_tranche_trigger_price", result)
        self.assertIn("full_exit_price", result)


class HealthScoreTest(unittest.TestCase):
    def test_health_score_buckets_five_synthetic_positions(self):
        samples = [
            {"pnl_pct": 25.0, "current_price": 125.0, "sma21": 110.0, "sma50": 105.0, "rs_line": 1.2, "rs_ma21": 1.1, "rs_ma50": 1.0, "distribution_days_25": 1},
            {"pnl_pct": 12.0, "current_price": 112.0, "sma21": 110.0, "sma50": 108.0, "rs_line": 1.0, "rs_ma21": 1.0, "rs_ma50": 1.0, "distribution_days_25": 2},
            {"pnl_pct": 4.0, "current_price": 104.0, "sma21": 105.0, "sma50": 102.0, "rs_line": .95, "rs_ma21": 1.0, "rs_ma50": 1.0, "distribution_days_25": 4},
            {"pnl_pct": -4.0, "current_price": 96.0, "sma21": 100.0, "sma50": 100.0, "rs_line": .9, "rs_ma21": 1.0, "rs_ma50": .98, "distribution_days_25": 6},
            {"pnl_pct": -8.0, "current_price": 92.0, "sma21": 100.0, "sma50": 100.0, "rs_line": .9, "rs_ma21": 1.0, "rs_ma50": .98, "distribution_days_25": 7},
        ]
        results = [compute_sell_health_score({"ticker": f"T{i}", "metrics": sample}) for i, sample in enumerate(samples)]
        self.assertEqual(len(results), 5)
        self.assertTrue(all(0 <= item["health_score"] <= 100 for item in results))
        self.assertEqual(results[0]["status"], "Halten")
        self.assertEqual(results[-1]["status"], "Verkaufen")


class StrategyKeyTaggingTest(unittest.TestCase):
    def test_hub_signals_carry_strategy_key(self):
        """Hub signals must arrive with a strategy_key identifying their source strategy."""
        result = evaluate_sell_decision(
            _payload(pnl_pct=-8.0, buy_price=100.0, current_price=92.0, declining=True),
        )
        hub_signals = [s for s in result["killer_signals"] + result["tranche_signals"] if str(s.get("strategy_key", "")).startswith(("notbremse_", "drei_", "gewinn_", "ma", "drawdown_", "split_", "downside_", "stau_", "rueckkehr_", "verlusttage_", "rs_", "groesster_", "einfach", "atr_", "erschoepfungsluecke", "misslungener_"))]
        self.assertTrue(hub_signals, "At least one Hub-tagged signal expected on -8% pnl")
        for sig in hub_signals:
            self.assertIn(sig["strategy_key"], (
                "notbremse_verlust", "drei_stufen_nach_kauf", "gewinn_in_stufen", "ma21_bruch",
                "drawdown_vom_peak", "ma_abstand", "verlusttage_haeufung", "groesster_anstieg_volumen",
                "split_anstieg", "erschoepfungsluecke", "downside_reversal", "stau_tage",
                "rueckkehr_pivot", "ma_bruch_defensiv", "drei_verlustwochen", "groesster_einbruch",
                "rs_linie", "ma_basierte_sequenz", "einfach_halbe_position",
                "misslungener_ausbruch_5stufen", "einfache_verluststufen", "atr_basiert",
            ))

    def test_lm_native_signals_carry_lm_strategy_key(self):
        """Personality-check, weak-industry, watch signals get lm_* strategy_keys."""
        result = evaluate_sell_decision(
            _payload(pnl_pct=15.0, buy_price=100.0, current_price=115.0),
            manual_data={"personality_changed": True, "industry_group_status": "Schwach"},
        )
        personality_sigs = [s for s in result["tranche_signals"] if s["id"] == "tranche_personality_changed"]
        industry_sigs = [s for s in result["tranche_signals"] if s["id"] == "tranche_weak_industry_gain"]
        self.assertEqual(personality_sigs[0]["strategy_key"], "lm_personality_check")
        self.assertEqual(industry_sigs[0]["strategy_key"], "lm_industry_group")

    def test_active_strategies_subset_disables_others(self):
        """When active_strategies is restricted, only those Hub strategies fire."""
        payload = _payload(pnl_pct=-8.0, buy_price=100.0, current_price=92.0, declining=True,
                           setup={"active_strategies": ["notbremse_verlust"]})
        result = evaluate_sell_decision(payload)
        hub_keys = {s["strategy_key"] for s in result["killer_signals"] + result["tranche_signals"] if not str(s["strategy_key"]).startswith("lm_")}
        # Only notbremse_verlust should appear, NOT drei_stufen_nach_kauf or others.
        self.assertEqual(hub_keys, {"notbremse_verlust"})


class SetupOverrideTest(unittest.TestCase):
    def test_lm_setup_overrides_default_notbremse_thresholds(self):
        """Setup-Panel changes flow through metrics_payload['lm_setup'] to the Hub engine."""
        # With Bullisch default (-7%), pnl=-5 should NOT trigger Notbremse.
        result_default = evaluate_sell_decision(
            _payload(pnl_pct=-5.0, buy_price=100.0, current_price=95.0, declining=True),
            manual_data={"market_environment": "Bullisch"},
        )
        # With a stricter Bullisch-Setup (-4%), pnl=-5 SHOULD trigger Notbremse.
        result_strict = evaluate_sell_decision(
            _payload(
                pnl_pct=-5.0,
                buy_price=100.0,
                current_price=95.0,
                declining=True,
                setup={"notbremse_verlust_schwelle_bullisch_pct": 4.0},
            ),
            manual_data={"market_environment": "Bullisch"},
        )
        triggered_default = any("Notbremse" in s["label"] and s["contribution_percent"] >= 100 for s in result_default["killer_signals"])
        triggered_strict = any("Notbremse" in s["label"] and s["contribution_percent"] >= 100 for s in result_strict["killer_signals"])
        # The strict setup must trigger; the default at -5% bullisch must not.
        self.assertTrue(triggered_strict)
        self.assertFalse(triggered_default)


class NoOhlcGracefulFallbackTest(unittest.TestCase):
    """Calling evaluate_sell_decision without OHLC frames yields LM-only signals."""

    def test_distribution_watch_still_works_without_ohlc(self):
        result = evaluate_sell_decision({
            "ticker": "TEST",
            "buy_price": 100.0,
            "shares": 10.0,
            "metrics": {"current_price": 112.0, "pnl_pct": 12.0, "distribution_days_25": 5, "as_of_date": "2026-05-20"},
        })
        self.assertTrue(any("Distribution" in s["label"] for s in result["watch_signals"]))
        self.assertEqual(result["recommendation_percent"], 0)  # No Hub signals → HALTEN
        self.assertEqual(result["recommendation_label"], "HALTEN")


if __name__ == "__main__":
    unittest.main()
