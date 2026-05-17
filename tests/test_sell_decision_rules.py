import unittest

from sell_decision_rules import compute_sell_health_score, evaluate_sell_decision


def payload(metrics):
    return {"ticker": "TEST", "buy_price": 100.0, "shares": 10.0, "metrics": {"current_price": 110.0, **metrics}}


class SellDecisionRulesTest(unittest.TestCase):
    def test_loss_notbremse_returns_full_exit(self):
        result = evaluate_sell_decision(payload({"pnl_pct": -8.0}))
        self.assertEqual(result["recommendation_percent"], 100)
        self.assertEqual(result["recommendation_label"], "KOMPLETTVERKAUF")
        self.assertTrue(result["killer_signals"])

    def test_sma21_break_and_three_days_returns_50(self):
        result = evaluate_sell_decision(payload({"pnl_pct": 12.0, "days_under_sma21": 3}))
        self.assertEqual(result["recommendation_percent"], 50)
        self.assertEqual(result["target_total_sold_percent"], 50)

    def test_four_tranche_signals_minimum_75(self):
        result = evaluate_sell_decision(
            payload({
                "pnl_pct": 30.0,
                "days_under_sma21": 3,
                "drawdown_from_high_since_buy_pct": -8.5,
                "price_vs_sma50_pct": 30.0,
            })
        )
        self.assertGreaterEqual(result["target_total_sold_percent"], 75)
        self.assertGreaterEqual(result["recommendation_percent"], 75)

    def test_bearish_environment_raises_33_to_50(self):
        result = evaluate_sell_decision(
            payload({"pnl_pct": 12.0}),
            manual_data={"market_environment": "Bärisch"},
        )
        self.assertEqual(result["target_total_sold_percent"], 50)
        self.assertEqual(result["recommendation_percent"], 50)

    def test_no_tranche_sold_recommends_current_target(self):
        result = evaluate_sell_decision(payload({"pnl_pct": 22.0}))
        self.assertEqual(result["already_sold_percent"], 0)
        self.assertEqual(result["target_total_sold_percent"], 33)
        self.assertEqual(result["sell_now_percent"], 33)
        self.assertEqual(result["remaining_after_sale_percent"], 67)

    def test_already_sold_only_recommends_delta_to_higher_target(self):
        result = evaluate_sell_decision(
            payload({"pnl_pct": 22.0, "price_vs_sma50_pct": 30.0}),
            tranche_log=[{"ticker": "TEST", "tranche_percent": 33}],
        )
        self.assertEqual(result["already_sold_percent"], 33)
        self.assertEqual(result["target_total_sold_percent"], 66)
        self.assertEqual(result["sell_now_percent"], 33)
        self.assertEqual(result["remaining_after_sale_percent"], 34)

    def test_already_sold_prevents_double_sale(self):
        result = evaluate_sell_decision(
            payload({"pnl_pct": 22.0}),
            tranche_log=[{"ticker": "TEST", "tranche_percent": 33}],
        )
        self.assertEqual(result["target_total_sold_percent"], 33)
        self.assertEqual(result["recommendation_percent"], 0)
        self.assertEqual(result["sell_now_percent"], 0)
        self.assertIn("bereits", result["explanation_short"])

    def test_killer_signal_only_recommends_unsold_remainder(self):
        result = evaluate_sell_decision(
            payload({"pnl_pct": -8.0}),
            tranche_log=[{"ticker": "TEST", "tranche_percent": 75}],
        )
        self.assertEqual(result["already_sold_percent"], 75)
        self.assertEqual(result["target_total_sold_percent"], 100)
        self.assertEqual(result["sell_now_percent"], 25)
        self.assertEqual(result["remaining_after_sale_percent"], 0)

    def test_arbitrary_prior_sale_still_returns_allowed_tranche(self):
        result = evaluate_sell_decision(
            payload({"pnl_pct": 22.0, "price_vs_sma50_pct": 30.0}),
            tranche_log=[{"ticker": "TEST", "tranche_percent": 40}],
        )
        self.assertEqual(result["target_total_sold_percent"], 66)
        self.assertIn(result["sell_now_percent"], {0, 25, 33, 50, 66, 75, 100})
        self.assertEqual(result["sell_now_percent"], 25)

    def test_big_winner_regime_stays_active_with_small_weakness(self):
        result = evaluate_sell_decision(payload({"pnl_pct": 85.0, "days_under_sma21": 1}))
        self.assertEqual(result["regime"], "Großgewinner")
        self.assertEqual(result["target_total_sold_percent"], 25)
        self.assertIn(result["sell_now_percent"], {0, 25, 33, 50, 66, 75, 100})

    def test_health_score_for_five_synthetic_tickers(self):
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


if __name__ == "__main__":
    unittest.main()
