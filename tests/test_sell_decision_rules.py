import unittest

from sell_decision_rules import evaluate_sell_decision


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

    def test_already_sold_prevents_double_sale(self):
        result = evaluate_sell_decision(
            payload({"pnl_pct": 22.0}),
            tranche_log=[{"ticker": "TEST", "tranche_percent": 33}],
        )
        self.assertEqual(result["target_total_sold_percent"], 0)
        self.assertEqual(result["recommendation_percent"], 0)
        self.assertEqual(result["sell_now_percent"], 0)


if __name__ == "__main__":
    unittest.main()
