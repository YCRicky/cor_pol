import unittest

from src.lab.twap_price_path_tail_core import (
    BinanceTrade,
    PMQuote,
    evaluate_tail_decision,
)


class TwapPricePathTailCoreTest(unittest.TestCase):
    end_ts = 1300.0
    decision_ts = 1289.75

    def quote(self, bid, ts=None):
        return PMQuote(received_ts=self.decision_ts - 0.1 if ts is None else ts, best_bid=bid, best_ask=bid + 0.01)

    def trade(self, ts, price, trade_id=0, received_ts=None):
        return BinanceTrade(
            trade_ts=ts,
            price=price,
            received_ts=ts if received_ts is None else received_ts,
            trade_id=trade_id,
        )

    def decide(self, *, yes_quotes=None, no_quotes=None, trades=None, candle_open=100.0):
        return evaluate_tail_decision(
            end_ts=self.end_ts,
            decision_ts=self.decision_ts,
            candle_open=candle_open,
            yes_quotes=yes_quotes or [self.quote(0.91)],
            no_quotes=no_quotes or [self.quote(0.08)],
            binance_trades=trades or [
                self.trade(1270.0, 100.00, 1),
                self.trade(1280.0, 100.02, 2),
                self.trade(1289.5, 100.03, 3),
            ],
        )

    def test_weak_candle_passes_when_entire_tail_is_aligned(self):
        result = self.decide()

        self.assertTrue(result.eligible)
        self.assertEqual(result.side, "YES")
        self.assertEqual(result.reason, "weak_pass")
        self.assertAlmostEqual(result.signed_candle_bp, 3.0, places=4)

    def test_no_side_uses_the_same_signed_path_rule(self):
        result = self.decide(
            yes_quotes=[self.quote(0.06)],
            no_quotes=[self.quote(0.92)],
            trades=[
                self.trade(1270.0, 100.00, 1),
                self.trade(1280.0, 99.98, 2),
                self.trade(1289.5, 99.97, 3),
            ],
        )

        self.assertTrue(result.eligible)
        self.assertEqual(result.side, "NO")
        self.assertEqual(result.reason, "weak_pass")

    def test_stale_binance_trade_is_a_hard_skip(self):
        result = self.decide(trades=[
            self.trade(1270.0, 100.00, 1),
            self.trade(1286.9, 100.03, 2),
        ])

        self.assertFalse(result.eligible)
        self.assertEqual(result.reason, "stale_binance_trade")

    def test_weak_last_ten_second_reversal_is_rejected(self):
        result = self.decide(trades=[
            self.trade(1270.0, 100.00, 1),
            self.trade(1280.0, 100.04, 2),
            self.trade(1289.5, 100.03, 3),
        ])

        self.assertFalse(result.eligible)
        self.assertEqual(result.reason, "weak_last10_reversal")

    def test_strong_candle_keeps_the_replay_selected_no_veto_mode(self):
        result = self.decide(
            candle_open=99.90,
            trades=[
                self.trade(1270.0, 100.00, 1),
                self.trade(1280.0, 100.20, 2),
                self.trade(1289.5, 100.10, 3),
            ],
        )

        self.assertTrue(result.eligible)
        self.assertEqual(result.reason, "strong_pass")
        self.assertLess(result.signed_last10_bp, 0.0)

    def test_quotes_after_the_cutoff_are_not_used(self):
        result = self.decide(yes_quotes=[
            self.quote(0.91, self.decision_ts - 0.1),
            self.quote(0.05, self.decision_ts + 0.1),
        ])

        self.assertTrue(result.eligible)
        self.assertAlmostEqual(result.leader_bid, 0.91)

    def test_missing_pre_tail_anchor_is_rejected(self):
        result = self.decide(trades=[
            self.trade(1270.1, 100.00, 1),
            self.trade(1289.5, 100.03, 2),
        ])

        self.assertFalse(result.eligible)
        self.assertEqual(result.reason, "missing_binance_tail_anchor")


if __name__ == "__main__":
    unittest.main()
