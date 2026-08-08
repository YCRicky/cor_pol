import asyncio
import unittest

from src.execution import PolymarketLiveExecutor, _round_down_to_tick
from src.lab.correlation_arb_bot import GateConfig
from src.lab.twap_price_path_tail_live_bot import (
    TailLeg,
    execute_capped_buy,
    market_twap_rule_reason,
    tail_fee_per_share,
)


def valid_market():
    return {
        "eventStartTime": "2026-08-07T00:00:00Z",
        "endDate": "2026-08-07T00:05:00Z",
        "cryptoMarketConfig": {
            "asset": "btc",
            "twapEnabled": True,
            "twapLookbackSeconds": 30,
        },
        "resolutionSource": "https://data.chain.link/streams/btc-usd-twap-30s-streams",
        "acceptingOrders": True,
        "closed": False,
        "enableOrderBook": True,
    }


class TwapPricePathTailLiveBotTest(unittest.TestCase):
    def test_market_must_explicitly_advertise_twap_30(self):
        self.assertIsNone(market_twap_rule_reason(valid_market(), "BTC"))

        market = valid_market()
        market["cryptoMarketConfig"]["twapLookbackSeconds"] = 60
        self.assertEqual(market_twap_rule_reason(market, "BTC"), "twap_lookback_not_30s")

    def test_fee_uses_market_base_rate_and_explicit_rebate(self):
        self.assertAlmostEqual(tail_fee_per_share(0.50, 0.10, 0.0), 0.025)
        self.assertAlmostEqual(tail_fee_per_share(0.50, 0.10, 0.30), 0.0175)

    def test_price_cap_rounds_down_and_blocks_a_higher_ask_without_submission(self):
        self.assertEqual(_round_down_to_tick(0.999, "0.01"), 0.99)
        executor = PolymarketLiveExecutor.__new__(PolymarketLiveExecutor)

        result = executor.buy_limit_fak(
            label="BTC_YES",
            token_id="token",
            target_qty=5.0,
            best_ask=0.995,
            tick_size="0.01",
            neg_risk=False,
            slippage_ticks=1,
            price_cap=0.99,
        )

        self.assertFalse(result.ok)
        self.assertEqual(result.error, "best_ask_above_price_cap")
        self.assertEqual(result.raw["submission_state"], "not_submitted")

    def test_shadow_capped_buy_does_not_cross_the_price_cap(self):
        leg = TailLeg(
            asset="BTC",
            symbol="BTCUSDT",
            slug="btc-updown-5m-1",
            question="",
            start_ts=1,
            end_ts=301,
            yes_token="yes",
            no_token="no",
            condition_id="condition",
            tick_size="0.01",
            neg_risk=False,
            min_order_size=5.0,
            taker_base_fee_rate=0.1,
        )
        leg.yes_book.apply_snapshot(
            [{"price": "0.97", "size": "10"}],
            [
                {"price": "0.98", "size": "3"},
                {"price": "0.99", "size": "3"},
                {"price": "0.995", "size": "10"},
            ],
            received_ts=1.0,
        )

        result = asyncio.run(execute_capped_buy(
            leg=leg,
            side="YES",
            quantity=7.0,
            price_cap=0.99,
            slippage_ticks=1,
            live_executor=None,
            order_feed=None,
            gates=GateConfig(),
        ))

        self.assertEqual(result.filled_qty, 6.0)
        self.assertAlmostEqual(result.avg_price, 0.985)


if __name__ == "__main__":
    unittest.main()
