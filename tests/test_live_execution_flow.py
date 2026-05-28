import asyncio
import unittest
from pathlib import Path

from src.execution import ExecutionLegResult, PolymarketLiveExecutor
from src.lab.correlation_arb_bot import (
    GateConfig,
    MarketLeg,
    UserOrderFeed,
    confirm_live_result,
    execute_buy_pair,
    post_ack_immediate_no_fill,
)


class SilentFeed:
    ready = True
    connected = True

    async def wait_for_order(self, order_id, *, timeout_s, min_qty, target_qty=0, settle_s=0.5):
        return 0.0, None, False


class PairExecutor:
    def __init__(self):
        self.single_calls = []

    def buy_pair_limit_fak(self, *, leg_a, leg_b, slippage_ticks):
        return (
            ExecutionLegResult(
                leg_a["label"],
                leg_a["token_id"],
                leg_a["target_qty"],
                0.0,
                0.0,
                0.0,
                order_id="a",
                status="",
                ok=False,
                raw={"success": True, "order_type": "GTC_SHARE_IOC", "cancel_response": {"canceled": ["a"]}},
            ),
            ExecutionLegResult(
                leg_b["label"],
                leg_b["token_id"],
                leg_b["target_qty"],
                5.0,
                0.86,
                4.3,
                order_id="b",
                status="matched",
                ok=True,
                raw={"order_type": "GTC_SHARE_IOC"},
            ),
        )

    def buy_limit_fak(self, *, label, token_id, target_qty, best_ask, tick_size, neg_risk, slippage_ticks):
        self.single_calls.append((label, target_qty, slippage_ticks))
        return ExecutionLegResult(
            label,
            token_id,
            target_qty,
            target_qty,
            0.51,
            target_qty * 0.51,
            order_id="c",
            status="matched",
            ok=True,
            raw={"order_type": "GTC_SHARE_IOC"},
        )


def _leg(asset: str) -> MarketLeg:
    leg = MarketLeg(
        asset=asset,
        slug="",
        question="",
        start_ts=0,
        end_ts=9999999999,
        yes_token=f"{asset}Y",
        no_token=f"{asset}N",
        symbol="",
    )
    leg.yes_book.asks[0.50] = 100.0
    leg.no_book.asks[0.50] = 100.0
    return leg


class LiveExecutionFlowTests(unittest.TestCase):
    def test_post_ack_no_fill_is_not_unknown(self):
        result = ExecutionLegResult(
            "BTC_NO",
            "tok",
            5.0,
            0.0,
            0.0,
            0.0,
            order_id="o1",
            status="",
            ok=False,
            raw={"success": True, "order_type": "GTC_SHARE_IOC", "cancel_response": {"canceled": ["o1"]}},
        )
        self.assertTrue(post_ack_immediate_no_fill(result))
        confirmed = asyncio.run(
            confirm_live_result(
                result,
                live_executor=object(),
                order_feed=SilentFeed(),
                gates=GateConfig(exec_user_ws_confirm_timeout_s=0.01),
            )
        )
        self.assertEqual(confirmed.error, "no_fill")
        self.assertEqual(confirmed.raw["confirm_source"], "post_ack_no_fill")

    def test_post_ack_fill_survives_user_ws_timeout(self):
        result = ExecutionLegResult(
            "ETH_YES",
            "tok",
            5.0,
            5.0,
            0.86,
            4.3,
            order_id="o2",
            status="matched",
            ok=True,
            raw={"order_type": "GTC_SHARE_IOC"},
        )
        confirmed = asyncio.run(
            confirm_live_result(
                result,
                live_executor=object(),
                order_feed=SilentFeed(),
                gates=GateConfig(exec_user_ws_confirm_timeout_s=0.01),
            )
        )
        self.assertEqual(confirmed.filled_qty, 5.0)
        self.assertTrue(confirmed.ok)
        self.assertEqual(confirmed.raw["confirm_source"], "post_ack_after_user_ws_timeout")

    def test_clob_share_ioc_response_is_clamped_to_target_qty(self):
        raw = {
            "success": True,
            "orderID": "o3",
            "takingAmount": "5.82",
            "makingAmount": "5.0052",
            "order_type": "GTC_SHARE_IOC",
        }
        result = PolymarketLiveExecutor._parse_buy_response(
            object(),
            "ETH_NO",
            "tok",
            5.0,
            0.86,
            raw,
        )
        self.assertEqual(result.filled_qty, 5.0)
        self.assertEqual(result.notional, 4.3)
        self.assertEqual(result.raw["filled_qty_clamped_from"], 5.82)
        self.assertEqual(result.raw["notional_clamped_from"], 5.0052)

    def test_one_leg_fill_one_leg_no_fill_chases_short_leg(self):
        executor = PairExecutor()
        res_a, res_b, _events = asyncio.run(
            execute_buy_pair(
                legs_by_asset={"BTC": _leg("BTC"), "ETH": _leg("ETH")},
                leg_a="BTC_YES",
                leg_b="ETH_NO",
                qty=5.0,
                gates=GateConfig(exec_user_ws_confirm_timeout_s=0.01),
                live_executor=executor,
                order_feed=SilentFeed(),
            )
        )
        self.assertEqual(res_a.filled_qty, 5.0)
        self.assertEqual(res_b.filled_qty, 5.0)
        self.assertEqual(executor.single_calls, [("BTC_YES", 5.0, 1)])

    def test_user_ws_trade_weighted_average_price(self):
        async def run_case():
            feed = UserOrderFeed(markets=["m"], jsonl=Path("NUL"))
            await feed._record({
                "event_type": "trade",
                "id": "t1",
                "status": "MATCHED",
                "taker_order_id": "o3",
                "size": "2",
                "price": "0.40",
            })
            await feed._record({
                "event_type": "trade",
                "id": "t2",
                "status": "MATCHED",
                "taker_order_id": "o3",
                "size": "3",
                "price": "0.60",
            })
            return feed.matched_qty("o3"), feed.matched_price("o3")

        qty, price = asyncio.run(run_case())
        self.assertEqual(qty, 5.0)
        self.assertAlmostEqual(price, 0.52)


if __name__ == "__main__":
    unittest.main()
