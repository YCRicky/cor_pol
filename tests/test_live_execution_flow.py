import asyncio
import tempfile
import unittest
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from src.execution import ExecutionLegResult, PolymarketLiveExecutor
from src.lab.correlation_arb_bot import (
    GateConfig,
    LabCombo,
    MarketLeg,
    RunLedger,
    UserOrderFeed,
    _format_event,
    combo_exit_reason,
    confirm_live_result,
    crypto_taker_fee_per_share,
    entry_edge_gate_reason,
    execute_buy_pair,
    execution_unknown,
    fee_adjusted_model_edge,
    flatten_entry_exposure,
    is_us_stock_hours_utc8,
    is_weekend_rest_utc8,
    live_order_block_reason,
    post_ack_immediate_no_fill,
    resolve_and_record,
    us_stock_hours_price_gate_ok,
    weekend_rest_resume_ts_utc8,
)


class SilentFeed:
    ready = True
    connected = True

    async def wait_for_order(self, order_id, *, timeout_s, min_qty, target_qty=0, settle_s=0.5):
        return 0.0, None, False


class EmptyTerminalFeed:
    ready = True
    connected = True

    async def wait_for_order(self, order_id, *, timeout_s, min_qty, target_qty=0, settle_s=0.5):
        return 0.0, None, True


class DisconnectedFeed:
    ready = True
    connected = False

    async def wait_for_order(self, order_id, *, timeout_s, min_qty, target_qty=0, settle_s=0.5):
        raise AssertionError("disconnected user websocket must not be awaited")


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


class RetryExecutor:
    def __init__(self, single_fills):
        self.single_fills = list(single_fills)
        self.single_calls = []

    def buy_pair_limit_fak(self, *, leg_a, leg_b, slippage_ticks):
        return (
            ExecutionLegResult(
                leg_a["label"],
                leg_a["token_id"],
                leg_a["target_qty"],
                leg_a["target_qty"],
                0.50,
                leg_a["target_qty"] * 0.50,
                order_id="initial-a",
                status="ORDER_STATUS_MATCHED",
                ok=True,
                raw={"submission_state": "acknowledged", "execution_terminal_confirmed": True},
            ),
            ExecutionLegResult(
                leg_b["label"],
                leg_b["token_id"],
                leg_b["target_qty"],
                0.0,
                0.0,
                0.0,
                order_id="initial-b",
                status="ORDER_STATUS_CANCELED",
                ok=False,
                error="no_fill",
                raw={"submission_state": "acknowledged", "execution_terminal_confirmed": True},
            ),
        )

    def buy_limit_fak(self, *, label, token_id, target_qty, best_ask, tick_size, neg_risk, slippage_ticks):
        self.single_calls.append((label, target_qty, slippage_ticks))
        fill = self.single_fills.pop(0)
        return ExecutionLegResult(
            label,
            token_id,
            target_qty,
            fill,
            0.51 if fill > 0 else 0.0,
            fill * 0.51,
            order_id=f"single-{len(self.single_calls)}",
            status="ORDER_STATUS_MATCHED" if fill > 0 else "ORDER_STATUS_CANCELED",
            ok=fill > 0,
            error="" if fill > 0 else "no_fill",
            raw={"submission_state": "acknowledged", "execution_terminal_confirmed": True},
        )


class UnknownPairExecutor(RetryExecutor):
    def __init__(self):
        super().__init__([])

    def buy_pair_limit_fak(self, *, leg_a, leg_b, slippage_ticks):
        known, _ = super().buy_pair_limit_fak(
            leg_a=leg_a,
            leg_b=leg_b,
            slippage_ticks=slippage_ticks,
        )
        unknown = ExecutionLegResult(
            leg_b["label"],
            leg_b["token_id"],
            leg_b["target_qty"],
            0.0,
            0.0,
            0.0,
            status="",
            ok=False,
            error="request timeout",
            raw={"submission_state": "unknown"},
        )
        return known, unknown


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


UTC8 = timezone(timedelta(hours=8))


def _ts_utc8(year: int, month: int, day: int, hour: int, minute: int = 0) -> float:
    return datetime(year, month, day, hour, minute, tzinfo=UTC8).timestamp()


class LiveExecutionFlowTests(unittest.TestCase):
    def test_live_strategy_defaults_use_fee_adjusted_edge_gate(self):
        gates = GateConfig()
        self.assertEqual(gates.max_combos_per_round, 3)
        self.assertTrue(gates.weekend_rest_enabled)
        self.assertTrue(gates.us_stock_hours_filter_enabled)
        self.assertEqual(gates.min_model_edge, 0.05)
        self.assertEqual(gates.taker_fee_rate, 0.07)
        self.assertEqual(gates.taker_rebate_rate, 0.30)
        self.assertEqual(gates.entry_edge_reserve, 0.01)
        self.assertEqual(gates.min_net_model_edge, 0.02)
        self.assertEqual(gates.max_bad_quad_prob, 0.22)
        self.assertEqual(gates.max_bad_to_normal_ratio, 0.38)

    def test_crypto_taker_fee_matches_polymarket_formula(self):
        self.assertAlmostEqual(crypto_taker_fee_per_share(0.50, 0.07), 0.0175)
        self.assertAlmostEqual(crypto_taker_fee_per_share(0.30, 0.07), 0.0147)
        self.assertAlmostEqual(crypto_taker_fee_per_share(0.70, 0.07), 0.0147)

    def test_fee_adjusted_model_edge_subtracts_rebate_and_reserve(self):
        gates = GateConfig(
            taker_fee_rate=0.07,
            taker_rebate_rate=0.30,
            entry_edge_reserve=0.01,
        )
        net_edge, gross_fee, expected_fee = fee_adjusted_model_edge(
            0.06, 0.70, 0.20, gates,
        )
        self.assertAlmostEqual(gross_fee, 0.0259)
        self.assertAlmostEqual(expected_fee, 0.01813)
        self.assertAlmostEqual(net_edge, 0.03187)

    def test_fee_adjusted_model_edge_clamps_invalid_rebate(self):
        net_edge, gross_fee, expected_fee = fee_adjusted_model_edge(
            0.06,
            0.50,
            0.50,
            GateConfig(taker_rebate_rate=5.0, entry_edge_reserve=0.01),
        )
        self.assertAlmostEqual(gross_fee, 0.035)
        self.assertEqual(expected_fee, 0.0)
        self.assertAlmostEqual(net_edge, 0.05)

    def test_entry_edge_gate_requires_both_raw_and_net_edge(self):
        gates = GateConfig(min_model_edge=0.05, min_net_model_edge=0.02)
        self.assertTrue(entry_edge_gate_reason(0.049, 0.03, gates).startswith("model_edge_low"))
        self.assertTrue(entry_edge_gate_reason(0.06, 0.019, gates).startswith("net_model_edge_low"))
        self.assertEqual(entry_edge_gate_reason(float("nan"), 0.03, gates), "model_edge_invalid")
        self.assertIsNone(entry_edge_gate_reason(0.06, 0.03, gates))

    def test_invalid_fee_config_hard_rejects_entry_edge(self):
        gates = GateConfig(taker_fee_rate=float("nan"))
        net_edge, _, _ = fee_adjusted_model_edge(0.10, 0.70, 0.20, gates)
        self.assertEqual(entry_edge_gate_reason(0.10, net_edge, gates), "model_edge_invalid")

    def test_entry_notification_includes_raw_net_edge_and_fee(self):
        message = _format_event("entry", {
            "round": 1,
            "combo_id": 1,
            "tte": 120,
            "leg_a": "BTC_YES",
            "price_a": 0.70,
            "leg_b": "ETH_NO",
            "price_b": 0.20,
            "qty_a": 5.0,
            "qty_b": 5.0,
            "imbalance": 0.0,
            "gap": 0.10,
            "rho": 0.80,
            "execution": "live",
            "model_edge": 0.06,
            "net_model_edge": 0.03187,
            "expected_fee_per_combo": 0.09065,
        })
        self.assertIn("edge raw=+0.060 net=+0.032 fee~$0.091", message)

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

    def test_gtc_share_ioc_no_fill_requires_cancel_confirmation(self):
        result = ExecutionLegResult(
            "BTC_NO",
            "tok",
            5.0,
            0.0,
            0.0,
            0.0,
            order_id="o1",
            status="live",
            ok=False,
            raw={"success": True, "order_type": "GTC_SHARE_IOC", "order_lookup": {"status": "live"}},
        )
        self.assertFalse(post_ack_immediate_no_fill(result))

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
        self.assertEqual(confirmed.raw["confirm_source"], "clob_post_ack")

    def test_user_ws_empty_terminal_does_not_downgrade_clob_fill(self):
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
                order_feed=EmptyTerminalFeed(),
                gates=GateConfig(exec_user_ws_enabled=True, exec_user_ws_confirm_timeout_s=0.01),
            )
        )
        self.assertEqual(confirmed.filled_qty, 5.0)
        self.assertTrue(confirmed.ok)
        self.assertEqual(confirmed.raw["confirm_source"], "clob_after_user_ws_empty")

    def test_disconnected_user_ws_does_not_delay_clob_confirmation(self):
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
                order_feed=DisconnectedFeed(),
                gates=GateConfig(exec_user_ws_confirm_timeout_s=8.0),
            )
        )
        self.assertEqual(confirmed.filled_qty, 5.0)
        self.assertEqual(confirmed.raw["confirm_source"], "clob_post_ack")

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

    def test_stale_zero_lookup_does_not_override_post_ack_fill(self):
        raw = {
            "success": True,
            "orderID": "o4",
            "takingAmount": "5",
            "makingAmount": "2.5",
            "final_size_matched": "0",
            "final_order_status": "ORDER_STATUS_CANCELED",
            "order_type": "GTC_SHARE_IOC",
        }
        result = PolymarketLiveExecutor._parse_buy_response(
            object(),
            "BTC_YES",
            "tok",
            5.0,
            0.50,
            raw,
        )
        self.assertEqual(result.filled_qty, 5.0)
        self.assertEqual(result.notional, 2.5)

    def test_reconcile_waits_for_terminal_fill_after_cancel_ack(self):
        class ReconcileClient:
            def __init__(self):
                self.calls = 0

            def get_order(self, _order_id):
                self.calls += 1
                if self.calls == 1:
                    return {"status": "ORDER_STATUS_LIVE", "sizeMatched": "0"}
                return {"status": "ORDER_STATUS_CANCELED", "sizeMatched": "5"}

        executor = object.__new__(PolymarketLiveExecutor)
        executor.config = SimpleNamespace(reconcile_timeout_s=1.0, reconcile_poll_s=0.0)
        executor.client = ReconcileClient()
        raw = {
            "orderID": "o5",
            "submission_state": "acknowledged",
            "remainder_cancel_confirmed": True,
        }
        executor._reconcile_order(raw, 5.0)
        self.assertEqual(executor.client.calls, 2)
        self.assertEqual(raw["final_size_matched"], 5.0)
        self.assertTrue(raw["execution_terminal_confirmed"])

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

    def test_zero_fill_does_not_skip_remaining_chase_attempt(self):
        executor = RetryExecutor([0.0, 5.0])
        res_a, res_b, _events = asyncio.run(
            execute_buy_pair(
                legs_by_asset={"BTC": _leg("BTC"), "ETH": _leg("ETH")},
                leg_a="BTC_YES",
                leg_b="ETH_NO",
                qty=5.0,
                gates=GateConfig(exec_max_chase_attempts=2),
                live_executor=executor,
                order_feed=None,
            )
        )
        self.assertEqual(res_a.filled_qty, 5.0)
        self.assertEqual(res_b.filled_qty, 5.0)
        self.assertEqual(executor.single_calls, [
            ("ETH_NO", 5.0, 1),
            ("ETH_NO", 5.0, 2),
        ])

    def test_unknown_submission_never_chases_blindly(self):
        executor = UnknownPairExecutor()
        res_a, res_b, _events = asyncio.run(
            execute_buy_pair(
                legs_by_asset={"BTC": _leg("BTC"), "ETH": _leg("ETH")},
                leg_a="BTC_YES",
                leg_b="ETH_NO",
                qty=5.0,
                gates=GateConfig(exec_max_chase_attempts=2),
                live_executor=executor,
                order_feed=None,
            )
        )
        self.assertTrue(execution_unknown(res_a, res_b))
        self.assertEqual(executor.single_calls, [])

    def test_flatten_uses_full_ceiling_and_retries_after_zero_fill(self):
        executor = RetryExecutor([0.0, 5.0])
        legs = {"BTC": _leg("BTC"), "ETH": _leg("ETH")}
        res_a = ExecutionLegResult("BTC_YES", "BTCY", 5.0, 5.0, 0.5, 2.5)
        res_b = ExecutionLegResult("ETH_NO", "ETHN", 5.0, 0.0, 0.0, 0.0, ok=False)
        flat_a, flat_b, flat_ok, _events = asyncio.run(
            flatten_entry_exposure(
                legs_by_asset=legs,
                leg_a="BTC_YES",
                leg_b="ETH_NO",
                res_a=res_a,
                res_b=res_b,
                gates=GateConfig(
                    exec_flatten_slippage_ticks=24,
                    exec_flatten_max_attempts=3,
                ),
                live_executor=executor,
                order_feed=None,
            )
        )
        self.assertTrue(flat_ok)
        self.assertEqual(flat_a.filled_qty, 5.0)
        self.assertEqual(flat_b.filled_qty, 0.0)
        self.assertEqual(executor.single_calls, [
            ("BTC_NO", 5.0, 24),
            ("BTC_NO", 5.0, 24),
        ])

    def test_entry_abort_residual_bypasses_q4_signal(self):
        combo = LabCombo(
            combo_id=1,
            direction="A",
            leg_a="BTC_YES",
            leg_b="ETH_NO",
            price_a=0.7,
            price_b=0.2,
            qty=0.0,
            entered_at=0.0,
            qty_a=5.0,
            is_hedge=True,
        )
        self.assertEqual(
            combo_exit_reason(combo, -0.1, -0.1, 10.0, 10.0, GateConfig()),
            "entry_abort_residual",
        )

    def test_unresolved_pm_outcome_is_retried_before_ledger_record(self):
        unresolved = {"round": 7, "status": "UNRESOLVED"}
        resolved = {
            "round": 7,
            "status": "OK",
            "pnl": 1.25,
            "cost": 4.0,
            "gross": 5.0,
            "flip_pnl": 0.25,
            "combos_count": 1,
            "pm_btc_up": 1.0,
            "pm_eth_up": 0.0,
            "divergence": False,
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            pending = {"round": 7, "jsonl": str(Path(temp_dir) / "round.jsonl")}
            ledger = RunLedger()
            resolve_calls = []
            responses = iter((unresolved, resolved))

            async def fake_resolve_round(*args, **kwargs):
                resolve_calls.append((args, kwargs))
                return next(responses)

            async def fake_sleep(_seconds):
                return None

            with patch(
                "src.lab.correlation_arb_bot.resolve_round",
                new=fake_resolve_round,
            ), patch(
                "src.lab.correlation_arb_bot.asyncio.sleep",
                new=fake_sleep,
            ):
                summary = asyncio.run(
                    resolve_and_record(
                        pending,
                        GateConfig(pm_resolution_retry_s=1.0),
                        ledger,
                    )
                )
        self.assertEqual(len(resolve_calls), 2)
        self.assertEqual(summary["status"], "OK")
        self.assertEqual(summary["resolved_count"], 1)
        self.assertEqual(summary["run_pnl"], 1.25)

    def test_user_ws_state_never_blocks_live_ordering(self):
        self.assertIsNone(
            live_order_block_reason(
                live_executor=object(),
                gates=GateConfig(exec_user_ws_enabled=True),
                order_feed=None,
            )
        )

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

    def test_weekend_rest_window_uses_utc8_boundaries(self):
        self.assertFalse(is_weekend_rest_utc8(_ts_utc8(2026, 6, 6, 4, 59)))
        self.assertTrue(is_weekend_rest_utc8(_ts_utc8(2026, 6, 6, 5, 0)))
        self.assertTrue(is_weekend_rest_utc8(_ts_utc8(2026, 6, 7, 12, 0)))
        self.assertTrue(is_weekend_rest_utc8(_ts_utc8(2026, 6, 8, 4, 59)))
        self.assertFalse(is_weekend_rest_utc8(_ts_utc8(2026, 6, 8, 5, 0)))
        resume = datetime.fromtimestamp(weekend_rest_resume_ts_utc8(_ts_utc8(2026, 6, 6, 5, 1)), UTC8)
        self.assertEqual(resume.weekday(), 0)
        self.assertEqual(resume.time(), time(5, 0))

    def test_us_stock_hours_window_uses_utc8(self):
        self.assertFalse(is_us_stock_hours_utc8(_ts_utc8(2026, 6, 1, 21, 29)))
        self.assertTrue(is_us_stock_hours_utc8(_ts_utc8(2026, 6, 1, 21, 30)))
        self.assertTrue(is_us_stock_hours_utc8(_ts_utc8(2026, 6, 2, 3, 59)))
        self.assertFalse(is_us_stock_hours_utc8(_ts_utc8(2026, 6, 2, 4, 0)))
        self.assertTrue(is_us_stock_hours_utc8(_ts_utc8(2026, 6, 6, 3, 59)))
        self.assertFalse(is_us_stock_hours_utc8(_ts_utc8(2026, 6, 6, 4, 0)))

    def test_us_stock_hours_price_gate_accepts_threshold(self):
        gates = GateConfig(us_stock_hours_min_leg_price=0.70)
        self.assertFalse(us_stock_hours_price_gate_ok(0.69, 0.40, gates))
        self.assertTrue(us_stock_hours_price_gate_ok(0.70, 0.20, gates))
        self.assertTrue(us_stock_hours_price_gate_ok(0.71, 0.20, gates))


if __name__ == "__main__":
    unittest.main()
