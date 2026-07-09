import asyncio
import unittest
from unittest.mock import patch

from src.execution import ExecutionLegResult
from src.lab.correlation_arb_bot import GateConfig
from src.lab.empjp_core import (
    DEFAULT_CALIBRATION_PATH,
    EmpJPCell,
    EmpJPCalibration,
    EmpJPConfig,
    evaluate_empjp,
    state_path_dir,
    z_bin_label,
)
from src.lab.empjp_live_bot import (
    _entry_result_trackable,
    _entry_target_ok,
    execute_empjp_entry,
)


class Book:
    def __init__(self, bid=0.54, ask=0.55, size=20.0):
        self.bids = {bid: size}
        self.asks = {ask: size}

    def best_bid(self):
        p = max(self.bids)
        return p, self.bids[p]

    def best_ask(self):
        p = min(self.asks)
        return p, self.asks[p]


class FakeLeg:
    def __init__(self, yes_ask=0.40, no_ask=0.60):
        self.yes_book = Book(bid=yes_ask - 0.01, ask=yes_ask, size=20.0)
        self.no_book = Book(bid=no_ask - 0.01, ask=no_ask, size=20.0)


class EmpJPCoreTests(unittest.TestCase):
    def test_calibration_loads_frozen_cells(self):
        cal = EmpJPCalibration.load(DEFAULT_CALIBRATION_PATH)
        self.assertGreater(len(cal.cells), 100)
        self.assertGreaterEqual(cal.sigma_default, 0.5)
        self.assertEqual(cal.meta["strategy"], "empjp_e75_n30_c1_l1")

    def test_state_binning_matches_research_script(self):
        self.assertEqual(state_path_dir(8, -2), "up_dominant")
        self.assertEqual(state_path_dir(1, -9), "down_dominant")
        self.assertEqual(state_path_dir(4, -3), "balanced")
        self.assertEqual(z_bin_label(-5), "[-5.0, -4.5]")
        self.assertEqual(z_bin_label(0.1), "(0.0, 0.5]")

    def test_evaluate_empjp_can_emit_signal_from_known_cell(self):
        cal = EmpJPCalibration.load(DEFAULT_CALIBRATION_PATH)
        # Force a known high-UP probability cell so the pure evaluator can be
        # tested without depending on a live PM book.
        cal.cells[(120, "(0.0, 0.5]", "balanced")] = EmpJPCell(cell_n=100, emp_up=0.80)
        cfg = EmpJPConfig(edge_min=0.075, min_cell_n=30, min_entry_elapsed_s=45, max_entry_elapsed_s=255)
        spot_history = [(0.0, 100.0), (1.0, 100.0), (2.0, 100.0), (3.0, 100.0)]
        cand, reason = evaluate_empjp(
            calibration=cal,
            cfg=cfg,
            elapsed_s=120,
            tte=120,
            open_px=100.0,
            last_px=100.0,
            spot_history=spot_history,
            yes_book=Book(bid=0.54, ask=0.55),
            no_book=Book(bid=0.44, ask=0.45),
        )
        self.assertEqual(reason, "pass")
        self.assertIsNotNone(cand)
        assert cand is not None
        self.assertEqual(cand.side, "YES")
        self.assertGreater(cand.edge, 0.20)

    def test_entry_target_ok_uses_single_leg_tolerance(self):
        res = ExecutionLegResult("BTC_YES", "tok", 5.0, 4.5, 0.40, 1.80)
        self.assertTrue(_entry_target_ok(res, 5.0, 0.5))
        self.assertTrue(_entry_result_trackable(res))
        low = ExecutionLegResult("BTC_YES", "tok", 5.0, 3.0, 0.40, 1.20)
        self.assertFalse(_entry_target_ok(low, 5.0, 0.5))
        self.assertTrue(_entry_result_trackable(low))

    def test_execute_empjp_entry_chases_remaining_single_leg_qty(self):
        calls = []

        async def fake_buy_leg(**kwargs):
            calls.append((kwargs["qty"], kwargs["slippage_ticks"]))
            if len(calls) == 1:
                return ExecutionLegResult("BTC_YES", "tok", kwargs["qty"], 3.0, 0.40, 1.20)
            return ExecutionLegResult("BTC_YES", "tok", kwargs["qty"], 2.0, 0.41, 0.82)

        gates = GateConfig(
            leg_mismatch_tolerance_shares=0.5,
            exec_slippage_ticks=2,
            exec_chase_slippage_ticks=1,
            exec_max_chase_attempts=2,
        )
        cfg = EmpJPConfig(quantity=5.0, edge_min=0.075)
        with patch("src.lab.empjp_live_bot.execute_buy_leg", new=fake_buy_leg):
            execution = asyncio.run(
                execute_empjp_entry(
                    leg=FakeLeg(yes_ask=0.40),
                    side="YES",
                    target_qty=5.0,
                    side_prob=0.60,
                    cfg=cfg,
                    exec_gates=gates,
                    live_executor=None,
                    order_feed=None,
                )
            )

        self.assertTrue(execution.target_ok)
        self.assertTrue(execution.trackable)
        self.assertEqual(execution.result.filled_qty, 5.0)
        self.assertAlmostEqual(execution.result.avg_price, (1.20 + 0.82) / 5.0)
        self.assertEqual(calls, [(5.0, 2), (2.0, 1)])

    def test_execute_empjp_entry_tracks_partial_when_chase_not_enough(self):
        async def fake_buy_leg(**kwargs):
            return ExecutionLegResult("BTC_YES", "tok", kwargs["qty"], 3.0, 0.40, 1.20)

        gates = GateConfig(leg_mismatch_tolerance_shares=0.5, exec_max_chase_attempts=0)
        cfg = EmpJPConfig(quantity=5.0, edge_min=0.075)
        with patch("src.lab.empjp_live_bot.execute_buy_leg", new=fake_buy_leg):
            execution = asyncio.run(
                execute_empjp_entry(
                    leg=FakeLeg(yes_ask=0.40),
                    side="YES",
                    target_qty=5.0,
                    side_prob=0.60,
                    cfg=cfg,
                    exec_gates=gates,
                    live_executor=None,
                    order_feed=None,
                )
            )

        self.assertFalse(execution.target_ok)
        self.assertTrue(execution.trackable)
        self.assertEqual(execution.reason, "partial_tracked")
        self.assertEqual(execution.result.filled_qty, 3.0)

    def test_execute_empjp_entry_does_not_blind_chase_unknown_clob_state(self):
        calls = []

        async def fake_buy_leg(**kwargs):
            calls.append(kwargs["qty"])
            return ExecutionLegResult(
                "BTC_YES",
                "tok",
                kwargs["qty"],
                0.0,
                0.0,
                0.0,
                order_id="oid",
                ok=False,
                error="gtc_cancel_lookup_unconfirmed",
                raw={"submission_state": "unknown"},
            )

        gates = GateConfig(
            leg_mismatch_tolerance_shares=0.5,
            exec_slippage_ticks=2,
            exec_chase_slippage_ticks=1,
            exec_max_chase_attempts=2,
        )
        cfg = EmpJPConfig(quantity=5.0, edge_min=0.075)
        with patch("src.lab.empjp_live_bot.execute_buy_leg", new=fake_buy_leg):
            execution = asyncio.run(
                execute_empjp_entry(
                    leg=FakeLeg(yes_ask=0.40),
                    side="YES",
                    target_qty=5.0,
                    side_prob=0.60,
                    cfg=cfg,
                    exec_gates=gates,
                    live_executor=None,
                    order_feed=None,
                )
            )

        self.assertEqual(calls, [5.0])
        self.assertFalse(execution.trackable)
        self.assertEqual(execution.unknown_reason, "gtc_cancel_lookup_unconfirmed")

    def test_execute_empjp_entry_stops_chasing_when_edge_decays(self):
        calls = []
        leg = FakeLeg(yes_ask=0.60)

        async def fake_buy_leg(**kwargs):
            calls.append(kwargs["qty"])
            return ExecutionLegResult("BTC_YES", "tok", kwargs["qty"], 3.0, 0.40, 1.20)

        gates = GateConfig(leg_mismatch_tolerance_shares=0.5, exec_max_chase_attempts=2)
        cfg = EmpJPConfig(quantity=5.0, edge_min=0.075)
        with patch("src.lab.empjp_live_bot.execute_buy_leg", new=fake_buy_leg):
            execution = asyncio.run(
                execute_empjp_entry(
                    leg=leg,
                    side="YES",
                    target_qty=5.0,
                    side_prob=0.62,
                    cfg=cfg,
                    exec_gates=gates,
                    live_executor=None,
                    order_feed=None,
                )
            )

        self.assertEqual(calls, [5.0])
        self.assertFalse(execution.target_ok)
        self.assertTrue(execution.trackable)
        self.assertEqual(execution.events[-1]["reason"], "edge_decayed")


if __name__ == "__main__":
    unittest.main()
