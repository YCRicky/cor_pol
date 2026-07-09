import unittest

from src.lab.empjp_core import (
    DEFAULT_CALIBRATION_PATH,
    EmpJPCell,
    EmpJPCalibration,
    EmpJPConfig,
    evaluate_empjp,
    state_path_dir,
    z_bin_label,
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


if __name__ == "__main__":
    unittest.main()
