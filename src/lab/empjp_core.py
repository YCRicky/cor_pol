from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional


EFFECTIVE_TAKER_FEE_RATE = 0.07 * (1.0 - 0.30)
DEFAULT_CALIBRATION_PATH = Path(__file__).resolve().parents[2] / "data" / "empjp_e75_n30_c1_l1_calibration.json"


def taker_fee_per_share(price: float, fee_rate: float = EFFECTIVE_TAKER_FEE_RATE) -> float:
    price = min(max(float(price), 0.0), 1.0)
    return fee_rate * price * (1.0 - price)


def state_path_dir(path_max_bp: float, path_min_bp: float) -> str:
    if abs(path_max_bp) > abs(path_min_bp) + 2.0:
        return "up_dominant"
    if abs(path_min_bp) > abs(path_max_bp) + 2.0:
        return "down_dominant"
    return "balanced"


def z_bin_label(z_resid: float) -> str:
    edges = [x / 2 for x in range(-10, 11)]
    z = max(-5.0, min(5.0, float(z_resid)))
    idx = max(0, min(len(edges) - 2, int(math.floor((z + 5.0) / 0.5))))
    left, right = edges[idx], edges[idx + 1]
    if idx == 0:
        return f"[{left}, {right}]"
    return f"({left}, {right}]"


@dataclass(frozen=True)
class EmpJPCell:
    cell_n: int
    emp_up: float


@dataclass(frozen=True)
class EmpJPCandidate:
    side: str
    side_prob: float
    entry_price: float
    edge: float
    cell_n: int
    cell_key: str
    tte: float
    elapsed_s: int
    current_bp: float
    path_max_bp: float
    path_min_bp: float
    sigma: float
    yes_ask: Optional[float]
    no_ask: Optional[float]
    yes_spread: Optional[float]
    no_spread: Optional[float]
    yes_depth: float
    no_depth: float


class EmpJPCalibration:
    def __init__(self, *, cells: dict[tuple[int, str, str], EmpJPCell], sigma_default: float, meta: dict[str, Any]):
        self.cells = cells
        self.sigma_default = max(float(sigma_default or 1.0), 0.5)
        self.meta = meta

    @classmethod
    def load(cls, path: Path | str = DEFAULT_CALIBRATION_PATH) -> "EmpJPCalibration":
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        cells: dict[tuple[int, str, str], EmpJPCell] = {}
        for row in payload.get("cells", []):
            key = (int(row["tte_bin"]), str(row["z_bin"]), str(row["path_dir"]))
            cells[key] = EmpJPCell(cell_n=int(row["cell_n"]), emp_up=float(row["emp_up"]))
        return cls(cells=cells, sigma_default=float(payload.get("sigma_default", 1.0)), meta=dict(payload.get("meta", {})))

    def prob_up(
        self,
        *,
        tte: float,
        current_bp: float,
        sigma: Optional[float],
        path_max_bp: float,
        path_min_bp: float,
    ) -> tuple[Optional[float], int, tuple[int, str, str]]:
        sig = float(sigma) if sigma and math.isfinite(float(sigma)) and float(sigma) > 0 else self.sigma_default
        sig = max(sig, 0.5)
        z_resid = float(current_bp) / (sig * math.sqrt(max(float(tte), 1.0)))
        key = (int(math.floor(max(float(tte), 0.0) / 30.0) * 30), z_bin_label(z_resid), state_path_dir(path_max_bp, path_min_bp))
        cell = self.cells.get(key)
        if cell is None:
            return None, 0, key
        return cell.emp_up, cell.cell_n, key


@dataclass(frozen=True)
class EmpJPConfig:
    quantity: float = 5.0
    edge_min: float = 0.075
    min_cell_n: int = 30
    confirm_s: int = 1
    latency_s: int = 1
    min_entry_elapsed_s: float = 45.0
    max_entry_elapsed_s: float = 255.0
    min_tte_s: float = 45.0
    max_tte_s: float = 240.0
    min_ask: float = 0.18
    max_ask: float = 0.82
    max_spread: float = 0.05
    min_depth: float = 5.0
    effective_fee_rate: float = EFFECTIVE_TAKER_FEE_RATE

    @property
    def horizon_s(self) -> int:
        return max(0, int(self.confirm_s) + int(self.latency_s))


def best_price_size(book: Any, side: str) -> tuple[Optional[float], float]:
    got = book.best_ask() if side == "ask" else book.best_bid()
    if got is None:
        return None, 0.0
    return float(got[0]), float(got[1])


def depth_total(book: Any) -> float:
    return float(sum(book.asks.values()) + sum(book.bids.values()))


def spread(book: Any) -> Optional[float]:
    bid, _bid_sz = best_price_size(book, "bid")
    ask, _ask_sz = best_price_size(book, "ask")
    if bid is None or ask is None:
        return None
    return max(0.0, ask - bid)


def current_bp(open_px: Optional[float], last_px: Optional[float]) -> Optional[float]:
    if open_px is None or last_px is None or open_px <= 0 or last_px <= 0:
        return None
    return (last_px / open_px - 1.0) * 10000.0


def rolling_sigma_bp_per_s(spot_history: list[tuple[float, float]], fallback: float) -> float:
    if len(spot_history) < 3:
        return max(float(fallback), 0.5)
    returns: list[float] = []
    prev_px: Optional[float] = None
    for _ts, px in spot_history[-180:]:
        if prev_px and prev_px > 0 and px > 0:
            returns.append((px / prev_px - 1.0) * 10000.0)
        prev_px = px
    if len(returns) < 8:
        return max(float(fallback), 0.5)
    mu = sum(returns) / len(returns)
    var = sum((x - mu) ** 2 for x in returns) / max(len(returns) - 1, 1)
    return max(math.sqrt(var), 0.5)


def evaluate_empjp(
    *,
    calibration: EmpJPCalibration,
    cfg: EmpJPConfig,
    elapsed_s: int,
    tte: float,
    open_px: Optional[float],
    last_px: Optional[float],
    spot_history: list[tuple[float, float]],
    yes_book: Any,
    no_book: Any,
) -> tuple[Optional[EmpJPCandidate], str]:
    bp = current_bp(open_px, last_px)
    if bp is None:
        return None, "missing_spot_or_open"
    if elapsed_s < cfg.min_entry_elapsed_s:
        return None, "warming_up"
    if elapsed_s > cfg.max_entry_elapsed_s:
        return None, "entry_window_closed"
    if tte < cfg.min_tte_s or tte > cfg.max_tte_s:
        return None, "tte_out_of_band"

    bps: list[float] = []
    for _ts, px in spot_history:
        got_bp = current_bp(open_px, px)
        if got_bp is not None:
            bps.append(got_bp)
    path_max = max(bps) if bps else bp
    path_min = min(bps) if bps else bp
    sigma = rolling_sigma_bp_per_s(spot_history, calibration.sigma_default)
    emp_up, cell_n, cell_key = calibration.prob_up(tte=tte, current_bp=bp, sigma=sigma, path_max_bp=path_max, path_min_bp=path_min)
    if emp_up is None or cell_n < cfg.min_cell_n:
        return None, "no_calibration_cell"

    yes_ask, yes_ask_size = best_price_size(yes_book, "ask")
    no_ask, no_ask_size = best_price_size(no_book, "ask")
    yes_spread = spread(yes_book)
    no_spread = spread(no_book)
    yes_depth = depth_total(yes_book)
    no_depth = depth_total(no_book)

    candidates: list[tuple[float, str, float, float, Optional[float], float]] = []
    if yes_ask is not None:
        yes_edge = float(emp_up) - yes_ask - taker_fee_per_share(yes_ask, cfg.effective_fee_rate)
        candidates.append((yes_edge, "YES", float(emp_up), yes_ask, yes_spread, yes_depth))
    if no_ask is not None:
        no_prob = 1.0 - float(emp_up)
        no_edge = no_prob - no_ask - taker_fee_per_share(no_ask, cfg.effective_fee_rate)
        candidates.append((no_edge, "NO", no_prob, no_ask, no_spread, no_depth))
    if not candidates:
        return None, "no_complete_book"

    edge, side, prob, entry_price, lead_spread, lead_depth = max(candidates, key=lambda x: x[0])
    if not (cfg.min_ask <= entry_price <= cfg.max_ask):
        return None, "entry_price_out_of_band"
    if lead_spread is None or lead_spread > cfg.max_spread:
        return None, "spread_too_wide"
    if lead_depth < cfg.min_depth:
        return None, "depth_too_small"
    if edge < cfg.edge_min:
        return None, "edge_too_small"

    return EmpJPCandidate(
        side=side,
        side_prob=prob,
        entry_price=entry_price,
        edge=edge,
        cell_n=cell_n,
        cell_key="|".join(map(str, cell_key)),
        tte=float(tte),
        elapsed_s=int(elapsed_s),
        current_bp=float(bp),
        path_max_bp=float(path_max),
        path_min_bp=float(path_min),
        sigma=float(sigma),
        yes_ask=yes_ask,
        no_ask=no_ask,
        yes_spread=yes_spread,
        no_spread=no_spread,
        yes_depth=yes_depth,
        no_depth=no_depth,
    ), "pass"
