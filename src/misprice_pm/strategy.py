from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class MispriceConfig:
    """Misprice v3 thesis model parameters.

    The detector is not a cheap-ask stopgap.  It explicitly models Polymarket
    repricing lag after a BTC 5m path transition:

    required_reprice = min(0.40, reprice_per_bp * abs(transition_bp))
    actual_reprice = signal_ask - pre_ask
    lag_depth = required_reprice - actual_reprice

    Enter only when the lag is still large enough and executable.
    """

    lookback_s: int = 15
    min_transition_bp: float = 3.0
    max_pre_abs_bp: float = 2.5
    min_abs_bp: float = 3.5
    reprice_per_bp: float = 0.04
    min_lag_depth: float = 0.035
    min_elapsed_s: int = 20
    max_elapsed_s: int = 220
    ban_elapsed_start_s: int = -1
    ban_elapsed_end_s: int = -1
    min_entry_ask: float = 0.35
    max_entry_ask: float = 0.65
    max_spread: float = 0.05
    min_depth: float = 5.0
    max_book_age_s: float = 2.0


@dataclass(frozen=True)
class BookSnapshot:
    yes_bid: Optional[float]
    yes_ask: Optional[float]
    yes_ask_size: float
    no_bid: Optional[float]
    no_ask: Optional[float]
    no_ask_size: float
    age_s: float = 0.0


@dataclass(frozen=True)
class Decision:
    action: str
    reason: str
    side: str = ""
    slug: str = ""
    entry_ask: Optional[float] = None
    elapsed_s: int = 0
    tte_s: float = 0.0
    pre_bp: Optional[float] = None
    signal_bp: Optional[float] = None
    transition_bp: Optional[float] = None
    pre_ask: Optional[float] = None
    signal_ask: Optional[float] = None
    required_reprice: Optional[float] = None
    actual_reprice: Optional[float] = None
    lag_depth: Optional[float] = None
    spread: Optional[float] = None
    depth: Optional[float] = None


class StrategyState:
    def __init__(self, *, open_price: float):
        self.open_price = float(open_price)
        self.spot_points: deque[tuple[float, float]] = deque(maxlen=4096)
        self.book_points: deque[tuple[float, BookSnapshot]] = deque(maxlen=4096)

    def record_spot(self, *, ts: float, price: float) -> None:
        timestamp = float(ts)
        # The runner only calls this with a source observation made at this
        # timestamp.  Do not let a delayed response rewrite the rolling path.
        if self.spot_points and timestamp < self.spot_points[-1][0]:
            return
        self.spot_points.append((timestamp, float(price)))

    def spot_at_or_before(self, ts: float) -> Optional[float]:
        result = None
        for point_ts, price in self.spot_points:
            if point_ts <= ts:
                result = price
            else:
                break
        return result

    def record_book(self, *, ts: float, book: BookSnapshot) -> None:
        timestamp = float(ts)
        if self.book_points and timestamp < self.book_points[-1][0]:
            return
        self.book_points.append((timestamp, book))

    def book_at_or_before(self, ts: float) -> Optional[BookSnapshot]:
        result = None
        for point_ts, book in self.book_points:
            if point_ts <= ts:
                result = book
            else:
                break
        return result


def bp(open_price: float, price: float) -> float:
    return (float(price) / float(open_price) - 1.0) * 10000.0


def side_from_transition(transition_bp: float) -> str:
    return "YES" if transition_bp > 0 else "NO"


def entry_terms(book: BookSnapshot, side: str) -> tuple[Optional[float], float, float]:
    if side == "YES":
        if book.yes_bid is None or book.yes_ask is None:
            return None, 999.0, 0.0
        return book.yes_ask, book.yes_ask - book.yes_bid, book.yes_ask_size
    if book.no_bid is None or book.no_ask is None:
        return None, 999.0, 0.0
    return book.no_ask, book.no_ask - book.no_bid, book.no_ask_size


def required_reprice(cfg: MispriceConfig, transition_bp: float) -> float:
    return min(0.40, cfg.reprice_per_bp * abs(float(transition_bp)))


def lag_depth_for(*, cfg: MispriceConfig, transition_bp: float, pre_ask: float, signal_ask: float) -> tuple[float, float, float]:
    required = required_reprice(cfg, transition_bp)
    actual = float(signal_ask) - float(pre_ask)
    lag_depth = required - actual
    return required, actual, lag_depth


def _base_hold(
    reason: str,
    *,
    slug: str,
    elapsed_s: int,
    tte_s: float,
    side: str = "",
    entry_ask: Optional[float] = None,
    pre_bp: Optional[float] = None,
    signal_bp: Optional[float] = None,
    transition_bp: Optional[float] = None,
    pre_ask: Optional[float] = None,
    signal_ask: Optional[float] = None,
    required: Optional[float] = None,
    actual: Optional[float] = None,
    lag_depth: Optional[float] = None,
    spread: Optional[float] = None,
    depth: Optional[float] = None,
) -> Decision:
    return Decision(
        "hold",
        reason,
        side=side,
        slug=slug,
        entry_ask=entry_ask,
        elapsed_s=elapsed_s,
        tte_s=tte_s,
        pre_bp=pre_bp,
        signal_bp=signal_bp,
        transition_bp=transition_bp,
        pre_ask=pre_ask,
        signal_ask=signal_ask,
        required_reprice=required,
        actual_reprice=actual,
        lag_depth=lag_depth,
        spread=spread,
        depth=depth,
    )


def evaluate_tick(
    *,
    cfg: MispriceConfig,
    state: StrategyState,
    now_ts: float,
    round_start_ts: int,
    round_end_ts: int,
    slug: str,
    spot_price: float,
    book: BookSnapshot,
) -> Decision:
    elapsed_s = int(now_ts - round_start_ts)
    tte_s = max(0.0, float(round_end_ts) - float(now_ts))
    target_ts = float(now_ts) - float(cfg.lookback_s)
    pre_book = state.book_at_or_before(target_ts)
    state.record_spot(ts=now_ts, price=spot_price)
    state.record_book(ts=now_ts, book=book)

    if elapsed_s < cfg.min_elapsed_s or elapsed_s > cfg.max_elapsed_s:
        return _base_hold("elapsed_out_of_regime", slug=slug, elapsed_s=elapsed_s, tte_s=tte_s)
    if cfg.ban_elapsed_start_s <= elapsed_s < cfg.ban_elapsed_end_s:
        return _base_hold("elapsed_banned_mid_window", slug=slug, elapsed_s=elapsed_s, tte_s=tte_s)
    if book.age_s > cfg.max_book_age_s:
        return _base_hold("book_stale", slug=slug, elapsed_s=elapsed_s, tte_s=tte_s)

    # These must be genuinely captured lookback observations.  The runner never
    # fabricates them from the round open or process-start price.
    pre_price = state.spot_at_or_before(target_ts)
    if pre_price is None:
        return _base_hold("insufficient_lookback", slug=slug, elapsed_s=elapsed_s, tte_s=tte_s)
    if pre_book is None:
        return _base_hold("insufficient_book_lookback", slug=slug, elapsed_s=elapsed_s, tte_s=tte_s)

    pre_bp = bp(state.open_price, pre_price)
    signal_bp = bp(state.open_price, spot_price)
    transition_bp = signal_bp - pre_bp
    if abs(pre_bp) > cfg.max_pre_abs_bp:
        return _base_hold(
            "pre_path_too_directional",
            slug=slug,
            elapsed_s=elapsed_s,
            tte_s=tte_s,
            pre_bp=pre_bp,
            signal_bp=signal_bp,
            transition_bp=transition_bp,
        )
    if abs(signal_bp) < cfg.min_abs_bp or abs(transition_bp) < cfg.min_transition_bp:
        return _base_hold(
            "no_path_transition",
            slug=slug,
            elapsed_s=elapsed_s,
            tte_s=tte_s,
            pre_bp=pre_bp,
            signal_bp=signal_bp,
            transition_bp=transition_bp,
        )

    side = side_from_transition(transition_bp)
    pre_ask, _pre_spread, _pre_depth = entry_terms(pre_book, side)
    if pre_ask is None:
        return _base_hold(
            "no_pre_ask",
            side=side,
            slug=slug,
            elapsed_s=elapsed_s,
            tte_s=tte_s,
            pre_bp=pre_bp,
            signal_bp=signal_bp,
            transition_bp=transition_bp,
        )

    entry_ask, spread, depth = entry_terms(book, side)
    if entry_ask is None:
        return _base_hold(
            "no_entry_ask",
            side=side,
            slug=slug,
            elapsed_s=elapsed_s,
            tte_s=tte_s,
            pre_bp=pre_bp,
            signal_bp=signal_bp,
            transition_bp=transition_bp,
            pre_ask=pre_ask,
        )

    required, actual, lag_depth = lag_depth_for(
        cfg=cfg, transition_bp=transition_bp, pre_ask=pre_ask, signal_ask=entry_ask
    )
    if lag_depth < cfg.min_lag_depth:
        return _base_hold(
            "repricing_lag_too_small",
            side=side,
            slug=slug,
            entry_ask=entry_ask,
            elapsed_s=elapsed_s,
            tte_s=tte_s,
            pre_bp=pre_bp,
            signal_bp=signal_bp,
            transition_bp=transition_bp,
            pre_ask=pre_ask,
            signal_ask=entry_ask,
            required=required,
            actual=actual,
            lag_depth=lag_depth,
            spread=spread,
            depth=depth,
        )
    if not (cfg.min_entry_ask <= entry_ask <= cfg.max_entry_ask):
        return _base_hold(
            "entry_ask_out_of_range",
            side=side,
            slug=slug,
            entry_ask=entry_ask,
            elapsed_s=elapsed_s,
            tte_s=tte_s,
            pre_bp=pre_bp,
            signal_bp=signal_bp,
            transition_bp=transition_bp,
            pre_ask=pre_ask,
            signal_ask=entry_ask,
            required=required,
            actual=actual,
            lag_depth=lag_depth,
            spread=spread,
            depth=depth,
        )
    if spread > cfg.max_spread:
        return _base_hold(
            "spread_too_wide",
            side=side,
            slug=slug,
            entry_ask=entry_ask,
            elapsed_s=elapsed_s,
            tte_s=tte_s,
            pre_bp=pre_bp,
            signal_bp=signal_bp,
            transition_bp=transition_bp,
            pre_ask=pre_ask,
            signal_ask=entry_ask,
            required=required,
            actual=actual,
            lag_depth=lag_depth,
            spread=spread,
            depth=depth,
        )
    if depth < cfg.min_depth:
        return _base_hold(
            "depth_too_low",
            side=side,
            slug=slug,
            entry_ask=entry_ask,
            elapsed_s=elapsed_s,
            tte_s=tte_s,
            pre_bp=pre_bp,
            signal_bp=signal_bp,
            transition_bp=transition_bp,
            pre_ask=pre_ask,
            signal_ask=entry_ask,
            required=required,
            actual=actual,
            lag_depth=lag_depth,
            spread=spread,
            depth=depth,
        )

    return Decision(
        "enter",
        "repricing_lag_underreaction",
        side=side,
        slug=slug,
        entry_ask=entry_ask,
        elapsed_s=elapsed_s,
        tte_s=tte_s,
        pre_bp=pre_bp,
        signal_bp=signal_bp,
        transition_bp=transition_bp,
        pre_ask=pre_ask,
        signal_ask=entry_ask,
        required_reprice=required,
        actual_reprice=actual,
        lag_depth=lag_depth,
        spread=spread,
        depth=depth,
    )
