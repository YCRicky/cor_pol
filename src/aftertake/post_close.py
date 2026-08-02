"""Event-driven post-close residual-liquidity classifiers.

This module deliberately has no HTTP, WebSocket, wallet, or notification
dependencies.  It accepts only timestamped, executable CLOB book observations
and decides whether the close-after book sequence has shown enough winner-side
bid support plus scored loser-side vacuum to produce a dry-run candidate.  A low
ask on its own is never a signal.
"""

from __future__ import annotations

import math
import threading
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, List, Optional, Tuple

V7_STRATEGY_VERSION = "aftertake_v7_event_driven_one_sided_vacuum"
V8_STRATEGY_VERSION = "aftertake_v8_1_stable_book_refill_guard_250ms"
ACTIVE_CLASSIFIER_FAMILY = "v8"
STRATEGY_VERSION = V8_STRATEGY_VERSION


@dataclass(frozen=True)
class SideBook:
    """Executable top-of-book state for one outcome token."""

    best_bid: Optional[float]
    bid_size: float
    bid_depth: float
    best_ask: Optional[float]
    ask_size: float
    near_touch_bid_depth: float = 0.0
    # V9 uses the executable ask ladder to model a marketable .99 ceiling.
    # Existing V7/V8 callers remain top-of-book compatible when this is empty.
    ask_levels: Tuple[Tuple[float, float], ...] = ()

    def __post_init__(self) -> None:
        # Backward-compatible test fixtures created before V6.4-strict did not
        # pass near-touch depth.  Treat the displayed best-bid size as the only
        # near-touch liquidity in that case, never the whole far-away book.
        if self.near_touch_bid_depth <= 0 and self.bid_size > 0:
            object.__setattr__(self, "near_touch_bid_depth", float(self.bid_size))


@dataclass(frozen=True)
class PairedBook:
    """A locally received paired YES/NO CLOB observation."""

    observed_at: float
    yes: SideBook
    no: SideBook
    source_timestamp: Optional[float] = None
    yes_updated_at: Optional[float] = None
    no_updated_at: Optional[float] = None

    def __post_init__(self) -> None:
        # Deterministic fixtures and single-book historical reconstructions
        # represent an atomic paired observation.  The live stream supplies
        # separate timestamps so V7 can reject half-fresh paired snapshots.
        if self.yes_updated_at is None:
            object.__setattr__(self, "yes_updated_at", float(self.observed_at))
        if self.no_updated_at is None:
            object.__setattr__(self, "no_updated_at", float(self.observed_at))


@dataclass(frozen=True)
class PostCloseConfig:
    """Fixed V7 event-driven timing and score thresholds.

    These are intentionally not environment switches. The thesis gates are
    versioned code so a dry-run cannot silently loosen into another strategy.
    """

    pre_close_window_s: float = 10.0
    pre_close_min_observations: int = 3
    pre_close_min_span_s: float = 0.100
    pre_close_latest_max_age_s: float = 0.250
    pre_close_bid_low: float = 0.20
    pre_close_bid_high: float = 0.80
    pre_close_max_edge: float = 0.12
    pre_close_max_bid_range: float = 0.08
    post_close_start_s: float = 0.050
    post_close_end_s: float = 1.000
    confirmations: int = 2
    confirmation_spacing_s: float = 0.0
    near_touch_band: float = 0.02
    min_winner_bid: float = 0.50
    min_near_touch_qty_multiplier: float = 2.0
    max_winner_bid_retrace: float = 0.02
    min_depth_retention: float = 0.80
    min_loser_bid_drop: float = 0.03
    max_loser_depth_retention: float = 0.50
    max_loser_recovery_ratio: float = 0.75
    no_reclaim_gap: float = 0.02
    max_ask_reprice: float = 0.01
    support_score_required: int = 5
    vacuum_score_required: int = 3
    history_size: int = 4096
    strategy_version: str = V7_STRATEGY_VERSION
    entry_reason: str = "v7_event_driven_one_sided_vacuum"
    allow_one_sided_loser_bid: bool = True
    distinct_evidence_confirmations: bool = True
    require_fresh_paired_post_close: bool = True
    require_loser_refill_failure: bool = False
    require_stable_post_close_leader: bool = False


def legacy_v67_config() -> PostCloseConfig:
    """Return the previous live classifier contract for controlled A/B replay."""

    return PostCloseConfig(
        confirmations=3,
        confirmation_spacing_s=0.100,
        strategy_version="aftertake_v6.7_start_50ms_spacing_100ms",
        entry_reason="v67_start_50ms_spacing_100ms_support_vacuum_score",
        allow_one_sided_loser_bid=False,
        distinct_evidence_confirmations=False,
        require_fresh_paired_post_close=False,
    )


def classifier_family_config(family: str) -> PostCloseConfig:
    """Return a validated classifier-family baseline for experiments."""

    if family == "v67":
        return legacy_v67_config()
    if family == "v7":
        return PostCloseConfig()
    if family == "v8":
        return PostCloseConfig(
            post_close_end_s=0.250,
            strategy_version=V8_STRATEGY_VERSION,
            entry_reason="v8_1_stable_book_refill_guard_250ms",
            # Two separately received, fresh paired books prove that support
            # remained present even when the executable top/depth values did
            # not move. Withdrawal, decay, leader reversal and loser refill
            # are still rejected by the gates below.
            distinct_evidence_confirmations=False,
            require_loser_refill_failure=True,
            require_stable_post_close_leader=True,
        )
    raise ValueError(f"unknown Aftertake classifier family: {family!r}")


def active_classifier_config() -> PostCloseConfig:
    """Return the immutable classifier profile selected for the live runner."""

    return classifier_family_config(ACTIVE_CLASSIFIER_FAMILY)


@dataclass(frozen=True)
class PostCloseDecision:
    action: str
    reason: str
    side: str = ""
    entry_ask: Optional[float] = None
    entry_ask_size: float = 0.0
    winner_bid: Optional[float] = None
    loser_bid: Optional[float] = None
    confirmations: int = 0
    audit: Dict[str, Any] = field(default_factory=dict)


class PostCloseWinnerClassifier:
    """Classify V7 winner-side support and scored loser vacuum after close.

    Pre-close history is only the low-vol / price-to-beat ambiguity scene gate.
    Direction is selected only from the post-close L2 repricing sequence.
    """

    def __init__(self, cfg: Optional[PostCloseConfig] = None):
        self.cfg = cfg or PostCloseConfig()
        self._books: Deque[PairedBook] = deque(maxlen=self.cfg.history_size)
        self._lock = threading.Lock()

    def record(self, book: PairedBook) -> None:
        with self._lock:
            if not self._book_valid(book):
                return
            if self._books and book.observed_at <= self._books[-1].observed_at:
                return
            self._books.append(book)

    def reset(self) -> None:
        """Discard observations from a dead WebSocket generation.

        A reconnect starts from a fresh paired snapshot. Keeping the previous
        generation in the deque would mix stale pre-close evidence with the
        new stream and could make a later book look like a valid confirmation.
        """

        with self._lock:
            self._books.clear()

    @staticmethod
    def _finite(value: Optional[float]) -> bool:
        return value is not None and math.isfinite(float(value))

    @classmethod
    def _side_valid(cls, side: SideBook) -> bool:
        prices = [side.best_bid, side.best_ask]
        if any(price is not None and (not cls._finite(price) or not 0 < float(price) < 1) for price in prices):
            return False
        if side.best_bid is not None and side.best_ask is not None and side.best_bid >= side.best_ask:
            return False
        return side.bid_size >= 0 and side.bid_depth >= 0 and side.ask_size >= 0 and side.near_touch_bid_depth >= 0

    @classmethod
    def _book_valid(cls, book: PairedBook) -> bool:
        return math.isfinite(float(book.observed_at)) and cls._side_valid(book.yes) and cls._side_valid(book.no)

    def _leader(self, book: PairedBook) -> str:
        yes_bid = book.yes.best_bid
        no_bid = book.no.best_bid
        if (
            not self.cfg.allow_one_sided_loser_bid
            and (yes_bid is None or no_bid is None)
        ):
            return ""
        if yes_bid is None and no_bid is None:
            return ""
        if yes_bid is None:
            return "NO"
        if no_bid is None:
            return "YES"
        if yes_bid == no_bid:
            return ""
        return "YES" if yes_bid > no_bid else "NO"

    @staticmethod
    def _side(book: PairedBook, side: str) -> Tuple[SideBook, SideBook]:
        if side == "YES":
            return book.yes, book.no
        if side == "NO":
            return book.no, book.yes
        raise ValueError("side must be YES or NO")

    @staticmethod
    def _series(books: List[PairedBook], side: str, attr: str) -> List[float]:
        values: List[float] = []
        for book in books:
            side_book, _ = PostCloseWinnerClassifier._side(book, side)
            value = getattr(side_book, attr)
            values.append(float(value) if value is not None else 0.0)
        return values

    @staticmethod
    def _evidence_signature(book: PairedBook) -> Tuple[Any, ...]:
        """Return only executable/top-depth fields that can change the thesis."""

        return (
            book.yes.best_bid,
            book.yes.bid_size,
            book.yes.near_touch_bid_depth,
            book.yes.best_ask,
            book.yes.ask_size,
            book.no.best_bid,
            book.no.bid_size,
            book.no.near_touch_bid_depth,
            book.no.best_ask,
            book.no.ask_size,
        )

    def _base_audit(self, round_end_ts: float, now_ts: float, books: Tuple[PairedBook, ...]) -> Dict[str, Any]:
        return {
            "strategy_version": self.cfg.strategy_version,
            "round_end_ts": float(round_end_ts),
            "now_ts": float(now_ts),
            "observations_total": len(books),
            "thresholds": {
                "pre_close_min_observations": self.cfg.pre_close_min_observations,
                "pre_close_max_edge": self.cfg.pre_close_max_edge,
                "pre_close_max_bid_range": self.cfg.pre_close_max_bid_range,
                "post_close_confirmations": self.cfg.confirmations,
                "near_touch_band": self.cfg.near_touch_band,
                "min_winner_bid": self.cfg.min_winner_bid,
                "support_score_required": self.cfg.support_score_required,
                "vacuum_score_required": self.cfg.vacuum_score_required,
            },
        }

    def _hold(self, reason: str, audit: Dict[str, Any], *, side: str = "", confirmations: int = 0) -> PostCloseDecision:
        reject_reasons = list(audit.get("reject_reasons") or [])
        if reason not in reject_reasons:
            reject_reasons.append(reason)
        audit = {**audit, "reject_reasons": reject_reasons}
        return PostCloseDecision("hold", reason, side=side, confirmations=confirmations, audit=audit)

    def _pre_close_scene(self, books: Tuple[PairedBook, ...], round_end_ts: float) -> Tuple[bool, Dict[str, Any], str, Optional[PairedBook]]:
        lower = float(round_end_ts) - self.cfg.pre_close_window_s
        pre = [book for book in books if lower <= book.observed_at < round_end_ts]
        audit: Dict[str, Any] = {"preclose_count": len(pre)}
        if len(pre) < self.cfg.pre_close_min_observations:
            return False, audit, "preclose_insufficient_observations", None
        latest = pre[-1]
        span = pre[-1].observed_at - pre[0].observed_at
        latest_age = float(round_end_ts) - latest.observed_at
        yes_bids = [float(book.yes.best_bid or 0.0) for book in pre]
        no_bids = [float(book.no.best_bid or 0.0) for book in pre]
        latest_edge = abs(float(latest.yes.best_bid or 0.0) - float(latest.no.best_bid or 0.0))
        latest_ambiguous = (
            latest.yes.best_bid is not None
            and latest.no.best_bid is not None
            and self.cfg.pre_close_bid_low <= latest.yes.best_bid <= self.cfg.pre_close_bid_high
            and self.cfg.pre_close_bid_low <= latest.no.best_bid <= self.cfg.pre_close_bid_high
            and latest_edge <= self.cfg.pre_close_max_edge
        )
        yes_range = max(yes_bids) - min(yes_bids)
        no_range = max(no_bids) - min(no_bids)
        low_vol = yes_range <= self.cfg.pre_close_max_bid_range and no_range <= self.cfg.pre_close_max_bid_range
        audit.update(
            {
                "preclose_span_s": span,
                "preclose_latest_age_s": latest_age,
                "preclose_yes_bids": yes_bids[-5:],
                "preclose_no_bids": no_bids[-5:],
                "preclose_latest_edge": latest_edge,
                "preclose_yes_range": yes_range,
                "preclose_no_range": no_range,
                "preclose_latest_ambiguous": latest_ambiguous,
                "preclose_low_vol": low_vol,
            }
        )
        warnings = []
        if span < self.cfg.pre_close_min_span_s:
            warnings.append("preclose_span_too_short")
        if latest_age > self.cfg.pre_close_latest_max_age_s:
            warnings.append("preclose_latest_book_stale")
        if not latest_ambiguous:
            warnings.append("preclose_price_ambiguous_failed")
        if not low_vol:
            warnings.append("preclose_bid_volatility_excessive")
        audit["preclose_scene_warnings"] = warnings
        audit["preclose_scene_gate"] = "audit_only"
        if latest_ambiguous and low_vol:
            audit["preclose_scene_label"] = "contested_low_vol"
        elif latest_ambiguous:
            audit["preclose_scene_label"] = "contested_volatile"
        elif low_vol:
            audit["preclose_scene_label"] = "directional_low_vol"
        else:
            audit["preclose_scene_label"] = "directional_volatile"
        return True, audit, "", latest

    def evaluate(
        self,
        *,
        round_end_ts: float,
        now_ts: float,
        qty: float,
        max_entry_ask: Optional[float] = None,
        min_near_touch_qty_multiplier: Optional[float] = None,
    ) -> PostCloseDecision:
        if qty <= 0:
            raise ValueError("qty must be > 0")
        # Kept as an internal compatibility argument for historical replay
        # callers. V7 has no blind entry-price environment cap.
        del max_entry_ask
        start = float(round_end_ts) + self.cfg.post_close_start_s
        end = float(round_end_ts) + self.cfg.post_close_end_s
        with self._lock:
            books = tuple(self._books)
        audit = self._base_audit(round_end_ts, now_ts, books)
        audit["timing"] = {"post_start": start, "post_end": end}
        audit["confirmation_policy"] = (
            "distinct_evidence_states"
            if self.cfg.distinct_evidence_confirmations
            else "fresh_paired_observations"
        )
        if now_ts < start:
            return self._hold("post_close_window_not_open", audit)
        if now_ts > end:
            return self._hold("post_close_window_expired", audit)

        scene_ok, scene_audit, scene_reason, pre_latest = self._pre_close_scene(books, round_end_ts)
        audit.update(scene_audit)
        if not scene_ok or pre_latest is None:
            return self._hold(scene_reason or "preclose_scene_failed", audit)

        post_all = [book for book in books if start <= book.observed_at <= min(end, now_ts)]
        post = (
            [
                book
                for book in post_all
                if float(book.yes_updated_at or 0.0) >= float(round_end_ts)
                and float(book.no_updated_at or 0.0) >= float(round_end_ts)
            ]
            if self.cfg.require_fresh_paired_post_close
            else post_all
        )
        audit["postclose_count"] = len(post_all)
        audit["fresh_paired_postclose_count"] = len(post)
        if post_all and not post:
            return self._hold("paired_post_close_state_not_fresh", audit)
        if len(post) < self.cfg.confirmations:
            return self._hold("insufficient_post_close_observations", audit, confirmations=len(post))
        # Websocket updates can arrive many times inside a few milliseconds.
        # Walk backward from the latest state and count only evidence-changing
        # executable states. Legacy replay can additionally require wall-clock
        # spacing through its explicit compatibility config.
        confirmed_desc = [post[-1]]
        anchor_ts = post[-1].observed_at
        anchor_signature = self._evidence_signature(post[-1])
        for book in reversed(post[:-1]):
            signature = self._evidence_signature(book)
            if (
                anchor_ts - book.observed_at >= self.cfg.confirmation_spacing_s
                and (
                    not self.cfg.distinct_evidence_confirmations
                    or signature != anchor_signature
                )
            ):
                confirmed_desc.append(book)
                anchor_ts = book.observed_at
                anchor_signature = signature
                if len(confirmed_desc) == self.cfg.confirmations:
                    break
        confirmed = list(reversed(confirmed_desc))
        spacing = [right.observed_at - left.observed_at for left, right in zip(confirmed, confirmed[1:])]
        audit["confirmation_timestamps"] = [book.observed_at for book in confirmed]
        audit["confirmation_spacing_s"] = spacing
        if len(confirmed) < self.cfg.confirmations:
            return self._hold("bid_support_not_yet_persistent", audit, confirmations=len(confirmed))

        leaders = [self._leader(book) for book in confirmed]
        audit["postclose_leaders"] = leaders
        if not leaders[-1] or any(leader != leaders[-1] for leader in leaders):
            return self._hold("bid_support_not_persistent", audit, confirmations=len(confirmed))

        side = leaders[-1]
        leader_path = []
        for book in post:
            leader = self._leader(book)
            if leader:
                leader_path.append(leader)
        audit["postclose_leader_path"] = leader_path
        if (
            self.cfg.require_stable_post_close_leader
            and leader_path
            and any(leader != leader_path[0] for leader in leader_path)
        ):
            return self._hold(
                "post_close_leader_reversed",
                audit,
                side=side,
                confirmations=len(confirmed),
            )
        opposite = "NO" if side == "YES" else "YES"
        latest = confirmed[-1]
        winner, loser = self._side(latest, side)
        pre_winner, pre_loser = self._side(pre_latest, side)
        audit["candidate_side"] = side
        audit["winner_bid_series"] = self._series(confirmed, side, "best_bid")
        audit["loser_bid_series"] = self._series(confirmed, opposite, "best_bid")
        audit["winner_near_touch_depth_series"] = self._series(confirmed, side, "near_touch_bid_depth")
        audit["loser_near_touch_depth_series"] = self._series(confirmed, opposite, "near_touch_bid_depth")
        audit["winner_ask_series"] = self._series(confirmed, side, "best_ask")

        if winner.best_bid is None:
            return self._hold("winner_post_close_bid_missing", audit, side=side, confirmations=len(confirmed))
        if winner.best_ask is None:
            return self._hold("winner_residual_ask_missing", audit, side=side, confirmations=len(confirmed))
        if winner.best_bid >= winner.best_ask:
            return self._hold("winner_book_locked_or_crossed", audit, side=side, confirmations=len(confirmed))

        winner_bids = [float(self._side(book, side)[0].best_bid or 0.0) for book in confirmed]
        winner_depths = [float(self._side(book, side)[0].near_touch_bid_depth) for book in confirmed]
        winner_sizes = [float(self._side(book, side)[0].bid_size) for book in confirmed]
        loser_bids = [float(self._side(book, side)[1].best_bid or 0.0) for book in confirmed]
        loser_depths = [float(self._side(book, side)[1].near_touch_bid_depth) for book in confirmed]
        ask_series = [float(self._side(book, side)[0].best_ask or 0.0) for book in confirmed]

        near_touch_multiplier = (
            float(min_near_touch_qty_multiplier)
            if min_near_touch_qty_multiplier is not None
            else self.cfg.min_near_touch_qty_multiplier
        )
        min_near_depth = qty * near_touch_multiplier
        winner_bid_floor = min(winner_bids) >= self.cfg.min_winner_bid
        winner_best_size_ok = min(winner_sizes) >= qty
        winner_near_depth_ok = min(winner_depths) >= min_near_depth
        winner_non_decay = winner_bids[-1] >= max(winner_bids) - self.cfg.max_winner_bid_retrace
        depth_non_decay = winner_depths[-1] >= winner_depths[0] * self.cfg.min_depth_retention or winner_depths[-1] >= winner_depths[0] + qty
        support_components = {
            "winner_bid_floor": winner_bid_floor,
            "winner_best_size_ok": winner_best_size_ok,
            "winner_near_touch_depth_ok": winner_near_depth_ok,
            "winner_bid_non_decay": winner_non_decay,
            "winner_depth_non_decay_or_refill": depth_non_decay,
        }
        support_score = sum(1 for ok in support_components.values() if ok)
        audit["support_components"] = support_components
        audit["support_score"] = support_score
        audit["support_required"] = self.cfg.support_score_required
        audit["min_near_touch_qty_multiplier"] = near_touch_multiplier
        audit["min_near_touch_depth_required"] = min_near_depth
        if not winner_near_depth_ok:
            return self._hold("winner_near_touch_depth_too_thin", audit, side=side, confirmations=len(confirmed))
        for reason, ok in (
            ("winner_bid_floor_too_low", winner_bid_floor),
            ("winner_bid_support_too_thin", winner_best_size_ok),
            ("winner_near_touch_depth_too_thin", winner_near_depth_ok),
            ("winner_bid_decayed", winner_non_decay),
            ("winner_depth_decayed", depth_non_decay),
        ):
            if not ok:
                return self._hold(reason, audit, side=side, confirmations=len(confirmed))

        pre_loser_bid = float(pre_loser.best_bid or 0.0)
        pre_loser_depth = float(pre_loser.near_touch_bid_depth or pre_loser.bid_size or 0.0)
        loser_bid_drop = pre_loser_bid - loser_bids[-1]
        loser_depth_retention = (loser_depths[-1] / pre_loser_depth) if pre_loser_depth > 0 else 0.0
        loser_bid_missing = loser.best_bid is None
        loser_bid_drop_ok = loser_bid_missing or loser_bid_drop >= self.cfg.min_loser_bid_drop
        loser_depth_decay_ok = pre_loser_depth <= 0 or loser_depth_retention <= self.cfg.max_loser_depth_retention
        loser_refill_failure_ok = max(loser_depths) <= max(qty, pre_loser_depth * self.cfg.max_loser_recovery_ratio)
        loser_no_reclaim_ok = all(lb <= wb - self.cfg.no_reclaim_gap for lb, wb in zip(loser_bids, winner_bids))
        vacuum_components = {
            "loser_bid_drop_ok": loser_bid_drop_ok,
            "loser_depth_decay_ok": loser_depth_decay_ok,
            "loser_refill_failure_ok": loser_refill_failure_ok,
            "loser_no_reclaim_ok": loser_no_reclaim_ok,
        }
        vacuum_score = sum(1 for ok in vacuum_components.values() if ok)
        audit["vacuum_components"] = vacuum_components
        audit["vacuum_score"] = vacuum_score
        audit["vacuum_required"] = self.cfg.vacuum_score_required
        audit["loser_bid_missing"] = loser_bid_missing
        audit["loser_bid_drop"] = loser_bid_drop
        audit["loser_depth_retention"] = loser_depth_retention
        vacuum_reject_components = [
            reason
            for reason, ok in (
                ("loser_bid_drop_insufficient", loser_bid_drop_ok),
                ("loser_bid_depth_decay_insufficient", loser_depth_decay_ok),
                ("loser_bid_refilled", loser_refill_failure_ok),
                ("loser_reclaimed_bid", loser_no_reclaim_ok),
            )
            if not ok
        ]
        audit["vacuum_reject_components"] = vacuum_reject_components
        if self.cfg.require_loser_refill_failure and not loser_refill_failure_ok:
            return self._hold(
                "loser_bid_refilled",
                audit,
                side=side,
                confirmations=len(confirmed),
            )
        if vacuum_score < self.cfg.vacuum_score_required:
            reason = vacuum_reject_components[0] if vacuum_reject_components else "loser_vacuum_score_insufficient"
            return self._hold(reason, audit, side=side, confirmations=len(confirmed))

        ask_reprice = max(ask_series) - min(ask_series)
        audit["ask_lag"] = {
            "entry_ask": winner.best_ask,
            "entry_ask_size": winner.ask_size,
            "ask_reprice": ask_reprice,
            "entry_price_cap": "disabled",
        }
        if winner.ask_size < qty:
            return PostCloseDecision(
                "hold",
                "winner_residual_ask_too_thin",
                side=side,
                entry_ask=winner.best_ask,
                entry_ask_size=winner.ask_size,
                winner_bid=winner.best_bid,
                loser_bid=loser.best_bid,
                confirmations=len(confirmed),
                audit={**audit, "reject_reasons": ["winner_residual_ask_too_thin"]},
            )
        audit["ask_reprice_observed"] = ask_reprice

        audit["reject_reasons"] = []
        return PostCloseDecision(
            "enter",
            self.cfg.entry_reason,
            side=side,
            entry_ask=winner.best_ask,
            entry_ask_size=winner.ask_size,
            winner_bid=winner.best_bid,
            loser_bid=loser.best_bid,
            confirmations=len(confirmed),
            audit=audit,
        )
