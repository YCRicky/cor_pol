"""Independent V9 dual-lane close classifier.

V9 is deliberately separate from :mod:`aftertake.post_close`'s V8
classifier.  It consumes the same immutable paired-book value objects, but it
has its own timing, dominance, executable-ask, and abort rules.  The runner
may select it only through an explicit feature flag; the normal default stays
V8.

The module is pure apart from its in-memory book deque.  Reservation, sizing,
and submission remain the caller's shared fail-closed path.  In particular,
an ``enter`` result is only a candidate for one market reservation; it is not
an order submission.
"""

from __future__ import annotations

import math
import os
import threading
from collections import deque
from dataclasses import dataclass, replace
from typing import Any, Deque, Dict, List, Optional, Tuple

from .post_close import PairedBook, PostCloseDecision, SideBook

V9_STRATEGY_VERSION = "aftertake_v9_dual_lane_shadow_099"
V9_ENTRY_REASON = "v9_dual_lane_marketable_099"
V9_CODE_SHA = os.getenv("AFTERTAKE_CODE_SHA", "unknown")


@dataclass(frozen=True)
class V9Config:
    """Versioned V9 thresholds; none are read from the live environment."""

    post_close_start_s: float = 0.0
    post_close_end_s: float = 0.250
    sweep_start_s: float = 0.050
    max_book_age_s: float = 0.150
    take_price: float = 0.99
    near_touch_band: float = 0.02
    # A residual taker may deliberately buy a still-cheap winner ask.  The
    # dominance gap, paired freshness, and reclaim abort—not an arbitrary .50
    # bid floor—provide the directional evidence for this independent lane.
    winner_bid_floor: float = 0.20
    residual_dominance_gap: float = 0.10
    sweep_dominance_gap: float = 0.15
    loser_reclaim_gap: float = 0.03
    residual_min_near_touch_qty_multiplier: float = 1.0
    sweep_min_near_touch_qty_multiplier: float = 1.0
    residual_max_confirmed_qty: float = 100.0
    residual_max_notional: float = 99.0
    sweep_confirmations: int = 2
    sweep_max_qty: float = 25.0
    sweep_max_notional: float = 24.75
    history_size: int = 4096
    strategy_version: str = V9_STRATEGY_VERSION
    entry_reason: str = V9_ENTRY_REASON

    # Compatibility attributes used by the runner status surface.  They are
    # descriptive only; V9 does not inherit V8 confirmation semantics.
    confirmations: int = 1
    confirmation_spacing_s: float = 0.0
    distinct_evidence_confirmations: bool = False
    require_fresh_paired_post_close: bool = True
    require_loser_refill_failure: bool = False
    require_stable_post_close_leader: bool = False


def active_v9_config() -> V9Config:
    """Return the immutable V9 profile used by an explicitly selected runner."""

    return V9Config()


def _finite(value: Optional[float]) -> bool:
    return value is not None and math.isfinite(float(value))


def _side_valid(side: SideBook) -> bool:
    prices = [side.best_bid, side.best_ask]
    if any(price is not None and (not _finite(price) or not 0 < float(price) < 1) for price in prices):
        return False
    if side.best_bid is not None and side.best_ask is not None and side.best_bid >= side.best_ask:
        return False
    if side.bid_size < 0 or side.bid_depth < 0 or side.ask_size < 0 or side.near_touch_bid_depth < 0:
        return False
    return all(
        _finite(price) and _finite(size) and 0 < float(price) <= 1 and float(size) > 0
        for price, size in side.ask_levels
    )


def _book_valid(book: PairedBook) -> bool:
    return _finite(book.observed_at) and _side_valid(book.yes) and _side_valid(book.no)


def _side(book: PairedBook, side: str) -> Tuple[SideBook, SideBook]:
    if side == "YES":
        return book.yes, book.no
    if side == "NO":
        return book.no, book.yes
    raise ValueError("side must be YES or NO")


class V9DualLaneClassifier:
    """Evaluate residual-taker and high-confidence sweep lanes on one feed."""

    def __init__(
        self,
        cfg: Optional[V9Config] = None,
        *,
        settlement_label: str = "unverified",
        code_sha: str = V9_CODE_SHA,
    ) -> None:
        self.cfg = cfg or active_v9_config()
        self.settlement_label = str(settlement_label or "unverified")
        self.code_sha = str(code_sha or "unknown")
        self._books: Deque[PairedBook] = deque(maxlen=self.cfg.history_size)
        self._lock = threading.Lock()

    def record(self, book: PairedBook) -> None:
        if not _book_valid(book):
            return
        with self._lock:
            if self._books and book.observed_at <= self._books[-1].observed_at:
                return
            self._books.append(book)

    def reset(self) -> None:
        """Drop a disconnected WebSocket generation before it can qualify."""

        with self._lock:
            self._books.clear()

    @staticmethod
    def _leader(book: PairedBook, dominance_gap: float) -> str:
        yes = book.yes.best_bid
        no = book.no.best_bid
        if yes is None and no is None:
            return ""
        if yes is None:
            return "NO"
        if no is None:
            return "YES"
        if float(yes) - float(no) >= dominance_gap:
            return "YES"
        if float(no) - float(yes) >= dominance_gap:
            return "NO"
        return ""

    @staticmethod
    def _levels_at_or_below(side: SideBook, ceiling: float) -> Tuple[float, Tuple[Tuple[float, float], ...]]:
        if not side.ask_levels:
            return 0.0, ()
        levels = tuple((float(price), float(size)) for price, size in side.ask_levels if price <= ceiling + 1e-9)
        return sum(size for _, size in levels), levels

    @staticmethod
    def _book_age_ms(book: Optional[PairedBook], now_ts: float) -> Dict[str, Optional[float]]:
        if book is None:
            return {"observed": None, "yes_updated": None, "no_updated": None}
        return {
            "observed": max(0.0, (float(now_ts) - float(book.observed_at)) * 1000.0),
            "yes_updated": (
                max(0.0, (float(now_ts) - float(book.yes_updated_at)) * 1000.0)
                if book.yes_updated_at is not None
                else None
            ),
            "no_updated": (
                max(0.0, (float(now_ts) - float(book.no_updated_at)) * 1000.0)
                if book.no_updated_at is not None
                else None
            ),
        }

    def _base_audit(
        self,
        *,
        lane: str,
        round_end_ts: float,
        now_ts: float,
        books: Tuple[PairedBook, ...],
        post_books: List[PairedBook],
        fresh_books: List[PairedBook],
    ) -> Dict[str, Any]:
        latest = fresh_books[-1] if fresh_books else (post_books[-1] if post_books else None)
        latest_yes = latest.yes if latest is not None else None
        latest_no = latest.no if latest is not None else None
        return {
            "lane": lane,
            "first_blocker": None,
            "event_ts": latest.source_timestamp if latest is not None else None,
            "receive_ts": latest.observed_at if latest is not None else None,
            "decision_ts": float(now_ts),
            "book_age_ms": self._book_age_ms(latest, now_ts),
            "observations_total": len(books),
            "postclose_count": len(post_books),
            "fresh_paired_postclose_count": len(fresh_books),
            "winner": {
                "best_bid": None,
                "bid_size": None,
                "bid_depth": None,
                "near_touch_depth": None,
                "best_ask": None,
                "ask_size": None,
            },
            "loser": {
                "best_bid": None,
                "bid_size": None,
                "bid_depth": None,
                "near_touch_depth": None,
                "best_ask": None,
                "ask_size": None,
            },
            "entry": {"take_price": self.cfg.take_price, "ask_size": 0.0, "levels": []},
            "would_enter": False,
            "code_sha": self.code_sha,
            "strategy_version": self.cfg.strategy_version,
            "settlement_label": self.settlement_label,
            "reservation_scope": "one_per_market_shared_risk",
            "post_order_allowed": False,
            "confirmation_policy": "v9_lane_specific_fresh_paired",
            "lane_thresholds": {
                "take_price": self.cfg.take_price,
                "max_book_age_s": self.cfg.max_book_age_s,
                "loser_reclaim_gap": self.cfg.loser_reclaim_gap,
            },
            "raw_top": {
                "yes": self._top_payload(latest_yes),
                "no": self._top_payload(latest_no),
            },
        }

    @staticmethod
    def _top_payload(side: Optional[SideBook]) -> Dict[str, Any]:
        if side is None:
            return {"best_bid": None, "bid_size": None, "near_touch_depth": None, "best_ask": None, "ask_size": None}
        return {
            "best_bid": side.best_bid,
            "bid_size": side.bid_size,
            "bid_depth": side.bid_depth,
            "near_touch_depth": side.near_touch_bid_depth,
            "best_ask": side.best_ask,
            "ask_size": side.ask_size,
            "ask_levels": list(side.ask_levels),
        }

    @staticmethod
    def _hold(
        lane: str,
        reason: str,
        audit: Dict[str, Any],
        *,
        side: str = "",
        confirmations: int = 0,
    ) -> PostCloseDecision:
        audit = dict(audit)
        audit["first_blocker"] = reason
        audit["would_enter"] = False
        audit["post_order_allowed"] = False
        audit["reject_reasons"] = [reason]
        audit["lane"] = lane
        return PostCloseDecision("hold", reason, side=side, confirmations=confirmations, audit=audit)

    @staticmethod
    def _enter(
        lane: str,
        side: str,
        qty: float,
        confirmations: int,
        audit: Dict[str, Any],
        ask_size: float,
        levels: Tuple[Tuple[float, float], ...],
    ) -> PostCloseDecision:
        audit = dict(audit)
        audit["lane"] = lane
        audit["first_blocker"] = None
        audit["would_enter"] = True
        audit["post_order_allowed"] = True
        audit["entry"] = {
            "take_price": audit["entry"]["take_price"],
            "ask_size": ask_size,
            "levels": list(levels),
        }
        return PostCloseDecision(
            "enter",
            V9_ENTRY_REASON,
            side=side,
            entry_ask=float(audit["entry"]["take_price"]),
            entry_ask_size=float(ask_size),
            winner_bid=audit["winner"]["best_bid"],
            loser_bid=audit["loser"]["best_bid"],
            confirmations=confirmations,
            audit=audit,
        )

    def _window_books(
        self,
        *,
        round_end_ts: float,
        now_ts: float,
        start_offset_s: float,
    ) -> Tuple[Tuple[PairedBook, ...], List[PairedBook], List[PairedBook], Optional[str]]:
        with self._lock:
            books = tuple(self._books)
        start = float(round_end_ts) + float(start_offset_s)
        end = float(round_end_ts) + float(self.cfg.post_close_end_s)
        if now_ts < start:
            return books, [], [], "window_not_open"
        if now_ts > end:
            return books, [], [], "window_expired"
        post = [book for book in books if start <= book.observed_at <= min(end, now_ts)]
        fresh = [
            book
            for book in post
            if float(book.yes_updated_at or 0.0) >= float(round_end_ts)
            and float(book.no_updated_at or 0.0) >= float(round_end_ts)
            and max(
                float(now_ts) - float(book.yes_updated_at or 0.0),
                float(now_ts) - float(book.no_updated_at or 0.0),
            ) <= self.cfg.max_book_age_s
        ]
        return books, post, fresh, None

    def _common_gate(
        self,
        *,
        lane: str,
        round_end_ts: float,
        now_ts: float,
        qty: float,
        start_offset_s: float,
        confirmations: int,
        dominance_gap: float,
        near_touch_multiplier: float,
        max_qty: float,
        max_notional: float,
        require_settlement_label: bool,
        stable_confirmations: bool,
    ) -> PostCloseDecision:
        books, post, fresh, window_reason = self._window_books(
            round_end_ts=round_end_ts, now_ts=now_ts, start_offset_s=start_offset_s
        )
        audit = self._base_audit(
            lane=lane,
            round_end_ts=round_end_ts,
            now_ts=now_ts,
            books=books,
            post_books=post,
            fresh_books=fresh,
        )
        audit["timing"] = {
            "post_start": float(round_end_ts) + float(start_offset_s),
            "post_end": float(round_end_ts) + float(self.cfg.post_close_end_s),
        }
        audit["requested_qty"] = float(qty)
        audit["confirmation_required"] = confirmations
        audit["dominance_gap_required"] = dominance_gap
        audit["settlement_label_required"] = require_settlement_label
        if window_reason == "window_not_open":
            return self._hold(lane, "%s_window_not_open" % lane.lower(), audit)
        if window_reason == "window_expired":
            return self._hold(lane, "%s_window_expired" % lane.lower(), audit)
        if qty <= 0:
            return self._hold(lane, "invalid_quantity", audit)
        if qty > max_qty:
            return self._hold(lane, "%s_quantity_cap" % lane.lower(), audit)
        if qty * self.cfg.take_price > max_notional + 1e-9:
            return self._hold(lane, "%s_notional_cap" % lane.lower(), audit)
        if require_settlement_label and self.settlement_label not in {"binary_up_down", "binary_yes_no"}:
            return self._hold(lane, "settlement_semantics_unverified", audit)
        if not post:
            return self._hold(lane, "%s_no_post_close_book" % lane.lower(), audit)
        if not fresh:
            return self._hold(lane, "%s_fresh_paired_missing" % lane.lower(), audit)
        if len(fresh) < confirmations:
            return self._hold(
                lane,
                "%s_confirmation_insufficient" % lane.lower(),
                audit,
                confirmations=len(fresh),
            )
        selected = fresh[-confirmations:]
        latest = selected[-1]
        audit["confirmation_timestamps"] = [book.observed_at for book in selected]
        audit["confirmation_spacing_s"] = [
            right.observed_at - left.observed_at
            for left, right in zip(selected, selected[1:])
        ]
        side = self._leader(latest, dominance_gap)
        if not side:
            return self._hold(lane, "%s_dominance_not_clear" % lane.lower(), audit, confirmations=len(selected))
        if stable_confirmations:
            leaders = [self._leader(book, dominance_gap) for book in selected]
            if any(leader != side for leader in leaders):
                return self._hold(lane, "%s_leader_not_stable" % lane.lower(), audit, side=side, confirmations=len(selected))
        for book in post:
            leader = self._leader(book, dominance_gap)
            if leader and leader != side:
                return self._hold(lane, "%s_dominance_reversed" % lane.lower(), audit, side=side, confirmations=len(selected))
            winner, loser = _side(book, side)
            if winner.best_bid is not None and loser.best_bid is not None:
                if float(loser.best_bid) > float(winner.best_bid) - self.cfg.loser_reclaim_gap:
                    return self._hold(lane, "%s_loser_reclaim_abort" % lane.lower(), audit, side=side, confirmations=len(selected))
        winner, loser = _side(latest, side)
        audit["candidate_side"] = side
        audit["winner"] = self._top_payload(winner)
        audit["loser"] = self._top_payload(loser)
        if winner.best_bid is None or winner.best_bid < self.cfg.winner_bid_floor:
            return self._hold(lane, "%s_winner_bid_floor" % lane.lower(), audit, side=side, confirmations=len(selected))
        if winner.bid_size < qty:
            return self._hold(lane, "%s_winner_bid_size" % lane.lower(), audit, side=side, confirmations=len(selected))
        if winner.near_touch_bid_depth < qty * near_touch_multiplier:
            return self._hold(lane, "%s_winner_depth" % lane.lower(), audit, side=side, confirmations=len(selected))
        if winner.best_ask is None or winner.best_ask > self.cfg.take_price + 1e-9:
            return self._hold(lane, "%s_ask_above_099" % lane.lower(), audit, side=side, confirmations=len(selected))
        ask_depth, levels = self._levels_at_or_below(winner, self.cfg.take_price)
        if not winner.ask_levels:
            return self._hold(lane, "%s_ask_levels_missing" % lane.lower(), audit, side=side, confirmations=len(selected))
        audit["entry"]["ask_size"] = ask_depth
        audit["entry"]["levels"] = list(levels)
        if ask_depth < qty:
            return self._hold(lane, "%s_executable_ask_depth" % lane.lower(), audit, side=side, confirmations=len(selected))
        return self._enter(lane, side, qty, len(selected), audit, ask_depth, levels)

    def evaluate_lanes(self, *, round_end_ts: float, now_ts: float, qty: float) -> Dict[str, PostCloseDecision]:
        residual = self._common_gate(
            lane="R",
            round_end_ts=round_end_ts,
            now_ts=now_ts,
            qty=qty,
            start_offset_s=self.cfg.post_close_start_s,
            confirmations=1,
            dominance_gap=self.cfg.residual_dominance_gap,
            near_touch_multiplier=self.cfg.residual_min_near_touch_qty_multiplier,
            max_qty=self.cfg.residual_max_confirmed_qty,
            max_notional=self.cfg.residual_max_notional,
            require_settlement_label=True,
            stable_confirmations=False,
        )
        sweep = self._common_gate(
            lane="S",
            round_end_ts=round_end_ts,
            now_ts=now_ts,
            qty=qty,
            start_offset_s=self.cfg.sweep_start_s,
            confirmations=self.cfg.sweep_confirmations,
            dominance_gap=self.cfg.sweep_dominance_gap,
            near_touch_multiplier=self.cfg.sweep_min_near_touch_qty_multiplier,
            max_qty=self.cfg.sweep_max_qty,
            max_notional=self.cfg.sweep_max_notional,
            require_settlement_label=True,
            stable_confirmations=True,
        )
        return {"R": residual, "S": sweep}

    def evaluate(
        self,
        *,
        round_end_ts: float,
        now_ts: float,
        qty: float,
        max_entry_ask: Optional[float] = None,
        min_near_touch_qty_multiplier: Optional[float] = None,
    ) -> PostCloseDecision:
        """Return one candidate/hold while retaining both lane outcomes in audit."""

        del max_entry_ask, min_near_touch_qty_multiplier
        results = self.evaluate_lanes(round_end_ts=round_end_ts, now_ts=now_ts, qty=qty)
        if results["R"].action == "enter":
            selected = results["R"]
        elif results["S"].action == "enter":
            selected = results["S"]
        else:
            selected = results["R"]
        combined = dict(selected.audit)
        combined["lane_results"] = {
            lane: {
                "action": decision.action,
                "reason": decision.reason,
                "side": decision.side,
                "confirmations": decision.confirmations,
                "would_enter": bool(decision.audit.get("would_enter")),
                "first_blocker": decision.audit.get("first_blocker"),
            }
            for lane, decision in results.items()
        }
        combined["lane_audits"] = {
            lane: dict(decision.audit) for lane, decision in results.items()
        }
        combined["first_blocker"] = None if selected.action == "enter" else selected.reason
        return replace(selected, audit=combined)
