"""Fail-closed close+500ms signal freeze for the live Aftertake entry path.

The live strategy makes one causal decision at ``round_end + 0.5s`` from the
latest locally received paired YES/NO snapshot.  This module deliberately has
no network, account, state, or execution dependencies; the runner owns the
schedule and durable order lifecycle.

``source_timestamp`` is carried by :class:`PairedBook` for observability only.
Freshness here is exclusively local receive age, so a missing or skewed venue
timestamp cannot turn a locally fresh paired book into a false rejection.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional

from .post_close import PairedBook, PostCloseDecision, SideBook

POST_CLOSE_SNAPSHOT_STRATEGY_VERSION = "aftertake_postclose_snapshot_v1_plus0_5_leader_bid_gt_080"
POST_CLOSE_SNAPSHOT_LIMIT_PRICE = 0.99
POST_CLOSE_SNAPSHOT_DELAY_S = 0.5
POST_CLOSE_SNAPSHOT_PAIRED_MAX_AGE_S = 0.250
POST_CLOSE_SNAPSHOT_MAX_LATENESS_S = 0.250


@dataclass(frozen=True)
class PostCloseSnapshotConfig:
    """Small, explicit close+500ms timing and eligibility contract."""

    snapshot_delay_s: float = POST_CLOSE_SNAPSHOT_DELAY_S
    leader_bid_threshold: float = 0.80
    paired_max_age_s: float = POST_CLOSE_SNAPSHOT_PAIRED_MAX_AGE_S
    max_decision_lateness_s: float = POST_CLOSE_SNAPSHOT_MAX_LATENESS_S
    limit_price: float = POST_CLOSE_SNAPSHOT_LIMIT_PRICE
    strategy_version: str = POST_CLOSE_SNAPSHOT_STRATEGY_VERSION

    def validate(self) -> None:
        if self.snapshot_delay_s <= 0:
            raise ValueError("post-close snapshot delay must be > 0")
        if not 0 < self.leader_bid_threshold < 1:
            raise ValueError("post-close leader bid threshold must be in (0, 1)")
        if not 0 < self.paired_max_age_s <= 1:
            raise ValueError("post-close paired freshness must be in (0, 1]")
        if not 0 <= self.max_decision_lateness_s <= 1:
            raise ValueError("post-close maximum decision lateness must be in [0, 1]")
        if not 0 < self.limit_price < 1:
            raise ValueError("post-close limit price must be in (0, 1)")


def active_post_close_snapshot_config() -> PostCloseSnapshotConfig:
    config = PostCloseSnapshotConfig()
    config.validate()
    return config


def _valid_bid(value: Optional[float]) -> bool:
    if value is None:
        return False
    try:
        return math.isfinite(float(value)) and 0 < float(value) < 1
    except (TypeError, ValueError):
        return False


def _side_payload(side: SideBook) -> Dict[str, Any]:
    return {
        "best_bid": side.best_bid,
        "bid_size": float(side.bid_size),
        "bid_depth": float(side.bid_depth),
        "best_ask": side.best_ask,
        "ask_size": float(side.ask_size),
    }


def select_post_close_snapshot_signal(
    observations: Iterable[PairedBook],
    *,
    round_end_ts: float,
    decision_ts: Optional[float] = None,
    config: Optional[PostCloseSnapshotConfig] = None,
) -> PostCloseDecision:
    """Select one signal from observations available at the close+500ms boundary.

    The normal decision timestamp is exactly ``round_end + 0.5s``.  A delayed
    scheduler may pass its actual decision time through ``decision_ts`` while
    remaining inside the fixed lateness cutoff.  Observations received after
    that actual decision time are never considered.  A HOLD is returned for missing,
    invalid, stale, or tied paired bids.  On ENTER, ``entry_ask`` is always the
    aggressive ``.99`` ceiling, not the observed ask or an expected fill price.
    """

    cfg = config or active_post_close_snapshot_config()
    cfg.validate()
    close_ts = float(round_end_ts)
    scheduled_snapshot_ts = close_ts + float(cfg.snapshot_delay_s)
    effective_decision_ts = (
        scheduled_snapshot_ts if decision_ts is None else float(decision_ts)
    )
    base_audit: Dict[str, Any] = {
        "strategy_version": cfg.strategy_version,
        "post_close_snapshot_ts": scheduled_snapshot_ts,
        "decision_ts": effective_decision_ts,
        "close_ts": close_ts,
        "snapshot_delay_s": cfg.snapshot_delay_s,
        "leader_bid_threshold": cfg.leader_bid_threshold,
        "paired_max_age_s": cfg.paired_max_age_s,
        "limit_price": cfg.limit_price,
        "decision_cutoff_ts": scheduled_snapshot_ts + float(cfg.max_decision_lateness_s),
        "source_timestamp_used_for_gate": False,
    }
    if effective_decision_ts < scheduled_snapshot_ts:
        return PostCloseDecision(
            "hold",
            "post_close_snapshot_not_due",
            audit=base_audit,
        )
    if effective_decision_ts > scheduled_snapshot_ts + float(cfg.max_decision_lateness_s):
        return PostCloseDecision(
            "hold",
            "post_close_snapshot_decision_too_late",
            audit=base_audit,
        )

    candidates = []
    for book in observations:
        try:
            observed_at = float(book.observed_at)
        except (TypeError, ValueError):
            continue
        if math.isfinite(observed_at) and observed_at <= effective_decision_ts:
            candidates.append(book)
    base_audit["observation_count_at_decision"] = len(candidates)
    if not candidates:
        return PostCloseDecision(
            "hold",
            "post_close_snapshot_no_paired_observation",
            audit=base_audit,
        )

    book = max(candidates, key=lambda item: float(item.observed_at))
    observed_at = float(book.observed_at)
    snapshot_age = effective_decision_ts - observed_at
    audit = {
        **base_audit,
        "snapshot_observed_ts": observed_at,
        "snapshot_age_ms": max(0.0, snapshot_age * 1000.0),
        "source_timestamp": book.source_timestamp,
        "yes": _side_payload(book.yes),
        "no": _side_payload(book.no),
    }
    if snapshot_age < 0 or snapshot_age > cfg.paired_max_age_s:
        return PostCloseDecision("hold", "post_close_snapshot_stale", audit=audit)

    yes_bid = book.yes.best_bid
    no_bid = book.no.best_bid
    if not (_valid_bid(yes_bid) and _valid_bid(no_bid)):
        return PostCloseDecision("hold", "post_close_snapshot_missing_or_invalid_bid", audit=audit)
    if float(yes_bid) == float(no_bid):
        return PostCloseDecision("hold", "post_close_snapshot_bid_tie", audit=audit)

    side = "YES" if float(yes_bid) > float(no_bid) else "NO"
    winner = book.yes if side == "YES" else book.no
    loser = book.no if side == "YES" else book.yes
    winner_bid = float(winner.best_bid)
    audit.update(
        {
            "selected_side": side,
            "selected_token": side,
            "winner_best_bid": winner_bid,
            "loser_best_bid": float(loser.best_bid),
            "winner_best_ask": winner.best_ask,
            "winner_ask_size": float(winner.ask_size),
        }
    )
    if winner_bid <= cfg.leader_bid_threshold:
        return PostCloseDecision(
            "hold",
            "post_close_leader_bid_not_strictly_above_threshold",
            side=side,
            winner_bid=winner_bid,
            loser_bid=float(loser.best_bid),
            audit=audit,
        )

    return PostCloseDecision(
        "enter",
        "post_close_leader_bid_strictly_above_threshold",
        side=side,
        entry_ask=cfg.limit_price,
        entry_ask_size=float(winner.ask_size),
        winner_bid=winner_bid,
        loser_bid=float(loser.best_bid),
        audit=audit,
    )
