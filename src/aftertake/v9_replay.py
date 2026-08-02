"""Deterministic, in-memory replay contract for V8.1 and independent V9 lanes.

This module deliberately accepts only timestamped :class:`PairedBook` values.
It has no network, filesystem, wallet, or settlement dependency.  A caller
may attach an offline outcome label for evaluation, but the label is never
passed into either classifier's live eligibility path.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, replace
from typing import Any, Dict, Iterable, Optional, Sequence, Tuple

from .post_close import (
    PairedBook,
    PostCloseDecision,
    PostCloseWinnerClassifier,
    classifier_family_config,
)
from .v9 import V9Config, V9DualLaneClassifier, active_v9_config


@dataclass(frozen=True)
class ReplayPolicy:
    """Optional one-factor overrides for a locked replay comparison."""

    v8_confirmations: Optional[int] = None
    v8_require_loser_refill_failure: Optional[bool] = None
    v9_sweep_confirmations: Optional[int] = None
    post_close_end_s: Optional[float] = None

    def __post_init__(self) -> None:
        for name in ("v8_confirmations", "v9_sweep_confirmations"):
            value = getattr(self, name)
            if value is not None and int(value) != value:
                raise ValueError("%s must be an integer" % name)
            if value is not None and int(value) <= 0:
                raise ValueError("%s must be > 0" % name)
        if self.post_close_end_s is not None and float(self.post_close_end_s) <= 0:
            raise ValueError("post_close_end_s must be > 0")


@dataclass(frozen=True)
class ReplayCheckpoint:
    """One deterministic decision point from one progressively observed book."""

    observed_ts: float
    decision_ts: float
    control_v8: PostCloseDecision
    lane_r: PostCloseDecision
    lane_s: PostCloseDecision

    def decision_for(self, lane: str) -> PostCloseDecision:
        choices = {
            "control_v8": self.control_v8,
            "R": self.lane_r,
            "S": self.lane_s,
        }
        try:
            return choices[lane]
        except KeyError as exc:
            raise ValueError("lane must be control_v8, R, or S") from exc


def counterfactual_policies() -> Dict[str, ReplayPolicy]:
    """Return locked control plus one-factor replay policies.

    These labels are intentionally explicit so callers do not tune several
    gates together on the same holdout.  The returned policies are immutable.
    """

    return {
        "control": ReplayPolicy(),
        "confirmation_only": ReplayPolicy(v8_confirmations=1, v9_sweep_confirmations=1),
        "loser_refill_only": ReplayPolicy(v8_require_loser_refill_failure=False),
        "window_horizon_only": ReplayPolicy(post_close_end_s=1.0),
    }


def _configs(policy: ReplayPolicy) -> Tuple[Any, V9Config]:
    v8_cfg = classifier_family_config("v8")
    v9_cfg = active_v9_config()
    if policy.v8_confirmations is not None:
        v8_cfg = replace(v8_cfg, confirmations=policy.v8_confirmations)
    if policy.v8_require_loser_refill_failure is not None:
        v8_cfg = replace(
            v8_cfg,
            require_loser_refill_failure=policy.v8_require_loser_refill_failure,
        )
    if policy.v9_sweep_confirmations is not None:
        v9_cfg = replace(v9_cfg, sweep_confirmations=policy.v9_sweep_confirmations)
    if policy.post_close_end_s is not None:
        v8_cfg = replace(v8_cfg, post_close_end_s=policy.post_close_end_s)
        v9_cfg = replace(v9_cfg, post_close_end_s=policy.post_close_end_s)
    return v8_cfg, v9_cfg


def replay_paired_books(
    books: Iterable[PairedBook],
    *,
    round_end_ts: float,
    qty: float,
    settlement_label: str = "unverified",
    policy: Optional[ReplayPolicy] = None,
    decision_latency_s: float = 0.0,
    code_sha: str = "replay",
) -> Tuple[ReplayCheckpoint, ...]:
    """Replay one ordered book sequence without I/O.

    Each input book is recorded once into control V8.1 and V9.  The classifiers
    are then evaluated at ``observed_at + decision_latency_s``.  This makes
    window-horizon and latency counterfactuals deterministic while preserving
    the distinction between observation time and decision time.
    """

    if qty <= 0:
        raise ValueError("qty must be > 0")
    if decision_latency_s < 0 or not math.isfinite(float(decision_latency_s)):
        raise ValueError("decision_latency_s must be finite and >= 0")
    replay_policy = policy or ReplayPolicy()
    v8_cfg, v9_cfg = _configs(replay_policy)
    control = PostCloseWinnerClassifier(v8_cfg)
    v9 = V9DualLaneClassifier(v9_cfg, settlement_label=settlement_label, code_sha=code_sha)
    rows = []
    for book in books:
        control.record(book)
        v9.record(book)
        observed_ts = float(book.observed_at)
        decision_ts = observed_ts + float(decision_latency_s)
        lanes = v9.evaluate_lanes(
            round_end_ts=float(round_end_ts),
            now_ts=decision_ts,
            qty=float(qty),
        )
        rows.append(
            ReplayCheckpoint(
                observed_ts=observed_ts,
                decision_ts=decision_ts,
                control_v8=control.evaluate(
                    round_end_ts=float(round_end_ts),
                    now_ts=decision_ts,
                    qty=float(qty),
                ),
                lane_r=lanes["R"],
                lane_s=lanes["S"],
            )
        )
    return tuple(rows)


def chronological_split(
    rows: Sequence[ReplayCheckpoint],
    *,
    train_fraction: float = 0.6,
    validation_fraction: float = 0.2,
) -> Dict[str, Tuple[ReplayCheckpoint, ...]]:
    """Split already chronologically ordered rows without shuffling."""

    if not 0 < train_fraction < 1:
        raise ValueError("train_fraction must be in (0, 1)")
    if not 0 <= validation_fraction < 1:
        raise ValueError("validation_fraction must be in [0, 1)")
    if train_fraction + validation_fraction >= 1:
        raise ValueError("train plus validation fractions must be < 1")
    train_end = int(len(rows) * train_fraction)
    validation_end = train_end + int(len(rows) * validation_fraction)
    return {
        "train": tuple(rows[:train_end]),
        "validation": tuple(rows[train_end:validation_end]),
        "unseen_holdout": tuple(rows[validation_end:]),
    }


def _normalized_outcome(outcome: Optional[str]) -> Optional[str]:
    if outcome is None:
        return None
    value = str(outcome).strip().upper()
    if value in {"YES", "UP"}:
        return "YES"
    if value in {"NO", "DOWN"}:
        return "NO"
    raise ValueError("outcome must be YES/NO or UP/DOWN")


def _wilson_lower_bound(correct: int, total: int) -> Optional[float]:
    if total <= 0:
        return None
    z = 1.959963984540054
    p = correct / total
    denominator = 1.0 + z * z / total
    center = p + z * z / (2.0 * total)
    margin = z * math.sqrt((p * (1.0 - p) + z * z / (4.0 * total)) / total)
    return (center - margin) / denominator


def summarize_lane(
    rows: Sequence[ReplayCheckpoint],
    *,
    lane: str,
    outcome: Optional[str] = None,
    qty: Optional[float] = None,
) -> Dict[str, Any]:
    """Summarize one replay sequence without inventing unlabeled metrics."""

    normalized = _normalized_outcome(outcome)
    candidate = next(
        (row.decision_for(lane) for row in rows if row.decision_for(lane).action == "enter"),
        None,
    )
    requested_qty = float(qty) if qty is not None else None
    if requested_qty is not None and requested_qty <= 0:
        raise ValueError("qty must be > 0")
    selected_qty = requested_qty or 0.0
    executable_qty = float(candidate.entry_ask_size) if candidate is not None else 0.0
    if candidate is not None and selected_qty <= 0:
        selected_qty = executable_qty
    cost = (
        float(candidate.entry_ask or 0.0) * selected_qty
        if candidate is not None
        else 0.0
    )
    labeled = candidate is not None and normalized is not None
    correct = int(labeled and candidate.side == normalized)
    return {
        "lane": lane,
        "checkpoint_count": len(rows),
        "opportunity_count": int(candidate is not None),
        "coverage": 1.0 if candidate is not None else 0.0,
        "theoretical_executable_qty": executable_qty,
        "observed_precision": (correct / 1.0) if labeled else None,
        "wilson_lower_bound": _wilson_lower_bound(correct, 1) if labeled else None,
        "maximum_loss": cost if labeled and not correct else (0.0 if labeled else None),
        "one_trade_tail_loss": cost if labeled and not correct else (0.0 if labeled else None),
        "decision_latency_s": (
            rows[-1].decision_ts - rows[-1].observed_ts if rows else None
        ),
        "outcome_label": normalized,
    }
