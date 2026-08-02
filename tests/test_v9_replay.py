import pytest

from aftertake.post_close import PairedBook, SideBook
from aftertake.v9_replay import (
    ReplayPolicy,
    chronological_split,
    counterfactual_policies,
    replay_paired_books,
    summarize_lane,
)


def _side(bid, ask, *, asks):
    return SideBook(bid, 10.0, 10.0, ask, asks[0][1], 10.0, tuple(asks))


def _book(ts, *, no_bid=0.14):
    return PairedBook(
        observed_at=ts,
        source_timestamp=ts - 0.001,
        yes=_side(0.30, 0.35, asks=((0.35, 5.0), (0.99, 5.0))),
        no=_side(no_bid, 0.80, asks=((0.80, 10.0),)),
        yes_updated_at=ts,
        no_updated_at=ts,
    )


def test_replay_runs_control_and_both_v9_lanes_on_same_sequence():
    rows = replay_paired_books(
        [_book(999.9), _book(1000.01), _book(1000.06)],
        round_end_ts=1000.0,
        qty=5.0,
        settlement_label="binary_up_down",
    )

    assert len(rows) == 3
    assert rows[-1].control_v8.audit["strategy_version"].startswith("aftertake_v8")
    assert rows[-1].lane_r.action == "enter"
    assert rows[-1].lane_s.reason == "s_confirmation_insufficient"
    assert rows[-1].lane_r.audit["event_ts"] == 1000.059


def test_replay_counterfactuals_and_chronological_split_are_explicit():
    policies = counterfactual_policies()
    assert set(policies) == {
        "control",
        "confirmation_only",
        "loser_refill_only",
        "window_horizon_only",
    }
    assert policies["control"] == ReplayPolicy()
    assert policies["confirmation_only"].v8_confirmations == 1
    assert policies["loser_refill_only"].v8_require_loser_refill_failure is False
    assert policies["window_horizon_only"].post_close_end_s == 1.0

    rows = replay_paired_books(
        [_book(999.9), _book(1000.01), _book(1000.02)],
        round_end_ts=1000.0,
        qty=5.0,
        settlement_label="binary_up_down",
    )
    split = chronological_split(rows)
    assert split["train"] == (rows[0],)
    assert split["validation"] == ()
    assert split["unseen_holdout"] == (rows[1], rows[2])


def test_replay_summary_does_not_claim_precision_without_outcome_and_handles_latency():
    rows = replay_paired_books(
        [_book(1000.01)],
        round_end_ts=1000.0,
        qty=5.0,
        settlement_label="binary_up_down",
        decision_latency_s=0.02,
    )
    unlabeled = summarize_lane(rows, lane="R", qty=5.0)
    assert unlabeled["opportunity_count"] == 1
    assert unlabeled["observed_precision"] is None
    assert unlabeled["wilson_lower_bound"] is None
    assert unlabeled["decision_latency_s"] == pytest.approx(0.02)

    labeled = summarize_lane(rows, lane="R", outcome="UP", qty=5.0)
    assert labeled["observed_precision"] == 1.0
    assert labeled["wilson_lower_bound"] < 1.0
