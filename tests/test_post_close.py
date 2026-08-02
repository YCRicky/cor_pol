import pytest

from aftertake.post_close import (
    STRATEGY_VERSION,
    V7_STRATEGY_VERSION,
    PairedBook,
    PostCloseConfig,
    PostCloseWinnerClassifier,
    SideBook,
    active_classifier_config,
    classifier_family_config,
    legacy_v67_config,
)

ROUND_END = 1_000.0


def side(bid, ask, *, bid_size=20.0, depth=20.0, ask_size=20.0, near=None):
    return SideBook(bid, bid_size, depth, ask, ask_size, bid_size if near is None else near)


def paired(
    ts,
    yes_bid,
    yes_ask,
    no_bid,
    no_ask,
    *,
    yes_size=20.0,
    no_size=20.0,
    yes_depth=20.0,
    no_depth=20.0,
    yes_near=None,
    no_near=None,
    yes_updated_at=None,
    no_updated_at=None,
):
    return PairedBook(
        observed_at=ts,
        yes=side(yes_bid, yes_ask, bid_size=yes_size, depth=yes_depth, near=yes_near),
        no=side(no_bid, no_ask, bid_size=no_size, depth=no_depth, near=no_near),
        yes_updated_at=yes_updated_at,
        no_updated_at=no_updated_at,
    )


def classifier_with_v64_scene():
    classifier = PostCloseWinnerClassifier()
    classifier.record(paired(999.70, 0.47, 0.50, 0.51, 0.54, yes_near=18, no_near=18))
    classifier.record(paired(999.82, 0.48, 0.51, 0.50, 0.53, yes_near=18, no_near=18))
    classifier.record(paired(999.95, 0.49, 0.52, 0.50, 0.53, yes_near=18, no_near=18))
    return classifier


def test_default_post_close_profile_is_v7_event_driven_two_confirmation_profile():
    cfg = PostCloseConfig()

    assert cfg.post_close_start_s == 0.050
    assert cfg.confirmations == 2
    assert cfg.confirmation_spacing_s == 0.0


def test_active_post_close_profile_is_v8_clob_refill_guard():
    cfg = active_classifier_config()

    assert cfg.strategy_version == STRATEGY_VERSION
    assert cfg.post_close_start_s == 0.050
    assert cfg.post_close_end_s == 0.250
    assert cfg.confirmations == 2
    assert cfg.confirmation_spacing_s == 0.0
    assert cfg.distinct_evidence_confirmations is False
    assert cfg.require_loser_refill_failure is True
    assert cfg.require_stable_post_close_leader is True


def test_classifier_family_config_rejects_unknown_family():
    with pytest.raises(ValueError, match="unknown Aftertake classifier family"):
        classifier_family_config("v77")


def test_legacy_v67_profile_remains_available_for_same_corpus_comparison():
    classifier = PostCloseWinnerClassifier(legacy_v67_config())
    classifier.record(paired(999.70, 0.47, 0.50, 0.51, 0.54))
    classifier.record(paired(999.82, 0.48, 0.51, 0.50, 0.53))
    classifier.record(paired(999.95, 0.49, 0.52, 0.50, 0.53))
    for ts in (1_000.10, 1_000.22, 1_000.35):
        classifier.record(
            paired(ts, 0.60, 0.70, None, 0.99, yes_size=18, no_size=0, yes_near=18)
        )

    decision = classifier.evaluate(round_end_ts=ROUND_END, now_ts=1_000.35, qty=5.0)

    assert decision.action == "hold"
    assert decision.reason == "bid_support_not_persistent"
    assert decision.audit["strategy_version"] == "aftertake_v6.7_start_50ms_spacing_100ms"


def add_no_winner_sequence(classifier, *, no_ask=0.85, loser_ask=0.37, no_near=18.0, no_size=18.0):
    classifier.record(paired(1_000.10, 0.35, loser_ask, 0.58, no_ask, yes_size=2, no_size=no_size, yes_near=2, no_near=no_near))
    classifier.record(paired(1_000.22, 0.30, loser_ask, 0.60, no_ask, yes_size=2, no_size=no_size, yes_near=2, no_near=no_near))
    classifier.record(paired(1_000.35, 0.28, loser_ask, 0.61, no_ask, yes_size=2, no_size=no_size, yes_near=2, no_near=no_near))


def test_v7_enters_after_support_and_scored_vacuum():
    classifier = classifier_with_v64_scene()
    add_no_winner_sequence(classifier)

    decision = classifier.evaluate(round_end_ts=ROUND_END, now_ts=1_000.35, qty=5.0, max_entry_ask=0.90)

    assert decision.action == "enter"
    assert decision.side == "NO"
    assert decision.entry_ask == 0.85
    assert decision.reason == "v7_event_driven_one_sided_vacuum"
    assert decision.audit["strategy_version"] == V7_STRATEGY_VERSION
    assert decision.audit["support_score"] == decision.audit["support_required"] == 5
    assert decision.audit["vacuum_score"] >= decision.audit["vacuum_required"] == 3


def test_v8_hard_blocks_candidate_while_loser_side_is_still_refilling():
    books = (
        paired(999.70, 0.47, 0.50, 0.51, 0.54, yes_near=100, no_near=100),
        paired(999.82, 0.48, 0.51, 0.50, 0.53, yes_near=100, no_near=100),
        paired(999.95, 0.50, 0.52, 0.48, 0.50, yes_near=100, no_near=100),
        paired(
            1_000.10,
            0.34,
            0.36,
            0.64,
            0.68,
            yes_size=10,
            no_size=15,
            yes_near=113,
            no_near=15,
        ),
        paired(
            1_000.12,
            0.32,
            0.36,
            0.64,
            0.68,
            yes_size=10,
            no_size=15,
            yes_near=37,
            no_near=15,
        ),
    )
    v7 = PostCloseWinnerClassifier(classifier_family_config("v7"))
    v8 = PostCloseWinnerClassifier(classifier_family_config("v8"))
    for book in books:
        v7.record(book)
        v8.record(book)

    v7_decision = v7.evaluate(round_end_ts=ROUND_END, now_ts=1_000.12, qty=5.0)
    v8_decision = v8.evaluate(round_end_ts=ROUND_END, now_ts=1_000.12, qty=5.0)

    assert v7_decision.action == "enter"
    assert v7_decision.audit["vacuum_score"] == 3
    assert v7_decision.audit["vacuum_components"]["loser_refill_failure_ok"] is False
    assert v8_decision.action == "hold"
    assert v8_decision.reason == "loser_bid_refilled"


def test_v8_does_not_classify_a_new_candidate_after_250ms():
    classifier = PostCloseWinnerClassifier(classifier_family_config("v8"))
    for book in (
        paired(999.70, 0.47, 0.50, 0.51, 0.54, yes_near=18, no_near=18),
        paired(999.82, 0.48, 0.51, 0.50, 0.53, yes_near=18, no_near=18),
        paired(999.95, 0.49, 0.52, 0.50, 0.53, yes_near=18, no_near=18),
        paired(1_000.30, 0.28, 0.37, 0.61, 0.85, yes_size=2, no_size=18, yes_near=2, no_near=18),
        paired(1_000.32, 0.27, 0.37, 0.62, 0.85, yes_size=2, no_size=18, yes_near=2, no_near=18),
    ):
        classifier.record(book)

    decision = classifier.evaluate(round_end_ts=ROUND_END, now_ts=1_000.32, qty=5.0)

    assert decision.action == "hold"
    assert decision.reason == "post_close_window_expired"


def test_v8_abstains_after_the_initial_post_close_leader_reverses():
    books = (
        paired(999.70, 0.47, 0.50, 0.51, 0.54, yes_near=100, no_near=100),
        paired(999.82, 0.48, 0.51, 0.50, 0.53, yes_near=100, no_near=100),
        paired(999.95, 0.50, 0.52, 0.48, 0.50, yes_near=100, no_near=100),
        paired(1_000.06, 0.64, 0.66, 0.30, 0.32, yes_near=100, no_near=10),
        paired(1_000.10, 0.20, 0.22, 0.70, 0.75, yes_near=10, no_near=100),
        paired(1_000.12, 0.18, 0.20, 0.72, 0.75, yes_near=8, no_near=100),
    )
    v7 = PostCloseWinnerClassifier(classifier_family_config("v7"))
    v8 = PostCloseWinnerClassifier(classifier_family_config("v8"))
    for book in books:
        v7.record(book)
        v8.record(book)

    v7_decision = v7.evaluate(round_end_ts=ROUND_END, now_ts=1_000.12, qty=5.0)
    v8_decision = v8.evaluate(round_end_ts=ROUND_END, now_ts=1_000.12, qty=5.0)

    assert v7_decision.action == "enter"
    assert v8_decision.action == "hold"
    assert v8_decision.reason == "post_close_leader_reversed"
    assert v8_decision.audit["postclose_leader_path"] == ["YES", "NO", "NO"]


def test_one_sided_loser_bid_disappearance_is_strong_vacuum_evidence():
    classifier = classifier_with_v64_scene()
    classifier.record(
        paired(
            1_000.10,
            0.60,
            0.70,
            None,
            0.99,
            yes_size=18,
            no_size=0,
            yes_near=18,
            no_near=0,
        )
    )
    classifier.record(
        paired(
            1_000.22,
            0.61,
            0.70,
            None,
            0.99,
            yes_size=18,
            no_size=0,
            yes_near=18,
            no_near=0,
        )
    )
    classifier.record(
        paired(
            1_000.35,
            0.62,
            0.70,
            None,
            0.99,
            yes_size=18,
            no_size=0,
            yes_near=18,
            no_near=0,
        )
    )

    decision = classifier.evaluate(round_end_ts=ROUND_END, now_ts=1_000.35, qty=5.0)

    assert decision.action == "enter"
    assert decision.side == "YES"
    assert decision.entry_ask == 0.70
    assert decision.audit["loser_bid_missing"] is True
    assert decision.audit["vacuum_score"] == 4


def test_v7_confirms_on_two_distinct_book_events_without_waiting_100ms():
    classifier = classifier_with_v64_scene()
    classifier.record(
        paired(
            1_000.060,
            0.60,
            0.70,
            0.20,
            0.99,
            yes_size=18,
            no_size=2,
            yes_near=18,
            no_near=2,
        )
    )
    classifier.record(
        paired(
            1_000.064,
            0.61,
            0.70,
            0.19,
            0.99,
            yes_size=18,
            no_size=2,
            yes_near=18,
            no_near=2,
        )
    )

    decision = classifier.evaluate(round_end_ts=ROUND_END, now_ts=1_000.064, qty=5.0)

    assert decision.action == "enter"
    assert decision.side == "YES"
    assert decision.audit["confirmation_timestamps"] == [1_000.060, 1_000.064]


def test_v7_does_not_count_an_identical_repeated_snapshot_as_confirmation():
    classifier = classifier_with_v64_scene()
    for ts in (1_000.060, 1_000.064):
        classifier.record(
            paired(
                ts,
                0.60,
                0.70,
                0.20,
                0.99,
                yes_size=18,
                no_size=2,
                yes_near=18,
                no_near=2,
            )
        )

    decision = classifier.evaluate(round_end_ts=ROUND_END, now_ts=1_000.064, qty=5.0)

    assert decision.action == "hold"
    assert decision.reason == "bid_support_not_yet_persistent"
    assert decision.confirmations == 1


def test_v8_counts_fresh_unchanged_winner_book_as_persistent_support():
    classifier = PostCloseWinnerClassifier(classifier_family_config("v8"))
    for book in (
        paired(999.70, 0.47, 0.50, 0.51, 0.54, yes_near=20, no_near=20),
        paired(999.82, 0.48, 0.51, 0.50, 0.53, yes_near=20, no_near=20),
        paired(999.95, 0.49, 0.52, 0.50, 0.53, yes_near=20, no_near=20),
        paired(
            1_000.060,
            0.70,
            0.99,
            0.20,
            0.99,
            yes_size=20,
            no_size=2,
            yes_near=20,
            no_near=2,
        ),
        paired(
            1_000.064,
            0.70,
            0.99,
            0.20,
            0.99,
            yes_size=20,
            no_size=2,
            yes_near=20,
            no_near=2,
        ),
    ):
        classifier.record(book)

    decision = classifier.evaluate(round_end_ts=ROUND_END, now_ts=1_000.064, qty=5.0)

    assert decision.action == "enter"
    assert decision.side == "YES"
    assert decision.entry_ask == 0.99
    assert decision.confirmations == 2
    assert decision.audit["confirmation_timestamps"] == [1_000.060, 1_000.064]
    assert decision.audit["confirmation_policy"] == "fresh_paired_observations"


def test_v7_waits_until_both_token_books_are_fresh_after_frontend_close():
    classifier = classifier_with_v64_scene()
    classifier.record(
        paired(
            1_000.060,
            0.60,
            0.70,
            0.20,
            0.99,
            yes_size=18,
            no_size=2,
            yes_near=18,
            no_near=2,
            yes_updated_at=1_000.060,
            no_updated_at=999.950,
        )
    )
    classifier.record(
        paired(
            1_000.064,
            0.61,
            0.70,
            0.19,
            0.99,
            yes_size=18,
            no_size=2,
            yes_near=18,
            no_near=2,
            yes_updated_at=1_000.064,
            no_updated_at=999.950,
        )
    )

    decision = classifier.evaluate(round_end_ts=ROUND_END, now_ts=1_000.064, qty=5.0)

    assert decision.action == "hold"
    assert decision.reason == "paired_post_close_state_not_fresh"


def test_v7_accepts_a_token_update_after_close_but_before_the_50ms_decision_start():
    classifier = classifier_with_v64_scene()
    classifier.record(
        paired(
            1_000.060,
            0.60,
            0.70,
            0.20,
            0.99,
            yes_size=18,
            no_size=2,
            yes_near=18,
            no_near=2,
            yes_updated_at=1_000.010,
            no_updated_at=1_000.055,
        )
    )
    classifier.record(
        paired(
            1_000.064,
            0.61,
            0.70,
            0.19,
            0.99,
            yes_size=18,
            no_size=2,
            yes_near=18,
            no_near=2,
            yes_updated_at=1_000.064,
            no_updated_at=1_000.055,
        )
    )

    decision = classifier.evaluate(round_end_ts=ROUND_END, now_ts=1_000.064, qty=5.0)

    assert decision.action == "enter"
    assert decision.audit["confirmation_timestamps"] == [1_000.060, 1_000.064]


def test_v7_rejects_when_both_outcome_bids_are_missing():
    classifier = classifier_with_v64_scene()
    classifier.record(paired(1_000.060, None, 0.70, None, 0.99, yes_size=0, no_size=0))
    classifier.record(paired(1_000.064, None, 0.71, None, 0.98, yes_size=0, no_size=0))

    decision = classifier.evaluate(round_end_ts=ROUND_END, now_ts=1_000.064, qty=5.0)

    assert decision.action == "hold"
    assert decision.reason == "bid_support_not_persistent"


def test_cheap_loser_ask_is_rejected_by_side_identity():
    classifier = classifier_with_v64_scene()
    add_no_winner_sequence(classifier, loser_ask=0.36)

    decision = classifier.evaluate(round_end_ts=ROUND_END, now_ts=1_000.35, qty=5.0, max_entry_ask=0.90)

    assert decision.action == "enter"
    assert decision.side == "NO"
    assert decision.entry_ask == 0.85


def test_rejects_until_two_distinct_post_close_observations():
    classifier = classifier_with_v64_scene()
    classifier.record(paired(1_000.10, 0.35, 0.37, 0.58, 0.85, yes_size=2, no_size=18, yes_near=2, no_near=18))

    decision = classifier.evaluate(round_end_ts=ROUND_END, now_ts=1_000.10, qty=5.0, max_entry_ask=0.90)

    assert decision.action == "hold"
    assert decision.reason == "insufficient_post_close_observations"


def test_clean_terminal_preclose_is_observed_not_hard_rejected():
    classifier = PostCloseWinnerClassifier()
    classifier.record(paired(999.70, 0.01, 0.02, 0.98, 0.99))
    classifier.record(paired(999.82, 0.01, 0.02, 0.98, 0.99))
    classifier.record(paired(999.95, 0.01, 0.02, 0.98, 0.99))
    add_no_winner_sequence(classifier)

    decision = classifier.evaluate(round_end_ts=ROUND_END, now_ts=1_000.35, qty=5.0, max_entry_ask=0.90)

    assert decision.action == "enter"
    assert decision.reason == "v7_event_driven_one_sided_vacuum"
    assert decision.audit["preclose_scene_gate"] == "audit_only"
    assert decision.audit["preclose_scene_label"] == "directional_low_vol"
    assert "preclose_price_ambiguous_failed" in decision.audit["preclose_scene_warnings"]
    assert decision.audit["support_score"] == 5
    assert decision.audit["vacuum_score"] == 3


def test_preclose_volatility_excessive_is_observed_not_hard_rejected():
    classifier = PostCloseWinnerClassifier()
    classifier.record(paired(999.70, 0.30, 0.32, 0.70, 0.72))
    classifier.record(paired(999.82, 0.48, 0.51, 0.50, 0.53))
    classifier.record(paired(999.95, 0.49, 0.52, 0.50, 0.53))
    add_no_winner_sequence(classifier)

    decision = classifier.evaluate(round_end_ts=ROUND_END, now_ts=1_000.35, qty=5.0, max_entry_ask=0.90)

    assert decision.action == "enter"
    assert decision.reason == "v7_event_driven_one_sided_vacuum"
    assert decision.audit["preclose_scene_gate"] == "audit_only"
    assert "preclose_bid_volatility_excessive" in decision.audit["preclose_scene_warnings"]


def test_support_requires_near_touch_depth_not_far_away_depth():
    classifier = classifier_with_v64_scene()
    add_no_winner_sequence(classifier, no_near=4.0, no_size=18.0)

    decision = classifier.evaluate(round_end_ts=ROUND_END, now_ts=1_000.35, qty=5.0, max_entry_ask=0.90)

    assert decision.action == "hold"
    assert decision.reason == "winner_near_touch_depth_too_thin"


def test_loser_reclaim_blocks_candidate():
    classifier = classifier_with_v64_scene()
    classifier.record(paired(1_000.10, 0.57, 0.59, 0.58, 0.85, yes_size=12, no_size=18, yes_near=12, no_near=18))
    classifier.record(paired(1_000.22, 0.57, 0.59, 0.60, 0.85, yes_size=12, no_size=18, yes_near=12, no_near=18))
    classifier.record(paired(1_000.35, 0.60, 0.62, 0.61, 0.85, yes_size=12, no_size=18, yes_near=12, no_near=18))

    decision = classifier.evaluate(round_end_ts=ROUND_END, now_ts=1_000.35, qty=5.0, max_entry_ask=0.90)

    assert decision.action == "hold"
    assert decision.reason in {"loser_bid_drop_insufficient", "loser_reclaimed_bid"}


def test_winner_residual_ask_reprice_is_observed_not_hard_rejected():
    classifier = classifier_with_v64_scene()
    classifier.record(paired(1_000.10, 0.35, 0.37, 0.58, 0.80, yes_size=2, no_size=18, yes_near=2, no_near=18))
    classifier.record(paired(1_000.22, 0.30, 0.37, 0.60, 0.84, yes_size=2, no_size=18, yes_near=2, no_near=18))
    classifier.record(paired(1_000.35, 0.28, 0.37, 0.61, 0.86, yes_size=2, no_size=18, yes_near=2, no_near=18))

    decision = classifier.evaluate(round_end_ts=ROUND_END, now_ts=1_000.35, qty=5.0, max_entry_ask=0.90)

    assert decision.action == "enter"
    assert decision.reason == "v7_event_driven_one_sided_vacuum"
    assert decision.audit["ask_reprice_observed"] > 0.01


def test_winner_bid_floor_is_mandatory_not_score_optional():
    classifier = classifier_with_v64_scene()
    classifier.record(paired(1_000.10, 0.49, 0.60, 0.40, 0.42, yes_near=18, no_near=2))
    classifier.record(paired(1_000.22, 0.49, 0.60, 0.35, 0.37, yes_near=18, no_near=2))
    classifier.record(paired(1_000.35, 0.49, 0.60, 0.30, 0.32, yes_near=18, no_near=2))

    decision = classifier.evaluate(round_end_ts=ROUND_END, now_ts=1_000.35, qty=5.0, max_entry_ask=0.90)

    assert decision.action == "hold"
    assert decision.reason == "winner_bid_floor_too_low"


def test_winner_best_bid_size_is_mandatory_not_score_optional():
    classifier = classifier_with_v64_scene()
    classifier.record(paired(1_000.10, 0.58, 0.64, 0.35, 0.37, yes_size=2, yes_near=15, no_near=2))
    classifier.record(paired(1_000.22, 0.60, 0.64, 0.30, 0.32, yes_size=2, yes_near=15, no_near=2))
    classifier.record(paired(1_000.35, 0.61, 0.64, 0.28, 0.30, yes_size=2, yes_near=15, no_near=2))

    decision = classifier.evaluate(round_end_ts=ROUND_END, now_ts=1_000.35, qty=5.0, max_entry_ask=0.90)

    assert decision.action == "hold"
    assert decision.reason == "winner_bid_support_too_thin"


def test_loser_bid_drop_is_scored_not_individually_mandatory():
    classifier = classifier_with_v64_scene()
    classifier.record(paired(1_000.10, 0.48, 0.50, 0.58, 0.64, yes_size=2, no_size=18, yes_near=2, no_near=18))
    classifier.record(paired(1_000.22, 0.48, 0.50, 0.60, 0.64, yes_size=2, no_size=18, yes_near=2, no_near=18))
    classifier.record(paired(1_000.35, 0.48, 0.50, 0.61, 0.64, yes_size=2, no_size=18, yes_near=2, no_near=18))

    decision = classifier.evaluate(round_end_ts=ROUND_END, now_ts=1_000.35, qty=5.0, max_entry_ask=0.90)

    assert decision.action == "enter"
    assert decision.reason == "v7_event_driven_one_sided_vacuum"
    assert decision.audit["vacuum_score"] >= decision.audit["vacuum_required"]
    assert "loser_bid_drop_insufficient" in decision.audit["vacuum_reject_components"]


def test_winner_residual_ask_cap_is_disabled_by_owner_override():
    classifier = classifier_with_v64_scene()
    add_no_winner_sequence(classifier, no_ask=0.91)

    decision = classifier.evaluate(round_end_ts=ROUND_END, now_ts=1_000.35, qty=5.0, max_entry_ask=0.01)

    assert decision.action == "enter"
    assert decision.reason == "v7_event_driven_one_sided_vacuum"
    assert decision.entry_ask == 0.91
    assert decision.audit["ask_lag"]["entry_price_cap"] == "disabled"
