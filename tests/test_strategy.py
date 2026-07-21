from misprice_pm.strategy import (
    BookSnapshot,
    MispriceConfig,
    StrategyState,
    evaluate_tick,
    lag_depth_for,
)


def make_book(yes_ask=0.52, no_ask=0.48, yes_bid=None, no_bid=None):
    return BookSnapshot(
        yes_bid=yes_bid if yes_bid is not None else yes_ask - 0.01,
        yes_ask=yes_ask,
        yes_ask_size=20.0,
        no_bid=no_bid if no_bid is not None else no_ask - 0.01,
        no_ask=no_ask,
        no_ask_size=20.0,
        age_s=0.1,
    )


def seeded_state(*, open_price=100.0, pre_ts=114.0, pre_price=100.00, pre_book=None):
    state = StrategyState(open_price=open_price)
    state.record_spot(ts=0.0, price=open_price)
    state.record_spot(ts=pre_ts, price=pre_price)
    state.record_book(ts=pre_ts, book=pre_book or make_book(yes_ask=0.49, no_ask=0.51))
    return state


def test_v3_lag_detector_enters_when_transition_reprices_less_than_required():
    cfg = MispriceConfig()
    state = seeded_state(pre_book=make_book(yes_ask=0.49, no_ask=0.51))

    decision = evaluate_tick(
        cfg=cfg,
        state=state,
        now_ts=130.0,
        round_start_ts=0,
        round_end_ts=300,
        slug="btc-updown-5m-test",
        spot_price=100.0406,
        book=make_book(yes_ask=0.49, no_ask=0.51),
    )

    assert decision.action == "enter"
    assert decision.reason == "repricing_lag_underreaction"
    assert decision.side == "YES"
    assert decision.entry_ask == 0.49
    assert decision.pre_ask == 0.49
    assert decision.signal_ask == 0.49
    assert decision.transition_bp is not None and decision.transition_bp >= 3.0
    assert decision.required_reprice is not None and decision.required_reprice > 0.12
    assert decision.actual_reprice == 0.0
    assert decision.lag_depth is not None and decision.lag_depth > 0.10


def test_v3_lag_detector_blocks_when_pm_already_repriced():
    cfg = MispriceConfig()
    state = seeded_state(pre_book=make_book(yes_ask=0.49, no_ask=0.51))

    decision = evaluate_tick(
        cfg=cfg,
        state=state,
        now_ts=130.0,
        round_start_ts=0,
        round_end_ts=300,
        slug="btc-updown-5m-test",
        spot_price=100.0406,
        book=make_book(yes_ask=0.63, no_ask=0.37),
    )

    assert decision.action == "hold"
    assert decision.reason == "repricing_lag_too_small"
    assert decision.required_reprice is not None
    assert decision.actual_reprice is not None
    assert decision.lag_depth is not None
    assert decision.lag_depth < cfg.min_lag_depth


def test_v3_allows_above_60c_when_lag_depth_survives():
    cfg = MispriceConfig()
    state = seeded_state(pre_book=make_book(yes_ask=0.60, no_ask=0.40))

    decision = evaluate_tick(
        cfg=cfg,
        state=state,
        now_ts=130.0,
        round_start_ts=0,
        round_end_ts=300,
        slug="btc-updown-5m-test",
        spot_price=100.0406,
        book=make_book(yes_ask=0.64, no_ask=0.36),
    )

    assert decision.action == "enter"
    assert decision.entry_ask == 0.64
    assert decision.lag_depth is not None and decision.lag_depth >= cfg.min_lag_depth


def test_v3_blocks_above_65c_optimized_risk_cap_even_if_lag_depth_exists():
    cfg = MispriceConfig()
    state = seeded_state(pre_book=make_book(yes_ask=0.64, no_ask=0.36))

    decision = evaluate_tick(
        cfg=cfg,
        state=state,
        now_ts=130.0,
        round_start_ts=0,
        round_end_ts=300,
        slug="btc-updown-5m-test",
        spot_price=100.0406,
        book=make_book(yes_ask=0.66, no_ask=0.34),
    )

    assert decision.action == "hold"
    assert decision.reason == "entry_ask_out_of_range"


def test_v3_requires_real_book_lookback_not_just_spot_path():
    cfg = MispriceConfig()
    state = StrategyState(open_price=100.0)
    state.record_spot(ts=114.0, price=100.00)

    decision = evaluate_tick(
        cfg=cfg,
        state=state,
        now_ts=130.0,
        round_start_ts=0,
        round_end_ts=300,
        slug="btc-updown-5m-test",
        spot_price=100.0406,
        book=make_book(yes_ask=0.49, no_ask=0.51),
    )

    assert decision.action == "hold"
    assert decision.reason == "insufficient_book_lookback"


def test_lag_depth_formula_matches_required_minus_actual_reprice():
    cfg = MispriceConfig(reprice_per_bp=0.035)

    required, actual, lag_depth = lag_depth_for(
        cfg=cfg, transition_bp=3.0, pre_ask=0.49, signal_ask=0.50
    )

    assert round(required, 6) == 0.105
    assert round(actual, 6) == 0.01
    assert round(lag_depth, 6) == 0.095
