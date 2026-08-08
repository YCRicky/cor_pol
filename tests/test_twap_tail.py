from aftertake.twap_tail import (
    TWAP_CUTOVER_TS,
    BinanceTailInput,
    BinanceTrade,
    PMQuote,
    evaluate_tail_decision,
    replay_feature_decision,
    twap_market_gate,
)


ROUND_START = TWAP_CUTOVER_TS + 300
ROUND_END = ROUND_START + 300
TARGET_TS = ROUND_END - 10.25
DECISION_TS = ROUND_END - 10.20


def _trade(offset_s, price, *, received_offset_s=None):
    received = offset_s if received_offset_s is None else received_offset_s
    return BinanceTrade(
        trade_ms=int((ROUND_START + offset_s) * 1000),
        received_ms=int((ROUND_START + received) * 1000),
        price=price,
    )


def _quote(*, yes_bid=0.93, no_bid=0.06, observed_at=TARGET_TS):
    return PMQuote(
        observed_at=observed_at,
        yes_bid=yes_bid,
        no_bid=no_bid,
        yes_ask=0.95,
        no_ask=0.08,
        yes_ask_size=8.0,
        no_ask_size=8.0,
    )


def _tape(*trades, complete_coverage=True, open_price=100.0):
    return BinanceTailInput(
        asset="BTC",
        round_start_ms=ROUND_START * 1000,
        complete_coverage=complete_coverage,
        trades=trades,
        candle_open_price=open_price,
        candle_open_received_ms=(ROUND_START + 1) * 1000,
    )


def test_strong_spot_candle_enters_with_only_pre_target_data():
    decision = evaluate_tail_decision(
        quotes=[_quote(), _quote(yes_bid=0.10, no_bid=0.94, observed_at=TARGET_TS + 0.01)],
        binance=_tape(
            _trade(0.01, 99.90),  # deliberately not the official kline open
            _trade(270.0, 100.04),
            _trade(280.0, 100.06),
            _trade(289.7, 100.10),
            _trade(291.0, 99.80),  # arrives after E-10.25 and must not leak in
        ),
        round_end_ts=ROUND_END,
        decision_ts=DECISION_TS,
    )
    assert decision.action == "enter"
    assert decision.side == "YES"
    assert decision.audit["tail_feature_cutoff_ts"] == TARGET_TS
    assert decision.audit["leader_bid"] == 0.93
    assert decision.audit["binance_candle_open_price"] == 100.0
    assert decision.audit["path_gate"] == "strong_pass"


def test_weak_spot_candle_final_reversal_is_rejected():
    decision = evaluate_tail_decision(
        quotes=[_quote()],
        binance=_tape(
            _trade(0.01, 100.00),
            _trade(270.0, 100.02),
            _trade(280.0, 100.04),
            _trade(286.0, 100.06),
            _trade(289.7, 100.03),
        ),
        round_end_ts=ROUND_END,
        decision_ts=DECISION_TS,
    )
    assert decision.action == "hold"
    assert decision.reason in {
        "tail_weak_last10_reversal",
        "tail_weak_end_reversal",
    }


def test_exact_ninety_cent_leader_is_inclusive_like_the_replay():
    decision = evaluate_tail_decision(
        quotes=[_quote(yes_bid=0.90, no_bid=0.09)],
        binance=_tape(
            _trade(0.01, 100.00),
            _trade(270.0, 100.04),
            _trade(280.0, 100.06),
            _trade(289.7, 100.10),
        ),
        round_end_ts=ROUND_END,
        decision_ts=DECISION_TS,
    )
    assert decision.action == "enter"
    assert decision.audit["leader_bid_comparison"] == "greater_than_or_equal"


def test_incomplete_spot_coverage_is_fail_closed():
    decision = evaluate_tail_decision(
        quotes=[_quote()],
        binance=_tape(complete_coverage=False),
        round_end_ts=ROUND_END,
        decision_ts=DECISION_TS,
    )
    assert decision.action == "hold"
    assert decision.reason == "binance_spot_coverage_incomplete"


def test_cached_feature_rule_uses_no_outcome_label_for_selection():
    row = {
        "pm_side": "YES",
        "leader_bid": 0.90,
        "leader_quote_age_ms": 2_000,
        "binance_last_trade_age_ms": 2_000,
        "signed_candle_bp": 4.0,
        "signed_net20_bp": 1.0,
        "signed_last10_bp": 0.5,
        "adverse_end_reversal_bp": 2.0,
        "pm_winner": "NO",  # selection deliberately ignores this outcome label
    }
    assert replay_feature_decision(row) == (True, "weak_pass")


def test_twap_market_gate_requires_gamma_30_second_metadata():
    assert twap_market_gate("BTC", {"cryptoMarketConfig": {"twapEnabled": True, "twapLookbackSeconds": 30}}, ROUND_START) is None
    assert twap_market_gate("HYPE", {"cryptoMarketConfig": {"twapEnabled": True, "twapLookbackSeconds": 30}}, ROUND_START) == "tail_asset_not_supported"
    assert twap_market_gate("BTC", {}, ROUND_START) == "tail_gamma_twap_metadata_missing"
