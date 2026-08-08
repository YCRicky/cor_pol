from aftertake.twap_tail import (
    TWAP_CUTOVER_TS,
    BinanceTailInput,
    BinanceTrade,
    PMQuote,
    evaluate_tail_decision,
    twap_market_gate,
)

ROUND_START = TWAP_CUTOVER_TS + 300
ROUND_END = ROUND_START + 300
DECISION_TS = ROUND_END - 9.95


def _trade(offset_s, price, *, received_offset_s=None):
    received = offset_s if received_offset_s is None else received_offset_s
    return BinanceTrade(
        trade_ms=int((ROUND_START + offset_s) * 1000),
        received_ms=int((ROUND_START + received) * 1000),
        price=price,
    )


def _quote(*, yes_bid=0.93, no_bid=0.06, yes_ask_size=8.0, observed_at=DECISION_TS):
    return PMQuote(
        observed_at=observed_at,
        yes_bid=yes_bid,
        no_bid=no_bid,
        yes_ask=0.95,
        no_ask=0.08,
        yes_ask_size=yes_ask_size,
        no_ask_size=8.0,
    )


def _tape(*trades):
    return BinanceTailInput(
        asset="BTC",
        round_start_ms=ROUND_START * 1000,
        complete_coverage=True,
        trades=trades,
    )


def test_pm_tail_enters_without_using_future_futures_data():
    decision = evaluate_tail_decision(
        quotes=[
            _quote(yes_ask_size=0.0),
            _quote(yes_bid=0.10, no_bid=0.94, observed_at=DECISION_TS + 1),
        ],
        binance=_tape(
            _trade(0.01, 100.00),
            _trade(270.0, 100.04),
            _trade(280.0, 100.06),
            _trade(289.7, 100.10),
            _trade(291.0, 99.80),
        ),
        round_end_ts=ROUND_END,
        decision_ts=DECISION_TS,
    )
    assert decision.action == "enter"
    assert decision.side == "YES"
    assert decision.audit["binance_gate_applied"] is False


def test_futures_reversal_is_observational_and_does_not_block_pm_tail():
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
    assert decision.action == "enter"
    assert decision.reason == "tail_pm_twap_qualified"


def test_incomplete_futures_coverage_is_observational_only():
    decision = evaluate_tail_decision(
        quotes=[_quote()],
        binance=BinanceTailInput("BTC", ROUND_START * 1000, False, (), "stream_disconnected"),
        round_end_ts=ROUND_END,
        decision_ts=DECISION_TS,
    )
    assert decision.action == "enter"
    assert decision.audit["binance_gate_applied"] is False
    assert decision.audit["binance_invalid_reason"] == "stream_disconnected"


def test_twap_market_gate_requires_gamma_30_second_metadata():
    assert twap_market_gate("BTC", {"cryptoMarketConfig": {"twapEnabled": True, "twapLookbackSeconds": 30}}, ROUND_START) is None
    assert twap_market_gate("HYPE", {"cryptoMarketConfig": {"twapEnabled": True, "twapLookbackSeconds": 30}}, ROUND_START) == "tail_asset_not_supported"
    assert twap_market_gate("BTC", {}, ROUND_START) == "tail_gamma_twap_metadata_missing"
