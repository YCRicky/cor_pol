from aftertake.live_sizing import compute_live_entry_size, floor_to_step
from aftertake.pm_client import BalanceAllowance, MarketMetadata


def _metadata(min_order_size=1.0):
    return MarketMetadata(
        condition_id="condition",
        tick_size="0.01",
        min_order_size=min_order_size,
        neg_risk=False,
        fee_rate=0.0,
        tokens={"up": "up-token", "down": "down-token"},
        raw={},
    )


def _fee_metadata():
    return MarketMetadata(
        condition_id="condition",
        tick_size="0.01",
        min_order_size=1.0,
        neg_risk=False,
        fee_rate=0.07,
        fee_exponent=1.0,
        builder_taker_fee_bps=100,
        tokens={"up": "up-token", "down": "down-token"},
        raw={},
    )


def test_floor_to_integer_step_never_rounds_up():
    assert floor_to_step(67.5, 1) == 67
    assert floor_to_step(67.999, 1) == 67
    assert floor_to_step(28, 1) == 28


def test_live_sizing_takes_all_available_when_under_half_account_balance():
    sizing = compute_live_entry_size(
        price=0.74,
        available_size=28,
        collateral=BalanceAllowance(balance=100, allowance=100, raw={}),
        metadata=_metadata(),
        max_account_fraction=0.5,
        quantity_step=1,
    )

    assert sizing.accepted is True
    assert sizing.qty == 28
    assert sizing.requested_notional == 20.72
    assert sizing.risk_budget == 50


def test_live_sizing_caps_to_half_account_balance_and_floors_qty():
    sizing = compute_live_entry_size(
        price=0.74,
        available_size=280,
        collateral=BalanceAllowance(balance=100, allowance=100, raw={}),
        metadata=_metadata(),
        max_account_fraction=0.5,
        quantity_step=1,
    )

    assert sizing.accepted is True
    assert sizing.qty == 67
    assert round(sizing.requested_notional, 2) == 49.58
    assert sizing.requested_notional <= 50


def test_live_sizing_uses_allowance_as_execution_cap_without_expanding_account_risk():
    sizing = compute_live_entry_size(
        price=0.50,
        available_size=200,
        collateral=BalanceAllowance(balance=100, allowance=20, raw={}),
        metadata=_metadata(),
        max_account_fraction=0.5,
        quantity_step=1,
    )

    assert sizing.accepted is True
    assert sizing.qty == 40
    assert sizing.requested_notional == 20


def test_live_sizing_rejects_when_floor_qty_below_market_minimum():
    sizing = compute_live_entry_size(
        price=0.74,
        available_size=3,
        collateral=BalanceAllowance(balance=100, allowance=100, raw={}),
        metadata=_metadata(min_order_size=5),
        max_account_fraction=0.5,
        quantity_step=1,
    )

    assert sizing.accepted is False
    assert sizing.reason == "sized_qty_below_market_minimum"


def test_live_sizing_zero_balance_blocks_live_order():
    sizing = compute_live_entry_size(
        price=0.74,
        available_size=28,
        collateral=BalanceAllowance(balance=0, allowance=100, raw={}),
        metadata=_metadata(),
        max_account_fraction=0.5,
        quantity_step=1,
    )

    assert sizing.accepted is False
    assert sizing.reason == "missing_account_balance"


def test_live_sizing_includes_market_and_builder_fees_in_the_account_cap():
    sizing = compute_live_entry_size(
        price=0.50,
        available_size=200,
        collateral=BalanceAllowance(balance=100, allowance=100, raw={}),
        metadata=_fee_metadata(),
        max_account_fraction=0.5,
        quantity_step=1,
    )

    # A price-only cap would submit 100 shares.  At this market fee schedule,
    # fees make that exceed the $50 risk budget, so the quantity must be lower.
    assert sizing.accepted is True
    assert sizing.qty == 95
    assert sizing.estimated_fee > 0
    assert sizing.estimated_total_cost <= sizing.risk_budget
