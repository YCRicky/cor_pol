import pytest

from misprice_pm.settlement import fee_total, settle_trade


def test_fee_total_uses_supplied_current_market_formula():
    assert round(fee_total(0.60, 5.0, 0.07), 6) == 0.084
    assert round(fee_total(0.60, 5.0, 0.07, fee_exponent=2), 6) == 0.02016


def test_settlement_rejects_unrecorded_or_unspecified_fee():
    with pytest.raises(ValueError, match="fee"):
        settle_trade(side="YES", entry_price=0.64, qty=5.0, pm_up=True)


def test_settle_yes_win_uses_pm_outcome_and_explicit_fee_rate():
    result = settle_trade(
        side="YES", entry_price=0.64, qty=5.0, pm_up=True, friction_per_share=0.02, fee_rate=0.07
    )

    assert result.win is True
    assert round(result.pnl, 6) == 1.61936
    assert result.settlement_source == "pm"
    assert result.fee_source == "market_fee_rate"


def test_settle_no_loss_with_recorded_actual_fee():
    result = settle_trade(side="NO", entry_price=0.56, qty=5.0, pm_up=True, entry_fee=0.08624)

    assert result.win is False
    assert round(result.pnl, 6) == -2.88624
    assert result.fee_source == "recorded"
