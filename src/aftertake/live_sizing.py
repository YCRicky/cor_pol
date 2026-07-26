"""Live order sizing for Aftertake residual-ask entries.

The live strategy wants to take as much displayed ask depth as possible, but the
worst-case cost of a buy-to-settle order must stay inside an account-level risk
budget.  All sizing is floor-only so we never submit more size than the local
calculation approved.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

from .pm_client import BalanceAllowance, MarketMetadata
from .settlement import builder_fee_total, fee_total


@dataclass(frozen=True)
class LiveSizingDecision:
    qty: float
    requested_notional: float
    account_balance: float
    collateral_allowance: float
    risk_budget: float
    available_size: float
    price: float
    precision_step: float
    reason: str = ""
    estimated_fee: float = 0.0
    estimated_total_cost: float = 0.0

    @property
    def accepted(self) -> bool:
        return self.qty > 0 and not self.reason


def floor_to_step(value: float, step: float) -> float:
    """Floor ``value`` to a positive quantity step without rounding up."""

    value = float(value)
    step = float(step)
    if value <= 0 or step <= 0:
        return 0.0
    scaled = math.floor((value / step) + 1e-12)
    return round(scaled * step, 12)


def live_quantity_step(metadata: MarketMetadata, configured_step: float) -> float:
    """Choose the floor step for live share size.

    The owner explicitly prefers integer share sizing for this Aftertake live
    system (e.g. 67.5 -> 67).  Keep this configurable so a future Polymarket SDK
    precision upgrade can use a smaller step without changing strategy code.
    """

    del metadata  # Current V2 market metadata does not expose a separate size step.
    return max(1.0, float(configured_step))


def compute_live_entry_size(
    *,
    price: float,
    available_size: float,
    collateral: BalanceAllowance,
    metadata: MarketMetadata,
    max_account_fraction: float,
    quantity_step: float,
) -> LiveSizingDecision:
    """Return the largest safe floor-sized entry for a displayed ask.

    The sizing budget includes the current market fee and configured Builder
    taker fee.  Ignoring fees would make the order *less* conservative: a
    price-only calculation can exceed both collateral allowance and the stated
    account-risk cap at submission time.
    """

    price = float(price)
    available_size = float(available_size)
    account_balance = float(collateral.balance)
    allowance = float(collateral.allowance)
    max_account_fraction = float(max_account_fraction)
    if price <= 0 or price >= 1:
        return LiveSizingDecision(0.0, 0.0, account_balance, allowance, 0.0, available_size, price, 0.0, "invalid_price")
    if available_size <= 0:
        return LiveSizingDecision(0.0, 0.0, account_balance, allowance, 0.0, available_size, price, 0.0, "no_displayed_ask_depth")
    if account_balance <= 0:
        return LiveSizingDecision(0.0, 0.0, account_balance, allowance, 0.0, available_size, price, 0.0, "missing_account_balance")
    if allowance <= 0:
        return LiveSizingDecision(0.0, 0.0, account_balance, allowance, 0.0, available_size, price, 0.0, "missing_collateral_allowance")
    if not 0 < max_account_fraction <= 1:
        return LiveSizingDecision(0.0, 0.0, account_balance, allowance, 0.0, available_size, price, 0.0, "invalid_account_risk_fraction")

    risk_budget = account_balance * max_account_fraction
    spend_cap = min(risk_budget, allowance)
    unit_fee = fee_total(
        price, 1.0, metadata.fee_rate, metadata.fee_exponent
    ) + builder_fee_total(price, 1.0, metadata.builder_taker_fee_bps)
    unit_cost = price + unit_fee
    raw_qty_cap = min(available_size, spend_cap / unit_cost)
    step = live_quantity_step(metadata, quantity_step)
    qty = floor_to_step(raw_qty_cap, step)
    notional = qty * price
    estimated_fee = fee_total(
        price, qty, metadata.fee_rate, metadata.fee_exponent
    ) + builder_fee_total(price, qty, metadata.builder_taker_fee_bps)
    total_cost = notional + estimated_fee
    if qty < metadata.min_order_size:
        return LiveSizingDecision(qty, notional, account_balance, allowance, risk_budget, available_size, price, step, "sized_qty_below_market_minimum", estimated_fee, total_cost)
    if total_cost > risk_budget + 1e-9:
        return LiveSizingDecision(qty, notional, account_balance, allowance, risk_budget, available_size, price, step, "sized_total_cost_exceeds_account_risk_budget", estimated_fee, total_cost)
    if total_cost > allowance + 1e-9:
        return LiveSizingDecision(qty, notional, account_balance, allowance, risk_budget, available_size, price, step, "sized_total_cost_exceeds_collateral_allowance", estimated_fee, total_cost)
    return LiveSizingDecision(
        qty,
        notional,
        account_balance,
        allowance,
        risk_budget,
        available_size,
        price,
        step,
        estimated_fee=estimated_fee,
        estimated_total_cost=total_cost,
    )
