"""Official-outcome settlement math without hard-coded market fee assumptions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class SettlementResult:
    side: str
    qty: float
    entry_price: float
    pm_up: bool
    win: bool
    payoff: float
    entry_cost: float
    entry_fee: float
    friction: float
    pnl: float
    settlement_source: str = "pm"
    fee_source: str = "recorded"


def fee_total(
    price: float, qty: float, fee_rate: float, fee_exponent: float = 1.0
) -> float:
    """Calculate a fee only when the current market rate is supplied."""

    p = max(0.0, min(1.0, float(price)))
    rate = float(fee_rate)
    exponent = float(fee_exponent)
    if rate < 0:
        raise ValueError("fee_rate must be non-negative")
    if exponent < 0:
        raise ValueError("fee_exponent must be non-negative")
    return round(float(qty) * rate * (p * (1.0 - p)) ** exponent, 5)


def builder_fee_total(price: float, qty: float, builder_fee_bps: float) -> float:
    bps = float(builder_fee_bps)
    if bps < 0:
        raise ValueError("builder_fee_bps must be non-negative")
    return round(float(price) * float(qty) * bps / 10_000.0, 5)


def settle_trade(
    *,
    side: str,
    entry_price: float,
    qty: float,
    pm_up: bool,
    friction_per_share: float = 0.0,
    fee_rate: Optional[float] = None,
    entry_fee: Optional[float] = None,
    builder_fee_bps: float = 0.0,
    fee_exponent: float = 1.0,
) -> SettlementResult:
    """Settle a confirmed fill from official PM outcomes.

    V2 fees are determined per market at match time.  A caller must provide
    either the actual recorded fee or the current metadata fee rate; silently
    applying a legacy 7% assumption is prohibited.
    """

    normalized = side.upper()
    if normalized not in {"YES", "NO"}:
        raise ValueError("side must be YES or NO")
    if qty <= 0 or not 0 < entry_price < 1:
        raise ValueError("settlement requires a positive confirmed quantity and price")
    if entry_fee is not None and fee_rate is not None:
        raise ValueError("supply recorded entry_fee or fee_rate, not both")
    if entry_fee is None and fee_rate is None:
        raise ValueError("settlement requires recorded entry_fee or current market fee_rate")
    win = (normalized == "YES" and pm_up) or (normalized == "NO" and not pm_up)
    payoff = float(qty) if win else 0.0
    entry_cost = float(entry_price) * float(qty)
    if entry_fee is not None:
        fee = float(entry_fee)
        fee_source = "recorded"
    else:
        fee = fee_total(
            entry_price, qty, float(fee_rate), fee_exponent
        ) + builder_fee_total(
            entry_price, qty, builder_fee_bps
        )
        fee_source = "market_fee_rate"
    friction = float(friction_per_share) * float(qty)
    pnl = payoff - entry_cost - fee - friction
    return SettlementResult(
        side=normalized,
        qty=float(qty),
        entry_price=float(entry_price),
        pm_up=bool(pm_up),
        win=win,
        payoff=payoff,
        entry_cost=entry_cost,
        entry_fee=fee,
        friction=friction,
        pnl=pnl,
        settlement_source="pm",
        fee_source=fee_source,
    )
