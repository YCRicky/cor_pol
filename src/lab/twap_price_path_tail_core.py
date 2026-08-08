"""Pure decision rule for the 30-second-TWAP price-path tail strategy.

The rule deliberately separates the observed Binance Spot path from the
Polymarket/Chainlink settlement source.  Binance is a veto feature only; the
caller must account and label resolved positions from Polymarket.
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from typing import Optional, Sequence


@dataclass(frozen=True)
class TailRuleConfig:
    """Parameters selected by the post-cutover chronological replay."""

    min_leader_bid: float = 0.90
    max_pm_quote_age_s: float = 2.0
    max_binance_trade_age_s: float = 2.0
    weak_candle_max_bp: float = 5.0
    weak_adverse_cap_bp: float = 2.0
    strong_last10_tolerance_bp: Optional[float] = None
    strong_adverse_cap_bp: Optional[float] = None


@dataclass(frozen=True)
class PMQuote:
    """A locally received best bid/ask state for one PM outcome token."""

    received_ts: float
    best_bid: Optional[float]
    best_ask: Optional[float] = None
    bid_size: Optional[float] = None
    ask_size: Optional[float] = None


@dataclass(frozen=True)
class BinanceTrade:
    """One Binance Spot aggregate trade, with exchange and local receipt time."""

    trade_ts: float
    price: float
    received_ts: float
    trade_id: int = 0


@dataclass(frozen=True)
class TailDecision:
    eligible: bool
    reason: str
    side: Optional[str] = None
    leader_bid: Optional[float] = None
    pm_quote_age_ms: Optional[float] = None
    binance_last_trade_age_ms: Optional[float] = None
    candle_open: Optional[float] = None
    tail_start_price: Optional[float] = None
    tminus20_price: Optional[float] = None
    decision_price: Optional[float] = None
    signed_candle_bp: Optional[float] = None
    signed_net20_bp: Optional[float] = None
    signed_last10_bp: Optional[float] = None
    adverse_end_reversal_bp: Optional[float] = None
    binance_tail_trade_count: int = 0

    def to_record(self) -> dict:
        return asdict(self)


def _finite(value: object) -> Optional[float]:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def basis_points(numerator: float, denominator: float) -> float:
    return 10_000.0 * (numerator / denominator - 1.0)


def quote_at_or_before(quotes: Sequence[PMQuote], cutoff_ts: float) -> Optional[PMQuote]:
    """Return the latest locally received quote visible at the decision cutoff."""

    visible = [quote for quote in quotes if quote.received_ts <= cutoff_ts]
    return max(visible, key=lambda quote: quote.received_ts) if visible else None


def _visible_trades(trades: Sequence[BinanceTrade], cutoff_ts: float) -> list[BinanceTrade]:
    """Use only trades both exchanged and locally received by the cutoff.

    This prevents a late websocket packet from silently changing an already
    frozen decision, while keeping Binance's exchange trade timestamp as the
    freshness clock.
    """

    result = [
        trade
        for trade in trades
        if trade.trade_ts <= cutoff_ts and trade.received_ts <= cutoff_ts and _finite(trade.price)
    ]
    result.sort(key=lambda trade: (trade.trade_ts, trade.trade_id, trade.received_ts))
    return result


def _latest_at_or_before(trades: Sequence[BinanceTrade], ts: float) -> Optional[BinanceTrade]:
    candidates = [trade for trade in trades if trade.trade_ts <= ts]
    return candidates[-1] if candidates else None


def evaluate_tail_decision(
    *,
    end_ts: float,
    decision_ts: float,
    candle_open: Optional[float],
    yes_quotes: Sequence[PMQuote],
    no_quotes: Sequence[PMQuote],
    binance_trades: Sequence[BinanceTrade],
    config: TailRuleConfig = TailRuleConfig(),
) -> TailDecision:
    """Evaluate the replayed price-path rule at one fixed decision cutoff.

    ``decision_ts`` is normally ``end_ts - 10.25``: ten seconds before the
    scheduled market end plus the 250 ms information safety buffer used by the
    replay.  The last-ten-second feature intentionally anchors at ``E - 20``
    so the live calculation matches the historical implementation exactly.
    """

    yes = quote_at_or_before(yes_quotes, decision_ts)
    no = quote_at_or_before(no_quotes, decision_ts)
    if yes is None or no is None:
        return TailDecision(False, "missing_pm_quote")
    yes_bid = _finite(yes.best_bid)
    no_bid = _finite(no.best_bid)
    if yes_bid is None or no_bid is None:
        return TailDecision(False, "missing_pm_best_bid")
    if yes_bid == no_bid:
        return TailDecision(False, "tied_pm_leader")

    side, quote, leader_bid = ("YES", yes, yes_bid) if yes_bid > no_bid else ("NO", no, no_bid)
    quote_age_ms = 1000.0 * (decision_ts - quote.received_ts)
    if quote_age_ms < 0.0:
        return TailDecision(False, "pm_quote_after_cutoff", side, leader_bid, quote_age_ms)
    if leader_bid < config.min_leader_bid:
        return TailDecision(False, "pm_bid_below_threshold", side, leader_bid, quote_age_ms)
    if quote_age_ms > config.max_pm_quote_age_s * 1000.0:
        return TailDecision(False, "stale_pm_quote", side, leader_bid, quote_age_ms)

    open_price = _finite(candle_open)
    if open_price is None or open_price <= 0.0:
        return TailDecision(False, "missing_binance_candle_open", side, leader_bid, quote_age_ms)

    visible = _visible_trades(binance_trades, decision_ts)
    tail_start_ts = end_ts - 30.0
    tminus20_ts = end_ts - 20.0
    anchor = _latest_at_or_before(visible, tail_start_ts)
    if anchor is None:
        return TailDecision(False, "missing_binance_tail_anchor", side, leader_bid, quote_age_ms, candle_open=open_price)
    tminus20 = _latest_at_or_before(visible, tminus20_ts) or anchor
    last_trade = visible[-1] if visible else anchor
    last_trade_age_ms = 1000.0 * (decision_ts - last_trade.trade_ts)
    if last_trade_age_ms < 0.0:
        return TailDecision(
            False, "binance_trade_after_cutoff", side, leader_bid, quote_age_ms, last_trade_age_ms, open_price,
        )
    if last_trade_age_ms > config.max_binance_trade_age_s * 1000.0:
        return TailDecision(
            False, "stale_binance_trade", side, leader_bid, quote_age_ms, last_trade_age_ms, open_price,
        )

    direction = 1.0 if side == "YES" else -1.0
    tail = [trade for trade in visible if tail_start_ts < trade.trade_ts <= decision_ts]
    prices = [anchor.price, *(trade.price for trade in tail)]
    p30 = anchor.price
    p20 = tminus20.price
    decision_price = last_trade.price
    signed_candle_bp = direction * basis_points(decision_price, open_price)
    signed_net20_bp = direction * basis_points(decision_price, p30)
    signed_last10_bp = direction * basis_points(decision_price, p20)
    adverse_end_reversal_bp = (
        basis_points(max(prices), decision_price)
        if direction > 0.0
        else basis_points(decision_price, min(prices))
    )
    details = dict(
        side=side,
        leader_bid=leader_bid,
        pm_quote_age_ms=quote_age_ms,
        binance_last_trade_age_ms=last_trade_age_ms,
        candle_open=open_price,
        tail_start_price=p30,
        tminus20_price=p20,
        decision_price=decision_price,
        signed_candle_bp=signed_candle_bp,
        signed_net20_bp=signed_net20_bp,
        signed_last10_bp=signed_last10_bp,
        adverse_end_reversal_bp=adverse_end_reversal_bp,
        binance_tail_trade_count=len(tail),
    )
    if signed_candle_bp <= 0.0:
        return TailDecision(False, "binance_candle_opposes_pm_side", **details)
    if signed_candle_bp <= config.weak_candle_max_bp:
        if signed_net20_bp <= 0.0:
            return TailDecision(False, "weak_net_tail_reversal", **details)
        if signed_last10_bp <= 0.0:
            return TailDecision(False, "weak_last10_reversal", **details)
        if adverse_end_reversal_bp > config.weak_adverse_cap_bp:
            return TailDecision(False, "weak_end_reversal", **details)
        return TailDecision(True, "weak_pass", **details)
    if (
        config.strong_last10_tolerance_bp is not None
        and signed_last10_bp < -float(config.strong_last10_tolerance_bp)
    ):
        return TailDecision(False, "strong_last10_reversal", **details)
    if (
        config.strong_adverse_cap_bp is not None
        and adverse_end_reversal_bp > float(config.strong_adverse_cap_bp)
    ):
        return TailDecision(False, "strong_end_reversal", **details)
    return TailDecision(True, "strong_pass", **details)
