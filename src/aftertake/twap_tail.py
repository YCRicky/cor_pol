"""Causal pre-close gate for Polymarket 30-second crypto TWAP markets.

The Polymarket outcome is resolved by its published oracle/TWAP rule, not by
Binance. Binance USD-M Futures is collected for diagnostics only; it does not
select the side or block an otherwise-qualified PM decision.

This module has no network or execution side effects.  All observations are
filtered ``as_of`` the decision timestamp so an offline replay cannot leak a
later tick into an earlier decision.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional, Sequence

from .post_close import PostCloseDecision

STRATEGY_VERSION = "aftertake_twap_pm_tail_v3"
TWAP_CUTOVER_TS = 1_786_060_800
SUPPORTED_ASSETS = ("BTC", "ETH", "SOL", "XRP", "BNB", "DOGE")


@dataclass(frozen=True)
class TailRuleConfig:
    """Explicit, fail-closed thresholds for the live tail entry."""

    decision_lead_s: float = 10.0
    max_decision_lateness_s: float = 1.0
    leader_bid_threshold: float = 0.90
    pm_quote_max_age_s: float = 2.0
    entry_limit_price: float = 0.99
    strategy_version: str = STRATEGY_VERSION

    def validate(self) -> None:
        if not 0 < self.decision_lead_s < 30:
            raise ValueError("tail decision lead must be in (0, 30) seconds")
        if not 0 <= self.max_decision_lateness_s <= 1:
            raise ValueError("tail maximum decision lateness must be in [0, 1] seconds")
        if not 0 < self.leader_bid_threshold < 1:
            raise ValueError("tail leader bid threshold must be in (0, 1)")
        if not 0 < self.pm_quote_max_age_s <= 5:
            raise ValueError("tail PM quote maximum age must be in (0, 5] seconds")
        if not 0 < self.entry_limit_price < 1:
            raise ValueError("tail entry limit price must be in (0, 1)")


@dataclass(frozen=True)
class PMQuote:
    """One locally-received paired CLOB top-of-book observation."""

    observed_at: float
    yes_bid: Optional[float]
    no_bid: Optional[float]
    yes_ask: Optional[float]
    no_ask: Optional[float]
    yes_ask_size: float = 0.0
    no_ask_size: float = 0.0


@dataclass(frozen=True)
class BinanceTrade:
    """A Binance USD-M Futures aggregate trade with source and local timestamps."""

    trade_ms: int
    received_ms: int
    price: float


@dataclass(frozen=True)
class BinanceTailInput:
    """Bounded, continuously collected Futures tape for one five-minute round."""

    asset: str
    round_start_ms: int
    complete_coverage: bool
    trades: Sequence[BinanceTrade]
    invalid_reason: str = ""


def _finite(value: Optional[float], *, low: float = 0.0, high: float = float("inf")) -> bool:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(number) and low < number < high


def _side_for_change(change: float) -> str:
    return "YES" if change > 0 else "NO" if change < 0 else ""


def _price_change_bps(start: float, end: float) -> float:
    return (float(end) - float(start)) / float(start) * 10_000.0


def _latest_at_or_before(trades: Sequence[BinanceTrade], at_ms: int) -> Optional[BinanceTrade]:
    eligible = [trade for trade in trades if trade.trade_ms <= at_ms and trade.received_ms <= at_ms]
    return max(eligible, key=lambda trade: (trade.trade_ms, trade.received_ms), default=None)


def _adverse_reversal_bps(trades: Sequence[BinanceTrade], side: str, start_ms: int, end_ms: int) -> Optional[float]:
    path = [
        trade.price
        for trade in trades
        if start_ms <= trade.trade_ms <= end_ms and trade.received_ms <= end_ms and _finite(trade.price)
    ]
    if not path:
        return None
    final_price = path[-1]
    if side == "YES":
        peak = max(path)
        return max(0.0, (peak - final_price) / peak * 10_000.0)
    trough = min(path)
    return max(0.0, (final_price - trough) / trough * 10_000.0)


def _hold(reason: str, audit: Dict[str, Any], *, side: str = "") -> PostCloseDecision:
    return PostCloseDecision("hold", reason, side=side, audit=audit)


def evaluate_tail_decision(
    *,
    quotes: Iterable[PMQuote],
    binance: Optional[BinanceTailInput],
    round_end_ts: float,
    decision_ts: float,
    config: Optional[TailRuleConfig] = None,
) -> PostCloseDecision:
    """Evaluate a single pre-close tail decision without future information.

    ``decision_ts`` must be near ``round_end - decision_lead``.  The CLOB
    leader is the candidate side. Binance Futures is recorded for diagnostics
    only and never blocks or selects the PM side.
    """

    cfg = config or TailRuleConfig()
    cfg.validate()
    round_end = float(round_end_ts)
    target_ts = round_end - cfg.decision_lead_s
    now = float(decision_ts)
    audit: Dict[str, Any] = {
        "strategy_version": cfg.strategy_version,
        "round_end_ts": round_end,
        "tail_decision_target_ts": target_ts,
        "decision_ts": now,
        "decision_cutoff_ts": target_ts + cfg.max_decision_lateness_s,
        "leader_bid_threshold": cfg.leader_bid_threshold,
        "pm_quote_max_age_s": cfg.pm_quote_max_age_s,
        "binance_source": "usd_m_futures_aggTrade_observational_only",
        "binance_gate_applied": False,
    }
    if now < target_ts:
        return _hold("tail_decision_not_due", audit)
    if now > target_ts + cfg.max_decision_lateness_s:
        return _hold("tail_decision_too_late", audit)
    if binance is None:
        audit.update({"binance_complete_coverage": False, "binance_invalid_reason": "proxy_missing"})
    else:
        audit.update(
            {
                "binance_complete_coverage": bool(binance.complete_coverage),
                "binance_invalid_reason": binance.invalid_reason,
                "binance_observed_trade_count": len(binance.trades),
            }
        )

    eligible_quotes = [quote for quote in quotes if _finite(quote.observed_at) and quote.observed_at <= now]
    quote = max(eligible_quotes, key=lambda item: item.observed_at, default=None)
    audit["pm_quote_count_at_decision"] = len(eligible_quotes)
    if quote is None:
        return _hold("tail_no_paired_pm_quote", audit)
    quote_age = now - quote.observed_at
    audit.update(
        {
            "pm_quote_observed_ts": quote.observed_at,
            "pm_quote_age_ms": max(0.0, quote_age * 1000.0),
            "yes_bid": quote.yes_bid,
            "no_bid": quote.no_bid,
            "yes_ask": quote.yes_ask,
            "no_ask": quote.no_ask,
        }
    )
    if quote_age < 0 or quote_age > cfg.pm_quote_max_age_s:
        return _hold("tail_pm_quote_stale", audit)

    yes_valid = _finite(quote.yes_bid, high=1.0)
    no_valid = _finite(quote.no_bid, high=1.0)
    if not yes_valid and not no_valid:
        return _hold("tail_pm_bid_missing_or_invalid", audit)
    if yes_valid and no_valid and float(quote.yes_bid) == float(quote.no_bid):
        return _hold("tail_pm_bid_tie", audit)
    side = "YES" if yes_valid and (not no_valid or float(quote.yes_bid) > float(quote.no_bid)) else "NO"
    leader_bid = float(quote.yes_bid if side == "YES" else quote.no_bid)
    entry_ask = quote.yes_ask if side == "YES" else quote.no_ask
    entry_ask_size = float(quote.yes_ask_size if side == "YES" else quote.no_ask_size)
    audit.update({"selected_side": side, "leader_bid": leader_bid, "entry_ask_size": entry_ask_size})
    if leader_bid <= cfg.leader_bid_threshold:
        return _hold("tail_pm_leader_bid_not_strictly_above_threshold", audit, side=side)
    if not _finite(entry_ask, high=1.0) or float(entry_ask) > cfg.entry_limit_price:
        return _hold("tail_pm_entry_ask_not_marketable_within_cap", audit, side=side)

    return PostCloseDecision(
        "enter",
        "tail_pm_twap_qualified",
        side=side,
        entry_ask=cfg.entry_limit_price,
        entry_ask_size=entry_ask_size,
        winner_bid=leader_bid,
        loser_bid=float(quote.no_bid if side == "YES" else quote.yes_bid)
        if _finite(quote.no_bid if side == "YES" else quote.yes_bid, high=1.0)
        else None,
        audit=audit,
    )


def twap_market_gate(asset: str, market_raw: Optional[Dict[str, Any]], round_start: int) -> Optional[str]:
    """Return a fail-closed reason when Gamma metadata is not a 30s TWAP market."""

    normalized = str(asset).upper().strip()
    if normalized not in SUPPORTED_ASSETS:
        return "tail_asset_not_supported"
    if int(round_start) < TWAP_CUTOVER_TS:
        return "tail_market_before_twap_cutover"
    raw = market_raw or {}
    config = raw.get("cryptoMarketConfig") if isinstance(raw, dict) else None
    if not isinstance(config, dict):
        return "tail_gamma_twap_metadata_missing"
    if config.get("twapEnabled") is not True:
        return "tail_gamma_twap_not_enabled"
    try:
        lookback = int(config.get("twapLookbackSeconds"))
    except (TypeError, ValueError):
        return "tail_gamma_twap_lookback_missing"
    if lookback != 30:
        return "tail_gamma_twap_lookback_not_30s"
    return None
