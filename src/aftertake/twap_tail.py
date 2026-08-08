"""Causal pre-close gate for Polymarket 30-second crypto TWAP markets.

The Polymarket outcome is resolved by its published oracle/TWAP rule, not by
Binance.  Binance USD-M Futures is used here only as a fast, public *path filter*: if a
small five-minute move is already reversing in the final thirty seconds, the
runner stands down rather than treating a stale CLOB leader as a certainty.

This module has no network or execution side effects.  All observations are
filtered ``as_of`` the decision timestamp so an offline replay cannot leak a
later tick into an earlier decision.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional, Sequence

from .post_close import PostCloseDecision

STRATEGY_VERSION = "aftertake_twap_price_path_tail_v2"
TWAP_CUTOVER_TS = 1_786_060_800
SUPPORTED_ASSETS = ("BTC", "ETH", "SOL", "XRP", "BNB", "DOGE")


@dataclass(frozen=True)
class TailRuleConfig:
    """Explicit, fail-closed thresholds for the live tail entry."""

    decision_lead_s: float = 10.25
    max_decision_lateness_s: float = 0.25
    leader_bid_threshold: float = 0.90
    pm_quote_max_age_s: float = 2.0
    binance_max_trade_age_s: float = 2.0
    weak_candle_abs_move_bps: float = 5.0
    weak_path_reversal_bps: float = 2.0
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
        if not 0 < self.binance_max_trade_age_s <= 5:
            raise ValueError("tail Binance trade maximum age must be in (0, 5] seconds")
        if self.weak_candle_abs_move_bps <= 0:
            raise ValueError("tail weak candle threshold must be > 0 bps")
        if self.weak_path_reversal_bps < 0:
            raise ValueError("tail reversal threshold must be >= 0 bps")
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
    binance: BinanceTailInput,
    round_end_ts: float,
    decision_ts: float,
    config: Optional[TailRuleConfig] = None,
) -> PostCloseDecision:
    """Evaluate a single pre-close tail decision without future information.

    ``decision_ts`` must be near ``round_end - decision_lead``.  The CLOB
    leader is the candidate side.  The complete Binance Futures tape only filters
    obvious weak-candle reversals; it never replaces PM's resolution oracle.
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
        "binance_max_trade_age_s": cfg.binance_max_trade_age_s,
        "weak_candle_abs_move_bps": cfg.weak_candle_abs_move_bps,
        "weak_path_reversal_bps": cfg.weak_path_reversal_bps,
        "binance_source": "usd_m_futures_aggTrade_path_filter_not_settlement_oracle",
    }
    if now < target_ts:
        return _hold("tail_decision_not_due", audit)
    if now > target_ts + cfg.max_decision_lateness_s:
        return _hold("tail_decision_too_late", audit)
    if not binance.complete_coverage:
        audit["binance_invalid_reason"] = binance.invalid_reason or "incomplete_coverage"
        return _hold("binance_futures_coverage_incomplete", audit)

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

    as_of_ms = int(now * 1000)
    start_ms = int(binance.round_start_ms)
    causal_trades = tuple(
        trade
        for trade in binance.trades
        if start_ms <= trade.trade_ms <= as_of_ms
        and trade.received_ms <= as_of_ms
        and _finite(trade.price)
    )
    first = min(causal_trades, key=lambda trade: (trade.trade_ms, trade.received_ms), default=None)
    latest = _latest_at_or_before(causal_trades, as_of_ms)
    audit["binance_causal_trade_count"] = len(causal_trades)
    if first is None or latest is None:
        return _hold("tail_binance_futures_tape_missing", audit, side=side)
    latest_age_s = (as_of_ms - latest.received_ms) / 1000.0
    audit.update(
        {
            "binance_open_price": first.price,
            "binance_latest_price": latest.price,
            "binance_latest_trade_ms": latest.trade_ms,
            "binance_latest_received_ms": latest.received_ms,
            "binance_latest_age_ms": max(0.0, latest_age_s * 1000.0),
        }
    )
    if latest_age_s < 0 or latest_age_s > cfg.binance_max_trade_age_s:
        return _hold("tail_binance_futures_tape_stale", audit, side=side)

    candle_bps = _price_change_bps(first.price, latest.price)
    candle_side = _side_for_change(candle_bps)
    audit.update({"binance_candle_move_bps": candle_bps, "binance_candle_side": candle_side})
    if not candle_side:
        return _hold("tail_binance_candle_flat", audit, side=side)
    if candle_side != side:
        return _hold("tail_binance_candle_direction_mismatch", audit, side=side)

    if abs(candle_bps) > cfg.weak_candle_abs_move_bps:
        audit["path_gate"] = "strong_candle_direction_only"
    else:
        window30 = _latest_at_or_before(causal_trades, int((round_end - 30.0) * 1000))
        window20 = _latest_at_or_before(causal_trades, int((round_end - 20.0) * 1000))
        if window30 is None or window20 is None:
            return _hold("tail_binance_path_anchor_missing", audit, side=side)
        move30_bps = _price_change_bps(window30.price, latest.price)
        move20_bps = _price_change_bps(window20.price, latest.price)
        move30_side = _side_for_change(move30_bps)
        move20_side = _side_for_change(move20_bps)
        reversal_bps = _adverse_reversal_bps(
            causal_trades, side, int((round_end - 30.0) * 1000), as_of_ms
        )
        audit.update(
            {
                "path_gate": "weak_candle_30s_20s_direction_and_reversal",
                "binance_move_30s_to_decision_bps": move30_bps,
                "binance_move_20s_to_decision_bps": move20_bps,
                "binance_move_30s_side": move30_side,
                "binance_move_20s_side": move20_side,
                "binance_adverse_reversal_bps": reversal_bps,
            }
        )
        if move30_side != side or move20_side != side:
            return _hold("tail_weak_candle_path_direction_mismatch", audit, side=side)
        if reversal_bps is None or reversal_bps > cfg.weak_path_reversal_bps:
            return _hold("tail_weak_candle_reversal_too_large", audit, side=side)

    return PostCloseDecision(
        "enter",
        "tail_twap_price_path_qualified",
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
