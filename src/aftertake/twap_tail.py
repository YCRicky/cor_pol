"""Replay-parity gate for Polymarket 30-second crypto TWAP markets.

This module implements the *candidate-side rule* used by the recorded
443/443 research replay.  It freezes all strategy inputs at ``E - 10.25s``:

* PM's locally received best-bid leader must be at least 0.90;
* Binance **Spot** 5m candle open and aggregate-trade path may veto that side;
* a weak (<= 5bp) signed candle needs a non-reversing final 30/20-second path
  and an adverse endpoint reversal no greater than 2bp.

Polymarket's published oracle/TWAP rule remains the settlement source.
Binance is only a causal proxy risk filter.  Executability, fees, account
limits, and order reconciliation are deliberately handled by ``runner.py``
after this candidate gate, so they cannot be mistaken for the 443/443 label
replay.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence, Tuple

from .post_close import PostCloseDecision


STRATEGY_VERSION = "aftertake_twap_price_path_tail_replay_parity_v1"
TWAP_CUTOVER_TS = 1_786_060_800
SUPPORTED_ASSETS = ("BTC", "ETH", "SOL", "XRP", "BNB", "DOGE")


@dataclass(frozen=True)
class TailRuleConfig:
    """Fixed parameters of the recorded global 443/443 research rule."""

    # The replay reads local PM state through E-10s-250ms.  The runner may
    # submit shortly afterwards, but features are always frozen at this target.
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
        if not 0 <= self.max_decision_lateness_s <= 0.25:
            raise ValueError("tail maximum decision lateness must be in [0, 0.25] seconds")
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
    """One locally received paired CLOB top-of-book observation."""

    observed_at: float
    yes_bid: Optional[float]
    no_bid: Optional[float]
    yes_ask: Optional[float]
    no_ask: Optional[float]
    yes_ask_size: float = 0.0
    no_ask_size: float = 0.0


@dataclass(frozen=True)
class BinanceTrade:
    """A Binance Spot aggregate trade with exchange and local timestamps."""

    trade_ms: int
    received_ms: int
    price: float


@dataclass(frozen=True)
class BinanceTailInput:
    """Bounded, continuously collected Spot tape for one five-minute candle."""

    asset: str
    round_start_ms: int
    complete_coverage: bool
    trades: Sequence[BinanceTrade]
    invalid_reason: str = ""
    # The official Spot kline open, captured causally from ``@kline_5m``.
    # It is required because the research replay used the 5m kline open, not
    # the first aggregate trade observed by this process.
    candle_open_price: Optional[float] = None
    candle_open_received_ms: Optional[int] = None


def _finite(value: Optional[float], *, low: float = 0.0, high: float = float("inf")) -> bool:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(number) and low < number < high


def _price_change_bps(start: float, end: float) -> float:
    return (float(end) - float(start)) / float(start) * 10_000.0


def _side_sign(side: str) -> Optional[float]:
    normalized = str(side).upper().strip()
    if normalized == "YES":
        return 1.0
    if normalized == "NO":
        return -1.0
    return None


def _latest_at_or_before(trades: Sequence[BinanceTrade], at_ms: int) -> Optional[BinanceTrade]:
    eligible = [trade for trade in trades if trade.trade_ms <= at_ms and trade.received_ms <= at_ms]
    return max(eligible, key=lambda trade: (trade.trade_ms, trade.received_ms), default=None)


def _adverse_reversal_bps(side: str, prices: Sequence[float]) -> Optional[float]:
    if not prices:
        return None
    final_price = float(prices[-1])
    if side == "YES":
        peak = max(float(price) for price in prices)
        return max(0.0, (peak - final_price) / peak * 10_000.0)
    trough = min(float(price) for price in prices)
    return max(0.0, (final_price - trough) / trough * 10_000.0)


def _hold(reason: str, audit: Dict[str, Any], *, side: str = "") -> PostCloseDecision:
    return PostCloseDecision("hold", reason, side=side, audit=audit)


def _path_gate(
    *,
    side: str,
    signed_candle_bps: float,
    signed_net20_bps: float,
    signed_last10_bps: float,
    adverse_reversal_bps: Optional[float],
    config: TailRuleConfig,
) -> Tuple[bool, str]:
    """The exact path rule shared by live input and cached replay features."""

    if not all(math.isfinite(float(value)) for value in (signed_candle_bps, signed_net20_bps, signed_last10_bps)):
        return False, "invalid_binance_path"
    if signed_candle_bps <= 0.0:
        return False, "binance_candle_opposes_pm_side"
    if signed_candle_bps > config.weak_candle_abs_move_bps:
        # The selected 443/443 global rule had no extra strong-candle veto.
        return True, "strong_pass"
    if signed_net20_bps <= 0.0:
        return False, "weak_net_tail_reversal"
    if signed_last10_bps <= 0.0:
        return False, "weak_last10_reversal"
    if adverse_reversal_bps is None or not math.isfinite(float(adverse_reversal_bps)):
        return False, "weak_reversal_unavailable"
    if float(adverse_reversal_bps) > config.weak_path_reversal_bps:
        return False, "weak_end_reversal"
    return True, "weak_pass"


def replay_feature_decision(
    row: Mapping[str, Any], *, config: Optional[TailRuleConfig] = None
) -> Tuple[bool, str]:
    """Evaluate one cached research feature row with the production path rule.

    This helper has no PM/UMA outcome input and therefore cannot leak the
    outcome label.  ``scripts/verify_twap_replay_parity.py`` uses it to prove
    that the committed candidate gate selects the recorded 443 rows.
    """

    cfg = config or TailRuleConfig()
    cfg.validate()
    try:
        side = str(row["pm_side"]).upper().strip()
        leader_bid = float(row["leader_bid"])
        pm_age_ms = int(row["leader_quote_age_ms"])
        binance_age_ms = int(row["binance_last_trade_age_ms"])
        signed_candle_bps = float(row["signed_candle_bp"])
        signed_net20_bps = float(row["signed_net20_bp"])
        signed_last10_bps = float(row["signed_last10_bp"])
        adverse_reversal_bps = float(row["adverse_end_reversal_bp"])
    except (KeyError, TypeError, ValueError):
        return False, "invalid_replay_feature"
    if _side_sign(side) is None:
        return False, "invalid_pm_side"
    # The report explicitly used >= 0.90, not a strict > comparison.
    if leader_bid < cfg.leader_bid_threshold:
        return False, "pm_bid_below_threshold"
    if pm_age_ms > int(cfg.pm_quote_max_age_s * 1000):
        return False, "stale_pm_quote"
    if binance_age_ms > int(cfg.binance_max_trade_age_s * 1000):
        return False, "stale_binance_trade"
    return _path_gate(
        side=side,
        signed_candle_bps=signed_candle_bps,
        signed_net20_bps=signed_net20_bps,
        signed_last10_bps=signed_last10_bps,
        adverse_reversal_bps=adverse_reversal_bps,
        config=cfg,
    )


def evaluate_tail_decision(
    *,
    quotes: Iterable[PMQuote],
    binance: Optional[BinanceTailInput],
    round_end_ts: float,
    decision_ts: float,
    config: Optional[TailRuleConfig] = None,
) -> PostCloseDecision:
    """Evaluate one live candidate using only data frozen at ``E - 10.25s``.

    ``decision_ts`` is the real scheduler time.  It can be up to 250ms later
    than the target, but the quote and Spot tape are always evaluated at the
    target itself, exactly as in the labelled replay.
    """

    cfg = config or TailRuleConfig()
    cfg.validate()
    round_end = float(round_end_ts)
    target_ts = round_end - cfg.decision_lead_s
    now = float(decision_ts)
    as_of_ms = int(target_ts * 1000)
    audit: Dict[str, Any] = {
        "strategy_version": cfg.strategy_version,
        "round_end_ts": round_end,
        "tail_feature_cutoff_ts": target_ts,
        "tail_decision_target_ts": target_ts,
        "decision_ts": now,
        "decision_cutoff_ts": target_ts + cfg.max_decision_lateness_s,
        "leader_bid_threshold": cfg.leader_bid_threshold,
        "leader_bid_comparison": "greater_than_or_equal",
        "pm_quote_max_age_s": cfg.pm_quote_max_age_s,
        "binance_max_trade_age_s": cfg.binance_max_trade_age_s,
        "weak_candle_abs_move_bps": cfg.weak_candle_abs_move_bps,
        "weak_path_reversal_bps": cfg.weak_path_reversal_bps,
        "binance_source": "spot_kline_open_plus_aggTrade_path_filter_not_settlement_oracle",
    }
    if now < target_ts:
        return _hold("tail_decision_not_due", audit)
    if now > target_ts + cfg.max_decision_lateness_s:
        return _hold("tail_decision_too_late", audit)
    if binance is None:
        return _hold("binance_spot_proxy_missing", audit)
    if not binance.complete_coverage:
        audit["binance_invalid_reason"] = binance.invalid_reason or "incomplete_coverage"
        return _hold("binance_spot_coverage_incomplete", audit)

    eligible_quotes = [quote for quote in quotes if _finite(quote.observed_at) and quote.observed_at <= target_ts]
    quote = max(eligible_quotes, key=lambda item: item.observed_at, default=None)
    audit["pm_quote_count_at_feature_cutoff"] = len(eligible_quotes)
    if quote is None:
        return _hold("tail_no_paired_pm_quote", audit)
    quote_age = target_ts - quote.observed_at
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
    audit.update({"selected_side": side, "leader_bid": leader_bid, "entry_ask": entry_ask, "entry_ask_size": entry_ask_size})
    if leader_bid < cfg.leader_bid_threshold:
        return _hold("tail_pm_leader_bid_below_threshold", audit, side=side)

    if not _finite(binance.candle_open_price):
        return _hold("tail_binance_spot_kline_open_missing", audit, side=side)
    if binance.candle_open_received_ms is not None and int(binance.candle_open_received_ms) > as_of_ms:
        return _hold("tail_binance_spot_kline_open_not_causal", audit, side=side)

    start_ms = int(binance.round_start_ms)
    causal_trades = tuple(
        sorted(
            (
                trade
                for trade in binance.trades
                if start_ms <= trade.trade_ms <= as_of_ms
                and trade.received_ms <= as_of_ms
                and _finite(trade.price)
            ),
            key=lambda trade: (trade.trade_ms, trade.received_ms),
        )
    )
    latest = _latest_at_or_before(causal_trades, as_of_ms)
    anchor30 = _latest_at_or_before(causal_trades, int((round_end - 30.0) * 1000))
    anchor20 = _latest_at_or_before(causal_trades, int((round_end - 20.0) * 1000))
    audit["binance_causal_trade_count"] = len(causal_trades)
    if latest is None or anchor30 is None or anchor20 is None:
        return _hold("tail_binance_spot_path_anchor_missing", audit, side=side)
    latest_age_s = (as_of_ms - latest.trade_ms) / 1000.0
    tail_prices = [anchor30.price] + [
        trade.price
        for trade in causal_trades
        if int((round_end - 30.0) * 1000) < trade.trade_ms <= as_of_ms
    ]
    sign = _side_sign(side)
    assert sign is not None
    signed_candle_bps = sign * _price_change_bps(float(binance.candle_open_price), latest.price)
    signed_net20_bps = sign * _price_change_bps(anchor30.price, latest.price)
    signed_last10_bps = sign * _price_change_bps(anchor20.price, latest.price)
    adverse_reversal = _adverse_reversal_bps(side, tail_prices)
    audit.update(
        {
            "binance_candle_open_price": binance.candle_open_price,
            "binance_candle_open_received_ms": binance.candle_open_received_ms,
            "binance_tail_start_price": anchor30.price,
            "binance_tminus20_price": anchor20.price,
            "binance_latest_price": latest.price,
            "binance_latest_trade_ms": latest.trade_ms,
            "binance_latest_received_ms": latest.received_ms,
            "binance_latest_age_ms": max(0.0, latest_age_s * 1000.0),
            "signed_candle_bp": signed_candle_bps,
            "signed_net20_bp": signed_net20_bps,
            "signed_last10_bp": signed_last10_bps,
            "adverse_end_reversal_bp": adverse_reversal,
        }
    )
    if latest_age_s < 0 or latest_age_s > cfg.binance_max_trade_age_s:
        return _hold("tail_binance_spot_tape_stale", audit, side=side)
    accepted, reason = _path_gate(
        side=side,
        signed_candle_bps=signed_candle_bps,
        signed_net20_bps=signed_net20_bps,
        signed_last10_bps=signed_last10_bps,
        adverse_reversal_bps=adverse_reversal,
        config=cfg,
    )
    audit["path_gate"] = reason
    if not accepted:
        return _hold(f"tail_{reason}", audit, side=side)

    return PostCloseDecision(
        "enter",
        "tail_twap_price_path_replay_parity_qualified",
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
    market_config = raw.get("cryptoMarketConfig") if isinstance(raw, dict) else None
    if not isinstance(market_config, dict):
        return "tail_gamma_twap_metadata_missing"
    if market_config.get("twapEnabled") is not True:
        return "tail_gamma_twap_not_enabled"
    try:
        lookback = int(market_config.get("twapLookbackSeconds"))
    except (TypeError, ValueError):
        return "tail_gamma_twap_lookback_missing"
    if lookback != 30:
        return "tail_gamma_twap_lookback_not_30s"
    return None
