"""Live CLOB runner for the post-2026-08-07 30-second-TWAP tail strategy.

The signal is frozen at E-10.25s from PM best bids plus Binance Spot aggregate
trades.  A positive signal is then sent through the repository's established
share-sized marketable-GTC/cancel/reconcile executor when ``DRY_RUN=false``.
No Binance value is ever used as a settlement label.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import sys
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Deque, Optional

import websockets

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
for item in (str(ROOT), str(SRC)):
    if item not in sys.path:
        sys.path.insert(0, item)

from common import (  # noqa: E402
    discover_current_market,
    fetch_market_by_slug,
    get_json,
    iso_to_ts,
    parse_clob_token_ids,
    parse_outcomes,
)
from execution import ExecutionLegResult, LiveExecutionConfig, PolymarketLiveExecutor  # noqa: E402
from lab.correlation_arb_bot import (  # noqa: E402
    GateConfig,
    UserOrderFeed,
    _build_notifier,
    _envb,
    _envf,
    _envi,
    _log,
    confirm_live_result,
    execution_unknown,
    execution_unknown_reason,
)
from lab.twap_price_path_tail_core import (  # noqa: E402
    BinanceTrade,
    PMQuote,
    TailDecision,
    TailRuleConfig,
    evaluate_tail_decision,
)


STRATEGY_VERSION = "twap_price_path_tail_v2"
TWAP_CUTOVER_TS = 1_786_060_800  # 2026-08-07T00:00:00Z
PM_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
ASSET_SYMBOLS = {
    "BTC": "BTCUSDT",
    "ETH": "ETHUSDT",
    "SOL": "SOLUSDT",
    "XRP": "XRPUSDT",
    "BNB": "BNBUSDT",
    "DOGE": "DOGEUSDT",
}
OUT = ROOT / "out" / STRATEGY_VERSION


def _finite(value: Any) -> Optional[float]:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def _normalized_book_side(value: Any) -> str:
    return "BID" if str(value or "").upper() in {"BID", "BUY"} else "ASK"


def _outcome_token_map(market: dict[str, Any]) -> tuple[str, str]:
    outcomes = parse_outcomes(market)
    token_ids = parse_clob_token_ids(market)
    mapping = dict(zip(outcomes, token_ids))
    return (
        str(mapping.get("Up") or mapping.get("Yes") or token_ids[0]),
        str(mapping.get("Down") or mapping.get("No") or token_ids[1]),
    )


class TailBook:
    """Small local CLOB book plus an as-of BBO history for the decision clock."""

    def __init__(self) -> None:
        self.bids: dict[float, float] = {}
        self.asks: dict[float, float] = {}
        self.quotes: Deque[PMQuote] = deque(maxlen=4_000)

    @staticmethod
    def _levels(rows: Any) -> dict[float, float]:
        result: dict[float, float] = {}
        if not isinstance(rows, list):
            return result
        for row in rows:
            if not isinstance(row, dict):
                continue
            price = _finite(row.get("price"))
            size = _finite(row.get("size"))
            if price is not None and size is not None and price > 0.0 and size > 0.0:
                result[price] = size
        return result

    def apply_snapshot(self, bids: Any, asks: Any, received_ts: float) -> None:
        self.bids = self._levels(bids)
        self.asks = self._levels(asks)
        self.record_quote(received_ts)

    def apply_changes(self, changes: Any, received_ts: float) -> None:
        if not isinstance(changes, list):
            return
        changed = False
        for change in changes:
            if not isinstance(change, dict):
                continue
            price = _finite(change.get("price"))
            size = _finite(change.get("size"))
            if price is None or size is None or price <= 0.0:
                continue
            book = self.bids if _normalized_book_side(change.get("side")) == "BID" else self.asks
            if size <= 0.0:
                book.pop(price, None)
            else:
                book[price] = size
            changed = True
        if changed:
            self.record_quote(received_ts)

    def best_bid(self) -> Optional[tuple[float, float]]:
        if not self.bids:
            return None
        price = max(self.bids)
        return price, self.bids[price]

    def best_ask(self) -> Optional[tuple[float, float]]:
        if not self.asks:
            return None
        price = min(self.asks)
        return price, self.asks[price]

    def record_quote(self, received_ts: float) -> None:
        bid = self.best_bid()
        ask = self.best_ask()
        if bid is None and ask is None:
            return
        self.quotes.append(PMQuote(
            received_ts=received_ts,
            best_bid=bid[0] if bid else None,
            best_ask=ask[0] if ask else None,
            bid_size=bid[1] if bid else None,
            ask_size=ask[1] if ask else None,
        ))

    def ask_depth_through(self, price_cap: float) -> float:
        return sum(size for price, size in self.asks.items() if price <= price_cap + 1e-12)


@dataclass
class TailLeg:
    asset: str
    symbol: str
    slug: str
    question: str
    start_ts: int
    end_ts: int
    yes_token: str
    no_token: str
    condition_id: str
    tick_size: str
    neg_risk: bool
    min_order_size: float
    taker_base_fee_rate: float
    yes_book: TailBook = field(default_factory=TailBook)
    no_book: TailBook = field(default_factory=TailBook)
    candle_open: Optional[float] = None
    tail_anchor: Optional[BinanceTrade] = None
    tail_trades: Deque[BinanceTrade] = field(default_factory=lambda: deque(maxlen=20_000))
    tail_trade_overflow: bool = False

    def add_trade(self, trade: BinanceTrade) -> None:
        tail_start_ts = self.end_ts - 30.0
        if trade.trade_ts <= tail_start_ts:
            if self.tail_anchor is None or (trade.trade_ts, trade.trade_id) >= (self.tail_anchor.trade_ts, self.tail_anchor.trade_id):
                self.tail_anchor = trade
        else:
            if self.tail_trades.maxlen is not None and len(self.tail_trades) >= self.tail_trades.maxlen:
                self.tail_trade_overflow = True
            self.tail_trades.append(trade)

    def decision_trades(self) -> list[BinanceTrade]:
        result = list(self.tail_trades)
        if self.tail_anchor is not None:
            result.append(self.tail_anchor)
        return result


@dataclass(frozen=True)
class TailLiveConfig:
    quantity: float
    decision_lead_s: float
    decision_grace_s: float
    price_cap: float
    min_visible_ask_qty: float
    min_net_win_per_share: float
    fee_rebate_rate: float
    execution_slippage_ticks: int
    max_entries_per_round: int
    max_cost_per_round_usd: float
    settlement_poll_s: float
    user_ws_enabled: bool
    user_ws_confirm_timeout_s: float
    rule: TailRuleConfig

    def validate(self) -> None:
        if self.quantity <= 0.0:
            raise ValueError("TAIL_QTY must be positive")
        if not (0.0 < self.price_cap < 1.0):
            raise ValueError("TAIL_PRICE_CAP must be between 0 and 1")
        if self.decision_lead_s <= 0.0 or self.decision_grace_s < 0.0:
            raise ValueError("invalid tail decision timing")
        if not (0.0 <= self.fee_rebate_rate < 1.0):
            raise ValueError("TAIL_TAKER_REBATE_RATE must be in [0, 1)")
        if self.max_entries_per_round < 1:
            raise ValueError("TAIL_MAX_ENTRIES_PER_ROUND must be positive")


@dataclass
class TailTrade:
    trade_id: str
    round_idx: int
    asset: str
    slug: str
    start_ts: int
    end_ts: int
    side: str
    qty: float
    entry_price: float
    entry_fee: float
    leader_bid: float
    decision_ts: float
    decision_reason: str
    order_id: str = ""
    execution_status: str = ""
    execution_error: str = ""
    jsonl: str = ""


@dataclass
class TailRuntimeState:
    pending: list[TailTrade] = field(default_factory=list)
    recent_attempts: list[str] = field(default_factory=list)
    settled: int = 0
    wins: int = 0
    total_pnl: float = 0.0
    total_cost: float = 0.0

    def attempted(self, key: str) -> bool:
        return key in self.recent_attempts

    def mark_attempted(self, key: str) -> None:
        if key not in self.recent_attempts:
            self.recent_attempts.append(key)
            self.recent_attempts = self.recent_attempts[-512:]

    def to_record(self) -> dict[str, Any]:
        return {
            "schema_version": "twap_price_path_tail_state.v2",
            "strategy_version": STRATEGY_VERSION,
            "pending": [asdict(trade) for trade in self.pending],
            "recent_attempts": self.recent_attempts,
            "settled": self.settled,
            "wins": self.wins,
            "total_pnl": self.total_pnl,
            "total_cost": self.total_cost,
            "updated_at": time.time(),
        }


def write_json_atomic(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(record, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temp.replace(path)


def load_state(path: Path) -> TailRuntimeState:
    if not path.exists():
        return TailRuntimeState()
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return TailRuntimeState()
    pending: list[TailTrade] = []
    for item in raw.get("pending", []) if isinstance(raw, dict) else []:
        if not isinstance(item, dict):
            continue
        try:
            pending.append(TailTrade(**item))
        except (TypeError, ValueError):
            continue
    return TailRuntimeState(
        pending=pending,
        recent_attempts=[str(value) for value in raw.get("recent_attempts", [])][-512:] if isinstance(raw, dict) else [],
        settled=int(raw.get("settled") or 0) if isinstance(raw, dict) else 0,
        wins=int(raw.get("wins") or 0) if isinstance(raw, dict) else 0,
        total_pnl=float(raw.get("total_pnl") or 0.0) if isinstance(raw, dict) else 0.0,
        total_cost=float(raw.get("total_cost") or 0.0) if isinstance(raw, dict) else 0.0,
    )


def market_twap_rule_reason(market: dict[str, Any], asset: str) -> Optional[str]:
    """Require the live Gamma market to explicitly advertise the 30s TWAP rule."""

    try:
        start_ts = iso_to_ts(str(market["eventStartTime"]))
        end_ts = iso_to_ts(str(market["endDate"]))
    except (KeyError, TypeError, ValueError):
        return "missing_market_window"
    if start_ts < TWAP_CUTOVER_TS:
        return "pre_twap_cutover_market"
    if end_ts - start_ts != 300:
        return "not_5m_market"
    config = market.get("cryptoMarketConfig")
    if not isinstance(config, dict):
        return "missing_crypto_market_config"
    if config.get("twapEnabled") is not True:
        return "twap_not_enabled"
    if int(_finite(config.get("twapLookbackSeconds")) or 0) != 30:
        return "twap_lookback_not_30s"
    if str(config.get("asset") or "").upper() != asset.upper():
        return "twap_asset_mismatch"
    source = str(market.get("resolutionSource") or "").lower()
    expected_stream = f"{asset.lower()}-usd-twap-30s-streams"
    if expected_stream not in source:
        return "unexpected_twap_resolution_source"
    if not bool(market.get("acceptingOrders", False)):
        return "market_not_accepting_orders"
    if market.get("closed") is True or market.get("enableOrderBook", True) is False:
        return "market_not_tradable"
    return None


def discover_tail_leg(asset: str) -> TailLeg:
    market = discover_current_market(asset, 5)
    if not isinstance(market, dict):
        raise RuntimeError(f"{asset}: market_discovery_failed")
    reason = market_twap_rule_reason(market, asset)
    if reason is not None:
        raise RuntimeError(f"{asset}: {reason}")
    yes_token, no_token = _outcome_token_map(market)
    base_fee_bps = _finite(market.get("takerBaseFee")) or 0.0
    fees_enabled = bool(market.get("feesEnabled", False))
    min_order_size = _finite(market.get("orderMinSize")) or 5.0
    return TailLeg(
        asset=asset,
        symbol=ASSET_SYMBOLS[asset],
        slug=str(market["slug"]),
        question=str(market.get("question") or ""),
        start_ts=iso_to_ts(str(market["eventStartTime"])),
        end_ts=iso_to_ts(str(market["endDate"])),
        yes_token=yes_token,
        no_token=no_token,
        condition_id=str(market.get("conditionId") or market.get("condition_id") or ""),
        tick_size=str(market.get("orderPriceMinTickSize") or market.get("minimumTickSize") or "0.01"),
        neg_risk=bool(market.get("negRisk") or market.get("neg_risk") or False),
        min_order_size=min_order_size,
        taker_base_fee_rate=(base_fee_bps / 10_000.0) if fees_enabled else 0.0,
    )


def tail_fee_per_share(price: float, base_fee_rate: float, rebate_rate: float) -> float:
    bounded = min(1.0, max(0.0, price))
    return max(0.0, base_fee_rate) * max(0.0, 1.0 - rebate_rate) * bounded * (1.0 - bounded)


async def fetch_candle_open(leg: TailLeg) -> Optional[float]:
    params = {
        "symbol": leg.symbol,
        "interval": "5m",
        "startTime": int(leg.start_ts * 1000),
        "endTime": int(leg.end_ts * 1000) - 1,
        "limit": 1,
    }
    try:
        data = await asyncio.get_running_loop().run_in_executor(
            None,
            lambda: get_json("https://api.binance.com/api/v3/klines", params, 4.0),
        )
    except Exception:
        return None
    if not isinstance(data, list) or not data or not isinstance(data[0], list) or len(data[0]) < 2:
        return None
    if int(data[0][0]) != int(leg.start_ts * 1000):
        return None
    return _finite(data[0][1])


async def hydrate_candle_opens(legs: list[TailLeg]) -> None:
    values = await asyncio.gather(*(fetch_candle_open(leg) for leg in legs), return_exceptions=True)
    for leg, value in zip(legs, values):
        leg.candle_open = None if isinstance(value, Exception) else value


def _record_pm_item(item: dict[str, Any], token_map: dict[str, tuple[TailLeg, str]], received_ts: float) -> None:
    if isinstance(item.get("price_changes"), list):
        touched: set[tuple[str, str]] = set()
        for change in item["price_changes"]:
            if not isinstance(change, dict):
                continue
            token = str(change.get("asset_id") or change.get("assetId") or "")
            item_ref = token_map.get(token)
            if item_ref is None:
                continue
            leg, side = item_ref
            book = leg.yes_book if side == "YES" else leg.no_book
            book.apply_changes([change], received_ts)
            touched.add((leg.asset, side))
        return
    token = str(item.get("asset_id") or item.get("assetId") or "")
    item_ref = token_map.get(token)
    if item_ref is None:
        return
    leg, side = item_ref
    book = leg.yes_book if side == "YES" else leg.no_book
    if isinstance(item.get("bids"), list) and isinstance(item.get("asks"), list):
        book.apply_snapshot(item["bids"], item["asks"], received_ts)
    elif isinstance(item.get("changes"), list):
        book.apply_changes(item["changes"], received_ts)


async def pm_book_consumer(legs: list[TailLeg], stop_at: float, jsonl: Path) -> None:
    token_map: dict[str, tuple[TailLeg, str]] = {}
    for leg in legs:
        token_map[leg.yes_token] = (leg, "YES")
        token_map[leg.no_token] = (leg, "NO")
    while time.time() < stop_at:
        try:
            async with websockets.connect(PM_WS_URL, ping_interval=20, close_timeout=1) as ws:
                await ws.send(json.dumps({
                    "type": "market",
                    "assets_ids": list(token_map),
                    "initial_dump": True,
                    "custom_feature_enabled": True,
                }))
                _log(jsonl, {"kind": "pm_subscribe", "ts": time.time(), "asset_ids": list(token_map)})
                while time.time() < stop_at:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                    except asyncio.TimeoutError:
                        continue
                    if isinstance(raw, bytes):
                        raw = raw.decode("utf-8", errors="ignore")
                    if isinstance(raw, str) and raw.strip().upper() in {"PING", "PONG"}:
                        continue
                    received_ts = time.time()
                    payload = json.loads(raw)
                    for item in payload if isinstance(payload, list) else [payload]:
                        if isinstance(item, dict):
                            _record_pm_item(item, token_map, received_ts)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _log(jsonl, {"kind": "pm_ws_error", "ts": time.time(), "error": repr(exc)})
            await asyncio.sleep(0.5)


def binance_stream_url(legs: list[TailLeg]) -> str:
    streams = "/".join(f"{leg.symbol.lower()}@aggTrade" for leg in legs)
    return f"wss://stream.binance.com:9443/stream?streams={streams}"


async def binance_trade_consumer(legs: list[TailLeg], stop_at: float, jsonl: Path) -> None:
    by_symbol = {leg.symbol.upper(): leg for leg in legs}
    url = binance_stream_url(legs)
    while time.time() < stop_at:
        try:
            async with websockets.connect(url, ping_interval=20, close_timeout=1) as ws:
                _log(jsonl, {"kind": "binance_subscribe", "ts": time.time(), "url": url})
                while time.time() < stop_at:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                    except asyncio.TimeoutError:
                        continue
                    received_ts = time.time()
                    payload = json.loads(raw)
                    data = payload.get("data", payload) if isinstance(payload, dict) else None
                    if not isinstance(data, dict):
                        continue
                    leg = by_symbol.get(str(data.get("s") or "").upper())
                    event_ms = _finite(data.get("T"))
                    price = _finite(data.get("p"))
                    trade_id = int(_finite(data.get("a")) or 0)
                    if leg is None or event_ms is None or price is None or price <= 0.0:
                        continue
                    leg.add_trade(BinanceTrade(
                        trade_ts=event_ms / 1000.0,
                        price=price,
                        received_ts=received_ts,
                        trade_id=trade_id,
                    ))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _log(jsonl, {"kind": "binance_ws_error", "ts": time.time(), "error": repr(exc)})
            await asyncio.sleep(0.5)


def _attempt_key(leg: TailLeg) -> str:
    return f"{leg.asset}:{leg.start_ts}"


def execution_gate(leg: TailLeg, decision: TailDecision, config: TailLiveConfig) -> tuple[Optional[str], dict[str, float]]:
    if config.quantity + 1e-12 < leg.min_order_size:
        return "quantity_below_market_minimum", {}
    book = leg.yes_book if decision.side == "YES" else leg.no_book
    if not book.quotes:
        return "missing_pm_entry_quote", {}
    entry_quote_age_ms = 1000.0 * (time.time() - book.quotes[-1].received_ts)
    if entry_quote_age_ms > config.rule.max_pm_quote_age_s * 1000.0:
        return "stale_pm_entry_quote", {"entry_quote_age_ms": entry_quote_age_ms}
    ask = book.best_ask()
    if ask is None:
        return "no_best_ask_at_entry", {}
    ask_price, ask_size = ask
    if ask_price > config.price_cap + 1e-12:
        return "best_ask_above_price_cap", {
            "best_ask": ask_price,
            "best_ask_size": ask_size,
            "entry_quote_age_ms": entry_quote_age_ms,
        }
    visible_depth = book.ask_depth_through(config.price_cap)
    required_depth = max(config.quantity, config.min_visible_ask_qty)
    if visible_depth + 1e-12 < required_depth:
        return "insufficient_visible_depth_through_cap", {
            "best_ask": ask_price,
            "best_ask_size": ask_size,
            "visible_depth": visible_depth,
            "required_depth": required_depth,
            "entry_quote_age_ms": entry_quote_age_ms,
        }
    worst_fee = tail_fee_per_share(config.price_cap, leg.taker_base_fee_rate, config.fee_rebate_rate)
    net_win_per_share = 1.0 - config.price_cap - worst_fee
    if net_win_per_share < config.min_net_win_per_share:
        return "net_win_after_fee_below_floor", {
            "best_ask": ask_price,
            "best_ask_size": ask_size,
            "visible_depth": visible_depth,
            "net_win_per_share": net_win_per_share,
            "entry_quote_age_ms": entry_quote_age_ms,
        }
    return None, {
        "best_ask": ask_price,
        "best_ask_size": ask_size,
        "visible_depth": visible_depth,
        "net_win_per_share": net_win_per_share,
        "entry_quote_age_ms": entry_quote_age_ms,
    }


def select_risk_capped(candidates: list[tuple[TailLeg, TailDecision]], config: TailLiveConfig) -> tuple[list[tuple[TailLeg, TailDecision]], set[str]]:
    selected: list[tuple[TailLeg, TailDecision]] = []
    skipped: set[str] = set()
    reserved_cost = 0.0
    for leg, decision in sorted(candidates, key=lambda item: (-(item[1].leader_bid or 0.0), item[0].asset)):
        if len(selected) >= config.max_entries_per_round:
            skipped.add(leg.asset)
            continue
        next_cost = config.quantity * config.price_cap
        if config.max_cost_per_round_usd > 0.0 and reserved_cost + next_cost > config.max_cost_per_round_usd + 1e-12:
            skipped.add(leg.asset)
            continue
        selected.append((leg, decision))
        reserved_cost += next_cost
    return selected, skipped


def _format_notification(kind: str, data: dict[str, Any]) -> str:
    if kind == "boot":
        return (
            f"[{STRATEGY_VERSION}] BOOT\n"
            f"mode={data['mode']} rounds={data['rounds']} assets={','.join(data['assets'])}\n"
            f"decision=E-{data['lead_s']:.2f}s bid>={data['min_bid']:.2f} cap={data['price_cap']:.3f}\n"
            f"qty={data['qty']:.2f} max_entries={data['max_entries']}"
        )
    if kind == "entry":
        return (
            f"[{STRATEGY_VERSION}] ENTRY {data['asset']} {data['side']}\n"
            f"filled={data['filled_qty']:.2f}/{data['target_qty']:.2f} @ {data['avg_price']:.4f}\n"
            f"leader_bid={data['leader_bid']:.3f} candle={data['candle_bp']:+.2f}bp {data['reason']}\n"
            f"slug={data['slug']}"
        )
    if kind == "settle":
        return (
            f"[{STRATEGY_VERSION}] SETTLE {data['asset']} {data['side']} win={int(data['win'])}\n"
            f"pnl={data['pnl']:+.4f}U resolved={data['settled']} wins={data['wins']}\n"
            f"slug={data['slug']}"
        )
    if kind == "alert":
        return f"[{STRATEGY_VERSION}] ALERT {data.get('asset', '')} {data.get('reason', '')} {data.get('order_id', '')}"
    return f"[{STRATEGY_VERSION}] {kind} {data}"


def make_notifier():
    notifier = _build_notifier()

    def notify(kind: str, data: dict[str, Any]) -> None:
        text = _format_notification(kind, data)
        print(f"[NOTIFY] {text.splitlines()[0]}", flush=True)
        if notifier is not None:
            try:
                notifier.send(text)
            except Exception as exc:
                print(f"[NOTIFY] send_failed: {exc}", flush=True)

    return notify


async def execute_capped_buy(
    *,
    leg: TailLeg,
    side: str,
    quantity: float,
    price_cap: float,
    slippage_ticks: int,
    live_executor: Optional[PolymarketLiveExecutor],
    order_feed: Optional[UserOrderFeed],
    gates: GateConfig,
) -> ExecutionLegResult:
    """Submit exactly one capped share-sized order, or model that same fill in dry-run."""

    book = leg.yes_book if side == "YES" else leg.no_book
    token_id = leg.yes_token if side == "YES" else leg.no_token
    label = f"{leg.asset}_{side}"
    ask = book.best_ask()
    if ask is None:
        return ExecutionLegResult(label, token_id, quantity, 0.0, 0.0, 0.0, ok=False, error="no_best_ask")
    if ask[0] > price_cap + 1e-12:
        return ExecutionLegResult(
            label,
            token_id,
            quantity,
            0.0,
            0.0,
            0.0,
            ok=False,
            error="best_ask_above_price_cap",
            raw={"submission_state": "not_submitted"},
        )
    if live_executor is None:
        remaining = quantity
        filled = 0.0
        notional = 0.0
        for price, size in sorted(book.asks.items()):
            if price > price_cap + 1e-12 or remaining <= 0.0:
                break
            take = min(remaining, size)
            filled += take
            notional += take * price
            remaining -= take
        average = notional / filled if filled > 0.0 else 0.0
        return ExecutionLegResult(
            label,
            token_id,
            quantity,
            filled,
            average,
            notional,
            order_id="shadow",
            status="shadow_fill" if filled > 0.0 else "shadow_no_fill",
            ok=filled > 0.0,
        )
    result = await asyncio.get_running_loop().run_in_executor(
        None,
        lambda: live_executor.buy_limit_fak(
            label=label,
            token_id=token_id,
            target_qty=quantity,
            best_ask=ask[0],
            tick_size=leg.tick_size,
            neg_risk=leg.neg_risk,
            slippage_ticks=slippage_ticks,
            price_cap=price_cap,
        ),
    )
    return await confirm_live_result(
        result,
        live_executor=live_executor,
        order_feed=order_feed,
        gates=gates,
    )


async def execute_candidate(
    *,
    leg: TailLeg,
    decision: TailDecision,
    round_idx: int,
    config: TailLiveConfig,
    exec_gates: GateConfig,
    live_executor: Optional[PolymarketLiveExecutor],
    order_feed: Optional[UserOrderFeed],
    state: TailRuntimeState,
    state_path: Path,
    jsonl: Path,
    notify,
) -> Optional[TailTrade]:
    gate_reason, entry_snapshot = execution_gate(leg, decision, config)
    if gate_reason is not None:
        _log(jsonl, {
            "kind": "entry_skip",
            "round": round_idx,
            "asset": leg.asset,
            "reason": gate_reason,
            "decision": decision.to_record(),
            "entry_snapshot": entry_snapshot,
        })
        return None
    result = await execute_capped_buy(
        leg=leg,
        side=str(decision.side),
        quantity=config.quantity,
        price_cap=config.price_cap,
        slippage_ticks=config.execution_slippage_ticks,
        live_executor=live_executor,
        order_feed=order_feed,
        gates=exec_gates,
    )
    unknown_reason = execution_unknown_reason(result) if execution_unknown(result) else ""
    _log(jsonl, {
        "kind": "entry_execution",
        "round": round_idx,
        "asset": leg.asset,
        "decision": decision.to_record(),
        "entry_snapshot": entry_snapshot,
        "result": asdict(result),
        "unknown_reason": unknown_reason,
    })
    if unknown_reason:
        notify("alert", {
            "asset": leg.asset,
            "reason": unknown_reason,
            "order_id": result.order_id,
        })
    if result.filled_qty <= 0.0 or result.avg_price <= 0.0:
        return None
    fee = result.filled_qty * tail_fee_per_share(result.avg_price, leg.taker_base_fee_rate, config.fee_rebate_rate)
    trade = TailTrade(
        trade_id=f"{leg.start_ts}-{leg.asset}-{decision.side}-{result.order_id or 'filled'}",
        round_idx=round_idx,
        asset=leg.asset,
        slug=leg.slug,
        start_ts=leg.start_ts,
        end_ts=leg.end_ts,
        side=str(decision.side),
        qty=result.filled_qty,
        entry_price=result.avg_price,
        entry_fee=fee,
        leader_bid=float(decision.leader_bid or 0.0),
        decision_ts=leg.end_ts - config.decision_lead_s,
        decision_reason=decision.reason,
        order_id=result.order_id,
        execution_status=result.status,
        execution_error=result.error or unknown_reason,
        jsonl=str(jsonl),
    )
    state.pending.append(trade)
    state.total_cost += trade.qty * trade.entry_price + trade.entry_fee
    write_json_atomic(state_path, state.to_record())
    _log(jsonl, {"kind": "trade_open", **asdict(trade), "raw_execution": result.raw})
    notify("entry", {
        "asset": leg.asset,
        "side": str(decision.side),
        "filled_qty": result.filled_qty,
        "target_qty": config.quantity,
        "avg_price": result.avg_price,
        "leader_bid": float(decision.leader_bid or 0.0),
        "candle_bp": float(decision.signed_candle_bp or 0.0),
        "reason": decision.reason,
        "slug": leg.slug,
    })
    return trade


def _outcome_prices(raw: Any) -> Optional[list[float]]:
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return None
    if not isinstance(raw, list):
        return None
    values = [_finite(value) for value in raw]
    return [value for value in values if value is not None] if len(values) >= 2 else None


async def fetch_resolved_pm_up(slug: str) -> Optional[float]:
    try:
        market = await asyncio.get_running_loop().run_in_executor(None, fetch_market_by_slug, slug)
    except Exception:
        return None
    if not isinstance(market, dict):
        return None
    prices = _outcome_prices(market.get("outcomePrices"))
    outcomes = parse_outcomes(market)
    if prices is None or len(outcomes) < 2 or len(prices) < 2:
        return None
    if not (market.get("closed") is True or market.get("umaResolutionStatus") == "resolved"):
        return None
    up_index = next((index for index, value in enumerate(outcomes) if str(value).lower() in {"up", "yes"}), 0)
    if prices[up_index] >= 0.99:
        return 1.0
    if prices[up_index] <= 0.01:
        return 0.0
    return None


async def settlement_loop(
    *,
    state: TailRuntimeState,
    state_path: Path,
    config: TailLiveConfig,
    notify,
) -> None:
    while True:
        now = time.time()
        changed = False
        remaining: list[TailTrade] = []
        for trade in state.pending:
            if now < trade.end_ts + 2.0:
                remaining.append(trade)
                continue
            pm_up = await fetch_resolved_pm_up(trade.slug)
            if pm_up is None:
                remaining.append(trade)
                continue
            win = (pm_up >= 0.5) if trade.side == "YES" else (pm_up < 0.5)
            pnl = trade.qty * float(win) - trade.qty * trade.entry_price - trade.entry_fee
            state.settled += 1
            state.wins += int(win)
            state.total_pnl += pnl
            changed = True
            payload = {
                "kind": "settle",
                "ts": now,
                "trade_id": trade.trade_id,
                "asset": trade.asset,
                "slug": trade.slug,
                "side": trade.side,
                "qty": trade.qty,
                "entry_price": trade.entry_price,
                "entry_fee": trade.entry_fee,
                "pm_up": pm_up,
                "win": win,
                "pnl": pnl,
                "settled": state.settled,
                "wins": state.wins,
                "total_pnl": state.total_pnl,
            }
            _log(Path(trade.jsonl), payload)
            notify("settle", payload)
        state.pending = remaining
        if changed:
            write_json_atomic(state_path, state.to_record())
        await asyncio.sleep(max(1.0, config.settlement_poll_s))


async def discover_legs() -> tuple[list[TailLeg], dict[str, str]]:
    results = await asyncio.gather(
        *(asyncio.to_thread(discover_tail_leg, asset) for asset in ASSET_SYMBOLS),
        return_exceptions=True,
    )
    legs: list[TailLeg] = []
    errors: dict[str, str] = {}
    for asset, result in zip(ASSET_SYMBOLS, results):
        if isinstance(result, TailLeg):
            legs.append(result)
        else:
            errors[asset] = repr(result)
    return legs, errors


async def run_round(
    *,
    round_idx: int,
    config: TailLiveConfig,
    exec_gates: GateConfig,
    live_executor: Optional[PolymarketLiveExecutor],
    state: TailRuntimeState,
    state_path: Path,
    notify,
) -> None:
    legs, discovery_errors = await discover_legs()
    if not legs:
        print(f"[{STRATEGY_VERSION}] discovery failed: {discovery_errors}", flush=True)
        await asyncio.sleep(3.0)
        return
    await hydrate_candle_opens(legs)
    out_dir = state_path.parent
    jsonl = out_dir / f"round_{round_idx:06d}_{min(leg.start_ts for leg in legs)}.jsonl"
    _log(jsonl, {
        "kind": "round_start",
        "round": round_idx,
        "strategy_version": STRATEGY_VERSION,
        "assets": [{
            "asset": leg.asset,
            "slug": leg.slug,
            "start_ts": leg.start_ts,
            "end_ts": leg.end_ts,
            "candle_open": leg.candle_open,
            "taker_base_fee_rate": leg.taker_base_fee_rate,
        } for leg in legs],
        "discovery_errors": discovery_errors,
        "config": asdict(config),
        "execution": "live" if live_executor is not None else "shadow",
    })
    stop_at = max(leg.end_ts for leg in legs) + 1.0
    order_feed = (
        UserOrderFeed(markets=[leg.condition_id for leg in legs], jsonl=jsonl)
        if live_executor is not None and config.user_ws_enabled
        else None
    )
    tasks = [
        asyncio.create_task(pm_book_consumer(legs, stop_at, jsonl)),
        asyncio.create_task(binance_trade_consumer(legs, stop_at, jsonl)),
    ]
    if order_feed is not None:
        tasks.append(asyncio.create_task(order_feed.run(stop_at)))
    decision_done: set[str] = {key for leg in legs if state.attempted(key := _attempt_key(leg))}
    try:
        while time.time() < stop_at:
            now = time.time()
            due = [leg for leg in legs if _attempt_key(leg) not in decision_done and now >= leg.end_ts - config.decision_lead_s]
            if due:
                candidates: list[tuple[TailLeg, TailDecision]] = []
                for leg in due:
                    key = _attempt_key(leg)
                    decision_done.add(key)
                    state.mark_attempted(key)
                    decision_ts = leg.end_ts - config.decision_lead_s
                    if now > decision_ts + config.decision_grace_s:
                        _log(jsonl, {
                            "kind": "decision_skip",
                            "round": round_idx,
                            "asset": leg.asset,
                            "reason": "decision_missed",
                            "now": now,
                            "decision_ts": decision_ts,
                        })
                        continue
                    decision = (
                        TailDecision(False, "binance_tail_buffer_overflow")
                        if leg.tail_trade_overflow
                        else evaluate_tail_decision(
                            end_ts=float(leg.end_ts),
                            decision_ts=decision_ts,
                            candle_open=leg.candle_open,
                            yes_quotes=list(leg.yes_book.quotes),
                            no_quotes=list(leg.no_book.quotes),
                            binance_trades=leg.decision_trades(),
                            config=config.rule,
                        )
                    )
                    _log(jsonl, {
                        "kind": "decision",
                        "round": round_idx,
                        "asset": leg.asset,
                        "slug": leg.slug,
                        "decision_ts": decision_ts,
                        "decision": decision.to_record(),
                    })
                    if decision.eligible:
                        candidates.append((leg, decision))
                write_json_atomic(state_path, state.to_record())
                selected, risk_skips = select_risk_capped(candidates, config)
                for leg, decision in candidates:
                    if leg.asset in risk_skips:
                        _log(jsonl, {
                            "kind": "entry_skip",
                            "round": round_idx,
                            "asset": leg.asset,
                            "reason": "round_risk_cap",
                            "decision": decision.to_record(),
                        })
                await asyncio.gather(*(
                    execute_candidate(
                        leg=leg,
                        decision=decision,
                        round_idx=round_idx,
                        config=config,
                        exec_gates=exec_gates,
                        live_executor=live_executor,
                        order_feed=order_feed,
                        state=state,
                        state_path=state_path,
                        jsonl=jsonl,
                        notify=notify,
                    )
                    for leg, decision in selected
                ))
            next_due = min(
                (leg.end_ts - config.decision_lead_s for leg in legs if _attempt_key(leg) not in decision_done),
                default=stop_at,
            )
            await asyncio.sleep(max(0.01, min(0.05, next_due - time.time())))
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def build_config() -> TailLiveConfig:
    config = TailLiveConfig(
        quantity=_envf("TAIL_QTY", 5.0),
        decision_lead_s=_envf("TAIL_DECISION_LEAD_S", 10.25),
        decision_grace_s=_envf("TAIL_DECISION_GRACE_S", 0.75),
        price_cap=_envf("TAIL_PRICE_CAP", 0.99),
        min_visible_ask_qty=_envf("TAIL_MIN_VISIBLE_ASK_QTY", 5.0),
        min_net_win_per_share=_envf("TAIL_MIN_NET_WIN_PER_SHARE", 0.001),
        fee_rebate_rate=_envf("TAIL_TAKER_REBATE_RATE", 0.0),
        execution_slippage_ticks=_envi("TAIL_EXEC_SLIPPAGE_TICKS", 1),
        max_entries_per_round=_envi("TAIL_MAX_ENTRIES_PER_ROUND", 6),
        max_cost_per_round_usd=_envf("TAIL_MAX_COST_PER_ROUND_USD", 0.0),
        settlement_poll_s=_envf("TAIL_PM_RESOLUTION_POLL_S", 15.0),
        user_ws_enabled=_envb("TAIL_USER_WS_ENABLED", False),
        user_ws_confirm_timeout_s=_envf("TAIL_USER_WS_CONFIRM_TIMEOUT_S", 8.0),
        rule=TailRuleConfig(
            min_leader_bid=_envf("TAIL_MIN_LEADER_BID", 0.90),
            max_pm_quote_age_s=_envf("TAIL_MAX_PM_QUOTE_AGE_S", 2.0),
            max_binance_trade_age_s=_envf("TAIL_MAX_BINANCE_TRADE_AGE_S", 2.0),
            weak_candle_max_bp=_envf("TAIL_WEAK_CANDLE_MAX_BP", 5.0),
            weak_adverse_cap_bp=_envf("TAIL_WEAK_ADVERSE_CAP_BP", 2.0),
        ),
    )
    config.validate()
    return config


async def main_async(args: argparse.Namespace) -> None:
    config = build_config()
    OUT.mkdir(parents=True, exist_ok=True)
    state_path = OUT / "state.json"
    state = load_state(state_path)
    write_json_atomic(state_path, state.to_record())
    dry_run = _envb("DRY_RUN", True)
    live_executor: Optional[PolymarketLiveExecutor] = None
    if not dry_run:
        live_config = LiveExecutionConfig.from_env()
        live_executor = PolymarketLiveExecutor(live_config)
        live_executor.sync_collateral()
        print(
            f"[BOOT] LIVE CLOB enabled host={live_config.host} sig={live_config.signature_type} "
            f"funder={live_config.funder[:6]}...",
            flush=True,
        )
    exec_gates = GateConfig(
        combo_qty=config.quantity,
        leg_mismatch_tolerance_shares=0.0,
        exec_slippage_ticks=config.execution_slippage_ticks,
        exec_chase_slippage_ticks=0,
        exec_max_chase_attempts=0,
        exec_user_ws_enabled=config.user_ws_enabled,
        exec_user_ws_confirm_timeout_s=config.user_ws_confirm_timeout_s,
    )
    notify = make_notifier()
    notify("boot", {
        "mode": "shadow" if dry_run else "live",
        "rounds": args.rounds,
        "assets": list(ASSET_SYMBOLS),
        "lead_s": config.decision_lead_s,
        "min_bid": config.rule.min_leader_bid,
        "price_cap": config.price_cap,
        "qty": config.quantity,
        "max_entries": config.max_entries_per_round,
    })
    settlement_task = asyncio.create_task(settlement_loop(
        state=state,
        state_path=state_path,
        config=config,
        notify=notify,
    ))
    try:
        if args.start_mode == "next":
            wait_s = 300 - (time.time() % 300)
            print(f"[{STRATEGY_VERSION}] waiting {wait_s:.1f}s for next 5m boundary", flush=True)
            await asyncio.sleep(wait_s + 0.1)
        round_idx = 0
        while args.rounds == 0 or round_idx < args.rounds:
            round_idx += 1
            await run_round(
                round_idx=round_idx,
                config=config,
                exec_gates=exec_gates,
                live_executor=live_executor,
                state=state,
                state_path=state_path,
                notify=notify,
            )
            if args.rounds == 0 or round_idx < args.rounds:
                now = time.time()
                wait_s = max(0.2, 300 - (now % 300) + 0.1)
                await asyncio.sleep(wait_s)
    finally:
        settlement_task.cancel()
        await asyncio.gather(settlement_task, return_exceptions=True)
        write_json_atomic(state_path, state.to_record())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="30s-TWAP price-path tail Polymarket live/shadow runner")
    parser.add_argument("--rounds", type=int, default=_envi("TAIL_ROUNDS", _envi("EMPJP_ROUNDS", 1_000_000)), help="0 means run forever")
    parser.add_argument("--start-mode", choices=("current", "next"), default=os.getenv("TAIL_START_MODE", "next"))
    return parser.parse_args()


def main() -> None:
    asyncio.run(main_async(parse_args()))


if __name__ == "__main__":
    main()
