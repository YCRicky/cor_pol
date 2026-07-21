"""Round scheduler and fail-closed production runtime.

The runner starts a new BTC 5m session only at a fresh boundary.  It captures
real observations over time, reserves at most one entry per slug in SQLite,
and never treats an order acknowledgement as a fill.
"""

from __future__ import annotations

import argparse
import json
import time
import urllib.request
from dataclasses import asdict, dataclass, replace
from typing import Any, Callable, Dict, List, Optional, Tuple

from .config import Settings
from .execution import OrderExecutor, OrderResult
from .ledger import append_jsonl, rebuild_ledger
from .notifier import Notifier, format_event, redacted_chat
from .pm_client import (
    GammaMarket,
    LivePreflightError,
    MarketMetadata,
    PolymarketPublicClient,
    PublicHttpClient,
    V2ClobGateway,
    parse_pm_up,
    source_timestamp_s,
)
from .risk import RiskRejected, check_entry_risk
from .settlement import builder_fee_total, fee_total, settle_trade
from .state import RuntimeLock, StateStore
from .strategy import (
    BookSnapshot,
    Decision,
    MispriceConfig,
    StrategyState,
    entry_terms,
    evaluate_tick,
    lag_depth_for,
)


@dataclass(frozen=True)
class SpotObservation:
    price: float
    observed_at: float


def binance_price(
    symbol: str,
    clock: Callable[[], float] = time.time,
) -> SpotObservation:
    """Fetch a current spot observation and timestamp its receipt locally."""

    url = "https://api.binance.com/api/v3/ticker/price?symbol=%s" % symbol
    request = urllib.request.Request(url, headers={"User-Agent": "misprice-pm/0.2"})
    with urllib.request.urlopen(request, timeout=10) as response:
        data = json.loads(response.read().decode("utf-8"))
    return SpotObservation(price=float(data["price"]), observed_at=float(clock()))


def _float_or_none(value: Any) -> Optional[float]:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _best_bid_ask(raw: Dict[str, Any]) -> Tuple[Optional[float], Optional[float], float]:
    bids = raw.get("bids") or []
    asks = raw.get("asks") or []

    def price_size(row: Any) -> Tuple[Optional[float], float]:
        if not isinstance(row, dict):
            return None, 0.0
        return _float_or_none(row.get("price")), _float_or_none(row.get("size")) or 0.0

    parsed_bids = [price_size(row) for row in bids]
    parsed_asks = [price_size(row) for row in asks]
    parsed_bids = [item for item in parsed_bids if item[0] is not None]
    parsed_asks = [item for item in parsed_asks if item[0] is not None]
    bid = max(parsed_bids, default=(None, 0.0), key=lambda item: item[0] or -1.0)
    ask = min(parsed_asks, default=(None, 0.0), key=lambda item: item[0] or 99.0)
    return bid[0], ask[0], ask[1]


def book_pair_snapshot(
    pm: PolymarketPublicClient,
    yes_token_id: str,
    no_token_id: str,
    clock: Callable[[], float] = time.time,
) -> BookSnapshot:
    yes_raw = pm.book(yes_token_id)
    no_raw = pm.book(no_token_id)
    now = float(clock())
    yes_bid, yes_ask, yes_ask_size = _best_bid_ask(yes_raw)
    no_bid, no_ask, no_ask_size = _best_bid_ask(no_raw)
    source_times = [source_timestamp_s(yes_raw), source_timestamp_s(no_raw)]
    if any(value is None for value in source_times):
        # Do not pretend a REST response latency proves book freshness.
        age_s = float("inf")
    else:
        age_s = max(0.0, now - min(float(value) for value in source_times if value is not None))
    return BookSnapshot(
        yes_bid=yes_bid,
        yes_ask=yes_ask,
        yes_ask_size=yes_ask_size,
        no_bid=no_bid,
        no_ask=no_ask,
        no_ask_size=no_ask_size,
        age_s=age_s,
    )


def current_btc_5m_slug(now: Optional[int] = None) -> Tuple[str, int, int]:
    timestamp = int(now if now is not None else time.time())
    start = timestamp - timestamp % 300
    end = start + 300
    return "btc-updown-5m-%s" % start, start, end


def _strategy_config(settings: Settings) -> MispriceConfig:
    return MispriceConfig(
        min_entry_ask=settings.min_entry_ask,
        max_entry_ask=settings.max_entry_ask,
        min_transition_bp=settings.min_transition_bp,
        max_pre_abs_bp=settings.max_pre_abs_bp,
        min_abs_bp=settings.min_abs_bp,
        reprice_per_bp=settings.reprice_per_bp,
        min_lag_depth=settings.min_lag_depth,
        min_elapsed_s=settings.min_elapsed_s,
        max_elapsed_s=settings.max_elapsed_s,
        ban_elapsed_start_s=settings.ban_elapsed_start_s,
        ban_elapsed_end_s=settings.ban_elapsed_end_s,
        max_book_age_s=settings.max_book_age_s,
    )


def _audit(settings: Settings, store: StateStore, kind: str, payload: Dict[str, Any], slug: str = "") -> None:
    store.append_event(kind, payload, slug=slug)
    target = settings.out_dir / ("misprice_pm_%s.jsonl" % slug if slug else "runtime.jsonl")
    append_jsonl(target, {"kind": kind, **payload})


def _safe_notify(
    notifier: Notifier,
    settings: Settings,
    store: StateStore,
    kind: str,
    payload: Dict[str, Any],
    slug: str,
) -> None:
    if not notifier.enabled:
        return
    try:
        notifier.send(format_event(kind, payload, slug))
        _audit(settings, store, "notification_sent", {"event": kind}, slug)
    except Exception as exc:
        # Notification failures must never restart a process into duplicate risk.
        _audit(settings, store, "notification_failed", {"error": str(exc), "event": kind}, slug)


def _notify_order_result(
    notifier: Notifier,
    settings: Settings,
    store: StateStore,
    result: OrderResult,
    slug: str,
) -> None:
    if result.filled_qty > 0 and result.terminal:
        kind = "entry"
        payload = {
            "side": result.side,
            "filled_qty": result.filled_qty,
            "avg_price": result.avg_price,
            "requested_qty": result.requested_qty,
            "order_id": result.order_id,
            "status": result.status,
        }
    elif result.submission_state == "unknown":
        kind = "alert"
        payload = {"reason": result.error or "execution_unknown"}
    else:
        kind = "order_result"
        payload = {
            "side": result.side,
            "status": result.status,
            "filled_qty": result.filled_qty,
            "avg_price": result.avg_price,
            "submission_state": result.submission_state,
            "order_id": result.order_id,
        }
    _safe_notify(notifier, settings, store, kind, payload, slug)


def _metadata_token_matches(market: GammaMarket, metadata: MarketMetadata, side: str) -> str:
    token_id = market.token_for_side(side)
    aliases = ("yes", "up") if side.upper() == "YES" else ("no", "down")
    metadata_token = ""
    for alias in aliases:
        if alias in metadata.tokens:
            metadata_token = metadata.tokens[alias]
            break
    if metadata_token != token_id:
        raise LivePreflightError("Gamma and CLOB V2 token mappings disagree")
    return token_id


def _build_dry_metadata(market: GammaMarket) -> MarketMetadata:
    tokens: Dict[str, str] = {}
    for outcome, token_id in zip(market.outcomes, market.clob_token_ids):
        tokens[outcome.strip().lower()] = token_id
    return MarketMetadata(
        condition_id=market.condition_id,
        tick_size="0.01",
        min_order_size=0.0,
        neg_risk=False,
        fee_rate=0.0,
        tokens=tokens,
        raw={"mode": "dry_run_no_authenticated_metadata"},
        builder_taker_fee_bps=0.0,
    )


def run_round(
    *,
    settings: Settings,
    store: StateStore,
    public: PolymarketPublicClient,
    executor: OrderExecutor,
    live_gateway: Optional[V2ClobGateway],
    round_start: int,
    clock: Callable[[], float] = time.time,
    sleep: Callable[[float], None] = time.sleep,
    max_ticks: Optional[int] = None,
    notifier: Optional[Notifier] = None,
) -> List[Decision]:
    """Run exactly one fresh 5-minute round; return observed decisions.

    ``round_start`` must be a real boundary chosen by the scheduler.  A process
    started in the middle of a market waits for the next boundary rather than
    inventing a round-open price.
    """

    slug, expected_start, round_end = current_btc_5m_slug(round_start)
    if expected_start != int(round_start):
        raise ValueError("run_round requires a 5-minute boundary")
    open_spot = binance_price(settings.binance_symbol, clock=clock)
    if open_spot.observed_at - round_start > settings.max_open_capture_delay_s:
        raise RuntimeError("round open observation arrived too late; refusing this market")
    market = public.market_by_slug(slug)
    if not market.condition_id:
        raise LivePreflightError("Gamma market has no condition ID")
    store.observe_market(slug, market.condition_id, round_start)

    metadata = _build_dry_metadata(market)

    state = StrategyState(open_price=open_spot.price)
    state.record_spot(ts=open_spot.observed_at, price=open_spot.price)
    cfg = _strategy_config(settings)
    yes_token = market.token_for_side("YES")
    no_token = market.token_for_side("NO")
    opening_book = book_pair_snapshot(public, yes_token, no_token, clock=clock)
    if opening_book.age_s <= settings.max_book_age_s:
        state.record_book(ts=open_spot.observed_at, book=opening_book)
    decisions: List[Decision] = []
    notifier = notifier or Notifier(
        token=settings.telegram_token, chat_id=settings.telegram_chat_id
    )
    ticks = 0
    blocked_notifications: set = set()

    while True:
        now = float(clock())
        if now >= round_end:
            break
        if max_ticks is not None and ticks >= max_ticks:
            break
        ticks += 1
        try:
            spot = binance_price(settings.binance_symbol, clock=clock)
            now = float(clock())
            if now - spot.observed_at > settings.max_spot_age_s:
                raise RuntimeError("spot_observation_stale")
            state.record_spot(ts=spot.observed_at, price=spot.price)
            book = book_pair_snapshot(public, yes_token, no_token, clock=clock)
            now = float(clock())
            if now >= round_end:
                break
            decision = evaluate_tick(
                cfg=cfg,
                state=state,
                now_ts=now,
                round_start_ts=round_start,
                round_end_ts=round_end,
                slug=slug,
                spot_price=spot.price,
                book=book,
            )
        except Exception as exc:
            _audit(settings, store, "data_guard", {"reason": str(exc)}, slug)
            sleep(settings.loop_interval_s)
            continue

        decisions.append(decision)
        _audit(settings, store, "decision", {**asdict(decision), "spot": spot.price}, slug)
        if decision.action != "enter" or decision.entry_ask is None:
            sleep(settings.loop_interval_s)
            continue
        try:
            execution_price = float(decision.entry_ask)
            selected_depth = book.yes_ask_size if decision.side == "YES" else book.no_ask_size
            if live_gateway is not None:
                # Geo eligibility, wallet buying power/allowance, tick size,
                # min order size, neg-risk, and outcome mapping are all
                # re-read immediately before every real order.
                geo = public.geoblock_status(settings.geo_endpoint)
                metadata = live_gateway.market_metadata(market.condition_id)
                live_gateway.preflight(geo, 0.0)
                # Preflight can take long enough for the executable edge and
                # displayed depth to disappear. The final network read before
                # reservation/submission is therefore a fresh CLOB book.
                final_book = book_pair_snapshot(public, yes_token, no_token, clock=clock)
                final_now = float(clock())
                if final_now - spot.observed_at > settings.max_spot_age_s:
                    raise RiskRejected("spot_observation_stale_before_submit")
                if final_book.age_s > settings.max_book_age_s:
                    raise RiskRejected("orderbook_stale_before_submit")
                final_ask, final_spread, selected_depth = entry_terms(final_book, decision.side)
                if final_ask is None:
                    raise RiskRejected("executable_ask_missing_before_submit")
                if not settings.min_entry_ask <= final_ask <= settings.max_entry_ask:
                    raise RiskRejected("executable_ask_outside_entry_bounds")
                if final_spread > cfg.max_spread:
                    raise RiskRejected("executable_spread_too_wide_before_submit")
                if selected_depth < cfg.min_depth:
                    raise RiskRejected("executable_depth_too_low_before_submit")
                if decision.pre_ask is None or decision.transition_bp is None:
                    raise RiskRejected("repricing_lag_context_missing_before_submit")
                final_required, final_actual, final_lag_depth = lag_depth_for(
                    cfg=cfg,
                    transition_bp=decision.transition_bp,
                    pre_ask=decision.pre_ask,
                    signal_ask=float(final_ask),
                )
                if final_lag_depth < cfg.min_lag_depth:
                    raise RiskRejected("repricing_lag_collapsed_before_submit")
                execution_price = float(final_ask)
                decision = replace(
                    decision,
                    entry_ask=execution_price,
                    signal_ask=execution_price,
                    required_reprice=final_required,
                    actual_reprice=final_actual,
                    lag_depth=final_lag_depth,
                    spread=final_spread,
                    depth=selected_depth,
                )
                required_cash = (
                    execution_price * settings.qty
                    + fee_total(
                        execution_price,
                        settings.qty,
                        metadata.fee_rate,
                        metadata.fee_exponent,
                    )
                    + builder_fee_total(
                        execution_price,
                        settings.qty,
                        metadata.builder_taker_fee_bps,
                    )
                )
                live_gateway.preflight(geo, required_cash)
            check_entry_risk(
                settings=settings,
                store=store,
                slug=slug,
                price=execution_price,
                qty=settings.qty,
                displayed_ask_size=selected_depth,
                now_ts=final_now if live_gateway is not None else now,
            )
            token_id = _metadata_token_matches(market, metadata, decision.side)
            if settings.is_live and settings.qty < metadata.min_order_size:
                raise RiskRejected("requested_qty_below_market_minimum")
            record = store.reserve_entry(
                slug=slug,
                condition_id=market.condition_id,
                round_start=round_start,
                token_id=token_id,
                side=decision.side,
                requested_qty=settings.qty,
                requested_price=execution_price,
                fee_rate=metadata.fee_rate,
                fee_exponent=metadata.fee_exponent,
                builder_taker_fee_bps=metadata.builder_taker_fee_bps,
            )
            if record is None:
                _audit(settings, store, "entry_blocked", {"reason": "market_already_reserved"}, slug)
                break
            result = executor.execute_reserved(record, metadata)
            # The order is already terminal or frozen before operator I/O is
            # drained. This preserves SUBMITTED -> final Telegram ordering
            # without allowing notification latency to extend the GTC TTL.
            executor.wait_for_event_delivery()
            _audit(settings, store, "entry_result", {**asdict(result), **asdict(decision)}, slug)
            _notify_order_result(notifier, settings, store, result, slug)
            # The persisted reservation is the one-entry-per-market guard.
            break
        except (RiskRejected, LivePreflightError) as exc:
            _audit(settings, store, "entry_blocked", {"reason": str(exc)}, slug)
            reason = str(exc)
            if reason not in blocked_notifications:
                _safe_notify(
                    notifier, settings, store, "blocked", {"reason": reason}, slug
                )
                blocked_notifications.add(reason)
        except Exception as exc:
            _audit(settings, store, "entry_runtime_error", {"error": str(exc)}, slug)
            _safe_notify(notifier, settings, store, "alert", {"reason": str(exc)}, slug)
            break
        sleep(settings.loop_interval_s)

    _audit(settings, store, "round_complete", {"ticks": ticks, "decisions": len(decisions)}, slug)
    if not any(decision.action == "enter" for decision in decisions):
        _safe_notify(
            notifier,
            settings,
            store,
            "round",
            {
                "ticks": ticks,
                "decisions": len(decisions),
                "last_reason": decisions[-1].reason if decisions else "no_valid_decision",
            },
            slug,
        )
    return decisions


def _wait_for_next_boundary(
    clock: Callable[[], float] = time.time, sleep: Callable[[float], None] = time.sleep
) -> int:
    current = int(clock())
    next_start = current - current % 300 + 300
    sleep(max(0.0, float(next_start) - clock()))
    return next_start


def _reconcile_startup(
    settings: Settings,
    store: StateStore,
    executor: OrderExecutor,
    notifier: Notifier,
) -> None:
    unresolved = store.unresolved_orders()
    for record in unresolved:
        result = executor.reconcile_existing(record)
        _notify_order_result(notifier, settings, store, result, record.slug)
        if not result.terminal:
            raise RuntimeError("startup reconciliation remains unknown for %s" % record.slug)
    if store.has_execution_unknown():
        raise RuntimeError("execution_unknown remains; manual CLOB reconciliation is required")


def _live_runtime(
    settings: Settings,
    store: StateStore,
    public: PolymarketPublicClient,
    notifier: Notifier,
) -> Tuple[Optional[V2ClobGateway], OrderExecutor]:
    if not settings.is_live:
        return None, OrderExecutor(settings=settings, store=store)
    geo = public.geoblock_status(settings.geo_endpoint)
    gateway = V2ClobGateway.from_settings(settings)
    gateway.preflight(geo, 0.0)
    def report_submission(kind: str, payload: Dict[str, Any]) -> None:
        if kind == "submitted":
            _safe_notify(
                notifier, settings, store, "submitted", payload, str(payload["slug"])
            )

    executor = OrderExecutor(
        settings=settings,
        store=store,
        gateway=gateway,
        event_callback=report_submission,
    )
    _reconcile_startup(settings, store, executor, notifier)
    return gateway, executor


def settle_slug(
    *,
    settings: Settings,
    public: PolymarketPublicClient,
    slug: str,
    side: str,
    entry_price: float,
    qty: float,
    fee_rate: Optional[float] = None,
    entry_fee: Optional[float] = None,
    builder_fee_bps: float = 0.0,
    fee_exponent: float = 1.0,
    store: Optional[StateStore] = None,
    notifier: Optional[Notifier] = None,
) -> Dict[str, Any]:
    market = public.market_by_slug(slug, allow_closed=True)
    pm_up = parse_pm_up(market)
    if pm_up is None:
        raise RuntimeError("PM market not resolved yet for %s" % slug)
    intent_id = ""
    if store is not None and store.market_state(slug) == "open":
        matches = [record for record in store.open_positions() if record.slug == slug]
        if len(matches) != 1:
            raise RuntimeError("authoritative open fill is not uniquely identifiable")
        record = matches[0]
        intent_id = record.intent_id
        side = record.side
        entry_price = record.avg_price
        qty = record.filled_qty
        recorded_platform_fee = _recorded_entry_fee(record.raw, record.fee_exponent)
        if recorded_platform_fee is not None:
            entry_fee = recorded_platform_fee + builder_fee_total(
                record.avg_price,
                record.filled_qty,
                record.builder_taker_fee_bps,
            )
            fee_rate = None
            builder_fee_bps = 0.0
        else:
            entry_fee = None
            fee_rate = record.fee_rate
            builder_fee_bps = record.builder_taker_fee_bps
    result = settle_trade(
        side=side,
        entry_price=entry_price,
        qty=qty,
        pm_up=pm_up,
        fee_rate=fee_rate,
        entry_fee=entry_fee,
        builder_fee_bps=builder_fee_bps,
        fee_exponent=record.fee_exponent if intent_id else fee_exponent,
    )
    payload = {"slug": slug, **({"intent_id": intent_id} if intent_id else {}), **asdict(result)}
    if store is not None and store.market_state(slug) == "open":
        store.record_settlement(slug, result.pnl, payload)
    append_jsonl(settings.out_dir / "settlements.jsonl", {"kind": "settle", **payload})
    if store is not None and notifier is not None:
        _safe_notify(notifier, settings, store, "settle", payload, slug)
    return payload


def _recorded_entry_fee(
    raw: Dict[str, Any], fee_exponent: float = 1.0
) -> Optional[float]:
    trades = raw.get("trades")
    if not isinstance(trades, list):
        return None
    total = 0.0
    found = False
    for trade in trades:
        if not isinstance(trade, dict):
            continue
        for key in ("fee_usdc", "feeUsdc", "fee_amount", "feeAmount"):
            if key not in trade:
                continue
            try:
                total += float(trade[key])
                found = True
            except (TypeError, ValueError):
                pass
            break
    if found:
        return total
    for trade in trades:
        if not isinstance(trade, dict):
            continue
        try:
            fee_rate_bps = float(trade["fee_rate_bps"])
            size = float(trade.get("size") or trade.get("amount"))
            price = float(trade.get("price") or trade.get("execution_price"))
        except (KeyError, TypeError, ValueError):
            continue
        total += fee_total(price, size, fee_rate_bps / 10_000.0, fee_exponent)
        found = True
    return total if found else None


def settle_open_positions(
    *,
    settings: Settings,
    store: StateStore,
    public: PolymarketPublicClient,
    notifier: Optional[Notifier] = None,
) -> List[Dict[str, Any]]:
    """Settle confirmed fills only from official resolved Gamma outcomes."""

    settled: List[Dict[str, Any]] = []
    for record in store.open_positions():
        try:
            market = public.market_by_slug(record.slug, allow_closed=True)
            pm_up = parse_pm_up(market)
            if pm_up is None:
                store.append_event("settlement_pending", {"reason": "pm_unresolved"}, record.slug)
                continue
            actual_fee = _recorded_entry_fee(record.raw, record.fee_exponent)
            if actual_fee is not None:
                actual_fee += builder_fee_total(
                    record.avg_price,
                    record.filled_qty,
                    record.builder_taker_fee_bps,
                )
                result = settle_trade(
                    side=record.side,
                    entry_price=record.avg_price,
                    qty=record.filled_qty,
                    pm_up=pm_up,
                    entry_fee=actual_fee,
                )
            elif record.fee_rate >= 0:
                result = settle_trade(
                    side=record.side,
                    entry_price=record.avg_price,
                    qty=record.filled_qty,
                    pm_up=pm_up,
                    fee_rate=record.fee_rate,
                    fee_exponent=record.fee_exponent,
                    builder_fee_bps=record.builder_taker_fee_bps,
                )
            else:
                store.append_event(
                    "settlement_pending", {"reason": "entry_fee_unavailable"}, record.slug
                )
                continue
            payload = {
                "slug": record.slug,
                "intent_id": record.intent_id,
                **asdict(result),
            }
            store.record_settlement(record.slug, result.pnl, payload)
            append_jsonl(settings.out_dir / "settlements.jsonl", {"kind": "settle", **payload})
            if notifier is not None:
                _safe_notify(notifier, settings, store, "settle", payload, record.slug)
            settled.append(payload)
        except Exception as exc:
            store.append_event("settlement_pending", {"reason": str(exc)}, record.slug)
    return settled


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Misprice PM v3 repricing-lag round runner")
    parser.add_argument("--rounds", type=int, default=1, help="number of fresh 5m rounds to observe")
    parser.add_argument("--forever", action="store_true", help="run fresh 5m rounds until stopped")
    parser.add_argument("--dry-run", action="store_true", help="force safe shadow mode regardless of env")
    parser.add_argument("--status", action="store_true", help="print sanitized settings and exit")
    parser.add_argument("--ledger", action="store_true", help="rebuild the PM-only JSONL audit ledger")
    parser.add_argument("--preflight", action="store_true", help="run documented live checks without submitting an order")
    parser.add_argument(
        "--sync-allowance",
        action="store_true",
        help="refresh the CLOB pUSD allowance cache; never submits an order",
    )
    parser.add_argument(
        "--attach-order-id",
        metavar="INTENT_ID",
        help="attach an operator-recovered CLOB order ID, then reconcile it",
    )
    parser.add_argument("--order-id", help="CLOB order ID for --attach-order-id")
    parser.add_argument("--settle", metavar="SLUG", help="settle one historical slug using official PM Gamma")
    parser.add_argument("--side", choices=["YES", "NO"], help="side for --settle")
    parser.add_argument("--entry-price", type=float, help="entry price for --settle")
    parser.add_argument("--qty", type=float, help="qty override for --settle")
    parser.add_argument("--fee-rate", type=float, help="current/recorded market fee rate for --settle")
    parser.add_argument("--entry-fee", type=float, help="actual total entry fee for --settle")
    parser.add_argument(
        "--builder-fee-bps", type=float, default=0.0, help="builder taker fee when estimating"
    )
    parser.add_argument(
        "--fee-exponent", type=float, default=1.0, help="captured platform fee exponent"
    )
    return parser


def _status_payload(settings: Settings, store: StateStore) -> Dict[str, Any]:
    return {
        "strategy": "misprice_v3_repricing_lag_detector",
        "dry_run": settings.dry_run,
        "qty": settings.qty,
        "entry_ask": [settings.min_entry_ask, settings.max_entry_ask],
        "lag_model": {
            "min_transition_bp": settings.min_transition_bp,
            "max_pre_abs_bp": settings.max_pre_abs_bp,
            "min_abs_bp": settings.min_abs_bp,
            "reprice_per_bp": settings.reprice_per_bp,
            "min_lag_depth": settings.min_lag_depth,
            "min_elapsed_s": settings.min_elapsed_s,
            "max_elapsed_s": settings.max_elapsed_s,
        },
        "max_daily_loss": settings.max_daily_loss,
        "max_open_positions": settings.max_open_positions,
        "max_consecutive_losses": settings.max_consecutive_losses,
        "min_seconds_between_entries": settings.min_seconds_between_entries,
        "signature_type": settings.polymarket_signature_type,
        "funder": redacted_chat(settings.polymarket_funder),
        "static_l2_creds": settings.has_static_api_creds,
        "unresolved_orders": len(store.unresolved_orders()),
        "execution_unknown": store.has_execution_unknown(),
        "telegram": redacted_chat(settings.telegram_chat_id),
    }


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    settings = Settings.from_env()
    if args.dry_run:
        settings = replace(settings, dry_run=True)
        settings.validate()
    settings.out_dir.mkdir(parents=True, exist_ok=True)
    store = StateStore(settings.state_db)
    public = PolymarketPublicClient(
        gamma_host=settings.gamma_host, clob_host=settings.clob_host, http=PublicHttpClient()
    )
    notifier = Notifier(token=settings.telegram_token, chat_id=settings.telegram_chat_id)
    executor: Optional[OrderExecutor] = None
    try:
        if args.status:
            print(json.dumps(_status_payload(settings, store), indent=2, sort_keys=True))
            return 0
        if args.ledger:
            print(
                json.dumps(
                    asdict(rebuild_ledger(sorted(settings.out_dir.glob("*.jsonl")))),
                    default=list,
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0
        if args.settle:
            if (
                not args.side
                or args.entry_price is None
                or (args.fee_rate is None) == (args.entry_fee is None)
            ):
                raise SystemExit(
                    "--settle requires --side, --entry-price, and exactly one of "
                    "--entry-fee or --fee-rate"
                )
            print(
                json.dumps(
                    settle_slug(
                        settings=settings,
                        public=public,
                        slug=args.settle,
                        side=args.side,
                        entry_price=args.entry_price,
                        qty=args.qty or settings.qty,
                        fee_rate=args.fee_rate,
                        entry_fee=args.entry_fee,
                        builder_fee_bps=args.builder_fee_bps,
                        fee_exponent=args.fee_exponent,
                        store=store,
                        notifier=notifier,
                    ),
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0

        lock_path = settings.state_db.with_suffix(settings.state_db.suffix + ".lock")
        with RuntimeLock(lock_path):
            if args.attach_order_id:
                if not settings.is_live or not args.order_id:
                    raise SystemExit(
                        "--attach-order-id requires intentional live configuration and --order-id"
                    )
                store.attach_recovered_order_id(args.attach_order_id, args.order_id)
                _safe_notify(
                    notifier,
                    settings,
                    store,
                    "recovery",
                    {
                        "intent_id": args.attach_order_id,
                        "order_id": args.order_id,
                    },
                    "",
                )
                _live_runtime(settings, store, public, notifier)
                print(json.dumps(_status_payload(settings, store), indent=2, sort_keys=True))
                return 0
            if args.sync_allowance:
                if not settings.is_live:
                    raise SystemExit("--sync-allowance requires intentional live configuration")
                geo = public.geoblock_status(settings.geo_endpoint)
                gateway = V2ClobGateway.from_settings(settings)
                response = gateway.sync_collateral_allowance()
                preflight = gateway.preflight(geo, 0.0)
                print(
                    json.dumps(
                        {
                            "synced": True,
                            "balance": preflight.collateral.balance,
                            "allowance": preflight.collateral.allowance,
                            "response": response,
                        },
                        indent=2,
                        sort_keys=True,
                        default=str,
                    )
                )
                return 0
            gateway, executor = _live_runtime(settings, store, public, notifier)
            if args.preflight:
                print(
                    json.dumps(
                        {
                            "mode": "live_preflight_passed" if settings.is_live else "dry_run",
                            **_status_payload(settings, store),
                        },
                        indent=2,
                        sort_keys=True,
                    )
                )
                return 0

            _safe_notify(
                notifier,
                settings,
                store,
                "boot",
                {
                    "dry_run": settings.dry_run,
                    "qty": settings.qty,
                },
                "",
            )
            completed = 0
            while args.forever or completed < max(0, args.rounds):
                round_start = _wait_for_next_boundary()
                settle_open_positions(
                    settings=settings, store=store, public=public, notifier=notifier
                )
                run_round(
                    settings=settings,
                    store=store,
                    public=public,
                    executor=executor,
                    live_gateway=gateway,
                    round_start=round_start,
                    notifier=notifier,
                )
                completed += 1
            return 0
    finally:
        if executor is not None:
            executor.wait_for_event_delivery()
        store.close()


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
