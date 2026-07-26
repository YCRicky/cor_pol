"""Aftertake runtime: post-close CLOB book classification and safe execution.

Aftertake never predicts direction before the market frontend closes.  It keeps
the public Polymarket CLOB WebSocket warm, then considers only a winner side
with persistent bid support and a still-displayed residual ask.  All account
checks happen before the close; the critical path is one SQLite reservation and
one bounded GTC submission.
"""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import asdict, replace
from typing import Any, Callable, Dict, List, Optional, Tuple

from .config import Settings
from .execution import OrderExecutor, OrderResult
from .ledger import append_jsonl, rebuild_ledger
from .live_sizing import LiveSizingDecision, compute_live_entry_size
from .market_stream import MarketBookStream
from .notifier import Notifier, format_event, redacted_chat
from .pm_client import (
    BalanceAllowance,
    GammaMarket,
    LivePreflight,
    LivePreflightError,
    MarketMetadata,
    PolymarketPublicClient,
    PublicHttpClient,
    V2ClobGateway,
    parse_pm_up,
)
from .post_close import (
    STRATEGY_VERSION,
    PostCloseConfig,
    PostCloseDecision,
    PostCloseWinnerClassifier,
)
from .resolver import parse_resolve_overrides
from .risk import RiskRejected, check_entry_risk
from .settlement import builder_fee_total, fee_total, settle_trade
from .state import RuntimeLock, StateStore

RUNTIME_RETRY_S = 5.0


def current_btc_5m_slug(now: Optional[int] = None) -> Tuple[str, int, int]:
    """Return the official current BTC 5m Gamma slug and its UTC boundaries."""

    timestamp = int(now if now is not None else time.time())
    start = timestamp - timestamp % 300
    return "btc-updown-5m-%s" % start, start, start + 300


def _audit(
    settings: Settings,
    store: StateStore,
    kind: str,
    payload: Dict[str, Any],
    slug: str = "",
) -> None:
    store.append_event(kind, payload, slug=slug)
    target = settings.out_dir / ("aftertake_%s.jsonl" % slug if slug else "runtime.jsonl")
    append_jsonl(target, {"kind": kind, **payload})


def _safe_notify(
    notifier: Notifier,
    settings: Settings,
    store: StateStore,
    kind: str,
    payload: Dict[str, Any],
    slug: str = "",
) -> None:
    if not notifier.enabled:
        return
    try:
        notifier.send(format_event(kind, payload, slug))
        _audit(settings, store, "notification_sent", {"event": kind}, slug)
    except Exception as exc:
        # Operator I/O must never create an order retry or process restart.
        _audit(
            settings,
            store,
            "notification_failed",
            {"event": kind, "error": str(exc)},
            slug,
        )


def _notify_order_result(
    notifier: Notifier,
    settings: Settings,
    store: StateStore,
    result: OrderResult,
    slug: str,
    *,
    available_size: Optional[float] = None,
    simulated_take: bool = False,
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
            "dry_run": result.dry_run,
            "requested_price": result.price,
            "available_size": available_size,
            "simulated_take": simulated_take or result.dry_run,
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
    metadata_token = next((metadata.tokens.get(alias, "") for alias in aliases if alias in metadata.tokens), "")
    if metadata_token != token_id:
        raise LivePreflightError("Gamma and CLOB V2 token mappings disagree")
    return token_id


def _build_dry_metadata(market: GammaMarket) -> MarketMetadata:
    return MarketMetadata(
        condition_id=market.condition_id,
        tick_size="0.01",
        min_order_size=0.0,
        neg_risk=False,
        fee_rate=0.0,
        tokens={name.strip().lower(): token for name, token in zip(market.outcomes, market.clob_token_ids)},
        raw={"mode": "dry_run_no_authenticated_metadata"},
        builder_taker_fee_bps=0.0,
    )


def _required_cash(settings: Settings, metadata: MarketMetadata) -> float:
    """Reserve enough cash for the smallest legal live order before close.

    Dynamic live sizing happens only after the residual ask is observed.  This
    pre-close check deliberately avoids the old fixed-qty/max-ask ceiling while
    still proving the account is not empty or approval-less before the critical
    100--1000ms post-close window.
    """

    del settings
    price = 0.99
    qty = metadata.min_order_size
    return (
        price * qty
        + fee_total(price, qty, metadata.fee_rate, metadata.fee_exponent)
        + builder_fee_total(price, qty, metadata.builder_taker_fee_bps)
    )


def _entry_qty_for_decision(
    *,
    settings: Settings,
    decision: PostCloseDecision,
    metadata: MarketMetadata,
    preflight: Optional[LivePreflight],
) -> tuple[float, Optional[LiveSizingDecision]]:
    if decision.entry_ask is None or decision.entry_ask_size is None:
        raise RiskRejected("entry decision has no executable ask")
    if settings.dry_run:
        simulated_collateral = BalanceAllowance(
            balance=settings.dry_run_simulated_balance,
            allowance=settings.dry_run_simulated_balance,
            raw={"simulated": True},
        )
        sizing = compute_live_entry_size(
            price=decision.entry_ask,
            available_size=decision.entry_ask_size,
            collateral=simulated_collateral,
            metadata=metadata,
            max_account_fraction=settings.live_max_account_risk_fraction,
            quantity_step=settings.live_quantity_floor_step,
        )
        if not sizing.accepted:
            raise RiskRejected(sizing.reason or "dry_run_simulated_sizing_rejected")
        return sizing.qty, sizing
    if preflight is None:
        raise RiskRejected("live_preflight_snapshot_missing")
    sizing = compute_live_entry_size(
        price=decision.entry_ask,
        available_size=decision.entry_ask_size,
        collateral=preflight.collateral,
        metadata=metadata,
        max_account_fraction=settings.live_max_account_risk_fraction,
        quantity_step=settings.live_quantity_floor_step,
    )
    if not sizing.accepted:
        raise RiskRejected(sizing.reason or "live_sizing_rejected")
    return sizing.qty, sizing


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
    notifier: Optional[Notifier] = None,
    stream_factory: Callable[..., MarketBookStream] = MarketBookStream,
) -> List[PostCloseDecision]:
    """Run exactly one newly opened 5-minute Aftertake round.

    The expensive authenticated checks are complete ten seconds before close.
    The entry path after the classifier confirms does not perform REST, Telegram
    or retry I/O, so a qualifying residual ask is not intentionally delayed.
    """

    slug, expected_start, round_end = current_btc_5m_slug(round_start)
    if expected_start != int(round_start):
        raise ValueError("run_round requires a 5-minute boundary")
    market = public.market_by_slug(slug)
    if not market.condition_id:
        raise LivePreflightError("Gamma market has no condition ID")
    store.observe_market(slug, market.condition_id, round_start)
    yes_token = market.token_for_side("YES")
    no_token = market.token_for_side("NO")
    metadata = _build_dry_metadata(market)
    classifier_cfg = PostCloseConfig()
    classifier = PostCloseWinnerClassifier(classifier_cfg)
    notifier = notifier or Notifier(token=settings.telegram_token, chat_id=settings.telegram_chat_id)
    stream = stream_factory(
        yes_token_id=yes_token,
        no_token_id=no_token,
        on_book=classifier.record,
        clock=clock,
        near_touch_band=classifier_cfg.near_touch_band,
        resolve_overrides=parse_resolve_overrides(settings.resolve_overrides),
    )
    decisions: List[PostCloseDecision] = []
    seen_decisions: set = set()
    preflight_done = settings.dry_run
    live_preflight: Optional[LivePreflight] = None
    preflight_at = float(round_end) - 10.0

    stream.start()
    try:
        while True:
            now = float(clock())
            if now < preflight_at:
                # Stay subscribed for the complete scene-gate history without polling.
                sleep(min(0.5, max(0.0, preflight_at - now)))
                continue
            if not stream.ready:
                reason = "CLOB market stream not ready before close"
                if stream.last_error:
                    reason += ": " + stream.last_error
                decisions.append(PostCloseDecision("hold", "market_stream_not_ready"))
                _audit(settings, store, "data_guard", {"reason": reason}, slug)
                _safe_notify(notifier, settings, store, "alert", {"reason": reason}, slug)
                break
            if not preflight_done:
                if now >= round_end:
                    decisions.append(PostCloseDecision("hold", "post_close_preflight_missed"))
                    break
                if live_gateway is None:
                    raise RuntimeError("live Aftertake requires a CLOB V2 gateway")
                geo = public.geoblock_status(settings.geo_endpoint)
                metadata = live_gateway.market_metadata(market.condition_id)
                _metadata_token_matches(market, metadata, "YES")
                _metadata_token_matches(market, metadata, "NO")
                live_preflight = live_gateway.preflight(geo, _required_cash(settings, metadata))
                if float(clock()) >= round_end:
                    decisions.append(PostCloseDecision("hold", "post_close_preflight_missed"))
                    _audit(
                        settings,
                        store,
                        "data_guard",
                        {"reason": "live preflight crossed frontend close"},
                        slug,
                    )
                    break
                preflight_done = True
                continue
            if now > round_end + 1.0:
                if not decisions:
                    decisions.append(PostCloseDecision("hold", "post_close_window_expired"))
                break

            decision = classifier.evaluate(
                round_end_ts=round_end,
                now_ts=now,
                qty=settings.qty,
            )
            latest_ts = decision.audit.get("confirmation_timestamps", [None])[-1] if decision.audit else None
            key = (decision.action, decision.reason, decision.side, latest_ts)
            if key not in seen_decisions:
                decisions.append(decision)
                seen_decisions.add(key)
                _audit(
                    settings,
                    store,
                    "aftertake_decision",
                    {
                        "action": decision.action,
                        "reason": decision.reason,
                        "side": decision.side,
                        "entry_ask": decision.entry_ask,
                        "entry_ask_size": decision.entry_ask_size,
                        "winner_bid": decision.winner_bid,
                        "loser_bid": decision.loser_bid,
                        "confirmations": decision.confirmations,
                        "strategy": STRATEGY_VERSION,
                        "audit": decision.audit,
                    },
                    slug,
                )
            if decision.action != "enter" or decision.entry_ask is None:
                sleep(0.01)
                continue

            try:
                # Do not re-fetch the public REST book here: the websocket
                # observation is the executable premise and this is its short window.
                entry_qty, live_sizing = _entry_qty_for_decision(
                    settings=settings,
                    decision=decision,
                    metadata=metadata,
                    preflight=live_preflight,
                )
                if live_sizing is not None:
                    # Dynamic sizing can be larger than AFTERTAKE_QTY, which is
                    # deliberately kept as a stable baseline. Do not let a
                    # small-quantity support proof authorize a materially larger
                    # dry-run shadow fill or live order: re-evaluate the same
                    # in-memory post-close evidence at the final size. This is
                    # CPU-only and performs no REST or notification I/O inside
                    # the short take window.
                    sized_decision = classifier.evaluate(
                        round_end_ts=round_end,
                        now_ts=now,
                        qty=entry_qty,
                        min_near_touch_qty_multiplier=1.0,
                    )
                    if sized_decision.action != "enter":
                        prefix = "live" if settings.is_live else "dry_run"
                        raise RiskRejected(
                            "%s_quantity_not_supported:%s" % (prefix, sized_decision.reason)
                        )
                    decision = sized_decision
                check_entry_risk(
                    settings=settings,
                    store=store,
                    slug=slug,
                    price=decision.entry_ask,
                    qty=entry_qty,
                    displayed_ask_size=decision.entry_ask_size,
                    now_ts=now,
                )
                token_id = _metadata_token_matches(market, metadata, decision.side)
                if settings.is_live and entry_qty < metadata.min_order_size:
                    raise RiskRejected("requested_qty_below_market_minimum")
                record = store.reserve_entry(
                    slug=slug,
                    condition_id=market.condition_id,
                    round_start=round_start,
                    token_id=token_id,
                    side=decision.side,
                    requested_qty=entry_qty,
                    requested_price=decision.entry_ask,
                    fee_rate=metadata.fee_rate,
                    fee_exponent=metadata.fee_exponent,
                    builder_taker_fee_bps=metadata.builder_taker_fee_bps,
                )
                if record is None:
                    decisions.append(PostCloseDecision("hold", "market_already_reserved"))
                    break
                result = executor.execute_reserved(record, metadata, fast=True)
                executor.wait_for_event_delivery()
                _audit(
                    settings,
                    store,
                    "entry_result",
                    {
                        **asdict(result),
                        "strategy": STRATEGY_VERSION,
                        "winner_bid": decision.winner_bid,
                        "loser_bid": decision.loser_bid,
                        "confirmations": decision.confirmations,
                        "classifier_audit": decision.audit,
                        "available_size": decision.entry_ask_size,
                        "live_sizing": asdict(live_sizing) if live_sizing else None,
                        "simulated_take": result.dry_run,
                        "no_live_order": result.dry_run,
                    },
                    slug,
                )
                _notify_order_result(
                    notifier,
                    settings,
                    store,
                    result,
                    slug,
                    available_size=decision.entry_ask_size,
                    simulated_take=result.dry_run,
                )
                break
            except (RiskRejected, LivePreflightError) as exc:
                _audit(settings, store, "entry_blocked", {"reason": str(exc)}, slug)
                decisions.append(PostCloseDecision("hold", str(exc), side=decision.side))
                break
            except Exception as exc:
                _audit(settings, store, "entry_runtime_error", {"error": str(exc)}, slug)
                _safe_notify(notifier, settings, store, "alert", {"reason": str(exc)}, slug)
                break
    finally:
        stream.close()

    return decisions


def _wait_for_next_boundary(
    clock: Callable[[], float] = time.time, sleep: Callable[[float], None] = time.sleep
) -> int:
    current = int(clock())
    next_start = current - current % 300 + 300
    sleep(max(0.0, float(next_start) - clock()))
    return next_start


def _select_next_round_start(
    *,
    now: float,
    processed_round_starts: set[int],
    sleep: Callable[[float], None] = time.sleep,
) -> int:
    """Return the next runnable BTC 5m round without skipping active markets.

    The strategy needs the close-adjacent scene, not the full 5-minute history.
    After a round finishes at close+~1s, the next market is already active but
    still has almost five minutes before its own close.  Joining that active
    round prevents the old 10-minute cadence bug.
    """

    current = int(now)
    active_start = current - current % 300
    active_end = active_start + 300
    minimum_lead_s = PostCloseConfig().pre_close_window_s + PostCloseConfig().pre_close_latest_max_age_s
    if active_start not in processed_round_starts and active_end - float(now) >= minimum_lead_s:
        return active_start
    next_start = active_start + 300
    sleep(max(0.0, float(next_start) - float(now)))
    return next_start


def _reconcile_startup(
    settings: Settings, store: StateStore, executor: OrderExecutor, notifier: Notifier
) -> None:
    for record in store.unresolved_orders():
        result = executor.reconcile_existing(record)
        _notify_order_result(notifier, settings, store, result, record.slug)
        if not result.terminal:
            raise RuntimeError("startup reconciliation remains unknown for %s" % record.slug)
    if store.has_execution_unknown():
        raise RuntimeError("execution_unknown remains; manual CLOB reconciliation is required")


def _live_runtime(
    settings: Settings, store: StateStore, public: PolymarketPublicClient, notifier: Notifier
) -> Tuple[Optional[V2ClobGateway], OrderExecutor]:
    if not settings.is_live:
        return None, OrderExecutor(settings=settings, store=store)
    gateway = V2ClobGateway.from_settings(settings)
    geo = public.geoblock_status(settings.geo_endpoint)
    gateway.preflight(geo, 0.0)

    def report_submission(kind: str, payload: Dict[str, Any]) -> None:
        if kind == "submitted":
            _safe_notify(notifier, settings, store, "submitted", payload, str(payload["slug"]))

    executor = OrderExecutor(
        settings=settings, store=store, gateway=gateway, event_callback=report_submission
    )
    _reconcile_startup(settings, store, executor, notifier)
    return gateway, executor


def _run_round_loop(
    *,
    settings: Settings,
    store: StateStore,
    public: PolymarketPublicClient,
    notifier: Notifier,
    forever: bool,
    rounds: int,
    live_runtime_factory: Callable[
        [Settings, StateStore, PolymarketPublicClient, Notifier], Tuple[Optional[V2ClobGateway], OrderExecutor]
    ] = _live_runtime,
    wait_for_next_boundary: Callable[[], int] = _wait_for_next_boundary,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Keep the daemon alive while PM transport/account checks recover.

    A Polymarket outage must suppress entries, not turn into a failed systemd
    unit.  The strategy waits for a fresh round after recovery, so it never
    backdates observations into a round whose pre-close book it did not see.
    """

    gateway: Optional[V2ClobGateway] = None
    executor = OrderExecutor(settings=settings, store=store)
    completed = 0
    last_runtime_error = ""
    processed_round_starts: set[int] = set()

    while forever or completed < max(1, rounds):
        settle_open_positions(settings=settings, store=store, public=public, notifier=notifier)
        if settings.is_live and gateway is None:
            try:
                gateway, executor = live_runtime_factory(settings, store, public, notifier)
                if gateway is None:
                    raise RuntimeError("live runtime did not provide a CLOB gateway")
            except Exception as exc:
                reason = "PM runtime unavailable; retrying: %s: %s" % (type(exc).__name__, str(exc))
                _audit(settings, store, "runtime_connect_retry", {"reason": reason})
                # Notify only when the failure state changes.  A multi-minute
                # maintenance window must not flood Telegram every five seconds.
                if reason != last_runtime_error:
                    _safe_notify(notifier, settings, store, "alert", {"reason": reason})
                last_runtime_error = reason
                sleep(RUNTIME_RETRY_S)
                continue
            if last_runtime_error:
                _audit(settings, store, "runtime_recovered", {"previous_error": last_runtime_error})
                _safe_notify(notifier, settings, store, "alert", {"reason": "PM runtime recovered"})
                last_runtime_error = ""

        if wait_for_next_boundary is not _wait_for_next_boundary:
            start = wait_for_next_boundary()
        else:
            start = _select_next_round_start(
                now=time.time(),
                processed_round_starts=processed_round_starts,
                sleep=time.sleep,
            )
        processed_round_starts.add(start)
        try:
            run_round(
                settings=settings,
                store=store,
                public=public,
                executor=executor,
                live_gateway=gateway,
                round_start=start,
                notifier=notifier,
            )
        except Exception as exc:
            _audit(settings, store, "round_runtime_error", {"error": str(exc)})
            _safe_notify(notifier, settings, store, "alert", {"reason": str(exc)})
            # Rebuild the live gateway before the next fresh round. This covers
            # CLOB/Gamma/auth transport failures without attempting an order retry.
            if settings.is_live:
                gateway = None
                executor = OrderExecutor(settings=settings, store=store)
        completed += 1


def _probe_stream(
    market: GammaMarket,
    *,
    stream_factory: Callable[..., MarketBookStream] = MarketBookStream,
    timeout_s: float = 10.0,
) -> None:
    """Prove the official public WebSocket can return paired books in time.

    ``MarketBookStream`` deliberately reconnects after transient network and
    provider errors.  The deployment probe must use the same bounded retry
    behaviour instead of treating the first connection timeout as terminal.
    """

    stream = stream_factory(
        yes_token_id=market.token_for_side("YES"),
        no_token_id=market.token_for_side("NO"),
        on_book=lambda _book: None,
    )
    stream.start()
    try:
        deadline = time.monotonic() + timeout_s
        last_error = ""
        while time.monotonic() < deadline:
            if stream.ready:
                return
            if stream.last_error:
                last_error = stream.last_error
            time.sleep(0.05)
        detail = ": %s" % last_error if last_error else ""
        raise LivePreflightError("CLOB market stream did not produce paired books%s" % detail)
    finally:
        stream.close()


def deployment_check(
    *,
    settings: Settings,
    public: PolymarketPublicClient,
    gateway: Optional[V2ClobGateway],
    notifier: Notifier,
    clock: Callable[[], float] = time.time,
    stream_factory: Callable[..., MarketBookStream] = MarketBookStream,
) -> Dict[str, Any]:
    """Verify Gamma, CLOB WebSocket, Telegram and (when live) account readiness.

    This function performs no reservation, signing or order submission.
    """

    if not notifier.enabled:
        raise LivePreflightError("deployment requires TG_BOT_TOKEN and TG_CHAT_ID")
    slug, _, _ = current_btc_5m_slug(int(clock()))
    market = public.market_by_slug(slug)
    if not market.condition_id:
        raise LivePreflightError("Gamma market has no condition ID")
    _probe_stream(market, stream_factory=stream_factory)

    metadata: Optional[MarketMetadata] = None
    if settings.is_live:
        if gateway is None:
            raise RuntimeError("live deployment check requires a V2 CLOB gateway")
        metadata = gateway.market_metadata(market.condition_id)
        _metadata_token_matches(market, metadata, "YES")
        _metadata_token_matches(market, metadata, "NO")
        gateway.preflight(public.geoblock_status(settings.geo_endpoint), _required_cash(settings, metadata))

    notifier.send(format_event("preflight", {"dry_run": settings.dry_run, "qty": settings.qty, "slug": slug}))
    return {
        "mode": "live_deployment_check_passed" if settings.is_live else "dry_run_deployment_check_passed",
        "slug": slug,
        "gamma_condition_id": market.condition_id,
        "websocket_verified": True,
        "metadata_verified": metadata is not None,
        "telegram_verified": True,
    }


def _recorded_entry_fee(raw: Dict[str, Any], fee_exponent: float = 1.0) -> Optional[float]:
    trades = raw.get("trades")
    if not isinstance(trades, list):
        return None
    total, found = 0.0, False
    for trade in trades:
        if not isinstance(trade, dict):
            continue
        for key in ("fee_usdc", "feeUsdc", "fee_amount", "feeAmount"):
            if key in trade:
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
            total += fee_total(
                float(trade.get("price") or trade.get("execution_price")),
                float(trade.get("size") or trade.get("amount")),
                float(trade["fee_rate_bps"]) / 10_000.0,
                fee_exponent,
            )
            found = True
        except (KeyError, TypeError, ValueError):
            continue
    return total if found else None


def settle_open_positions(
    *, settings: Settings, store: StateStore, public: PolymarketPublicClient, notifier: Optional[Notifier] = None
) -> List[Dict[str, Any]]:
    """Settle confirmed fills only from the resolved Polymarket Gamma outcome."""

    settled: List[Dict[str, Any]] = []
    for record in store.open_positions():
        try:
            pm_up = parse_pm_up(public.market_by_slug(record.slug, allow_closed=True))
            if pm_up is None:
                store.append_event("settlement_pending", {"reason": "pm_unresolved"}, record.slug)
                continue
            recorded_fee = _recorded_entry_fee(record.raw, record.fee_exponent)
            if recorded_fee is not None:
                result = settle_trade(
                    side=record.side,
                    entry_price=record.avg_price,
                    qty=record.filled_qty,
                    pm_up=pm_up,
                    entry_fee=recorded_fee
                    + builder_fee_total(record.avg_price, record.filled_qty, record.builder_taker_fee_bps),
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
                store.append_event("settlement_pending", {"reason": "entry_fee_unavailable"}, record.slug)
                continue
            payload = {"slug": record.slug, "intent_id": record.intent_id, **asdict(result)}
            store.record_settlement(record.slug, result.pnl, payload)
            append_jsonl(settings.out_dir / "settlements.jsonl", {"kind": "settle", **payload})
            if notifier is not None:
                _safe_notify(notifier, settings, store, "settle", payload, record.slug)
            settled.append(payload)
        except Exception as exc:
            store.append_event("settlement_pending", {"reason": str(exc)}, record.slug)
    return settled


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Aftertake post-close CLOB runner")
    parser.add_argument("--rounds", type=int, default=1, help="number of fresh BTC 5m rounds")
    parser.add_argument("--forever", action="store_true", help="run fresh rounds until stopped")
    parser.add_argument("--dry-run", action="store_true", help="force shadow mode regardless of .env")
    parser.add_argument("--status", action="store_true", help="print sanitized runtime status")
    parser.add_argument("--ledger", action="store_true", help="rebuild the local audit ledger")
    parser.add_argument("--deployment-check", action="store_true", help="check Gamma, CLOB WS, Telegram and live account; no order")
    parser.add_argument("--sync-allowance", action="store_true", help="refresh CLOB pUSD allowance; no order")
    parser.add_argument("--attach-order-id", metavar="INTENT_ID", help="attach a recovered CLOB order ID")
    parser.add_argument("--order-id", help="CLOB order ID used with --attach-order-id")
    return parser


def _status_payload(settings: Settings, store: StateStore) -> Dict[str, Any]:
    return {
        "strategy": STRATEGY_VERSION,
        "dry_run": settings.dry_run,
        "qty": settings.qty,
        "live_max_account_risk_fraction": settings.live_max_account_risk_fraction,
        "live_quantity_floor_step": settings.live_quantity_floor_step,
        "dry_run_simulated_balance": settings.dry_run_simulated_balance,
        "resolve_overrides_enabled": bool(parse_resolve_overrides(settings.resolve_overrides)),
        "entry_window_ms": [100, 1000],
        "confirmations": PostCloseConfig().confirmations,
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
        gamma_host=settings.gamma_host,
        clob_host=settings.clob_host,
        http=PublicHttpClient(resolve_overrides=parse_resolve_overrides(settings.resolve_overrides)),
    )
    notifier = Notifier(token=settings.telegram_token, chat_id=settings.telegram_chat_id)
    try:
        if args.status:
            print(json.dumps(_status_payload(settings, store), indent=2, sort_keys=True))
            return 0
        if args.ledger:
            print(json.dumps(asdict(rebuild_ledger(sorted(settings.out_dir.glob("*.jsonl")))), default=list, indent=2, sort_keys=True))
            return 0
        with RuntimeLock(settings.runtime_lock):
            manual_live_operation = args.sync_allowance or bool(args.attach_order_id) or args.deployment_check
            if manual_live_operation:
                gateway, executor = _live_runtime(settings, store, public, notifier)
            else:
                gateway, executor = None, OrderExecutor(settings=settings, store=store)
            if args.sync_allowance:
                if gateway is None:
                    raise RuntimeError("--sync-allowance requires AFTERTAKE_DRY_RUN=false")
                print(json.dumps(gateway.sync_collateral_allowance(), default=str, indent=2, sort_keys=True))
                return 0
            if args.attach_order_id:
                if gateway is None or not args.order_id:
                    raise RuntimeError("--attach-order-id requires live mode and --order-id")
                store.attach_recovered_order_id(args.attach_order_id, args.order_id)
                _reconcile_startup(settings, store, executor, notifier)
                return 0
            if args.deployment_check:
                try:
                    result = deployment_check(
                        settings=settings,
                        public=public,
                        gateway=gateway,
                        notifier=notifier,
                    )
                except Exception as exc:
                    _safe_notify(notifier, settings, store, "alert", {"reason": str(exc)})
                    raise
                print(json.dumps(result, indent=2, sort_keys=True))
                return 0
            _safe_notify(notifier, settings, store, "boot", {"dry_run": settings.dry_run, "qty": settings.qty})
            _run_round_loop(
                settings=settings,
                store=store,
                public=public,
                notifier=notifier,
                forever=args.forever,
                rounds=args.rounds,
            )
    finally:
        store.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
