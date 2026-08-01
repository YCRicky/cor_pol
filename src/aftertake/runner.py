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
import os
import re
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import asdict, replace
from pathlib import Path
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
    PostCloseDecision,
    PostCloseWinnerClassifier,
    active_classifier_config,
)
from .resolver import parse_resolve_overrides
from .risk import RiskRejected, check_entry_risk
from .rounds import CRYPTO_5M_WINDOW_S
from .settlement import builder_fee_total, fee_total, settle_trade
from .state import RuntimeLock, StateStore

RUNTIME_RETRY_S = 5.0
# A normal round's external work is bounded by the public HTTP timeout and the
# order reconciliation deadline.  This larger supervisor bound is a last
# resort for an SDK/socket call that ignores its own timeout; it must never
# turn one asset into a permanently waiting multi-asset round.
ASSET_ROUND_TIMEOUT_S = 90.0
RUNTIME_STALL_TIMEOUT_S = 180.0
RUNTIME_WATCHDOG_INTERVAL_S = 5.0
# This only bounds how long the runner waits to re-check an already-recorded
# websocket observation. Event confirmation and all thresholds remain in
# PostCloseConfig.
POST_CLOSE_POLL_INTERVAL_S = 0.005


class RuntimeWatchdog:
    """Exit a genuinely stalled live loop so systemd can recreate it.

    The watchdog is intentionally process-level rather than a retry loop.  A
    thread blocked inside an SDK call cannot be safely killed in Python; an
    exit is the only deterministic way to release it.  The state store already
    reserves an intent before submit, so startup recovery remains fail-closed
    and cannot duplicate an ambiguous order.
    """

    def __init__(
        self,
        *,
        stale_after_s: float = RUNTIME_STALL_TIMEOUT_S,
        interval_s: float = RUNTIME_WATCHDOG_INTERVAL_S,
        monotonic: Callable[[], float] = time.monotonic,
        exit_fn: Callable[[int], None] = os._exit,
    ):
        self.stale_after_s = max(0.01, float(stale_after_s))
        self.interval_s = max(0.1, float(interval_s))
        self._monotonic = monotonic
        self._exit_fn = exit_fn
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._last_progress = self._monotonic()
        self._stage = "boot"
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._run,
            daemon=True,
            name="aftertake-runtime-watchdog",
        )
        self._thread.start()

    def beat(self, stage: str) -> None:
        with self._lock:
            self._last_progress = self._monotonic()
            self._stage = str(stage or "unknown")

    @property
    def stage(self) -> str:
        with self._lock:
            return self._stage

    def _run(self) -> None:
        while not self._stop.wait(self.interval_s):
            with self._lock:
                age = self._monotonic() - self._last_progress
                stage = self._stage
            # The scheduler can legitimately sleep until the next five-minute
            # boundary.  It is not a stalled external operation and therefore
            # is explicitly exempt; active round/runtime stages are bounded.
            if stage == "waiting_for_round":
                continue
            if age >= self.stale_after_s:
                self._exit_fn(1)
                return

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=max(1.0, self.interval_s + 1.0))

    def request_restart(self) -> None:
        """Fail closed immediately when a worker cannot be cancelled safely."""

        self._exit_fn(1)


def _resolve_code_sha(source_file: Optional[Path] = None) -> str:
    """Return the checked-out revision for this installed source, if available."""

    try:
        repo_root = (source_file or Path(__file__)).resolve().parents[2]
        git_dir = (repo_root / ".git").resolve()
        head = (git_dir / "HEAD").read_text(encoding="ascii").strip()
        if re.fullmatch(r"[0-9a-f]{40,64}", head, flags=re.IGNORECASE):
            return head
        if not head.startswith("ref: "):
            return "unknown"

        ref_name = head.removeprefix("ref: ").strip()
        ref_path = (git_dir / ref_name).resolve()
        try:
            ref_path.relative_to(git_dir)
        except ValueError:
            return "unknown"

        code_sha = ref_path.read_text(encoding="ascii").strip()
        return code_sha if re.fullmatch(r"[0-9a-f]{40,64}", code_sha, flags=re.IGNORECASE) else "unknown"
    except Exception:
        return "unknown"


def current_crypto_5m_slug(asset: str, now: Optional[int] = None) -> Tuple[str, int, int]:
    """Return the official current crypto 5m Gamma slug and its UTC boundaries."""

    timestamp = int(now if now is not None else time.time())
    start = timestamp - timestamp % CRYPTO_5M_WINDOW_S
    normalized = str(asset).lower().strip()
    if not normalized:
        raise ValueError("asset is required")
    return (
        "%s-updown-5m-%s" % (normalized, start),
        start,
        start + CRYPTO_5M_WINDOW_S,
    )


def current_btc_5m_slug(now: Optional[int] = None) -> Tuple[str, int, int]:
    """Backward-compatible BTC 5m slug helper."""

    return current_crypto_5m_slug("BTC", now)


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




def _latency_payload_from_result(result: OrderResult) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "event_ts": result.event_ts,
        "decision_to_submit_ms": result.decision_to_submit_ms,
        "submit_roundtrip_ms": result.submit_roundtrip_ms,
        "reconcile_duration_ms": result.reconcile_duration_ms,
        "observed_book_age_ms": result.observed_book_age_ms,
        "immediate_taker_order_delay_enabled": result.immediate_taker_order_delay_enabled,
        "expected_taker_delay_ms": result.expected_taker_delay_ms,
    }
    raw = result.raw or {}
    timing = raw.get("timing") or raw.get("submit", {}).get("_timing") or {}
    for key in (
        "book_observed_ts",
        "decision_ts",
        "round_end_ts",
        "seconds_after_close_at_decision",
        "submit_start_ts",
        "submit_end_ts",
        "reconcile_start_ts",
        "reconcile_end_ts",
    ):
        if key in timing:
            payload[key] = timing[key]
    return payload


def _audit_decision(
    settings: Settings,
    store: StateStore,
    decision: PostCloseDecision,
    slug: str,
) -> None:
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
            "strategy": (decision.audit or {}).get("strategy_version", STRATEGY_VERSION),
            "audit": decision.audit,
        },
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
            **_latency_payload_from_result(result),
        }
    elif result.submission_state == "unknown":
        kind = "alert"
        raw = result.raw or {}
        payload = {
            "reason": result.error or "execution_unknown",
            "submission_state": result.submission_state,
            "order_id": result.order_id or "n/a",
            "order_type": settings.order_type,
            "error_type": raw.get("error_type", ""),
            "status_code": raw.get("status_code", ""),
            "error_message": raw.get("error_message", raw.get("submit_error", "")),
            "error_hint": raw.get("error_hint", ""),
            **_latency_payload_from_result(result),
        }
    else:
        kind = "order_result"
        raw = result.raw or {}
        payload = {
            "side": result.side,
            "status": result.status,
            "filled_qty": result.filled_qty,
            "avg_price": result.avg_price,
            "submission_state": result.submission_state,
            "order_id": result.order_id,
            "reason": result.error or raw.get("classification", ""),
            "order_type": settings.order_type,
            "requested_qty": result.requested_qty,
            "requested_price": result.price,
            "available_size": available_size,
            "error_message": raw.get("error_message", raw.get("submit_error", "")),
            "status_code": raw.get("status_code", ""),
            "error_hint": raw.get("error_hint", ""),
            **_latency_payload_from_result(result),
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
    50--1000ms post-close window.
    """

    del settings
    price = 0.99
    qty = metadata.min_order_size
    return (
        price * qty
        + fee_total(price, qty, metadata.fee_rate, metadata.fee_exponent)
        + builder_fee_total(price, qty, metadata.builder_taker_fee_bps)
    )


class _RoundAccountPreflight:
    """Share one live account snapshot across the configured assets in a round."""

    def __init__(self, settings: Settings, public: PolymarketPublicClient, gateway: V2ClobGateway):
        self._settings = settings
        self._public = public
        self._gateway = gateway
        self._lock = threading.Lock()
        self._snapshot: Optional[LivePreflight] = None
        self._error: Optional[BaseException] = None

    def snapshot(self) -> LivePreflight:
        with self._lock:
            if self._error is not None:
                raise self._error
            if self._snapshot is None:
                try:
                    geo = self._public.geoblock_status(self._settings.geo_endpoint)
                    # Per-market metadata determines the minimum order requirement.
                    # It is checked against this one fresh account snapshot below.
                    self._snapshot = self._gateway.preflight(geo, 0.0)
                except Exception as exc:
                    self._error = exc
                    raise
            return self._snapshot


def _check_preflight_collateral(preflight: LivePreflight, required_cash: float) -> None:
    if preflight.collateral.balance < required_cash:
        raise LivePreflightError("deposit wallet pUSD balance is below the final order requirement")
    if preflight.collateral.allowance < required_cash:
        raise LivePreflightError("deposit wallet pUSD allowance is below the final order requirement")


_SENSITIVE_ERROR_VALUE = re.compile(
    r"(?i)\b(api[_-]?(?:key|secret)|passphrase|private[_-]?key|authorization|bearer|token)\b"
    r"\s*(?:([=:])\s*|(\s+))(?:bearer\s+)?[^\s,;]+"
)


def _safe_transport_message(exc: BaseException) -> str:
    value = getattr(exc, "error_message", None)
    if value is None:
        value = getattr(exc, "error_msg", None)
    if value is None:
        value = str(exc)
    if isinstance(value, dict):
        value = {key: value.get(key) for key in ("error", "message", "detail", "code") if key in value}
    text = " ".join(str(value or "").split())
    text = _SENSITIVE_ERROR_VALUE.sub(
        lambda match: "%s%s[redacted]" % (match.group(1), match.group(2) or match.group(3)), text
    )
    return text[:500] + "..." if len(text) > 500 else text


def _is_transient_transport_error(exc: BaseException) -> bool:
    status_code = getattr(exc, "status_code", None)
    message = _safe_transport_message(exc).lower()
    if status_code in {408, 429, 500, 502, 503, 504}:
        return True
    if isinstance(exc, (ConnectionError, OSError, TimeoutError)):
        return True
    return status_code is None and any(
        phrase in message
        for phrase in ("request exception", "connection", "timeout", "temporarily unavailable")
    )


def _audit_asset_transport_error(
    settings: Settings,
    store: StateStore,
    *,
    asset: str,
    slug: str,
    phase: str,
    exc: BaseException,
    notifier: Optional[Notifier] = None,
) -> None:
    status_code = getattr(exc, "status_code", None)
    if status_code is not None and not isinstance(status_code, (bool, float, int, str)):
        status_code = str(status_code)
    payload = {
        "asset": asset,
        "slug": slug,
        "phase": phase,
        "error_type": type(exc).__name__,
        "status_code": status_code,
        "error_message": _safe_transport_message(exc),
    }
    _audit(
        settings,
        store,
        "asset_transport_error",
        payload,
        slug,
    )
    if notifier is not None:
        _safe_notify(
            notifier,
            settings,
            store,
            "alert",
            {"reason": "asset transport error", **payload},
            slug,
        )


def _update_asset_error_state(
    *,
    settings: Settings,
    store: StateStore,
    notifier: Optional[Notifier],
    asset: str,
    decisions: List[PostCloseDecision],
    error_state: Optional[set[str]],
) -> None:
    if error_state is None:
        return
    key = "asset:%s" % asset
    failed = any(
        "transport_error" in str(decision.reason)
        or str(decision.reason) == "asset_round_timeout"
        for decision in decisions
    )
    if failed:
        error_state.add(key)
    elif key in error_state:
        error_state.remove(key)
        if notifier is not None:
            _safe_notify(
                notifier,
                settings,
                store,
                "recovery_success",
                {"component": "asset:%s" % asset, "reason": "asset round recovered"},
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
    asset: Optional[str] = None,
    round_preflight: Optional[_RoundAccountPreflight] = None,
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

    active_asset = str(asset or settings.asset).upper().strip()
    slug, expected_start, round_end = current_crypto_5m_slug(active_asset, round_start)
    if expected_start != int(round_start):
        raise ValueError("run_round requires a 5-minute boundary")
    market = public.market_by_slug(slug)
    if not market.condition_id:
        raise LivePreflightError("Gamma market has no condition ID")
    store.observe_market(slug, market.condition_id, round_start)
    yes_token = market.token_for_side("YES")
    no_token = market.token_for_side("NO")
    metadata = _build_dry_metadata(market)
    classifier_cfg = active_classifier_config()
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
    stream_recovery_pending = False

    stream.start()
    stream_generation = int(getattr(stream, "generation", 0))
    try:
        while True:
            now = float(clock())
            current_generation = int(getattr(stream, "generation", stream_generation))
            if current_generation != stream_generation:
                # A reconnect clears the stream's paired books. Do not let the
                # classifier combine the old generation's pre-close evidence
                # with a fresh snapshot from the new TCP connection.
                classifier.reset()
                stream_generation = current_generation
                _audit(
                    settings,
                    store,
                    "market_stream_reconnected",
                    {
                        "generation": stream_generation,
                        "last_error": str(getattr(stream, "last_error", "") or ""),
                    },
                    slug,
                )
                if int(getattr(stream, "reconnect_count", 0)) > 0:
                    stream_recovery_pending = True
                    _safe_notify(
                        notifier,
                        settings,
                        store,
                        "alert",
                        {
                            "reason": "market stream reconnecting",
                            "component": "market_stream",
                            "generation": stream_generation,
                            "reconnect_count": int(getattr(stream, "reconnect_count", 0)),
                            "error_message": str(getattr(stream, "last_error", "") or ""),
                        },
                        slug,
                    )
            if stream_recovery_pending and stream.ready:
                stream_recovery_pending = False
                _safe_notify(
                    notifier,
                    settings,
                    store,
                    "recovery_success",
                    {
                        "component": "market_stream",
                        "reason": "fresh paired book restored",
                        "details": "generation=%s reconnect_count=%s"
                        % (
                            current_generation,
                            int(getattr(stream, "reconnect_count", 0)),
                        ),
                    },
                    slug,
                )
            if now < preflight_at:
                # Stay subscribed for the complete scene-gate history without polling.
                sleep(min(0.5, max(0.0, preflight_at - now)))
                continue
            if not stream.ready:
                # A transient reconnect at the preflight boundary is not a
                # reason to abandon the round immediately. Wait until close
                # for a fresh paired snapshot; the classifier's pre-close
                # gate will still fail closed if the recovery came too late.
                if now >= round_end:
                    reason = "CLOB market stream not ready before close"
                    if stream.last_error:
                        reason += ": " + stream.last_error
                    decisions.append(PostCloseDecision("hold", "market_stream_not_ready"))
                    _audit(settings, store, "data_guard", {"reason": reason}, slug)
                    _safe_notify(notifier, settings, store, "alert", {"reason": reason}, slug)
                    break
                sleep(min(0.05, max(0.0, round_end - now)))
                continue
            if not preflight_done:
                if now >= round_end:
                    decisions.append(PostCloseDecision("hold", "post_close_preflight_missed"))
                    _safe_notify(
                        notifier,
                        settings,
                        store,
                        "alert",
                        {"reason": "post-close preflight missed", "component": "account_preflight"},
                        slug,
                    )
                    break
                if live_gateway is None:
                    raise RuntimeError("live Aftertake requires a CLOB V2 gateway")
                phase = "account_preflight" if round_preflight is not None else "market_metadata"
                try:
                    if round_preflight is not None:
                        # A shared account snapshot failure applies to every
                        # asset, but it is still reported with this asset's
                        # slug so the operator can identify the blocked round.
                        live_preflight = round_preflight.snapshot()
                        phase = "market_metadata"
                    metadata = live_gateway.market_metadata(market.condition_id)
                    _metadata_token_matches(market, metadata, "YES")
                    _metadata_token_matches(market, metadata, "NO")
                    required_cash = _required_cash(settings, metadata)
                    if round_preflight is not None:
                        _check_preflight_collateral(live_preflight, required_cash)
                    else:
                        geo = public.geoblock_status(settings.geo_endpoint)
                        live_preflight = live_gateway.preflight(geo, required_cash)
                except Exception as exc:
                    # A single CLOB request failure has a concrete asset/slug;
                    # report it immediately without rebuilding the gateway for
                    # a transient asset-local fault.
                    _audit_asset_transport_error(
                        settings,
                        store,
                        asset=active_asset,
                        slug=slug,
                        phase=phase,
                        exc=exc,
                        notifier=notifier,
                    )
                    if not _is_transient_transport_error(exc):
                        raise
                    decisions.append(PostCloseDecision("hold", "%s_transport_error" % phase))
                    break
                if float(clock()) >= round_end:
                    decisions.append(PostCloseDecision("hold", "post_close_preflight_missed"))
                    _audit(
                        settings,
                        store,
                        "data_guard",
                        {"reason": "live preflight crossed frontend close"},
                        slug,
                    )
                    _safe_notify(
                        notifier,
                        settings,
                        store,
                        "alert",
                        {
                            "reason": "live preflight crossed frontend close",
                            "component": "account_preflight",
                        },
                        slug,
                    )
                    break
                preflight_done = True
                continue
            if now > round_end + classifier_cfg.post_close_end_s:
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
            audit_decision = None
            if key not in seen_decisions:
                decisions.append(decision)
                seen_decisions.add(key)
                audit_decision = decision
            if decision.action != "enter" or decision.entry_ask is None:
                if audit_decision is not None:
                    _audit_decision(settings, store, audit_decision, slug)
                sleep(POST_CLOSE_POLL_INTERVAL_S)
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
                    if audit_decision is not None:
                        _audit_decision(settings, store, audit_decision, slug)
                    decisions.append(PostCloseDecision("hold", "market_already_reserved"))
                    break
                decision_ts = float(clock())
                book_observed_ts = None
                try:
                    book_observed_ts = float((decision.audit or {}).get("confirmation_timestamps", [None])[-1])
                except (TypeError, ValueError):
                    book_observed_ts = None
                timing_context = {
                    "asset": active_asset,
                    "round_start": int(round_start),
                    "round_end_ts": float(round_end),
                    "decision_ts": decision_ts,
                    "seconds_after_close_at_decision": decision_ts - float(round_end),
                    "book_observed_ts": book_observed_ts,
                    "immediate_taker_order_delay_enabled": metadata.immediate_taker_order_delay_enabled,
                    "expected_taker_delay_ms": metadata.expected_taker_delay_ms,
                }
                result = executor.execute_reserved(record, metadata, fast=True, timing_context=timing_context)
                executor.wait_for_event_delivery()
                if audit_decision is not None:
                    _audit_decision(settings, store, audit_decision, slug)
                _audit(
                    settings,
                    store,
                    "entry_result",
                    {
                        **asdict(result),
                        "strategy": (decision.audit or {}).get(
                            "strategy_version", STRATEGY_VERSION
                        ),
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
                if audit_decision is not None:
                    _audit_decision(settings, store, audit_decision, slug)
                _audit(settings, store, "entry_blocked", {"reason": str(exc)}, slug)
                decisions.append(PostCloseDecision("hold", str(exc), side=decision.side))
                break
            except Exception as exc:
                if audit_decision is not None:
                    _audit_decision(settings, store, audit_decision, slug)
                _audit(settings, store, "entry_runtime_error", {"error": str(exc)}, slug)
                _safe_notify(notifier, settings, store, "alert", {"reason": str(exc)}, slug)
                break
    finally:
        stream.close()

    return decisions


def _run_asset_rounds(
    *,
    settings: Settings,
    store: StateStore,
    public: PolymarketPublicClient,
    executor: OrderExecutor,
    live_gateway: Optional[V2ClobGateway],
    round_start: int,
    notifier: Optional[Notifier] = None,
    stream_factory: Callable[..., MarketBookStream] = MarketBookStream,
    clock: Callable[[], float] = time.time,
    sleep: Callable[[float], None] = time.sleep,
    timeout_s: float = ASSET_ROUND_TIMEOUT_S,
    error_state: Optional[set[str]] = None,
) -> Dict[str, List[PostCloseDecision]]:
    """Run all configured assets for the same 5-minute round concurrently."""

    assets = tuple(settings.assets)
    round_preflight = (
        _RoundAccountPreflight(settings, public, live_gateway)
        if settings.is_live and live_gateway is not None
        else None
    )
    results: Dict[str, List[PostCloseDecision]] = {}
    # Do not use the executor as a context manager here: __exit__ waits for
    # every worker, which recreates the old silent-freeze when one SDK call
    # ignores its socket timeout.  Workers are bounded by the supervisor and
    # are explicitly detached on timeout; the process watchdog is the final
    # backstop for a truly wedged native/network call.
    pool = ThreadPoolExecutor(max_workers=len(assets), thread_name_prefix="aftertake-asset")
    futures = {
        pool.submit(
            run_round,
            settings=settings,
            store=store,
            public=public,
            executor=executor,
            live_gateway=live_gateway,
            round_start=round_start,
            asset=asset,
            round_preflight=round_preflight,
            notifier=notifier,
            stream_factory=stream_factory,
            clock=clock,
            sleep=sleep,
        ): asset
        for asset in assets
    }
    pending = set(futures)
    deadline = time.monotonic() + max(0.01, float(timeout_s))
    try:
        while pending:
            remaining = max(0.0, deadline - time.monotonic())
            done, pending = wait(pending, timeout=remaining, return_when=FIRST_COMPLETED)
            if not done:
                for future in pending:
                    asset = futures[future]
                    slug, _, _ = current_crypto_5m_slug(asset, round_start)
                    timeout_error = TimeoutError(
                        "asset round exceeded supervisor timeout %.1fs" % float(timeout_s)
                    )
                    _audit_asset_transport_error(
                        settings,
                        store,
                        asset=asset,
                        slug=slug,
                        phase="asset_round_timeout",
                        exc=timeout_error,
                        notifier=notifier,
                    )
                    results[asset] = [PostCloseDecision("hold", "asset_round_timeout")]
                    _update_asset_error_state(
                        settings=settings,
                        store=store,
                        notifier=notifier,
                        asset=asset,
                        decisions=results[asset],
                        error_state=error_state,
                    )
                break
            for future in done:
                asset = futures[future]
                try:
                    results[asset] = future.result()
                except Exception as exc:
                    slug, _, _ = current_crypto_5m_slug(asset, round_start)
                    _audit_asset_transport_error(
                        settings,
                        store,
                        asset=asset,
                        slug=slug,
                        phase="asset_round_unhandled",
                        exc=exc,
                        notifier=notifier,
                    )
                    results[asset] = [PostCloseDecision("hold", "asset_round_transport_error")]
                    if not _is_transient_transport_error(exc):
                        raise
                _update_asset_error_state(
                    settings=settings,
                    store=store,
                    notifier=notifier,
                    asset=asset,
                    decisions=results[asset],
                    error_state=error_state,
                )
    finally:
        for future in pending:
            future.cancel()
        # Never wait for a worker that has already crossed the external
        # timeout.  ``cancel_futures`` is available on supported Python 3.9+
        # runtimes; retain a compatibility fallback for older deployments.
        try:
            pool.shutdown(wait=False, cancel_futures=True)
        except TypeError:
            pool.shutdown(wait=False)
    return results


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
    """Return the next runnable 5m round without skipping active markets.

    The strategy needs the close-adjacent scene, not the full 5-minute history.
    After a round finishes at close+~1s, the next market is already active but
    still has almost five minutes before its own close.  Joining that active
    round prevents the old 10-minute cadence bug.
    """

    current = int(now)
    active_start = current - current % 300
    active_end = active_start + 300
    classifier_cfg = active_classifier_config()
    minimum_lead_s = (
        classifier_cfg.pre_close_window_s
        + classifier_cfg.pre_close_latest_max_age_s
    )
    if active_start not in processed_round_starts and active_end - float(now) >= minimum_lead_s:
        return active_start
    next_start = active_start + 300
    sleep(max(0.0, float(next_start) - float(now)))
    return next_start


def _reconcile_startup(
    settings: Settings, store: StateStore, executor: OrderExecutor, notifier: Notifier
) -> None:
    unresolved = store.unresolved_orders()
    for record in unresolved:
        if record.state == "execution_unknown" and not record.order_id:
            # Legacy versions stored submit-path PM infrastructure failures as
            # execution_unknown with no durable order id.  Current policy is to
            # terminal-skip only that affected market at startup, not to keep
            # alerting or globally block new entries.
            raw = dict(record.raw or {})
            raw["classification"] = raw.get("classification") or record.error or "legacy_execution_unknown"
            raw["startup_terminal_skip"] = True
            raw["terminal_skip"] = True
            raw["order_type"] = raw.get("order_type") or settings.order_type
            store.mark_terminal_execution(record.intent_id, 0.0, 0.0, raw, "startup_skipped")
            _audit(
                settings,
                store,
                "startup_terminal_skip",
                {"reason": record.error or "legacy_execution_unknown", "order_id": "n/a"},
                record.slug,
            )
            continue
        try:
            result = executor.reconcile_existing(record)
        except Exception as exc:
            # One stale/temporarily unreachable order must not prevent the
            # daemon from scanning fresh markets.  The intent remains in the
            # durable unresolved state and will be retried on the next boot;
            # risk checks continue to count it fail-closed in the meantime.
            _audit(
                settings,
                store,
                "startup_reconciliation_error",
                {
                    "reason": _safe_transport_message(exc),
                    "error_type": type(exc).__name__,
                    "order_id": record.order_id or "n/a",
                },
                record.slug,
            )
            _safe_notify(
                notifier,
                settings,
                store,
                "alert",
                {
                    "reason": "startup reconciliation error",
                    "component": "startup_reconciliation",
                    "error_type": type(exc).__name__,
                    "error_message": _safe_transport_message(exc),
                    "order_id": record.order_id or "n/a",
                },
                record.slug,
            )
            continue
        if result.terminal:
            _notify_order_result(notifier, settings, store, result, record.slug)
            continue
        _audit(
            settings,
            store,
            "startup_reconciliation_still_pending",
            {"reason": result.error or result.status, "order_id": result.order_id or "n/a"},
            record.slug,
        )


def reconcile_submitted_orders(
    *, settings: Settings, store: StateStore, executor: OrderExecutor, notifier: Notifier
) -> List[OrderResult]:
    """Reconcile live submitted GTC/GTD orders without blocking fresh runtime.

    Pending GTC orders are intentionally left working after the initial submit
    window.  Each loop probes their CLOB status; terminal fills become open
    positions and terminal zero-fills become no_fill. Non-terminal submitted
    orders stay pending and are not treated as runtime failures.
    """

    results: List[OrderResult] = []
    if not settings.is_live:
        return results
    for record in store.unresolved_orders():
        if record.state != "submitted" or not record.order_id:
            continue
        try:
            result = executor.reconcile_existing(record)
        except Exception as exc:
            # Reconciliation is best-effort and must not hold the next market
            # boundary hostage to one order or one provider outage.
            _audit(
                settings,
                store,
                "submitted_reconcile_error",
                {
                    "reason": _safe_transport_message(exc),
                    "error_type": type(exc).__name__,
                    "order_id": record.order_id,
                },
                record.slug,
            )
            _safe_notify(
                notifier,
                settings,
                store,
                "alert",
                {
                    "reason": "submitted order reconciliation error",
                    "component": "submitted_reconciliation",
                    "error_type": type(exc).__name__,
                    "error_message": _safe_transport_message(exc),
                    "order_id": record.order_id,
                },
                record.slug,
            )
            continue
        results.append(result)
        if result.terminal:
            _notify_order_result(notifier, settings, store, result, record.slug)
    return results


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
        elif kind == "heartbeat_error":
            _safe_notify(notifier, settings, store, "alert", payload)
        elif kind == "heartbeat_recovered":
            _safe_notify(notifier, settings, store, "recovery_success", {
                **payload,
                "component": "clob_heartbeat",
            })

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
    runtime_watchdog: Optional[RuntimeWatchdog] = None,
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
    active_error_components: set[str] = set()
    processed_round_starts: set[int] = set()

    while forever or completed < max(1, rounds):
        if runtime_watchdog is not None:
            runtime_watchdog.beat("runtime_connect" if settings.is_live and gateway is None else "waiting_for_round")
        if settings.is_live and gateway is None:
            try:
                gateway, executor = live_runtime_factory(settings, store, public, notifier)
                if gateway is None:
                    raise RuntimeError("live runtime did not provide a CLOB gateway")
            except Exception as exc:
                reason = "PM runtime unavailable; retrying: %s: %s" % (type(exc).__name__, str(exc))
                _audit(settings, store, "runtime_connect_retry", {"reason": reason})
                # Every occurrence is intentional operator evidence. Telegram
                # noise is preferable to another silent runtime freeze.
                _safe_notify(
                    notifier,
                    settings,
                    store,
                    "alert",
                    {"reason": reason, "component": "pm_runtime"},
                )
                last_runtime_error = reason
                if runtime_watchdog is not None:
                    runtime_watchdog.beat("runtime_retry_wait")
                sleep(RUNTIME_RETRY_S)
                continue
            if last_runtime_error:
                _audit(settings, store, "runtime_recovered", {"previous_error": last_runtime_error})
                _safe_notify(
                    notifier,
                    settings,
                    store,
                    "recovery_success",
                    {"component": "pm_runtime", "reason": "PM runtime recovered"},
                )
                last_runtime_error = ""
            if runtime_watchdog is not None:
                runtime_watchdog.beat("waiting_for_round")

        if wait_for_next_boundary is not _wait_for_next_boundary:
            start = wait_for_next_boundary()
        else:
            start = _select_next_round_start(
                now=time.time(),
                processed_round_starts=processed_round_starts,
                sleep=time.sleep,
            )
        processed_round_starts.add(start)
        if runtime_watchdog is not None:
            runtime_watchdog.beat("active_round")
        try:
            round_results = _run_asset_rounds(
                settings=settings,
                store=store,
                public=public,
                executor=executor,
                live_gateway=gateway,
                round_start=start,
                notifier=notifier,
                error_state=active_error_components,
            )
            timed_out_assets = [
                asset
                for asset, decisions in round_results.items()
                if any(item.reason == "asset_round_timeout" for item in decisions)
            ]
            if timed_out_assets:
                _audit(
                    settings,
                    store,
                    "asset_supervisor_restart",
                    {"assets": timed_out_assets, "reason": "worker_timeout_uncancellable"},
                )
                if runtime_watchdog is not None:
                    # A Python worker cannot be force-killed. Restart the
                    # process instead of allowing a late SDK return to submit
                    # against the next round; SQLite recovery handles any
                    # reserved intent conservatively on the fresh boot.
                    runtime_watchdog.request_restart()
            if runtime_watchdog is not None:
                runtime_watchdog.beat("round_complete")
        except Exception as exc:
            _audit(settings, store, "round_runtime_error", {"error": str(exc)})
            _safe_notify(notifier, settings, store, "alert", {"reason": str(exc)})
            # Rebuild the live gateway before the next fresh round. This covers
            # CLOB/Gamma/auth transport failures without attempting an order retry.
            if settings.is_live:
                gateway = None
                executor = OrderExecutor(settings=settings, store=store)
            if runtime_watchdog is not None:
                # The asset supervisor may have detached a worker that raised
                # a non-transport exception. Do not keep running alongside an
                # uncancellable worker with shared state/client objects.
                runtime_watchdog.request_restart()
        finally:
            # Post-round only: pending GTC reconciliation and official settlement
            # must never delay market discovery for the next 5-minute boundary.
            if settings.is_live and gateway is not None:
                try:
                    reconcile_submitted_orders(settings=settings, store=store, executor=executor, notifier=notifier)
                except Exception as exc:
                    _audit(settings, store, "submitted_reconcile_error", {"error": str(exc)})
                    _safe_notify(
                        notifier,
                        settings,
                        store,
                        "alert",
                        {
                            "reason": "submitted reconciliation sweep error",
                            "component": "submitted_reconciliation_sweep",
                            "error_type": type(exc).__name__,
                            "error_message": _safe_transport_message(exc),
                        },
                    )
            try:
                settle_open_positions(settings=settings, store=store, public=public, notifier=notifier)
            except Exception as exc:
                # Settlement is informational and per-position fail-closed. A
                # database/provider fault in the sweep must not stop discovery
                # of the next round.
                _audit(settings, store, "settlement_sweep_error", {"error": str(exc)})
                _safe_notify(
                    notifier,
                    settings,
                    store,
                    "alert",
                    {
                        "reason": "settlement sweep error",
                        "component": "settlement_sweep",
                        "error_type": type(exc).__name__,
                        "error_message": _safe_transport_message(exc),
                    },
                )
            if runtime_watchdog is not None:
                runtime_watchdog.beat("finalize_complete")
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
    slug_markets: Dict[str, GammaMarket] = {}
    for asset in settings.assets:
        slug, _, _ = current_crypto_5m_slug(asset, int(clock()))
        market = public.market_by_slug(slug)
        if not market.condition_id:
            raise LivePreflightError("Gamma market has no condition ID for %s" % slug)
        slug_markets[slug] = market

    first_slug, first_market = next(iter(slug_markets.items()))
    for market in slug_markets.values():
        _probe_stream(market, stream_factory=stream_factory)

    metadata_verified = False
    if settings.is_live:
        if gateway is None:
            raise RuntimeError("live deployment check requires a V2 CLOB gateway")
        for market in slug_markets.values():
            metadata = gateway.market_metadata(market.condition_id)
            _metadata_token_matches(market, metadata, "YES")
            _metadata_token_matches(market, metadata, "NO")
            metadata_verified = True
        gateway.preflight(public.geoblock_status(settings.geo_endpoint), _required_cash(settings, metadata))

    notifier.send(
        format_event(
            "preflight",
            {"dry_run": settings.dry_run, "qty": settings.qty, "assets": list(settings.assets), "slug": first_slug},
        )
    )
    return {
        "mode": "live_deployment_check_passed" if settings.is_live else "dry_run_deployment_check_passed",
        "assets": list(settings.assets),
        "slugs": sorted(slug_markets),
        "gamma_condition_id": first_market.condition_id,
        "websocket_verified": True,
        "metadata_verified": metadata_verified,
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
            if notifier is not None:
                _safe_notify(
                    notifier,
                    settings,
                    store,
                    "alert",
                    {
                        "reason": "settlement position error",
                        "component": "settlement_position",
                        "error_type": type(exc).__name__,
                        "error_message": _safe_transport_message(exc),
                    },
                    record.slug,
                )
    return settled


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Aftertake post-close CLOB runner")
    parser.add_argument("--rounds", type=int, default=1, help="number of fresh 5m rounds per configured asset")
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
    classifier_cfg = active_classifier_config()
    return {
        "strategy": classifier_cfg.strategy_version,
        "dry_run": settings.dry_run,
        "qty": settings.qty,
        "assets": list(settings.assets),
        "live_max_account_risk_fraction": settings.live_max_account_risk_fraction,
        "live_quantity_floor_step": settings.live_quantity_floor_step,
        "dry_run_simulated_balance": settings.dry_run_simulated_balance,
        "resolve_overrides_enabled": bool(parse_resolve_overrides(settings.resolve_overrides)),
        "entry_window_ms": [
            int(classifier_cfg.post_close_start_s * 1000),
            int(classifier_cfg.post_close_end_s * 1000),
        ],
        "confirmations": classifier_cfg.confirmations,
        "confirmation_spacing_ms": int(
            classifier_cfg.confirmation_spacing_s * 1000
        ),
        "require_loser_refill_failure": classifier_cfg.require_loser_refill_failure,
        "require_stable_post_close_leader": classifier_cfg.require_stable_post_close_leader,
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
            boot_payload = {
                "dry_run": settings.dry_run,
                "qty": settings.qty,
                "assets": list(settings.assets),
                "pid": os.getpid(),
                "code_sha": _resolve_code_sha(),
            }
            _audit(settings, store, "boot", boot_payload)
            _safe_notify(notifier, settings, store, "boot", boot_payload)
            runtime_watchdog = RuntimeWatchdog()
            runtime_watchdog.start()
            try:
                _run_round_loop(
                    settings=settings,
                    store=store,
                    public=public,
                    notifier=notifier,
                    forever=args.forever,
                    rounds=args.rounds,
                    runtime_watchdog=runtime_watchdog,
                )
            finally:
                runtime_watchdog.stop()
    finally:
        store.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
