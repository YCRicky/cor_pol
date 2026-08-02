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
import queue
import re
import sys
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import asdict, replace
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from .config import Settings
from .execution import HeartbeatLoop, OrderExecutor, OrderResult
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
from .v9 import V9DualLaneClassifier, active_v9_config

_TELEGRAM_NOTIFIER_TYPE = Notifier

RUNTIME_RETRY_S = 5.0
# A normal round's external work is bounded by the public HTTP timeout and the
# order reconciliation deadline.  This larger supervisor bound is a last
# resort for an SDK/socket call that ignores its own timeout; it must never
# turn one asset into a permanently waiting multi-asset round.
ASSET_ROUND_TIMEOUT_S = 90.0
# Allow the active round to reach its close plus stream shutdown/reconciliation
# even when the process joins it after the five-minute boundary.  The fixed
# timeout remains the minimum bound for a worker that is already past its close.
ASSET_ROUND_COMPLETION_GRACE_S = 30.0
RUNTIME_STALL_TIMEOUT_S = 180.0
RUNTIME_WATCHDOG_INTERVAL_S = 5.0
RUNTIME_WAITING_STAGE_TIMEOUT_S = 360.0
RUNTIME_ACTIVE_STAGE_TIMEOUT_S = 600.0
MAINTENANCE_TIMEOUT_S = 240.0
OBSERVABILITY_RESTART_GRACE_S = 2.0
# This only bounds how long the runner waits to re-check an already-recorded
# websocket observation. Event confirmation and all thresholds remain in
# PostCloseConfig.
POST_CLOSE_POLL_INTERVAL_S = 0.005
# Six assets share one conservative SDK client. Account preflight plus bounded
# sequential metadata retries can consume roughly 75 seconds at their documented
# socket limits; finish that I/O well before the ten-second scene gate.
LIVE_PREFLIGHT_LEAD_S = 150.0
_AUDIT_QUEUE: queue.Queue[tuple] = queue.Queue(maxsize=4096)
_AUDIT_THREAD: Optional[threading.Thread] = None
_AUDIT_THREAD_LOCK = threading.Lock()
_NOTIFY_QUEUE: queue.Queue[tuple] = queue.Queue(maxsize=512)
_NOTIFY_THREAD: Optional[threading.Thread] = None
_NOTIFY_THREAD_LOCK = threading.Lock()
_NOTIFY_REGISTRY: Dict[str, tuple] = {}
_NOTIFY_REGISTRY_LOCK = threading.Lock()
_NOTIFY_QUEUED: set[str] = set()
_NOTIFY_QUEUED_LOCK = threading.Lock()
_NOTIFY_RETRY_CAP_S = 60.0


def _run_diagnostics_then_restart(
    restart_fn: Callable[[], None],
    diagnostics: Callable[[], None],
    *,
    grace_s: float = OBSERVABILITY_RESTART_GRACE_S,
) -> None:
    """Persist a fatal-path alert before exit, with a bounded escape hatch.

    SQLite or Telegram may themselves be wedged. The fallback thread therefore
    requests the process restart after a short grace period, while the normal
    path runs diagnostics first so the durable notification outbox is populated
    before systemd recreates the process.
    """

    requested = threading.Event()
    diagnostics_done = threading.Event()
    request_lock = threading.Lock()

    def request_once() -> None:
        with request_lock:
            if requested.is_set():
                return
            requested.set()
        try:
            restart_fn()
        except Exception:
            # The process watchdog/systemd is the final recovery boundary. A
            # test or injected restart callback may return or raise, but that
            # must never strand the diagnostics worker.
            return

    def emergency_restart() -> None:
        if not diagnostics_done.wait(max(0.0, float(grace_s))):
            request_once()

    fallback = threading.Thread(
        target=emergency_restart,
        daemon=True,
        name="aftertake-observability-restart",
    )
    fallback.start()
    try:
        diagnostics()
    finally:
        diagnostics_done.set()
        request_once()


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
        fatal_callback: Optional[Callable[[str, Dict[str, Any]], None]] = None,
    ):
        self.stale_after_s = max(0.01, float(stale_after_s))
        self.interval_s = max(0.1, float(interval_s))
        self._monotonic = monotonic
        self._exit_fn = exit_fn
        self._fatal_callback = fatal_callback
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._last_progress = self._monotonic()
        self._stage = "boot"
        self._thread: Optional[threading.Thread] = None

    def set_fatal_callback(
        self, callback: Optional[Callable[[str, Dict[str, Any]], None]]
    ) -> None:
        """Attach durable diagnostics after startup dependencies are ready."""

        self._fatal_callback = callback

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
            # These stages legitimately outlive the default 180-second stall
            # threshold, but they are not exempt forever. A broken scheduler or
            # supervisor must still be recreated by systemd.
            limit = self.stale_after_s
            if stage == "waiting_for_round":
                limit = max(limit, RUNTIME_WAITING_STAGE_TIMEOUT_S)
            elif stage == "active_round":
                limit = max(limit, RUNTIME_ACTIVE_STAGE_TIMEOUT_S)
            if age >= limit:
                payload = {
                    "stage": stage,
                    "age_s": age,
                    "limit_s": limit,
                    "action": "process_restart_requested",
                }

                def persist_stall_diagnostics(
                    stall_items: tuple = tuple(payload.items()),
                ) -> None:
                    stall_payload = dict(stall_items)
                    print(
                        json.dumps(
                            {
                                "kind": "runtime_watchdog_stall",
                                "reason": "runtime made no progress before watchdog deadline",
                                **stall_payload,
                            },
                            sort_keys=True,
                        ),
                        file=sys.stderr,
                        flush=True,
                    )
                    callback = self._fatal_callback
                    if callback is not None:
                        callback("runtime watchdog stall", dict(stall_payload))

                _run_diagnostics_then_restart(
                    lambda: self._exit_fn(1),
                    persist_stall_diagnostics,
                )
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


def _write_audit_now(
    settings: Settings,
    store: StateStore,
    kind: str,
    payload: Dict[str, Any],
    slug: str = "",
) -> None:
    if getattr(store, "closed", False):
        return
    failures: List[str] = []
    try:
        store.append_event(kind, payload, slug=slug)
    except Exception as exc:
        failures.append("sqlite:%s" % _safe_transport_message(exc))
    target = settings.out_dir / ("aftertake_%s.jsonl" % slug if slug else "runtime.jsonl")
    try:
        append_jsonl(target, {"kind": kind, **payload})
    except Exception as exc:
        failures.append("jsonl:%s" % _safe_transport_message(exc))
    if failures:
        # stderr is captured by journald under systemd. Diagnostic persistence
        # failures must be visible, but must never suppress Telegram or turn a
        # qualified order into a retry.
        print(
            json.dumps(
                {
                    "kind": "audit_persistence_failed",
                    "event": kind,
                    "slug": slug,
                    "errors": failures,
                },
                sort_keys=True,
            ),
            file=sys.stderr,
            flush=True,
        )


def _audit_worker() -> None:
    while True:
        settings, store, kind, payload, slug = _AUDIT_QUEUE.get()
        try:
            try:
                _write_audit_now(settings, store, kind, payload, slug)
            except Exception as exc:
                print(
                    json.dumps(
                        {
                            "kind": "audit_worker_error",
                            "event": kind,
                            "slug": slug,
                            "error": _safe_transport_message(exc),
                        },
                        sort_keys=True,
                    ),
                    file=sys.stderr,
                    flush=True,
                )
        finally:
            _AUDIT_QUEUE.task_done()


def _ensure_audit_worker() -> None:
    global _AUDIT_THREAD
    with _AUDIT_THREAD_LOCK:
        if _AUDIT_THREAD is None or not _AUDIT_THREAD.is_alive():
            _AUDIT_THREAD = threading.Thread(
                target=_audit_worker,
                daemon=True,
                name="aftertake-audit-writer",
            )
            _AUDIT_THREAD.start()


def _audit(
    settings: Settings,
    store: StateStore,
    kind: str,
    payload: Dict[str, Any],
    slug: str = "",
) -> None:
    # Asset/notification/maintenance workers must never block on diagnostic
    # SQLite or disk I/O.  One bounded writer replaces unbounded per-event
    # threads and keeps authoritative reservation state on the caller thread.
    background_names = (
        "aftertake-asset",
        "aftertake-notifier",
        "aftertake-maintenance",
        "aftertake-order-event",
    )
    if threading.current_thread().name.startswith(background_names):
        _ensure_audit_worker()
        try:
            _AUDIT_QUEUE.put_nowait((settings, store, kind, dict(payload), slug))
        except queue.Full:
            print(
                json.dumps(
                    {"kind": "audit_queue_full", "event": kind, "slug": slug},
                    sort_keys=True,
                ),
                file=sys.stderr,
                flush=True,
            )
        return
    _write_audit_now(settings, store, kind, payload, slug)


def _mark_component_unhealthy(store: StateStore, component: str, detail: str) -> bool:
    try:
        return store.mark_component_unhealthy(component, detail)
    except Exception as exc:
        print(
            json.dumps(
                {
                    "kind": "component_health_persistence_failed",
                    "component": component,
                    "target_status": "unhealthy",
                    "error": str(exc),
                },
                sort_keys=True,
            ),
            file=sys.stderr,
            flush=True,
        )
        return False


def _mark_component_healthy(store: StateStore, component: str, detail: str) -> bool:
    try:
        return store.mark_component_healthy(component, detail)
    except Exception as exc:
        print(
            json.dumps(
                {
                    "kind": "component_health_persistence_failed",
                    "component": component,
                    "target_status": "healthy",
                    "error": str(exc),
                },
                sort_keys=True,
            ),
            file=sys.stderr,
            flush=True,
        )
        return False


def _notification_component(kind: str, payload: Dict[str, Any], slug: str) -> str:
    component = str(payload.get("component") or "").strip()
    asset = str(payload.get("asset") or "").strip().upper()
    if component.startswith("asset:"):
        return component
    if not component and asset:
        return "asset:%s" % asset
    if component and slug:
        return "%s:%s" % (component, slug)
    if component:
        return component
    if kind in {"alert", "recovery_success"} and slug:
        return "market:%s" % slug
    return ""


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

    message = format_event(kind, payload, slug)
    component = _notification_component(kind, payload, slug)

    # Production Telegram delivery is durable and at-least-once. Persist first
    # even for an asset/heartbeat callback, then let one bounded sender perform
    # network I/O outside the strategy and heartbeat threads. A failed send
    # remains pending across process restarts.
    # Injected test notifiers stay synchronous for deterministic assertions.
    if isinstance(notifier, _TELEGRAM_NOTIFIER_TYPE):
        notification_id = ""
        try:
            notification_id = store.enqueue_notification(
                kind, message, slug, component
            )
        except Exception as exc:
            # Journald remains the final observability channel if even the
            # durable state volume is unavailable. Still attempt one queued
            # TG delivery, but never perform Telegram I/O on this caller.
            print(
                json.dumps(
                    {
                        "kind": "notification_persistence_failed",
                        "event": kind,
                        "slug": slug,
                        "error": _safe_transport_message(exc),
                    },
                    sort_keys=True,
                ),
                file=sys.stderr,
                flush=True,
            )
        _register_notification_context(notifier, settings, store)
        _queue_notification_delivery(
            notifier,
            settings,
            store,
            notification_id,
            message,
            kind,
            slug,
            component,
            persist_before_send=False,
        )
        return

    try:
        notifier.send(message)
        _audit(settings, store, "notification_sent", {"event": kind}, slug)
    except Exception as exc:
        _audit(
            settings,
            store,
            "notification_failed",
            {"event": kind, "error": str(exc)},
            slug,
        )


def _transition_component_and_notify(
    *,
    store: StateStore,
    component: str,
    status: str,
    detail: str,
    notifier: Optional[Notifier],
    settings: Settings,
    kind: str,
    payload: Dict[str, Any],
    slug: str = "",
    notify_on_no_transition: bool = False,
) -> bool:
    """Atomically persist a health edge and its Telegram outbox row."""

    enabled = notifier is not None and notifier.enabled
    if not enabled or not isinstance(notifier, _TELEGRAM_NOTIFIER_TYPE):
        transitioned = (
            _mark_component_healthy(store, component, detail)
            if status == "healthy"
            else _mark_component_unhealthy(store, component, detail)
        )
        if enabled and (transitioned or notify_on_no_transition):
            _safe_notify(notifier, settings, store, kind, payload, slug)
        return transitioned

    message = format_event(kind, payload, slug)
    try:
        transitioned, notification_id = store.transition_component_and_enqueue_notification(
            component=component,
            status=status,
            detail=detail,
            kind=kind,
            message=message,
            slug=slug,
            enqueue_on_no_transition=notify_on_no_transition,
        )
    except Exception as exc:
        print(
            json.dumps(
                {
                    "kind": "component_notification_transaction_failed",
                    "component": component,
                    "target_status": status,
                    "error": _safe_transport_message(exc),
                },
                sort_keys=True,
            ),
            file=sys.stderr,
            flush=True,
        )
        transitioned = (
            _mark_component_healthy(store, component, detail)
            if status == "healthy"
            else _mark_component_unhealthy(store, component, detail)
        )
        if transitioned or notify_on_no_transition:
            _safe_notify(notifier, settings, store, kind, payload, slug)
        return transitioned

    if notification_id:
        _register_notification_context(notifier, settings, store)
        _queue_notification_delivery(
            notifier,
            settings,
            store,
            notification_id,
            message,
            kind,
            slug,
            component,
        )
    return transitioned


def _register_notification_context(
    notifier: Notifier,
    settings: Settings,
    store: StateStore,
) -> None:
    key = str(store.path.resolve())
    with _NOTIFY_REGISTRY_LOCK:
        _NOTIFY_REGISTRY[key] = (notifier, settings, store)
    _ensure_notification_worker()


def _queue_notification_delivery(
    notifier: Notifier,
    settings: Settings,
    store: StateStore,
    notification_id: str,
    message: str,
    kind: str,
    slug: str,
    component: str = "",
    *,
    persist_before_send: bool = False,
) -> bool:
    dedupe_id = str(notification_id or "")
    if dedupe_id:
        with _NOTIFY_QUEUED_LOCK:
            if dedupe_id in _NOTIFY_QUEUED:
                return True
            _NOTIFY_QUEUED.add(dedupe_id)
    try:
        _NOTIFY_QUEUE.put_nowait(
            (
                notifier,
                settings,
                store,
                dedupe_id,
                str(message),
                str(kind),
                str(slug),
                str(component),
                bool(persist_before_send),
            )
        )
        return True
    except queue.Full:
        if dedupe_id:
            with _NOTIFY_QUEUED_LOCK:
                _NOTIFY_QUEUED.discard(dedupe_id)
        if persist_before_send and not dedupe_id:
            try:
                store.enqueue_notification(kind, message, slug, component)
            except Exception as exc:
                print(
                    json.dumps(
                        {
                            "kind": "notification_persistence_failed",
                            "event": kind,
                            "slug": slug,
                            "error": _safe_transport_message(exc),
                        },
                        sort_keys=True,
                    ),
                    file=sys.stderr,
                    flush=True,
                )
        print(
            json.dumps(
                {
                    "kind": "notification_queue_full",
                    "event": kind,
                    "slug": slug,
                    "message": str(message)[:1000],
                },
                sort_keys=True,
            ),
            file=sys.stderr,
            flush=True,
        )
        # Persisted rows are recovered by the worker's periodic outbox scan.
        return False


def _notification_retry_delay(attempts: int) -> float:
    return min(_NOTIFY_RETRY_CAP_S, float(2 ** min(max(0, int(attempts)), 6)))


def _deliver_notification_task(task: tuple) -> None:
    (
        notifier,
        settings,
        store,
        notification_id,
        fallback_message,
        fallback_kind,
        fallback_slug,
        fallback_component,
        persist_before_send,
    ) = task
    if getattr(store, "closed", False):
        return
    row: Optional[Dict[str, Any]] = None
    if persist_before_send and not notification_id:
        try:
            notification_id = store.enqueue_notification(
                fallback_kind,
                fallback_message,
                fallback_slug,
                fallback_component,
            )
        except Exception as exc:
            print(
                json.dumps(
                    {
                        "kind": "notification_persistence_failed",
                        "event": fallback_kind,
                        "slug": fallback_slug,
                        "error": _safe_transport_message(exc),
                    },
                    sort_keys=True,
                ),
                file=sys.stderr,
                flush=True,
            )
    if notification_id:
        try:
            row = store.deliverable_notification(
                notification_id, ready_at=time.time()
            )
        except Exception as exc:
            print(
                json.dumps(
                    {
                        "kind": "notification_outbox_read_failed",
                        "notification_id": notification_id,
                        "error": _safe_transport_message(exc),
                    },
                    sort_keys=True,
                ),
                file=sys.stderr,
                flush=True,
            )
            return
        if row is None:
            return
    message = str((row or {}).get("message") or fallback_message)
    kind = str((row or {}).get("kind") or fallback_kind)
    slug = str((row or {}).get("slug") or fallback_slug)
    attempts = int((row or {}).get("attempts") or 0)
    try:
        notifier.send(message)
        if notification_id:
            store.mark_notification_sent(notification_id)
        _audit(settings, store, "notification_sent", {"event": kind}, slug)
    except Exception as exc:
        if notification_id:
            try:
                store.mark_notification_failed(
                    notification_id,
                    _safe_transport_message(exc),
                    retry_at=time.time() + _notification_retry_delay(attempts),
                )
            except Exception as persistence_exc:
                print(
                    json.dumps(
                        {
                            "kind": "notification_outbox_ack_failed",
                            "notification_id": notification_id,
                            "error": _safe_transport_message(persistence_exc),
                        },
                        sort_keys=True,
                    ),
                    file=sys.stderr,
                    flush=True,
                )
        _audit(
            settings,
            store,
            "notification_failed",
            {"event": kind, "error": _safe_transport_message(exc)},
            slug,
        )


def _queue_pending_notifications() -> None:
    with _NOTIFY_REGISTRY_LOCK:
        contexts = list(_NOTIFY_REGISTRY.values())
    ready_at = time.time()
    for notifier, settings, store in contexts:
        if getattr(store, "closed", False):
            with _NOTIFY_REGISTRY_LOCK:
                _NOTIFY_REGISTRY.pop(str(store.path.resolve()), None)
            continue
        try:
            rows = store.pending_notifications(ready_at=ready_at, limit=100)
        except Exception:
            continue
        for row in rows:
            _queue_notification_delivery(
                notifier,
                settings,
                store,
                str(row["notification_id"]),
                str(row["message"]),
                str(row["kind"]),
                str(row["slug"]),
                str(row.get("component") or ""),
            )


def _notification_worker() -> None:
    while True:
        try:
            task = _NOTIFY_QUEUE.get(timeout=0.5)
        except queue.Empty:
            _queue_pending_notifications()
            continue
        notification_id = str(task[3] or "")
        try:
            _deliver_notification_task(task)
        except Exception as exc:
            # One malformed row or closed test store must never kill the sole
            # sender and strand every later production notification.
            print(
                json.dumps(
                    {
                        "kind": "notification_worker_error",
                        "error": _safe_transport_message(exc),
                    },
                    sort_keys=True,
                ),
                file=sys.stderr,
                flush=True,
            )
        finally:
            if notification_id:
                with _NOTIFY_QUEUED_LOCK:
                    _NOTIFY_QUEUED.discard(notification_id)
            _NOTIFY_QUEUE.task_done()
        if _NOTIFY_QUEUE.empty():
            _queue_pending_notifications()


def _ensure_notification_worker() -> None:
    global _NOTIFY_THREAD
    with _NOTIFY_THREAD_LOCK:
        if _NOTIFY_THREAD is None or not _NOTIFY_THREAD.is_alive():
            _NOTIFY_THREAD = threading.Thread(
                target=_notification_worker,
                daemon=True,
                name="aftertake-notifier",
            )
            _NOTIFY_THREAD.start()




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


def _settlement_semantics_label(market: GammaMarket) -> str:
    """Return only a structural Gamma label; never infer the future outcome."""

    outcomes = {str(outcome).strip().lower() for outcome in market.outcomes}
    if outcomes == {"up", "down"} and len(market.outcomes) == 2:
        return "binary_up_down"
    if outcomes == {"yes", "no"} and len(market.outcomes) == 2:
        return "binary_yes_no"
    return "unverified"


def _classifier_config_for_settings(settings: Settings) -> Any:
    if settings.strategy_family == "v9":
        return active_v9_config()
    return active_classifier_config()


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
    """Share one snapshot and one atomic spend budget across all round assets."""

    def __init__(
        self,
        settings: Settings,
        store: StateStore,
        public: PolymarketPublicClient,
        gateway: V2ClobGateway,
    ):
        self._settings = settings
        self._store = store
        self._public = public
        self._gateway = gateway
        self._lock = threading.RLock()
        self._snapshot: Optional[LivePreflight] = None
        self._error: Optional[BaseException] = None
        self._existing_exposure = 0.0
        self._claimed_costs: Dict[int, float] = {}
        self._next_claim_id = 1

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
                    # Freeze pre-existing durable risk once. New reservations in
                    # this same round are represented by ``_claimed_costs`` so
                    # they are never double-counted while workers race.
                    self._existing_exposure = max(0.0, self._store.total_risk_exposure())
                except Exception as exc:
                    self._error = exc
                    raise
            return self._snapshot

    def claim_entry_size(
        self,
        *,
        price: float,
        available_size: float,
        metadata: MarketMetadata,
    ) -> tuple[LiveSizingDecision, int]:
        """Floor-size and atomically reserve cash for one candidate."""

        with self._lock:
            preflight = self.snapshot()
            collateral = preflight.collateral
            total_budget = min(
                float(collateral.balance)
                * float(self._settings.live_max_account_risk_fraction),
                float(collateral.allowance),
            )
            remaining = max(
                0.0,
                total_budget
                - self._existing_exposure
                - sum(self._claimed_costs.values()),
            )
            if remaining <= 0:
                raise RiskRejected("shared_account_risk_budget_exhausted")

            fraction = float(self._settings.live_max_account_risk_fraction)
            effective_collateral = BalanceAllowance(
                balance=min(float(collateral.balance), remaining / fraction),
                allowance=min(float(collateral.allowance), remaining),
                raw={"shared_round_budget": True},
            )
            sizing = compute_live_entry_size(
                price=price,
                available_size=available_size,
                collateral=effective_collateral,
                metadata=metadata,
                max_account_fraction=fraction,
                quantity_step=self._settings.live_quantity_floor_step,
            )
            if not sizing.accepted:
                raise RiskRejected(sizing.reason or "shared_account_sizing_rejected")
            if sizing.estimated_total_cost > remaining + 1e-9:
                raise RiskRejected("shared_account_risk_budget_exceeded")
            claim_id = self._next_claim_id
            self._next_claim_id += 1
            self._claimed_costs[claim_id] = sizing.estimated_total_cost
            # Preserve the real account snapshot in audit output while the
            # sizing budget records the remaining shared cap used by this claim.
            sizing = replace(
                sizing,
                account_balance=float(collateral.balance),
                collateral_allowance=float(collateral.allowance),
                risk_budget=remaining,
            )
            return sizing, claim_id

    def release_claim(self, claim_id: int) -> None:
        """Release a claim only when no durable order intent was created."""

        with self._lock:
            self._claimed_costs.pop(int(claim_id), None)


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
        for phrase in (
            "request exception",
            "connection",
            "timeout",
            "temporarily unavailable",
            "market stream",
            "websocket",
            "socket",
        )
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
    component = "asset:%s" % asset
    detail = "%s: %s" % (phase, payload["error_message"])
    _audit(
        settings,
        store,
        "asset_transport_error",
        payload,
        slug,
    )
    if notifier is not None:
        _transition_component_and_notify(
            store=store,
            component=component,
            status="unhealthy",
            detail=detail,
            notifier=notifier,
            settings=settings,
            kind="alert",
            payload={
                "reason": "asset transport error",
                "component": component,
                **payload,
            },
            slug=slug,
            notify_on_no_transition=True,
        )
    else:
        _mark_component_unhealthy(store, component, detail)


def _update_asset_error_state(
    *,
    settings: Settings,
    store: StateStore,
    notifier: Optional[Notifier],
    asset: str,
    decisions: List[PostCloseDecision],
    error_state: Optional[set[str]],
) -> None:
    key = "asset:%s" % asset
    failed = any(
        "transport_error" in str(decision.reason)
        or str(decision.reason)
        in {
            "asset_round_timeout",
            "asset_round_error",
            "market_stream_not_ready",
            "paired_post_close_state_not_fresh",
            "post_close_preflight_missed",
            "entry_runtime_error",
        }
        for decision in decisions
    )
    if failed:
        detail = ",".join(sorted({str(item.reason) for item in decisions}))
        if notifier is not None and any(
            str(item.reason) == "paired_post_close_state_not_fresh"
            for item in decisions
        ):
            _transition_component_and_notify(
                store=store,
                component=key,
                status="unhealthy",
                detail=detail,
                notifier=notifier,
                settings=settings,
                kind="alert",
                payload={
                    "reason": "paired post-close book freshness failed",
                    "component": key,
                    "asset": asset,
                    "error_type": "MarketDataFreshnessError",
                    "error_message": detail,
                },
                notify_on_no_transition=True,
            )
        else:
            _mark_component_unhealthy(store, key, detail)
        if error_state is not None:
            error_state.add(key)
    else:
        force_notify = error_state is not None and key in error_state
        if error_state is not None:
            error_state.discard(key)
        _transition_component_and_notify(
            store=store,
            component=key,
            status="healthy",
            detail="asset round completed",
            notifier=notifier,
            settings=settings,
            kind="recovery_success",
            payload={"component": key, "reason": "asset round recovered"},
            notify_on_no_transition=force_notify,
        )


def _restart_after_executor_timeout(
    *,
    settings: Settings,
    store: StateStore,
    notifier: Optional[Notifier],
    executor: OrderExecutor,
    restart_fn: Optional[Callable[[], None]],
    component: str = "clob_executor",
    slug: str = "",
) -> bool:
    """Persist a fatal SDK-timeout alert, then recreate the process once."""

    if not bool(getattr(executor, "process_restart_required", False)):
        return False
    reason = str(
        getattr(executor, "process_restart_reason", "")
        or "CLOB SDK call exceeded its bounded deadline"
    )

    def persist_timeout_diagnostics() -> None:
        payload = {
            "reason": "CLOB SDK worker exceeded bounded timeout",
            "component": component,
            "error_type": "TimeoutError",
            "error_message": reason,
            "action": "process_restart_requested",
        }
        _audit(settings, store, "clob_worker_timeout", payload, slug)
        if notifier is not None:
            _transition_component_and_notify(
                store=store,
                component=component,
                status="unhealthy",
                detail=reason,
                notifier=notifier,
                settings=settings,
                kind="alert",
                payload=payload,
                slug=slug,
                notify_on_no_transition=True,
            )

    if restart_fn is not None:
        _run_diagnostics_then_restart(restart_fn, persist_timeout_diagnostics)
    else:
        persist_timeout_diagnostics()
    return True


def _restart_after_heartbeat_fatal(
    *,
    settings: Settings,
    store: StateStore,
    notifier: Optional[Notifier],
    reason: str,
    payload: Dict[str, Any],
    restart_fn: Optional[Callable[[], None]],
) -> None:
    """Persist the heartbeat ALERT before the process-level recovery boundary."""

    alert_payload = {
        **payload,
        "reason": reason,
        "component": "clob_heartbeat",
        "error_type": "HeartbeatFatal",
        "error_message": str(payload.get("error_message") or reason),
        "action": "process_restart_requested",
    }

    def persist_heartbeat_diagnostics() -> None:
        # Keep journald as an immediate fallback, but persist the same ALERT
        # before asking systemd to recreate the process. The old path exited
        # here first, so a full heartbeat status queue could lose the only
        # Telegram/outbox record of the failure.
        print(
            json.dumps(
                {
                    "kind": "heartbeat_process_restart",
                    **alert_payload,
                },
                sort_keys=True,
            ),
            file=sys.stderr,
            flush=True,
        )
        _audit(settings, store, "heartbeat_fatal", alert_payload)
        _transition_component_and_notify(
            store=store,
            component="clob_heartbeat",
            status="unhealthy",
            detail=str(alert_payload["error_message"]),
            notifier=notifier,
            settings=settings,
            kind="alert",
            payload=alert_payload,
            notify_on_no_transition=True,
        )

    def hard_exit() -> None:
        os._exit(1)

    _run_diagnostics_then_restart(
        restart_fn if restart_fn is not None else hard_exit,
        persist_heartbeat_diagnostics,
    )


def _entry_qty_for_decision(
    *,
    settings: Settings,
    decision: PostCloseDecision,
    metadata: MarketMetadata,
    preflight: Optional[LivePreflight],
    round_preflight: Optional[_RoundAccountPreflight] = None,
) -> tuple[float, Optional[LiveSizingDecision], Optional[int]]:
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
        return sizing.qty, sizing, None
    if preflight is None:
        raise RiskRejected("live_preflight_snapshot_missing")
    if round_preflight is not None:
        sizing, claim_id = round_preflight.claim_entry_size(
            price=decision.entry_ask,
            available_size=decision.entry_ask_size,
            metadata=metadata,
        )
        return sizing.qty, sizing, claim_id
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
    return sizing.qty, sizing, None


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
    restart_fn: Optional[Callable[[], None]] = None,
) -> List[PostCloseDecision]:
    """Run exactly one newly opened 5-minute Aftertake round.

    The expensive authenticated checks begin two minutes before close and must
    complete before the close-critical window.
    The entry path after the classifier confirms does not perform REST, Telegram
    or retry I/O, so a qualifying residual ask is not intentionally delayed.
    """

    active_asset = str(asset or settings.asset).upper().strip()
    slug, expected_start, round_end = current_crypto_5m_slug(active_asset, round_start)
    if expected_start != int(round_start):
        raise ValueError("run_round requires a 5-minute boundary")
    if settings.strategy_family == "v9" and settings.is_live and not settings.v9_live_enabled:
        raise LivePreflightError("V9 live trading requires AFTERTAKE_V9_LIVE_ENABLED=true")
    market = public.market_by_slug(slug)
    if not market.condition_id:
        raise LivePreflightError("Gamma market has no condition ID")
    store.observe_market(slug, market.condition_id, round_start)
    yes_token = market.token_for_side("YES")
    no_token = market.token_for_side("NO")
    metadata = _build_dry_metadata(market)
    classifier_cfg = _classifier_config_for_settings(settings)
    if settings.strategy_family == "v9":
        classifier = V9DualLaneClassifier(
            classifier_cfg,
            settlement_label=_settlement_semantics_label(market),
            code_sha=_resolve_code_sha(),
        )
    else:
        classifier = PostCloseWinnerClassifier(classifier_cfg)
    notifier = notifier or Notifier(token=settings.telegram_token, chat_id=settings.telegram_chat_id)
    stream = stream_factory(
        yes_token_id=yes_token,
        no_token_id=no_token,
        on_book=classifier.record,
        on_reset=classifier.reset,
        clock=clock,
        near_touch_band=classifier_cfg.near_touch_band,
        resolve_overrides=parse_resolve_overrides(settings.resolve_overrides),
    )
    decisions: List[PostCloseDecision] = []
    seen_decisions: set = set()
    preflight_done = settings.dry_run
    live_preflight: Optional[LivePreflight] = None
    preflight_at = float(round_end) - LIVE_PREFLIGHT_LEAD_S
    stream_health_confirmed = False
    stream_component = "market_stream:%s" % active_asset

    stream.start()
    stream_generation = int(getattr(stream, "generation", 0))
    try:
        while True:
            now = float(clock())
            current_generation = int(getattr(stream, "generation", stream_generation))
            if current_generation != stream_generation:
                reconnect_count = int(getattr(stream, "reconnect_count", 0))
                # MarketBookStream clears the classifier synchronously at the
                # generation boundary, before it can publish new books.  This
                # loop only observes/report the transition.  Clearing here is
                # racy because a fresh snapshot may already have arrived.
                stream_generation = current_generation
                _audit(
                    settings,
                    store,
                    (
                        "market_stream_reconnected"
                        if reconnect_count > 0
                        else "market_stream_initialized"
                    ),
                    {
                        "generation": stream_generation,
                        "reconnect_count": reconnect_count,
                        "last_error": str(getattr(stream, "last_error", "") or ""),
                    },
                    slug,
                )
                if reconnect_count > 0:
                    stream_health_confirmed = False
                    stream_error = str(
                        getattr(stream, "last_error", "") or "market stream disconnected"
                    )
                    _transition_component_and_notify(
                        store=store,
                        component=stream_component,
                        status="unhealthy",
                        detail=stream_error,
                        notifier=notifier,
                        settings=settings,
                        kind="alert",
                        payload={
                            "reason": "market stream reconnecting",
                            "component": stream_component,
                            "generation": stream_generation,
                            "reconnect_count": reconnect_count,
                            "error_message": stream_error,
                        },
                        slug=slug,
                        notify_on_no_transition=True,
                    )
            if stream.ready and not stream_health_confirmed:
                stream_health_confirmed = True
                _transition_component_and_notify(
                    store=store,
                    component=stream_component,
                    status="healthy",
                    detail="fresh paired book restored",
                    notifier=notifier,
                    settings=settings,
                    kind="recovery_success",
                    payload={
                        "component": stream_component,
                        "reason": "fresh paired book restored",
                        "details": "generation=%s reconnect_count=%s"
                        % (
                            current_generation,
                            int(getattr(stream, "reconnect_count", 0)),
                        ),
                    },
                    slug=slug,
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
                    _transition_component_and_notify(
                        store=store,
                        component=stream_component,
                        status="unhealthy",
                        detail=reason,
                        notifier=notifier,
                        settings=settings,
                        kind="alert",
                        payload={"reason": reason, "component": stream_component},
                        slug=slug,
                        notify_on_no_transition=True,
                    )
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
            # One audit row per logical transition avoids close-time disk I/O
            # on every websocket timestamp.
            key = (decision.action, decision.reason, decision.side)
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

            round_claim_id: Optional[int] = None
            claim_committed = False
            try:
                # Do not re-fetch the public REST book here: the websocket
                # observation is the executable premise and this is its short window.
                entry_qty, live_sizing, round_claim_id = _entry_qty_for_decision(
                    settings=settings,
                    decision=decision,
                    metadata=metadata,
                    preflight=live_preflight,
                    round_preflight=round_preflight,
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
                # SQLite now owns the fail-closed reservation. Keep the shared
                # cash claim even if submission acknowledgement is ambiguous.
                claim_committed = True
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
                if (
                    round_claim_id is not None
                    and result.terminal
                    and result.filled_qty <= 0
                    and round_preflight is not None
                ):
                    # A definitive zero-fill consumes no account collateral;
                    # let another asset in this same close window use it.
                    round_preflight.release_claim(round_claim_id)
                    round_claim_id = None
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
                if settings.is_live and _restart_after_executor_timeout(
                    settings=settings,
                    store=store,
                    notifier=notifier,
                    executor=executor,
                    restart_fn=restart_fn,
                    component="clob_executor:%s" % active_asset,
                    slug=slug,
                ):
                    decisions.append(
                        PostCloseDecision("hold", "clob_worker_timeout", side=decision.side)
                    )
                    break
                result_raw = result.raw or {}
                if (
                    (
                        result.submission_state == "unknown"
                        and bool(result_raw.get("ambiguous_submission"))
                    )
                    or bool(result_raw.get("reconcile_transport_error"))
                ):
                    decisions.append(
                        PostCloseDecision("hold", "submit_transport_error", side=decision.side)
                    )
                break
            except (RiskRejected, LivePreflightError) as exc:
                if audit_decision is not None:
                    _audit_decision(settings, store, audit_decision, slug)
                _audit(settings, store, "entry_blocked", {"reason": str(exc)}, slug)
                _safe_notify(
                    notifier,
                    settings,
                    store,
                    "alert",
                    {
                        "reason": "entry blocked by risk/preflight",
                        "component": "entry_risk",
                        "error_type": type(exc).__name__,
                        "error_message": str(exc),
                    },
                    slug,
                )
                decisions.append(PostCloseDecision("hold", str(exc), side=decision.side))
                break
            except Exception as exc:
                if audit_decision is not None:
                    _audit_decision(settings, store, audit_decision, slug)
                _audit(settings, store, "entry_runtime_error", {"error": str(exc)}, slug)
                _safe_notify(notifier, settings, store, "alert", {"reason": str(exc)}, slug)
                decisions.append(PostCloseDecision("hold", "entry_runtime_error", side=decision.side))
                break
            finally:
                if round_claim_id is not None and not claim_committed and round_preflight is not None:
                    round_preflight.release_claim(round_claim_id)
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
    restart_fn: Optional[Callable[[], None]] = None,
) -> Dict[str, List[PostCloseDecision]]:
    """Run all configured assets for the same 5-minute round concurrently."""

    assets = tuple(settings.assets)
    round_preflight = (
        _RoundAccountPreflight(settings, store, public, live_gateway)
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
            restart_fn=restart_fn,
        ): asset
        for asset in assets
    }
    pending = set(futures)
    # ``_select_next_round_start`` intentionally joins an active market when
    # enough pre-close lead remains.  A fixed 90-second deadline would then
    # kill a perfectly healthy worker before that market closes (the exact
    # failure mode seen after a mid-round service restart).  Budget through
    # the round close, the configured reconciliation lifecycle, and cleanup,
    # while retaining the fixed timeout as the minimum for already-expired or
    # genuinely stuck workers.
    round_completion_s = (
        float(round_start)
        + CRYPTO_5M_WINDOW_S
        + float(active_classifier_config().post_close_end_s)
        + float(settings.reconcile_timeout_s)
        + ASSET_ROUND_COMPLETION_GRACE_S
        - time.time()
    )
    effective_timeout_s = max(0.01, float(timeout_s), round_completion_s)
    deadline = time.monotonic() + effective_timeout_s
    try:
        while pending:
            remaining = max(0.0, deadline - time.monotonic())
            done, pending = wait(pending, timeout=remaining, return_when=FIRST_COMPLETED)
            if not done:
                timed_out_pending = tuple(pending)
                timed_out_names: List[str] = []

                def persist_timeout_diagnostics(
                    pending_futures: tuple = timed_out_pending,
                    timed_out_assets: List[str] = timed_out_names,
                ) -> None:
                    for future in pending_futures:
                        asset = futures[future]
                        timed_out_assets.append(asset)
                        slug, _, _ = current_crypto_5m_slug(asset, round_start)
                        timeout_error = TimeoutError(
                            "asset round exceeded supervisor timeout %.1fs" % effective_timeout_s
                        )
                        _audit_asset_transport_error(
                            settings,
                            store,
                            asset=asset,
                            slug=slug,
                            phase="asset_round_timeout",
                            exc=timeout_error,
                            notifier=None,
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
                    if notifier is not None:
                        _safe_notify(
                            notifier,
                            settings,
                            store,
                            "alert",
                            {
                                "reason": "asset round supervisor timeout",
                                "component": "asset_supervisor",
                                "asset": ",".join(sorted(timed_out_assets)),
                                "phase": "asset_round_timeout",
                                "error_type": "TimeoutError",
                                "error_message": "uncancellable workers require process restart",
                            },
                        )

                if restart_fn is not None:
                    _run_diagnostics_then_restart(restart_fn, persist_timeout_diagnostics)
                else:
                    persist_timeout_diagnostics()
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
                    reason = (
                        "asset_round_transport_error"
                        if _is_transient_transport_error(exc)
                        else "asset_round_error"
                    )
                    # A completed asset-local failure cannot leave an
                    # uncancellable worker behind. Isolate the asset and allow
                    # every other market plus the next round to continue.
                    results[asset] = [PostCloseDecision("hold", reason)]
                # Do not restart on the first completed asset-local transport
                # error: another worker may already have POSTed and still be
                # persisting its acknowledgement. The outer round loop requests
                # one process restart only after all bounded workers finish.
                # A genuinely hung worker is handled by the supervisor timeout
                # branch above, where an immediate restart is required.
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


def _round_lifecycle_payload(
    *,
    round_start: int,
    assets: tuple[str, ...],
    round_results: Optional[Dict[str, List[PostCloseDecision]]] = None,
) -> Dict[str, Any]:
    """Build one compact, durable proof that the scheduler ran a round."""

    payload: Dict[str, Any] = {
        "round_start": int(round_start),
        "assets": list(assets),
    }
    if round_results is None:
        return payload

    asset_results: Dict[str, Dict[str, Any]] = {}
    qualified_assets: List[str] = []
    missing_assets: List[str] = []
    for asset in assets:
        decisions = list(round_results.get(asset, []))
        qualified_candidate = any(decision.action == "enter" for decision in decisions)
        if qualified_candidate:
            qualified_assets.append(asset)
        if not decisions:
            missing_assets.append(asset)
        final = decisions[-1] if decisions else None
        asset_results[asset] = {
            "decision_count": len(decisions),
            # A classifier candidate may still be blocked before reservation
            # or POST. Actual submission remains authoritative in `orders` and
            # the ORDER_SUBMITTED lifecycle event.
            "qualified_candidate": qualified_candidate,
            "final_action": final.action if final is not None else "missing",
            "final_reason": final.reason if final is not None else "missing_result",
        }
    payload.update(
        {
            "asset_results": asset_results,
            "qualified_assets": qualified_assets,
            "missing_assets": missing_assets,
        }
    )
    return payload


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
    clock: Callable[[], float] = time.time,
) -> int:
    """Return the next runnable 5m round without skipping active markets.

    The strategy needs the close-adjacent scene, not the full 5-minute history.
    After a round finishes at close+~1s, the next market is already active but
    still has almost five minutes before its own close.  Joining that active
    round prevents the old 10-minute cadence bug.
    """

    observed_now = float(now)
    while True:
        current = int(observed_now)
        active_start = current - current % 300
        active_end = active_start + 300
        classifier_cfg = active_classifier_config()
        minimum_lead_s = max(
            classifier_cfg.pre_close_window_s + classifier_cfg.pre_close_latest_max_age_s,
            LIVE_PREFLIGHT_LEAD_S,
        )
        if (
            active_start not in processed_round_starts
            and active_end - observed_now >= minimum_lead_s
        ):
            return active_start
        next_start = active_start + 300
        sleep(max(0.0, float(next_start) - observed_now))
        observed_now = float(clock())


def _reconcile_startup(
    settings: Settings, store: StateStore, executor: OrderExecutor, notifier: Notifier
) -> None:
    unresolved = store.unresolved_orders()
    for record in unresolved:
        component = "startup_reconciliation:%s" % record.intent_id
        if not record.order_id:
            # A crash can occur after the request leaves the process but before
            # its acknowledgement/order id is persisted.  No local evidence can
            # prove zero-fill, even on a later boot. Keep the risk unresolved,
            # never retry the slug, and make the operator attach an
            # authoritative id before reconciliation can continue.
            raw = dict(record.raw or {})
            reason = record.error or "startup_intent_has_no_order_id"
            raw["classification"] = raw.get("classification") or reason
            raw["ambiguous_submission"] = True
            raw["requires_operator_order_id_recovery"] = True
            raw["order_type"] = raw.get("order_type") or settings.order_type
            store.mark_execution_unknown(record.intent_id, reason, raw)
            _audit(
                settings,
                store,
                "startup_execution_unknown",
                {"reason": reason, "order_id": "n/a", "intent_id": record.intent_id},
                record.slug,
            )
            _transition_component_and_notify(
                store=store,
                component=component,
                status="unhealthy",
                detail=reason,
                notifier=notifier,
                settings=settings,
                kind="alert",
                payload={
                    "reason": "startup execution unknown; order_id recovery required",
                    "component": component,
                    "error_type": "MissingOrderId",
                    "error_message": reason,
                    "order_id": "n/a",
                    "intent_id": record.intent_id,
                },
                slug=record.slug,
                notify_on_no_transition=True,
            )
            if bool(getattr(executor, "process_restart_required", False)):
                # The bounded probe's daemon thread is still uncancellable;
                # do not start another probe while the outer maintenance
                # worker is preparing the durable ALERT and process restart.
                break
            continue
        try:
            reconcile_once = getattr(executor, "reconcile_existing_once", None)
            result = (
                reconcile_once(record)
                if callable(reconcile_once)
                else executor.reconcile_existing(record)
            )
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
            _transition_component_and_notify(
                store=store,
                component=component,
                status="unhealthy",
                detail=_safe_transport_message(exc),
                notifier=notifier,
                settings=settings,
                kind="alert",
                payload={
                    "reason": "startup reconciliation error",
                    "component": component,
                    "error_type": type(exc).__name__,
                    "error_message": _safe_transport_message(exc),
                    "order_id": record.order_id or "n/a",
                },
                slug=record.slug,
                notify_on_no_transition=True,
            )
            if bool(getattr(executor, "process_restart_required", False)):
                # The bounded probe's daemon thread is still uncancellable;
                # do not start another probe while the outer maintenance
                # worker is preparing the durable ALERT and process restart.
                break
            continue
        result_raw = result.raw or {}
        reconciliation_transport_error = bool(
            result_raw.get("reconcile_transport_error")
            or result.error == "reconcile_transport_timeout"
        )
        if reconciliation_transport_error:
            detail = str(
                result_raw.get("error_message")
                or result.error
                or "authoritative CLOB reconciliation timed out"
            )
            _transition_component_and_notify(
                store=store,
                component=component,
                status="unhealthy",
                detail=detail,
                notifier=notifier,
                settings=settings,
                kind="alert",
                payload={
                    "reason": "startup reconciliation transport error",
                    "component": component,
                    "error_type": "TimeoutError",
                    "error_message": detail,
                    "order_id": result.order_id or record.order_id,
                },
                slug=record.slug,
                notify_on_no_transition=True,
            )
        else:
            _transition_component_and_notify(
                store=store,
                component=component,
                status="healthy",
                detail="authoritative CLOB reconciliation completed",
                notifier=notifier,
                settings=settings,
                kind="recovery_success",
                payload={
                    "component": component,
                    "reason": "authoritative CLOB reconciliation restored",
                    "order_id": result.order_id or record.order_id,
                },
                slug=record.slug,
            )
        if executor.process_restart_required:
            # Do not start another bounded SDK probe while the previous one is
            # still alive. The outer runtime loop persists the process-level
            # alert and lets systemd recreate the client/pool.
            break
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
        component = "submitted_reconciliation:%s" % record.intent_id
        try:
            reconcile_once = getattr(executor, "reconcile_existing_once", None)
            result = (
                reconcile_once(record)
                if callable(reconcile_once)
                else executor.reconcile_existing(record)
            )
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
            _transition_component_and_notify(
                store=store,
                component=component,
                status="unhealthy",
                detail=_safe_transport_message(exc),
                notifier=notifier,
                settings=settings,
                kind="alert",
                payload={
                    "reason": "submitted order reconciliation error",
                    "component": component,
                    "error_type": type(exc).__name__,
                    "error_message": _safe_transport_message(exc),
                    "order_id": record.order_id,
                },
                slug=record.slug,
                notify_on_no_transition=True,
            )
            if bool(getattr(executor, "process_restart_required", False)):
                # The bounded probe's daemon thread is still uncancellable;
                # do not start another probe while the outer maintenance
                # worker is preparing the durable ALERT and process restart.
                break
            continue
        result_raw = result.raw or {}
        reconciliation_transport_error = bool(
            result_raw.get("reconcile_transport_error")
            or result.error == "reconcile_transport_timeout"
        )
        if reconciliation_transport_error:
            detail = str(
                result_raw.get("error_message")
                or result.error
                or "authoritative CLOB status probe timed out"
            )
            _transition_component_and_notify(
                store=store,
                component=component,
                status="unhealthy",
                detail=detail,
                notifier=notifier,
                settings=settings,
                kind="alert",
                payload={
                    "reason": "submitted reconciliation transport error",
                    "component": component,
                    "error_type": "TimeoutError",
                    "error_message": detail,
                    "order_id": result.order_id or record.order_id,
                },
                slug=record.slug,
                notify_on_no_transition=True,
            )
        else:
            _transition_component_and_notify(
                store=store,
                component=component,
                status="healthy",
                detail="authoritative CLOB status read succeeded",
                notifier=notifier,
                settings=settings,
                kind="recovery_success",
                payload={
                    "component": component,
                    "reason": "submitted order reconciliation restored",
                    "order_id": result.order_id or record.order_id,
                },
                slug=record.slug,
            )
        results.append(result)
        if result.terminal:
            _notify_order_result(notifier, settings, store, result, record.slug)
        if bool(getattr(executor, "process_restart_required", False)):
            # Stop before the next unresolved order so a single wedged SDK
            # call cannot accumulate one live daemon thread per record.
            break
    return results


def _live_runtime(
    settings: Settings,
    store: StateStore,
    public: PolymarketPublicClient,
    notifier: Notifier,
    *,
    restart_fn: Optional[Callable[[], None]] = None,
) -> Tuple[Optional[V2ClobGateway], OrderExecutor]:
    if not settings.is_live:
        return None, OrderExecutor(settings=settings, store=store)
    gateway = V2ClobGateway.from_settings(settings)

    def report_submission(kind: str, payload: Dict[str, Any]) -> None:
        if kind == "submitted":
            _safe_notify(notifier, settings, store, "submitted", payload, str(payload["slug"]))
        elif kind == "heartbeat_error":
            error_payload = {**payload, "component": "clob_heartbeat"}
            _transition_component_and_notify(
                store=store,
                component="clob_heartbeat",
                status="unhealthy",
                detail=str(
                    payload.get("error_message")
                    or payload.get("reason")
                    or "heartbeat failed"
                ),
                notifier=notifier,
                settings=settings,
                kind="alert",
                payload=error_payload,
                notify_on_no_transition=True,
            )
        elif kind == "heartbeat_recovered":
            _transition_component_and_notify(
                store=store,
                component="clob_heartbeat",
                status="healthy",
                detail="heartbeat acknowledged",
                notifier=notifier,
                settings=settings,
                kind="recovery_success",
                payload={
                    **payload,
                    "component": "clob_heartbeat",
                },
            )

    def heartbeat_fatal(reason: str, payload: Dict[str, Any]) -> None:
        _restart_after_heartbeat_fatal(
            settings=settings,
            store=store,
            notifier=notifier,
            reason=reason,
            payload=payload,
            restart_fn=restart_fn,
        )

    heartbeat = HeartbeatLoop(
        gateway,
        settings.heartbeat_interval_s,
        fatal_callback=heartbeat_fatal,
    )
    heartbeat.status_callback = report_submission
    try:
        prepare = getattr(gateway, "prepare_order_submission", None)
        if callable(prepare):
            prepare()
        heartbeat.start()
        geo = public.geoblock_status(settings.geo_endpoint)
        gateway.preflight(geo, 0.0)
        executor = OrderExecutor(
            settings=settings,
            store=store,
            gateway=gateway,
            event_callback=report_submission,
            account_heartbeat=heartbeat,
        )
        _reconcile_startup(settings, store, executor, notifier)
        return gateway, executor
    except Exception:
        heartbeat.stop()
        raise


class _MaintenanceWorker:
    """Run reconciliation/settlement off the five-minute scheduler thread.

    Only one sweep may run at a time. A new round is always more important than
    duplicating an already-running best-effort maintenance pass.
    """

    def __init__(
        self,
        *,
        settings: Settings,
        store: StateStore,
        public: PolymarketPublicClient,
        notifier: Notifier,
        restart_fn: Callable[[], None],
        timeout_s: float = MAINTENANCE_TIMEOUT_S,
    ) -> None:
        self._settings = settings
        self._store = store
        self._public = public
        self._notifier = notifier
        self._restart_fn = restart_fn
        self._timeout_s = max(0.01, float(timeout_s))
        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._timer: Optional[threading.Timer] = None
        self._done = threading.Event()

    def trigger(self, executor: OrderExecutor) -> bool:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return False
            thread = threading.Thread(
                target=self._run_guarded,
                args=(executor,),
                daemon=True,
                name="aftertake-maintenance",
            )
            self._thread = thread
            self._done = threading.Event()
            timer = threading.Timer(self._timeout_s, self._timeout)
            timer.daemon = True
            self._timer = timer
            thread.start()
            timer.start()
            return True

    def wait(self, timeout_s: float) -> None:
        with self._lock:
            thread = self._thread
        if thread is not None:
            thread.join(timeout=max(0.0, float(timeout_s)))

    def _timeout(self) -> None:
        if self._done.is_set():
            return

        def persist_timeout_diagnostics() -> None:
            print(
                json.dumps(
                    {
                        "kind": "maintenance_timeout",
                        "timeout_s": self._timeout_s,
                        "action": "process_restart_requested",
                    },
                    sort_keys=True,
                ),
                file=sys.stderr,
                flush=True,
            )
            _audit(
                self._settings,
                self._store,
                "maintenance_timeout",
                {"timeout_s": self._timeout_s},
            )
            _transition_component_and_notify(
                store=self._store,
                component="maintenance_worker",
                status="unhealthy",
                detail="maintenance exceeded %.1fs" % self._timeout_s,
                notifier=self._notifier,
                settings=self._settings,
                kind="alert",
                payload={
                    "reason": "maintenance worker timeout",
                    "component": "maintenance_worker",
                    "error_type": "TimeoutError",
                    "error_message": "uncancellable maintenance requires process restart",
                },
                notify_on_no_transition=True,
            )

        # Persist the alert before the normal restart. If SQLite/Telegram is
        # itself wedged, the bounded fallback still gives systemd a recovery
        # boundary instead of leaving a detached worker alive forever.
        _run_diagnostics_then_restart(self._restart_fn, persist_timeout_diagnostics)

    def _run_guarded(self, executor: OrderExecutor) -> None:
        try:
            self._run(executor)
            _transition_component_and_notify(
                store=self._store,
                component="maintenance_worker",
                status="healthy",
                detail="maintenance sweep completed",
                notifier=self._notifier,
                settings=self._settings,
                kind="recovery_success",
                payload={
                    "component": "maintenance_worker",
                    "reason": "maintenance sweep recovered",
                },
            )
        except Exception as exc:
            _audit(
                self._settings,
                self._store,
                "maintenance_unhandled_error",
                {"error": _safe_transport_message(exc)},
            )
            _transition_component_and_notify(
                store=self._store,
                component="maintenance_worker",
                status="unhealthy",
                detail=_safe_transport_message(exc),
                notifier=self._notifier,
                settings=self._settings,
                kind="alert",
                payload={
                    "reason": "maintenance worker error",
                    "component": "maintenance_worker",
                    "error_type": type(exc).__name__,
                    "error_message": _safe_transport_message(exc),
                },
                notify_on_no_transition=True,
            )
        finally:
            self._done.set()
            with self._lock:
                timer = self._timer
                self._timer = None
            if timer is not None:
                timer.cancel()

    def _run(self, executor: OrderExecutor) -> None:
        # py-clob-client-v2 owns one module-global httpx pool. Constructing a
        # second wrapper is not transport isolation and only obscures failures.
        # Single-probe reconciliation is low volume and the 240s worker timeout
        # forces a clean process restart well before the next close.
        maintenance_executor = executor
        if self._settings.is_live and getattr(maintenance_executor, "gateway", None) is not None:
            try:
                reconcile_submitted_orders(
                    settings=self._settings,
                    store=self._store,
                    executor=maintenance_executor,
                    notifier=self._notifier,
                )
                if _restart_after_executor_timeout(
                    settings=self._settings,
                    store=self._store,
                    notifier=self._notifier,
                    executor=maintenance_executor,
                    restart_fn=self._restart_fn,
                    component="submitted_reconciliation",
                ):
                    return
            except Exception as exc:
                if _restart_after_executor_timeout(
                    settings=self._settings,
                    store=self._store,
                    notifier=self._notifier,
                    executor=maintenance_executor,
                    restart_fn=self._restart_fn,
                    component="submitted_reconciliation",
                ):
                    return
                _audit(self._settings, self._store, "submitted_reconcile_error", {"error": str(exc)})
                _safe_notify(
                    self._notifier,
                    self._settings,
                    self._store,
                    "alert",
                    {
                        "reason": "submitted reconciliation sweep error",
                        "component": "submitted_reconciliation_sweep",
                        "error_type": type(exc).__name__,
                        "error_message": _safe_transport_message(exc),
                    },
                )
        try:
            settle_open_positions(
                settings=self._settings,
                store=self._store,
                public=self._public,
                notifier=self._notifier,
            )
        except Exception as exc:
            _audit(self._settings, self._store, "settlement_sweep_error", {"error": str(exc)})
            _safe_notify(
                self._notifier,
                self._settings,
                self._store,
                "alert",
                {
                    "reason": "settlement sweep error",
                    "component": "settlement_sweep",
                    "error_type": type(exc).__name__,
                    "error_message": _safe_transport_message(exc),
                },
            )


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
    runtime_ready_sent = False
    maintenance = _MaintenanceWorker(
        settings=settings,
        store=store,
        public=public,
        notifier=notifier,
        restart_fn=(runtime_watchdog.request_restart if runtime_watchdog is not None else lambda: os._exit(1)),
    )

    while forever or completed < max(1, rounds):
        if runtime_watchdog is not None:
            runtime_watchdog.beat("runtime_connect" if settings.is_live and gateway is None else "waiting_for_round")
        if settings.is_live and gateway is None:
            try:
                gateway, executor = live_runtime_factory(settings, store, public, notifier)
                if gateway is None:
                    raise RuntimeError("live runtime did not provide a CLOB gateway")
                if _restart_after_executor_timeout(
                    settings=settings,
                    store=store,
                    notifier=notifier,
                    executor=executor,
                    restart_fn=(
                        runtime_watchdog.request_restart
                        if runtime_watchdog is not None
                        else None
                    ),
                    component="clob_executor_startup",
                ):
                    return
            except Exception as exc:
                reason = "PM runtime unavailable; retrying: %s: %s" % (type(exc).__name__, str(exc))
                _audit(settings, store, "runtime_connect_retry", {"reason": reason})
                # Every occurrence is intentional operator evidence. Telegram
                # noise is preferable to another silent runtime freeze.
                _transition_component_and_notify(
                    store=store,
                    component="pm_runtime",
                    status="unhealthy",
                    detail=reason,
                    notifier=notifier,
                    settings=settings,
                    kind="alert",
                    payload={"reason": reason, "component": "pm_runtime"},
                    notify_on_no_transition=True,
                )
                last_runtime_error = reason
                if runtime_watchdog is not None:
                    runtime_watchdog.beat("runtime_retry_wait")
                sleep(RUNTIME_RETRY_S)
                continue
            runtime_recovered = _transition_component_and_notify(
                store=store,
                component="pm_runtime",
                status="healthy",
                detail="live preflight passed",
                notifier=notifier,
                settings=settings,
                kind="recovery_success",
                payload={"component": "pm_runtime", "reason": "PM runtime recovered"},
                notify_on_no_transition=bool(last_runtime_error),
            )
            if last_runtime_error or runtime_recovered:
                _audit(
                    settings,
                    store,
                    "runtime_recovered",
                    {"previous_error": last_runtime_error or "persisted failure from prior process"},
                )
                last_runtime_error = ""
            if not runtime_ready_sent:
                ready_payload = {
                    "dry_run": settings.dry_run,
                    "assets": list(settings.assets),
                    "pid": os.getpid(),
                    "code_sha": _resolve_code_sha(),
                }
                _audit(settings, store, "runtime_ready", ready_payload)
                _safe_notify(notifier, settings, store, "ready", ready_payload)
                runtime_ready_sent = True
            if runtime_watchdog is not None:
                runtime_watchdog.beat("waiting_for_round")

        # Dry-run has no live gateway/preflight branch above, but it must still
        # publish the same readiness marker used by the deployment gate.  A
        # process that reaches the scheduler is operational in either mode.
        if not settings.is_live and not runtime_ready_sent:
            ready_payload = {
                "dry_run": settings.dry_run,
                "assets": list(settings.assets),
                "pid": os.getpid(),
                "code_sha": _resolve_code_sha(),
            }
            _audit(settings, store, "runtime_ready", ready_payload)
            _safe_notify(notifier, settings, store, "ready", ready_payload)
            runtime_ready_sent = True

        if wait_for_next_boundary is not _wait_for_next_boundary:
            start = wait_for_next_boundary()
        else:
            start = _select_next_round_start(
                now=time.time(),
                processed_round_starts=processed_round_starts,
                sleep=sleep,
                clock=time.time,
            )
        processed_round_starts.add(start)
        _audit(
            settings,
            store,
            "round_started",
            _round_lifecycle_payload(
                round_start=start,
                assets=tuple(settings.assets),
            ),
        )
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
                restart_fn=(runtime_watchdog.request_restart if runtime_watchdog is not None else None),
            )
            _audit(
                settings,
                store,
                "round_complete",
                _round_lifecycle_payload(
                    round_start=start,
                    assets=tuple(settings.assets),
                    round_results=round_results,
                ),
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
            transport_failed_assets = [
                asset
                for asset, decisions in round_results.items()
                if any("transport_error" in str(item.reason) for item in decisions)
            ]
            if settings.is_live and transport_failed_assets and not timed_out_assets:
                # py-clob-client-v2 uses one module-global httpx pool, so
                # rebuilding only our gateway wrapper cannot replace a poisoned
                # transport. Persist ambiguous risk, alert, then let systemd
                # recreate the process and SDK pool; never retry this market.
                _audit(
                    settings,
                    store,
                    "live_transport_restart",
                    {"assets": transport_failed_assets},
                )
                if runtime_watchdog is not None:
                    runtime_watchdog.request_restart()
                    return
            if runtime_watchdog is not None:
                runtime_watchdog.beat("round_complete")
        except Exception as exc:
            _audit(settings, store, "round_runtime_error", {"error": str(exc)})
            _safe_notify(notifier, settings, store, "alert", {"reason": str(exc)})
            if runtime_watchdog is not None:
                # The asset supervisor may have detached a worker that raised
                # a non-transport exception. Do not keep running alongside an
                # uncancellable worker with shared state/client objects.
                runtime_watchdog.request_restart()
        finally:
            # Network maintenance runs on one coalescing daemon worker. It may
            # overlap the quiet part of the next market, but can never hold the
            # scheduler thread hostage at the next boundary.
            maintenance.trigger(executor)
            if runtime_watchdog is not None:
                runtime_watchdog.beat("finalize_complete")
        completed += 1

    # Finite/manual runs retain the old observable contract for quick sweeps,
    # without imposing an unbounded shutdown wait on a stuck provider call.
    maintenance.wait(1.0)
    executor.close()


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
        component = "settlement_position:%s" % record.intent_id
        try:
            pm_up = parse_pm_up(public.market_by_slug(record.slug, allow_closed=True))
            _transition_component_and_notify(
                store=store,
                component=component,
                status="healthy",
                detail="Gamma settlement lookup succeeded",
                notifier=notifier,
                settings=settings,
                kind="recovery_success",
                payload={
                    "component": component,
                    "reason": "settlement lookup restored",
                },
                slug=record.slug,
            )
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
                _transition_component_and_notify(
                    store=store,
                    component=component,
                    status="unhealthy",
                    detail=_safe_transport_message(exc),
                    notifier=notifier,
                    settings=settings,
                    kind="alert",
                    payload={
                        "reason": "settlement position error",
                        "component": component,
                        "error_type": type(exc).__name__,
                        "error_message": _safe_transport_message(exc),
                    },
                    slug=record.slug,
                    notify_on_no_transition=True,
                )
            else:
                _mark_component_unhealthy(store, component, _safe_transport_message(exc))
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
    classifier_cfg = _classifier_config_for_settings(settings)
    if settings.strategy_family == "v9":
        confirmation_policy = "v9_lane_specific_fresh_paired"
        confirmations: Any = {"R": 1, "S": classifier_cfg.sweep_confirmations}
        stable_leader: Any = "S_only"
    else:
        confirmation_policy = (
            "distinct_evidence_states"
            if classifier_cfg.distinct_evidence_confirmations
            else "fresh_paired_observations"
        )
        confirmations = classifier_cfg.confirmations
        stable_leader = classifier_cfg.require_stable_post_close_leader
    return {
        "strategy": classifier_cfg.strategy_version,
        "strategy_family": settings.strategy_family,
        "v9_live_enabled": settings.v9_live_enabled,
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
        "confirmations": confirmations,
        "confirmation_spacing_ms": int(
            classifier_cfg.confirmation_spacing_s * 1000
        ),
        "confirmation_policy": confirmation_policy,
        "lane_confirmations": {"R": 1, "S": classifier_cfg.sweep_confirmations}
        if settings.strategy_family == "v9"
        else None,
        "require_loser_refill_failure": classifier_cfg.require_loser_refill_failure,
        "require_stable_post_close_leader": stable_leader,
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
    # Start the process watchdog before any output-directory, SQLite, BOOT
    # audit, or Telegram work. A Type=simple systemd unit otherwise considers
    # a process stuck in startup I/O healthy forever.
    startup_watchdog = RuntimeWatchdog()
    startup_watchdog.start()
    startup_watchdog.beat("boot")
    settings.out_dir.mkdir(parents=True, exist_ok=True)
    store = StateStore(settings.state_db)
    public = PolymarketPublicClient(
        gamma_host=settings.gamma_host,
        clob_host=settings.clob_host,
        http=PublicHttpClient(resolve_overrides=parse_resolve_overrides(settings.resolve_overrides)),
    )
    notifier = Notifier(token=settings.telegram_token, chat_id=settings.telegram_chat_id)

    def persist_runtime_watchdog_alert(reason: str, payload: Dict[str, Any]) -> None:
        alert_payload = {
            **payload,
            "reason": reason,
            "component": "runtime_watchdog",
            "error_type": "RuntimeStall",
            "error_message": "runtime watchdog detected no progress",
        }
        _audit(settings, store, "runtime_watchdog_stall", alert_payload)
        _transition_component_and_notify(
            store=store,
            component="runtime_watchdog",
            status="unhealthy",
            detail=reason,
            notifier=notifier,
            settings=settings,
            kind="alert",
            payload=alert_payload,
            notify_on_no_transition=True,
        )

    startup_watchdog.set_fatal_callback(persist_runtime_watchdog_alert)
    executor: Optional[OrderExecutor] = None
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
            runtime_watchdog = startup_watchdog
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
        if executor is not None:
            executor.close()
        store.close()
        startup_watchdog.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
