"""Explicit V2 CLOB order lifecycle with durable recovery semantics."""

from __future__ import annotations

import json
import queue
import re
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Protocol

from .config import Settings
from .pm_client import MarketMetadata, is_matching_engine_restart_error
from .state import OrderRecord, StateStore


class OrderGateway(Protocol):
    def submit_limit_buy(
        self, token_id: str, price: float, qty: float, metadata: MarketMetadata, order_type: str = "GTC"
    ) -> Dict[str, Any]:
        ...

    def get_order(self, order_id: str) -> Dict[str, Any]:
        ...

    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        ...

    def order_trades(self, token_id: str, order_id: str) -> List[Dict[str, Any]]:
        ...

    def post_heartbeat(self, heartbeat_id: str = "") -> Dict[str, Any]:
        ...


@dataclass(frozen=True)
class OrderResult:
    dry_run: bool
    intent_id: str
    order_id: str
    status: str
    side: str
    token_id: str
    price: float
    requested_qty: float
    filled_qty: float
    avg_price: float
    notional: float
    terminal: bool
    submission_state: str
    error: str = ""
    raw: Optional[Dict[str, Any]] = None
    event_ts: float = 0.0
    decision_to_submit_ms: float = -1.0
    submit_roundtrip_ms: float = -1.0
    reconcile_duration_ms: float = -1.0
    observed_book_age_ms: float = -1.0
    immediate_taker_order_delay_enabled: bool = False
    expected_taker_delay_ms: float = 0.0

    @property
    def qty(self) -> float:
        """Compatibility alias; callers must prefer ``filled_qty`` for PnL."""

        return self.filled_qty


def _heartbeat_id_from_value(value: Any) -> str:
    """Extract a server-issued heartbeat id from success or SDK error data.

    ``py-clob-client-v2`` exposes failed HTTP bodies through different shapes
    across releases (a dict, a JSON string, or an exception's ``error_msg``).
    The CLOB heartbeat contract includes a replacement id in an invalid-id
    response, so parsing all of these shapes is necessary for recovery.
    """

    if isinstance(value, BaseException):
        for attribute in ("error_msg", "error_message", "response", "body"):
            nested = getattr(value, attribute, None)
            heartbeat_id = _heartbeat_id_from_value(nested)
            if heartbeat_id:
                return heartbeat_id
        value = str(value)
    if isinstance(value, dict):
        for key in ("heartbeat_id", "heartbeatId", "heartbeatID"):
            candidate = value.get(key)
            if candidate is not None and str(candidate).strip():
                return str(candidate).strip()
        for key in ("error", "error_msg", "message", "detail", "body", "response"):
            heartbeat_id = _heartbeat_id_from_value(value.get(key))
            if heartbeat_id:
                return heartbeat_id
        return ""
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8", errors="replace")
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        parsed = json.loads(text)
    except (TypeError, ValueError):
        parsed = None
    if parsed is not None and parsed is not value:
        heartbeat_id = _heartbeat_id_from_value(parsed)
        if heartbeat_id:
            return heartbeat_id
    match = re.search(
        r"[\"']heartbeat[_-]?id[\"']\s*:\s*[\"']([^\"']+)[\"']",
        text,
        flags=re.IGNORECASE,
    )
    return match.group(1).strip() if match else ""


class HeartbeatLoop:
    """CLOB order heartbeat, separate from WebSocket ping/keepalive."""

    def __init__(
        self,
        gateway: OrderGateway,
        interval_s: float,
        *,
        fatal_callback: Optional[Callable[[str, Dict[str, Any]], None]] = None,
    ):
        self.gateway = gateway
        self.interval_s = float(interval_s)
        # Live uses ``use_server_time=False``, so this path contains one HTTP
        # request with the pinned SDK's five-second timeout. Bound a provider
        # call just beyond that timeout; a two-second wrapper would turn a slow
        # but valid response into a restart storm. Small test intervals remain
        # responsive.
        self.call_timeout_s = max(0.2, min(5.5, self.interval_s * 2.5))
        # The pinned SDK may use a five-second HTTP timeout.  Keep enough
        # room for the default 4-second cadence, that timeout, and a late
        # completion before declaring the account heartbeat unrecoverable.
        self.hard_failure_s = 20.0
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._request_thread: Optional[threading.Thread] = None
        self._request_done: Optional[threading.Event] = None
        self._request_result: Dict[str, Any] = {}
        self._request_failure: List[BaseException] = []
        self._request_started_mono = 0.0
        self._request_lock = threading.Lock()
        self.last_error = ""
        self._heartbeat_id = ""
        self.last_success_at = 0.0
        self.consecutive_failures = 0
        self.status_callback: Optional[Callable[[str, Dict[str, Any]], None]] = None
        self.fatal_callback = fatal_callback
        self._fatal_requested = threading.Event()
        self._status_queue: queue.Queue[tuple] = queue.Queue(maxsize=64)
        self._status_stop = threading.Event()
        self._status_thread: Optional[threading.Thread] = None

    @property
    def heartbeat_id(self) -> str:
        """Return the most recent server-issued heartbeat id."""

        return self._heartbeat_id

    def start(self) -> None:
        if self._thread is not None:
            return
        self._status_thread = threading.Thread(
            target=self._status_worker,
            daemon=True,
            name="aftertake-heartbeat-status",
        )
        self._status_thread.start()
        self._bootstrap()
        self._thread = threading.Thread(target=self._run, daemon=True, name="aftertake-heartbeat")
        self._thread.start()

    def _bootstrap(self) -> None:
        """Establish a fresh server heartbeat before the runtime is READY.

        The CLOB protocol uses an empty id to start a session.  An expired
        session may answer that first request with a replacement id; that is
        expected startup negotiation, not an operator-facing outage.  A
        timeout still owns its request until it completes, so bootstrap never
        overlaps SDK calls while it waits for a valid id.
        """

        deadline = time.monotonic() + self.hard_failure_s
        while not self._stop.is_set():
            try:
                response = self._post_heartbeat_bounded()
                heartbeat_id = _heartbeat_id_from_value(response)
                if not heartbeat_id:
                    raise RuntimeError("CLOB heartbeat bootstrap returned no heartbeat id")
                self._heartbeat_id = heartbeat_id
                self.last_error = ""
                self.last_success_at = time.time()
                self.consecutive_failures = 0
                status_ack = self._notify_status(
                    "heartbeat_recovered",
                    {"reason": "CLOB heartbeat restored", "heartbeat_id": self._heartbeat_id},
                )
                if status_ack is not None:
                    status_ack.wait(0.1)
                return
            except Exception as exc:
                self.consecutive_failures += 1
                replacement_id = _heartbeat_id_from_value(exc)
                if replacement_id:
                    self._heartbeat_id = replacement_id
                error_value = getattr(exc, "error_msg", None)
                self.last_error = _safe_error_message(error_value if error_value is not None else exc)
                now_mono = time.monotonic()
                if now_mono >= deadline:
                    self._request_fatal(
                        "CLOB heartbeat bootstrap exceeded hard failure deadline",
                        {
                            "error_message": self.last_error,
                            "consecutive_failures": self.consecutive_failures,
                            "failure_age_s": max(0.0, self.hard_failure_s),
                        },
                    )
                    raise TimeoutError(
                        "CLOB heartbeat bootstrap exceeded hard failure deadline"
                    ) from exc
                # A replacement id means the request completed with the
                # expected 400 response; retry immediately with that id.
                if replacement_id:
                    continue
                self._stop.wait(min(0.25, max(0.01, deadline - now_mono)))
        raise RuntimeError("heartbeat stopped during bootstrap")

    def _post_heartbeat_bounded(self) -> Dict[str, Any]:
        """Bound one SDK call and retain a completion that arrives late."""

        with self._request_lock:
            if self._request_thread is not None:
                if self._request_done is None or not self._request_done.is_set():
                    raise TimeoutError("previous heartbeat request is still in flight")
                return self._consume_request_locked()
            done = threading.Event()
            self._request_done = done
            self._request_result = {}
            self._request_failure = []
            self._request_started_mono = time.monotonic()

            def call() -> None:
                try:
                    response = self.gateway.post_heartbeat(self._heartbeat_id)
                    if isinstance(response, dict):
                        self._request_result.update(response)
                    else:
                        self._request_result["raw"] = response
                except Exception as exc:  # propagate SDK failures to the loop
                    self._request_failure.append(exc)
                finally:
                    done.set()

            request_thread = threading.Thread(
                target=call,
                daemon=True,
                name="aftertake-heartbeat-request",
            )
            self._request_thread = request_thread
            request_thread.start()

        if not done.wait(self.call_timeout_s):
            raise TimeoutError("heartbeat request timed out")
        with self._request_lock:
            return self._consume_request_locked()

    def _consume_request_locked(self) -> Dict[str, Any]:
        """Consume exactly one completed request while ``_request_lock`` is held."""

        # ``done`` is published only after the request result/failure has been
        # stored.  The worker Thread can remain ``is_alive()`` for a tiny
        # scheduler window after setting the event; requiring full thread exit
        # here falsely classified a completed heartbeat as a hung SDK call and
        # restarted the entire service.
        if (
            self._request_thread is None
            or self._request_done is None
            or not self._request_done.is_set()
        ):
            raise TimeoutError("previous heartbeat request is still in flight")
        failure = list(self._request_failure)
        result = dict(self._request_result)
        self._request_thread = None
        self._request_done = None
        self._request_result = {}
        self._request_failure = []
        self._request_started_mono = 0.0
        if failure:
            raise failure[0]
        return result

    def _in_flight_age(self, now_mono: float) -> float:
        with self._request_lock:
            if (
                self._request_thread is None
                or self._request_done is None
                or self._request_done.is_set()
                or self._request_started_mono <= 0
            ):
                return 0.0
            return max(0.0, float(now_mono) - self._request_started_mono)

    def _notify_status(self, kind: str, payload: Dict[str, Any]) -> Optional[threading.Event]:
        if self.status_callback is None:
            return None
        acknowledged = threading.Event()
        try:
            self._status_queue.put_nowait((str(kind), dict(payload), acknowledged))
            return acknowledged
        except queue.Full:
            # A blocked diagnostics consumer must never block the dead-man
            # heartbeat itself. Losing heartbeat status is a fatal
            # observability fault, so recreate the process instead.
            self._request_fatal(
                "heartbeat status queue is full",
                {"consecutive_failures": self.consecutive_failures},
            )
            return None

    def _status_worker(self) -> None:
        while not self._status_stop.is_set():
            try:
                kind, payload, acknowledged = self._status_queue.get(timeout=0.1)
            except queue.Empty:
                continue
            try:
                callback = self.status_callback
                if callback is not None:
                    callback(kind, payload)
            except Exception:
                # Diagnostics must never terminate heartbeat processing.
                pass
            finally:
                acknowledged.set()
                self._status_queue.task_done()

    def _request_fatal(
        self,
        reason: str,
        payload: Dict[str, Any],
        acknowledged: Optional[threading.Event] = None,
    ) -> None:
        if self._fatal_requested.is_set():
            return
        self._fatal_requested.set()
        callback = self.fatal_callback
        if callback is None:
            return

        def invoke() -> None:
            try:
                if acknowledged is not None:
                    # Give the independent status worker a short chance to
                    # commit the durable ALERT. A locked/broken database must
                    # never postpone process recovery beyond this bound.
                    acknowledged.wait(0.1)
                callback(str(reason), dict(payload))
            except Exception:
                return

        threading.Thread(
            target=invoke,
            daemon=True,
            name="aftertake-heartbeat-restart",
        ).start()

    @property
    def fatal_requested(self) -> bool:
        return self._fatal_requested.is_set()

    def _run(self) -> None:
        had_error = False
        started_mono = time.monotonic()
        last_success_mono = started_mono
        next_due = started_mono + self.interval_s
        # Bootstrap already sent the first heartbeat before this thread was
        # created. The background account-wide loop starts at the next normal
        # interval, after a valid id is warm for any order acknowledgement.
        while not self._stop.is_set():
            wait_s = max(0.0, next_due - time.monotonic())
            if self._stop.wait(wait_s):
                return
            try:
                response = self._post_heartbeat_bounded()
                heartbeat_id = _heartbeat_id_from_value(response)
                if heartbeat_id:
                    self._heartbeat_id = heartbeat_id
                self.last_error = ""
                self.last_success_at = time.time()
                last_success_mono = time.monotonic()
                self.consecutive_failures = 0
                if had_error:
                    self._notify_status(
                        "heartbeat_recovered",
                        {"reason": "CLOB heartbeat restored", "heartbeat_id": self._heartbeat_id},
                    )
                had_error = False
                next_due += self.interval_s
            except Exception as exc:  # execution reconciliation will fail closed
                # Polymarket returns the replacement heartbeat_id alongside a
                # 400 when the previous id expired.  Keeping the old id here
                # creates the exact infinite-invalid-heartbeat loop seen in
                # production and can leave the order lifecycle without a live
                # heartbeat.  Recover the replacement id and retry promptly.
                self.consecutive_failures += 1
                replacement_id = _heartbeat_id_from_value(exc)
                if replacement_id:
                    self._heartbeat_id = replacement_id
                error_value = getattr(exc, "error_msg", None)
                self.last_error = _safe_error_message(error_value if error_value is not None else exc)
                status_ack = None
                if not had_error:
                    status_ack = self._notify_status(
                        "heartbeat_error",
                        {
                            "reason": "CLOB heartbeat failed",
                            "error_message": self.last_error,
                            "heartbeat_id": self._heartbeat_id,
                            "consecutive_failures": self.consecutive_failures,
                        },
                    )
                had_error = True
                now_mono = time.monotonic()
                failure_age = now_mono - last_success_mono
                request_in_flight = self._in_flight_age(now_mono) > 0
                hard_failure = failure_age >= self.hard_failure_s and (
                    request_in_flight or self.consecutive_failures >= 2
                )
                if hard_failure:
                    self._request_fatal(
                        "CLOB heartbeat is not recoverable in-process",
                        {
                            "error_message": self.last_error,
                            "consecutive_failures": self.consecutive_failures,
                            "failure_age_s": failure_age,
                        },
                        acknowledged=status_ack,
                    )
                if replacement_id:
                    next_due = now_mono + min(0.25, self.interval_s)
                elif request_in_flight:
                    # Keep polling the completion state, never start a second
                    # SDK request while the original call can still return.
                    next_due = now_mono + min(0.25, self.interval_s)
                else:
                    next_due += self.interval_s
            # If a call consumed the full cadence, retry immediately rather
            # than adding request latency to every heartbeat interval.
            next_due = max(next_due, time.monotonic())

    def stop(self) -> None:
        self._stop.set()
        # `_post_heartbeat_bounded` publishes and starts the request while
        # holding this same lock.  Taking the snapshot under the lock prevents
        # joining a Thread object in the tiny pre-start interval.
        with self._request_lock:
            request_thread = self._request_thread
        if request_thread is not None:
            request_thread.join(timeout=min(1.0, self.call_timeout_s + 0.25))
        if self._thread is not None:
            self._thread.join(timeout=min(2.0, max(1.0, self.interval_s + 0.25)))
        if request_thread is not None and request_thread.is_alive():
            self.last_error = "heartbeat request remained alive during shutdown"
            self._notify_status(
                "heartbeat_error",
                {
                    "reason": "CLOB heartbeat shutdown failed",
                    "error_message": self.last_error,
                    "heartbeat_id": self._heartbeat_id,
                    "consecutive_failures": self.consecutive_failures,
                },
            )
        self._status_stop.set()
        if self._status_thread is not None:
            self._status_thread.join(timeout=0.25)


def _number(payload: Dict[str, Any], keys: tuple, default: float = 0.0) -> float:
    values: List[float] = []
    for key in keys:
        value = payload.get(key)
        try:
            values.append(float(value))
        except (TypeError, ValueError):
            continue
    return max(values) if values else float(default)


def _normalise_status(status: str) -> str:
    """Map the CLOB API's documented ``ORDER_STATUS_*`` values to wire names."""

    normalised = str(status).strip().lower()
    prefix = "order_status_"
    if normalised.startswith(prefix):
        normalised = normalised[len(prefix) :]
    return normalised


def _terminal_status(status: str) -> bool:
    return _normalise_status(status) in {
        "matched",
        "filled",
        "canceled",
        "cancelled",
        "rejected",
        "failed",
        "expired",
    }


def _market_delay_from_raw(raw: Dict[str, Any]) -> Dict[str, Any]:
    payload = raw.get("_market_delay")
    if not isinstance(payload, dict):
        submit = raw.get("submit")
        payload = submit.get("_market_delay") if isinstance(submit, dict) else {}
    enabled = bool((payload or {}).get("immediate_taker_order_delay_enabled", False))
    try:
        expected = float((payload or {}).get("expected_taker_delay_ms", 0.0))
    except (TypeError, ValueError):
        expected = 0.0
    return {
        "immediate_taker_order_delay_enabled": enabled,
        "expected_taker_delay_ms": expected if expected > 0 else (250.0 if enabled else 0.0),
    }


def _truncate(text: Any, limit: int = 500) -> str:
    text = " ".join(str(text or "").split())
    return text[:limit] + "..." if len(text) > limit else text


def _safe_error_message(value: Any) -> str:
    if isinstance(value, dict):
        allowed = {k: value.get(k) for k in ("error", "message", "detail", "code") if k in value}
        return _truncate(allowed or value, 500)
    return _truncate(value, 500)


def _is_fak_no_match_exception(exc: BaseException, order_type: str) -> bool:
    if str(order_type or "").upper().strip() != "FAK":
        return False
    status_code = getattr(exc, "status_code", None)
    if status_code not in {400, 422}:
        return False
    error_msg = getattr(exc, "error_msg", None)
    message = _safe_error_message(error_msg if error_msg is not None else str(exc)).lower()
    return (
        "no orders found" in message
        and "match" in message
        and "fak" in message
    )


def _is_definite_submit_rejection(exc: BaseException) -> bool:
    """Return true only for documented failures that cannot have inserted an order.

    HTTP status alone is not enough.  In particular, Polymarket reports a
    duplicated signed order as HTTP 400 even though the identical order may
    already be live.  Treating every 4xx as zero risk can therefore hide a real
    position.
    """

    status_code = getattr(exc, "status_code", None)
    try:
        status_code = int(status_code)
    except (TypeError, ValueError):
        return False
    if status_code in {401, 403}:
        return True
    if status_code not in {400, 404, 422}:
        return False
    error_value = getattr(exc, "error_msg", None)
    message = _safe_error_message(error_value if error_value is not None else str(exc)).lower()
    if any(marker in message for marker in ("duplicat", "already been placed")):
        return False
    definite_markers = (
        "invalid order payload",
        "invalid order type",
        "owner of the api key",
        "signer address has to be",
        "address banned",
        "closed only mode",
        "breaks minimum tick size",
        "lower than the minimum",
        "not enough balance",
        "not enough allowance",
        "invalid expiration",
        "market is not yet ready",
    )
    return any(marker in message for marker in definite_markers)


def _sanitize_exception(exc: BaseException, *, phase: str, order_type: str) -> Dict[str, Any]:
    status_code = getattr(exc, "status_code", None)
    error_msg = getattr(exc, "error_msg", None)
    message = _safe_error_message(error_msg if error_msg is not None else str(exc))
    error_type = type(exc).__name__
    lowered = message.lower()
    hint = "unknown"
    if "fak" in lowered or "order type" in lowered or "ordertype" in lowered:
        hint = "order_type_compatibility"
    elif status_code in {400, 422}:
        hint = "clob_rejected_request"
    elif status_code in {401, 403}:
        hint = "clob_auth_or_permission"
    elif status_code in {408, 429, 500, 502, 503, 504} or "request exception" in lowered or "timeout" in lowered:
        hint = "transport_or_clob_transient"
    return {
        "phase": phase,
        "order_type": str(order_type or "").upper(),
        "error_type": error_type,
        "status_code": status_code,
        "error_message": message,
        "error_hint": hint,
    }


class OrderExecutor:
    """Submit one already-reserved entry and reconcile it before returning.

    It never automatically retries a failed/ambiguous submission for the same
    market.  A definite venue rejection is terminal for that slug; a timeout,
    disconnect, 5xx, or missing acknowledgement remains durable
    ``execution_unknown`` because the server may already have accepted it.
    """

    def __init__(
        self,
        settings: Settings,
        store: StateStore,
        gateway: Optional[OrderGateway] = None,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        wall_clock: Callable[[], float] = time.time,
        heartbeat_factory: Callable[[OrderGateway, float], HeartbeatLoop] = HeartbeatLoop,
        account_heartbeat: Optional[HeartbeatLoop] = None,
        event_callback: Optional[Callable[[str, Dict[str, Any]], None]] = None,
    ):
        self.settings = settings
        self.store = store
        self.gateway = gateway
        self._sleep = sleep
        self._monotonic = monotonic
        self._wall_clock = wall_clock
        self._heartbeat_factory = heartbeat_factory
        # Heartbeats are an account-wide dead-man switch: one chain protects
        # every open order.  The live runner owns one long-lived instance.
        # Per-order loops race their heartbeat ids and cancel pending GTCs when
        # the individual reconciliation call returns.
        self.account_heartbeat = account_heartbeat
        self._event_callback = event_callback
        self._event_threads: List[tuple] = []
        self._event_threads_lock = threading.Lock()
        self._process_restart_required = threading.Event()
        self._process_restart_reason = ""

    def _new_heartbeat(self) -> HeartbeatLoop:
        heartbeat = self._heartbeat_factory(self.gateway, self.settings.heartbeat_interval_s)
        if hasattr(heartbeat, "status_callback"):
            heartbeat.status_callback = lambda kind, payload: self._emit(kind, payload)
        return heartbeat

    def close(self) -> None:
        """Stop the account-wide heartbeat owned by this executor, if any."""

        heartbeat = self.account_heartbeat
        self.account_heartbeat = None
        if heartbeat is not None:
            heartbeat.stop()

    @property
    def process_restart_required(self) -> bool:
        """Return whether an SDK call outlived its hard reconciliation deadline."""

        return self._process_restart_required.is_set()

    @property
    def process_restart_reason(self) -> str:
        return self._process_restart_reason

    def _mark_process_restart_required(self, reason: str) -> None:
        if not self._process_restart_required.is_set():
            self._process_restart_reason = str(reason)
            self._process_restart_required.set()

    def _emit(self, kind: str, payload: Dict[str, Any]) -> None:
        if self._event_callback is None:
            return

        def dispatch() -> None:
            try:
                self._event_callback(kind, payload)
            except Exception:
                # Operator reporting can never alter or retry an order.
                return

        # Telegram has a network timeout longer than the order TTL. Never hold
        # up authenticated reconciliation/cancellation on notification I/O.
        thread = threading.Thread(
            target=dispatch,
            daemon=True,
            name="aftertake-order-event",
        )
        with self._event_threads_lock:
            self._event_threads.append((thread, kind))
        thread.start()

    def wait_for_event_delivery(self, timeout_s: float = 16.0) -> None:
        """Drain order-event reporting only after order risk is terminal."""

        with self._event_threads_lock:
            pending = list(self._event_threads)
            self._event_threads.clear()
        deadline = time.monotonic() + max(0.0, float(timeout_s))
        for thread, kind in pending:
            thread.join(timeout=max(0.0, deadline - time.monotonic()))
            if thread.is_alive():
                self.store.append_event(
                    "notification_failed",
                    {"event": kind, "error": "delivery_timeout"},
                )

    def execute_reserved(
        self,
        record: OrderRecord,
        metadata: MarketMetadata,
        *,
        fast: bool = False,
        timing_context: Optional[Dict[str, Any]] = None,
    ) -> OrderResult:
        timing_context = dict(timing_context or {})
        execute_start_wall = self._wall_clock()
        execute_start_mono = self._monotonic()
        if self.settings.dry_run:
            raw = {
                "mode": "shadow",
                "reason": "AFTERTAKE_DRY_RUN=true",
                "simulated_take": True,
                "no_live_order": True,
                "timing": {**timing_context, "execute_start_ts": execute_start_wall},
                "market_delay": {
                    "immediate_taker_order_delay_enabled": bool(metadata.immediate_taker_order_delay_enabled),
                    "expected_taker_delay_ms": float(metadata.expected_taker_delay_ms),
                },
            }
            self.store.mark_terminal_execution(
                record.intent_id,
                record.requested_qty,
                record.requested_price,
                raw,
                "shadow_fill",
            )
            return OrderResult(
                dry_run=True,
                intent_id=record.intent_id,
                order_id="shadow-" + uuid.uuid4().hex[:12],
                status="shadow_fill",
                side=record.side,
                token_id=record.token_id,
                price=record.requested_price,
                requested_qty=record.requested_qty,
                filled_qty=record.requested_qty,
                avg_price=record.requested_price,
                notional=record.requested_price * record.requested_qty,
                terminal=True,
                submission_state="not_submitted",
                raw=raw,
                event_ts=self._wall_clock(),
                immediate_taker_order_delay_enabled=metadata.immediate_taker_order_delay_enabled,
                expected_taker_delay_ms=metadata.expected_taker_delay_ms,
            )
        if self.gateway is None:
            raise RuntimeError("live order executor requires a V2 CLOB gateway")

        try:
            submit_start_wall = 0.0
            submit_start_mono = 0.0
            submit_end_wall = 0.0
            submit_end_mono = 0.0
            try:
                fast_submit = getattr(self.gateway, "submit_limit_buy_fast", None) if fast else None
                submit_start_wall = self._wall_clock()
                submit_start_mono = self._monotonic()
                if fast_submit is not None:
                    submitted = fast_submit(
                        record.token_id,
                        record.requested_price,
                        record.requested_qty,
                        metadata,
                        self.settings.order_type,
                    )
                else:
                    submitted = self.gateway.submit_limit_buy(
                        record.token_id,
                        record.requested_price,
                        record.requested_qty,
                        metadata,
                        self.settings.order_type,
                    )
                submit_end_mono = self._monotonic()
                submit_end_wall = self._wall_clock()
            except Exception as exc:
                submit_end_mono = self._monotonic()
                submit_end_wall = self._wall_clock()
                timing = self._build_timing(
                    timing_context,
                    execute_start_wall=execute_start_wall,
                    submit_start_wall=submit_start_wall or execute_start_wall,
                    submit_end_wall=submit_end_wall,
                    submit_start_mono=submit_start_mono or execute_start_mono,
                    submit_end_mono=submit_end_mono,
                )
                raw = {
                    "submit_error": _safe_error_message(str(exc)),
                    "timing": timing,
                    "_market_delay": {
                        "immediate_taker_order_delay_enabled": bool(metadata.immediate_taker_order_delay_enabled),
                        "expected_taker_delay_ms": float(metadata.expected_taker_delay_ms),
                    },
                    **_sanitize_exception(exc, phase="post_order", order_type=self.settings.order_type),
                }
                if _is_fak_no_match_exception(exc, self.settings.order_type):
                    # Polymarket CLOB returns an explicit 400/422 no-match for
                    # zero-fill FAK attempts.  No order id is expected because
                    # no resting order matched; treat it as a terminal miss, not
                    # an ambiguous submission that freezes future unrelated risk.
                    reason = "fak_no_matching_resting_order"
                    raw["classification"] = reason
                    raw["terminal_no_fill"] = True
                    self.store.mark_terminal_execution(record.intent_id, 0.0, 0.0, raw, "no_fill")
                    return self._result_from_record(
                        record, "no_fill", 0.0, 0.0, True, "venue_no_match", reason, raw
                    )

                reason = "matching_engine_restart" if is_matching_engine_restart_error(exc) else "submit_exception"
                raw["classification"] = reason
                if _is_definite_submit_rejection(exc):
                    # A concrete 4xx response (other than timeout/rate limit)
                    # is a negative venue acknowledgement: the request was
                    # rejected and cannot have created a resting order.
                    raw["terminal_skip"] = True
                    self.store.mark_terminal_execution(record.intent_id, 0.0, 0.0, raw, "submit_skipped")
                    return self._result_from_record(
                        record, "submit_skipped", 0.0, 0.0, True, "skipped", reason, raw
                    )

                # The request may have reached the matching engine before the
                # transport failed.  Retrying could duplicate live risk and
                # declaring zero-fill would hide it, so preserve the intent as
                # unresolved until an operator attaches an authoritative id.
                raw["ambiguous_submission"] = True
                self.store.mark_execution_unknown(record.intent_id, reason, raw)
                return self._result_from_record(
                    record, "execution_unknown", 0.0, 0.0, False, "unknown", reason, raw
                )

            timing = self._build_timing(
                timing_context,
                execute_start_wall=execute_start_wall,
                submit_start_wall=submit_start_wall,
                submit_end_wall=submit_end_wall,
                submit_start_mono=submit_start_mono,
                submit_end_mono=submit_end_mono,
            )
            submitted = dict(submitted)
            submitted["_timing"] = timing
            submitted["_market_delay"] = {
                "immediate_taker_order_delay_enabled": bool(metadata.immediate_taker_order_delay_enabled),
                "expected_taker_delay_ms": float(metadata.expected_taker_delay_ms),
            }
            order_id = str(submitted.get("orderID") or submitted.get("id") or "")
            if not order_id:
                reason = "missing_order_id_after_submit"
                raw = dict(submitted)
                raw["timing"] = timing
                raw["classification"] = reason
                raw["ambiguous_submission"] = True
                self.store.mark_execution_unknown(record.intent_id, reason, raw)
                return self._result_from_record(
                    record, "execution_unknown", 0.0, 0.0, False, "unknown", reason, raw
                )
            self.store.mark_submitted(record.intent_id, order_id, submitted)
            self._emit(
                "submitted",
                {
                    "slug": record.slug,
                    "side": record.side,
                    "order_id": order_id,
                    "requested_qty": record.requested_qty,
                    "requested_price": record.requested_price,
                    "order_type": self.settings.order_type,
                    "event_ts": submit_end_wall,
                    "scheduled_close_ts": timing.get("scheduled_close_ts"),
                    "actual_submit_ts": timing.get("submit_start_ts"),
                    "submit_lag_ms": timing.get("submit_lag_ms"),
                    "snapshot_decision_ts": timing.get("snapshot_decision_ts"),
                    "post_close_snapshot_ts": timing.get("post_close_snapshot_ts"),
                    "decision_to_submit_ms": timing.get("decision_to_submit_ms", -1.0),
                    "submit_roundtrip_ms": timing.get("submit_roundtrip_ms", -1.0),
                    "observed_book_age_ms": timing.get("observed_book_age_ms", -1.0),
                    "immediate_taker_order_delay_enabled": metadata.immediate_taker_order_delay_enabled,
                    "expected_taker_delay_ms": metadata.expected_taker_delay_ms,
                },
            )
            return self._reconcile(record, order_id, submitted)
        finally:
            # The account heartbeat is deliberately not stopped here.  A GTC
            # may remain live after this bounded reconciliation window.
            pass

    def reconcile_existing(self, record: OrderRecord) -> OrderResult:
        """Reconcile a persisted submitted order before new live risk is allowed."""

        if self.gateway is None:
            raise RuntimeError("reconciliation requires a V2 CLOB gateway")
        if not record.order_id:
            reason = "persisted_intent_has_no_order_id"
            self.store.mark_execution_unknown(record.intent_id, reason)
            return self._result_from_record(
                record, "execution_unknown", 0.0, 0.0, False, "unknown", reason, {}
            )
        return self._reconcile(record, record.order_id, {"recovery": True})

    def reconcile_existing_once(self, record: OrderRecord) -> OrderResult:
        """Perform one authoritative status probe without a 45-second sweep.

        Startup and background maintenance may have many persisted orders. A
        full reconciliation deadline per record can starve the scheduler or
        create a permanent reboot loop. One successful CLOB lookup is enough to
        settle a terminal order or safely leave a GTC pending for the next pass.
        """

        if self.gateway is None:
            raise RuntimeError("reconciliation requires a V2 CLOB gateway")
        if not record.order_id:
            reason = "persisted_intent_has_no_order_id"
            self.store.mark_execution_unknown(record.intent_id, reason)
            return self._result_from_record(
                record, "execution_unknown", 0.0, 0.0, False, "unknown", reason, {}
            )

        reconcile_start_wall = self._wall_clock()
        started_at = self._monotonic()
        deadline = started_at + self.settings.reconcile_timeout_s
        try:
            order = self._call_bounded(
                lambda: self.gateway.get_order(record.order_id),
                max(0.01, deadline - self._monotonic()),
                "get_order",
            )
        except Exception as exc:
            raw = {
                "submit": record.raw or {},
                "classification": "reconcile_transport_timeout",
                "ambiguous_submission": True,
                "error_message": _safe_error_message(str(exc)),
                "timing": self._merge_reconcile_timing(
                    {"_timing": dict((record.raw or {}).get("timing") or {})},
                    reconcile_start_wall,
                    started_at,
                ),
            }
            self.store.mark_execution_unknown(
                record.intent_id, "reconcile_transport_timeout", raw
            )
            return self._result_from_record(
                record,
                "execution_unknown",
                0.0,
                0.0,
                False,
                "unknown",
                "reconcile_transport_timeout",
                raw,
            )
        if record.raw.get("requires_identity_validation"):
            identity_error = self._recovered_identity_error(record, record.order_id, order)
            if identity_error:
                raw = {
                    **dict(record.raw or {}),
                    "order": order,
                    "identity_error": identity_error,
                    "requires_identity_validation": True,
                    "classification": "recovered_order_identity_mismatch",
                }
                self.store.mark_execution_unknown(
                    record.intent_id, "recovered_order_identity_mismatch", raw
                )
                return self._result_from_record(
                    record,
                    "execution_unknown",
                    0.0,
                    0.0,
                    False,
                    "unknown",
                    identity_error,
                    raw,
                )
        status = _normalise_status(order.get("status") or "")
        trades: List[Dict[str, Any]] = []
        provisional_qty, provisional_avg = self._summarize_fill(record, order, trades)
        if _terminal_status(status) and provisional_qty > 0 and provisional_avg <= 0:
            try:
                trades = self._call_bounded(
                    lambda: self.gateway.order_trades(record.token_id, record.order_id),
                    max(0.01, deadline - self._monotonic()),
                    "order_trades",
                )
            except Exception as exc:
                if isinstance(exc, TimeoutError):
                    raw = {
                        "submit": record.raw or {},
                        "order": order,
                        "classification": "reconcile_transport_timeout",
                        "ambiguous_submission": True,
                        "error_message": _safe_error_message(str(exc)),
                        "timing": self._merge_reconcile_timing(
                            {"_timing": dict((record.raw or {}).get("timing") or {})},
                            reconcile_start_wall,
                            started_at,
                        ),
                    }
                    self.store.mark_execution_unknown(
                        record.intent_id, "reconcile_transport_timeout", raw
                    )
                    return self._result_from_record(
                        record,
                        "execution_unknown",
                        provisional_qty,
                        0.0,
                        False,
                        "unknown",
                        "reconcile_transport_timeout",
                        raw,
                    )
                order = {**order, "trades_lookup_error": str(exc)}
        timing = self._merge_reconcile_timing(
            {"_timing": dict((record.raw or {}).get("timing") or {})},
            reconcile_start_wall,
            started_at,
        )
        raw = {
            "submit": record.raw or {},
            "order": order,
            "trades": trades,
            "timing": timing,
            "single_probe": True,
        }
        if _terminal_status(status):
            filled_qty, avg_price = self._summarize_fill(record, order, trades)
            if filled_qty > 0 and avg_price <= 0:
                reason = "confirmed_fill_without_execution_price"
                self.store.mark_execution_unknown(record.intent_id, reason, raw)
                return self._result_from_record(
                    record,
                    "execution_unknown",
                    filled_qty,
                    0.0,
                    False,
                    "unknown",
                    reason,
                    raw,
                )
            self.store.mark_terminal_execution(record.intent_id, filled_qty, avg_price, raw, status)
            return self._result_from_record(
                record,
                status,
                filled_qty,
                avg_price,
                True,
                "acknowledged",
                "",
                raw,
            )

        raw["awaiting_settlement"] = True
        raw["order_type"] = self.settings.order_type
        return self._result_from_record(
            record,
            "submitted_pending",
            0.0,
            0.0,
            False,
            "awaiting_settlement",
            "gtc_awaiting_settlement",
            raw,
        )

    def _reconcile(
        self, record: OrderRecord, order_id: str, submitted: Dict[str, Any]
    ) -> OrderResult:
        assert self.gateway is not None
        reconcile_start_wall = self._wall_clock()
        started_at = self._monotonic()
        deadline = started_at + self.settings.reconcile_timeout_s
        cancelable_order = self.settings.order_type in {"GTD"}
        # Owner-approved live policy: default GTC should not be locally
        # cancelled after a tiny TTL. A submitted GTC may rest through the
        # post-close window; keep it pending and let later reconciliation /
        # official settlement decide the outcome. GTD remains the only
        # explicitly locally-cancelled resting order type.
        cancel_at = min(started_at + self.settings.order_ttl_s, deadline - 0.5) if cancelable_order else float("inf")
        cancel_sent = False
        cancel_raw: Dict[str, Any] = {}
        cancel_errors: List[str] = []
        last_order: Dict[str, Any] = {}
        last_trades: List[Dict[str, Any]] = []
        reconcile_transport_error = False

        while self._monotonic() <= deadline:
            order_lookup_ok = False
            try:
                last_order = self._call_bounded(
                    lambda: self.gateway.get_order(order_id),
                    max(0.01, deadline - self._monotonic()),
                    "get_order",
                )
                order_lookup_ok = True
            except Exception as exc:
                last_order = {"lookup_error": str(exc)}
                if isinstance(exc, TimeoutError):
                    reconcile_transport_error = True
                    break
            if order_lookup_ok and record.raw.get("requires_identity_validation"):
                identity_error = self._recovered_identity_error(record, order_id, last_order)
                if identity_error:
                    raw = {
                        **dict(record.raw or {}),
                        "order": last_order,
                        "identity_error": identity_error,
                        "requires_identity_validation": True,
                        "classification": "recovered_order_identity_mismatch",
                    }
                    self.store.mark_execution_unknown(
                        record.intent_id, "recovered_order_identity_mismatch", raw
                    )
                    return self._result_from_record(
                        record,
                        "execution_unknown",
                        0.0,
                        0.0,
                        False,
                        "unknown",
                        identity_error,
                        raw,
                    )
            status = _normalise_status(last_order.get("status") or "") if order_lookup_ok else ""
            if order_lookup_ok and _terminal_status(status):
                # A live/unmatched order needs only the direct ID lookup.  The
                # old loop paginated the account's full trade history on every
                # 500ms poll, multiplying one pending GTC into thousands of
                # HTTP requests across six assets. Query trades only when a
                # terminal fill has quantity but no authoritative average.
                provisional_qty, provisional_avg = self._summarize_fill(
                    record, last_order, []
                )
                if provisional_qty > 0 and provisional_avg <= 0:
                    try:
                        last_trades = self._call_bounded(
                            lambda: self.gateway.order_trades(record.token_id, order_id),
                            max(0.01, deadline - self._monotonic()),
                            "order_trades",
                        )
                    except Exception as exc:
                        if isinstance(exc, TimeoutError):
                            last_order["trades_lookup_error"] = str(exc)
                            raw = {
                                "submit": submitted,
                                "order": last_order,
                                "cancel": cancel_raw,
                                "trades": [],
                                "classification": "reconcile_transport_timeout",
                                "ambiguous_submission": True,
                                "timing": self._merge_reconcile_timing(
                                    submitted, reconcile_start_wall, started_at
                                ),
                            }
                            self.store.mark_execution_unknown(
                                record.intent_id, "reconcile_transport_timeout", raw
                            )
                            return self._result_from_record(
                                record,
                                "execution_unknown",
                                provisional_qty,
                                0.0,
                                False,
                                "unknown",
                                "reconcile_transport_timeout",
                                raw,
                            )
                        last_trades = []
                        last_order["trades_lookup_error"] = str(exc)
                filled_qty, avg_price = self._summarize_fill(record, last_order, last_trades)
                timing = self._merge_reconcile_timing(submitted, reconcile_start_wall, started_at)
                raw = {
                    "submit": submitted,
                    "order": last_order,
                    "cancel": cancel_raw,
                    "trades": last_trades,
                    "timing": timing,
                }
                if filled_qty > 0 and avg_price <= 0:
                    reason = "confirmed_fill_without_execution_price"
                    self.store.mark_execution_unknown(record.intent_id, reason, raw)
                    return self._result_from_record(
                        record,
                        "execution_unknown",
                        filled_qty,
                        0.0,
                        False,
                        "unknown",
                        reason,
                        raw,
                    )
                self.store.mark_terminal_execution(record.intent_id, filled_qty, avg_price, raw, status)
                return self._result_from_record(
                    record,
                    status,
                    filled_qty,
                    avg_price,
                    True,
                    "acknowledged",
                    "",
                    raw,
                )

            if cancelable_order and not cancel_sent and self._monotonic() >= cancel_at:
                # GTD only: explicitly cancel after bounded TTL. GTC is left
                # working; FAK/FOK rely on exchange-side immediate semantics.
                try:
                    cancel_raw = self.gateway.cancel_order(order_id)
                    cancel_sent = True
                except Exception as exc:
                    # Cancellation is idempotent and safe to retry.  A restart
                    # may return 425 temporarily, so keep trying until the
                    # bounded reconciliation deadline.
                    cancel_errors.append(str(exc))
                    cancel_raw = {"cancel_errors": list(cancel_errors)}
            elapsed = max(0.0, self._monotonic() - started_at)
            poll_s = min(2.0, max(0.5, elapsed / 4.0))
            self._sleep(min(poll_s, max(0.01, deadline - self._monotonic())))

        timing = self._merge_reconcile_timing(submitted, reconcile_start_wall, started_at)
        raw = {
            "submit": submitted,
            "order": last_order,
            "cancel": cancel_raw,
            "trades": last_trades,
            "timing": timing,
        }
        if reconcile_transport_error:
            raw["classification"] = "reconcile_transport_timeout"
            raw["reconcile_transport_error"] = True
            raw["error_message"] = str(last_order.get("lookup_error") or "reconcile transport timeout")
        reason = "order_not_terminal_after_reconcile_timeout"
        if self.settings.order_type == "GTC" and order_id:
            raw["awaiting_settlement"] = True
            raw["order_type"] = self.settings.order_type
            return self._result_from_record(
                record,
                "submitted_pending",
                0.0,
                0.0,
                False,
                "awaiting_settlement",
                "reconcile_transport_timeout" if reconcile_transport_error else "gtc_awaiting_settlement",
                raw,
            )
        self.store.mark_execution_unknown(record.intent_id, reason, raw)
        return self._result_from_record(
            record, "execution_unknown", 0.0, 0.0, False, "unknown", reason, raw
        )

    def _call_bounded(
        self,
        call: Callable[[], Any], timeout_s: float, label: str
    ) -> Any:
        """Run one SDK probe under a deadline even if its client ignores it."""

        done = threading.Event()
        result: List[Any] = []
        failure: List[BaseException] = []

        def invoke() -> None:
            try:
                result.append(call())
            except BaseException as exc:  # preserve SDK exception types
                failure.append(exc)
            finally:
                done.set()

        thread = threading.Thread(
            target=invoke,
            daemon=True,
            name="aftertake-clob-%s" % str(label),
        )
        thread.start()
        if not done.wait(max(0.01, float(timeout_s))):
            reason = "%s probe exceeded reconciliation deadline" % label
            self._mark_process_restart_required(reason)
            raise TimeoutError(reason)
        if failure:
            raise failure[0]
        return result[0] if result else None


    def _build_timing(
        self,
        context: Dict[str, Any],
        *,
        execute_start_wall: float,
        submit_start_wall: float,
        submit_end_wall: float,
        submit_start_mono: float,
        submit_end_mono: float,
    ) -> Dict[str, Any]:
        timing = dict(context)
        timing["execute_start_ts"] = float(execute_start_wall)
        timing["submit_start_ts"] = float(submit_start_wall)
        timing["submit_end_ts"] = float(submit_end_wall)
        decision_ts = timing.get("decision_ts")
        try:
            timing["decision_to_submit_ms"] = max(0.0, (float(submit_start_wall) - float(decision_ts)) * 1000.0)
        except (TypeError, ValueError):
            timing["decision_to_submit_ms"] = -1.0
        book_ts = timing.get("book_observed_ts")
        try:
            timing["observed_book_age_ms"] = max(0.0, (float(submit_start_wall) - float(book_ts)) * 1000.0)
        except (TypeError, ValueError):
            timing["observed_book_age_ms"] = -1.0
        timing["submit_roundtrip_ms"] = max(0.0, (float(submit_end_mono) - float(submit_start_mono)) * 1000.0)
        scheduled_close = timing.get("scheduled_close_ts")
        try:
            timing["submit_lag_ms"] = max(0.0, (float(submit_start_wall) - float(scheduled_close)) * 1000.0)
        except (TypeError, ValueError):
            timing["submit_lag_ms"] = -1.0
        return timing

    def _merge_reconcile_timing(
        self, submitted: Dict[str, Any], reconcile_start_wall: float, reconcile_start_mono: float
    ) -> Dict[str, Any]:
        timing = dict(submitted.get("_timing") or {})
        timing["reconcile_start_ts"] = float(reconcile_start_wall)
        timing["reconcile_end_ts"] = float(self._wall_clock())
        timing["reconcile_duration_ms"] = max(0.0, (self._monotonic() - reconcile_start_mono) * 1000.0)
        return timing

    @staticmethod
    def _summarize_fill(
        record: OrderRecord, order: Dict[str, Any], trades: List[Dict[str, Any]]
    ) -> tuple:
        order_qty = _number(
            order,
            ("size_matched", "sizeMatched", "filled_size", "filled_qty", "matched_size"),
        )
        trade_qty = 0.0
        trade_notional = 0.0
        for trade in trades:
            quantity = _number(trade, ("size", "amount", "matched_size", "size_matched"))
            price = _number(trade, ("price", "execution_price", "avg_price"))
            if quantity > 0 and price > 0:
                trade_qty += quantity
                trade_notional += quantity * price
        filled_qty = min(record.requested_qty, max(order_qty, trade_qty))
        order_avg = _number(
            order, ("average_price", "avg_price", "execution_price"), 0.0
        )
        if order_avg > 0:
            avg_price = order_avg
        elif (
            trade_qty > 0
            and trade_notional > 0
            and abs(trade_qty - filled_qty) <= 1e-9
        ):
            avg_price = trade_notional / trade_qty
        else:
            avg_price = 0.0
        if filled_qty <= 0:
            avg_price = 0.0
        return filled_qty, avg_price

    def _recovered_identity_error(
        self, record: OrderRecord, order_id: str, order: Dict[str, Any]
    ) -> str:
        observed_id = str(order.get("id") or order.get("orderID") or "")
        token_id = str(
            order.get("asset_id")
            or order.get("assetId")
            or order.get("token_id")
            or order.get("tokenID")
            or ""
        )
        side = str(order.get("side") or "").upper()
        original_size = _number(
            order, ("original_size", "originalSize", "size"), -1.0
        )
        limit_price = _number(order, ("price",), -1.0)
        if observed_id != order_id:
            return "recovered order ID does not match authenticated response"
        if token_id != record.token_id:
            return "recovered order token does not match reserved intent"
        if side != "BUY":
            return "recovered order side is not BUY"
        if abs(original_size - record.requested_qty) > 1e-9:
            return "recovered order size does not match reserved intent"
        if abs(limit_price - record.requested_price) > 1e-9:
            return "recovered order price does not match reserved intent"
        maker = str(
            order.get("maker_address")
            or order.get("makerAddress")
            or order.get("maker")
            or ""
        )
        if maker and self.settings.polymarket_funder:
            if maker.lower() != self.settings.polymarket_funder.lower():
                return "recovered order maker does not match configured funder"
        return ""

    @staticmethod
    def _result_from_record(
        record: OrderRecord,
        status: str,
        filled_qty: float,
        avg_price: float,
        terminal: bool,
        submission_state: str,
        error: str,
        raw: Dict[str, Any],
    ) -> OrderResult:
        timing = dict(raw.get("timing") or raw.get("submit", {}).get("_timing") or {})
        market_delay = _market_delay_from_raw(raw)
        return OrderResult(
            dry_run=False,
            intent_id=record.intent_id,
            order_id=str(raw.get("submit", {}).get("orderID") or record.order_id or ""),
            status=status,
            side=record.side,
            token_id=record.token_id,
            price=record.requested_price,
            requested_qty=record.requested_qty,
            filled_qty=filled_qty,
            avg_price=avg_price,
            notional=filled_qty * avg_price,
            terminal=terminal,
            submission_state=submission_state,
            error=error,
            raw=raw,
            event_ts=float(timing.get("reconcile_end_ts") or timing.get("submit_end_ts") or 0.0),
            decision_to_submit_ms=float(timing.get("decision_to_submit_ms", -1.0)),
            submit_roundtrip_ms=float(timing.get("submit_roundtrip_ms", -1.0)),
            reconcile_duration_ms=float(timing.get("reconcile_duration_ms", -1.0)),
            observed_book_age_ms=float(timing.get("observed_book_age_ms", -1.0)),
            immediate_taker_order_delay_enabled=market_delay["immediate_taker_order_delay_enabled"],
            expected_taker_delay_ms=market_delay["expected_taker_delay_ms"],
        )
