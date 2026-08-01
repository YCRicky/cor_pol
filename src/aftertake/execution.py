"""Explicit V2 CLOB order lifecycle with durable recovery semantics."""

from __future__ import annotations

import json
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

    def __init__(self, gateway: OrderGateway, interval_s: float):
        self.gateway = gateway
        self.interval_s = float(interval_s)
        self.call_timeout_s = max(0.1, min(2.0, self.interval_s * 0.5))
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._request_thread: Optional[threading.Thread] = None
        self._request_lock = threading.Lock()
        self.last_error = ""
        self._heartbeat_id = ""
        self.last_success_at = 0.0
        self.consecutive_failures = 0
        self.status_callback: Optional[Callable[[str, Dict[str, Any]], None]] = None

    @property
    def heartbeat_id(self) -> str:
        """Return the most recent server-issued heartbeat id."""

        return self._heartbeat_id

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._run, daemon=True, name="aftertake-heartbeat")
        self._thread.start()

    def _post_heartbeat_bounded(self) -> Dict[str, Any]:
        """Bound one SDK call and prevent overlapping hung request threads."""

        with self._request_lock:
            if self._request_thread is not None and self._request_thread.is_alive():
                raise TimeoutError("previous heartbeat request is still in flight")
            done = threading.Event()
            result: Dict[str, Any] = {}
            failure: List[BaseException] = []

            def call() -> None:
                try:
                    response = self.gateway.post_heartbeat(self._heartbeat_id)
                    if isinstance(response, dict):
                        result.update(response)
                    else:
                        result["raw"] = response
                except Exception as exc:  # propagate SDK failures to the loop
                    failure.append(exc)
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
            self._request_thread = None
        if failure:
            raise failure[0]
        return result

    def _notify_status(self, kind: str, payload: Dict[str, Any]) -> None:
        callback = self.status_callback
        if callback is None:
            return
        try:
            callback(kind, payload)
        except Exception:
            # Diagnostics must never terminate the heartbeat loop.
            return

    def _run(self) -> None:
        had_error = False
        while not self._stop.is_set():
            wait_s = self.interval_s
            try:
                response = self._post_heartbeat_bounded()
                heartbeat_id = _heartbeat_id_from_value(response)
                if heartbeat_id:
                    self._heartbeat_id = heartbeat_id
                self.last_error = ""
                self.last_success_at = time.time()
                self.consecutive_failures = 0
                if had_error:
                    self._notify_status(
                        "heartbeat_recovered",
                        {"reason": "CLOB heartbeat restored", "heartbeat_id": self._heartbeat_id},
                    )
                had_error = False
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
                    wait_s = min(0.25, self.interval_s)
                error_value = getattr(exc, "error_msg", None)
                self.last_error = _safe_error_message(error_value if error_value is not None else exc)
                had_error = True
                self._notify_status(
                    "heartbeat_error",
                    {
                        "reason": "CLOB heartbeat failed",
                        "error_message": self.last_error,
                        "heartbeat_id": self._heartbeat_id,
                        "consecutive_failures": self.consecutive_failures,
                    },
                )
            self._stop.wait(max(0.01, wait_s))

    def stop(self) -> None:
        self._stop.set()
        request_thread = self._request_thread
        if request_thread is not None:
            request_thread.join(timeout=self.call_timeout_s + 0.25)
        if self._thread is not None:
            self._thread.join(timeout=max(1.0, self.interval_s + 1.0))


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
    market.  Polymarket CLOB/network submit-path failures are terminal-skipped
    for the affected slug; confirmed fills/reconciliation still update durable
    state, but stale infrastructure errors must not globally block future entries.
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
        event_callback: Optional[Callable[[str, Dict[str, Any]], None]] = None,
    ):
        self.settings = settings
        self.store = store
        self.gateway = gateway
        self._sleep = sleep
        self._monotonic = monotonic
        self._wall_clock = wall_clock
        self._heartbeat_factory = heartbeat_factory
        self._event_callback = event_callback
        self._event_threads: List[tuple] = []
        self._event_threads_lock = threading.Lock()

    def _new_heartbeat(self) -> HeartbeatLoop:
        heartbeat = self._heartbeat_factory(self.gateway, self.settings.heartbeat_interval_s)
        if hasattr(heartbeat, "status_callback"):
            heartbeat.status_callback = lambda kind, payload: self._emit(kind, payload)
        return heartbeat

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

        heartbeat = self._new_heartbeat()
        heartbeat.start()
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

                # Polymarket CLOB/network failures are common enough that they
                # must not globally freeze future entries.  Do not retry this
                # market after a submit-path exception, but persist diagnostics
                # and terminal-skip only the affected slug.
                reason = "matching_engine_restart" if is_matching_engine_restart_error(exc) else "submit_exception"
                raw["classification"] = reason
                raw["terminal_skip"] = True
                self.store.mark_terminal_execution(record.intent_id, 0.0, 0.0, raw, "submit_skipped")
                return self._result_from_record(
                    record, "submit_skipped", 0.0, 0.0, True, "skipped", reason, raw
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
                raw["terminal_skip"] = True
                self.store.mark_terminal_execution(record.intent_id, 0.0, 0.0, raw, "submit_skipped")
                return self._result_from_record(
                    record, "submit_skipped", 0.0, 0.0, True, "skipped", reason, raw
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
                    "event_ts": submit_end_wall,
                    "decision_to_submit_ms": timing.get("decision_to_submit_ms", -1.0),
                    "submit_roundtrip_ms": timing.get("submit_roundtrip_ms", -1.0),
                    "observed_book_age_ms": timing.get("observed_book_age_ms", -1.0),
                    "immediate_taker_order_delay_enabled": metadata.immediate_taker_order_delay_enabled,
                    "expected_taker_delay_ms": metadata.expected_taker_delay_ms,
                },
            )
            return self._reconcile(record, order_id, submitted)
        finally:
            heartbeat.stop()

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
        heartbeat = self._new_heartbeat()
        heartbeat.start()
        try:
            return self._reconcile(record, record.order_id, {"recovery": True})
        finally:
            heartbeat.stop()

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

        while self._monotonic() <= deadline:
            order_lookup_ok = False
            try:
                last_order = self.gateway.get_order(order_id)
                order_lookup_ok = True
            except Exception as exc:
                last_order = {"lookup_error": str(exc)}
            if order_lookup_ok and record.raw.get("requires_identity_validation"):
                identity_error = self._recovered_identity_error(record, order_id, last_order)
                if identity_error:
                    raw = {"order": last_order, "identity_error": identity_error}
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
            try:
                last_trades = self.gateway.order_trades(record.token_id, order_id)
            except Exception as exc:
                # A lookup failure cannot prove a known terminal state.
                last_trades = []
                last_order["trades_lookup_error"] = str(exc)
            status = _normalise_status(last_order.get("status") or "") if order_lookup_ok else ""
            if order_lookup_ok and _terminal_status(status):
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
            self._sleep(min(0.5, max(0.01, deadline - self._monotonic())))

        timing = self._merge_reconcile_timing(submitted, reconcile_start_wall, started_at)
        raw = {
            "submit": submitted,
            "order": last_order,
            "cancel": cancel_raw,
            "trades": last_trades,
            "timing": timing,
        }
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
                "gtc_awaiting_settlement",
                raw,
            )
        self.store.mark_execution_unknown(record.intent_id, reason, raw)
        return self._result_from_record(
            record, "execution_unknown", 0.0, 0.0, False, "unknown", reason, raw
        )


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
