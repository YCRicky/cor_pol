"""Explicit V2 CLOB order lifecycle with durable recovery semantics."""

from __future__ import annotations

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

    @property
    def qty(self) -> float:
        """Compatibility alias; callers must prefer ``filled_qty`` for PnL."""

        return self.filled_qty


class HeartbeatLoop:
    """CLOB order heartbeat, separate from WebSocket ping/keepalive."""

    def __init__(self, gateway: OrderGateway, interval_s: float):
        self.gateway = gateway
        self.interval_s = float(interval_s)
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self.last_error = ""
        self._heartbeat_id = ""

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._run, daemon=True, name="aftertake-heartbeat")
        self._thread.start()

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                response = self.gateway.post_heartbeat(self._heartbeat_id)
                heartbeat_id = str(response.get("heartbeat_id") or "")
                if heartbeat_id:
                    self._heartbeat_id = heartbeat_id
            except Exception as exc:  # execution reconciliation will fail closed
                self.last_error = str(exc)
            self._stop.wait(self.interval_s)

    def stop(self) -> None:
        self._stop.set()
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

    It never automatically retries a failed/ambiguous submission.  A CLOB
    timeout, HTTP 425 restart, missing order ID, or non-terminal cancellation
    produces ``execution_unknown`` and freezes future entries through the
    durable state store.
    """

    def __init__(
        self,
        settings: Settings,
        store: StateStore,
        gateway: Optional[OrderGateway] = None,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        heartbeat_factory: Callable[[OrderGateway, float], HeartbeatLoop] = HeartbeatLoop,
        event_callback: Optional[Callable[[str, Dict[str, Any]], None]] = None,
    ):
        self.settings = settings
        self.store = store
        self.gateway = gateway
        self._sleep = sleep
        self._monotonic = monotonic
        self._heartbeat_factory = heartbeat_factory
        self._event_callback = event_callback
        self._event_threads: List[tuple] = []
        self._event_threads_lock = threading.Lock()

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
        self, record: OrderRecord, metadata: MarketMetadata, *, fast: bool = False
    ) -> OrderResult:
        if self.settings.dry_run:
            raw = {
                "mode": "shadow",
                "reason": "AFTERTAKE_DRY_RUN=true",
                "simulated_take": True,
                "no_live_order": True,
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
            )
        if self.gateway is None:
            raise RuntimeError("live order executor requires a V2 CLOB gateway")

        heartbeat = self._heartbeat_factory(self.gateway, self.settings.heartbeat_interval_s)
        heartbeat.start()
        try:
            try:
                fast_submit = getattr(self.gateway, "submit_limit_buy_fast", None) if fast else None
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
            except Exception as exc:
                raw = {
                    "submit_error": _safe_error_message(str(exc)),
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
                        record, "no_fill", 0.0, 0.0, True, "acknowledged", reason, raw
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

            order_id = str(submitted.get("orderID") or submitted.get("id") or "")
            if not order_id:
                reason = "missing_order_id_after_submit"
                raw = dict(submitted)
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
        heartbeat = self._heartbeat_factory(self.gateway, self.settings.heartbeat_interval_s)
        heartbeat.start()
        try:
            return self._reconcile(record, record.order_id, {"recovery": True})
        finally:
            heartbeat.stop()

    def _reconcile(
        self, record: OrderRecord, order_id: str, submitted: Dict[str, Any]
    ) -> OrderResult:
        assert self.gateway is not None
        started_at = self._monotonic()
        deadline = started_at + self.settings.reconcile_timeout_s
        cancelable_order = self.settings.order_type in {"GTC", "GTD"}
        # FAK/FOK are exchange-side immediate-or-cancel/fill-or-kill intents;
        # do not emulate taker behavior by submitting GTC and cancelling after
        # a tiny local TTL. Only resting-capable orders receive a local cancel.
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
                raw = {
                    "submit": submitted,
                    "order": last_order,
                    "cancel": cancel_raw,
                    "trades": last_trades,
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
                # Resting-capable order: explicitly cancel after bounded TTL.
                # FAK/FOK skip this path and rely on exchange-side immediate
                # semantics while reconciliation polls longer for slow status propagation.
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

        raw = {
            "submit": submitted,
            "order": last_order,
            "cancel": cancel_raw,
            "trades": last_trades,
        }
        reason = "order_not_terminal_after_reconcile_timeout"
        self.store.mark_execution_unknown(record.intent_id, reason, raw)
        return self._result_from_record(
            record, "execution_unknown", 0.0, 0.0, False, "unknown", reason, raw
        )

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
        )
