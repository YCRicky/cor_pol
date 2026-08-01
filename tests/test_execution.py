import threading

import pytest

from aftertake.config import Settings
from aftertake.execution import HeartbeatLoop, OrderExecutor
from aftertake.pm_client import MarketMetadata
from aftertake.state import StateStore


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


class NoopHeartbeat:
    def __init__(self, gateway, interval):
        self.gateway = gateway

    def start(self):
        return None

    def stop(self):
        return None


def test_heartbeat_loop_adopts_replacement_id_from_invalid_id_response():
    calls = []
    recovered = threading.Event()
    statuses = []

    class InvalidHeartbeat(Exception):
        status_code = 400
        error_msg = {"heartbeat_id": "replacement-id", "error_msg": "Invalid Heartbeat ID"}

    class Gateway:
        def post_heartbeat(self, heartbeat_id=""):
            calls.append(heartbeat_id)
            if not heartbeat_id:
                raise InvalidHeartbeat("request error")
            recovered.set()
            return {"heartbeat_id": heartbeat_id}

    loop = HeartbeatLoop(Gateway(), 0.01)
    loop.status_callback = lambda kind, payload: statuses.append((kind, payload))
    loop.start()
    try:
        assert recovered.wait(1.0)
    finally:
        loop.stop()

    assert calls[0] == ""
    assert "replacement-id" in calls
    assert loop.heartbeat_id == "replacement-id"
    assert loop.last_error == ""
    assert statuses[0][0] == "heartbeat_error"
    assert statuses[0][1]["consecutive_failures"] == 1
    assert statuses[0][1]["heartbeat_id"] == "replacement-id"
    assert statuses[-1][0] == "heartbeat_recovered"


def test_heartbeat_loop_bounds_and_does_not_overlap_a_hung_sdk_call():
    started = threading.Event()
    release = threading.Event()
    calls = []

    class Gateway:
        def post_heartbeat(self, heartbeat_id=""):
            calls.append(heartbeat_id)
            started.set()
            release.wait(timeout=1.0)
            return {"heartbeat_id": "h"}

    loop = HeartbeatLoop(Gateway(), 0.01)
    loop.start()
    try:
        assert started.wait(1.0)
        # The first request is still in flight; a second call must not be
        # started on every short interval and create an unbounded client pile.
        threading.Event().wait(0.25)
        assert len(calls) == 1
        assert "timed out" in loop.last_error or "in flight" in loop.last_error
    finally:
        release.set()
        loop.stop()


class ConfirmingGateway:
    def __init__(self):
        self.cancelled = False
        self.submits = 0

    def submit_limit_buy(self, token_id, price, qty, metadata, order_type="GTC"):
        self.submits += 1
        return {"orderID": "order-1", "status": "live"}

    def get_order(self, order_id):
        if self.cancelled:
            return {"id": order_id, "status": "canceled", "size_matched": "2", "price": "0.51"}
        return {"id": order_id, "status": "live", "size_matched": "0"}

    def cancel_order(self, order_id):
        self.cancelled = True
        return {"canceled": [order_id]}

    def order_trades(self, token_id, order_id):
        if self.cancelled:
            return [{"order_id": order_id, "size": "2", "price": "0.51"}]
        return []

    def post_heartbeat(self, heartbeat_id=""):
        return {"heartbeat_id": "h1"}




class PendingGtcGateway(ConfirmingGateway):
    def get_order(self, order_id):
        return {"id": order_id, "status": "live", "size_matched": "0"}

    def cancel_order(self, order_id):
        raise AssertionError("GTC must not be locally cancelled")

    def order_trades(self, token_id, order_id):
        return []


class SubmittedThenMatchedGateway(ConfirmingGateway):
    def __init__(self):
        super().__init__()
        self.lookups = 0

    def get_order(self, order_id):
        self.lookups += 1
        if self.lookups <= 2:
            return {"id": order_id, "status": "live", "size_matched": "0"}
        return {"id": order_id, "status": "matched", "size_matched": "5", "average_price": "0.51"}

    def cancel_order(self, order_id):
        raise AssertionError("GTC must not be locally cancelled")

    def order_trades(self, token_id, order_id):
        if self.lookups <= 2:
            return []
        return [{"order_id": order_id, "size": "5", "price": "0.51"}]

class TimeoutGateway(ConfirmingGateway):
    def submit_limit_buy(self, token_id, price, qty, metadata, order_type="GTC"):
        raise TimeoutError("request timed out after send")


class FastGateway(ConfirmingGateway):
    def __init__(self):
        super().__init__()
        self.fast_submits = 0

    def submit_limit_buy_fast(self, token_id, price, qty, metadata, order_type="FAK"):
        self.fast_submits += 1
        return {"orderID": "order-1", "status": "matched"}

    def get_order(self, order_id):
        return {
            "id": order_id,
            "status": "matched",
            "size_matched": "5",
            "average_price": "0.51",
        }

    def order_trades(self, token_id, order_id):
        return [{"order_id": order_id, "size": "5", "price": "0.51"}]






class FakNoMatchError(Exception):
    status_code = 400
    error_msg = {"error": "no orders found to match with FAK order. FAK orders are partially filled or killed if no match is found."}


class FakNoMatchGateway(ConfirmingGateway):
    def submit_limit_buy_fast(self, token_id, price, qty, metadata, order_type="FAK"):
        raise FakNoMatchError("request error")


class PolyStyleError(Exception):
    status_code = 400
    error_msg = {"error": "invalid order type FAK for this request"}


class DetailedSubmitExceptionGateway(ConfirmingGateway):
    def submit_limit_buy_fast(self, token_id, price, qty, metadata, order_type="FAK"):
        raise PolyStyleError("should prefer structured error_msg")


class RetryingCancelGateway(ConfirmingGateway):
    def __init__(self):
        super().__init__()
        self.cancel_attempts = 0

    def cancel_order(self, order_id):
        self.cancel_attempts += 1
        if self.cancel_attempts == 1:
            raise RuntimeError("425 matching engine restarting")
        return super().cancel_order(order_id)

    def get_order(self, order_id):
        if not self.cancelled:
            return {"id": order_id, "status": "unmatched", "size_matched": "0"}
        return super().get_order(order_id)


class MatchedAckLookupFailure(ConfirmingGateway):
    def submit_limit_buy(self, token_id, price, qty, metadata, order_type="GTC"):
        return {"orderID": "order-1", "status": "matched"}

    def get_order(self, order_id):
        raise TimeoutError("temporary authenticated lookup failure")

    def order_trades(self, token_id, order_id):
        return []


class FillWithoutPriceGateway(ConfirmingGateway):
    def submit_limit_buy(self, token_id, price, qty, metadata, order_type="GTC"):
        return {"orderID": "order-1", "status": "matched"}

    def get_order(self, order_id):
        return {"id": order_id, "status": "matched", "size_matched": "2"}

    def order_trades(self, token_id, order_id):
        return []


class PartialTradeCoverageGateway(FillWithoutPriceGateway):
    def get_order(self, order_id):
        return {"id": order_id, "status": "matched", "size_matched": "5"}

    def order_trades(self, token_id, order_id):
        return [{"order_id": order_id, "size": "2", "price": "0.51"}]


class WrongRecoveredOrderGateway(ConfirmingGateway):
    def get_order(self, order_id):
        return {
            "id": order_id,
            "status": "matched",
            "asset_id": "wrong-token",
            "side": "BUY",
            "original_size": "5",
            "price": "0.51",
            "size_matched": "5",
            "average_price": "0.51",
        }


class MatchingRecoveredOrderGateway(WrongRecoveredOrderGateway):
    def get_order(self, order_id):
        return {
            "id": order_id,
            "status": "matched",
            "asset_id": "token",
            "side": "BUY",
            "original_size": "5",
            "price": "0.51",
            "size_matched": "5",
            "average_price": "0.51",
        }


class PrefixedTerminalStatusGateway(ConfirmingGateway):
    def get_order(self, order_id):
        return {
            "id": order_id,
            "status": "ORDER_STATUS_CANCELED",
            "size_matched": "2",
            "average_price": "0.51",
        }

    def order_trades(self, token_id, order_id):
        return [{"order_id": order_id, "size": "2", "price": "0.51"}]


def _metadata():
    return MarketMetadata(
        condition_id="condition",
        tick_size="0.01",
        min_order_size=1.0,
        neg_risk=False,
        fee_rate=0.07,
        tokens={"up": "token"},
        raw={},
    )


def _reserve(store):
    return store.reserve_entry(
        slug="btc-updown-5m-0",
        condition_id="condition",
        round_start=0,
        token_id="token",
        side="YES",
        requested_qty=5,
        requested_price=0.51,
    )


def test_dry_run_reservation_records_a_shadow_simulated_take_without_real_order(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        record = _reserve(store)
        result = OrderExecutor(Settings(dry_run=True), store).execute_reserved(record, _metadata())

        assert result.dry_run is True
        assert result.status == "shadow_fill"
        assert result.submission_state == "not_submitted"
        assert result.raw is not None
        assert result.raw["no_live_order"] is True
        assert result.raw["simulated_take"] is True
        assert result.filled_qty == 5
        assert result.avg_price == 0.51
        assert store.total_open_exposure() == 2.55
    finally:
        store.close()


def test_gtc_submit_is_left_pending_without_local_cancel(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        record = _reserve(store)
        clock = FakeClock()
        gateway = PendingGtcGateway()
        settings = Settings(dry_run=False, order_type="GTC", order_ttl_s=1, reconcile_timeout_s=3, heartbeat_interval_s=1)
        executor = OrderExecutor(
            settings,
            store,
            gateway=gateway,
            sleep=clock.sleep,
            monotonic=clock,
            heartbeat_factory=NoopHeartbeat,
        )

        result = executor.execute_reserved(record, _metadata())

        assert gateway.submits == 1
        assert gateway.cancelled is False
        assert result.terminal is False
        assert result.status == "submitted_pending"
        assert result.submission_state == "awaiting_settlement"
        assert result.error == "gtc_awaiting_settlement"
        assert store.market_state(record.slug) == "submitted"
        assert store.total_risk_exposure() == record.requested_notional
        assert store.has_execution_unknown() is False
    finally:
        store.close()


def test_gtc_pending_order_can_reconcile_later_to_confirmed_fill(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        record = _reserve(store)
        first_clock = FakeClock()
        first_gateway = PendingGtcGateway()
        settings = Settings(dry_run=False, order_type="GTC", order_ttl_s=1, reconcile_timeout_s=1, heartbeat_interval_s=1)
        first = OrderExecutor(
            settings,
            store,
            gateway=first_gateway,
            sleep=first_clock.sleep,
            monotonic=first_clock,
            heartbeat_factory=NoopHeartbeat,
        ).execute_reserved(record, _metadata())
        assert first.status == "submitted_pending"

        pending = store.unresolved_orders()[0]
        second_clock = FakeClock()
        result = OrderExecutor(
            settings,
            store,
            gateway=SubmittedThenMatchedGateway(),
            sleep=second_clock.sleep,
            monotonic=second_clock,
            heartbeat_factory=NoopHeartbeat,
        ).reconcile_existing(pending)

        assert result.terminal is True
        assert result.status == "matched"
        assert result.filled_qty == 5
        assert result.avg_price == 0.51
        assert store.market_state(record.slug) == "open"
        assert store.total_open_exposure() == 2.55
    finally:
        store.close()


def test_submit_timeout_skips_only_current_market_without_global_freeze(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        record = _reserve(store)
        clock = FakeClock()
        settings = Settings(dry_run=False, order_type="GTC", order_ttl_s=1, reconcile_timeout_s=3, heartbeat_interval_s=1)
        result = OrderExecutor(
            settings,
            store,
            gateway=TimeoutGateway(),
            sleep=clock.sleep,
            monotonic=clock,
            heartbeat_factory=NoopHeartbeat,
        ).execute_reserved(record, _metadata())

        assert result.terminal is True
        assert result.status == "submit_skipped"
        assert result.submission_state == "skipped"
        assert result.error == "submit_exception"
        assert result.raw["terminal_skip"] is True
        assert store.has_execution_unknown() is False
        assert _reserve(store) is None  # same slug still cannot be retried
        assert store.reserve_entry("eth-updown-5m-300", "condition", 300, "token", "YES", 5, 0.51) is not None
    finally:
        store.close()


def test_gtd_unmatched_order_is_cancelled_and_cancel_425_is_retried(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        record = _reserve(store)
        clock = FakeClock()
        gateway = RetryingCancelGateway()
        settings = Settings(
            dry_run=False, order_type="GTD", order_ttl_s=0.5, reconcile_timeout_s=3, heartbeat_interval_s=1
        )
        result = OrderExecutor(
            settings,
            store,
            gateway=gateway,
            sleep=clock.sleep,
            monotonic=clock,
            heartbeat_factory=NoopHeartbeat,
        ).execute_reserved(record, _metadata())

        assert gateway.cancel_attempts == 2
        assert result.terminal is True
        assert result.status == "canceled"
    finally:
        store.close()


def test_fast_execution_prefers_single_attempt_fast_gateway_method(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        record = _reserve(store)
        gateway = FastGateway()
        executor = OrderExecutor(
            Settings(dry_run=False, order_type="GTC", order_ttl_s=1, reconcile_timeout_s=3, heartbeat_interval_s=1),
            store,
            gateway=gateway,
            heartbeat_factory=NoopHeartbeat,
        )

        result = executor.execute_reserved(record, _metadata(), fast=True)

        assert gateway.fast_submits == 1
        assert gateway.submits == 0
        assert result.terminal is True
        assert result.filled_qty == 5
    finally:
        store.close()


def test_documented_prefixed_terminal_status_is_reconciled(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        record = _reserve(store)
        clock = FakeClock()
        result = OrderExecutor(
            Settings(dry_run=False, order_type="GTC", order_ttl_s=0.5, reconcile_timeout_s=2),
            store,
            gateway=PrefixedTerminalStatusGateway(),
            sleep=clock.sleep,
            monotonic=clock,
            heartbeat_factory=NoopHeartbeat,
        ).execute_reserved(record, _metadata())

        assert result.terminal is True
        assert result.status == "canceled"
        assert result.filled_qty == 2
    finally:
        store.close()


def test_submit_ack_status_is_not_used_as_fill_when_lookup_fails(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        record = _reserve(store)
        clock = FakeClock()
        result = OrderExecutor(
            Settings(dry_run=False, order_type="GTC", order_ttl_s=0.5, reconcile_timeout_s=2),
            store,
            gateway=MatchedAckLookupFailure(),
            sleep=clock.sleep,
            monotonic=clock,
            heartbeat_factory=NoopHeartbeat,
        ).execute_reserved(record, _metadata())

        assert result.status == "submitted_pending"
        assert result.submission_state == "awaiting_settlement"
        assert store.has_execution_unknown() is False
        assert store.total_open_exposure() == 0
        assert store.total_risk_exposure() == record.requested_notional
    finally:
        store.close()


def test_confirmed_fill_without_execution_price_remains_unknown(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        record = _reserve(store)
        clock = FakeClock()
        result = OrderExecutor(
            Settings(dry_run=False, order_type="GTC", order_ttl_s=0.5, reconcile_timeout_s=2),
            store,
            gateway=FillWithoutPriceGateway(),
            sleep=clock.sleep,
            monotonic=clock,
            heartbeat_factory=NoopHeartbeat,
        ).execute_reserved(record, _metadata())

        assert result.status == "execution_unknown"
        assert result.filled_qty == 2
        assert store.has_execution_unknown() is True
    finally:
        store.close()


def test_partial_trade_page_cannot_price_the_full_matched_quantity(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        record = _reserve(store)
        clock = FakeClock()
        result = OrderExecutor(
            Settings(dry_run=False, order_type="GTC", order_ttl_s=0.5, reconcile_timeout_s=2),
            store,
            gateway=PartialTradeCoverageGateway(),
            sleep=clock.sleep,
            monotonic=clock,
            heartbeat_factory=NoopHeartbeat,
        ).execute_reserved(record, _metadata())

        assert result.status == "execution_unknown"
        assert result.filled_qty == 5
        assert result.avg_price == 0
    finally:
        store.close()


def test_recovered_order_identity_mismatch_stays_unknown(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        record = _reserve(store)
        store.mark_execution_unknown(record.intent_id, "missing_ack")
        store.attach_recovered_order_id(record.intent_id, "order-recovered")
        recovered = store.unresolved_orders()[0]
        clock = FakeClock()

        result = OrderExecutor(
            Settings(dry_run=False, order_type="GTC", order_ttl_s=0.5, reconcile_timeout_s=2),
            store,
            gateway=WrongRecoveredOrderGateway(),
            sleep=clock.sleep,
            monotonic=clock,
            heartbeat_factory=NoopHeartbeat,
        ).reconcile_existing(recovered)

        assert result.status == "execution_unknown"
        assert "token" in result.error
        assert store.has_execution_unknown() is True
    finally:
        store.close()


def test_matching_recovered_order_identity_can_reconcile(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        record = _reserve(store)
        store.mark_execution_unknown(record.intent_id, "missing_ack")
        store.attach_recovered_order_id(record.intent_id, "order-recovered")
        recovered = store.unresolved_orders()[0]
        clock = FakeClock()

        result = OrderExecutor(
            Settings(dry_run=False, order_type="GTC", order_ttl_s=0.5, reconcile_timeout_s=2),
            store,
            gateway=MatchingRecoveredOrderGateway(),
            sleep=clock.sleep,
            monotonic=clock,
            heartbeat_factory=NoopHeartbeat,
        ).reconcile_existing(recovered)

        assert result.terminal is True
        assert result.filled_qty == 5
        assert store.market_state(record.slug) == "open"
    finally:
        store.close()


def test_fak_order_does_not_send_local_cancel(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        record = _reserve(store)
        clock = FakeClock()
        gateway = ConfirmingGateway()
        result = OrderExecutor(
            Settings(dry_run=False, order_type="FAK", reconcile_timeout_s=2),
            store,
            gateway=gateway,
            sleep=clock.sleep,
            monotonic=clock,
            heartbeat_factory=NoopHeartbeat,
        ).execute_reserved(record, _metadata())

        assert gateway.submits == 1
        assert gateway.cancelled is False
        assert result.status == "execution_unknown"
        assert result.submission_state == "unknown"
        assert store.has_execution_unknown() is True
    finally:
        store.close()


def test_acknowledged_gtc_order_emits_submitted_event_and_stays_pending(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        record = _reserve(store)
        clock = FakeClock()
        events = []
        emitted = threading.Event()

        def capture(kind, payload):
            events.append((kind, payload))
            emitted.set()

        executor = OrderExecutor(
            Settings(dry_run=False, order_type="GTC", order_ttl_s=1, reconcile_timeout_s=3),
            store,
            gateway=ConfirmingGateway(),
            sleep=clock.sleep,
            monotonic=clock,
            heartbeat_factory=NoopHeartbeat,
            event_callback=capture,
        )

        result = executor.execute_reserved(record, _metadata())

        assert result.terminal is False
        assert result.status == "submitted_pending"
        assert result.submission_state == "awaiting_settlement"
        assert emitted.wait(1)
        assert len(events) == 1
        kind, payload = events[0]
        assert kind == "submitted"
        assert payload["slug"] == record.slug
        assert payload["side"] == "YES"
        assert payload["order_id"] == "order-1"
        assert payload["requested_qty"] == 5.0
        assert payload["requested_price"] == 0.51
        assert payload["submit_roundtrip_ms"] >= 0
        assert "decision_to_submit_ms" in payload
        assert "observed_book_age_ms" in payload
    finally:
        store.close()


def test_slow_submission_notification_does_not_delay_gtc_pending_reconciliation(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    release_notification = threading.Event()
    notification_started = threading.Event()
    try:
        record = _reserve(store)
        clock = FakeClock()
        gateway = ConfirmingGateway()

        def slow_callback(kind, payload):
            notification_started.set()
            release_notification.wait(2)

        executor = OrderExecutor(
            Settings(dry_run=False, order_type="GTC", order_ttl_s=1, reconcile_timeout_s=3),
            store,
            gateway=gateway,
            sleep=clock.sleep,
            monotonic=clock,
            heartbeat_factory=NoopHeartbeat,
            event_callback=slow_callback,
        )

        result = executor.execute_reserved(record, _metadata())

        assert notification_started.wait(1)
        assert release_notification.is_set() is False
        assert gateway.cancelled is False
        assert result.terminal is False
        assert result.status == "submitted_pending"
        assert clock.now <= 3.1
        executor.wait_for_event_delivery(timeout_s=0)
        audit = store._conn.execute(
            """SELECT kind, payload_json FROM audit_events
               WHERE kind = 'notification_failed' ORDER BY id DESC LIMIT 1"""
        ).fetchone()
        assert audit["kind"] == "notification_failed"
        assert "delivery_timeout" in audit["payload_json"]
        release_notification.set()
        executor.wait_for_event_delivery()
    finally:
        release_notification.set()
        store.close()


def test_submit_exception_persists_sanitized_diagnostics(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        record = _reserve(store)
        result = OrderExecutor(
            Settings(dry_run=False, order_type="FAK", reconcile_timeout_s=2),
            store,
            gateway=DetailedSubmitExceptionGateway(),
            heartbeat_factory=NoopHeartbeat,
        ).execute_reserved(record, _metadata(), fast=True)

        assert result.terminal is True
        assert result.status == "submit_skipped"
        assert result.submission_state == "skipped"
        assert result.error == "submit_exception"
        assert result.raw["error_type"] == "PolyStyleError"
        assert result.raw["status_code"] == 400
        assert result.raw["order_type"] == "FAK"
        assert result.raw["error_hint"] == "order_type_compatibility"
        assert result.raw["terminal_skip"] is True
        assert "invalid order type FAK" in result.raw["error_message"]
        assert store.has_execution_unknown() is False
        assert store.reserve_entry("eth-updown-5m-300", "condition", 300, "token", "YES", 5, 0.51) is not None
    finally:
        store.close()


def test_execution_result_persists_decision_submit_timing_context(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        record = _reserve(store)
        clock = FakeClock()
        wall = FakeClock()
        wall.now = 1000.0
        gateway = FastGateway()
        result = OrderExecutor(
            Settings(dry_run=False, order_type="GTC", order_ttl_s=1, reconcile_timeout_s=1),
            store,
            gateway=gateway,
            sleep=clock.sleep,
            monotonic=clock,
            wall_clock=wall,
            heartbeat_factory=NoopHeartbeat,
        ).execute_reserved(
            record,
            _metadata(),
            fast=True,
            timing_context={
                "decision_ts": 999.9,
                "book_observed_ts": 999.8,
                "round_end_ts": 999.7,
                "seconds_after_close_at_decision": 0.2,
            },
        )

        assert result.terminal is True
        assert result.decision_to_submit_ms == pytest.approx(100.0)
        assert result.observed_book_age_ms == pytest.approx(200.0)
        assert result.submit_roundtrip_ms >= 0.0
        assert result.reconcile_duration_ms >= 0.0
        assert result.raw["timing"]["decision_ts"] == 999.9
        assert result.raw["timing"]["book_observed_ts"] == 999.8
    finally:
        store.close()


def test_live_execution_result_preserves_official_itode_delay_metadata(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        record = _reserve(store)
        metadata = MarketMetadata(
            condition_id="condition",
            tick_size="0.01",
            min_order_size=1.0,
            neg_risk=False,
            fee_rate=0.0,
            tokens={"up": "token"},
            raw={"itode": True},
            immediate_taker_order_delay_enabled=True,
            expected_taker_delay_ms=250.0,
        )
        result = OrderExecutor(
            Settings(dry_run=False),
            store,
            gateway=FastGateway(),
            heartbeat_factory=NoopHeartbeat,
            sleep=lambda _: None,
        ).execute_reserved(record, metadata, fast=True)

        assert result.immediate_taker_order_delay_enabled is True
        assert result.expected_taker_delay_ms == 250.0
        assert result.raw["submit"]["_market_delay"] == {
            "immediate_taker_order_delay_enabled": True,
            "expected_taker_delay_ms": 250.0,
        }
    finally:
        store.close()


def test_fak_no_match_error_is_terminal_no_fill_not_execution_unknown(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        record = _reserve(store)
        result = OrderExecutor(
            Settings(dry_run=False, order_type="FAK", reconcile_timeout_s=2),
            store,
            gateway=FakNoMatchGateway(),
            heartbeat_factory=NoopHeartbeat,
        ).execute_reserved(record, _metadata(), fast=True)

        assert result.terminal is True
        assert result.status == "no_fill"
        assert result.submission_state == "venue_no_match"
        assert result.filled_qty == 0
        assert result.error == "fak_no_matching_resting_order"
        assert result.raw["terminal_no_fill"] is True
        assert store.has_execution_unknown() is False
        assert _reserve(store) is None
        assert store.reserve_entry("eth-updown-5m-300", "condition", 300, "token", "YES", 5, 0.51) is not None
    finally:
        store.close()
