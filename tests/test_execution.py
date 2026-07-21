import threading

from misprice_pm.config import Settings
from misprice_pm.execution import OrderExecutor
from misprice_pm.pm_client import MarketMetadata
from misprice_pm.state import StateStore


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


class ConfirmingGateway:
    def __init__(self):
        self.cancelled = False
        self.submits = 0

    def submit_limit_buy(self, token_id, price, qty, metadata):
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


class TimeoutGateway(ConfirmingGateway):
    def submit_limit_buy(self, token_id, price, qty, metadata):
        raise TimeoutError("request timed out after send")


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
    def submit_limit_buy(self, token_id, price, qty, metadata):
        return {"orderID": "order-1", "status": "matched"}

    def get_order(self, order_id):
        raise TimeoutError("temporary authenticated lookup failure")

    def order_trades(self, token_id, order_id):
        return []


class FillWithoutPriceGateway(ConfirmingGateway):
    def submit_limit_buy(self, token_id, price, qty, metadata):
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


def test_dry_run_reservation_never_creates_a_real_fill(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        record = _reserve(store)
        result = OrderExecutor(Settings(dry_run=True), store).execute_reserved(record, _metadata())

        assert result.dry_run is True
        assert result.status == "shadow_no_order"
        assert result.filled_qty == 0
        assert store.total_open_exposure() == 0
    finally:
        store.close()


def test_live_executor_cancels_remainder_and_records_confirmed_partial_fill(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        record = _reserve(store)
        clock = FakeClock()
        gateway = ConfirmingGateway()
        settings = Settings(dry_run=False, order_ttl_s=1, reconcile_timeout_s=3, heartbeat_interval_s=1)
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
        assert gateway.cancelled is True
        assert result.terminal is True
        assert result.filled_qty == 2
        assert result.avg_price == 0.51
        assert store.total_open_exposure() == 1.02
    finally:
        store.close()


def test_submit_timeout_is_unknown_and_blocks_future_entries(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        record = _reserve(store)
        clock = FakeClock()
        settings = Settings(dry_run=False, order_ttl_s=1, reconcile_timeout_s=3, heartbeat_interval_s=1)
        result = OrderExecutor(
            settings,
            store,
            gateway=TimeoutGateway(),
            sleep=clock.sleep,
            monotonic=clock,
            heartbeat_factory=NoopHeartbeat,
        ).execute_reserved(record, _metadata())

        assert result.submission_state == "unknown"
        assert store.has_execution_unknown() is True
        assert _reserve(store) is None
    finally:
        store.close()


def test_unmatched_order_is_cancelled_and_cancel_425_is_retried(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        record = _reserve(store)
        clock = FakeClock()
        gateway = RetryingCancelGateway()
        settings = Settings(
            dry_run=False, order_ttl_s=0.5, reconcile_timeout_s=3, heartbeat_interval_s=1
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


def test_documented_prefixed_terminal_status_is_reconciled(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        record = _reserve(store)
        clock = FakeClock()
        result = OrderExecutor(
            Settings(dry_run=False, order_ttl_s=0.5, reconcile_timeout_s=2),
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
            Settings(dry_run=False, order_ttl_s=0.5, reconcile_timeout_s=2),
            store,
            gateway=MatchedAckLookupFailure(),
            sleep=clock.sleep,
            monotonic=clock,
            heartbeat_factory=NoopHeartbeat,
        ).execute_reserved(record, _metadata())

        assert result.status == "execution_unknown"
        assert store.has_execution_unknown() is True
        assert store.total_open_exposure() == 0
    finally:
        store.close()


def test_confirmed_fill_without_execution_price_remains_unknown(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        record = _reserve(store)
        clock = FakeClock()
        result = OrderExecutor(
            Settings(dry_run=False, order_ttl_s=0.5, reconcile_timeout_s=2),
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
            Settings(dry_run=False, order_ttl_s=0.5, reconcile_timeout_s=2),
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
            Settings(dry_run=False, order_ttl_s=0.5, reconcile_timeout_s=2),
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
            Settings(dry_run=False, order_ttl_s=0.5, reconcile_timeout_s=2),
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


def test_acknowledged_order_emits_submitted_event_before_reconciliation(tmp_path):
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
            Settings(dry_run=False, order_ttl_s=1, reconcile_timeout_s=3),
            store,
            gateway=ConfirmingGateway(),
            sleep=clock.sleep,
            monotonic=clock,
            heartbeat_factory=NoopHeartbeat,
            event_callback=capture,
        )

        result = executor.execute_reserved(record, _metadata())

        assert result.terminal is True
        assert emitted.wait(1)
        assert events == [
            (
                "submitted",
                {
                    "slug": record.slug,
                    "side": "YES",
                    "order_id": "order-1",
                    "requested_qty": 5.0,
                    "requested_price": 0.51,
                },
            )
        ]
    finally:
        store.close()


def test_slow_submission_notification_does_not_delay_cancel_reconciliation(tmp_path):
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
            Settings(dry_run=False, order_ttl_s=1, reconcile_timeout_s=3),
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
        assert gateway.cancelled is True
        assert result.terminal is True
        assert clock.now <= 3
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
