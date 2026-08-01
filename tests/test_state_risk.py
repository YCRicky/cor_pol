import sqlite3
import time

import pytest

from aftertake.config import Settings
from aftertake.risk import RiskRejected, check_entry_risk
from aftertake.state import RuntimeLock, StateStore


def _reserve(store, slug="btc-updown-5m-0"):
    return store.reserve_entry(
        slug=slug,
        condition_id="condition-" + slug,
        round_start=0,
        token_id="token",
        side="YES",
        requested_qty=5,
        requested_price=0.5,
    )


def _settle_loss(store, slug):
    record = _reserve(store, slug)
    store.mark_terminal_execution(
        record.intent_id, filled_qty=5, avg_price=0.5, raw={}, reason="matched"
    )
    store.record_settlement(slug, -5.0, {"win": False})


def test_sqlite_reservation_is_one_entry_per_market(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        first = _reserve(store)
        second = _reserve(store)

        assert first is not None
        assert second is None
    finally:
        store.close()


def test_unknown_execution_does_not_globally_freeze_new_entries(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        record = _reserve(store)
        store.mark_execution_unknown(record.intent_id, "timeout")

        snapshot = check_entry_risk(
            settings=Settings(),
            store=store,
            slug="eth-updown-5m-300",
            price=0.5,
            qty=5,
            displayed_ask_size=10,
        )

        assert snapshot.requested_notional == 2.5
    finally:
        store.close()


def test_displayed_depth_and_per_asset_open_position_limit_are_enforced(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        with pytest.raises(RiskRejected, match="depth"):
            check_entry_risk(
                settings=Settings(),
                store=store,
                slug="market",
                price=0.5,
                qty=5,
                displayed_ask_size=4,
            )

        record = _reserve(store, "btc-updown-5m-0")
        store.mark_terminal_execution(
            record.intent_id, filled_qty=5, avg_price=0.5, raw={}, reason="matched"
        )
        with pytest.raises(RiskRejected, match="max_open_positions"):
            check_entry_risk(
                settings=Settings(max_open_positions=1),
                store=store,
                slug="btc-updown-5m-300",
                price=0.5,
                qty=5,
                displayed_ask_size=10,
                now_ts=time.time() + 120,
            )
    finally:
        store.close()


def test_original_daily_loss_and_loss_streak_limits_are_enforced(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        _settle_loss(store, "loss-0")
        _settle_loss(store, "loss-1")
        _settle_loss(store, "loss-2")
        _settle_loss(store, "loss-3")
        _settle_loss(store, "loss-4")

        with pytest.raises(RiskRejected, match="consecutive_loss_limit"):
            check_entry_risk(
                settings=Settings(max_daily_loss=100),
                store=store,
                slug="next",
                price=0.5,
                qty=5,
                displayed_ask_size=10,
                now_ts=time.time() + 120,
            )
        with pytest.raises(RiskRejected, match="daily_loss_limit"):
            check_entry_risk(
                settings=Settings(max_consecutive_losses=10, max_daily_loss=25),
                store=store,
                slug="next",
                price=0.5,
                qty=5,
                displayed_ask_size=10,
                now_ts=time.time() + 120,
            )
    finally:
        store.close()


def test_per_asset_entry_cooldown_is_enforced(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        _reserve(store, "btc-updown-5m-0")
        with pytest.raises(RiskRejected, match="entry_cooldown"):
            check_entry_risk(
                settings=Settings(),
                store=store,
                slug="btc-updown-5m-300",
                price=0.5,
                qty=5,
                displayed_ask_size=10,
                now_ts=time.time(),
            )
    finally:
        store.close()


def test_runtime_lock_rejects_second_process_owner(tmp_path):
    path = tmp_path / "runtime.lock"
    with RuntimeLock(path):
        with pytest.raises(RuntimeError, match="another"):
            with RuntimeLock(path):
                pass


def test_unknown_order_can_recover_after_operator_attaches_order_id(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        record = _reserve(store)
        store.mark_execution_unknown(record.intent_id, "missing_ack")
        store.attach_recovered_order_id(record.intent_id, "clob-order-1")
        recovered = store.unresolved_orders()[0]

        assert recovered.order_id == "clob-order-1"
        assert recovered.state == "submitted"
        store.mark_terminal_execution(
            recovered.intent_id,
            filled_qty=2,
            avg_price=0.5,
            raw={"order": {"status": "matched"}},
            reason="matched",
        )
        assert store.market_state(record.slug) == "open"
    finally:
        store.close()


def test_component_failure_and_recovery_transition_survives_process_reopen(tmp_path):
    path = tmp_path / "state.sqlite3"
    first = StateStore(path)
    try:
        assert first.mark_component_unhealthy("pm_runtime", "timeout") is True
        assert first.mark_component_unhealthy("pm_runtime", "timeout again") is False
        assert first.component_status("pm_runtime") == "unhealthy"
    finally:
        first.close()

    second = StateStore(path)
    try:
        assert second.component_status("pm_runtime") == "unhealthy"
        assert second.mark_component_healthy("pm_runtime", "preflight passed") is True
        assert second.mark_component_healthy("pm_runtime", "still healthy") is False
        assert second.component_status("pm_runtime") == "healthy"
    finally:
        second.close()


def test_component_transition_and_notification_are_one_transaction(tmp_path):
    path = tmp_path / "state.sqlite3"
    first = StateStore(path)
    transitioned, notification_id = first.transition_component_and_enqueue_notification(
        component="pm_runtime",
        status="unhealthy",
        detail="timeout",
        kind="alert",
        message="failed",
        enqueue_on_no_transition=True,
    )
    assert transitioned is True
    assert notification_id
    first.close()

    second = StateStore(path)
    try:
        transitioned, recovery_id = second.transition_component_and_enqueue_notification(
            component="pm_runtime",
            status="healthy",
            detail="preflight passed",
            kind="recovery_success",
            message="recovered",
        )
        assert transitioned is True
        assert recovery_id
        assert second.component_status("pm_runtime") == "healthy"
        pending = second.pending_notifications(ready_at=time.time())
        assert [row["notification_id"] for row in pending] == [notification_id]
        assert second.notification(recovery_id)["message"] == "recovered"
    finally:
        second.close()


def test_notification_predecessor_blocks_recovery_until_alert_is_sent(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        _, alert_id = store.transition_component_and_enqueue_notification(
            component="clob_heartbeat",
            status="unhealthy",
            detail="timeout",
            kind="alert",
            message="alert",
            enqueue_on_no_transition=True,
        )
        _, recovery_id = store.transition_component_and_enqueue_notification(
            component="clob_heartbeat",
            status="healthy",
            detail="restored",
            kind="recovery_success",
            message="recovery",
        )
        assert [row["notification_id"] for row in store.pending_notifications(ready_at=time.time())] == [alert_id]
        assert store.deliverable_notification(recovery_id, ready_at=time.time()) is None
        store.mark_notification_sent(alert_id)
        assert store.deliverable_notification(recovery_id, ready_at=time.time())["message"] == "recovery"
    finally:
        store.close()


def test_legacy_notification_outbox_schema_migrates_component_column(tmp_path):
    path = tmp_path / "legacy.sqlite3"
    conn = sqlite3.connect(path)
    conn.execute(
        """CREATE TABLE notification_outbox (
           notification_id TEXT PRIMARY KEY, kind TEXT NOT NULL,
           slug TEXT NOT NULL DEFAULT '', message TEXT NOT NULL,
           status TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0,
           next_attempt_at REAL NOT NULL DEFAULT 0, last_error TEXT NOT NULL DEFAULT '',
           created_at TEXT NOT NULL, updated_at TEXT NOT NULL)"""
    )
    conn.commit()
    conn.close()
    store = StateStore(path)
    try:
        columns = {
            row["name"]
            for row in store._conn.execute("PRAGMA table_info(notification_outbox)").fetchall()
        }
        assert "component" in columns
    finally:
        store.close()
