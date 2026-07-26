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


def test_unknown_execution_freezes_risk_before_external_submit(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        record = _reserve(store)
        store.mark_execution_unknown(record.intent_id, "timeout")

        with pytest.raises(RiskRejected, match="unknown"):
            check_entry_risk(
                settings=Settings(),
                store=store,
                slug="new-market",
                price=0.5,
                qty=5,
                displayed_ask_size=10,
            )
    finally:
        store.close()


def test_displayed_depth_and_original_open_position_limit_are_enforced(tmp_path):
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

        record = _reserve(store, "first")
        store.mark_terminal_execution(
            record.intent_id, filled_qty=5, avg_price=0.5, raw={}, reason="matched"
        )
        with pytest.raises(RiskRejected, match="max_open_positions"):
            check_entry_risk(
                settings=Settings(),
                store=store,
                slug="second",
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


def test_original_entry_cooldown_is_enforced(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        _reserve(store, "first")
        with pytest.raises(RiskRejected, match="entry_cooldown"):
            check_entry_risk(
                settings=Settings(),
                store=store,
                slug="second",
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
