import json
import threading
from pathlib import Path

import aftertake.runner as runner_module
from aftertake.config import Settings
from aftertake.execution import OrderExecutor
from aftertake.notifier import Notifier
from aftertake.pm_client import (
    BalanceAllowance,
    GammaMarket,
    GeoStatus,
    LivePreflight,
    MarketMetadata,
)
from aftertake.post_close import PairedBook, SideBook
from aftertake.risk import RiskRejected, check_entry_risk
from aftertake.runner import (
    _probe_stream,
    _reconcile_startup,
    _run_asset_rounds,
    _run_round_loop,
    _select_next_round_start,
    current_crypto_5m_slug,
    deployment_check,
    reconcile_submitted_orders,
    run_round,
    settle_open_positions,
)
from aftertake.state import StateStore


class FakePublic:
    def market_by_slug(self, slug, allow_closed=False):
        return GammaMarket(
            slug=slug,
            condition_id="condition",
            outcomes=("Up", "Down"),
            clob_token_ids=("up-token", "down-token"),
            active=True,
        )


class ResolvedPublic(FakePublic):
    def market_by_slug(self, slug, allow_closed=False):
        return GammaMarket(
            slug=slug,
            condition_id="condition",
            outcomes=("Up", "Down"),
            outcome_prices=(1.0, 0.0),
            clob_token_ids=("up-token", "down-token"),
            closed=True,
            active=False,
        )


class LivePublic(FakePublic):
    def geoblock_status(self, endpoint):
        return GeoStatus(blocked=False, country="KR", region="", ip="")


class CaptureNotifier:
    enabled = True

    def __init__(self):
        self.messages = []

    def send(self, text):
        self.messages.append(text)
        return True


class PairedStream:
    no_bid_size = 18
    no_near_depth = 18
    yes_ask_size = 20
    no_ask_size = 20

    def __init__(self, *, on_book, **_kwargs):
        self._on_book = on_book
        self.ready = False
        self.last_error = ""

    def start(self):
        def add(ts, yes_bid, yes_ask, no_bid, no_ask, *, yes_size=20, no_size=20, yes_near=None, no_near=None):
            self._on_book(
                PairedBook(
                    observed_at=ts,
                    yes=SideBook(yes_bid, yes_size, yes_size, yes_ask, self.yes_ask_size, yes_size if yes_near is None else yes_near),
                    no=SideBook(no_bid, no_size, no_size, no_ask, self.no_ask_size, no_size if no_near is None else no_near),
                )
            )

        add(1199.70, 0.47, 0.50, 0.51, 0.54, yes_near=18, no_near=18)
        add(1199.82, 0.48, 0.51, 0.50, 0.53, yes_near=18, no_near=18)
        add(1199.95, 0.49, 0.64, 0.50, 0.53, yes_near=18, no_near=18)
        add(1200.10, 0.35, 0.37, 0.58, 0.64, yes_size=2, no_size=self.no_bid_size, yes_near=2, no_near=self.no_near_depth)
        add(1200.22, 0.30, 0.37, 0.60, 0.64, yes_size=2, no_size=self.no_bid_size, yes_near=2, no_near=self.no_near_depth)
        add(1200.35, 0.28, 0.37, 0.61, 0.64, yes_size=2, no_size=self.no_bid_size, yes_near=2, no_near=self.no_near_depth)
        self.ready = True

    def close(self):
        return None


class ReadyEmptyStream:
    def __init__(self, **_kwargs):
        self.ready = False
        self.last_error = ""

    def start(self):
        self.ready = True

    def close(self):
        return None


class DeepSupportPairedStream(PairedStream):
    no_bid_size = 40
    no_near_depth = 40


class ResidualTenSupportPairedStream(PairedStream):
    no_ask_size = 10




class NoOrderIdStartupExecutor:
    def __init__(self, store):
        self.store = store

    def reconcile_existing(self, record):
        return OrderExecutor(Settings(dry_run=False), self.store, gateway=object())._result_from_record(
            record,
            "execution_unknown",
            0.0,
            0.0,
            False,
            "unknown",
            "persisted_intent_has_no_order_id",
            {},
        )




class PendingThenMatchedStartupExecutor:
    def __init__(self):
        self.calls = 0

    def reconcile_existing(self, record):
        self.calls += 1
        if self.calls == 1:
            return OrderExecutor(Settings(dry_run=False), StateStore.__new__(StateStore), gateway=object())._result_from_record(
                record,
                "submitted_pending",
                0.0,
                0.0,
                False,
                "awaiting_settlement",
                "gtc_awaiting_settlement",
                {"awaiting_settlement": True},
            )
        return OrderExecutor(Settings(dry_run=False), StateStore.__new__(StateStore), gateway=object())._result_from_record(
            record, "matched", record.requested_qty, record.requested_price, True, "acknowledged", "", {}
        )

class InstantGateway:
    submitted_qty = 0.0

    def market_metadata(self, condition_id):
        return MarketMetadata(
            condition_id=condition_id,
            tick_size="0.01",
            min_order_size=1,
            neg_risk=False,
            fee_rate=0.0,
            tokens={"up": "up-token", "down": "down-token"},
            raw={},
        )

    def preflight(self, geo, required_cash):
        return LivePreflight(
            geo=geo,
            collateral=BalanceAllowance(balance=100, allowance=100, raw={}),
            closed_only=False,
        )

    def submit_limit_buy_fast(self, token_id, price, qty, metadata, order_type="FAK"):
        self.submitted_qty = qty
        return {"orderID": "order-live-1"}

    def get_order(self, order_id):
        return {"id": order_id, "status": "matched", "size_matched": str(self.submitted_qty), "average_price": "0.64"}

    def cancel_order(self, order_id):
        raise AssertionError("matched order must not be cancelled")

    def order_trades(self, token_id, order_id):
        return [{"order_id": order_id, "size": str(self.submitted_qty), "price": "0.64", "feeUsdc": "0"}]

    def post_heartbeat(self, heartbeat_id=""):
        return {"heartbeat_id": "h"}


def test_shadow_round_uses_websocket_classifier_and_never_sends_an_order(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        settings = Settings(dry_run=True, out_dir=tmp_path / "out", state_db=tmp_path / "state.sqlite3")
        decisions = run_round(
            settings=settings,
            store=store,
            public=FakePublic(),
            executor=OrderExecutor(settings, store),
            live_gateway=None,
            round_start=900,
            clock=lambda: 1200.22,
            sleep=lambda _: None,
            stream_factory=DeepSupportPairedStream,
        )
        assert any(item.action == "enter" and item.side == "NO" for item in decisions)
        assert store.market_state("btc-updown-5m-900") == "open"
        open_positions = store.open_positions()
        assert len(open_positions) == 1
        assert open_positions[0].state == "filled"
        assert open_positions[0].filled_qty == 20
    finally:
        store.close()


def test_runtime_status_reports_active_v8_guards(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        settings = Settings(
            dry_run=True,
            out_dir=tmp_path / "out",
            state_db=tmp_path / "state.sqlite3",
        )

        status = runner_module._status_payload(settings, store)

        assert status["strategy"] == "aftertake_v8_clob_refill_guard_250ms"
        assert status["entry_window_ms"] == [50, 250]
        assert status["confirmations"] == 2
        assert status["confirmation_spacing_ms"] == 0
        assert status["require_loser_refill_failure"] is True
        assert status["require_stable_post_close_leader"] is True
    finally:
        store.close()


def test_residual_ten_ask_is_supported_when_near_touch_depth_covers_final_size(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        settings = Settings(dry_run=True, out_dir=tmp_path / "out", state_db=tmp_path / "state.sqlite3")
        decisions = run_round(
            settings=settings,
            store=store,
            public=FakePublic(),
            executor=OrderExecutor(settings, store),
            live_gateway=None,
            round_start=900,
            clock=lambda: 1200.22,
            sleep=lambda _: None,
            stream_factory=ResidualTenSupportPairedStream,
        )
        assert any(item.action == "enter" and item.side == "NO" for item in decisions)
        open_positions = store.open_positions()
        assert len(open_positions) == 1
        assert open_positions[0].filled_qty == 10
    finally:
        store.close()


def test_live_round_emits_only_execution_lifecycle_messages(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        settings = Settings(dry_run=False, order_type="GTC", out_dir=tmp_path / "out", state_db=tmp_path / "state.sqlite3")
        notifier = CaptureNotifier()
        gateway = InstantGateway()
        timestamps = iter((1190.0, 1190.1, 1200.22, 1200.24))
        decisions = run_round(
            settings=settings,
            store=store,
            public=LivePublic(),
            executor=OrderExecutor(settings, store, gateway=gateway, wall_clock=lambda: 1200.36),
            live_gateway=gateway,
            round_start=900,
            clock=lambda: next(timestamps, 1200.24),
            sleep=lambda _: None,
            notifier=notifier,
            stream_factory=DeepSupportPairedStream,
        )
        assert any(item.action == "enter" for item in decisions)
        assert store.market_state("btc-updown-5m-900") == "open"
        assert gateway.submitted_qty == 20
        assert [message.splitlines()[0] for message in notifier.messages] == ["[Aftertake] ENTRY_CONFIRMED"]
        assert "Market: slug=btc-updown-5m-900 side=NO" in notifier.messages[0]
        assert "Qty: requested=20.0000 filled=20.0000 unfilled=0.0000 fill_rate=100.00%" in notifier.messages[0]
        assert "decision_to_submit_ms=" in notifier.messages[0]
        assert "reconcile_duration_ms=" in notifier.messages[0]
        assert "Price: take=0.6400 avg=0.6400 available=20.0000" in notifier.messages[0]
        assert store.open_positions()[0].raw["timing"]["book_observed_ts"] == 1200.22
    finally:
        store.close()


def test_post_close_hold_polling_uses_five_millisecond_cadence(tmp_path):
    class AdvancingClock:
        def __init__(self):
            self.now = 1200.0
            self.sleeps = []

        def __call__(self):
            return self.now

        def sleep(self, seconds):
            self.sleeps.append(seconds)
            self.now += seconds

    store = StateStore(tmp_path / "state.sqlite3")
    try:
        clock = AdvancingClock()
        settings = Settings(dry_run=True, out_dir=tmp_path / "out", state_db=tmp_path / "state.sqlite3")
        decisions = run_round(
            settings=settings,
            store=store,
            public=FakePublic(),
            executor=OrderExecutor(settings, store),
            live_gateway=None,
            round_start=900,
            clock=clock,
            sleep=clock.sleep,
            stream_factory=ReadyEmptyStream,
        )

        assert decisions
        assert clock.sleeps
        assert set(clock.sleeps) == {0.005}
    finally:
        store.close()


def test_qualifying_decision_audit_is_deferred_until_after_submit(tmp_path, monkeypatch):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        settings = Settings(dry_run=False, order_type="GTC", out_dir=tmp_path / "out", state_db=tmp_path / "state.sqlite3")
        gateway = InstantGateway()
        executor = OrderExecutor(settings, store, gateway=gateway, wall_clock=lambda: 1200.36)
        events = []
        original_audit = runner_module._audit
        original_execute = executor.execute_reserved

        def track_audit(settings, store, kind, payload, slug=""):
            if kind == "aftertake_decision":
                events.append("decision_audit")
            return original_audit(settings, store, kind, payload, slug)

        def track_execute(*args, **kwargs):
            events.append("submit")
            return original_execute(*args, **kwargs)

        monkeypatch.setattr(runner_module, "_audit", track_audit)
        monkeypatch.setattr(executor, "execute_reserved", track_execute)
        timestamps = iter((1190.0, 1190.1, 1200.22, 1200.24))

        run_round(
            settings=settings,
            store=store,
            public=LivePublic(),
            executor=executor,
            live_gateway=gateway,
            round_start=900,
            clock=lambda: next(timestamps, 1200.24),
            sleep=lambda _: None,
            stream_factory=DeepSupportPairedStream,
        )

        assert events == ["submit", "decision_audit"]
    finally:
        store.close()


def test_live_round_blocks_dynamic_quantity_that_the_observed_bid_support_cannot_cover(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        settings = Settings(dry_run=False, order_type="GTC", out_dir=tmp_path / "out", state_db=tmp_path / "state.sqlite3")
        notifier = CaptureNotifier()
        gateway = InstantGateway()
        timestamps = iter((1190.0, 1190.1, 1200.22))
        decisions = run_round(
            settings=settings,
            store=store,
            public=LivePublic(),
            executor=OrderExecutor(settings, store, gateway=gateway),
            live_gateway=gateway,
            round_start=900,
            clock=lambda: next(timestamps, 1200.25),
            sleep=lambda _: None,
            notifier=notifier,
            stream_factory=PairedStream,
        )
        assert any(item.action == "enter" for item in decisions)
        assert gateway.submitted_qty == 0
        assert store.market_state("btc-updown-5m-900") == "observing"
        assert notifier.messages == []
    finally:
        store.close()



def test_crypto_slug_helper_supports_configured_assets():
    assert current_crypto_5m_slug("BTC", 901) == ("btc-updown-5m-900", 900, 1200)
    assert current_crypto_5m_slug("ETH", 901) == ("eth-updown-5m-900", 900, 1200)
    assert current_crypto_5m_slug("XRP", 901) == ("xrp-updown-5m-900", 900, 1200)
    assert current_crypto_5m_slug("HYPE", 901) == ("hype-updown-5m-900", 900, 1200)
    assert current_crypto_5m_slug("DOGE", 901) == ("doge-updown-5m-900", 900, 1200)
    assert current_crypto_5m_slug("SOL", 901) == ("sol-updown-5m-900", 900, 1200)


def test_configured_assets_run_for_the_same_round_boundary(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        settings = Settings(
            dry_run=True,
            assets=("BTC", "ETH", "XRP", "HYPE", "DOGE", "SOL"),
            out_dir=tmp_path / "out",
            state_db=tmp_path / "state.sqlite3",
        )
        results = _run_asset_rounds(
            settings=settings,
            store=store,
            public=FakePublic(),
            executor=OrderExecutor(settings, store),
            live_gateway=None,
            round_start=900,
            notifier=CaptureNotifier(),
            stream_factory=DeepSupportPairedStream,
        )
        assert sorted(results) == ["BTC", "DOGE", "ETH", "HYPE", "SOL", "XRP"]
        for asset in settings.assets:
            assert store.market_state(f"{asset.lower()}-updown-5m-900") in {"observing", "open"}
    finally:
        store.close()


def test_live_round_account_preflight_is_shared_once_across_assets(tmp_path):
    class AssetPublic(LivePublic):
        def __init__(self):
            self.geo_calls = 0

        def geoblock_status(self, endpoint):
            self.geo_calls += 1
            return super().geoblock_status(endpoint)

    class CountingGateway(InstantGateway):
        def __init__(self):
            self.preflight_calls = 0

        def preflight(self, geo, required_cash):
            self.preflight_calls += 1
            assert required_cash == 0.0
            return super().preflight(geo, required_cash)

    class PreflightThenExpiredClock:
        def __init__(self):
            self._local = threading.local()

        def __call__(self):
            calls = getattr(self._local, "calls", 0) + 1
            self._local.calls = calls
            return 1190.0 if calls <= 2 else 1201.1

    store = StateStore(tmp_path / "state.sqlite3")
    try:
        settings = Settings(
            dry_run=False,
            assets=("BTC", "ETH", "XRP"),
            out_dir=tmp_path / "out",
            state_db=tmp_path / "state.sqlite3",
        )
        public = AssetPublic()
        gateway = CountingGateway()
        results = _run_asset_rounds(
            settings=settings,
            store=store,
            public=public,
            executor=OrderExecutor(settings, store, gateway=gateway),
            live_gateway=gateway,
            round_start=900,
            notifier=CaptureNotifier(),
            stream_factory=DeepSupportPairedStream,
            clock=PreflightThenExpiredClock(),
            sleep=lambda _seconds: None,
        )

        assert sorted(results) == ["BTC", "ETH", "XRP"]
        assert public.geo_calls == 1
        assert gateway.preflight_calls == 1
    finally:
        store.close()


def test_live_asset_transport_failure_isolated_without_rebuilding_runtime(tmp_path, monkeypatch):
    class PolyApiExceptionLike(Exception):
        status_code = None
        error_message = "Request exception!"

    class AssetPublic(LivePublic):
        def market_by_slug(self, slug, allow_closed=False):
            market = super().market_by_slug(slug, allow_closed=allow_closed)
            return GammaMarket(
                slug=market.slug,
                condition_id="condition-" + slug.split("-", 1)[0],
                outcomes=market.outcomes,
                clob_token_ids=market.clob_token_ids,
                active=market.active,
            )

    class FlakyGateway(InstantGateway):
        def market_metadata(self, condition_id):
            if condition_id == "condition-eth":
                raise PolyApiExceptionLike()
            return super().market_metadata(condition_id)

    class PreflightThenExpiredClock:
        def __init__(self):
            self._local = threading.local()

        def __call__(self):
            calls = getattr(self._local, "calls", 0) + 1
            self._local.calls = calls
            return 1190.0 if calls <= 2 else 1201.1

    store = StateStore(tmp_path / "state.sqlite3")
    try:
        settings = Settings(
            dry_run=False,
            assets=("BTC", "ETH", "XRP"),
            out_dir=tmp_path / "out",
            state_db=tmp_path / "state.sqlite3",
        )
        notifier = CaptureNotifier()
        gateway = FlakyGateway()
        runtime_calls = []
        asset_results = {}

        def live_runtime(*_args):
            runtime_calls.append("runtime")
            return gateway, OrderExecutor(settings, store, gateway=gateway)

        def run_with_test_clock(**kwargs):
            asset_results.update(
                _run_asset_rounds(
                    **kwargs,
                    stream_factory=DeepSupportPairedStream,
                    clock=PreflightThenExpiredClock(),
                    sleep=lambda _seconds: None,
                )
            )
            return asset_results

        monkeypatch.setattr(runner_module, "_run_asset_rounds", run_with_test_clock)
        _run_round_loop(
            settings=settings,
            store=store,
            public=AssetPublic(),
            notifier=notifier,
            forever=False,
            rounds=1,
            live_runtime_factory=live_runtime,
            wait_for_next_boundary=lambda: 900,
            sleep=lambda _seconds: None,
        )

        assert runtime_calls == ["runtime"]
        assert sorted(asset_results) == ["BTC", "ETH", "XRP"]
        assert asset_results["ETH"][-1].reason == "market_metadata_transport_error"
        assert all(asset_results[asset] for asset in ("BTC", "XRP"))
        assert notifier.messages == []
        audit = store._conn.execute(
            """SELECT slug, payload_json FROM audit_events
               WHERE kind = 'asset_transport_error' ORDER BY id DESC LIMIT 1"""
        ).fetchone()
        assert audit["slug"] == "eth-updown-5m-900"
        payload = json.loads(audit["payload_json"])
        assert payload == {
            "asset": "ETH",
            "error_message": "Request exception!",
            "error_type": "PolyApiExceptionLike",
            "phase": "market_metadata",
            "slug": "eth-updown-5m-900",
            "status_code": None,
        }
    finally:
        store.close()


def test_unhandled_asset_transport_failure_isolated_without_rebuilding_runtime(tmp_path, monkeypatch):
    class PolyApiExceptionLike(Exception):
        status_code = None
        error_message = "Request exception!"

    store = StateStore(tmp_path / "state.sqlite3")
    try:
        settings = Settings(
            dry_run=False,
            assets=("BTC", "ETH", "XRP"),
            out_dir=tmp_path / "out",
            state_db=tmp_path / "state.sqlite3",
        )
        notifier = CaptureNotifier()
        gateway = InstantGateway()
        runtime_calls = []
        worker_threads = []
        asset_result_rounds = []

        def live_runtime(*_args):
            runtime_calls.append("runtime")
            return gateway, OrderExecutor(settings, store, gateway=gateway)

        def run_round_with_eth_transport_error(*, asset, **_kwargs):
            worker_threads.append(threading.current_thread().name)
            if asset == "ETH":
                raise PolyApiExceptionLike()
            return [runner_module.PostCloseDecision("hold", "%s_complete" % asset.lower())]

        original_run_asset_rounds = runner_module._run_asset_rounds

        def capture_asset_results(**kwargs):
            results = original_run_asset_rounds(**kwargs)
            asset_result_rounds.append(results)
            return results

        round_starts = iter((900, 1200))
        monkeypatch.setattr(runner_module, "run_round", run_round_with_eth_transport_error)
        monkeypatch.setattr(runner_module, "_run_asset_rounds", capture_asset_results)
        _run_round_loop(
            settings=settings,
            store=store,
            public=LivePublic(),
            notifier=notifier,
            forever=False,
            rounds=2,
            live_runtime_factory=live_runtime,
            wait_for_next_boundary=lambda: next(round_starts),
            sleep=lambda _seconds: None,
        )

        assert runtime_calls == ["runtime"]
        assert len(asset_result_rounds) == 2
        assert all(sorted(results) == ["BTC", "ETH", "XRP"] for results in asset_result_rounds)
        assert all(thread.startswith("aftertake-asset") for thread in worker_threads)
        for results in asset_result_rounds:
            assert results["BTC"][-1].reason == "btc_complete"
            assert results["XRP"][-1].reason == "xrp_complete"
            assert (results["ETH"][-1].action, results["ETH"][-1].reason) == (
                "hold",
                "asset_round_transport_error",
            )
        assert notifier.messages == []
        assert store._conn.execute(
            "SELECT COUNT(*) FROM audit_events WHERE kind = 'round_runtime_error'"
        ).fetchone()[0] == 0
        audits = store._conn.execute(
            """SELECT slug, payload_json FROM audit_events
               WHERE kind = 'asset_transport_error' ORDER BY id"""
        ).fetchall()
        assert [audit["slug"] for audit in audits] == ["eth-updown-5m-900", "eth-updown-5m-1200"]
        for audit in audits:
            assert json.loads(audit["payload_json"]) == {
                "asset": "ETH",
                "error_message": "Request exception!",
                "error_type": "PolyApiExceptionLike",
                "phase": "asset_round_unhandled",
                "slug": audit["slug"],
                "status_code": None,
            }
    finally:
        store.close()


def test_configured_assets_do_not_block_each_other_by_position_or_cooldown(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        settings = Settings(max_open_positions=1, min_seconds_between_entries=60, state_db=tmp_path / "state.sqlite3")
        btc = store.reserve_entry("btc-updown-5m-900", "condition-btc", 900, "btc-token", "NO", 5, 0.64)
        assert btc is not None
        store.mark_terminal_execution(btc.intent_id, 5, 0.64, {"test": True}, "matched")

        # Same asset is capped/cooldowned.
        try:
            check_entry_risk(
                settings=settings,
                store=store,
                slug="btc-updown-5m-1200",
                price=0.64,
                qty=5,
                displayed_ask_size=5,
                now_ts=1201,
            )
        except RiskRejected as exc:
            assert str(exc) in {"max_open_positions_for_asset", "entry_cooldown_for_asset"}
        else:
            raise AssertionError("same asset must respect per-asset risk gates")

        # Different assets must not be blocked by BTC's position or cooldown.
        snapshot = check_entry_risk(
            settings=settings,
            store=store,
            slug="eth-updown-5m-1200",
            price=0.64,
            qty=5,
            displayed_ask_size=5,
            now_ts=1201,
        )
        assert snapshot.open_positions == 0
        assert snapshot.seconds_since_last_entry is None
    finally:
        store.close()


def test_round_scheduler_joins_active_round_after_previous_close_instead_of_skipping():
    slept = []
    processed = {900}

    selected = _select_next_round_start(
        now=1201.0,
        processed_round_starts=processed,
        sleep=lambda seconds: slept.append(seconds),
    )

    assert selected == 1200
    assert slept == []


def test_round_scheduler_waits_when_active_round_is_too_close_to_close():
    slept = []
    processed = set()

    selected = _select_next_round_start(
        now=1494.0,
        processed_round_starts=processed,
        sleep=lambda seconds: slept.append(seconds),
    )

    assert selected == 1500
    assert slept == [6.0]


def test_deployment_check_requires_tg_and_verifies_paired_websocket():
    class CountingPairedStream(PairedStream):
        starts = 0

        def start(self):
            type(self).starts += 1
            return super().start()

    notifier = CaptureNotifier()
    result = deployment_check(
        settings=Settings(dry_run=True),
        public=FakePublic(),
        gateway=None,
        notifier=notifier,
        clock=lambda: 1200.0,
        stream_factory=CountingPairedStream,
    )
    assert result["websocket_verified"] is True
    assert CountingPairedStream.starts == 6
    assert result["telegram_verified"] is True
    assert notifier.messages[0].startswith("[Aftertake] DEPLOYMENT_CHECK_OK")

    try:
        deployment_check(
            settings=Settings(dry_run=True),
            public=FakePublic(),
            gateway=None,
            notifier=Notifier(),
            clock=lambda: 1200.0,
            stream_factory=PairedStream,
        )
    except Exception as exc:
        assert "TG_BOT_TOKEN and TG_CHAT_ID" in str(exc)
    else:
        raise AssertionError("deployment check must reject missing Telegram")


def test_stream_probe_allows_a_transient_websocket_timeout_before_paired_books_arrive():
    class TransientThenReadyStream:
        def __init__(self, **_kwargs):
            self._ready_checks = 0
            self.last_error = "WebSocketTimeoutException: Connection timed out"
            self.closed = False

        @property
        def ready(self):
            self._ready_checks += 1
            return self._ready_checks >= 2

        def start(self):
            return None

        def close(self):
            self.closed = True

    # The actual stream reconnects after a connect timeout.  Deployment must
    # wait through the bounded probe window instead of rejecting that first
    # retryable error immediately.
    _probe_stream(FakePublic().market_by_slug("btc-updown-5m-0"), stream_factory=TransientThenReadyStream)


def test_forever_runner_retries_a_live_pm_bootstrap_failure_without_exiting(tmp_path):
    class StopLoop(Exception):
        pass

    store = StateStore(tmp_path / "state.sqlite3")
    try:
        settings = Settings(dry_run=False, order_type="GTC", out_dir=tmp_path / "out", state_db=tmp_path / "state.sqlite3")
        notifier = CaptureNotifier()
        attempts = []

        def flaky_runtime(*_args):
            attempts.append("attempt")
            if len(attempts) == 1:
                raise RuntimeError("temporary Polymarket transport outage")
            return InstantGateway(), OrderExecutor(settings, store, gateway=InstantGateway())

        def next_boundary():
            raise StopLoop()

        try:
            _run_round_loop(
                settings=settings,
                store=store,
                public=LivePublic(),
                notifier=notifier,
                forever=True,
                rounds=1,
                live_runtime_factory=flaky_runtime,
                wait_for_next_boundary=next_boundary,
                sleep=lambda _seconds: None,
            )
        except StopLoop:
            pass
        else:
            raise AssertionError("test loop must stop after the recovered runtime reaches a boundary")

        assert attempts == ["attempt", "attempt"]
        assert any("temporary Polymarket transport outage" in message for message in notifier.messages)
    finally:
        store.close()


def test_service_main_reports_boot_before_any_live_pm_connection(monkeypatch, tmp_path):
    settings = Settings(dry_run=False, order_type="GTC", out_dir=tmp_path / "out", state_db=tmp_path / "state.sqlite3")
    notifier = CaptureNotifier()
    code_sha = "b" * 40

    class NoLock:
        def __init__(self, _path):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    def assert_boot_then_stop(**_kwargs):
        assert len(notifier.messages) == 1
        assert notifier.messages[0].startswith(
            "[Aftertake] BOOT\nmode=LIVE qty=5.0000 assets=BTC,ETH,XRP,HYPE,DOGE,SOL\n"
            f"pid=4242 code_sha={code_sha}\n"
            "multi-asset per-asset risk gates + SQLite recovery + CLOB V2 preflight\n"
        )
        assert "notification_ts_utc=" in notifier.messages[0]
        assert "notification_ts_ms=" in notifier.messages[0]
        audit = _kwargs["store"]._conn.execute(
            "SELECT payload_json FROM audit_events WHERE kind = 'boot' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        assert json.loads(audit["payload_json"])["pid"] == 4242
        assert json.loads(audit["payload_json"])["code_sha"] == code_sha

    monkeypatch.setattr(runner_module.Settings, "from_env", staticmethod(lambda: settings))
    monkeypatch.setattr(runner_module, "RuntimeLock", NoLock)
    monkeypatch.setattr(runner_module, "PolymarketPublicClient", lambda **_kwargs: object())
    monkeypatch.setattr(runner_module, "Notifier", lambda **_kwargs: notifier)
    monkeypatch.setattr(runner_module.os, "getpid", lambda: 4242)
    monkeypatch.setattr(runner_module, "_resolve_code_sha", lambda: code_sha)
    monkeypatch.setattr(runner_module, "_run_round_loop", assert_boot_then_stop)

    assert runner_module.main(["--forever"]) == 0


def _boot_source_file(tmp_path: Path) -> Path:
    source_file = tmp_path / "src" / "aftertake" / "runner.py"
    source_file.parent.mkdir(parents=True)
    source_file.touch()
    return source_file


def test_resolve_code_sha_reads_direct_head_metadata(tmp_path):
    source_file = _boot_source_file(tmp_path)
    code_sha = "c" * 40
    git_dir = tmp_path / ".git"
    git_dir.mkdir()
    (git_dir / "HEAD").write_text(f"{code_sha}\n", encoding="ascii")

    assert runner_module._resolve_code_sha(source_file) == code_sha


def test_resolve_code_sha_reads_in_tree_head_ref_metadata(tmp_path):
    source_file = _boot_source_file(tmp_path)
    code_sha = "d" * 64
    git_dir = tmp_path / ".git"
    ref_path = git_dir / "refs" / "heads" / "main"
    ref_path.parent.mkdir(parents=True)
    (git_dir / "HEAD").write_text("ref: refs/heads/main\n", encoding="ascii")
    ref_path.write_text(f"{code_sha}\n", encoding="ascii")

    assert runner_module._resolve_code_sha(source_file) == code_sha


def test_resolve_code_sha_returns_unknown_for_missing_malformed_or_unsafe_metadata(tmp_path):
    source_file = _boot_source_file(tmp_path)

    assert runner_module._resolve_code_sha(source_file) == "unknown"

    git_dir = tmp_path / ".git"
    git_dir.mkdir()
    (git_dir / "HEAD").write_text("not-a-sha\n", encoding="ascii")
    assert runner_module._resolve_code_sha(source_file) == "unknown"

    outside_sha = "e" * 40
    (tmp_path / "outside-ref").write_text(f"{outside_sha}\n", encoding="ascii")
    (git_dir / "HEAD").write_text("ref: ../outside-ref\n", encoding="ascii")
    assert runner_module._resolve_code_sha(source_file) == "unknown"


def test_confirmed_open_fill_is_settled_from_pm_outcome(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        record = store.reserve_entry("btc-updown-5m-0", "condition", 0, "up-token", "YES", 5, 0.6, fee_rate=0)
        store.mark_terminal_execution(record.intent_id, 5, 0.6, {"trades": [{"feeUsdc": "0"}]}, "matched")
        settings = Settings(out_dir=tmp_path / "out", state_db=tmp_path / "state.sqlite3")
        settled = settle_open_positions(settings=settings, store=store, public=ResolvedPublic())
        assert settled[0]["settlement_source"] == "pm"
        assert settled[0]["win"] is True
        assert store.market_state(record.slug) == "settled"
    finally:
        store.close()


def test_startup_reconciliation_missing_order_id_terminal_skips_without_alert_spam(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        record = store.reserve_entry(
            "btc-updown-5m-900",
            "condition",
            900,
            "token",
            "NO",
            5,
            0.64,
        )
        store.mark_execution_unknown(record.intent_id, "submit_exception", {"order_type": "FAK"})
        notifier = CaptureNotifier()

        _reconcile_startup(
            Settings(dry_run=False, order_type="FAK", out_dir=tmp_path / "out", state_db=tmp_path / "state.sqlite3"),
            store,
            NoOrderIdStartupExecutor(store),
            notifier,
        )

        assert store.has_execution_unknown() is False
        unresolved = store.unresolved_orders()
        assert unresolved == []
        assert notifier.messages == []
        assert store.reserve_entry("eth-updown-5m-1200", "condition", 1200, "token", "YES", 5, 0.51) is not None
    finally:
        store.close()


def test_reconcile_submitted_orders_keeps_pending_quiet_then_notifies_terminal(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        record = store.reserve_entry("btc-updown-5m-900", "condition", 900, "up-token", "YES", 5, 0.64)
        assert record is not None
        store.mark_submitted(record.intent_id, "order-live-1", {"orderID": "order-live-1"})
        notifier = CaptureNotifier()
        executor = PendingThenMatchedStartupExecutor()
        settings = Settings(dry_run=False, order_type="GTC", out_dir=tmp_path / "out", state_db=tmp_path / "state.sqlite3")

        first = reconcile_submitted_orders(settings=settings, store=store, executor=executor, notifier=notifier)
        assert first[0].status == "submitted_pending"
        assert notifier.messages == []
        assert store.market_state("btc-updown-5m-900") == "submitted"

        second = reconcile_submitted_orders(settings=settings, store=store, executor=executor, notifier=notifier)
        assert second[0].terminal is True
        assert notifier.messages
        assert notifier.messages[-1].startswith("[Aftertake] ENTRY_CONFIRMED")
    finally:
        store.close()


def test_round_loop_scans_assets_before_pending_order_reconciliation(monkeypatch, tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        settings = Settings(dry_run=False, order_type="GTC", out_dir=tmp_path / "out", state_db=tmp_path / "state.sqlite3")
        notifier = CaptureNotifier()
        events = []

        def live_runtime(*_args):
            events.append("runtime")
            gateway = InstantGateway()
            return gateway, OrderExecutor(settings, store, gateway=gateway)

        def boundary():
            events.append("boundary")
            return 900

        def scan_assets(**_kwargs):
            events.append("scan_assets")
            return {}

        def reconcile_pending(**_kwargs):
            events.append("reconcile_pending")
            return []

        def settle(**_kwargs):
            events.append("settle")
            return []

        monkeypatch.setattr(runner_module, "_run_asset_rounds", scan_assets)
        monkeypatch.setattr(runner_module, "reconcile_submitted_orders", reconcile_pending)
        monkeypatch.setattr(runner_module, "settle_open_positions", settle)

        _run_round_loop(
            settings=settings,
            store=store,
            public=LivePublic(),
            notifier=notifier,
            forever=False,
            rounds=1,
            live_runtime_factory=live_runtime,
            wait_for_next_boundary=boundary,
            sleep=lambda _seconds: None,
        )

        assert events == ["runtime", "boundary", "scan_assets", "reconcile_pending", "settle"]
    finally:
        store.close()
