import json
import signal
import threading
import time
from pathlib import Path

import pytest

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
    V2ClobGateway,
)
from aftertake.post_close import PairedBook, SideBook
from aftertake.risk import RiskRejected, check_entry_risk
from aftertake.runner import (
    RuntimeWatchdog,
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
    yes_ask_size = 60
    no_ask_size = 60

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

        # The live contract freezes from the latest local paired snapshot at
        # close+0.5s; later observations are intentionally ignored for side
        # selection. Keep eligible close-window snapshots for lifecycle tests.
        add(1199.40, 0.47, 0.50, 0.85, 0.99, yes_near=18, no_near=18)
        add(1199.70, 0.47, 0.50, 0.51, 0.54, yes_near=18, no_near=18)
        add(1199.82, 0.48, 0.51, 0.50, 0.53, yes_near=18, no_near=18)
        add(1199.95, 0.49, 0.64, 0.50, 0.53, yes_near=18, no_near=18)
        add(1200.10, 0.35, 0.37, 0.58, 0.64, yes_size=2, no_size=self.no_bid_size, yes_near=2, no_near=self.no_near_depth)
        add(1200.22, 0.30, 0.37, 0.60, 0.64, yes_size=2, no_size=self.no_bid_size, yes_near=2, no_near=self.no_near_depth)
        add(1200.35, 0.28, 0.37, 0.61, 0.64, yes_size=2, no_size=self.no_bid_size, yes_near=2, no_near=self.no_near_depth)
        add(1200.40, 0.28, 0.37, 0.85, 0.64, yes_size=2, no_size=self.no_bid_size, yes_near=2, no_near=self.no_near_depth)
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


class StableWinnerAsk99PairedStream(PairedStream):
    """Two fresh post-close events with unchanged executable winner support."""

    def start(self):
        def add(ts, yes_bid, yes_ask, no_bid, no_ask, *, yes_size, no_size, yes_near, no_near):
            self._on_book(
                PairedBook(
                    observed_at=ts,
                    yes=SideBook(yes_bid, yes_size, yes_size, yes_ask, 60, yes_near),
                    no=SideBook(no_bid, no_size, no_size, no_ask, 60, no_near),
                )
            )

        add(1199.40, 0.20, 0.99, 0.85, 0.99, yes_size=2, no_size=60, yes_near=2, no_near=60)
        add(1199.70, 0.47, 0.50, 0.51, 0.54, yes_size=20, no_size=20, yes_near=20, no_near=20)
        add(1199.82, 0.48, 0.51, 0.50, 0.53, yes_size=20, no_size=20, yes_near=20, no_near=20)
        add(1199.95, 0.49, 0.52, 0.50, 0.53, yes_size=20, no_size=20, yes_near=20, no_near=20)
        for ts in (1200.060, 1200.064):
            add(ts, 0.20, 0.99, 0.70, 0.99, yes_size=2, no_size=20, yes_near=2, no_near=20)
        add(1200.40, 0.20, 0.99, 0.85, 0.99, yes_size=2, no_size=60, yes_near=2, no_near=60)
        self.ready = True


class ResidualTenSupportPairedStream(PairedStream):
    no_ask_size = 10




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
            clock=lambda: 1200.50,
            sleep=lambda _: None,
            stream_factory=DeepSupportPairedStream,
        )
        assert any(item.action == "enter" and item.side == "NO" for item in decisions)
        assert store.market_state("btc-updown-5m-900") == "open"
        open_positions = store.open_positions()
        assert len(open_positions) == 1
        assert open_positions[0].state == "filled"
        assert open_positions[0].filled_qty == 50
    finally:
        store.close()


def test_run_round_does_not_arm_market_data_watchdog_for_quiet_book(tmp_path):
    streams = []

    class TrackingStream(DeepSupportPairedStream):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.watchdog_arms = []

        def arm_market_data_watchdog(self, timeout_s):
            self.watchdog_arms.append(timeout_s)

    def stream_factory(**kwargs):
        stream = TrackingStream(**kwargs)
        streams.append(stream)
        return stream

    store = StateStore(tmp_path / "state.sqlite3")
    try:
        settings = Settings(
            dry_run=True,
            out_dir=tmp_path / "out",
            state_db=tmp_path / "state.sqlite3",
        )
        run_round(
            settings=settings,
            store=store,
            public=FakePublic(),
            executor=OrderExecutor(settings, store),
            live_gateway=None,
            round_start=900,
            clock=lambda: 1200.50,
            sleep=lambda _: None,
            stream_factory=stream_factory,
        )
        assert len(streams) == 1
        assert streams[0].watchdog_arms == []
    finally:
        store.close()


def test_initial_stream_generation_transition_does_not_discard_qualifying_books(tmp_path):
    """The socket thread may publish its first books before runner observes generation 1."""

    streams = []

    class StartupRaceStream(PairedStream):
        def __init__(self, **kwargs):
            self._on_reset = kwargs.get("on_reset")
            super().__init__(**kwargs)
            self.generation = 0

        def start(self):
            # Model start() returning before the socket thread resets the book
            # generation and receives its initial paired snapshot.
            return None

        def publish_initial_generation(self):
            self._on_reset()
            super().start()
            self.generation = 1

    def stream_factory(**kwargs):
        stream = StartupRaceStream(**kwargs)
        streams.append(stream)
        return stream

    class StartupRaceClock:
        def __init__(self):
            self.now = 1049.0
            self.published = False

        def __call__(self):
            return self.now

        def sleep(self, seconds):
            if not self.published:
                streams[0].publish_initial_generation()
                self.published = True
                self.now = 1200.50
                return
            self.now += seconds

    store = StateStore(tmp_path / "state.sqlite3")
    try:
        clock = StartupRaceClock()
        settings = Settings(
            dry_run=True,
            out_dir=tmp_path / "out",
            state_db=tmp_path / "state.sqlite3",
        )
        decisions = run_round(
            settings=settings,
            store=store,
            public=FakePublic(),
            executor=OrderExecutor(settings, store),
            live_gateway=None,
            round_start=900,
            clock=clock,
            sleep=clock.sleep,
            stream_factory=stream_factory,
        )

        assert any(item.action == "enter" and item.side == "NO" for item in decisions)
    finally:
        store.close()


def test_reconnect_generation_transition_does_not_discard_fresh_books(tmp_path):
    """Fresh books can arrive before runner observes the reconnect generation."""

    streams = []

    class ReconnectRaceStream(PairedStream):
        def __init__(self, **kwargs):
            self._on_reset = kwargs.get("on_reset")
            super().__init__(**kwargs)
            self.generation = 1
            self.reconnect_count = 0

        def start(self):
            return None

        def publish_reconnected_generation(self):
            self._on_reset()
            super().start()
            self.generation = 2
            self.reconnect_count = 1

    def stream_factory(**kwargs):
        stream = ReconnectRaceStream(**kwargs)
        streams.append(stream)
        return stream

    class ReconnectRaceClock:
        def __init__(self):
            self.now = 1049.0
            self.published = False

        def __call__(self):
            return self.now

        def sleep(self, seconds):
            if not self.published:
                streams[0].publish_reconnected_generation()
                self.published = True
                self.now = 1200.50
                return
            self.now += seconds

    store = StateStore(tmp_path / "state.sqlite3")
    try:
        clock = ReconnectRaceClock()
        settings = Settings(
            dry_run=True,
            out_dir=tmp_path / "out",
            state_db=tmp_path / "state.sqlite3",
        )
        decisions = run_round(
            settings=settings,
            store=store,
            public=FakePublic(),
            executor=OrderExecutor(settings, store),
            live_gateway=None,
            round_start=900,
            clock=clock,
            sleep=clock.sleep,
            stream_factory=stream_factory,
        )

        assert any(item.action == "enter" and item.side == "NO" for item in decisions)
    finally:
        store.close()


def test_runtime_status_reports_active_post_close_snapshot_contract(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        settings = Settings(
            dry_run=True,
            out_dir=tmp_path / "out",
            state_db=tmp_path / "state.sqlite3",
        )

        status = runner_module._status_payload(settings, store)

        assert status["strategy"] == "aftertake_postclose_snapshot_v1_plus0_5_leader_bid_gt_080"
        assert status["post_close_snapshot_delay_ms"] == 500
        assert status["max_decision_lateness_ms"] == 250
        assert status["decision_window_ms"] == [500, 750]
        assert status["leader_bid_threshold"] == 0.80
        assert status["leader_bid_comparison"] == "strictly_greater_than"
        assert status["paired_receive_max_age_ms"] == 250
        assert status["confirmations"] == 0
        assert status["confirmation_spacing_ms"] == 0
        assert status["confirmation_policy"] == "none_post_close_snapshot_frozen"
        assert status["post_close_classifier_for_live_entry"] is False
        assert status["order_type"] == "GTC"
    finally:
        store.close()


def test_post_close_fixed_qty_ignores_displayed_ask_depth(tmp_path):
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
            clock=lambda: 1200.50,
            sleep=lambda _: None,
            stream_factory=ResidualTenSupportPairedStream,
        )
        assert any(item.action == "enter" and item.side == "NO" for item in decisions)
        assert not any("requested_qty_exceeds_displayed_ask_depth" in item.reason for item in decisions)
        assert store.open_positions()
    finally:
        store.close()


def test_live_round_emits_only_execution_lifecycle_messages(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        settings = Settings(dry_run=False, order_type="GTC", out_dir=tmp_path / "out", state_db=tmp_path / "state.sqlite3")
        notifier = CaptureNotifier()
        gateway = InstantGateway()
        timestamps = iter((1190.0, 1190.1, 1200.40, 1200.50))
        decisions = run_round(
            settings=settings,
            store=store,
            public=LivePublic(),
            executor=OrderExecutor(settings, store, gateway=gateway, wall_clock=lambda: 1200.36),
            live_gateway=gateway,
            round_start=900,
            clock=lambda: next(timestamps, 1200.50),
            sleep=lambda _: None,
            notifier=notifier,
            stream_factory=DeepSupportPairedStream,
        )
        assert any(item.action == "enter" for item in decisions)
        assert store.market_state("btc-updown-5m-900") == "open"
        assert gateway.submitted_qty == 50
        headlines = [message.splitlines()[0] for message in notifier.messages]
        assert "[Aftertake] POST_CLOSE_SNAPSHOT_FROZEN" in headlines
        assert "[Aftertake] ENTRY_CONFIRMED" in headlines
        assert "Market: slug=btc-updown-5m-900 side=NO" in notifier.messages[-1]
        assert "Qty: requested=50.0000 filled=50.0000 unfilled=0.0000 fill_rate=100.00%" in notifier.messages[-1]
        assert "decision_to_submit_ms=" in notifier.messages[-1]
        assert "reconcile_duration_ms=" in notifier.messages[-1]
        assert "Price: take=0.9900 avg=0.6400 available=60.0000" in notifier.messages[-1]
        assert store.open_positions()[0].raw["timing"]["book_observed_ts"] == 1200.40
    finally:
        store.close()


def test_live_round_reaches_single_gtc_post_through_v2_gateway(tmp_path):
    class Value:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    class Client:
        def __init__(self):
            self.posts = []

        def get_clob_market_info(self, condition_id):
            return {
                "ao": True,
                "mts": "0.01",
                "mos": "1",
                "t": [
                    {"t": "up-token", "o": "Up"},
                    {"t": "down-token", "o": "Down"},
                ],
                "fd": {"r": 0.0, "e": 1.0},
            }

        def get_closed_only_mode(self):
            return False

        def get_balance_allowance(self, _params):
            return {
                "balance": "100000000",
                "allowances": {"exchange": "100000000"},
            }

        def create_order(self, args, options=None):
            return {"args": args.kwargs, "options": options.kwargs}

        def post_order(self, order, *, order_type, post_only, defer_exec):
            self.posts.append(
                {
                    "order": order,
                    "order_type": order_type,
                    "post_only": post_only,
                    "defer_exec": defer_exec,
                }
            )
            return {"orderID": "order-v2-1"}

        def get_order(self, order_id):
            return {
                "id": order_id,
                "status": "matched",
                "size_matched": "50",
                "average_price": "0.64",
            }

    client = Client()
    gateway = V2ClobGateway(
        client,
        {
            "BalanceAllowanceParams": Value,
            "OrderArgs": Value,
            "PartialCreateOrderOptions": Value,
            "OrderPayload": Value,
            "TradeParams": Value,
            "BUY": "BUY",
            "OrderType": type("OrderType", (), {"GTC": "GTC"}),
            "AssetType": type("AssetType", (), {"COLLATERAL": "COLLATERAL"}),
        },
    )
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        settings = Settings(
            dry_run=False,
            order_type="GTC",
            out_dir=tmp_path / "out",
            state_db=tmp_path / "state.sqlite3",
        )
        timestamps = iter((1190.0, 1190.1, 1200.40, 1200.50))

        decisions = run_round(
            settings=settings,
            store=store,
            public=LivePublic(),
            executor=OrderExecutor(settings, store, gateway=gateway),
            live_gateway=gateway,
            round_start=900,
            clock=lambda: next(timestamps, 1200.50),
            sleep=lambda _seconds: None,
            notifier=CaptureNotifier(),
            stream_factory=StableWinnerAsk99PairedStream,
        )

        assert any(item.action == "enter" for item in decisions)
        assert store.market_state("btc-updown-5m-900") == "open"
        assert len(client.posts) == 1
        assert client.posts[0]["order_type"] == "GTC"
        assert client.posts[0]["post_only"] is False
        assert client.posts[0]["defer_exec"] is False
        assert client.posts[0]["order"]["args"]["token_id"] == "down-token"
        assert client.posts[0]["order"]["args"]["price"] == 0.99
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
        assert decisions[0].reason == "post_close_snapshot_no_paired_observation"
        assert clock.sleeps
    finally:
        store.close()


def test_post_close_snapshot_decision_audit_precedes_submit(tmp_path, monkeypatch):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        settings = Settings(dry_run=False, order_type="GTC", out_dir=tmp_path / "out", state_db=tmp_path / "state.sqlite3")
        gateway = InstantGateway()
        executor = OrderExecutor(settings, store, gateway=gateway, wall_clock=lambda: 1200.36)
        events = []
        original_audit = runner_module._audit
        original_execute = executor.execute_reserved

        def track_audit(settings, store, kind, payload, slug=""):
            if kind == "post_close_snapshot_frozen":
                events.append("decision_audit")
            return original_audit(settings, store, kind, payload, slug)

        def track_execute(*args, **kwargs):
            events.append("submit")
            return original_execute(*args, **kwargs)

        monkeypatch.setattr(runner_module, "_audit", track_audit)
        monkeypatch.setattr(executor, "execute_reserved", track_execute)
        timestamps = iter((1190.0, 1190.1, 1200.40, 1200.50))

        run_round(
            settings=settings,
            store=store,
            public=LivePublic(),
            executor=executor,
            live_gateway=gateway,
            round_start=900,
            clock=lambda: next(timestamps, 1200.50),
            sleep=lambda _: None,
            stream_factory=DeepSupportPairedStream,
        )

        assert events == ["decision_audit", "submit"]
    finally:
        store.close()


def test_live_round_submits_fixed_quantity_regardless_of_observed_ask_depth(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        settings = Settings(dry_run=False, qty=70, order_type="GTC", out_dir=tmp_path / "out", state_db=tmp_path / "state.sqlite3")
        notifier = CaptureNotifier()
        gateway = InstantGateway()
        timestamps = iter((1190.0, 1190.1, 1200.50))
        decisions = run_round(
            settings=settings,
            store=store,
            public=LivePublic(),
            executor=OrderExecutor(settings, store, gateway=gateway),
            live_gateway=gateway,
            round_start=900,
            clock=lambda: next(timestamps, 1200.50),
            sleep=lambda _: None,
            notifier=notifier,
            stream_factory=PairedStream,
        )
        assert any(item.action == "enter" for item in decisions)
        assert gateway.submitted_qty == 70.0
        alerts = [message for message in notifier.messages if message.startswith("[Aftertake] ALERT")]
        assert not any("entry blocked by risk/preflight" in message for message in alerts)
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


def test_all_assets_submit_without_shared_account_budget_cap(tmp_path):
    assets = ("BTC", "ETH", "XRP", "HYPE", "DOGE", "SOL")

    class AssetPublic(LivePublic):
        def market_by_slug(self, slug, allow_closed=False):
            asset = slug.split("-", 1)[0]
            return GammaMarket(
                slug=slug,
                condition_id="condition-" + asset,
                outcomes=("Up", "Down"),
                clob_token_ids=(asset + "-up-token", asset + "-down-token"),
                active=True,
            )

    class CapturingGateway:
        def __init__(self):
            self.preflight_calls = 0
            self._lock = threading.Lock()
            self._orders = {}

        def preflight(self, geo, required_cash):
            with self._lock:
                self.preflight_calls += 1
            assert required_cash == 0.0
            return LivePreflight(
                geo=geo,
                collateral=BalanceAllowance(balance=200.0, allowance=200.0, raw={}),
                closed_only=False,
            )

        def market_metadata(self, condition_id):
            asset = condition_id.removeprefix("condition-")
            return MarketMetadata(
                condition_id=condition_id,
                tick_size="0.01",
                min_order_size=1,
                neg_risk=False,
                fee_rate=0.0,
                tokens={"up": asset + "-up-token", "down": asset + "-down-token"},
                raw={},
            )

        def submit_limit_buy_fast(self, token_id, price, qty, metadata, order_type="GTC"):
            order_id = "order-" + token_id
            with self._lock:
                self._orders[order_id] = (str(token_id), float(price), float(qty))
            return {"orderID": order_id}

        def get_order(self, order_id):
            _token_id, price, qty = self._orders[order_id]
            return {
                "id": order_id,
                "status": "matched",
                "size_matched": str(qty),
                "average_price": str(price),
            }

        def order_trades(self, token_id, order_id):
            _stored_token, price, qty = self._orders[order_id]
            return [{"order_id": order_id, "size": str(qty), "price": str(price)}]

        def post_heartbeat(self, heartbeat_id=""):
            return {"heartbeat_id": heartbeat_id or "h"}

        @property
        def submitted_cost(self):
            with self._lock:
                return sum(price * qty for _token, price, qty in self._orders.values())

    class EntryClock:
        def __init__(self):
            self._local = threading.local()

        def __call__(self):
            calls = getattr(self._local, "calls", 0) + 1
            self._local.calls = calls
            return 1190.0 if calls <= 2 else 1200.50

    store = StateStore(tmp_path / "state.sqlite3")
    try:
        # Existing unresolved risk consumes $10 of the shared account budget.
        existing = store.reserve_entry(
            "legacy-updown-5m-0", "legacy-condition", 0, "legacy-token", "YES", 10, 1.0
        )
        assert existing is not None
        assert store.total_risk_exposure() == 10.0

        settings = Settings(
            dry_run=False,
            assets=assets,
            order_type="GTC",
            live_max_account_risk_fraction=0.50,
            out_dir=tmp_path / "out",
            state_db=tmp_path / "state.sqlite3",
        )
        gateway = CapturingGateway()
        results = _run_asset_rounds(
            settings=settings,
            store=store,
            public=AssetPublic(),
            executor=OrderExecutor(settings, store, gateway=gateway),
            live_gateway=gateway,
            round_start=900,
            notifier=Notifier(),
            stream_factory=DeepSupportPairedStream,
            clock=EntryClock(),
            sleep=lambda _seconds: None,
        )

        assert sorted(results) == sorted(assets)
        assert gateway.preflight_calls == 1
        assert gateway.submitted_cost == pytest.approx(6 * 50 * 0.99)
        assert all(any(item.action == "enter" for item in decisions) for decisions in results.values())
    finally:
        store.close()


def test_live_asset_transport_failure_isolated_and_schedules_runtime_rebuild(tmp_path, monkeypatch):
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
        asset_clocks = {
            asset: PreflightThenExpiredClock() for asset in settings.assets
        }
        original_run_round = runner_module.run_round

        def live_runtime(*_args):
            runtime_calls.append("runtime")
            return gateway, OrderExecutor(settings, store, gateway=gateway)

        def run_with_asset_clock(*, asset, clock, **kwargs):
            return original_run_round(
                asset=asset,
                clock=asset_clocks[asset],
                **kwargs,
            )

        def run_with_test_clock(**kwargs):
            asset_results.update(
                _run_asset_rounds(
                    **kwargs,
                    stream_factory=DeepSupportPairedStream,
                    sleep=lambda _seconds: None,
                )
            )
            return asset_results

        monkeypatch.setattr(runner_module, "run_round", run_with_asset_clock)
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
        alerts = [message for message in notifier.messages if message.startswith("[Aftertake] ALERT")]
        transport_alerts = [message for message in alerts if "asset transport error" in message]
        assert len(transport_alerts) == 1
        assert any(message.startswith("[Aftertake] RUNTIME_READY") for message in notifier.messages)
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


def test_unhandled_asset_transport_failure_isolated_and_rebuilds_runtime(tmp_path, monkeypatch):
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
        restart_requested = []

        class FakeWatchdog:
            def beat(self, _stage):
                return None

            def restart_after_diagnostics(self):
                restart_requested.append(True)

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
            runtime_watchdog=FakeWatchdog(),
        )

        assert runtime_calls == ["runtime"]
        assert restart_requested == []
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
        alerts = [message for message in notifier.messages if message.startswith("[Aftertake] ALERT")]
        assert len(alerts) == 2
        assert any(message.startswith("[Aftertake] RUNTIME_READY") for message in notifier.messages)
        assert store._conn.execute(
            "SELECT COUNT(*) FROM audit_events WHERE kind = 'round_runtime_error'"
        ).fetchone()[0] == 0
        audits = store._conn.execute(
            """SELECT slug, payload_json FROM audit_events
               WHERE kind = 'asset_transport_error' ORDER BY id"""
        ).fetchall()
        assert [audit["slug"] for audit in audits] == [
            "eth-updown-5m-900",
            "eth-updown-5m-1200",
        ]
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


def test_single_asset_non_transient_error_is_held_without_aborting_other_assets(
    tmp_path, monkeypatch
):
    assets = ("BTC", "ETH", "XRP", "HYPE", "DOGE", "SOL")
    completed = []
    completed_lock = threading.Lock()

    def one_broken_asset(*, asset, **_kwargs):
        if asset == "ETH":
            raise ValueError("malformed ETH market metadata")
        with completed_lock:
            completed.append(asset)
        return [runner_module.PostCloseDecision("hold", "clean_round")]

    monkeypatch.setattr(runner_module, "run_round", one_broken_asset)
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        settings = Settings(
            dry_run=True,
            assets=assets,
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
            timeout_s=1.0,
        )

        assert sorted(results) == sorted(assets)
        assert sorted(completed) == sorted(set(assets) - {"ETH"})
        assert results["ETH"][-1].action == "hold"
        assert "error" in results["ETH"][-1].reason
    finally:
        store.close()


def test_asset_transport_error_sends_recovery_success_on_next_clean_round(tmp_path, monkeypatch):
    class PolyApiExceptionLike(Exception):
        status_code = None
        error_message = "Request exception!"

    store = StateStore(tmp_path / "state.sqlite3")
    try:
        settings = Settings(
            dry_run=True,
            assets=("ETH",),
            out_dir=tmp_path / "out",
            state_db=tmp_path / "state.sqlite3",
        )
        notifier = CaptureNotifier()
        calls = {"count": 0}

        def flaky_round(**_kwargs):
            calls["count"] += 1
            if calls["count"] == 1:
                raise PolyApiExceptionLike()
            return [runner_module.PostCloseDecision("hold", "clean_round")]

        monkeypatch.setattr(runner_module, "run_round", flaky_round)
        error_state = set()
        for round_start in (900, 1200):
            _run_asset_rounds(
                settings=settings,
                store=store,
                public=FakePublic(),
                executor=OrderExecutor(settings, store),
                live_gateway=None,
                round_start=round_start,
                notifier=notifier,
                error_state=error_state,
                timeout_s=1.0,
            )

        assert len(notifier.messages) == 2
        assert notifier.messages[0].startswith("[Aftertake] ALERT")
        assert notifier.messages[1].startswith("[Aftertake] RECOVERY_SUCCESS")
        assert "component=asset:ETH" in notifier.messages[1]
        assert error_state == set()
    finally:
        store.close()


def test_asset_supervisor_returns_timeout_without_waiting_for_hung_worker(tmp_path, monkeypatch):
    release = threading.Event()
    restarts = []
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        settings = Settings(
            dry_run=True,
            assets=("BTC", "ETH"),
            out_dir=tmp_path / "out",
            state_db=tmp_path / "state.sqlite3",
        )

        def hung_round(*, asset, **_kwargs):
            if asset == "BTC":
                release.wait(timeout=2.0)
            return [runner_module.PostCloseDecision("hold", "%s_complete" % asset.lower())]

        monkeypatch.setattr(runner_module, "run_round", hung_round)
        started = time.monotonic()
        results = _run_asset_rounds(
            settings=settings,
            store=store,
            public=FakePublic(),
            executor=OrderExecutor(settings, store),
            live_gateway=None,
            round_start=900,
            timeout_s=0.02,
            restart_fn=lambda: restarts.append("restart"),
        )
        elapsed = time.monotonic() - started
        assert elapsed < 0.5
        assert results["BTC"][-1].reason == "asset_round_timeout"
        assert results["ETH"][-1].reason == "eth_complete"
        assert restarts == []
        timeout_audit = store._conn.execute(
            "SELECT payload_json FROM audit_events WHERE kind = 'asset_transport_error' AND slug = 'btc-updown-5m-900'"
        ).fetchone()
        assert timeout_audit is not None
        assert json.loads(timeout_audit["payload_json"])["phase"] == "asset_round_timeout"
    finally:
        release.set()
        store.close()


def test_live_asset_supervisor_timeout_is_the_only_worker_restart_boundary(tmp_path, monkeypatch):
    release = threading.Event()
    restarts = []
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        settings = Settings(
            dry_run=False,
            assets=("BTC",),
            out_dir=tmp_path / "out",
            state_db=tmp_path / "state.sqlite3",
        )

        def hung_round(**_kwargs):
            release.wait(timeout=2.0)
            return [runner_module.PostCloseDecision("hold", "late_complete")]

        monkeypatch.setattr(runner_module, "run_round", hung_round)
        results = _run_asset_rounds(
            settings=settings,
            store=store,
            public=FakePublic(),
            executor=OrderExecutor(settings, store),
            live_gateway=None,
            round_start=900,
            timeout_s=0.02,
            restart_fn=lambda: restarts.append("restart"),
        )

        assert results["BTC"][-1].reason == "asset_round_timeout"
        assert restarts == ["restart"]
    finally:
        release.set()
        store.close()


def test_asset_supervisor_budget_covers_remaining_active_round(tmp_path, monkeypatch):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        settings = Settings(
            dry_run=True,
            assets=("BTC",),
            out_dir=tmp_path / "out",
            state_db=tmp_path / "state.sqlite3",
        )
        notifier = CaptureNotifier()
        completed = threading.Event()

        def round_that_reaches_close(*_args, **_kwargs):
            # Simulate a service joining the 5m market 140s before its close.
            time.sleep(0.08)
            completed.set()
            return [runner_module.PostCloseDecision("hold", "clean_round")]

        monkeypatch.setattr(runner_module, "run_round", round_that_reaches_close)
        monkeypatch.setattr(runner_module.time, "time", lambda: 1060.0)
        results = _run_asset_rounds(
            settings=settings,
            store=store,
            public=FakePublic(),
            executor=OrderExecutor(settings, store),
            live_gateway=None,
            round_start=900,
            timeout_s=0.02,
            notifier=notifier,
        )

        assert completed.is_set()
        assert results["BTC"][-1].reason == "clean_round"
        assert notifier.messages == []
    finally:
        store.close()


def test_runtime_watchdog_stall_alerts_and_continues_without_exit():
    exits = []
    alerts = []
    watchdog = RuntimeWatchdog(
        stale_after_s=0.01,
        interval_s=0.001,
        exit_fn=exits.append,
        fatal_callback=lambda reason, payload: alerts.append((reason, payload)),
    )
    watchdog.start()
    try:
        deadline = time.monotonic() + 0.5
        while not exits and time.monotonic() < deadline:
            time.sleep(0.005)
        assert alerts
        assert exits == []
        assert watchdog._thread is not None and watchdog._thread.is_alive()
    finally:
        watchdog.stop()


def test_runtime_watchdog_stall_alerts_without_process_restart():
    events = []
    watchdog = RuntimeWatchdog(
        stale_after_s=0.01,
        interval_s=0.001,
        exit_fn=lambda _code: events.append("restart"),
        fatal_callback=lambda _reason, _payload: events.append("alert"),
    )
    watchdog.start()
    try:
        deadline = time.monotonic() + 0.5
        while not events and time.monotonic() < deadline:
            time.sleep(0.005)
        assert events == ["alert"]
    finally:
        watchdog.stop()


def test_runtime_watchdog_does_not_restart_while_waiting_for_boundary():
    exits = []
    watchdog = RuntimeWatchdog(stale_after_s=0.01, interval_s=0.001, exit_fn=exits.append)
    watchdog.beat("waiting_for_round")
    watchdog.start()
    try:
        time.sleep(0.03)
        assert exits == []
    finally:
        watchdog.stop()


def test_runtime_watchdog_does_not_restart_while_active_round_is_waiting():
    exits = []
    watchdog = RuntimeWatchdog(stale_after_s=0.01, interval_s=0.001, exit_fn=exits.append)
    watchdog.beat("active_round")
    watchdog.start()
    try:
        time.sleep(0.03)
        assert exits == []
    finally:
        watchdog.stop()


def test_runtime_watchdog_active_round_stall_alerts_without_exit(monkeypatch):
    exits = []
    alerts = []
    monkeypatch.setattr(runner_module, "RUNTIME_ACTIVE_STAGE_TIMEOUT_S", 0.02)
    watchdog = RuntimeWatchdog(
        stale_after_s=0.01,
        interval_s=0.001,
        exit_fn=exits.append,
        fatal_callback=lambda reason, payload: alerts.append((reason, payload)),
    )
    watchdog.beat("active_round")
    watchdog.start()
    try:
        deadline = time.monotonic() + 0.5
        while not exits and time.monotonic() < deadline:
            time.sleep(0.005)
        assert alerts
        assert exits == []
    finally:
        watchdog.stop()


def test_audit_persistence_failure_does_not_suppress_transport_alert(tmp_path, monkeypatch):
    store = StateStore(tmp_path / "state.sqlite3")
    notifier = CaptureNotifier()

    def fail_audit(*_args, **_kwargs):
        raise OSError("injected disk failure")

    try:
        monkeypatch.setattr(store, "append_event", fail_audit)
        monkeypatch.setattr(runner_module, "append_jsonl", fail_audit)
        runner_module._audit_asset_transport_error(
            Settings(out_dir=tmp_path / "out", state_db=tmp_path / "state.sqlite3"),
            store,
            asset="BTC",
            slug="btc-updown-5m-900",
            phase="market_stream",
            exc=TimeoutError("wire timed out"),
            notifier=notifier,
        )

        assert len(notifier.messages) == 1
        assert notifier.messages[0].startswith("[Aftertake] ALERT")
    finally:
        store.close()


def test_fatal_restart_runs_after_diagnostics_persist(tmp_path):
    events = []

    runner_module._run_diagnostics_then_restart(
        lambda: events.append("restart"),
        lambda: events.append("alert_outbox_persisted"),
        grace_s=0.1,
    )

    assert events == ["alert_outbox_persisted", "restart"]


def test_heartbeat_fatal_alerts_without_process_restart(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    notifier = CaptureNotifier()

    try:
        runner_module._report_heartbeat_fatal(
            settings=Settings(state_db=tmp_path / "state.sqlite3"),
            store=store,
            notifier=notifier,
            reason="heartbeat status queue is full",
            payload={"consecutive_failures": 3},
        )
        assert notifier.messages[0].startswith("[Aftertake] ALERT")
    finally:
        store.close()


def test_executor_timeout_alerts_without_process_restart(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    notifier = CaptureNotifier()
    executor = OrderExecutor(Settings(state_db=tmp_path / "state.sqlite3"), store)
    executor._mark_read_probe_stalled("get_order probe exceeded reconciliation deadline")
    try:
        assert runner_module._report_executor_timeout(
            settings=Settings(state_db=tmp_path / "state.sqlite3"),
            store=store,
            notifier=notifier,
            executor=executor,
        ) is True
        assert notifier.messages[0].startswith("[Aftertake] ALERT")
    finally:
        store.close()


def test_live_asset_notification_transport_never_blocks_asset_worker(tmp_path, monkeypatch):
    store = StateStore(tmp_path / "state.sqlite3")
    notifier = Notifier(token="test-token", chat_id="test-chat")
    started = threading.Event()
    release = threading.Event()
    worker_returned = threading.Event()

    def slow_send(_text):
        started.set()
        release.wait(timeout=2.0)
        return True

    monkeypatch.setattr(notifier, "send", slow_send)

    def asset_worker():
        runner_module._safe_notify(
            notifier,
            Settings(out_dir=tmp_path / "out", state_db=tmp_path / "state.sqlite3"),
            store,
            "alert",
            {"reason": "injected"},
        )
        worker_returned.set()

    worker = threading.Thread(target=asset_worker, name="aftertake-asset-test")
    try:
        worker.start()
        assert started.wait(1.0)
        assert worker_returned.wait(0.1), "Telegram blocked the close-critical asset worker"
    finally:
        release.set()
        worker.join(timeout=1.0)
        deadline = time.monotonic() + 1.0
        while time.monotonic() < deadline:
            count = store._conn.execute(
                "SELECT COUNT(*) FROM audit_events WHERE kind = 'notification_sent'"
            ).fetchone()[0]
            if count:
                break
            time.sleep(0.01)
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
    current = [1494.0]

    def sleep(seconds):
        slept.append(seconds)
        current[0] += seconds

    selected = _select_next_round_start(
        now=1494.0,
        processed_round_starts=processed,
        sleep=sleep,
        clock=lambda: current[0],
    )

    assert selected == 1500
    assert slept == [6.0]


def test_round_scheduler_requires_enough_lead_for_bounded_multi_asset_preflight():
    slept = []
    current = [1381.0]

    def sleep(seconds):
        slept.append(seconds)
        current[0] += seconds

    selected = _select_next_round_start(
        now=1381.0,  # 119 seconds remain; live preflight requires 120.
        processed_round_starts=set(),
        sleep=sleep,
        clock=lambda: current[0],
    )

    assert selected == 1500
    assert slept == [119.0]


def test_round_scheduler_rechecks_clock_after_oversleep_and_skips_stale_boundary():
    slept = []
    current = [1494.0]

    def sleep(seconds):
        slept.append(seconds)
        # Simulate a suspended process waking 185 seconds late.
        current[0] += seconds + (185.0 if len(slept) == 1 else 0.0)

    selected = _select_next_round_start(
        now=1494.0,
        processed_round_starts=set(),
        sleep=sleep,
        clock=lambda: current[0],
    )

    assert selected == 1800
    assert slept == [6.0, 115.0]


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
        assert any(message.startswith("[Aftertake] RECOVERY_SUCCESS") for message in notifier.messages)
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
                "[Aftertake] BOOT\nmode=LIVE qty=50.0000 assets=BTC,ETH,XRP,HYPE,DOGE,SOL\n"
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


def test_sigterm_raises_system_exit_and_closes_runtime_resources(monkeypatch, tmp_path):
    settings = Settings(
        dry_run=True,
        out_dir=tmp_path / "out",
        state_db=tmp_path / "state.sqlite3",
    )
    store = StateStore(settings.state_db)
    closed = []
    original_store_close = store.close

    def close_store():
        closed.append("store")
        original_store_close()

    store.close = close_store

    class FakeExecutor:
        def __init__(self, **_kwargs):
            self.closed = False

        def close(self):
            self.closed = True
            closed.append("executor")

    class NoLock:
        def __init__(self, _path):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    registered = []

    def capture_signal(signum, handler):
        if signum == signal.SIGTERM:
            registered.append(handler)

    monkeypatch.setattr(runner_module.Settings, "from_env", staticmethod(lambda: settings))
    monkeypatch.setattr(runner_module, "StateStore", lambda _path: store)
    monkeypatch.setattr(runner_module, "OrderExecutor", FakeExecutor)
    monkeypatch.setattr(runner_module, "RuntimeLock", NoLock)
    monkeypatch.setattr(runner_module, "PolymarketPublicClient", lambda **_kwargs: object())
    monkeypatch.setattr(runner_module, "Notifier", lambda **_kwargs: CaptureNotifier())
    monkeypatch.setattr(runner_module.signal, "signal", capture_signal)
    monkeypatch.setattr(
        runner_module.os,
        "_exit",
        lambda code: (_ for _ in ()).throw(SystemExit(code)),
    )

    def stop_round(**_kwargs):
        assert registered
        registered[0](signal.SIGTERM, None)

    monkeypatch.setattr(runner_module, "_run_round_loop", stop_round)

    with pytest.raises(SystemExit) as exc_info:
        runner_module.main(["--forever"])

    assert exc_info.value.code == 0
    assert closed == ["executor", "store"]


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


def test_startup_reconciliation_keeps_invalid_order_ambiguous_and_alerts_once(tmp_path):
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
        assert record is not None
        store.mark_submitted(record.intent_id, "old-order", {"order_type": "GTC"})
        record = store.unresolved_orders()[0]
        notifier = CaptureNotifier()
        settings = Settings(
            dry_run=False,
            order_type="GTC",
            out_dir=tmp_path / "out",
            state_db=tmp_path / "state.sqlite3",
        )
        class InvalidOrderGateway:
            def get_order(self, _order_id):
                raise RuntimeError("CLOB returned an invalid order lookup")

        executor = OrderExecutor(settings, store, gateway=InvalidOrderGateway())

        for boot_number in (1, 2):
            _reconcile_startup(settings, store, executor, notifier)

            unresolved = store.unresolved_orders()
            assert len(unresolved) == 1, "boot %s discarded ambiguous exposure" % boot_number
            assert unresolved[0].intent_id == record.intent_id
            assert unresolved[0].state == "execution_unknown"
            assert store.has_execution_unknown() is True

        assert len(notifier.messages) == 1
        assert all(message.startswith("[Aftertake] ALERT") for message in notifier.messages)
        assert all("order_id" in message.lower() for message in notifier.messages)
        assert store.component_status("startup_reconciliation:%s" % record.intent_id) == "unhealthy"
    finally:
        store.close()


def test_dry_run_runner_emits_runtime_ready_before_scheduler(tmp_path, monkeypatch):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        settings = Settings(
            dry_run=True,
            assets=("BTC",),
            out_dir=tmp_path / "out",
            state_db=tmp_path / "state.sqlite3",
        )
        notifier = CaptureNotifier()
        monkeypatch.setattr(
            runner_module,
            "_run_asset_rounds",
            lambda **_kwargs: {
                "BTC": [runner_module.PostCloseDecision("hold", "no_candidate")]
            },
        )

        _run_round_loop(
            settings=settings,
            store=store,
            public=FakePublic(),
            notifier=notifier,
            forever=False,
            rounds=1,
            wait_for_next_boundary=lambda: 900,
            sleep=lambda _seconds: None,
        )

        assert [message.splitlines()[0] for message in notifier.messages] == [
            "[Aftertake] RUNTIME_READY"
        ]
        payload = store._conn.execute(
            "SELECT payload_json FROM audit_events WHERE kind = 'runtime_ready'"
        ).fetchone()
        assert json.loads(payload["payload_json"])["dry_run"] is True
        lifecycle_rows = store._conn.execute(
            """SELECT kind, payload_json FROM audit_events
               WHERE kind IN ('round_started', 'round_complete') ORDER BY id"""
        ).fetchall()
        assert [row["kind"] for row in lifecycle_rows] == [
            "round_started",
            "round_complete",
        ]
        started = json.loads(lifecycle_rows[0]["payload_json"])
        completed = json.loads(lifecycle_rows[1]["payload_json"])
        assert started == {"assets": ["BTC"], "round_start": 900}
        assert completed == {
            "asset_results": {
                "BTC": {
                    "decision_count": 1,
                    "final_action": "hold",
                    "final_reason": "no_candidate",
                    "qualified_candidate": False,
                }
            },
            "assets": ["BTC"],
            "missing_assets": [],
            "qualified_assets": [],
            "round_start": 900,
        }
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


def test_reconcile_submitted_orders_stops_after_uncancellable_timeout(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        records = []
        for index in (1, 2):
            record = store.reserve_entry(
                "btc-updown-5m-%s" % (900 + index),
                "condition-%s" % index,
                900 + index,
                "up-token",
                "YES",
                5,
                0.64,
            )
            assert record is not None
            store.mark_submitted(record.intent_id, "order-live-%s" % index, {})
            records.append(record)

        class TimeoutExecutor:
            read_probe_stalled = False

            def __init__(self):
                self.calls = 0

            def reconcile_existing_once(self, record):
                self.calls += 1
                self.read_probe_stalled = True
                return OrderExecutor(
                    Settings(dry_run=False),
                    StateStore.__new__(StateStore),
                    gateway=object(),
                )._result_from_record(
                    record,
                    "execution_unknown",
                    0.0,
                    0.0,
                    False,
                    "unknown",
                    "reconcile_transport_timeout",
                    {"reconcile_transport_error": True},
                )

        executor = TimeoutExecutor()
        results = reconcile_submitted_orders(
            settings=Settings(
                dry_run=False,
                order_type="GTC",
                out_dir=tmp_path / "out",
                state_db=tmp_path / "state.sqlite3",
            ),
            store=store,
            executor=executor,
            notifier=CaptureNotifier(),
        )

        assert executor.calls == 1
        assert len(results) == 1
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


@pytest.mark.parametrize("slow_component", ["reconcile", "settlement"])
def test_slow_post_round_maintenance_does_not_block_next_round_scheduler(
    slow_component, monkeypatch, tmp_path
):
    store = StateStore(tmp_path / slow_component / "state.sqlite3")
    release_maintenance = threading.Event()
    maintenance_started = threading.Event()
    second_scan_started = threading.Event()
    runner_errors = []
    scanned_rounds = []
    try:
        settings = Settings(
            dry_run=False,
            assets=("BTC",),
            order_type="GTC",
            out_dir=tmp_path / slow_component / "out",
            state_db=tmp_path / slow_component / "state.sqlite3",
        )

        def live_runtime(*_args):
            gateway = InstantGateway()
            return gateway, OrderExecutor(settings, store, gateway=gateway)

        boundaries = iter((900, 1200))

        def scan_assets(*, round_start, **_kwargs):
            scanned_rounds.append(round_start)
            if round_start == 1200:
                second_scan_started.set()
            return {}

        def fast_maintenance(**_kwargs):
            return []

        def slow_maintenance(**_kwargs):
            maintenance_started.set()
            release_maintenance.wait(timeout=2.0)
            return []

        monkeypatch.setattr(runner_module, "_run_asset_rounds", scan_assets)
        monkeypatch.setattr(
            runner_module,
            "reconcile_submitted_orders",
            slow_maintenance if slow_component == "reconcile" else fast_maintenance,
        )
        monkeypatch.setattr(
            runner_module,
            "settle_open_positions",
            slow_maintenance if slow_component == "settlement" else fast_maintenance,
        )

        def run_loop():
            try:
                _run_round_loop(
                    settings=settings,
                    store=store,
                    public=LivePublic(),
                    notifier=CaptureNotifier(),
                    forever=False,
                    rounds=2,
                    live_runtime_factory=live_runtime,
                    wait_for_next_boundary=lambda: next(boundaries),
                    sleep=lambda _seconds: None,
                )
            except BaseException as exc:
                runner_errors.append(exc)

        runner_thread = threading.Thread(target=run_loop, daemon=True)
        runner_thread.start()
        assert maintenance_started.wait(1.0)
        try:
            assert second_scan_started.wait(0.25), (
                "%s blocked discovery of the next five-minute round" % slow_component
            )
        finally:
            release_maintenance.set()
            runner_thread.join(timeout=2.0)

        assert runner_errors == []
        assert runner_thread.is_alive() is False
        assert scanned_rounds == [900, 1200]
    finally:
        release_maintenance.set()
        store.close()


def test_uncancellable_maintenance_alerts_without_process_restart(tmp_path, monkeypatch):
    store = StateStore(tmp_path / "state.sqlite3")
    release = threading.Event()

    def hung_settlement(**_kwargs):
        release.wait(timeout=1.0)
        return []

    monkeypatch.setattr(runner_module, "settle_open_positions", hung_settlement)
    worker = runner_module._MaintenanceWorker(
        settings=Settings(out_dir=tmp_path / "out", state_db=tmp_path / "state.sqlite3"),
        store=store,
        public=FakePublic(),
        notifier=CaptureNotifier(),
        timeout_s=0.03,
    )
    try:
        assert worker.trigger(OrderExecutor(Settings(), store)) is True
        time.sleep(0.1)
        deadline = time.monotonic() + 0.5
        while (
            store.component_status("maintenance_worker") != "unhealthy"
            and time.monotonic() < deadline
        ):
            time.sleep(0.005)
        assert store.component_status("maintenance_worker") == "unhealthy"
    finally:
        release.set()
        worker.wait(1.0)
        store.close()
