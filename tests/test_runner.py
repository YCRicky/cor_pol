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
from aftertake.runner import (
    _probe_stream,
    _run_round_loop,
    _select_next_round_start,
    deployment_check,
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

    def __init__(self, *, on_book, **_kwargs):
        self._on_book = on_book
        self.ready = False
        self.last_error = ""

    def start(self):
        def add(ts, yes_bid, yes_ask, no_bid, no_ask, *, yes_size=20, no_size=20, yes_near=None, no_near=None):
            self._on_book(
                PairedBook(
                    observed_at=ts,
                    yes=SideBook(yes_bid, yes_size, yes_size, yes_ask, 20, yes_size if yes_near is None else yes_near),
                    no=SideBook(no_bid, no_size, no_size, no_ask, 20, no_size if no_near is None else no_near),
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


class DeepSupportPairedStream(PairedStream):
    no_bid_size = 40
    no_near_depth = 40


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

    def submit_limit_buy_fast(self, token_id, price, qty, metadata):
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
            clock=lambda: 1200.35,
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


def test_live_round_emits_only_execution_lifecycle_messages(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        settings = Settings(dry_run=False, out_dir=tmp_path / "out", state_db=tmp_path / "state.sqlite3")
        notifier = CaptureNotifier()
        gateway = InstantGateway()
        timestamps = iter((1190.0, 1190.1, 1200.35))
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
            stream_factory=DeepSupportPairedStream,
        )
        assert any(item.action == "enter" for item in decisions)
        assert store.market_state("btc-updown-5m-900") == "open"
        assert gateway.submitted_qty == 20
        assert [message.splitlines()[0] for message in notifier.messages] == ["[Aftertake] ENTRY_CONFIRMED"]
        assert "NO qty=20.0000" in notifier.messages[0]
        assert "take_price=0.6400" in notifier.messages[0]
        assert "available_size=20.0000" in notifier.messages[0]
    finally:
        store.close()


def test_live_round_blocks_dynamic_quantity_that_the_observed_bid_support_cannot_cover(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        settings = Settings(dry_run=False, out_dir=tmp_path / "out", state_db=tmp_path / "state.sqlite3")
        notifier = CaptureNotifier()
        gateway = InstantGateway()
        timestamps = iter((1190.0, 1190.1, 1200.35))
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
    notifier = CaptureNotifier()
    result = deployment_check(
        settings=Settings(dry_run=True),
        public=FakePublic(),
        gateway=None,
        notifier=notifier,
        clock=lambda: 1200.0,
        stream_factory=PairedStream,
    )
    assert result["websocket_verified"] is True
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
        settings = Settings(dry_run=False, out_dir=tmp_path / "out", state_db=tmp_path / "state.sqlite3")
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
    settings = Settings(dry_run=False, out_dir=tmp_path / "out", state_db=tmp_path / "state.sqlite3")
    notifier = CaptureNotifier()

    class NoLock:
        def __init__(self, _path):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    def assert_boot_then_stop(**_kwargs):
        assert notifier.messages == [
            "[Aftertake] BOOT\nmode=LIVE qty=5.0000\none-entry-per-market + SQLite recovery + CLOB V2 preflight"
        ]

    monkeypatch.setattr(runner_module.Settings, "from_env", staticmethod(lambda: settings))
    monkeypatch.setattr(runner_module, "RuntimeLock", NoLock)
    monkeypatch.setattr(runner_module, "PolymarketPublicClient", lambda **_kwargs: object())
    monkeypatch.setattr(runner_module, "Notifier", lambda **_kwargs: notifier)
    monkeypatch.setattr(runner_module, "_run_round_loop", assert_boot_then_stop)

    assert runner_module.main(["--forever"]) == 0


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
