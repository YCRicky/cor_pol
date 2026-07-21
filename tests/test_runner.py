import pytest

from misprice_pm.config import Settings
from misprice_pm.execution import OrderExecutor
from misprice_pm.pm_client import GammaMarket, GeoStatus, MarketMetadata
from misprice_pm.runner import SpotObservation, run_round, settle_open_positions, settle_slug
from misprice_pm.state import StateStore
from misprice_pm.strategy import BookSnapshot


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


class PreflightPublic(FakePublic):
    def geoblock_status(self, endpoint):
        return GeoStatus(blocked=False, country="KR", region="", ip="")


class PreflightGateway:
    def market_metadata(self, condition_id):
        return MarketMetadata(
            condition_id=condition_id,
            tick_size="0.01",
            min_order_size=1,
            neg_risk=False,
            fee_rate=0.07,
            tokens={"up": "up-token", "down": "down-token"},
            raw={},
        )

    def preflight(self, geo, required_cash):
        return None


class InstantFillGateway(PreflightGateway):
    def submit_limit_buy(self, token_id, price, qty, metadata):
        return {"orderID": "order-live-1", "status": "matched"}

    def get_order(self, order_id):
        return {
            "id": order_id,
            "status": "matched",
            "size_matched": "5",
            "average_price": "0.60",
        }

    def cancel_order(self, order_id):
        raise AssertionError("fully matched order must not be cancelled")

    def order_trades(self, token_id, order_id):
        return [
            {
                "order_id": order_id,
                "size": "5",
                "price": "0.60",
                "fee_rate_bps": "700",
            }
        ]

    def post_heartbeat(self, heartbeat_id=""):
        return {"heartbeat_id": "heartbeat-1"}


class CaptureNotifier:
    enabled = True

    def __init__(self):
        self.messages = []

    def send(self, text):
        self.messages.append(text)
        return True


def test_fresh_round_records_one_shadow_entry_at_most(monkeypatch, tmp_path):
    observations = iter([SpotObservation(100.0, 0.0), SpotObservation(100.06, 40.0)])
    monkeypatch.setattr("misprice_pm.runner.binance_price", lambda *args, **kwargs: next(observations))
    monkeypatch.setattr(
        "misprice_pm.runner.book_pair_snapshot",
        lambda *args, **kwargs: BookSnapshot(
            yes_bid=0.58,
            yes_ask=0.59,
            yes_ask_size=20.0,
            no_bid=0.39,
            no_ask=0.40,
            no_ask_size=20.0,
            age_s=0.1,
        ),
    )
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        settings = Settings(dry_run=True, out_dir=tmp_path / "out", state_db=tmp_path / "state.sqlite3")
        decisions = run_round(
            settings=settings,
            store=store,
            public=FakePublic(),
            executor=OrderExecutor(settings, store),
            live_gateway=None,
            round_start=0,
            clock=lambda: 40.0,
            sleep=lambda _: None,
            max_ticks=1,
        )

        assert len(decisions) == 1
        assert decisions[0].action == "enter"
        assert store.unresolved_orders() == []
        # The reservation is terminal after the shadow path, so a second
        # process cannot re-enter the same slug.
        assert store.reserve_entry("btc-updown-5m-0", "condition", 0, "up-token", "YES", 5, 0.59) is None
    finally:
        store.close()


def test_late_round_open_capture_refuses_to_trade(monkeypatch, tmp_path):
    monkeypatch.setattr(
        "misprice_pm.runner.binance_price", lambda *args, **kwargs: SpotObservation(100.0, 4.0)
    )
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        settings = Settings(max_open_capture_delay_s=3, out_dir=tmp_path / "out", state_db=tmp_path / "state.sqlite3")
        with pytest.raises(RuntimeError, match="too late"):
            run_round(
                settings=settings,
                store=store,
                public=FakePublic(),
                executor=OrderExecutor(settings, store),
                live_gateway=None,
                round_start=0,
                clock=lambda: 0.0,
                sleep=lambda _: None,
                max_ticks=1,
            )
    finally:
        store.close()


def test_confirmed_open_fill_is_settled_from_pm_outcome(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        record = store.reserve_entry(
            "btc-updown-5m-0",
            "condition",
            0,
            "up-token",
            "YES",
            5,
            0.6,
            fee_rate=0.07,
        )
        store.mark_terminal_execution(
            record.intent_id,
            5,
            0.6,
            {"trades": [{"feeUsdc": "0.084"}]},
            "matched",
        )
        settings = Settings(out_dir=tmp_path / "out", state_db=tmp_path / "state.sqlite3")

        notifier = CaptureNotifier()
        settled = settle_open_positions(
            settings=settings,
            store=store,
            public=ResolvedPublic(),
            notifier=notifier,
        )

        assert len(settled) == 1
        assert settled[0]["settlement_source"] == "pm"
        assert settled[0]["entry_fee"] == 0.084
        assert store.market_state("btc-updown-5m-0") == "settled"
        assert notifier.messages[0].startswith("[Misprice PM] SETTLE")
        assert "result=WIN" in notifier.messages[0]
    finally:
        store.close()


def test_manual_settlement_uses_persisted_fill_not_cli_overrides(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        record = store.reserve_entry(
            "btc-updown-5m-0",
            "condition",
            0,
            "up-token",
            "YES",
            5,
            0.6,
            fee_rate=0.07,
        )
        store.mark_terminal_execution(
            record.intent_id,
            5,
            0.6,
            {"trades": [{"feeUsdc": "0.084"}]},
            "matched",
        )
        settings = Settings(out_dir=tmp_path / "out", state_db=tmp_path / "state.sqlite3")

        result = settle_slug(
            settings=settings,
            public=ResolvedPublic(),
            store=store,
            slug="btc-updown-5m-0",
            side="NO",
            entry_price=0.1,
            qty=99,
            entry_fee=99,
        )

        assert result["side"] == "YES"
        assert result["entry_price"] == 0.6
        assert result["qty"] == 5
        assert result["entry_fee"] == 0.084
    finally:
        store.close()


def test_live_preflight_rechecks_book_and_blocks_collapsed_lag(monkeypatch, tmp_path):
    observations = iter(
        [
            SpotObservation(100.0, 0.0),
            SpotObservation(100.06, 40.0),
            SpotObservation(100.06, 40.0),
        ]
    )
    books = iter(
        [
            BookSnapshot(0.48, 0.49, 20.0, 0.50, 0.51, 20.0, 0.1),
            BookSnapshot(0.49, 0.50, 20.0, 0.49, 0.50, 20.0, 0.1),
            BookSnapshot(0.69, 0.70, 20.0, 0.29, 0.30, 20.0, 0.1),
            BookSnapshot(0.49, 0.50, 20.0, 0.49, 0.50, 20.0, 0.1),
            BookSnapshot(0.69, 0.70, 20.0, 0.29, 0.30, 20.0, 0.1),
        ]
    )
    monkeypatch.setattr("misprice_pm.runner.binance_price", lambda *args, **kwargs: next(observations))
    monkeypatch.setattr(
        "misprice_pm.runner.book_pair_snapshot", lambda *args, **kwargs: next(books)
    )
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        settings = Settings(out_dir=tmp_path / "out", state_db=tmp_path / "state.sqlite3")
        notifier = CaptureNotifier()
        decisions = run_round(
            settings=settings,
            store=store,
            public=PreflightPublic(),
            executor=OrderExecutor(settings, store),
            live_gateway=PreflightGateway(),
            round_start=0,
            clock=lambda: 40.0,
            sleep=lambda _: None,
            max_ticks=2,
            notifier=notifier,
        )

        assert [decision.action for decision in decisions] == ["enter", "enter"]
        assert store.market_state("btc-updown-5m-0") == "observing"
        assert notifier.messages[0].startswith("[Misprice PM] ENTRY_BLOCKED")
        assert len(notifier.messages) == 1
    finally:
        store.close()


def test_strategy_to_live_fill_emits_entry_notification_only(
    monkeypatch, tmp_path
):
    observations = iter([SpotObservation(100.0, 0.0), SpotObservation(100.06, 40.0)])
    monkeypatch.setattr(
        "misprice_pm.runner.binance_price", lambda *args, **kwargs: next(observations)
    )
    monkeypatch.setattr(
        "misprice_pm.runner.book_pair_snapshot",
        lambda *args, **kwargs: BookSnapshot(
            yes_bid=0.58,
            yes_ask=0.59,
            yes_ask_size=20.0,
            no_bid=0.39,
            no_ask=0.40,
            no_ask_size=20.0,
            age_s=0.1,
        ),
    )
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        settings = Settings(
            dry_run=False,
            out_dir=tmp_path / "out",
            state_db=tmp_path / "state.sqlite3",
        )
        gateway = InstantFillGateway()
        notifier = CaptureNotifier()
        decisions = run_round(
            settings=settings,
            store=store,
            public=PreflightPublic(),
            executor=OrderExecutor(settings, store, gateway=gateway),
            live_gateway=gateway,
            round_start=0,
            clock=lambda: 40.0,
            sleep=lambda _: None,
            max_ticks=1,
            notifier=notifier,
        )

        assert decisions[0].action == "enter"
        assert store.market_state("btc-updown-5m-0") == "open"
        assert [message.splitlines()[0] for message in notifier.messages] == [
            "[Misprice PM] ENTRY_CONFIRMED",
        ]
        assert "order=order-live-1" in notifier.messages[0]
        assert "status=matched" in notifier.messages[0]
    finally:
        store.close()
