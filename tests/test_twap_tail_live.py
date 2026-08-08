from __future__ import annotations

from aftertake.config import Settings
from aftertake.execution import OrderExecutor
from aftertake.pm_client import (
    BalanceAllowance,
    GammaMarket,
    GeoStatus,
    LivePreflight,
    MarketMetadata,
)
from aftertake.post_close import PairedBook, SideBook
from aftertake.runner import run_round
from aftertake.state import StateStore
from aftertake.twap_tail import TWAP_CUTOVER_TS, BinanceTailInput, BinanceTrade

ROUND_START = TWAP_CUTOVER_TS + 300
ROUND_END = ROUND_START + 300


class _Clock:
    def __init__(self, now):
        self.now = now

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.now += max(0.0, float(seconds))


class _Stream:
    def __init__(self, *, on_book, **_kwargs):
        self._on_book = on_book
        self.ready = False

    def start(self):
        self._on_book(
            PairedBook(
                observed_at=ROUND_END - 10.20,
                yes=SideBook(0.93, 10, 10, 0.95, 10),
                no=SideBook(0.06, 10, 10, 0.08, 10),
            )
        )
        self.ready = True

    def close(self):
        return None


class _Public:
    def market_by_slug(self, slug, allow_closed=False):
        return GammaMarket(
            slug=slug,
            condition_id="condition",
            outcomes=("Up", "Down"),
            clob_token_ids=("up-token", "down-token"),
            active=True,
            raw={"cryptoMarketConfig": {"twapEnabled": True, "twapLookbackSeconds": 30}},
        )

    def geoblock_status(self, _endpoint):
        return GeoStatus(blocked=False, country="", region="", ip="")


class _Gateway:
    def __init__(self):
        self.posts = []

    def preflight(self, geo, _required_cash):
        return LivePreflight(
            geo=geo,
            collateral=BalanceAllowance(balance=100.0, allowance=100.0, raw={}),
            closed_only=False,
        )

    def market_metadata(self, condition_id):
        return MarketMetadata(
            condition_id=condition_id,
            tick_size="0.01",
            min_order_size=1.0,
            neg_risk=False,
            fee_rate=0.0,
            tokens={"up": "up-token", "down": "down-token"},
            raw={},
        )

    def submit_limit_buy_fast(self, token_id, price, qty, metadata, order_type="GTC"):
        self.posts.append((token_id, price, qty, metadata.condition_id, order_type))
        return {"orderID": "twap-tail-order-1"}

    def get_order(self, order_id):
        return {
            "id": order_id,
            "status": "matched",
            "size_matched": "5",
            "average_price": "0.95",
        }

    def order_trades(self, _token_id, _order_id):
        return []


class _Proxy:
    def tail_input(self, asset, round_start):
        assert asset == "BTC"
        assert round_start == ROUND_START
        return BinanceTailInput(
            asset="BTC",
            round_start_ms=ROUND_START * 1000,
            complete_coverage=True,
            trades=(
                BinanceTrade((ROUND_START * 1000) + 10, (ROUND_START * 1000) + 10, 100.0),
                BinanceTrade((ROUND_END - 30) * 1000, (ROUND_END - 30) * 1000, 100.04),
                BinanceTrade((ROUND_END - 20) * 1000, (ROUND_END - 20) * 1000, 100.06),
                BinanceTrade((ROUND_END * 1000) - 10_300, (ROUND_END * 1000) - 10_300, 100.10),
            ),
        )


def test_live_path_uses_preclose_twap_tail_and_dry_run_never_sends_order(tmp_path):
    clock = _Clock(ROUND_END - 10.20)
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        settings = Settings(
            dry_run=True,
            out_dir=tmp_path / "out",
            state_db=tmp_path / "state.sqlite3",
            min_seconds_between_entries=0,
        )
        decisions = run_round(
            settings=settings,
            store=store,
            public=_Public(),
            executor=OrderExecutor(settings, store, wall_clock=clock),
            live_gateway=None,
            round_start=ROUND_START,
            clock=clock,
            sleep=clock.sleep,
            stream_factory=_Stream,
            binance_proxy=_Proxy(),
        )
        assert decisions[0].action == "enter"
        assert decisions[0].side == "YES"
        assert decisions[0].audit["strategy_version"] == "aftertake_twap_price_path_tail_v2"
        assert store.open_positions()[0].requested_qty == 5.0
    finally:
        store.close()


def test_live_path_rejects_late_scheduler_without_reservation(tmp_path):
    clock = _Clock(ROUND_END - 9.99)
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        settings = Settings(dry_run=True, out_dir=tmp_path / "out", state_db=tmp_path / "state.sqlite3")
        decisions = run_round(
            settings=settings,
            store=store,
            public=_Public(),
            executor=OrderExecutor(settings, store, wall_clock=clock),
            live_gateway=None,
            round_start=ROUND_START,
            clock=clock,
            sleep=clock.sleep,
            stream_factory=_Stream,
            binance_proxy=_Proxy(),
        )
        assert decisions[-1].reason == "tail_decision_too_late"
        assert not store.open_positions()
    finally:
        store.close()


def test_live_twap_tail_path_submits_exactly_one_gtc_order(tmp_path):
    clock = _Clock(ROUND_END - 10.20)
    store = StateStore(tmp_path / "state.sqlite3")
    gateway = _Gateway()
    try:
        settings = Settings(
            dry_run=False,
            order_type="GTC",
            out_dir=tmp_path / "out",
            state_db=tmp_path / "state.sqlite3",
            min_seconds_between_entries=0,
        )
        decisions = run_round(
            settings=settings,
            store=store,
            public=_Public(),
            executor=OrderExecutor(settings, store, gateway=gateway, wall_clock=clock),
            live_gateway=gateway,
            round_start=ROUND_START,
            clock=clock,
            sleep=clock.sleep,
            stream_factory=_Stream,
            binance_proxy=_Proxy(),
        )

        assert decisions[0].action == "enter"
        assert gateway.posts == [("up-token", 0.99, 5.0, "condition", "GTC")]
        assert store.open_positions()[0].order_id == "twap-tail-order-1"
    finally:
        store.close()
