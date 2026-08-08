from __future__ import annotations

from aftertake.config import Settings
from aftertake.execution import OrderExecutor
from aftertake.pm_client import GammaMarket
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
