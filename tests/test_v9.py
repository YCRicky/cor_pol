import pytest

from aftertake.config import Settings
from aftertake.execution import OrderExecutor
from aftertake.market_stream import MarketBookStream
from aftertake.pm_client import (
    BalanceAllowance,
    GammaMarket,
    LivePreflight,
    LivePreflightError,
    MarketMetadata,
)
from aftertake.post_close import PairedBook, SideBook
from aftertake.runner import run_round
from aftertake.state import StateStore
from aftertake.v9 import V9DualLaneClassifier


def _side(bid, ask, *, bid_size=10.0, near=10.0, asks=None):
    ask_levels = ((ask, 10.0),) if asks is None and ask is not None else tuple(asks or ())
    return SideBook(bid, bid_size, near, ask, ask_levels[0][1] if ask_levels else 0.0, near, ask_levels)


def _book(
    ts,
    *,
    yes_bid=0.30,
    no_bid=0.14,
    yes_ask=0.35,
    no_ask=0.80,
    yes_asks=((0.35, 5.0), (0.99, 5.0)),
    no_asks=((0.80, 10.0),),
):
    return PairedBook(
        observed_at=ts,
        source_timestamp=ts - 0.001,
        yes=_side(yes_bid, yes_ask, asks=yes_asks),
        no=_side(no_bid, no_ask, asks=no_asks),
        yes_updated_at=ts,
        no_updated_at=ts,
    )


def test_v9_residual_lane_uses_marketable_099_ceiling_and_full_ask_ladder():
    classifier = V9DualLaneClassifier(settlement_label="binary_up_down")
    classifier.record(_book(1000.010))

    decision = classifier.evaluate(round_end_ts=1000.0, now_ts=1000.020, qty=5.0)

    assert decision.action == "enter"
    assert decision.side == "YES"
    assert decision.audit["lane"] == "R"
    assert decision.entry_ask == 0.99
    assert decision.entry_ask_size == 10.0
    assert decision.audit["would_enter"] is True
    assert decision.audit["post_order_allowed"] is True
    assert decision.audit["entry"]["levels"] == [(0.35, 5.0), (0.99, 5.0)]
    assert decision.audit["winner"]["bid_depth"] == 10.0
    assert decision.audit["event_ts"] == 1000.009
    assert decision.audit["receive_ts"] == 1000.010


def test_v9_sweep_lane_requires_two_fresh_books_and_settlement_semantics():
    classifier = V9DualLaneClassifier(settlement_label="binary_up_down")
    classifier.record(_book(1000.060))
    classifier.record(_book(1000.080))

    lanes = classifier.evaluate_lanes(round_end_ts=1000.0, now_ts=1000.090, qty=5.0)

    assert lanes["S"].action == "enter"
    assert lanes["S"].audit["lane"] == "S"
    assert lanes["S"].confirmations == 2
    assert lanes["S"].audit["settlement_label"] == "binary_up_down"

    unknown = V9DualLaneClassifier(settlement_label="unverified")
    unknown.record(_book(1000.060))
    unknown.record(_book(1000.080))
    assert unknown.evaluate_lanes(round_end_ts=1000.0, now_ts=1000.090, qty=5.0)["S"].reason == (
        "settlement_semantics_unverified"
    )


def test_v9_does_not_require_empty_loser_but_aborts_on_quantitative_reclaim():
    classifier = V9DualLaneClassifier(settlement_label="binary_up_down")
    classifier.record(_book(1000.010, no_bid=0.28))
    classifier.record(_book(1000.020, no_bid=0.14))

    decision = classifier.evaluate(round_end_ts=1000.0, now_ts=1000.030, qty=5.0)

    assert decision.action == "hold"
    assert decision.reason == "r_loser_reclaim_abort"
    assert decision.audit["would_enter"] is False


def test_v9_fails_closed_without_full_ask_levels():
    classifier = V9DualLaneClassifier(settlement_label="binary_up_down")
    classifier.record(_book(1000.010, yes_asks=()))

    decision = classifier.evaluate(round_end_ts=1000.0, now_ts=1000.020, qty=5.0)

    assert decision.action == "hold"
    assert decision.reason == "r_ask_levels_missing"


def test_market_stream_preserves_all_executable_ask_levels():
    snapshots = []
    stream = MarketBookStream(
        yes_token_id="yes-token", no_token_id="no-token", on_book=snapshots.append
    )
    stream.process_message(
        {
            "type": "book",
            "payload": {
                "tokenId": "yes-token",
                "bids": [{"price": "0.70", "size": "10"}],
                "asks": [
                    {"price": "0.35", "size": "5"},
                    {"price": "0.99", "size": "5"},
                    {"price": "1.00", "size": "20"},
                ],
            },
        },
        received_at=1000.01,
    )
    stream.process_message(
        {
            "type": "book",
            "payload": {
                "tokenId": "no-token",
                "bids": [{"price": "0.20", "size": "10"}],
                "asks": [{"price": "0.80", "size": "10"}],
            },
        },
        received_at=1000.02,
    )

    assert snapshots[-1].yes.ask_levels == ((0.35, 5.0), (0.99, 5.0), (1.0, 20.0))


def test_v9_live_runner_requires_explicit_live_flag(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    settings = Settings(
        dry_run=False,
        strategy_family="v9",
        v9_live_enabled=False,
        out_dir=tmp_path / "out",
        state_db=tmp_path / "state.sqlite3",
    )
    try:
        with pytest.raises(LivePreflightError, match="AFTERTAKE_STRATEGY=twap_tail_v2"):
            run_round(
                settings=settings,
                store=store,
                public=object(),
                executor=OrderExecutor(settings, store),
                live_gateway=None,
                round_start=900,
            )
    finally:
        store.close()


class _V9Public:
    def market_by_slug(self, slug, allow_closed=False):
        return GammaMarket(
            slug=slug,
            condition_id="condition-v9",
            outcomes=("Up", "Down"),
            clob_token_ids=("up-token", "down-token"),
            active=True,
        )

    def geoblock_status(self, endpoint):
        return type("Geo", (), {"blocked": False, "country": "", "region": "", "ip": ""})()


class _V9Stream:
    def __init__(self, *, on_book, **_kwargs):
        self._on_book = on_book
        self.ready = False
        self.last_error = ""

    def start(self):
        self._on_book(
            _book(
                1200.40,
                yes_bid=0.85,
                no_bid=0.70,
                yes_asks=((0.99, 60.0),),
                no_asks=((0.99, 60.0),),
            )
        )
        self.ready = True

    def close(self):
        return None


class _V9Gateway:
    def __init__(self):
        self.posts = []

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
            collateral=BalanceAllowance(balance=100.0, allowance=100.0, raw={}),
            closed_only=False,
        )

    def submit_limit_buy_fast(self, token_id, price, qty, metadata, order_type="FAK"):
        self.posts.append((token_id, price, qty, order_type))
        return {"orderID": "v9-order-1"}

    def get_order(self, order_id):
        qty = self.posts[-1][2]
        return {"id": order_id, "status": "matched", "size_matched": str(qty), "average_price": "0.35"}

    def order_trades(self, token_id, order_id):
        qty = self.posts[-1][2]
        return [{"order_id": order_id, "size": str(qty), "price": "0.35", "feeUsdc": "0"}]

    def cancel_order(self, order_id):
        raise AssertionError("matched V9 order must not be cancelled")

    def post_heartbeat(self, heartbeat_id=""):
        return {"heartbeat_id": "v9-heartbeat"}


def test_v9_runner_cannot_reach_live_post_after_twap_tail_cutover(tmp_path):
    store = StateStore(tmp_path / "state.sqlite3")
    gateway = _V9Gateway()
    settings = Settings(
        dry_run=False,
        strategy_family="v9",
        v9_live_enabled=True,
        order_type="GTC",
        out_dir=tmp_path / "out",
        state_db=tmp_path / "state.sqlite3",
    )
    try:
        times = iter((1190.0, 1190.1, 1200.40, 1200.50, 1200.50))
        with pytest.raises(LivePreflightError, match="AFTERTAKE_STRATEGY=twap_tail_v2"):
            run_round(
                settings=settings,
                store=store,
                public=_V9Public(),
                executor=OrderExecutor(settings, store, gateway=gateway, wall_clock=lambda: 1200.50),
                live_gateway=gateway,
                round_start=900,
                clock=lambda: next(times, 1200.50),
                sleep=lambda _: None,
                stream_factory=_V9Stream,
            )

        assert gateway.posts == []
    finally:
        store.close()
