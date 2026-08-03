from __future__ import annotations

import pytest

import aftertake.runner as runner_module
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
from aftertake.post_close_snapshot import (
    select_post_close_snapshot_signal,
)
from aftertake.runner import run_round
from aftertake.state import StateStore


def _book(ts: float, yes_bid: float | None, no_bid: float | None, *, source_timestamp=None) -> PairedBook:
    return PairedBook(
        observed_at=ts,
        source_timestamp=source_timestamp,
        yes=SideBook(yes_bid, 60, 60, 0.99, 60),
        no=SideBook(no_bid, 60, 60, 0.99, 60),
    )


def test_post_close_snapshot_uses_close_plus_half_second_and_latest_local_snapshot():
    decision = select_post_close_snapshot_signal(
        [
            _book(100.40, 0.81, 0.70, source_timestamp=None),
            _book(100.60, 0.20, 0.90, source_timestamp=-999999),
        ],
        round_end_ts=100.0,
    )

    assert decision.action == "enter"
    assert decision.side == "YES"
    assert decision.entry_ask == 0.99
    assert decision.audit["post_close_snapshot_ts"] == pytest.approx(100.5)
    assert decision.audit["decision_ts"] == pytest.approx(100.5)
    assert decision.audit["snapshot_observed_ts"] == pytest.approx(100.4)
    assert decision.audit["snapshot_age_ms"] == pytest.approx(100.0)
    assert decision.audit["source_timestamp_used_for_gate"] is False


@pytest.mark.parametrize(
    ("observations", "reason"),
    [
        ([_book(100.40, 0.80, 0.70)], "post_close_leader_bid_not_strictly_above_threshold"),
        ([_book(100.40, 0.80, 0.80)], "post_close_snapshot_bid_tie"),
        ([_book(100.40, None, 0.80)], "post_close_leader_bid_not_strictly_above_threshold"),
        ([_book(100.40, None, None)], "post_close_snapshot_missing_or_invalid_bid"),
        ([_book(100.20, 0.90, 0.70)], "post_close_snapshot_stale"),
        ([_book(100.60, 0.90, 0.70)], "post_close_snapshot_no_paired_observation"),
    ],
)
def test_post_close_snapshot_hold_boundaries(observations, reason):
    decision = select_post_close_snapshot_signal(observations, round_end_ts=100.0)
    assert decision.action == "hold"
    assert decision.reason == reason


def test_post_close_snapshot_strict_threshold_and_not_due_boundary():
    before_due = select_post_close_snapshot_signal(
        [_book(100.49, 0.81, 0.70)],
        round_end_ts=100.0,
        decision_ts=100.499,
    )
    assert before_due.action == "hold"
    assert before_due.reason == "post_close_snapshot_not_due"

    eligible = select_post_close_snapshot_signal(
        [_book(100.50, 0.81, 0.70)],
        round_end_ts=100.0,
        decision_ts=100.500,
    )
    assert eligible.action == "enter"
    assert eligible.side == "YES"

    late_eligible = select_post_close_snapshot_signal(
        [_book(100.75, 0.81, 0.70)],
        round_end_ts=100.0,
        decision_ts=100.750,
    )
    assert late_eligible.action == "enter"
    too_late = select_post_close_snapshot_signal(
        [_book(100.751, 0.81, 0.70)],
        round_end_ts=100.0,
        decision_ts=100.751,
    )
    assert too_late.action == "hold"
    assert too_late.reason == "post_close_snapshot_decision_too_late"


@pytest.mark.parametrize(
    ("yes_bid", "no_bid", "expected_side"),
    [(0.99, None, "YES"), (None, 0.99, "NO")],
)
def test_post_close_snapshot_enters_when_only_one_side_has_a_valid_bid(
    yes_bid, no_bid, expected_side
):
    decision = select_post_close_snapshot_signal(
        [_book(100.49, yes_bid, no_bid)],
        round_end_ts=100.0,
    )
    assert decision.action == "enter"
    assert decision.side == expected_side
    assert decision.winner_bid == pytest.approx(0.99)
    assert decision.loser_bid is None


def test_post_close_snapshot_freeze_is_not_changed_by_later_leader_flip():
    decision = select_post_close_snapshot_signal(
        [_book(100.40, 0.85, 0.70), _book(100.75, 0.20, 0.95)],
        round_end_ts=100.0,
    )
    assert decision.action == "enter"
    assert decision.side == "YES"


class _Clock:
    def __init__(self, now: float):
        self.now = now
        self.sleep_calls: list[float] = []
        self.stream = None
        self.flipped = False

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleep_calls.append(seconds)
        self.now += max(0.0, float(seconds))
        if self.stream is not None and not self.flipped and self.now >= 300.5:
            self.stream.emit(_book(300.75, 0.20, 0.95))
            self.flipped = True


class _TimelineStream:
    def __init__(self, *, on_book, **_kwargs):
        self.on_book = on_book
        self.ready = False
        self.last_error = ""
        self.generation = 0
        self.reconnect_count = 0

    def start(self):
        self.on_book(_book(300.40, 0.81, 0.70))
        self.ready = True

    def emit(self, book):
        self.on_book(book)

    def close(self):
        return None


class _DecisionCutoffTimelineStream(_TimelineStream):
    def start(self):
        self.on_book(_book(300.75, 0.81, 0.70))
        self.ready = True


class _Public:
    def market_by_slug(self, slug, allow_closed=False):
        return GammaMarket(
            slug=slug,
            condition_id="condition",
            outcomes=("Up", "Down"),
            clob_token_ids=("up-token", "down-token"),
            active=True,
        )

    def geoblock_status(self, endpoint):
        return GeoStatus(blocked=False, country="TW", region="", ip="")


class _Gateway:
    def __init__(self, clock):
        self.clock = clock
        self.submits = []

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

    def submit_limit_buy_fast(self, token_id, price, qty, metadata, order_type="GTC"):
        self.submits.append(
            {
                "token_id": token_id,
                "price": price,
                "qty": qty,
                "order_type": order_type,
                "at": self.clock.now,
            }
        )
        return {"orderID": "order-1"}

    def get_order(self, order_id):
        return {"id": order_id, "status": "matched", "size_matched": "50", "average_price": "0.99"}

    def order_trades(self, token_id, order_id):
        return []

    def post_heartbeat(self, heartbeat_id=""):
        return {"heartbeat_id": "h"}


def test_live_runner_freezes_at_close_plus_half_second_and_submits_immediately(tmp_path, monkeypatch):
    clock = _Clock(299.0)
    stream = _TimelineStream(on_book=lambda _book: None)

    def stream_factory(**kwargs):
        nonlocal stream
        stream = _TimelineStream(**kwargs)
        clock.stream = stream
        return stream

    audit_rows = []
    monkeypatch.setattr(
        runner_module,
        "_audit",
        lambda settings, store, kind, payload, slug="": audit_rows.append((kind, payload, slug)),
    )
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        settings = Settings(
            dry_run=False,
            out_dir=tmp_path / "out",
            state_db=tmp_path / "state.sqlite3",
            min_seconds_between_entries=0,
        )
        gateway = _Gateway(clock)
        decisions = run_round(
            settings=settings,
            store=store,
            public=_Public(),
            executor=OrderExecutor(settings, store, gateway=gateway, wall_clock=clock),
            live_gateway=gateway,
            round_start=0,
            clock=clock,
            sleep=clock.sleep,
            stream_factory=stream_factory,
        )

        frozen = [
            payload for kind, payload, _slug in audit_rows if kind == "post_close_snapshot_frozen"
        ]
        assert frozen and frozen[0]["snapshot_decision_ts"] == pytest.approx(300.5)
        assert frozen[0]["post_close_snapshot_ts"] == pytest.approx(300.5)
        assert decisions[0].side == "YES"
        assert len(gateway.submits) == 1
        assert gateway.submits[0]["token_id"] == "up-token"
        assert gateway.submits[0]["price"] == pytest.approx(0.99)
        assert gateway.submits[0]["qty"] == pytest.approx(50.0)
        assert gateway.submits[0]["order_type"] == "GTC"
        assert gateway.submits[0]["at"] == pytest.approx(300.5)
        assert store.open_positions()[0].filled_qty == pytest.approx(50.0)
    finally:
        store.close()


def test_live_runner_allows_250ms_decision_lateness_and_submits_once(tmp_path):
    clock = _Clock(300.75)
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        settings = Settings(
            dry_run=False,
            out_dir=tmp_path / "out",
            state_db=tmp_path / "state.sqlite3",
            min_seconds_between_entries=0,
        )
        gateway = _Gateway(clock)
        run_round(
            settings=settings,
            store=store,
            public=_Public(),
            executor=OrderExecutor(settings, store, gateway=gateway, wall_clock=clock),
            live_gateway=gateway,
            round_start=0,
            clock=clock,
            sleep=clock.sleep,
            stream_factory=_DecisionCutoffTimelineStream,
        )
        assert len(gateway.submits) == 1
        assert gateway.submits[0]["at"] == pytest.approx(300.75)
    finally:
        store.close()


def test_live_runner_skips_after_decision_lateness_cutoff(tmp_path):
    clock = _Clock(300.751)
    store = StateStore(tmp_path / "state.sqlite3")
    try:
        settings = Settings(dry_run=True, out_dir=tmp_path / "out", state_db=tmp_path / "state.sqlite3")
        decisions = run_round(
            settings=settings,
            store=store,
            public=_Public(),
            executor=OrderExecutor(settings, store),
            live_gateway=None,
            round_start=0,
            clock=clock,
            sleep=clock.sleep,
            stream_factory=_DecisionCutoffTimelineStream,
        )
        assert any(item.reason == "post_close_snapshot_decision_too_late" for item in decisions)
        assert not store.open_positions()
    finally:
        store.close()
