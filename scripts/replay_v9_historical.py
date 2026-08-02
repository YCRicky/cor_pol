#!/usr/bin/env python3
"""Run a real chronological V9/V8.1 replay over local PMData Parquet files.

The runner is deliberately research-only.  It reads a local ``poly_l2`` cache,
reconstructs the book in PMData receive order, evaluates one candidate per
market and lane, and writes a small manifest/result report.  It never creates
an order, imports the live runner, or sends a network request.

PMData's documented ``poly_l2`` slug file contains one binary-market L2 book.
The complementary token is reconstructed level-by-level from the binary
relationship and every output labels that fact as an inference.  This is a
real historical market-state replay, but it is not equivalent to an archived
independent YES and NO websocket capture.
"""

# The runner bootstraps the local source tree before importing repo modules.
# Keep that deliberate import boundary explicit to the linter.
# ruff: noqa: E402, I001

from __future__ import annotations

import argparse
import bisect
import hashlib
import json
import math
import re
import subprocess
import sys
from collections import Counter
from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from aftertake.live_sizing import compute_live_entry_size  # noqa: E402
from aftertake.pm_client import BalanceAllowance, MarketMetadata  # noqa: E402
from aftertake.post_close import (
    PairedBook,
    PostCloseDecision,
    PostCloseWinnerClassifier,
    SideBook,
    classifier_family_config,
)  # noqa: E402
from aftertake.v9 import V9DualLaneClassifier, active_v9_config  # noqa: E402


RUNNER_VERSION = "v9_historical_replay_20260803_v2"
DEFAULT_CACHE = ROOT / "out" / "pmdata_profile_replay_cache"
DEFAULT_CHAINLINK = Path(r"C:\Users\ycric\OneDrive\Desktop\chainlink_btc_high_frequency_0731.log")
DEFAULT_MANIFEST = ROOT / "reports" / "v9_historical_backtest_manifest_20260803.json"
DEFAULT_REPORT = ROOT / "reports" / "v9_historical_backtest_20260803.md"
DEFAULT_LATENCIES = (0.0, 0.100, 0.300, 0.700, 0.970)
REPLAY_BASE_QTY = 5.0
REPLAY_BALANCE = 100.0
REPLAY_RISK_FRACTION = 0.50
REPLAY_QTY_STEP = 1.0
REPLAY_FEE_BPS = 0.0
MARKET_RE = re.compile(r"^(?P<asset>[a-z]+)-updown-5m-(?P<start>[0-9]+)$")


@dataclass(frozen=True)
class V9ReplayProfile:
    name: str
    residual_dominance_gap: float = 0.10
    sweep_dominance_gap: float = 0.15
    winner_bid_floor: float = 0.20
    loser_reclaim_gap: float = 0.03
    post_close_end_s: float = 0.250
    sweep_confirmations: int = 2
    selection_note: str = "V9 commit default"

    def config(self):
        base = active_v9_config()
        return replace(
            base,
            residual_dominance_gap=self.residual_dominance_gap,
            sweep_dominance_gap=self.sweep_dominance_gap,
            winner_bid_floor=self.winner_bid_floor,
            loser_reclaim_gap=self.loser_reclaim_gap,
            post_close_end_s=self.post_close_end_s,
            sweep_confirmations=self.sweep_confirmations,
            strategy_version="%s_replay_%s" % (base.strategy_version, self.name),
        )


# Every profile after the first is a single-factor train-only counterfactual.
# No profile is selected from validation or unseen holdout rows.
V9_PROFILES: Tuple[V9ReplayProfile, ...] = (
    V9ReplayProfile(name="v9_current"),
    V9ReplayProfile(
        name="v9_r_gap_15",
        residual_dominance_gap=0.15,
        selection_note="train-only residual dominance sensitivity",
    ),
    V9ReplayProfile(
        name="v9_floor_30",
        winner_bid_floor=0.30,
        selection_note="train-only winner bid-floor sensitivity",
    ),
    V9ReplayProfile(
        name="v9_reclaim_05",
        loser_reclaim_gap=0.05,
        selection_note="train-only loser reclaim sensitivity",
    ),
    V9ReplayProfile(
        name="v9_horizon_500",
        post_close_end_s=0.500,
        selection_note="train-only post-close horizon sensitivity",
    ),
    V9ReplayProfile(
        name="v9_s_confirm_1",
        sweep_confirmations=1,
        selection_note="train-only sweep confirmation sensitivity",
    ),
)


@dataclass(frozen=True)
class MarketRecord:
    slug: str
    round_start: int
    round_end: int
    books: Tuple[PairedBook, ...]
    outcome_side: Optional[str]
    outcome_values: Tuple[str, ...]
    outcome_label_source: str
    event_counts: Dict[str, int]
    rows_total: int
    reconstructed_events: int
    initial_snapshot_available: bool
    local_first: Optional[float]
    local_last: Optional[float]
    event_first: Optional[float]
    event_last: Optional[float]
    file_bytes: int
    file_sha256: str
    paired_book_quality: str = "inferred_binary_complement"


def _finite_float(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _datetime_seconds(values: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(values, utc=True, errors="coerce")
    unit = str(getattr(parsed.dtype, "unit", "ns"))
    divisor = {"s": 1.0, "ms": 1_000.0, "us": 1_000_000.0, "ns": 1_000_000_000.0}.get(unit)
    if divisor is None:
        raise RuntimeError("unsupported datetime unit: %s" % unit)
    seconds = pd.Series(parsed.array.asi8, index=values.index, dtype="float64") / divisor
    seconds.loc[parsed.isna()] = float("nan")
    return seconds


def _levels(prices: Any, sizes: Any) -> Dict[float, float]:
    if prices is None or sizes is None:
        return {}
    result: Dict[float, float] = {}
    try:
        pairs = zip(prices, sizes)
    except TypeError:
        return result
    for price_raw, size_raw in pairs:
        price = _finite_float(price_raw)
        size = _finite_float(size_raw)
        if price is not None and size is not None and 0.0 < price < 1.0 and size > 0.0:
            result[price] = size
    return result


def _side_book(bids: Dict[float, float], asks: Dict[float, float]) -> SideBook:
    best_bid = max(bids) if bids else None
    best_ask = min(asks) if asks else None
    bid_size = float(bids.get(best_bid, 0.0)) if best_bid is not None else 0.0
    ask_size = float(asks.get(best_ask, 0.0)) if best_ask is not None else 0.0
    near_touch = (
        sum(size for price, size in bids.items() if price >= best_bid - 0.02)
        if best_bid is not None
        else 0.0
    )
    return SideBook(
        best_bid=best_bid,
        bid_size=bid_size,
        bid_depth=sum(bids.values()),
        best_ask=best_ask,
        ask_size=ask_size,
        near_touch_bid_depth=near_touch,
        ask_levels=tuple(sorted((float(price), float(size)) for price, size in asks.items())),
    )


def _paired_book(
    observed_at: Optional[float],
    source_timestamp: Optional[float],
    yes_bids: Dict[float, float],
    yes_asks: Dict[float, float],
) -> Optional[PairedBook]:
    if observed_at is None:
        return None
    # PMData's documented L2 file is a binary-market book.  Preserve all
    # complementary levels so V9's <=.99 sweep is a real ladder calculation,
    # while marking the pair as inferred in every manifest/report.
    no_bids = {round(1.0 - price, 12): size for price, size in yes_asks.items()}
    no_asks = {round(1.0 - price, 12): size for price, size in yes_bids.items()}
    yes = _side_book(yes_bids, yes_asks)
    no = _side_book(no_bids, no_asks)
    if yes.best_bid is None and yes.best_ask is None:
        return None
    return PairedBook(
        observed_at=observed_at,
        source_timestamp=source_timestamp,
        yes=yes,
        no=no,
        # A single PMData callback updates the inferred pair atomically.  This
        # is not a claim that two independent token feeds were archived.
        yes_updated_at=observed_at,
        no_updated_at=observed_at,
    )


def _normal_side(value: Any) -> Optional[str]:
    normalized = str(value or "").strip().upper()
    if normalized in {"YES", "UP"}:
        return "YES"
    if normalized in {"NO", "DOWN"}:
        return "NO"
    return None


def _slug_bounds(slug: str) -> Tuple[int, int]:
    match = MARKET_RE.fullmatch(slug)
    if not match:
        raise ValueError("unsupported slug: %s" % slug)
    start = int(match.group("start"))
    return start, start + 300


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _reconstruct_books(
    frame: pd.DataFrame,
    *,
    round_end: int,
    horizon_s: float,
) -> Tuple[Tuple[PairedBook, ...], bool]:
    frame = frame[frame["_local"].notna()].sort_values("_local", kind="stable").reset_index(drop=True)
    if frame.empty:
        return (), False
    start_local = round_end - 10.5
    end_local = round_end + horizon_s
    full_before = frame.index[
        frame["event_type"].eq("book") & (frame["_local_seconds"] <= start_local)
    ].tolist()
    initial_snapshot = bool(full_before)
    if full_before:
        start_index = full_before[-1]
    else:
        full_until_end = frame.index[
            frame["event_type"].eq("book") & (frame["_local_seconds"] <= end_local)
        ].tolist()
        start_index = full_until_end[0] if full_until_end else len(frame)

    local_seconds = frame["_local_seconds"].to_numpy(dtype=float)
    stop_index = bisect.bisect_right(local_seconds, end_local) - 1
    if start_index > stop_index:
        return (), initial_snapshot

    yes_bids: Dict[float, float] = {}
    yes_asks: Dict[float, float] = {}
    books: List[PairedBook] = []
    previous_local = -math.inf
    replay_columns = [
        "event_type",
        "bid_prices",
        "bid_sizes",
        "ask_prices",
        "ask_sizes",
        "pc_price",
        "pc_size",
        "pc_side",
        "_local_seconds",
        "_event_seconds",
    ]
    replay_frame = frame.iloc[start_index : stop_index + 1][replay_columns]
    for (
        event_type,
        bid_prices,
        bid_sizes,
        ask_prices,
        ask_sizes,
        pc_price_raw,
        pc_size_raw,
        pc_side_raw,
        local_ts_raw,
        source_ts_raw,
    ) in replay_frame.itertuples(index=False, name=None):
        local_ts = _finite_float(local_ts_raw)
        if local_ts is None:
            continue
        event_type = str(event_type or "")
        if event_type == "book":
            yes_bids = _levels(bid_prices, bid_sizes)
            yes_asks = _levels(ask_prices, ask_sizes)
        elif event_type == "price_change":
            price = _finite_float(pc_price_raw)
            size = _finite_float(pc_size_raw)
            side = str(pc_side_raw or "").upper()
            if price is None or size is None or side not in {"BUY", "SELL"}:
                continue
            target = yes_bids if side == "BUY" else yes_asks
            if size <= 0:
                target.pop(price, None)
            else:
                target[price] = size
        else:
            continue
        if not yes_bids or not yes_asks:
            continue
        paired = _paired_book(local_ts, _finite_float(source_ts_raw), yes_bids, yes_asks)
        if paired is None or paired.observed_at <= previous_local:
            continue
        books.append(paired)
        previous_local = paired.observed_at
    return tuple(books), initial_snapshot


def _load_market(path: Path, *, horizon_s: float) -> MarketRecord:
    slug = path.stem
    round_start, round_end = _slug_bounds(slug)
    required = {
        "market_slug",
        "timestamp",
        "local_timestamp",
        "event_type",
        "ask_prices",
        "ask_sizes",
        "bid_prices",
        "bid_sizes",
        "pc_price",
        "pc_size",
        "pc_side",
        "winning_outcome",
    }
    frame = pd.read_parquet(path, columns=sorted(required))
    missing = sorted(required - set(frame.columns))
    if missing:
        raise RuntimeError("%s missing columns: %s" % (slug, ",".join(missing)))
    frame["_local"] = pd.to_datetime(frame["local_timestamp"], utc=True, errors="coerce")
    frame["_event"] = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
    frame["_local_seconds"] = _datetime_seconds(frame["_local"])
    frame["_event_seconds"] = _datetime_seconds(frame["_event"])
    outcome_values = tuple(
        sorted({str(value).strip().lower() for value in frame["winning_outcome"].dropna().unique()})
    )
    normalized_outcomes = {_normal_side(value) for value in outcome_values}
    normalized_outcomes.discard(None)
    outcome_side = next(iter(normalized_outcomes)) if len(normalized_outcomes) == 1 else None
    event_counts = Counter(str(value) for value in frame["event_type"].dropna())
    if outcome_side and event_counts.get("market_resolved", 0) > 0:
        label_source = "pmdata_market_resolved"
    elif outcome_side:
        label_source = "pmdata_winning_outcome_field_without_resolved_event"
    else:
        label_source = "unlabelled"
    books, initial_snapshot = _reconstruct_books(frame, round_end=round_end, horizon_s=horizon_s)
    local_values = frame["_local_seconds"].dropna()
    event_values = frame["_event_seconds"].dropna()
    return MarketRecord(
        slug=slug,
        round_start=round_start,
        round_end=round_end,
        books=books,
        outcome_side=outcome_side,
        outcome_values=outcome_values,
        outcome_label_source=label_source,
        event_counts=dict(event_counts),
        rows_total=len(frame),
        reconstructed_events=len(books),
        initial_snapshot_available=initial_snapshot,
        local_first=float(local_values.min()) if not local_values.empty else None,
        local_last=float(local_values.max()) if not local_values.empty else None,
        event_first=float(event_values.min()) if not event_values.empty else None,
        event_last=float(event_values.max()) if not event_values.empty else None,
        file_bytes=path.stat().st_size,
        file_sha256=_sha256(path),
        paired_book_quality=(
            "inferred_binary_complement"
            if books
            else "unavailable_no_complete_two_sided_state"
        ),
    )


def _metadata() -> MarketMetadata:
    return MarketMetadata(
        condition_id="historical-replay",
        tick_size="0.01",
        min_order_size=1.0,
        neg_risk=False,
        fee_rate=0.0,
        tokens={},
        raw={"historical_assumption": True},
    )


def _size_for(decision: PostCloseDecision) -> Optional[float]:
    if decision.entry_ask is None:
        return None
    sizing = compute_live_entry_size(
        price=decision.entry_ask,
        available_size=decision.entry_ask_size,
        collateral=BalanceAllowance(
            balance=REPLAY_BALANCE,
            allowance=REPLAY_BALANCE,
            raw={"historical_replay": True},
        ),
        metadata=_metadata(),
        max_account_fraction=REPLAY_RISK_FRACTION,
        quantity_step=REPLAY_QTY_STEP,
    )
    return float(sizing.qty) if sizing.accepted else None


def _audit_projection(decision: Optional[PostCloseDecision]) -> Dict[str, Any]:
    if decision is None:
        return {}
    audit = decision.audit or {}
    return {
        "action": decision.action,
        "reason": decision.reason,
        "side": decision.side,
        "entry_ask": decision.entry_ask,
        "entry_ask_size": decision.entry_ask_size,
        "confirmations": decision.confirmations,
        "event_ts": audit.get("event_ts"),
        "receive_ts": audit.get("receive_ts"),
        "decision_ts": audit.get("decision_ts", audit.get("now_ts")),
        "book_age_ms": audit.get("book_age_ms"),
        "winner": audit.get("winner"),
        "loser": audit.get("loser"),
        "raw_top": audit.get("raw_top"),
        "first_blocker": audit.get("first_blocker", decision.reason),
        "strategy_version": audit.get("strategy_version"),
        "code_sha": audit.get("code_sha"),
        "settlement_label": audit.get("settlement_label"),
        "confirmation_timestamps": audit.get("confirmation_timestamps"),
    }


def _signal_row(
    market: MarketRecord,
    profile: str,
    lane: str,
    decision: Optional[PostCloseDecision],
    qty: Optional[float],
    last_decision: Optional[PostCloseDecision],
    reservation_lane: Optional[str] = None,
) -> Dict[str, Any]:
    audit = decision.audit if decision is not None else {}
    return {
        "market_slug": market.slug,
        "round_start": market.round_start,
        "round_end": market.round_end,
        "split": None,
        "profile": profile,
        "lane": lane,
        "signal": bool(decision is not None and decision.action == "enter" and qty),
        "qty": qty,
        "side": decision.side if decision is not None else None,
        "limit_price": decision.entry_ask if decision is not None else None,
        "signal_ask_size": decision.entry_ask_size if decision is not None else None,
        "decision_ts": audit.get("decision_ts", audit.get("now_ts")) if decision else None,
        "event_ts": audit.get("event_ts") if decision else None,
        "receive_ts": audit.get("receive_ts") if decision else None,
        "book_age_ms": audit.get("book_age_ms") if decision else None,
        "strategy_version": audit.get("strategy_version") if decision else None,
        "code_sha": audit.get("code_sha") if decision else None,
        "settlement_label": audit.get("settlement_label") if decision else None,
        "decision_audit": _audit_projection(decision or last_decision),
        "last_blocker": (
            "shared_reservation_held_by_%s" % reservation_lane
            if reservation_lane is not None and reservation_lane != lane
            else (last_decision.reason if last_decision is not None else "no_post_close_observation")
        ),
        "reservation_scope": "one_per_market_profile",
        "reservation_lane": reservation_lane,
        "reservation_blocked": bool(reservation_lane is not None and reservation_lane != lane),
        "replayable": bool(market.reconstructed_events > 0),
        "replayability_reason": (
            "paired_book_reconstructed"
            if market.reconstructed_events > 0
            else "no_complete_two_sided_state_in_replay_window"
        ),
        "outcome_side": market.outcome_side,
        "outcome_values": list(market.outcome_values),
        "outcome_label_source": market.outcome_label_source,
        "paired_book_quality": market.paired_book_quality,
        "latencies": {},
    }


def _v9_rows(market: MarketRecord, profile: V9ReplayProfile) -> List[Dict[str, Any]]:
    classifier = V9DualLaneClassifier(
        profile.config(),
        settlement_label="binary_up_down",
        code_sha="historical_replay",
    )
    candidates: Dict[str, Optional[PostCloseDecision]] = {"R": None, "S": None}
    quantities: Dict[str, Optional[float]] = {"R": None, "S": None}
    last: Dict[str, Optional[PostCloseDecision]] = {"R": None, "S": None}
    reservation_lane: Optional[str] = None
    end = market.round_end + profile.post_close_end_s
    for book in market.books:
        classifier.record(book)
        now = book.observed_at
        if now < market.round_end or now > end:
            continue
        lanes = classifier.evaluate_lanes(
            round_end_ts=float(market.round_end),
            now_ts=now,
            qty=REPLAY_BASE_QTY,
        )
        for lane in ("R", "S"):
            if reservation_lane is not None:
                continue
            last[lane] = lanes[lane]
            if candidates[lane] is not None or lanes[lane].action != "enter":
                continue
            quantity = _size_for(lanes[lane])
            if quantity is None:
                continue
            sized = classifier.evaluate_lanes(
                round_end_ts=float(market.round_end),
                now_ts=now,
                qty=quantity,
            )[lane]
            if sized.action == "enter":
                candidates[lane] = sized
                quantities[lane] = quantity
                reservation_lane = lane
        if reservation_lane is not None:
            break
    return [
        _signal_row(
            market,
            profile.name,
            lane,
            candidates[lane],
            quantities[lane],
            last[lane],
            reservation_lane,
        )
        for lane in ("R", "S")
    ]


def _v8_row(market: MarketRecord) -> Dict[str, Any]:
    config = replace(
        classifier_family_config("v8"),
        strategy_version="aftertake_v8_1_stable_book_refill_guard_250ms_replay",
    )
    classifier = PostCloseWinnerClassifier(config)
    candidate: Optional[PostCloseDecision] = None
    quantity: Optional[float] = None
    last: Optional[PostCloseDecision] = None
    for book in market.books:
        classifier.record(book)
        now = book.observed_at
        if now < market.round_end + config.post_close_start_s:
            continue
        if now > market.round_end + config.post_close_end_s:
            break
        decision = classifier.evaluate(
            round_end_ts=float(market.round_end),
            now_ts=now,
            qty=REPLAY_BASE_QTY,
        )
        last = decision
        if decision.action != "enter":
            continue
        sized_qty = _size_for(decision)
        if sized_qty is None:
            continue
        sized = classifier.evaluate(
            round_end_ts=float(market.round_end),
            now_ts=now,
            qty=sized_qty,
            min_near_touch_qty_multiplier=1.0,
        )
        if sized.action == "enter":
            candidate = sized
            quantity = sized_qty
            break
    return _signal_row(market, "v8_1", "control", candidate, quantity, last)


def _execute_ladder(side: SideBook, limit_price: float, qty: float) -> Dict[str, Any]:
    levels = list(side.ask_levels)
    if not levels and side.best_ask is not None and side.ask_size > 0:
        levels = [(float(side.best_ask), float(side.ask_size))]
    remaining = float(qty)
    consumed = 0.0
    notional = 0.0
    consumed_levels: List[Tuple[float, float]] = []
    for price, size in sorted(levels):
        if price > limit_price + 1e-9:
            break
        take = min(remaining, float(size))
        if take <= 0:
            continue
        consumed += take
        notional += take * float(price)
        consumed_levels.append((float(price), take))
        remaining -= take
        if remaining <= 1e-9:
            break
    return {
        "fillable": remaining <= 1e-9,
        "available_qty": consumed,
        "vwap": (notional / consumed) if consumed > 0 else None,
        "levels": consumed_levels,
        "limit_price": limit_price,
    }


def _arrival(row: Dict[str, Any], market: MarketRecord, latency_s: float) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "latency_s": latency_s,
        "arrival_complete": False,
        "fillable_proxy": False,
        "proxy_reason": "no_signal",
        "hit": None,
        "pnl_usd": None,
        "fee_usd": None,
        "arrival_vwap": None,
        "arrival_available_qty": None,
        "arrival_event_ts": None,
        "arrival_receive_ts": None,
    }
    if not row.get("signal"):
        return result
    decision_ts = float(row["decision_ts"])
    target_ts = decision_ts + latency_s
    observed = [book.observed_at for book in market.books]
    index = bisect.bisect_right(observed, target_ts) - 1
    if index < 0:
        result["proxy_reason"] = "no_book_before_arrival"
        return result
    if index + 1 >= len(market.books):
        result["proxy_reason"] = "no_future_book_after_arrival"
        return result
    book = market.books[index]
    side = book.yes if row["side"] == "YES" else book.no
    execution = _execute_ladder(side, float(row["limit_price"]), float(row["qty"]))
    result.update(
        {
            "arrival_complete": True,
            "fillable_proxy": bool(execution["fillable"]),
            "proxy_reason": (
                "displayed_ladder_marketable"
                if execution["fillable"]
                else "limit_or_displayed_depth_not_marketable"
            ),
            "arrival_vwap": execution["vwap"],
            "arrival_available_qty": execution["available_qty"],
            "arrival_levels": execution["levels"],
            "arrival_event_ts": book.source_timestamp,
            "arrival_receive_ts": book.observed_at,
            "arrival_best_ask": side.best_ask,
            "arrival_best_ask_size": side.ask_size,
        }
    )
    if not execution["fillable"] or market.outcome_side is None:
        return result
    hit = row["side"] == market.outcome_side
    gross = (
        (1.0 - float(execution["vwap"])) * float(row["qty"])
        if hit
        else -float(execution["vwap"]) * float(row["qty"])
    )
    fee = float(execution["vwap"]) * float(row["qty"]) * REPLAY_FEE_BPS / 10000.0
    result.update({"hit": hit, "fee_usd": fee, "pnl_usd": gross - fee})
    return result


def _attach_arrivals(rows: List[Dict[str, Any]], market: MarketRecord, latencies: Sequence[float]) -> None:
    for row in rows:
        row["latencies"] = {"%.3f" % latency: _arrival(row, market, latency) for latency in latencies}


def _git_head() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True, stderr=subprocess.DEVNULL
        ).strip()
    except Exception:
        return "unknown"


def _chainlink_manifest(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"exists": False, "path": str(path), "label_join_status": "missing"}
    line_count = 0
    data_rows = 0
    telemetry_rows = 0
    first = None
    last = None
    age_min = None
    age_max = None
    receive_re = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})")
    age_re = re.compile(r"([0-9]+)ms")
    with path.open("r", encoding="utf-8", errors="replace") as source:
        for line in source:
            line_count += 1
            match = receive_re.match(line)
            if not match:
                if not line.startswith("#"):
                    telemetry_rows += 1
                continue
            data_rows += 1
            receive = match.group(1)
            first = first or receive
            last = receive
            age_match = age_re.search(line)
            if age_match:
                age = int(age_match.group(1))
                age_min = age if age_min is None else min(age_min, age)
                age_max = age if age_max is None else max(age_max, age)
    return {
        "exists": True,
        "path": str(path),
        "bytes": path.stat().st_size,
        "line_count": line_count,
        "data_rows": data_rows,
        "telemetry_rows": telemetry_rows,
        "receive_first_et": first,
        "receive_last_et": last,
        "age_min_ms": age_min,
        "age_max_ms": age_max,
        "label_join_status": "not_joined_to_cached_markets",
    }


def _wilson_lower(wins: int, total: int) -> Optional[float]:
    if total <= 0:
        return None
    z = 1.959963984540054
    p = wins / total
    denominator = 1.0 + z * z / total
    center = p + z * z / (2.0 * total)
    margin = z * math.sqrt((p * (1.0 - p) + z * z / (4.0 * total)) / total)
    return (center - margin) / denominator


def _metric(rows: Sequence[Dict[str, Any]], profile: str, lane: str, split: str, latency_key: str) -> Dict[str, Any]:
    subset = [
        row
        for row in rows
        if row["profile"] == profile and row["lane"] == lane and row["split"] == split
    ]
    candidates = [row for row in subset if row.get("signal")]
    arrivals = [row for row in candidates if row["latencies"].get(latency_key, {}).get("arrival_complete")]
    fills = [row for row in arrivals if row["latencies"][latency_key].get("fillable_proxy")]
    labelled = [row for row in fills if row.get("outcome_side")]
    wins = sum(row["latencies"][latency_key].get("hit") is True for row in labelled)
    losses = sum(row["latencies"][latency_key].get("hit") is False for row in labelled)
    pnls = [
        float(row["latencies"][latency_key]["pnl_usd"])
        for row in labelled
        if row["latencies"][latency_key].get("pnl_usd") is not None
    ]
    vwap = [
        float(row["latencies"][latency_key]["arrival_vwap"])
        for row in fills
        if row["latencies"][latency_key].get("arrival_vwap") is not None
    ]
    order_qtys = [float(row["qty"]) for row in candidates if row.get("qty") is not None]
    displayed_depth = [
        float(row["signal_ask_size"])
        for row in candidates
        if row.get("signal_ask_size") is not None
    ]
    unique_markets = len({row["market_slug"] for row in subset})
    replayable_markets = len(
        {row["market_slug"] for row in subset if row.get("replayable")}
    )
    return {
        "profile": profile,
        "lane": lane,
        "split": split,
        "latency_s": float(latency_key),
        "unique_markets": unique_markets,
        "replayable_markets": replayable_markets,
        "candidate_entries": len(candidates),
        "candidate_coverage": len(candidates) / unique_markets if unique_markets else None,
        "candidate_coverage_replayable": (
            len(candidates) / replayable_markets if replayable_markets else None
        ),
        "arrival_complete": len(arrivals),
        "fillable_entries": len(fills),
        "fillable_coverage": len(fills) / unique_markets if unique_markets else None,
        "fillable_coverage_replayable": (
            len(fills) / replayable_markets if replayable_markets else None
        ),
        "wins": wins,
        "losses": losses,
        "observed_precision": wins / len(labelled) if labelled else None,
        "wilson_lower_bound": _wilson_lower(wins, len(labelled)),
        "average_entry_vwap": sum(vwap) / len(vwap) if vwap else None,
        "average_order_qty": sum(order_qtys) / len(order_qtys) if order_qtys else None,
        "total_order_qty": sum(order_qtys) if order_qtys else 0.0,
        "average_displayed_executable_depth": (
            sum(displayed_depth) / len(displayed_depth) if displayed_depth else None
        ),
        "displayed_executable_depth_total": sum(displayed_depth) if displayed_depth else 0.0,
        "total_pnl_usd": sum(pnls) if pnls else 0.0,
        "maximum_single_trade_loss_usd": max([-pnl for pnl in pnls if pnl < 0.0], default=0.0),
        "fee_bps": REPLAY_FEE_BPS,
        "labelled_fill_count": len(labelled),
    }


def _select_train_profile(rows: Sequence[Dict[str, Any]], lane: str) -> Tuple[str, List[Dict[str, Any]]]:
    candidates = []
    for profile in V9_PROFILES:
        metric = _metric(rows, profile.name, lane, "train", "0.000")
        if metric["fillable_entries"] > 0:
            candidates.append(metric)
    if not candidates:
        return "v9_current", []
    selected = max(
        candidates,
        key=lambda metric: (
            float(metric["observed_precision"] or -1.0),
            float(metric["wilson_lower_bound"] or -1.0),
            int(metric["fillable_entries"]),
            float(metric["total_pnl_usd"]),
        ),
    )
    return str(selected["profile"]), candidates


def _split_for_index(index: int, total: int, train_fraction: float, validation_fraction: float) -> str:
    train_end = int(total * train_fraction)
    validation_end = train_end + int(total * validation_fraction)
    return "train" if index < train_end else ("validation" if index < validation_end else "unseen_holdout")


def _manifest_record(market: MarketRecord, index: int, split: str) -> Dict[str, Any]:
    return {
        "index": index,
        "split": split,
        "market_slug": market.slug,
        "round_start": market.round_start,
        "round_end": market.round_end,
        "rows_total": market.rows_total,
        "event_counts": market.event_counts,
        "reconstructed_events": market.reconstructed_events,
        "initial_snapshot_available": market.initial_snapshot_available,
        "local_first": market.local_first,
        "local_last": market.local_last,
        "event_first": market.event_first,
        "event_last": market.event_last,
        "outcome_values": list(market.outcome_values),
        "outcome_side": market.outcome_side,
        "outcome_label_source": market.outcome_label_source,
        "paired_book_quality": market.paired_book_quality,
        "file_bytes": market.file_bytes,
        "file_sha256": market.file_sha256,
    }


def _json_default(value: Any) -> Any:
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    raise TypeError(type(value).__name__)


def _write_manifest(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default) + "\n", encoding="utf-8")


def _fmt(value: Any, digits: int = 4) -> str:
    if value is None:
        return "NA"
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value)


def _report(
    *,
    manifest: Dict[str, Any],
    rows: Sequence[Dict[str, Any]],
    selected: Dict[str, str],
    train_candidates: Dict[str, List[Dict[str, Any]]],
    latencies: Sequence[float],
    address_baseline_path: Path,
) -> str:
    split_counts = manifest["split_counts"]
    replayable_split_counts = manifest.get("replayable_split_counts", {})
    lines = [
        "# V9 historical PMData replay",
        "",
        f"Generated by `{RUNNER_VERSION}`. Git HEAD at run: `{manifest['git_head']}`.",
        "This is a historical displayed-ladder marketability replay, not pytest and not a live-order test.",
        "",
        "## Decision",
        "",
    ]
    validation_results = []
    holdout_results = []
    for lane, profile in selected.items():
        validation_results.append(_metric(rows, profile, lane, "validation", "0.000"))
        metric = _metric(rows, profile, lane, "unseen_holdout", "0.000")
        holdout_results.append(metric)
    if all(
        validation["fillable_entries"] > 0
        and validation["observed_precision"] == 1.0
        and holdout["fillable_entries"] > 0
        and holdout["observed_precision"] == 1.0
        for validation, holdout in zip(validation_results, holdout_results)
    ):
        lines.append(
            "Both selected lanes have non-zero fillable proxies and observed 100% direction precision "
            "in both validation and unseen holdout. This is an observed sample result, not a future guarantee."
        )
    else:
        lines.append(
            "No selected lane establishes the requested combination of non-zero entries and observed "
            "100% precision in both chronological validation and unseen holdout; the tables preserve "
            "the highest verifiable result without tuning holdout."
        )
    lines.extend(
        [
            "",
            "## Data and label contract",
            "",
            f"- Local PMData cache: `{manifest['cache_dir']}`; {manifest['unique_markets']} unique markets, "
            f"{manifest['total_bytes']} bytes, no duplicate slugs.",
            f"- Splits: train={split_counts.get('train', 0)}, validation={split_counts.get('validation', 0)}, "
            f"unseen_holdout={split_counts.get('unseen_holdout', 0)} in chronological round-start order.",
            f"- Replayable paired-book markets: total={manifest.get('replayable_markets', 0)}, "
            f"unreplayable={manifest.get('unreplayable_markets', 0)}; by split train={replayable_split_counts.get('train', 0)}, "
            f"validation={replayable_split_counts.get('validation', 0)}, "
            f"unseen_holdout={replayable_split_counts.get('unseen_holdout', 0)}. "
            "Unreplayable files have no complete two-sided state in the post-close reconstruction window "
            "and are not evidence of a hold.",
            "- PMData event timestamp is `timestamp`; replay order and observed time are `local_timestamp`.",
            "- The result uses PMData `market_resolved`/`winning_outcome` as the replay outcome label. "
            "The supplied Chainlink log is July 31 ET and does not overlap this July 17-18 UTC cache; "
            "therefore Chainlink labels for these markets are explicitly unavailable, not guessed.",
            "- NO is reconstructed from the binary complement of the archived L2 book; native independent "
            "YES/NO websocket evidence is not present in these files.",
            "- Fee assumption is 0 bps because historical fee metadata is unavailable; PnL is pre-fee. "
            "Queue priority, hidden liquidity, and exchange acknowledgement are not claimed.",
            "",
            "Official sources: [PMData L2 schema](https://pmdata.dev/docs/datasets/l2), "
            "[PMData slug API](https://pmdata.dev/docs/api/slug-api), "
            "[Polymarket orderbook](https://docs.polymarket.com/trading/orderbook).",
            "",
            "## Train-only threshold selection",
            "",
            "Selection maximizes train observed precision, then Wilson lower bound, then fillable entries, "
            "with no validation or holdout outcome read during selection. Profiles after `v9_current` change "
            "one V9 threshold only.",
            "",
            "| Lane | Selected profile | Train candidates |",
            "|---|---|---:|",
        ]
    )
    for lane in ("R", "S"):
        lines.append(f"| {lane} | `{selected[lane]}` | {len(train_candidates.get(lane, []))} |")
    lines.extend(["", "### Train candidate metrics", "", "| Lane | Profile | Fillable | Precision | Wilson lower |", "|---|---|---:|---:|---:|"])
    for lane in ("R", "S"):
        for metric in train_candidates.get(lane, []):
            lines.append(
                f"| {lane} | `{metric['profile']}` | {metric['fillable_entries']} | "
                f"{_fmt(metric['observed_precision'])} | {_fmt(metric['wilson_lower_bound'])} |"
            )
    lines.extend(
        [
            "",
            "## Main results at zero added latency",
            "",
            "| Profile | Lane | Split | Markets | Replayable | Candidates | Fillable | Order qty | Displayed <=.99 depth | W/L | Precision | Wilson lower | Coverage all/replayable | PnL | Max loss |",
            "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    profiles_to_report = [("v8_1", "control"), (selected["R"], "R"), (selected["S"], "S")]
    for profile, lane in profiles_to_report:
        for split in ("train", "validation", "unseen_holdout"):
            metric = _metric(rows, profile, lane, split, "0.000")
            lines.append(
                f"| `{profile}` | {lane} | {split} | {metric['unique_markets']} | "
                f"{metric['replayable_markets']} | {metric['candidate_entries']} | {metric['fillable_entries']} | "
                f"{_fmt(metric['total_order_qty'])} | {_fmt(metric['displayed_executable_depth_total'])} | "
                f"{metric['wins']}/{metric['losses']} | {_fmt(metric['observed_precision'])} | "
                f"{_fmt(metric['wilson_lower_bound'])} | {_fmt(metric['fillable_coverage'])}/"
                f"{_fmt(metric['fillable_coverage_replayable'])} | {_fmt(metric['total_pnl_usd'])} | "
                f"{_fmt(metric['maximum_single_trade_loss_usd'])} |"
            )
    lines.extend(
        [
            "",
            "## Validation/holdout profile sensitivity (diagnostic only)",
            "",
            "These rows are reported after the frozen train selection and are not used to choose a profile.",
            "",
            "| Profile | Lane | Split | Replayable | Candidates | Fillable | W/L | Precision | Wilson lower |",
            "|---|---|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for profile_config in V9_PROFILES:
        for lane in ("R", "S"):
            for split in ("validation", "unseen_holdout"):
                metric = _metric(rows, profile_config.name, lane, split, "0.000")
                lines.append(
                    f"| `{profile_config.name}` | {lane} | {split} | {metric['replayable_markets']} | "
                    f"{metric['candidate_entries']} | {metric['fillable_entries']} | "
                    f"{metric['wins']}/{metric['losses']} | {_fmt(metric['observed_precision'])} | "
                    f"{_fmt(metric['wilson_lower_bound'])} |"
                )
    lines.extend(["", "## Latency sensitivity for selected lanes", "", "| Profile | Lane | Split | Latency ms | Candidates | Fillable | W/L | Precision | Avg VWAP | PnL |", "|---|---|---|---:|---:|---:|---:|---:|---:|---:|"])
    for profile, lane in ((selected["R"], "R"), (selected["S"], "S")):
        for split in ("validation", "unseen_holdout"):
            for latency in latencies:
                key = "%.3f" % latency
                metric = _metric(rows, profile, lane, split, key)
                lines.append(
                    f"| `{profile}` | {lane} | {split} | {latency * 1000:.0f} | {metric['candidate_entries']} | "
                    f"{metric['fillable_entries']} | {metric['wins']}/{metric['losses']} | {_fmt(metric['observed_precision'])} | "
                    f"{_fmt(metric['average_entry_vwap'])} | {_fmt(metric['total_pnl_usd'])} |"
                )
    lines.extend(
        [
            "",
            "## Address baseline",
            "",
            f"The available public-address baseline is the prior analysis `{address_baseline_path.relative_to(ROOT) if address_baseline_path.is_relative_to(ROOT) else address_baseline_path}`. "
            "It reports 1,072 crypto 5m BUY rows across 510 markets and 510/510 terminal-winner "
            "markets (observed 100%; Wilson lower bound 99.414%), but BTC contributed zero rows in that "
            "snapshot. It is therefore an external address baseline, not a same-market comparator for "
            "this 100-market BTC corpus, and it does not prove future 100% precision.",
            "",
            "## Leakage, duplication, and counterexample checks",
            "",
            "- One row per `(market_slug, profile, lane)`; V9 R/S share a one-reservation-per-market "
            "arbitration, so a market cannot contribute fills to both lanes of one profile.",
            "- Classifiers receive only reconstructed books and structural settlement semantics; outcome is "
            "read only after a candidate/arrival proxy is produced.",
            "- Train selection is frozen before validation/unseen metrics; no holdout threshold search is run.",
            "- Arrival uses the last book observed at or before `decision_ts + latency`, and requires a later "
            "book for a complete horizon; future books cannot create the signal itself.",
        ]
    )
    counterexamples = []
    for lane, profile in selected.items():
        for row in rows:
            if row["profile"] != profile or row["lane"] != lane or row["split"] != "unseen_holdout" or not row.get("signal"):
                continue
            arrival = row["latencies"].get("0.000", {})
            if arrival.get("hit") is False:
                counterexamples.append(
                    f"{lane} `{row['market_slug']}` side={row['side']} outcome={row['outcome_side']} "
                    f"vwap={_fmt(arrival.get('arrival_vwap'))}"
                )
    if counterexamples:
        lines.extend(["", "Minimal unseen-holdout counterexamples:", ""])
        lines.extend(f"- {item}" for item in counterexamples[:10])
    else:
        lines.extend(["", "No losing unseen-holdout fillable proxy was observed for the selected lanes at zero added latency; sample sizes remain in the tables above."])
    return "\n".join(lines) + "\n"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE)
    parser.add_argument("--chainlink-log", type=Path, default=DEFAULT_CHAINLINK)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--train-fraction", type=float, default=0.60)
    parser.add_argument("--validation-fraction", type=float, default=0.20)
    parser.add_argument("--latencies-ms", default="0,100,300,700,970")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    if not 0 < args.train_fraction < 1 or not 0 <= args.validation_fraction < 1:
        raise SystemExit("invalid chronological split fractions")
    if args.train_fraction + args.validation_fraction >= 1:
        raise SystemExit("train plus validation fractions must be < 1")
    latencies = tuple(float(value.strip()) / 1000.0 for value in args.latencies_ms.split(",") if value.strip())
    if not latencies or any(value < 0 for value in latencies):
        raise SystemExit("latencies must be non-negative milliseconds")
    files = sorted(args.cache_dir.glob("btc-updown-5m-*.parquet"), key=lambda path: _slug_bounds(path.stem)[0])
    if args.limit > 0:
        files = files[: args.limit]
    if not files:
        raise SystemExit("no local PMData Parquet files found")
    if len({path.stem for path in files}) != len(files):
        raise SystemExit("duplicate market slugs in cache")
    horizon_s = max(profile.post_close_end_s for profile in V9_PROFILES) + max(latencies) + 0.5
    horizon_s = max(horizon_s, 1.5)
    rows: List[Dict[str, Any]] = []
    market_manifest: List[Dict[str, Any]] = []
    split_counts = Counter()
    for index, path in enumerate(files):
        market = _load_market(path, horizon_s=horizon_s)
        split = _split_for_index(index, len(files), args.train_fraction, args.validation_fraction)
        split_counts[split] += 1
        market_manifest.append(_manifest_record(market, index, split))
        for profile in V9_PROFILES:
            for row in _v9_rows(market, profile):
                row["split"] = split
                _attach_arrivals([row], market, latencies)
                rows.append(row)
        control = _v8_row(market)
        control["split"] = split
        _attach_arrivals([control], market, latencies)
        rows.append(control)
        print(json.dumps({"kind": "market_complete", "index": index + 1, "total": len(files), "slug": market.slug, "reconstructed_events": market.reconstructed_events}), flush=True)

    selected: Dict[str, str] = {}
    train_candidates: Dict[str, List[Dict[str, Any]]] = {}
    for lane in ("R", "S"):
        selected[lane], train_candidates[lane] = _select_train_profile(rows, lane)
    replayable_records = [
        record for record in market_manifest if record["reconstructed_events"] > 0
    ]
    replayable_split_counts = Counter(record["split"] for record in replayable_records)
    manifest = {
        "runner_version": RUNNER_VERSION,
        "git_head": _git_head(),
        "cache_dir": str(args.cache_dir.relative_to(ROOT)) if args.cache_dir.is_relative_to(ROOT) else str(args.cache_dir),
        "source_dataset": "PMData poly_l2 local cache",
        "official_l2_schema": "https://pmdata.dev/docs/datasets/l2",
        "official_slug_api": "https://pmdata.dev/docs/api/slug-api",
        "credential_status": "not_used; source-thread credential was not transmitted by this run",
        "public_endpoint_probe": "401_without_credentials",
        "raw_data_commit_status": "not_committed",
        "unique_markets": len(files),
        "total_bytes": sum(path.stat().st_size for path in files),
        "duplicate_market_slugs": 0,
        "split_fractions": {"train": args.train_fraction, "validation": args.validation_fraction, "unseen_holdout": 1.0 - args.train_fraction - args.validation_fraction},
        "split_counts": dict(split_counts),
        "replayable_markets": len(replayable_records),
        "unreplayable_markets": len(market_manifest) - len(replayable_records),
        "replayable_split_counts": dict(replayable_split_counts),
        "replayability_contract": "complete two-sided state reconstructed from the PMData binary book within the replay window",
        "latencies_s": list(latencies),
        "fee_bps": REPLAY_FEE_BPS,
        "sizing_assumptions": {"base_qty": REPLAY_BASE_QTY, "balance": REPLAY_BALANCE, "risk_fraction": REPLAY_RISK_FRACTION, "quantity_step": REPLAY_QTY_STEP},
        "paired_book_quality": "inferred_binary_complement",
        "chainlink": _chainlink_manifest(args.chainlink_log),
        "target_slug": "btc-updown-5m-1785677100",
        "target_slug_local_cache_status": "not_present" if not any(path.stem.endswith("1785677100") for path in files) else "present",
        "target_address": "0xa11afe967a780acad40841fa647a671874fb64a2",
        "address_baseline_report": "reports/wallet_a11afe_aftertake_analysis.md",
        "markets": market_manifest,
    }
    manifest_bytes = json.dumps(manifest, ensure_ascii=False, sort_keys=True, default=_json_default).encode("utf-8")
    manifest["manifest_sha256"] = hashlib.sha256(manifest_bytes).hexdigest()
    _write_manifest(args.manifest, manifest)
    report = _report(
        manifest=manifest,
        rows=rows,
        selected=selected,
        train_candidates=train_candidates,
        latencies=latencies,
        address_baseline_path=ROOT / "reports" / "wallet_a11afe_aftertake_analysis.md",
    )
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(report, encoding="utf-8")
    summary = {
        "kind": "complete",
        "manifest": str(args.manifest),
        "report": str(args.report),
        "markets": len(files),
        "rows": len(rows),
        "selected": selected,
        "holdout": {lane: _metric(rows, profile, lane, "unseen_holdout", "0.000") for lane, profile in selected.items()},
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2, default=_json_default), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
