#!/usr/bin/env python3
"""Replay Aftertake timing profiles from PMData L2 with conservative fill proxies.

This is a research tool, never an order runner.  It reads ``PMDATA_API_KEY``
only from the process environment, writes resumable local reports under
``out/``, and makes no Telegram or authenticated Polymarket calls.

PMData's ``poly_l2`` file is a single binary-market book.  The paired NO book
is inferred level-by-level from the YES book: NO bid ``= 1 - YES ask`` and NO
ask ``= 1 - YES bid`` (with the same size).  This replay therefore relies on
the separately observed live top-of-book complement relationship; it is not a
replacement for archived independent token-book captures.  The replay uses
PMData's *local_timestamp* as both the observed timestamp and callback order;
the exchange timestamp is retained only as source metadata.

The arrival result is deliberately labelled a marketability proxy: at the
measured submit RTT after the signal, the last locally observed book must still
show an ask no higher than the original GTC limit with enough displayed size.
It does not claim queue priority, hidden liquidity, exchange acknowledgement,
or an actual fill.
"""

from __future__ import annotations

import argparse
import bisect
import csv
import hashlib
import json
import math
import os
import time
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator, Optional

import pandas as pd

from aftertake.config import Settings
from aftertake.live_sizing import compute_live_entry_size
from aftertake.pm_client import BalanceAllowance, MarketMetadata
from aftertake.post_close import (
    PairedBook,
    PostCloseConfig,
    PostCloseDecision,
    PostCloseWinnerClassifier,
    SideBook,
)

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BASE_ROUND = 1_784_332_800
DEFAULT_COUNT = 100
DEFAULT_LATENCY_S = 0.970
METHOD_VERSION = "aftertake_pmdata_local_arrival_v2"
PROFILE_DEFINITIONS = (
    ("legacy_100_100_3", 0.100, 0.100, 3, "previous live profile"),
    ("requested_50_100_3", 0.050, 0.100, 3, "requested production profile"),
    ("prior_50_50_3", 0.050, 0.050, 3, "previous 50ms-spacing profile"),
    ("exploratory_10_10_3", 0.010, 0.010, 3, "research-only faster profile"),
)


@dataclass(frozen=True)
class TimingProfile:
    name: str
    start_s: float
    spacing_s: float
    confirmations: int
    description: str


@dataclass(frozen=True)
class Signal:
    profile: str
    signal_at: float
    side: str
    limit_price: float
    qty: float
    support_score: int
    vacuum_score: int


@dataclass(frozen=True)
class ReplaySizing:
    base_qty: float
    simulated_balance: float
    max_account_fraction: float
    quantity_step: float


def _profiles() -> tuple[TimingProfile, ...]:
    return tuple(TimingProfile(*values) for values in PROFILE_DEFINITIONS)


def _finite_float(value: Any) -> Optional[float]:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _levels(prices: Any, sizes: Any) -> list[tuple[float, float]]:
    if prices is None or sizes is None:
        return []
    result: list[tuple[float, float]] = []
    for price_raw, size_raw in zip(prices, sizes):
        price = _finite_float(price_raw)
        size = _finite_float(size_raw)
        if price is not None and size is not None and 0 < price < 1 and size > 0:
            result.append((price, size))
    return result


def _side_book(bids: dict[float, float], asks: dict[float, float]) -> SideBook:
    best_bid = max(bids, default=None)
    best_ask = min(asks, default=None)
    bid_size = float(bids.get(best_bid, 0.0)) if best_bid is not None else 0.0
    ask_size = float(asks.get(best_ask, 0.0)) if best_ask is not None else 0.0
    near_depth = (
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
        near_touch_bid_depth=near_depth,
    )


def _paired_book(row: Any, yes_bids: dict[float, float], yes_asks: dict[float, float]) -> Optional[PairedBook]:
    observed_at = _timestamp_seconds(row.get("local_timestamp"))
    source_timestamp = _timestamp_seconds(row.get("timestamp"))
    if observed_at is None or source_timestamp is None:
        return None
    # Preserve the whole depth, not merely the best level.  The reconstruction
    # assumes the observed live top-of-book YES/NO complement extends to the
    # provided L2 levels; report it as an inference rather than raw paired data.
    no_bids = {round(1.0 - price, 12): size for price, size in yes_asks.items()}
    no_asks = {round(1.0 - price, 12): size for price, size in yes_bids.items()}
    yes = _side_book(yes_bids, yes_asks)
    no = _side_book(no_bids, no_asks)
    if yes.best_bid is None or yes.best_ask is None or no.best_bid is None or no.best_ask is None:
        return None
    return PairedBook(observed_at=observed_at, source_timestamp=source_timestamp, yes=yes, no=no)


def _timestamp_seconds(value: Any) -> Optional[float]:
    if value is None or pd.isna(value):
        return None
    try:
        timestamp = pd.Timestamp(value)
        if timestamp.tzinfo is None:
            timestamp = timestamp.tz_localize("UTC")
        return float(timestamp.timestamp())
    except (TypeError, ValueError, OverflowError):
        return None


def _replay_slice(frame: pd.DataFrame, round_end: int, latency_s: float) -> pd.DataFrame:
    """Keep the last full book before the pre-close window and its deltas."""

    # Live MarketBookStream applies callback state in local receive order.  Do
    # the same here; exchange timestamps are source metadata, not a substitute
    # for callback order when a delayed packet arrives.
    frame = frame.copy()
    frame["_pmdata_local"] = pd.to_datetime(frame["local_timestamp"], utc=True, errors="coerce")
    frame = frame[frame["_pmdata_local"].notna()].sort_values("_pmdata_local", kind="stable").reset_index(drop=True)
    if frame.empty:
        return frame
    start = pd.to_datetime(round_end - 10.5, unit="s", utc=True)
    end = pd.to_datetime(round_end + 1.0 + latency_s + 0.5, unit="s", utc=True)
    snapshot_indices = frame.index[(frame["event_type"] == "book") & (frame["_pmdata_local"] <= start)].tolist()
    if snapshot_indices:
        first = snapshot_indices[-1]
    else:
        later = frame.index[(frame["event_type"] == "book") & (frame["_pmdata_local"] <= end)].tolist()
        first = later[0] if later else 0
    return frame.loc[first:][frame.loc[first:, "_pmdata_local"] <= end].copy()


def _reconstructed_books(frame: pd.DataFrame) -> Iterator[PairedBook]:
    yes_bids: dict[float, float] = {}
    yes_asks: dict[float, float] = {}
    for _, row in frame.iterrows():
        event_type = str(row.get("event_type") or "")
        if event_type == "book":
            yes_bids = dict(_levels(row.get("bid_prices"), row.get("bid_sizes")))
            yes_asks = dict(_levels(row.get("ask_prices"), row.get("ask_sizes")))
        elif event_type == "price_change":
            price = _finite_float(row.get("pc_price"))
            size = _finite_float(row.get("pc_size"))
            side = str(row.get("pc_side") or "").upper()
            if price is None or size is None or side not in {"BUY", "SELL"}:
                continue
            target = yes_bids if side == "BUY" else yes_asks
            if size <= 0:
                target.pop(price, None)
            else:
                target[price] = size
        else:
            continue
        paired = _paired_book(row, yes_bids, yes_asks)
        if paired is not None:
            yield paired


def _normalized_outcome(values: Iterable[Any]) -> str:
    for value in values:
        normalized = str(value).strip().upper()
        if normalized in {"YES", "UP"}:
            return "YES"
        if normalized in {"NO", "DOWN"}:
            return "NO"
    return ""


def _metadata() -> MarketMetadata:
    """Historical PMData L2 has no archived CLOB fee/minimum metadata.

    The dynamic sizing *path* is the production one, using the current dry-run
    balance/risk defaults.  Minimum size 1 and zero fee are explicit replay
    assumptions rather than a claim about an old market's fee schedule.
    """

    return MarketMetadata(
        condition_id="pmdata-replay",
        tick_size="0.01",
        min_order_size=1.0,
        neg_risk=False,
        fee_rate=0.0,
        tokens={},
        raw={"replay_assumption": True},
    )


def _sized_signal(
    profile: TimingProfile,
    decision: PostCloseDecision,
    now: float,
    sizing_cfg: ReplaySizing,
) -> Optional[Signal]:
    if decision.action != "enter" or decision.entry_ask is None or decision.entry_ask_size is None:
        return None
    sizing = compute_live_entry_size(
        price=decision.entry_ask,
        available_size=decision.entry_ask_size,
        collateral=BalanceAllowance(
            balance=sizing_cfg.simulated_balance,
            allowance=sizing_cfg.simulated_balance,
            raw={"simulated": True},
        ),
        metadata=_metadata(),
        max_account_fraction=sizing_cfg.max_account_fraction,
        quantity_step=sizing_cfg.quantity_step,
    )
    if not sizing.accepted:
        return None
    return Signal(
        profile=profile.name,
        signal_at=now,
        side=decision.side,
        limit_price=float(decision.entry_ask),
        qty=float(sizing.qty),
        support_score=int(decision.audit.get("support_score") or 0),
        vacuum_score=int(decision.audit.get("vacuum_score") or 0),
    )


def _run_profile(
    profile: TimingProfile,
    books: list[PairedBook],
    round_end: int,
    sizing_cfg: ReplaySizing,
) -> Optional[Signal]:
    cfg = PostCloseConfig(
        post_close_start_s=profile.start_s,
        confirmation_spacing_s=profile.spacing_s,
        confirmations=profile.confirmations,
    )
    classifier = PostCloseWinnerClassifier(cfg)
    for book in books:
        classifier.record(book)
        if not (round_end + profile.start_s <= book.observed_at <= round_end + cfg.post_close_end_s):
            continue
        initial = classifier.evaluate(
            round_end_ts=float(round_end), now_ts=book.observed_at, qty=sizing_cfg.base_qty
        )
        signal = _sized_signal(profile, initial, book.observed_at, sizing_cfg)
        if signal is None:
            continue
        # This is the runner's exact second pass after dynamic size has changed
        # the bid/near-touch support required for the residual ask.
        sized = classifier.evaluate(
            round_end_ts=float(round_end),
            now_ts=book.observed_at,
            qty=signal.qty,
            min_near_touch_qty_multiplier=1.0,
        )
        if sized.action == "enter":
            return signal
    return None


def _at_arrival(signal: Signal, books: list[PairedBook], latency_s: float) -> dict[str, Any]:
    target = signal.signal_at + latency_s
    observed = [book.observed_at for book in books]
    index = bisect.bisect_right(observed, target) - 1
    has_future_horizon = index + 1 < len(books)
    if index < 0:
        return {"arrival_complete": False, "fillable_proxy": False, "proxy_reason": "no_book_before_arrival"}
    book = books[index]
    side = book.yes if signal.side == "YES" else book.no
    if not has_future_horizon:
        return {
            "arrival_complete": False,
            "fillable_proxy": False,
            "proxy_reason": "no_book_after_arrival",
            "arrival_book_offset_ms": round((book.observed_at - target) * 1000, 3),
        }
    price_ok = side.best_ask is not None and float(side.best_ask) <= signal.limit_price + 1e-12
    size_ok = side.ask_size + 1e-12 >= signal.qty
    return {
        "arrival_complete": True,
        "arrival_book_offset_ms": round((book.observed_at - target) * 1000, 3),
        "arrival_ask": side.best_ask,
        "arrival_ask_size": side.ask_size,
        "fillable_proxy": bool(price_ok and size_ok),
        "proxy_reason": "displayed_ask_marketable" if price_ok and size_ok else "limit_or_displayed_size_not_marketable",
    }


def _pnl_proxy(signal: Signal, arrival: dict[str, Any], winning_side: str) -> tuple[Optional[bool], Optional[float]]:
    if not arrival.get("fillable_proxy") or not winning_side:
        return None, None
    price = _finite_float(arrival.get("arrival_ask"))
    if price is None:
        return None, None
    hit = signal.side == winning_side
    return hit, (1.0 - price) * signal.qty if hit else -price * signal.qty


def _load_key() -> str:
    key = os.environ.get("PMDATA_API_KEY", "").strip()
    if not key:
        raise RuntimeError("PMDATA_API_KEY must be supplied only through this process environment")
    return key


def _download(slug: str, cache: Path, key: str) -> Path:
    cache.mkdir(parents=True, exist_ok=True)
    destination = cache / f"{slug}.parquet"
    if destination.exists() and destination.stat().st_size > 4:
        return destination
    request = urllib.request.Request(
        f"https://api.pmdata.dev/download/poly_l2/{slug}.parquet",
        headers={"User-Agent": "aftertake-profile-replay/1.0", "api_key": key},
    )
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            data = response.read()
    except urllib.error.URLError as exc:
        raise RuntimeError(f"PMData download failed for {slug}: {exc.reason}") from exc
    if not (data.startswith(b"PAR1") and data.endswith(b"PAR1")):
        raise RuntimeError(f"PMData response for {slug} is not parquet")
    temporary = destination.with_suffix(".part")
    temporary.write_bytes(data)
    temporary.replace(destination)
    return destination


def _market_record(
    slug: str,
    key: str,
    cache: Path,
    latency_s: float,
    sizing_cfg: ReplaySizing,
    fingerprint: str,
) -> dict[str, Any]:
    round_end = int(slug.rsplit("-", 1)[1])
    source = _download(slug, cache, key)
    frame = pd.read_parquet(source)
    required = {"timestamp", "local_timestamp", "event_type", "winning_outcome"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise RuntimeError(f"{slug} missing PMData columns: {','.join(missing)}")
    winning_side = _normalized_outcome(frame["winning_outcome"].dropna().unique())
    books = list(_reconstructed_books(_replay_slice(frame, round_end, latency_s)))
    # ``record`` ignores duplicate/non-increasing local timestamps.  Apply the
    # same exclusion before both replay and arrival lookup.
    monotonic_books: list[PairedBook] = []
    previous = -math.inf
    for book in books:
        if book.observed_at > previous:
            monotonic_books.append(book)
            previous = book.observed_at
    records: list[dict[str, Any]] = []
    for profile in _profiles():
        signal = _run_profile(profile, monotonic_books, round_end, sizing_cfg)
        row: dict[str, Any] = {
            "slug": slug,
            "round_end": round_end,
            "profile": profile.name,
            "profile_start_ms": int(profile.start_s * 1000),
            "profile_spacing_ms": int(profile.spacing_s * 1000),
            "profile_confirmations": profile.confirmations,
            "winning_side": winning_side or None,
            "outcome_labelled": bool(winning_side),
            "reconstructed_callbacks": len(monotonic_books),
        }
        if signal is None:
            row.update({"signal": False, "arrival_complete": False, "fillable_proxy": False})
        else:
            arrival = _at_arrival(signal, monotonic_books, latency_s)
            hit, pnl = _pnl_proxy(signal, arrival, winning_side)
            row.update(
                {
                    "signal": True,
                    "signal_offset_ms": round((signal.signal_at - round_end) * 1000, 3),
                    "signal_side": signal.side,
                    "limit_price": signal.limit_price,
                    "qty": signal.qty,
                    "support_score": signal.support_score,
                    "vacuum_score": signal.vacuum_score,
                    **arrival,
                    "hit_proxy": hit,
                    "pnl_proxy_usd": pnl,
                }
            )
        records.append(row)
    return {"slug": slug, "ok": True, "fingerprint": fingerprint, "rows": records}


def _read_checkpoint(path: Path, fingerprint: str) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    completed: dict[str, dict[str, Any]] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        slug = record.get("slug")
        if isinstance(slug, str) and record.get("ok") is True and record.get("fingerprint") == fingerprint:
            completed[slug] = record
    return completed


def _append_checkpoint(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as output:
        output.write(json.dumps(record, sort_keys=True) + "\n")
        output.flush()
        os.fsync(output.fileno())


def _summary(rows: list[dict[str, Any]], latency_s: float, sizing_cfg: ReplaySizing) -> dict[str, Any]:
    profiles: dict[str, dict[str, Any]] = {}
    for profile in _profiles():
        subset = [row for row in rows if row["profile"] == profile.name]
        signals = [row for row in subset if row.get("signal")]
        complete = [row for row in signals if row.get("arrival_complete")]
        fillable = [row for row in complete if row.get("fillable_proxy")]
        settled = [row for row in fillable if row.get("outcome_labelled")]
        pnl = [float(row["pnl_proxy_usd"]) for row in settled if row.get("pnl_proxy_usd") is not None]
        profiles[profile.name] = {
            **asdict(profile),
            "markets": len(subset),
            "signals": len(signals),
            "median_signal_offset_ms": round(float(pd.Series([row["signal_offset_ms"] for row in signals]).median()), 3) if signals else None,
            "arrival_horizon_complete": len(complete),
            "displayed_marketable_proxies": len(fillable),
            "displayed_marketable_rate_of_signals": round(len(fillable) / len(signals), 6) if signals else None,
            "outcome_labelled_marketable_proxies": len(settled),
            "wins_proxy": sum(row.get("hit_proxy") is True for row in settled),
            "pnl_proxy_total_usd": round(sum(pnl), 6) if pnl else None,
            "pnl_proxy_mean_usd": round(sum(pnl) / len(pnl), 6) if pnl else None,
        }
    return {
        "method": "PMData local-arrival timing replay with GTC displayed-marketability proxy",
        "method_version": METHOD_VERSION,
        "profiles": profiles,
        "submit_latency_s": latency_s,
        "sizing": {
            "path": "current compute_live_entry_size then runner-style second classifier pass",
            "base_classification_qty": sizing_cfg.base_qty,
            "simulated_balance_and_allowance": sizing_cfg.simulated_balance,
            "max_account_fraction": sizing_cfg.max_account_fraction,
            "quantity_step": sizing_cfg.quantity_step,
            "historical_metadata_assumption": "min_order_size=1, market_fee=0, builder_fee=0",
        },
        "limitations": [
            "PMData local_timestamp is its collector receive time, not this EC2's receive time.",
            "NO depth is inferred from a single PMData book using the observed live top-of-book complement; it is not a raw second-token archive.",
            "A displayed-marketability proxy is not an exchange acknowledgement, queue position, or fill.",
            "Unlabelled outcomes are excluded from hit and PnL proxy totals.",
            "Results cover BTC 5-minute slugs only unless the script is extended deliberately.",
            "It does not model portfolio cooldown, pending GTC lifecycle, or multi-market risk state.",
        ],
    }


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-round", type=int, default=DEFAULT_BASE_ROUND)
    parser.add_argument("--count", type=int, default=DEFAULT_COUNT)
    parser.add_argument("--latency-ms", type=float, default=DEFAULT_LATENCY_S * 1000)
    parser.add_argument("--cache-dir", type=Path, default=ROOT / "out" / "pmdata_profile_replay_cache")
    parser.add_argument("--checkpoint", type=Path, default=ROOT / "out" / "aftertake_profile_replay_v2_100.checkpoint.jsonl")
    parser.add_argument("--report-prefix", type=Path, default=ROOT / "out" / "aftertake_profile_replay_v2_100")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    if args.count <= 0:
        raise SystemExit("--count must be > 0")
    if args.latency_ms < 0:
        raise SystemExit("--latency-ms must be >= 0")
    key = _load_key()
    latency_s = args.latency_ms / 1000.0
    settings = Settings.from_env()
    sizing_cfg = ReplaySizing(
        base_qty=settings.qty,
        simulated_balance=settings.dry_run_simulated_balance,
        max_account_fraction=settings.live_max_account_risk_fraction,
        quantity_step=settings.live_quantity_floor_step,
    )
    fingerprint_payload = {
        "method_version": METHOD_VERSION,
        "profiles": [asdict(profile) for profile in _profiles()],
        "latency_s": latency_s,
        "sizing": asdict(sizing_cfg),
    }
    fingerprint = hashlib.sha256(json.dumps(fingerprint_payload, sort_keys=True).encode("utf-8")).hexdigest()
    slugs = [f"btc-updown-5m-{args.base_round + 300 * index}" for index in range(args.count)]
    completed = _read_checkpoint(args.checkpoint, fingerprint)
    for index, slug in enumerate(slugs, start=1):
        if slug in completed:
            print(json.dumps({"kind": "resume_skip", "slug": slug, "index": index, "total": len(slugs)}), flush=True)
            continue
        try:
            record = _market_record(slug, key, args.cache_dir, latency_s, sizing_cfg, fingerprint)
        except Exception as exc:  # Per-market error must not discard preceding results.
            record = {
                "slug": slug,
                "ok": False,
                "fingerprint": fingerprint,
                "error_type": type(exc).__name__,
                "error": str(exc),
            }
        _append_checkpoint(args.checkpoint, record)
        completed[slug] = record
        print(json.dumps({"kind": "market_complete", "slug": slug, "ok": record["ok"], "index": index, "total": len(slugs)}), flush=True)
        time.sleep(0.05)

    selected = [completed[slug] for slug in slugs if completed.get(slug, {}).get("ok") is True]
    rows = [row for record in selected for row in record["rows"]]
    report = _summary(rows, latency_s, sizing_cfg)
    report.update(
        {
            "requested_markets": len(slugs),
            "completed_markets": len(selected),
            "failed_markets": [completed[slug] for slug in slugs if completed.get(slug, {}).get("ok") is False],
            "checkpoint": str(args.checkpoint),
            "fingerprint": fingerprint,
        }
    )
    args.report_prefix.parent.mkdir(parents=True, exist_ok=True)
    args.report_prefix.with_suffix(".json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    _write_csv(args.report_prefix.with_suffix(".csv"), rows)
    print(json.dumps({"kind": "complete", "report": str(args.report_prefix.with_suffix(".json")), "completed_markets": len(selected)}), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
