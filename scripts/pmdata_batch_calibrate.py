#!/usr/bin/env python3
"""Observation-first PMData batch replay/calibration for Aftertake.

This script is research-only. It never prints PMDATA_API_KEY. It downloads
PMData poly_l2 parquet files, reconstructs YES/NO binary order books, replays
Aftertake's classifier across the post-close window, and emits near-miss / loose
rule diagnostics so we can calibrate the strategy before changing live dry-run.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import statistics
import time
import urllib.error
import urllib.request
from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd

from aftertake.post_close import PairedBook, PostCloseWinnerClassifier, SideBook

ROOT = Path("/Users/fatsolerc/.local/share/aftertake")
CACHE = ROOT / "pmdata_cache"
REPORTS = ROOT / "reports"
ENV_PATH = ROOT / ".env"


def load_pmdata_key() -> str:
    env = ENV_PATH.read_text(errors="ignore")
    match = re.search(r"^PMDATA_API_KEY=(.+)$", env, flags=re.M)
    if not match:
        raise SystemExit("PMDATA_API_KEY missing in .env")
    return match.group(1).strip()


def download_l2(slug: str, key: str, *, sleep_s: float = 0.0) -> Path:
    CACHE.mkdir(parents=True, exist_ok=True)
    out = CACHE / f"{slug}.parquet"
    if out.exists() and out.stat().st_size > 0:
        return out
    url = f"https://api.pmdata.dev/download/poly_l2/{slug}.parquet"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0", "api_key": key})
    with urllib.request.urlopen(req, timeout=120) as response:
        data = response.read()
    if not data.startswith(b"PAR1") or not data.endswith(b"PAR1"):
        raise RuntimeError(f"bad parquet magic for {slug}")
    out.write_bytes(data)
    if sleep_s > 0:
        time.sleep(sleep_s)
    return out


def levels(prices: Any, sizes: Any) -> list[tuple[float, float]]:
    if prices is None or sizes is None:
        return []
    return [
        (float(price), float(size))
        for price, size in zip(prices, sizes)
        if price is not None and size is not None and math.isfinite(float(price)) and float(size) > 0
    ]


def size_at(book_levels: list[tuple[float, float]], price: float | None) -> float:
    if price is None:
        return 0.0
    return sum(size for level_price, size in book_levels if abs(level_price - price) < 1e-9)


def near_bid_depth(book_levels: list[tuple[float, float]], best: float | None, band: float = 0.02) -> float:
    if best is None:
        return 0.0
    return sum(size for price, size in book_levels if price >= best - band)


def near_no_bid_depth_from_yes_asks(ask_levels: list[tuple[float, float]], yes_best_ask: float | None, band: float = 0.02) -> float:
    if yes_best_ask is None:
        return 0.0
    # NO bid = 1 - YES ask. Near NO touch is YES asks close to the best YES ask.
    return sum(size for price, size in ask_levels if price <= yes_best_ask + band)


def paired_from_levels(row: Any, bid_levels: dict[float, float], ask_levels: dict[float, float]) -> PairedBook:
    bids = [(price, size) for price, size in bid_levels.items() if size > 0]
    asks = [(price, size) for price, size in ask_levels.items() if size > 0]
    yes_bid = max([price for price, _ in bids], default=None)
    yes_ask = min([price for price, _ in asks], default=None)
    yes = SideBook(
        best_bid=yes_bid,
        bid_size=size_at(bids, yes_bid),
        bid_depth=sum(size for _, size in bids),
        best_ask=yes_ask,
        ask_size=size_at(asks, yes_ask),
        near_touch_bid_depth=near_bid_depth(bids, yes_bid),
    )
    no_bid = 1.0 - yes_ask if yes_ask is not None else None
    no_ask = 1.0 - yes_bid if yes_bid is not None else None
    no = SideBook(
        best_bid=no_bid,
        bid_size=size_at(asks, yes_ask),
        bid_depth=sum(size for _, size in asks),
        best_ask=no_ask,
        ask_size=size_at(bids, yes_bid),
        near_touch_bid_depth=near_no_bid_depth_from_yes_asks(asks, yes_ask),
    )
    return PairedBook(
        observed_at=row["timestamp"].timestamp(),
        source_timestamp=row["local_timestamp"].timestamp(),
        yes=yes,
        no=no,
    )


def iter_reconstructed_books(df: pd.DataFrame):
    bid_levels: dict[float, float] = {}
    ask_levels: dict[float, float] = {}
    for _, row in df.sort_values("timestamp").iterrows():
        event_type = row["event_type"]
        if event_type == "book":
            bid_levels = {price: size for price, size in levels(row["bid_prices"], row["bid_sizes"])}
            ask_levels = {price: size for price, size in levels(row["ask_prices"], row["ask_sizes"])}
        elif event_type == "price_change":
            price = row.get("pc_price")
            side = row.get("pc_side")
            size = row.get("pc_size")
            if price is None or side is None or size is None:
                continue
            book_side = bid_levels if str(side).upper() == "BUY" else ask_levels
            price = float(price)
            size = float(size)
            if size <= 0:
                book_side.pop(price, None)
            else:
                book_side[price] = size
        else:
            continue
        if bid_levels and ask_levels:
            yield paired_from_levels(row, bid_levels, ask_levels)


def outcome_to_side(values: list[str]) -> str:
    for value in values:
        v = str(value).strip().upper()
        if v in {"YES", "NO"}:
            return v
    return ""


def side_books(book: PairedBook, side: str) -> tuple[SideBook, SideBook]:
    if side == "YES":
        return book.yes, book.no
    return book.no, book.yes


def side_top_ask(book: PairedBook, side: str) -> tuple[float | None, float]:
    if side == "YES":
        return book.yes.best_ask, book.yes.ask_size
    if side == "NO":
        return book.no.best_ask, book.no.ask_size
    return None, 0.0


def pnl_for(side: str, winning_side: str, entry_ask: float | None) -> tuple[bool | None, float | None]:
    if not side or not winning_side or entry_ask is None:
        return None, None
    hit = side == winning_side
    return hit, (1.0 - float(entry_ask)) if hit else -float(entry_ask)


def extract_snapshot(decision: Any, now_book: PairedBook, winning_side: str, qty: float) -> dict[str, Any]:
    audit = decision.audit
    side = decision.side or audit.get("candidate_side") or ""
    ask_series = audit.get("winner_ask_series") or []
    top_ask, top_ask_size = side_top_ask(now_book, side)
    entry_ask = decision.entry_ask if decision.entry_ask is not None else (ask_series[-1] if ask_series else top_ask)
    support_score = audit.get("support_score")
    vacuum_score = audit.get("vacuum_score")
    hit, pnl = pnl_for(side, winning_side, entry_ask)
    return {
        "signal_ts": now_book.observed_at,
        "now_offset_ms": round((now_book.observed_at - float(audit.get("round_end_ts", now_book.observed_at))) * 1000, 3),
        "action": decision.action,
        "reason": decision.reason,
        "side": side,
        "winning_side": winning_side,
        "hit": hit,
        "entry_ask_proxy": entry_ask,
        "entry_ask_size_proxy": top_ask_size,
        "top_ask_fillable_qty": top_ask_size >= qty,
        "pnl_proxy_1x": pnl,
        "support_score": support_score,
        "vacuum_score": vacuum_score,
        "support_components": audit.get("support_components"),
        "vacuum_components": audit.get("vacuum_components"),
        "winner_bid_series": audit.get("winner_bid_series"),
        "loser_bid_series": audit.get("loser_bid_series"),
        "winner_ask_series": ask_series,
        "reject_reasons": audit.get("reject_reasons"),
    }


def latency_fill(signal: dict[str, Any], post_books: list[PairedBook], latency_s: float, qty: float, round_end: int) -> dict[str, Any]:
    side = signal.get("side") or ""
    target_ts = float(signal.get("signal_ts") or 0.0) + latency_s
    fill_book = next((book for book in post_books if book.observed_at >= target_ts), None)
    if fill_book is None:
        return {"fillable": False, "reason": "no_future_book"}
    ask, ask_size = side_top_ask(fill_book, side)
    if ask is None:
        return {"fillable": False, "reason": "ask_missing", "fill_ts": fill_book.observed_at}
    if ask_size < qty:
        return {"fillable": False, "reason": "ask_size_too_thin", "fill_ts": fill_book.observed_at, "fill_ask": ask, "fill_ask_size": ask_size}
    hit, pnl = pnl_for(side, str(signal.get("winning_side") or ""), ask)
    return {
        "fillable": True,
        "reason": "fillable_top_ask",
        "fill_ts": fill_book.observed_at,
        "fill_offset_ms": round((fill_book.observed_at - round_end) * 1000, 3),
        "latency_s": latency_s,
        "fill_ask": ask,
        "fill_ask_size": ask_size,
        "hit": hit,
        "pnl_1x": pnl,
    }


def replay_slice(df: pd.DataFrame, round_end: int) -> pd.DataFrame:
    """Return the minimal rows needed to reconstruct close-10s through close+1s.

    A full 5m market file can contain 80k-140k rows. For calibration we need
    the last full book snapshot before close-10s plus all deltas through close+1s.
    This keeps batch replay fast without changing semantics.
    """
    start_ts = pd.to_datetime(round_end - 10, unit="s")
    end_ts = pd.to_datetime(round_end + 1, unit="s")
    before_start_books = df.index[(df["event_type"].eq("book")) & (df["timestamp"] <= start_ts)].tolist()
    if before_start_books:
        start_idx = before_start_books[-1]
    else:
        after_start_books = df.index[(df["event_type"].eq("book")) & (df["timestamp"] <= end_ts)].tolist()
        start_idx = after_start_books[0] if after_start_books else 0
    return df.loc[start_idx:][df.loc[start_idx:, "timestamp"] <= end_ts]


def analyze_market(slug: str, key: str, qty: float) -> dict[str, Any]:
    parquet = download_l2(slug, key)
    df = pd.read_parquet(parquet)
    round_end = int(slug.rsplit("-", 1)[1])
    winning_side = outcome_to_side([str(v) for v in df["winning_outcome"].dropna().unique().tolist()])
    df_slice = replay_slice(df, round_end)
    reconstructed = list(iter_reconstructed_books(df_slice))
    replay_books = [book for book in reconstructed if round_end - 10 <= book.observed_at <= round_end + 1]
    post_books = [book for book in replay_books if round_end + 0.100 <= book.observed_at <= round_end + 1.000]

    clf = PostCloseWinnerClassifier()
    snapshots: list[dict[str, Any]] = []
    first_strict_enter: dict[str, Any] | None = None
    best_snapshot: dict[str, Any] | None = None
    replay_iter = iter(replay_books)
    pending = next(replay_iter, None)
    for now_book in post_books:
        while pending is not None and pending.observed_at <= now_book.observed_at:
            clf.record(pending)
            pending = next(replay_iter, None)
        decision = clf.evaluate(round_end_ts=float(round_end), now_ts=now_book.observed_at, qty=qty, max_entry_ask=0.65)
        snapshot = extract_snapshot(decision, now_book, winning_side, qty)
        snapshots.append(snapshot)
        if first_strict_enter is None and decision.action == "enter":
            first_strict_enter = snapshot
        if snapshot.get("side") and snapshot.get("entry_ask_proxy") is not None:
            rank = (
                int(snapshot.get("support_score") or -1),
                int(snapshot.get("vacuum_score") or -1),
                -abs(float(snapshot["entry_ask_proxy"]) - 0.5),
            )
            if best_snapshot is None:
                best_snapshot = snapshot
            else:
                prev = (
                    int(best_snapshot.get("support_score") or -1),
                    int(best_snapshot.get("vacuum_score") or -1),
                    -abs(float(best_snapshot.get("entry_ask_proxy") or 0.5) - 0.5),
                )
                if rank > prev:
                    best_snapshot = snapshot

    # Observation-first loose candidates: not a production signal. These show
    # where evidence would appear if vacuum were treated as score buckets.
    loose_candidates: dict[str, dict[str, Any] | None] = {}
    for vacuum_min in range(0, 5):
        chosen = None
        for snap in snapshots:
            if not snap.get("side") or snap.get("entry_ask_proxy") is None:
                continue
            if (snap.get("support_score") or 0) >= 5 and (snap.get("vacuum_score") or 0) >= vacuum_min:
                chosen = snap
                break
        if chosen is not None:
            chosen = dict(chosen)
            chosen["latency_fills"] = {
                str(latency): latency_fill(chosen, post_books, latency, qty, round_end)
                for latency in (0.0, 0.1, 0.25)
            }
        loose_candidates[f"support5_vacuum{vacuum_min}"] = chosen

    return {
        "slug": slug,
        "round_end_utc": pd.to_datetime(round_end, unit="s").isoformat(),
        "rows_total": int(len(df)),
        "reconstructed_events": len(reconstructed),
        "replay_events": len(replay_books),
        "post_events": len(post_books),
        "winning_side": winning_side,
        "strict_first_enter": first_strict_enter,
        "final_reason": snapshots[-1]["reason"] if snapshots else "no_post_events",
        "reason_counts": dict(Counter(s["reason"] for s in snapshots)),
        "max_support_score": max([s.get("support_score") or -1 for s in snapshots], default=None),
        "max_vacuum_score": max([s.get("vacuum_score") or -1 for s in snapshots], default=None),
        "best_snapshot": best_snapshot,
        "loose_candidates": loose_candidates,
    }


def summarize(markets: list[dict[str, Any]]) -> dict[str, Any]:
    ok_markets = [m for m in markets if "error" not in m]
    strict_entries = [m["strict_first_enter"] for m in ok_markets if m.get("strict_first_enter")]
    final_reasons = Counter(m.get("final_reason") for m in ok_markets)
    max_vacuum = Counter(str(m.get("max_vacuum_score")) for m in ok_markets)
    loose_summary: dict[str, Any] = {}
    for key in [f"support5_vacuum{i}" for i in range(5)]:
        candidates = [m["loose_candidates"].get(key) for m in ok_markets if m.get("loose_candidates", {}).get(key)]
        pnls = [float(c["pnl_proxy_1x"]) for c in candidates if c.get("pnl_proxy_1x") is not None]
        hits = [bool(c["hit"]) for c in candidates if c.get("hit") is not None]
        asks = [float(c["entry_ask_proxy"]) for c in candidates if c.get("entry_ask_proxy") is not None]
        latency_summary: dict[str, Any] = {}
        for latency in ("0.0", "0.1", "0.25"):
            fills = [c.get("latency_fills", {}).get(latency) for c in candidates]
            fills = [f for f in fills if f]
            fillable = [f for f in fills if f.get("fillable")]
            fill_pnls = [float(f["pnl_1x"]) for f in fillable if f.get("pnl_1x") is not None]
            fill_hits = [bool(f["hit"]) for f in fillable if f.get("hit") is not None]
            fill_asks = [float(f["fill_ask"]) for f in fillable if f.get("fill_ask") is not None]
            latency_summary[latency] = {
                "fillable": len(fillable),
                "unfillable": len(fills) - len(fillable),
                "fill_rate": (len(fillable) / len(fills)) if fills else None,
                "hit_rate": (sum(fill_hits) / len(fill_hits)) if fill_hits else None,
                "avg_pnl_1x": statistics.fmean(fill_pnls) if fill_pnls else None,
                "sum_pnl_1x": sum(fill_pnls) if fill_pnls else None,
                "avg_fill_ask": statistics.fmean(fill_asks) if fill_asks else None,
                "unfillable_reasons": dict(Counter(str(f.get("reason")) for f in fills if not f.get("fillable"))),
            }
        loose_summary[key] = {
            "trades": len(candidates),
            "hit_rate": (sum(hits) / len(hits)) if hits else None,
            "avg_pnl_1x": statistics.fmean(pnls) if pnls else None,
            "sum_pnl_1x": sum(pnls) if pnls else None,
            "avg_entry_ask": statistics.fmean(asks) if asks else None,
            "latency_fill_summary": latency_summary,
        }
    return {
        "key": "[REDACTED]",
        "markets_attempted": len(markets),
        "markets_ok": len(ok_markets),
        "markets_error": len(markets) - len(ok_markets),
        "strict_entries": len(strict_entries),
        "final_reasons": dict(final_reasons),
        "max_vacuum_score_distribution": dict(max_vacuum),
        "loose_rule_summary": loose_summary,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", type=int, default=1784332800)
    parser.add_argument("--count", type=int, default=72)
    parser.add_argument("--before", type=int, default=24)
    parser.add_argument("--qty", type=float, default=5.0)
    parser.add_argument("--sleep", type=float, default=0.0)
    parser.add_argument("--out", default=str(REPORTS / "pmdata_batch_calibration.json"))
    args = parser.parse_args()
    key = load_pmdata_key()
    start = args.base - args.before * 300
    slugs = [f"btc-updown-5m-{start + i * 300}" for i in range(args.count)]
    markets = []
    for slug in slugs:
        try:
            markets.append(analyze_market(slug, key, args.qty))
        except urllib.error.HTTPError as exc:
            markets.append({"slug": slug, "error": f"HTTP_{exc.code}"})
        except Exception as exc:  # noqa: BLE001 - research report should keep going.
            markets.append({"slug": slug, "error": type(exc).__name__, "message": str(exc)[:200]})
        if args.sleep > 0:
            time.sleep(args.sleep)
    report = {"summary": summarize(markets), "markets": markets}
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    print(json.dumps(report["summary"], ensure_ascii=False, indent=2, default=str))
    print(f"report={out_path}")


if __name__ == "__main__":
    main()
