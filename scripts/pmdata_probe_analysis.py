import json
import re
import urllib.request
from pathlib import Path

import pandas as pd

from aftertake.post_close import PairedBook, PostCloseWinnerClassifier, SideBook
from aftertake.rounds import crypto_5m_bounds_from_slug


def load_pmdata_key() -> str:
    env_path = Path("/Users/fatsolerc/.local/share/aftertake/.env")
    env = env_path.read_text(errors="ignore")
    match = re.search(r"^PMDATA_API_KEY=(.+)$", env, flags=re.M)
    if not match:
        raise SystemExit("PMDATA_API_KEY missing")
    return match.group(1).strip()


def download_l2(slug: str, key: str) -> Path:
    out_dir = Path("/Users/fatsolerc/.local/share/aftertake/pmdata_cache")
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"{slug}.parquet"
    if out.exists() and out.stat().st_size > 0:
        return out
    url = f"https://api.pmdata.dev/download/poly_l2/{slug}.parquet"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0", "api_key": key})
    out.write_bytes(urllib.request.urlopen(req, timeout=120).read())
    return out


def size_at(levels, price):
    if price is None:
        return 0.0
    return sum(size for level_price, size in levels if abs(level_price - price) < 1e-9)


def near_bid_depth(levels, best):
    if best is None:
        return 0.0
    return sum(size for price, size in levels if price >= best - 0.02)


def near_no_bid_depth_from_yes_asks(ask_levels, yes_best_ask):
    if yes_best_ask is None:
        return 0.0
    return sum(size for price, size in ask_levels if price <= yes_best_ask + 0.02)


def levels(prices, sizes):
    if prices is None or sizes is None:
        return []
    return [
        (float(price), float(size))
        for price, size in zip(prices, sizes)
        if price is not None and size is not None and float(size) > 0
    ]


def paired_from_levels(row, bid_levels: dict[float, float], ask_levels: dict[float, float]) -> PairedBook:
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
    # PMData L2 is the YES book. In a binary pair, the NO book can be derived:
    # NO bid = 1 - YES ask; NO ask = 1 - YES bid. Sizes come from the opposite side.
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


def iter_reconstructed_books(df):
    """Yield reconstructed YES/NO books from PMData book snapshots + price_change deltas."""
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


def analyze_slug(slug: str) -> dict:
    key = load_pmdata_key()
    parquet_path = download_l2(slug, key)
    df = pd.read_parquet(parquet_path)
    books = df[df["event_type"].eq("book")].copy()
    round_start, round_end = crypto_5m_bounds_from_slug(slug)
    start = pd.to_datetime(round_end - 10, unit="s")
    end = pd.to_datetime(round_end + 1, unit="s")
    window = books[(books["timestamp"] >= start) & (books["timestamp"] <= end)]
    clf = PostCloseWinnerClassifier()
    all_books = [book for book in iter_reconstructed_books(df)]
    window_books = [book for book in all_books if round_end - 10 <= book.observed_at <= round_end + 1]
    clf = PostCloseWinnerClassifier()
    for book in window_books:
        clf.record(book)
    decision = clf.evaluate(round_end_ts=float(round_end), now_ts=float(round_end + 1.0), qty=5.0, max_entry_ask=0.65)
    return {
        "key": "[REDACTED]",
        "download_probe": "passed",
        "file": str(parquet_path),
        "slug": slug,
        "rows_total": int(len(df)),
        "book_snapshots_total": int(len(books)),
        "reconstructed_book_events_total": int(len(all_books)),
        "window_book_snapshots": int(len(window)),
        "window_reconstructed_events": int(len(window_books)),
        "round_start_utc": pd.to_datetime(round_start, unit="s").isoformat(),
        "round_end_utc": pd.to_datetime(round_end, unit="s").isoformat(),
        "time_min": str(df["timestamp"].min()),
        "time_max": str(df["timestamp"].max()),
        "winning_outcome_values": [str(value) for value in df["winning_outcome"].dropna().unique()[:5]],
        "decision": {
            "action": decision.action,
            "reason": decision.reason,
            "side": decision.side,
            "entry_ask": decision.entry_ask,
            "entry_ask_size": decision.entry_ask_size,
            "winner_bid": decision.winner_bid,
            "loser_bid": decision.loser_bid,
            "confirmations": decision.confirmations,
        },
        "audit_subset": {
            "preclose_scene_gate": decision.audit.get("preclose_scene_gate"),
            "preclose_scene_label": decision.audit.get("preclose_scene_label"),
            "preclose_scene_warnings": decision.audit.get("preclose_scene_warnings"),
            "postclose_count": decision.audit.get("postclose_count"),
            "support_score": decision.audit.get("support_score"),
            "vacuum_score": decision.audit.get("vacuum_score"),
            "reject_reasons": decision.audit.get("reject_reasons"),
            "winner_bid_series": decision.audit.get("winner_bid_series"),
            "loser_bid_series": decision.audit.get("loser_bid_series"),
            "winner_ask_series": decision.audit.get("winner_ask_series"),
            "ask_lag": decision.audit.get("ask_lag"),
        },
    }


if __name__ == "__main__":
    print(json.dumps(analyze_slug("btc-updown-5m-1784332800"), ensure_ascii=False, indent=2, default=str))
