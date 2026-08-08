from __future__ import annotations

import argparse
import gzip
import json
import math
import re
from collections import Counter
from pathlib import Path
from statistics import mean
from typing import Any, Iterator, Optional


ROUND_RE = re.compile(r"^round_(\d{6})_")
PM_RESOLVED_STATUSES = {"resolved", "finalized"}
DEFAULT_DATA_DIR = Path("research_data/four_quadrant_resolution_window_v1")


def _as_float(value: Any) -> Optional[float]:
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) else None


def _received_ms(record: dict[str, Any]) -> Optional[int]:
    value = _as_float(record.get("received_ts_ms"))
    if value is not None:
        return int(value)
    value = _as_float(record.get("received_ts"))
    return None if value is None else int(value * 1000)


def _best_bid(record: dict[str, Any]) -> Optional[float]:
    bid = _as_float(record.get("best_bid"))
    if bid is not None:
        return bid
    if record.get("kind") != "pm_book_snapshot":
        return None
    bids = record.get("bids")
    if not isinstance(bids, list):
        return None
    candidates = [
        price
        for level in bids
        if isinstance(level, dict)
        for price in [_as_float(level.get("price"))]
        if price is not None
    ]
    return max(candidates, default=None)


def _read_jsonl(path: Path) -> Iterator[dict[str, Any]]:
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as source:
        for line in source:
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                yield value


def t1_quotes(
    pm_events: Path,
    *,
    end_ts_ms: int,
    asset: str,
    decision_offset_ms: int = 1_000,
) -> dict[str, Optional[dict[str, Any]]]:
    """Return quotes at the decision clock; a negative offset is post-end."""
    cutoff_ms = end_ts_ms - decision_offset_ms
    quotes: dict[str, Optional[dict[str, Any]]] = {"YES": None, "NO": None}
    last_received_ms = -1
    for record in _read_jsonl(pm_events):
        received_ms = _received_ms(record)
        if received_ms is None:
            continue
        # The collector appends records in receive order. The first later message
        # therefore marks the end of information available to a T−1 decision.
        if received_ms > cutoff_ms:
            break
        last_received_ms = max(last_received_ms, received_ms)
        if record.get("asset") != asset:
            continue
        outcome = str(record.get("outcome") or "").upper()
        if outcome not in quotes:
            continue
        bid = _best_bid(record)
        if bid is not None:
            quotes[outcome] = {
                "best_bid": bid,
                "received_ts_ms": received_ms,
                "source_ts_ms": _as_float(record.get("source_ts_ms")),
                "kind": record.get("kind"),
            }
    return quotes


def pm_winner(summary: dict[str, Any], *, asset: str) -> tuple[str, dict[str, Any]]:
    resolution = summary.get("resolution")
    if not isinstance(resolution, dict) or resolution.get("status") != "resolved":
        raise ValueError("round lacks observed PM resolution")
    markets = resolution.get("markets")
    if not isinstance(markets, list):
        raise ValueError("round lacks PM market resolution details")
    market = next((item for item in markets if item.get("asset") == asset), None)
    if not isinstance(market, dict):
        raise ValueError(f"round lacks {asset} PM resolution")
    status = str(market.get("uma_resolution_status") or "").lower()
    up_price = _as_float(market.get("up_price"))
    if not market.get("resolved") or status not in PM_RESOLVED_STATUSES:
        raise ValueError("final label is not PM UMA resolution")
    if up_price is not None and up_price >= 0.99:
        return "YES", market
    if up_price is not None and up_price <= 0.01:
        return "NO", market
    raise ValueError("PM resolved market has no terminal Up price")


def evaluate_round(
    round_dir: Path,
    *,
    asset: str = "BTC",
    min_leader_bid: float = 0.80,
    decision_offset_ms: int = 1_000,
) -> dict[str, Any]:
    metadata = json.loads((round_dir / "metadata.json").read_text())
    end_ts_ms = int(metadata["markets"][asset]["end_ts_ms"])
    summary = json.loads((round_dir / "round_summary.json").read_text())
    winner, pm_evidence = pm_winner(summary, asset=asset)
    event_paths = sorted(round_dir.glob("pm_book_events.jsonl*"))
    if len(event_paths) != 1:
        raise ValueError("expected exactly one PM order-book event file")
    quotes = t1_quotes(
        event_paths[0],
        end_ts_ms=end_ts_ms,
        asset=asset,
        decision_offset_ms=decision_offset_ms,
    )
    yes, no = quotes["YES"], quotes["NO"]
    result: dict[str, Any] = {
        "round": int(ROUND_RE.match(round_dir.name).group(1)),
        "round_dir": str(round_dir),
        "asset": asset,
        "decision_cutoff_ms": end_ts_ms - decision_offset_ms,
        "pm_winner": winner,
        "pm_resolution_evidence": {
            "status": summary["resolution"]["status"],
            "uma_resolution_status": pm_evidence["uma_resolution_status"],
            "resolution_reason": pm_evidence["resolution_reason"],
            "up_price": pm_evidence["up_price"],
        },
        "yes_quote": yes,
        "no_quote": no,
        "eligible": False,
        "decision": None,
        "correct": None,
        "skip_reason": None,
    }
    if yes is None or no is None:
        result["skip_reason"] = "missing_t1_quote"
        return result
    yes_bid, no_bid = float(yes["best_bid"]), float(no["best_bid"])
    if yes_bid == no_bid:
        result["skip_reason"] = "tied_best_bid"
        return result
    leader = "YES" if yes_bid > no_bid else "NO"
    leader_quote = quotes[leader]
    assert leader_quote is not None
    if float(leader_quote["best_bid"]) < min_leader_bid:
        result["skip_reason"] = "leader_below_threshold"
        return result
    result.update({
        "eligible": True,
        "decision": leader,
        "leader_best_bid": leader_quote["best_bid"],
        "leader_quote_age_ms": result["decision_cutoff_ms"] - leader_quote["received_ts_ms"],
        "correct": leader == winner,
    })
    return result


def _round_dirs(data_dir: Path, *, max_round: int) -> list[Path]:
    parsed: list[tuple[int, Path]] = []
    for path in data_dir.glob("round_*"):
        match = ROUND_RE.match(path.name)
        if match and int(match.group(1)) <= max_round:
            parsed.append((int(match.group(1)), path))
    parsed.sort()
    return [path for _, path in parsed]


def _wilson_95(successes: int, trials: int) -> Optional[list[float]]:
    if trials == 0:
        return None
    z = 1.959963984540054
    p = successes / trials
    denom = 1 + z * z / trials
    center = (p + z * z / (2 * trials)) / denom
    margin = z * math.sqrt((p * (1 - p) + z * z / (4 * trials)) / trials) / denom
    return [center - margin, center + margin]


def metrics_at_threshold(rows: list[dict[str, Any]], *, min_leader_bid: float) -> dict[str, Any]:
    """Re-score identical decision-clock quotes under a different entry threshold."""
    eligible: list[dict[str, Any]] = []
    skips: Counter[str] = Counter()
    quote_ages: list[float] = []
    for row in rows:
        yes, no = row.get("yes_quote"), row.get("no_quote")
        if yes is None or no is None:
            skips["missing_t1_quote"] += 1
            continue
        yes_bid, no_bid = float(yes["best_bid"]), float(no["best_bid"])
        if yes_bid == no_bid:
            skips["tied_best_bid"] += 1
            continue
        leader = "YES" if yes_bid > no_bid else "NO"
        leader_quote = yes if leader == "YES" else no
        if float(leader_quote["best_bid"]) < min_leader_bid:
            skips["leader_below_threshold"] += 1
            continue
        eligible.append({**row, "decision": leader, "correct": leader == row["pm_winner"]})
        quote_ages.append(float(row["decision_cutoff_ms"] - leader_quote["received_ts_ms"]))
    wins = sum(bool(row["correct"]) for row in eligible)
    return {
        "min_leader_best_bid": min_leader_bid,
        "eligible_decisions": len(eligible),
        "skipped_rounds": len(rows) - len(eligible),
        "skip_reasons": dict(sorted(skips.items())),
        "wins": wins,
        "losses": len(eligible) - wins,
        "accuracy": None if not eligible else wins / len(eligible),
        "wilson_95_confidence_interval": _wilson_95(wins, len(eligible)),
        "leader_quote_age_ms": None if not quote_ages else {
            "mean": mean(quote_ages), "max": max(quote_ages),
        },
    }


def evaluate(
    data_dir: Path,
    *,
    max_round: int = 240,
    asset: str = "BTC",
    min_leader_bid: float = 0.80,
    decision_offset_ms: int = 1_000,
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    invalid: list[dict[str, Any]] = []
    round_dirs = _round_dirs(data_dir, max_round=max_round)
    expected = set(range(1, max_round + 1))
    observed = {int(ROUND_RE.match(path.name).group(1)) for path in round_dirs}
    for round_dir in round_dirs:
        try:
            rows.append(evaluate_round(
                round_dir,
                asset=asset,
                min_leader_bid=min_leader_bid,
                decision_offset_ms=decision_offset_ms,
            ))
        except Exception as exc:
            invalid.append({"round_dir": str(round_dir), "error": repr(exc)})
    eligible = [row for row in rows if row["eligible"]]
    wins = sum(bool(row["correct"]) for row in eligible)
    skip_counts = Counter(row["skip_reason"] for row in rows if row["skip_reason"])
    quote_ages = [float(row["leader_quote_age_ms"]) for row in eligible]
    return {
        "strategy": {
            "asset": asset,
            "decision_clock": "latest locally received PM best-bid state at end_ts_ms - decision_offset_ms",
            "decision_offset_ms": decision_offset_ms,
            "decision_relative_to_end_ms": -decision_offset_ms,
            "offset_convention": "positive decision_offset_ms means T-minus; negative means T-plus",
            "leader_rule": "higher of YES/NO best bid; ties skip",
            "min_leader_best_bid": min_leader_bid,
            "settlement_label": "PM UMA resolved/finalized final Up price only",
            "not_measured": "T+0 GTC 0.99 fill probability, fill quantity, fees, and PnL",
        },
        "input": {
            "data_dir": str(data_dir),
            "requested_rounds": max_round,
            "found_rounds": len(round_dirs),
            "missing_rounds": sorted(expected - observed),
            "invalid_rounds": invalid,
        },
        "metrics": {
            "pm_resolved_labeled_rounds": len(rows),
            "eligible_decisions": len(eligible),
            "skipped_rounds": len(rows) - len(eligible),
            "skip_reasons": dict(sorted(skip_counts.items())),
            "wins": wins,
            "losses": len(eligible) - wins,
            "accuracy": None if not eligible else wins / len(eligible),
            "wilson_95_confidence_interval": _wilson_95(wins, len(eligible)),
            "leader_quote_age_ms": None if not quote_ages else {
                "mean": mean(quote_ages), "max": max(quote_ages),
            },
        },
        "rounds": rows,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay a T−1 PM best-bid leader rule against PM UMA resolution.")
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument("--max-round", type=int, default=240)
    parser.add_argument("--asset", choices=("BTC", "ETH"), default="BTC")
    parser.add_argument("--min-leader-bid", type=float, default=0.80)
    parser.add_argument(
        "--decision-offset-ms",
        type=int,
        default=1_000,
        help="positive means T-minus; e.g. -500 means T+0.5 seconds",
    )
    parser.add_argument(
        "--compare-threshold",
        type=float,
        action="append",
        help="additional leader-bid thresholds to score from the same replay",
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    report = evaluate(
        args.data_dir,
        max_round=args.max_round,
        asset=args.asset,
        min_leader_bid=args.min_leader_bid,
        decision_offset_ms=args.decision_offset_ms,
    )
    if args.compare_threshold:
        report["threshold_comparisons"] = {
            str(threshold): metrics_at_threshold(report["rounds"], min_leader_bid=threshold)
            for threshold in dict.fromkeys(args.compare_threshold)
        }
    encoded = json.dumps(report, ensure_ascii=False, indent=2)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded + "\n")
    print(encoded)


if __name__ == "__main__":
    main()
