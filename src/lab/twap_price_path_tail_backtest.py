#!/usr/bin/env python3
"""Replay a price-path tail veto for PM crypto 5m markets.

This is deliberately a research-only, no-order tool.  It uses a pre-decision
PM best-bid leader as the proposed side, official Binance Spot aggregate trades
as a *proxy* price path, and only the locally observed final PM/UMA resolution
as the label.  It never treats Binance as the settlement oracle.

The strategy under test is the user's hypothesis:

* decide at ``E - 10s - safety_buffer``;
* require the PM leader to agree with the intrabar Binance direction;
* when the 5m move is weak (<= 5 bp), veto a tail reversal;
* when the 5m move is strong (> 5 bp), allow progressively more tail noise.

It caches only compact decision features, not downloaded raw market data, so a
parameter rerun does not rescan multi-gigabyte CLOB captures or re-fetch APIs.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import csv
import gzip
import json
import math
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
for item in (str(ROOT), str(SRC)):
    if item not in sys.path:
        sys.path.insert(0, item)

from lab.t1_leader_backtest import pm_winner


ASSET_SYMBOLS = {
    "BTC": "BTCUSDT",
    "ETH": "ETHUSDT",
    "SOL": "SOLUSDT",
    "XRP": "XRPUSDT",
    "BNB": "BNBUSDT",
    "DOGE": "DOGEUSDT",
}
DEFAULT_ASSETS = tuple(ASSET_SYMBOLS)
DEFAULT_CUTOVER_TS = 1_786_060_800  # 2026-08-07T00:00:00Z
DEFAULT_API_BASE = "https://data-api.binance.vision/api/v3"
SCHEMA_VERSION = "twap_price_path_tail_backtest.v1"


class DataError(RuntimeError):
    """A source was unavailable or did not satisfy the replay contract."""


def finite(value: Any) -> Optional[float]:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def parse_csv_set(value: str) -> tuple[str, ...]:
    requested = tuple(dict.fromkeys(piece.strip().upper() for piece in value.split(",") if piece.strip()))
    unknown = [asset for asset in requested if asset not in ASSET_SYMBOLS]
    if unknown:
        raise argparse.ArgumentTypeError(f"unsupported assets: {','.join(unknown)}")
    if not requested:
        raise argparse.ArgumentTypeError("at least one asset is required")
    return requested


def open_bytes(path: Path):
    return gzip.open(path, "rb") if path.suffix == ".gz" else open(path, "rb")


def number_at(line: bytes, marker: bytes) -> Optional[float]:
    """Fast scalar parser for the compact JSONL event format."""
    index = line.rfind(marker)
    if index < 0:
        return None
    index += len(marker)
    end = index
    while end < len(line) and line[end] not in b",}":
        end += 1
    raw = line[index:end]
    if raw in (b"", b"null"):
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def received_ms(line: bytes) -> Optional[int]:
    value = number_at(line, b'"received_ts_ms":')
    if value is not None:
        return int(value)
    value = number_at(line, b'"received_ts":')
    return None if value is None else int(value * 1000.0)


def snapshot_bid(line: bytes) -> Optional[float]:
    try:
        bids = json.loads(line).get("bids")
    except (json.JSONDecodeError, AttributeError):
        return None
    if not isinstance(bids, list):
        return None
    values = [finite(level.get("price")) for level in bids if isinstance(level, dict)]
    return max((value for value in values if value is not None), default=None)


def event_asset(line: bytes, assets: tuple[str, ...]) -> Optional[str]:
    for asset in assets:
        if f'"asset":"{asset}"'.encode() in line:
            return asset
    return None


def event_outcome(line: bytes) -> Optional[str]:
    if b'"outcome":"YES"' in line:
        return "YES"
    if b'"outcome":"NO"' in line:
        return "NO"
    return None


def round_number(round_dir: Path) -> int:
    parts = round_dir.name.split("_")
    if len(parts) < 3:
        raise ValueError(f"unexpected round directory: {round_dir}")
    return int(parts[1])


def round_candidate_rows(
    round_dir: Path,
    assets: tuple[str, ...],
    cutover_ts: int,
    decision_tte_ms: int,
    safety_buffer_ms: int,
    base_min_bid: float,
) -> tuple[list[dict[str, Any]], Counter[str]]:
    """Extract exact pre-cutoff PM leader states from one local raw round."""
    skips: Counter[str] = Counter()
    try:
        metadata = json.loads((round_dir / "metadata.json").read_text())
        summary = json.loads((round_dir / "round_summary.json").read_text())
    except (OSError, json.JSONDecodeError) as exc:
        return [], Counter({f"metadata_or_summary_error:{type(exc).__name__}": 1})
    markets = metadata.get("markets")
    if not isinstance(markets, dict):
        return [], Counter({"missing_markets": 1})
    available = tuple(asset for asset in assets if isinstance(markets.get(asset), dict))
    if not available:
        return [], Counter({"no_requested_assets": 1})
    start_values = [int(markets[asset].get("start_ts") or 0) for asset in available]
    if not start_values or min(start_values) < cutover_ts:
        return [], Counter({"pre_cutover_round": len(available)})

    # All six markets in a capture normally share an end clock, but preserve the
    # per-asset end in output so an anomalous round cannot silently be aligned.
    end_by_asset = {asset: int(markets[asset]["end_ts_ms"]) for asset in available}
    cutoff_by_asset = {
        asset: end_ms - decision_tte_ms - safety_buffer_ms
        for asset, end_ms in end_by_asset.items()
    }
    shared_cutoff = min(cutoff_by_asset.values())
    paths = list(round_dir.glob("pm_book_events.jsonl*"))
    if len(paths) != 1:
        return [], Counter({"missing_pm_event_file": len(available)})

    state: dict[str, dict[str, Optional[dict[str, float]]]] = {
        asset: {"YES": None, "NO": None} for asset in available
    }
    try:
        with open_bytes(paths[0]) as source:
            for line in source:
                event_time = received_ms(line)
                if event_time is None:
                    continue
                # Files are append-only in local receive order.  The first later
                # record is therefore the information boundary of this replay.
                if event_time > shared_cutoff:
                    break
                asset = event_asset(line, available)
                if asset is None or event_time > cutoff_by_asset[asset]:
                    continue
                outcome = event_outcome(line)
                if outcome is None:
                    continue
                bid = number_at(line, b'"best_bid":')
                if bid is None and b'"kind":"pm_book_snapshot"' in line:
                    bid = snapshot_bid(line)
                if bid is not None:
                    state[asset][outcome] = {
                        "best_bid": bid,
                        "received_ts_ms": float(event_time),
                    }
    except (OSError, EOFError, gzip.BadGzipFile) as exc:
        return [], Counter({f"pm_read_error:{type(exc).__name__}": len(available)})

    rows: list[dict[str, Any]] = []
    for asset in available:
        skips["asset_windows"] += 1
        yes, no = state[asset]["YES"], state[asset]["NO"]
        if yes is None or no is None:
            skips["missing_pm_quote"] += 1
            continue
        yes_bid, no_bid = float(yes["best_bid"]), float(no["best_bid"])
        if yes_bid == no_bid:
            skips["tied_pm_leader"] += 1
            continue
        side, leader_quote = ("YES", yes) if yes_bid > no_bid else ("NO", no)
        leader_bid = float(leader_quote["best_bid"])
        if leader_bid < base_min_bid:
            skips["leader_below_base_bid"] += 1
            continue
        try:
            winner, evidence = pm_winner(summary, asset=asset)
        except (ValueError, KeyError, TypeError) as exc:
            skips[f"invalid_pm_uma_label:{type(exc).__name__}"] += 1
            continue
        cutoff = cutoff_by_asset[asset]
        rows.append({
            "round": round_number(round_dir),
            "round_dir": str(round_dir),
            "asset": asset,
            "slug": markets[asset].get("slug"),
            "start_ts_ms": int(markets[asset]["start_ts"]) * 1000,
            "end_ts_ms": end_by_asset[asset],
            "decision_cutoff_ms": cutoff,
            "pm_side": side,
            "pm_winner": winner,
            "leader_bid": leader_bid,
            "yes_bid": yes_bid,
            "no_bid": no_bid,
            "leader_quote_received_ms": int(leader_quote["received_ts_ms"]),
            "leader_quote_age_ms": cutoff - int(leader_quote["received_ts_ms"]),
            "pm_resolution_evidence": {
                "uma_resolution_status": evidence.get("uma_resolution_status"),
                "up_price": evidence.get("up_price"),
                "resolution_reason": evidence.get("resolution_reason"),
            },
        })
    return rows, skips


def _candidate_task(args: tuple[Path, tuple[str, ...], int, int, int, float]) -> tuple[list[dict[str, Any]], Counter[str]]:
    return round_candidate_rows(*args)


def discover_rounds(data_dir: Path, round_min: int, round_max: int) -> list[Path]:
    result: list[Path] = []
    for summary_path in data_dir.glob("round_*/round_summary.json"):
        try:
            number = round_number(summary_path.parent)
        except ValueError:
            continue
        if round_min <= number <= round_max:
            result.append(summary_path.parent)
    return sorted(result, key=round_number)


def extract_candidates(
    *,
    data_dir: Path,
    assets: tuple[str, ...],
    cutover_ts: int,
    decision_tte_ms: int,
    safety_buffer_ms: int,
    base_min_bid: float,
    round_min: int,
    round_max: int,
    workers: int,
) -> dict[str, Any]:
    rounds = discover_rounds(data_dir, round_min, round_max)
    task_args = [
        (round_dir, assets, cutover_ts, decision_tte_ms, safety_buffer_ms, base_min_bid)
        for round_dir in rounds
    ]
    all_rows: list[dict[str, Any]] = []
    skips: Counter[str] = Counter()
    print(json.dumps({"phase": "pm_candidate_extract", "round_dirs": len(task_args), "workers": workers}), flush=True)
    if workers <= 1:
        iterator: Iterable[tuple[list[dict[str, Any]], Counter[str]]] = map(_candidate_task, task_args)
        for index, (rows, local_skips) in enumerate(iterator, start=1):
            all_rows.extend(rows)
            skips.update(local_skips)
            if index % 10 == 0 or index == len(task_args):
                print(json.dumps({"phase": "pm_candidate_extract", "done": index, "total": len(task_args)}), flush=True)
    else:
        with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(_candidate_task, task) for task in task_args]
            for index, future in enumerate(concurrent.futures.as_completed(futures), start=1):
                rows, local_skips = future.result()
                all_rows.extend(rows)
                skips.update(local_skips)
                if index % 10 == 0 or index == len(task_args):
                    print(json.dumps({"phase": "pm_candidate_extract", "done": index, "total": len(task_args)}), flush=True)
    all_rows.sort(key=lambda row: (int(row["start_ts_ms"]), str(row["asset"]), int(row["round"])))
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "pm_predecision_candidate_cache",
        "data_dir": str(data_dir),
        "assets": list(assets),
        "cutover_ts": cutover_ts,
        "decision_tte_ms": decision_tte_ms,
        "safety_buffer_ms": safety_buffer_ms,
        "base_min_bid": base_min_bid,
        "round_min": round_min,
        "round_max": round_max,
        "round_dirs_scanned": len(rounds),
        "skip_counts": dict(sorted(skips.items())),
        "records": all_rows,
    }


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n")


def load_matching_cache(path: Path, expected: dict[str, Any]) -> Optional[dict[str, Any]]:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    if all(payload.get(key) == value for key, value in expected.items()):
        return payload
    return None


def api_json(base_url: str, endpoint: str, params: dict[str, Any], *, retries: int = 5) -> Any:
    url = f"{base_url.rstrip('/')}/{endpoint}?{urlencode(params)}"
    last_error: Optional[BaseException] = None
    for attempt in range(retries):
        try:
            request = Request(url, headers={"User-Agent": "cor-pol-research/1.0"})
            with urlopen(request, timeout=25) as response:  # nosec B310: fixed official HTTPS base
                raw = response.read()
            return json.loads(raw)
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_error = exc
            # Public API 429/5xx is transient; bounded exponential backoff avoids
            # hammering the source and lets the caller mark true failures missing.
            if attempt + 1 < retries:
                time.sleep(min(8.0, 0.5 * (2 ** attempt)))
    raise DataError(f"{endpoint} failed after {retries} attempts: {last_error!r}")


def fetch_kline_opens(base_url: str, asset: str, starts: list[int]) -> dict[int, float]:
    if not starts:
        return {}
    symbol = ASSET_SYMBOLS[asset]
    lo, hi = min(starts), max(starts) + 299_999
    result: dict[int, float] = {}
    cursor = lo
    # A 5m range is normally only ~200 candles in this data set.  Keep the loop
    # for a future longer local collection without silently truncating at 1,000.
    while cursor <= hi:
        rows = api_json(base_url, "klines", {
            "symbol": symbol,
            "interval": "5m",
            "startTime": cursor,
            "endTime": hi,
            "limit": 1000,
        })
        if not isinstance(rows, list) or not rows:
            break
        for row in rows:
            if not isinstance(row, list) or len(row) < 2:
                continue
            open_ms = int(row[0])
            value = finite(row[1])
            if value is not None:
                result[open_ms] = value
        if len(rows) < 1000:
            break
        cursor = int(rows[-1][0]) + 300_000
    return result


def fetch_agg_window(base_url: str, symbol: str, start_ms: int, cutoff_ms: int) -> list[dict[str, Any]]:
    """Fetch all aggregate trades in a short range without same-ms gaps."""
    params: dict[str, Any] = {
        "symbol": symbol,
        "startTime": start_ms,
        "endTime": cutoff_ms,
        "limit": 1000,
    }
    seen: set[int] = set()
    records: list[dict[str, Any]] = []
    for _page in range(12):
        page = api_json(base_url, "aggTrades", params)
        if not isinstance(page, list):
            raise DataError("aggTrades response is not a list")
        if not page:
            break
        for row in page:
            if not isinstance(row, dict):
                continue
            trade_id = row.get("a")
            trade_ms = finite(row.get("T"))
            if trade_id is None or trade_ms is None:
                continue
            trade_id = int(trade_id)
            if trade_id in seen:
                continue
            seen.add(trade_id)
            records.append(row)
        latest = page[-1] if page else None
        latest_ms = finite(latest.get("T")) if isinstance(latest, dict) else None
        latest_id = latest.get("a") if isinstance(latest, dict) else None
        if len(page) < 1000 or latest_ms is None or latest_ms > cutoff_ms:
            break
        if latest_id is None:
            raise DataError("aggTrades page lacks aggregate id")
        # fromId is inclusive by contract, hence +1.  Do not advance by time:
        # several aggregate trades can share the same millisecond.
        params = {"symbol": symbol, "fromId": int(latest_id) + 1, "limit": 1000}
    else:
        raise DataError("aggTrades pagination exceeded 12 pages in a 31s window")
    records.sort(key=lambda row: (int(row.get("T") or 0), int(row.get("a") or 0)))
    return records


def fetch_anchor_before(base_url: str, symbol: str, tail_start_ms: int) -> dict[str, Any]:
    """Fetch the most recent aggregate trade no later than the tail window.

    The short-window query normally includes this anchor.  Low-turnover pairs
    can go quiet for more than one second, in which case their *live* stream
    would still have a last price.  This bounded fallback reconstructs that
    state instead of falsely calling the historical path missing.
    """
    rows = api_json(base_url, "aggTrades", {
        "symbol": symbol,
        "endTime": tail_start_ms,
        "limit": 1,
    })
    if not isinstance(rows, list) or not rows or not isinstance(rows[0], dict):
        raise DataError("no price anchor before tail start")
    trade_ms = finite(rows[0].get("T"))
    price = finite(rows[0].get("p"))
    if trade_ms is None or price is None or trade_ms > tail_start_ms:
        raise DataError("invalid price anchor before tail start")
    return rows[0]


def bp(numerator: float, denominator: float) -> float:
    return 10_000.0 * (numerator / denominator - 1.0)


def price_features_for_row(row: dict[str, Any], candle_open: float, base_url: str) -> dict[str, Any]:
    end_ms = int(row["end_ts_ms"])
    cutoff_ms = int(row["decision_cutoff_ms"])
    tail_start_ms = end_ms - 30_000
    # One second of pre-window history supplies a genuine forward-fill anchor.
    records = fetch_agg_window(
        base_url,
        ASSET_SYMBOLS[str(row["asset"])],
        tail_start_ms - 1_000,
        cutoff_ms,
    )
    parsed: list[tuple[int, int, float]] = []
    for trade in records:
        trade_ms = finite(trade.get("T"))
        price = finite(trade.get("p"))
        trade_id = finite(trade.get("a"))
        if trade_ms is None or price is None or trade_id is None:
            continue
        parsed.append((int(trade_ms), int(trade_id), price))
    anchor = [record for record in parsed if record[0] <= tail_start_ms]
    anchor_source = "tail_window"
    if not anchor:
        fallback = fetch_anchor_before(base_url, ASSET_SYMBOLS[str(row["asset"])], tail_start_ms)
        fallback_ms = finite(fallback.get("T"))
        fallback_price = finite(fallback.get("p"))
        fallback_id = finite(fallback.get("a"))
        if fallback_ms is None or fallback_price is None or fallback_id is None:
            raise DataError("invalid price anchor before tail start")
        anchor = [(int(fallback_ms), int(fallback_id), fallback_price)]
        anchor_source = "separate_anchor_query"
    tail = [record for record in parsed if tail_start_ms < record[0] <= cutoff_ms]
    p30 = anchor[-1][2]
    p20 = p30
    p_decision = p30
    last_trade_ms = anchor[-1][0]
    observed_prices = [p30]
    twenty_ms = end_ms - 20_000
    for trade_ms, _trade_id, price in tail:
        observed_prices.append(price)
        if trade_ms <= twenty_ms:
            p20 = price
        p_decision = price
        last_trade_ms = trade_ms
    direction = 1.0 if row["pm_side"] == "YES" else -1.0
    signed_candle_bp = direction * bp(p_decision, candle_open)
    signed_net20_bp = direction * bp(p_decision, p30)
    signed_last10_bp = direction * bp(p_decision, p20)
    if direction > 0:
        adverse_end_reversal_bp = bp(max(observed_prices), p_decision)
    else:
        adverse_end_reversal_bp = bp(p_decision, min(observed_prices))
    return {
        **row,
        "binance_symbol": ASSET_SYMBOLS[str(row["asset"])],
        "binance_candle_open": candle_open,
        "binance_tail_start_price": p30,
        "binance_tminus20_price": p20,
        "binance_decision_price": p_decision,
        "binance_tail_trade_count": len(tail),
        "binance_anchor_source": anchor_source,
        "binance_anchor_age_ms": tail_start_ms - anchor[-1][0],
        "binance_last_trade_age_ms": cutoff_ms - last_trade_ms,
        "signed_candle_bp": signed_candle_bp,
        "signed_net20_bp": signed_net20_bp,
        "signed_last10_bp": signed_last10_bp,
        "adverse_end_reversal_bp": adverse_end_reversal_bp,
    }


def build_feature_cache(
    candidates: list[dict[str, Any]],
    *,
    api_base: str,
    workers: int,
) -> dict[str, Any]:
    by_asset: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in candidates:
        by_asset[str(row["asset"])].append(row)
    opens: dict[str, dict[int, float]] = {}
    print(json.dumps({"phase": "binance_5m_open", "assets": sorted(by_asset)}), flush=True)
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(workers, max(1, len(by_asset)))) as pool:
        future_asset = {
            pool.submit(fetch_kline_opens, api_base, asset, [int(row["start_ts_ms"]) for row in rows]): asset
            for asset, rows in by_asset.items()
        }
        for future in concurrent.futures.as_completed(future_asset):
            asset = future_asset[future]
            opens[asset] = future.result()
            print(json.dumps({"phase": "binance_5m_open", "asset": asset, "opens": len(opens[asset])}), flush=True)

    successful: list[dict[str, Any]] = []
    errors: Counter[str] = Counter()
    tasks: list[tuple[dict[str, Any], float]] = []
    for row in candidates:
        candle_open = opens.get(str(row["asset"]), {}).get(int(row["start_ts_ms"]))
        if candle_open is None:
            errors["missing_5m_open"] += 1
            continue
        tasks.append((row, candle_open))
    print(json.dumps({"phase": "binance_agg_path", "candidates": len(tasks), "workers": workers}), flush=True)

    def worker(task: tuple[dict[str, Any], float]) -> dict[str, Any]:
        return price_features_for_row(task[0], task[1], api_base)

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(worker, task) for task in tasks]
        for index, future in enumerate(concurrent.futures.as_completed(futures), start=1):
            try:
                successful.append(future.result())
            except DataError as exc:
                errors[str(exc).split(":", 1)[0]] += 1
            except Exception as exc:  # retain batch progress rather than discard a full replay
                errors[f"unexpected:{type(exc).__name__}"] += 1
            if index % 50 == 0 or index == len(futures):
                print(json.dumps({"phase": "binance_agg_path", "done": index, "total": len(futures), "usable": len(successful)}), flush=True)
    successful.sort(key=lambda row: (int(row["start_ts_ms"]), str(row["asset"]), int(row["round"])))
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "binance_price_path_feature_cache",
        "api_base": api_base,
        "records_requested": len(candidates),
        "records_usable": len(successful),
        "error_counts": dict(sorted(errors.items())),
        "records": successful,
    }


def wilson_95(successes: int, trials: int) -> Optional[tuple[float, float]]:
    if trials <= 0:
        return None
    z = 1.959963984540054
    p = successes / trials
    denom = 1.0 + z * z / trials
    center = (p + z * z / (2.0 * trials)) / denom
    margin = z * math.sqrt((p * (1.0 - p) + z * z / (4.0 * trials)) / trials) / denom
    return center - margin, center + margin


def params_label(params: dict[str, Any]) -> str:
    def cap(value: Any) -> str:
        return "off" if value is None else f"{float(value):g}bp"
    return (
        f"bid>={float(params['min_bid']):.2f}; weak_rev<={float(params['weak_adverse_cap_bp']):g}bp; "
        f"strong_last10_tol={cap(params['strong_last10_tolerance_bp'])}; "
        f"strong_rev<={cap(params['strong_adverse_cap_bp'])}"
    )


def decision(row: dict[str, Any], params: dict[str, Any], *, max_pm_quote_age_ms: int, max_binance_age_ms: int) -> tuple[bool, str]:
    if float(row["leader_bid"]) < float(params["min_bid"]):
        return False, "pm_bid_below_threshold"
    if int(row["leader_quote_age_ms"]) > max_pm_quote_age_ms:
        return False, "stale_pm_quote"
    if int(row["binance_last_trade_age_ms"]) > max_binance_age_ms:
        return False, "stale_binance_trade"
    candle = float(row["signed_candle_bp"])
    if candle <= 0.0:
        return False, "binance_candle_opposes_pm_side"
    if candle <= 5.0:
        # Literal "weak candle + reverse = do not touch" converted into an
        # observable, latency-safe rule.  A zero cap means no end reversal at
        # all; positive caps form asset-specific market-noise bands.
        if float(row["signed_net20_bp"]) <= 0.0:
            return False, "weak_net_tail_reversal"
        if float(row["signed_last10_bp"]) <= 0.0:
            return False, "weak_last10_reversal"
        if float(row["adverse_end_reversal_bp"]) > float(params["weak_adverse_cap_bp"]):
            return False, "weak_end_reversal"
        return True, "weak_pass"
    tolerance = params["strong_last10_tolerance_bp"]
    if tolerance is not None and float(row["signed_last10_bp"]) < -float(tolerance):
        return False, "strong_last10_reversal"
    cap = params["strong_adverse_cap_bp"]
    if cap is not None and float(row["adverse_end_reversal_bp"]) > float(cap):
        return False, "strong_end_reversal"
    return True, "strong_pass"


def evaluate(rows: list[dict[str, Any]], params: dict[str, Any], *, max_pm_quote_age_ms: int, max_binance_age_ms: int) -> dict[str, Any]:
    skips: Counter[str] = Counter()
    selected: list[dict[str, Any]] = []
    for row in rows:
        take, reason = decision(row, params, max_pm_quote_age_ms=max_pm_quote_age_ms, max_binance_age_ms=max_binance_age_ms)
        if not take:
            skips[reason] += 1
            continue
        selected.append(row)
    wins = sum(row["pm_side"] == row["pm_winner"] for row in selected)
    total = len(selected)
    ci = wilson_95(wins, total)
    return {
        "params": params,
        "params_label": params_label(params),
        "input_rows": len(rows),
        "eligible": total,
        "wins": wins,
        "losses": total - wins,
        "accuracy": None if total == 0 else wins / total,
        "wilson_95": None if ci is None else [ci[0], ci[1]],
        "skip_counts": dict(sorted(skips.items())),
        "selected": selected,
    }


def pm_baseline(rows: list[dict[str, Any]], *, min_bid: float, max_pm_quote_age_ms: int) -> dict[str, Any]:
    selected = [
        row for row in rows
        if float(row["leader_bid"]) >= min_bid and int(row["leader_quote_age_ms"]) <= max_pm_quote_age_ms
    ]
    wins = sum(row["pm_side"] == row["pm_winner"] for row in selected)
    ci = wilson_95(wins, len(selected))
    return {
        "min_bid": min_bid,
        "input_rows": len(rows),
        "eligible": len(selected),
        "wins": wins,
        "losses": len(selected) - wins,
        "accuracy": None if not selected else wins / len(selected),
        "wilson_95": None if ci is None else [ci[0], ci[1]],
        "selected": selected,
    }


def parameter_grid() -> list[dict[str, Any]]:
    # Four strong-candle modes, deliberately small to keep the calibration
    # interpretable instead of running a broad p-hacking sweep.
    strong_modes = [
        (None, None),       # user's "strong candle may absorb the reversal"
        (0.0, None),        # strong candle: last 10 seconds may not net reverse
        (1.0, 5.0),         # permit <=1bp net fade, cap endpoint reversal at 5bp
        (2.0, 10.0),        # looser, but still bounded
    ]
    result: list[dict[str, Any]] = []
    for min_bid in (0.80, 0.85, 0.90):
        for weak_cap in (0.0, 0.5, 1.0, 2.0):
            for last10_tolerance, strong_cap in strong_modes:
                result.append({
                    "min_bid": min_bid,
                    "weak_adverse_cap_bp": weak_cap,
                    "strong_last10_tolerance_bp": last10_tolerance,
                    "strong_adverse_cap_bp": strong_cap,
                })
    return result


def pm_floor_grid() -> tuple[float, ...]:
    """Small, economically interpretable confidence floors for the PM leader."""
    return (0.80, 0.85, 0.90, 0.95)


def choose_params(
    train: list[dict[str, Any]],
    *,
    max_pm_quote_age_ms: int,
    max_binance_age_ms: int,
    min_train_trades: int,
) -> tuple[Optional[dict[str, Any]], list[dict[str, Any]]]:
    scored: list[dict[str, Any]] = []
    for params in parameter_grid():
        score = evaluate(train, params, max_pm_quote_age_ms=max_pm_quote_age_ms, max_binance_age_ms=max_binance_age_ms)
        scored.append(score)
    zero_loss = [score for score in scored if score["eligible"] >= min_train_trades and score["losses"] == 0]
    if not zero_loss:
        return None, scored
    # Promote the widest zero-loss training rule; ties favour a lower PM price
    # threshold and wider noise bands, so it is not secretly selected for being
    # the narrowest one-trade configuration.
    def rank(score: dict[str, Any]) -> tuple[Any, ...]:
        params = score["params"]
        strong_tolerance = params["strong_last10_tolerance_bp"]
        strong_cap = params["strong_adverse_cap_bp"]
        return (
            int(score["eligible"]),
            -float(params["min_bid"]),
            float(params["weak_adverse_cap_bp"]),
            math.inf if strong_tolerance is None else float(strong_tolerance),
            math.inf if strong_cap is None else float(strong_cap),
        )
    selected = max(zero_loss, key=rank)
    return dict(selected["params"]), scored


def choose_global_safety_rule(
    train: list[dict[str, Any]],
    *,
    max_pm_quote_age_ms: int,
    max_binance_age_ms: int,
    min_train_trades: int,
) -> tuple[Optional[dict[str, Any]], dict[str, Any]]:
    """Choose a PM confidence floor before the Binance proxy refinement.

    Keeping this independent prevents a price gate from hiding a low-bid PM
    mistake in training and then making that lower confidence floor look safe.
    """
    floor_scores = [
        pm_baseline(train, min_bid=floor, max_pm_quote_age_ms=max_pm_quote_age_ms)
        for floor in pm_floor_grid()
    ]
    viable = [
        score for score in floor_scores
        if score["eligible"] >= min_train_trades and score["losses"] == 0
    ]
    if not viable:
        return None, {"pm_floor_scores": floor_scores, "tail_scores": []}
    # Maximise training capacity without using the later price path.  For equal
    # capacity, choose the lower threshold rather than a one-trade corner case.
    floor_score = max(viable, key=lambda score: (int(score["eligible"]), -float(score["min_bid"])))
    floor = float(floor_score["min_bid"])
    tail_scores: list[dict[str, Any]] = []
    for params in parameter_grid():
        if float(params["min_bid"]) != floor:
            continue
        tail_scores.append(evaluate(
            train,
            params,
            max_pm_quote_age_ms=max_pm_quote_age_ms,
            max_binance_age_ms=max_binance_age_ms,
        ))
    viable_tail = [
        score for score in tail_scores
        if score["eligible"] >= min_train_trades and score["losses"] == 0
    ]
    if not viable_tail:
        return None, {"pm_floor_scores": floor_scores, "tail_scores": tail_scores}

    def rank(score: dict[str, Any]) -> tuple[Any, ...]:
        params = score["params"]
        return (
            int(score["eligible"]),
            float(params["weak_adverse_cap_bp"]),
            math.inf if params["strong_last10_tolerance_bp"] is None else float(params["strong_last10_tolerance_bp"]),
            math.inf if params["strong_adverse_cap_bp"] is None else float(params["strong_adverse_cap_bp"]),
        )

    selected = max(viable_tail, key=rank)
    return dict(selected["params"]), {"pm_floor_scores": floor_scores, "tail_scores": tail_scores}


def split_time(rows: list[dict[str, Any]], train_ratio: float = 0.60) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    ordered = sorted(rows, key=lambda row: (int(row["start_ts_ms"]), int(row["round"])))
    cut = max(1, min(len(ordered) - 1, int(len(ordered) * train_ratio)))
    return ordered[:cut], ordered[cut:]


def fmt_rate(value: Optional[float]) -> str:
    return "—" if value is None else f"{value * 100:.1f}%"


def fmt_wl(metric: dict[str, Any]) -> str:
    return f"{metric['wins']}/{metric['eligible']} ({fmt_rate(metric['accuracy'])})"


def compact_trade(row: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "asset", "slug", "round", "start_ts_ms", "pm_side", "pm_winner", "leader_bid",
        "signed_candle_bp", "signed_net20_bp", "signed_last10_bp", "adverse_end_reversal_bp",
        "binance_tail_trade_count", "binance_last_trade_age_ms", "leader_quote_age_ms",
    )
    return {key: row.get(key) for key in keys}


def report_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# TWAP 尾盤價格路徑篩選回測",
        "",
        "## 結論邊界",
        "",
        "- 決策使用 PM 本地接收時間在 `E−10s−250ms` 前可見的 best-bid leader。",
        "- Binance Spot aggTrades 只作風險篩選；勝負標籤一律是本地觀察到的 PM/UMA 最終 resolution。",
        "- 這是方向正確率 replay，**不含** CLOB ask、部分成交、撤單、費用或真實延遲，因此不能宣稱可實盤獲利。",
        "- 訓練段只選「至少 8 筆且 0 loss」的每幣參數；後 40% 時間序列完全保留做測試。若測試有一筆 loss，該幣不應進入你要求的 100% gate。",
        "",
        "## 對照結果（測試段）",
        "",
        "| 幣種 | 可用列 | PM leader 基線 | 價格路徑篩選 | 訓練選出的規則 |",
        "| --- | ---: | --- | --- | --- |",
    ]
    for asset, value in report["per_asset"].items():
        chosen = value["chosen_params"]
        label = "未推廣（訓練無 >=8 筆 0-loss 規則）" if chosen is None else params_label(chosen)
        lines.append(
            f"| {asset} | {value['all_rows']} | {fmt_wl(value['test_pm_baseline'])} | "
            f"{fmt_wl(value['test_adaptive'])} | {label} |"
        )
    total = report["overall"]
    lines.extend([
        "",
        f"合計：PM leader 基線 {fmt_wl(total['test_pm_baseline'])}；價格路徑篩選 {fmt_wl(total['test_adaptive'])}。",
        "",
        "## 分層安全版（全幣共用 PM 信心底線）",
        "",
    ])
    safety = report["global_safety"]
    if safety["chosen_params"] is None:
        lines.append("前 60% 訓練資料沒有符合零 loss / 最低樣本數條件的全域規則；安全版不推廣。")
    else:
        lines.extend([
            f"- 先只用 PM 訓練段選 confidence floor：`{safety['chosen_params']['min_bid']:.2f}`；再選尾段 gate：`{params_label(safety['chosen_params'])}`。",
            f"- 訓練：{fmt_wl(safety['train_rule'])}；後 40% 測試：{fmt_wl(safety['test_rule'])}，Wilson 95% 下界 {safety['test_rule']['wilson_95'][0] * 100:.2f}%。",
            "- 這比「先讓 Binance gate 把訓練錯單遮掉，再選 0.80」更保守；但本次精煉仍源自同一段歷史資料，先做 forward shadow，不把 193/193 叫作已證明的 100%。",
        ])
    lines.extend([
        "",
        "## 被檢驗的規則",
        "",
        "令 PM 候選邊為 `s ∈ {+1(YES), -1(NO)}`、決策價為 `P_D`：",
        "",
        "- `candle_bp = s × 10,000 × (P_D / 5m_open − 1)`；若 `<=0`，PM 邊與 Binance 當根方向相反，跳過。",
        "- 弱 K（`0 < candle_bp <= 5`）：要求 30→10 秒淨變動與最後 10 秒淨變動皆同方向，且由窗口高/低點回到 `P_D` 的反向回撤不超過該幣訓練出的 noise band。",
        "- 強 K（`candle_bp > 5`）：套用每幣訓練選出的強勢模式；最寬鬆模式不因短尾端逆向而跳過，較嚴模式限制最後 10 秒淨逆向與 endpoint 回撤。",
        "",
        "「任何反向」不能直接以 0 個價格跳動定義，否則高頻成交的單一 tick 就會讓幾乎所有弱 K 無法交易；報表把 0 / 0.5 / 1 / 2 bp 明確列入訓練選項，並只報告未見過的後段結果。",
        "",
        "## 測試段失敗單",
        "",
    ])
    losses = report["overall"]["test_adaptive_losses"]
    if not losses:
        lines.append("無。這仍只代表此保留樣本未觀察到錯誤，不是 100% 的統計保證。")
    else:
        lines.extend([
            "| 幣種 | 場次 | PM 候選 / UMA | bid | candle bp | 30→10 bp | 最後10秒 bp | 回撤 bp |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |",
        ])
        for row in losses:
            lines.append(
                f"| {row['asset']} | {row['slug']} | {row['pm_side']} / {row['pm_winner']} | "
                f"{float(row['leader_bid']):.2f} | {float(row['signed_candle_bp']):.2f} | "
                f"{float(row['signed_net20_bp']):.2f} | {float(row['signed_last10_bp']):.2f} | "
                f"{float(row['adverse_end_reversal_bp']):.2f} |"
            )
    lines.extend([
        "",
        "## 資料覆蓋",
        "",
        f"- PM 原始候選：{report['coverage']['candidate_records']}；成功補上 Binance 5m open + tail aggTrade 路徑：{report['coverage']['feature_records']}。",
        f"- PM 候選擷取跳過：`{json.dumps(report['coverage']['candidate_skip_counts'], ensure_ascii=False, sort_keys=True)}`。",
        f"- Binance 路徑缺失/錯誤：`{json.dumps(report['coverage']['feature_error_counts'], ensure_ascii=False, sort_keys=True)}`。",
        "",
        "可重跑命令見本報表同目錄的 `report.json` metadata；僅用讀取本地資料與公開官方市場資料，未連錢包、未下單。",
    ])
    return "\n".join(lines) + "\n"


def build_report(
    *,
    candidates: dict[str, Any],
    features: dict[str, Any],
    assets: tuple[str, ...],
    max_pm_quote_age_ms: int,
    max_binance_age_ms: int,
    min_train_trades: int,
) -> dict[str, Any]:
    by_asset: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in features.get("records", []):
        if str(row.get("asset")) in assets:
            by_asset[str(row["asset"])].append(row)
    baseline_params = {"min_bid": 0.80, "weak_adverse_cap_bp": 0.0, "strong_last10_tolerance_bp": None, "strong_adverse_cap_bp": None}
    per_asset: dict[str, Any] = {}
    all_train_rows: list[dict[str, Any]] = []
    all_test_rows: list[dict[str, Any]] = []
    all_test_adaptive_selected: list[dict[str, Any]] = []
    for asset in assets:
        rows = sorted(by_asset.get(asset, []), key=lambda row: (int(row["start_ts_ms"]), int(row["round"])))
        if len(rows) < 2:
            per_asset[asset] = {
                "all_rows": len(rows), "train_rows": 0, "test_rows": 0,
                "chosen_params": None,
                "train_pm_baseline": pm_baseline([], min_bid=0.80, max_pm_quote_age_ms=max_pm_quote_age_ms),
                "test_pm_baseline": pm_baseline([], min_bid=0.80, max_pm_quote_age_ms=max_pm_quote_age_ms),
                "test_adaptive": evaluate([], baseline_params, max_pm_quote_age_ms=max_pm_quote_age_ms, max_binance_age_ms=max_binance_age_ms),
                "selection_reason": "too_few_rows",
            }
            continue
        train, test = split_time(rows)
        all_train_rows.extend(train)
        params, score_grid = choose_params(
            train,
            max_pm_quote_age_ms=max_pm_quote_age_ms,
            max_binance_age_ms=max_binance_age_ms,
            min_train_trades=min_train_trades,
        )
        adaptive = evaluate(
            test,
            params or baseline_params,
            max_pm_quote_age_ms=max_pm_quote_age_ms,
            max_binance_age_ms=max_binance_age_ms,
        )
        # If no configuration met the predeclared 0-loss train condition, treat
        # this asset as disabled rather than quietly applying a lossy fallback.
        if params is None:
            adaptive = {**adaptive, "eligible": 0, "wins": 0, "losses": 0, "accuracy": None, "wilson_95": None, "selected": [], "skip_counts": {"not_promoted": len(test)}}
        else:
            all_test_adaptive_selected.extend(adaptive["selected"])
        all_test_rows.extend(test)
        per_asset[asset] = {
            "all_rows": len(rows),
            "train_rows": len(train),
            "test_rows": len(test),
            "chosen_params": params,
            "selection_reason": "zero_loss_train_rule" if params else f"no_zero_loss_rule_with_{min_train_trades}_train_trades",
            "train_pm_baseline": pm_baseline(train, min_bid=0.80, max_pm_quote_age_ms=max_pm_quote_age_ms),
            "test_pm_baseline": pm_baseline(test, min_bid=0.80, max_pm_quote_age_ms=max_pm_quote_age_ms),
            "test_adaptive": adaptive,
            "train_grid": [
                {key: value for key, value in score.items() if key != "selected"}
                for score in score_grid
            ],
        }
    all_pm = pm_baseline(all_test_rows, min_bid=0.80, max_pm_quote_age_ms=max_pm_quote_age_ms)
    all_wins = sum(row["pm_side"] == row["pm_winner"] for row in all_test_adaptive_selected)
    all_ci = wilson_95(all_wins, len(all_test_adaptive_selected))
    losses = [compact_trade(row) for row in all_test_adaptive_selected if row["pm_side"] != row["pm_winner"]]
    global_params, global_selection = choose_global_safety_rule(
        all_train_rows,
        max_pm_quote_age_ms=max_pm_quote_age_ms,
        max_binance_age_ms=max_binance_age_ms,
        min_train_trades=min_train_trades,
    )
    global_train = evaluate(
        all_train_rows,
        global_params or baseline_params,
        max_pm_quote_age_ms=max_pm_quote_age_ms,
        max_binance_age_ms=max_binance_age_ms,
    )
    global_test = evaluate(
        all_test_rows,
        global_params or baseline_params,
        max_pm_quote_age_ms=max_pm_quote_age_ms,
        max_binance_age_ms=max_binance_age_ms,
    )
    if global_params is None:
        global_train = {**global_train, "eligible": 0, "wins": 0, "losses": 0, "accuracy": None, "wilson_95": None, "selected": [], "skip_counts": {"not_promoted": len(all_train_rows)}}
        global_test = {**global_test, "eligible": 0, "wins": 0, "losses": 0, "accuracy": None, "wilson_95": None, "selected": [], "skip_counts": {"not_promoted": len(all_test_rows)}}
    return {
        "schema_version": SCHEMA_VERSION,
        "method": {
            "decision": "PM best-bid leader at E-10s-safety_buffer, then Binance price-path veto",
            "label": "local PM final UMA resolution only",
            "binance_role": "proxy risk filter, not settlement oracle",
            "weak_candle_boundary_bp": 5.0,
            "max_pm_quote_age_ms": max_pm_quote_age_ms,
            "max_binance_trade_age_ms": max_binance_age_ms,
            "per_asset_train_ratio": 0.60,
            "min_train_trades_for_promotion": min_train_trades,
        },
        "coverage": {
            "candidate_records": len(candidates.get("records", [])),
            "candidate_skip_counts": candidates.get("skip_counts", {}),
            "feature_records": len(features.get("records", [])),
            "feature_error_counts": features.get("error_counts", {}),
        },
        "per_asset": per_asset,
        "overall": {
            "test_pm_baseline": {key: value for key, value in all_pm.items() if key != "selected"},
            "test_adaptive": {
                "eligible": len(all_test_adaptive_selected),
                "wins": all_wins,
                "losses": len(all_test_adaptive_selected) - all_wins,
                "accuracy": None if not all_test_adaptive_selected else all_wins / len(all_test_adaptive_selected),
                "wilson_95": None if all_ci is None else [all_ci[0], all_ci[1]],
            },
            "test_adaptive_losses": losses,
        },
        "global_safety": {
            "chosen_params": global_params,
            "selection": {
                "pm_floor_scores": [{key: value for key, value in score.items() if key != "selected"} for score in global_selection["pm_floor_scores"]],
                "tail_scores": [{key: value for key, value in score.items() if key != "selected"} for score in global_selection["tail_scores"]],
            },
            "train_rule": global_train,
            "test_rule": global_test,
        },
    }


def write_trade_csv(path: Path, per_asset: dict[str, Any]) -> None:
    rows: list[dict[str, Any]] = []
    for asset, value in per_asset.items():
        for row in value["test_adaptive"].get("selected", []):
            rows.append({**compact_trade(row), "selected_rule": params_label(value["chosen_params"]), "out_of_sample": True})
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "asset", "slug", "round", "start_ts_ms", "pm_side", "pm_winner", "leader_bid",
        "signed_candle_bp", "signed_net20_bp", "signed_last10_bp", "adverse_end_reversal_bp",
        "binance_tail_trade_count", "binance_last_trade_age_ms", "leader_quote_age_ms", "selected_rule", "out_of_sample",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_global_safety_csv(path: Path, safety: dict[str, Any]) -> None:
    params = safety.get("chosen_params")
    rows = [
        {**compact_trade(row), "selected_rule": params_label(params), "out_of_sample": True}
        for row in safety.get("test_rule", {}).get("selected", [])
    ] if params else []
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "asset", "slug", "round", "start_ts_ms", "pm_side", "pm_winner", "leader_bid",
        "signed_candle_bp", "signed_net20_bp", "signed_last10_bp", "adverse_end_reversal_bp",
        "binance_tail_trade_count", "binance_last_trade_age_ms", "leader_quote_age_ms", "selected_rule", "out_of_sample",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=Path("research_data/crypto_universe_resolution_window_v1"))
    parser.add_argument("--out-dir", type=Path, default=Path("research_outputs/twap_price_path_tail_v1"))
    parser.add_argument("--assets", type=parse_csv_set, default=DEFAULT_ASSETS)
    parser.add_argument("--cutover-ts", type=int, default=DEFAULT_CUTOVER_TS)
    parser.add_argument("--decision-tte-ms", type=int, default=10_000)
    parser.add_argument("--safety-buffer-ms", type=int, default=250)
    parser.add_argument("--base-min-bid", type=float, default=0.80)
    parser.add_argument("--round-min", type=int, default=0)
    parser.add_argument("--round-max", type=int, default=10**9)
    parser.add_argument("--extract-workers", type=int, default=2)
    parser.add_argument("--api-workers", type=int, default=4)
    parser.add_argument("--api-base", default=DEFAULT_API_BASE)
    parser.add_argument("--max-pm-quote-age-ms", type=int, default=2_000)
    parser.add_argument("--max-binance-trade-age-ms", type=int, default=2_000)
    parser.add_argument("--min-train-trades", type=int, default=8)
    parser.add_argument("--rebuild-candidates", action="store_true")
    parser.add_argument("--rebuild-features", action="store_true")
    args = parser.parse_args()

    if args.extract_workers < 1 or args.api_workers < 1:
        parser.error("worker counts must be positive")
    if not (0.0 < args.base_min_bid < 1.0):
        parser.error("--base-min-bid must be between 0 and 1")
    out_dir: Path = args.out_dir
    candidate_path = out_dir / "pm_candidates.json"
    feature_path = out_dir / "price_path_features.json"
    candidate_expected = {
        "schema_version": SCHEMA_VERSION,
        "kind": "pm_predecision_candidate_cache",
        "data_dir": str(args.data_dir),
        "assets": list(args.assets),
        "cutover_ts": args.cutover_ts,
        "decision_tte_ms": args.decision_tte_ms,
        "safety_buffer_ms": args.safety_buffer_ms,
        "base_min_bid": args.base_min_bid,
        "round_min": args.round_min,
        "round_max": args.round_max,
    }
    candidates = None if args.rebuild_candidates else load_matching_cache(candidate_path, candidate_expected)
    if candidates is None:
        candidates = extract_candidates(
            data_dir=args.data_dir,
            assets=args.assets,
            cutover_ts=args.cutover_ts,
            decision_tte_ms=args.decision_tte_ms,
            safety_buffer_ms=args.safety_buffer_ms,
            base_min_bid=args.base_min_bid,
            round_min=args.round_min,
            round_max=args.round_max,
            workers=args.extract_workers,
        )
        write_json(candidate_path, candidates)
    else:
        print(json.dumps({"phase": "pm_candidate_extract", "cache": str(candidate_path), "records": len(candidates.get("records", []))}), flush=True)

    feature_expected = {
        "schema_version": SCHEMA_VERSION,
        "kind": "binance_price_path_feature_cache",
        "api_base": args.api_base,
    }
    features = None if args.rebuild_features else load_matching_cache(feature_path, feature_expected)
    if features is None:
        features = build_feature_cache(candidates.get("records", []), api_base=args.api_base, workers=args.api_workers)
        write_json(feature_path, features)
    else:
        print(json.dumps({"phase": "binance_agg_path", "cache": str(feature_path), "records": len(features.get("records", []))}), flush=True)

    report = build_report(
        candidates=candidates,
        features=features,
        assets=args.assets,
        max_pm_quote_age_ms=args.max_pm_quote_age_ms,
        max_binance_age_ms=args.max_binance_trade_age_ms,
        min_train_trades=args.min_train_trades,
    )
    report["run"] = {
        "data_dir": str(args.data_dir),
        "out_dir": str(out_dir),
        "assets": list(args.assets),
        "cutover_ts": args.cutover_ts,
        "decision_tte_ms": args.decision_tte_ms,
        "safety_buffer_ms": args.safety_buffer_ms,
        "base_min_bid": args.base_min_bid,
        "round_min": args.round_min,
        "round_max": args.round_max,
    }
    write_json(out_dir / "report.json", report)
    (out_dir / "report.md").write_text(report_markdown(report), encoding="utf-8")
    write_trade_csv(out_dir / "out_of_sample_selected_trades.csv", report["per_asset"])
    write_global_safety_csv(out_dir / "out_of_sample_global_safety_trades.csv", report["global_safety"])
    public = {key: value for key, value in report.items() if key not in {"per_asset"}}
    print(json.dumps({"phase": "complete", "overall": public["overall"], "out_dir": str(out_dir)}, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
