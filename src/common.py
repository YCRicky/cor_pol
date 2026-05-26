from __future__ import annotations

import json
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Iterable

GAMMA_API_BASE = "https://gamma-api.polymarket.com"
BINANCE_API_BASE = "https://api.binance.com"


def get_json(url: str, params: dict[str, Any] | None = None, timeout: float = 10.0) -> Any:
    if params:
        query = urllib.parse.urlencode(params, doseq=True)
        url = f"{url}{'&' if '?' in url else '?'}{query}"
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "poly-box-controller/1.0",
            "Accept": "application/json",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = resp.read().decode("utf-8")
    return json.loads(data)


def _safe_json_list(raw: Any) -> list[Any]:
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                return parsed
        except json.JSONDecodeError:
            return []
    return []


def iso_to_ts(value: str) -> int:
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())


def current_window_start(window_minutes: int = 5, now: datetime | None = None) -> datetime:
    now_utc = now or datetime.now(timezone.utc)
    minute = (now_utc.minute // window_minutes) * window_minutes
    return now_utc.replace(minute=minute, second=0, microsecond=0)


def fetch_market_by_slug(slug: str) -> dict[str, Any] | None:
    try:
        event = get_json(f"{GAMMA_API_BASE}/events/slug/{urllib.parse.quote(slug)}")
    except Exception:
        return None
    if isinstance(event, dict):
        markets = event.get("markets")
        if isinstance(markets, list) and markets:
            first = markets[0]
            if isinstance(first, dict):
                return first
    return None


def _candidate_slugs(asset: str, window_minutes: int) -> Iterable[str]:
    prefix = f"{asset.lower()}-updown-{window_minutes}m"
    base_ts = int(current_window_start(window_minutes).timestamp())
    offsets = [0, -60, 60, -120, 120, -180, 180, -240, 240, -300, 300]
    for off in offsets:
        yield f"{prefix}-{base_ts + off}"


def _discover_by_active_scan(asset: str, window_minutes: int) -> dict[str, Any] | None:
    prefix = f"{asset.lower()}-updown-{window_minutes}m"
    try:
        payload = get_json(
            f"{GAMMA_API_BASE}/events",
            params={"active": "true", "closed": "false", "limit": 200},
            timeout=12.0,
        )
    except Exception:
        return None
    if not isinstance(payload, list):
        return None

    best: tuple[int, dict[str, Any]] | None = None
    now_ts = int(datetime.now(timezone.utc).timestamp())
    for event in payload:
        if not isinstance(event, dict):
            continue
        markets = event.get("markets")
        if not isinstance(markets, list):
            continue
        for market in markets:
            if not isinstance(market, dict):
                continue
            slug = str(market.get("slug", ""))
            if not slug.startswith(prefix):
                continue
            start_iso = market.get("eventStartTime")
            if not isinstance(start_iso, str):
                continue
            try:
                start_ts = iso_to_ts(start_iso)
            except Exception:
                continue
            score = abs(start_ts - now_ts)
            if best is None or score < best[0]:
                best = (score, market)
    return best[1] if best else None


def discover_current_market(asset: str = "BTC", window_minutes: int = 5) -> dict[str, Any] | None:
    for slug in _candidate_slugs(asset, window_minutes):
        market = fetch_market_by_slug(slug)
        if market and market.get("enableOrderBook", True):
            return market
    return _discover_by_active_scan(asset, window_minutes)


def parse_outcomes(market: dict[str, Any]) -> list[str]:
    raw = market.get("outcomes")
    items = _safe_json_list(raw)
    out = [str(x) for x in items if isinstance(x, (str, int, float))]
    if out:
        return out

    tokens = market.get("tokens")
    if isinstance(tokens, list):
        token_outcomes = []
        for t in tokens:
            if isinstance(t, dict):
                oc = t.get("outcome")
                if oc is not None:
                    token_outcomes.append(str(oc))
        if token_outcomes:
            return token_outcomes
    return ["Yes", "No"]


def parse_clob_token_ids(market: dict[str, Any]) -> list[str]:
    raw = market.get("clobTokenIds")
    token_ids = [str(x) for x in _safe_json_list(raw)]
    if token_ids:
        return token_ids

    tokens = market.get("tokens")
    if isinstance(tokens, list):
        out: list[str] = []
        for t in tokens:
            if not isinstance(t, dict):
                continue
            tid = t.get("token_id") or t.get("tokenId") or t.get("clobTokenId")
            if tid:
                out.append(str(tid))
        if out:
            return out

    alt = market.get("tokenIds")
    token_ids = [str(x) for x in _safe_json_list(alt)]
    if token_ids:
        return token_ids
    raise ValueError("missing token ids in market payload")


def fetch_all_agg_trades(symbol: str, start_ms: int, end_ms: int, limit: int = 1000) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    cursor = int(start_ms)
    hard_end = int(end_ms)
    while cursor <= hard_end:
        batch = get_json(
            f"{BINANCE_API_BASE}/api/v3/aggTrades",
            params={
                "symbol": symbol.upper(),
                "startTime": cursor,
                "endTime": hard_end,
                "limit": limit,
            },
            timeout=8.0,
        )
        if not isinstance(batch, list) or not batch:
            break
        out.extend([x for x in batch if isinstance(x, dict)])
        last_ts = int(batch[-1].get("T", cursor))
        if len(batch) < limit or last_ts >= hard_end:
            break
        cursor = last_ts + 1
    return out


def build_second_price_series(agg_trades: list[dict[str, Any]], start_s: int, end_s: int) -> dict[int, float]:
    by_second: dict[int, float] = {}
    for tr in agg_trades:
        try:
            ts = int(float(tr.get("T", 0)) // 1000)
            price = float(tr.get("p"))
        except Exception:
            continue
        if start_s <= ts <= end_s:
            by_second[ts] = price

    out: dict[int, float] = {}
    last: float | None = None
    for sec in range(int(start_s), int(end_s) + 1):
        if sec in by_second:
            last = by_second[sec]
        if last is not None:
            out[sec] = last
    return out
