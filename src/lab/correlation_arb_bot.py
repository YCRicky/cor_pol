from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Deque, Optional, Tuple

import websockets

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
for p in (str(ROOT), str(SRC)):
    if p not in sys.path:
        sys.path.insert(0, p)

from common import (  # noqa: E402
    discover_current_market,
    get_json,
    iso_to_ts,
    parse_clob_token_ids,
    parse_outcomes,
)
from lab.correlation_arb_core import (  # noqa: E402
    ArbSignal,
    GapHistogram,
    RollingStats,
    evaluate_arb_box,
    evaluate_reverse_box,
    fair_up_from_spot,
    leg_fail_prob,
    policy_g_kill,
)
from notifier import TelegramConfig, TelegramNotifier  # noqa: E402

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
OUT = ROOT / "out"
OUT.mkdir(parents=True, exist_ok=True)


class SimpleBook:
    def __init__(self) -> None:
        self.bids: dict[float, float] = {}
        self.asks: dict[float, float] = {}

    def apply_snapshot(self, bids, asks) -> None:
        self.bids = {float(x["price"]): float(x["size"]) for x in bids if float(x["size"]) > 0}
        self.asks = {float(x["price"]): float(x["size"]) for x in asks if float(x["size"]) > 0}

    def apply_changes(self, changes) -> None:
        for ch in changes:
            side = (ch.get("side") or "").upper()
            price = float(ch["price"])
            size = float(ch["size"])
            book = self.bids if side == "BID" else self.asks
            if size <= 0:
                book.pop(price, None)
            else:
                book[price] = size

    def best_bid(self) -> Optional[Tuple[float, float]]:
        if not self.bids:
            return None
        p = max(self.bids)
        return p, self.bids[p]

    def best_ask(self) -> Optional[Tuple[float, float]]:
        if not self.asks:
            return None
        p = min(self.asks)
        return p, self.asks[p]


@dataclass
class MarketLeg:
    asset: str
    slug: str
    question: str
    start_ts: int
    end_ts: int
    yes_token: str
    no_token: str
    symbol: str
    yes_book: SimpleBook = field(default_factory=SimpleBook)
    no_book: SimpleBook = field(default_factory=SimpleBook)
    spot_history: Deque[Tuple[float, float]] = field(default_factory=lambda: deque(maxlen=900))
    open_px: Optional[float] = None
    final_px: Optional[float] = None

    def last_spot(self) -> Optional[float]:
        return self.spot_history[-1][1] if self.spot_history else None

    def update_spot(self, ts: float, px: float) -> None:
        if self.open_px is None and ts >= float(self.start_ts):
            self.open_px = px
        self.spot_history.append((ts, px))


def discover_leg(asset: str, symbol: str) -> MarketLeg:
    market = discover_current_market(asset, 5)
    if not market:
        raise RuntimeError(f"failed to discover {asset} market")
    outcomes = parse_outcomes(market)
    token_ids = parse_clob_token_ids(market)
    mapping = dict(zip(outcomes, token_ids))
    yes_tok = mapping.get("Up") or mapping.get("Yes") or token_ids[0]
    no_tok = mapping.get("Down") or mapping.get("No") or token_ids[1]
    return MarketLeg(
        asset=asset,
        slug=market["slug"],
        question=market["question"],
        start_ts=iso_to_ts(market["eventStartTime"]),
        end_ts=iso_to_ts(market["endDate"]),
        yes_token=str(yes_tok),
        no_token=str(no_tok),
        symbol=symbol,
    )


async def poll_spot(symbol: str, timeout_s: float = 2.0) -> float:
    loop = asyncio.get_event_loop()
    data = await loop.run_in_executor(
        None,
        lambda: get_json(f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}", None, timeout_s),
    )
    return float(data["price"])


def _parse_outcome_prices(raw) -> Optional[list[float]]:
    if raw is None:
        return None
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            return None
    if isinstance(raw, list):
        try:
            return [float(x) for x in raw]
        except Exception:
            return None
    return None


async def fetch_pm_payoff(
    slug: str,
    max_wait_s: float = 1200.0,
    poll_every_s: float = 15.0,
    on_progress=None,
) -> Optional[float]:
    """Poll gamma until UMA has actually resolved the market. Only returns 1.0 / 0.0 once
    `umaResolutionStatus == "resolved"` (or `closed == True`) AND outcomePrices pin to
    [1, 0] or [0, 1]. Returns None on timeout. No Binance fallback inside this function."""
    from common import fetch_market_by_slug
    loop = asyncio.get_event_loop()
    deadline = time.time() + max_wait_s
    last_prices: Optional[list[float]] = None
    while time.time() < deadline:
        try:
            market = await loop.run_in_executor(None, fetch_market_by_slug, slug)
        except Exception:
            market = None
        if market:
            prices = _parse_outcome_prices(market.get("outcomePrices"))
            outcomes = market.get("outcomes")
            if isinstance(outcomes, str):
                try:
                    outcomes = json.loads(outcomes)
                except Exception:
                    outcomes = None
            uma_status = market.get("umaResolutionStatus")
            closed = market.get("closed") is True
            if prices and outcomes and len(prices) >= 2 and len(outcomes) >= 2:
                idx_up = next((i for i, o in enumerate(outcomes) if str(o).lower() in ("up", "yes")), 0)
                uma_done = uma_status == "resolved" or closed
                if uma_done and prices[idx_up] >= 0.99:
                    return 1.0
                if uma_done and prices[idx_up] <= 0.01:
                    return 0.0
                last_prices = prices
            if on_progress:
                try:
                    on_progress({"slug": slug, "closed": closed, "uma": uma_status, "prices": last_prices})
                except Exception:
                    pass
        await asyncio.sleep(poll_every_s)
    return None


def _log(path: Path, record: dict) -> None:
    try:
        with path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, separators=(",", ":")) + "\n")
    except Exception:
        pass


async def ws_consumer(legs: list[MarketLeg], stop_at: float) -> None:
    token_map: dict[str, tuple[MarketLeg, str]] = {}
    for leg in legs:
        token_map[leg.yes_token] = (leg, "YES")
        token_map[leg.no_token] = (leg, "NO")
    while time.time() < stop_at:
        try:
            async with websockets.connect(WS_URL, ping_interval=30) as ws:
                await ws.send(json.dumps({
                    "type": "market",
                    "assets_ids": list(token_map.keys()),
                    "initial_dump": True,
                    "custom_feature_enabled": True,
                }))
                while time.time() < stop_at:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=10)
                    except asyncio.TimeoutError:
                        continue
                    payload = json.loads(raw)
                    items = payload if isinstance(payload, list) else [payload]
                    for item in items:
                        asset_id = item.get("asset_id") or item.get("assetId")
                        if asset_id not in token_map:
                            continue
                        leg, side = token_map[asset_id]
                        book = leg.yes_book if side == "YES" else leg.no_book
                        if "bids" in item and "asks" in item:
                            book.apply_snapshot(item["bids"], item["asks"])
                        elif "changes" in item:
                            book.apply_changes(item["changes"])
        except Exception:
            await asyncio.sleep(1.0)


async def spot_pump(legs: list[MarketLeg], stop_at: float, stats: RollingStats) -> None:
    while time.time() < stop_at:
        try:
            results = await asyncio.gather(*(poll_spot(leg.symbol) for leg in legs), return_exceptions=True)
            now = time.time()
            btc_px = None
            eth_px = None
            for leg, val in zip(legs, results):
                if isinstance(val, Exception):
                    continue
                leg.update_spot(now, float(val))
                if leg.asset == "BTC":
                    btc_px = float(val)
                elif leg.asset == "ETH":
                    eth_px = float(val)
            if btc_px is not None and eth_px is not None:
                stats.update(btc_px, eth_px)
        except Exception:
            pass
        await asyncio.sleep(1.0)


@dataclass
class LabCombo:
    """One BTC+ETH combo fill (each leg = combo_qty shares). A round may stack
    multiple combos subject to GateConfig caps. Each combo has its own kill state."""
    combo_id: int
    direction: str
    leg_a: str
    leg_b: str
    price_a: float
    price_b: float
    qty: float
    entered_at: float
    flipped_a: bool = False
    flipped_b: bool = False
    flip_price_a: float = 0.0
    flip_price_b: float = 0.0
    flip_reason_a: str = ""
    flip_reason_b: str = ""
    is_hedge: bool = False
    parent_combo_id: int = 0
    adverse_since_ts: Optional[float] = None  # reserved; Policy G fires immediately


def best_ask_tuple(book: SimpleBook) -> Optional[Tuple[float, float]]:
    return book.best_ask()


def best_bid_tuple(book: SimpleBook) -> Optional[Tuple[float, float]]:
    return book.best_bid()


def leg_token(leg: MarketLeg, side: str) -> str:
    return leg.yes_token if side == "YES" else leg.no_token


def asset_side_of(label: str) -> Tuple[str, str]:
    asset, side = label.split("_")
    return asset, side


def get_book_for_label(legs_by_asset: dict[str, MarketLeg], label: str) -> SimpleBook:
    asset, side = asset_side_of(label)
    leg = legs_by_asset[asset]
    return leg.yes_book if side == "YES" else leg.no_book


def opposite_label(label: str) -> str:
    asset, side = asset_side_of(label)
    return f"{asset}_{'NO' if side == 'YES' else 'YES'}"



@dataclass
class GateConfig:
    min_correlation: float = 0.65
    min_gap: float = 0.06
    max_gap: float = 0.18
    min_book_size: float = 5.0
    tte_min_s: int = 60
    tte_max_s: int = 270
    combo_qty: float = 5.0
    max_combos_per_round: int = 5
    max_cost_per_round_usd: float = 50.0
    combo_cooldown_s: float = 15.0
    taker_fee: float = 0.0
    fair_block_margin: float = 0.05
    pm_resolution_wait_s: float = 1200.0
    pm_resolution_poll_s: float = 15.0
    min_vol_bp_60s: float = 10.0
    # --- Finalized entry filter (OOS-validated on Run #2 -> Run #84) ---
    # Asymmetric mid: at entry, both PM legs must lean -- one >= asym_mid_hi, the
    # other <= asym_mid_lo. Both legs near 0.50 = coin-flip = Q4-prone, skip.
    entry_asym_mid_hi: float = 0.60
    entry_asym_mid_lo: float = 0.40
    # Minimum favorable bp: at entry, neither leg may already be deeper than
    # min_fav_bp adverse (negative = adverse). Default -4.0 bp.
    entry_min_fav_bp: float = -4.0
    # --- Policy G defensive flip ---
    # Trigger dual-flip when both legs have floating loss > eps_loss AND both have
    # fair-model fail_prob >= t_recov, OR when either leg's fail_prob >= t_lock.
    policy_g_eps_loss: float = 0.05
    policy_g_t_recov: float = 0.55
    policy_g_t_lock: float = 0.80
    # Tail hedge: not part of the finalized strategy, disabled by default.
    enable_tail_hedge: bool = False
    tail_hedge_qty_ratio: float = 0.30
    tail_hedge_max_box_cost: float = 1.15
    tail_hedge_max_total_cost: float = 1.95
    # Polling cadence: faster in the final fast_poll_tte_s seconds
    fast_poll_tte_s: int = 30
    poll_normal_s: float = 0.5
    poll_fast_s: float = 0.1


def shadow_fill(book: SimpleBook, side: str, target_qty: float) -> Tuple[float, float]:
    if side == "BUY":
        levels = sorted(book.asks.items())
    else:
        levels = sorted(book.bids.items(), reverse=True)
    if not levels:
        return 0.0, 0.0
    remaining = target_qty
    filled = 0.0
    notional = 0.0
    for px, sz in levels:
        if remaining <= 0:
            break
        take = min(sz, remaining)
        filled += take
        notional += take * px
        remaining -= take
    if filled <= 0:
        return 0.0, 0.0
    return filled, notional / filled


def try_entry(
    legs_by_asset: dict[str, MarketLeg],
    stats: RollingStats,
    gates: GateConfig,
    hist: GapHistogram,
    tte_s: int,
) -> Tuple[Optional[ArbSignal], dict]:
    btc = legs_by_asset["BTC"]
    eth = legs_by_asset["ETH"]
    diag = {"reason": "ok"}
    rho = stats.correlation()
    if rho is None:
        diag["reason"] = "rho_warming"
        return None, diag
    if rho < gates.min_correlation:
        diag["reason"] = f"rho_low:{rho:.3f}"
        return None, diag
    if tte_s < gates.tte_min_s:
        diag["reason"] = "tte_too_short"
        return None, diag
    if tte_s > gates.tte_max_s:
        diag["reason"] = "tte_too_long"
        return None, diag
    btc_spot = btc.last_spot()
    eth_spot = eth.last_spot()
    if btc_spot is None or eth_spot is None or btc.open_px is None or eth.open_px is None:
        diag["reason"] = "spot_warming"
        return None, diag
    sigma_b = stats.realized_sigma_per_sec("btc") or 3.5e-4
    sigma_e = stats.realized_sigma_per_sec("eth") or 4.0e-4
    vol_b_bp = sigma_b * math.sqrt(60.0) * 10000.0
    vol_e_bp = sigma_e * math.sqrt(60.0) * 10000.0
    diag["vol_b_bp_60s"] = vol_b_bp
    diag["vol_e_bp_60s"] = vol_e_bp
    if vol_b_bp < gates.min_vol_bp_60s or vol_e_bp < gates.min_vol_bp_60s:
        diag["reason"] = f"vol_floor:btc={vol_b_bp:.1f}/eth={vol_e_bp:.1f}<{gates.min_vol_bp_60s:.1f}bp"
        return None, diag
    fair_up_btc = fair_up_from_spot(btc.open_px, btc_spot, sigma_b, tte_s)
    fair_up_eth = fair_up_from_spot(eth.open_px, eth_spot, sigma_e, tte_s)
    sig = evaluate_arb_box(
        best_ask_tuple(btc.yes_book),
        best_ask_tuple(btc.no_book),
        best_ask_tuple(eth.yes_book),
        best_ask_tuple(eth.no_book),
        rho,
        fair_up_btc,
        fair_up_eth,
    )
    diag.update({
        "rho": rho,
        "fair_up_btc": fair_up_btc,
        "fair_up_eth": fair_up_eth,
        "sigma_btc": sigma_b,
        "sigma_eth": sigma_e,
    })
    if sig is None:
        diag["reason"] = "no_box"
        return None, diag
    diag["gap"] = sig.gap
    diag["direction"] = sig.direction
    if sig.gap < gates.min_gap:
        diag["reason"] = f"gap_small:{sig.gap:.4f}"
        return None, diag
    if sig.gap > gates.max_gap:
        diag["reason"] = f"gap_toxic:{sig.gap:.4f}"
        return None, diag
    if sig.size_a < gates.min_book_size or sig.size_b < gates.min_book_size:
        diag["reason"] = f"thin_book:{sig.size_a:.1f}/{sig.size_b:.1f}"
        return None, diag
    fair_a = fair_up_btc if sig.leg_a == "BTC_YES" else (1.0 - fair_up_btc) if sig.leg_a == "BTC_NO" else fair_up_eth if sig.leg_a == "ETH_YES" else (1.0 - fair_up_eth)
    fair_b = fair_up_btc if sig.leg_b == "BTC_YES" else (1.0 - fair_up_btc) if sig.leg_b == "BTC_NO" else fair_up_eth if sig.leg_b == "ETH_YES" else (1.0 - fair_up_eth)
    if fair_a + fair_b < (sig.price_a + sig.price_b) - gates.fair_block_margin:
        diag["reason"] = f"fair_blocks:{fair_a + fair_b:.3f}<{sig.price_a + sig.price_b:.3f}"
        return None, diag
    q = hist.quantile(0.95)
    if q is not None and sig.gap > q * 1.5 and len(hist.samples) > 100:
        diag["reason"] = f"gap_outlier:{sig.gap:.4f}>{q * 1.5:.4f}"
        return None, diag

    # --- Finalized entry filter (asym mid + min favorable bp) ---
    opp_a_label = opposite_label(sig.leg_a)
    opp_b_label = opposite_label(sig.leg_b)
    opp_a_ask = best_ask_tuple(get_book_for_label(legs_by_asset, opp_a_label))
    opp_b_ask = best_ask_tuple(get_book_for_label(legs_by_asset, opp_b_label))
    if not opp_a_ask or not opp_b_ask:
        diag["reason"] = "no_opp_quote_for_entry_filter"
        return None, diag
    mid_a = 1.0 - opp_a_ask[0]
    mid_b = 1.0 - opp_b_ask[0]
    bp_btc = (btc_spot / btc.open_px - 1.0) * 1e4
    bp_eth = (eth_spot / eth.open_px - 1.0) * 1e4
    side_a = label_to_side(sig.leg_a)
    side_b = label_to_side(sig.leg_b)
    bp_a = bp_btc if sig.leg_a.startswith("BTC") else bp_eth
    bp_b = bp_btc if sig.leg_b.startswith("BTC") else bp_eth
    fav_bp_a = bp_a if side_a == "YES" else -bp_a
    fav_bp_b = bp_b if side_b == "YES" else -bp_b
    diag.update({"mid_a": mid_a, "mid_b": mid_b,
                 "fav_bp_a": fav_bp_a, "fav_bp_b": fav_bp_b})
    m_hi = max(mid_a, mid_b)
    m_lo = min(mid_a, mid_b)
    if m_hi < gates.entry_asym_mid_hi or m_lo > gates.entry_asym_mid_lo:
        diag["reason"] = (f"entry_coinflip:mid={mid_a:.2f}/{mid_b:.2f} "
                          f"need hi>={gates.entry_asym_mid_hi:.2f} lo<={gates.entry_asym_mid_lo:.2f}")
        return None, diag
    if min(fav_bp_a, fav_bp_b) < gates.entry_min_fav_bp:
        diag["reason"] = (f"leg_underwater:fav_bp={fav_bp_a:+.1f}/{fav_bp_b:+.1f} "
                          f"need>={gates.entry_min_fav_bp:+.1f}")
        return None, diag

    diag["fair_a"] = fair_a
    diag["fair_b"] = fair_b
    return sig, diag


def label_to_side(label: str) -> str:
    return label.split("_")[1]


def label_to_asset(label: str) -> str:
    return label.split("_")[0]


async def strategy_loop(
    legs_by_asset: dict[str, MarketLeg],
    stats: RollingStats,
    gates: GateConfig,
    hist: GapHistogram,
    stop_at: float,
    end_ts: int,
    jsonl: Path,
    round_idx: int,
    notify=None,
) -> dict:
    combos: list[LabCombo] = []
    last_log = 0.0
    samples_since_eval = 0
    last_fill_at = 0.0
    next_combo_id = 1
    total_cost = 0.0

    def aggregate_cost() -> float:
        return sum((c.price_a + c.price_b) * c.qty for c in combos)

    while time.time() < stop_at:
        now = time.time()
        tte = max(end_ts - int(now), 0)

        btc = legs_by_asset["BTC"]
        eth = legs_by_asset["ETH"]
        ba = best_ask_tuple(btc.yes_book)
        na = best_ask_tuple(btc.no_book)
        eya = best_ask_tuple(eth.yes_book)
        ena = best_ask_tuple(eth.no_book)
        if ba and ena:
            hist.add(1.0 - ba[0] - ena[0])
        if na and eya:
            hist.add(1.0 - na[0] - eya[0])

        if now - last_log >= 5.0:
            rho = stats.correlation()
            _log(jsonl, {
                "kind": "tick",
                "ts": now,
                "tte": tte,
                "rho": rho,
                "btc_yes_ask": list(ba) if ba else None,
                "btc_no_ask": list(na) if na else None,
                "eth_yes_ask": list(eya) if eya else None,
                "eth_no_ask": list(ena) if ena else None,
                "btc_spot": btc.last_spot(),
                "eth_spot": eth.last_spot(),
                "gap_p95": hist.quantile(0.95),
                "samples": len(hist.samples),
                "combos_open": len(combos),
                "total_cost": round(total_cost, 4),
            })
            last_log = now

        main_combos = [c for c in combos if not c.is_hedge]
        cooldown_ok = (now - last_fill_at) >= gates.combo_cooldown_s
        capacity_ok = len(main_combos) < gates.max_combos_per_round and total_cost < gates.max_cost_per_round_usd
        if cooldown_ok and capacity_ok:
            sig, diag = try_entry(legs_by_asset, stats, gates, hist, tte)
            if samples_since_eval == 0:
                _log(jsonl, {"kind": "entry_eval", "ts": now, "tte": tte,
                             "combos_open": len(combos), "total_cost": round(total_cost, 4), **diag})
            samples_since_eval = (samples_since_eval + 1) % 20
            if sig is not None:
                book_a = get_book_for_label(legs_by_asset, sig.leg_a)
                book_b = get_book_for_label(legs_by_asset, sig.leg_b)
                budget_left = gates.max_cost_per_round_usd - total_cost
                max_qty_by_budget = budget_left / max(sig.price_a + sig.price_b, 1e-6)
                qty_target = min(gates.combo_qty, sig.size_a, sig.size_b, max_qty_by_budget)
                if qty_target <= 0:
                    _log(jsonl, {"kind": "entry_reject", "ts": now, "tte": tte,
                                 "reason": "no_budget_or_size", "budget_left": budget_left})
                else:
                    fa, pa = shadow_fill(book_a, "BUY", qty_target)
                    fb, pb = shadow_fill(book_b, "BUY", qty_target)
                    qty = min(fa, fb)
                    if qty > 0:
                        combo = LabCombo(
                            combo_id=next_combo_id,
                            direction=sig.direction,
                            leg_a=sig.leg_a, leg_b=sig.leg_b,
                            price_a=pa, price_b=pb, qty=qty,
                            entered_at=now,
                        )
                        combos.append(combo)
                        last_fill_at = now
                        total_cost = aggregate_cost()
                        next_combo_id += 1
                        _log(jsonl, {
                            "kind": "entry_fill", "ts": now, "tte": tte,
                            "combo_id": combo.combo_id,
                            "direction": sig.direction,
                            "leg_a": sig.leg_a, "price_a": pa, "qty_a": fa,
                            "leg_b": sig.leg_b, "price_b": pb, "qty_b": fb,
                            "qty": qty, "gap": sig.gap, "rho": sig.correlation,
                            "fair_diff": sig.fair_diff,
                            "is_hedge": False,
                            "combos_open": len(combos), "total_cost": round(total_cost, 4),
                        })
                        if notify:
                            notify("entry", {
                                "round": round_idx, "combo_id": combo.combo_id,
                                "tte": tte, "direction": sig.direction,
                                "leg_a": sig.leg_a, "price_a": pa,
                                "leg_b": sig.leg_b, "price_b": pb,
                                "qty": qty, "gap": sig.gap, "rho": sig.correlation,
                            })
                        if gates.enable_tail_hedge:
                            rev = evaluate_reverse_box(ba, na, eya, ena, sig.direction)
                            if rev is not None:
                                rev_cost = rev.price_a + rev.price_b
                                combined = (pa + pb) + rev_cost
                                hedge_qty_target = qty * gates.tail_hedge_qty_ratio
                                if rev_cost <= gates.tail_hedge_max_box_cost and \
                                   combined <= gates.tail_hedge_max_total_cost and \
                                   hedge_qty_target > 0:
                                    book_ha = get_book_for_label(legs_by_asset, rev.leg_a)
                                    book_hb = get_book_for_label(legs_by_asset, rev.leg_b)
                                    hfa, hpa = shadow_fill(book_ha, "BUY", hedge_qty_target)
                                    hfb, hpb = shadow_fill(book_hb, "BUY", hedge_qty_target)
                                    hqty = min(hfa, hfb)
                                    if hqty > 0:
                                        hedge = LabCombo(
                                            combo_id=next_combo_id,
                                            direction=rev.direction,
                                            leg_a=rev.leg_a, leg_b=rev.leg_b,
                                            price_a=hpa, price_b=hpb, qty=hqty,
                                            entered_at=now, is_hedge=True,
                                            parent_combo_id=combo.combo_id,
                                        )
                                        combos.append(hedge)
                                        total_cost = aggregate_cost()
                                        next_combo_id += 1
                                        _log(jsonl, {
                                            "kind": "tail_hedge_fill", "ts": now, "tte": tte,
                                            "combo_id": hedge.combo_id,
                                            "parent_combo_id": combo.combo_id,
                                            "direction": rev.direction,
                                            "leg_a": rev.leg_a, "price_a": hpa, "qty_a": hfa,
                                            "leg_b": rev.leg_b, "price_b": hpb, "qty_b": hfb,
                                            "qty": hqty, "main_cost": pa + pb,
                                            "hedge_cost": rev_cost, "combined": combined,
                                            "total_cost": round(total_cost, 4),
                                        })
                                else:
                                    _log(jsonl, {
                                        "kind": "tail_hedge_skip", "ts": now, "tte": tte,
                                        "parent_combo_id": combo.combo_id,
                                        "rev_direction": rev.direction,
                                        "rev_cost": rev_cost, "combined": combined,
                                        "reason": ("hedge_too_pricey" if rev_cost > gates.tail_hedge_max_box_cost
                                                   else "combined_no_margin"),
                                    })

        if combos:
            btc_spot = btc.last_spot()
            eth_spot = eth.last_spot()
            sigma_b = stats.realized_sigma_per_sec("btc") or 3.5e-4
            sigma_e = stats.realized_sigma_per_sec("eth") or 4.0e-4
            fub = fair_up_from_spot(btc.open_px or btc_spot or 1.0, btc_spot or 1.0, sigma_b, tte) if btc_spot else 0.5
            fue = fair_up_from_spot(eth.open_px or eth_spot or 1.0, eth_spot or 1.0, sigma_e, tte) if eth_spot else 0.5
            for combo in combos:
                if combo.flipped_a and combo.flipped_b:
                    continue
                asset_a = label_to_asset(combo.leg_a)
                asset_b = label_to_asset(combo.leg_b)
                side_a = label_to_side(combo.leg_a)
                side_b = label_to_side(combo.leg_b)
                fair_a = fub if asset_a == "BTC" else fue
                fair_b = fub if asset_b == "BTC" else fue
                opp_a_label = opposite_label(combo.leg_a)
                opp_b_label = opposite_label(combo.leg_b)
                opp_a_book = get_book_for_label(legs_by_asset, opp_a_label)
                opp_b_book = get_book_for_label(legs_by_asset, opp_b_label)
                opp_a = best_ask_tuple(opp_a_book)
                opp_b = best_ask_tuple(opp_b_book)
                plan = policy_g_kill(
                    leg_a_side=side_a, fair_up_a=fair_a, entry_a=combo.price_a,
                    leg_b_side=side_b, fair_up_b=fair_b, entry_b=combo.price_b,
                    leg_a_opp_ask=opp_a, leg_b_opp_ask=opp_b,
                    qty=combo.qty,
                    eps_loss=gates.policy_g_eps_loss,
                    t_recov=gates.policy_g_t_recov,
                    t_lock=gates.policy_g_t_lock,
                )
                if plan is None:
                    continue
                _apx, asz, areason = plan["leg_a"]
                _bpx, bsz, breason = plan["leg_b"]
                ffa, fpa = shadow_fill(opp_a_book, "BUY", combo.qty)
                ffb, fpb = shadow_fill(opp_b_book, "BUY", combo.qty)
                if ffa >= combo.qty and ffb >= combo.qty:
                    combo.flipped_a = True
                    combo.flip_price_a = fpa
                    combo.flip_reason_a = areason
                    combo.flipped_b = True
                    combo.flip_price_b = fpb
                    combo.flip_reason_b = breason
                    _log(jsonl, {
                        "kind": "combo_kill", "ts": now, "tte": tte,
                        "combo_id": combo.combo_id, "is_hedge": combo.is_hedge,
                        "leg_a": combo.leg_a, "opp_a": opp_a_label,
                        "reason_a": areason, "entry_a": combo.price_a,
                        "flip_price_a": fpa, "qty_a": ffa,
                        "leg_b": combo.leg_b, "opp_b": opp_b_label,
                        "reason_b": breason, "entry_b": combo.price_b,
                        "flip_price_b": fpb, "qty_b": ffb,
                        "fair_up_btc": fub, "fair_up_eth": fue,
                    })
                    if notify:
                        notify("flip", {
                            "round": round_idx, "combo_id": combo.combo_id,
                            "tte": tte, "reason": areason,
                            "leg_a": combo.leg_a, "entry_a": combo.price_a, "flip_a": fpa,
                            "leg_b": combo.leg_b, "entry_b": combo.price_b, "flip_b": fpb,
                            "qty": combo.qty,
                        })
                else:
                    _log(jsonl, {
                        "kind": "combo_kill_partial", "ts": now, "tte": tte,
                        "combo_id": combo.combo_id, "ffa": ffa, "ffb": ffb,
                        "qty_target": combo.qty,
                    })

        poll_dt = gates.poll_fast_s if tte <= gates.fast_poll_tte_s else gates.poll_normal_s
        await asyncio.sleep(poll_dt)

    btc = legs_by_asset["BTC"]
    eth = legs_by_asset["ETH"]
    btc_final = btc.last_spot() or btc.open_px or 0.0
    eth_final = eth.last_spot() or eth.open_px or 0.0
    bin_btc_up = 1.0 if (btc.open_px is not None and btc_final >= btc.open_px) else 0.0
    bin_eth_up = 1.0 if (eth.open_px is not None and eth_final >= eth.open_px) else 0.0
    pending = {
        "round": round_idx, "kind": "round_pending",
        "btc_slug": btc.slug, "eth_slug": eth.slug,
        "btc_open": btc.open_px, "btc_final": btc_final,
        "eth_open": eth.open_px, "eth_final": eth_final,
        "binance_btc_up": bin_btc_up, "binance_eth_up": bin_eth_up,
        "combos_count": len(combos),
        "combos": combos,
        "jsonl": str(jsonl),
    }
    _log(jsonl, {**pending, "combos": [c.__dict__ for c in combos]})
    print(f"[ROUND {round_idx}] window ended; trading phase done with {len(combos)} combos "
          f"(cost ${sum((c.price_a + c.price_b) * c.qty for c in combos):.2f}). "
          f"PM resolution deferred to post-run.")
    return pending


async def resolve_round(pending: dict, gates: GateConfig, notify=None) -> dict:
    """Take a `round_pending` dict and poll PM until both legs resolve (or timeout).
    Computes per-combo PnL using ONLY the PM/UMA outcome. No Binance fallback."""
    round_idx = pending["round"]
    btc_slug = pending["btc_slug"]
    eth_slug = pending["eth_slug"]
    combos: list[LabCombo] = pending["combos"]
    jsonl_path = Path(pending["jsonl"])
    bin_btc_up = pending["binance_btc_up"]
    bin_eth_up = pending["binance_eth_up"]
    print(f"[RESOLVE R{round_idx}] polling PM for {btc_slug} & {eth_slug} "
          f"(timeout {int(gates.pm_resolution_wait_s)}s)")
    pm_btc_up, pm_eth_up = await asyncio.gather(
        fetch_pm_payoff(btc_slug, max_wait_s=gates.pm_resolution_wait_s,
                        poll_every_s=gates.pm_resolution_poll_s),
        fetch_pm_payoff(eth_slug, max_wait_s=gates.pm_resolution_wait_s,
                        poll_every_s=gates.pm_resolution_poll_s),
    )
    resolved = pm_btc_up is not None and pm_eth_up is not None
    btc_divergence = pm_btc_up is not None and pm_btc_up != bin_btc_up
    eth_divergence = pm_eth_up is not None and pm_eth_up != bin_eth_up
    if btc_divergence or eth_divergence:
        print(f"[RESOLVE R{round_idx}] !!! PM/Binance divergence btc(pm={pm_btc_up} bin={bin_btc_up}) "
              f"eth(pm={pm_eth_up} bin={bin_eth_up})")
    summary: dict = {
        "round": round_idx, "kind": "round_end",
        "btc_slug": btc_slug, "eth_slug": eth_slug,
        "btc_open": pending["btc_open"], "btc_final": pending["btc_final"],
        "eth_open": pending["eth_open"], "eth_final": pending["eth_final"],
        "pm_btc_up": pm_btc_up, "pm_eth_up": pm_eth_up,
        "binance_btc_up": bin_btc_up, "binance_eth_up": bin_eth_up,
        "resolved": resolved,
        "divergence": btc_divergence or eth_divergence,
        "combos_count": len(combos),
    }
    if not resolved:
        print(f"[RESOLVE R{round_idx}] UNRESOLVED after timeout; skipping PnL computation")
        summary["status"] = "UNRESOLVED"
        summary["combos"] = [{
            "combo_id": c.combo_id, "direction": c.direction,
            "leg_a": c.leg_a, "price_a": c.price_a,
            "leg_b": c.leg_b, "price_b": c.price_b, "qty": c.qty,
            "flipped_a": c.flipped_a, "flip_price_a": c.flip_price_a,
            "flipped_b": c.flipped_b, "flip_price_b": c.flip_price_b,
        } for c in combos]
        _log(jsonl_path, summary)
        return summary
    btc_up = float(pm_btc_up)
    eth_up = float(pm_eth_up)
    payoffs = {"BTC_YES": btc_up, "BTC_NO": 1.0 - btc_up, "ETH_YES": eth_up, "ETH_NO": 1.0 - eth_up}
    total_cost = 0.0
    total_gross = 0.0
    total_flip = 0.0
    combo_rows = []
    for c in combos:
        leg_a_pay = payoffs[c.leg_a] * c.qty
        leg_b_pay = payoffs[c.leg_b] * c.qty
        cost = (c.price_a + c.price_b) * c.qty
        flip_pnl_a = (payoffs[opposite_label(c.leg_a)] - c.flip_price_a) * c.qty if c.flipped_a else 0.0
        flip_pnl_b = (payoffs[opposite_label(c.leg_b)] - c.flip_price_b) * c.qty if c.flipped_b else 0.0
        pnl_c = leg_a_pay + leg_b_pay - cost + flip_pnl_a + flip_pnl_b
        total_cost += cost
        total_gross += leg_a_pay + leg_b_pay
        total_flip += flip_pnl_a + flip_pnl_b
        combo_rows.append({
            "combo_id": c.combo_id, "direction": c.direction,
            "leg_a": c.leg_a, "price_a": c.price_a, "payoff_a": payoffs[c.leg_a],
            "leg_b": c.leg_b, "price_b": c.price_b, "payoff_b": payoffs[c.leg_b],
            "qty": c.qty,
            "flipped_a": c.flipped_a, "flip_price_a": c.flip_price_a, "flip_pnl_a": flip_pnl_a,
            "flipped_b": c.flipped_b, "flip_price_b": c.flip_price_b, "flip_pnl_b": flip_pnl_b,
            "cost": cost, "gross": leg_a_pay + leg_b_pay, "pnl": pnl_c,
        })
    pnl = total_gross - total_cost + total_flip
    summary.update({
        "status": "OK",
        "payoffs": payoffs,
        "combos": combo_rows,
        "cost": total_cost, "gross": total_gross,
        "flip_pnl": total_flip, "pnl": pnl,
    })
    _log(jsonl_path, summary)
    print(f"[RESOLVE R{round_idx}] DONE pnl=${pnl:+.2f} (cost=${total_cost:.2f} gross=${total_gross:.2f} flip=${total_flip:+.2f})")
    if notify:
        notify("settle", {
            "round": round_idx, "pnl": pnl,
            "cost": total_cost, "gross": total_gross, "flip_pnl": total_flip,
            "n_combos": len(combos),
            "pm_btc_up": pm_btc_up, "pm_eth_up": pm_eth_up,
            "divergence": btc_divergence or eth_divergence,
        })
    return summary


async def run_round(round_idx: int, gates: GateConfig, stats: RollingStats,
                    hist: GapHistogram, notify=None) -> dict:
    btc_leg = discover_leg("BTC", "BTCUSDT")
    eth_leg = discover_leg("ETH", "ETHUSDT")
    if btc_leg.start_ts != eth_leg.start_ts or btc_leg.end_ts != eth_leg.end_ts:
        print(f"[ROUND {round_idx}] WARNING: BTC/ETH market windows misaligned "
              f"btc={btc_leg.start_ts}-{btc_leg.end_ts} eth={eth_leg.start_ts}-{eth_leg.end_ts}")
    legs = [btc_leg, eth_leg]
    legs_by_asset = {"BTC": btc_leg, "ETH": eth_leg}
    end_ts = min(btc_leg.end_ts, eth_leg.end_ts)
    stop_at = end_ts + 2
    ts_tag = btc_leg.start_ts
    jsonl = OUT / f"lab_corr_arb_round{round_idx}_{ts_tag}.jsonl"
    print(f"[ROUND {round_idx}] start btc_slug={btc_leg.slug} eth_slug={eth_leg.slug} "
          f"window={btc_leg.start_ts}-{end_ts} jsonl={jsonl.name}")
    _log(jsonl, {
        "kind": "round_start", "round": round_idx,
        "btc_slug": btc_leg.slug, "eth_slug": eth_leg.slug,
        "btc_start_ts": btc_leg.start_ts, "btc_end_ts": btc_leg.end_ts,
        "eth_start_ts": eth_leg.start_ts, "eth_end_ts": eth_leg.end_ts,
        "gates": gates.__dict__,
    })
    results = await asyncio.gather(
        ws_consumer(legs, stop_at),
        spot_pump(legs, stop_at, stats),
        strategy_loop(legs_by_asset, stats, gates, hist, stop_at, end_ts, jsonl, round_idx, notify=notify),
    )
    pending = results[2]
    return pending


def _build_notifier():
    token = os.getenv("TG_BOT_TOKEN", "").strip()
    chat = os.getenv("TG_CHAT_ID", "").strip()
    if not token or not chat:
        print("[BOOT] TG notifier disabled (TG_BOT_TOKEN / TG_CHAT_ID not set)")
        return None
    thread = os.getenv("TG_THREAD_ID", "").strip() or None
    notifier = TelegramNotifier(TelegramConfig(bot_token=token, chat_id=chat, thread_id=thread))
    print(f"[BOOT] TG notifier enabled chat={chat[:4]}***")
    return notifier


def _format_event(kind: str, data: dict) -> str:
    if kind == "entry":
        return (f"\U0001f7e2 ENTRY R{data['round']} c{data['combo_id']} tte={data['tte']}s\n"
                f"  {data['leg_a']} @ {data['price_a']:.3f}\n"
                f"  {data['leg_b']} @ {data['price_b']:.3f}\n"
                f"  qty={data['qty']:.2f}  gap={data['gap']:+.4f}  rho={data['correlation']:.2f}"
                if 'correlation' in data else
                f"\U0001f7e2 ENTRY R{data['round']} c{data['combo_id']} tte={data['tte']}s\n"
                f"  {data['leg_a']} @ {data['price_a']:.3f}\n"
                f"  {data['leg_b']} @ {data['price_b']:.3f}\n"
                f"  qty={data['qty']:.2f}  gap={data['gap']:+.4f}  rho={data['rho']:.2f}")
    if kind == "flip":
        return (f"\U0001f534 FLIP R{data['round']} c{data['combo_id']} tte={data['tte']}s\n"
                f"  reason: {data['reason']}\n"
                f"  {data['leg_a']} entry={data['entry_a']:.3f} -> flip={data['flip_a']:.3f}\n"
                f"  {data['leg_b']} entry={data['entry_b']:.3f} -> flip={data['flip_b']:.3f}\n"
                f"  qty={data['qty']:.2f}")
    if kind == "settle":
        div = " (DIV)" if data.get("divergence") else ""
        return (f"\U0001f3c1 SETTLE R{data['round']}{div} pnl=${data['pnl']:+.2f}\n"
                f"  combos={data['n_combos']}  cost=${data['cost']:.2f}  gross=${data['gross']:.2f}  flip=${data['flip_pnl']:+.2f}\n"
                f"  pm_btc={data['pm_btc_up']}  pm_eth={data['pm_eth_up']}")
    return f"{kind}: {data}"


def _make_notify(notifier):
    def _notify(kind: str, data: dict) -> None:
        try:
            text = _format_event(kind, data)
        except Exception as exc:
            text = f"{kind} (fmt_err={exc}): {data}"
        print(f"[NOTIFY] {text.splitlines()[0]}")
        if notifier is None:
            return
        try:
            notifier.send(text)
        except Exception as exc:
            print(f"[NOTIFY] send_failed: {exc}")
    return _notify


async def main_async(rounds: int, start_mode: str, gates: GateConfig) -> None:
    stats = RollingStats(window=120)
    hist = GapHistogram(window=600)
    notifier = _build_notifier()
    notify = _make_notify(notifier)
    dry_run = os.getenv("DRY_RUN", "true").strip().lower() not in ("0", "false", "no")
    print(f"[BOOT] DRY_RUN={dry_run}  rounds={rounds}  start_mode={start_mode}")
    if not dry_run:
        print("[BOOT] WARNING: DRY_RUN=false -- live execution is NOT YET WIRED; staying shadow-only")
    if notifier is not None:
        try:
            notifier.send(f"\U0001f680 cor_pol boot\nDRY_RUN={dry_run} rounds={rounds} mode={start_mode}\n"
                          f"asym=[{gates.entry_asym_mid_lo:.2f},{gates.entry_asym_mid_hi:.2f}] "
                          f"min_fav_bp={gates.entry_min_fav_bp:+.1f}\n"
                          f"G eps={gates.policy_g_eps_loss:.2f} t_recov={gates.policy_g_t_recov:.2f} "
                          f"t_lock={gates.policy_g_t_lock:.2f}")
        except Exception as exc:
            print(f"[BOOT] tg boot ping failed: {exc}")
    if start_mode == "next":
        wait = 300 - (int(time.time()) % 300)
        print(f"[LAB] waiting {wait + 1}s for next 5m boundary")
        await asyncio.sleep(wait + 1)
    pendings: list[dict] = []
    for i in range(1, rounds + 1):
        try:
            pending = await run_round(i, gates, stats, hist, notify=notify)
            pendings.append(pending)
        except Exception as exc:
            print(f"[ROUND {i}] error: {exc}")
        if i < rounds:
            now = int(time.time())
            mod = now % 300
            if mod < 60:
                wait = 3
                print(f"[LAB] round {i} done; back-to-back start in {wait}s")
            else:
                wait = 300 - mod
                print(f"[LAB] round {i} done; waiting {wait + 1}s for next 5m boundary")
                wait = wait + 1
            await asyncio.sleep(wait)
    print(f"[LAB] all {rounds} trading phases done; resolving {len(pendings)} rounds via PM/UMA now...")
    summaries = await asyncio.gather(*(resolve_round(p, gates, notify=notify) for p in pendings))
    n_ok = sum(1 for s in summaries if s.get("status") == "OK")
    n_div = sum(1 for s in summaries if s.get("divergence"))
    total_pnl = sum(s.get("pnl", 0.0) for s in summaries if s.get("status") == "OK")
    print(f"[LAB] resolution complete: {n_ok}/{len(summaries)} resolved, {n_div} divergences, total_pnl=${total_pnl:+.2f}")
    if notifier is not None:
        try:
            notifier.send(f"\U0001f4ca cor_pol run done\nresolved={n_ok}/{len(summaries)} div={n_div} total_pnl=${total_pnl:+.2f}")
        except Exception:
            pass


def _envf(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _envi(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def main() -> None:
    parser = argparse.ArgumentParser(description="cor_pol: BTC/ETH correlation arb bot (shadow / dry-run)")
    parser.add_argument("--rounds", type=int, default=_envi("CORR_ROUNDS", 1000000))
    parser.add_argument("--start-mode", choices=("current", "next"),
                        default=os.getenv("CORR_START_MODE", "next"))
    args = parser.parse_args()
    gates = GateConfig(
        min_correlation=_envf("CORR_MIN_CORRELATION", 0.65),
        min_gap=_envf("CORR_MIN_GAP", 0.06),
        max_gap=_envf("CORR_MAX_GAP", 0.18),
        min_book_size=_envf("CORR_MIN_BOOK_SIZE", 5.0),
        tte_min_s=_envi("CORR_TTE_MIN_S", 60),
        tte_max_s=_envi("CORR_TTE_MAX_S", 270),
        combo_qty=_envf("CORR_COMBO_QTY", 5.0),
        max_combos_per_round=_envi("CORR_MAX_COMBOS_PER_ROUND", 5),
        max_cost_per_round_usd=_envf("CORR_MAX_COST_PER_ROUND_USD", 50.0),
        combo_cooldown_s=_envf("CORR_COMBO_COOLDOWN_S", 15.0),
        pm_resolution_wait_s=_envf("CORR_PM_RESOLUTION_WAIT_S", 1200.0),
        pm_resolution_poll_s=_envf("CORR_PM_RESOLUTION_POLL_S", 15.0),
        min_vol_bp_60s=_envf("CORR_MIN_VOL_BP_60S", 10.0),
        entry_asym_mid_hi=_envf("CORR_ENTRY_ASYM_MID_HI", 0.60),
        entry_asym_mid_lo=_envf("CORR_ENTRY_ASYM_MID_LO", 0.40),
        entry_min_fav_bp=_envf("CORR_ENTRY_MIN_FAV_BP", -4.0),
        policy_g_eps_loss=_envf("CORR_POLICY_G_EPS_LOSS", 0.05),
        policy_g_t_recov=_envf("CORR_POLICY_G_T_RECOV", 0.55),
        policy_g_t_lock=_envf("CORR_POLICY_G_T_LOCK", 0.80),
    )
    asyncio.run(main_async(args.rounds, args.start_mode, gates))


if __name__ == "__main__":
    main()