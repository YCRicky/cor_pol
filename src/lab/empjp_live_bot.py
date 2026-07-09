from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
for p in (str(ROOT), str(SRC)):
    if p not in sys.path:
        sys.path.insert(0, p)

from execution import LiveExecutionConfig, PolymarketLiveExecutor, ExecutionLegResult  # noqa: E402
from lab.correlation_arb_bot import (  # noqa: E402
    GateConfig,
    MarketLeg,
    UserOrderFeed,
    _build_notifier,
    _envb,
    _envf,
    _envi,
    _log,
    discover_leg,
    execute_buy_leg,
    fetch_pm_payoff,
    is_weekend_rest_utc8,
    utc8_now,
    weekend_rest_resume_ts_utc8,
    ws_consumer,
)
from lab.empjp_core import (  # noqa: E402
    DEFAULT_CALIBRATION_PATH,
    EmpJPCalibration,
    EmpJPCandidate,
    EmpJPConfig,
    evaluate_empjp,
)

OUT = ROOT / "out"
OUT.mkdir(parents=True, exist_ok=True)


@dataclass
class PendingSignal:
    due_ts: float
    side: str
    side_prob: float
    edge_at_signal: float
    cell_n: int
    cell_key: str
    signal_elapsed_s: int
    signal_tte: float
    signal_bp: float


@dataclass
class EmpJPTrade:
    round_idx: int
    slug: str
    start_ts: int
    end_ts: int
    side: str
    qty: float
    entry_price: float
    entry_fee: float
    entry_elapsed_s: int
    signal_elapsed_s: int
    side_prob: float
    edge: float
    cell_n: int
    cell_key: str
    order_id: str = ""
    execution_status: str = ""
    execution_error: str = ""


@dataclass
class EmpJPLedger:
    resolved_count: int = 0
    total_pnl: float = 0.0
    total_cost: float = 0.0
    wins: int = 0
    trades: int = 0

    def record(self, pnl: float, cost: float, win: bool) -> dict:
        self.resolved_count += 1
        self.trades += 1
        self.total_pnl += float(pnl)
        self.total_cost += float(cost)
        self.wins += int(win)
        return {
            "resolved_count": self.resolved_count,
            "run_pnl": self.total_pnl,
            "run_cost": self.total_cost,
            "run_wins": self.wins,
            "run_trades": self.trades,
        }


def _clean_env(name: str) -> str:
    return (os.getenv(name, "") or "").strip().strip('"').strip("'")


def _dry_run() -> bool:
    return os.getenv("DRY_RUN", "true").strip().lower() not in ("0", "false", "no", "off")


def _fee_total(price: float, qty: float, cfg: EmpJPConfig) -> float:
    return qty * cfg.effective_fee_rate * max(0.0, min(1.0, price)) * (1.0 - max(0.0, min(1.0, price)))


def _book_for_side(leg: MarketLeg, side: str):
    return leg.yes_book if side == "YES" else leg.no_book


def _format_event(kind: str, data: dict) -> str:
    if kind == "boot":
        return (
            "[EMPJP e75 n30 c1 l1] BOOT\n"
            f"mode={data['mode']} rounds={data['rounds']} start={data['start_mode']}\n"
            f"qty={data['qty']:.2f} edge>={data['edge_min']:.3f} cell_n>={data['min_cell_n']} "
            f"confirm={data['confirm_s']}s latency={data['latency_s']}s\n"
            f"calibration_cells={data['cells']} weekend_rest={data['weekend_rest']}"
        )
    if kind == "entry":
        return (
            "[EMPJP e75 n30 c1 l1] ENTRY\n"
            f"R{data['round']} {data['side']} qty={data['qty']:.2f} @ {data['entry_price']:.4f}\n"
            f"t+{data['entry_elapsed_s']}s tte={data['tte']:.0f}s bp={data['current_bp']:+.2f}\n"
            f"p_side={data['side_prob']:.3f} edge={data['edge']:+.4f} cell_n={data['cell_n']}\n"
            f"fill={data['filled_qty']:.2f} avg={data['avg_price']:.4f} exec={data['execution']} err={data.get('error','')}\n"
            f"slug={data['slug']}"
        )
    if kind == "settle":
        return (
            "[EMPJP e75 n30 c1 l1] SETTLE\n"
            f"R{data['round']} {data['side']} win={int(data['win'])} pm_up={data['pm_up']:.0f} pnl={data['pnl']:+.4f}U\n"
            f"entry={data['entry_price']:.4f} qty={data['qty']:.2f} fee={data['fee']:.4f}\n"
            f"run_pnl={data['run_pnl']:+.4f}U resolved={data['resolved_count']}"
        )
    if kind == "skip":
        return f"[EMPJP e75 n30 c1 l1] SKIP R{data.get('round')} reason={data.get('reason')} slug={data.get('slug','')}"
    return f"{kind}: {data}"


def _make_notify(notifier):
    def notify(kind: str, data: dict) -> None:
        try:
            text = _format_event(kind, data)
        except Exception as exc:
            text = f"{kind} fmt_error={exc}: {data}"
        print(f"[NOTIFY] {text.splitlines()[0]}", flush=True)
        if notifier is None:
            return
        try:
            notifier.send(text)
        except Exception as exc:
            print(f"[NOTIFY] send_failed: {exc}", flush=True)
    return notify


async def poll_btc_spot(leg: MarketLeg, stop_at: float) -> None:
    while time.time() < stop_at:
        try:
            data = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: __import__("common").get_json("https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT", None, 2.0),
            )
            leg.update_spot(time.time(), float(data["price"]))
        except Exception:
            pass
        await asyncio.sleep(1.0)


def _entry_result_ok(res: ExecutionLegResult, min_qty: float) -> bool:
    return res.filled_qty >= max(0.0, min_qty) and res.avg_price > 0.0 and not (res.error or "").startswith("user_ws_")


async def strategy_loop(
    *,
    leg: MarketLeg,
    round_idx: int,
    cfg: EmpJPConfig,
    calibration: EmpJPCalibration,
    exec_gates: GateConfig,
    stop_at: float,
    jsonl: Path,
    notify,
    live_executor: Optional[PolymarketLiveExecutor],
    order_feed: Optional[UserOrderFeed],
) -> dict:
    pending: Optional[PendingSignal] = None
    trade: Optional[EmpJPTrade] = None
    last_reason = ""
    start_ts = float(leg.start_ts)
    end_ts = float(leg.end_ts)

    while time.time() < stop_at:
        now = time.time()
        elapsed_s = int(max(0.0, now - start_ts))
        tte = max(0.0, end_ts - now)

        if trade is not None:
            await asyncio.sleep(0.25 if tte <= 30 else 0.75)
            continue

        spot_history = list(leg.spot_history)
        candidate, reason = evaluate_empjp(
            calibration=calibration,
            cfg=cfg,
            elapsed_s=elapsed_s,
            tte=tte,
            open_px=leg.open_px,
            last_px=leg.last_spot(),
            spot_history=spot_history,
            yes_book=leg.yes_book,
            no_book=leg.no_book,
        )

        if pending is None:
            if candidate is not None:
                pending = PendingSignal(
                    due_ts=now + cfg.horizon_s,
                    side=candidate.side,
                    side_prob=candidate.side_prob,
                    edge_at_signal=candidate.edge,
                    cell_n=candidate.cell_n,
                    cell_key=candidate.cell_key,
                    signal_elapsed_s=elapsed_s,
                    signal_tte=candidate.tte,
                    signal_bp=candidate.current_bp,
                )
                _log(jsonl, {"kind": "signal", "round": round_idx, **asdict(candidate), "due_ts": pending.due_ts})
            elif reason != last_reason:
                _log(jsonl, {"kind": "hold", "round": round_idx, "reason": reason, "elapsed_s": elapsed_s, "tte": tte})
                last_reason = reason
        else:
            if now >= pending.due_ts:
                # Backtest semantics: signal state is fixed at t, actual entry ask is sampled after confirm+latency.
                current_book = _book_for_side(leg, pending.side)
                ask = current_book.best_ask()
                spr_candidate, spr_reason = evaluate_empjp(
                    calibration=calibration,
                    cfg=cfg,
                    elapsed_s=elapsed_s,
                    tte=tte,
                    open_px=leg.open_px,
                    last_px=leg.last_spot(),
                    spot_history=spot_history,
                    yes_book=leg.yes_book,
                    no_book=leg.no_book,
                )
                if ask is None:
                    _log(jsonl, {"kind": "entry_skip", "round": round_idx, "reason": "no_best_ask_at_latency", "pending": asdict(pending)})
                    pending = None
                    continue
                entry_price = float(ask[0])
                entry_edge = pending.side_prob - entry_price - cfg.effective_fee_rate * entry_price * (1.0 - entry_price)
                if entry_edge < cfg.edge_min:
                    _log(jsonl, {"kind": "entry_skip", "round": round_idx, "reason": "edge_decayed", "edge": entry_edge, "pending": asdict(pending), "live_reason": spr_reason})
                    pending = None
                    continue
                label = f"BTC_{pending.side}"
                res = await execute_buy_leg(
                    legs_by_asset={"BTC": leg},
                    label=label,
                    qty=cfg.quantity,
                    live_executor=live_executor,
                    slippage_ticks=exec_gates.exec_slippage_ticks,
                    gates=exec_gates,
                    order_feed=order_feed,
                )
                min_qty = max(0.0, cfg.quantity - exec_gates.leg_mismatch_tolerance_shares)
                ok = _entry_result_ok(res, min_qty)
                _log(jsonl, {"kind": "entry_execution", "round": round_idx, "ok": ok, "result": asdict(res), "pending": asdict(pending), "entry_edge": entry_edge})
                if not ok:
                    pending = None
                    continue
                trade = EmpJPTrade(
                    round_idx=round_idx,
                    slug=leg.slug,
                    start_ts=leg.start_ts,
                    end_ts=leg.end_ts,
                    side=pending.side,
                    qty=res.filled_qty,
                    entry_price=res.avg_price,
                    entry_fee=_fee_total(res.avg_price, res.filled_qty, cfg),
                    entry_elapsed_s=elapsed_s,
                    signal_elapsed_s=pending.signal_elapsed_s,
                    side_prob=pending.side_prob,
                    edge=entry_edge,
                    cell_n=pending.cell_n,
                    cell_key=pending.cell_key,
                    order_id=res.order_id,
                    execution_status=res.status,
                    execution_error=res.error,
                )
                _log(jsonl, {"kind": "trade_open", **asdict(trade), "raw_execution": res.raw})
                notify("entry", {
                    "round": round_idx,
                    "slug": leg.slug,
                    "side": pending.side,
                    "qty": cfg.quantity,
                    "filled_qty": res.filled_qty,
                    "entry_price": res.avg_price,
                    "avg_price": res.avg_price,
                    "side_prob": pending.side_prob,
                    "edge": entry_edge,
                    "cell_n": pending.cell_n,
                    "entry_elapsed_s": elapsed_s,
                    "tte": tte,
                    "current_bp": pending.signal_bp,
                    "execution": "live" if live_executor else "shadow",
                    "error": res.error,
                })
                pending = None
        await asyncio.sleep(0.25 if tte <= 30 else 0.75)

    if trade is None:
        return {"status": "NO_TRADE", "round": round_idx, "slug": leg.slug, "jsonl": str(jsonl), "last_reason": last_reason}
    return {"status": "OPEN", "round": round_idx, "slug": leg.slug, "jsonl": str(jsonl), "trade": asdict(trade)}


async def resolve_empjp_round(pending: dict, cfg: EmpJPConfig, ledger: EmpJPLedger, notify) -> dict:
    if pending.get("status") != "OPEN":
        return pending
    trade = EmpJPTrade(**pending["trade"])
    jsonl = Path(pending["jsonl"])
    while True:
        pm_up = await fetch_pm_payoff(trade.slug, max_wait_s=_envf("EMPJP_PM_RESOLUTION_WAIT_S", 1200.0), poll_every_s=_envf("EMPJP_PM_RESOLUTION_POLL_S", 15.0))
        if pm_up is not None:
            break
        retry_s = max(1.0, _envf("EMPJP_PM_RESOLUTION_RETRY_S", 60.0))
        _log(jsonl, {"kind": "resolution_retry", "round": trade.round_idx, "retry_in_s": retry_s})
        await asyncio.sleep(retry_s)
    win = bool(pm_up >= 0.5) if trade.side == "YES" else bool(pm_up < 0.5)
    gross = trade.qty * (1.0 if win else 0.0)
    cost = trade.qty * trade.entry_price
    pnl = gross - cost - trade.entry_fee
    totals = ledger.record(pnl, cost, win)
    summary = {
        "status": "OK",
        "round": trade.round_idx,
        "slug": trade.slug,
        "side": trade.side,
        "pm_up": pm_up,
        "win": win,
        "qty": trade.qty,
        "entry_price": trade.entry_price,
        "fee": trade.entry_fee,
        "gross": gross,
        "cost": cost,
        "pnl": pnl,
        **totals,
    }
    _log(jsonl, {"kind": "settle", **summary})
    notify("settle", summary)
    return summary


async def run_round(
    *,
    round_idx: int,
    cfg: EmpJPConfig,
    calibration: EmpJPCalibration,
    exec_gates: GateConfig,
    notify,
    live_executor: Optional[PolymarketLiveExecutor],
) -> dict:
    leg = discover_leg("BTC", "BTCUSDT")
    stop_at = float(leg.end_ts) + 2.0
    jsonl = OUT / f"empjp_e75_n30_c1_l1_round{round_idx}_{leg.start_ts}.jsonl"
    _log(jsonl, {"kind": "round_start", "round": round_idx, "slug": leg.slug, "start_ts": leg.start_ts, "end_ts": leg.end_ts, "cfg": asdict(cfg), "execution": "live" if live_executor else "shadow"})
    order_feed = UserOrderFeed(markets=[leg.condition_id], jsonl=jsonl) if live_executor is not None and exec_gates.exec_user_ws_enabled else None
    tasks = [
        ws_consumer([leg], stop_at),
        poll_btc_spot(leg, stop_at),
        strategy_loop(
            leg=leg,
            round_idx=round_idx,
            cfg=cfg,
            calibration=calibration,
            exec_gates=exec_gates,
            stop_at=stop_at,
            jsonl=jsonl,
            notify=notify,
            live_executor=live_executor,
            order_feed=order_feed,
        ),
    ]
    if order_feed is not None:
        tasks.append(order_feed.run(stop_at))
    result = await asyncio.gather(*tasks)
    return result[2]


async def main_async(rounds: int, start_mode: str, cfg: EmpJPConfig, exec_gates: GateConfig, calibration_path: Path) -> None:
    calibration = EmpJPCalibration.load(calibration_path)
    notifier = _build_notifier()
    notify = _make_notify(notifier)
    dry_run = _dry_run()
    live_executor: Optional[PolymarketLiveExecutor] = None
    if not dry_run:
        live_cfg = LiveExecutionConfig.from_env()
        live_executor = PolymarketLiveExecutor(live_cfg)
        live_executor.sync_collateral()
        print(f"[BOOT] LIVE CLOB enabled host={live_cfg.host} sig={live_cfg.signature_type} funder={live_cfg.funder[:6]}...", flush=True)
    mode = "shadow" if dry_run else "live"
    print(f"[BOOT] EMPJP e75 n30 c1 l1 mode={mode} rounds={rounds} calibration_cells={len(calibration.cells)}", flush=True)
    notify("boot", {
        "mode": mode,
        "rounds": rounds,
        "start_mode": start_mode,
        "qty": cfg.quantity,
        "edge_min": cfg.edge_min,
        "min_cell_n": cfg.min_cell_n,
        "confirm_s": cfg.confirm_s,
        "latency_s": cfg.latency_s,
        "cells": len(calibration.cells),
        "weekend_rest": exec_gates.weekend_rest_enabled,
    })
    if start_mode == "next" and rounds > 0:
        wait = 300 - (int(time.time()) % 300)
        print(f"[EMPJP] waiting {wait + 1}s for next 5m boundary", flush=True)
        await asyncio.sleep(wait + 1)

    ledger = EmpJPLedger()
    resolve_tasks: set[asyncio.Task] = set()

    async def drain(done_only: bool = True) -> None:
        nonlocal resolve_tasks
        if not resolve_tasks:
            return
        done = {t for t in resolve_tasks if t.done()} if done_only else set(resolve_tasks)
        if not done:
            return
        if done_only:
            resolve_tasks -= done
            for task in done:
                try:
                    task.result()
                except Exception as exc:
                    print(f"[RESOLVE] task error: {exc}", flush=True)
        else:
            await asyncio.gather(*done, return_exceptions=True)
            resolve_tasks -= done

    async def wait_for_active_time() -> None:
        while exec_gates.weekend_rest_enabled and is_weekend_rest_utc8():
            now_ts = time.time()
            resume_ts = weekend_rest_resume_ts_utc8(now_ts)
            wait_s = max(1.0, resume_ts - now_ts)
            resume_local = utc8_now(resume_ts).strftime("%Y-%m-%d %H:%M:%S UTC+8")
            print(f"[EMPJP] weekend rest active; sleeping until {resume_local} ({int(wait_s)}s)", flush=True)
            await drain(done_only=True)
            await asyncio.sleep(min(wait_s, 3600.0))

    for i in range(1, rounds + 1):
        await wait_for_active_time()
        try:
            pending = await run_round(round_idx=i, cfg=cfg, calibration=calibration, exec_gates=exec_gates, notify=notify, live_executor=live_executor)
            if pending.get("status") == "OPEN":
                resolve_tasks.add(asyncio.create_task(resolve_empjp_round(pending, cfg, ledger, notify)))
            else:
                _log(Path(pending.get("jsonl", OUT / "empjp_no_trade.jsonl")), {"kind": "round_no_trade", **pending})
                notify("skip", pending)
        except Exception as exc:
            print(f"[ROUND {i}] error: {exc}", flush=True)
        await drain(done_only=True)
        if i < rounds:
            now = int(time.time())
            wait = 3 if now % 300 < 60 else 301 - (now % 300)
            print(f"[EMPJP] round {i} done; waiting {wait}s", flush=True)
            await asyncio.sleep(wait)
    print(f"[EMPJP] trading phases done; waiting for {len(resolve_tasks)} resolution tasks", flush=True)
    await drain(done_only=False)
    print(f"[EMPJP] done resolved={ledger.resolved_count} pnl={ledger.total_pnl:+.4f}U", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="EMPJP e75 n30 c1 l1 BTC 5m Polymarket live/shadow bot")
    parser.add_argument("--rounds", type=int, default=_envi("EMPJP_ROUNDS", _envi("CORR_ROUNDS", 1000000)))
    parser.add_argument("--start-mode", choices=("current", "next"), default=os.getenv("EMPJP_START_MODE", os.getenv("CORR_START_MODE", "next")))
    parser.add_argument("--calibration", type=Path, default=Path(os.getenv("EMPJP_CALIBRATION_PATH", str(DEFAULT_CALIBRATION_PATH))))
    args = parser.parse_args()
    cfg = EmpJPConfig(
        quantity=_envf("EMPJP_QTY", _envf("CORR_COMBO_QTY", 5.0)),
        edge_min=_envf("EMPJP_EDGE_MIN", 0.075),
        min_cell_n=_envi("EMPJP_MIN_CELL_N", 30),
        confirm_s=_envi("EMPJP_CONFIRM_S", 1),
        latency_s=_envi("EMPJP_LATENCY_S", 1),
        min_entry_elapsed_s=_envf("EMPJP_MIN_ENTRY_ELAPSED_S", 45.0),
        max_entry_elapsed_s=_envf("EMPJP_MAX_ENTRY_ELAPSED_S", 255.0),
        min_tte_s=_envf("EMPJP_MIN_TTE_S", 45.0),
        max_tte_s=_envf("EMPJP_MAX_TTE_S", 240.0),
        min_ask=_envf("EMPJP_MIN_ASK", 0.18),
        max_ask=_envf("EMPJP_MAX_ASK", 0.82),
        max_spread=_envf("EMPJP_MAX_SPREAD", 0.05),
        min_depth=_envf("EMPJP_MIN_DEPTH", 5.0),
    )
    exec_gates = GateConfig(
        combo_qty=cfg.quantity,
        weekend_rest_enabled=_envb("EMPJP_WEEKEND_REST_ENABLED", False),
        pm_resolution_wait_s=_envf("EMPJP_PM_RESOLUTION_WAIT_S", 1200.0),
        pm_resolution_poll_s=_envf("EMPJP_PM_RESOLUTION_POLL_S", 15.0),
        pm_resolution_retry_s=_envf("EMPJP_PM_RESOLUTION_RETRY_S", 60.0),
        leg_mismatch_tolerance_shares=_envf("EMPJP_LEG_MISMATCH_TOLERANCE_SHARES", 0.5),
        exec_slippage_ticks=_envi("EMPJP_EXEC_SLIPPAGE_TICKS", _envi("CORR_EXEC_SLIPPAGE_TICKS", 2)),
        exec_chase_slippage_ticks=_envi("EMPJP_EXEC_CHASE_SLIPPAGE_TICKS", _envi("CORR_EXEC_CHASE_SLIPPAGE_TICKS", 1)),
        exec_max_chase_attempts=_envi("EMPJP_EXEC_MAX_CHASE_ATTEMPTS", 0),
        exec_user_ws_enabled=_envb("EMPJP_USER_WS_ENABLED", _envb("CORR_USER_WS_ENABLED", False)),
        exec_user_ws_confirm_timeout_s=_envf("EMPJP_USER_WS_CONFIRM_TIMEOUT_S", _envf("CORR_USER_WS_CONFIRM_TIMEOUT_S", 8.0)),
    )
    asyncio.run(main_async(args.rounds, args.start_mode, cfg, exec_gates, args.calibration))


if __name__ == "__main__":
    main()
