from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from .ledger import append_jsonl


@dataclass(frozen=True)
class PendingTrade:
    trade_id: str
    slug: str
    side: str
    entry_price: float
    qty: float
    end_ts: int


class MarketClock:
    @staticmethod
    def current_slug(*, now: int, asset: str = "btc") -> tuple[str, int, int]:
        start = int(now) - int(now) % 300
        end = start + 300
        return f"{asset.lower()}-updown-5m-{start}", start, end


def should_poll_settlement(trade: PendingTrade, *, now_ts: float, grace_s: int) -> bool:
    return float(now_ts) >= float(trade.end_ts + grace_s)


class PendingBook:
    def __init__(self, trades: Iterable[PendingTrade] = ()):  # simple in-memory index
        self._trades = {t.trade_id: t for t in trades}

    def add(self, trade: PendingTrade) -> None:
        self._trades[trade.trade_id] = trade

    def remove(self, trade_id: str) -> None:
        self._trades.pop(trade_id, None)

    def all(self) -> list[PendingTrade]:
        return list(self._trades.values())

    def due(self, *, now_ts: float, grace_s: int) -> list[PendingTrade]:
        return [t for t in self.all() if should_poll_settlement(t, now_ts=now_ts, grace_s=grace_s)]


def record_loop_heartbeat(path: Path, *, slug: str, start_ts: int, end_ts: int, now_ts: float) -> None:
    append_jsonl(
        path,
        {
            "kind": "heartbeat",
            "slug": slug,
            "round_start_ts": start_ts,
            "round_end_ts": end_ts,
            "now_ts": now_ts,
        },
    )
