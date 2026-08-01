from __future__ import annotations

import datetime as dt
import json
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def append_jsonl(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    row = {"ts": utc_now_iso(), **data}
    # JSONL is a human-readable mirror; SQLite FULL-sync transactions are the
    # authoritative durable state.  Forcing an fsync on every close-time book
    # decision can stall the strategy thread and provides no additional order
    # safety, so the mirror is flushed to Python/OS only.
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")
        f.flush()


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows = []
    if not path.exists():
        return rows
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return rows


@dataclass
class LedgerSnapshot:
    """Read-only reconstruction of PM-confirmed settlement rows."""

    trades: int = 0
    wins: int = 0
    total_pnl: float = 0.0
    rejected_settlements: int = 0
    settled_trade_ids: set[str] = field(default_factory=set)
    pending_trade_ids: set[str] = field(default_factory=set)

    @property
    def win_rate(self) -> float:
        return self.wins / self.trades if self.trades else 0.0


def _trade_id(row: dict[str, Any]) -> str:
    if row.get("trade_id"):
        return str(row["trade_id"])
    return "|".join(
        [
            str(row.get("slug") or ""),
            str(row.get("side") or ""),
            str(row.get("entry_price") or row.get("price") or ""),
            str(row.get("qty") or ""),
        ]
    )


def rebuild_ledger(files: Iterable[Path]) -> LedgerSnapshot:
    """Rebuild a human-facing ledger without overriding SQLite execution state."""

    snapshot = LedgerSnapshot()
    trade_opens: dict[str, dict[str, Any]] = {}
    for path in files:
        for row in read_jsonl(path):
            kind = row.get("kind")
            if kind == "trade_open":
                trade_opens[_trade_id(row)] = row
                continue
            if kind != "settle":
                continue
            if row.get("settlement_source") != "pm":
                snapshot.rejected_settlements += 1
                continue
            trade_id = _trade_id(row)
            if trade_id in snapshot.settled_trade_ids:
                continue
            snapshot.settled_trade_ids.add(trade_id)
            snapshot.trades += 1
            snapshot.wins += int(bool(row.get("win")))
            snapshot.total_pnl += float(row.get("pnl") or 0.0)

    snapshot.pending_trade_ids = set(trade_opens) - snapshot.settled_trade_ids
    return snapshot
