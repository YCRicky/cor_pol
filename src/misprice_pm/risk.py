"""The strategy's original risk controls, backed by durable local state."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional

from .config import Settings
from .state import StateStore


class RiskRejected(RuntimeError):
    """An entry was intentionally refused before any CLOB submission."""


@dataclass(frozen=True)
class RiskSnapshot:
    requested_notional: float
    open_positions: int
    daily_loss: float
    consecutive_losses: int
    seconds_since_last_entry: Optional[float]


def check_entry_risk(
    *,
    settings: Settings,
    store: StateStore,
    slug: str,
    price: float,
    qty: float,
    displayed_ask_size: float,
    now_ts: Optional[float] = None,
) -> RiskSnapshot:
    """Apply only the risk controls that existed in the strategy configuration."""

    del slug  # Market uniqueness is enforced by StateStore.reserve_entry().
    price = float(price)
    qty = float(qty)
    displayed_ask_size = float(displayed_ask_size)
    if price <= 0 or price >= 1 or qty <= 0:
        raise RiskRejected("invalid_entry_size")
    if qty > displayed_ask_size:
        raise RiskRejected("requested_qty_exceeds_displayed_ask_depth")
    if store.has_execution_unknown():
        raise RiskRejected("execution_unknown_requires_manual_reconciliation")

    now = float(time.time() if now_ts is None else now_ts)
    open_positions = len(store.open_positions())
    daily_loss = store.daily_realized_loss()
    consecutive_losses = store.consecutive_losses()
    last_entry = store.last_entry_timestamp()
    elapsed = None if last_entry is None else max(0.0, now - last_entry)

    if open_positions >= settings.max_open_positions:
        raise RiskRejected("max_open_positions")
    if daily_loss >= abs(settings.max_daily_loss):
        raise RiskRejected("daily_loss_limit")
    if consecutive_losses >= settings.max_consecutive_losses:
        raise RiskRejected("consecutive_loss_limit")
    if elapsed is not None and elapsed < settings.min_seconds_between_entries:
        raise RiskRejected("entry_cooldown")

    return RiskSnapshot(
        requested_notional=price * qty,
        open_positions=open_positions,
        daily_loss=daily_loss,
        consecutive_losses=consecutive_losses,
        seconds_since_last_entry=elapsed,
    )
