"""Durable local state for safe order submission and recovery.

JSONL is useful as an audit export, but it cannot provide an atomic per-market
entry lock.  This store uses SQLite WAL and records an intent *before* an
external submission.  An interrupted or uncertain submission is never retried
for the same slug.  Definite venue rejections may be terminal for that market;
transport ambiguity remains ``execution_unknown`` and is reconciled or
escalated to a process restart rather than silently treated as a no-fill.
"""

from __future__ import annotations

import datetime as dt
import json
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional


def _utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _asset_slug_prefix(slug: str) -> str:
    return str(slug).split("-", 1)[0].lower() + "-%"


class RuntimeLock:
    """Cross-platform non-blocking lock preventing duplicate trading loops."""

    def __init__(self, path: Path):
        self.path = Path(path)
        self._handle = None

    def __enter__(self) -> RuntimeLock:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = self.path.open("a+b")
        self._handle.seek(0, 2)
        if self._handle.tell() == 0:
            self._handle.write(b"0")
            self._handle.flush()
        self._handle.seek(0)
        try:
            if __import__("os").name == "nt":
                import msvcrt

                msvcrt.locking(self._handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(self._handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (OSError, BlockingIOError) as exc:
            self._handle.close()
            self._handle = None
            raise RuntimeError("another aftertake process holds the runtime lock") from exc
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        if self._handle is None:
            return
        try:
            self._handle.seek(0)
            if __import__("os").name == "nt":
                import msvcrt

                msvcrt.locking(self._handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
        finally:
            self._handle.close()
            self._handle = None


@dataclass(frozen=True)
class OrderRecord:
    intent_id: str
    slug: str
    token_id: str
    side: str
    requested_qty: float
    requested_price: float
    requested_notional: float
    order_id: str
    state: str
    filled_qty: float
    avg_price: float
    fill_notional: float
    fee_rate: float
    fee_exponent: float
    builder_taker_fee_bps: float
    error: str
    raw: Dict[str, Any]


class StateStore:
    """A single-process-safe SQLite state machine.

    The database is intentionally conservative: an `entry_reserved` record
    after a crash is treated as potentially submitted rather than retried.
    """

    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._closed = False
        self._conn = sqlite3.connect(
            str(self.path), timeout=15.0, isolation_level=None, check_same_thread=False
        )
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=FULL")
            self._conn.execute("PRAGMA foreign_keys=ON")
            self._create_schema()

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._conn.close()

    @property
    def closed(self) -> bool:
        with self._lock:
            return self._closed

    def _create_schema(self) -> None:
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS markets (
                slug TEXT PRIMARY KEY,
                condition_id TEXT NOT NULL,
                round_start INTEGER NOT NULL,
                state TEXT NOT NULL,
                intent_id TEXT,
                reason TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS orders (
                intent_id TEXT PRIMARY KEY,
                slug TEXT NOT NULL REFERENCES markets(slug),
                token_id TEXT NOT NULL,
                side TEXT NOT NULL,
                requested_qty REAL NOT NULL,
                requested_price REAL NOT NULL,
                requested_notional REAL NOT NULL,
                order_id TEXT NOT NULL DEFAULT '',
                state TEXT NOT NULL,
                filled_qty REAL NOT NULL DEFAULT 0,
                avg_price REAL NOT NULL DEFAULT 0,
                fill_notional REAL NOT NULL DEFAULT 0,
                fee_rate REAL NOT NULL DEFAULT -1,
                fee_exponent REAL NOT NULL DEFAULT 1,
                builder_taker_fee_bps REAL NOT NULL DEFAULT 0,
                error TEXT NOT NULL DEFAULT '',
                raw_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_order_id
                ON orders(order_id) WHERE order_id <> '';
            CREATE INDEX IF NOT EXISTS idx_orders_slug ON orders(slug);

            CREATE TABLE IF NOT EXISTS settlements (
                slug TEXT PRIMARY KEY REFERENCES markets(slug),
                pnl REAL NOT NULL,
                payload_json TEXT NOT NULL,
                settled_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS audit_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                kind TEXT NOT NULL,
                slug TEXT NOT NULL DEFAULT '',
                payload_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS component_health (
                component TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                detail TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS notification_outbox (
                notification_id TEXT PRIMARY KEY,
                component TEXT NOT NULL DEFAULT '',
                kind TEXT NOT NULL,
                slug TEXT NOT NULL DEFAULT '',
                message TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0,
                next_attempt_at REAL NOT NULL DEFAULT 0,
                last_error TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_notification_outbox_pending
                ON notification_outbox(status, next_attempt_at, created_at);
            """
        )
        columns = {
            str(row["name"]) for row in self._conn.execute("PRAGMA table_info(orders)").fetchall()
        }
        if "fee_rate" not in columns:
            self._conn.execute(
                "ALTER TABLE orders ADD COLUMN fee_rate REAL NOT NULL DEFAULT -1"
            )
        if "builder_taker_fee_bps" not in columns:
            self._conn.execute(
                "ALTER TABLE orders ADD COLUMN builder_taker_fee_bps REAL NOT NULL DEFAULT 0"
            )
        if "fee_exponent" not in columns:
            self._conn.execute(
                "ALTER TABLE orders ADD COLUMN fee_exponent REAL NOT NULL DEFAULT 1"
            )

        notification_columns = {
            str(row["name"])
            for row in self._conn.execute("PRAGMA table_info(notification_outbox)").fetchall()
        }
        if "component" not in notification_columns:
            self._conn.execute(
                "ALTER TABLE notification_outbox ADD COLUMN component TEXT NOT NULL DEFAULT ''"
            )
        self._conn.execute(
            """CREATE INDEX IF NOT EXISTS idx_notification_outbox_component
               ON notification_outbox(component, status)"""
        )

    @contextmanager
    def _transaction(self):
        self._begin()
        try:
            yield
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    def _begin(self) -> None:
        self._conn.execute("BEGIN IMMEDIATE")

    def enqueue_notification(
        self,
        kind: str,
        message: str,
        slug: str = "",
        component: str = "",
    ) -> str:
        """Persist an operator notification before Telegram delivery.

        Telegram acknowledgement is not runtime health.  Keeping the message
        in the same FULL-synchronous state database means a transport failure
        or process restart cannot erase a pending error/recovery notification.
        """

        notification_id = uuid.uuid4().hex
        now = _utc_now()
        with self._lock, self._transaction():
            self._conn.execute(
                """INSERT INTO notification_outbox
                   (notification_id, component, kind, slug, message, status, attempts,
                    next_attempt_at, last_error, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, 'pending', 0, 0, '', ?, ?)""",
                (
                    notification_id,
                    str(component),
                    str(kind),
                    str(slug),
                    str(message),
                    now,
                    now,
                ),
            )
        return notification_id

    def transition_component_and_enqueue_notification(
        self,
        *,
        component: str,
        status: str,
        detail: str,
        kind: str,
        message: str,
        slug: str = "",
        enqueue_on_no_transition: bool = False,
    ) -> tuple[bool, str]:
        """Atomically change health and create its operator notification."""

        component = str(component or "").strip()
        if not component:
            raise ValueError("component is required")
        status = str(status or "").strip().lower()
        if status not in {"healthy", "unhealthy"}:
            raise ValueError("component status must be healthy or unhealthy")
        now = _utc_now()
        notification_id = ""
        with self._lock, self._transaction():
            row = self._conn.execute(
                "SELECT status FROM component_health WHERE component = ?", (component,)
            ).fetchone()
            previous = str(row["status"]) if row is not None else ""
            transitioned = previous != status and not (not previous and status == "healthy")
            if status == "unhealthy" and not previous:
                transitioned = True
            self._conn.execute(
                """INSERT INTO component_health (component, status, detail, updated_at)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT(component) DO UPDATE SET
                       status = excluded.status, detail = excluded.detail,
                       updated_at = excluded.updated_at""",
                (component, status, str(detail or "")[:1000], now),
            )
            if transitioned or enqueue_on_no_transition:
                notification_id = uuid.uuid4().hex
                self._conn.execute(
                    """INSERT INTO notification_outbox
                       (notification_id, component, kind, slug, message, status,
                        attempts, next_attempt_at, last_error, created_at, updated_at)
                       VALUES (?, ?, ?, ?, ?, 'pending', 0, 0, '', ?, ?)""",
                    (
                        notification_id,
                        component,
                        str(kind),
                        str(slug),
                        str(message),
                        now,
                        now,
                    ),
                )
        return transitioned, notification_id

    def notification(self, notification_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM notification_outbox WHERE notification_id = ?",
                (str(notification_id),),
            ).fetchone()
        return dict(row) if row is not None else None

    def deliverable_notification(
        self, notification_id: str, *, ready_at: float
    ) -> Optional[Dict[str, Any]]:
        """Return a due row only when no same-component alert precedes it."""

        with self._lock:
            row = self._conn.execute(
                """SELECT current.* FROM notification_outbox AS current
                   WHERE current.notification_id = ?
                     AND current.status = 'pending'
                     AND current.next_attempt_at <= ?
                     AND (
                         current.component = ''
                         OR NOT EXISTS (
                             SELECT 1 FROM notification_outbox AS predecessor
                             WHERE predecessor.component = current.component
                               AND predecessor.status = 'pending'
                               AND predecessor.rowid < current.rowid
                         )
                     )""",
                (str(notification_id), float(ready_at)),
            ).fetchone()
        return dict(row) if row is not None else None

    def pending_notifications(
        self, *, ready_at: float, limit: int = 100
    ) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                """SELECT current.* FROM notification_outbox AS current
                   WHERE current.status = 'pending'
                     AND current.next_attempt_at <= ?
                     AND (
                         current.component = ''
                         OR NOT EXISTS (
                             SELECT 1 FROM notification_outbox AS predecessor
                             WHERE predecessor.component = current.component
                               AND predecessor.status = 'pending'
                               AND predecessor.rowid < current.rowid
                         )
                     )
                   ORDER BY current.rowid LIMIT ?""",
                (float(ready_at), max(1, int(limit))),
            ).fetchall()
        return [dict(row) for row in rows]

    def mark_notification_sent(self, notification_id: str) -> None:
        now = _utc_now()
        with self._lock, self._transaction():
            self._conn.execute(
                """UPDATE notification_outbox
                   SET status = 'sent', attempts = attempts + 1,
                       last_error = '', updated_at = ?
                   WHERE notification_id = ? AND status = 'pending'""",
                (now, str(notification_id)),
            )

    def mark_notification_failed(
        self,
        notification_id: str,
        error: str,
        *,
        retry_at: float,
    ) -> None:
        now = _utc_now()
        with self._lock, self._transaction():
            self._conn.execute(
                """UPDATE notification_outbox
                   SET status = 'pending', attempts = attempts + 1,
                       next_attempt_at = ?, last_error = ?, updated_at = ?
                   WHERE notification_id = ? AND status = 'pending'""",
                (float(retry_at), str(error)[:500], now, str(notification_id)),
            )

    def observe_market(self, slug: str, condition_id: str, round_start: int) -> None:
        now = _utc_now()
        with self._lock:
            self._begin()
            try:
                row = self._conn.execute(
                    "SELECT condition_id FROM markets WHERE slug = ?", (slug,)
                ).fetchone()
                if row is None:
                    self._conn.execute(
                        """INSERT INTO markets
                           (slug, condition_id, round_start, state, created_at, updated_at)
                           VALUES (?, ?, ?, 'observing', ?, ?)""",
                        (slug, condition_id, int(round_start), now, now),
                    )
                elif row["condition_id"] != condition_id:
                    raise RuntimeError("condition ID changed for persisted slug %s" % slug)
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def reserve_entry(
        self,
        slug: str,
        condition_id: str,
        round_start: int,
        token_id: str,
        side: str,
        requested_qty: float,
        requested_price: float,
        fee_rate: float = -1.0,
        fee_exponent: float = 1.0,
        builder_taker_fee_bps: float = 0.0,
    ) -> Optional[OrderRecord]:
        """Atomically reserve the sole entry attempt for a market.

        A rejected/no-fill attempt remains terminal for this market.  This is a
        deliberate no-repeat policy, not a retry queue.
        """

        now = _utc_now()
        intent_id = uuid.uuid4().hex
        requested_notional = float(requested_qty) * float(requested_price)
        with self._lock:
            self._begin()
            try:
                market = self._conn.execute(
                    "SELECT state, condition_id FROM markets WHERE slug = ?", (slug,)
                ).fetchone()
                if market is None:
                    self._conn.execute(
                        """INSERT INTO markets
                           (slug, condition_id, round_start, state, intent_id, created_at, updated_at)
                           VALUES (?, ?, ?, 'entry_reserved', ?, ?, ?)""",
                        (slug, condition_id, int(round_start), intent_id, now, now),
                    )
                else:
                    if market["condition_id"] != condition_id:
                        raise RuntimeError("condition ID changed for persisted slug %s" % slug)
                    if market["state"] != "observing":
                        self._conn.execute("COMMIT")
                        return None
                    self._conn.execute(
                        """UPDATE markets SET state = 'entry_reserved', intent_id = ?,
                           updated_at = ? WHERE slug = ?""",
                        (intent_id, now, slug),
                    )
                self._conn.execute(
                    """INSERT INTO orders
                       (intent_id, slug, token_id, side, requested_qty, requested_price,
                        requested_notional, fee_rate, fee_exponent, builder_taker_fee_bps,
                        state, created_at, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'entry_reserved', ?, ?)""",
                    (
                        intent_id,
                        slug,
                        token_id,
                        side,
                        float(requested_qty),
                        float(requested_price),
                        requested_notional,
                        float(fee_rate),
                        float(fee_exponent),
                        float(builder_taker_fee_bps),
                        now,
                        now,
                    ),
                )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
        return OrderRecord(
            intent_id=intent_id,
            slug=slug,
            token_id=token_id,
            side=side,
            requested_qty=float(requested_qty),
            requested_price=float(requested_price),
            requested_notional=requested_notional,
            order_id="",
            state="entry_reserved",
            filled_qty=0.0,
            avg_price=0.0,
            fill_notional=0.0,
            fee_rate=float(fee_rate),
            fee_exponent=float(fee_exponent),
            builder_taker_fee_bps=float(builder_taker_fee_bps),
            error="",
            raw={},
        )

    def mark_submitted(self, intent_id: str, order_id: str, raw: Dict[str, Any]) -> None:
        if not order_id:
            self.mark_execution_unknown(intent_id, "CLOB acknowledgement missing order ID", raw)
            return
        now = _utc_now()
        with self._lock, self._transaction():
            cursor = self._conn.execute(
                """UPDATE orders SET state = 'submitted', order_id = ?, raw_json = ?, updated_at = ?
                   WHERE intent_id = ? AND state = 'entry_reserved'""",
                (order_id, json.dumps(raw, sort_keys=True, default=str), now, intent_id),
            )
            if cursor.rowcount != 1:
                raise RuntimeError("invalid state transition to submitted")
            self._conn.execute(
                """UPDATE markets SET state = 'submitted', updated_at = ?
                   WHERE intent_id = ? AND state = 'entry_reserved'""",
                (now, intent_id),
            )

    def mark_execution_unknown(
        self, intent_id: str, reason: str, raw: Optional[Dict[str, Any]] = None
    ) -> None:
        now = _utc_now()
        raw_json = json.dumps(raw or {}, sort_keys=True, default=str)
        with self._lock, self._transaction():
            self._conn.execute(
                """UPDATE orders SET state = 'execution_unknown', error = ?, raw_json = ?, updated_at = ?
                   WHERE intent_id = ?""",
                (reason, raw_json, now, intent_id),
            )
            self._conn.execute(
                """UPDATE markets SET state = 'execution_unknown', reason = ?, updated_at = ?
                   WHERE intent_id = ?""",
                (reason, now, intent_id),
            )

    def attach_recovered_order_id(self, intent_id: str, order_id: str) -> None:
        """Attach or replace an ID that must pass strict identity validation.

        Replacement is allowed only after a recovered ID itself failed that
        validation.  An arbitrary submitted/live order can never be redirected
        to a different ID.
        """

        if not order_id.strip():
            raise ValueError("order_id is required")
        now = _utc_now()
        with self._lock, self._transaction():
            current = self._conn.execute(
                """SELECT order_id, raw_json FROM orders
                   WHERE intent_id = ? AND state = 'execution_unknown'""",
                (intent_id,),
            ).fetchone()
            if current is None:
                raise RuntimeError("intent is not an unknown execution")
            raw = json.loads(str(current["raw_json"] or "{}"))
            previous_order_id = str(current["order_id"] or "")
            if previous_order_id and raw.get("classification") != "recovered_order_identity_mismatch":
                raise RuntimeError("existing recovered order ID is not replaceable")
            history = list(raw.get("recovered_order_id_history") or [])
            if previous_order_id and previous_order_id not in history:
                history.append(previous_order_id)
            raw["recovered_order_id_history"] = history
            raw["recovered_order_id"] = order_id.strip()
            raw["requires_identity_validation"] = True
            raw.pop("identity_error", None)
            raw["classification"] = "operator_attached_order_id"
            cursor = self._conn.execute(
                """UPDATE orders SET state = 'submitted', order_id = ?, error = '',
                   raw_json = ?, updated_at = ?
                   WHERE intent_id = ? AND state = 'execution_unknown'""",
                (order_id.strip(), json.dumps(raw, sort_keys=True), now, intent_id),
            )
            if cursor.rowcount != 1:
                raise RuntimeError("intent is not an unknown execution")
            self._conn.execute(
                """UPDATE markets SET state = 'submitted', reason = '', updated_at = ?
                   WHERE intent_id = ? AND state = 'execution_unknown'""",
                (now, intent_id),
            )

    def mark_terminal_execution(
        self,
        intent_id: str,
        filled_qty: float,
        avg_price: float,
        raw: Dict[str, Any],
        reason: str = "",
    ) -> None:
        now = _utc_now()
        filled_qty = max(0.0, float(filled_qty))
        avg_price = max(0.0, float(avg_price))
        fill_notional = filled_qty * avg_price
        market_state = "open" if filled_qty > 0 else "no_fill"
        order_state = "filled" if filled_qty > 0 else "no_fill"
        with self._lock, self._transaction():
            cursor = self._conn.execute(
                """UPDATE orders SET state = ?, filled_qty = ?, avg_price = ?, fill_notional = ?,
                   error = ?, raw_json = ?, updated_at = ?
                   WHERE intent_id = ?
                     AND state IN ('entry_reserved', 'submitted', 'execution_unknown')""",
                (
                    order_state,
                    filled_qty,
                    avg_price,
                    fill_notional,
                    reason,
                    json.dumps(raw, sort_keys=True, default=str),
                    now,
                    intent_id,
                ),
            )
            if cursor.rowcount != 1:
                raise RuntimeError("invalid state transition to terminal execution")
            self._conn.execute(
                """UPDATE markets SET state = ?, reason = ?, updated_at = ? WHERE intent_id = ?""",
                (market_state, reason, now, intent_id),
            )

    def unresolved_orders(self) -> List[OrderRecord]:
        with self._lock:
            rows = self._conn.execute(
                """SELECT o.*, m.state AS market_state FROM orders AS o
                   JOIN markets AS m ON m.slug = o.slug
                   WHERE m.state IN ('entry_reserved', 'submitted', 'execution_unknown')
                   ORDER BY o.created_at"""
            ).fetchall()
        return [self._record_from_row(row) for row in rows]

    def has_execution_unknown(self) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM markets WHERE state = 'execution_unknown' LIMIT 1"
            ).fetchone()
        return row is not None

    def market_exposure(self, slug: str) -> float:
        with self._lock:
            row = self._conn.execute(
                """SELECT COALESCE(SUM(o.fill_notional), 0) AS amount
                   FROM orders AS o JOIN markets AS m ON m.slug = o.slug
                   WHERE o.slug = ? AND m.state = 'open'""",
                (slug,),
            ).fetchone()
        return float(row["amount"] or 0.0)

    def total_open_exposure(self) -> float:
        with self._lock:
            row = self._conn.execute(
                """SELECT COALESCE(SUM(o.fill_notional), 0) AS amount
                   FROM orders AS o JOIN markets AS m ON m.slug = o.slug
                   WHERE m.state = 'open'"""
            ).fetchone()
        return float(row["amount"] or 0.0)

    def market_risk_exposure(self, slug: str) -> float:
        """Worst-case local exposure including unresolved intents."""

        with self._lock:
            row = self._conn.execute(
                """SELECT COALESCE(SUM(
                       CASE WHEN m.state = 'open' THEN o.fill_notional
                            ELSE o.requested_notional END
                   ), 0) AS amount
                   FROM orders AS o JOIN markets AS m ON m.slug = o.slug
                   WHERE o.slug = ?
                     AND m.state IN ('entry_reserved', 'submitted', 'execution_unknown', 'open')""",
                (slug,),
            ).fetchone()
        return float(row["amount"] or 0.0)

    def total_risk_exposure(self) -> float:
        """Worst-case account exposure including unresolved intents."""

        with self._lock:
            row = self._conn.execute(
                """SELECT COALESCE(SUM(
                       CASE WHEN m.state = 'open' THEN o.fill_notional
                            ELSE o.requested_notional END
                   ), 0) AS amount
                   FROM orders AS o JOIN markets AS m ON m.slug = o.slug
                   WHERE m.state IN ('entry_reserved', 'submitted', 'execution_unknown', 'open')"""
            ).fetchone()
        return float(row["amount"] or 0.0)

    def mark_component_unhealthy(self, component: str, detail: str = "") -> bool:
        """Persist a component failure; return true on a healthy->failed edge."""

        component = str(component or "").strip()
        if not component:
            raise ValueError("component is required")
        now = _utc_now()
        with self._lock, self._transaction():
            row = self._conn.execute(
                "SELECT status FROM component_health WHERE component = ?", (component,)
            ).fetchone()
            transitioned = row is None or str(row["status"]) != "unhealthy"
            self._conn.execute(
                """INSERT INTO component_health (component, status, detail, updated_at)
                   VALUES (?, 'unhealthy', ?, ?)
                   ON CONFLICT(component) DO UPDATE SET
                       status = 'unhealthy', detail = excluded.detail,
                       updated_at = excluded.updated_at""",
                (component, str(detail or "")[:1000], now),
            )
        return transitioned

    def mark_component_healthy(self, component: str, detail: str = "") -> bool:
        """Persist healthy state; return true only when recovering a failure."""

        component = str(component or "").strip()
        if not component:
            raise ValueError("component is required")
        now = _utc_now()
        with self._lock, self._transaction():
            row = self._conn.execute(
                "SELECT status FROM component_health WHERE component = ?", (component,)
            ).fetchone()
            recovered = row is not None and str(row["status"]) == "unhealthy"
            self._conn.execute(
                """INSERT INTO component_health (component, status, detail, updated_at)
                   VALUES (?, 'healthy', ?, ?)
                   ON CONFLICT(component) DO UPDATE SET
                       status = 'healthy', detail = excluded.detail,
                       updated_at = excluded.updated_at""",
                (component, str(detail or "")[:1000], now),
            )
        return recovered

    def component_status(self, component: str) -> Optional[str]:
        with self._lock:
            row = self._conn.execute(
                "SELECT status FROM component_health WHERE component = ?", (str(component),)
            ).fetchone()
        return str(row["status"]) if row is not None else None

    def market_state(self, slug: str) -> Optional[str]:
        with self._lock:
            row = self._conn.execute(
                "SELECT state FROM markets WHERE slug = ?", (slug,)
            ).fetchone()
        return str(row["state"]) if row is not None else None

    def open_positions(self) -> List[OrderRecord]:
        with self._lock:
            rows = self._conn.execute(
                """SELECT o.* FROM orders AS o
                   JOIN markets AS m ON m.slug = o.slug
                   WHERE m.state = 'open' AND o.filled_qty > 0
                   ORDER BY o.created_at"""
            ).fetchall()
        return [self._record_from_row(row) for row in rows]

    def open_positions_for_asset(self, slug: str) -> List[OrderRecord]:
        """Return open positions sharing the same asset slug prefix."""

        with self._lock:
            rows = self._conn.execute(
                """SELECT o.* FROM orders AS o
                   JOIN markets AS m ON m.slug = o.slug
                   WHERE m.state = 'open' AND o.filled_qty > 0 AND o.slug LIKE ?
                   ORDER BY o.created_at""",
                (_asset_slug_prefix(slug),),
            ).fetchall()
        return [self._record_from_row(row) for row in rows]

    def last_entry_timestamp(self) -> Optional[float]:
        """Return the most recent persisted strategy-entry timestamp."""

        return self.last_entry_timestamp_for_asset("")

    def last_entry_timestamp_for_asset(self, slug: str) -> Optional[float]:
        """Return the most recent persisted entry timestamp for one asset prefix.

        Empty slug keeps the legacy global query for status/debug callers.
        """

        with self._lock:
            if slug:
                row = self._conn.execute(
                    "SELECT created_at FROM orders WHERE slug LIKE ? ORDER BY created_at DESC LIMIT 1",
                    (_asset_slug_prefix(slug),),
                ).fetchone()
            else:
                row = self._conn.execute(
                    "SELECT created_at FROM orders ORDER BY created_at DESC LIMIT 1"
                ).fetchone()
        if row is None:
            return None
        try:
            return dt.datetime.fromisoformat(str(row["created_at"])).timestamp()
        except ValueError:
            return None

    def consecutive_losses(self) -> int:
        """Count the current loss streak from persisted official settlements."""

        with self._lock:
            rows = self._conn.execute(
                "SELECT pnl, payload_json FROM settlements ORDER BY settled_at DESC"
            ).fetchall()
        losses = 0
        for row in rows:
            try:
                payload = json.loads(str(row["payload_json"] or "{}"))
                won = bool(payload["win"])
            except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                won = float(row["pnl"]) >= 0
            if won:
                break
            losses += 1
        return losses

    def daily_realized_loss(self, day: Optional[dt.date] = None) -> float:
        target = (day or dt.datetime.now(dt.timezone.utc).date()).isoformat()
        with self._lock:
            row = self._conn.execute(
                """SELECT COALESCE(SUM(CASE WHEN pnl < 0 THEN -pnl ELSE 0 END), 0) AS loss
                   FROM settlements WHERE substr(settled_at, 1, 10) = ?""",
                (target,),
            ).fetchone()
        return float(row["loss"] or 0.0)

    def record_settlement(self, slug: str, pnl: float, payload: Dict[str, Any]) -> None:
        now = _utc_now()
        payload_json = json.dumps(payload, sort_keys=True, default=str)
        with self._lock, self._transaction():
            existing = self._conn.execute(
                "SELECT pnl, payload_json FROM settlements WHERE slug = ?", (slug,)
            ).fetchone()
            if existing is not None:
                if float(existing["pnl"]) != float(pnl) or existing["payload_json"] != payload_json:
                    raise RuntimeError("conflicting settlement already recorded")
                return
            market = self._conn.execute(
                "SELECT state FROM markets WHERE slug = ?", (slug,)
            ).fetchone()
            if market is None or market["state"] != "open":
                raise RuntimeError("only an open confirmed position can be settled")
            self._conn.execute(
                """INSERT INTO settlements(slug, pnl, payload_json, settled_at)
                   VALUES (?, ?, ?, ?)""",
                (slug, float(pnl), payload_json, now),
            )
            self._conn.execute(
                "UPDATE markets SET state = 'settled', updated_at = ? WHERE slug = ?",
                (now, slug),
            )

    def append_event(self, kind: str, payload: Dict[str, Any], slug: str = "") -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO audit_events(ts, kind, slug, payload_json) VALUES (?, ?, ?, ?)",
                (_utc_now(), kind, slug, json.dumps(payload, sort_keys=True, default=str)),
            )

    @staticmethod
    def _record_from_row(row: sqlite3.Row) -> OrderRecord:
        return OrderRecord(
            intent_id=str(row["intent_id"]),
            slug=str(row["slug"]),
            token_id=str(row["token_id"]),
            side=str(row["side"]),
            requested_qty=float(row["requested_qty"]),
            requested_price=float(row["requested_price"]),
            requested_notional=float(row["requested_notional"]),
            order_id=str(row["order_id"] or ""),
            state=str(row["state"]),
            filled_qty=float(row["filled_qty"]),
            avg_price=float(row["avg_price"]),
            fill_notional=float(row["fill_notional"]),
            fee_rate=float(row["fee_rate"]),
            fee_exponent=float(row["fee_exponent"]),
            builder_taker_fee_bps=float(row["builder_taker_fee_bps"]),
            error=str(row["error"] or ""),
            raw=json.loads(str(row["raw_json"] or "{}")),
        )
