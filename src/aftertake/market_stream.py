"""Public CLOB market-stream adapter for post-close book classification.

The execution client remains the authenticated CLOB V2 SDK.  This module is
only the public market-data path and never sends an order or account request.
"""

from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, Optional

from .post_close import PairedBook, SideBook
from .resolver import ResolveOverrides, scoped_getaddrinfo

MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
CONNECT_TIMEOUT_S = 2.0
RECEIVE_TIMEOUT_S = 0.25
PING_INTERVAL_S = 5.0
PONG_TIMEOUT_S = 12.0
RECONNECT_INITIAL_S = 0.5
RECONNECT_MAX_S = 10.0
RECONNECT_STABLE_S = 2.0


def _float(value: Any) -> Optional[float]:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _timestamp_s(value: Any) -> Optional[float]:
    timestamp = _float(value)
    if timestamp is None:
        return None
    return timestamp / 1000.0 if timestamp > 10_000_000_000 else timestamp


def _token_id(payload: Dict[str, Any]) -> str:
    for key in ("tokenId", "token_id", "asset_id", "assetId", "asset"):
        value = payload.get(key)
        if value is not None and str(value):
            return str(value)
    return ""


@dataclass
class _TokenBook:
    bids: Dict[float, float] = field(default_factory=dict)
    asks: Dict[float, float] = field(default_factory=dict)
    initialized: bool = False
    updated_at: Optional[float] = None

    @staticmethod
    def _levels(rows: Iterable[Any]) -> Dict[float, float]:
        levels: Dict[float, float] = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            price = _float(row.get("price"))
            size = _float(row.get("size"))
            if price is None or size is None or price <= 0 or size <= 0:
                continue
            levels[price] = size
        return levels

    def replace(self, bids: Iterable[Any], asks: Iterable[Any], *, observed_at: float) -> None:
        self.bids = self._levels(bids)
        self.asks = self._levels(asks)
        self.initialized = True
        self.updated_at = float(observed_at)

    def change(self, side: str, price: Any, size: Any, *, observed_at: float) -> bool:
        parsed_price = _float(price)
        parsed_size = _float(size)
        if parsed_price is None or parsed_price <= 0 or parsed_size is None:
            return False
        target = self.bids if str(side).upper() == "BUY" else self.asks
        if parsed_size <= 0:
            target.pop(parsed_price, None)
        else:
            target[parsed_price] = parsed_size
        self.updated_at = float(observed_at)
        return True

    def as_side_book(self, *, near_touch_band: float = 0.02) -> SideBook:
        best_bid = max(self.bids) if self.bids else None
        best_ask = min(self.asks) if self.asks else None
        bid_size = self.bids.get(best_bid, 0.0) if best_bid is not None else 0.0
        bid_depth = sum(self.bids.values())
        if best_bid is None:
            near_touch_bid_depth = 0.0
        else:
            near_touch_bid_depth = sum(
                size for price, size in self.bids.items() if price >= best_bid - near_touch_band
            )
        ask_size = self.asks.get(best_ask, 0.0) if best_ask is not None else 0.0
        return SideBook(best_bid, bid_size, bid_depth, best_ask, ask_size, near_touch_bid_depth)


class MarketBookStream:
    """Maintain paired CLOB books from the official public market stream.

    ``on_book`` is called on the stream thread.  It must be small and local;
    callers should persist/audit or enqueue work, never perform HTTP I/O from
    the callback.  A reconnect discards the old local book and waits for fresh
    snapshots, preventing stale levels from crossing the close boundary.
    """

    def __init__(
        self,
        *,
        yes_token_id: str,
        no_token_id: str,
        on_book: Callable[[PairedBook], None],
        clock: Callable[[], float] = time.time,
        url: str = MARKET_WS_URL,
        near_touch_band: float = 0.02,
        resolve_overrides: Optional[ResolveOverrides] = None,
    ):
        self.yes_token_id = str(yes_token_id)
        self.no_token_id = str(no_token_id)
        self._on_book = on_book
        self._clock = clock
        self._url = url
        self._near_touch_band = near_touch_band
        self._resolve_overrides = resolve_overrides or {}
        self._books = {
            self.yes_token_id: _TokenBook(),
            self.no_token_id: _TokenBook(),
        }
        self._lock = threading.Lock()
        self._ready = threading.Event()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self.last_error = ""
        self.last_message_at = 0.0
        self.reconnect_count = 0
        self._generation = 0

    @property
    def ready(self) -> bool:
        return self._ready.is_set()

    @property
    def generation(self) -> int:
        """Return the book generation; it changes whenever a reconnect resets state."""

        with self._lock:
            return self._generation

    def start(self) -> None:
        if self._thread is not None:
            raise RuntimeError("market stream is already running")
        self._thread = threading.Thread(target=self._run, daemon=True, name="aftertake-market-stream")
        self._thread.start()

    def close(self, timeout_s: float = 2.0) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=max(0.0, timeout_s))

    def _reset_books(self) -> None:
        with self._lock:
            self._books = {
                self.yes_token_id: _TokenBook(),
                self.no_token_id: _TokenBook(),
            }
            self._ready.clear()
            self._generation += 1

    def _run(self) -> None:
        try:
            import websocket  # type: ignore
        except ImportError:
            self.last_error = "websocket-client dependency is not installed"
            return

        reconnect_delay = RECONNECT_INITIAL_S
        while not self._stop.is_set():
            ws = None
            self._reset_books()
            connection_started = time.monotonic()
            try:
                # Use the official hostname and normal resolver/TLS path.
                # A hard-coded CDN IP can drift and is not an acceptable
                # replacement for the provider's endpoint.
                # TLS/WebSocket handshakes happen well before the close, so
                # permit a realistic connection window.  Restore the short
                # receive timeout immediately afterwards to keep the stream
                # responsive to stop/reconnect requests.
                with scoped_getaddrinfo(self._resolve_overrides):
                    ws = websocket.create_connection(self._url, timeout=CONNECT_TIMEOUT_S)
                ws.settimeout(RECEIVE_TIMEOUT_S)
                ws.send(
                    json.dumps(
                        {
                            "assets_ids": [self.yes_token_id, self.no_token_id],
                            "type": "market",
                        }
                    )
                )
                next_ping = time.monotonic() + PING_INTERVAL_S
                pong_deadline: Optional[float] = None
                while not self._stop.is_set():
                    now = time.monotonic()
                    if pong_deadline is not None and now >= pong_deadline:
                        raise RuntimeError("market stream heartbeat timeout")
                    # Keep one outstanding keepalive deadline. Replacing it
                    # on every 5-second tick would let a silent socket renew
                    # its own deadline forever and defeat the watchdog.
                    if pong_deadline is None and now >= next_ping:
                        ws.send("PING")
                        pong_deadline = now + PONG_TIMEOUT_S
                        next_ping = now + PING_INTERVAL_S
                    try:
                        raw = ws.recv()
                    except websocket.WebSocketTimeoutException:
                        continue
                    if raw in (None, ""):
                        raise RuntimeError("market stream closed")
                    self.last_message_at = float(self._clock())
                    self.last_error = ""
                    # The provider may answer with a text PONG rather than a
                    # WebSocket control frame. Any valid market message also
                    # proves the TCP path is alive, so clear the watchdog.
                    pong_deadline = None
                    if isinstance(raw, str) and raw.upper() == "PONG":
                        continue
                    self.process_message(raw, received_at=float(self._clock()))
                    if time.monotonic() - connection_started >= RECONNECT_STABLE_S:
                        reconnect_delay = RECONNECT_INITIAL_S
            except Exception as exc:
                self.last_error = "%s: %s" % (type(exc).__name__, str(exc))
                self.reconnect_count += 1
                self._stop.wait(reconnect_delay)
                reconnect_delay = min(RECONNECT_MAX_S, reconnect_delay * 2.0)
            finally:
                if ws is not None:
                    try:
                        ws.close()
                    except Exception:
                        pass

    def process_message(self, raw: Any, *, received_at: Optional[float] = None) -> None:
        """Apply one raw official stream message; exposed for deterministic tests."""

        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except json.JSONDecodeError:
                return
        if isinstance(raw, list):
            for item in raw:
                self.process_message(item, received_at=received_at)
            return
        if not isinstance(raw, dict):
            return

        event_type = str(raw.get("type") or raw.get("event_type") or "").lower()
        payload = raw.get("payload") if isinstance(raw.get("payload"), dict) else raw
        if not isinstance(payload, dict):
            return
        timestamp = _timestamp_s(payload.get("timestamp", raw.get("timestamp")))
        observed_at = float(self._clock() if received_at is None else received_at)

        changed = False
        with self._lock:
            if event_type == "book":
                token_id = _token_id(payload)
                if token_id in self._books:
                    self._books[token_id].replace(
                        payload.get("bids") or [],
                        payload.get("asks") or [],
                        observed_at=observed_at,
                    )
                    changed = True
            elif event_type == "price_change":
                default_token = _token_id(payload)
                changes = (
                    payload.get("priceChanges")
                    or payload.get("price_changes")
                    or payload.get("changes")
                    or []
                )
                if isinstance(changes, dict):
                    changes = [changes]
                for change in changes:
                    if not isinstance(change, dict):
                        continue
                    token_id = _token_id(change) or default_token
                    if token_id not in self._books:
                        continue
                    changed = (
                        self._books[token_id].change(
                            str(change.get("side") or ""),
                            change.get("price"),
                            change.get("size"),
                            observed_at=observed_at,
                        )
                        or changed
                    )
            if not changed:
                return
            yes = self._books[self.yes_token_id]
            no = self._books[self.no_token_id]
            if not (yes.initialized and no.initialized):
                return
            self._ready.set()
            snapshot = PairedBook(
                observed_at=observed_at,
                yes=yes.as_side_book(near_touch_band=self._near_touch_band),
                no=no.as_side_book(near_touch_band=self._near_touch_band),
                source_timestamp=timestamp,
                yes_updated_at=yes.updated_at,
                no_updated_at=no.updated_at,
            )
        self._on_book(snapshot)
