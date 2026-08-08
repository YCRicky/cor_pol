"""Memory-bounded Binance Spot tape for the replay-parity TWAP tail filter.

Binance is not Polymarket's settlement oracle.  This collector exists only to
provide the causal Spot kline-open and aggregate-trade path used by the
recorded tail-risk filter.
"""

from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, Iterable, Optional, Tuple

from .twap_tail import BinanceTailInput, BinanceTrade


WINDOW_MS = 300_000
MAX_TRADES_PER_CANDLE = 32_768
DEFAULT_SYMBOLS = {
    "BTC": "btcusdt",
    "ETH": "ethusdt",
    "SOL": "solusdt",
    "XRP": "xrpusdt",
    "BNB": "bnbusdt",
    "DOGE": "dogeusdt",
}


@dataclass(frozen=True)
class BinanceProxySignal:
    """Backward-compatible OHLC summary for offline callers only."""

    asset: str
    round_start: int
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    change_fraction: float
    side: str


@dataclass
class _Candle:
    complete_coverage: bool
    first_trade_ms: Optional[int] = None
    last_trade_ms: Optional[int] = None
    trades: list[BinanceTrade] = field(default_factory=list)
    invalid_reason: str = ""
    kline_open_price: Optional[float] = None
    kline_open_received_ms: Optional[int] = None


class BinanceFiveMinuteProxy:
    """Collect causal UTC five-minute Binance Spot tapes and kline opens."""

    def __init__(self, assets: Iterable[str], *, clock=time.time):
        self._clock = clock
        self._symbols = {
            DEFAULT_SYMBOLS[asset]: asset
            for asset in (str(value).upper() for value in assets)
            if asset in DEFAULT_SYMBOLS
        }
        streams = "/".join(
            stream
            for symbol in self._symbols
            for stream in (f"{symbol}@aggTrade", f"{symbol}@kline_5m")
        )
        self._url = f"wss://stream.binance.com:9443/stream?streams={streams}"
        self._lock = threading.RLock()
        self._candles: Dict[Tuple[str, int], _Candle] = {}
        self._invalid: Dict[Tuple[str, int], str] = {}
        self._connected_since_ms: Optional[int] = None
        self._last_cleanup_ms = 0
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._ws = None

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True, name="binance-spot-tail")
        self._thread.start()

    def close(self) -> None:
        self._stop.set()
        ws = self._ws
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass

    def _on_open(self, _ws) -> None:
        with self._lock:
            self._connected_since_ms = int(self._clock() * 1000)

    def _mark_invalid_locked(self, key: Tuple[str, int], reason: str) -> None:
        self._invalid[key] = reason
        candle = self._candles.get(key)
        if candle is not None:
            candle.complete_coverage = False
            candle.invalid_reason = reason

    def _candle_locked(self, key: Tuple[str, int]) -> _Candle:
        candle = self._candles.get(key)
        if candle is None:
            connected = self._connected_since_ms
            candle = _Candle(
                complete_coverage=bool(connected is not None and connected <= key[1] and key not in self._invalid),
                invalid_reason=self._invalid.get(key, ""),
            )
            self._candles[key] = candle
        return candle

    def _on_message(self, _ws, raw: str) -> None:
        try:
            payload = json.loads(raw)
            data = payload.get("data", payload)
            event = data.get("e")
            symbol = str(data["s"]).lower()
            asset = self._symbols.get(symbol)
            if not asset:
                return
            received_ms = int(self._clock() * 1000)
            if event == "aggTrade":
                trade_ms = int(data["T"])
                price = float(data["p"])
                if price <= 0:
                    return
                start_ms = (trade_ms // WINDOW_MS) * WINDOW_MS
                kind = "trade"
            elif event == "kline":
                kline = data["k"]
                start_ms = int(kline["t"])
                trade_ms = start_ms
                price = float(kline["o"])
                if price <= 0:
                    return
                kind = "kline"
            else:
                return
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            return

        key = (asset, start_ms)
        with self._lock:
            candle = self._candle_locked(key)
            if kind == "kline":
                candle.kline_open_price = price
                candle.kline_open_received_ms = received_ms
                self._cleanup_locked(received_ms)
                return
            if len(candle.trades) >= MAX_TRADES_PER_CANDLE:
                self._mark_invalid_locked(key, "trade_buffer_overflow")
                self._cleanup_locked(trade_ms)
                return
            candle.trades.append(BinanceTrade(trade_ms=trade_ms, received_ms=received_ms, price=price))
            candle.first_trade_ms = trade_ms if candle.first_trade_ms is None else min(candle.first_trade_ms, trade_ms)
            candle.last_trade_ms = trade_ms if candle.last_trade_ms is None else max(candle.last_trade_ms, trade_ms)
            self._cleanup_locked(trade_ms)

    def _on_close(self, _ws, *_args) -> None:
        now_ms = int(self._clock() * 1000)
        active_start = (now_ms // WINDOW_MS) * WINDOW_MS
        with self._lock:
            for asset in self._symbols.values():
                self._mark_invalid_locked((asset, active_start), "stream_disconnected")
            self._connected_since_ms = None

    def _cleanup_locked(self, now_ms: int) -> None:
        if now_ms - self._last_cleanup_ms < 3_600_000:
            return
        cutoff = now_ms - 7_200_000
        self._candles = {key: value for key, value in self._candles.items() if key[1] >= cutoff}
        self._invalid = {key: value for key, value in self._invalid.items() if key[1] >= cutoff}
        self._last_cleanup_ms = now_ms

    def tail_input(self, asset: str, round_start: int) -> BinanceTailInput:
        """Return a safe immutable tape; missing continuity is explicit."""

        key = (str(asset).upper(), int(round_start) * 1000)
        with self._lock:
            candle = self._candles.get(key)
            invalid_reason = self._invalid.get(key, "")
            if candle is None:
                return BinanceTailInput(key[0], key[1], False, (), invalid_reason or "candle_missing")
            return BinanceTailInput(
                asset=key[0],
                round_start_ms=key[1],
                complete_coverage=bool(candle.complete_coverage and not invalid_reason),
                trades=tuple(candle.trades),
                invalid_reason=invalid_reason or candle.invalid_reason,
                candle_open_price=candle.kline_open_price,
                candle_open_received_ms=candle.kline_open_received_ms,
            )

    def can_cover_candle(self, asset: str, round_start: int) -> bool:
        """Return whether this connection existed before a candle began.

        It is safe to use immediately after a boundary: actual kline/trade
        presence is still checked later by ``tail_input`` at E-10.25s.
        """

        key = (str(asset).upper(), int(round_start) * 1000)
        with self._lock:
            return bool(
                key[0] in self._symbols.values()
                and self._connected_since_ms is not None
                and self._connected_since_ms <= key[1]
                and key not in self._invalid
            )

    def signal(self, asset: str, round_start: int) -> Optional[BinanceProxySignal]:
        """Compatibility OHLC view; live tail execution uses :meth:`tail_input`."""

        tape = self.tail_input(asset, round_start)
        if not tape.complete_coverage or not tape.trades:
            return None
        ordered = sorted(tape.trades, key=lambda trade: (trade.trade_ms, trade.received_ms))
        prices = [trade.price for trade in ordered]
        open_price = tape.candle_open_price if tape.candle_open_price is not None else prices[0]
        close_price = prices[-1]
        if open_price <= 0:
            return None
        change = (close_price - open_price) / open_price
        side = "YES" if change > 0 else "NO" if change < 0 else ""
        return BinanceProxySignal(
            tape.asset,
            int(round_start),
            open_price,
            max(prices),
            min(prices),
            close_price,
            change,
            side,
        )

    def _run(self) -> None:
        try:
            import websocket  # type: ignore
        except ImportError:
            return
        while not self._stop.is_set():
            try:
                self._ws = websocket.WebSocketApp(
                    self._url,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_close=self._on_close,
                    on_error=lambda _ws, _error: None,
                )
                self._ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception:
                pass
            if self._stop.wait(1.0):
                break
