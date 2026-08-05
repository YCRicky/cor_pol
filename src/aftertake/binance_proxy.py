"""Memory-bounded Binance Spot 5-minute OHLC proxy."""

from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass
from typing import Dict, Iterable, Optional, Tuple

import websocket


WINDOW_MS = 300_000
DEFAULT_SYMBOLS = {
    "BTC": "btcusdt",
    "ETH": "ethusdt",
    "XRP": "xrpusdt",
    "HYPE": "hypeusdt",
    "DOGE": "dogeusdt",
    "SOL": "solusdt",
}


@dataclass(frozen=True)
class BinanceProxySignal:
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
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    first_trade_ms: int
    last_trade_ms: int
    complete_coverage: bool


class BinanceFiveMinuteProxy:
    """Aggregate public Spot trades into exact UTC five-minute buckets."""

    def __init__(self, assets: Iterable[str], *, clock=time.time):
        self._clock = clock
        self._symbols = {
            DEFAULT_SYMBOLS[asset]: asset
            for asset in (str(value).upper() for value in assets)
            if asset in DEFAULT_SYMBOLS
        }
        streams = "/".join(f"{symbol}@trade" for symbol in self._symbols)
        self._url = f"wss://stream.binance.com:9443/stream?streams={streams}"
        self._lock = threading.RLock()
        self._candles: Dict[Tuple[str, int], _Candle] = {}
        self._invalid: set[Tuple[str, int]] = set()
        self._connected_since_ms: Optional[int] = None
        self._last_cleanup_ms = 0
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._ws = None

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True, name="binance-5m-proxy")
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

    def _on_message(self, _ws, raw: str) -> None:
        try:
            payload = json.loads(raw)
            data = payload.get("data", payload)
            if data.get("e") != "trade":
                return
            symbol = str(data["s"]).lower()
            asset = self._symbols.get(symbol)
            if not asset:
                return
            trade_ms = int(data["T"])
            price = float(data["p"])
            start_ms = (trade_ms // WINDOW_MS) * WINDOW_MS
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            return
        key = (asset, start_ms)
        with self._lock:
            connected = self._connected_since_ms
            candle = self._candles.get(key)
            if candle is None:
                candle = _Candle(
                    price,
                    price,
                    price,
                    price,
                    trade_ms,
                    trade_ms,
                    connected is not None and connected <= start_ms and key not in self._invalid,
                )
                self._candles[key] = candle
            else:
                candle.high_price = max(candle.high_price, price)
                candle.low_price = min(candle.low_price, price)
                if trade_ms < candle.first_trade_ms:
                    candle.first_trade_ms = trade_ms
                    candle.open_price = price
                if trade_ms >= candle.last_trade_ms:
                    candle.last_trade_ms = trade_ms
                    candle.close_price = price
            self._cleanup_locked(trade_ms)

    def _on_close(self, _ws, *_args) -> None:
        now_ms = int(self._clock() * 1000)
        active_start = (now_ms // WINDOW_MS) * WINDOW_MS
        with self._lock:
            for asset in self._symbols.values():
                self._invalid.add((asset, active_start))
                candle = self._candles.get((asset, active_start))
                if candle is not None:
                    candle.complete_coverage = False
            self._connected_since_ms = None

    def _cleanup_locked(self, now_ms: int) -> None:
        if now_ms - self._last_cleanup_ms < 3_600_000:
            return
        cutoff = now_ms - 7_200_000
        self._candles = {key: value for key, value in self._candles.items() if key[1] >= cutoff}
        self._invalid = {key for key in self._invalid if key[1] >= cutoff}
        self._last_cleanup_ms = now_ms

    def signal(self, asset: str, round_start: int) -> Optional[BinanceProxySignal]:
        key = (str(asset).upper(), int(round_start) * 1000)
        with self._lock:
            candle = self._candles.get(key)
            if candle is None or not candle.complete_coverage or key in self._invalid:
                return None
            values = (
                candle.open_price,
                candle.high_price,
                candle.low_price,
                candle.close_price,
            )
        if values[0] <= 0:
            return None
        change = (values[3] - values[0]) / values[0]
        side = "YES" if change > 0 else "NO" if change < 0 else ""
        return BinanceProxySignal(key[0], int(round_start), *values, change, side)

    def _run(self) -> None:
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
            if not self._stop.wait(1.0):
                continue
            break
