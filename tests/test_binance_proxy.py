import json

import pytest

from aftertake.binance_proxy import BinanceFiveMinuteProxy


def _trade(symbol, timestamp_ms, price):
    return json.dumps(
        {"stream": symbol.lower() + "@trade", "data": {"e": "trade", "s": symbol, "T": timestamp_ms, "p": str(price)}}
    )


def test_proxy_builds_complete_five_minute_ohlc_and_direction():
    clock = lambda: 899.0
    proxy = BinanceFiveMinuteProxy(("XRP",), clock=clock)
    proxy._on_open(None)
    proxy._on_message(None, _trade("XRPUSDT", 900_001, 1.0))
    proxy._on_message(None, _trade("XRPUSDT", 1_199_900, 1.002))

    signal = proxy.signal("XRP", 900)
    assert signal is not None
    assert signal.open_price == pytest.approx(1.0)
    assert signal.close_price == pytest.approx(1.002)
    assert signal.change_fraction == pytest.approx(0.002)
    assert signal.side == "YES"


def test_proxy_rejects_candle_started_before_connection_or_crossed_disconnect():
    clock_value = [905.0]
    proxy = BinanceFiveMinuteProxy(("XRP",), clock=lambda: clock_value[0])
    proxy._on_open(None)
    proxy._on_message(None, _trade("XRPUSDT", 910_000, 1.0))
    assert proxy.signal("XRP", 900) is None

    clock_value[0] = 1201.0
    proxy._on_close(None)
    assert proxy.signal("XRP", 1200) is None
