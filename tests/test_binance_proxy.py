import json

import pytest

from aftertake.binance_proxy import BinanceFiveMinuteProxy


def _trade(symbol, timestamp_ms, price):
    return json.dumps(
        {
            "stream": symbol.lower() + "@aggTrade",
            "data": {"e": "aggTrade", "s": symbol, "T": timestamp_ms, "p": str(price)},
        }
    )


def test_proxy_builds_complete_spot_tape_and_compatibility_ohlc():
    clock_value = [900.0]
    proxy = BinanceFiveMinuteProxy(("XRP",), clock=lambda: clock_value[0])
    assert proxy._url.startswith("wss://stream.binance.com:9443/stream?")
    assert "@aggTrade" in proxy._url
    proxy._on_open(None)
    proxy._on_message(None, _trade("XRPUSDT", 900_001, 1.0))
    clock_value[0] = 1_199.9
    proxy._on_message(None, _trade("XRPUSDT", 1_199_900, 1.002))

    tape = proxy.tail_input("XRP", 900)
    assert tape.complete_coverage
    assert len(tape.trades) == 2
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
    assert not proxy.tail_input("XRP", 900).complete_coverage

    clock_value[0] = 1_201.0
    proxy._on_close(None)
    assert not proxy.tail_input("XRP", 1200).complete_coverage
