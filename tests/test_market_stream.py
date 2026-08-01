import sys
import threading
import time
from types import SimpleNamespace

import pytest

import aftertake.market_stream as market_stream_module
from aftertake.market_stream import MarketBookStream


def _book(token_id, bids, asks, timestamp="1000000"):
    return {
        "type": "book",
        "payload": {"tokenId": token_id, "bids": bids, "asks": asks, "timestamp": timestamp},
    }


def test_market_stream_builds_a_paired_book_from_official_book_and_price_change_messages():
    snapshots = []
    stream = MarketBookStream(
        yes_token_id="yes-token", no_token_id="no-token", on_book=snapshots.append, clock=lambda: 10.0
    )
    stream.process_message(
        _book("yes-token", [{"price": "0.48", "size": "20"}], [{"price": "0.50", "size": "30"}]),
        received_at=1_000.10,
    )
    assert snapshots == []

    stream.process_message(
        _book("no-token", [{"price": "0.50", "size": "25"}], [{"price": "0.52", "size": "20"}]),
        received_at=1_000.11,
    )

    assert stream.ready is True
    assert len(snapshots) == 1
    assert snapshots[-1].yes.best_bid == 0.48
    assert snapshots[-1].no.best_ask == 0.52
    assert snapshots[-1].yes_updated_at == 1_000.10
    assert snapshots[-1].no_updated_at == 1_000.11

    stream.process_message(
        {
            "type": "price_change",
            "payload": {
                "timestamp": "1780000250000",
                "priceChanges": [
                    {"tokenId": "no-token", "price": "0.84", "size": "15", "side": "BUY"}
                ],
            },
        },
        received_at=1_000.25,
    )

    assert len(snapshots) == 2
    assert snapshots[-1].no.best_bid == 0.84
    assert snapshots[-1].no.bid_depth == 40.0
    assert snapshots[-1].no.near_touch_bid_depth == 15.0
    assert snapshots[-1].source_timestamp == 1_780_000_250.0
    assert snapshots[-1].yes_updated_at == 1_000.10
    assert snapshots[-1].no_updated_at == 1_000.25


def test_market_stream_removes_zero_sized_level_and_accepts_legacy_message_shape():
    snapshots = []
    stream = MarketBookStream(
        yes_token_id="yes-token", no_token_id="no-token", on_book=snapshots.append
    )
    stream.process_message(
        {
            "event_type": "book",
            "asset_id": "yes-token",
            "bids": [{"price": "0.60", "size": "10"}],
            "asks": [{"price": "0.61", "size": "10"}],
        },
        received_at=1_000.10,
    )
    stream.process_message(
        _book("no-token", [{"price": "0.30", "size": "10"}], [{"price": "0.31", "size": "10"}]),
        received_at=1_000.11,
    )
    stream.process_message(
        {
            "event_type": "price_change",
            "asset_id": "yes-token",
            "price_changes": [{"price": "0.60", "size": "0", "side": "BUY"}],
        },
        received_at=1_000.25,
    )

    assert snapshots[-1].yes.best_bid is None


def test_market_stream_uses_the_official_hostname_without_a_hard_coded_ip_fallback():
    stream = MarketBookStream(
        yes_token_id="yes-token", no_token_id="no-token", on_book=lambda _snapshot: None
    )

    assert stream._url == "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    assert not hasattr(stream, "_resolve_ip")


def test_market_stream_allows_a_realistic_handshake_then_restores_fast_receive_timeout(monkeypatch):
    stream = MarketBookStream(
        yes_token_id="yes-token", no_token_id="no-token", on_book=lambda _snapshot: None
    )
    calls = {"connect_timeout": None, "receive_timeout": None}

    class Timeout(Exception):
        pass

    class Socket:
        def settimeout(self, timeout):
            calls["receive_timeout"] = timeout

        def send(self, _payload):
            return None

        def recv(self):
            stream._stop.set()
            raise Timeout("stop after checking timeouts")

        def close(self):
            return None

    def create_connection(_url, timeout):
        calls["connect_timeout"] = timeout
        return Socket()

    monkeypatch.setitem(
        sys.modules,
        "websocket",
        SimpleNamespace(create_connection=create_connection, WebSocketTimeoutException=Timeout),
    )

    stream._run()

    assert calls == {"connect_timeout": 2.0, "receive_timeout": 0.05}


def test_market_stream_reconnects_after_a_silent_socket_heartbeat_timeout(monkeypatch):
    stream = MarketBookStream(
        yes_token_id="yes-token", no_token_id="no-token", on_book=lambda _snapshot: None
    )
    observed = []

    class Timeout(Exception):
        pass

    class Socket:
        def settimeout(self, _timeout):
            return None

        def send(self, payload):
            observed.append(payload)

        def recv(self):
            raise Timeout("receive timeout")

        def close(self):
            return None

    def create_connection(_url, timeout):
        assert timeout == 2.0
        return Socket()

    # Non-zero intervals are important here: a buggy implementation that
    # renews the deadline on every ping would otherwise appear healthy when
    # both values are zero.
    monkeypatch.setattr(market_stream_module, "PING_INTERVAL_S", 0.01)
    monkeypatch.setattr(market_stream_module, "PONG_TIMEOUT_S", 0.03)
    monkeypatch.setitem(
        sys.modules,
        "websocket",
        SimpleNamespace(create_connection=create_connection, WebSocketTimeoutException=Timeout),
    )

    stream.start()
    deadline = time.monotonic() + 1.5
    while stream.generation < 2 and time.monotonic() < deadline:
        time.sleep(0.01)
    stream.close(timeout_s=1.0)

    assert "market stream heartbeat timeout" in stream.last_error
    assert stream.reconnect_count >= 1
    assert stream.generation >= 2
    assert "PING" in observed


def test_market_stream_invalidates_ready_book_before_reconnect_backoff(monkeypatch):
    """A disconnected feed must stop advertising its old paired book immediately."""

    allow_disconnect = threading.Event()
    stream = MarketBookStream(
        yes_token_id="yes-token", no_token_id="no-token", on_book=lambda _snapshot: None
    )

    class Timeout(Exception):
        pass

    class Disconnect(Exception):
        pass

    class Socket:
        def __init__(self):
            self._messages = [
                _book(
                    "yes-token",
                    [{"price": "0.60", "size": "10"}],
                    [{"price": "0.61", "size": "10"}],
                ),
                _book(
                    "no-token",
                    [{"price": "0.30", "size": "10"}],
                    [{"price": "0.31", "size": "10"}],
                ),
            ]

        def settimeout(self, _timeout):
            return None

        def send(self, _payload):
            return None

        def recv(self):
            if self._messages:
                return self._messages.pop(0)
            assert allow_disconnect.wait(timeout=1.0)
            raise Disconnect("injected connection reset")

        def close(self):
            return None

    monkeypatch.setattr(market_stream_module, "RECONNECT_INITIAL_S", 5.0)
    monkeypatch.setitem(
        sys.modules,
        "websocket",
        SimpleNamespace(
            create_connection=lambda _url, timeout: Socket(),
            WebSocketTimeoutException=Timeout,
        ),
    )

    stream.start()
    try:
        ready_deadline = time.monotonic() + 1.0
        while not stream.ready and time.monotonic() < ready_deadline:
            time.sleep(0.005)
        assert stream.ready is True
        connected_generation = stream.generation

        allow_disconnect.set()
        disconnect_deadline = time.monotonic() + 1.0
        while stream.reconnect_count < 1 and time.monotonic() < disconnect_deadline:
            time.sleep(0.005)

        assert stream.reconnect_count == 1
        assert stream.ready is False
        assert stream.generation > connected_generation
    finally:
        stream.close(timeout_s=1.0)


def test_market_stream_watchdog_interrupts_recv_that_ignores_socket_timeout(monkeypatch):
    release_recv = threading.Event()
    close_called = threading.Event()
    stream = MarketBookStream(
        yes_token_id="yes-token",
        no_token_id="no-token",
        on_book=lambda _snapshot: None,
    )

    class Timeout(Exception):
        pass

    class Socket:
        def __init__(self):
            self.messages = [
                _book("yes-token", [{"price": "0.60", "size": "5"}], [{"price": "0.61", "size": "5"}]),
                _book("no-token", [{"price": "0.30", "size": "5"}], [{"price": "0.31", "size": "5"}]),
            ]

        def settimeout(self, _timeout):
            return None

        def send(self, _payload):
            return None

        def recv(self):
            if self.messages:
                return self.messages.pop(0)
            release_recv.wait(timeout=2.0)
            return None

        def close(self):
            close_called.set()

    socket = Socket()
    monkeypatch.setitem(
        sys.modules,
        "websocket",
        SimpleNamespace(
            create_connection=lambda _url, timeout: socket,
            WebSocketTimeoutException=Timeout,
        ),
    )

    stream.start()
    try:
        deadline = time.monotonic() + 1.0
        while not stream.ready and time.monotonic() < deadline:
            time.sleep(0.005)
        assert stream.ready is True
        stream.arm_market_data_watchdog(0.05)
        deadline = time.monotonic() + 1.0
        while stream.ready and time.monotonic() < deadline:
            time.sleep(0.005)
        assert stream.ready is False
        assert close_called.wait(0.2)
        with pytest.raises(RuntimeError, match="did not stop"):
            stream.close(timeout_s=0.01)
    finally:
        release_recv.set()
        if stream._thread is not None:
            stream._thread.join(timeout=1.0)
        stream._watchdog_stop.set()
        if stream._watchdog_thread is not None:
            stream._watchdog_thread.join(timeout=1.0)


def test_market_stream_counts_only_near_touch_bid_depth():
    snapshots = []
    stream = MarketBookStream(
        yes_token_id="yes-token",
        no_token_id="no-token",
        on_book=snapshots.append,
        near_touch_band=0.02,
    )
    stream.process_message(
        _book(
            "yes-token",
            [{"price": "0.60", "size": "3"}, {"price": "0.59", "size": "4"}, {"price": "0.40", "size": "1000"}],
            [{"price": "0.62", "size": "10"}],
        ),
        received_at=1_000.10,
    )
    stream.process_message(
        _book("no-token", [{"price": "0.30", "size": "10"}], [{"price": "0.32", "size": "10"}]),
        received_at=1_000.11,
    )

    assert snapshots[-1].yes.bid_depth == 1007.0
    assert snapshots[-1].yes.near_touch_bid_depth == 7.0
