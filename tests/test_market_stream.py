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
