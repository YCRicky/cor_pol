from misprice_pm.engine import MarketClock, PendingTrade, should_poll_settlement


def test_market_clock_rolls_to_five_minute_slug():
    slug, start, end = MarketClock.current_slug(now=1784518299)
    assert slug == "btc-updown-5m-1784518200"
    assert start == 1784518200
    assert end == 1784518500

    next_slug, next_start, _ = MarketClock.current_slug(now=1784518500)
    assert next_slug == "btc-updown-5m-1784518500"
    assert next_start == 1784518500


def test_should_poll_settlement_only_after_round_end_plus_grace():
    trade = PendingTrade(trade_id="t1", slug="s", side="YES", entry_price=0.6, qty=5, end_ts=100)

    assert should_poll_settlement(trade, now_ts=101, grace_s=5) is False
    assert should_poll_settlement(trade, now_ts=106, grace_s=5) is True
