import json

import pytest

from misprice_pm.pm_client import (
    GammaMarket,
    GeoStatus,
    LivePreflightError,
    MarketMetadata,
    PolymarketPublicClient,
    PublicHttpClient,
    V2ClobGateway,
    parse_pm_up,
    source_timestamp_s,
)


def test_parse_pm_up_from_outcome_prices_up_wins():
    market = GammaMarket.from_payload(
        {"outcomes": '["Up", "Down"]', "outcomePrices": '["1", "0"]', "closed": True}
    )
    assert parse_pm_up(market) is True


def test_parse_pm_up_from_outcome_prices_down_wins():
    market = GammaMarket.from_payload(
        {"outcomes": ["Up", "Down"], "outcomePrices": ["0", "1"], "closed": True}
    )
    assert parse_pm_up(market) is False


def test_gamma_market_parses_nested_event_market_payload():
    payload = {
        "markets": [
            {
                "slug": "btc-updown-5m-x",
                "outcomes": json.dumps(["Up", "Down"]),
                "outcomePrices": json.dumps(["0", "1"]),
                "conditionId": "0xabc",
                "closed": True,
            }
        ]
    }
    markets = GammaMarket.list_from_payload(payload)
    assert len(markets) == 1
    assert markets[0].slug == "btc-updown-5m-x"
    assert parse_pm_up(markets[0]) is False


def test_gamma_prices_do_not_settle_before_market_is_officially_closed():
    market = GammaMarket.from_payload(
        {"outcomes": ["Up", "Down"], "outcomePrices": ["1", "0"], "active": True}
    )
    assert parse_pm_up(market) is None


def test_gamma_outcome_mapping_is_explicit_not_array_position_assumption():
    market = GammaMarket.from_payload(
        {"outcomes": ["Down", "Up"], "clobTokenIds": ["down-token", "up-token"]}
    )

    assert market.token_for_side("YES") == "up-token"
    assert market.token_for_side("NO") == "down-token"


def test_source_timestamp_rejects_missing_and_converts_milliseconds():
    assert source_timestamp_s({}) is None
    assert source_timestamp_s({"timestamp": "1700000000123"}) == 1700000000.123
    assert source_timestamp_s({"timestamp": "2026-07-20T00:00:00Z"}) == 1784505600.0


def test_geoblock_status_parses_official_response_without_bypass():
    class FakeHttp(PublicHttpClient):
        def get_json(self, url, timeout=20.0):
            return {"blocked": True, "country": "TW", "region": "", "ip": "203.0.113.1"}

    status = PolymarketPublicClient(http=FakeHttp()).geoblock_status("https://polymarket.com/api/geoblock")

    assert status.blocked is True
    assert status.country == "TW"


def test_v2_preflight_refuses_blocked_geography_before_balance_checks():
    gateway = V2ClobGateway(client=object(), sdk={})

    with pytest.raises(LivePreflightError, match="geoblock"):
        gateway.preflight(GeoStatus(blocked=True, country="TW", region="", ip=""), 5)


def test_deposit_wallet_allowance_calls_include_signature_type():
    class Params:
        def __init__(self, asset_type=None, signature_type=None):
            self.asset_type = asset_type
            self.signature_type = signature_type

    class Client:
        seen = []

        def get_closed_only_mode(self):
            return {"closed_only": False}

        def get_balance_allowance(self, params):
            self.seen.append(("get", params.signature_type))
            return {"balance": "10", "allowance": "10"}

        def update_balance_allowance(self, params):
            self.seen.append(("update", params.signature_type))
            return {"ok": True}

    client = Client()
    gateway = V2ClobGateway(
        client,
        {"BalanceAllowanceParams": Params, "AssetType": type("Asset", (), {"COLLATERAL": "pUSD"})},
        signature_type=3,
    )

    gateway.sync_collateral_allowance()
    gateway.preflight(GeoStatus(blocked=False, country="", region="", ip=""), 5)

    assert client.seen == [("update", 3), ("get", 3)]


def test_v2_preflight_refuses_boolean_close_only_response():
    class Client:
        def get_closed_only_mode(self):
            return True

    gateway = V2ClobGateway(Client(), {})
    with pytest.raises(LivePreflightError, match="close-only"):
        gateway.preflight(GeoStatus(blocked=False, country="", region="", ip=""), 0)


def test_trade_filter_matches_nested_maker_order_ids():
    class Client:
        only_first_page = None

        def get_trades(self, params, only_first_page=True):
            self.only_first_page = only_first_page
            return [
                {
                    "id": "trade-1",
                    "maker_orders": [{"order_id": "target-order"}],
                    "size": "2",
                    "price": "0.51",
                }
            ]

    class Params:
        def __init__(self, asset_id=None):
            self.asset_id = asset_id

    client = Client()
    gateway = V2ClobGateway(client, {"TradeParams": Params})
    assert gateway.order_trades("token", "target-order")[0]["id"] == "trade-1"
    assert client.only_first_page is False


def test_market_metadata_keeps_fee_exponent_and_builder_specific_rate():
    class Client:
        def get_clob_market_info(self, condition_id):
            return {
                "mts": "0.01",
                "mos": 5,
                "ao": True,
                "nr": False,
                "fd": {"r": 0.07, "e": 2},
                "tbf": 9999,
                "t": [{"t": "up-token", "o": "Up"}, {"t": "down-token", "o": "Down"}],
            }

        def _get_builder_taker_fee_rate(self, builder_code):
            return 0.008

    gateway = V2ClobGateway(Client(), {}, builder_code="0x" + "1" * 64)
    metadata = gateway.market_metadata("condition")

    assert metadata.fee_exponent == 2
    assert metadata.builder_taker_fee_bps == 80
    assert metadata.accepting_orders is True


def test_market_metadata_refuses_market_not_accepting_orders():
    class Client:
        def get_clob_market_info(self, condition_id):
            return {"ao": False}

    gateway = V2ClobGateway(Client(), {})
    with pytest.raises(LivePreflightError, match="not accepting orders"):
        gateway.market_metadata("condition")


def test_http_425_retries_the_same_signed_order_object():
    class Args:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    class Options:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    class Client:
        posted = []

        def create_order(self, args, options=None):
            return object()

        def post_order(self, order, order_type):
            self.posted.append(order)
            if len(self.posted) == 1:
                raise RuntimeError("HTTP 425 Too Early")
            return {"orderID": "order-1"}

    client = Client()
    gateway = V2ClobGateway(
        client,
        {
            "OrderArgs": Args,
            "PartialCreateOrderOptions": Options,
            "BUY": "BUY",
            "OrderType": type("OrderType", (), {"GTC": "GTC"}),
        },
        sleep=lambda _: None,
    )
    metadata = MarketMetadata("condition", "0.01", 1, False, 0.07, {}, {})

    result = gateway.submit_limit_buy("token", 0.5, 5, metadata)

    assert result["orderID"] == "order-1"
    assert len(client.posted) == 2
    assert client.posted[0] is client.posted[1]
