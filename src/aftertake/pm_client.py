"""Polymarket public-data and authenticated CLOB V2 adapters.

The V2 SDK is imported only for an intentional live run.  Dry-run tests and
market-data inspection never need a private key or the live optional package.
"""

from __future__ import annotations

import datetime as dt
import json
import ssl
import threading
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple

from .config import Settings
from .resolver import ResolveOverrides, scoped_getaddrinfo


class LivePreflightError(RuntimeError):
    """A documented PM prerequisite was not satisfied; no order may be sent."""


@dataclass(frozen=True)
class GammaMarket:
    slug: str = ""
    condition_id: str = ""
    outcomes: Tuple[str, ...] = ()
    outcome_prices: Tuple[float, ...] = ()
    clob_token_ids: Tuple[str, ...] = ()
    closed: Optional[bool] = None
    active: Optional[bool] = None
    raw: Optional[Dict[str, Any]] = None

    @classmethod
    def from_payload(cls, payload: Dict[str, Any]) -> GammaMarket:
        def parse_jsonish(value: Any) -> List[Any]:
            if value is None:
                return []
            if isinstance(value, str):
                try:
                    parsed = json.loads(value)
                    return list(parsed) if isinstance(parsed, list) else []
                except json.JSONDecodeError:
                    return []
            if isinstance(value, list):
                return value
            return []

        return cls(
            slug=str(payload.get("slug") or ""),
            condition_id=str(payload.get("conditionId") or payload.get("condition_id") or ""),
            outcomes=tuple(str(x) for x in parse_jsonish(payload.get("outcomes"))),
            outcome_prices=tuple(float(x) for x in parse_jsonish(payload.get("outcomePrices"))),
            clob_token_ids=tuple(str(x) for x in parse_jsonish(payload.get("clobTokenIds"))),
            closed=payload.get("closed") if isinstance(payload.get("closed"), bool) else None,
            active=payload.get("active") if isinstance(payload.get("active"), bool) else None,
            raw=payload,
        )

    @classmethod
    def list_from_payload(cls, payload: Any) -> List[GammaMarket]:
        if isinstance(payload, list):
            return [cls.from_payload(item) for item in payload if isinstance(item, dict)]
        if isinstance(payload, dict):
            if isinstance(payload.get("markets"), list):
                return [cls.from_payload(item) for item in payload["markets"] if isinstance(item, dict)]
            return [cls.from_payload(payload)]
        return []

    def token_for_side(self, side: str) -> str:
        """Map strategy YES/NO to the market's explicit outcome/token mapping."""

        normalized = side.upper()
        if normalized not in {"YES", "NO"}:
            raise ValueError("side must be YES or NO")
        if len(self.outcomes) != len(self.clob_token_ids):
            raise LivePreflightError("Gamma outcome/token mapping is incomplete")
        normalized_tokens = {
            outcome.strip().lower(): token
            for outcome, token in zip(self.outcomes, self.clob_token_ids)
        }
        aliases = ("yes", "up") if normalized == "YES" else ("no", "down")
        for alias in aliases:
            if alias in normalized_tokens:
                return normalized_tokens[alias]
        raise LivePreflightError("market outcome mapping does not contain %s" % normalized)


@dataclass(frozen=True)
class GeoStatus:
    blocked: bool
    country: str
    region: str
    ip: str


@dataclass(frozen=True)
class MarketMetadata:
    condition_id: str
    tick_size: str
    min_order_size: float
    neg_risk: bool
    fee_rate: float
    tokens: Dict[str, str]
    raw: Dict[str, Any]
    fee_exponent: float = 1.0
    builder_taker_fee_bps: float = 0.0
    accepting_orders: bool = True
    immediate_taker_order_delay_enabled: bool = False
    expected_taker_delay_ms: float = 0.0


@dataclass(frozen=True)
class BalanceAllowance:
    balance: float
    allowance: float
    raw: Dict[str, Any]


@dataclass(frozen=True)
class LivePreflight:
    geo: GeoStatus
    collateral: BalanceAllowance
    closed_only: bool


class PublicHttpClient:
    """Plain HTTPS client using the provider's official hostname and TLS path."""

    def __init__(self, user_agent: str = "aftertake/0.2", resolve_overrides: Optional[ResolveOverrides] = None):
        self.user_agent = user_agent
        self.resolve_overrides = resolve_overrides or {}
        self._ssl_context = ssl.create_default_context(cafile=self._certifi_ca())

    @staticmethod
    def _certifi_ca() -> Optional[str]:
        try:
            import certifi  # type: ignore

            return str(certifi.where())
        except Exception:
            return None

    def get_json(self, url: str, timeout: float = 20.0) -> Any:
        req = urllib.request.Request(
            url, headers={"User-Agent": self.user_agent, "Accept": "application/json"}
        )
        with scoped_getaddrinfo(self.resolve_overrides):
            with urllib.request.urlopen(req, timeout=timeout, context=self._ssl_context) as response:
                raw = response.read().decode("utf-8")
        return json.loads(raw)


def _as_float(value: Any, label: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise LivePreflightError("invalid %s returned by Polymarket" % label) from exc


PUSD_BASE_UNITS = Decimal("1000000")


def _as_pusd(value: Any, label: str) -> float:
    """Convert a CLOB V2 pUSD base-unit amount to USD safely.

    CLOB's balance-allowance endpoint returns ERC-20 base units. pUSD has six
    decimals, so treating an atomic response as USD would inflate live sizing
    by 1,000,000x.
    """

    if isinstance(value, bool) or value is None or isinstance(value, (dict, list)):
        raise LivePreflightError("invalid %s returned by Polymarket" % label)
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise LivePreflightError("invalid %s returned by Polymarket" % label) from exc
    if not amount.is_finite() or amount < 0:
        raise LivePreflightError("invalid %s returned by Polymarket" % label)
    return float(amount / PUSD_BASE_UNITS)


def _minimum_pusd_allowance(value: Any, label: str) -> float:
    """Return a fail-closed allowance across the CLOB exchange-spender map."""

    if not isinstance(value, dict) or not value:
        raise LivePreflightError("invalid %s returned by Polymarket" % label)
    amounts = [_as_pusd(amount, "%s entry" % label) for amount in value.values()]
    return min(amounts)


def parse_pm_up(market: GammaMarket) -> Optional[bool]:
    if market.closed is not True and market.active is not False:
        return None
    if not market.outcomes or not market.outcome_prices:
        return None
    prices = {
        name.strip().lower(): price for name, price in zip(market.outcomes, market.outcome_prices)
    }
    if "up" in prices and "down" in prices:
        if prices["up"] >= 0.999 and prices["down"] <= 0.001:
            return True
        if prices["down"] >= 0.999 and prices["up"] <= 0.001:
            return False
    return None


class PolymarketPublicClient:
    def __init__(
        self,
        gamma_host: str = "https://gamma-api.polymarket.com",
        clob_host: str = "https://clob.polymarket.com",
        http: Optional[PublicHttpClient] = None,
    ):
        self.gamma_host = gamma_host.rstrip("/")
        self.clob_host = clob_host.rstrip("/")
        self.http = http or PublicHttpClient()

    def market_by_slug(self, slug: str, allow_closed: bool = False) -> GammaMarket:
        urls = (
            "%s/markets/slug/%s" % (self.gamma_host, urllib.parse.quote(slug)),
            "%s/events/slug/%s" % (self.gamma_host, urllib.parse.quote(slug)),
        )
        last_error: Optional[Exception] = None
        for url in urls:
            try:
                markets = GammaMarket.list_from_payload(self.http.get_json(url))
                for market in markets:
                    if market.slug == slug or len(markets) == 1:
                        if not allow_closed and (market.closed is True or market.active is False):
                            raise LivePreflightError("market is not active: %s" % slug)
                        return market
            except Exception as exc:  # keep the final diagnostic for the caller
                last_error = exc
        raise LookupError("Gamma market not found for slug=%s: %s" % (slug, last_error))

    def book(self, token_id: str) -> Dict[str, Any]:
        url = "%s/book?token_id=%s" % (self.clob_host, urllib.parse.quote(str(token_id)))
        raw = self.http.get_json(url)
        if not isinstance(raw, dict):
            raise LivePreflightError("CLOB returned a non-object order book")
        return raw

    def geoblock_status(self, endpoint: str) -> GeoStatus:
        raw = self.http.get_json(endpoint)
        if not isinstance(raw, dict) or "blocked" not in raw:
            raise LivePreflightError("invalid response from Polymarket geoblock endpoint")
        return GeoStatus(
            blocked=bool(raw["blocked"]),
            country=str(raw.get("country") or ""),
            region=str(raw.get("region") or ""),
            ip=str(raw.get("ip") or ""),
        )


class V2ClobGateway:
    """Narrow adapter around ``py-clob-client-v2``.

    All methods return raw server payloads as well as typed summaries so the
    execution layer can persist the source-of-truth response for recovery.
    """

    def __init__(
        self,
        client: Any,
        sdk: Any,
        signature_type: Optional[int] = None,
        builder_code: str = "",
        sleep: Any = time.sleep,
    ):
        self._client = client
        self._sdk = sdk
        self._signature_type = signature_type
        self._builder_code = builder_code
        self._sleep = sleep
        # `get_clob_market_info()` updates the V2 SDK's market/token/fee caches.
        # Asset rounds call that method concurrently before close, so only the
        # metadata/preflight path is serialized. Fast post-close order submits
        # intentionally remain outside this lock.
        self._client_lock = threading.RLock()

    @classmethod
    def from_settings(cls, settings: Settings) -> V2ClobGateway:
        if not settings.is_live:
            raise RuntimeError("V2 CLOB gateway is only constructed for live mode")
        try:
            from py_clob_client_v2 import (
                ApiCreds,  # type: ignore
                BuilderConfig,  # type: ignore
                ClobClient,  # type: ignore
            )
        except ImportError as exc:
            raise RuntimeError(
                "Install the pinned live extra: pip install -e '.[live]'"
            ) from exc

        # Settings.validate() requires this exact L2 credential set for a live
        # Aftertake account. Keep the defensive check here too because callers
        # can construct Settings directly in code.
        if not settings.has_static_api_creds:
            raise RuntimeError(
                "live V2 CLOB gateway requires POLYMARKET_API_KEY, POLYMARKET_API_SECRET, and POLYMARKET_PASSPHRASE"
            )
        creds = ApiCreds(
            api_key=settings.clob_api_key,
            api_secret=settings.clob_api_secret,
            api_passphrase=settings.clob_api_passphrase,
        )

        builder_config = None
        if settings.builder_code:
            builder_config = BuilderConfig(builder_code=settings.builder_code)
        client = ClobClient(
            host=settings.clob_host,
            chain_id=settings.polymarket_chain_id,
            key=settings.polymarket_private_key,
            creds=creds,
            signature_type=settings.polymarket_signature_type,
            funder=settings.polymarket_funder,
            builder_config=builder_config,
            use_server_time=True,
            retry_on_error=False,
        )
        sdk = {
            "ApiCreds": ApiCreds,
            "AssetType": __import__("py_clob_client_v2", fromlist=["AssetType"]).AssetType,
            "BalanceAllowanceParams": __import__(
                "py_clob_client_v2", fromlist=["BalanceAllowanceParams"]
            ).BalanceAllowanceParams,
            "OrderArgs": __import__("py_clob_client_v2", fromlist=["OrderArgs"]).OrderArgs,
            "OrderPayload": __import__("py_clob_client_v2", fromlist=["OrderPayload"]).OrderPayload,
            "OrderType": __import__("py_clob_client_v2", fromlist=["OrderType"]).OrderType,
            "PartialCreateOrderOptions": __import__(
                "py_clob_client_v2", fromlist=["PartialCreateOrderOptions"]
            ).PartialCreateOrderOptions,
            "TradeParams": __import__("py_clob_client_v2", fromlist=["TradeParams"]).TradeParams,
            "BUY": __import__(
                "py_clob_client_v2.order_builder.constants", fromlist=["BUY"]
            ).BUY,
        }
        return cls(client, sdk, settings.polymarket_signature_type, settings.builder_code)

    def preflight(self, geo: GeoStatus, required_notional: float) -> LivePreflight:
        with self._client_lock:
            if geo.blocked:
                raise LivePreflightError(
                    "Polymarket geoblock prohibits new orders from %s %s" % (geo.country, geo.region)
                )
            closed_only = self._client.get_closed_only_mode()
            is_closed_only = (
                bool(closed_only)
                if isinstance(closed_only, bool)
                else bool(
                    closed_only.get("closed_only") or closed_only.get("closedOnly")
                )
                if isinstance(closed_only, dict)
                else bool(getattr(closed_only, "closed_only", False))
            )
            if is_closed_only:
                raise LivePreflightError("CLOB account is in close-only mode")
            collateral = self.collateral_balance_allowance()
            if collateral.balance < required_notional:
                raise LivePreflightError("deposit wallet pUSD balance is below the final order requirement")
            if collateral.allowance < required_notional:
                raise LivePreflightError("deposit wallet pUSD allowance is below the final order requirement")
            return LivePreflight(geo=geo, collateral=collateral, closed_only=False)

    def collateral_balance_allowance(self) -> BalanceAllowance:
        with self._client_lock:
            params = self._sdk["BalanceAllowanceParams"](
                asset_type=self._sdk["AssetType"].COLLATERAL,
                signature_type=self._signature_type,
            )
            raw = self._client.get_balance_allowance(params)
            if not isinstance(raw, dict):
                raise LivePreflightError("invalid CLOB collateral balance response")
            balance = _as_pusd(raw.get("balance"), "collateral balance")
            # CLOB V2 returns ``allowances`` (plural): a pUSD base-unit allowance
            # for each exchange spender. Before the exact market exchange is
            # selected, the minimum is the only safe generic buying-power limit.
            allowances = raw.get("allowances")
            if allowances is not None:
                allowance = _minimum_pusd_allowance(allowances, "collateral allowances")
            else:
                # Preserve support for a legacy scalar response while still
                # treating it as six-decimal pUSD base units.
                allowance_value = raw.get("allowance")
                allowance = (
                    _minimum_pusd_allowance(allowance_value, "collateral allowance")
                    if isinstance(allowance_value, dict)
                    else _as_pusd(allowance_value, "collateral allowance")
                )
            return BalanceAllowance(balance=balance, allowance=allowance, raw=raw)

    def sync_collateral_allowance(self) -> Dict[str, Any]:
        """Refresh the CLOB cache after the operator confirms wallet approval."""

        with self._client_lock:
            params = self._sdk["BalanceAllowanceParams"](
                asset_type=self._sdk["AssetType"].COLLATERAL,
                signature_type=self._signature_type,
            )
            raw = self._client.update_balance_allowance(params)
            return raw if isinstance(raw, dict) else {"raw": raw}

    def market_metadata(self, condition_id: str) -> MarketMetadata:
        with self._client_lock:
            raw = self._client.get_clob_market_info(condition_id)
            if not isinstance(raw, dict):
                raise LivePreflightError("invalid CLOB market metadata")
            accepting_orders = raw.get("ao", raw.get("accepting_orders"))
            if accepting_orders is not True:
                raise LivePreflightError("CLOB market is not accepting orders")
            tick_size = str(raw.get("mts") or "")
            min_order_size = _as_float(raw.get("mos"), "minimum order size")
            if not tick_size or min_order_size <= 0:
                raise LivePreflightError("CLOB market metadata is incomplete")
            tokens: Dict[str, str] = {}
            for token in raw.get("t") or []:
                if not isinstance(token, dict):
                    continue
                token_id = str(token.get("t") or "")
                outcome = str(token.get("o") or "").strip().lower()
                if token_id and outcome:
                    tokens[outcome] = token_id
            if not tokens:
                raise LivePreflightError("CLOB market metadata has no outcome/token mapping")
            fee_details = raw.get("fd") or {}
            fee_rate = _as_float(fee_details.get("r", 0.0), "market fee rate")
            fee_exponent = _as_float(fee_details.get("e", 1.0), "market fee exponent")
            builder_taker_fee_bps = 0.0
            if self._builder_code:
                # The pinned V2 client resolves the fee configured for this exact
                # builder code. Market `tbf` is not the builder-program fee.
                builder_rate = self._client._get_builder_taker_fee_rate(self._builder_code)
                builder_taker_fee_bps = _as_float(builder_rate, "builder taker fee") * 10_000.0
            itode = bool(raw.get("itode", raw.get("immediate_taker_order_delay_enabled", False)))
            return MarketMetadata(
                condition_id=condition_id,
                tick_size=tick_size,
                min_order_size=min_order_size,
                neg_risk=bool(raw.get("nr", False)),
                fee_rate=fee_rate,
                tokens=tokens,
                raw=raw,
                fee_exponent=fee_exponent,
                builder_taker_fee_bps=builder_taker_fee_bps,
                accepting_orders=True,
                immediate_taker_order_delay_enabled=itode,
                expected_taker_delay_ms=250.0 if itode else 0.0,
            )

    def submit_limit_buy(
        self, token_id: str, price: float, qty: float, metadata: MarketMetadata, order_type: str = "GTC"
    ) -> Dict[str, Any]:
        return self._submit_limit_buy(token_id, price, qty, metadata, retry_on_restart=True, order_type=order_type)

    def submit_limit_buy_fast(
        self, token_id: str, price: float, qty: float, metadata: MarketMetadata, order_type: str = "FAK"
    ) -> Dict[str, Any]:
        """Submit exactly once for a sub-second post-close opportunity.

        The normal path deliberately retries a matching-engine restart.  That
        delay is correct for ordinary entries but would turn a 50--1000ms
        residual-ask attempt into a stale order.  The caller decides how to
        classify submit-path infrastructure failures; current Aftertake policy
        skips only the affected market and never globally freezes new entries.
        """

        return self._submit_limit_buy(token_id, price, qty, metadata, retry_on_restart=False, order_type=order_type)

    def _submit_limit_buy(
        self,
        token_id: str,
        price: float,
        qty: float,
        metadata: MarketMetadata,
        *,
        retry_on_restart: bool,
        order_type: str,
    ) -> Dict[str, Any]:
        if qty < metadata.min_order_size:
            raise LivePreflightError("requested quantity is below the CLOB minimum order size")
        order = self._client.create_order(
            self._sdk["OrderArgs"](
                token_id=str(token_id), price=float(price), size=float(qty), side=self._sdk["BUY"]
            ),
            options=self._sdk["PartialCreateOrderOptions"](
                tick_size=metadata.tick_size, neg_risk=metadata.neg_risk
            ),
        )
        raw = None
        attempts = 3 if retry_on_restart else 1
        for attempt in range(attempts):
            try:
                # Reuse the exact same signed order across a short 425
                # restart retry; never create a second salt/order intent.
                # Polymarket py-clob-client documents FAK/FOK as order types
                # on post_order with post_only disabled. Pass by keyword first
                # so SDK parameter-name drift does not silently alter intent.
                raw = self._post_order(order, self._order_type(order_type))
                break
            except Exception as exc:
                if not is_matching_engine_restart_error(exc) or attempt == attempts - 1:
                    raise
                self._sleep(float(2**attempt))
        if not isinstance(raw, dict):
            raise RuntimeError("CLOB returned an invalid order response")
        return raw

    def _post_order(self, order: Any, order_type: Any) -> Any:
        try:
            return self._client.post_order(order, order_type=order_type, post_only=False)
        except TypeError as first_exc:
            try:
                return self._client.post_order(order, orderType=order_type, post_only=False)
            except TypeError:
                try:
                    return self._client.post_order(order, order_type)
                except TypeError:
                    raise first_exc from None

    def _order_type(self, order_type: str) -> Any:
        normalized = str(order_type or "").upper().strip()
        value = getattr(self._sdk["OrderType"], normalized, None)
        if value is None:
            raise LivePreflightError(f"CLOB SDK does not support OrderType.{normalized}")
        return value

    def get_order(self, order_id: str) -> Dict[str, Any]:
        raw = self._client.get_order(order_id)
        if not isinstance(raw, dict):
            raise RuntimeError("CLOB returned an invalid order lookup")
        return raw

    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        raw = self._client.cancel_order(self._sdk["OrderPayload"](orderID=order_id))
        return raw if isinstance(raw, dict) else {"raw": raw}

    def order_trades(self, token_id: str, order_id: str) -> List[Dict[str, Any]]:
        # The pinned V2 SDK paginates until its END_CURSOR by default. A first
        # page only can understate matched quantity and corrupt the fill VWAP.
        trades = self._client.get_trades(
            self._sdk["TradeParams"](asset_id=str(token_id)),
            only_first_page=False,
        )
        if not isinstance(trades, list):
            return []
        matches: List[Dict[str, Any]] = []
        for trade in trades:
            if not isinstance(trade, dict):
                continue
            identifiers = {
                str(trade.get(key) or "")
                for key in ("order_id", "orderID", "maker_order_id", "taker_order_id")
            }
            for maker_order in trade.get("maker_orders") or []:
                if isinstance(maker_order, dict):
                    identifiers.add(
                        str(maker_order.get("order_id") or maker_order.get("orderID") or "")
                    )
            if order_id in identifiers:
                matches.append(trade)
        return matches

    def post_heartbeat(self, heartbeat_id: str = "") -> Dict[str, Any]:
        raw = self._client.post_heartbeat(heartbeat_id)
        return raw if isinstance(raw, dict) else {"raw": raw}


def is_matching_engine_restart_error(exc: BaseException) -> bool:
    text = str(exc).lower()
    return "425" in text or "matching engine" in text or "too early" in text


def source_timestamp_s(raw: Dict[str, Any]) -> Optional[float]:
    """Return a CLOB-provided timestamp in seconds, without inventing one."""

    value = raw.get("timestamp") or raw.get("time") or raw.get("ts")
    if value is None:
        return None
    try:
        timestamp = float(value)
    except (TypeError, ValueError):
        try:
            text = str(value).strip().replace("Z", "+00:00")
            timestamp = dt.datetime.fromisoformat(text).timestamp()
        except (TypeError, ValueError):
            return None
    # CLOB timestamps are commonly milliseconds; preserve true seconds.
    return timestamp / 1000.0 if timestamp > 10_000_000_000 else timestamp


def now_s() -> float:
    return time.time()
