from __future__ import annotations

import os
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any, Optional


@dataclass
class ExecutionLegResult:
    label: str
    token_id: str
    requested_qty: float
    filled_qty: float
    avg_price: float
    notional: float
    order_id: str = ""
    status: str = ""
    ok: bool = True
    estimated: bool = False
    error: str = ""
    raw: dict[str, Any] = field(default_factory=dict)


def _obj_to_dict(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    out: dict[str, Any] = {}
    for name in dir(value):
        if name.startswith("_"):
            continue
        try:
            attr = getattr(value, name)
        except Exception:
            continue
        if callable(attr):
            continue
        if isinstance(attr, (str, int, float, bool, list, tuple, dict, type(None))):
            out[name] = attr
    return out


def _first(raw: dict[str, Any], names: tuple[str, ...]) -> Any:
    for name in names:
        if name in raw and raw[name] not in (None, ""):
            return raw[name]
    return None


def _float_or_none(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_amount(raw_value: Any, expected: float) -> Optional[float]:
    value = _float_or_none(raw_value)
    if value is None:
        return None
    candidates = [value, value / 1_000_000.0]
    hi = max(expected * 2.0, expected + 10.0, 10.0)
    valid = [x for x in candidates if 0.0 <= x <= hi]
    if not valid:
        return value
    return min(valid, key=lambda x: abs(x - expected))


def _round_up_to_tick(price: float, tick_size: str) -> float:
    tick = float(tick_size or "0.01")
    if tick <= 0:
        tick = 0.01
    # Keep a little integer slack for float representation before ceiling.
    units = int((price / tick) + 0.999999)
    decimals = max(0, len(str(tick).split(".")[-1]) if "." in str(tick) else 0)
    max_price = round(1.0 - tick, decimals)
    return round(min(max_price, max(tick, units * tick)), decimals)


def _clean_env(value: str | None) -> str:
    return (value or "").strip().strip('"').strip("'")


def _signature_type_from_env() -> str:
    raw = _clean_env(os.getenv("CLOB_SIGNATURE_TYPE") or os.getenv("SIGNATURE_TYPE") or "POLY_PROXY").upper()
    aliases = {
        "EOA": "0",
        "POLY_PROXY": "1",
        "PROXY": "1",
        "GNOSIS_SAFE": "2",
        "POLY_GNOSIS_SAFE": "2",
        "SAFE": "2",
        "POLY_1271": "3",
        "POLY1271": "3",
        "DEPOSIT": "3",
        "DEPOSIT_WALLET": "3",
    }
    return aliases.get(raw, raw)


def _builder_code_from_env() -> str:
    code = _clean_env(os.getenv("POLY_BUILDER_CODE") or os.getenv("CLOB_BUILDER_CODE"))
    if not code:
        return ""
    if not code.startswith("0x"):
        code = "0x" + code
    if len(code) != 66:
        raise RuntimeError("POLY_BUILDER_CODE must be a bytes32 hex value")
    int(code[2:], 16)
    return code


def _merge_results(base: ExecutionLegResult, extra: ExecutionLegResult) -> ExecutionLegResult:
    filled = base.filled_qty + extra.filled_qty
    notional = base.notional + extra.notional
    avg = notional / filled if filled > 0 else 0.0
    return ExecutionLegResult(
        label=base.label,
        token_id=base.token_id,
        requested_qty=base.requested_qty + extra.requested_qty,
        filled_qty=filled,
        avg_price=avg,
        notional=notional,
        order_id=",".join(x for x in (base.order_id, extra.order_id) if x),
        status=",".join(x for x in (base.status, extra.status) if x),
        ok=base.ok and extra.ok,
        estimated=base.estimated or extra.estimated,
        error="; ".join(x for x in (base.error, extra.error) if x),
        raw={"parts": [base.raw, extra.raw]},
    )


@dataclass
class LiveExecutionConfig:
    host: str
    chain_id: int
    private_key: str
    api_key: str
    api_secret: str
    api_passphrase: str
    funder: str
    signature_type: str = "POLY_PROXY"
    builder_code: str = ""
    order_type: str = "FAK"
    slippage_ticks: int = 2
    chase_slippage_ticks: int = 1
    mismatch_tolerance: float = 1.0
    max_chase_attempts: int = 2

    @classmethod
    def from_env(cls) -> "LiveExecutionConfig":
        private_key = _clean_env(os.getenv("PRIVATE_KEY") or os.getenv("POLYMARKET_PRIVATE_KEY"))
        api_key = _clean_env(os.getenv("CLOB_API_KEY") or os.getenv("POLYMARKET_API_KEY") or os.getenv("POLY_API_KEY"))
        api_secret = _clean_env(
            os.getenv("CLOB_SECRET")
            or os.getenv("CLOB_API_SECRET")
            or os.getenv("POLYMARKET_API_SECRET")
            or os.getenv("POLY_SECRET")
        )
        api_passphrase = (
            os.getenv("CLOB_PASS_PHRASE")
            or os.getenv("CLOB_API_PASS_PHRASE")
            or os.getenv("CLOB_API_PASSPHRASE")
            or os.getenv("CLOB_PASSPHRASE")
            or os.getenv("POLYMARKET_PASSPHRASE")
            or os.getenv("POLYMARKET_PASS_PHRASE")
            or os.getenv("POLY_PASSPHRASE")
            or ""
        )
        api_passphrase = _clean_env(api_passphrase)
        funder = (
            os.getenv("CLOB_FUNDER_ADDRESS")
            or os.getenv("FUNDER_ADDRESS")
            or os.getenv("POLYMARKET_PROXY_ADDRESS")
            or os.getenv("PROXY_WALLET_ADDRESS")
            or os.getenv("DEPOSIT_WALLET_ADDRESS")
            or os.getenv("DEPOSIT_WALLET")
            or ""
        )
        funder = _clean_env(funder)
        signature_type = _signature_type_from_env()
        missing = [
            name for name, value in (
                ("PRIVATE_KEY", private_key),
                ("CLOB_API_KEY", api_key),
                ("CLOB_SECRET", api_secret),
                ("CLOB_PASS_PHRASE", api_passphrase),
                ("CLOB_FUNDER_ADDRESS", funder),
            )
            if not value
        ]
        if missing:
            raise RuntimeError("missing live CLOB env: " + ", ".join(missing))
        return cls(
            host=os.getenv("CLOB_API_URL", "https://clob.polymarket.com"),
            chain_id=int(os.getenv("CHAIN_ID", "137")),
            private_key=private_key,
            api_key=api_key,
            api_secret=api_secret,
            api_passphrase=api_passphrase,
            funder=funder,
            signature_type=signature_type,
            builder_code=_builder_code_from_env(),
            order_type=os.getenv("CORR_EXEC_ORDER_TYPE", "FAK").upper(),
            slippage_ticks=int(os.getenv("CORR_EXEC_SLIPPAGE_TICKS", "2")),
            chase_slippage_ticks=int(os.getenv("CORR_EXEC_CHASE_SLIPPAGE_TICKS", "1")),
            mismatch_tolerance=float(os.getenv("CORR_LEG_MISMATCH_TOLERANCE_SHARES", "1.0")),
            max_chase_attempts=int(os.getenv("CORR_EXEC_MAX_CHASE_ATTEMPTS", "2")),
        )


class PolymarketLiveExecutor:
    def __init__(self, config: LiveExecutionConfig):
        self.config = config
        try:
            from py_clob_client_v2 import (  # type: ignore
                ApiCreds,
                AssetType,
                BalanceAllowanceParams,
                ClobClient,
                OrderArgs,
                OrderType,
                PartialCreateOrderOptions,
                Side,
                SignatureTypeV2,
            )
        except Exception as exc:
            raise RuntimeError(
                "py-clob-client-v2 is required for live trading; install requirements.txt"
            ) from exc
        try:
            from py_clob_client_v2 import PostOrdersV2Args  # type: ignore
        except Exception:
            PostOrdersV2Args = SimpleNamespace
        self.OrderArgs = OrderArgs
        self.OrderType = OrderType
        self.PartialCreateOrderOptions = PartialCreateOrderOptions
        self.PostOrdersV2Args = PostOrdersV2Args
        self.Side = Side
        sig_type = getattr(SignatureTypeV2, config.signature_type, None)
        if sig_type is None:
            sig_type = int(config.signature_type)
        creds = ApiCreds(
            api_key=config.api_key,
            api_secret=config.api_secret,
            api_passphrase=config.api_passphrase,
        )
        self.client = ClobClient(
            host=config.host,
            chain_id=config.chain_id,
            key=config.private_key,
            creds=creds,
            signature_type=sig_type,
            funder=config.funder,
        )
        self._asset_type = AssetType
        self._balance_params = BalanceAllowanceParams
        self._signature_type = sig_type

    def sync_collateral(self) -> None:
        self.client.update_balance_allowance(
            self._balance_params(
                asset_type=self._asset_type.COLLATERAL,
                signature_type=self._signature_type,
            )
        )

    def _build_buy_order(
        self,
        *,
        token_id: str,
        target_qty: float,
        best_ask: float,
        tick_size: str,
        neg_risk: bool,
        slippage_ticks: int,
    ) -> tuple[Any, float]:
        tick = float(tick_size or "0.01")
        limit_price = _round_up_to_tick(best_ask + max(slippage_ticks, 0) * tick, tick_size)
        order_kwargs: dict[str, Any] = {
            "token_id": token_id,
            "price": limit_price,
            "size": target_qty,
            "side": self.Side.BUY,
        }
        if self.config.builder_code:
            order_kwargs["builder_code"] = self.config.builder_code
        signed_order = self.client.create_order(
            order_args=self.OrderArgs(**order_kwargs),
            options=self.PartialCreateOrderOptions(tick_size=str(tick_size), neg_risk=bool(neg_risk)),
        )
        return signed_order, limit_price

    def buy_limit_fak(
        self,
        *,
        label: str,
        token_id: str,
        target_qty: float,
        best_ask: float,
        tick_size: str,
        neg_risk: bool,
        slippage_ticks: int,
    ) -> ExecutionLegResult:
        if target_qty <= 0:
            return ExecutionLegResult(label, token_id, target_qty, 0.0, 0.0, 0.0, ok=False, error="target_qty<=0")
        try:
            signed_order, limit_price = self._build_buy_order(
                token_id=token_id,
                target_qty=target_qty,
                best_ask=best_ask,
                tick_size=tick_size,
                neg_risk=neg_risk,
                slippage_ticks=slippage_ticks,
            )
            resp = self.client.post_order(
                signed_order,
                order_type=getattr(self.OrderType, self.config.order_type, self.OrderType.FAK),
            )
            raw = _obj_to_dict(resp)
            raw.setdefault("order_type", self.config.order_type)
            raw.setdefault("limit_price", limit_price)
            raw.setdefault("target_qty", target_qty)
            return self._parse_buy_response(label, token_id, target_qty, limit_price, raw)
        except Exception as exc:
            return ExecutionLegResult(
                label=label,
                token_id=token_id,
                requested_qty=target_qty,
                filled_qty=0.0,
                avg_price=0.0,
                notional=0.0,
                ok=False,
                error=str(exc),
            )

    def buy_pair_limit_fak(
        self,
        *,
        leg_a: dict[str, Any],
        leg_b: dict[str, Any],
        slippage_ticks: int,
    ) -> tuple[ExecutionLegResult, ExecutionLegResult]:
        specs = (leg_a, leg_b)
        try:
            signed_orders: list[Any] = []
            limit_prices: list[float] = []
            for spec in specs:
                signed_order, limit_price = self._build_buy_order(
                    token_id=str(spec["token_id"]),
                    target_qty=float(spec["target_qty"]),
                    best_ask=float(spec["best_ask"]),
                    tick_size=str(spec["tick_size"]),
                    neg_risk=bool(spec["neg_risk"]),
                    slippage_ticks=slippage_ticks,
                )
                signed_orders.append(signed_order)
                limit_prices.append(limit_price)
            order_type = getattr(self.OrderType, self.config.order_type, self.OrderType.FAK)
            post_args = [
                self.PostOrdersV2Args(order=signed_orders[0], orderType=order_type),
                self.PostOrdersV2Args(order=signed_orders[1], orderType=order_type),
            ]
            resp = self.client.post_orders(post_args)
            raws = self._split_batch_response(resp, 2)
            for raw, limit_price, spec in zip(raws, limit_prices, specs):
                raw.setdefault("order_type", self.config.order_type)
                raw.setdefault("limit_price", limit_price)
                raw.setdefault("target_qty", float(spec["target_qty"]))
            return (
                self._parse_buy_response(
                    str(leg_a["label"]),
                    str(leg_a["token_id"]),
                    float(leg_a["target_qty"]),
                    limit_prices[0],
                    raws[0],
                ),
                self._parse_buy_response(
                    str(leg_b["label"]),
                    str(leg_b["token_id"]),
                    float(leg_b["target_qty"]),
                    limit_prices[1],
                    raws[1],
                ),
            )
        except Exception as exc:
            err = str(exc)
            return (
                ExecutionLegResult(
                    label=str(leg_a.get("label", "")),
                    token_id=str(leg_a.get("token_id", "")),
                    requested_qty=float(leg_a.get("target_qty", 0.0) or 0.0),
                    filled_qty=0.0,
                    avg_price=0.0,
                    notional=0.0,
                    ok=False,
                    error=err,
                ),
                ExecutionLegResult(
                    label=str(leg_b.get("label", "")),
                    token_id=str(leg_b.get("token_id", "")),
                    requested_qty=float(leg_b.get("target_qty", 0.0) or 0.0),
                    filled_qty=0.0,
                    avg_price=0.0,
                    notional=0.0,
                    ok=False,
                    error=err,
                ),
            )

    def _split_batch_response(self, resp: Any, expected: int) -> list[dict[str, Any]]:
        if isinstance(resp, list):
            items = resp
        elif isinstance(resp, dict):
            items = resp.get("orders") or resp.get("data") or resp.get("results") or resp.get("responses")
            if not isinstance(items, list):
                items = []
        else:
            items = []
        raw_batch = _obj_to_dict(resp)
        out: list[dict[str, Any]] = []
        for idx in range(expected):
            if idx < len(items):
                raw = _obj_to_dict(items[idx])
                raw["batch_index"] = idx
                out.append(raw)
            else:
                out.append({
                    "success": False,
                    "errorMsg": "missing_batch_response",
                    "batch_index": idx,
                    "batch_response": raw_batch,
                })
        return out

    def _parse_buy_response(
        self,
        label: str,
        token_id: str,
        target_qty: float,
        limit_price: float,
        raw: dict[str, Any],
    ) -> ExecutionLegResult:
        order_id = str(_first(raw, ("orderID", "order_id", "id")) or "")
        status = str(_first(raw, ("status", "state")) or "")
        success = raw.get("success")
        error_msg = str(_first(raw, ("errorMsg", "error_msg", "error")) or "")

        qty_raw = _first(raw, (
            "takingAmount",
            "taking_amount",
            "sizeMatched",
            "size_matched",
            "matchedAmount",
            "matched_amount",
            "filledSize",
            "filled_size",
        ))
        notional_raw = _first(raw, ("makingAmount", "making_amount", "notional", "amount"))
        filled_qty = _normalize_amount(qty_raw, target_qty)
        notional = _normalize_amount(notional_raw, target_qty * limit_price)

        estimated = False
        if filled_qty is None:
            filled_qty = 0.0
            notional = 0.0

        if notional is None:
            notional = filled_qty * limit_price
            estimated = True

        avg_price = notional / filled_qty if filled_qty > 0 else 0.0
        ok = (success is not False) and not error_msg and filled_qty > 0
        return ExecutionLegResult(
            label=label,
            token_id=token_id,
            requested_qty=target_qty,
            filled_qty=filled_qty,
            avg_price=avg_price,
            notional=notional,
            order_id=order_id,
            status=status,
            ok=ok,
            estimated=estimated,
            error=error_msg,
            raw=raw,
        )


def merge_execution_results(base: ExecutionLegResult, extra: ExecutionLegResult) -> ExecutionLegResult:
    return _merge_results(base, extra)
