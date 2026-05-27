from __future__ import annotations

import os
from dataclasses import dataclass, field
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
    chase_slippage_ticks: int = 4
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
        signature_type = _clean_env(os.getenv("CLOB_SIGNATURE_TYPE") or os.getenv("SIGNATURE_TYPE") or "POLY_PROXY").upper()
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
            chase_slippage_ticks=int(os.getenv("CORR_EXEC_CHASE_SLIPPAGE_TICKS", "4")),
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
        self.OrderArgs = OrderArgs
        self.OrderType = OrderType
        self.PartialCreateOrderOptions = PartialCreateOrderOptions
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
        tick = float(tick_size or "0.01")
        limit_price = _round_up_to_tick(best_ask + max(slippage_ticks, 0) * tick, tick_size)
        try:
            order_kwargs: dict[str, Any] = {
                "token_id": token_id,
                "price": limit_price,
                "size": target_qty,
                "side": self.Side.BUY,
            }
            if self.config.builder_code:
                order_kwargs["builder_code"] = self.config.builder_code
            resp = self.client.create_and_post_order(
                order_args=self.OrderArgs(**order_kwargs),
                options=self.PartialCreateOrderOptions(tick_size=str(tick_size), neg_risk=bool(neg_risk)),
                order_type=getattr(self.OrderType, self.config.order_type, self.OrderType.FAK),
            )
            raw = _obj_to_dict(resp)
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

        if filled_qty is None and order_id:
            try:
                order = _obj_to_dict(self.client.get_order(order_id))
                qty_raw = _first(order, ("size_matched", "sizeMatched", "matchedAmount", "matched_amount"))
                filled_qty = _normalize_amount(qty_raw, target_qty)
                px = _float_or_none(_first(order, ("price", "avg_price", "average_price")))
                if filled_qty is not None and px is not None:
                    notional = filled_qty * px
                raw = {**raw, "order_lookup": order}
            except Exception as exc:
                raw = {**raw, "order_lookup_error": str(exc)}

        estimated = False
        if filled_qty is None:
            matched_status = status.lower() in ("matched", "filled", "mined", "confirmed")
            if success is True and matched_status:
                filled_qty = target_qty
                notional = target_qty * limit_price
                estimated = True
            else:
                filled_qty = 0.0
                notional = 0.0

        if notional is None:
            notional = filled_qty * limit_price
            estimated = True

        avg_price = notional / filled_qty if filled_qty > 0 else 0.0
        ok = (success is not False) and not error_msg
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
