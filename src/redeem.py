from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from typing import Any, Optional


PUSD_ADDRESS = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
CTF_COLLATERAL_ADAPTER = "0xAdA100Db00Ca00073811820692005400218FcE1f"
NEG_RISK_CTF_COLLATERAL_ADAPTER = "0xadA2005600Dec949baf300f4C6120000bDB6eAab"
ZERO_BYTES32 = "0x" + ("0" * 64)


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _clean_hex32(value: str) -> str:
    value = (value or "").strip()
    if not value:
        raise ValueError("empty condition id")
    if not value.startswith("0x"):
        value = "0x" + value
    if len(value) != 66:
        raise ValueError(f"condition id must be bytes32 hex, got {value}")
    int(value[2:], 16)
    return value


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


def encode_redeem_positions(
    *,
    collateral_token: str,
    parent_collection_id: str,
    condition_id: str,
    index_sets: list[int],
) -> str:
    try:
        from eth_abi import encode  # type: ignore
        from eth_utils import keccak, to_checksum_address  # type: ignore
    except Exception as exc:
        raise RuntimeError("eth_abi and eth_utils are required for auto redeem") from exc

    selector = keccak(text="redeemPositions(address,bytes32,bytes32,uint256[])")[:4]
    parent = bytes.fromhex(_clean_hex32(parent_collection_id)[2:])
    condition = bytes.fromhex(_clean_hex32(condition_id)[2:])
    encoded_args = encode(
        ["address", "bytes32", "bytes32", "uint256[]"],
        [to_checksum_address(collateral_token), parent, condition, index_sets],
    )
    return "0x" + (selector + encoded_args).hex()


@dataclass
class AutoRedeemConfig:
    relayer_url: str
    chain_id: int
    private_key: str
    builder_api_key: str
    builder_secret: str
    builder_passphrase: str
    deposit_wallet_address: str
    collateral_token: str = PUSD_ADDRESS
    ctf_adapter: str = CTF_COLLATERAL_ADAPTER
    neg_risk_adapter: str = NEG_RISK_CTF_COLLATERAL_ADAPTER
    parent_collection_id: str = ZERO_BYTES32
    index_sets: list[int] = field(default_factory=lambda: [1, 2])
    deadline_seconds: int = 600
    wait: bool = True

    @classmethod
    def from_env_if_enabled(cls) -> Optional["AutoRedeemConfig"]:
        if not _env_bool("CORR_AUTO_REDEEM", False):
            return None
        private_key = os.getenv("PRIVATE_KEY") or os.getenv("POLYMARKET_PRIVATE_KEY") or ""
        builder_api_key = os.getenv("BUILDER_API_KEY") or os.getenv("RELAYER_API_KEY") or ""
        builder_secret = os.getenv("BUILDER_SECRET") or os.getenv("RELAYER_SECRET") or ""
        builder_passphrase = (
            os.getenv("BUILDER_PASS_PHRASE")
            or os.getenv("BUILDER_PASSPHRASE")
            or os.getenv("RELAYER_PASS_PHRASE")
            or os.getenv("RELAYER_PASSPHRASE")
            or ""
        )
        deposit_wallet = os.getenv("DEPOSIT_WALLET_ADDRESS") or os.getenv("DEPOSIT_WALLET") or ""
        missing = [
            name for name, value in (
                ("PRIVATE_KEY", private_key),
                ("BUILDER_API_KEY", builder_api_key),
                ("BUILDER_SECRET", builder_secret),
                ("BUILDER_PASS_PHRASE", builder_passphrase),
                ("DEPOSIT_WALLET_ADDRESS", deposit_wallet),
            )
            if not value
        ]
        if missing:
            raise RuntimeError("missing auto-redeem env: " + ", ".join(missing))
        raw_index_sets = os.getenv("CORR_REDEEM_INDEX_SETS", "1,2")
        index_sets = [int(x.strip()) for x in raw_index_sets.split(",") if x.strip()]
        if not index_sets:
            index_sets = [1, 2]
        return cls(
            relayer_url=os.getenv("RELAYER_URL", "https://relayer-v2.polymarket.com/"),
            chain_id=int(os.getenv("CHAIN_ID", "137")),
            private_key=private_key,
            builder_api_key=builder_api_key,
            builder_secret=builder_secret,
            builder_passphrase=builder_passphrase,
            deposit_wallet_address=deposit_wallet,
            collateral_token=os.getenv("PUSD_ADDRESS", PUSD_ADDRESS),
            ctf_adapter=os.getenv("CTF_COLLATERAL_ADAPTER", CTF_COLLATERAL_ADAPTER),
            neg_risk_adapter=os.getenv("NEG_RISK_CTF_COLLATERAL_ADAPTER", NEG_RISK_CTF_COLLATERAL_ADAPTER),
            parent_collection_id=os.getenv("CORR_REDEEM_PARENT_COLLECTION_ID", ZERO_BYTES32),
            index_sets=index_sets,
            deadline_seconds=int(os.getenv("CORR_REDEEM_DEADLINE_S", "600")),
            wait=_env_bool("CORR_REDEEM_WAIT", True),
        )


@dataclass
class RedeemResult:
    slug: str
    condition_id: str
    neg_risk: bool
    adapter: str
    ok: bool
    transaction_id: str = ""
    transaction_hash: str = ""
    confirmed: bool = False
    error: str = ""
    raw: dict[str, Any] = field(default_factory=dict)


class AutoRedeemer:
    def __init__(self, config: AutoRedeemConfig):
        self.config = config
        try:
            from py_builder_relayer_client.client import RelayClient  # type: ignore
            from py_builder_relayer_client.models import DepositWalletCall, TransactionType  # type: ignore
            from py_builder_signing_sdk.config import BuilderApiKeyCreds, BuilderConfig  # type: ignore
        except Exception as exc:
            raise RuntimeError(
                "py-builder-relayer-client is required for CORR_AUTO_REDEEM=true"
            ) from exc

        self.DepositWalletCall = DepositWalletCall
        self.TransactionType = TransactionType
        builder_config = BuilderConfig(
            local_builder_creds=BuilderApiKeyCreds(
                key=config.builder_api_key,
                secret=config.builder_secret,
                passphrase=config.builder_passphrase,
            )
        )
        self.client = RelayClient(
            config.relayer_url,
            config.chain_id,
            config.private_key,
            builder_config,
        )

    def redeem_condition(self, *, slug: str, condition_id: str, neg_risk: bool = False) -> RedeemResult:
        adapter = self.config.neg_risk_adapter if neg_risk else self.config.ctf_adapter
        try:
            condition_id = _clean_hex32(condition_id)
            data = encode_redeem_positions(
                collateral_token=self.config.collateral_token,
                parent_collection_id=self.config.parent_collection_id,
                condition_id=condition_id,
                index_sets=self.config.index_sets,
            )
            nonce_payload = self.client.get_nonce(
                self.client.signer.address(),
                self.TransactionType.WALLET.value,
            )
            nonce = str(nonce_payload["nonce"])
            deadline = str(int(time.time()) + self.config.deadline_seconds)
            call = self.DepositWalletCall(target=adapter, value="0", data=data)
            resp = self.client.execute_deposit_wallet_batch(
                calls=[call],
                wallet_address=self.config.deposit_wallet_address,
                nonce=nonce,
                deadline=deadline,
            )
            raw = _obj_to_dict(resp)
            confirmed = None
            if self.config.wait:
                confirmed = resp.wait()
                raw["wait_result"] = _obj_to_dict(confirmed) or {"repr": repr(confirmed)}
            ok = bool(resp.transaction_id or resp.transaction_hash)
            if self.config.wait:
                ok = ok and confirmed is not None
            return RedeemResult(
                slug=slug,
                condition_id=condition_id,
                neg_risk=neg_risk,
                adapter=adapter,
                ok=ok,
                transaction_id=str(resp.transaction_id or ""),
                transaction_hash=str(resp.transaction_hash or ""),
                confirmed=confirmed is not None if self.config.wait else False,
                raw=raw,
            )
        except Exception as exc:
            return RedeemResult(
                slug=slug,
                condition_id=condition_id,
                neg_risk=neg_risk,
                adapter=adapter,
                ok=False,
                error=str(exc),
            )
