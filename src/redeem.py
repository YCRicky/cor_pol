from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from typing import Any, Optional

import requests


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
    relayer_api_key: str
    relayer_api_key_address: str
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
        relayer_api_key = os.getenv("RELAYER_API_KEY") or ""
        relayer_api_key_address = os.getenv("RELAYER_API_KEY_ADDRESS") or ""
        deposit_wallet = os.getenv("DEPOSIT_WALLET_ADDRESS") or os.getenv("DEPOSIT_WALLET") or ""
        missing = [
            name for name, value in (
                ("PRIVATE_KEY", private_key),
                ("RELAYER_API_KEY", relayer_api_key),
                ("RELAYER_API_KEY_ADDRESS", relayer_api_key_address),
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
            relayer_api_key=relayer_api_key,
            relayer_api_key_address=relayer_api_key_address,
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
            from py_builder_relayer_client.builder.deposit_wallet import build_deposit_wallet_batch_request  # type: ignore
            from py_builder_relayer_client.config import get_contract_config  # type: ignore
            from py_builder_relayer_client.models import (  # type: ignore
                DepositWalletCall,
                DepositWalletTransactionArgs,
                RelayerTransactionState,
                TransactionType,
            )
            from py_builder_relayer_client.signer import Signer  # type: ignore
        except Exception as exc:
            raise RuntimeError(
                "py-builder-relayer-client is required for CORR_AUTO_REDEEM=true"
            ) from exc

        self.build_deposit_wallet_batch_request = build_deposit_wallet_batch_request
        self.DepositWalletTransactionArgs = DepositWalletTransactionArgs
        self.DepositWalletCall = DepositWalletCall
        self.RelayerTransactionState = RelayerTransactionState
        self.TransactionType = TransactionType
        self.signer = Signer(config.private_key, config.chain_id)
        self.contract_config = get_contract_config(config.chain_id)
        self.relayer_url = config.relayer_url.rstrip("/")

    def _headers(self) -> dict[str, str]:
        return {
            "RELAYER_API_KEY": self.config.relayer_api_key,
            "RELAYER_API_KEY_ADDRESS": self.config.relayer_api_key_address,
            "Content-Type": "application/json",
        }

    def _get(self, path: str, params: Optional[dict[str, str]] = None) -> Any:
        resp = requests.get(
            f"{self.relayer_url}{path}",
            headers=self._headers(),
            params=params,
            timeout=20,
        )
        if resp.status_code != 200:
            raise RuntimeError(f"relayer GET {path} failed {resp.status_code}: {resp.text}")
        return resp.json()

    def _post(self, path: str, body: dict[str, Any]) -> Any:
        resp = requests.post(
            f"{self.relayer_url}{path}",
            headers=self._headers(),
            json=body,
            timeout=20,
        )
        if resp.status_code != 200:
            raise RuntimeError(f"relayer POST {path} failed {resp.status_code}: {resp.text}")
        return resp.json()

    def _get_nonce(self) -> str:
        payload = self._get("/nonce", {
            "address": self.signer.address(),
            "type": self.TransactionType.WALLET.value,
        })
        return str(payload["nonce"])

    def _poll_confirmed(self, transaction_id: str) -> Optional[dict[str, Any]]:
        target = {
            self.RelayerTransactionState.STATE_MINED.value,
            self.RelayerTransactionState.STATE_CONFIRMED.value,
        }
        fail = self.RelayerTransactionState.STATE_FAILED.value
        for _ in range(30):
            payload = self._get("/transaction", {"id": transaction_id})
            rows = payload if isinstance(payload, list) else [payload]
            if rows:
                txn = rows[0]
                state = txn.get("state") if isinstance(txn, dict) else None
                if state in target:
                    return txn
                if state == fail:
                    return None
            time.sleep(2)
        return None

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
            nonce = self._get_nonce()
            deadline = str(int(time.time()) + self.config.deadline_seconds)
            call = self.DepositWalletCall(target=adapter, value="0", data=data)
            args = self.DepositWalletTransactionArgs(
                from_address=self.signer.address(),
                chain_id=self.config.chain_id,
                wallet_address=self.config.deposit_wallet_address,
                nonce=nonce,
                calls=[call],
                deadline=deadline,
            )
            body = self.build_deposit_wallet_batch_request(
                signer=self.signer,
                args=args,
                config=self.contract_config,
            ).to_dict()
            resp = self._post("/submit", body)
            raw = dict(resp) if isinstance(resp, dict) else {"response": resp}
            confirmed = None
            if self.config.wait:
                transaction_id = str(raw.get("transactionID") or "")
                confirmed = self._poll_confirmed(transaction_id) if transaction_id else None
                raw["wait_result"] = _obj_to_dict(confirmed) or {"repr": repr(confirmed)}
            transaction_id = str(raw.get("transactionID") or "")
            transaction_hash = str(raw.get("transactionHash") or "")
            ok = bool(transaction_id or transaction_hash)
            if self.config.wait:
                ok = ok and confirmed is not None
            return RedeemResult(
                slug=slug,
                condition_id=condition_id,
                neg_risk=neg_risk,
                adapter=adapter,
                ok=ok,
                transaction_id=transaction_id,
                transaction_hash=transaction_hash,
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
