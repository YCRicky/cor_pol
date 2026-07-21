from __future__ import annotations

import json
import urllib.parse
import urllib.request
from typing import Any, Dict


def _number(value: Any, digits: int = 4) -> str:
    try:
        return f"{float(value):.{digits}f}"
    except (TypeError, ValueError):
        return "n/a"


def format_event(kind: str, payload: Dict[str, Any], slug: str = "") -> str:
    """Format the complete operator-facing trading lifecycle without secrets."""

    if kind == "boot":
        return (
            "[Misprice PM] BOOT\n"
            f"mode={'SHADOW' if payload['dry_run'] else 'LIVE'} "
            f"qty={_number(payload['qty'])}\n"
            "one-entry-per-market + SQLite recovery + CLOB V2 preflight"
        )
    if kind == "submitted":
        return (
            "[Misprice PM] ORDER_SUBMITTED\n"
            f"{payload['side']} qty={_number(payload['requested_qty'])} "
            f"limit={_number(payload['requested_price'])}\n"
            f"order={payload['order_id']} slug={slug}"
        )
    if kind == "recovery":
        return (
            "[Misprice PM] ORDER_RECOVERY\n"
            "operator attached an order ID; authenticated identity check pending\n"
            f"order={payload['order_id']} intent={payload['intent_id']} slug={slug or 'unknown'}"
        )
    if kind == "entry":
        return (
            "[Misprice PM] ENTRY_CONFIRMED\n"
            f"{payload['side']} filled={_number(payload['filled_qty'])} "
            f"avg={_number(payload['avg_price'])} "
            f"requested={_number(payload['requested_qty'])}\n"
            f"status={payload['status']} order={payload['order_id']} slug={slug}"
        )
    if kind == "order_result":
        return (
            "[Misprice PM] ORDER_RESULT\n"
            f"{payload['side']} status={payload['status']} "
            f"filled={_number(payload['filled_qty'])} "
            f"avg={_number(payload['avg_price'])}\n"
            f"submission={payload['submission_state']} "
            f"order={payload.get('order_id') or 'n/a'} slug={slug}"
        )
    if kind == "blocked":
        return (
            "[Misprice PM] ENTRY_BLOCKED\n"
            f"reason={payload['reason']}\nslug={slug}"
        )
    if kind == "round":
        return (
            "[Misprice PM] NO_ENTRY\n"
            f"ticks={payload['ticks']} decisions={payload['decisions']} "
            f"last_reason={payload['last_reason']}\nslug={slug}"
        )
    if kind == "settle":
        return (
            "[Misprice PM] SETTLE\n"
            f"{payload['side']} result={'WIN' if payload['win'] else 'LOSS'} "
            f"pm_up={payload['pm_up']} "
            f"qty={_number(payload['qty'])} pnl=${float(payload['pnl']):+.2f}\n"
            f"entry={_number(payload['entry_price'])} "
            f"fee=${_number(payload['entry_fee'], 4)} slug={slug}"
        )
    if kind == "alert":
        return f"[Misprice PM] ALERT\nreason={payload['reason']}\nslug={slug or 'runtime'}"
    raise ValueError(f"unsupported notification event: {kind}")


class Notifier:
    def __init__(self, *, token: str = "", chat_id: str = ""):
        self.token = token
        self.chat_id = chat_id

    @property
    def enabled(self) -> bool:
        return bool(self.token and self.chat_id)

    def send(self, text: str) -> bool:
        if not self.enabled:
            return False
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        body = urllib.parse.urlencode({"chat_id": self.chat_id, "text": text}).encode()
        req = urllib.request.Request(url, data=body, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                raw = resp.read().decode("utf-8")
        except Exception as exc:
            # urllib exceptions may include the token-bearing request URL.
            raise RuntimeError(
                f"Telegram transport failed ({type(exc).__name__})"
            ) from exc
        try:
            result = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RuntimeError("Telegram returned invalid JSON") from exc
        if not isinstance(result, dict) or result.get("ok") is not True:
            description = result.get("description", "unknown error") if isinstance(result, dict) else ""
            raise RuntimeError(f"Telegram rejected message: {description}")
        return True


def redacted_chat(chat_id: str) -> str:
    if not chat_id:
        return "disabled"
    if len(chat_id) <= 6:
        return "***"
    return chat_id[:4] + "***"
