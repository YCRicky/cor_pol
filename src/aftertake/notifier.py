from __future__ import annotations

import json
import ssl
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
            "[Aftertake] BOOT\n"
            f"mode={'SHADOW' if payload['dry_run'] else 'LIVE'} "
            f"qty={_number(payload['qty'])} assets={','.join(payload.get('assets', []))}\n"
            "multi-asset per-asset risk gates + SQLite recovery + CLOB V2 preflight"
        )
    if kind == "preflight":
        return (
            "[Aftertake] DEPLOYMENT_CHECK_OK\n"
            f"mode={'SHADOW' if payload['dry_run'] else 'LIVE'} "
            f"qty={_number(payload['qty'])} assets={','.join(payload.get('assets', []))}\n"
            f"slug={payload['slug']} Gamma + CLOB WebSocket + Telegram ready"
        )
    if kind == "submitted":
        return (
            "[Aftertake] ORDER_SUBMITTED\n"
            f"{payload['side']} qty={_number(payload['requested_qty'])} "
            f"limit={_number(payload['requested_price'])}\n"
            f"order={payload['order_id']} slug={slug}"
        )
    if kind == "recovery":
        return (
            "[Aftertake] ORDER_RECOVERY\n"
            "operator attached an order ID; authenticated identity check pending\n"
            f"order={payload['order_id']} intent={payload['intent_id']} slug={slug or 'unknown'}"
        )
    if kind == "entry":
        headline = "DRY_RUN_SIMULATED_TAKE" if payload.get("dry_run") else "ENTRY_CONFIRMED"
        return (
            f"[Aftertake] {headline}\n"
            f"{payload['side']} qty={_number(payload['requested_qty'])} "
            f"take_price={_number(payload.get('requested_price', payload.get('avg_price')))} "
            f"available_size={_number(payload.get('available_size'))}\n"
            f"simulated_take={str(bool(payload.get('simulated_take'))).lower()} "
            f"dry_run={str(bool(payload.get('dry_run'))).lower()}\n"
            f"filled={_number(payload['filled_qty'])} avg={_number(payload['avg_price'])} "
            f"status={payload['status']} order={payload['order_id']} slug={slug}"
        )
    if kind == "order_result":
        return (
            "[Aftertake] ORDER_RESULT\n"
            f"{payload['side']} status={payload['status']} "
            f"filled={_number(payload['filled_qty'])} "
            f"avg={_number(payload['avg_price'])}\n"
            f"submission={payload['submission_state']} "
            f"order={payload.get('order_id') or 'n/a'} slug={slug}"
        )
    if kind == "blocked":
        return (
            "[Aftertake] ENTRY_BLOCKED\n"
            f"reason={payload['reason']}\nslug={slug}"
        )
    if kind == "round":
        return (
            "[Aftertake] NO_ENTRY\n"
            f"ticks={payload['ticks']} decisions={payload['decisions']} "
            f"last_reason={payload['last_reason']}\nslug={slug}"
        )
    if kind == "settle":
        return (
            "[Aftertake] SETTLE\n"
            f"{payload['side']} result={'WIN' if payload['win'] else 'LOSS'} "
            f"pm_up={payload['pm_up']} "
            f"qty={_number(payload['qty'])} pnl=${float(payload['pnl']):+.2f}\n"
            f"entry={_number(payload['entry_price'])} "
            f"fee=${_number(payload['entry_fee'], 4)} slug={slug}"
        )
    if kind == "alert":
        lines = ["[Aftertake] ALERT", f"reason={payload['reason']}"]
        for key in ("error_type", "status_code", "error_hint", "order_type", "submission_state", "order_id"):
            value = payload.get(key)
            if value not in (None, ""):
                lines.append(f"{key}={value}")
        message = str(payload.get("error_message") or "")
        if message:
            message = " ".join(message.split())
            if len(message) > 400:
                message = message[:400] + "..."
            lines.append(f"error_message={message}")
        lines.append(f"slug={slug or 'runtime'}")
        return "\n".join(lines)
    raise ValueError(f"unsupported notification event: {kind}")


class Notifier:
    def __init__(self, *, token: str = "", chat_id: str = ""):
        self.token = token
        self.chat_id = chat_id
        self._ssl_context = ssl.create_default_context(cafile=self._certifi_ca())

    @staticmethod
    def _certifi_ca() -> str | None:
        try:
            import certifi  # type: ignore

            return str(certifi.where())
        except Exception:
            return None

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
            try:
                response = urllib.request.urlopen(req, timeout=15, context=self._ssl_context)
            except TypeError:
                # Preserve compatibility with tests or custom monkeypatches that implement
                # urlopen(request, timeout) only.
                response = urllib.request.urlopen(req, timeout=15)
            with response as resp:
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
