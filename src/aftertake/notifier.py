from __future__ import annotations

import json
import ssl
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, Optional


def _number(value: Any, digits: int = 4) -> str:
    try:
        return f"{float(value):.{digits}f}"
    except (TypeError, ValueError):
        return "n/a"


def _positive_number(value: Any, digits: int = 1) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "n/a"
    return f"{number:.{digits}f}" if number >= 0 else "n/a"


def _fill_rate(payload: Dict[str, Any]) -> str:
    try:
        requested = float(payload.get("requested_qty"))
        filled = float(payload.get("filled_qty"))
    except (TypeError, ValueError):
        return "n/a"
    if requested <= 0:
        return "n/a"
    return f"{filled / requested * 100:.2f}%"


def _unfilled_qty(payload: Dict[str, Any]) -> str:
    try:
        return _number(max(0.0, float(payload.get("requested_qty")) - float(payload.get("filled_qty"))))
    except (TypeError, ValueError):
        return "n/a"


def _timestamp_fields(prefix: str, timestamp: Any) -> str:
    try:
        ts = float(timestamp)
    except (TypeError, ValueError):
        return f"{prefix}_utc=n/a {prefix}_ms=n/a"
    if ts <= 0:
        return f"{prefix}_utc=n/a {prefix}_ms=n/a"
    iso = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    return f"{prefix}_utc={iso} {prefix}_ms={int(ts * 1000)}"


def _event_timestamp_fields(payload: Dict[str, Any]) -> str:
    return _timestamp_fields("event_ts", payload.get("event_ts"))


def _notification_timestamp_line(timestamp: float) -> str:
    return _timestamp_fields("notification_ts", timestamp)


def _text(value: Any, default: str = "n/a") -> str:
    return default if value in (None, "") else str(value)


def _market_line(payload: Dict[str, Any], slug: str) -> str:
    return f"Market: slug={slug or _text(payload.get('slug'))} side={_text(payload.get('side'))}"


def _order_line(payload: Dict[str, Any], *, default_status: str = "n/a") -> str:
    fields = [
        f"order={_text(payload.get('order_id'))}",
        f"type={_text(payload.get('order_type'))}",
        f"status={_text(payload.get('status'), default_status)}",
    ]
    submission_state = payload.get("submission_state")
    if submission_state not in (None, ""):
        fields.append(f"submission={submission_state}")
    return "Order: " + " ".join(fields)


def _qty_line(payload: Dict[str, Any]) -> str:
    return (
        f"Qty: requested={_number(payload.get('requested_qty'))} "
        f"filled={_number(payload.get('filled_qty'))} "
        f"unfilled={_unfilled_qty(payload)} fill_rate={_fill_rate(payload)}"
    )


def _price_line(payload: Dict[str, Any], *, label: str) -> str:
    if label == "take":
        requested_price = payload.get("requested_price", payload.get("avg_price"))
    else:
        requested_price = payload.get("requested_price")
    return (
        f"Price: {label}={_number(requested_price)} "
        f"avg={_number(payload.get('avg_price'))} available={_number(payload.get('available_size'))}"
    )


def _timing_line(payload: Dict[str, Any]) -> str:
    return f"Timing: {_event_timestamp_fields(payload)}"


def _latency_line(payload: Dict[str, Any]) -> str:
    fields = [
        f"book_age_ms={_positive_number(payload.get('observed_book_age_ms'))}",
        f"decision_to_submit_ms={_positive_number(payload.get('decision_to_submit_ms'))}",
        f"submit_roundtrip_ms={_positive_number(payload.get('submit_roundtrip_ms'))}",
        f"reconcile_duration_ms={_positive_number(payload.get('reconcile_duration_ms'))}",
        f"itode={str(bool(payload.get('immediate_taker_order_delay_enabled'))).lower()}",
        f"expected_taker_delay_ms={_positive_number(payload.get('expected_taker_delay_ms'))}",
    ]
    if "submit_lag_ms" in payload:
        fields.insert(2, f"submit_lag_ms={_positive_number(payload.get('submit_lag_ms'))}")
    return "Latency: " + " ".join(fields)


def format_event(
    kind: str,
    payload: Dict[str, Any],
    slug: str = "",
    *,
    notification_ts: Optional[float] = None,
) -> str:
    """Format an operator-facing Telegram notification without secrets.

    Every formatted notification carries its own UTC and millisecond-epoch
    timestamp. ``event_ts`` remains separately available for order lifecycle
    observability and is never substituted for notification time.
    """

    message = _format_event(kind, payload, slug)
    sent_at = time.time() if notification_ts is None else notification_ts
    return f"{message}\n{_notification_timestamp_line(sent_at)}"


def _format_event(kind: str, payload: Dict[str, Any], slug: str = "") -> str:

    if kind == "boot":
        return (
            "[Aftertake] BOOT\n"
            f"mode={'SHADOW' if payload['dry_run'] else 'LIVE'} "
            f"qty={_number(payload['qty'])} assets={','.join(payload.get('assets', []))}\n"
            f"pid={_text(payload.get('pid'), 'unknown')} code_sha={_text(payload.get('code_sha'), 'unknown')}\n"
            "multi-asset per-asset risk gates + SQLite recovery + CLOB V2 preflight\n"
            "twap_tail="
            f"strategy_version={_text(payload.get('strategy_version'))} "
            f"decision=T-{_positive_number(payload.get('tail_decision_lead_s'), 3)}s "
            f"leader_bid>{_number(payload.get('leader_bid_threshold'))} "
            f"pm_age<={_positive_number(payload.get('paired_receive_max_age_s'), 3)}s "
            f"binance_age<={_positive_number(payload.get('binance_trade_max_age_s'), 3)}s "
            f"lateness<={_positive_number(payload.get('max_decision_lateness_s'), 3)}s "
            f"limit={_number(payload.get('entry_limit_price'))} "
            f"order={_text(payload.get('order_type'), 'GTC')}"
        )
    if kind == "preflight":
        return (
            "[Aftertake] DEPLOYMENT_CHECK_OK\n"
            f"mode={'SHADOW' if payload['dry_run'] else 'LIVE'} "
            f"qty={_number(payload['qty'])} assets={','.join(payload.get('assets', []))}\n"
            f"slug={payload['slug']} Gamma + CLOB WebSocket + Telegram ready"
        )
    if kind == "ready":
        reason = (
            "account preflight passed; scheduler armed"
            if not payload.get("dry_run")
            else "scheduler armed"
        )
        return (
            "[Aftertake] RUNTIME_READY\n"
            f"mode={'SHADOW' if payload['dry_run'] else 'LIVE'} "
            f"assets={','.join(payload.get('assets', []))}\n"
            f"component=pm_runtime reason={reason}\n"
            "twap_tail="
            f"strategy_version={_text(payload.get('strategy_version'))} "
            f"decision=T-{_positive_number(payload.get('tail_decision_lead_s'), 3)}s "
            f"leader_bid>{_number(payload.get('leader_bid_threshold'))} "
            f"pm_age<={_positive_number(payload.get('paired_receive_max_age_s'), 3)}s "
            f"binance_age<={_positive_number(payload.get('binance_trade_max_age_s'), 3)}s "
            f"lateness<={_positive_number(payload.get('max_decision_lateness_s'), 3)}s "
            f"limit={_number(payload.get('entry_limit_price'))} "
            f"order={_text(payload.get('order_type'), 'GTC')}"
        )
    if kind in {"post_close_snapshot_frozen", "post_close_snapshot_hold"}:
        frozen = kind == "post_close_snapshot_frozen"
        headline = "POST_CLOSE_SNAPSHOT_FROZEN" if frozen else "POST_CLOSE_SNAPSHOT_HOLD"
        return "\n".join(
            [
                f"[Aftertake] {headline}",
                f"Market: slug={slug or _text(payload.get('slug'))} side={_text(payload.get('side'), 'NONE')}",
                "Close: "
                f"close_ts={_text(payload.get('close_ts'))} "
                f"snapshot_ts={_text(payload.get('post_close_snapshot_ts'))} "
                f"decision_ts={_text(payload.get('snapshot_decision_ts'))} "
                f"snapshot_age_ms={_positive_number(payload.get('snapshot_age_ms'))}",
                "Book: "
                f"YES_bid={_number(payload.get('yes', {}).get('best_bid')) if isinstance(payload.get('yes'), dict) else 'n/a'} "
                f"NO_bid={_number(payload.get('no', {}).get('best_bid')) if isinstance(payload.get('no'), dict) else 'n/a'} "
                f"threshold={_number(payload.get('leader_bid_threshold'))}",
                f"Decision: action={_text(payload.get('action'))} reason={_text(payload.get('reason'))} "
                f"strategy={_text(payload.get('strategy_version'))}",
            ]
        )
    if kind == "submitted":
        lines = [
            "[Aftertake] ORDER_SUBMITTED",
            _market_line(payload, slug),
            _order_line(payload, default_status="submitted"),
            _qty_line(payload),
            _price_line(payload, label="limit"),
        ]
        if any(key in payload for key in ("scheduled_close_ts", "actual_submit_ts", "submit_lag_ms")):
            lines.append(
                "Schedule: "
                f"close_ts={_text(payload.get('scheduled_close_ts'))} "
                f"actual_submit_ts={_text(payload.get('actual_submit_ts'), payload.get('event_ts'))} "
                f"submit_lag_ms={_positive_number(payload.get('submit_lag_ms'))}"
            )
        lines.extend((_timing_line(payload), _latency_line(payload)))
        return "\n".join(lines)
    if kind == "recovery":
        return (
            "[Aftertake] ORDER_RECOVERY\n"
            "operator attached an order ID; authenticated identity check pending\n"
            f"order={payload['order_id']} intent={payload['intent_id']} slug={slug or 'unknown'}"
        )
    if kind == "recovery_success":
        lines = [
            "[Aftertake] RECOVERY_SUCCESS",
            f"component={_text(payload.get('component'), 'runtime')}",
            f"reason={_text(payload.get('reason'), 'recovered')}",
        ]
        if payload.get("details") not in (None, ""):
            lines.append(f"details={payload['details']}")
        lines.append(f"slug={slug or 'runtime'}")
        return "\n".join(lines)
    if kind == "entry":
        if payload.get("dry_run"):
            headline = "DRY_RUN_SIMULATED_TAKE"
        else:
            try:
                partial = 0.0 < float(payload.get("filled_qty", 0)) < float(payload.get("requested_qty", 0))
            except (TypeError, ValueError):
                partial = False
            headline = "ENTRY_PARTIAL_CONFIRMED" if partial else "ENTRY_CONFIRMED"
        return "\n".join(
            [
                f"[Aftertake] {headline}",
                _market_line(payload, slug),
                _order_line(payload),
                _qty_line(payload),
                _price_line(payload, label="take"),
                _timing_line(payload),
                _latency_line(payload),
                (
                    "Notes: "
                    f"simulated_take={str(bool(payload.get('simulated_take'))).lower()} "
                    f"dry_run={str(bool(payload.get('dry_run'))).lower()}"
                ),
            ]
        )
    if kind == "order_result":
        lines = [
            "[Aftertake] ORDER_RESULT",
            _market_line(payload, slug),
            _order_line(payload),
            _qty_line(payload),
            _price_line(payload, label="limit"),
            _timing_line(payload),
            _latency_line(payload),
        ]
        reason = payload.get("reason")
        if reason not in (None, ""):
            lines.append(f"Reason: {reason}")
        notes = []
        for key in ("status_code", "error_hint"):
            value = payload.get(key)
            if value not in (None, ""):
                notes.append(f"{key}={value}")
        if notes:
            lines.append("Notes: " + " ".join(notes))
        message = str(payload.get("error_message") or "")
        if message:
            message = " ".join(message.split())
            if len(message) > 300:
                message = message[:300] + "..."
            lines.append(f"Error: {message}")
        return "\n".join(lines)
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
        for key in (
            "component",
            "asset",
            "phase",
            "generation",
            "reconnect_count",
            "consecutive_failures",
            "error_type",
            "status_code",
            "error_hint",
            "order_type",
            "submission_state",
            "order_id",
        ):
            value = payload.get(key)
            if value not in (None, ""):
                lines.append(f"{key}={value}")
        message = str(payload.get("error_message") or "")
        if message:
            message = " ".join(message.split())
            if len(message) > 400:
                message = message[:400] + "..."
            lines.append(f"error_message={message}")
        if any(
            payload.get(k, -1) not in (None, "", -1, -1.0)
            for k in (
                "decision_to_submit_ms",
                "submit_roundtrip_ms",
                "reconcile_duration_ms",
                "observed_book_age_ms",
            )
        ):
            lines.append(_timing_line(payload))
            lines.append(_latency_line(payload))
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
