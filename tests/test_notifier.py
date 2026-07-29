import json
import re
import urllib.parse

import pytest

from aftertake.notifier import Notifier, format_event


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def read(self):
        return json.dumps(self.payload).encode()


def test_notifier_posts_aftertake_environment_to_telegram_and_requires_ok(monkeypatch):
    captured = {}

    def fake_urlopen(request, timeout):
        captured["url"] = request.full_url
        captured["body"] = urllib.parse.parse_qs(request.data.decode())
        captured["timeout"] = timeout
        return FakeResponse({"ok": True, "result": {"message_id": 1}})

    monkeypatch.setattr("aftertake.notifier.urllib.request.urlopen", fake_urlopen)

    assert Notifier(token="bot-token", chat_id="-100123").send("hello") is True
    assert captured["url"].endswith("/botbot-token/sendMessage")
    assert captured["body"] == {"chat_id": ["-100123"], "text": ["hello"]}
    assert captured["timeout"] == 15


def test_notifier_rejects_telegram_level_error_even_on_http_success(monkeypatch):
    monkeypatch.setattr(
        "aftertake.notifier.urllib.request.urlopen",
        lambda *args, **kwargs: FakeResponse({"ok": False, "description": "chat not found"}),
    )

    with pytest.raises(RuntimeError, match="chat not found"):
        Notifier(token="bot-token", chat_id="missing").send("hello")


def test_notifier_sanitizes_transport_error_that_contains_token_url(monkeypatch):
    def fail(*args, **kwargs):
        raise OSError("https://api.telegram.org/botSUPER-SECRET/sendMessage")

    monkeypatch.setattr("aftertake.notifier.urllib.request.urlopen", fail)

    with pytest.raises(RuntimeError) as exc_info:
        Notifier(token="SUPER-SECRET", chat_id="chat").send("hello")
    assert "SUPER-SECRET" not in str(exc_info.value)


@pytest.mark.parametrize(
    ("kind", "payload", "headline"),
    [
        (
            "order_result",
            {
                "side": "YES",
                "status": "canceled",
                "filled_qty": 0,
                "avg_price": 0,
                "submission_state": "acknowledged",
                "order_id": "order-1",
            },
            "ORDER_RESULT",
        ),
        (
            "settle",
            {
                "side": "YES",
                "win": True,
                "pm_up": True,
                "qty": 5,
                "pnl": 1.9,
                "entry_price": 0.6,
                "entry_fee": 0.1,
            },
            "SETTLE",
        ),
    ],
)
def test_operator_lifecycle_messages_are_stable(kind, payload, headline):
    text = format_event(kind, payload, "btc-updown-5m-0")

    assert text.startswith(f"[Aftertake] {headline}")
    assert "btc-updown-5m-0" in text
    assert "token" not in text.lower()


@pytest.mark.parametrize(
    ("kind", "payload", "slug"),
    [
        ("boot", {"dry_run": True, "qty": 5, "assets": ("BTC",)}, ""),
        ("preflight", {"dry_run": True, "qty": 5, "assets": ("BTC",), "slug": "btc-updown-5m-0"}, ""),
        (
            "submitted",
            {"side": "YES", "requested_qty": 5, "requested_price": 0.64, "order_id": "order-1"},
            "btc-updown-5m-0",
        ),
        ("recovery", {"order_id": "order-1", "intent_id": "intent-1"}, "btc-updown-5m-0"),
        (
            "entry",
            {
                "side": "YES",
                "filled_qty": 5,
                "avg_price": 0.64,
                "requested_qty": 5,
                "status": "matched",
                "order_id": "order-1",
            },
            "btc-updown-5m-0",
        ),
        (
            "order_result",
            {"side": "YES", "status": "canceled", "filled_qty": 0, "avg_price": 0, "submission_state": "acknowledged"},
            "btc-updown-5m-0",
        ),
        ("blocked", {"reason": "risk_rejected"}, "btc-updown-5m-0"),
        ("round", {"ticks": 1, "decisions": 1, "last_reason": "hold"}, "btc-updown-5m-0"),
        (
            "settle",
            {"side": "YES", "win": True, "pm_up": True, "qty": 5, "pnl": 1.0, "entry_price": 0.64, "entry_fee": 0.1},
            "btc-updown-5m-0",
        ),
        ("alert", {"reason": "runtime_unavailable"}, "btc-updown-5m-0"),
    ],
)
def test_every_formatted_notification_includes_utc_and_millisecond_epoch(kind, payload, slug):
    text = format_event(kind, payload, slug, notification_ts=1785189900.123)

    assert re.search(
        r"notification_ts_utc=2026-07-27T\d{2}:\d{2}:\d{2}\.123Z notification_ts_ms=1785189900123$",
        text,
    )


def test_dry_run_entry_message_reports_simulated_take_price_and_available_size():
    text = format_event(
        "entry",
        {
            "side": "NO",
            "filled_qty": 5,
            "avg_price": 0.64,
            "requested_price": 0.64,
            "requested_qty": 5,
            "available_size": 18,
            "status": "shadow_fill",
            "order_id": "shadow-abc",
            "dry_run": True,
            "simulated_take": True,
        },
        "btc-updown-5m-0",
    )

    assert text.startswith("[Aftertake] DRY_RUN_SIMULATED_TAKE")
    assert "Price: take=0.6400 avg=0.6400 available=18.0000" in text
    assert "Notes: simulated_take=true dry_run=true" in text
    assert "token" not in text.lower()


def test_deployment_check_notification_is_not_a_strategy_signal():
    text = format_event(
        "preflight",
        {"dry_run": False, "qty": 5, "slug": "btc-updown-5m-0"},
    )

    assert "DEPLOYMENT_CHECK_OK" in text
    assert "SIGNAL" not in text


def test_alert_formatter_expands_submit_exception_diagnostics():
    text = format_event(
        "alert",
        {
            "reason": "submit_exception",
            "error_type": "PolyApiException",
            "status_code": 400,
            "error_hint": "order_type_compatibility",
            "order_type": "FAK",
            "submission_state": "unknown",
            "order_id": "n/a",
            "error_message": "invalid order type FAK",
        },
        "btc-updown-5m-1",
    )

    assert "reason=submit_exception" in text
    assert "error_type=PolyApiException" in text
    assert "status_code=400" in text
    assert "error_hint=order_type_compatibility" in text
    assert "order_type=FAK" in text
    assert "order_id=n/a" in text
    assert "error_message=invalid order type FAK" in text


def test_boot_formatter_uses_multi_asset_wording():
    code_sha = "a" * 40
    text = format_event(
        "boot",
        {
            "dry_run": False,
            "qty": 5,
            "assets": ("BTC", "ETH", "XRP", "HYPE", "SOL"),
            "pid": 4242,
            "code_sha": code_sha,
        },
    )

    assert "assets=BTC,ETH,XRP,HYPE,SOL" in text
    assert "pid=4242" in text
    assert f"code_sha={code_sha}" in text
    assert "multi-asset per-asset risk gates" in text
    assert "one-entry-per-market" not in text


def test_order_result_includes_no_match_diagnostics_and_requested_context():
    text = format_event(
        "order_result",
        {
            "side": "YES",
            "status": "no_fill",
            "filled_qty": 0,
            "avg_price": 0,
            "submission_state": "venue_no_match",
            "order_id": "",
            "reason": "fak_no_matching_resting_order",
            "order_type": "FAK",
            "status_code": 400,
            "requested_qty": 50,
            "requested_price": 0.99,
            "available_size": 509.46,
            "error_message": "no orders found to match with FAK order",
            "event_ts": 1785189900.123,
            "decision_to_submit_ms": 73.4,
            "submit_roundtrip_ms": 112.8,
            "reconcile_duration_ms": 68.5,
            "observed_book_age_ms": 146.2,
            "immediate_taker_order_delay_enabled": True,
            "expected_taker_delay_ms": 250.0,
        },
        "btc-updown-5m-1785167400",
        notification_ts=1785189901.456,
    )

    assert text.splitlines() == [
        "[Aftertake] ORDER_RESULT",
        "Market: slug=btc-updown-5m-1785167400 side=YES",
        "Order: order=n/a type=FAK status=no_fill submission=venue_no_match",
        "Qty: requested=50.0000 filled=0.0000 unfilled=50.0000 fill_rate=0.00%",
        "Price: limit=0.9900 avg=0.0000 available=509.4600",
        "Timing: event_ts_utc=2026-07-27T22:05:00.123Z event_ts_ms=1785189900123",
        "Latency: book_age_ms=146.2 decision_to_submit_ms=73.4 submit_roundtrip_ms=112.8 reconcile_duration_ms=68.5 "
        "itode=true expected_taker_delay_ms=250.0",
        "Reason: fak_no_matching_resting_order",
        "Notes: status_code=400",
        "Error: no orders found to match with FAK order",
        "notification_ts_utc=2026-07-27T22:05:01.456Z notification_ts_ms=1785189901456",
    ]


def test_live_partial_entry_message_reports_fill_rate_unfilled_and_latency():
    text = format_event(
        "entry",
        {
            "side": "YES",
            "filled_qty": 2.7,
            "avg_price": 0.99,
            "requested_price": 0.99,
            "requested_qty": 12,
            "available_size": 12.81,
            "status": "canceled",
            "order_id": "order-live",
            "dry_run": False,
            "simulated_take": False,
            "decision_to_submit_ms": 73.4,
            "submit_roundtrip_ms": 112.8,
            "reconcile_duration_ms": 68.5,
            "observed_book_age_ms": 146.2,
            "event_ts": 1785189900.123,
            "immediate_taker_order_delay_enabled": True,
            "expected_taker_delay_ms": 250.0,
        },
        "sol-updown-5m-1785189900",
        notification_ts=1785189901.456,
    )

    assert text.splitlines() == [
        "[Aftertake] ENTRY_PARTIAL_CONFIRMED",
        "Market: slug=sol-updown-5m-1785189900 side=YES",
        "Order: order=order-live type=n/a status=canceled",
        "Qty: requested=12.0000 filled=2.7000 unfilled=9.3000 fill_rate=22.50%",
        "Price: take=0.9900 avg=0.9900 available=12.8100",
        "Timing: event_ts_utc=2026-07-27T22:05:00.123Z event_ts_ms=1785189900123",
        "Latency: book_age_ms=146.2 decision_to_submit_ms=73.4 submit_roundtrip_ms=112.8 reconcile_duration_ms=68.5 "
        "itode=true expected_taker_delay_ms=250.0",
        "Notes: simulated_take=false dry_run=false",
        "notification_ts_utc=2026-07-27T22:05:01.456Z notification_ts_ms=1785189901456",
    ]


def test_submitted_message_uses_compact_ordered_sections():
    text = format_event(
        "submitted",
        {
            "side": "YES",
            "requested_qty": 12,
            "requested_price": 0.99,
            "order_id": "order-submit",
            "event_ts": 1785189900.123,
            "decision_to_submit_ms": 73.4,
            "submit_roundtrip_ms": 112.8,
            "observed_book_age_ms": 146.2,
            "immediate_taker_order_delay_enabled": True,
            "expected_taker_delay_ms": 250.0,
        },
        "sol-updown-5m-1785189900",
        notification_ts=1785189901.456,
    )

    assert text.splitlines() == [
        "[Aftertake] ORDER_SUBMITTED",
        "Market: slug=sol-updown-5m-1785189900 side=YES",
        "Order: order=order-submit type=n/a status=submitted",
        "Qty: requested=12.0000 filled=n/a unfilled=n/a fill_rate=n/a",
        "Price: limit=0.9900 avg=n/a available=n/a",
        "Timing: event_ts_utc=2026-07-27T22:05:00.123Z event_ts_ms=1785189900123",
        "Latency: book_age_ms=146.2 decision_to_submit_ms=73.4 submit_roundtrip_ms=112.8 reconcile_duration_ms=n/a "
        "itode=true expected_taker_delay_ms=250.0",
        "notification_ts_utc=2026-07-27T22:05:01.456Z notification_ts_ms=1785189901456",
    ]
    assert "expected_taker_delay_ms=250.0" in text
    assert "token" not in text.lower()
