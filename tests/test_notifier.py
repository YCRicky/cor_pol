import json
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
    assert "take_price=0.6400" in text
    assert "available_size=18.0000" in text
    assert "simulated_take=true" in text
    assert "dry_run=true" in text
    assert "token" not in text.lower()


def test_deployment_check_notification_is_not_a_strategy_signal():
    text = format_event(
        "preflight",
        {"dry_run": False, "qty": 5, "slug": "btc-updown-5m-0"},
    )

    assert "DEPLOYMENT_CHECK_OK" in text
    assert "SIGNAL" not in text
