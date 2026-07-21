import json
import urllib.parse

import pytest

from misprice_pm.notifier import Notifier, format_event


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def read(self):
        return json.dumps(self.payload).encode()


def test_notifier_posts_cor_pol_environment_to_telegram_and_requires_ok(monkeypatch):
    captured = {}

    def fake_urlopen(request, timeout):
        captured["url"] = request.full_url
        captured["body"] = urllib.parse.parse_qs(request.data.decode())
        captured["timeout"] = timeout
        return FakeResponse({"ok": True, "result": {"message_id": 1}})

    monkeypatch.setattr("misprice_pm.notifier.urllib.request.urlopen", fake_urlopen)

    assert Notifier(token="bot-token", chat_id="-100123").send("hello") is True
    assert captured["url"].endswith("/botbot-token/sendMessage")
    assert captured["body"] == {"chat_id": ["-100123"], "text": ["hello"]}
    assert captured["timeout"] == 15


def test_notifier_rejects_telegram_level_error_even_on_http_success(monkeypatch):
    monkeypatch.setattr(
        "misprice_pm.notifier.urllib.request.urlopen",
        lambda *args, **kwargs: FakeResponse({"ok": False, "description": "chat not found"}),
    )

    with pytest.raises(RuntimeError, match="chat not found"):
        Notifier(token="bot-token", chat_id="missing").send("hello")


def test_notifier_sanitizes_transport_error_that_contains_token_url(monkeypatch):
    def fail(*args, **kwargs):
        raise OSError("https://api.telegram.org/botSUPER-SECRET/sendMessage")

    monkeypatch.setattr("misprice_pm.notifier.urllib.request.urlopen", fail)

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

    assert text.startswith(f"[Misprice PM] {headline}")
    assert "btc-updown-5m-0" in text
    assert "token" not in text.lower()
