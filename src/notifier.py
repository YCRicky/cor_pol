from __future__ import annotations

import json
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Optional


@dataclass
class TelegramConfig:
    bot_token: str
    chat_id: str
    thread_id: Optional[str] = None
    silent: bool = False


class TelegramNotifier:
    def __init__(self, config: TelegramConfig):
        self.config = config

    def send(self, text: str) -> dict:
        url = f"https://api.telegram.org/bot{self.config.bot_token}/sendMessage"
        payload = {
            'chat_id': self.config.chat_id,
            'text': text,
            'disable_notification': self.config.silent,
        }
        if self.config.thread_id:
            payload['message_thread_id'] = self.config.thread_id
        body = urllib.parse.urlencode(payload).encode()
        req = urllib.request.Request(url, data=body, method='POST')
        with urllib.request.urlopen(req, timeout=20) as resp:
            return json.loads(resp.read().decode())
