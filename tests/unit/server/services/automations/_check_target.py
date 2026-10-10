"""The channel gateway's ``POST /agent/check-target``, faked at the HTTP
transport, for the tests of an automation's delivery chats."""

from __future__ import annotations

import json
from collections.abc import Callable

import httpx

CHAT = "slack:T1/C0123"
# The same chat as a writer may spell it, which the gateway files as CHAT.
CHAT_SPELLED = "slack:t1/c0123"
REFUSED_CHAT = "slack:T1/CNOPE"
REFUSAL = "the bot is not in that channel"
# The user's direct messages by address, which pins them where an app name
# would follow the app's chain; the gateway files them as written.
DISCORD_DM = "discord:@me"
SLACK_DM = "slack:T1"
_AS_WRITTEN = (DISCORD_DM, SLACK_DM)
# An app the user hasn't linked, which the messaging service refuses by name.
UNLINKED_APP = "telegram"
UNLINKED = "Telegram isn't linked to your account"


class FakeCheckTarget:
    def __init__(self) -> None:
        self.asked: list[str] = []
        # What ``at`` answered as each check ran, for a test that pins when.
        self.seen: list[object] = []
        self.at: Callable[[], object] = lambda: None
        self.error: Exception | None = None

    def handle(self, request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.url.path == "/api/prefix/agent/check-target"
        assert request.headers["X-Service-Token"] == "svc-token"
        body = json.loads(request.content)
        assert body["purpose"] == "automation"
        self.asked.append(body["address"])
        self.seen.append(self.at())
        if self.error is not None:
            raise self.error
        if body["address"].startswith(REFUSED_CHAT) or body["address"] == UNLINKED_APP:
            message = UNLINKED if body["address"] == UNLINKED_APP else REFUSAL
            return httpx.Response(
                200,
                json={"ok": False, "address": None, "name": None, "message": message},
            )
        # An app name resolves to a chat too, which the save must not keep.
        canonical = body["address"] if body["address"] in _AS_WRITTEN else CHAT
        return httpx.Response(
            200,
            json={"ok": True, "address": canonical, "name": "#alerts", "message": None},
        )
