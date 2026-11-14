"""Which of our apps sent the request being served, for telemetry.

Turns record ``web`` for the browser, the desktop app and the phone app
alike, so nothing else on the server can tell them apart. The user agent
can: the desktop app is Electron, the phone app's native networking names
CFNetwork or okhttp and never Mozilla, and ``la`` names itself.

Telemetry only. A client is never authorization, since any caller can send
any user agent.
"""

from __future__ import annotations

import re
from contextvars import ContextVar
from typing import Optional

from src.server.models.computer_cause import Client

_client: ContextVar[Optional[Client]] = ContextVar("request_client", default=None)

_NATIVE_MOBILE = re.compile(r"CFNetwork|Darwin/|okhttp|Expo", re.IGNORECASE)


def classify_user_agent(user_agent: Optional[str]) -> Optional[Client]:
    """``browser``, ``desktop``, ``mobile`` or ``cli`` for *user_agent*; None
    for anything else (a script, a server, the chat-app gateway)."""
    if not user_agent:
        return None
    if user_agent.startswith("langalpha-cli/"):
        return "cli"
    if "Electron/" in user_agent:
        return "desktop"
    if user_agent.startswith("Mozilla/"):
        return "browser"
    if _NATIVE_MOBILE.search(user_agent):
        return "mobile"
    return None


def request_client() -> Optional[Client]:
    """The client of the request this task serves, None outside one."""
    return _client.get()


class RequestClientMiddleware:
    """Sets :func:`request_client` for every HTTP request.

    Pure ASGI, so the value is set in the task the endpoint runs in and is
    copied into every task it starts, a background start included.
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        user_agent = None
        for name, value in scope.get("headers") or ():
            if name == b"user-agent":
                user_agent = value.decode("latin-1")
                break
        token = _client.set(classify_user_agent(user_agent))
        try:
            await self.app(scope, receive, send)
        finally:
            _client.reset(token)
