"""Where an automation's runs deliver, as the automation editor reads and
sets it.

The messaging service knows the chats each linked app offers, and keeps each
workspace's default chat on each app: the one an entry naming only the app
delivers to. This asks it and reads its answers. A call that doesn't land
raises one of the errors below with the sentence the user is shown, and the
REST routes answer each as a status. The run's own delivery, its start and
its finish, is ``automation_delivery``'s.

Home is none of the user's workspaces: its automations deliver as a run with
no workspace does, so it has no default chat. The workspace these calls take
is None for Home as for an automation with no workspace.
"""

from __future__ import annotations

import logging
from typing import Any, Optional

import httpx

from src.tools.messaging import tools as messaging

logger = logging.getLogger(__name__)

# Listing the chats asks each linked app; saving a default checks the chat.
_TIMEOUT = httpx.Timeout(20.0, connect=5.0)

_HOME_HAS_NO_DEFAULT = (
    "Home has no default chat: its automations deliver to each app's preferred chat."
)


class Unavailable(Exception):
    """The messaging service couldn't be reached, failed, or answered
    something that could not be read."""


class NoService(Exception):
    """This server has no messaging service to keep a default."""


class Busy(Exception):
    """The default is being changed elsewhere; asking again may land."""


class Refused(Exception):
    """A default that can't be saved, with each problem as the messaging
    service listed it."""

    def __init__(self, message: str, problems: list[Any]) -> None:
        super().__init__(message)
        self.problems = problems


def _message(answer: messaging.GatewayAnswer, fallback: str) -> str:
    message = (answer.data or {}).get("message")
    return message if isinstance(message, str) and message else fallback


async def options(user_id: str, workspace_id: Optional[str]) -> dict[str, Any]:
    """The chats each linked app offers an automation's runs, and the one an
    entry naming only the app reaches, for an automation in ``workspace_id``
    or in none. ``workspace_id`` comes back as the workspace the answer was
    made for; ``enabled`` is false, with no apps, on a server with no
    messaging service.

    Raises:
        Unavailable: the messaging service gave no answer to read
    """
    if not messaging.messaging_enabled():
        return {"enabled": False, "apps": {}, "workspace_id": workspace_id}
    try:
        answer = await messaging.gateway_request(
            "GET",
            "/agent/automation-targets",
            user_id=user_id,
            timeout=_TIMEOUT,
            params={"workspace_id": workspace_id} if workspace_id else None,
        )
    except messaging.GatewayError as e:
        raise Unavailable(e.message) from e
    if answer.status != 200:
        logger.warning(f"[AUTOMATIONS] Delivery options answered {answer.status}")
        raise Unavailable(_message(answer, f"The messaging service failed ({answer.status})."))
    apps = (answer.data or {}).get("apps")
    if not isinstance(apps, dict):
        raise Unavailable("The messaging service sent an answer that could not be read.")
    return {"enabled": True, "apps": apps, "workspace_id": workspace_id}


async def set_default(
    user_id: str, workspace_id: Optional[str], platform: str, address: Optional[str]
) -> dict[str, Any]:
    """Set the chat on ``platform`` that ``workspace_id``'s automations
    deliver to when an entry names only the app, or clear it with a null
    ``address``. Answers the chat as saved. Home has none to set, which is
    refused before the messaging service is asked.

    Raises:
        Refused: Home, or a chat the messaging service refused
        NoService: no messaging service is configured
        Busy: the default is being changed elsewhere
        Unavailable: the messaging service gave no answer to read
    """
    if workspace_id is None:
        raise Refused(
            _HOME_HAS_NO_DEFAULT,
            [{"field": "workspace_id", "message": _HOME_HAS_NO_DEFAULT}],
        )
    if not messaging.messaging_enabled():
        raise NoService("No messaging service is configured.")
    try:
        answer = await messaging.gateway_request(
            "PUT",
            "/agent/automation-output",
            user_id=user_id,
            timeout=_TIMEOUT,
            body={"workspace_id": workspace_id, "platform": platform, "address": address},
        )
    except messaging.GatewayError as e:
        raise Unavailable(e.message) from e
    data = answer.data or {}
    if answer.status == 200 and answer.data is not None:
        return {"address": data.get("address"), "name": data.get("name")}
    if answer.status == 400:
        problems = data.get("problems")
        raise Refused(
            _message(answer, "The default was not saved."),
            problems if isinstance(problems, list) else [],
        )
    if answer.status == 409:
        raise Busy(_message(answer, "The default is being changed elsewhere. Try again."))
    logger.warning(f"[AUTOMATIONS] Delivery default answered {answer.status}")
    if answer.status == 200:
        raise Unavailable("The messaging service sent an answer that could not be read.")
    raise Unavailable(_message(answer, f"The messaging service failed ({answer.status})."))
