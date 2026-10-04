"""An automation run that delivers through the messaging service.

With a messaging service configured, an automation's results are the run's
own to send. Before the turn starts, the service resolves each delivery entry
to one chat and holds the run; the agent is told where to send and does so
with ``send_message``. When the run settles, the service posts the final
answer to every chat the agent didn't reach, or a short notice to all of them
when the run failed or was stopped, and answers where each one landed, which
becomes the execution's ``delivery_result``. A finish that got no answer, or
found the service briefly unavailable, is worth asking again
(``Finish.retry``): the service ends a run once and answers the same record
to every later ask.

A run the service never took (no service, no entries, the start failed, or
no entry resolved) delivers through the webhook (``webhook_client``) instead,
exactly as before. Which way a run delivers is decided once, at its start,
and stamped on its run row (``DELIVERY_KEY``), so the settle, which may run on
another worker, follows the same way.

Nothing here raises: a delivery problem is never worth failing a run or a
settle for, and each failure falls back or is recorded as one.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, Literal, Optional

import httpx

from src.tools.messaging import tools as messaging

logger = logging.getLogger(__name__)

#: The run-row metadata key a run delivering through the messaging service
#: carries, holding the targets its start resolved.
DELIVERY_KEY = "automation_delivery"

FinishStatus = Literal["completed", "failed", "stopped"]

# A start resolves every entry and checks each chat with its app.
_START_TIMEOUT = httpx.Timeout(20.0, connect=5.0)
# A finish posts to every chat the run didn't reach. It runs in a settle's
# tail, which shutdown waits on for about one webhook's timeout.
_FINISH_TIMEOUT = httpx.Timeout(15.0, connect=5.0)

# What a fallback carries: the same cap ``send_message`` puts on a message.
_FINAL_TEXT_CHARS = messaging.MAX_TEXT_CHARS

#: The wait before each further ask after a finish worth retrying: three
#: asks in all.
FINISH_RETRY_DELAYS: tuple[float, ...] = (2.0, 5.0)
# The service, or the proxy in front of it, unavailable for a moment. Any
# other status is an answer, and asking again would get the same one.
_PASSING_STATUSES = frozenset({502, 503, 504})

_APP_NAMES = {
    "slack": "Slack",
    "discord": "Discord",
    "telegram": "Telegram",
    "imessage": "iMessage",
    "feishu": "Feishu",
}
_VIAS = frozenset({"agent", "fallback", "notice"})


@dataclass(frozen=True)
class Target:
    """One delivery entry as the run's start resolved it."""

    entry: str
    address: Optional[str]
    name: Optional[str]
    ok: bool
    message: Optional[str] = None

    def as_dict(self) -> Dict[str, Any]:
        return {
            "entry": self.entry,
            "address": self.address,
            "name": self.name,
            "ok": self.ok,
            "message": self.message,
        }

    @classmethod
    def read(cls, data: Any) -> Optional["Target"]:
        if not isinstance(data, dict) or not isinstance(data.get("entry"), str):
            return None
        return cls(
            entry=data["entry"],
            address=_text(data.get("address")),
            name=_text(data.get("name")),
            ok=data.get("ok") is True,
            message=_text(data.get("message")),
        )


def _text(value: Any) -> Optional[str]:
    return value if isinstance(value, str) and value else None


def _entries(automation: Dict[str, Any]) -> list[str]:
    config = automation.get("delivery_config")
    methods = config.get("methods") if isinstance(config, dict) else None
    return [m for m in methods or [] if isinstance(m, str) and m]


# -- start ------------------------------------------------------------------


async def start_run(
    automation: Dict[str, Any], execution_id: str, workspace_id: Optional[str]
) -> Optional[list[Target]]:
    """The run's targets once the messaging service holds the run; None when
    the run delivers through the webhook instead.

    Only a run with delivery entries on a server with a messaging service
    asks. A start that fails, or one that resolved no entry to a chat, is
    left to the webhook. The service files the run under ``execution_id``, so
    asking again for the same firing answers the targets it already holds.
    """
    entries = _entries(automation)
    if not entries or not messaging.messaging_enabled():
        return None
    try:
        answer = await messaging.gateway_request(
            "POST",
            "/agent/automation-runs",
            user_id=automation["user_id"],
            timeout=_START_TIMEOUT,
            body={
                "execution_id": execution_id,
                "workspace_id": workspace_id,
                "automation_name": automation.get("name") or "",
                "entries": entries,
            },
        )
    except messaging.GatewayError as e:
        logger.warning(
            f"[AUTOMATION_DELIVERY] Start failed, delivering by webhook: "
            f"execution_id={execution_id} error={e.message}"
        )
        return None
    except Exception as e:
        logger.error(
            f"[AUTOMATION_DELIVERY] Start failed, delivering by webhook: "
            f"execution_id={execution_id} error={e!r}"
        )
        return None
    raw = (answer.data or {}).get("targets")
    if answer.status != 200 or not isinstance(raw, list):
        logger.warning(
            f"[AUTOMATION_DELIVERY] Start answered {answer.status}, delivering by "
            f"webhook: execution_id={execution_id}"
        )
        return None
    targets = [t for t in (Target.read(item) for item in raw) if t is not None]
    if not any(t.ok for t in targets):
        logger.info(
            f"[AUTOMATION_DELIVERY] No entry resolved to a chat, delivering by "
            f"webhook: execution_id={execution_id}"
        )
        return None
    logger.info(
        f"[AUTOMATION_DELIVERY] The run sends its own results: "
        f"execution_id={execution_id} targets={sum(t.ok for t in targets)}/{len(targets)}"
    )
    return targets


def run_metadata(targets: list[Target]) -> Dict[str, Any]:
    """The run-row stamp a settle reads to follow this run's way of delivery."""
    return {DELIVERY_KEY: {"targets": [t.as_dict() for t in targets]}}


def targets_of_run(run: Optional[Dict[str, Any]]) -> Optional[list[Target]]:
    """The targets a run's start resolved, by its run row; None when the run
    delivers through the webhook."""
    metadata = (run or {}).get("metadata") or {}
    stamp = metadata.get(DELIVERY_KEY) if isinstance(metadata, dict) else None
    if not isinstance(stamp, dict):
        return None
    raw = stamp.get("targets")
    return [t for t in (Target.read(i) for i in raw or []) if t is not None]


def _app_name(address: str) -> str:
    app = address.split(":", 1)[0].lower()
    return _APP_NAMES.get(app, app.capitalize())


def _is_dm(address: str) -> bool:
    """``<app>:@me`` (or the bare app), or ``slack:<team>`` on Slack."""
    app, _, rest = address.partition(":")
    return rest in ("", "@me") or (app.lower() == "slack" and "/" not in rest)


def _describe(target: Target, address: str) -> str:
    app = _app_name(address)
    if _is_dm(address):
        return f"your {app} DM `{address}`"
    if target.name:
        return f"{target.name} ({app}) `{address}`"
    return f"{app} `{address}`"


def reminder(targets: list[Target]) -> str:
    """What the run's agent is told about its delivery: where to send when it
    finishes. An entry that didn't resolve is the run record's to name, not
    the agent's to try."""
    reached = ", ".join(_describe(t, t.address) for t in targets if t.ok and t.address)
    return (
        f"When you finish, send the result with send_message to: {reached}. "
        "Write it for chat; attach files for detail."
    )


# -- finish -----------------------------------------------------------------


@dataclass(frozen=True)
class Finish:
    """One ask to end a run: the ``delivery_result`` it gives, one item per
    target, and whether asking again may land where this one didn't."""

    result: list[Dict[str, Any]]
    retry: bool = False


def _attempt(data: Dict[str, Any]) -> Dict[str, Any]:
    """One target of the finish's answer as a ``delivery_result`` item."""
    via = data.get("via") if data.get("via") in _VIAS else None
    return {
        "method": str(data.get("entry") or ""),
        "address": _text(data.get("address")),
        "name": _text(data.get("name")),
        # Reached by the agent, or posted to by the finish.
        "success": data.get("reached") is True or via is not None,
        "via": via,
        "error": _text(data.get("error")),
    }


def _unconfirmed(
    automation: Dict[str, Any], targets: list[Target], why: str
) -> list[Dict[str, Any]]:
    """Every target as failed, for a finish that gave no answer to read."""
    error = f"Delivery couldn't be confirmed. {why}"
    if not targets:
        targets = [Target(entry=e, address=None, name=None, ok=True) for e in _entries(automation)]
    return [
        {
            "method": t.entry,
            "address": t.address,
            "name": t.name,
            "success": False,
            "via": None,
            # A target the start refused failed for its own reason.
            "error": error if t.ok else (t.message or error),
        }
        for t in targets
    ]


async def finish_run(
    automation: Dict[str, Any],
    execution_id: str,
    status: FinishStatus,
    *,
    targets: list[Target],
    final_text: Optional[str] = None,
) -> Finish:
    """Ask the messaging service to end the run, once.

    A completed run hands over its final answer, which the service posts to
    every target the agent didn't reach; a failed or stopped one has a short
    notice posted to every target. Ending a run twice posts nothing more. A
    finish that gets no readable answer records every target as failed,
    since where the run landed can't be told; one that got no answer at all,
    or a passing 502, 503 or 504, is marked worth asking again. A refused
    token, a run the service doesn't know or any other refusal is not.
    """
    body: Dict[str, Any] = {"status": status}
    if status == "completed" and final_text and final_text.strip():
        if len(final_text) > _FINAL_TEXT_CHARS:
            final_text = final_text[: _FINAL_TEXT_CHARS - 1] + "…"
        body["final_text"] = final_text
    try:
        answer = await messaging.gateway_request(
            "POST",
            f"/agent/automation-runs/{execution_id}/finish",
            user_id=automation["user_id"],
            timeout=_FINISH_TIMEOUT,
            body=body,
        )
    except messaging.GatewayError as e:
        logger.warning(
            f"[AUTOMATION_DELIVERY] Finish failed: execution_id={execution_id} "
            f"error={e.message}"
        )
        # No answer came back, unless the service refused the token.
        return Finish(_unconfirmed(automation, targets, e.message), retry=e.status is None)
    except Exception as e:
        logger.error(
            f"[AUTOMATION_DELIVERY] Finish failed: execution_id={execution_id} error={e!r}"
        )
        return Finish(_unconfirmed(automation, targets, "The finish could not be sent."))
    raw = (answer.data or {}).get("targets")
    if answer.status == 404:
        why = "The messaging service has no record of this run."
    elif answer.status != 200:
        why = f"The messaging service failed ({answer.status})."
    elif not isinstance(raw, list):
        why = "The messaging service sent an answer that could not be read."
    else:
        return Finish([_attempt(t) for t in raw if isinstance(t, dict)])
    logger.warning(
        f"[AUTOMATION_DELIVERY] Finish answered {answer.status}: "
        f"execution_id={execution_id}"
    )
    return Finish(
        _unconfirmed(automation, targets, why),
        retry=answer.status in _PASSING_STATUSES,
    )
