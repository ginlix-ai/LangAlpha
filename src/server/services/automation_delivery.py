"""An automation run that delivers through the messaging service.

With a messaging service configured, an automation's results are the run's
own to send. Before the turn starts, the service resolves each delivery entry
to one chat and holds the run; the agent is told where to send and does so
with ``send_message``. When the run settles, the service posts the final
answer to every chat the agent didn't reach, or a short notice to all of them
when the run failed or was stopped, and answers where each one landed, which
becomes the execution's ``delivery_result``. A finish that got no answer,
found the service briefly unavailable, or got a record still being posted is
worth asking again (``Finish.retry``): the service ends a run once and
answers the same record to every later ask. Both calls name the run's
thread, so what the service posts links back to it and a reply there
continues it.

The run's turn carries the run in its config (``turn_configurable``). Work it
hands off is reported back by a later turn, after the run has settled; the
hand-off records the run there, so that turn is told where the run delivers
and sends there under the same id (``report_back.handoff.requested_from``). A
report-back of hand-offs from several runs carries each of them, and every
send names the run whose targets hold the chat it goes to (``sends_for``).

A run the service never took (no service, no entries, the start failed, or
no entry resolved) delivers through the webhook (``webhook_client``) instead,
exactly as before; when the webhook has nowhere to post either, a start that
failed records each entry as undelivered, with why (``unsent``). Which way a
run delivers is decided at its start, and stamped on its run row
(``DELIVERY_KEY``), so the settle, which may run on another worker, follows
the same way. The run row carries it only from the turn's START, so the
execution row keeps it too (its ``delivery`` column) from the moment the
start returns, for a sweep after a process that died in between. A run that
waited for its thread starts again, under an id of its own, when its entries
changed meanwhile (``Delivery.id``), and both rows then carry that start.

Nothing here raises: a delivery problem is never worth failing a run or a
settle for, and each failure falls back or is recorded as one.
"""

from __future__ import annotations

import asyncio
import logging
import unicodedata
from dataclasses import dataclass
from typing import Any, Dict, Literal, Optional, Union

import httpx

from src.tools.messaging import tools as messaging

logger = logging.getLogger(__name__)

#: The run-row metadata key a run whose start asked the messaging service
#: carries: the id the run is filed under and the targets its start resolved.
DELIVERY_KEY = "automation_delivery"
#: The key a report-back turn keeps the other held runs its requests came from
#: under, in the order of their requests: in its request, its config and its
#: run row. The turn's own run, under ``DELIVERY_KEY``, is the latest one.
EARLIER_KEY = "automation_earlier_runs"

FinishStatus = Literal["completed", "failed", "stopped"]

# A start resolves every entry and checks each chat with its app.
_START_TIMEOUT = httpx.Timeout(20.0, connect=5.0)
#: The wait before a start's second ask, after a first that got no answer or
#: found the service briefly unavailable.
START_RETRY_DELAY = 1.0
# A finish posts to every chat the run didn't reach. It runs in a hook outbox
# job, or in a settle's tail, which shutdown waits on for about one webhook's
# timeout.
_FINISH_TIMEOUT = httpx.Timeout(15.0, connect=5.0)

# What a fallback carries: the same cap ``send_message`` puts on a message.
_FINAL_TEXT_CHARS = messaging.MAX_TEXT_CHARS

#: The wait before each further ask after a finish worth retrying, when the
#: settling process asks again itself (a firing that never reached the run
#: ledger; any other's are a hook outbox job's): three asks in all.
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
_UNCONFIRMED = "Delivery couldn't be confirmed."


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


def entries_of(automation: Dict[str, Any]) -> list[str]:
    """The automation's delivery entries, as a run's start hands them over."""
    config = automation.get("delivery_config")
    methods = config.get("methods") if isinstance(config, dict) else None
    return [m for m in methods or [] if isinstance(m, str) and m]


# -- start ------------------------------------------------------------------


@dataclass(frozen=True)
class Delivery:
    """How a run's start left its delivery.

    The messaging service holds the run (``held``) once any of its entries
    resolved to a chat: the service files the run under ``id``, the run's
    agent sends to the ``ok`` targets naming ``id``, and the settle ends the
    run there. A start the service didn't take leaves the run to the
    webhook, and its ``targets`` say why each entry wasn't taken, which the
    run records when the webhook has nowhere to post either (``unsent``).
    """

    id: str
    targets: list[Target]

    @property
    def held(self) -> bool:
        return any(t.ok for t in self.targets)


# Why an entry went undelivered when the start got no answer to give one.
_NO_ANSWER = "The messaging service could not be reached."
_NOT_RESOLVED = "The messaging service didn't resolve it to a chat."


def _not_taken(
    execution_id: str, entries: list[str], why: str, answered: Optional[list[Target]] = None
) -> Delivery:
    """A start the service didn't take: every entry refused, for the reason
    the service gave it, else ``why``."""
    reasons = {t.entry: t.message for t in answered or [] if t.message}
    return Delivery(
        id=execution_id,
        targets=[
            Target(entry=e, address=None, name=None, ok=False, message=reasons.get(e) or why)
            for e in dict.fromkeys(entries)
        ],
    )


async def start_run(
    automation: Dict[str, Any],
    execution_id: str,
    workspace_id: Optional[str],
    thread_id: Optional[str] = None,
) -> Optional[Delivery]:
    """How the run delivers: through the messaging service once it holds the
    run (``Delivery.held``), else through the webhook. None when nothing was
    asked.

    Only a run with delivery entries on a server with a messaging service
    asks. A start that got no answer, or found the service briefly
    unavailable, asks once more: the service files the run under
    ``execution_id``, so asking again under the same id answers the targets
    it already holds, whatever the entries say now. A start that still
    fails, or one that resolved no entry to a chat, is left to the webhook,
    with each entry's reason.
    """
    entries = entries_of(automation)
    if not entries or not messaging.messaging_enabled():
        return None
    body: Dict[str, Any] = {
        "execution_id": execution_id,
        "workspace_id": workspace_id,
        "automation_name": automation.get("name") or "",
        "entries": entries,
    }
    if thread_id:
        body["thread_id"] = str(thread_id)
    answer = await _ask_start(automation["user_id"], body)
    if _asks_again(answer):
        logger.warning(
            f"[AUTOMATION_DELIVERY] Start got no answer, asking again: "
            f"execution_id={execution_id}"
        )
        await asyncio.sleep(START_RETRY_DELAY)
        answer = await _ask_start(automation["user_id"], body)
    if isinstance(answer, messaging.GatewayError):
        logger.warning(
            f"[AUTOMATION_DELIVERY] Start failed, delivering by webhook: "
            f"execution_id={execution_id} error={answer.message}"
        )
        return _not_taken(execution_id, entries, answer.message)
    raw = (answer.data or {}).get("targets")
    if answer.status != 200 or not isinstance(raw, list):
        logger.warning(
            f"[AUTOMATION_DELIVERY] Start answered {answer.status}, delivering by "
            f"webhook: execution_id={execution_id}"
        )
        why = (
            f"The messaging service failed ({answer.status})."
            if answer.status != 200
            else "The messaging service sent an answer that could not be read."
        )
        return _not_taken(execution_id, entries, why)
    targets = [t for t in (Target.read(item) for item in raw) if t is not None]
    if not any(t.ok for t in targets):
        logger.info(
            f"[AUTOMATION_DELIVERY] No entry resolved to a chat, delivering by "
            f"webhook: execution_id={execution_id}"
        )
        return _not_taken(execution_id, entries, _NOT_RESOLVED, targets)
    logger.info(
        f"[AUTOMATION_DELIVERY] The run sends its own results: "
        f"execution_id={execution_id} targets={sum(t.ok for t in targets)}/{len(targets)}"
    )
    return Delivery(id=execution_id, targets=targets)


async def _ask_start(
    user_id: str, body: Dict[str, Any]
) -> Union[messaging.GatewayAnswer, messaging.GatewayError]:
    """The service's answer to a start, or the error that stands for none."""
    try:
        return await messaging.gateway_request(
            "POST",
            "/agent/automation-runs",
            user_id=user_id,
            timeout=_START_TIMEOUT,
            body=body,
        )
    except messaging.GatewayError as e:
        return e
    except Exception as e:
        logger.error(
            f"[AUTOMATION_DELIVERY] Start failed: "
            f"execution_id={body['execution_id']} error={e!r}"
        )
        return messaging.GatewayError(_NO_ANSWER, delivered_unknown=False)


def _asks_again(answer: Union[messaging.GatewayAnswer, messaging.GatewayError]) -> bool:
    """Whether a start's first ask is worth a second: one that may have
    reached the service with no answer back, or found it briefly
    unavailable. A refusal or a failure is an answer, and so is a 200."""
    if isinstance(answer, messaging.GatewayError):
        return answer.delivered_unknown
    return answer.status in _PASSING_STATUSES


def stamp(delivery: Delivery) -> Dict[str, Any]:
    """The delivery as it is written down: on the run row and the
    execution row, in the turn's config, and in a hand-off's report-back
    record."""
    return {"id": delivery.id, "targets": [t.as_dict() for t in delivery.targets]}


def read_stamp(data: Any, default_id: Optional[str] = None) -> Optional[Delivery]:
    """The delivery a stamp names; None when it names no targets list. A
    stamp naming no id is filed under ``default_id``, and with neither it
    names nothing."""
    if not isinstance(data, dict) or not isinstance(data.get("targets"), list):
        return None
    delivery_id = _text(data.get("id")) or default_id
    if not delivery_id:
        return None
    return Delivery(
        id=delivery_id,
        targets=[t for t in (Target.read(i) for i in data["targets"]) if t is not None],
    )


def read_stamps(data: Any) -> list[Delivery]:
    """The deliveries a list of stamps names, leaving out any stamp that names
    none."""
    if not isinstance(data, list):
        return []
    return [d for d in (read_stamp(item) for item in data) if d is not None]


def run_metadata(delivery: Delivery) -> Dict[str, Any]:
    """The run-row stamp a settle reads to follow this run's way of delivery."""
    return {DELIVERY_KEY: stamp(delivery)}


def delivery_of_run(run: Optional[Dict[str, Any]], execution_id: str) -> Optional[Delivery]:
    """How a run's start left its delivery, by its run row; None when the
    start asked nothing. A stamp naming no id is filed under the
    execution's."""
    metadata = (run or {}).get("metadata") or {}
    data = metadata.get(DELIVERY_KEY) if isinstance(metadata, dict) else None
    return read_stamp(data, execution_id)


def turn_configurable(
    delivery: Delivery, earlier: Optional[list[Delivery]] = None
) -> Dict[str, Any]:
    """What a held run's turn carries in its config for its tools.

    ``send_message`` names the id, so its sends may reach the run's targets.
    A hand-off records the targets too, so the turn that reports its result
    back can say where the run delivers and send there under the same id. A
    report-back of requests from several runs carries the ``earlier`` ones
    too, so a send to one of their targets names that run (``sends_for``).
    """
    configurable = {"automation_execution_id": delivery.id, DELIVERY_KEY: stamp(delivery)}
    if earlier:
        configurable[EARLIER_KEY] = [stamp(run) for run in earlier]
    return configurable


def delivery_of_turn(configurable: Dict[str, Any]) -> Optional[Delivery]:
    """The held run a turn sends for, by its config; None when it names none.

    A config that names the id without its targets (a turn started before
    they were carried) gives the run with no targets.
    """
    delivery_id = _text(configurable.get("automation_execution_id"))
    if not delivery_id:
        return None
    delivery = read_stamp(configurable.get(DELIVERY_KEY), delivery_id)
    if delivery is None or delivery.id != delivery_id:
        return Delivery(id=delivery_id, targets=[])
    return delivery


def runs_of_turn(configurable: Dict[str, Any]) -> list[Delivery]:
    """Every held run a turn sends for, by its config, its own run last: a
    report-back of requests from several runs names the earlier ones first.
    Empty when it sends for none."""
    run = delivery_of_turn(configurable)
    if run is None:
        return []
    earlier = [r for r in read_stamps(configurable.get(EARLIER_KEY)) if r.id != run.id]
    return [*earlier, run]


def turn_metadata(configurable: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """The run-row stamp of the held run a turn sends for, by its config, and
    of the earlier runs a report-back names, so the turn that resumes or
    retries it sends for them too (``carry.carried_delivery``); empty when it
    sends for none."""
    runs = runs_of_turn(configurable or {})
    if not runs or not runs[-1].held:
        return {}
    stamped = run_metadata(runs[-1])
    if len(runs) > 1:
        stamped[EARLIER_KEY] = [stamp(run) for run in runs[:-1]]
    return stamped


def _chat(address: Optional[str]) -> Optional[tuple[str, ...]]:
    """The chat an address names, as a key every spelling of it shares: the
    app and the chat, without a thread. A DM is the app's (on Slack, the
    team's, which a DM's own ``D…`` channel names too). None for no address.
    """
    app, _, path = (address or "").strip().partition(":")
    app = app.lower()
    if not app:
        return None
    if path.lower() in ("", "@me"):
        return (app,)
    parts = path.split("/")
    if app == "slack":
        if len(parts) == 1 or parts[1].startswith("D"):
            return (app, parts[0])
        return (app, parts[0], parts[1])
    if app == "discord":
        return (app, *parts[:2])
    if app == "telegram":
        return (app, parts[0])
    # An iMessage space id or a Feishu chat id is the whole path.
    return (app, path)


def sends_for(configurable: Dict[str, Any], target: Optional[str]) -> Optional[str]:
    """The id a send to ``target`` names: the latest of the turn's runs whose
    targets hold that chat, else the turn's own run. None when the turn sends
    for none.

    Only a report-back of requests from several runs has a choice to make. The
    messaging service counts a send as a run's own only at that run's
    targets, so each outcome goes out under the run its request came from.
    A chat several runs' targets hold names the latest of them. A send into
    the turn's conversation, or to a chat this can't match to any run's
    targets, names the turn's own run, and the service, which knows every
    chat, decides whether it is one of that run's.
    """
    runs = runs_of_turn(configurable)
    if not runs:
        return None
    chat = _chat(target)
    if chat is not None:
        for run in reversed(runs):
            if any(_chat(t.address) == chat for t in reached(run.targets)):
                return run.id
    return runs[-1].id


def unsent(delivery: Delivery) -> list[Dict[str, Any]]:
    """The ``delivery_result`` of a run the service didn't take, when the
    webhook has nowhere to post either: every entry failed, for the reason
    its start gave."""
    return [
        {
            "method": t.entry,
            "address": None,
            "name": None,
            "success": False,
            "via": None,
            "error": t.message or _NO_ANSWER,
        }
        for t in delivery.targets
    ]


def app_name(address: str) -> str:
    """The app an address or a bare app name is on, as the user knows it."""
    app = address.split(":", 1)[0].lower()
    return _APP_NAMES.get(app, app.capitalize())


def _is_dm(address: str) -> bool:
    """``<app>:@me`` (or the bare app), or ``slack:<team>`` on Slack."""
    app, _, rest = address.partition(":")
    return rest in ("", "@me") or (app.lower() == "slack" and "/" not in rest)


def one_line(text: Optional[str], chars: int) -> Optional[str]:
    """Free text fit for a reminder, or None when nothing is left.

    A reminder is a directive the agent trusts, so text from elsewhere stays
    one short line of plain words: control and format characters, line
    breaks included, become spaces, backticks go, since they would close a
    code span beside it, whitespace collapses and long text is cut.
    """
    if not text:
        return None
    text = "".join(
        " " if c.isspace() or unicodedata.category(c) in ("Cc", "Cf", "Cs") else c
        for c in text.replace("`", "")
    )
    text = " ".join(text.split())
    if len(text) > chars:
        text = text[: chars - 1].rstrip() + "…"
    return text or None


def _describe(address: str) -> str:
    """One target as the reminder names it: by its app and the address the
    service resolved, which the agent passes to ``send_message``. A chat's
    name is left out, since whoever runs a shared chat can set it (a Telegram
    group's title, an iMessage group's name) and a reminder is a directive
    the agent trusts."""
    app = app_name(address)
    if _is_dm(address):
        return f"your {app} DM `{address}`"
    return f"{app} `{address}`"


def reached(targets: list[Target]) -> list[Target]:
    """The targets a run's agent may send to: the ones its start resolved."""
    return [t for t in targets if t.ok and t.address]


def described(targets: list[Target]) -> str:
    """The targets the run reaches, as its reminders name them."""
    return ", ".join(_describe(t.address) for t in reached(targets))


def reminder(targets: list[Target]) -> str:
    """What the run's agent is told about its delivery: where to send when it
    finishes. An entry that didn't resolve is the run record's to name, not
    the agent's to try."""
    return (
        f"When you finish, send the result with send_message to: {described(targets)}. "
        "Write it for chat; attach files for detail."
    )


# -- finish -----------------------------------------------------------------


@dataclass(frozen=True)
class Finish:
    """One ask to end a run: the ``delivery_result`` it gives, one item per
    target, and whether asking again may land where this one didn't."""

    result: list[Dict[str, Any]]
    retry: bool = False
    #: The service answered with the run's record, final or still posting.
    answered: bool = False


def _still_posting(item: Dict[str, Any]) -> bool:
    """A row the service hasn't finished posting: neither reached nor
    posted, and no error, which a row that didn't land always has."""
    return not item["success"] and item["error"] is None


def _attempt(data: Dict[str, Any]) -> Dict[str, Any]:
    """One row of the finish's answer as a ``delivery_result`` item. Its
    ``address`` is where the post landed, possibly a thread, so a row is
    matched to its target by ``method``, the entry."""
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
    error = f"{_UNCONFIRMED} {why}"
    if not targets:
        targets = [Target(entry=e, address=None, name=None, ok=True) for e in entries_of(automation)]
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
    thread_id: Optional[str] = None,
) -> Finish:
    """Ask the messaging service to end the run, once.

    A completed run hands over its final answer, which the service posts to
    every target the agent didn't reach; a failed or stopped one has a short
    notice posted to every target. Ending a run twice posts nothing more. A
    finish that gets no readable answer records every target as failed,
    since where the run landed can't be told; one that got no answer at all,
    or a passing 502, 503 or 504, is marked worth asking again. A refused
    token, a run the service doesn't know or any other refusal is not. An
    answer with rows still being posted, which the service gives when
    another finish for the run is mid-post, is worth asking again too; its
    rows that have landed stand, and the ones still being posted read as
    unconfirmed until an answer says where they landed.
    """
    body: Dict[str, Any] = {"status": status}
    if thread_id:
        body["thread_id"] = str(thread_id)
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
        rows = [_attempt(t) for t in raw if isinstance(t, dict)]
        posting = any(_still_posting(row) for row in rows)
        if posting:
            rows = [{**row, "error": _UNCONFIRMED} if _still_posting(row) else row for row in rows]
        return Finish(rows, retry=posting, answered=True)
    logger.warning(
        f"[AUTOMATION_DELIVERY] Finish answered {answer.status}: "
        f"execution_id={execution_id}"
    )
    return Finish(
        _unconfirmed(automation, targets, why),
        retry=answer.status in _PASSING_STATUSES,
    )
