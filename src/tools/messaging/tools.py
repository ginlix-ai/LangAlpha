"""Tools that message the user on the chat apps connected to their account.

Delivery belongs to the channel gateway: it holds the user's linked accounts,
knows what each app accepts, and keeps the list of chats this user allowed.
These tools carry the turn's identity and the model's request to it and hand
the answer back. An address the model passes is a claim the gateway re-checks
on every send, never a credential, so nothing here decides who may be reached.

Every failure comes back as a result the model can read, never as an exception
into the graph: a messaging hiccup is not worth failing the turn for, and the
model has to know whether anything went out before it tells the user so.
"""

import http.cookiejar
import logging
import os
import re
import time
import uuid
from collections.abc import Sequence
from typing import Annotated, Any, NamedTuple

import httpx
from langchain_core.runnables import RunnableConfig
from langchain_core.tools import BaseTool, StructuredTool

from ptc_agent.agent.roles import AgentRole
from ptc_agent.agent.tools.computer_reach import (
    Attached,
    ComputerReach,
    ComputerUnreachable,
)
from src.config import env

try:
    from langchain.tools import InjectedToolCallId
except ImportError:  # pragma: no cover - older langchain
    from langchain_core.tools import InjectedToolCallId

logger = logging.getLogger(__name__)

MAX_TEXT_CHARS = 20_000
MAX_FILES = 10
MAX_PATH_CHARS = 1024

# The ``code`` of a send that repeats a call the messaging service is still
# delivering, with ``status`` unknown: neither sent nor failed yet.
IN_FLIGHT = "in_flight"

# What the delivery card says of a send whose outcome is unknown.
UNKNOWN_SHOWN = "Whether the message went out is unknown."

# A send fetches each file from the workspace and uploads it to the app
# before answering, so it gets far longer than a listing does.
_SEND_TIMEOUT = httpx.Timeout(90.0, connect=5.0)
_TARGETS_TIMEOUT = httpx.Timeout(20.0, connect=5.0)


def _service_token() -> str:
    return os.getenv("INTERNAL_SERVICE_TOKEN", "")


def messaging_enabled() -> bool:
    """Whether a gateway is configured to deliver through.

    Read at build time, per turn, so the tools appear and disappear with the
    deployment's configuration rather than with anything a request says.
    """
    return bool(env.CHANNEL_GATEWAY_URL and _service_token().strip())


_shared_client: httpx.AsyncClient | None = None


def _client() -> httpx.AsyncClient:
    """The worker's client for the gateway, built on first use, so calls reuse
    its connections. Each call passes its own timeout."""
    global _shared_client
    if _shared_client is None or _shared_client.is_closed:
        _shared_client = httpx.AsyncClient(
            follow_redirects=False,
            # Every user's calls share this client, so a jar that accepts no
            # domain keeps a cookie one answer sets off the next user's call.
            cookies=http.cookiejar.CookieJar(
                policy=http.cookiejar.DefaultCookiePolicy(allowed_domains=[])
            ),
        )
    return _shared_client


async def close_client() -> None:
    """Close the gateway client. Called at application shutdown."""
    global _shared_client
    if _shared_client is not None and not _shared_client.is_closed:
        await _shared_client.aclose()
    _shared_client = None


def _turn(config: RunnableConfig | None) -> dict[str, Any]:
    configurable = (config or {}).get("configurable") or {}
    return {
        "user_id": configurable.get("user_id"),
        "thread_id": configurable.get("thread_id"),
        "run_id": configurable.get("run_id"),
        "workspace_id": configurable.get("workspace_id"),
        "turn_platform": configurable.get("platform"),
    }


def _sends_for(config: RunnableConfig | None, target: str | None) -> str | None:
    """The automation run a send to ``target`` names, if any. An automation's
    turn whose delivery the messaging service holds names its run; the turn
    reporting back work such turns handed off names the run whose targets
    hold the chat (``automation_delivery.sends_for``)."""
    from src.server.services import automation_delivery

    configurable = (config or {}).get("configurable") or {}
    return automation_delivery.sends_for(configurable, target)


# -- results ------------------------------------------------------------------


def _result(status: str, code: str | None, message: str) -> str:
    lines = [f"status: {status}"]
    if code:
        lines.append(f"code: {code}")
    lines.append(message)
    return "\n".join(lines)


def send_artifact(data: dict[str, Any]) -> dict[str, Any]:
    """The delivery outcome as the client renders it; the model never sees it.

    Takes the gateway's answer, or the same keys built locally for a send that
    never got one, and applies the same defaults ``format_send_result`` does.
    """
    address = data.get("address") or None
    message = str(data.get("message") or "")
    if data.get("code") == IN_FLIGHT:
        # The messaging service's words for this are for the model ("don't
        # send it again"); the card says what the local unknown says.
        message = UNKNOWN_SHOWN
    files = [
        {
            "path": entry.get("path"),
            "status": entry.get("status") or "failed",
            "reason": entry.get("reason") or None,
        }
        for entry in data.get("files") or []
        if isinstance(entry, dict)
    ]
    return {
        "type": "message_delivery",
        "status": data.get("status") or "failed",
        "code": data.get("code") or None,
        "address": address,
        "platform": str(address).split(":", 1)[0].lower() if address else None,
        "current": bool(data.get("current")),
        "duplicate": bool(data.get("duplicate")),
        "message": message,
        "files": files,
    }


def _refusal(
    status: str, code: str | None, message: str, *, shown: str | None = None
) -> tuple[str, dict[str, Any]]:
    """A send that got no gateway answer: what the model reads, and the artifact.

    ``shown`` replaces ``message`` in the artifact when the model's sentence
    carries an instruction meant only for the model.
    """
    artifact = send_artifact(
        {"status": status, "code": code, "message": shown or message}
    )
    return _result(status, code, message), artifact


def send_result(data: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """The gateway's delivery answer: what the model reads, and the artifact."""
    return format_send_result(data), send_artifact(data)


def format_send_result(data: dict[str, Any]) -> str:
    """The gateway's delivery answer, as the model reads it."""
    status = data.get("status") or "failed"
    lines = [f"status: {status}"]
    if data.get("code"):
        lines.append(f"code: {data['code']}")
    if data.get("address"):
        here = " (this conversation)" if data.get("current") else ""
        lines.append(f"to: {data['address']}{here}")
    if data.get("duplicate"):
        lines.append(
            "An earlier attempt of this same call already delivered it; nothing was sent twice."
        )
    if data.get("code") == IN_FLIGHT:
        # The tool's own words, as for a duplicate: both describe a replay of
        # this call. The messaging service's message says the same.
        lines.append(
            "An earlier attempt of this same call is still delivering it, and it will "
            "arrive or fail on its own. Do not send it again, and do not tell the user "
            "it was sent."
        )
    elif data.get("message"):
        lines.append(str(data["message"]))
    files = data.get("files") or []
    if files:
        lines.append("files:")
        for entry in files:
            if not isinstance(entry, dict):
                continue
            outcome = entry.get("status") or "failed"
            reason = entry.get("reason")
            if not reason and outcome == "linked":
                reason = "sent as a download link"
            detail = f" ({reason})" if reason else ""
            lines.append(f"- {entry.get('path')}: {outcome}{detail}")
    return "\n".join(lines)


def _now() -> float:
    """The clock the listing's ages are read against; tests replace it."""
    return time.time()


def _age(epoch: Any) -> str | None:
    """How long ago an epoch-seconds time was, or None when it is not one."""
    if isinstance(epoch, bool) or not isinstance(epoch, int):
        return None
    seconds = max(0, int(_now()) - epoch)
    if seconds < 60:
        return "just now"
    if seconds < 3600:
        return f"{seconds // 60}m ago"
    if seconds < 48 * 3600:
        return f"{seconds // 3600}h ago"
    return f"{seconds // 86400}d ago"


def format_targets_result(data: dict[str, Any]) -> str:
    """The gateway's list of reachable addresses, as the model reads it."""
    if _listing_failed(data):
        return (
            "The messaging service could not list the targets. "
            "Where you can send is unknown right now."
        )
    lines: list[str] = []
    current = data.get("current")
    if isinstance(current, dict) and current.get("address"):
        lines.append(
            f"This conversation: {current['address']}. Omit target to send here."
        )
    else:
        lines.append(
            "This turn is not in a messaging conversation, so every send needs a target."
        )
    targets = [t for t in data.get("targets") or [] if isinstance(t, dict)]
    if targets:
        lines.append("Targets:")
        for target in targets:
            kind = f" ({target['kind']})" if target.get("kind") else ""
            name = f": {target['name']}" if target.get("name") else ""
            thread = target.get("thread")
            age = _age(thread.get("last_used_at")) if isinstance(thread, dict) else None
            note = "; preferred" if target.get("preferred") is True else ""
            if age:
                note += f"; this conversation's thread is here, last used {age}"
            lines.append(f"- {target.get('address')}{kind}{name}{note}")
    else:
        lines.append("Targets: none.")
    unavailable = [u for u in data.get("unavailable") or [] if isinstance(u, dict)]
    if unavailable:
        lines.append("Unavailable:")
        for entry in unavailable:
            reason = f" ({entry['reason']})" if entry.get("reason") else ""
            message = f": {entry['message']}" if entry.get("message") else ""
            lines.append(f"- {entry.get('platform')}{reason}{message}")
    if data.get("settings_url"):
        lines.append(f"Settings: {data['settings_url']}")
    return "\n".join(lines)


def _listing_failed(data: dict[str, Any]) -> bool:
    """Whether the answer is the one the messaging service gives when the listing
    itself failed: nothing reachable, and every app it names unavailable. Its
    current conversation is then unknown, not absent."""
    current = data.get("current")
    if (isinstance(current, dict) and current.get("address")) or data.get("targets"):
        return False
    unavailable = [u for u in data.get("unavailable") or [] if isinstance(u, dict)]
    return bool(unavailable) and all(u.get("reason") == "unavailable" for u in unavailable)


# -- transport ----------------------------------------------------------------


class GatewayError(Exception):
    """A call that produced no answer the model can act on.

    ``delivered_unknown`` is True when the request may have reached the
    gateway and been acted on, so a send cannot be called either way.
    ``status`` is the status of an answer that refused this server's token;
    None when no answer came back at all.
    """

    def __init__(
        self, message: str, *, delivered_unknown: bool, status: int | None = None
    ) -> None:
        super().__init__(message)
        self.message = message
        self.delivered_unknown = delivered_unknown
        self.status = status


class GatewayAnswer(NamedTuple):
    status: int
    #: The JSON object answered, or None when the body is not one.
    data: dict[str, Any] | None
    text: str


async def gateway_request(
    method: str,
    path: str,
    *,
    user_id: str,
    timeout: httpx.Timeout,
    params: dict[str, Any] | None = None,
    body: dict[str, Any] | None = None,
) -> GatewayAnswer:
    """One call to the gateway for ``user_id``, and its answer whatever the
    status, so a caller with statuses of its own (a version conflict, a list
    of problems) reads them. Raises ``GatewayError`` when no answer came back
    or the gateway refused this server's token."""
    headers = {"X-Service-Token": _service_token(), "X-User-Id": str(user_id)}
    url = f"{env.CHANNEL_GATEWAY_URL}{path}"
    try:
        response = await _client().request(
            method, url, params=params, json=body, headers=headers, timeout=timeout
        )
    # A wait for a free connection ends before the request is written.
    except (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout) as e:
        logger.warning("[messaging] gateway unreachable for %s: %r", path, e)
        raise GatewayError(
            "The messaging service could not be reached.", delivered_unknown=False
        ) from e
    except httpx.TimeoutException as e:
        logger.warning("[messaging] gateway timed out for %s: %r", path, e)
        raise GatewayError(
            "The messaging service did not answer in time.", delivered_unknown=True
        ) from e
    except httpx.HTTPError as e:
        logger.warning("[messaging] gateway call failed for %s: %r", path, e)
        raise GatewayError(
            "The connection to the messaging service broke.", delivered_unknown=True
        ) from e
    try:
        data = response.json()
    except ValueError:
        data = None
    if response.status_code in (401, 403):
        logger.error(
            "[messaging] gateway refused this server's service token (%s) on %s",
            response.status_code,
            path,
        )
        raise GatewayError(
            "The messaging service refused this server's credentials.",
            delivered_unknown=False,
            status=response.status_code,
        )
    return GatewayAnswer(
        response.status_code, data if isinstance(data, dict) else None, response.text
    )


async def _call(
    method: str,
    path: str,
    *,
    user_id: str,
    timeout: httpx.Timeout,
    params: dict[str, Any] | None = None,
    body: dict[str, Any] | None = None,
) -> dict[str, Any]:
    answer = await gateway_request(
        method, path, user_id=user_id, timeout=timeout, params=params, body=body
    )
    if answer.status == 422:
        raise GatewayError(
            f"The messaging service rejected the request: {_detail(answer)}",
            delivered_unknown=False,
        )
    if answer.status != 200:
        logger.warning("[messaging] gateway answered %s on %s", answer.status, path)
        raise GatewayError(
            f"The messaging service failed ({answer.status}).",
            # A redirect this client doesn't follow, or a 4xx from the service
            # or a proxy in front of it, refused the request before anything
            # acted on it. A 5xx may come after the send went out.
            delivered_unknown=answer.status >= 500,
        )
    if answer.data is None:
        raise GatewayError(
            "The messaging service sent an answer that could not be read.",
            delivered_unknown=True,
        )
    return answer.data


def _detail(answer: GatewayAnswer) -> str:
    detail = (answer.data or {}).get("detail")
    text = detail if isinstance(detail, str) else (str(detail) if detail else "")
    return (text or answer.text or "malformed request")[:300]


# -- the tools ----------------------------------------------------------------


#: A file the Chief of Staff names on one of the user's computers: the
#: computer, a colon, and the file's whole path there. The path is what has
#: to start with ``/`` or ``~``; a name may hold a slash.
_ON_COMPUTER = re.compile(r"^(?P<computer>[^/~].*?):(?P<path>[/~].*)$")

#: The messaging service's limits on one attached file and on a send's files
#: in all, which a copy made for a send must not outgrow.
ATTACH_MAX_BYTES = 20 * 1024 * 1024
SEND_MAX_BYTES = 50 * 1024 * 1024


async def _attach_all(
    paths: Sequence[str], workspace_id: str | None, computers: ComputerReach | None
) -> tuple[list[dict[str, Any]], dict[int, str]]:
    """The files as the messaging service fetches them, and what the model
    called each one it named on a computer, by position. Raises
    ``ComputerUnreachable`` when one of those cannot go."""
    on: dict[int, re.Match[str]] = {}
    found: Sequence[Attached] = ()
    if computers is not None:
        on = {i: match for i, spec in enumerate(paths) if (match := _ON_COMPUTER.match(spec))}
    if computers is not None and on:
        found = await computers.attach([(m["computer"].strip(), m["path"]) for m in on.values()])
    attached = dict(zip(on, found, strict=True))
    refs: list[dict[str, Any]] = []
    named: dict[int, str] = {}
    for index, spec in enumerate(paths):
        got = attached.get(index)
        if got is None:
            refs.append({"path": spec, "workspace_id": workspace_id})
            continue
        refs.append({"path": got.path, "workspace_id": got.workspace_id})
        named[index] = f"{spec} (copied to Home as {got.path})" if got.copied else spec
    return refs, named


def _as_named(data: dict[str, Any], named: dict[int, str]) -> dict[str, Any]:
    """The answer with each file the model named on a computer called as it
    called it, so the result reads in its own terms."""
    files = data.get("files")
    if not named or not isinstance(files, list):
        return data
    return {
        **data,
        "files": [
            {**entry, "path": named[i]} if i in named and isinstance(entry, dict) else entry
            for i, entry in enumerate(files)
        ],
    }


async def _send(
    *,
    text: str,
    files: list[str] | None,
    target: str | None,
    reply: bool,
    new_thread: bool,
    files_workspace_id: str | None,
    config: RunnableConfig | None,
    tool_call_id: str,
    computers: ComputerReach | None = None,
) -> tuple[str, dict[str, Any]]:
    turn = _turn(config)
    if not turn["user_id"]:
        return _refusal(
            "failed",
            "unavailable",
            "This turn has no user attached, so there is no one to message.",
        )
    text = text or ""
    paths = [p.strip() for p in files or [] if isinstance(p, str) and p.strip()]
    if not text.strip() and not paths:
        return _refusal(
            "failed", "invalid_request", "Nothing to send: pass text, files, or both."
        )
    if len(text) > MAX_TEXT_CHARS:
        return _refusal(
            "failed",
            "invalid_request",
            f"The text is {len(text)} characters; the limit is {MAX_TEXT_CHARS}. "
            "Split it across messages.",
        )
    if len(paths) > MAX_FILES:
        return _refusal(
            "failed",
            "invalid_request",
            f"{len(paths)} files given; a message carries at most {MAX_FILES}.",
        )
    too_long = [p for p in paths if len(p) > MAX_PATH_CHARS]
    if too_long:
        return _refusal(
            "failed",
            "invalid_request",
            f"A file path is {len(too_long[0])} characters; the limit is {MAX_PATH_CHARS}.",
        )
    files_workspace_id = (files_workspace_id or "").strip() or None
    if files_workspace_id is not None:
        # The messaging service fetches each file from this workspace by its
        # id, so only a workspace id may go: anything else is a path.
        try:
            files_workspace_id = str(uuid.UUID(files_workspace_id))
        except ValueError:
            return _refusal(
                "failed",
                "invalid_request",
                "workspace_id is not a workspace id; take it from manage_workspaces. "
                "Nothing was sent.",
            )

    try:
        refs, named = await _attach_all(paths, files_workspace_id, computers)
    except ComputerUnreachable as e:
        return _refusal("failed", "invalid_request", f"{e} Nothing was sent.")

    if not tool_call_id:
        # The messaging service answers a repeat of a call it delivered by the
        # call's id, so a send without one that is replayed goes out again.
        logger.warning(
            "[messaging] send without a tool call id: thread=%s run=%s",
            turn["thread_id"],
            turn["run_id"],
        )
    body = {
        "thread_id": turn["thread_id"],
        "run_id": turn["run_id"],
        "tool_call_id": tool_call_id or None,
        "turn_platform": turn["turn_platform"],
        "workspace_id": turn["workspace_id"],
        "target": (target or "").strip() or None,
        "text": text,
        "files": refs,
        "reply": bool(reply),
        "new_thread": bool(new_thread),
    }
    # An automation's turn names its run, so the send may reach the run's targets.
    execution_id = _sends_for(config, body["target"])
    if execution_id:
        body["automation_execution_id"] = execution_id
    try:
        data = await _call(
            "POST",
            "/agent/send",
            user_id=turn["user_id"],
            timeout=_SEND_TIMEOUT,
            body=body,
        )
    except GatewayError as e:
        if e.delivered_unknown:
            return _refusal(
                "unknown",
                "unavailable",
                f"{e.message} Whether the message went out is unknown. Do not send it "
                "again, and tell the user delivery couldn't be confirmed.",
                shown=f"{e.message} {UNKNOWN_SHOWN}",
            )
        return _refusal("failed", "unavailable", f"{e.message} Nothing was sent.")
    return format_send_result(_as_named(data, named)), send_artifact(data)


_TEXT_ARG = Annotated[
    str,
    f"The message, in markdown, up to {MAX_TEXT_CHARS} characters. May be empty only "
    "when files are given.",
]
_TARGET_ARG = Annotated[
    str | None,
    "An address from list_message_targets. Omit it to send into the conversation this "
    "turn is in, which exists only when the turn arrived from a messaging app. Address "
    "a channel or chat, never a thread: where the chat has threads, this conversation's "
    "messages to it go in its thread there (made on the first send, or the one the user "
    "started), and the result says where each one landed.",
]
_REPLY_ARG = Annotated[
    bool,
    "True to reply to the user's message that started this turn, where the app "
    "supports it. Applies only to the conversation this turn is in.",
]
_NEW_THREAD_ARG = Annotated[
    bool,
    "True to start a new thread in that chat (a new topic on Telegram and Feishu) instead of "
    "continuing this conversation's thread there; it then becomes this conversation's "
    "thread. Use it for an unrelated update, or when list_message_targets shows the "
    "thread was last used long ago. Has no effect where the chat has no threads.",
]


async def _send_from_workspace(
    text: _TEXT_ARG,
    config: RunnableConfig,
    files: Annotated[
        list[str] | None,
        f"Up to {MAX_FILES} workspace file paths to attach, such as results/report.xlsx. "
        "Each file can be up to 20 MB, and a send's files hold 50 MB in all.",
    ] = None,
    target: _TARGET_ARG = None,
    reply: _REPLY_ARG = False,
    new_thread: _NEW_THREAD_ARG = False,
    tool_call_id: Annotated[str, InjectedToolCallId] = "",
) -> tuple[str, dict[str, Any]]:
    return await _send(
        text=text,
        files=files,
        target=target,
        reply=reply,
        new_thread=new_thread,
        files_workspace_id=None,
        config=config,
        tool_call_id=tool_call_id,
    )


_FILES_FROM_HOME = (
    f"Up to {MAX_FILES} file paths to attach: in Home, such as nvda_amd/digest.md, "
    "or inside the folder of the workspace named in workspace_id, such as "
    "dcf/report.md. Each file can be up to 20 MB, and a send's files hold 50 MB in all."
)
_FILES_ANYWHERE = (
    f"Up to {MAX_FILES} file paths to attach: in Home, such as nvda_amd/digest.md, or "
    "inside the folder of the workspace named in workspace_id, such as dcf/report.md. "
    "A file on one of the user's computers goes as that computer's name, a colon and "
    "its whole path there, such as `MacBook Pro:/Users/me/Documents/budget.pdf`; one "
    "outside every workspace folder is copied into Home's `sent` folder to send. Each "
    "file can be up to 20 MB, and a send's files hold 50 MB in all."
)


def _send_from_home(computers: ComputerReach | None) -> BaseTool:
    """The Chief of Staff's ``send_message``, which attaches from Home or the
    workspace it names, since it reads every workspace; and, given the user's
    other ``computers``, from any of them."""

    async def send_message(
        text: _TEXT_ARG,
        config: RunnableConfig,
        files: Annotated[
            list[str] | None, _FILES_ANYWHERE if computers is not None else _FILES_FROM_HOME
        ] = None,
        workspace_id: Annotated[
            str | None,
            "The id of the workspace the files are in, from manage_workspaces, when they "
            "are not in Home. Omit it for Home's files.",
        ] = None,
        target: _TARGET_ARG = None,
        reply: _REPLY_ARG = False,
        new_thread: _NEW_THREAD_ARG = False,
        tool_call_id: Annotated[str, InjectedToolCallId] = "",
    ) -> tuple[str, dict[str, Any]]:
        return await _send(
            text=text,
            files=files,
            target=target,
            reply=reply,
            new_thread=new_thread,
            files_workspace_id=workspace_id,
            config=config,
            tool_call_id=tool_call_id,
            computers=computers,
        )

    return StructuredTool.from_function(
        coroutine=send_message,
        name="send_message",
        description=SEND_MESSAGE_DESCRIPTION,
        response_format="content_and_artifact",
    )


async def _list_message_targets(config: RunnableConfig) -> str:
    turn = _turn(config)
    if not turn["user_id"]:
        return "This turn has no user attached, so there is no one to message."
    params = {
        key: value
        for key, value in (
            ("thread_id", turn["thread_id"]),
            ("run_id", turn["run_id"]),
            ("turn_platform", turn["turn_platform"]),
        )
        if value
    }
    try:
        data = await _call(
            "GET",
            "/agent/targets",
            user_id=turn["user_id"],
            timeout=_TARGETS_TIMEOUT,
            params=params,
        )
    except GatewayError as e:
        return f"{e.message} Where you can send is unknown right now."
    return format_targets_result(data)


SEND_MESSAGE_DESCRIPTION = """Send a message to the user on a messaging app connected to their account, with files attached if needed. Use it when the user asks for something to be sent to them or to a chat, or when this conversation's delivery rules say to reply through it.
Reaches only the conversation this turn is in, the user's own direct messages, and the shared chats they allowed. When the user names only an app, send to that app's preferred chat. A target that is only an app name, like `discord`, is the user's direct messages there, the same as `discord:@me`; on Slack with several workspaces, name the team as `slack:<team>`.

Returns:
    The delivery status (sent, partial, failed or unknown), where it went, and each file's outcome.

Tell the user something was sent only when the status is sent. On partial only part went, and the result's message says which."""

LIST_MESSAGE_TARGETS_DESCRIPTION = """List where send_message can deliver for this user: the conversation this turn is in, if any, then their direct messages and allowed shared chats on each connected messaging app with each app's preferred chat marked, and the apps that are unavailable with the reason. A direct message is listed as `<app>:@me`, or `slack:<team>` on Slack.

Returns:
    Addresses to pass as send_message's target, and a link to the settings where the user allows more."""


_SEND_MESSAGE = StructuredTool.from_function(
    coroutine=_send_from_workspace,
    name="send_message",
    description=SEND_MESSAGE_DESCRIPTION,
    response_format="content_and_artifact",
)
_LIST_MESSAGE_TARGETS = StructuredTool.from_function(
    coroutine=_list_message_targets,
    name="list_message_targets",
    description=LIST_MESSAGE_TARGETS_DESCRIPTION,
)


def build_messaging_tools(
    role: AgentRole, *, computers: ComputerReach | None = None
) -> list[BaseTool]:
    """The messaging tools for one agent build, or none when unconfigured.

    ``role`` picks the ``send_message`` shape: an analyst attaches files from
    its workspace, and the Chief of Staff from Home or from the workspace it
    names, since it reads every workspace. ``computers`` is given when the
    user has several: the Chief of Staff's files may then be on any.
    """
    if not messaging_enabled():
        return []
    if role == "chief_of_staff":
        return [_send_from_home(computers), _LIST_MESSAGE_TARGETS]
    return [_SEND_MESSAGE, _LIST_MESSAGE_TARGETS]
