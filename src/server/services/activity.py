"""An organization's activity log: what its members did that changes who can
see or reach its work.

Recorded only for an account the auth service places in an organization; with
the ``teams`` flag off nothing is read, written or scheduled. :func:`record`
takes what the row needs where it is called (the request's caller and trace
included) and returns at once; a background task asks ``account_for`` and
writes the row, bounded as a whole by
:data:`~src.server.database.activity_events.RECORD_BOUND_S`, so a slow auth
service or database never holds up or fails the act. The log is best-effort:
a write that fails or overruns is logged at error level, at most
:data:`_MAX_PENDING` writes wait on each process, and a process that dies with
writes pending loses them (shutdown waits :data:`SHUTDOWN_DRAIN_S` for them).

``detail`` is checked here so no call site can put a credential in the log:
it is a flat mapping of at most :data:`_MAX_KEYS` entries whose keys never
name a secret (:data:`_SECRET_KEY`), and whose values are JSON scalars, strings
of at most :data:`_MAX_TEXT` characters, or short lists of them. An entry that
breaks a rule is left out of the row and logged; the event is still written.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
from contextvars import ContextVar
from enum import StrEnum
from typing import Any, Literal
from urllib.parse import urlsplit
from uuid import UUID

from starlette.types import Scope

from src.server.database import activity_events
from src.server.services import account as accounts
from src.server.services import membership
from src.server.services.policy import Policy, PolicyDenied
from src.server.utils.api import caller_of

logger = logging.getLogger(__name__)

Outcome = Literal["done", "refused"]


class Action(StrEnum):
    """Every act the log records; a name outside this set cannot be written."""

    #: A thread's public link was turned on.
    THREAD_LINK_ON = "sharing.thread_link_on"
    #: A thread's public link was turned off.
    THREAD_LINK_OFF = "sharing.thread_link_off"
    #: What a thread's public link, already on, lets a visitor do changed.
    THREAD_LINK_PERMISSIONS_CHANGED = "sharing.thread_link_permissions_changed"
    #: A file's link was published, or its shared version updated.
    FILE_PUBLISHED = "sharing.file_published"
    #: A file's link stopped being shared.
    FILE_UNSHARED = "sharing.file_unshared"
    #: Who a file's link opens to changed: anyone, or members of the organization.
    FILE_AUDIENCE_CHANGED = "sharing.file_audience_changed"

    #: A thread, file or skill was published to the organization library.
    LIBRARY_PUBLISHED = "library.item_published"
    #: A library item's title or summary changed, or a skill was published anew.
    LIBRARY_UPDATED = "library.item_updated"
    #: A library item was removed, by its publisher or from the console.
    LIBRARY_REMOVED = "library.item_removed"

    #: One of the member's own MCP servers was added.
    CONNECTOR_ADDED = "connector.added"
    #: One of the member's own MCP servers was deleted.
    CONNECTOR_REMOVED = "connector.removed"
    #: An MCP server was turned on, for the account or for one workspace.
    CONNECTOR_ON = "connector.turned_on"
    #: An MCP server was turned off, for the account or for one workspace.
    CONNECTOR_OFF = "connector.turned_off"
    #: The member signed in to an MCP server, a brokerage or an organization connector.
    CONNECTOR_SIGNED_IN = "connector.signed_in"
    #: The member's sign-in to an MCP server or organization connector was removed.
    CONNECTOR_SIGNED_OUT = "connector.signed_out"
    #: An organization connector was turned on again for a workspace.
    ORG_CONNECTOR_ON = "org_connector.turned_on"
    #: An organization connector was turned off for a workspace.
    ORG_CONNECTOR_OFF = "org_connector.turned_off"
    #: A plugin was installed.
    PLUGIN_INSTALLED = "plugin.installed"
    #: A plugin was replaced by a newer copy.
    PLUGIN_UPDATED = "plugin.updated"
    #: A plugin was uninstalled.
    PLUGIN_REMOVED = "plugin.removed"

    #: One of the member's own model API keys was saved.
    MODEL_KEY_SAVED = "credential.model_key_saved"
    #: One of the member's own model API keys was removed.
    MODEL_KEY_REMOVED = "credential.model_key_removed"
    #: Where one of the member's own model API keys is sent changed.
    MODEL_KEY_ENDPOINT_CHANGED = "credential.model_key_endpoint_changed"
    #: The member signed in to a model provider or saved a Claude Code token.
    MODEL_SIGN_IN_CONNECTED = "credential.model_sign_in_connected"
    #: A model sign-in or Claude Code token was removed.
    MODEL_SIGN_IN_DISCONNECTED = "credential.model_sign_in_disconnected"
    #: A vault secret was created.
    SECRET_CREATED = "credential.secret_created"
    #: A vault secret's value was replaced.
    SECRET_REPLACED = "credential.secret_replaced"
    #: A vault secret was deleted.
    SECRET_DELETED = "credential.secret_deleted"

    #: A computer was added.
    COMPUTER_ADDED = "computer.added"
    #: A computer was removed.
    COMPUTER_REMOVED = "computer.removed"

    #: A shared workspace was created; its creator is its first manager.
    SHARED_WORKSPACE_CREATED = "shared_workspace.created"
    #: A shared workspace was deleted.
    SHARED_WORKSPACE_DELETED = "shared_workspace.deleted"
    #: A member was added to a shared workspace.
    SHARED_WORKSPACE_MEMBER_ADDED = "shared_workspace.member_added"
    #: A member was removed from a shared workspace, or left it.
    SHARED_WORKSPACE_MEMBER_REMOVED = "shared_workspace.member_removed"
    #: A member's role in a shared workspace changed, or whether everyone in
    #: the organization may use it.
    SHARED_WORKSPACE_ROLE_CHANGED = "shared_workspace.role_changed"
    #: Who may share a shared workspace's work outside it changed: its
    #: managers only, any member, or no one.
    SHARED_WORKSPACE_SHARING_CHANGED = "shared_workspace.sharing_changed"

    #: What the agent may do with orders changed.
    TRADING_PERMISSION_CHANGED = "trading.permission_changed"
    #: The member granted a brokerage's capability groups when connecting it.
    BROKERAGE_CONSENT_GRANTED = "trading.brokerage_consent_granted"

    #: The policy refused the member, or the member's agent, something.
    POLICY_REFUSED = "policy.refused"


#: A bound on one background write, the account read included: long enough
#: for a cold membership read to finish and be cached for the member's next
#: act, and the bound the export's settle horizon is derived from.
_WRITE_TIMEOUT_S = activity_events.RECORD_BOUND_S
#: Writes one process lets wait at once; past it an event is dropped and
#: logged rather than queued without limit behind a stalled database.
_MAX_PENDING = 512
#: How long shutdown waits for pending writes before abandoning them.
SHUTDOWN_DRAIN_S = 5.0
#: How long a refusal of a call the member's agent repeats is recorded only
#: once for the same key, so code retrying it in a loop writes one row.
_ONCE_WINDOW_S = 300

#: Strong references to the writes in flight, so the loop cannot collect one
#: before it lands. Execution context only: nothing reads it as state.
_pending: set[asyncio.Task] = set()

_MAX_KEYS = 16
_MAX_TEXT = 256
_MAX_LIST = 32
#: A key whose name says it holds a credential; such an entry is never kept.
_SECRET_KEY = re.compile(
    r"secret|token|passw|credential|api_?key|authoriz|bearer|cookie|private|"
    r"^value$|^key$|^code$|^header",
    re.IGNORECASE,
)

_REFUSAL_SEEN = "_activity_recorded"


def _scalar(value: Any) -> bool:
    if value is None or isinstance(value, (bool, int, float)):
        return True
    return isinstance(value, str) and len(value) <= _MAX_TEXT


def _allowed(key: Any, value: Any) -> bool:
    if not isinstance(key, str) or not key or _SECRET_KEY.search(key):
        return False
    if isinstance(value, (list, tuple)):
        return len(value) <= _MAX_LIST and all(_scalar(v) for v in value)
    return _scalar(value)


def clean_detail(detail: dict[str, Any] | None) -> tuple[dict[str, Any], list[str]]:
    """``detail`` as it may be written, and the keys left out of it."""
    kept: dict[str, Any] = {}
    refused: list[str] = []
    for key, value in (detail or {}).items():
        if len(kept) < _MAX_KEYS and _allowed(key, value):
            kept[key] = list(value) if isinstance(value, tuple) else value
        else:
            refused.append(str(key)[:64])
    return kept, refused


#: The request being served in this context, so a call site deep in a service
#: needs no request passed down to name the caller and trace. Execution
#: context only: each request's task sees its own.
_REQUEST: ContextVar[Scope | None] = ContextVar("activity_request", default=None)


def bind_request(scope: Scope) -> None:
    """Make ``scope`` the request records made in this context belong to."""
    _REQUEST.set(scope)


def _from_request(user_id: str) -> dict[str, Any]:
    scope = _REQUEST.get()
    if scope is None:
        return {}
    state = scope.get("state") or {}
    trace_id = state.get("trace_id")
    caller = caller_of(scope)
    found: dict[str, Any] = {"trace_id": str(trace_id) if trace_id else None}
    if caller is not None and caller.user_id == user_id:
        found.update(auth=caller.auth, client_id=caller.client_id)
    return found


def _id(value: Any) -> str | None:
    return None if value is None else str(value)


def _uuid(value: Any) -> str | None:
    """A workspace or thread id for its UUID column; anything else is left
    out rather than failing the whole row."""
    if value is None:
        return None
    try:
        return str(UUID(str(value)))
    except ValueError:
        return None


def once_key(*parts: Any) -> str:
    """The Redis key that claims one row per window for ``parts``."""
    digest = hashlib.sha256(json.dumps([str(p) for p in parts]).encode()).hexdigest()
    return f"activity:once:{digest[:40]}"


async def _first_in_window(once: tuple[Any, ...]) -> bool:
    """Whether this write claimed the window for ``once``. A Redis that
    cannot answer counts as claimed, so an outage drops repeats rather than
    writing a row per retried call."""
    from src.utils.cache.redis_cache import get_cache_client

    claimed = await get_cache_client().acquire_lock(
        once_key(*once), "1", _ONCE_WINDOW_S * 1000
    )
    return claimed is True


async def _write(
    user_id: str, action: Action, row: dict[str, Any], once: tuple[Any, ...] | None
) -> None:
    try:
        async with asyncio.timeout(_WRITE_TIMEOUT_S):
            account = await accounts.account_for(user_id)
            if account is None:
                logger.warning(
                    "[activity] not recorded, account unknown: action=%s actor=%s",
                    action, user_id,
                )
                return
            if account.org is None:
                return
            if once is not None and not await _first_in_window(once):
                return
            await activity_events.insert(
                org_id=account.org.id, actor_id=user_id, action=action.value, **row
            )
    except asyncio.CancelledError:
        logger.error(
            "[activity] not recorded, abandoned: action=%s actor=%s", action, user_id
        )
        raise
    except Exception:
        logger.error(
            "[activity] not recorded: action=%s actor=%s", action, user_id, exc_info=True
        )


def _spawn(
    user_id: str, action: Action, row: dict[str, Any], once: tuple[Any, ...] | None
) -> None:
    if len(_pending) >= _MAX_PENDING:
        logger.error(
            "[activity] not recorded, %d writes already pending: action=%s actor=%s",
            len(_pending), action, user_id,
        )
        return
    task = asyncio.get_running_loop().create_task(
        _write(user_id, action, row, once), name=f"activity-{action.value}"
    )
    _pending.add(task)
    task.add_done_callback(_pending.discard)


async def drain(timeout: float | None = None) -> None:
    """Wait for this loop's pending writes; past ``timeout`` the rest are
    cancelled, each logged as not recorded. Shutdown passes
    :data:`SHUTDOWN_DRAIN_S`; a test passes none to assert the rows."""
    loop = asyncio.get_running_loop()
    deadline = None if timeout is None else loop.time() + timeout
    while waiting := [t for t in _pending if t.get_loop() is loop and not t.done()]:
        left = None if deadline is None else deadline - loop.time()
        if left is not None and left <= 0:
            for task in waiting:
                task.cancel()
            await asyncio.gather(*waiting, return_exceptions=True)
            return
        await asyncio.wait(waiting, timeout=left)


def record(
    user_id: str,
    action: Action,
    *,
    outcome: Outcome = "done",
    resource: tuple[str, Any] | None = None,
    workspace_id: Any = None,
    thread_id: Any = None,
    detail: dict[str, Any] | None = None,
    once: tuple[Any, ...] | None = None,
    auth: str | None = None,
) -> None:
    """Record that ``user_id`` did ``action``, if their account is in an
    organization. Returns at once and never raises; the row is written in the
    background.

    ``resource`` is ``(kind, id)``. ``once`` keeps the event to one row per
    window for that key, for a refusal the member's agent may repeat.
    ``auth`` says how the act was made when no member request made it
    (:data:`AGENT`), in place of the request's caller.
    """
    try:
        if not isinstance(action, Action):
            raise TypeError(f"not an activity action: {action!r}")
        if not membership.teams_door_open():
            return
        kept, refused = clean_detail(detail)
        if refused:
            logger.error(
                "[activity] left out of %s detail, not a name, id or flag: %s",
                action, refused,
            )
        kind, rid = resource if resource is not None else (None, None)
        row: dict[str, Any] = {
            "outcome": outcome,
            "resource_kind": kind,
            "resource_id": _id(rid),
            "workspace_id": _uuid(workspace_id),
            "thread_id": _uuid(thread_id),
            "detail": kept,
            **_from_request(user_id),
        }
        if auth is not None:
            row.update(auth=auth, client_id=None)
        _spawn(user_id, action, row, once)
    except Exception:
        logger.error(
            "[activity] not recorded: action=%s actor=%s", action, user_id, exc_info=True
        )


def record_refusal(
    exc: BaseException,
    user_id: str | None,
    *,
    workspace_id: Any = None,
    thread_id: Any = None,
    detail: dict[str, Any] | None = None,
    once: tuple[Any, ...] | None = None,
    auth: str | None = None,
) -> None:
    """Record a policy refusal once, whichever of its surfaces sees it first.

    A refusal can pass a turn's error frame and then the exception handler on
    its way out; the mark on the exception keeps it to one row. The ids the
    request's path names say what was refused, so no raise site passes them.
    """
    if not isinstance(exc, PolicyDenied) or not user_id or getattr(exc, _REFUSAL_SEEN, False):
        return
    setattr(exc, _REFUSAL_SEEN, True)
    found = exc.detail if isinstance(exc.detail, dict) else {}
    ids = _path_ids()
    rule = found.get("rule")
    record(
        user_id,
        Action.POLICY_REFUSED,
        outcome="refused",
        resource=("rule", rule) if rule else None,
        workspace_id=workspace_id or ids.pop("workspace_id", None),
        thread_id=thread_id or ids.pop("thread_id", None),
        detail={
            "rule": rule,
            "set_by": found.get("set_by"),
            **({"model": found["model"]} if found.get("model") else {}),
            **_route(),
            **{k: v for k, v in ids.items() if k not in ("workspace_id", "thread_id")},
            **(detail or {}),
        },
        once=once,
        auth=auth,
    )


#: The ``auth`` of a row the member's agent made, not a member request: a
#: tool call, or a relay call made with the relay's own token.
AGENT = "agent"


def record_withheld_call(
    user_id: str,
    policy: Policy | None,
    rule: str,
    *,
    via: str,
    caller: str | None,
    server: str | None,
    tool: str | None,
    workspace_id: Any = None,
    detail: dict[str, Any] | None = None,
) -> None:
    """A call the member's agent made that the policy withheld, at the gate
    ``via`` names (``relay``, ``direct``), once per member, rule, server and
    tool in a window however often the agent retries it. ``caller`` is who
    dialled it (sandbox code or the host). An unknown policy records nothing:
    the account it would be recorded under is unknown too."""
    if policy is None:
        return
    record(
        user_id,
        Action.POLICY_REFUSED,
        outcome="refused",
        resource=("rule", rule),
        workspace_id=workspace_id,
        detail={
            "rule": rule, "set_by": policy.set_by(rule),
            "via": via, "caller": caller, "server": server, "tool": tool,
            **(detail or {}),
        },
        once=(user_id, rule, server, tool),
        auth=AGENT,
    )


def host_of(url: Any) -> str | None:
    """The host a URL names, for ``detail``: never its userinfo, path or
    query, and None for anything that does not parse. Never raises."""
    try:
        return urlsplit(str(url)).hostname if url else None
    except Exception:
        return None


def _route() -> dict[str, Any]:
    """The route a refusal answered, as its template so no id rides along."""
    scope = _REQUEST.get()
    if scope is None:
        return {}
    path = getattr(scope.get("route"), "path", None)
    return {"method": scope.get("method"), "route": path} if path else {}


def _path_ids() -> dict[str, str]:
    """The ids the request's path names (``thread_id``, ``computer_id``, ...)
    that are UUIDs and whose names may be kept."""
    scope = _REQUEST.get()
    params = (scope or {}).get("path_params") or {}
    ids: dict[str, str] = {}
    for name, value in params.items():
        if isinstance(name, str) and name.endswith("_id") and not _SECRET_KEY.search(name):
            if (found := _uuid(value)) is not None:
                ids[name] = found
    return ids
