"""The user's OK before a computer's token deletes, creates a workspace,
removes a computer, or starts work on a computer other than its own.

Agent code holds a computer's token, so what cannot be undone, what adds a
place for work to go, and what runs a turn on another computer (a message, a
retry, an automation created, edited or run there) waits for the user's
answer in the app. The request itself waits: each such route calls its
surface here once, after its own checks and before its effect, and ``gate``
writes the request down (or finds it, when the same command asks again),
tells the app, and holds the request open for an answer, so an approval runs
the route in that same request and the agent sees the real result. A request
still unanswered when the hold ends is refused with ``approval_pending``, and
the CLI sends it again naming that request (``APPROVAL_HEADER``), which holds
on it again until it is answered or expires. A send naming a request that is
no longer waiting is refused with ``approval_expired`` and asks nothing.

A turn is held before its admission, so a declined or unanswered send leaves
no run behind. What a request is for is its intent (``_ask``): the action,
the ids its path names, the query that changes what it does and its whole
body, so a different message asks anew and the same one sent again finds its
own request.

What could never happen is refused before anything is asked, so the user is
never asked to approve a request that then fails: the thread or the
workspace the command runs in, and what each route checks before it calls
its surface. A target the user does not have passes through unasked, since
the route answers its own 404 and does nothing, as does a turn's retransmit,
which its route answers as a duplicate.

The requester the app shows comes from the call's own report of the thread
it runs in, a label, never authority.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from typing import Any, Final

from fastapi import HTTPException, Request
from pydantic import BaseModel, ValidationError

from ptc_agent.core.sandbox.providers import MACHINE_KINDS
from src.config.settings import (
    get_cli_approval_hold_seconds,
    get_cli_approval_max_pending,
    get_cli_approval_pending_ttl,
)
from src.server.database.automation import get_automation
from src.server.database.computer import (
    get_computer,
    get_primary_computer,
)
from src.server.database.conversation.threads_read import (
    get_thread_auth_meta,
    get_thread_by_id,
)
from src.server.database.home_workspace import is_home_row
from src.server.database.runs import lifecycle as runs_db
from src.server.database.turn_targets import Place, in_shared_workspace, turn_place
from src.server.database.workspace import (
    free_workspace_name,
    get_live_workspace_ids_for_computer,
    get_workspace,
)
from src.server.dependencies.principal import (
    SHARED_WORKSPACE_REFUSAL,
    CliCall,
    Principal,
    check_turn_body,
    computer_call,
    forbidden,
    reaches_shared,
    refuse_loop,
    refuse_own,
    refuse_rewrite,
    uuid_or_none,
)
from src.server.models.automation import AutomationCreate, AutomationUpdate
from src.server.models.chat import ChatRequest
from src.server.models.computer import ComputerRemoveRequest
from src.server.models.workflow import RetryRequest
from src.server.models.workspace import WorkspaceCreate
from src.server.services import cli_approvals as store
from src.server.services import membership
from src.server.services.home_placement import home_turn_computer
from src.server.services.runs.admission import run_named_by_key
from src.server.services.runs.pending_input import can_retry
from src.server.services.automations.lifecycle import (
    TRIGGERABLE,
    AutomationRefusal,
    check_claude_code_hosted,
    check_delivery,
    check_model,
    resume_refusal,
)
from src.server.services.turn_runtime import ensure_home, named_workspace_id
from src.tools.secretary.approvals import AUTO_APPROVE_WORKSPACE_CREATION, standing_approval

logger = logging.getLogger(__name__)

#: Header naming the request a send already waits on, so a send that crosses
#: the user's answer, or outlives its request, is told so rather than asking
#: again.
APPROVAL_HEADER: Final = "X-Langalpha-Approval"

#: How much of a message or an automation's prompt the app shows.
PREVIEW_CHARS: Final = 280

# A held request asks again at most this many times, should copies of it keep
# taking each other's approvals or moving its request.
_MAX_ROUNDS: Final = 5


@dataclass(frozen=True)
class Ask:
    """What the user is asked to approve. ``intent`` is what an approval is
    for, so only the same command takes it; ``target`` and ``details`` are
    what the app shows."""

    action: str
    intent: dict[str, Any]
    target: dict[str, Any]
    details: dict[str, Any]

    @property
    def digest(self) -> str:
        return hashlib.sha256(
            json.dumps(self.intent, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()


def _ask(
    action: str,
    *,
    target: dict[str, Any],
    details: dict[str, Any],
    kind: str | None = None,
    path: dict[str, Any] | None = None,
    query: dict[str, Any] | None = None,
    body: BaseModel | None = None,
    computer_id: str | None = None,
    acts_on: dict[str, Any] | None = None,
) -> Ask:
    """The one rule for what a request is for: the action (and ``kind``, the
    route among those sharing one), the ids its path names, the parsed query
    that changes what it does, the whole parsed body, and the computer the
    user is shown it runs or is created on, so an approval never carries the
    work to another one (a workspace may move, or the primary change, while
    the request waits), and ``acts_on``, the stored state that decides what
    runs, so an edit while it waits needs a new OK. A turn's request key is in its body, and a retransmit
    sends the same one, so the same command sent again still finds its own
    request."""
    intent = {
        "action": action,
        "kind": kind,
        "path": path or {},
        "query": query or {},
        # What the request set, so a field left out and one sent as its
        # default stay apart where the route tells them apart.
        "body": body.model_dump(mode="json", exclude_unset=True) if body is not None else None,
    }
    if computer_id is not None:
        intent["computer_id"] = computer_id
    if acts_on is not None:
        intent["acts_on"] = json.loads(json.dumps(acts_on, default=str))
    return Ask(action, intent, target, details)


Builder = Callable[[CliCall | None], Awaitable[Ask | None]]


def held_for_approval(principal: Principal) -> bool:
    """Whether the caller is a computer's token, which the user approves for
    (a signed-in person and a person's own CLI login are not): the one test,
    for a route that refuses such a caller instead of asking."""
    return principal.computer_id is not None


async def gate(request: Request, principal: Principal, build: Builder) -> None:
    """Return once the user approved what ``build`` asks, taking the
    approval; raise otherwise. Only a computer's token is held, and ``build``
    answering None asks nothing."""
    if not held_for_approval(principal):
        return
    call = computer_call(request.scope)
    ask = await _built(build, call)
    if ask is None:
        return
    try:
        await _hold(request, principal, call, ask)
    except store.Unavailable:
        # Nothing runs without an approved record to take, and none can be
        # read or written.
        logger.warning(
            "CLI approval unavailable (user=%s, action=%s)", principal.user_id, ask.action
        )
        raise _refusal(
            503,
            "approval_unavailable",
            "Can't ask for approval right now; nothing was done. Try again shortly.",
        ) from None
    # The user approved what they were shown; where it would run may have
    # changed while the request waited (its workspace or automation moved).
    again = await _built(build, call)
    if again is not None and again.digest != ask.digest:
        raise _expired()


async def _built(build: Builder, call: CliCall | None) -> Ask | None:
    """What ``build`` asks, as the call's own: the dialog names the thread
    that asked, so the same request from another thread is another one."""
    ask = await build(call)
    if ask is None:
        return None
    return replace(ask, intent={**ask.intent, "requester": call.thread_id if call else None})


async def _hold(request: Request, principal: Principal, call: CliCall | None, ask: Ask) -> None:
    user_id = principal.user_id
    named = uuid_or_none(request.headers.get(APPROVAL_HEADER))
    deadline = time.monotonic() + get_cli_approval_hold_seconds()
    summary: dict[str, Any] | None = None
    last: store.Approval | None = None
    for _ in range(_MAX_ROUNDS):
        if await request.is_disconnected():
            # The command quit: an OK given meanwhile is left untaken.
            raise _expired()
        found = await store.ask(
            user_id,
            principal.computer_id,
            ask.digest,
            action=ask.action,
            named=named,
            summary=summary,
            ttl=get_cli_approval_pending_ttl(),
            max_pending=get_cli_approval_max_pending(),
        )
        if found.outcome == store.TAKEN:
            await store.announce(user_id, found.approval_id, store.USED)
            return
        if found.outcome == store.REJECTED:
            raise _rejected()
        if found.outcome == store.LIMIT:
            raise _refusal(
                429,
                "approval_limit",
                "Too many requests are waiting for the user's answer in the app; "
                "wait for them to be answered or to expire",
            )
        if found.outcome == store.ABSENT:
            if named is not None:
                # What it waited on lapsed, or a copy of the command took it.
                raise _expired()
            summary = {
                "target": ask.target,
                "details": ask.details,
                "requester": await _requester(principal, call),
            }
            continue
        if found.outcome == store.MOVED:
            continue
        if found.outcome == store.CREATED:
            await store.announce(user_id, found.approval_id, store.PENDING)
        answer = await store.wait_for_answer(user_id, found.approval_id, deadline)
        if answer is not None:
            last = answer
            if answer.status == store.REJECTED:
                raise _rejected()
            if answer.status == store.PENDING:
                raise _pending(answer)
        # Approved, which the next round takes; or used by a copy of the
        # command, or gone, which the next round finds absent.
    if last is not None:
        raise _pending(last)
    raise store.Unavailable()


def _refusal(status: int, kind: str, message: str, **extra: Any) -> HTTPException:
    return HTTPException(status_code=status, detail={"type": kind, "message": message, **extra})


def _expired() -> HTTPException:
    return _refusal(
        403,
        "approval_expired",
        "This request is no longer waiting in the app; nothing was done. "
        "Run the command again to ask again.",
    )


def _rejected() -> HTTPException:
    return _refusal(
        403, "approval_rejected", "The user declined this in the app; nothing was done"
    )


def _pending(approval: store.Approval) -> HTTPException:
    return _refusal(
        403,
        "approval_pending",
        "Waiting for the user to approve this in the LangAlpha app",
        approval_id=approval.approval_id,
        expires_at=approval.expires_at.isoformat(),
    )


async def _requester(principal: Principal, call: CliCall | None) -> dict[str, Any]:
    """Who asked, as the call reports it: the thread's agent, or a command
    that names no thread."""
    computer = await get_computer(principal.computer_id)
    requester: dict[str, Any] = {
        "kind": "command",
        "thread_id": None,
        "thread_title": None,
        "workspace_id": None,
        "workspace_name": None,
        "computer_name": computer.get("name") if computer else None,
    }
    thread = await get_thread_auth_meta(call.thread_id) if call and call.thread_id else None
    # The id was the user's thread when the command started; deleted since,
    # it can be another account's now, whose names are not this user's to see.
    if thread is None or thread["user_id"] != principal.user_id:
        return requester
    workspace = await get_workspace(str(thread["workspace_id"]))
    home = is_home_row(workspace)
    requester.update(
        kind="chief_of_staff" if home else "analyst",
        thread_id=call.thread_id,
        thread_title=thread.get("title"),
        workspace_id=str(thread["workspace_id"]),
        workspace_name=None if home or workspace is None else workspace.get("name"),
    )
    return requester


# -- what cannot be undone, and a new workspace -------------------------------------


async def thread_delete(
    request: Request, principal: Principal, thread_id: str, thread: dict[str, Any] | None
) -> None:
    """Before a thread the user owns is deleted; ``thread`` is its row."""

    async def build(call: CliCall | None) -> Ask:
        target_id = uuid_or_none(thread_id)
        refuse_own(call, target_id, "A command cannot delete the conversation it runs in")
        # Read again each build: the one after the OK must still find the
        # user's thread at this id, which a deleted one's may no longer be.
        row = await get_thread_auth_meta(target_id) if target_id is not None else None
        if row is None or row["user_id"] != principal.user_id:
            raise HTTPException(status_code=404, detail="Thread not found")
        return _ask(
            "thread_delete",
            path={"thread_id": target_id},
            target={"id": target_id, "name": row.get("title")},
            details={},
            # A thread made again at this id is another thread.
            acts_on={"created_at": row.get("created_at")},
        )

    await gate(request, principal, build)


async def workspace_delete(
    request: Request, principal: Principal, workspace: dict[str, Any], *, delete_files: bool
) -> None:
    """Before a workspace the user owns, never Home, is deleted."""
    workspace_id = str(workspace["workspace_id"])

    async def build(call: CliCall | None) -> Ask:
        running = call.running_thread() if call is not None else None
        if running is not None:
            thread = await get_thread_by_id(running)
            if thread is not None and str(thread["workspace_id"]) == workspace_id:
                raise forbidden("A command cannot delete the workspace it runs in")
        return _ask(
            "workspace_delete",
            path={"workspace_id": workspace_id},
            query={"delete_files": delete_files},
            target={"id": workspace_id, "name": workspace.get("name")},
            details={"delete_files": delete_files},
        )

    await gate(request, principal, build)


async def workspace_create(
    request: Request, principal: Principal, body: WorkspaceCreate
) -> str | None:
    """Before a workspace is created, unless the user lets workspaces be
    created unasked. Returns the computer the user approved it on, which
    the route creates it on: one naming none shows the primary, which may
    change after the user's OK.

    Raises:
        WorkspaceNameInvalid, WorkspaceNameTaken: what the insert would
            refuse, which the route answers as it answers the insert's.
    """

    async def build(call: CliCall | None) -> Ask | None:
        if await standing_approval(principal.user_id, AUTO_APPROVE_WORKSPACE_CREATION):
            return None
        name = await free_workspace_name(principal.user_id, body.name)
        requested = str(body.computer_id) if body.computer_id else None
        if requested is None:
            computer = await get_primary_computer(principal.user_id)
        else:
            computer = await get_computer(requested)
            if computer is None or computer.get("user_id") != principal.user_id:
                return None  # The route's own 404.
        on = str(computer["computer_id"]) if computer else None
        # The first build's is the one the user is shown; the one after the
        # OK may find "Always allow" just turned on and ask nothing.
        if on is not None and not approved:
            approved.append(on)
        return _ask(
            "workspace_create",
            body=body,
            target={"id": None, "name": name},
            details={
                "description": body.description,
                "computer_id": requested,
                "computer_name": computer.get("name") if computer else None,
            },
            computer_id=on,
        )

    approved: list[str] = []
    await gate(request, principal, build)
    return approved[0] if approved else None


async def automation_delete(request: Request, principal: Principal, automation_id: str) -> None:
    async def build(call: CliCall | None) -> Ask | None:
        automation = await get_automation(automation_id, principal.user_id)
        if automation is None:
            return None  # The route's own 404.
        return _ask(
            "automation_delete",
            path={"automation_id": automation_id},
            target={"id": automation_id, "name": automation.get("name")},
            details={},
        )

    await gate(request, principal, build)


async def computer_remove(
    request: Request,
    principal: Principal,
    computer: dict[str, Any],
    *,
    remove_key: bool,
    delete_files: bool,
    delete_workspaces: list[str],
    body: ComputerRemoveRequest | None,
) -> None:
    """Before a computer the user owns is removed, once the route found
    nothing that would refuse it (``WorkspaceManager.removal_refusal``)."""
    computer_id = str(computer["computer_id"])

    async def build(call: CliCall | None) -> Ask:
        on_it = [
            str(w)
            for w in await get_live_workspace_ids_for_computer(computer_id, exclude_stand_in=True)
        ]
        names = [(await get_workspace(w) or {}).get("name") for w in on_it]
        # The folders go only from a whole machine, and the key line only from an
        # ssh one, so the user is told of each only where it acts.
        kind = computer.get("kind")
        machine = kind in MACHINE_KINDS
        ssh = kind == "ssh"
        return _ask(
            "computer_remove",
            path={"computer_id": computer_id},
            query={
                "remove_key": remove_key,
                "delete_files": delete_files,
                "delete_workspaces": delete_workspaces,
            },
            body=body,
            target={"id": computer_id, "name": computer.get("name")},
            details={
                "remove_key": remove_key and ssh,
                "delete_files": delete_files and machine,
                "delete_workspaces": on_it,
                "delete_workspace_names": names,
            },
        )

    await gate(request, principal, build)


# -- work started on another computer ---------------------------------------------


async def _refuse_shared(
    principal: Principal, *, workspace_id: Any = None, thread_id: Any = None
) -> None:
    """A computer's token starts no turn in a shared workspace its request's
    body names, as ``require_scopes`` refuses one its path or query names.
    The turn would run on the workspace's own computer, none of the user's,
    so no OK of theirs covers it, and the token is their own agent's, which
    the workspace's members did not let in. Read nothing with the ``teams``
    door shut, and leave what the user does not reach to the route, as
    ``require_scopes`` does (``reaches_shared``)."""
    if principal.computer_id is None or not membership.teams_door_open():
        return
    ids = {"workspace_id": uuid_or_none(workspace_id), "thread_id": uuid_or_none(thread_id)}
    if await in_shared_workspace(**ids) and await reaches_shared(principal.user_id, **ids):
        raise forbidden(SHARED_WORKSPACE_REFUSAL)


async def _turn_place(
    user_id: str, *, workspace_id: str | None = None, thread_id: str | None = None
) -> Place | None:
    """``turn_place``, with Home created and put on its computer before the
    user is asked, as its first turn would: a Home on none follows the
    primary, which could change between the user's OK and the turn, and a
    bound one never moves. A failure refuses the request rather than ask
    about a computer the turn might not use.

    A Home turn is judged on the computer it runs on, which the user is shown
    (``home_placement.home_turn_computer``). Read only: the turn's bind places
    the stand-in, and its START refuses one that then runs anywhere else
    (``admission.refuse_moved_off``)."""
    place = await turn_place(user_id, workspace_id=workspace_id, thread_id=thread_id)
    if place is None or not place.in_home:
        return place
    await ensure_home(user_id)
    place = await turn_place(user_id, workspace_id=workspace_id, thread_id=thread_id)
    if place is None:
        return None
    cloud, stand_in = await home_turn_computer(user_id)
    if not stand_in:
        return place
    return replace(place, computer_id=str(cloud["computer_id"]), computer_name=cloud.get("name"))


def _checked(checked: list[str], place: Place | None) -> None:
    """Record the computer a turn was first checked to run on: the one the
    user is shown when it asks, and the one a move must not take it off when
    it asks nothing (``admission.refuse_moved_off``)."""
    if not checked and place is not None and place.computer_id is not None:
        checked.append(place.computer_id)


def _elsewhere(principal: Principal, place: Place | None) -> bool:
    """Whether a turn there runs on a computer other than the token's own;
    a place the user does not have is the route's own 404."""
    return place is not None and place.computer_id != principal.computer_id


def _place_details(place: Place) -> dict[str, Any]:
    return {
        "workspace_name": place.workspace_name,
        "computer_name": place.computer_name,
        "in_home": place.in_home,
    }


def _preview(text: str | None) -> str | None:
    text = (text or "").strip()
    if not text:
        return None
    return text if len(text) <= PREVIEW_CHARS else text[: PREVIEW_CHARS - 1].rstrip() + "…"


async def _retransmit(thread_id: str, request_key: str | None) -> bool:
    """Whether this key already reached a run: its route answers the copy
    as a duplicate, so asking would put a request already done to the user."""
    return bool(request_key) and await run_named_by_key(thread_id, request_key) is not None


async def admit_turn(
    request: Request,
    principal: Principal,
    body: ChatRequest,
    thread_id: str | None,
    *,
    from_path: bool = False,
) -> str | None:
    """What a turn's body may ask (``check_turn_body``), then the user's OK
    for one that would run on another computer, before its admission.
    ``thread_id`` is the path's, or the one an external id resolved to; None
    for a thread minted for it. Returns the computer it was checked to run
    on, for a computer's token, which its START holds it to (``_checked``)."""
    await check_turn_body(principal, request, body, thread_id, from_path=from_path)
    # A path's thread is ``require_scopes``'s; an external id's is not.
    await _refuse_shared(
        principal, workspace_id=body.workspace_id, thread_id=None if from_path else thread_id
    )

    async def build(call: CliCall | None) -> Ask | None:
        tid = uuid_or_none(thread_id) if thread_id is not None else None
        if thread_id is not None and tid is None:
            return None  # The route's own 404.
        thread = await get_thread_auth_meta(tid) if tid is not None else None
        if thread is not None and (
            thread["user_id"] != principal.user_id or await _retransmit(tid, body.request_key)
        ):
            return None  # The route's own 403, or its duplicate answer.
        # Again here, since the gate builds again after the user's OK: a
        # send into the caller's thread meanwhile changes who sent its turn.
        await refuse_loop(principal, call, tid)
        # Where ``_handle_send_message`` runs it.
        workspace_id = named_workspace_id(body.workspace_id, thread)
        if workspace_id is None:
            return None  # The route's own 400.
        place = await _turn_place(principal.user_id, workspace_id=workspace_id)
        _checked(checked, place)
        if not _elsewhere(principal, place):
            return None
        details = {"message_preview": _preview(body.message_text()), **_place_details(place)}
        if thread is not None:
            return _ask(
                "thread_send",
                kind="send",
                path={"thread_id": tid},
                body=body,
                target={"id": tid, "name": thread.get("title")},
                details={"kind": "send", **details},
                computer_id=place.computer_id,
            )
        return _ask(
            "thread_create",
            path={"thread_id": tid},
            body=body,
            target={"id": place.workspace_id, "name": place.workspace_name},
            details=details,
            computer_id=place.computer_id,
        )

    checked: list[str] = []
    await gate(request, principal, build)
    return checked[0] if checked else None


async def admit_retry(
    request: Request, principal: Principal, thread_id: str, body: RetryRequest | None
) -> str | None:
    """The history a retry may not rewrite, then the user's OK for one that
    would run on another computer, before its admission. A retry the route
    refuses once admitted (none to retry, a stale run, a retransmit) is not
    asked. Returns the computer it was checked to run on, as ``admit_turn``."""
    refuse_rewrite(principal, body.checkpoint_id if body else None)
    # The path's thread is ``require_scopes``'s.
    await _refuse_shared(principal, workspace_id=body.workspace_id if body else None)

    async def build(call: CliCall | None) -> Ask | None:
        tid = uuid_or_none(thread_id)
        thread = await get_thread_auth_meta(tid) if tid is not None else None
        if thread is None or thread["user_id"] != principal.user_id:
            return None
        await refuse_loop(principal, call, tid)
        if body is not None and await _retransmit(tid, body.request_key):
            return None
        latest = await runs_db.get_latest_attempt(tid)
        if latest is None or latest["status"] == "in_progress" or not can_retry(latest):
            return None
        if body is not None and body.run_id and body.run_id != str(
            latest["conversation_response_id"]
        ):
            return None
        workspace_id = named_workspace_id(body.workspace_id if body else None, thread)
        place = await _turn_place(principal.user_id, workspace_id=workspace_id)
        _checked(checked, place)
        if not _elsewhere(principal, place):
            return None
        return _ask(
            "thread_send",
            kind="retry",
            path={"thread_id": tid},
            body=body,
            target={"id": tid, "name": thread.get("title")},
            details={"kind": "retry", "message_preview": None, **_place_details(place)},
            computer_id=place.computer_id,
        )

    checked: list[str] = []
    await gate(request, principal, build)
    return checked[0] if checked else None


#: What decides the work an automation's runs do, so a change to any of
#: them while a request waits needs a new OK.
_RUN_FIELDS: Final = (
    "name",
    "instruction",
    "llm_model",
    "additional_context",
    "workspace_id",
    "thread_strategy",
    "conversation_thread_id",
    "trigger_type",
    "cron_expression",
    "trigger_config",
    "timezone",
    "delivery_config",
)


async def automation_runs(
    request: Request,
    principal: Principal,
    kind: str,
    *,
    automation_id: str | None = None,
    body: AutomationCreate | AutomationUpdate | None = None,
) -> str | None:
    """Before an automation is created (``body``), edited (``automation_id``
    and ``body``), run now or resumed (``automation_id``): never into the thread the
    call runs in, or the one that sent it its current turn, whose turns its
    runs would start, and with the user's OK when its runs would go to
    another computer.

    Returns the computer a run now was checked to run on, which its turn is
    held to at START, as a send's is."""
    if principal.computer_id is None:
        return None
    if body is not None and body.conversation_thread_id is not None:
        await refuse_loop(principal, computer_call(request.scope), body.conversation_thread_id)

    async def build(call: CliCall | None) -> Ask | None:
        stored: dict[str, Any] = {}
        if automation_id is not None:
            stored = await get_automation(automation_id, principal.user_id) or {}
            if not stored:
                return None  # The route's own 404.
        if isinstance(body, AutomationUpdate) and stored:
            # The route reads an edit by the stored trigger's kind.
            try:
                AutomationUpdate.model_validate(
                    body.model_dump(exclude_unset=True),
                    context={"trigger_type": stored.get("trigger_type")},
                )
            except ValidationError:
                return None  # The route's own 422.
        # What the route saves: it drops a field the body sets to null.
        fields = body.model_dump(exclude_none=True) if body is not None else {}

        def after(name: str) -> Any:
            return fields.get(name, stored.get(name))

        moved = "workspace_id" in fields
        workspace_id = after("workspace_id")
        if not workspace_id:
            # A create naming none is the route's refusal; an older row on
            # none fails its runs, so none start.
            return None
        # What the route itself refuses for this kind, before anyone is asked.
        if kind == "trigger" and stored.get("status") not in TRIGGERABLE:
            return None
        if kind == "resume" and resume_refusal(stored) is not None:
            return None
        # A model the route would refuse, checked as it checks one: only a new name.
        model = body.llm_model if body is not None else None
        runs_on = after("llm_model")
        try:
            if model and model != stored.get("llm_model"):
                await check_model(principal.user_id, model, None)
            # A Claude Code model and a place are judged together, as the route does.
            if not stored or runs_on != stored.get("llm_model") or moved:
                await check_claude_code_hosted(principal.user_id, runs_on, str(workspace_id))
        except AutomationRefusal:
            return None  # The route's own refusal.
        # A pin names a thread in the automation's workspace, or the route
        # refuses the write.
        pin = fields.get("conversation_thread_id")
        if pin is not None:
            pinned = await get_thread_auth_meta(str(pin))
            if (
                pinned is None
                or pinned["user_id"] != principal.user_id
                or str(pinned["workspace_id"]).lower() != str(workspace_id).lower()
            ):
                return None  # The route's own refusal.
        # The thread its runs would start turns in once written: the one the
        # write names, else none for a move to another workspace, which
        # unpins, else the stored one. Checked again after the OK.
        if pin is not None:
            runs_into = pin
        elif moved and uuid_or_none(str(workspace_id)) != uuid_or_none(
            str(stored.get("workspace_id") or "")
        ):
            runs_into = None
        else:
            runs_into = stored.get("conversation_thread_id")
        await refuse_loop(principal, call, runs_into)
        # Its runs go to its workspace: the executor continues a pinned thread
        # only while it lives there, and makes a missing one again there.
        place = await _turn_place(principal.user_id, workspace_id=str(workspace_id))
        _checked(checked, place)
        if not _elsewhere(principal, place):
            return None
        # A chat the route would refuse to deliver to, checked only before
        # asking, since it asks the messaging service: a save that cannot
        # succeed does not ask the user first.
        delivery = fields.get("delivery_config") or {}
        if delivery.get("methods"):
            saved = (stored.get("delivery_config") or {}).get("methods") or []
            try:
                await check_delivery(principal.user_id, delivery["methods"], stored=saved)
            except AutomationRefusal:
                return None  # The route's own refusal.
        name = after("name")
        return _ask(
            "automation_change",
            kind=kind,
            path={"automation_id": automation_id},
            body=body,
            target={"id": automation_id, "name": name},
            details={
                "kind": kind,
                "automation_name": name,
                "prompt_preview": _preview(after("instruction")),
                **_place_details(place),
            },
            computer_id=place.computer_id,
            acts_on={field: stored.get(field) for field in _RUN_FIELDS} if stored else None,
        )

    checked: list[str] = []
    await gate(request, principal, build)
    return checked[0] if checked else None
