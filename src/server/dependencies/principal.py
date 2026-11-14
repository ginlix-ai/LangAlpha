"""Caller identity, and what it may do, for the routes that also serve tokens.

Opt-in on purpose. ``get_current_user_id`` refuses every token but a session,
so a route that does not depend on ``require_scopes`` keeps refusing an
outside app, a personal access token and a computer's token alike. What no
scope grants (orders, secrets, publishing, identity) stays on those routes.

A computer's token (``lac1.``, what the ``la`` CLI presents from inside a
computer) is held by agent code, and every computer's is held to one rule.
It reaches all of the user's computers and Home, since the Chief of Staff
manages everything through ``la`` and an analyst reads the workspaces and
threads it builds on. It adds no computer, leaves its own computer running,
answers no approval, rewrites no history, and sends nothing into the thread
its call came from or the one that sent that thread its current turn. What
cannot be undone, what adds a place for work to go, and what starts work on
another computer waits until the user approves that exact request in the app
(``cli_approval``): a delete, a new workspace, a removed computer, and a
message, a retry or an automation whose turn would run on a computer other
than the token's own. Home runs on whichever computer holds it.
"""

from __future__ import annotations

import functools
import uuid
from dataclasses import dataclass
from typing import Annotated, Any, Final, Literal

from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from starlette.types import Scope

from src.server.auth import computer_token_core, oauth_resource, pats
from src.server.database import order_attempts, turn_targets
from src.server.models.chat import ChatRequest, ThreadOrigin
from src.server.services import cli_tokens
from src.server.services.livefs import outcomes
from src.server.utils.api import _service_token_user_id, get_current_user_id

_bearer = HTTPBearer(auto_error=False)

#: The call a computer's ``la`` request came from, ``LIVEFS_CALL_ID`` of the
#: command that ran it.
CALL_HEADER = "X-Langalpha-Call"

Auth = Literal["session", "service", "oauth", "pat", "computer"]


@dataclass(frozen=True)
class Principal:
    user_id: str
    auth: Auth
    client_id: str | None = None
    # What a token was granted. Empty for a first-party session or the
    # server's own call, which no scope limits.
    scopes: tuple[str, ...] = ()
    # The computer whose token this is; None for every other caller.
    computer_id: str | None = None

    @property
    def unlimited(self) -> bool:
        return self.auth in ("session", "service")


@dataclass(frozen=True)
class CliCall:
    """A computer's request beyond its token, read once in ``get_principal``.

    ``thread_id`` is the thread the call ran in, as its command filed it: a
    loop guard and a label, never authority, since any process on the
    computer can read a sibling's environment and send its id.
    """

    principal: Principal
    call_id: str | None
    thread_id: str | None

    def running_thread(self) -> str | None:
        """The thread the command runs in; None for a command that names no
        call, a person's or a schedule's on the machine.

        Raises:
            HTTPException: the command named its call, but what it filed is
                gone (Redis is down, or the command outlived it), so any
                thread might be the one it runs in.
        """
        if self.thread_id is None and self.call_id is not None:
            raise forbidden(
                "Cannot tell which conversation this command runs in, so it may not "
                "start or stop a run; run it from a new command"
            )
        return self.thread_id


_CLI_CALL: Final = "cli_call"


def computer_call(scope: Scope) -> CliCall | None:
    """The computer's call this request is, once ``get_principal`` has read
    it; None for every other caller."""
    call = (scope.get("state") or {}).get(_CLI_CALL)
    return call if isinstance(call, CliCall) else None


def _invalid_token(message: str) -> HTTPException:
    return HTTPException(
        status_code=401,
        detail=message,
        headers={"WWW-Authenticate": 'Bearer error="invalid_token"'},
    )


def forbidden(message: str, kind: str = "forbidden_for_token") -> HTTPException:
    return HTTPException(status_code=403, detail={"message": message, "type": kind})


async def _computer_principal(request: Request, token: str) -> Principal:
    try:
        identity = await cli_tokens.authenticate(token)
    except computer_token_core.Refused:
        raise _invalid_token("Invalid or expired computer token") from None
    except cli_tokens.Throttled as e:
        raise HTTPException(
            status_code=429,
            detail={"message": "Too many requests from this computer", "type": "rate_limited"},
            headers={"Retry-After": str(e.retry_after)},
        ) from None
    principal = Principal(
        user_id=identity.user_id,
        auth="computer",
        scopes=identity.scopes,
        computer_id=identity.computer_id,
    )
    call_id = outcomes.valid_call_id(request.headers.get(CALL_HEADER))
    context = (
        await outcomes.call_context(identity.computer_id, call_id)
        if call_id is not None
        else None
    )
    thread_id = context.thread_id if context is not None else None
    request.scope.setdefault("state", {})[_CLI_CALL] = CliCall(
        principal, call_id, str(thread_id) if thread_id else None
    )
    return principal


async def get_principal(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Depends(_bearer),
) -> Principal:
    """The caller, chosen by the credential it presents.

    The server's own token comes first, as in ``get_current_user_id``; then a
    bearer of a token kind is that kind's alone, so a refused one never falls
    through to the session check.
    """
    matched, service_user = _service_token_user_id(request)
    if matched:
        if not service_user:
            raise HTTPException(status_code=401, detail="Missing X-User-Id")
        return Principal(user_id=service_user, auth="service")
    token = credentials.credentials if credentials is not None else ""
    if cli_tokens.is_cli_token(token):
        return await _computer_principal(request, token)
    if pats.is_pat(token):
        key = await pats.verify_pat(token)
        return Principal(user_id=key.user_id, auth="pat", scopes=key.scopes)
    if token and oauth_resource.from_resource_issuer(token):
        app = await oauth_resource.verify_access_token(token)
        return Principal(
            user_id=app.user_id, auth="oauth", client_id=app.client_id, scopes=app.scopes
        )
    return Principal(
        user_id=await get_current_user_id(request, credentials), auth="session"
    )


CurrentPrincipal = Annotated[Principal, Depends(get_principal)]


def uuid_or_none(value: Any) -> str | None:
    """``value`` as a canonical UUID string; None for one that is not a UUID,
    which a route answers as its own 404."""
    try:
        return str(uuid.UUID(str(value)))
    except ValueError:
        return None


def refuse_own(call: CliCall | None, thread_id: str | None, message: str) -> None:
    """Refuse ``message`` when ``thread_id`` is the thread the command runs
    in, or might be (``CliCall.running_thread``)."""
    if call is None or thread_id is None:
        return
    if call.running_thread() == thread_id:
        raise forbidden(message)


async def refuse_loop(principal: Principal, call: CliCall | None, thread_id: Any) -> None:
    """The thread the call runs in, and the one that sent that thread its
    current turn: an analyst a hand-off started, or a thread ``la`` sent
    into, answering into its sender would start the sender's next turn, and
    so on round."""
    target = uuid_or_none(thread_id)
    refuse_own(call, target, "A command cannot send into the conversation it runs in")
    if call is None or call.thread_id is None or target is None:
        return
    if await turn_targets.turn_sender(principal.user_id, call.thread_id) == target:
        raise forbidden(
            "A command cannot send into the conversation that sent it its current turn"
        )


async def _computer_limits(
    request: Request,
    principal: Principal,
    *,
    own_computer: bool,
    own_thread: bool,
    adds_computer: bool,
    gated: bool,
) -> None:
    if adds_computer:
        # A machine it added would hold a token of its own, which outlives
        # revoking this one.
        raise forbidden("A computer's token cannot add or pair a computer")
    if request.method == "DELETE" and not gated:
        raise forbidden("A computer's token cannot delete; archive or pause instead")
    call = computer_call(request.scope)
    params = request.path_params
    if (
        not own_computer
        and "computer_id" in params
        and uuid_or_none(params["computer_id"]) == principal.computer_id
    ):
        raise forbidden("A computer cannot stop, archive or remove itself")
    # A command that runs in no thread has none to loop into.
    if own_thread or call is None or call.running_thread() is None:
        return
    if "thread_id" in params:
        await refuse_loop(principal, call, params["thread_id"])
    # An automation's runs are guarded by the thread its write leaves pinned,
    # which only the route's body tells (``cli_approval.automation_runs``).


@functools.cache
def require_scopes(
    *scopes: str,
    own_computer: bool = True,
    own_thread: bool = True,
    adds_computer: bool = False,
    gated: bool = False,
):
    """A dependency answering the caller once it holds every one of ``scopes``.

    A session or the server's own call passes. For a computer's token,
    ``own_computer=False`` marks an action it may not take on the computer it
    runs on, ``own_thread=False`` one it may not take on the thread its call
    runs in (sending into it, stopping its run, or into it through an
    automation) or on the thread that sent that thread its current turn,
    which would loop the conversation into itself, and ``adds_computer=True``
    one it may not take at all. A delete is refused unless ``gated`` says
    its route holds it for the user's OK itself (``cli_approval``), so a
    delete route that forgets the gate refuses rather than deletes. Cached
    per arguments, so FastAPI runs it once a request however many parameters
    name it.
    """

    async def dependency(
        request: Request, principal: Principal = Depends(get_principal)
    ) -> Principal:
        if principal.unlimited:
            return principal
        missing = [s for s in scopes if s not in principal.scopes]
        if missing:
            raise HTTPException(
                status_code=403,
                detail={
                    "message": f"This token lacks the {', '.join(missing)} scope",
                    "type": "insufficient_scope",
                    "scopes": list(scopes),
                },
                headers={
                    "WWW-Authenticate": (
                        f'Bearer error="insufficient_scope", scope="{" ".join(scopes)}"'
                    )
                },
            )
        if principal.computer_id is not None:
            await _computer_limits(
                request,
                principal,
                own_computer=own_computer,
                own_thread=own_thread,
                adds_computer=adds_computer,
                gated=gated,
            )
        return principal

    return dependency


def _user_of(guard):
    async def dependency(principal: Principal = Depends(guard)) -> str:
        return principal.user_id

    return dependency


_READ = require_scopes("agent:read")
_RUN = require_scopes("agent:run", own_thread=False)
_WORKSPACE_WRITE = require_scopes("workspace:write")
#: Edits that may send into a thread (an automation's), so not the call's own.
_WORKSPACE_SEND = require_scopes("workspace:write", own_thread=False)
_COMPUTER_WRITE = require_scopes("computer:write")
#: Stop, archive and remove: what a computer must not do to itself.
_COMPUTER_END = require_scopes("computer:write", own_computer=False)
#: Create a computer, or pair or probe a machine to be one.
_COMPUTER_ADD = require_scopes("computer:write", adds_computer=True)

ReadPrincipal = Annotated[Principal, Depends(_READ)]
RunPrincipal = Annotated[Principal, Depends(_RUN)]
WorkspaceWritePrincipal = Annotated[Principal, Depends(_WORKSPACE_WRITE)]
WorkspaceSendPrincipal = Annotated[Principal, Depends(_WORKSPACE_SEND)]
#: The deletes whose routes hold a computer's request for the user's OK.
WorkspaceDeletePrincipal = Annotated[
    Principal, Depends(require_scopes("workspace:write", gated=True))
]
ComputerRemovePrincipal = Annotated[
    Principal, Depends(require_scopes("computer:write", own_computer=False, gated=True))
]

ReadUserId = Annotated[str, Depends(_user_of(_READ))]
RunUserId = Annotated[str, Depends(_user_of(_RUN))]
WorkspaceWriteUserId = Annotated[str, Depends(_user_of(_WORKSPACE_WRITE))]
WorkspaceSendUserId = Annotated[str, Depends(_user_of(_WORKSPACE_SEND))]
ComputerWriteUserId = Annotated[str, Depends(_user_of(_COMPUTER_WRITE))]
DataReadUserId = Annotated[str, Depends(_user_of(require_scopes("data:read")))]
ComputerEndUserId = Annotated[str, Depends(_user_of(_COMPUTER_END))]
ComputerAddUserId = Annotated[str, Depends(_user_of(_COMPUTER_ADD))]


def refuse_computer(principal: Principal, message: str) -> None:
    """For a surface only a person reads: a computer's token is refused even
    where its scopes would pass."""
    if principal.computer_id is not None:
        raise forbidden(message)


def refuse_rewrite(
    principal: Principal, checkpoint_id: Any, fork_from_turn: Any = None
) -> None:
    """A turn a computer's token runs only adds to its thread: replaying an
    older checkpoint or forking from a turn would drop the turns after it."""
    if principal.computer_id is not None and (
        checkpoint_id is not None or fork_from_turn is not None
    ):
        raise forbidden("A computer's token cannot edit or regenerate a turn")


async def check_turn_body(
    principal: Principal,
    request: Request,
    body: ChatRequest,
    thread_id: str | None,
    *,
    from_path: bool = False,
) -> None:
    """Refuse what a turn's body asks that a token may not.

    Approving an order rides the same route as chatting, so only a session
    answers one; a computer's agent answers no approval at all, since one
    approving its own requests defeats them, and starts no turn from an
    earlier checkpoint, since an edit or regenerate deletes the turns after
    it. A thread the body names (None for one minted for it) is held to the
    send guard like a path's; ``from_path`` says the route's path named it,
    so ``require_scopes`` already did. Where the turn runs is the approval's
    (``cli_approval.admit_turn``, which runs this first).
    """
    if principal.unlimited:
        return
    answers = body.hitl_response or {}
    # Any answer to a pending order decides it, a plain one included (the
    # order gate refuses an attempt given no keyed verdict), so a token
    # answers nothing while one waits.
    if answers and (
        any(getattr(answer, "order_decisions", None) for answer in answers.values())
        or (
            (waiting := uuid_or_none(thread_id)) is not None
            and await order_attempts.thread_awaits_order(waiting)
        )
    ):
        raise forbidden("Orders are answered only in the app", "order_decision_forbidden")
    if principal.computer_id is None:
        return
    if answers:
        raise forbidden("A computer's token cannot answer approval requests")
    refuse_rewrite(principal, body.checkpoint_id, body.fork_from_turn)
    if thread_id is not None and not from_path:
        await refuse_loop(principal, computer_call(request.scope), thread_id)


def stamp_origin(principal: Principal, request: Request, body: ChatRequest) -> ChatRequest:
    """``body`` with the origin the server states for it.

    The origin labels a message as another agent's or an automation's and
    links the thread it came from, so only a session or the server's own
    call states its own. A computer's turn takes the server's: the thread
    whose command sent it, so the thread it starts and the message it sends
    say which conversation's agent asked, and none when the call names no
    thread, since a person or a schedule on the machine is the user. Any
    other token's turn has none.
    """
    if principal.unlimited:
        return body
    if principal.computer_id is None:
        return body.model_copy(update={"origin": None})
    call = computer_call(request.scope)
    thread_id = call.thread_id if call is not None else None
    origin = ThreadOrigin(type="agent", agent="cli", id=thread_id) if thread_id else None
    return body.model_copy(update={"origin": origin})
