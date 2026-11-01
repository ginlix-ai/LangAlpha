"""Caller identity, and what it may do, for the routes that also serve tokens.

Opt-in on purpose. ``get_current_user_id`` refuses every token but a session,
so a route that does not depend on ``require_scopes`` keeps refusing an
outside app, a personal access token and a computer's token alike. What no
scope grants (orders, secrets, publishing, identity) stays on those routes.

A computer's token (``lac1.``, what the ``la`` CLI presents from inside a
computer) is held by agent code, so it is narrower than its scopes say: it
reaches only what its own computer holds and never Home, deletes nothing,
adds no computer, leaves its own computer running, answers no approval,
rewrites no history, and sends nothing into the thread its call came from.
Home is out of reach wherever it sits because a turn there runs the Chief of
Staff, whose tools reach every computer and hand work to any workspace.
"""

from __future__ import annotations

import functools
import uuid
from dataclasses import dataclass, field
from typing import Annotated, Any, Final, Literal

from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from starlette.types import Scope

from src.server.auth import computer_token_core, oauth_resource, pats
from src.server.database import order_attempts, principal_reach
from src.server.database.home_workspace import is_home_id
from src.server.database.principal_reach import HOME, MISSING, Reach, ReachKind
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
    computer can read a sibling's environment and send its id. ``reached``
    keeps the reach answers already read for the request, so a turn's body
    check does not ask again what its path's did.
    """

    principal: Principal
    call_id: str | None
    thread_id: str | None
    reached: dict[tuple[ReachKind, str], Reach] = field(
        default_factory=dict, compare=False, repr=False
    )


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


def _forbidden(message: str, kind: str = "forbidden_for_token") -> HTTPException:
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


def _uuid(value: Any) -> str | None:
    try:
        return str(uuid.UUID(str(value)))
    except ValueError:
        return None


async def _computer_of(
    principal: Principal, kind: ReachKind, value: Any, call: CliCall | None = None
) -> Reach:
    """The computer the resource sits on, ``MISSING`` for one the user does
    not have (the route answers its own 404), ``HOME`` for one in Home.
    ``call`` keeps the answer for the rest of the request.

    Home's id is known without its row, which a turn naming it would create,
    so it never reads as one the user lacks.
    """
    resource_id = _uuid(value)
    if resource_id is None:
        return MISSING
    if kind == "workspace" and is_home_id(principal.user_id, resource_id):
        return HOME
    key = (kind, resource_id)
    if call is not None and key in call.reached:
        return call.reached[key]
    found = await principal_reach.computer_of(kind, principal.user_id, resource_id)
    if call is not None:
        call.reached[key] = found
    return found


def _refuse_off_computer(principal: Principal, found: Reach) -> None:
    if found is HOME:
        raise _forbidden("A computer's token never reaches Home")
    if found is not MISSING and found != principal.computer_id:
        raise _forbidden("A computer's token reaches only what that computer holds")


_UNKNOWN_CALL_THREAD: Final = (
    "Cannot tell which conversation this command runs in, so it may not "
    "start or stop a run; run it from a new command"
)


def _refuse_own_thread(call: CliCall | None, thread_id: Any) -> None:
    if call is None or thread_id is None:
        return
    if call.thread_id is None:
        if call.call_id is not None:
            # The command named its call, but what it filed is gone, so the
            # thread it names might be the one the command runs in.
            raise _forbidden(_UNKNOWN_CALL_THREAD)
        return
    if call.thread_id == _uuid(thread_id):
        raise _forbidden("A command cannot send into the conversation it runs in")


# The ids a route may name that sit on a computer, and where it may name them:
# the path, or for a workspace and a thread, the query too, as a list's filter.
_REACH_IDS: Final[tuple[tuple[str, ReachKind], ...]] = (
    ("workspace_id", "workspace"),
    ("thread_id", "thread"),
    ("automation_id", "automation"),
)
_QUERY_IDS: Final = ("workspace_id", "thread_id")


async def _within_computer(
    request: Request,
    principal: Principal,
    *,
    own_computer: bool,
    own_thread: bool,
    adds_computer: bool,
) -> None:
    if adds_computer:
        # A machine it added would hold a token of its own, which outlives
        # revoking this one.
        raise _forbidden("A computer's token cannot add or pair a computer")
    if request.method == "DELETE":
        raise _forbidden("A computer's token cannot delete; archive or pause instead")
    call = computer_call(request.scope)
    params = request.path_params
    if "computer_id" in params:
        target = _uuid(params["computer_id"])
        if target != principal.computer_id:
            raise _forbidden("A computer's token reaches only its own computer")
        if not own_computer:
            raise _forbidden("A computer cannot stop, archive or remove itself")
    named = {k: request.query_params[k] for k in _QUERY_IDS if k in request.query_params}
    named.update(params)
    for name, kind in _REACH_IDS:
        if name in named:
            found = await _computer_of(principal, kind, named[name], call)
            _refuse_off_computer(principal, found)
    if own_thread or call is None or call.call_id is None:
        return
    if call.thread_id is None:
        # The command named its call, but what it filed is gone (Redis is
        # down, or the command outlived it), so the run it would start might
        # be in its own conversation.
        raise _forbidden(_UNKNOWN_CALL_THREAD)
    if "thread_id" in params:
        _refuse_own_thread(call, params["thread_id"])
    automation_id = _uuid(params.get("automation_id"))
    if automation_id is not None:
        # Its run would start a turn in the thread it sends into.
        _refuse_own_thread(
            call, await principal_reach.automation_thread(principal.user_id, automation_id)
        )


@functools.cache
def require_scopes(
    *scopes: str,
    own_computer: bool = True,
    own_thread: bool = True,
    adds_computer: bool = False,
):
    """A dependency answering the caller once it holds every one of ``scopes``.

    A session or the server's own call passes. For a computer's token,
    ``own_computer=False`` marks an action it may not take on the computer it
    runs on, ``own_thread=False`` one it may not take on the thread its call
    runs in (sending into it, stopping its run, or into it through an
    automation), which would loop the conversation into itself, and
    ``adds_computer=True`` one it may not take at all. Cached per arguments,
    so FastAPI runs it once a request however many parameters name it.
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
            await _within_computer(
                request,
                principal,
                own_computer=own_computer,
                own_thread=own_thread,
                adds_computer=adds_computer,
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

ReadUserId = Annotated[str, Depends(_user_of(_READ))]
RunUserId = Annotated[str, Depends(_user_of(_RUN))]
WorkspaceWriteUserId = Annotated[str, Depends(_user_of(_WORKSPACE_WRITE))]
WorkspaceSendUserId = Annotated[str, Depends(_user_of(_WORKSPACE_SEND))]
ComputerWriteUserId = Annotated[str, Depends(_user_of(_COMPUTER_WRITE))]
DataReadUserId = Annotated[str, Depends(_user_of(require_scopes("data:read")))]
ComputerEndUserId = Annotated[str, Depends(_user_of(_COMPUTER_END))]
ComputerAddUserId = Annotated[str, Depends(_user_of(_COMPUTER_ADD))]


async def assert_reaches(
    principal: Principal, *, workspace_id: Any = None, thread_id: Any = None
) -> None:
    """For what a route names in its body or finds itself rather than in its
    path or query; None names nothing. Home's id is refused like any place
    off the computer."""
    if principal.computer_id is None:
        return
    if workspace_id is not None:
        _refuse_off_computer(principal, await _computer_of(principal, "workspace", workspace_id))
    if thread_id is not None:
        _refuse_off_computer(principal, await _computer_of(principal, "thread", thread_id))


def pin_computer(principal: Principal, requested: str | None) -> str | None:
    """The computer a new workspace goes on. A computer's token adds one only
    beside its own, and another it names is answered as one the user lacks."""
    if principal.computer_id is None:
        return requested
    if requested is not None and requested != principal.computer_id:
        raise HTTPException(status_code=404, detail="Computer not found")
    return principal.computer_id


async def assert_automation_target(
    principal: Principal, request: Request, *, workspace_id: Any, thread_id: Any
) -> None:
    """Where an automation's runs go, held like a turn's: on the computer,
    out of Home, and never the thread the call runs in, whose turns its runs
    would start, the loop the send guard refuses."""
    await assert_reaches(principal, workspace_id=workspace_id, thread_id=thread_id)
    if principal.computer_id is not None and thread_id is not None:
        _refuse_own_thread(computer_call(request.scope), thread_id)


def refuse_rewrite(principal: Principal, checkpoint_id: Any, fork_from_turn: Any = None) -> None:
    """A turn a computer's token runs only adds to its thread: replaying an
    older checkpoint or forking from a turn would drop the turns after it."""
    if principal.computer_id is not None and (checkpoint_id is not None or fork_from_turn is not None):
        raise _forbidden("A computer's token cannot edit or regenerate a turn")


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
    it. The thread the turn goes to (None for one minted for it) and the
    workspace the body names are held to the computer like a path's, a new
    thread naming none being Home's; ``from_path`` says the route's path
    named the thread, so ``require_scopes`` already held it there.
    """
    if principal.unlimited:
        return
    answers = body.hitl_response or {}
    # Any answer to a pending order decides it, a plain one included (the
    # order gate refuses an attempt given no keyed verdict), so a token
    # answers nothing while one waits.
    if answers and (
        any(getattr(answer, "order_decisions", None) for answer in answers.values())
        or ((waiting := _uuid(thread_id)) is not None and await order_attempts.thread_awaits_order(waiting))
    ):
        raise _forbidden("Orders are answered only in the app", "order_decision_forbidden")
    if principal.computer_id is None:
        return
    if answers:
        raise _forbidden("A computer's token cannot answer approval requests")
    refuse_rewrite(principal, body.checkpoint_id, body.fork_from_turn)
    call = computer_call(request.scope)
    new_thread = True
    if thread_id is not None:
        thread_on = await _computer_of(principal, "thread", thread_id, call)
        new_thread = thread_on is MISSING
        if not from_path:
            _refuse_off_computer(principal, thread_on)
            _refuse_own_thread(call, thread_id)
    workspace_id = body.workspace_id
    if workspace_id is None and new_thread:
        workspace_id = principal_reach.workspace_or_home(principal.user_id, None)
    if workspace_id is not None:
        found = await _computer_of(principal, "workspace", workspace_id, call)
        _refuse_off_computer(principal, found)


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
