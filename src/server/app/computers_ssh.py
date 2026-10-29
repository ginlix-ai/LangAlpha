"""Routes for adding a machine its owner brings, reached over SSH.

The key, the probe and the pairing the dialog walks through, and the public
routes the pairing's script, or LangAlpha's helper, calls from the machine,
whose token is the credential. The create itself is ``POST /api/v1/computers``.
"""

from __future__ import annotations

import contextlib
import functools
import logging
import shlex
from typing import Any, Awaitable, Callable, Iterator, Optional, TypeVar
from urllib.parse import parse_qsl

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel
from starlette.requests import ClientDisconnect

from ptc_agent.config.core import SSHConfig
from ptc_agent.core.sandbox.providers.ssh_errors import SSHConnectError
from src.server.app import computers
from src.server.database.computer import get_computer, get_computers_for_user
from src.server.dependencies.usage_limits import assert_provider_allowed
from src.server.models.computer import (
    ComputerSSHAddress,
    ComputerSSHKeyResponse,
    ComputerSSHPairingCreate,
    ComputerSSHPairingReport,
    ComputerSSHPairingResponse,
    ComputerSSHPairingStatus,
    ComputerSSHProbeRequest,
    ComputerSSHProbeResponse,
    ComputerSSHTunnelRequest,
)
from src.server.services import ssh_pairing
from src.server.services.ssh_dial_budget import SSHDialBudgetExceeded, spend_dials
from src.server.services.ssh_computers import (
    SSH_KIND,
    SSHComputerLimit,
    SSHUnavailable,
    check_ssh_capacity,
    client_key,
    ensure_client_key,
    key_install,
    probe,
    probe_response,
    suggested_root_dir,
)
from src.server.utils.api import CurrentUserId

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/computers/ssh", tags=["Computers"])


async def _ssh_settings(user_id: Optional[str]) -> SSHConfig:
    """The deployment's SSH settings, refusing where the kind is not on offer.

    With a caller, the entitlement is asked as well: a probe dials an address
    the caller names, which only someone who may add such a machine needs.
    None is the script's public routes, with no caller to ask, whose machine
    the create checks the rest of.
    """
    try:
        sandbox = computers._sandbox_config()
    except computers.ComputerRuntimeUnavailable as e:
        logger.warning("Computer runtime unavailable for ssh: %s", e)
        raise HTTPException(
            status_code=503,
            detail=(
                "Computer runtime is not available"
                if user_id is not None
                else "Computers are not available right now."
            ),
        ) from None
    if SSH_KIND not in sandbox.creatable_providers():
        raise HTTPException(
            status_code=404, detail="SSH computers are not offered on this server."
        )
    if user_id is not None:
        await assert_provider_allowed(user_id, SSH_KIND, default_kind=sandbox.provider)
    return sandbox.settings_for(SSH_KIND)


@contextlib.contextmanager
def _ssh_errors(*, structured: bool = False) -> Iterator[None]:
    """Answer the SSH services' failures. ``structured`` is the probe's 422,
    with the ``code`` the client words its copy from."""
    try:
        yield
    except SSHComputerLimit as e:
        raise HTTPException(status_code=409, detail=e.detail()) from None
    except SSHUnavailable as e:
        raise HTTPException(status_code=503, detail=str(e)) from None
    except ssh_pairing.PairingNotFound:
        raise HTTPException(
            status_code=404, detail="This command has expired."
        ) from None
    except SSHDialBudgetExceeded as e:
        raise _budget_refusal(e, structured=structured) from None
    except SSHConnectError as e:
        detail: Any = {"code": e.code, "message": str(e)} if structured else str(e)
        raise HTTPException(status_code=422, detail=detail) from None


def _budget_refusal(
    e: SSHDialBudgetExceeded, *, structured: bool = False
) -> HTTPException:
    """The 429 for a spent dial budget; ``retry_after`` lets a client word the wait itself."""
    detail: Any = (
        {"code": e.code, "message": str(e), "retry_after": e.retry_after}
        if structured
        else str(e)
    )
    return HTTPException(
        status_code=429, detail=detail, headers={"Retry-After": str(e.retry_after)}
    )


def _plain_text_errors(
    route: Callable[..., Awaitable[Any]],
) -> Callable[..., Awaitable[Any]]:
    """A script's route answers a refusal as the line its terminal prints.

    The script prints the answer's body, so a JSON detail would reach the
    machine's owner as noise.
    """

    @functools.wraps(route)
    async def answer(*args: Any, **kwargs: Any) -> Any:
        try:
            return await route(*args, **kwargs)
        except HTTPException as e:
            return PlainTextResponse(f"{e.detail}\n", status_code=e.status_code)

    return answer


# Far past any form the script posts, a few lines and the machine's host
# keys. A form parser holds each field whole however long it runs, so the cap
# is kept while the body is read, before any of it is parsed.
_MAX_SCRIPT_BODY = 256 * 1024
_MAX_SCRIPT_FIELDS = 32

_ScriptFormT = TypeVar("_ScriptFormT", bound=BaseModel)


async def _script_form(request: Request, model: type[_ScriptFormT]) -> _ScriptFormT:
    """The urlencoded form the script posts, refused once past the cap."""
    too_large = HTTPException(
        status_code=413, detail="What the machine sent was too large to read."
    )
    declared = request.headers.get("content-length", "")
    if declared.isdigit() and int(declared) > _MAX_SCRIPT_BODY:
        raise too_large
    body = bytearray()
    try:
        async for chunk in request.stream():
            body += chunk
            if len(body) > _MAX_SCRIPT_BODY:
                raise too_large
    except ClientDisconnect:
        raise HTTPException(status_code=400, detail="The report was cut off.") from None
    try:
        # latin-1 keeps every byte as it came; the percent escapes decode as UTF-8.
        fields = parse_qsl(
            body.decode("latin-1"),
            keep_blank_values=True,
            max_num_fields=_MAX_SCRIPT_FIELDS,
        )
    except ValueError:
        raise too_large from None
    return model.model_validate(dict(fields))


@router.get("/key", response_model=ComputerSSHKeyResponse)
async def get_ssh_key(x_user_id: CurrentUserId):
    """The caller's SSH client key, minted on first call, and how to install it."""
    settings = await _ssh_settings(x_user_id)
    with _ssh_errors():
        return key_install(await ensure_client_key(x_user_id), settings)


@router.post("/probe", response_model=ComputerSSHProbeResponse)
async def probe_ssh(request: ComputerSSHProbeRequest, x_user_id: CurrentUserId):
    """Log in to a machine with the caller's key and report what is there.

    Its host key comes back for the user to confirm; the create pins it. A
    machine that refuses or cannot be reached answers 422 with a ``code`` the
    client words its copy from and a sentence it may show as is; a caller with
    as many ssh computers as allowed, 409 ``ssh_computer_limit``, unless
    ``computer_id`` names the computer the machine is reached again for, as
    for a bound pairing.
    """
    settings = await _ssh_settings(x_user_id)
    if request.computer_id is not None:
        await _repairable_ssh_computer(request.computer_id, x_user_id)
    with _ssh_errors(structured=True):
        # At the cap a dial would only find a machine it cannot add.
        if request.computer_id is None:
            await check_ssh_capacity(x_user_id, settings)
        await spend_dials(x_user_id, 1, settings=settings)
        found = await probe(
            await client_key(x_user_id),
            settings=settings,
            host=request.host,
            port=request.port,
            username=request.username,
        )
    return probe_response(
        found,
        suggested_root_dir=suggested_root_dir(
            await get_computers_for_user(x_user_id),
            home=found.home,
            host_key=found.host_key,
            host=request.host,
            port=request.port,
        ),
    )


@router.post("/pairings", response_model=ComputerSSHPairingResponse, status_code=201)
async def create_ssh_pairing(
    request: Request,
    x_user_id: CurrentUserId,
    body: Optional[ComputerSSHPairingCreate] = None,
):
    """A command that adds a machine by being run on it, as the dialog shows it.

    It authorizes the caller's key and reports how to reach the machine, which
    is dialed at once; GET /ssh/pairings/{token} then has the probe a create
    needs. It lives 15 minutes, and running it again re-checks the machine.
    A caller with as many ssh computers as allowed is refused with 409
    ``ssh_computer_limit``, before the command puts the key on a machine.

    With ``computer_id``, one of the caller's ssh computers, the command pairs
    that computer's machine again, for ``POST /{computer_id}/reconnect``: no
    computer is added, so the limit is not asked, and the ready probe suggests
    the computer's own folder (404 for no such computer, 400 ``not_ssh``).
    """
    settings = await _ssh_settings(x_user_id)
    computer_id = body.computer_id if body is not None else None
    if computer_id is not None:
        await _repairable_ssh_computer(computer_id, x_user_id)
    with _ssh_errors():
        if computer_id is None:
            await check_ssh_capacity(x_user_id, settings)
        return await ssh_pairing.create_pairing(
            x_user_id, request_base=str(request.base_url), computer_id=computer_id
        )


async def _repairable_ssh_computer(computer_id: str, user_id: str) -> None:
    """Refuse a bound pairing for anything but the caller's own ssh computer."""
    computer = await get_computer(computer_id)
    if computer is None or str(computer.get("user_id")) != user_id:
        raise HTTPException(status_code=404, detail="Computer not found")
    if computer.get("kind") != SSH_KIND:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "not_ssh",
                "message": "Only a computer on a machine you brought can be paired again.",
            },
        )


@router.get("/pairings/{token}", response_model=ComputerSSHPairingStatus)
async def get_ssh_pairing(token: str, x_user_id: CurrentUserId):
    """Where the caller's pairing stands; 404 once it has expired."""
    with _ssh_errors():
        return await ssh_pairing.pairing_status(x_user_id, token)


@router.post("/pairings/{token}/connect", response_model=ComputerSSHPairingStatus)
async def connect_ssh_pairing(
    token: str, request: ComputerSSHAddress, x_user_id: CurrentUserId
):
    """Try an address the caller knows for a machine that checked in.

    For a machine its report did not lead to, such as one behind a port
    forward. Its account and host keys are the ones the machine reported.
    """
    settings = await _ssh_settings(x_user_id)
    with _ssh_errors():
        return await ssh_pairing.connect(
            x_user_id, token, host=request.host, port=request.port, settings=settings
        )


def _shell_message(message: str) -> str:
    return f"#!/bin/sh\necho {shlex.quote(message)} >&2\nexit 1\n"


@router.get("/pairings/{token}/script", response_class=PlainTextResponse)
async def ssh_pairing_script(token: str):
    """The script a pairing's command runs. Public: the token is the credential.

    An expired or unknown token still answers 200, with a script that says so:
    the command fetches with curl -f, which would print only a status code.
    """
    try:
        settings = await _ssh_settings(None)
        script = await ssh_pairing.render_script(token, settings)
    except HTTPException as e:
        script = _shell_message(str(e.detail))
    except SSHUnavailable as e:
        script = _shell_message(str(e))
    return PlainTextResponse(script, headers={"Cache-Control": "no-store"})


@router.get("/pairings/{token}/key", response_class=PlainTextResponse)
@_plain_text_errors
async def ssh_pairing_key(token: str):
    """The line the script authorizes, for a machine that pairs without running it.

    Public: the token is the credential. LangAlpha's helper reads it here and
    keeps it in its own folder. 404 once expired.
    """
    settings = await _ssh_settings(None)
    with _ssh_errors():
        line = await ssh_pairing.pairing_key(token, settings)
    if line is None:
        raise HTTPException(status_code=404, detail=ssh_pairing.EXPIRED)
    return PlainTextResponse(f"{line}\n", headers={"Cache-Control": "no-store"})


@router.post("/pairings/{token}/report", response_class=PlainTextResponse)
@_plain_text_errors
async def ssh_pairing_report(token: str, request: Request):
    """The machine's report from the script. Public: the token is the credential.

    Answers what the script prints: 200 when the machine was reached, 422 when
    it checked in but could not be used, 404 once expired. 202 is for a machine
    no address reached that may connect out instead: its first line is
    ``tunnel``, the cue for the script to ask for one at /tunnel and report
    again with its id, and the second is a sentence for the terminal. A lone
    ``failed`` field is the script stopping, with the sentence it printed,
    which fails the pairing unless the machine was already reached.
    """
    settings = await _ssh_settings(None)
    form = await _script_form(request, ComputerSSHPairingReport)
    with _ssh_errors():
        status, message = await ssh_pairing.report(
            token,
            form,
            sources=ssh_pairing.source_addresses(
                request.client.host if request.client else None, request.headers
            ),
            settings=settings,
        )
    return PlainTextResponse(f"{message}\n", status_code=status)


@router.post("/pairings/{token}/tunnel", response_class=PlainTextResponse)
@_plain_text_errors
async def ssh_pairing_tunnel(token: str, request: Request):
    """A tunnel for a machine the server could not dial. Public: the token is the credential.

    The script asks once its report answered 202, with ``public_key``, the
    public half of a key it made for the tunnel, and ``hostname``. Answers the
    ``key=value`` lines it sets the tunnel up from: ``tunnel_id``,
    ``gateway_host``, ``gateway_port`` and ``known_hosts``. 404 once expired,
    503 where this server runs no gateway, 422 for a key that does not parse.
    """
    await _ssh_settings(None)
    form = await _script_form(request, ComputerSSHTunnelRequest)
    with _ssh_errors():
        status, message = await ssh_pairing.issue_tunnel(token, form)
    return PlainTextResponse(f"{message}\n", status_code=status)
