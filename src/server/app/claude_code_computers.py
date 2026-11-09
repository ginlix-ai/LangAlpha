"""Claude Code on each of the user's own computers, for Settings: whether a
turn can run there, and the setup token kept on its machine.

- GET    /api/v1/claude-code/computers                               Every own computer and where it stands, each cloud computer too small for Claude Code, and the own computer that runs it for cloud ones
- POST   /api/v1/claude-code/computers/{computer_id}/sign-in         Start a sign-in that makes a token for it
- POST   /api/v1/claude-code/computers/{computer_id}/sign-in/code    Finish it with the code Claude's page shows
- PUT    /api/v1/claude-code/computers/{computer_id}/token           Place a token the user made themselves
- DELETE /api/v1/claude-code/computers/{computer_id}/token           Remove the machine's token
- POST   /api/v1/claude-code/computers/{computer_id}/check           Check now: read its own token's Claude plan with one minimal request
- PUT    /api/v1/claude-code/computers/{computer_id}/clis            How many Claude Code agents it runs at once

The flow and the machine's token are ``services.claude_code`` (``flow``, ``machine``).
"""

import asyncio

from fastapi import APIRouter, Response

from src.server.models.claude_code import ClaudeCodeComputer, ClaudeCodeComputers, ClisBody, CodeBody, TokenBody
from src.server.services.claude_code import flow
from src.server.services.claude_code.flow import pasted_code, pasted_token, require, rows_on
from src.server.services.claude_code.hosted import cloud_view, picks_own, runs_on_view
from src.server.services.claude_code.machine import MachineHome, check_now, list_computers, set_clis
from src.server.utils.api import CurrentUserId

router = APIRouter(prefix="/api/v1/claude-code/computers", tags=["claude-code"])


@router.get("", response_model=ClaudeCodeComputers)
async def get_computers(x_user_id: CurrentUserId):
    listed, (refusals, where), on = await asyncio.gather(
        list_computers(x_user_id), cloud_view(x_user_id), rows_on(x_user_id),
    )
    if on:
        listed.cloud_refusals = refusals
        listed.cli_computer = runs_on_view(where) if picks_own(where) else None
    return listed


@router.post("/{computer_id}/sign-in")
async def start_sign_in(computer_id: str, x_user_id: CurrentUserId):
    require(await rows_on(x_user_id))
    return await flow.start_sign_in(await MachineHome.of(x_user_id, computer_id))


@router.post("/{computer_id}/sign-in/code", response_model=ClaudeCodeComputer)
async def finish_sign_in(computer_id: str, body: CodeBody, x_user_id: CurrentUserId):
    require(await rows_on(x_user_id))
    raw = pasted_code(body)
    return await flow.finish_sign_in(await MachineHome.of(x_user_id, computer_id), raw)


@router.put("/{computer_id}/token", response_model=ClaudeCodeComputer)
async def save_token(computer_id: str, body: TokenBody, x_user_id: CurrentUserId):
    require(await rows_on(x_user_id))
    token, rejected = pasted_token(body)
    return await flow.paste(await MachineHome.of(x_user_id, computer_id), token, rejected)


@router.delete("/{computer_id}/token", status_code=204)
async def remove_token(computer_id: str, x_user_id: CurrentUserId):
    """Remove the machine's token, whether or not Claude Code is still on for the user or the server."""
    await flow.remove(await MachineHome.of(x_user_id, computer_id))
    return Response(status_code=204)


@router.put("/{computer_id}/clis", response_model=ClaudeCodeComputer)
async def put_clis(computer_id: str, body: ClisBody, x_user_id: CurrentUserId):
    return await set_clis(x_user_id, computer_id, body.clis)


@router.post("/{computer_id}/check", status_code=204)
async def check_computer(computer_id: str, x_user_id: CurrentUserId):
    await check_now(x_user_id, computer_id)
    return Response(status_code=204)
