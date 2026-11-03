"""Where a Claude Code turn on a cloud computer runs, and on which credential.

The CLI runs in the backend worker that owns the turn, never in the
sandbox: the turn's tools reach the sandbox through the in-process MCP
bridge, and a credential in the sandbox would sit beside code the model
writes. On a self-hosted server the credential is the setup token each user
saves in Settings (``saved``). On the platform it is the one the user chose
for cloud computers (``claude_code_custody``): their own Anthropic API key,
once the operator turned that on (``claude_code.hosted_api_key``). With the
switch off nothing here reads a user's choice, and cloud computers there run
no Claude Code, as before the choice existed.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from src.config.settings import get_claude_code_config
from src.llms.claude_code import backend_available, rows_available
from src.server.database.api_keys import saved_key
from src.server.database.claude_code_hosted import Mode, get_custody_mode, set_custody_mode
from src.server.models.claude_code import ClaudeCodeCustody

#: The BYOK provider whose key a turn on the user's API key runs on.
ANTHROPIC = "anthropic"


@dataclass(frozen=True)
class Hosted:
    """How the user's turns on a cloud computer run: on ``runs_on``, or
    refused for ``refusal``, a ``CLAUDE_CODE_OFF`` reason."""

    runs_on: Literal["saved_token", "api_key"] | None = None
    refusal: str | None = None

    def __post_init__(self) -> None:
        if (self.runs_on is None) == (self.refusal is None):
            raise ValueError("a turn on a cloud computer either runs on a credential or is refused")


@dataclass(frozen=True)
class Placement:
    """Where a Claude Code turn in a workspace runs: on ``own_computer``, the
    row of the user's own computer it is on, else as ``hosted`` says, None
    where this server runs none on a cloud computer."""

    own_computer: dict | None = None
    hosted: Hosted | None = None


def choice_offered() -> bool:
    """Whether users choose how Claude Code runs on this server's cloud
    computers: on the platform, once the operator turned it on."""
    from src.config.settings import HOST_MODE

    return HOST_MODE == "platform" and rows_available() and get_claude_code_config().hosted_api_key


async def hosted(user_id: str, *, conn=None) -> Hosted | None:
    """How the user's turns on a cloud computer run here; None where none
    do. Asked at admission and again as each run starts, since the user's
    choice and their key may change in between. ``conn`` reads on a caller's
    connection, as ``placement`` takes it."""
    if backend_available():
        return Hosted("saved_token")
    if not choice_offered():
        return None
    if await get_custody_mode(user_id, conn=conn) != "api_key":
        return Hosted(refusal="hosted_off")
    return _on_key(await saved_key(user_id, ANTHROPIC, conn=conn))


def _on_key(key: dict | None) -> Hosted:
    """A turn on the user's saved Anthropic key ``key``: refused with none, and
    with one set to another endpoint, since the worker sends it to Anthropic
    alone."""
    if key is None:
        return Hosted(refusal="needs_api_key")
    if key["base_url"]:
        return Hosted(refusal="api_key_endpoint")
    return Hosted("api_key")


async def placement(user_id: str, workspace_id: str | None, *, conn=None) -> Placement:
    """Where the user's Claude Code turn in ``workspace_id`` runs, settled once
    at admission: the own computer first, so a workspace on one never reads
    the choice for cloud computers. ``conn`` reads on a caller's connection,
    as an automation file save that holds the user's locks passes it, so the
    check never waits on a second pool slot."""
    from src.server.database.computer import get_computer_for_workspace
    from src.server.services.ssh_computers import is_own_computer

    computer = await get_computer_for_workspace(workspace_id, conn=conn) if workspace_id else None
    if is_own_computer(computer):
        return Placement(own_computer=computer)
    return Placement(hosted=await hosted(user_id, conn=conn))


async def custody(user_id: str) -> ClaudeCodeCustody | None:
    """The user's choice as Settings shows it, with the verdict admission
    gives a turn there; None where none is offered."""
    if not choice_offered():
        return None
    mode = await get_custody_mode(user_id)
    key = await saved_key(user_id, ANTHROPIC)
    verdict = _on_key(key) if mode == "api_key" else Hosted(refusal="hosted_off")
    return ClaudeCodeCustody(
        mode="api_key" if mode == "api_key" else "local", api_key_saved=key is not None,
        runs_on=verdict.runs_on, refusal=verdict.refusal,
    )


async def choose(user_id: str, mode: Mode) -> ClaudeCodeCustody | None:
    """Save the user's choice; None where none is offered."""
    if not choice_offered():
        return None
    await set_custody_mode(user_id, mode)
    return await custody(user_id)
