"""Where a Claude Code turn's CLI runs, and on which credential.

The one module here that imports the runtime at its top, so it is imported
only as a Claude Code turn is built (``AgentInputs.claude_code_launcher``):
the SDK it loads is an optional extra that no other turn and no startup pays
for.
"""

from __future__ import annotations

import asyncio
import weakref
from typing import TYPE_CHECKING, Any, Literal

from ptc_agent.agent.claude_code.launcher import CliSlots, Credential, Launcher, LocalLauncher
from ptc_agent.agent.claude_code.machine_token import token_name
from ptc_agent.agent.claude_code.sandbox_launcher import sandbox_launcher
from ptc_agent.agent.claude_code.ssh_launcher import SSHLauncher
from ptc_agent.core.sandbox.providers._ssh_transport import Link, Machine
from ptc_agent.core.sandbox.providers.ssh import SSHProvider
from src.config.settings import get_claude_code_config
from src.llms.claude_code import ClaudeTurnFailed
from src.server.database.api_keys import get_key_with_base_url

from .hosted import ANTHROPIC, hosted
from .machine import ComputerToken
from .saved import refusal_stamp, refuse, stored_row

if TYPE_CHECKING:
    from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

#: This worker's slots, one set per event loop as a connection is: one in the
#: server, one per test loop.
_slots: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, CliSlots] = weakref.WeakKeyDictionary()


def worker_slots() -> CliSlots | None:
    """The CLIs this worker runs at once for cloud computers on the platform
    (``claude_code.worker_cli_slots``), where many users' turns share it; a
    self-hosted server's backend is its operator's own, and runs no cap."""
    from src.config.settings import HOST_MODE

    if HOST_MODE != "platform":
        return None
    loop = asyncio.get_running_loop()
    if (slots := _slots.get(loop)) is None:
        slots = _slots[loop] = CliSlots(get_claude_code_config().worker_cli_slots)
    return slots


class HostedCredential:
    """The credential a turn on a cloud computer signs in with, as its
    launcher reads it (``launcher.TokenStore``): as each run starts, by the
    rule admission asked (``hosted``), so a run whose user changed their
    choice or removed their key since is refused as admission would refuse
    it, and so is one whose credential is no longer the one admission let it
    run on (``admitted``), which its run's row is stamped with. A setup token Anthropic refuses is recorded as refused
    (``saved.refuse``) and ends the turn saying so, as admission will from
    then on; an API key it refuses ends the turn saying so and is
    never flagged, since the key's own status lives in Model access."""

    def __init__(self, user_id: str, admitted: str | None) -> None:
        self.user_id = user_id
        self.admitted = admitted
        #: What the last read handed out, for ``refused``: the API key, or the
        #: stored token's refusal stamp, never the token.
        self._handed: tuple[str, Any] | Literal["api_key"] | None = None

    async def read(self) -> Credential | None:
        state = await hosted(self.user_id)
        if state is None:
            raise ClaudeTurnFailed("needs_own_computer", "this server runs Claude Code only on the user's own computers")
        if state.refusal is not None:
            raise ClaudeTurnFailed(state.refusal, "the user's choice for cloud computers changed since admission")
        if state.runs_on != self.admitted:
            raise ClaudeTurnFailed("choice_changed", "the credential the turn runs on changed since admission")
        self._handed = None
        if state.runs_on in ("saved_token", "stored_token"):
            row = await stored_row(self.user_id)
            if row is None:
                return None
            self._handed = refusal_stamp(row)
            return Credential(row["access_token"])
        key = await get_key_with_base_url(self.user_id, ANTHROPIC)
        if not key:
            raise ClaudeTurnFailed("needs_api_key", "the user's Anthropic API key was removed since admission")
        if key["base_url"]:
            raise ClaudeTurnFailed("api_key_endpoint", "the user's Anthropic API key was set to another endpoint")
        self._handed = "api_key"
        return Credential(key["api_key"], kind="api_key")

    async def refused(self) -> None:
        if self._handed == "api_key":
            raise ClaudeTurnFailed("api_key_refused", "Anthropic refused the user's API key")
        if self._handed is not None:
            await refuse(self.user_id, *self._handed)
        raise ClaudeTurnFailed("token_refused", "Claude refused the saved setup token")


def turn_launcher(
    sandbox: PTCSandbox, *, computer_id: str | None, user_id: str, admitted: str | None, saved: str | None,
) -> Launcher:
    """The launcher of a turn in ``sandbox``, on the computer ``computer_id``.

    On a computer of the user's own the CLI runs on its machine, signed in
    with the computer's token kept there, else the saved one where admission
    let the turn be relayed it (``saved``), so no turn signs in with a token
    its run's row is not stamped for; the launcher tells the backend which it used, whether
    Claude refused it, and whether an organization's policy is installed
    there. Anywhere else it runs on the credential the user's turns on a
    cloud computer run on (``HostedCredential``): inside the computer's own
    sandbox where ``claude_code.sandbox_placement`` is on, else in this
    worker, in one of the worker's slots on the platform.
    """
    if not isinstance(sandbox.provider, SSHProvider):
        if get_claude_code_config().sandbox_placement:
            return sandbox_launcher(sandbox, identity=user_id, credential=HostedCredential(user_id, admitted))
        return LocalLauncher(saved=HostedCredential(user_id, admitted), slots=worker_slots())
    if computer_id is None:
        raise RuntimeError("a turn on a computer of the user's own names that computer")
    token = ComputerToken(user_id, computer_id)

    async def link() -> Link:
        await sandbox.wait_ready()
        return await sandbox.runtime.link()

    return SSHLauncher(
        Machine(link, lambda: sandbox.working_dir), identity=user_id, token_file=token_name(computer_id),
        credential=token if saved else None, on_announced=token.announced, on_file_refused=token.file_refused,
        on_blocked=token.blocked, on_no_perl=token.no_perl, on_unsupported=token.unsupported,
    )
