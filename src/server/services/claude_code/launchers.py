"""Where a Claude Code turn's CLI runs, and on which credential.

The one module here that imports the runtime at its top, so it is imported
only as a Claude Code turn is built (``AgentInputs.claude_code_launcher``):
the SDK it loads is an optional extra that no other turn and no startup pays
for.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

from ptc_agent.agent.claude_code.launcher import Credential, Launcher, LocalLauncher
from ptc_agent.agent.claude_code.machine_token import token_name
from ptc_agent.agent.claude_code.sandbox_launcher import sandbox_launcher
from ptc_agent.agent.claude_code.ssh_launcher import SSHLauncher
from ptc_agent.core.sandbox.providers._ssh_transport import Link, Machine
from ptc_agent.core.sandbox.providers.ssh import SSHProvider
from src.llms.claude_code import ClaudeTurnFailed
from src.server.database.api_keys import get_key_with_base_url

from .hosted import ANTHROPIC, cloud_placement, cloud_runs_in, in_sandbox
from .machine import ComputerToken
from .saved import refusal_stamp, refuse, stored_row

if TYPE_CHECKING:
    from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox


class HostedCredential:
    """The credential a turn on a cloud computer signs in with, as its
    launcher reads it (``launcher.TokenStore``): as each run starts, by the
    rule admission asked (``cloud_placement``, on the computer
    ``computer_id``), so a run whose user changed their choice or removed
    their key since, or whose computer was resized below the tier the CLI
    runs at, is refused as admission would refuse it, before any secret is
    read, and so is one whose credential is no longer the one admission let
    it run on (``admitted``), which its run's row is stamped with, ahead of
    the size, as a credential is. A setup token Anthropic refuses is recorded as refused
    (``saved.refuse``) and ends the turn saying so, as admission will from
    then on; an API key it refuses ends the turn saying so and is
    never flagged, since the key's own status lives in Model access."""

    def __init__(self, user_id: str, admitted: str | None, computer_id: str | None) -> None:
        self.user_id = user_id
        self.admitted = admitted
        self.computer_id = computer_id
        #: What the last read handed out, for ``refused``: the API key, or the
        #: stored token's refusal stamp, never the token.
        self._handed: tuple[str, Any] | Literal["api_key"] | None = None

    async def read(self) -> Credential | None:
        from src.server.database.computer import get_computer

        computer = await get_computer(self.computer_id) if self.computer_id and in_sandbox() else None
        where = await cloud_placement(self.user_id, computer)
        runs_on = where.hosted.runs_on if where.hosted is not None else None
        if runs_on is not None and runs_on != self.admitted:
            raise ClaudeTurnFailed("choice_changed", "the credential the turn runs on changed since admission")
        if where.refusal is not None:
            raise ClaudeTurnFailed(where.refusal, "what admission let the turn run on changed as it started")
        self._handed = None
        if runs_on in ("saved_token", "stored_token"):
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
    cloud computer run on (``HostedCredential``), where ``cloud_runs_in``
    says: inside the computer's own sandbox, else in this worker. Where
    this server runs none, the credential's read refuses the run as it
    starts (``needs_own_computer``), which is where a refusal reaches the
    turn as one.
    """
    if not isinstance(sandbox.provider, SSHProvider):
        credential = HostedCredential(user_id, admitted, computer_id)
        if cloud_runs_in() == "sandbox":
            return sandbox_launcher(sandbox, identity=user_id, credential=credential)
        return LocalLauncher(saved=credential)
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
