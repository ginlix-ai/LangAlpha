"""Where a Claude Code turn's CLI runs, and on which credential.

The one module here that imports the runtime at its top, so it is imported
only as a Claude Code turn is built (``AgentInputs.claude_code_launcher``):
the SDK it loads is an optional extra that no other turn and no startup pays
for.
"""

from __future__ import annotations

import asyncio
from functools import partial
from typing import TYPE_CHECKING, Any

from ptc_agent.agent.claude_code.launcher import Credential, Launcher, LocalLauncher
from ptc_agent.agent.claude_code.machine_token import token_name
from ptc_agent.agent.claude_code.sandbox_launcher import sandbox_launcher
from ptc_agent.agent.claude_code.ssh_launcher import CLI_SLOT_WAIT_S, SSHLauncher
from ptc_agent.config.agent import Admitted
from ptc_agent.core.sandbox.providers._ssh_transport import Link, Machine
from ptc_agent.core.sandbox.providers.ssh import SSHProvider
from src.llms.claude_code import ClaudeTurnFailed
from src.server.services.computer_usage.causes import computer_cause

from .cli_slots import computer_cli_slot
from .hosted import OnOwn, Refused, cli_placement, cloud_runs_in
from .machine import RUNS_CLI, ComputerToken
from .plan import UserPlan
from .saved import refusal_stamp, refuse, stored_row

if TYPE_CHECKING:
    from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox


class HostedCredential:
    """The credential a turn on a cloud computer signs in with, as its
    launcher reads it (``launcher.TokenStore``): as each run starts, by the
    rule admission asked (``cli_placement``, for the workspace
    ``workspace_id``), so a run whose user changed their choice since, or
    whose computer was resized below the tier the CLI runs at, is refused as
    admission would refuse it, before any secret is read, and so is one whose
    credential is no longer the one admission let it run on (``admitted``),
    which its run's row is stamped with, ahead of the size, as a credential
    is. A setup token Anthropic refuses is recorded as refused
    (``saved.refuse``) and ends the turn saying so, as admission will from
    then on."""

    def __init__(self, user_id: str, admitted: str | None, workspace_id: str | None) -> None:
        self.user_id = user_id
        self.admitted = admitted
        self.workspace_id = workspace_id
        #: What the last read handed out, for ``refused``: the stored token's
        #: refusal stamp, never the token.
        self._handed: tuple[str, Any] | None = None

    async def read(self) -> Credential | None:
        where = await cli_placement(self.user_id, self.workspace_id)
        hosted = None if isinstance(where, OnOwn) else where.hosted
        # Admission ran it on a cloud computer, whose CLIs now run on the
        # user's own computers, or on another credential.
        if hosted is None or hosted.runs_on not in (None, self.admitted):
            raise ClaudeTurnFailed("choice_changed", "the credential the turn runs on changed since admission")
        if isinstance(where, Refused):
            raise ClaudeTurnFailed(where.reason, "what admission let the turn run on changed as it started", **where.fields)
        self._handed = None
        row = await stored_row(self.user_id)
        if row is None:
            return None
        self._handed = refusal_stamp(row)
        return Credential(row["access_token"])

    async def refused(self) -> None:
        if self._handed is not None:
            await refuse(self.user_id, *self._handed)
        raise ClaudeTurnFailed("token_refused", "Claude refused the saved setup token")


def turn_launcher(
    sandbox: PTCSandbox, *, computer_id: str | None, workspace_id: str | None, user_id: str, admitted: Admitted,
    slot_wait_s: float | None = None,
) -> Launcher:
    """The launcher of a turn in ``sandbox``, in the workspace
    ``workspace_id`` on the computer ``computer_id``, by what admission let it
    run on (``admitted``).

    On a computer of the user's own the CLI runs on its machine, signed in
    with the computer's token kept there, else the saved one where admission
    let the turn be relayed it (``Admitted.saved``), so no turn signs in
    with a token its run's row is not stamped for; the launcher tells the
    backend which it used, whether Claude refused it, and whether an
    organization's policy is installed there. A turn admission let run its
    CLI on another own computer of the user's (``Admitted.cli_computer``), the
    workspace's computer running none for them, runs it on that computer's
    machine, from its root, while its tools stay on ``sandbox``. Anywhere
    else it runs on the credential the user's turns on a cloud computer run
    on (``HostedCredential``), where ``cloud_runs_in`` says: inside the
    computer's own sandbox, holding one of the CLIs it runs at once as on an
    own computer, else in this worker, whose memory is not the computer's. Where the user's choice
    changed since admission, the run is refused as it starts
    (``choice_changed``): by the credential's read on a cloud computer, by
    the first dial of a picked one; that is where a refusal reaches the turn
    as one.
    Either way the launcher keeps what the CLI reports of the Claude plan
    behind the credential it signed in with (``UserPlan``). ``slot_wait_s``
    is how long the CLI on a computer waits for a slot there, by default a
    turn's.
    """
    if admitted.cli_computer is not None:
        picked = _PickedComputer(admitted.cli_computer, user_id, workspace_id)
        return _ssh_launcher(
            Machine(picked.link, picked.root), user_id=user_id, computer_id=admitted.cli_computer, saved=admitted.saved,
            slot_wait_s=slot_wait_s,
        )
    if not isinstance(sandbox.provider, SSHProvider):
        plan = UserPlan(user_id)
        credential = HostedCredential(user_id, admitted.hosted, workspace_id)
        if cloud_runs_in() == "sandbox":
            return sandbox_launcher(
                sandbox, identity=user_id, credential=credential, plan=plan,
                cli_slot=partial(computer_cli_slot, computer_id) if computer_id is not None else None,
                slot_wait_s=CLI_SLOT_WAIT_S if slot_wait_s is None else slot_wait_s,
            )
        return LocalLauncher(saved=credential, plan=plan)
    if computer_id is None:
        raise RuntimeError("a turn on a computer of the user's own names that computer")

    async def link() -> Link:
        await sandbox.wait_ready()
        return await sandbox.runtime.link()

    return _ssh_launcher(
        Machine(link, lambda: sandbox.working_dir), user_id=user_id, computer_id=computer_id, saved=admitted.saved,
        slot_wait_s=slot_wait_s,
    )


def _ssh_launcher(
    machine: Machine[Link], *, user_id: str, computer_id: str, saved: str | None, slot_wait_s: float | None,
) -> SSHLauncher:
    """The CLI on the own computer ``computer_id``'s ``machine``, signed in with
    the token kept there, or relayed the saved one where admission let it
    (``saved``), and telling the backend what it found of either."""
    token = ComputerToken(user_id, computer_id)
    return SSHLauncher(
        machine, identity=user_id, token_file=token_name(computer_id), cli_slot=partial(computer_cli_slot, computer_id),
        credential=token if saved else None, on_announced=token.announced, on_file_refused=token.file_refused,
        on_blocked=token.blocked, on_no_perl=token.no_perl, on_unsupported=token.unsupported,
        plan=UserPlan(user_id, computer_id),
        slot_wait_s=CLI_SLOT_WAIT_S if slot_wait_s is None else slot_wait_s,
    )


#: The refusals of a pick of the user's own computer that finds none online
#: (``hosted.own_cli_computer``).
_PICK_REFUSALS = frozenset({"no_own_computer", "cli_computer_offline"})


class _PickedComputer:
    """The own computer ``computer_id`` that runs a turn's CLI for the
    workspace ``workspace_id`` on another, dialed as the run starts. First
    the rule admission asked (``cli_placement``) is asked again, as
    ``HostedCredential`` does: where it no longer runs the CLI on an own
    computer the user's choice changed (``choice_changed``). A pick that now
    lands on another own computer, or finds none, as while another turn
    starts the admitted one, keeps the admitted one, whose own reach the
    dial checks. One the
    idle sweep stopped is started first, as a workspace's acquire starts its
    computer, and one gone out of reach since admission, or since a link
    there, ends the turn saying so (``cli_computer_offline``). Each link
    counts as the computer's activity, so the idle sweep keeps one that runs
    only other workspaces' CLIs."""

    def __init__(self, computer_id: str, user_id: str, workspace_id: str | None) -> None:
        self.computer_id = computer_id
        self.user_id = user_id
        self.workspace_id = workspace_id
        self.name = computer_id
        self.machine: Machine[Link] | None = None

    def root(self) -> str:
        return self.machine.root()

    async def link(self) -> Link:
        from src.server.database.computer import update_computer_activity

        try:
            if self.machine is None:
                self.machine = await self._dial()
            linked = await self.machine.link()
        except ClaudeTurnFailed:
            raise
        except Exception as e:  # noqa: BLE001
            raise ClaudeTurnFailed("cli_computer_offline", str(e) or type(e).__name__, name=self.name) from e
        await update_computer_activity(self.computer_id)
        return linked

    async def _dial(self) -> Machine[Link]:
        from ptc_agent.core.project_context import context_without_project
        from src.server.database.computer import get_computer
        from src.server.models.computer import ComputerStatus as Lifecycle
        from src.server.services.workspace_manager import WorkspaceManager

        from .machine import machine_of

        where = await cli_placement(self.user_id, self.workspace_id)
        # Only the pick's own refusals leave the choice as it was; any other
        # is a choice that moved off the user's own computers.
        if not (isinstance(where, OnOwn) and where.picked or isinstance(where, Refused) and where.reason in _PICK_REFUSALS):
            raise ClaudeTurnFailed("choice_changed", "the user's computers no longer run the turn's CLI")
        computer = await get_computer(self.computer_id)
        self.name = computer["name"] if computer else self.computer_id
        # One gone, or no longer the user's, runs no CLI.
        if computer is None or computer.get("user_id") != self.user_id:
            raise ClaudeTurnFailed("cli_computer_offline", "the computer is gone", name=self.name)
        status = computer.get("status")
        if status not in RUNS_CLI:
            raise ClaudeTurnFailed("cli_computer_offline", f"the computer is {status}", name=self.name)
        if status != Lifecycle.RUNNING:
            # Out of the turn's workspace: a start lays out the bound one's
            # folder, which has no place on this computer.
            with computer_cause("claude_code"):
                wake_task = asyncio.get_running_loop().create_task(
                    WorkspaceManager.get_instance().wake_computer(self.computer_id, user_id=self.user_id),
                    context=context_without_project(),
                )
            await wake_task
            computer = await get_computer(self.computer_id) or computer
        return await machine_of(computer)
