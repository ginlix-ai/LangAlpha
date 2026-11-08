"""Where a Claude Code turn on a cloud computer runs, and on which credential.

On a self-hosted server the credential is the setup token each user saves in
Settings (``saved``), and the CLI runs in the backend worker that owns the
turn, or inside the computer's own sandbox with
``claude_code.sandbox_placement`` on. The platform runs it only inside the
sandbox, so its users choose how Claude Code runs on cloud computers
(``claude_code_custody``) only with that switch on: on the setup token they
let the server store, behind its operator's switch
(``claude_code.stored_custody``), which also signs in their own computers
that have none. With placement off, or that switch off, no turn reads a
user's choice, and cloud computers there run no Claude Code, as before the
choice existed; only turning Claude Code off in
Experiments reads it (``opted_out``). Where the CLI runs is decided once
(``cloud_runs_in``), and wherever it runs in the sandbox only a computer at
``SANDBOX_MIN_TIER`` or above runs it (``tier_refusal``).
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from functools import partial
from typing import Any, Literal

from src.config.settings import get_claude_code_config
from src.llms.claude_code import backend_available, rows_available
from src.server.database.claude_code_hosted import (
    Custody,
    Mode,
    StoringBlocked,
    get_custody,
    live_stored_task_runs,
    live_stored_turns,
    write_custody,
)
from src.server.models.claude_code import ClaudeCodeCustody, ComputerRefusal, HostedMode

logger = logging.getLogger(__name__)

#: The version of the consent text a user accepts to store their setup token.
DISCLOSURE = "2026-11-v1"
#: The smallest computer that runs the CLI inside its own sandbox: Medium,
#: whose resource tier key this is.
SANDBOX_MIN_TIER = "performance"

#: The memory each Claude Code CLI on a cloud computer is counted at: its
#: tier's memory over this is how many it runs at once.
CLI_MEMORY_MB = 800
#: Why a Task is refused once the computer runs that many, as the refusal
#: reads after "could not start <task>".
CLI_CAP_REFUSAL = (
    "Claude Code is already running {n} agents on this computer, the most it runs at once. "
    "Wait for one of your subagents to finish, then launch this one again"
)

RunsOn = Literal["saved_token", "stored_token"]
#: Where a turn's CLI runs on a cloud computer: inside the computer's own
#: sandbox, or in the backend worker that owns the turn.
CloudRunsIn = Literal["sandbox", "worker"]


@dataclass(frozen=True)
class Hosted:
    """How the user's turns on a cloud computer run: on ``runs_on``, or
    refused for ``refusal``, a ``CLAUDE_CODE_OFF`` reason. ``saved_token``
    is a self-hosted server's, ``stored_token`` the same row on the platform,
    kept there only by the user's choice."""

    runs_on: RunsOn | None = None
    refusal: str | None = None

    def __post_init__(self) -> None:
        if (self.runs_on is None) == (self.refusal is None):
            raise ValueError("a turn on a cloud computer either runs on a credential or is refused")

    @property
    def token_signs_in(self) -> bool:
        """Whether the setup token the user saved signs these turns in, and so
        is also relayed to their own computers that have none."""
        return self.runs_on in ("saved_token", "stored_token")


@dataclass(frozen=True)
class Placement:
    """Where a Claude Code turn in a workspace runs: on ``own_computer``, the
    row of the user's own computer, else on the credential ``hosted`` names.
    ``refusal`` is the first of every reason a turn on a cloud computer is
    refused (``cloud_placement``), the credential's among them, so every
    caller tells the user the same one."""

    own_computer: dict | None = None
    hosted: Hosted | None = None
    refusal: str | None = None


class TurnsNotStopped(Exception):
    """The user's stored setup token is gone, but ``count`` of their turns on it did not stop."""

    def __init__(self, count: int) -> None:
        super().__init__(count)
        self.count = count


class CustodyRefused(Exception):
    """A choice this server or this user's block does not allow, for ``reason``."""

    def __init__(self, reason: Literal["unavailable", "stored_blocked", "disclosure"]) -> None:
        super().__init__(reason)
        self.reason = reason


def _platform() -> bool:
    from src.config.settings import HOST_MODE

    return HOST_MODE == "platform" and rows_available()


def _offered() -> list[HostedMode]:
    """The modes the operator lets users choose for cloud computers on the platform."""
    return ["stored"] if get_claude_code_config().stored_custody else []


def cloud_runs_in() -> CloudRunsIn | None:
    """Where this server runs the CLI of a turn on a cloud computer, the one
    read of ``claude_code.sandbox_placement``: inside the computer's sandbox
    with it on, else in the worker on a self-hosted server. The platform runs
    it only in the sandbox, and only once storing a token is offered;
    None where this server runs none on a cloud computer."""
    in_sandbox = get_claude_code_config().sandbox_placement
    if backend_available():
        return "sandbox" if in_sandbox else "worker"
    return "sandbox" if in_sandbox and _platform() and _offered() else None


def in_sandbox() -> bool:
    """Whether a turn on a cloud computer runs its CLI inside the computer's
    own sandbox, the one placement where the computer's size refuses it."""
    return cloud_runs_in() == "sandbox"


def checks_automations() -> bool:
    """Whether a Claude Code automation's save is checked against how its
    turns would run: where users choose how it runs on cloud computers,
    whose stored token automations never run on, here or relayed to their own
    computers, and where a cloud computer's size can refuse it."""
    return choice_offered() or in_sandbox()


def choice_offered() -> bool:
    """Whether users choose how Claude Code runs on this server's cloud
    computers: on the platform, once the operator turned storing a token on.
    The choice stays theirs to change with the CLI's placement off, where
    their stored token still signs in their own computers and leaving
    ``stored`` deletes it; their cloud turns are refused there
    (``cloud_placement``)."""
    return _platform() and bool(_offered())


async def hosted(user_id: str, *, conn=None) -> Hosted | None:
    """How the user's turns on a cloud computer run here; None where none
    do. Asked at admission and again as each run starts, since the user's
    choice may change in between. ``conn`` reads on a caller's
    connection, as ``placement`` takes it."""
    if backend_available():
        return Hosted("saved_token")
    if not choice_offered():
        return None
    return _verdict(await get_custody(user_id, conn=conn))


def _verdict(current: Custody) -> Hosted:
    """How a turn runs on the choice ``current``, read only while storing is
    offered (``choice_offered``). A blocked user is told storing is off for
    them, whatever they chose."""
    if current.blocked:
        return Hosted(refusal="stored_blocked")
    return Hosted("stored_token") if current.mode == "stored" else Hosted(refusal="hosted_off")


def tier_refusal(computer: Mapping[str, Any]) -> ComputerRefusal | None:
    """Why a turn on the cloud computer ``computer`` is refused whatever its
    credential where the CLI runs inside its sandbox: ``needs_bigger_computer``
    where its sandbox, as its provider builds it (``ComputerManager.tier_size``),
    gets less cpu or memory than one at ``SANDBOX_MIN_TIER``. While a resize
    is under way, or once one was interrupted, the sandbox may run at either
    tier, so both must fit. The
    server checks at startup that every kind it creates offers
    ``SANDBOX_MIN_TIER`` (``check_sandbox_tiers``)."""
    from src.server.services.computer_manager import ComputerManager

    floor = ComputerManager.get_instance().tier_size(computer, SANDBOX_MIN_TIER)
    for size in _sizes(computer):
        if floor is None or size is None or size[0] < floor[0] or size[1] < floor[1]:
            return "needs_bigger_computer"
    return None


def cli_cap(computer: Mapping[str, Any]) -> int | None:
    """How many Claude Code CLIs ``computer`` runs at once, its turns' and
    their subagents' together: an own computer as many as its connection's
    CLI slots, any other one per ``CLI_MEMORY_MB`` of the smallest size its
    sandbox may run at (``tier_refusal``). Where the CLIs run in the
    backend instead (``cloud_runs_in`` ``worker``), the computer's size
    is not theirs, so it runs as many as one at ``SANDBOX_MIN_TIER``. None
    where a size is unknown."""
    from ptc_agent.core.sandbox.providers._ssh_transport import _CLI_SLOTS
    from src.server.services.computer_manager import ComputerManager
    from src.server.services.ssh_computers import is_own_computer

    if is_own_computer(computer):
        return _CLI_SLOTS
    sizes = _sizes(computer) if in_sandbox() else [ComputerManager.get_instance().tier_size(computer, SANDBOX_MIN_TIER)]
    if any(size is None for size in sizes):
        return None
    return int(min(size[1] for size in sizes) * 1024 // CLI_MEMORY_MB)


def _sizes(computer: Mapping[str, Any]) -> list[tuple[float, float] | None]:
    """The sizes ``computer``'s sandbox may run at: its tier's, and while a
    resize is under way, or once one was interrupted, both tiers'."""
    from src.server.services.computer_manager import ComputerManager
    from src.server.services.spec_change import spec_change_from_row

    change = spec_change_from_row(computer)
    tiers = {computer.get("resource_tier")}
    if change is not None and (
        change.state == "in_progress" or (change.error is not None and change.error.code == "interrupted")
    ):
        tiers |= {change.from_tier, change.target_tier}
    manager = ComputerManager.get_instance()
    return [manager.tier_size(computer, tier) for tier in tiers]


def check_sandbox_tiers(sandbox: Any) -> None:
    """Fail startup where the CLI runs inside cloud computers' sandboxes and a
    kind this server creates cloud computers on cannot size one at
    ``SANDBOX_MIN_TIER`` (``ComputerManager.tier_size``): every computer of
    that kind would be refused as too small.

    Raises:
        RuntimeError: A creatable cloud kind cannot size ``SANDBOX_MIN_TIER``.
    """
    from src.server.services.computer_manager import ComputerManager

    manager = ComputerManager.current()
    if not in_sandbox() or manager is None:
        return
    for kind in sandbox.creatable_providers():
        if kind != "ssh" and manager.tier_size({"kind": kind}, SANDBOX_MIN_TIER) is None:
            raise RuntimeError(
                f"claude_code.sandbox_placement is on, but {kind} computers offer no {SANDBOX_MIN_TIER!r} tier"
            )


async def cloud_refusals(user_id: str) -> dict[str, ComputerRefusal]:
    """Each of the user's cloud computers that refuses a turn whatever its
    credential (``tier_refusal``), by id; none where the CLI does not run in
    their sandboxes."""
    from src.server.database.computer import get_computers_for_user
    from src.server.services.ssh_computers import is_own_computer

    if not in_sandbox():
        return {}
    refusals: dict[str, ComputerRefusal] = {}
    for computer in await get_computers_for_user(user_id):
        if is_own_computer(computer):
            continue
        try:
            reason = tier_refusal(computer)
        except ValueError as e:
            logger.warning(f"[claude_code] cannot size computer_id={computer['computer_id']} for Claude Code: {e}")
            continue
        if reason:
            refusals[str(computer["computer_id"])] = reason
    return refusals


async def placement(user_id: str, workspace_id: str | None, *, conn=None, token: bool = False) -> Placement:
    """Where the user's Claude Code turn in ``workspace_id`` runs, settled once
    at admission: the own computer first, so a workspace on one never reads
    the choice for cloud computers, else as ``cloud_placement`` says.
    ``conn`` reads on a caller's connection, as an automation file save that
    holds the user's locks passes it, so the check never waits on a second
    pool slot."""
    from src.server.database.computer import get_computer_for_workspace
    from src.server.services.ssh_computers import is_own_computer

    computer = await get_computer_for_workspace(workspace_id, conn=conn) if workspace_id else None
    if is_own_computer(computer):
        return Placement(own_computer=computer)
    return await cloud_placement(user_id, computer, conn=conn, token=token)


async def cloud_placement(
    user_id: str, computer: Mapping[str, Any] | None, *, conn=None, token: bool = False,
) -> Placement:
    """Where the user's turn on the cloud computer ``computer`` (its row, or
    None where none is bound yet) runs, and the first reason it is refused:
    none run here (``needs_own_computer``), the credential (``hosted``), an
    organization's policy on the worker that would run it, with ``token`` the
    saved setup token known not to work (admission's, which reads the token
    store), and only then the computer's size, so a user hears of the size
    once the credential would run. A computer not bound yet is judged as the
    run starts, where it always is."""
    runs_in = cloud_runs_in()
    state = await hosted(user_id, conn=conn) if runs_in is not None else None
    if state is None:
        return Placement(refusal="needs_own_computer")
    if state.refusal is not None:
        return Placement(hosted=state, refusal=state.refusal)
    if runs_in == "worker":
        from ptc_agent.core.claude_code_managed import local_managed

        if await local_managed():
            return Placement(hosted=state, refusal="policy_blocked")
    if token and state.token_signs_in:
        from .saved import saved_refusal

        if reason := await saved_refusal(user_id):
            return Placement(hosted=state, refusal=reason)
    if runs_in == "sandbox" and computer is not None and (reason := tier_refusal(computer)):
        return Placement(hosted=state, refusal=reason)
    return Placement(hosted=state)


def relay_offered() -> bool:
    """Whether a turn on an own computer is offered the saved token at all,
    decided without reading a choice: the machine then waits for one only
    where some user's token signs their turns in (``saved_token_runs``)."""
    return backend_available() or (_platform() and "stored" in _offered())


async def saved_token_runs(user_id: str, *, conn=None) -> bool:
    """Whether the setup token the user saved signs in their turns
    (``Hosted.token_signs_in``), and is relayed to their own computers that
    have none. ``conn`` reads on a caller's connection, as ``hosted`` takes it."""
    state = await hosted(user_id, conn=conn)
    return state is not None and state.token_signs_in


async def custody(user_id: str) -> ClaudeCodeCustody | None:
    """The user's choice as Settings shows it, with the verdict admission
    gives a turn there, ``needs_own_computer`` while this server runs none on
    a cloud computer. None where none is offered."""
    if not choice_offered():
        return None
    current = await get_custody(user_id)
    verdict = Hosted(refusal="needs_own_computer") if cloud_runs_in() is None else _verdict(current)
    return ClaudeCodeCustody(
        mode=current.mode,
        offered=[mode for mode in _offered() if not (mode == "stored" and current.blocked)], blocked=current.blocked,
        runs_on=verdict.runs_on, refusal=verdict.refusal, disclosure=DISCLOSURE,
    )


async def choose(user_id: str, mode: Mode, disclosure: str | None = None) -> ClaudeCodeCustody | None:
    """Save the user's choice; None where none is offered. Storing their
    token needs its switch, no block, and the current consent text accepted;
    any other choice deletes the token and stops the turns on it."""
    current = await custody(user_id)
    if current is None:
        return None
    if mode != "local" and mode not in current.offered:
        raise CustodyRefused("stored_blocked" if mode == "stored" and current.blocked else "unavailable")
    if mode == "stored":
        if disclosure != DISCLOSURE:
            raise CustodyRefused("disclosure")
        try:
            await write_custody(user_id, mode, disclosure=disclosure)
        except StoringBlocked:
            raise CustodyRefused("stored_blocked") from None
    else:
        await drop_stored(user_id, mode)
    return await custody(user_id)


async def drop_stored(user_id: str, mode: Mode = "local", *, block_reason: str | None = None) -> int:
    """Delete the user's stored setup token as their choice becomes ``mode``
    (an operator's block with ``block_reason``), forget what its check
    found, and stop each of their turns on it; how many."""
    from src.utils.cache.redis_cache import get_cache_client

    from .saved import token_check_key

    token = await write_custody(user_id, mode, block_reason=block_reason)
    if token is not None:
        await get_cache_client().delete(token_check_key(token))
    stopped = await stop_stored_turns(user_id)
    if token is not None or stopped or block_reason is not None:
        logger.info(
            f"[claude_code] stored token dropped for user_id={user_id} mode={mode} "
            f"blocked={block_reason is not None} turns_stopped={stopped}"
        )
    return stopped


async def stop_stored_turns(user_id: str) -> int:
    """Stop each of the user's turns on their stored setup token, once it is
    gone, and each subagent run on it, which may outlive its turn; how many.
    Only the platform stamps them. Each is asked to stop before a failed stop
    raises, since every one still runs on the token."""
    from src.config.settings import HOST_MODE
    from src.server.services.cancel_dispatch import cancel_subagent_task, cancel_workflow

    if HOST_MODE != "platform":
        return 0
    stops = [partial(cancel_workflow, thread_id, run_id, by_user=False) for thread_id, run_id in await live_stored_turns(user_id)]
    stops += [
        partial(cancel_subagent_task, thread_id, task_id, task_run_id=run_id)
        for thread_id, task_id, run_id in await live_stored_task_runs(user_id)
    ]
    failed: list[Exception] = []
    for stop in stops:
        try:
            await stop()
        except Exception as exc:
            failed.append(exc)
    if failed:
        logger.warning(f"[claude_code] {len(failed)} of {len(stops)} stored-token runs did not stop for user_id={user_id}")
        raise TurnsNotStopped(len(failed)) from failed[0]
    return len(stops)


async def opted_out(user_id: str) -> None:
    """The user is turning Claude Code off: on the platform the token stored
    for them goes, as leaving ``stored`` takes it, whatever the switches say
    now; a self-hosted server keeps the token it saved, for when they turn it
    on again. With it gone already, the turns still on it are stopped again:
    a drop whose stop failed left the choice ``local`` and saved nothing, so
    the retry is the one left to stop them."""
    from src.config.settings import HOST_MODE

    if HOST_MODE != "platform":
        return
    if (await get_custody(user_id)).mode == "stored":
        await drop_stored(user_id)
    else:
        await stop_stored_turns(user_id)


def register_opt_out() -> None:
    """Have every way the user's Claude Code feature resolves off (their
    override set off, or cleared) run ``opted_out`` before it is saved: once
    it is back on, nothing could tell a token left by a failed drop from one
    they store."""
    from src.llms.claude_code import FEATURE
    from src.server.services.features import register_feature_off_hook

    register_feature_off_hook(FEATURE, opted_out, before_save=True)
