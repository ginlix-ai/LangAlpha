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
from dataclasses import dataclass, field
from functools import partial
from typing import Any, Literal

from ptc_agent.core.claude_code_clis import CLIS_DEFAULT, CLIS_MAX, agents
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
from src.server.models.claude_code import (
    ClaudeCodeCustody, ClaudeCodeRunsOn, ComputerRefusal, HostedMode, PlacementRefusal,
)

logger = logging.getLogger(__name__)

#: The version of the consent text a user accepts to store their setup token.
DISCLOSURE = "2026-11-v2"
#: The smallest computer that runs the CLI inside its own sandbox: Medium,
#: whose resource tier key this is.
SANDBOX_MIN_TIER = "performance"

#: Where a computer of the user's own keeps, in its settings, how many Claude
#: Code CLIs it runs at once (``own_clis``).
CLIS_KEY = "claude_code_clis"

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
class OnOwn:
    """A Claude Code turn's CLI runs on ``computer``, the row of one of the
    user's own computers: the workspace's own, or one the server picked
    (``picked``) for a workspace on a cloud computer that runs none for them,
    whose tools stay on that cloud computer."""

    computer: dict
    picked: bool = False


@dataclass(frozen=True)
class OnCloud:
    """A Claude Code turn on the cloud computer ``computer`` (its row, None
    where none is bound yet) runs on the credential ``hosted`` names, its CLI
    where ``cloud_runs_in`` says."""

    hosted: Hosted
    computer: dict | None = None


@dataclass(frozen=True)
class Refused:
    """No Claude Code turn runs there, for ``reason``, the first of every
    reason, so every caller tells the user the same one; ``fields`` is what
    its words name (the offline computer's ``name``, or for a computer too
    small whether it is Home's, ``home``, and the ``computer_id`` and
    ``name`` its fix acts on), and ``hosted`` the credential
    that would run there where the refusal is not the credential's."""

    reason: PlacementRefusal
    fields: Mapping[str, Any] = field(default_factory=dict)
    hosted: Hosted | None = None


#: Where a Claude Code turn in a workspace runs (``cli_placement``).
Placement = OnOwn | OnCloud | Refused


def picks_own(where: Placement) -> bool:
    """Whether ``where`` is a cloud computer that runs none of the user's CLIs,
    so an own computer the server picks runs them, or none is online."""
    if isinstance(where, OnOwn):
        return where.picked
    return isinstance(where, Refused) and where.reason in ("no_own_computer", "cli_computer_offline")


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
    turns would run: where users choose how it runs on cloud computers, and
    where a cloud computer's size can refuse it."""
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


def own_clis(computer: Mapping[str, Any]) -> int:
    """How many Claude Code CLIs the user's own ``computer`` runs at once:
    the number they set for it (``CLIS_KEY``), else ``CLIS_DEFAULT``."""
    clis = (computer.get("config") or {}).get(CLIS_KEY)
    return clis if type(clis) is int and 1 <= clis <= CLIS_MAX else CLIS_DEFAULT


async def computer_clis(computer_id: str) -> int:
    """``cli_cap`` of the computer ``computer_id`` as it stands now, for a
    CLI starting there; one gone, or of a size not known, runs ``CLIS_DEFAULT``."""
    from src.server.database.computer import get_computer

    computer = await get_computer(computer_id)
    cap = cli_cap(computer) if computer is not None else None
    return CLIS_DEFAULT if cap is None else cap


def cli_cap_refusal(cap: int, *, alone: bool) -> str:
    """Why a Task is refused once its computer runs ``cap`` CLIs, as the
    refusal reads after "could not start <task>". Where the turn launching
    it is the one CLI the computer runs (``alone``), no CLI that ends makes
    room, so it is not told to wait."""
    if alone:
        return (
            "Claude Code runs only 1 agent at once on this computer, and this turn is that agent, "
            "so this subagent can't run on Claude Code beside it"
        )
    return (
        f"Claude Code is already running {agents(cap)} on this computer, the most it runs at once. "
        "Wait for one of your subagents to finish, then launch this one again"
    )


def cli_cap(computer: Mapping[str, Any]) -> int | None:
    """How many Claude Code CLIs ``computer`` runs at once, its turns' and
    their subagents' together: an own computer as many as its owner set
    (``own_clis``), any other one per ``claude_code.cli_memory_mb`` of the smallest size its
    sandbox may run at (``tier_refusal``). Where the CLIs run in the
    backend instead (``cloud_runs_in`` ``worker``), the computer's size
    is not theirs, so it runs as many as one at ``SANDBOX_MIN_TIER``. None
    where a size is unknown; one that runs Claude Code at all runs at least one."""
    from src.server.services.computer_manager import ComputerManager
    from src.server.services.ssh_computers import is_own_computer

    if is_own_computer(computer):
        return own_clis(computer)
    sizes = _sizes(computer) if in_sandbox() else [ComputerManager.get_instance().tier_size(computer, SANDBOX_MIN_TIER)]
    if any(size is None for size in sizes):
        return None
    return max(1, int(min(size[1] for size in sizes) * 1024 // get_claude_code_config().cli_memory_mb))


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


async def cloud_view(user_id: str) -> tuple[dict[str, ComputerRefusal], Placement]:
    """Each of the user's cloud computers too small for Claude Code
    (``cloud_refusals``), and where their cloud computers' turns run, judged
    as one not bound yet is (``cloud_placement``), read once for both."""
    where = await cloud_placement(user_id, None)
    return await cloud_refusals(user_id, where), where


async def cloud_refusals(user_id: str, where: Placement) -> dict[str, ComputerRefusal]:
    """Each of the user's cloud computers that refuses a turn whatever its
    credential (``tier_refusals``), by id; none where their CLI runs on
    their own computers instead, as ``where``, their cloud computers'
    placement, says (``picks_own``), where a cloud computer's size refuses
    nothing."""
    return {} if picks_own(where) else await tier_refusals(user_id)


async def tier_refusals(user_id: str) -> dict[str, ComputerRefusal]:
    """Each of the user's cloud computers too small to run the CLI inside
    its sandbox (``tier_refusal``), by id, whatever the user chose for
    cloud computers, so a choice to run it there can be warned of before it
    is made; none where the CLI does not run in their sandboxes."""
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


async def home_computer(user_id: str, *, conn=None) -> dict | None:
    """Home's computer: the one it is bound to, else the primary computer it
    binds to on its first start or turn."""
    from src.server.database.computer import get_computer_for_workspace, get_primary_computer
    from src.server.database.home_workspace import get_home_workspace_id

    return await get_computer_for_workspace(get_home_workspace_id(user_id), conn=conn) or await get_primary_computer(
        user_id, conn=conn,
    )


async def cli_placement(user_id: str, workspace_id: str | None, *, conn=None, token: bool = False) -> Placement:
    """Where the CLI of the user's Claude Code turn in ``workspace_id`` runs,
    the one rule for analysts and the Chief of Staff alike: on the
    workspace's computer where it is one of the user's own, so a workspace on
    one never reads the choice for cloud computers, else as
    ``cloud_placement`` says. Home not bound yet is judged on the primary
    computer it binds to, and a refusal for a computer too small says whether
    it is Home's. ``conn`` reads on a caller's connection, as an automation
    file save that holds the user's locks passes it, so the check never
    waits on a second pool slot."""
    from src.server.database.computer import get_computer_for_workspace
    from src.server.database.home_workspace import is_home_id
    from src.server.services.ssh_computers import is_own_computer

    home = bool(workspace_id) and is_home_id(user_id, workspace_id)
    if home:
        computer = await home_computer(user_id, conn=conn)
    else:
        computer = await get_computer_for_workspace(workspace_id, conn=conn) if workspace_id else None
    if is_own_computer(computer):
        return OnOwn(computer)
    return await cloud_placement(user_id, computer, home=home, conn=conn, token=token)


async def _runs_none(user_id: str, state: Hosted | None, *, conn=None) -> bool:
    """Whether a cloud computer's verdict ``state`` (None where this server
    runs none there) means the user's turns there run their CLI on a computer
    of their own: they chose their own computers, storing their token is
    blocked for them, or the server runs none on cloud computers, except for
    a user who chose to store their token there, since nothing falls back
    from that choice to theirs."""
    if state is not None or not choice_offered():
        return True
    return _verdict(await get_custody(user_id, conn=conn)).runs_on is None


async def own_cli_computer(user_id: str, *, conn=None) -> Placement:
    """The computer of the user's own that runs the CLI of their turns in
    workspaces on cloud computers (``machine.pick_cli_computer``): one
    picked but offline is named (``cli_computer_offline``); with none to
    pick, the user is told to sign one in (``no_own_computer``)."""
    from .machine import pick_cli_computer

    computer, offline = await pick_cli_computer(user_id, conn=conn)
    if computer is not None:
        return OnOwn(computer, picked=True)
    return Refused("cli_computer_offline", {"name": offline}) if offline else Refused("no_own_computer")


async def cloud_placement(
    user_id: str, computer: Mapping[str, Any] | None, *, home: bool = False, conn=None, token: bool = False,
) -> Placement:
    """Where the user's turn on the cloud computer ``computer`` (its row, or
    None where none is bound yet) runs. Where the cloud computer runs none
    for them (``_runs_none``), on a computer of their own
    (``own_cli_computer``). Else the first reason it is refused: none run here
    (``needs_own_computer``), an organization's policy on the worker that
    would run it, with ``token`` the saved setup token known not to work
    (admission's, which reads the token store), and only then the
    computer's size, so a user hears of the size once the credential would
    run; ``home`` says the computer is Home's. A computer not bound yet is
    judged as the run starts, where it always is."""
    runs_in = cloud_runs_in()
    state = await hosted(user_id, conn=conn) if runs_in is not None else None
    if state is None or state.refusal is not None:
        if await _runs_none(user_id, state, conn=conn):
            return await own_cli_computer(user_id, conn=conn)
        return Refused("needs_own_computer")
    if runs_in == "worker":
        from ptc_agent.core.claude_code_managed import local_managed

        if await local_managed():
            return Refused("policy_blocked", hosted=state)
    if token and state.token_signs_in:
        from .saved import saved_refusal

        if reason := await saved_refusal(user_id, conn=conn):
            return Refused(reason, hosted=state)
    if runs_in == "sandbox" and computer is not None and (reason := tier_refusal(computer)):
        fields = {"home": home, "computer_id": str(computer["computer_id"]), "name": computer["name"]}
        return Refused(reason, fields, hosted=state)
    return OnCloud(state, dict(computer) if computer is not None else None)


def runs_on_view(where: Placement) -> ClaudeCodeRunsOn:
    """Where a turn's CLI runs by ``where``, as Settings and the composer
    show it: on an own computer, the workspace's (``own``) or one the server
    picked (``picked``), inside the cloud computer's sandbox
    (``home_cloud``, read for Home), or in this server's workers
    (``server``); or why it runs nowhere, there."""
    if isinstance(where, OnOwn):
        computer = where.computer
        return ClaudeCodeRunsOn(
            kind="picked" if where.picked else "own", computer_id=str(computer["computer_id"]), name=computer["name"],
        )
    cloud = "home_cloud" if in_sandbox() else "server"
    if isinstance(where, Refused):
        return ClaudeCodeRunsOn(
            kind="picked" if picks_own(where) else cloud, computer_id=where.fields.get("computer_id"),
            name=where.fields.get("name"), refusal=where.reason,
        )
    if cloud == "server":
        return ClaudeCodeRunsOn(kind="server")
    computer = where.computer
    return ClaudeCodeRunsOn(
        kind="home_cloud", computer_id=str(computer["computer_id"]) if computer else None,
        name=computer["name"] if computer else None,
    )


async def cli_host(thread_id: str, cli_computer: str | None) -> dict | None:
    """The computer that runs the CLIs of a thread's Claude Code runs and
    counts them (``cli_cap``): the own computer ``cli_computer`` admission
    picked for its workspace, else the thread's."""
    from src.server.database.computer import get_computer, get_computer_for_thread

    return await (get_computer(cli_computer) if cli_computer else get_computer_for_thread(thread_id))


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
        runs_on=verdict.runs_on, refusal=verdict.refusal,
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
