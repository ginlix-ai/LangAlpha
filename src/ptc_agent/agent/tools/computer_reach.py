"""The Chief of Staff's machine tools, run on any of the user's computers.

A turn's tools close over the one sandbox the turn acquired. The Chief of Staff
works for every workspace, and the user's computers hold work its own computer
cannot see (a Mac, a second cloud computer), so its Bash, ExecuteCode and file
tools take a ``computer`` argument. A call that names another computer runs
the same tool, built over that computer's session, from that computer's root.
The root is bound as the call's project, so every reader of the ambient
project (path folding, the allow list, job paths, the working directory)
agrees on where the call is. Its paths are the machine's own, whole both ways:
the short spelling (``/x``) is anchored on a turn's folder, and from another
computer it would name a file under the root that the agent never meant.

The host resolves and wakes the computer (``ComputerReach``); this module only
routes a call to it.
"""

from __future__ import annotations

import ast
import asyncio
import posixpath
import re
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol, TypeVar

import structlog
from langchain_core.runnables import RunnableConfig
from langchain_core.tools import BaseTool, StructuredTool
from langchain_core.tools.base import _get_runnable_config_param
from pydantic import Field, create_model

from ptc_agent.core.paths import (
    THREAD_INDEX,
    SandboxLayout,
    WorkspaceLayout,
    on_file_mount,
)
from ptc_agent.core.project_context import ProjectContext, project_bound
from ptc_agent.core.sandbox.livefs_runtime.protocol import GENERATIONS, MOUNT
from src.observability import stamp_tool_channel, stamp_tool_provider

if TYPE_CHECKING:
    from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox
    from ptc_agent.core.session import Session

logger = structlog.get_logger(__name__)

T = TypeVar("T")

#: How often a call running on another computer counts as use of it. The idle
#: sweep stops a computer five minutes idle at the soonest, and a foreground
#: call can run ten.
_IN_USE_EVERY_S = 60.0

#: The tools that act on a machine, which the argument is added to.
REACHABLE_TOOLS: tuple[str, ...] = (
    "Bash",
    "ExecuteCode",
    "Read",
    "Write",
    "Edit",
    "Glob",
    "Grep",
)

#: The name of the argument, which the file-card middleware and the history
#: projector also read: a call carrying it made no card.
COMPUTER_ARG = "computer"

COMPUTER_FIELD = (
    "The name of another of the user's computers to run on, as `<activity>` "
    "lists it. There this call starts at that machine's root, not in a "
    "workspace folder, whatever the rest of this description says: write "
    "paths whole. Only its workspace folders are backed up, so a file to keep "
    "goes in one. Leave it out to run on your own computer, in Home."
)

#: The searches that, run over a computer's root, cover every workspace on it.
_SEARCHES = ("Glob", "Grep")

#: The argument each file tool names its path in.
_PATH_ARGS: Mapping[str, str] = {
    "Read": "file_path",
    "Write": "file_path",
    "Edit": "file_path",
    "Glob": "path",
    "Grep": "path",
}

# Where a computer's file mount serves them, which a file call on the whole
# machine could otherwise name: a save there gets none of the answers a save
# through Home does.
_MOUNTED = (MOUNT, GENERATIONS)

_SERVER_KEPT_ELSEWHERE = (
    "Memory, memos, the profile, automations and workflows are the same on "
    "every computer: leave out `computer` to use them."
)

# A workspace's own share of what the mount serves, which Home's spelling of
# the same path would not reach: there it names a folder inside Home's own.
_WORKSPACE_OWN = (WorkspaceLayout.MEMORY_DIR, WorkspaceLayout.TRANSCRIPTS_DIR)

_WORKSPACE_KEPT_ELSEWHERE = (
    "A workspace's memory and past threads are kept by the server, not on its "
    "computer: read its threads with `agent_output`, and hand a change to its "
    "memory to its analyst."
)

# The data modules exist only where a workspace's tool overlay does, which
# another computer has none of for Home.
_DATA_TOOLS_IMPORT = re.compile(r"^\s*(from\s+tools(\.|\s)|import\s+tools\b)", re.MULTILINE)


def _imports_data_tools(code: str) -> bool:
    """Whether ``code`` imports the data modules in any import statement.
    Read from its syntax, so a module built at runtime passes, as the mount
    guard lets it; code that does not parse is read from its text."""
    try:
        tree = compile(code, "<code>", "exec", ast.PyCF_ONLY_AST | ast.PyCF_ALLOW_TOP_LEVEL_AWAIT)
    except (SyntaxError, ValueError):
        return bool(_DATA_TOOLS_IMPORT.search(code))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom) and not node.level:
            modules = [node.module or ""]
        else:
            continue
        if any(m == "tools" or m.startswith("tools.") for m in modules):
            return True
    return False

_DATA_TOOLS_ELSEWHERE = (
    "The data tools (`tools.*`) run only on your own computer. Fetch the "
    "data there, then save what this computer needs with Write and the same "
    "`computer`, or run code here that does not import them."
)


def ran_elsewhere(args: Mapping[str, Any] | None) -> bool:
    """Whether a tool call's arguments name a computer. What it wrote is on
    that computer, so nothing that opens the path on the turn's own (a file
    card, the agent.md hook) may take it for a file there. Read from the
    arguments alone, which the live stream and a replay of the call share."""
    computer = (args or {}).get(COMPUTER_ARG)
    return isinstance(computer, str) and bool(computer.strip())


class ComputerUnreachable(Exception):
    """The named computer cannot take the call; the message is for the agent."""


@dataclass(frozen=True)
class ComputerTarget:
    """Another of the user's computers, as one call reaches it."""

    computer_id: str
    name: str
    #: That computer's session, which its tools are built over.
    session: Session
    #: The computer's root, with its workspace folders as the siblings, named
    #: by the machine's own paths.
    project: ProjectContext
    #: Where its workspace folders sit, and where a call on it starts.
    root: str

    @property
    def sandbox(self) -> PTCSandbox:
        """The session's sandbox, which a reach hands back only once it is up."""
        sandbox = self.session.sandbox
        assert sandbox is not None, f"computer {self.computer_id} was reached with no sandbox"
        return sandbox


@dataclass(frozen=True)
class Attached:
    """A file a send names on one of the user's computers, as the messaging
    service fetches it."""

    #: Its path inside the folder of ``workspace_id``, or of Home.
    path: str
    #: The workspace whose folder holds it; None for a copy made in Home.
    workspace_id: str | None
    #: The bytes copied into Home, 0 for a file sent from its own folder.
    copied: int = 0


class ComputerReach(Protocol):
    """Resolves the ``computer`` a call names. The host implements it,
    since the user's computers and their sessions are its own."""

    async def reach(self, computer: str) -> ComputerTarget | None:
        """The computer ``computer`` names, awake and ready for a call, or
        None when it is the turn's own. Raises ``ComputerUnreachable``."""

    async def in_use(self, target: ComputerTarget) -> None:
        """Count a call still running on ``target`` as use of it: the idle
        sweep counts turns on a computer's own workspaces, not this one."""

    async def elsewhere(self) -> Sequence[tuple[str, str]]:
        """The user's other computers that hold workspaces, as (name, root).
        Read from the server, so it wakes none of them."""

    async def attach(self, files: Sequence[tuple[str, str]]) -> Sequence[Attached]:
        """The files a send names, each as (computer, its whole path there),
        as the messaging service fetches them, in order. All are found before
        any is copied, so a file that cannot go leaves no copy of the others.
        Raises ``ComputerUnreachable``."""


ToolsOn = Callable[[ComputerTarget], Mapping[str, BaseTool]]


class ComputerTools:
    """The tools of one build, per computer it has reached.

    Built at a computer's first call and rebuilt when its session changes
    under the turn (a restart), since each tool closes over the session it
    was built with. ``sandbox`` is the turn's own, whose root a search
    covering every workspace there names.
    """

    def __init__(self, reach: ComputerReach, build: ToolsOn, sandbox: PTCSandbox) -> None:
        self._reach = reach
        self._build = build
        self._sandbox = sandbox
        self._built: dict[str, tuple[Session, Mapping[str, BaseTool]]] = {}

    def on(self, target: ComputerTarget) -> Mapping[str, BaseTool]:
        built = self._built.get(target.computer_id)
        if built is None or built[0] is not target.session:
            built = (target.session, self._build(target))
            self._built[target.computer_id] = built
        return built[1]

    def wrap(self, tool: BaseTool) -> BaseTool:
        """``tool`` with a ``computer`` argument that sends a call elsewhere."""
        return _with_computer(tool, self, self._reach)

    def covers_root(self, tool_name: str, kwargs: Mapping[str, Any]) -> bool:
        """Whether a call on the turn's own computer searched its root, so
        every workspace there and none of another computer's."""
        if tool_name not in _SEARCHES:
            return False
        path = kwargs.get(_PATH_ARGS[tool_name])
        if not isinstance(path, str) or not path.strip():
            return False
        root = self._sandbox.working_dir.rstrip("/")
        try:
            searched = self._sandbox.normalize_path(path.strip()).rstrip("/")
        except Exception:  # noqa: BLE001 - the hint is never worth a failed call
            return False
        return bool(root) and searched == root


def _refusal(tool: BaseTool, message: str) -> Any:
    """``message`` as the tool's own error result."""
    text = f"ERROR: {message}"
    if tool.response_format == "content_and_artifact":
        return text, {"mcp_trace": []}
    return text


def _with_computer(tool: BaseTool, tools: ComputerTools, reach: ComputerReach) -> BaseTool:
    schema = create_model(
        f"{tool.name}WithComputer",
        __base__=tool.args_schema,
        **{COMPUTER_ARG: (str | None, Field(default=None, description=COMPUTER_FIELD))},
    )
    inner = tool.coroutine
    assert inner is not None, f"{tool.name} has no coroutine to route"

    async def here(kwargs: dict[str, Any], config: RunnableConfig) -> Any:
        result = await _run(tool, kwargs, config)
        if isinstance(result, str) and tools.covers_root(tool.name, kwargs):
            if note := await _unsearched(reach):
                return f"{result}\n\n{note}"
        return result

    async def call(*, config: RunnableConfig, **kwargs: Any) -> Any:
        computer = (kwargs.pop(COMPUTER_ARG, None) or "").strip()
        if not computer:
            return await here(kwargs, config)
        try:
            target = await reach.reach(computer)
        except ComputerUnreachable as e:
            return _refusal(tool, str(e))
        if target is None:
            return await here(kwargs, config)
        if tool.name == "ExecuteCode" and _imports_data_tools(kwargs.get("code") or ""):
            return _refusal(tool, _DATA_TOOLS_ELSEWHERE)
        if refused := file_refusal(target, tool.name, kwargs):
            return _refusal(tool, refused)
        there = tools.on(target)[tool.name]
        # The tool's metadata names the turn's own computer.
        stamp_tool_provider(target.sandbox.config.sandbox.provider)
        stamp_tool_channel(target.sandbox.channel)
        with project_bound(target.project):
            return await _while_in_use(reach, target, _run(there, kwargs, config))

    return StructuredTool(
        name=tool.name,
        description=tool.description,
        args_schema=schema,
        coroutine=call,
        response_format=tool.response_format,
        metadata=tool.metadata,
    )


def file_refusal(target: ComputerTarget, tool_name: str, kwargs: Mapping[str, Any]) -> str | None:
    """Why a file tool's path cannot be used on ``target``, said with what
    the agent needs to fix it, or None. The tool would refuse most of these
    itself, but without the computer's root, which the agent may not have."""
    arg = _PATH_ARGS.get(tool_name)
    path = kwargs.get(arg) if arg else None
    if not isinstance(path, str) or not path.strip():
        return None
    path = path.strip()
    sandbox = target.sandbox
    where = f'computer "{target.name}"'
    home = target.project.machine.home if target.project.machine is not None else None
    if path.startswith("~") and not (home and (path == "~" or path.startswith("~/"))):
        whole = "Write the path whole." if home else f"Write the path whole, under its root `{target.root}`."
        return f"`{path.split('/', 1)[0]}` is not expanded on {where}. {whole}"
    whole = sandbox.normalize_path(path, target.project)
    # Served by the server for every computer alike, at the root and in every
    # workspace folder: a call that reaches them through another computer gets
    # that machine's files under the same name, which the server never reads,
    # and where no mount serves there, a save lands on plain disk.
    if any(whole == m or whole.startswith(f"{m}/") for m in _MOUNTED):
        return _SERVER_KEPT_ELSEWHERE
    if on_file_mount(whole, SandboxLayout.for_root(target.root).for_workspace()):
        relative = posixpath.relpath(whole, target.root)
        own = relative == THREAD_INDEX or any(
            f"/{d}/" in f"/{relative}/" for d in _WORKSPACE_OWN
        )
        return _WORKSPACE_KEPT_ELSEWHERE if own else _SERVER_KEPT_ELSEWHERE
    if not sandbox.validate_path(path, target.project):
        if home:
            return f"The file tools do not reach {path} on {where}."
        return (
            f"{path} is outside what the file tools reach on {where}: "
            f"its root `{target.root}` and `/tmp`."
        )
    return None


async def _unsearched(reach: ComputerReach) -> str | None:
    """What a search of the turn's own computer left out: the workspaces on
    the user's other computers, which a model finding nothing here otherwise
    takes for nowhere."""
    try:
        others = await reach.elsewhere()
    except Exception as e:  # noqa: BLE001 - the search itself answered
        logger.debug("could not list the other computers", error=str(e))
        return None
    if not others:
        return None
    named = " and ".join(f'"{name}" (root `{root}`)' for name, root in others)
    return (
        f"This searched only your own computer. The workspaces on {named} were "
        "not searched: pass that computer as `computer` to search them."
    )


async def _while_in_use(reach: ComputerReach, target: ComputerTarget, work: Awaitable[T]) -> T:
    """``work``, with ``target`` counted as in use until it ends, so the idle
    sweep does not stop the computer under a call still running there."""

    async def beat() -> None:
        while True:
            await asyncio.sleep(_IN_USE_EVERY_S)
            try:
                await reach.in_use(target)
            except Exception as e:  # noqa: BLE001 - the call goes on
                logger.debug("could not count the computer as in use", error=str(e))

    beating = asyncio.create_task(beat())
    try:
        return await work
    finally:
        beating.cancel()


async def _run(tool: BaseTool, kwargs: dict[str, Any], config: RunnableConfig) -> Any:
    coroutine = tool.coroutine
    assert coroutine is not None
    if name := _get_runnable_config_param(coroutine):
        return await coroutine(**kwargs, **{name: config})
    return await coroutine(**kwargs)
