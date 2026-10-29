"""`ThreadStatus`: what is running or set to report back to a conversation.

The agent's copy of the composer tray, read from the same records: the
command ledger, the task ledger, the thread's wakeups, the automations that
run in it, and on a Chief of Staff turn the analysts it handed work to. All of
it is read from the server, so a look never wakes a computer. Main agent only:
a subagent has no thread of its own for anything to come back to.

Showing a command's end is its report: the notice it would have sent is
dropped (``stamp_delivered``), so a look never opens a turn about something
the agent has already seen. The commands a call stamps ride its artifact
(``REPORTED_COMMANDS_KEY``), from which the provenance middleware emits their
MCP sources, as the notice would have brought them.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import re
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, ClassVar, NamedTuple, Protocol

from langchain_core.runnables import RunnableConfig
from langchain_core.tools import BaseTool, StructuredTool

from ptc_agent.agent.limits import pending_read
from ptc_agent.agent.middleware.provenance.command_trace import REPORTED_COMMANDS_KEY
from ptc_agent.core.paths import SandboxLayout
from src.server.database import automation
from src.server.database.runs import command_runs, subagent_runs
from src.server.database.wakeups import read as wakeups_db
from src.server.services.commands import control
from src.server.services.report_back.handoff import status as handoff_status
from src.tools.thread_status import listing
from src.tools.thread_status.listing import (
    RECENT,
    TYPES,
    Item,
    ItemStatus,
    ItemType,
)

if TYPE_CHECKING:
    from ptc_agent.agent.middleware.background_subagent.registry import (
        BackgroundTaskRegistry,
    )

logger = logging.getLogger(__name__)

TOOL_NAME = "ThreadStatus"

THREAD_STATUS_DOC = """\
What is running or set to report back to this conversation: background commands and code runs, subagent tasks, wakeups, automations that run here, and analysts you handed work to.

Use it before waiting on, restarting, or answering a question about work already under way.

Args:
    type: Only this type: command (Bash and ExecuteCode), task, wakeup, automation or analyst.
    status: Only this status: running, pending (set to report back later), completed, failed or stopped.
    id: One item's id as the listing shows it (bg-…, Task-…, w_…, or an automation's file name), for its detail.
    stop: A background command's id to stop it. A stopped command does not report back.

Omit type, status and id for the full view.

Returns:
    One line per item with its id, status and timing: running, then pending, then up to 10 that completed, failed or stopped in the last 24 hours. A command's line names its log file.
"""

#: What a task id minted by the registry looks like (``token_urlsafe``).
_BARE_TASK_ID = re.compile(r"[A-Za-z0-9_-]{6}")

# The wakeups that ended in the listing's window, at most.
_ENDED_WAKEUPS = 200


def build_thread_status_tool(
    *,
    thread_id: str,
    user_id: str | None,
    analysts: bool = False,
    registry: BackgroundTaskRegistry | None = None,
) -> BaseTool:
    """The tool for one turn on ``thread_id``. ``analysts`` lists the Chief of
    Staff's hand-offs; ``registry`` gives a task this worker runs its progress."""

    async def thread_status(
        type: ItemType | None = None,
        status: ItemStatus | None = None,
        id: str | None = None,
        stop: str | None = None,
        *,
        config: RunnableConfig,
    ) -> tuple[str, dict[str, Any] | None]:
        configurable = (config or {}).get("configurable") or {}
        reader = _Reader(
            thread_id=thread_id,
            user_id=user_id,
            user_tz=str(configurable.get("timezone") or "UTC"),
            registry=registry,
        )
        if stop:
            return reader.answer(*await reader.stop(stop.strip()))
        # In the order an id is looked for: a bare task id can also be a
        # wakeup's short id or a file name, so tasks come first, and any id
        # can be an automation's file name, so automations come last.
        sources: list[Source] = [
            _Commands(reader),
            _Tasks(reader),
            _Wakeups(reader),
            *([_Analysts(reader)] if analysts else []),
            _Automations(reader),
        ]
        if id:
            return reader.answer(*await reader.detail(sources, id.strip()))
        return reader.answer(*await reader.listing(sources, type, status))

    return StructuredTool.from_function(
        coroutine=thread_status,
        name=TOOL_NAME,
        description=inspect.cleandoc(THREAD_STATUS_DOC),
        response_format="content_and_artifact",
    )


class Shown(NamedTuple):
    """One item's detail, and its status (None for an automation the listing
    would leave out), which marks a look at running work as a poll."""

    text: str
    status: ItemStatus | None


class Source(Protocol):
    """One type of item: its lines, the ids that may name one, and one detail."""

    type: ClassVar[ItemType]

    async def items(self) -> list[Item]: ...

    def claims(self, item_id: str) -> bool: ...

    def owns(self, item_id: str) -> bool:
        """Whether the id is in this type's own form, which no other type's
        item takes: one missed here could only be an automation's file."""
        ...

    async def find(self, item_id: str) -> Shown | None: ...


class _Reader:
    """One call's reads, against one thread."""

    def __init__(
        self,
        *,
        thread_id: str,
        user_id: str | None,
        user_tz: str,
        registry: BackgroundTaskRegistry | None,
    ) -> None:
        self.thread_id = thread_id
        self.user_id = user_id
        self.user_tz = user_tz
        self.registry = registry
        self.now = datetime.now(timezone.utc)
        # The commands this call stamped delivered, in the order shown.
        self.stamped: list[str] = []

    def answer(
        self, text: str, artifact: dict[str, Any] | None = None
    ) -> tuple[str, dict[str, Any] | None]:
        """The call's result, naming the commands it reported on."""
        if not self.stamped:
            return text, artifact
        return text, {**(artifact or {}), REPORTED_COMMANDS_KEY: list(self.stamped)}

    async def saw_end(self, command_ids: Sequence[str]) -> None:
        """Drop the notices of the commands whose end the agent now sees; the
        ledger stamps only those that report and are not yet delivered. Only
        this call's stamps ride its artifact: a notice that got there first
        brought the command's sources with it."""
        if not command_ids:
            return
        try:
            stamped = set(await command_runs.stamp_delivered(command_ids))
        except Exception:
            # The notices then arrive as well, which costs a turn, not a result.
            logger.warning(
                "[ThreadStatus] stamping %s delivered failed",
                ", ".join(command_ids),
                exc_info=True,
            )
            return
        self.stamped.extend(c for c in command_ids if c in stamped)

    async def progress(self, row: Mapping[str, Any]) -> str | None:
        """A running task's tool calls, when this worker runs it."""
        if self.registry is None or row.get("status") != "in_progress":
            return None
        live = await self.registry.live_progress(str(row["task_id"]))
        return listing.task_progress(*live) if live else None

    async def listing(
        self,
        sources: Sequence[Source],
        type_: ItemType | None,
        status: ItemStatus | None,
    ) -> tuple[str, dict[str, Any] | None]:
        picked = [s for s in sources if type_ is None or s.type == type_]
        if not picked:
            return "Analysts are listed on a Chief of Staff turn only.", None
        results = await asyncio.gather(*(s.items() for s in picked), return_exceptions=True)
        items: list[Item] = []
        unread: list[ItemType] = []
        for source, result in zip(picked, results):
            if isinstance(result, BaseException):
                logger.warning(
                    "[ThreadStatus] %s read failed thread_id=%s",
                    source.type,
                    self.thread_id,
                    exc_info=result,
                )
                unread.append(source.type)
            else:
                items.extend(result)
        shown = listing.render_listing(items, status=status)
        await self.saw_end([i.id for i in shown.shown_ended if i.type == "command"])
        text = shown.text
        if unread:
            text += "\n" + _could_not_read(unread)
        # A look at running work is a poll, which the loop guard tells from
        # progress by this mark: the text changes on every read.
        return text, pending_read() if shown.running else None

    async def detail(
        self, sources: Sequence[Source], item_id: str
    ) -> tuple[str, dict[str, Any] | None]:
        owned_missed = False
        for source in sources:
            if not source.claims(item_id):
                continue
            try:
                shown = await source.find(item_id)
            except Exception:
                logger.warning(
                    "[ThreadStatus] %s read failed thread_id=%s",
                    source.type,
                    self.thread_id,
                    exc_info=True,
                )
                if owned_missed:
                    # Its own type read it as not there; only a file named
                    # like it went unread.
                    return _unknown(item_id), None
                # As the listing says it: not knowing is not "no such item".
                return _could_not_read([source.type]), None
            if shown is not None:
                # A look at one running item is a poll too, as in the listing.
                return shown.text, pending_read() if shown.status == "running" else None
            owned_missed = owned_missed or source.owns(item_id)
        return _unknown(item_id), None

    async def stop(self, command_id: str) -> tuple[str, dict[str, Any] | None]:
        if not command_id.startswith("bg-"):
            return (
                f"ERROR: {command_id} is not a background command. stop takes a "
                "command's id (bg-…) only."
            ), None
        # Named on the result as a hand-off names its command, so a view whose
        # stream never saw this call (one reconnected since) still re-reads
        # the thread's commands.
        return await self._stop(command_id), {"command_id": command_id}

    async def _stop(self, command_id: str) -> str:
        try:
            result = await control.stop_thread_command(self.thread_id, command_id, by="agent")
        except Exception as e:
            logger.warning("[ThreadStatus] stop of %s failed", command_id, exc_info=True)
            return f"ERROR: Could not stop {command_id}, so it may still be running: {e}"
        row = result.row or {}
        wrote = f"What it wrote is in {row.get('log_path')}."
        match result.outcome:
            case "not_found":
                return _unknown(command_id)
            case "stopped":
                return f"Stopped {command_id}. It does not report back. {wrote}"
            case "stopping" if row.get("stopped_by"):
                # Its computer did not answer in time; the mark on the row
                # has the watcher finish the stop.
                return (
                    f"Stopping {command_id}: its computer did not answer in time, so it "
                    f"ends within a minute. It does not report back. {wrote}"
                )
            case "stopping":
                return f"ERROR: Could not stop {command_id} just now; it is still running."
            case "ended":
                await self.saw_end([command_id])
                return (
                    f"{command_id} is not running: "
                    f"{listing.command_state(row, self.now)}. {wrote}"
                )


@dataclass(frozen=True)
class _Commands:
    r: _Reader
    type: ClassVar[ItemType] = "command"

    async def items(self) -> list[Item]:
        rows = await command_runs.list_for_thread(
            self.r.thread_id, since=self.r.now - RECENT, limit=None
        )
        return [listing.command_item(row, self.r.now) for row in rows]

    def claims(self, item_id: str) -> bool:
        return item_id.startswith("bg-")

    def owns(self, item_id: str) -> bool:
        return self.claims(item_id)

    async def find(self, item_id: str) -> Shown | None:
        row = await command_runs.get_thread_command(self.r.thread_id, item_id)
        if row is None:
            return None
        if row["status"] != "running":
            await self.r.saw_end([item_id])
        return Shown(listing.command_detail(row, self.r.now), row["status"])


@dataclass(frozen=True)
class _Tasks:
    r: _Reader
    type: ClassVar[ItemType] = "task"

    async def items(self) -> list[Item]:
        rows = await subagent_runs.list_reporting_tasks(
            self.r.thread_id, since=self.r.now - RECENT
        )
        return [listing.task_item(row, self.r.now, await self.r.progress(row)) for row in rows]

    def claims(self, item_id: str) -> bool:
        return _task_id(item_id) is not None

    def owns(self, item_id: str) -> bool:
        # A bare id may as well be a file's name.
        return self.claims(item_id) and item_id.startswith("Task-")

    async def find(self, item_id: str) -> Shown | None:
        rows = await subagent_runs.list_reporting_tasks(
            self.r.thread_id, task_id=_task_id(item_id)
        )
        if not rows:
            return None
        row = rows[0]
        detail = listing.task_detail(row, self.r.now, await self.r.progress(row))
        return Shown(detail, listing.task_status(row))


def _task_id(item_id: str) -> str | None:
    """The task an id names: ``Task-`` and its id, or a bare id, which minted
    ids being url-safe can hold _ or -."""
    if item_id.startswith("Task-") and len(item_id) > 6:
        return item_id.removeprefix("Task-")
    return item_id if _BARE_TASK_ID.fullmatch(item_id) else None


@dataclass(frozen=True)
class _Wakeups:
    r: _Reader
    type: ClassVar[ItemType] = "wakeup"

    async def items(self) -> list[Item]:
        rows = await wakeups_db.list_thread_wakeups(
            self.r.thread_id, ended_since=self.r.now - RECENT, ended_limit=_ENDED_WAKEUPS
        )
        items = [listing.wakeup_item(row, self.r.now, self.r.user_tz) for row in rows]
        # An ended one stays open while its notice is unread; the window
        # applies to when it ended.
        return [i for i in items if i.status == "pending" or self.r.now - i.at <= RECENT]

    def claims(self, item_id: str) -> bool:
        return item_id.startswith("w_")

    def owns(self, item_id: str) -> bool:
        return self.claims(item_id)

    async def find(self, item_id: str) -> Shown | None:
        row = await wakeups_db.get_thread_wakeup(self.r.thread_id, item_id)
        if row is None:
            return None
        item = listing.wakeup_item(row, self.r.now, self.r.user_tz)
        detail = listing.wakeup_detail(item, row, self.r.now, self.r.user_tz)
        return Shown(detail, item.status)


@dataclass(frozen=True)
class _Analysts:
    """The Chief of Staff's hand-offs still running, named by their threads."""

    r: _Reader
    type: ClassVar[ItemType] = "analyst"

    async def items(self) -> list[Item]:
        owed = await handoff_status.read_owed_analysts(self.r.thread_id, self.r.user_id)
        return [listing.analyst_item(label, self.r.now) for label in owed]

    def claims(self, item_id: str) -> bool:
        try:
            uuid.UUID(item_id)
        except ValueError:
            return False
        return True

    def owns(self, item_id: str) -> bool:
        return self.claims(item_id)

    async def find(self, item_id: str) -> Shown | None:
        owed = await handoff_status.read_owed_analysts(self.r.thread_id, self.r.user_id)
        wanted = item_id.lower()
        label = next((a for a in owed if str(a.get("thread_id")).lower() == wanted), None)
        return None if label is None else Shown(listing.analyst_detail(label), "running")


@dataclass(frozen=True)
class _Automations:
    """The user's automations that run in this thread, by file name."""

    r: _Reader
    type: ClassVar[ItemType] = "automation"

    def _here(self, row: Mapping[str, Any]) -> bool:
        return str(row.get("conversation_thread_id") or "") == self.r.thread_id

    async def items(self) -> list[Item]:
        if not self.r.user_id:
            return []
        rows = await automation.list_all_automations(self.r.user_id)
        return [
            item
            for row in rows
            if self._here(row)
            and (item := listing.automation_item(row, self.r.now)) is not None
        ]

    def claims(self, item_id: str) -> bool:
        return True

    def owns(self, item_id: str) -> bool:
        return False

    async def find(self, item_id: str) -> Shown | None:
        if not self.r.user_id:
            return None
        name = item_id.rsplit("/", 1)[-1]
        for file_name in (name, f"{name}.json"):
            row = await automation.get_automation_file(self.r.user_id, file_name)
            if row is not None and self._here(row):
                item = listing.automation_item(row, self.r.now)
                detail = listing.automation_detail(
                    row, self.r.now, SandboxLayout.AUTOMATIONS_DIR
                )
                return Shown(detail, item.status if item is not None else None)
        return None


def _could_not_read(types: Sequence[ItemType]) -> str:
    return f"Could not read {', '.join(f'{t}s' for t in TYPES if t in types)} just now."


def _unknown(item_id: str) -> str:
    return f"No {item_id} in this conversation. ThreadStatus lists the ids."


__all__ = ["THREAD_STATUS_DOC", "TOOL_NAME", "build_thread_status_tool"]
