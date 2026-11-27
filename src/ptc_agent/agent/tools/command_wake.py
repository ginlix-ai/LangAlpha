"""What ends a Bash or ExecuteCode call's wait early: something arriving for
the agent to read.

A call waiting on its command makes no events, so whatever reaches the agent
meanwhile (a message sent to it, a report owed it) waits behind the command:
until it ends, or until the main agent's call hands it off. A command that is
only waiting (``waiting``) is cut short by anything the agent's next model
call would take in, read with the same predicate the take uses, so a wait
never ends for something the take would leave. One that is working is cut
short only by a message its sender marked Send now. Either way the command
keeps running: what becomes of it is ``command_handoff``'s.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from functools import partial
from typing import Any, Protocol

from ptc_agent.agent.tools.command_shelf import current_shelf
from ptc_agent.core.sandbox.jobs import Arrival


class Wake(Protocol):
    """Where a tool's calls read whether something arrived."""

    def arrival(self, config: Mapping[str, Any] | None, *, waiting: bool) -> Arrival | None:
        """The arrival this call watches for: anything, for a command that is
        only ``waiting``, else a message marked Send now. None when nothing
        can arrive."""
        ...


@dataclass(frozen=True)
class ThreadWake:
    """The main agent's: what its turn's next model call takes in, as the
    steering middleware's ``kinds`` name it. The inbox is read off the
    registry at call time, which the server sets after the agent is built."""

    registry: Any
    thread_id: str
    kinds: Sequence[str]

    def arrival(self, config: Mapping[str, Any] | None, *, waiting: bool) -> Arrival | None:
        from ptc_agent.agent.tools.command_handoff import run_id_of

        arrived = getattr(getattr(self.registry, "report_inbox", None), "arrived", None)
        run_id = run_id_of(config)
        if arrived is None or not run_id:
            return None
        # With ``now_only`` the inbox reads Send now steers alone.
        return partial(arrived, self.thread_id, run_id, kinds=self.kinds, now_only=not waiting)


@dataclass(frozen=True)
class TaskWake:
    """A background subagent's: a message sent to its task. Armed only in a
    run with a shelf, which is where a command it cuts short goes."""

    registry: Any

    def arrival(self, config: Mapping[str, Any] | None, *, waiting: bool) -> Arrival | None:
        from ptc_agent.agent.middleware.background_subagent.context import (
            current_background_tool_call_id,
        )

        registry = self.registry
        arrived = getattr(getattr(registry, "report_inbox", None), "task_arrived", None)
        tool_call_id = current_background_tool_call_id.get()
        if arrived is None or not tool_call_id or current_shelf.get() is None:
            return None
        task = registry.get_by_tool_call_id(tool_call_id)
        task_id = getattr(task, "task_id", None)
        task_run_id = getattr(task, "task_run_id", None)
        if not task_id or not task_run_id:
            return None
        return partial(
            arrived, registry.thread_id or "", task_id, task_run_id, now_only=not waiting
        )
