"""A thread's background commands as the web reads them: tool cards and the tray chip.

A tool card finds its command by ``tool_call_id``, the call that started it.
Copy names a command by its ``description``, never its id.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any, List, Literal, Optional

from pydantic import BaseModel, Field, field_validator, model_validator

CommandStatus = Literal["running", "completed", "failed", "stopped"]

#: Why a failed command failed: a non-zero exit, its computer stopped under
#: it, it ended with no exit recorded while its computer ran (killed from
#: outside), or it was stopped an hour after its turn ended.
FailureReason = Literal["exit", "lost", "vanished", "after_turn_limit"]

#: Who asked a running command to stop, as ``stopped_by`` records it: the
#: agent, the user, its thread archived or deleted, its workspace deleted, or
#: the after-turn hour. A stopped command keeps the mark; one the hour stopped
#: ends ``failed`` and loses it.
StopMark = Literal["agent", "user", "archive", "delete", "workspace", "after_turn_limit"]


class CommandOut(BaseModel):
    command_id: str
    tool: Literal["Bash", "ExecuteCode"]
    description: Optional[str] = None
    status: CommandStatus
    exit_code: Optional[int] = None
    failure_reason: Optional[FailureReason] = None
    tool_call_id: Optional[str] = None
    log_path: str
    started_at: datetime
    handed_off_at: datetime
    ended_at: Optional[datetime] = None
    # A stop is under way that the server finishes on its own, as when it
    # could not reach the computer yet: the row is marked and still running.
    stopping: bool = False

    @field_validator("description", mode="before")
    @classmethod
    def _no_blank_description(cls, value: Optional[str]) -> Optional[str]:
        # The row stores no description as "": copy reads a missing one.
        return value or None

    @model_validator(mode="before")
    @classmethod
    def _stopping_from_mark(cls, row: Any) -> Any:
        # A running row's ``stopped_by`` is that stop's mark; the mark itself
        # stays on the server.
        if isinstance(row, Mapping) and "stopping" not in row:
            marked = row.get("status") == "running" and row.get("stopped_by") is not None
            return {**row, "stopping": marked}
        return row


class CommandsResponse(BaseModel):
    thread_id: str
    commands: List[CommandOut] = Field(default_factory=list)


class CommandStopResponse(BaseModel):
    # Whether this stop ended it. False for one that had already ended or
    # been stopped, or whose computer the stop could not reach in time, which
    # the server then finishes on its own.
    stopped: bool
    # The row after the stop.
    command: Optional[CommandOut] = None
