"""A main turn's waits for one of its computer's Claude Code CLIs.

The slot its CLI starts in tells it (``CliQueue``); it streams each wait as
it starts and ends, and keeps the ones long enough to note as ``ui`` marks
for the step that commits next, so replay shows them where they happened.
A subagent's launcher is given none: its wait shows in its own task.
"""

from __future__ import annotations

from typing import Any

from ptc_agent.agent.claude_code.signals import emit
from src.llms.claude_code import NOTED_WAIT_S, CliWait


class CliWaits:
    def __init__(self) -> None:
        self._wait: CliWait | None = None
        self._marks: list[dict[str, Any]] = []
        #: The last message before the CLIs the turn starts now, which a wait
        #: is placed after on replay.
        self.after: str | None = None

    def waiting(self, wait: CliWait) -> None:
        self._wait = wait
        emit(wait.event("waiting"))

    def admitted(self, waited_s: float) -> None:
        if self._wait is None:
            return
        wait, self._wait = self._wait, None
        waited = round(waited_s)
        if waited_s < NOTED_WAIT_S:
            emit(wait.event("cleared", waited))
            return
        self._marks.append(wait.mark(waited, self.after))
        emit(wait.event("admitted", waited))

    def gave_up(self, waited_s: float) -> None:
        """A wait that ended with no slot: the line goes, and the turn's own
        failure, if any, says why."""
        if self._wait is not None:
            wait, self._wait = self._wait, None
            emit(wait.event("cleared", round(waited_s)))

    def drain(self) -> list[dict[str, Any]]:
        marks, self._marks = self._marks, []
        return marks
