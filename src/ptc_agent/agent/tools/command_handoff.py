"""A Bash or ExecuteCode call's job, from its launch to what the call returns.

``run_command`` drives both tools. The main agent's call waits on its job for
a while; one still running then is handed off: its row is written to the
command ledger, which the watcher and the report-back read from then on, and
the tool returns at once with what the agent is told here. Nothing is stopped
at hand-off. A subagent's call waits until its job ends instead
(``hand_off=None`` on the tool factories), up to its own clock: it already
runs in the background, so its wait blocks nobody, and its own result covers
the command.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable, Mapping
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import structlog

from ptc_agent.agent.tools.utils import mark_sandbox_failed, note_sandbox_exit
from ptc_agent.core.sandbox.jobs import Ended, Failed, JobOutcome, JobRef, Running
from src.utils.durations import format_duration

logger = structlog.get_logger(__name__)

#: How long ExecuteCode waits before it hands a run off, and Bash by default.
#: A subagent's call waits at most this long first too, holding its slot.
HANDOFF_WAIT_S = 300
# Before the turn is asked to finish, a wait is cut to the ask only when this
# much is left: a shorter one would hand off at once for nothing.
_ASK_CUT_MIN_S = 60
# After it, a hand-off leaves this much before the tools stop (or half of
# what is left), for the agent to act on the result.
_ACT_MARGIN_S = 60

# Stops of jobs whose row a cancelled call was still writing, held so they
# finish after the call is gone. Execution context only: nothing reads it.
_after_cancel: set[asyncio.Task[None]] = set()

_STOPPED_AFTER_THE_HOUR = "It is stopped an hour after your turn ends."
_WAKEUP_CHECK = (
    "It is stopped an hour after your turn ends. If it needs longer, schedule a "
    "wakeup to check on it before then: it keeps running until an hour after "
    "each check, and each check can schedule the next."
)

#: Renders a job that ended, or a call that failed, as the tool returns it.
Render = Callable[[Ended | Failed], Awaitable[tuple[str, dict[str, Any]]]]
#: Starts the tool's job with the wait it is given.
Launch = Callable[[float], Awaitable[JobOutcome]]


@dataclass(frozen=True)
class HandOff:
    """What a main-agent tool needs to hand a command to its thread."""

    thread_id: str
    user_id: str
    workspace_id: str
    #: Whether the user has wakeups, which decides what the agent is told
    #: about outliving its turn.
    wakeups: bool
    #: The computer the command runs on, when it is not the turn's own. The
    #: log is there, so the row names it beside the log for every later
    #: mention (the notice, ThreadStatus) to read, by its id too, which a
    #: rename meanwhile leaves reaching it.
    computer: str | None = None
    computer_id: str | None = None

    def log_path(self, ref: JobRef) -> str:
        if self.computer is None:
            return ref.log_relpath
        held = f" (computer_id {self.computer_id})" if self.computer_id else ""
        return f'{ref.log_relpath} on computer "{self.computer}"{held}'


def log_of(ref: JobRef, hand_off: HandOff | None) -> str:
    """Where a call says its job's log is: on another computer, named, since
    the path alone reads from the turn's own."""
    return hand_off.log_path(ref) if hand_off is not None else ref.log_relpath


@dataclass(frozen=True)
class CommandCall:
    """The tool call a command runs for, as its row records it."""

    tool: str
    command: str
    description: str | None
    config: Mapping[str, Any] | None
    tool_call_id: str | None = None
    # The machine's session, read when the row is written: a long turn's
    # session can be rebound after the tool is built.
    session: Any = None

    def computer_id(self) -> str | None:
        computer_id = getattr(self.session, "computer_id", None)
        return str(computer_id) if computer_id else None


def run_id_of(config: Mapping[str, Any] | None) -> str | None:
    """The turn's run id inside a tool call: a tool's config keeps it only in
    its metadata, since the top-level key is dropped for child runs."""
    if not config:
        return None
    run_id = (config.get("metadata") or {}).get("run_id") or config.get("run_id")
    return str(run_id) if run_id else None


def wait_left(config: Mapping[str, Any] | None) -> float | None:
    """Seconds a wait may take on the caller's own clock, up to its tools
    stop; None when it has no clock.

    A subagent's clock is checked between its events, and a call waiting on
    a command makes none, so the wait itself has to stop at it. It stops at
    the tools stop rather than the end, leaving the run the time between the
    two for the answer its tools stopping asks for.
    """
    from ptc_agent.agent.middleware.runtime_context.turn import run_clock

    clock = run_clock((config or {}).get("configurable"))
    if clock is None:
        return None
    deadline, _, stop_before = clock
    return max((deadline - stop_before - datetime.now(UTC)).total_seconds(), 1.0)


def main_wait(wait_s: float, config: Mapping[str, Any] | None) -> float:
    """How long the main agent's call waits: ``wait_s``, cut so the hand-off
    lands before the turn is asked to finish.

    The turn's clock is checked between events, and a waiting call makes
    none, so a wait past the ask would spend the time the agent was left to
    answer in. Handing off by the ask leaves it its tools (to schedule a
    wakeup that checks on the command) and its answer. A call made after the
    ask, or too close to it for a wait worth having, hands off ahead of the
    tools stop by enough to still act on the result, since a wakeup set
    after the stop is refused.
    """
    from ptc_agent.agent.middleware.runtime_context.turn import run_clock

    clock = run_clock((config or {}).get("configurable"))
    if clock is None:
        return wait_s
    deadline, ask_before, stop_before = clock
    now = datetime.now(UTC)
    ask_at = (deadline - ask_before - now).total_seconds()
    stop_at = (deadline - stop_before - now).total_seconds()
    if ask_at >= _ASK_CUT_MIN_S:
        by = ask_at
    else:
        by = max(ask_at, stop_at - min(_ACT_MARGIN_S, stop_at / 2))
    return max(min(wait_s, by), 1.0)


def _first_wait(
    config: Mapping[str, Any] | None,
    hand_off: HandOff | None,
    wait_s: float,
    background: bool,
) -> tuple[float, bool]:
    """The launch's wait, and whether a job still running after it is waited
    on to the call's clock (when it is not handed off)."""
    if hand_off is not None:
        return (0 if background else main_wait(wait_s, config)), True
    left = wait_left(config)
    if left is None or left > HANDOFF_WAIT_S:
        return HANDOFF_WAIT_S, True
    return left, False


async def run_command(
    backend: Any,
    call: CommandCall,
    launch: Launch,
    render: Render,
    *,
    hand_off: HandOff | None,
    first_wait: float = HANDOFF_WAIT_S,
    background: bool = False,
    slot: Callable[[], AbstractAsyncContextManager[Any]] | None = None,
) -> tuple[str, dict[str, Any]]:
    """Launch a command, wait on it, and return what the call answers.

    The main agent's call waits ``first_wait``, cut to its turn's clock
    (``main_wait``), or not at all for ``background``, then hands a job
    still running to its thread. A subagent's call waits at most
    ``HANDOFF_WAIT_S`` first too. ``slot`` is held for that first wait
    alone, so a long run does not hold up every other run on the computer,
    and the wait is read once it is held: time queued for it comes off the
    wait. A job not handed off is waited on to the call's clock and stopped
    there, or stopped at its first wait when nothing times the call. Until
    the ledger owns a job, a cancel stops it, since nothing else would ever
    end it.
    """
    held = _Held(backend)
    try:
        async with slot() if slot is not None else contextlib.nullcontext():
            wait_s, waits_on = _first_wait(call.config, hand_off, first_wait, background)
            # Held before the slot is let go, whose release can be cancelled.
            outcome = held.hold(await launch(wait_s))
        if not isinstance(outcome, Running):
            return await render(outcome)
        if hand_off is not None:
            if await held.hand_over(call, hand_off):
                log = hand_off.log_path(outcome.ref)
                text = (
                    background_result(outcome, wakeups=hand_off.wakeups, log=log)
                    if background
                    else handoff_result(
                        outcome, waited_s=wait_s, wakeups=hand_off.wakeups, log=log
                    )
                )
                return text, {"mcp_trace": [], "command_id": outcome.ref.command_id}
            if background:
                # Unrecorded, nothing would report it or stop it after the turn.
                await held.stop()
                return not_handed_off_result(), {"mcp_trace": []}
        left = wait_left(call.config) if waits_on else None
        if left is not None:
            # The call keeps the command it started, until its tools stop:
            # past that the turn could not end. With no clock (an agent built
            # outside a turn, or a turn with no limit) its first wait was its
            # bound, as its timeout was before commands ran as jobs.
            outcome = held.hold(await backend.continue_job(outcome, left))
            if not isinstance(outcome, Running):
                return await render(outcome)
        return await stop_at_deadline(backend, outcome, render, log_of(outcome.ref, hand_off))
    except asyncio.CancelledError:
        await held.release()
        raise


class _Held:
    """The job a call holds, until it ends or the ledger owns it."""

    def __init__(self, backend: Any) -> None:
        self._backend = backend
        self.running: Running | None = None
        self._write: asyncio.Future[bool] | None = None

    def hold(self, outcome: JobOutcome) -> JobOutcome:
        self.running = outcome if isinstance(outcome, Running) else None
        return outcome

    def _stop(self, running: Running) -> Awaitable[Any]:
        return self._backend.stop_job(running.ref, discard=True)

    async def hand_over(self, call: CommandCall, hand_off: HandOff) -> bool:
        """Write the held job's row; whether it was written.

        False when the call has no computer to name, or the write failed and
        no row is there to read. A cancel while the write is in flight leaves
        the write to finish (``release``).
        """
        running = self.running
        assert running is not None
        computer_id = call.computer_id()
        if computer_id is None:
            logger.warning(
                "Command not handed off: no computer on the session",
                command_id=running.ref.command_id,
            )
            return False
        self._write = asyncio.ensure_future(_write_row(hand_off, call, running, computer_id))
        recorded = await asyncio.shield(self._write)
        self._write = None
        if recorded:
            self.running = None
        return recorded

    async def stop(self) -> None:
        """Stop the held job now; a cancel meanwhile does not cut the stop."""
        running, self.running = self.running, None
        assert running is not None
        await asyncio.shield(self._stop(running))

    async def release(self) -> None:
        """On a cancel: stop the held job unless the ledger owns it. A row
        still being written decides after the call is gone: a job with no row
        has nothing to follow it, and one with a row is the watcher's."""
        running, self.running = self.running, None
        if running is None:
            return
        if self._write is not None:
            task = asyncio.ensure_future(
                _stop_unless_recorded(self._write, lambda: self._stop(running), running)
            )
            _after_cancel.add(task)
            task.add_done_callback(_after_cancel.discard)
            return
        try:
            await asyncio.shield(self._stop(running))
        except Exception as e:  # noqa: BLE001 - a later launch prunes its state
            logger.warning(
                "Could not stop a job whose call was cancelled",
                command_id=running.ref.command_id,
                error=str(e),
            )


async def _stop_unless_recorded(
    write: asyncio.Future[bool], stop: Callable[[], Awaitable[Any]], running: Running
) -> None:
    try:
        # Shielded: a cancel of this task must not cut the write it decides on.
        if await asyncio.shield(write):
            return
        await stop()
        logger.info(
            "Stopped a job whose call was cancelled before its row was written",
            command_id=running.ref.command_id,
        )
    except Exception as e:  # noqa: BLE001 - a later launch prunes its state
        logger.warning(
            "Could not stop a job whose call was cancelled before its row was written",
            command_id=running.ref.command_id,
            error=str(e),
        )


async def _write_row(
    hand_off: HandOff, call: CommandCall, running: Running, computer_id: str
) -> bool:
    ref = running.ref
    try:
        # Lazy: a build with no server behind it never hands off.
        from src.server.database.runs.command_runs import record_handoff

        await record_handoff(
            command_id=ref.command_id,
            thread_id=hand_off.thread_id,
            user_id=hand_off.user_id,
            workspace_id=hand_off.workspace_id,
            computer_id=computer_id,
            run_id=run_id_of(call.config),
            tool_call_id=call.tool_call_id,
            tool=call.tool,
            description=call.description or "",
            command_preview=call.command,
            log_path=hand_off.log_path(ref),
            job_ref=ref.to_dict(),
            started_at=running.handle.started_at,
            # A job still running has harvested nothing: this is what an
            # auto-install's earlier attempts called, which finalize keeps.
            mcp_trace=list(running.handle.carried) or None,
        )
    except Exception as e:
        # The pool commits each statement, so an error after the commit (a
        # lost reply) leaves a row that only a read can see.
        if await _row_exists(hand_off.thread_id, ref.command_id):
            logger.warning(
                "Command hand-off reported an error after its row was written",
                command_id=ref.command_id,
                error=str(e),
            )
            return True
        logger.error(
            "Command hand-off could not be recorded",
            command_id=ref.command_id,
            error=str(e),
            exc_info=True,
        )
        return False
    return True


async def _row_exists(thread_id: str, command_id: str) -> bool:
    """Whether the command's row is there; False when the read fails too."""
    try:
        from src.server.database.runs.command_runs import get_thread_command

        return await get_thread_command(thread_id, command_id) is not None
    except Exception:  # noqa: BLE001 - unknown reads as unrecorded
        return False


async def stop_at_deadline(
    backend: Any, outcome: Running, render: Render, log: str
) -> tuple[str, dict[str, Any]]:
    """Stop a job the call's clock ran out on, and what the call returns.

    The MCP calls ride the result as an ended job's do, read by the stop's
    own exec: a wait would reap the stopped job and move the log the result
    names. A job that ended between the call's last wait and the stop is
    read, and returned as ``render`` returns any ended job; a read that
    fails leaves its log for the deadline result to name.
    """
    stopped = await backend.stop_job_with_trace(outcome)
    if stopped.had_ended:
        try:
            done = await backend.read_ended_job(outcome)
        except Exception as e:  # noqa: BLE001 - the deadline result still holds
            logger.warning(
                "Could not read a job that ended at its call's deadline",
                command_id=outcome.ref.command_id,
                error=str(e),
            )
        else:
            if not isinstance(done, Running):
                return await render(done)
    return deadline_result(outcome, log), {"mcp_trace": list(stopped.mcp_trace)}


async def settle(backend: Any, outcome: Ended | Failed, hand_off: HandOff | None) -> str:
    """The output a call shows for its job, after the run's accounting.

    A call the sandbox failed marks the run failed. A nonzero exit is
    offered to the computer's usage run, which counts an OOM kill, unless the
    wrapper was killed with no exit written: nobody saw that one OOM-killed,
    as the watcher's finalize also holds. Output cut to its end leads with
    where the whole log is.
    """
    match outcome:
        case Failed():
            mark_sandbox_failed(outcome.failure)
            return outcome.message
        case Ended(exit_code=code, killed=killed):
            if code != 0 and not killed:
                await note_sandbox_exit(backend, code, None)
            if outcome.truncated:
                return truncated_note(log_of(outcome.ref, hand_off)) + outcome.output
            return outcome.output


def _ran_for(seconds: float) -> str:
    return format_duration(round(seconds))


def handoff_result(outcome: Running, *, waited_s: float, wakeups: bool, log: str) -> str:
    """What a call that waited and handed its job off returns.

    ``waited_s`` is the wait the call asked for, not the job's age: the
    launch's own round trip, or an auto-install rerun that started the clock
    over, would otherwise read as a wait nobody chose ("21s" for 20).
    """
    middle = _WAKEUP_CHECK if wakeups else _STOPPED_AFTER_THE_HOUR
    return (
        f"Still running after {format_duration(waited_s)}, so it moved to the "
        f"background as command_id {outcome.ref.command_id}. Its output goes to "
        f"{log} as it runs, and you will be notified when it "
        "finishes. To stop it early, use ThreadStatus with stop.\n\n"
        f"{middle}\n\n"
        "Meanwhile do other work, or tell the user it is running and end your turn."
    )


def background_result(outcome: Running, *, wakeups: bool, log: str) -> str:
    """What ``run_in_background`` returns at launch."""
    middle = _WAKEUP_CHECK if wakeups else _STOPPED_AFTER_THE_HOUR
    return (
        f"Started in the background as command_id {outcome.ref.command_id}. Its "
        f"output goes to {log} as it runs, and you will be "
        "notified when it finishes. To stop it early, use ThreadStatus with stop.\n\n"
        f"{middle}"
    )


def deadline_result(outcome: Running, log: str) -> str:
    """What a call returns when its own clock ran out first."""
    return (
        f"ERROR: Stopped after {_ran_for(outcome.handle.ran_for())}: your time limit ran "
        f"out before it ended. What it wrote is in {log}."
    )


def not_handed_off_result() -> str:
    """What ``run_in_background`` returns when the command could not be
    recorded, and so was stopped: nothing would report it or stop it later."""
    return (
        "ERROR: The command could not be moved to the background, so it was "
        "stopped. Run it again, or without run_in_background."
    )


def truncated_note(log: str) -> str:
    """The line ahead of output cut to its end, naming the whole log."""
    return f"(The start of the output was cut; the whole log is {log}.)\n"
