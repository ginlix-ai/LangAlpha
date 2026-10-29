"""A background command's notice: what the agent reads when one ends, and what
the turn's caption shows of it. Prompt surface, approved on the design page:
a change here is a change to what the agent is told.

The notice names the log file and never carries the output: the agent reads
it with Read or tail.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any, Mapping, Sequence

from src.utils.durations import format_duration

if TYPE_CHECKING:
    from src.server.models.chat import CommandReport


def _ran_for(row: Mapping[str, Any]) -> str:
    started, ended = row.get("started_at"), row.get("ended_at")
    if not isinstance(started, datetime) or not isinstance(ended, datetime):
        return format_duration(0)
    return format_duration((ended - started).total_seconds())


# Signals numbered alike on Linux and macOS, the systems a computer runs;
# any other reads by its number.
_SIGNALS = {
    1: "SIGHUP", 2: "SIGINT", 3: "SIGQUIT", 4: "SIGILL", 5: "SIGTRAP", 6: "SIGABRT",
    8: "SIGFPE", 9: "SIGKILL", 11: "SIGSEGV", 13: "SIGPIPE", 14: "SIGALRM", 15: "SIGTERM",
}


def killed_by(exit_code: int | None) -> str | None:
    """The signal a shell's exit code past 128 says ended the command, or
    None for an ordinary exit. A best guess: a command can exit 143 itself."""
    if exit_code is None or not 128 < exit_code <= 128 + 64:
        return None
    number = exit_code - 128
    return _SIGNALS.get(number, f"signal {number}")


def _one_line(text: str) -> str:
    """Text the agent or the user wrote, fit to sit inside a ``<system>``
    block: one line, no angle brackets to close it early."""
    kept = "".join(c for c in text if c not in "<>" and (c.isprintable() or c.isspace()))
    return " ".join(kept.split())


def command_report(row: Mapping[str, Any]) -> "CommandReport":
    """What the turn's caption shows of the command, read off its row."""
    from src.server.models.chat import CommandReport

    return CommandReport(
        kind="command_report",
        command_id=str(row["command_id"]),
        tool=row["tool"],
        description=row.get("description") or None,
        status=row["status"],
        failure_reason=row.get("failure_reason"),
        tool_call_id=row.get("tool_call_id") or None,
    )


def wakeups_line(wakeups: Sequence[Mapping[str, Any]]) -> str | None:
    """The line naming the time wakeups still pending, or None for none."""
    named = []
    for w in wakeups:
        if not w.get("short_id"):
            continue
        label = _one_line(str(w.get("label") or ""))
        named.append(f"{w['short_id']} ({label})" if label else str(w["short_id"]))
    if not named:
        return None
    return (
        f"Wakeups still pending: {', '.join(named)}. "
        "Cancel any that only watched this command."
    )


def command_report_message(
    row: Mapping[str, Any],
    wakeups: Sequence[Mapping[str, Any]] = (),
    *,
    steered: bool = False,
) -> str:
    """The notice the agent reads when a command ends, as its own turn or as
    a steer into a live one.

    "while you were working" only when a live turn takes it in, and never
    for a command the after-turn limit stopped, which ended while no turn
    ran, or one the user stopped, which the user saw happen."""
    code_run = row.get("tool") == "ExecuteCode"
    noun = "code run" if code_run else "command"
    description = _one_line(str(row.get("description") or ""))
    head = f"Background {noun} {row['command_id']}" + (
        f" ({description})" if description else ""
    )
    took = _ran_for(row)
    log = row.get("log_path") or ""
    reason = row.get("failure_reason")
    exit_code = row.get("exit_code")
    when = " while you were working" if steered and reason != "after_turn_limit" else ""

    if row.get("status") == "stopped":
        # Only the user's Stop reports (``Outcome.report``).
        body = f"was stopped by the user after {took}. Output: {log}"
    elif row.get("status") == "completed":
        # A code run that completed exited 0 by definition; the exit code is
        # a shell's vocabulary.
        exited = "" if code_run or exit_code is None else f" with exit code {exit_code}"
        body = f"completed{exited} after {took}{when}. Output: {log}"
    elif reason == "after_turn_limit":
        body = (
            f"FAILED after {took}: it was stopped an hour after your turn ended, "
            f"with no wakeup checking on it. What it wrote is in {log}. If the "
            "user still needs it, rerun it; it resumes where it stopped only if "
            "it saved its progress."
        )
    elif reason == "lost":
        body = (
            f"FAILED after {took}{when}: its computer stopped before the {noun} "
            f"completed. What it wrote is in {log}."
        )
    elif reason == "vanished":
        body = (
            f"FAILED after {took}{when}: it ended without recording how, as a "
            f"process killed from outside does. What it wrote is in {log}."
        )
    elif signal := killed_by(exit_code):
        body = f"was killed by {signal} after {took}{when}. Output: {log}"
    else:
        exited = "" if exit_code is None else f" with exit code {exit_code}"
        body = f"FAILED{exited} after {took}{when}. Output: {log}"

    lines = ["<system>", f"{head} {body}"]
    pending = wakeups_line(wakeups)
    if pending:
        lines.append(pending)
    lines.append("</system>")
    return "\n".join(lines)
