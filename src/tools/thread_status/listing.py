"""ThreadStatus's lines, worded from each source's own rows.

One item per command, task, wakeup, automation or analyst, led by the id the
agent already uses for it, in one vocabulary of statuses: running, pending,
completed, failed and stopped. Pure: the tool reads the rows, this words them.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Literal, get_args

from src.server.database.wakeups.contract import as_datetime
from src.server.services.commands.notice import killed_by
from src.tools.wakeups.clock import user_zone_of, zone_of
from src.tools.wakeups.describe import pending_line, schedule_of, short_desc
from src.utils.durations import format_duration

ItemType = Literal["command", "task", "wakeup", "automation", "analyst"]
ItemStatus = Literal["running", "pending", "completed", "failed", "stopped"]
#: The types in the order running items are listed.
TYPES: tuple[ItemType, ...] = get_args(ItemType)
ENDED: tuple[ItemStatus, ...] = ("completed", "failed", "stopped")

#: How far back an ended item is listed.
RECENT = timedelta(hours=24)
#: Ended items in the full view, and in a view filtered to one ended status.
LISTED_ENDED_MAX = 10
FILTERED_ENDED_MAX = 50

_DESCRIPTION_MAX = 80
_TAIL_LINES = 20

_TASK_STATUS: dict[str, ItemStatus] = {
    "in_progress": "running",
    "completed": "completed",
    "error": "failed",
    "cancelled": "stopped",
    "interrupted": "stopped",
}
_RUN_WORD = {
    "completed": "completed",
    "failed": "failed",
    "timeout": "timed out",
    "skipped": "skipped",
}
_DAYS = ("Mondays", "Tuesdays", "Wednesdays", "Thursdays", "Fridays", "Saturdays", "Sundays")


@dataclass(frozen=True)
class Item:
    """One line of the listing, with the lines under it."""

    id: str
    type: ItemType
    status: ItemStatus
    text: str
    #: Running: when it started. Pending: when it next runs. Ended: when it
    #: ended, which orders the ended ones newest first.
    at: datetime
    more: tuple[str, ...] = ()


def _one_line(text: Any, cap: int = _DESCRIPTION_MAX) -> str:
    kept = "".join(c for c in str(text or "") if c.isprintable() or c.isspace())
    line = " ".join(kept.split())
    if len(line) > cap:
        line = line[: cap - 1].rstrip() + "…"
    return line


def _parts(*parts: str | None) -> str:
    return " · ".join(p for p in parts if p)


def _took(start: Any, end: Any) -> str:
    started, ended = as_datetime(start), as_datetime(end)
    if started is None or ended is None:
        return format_duration(0)
    return format_duration((ended - started).total_seconds())


def _when(at: datetime, now: datetime, user_tz: str) -> str:
    """``16:00 today``, ``09:00 tomorrow`` or ``Tue 10-27 09:00``, on the user's clock."""
    zone = user_zone_of(user_tz)
    local, today = at.astimezone(zone), now.astimezone(zone).date()
    if local.date() == today:
        return f"{local:%H:%M} today"
    if local.date() == today + timedelta(days=1):
        return f"{local:%H:%M} tomorrow"
    if local.date() == today - timedelta(days=1):
        return f"{local:%H:%M} yesterday"
    return f"{local:%a %m-%d %H:%M}"


# -- Commands -------------------------------------------------------------------


def command_state(row: Mapping[str, Any], now: datetime) -> str:
    """How a command stands: its running time, or how it ended and after how long."""
    status = row.get("status")
    if status == "running":
        took = _took(row.get("started_at"), now)
        # A stop under way, or the after-turn hour ending it.
        return f"{took}, stopping" if row.get("stopped_by") else took
    took = _took(row.get("started_at"), row.get("ended_at"))
    if status == "completed":
        return f"completed after {took}"
    if status == "stopped":
        by = {"agent": " by you", "user": " by the user"}.get(row.get("stopped_by") or "", "")
        return f"stopped{by} after {took}"
    reason = row.get("failure_reason")
    if reason == "lost":
        return f"failed after {took}: its computer stopped"
    if reason == "vanished":
        return f"failed after {took}: it ended without recording how"
    if reason == "after_turn_limit":
        return f"failed after {took}: stopped an hour after your turn ended"
    code = row.get("exit_code")
    if signal := killed_by(code):
        return f"killed by {signal} after {took}"
    return f"failed, exit {code} after {took}" if code is not None else f"failed after {took}"


def command_item(row: Mapping[str, Any], now: datetime) -> Item:
    """A command's line; an ended one says ``reported`` once the ledger has
    its end reaching the agent."""
    status = row["status"]
    ended = status != "running"
    return Item(
        id=str(row["command_id"]),
        type="command",
        status=status,
        text=_parts(
            str(row.get("tool") or ""),
            _one_line(row.get("description")),
            command_state(row, now),
            "reported" if ended and row.get("reported") else None,
        ),
        more=(f"log {row.get('log_path')}",),
        at=as_datetime(row.get("ended_at") if ended else row.get("started_at")) or now,
    )


def command_detail(row: Mapping[str, Any], now: datetime) -> str:
    running = row["status"] == "running"
    state = command_state(row, now)
    lines = [
        f"{row['command_id']}  command · {_parts(str(row.get('tool') or ''), _one_line(row.get('description')))}",
        f"Status: running for {state}"
        if running
        else f"Status: {_parts(state, 'reported' if row.get('reported') else None)}",
        f"Log: {row.get('log_path')}",
        f"Command: {_one_line(row.get('command_preview'), 500)}",
    ]
    tail = (row.get("output_tail") or "").rstrip()
    if not running and tail:
        lines.append(f"Last lines of the log:\n{_last_lines(tail)}")
    return "\n".join(lines)


def _last_lines(text: str) -> str:
    return "\n".join(text.splitlines()[-_TAIL_LINES:])


# -- Tasks ----------------------------------------------------------------------


def task_status(row: Mapping[str, Any]) -> ItemStatus:
    return _TASK_STATUS.get(str(row.get("status")), "stopped")


def task_state(row: Mapping[str, Any], now: datetime) -> str:
    raw = str(row.get("status"))
    if raw == "in_progress":
        return _took(row.get("started_at"), now)
    took = _took(row.get("started_at"), row.get("finalized_at") or now)
    if raw == "interrupted":
        return f"stopped at an interrupt after {took}"
    return f"{task_status(row)} after {took}"


def task_item(row: Mapping[str, Any], now: datetime, progress: str | None = None) -> Item:
    status = task_status(row)
    ended = status != "running"
    return Item(
        id=f"Task-{row['task_id']}",
        type="task",
        status=status,
        text=_parts(
            str(row.get("subagent_type") or "subagent"),
            _one_line(row.get("description")),
            task_state(row, now),
            None if ended else progress,
        ),
        at=as_datetime(row.get("finalized_at") if ended else row.get("started_at")) or now,
    )


def task_detail(row: Mapping[str, Any], now: datetime, progress: str | None = None) -> str:
    state = task_state(row, now)
    status = (
        f"running for {state}" + (f" · {progress}" if progress else "")
        if task_status(row) == "running"
        else state
    )
    return "\n".join(
        [
            f"Task-{row['task_id']}  task · {_parts(str(row.get('subagent_type') or 'subagent'), _one_line(row.get('description'), 300))}",
            f"Status: {status}",
            f'TaskOutput(task_id="{row["task_id"]}") reads its result.',
        ]
    )


def task_progress(total_tool_calls: int, current_tool: str | None) -> str:
    calls = f"{total_tool_calls} tool call" + ("" if total_tool_calls == 1 else "s")
    return f"{calls}, now {current_tool}" if current_tool else calls


# -- Wakeups --------------------------------------------------------------------


def _wakeup_ended(row: Mapping[str, Any], now: datetime) -> str | None:
    """How a wakeup ended, or None while it can still fire."""
    status = row.get("status")
    if status == "armed":
        expires = as_datetime(row.get("expires_at"))
        return "expired" if expires is not None and expires <= now else None
    return {"fired": "fired", "finished": "finished", "cancelled": "cancelled"}.get(
        str(status), "expired"
    )


def _wakeup_when(row: Mapping[str, Any], now: datetime, user_tz: str) -> str:
    kind = row.get("kind")
    fires = schedule_of(row)
    if kind == "at" and fires:
        return f"at {_when(fires[0], now, user_tz)}"
    if kind == "cron":
        nxt = as_datetime(row.get("next_fire_at"))
        if nxt is None:
            done = int(row.get("fires_done") or 0)
            nxt = fires[done] if done < len(fires) else None
        head = f"cron {row.get('cron')}"
        return f"{head}, next {_when(nxt, now, user_tz)}" if nxt else head
    # A price or an order says what it watches, as the cancel names it.
    return short_desc({**row, "label": None})


def wakeup_item(row: Mapping[str, Any], now: datetime, user_tz: str) -> Item:
    ended = _wakeup_ended(row, now)
    label = _one_line(row.get("label"))
    if ended is None:
        nxt = as_datetime(row.get("next_fire_at")) or (schedule_of(row) or [None])[0]
        return Item(
            id=str(row.get("short_id")),
            type="wakeup",
            status="pending",
            text=_parts(label, _wakeup_when(row, now, user_tz)),
            at=nxt or now,
        )
    at = as_datetime(
        row.get("last_fired_at") if ended == "fired" else row.get("updated_at")
    ) or now
    how = f"fired {_when(at, now, user_tz)}" if ended == "fired" else ended
    return Item(
        id=str(row.get("short_id")),
        type="wakeup",
        status="stopped" if ended == "cancelled" else "completed",
        text=_parts(label or _wakeup_when(row, now, user_tz), how),
        at=at,
    )


def wakeup_detail(item: Item, row: Mapping[str, Any], now: datetime, user_tz: str) -> str:
    """``item`` is ``wakeup_item`` of the same row."""
    if item.status == "pending":
        return pending_line(row, now, user_tz).removeprefix("- ")
    return f"{item.id}  wakeup · {item.text}"


# -- Automations ----------------------------------------------------------------


def cron_words(expr: str | None) -> str | None:
    """``weekdays 08:30`` for the common shapes of a cron, else None."""
    fields = (expr or "").split()
    if len(fields) != 5:
        return None
    minute, hour, dom, month, dow = fields
    if not (minute.isdigit() and hour.isdigit()) or dom != "*" or month != "*":
        return None
    at = f"{int(hour):02d}:{int(minute):02d}"
    if dow == "*":
        return f"daily {at}"
    if dow.upper() in ("1-5", "MON-FRI"):
        return f"weekdays {at}"
    if dow.isdigit() and 0 <= int(dow) <= 7:
        return f"{_DAYS[(int(dow) - 1) % 7]} {at}"
    return None


def _schedule(row: Mapping[str, Any]) -> str:
    kind = row.get("trigger_type")
    if kind == "cron":
        return cron_words(row.get("cron_expression")) or f"cron {row.get('cron_expression')}"
    if kind == "once":
        return "once"
    return "on a price move" if kind == "price" else str(kind or "")


def _local(at: datetime, row: Mapping[str, Any]) -> datetime:
    return at.astimezone(zone_of(row.get("timezone"), "UTC"))


def automation_item(row: Mapping[str, Any], now: datetime) -> Item | None:
    """None for an automation that neither runs, waits to, nor ran in the window."""
    last = row.get("last_execution") or {}
    run_status = last.get("status")
    name = _one_line(row.get("name"))
    file_name = str(row.get("file_name"))
    if run_status in ("pending", "waiting", "running"):
        started = as_datetime(last.get("started_at")) or as_datetime(last.get("created_at")) or now
        return Item(
            id=file_name,
            type="automation",
            status="running",
            text=_parts(name, _schedule(row), f"running for {_took(started, now)}"),
            at=started,
        )
    last_word = _RUN_WORD.get(str(run_status)) if run_status else None
    next_run = as_datetime(row.get("next_run_at"))
    if row.get("status") == "active" and next_run is not None:
        return Item(
            id=file_name,
            type="automation",
            status="pending",
            text=_parts(
                name,
                _schedule(row),
                f"next run {_local(next_run, row):%a %H:%M}",
                f"last run {last_word}" if last_word else None,
            ),
            at=next_run,
        )
    ended = as_datetime(last.get("completed_at"))
    if last_word and ended is not None and now - ended <= RECENT and run_status != "skipped":
        return Item(
            id=file_name,
            type="automation",
            status="completed" if run_status == "completed" else "failed",
            text=_parts(name, _schedule(row), f"last run {last_word}"),
            at=ended,
        )
    return None


def automation_detail(row: Mapping[str, Any], now: datetime, automations_dir: str) -> str:
    last = row.get("last_execution") or {}
    next_run = as_datetime(row.get("next_run_at"))
    last_at = as_datetime(last.get("completed_at") or last.get("started_at"))
    lines = [
        f"{row.get('file_name')}  automation · {_one_line(row.get('name'))}",
        f"Schedule: {_schedule(row)} ({row.get('timezone') or 'UTC'}), {row.get('status')}",
    ]
    if next_run is not None and row.get("status") == "active":
        lines.append(f"Next run: {_local(next_run, row):%a %Y-%m-%d %H:%M}")
    if last.get("status"):
        word = _RUN_WORD.get(str(last["status"]), str(last["status"]))
        when = f" {_local(last_at, row):%a %Y-%m-%d %H:%M}" if last_at else ""
        lines.append(f"Last run: {word}{when}")
    lines.append(f"File: {automations_dir}/{row.get('file_name')}")
    return "\n".join(lines)


# -- Analysts -------------------------------------------------------------------


def analyst_item(label: Mapping[str, Any], now: datetime) -> Item:
    return Item(
        id=str(label.get("thread_id")),
        type="analyst",
        status="running",
        text=_parts(_one_line(label.get("workspace_name")), _one_line(label.get("title")), "working"),
        at=now,
    )


def analyst_detail(label: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            f"{label.get('thread_id')}  analyst · "
            + _parts(_one_line(label.get("workspace_name")), _one_line(label.get("title"))),
            "Status: working",
        ]
    )


# -- The listing ----------------------------------------------------------------


@dataclass(frozen=True)
class Listing:
    text: str
    #: The ended items shown, which is what the agent has now seen end.
    shown_ended: tuple[Item, ...]
    running: bool


def _lines(items: Sequence[Item], width: int) -> list[str]:
    out: list[str] = []
    for item in items:
        out.append(f"  {item.id:<{width}}{item.type} · {item.text}")
        out.extend(f"  {'':<{width}}{line}" for line in item.more)
    return out


def render_listing(items: Iterable[Item], *, status: ItemStatus | None = None) -> Listing:
    """Running, then pending, then up to 10 ended newest first, for ``status``
    only when given."""
    items = [i for i in items if status is None or i.status == status]
    order = {t: n for n, t in enumerate(TYPES)}
    running = sorted(
        (i for i in items if i.status == "running"), key=lambda i: (order[i.type], i.at)
    )
    pending = sorted((i for i in items if i.status == "pending"), key=lambda i: i.at)
    ended = sorted((i for i in items if i.status in ENDED), key=lambda i: i.at, reverse=True)
    cap = FILTERED_ENDED_MAX if status in ENDED else LISTED_ENDED_MAX
    shown_ended, hidden = ended[:cap], len(ended) - min(len(ended), cap)
    shown = [*running, *pending, *shown_ended]
    if not shown:
        return Listing(_nothing(status), (), False)
    width = max(len(i.id) for i in shown) + 2
    lines: list[str] = []
    if running:
        lines += ["Running", *_lines(running, width)]
    if pending:
        lines += ["Pending", *_lines(pending, width)]
    if shown_ended:
        head = (
            f"{status.capitalize()} in the last 24 hours"
            if status in ENDED
            else "Completed, failed or stopped in the last 24 hours"
        )
        lines += [head, *_lines(shown_ended, width)]
        if hidden and status in ENDED:
            lines.append(f"  +{hidden} more.")
        elif hidden:
            lines.append(
                f"  +{hidden} more. Filter by status (completed, failed or stopped) to see them."
            )
    return Listing("\n".join(lines), tuple(shown_ended), bool(running))


def _nothing(status: ItemStatus | None) -> str:
    if status is None:
        return (
            "Nothing is running or set to report back to this conversation, and "
            "nothing ended in the last 24 hours."
        )
    if status in ENDED:
        return f"Nothing {status} in the last 24 hours."
    return f"Nothing is {status}."
