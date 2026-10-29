"""What code saved through the mount, for the tool call that ran it.

A save through the mount fails at close() with nothing but an errno, so the
reason reaches the agent in the result of the tool call that ran the code:
the daemon tags each request with the call id of the process making it, the
server files the outcome under that id, and the tool collects the list when
its command returns. A process still saving after that, such as a background
job, files under its thread's late list, which the next command in that
thread, or the next read of a background job's output, picks up. A save whose call left no thread on file, or whose call no
command filed, goes to the computer's late list, which the next command
anywhere on it picks up.

Redis only: an outcome is a report, not state, and losing one to a Redis
outage costs a line in a tool result, never a save.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import asdict
from typing import Any

from ptc_agent.core.sandbox.livefs_mount import CallContext
from src.server.services.livefs import cache

logger = logging.getLogger(__name__)

_CALL_TTL_S = 3600
_LATE_TTL_S = 86400
# Who a call runs for, and that it returned, are kept as long as a late
# report: a process the call left running (a background job, anything
# detached) saves under its id until it ends, and with either key gone its
# save runs for no workspace and reports to the next command on the computer.
_FILED_TTL_S = _LATE_TTL_S
_LATE_CAP = 50
# Code in the sandbox can save without end under one call, and the report
# shows ten failures with a count of the rest.
_CALL_CAP = 200
# A path or error can carry whatever a command named.
_TEXT_CAP = 512
_CALL_ID_RE = re.compile(r"^[A-Za-z0-9_-]{8,64}$")

# Filed under the call unless it was already collected, in which case the
# save came after its command returned and belongs to the late list. One
# script, so a collect can never land between the check and the push.
_RECORD = """
if redis.call('EXISTS', KEYS[2]) == 0 then
  redis.call('RPUSH', KEYS[1], ARGV[1])
  redis.call('LTRIM', KEYS[1], -tonumber(ARGV[5]), -1)
  redis.call('EXPIRE', KEYS[1], ARGV[2])
  return 1
end
redis.call('RPUSH', KEYS[3], ARGV[1])
redis.call('LTRIM', KEYS[3], -tonumber(ARGV[4]), -1)
redis.call('EXPIRE', KEYS[3], ARGV[3])
return 0
"""

# ``_RECORD``, once per call and only for a call a command filed: a
# throttled request is refused before it costs anything else, so an id the
# sandbox made up must leave no keys either.
_THROTTLED = """
if redis.call('EXISTS', KEYS[4]) == 0 then
  return -1
end
if not redis.call('SET', KEYS[5], 1, 'NX', 'EX', ARGV[2]) then
  return -1
end
""" + _RECORD


def valid_call_id(call_id: str | None) -> str | None:
    return call_id if call_id and _CALL_ID_RE.fullmatch(call_id) else None


def _late_key(computer_id: str, thread_id: str | None) -> str:
    late = f"{cache.tag(computer_id)}:late"
    return f"{late}:{thread_id}" if thread_id else late


def _keys(computer_id: str, call_id: str, thread_id: str | None) -> tuple[str, str, str]:
    tag = cache.tag(computer_id)
    return (
        f"{tag}:out:{call_id}",
        f"{tag}:done:{call_id}",
        _late_key(computer_id, thread_id),
    )


def _context_key(computer_id: str, call_id: str) -> str:
    return f"{cache.tag(computer_id)}:ctx:{call_id}"


async def open_call(computer_id: str, call_id: str, context: CallContext) -> None:
    """File who a command runs for, for the saves it makes. Lost to a Redis
    outage, its saves run for no conversation or workspace, on the user's
    own clock."""
    client = cache.client()
    if client is None:
        return
    try:
        await client.set(
            _context_key(computer_id, call_id), json.dumps(asdict(context)), ex=_FILED_TTL_S
        )
    except Exception:
        logger.warning("livefs call context not recorded", exc_info=True)


async def call_context(computer_id: str, call_id: str | None) -> CallContext | None:
    client = cache.client()
    if not call_id or client is None:
        return None
    try:
        raw = await client.get(_context_key(computer_id, call_id))
        filed = json.loads(raw) if raw else None
    except Exception:
        logger.warning("livefs call context not read", exc_info=True)
        return None
    if not isinstance(filed, dict):
        return None
    return CallContext(
        workspace_id=filed.get("workspace_id"),
        thread_id=filed.get("thread_id"),
        timezone=filed.get("timezone"),
    )


def _thread_of(context: CallContext | None) -> str | None:
    """The thread whose late list a call's later saves go to. None (no
    thread, or the filing is gone) means the computer's."""
    thread_id = context.thread_id if context is not None else None
    return str(thread_id) if thread_id else None


def _clipped(outcome: dict[str, Any]) -> dict[str, Any]:
    """``outcome`` with its paths kept to their ends, which name the file,
    and its error to its start."""
    kept = dict(outcome)
    for key in ("path", "from", "error"):
        text = kept.get(key)
        if isinstance(text, str) and len(text) > _TEXT_CAP:
            cut = _TEXT_CAP - 3
            kept[key] = text[:cut] + "..." if key == "error" else "..." + text[-cut:]
    return kept


async def record(
    computer_id: str,
    call_id: str | None,
    outcome: dict[str, Any],
    context: CallContext | None = None,
) -> None:
    """File one outcome for its call. ``context`` is the filing the save ran
    for, as its caller read it: a second read could find it expired and hand
    the report to whichever conversation runs next. None reads it here.

    A call id nothing filed gets no list of its own and reports as no
    call's: the id comes from the sandbox, whose code can make up one per
    request."""
    client = cache.client()
    if client is None:
        return
    if context is None:
        context = await call_context(computer_id, call_id)
    if context is None:
        call_id = None
    payload = json.dumps(_clipped(outcome))
    out_key, done_key, late_key = _keys(computer_id, call_id or "-", _thread_of(context))
    try:
        if call_id:
            await client.eval(
                _RECORD, 3, out_key, done_key, late_key,
                payload, _CALL_TTL_S, _LATE_TTL_S, _LATE_CAP, _CALL_CAP,
            )
            return
        async with client.pipeline(transaction=True) as pipe:
            pipe.rpush(late_key, payload)
            pipe.ltrim(late_key, -_LATE_CAP, -1)
            pipe.expire(late_key, _LATE_TTL_S)
            await pipe.execute()
    except Exception:
        logger.warning("livefs outcome not recorded", exc_info=True)


async def throttled(computer_id: str, call_id: str) -> None:
    """Note once per call that the computer was past its request rate: a
    throttled command can be refused hundreds of times, and one line says
    all the agent can act on."""
    client = cache.client()
    if client is None:
        return
    # Read for the thread a closed call's note goes to. An id nothing filed
    # stops here, before any write.
    context = await call_context(computer_id, call_id)
    if context is None:
        return
    try:
        await client.eval(
            _THROTTLED, 5,
            *_keys(computer_id, call_id, _thread_of(context)),
            _context_key(computer_id, call_id), f"{cache.tag(computer_id)}:thr:{call_id}",
            json.dumps({"op": "throttled", "ok": False}),
            _CALL_TTL_S, _LATE_TTL_S, _LATE_CAP, _CALL_CAP,
        )
    except Exception:
        logger.warning("livefs throttle not recorded", exc_info=True)


def _decode(raw: list[Any], *, late: bool) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for item in raw or ():
        try:
            outcome = json.loads(item)
        except (TypeError, ValueError):
            continue
        if late:
            outcome["late"] = True
        out.append(outcome)
    return out


async def collect(
    computer_id: str, call_id: str, thread_id: str | None = None
) -> list[dict[str, Any]]:
    """Close the call to further outcomes and return its own plus the late
    ones of its thread and of the computer. The caller names the thread,
    which it ran the command for, so a collect reads no filing back."""
    client = cache.client()
    if client is None:
        return []
    out_key, done_key, late_key = _keys(computer_id, call_id, thread_id)
    shared_key = _late_key(computer_id, None)
    late_keys = [shared_key] if late_key == shared_key else [shared_key, late_key]
    try:
        async with client.pipeline(transaction=True) as pipe:
            pipe.set(done_key, 1, ex=_FILED_TTL_S)
            pipe.lrange(out_key, 0, -1)
            pipe.delete(out_key)
            for key in late_keys:
                pipe.lrange(key, 0, -1)
                pipe.delete(key)
            _, own, _, *lates = await pipe.execute()
    except Exception:
        logger.warning("livefs outcomes not collected", exc_info=True)
        return []
    late = [item for raw in lates[::2] for item in _decode(raw, late=True)]
    return late + _decode(own, late=False)


_REPORT_CAP = 10


def _late(outcome: dict[str, Any]) -> str:
    return " (from an earlier command)" if outcome.get("late") else ""


def describe(outcomes: list[dict[str, Any]]) -> str:
    """The failed saves, by the sandbox path the agent used, for the tool
    result: a program writing through the mount learns of a refusal only at
    close(), which most never check. Also what a change did, when its file
    reports that (an automation's file names what it created, updated,
    deleted or renamed), and whether requests were refused as too many."""
    reports = [
        ("From an earlier command: " if o.get("late") else "") + str(o["report"])
        for o in outcomes
        if o.get("ok") and o.get("report")
    ]
    sections = reports[-_REPORT_CAP:]
    if len(reports) > _REPORT_CAP:
        earlier = len(reports) - _REPORT_CAP
        sections.insert(0, f"{earlier} earlier changes are not listed; these are the last {_REPORT_CAP}.")
    throttled = [o for o in outcomes if o.get("op") == "throttled"]
    failed = [o for o in outcomes if not o.get("ok") and o.get("op") != "throttled"]
    if throttled:
        sections.append(
            ("From an earlier command: " if all(o.get("late") for o in throttled) else "")
            + "Some file requests were refused as too many: files may be missing "
            "from what the command read, or saves not made; retry the command."
        )
    if failed:
        lines = [
            f"- {o.get('path', '')}: {o.get('error') or 'failed'}"
            + _late(o)
            for o in failed[:_REPORT_CAP]
        ]
        if len(failed) > _REPORT_CAP:
            lines.append(f"- and {len(failed) - _REPORT_CAP} more")
        sections.append("NOT SAVED (the exit status does not show this):\n" + "\n".join(lines))
    return "\n\n".join(sections)
