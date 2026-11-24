"""A command's call id reaches the server whether or not a mount serves.

The ``la`` CLI names the call it runs in, and the server reads the call's
thread from what was filed under it, so every command files its call through
the sandbox's filer, on a machine the mount never serves as on one it does.
"""

from __future__ import annotations

import pytest

from ptc_agent.core.sandbox import livefs_mount
from ptc_agent.core.sandbox.drive_mount import NO_DRIVE
from ptc_agent.core.sandbox.livefs_mount import CallContext

CONTEXT = CallContext(workspace_id="ws-1", thread_id="th-1")


def _call(run, mount, filer, context, **more):
    return livefs_mount.run_call(
        run,
        command=more.pop("command", "ls"),
        mount=mount,
        drive=more.pop("drive", NO_DRIVE),
        filer=filer,
        context=context,
        **more,
    )


async def _run(call_id):
    return f"ran {call_id}", {}


@pytest.mark.asyncio
async def test_with_no_mount_the_call_is_filed_and_named_to_the_command():
    filed = []

    async def filer(call_id, context):
        filed.append((call_id, context))

    text, _ = await _call(_run, None, filer, CONTEXT)

    assert len(filed) == 1
    call_id, context = filed[0]
    assert context == CONTEXT
    assert text == f"ran {call_id}"


@pytest.mark.asyncio
async def test_with_no_mount_and_no_filer_the_command_runs_without_a_call():
    text, _ = await _call(_run, None, None, CONTEXT)
    assert text == "ran None"


@pytest.mark.asyncio
async def test_with_no_context_nothing_is_filed():
    filed = []

    async def filer(call_id, context):
        filed.append(call_id)

    text, _ = await _call(_run, None, filer, None)
    assert text == "ran None"
    assert filed == []


class _Mount:
    def __init__(self, events) -> None:
        self.events = events

    async def report(self, call_id, output, context=None):
        self.events.append(("report", call_id, output))
        return "1 save failed"


@pytest.mark.asyncio
async def test_with_a_mount_the_call_is_filed_before_it_runs_and_reported_after():
    events = []

    async def filer(call_id, context):
        events.append(("file", call_id, context))

    async def run(call_id):
        events.append(("run", call_id))
        return "ok", {}

    text, _ = await _call(run, _Mount(events), filer, CONTEXT)

    (_, call_id, context), ran, reported = events
    assert call_id and context == CONTEXT
    assert ran == ("run", call_id) and reported == ("report", call_id, "ok")
    assert text == "ok\n\n1 save failed"


@pytest.mark.asyncio
async def test_with_a_mount_and_no_context_the_call_still_runs_under_an_id_and_reports():
    events = []

    async def filer(call_id, context):
        events.append(("file", call_id))

    async def run(call_id):
        events.append(("run", call_id))
        return "ok", {}

    await _call(run, _Mount(events), filer, None)

    (_, call_id), (report, reported_id, _) = events
    assert call_id and (report, reported_id) == ("report", call_id)


class _Drive:
    def __init__(self, events) -> None:
        self.events = events

    async def push_saves(self, text, call_id):
        self.events.append(("push", call_id, text))
        return True

    async def after(self, output, *, text="", call_id=None, settled=False):
        self.events.append(("after", call_id, output, settled, text))
        return "Saved now: x."


@pytest.mark.asyncio
async def test_a_call_files_runs_settles_its_drive_saves_then_reports_in_order():
    """Each step reads what the one before left: the Drive's refusals are
    filed before the mount's report collects them, and the Drive's note
    comes last, after the paid calls and the report."""
    events = []

    async def filer(call_id, context):
        events.append(("file", call_id))

    async def run(call_id):
        events.append(("run", call_id))
        return "out", {}

    async def paid_calls(call_id):
        events.append(("paid", call_id))
        return "1 paid call"

    text, _ = await _call(
        run,
        _Mount(events),
        filer,
        CONTEXT,
        command="cat .agents/drive/personal/x",
        drive=_Drive(events),
        paid_calls=paid_calls,
    )

    call_id = events[0][1]
    assert call_id
    assert events == [
        ("file", call_id),
        ("run", call_id),
        ("push", call_id, "cat .agents/drive/personal/x"),
        ("paid", call_id),
        ("report", call_id, "out\n\n1 paid call"),
        ("after", call_id, "out\n\n1 paid call\n\n1 save failed", True, "cat .agents/drive/personal/x"),
    ]
    assert text == "out\n\n1 paid call\n\n1 save failed\n\nSaved now: x."
