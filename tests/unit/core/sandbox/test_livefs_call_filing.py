"""A command's call id reaches the server whether or not a mount serves.

The ``la`` CLI names the call it runs in, and the server reads the call's
thread from what was filed under it, so every command files its call through
the sandbox's filer, on a machine the mount never serves as on one it does.
"""

from __future__ import annotations

import pytest

from ptc_agent.core.sandbox import livefs_mount
from ptc_agent.core.sandbox.livefs_mount import CallContext

CONTEXT = CallContext(workspace_id="ws-1", thread_id="th-1")


async def _run(call_id):
    return f"ran {call_id}", {}


@pytest.mark.asyncio
async def test_with_no_mount_the_call_is_filed_and_named_to_the_command():
    filed = []

    async def filer(call_id, context):
        filed.append((call_id, context))

    text, _ = await livefs_mount.through_mount(None, filer, CONTEXT, _run)

    assert len(filed) == 1
    call_id, context = filed[0]
    assert context == CONTEXT
    assert text == f"ran {call_id}"


@pytest.mark.asyncio
async def test_with_no_mount_and_no_filer_the_command_runs_without_a_call():
    text, _ = await livefs_mount.through_mount(None, None, CONTEXT, _run)
    assert text == "ran None"


@pytest.mark.asyncio
async def test_with_no_context_nothing_is_filed():
    filed = []

    async def filer(call_id, context):
        filed.append(call_id)

    text, _ = await livefs_mount.through_mount(None, filer, None, _run)
    assert text == "ran None"
    assert filed == []


class _Mount:
    def __init__(self, events) -> None:
        self.events = events

    async def report(self, call_id, output, context=None):
        self.events.append(("report", call_id))
        return "1 save failed"


@pytest.mark.asyncio
async def test_with_a_mount_the_call_is_filed_before_it_runs_and_reported_after():
    events = []

    async def filer(call_id, context):
        events.append(("file", call_id, context))

    async def run(call_id):
        events.append(("run", call_id))
        return "ok", {}

    text, _ = await livefs_mount.through_mount(_Mount(events), filer, CONTEXT, run)

    (_, call_id, context), ran, reported = events
    assert call_id and context == CONTEXT
    assert ran == ("run", call_id) and reported == ("report", call_id)
    assert text == "ok\n\n1 save failed"


@pytest.mark.asyncio
async def test_with_a_mount_and_no_context_the_call_still_runs_under_an_id_and_reports():
    events = []

    async def filer(call_id, context):
        events.append(("file", call_id))

    async def run(call_id):
        events.append(("run", call_id))
        return "ok", {}

    await livefs_mount.through_mount(_Mount(events), filer, None, run)

    (_, call_id), reported = events
    assert call_id and reported == ("report", call_id)
