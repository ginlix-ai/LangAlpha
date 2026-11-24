"""What a Bash or ExecuteCode command meets of the Drive around it: refused
while the Drive is due but not on disk, its saves settled before the file
mount's report when it names the Drive, and the handle's notes after it,
told whether those saves were settled."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from ptc_agent.agent.tools import mount_guard
from ptc_agent.core.sandbox.drive_mount import (
    DRIVE_NOT_HERE,
    DRIVE_UNAVAILABLE,
    NO_DRIVE,
    DriveDisk,
    drive_refusal,
)
from ptc_agent.core.sandbox.livefs_mount import CallContext

NAMES_DRIVE = "ls .agents/drive/personal"


class Drive:
    def __init__(self, disk: DriveDisk = DriveDisk.ON, note: str = "") -> None:
        self.on_disk = AsyncMock(return_value=disk)
        self.push_saves = AsyncMock(return_value=True)
        self.after = AsyncMock(return_value=note)


def _backend(drive) -> SimpleNamespace:
    sandbox = SimpleNamespace(drive=drive, call_filer=AsyncMock(), paid_calls=None)
    return SimpleNamespace(
        sandbox=sandbox,
        config_folder=AsyncMock(return_value=None),
        settled_livefs=AsyncMock(return_value=None),
    )


async def _guarded(backend, text: str):
    run = AsyncMock(return_value=("out", {}))
    result = await mount_guard.run_guarded(
        backend,
        text,
        CallContext(workspace_id="ws-1", thread_id="t-1"),
        run,
        memory_error="memory",
        files_error="files {dir}",
        blocked_event="blocked",
    )
    return result, run


@pytest.mark.asyncio
async def test_a_command_naming_a_drive_not_on_disk_is_refused_after_the_wait():
    drive = Drive(DriveDisk.UNAVAILABLE)
    (text, _), run = await _guarded(_backend(drive), NAMES_DRIVE)
    assert text == DRIVE_UNAVAILABLE
    drive.on_disk.assert_awaited_once_with(wait_s=mount_guard.DRIVE_WAIT_S)
    assert mount_guard.DRIVE_WAIT_S == 8.0
    run.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_drive_that_is_not_this_computers_keeps_its_path_reserved():
    drive = Drive(DriveDisk.OFF)
    (text, _), run = await _guarded(_backend(drive), NAMES_DRIVE)
    assert text == DRIVE_NOT_HERE
    run.assert_not_awaited()


@pytest.mark.asyncio
async def test_the_call_settled_is_handed_to_after_with_its_id():
    drive = Drive(note="Saved now: x.")
    (text, _), run = await _guarded(_backend(drive), NAMES_DRIVE)
    call_id = run.await_args.args[0]
    assert call_id
    drive.push_saves.assert_awaited_once_with(NAMES_DRIVE, call_id)
    drive.after.assert_awaited_once_with(
        "out", text=NAMES_DRIVE, call_id=call_id, settled=True
    )
    assert text == "out\n\nSaved now: x."


@pytest.mark.asyncio
async def test_a_command_not_naming_the_drive_is_not_asked_about_its_disk():
    drive = Drive(DriveDisk.UNAVAILABLE)
    drive.push_saves.return_value = False
    (text, _), _ = await _guarded(_backend(drive), "ls")
    assert text == "out"
    drive.on_disk.assert_not_awaited()
    assert drive.after.await_args.kwargs["settled"] is False


@pytest.mark.asyncio
async def test_a_sandbox_handed_no_drive_runs_as_before():
    (text, _), _ = await _guarded(_backend(NO_DRIVE), NAMES_DRIVE)
    assert text == "out"


@pytest.mark.asyncio
async def test_a_sandbox_with_no_drive_field_runs_as_before():
    backend = _backend(NO_DRIVE)
    del backend.sandbox.drive
    (text, _), _ = await _guarded(backend, NAMES_DRIVE)
    assert text == "out"


def test_the_reserved_path_is_refused_in_every_state_but_on():
    assert drive_refusal(DriveDisk.ON) is None
    assert drive_refusal(DriveDisk.UNAVAILABLE) == DRIVE_UNAVAILABLE
    assert (
        drive_refusal(DriveDisk.OFF)
        == DRIVE_NOT_HERE
        == "The Drive is not available here: it is an experiment the user turns on in "
        "Settings, under Experiments, and it does not reach shared workspaces."
    )
