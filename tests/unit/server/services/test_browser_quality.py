"""What reaches a computer's machine when its owner chooses the live view quality."""

from __future__ import annotations

import json
import typing
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from ptc_agent.core.sandbox.browser_runtime import protocol
from src.server.models.computer import ComputerBrowserQualityRequest
from src.server.services import browser_quality


def _sandbox(answer: object = None, *, error: Exception | None = None):
    stdout = answer if isinstance(answer, str) else json.dumps({"done": True} if answer is None else answer)
    runtime = SimpleNamespace(
        exec=AsyncMock(side_effect=error, return_value=SimpleNamespace(exit_code=0, stdout=stdout))
    )
    return SimpleNamespace(
        sandbox_id="sb-1", runtime=runtime, layout=SimpleNamespace(internal_src="/r/_internal/src")
    )


def _computer(kind: str = "docker", quality: object = None) -> dict:
    config = {} if quality is None else {browser_quality.CONFIG_KEY: quality}
    return {"computer_id": "c-1", "kind": kind, "config": config, "status": "running", "provider_ref": "p-1"}


def test_the_request_offers_exactly_what_the_machine_knows():
    field = ComputerBrowserQualityRequest.model_fields["quality"].annotation
    offered = {arg for arg in typing.get_args(typing.get_args(field)[0])}
    assert offered == set(protocol.STREAM_QUALITIES)


@pytest.mark.parametrize(("quality", "chosen"), [("sharp", "sharp"), (None, None), ("blurry", None)])
def test_only_a_quality_the_machine_knows_is_a_choice(quality, chosen):
    assert browser_quality.chosen(_computer(quality=quality)) == chosen


@pytest.mark.parametrize(("quality", "argument"), [("sharp", "sharp"), (None, "default")])
@pytest.mark.asyncio
async def test_the_choice_goes_through_the_runtime(quality, argument):
    """The runtime owns where it keeps the choice; no choice is ``default``,
    which leaves the machine to follow its size."""
    sandbox = _sandbox()
    await browser_quality.keep(sandbox, kind="docker", quality=quality, revision="r1")
    assert sandbox.runtime.exec.await_args.args[0] == (
        f"python3 -I /r/_internal/src/browser_runtime quality {argument}"
    )


@pytest.mark.asyncio
async def test_an_acquire_that_finds_it_unchanged_writes_nothing():
    sandbox = _sandbox()
    await browser_quality.keep(sandbox, kind="docker", quality="sharp", revision="r1")
    await browser_quality.keep(sandbox, kind="docker", quality="sharp", revision="r1")
    assert sandbox.runtime.exec.await_count == 1

    await browser_quality.keep(sandbox, kind="docker", quality=None, revision="r2")
    await browser_quality.keep(sandbox, kind="docker", quality=None, revision="r2")
    assert sandbox.runtime.exec.await_count == 2


@pytest.mark.asyncio
async def test_a_choice_saved_again_is_handed_again_though_it_reads_the_same():
    """Auto, Sharp, then back to Auto: Sharp's write went straight to the
    machine on whichever worker took it, and Auto's may have failed, so this
    sandbox object, which handed Auto at its acquire, cannot know the machine
    still has Auto. The save it reads is a new one, so it hands it again."""
    sandbox = _sandbox()
    await browser_quality.keep(sandbox, kind="docker", quality=None, revision="auto-1")
    await browser_quality.keep(sandbox, kind="docker", quality=None, revision="auto-2")
    assert sandbox.runtime.exec.await_count == 2
    assert sandbox.runtime.exec.await_args.args[0].endswith("quality default")


def test_each_save_is_a_new_one_written_with_the_choice():
    first, second = browser_quality.saved(None), browser_quality.saved(None)
    assert first[browser_quality.CONFIG_KEY] is None
    assert first[browser_quality.REVISION_KEY] != second[browser_quality.REVISION_KEY]
    computer = {"config": {browser_quality.CONFIG_KEY: "sharp", browser_quality.REVISION_KEY: "r9"}}
    assert (browser_quality.chosen(computer), browser_quality.revision(computer)) == ("sharp", "r9")
    assert browser_quality.revision({"config": {}}) is None


@pytest.mark.asyncio
async def test_a_machine_with_no_browser_is_not_touched():
    sandbox = _sandbox()
    await browser_quality.keep(sandbox, kind="ssh", quality="sharp", revision="r1")
    sandbox.runtime.exec.assert_not_awaited()


@pytest.mark.parametrize(
    "answer",
    [{"done": False}, "usage: browser_runtime", RuntimeError("gone")],
    ids=["refused", "older-runtime", "raises"],
)
@pytest.mark.asyncio
async def test_a_write_that_fails_never_fails_the_acquire_and_is_tried_again(answer):
    failing = (
        _sandbox(error=answer) if isinstance(answer, Exception) else _sandbox(answer)
    )
    await browser_quality.keep(failing, kind="docker", quality="light", revision="r1")
    await browser_quality.keep(failing, kind="docker", quality="light", revision="r1")
    assert failing.runtime.exec.await_count == 2


@pytest.mark.parametrize(
    "computer",
    [
        {**_computer(quality="sharp"), "status": "stopped"},
        {**_computer(quality="sharp"), "provider_ref": None},
        _computer(kind="ssh", quality="sharp"),
    ],
    ids=["stopped", "never-built", "no-browser"],
)
@pytest.mark.asyncio
async def test_a_change_reaches_only_a_running_machine_with_a_browser(computer, monkeypatch):
    from src.server.services import data_token_sweep

    # Recorded rather than raised: apply_now swallows what the attach raises.
    attached = []
    monkeypatch.setattr(data_token_sweep, "running_sandbox", attached.append)
    await browser_quality.apply_now(computer)
    assert attached == []


@pytest.mark.asyncio
async def test_a_change_that_lands_meanwhile_is_the_one_left_on_the_machine(monkeypatch):
    """Two changes can reach the machine in either order: the one that wrote
    an older choice puts the stored one there after it."""
    from src.server.database import computer as computer_db

    handed = []

    async def apply(computer):
        handed.append(browser_quality.chosen(computer))
        return True

    stored = iter([_computer(quality="retina"), _computer(quality="retina")])
    monkeypatch.setattr(browser_quality, "_apply", apply)
    monkeypatch.setattr(computer_db, "get_computer", AsyncMock(side_effect=lambda _id: next(stored)))
    await browser_quality.apply_now(_computer(quality="light"))
    assert handed == ["light", "retina"]


@pytest.mark.asyncio
async def test_a_change_that_reaches_no_machine_reads_nothing_again(monkeypatch):
    from src.server.database import computer as computer_db

    read = AsyncMock()
    monkeypatch.setattr(computer_db, "get_computer", read)
    await browser_quality.apply_now({**_computer(quality="sharp"), "status": "stopped"})
    read.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_acquire_hands_over_the_choice_on_the_row_it_already_read():
    """The binding every acquire resolves carries the choice, so keeping it
    reads nothing more."""
    from src.server.services.computer_manager import ComputerManager

    binding = ComputerManager._binding_from_computer("ws-1", _computer(quality="light"))
    assert binding.browser_quality == "light"

    sandbox = _sandbox()
    await ComputerManager._keep_browser_quality(binding, sandbox)
    assert sandbox.runtime.exec.await_args.args[0].endswith("quality light")
