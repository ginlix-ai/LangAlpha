"""What reaches a computer's machine when its owner chooses the live view
quality or whether its browser keeps sign-ins."""

from __future__ import annotations

import json
import typing
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from ptc_agent.core.sandbox.browser_runtime import protocol, signin
from src.server.models.computer import ComputerBrowserQualityRequest
from src.server.services import browser_settings
from src.server.services.browser_settings import Choices

_PREFIX = "python3 -I /r/_internal/src/browser_runtime"


def _sandbox(answer: object = None, *, error: Exception | None = None):
    stdout = answer if isinstance(answer, str) else json.dumps({"done": True} if answer is None else answer)
    runtime = SimpleNamespace(
        exec=AsyncMock(side_effect=error, return_value=SimpleNamespace(exit_code=0, stdout=stdout))
    )
    return SimpleNamespace(
        sandbox_id="sb-1", runtime=runtime, layout=SimpleNamespace(internal_src="/r/_internal/src")
    )


def _computer(kind: str = "docker", quality: object = None, **config: object) -> dict:
    if quality is not None:
        config[browser_settings.QUALITY_KEY] = quality
    return {
        "computer_id": "c-1",
        "user_id": "user-1",
        "kind": kind,
        "config": config,
        "status": "running",
        "provider_ref": "p-1",
    }


def _ran(sandbox) -> list[str]:
    return [call.args[0].removeprefix(f"{_PREFIX} ") for call in sandbox.runtime.exec.await_args_list]


def test_the_request_offers_exactly_what_the_machine_knows():
    field = ComputerBrowserQualityRequest.model_fields["quality"].annotation
    offered = {arg for arg in typing.get_args(typing.get_args(field)[0])}
    assert offered == set(protocol.STREAM_QUALITIES)


@pytest.mark.parametrize(("quality", "chosen"), [("sharp", "sharp"), (None, None), ("blurry", None)])
def test_only_a_quality_the_machine_knows_is_a_choice(quality, chosen):
    assert browser_settings.read(_computer(quality=quality)).quality == chosen


@pytest.mark.asyncio
async def test_the_choices_go_through_the_runtime():
    """The runtime owns where it keeps them; no quality is ``default``, which
    leaves the machine to follow its size."""
    sandbox = _sandbox()
    await browser_settings.keep(sandbox, kind="docker", choices=Choices(quality="sharp", signins=False))
    assert _ran(sandbox) == ["quality sharp", f"signin off {browser_settings.NO_REVISION}"]
    sandbox = _sandbox()
    revision = browser_settings.saved_signins(True)[browser_settings.SIGNINS_REVISION_KEY]
    await browser_settings.keep(
        sandbox, kind="docker", choices=Choices(signins=True, signins_revision=revision)
    )
    assert _ran(sandbox) == ["quality default", f"signin on {revision}"]


@pytest.mark.asyncio
async def test_an_acquire_that_finds_them_unchanged_writes_nothing():
    sandbox = _sandbox()
    await browser_settings.keep(sandbox, kind="docker", choices=Choices("sharp", "r1"))
    await browser_settings.keep(sandbox, kind="docker", choices=Choices("sharp", "r1"))
    assert sandbox.runtime.exec.await_count == 2

    await browser_settings.keep(sandbox, kind="docker", choices=Choices(None, "r2"))
    await browser_settings.keep(sandbox, kind="docker", choices=Choices(None, "r2"))
    assert sandbox.runtime.exec.await_count == 4


@pytest.mark.asyncio
async def test_a_choice_saved_again_is_handed_again_though_it_reads_the_same():
    """Auto, Sharp, then back to Auto: Sharp's write went straight to the
    machine on whichever worker took it, and Auto's may have failed, so this
    sandbox object, which handed Auto at its acquire, cannot know the machine
    still has Auto. The save it reads is a new one, so it hands it again."""
    sandbox = _sandbox()
    await browser_settings.keep(sandbox, kind="docker", choices=Choices(None, "auto-1"))
    await browser_settings.keep(sandbox, kind="docker", choices=Choices(None, "auto-2"))
    assert _ran(sandbox)[2] == "quality default"


def test_each_save_is_a_new_one_written_with_the_choice():
    first, second = browser_settings.saved_quality(None), browser_settings.saved_quality(None)
    assert first[browser_settings.QUALITY_KEY] is None
    assert first[browser_settings.QUALITY_REVISION_KEY] != second[browser_settings.QUALITY_REVISION_KEY]
    computer = _computer(quality="sharp", **{browser_settings.QUALITY_REVISION_KEY: "r9"})
    assert browser_settings.read(computer) == Choices("sharp", "r9")


def test_each_save_of_sign_ins_sorts_after_the_one_before_as_the_machine_reads_them():
    """The machine ignores a save older than its own, so an acquire that read
    the row before a change never erases a sign-in made after it."""
    first = browser_settings.saved_signins(True)[browser_settings.SIGNINS_REVISION_KEY]
    second = browser_settings.saved_signins(True)[browser_settings.SIGNINS_REVISION_KEY]
    for revision in (first, second, browser_settings.NO_REVISION):
        assert signin.REVISION.fullmatch(revision)
    assert browser_settings.NO_REVISION < first < second


def test_sign_ins_are_kept_unless_the_owner_turned_them_off():
    assert browser_settings.read(_computer()).signins is True
    assert browser_settings.read(_computer(**browser_settings.saved_signins(False))).signins is False


@pytest.mark.parametrize(
    ("user_id", "kept"), [("user-1", True), ("shared-ws-1", False)], ids=["person", "shared-holder"]
)
def test_a_computer_holding_shared_workspaces_never_keeps_sign_ins(user_id, kept):
    """Every member's agent drives its browser, which would browse as whoever signed in."""
    computer = {**_computer(**browser_settings.saved_signins(True)), "user_id": user_id}
    assert browser_settings.read(computer).signins is kept


@pytest.mark.asyncio
async def test_a_machine_with_no_browser_is_not_touched():
    sandbox = _sandbox()
    await browser_settings.keep(sandbox, kind="ssh", choices=Choices("sharp", "r1"))
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
    await browser_settings.keep(failing, kind="docker", choices=Choices("light", "r1"))
    await browser_settings.keep(failing, kind="docker", choices=Choices("light", "r1"))
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
    await browser_settings.apply_now(computer)
    assert attached == []


@pytest.mark.asyncio
async def test_a_change_that_lands_meanwhile_is_the_one_left_on_the_machine(monkeypatch):
    """Two changes can reach the machine in either order: the one that wrote
    older choices puts the stored ones there after it."""
    from src.server.database import computer as computer_db

    handed = []

    async def apply(computer, choices):
        handed.append(choices.quality)
        return True

    stored = iter([_computer(quality="retina"), _computer(quality="retina")])
    monkeypatch.setattr(browser_settings, "_apply", apply)
    monkeypatch.setattr(computer_db, "get_computer", AsyncMock(side_effect=lambda _id: next(stored)))
    await browser_settings.apply_now(_computer(quality="light"))
    assert handed == ["light", "retina"]


@pytest.mark.asyncio
async def test_a_change_that_reaches_no_machine_reads_nothing_again(monkeypatch):
    from src.server.database import computer as computer_db

    read = AsyncMock()
    monkeypatch.setattr(computer_db, "get_computer", read)
    await browser_settings.apply_now({**_computer(quality="sharp"), "status": "stopped"})
    read.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_acquire_hands_over_the_choices_on_the_row_it_already_read():
    """The binding every acquire resolves carries the choices, so keeping them
    reads nothing more."""
    from src.server.services.computer_manager import ComputerManager

    binding = ComputerManager._binding_from_computer("ws-1", _computer(quality="light"))
    assert binding.browser.quality == "light"

    sandbox = _sandbox()
    await ComputerManager._keep_browser_settings(binding, sandbox)
    assert _ran(sandbox)[0] == "quality light"
