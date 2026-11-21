"""What reaches a computer's machine when its owner chooses the live view
quality, whether its browser keeps sign-ins, or whether it shows as a window."""

from __future__ import annotations

import contextlib
import json
import shlex
import typing
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from ptc_agent.core.sandbox.browser_runtime import protocol
from ptc_agent.core.sandbox.browser_runtime.protocol import NO_REVISION, REVISION_PATTERN, Choices
from src.server.database import computer as computer_db
from src.server.models.computer import BrowserChoicesUpdate, ComputerBrowserQualityRequest
from src.server.services import browser_settings, browser_view

REV = "0" * 19 + "1"
LATER = "0" * 19 + "2"


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
        config[computer_db.BROWSER_QUALITY_KEY] = quality
    return {
        "computer_id": "c-1",
        "user_id": "user-1",
        "kind": kind,
        "config": config,
        "status": "running",
        "provider_ref": "p-1",
    }


def _ran(sandbox) -> list[list[str]]:
    return [shlex.split(call.args[0])[3:] for call in sandbox.runtime.exec.await_args_list]


def test_the_request_offers_exactly_what_the_machine_knows():
    for model in (ComputerBrowserQualityRequest, BrowserChoicesUpdate):
        field = model.model_fields["quality"].annotation
        offered = {arg for arg in typing.get_args(typing.get_args(field)[0])}
        assert offered == set(protocol.STREAM_QUALITIES)


@pytest.mark.parametrize(("quality", "chosen"), [("sharp", "sharp"), (None, None), ("blurry", None)])
def test_only_a_quality_the_machine_knows_is_a_choice(quality, chosen):
    assert browser_settings.read(_computer(quality=quality)).quality == chosen


@pytest.mark.asyncio
async def test_the_choices_go_to_the_machine_in_one_command():
    choices = Choices(quality="sharp", signins=False, signins_revision=REV, revision=LATER)
    sandbox = _sandbox()
    await browser_settings.keep(sandbox, choices)
    [[command, payload]] = _ran(sandbox)
    assert command == "choices"
    assert Choices.from_wire(payload) == choices


@pytest.mark.asyncio
async def test_an_acquire_that_finds_them_unchanged_writes_nothing():
    sandbox = _sandbox()
    await browser_settings.keep(sandbox, Choices("sharp", revision=REV))
    await browser_settings.keep(sandbox, Choices("sharp", revision=REV))
    assert sandbox.runtime.exec.await_count == 1

    # A later save is handed again though it reads the same: a write straight
    # to the machine meanwhile may have left it elsewhere.
    await browser_settings.keep(sandbox, Choices("sharp", revision=LATER))
    assert sandbox.runtime.exec.await_count == 2


@pytest.mark.asyncio
async def test_a_binding_made_without_the_row_hands_nothing():
    sandbox = _sandbox()
    await browser_settings.keep(sandbox, None)
    await browser_settings.keep(None, Choices())
    sandbox.runtime.exec.assert_not_awaited()


@pytest.mark.parametrize(
    ("helper", "os_name", "shown"),
    [(True, "Darwin", True), (False, "Darwin", False), (True, "Linux", False)],
    ids=["mac-under-remote-control", "mac-over-its-own-ssh", "linux"],
)
def test_only_a_mac_under_remote_control_takes_a_window(helper, os_name, shown):
    """A window saved before the machine was reached another way never reaches
    a machine that could not show it."""
    computer = {
        **_computer("ssh", **{computer_db.BROWSER_WINDOW_KEY: True}),
        "provider_config": {"helper": helper, "os": os_name},
    }
    assert browser_settings.read(computer).window is shown
    assert browser_settings.shown(computer).window_available is shown
    docker = {**_computer("docker", **{computer_db.BROWSER_WINDOW_KEY: True}), "provider_config": {}}
    assert browser_settings.read(docker).window is False


def test_the_response_carries_the_choices_as_the_machine_gets_them():
    computer = {**_computer(quality="retina", **{computer_db.BROWSER_SIGNINS_KEY: False}), "user_id": "user-1"}
    assert browser_settings.shown(computer).model_dump() == {
        "quality": "retina",
        "signins": False,
        "window": False,
        "window_available": False,
    }


@pytest.mark.parametrize(
    ("update", "saved"),
    [
        ({"quality": "sharp"}, {"browser_quality": "sharp"}),
        ({"quality": None}, {"browser_quality": None}),
        ({"window": True}, {"browser_window": True}),
        ({"signins": False, "quality": "light"}, {"browser_signins": False, "browser_quality": "light"}),
    ],
    ids=["quality", "follows-size", "window", "several"],
)
@pytest.mark.asyncio
async def test_a_save_writes_only_what_it_names_in_one_write(monkeypatch, update, saved):
    """None removes the quality's key, which is what following the size is."""
    row = _computer()
    write = AsyncMock(return_value=row)
    monkeypatch.setattr(computer_db, "save_browser_choices", write)
    assert await browser_settings.save("c-1", "user-1", BrowserChoicesUpdate(**update)) is row
    write.assert_awaited_once_with("c-1", "user-1", saved)


@pytest.mark.parametrize("body", [{}, {"window": None}, {"signins": None}, {"signins": "yes please"}])
def test_an_update_names_a_choice_and_only_the_quality_may_be_null(body):
    with pytest.raises(ValueError):
        BrowserChoicesUpdate.model_validate(body)


def _executed(monkeypatch) -> list[tuple[str, tuple]]:
    executed: list[tuple[str, tuple]] = []

    class _Cursor:
        async def execute(self, sql, params):
            executed.append((" ".join(sql.split()), params))

        async def fetchone(self):
            return {"computer_id": "c-1"}

    @contextlib.asynccontextmanager
    async def cursor(conn=None):
        yield _Cursor()

    monkeypatch.setattr(computer_db, "_computer_cursor", cursor)
    return executed


@pytest.mark.parametrize("signins", [False, True], ids=["quality", "sign-ins"])
@pytest.mark.asyncio
async def test_every_save_takes_one_revision_made_under_the_rows_lock(monkeypatch, signins):
    """Made before the write, two saves could commit in the opposite order to
    their revisions and leave the row behind what the machine took, which
    drops an older one for good. So the UPDATE makes it from the stored ones,
    once, and a save of the sign-ins stamps that same one on them."""
    executed = _executed(monkeypatch)
    choices = {"browser_quality": None, "browser_window": True}
    if signins:
        choices["browser_signins"] = True
    assert await computer_db.save_browser_choices("c-1", "user-1", choices) == {"computer_id": "c-1"}
    [(sql, params)] = executed
    removed, kept, *where = params
    assert removed == ["browser_quality"]
    assert kept.obj == {key: value for key, value in choices.items() if value is not None}
    assert where == ["c-1", "user-1"]
    assert sql.count("clock_timestamp()") == 1
    assert "right(config->>'browser_rev', 16))::bit(64)::bigint ELSE 0 END + 1" in sql
    assert "right(config->>'browser_signins_rev', 16))::bit(64)::bigint ELSE 0 END + 1" in sql
    stamps = "'browser_rev', saved.revision" + (", 'browser_signins_rev', saved.revision" if signins else "")
    assert f"saved.config || jsonb_build_object({stamps})" in sql
    assert "WHERE computer_id = %s AND user_id = %s AND" in sql
    assert "RETURNING" in sql


def test_the_machine_reads_every_revision_the_server_makes():
    assert REVISION_PATTERN.fullmatch(NO_REVISION)
    assert REVISION_PATTERN.fullmatch(f"{2**62:020x}")


def test_sign_ins_are_kept_unless_the_owner_turned_them_off():
    assert browser_settings.read(_computer()).signins is True
    assert browser_settings.read(_computer(**{computer_db.BROWSER_SIGNINS_KEY: False})).signins is False


@pytest.mark.parametrize(
    ("user_id", "kept"), [("user-1", True), ("shared-ws-1", False)], ids=["person", "shared-holder"]
)
def test_a_computer_holding_shared_workspaces_never_keeps_sign_ins(user_id, kept):
    """Every member's agent drives its browser, which would browse as whoever signed in."""
    computer = {**_computer(**{computer_db.BROWSER_SIGNINS_KEY: True}), "user_id": user_id}
    assert browser_settings.read(computer).signins is kept
    assert browser_settings.shown(computer).signins is kept


@pytest.mark.parametrize(
    ("config", "revision", "signins_revision"),
    [
        ({}, NO_REVISION, NO_REVISION),
        ({"browser_signins_rev": REV}, REV, REV),
        ({"browser_rev": "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6"}, NO_REVISION, NO_REVISION),
        ({"browser_rev": LATER, "browser_signins_rev": REV}, LATER, REV),
    ],
    ids=["never-saved", "sign-ins-saved-only", "old-token", "both"],
)
def test_a_row_without_a_revision_reads_as_its_last_sign_ins_save(config, revision, signins_revision):
    read = browser_settings.read(_computer(**config))
    assert (read.revision, read.signins_revision) == (revision, signins_revision)


def test_the_choices_name_the_computer_they_are_for():
    """Revisions order one computer's saves, so a machine tells a folder taken
    over from another computer by whose choices it holds."""
    assert browser_settings.read(_computer()).computer == "c-1"


@pytest.mark.parametrize(
    "answer",
    [{"done": False}, "usage: browser_runtime", RuntimeError("gone")],
    ids=["refused", "older-runtime", "raises"],
)
@pytest.mark.asyncio
async def test_a_write_that_fails_never_fails_the_acquire_and_is_tried_again(answer):
    failing = _sandbox(error=answer) if isinstance(answer, Exception) else _sandbox(answer)
    await browser_settings.keep(failing, Choices("light", revision=REV))
    await browser_settings.keep(failing, Choices("light", revision=REV))
    assert failing.runtime.exec.await_count == 2


@pytest.mark.asyncio
async def test_a_change_goes_through_the_one_way_to_reach_a_running_machine(monkeypatch):
    asked = AsyncMock(return_value={"done": True})
    monkeypatch.setattr(browser_settings, "ask_running", asked)
    computer = _computer(quality="sharp", browser_rev=REV)
    await browser_settings.apply_now(computer)
    [call] = asked.await_args_list
    assert call.args[0] is computer
    assert call.args[1] == "choices"
    assert Choices.from_wire(call.args[2]) == browser_settings.read(computer)


@pytest.mark.asyncio
async def test_an_acquire_hands_over_the_choices_on_the_row_it_already_read():
    """The binding every acquire resolves carries the choices, so keeping them
    reads nothing more before it writes."""
    from src.server.services.computer_manager import ComputerManager

    binding = ComputerManager._binding_from_computer("ws-1", _computer(quality="light"))
    assert binding.browser is not None and binding.browser.quality == "light"

    sandbox = _sandbox()
    sandbox.installs_browser = False
    await ComputerManager._keep_browser_settings(binding, sandbox)
    [[command, payload]] = _ran(sandbox)
    assert (command, json.loads(payload)["quality"]) == ("choices", "light")


@pytest.mark.parametrize(
    ("answer", "applied"),
    [
        ({"done": True}, True),
        (browser_view.NOT_RUNNING, True),
        ({"done": False}, False),
        (None, False),
        (RuntimeError("gone"), False),
    ],
    ids=["taken", "not-running", "refused", "older-runtime", "raises"],
)
@pytest.mark.asyncio
async def test_a_save_says_whether_the_machine_has_it(monkeypatch, answer, applied):
    """A machine not running takes it at its next acquire, before any browser
    of its opens; a running one that did not take it keeps an open browser as it was."""
    monkeypatch.setattr(
        browser_settings,
        "ask_running",
        AsyncMock(side_effect=answer) if isinstance(answer, Exception) else AsyncMock(return_value=answer),
    )
    assert await browser_settings.apply_now(_computer(quality="sharp")) is applied
