"""Bringing in an everyday browser's sign-ins, as the server decides whether to ask the machine."""

from __future__ import annotations

import pytest

from ptc_agent.core.sandbox.browser_runtime import protocol
from src.server.models.computer import (
    BrowserSigninBringResponse,
    BrowserSigninSource,
    BrowserSigninSourcesResponse,
)
from src.server.services import browser_everyday, browser_view

ARC = {"browser": "arc", "name": "Arc", "profile": "Default", "profile_name": "Work"}


@pytest.fixture
def machine(monkeypatch) -> dict:
    """The machine behind ``ask_running``: what it was asked, and what it answers."""
    state: dict = {"asked": [], "answers": {}}

    async def ask_running(computer, *command, timeout):
        state["asked"].append(command)
        answer = state["answers"].get(command[1])
        if isinstance(answer, Exception):
            raise answer
        return answer

    monkeypatch.setattr(browser_everyday, "ask_running", ask_running)
    monkeypatch.setattr(browser_everyday, "keeps_signins", lambda computer: computer.get("keep", True))
    return state


def _computer(**overrides):
    return {"computer_id": "c1", "kind": "ssh", "provider_ref": "ref", "status": "running", **overrides}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("overrides", "reason"),
    [({"kind": "docker"}, "unavailable"), ({"keep": False}, "unavailable")],
    ids=["not-own-machine", "sign-ins-off"],
)
async def test_a_machine_with_nowhere_to_bring_sign_ins_is_not_asked(machine, overrides, reason):
    """Not the machine's ``not_kept``, whose copy is about a browser already open."""
    assert await browser_everyday.sources(_computer(**overrides)) == BrowserSigninSourcesResponse(reason=reason)
    assert await browser_everyday.bring(_computer(**overrides), "arc", "Default") == BrowserSigninBringResponse(
        done=False, reason=reason
    )
    assert machine["asked"] == []


@pytest.mark.asyncio
async def test_only_well_formed_sources_reach_the_app(machine):
    # One a bring would refuse is not listed: the app could only fail it.
    odd = {**ARC, "profile": "Profile/../x"}
    machine["answers"]["list"] = {"sources": [ARC, {"browser": 1}, odd]}
    assert await browser_everyday.sources(_computer()) == BrowserSigninSourcesResponse(
        sources=[BrowserSigninSource(**ARC)]
    )
    assert machine["asked"] == [("everyday", "list")]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("answer", "result"),
    [
        ({"done": True, "count": 12, "skipped": 3}, BrowserSigninBringResponse(done=True, count=12, skipped=3)),
        ({"done": True, "count": 4}, BrowserSigninBringResponse(done=True, count=4)),
        ({"done": True, "count": -1, "skipped": 0}, BrowserSigninBringResponse(done=False, reason="failed")),
        ({"done": False}, BrowserSigninBringResponse(done=False, reason="failed")),
        *(
            ({"done": False, "reason": reason}, BrowserSigninBringResponse(done=False, reason=reason))
            for reason in protocol.BRING_REFUSALS
        ),
        ({"done": False, "reason": "not_running"}, BrowserSigninBringResponse(done=False, reason="failed")),
        ({"done": False, "reason": "new"}, BrowserSigninBringResponse(done=False, reason="failed")),
        (None, BrowserSigninBringResponse(done=False, reason="failed")),
        (RuntimeError("gone"), BrowserSigninBringResponse(done=False, reason="failed")),
        (browser_view.NOT_RUNNING, BrowserSigninBringResponse(done=False, reason="not_running")),
    ],
    ids=lambda value: repr(value)[:40],
)
async def test_only_the_machines_own_refusals_reach_the_app(machine, answer, result):
    """A reason only the server gives, or one the machine has no word for, is a failure."""
    machine["answers"]["bring"] = answer
    assert await browser_everyday.bring(_computer(), "arc", "Default") == result
    assert machine["asked"] == [("everyday", "bring", "arc", "Default")]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("answer", "reason"),
    [(browser_view.NOT_RUNNING, "not_running"), (RuntimeError("gone"), "failed"), ({"sources": "x"}, "failed")],
    ids=["not-running", "raises", "malformed"],
)
async def test_sources_that_could_not_be_listed_say_why(machine, answer, reason):
    machine["answers"]["list"] = answer
    assert await browser_everyday.sources(_computer()) == BrowserSigninSourcesResponse(reason=reason)
