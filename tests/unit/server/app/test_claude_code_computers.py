"""Claude Code on the user's own computers, for Settings: each computer's
state without waking or dialing one that sleeps, and the token placed on its
machine only once Claude serves it there, never kept by the backend."""

import asyncio
import json
import logging
import sys
import time
from contextlib import ExitStack
from dataclasses import asdict
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
import structlog
from fastapi.responses import JSONResponse
from httpx import ASGITransport, AsyncClient

from ptc_agent.agent.claude_code.cli_install import CliMissing, CliUnsupported
from ptc_agent.agent.claude_code.machine_token import MachineState
from ptc_agent.agent.claude_code.ssh_launcher import START_TIMEOUT_S, SSHLauncher
from ptc_agent.core.sandbox.providers._ssh_transport import Machine
from ptc_agent.core.sandbox.providers.ssh import MUSL_UNSUPPORTED
from ptc_agent.core.sandbox.runtime import SandboxTransientError
from src.server.app import claude_code_computers as route
from src.server.services import claude_oauth
from src.server.services.claude_code import checks, flow, machine as machines
from src.server.services.claude_code.machine import MachineAnswer, answer_key, change_lock, unreached_key
from src.server.services.claude_code.saved import token_check_key
from src.server.services.oauth_signin import OAuthRefusal
from tests.conftest import create_test_app
from tests.unit.server.services.claude_code.fakes import Cache, serving

MACHINES = "src.server.services.claude_code.machine"
TOKENS = "ptc_agent.agent.claude_code.machine_token"
USER = "test-user-123"
CID = "11111111-1111-1111-1111-111111111111"
BASE = f"/api/v1/claude-code/computers/{CID}"
MINTED = claude_oauth.SetupToken(
    access_token="sk-ant-oat01-minted", expires_at=datetime(2027, 10, 29, tzinfo=timezone.utc),
    token_uuid="tok-uuid", email="user@example.com",
)


def _app(handler=None):
    app = create_test_app(route.router)
    app.add_exception_handler(
        OAuthRefusal, handler or (lambda _, exc: JSONResponse({"detail": exc.detail, "error": exc.error}, exc.status_code))
    )
    return app


@pytest_asyncio.fixture
async def client():
    async with AsyncClient(transport=ASGITransport(app=_app()), base_url="http://test") as c:
        yield c


STAGED = f".staged.{CID}.Ab12Cd"
#: The tokens' folder, as a login with its own XDG_CONFIG_HOME under its home names it.
FOLDER = "~/cfg/langalpha/claude-code"


class _Machine:
    """The computer's token file and one staged beside it, as the
    machine-token scripts reach them, and the CLI's check: True, False, or an
    exception the check raised."""

    def __init__(self):
        self.token, self.staged, self.answer, self.invalid, self.perl, self.musl = None, None, True, False, True, False
        self.managed: tuple[str, ...] = ()
        self.calls, self.fail, self.checked = [], {}, []

    async def run(self, script, **kw):
        """The launcher's one look at what Claude Code needs there, before it installs anything."""
        self.calls.append("facts")
        if "facts" in self.fail:
            raise self.fail["facts"]
        there = int(self.invalid or self.token is not None)
        out = f"{' '.join(self.managed)}\n{there}\n{'musl' if self.musl else 'perl' if self.perl else 'none'}\n"
        return SimpleNamespace(stdout=out.encode(), stderr=b"", exit_code=0)

    async def install(self, link, root, **kw):
        self.calls.append("install")
        if "install" in self.fail:
            raise self.fail["install"]
        return f"{root}/_internal/claude-code/cli/claude"

    async def stage(self, link, computer_id, token):
        assert computer_id == CID
        self.calls.append("write")
        if "write" in self.fail:
            raise self.fail["write"]
        self.staged = token
        return STAGED

    async def promote(self, link, computer_id, staged):
        assert (computer_id, staged) == (CID, STAGED)
        self.calls.append("promote")
        if "promote" in self.fail:
            raise self.fail["promote"]
        self.token, self.staged = self.staged, None
        return FOLDER

    async def discard(self, link, staged):
        assert staged == STAGED
        self.calls.append("discard")
        if "discard" in self.fail:
            raise self.fail["discard"]
        self.staged = None

    async def remove(self, link, computer_id):
        assert computer_id == CID
        self.calls.append("remove")
        if "remove" in self.fail:
            raise self.fail["remove"]
        had, self.token = self.token is not None, None
        return had

    async def state(self, link, computer_id):
        assert computer_id == CID
        self.calls.append("state")
        if "state" in self.fail:
            raise self.fail["state"]
        file = (
            "policy" if self.managed else "unsupported" if self.musl else "no_perl" if not self.perl
            else "invalid" if self.invalid else "file" if self.token else "none"
        )
        return MachineState(file, self.invalid or self.token is not None, FOLDER)

    async def works(self, launcher, model):
        self.calls.append("check")
        self.checked.append(launcher)
        if isinstance(self.answer, BaseException):
            raise self.answer
        return self.answer


def _computer(status="running", **kw):
    """The computer's full row, as ``get_computer`` reads it."""
    return {
        "computer_id": CID, "user_id": USER, "name": "Studio Mac", "kind": "ssh", "status": status,
        "provider_config": {"helper": True, "tunnel": "tun-1"}, "provider_ref": "ref", "root_dir": None, **kw,
    }


def _roster_row(computer):
    """The same computer as the roster reads it: no provider_ref, which dialing needs."""
    return {k: computer[k] for k in ("computer_id", "name", "kind", "provider_config", "root_dir", "status")} | {"is_home": False}


class _World:
    """One own computer whose machine answers; every knob a test turns is on it."""

    def __init__(self):
        self.machine, self.cache, self.computer = _Machine(), Cache(), _computer()
        self.tunnel_live, self.backend, self.opted_in, self.rows, self.saved = True, False, True, True, None
        link = SimpleNamespace(run=self.machine.run, target=SimpleNamespace(host="studio"))
        self.handle = Machine(AsyncMock(return_value=link), lambda: "/home/me/langalpha")
        self.dialed: list[dict] = []
        self.dial_error: BaseException | None = None

    async def dial(self, computer):
        self.dialed.append(computer)
        if self.dial_error is not None:
            raise self.dial_error
        return self.handle


@pytest.fixture
def world():
    w = _World()
    m = w.machine
    patches = [
        serving(w.cache),
        patch("src.server.services.claude_code.flow.rows_available", side_effect=lambda: w.rows),
        patch("src.server.services.claude_code.flow.user_feature_enabled", new_callable=AsyncMock, side_effect=lambda *_: w.opted_in),
        patch(f"{MACHINES}.backend_available", side_effect=lambda: w.backend),
        patch(f"{MACHINES}.get_computer", new_callable=AsyncMock, side_effect=lambda _: w.computer),
        patch(f"{MACHINES}.get_computer_roster", new_callable=AsyncMock, side_effect=lambda _: [_roster_row(w.computer)]),
        patch(f"{MACHINES}.tunnels_for", new_callable=AsyncMock, side_effect=lambda *_: {"tun-1": {"live": w.tunnel_live}}),
        patch(f"{MACHINES}.stored_token", new_callable=AsyncMock, side_effect=lambda _: w.saved),
        patch(f"{MACHINES}.machine_of", new=w.dial),
        patch(f"{TOKENS}.stage_machine_token", new=m.stage),
        patch(f"{TOKENS}.promote_machine_token", new=m.promote),
        patch(f"{TOKENS}.discard_staged_token", new=m.discard),
        patch(f"{TOKENS}.remove_machine_token", new=m.remove),
        patch(f"{TOKENS}.machine_state", new=m.state),
        patch("ptc_agent.agent.claude_code.cli_install.ensure_cli", new=m.install),
        patch("ptc_agent.agent.claude_code.runtime.token_works", new=m.works),
    ]
    with ExitStack() as stack:
        for p in patches:
            stack.enter_context(p)
        yield w


async def _read(client, path=None):
    """The list's body, or with no path the one computer's entry in it."""
    response = await client.get(path or "/api/v1/claude-code/computers")
    await asyncio.gather(*checks._checks)
    assert response.status_code == 200, response.text
    return response.json() if path else response.json()["computers"][0]


def _kept(cache):
    raw = cache.values.get(answer_key(CID))
    return MachineAnswer(**raw) if raw else None


def _entry(state, source=None, email=None, expires_at=None, own_file=None):
    return {
        "computer_id": CID, "name": "Studio Mac", "kind": "helper", "state": state, "source": source,
        "own_file": source == "machine" if own_file is None else own_file, "email": email, "expires_at": expires_at,
    }


def _flow(cache):
    cache.values[f"claude_code:sign_in:{USER}:{CID}"] = json.dumps({"verifier": "verifier", "state": "state-1"})


async def _error(response):
    return response.status_code, response.json()["error"]["code"]


# -- Status --


@pytest.mark.asyncio
@pytest.mark.parametrize(("rows", "opted_in"), [(False, True), (True, False)])
@pytest.mark.parametrize(("status", "live"), [("stopped", True), ("running", False)])
async def test_with_claude_code_off_a_computer_asleep_or_out_of_reach_reads_off_and_is_never_dialed(
    client, world, rows, opted_in, status, live,
):
    world.rows, world.opted_in = rows, opted_in
    world.computer, world.tunnel_live = _computer(status), live
    assert await _read(client, "/api/v1/claude-code/computers") == {"computers": [_entry("off")]}
    assert world.dialed == [] and world.machine.calls == [] and not world.cache.locks


@pytest.mark.asyncio
@pytest.mark.parametrize(("rows", "opted_in"), [(False, True), (True, False)])
async def test_with_claude_code_off_and_no_answer_kept_an_awake_computer_is_looked_at_once_never_checked(
    client, world, rows, opted_in,
):
    world.rows, world.opted_in = rows, opted_in
    world.machine.token = "sk-ant-oat01-mine"
    assert await _read(client) == _entry("off")
    assert world.machine.calls == ["state"], "a look reads the token file's state, never runs a check or installs"
    assert _kept(world.cache).file == "unknown" and not world.cache.locks
    assert await _read(client) == _entry("off", "machine")
    assert world.machine.calls == ["state"] and len(world.dialed) == 1, "a kept answer is read, not looked at again"


@pytest.mark.asyncio
async def test_with_claude_code_off_a_look_finding_no_token_names_none_and_one_not_reached_is_not_tried_again(
    client, world,
):
    world.opted_in = False
    assert await _read(client) == _entry("off")
    assert _kept(world.cache).file == "none"
    assert await _read(client) == _entry("off") and world.machine.calls == ["state"]
    world.cache.values.clear()
    world.dial_error = SandboxTransientError("connection refused")
    assert await _read(client) == _entry("off")
    assert await _read(client) == _entry("off")
    assert len(world.dialed) == 2 and world.cache.values[unreached_key(CID)] is True


@pytest.mark.asyncio
@pytest.mark.parametrize(("file", "source"), [("works", "machine"), ("refused", "machine"), ("invalid", "machine"), ("none", None)])
async def test_with_claude_code_off_a_computer_still_names_the_token_it_last_held(client, world, file, source):
    world.opted_in = False
    await machines.keep_answer(CID, MachineAnswer(file))
    assert await _read(client) == _entry("off", source)
    assert world.dialed == [] and world.machine.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(("status", "live"), [("stopped", True), ("running", False)])
async def test_a_computer_asleep_or_out_of_reach_is_never_dialed_for_status(client, world, status, live):
    world.computer, world.tunnel_live = _computer(status), live
    assert await _read(client) == _entry("offline")
    assert world.dialed == [] and not world.cache.locks


@pytest.mark.asyncio
async def test_a_computer_asleep_reads_as_it_last_answered(client, world):
    world.computer = _computer("stopped")
    world.cache.values[answer_key(CID)] = {"file": "works", "email": "user@example.com", "expires_at": None, "checked_at": 1.0}
    assert await _read(client) == _entry("signed_in", "machine", "user@example.com")
    assert world.dialed == []


@pytest.mark.asyncio
@pytest.mark.parametrize(("answer", "state"), [(True, "signed_in"), (False, "rejected"), (TimeoutError(), "unverified")])
async def test_an_awake_computer_is_checked_once_in_the_background(client, world, answer, state):
    world.machine.token, world.machine.answer = "sk-ant-oat01-mine", answer
    assert await _read(client) == _entry("checking")
    assert world.machine.calls == ["state", "check"]
    assert await _read(client) == _entry(state, "machine")
    assert world.machine.calls == ["state", "check"], "a kept answer is read, not checked again"
    assert not world.cache.locks


@pytest.mark.asyncio
async def test_a_listing_dials_with_the_computers_full_row_not_the_rosters(client, world):
    world.machine.token = "sk-ant-oat01-mine"
    await _read(client)
    assert [row.get("provider_ref") for row in world.dialed] == ["ref"]


@pytest.mark.asyncio
async def test_a_status_check_never_installs_the_cli_and_one_missing_reads_unverified(client, world):
    world.machine.token, world.machine.answer = "sk-ant-oat01-mine", CliMissing("not installed yet")
    assert await _read(client) == _entry("checking")
    assert await _read(client) == _entry("unverified", "machine")
    [launcher] = world.machine.checked
    assert isinstance(launcher, SSHLauncher) and launcher.install is False
    assert "install" not in world.machine.calls and _kept(world.cache).file == "unknown"


@pytest.mark.asyncio
async def test_a_status_checks_hold_outlives_its_work_which_is_stopped_at_its_budget(client, world, monkeypatch):
    world.machine.token = "sk-ant-oat01-mine"
    assert await _read(client) == _entry("checking") and not world.cache.locks
    world.cache.values.clear()
    held = []

    async def slow(launcher, model):
        held.append(dict(world.cache.lock_ttls))
        await asyncio.sleep(60)

    monkeypatch.setattr(machines, "STATUS_CHECK_S", 0.05)
    with patch("ptc_agent.agent.claude_code.runtime.token_works", new=slow):
        assert await _read(client) == _entry("checking")
    assert held == [{change_lock(CID): (0.05 + checks.MARGIN_S) * 1000}]
    assert _kept(world.cache) is None and not world.cache.locks
    assert machines.STATUS_CHECK_S >= 0.05
    monkeypatch.undo()
    assert machines.STATUS_CHECK_S == 2 * machines.MACHINE_IO_S + checks.CHECK_S + checks.CHECK_CLEANUP_S


@pytest.mark.asyncio
async def test_a_check_that_cannot_reach_the_machine_is_not_tried_again_at_once(client, world):
    world.dial_error = SandboxTransientError("connection refused")
    assert await _read(client) == _entry("checking")
    assert await _read(client) == _entry("offline")
    assert len(world.dialed) == 1 and world.cache.values[unreached_key(CID)] is True


@pytest.mark.asyncio
async def test_a_machine_that_answers_with_an_error_is_kept_as_an_answer_not_offline(client, world, caplog):
    world.machine.fail["state"] = RuntimeError("Could not read the token's state: secret-ish detail")
    caplog.set_level(logging.DEBUG)
    assert await _read(client) == _entry("checking")
    assert await _read(client) == _entry("unverified", "machine")
    assert unreached_key(CID) not in world.cache.values and _kept(world.cache).file == "unknown"
    assert "RuntimeError" in caplog.text and "secret-ish detail" not in caplog.text


@pytest.mark.asyncio
async def test_a_machine_without_a_token_uses_the_saved_one_only_where_it_is_relayed(client, world):
    world.saved = "sk-ant-oat01-saved"
    world.cache.values[token_check_key("sk-ant-oat01-saved")] = "works"
    assert await _read(client) == _entry("checking")
    assert await _read(client) == _entry("no_token")
    world.backend = True
    assert await _read(client) == _entry("signed_in", "saved")


@pytest.mark.asyncio
async def test_a_file_the_machine_refuses_beside_the_relayed_token_is_still_named_for_removal(client, world):
    """Turns there sign in with the saved token, so it is the one shown, and
    the file stays named, so Settings offers to remove it."""
    world.backend, world.saved, world.machine.invalid = True, "sk-ant-oat01-saved", True
    world.cache.values[token_check_key("sk-ant-oat01-saved")] = "works"
    assert await _read(client) == _entry("checking")
    assert await _read(client) == _entry("signed_in", "saved", own_file=True)


@pytest.mark.asyncio
async def test_a_machine_without_perl_is_not_offered_the_saved_token(client, world):
    """The shim a relayed token goes through is perl, so a turn there could only fail."""
    world.backend, world.saved, world.machine.perl = True, "sk-ant-oat01-saved", False
    world.cache.values[token_check_key("sk-ant-oat01-saved")] = "works"
    assert await _read(client) == _entry("checking")
    assert await _read(client) == _entry("needs_perl")
    assert _kept(world.cache).file == "no_perl"


@pytest.mark.asyncio
@pytest.mark.parametrize("token", [None, "sk-ant-oat01-mine"])
async def test_a_machine_on_musl_is_not_offered_the_saved_token_nor_checked(client, world, token):
    """One registered before the probe refused musl: no build of the CLI runs
    there, so a turn could only fail, whichever token it signed in with."""
    world.backend, world.saved, world.machine.musl, world.machine.token = True, "sk-ant-oat01-saved", True, token
    world.cache.values[token_check_key("sk-ant-oat01-saved")] = "works"
    assert await _read(client) == _entry("checking")
    assert await _read(client) == _entry("computer_unsupported", "machine" if token else None)
    kept = _kept(world.cache)
    assert (kept.file, kept.names_file, kept.relays) == ("unsupported", token is not None, False)
    assert "check" not in world.machine.calls


@pytest.mark.asyncio
async def test_a_token_left_where_perl_is_missing_is_offered_for_removal_and_never_relayed_beside(client, world):
    """No turn can use it there, so the computer reads as needing perl, not as
    one Claude refused, with the token named so Settings offers to remove it,
    and the saved token is still not relayed."""
    world.backend, world.saved = True, "sk-ant-oat01-saved"
    world.cache.values[token_check_key("sk-ant-oat01-saved")] = "works"
    world.machine.perl, world.machine.token = False, "sk-ant-oat01-mine"
    assert await _read(client) == _entry("checking")
    assert await _read(client) == _entry("needs_perl", "machine")
    kept = _kept(world.cache)
    assert (kept.file, kept.names_file, kept.relays, kept.folder) == ("no_perl", True, False, FOLDER)


@pytest.mark.asyncio
async def test_a_turns_refusal_while_a_status_check_runs_is_not_overwritten_by_the_check(client, world):
    world.machine.token = "sk-ant-oat01-mine"

    async def refused_meanwhile(launcher, model):
        await machines.ComputerToken(USER, CID).refused("file")
        return True

    with patch("ptc_agent.agent.claude_code.runtime.token_works", new=refused_meanwhile):
        assert await _read(client) == _entry("checking")
    assert _kept(world.cache).file == "refused"
    assert await _read(client) == _entry("rejected", "machine")


# -- Placing a token --


@pytest.mark.asyncio
async def test_a_pasted_token_is_staged_checked_there_then_put_in_place_and_kept_as_an_answer_only(client, world):
    world.machine.token = "sk-ant-oat01-old"
    response = await client.put(f"{BASE}/token", json={"token": "  sk-ant-oat01-pasted\n"})
    assert response.status_code == 200, response.text
    body = response.json()
    assert datetime.fromisoformat(body.pop("expires_at")) > datetime.now(timezone.utc)
    assert body == {k: v for k, v in _entry("signed_in", "machine").items() if k != "expires_at"}
    assert (world.machine.token, world.machine.staged) == ("sk-ant-oat01-pasted", None)
    assert world.machine.calls == ["facts", "install", "write", "check", "promote"]
    [launcher] = world.machine.checked
    assert (launcher.machine, launcher.identity, launcher.relay, launcher.install) == (world.handle, USER, None, False)
    assert launcher.token_file == STAGED, "the check signs in with the staged token, not the one in use"
    assert launcher.slot_wait_s == START_TIMEOUT_S, "a check waits only briefly for a CLI slot"
    assert _kept(world.cache).file == "works"
    assert not world.cache.locks


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("answer", "status", "code"), [(False, 400, "token_rejected"), (TimeoutError(), 503, "check_unavailable")],
)
async def test_a_replacement_claude_does_not_serve_leaves_the_token_in_use_as_it_was(client, world, answer, status, code):
    world.machine.token, world.machine.answer = "sk-ant-oat01-old", answer
    kept = {"file": "works", "email": "a@example.com", "expires_at": None, "checked_at": 1.0}
    world.cache.values[answer_key(CID)] = dict(kept)
    response = await client.put(f"{BASE}/token", json={"token": "sk-ant-oat01-pasted"})
    assert await _error(response) == (status, code)
    assert (world.machine.token, world.machine.staged) == ("sk-ant-oat01-old", None)
    assert world.machine.calls == ["facts", "install", "write", "check", "discard"]
    assert world.cache.values[answer_key(CID)] == kept, "what the machine last said still stands"


@pytest.mark.asyncio
async def test_a_save_cancelled_mid_check_removes_only_its_staged_token(world):
    world.machine.token = "sk-ant-oat01-old"
    checking = asyncio.Event()

    async def hangs(launcher, model):
        checking.set()
        await asyncio.sleep(60)

    home = await machines.MachineHome.of(USER, CID)
    with patch("ptc_agent.agent.claude_code.runtime.token_works", new=hangs):
        save = asyncio.create_task(flow.paste(home, "sk-ant-oat01-pasted", OAuthRefusal(400, "token_rejected", "no")))
        await checking.wait()
        save.cancel()
        with pytest.raises(asyncio.CancelledError):
            await save
    assert (world.machine.token, world.machine.staged) == ("sk-ant-oat01-old", None)
    assert world.machine.calls[-1] == "discard" and not world.cache.locks


@pytest.mark.asyncio
async def test_a_computer_removed_mid_check_never_gets_its_token_back(client, world):
    async def removed_meanwhile(launcher, model):
        world.computer = None
        return True

    with patch("ptc_agent.agent.claude_code.runtime.token_works", new=removed_meanwhile):
        response = await client.put(f"{BASE}/token", json={"token": "sk-ant-oat01-pasted"})
    assert await _error(response) == (404, "computer_not_found")
    assert world.machine.calls[-1] == "discard" and "promote" not in world.machine.calls
    assert (world.machine.token, world.machine.staged) == (None, None)


@pytest.mark.asyncio
async def test_a_staged_token_that_cannot_be_put_in_place_is_removed_and_said(client, world):
    world.machine.token = "sk-ant-oat01-old"
    world.machine.fail["promote"] = OSError("the machine said no")
    response = await client.put(f"{BASE}/token", json={"token": "sk-ant-oat01-pasted"})
    assert await _error(response) == (502, "write_failed")
    assert world.machine.calls[-2:] == ["promote", "discard"] and world.machine.token == "sk-ant-oat01-old"
    assert _kept(world.cache) is None, "the next read asks the machine"


@pytest.mark.asyncio
async def test_a_write_that_fails_is_not_checked(client, world):
    world.machine.fail["write"] = OSError("disk full")
    response = await client.put(f"{BASE}/token", json={"token": "sk-ant-oat01-pasted"})
    assert await _error(response) == (502, "write_failed")
    assert world.machine.calls == ["facts", "install", "write"]


@pytest.mark.asyncio
async def test_the_cli_is_placed_before_the_computers_hold_is_taken(client, world):
    held_while_installing = []
    install = world.machine.install

    async def watching(link, root, **kw):
        held_while_installing.append(dict(world.cache.locks))
        return await install(link, root, **kw)

    with patch("ptc_agent.agent.claude_code.cli_install.ensure_cli", new=watching):
        response = await client.put(f"{BASE}/token", json={"token": "sk-ant-oat01-pasted"})
    assert response.status_code == 200, response.text
    assert held_while_installing == [{}]


@pytest.mark.asyncio
async def test_a_cli_that_cannot_be_placed_leaves_the_sign_in_unspent(client, world):
    _flow(world.cache)
    world.machine.fail["install"] = RuntimeError("no build for this platform")
    with patch.object(claude_oauth, "mint_setup_token", AsyncMock(return_value=MINTED)) as mint:
        response = await client.post(f"{BASE}/sign-in/code", json={"code": "code#state-1"})
    assert await _error(response) == (503, "cli_unavailable")
    mint.assert_not_awaited()
    assert world.machine.calls == ["facts", "install"] and f"claude_code:sign_in:{USER}:{CID}" in world.cache.values


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["paste", "sign-in"])
async def test_a_machine_on_musl_is_told_so_before_anything_is_spent(client, world, path):
    _flow(world.cache)
    world.machine.fail["install"] = CliUnsupported(MUSL_UNSUPPORTED)
    with patch.object(claude_oauth, "mint_setup_token", AsyncMock(return_value=MINTED)) as mint:
        if path == "paste":
            response = await client.put(f"{BASE}/token", json={"token": "sk-ant-oat01-pasted"})
        else:
            response = await client.post(f"{BASE}/sign-in/code", json={"code": "code#state-1"})
    assert response.status_code == 400
    assert response.json()["error"] == {"code": "computer_unsupported", "message": MUSL_UNSUPPORTED, "retryable": False}
    mint.assert_not_awaited()
    assert world.machine.calls == ["facts", "install"]


@pytest.mark.asyncio
@pytest.mark.parametrize("token", [None, "sk-ant-oat01-mine"])
@pytest.mark.parametrize("path", ["paste", "sign-in"])
async def test_a_machine_without_perl_is_told_so_before_anything_is_spent(client, world, path, token):
    _flow(world.cache)
    world.machine.perl, world.machine.token = False, token
    with patch.object(claude_oauth, "mint_setup_token", AsyncMock(return_value=MINTED)) as mint:
        if path == "paste":
            response = await client.put(f"{BASE}/token", json={"token": "sk-ant-oat01-pasted"})
        else:
            response = await client.post(f"{BASE}/sign-in/code", json={"code": "code#state-1"})
    assert response.status_code == 400
    assert response.json()["error"] == {
        "code": "needs_perl", "message": "Claude Code needs perl on this computer. Install perl, then try again.",
        "retryable": True,
    }
    mint.assert_not_awaited()
    assert world.machine.calls == ["facts"] and f"claude_code:sign_in:{USER}:{CID}" in world.cache.values
    assert await _read(client) == _entry("needs_perl", "machine" if token else None), (
        "Settings says so from then on, offering to remove a token left there"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("token", [None, "sk-ant-oat01-mine"])
@pytest.mark.parametrize("path", ["paste", "sign-in"])
async def test_a_machine_found_on_musl_as_it_is_readied_reads_so_from_then_on(client, world, path, token):
    _flow(world.cache)
    world.machine.musl, world.machine.token = True, token
    with patch.object(claude_oauth, "mint_setup_token", AsyncMock(return_value=MINTED)) as mint:
        if path == "paste":
            response = await client.put(f"{BASE}/token", json={"token": "sk-ant-oat01-pasted"})
        else:
            response = await client.post(f"{BASE}/sign-in/code", json={"code": "code#state-1"})
    assert response.status_code == 400
    assert response.json()["error"] == {"code": "computer_unsupported", "message": MUSL_UNSUPPORTED, "retryable": False}
    mint.assert_not_awaited()
    assert world.machine.calls == ["facts"]
    assert await _read(client) == _entry("computer_unsupported", "machine" if token else None), "Settings says so from then on"


# -- The organization's managed Claude Code settings --


@pytest.mark.asyncio
@pytest.mark.parametrize(("token", "source"), [("sk-ant-oat01-mine", "machine"), (None, None)])
@pytest.mark.parametrize("managed", [("settings",), ("dropins",), ("mcp",), ("profile",)])
async def test_a_computer_with_any_organizations_policy_installed_cannot_run_claude_code(client, world, token, source, managed):
    """No check is run there, and a token left there is named only so it can be removed."""
    world.machine.token, world.machine.managed, world.backend, world.saved = token, managed, True, "sk-ant-oat01-saved"
    await _read(client)
    assert await _read(client) == _entry("policy_blocked", source)
    assert world.machine.calls == ["state"]
    kept = _kept(world.cache)
    assert (kept.file, kept.present, kept.fresh) == ("policy", token is not None, True)
    world.cache.values[answer_key(CID)]["checked_at"] -= checks.SHORT_TTL_S + 1
    assert not _kept(world.cache).fresh, "checked again shortly, so a policy lifted there shows soon"


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["paste", "sign-in"])
async def test_a_machine_with_an_organizations_policy_installed_is_told_so_before_anything_is_spent(client, world, path):
    _flow(world.cache)
    world.machine.managed = ("settings",)
    with patch.object(claude_oauth, "mint_setup_token", AsyncMock(return_value=MINTED)) as mint:
        if path == "paste":
            response = await client.put(f"{BASE}/token", json={"token": "sk-ant-oat01-pasted"})
        else:
            response = await client.post(f"{BASE}/sign-in/code", json={"code": "code#state-1"})
    assert response.status_code == 400
    assert response.json()["error"] == {"code": "policy_blocked", "message": machines.POLICY_BLOCKED, "retryable": False}
    assert machines.POLICY_BLOCKED == (
        "Your organization's Claude Code policy is installed on this computer, so LangAlpha can't run Claude Code here."
    )
    mint.assert_not_awaited()
    assert world.machine.calls == ["facts"], "the CLI never installed"
    assert await _read(client) == _entry("policy_blocked"), "and the computer reads so from then on"
    assert world.machine.calls == ["facts"]


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["paste", "sign-in"])
@pytest.mark.parametrize("error", [RuntimeError("the machine said no"), TimeoutError()])
async def test_a_machine_that_cannot_say_what_claude_code_needs_there_is_refused_before_anything_is_spent(
    client, world, path, error,
):
    _flow(world.cache)
    world.machine.fail["facts"] = error
    with patch.object(claude_oauth, "mint_setup_token", AsyncMock(return_value=MINTED)) as mint:
        if path == "paste":
            response = await client.put(f"{BASE}/token", json={"token": "sk-ant-oat01-pasted"})
        else:
            response = await client.post(f"{BASE}/sign-in/code", json={"code": "code#state-1"})
    assert await _error(response) == (503, "cli_unavailable")
    mint.assert_not_awaited()
    assert world.machine.calls == ["facts"] and f"claude_code:sign_in:{USER}:{CID}" in world.cache.values


@pytest.mark.asyncio
@pytest.mark.parametrize("kept", [None, MachineAnswer("none")], ids=["no-answer", "answer-without-folder"])
async def test_a_token_placed_keeps_the_folder_the_machine_put_it_in(client, world, kept):
    """So a removal that cannot reach the machine names the token's real folder,
    though no status check named it first."""
    if kept is not None:
        world.cache.values[answer_key(CID)] = {**asdict(kept), "checked_at": time.time()}
    response = await client.put(f"{BASE}/token", json={"token": "sk-ant-oat01-pasted"})
    assert response.status_code == 200, response.text
    assert _kept(world.cache).folder == FOLDER


@pytest.mark.asyncio
@pytest.mark.parametrize(("before", "now"), [(("settings",), ()), ((), ("settings",))])
async def test_a_removal_keeps_the_policy_the_machine_shows_now_not_the_one_last_kept(client, world, before, now):
    """A computer blocked by a policy since lifted is usable once its token is removed, and the other way round."""
    world.machine.token, world.machine.managed = "sk-ant-oat01-mine", now
    await machines.keep_answer(CID, MachineAnswer("policy" if before else "unknown"))
    assert (await client.delete(f"{BASE}/token")).status_code == 204
    assert _kept(world.cache).file == ("policy" if now else "none")
    assert world.machine.calls == ["remove", "state"]


@pytest.mark.asyncio
async def test_a_removal_the_machine_cannot_say_the_state_after_leaves_it_to_the_next_read(client, world):
    world.machine.token, world.machine.fail["state"] = "sk-ant-oat01-mine", RuntimeError("the machine said no")
    await machines.keep_answer(CID, MachineAnswer("policy"))
    assert (await client.delete(f"{BASE}/token")).status_code == 204
    assert world.machine.token is None and _kept(world.cache) is None


@pytest.mark.asyncio
async def test_a_computer_that_cannot_be_reached_is_told_so_and_nothing_is_written(client, world):
    world.dial_error = TimeoutError()
    response = await client.put(f"{BASE}/token", json={"token": "sk-ant-oat01-pasted"})
    assert await _error(response) == (503, "computer_unreachable")
    assert world.machine.calls == []


@pytest.mark.asyncio
async def test_one_change_per_computer_at_a_time(client, world):
    world.cache.locks[change_lock(CID)] = "someone-else"
    response = await client.put(f"{BASE}/token", json={"token": "sk-ant-oat01-pasted"})
    assert await _error(response) == (409, "check_in_progress")
    assert world.machine.calls == ["facts", "install"], "placed before the hold, never written under another change"


@pytest.mark.asyncio
async def test_a_save_whose_hold_lapsed_mid_check_keeps_no_answer(client, world):
    async def lapses(launcher, model):
        world.cache.locks[change_lock(CID)] = "a-later-change"
        return True

    with patch("ptc_agent.agent.claude_code.runtime.token_works", new=lapses):
        response = await client.put(f"{BASE}/token", json={"token": "sk-ant-oat01-pasted"})
    assert await _error(response) == (409, "check_in_progress")
    assert _kept(world.cache) is None
    assert world.machine.calls[-1] == "discard" and world.machine.token is None, "nothing is put in place under another change"


@pytest.mark.asyncio
async def test_another_users_computer_or_a_hosted_one_is_not_found(client, world):
    world.computer = _computer(user_id="someone-else")
    assert (await client.put(f"{BASE}/token", json={"token": "t"})).status_code == 404
    world.computer = _computer(kind="daytona")
    assert (await client.put(f"{BASE}/token", json={"token": "t"})).status_code == 404
    assert world.machine.calls == []


# -- Sign-in --


@pytest.mark.asyncio
async def test_a_signed_in_token_is_placed_with_its_account(client, world):
    _flow(world.cache)
    with patch.object(claude_oauth, "mint_setup_token", AsyncMock(return_value=MINTED)) as mint:
        response = await client.post(f"{BASE}/sign-in/code", json={"code": "code#state-1"})
    assert response.status_code == 200, response.text
    assert response.json() == _entry("signed_in", "machine", "user@example.com", "2027-10-29T00:00:00Z")
    assert mint.await_args.args == ("code", "state-1", "verifier")
    assert world.machine.token == MINTED.access_token


@pytest.mark.asyncio
async def test_a_second_paste_of_a_saved_sign_in_reads_the_kept_answer(client, world):
    _flow(world.cache)
    mint = AsyncMock(return_value=MINTED)
    with patch.object(claude_oauth, "mint_setup_token", mint):
        first = await client.post(f"{BASE}/sign-in/code", json={"code": "code#state-1"})
        again = await client.post(f"{BASE}/sign-in/code", json={"code": "code#state-1"})
    assert first.json() == again.json() == _entry("signed_in", "machine", "user@example.com", "2027-10-29T00:00:00Z")
    mint.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_sign_in_for_a_computer_that_is_off_keeps_its_code_unspent(client, world):
    _flow(world.cache)
    world.dial_error = TimeoutError()
    with patch.object(claude_oauth, "mint_setup_token", AsyncMock(return_value=MINTED)) as mint:
        response = await client.post(f"{BASE}/sign-in/code", json={"code": "code#state-1"})
    assert response.status_code == 503
    mint.assert_not_awaited()
    assert f"claude_code:sign_in:{USER}:{CID}" in world.cache.values


@pytest.mark.asyncio
async def test_a_refused_exchange_writes_nothing(client, world):
    _flow(world.cache)
    with patch.object(claude_oauth, "mint_setup_token", AsyncMock(side_effect=claude_oauth.UnexpectedGrant("scope=None"))):
        response = await client.post(f"{BASE}/sign-in/code", json={"code": "code#state-1"})
    assert response.status_code == 502
    assert world.machine.calls == ["facts", "install"]


# -- Removing --


@pytest.mark.asyncio
async def test_remove_works_with_claude_code_off_for_the_user(client, world):
    world.opted_in, world.machine.token = False, "sk-ant-oat01-mine"
    response = await client.delete(f"{BASE}/token")
    assert response.status_code == 204
    assert world.machine.token is None and _kept(world.cache).file == "none"
    assert world.machine.calls == ["remove", "state"], "a removal neither needs perl nor installs anything"


@pytest.mark.asyncio
async def test_remove_works_with_claude_code_off_on_the_server_and_no_sdk_installed(client, world):
    """Settings offers Remove there, so it must work wherever a token was left."""
    world.rows, world.machine.token = False, "sk-ant-oat01-mine"
    with patch.dict(sys.modules, {"claude_agent_sdk": None}):
        response = await client.delete(f"{BASE}/token")
    assert response.status_code == 204, response.text
    assert world.machine.token is None and world.machine.calls == ["remove", "state"]


@pytest.mark.asyncio
async def test_a_removals_hold_covers_both_its_scripts_and_one_that_lapsed_keeps_no_answer(client, world):
    """The removal and the read after it run up to MACHINE_IO_S each; a hold
    lapsed by then may have let a save in, whose answer the removal's must not replace."""
    world.machine.token = "sk-ant-oat01-mine"
    state = world.machine.state

    async def lapses(link, computer_id):
        world.cache.locks[change_lock(CID)] = "a-later-change"
        return await state(link, computer_id)

    await machines.keep_answer(CID, MachineAnswer("works"))
    with patch(f"{TOKENS}.machine_state", new=lapses):
        assert (await client.delete(f"{BASE}/token")).status_code == 204
    assert world.cache.lock_ttls[change_lock(CID)] == (2 * flow.MACHINE_IO_S + checks.MARGIN_S) * 1000
    assert world.machine.calls == ["remove", "state"] and world.machine.token is None and _kept(world.cache) is None


@pytest.mark.asyncio
async def test_remove_still_checks_the_computer_is_the_users_own(client, world):
    world.computer = _computer(user_id="someone-else")
    assert (await client.delete(f"{BASE}/token")).status_code == 404
    assert world.machine.calls == []


# -- Token hygiene --

SENTINEL = "sk-ant-oat01-SENTINEL-0123456789"


def _chain(exc):
    """An exception and every one a traceback of it prints."""
    while exc is not None:
        yield exc
        exc = exc.__cause__ or (None if exc.__suppress_context__ else exc.__context__)


def _paths():
    """Every way a token reaches the backend for a computer, each ending where it ends."""

    def mint(effect):
        return patch.object(claude_oauth, "mint_setup_token", AsyncMock(side_effect=effect))

    def leak(kind):
        return kind(f"failed on {SENTINEL}")

    minted = claude_oauth.SetupToken(access_token=SENTINEL, expires_at=MINTED.expires_at, token_uuid="u", email=None)
    paste = ("PUT", f"{BASE}/token", {"token": SENTINEL})
    code = ("POST", f"{BASE}/sign-in/code", {"code": "code#state-1"})
    return {
        "saved": (paste, {}, None),
        "check_refuses": (paste, {"answer": False}, None),
        "check_unreachable": (paste, {"answer": leak(TimeoutError)}, None),
        "write_fails": (paste, {"write": leak(OSError)}, None),
        "discard_fails": (paste, {"answer": False, "discard": leak(OSError)}, None),
        "promote_fails": (paste, {"promote": leak(OSError)}, None),
        "machine_unreachable": (paste, {"dial": leak(ConnectionError)}, None),
        "install_fails": (paste, {"install": leak(RuntimeError)}, None),
        "signed_in": (code, {}, mint([minted])),
        "signed_in_check_refuses": (code, {"answer": False}, mint([minted])),
        "mint_fails": (code, {}, mint(leak(RuntimeError))),
        "mint_unexpected": (code, {}, mint(claude_oauth.UnexpectedGrant("scope=None"))),
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("path", list(_paths()))
async def test_a_token_reaches_no_log_response_error_or_cache(world, caplog, path):
    raised = []

    def answer(_, exc):
        raised.append(exc)
        return JSONResponse({"detail": exc.detail, "error": exc.error}, exc.status_code)

    (method, url, body), knobs, minting = _paths()[path]
    world.machine.answer = knobs.get("answer", True)
    for op in ("write", "promote", "discard", "remove", "install"):
        if op in knobs:
            world.machine.fail[op] = knobs[op]
    world.dial_error = knobs.get("dial")
    _flow(world.cache)
    caplog.set_level(logging.DEBUG)
    with structlog.testing.capture_logs() as logged, minting or patch.object(machines, "KEPT_TTL_S", machines.KEPT_TTL_S):
        async with AsyncClient(transport=ASGITransport(app=_app(answer)), base_url="http://test") as client:
            response = await client.request(method, url, json=body)
    assert response.status_code != 500, response.text
    assert SENTINEL not in response.text
    assert SENTINEL not in caplog.text
    assert not any(SENTINEL in str(r.args) or SENTINEL in r.getMessage() for r in caplog.records)
    assert SENTINEL not in repr(logged)
    for exc in raised:
        for link in _chain(exc):
            assert SENTINEL not in str(link) and SENTINEL not in repr(link)
            assert SENTINEL not in repr(getattr(link, "error", "")) and SENTINEL not in repr(getattr(link, "detail", ""))
    stored = json.dumps([world.cache.values, world.cache.locks], default=str)
    assert SENTINEL not in stored
    assert token_check_key(SENTINEL) not in world.cache.values, "a machine's token is not checked by the saved-token path"
