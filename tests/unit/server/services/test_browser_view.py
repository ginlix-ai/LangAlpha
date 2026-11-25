"""Which state a computer's browser is reported in, and that reading starts nothing."""

from __future__ import annotations

import asyncio
import contextlib
import json
import shlex
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest

from ptc_agent.core.sandbox.runtime import PreviewInfo, RuntimeState
from src.server.services import browser_view as module
from ptc_agent.core.sandbox.browser_runtime import protocol
from src.server.services.toolset import Answer
from src.server.services.browser_view import (
    VIEW_LIFETIME_S,
    browser_control,
    browser_navigate,
    browser_view,
)


def _sandbox(
    stdout: str = "",
    *,
    exec_error: Exception | None = None,
    url: str = "https://p.example.com/?t=1",
    state: RuntimeState = RuntimeState.RUNNING,
):
    runtime = SimpleNamespace(
        exec=AsyncMock(side_effect=exec_error, return_value=SimpleNamespace(stdout=stdout)),
        refresh_state=AsyncMock(return_value=state),
    )
    return SimpleNamespace(
        sandbox_id="sb-1",
        installs_browser=False,
        layout=SimpleNamespace(internal_src="/r/_internal/src"),
        runtime=runtime,
        get_preview_url=AsyncMock(
            return_value=PreviewInfo(url=url, token="signed", browser_ready=True)
        ),
    )


TOKEN = "A" * 42 + "_"


def _status(**fields) -> str:
    return json.dumps({"installed": True, "running": False, "token": None, **fields})


def _own_machine(*answers: object):
    """A sandbox of a kind whose machines set the browser up themselves."""
    sandbox = _sandbox()
    sandbox.installs_browser = True
    sandbox.runtime = SimpleNamespace(
        exec=AsyncMock(
            side_effect=[
                SimpleNamespace(stdout=a if isinstance(a, str) else json.dumps(a)) for a in answers
            ]
        ),
        refresh_state=AsyncMock(return_value=RuntimeState.RUNNING),
    )
    return sandbox


def _asked(sandbox) -> list[str]:
    return [shlex.split(call.args[0])[3:] for call in sandbox.runtime.exec.await_args_list]


@pytest.fixture(autouse=True)
def toolset(monkeypatch):
    """The computer's tool update, which answers nothing unless a test says."""
    stand_in = SimpleNamespace(
        ask_status=AsyncMock(return_value=None),
        # Starts one unless the version it is told is the table's, "t-new".
        converge_soon=Mock(side_effect=lambda sandbox, computer_id, known: known != "t-new"),
    )
    monkeypatch.setattr(module, "toolset", stand_in)
    return stand_in


@pytest.mark.asyncio
async def test_a_cloud_computer_without_the_engine_was_built_before_the_browser():
    sandbox = _sandbox(_status(installed=False))
    assert (await browser_view(sandbox, "ws-1", "c-1")).state == "outdated"
    assert sandbox.runtime.exec.await_count == 1


@pytest.mark.asyncio
async def test_a_cloud_computer_whose_tools_are_updating_is_installing(toolset):
    toolset.ask_status.return_value = Answer(None, converging=True)
    assert (await browser_view(_sandbox(_status(installed=False)), "ws-1", "c-1")).state == "installing"
    toolset.converge_soon.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("version", [None, "t-old"])
async def test_a_cloud_computer_behind_the_server_s_tools_is_updated_and_installing(toolset, version):
    toolset.ask_status.return_value = Answer(version)
    sandbox = _sandbox(_status(installed=False))
    assert (await browser_view(sandbox, "ws-1", "c-1")).state == "installing"
    toolset.converge_soon.assert_called_once_with(sandbox, "c-1", version)


@pytest.mark.asyncio
async def test_a_cloud_computer_current_and_still_without_the_engine_is_outdated(toolset):
    toolset.ask_status.return_value = Answer("t-new")
    assert (await browser_view(_sandbox(_status(installed=False)), "ws-1", "c-1")).state == "outdated"
    assert toolset.converge_soon.call_args.args[2] == "t-new"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("seen", "state", "known"),
    [
        ({"version": "t-old", "converging": False}, "installing", "t-old"),
        ({"version": None, "converging": False}, "installing", None),
        ({"version": "t-new", "converging": False}, "outdated", "t-new"),
        ({"version": "t-old", "converging": True}, "installing", "skip"),
    ],
)
async def test_the_browser_s_own_status_says_what_the_tools_hold_in_one_call(toolset, seen, state, known):
    sandbox = _sandbox(_status(installed=False, toolset=seen))
    assert (await browser_view(sandbox, "ws-1", "c-1")).state == state
    assert sandbox.runtime.exec.await_count == 1
    toolset.ask_status.assert_not_awaited()
    if known == "skip":
        toolset.converge_soon.assert_not_called()
    else:
        toolset.converge_soon.assert_called_once_with(sandbox, "c-1", known)


@pytest.mark.asyncio
async def test_a_runtime_that_does_not_report_the_tools_has_root_asked(toolset):
    toolset.ask_status.return_value = Answer("t-old")
    sandbox = _sandbox(_status(installed=False))
    assert (await browser_view(sandbox, "ws-1", "c-1")).state == "installing"
    toolset.ask_status.assert_awaited_once_with(sandbox.runtime, "/r/_internal/src")
    toolset.converge_soon.assert_called_once_with(sandbox, "c-1", "t-old")


@pytest.mark.asyncio
async def test_a_tool_status_that_fails_leaves_the_computer_outdated(toolset):
    toolset.ask_status.side_effect = RuntimeError("no answer")
    assert (await browser_view(_sandbox(_status(installed=False)), "ws-1", "c-1")).state == "outdated"
    toolset.converge_soon.assert_not_called()


@pytest.mark.asyncio
async def test_a_computer_with_the_browser_or_of_the_person_s_own_never_asks_for_tools(toolset):
    await browser_view(_sandbox(_status()), "ws-1", "c-1")
    sandbox = _own_machine(_status(installed=False), {"done": False, "reason": "unsupported"})
    assert (await browser_view(sandbox, "ws-1", "c-1")).state == "unavailable"
    toolset.ask_status.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_machine_of_the_person_s_own_with_no_browser_says_so():
    sandbox = _own_machine(
        _status(installed=False), {"done": False, "reason": "no_chrome", "chrome": None}
    )
    assert (await browser_view(sandbox, "ws-1", "c-1")).state == "no_chrome"
    assert _asked(sandbox) == [["status"], ["install"]]


@pytest.mark.asyncio
async def test_only_the_person_asking_downloads_chrome_for_testing():
    sandbox = _own_machine(_status(installed=False), {"done": True})
    assert (await browser_view(sandbox, "ws-1", "c-1", get_chrome=True)).state == "installing"
    assert _asked(sandbox) == [["status"], ["install", "chrome"]]


@pytest.mark.asyncio
async def test_asking_for_chrome_behind_a_setup_under_way_queues_the_download():
    """The setup may be one an acquire began, which downloads nothing; the
    machine runs one install at a time, and the queued one finds Chrome there
    after a download."""
    sandbox = _own_machine(_status(installed=False, installing=True), {"done": True})
    assert (await browser_view(sandbox, "ws-1", "c-1", get_chrome=True)).state == "installing"
    assert _asked(sandbox) == [["status"], ["install", "chrome"]]


@pytest.mark.asyncio
async def test_a_machine_setting_up_is_left_to_it():
    sandbox = _own_machine(_status(installed=False, installing=True))
    assert (await browser_view(sandbox, "ws-1", "c-1")).state == "installing"
    assert _asked(sandbox) == [["status"]]


@pytest.mark.asyncio
async def test_a_machine_set_up_while_the_person_waits_is_watched_on_its_own_port():
    sandbox = _own_machine(
        _status(installed=False),
        {"done": True, "reason": None, "chrome": "/opt/google/chrome/chrome"},
        _status(running=True, token=TOKEN, port=22097),
    )
    view = await browser_view(sandbox, "ws-1", "c-1")
    assert view.state == "running"
    assert view.url.startswith(f"https://p.example.com{protocol.PATH_PREFIX}{TOKEN}")
    assert sandbox.get_preview_url.await_args.args[0] == 22097


@pytest.mark.asyncio
async def test_a_cloud_computer_is_watched_on_the_image_s_port_whatever_it_reports():
    """Its agent can write the port it reports, and a preview link, which
    members may hold, must not open another."""
    sandbox = _sandbox(_status(running=True, token=TOKEN, port=22097))
    view = await browser_view(sandbox, "ws-1", "c-1")
    assert view.state == "running"
    assert sandbox.get_preview_url.await_args.args[0] == protocol.BRIDGE_PORT


@pytest.mark.asyncio
async def test_a_machine_that_stopped_and_so_did_not_answer_is_stopped():
    sandbox = _sandbox(exec_error=RuntimeError("gone"), state=RuntimeState.STOPPED)
    assert (await browser_view(sandbox, "ws-1", "c-1")).state == "stopped"


@pytest.mark.parametrize(("start", "state"), [(False, RuntimeState.RUNNING), (True, RuntimeState.STOPPED)])
@pytest.mark.asyncio
async def test_a_running_computer_that_did_not_answer_is_an_error_not_a_stopped_one(start, state):
    """A stopped answer offers to start a computer that is up; an error is asked
    again. A start has just brought the computer up, whatever its state reads."""
    sandbox = _sandbox(exec_error=TimeoutError(), state=state)
    with pytest.raises(TimeoutError):
        await browser_view(sandbox, "ws-1", "c-1", start=start)


@pytest.mark.asyncio
async def test_a_computer_that_stops_while_its_link_is_made_is_stopped():
    sandbox = _sandbox(_status(running=True, token=TOKEN), state=RuntimeState.STOPPED)
    sandbox.get_preview_url.side_effect = RuntimeError("sandbox is not running")
    assert (await browser_view(sandbox, "ws-1", "c-1")).state == "stopped"


@pytest.mark.asyncio
async def test_a_link_that_fails_on_a_running_computer_is_still_an_error():
    sandbox = _sandbox(_status(running=True, token=TOKEN))
    sandbox.get_preview_url.side_effect = RuntimeError("no preview")
    with pytest.raises(RuntimeError, match="no preview"):
        await browser_view(sandbox, "ws-1", "c-1")


@pytest.mark.parametrize("stdout", ["", "python3: can't open file", _status(installed=False)])
@pytest.mark.asyncio
async def test_a_computer_older_than_the_browser_is_outdated(stdout: str):
    assert (await browser_view(_sandbox(stdout), "ws-1", "c-1")).state == "outdated"


@pytest.mark.parametrize(
    "fields",
    [
        {},
        {"running": True},
        {"token": TOKEN},
        # Only a bridge's own token reaches a URL's path.
        {"running": True, "token": "t"},
        {"running": True, "token": "../../x?y=" + "A" * 33},
        {"running": True, "token": TOKEN + "\n"},
    ],
)
@pytest.mark.asyncio
async def test_no_browser_open_is_idle(fields: dict):
    assert (await browser_view(_sandbox(_status(**fields)), "ws-1", "c-1")).state == "idle"


@pytest.mark.asyncio
async def test_an_open_refused_for_memory_says_so():
    assert (await browser_view(_sandbox(_status(low_memory=True)), "ws-1", "c-1", start=True)).state == "low_memory"
    # A browser that is up is shown whatever else the answer says.
    up = await browser_view(_sandbox(_status(running=True, token=TOKEN, low_memory=True)), "ws-1", "c-1")
    assert up.state == "running"


@pytest.mark.asyncio
async def test_an_app_on_the_owners_own_machine_may_open_the_stream_past_the_server():
    """Its bridge listens on the machine's loopback, behind the same token."""
    sandbox = _own_machine(_status(running=True, token=TOKEN, port=22097))
    view = await browser_view(sandbox, "ws-1", "c-1")
    assert view.local_url == f"http://127.0.0.1:22097{protocol.PATH_PREFIX}{TOKEN}"


@pytest.mark.asyncio
async def test_no_loopback_for_a_cloud_computer_or_a_shared_workspaces_member():
    cloud = await browser_view(_sandbox(_status(running=True, token=TOKEN)), "ws-1", "c-1")
    assert cloud.state == "running" and cloud.local_url is None
    sandbox = _own_machine(_status(running=True, token=TOKEN, port=22097))
    with (
        patch.object(module, "preview_base", return_value=None),
        patch.object(module, "publish", AsyncMock(return_value="https://p.example.com/")),
    ):
        member = await browser_view(sandbox, "ws-1", "c-1", viewer_id="member-1")
    assert member.state == "running" and member.local_url is None


def test_whether_a_machine_sets_its_browser_up_is_its_providers():
    """The runtime a provider builds does not carry the flag, which left every
    machine of the person's own reading as one built before the browser."""
    from ptc_agent.core.sandbox.providers import provider_class
    from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

    own = SimpleNamespace(provider=provider_class("ssh").__new__(provider_class("ssh")))
    cloud = SimpleNamespace(provider=provider_class("docker").__new__(provider_class("docker")))
    assert PTCSandbox.installs_browser.fget(own) is True
    assert PTCSandbox.installs_browser.fget(cloud) is False


@pytest.mark.asyncio
async def test_a_signed_url_gets_the_token_path_and_keeps_its_query():
    view = await browser_view(_sandbox(_status(running=True, token=TOKEN)), "ws-1", "c-1")
    assert view.state == "running"
    assert view.url == f"https://p.example.com/b/{TOKEN}?t=1"
    assert view.expires_in == VIEW_LIFETIME_S


@pytest.mark.asyncio
async def test_reading_starts_nothing_and_only_start_opens_a_browser():
    reading = _sandbox(_status())
    await browser_view(reading, "ws-1", "c-1")
    assert reading.runtime.exec.await_args.args[0] == "python3 -I /r/_internal/src/browser_runtime status"

    starting = _sandbox(_status())
    await browser_view(starting, "ws-1", "c-1", start=True)
    assert starting.runtime.exec.await_args.args[0].endswith("browser_runtime start 30")


@pytest.mark.parametrize("start", [False, True])
@pytest.mark.asyncio
async def test_only_a_start_may_wake_the_computer_to_resolve_its_url(start: bool):
    """A computer that stops between the status and the URL stays stopped
    unless the call was one that starts it."""
    sandbox = _sandbox(_status(running=True, token=TOKEN))
    sandbox.get_preview_url = AsyncMock(return_value=PreviewInfo(url="http://localhost:49153", token=""))
    sandbox.get_preview_link = AsyncMock(return_value=PreviewInfo(url="http://127.0.0.1:1/", token=None))

    with (
        patch.object(module, "preview_base", return_value=object()),
        patch.object(module, "publish", AsyncMock(return_value="https://p.example.com/")),
    ):
        await browser_view(sandbox, "ws-1", "c-1", start=start)
    assert sandbox.get_preview_url.await_args.kwargs == {"wake": start}
    assert sandbox.get_preview_link.await_args.kwargs == {"wake": start}


@pytest.mark.parametrize("viewer", [None, "member-1"])
@pytest.mark.asyncio
async def test_the_stream_is_minted_for_the_viewer_it_was_asked_for(viewer):
    """A shared workspace's member gets a stream that serves only while they
    may use the workspace; the owner's is as any preview's."""
    sandbox = _sandbox(_status(running=True, token=TOKEN))
    sandbox.get_preview_url = AsyncMock(return_value=PreviewInfo(url="https://9224-x.example.com", token="signed"))
    publish = AsyncMock(return_value="https://9224-cap.preview.example.com/")

    with patch.object(module, "preview_base", return_value=None), patch.object(module, "publish", publish):
        await browser_view(sandbox, "ws-1", "c-1", viewer_id=viewer)
    assert publish.await_args.kwargs["viewer_id"] == viewer


@pytest.mark.asyncio
async def test_a_members_signed_stream_goes_through_the_proxy_where_one_runs():
    """A signed URL a member dials would outlive their leaving the workspace;
    through the proxy, their access is read again while the socket is open."""
    sandbox = _sandbox(_status(running=True, token=TOKEN))
    sandbox.get_preview_url = AsyncMock(return_value=PreviewInfo(url="https://9224-x.example.com", token="signed"))
    link = PreviewInfo(url="https://9224-x.example.com", token=None, auth_headers={"x-token": "t"})
    sandbox.get_preview_link = AsyncMock(return_value=link)
    publish = AsyncMock(return_value="https://9224-cap.preview.example.com/")

    with patch.object(module, "preview_base", return_value=object()), patch.object(module, "publish", publish):
        await browser_view(sandbox, "ws-1", "c-1", viewer_id="member-1")
    published = publish.await_args.args[0]
    assert (published.browser_ready, published.auth_headers) == (False, {"x-token": "t"})

    # The owner's signed URL is the provider's to revoke, as any preview's is.
    publish.reset_mock()
    with patch.object(module, "preview_base", return_value=object()), patch.object(module, "publish", publish):
        await browser_view(sandbox, "ws-1", "c-1")
    assert publish.await_args.args[0].token == "signed"


@pytest.mark.asyncio
async def test_a_url_with_no_credential_goes_through_the_proxy_where_one_runs():
    """A published Docker port is open to its host, so a revocation would never
    reach a socket a browser already holds on it."""
    sandbox = _sandbox(_status(running=True, token=TOKEN))
    sandbox.get_preview_url = AsyncMock(return_value=PreviewInfo(url="http://localhost:49153", token=""))
    link = PreviewInfo(url="http://host.docker.internal:49153", token=None)
    sandbox.get_preview_link = AsyncMock(return_value=link)
    publish = AsyncMock(return_value="https://9224-cap.preview.example.com/")

    with patch.object(module, "preview_base", return_value=object()), patch.object(module, "publish", publish):
        await browser_view(sandbox, "ws-1", "c-1")
    published = publish.await_args.args[0]
    assert (published.url, published.browser_ready, published.token) == (link.url, False, None)

    # With no proxy there is nothing to go through, and the URL is handed out as it is.
    publish.reset_mock()
    with patch.object(module, "preview_base", return_value=None), patch.object(module, "publish", publish):
        await browser_view(sandbox, "ws-1", "c-1")
    assert publish.await_args.args[0].url == "http://localhost:49153"


@pytest.mark.asyncio
async def test_a_signed_url_is_never_routed_through_the_proxy():
    sandbox = _sandbox(_status(running=True, token=TOKEN))
    sandbox.get_preview_link = AsyncMock()
    with patch.object(module, "preview_base", return_value=object()):
        view = await browser_view(sandbox, "ws-1", "c-1")
    assert view.url == f"https://p.example.com/b/{TOKEN}?t=1"
    sandbox.get_preview_link.assert_not_awaited()


@pytest.mark.asyncio
async def test_holding_says_what_the_machine_says():
    assert await browser_control(_sandbox(json.dumps({"held": True})), holder="a", action="take") is True
    sandbox = _sandbox(json.dumps({"held": False}))
    assert await browser_control(sandbox, holder="view-1", action="release") is False
    assert sandbox.runtime.exec.await_args.args[0].endswith("control view-1 release")


@pytest.mark.asyncio
async def test_a_hand_back_takes_what_the_person_still_had_down_to_the_machine():
    releases = [{"key": "Shift"}, {"button": "left", "x": 3, "y": 4}]
    sandbox = _sandbox(json.dumps({"held": False}))
    await browser_control(sandbox, holder="view-1", action="release", releases=releases)
    command = shlex.split(sandbox.runtime.exec.await_args.args[0])
    assert command[-4:] == ["control", "view-1", "release", json.dumps(releases, separators=(",", ":"))]
    # Only a hand-back lets go of anything.
    await browser_control(sandbox, holder="view-1", action="renew", releases=releases)
    assert sandbox.runtime.exec.await_args.args[0].endswith("control view-1 renew")


@pytest.mark.asyncio
async def test_a_machine_with_no_hold_to_give_holds_nothing():
    assert await browser_control(_sandbox(""), holder="a", action="take") is False


@pytest.mark.asyncio
async def test_a_machine_that_does_not_answer_says_so_rather_than_not_held():
    # A hand-back it never ran may have let go of nothing, so the viewer asks again.
    sandbox = _sandbox(exec_error=RuntimeError("gone"))
    assert await browser_control(sandbox, holder="a", action="release") is None


@pytest.mark.parametrize(
    "body",
    [
        {"hold": True},
        {"holder": "a", "action": "hold"},
        {"holder": "", "action": "take"},
        {"holder": "a b", "action": "take"},
        {"holder": "x" * 65, "action": "take"},
        {"holder": "a\n", "action": "renew"},
    ],
)
def test_a_hold_names_a_short_opaque_holder_and_what_it_does(body: dict):
    from pydantic import ValidationError

    from src.server.app.workspace_browser import BrowserControlRequest

    with pytest.raises(ValidationError):
        BrowserControlRequest.model_validate(body)
    assert BrowserControlRequest.model_validate({"holder": "6f1c-AB_9", "action": "release"}).action == "release"


@pytest.mark.parametrize(
    ("typed", "address"),
    [
        ("example.com", "https://example.com"),
        ("  https://example.com/a?b=1#c ", "https://example.com/a?b=1#c"),
        ("http://example.com", "http://example.com"),
        ("localhost:3000/app", "http://localhost:3000/app"),
        ("127.0.0.1:8080", "http://127.0.0.1:8080"),
        # Where a dev server says it listens.
        ("0.0.0.0:3000", "http://0.0.0.0:3000"),
        ("[::]:3000/app", "http://[::]:3000/app"),
        ("0.0.0.0.example.com", "https://0.0.0.0.example.com"),
        ("file:///etc/passwd", None),
        ("chrome://settings", None),
        ("javascript://alert(1)", None),
        ("ftp://example.com", None),
        ("two words", None),
        ("example.com\n/x", None),
        ("https://", None),
        ("", None),
        ("a" * 2049, None),
    ],
)
def test_an_address_is_read_as_an_address_bar_reads_it_and_only_a_web_page_opens(typed, address):
    assert protocol.page_address(typed) == address


@pytest.mark.asyncio
async def test_navigating_sends_the_holder_and_the_address_it_was_read_as():
    sandbox = _sandbox(json.dumps({"done": True}))
    assert await browser_navigate(sandbox, holder="view-1", action="open", address="example.com") is None
    assert sandbox.runtime.exec.await_args.args[0].endswith("navigate view-1 open https://example.com")
    await browser_navigate(sandbox, holder="view-1", action="back", address=None)
    assert sandbox.runtime.exec.await_args.args[0].endswith("navigate view-1 back")


@pytest.mark.asyncio
async def test_an_address_that_is_not_a_web_page_never_reaches_the_machine():
    sandbox = _sandbox(json.dumps({"done": True}))
    assert await browser_navigate(sandbox, holder="a", action="open", address="file:///etc/passwd") == "bad_address"
    sandbox.runtime.exec.assert_not_awaited()


@pytest.mark.parametrize(
    ("sandbox", "refusal"),
    [
        (_sandbox(json.dumps({"done": False, "reason": "not_held"})), "not_held"),
        (_sandbox(json.dumps({"done": False, "reason": "something new"})), "failed"),
        (_sandbox(""), "failed"),
        (_sandbox(exec_error=RuntimeError("gone")), "failed"),
    ],
)
@pytest.mark.asyncio
async def test_a_navigation_that_did_nothing_says_why(sandbox, refusal):
    assert await browser_navigate(sandbox, holder="a", action="forward", address=None) == refusal


@pytest.mark.parametrize(
    "body",
    [
        {"holder": "a", "action": "home"},
        {"holder": "a b", "action": "back"},
        {"holder": "a", "action": "open", "url": "x" * 2049},
    ],
)
def test_a_navigation_names_its_holder_and_one_of_the_ways_to_go(body: dict):
    from pydantic import ValidationError

    from src.server.app.workspace_browser import BrowserNavigateRequest

    with pytest.raises(ValidationError):
        BrowserNavigateRequest.model_validate(body)


def _running(runtime, *, opened: list | None = None):
    """``running_sandbox`` attaching to *runtime*, recording each attach."""

    @contextlib.asynccontextmanager
    async def running_sandbox(row):
        if opened is not None:
            opened.append(row["computer_id"])
        yield runtime

    return running_sandbox


def _row(**overrides):
    return {"computer_id": "c-1", "provider_ref": "p-1", "status": "running", **overrides}


@pytest.mark.asyncio
async def test_a_running_computer_is_asked_through_its_runtime(monkeypatch):
    from src.server.services import data_token_sweep

    runtime = SimpleNamespace(
        exec=AsyncMock(return_value=SimpleNamespace(stdout='{"done": true}')),
        refresh_state=AsyncMock(return_value=RuntimeState.RUNNING),
        fetch_working_dir=AsyncMock(return_value="/r"),
    )
    monkeypatch.setattr(data_token_sweep, "running_sandbox", _running(runtime))
    assert await module.ask_running(_row(), "everyday", "list", timeout=5) == {"done": True}
    assert shlex.split(runtime.exec.await_args.args[0])[3:] == ["everyday", "list"]


@pytest.mark.parametrize(
    ("row", "attached", "state"),
    [
        (_row(status="stopped"), None, RuntimeState.RUNNING),
        (_row(provider_ref=None), None, RuntimeState.RUNNING),
        (_row(), "none", RuntimeState.RUNNING),
        (_row(), "runtime", RuntimeState.STOPPED),
    ],
    ids=["stopped", "never-built", "not-running-at-the-provider", "paused-since-attach"],
)
@pytest.mark.asyncio
async def test_a_computer_not_running_is_not_asked_or_woken(monkeypatch, row, attached, state):
    """E2B resumes a sandbox it attaches to, so one found paused after the
    attach is not running, and nothing runs on it."""
    from src.server.services import data_token_sweep

    runtime = SimpleNamespace(exec=AsyncMock(), refresh_state=AsyncMock(return_value=state))
    opened: list = []
    yielded = runtime if attached == "runtime" else None
    monkeypatch.setattr(data_token_sweep, "running_sandbox", _running(yielded, opened=opened))
    assert await module.ask_running(row, "status", timeout=5) is module.NOT_RUNNING
    assert opened == ([] if attached is None else ["c-1"])
    runtime.exec.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_machine_of_the_person_s_own_is_set_up_once_per_sandbox_object():
    sandbox = _own_machine({"done": True})
    module.install_soon(sandbox)
    module.install_soon(sandbox)
    await asyncio.gather(*module._INSTALLS)
    assert _asked(sandbox) == [["install"]]

    cloud = _sandbox()
    module.install_soon(cloud)
    assert not module._INSTALLS
    cloud.runtime.exec.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_install_that_fails_is_started_again_by_the_next_acquire():
    sandbox = _own_machine()
    sandbox.runtime.exec = AsyncMock(side_effect=[RuntimeError("gone"), SimpleNamespace(stdout="{}")])
    module.install_soon(sandbox)
    await asyncio.gather(*module._INSTALLS)
    module.install_soon(sandbox)
    await asyncio.gather(*module._INSTALLS)
    assert sandbox.runtime.exec.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("answer", "again"),
    [
        ({"done": False, "reason": "engine"}, True),
        ({"done": False, "reason": "ports"}, True),
        ({"done": False, "reason": "no_chrome"}, False),
        ({"done": False, "reason": "unsupported"}, False),
    ],
)
async def test_a_setup_refused_on_what_can_pass_is_started_again_by_the_next_acquire(answer, again):
    sandbox = _own_machine(answer, {"done": True})
    module.install_soon(sandbox)
    await asyncio.gather(*module._INSTALLS)
    module.install_soon(sandbox)
    await asyncio.gather(*module._INSTALLS)
    assert sandbox.runtime.exec.await_count == (2 if again else 1)
