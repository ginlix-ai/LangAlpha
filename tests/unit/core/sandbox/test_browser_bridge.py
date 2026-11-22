"""The bridge is what stands in for the Origin check it removes, so what it lets
through is the contract; the wrapper and the gate decide what reaches the engine."""

from __future__ import annotations

import contextlib
import dataclasses
import fcntl
import io
import ipaddress
import json
import os
import re
import shutil
import subprocess
import threading
import time
from pathlib import Path

import pytest

from ptc_agent.core.sandbox._defaults import (
    SANDBOX_AGENT_BROWSER_SHA256,
    SANDBOX_AGENT_BROWSER_VERSION,
    SANDBOX_CHROME_POLICY,
    SANDBOX_CHROME_POLICY_PATH,
    SANDBOX_IMAGE_ENV,
)
from ptc_agent.core.sandbox._shared import _internal_package_files, _internal_package_uploads
from ptc_agent.core.sandbox.browser_runtime import (
    care,
    choices,
    daemon,
    gate,
    hold,
    host,
    install,
    launch,
    machine,
    protocol,
    proxy,
    quality,
    relay,
    signin,
    system,
    window,
    wrapper,
)
from ptc_agent.core.sandbox.browser_runtime.host import Host
from ptc_agent.core.sandbox.browser_runtime.launch import Launch
from ptc_agent.core.sandbox.browser_runtime.relay import rewrite_head
from ptc_agent.core.sandbox.providers import provider_class
from ptc_agent.core.sandbox.providers._image_recipe import _browser_commands

_ROOT = Path(__file__).resolve().parents[4]
_DOCKERFILE = (_ROOT / "Dockerfile.sandbox").read_text()
_SANDBOX_SRC = _ROOT / "src"

TOKEN = "s3cret-token"


def _head(target: str = f"/b/{TOKEN}?pacing=ack&maxFps=10", *extra: str) -> bytes:
    lines = [
        f"GET {target} HTTP/1.1",
        "Host: 9224-abc.preview.example.com",
        "Connection: Upgrade",
        "Upgrade: websocket",
        "Sec-WebSocket-Key: abc==",
        "Sec-WebSocket-Version: 13",
        *extra,
    ]
    return ("\r\n".join(lines) + "\r\n\r\n").encode()


def test_the_upgrade_loses_its_token_and_keeps_the_stream_settings():
    upgrade, _ = rewrite_head(_head(), TOKEN)
    assert upgrade is not None
    assert upgrade.split(b"\r\n")[0] == b"GET /?pacing=ack&maxFps=10 HTTP/1.1"
    assert TOKEN.encode() not in upgrade


def test_only_what_an_upgrade_needs_goes_on():
    upgrade, _ = rewrite_head(
        _head(
            f"/b/{TOKEN}",
            "Origin: https://app.example.com",
            "Cookie: session=1",
            "E2B-Traffic-Access-Token: provider-credential",
        ),
        TOKEN,
    )
    assert upgrade is not None
    lowered = upgrade.lower()
    assert b"origin" not in lowered
    assert b"cookie" not in lowered
    assert b"provider-credential" not in lowered
    assert b"sec-websocket-key: abc==" in lowered


@pytest.mark.parametrize(
    "target",
    [
        "/",
        "/b/",
        "/b/wrong",
        f"/b/{TOKEN.upper()}",
        f"/b/{TOKEN}x",
        f"/x/{TOKEN}",
        f"/?token={TOKEN}",
    ],
)
def test_nothing_but_the_token_opens_it(target: str):
    assert rewrite_head(_head(target), TOKEN) == (None, b"403 Forbidden")


def test_the_token_without_an_upgrade_is_refused():
    head = f"GET /b/{TOKEN} HTTP/1.1\r\nHost: x\r\n\r\n".encode()
    assert rewrite_head(head, TOKEN) == (None, b"426 Upgrade Required")


@pytest.mark.parametrize(
    "head", [b"", b"GET\r\n\r\n", f"POST /b/{TOKEN} HTTP/1.1\r\n\r\n".encode(), b"GET / HTTP/1.1"]
)
def test_anything_else_is_a_bad_request(head: bytes):
    assert rewrite_head(head, TOKEN) == (None, b"400 Bad Request")


def _use_host(monkeypatch, **recorded) -> Host:
    """Stand *recorded* in for what ``install`` would have recorded."""
    chosen = Host(**recorded)
    monkeypatch.setattr(host, "current", lambda: chosen)
    return chosen


def _read_proc(monkeypatch, root: Path) -> None:
    """Read processes from *root* as from Linux's ``/proc``, on any machine."""
    monkeypatch.setattr(system, "_PROC", root)
    monkeypatch.setattr(system, "processes", lambda wanted=None: system._linux_processes())


class _Ran(Exception):
    """Stands in for exec, which would replace the test process."""


_STANDARD = Launch("", "standard")
#: The real ones, where a fixture or test stands others in for them.
_REAL_CHECK = gate.check


def _admits(launched: Launch, release=lambda: None):
    """A gate that admits every command on *launched*."""
    return lambda use, user_agent: (launched, release)


def _check(use: str, user_agent: str = "") -> Launch:
    """The launch the gate settles, its admission let go at once, as a command
    that becomes the engine's lets it go."""
    launched, release = gate.check(use, user_agent)
    release()
    return launched


def _choose(**chosen: object) -> None:
    """Hold *chosen* as the server's last save, sign-ins off unless it says."""
    choices.keep(protocol.Choices(**{"signins": False, **chosen}))
_REAL_ENGINE_GONE = care.Look.engine_gone
_REAL_CURRENT = launch.current


@pytest.fixture
def engine(tmp_path, monkeypatch):
    """What ``wrapper.main`` would exec: the argv and environment, or nothing."""
    binary = tmp_path / "engine"
    binary.write_text("#!/bin/sh\n")
    binary.chmod(0o755)
    _use_host(monkeypatch, engine=str(binary))
    monkeypatch.setattr(wrapper.os, "getuid", lambda: 1000)
    monkeypatch.setattr(wrapper, "_OOM_SCORE", tmp_path / "oom_score_adj")
    monkeypatch.setattr(gate, "check", _admits(_STANDARD))
    monkeypatch.setattr(launch, "current", lambda: None)
    ran: dict = {}

    def execute(path, argv, env):
        ran.update(path=path, argv=argv, env=env)
        raise _Ran

    monkeypatch.setattr(wrapper.os, "execve", execute)
    monkeypatch.setattr(wrapper.os, "execvpe", execute)
    return ran


def _run(*args: str) -> int | None:
    try:
        return wrapper.main(list(args))
    except _Ran:
        return None
    except SystemExit as e:
        return e.code


def _use(*args: str) -> str:
    return wrapper.use(wrapper.expand(wrapper.parse(args)))


@pytest.mark.parametrize(
    "args",
    [
        ("state", "load", "x"),
        ("auth", "login"),
        ("connect", "9222"),
        ("open", "https://example.com", "--profile", "Default"),
        ("--state", "x.json", "open", "https://example.com"),
        ("--cdp", "9222", "open", "https://example.com"),
        ("-p", "browserbase", "open", "https://example.com"),
        ("--session=work", "snapshot"),
    ],
)
def test_the_wrapper_refuses_what_would_log_the_browser_in(engine, capsys, args):
    assert _run(*args) == protocol.REFUSED
    assert "logged out" in capsys.readouterr().err
    assert engine == {}


@pytest.mark.parametrize(
    "flag",
    [
        "--args", "--proxy=http://x", "--headed", "--executable-path",
        # The daemon's own settings: one that differs stops the daemon and its browser.
        "--idle-timeout=0", "--action-policy=p.json", "--confirm-actions=close", "--debug",
        "--no-auto-dialog",
    ],
)
def test_the_wrapper_refuses_a_launch_option_that_would_lose_the_pages(engine, capsys, flag):
    assert _run(flag, "get", "url") == protocol.REFUSED
    assert capsys.readouterr().err == protocol.RESTART_MESSAGE.format(flag.partition("=")[0]) + "\n"
    assert engine == {}


def test_a_bare_flag_with_a_value_is_not_the_flag(engine):
    assert _run("--headed=1", "get", "url") is None


def test_help_runs_whatever_the_browser_s_state(engine, monkeypatch):
    """It starts no browser, so memory or a person's hold has nothing to refuse."""

    def refuse(use, user_agent):
        raise gate.Refused(protocol.MEMORY_MESSAGE)

    monkeypatch.setattr(gate, "check", refuse)
    assert _run("--help") is None
    assert engine["argv"] == [host.current().engine, "--help"]
    assert _run("snapshot") == protocol.GATE_REFUSED


@pytest.mark.parametrize(
    ("setting_up", "own", "said"),
    [
        (True, True, protocol.SETTING_UP_MESSAGE),
        (False, True, protocol.NO_CHROME_MESSAGE),
        (False, False, protocol.NOT_INSTALLED_MESSAGE),
    ],
    ids=["being set up", "own machine with no Chrome", "neither"],
)
def test_a_computer_with_no_browser_to_run_says_why(engine, monkeypatch, capsys, setting_up, own, said):
    """What the agent tells the person, who acts on it in the Browser tab."""
    engine_bin = host.current().engine
    if own:
        _use_host(monkeypatch, own=True, engine=engine_bin, chrome="/nowhere/chrome")
    else:
        _use_host(monkeypatch, engine="/nowhere/agent-browser")
    monkeypatch.setattr(wrapper, "installing", lambda: setting_up)
    assert _run("get", "url") == protocol.NOT_INSTALLED
    assert capsys.readouterr().err == said + "\n"
    assert engine == {}


@pytest.mark.parametrize(
    "args,use",
    [
        (("snapshot", "-i"), "look"),
        (("--json", "get", "text", "@e1"), "look"),
        (("tab", "list"), "look"),
        (("tab",), "look"),
        (("network", "requests"), "look"),
        (("--help",), "none"),
        (("--version",), "none"),
        (("click", "--help"), "none"),
        (("batch", "close", "-h"), "none"),
        (("click", "@e1"), "act"),
        (("open", "https://example.com"), "act"),
        (("tab", "new"), "grow"),
        (("click", "@e1", "--new-tab"), "grow"),
        (("batch", "click --new-tab @e1"), "grow"),
        (("--user-agent", "Name a@example.com", "snapshot", "-i"), "look"),
        (("network", "route", "x"), "act"),
        # Script can open a tab, as the engine itself suggests for one it tracks.
        (("eval", "window.open('https://example.com')"), "grow"),
        (("addinitscript", "x.js"), "grow"),
        (("close",), "close"),
        (("--json", "tab", "--json", "new"), "grow"),
        # Closing and going on opens another browser.
        (("batch", "close", "open https://example.com"), "reopen"),
        (("batch", "close", "snapshot"), "reopen"),
        (("batch", "click @e1", "close"), "closes"),
        (("batch", "close", "quit"), "close"),
        (("batch", "close"), "close"),
        # What runs before a close may first open the browser it closes.
        (("batch", "snapshot", "close"), "closes"),
        (("batch", "tab new", "close"), "closes"),
        # The launch options the wrapper always sets make the engine launch
        # ahead of any command, so even a read of an address opens the browser.
        (("read", "https://example.com"), "look"),
    ],
)
def test_the_wrapper_sorts_what_only_reads_from_what_acts(args: tuple[str, ...], use: str):
    assert _use(*args) == use


def test_the_user_agent_asked_for_is_the_last_one_given():
    asked = wrapper.parse(["open", "--user-agent=One", "--user-agent", "Two", "x"])
    assert (wrapper.use([asked]), wrapper.asked_user_agent(asked, "Env")) == ("act", "Two")
    assert wrapper.asked_user_agent(wrapper.parse(["open", "x"]), "Env") == "Env"


@pytest.mark.parametrize(
    "args",
    [
        ("--allow-unknown", "click", "@e1"),
        ("--json", "click", "@e1"),
        ("", "click", "@e1"),
        ("--headed=1", "click", "@e1"),
        ("--allow-unknown", "batch", "click @e1"),
        ("batch", "snapshot", "batch 'click @e1'"),
        ("--download-path=/tmp", "click", "@e1"),
    ],
)
def test_an_option_before_the_command_never_hides_an_act(args: tuple[str, ...]):
    """The engine reads the first word that is not an option as the command, so
    one unknown here moves nothing: a click past it is still a click."""
    assert _use(*args) == "act"


@pytest.mark.parametrize(
    ("args", "use"),
    [
        (("--allow-unknown", "snapshot"), "act"),
        (("--allow-unknown", "--json", "snapshot"), "act"),
        (("--download-path=/tmp", "snapshot"), "look"),
        (("--json", "true", "snapshot"), "look"),
        (("--allow-unknown",), "look"),
        (("wait", "--fn=window.done"),"grow"),
        (("wait", "-f", "window.done"),"grow"),
        (("batch", "wait --max-output --fn 'document.body.click(),true'"),"grow"),
        (("batch", "wait --max-output -f 'document.body.click(),true'"),"grow"),
        (("wait", "#ready"), "look"),
    ],
)
def test_an_option_no_one_here_knows_makes_a_read_an_act(args: tuple[str, ...], use: str):
    """It could make the engine read another word as the command."""
    assert _use(*args) == use


def test_a_command_line_is_read_once_into_one_shape():
    read = wrapper.parse(["--json", "--user-agent=UA", "tab", "--session", "s", "new", "x"])
    assert read == wrapper.Invocation(
        name="tab",
        sub="new",
        args=("new", "x"),
        options=(("--json", None), ("--user-agent", "UA"), ("--session", "s")),
        given=("--json", "--user-agent=UA", "tab", "--session", "s", "new", "x"),
    )
    assert [each.name for each in wrapper.expand(wrapper.parse(["batch", "snapshot", "batch 'tab new'"]))] == [
        "batch",
        "snapshot",
        "batch",
        "tab",
    ]


@pytest.mark.parametrize(
    ("args", "named"),
    [
        (("--allow-unknown", "state", "load", "x"), "state"),
        (("--allow-unknown", "batch", "state load x"), "state"),
        (("batch", "snapshot", "batch 'state load x'"), "state"),
        (("-p=browserbase", "open", "https://example.com"), "-p"),
        (("cookies", "--allow-unknown", "set", "a", "b"), "cookies set"),
    ],
)
def test_a_login_past_an_unknown_option_or_inside_a_batch_is_refused(engine, capsys, args, named):
    assert _run(*args) == protocol.REFUSED
    assert capsys.readouterr().err == protocol.LOGGED_OUT_MESSAGE.format(named) + "\n"
    assert engine == {}


@pytest.mark.parametrize(
    "args",
    [("open",), ("--json", "open"), ("batch", "open", "open https://example.com")],
)
def test_an_open_with_no_address_is_refused_as_a_restart(engine, capsys, args):
    """With no address the engine launches the browser again over the open one,
    which loses its pages."""
    assert _run(*args) == protocol.REFUSED
    assert capsys.readouterr().err == protocol.RESTART_MESSAGE.format("`open` without an address") + "\n"
    assert engine == {}


def test_a_click_past_an_unknown_option_is_refused_while_the_person_holds_the_page(state, engine, monkeypatch, capsys):
    monkeypatch.setattr(gate, "check", _REAL_CHECK)
    hold.control("a", "take")
    assert _run("--allow-unknown", "click", "@e1") == protocol.GATE_REFUSED
    assert capsys.readouterr().err == protocol.HELD_MESSAGE + "\n"
    assert engine == {}


def test_a_command_runs_on_the_host_recorded_while_it_waited_for_admission(engine, tmp_path, monkeypatch):
    """A setup holds the same admission while it records another engine or
    ports, so what the command read before waiting is not what it runs on."""
    upgraded = tmp_path / "engine-new"
    upgraded.write_text("#!/bin/sh\n")
    upgraded.chmod(0o755)
    newer = Host(engine=str(upgraded), proxy_port=40123)

    def admitted_after_a_setup(use, user_agent):
        monkeypatch.setattr(host, "current", lambda: newer)
        return _STANDARD, lambda: None

    monkeypatch.setattr(gate, "check", admitted_after_a_setup)
    assert _run("get", "url") is None
    assert engine["argv"] == [str(upgraded), "get", "url"]
    assert engine["env"]["AGENT_BROWSER_PROXY"] == newer.proxy_url


def test_the_wrapper_runs_a_command_with_the_user_agent_the_gate_holds(engine, monkeypatch):
    monkeypatch.setattr(gate, "check", _admits(Launch("Held a@example.com", "standard")))
    monkeypatch.setenv("AGENT_BROWSER_PROFILE", "Default")
    assert _run("get", "url") is None
    assert engine["argv"] == [host.current().engine, "get", "url"]
    assert engine["env"]["AGENT_BROWSER_USER_AGENT"] == "Held a@example.com"
    assert engine["env"]["AGENT_BROWSER_STREAM_PORT"] == str(protocol.STREAM_PORT)
    assert "AGENT_BROWSER_PROFILE" not in engine["env"]


@pytest.mark.parametrize(
    "name",
    [
        "AGENT_BROWSER_DEBUG", "AGENT_BROWSER_ACTION_POLICY", "AGENT_BROWSER_CONFIRM_ACTIONS",
        "AGENT_BROWSER_DEFAULT_TIMEOUT", "AGENT_BROWSER_NO_AUTO_DIALOG",
    ],
)
def test_a_daemon_setting_from_the_environment_never_reaches_the_engine(engine, monkeypatch, name):
    """Each is part of the daemon's fingerprint, so a value that differs from the
    running daemon's restarts it, and the browser with it, even on a read."""
    monkeypatch.setenv(name, "1")
    assert _run("get", "url") is None
    assert name not in engine["env"]


@pytest.mark.parametrize(
    ("args", "named"),
    [
        (("cookies",), "`cookies`"),
        (("cookies", "get"), "`cookies`"),
        (("cookies", "list"), "`cookies`"),
        (("--json", "cookies"), "`cookies`"),
        # The engine takes the word after `cookies` as the operation and reads
        # any it does not know, a flag included.
        (("cookies", "--all", "clear"), "`cookies`"),
        (("cookies", "--user-agent=x", "clear"), "`cookies`"),
        (("storage", "local"), "`storage` read"),
        (("storage", "local", "access_token"), "`storage` read"),
        (("storage", "session", "get", "token"), "`storage` read"),
        (("network", "har", "start"), "`network har`"),
        (("get", "cdp-url"), "`get cdp-url`"),
        (("batch", "open https://example.com", "cookies"), "`cookies`"),
    ],
)
def test_what_would_print_kept_sign_ins_is_refused(engine, monkeypatch, capsys, args, named):
    """What the agent's commands print reaches the model, and is kept with the thread."""
    monkeypatch.setattr(signin, "kept", lambda: True)
    assert _run(*args) == protocol.REFUSED
    assert capsys.readouterr().err == protocol.SIGNINS_MESSAGE.format(named) + "\n"
    assert engine == {}


@pytest.mark.parametrize(
    "args",
    [("cookies", "clear"), ("storage", "local", "set", "k", "v"), ("storage", "local", "clear"), ("get", "url")],
)
def test_clearing_or_setting_what_a_page_keeps_prints_nothing_and_runs(engine, monkeypatch, args):
    monkeypatch.setattr(signin, "kept", lambda: True)
    assert _run(*args) is None
    assert engine["argv"][1:] == list(args)


def test_a_browser_that_keeps_no_sign_ins_reads_its_cookies(engine, monkeypatch):
    monkeypatch.setattr(signin, "kept", lambda: False)
    assert _run("cookies") is None


@pytest.mark.parametrize(
    ("launched", "chosen"), [(True, [False, False]), (False, [False, True])], ids=["settled kept", "chosen since"]
)
def test_the_sign_in_refusal_follows_the_settled_launch_and_the_choice(engine, monkeypatch, capsys, launched, chosen):
    """The gate settles on a browser kept or not after the wrapper has read its
    arguments, and a choice saved meanwhile keeps the next browser's sign-ins."""
    released = []
    monkeypatch.setattr(gate, "check", _admits(Launch("", "standard", kept=launched), lambda: released.append(True)))
    choices = iter(chosen)
    monkeypatch.setattr(signin, "kept", lambda: next(choices))
    assert _run("cookies") == protocol.REFUSED
    assert capsys.readouterr().err == protocol.SIGNINS_MESSAGE.format("`cookies`") + "\n"
    assert (engine, released) == ({}, [True])


@pytest.mark.parametrize(("open_kept", "chosen"), [(True, False), (False, True)], ids=["open kept", "chosen"])
def test_a_read_refused_where_it_reads_changes_nothing_the_gate_records(engine, monkeypatch, capsys, open_kept, chosen):
    """Refused before the gate, so a batch that would also close the browser or
    set a user agent leaves the launch the next command finds as it was."""
    monkeypatch.setattr(launch, "current", lambda: Launch("", "standard", kept=True) if open_kept else None)
    monkeypatch.setattr(signin, "kept", lambda: chosen)
    monkeypatch.setattr(gate, "check", lambda use, user_agent: pytest.fail("the gate ran"))
    assert _run("batch", "close", "cookies") == protocol.REFUSED
    assert capsys.readouterr().err == protocol.SIGNINS_MESSAGE.format("`cookies`") + "\n"
    assert engine == {}


def test_the_headers_that_carry_sign_ins_are_hidden_and_nothing_else():
    printed = json.dumps(
        {
            "data": {
                "requests": [
                    {
                        "url": "https://example.com/api",
                        "headers": {"Authorization": "Bearer t", "Cookie": "sid=1", "Accept": "*/*"},
                        "responseHeaders": {"set-cookie": "sid=2", "content-type": "text/html"},
                    }
                ],
                "har": [{"name": "Proxy-Authorization", "value": "Basic x"}, {"name": "Accept", "value": "*/*"}],
                "title": "株価",
            }
        }
    )
    hidden = json.loads(wrapper.hide_signins(printed))["data"]
    [request] = hidden["requests"]
    assert request["headers"] == {"Authorization": "[hidden]", "Cookie": "[hidden]", "Accept": "*/*"}
    assert request["responseHeaders"] == {"set-cookie": "[hidden]", "content-type": "text/html"}
    assert hidden["har"] == [{"name": "Proxy-Authorization", "value": "[hidden]"}, {"name": "Accept", "value": "*/*"}]
    assert "株価" in wrapper.hide_signins(printed)


def test_a_sign_in_header_is_hidden_whatever_its_value_holds():
    """A response's cookies come as a list of each when there are several."""
    printed = json.dumps({"responseHeaders": {"Set-Cookie": ["a=1", "b=2"], "Vary": ["Accept"]}})
    assert json.loads(wrapper.hide_signins(printed)) == {
        "responseHeaders": {"Set-Cookie": "[hidden]", "Vary": ["Accept"]}
    }


def test_a_batch_s_lines_are_each_hidden_and_text_passes_as_printed():
    printed = 'GET https://example.com\n{"headers": {"cookie": "sid=1"}}\n'
    assert wrapper.hide_signins(printed) == 'GET https://example.com\n{"headers": {"cookie": "[hidden]"}}\n'


def test_the_network_commands_of_a_kept_browser_run_beside_the_wrapper_with_sign_ins_hidden(
    engine, monkeypatch, capsys
):
    """It reads what the engine prints, so it does not become the engine, and
    lets the next command in once the engine's has started."""
    monkeypatch.setattr(signin, "kept", lambda: True)
    released = []
    monkeypatch.setattr(gate, "check", _admits(_STANDARD, lambda: released.append(True)))

    class Engine:
        returncode = 0

        def __init__(self, argv, env, stdout):
            self.argv = argv
            assert released == []

        def communicate(self):
            assert released == [True]
            return json.dumps({"headers": {"Cookie": "sid=1"}}).encode(), None

    monkeypatch.setattr(wrapper.subprocess, "Popen", Engine)
    assert _run("--json", "network", "requests") == 0
    assert json.loads(capsys.readouterr().out) == {"headers": {"Cookie": "[hidden]"}}
    assert engine == {}


def test_the_wrapper_passes_on_a_refusal_and_runs_nothing(engine, monkeypatch, capsys):
    def refuse(use, user_agent):
        raise gate.Refused(protocol.HELD_MESSAGE)

    monkeypatch.setattr(gate, "check", refuse)
    assert _run("click", "@e1") == protocol.GATE_REFUSED
    assert capsys.readouterr().err == protocol.HELD_MESSAGE + "\n"
    assert engine == {}


def test_a_gate_that_fails_otherwise_costs_the_view_not_the_command(engine, monkeypatch):
    def broken(use, user_agent):
        raise OSError("no state directory")

    monkeypatch.setattr(gate, "check", broken)
    monkeypatch.setenv("AGENT_BROWSER_USER_AGENT", "Asked a@example.com")
    assert _run("click", "@e1") is None
    assert "AGENT_BROWSER_USER_AGENT" not in engine["env"]


@pytest.mark.parametrize(
    ("uid", "can_drop", "dropped"),
    [(0, True, True), (0, False, False), (1000, True, False)],
    ids=["root", "root that cannot drop it", "not root"],
)
def test_the_wrapper_keeps_chrome_from_raising_its_threads_when_run_as_root(
    engine, monkeypatch, uid, can_drop, dropped
):
    monkeypatch.setattr(wrapper.os, "getuid", lambda: uid)
    monkeypatch.setattr(wrapper, "_can_drop_nice", lambda: can_drop)
    assert _run("get", "url") is None
    expected = [host.current().engine, "get", "url"]
    if dropped:
        expected = ["setpriv", "--bounding-set=-sys_nice", "--inh-caps=-sys_nice", *expected]
    assert engine["argv"] == expected


def test_the_uploaded_wrapper_runs_the_uploaded_runtime(tmp_path):
    """The wrapper on PATH finds the runtime from its own place, so lay both out
    as the upload does and run them under the machine's isolated Python."""
    src = tmp_path / "_internal" / "src"
    for local, rel, mode in _internal_package_uploads(_SANDBOX_SRC, provider_class("docker")):
        target = src / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(local.read_bytes())
        if mode is not None:
            target.chmod(mode)
    shipped = src / "la" / "bin" / "agent-browser"
    result = subprocess.run(
        ["sh", str(shipped), "auth", "login"], capture_output=True, text=True, timeout=10
    )
    assert (result.returncode, result.stderr) == (
        protocol.REFUSED,
        protocol.LOGGED_OUT_MESSAGE.format("auth") + "\n",
    )
    result = subprocess.run(
        ["python3", "-I", str(src / protocol.PACKAGE_NAME), "control", "view-1", "release"],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert json.loads(result.stdout) == {"held": False}


def _listening(up: bool):
    return lambda port, wait_s=0.0: up


@pytest.fixture
def state(tmp_path, monkeypatch):
    """The runtime's state directory, with no browser, bridge or proxy up. The
    engine's daemon counts as running, so what a test sets for the browser stands
    until it says the engine is gone."""
    monkeypatch.setattr(protocol, "_STATE_ROOT", tmp_path)
    directory = tmp_path / protocol.STATE_DIR
    directory.mkdir()
    # The machine's limits, none set until a test writes them.
    monkeypatch.setattr(system, "_CGROUP", tmp_path)
    monkeypatch.setattr(system, "_MEMINFO", tmp_path / "no-meminfo")
    monkeypatch.setattr(quality.os, "cpu_count", lambda: 1)
    monkeypatch.setattr(relay, "ensure", lambda wait_s=0.0: True)
    monkeypatch.setattr(proxy, "ensure", lambda: True)
    monkeypatch.setattr(care.Look, "engine_gone", lambda self: False)
    monkeypatch.setattr(daemon, "listening", _listening(False))
    monkeypatch.setattr(care, "_GONE_S", 0)
    # No process of the host's: the gate ends a browserless daemon it finds.
    _read_proc(monkeypatch, tmp_path / "no-proc")
    return directory


def test_reading_runs_and_acting_is_refused_while_the_person_holds_the_page(state):
    assert hold.control("a", "take") is True
    assert _check("look") == _STANDARD
    with pytest.raises(gate.Refused, match=re.escape(protocol.HELD_MESSAGE)):
        _check("act")


@pytest.mark.parametrize("failing", ["proxy", "bridge"])
def test_a_start_that_cannot_spawn_still_leaves_the_hold_to_decide(state, monkeypatch, failing):
    """Short of memory a fork fails, which is when the person's hold matters most."""

    def no_fork(*_args, **_kwargs):
        raise OSError(12, "Cannot allocate memory")

    monkeypatch.setattr(proxy if failing == "proxy" else relay, "ensure", no_fork)
    monkeypatch.setattr(daemon, "listening", _listening(True))
    hold.control("a", "take")
    assert _check("look") == _STANDARD
    with pytest.raises(gate.Refused, match=re.escape(protocol.HELD_MESSAGE)):
        _check("act")


def test_a_hold_that_stopped_renewing_lapses_by_itself(state, monkeypatch):
    hold.control("a", "take")
    later = time.time() + hold.CONTROL_LEASE_S + 1
    monkeypatch.setattr(hold.time, "time", lambda: later)
    assert hold.held() is False
    assert hold.control("a", "renew") is False


def test_acting_after_a_hold_is_refused_once_until_the_page_is_read(state):
    assert _check("act") == _STANDARD
    hold.control("a", "take")
    hold.control("a", "release")
    with pytest.raises(gate.Refused, match=re.escape(protocol.CHANGED_MESSAGE)):
        _check("act")
    assert _check("act") == _STANDARD

    hold.control("a", "take")
    hold.control("a", "release")
    assert _check("look") == _STANDARD
    assert _check("act") == _STANDARD


def test_a_read_during_a_hold_does_not_clear_the_refusal_after_it(state):
    hold.control("a", "take")
    assert _check("look") == _STANDARD
    hold.control("a", "release")
    with pytest.raises(gate.Refused, match=re.escape(protocol.CHANGED_MESSAGE)):
        _check("act")


def test_renewing_a_hold_is_not_a_new_hold(state):
    hold.control("a", "take")
    before = hold._read().epoch
    hold.control("a", "take")
    assert hold._read().epoch == before


def test_a_take_landing_while_the_agent_reads_is_not_marked_seen(state):
    """The read and its note of what the agent saw happen under the hold's lock,
    so a take cannot land between them and go unnoticed."""
    lock = protocol.state_file(hold._LOCK)
    with open(lock, "a") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        reader = threading.Thread(target=hold.refusal, args=("look",))
        reader.start()
        time.sleep(0.2)
        assert reader.is_alive()
        # The person's take, landing while the agent's read waits for the lock.
        protocol.write_state(
            hold._FILE, json.dumps({"leases": {"a": time.time() + 60}, "epoch": 1, "seen": 0})
        )
    reader.join(5)
    hold.control("a", "release")
    with pytest.raises(gate.Refused, match=re.escape(protocol.CHANGED_MESSAGE)):
        _check("act")


def test_the_hold_is_one_document_written_whole(state):
    hold.control("a", "take")
    stored = json.loads(protocol.state_file(hold._FILE).read_text())
    assert set(stored) == {"leases", "epoch", "seen"}
    assert (stored["epoch"], stored["seen"], list(stored["leases"])) == (1, 0, ["a"])
    assert sorted(path.name for path in state.iterdir()) == [hold._FILE, hold._LOCK]
    assert protocol.state_file(hold._FILE).stat().st_mode & 0o777 == 0o600
    hold.forget()
    assert hold._read() == hold.Hold()


def test_a_user_agent_holds_until_the_browser_closes(state, monkeypatch):
    up = [False]
    monkeypatch.setattr(daemon, "listening", lambda port, wait_s=0.0: up[0])
    one = Launch("One a@example.com", "standard")
    assert _check("act", "One a@example.com") == one
    up[0] = True
    assert _check("look") == one
    assert _check("act", "One a@example.com") == one

    with pytest.raises(gate.Refused, match=re.escape(protocol.USER_AGENT_MESSAGE)):
        _check("look", "Two a@example.com")
    assert _check("close") == dataclasses.replace(one, closing=True)
    # A close that never finished: the browser is up on what it was launched with.
    assert _check("look") == one
    assert _check("close") == dataclasses.replace(one, closing=True)
    with pytest.raises(gate.Refused):
        _check("look", "Two a@example.com")
    up[0] = False
    assert _check("act", "Two a@example.com") == Launch("Two a@example.com", "standard")


def test_a_user_agent_set_on_a_running_browser_is_refused(state, monkeypatch):
    monkeypatch.setattr(daemon, "listening", _listening(True))
    with pytest.raises(gate.Refused, match=re.escape(protocol.USER_AGENT_MESSAGE)):
        _check("act", "Late a@example.com")


@pytest.mark.parametrize(
    ("engine", "stream_up", "kept"),
    [
        ("daemon", True, True),
        ("daemon", False, True),
        ("command", False, True),
        (None, False, False),
    ],
    ids=["browser up", "browser starting", "command starting it", "browser closed"],
)
def test_what_belongs_to_a_browser_lasts_while_the_engine_runs(
    state, monkeypatch, engine, stream_up, kept
):
    """The bridge is no witness: it outlives a browser that closed on its idle
    timeout by its grace, and here it is up throughout."""
    hold.control("a", "take")
    launch.write(Launch("Kept a@example.com", "light"))
    daemons = [system.Process(pid=7, ppid=1, uid=0, argv=[])] if engine == "daemon" else []

    def gone(pid, sig):
        raise ProcessLookupError(pid)

    # A daemon that runs no Chrome the reading shows, whose end finds it gone.
    monkeypatch.setattr(care.os, "kill", gone)
    monkeypatch.setattr(care.Look, "engine_gone", _REAL_ENGINE_GONE)
    monkeypatch.setattr(care.Look, "daemons", lambda self: daemons)
    monkeypatch.setattr(care.Look, "busy", lambda self: engine == "command")
    monkeypatch.setattr(daemon, "listening", _listening(stream_up))
    _check("look")
    assert hold.held() is kept
    assert (launch.current().user_agent == "Kept a@example.com") is kept


def test_a_restarted_computer_keeps_nothing_of_the_last_browser(state, monkeypatch):
    """Its state files are left and every process is gone."""
    hold.control("a", "take")
    launch.write(Launch("Before the restart", "light"))
    monkeypatch.setattr(care.Look, "engine_gone", lambda self: True)
    assert _check("act") == _STANDARD
    assert hold.held() is False


@pytest.mark.parametrize(
    ("use", "up", "refused"),
    [
        ("grow", True, True),
        ("act", False, True),
        ("act", True, False),
        ("look", False, True),
        ("look", True, False),
        ("close", False, False),
        ("reopen", True, True),
        ("reopen", False, True),
        ("closes", False, True),
        ("closes", True, False),
    ],
)
def test_more_browser_is_refused_when_memory_is_short(state, monkeypatch, use, up, refused):
    (state / "memory.max").write_text("1000\n")
    (state / "memory.stat").write_text("anon 590\nshmem 20\nfile 300\n")
    monkeypatch.setattr(system, "_CGROUP", state)
    monkeypatch.setattr(daemon, "listening", _listening(up))
    if refused:
        with pytest.raises(gate.Refused, match=re.escape(protocol.MEMORY_MESSAGE)):
            _check(use)
    else:
        assert dataclasses.replace(_check(use), closing=False) == _STANDARD


def test_a_batch_that_ends_closed_runs_on_its_browser_and_ends_the_user_agent(state, monkeypatch):
    """What runs before its close works on the page as it is, so the launch stays,
    since a changed one restarts the browser; the user agent ends with the
    browser, and stays if the close never ran. What runs first acts on the page,
    so a person's hold refuses it."""
    up = [True]
    monkeypatch.setattr(daemon, "listening", lambda port, wait_s=0.0: up[0])
    launch.write(Launch("Old", "light"))
    assert _check("closes") == Launch("Old", "light", closing=True)
    assert launch.current() == Launch("Old", "light", closing=True)
    # The batch stopped before its close: the browser and what it runs with stay.
    assert _check("look") == Launch("Old", "light")
    assert _check("closes") == Launch("Old", "light", closing=True)
    up[0] = False
    assert _check("act").user_agent == ""
    up[0] = True
    hold.control("a", "take")
    with pytest.raises(gate.Refused, match=re.escape(protocol.HELD_MESSAGE)):
        _check("closes")


def test_a_batch_that_closes_and_goes_on_starts_its_browser_afresh(state, monkeypatch):
    """Its close ends the browser and the launch on record with it, so the user
    agent and quality are this command's, as for any browser that starts."""
    monkeypatch.setattr(daemon, "listening", _listening(True))
    launch.write(Launch("Old", "light"))
    assert _check("reopen", "New") == Launch("New", "standard")
    assert _check("reopen") == Launch("", "standard")


def test_a_browser_ended_while_tending_is_one_this_command_must_open(state, monkeypatch):
    """Its Chrome had died, so tending ended its daemon and
    stream: acting now opens a browser, which memory must have room for."""
    (state / "memory.max").write_text("1000\n")
    (state / "memory.stat").write_text("anon 590\nshmem 20\nfile 300\n")
    monkeypatch.setattr(system, "_CGROUP", state)
    stream = {"up": True}
    monkeypatch.setattr(daemon, "listening", lambda port, wait_s=0.0: stream["up"])
    monkeypatch.setattr(care.Look, "tend", lambda self, up: stream.update(up=False) or self)
    with pytest.raises(gate.Refused, match=re.escape(protocol.MEMORY_MESSAGE)):
        _check("act")


@pytest.mark.parametrize("cgroup", ["max\n", None], ids=["no limit", "no controller"])
@pytest.mark.parametrize(("anon", "full"), [(500, False), (590, True)])
def test_a_machine_with_no_limit_of_its_own_is_measured_against_all_its_memory(
    state, monkeypatch, tmp_path, cgroup, anon, full
):
    if cgroup:
        (state / "memory.max").write_text(cgroup)
    meminfo = tmp_path / "meminfo"
    meminfo.write_text(
        f"MemTotal:  1000 kB\nMemAvailable:  300 kB\nAnonPages:  {anon} kB\nShmem:  20 kB\nHugePages_Total:  0\n"
    )
    monkeypatch.setattr(system, "_CGROUP", state)
    monkeypatch.setattr(system, "_MEMINFO", meminfo)
    assert care.memory_full() is full


@pytest.mark.parametrize(("rss", "full"), [(500, False), (590, True)])
def test_a_cgroup_v1_limit_is_the_computer_s_share(state, monkeypatch, rss, full):
    (state / "memory").mkdir()
    (state / "memory" / "memory.limit_in_bytes").write_text("1000\n")
    (state / "memory" / "memory.stat").write_text(f"rss 1\nshmem 1\ntotal_rss {rss}\ntotal_shmem 20\ntotal_cache 300\n")
    monkeypatch.setattr(system, "_CGROUP", state)
    assert care.memory_full() is full


def test_a_cgroup_v1_with_no_limit_is_measured_against_all_memory(state, monkeypatch, tmp_path):
    (state / "memory").mkdir()
    (state / "memory" / "memory.limit_in_bytes").write_text("9223372036854771712\n")
    (state / "memory" / "memory.stat").write_text("total_rss 590\ntotal_shmem 20\n")
    meminfo = tmp_path / "meminfo"
    meminfo.write_text("MemTotal:  1000 kB\nAnonPages:  590 kB\nShmem:  20 kB\n")
    monkeypatch.setattr(system, "_CGROUP", state)
    monkeypatch.setattr(system, "_MEMINFO", meminfo)
    assert care.memory_full() is True


def test_a_machine_that_says_neither_is_never_full(state, monkeypatch, tmp_path):
    (state / "memory.max").write_text("max\n")
    monkeypatch.setattr(system, "_CGROUP", state)
    monkeypatch.setattr(system, "_MEMINFO", tmp_path / "absent")
    assert care.memory_full() is False


#: How the engine starts each Chrome: by its name for it, through the runtime's
#: proxy, on a throwaway profile.
_OUR_PROXY = f"--proxy-server=http://127.0.0.1:{protocol.PROXY_PORT}"
_ENGINE_CHROME = [protocol.CHROME_BIN, _OUR_PROXY, f"--user-data-dir=/tmp/{protocol.ENGINE_PROFILE_PREFIX}x"]


def _fake_proc(
    root: Path, pid: int, ppid: int, argv: list[str], exe: str = "/bin/sh", cwd: str = "/", run_state: str = "S"
) -> Path:
    """A process entry as ``care`` reads one: its stat, command line, exe and cwd."""
    entry = root / str(pid)
    entry.mkdir(parents=True)
    (entry / "stat").write_text(f"{pid} (a name) with spaces) {run_state} {ppid} 1 1 0")
    (entry / "cmdline").write_bytes("\0".join(argv).encode() + b"\0")
    (entry / "exe").symlink_to(exe)
    (entry / "cwd").symlink_to(cwd)
    (entry / "oom_score_adj").write_text("0")
    return entry


def test_one_reading_of_the_processes_carries_each_parent_owner_and_command(tmp_path, monkeypatch):
    _read_proc(monkeypatch, tmp_path)
    _fake_proc(tmp_path, 7, 3, ["python3", "-I", "x"])
    (tmp_path / "self").mkdir()
    assert care.look().processes == (system.Process(7, 3, os.getuid(), ["python3", "-I", "x", ""], "/bin/sh"),)


def test_a_process_that_has_ended_is_not_one_of_the_machine_s(tmp_path, monkeypatch):
    """Its files are closed; only its parent has yet to collect it, which on a
    machine with no init to do so is never."""
    _read_proc(monkeypatch, tmp_path)
    _fake_proc(tmp_path, 7, 3, ["python3"])
    _fake_proc(tmp_path, 8, 3, [], run_state="Z")
    assert [p.pid for p in care.look().processes] == [7]


def test_only_a_browser_whose_daemon_died_is_reaped(tmp_path, monkeypatch):
    monkeypatch.setattr(protocol, "_STATE_ROOT", tmp_path)
    monkeypatch.setattr(care, "_GONE_S", 0)
    profile = tmp_path / f"{protocol.ENGINE_PROFILE_PREFIX}1"
    profile.mkdir()
    processes = tmp_path / "processes"
    _read_proc(monkeypatch, processes)
    _fake_proc(processes, 1, 0, ["/sbin/init"], exe="/sbin/init")
    _fake_proc(processes, 2, 1, [protocol.ENGINE_BIN], exe=protocol.ENGINE_BIN)
    _fake_proc(processes, 10, 1, [protocol.CHROME_BIN, _OUR_PROXY, f"--user-data-dir={profile}"])
    _fake_proc(processes, 20, 2, _ENGINE_CHROME)
    killed = []
    monkeypatch.setattr(care.os, "kill", lambda pid, sig: killed.append(pid))
    care.tend(False)
    assert killed == [10]
    assert not profile.exists()


def test_another_engine_s_browser_on_the_machine_is_never_reaped(tmp_path, monkeypatch):
    """A second computer on the same machine, or the person's own engine, names
    its throwaway profiles the same; its daemon is not ours, but its browser is
    not ours to end either."""
    monkeypatch.setattr(protocol, "_STATE_ROOT", tmp_path)
    profile = tmp_path / f"{protocol.ENGINE_PROFILE_PREFIX}1"
    profile.mkdir()
    processes = tmp_path / "processes"
    _read_proc(monkeypatch, processes)
    _fake_proc(processes, 1, 0, ["/sbin/init"], exe="/sbin/init")
    _fake_proc(processes, 3, 1, ["/elsewhere/agent-browser"], exe="/elsewhere/agent-browser")
    other = [protocol.CHROME_BIN, "--proxy-server=http://127.0.0.1:1", f"--user-data-dir={profile}"]
    _fake_proc(processes, 30, 3, other)
    killed = []
    monkeypatch.setattr(care.os, "kill", lambda pid, sig: killed.append(pid))
    care.tend(False)
    assert killed == []
    assert profile.exists()


def test_a_reaped_browser_takes_its_hold_and_launch_along(state, tmp_path, monkeypatch):
    """The next browser starts neither held nor on the dead one's user agent,
    though the bridge, still up, keeps the gate from forgetting them itself."""
    processes = tmp_path / "processes"
    _read_proc(monkeypatch, processes)
    _fake_proc(processes, 1, 0, ["/sbin/init"], exe="/sbin/init")
    _fake_proc(processes, 10, 1, _ENGINE_CHROME)
    monkeypatch.setattr(care.os, "kill", lambda pid, sig: shutil.rmtree(processes / str(pid)))
    hold.control("a", "take")
    launch.write(Launch("Old", "light"))
    care.tend(False)
    assert not (processes / "10").exists()
    assert (hold.held(), launch.current()) == (False, None)
    assert _check("act") == _STANDARD


def test_the_bridge_reaps_nothing_while_a_command_is_admitted(state, tmp_path, monkeypatch):
    """What the command records may be for the browser it is about to start;
    its own gate reaps, under the lock, before it records anything."""
    processes = tmp_path / "processes"
    _read_proc(monkeypatch, processes)
    _fake_proc(processes, 1, 0, ["/sbin/init"], exe="/sbin/init")
    _fake_proc(processes, 10, 1, _ENGINE_CHROME)
    killed = []
    monkeypatch.setattr(care.os, "kill", lambda pid, sig: killed.append(pid))
    launch.write(Launch("New", "light"))
    with open(state / care.ADMISSION_LOCK, "a") as admitting:
        fcntl.flock(admitting, fcntl.LOCK_EX)
        care.tend(False)
    assert (killed, launch.current()) == ([], Launch("New", "light"))


def test_a_chrome_the_agent_started_on_its_own_profile_is_never_reaped(tmp_path, monkeypatch):
    """Started from the same path, its launcher gone, but on a profile the
    engine did not make: neither it nor its profile is the runtime's."""
    monkeypatch.setattr(protocol, "_STATE_ROOT", tmp_path)
    profile = tmp_path / "my-profile"
    profile.mkdir()
    processes = tmp_path / "processes"
    _read_proc(monkeypatch, processes)
    _fake_proc(processes, 1, 0, ["/sbin/init"], exe="/sbin/init")
    _fake_proc(processes, 10, 1, [protocol.CHROME_BIN, f"--user-data-dir={profile}"])
    _fake_proc(processes, 11, 1, [protocol.CHROME_BIN])
    killed = []
    monkeypatch.setattr(care.os, "kill", lambda pid, sig: killed.append(pid))
    care.tend(False)
    assert killed == []
    assert profile.exists()


def test_only_a_browser_its_daemon_runs_is_up(tmp_path, monkeypatch):
    """A Chrome whose daemon died answers no command, so a start must run one,
    whose gate reaps it, rather than wait on a stream that never comes."""
    _read_proc(monkeypatch, tmp_path)
    _fake_proc(tmp_path, 1, 0, ["/sbin/init"], exe="/sbin/init")
    _fake_proc(tmp_path, 10, 1, _ENGINE_CHROME)
    assert care.look().browser_up() is False
    _fake_proc(tmp_path, 2, 1, [protocol.ENGINE_BIN], exe=protocol.ENGINE_BIN)
    _fake_proc(tmp_path, 20, 2, _ENGINE_CHROME)
    assert care.look().browser_up() is True


def test_a_daemon_whose_chrome_died_is_ended_with_its_hold_and_launch(state, monkeypatch):
    """The OOM killer can take Chrome and leave the daemon's stream on the last
    frame; the closed browser's hold would then refuse the start that brings one back."""
    processes = state / "processes"
    _read_proc(monkeypatch, processes)
    monkeypatch.setattr(daemon, "listening", _listening(False))
    killed = []
    monkeypatch.setattr(care.os, "kill", lambda pid, sig: killed.append(pid))
    _fake_proc(processes, 1, 0, ["/sbin/init"], exe="/sbin/init")
    _fake_proc(processes, 2, 1, [protocol.ENGINE_BIN], exe=protocol.ENGINE_BIN)
    hold.control("view-1", "take")
    launch.write(Launch("", "standard"))
    assert care.look().end_browserless().daemons() == []
    assert killed == [2]
    assert hold.refusal("act") is None
    assert launch.current() is None


def test_a_daemon_with_its_chrome_or_one_still_starting_is_left(state, monkeypatch):
    processes = state / "processes"
    _read_proc(monkeypatch, processes)
    killed = []
    monkeypatch.setattr(care.os, "kill", lambda pid, sig: killed.append(pid))
    _fake_proc(processes, 2, 1, [protocol.ENGINE_BIN], exe=protocol.ENGINE_BIN)
    _fake_proc(processes, 20, 2, _ENGINE_CHROME)
    care.look().end_browserless()
    assert killed == []
    shutil.rmtree(processes / "20")
    # A command that may be launching the daemon's Chrome; the caller's own does not count.
    _fake_proc(processes, 3, 1, [protocol.ENGINE_BIN, "open", "x"], exe=protocol.ENGINE_BIN)
    care.look().end_browserless()
    assert killed == []
    shutil.rmtree(processes / "3")
    runtime = str(Path(care.__file__).resolve().parent)
    me = os.getpid()
    _fake_proc(processes, me, 1, ["python3", "-I", runtime, "agent-browser", "open", "x"], exe="/usr/bin/python3")
    monkeypatch.setattr(daemon, "listening", _listening(False))
    care.look().end_browserless()
    assert killed == [2]


def test_commands_arriving_together_end_a_browserless_daemon_one_at_a_time(state, monkeypatch):
    """Each would see the other as a command that may be launching Chrome and
    leave the dead browser's stream up; the one admitted ends it, and the next
    waits for the gate."""
    processes = state / "processes"
    _read_proc(monkeypatch, processes)
    killed = []
    monkeypatch.setattr(care.os, "kill", lambda pid, sig: killed.append(pid))
    _fake_proc(processes, 2, 1, [protocol.ENGINE_BIN], exe=protocol.ENGINE_BIN)
    runtime = str(Path(care.__file__).resolve().parent)
    _fake_proc(processes, 7, 1, ["python3", "-I", runtime, "agent-browser", "open", "x"], exe="/usr/bin/python3")
    care.look().end_browserless()
    assert killed == []
    with care.admitted() as seen:
        seen.end_browserless()
    assert killed == [2]
    _, release = gate.check("act")
    with open(state / "admitting.lock") as waiting, pytest.raises(BlockingIOError):
        fcntl.flock(waiting, fcntl.LOCK_EX | fcntl.LOCK_NB)
    release()
    with open(state / "admitting.lock") as waiting:
        fcntl.flock(waiting, fcntl.LOCK_EX | fcntl.LOCK_NB)


def test_ending_processes_returns_the_reading_without_them(state, monkeypatch):
    """Each check after it sees the machine as the end left it, not as read."""
    processes = state / "processes"
    _read_proc(monkeypatch, processes)
    monkeypatch.setattr(care.os, "kill", lambda pid, sig: None)
    _fake_proc(processes, 1, 0, ["/sbin/init"], exe="/sbin/init")
    _fake_proc(processes, 2, 1, [protocol.ENGINE_BIN], exe=protocol.ENGINE_BIN)
    _fake_proc(processes, 20, 2, _ENGINE_CHROME)
    _fake_proc(processes, 21, 20, ["/chrome/renderer"])
    seen = care.look()
    # A daemon takes the Chrome it runs, and that its own processes.
    left = seen.close(seen.daemons())
    assert (sorted(p.pid for p in seen.processes), [p.pid for p in left.processes]) == ([1, 2, 20, 21], [1])
    assert left.close([]) is left


def test_the_bridge_tick_leaves_a_command_being_admitted_to_its_gate(state, monkeypatch):
    """A command recording a new launch meanwhile would lose it to a browser
    the tick ends."""
    monkeypatch.setattr(care.Look, "tend", lambda self, up: pytest.fail("tended under a command's admission"))
    admitted = care.admit()
    try:
        care.tend(True)
        care.tend(False)
    finally:
        admitted.release()


def test_the_bridge_tick_that_cannot_take_the_gate_tends_nothing(state, monkeypatch):
    """It cannot tell whether a command holds it, so it leaves this tick."""

    def unopenable(name, *, wait=True):
        raise OSError(24, "Too many open files")

    monkeypatch.setattr(care, "locked", unopenable)
    monkeypatch.setattr(care.Look, "tend", lambda self, up: pytest.fail("tended without the gate's lock"))
    care.tend(True)
    care.tend(False)


def test_the_whole_browser_goes_first_and_nothing_beside_it(tmp_path, tmp_path_factory, monkeypatch):
    # The tick's lock file, kept apart from the process table it reads.
    monkeypatch.setattr(protocol, "_STATE_ROOT", tmp_path_factory.mktemp("state"))
    _read_proc(monkeypatch, tmp_path)
    _fake_proc(tmp_path, 2, 1, [protocol.ENGINE_BIN], exe=protocol.ENGINE_BIN)
    _fake_proc(tmp_path, 10, 2, _ENGINE_CHROME)
    _fake_proc(tmp_path, 11, 10, ["/chrome/renderer", "--type=renderer"])
    _fake_proc(tmp_path, 12, 11, ["/chrome/renderer-child"])
    _fake_proc(tmp_path, 30, 1, ["python3", "analysis.py"])
    care.tend(True)
    scores = {int(entry.name): (entry / "oom_score_adj").read_text() for entry in tmp_path.iterdir()}
    assert scores == {2: "0", 10: "1000", 11: "1000", 12: "1000", 30: "0"}


@pytest.fixture
def engine_runs(state, monkeypatch):
    """The engine commands ``navigate`` runs, on an open browser launched at retina."""
    monkeypatch.setattr(daemon, "listening", _listening(True))
    monkeypatch.setattr(care.Look, "browser_up", lambda self: True)
    launch.write(Launch("", "retina"))
    runs: list[tuple[list[str], dict[str, str]]] = []

    def run(argv, **kwargs):
        runs.append((argv, kwargs["env"]))
        return subprocess.CompletedProcess(argv, 0)

    monkeypatch.setattr(machine.subprocess, "run", run)
    return runs


def test_only_the_holder_takes_the_page_anywhere(engine_runs):
    assert machine.navigate("a", "open", "example.com") == {"done": False, "reason": "not_held"}
    hold.control("b", "take")
    assert machine.navigate("a", "back") == {"done": False, "reason": "not_held"}
    assert engine_runs == []
    hold.control("a", "take")
    assert machine.navigate("a", "open", "example.com") == {"done": True}
    assert engine_runs[-1][0] == [protocol.ENGINE_BIN, "open", "https://example.com"]


@pytest.mark.parametrize("action", ["back", "forward", "reload"])
def test_moving_in_place_is_judged_by_where_the_page_is_after(engine_runs, monkeypatch, action):
    hold.control("a", "take")
    runs: list[list[str]] = []

    def run(argv, **_):
        runs.append(argv[1:])
        # The engine's script in the page fails as the page leaves under it.
        return subprocess.CompletedProcess(argv, 1 if argv[1] == action else 0)

    monkeypatch.setattr(machine.subprocess, "run", run)
    assert machine.navigate("a", action) == {"done": True}
    assert runs == [[action], ["get", "url"]]


def test_navigating_runs_with_the_open_browser_s_launch_so_nothing_restarts(engine_runs):
    launch.write(Launch("ua/1\n", "retina"))
    hold.control("a", "take")
    machine.navigate("a", "forward")
    argv, env = engine_runs[0]
    assert argv == [protocol.ENGINE_BIN, "forward"]
    assert env["AGENT_BROWSER_USER_AGENT"] == "ua/1"
    assert env["AGENT_BROWSER_PROXY"] == f"http://127.0.0.1:{protocol.PROXY_PORT}"
    assert "--force-device-scale-factor=2" in env["AGENT_BROWSER_ARGS"]


def test_the_person_s_navigation_and_the_agent_s_command_run_with_one_launch(state, engine, monkeypatch):
    """The engine restarts the browser on any launch option that differs, so a
    navigation for the person must repeat exactly what the agent's commands ran."""
    monkeypatch.setattr(gate, "check", _REAL_CHECK)
    monkeypatch.setattr(launch, "current", _REAL_CURRENT)
    monkeypatch.setattr(quality.os, "cpu_count", lambda: 8)
    assert _run("--user-agent", "Agent a@example.com", "open", "https://example.com") is None
    monkeypatch.setattr(daemon, "listening", _listening(True))
    monkeypatch.setattr(care.Look, "browser_up", lambda self: True)
    # A choice made while the browser is open reaches the next one, not this one.
    _choose(quality="light")
    assert _run("snapshot") is None
    agent_env = engine["env"]
    hold.control("a", "take")
    runs = []

    def run(argv, **kwargs):
        runs.append(kwargs["env"])
        return subprocess.CompletedProcess(argv, 0)

    monkeypatch.setattr(machine.subprocess, "run", run)
    assert machine.navigate("a", "reload") == {"done": True}
    assert runs == [agent_env, agent_env]
    assert agent_env["AGENT_BROWSER_USER_AGENT"] == "Agent a@example.com"
    assert agent_env["AGENT_BROWSER_STREAM_MAX_WIDTH"] == "2560"


@pytest.mark.parametrize("ended", ["done", "timed out"])
def test_a_page_the_person_moved_is_news_to_the_agent_after_the_hand_back(engine_runs, monkeypatch, ended):
    """A navigation the engine finishes after the hand-back changes the page
    under what the agent read since, so its next action is refused once."""
    hold.control("a", "take")

    def run(argv, **kwargs):
        # While the engine is still taking the page there, the person hands it
        # back and the agent reads the page as it is so far.
        hold.control("a", "release")
        assert hold.refusal("look") is None
        if ended == "timed out":
            raise subprocess.TimeoutExpired(argv, 1)
        return subprocess.CompletedProcess(argv, 0)

    monkeypatch.setattr(machine.subprocess, "run", run)
    machine.navigate("a", "open", "example.com")
    assert hold.refusal("act") == protocol.CHANGED_MESSAGE
    assert hold.refusal("act") is None


def test_a_browser_with_no_launch_on_record_is_not_navigated(engine_runs):
    launch.forget()
    hold.control("a", "take")
    assert machine.navigate("a", "back") == {"done": False, "reason": "failed"}
    assert engine_runs == []


def test_navigating_opens_no_browser_and_no_page_but_a_web_page(engine_runs, monkeypatch):
    hold.control("a", "take")
    assert machine.navigate("a", "open", "file:///etc/passwd") == {"done": False, "reason": "bad_address"}
    monkeypatch.setattr(daemon, "listening", _listening(False))
    assert machine.navigate("a", "open", "example.com") == {"done": False, "reason": "not_running"}
    assert engine_runs == []


def test_a_stream_whose_chrome_died_is_not_navigated(engine_runs, monkeypatch):
    """The engine's daemon outlives a Chrome the OOM killer took, and the next
    command it runs launches another, outside the wrapper's memory check."""
    hold.control("a", "take")
    monkeypatch.setattr(care.Look, "browser_up", lambda self: False)
    assert machine.navigate("a", "reload") == {"done": False, "reason": "not_running"}
    assert engine_runs == []


def test_the_agent_s_action_waits_for_a_navigation_handed_back_mid_way(engine_runs, monkeypatch):
    """Handing the page back while it is still on its way frees the agent, whose
    action would land on a page still loading; it waits, then learns it moved."""
    hold.control("a", "take")
    started, finish = threading.Event(), threading.Event()

    def run(argv, **_):
        started.set()
        finish.wait(5)
        return subprocess.CompletedProcess(argv, 0)

    monkeypatch.setattr(machine.subprocess, "run", run)
    navigation = threading.Thread(target=machine.navigate, args=("a", "open", "example.com"))
    navigation.start()
    judged: list[str | None] = []
    agent = threading.Thread(target=lambda: judged.append(hold.refusal("act")))
    try:
        assert started.wait(5)
        hold.control("a", "release")
        agent.start()
        agent.join(0.3)
        assert agent.is_alive() and judged == []
        assert hold.refusal("look") is None
    finally:
        # Neither thread may outlive the state directory this test made.
        finish.set()
        navigation.join(5)
        if agent.is_alive():
            agent.join(5)
    assert judged == [protocol.CHANGED_MESSAGE]


def test_a_navigation_queued_behind_another_goes_nowhere_once_its_hold_ended(engine_runs, monkeypatch):
    """It waited on another viewer's navigation, and its own viewer handed the
    page back meanwhile."""
    hold.control("a", "take")
    hold.control("b", "take")
    started, finish = threading.Event(), threading.Event()

    def run(argv, **kwargs):
        if not started.is_set():
            started.set()
            finish.wait(5)
        engine_runs.append((argv, kwargs["env"]))
        return subprocess.CompletedProcess(argv, 0)

    monkeypatch.setattr(machine.subprocess, "run", run)
    first = threading.Thread(target=machine.navigate, args=("b", "open", "example.com"))
    first.start()
    answers: list[dict[str, object]] = []
    queued = threading.Thread(target=lambda: answers.append(machine.navigate("a", "reload")))
    try:
        assert started.wait(5)
        queued.start()
        time.sleep(0.2)
        hold.control("a", "release")
    finally:
        finish.set()
        first.join(5)
        queued.join(5)
    assert answers == [{"done": False, "reason": "not_held"}]
    assert [argv[1:] for argv, _ in engine_runs] == [["open", "https://example.com"]]


def test_a_navigation_queued_behind_another_goes_nowhere_once_the_browser_ended(engine_runs, monkeypatch):
    """Its Chrome went while it waited, and the engine would open another."""
    hold.control("a", "take")
    hold.control("b", "take")
    started, finish = threading.Event(), threading.Event()
    up = [True]
    monkeypatch.setattr(care.Look, "browser_up", lambda self: up[0])

    def run(argv, **kwargs):
        if not started.is_set():
            started.set()
            finish.wait(5)
        engine_runs.append((argv, kwargs["env"]))
        return subprocess.CompletedProcess(argv, 0)

    monkeypatch.setattr(machine.subprocess, "run", run)
    first = threading.Thread(target=machine.navigate, args=("b", "open", "example.com"))
    first.start()
    answers: list[dict[str, object]] = []
    queued = threading.Thread(target=lambda: answers.append(machine.navigate("a", "reload")))
    try:
        assert started.wait(5)
        queued.start()
        time.sleep(0.2)
        up[0] = False
    finally:
        finish.set()
        first.join(5)
        queued.join(5)
    assert answers == [{"done": False, "reason": "not_running"}]
    assert [argv[1:] for argv, _ in engine_runs] == [["open", "https://example.com"]]


def test_every_command_finds_the_one_daemon_whatever_folder_it_asks_for(monkeypatch):
    """Another folder holds another daemon, whose Chrome the gate, the hold and
    the viewer never see."""
    env = _STANDARD.engine_env({"AGENT_BROWSER_SOCKET_DIR": "/tmp/other", "HOME": "/tmp/elsewhere"}, Host())
    assert env["AGENT_BROWSER_SOCKET_DIR"] == os.path.join(
        launch.pwd.getpwuid(os.getuid()).pw_dir, ".agent-browser"
    )
    monkeypatch.setattr(launch.os, "getuid", lambda: 2**31 - 7)
    assert _STANDARD.engine_env({}, Host())["AGENT_BROWSER_SOCKET_DIR"].endswith("/agent-browser")


def test_a_page_the_engine_could_not_reach_is_a_failure_and_a_slow_one_is_not(engine_runs, monkeypatch):
    hold.control("a", "take")
    monkeypatch.setattr(machine.subprocess, "run", lambda argv, **_: subprocess.CompletedProcess(argv, 1))
    assert machine.navigate("a", "open", "example.com") == {"done": False, "reason": "failed"}

    def slow(argv, **_):
        raise subprocess.TimeoutExpired(argv, 30)

    monkeypatch.setattr(machine.subprocess, "run", slow)
    assert machine.navigate("a", "open", "example.com") == {"done": True}


def test_starting_leaves_a_browser_already_up_on_its_page(state, monkeypatch):
    monkeypatch.setattr(host.os, "access", lambda *_: True)
    monkeypatch.setattr(daemon, "listening", _listening(True))
    monkeypatch.setattr(care.Look, "browser_up", lambda self: True)
    (state / protocol.TOKEN_FILE).write_text("tok")
    spawned = []
    monkeypatch.setattr(daemon.subprocess, "Popen", lambda *a, **k: spawned.append(a))
    assert machine.start(0) == protocol.Status(installed=True, running=True, token="tok")
    assert spawned == []


def test_a_stream_whose_chrome_is_gone_is_not_up_and_a_start_brings_one_back(state, monkeypatch):
    """The engine's daemon keeps its stream listening after the OOM killer takes
    Chrome, and the next command it runs launches another."""
    monkeypatch.setattr(host.os, "access", lambda *_: True)
    monkeypatch.setattr(daemon, "listening", _listening(True))
    monkeypatch.setattr(care.Look, "browser_up", lambda self: False)
    monkeypatch.setattr(care.Look, "busy", lambda self: False)
    monkeypatch.setattr(care, "memory_full", lambda: False)
    (state / protocol.TOKEN_FILE).write_text("tok")
    spawned = []
    monkeypatch.setattr(daemon.subprocess, "Popen", lambda argv, **k: spawned.append(argv[3:]))
    assert machine.status(0) == protocol.Status(installed=True)
    assert machine.start(0) == protocol.Status(installed=True)
    assert spawned == [["agent-browser", *machine._ENGINE_START_ARGS]]


def test_an_open_with_memory_short_tries_nothing_and_says_so(state, monkeypatch):
    """The wrapper would refuse the open, and a viewer waiting out the start for a
    browser that never comes would learn nothing."""
    monkeypatch.setattr(host.os, "access", lambda *_: True)
    monkeypatch.setattr(care.Look, "busy", lambda self: False)
    monkeypatch.setattr(care, "memory_full", lambda: True)
    spawned = []
    monkeypatch.setattr(daemon.subprocess, "Popen", lambda *a, **k: spawned.append(a))
    status = machine.start(30)
    assert status == protocol.Status(installed=True, low_memory=True)
    assert protocol.Status.read(status.to_wire()) == status
    assert spawned == []


def test_asking_for_the_status_starts_the_bridge_once_and_waits_for_it(tmp_path, monkeypatch):
    """Each start truncates the bridge's log, so a wait spawns once and then watches."""
    monkeypatch.setattr(protocol, "_STATE_ROOT", tmp_path)
    looks = []

    def stream_only(port, wait_s=0.0):
        looks.append((port, wait_s))
        return port == protocol.STREAM_PORT

    monkeypatch.setattr(daemon, "listening", stream_only)
    monkeypatch.setattr(care.Look, "browser_up", lambda self: True)
    spawned = []
    monkeypatch.setattr(daemon.subprocess, "Popen", lambda argv, **k: spawned.append(argv[3:]))
    assert machine.status(0).running is False
    assert spawned == [["serve"]]
    assert looks == [
        (protocol.STREAM_PORT, 0),
        (protocol.BRIDGE_PORT, 0.0),
        (protocol.BRIDGE_PORT, pytest.approx(machine._BRIDGE_UP_S, abs=0.1)),
    ]
    assert (tmp_path / protocol.STATE_DIR / "bridge.log").exists()


def test_a_daemon_up_already_is_left_alone_and_one_down_is_waited_for(tmp_path, monkeypatch):
    monkeypatch.setattr(protocol, "_STATE_ROOT", tmp_path)
    spawned = []
    monkeypatch.setattr(daemon.subprocess, "Popen", lambda argv, **k: spawned.append(argv[3:]))
    monkeypatch.setattr(daemon, "listening", lambda port, wait_s=0.0: True)
    assert daemon.ensure("proxy", protocol.PROXY_PORT, "proxy.log", 3.0) is True
    assert spawned == []
    looks = []

    def comes_up(port, wait_s=0.0):
        looks.append(wait_s)
        return len(looks) > 1

    monkeypatch.setattr(daemon, "listening", comes_up)
    assert daemon.ensure("proxy", protocol.PROXY_PORT, "proxy.log", 3.0) is True
    assert (spawned, looks) == ([["proxy"]], [0.0, 3.0])


def test_a_state_file_is_replaced_whole_and_kept_private(tmp_path, monkeypatch):
    monkeypatch.setattr(protocol, "_STATE_ROOT", tmp_path)
    protocol.write_state("token", "one")
    protocol.write_state("token", "two")
    target = tmp_path / protocol.STATE_DIR / "token"
    assert target.read_text() == "two"
    assert target.stat().st_mode & 0o777 == 0o600
    assert [path.name for path in target.parent.iterdir()] == ["token"]


def test_dockerfile_mirrors_the_pins_and_the_policy():
    args = dict(re.findall(r"^ARG\s+(AGENT_BROWSER\w*)=(\S+)", _DOCKERFILE, re.MULTILINE))
    assert args["AGENT_BROWSER_VERSION"] == SANDBOX_AGENT_BROWSER_VERSION
    assert args["AGENT_BROWSER_SHA256_AMD64"] == SANDBOX_AGENT_BROWSER_SHA256["amd64"]
    assert args["AGENT_BROWSER_SHA256_ARM64"] == SANDBOX_AGENT_BROWSER_SHA256["arm64"]
    written = re.search(r"echo '(\{.*?\})' > " + re.escape(SANDBOX_CHROME_POLICY_PATH), _DOCKERFILE)
    assert written is not None
    assert json.loads(written.group(1)) == SANDBOX_CHROME_POLICY


@pytest.mark.parametrize("arch", ["amd64", "arm64"])
def test_the_recipe_picks_each_architecture_and_carries_on(arch: str):
    """Every step is one && chain, so a selection that ends non-zero aborts the
    image build on whichever architecture it was not written on."""
    command = _browser_commands(SANDBOX_IMAGE_ENV["PLAYWRIGHT_BROWSERS_PATH"])[0]
    selection = re.search(r'case "\$AB_ARCH" in .*? esac', command)
    assert selection is not None
    result = subprocess.run(
        ["sh", "-c", f'AB_ARCH={arch}; {selection.group(0)} && echo "$AB_SHA"'],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0
    assert result.stdout.strip() == SANDBOX_AGENT_BROWSER_SHA256[arch]


@pytest.mark.parametrize("kind", ["docker", "ssh"])
def test_every_machine_is_sent_the_browser(kind):
    """A machine of the person's own sets the browser up with what is sent."""
    names = {str(rel) for _, rel in _internal_package_files(_SANDBOX_SRC, provider_class(kind))}
    assert any(name.endswith("agent-browser") for name in names)
    assert f"{protocol.PACKAGE_NAME}/install.py" in names
    # The engine fails every command when the config it is pointed at is missing.
    config = Path(launch._ENGINE_ENV["AGENT_BROWSER_CONFIG"]).name
    assert f"{protocol.PACKAGE_NAME}/{config}" in names


def test_only_the_stream_s_own_settings_reach_it():
    """On some providers the signed preview credential rides in the query."""
    upgrade, _ = rewrite_head(
        _head(f"/b/{TOKEN}?pacing=ack&tkn=signed&maxFps=10&X-Daytona-Preview-Token=p&pacing"), TOKEN
    )
    assert upgrade is not None
    assert upgrade.split(b"\r\n")[0] == b"GET /?pacing=ack&maxFps=10&pacing HTTP/1.1"
    upgrade, _ = rewrite_head(_head(f"/b/{TOKEN}?tkn=signed"), TOKEN)
    assert upgrade is not None
    assert upgrade.split(b"\r\n")[0] == b"GET / HTTP/1.1"


@pytest.mark.parametrize(
    "extra",
    [
        ("X-Note: a\rb",),
        ("X-Note: a\nb",),
        (" folded",),
        ("\tfolded",),
        ("Host: other.example.com",),
        ("no colon",),
        ("Host : spaced",),
    ],
    ids=["bare CR", "bare LF", "space fold", "tab fold", "second Host", "no colon", "space in name"],
)
def test_a_head_two_parsers_could_read_differently_is_refused(extra: tuple[str, ...]):
    assert rewrite_head(_head(f"/b/{TOKEN}", *extra), TOKEN) == (None, b"400 Bad Request")


def test_a_bridge_that_cannot_listen_leaves_the_token_alone(state, monkeypatch):
    """Two starts can both bind; the one that cannot listen must not write the
    token, or the file names one the live bridge refuses."""
    (state / protocol.TOKEN_FILE).write_text("live")

    class Loser:
        def __init__(self, *args):
            pass

        def setsockopt(self, *args):
            pass

        def bind(self, address):
            pass

        def listen(self, backlog):
            raise OSError(98, "Address already in use")

        def close(self):
            pass

    monkeypatch.setattr(daemon.socket, "socket", Loser)
    assert relay.serve() == 0
    assert (state / protocol.TOKEN_FILE).read_text() == "live"


def test_a_second_viewer_holds_until_both_let_go(state):
    assert hold.control("a", "take") is True
    epoch = hold._read().epoch
    assert hold.control("b", "take") is True
    assert hold._read().epoch == epoch
    assert hold.control("a", "release") is False
    with pytest.raises(gate.Refused, match=re.escape(protocol.HELD_MESSAGE)):
        _check("act")
    assert hold.control("b", "renew") is True
    assert hold.control("b", "release") is False
    with pytest.raises(gate.Refused, match=re.escape(protocol.CHANGED_MESSAGE)):
        _check("act")
    assert _check("act") == _STANDARD


def test_a_renewal_after_a_release_takes_nothing_back(state):
    hold.control("a", "take")
    hold.control("a", "release")
    epoch = hold._read().epoch
    assert hold.control("a", "renew") is False
    assert hold.held() is False
    assert hold._read().epoch == epoch
    assert hold.control("never", "renew") is False
    assert hold.held() is False


_RELEASES = [{"key": "Shift"}, {"button": "left", "x": 120.6, "y": 40}]


def test_a_hand_back_lets_go_of_what_the_stream_could_not_before_the_hold_ends(engine_runs, monkeypatch, capsys):
    """A press the stream delivered stays down on the page until it is told, so
    the release the stream could not deliver goes through the engine, while the
    hold still keeps the agent's commands off the page."""
    holder = "11111111-1111-4111-8111-111111111111"
    hold.control(holder, "take")
    held_during: list[bool] = []

    def run(argv, **kwargs):
        held_during.append(hold.holds(holder))
        engine_runs.append((argv, kwargs["env"]))
        return subprocess.CompletedProcess(argv, 0)

    monkeypatch.setattr(machine.subprocess, "run", run)
    assert machine.main(["control", holder, "release", json.dumps(_RELEASES)]) == 0
    assert json.loads(capsys.readouterr().out) == {"held": False}
    assert [argv[1:] for argv, _ in engine_runs] == [
        ["keyup", "Shift"],
        ["mouse", "move", "121", "40"],
        ["mouse", "up", "left"],
    ]
    assert held_during == [True, True, True]
    assert hold.held() is False
    # With the open browser's launch, so the engine restarts nothing.
    assert "--force-device-scale-factor=2" in engine_runs[0][1]["AGENT_BROWSER_ARGS"]


@pytest.mark.parametrize("failure", ["exit", "timeout"])
def test_a_hand_back_that_could_not_let_go_keeps_the_hold(engine_runs, monkeypatch, failure):
    """The agent acting on the page with the key still down is what the release
    is for, so the hold stays while the viewer asks again."""
    hold.control("a", "take")

    def run(argv, **kwargs):
        engine_runs.append((argv, kwargs["env"]))
        if failure == "timeout":
            raise subprocess.TimeoutExpired(argv, kwargs["timeout"])
        return subprocess.CompletedProcess(argv, 1)

    monkeypatch.setattr(machine.subprocess, "run", run)
    assert machine.control("a", "release", protocol.release_commands(_RELEASES)) is True
    assert len(engine_runs) == 1
    assert hold.holds("a")
    # Asked again once the engine answers, it lets go and hands back.
    monkeypatch.setattr(machine.subprocess, "run", lambda argv, **kwargs: subprocess.CompletedProcess(argv, 0))
    assert machine.control("a", "release", protocol.release_commands(_RELEASES)) is False
    assert hold.held() is False


@pytest.mark.parametrize("why", ["not held", "browser closed", "no launch"])
def test_releases_reach_only_the_holder_s_open_browser(engine_runs, monkeypatch, why):
    """A lapsed hold leaves the page to the agent, and an engine command on a
    browser that ended would start another."""
    if why != "not held":
        hold.control("a", "take")
    if why == "browser closed":
        monkeypatch.setattr(care.Look, "browser_up", lambda self: False)
    if why == "no launch":
        launch.forget()
    assert machine.control("a", "release", protocol.release_commands(_RELEASES)) is False
    assert engine_runs == []
    assert hold.held() is False


def test_releases_ride_only_a_hand_back(engine_runs):
    assert machine.main(["control", "a", "take", json.dumps(_RELEASES)]) == 2
    assert machine.main(["control", "a", "release", "not json"]) == 2
    assert engine_runs == []


@pytest.mark.parametrize(
    "releases",
    [
        [{"key": "-h"}],
        [{"key": "--session"}],
        [{"key": ""}],
        [{"key": "x" * 33}],
        [{"key": "a\nb"}],
        [{"key": "a", "button": "left"}],
        [{"button": "back", "x": 1, "y": 1}],
        [{"button": "left", "x": -1, "y": 1}],
        [{"button": "left", "x": True, "y": 1}],
        [{"button": "left", "x": float("nan"), "y": 1}],
        [{"button": "left", "x": 1}],
        [{"key": "a"}] * (protocol.MAX_RELEASES + 1),
        {"key": "a"},
        ["a"],
    ],
)
def test_a_release_is_a_key_or_a_button_and_nothing_the_engine_reads_as_a_flag(releases):
    assert protocol.release_commands(releases) is None


def test_the_keys_a_page_names_are_released_as_named():
    keys = ["a", "-", " ", "Shift", "ArrowLeft", "Meta", "é"]
    assert protocol.release_commands([{"key": k} for k in keys]) == [["keyup", k] for k in keys]


@pytest.mark.parametrize(
    ("holder", "action"),
    [("", "take"), ("a b", "take"), ("x" * 65, "take"), ("a/../b", "release"), ("a\n", "take"), ("a", "hold")],
)
def test_only_a_short_opaque_holder_and_a_known_action_are_taken(state, holder, action):
    with pytest.raises(ValueError):
        hold.control(holder, action)
    assert machine.main(["control", holder, action]) == 2
    if not hold.valid_holder(holder):
        assert machine.main(["navigate", holder, "back"]) == 2


def test_a_start_drops_a_hold_taken_on_a_browser_that_closed(state, monkeypatch):
    """A take can land just after the browser closed; its lease would refuse the
    open that brings one back for as long as it runs."""
    monkeypatch.setattr(host.os, "access", lambda *_: True)
    monkeypatch.setattr(care.Look, "browser_up", lambda self: False)
    monkeypatch.setattr(care.Look, "busy", lambda self: False)
    monkeypatch.setattr(care, "memory_full", lambda: False)
    monkeypatch.setattr(daemon.subprocess, "Popen", lambda *a, **k: None)
    hold.control("late", "take")
    machine.start(0)
    assert hold.held() is False
    assert hold.refusal("act") is None


def test_a_start_only_reads_so_it_can_never_take_the_agent_s_page_away():
    """It can land after the agent's own first command whatever it checks first,
    and a read starts the browser without going anywhere."""
    assert _use(*machine._ENGINE_START_ARGS) == "look"


def test_an_open_waits_for_a_browser_the_agent_is_still_starting(state, monkeypatch):
    """Two starts at once reach one daemon, so the second is not sent."""
    monkeypatch.setattr(host.os, "access", lambda *_: True)
    spawned = []
    monkeypatch.setattr(daemon.subprocess, "Popen", lambda argv, **k: spawned.append(argv[3:]))
    monkeypatch.setattr(care.Look, "busy", lambda self: True)
    machine.start(0)
    assert spawned == []
    monkeypatch.setattr(care.Look, "busy", lambda self: False)
    machine.start(0)
    assert spawned == [["agent-browser", *machine._ENGINE_START_ARGS]]


def test_a_command_of_ours_or_the_engine_is_seen_starting(tmp_path, monkeypatch):
    runtime = str(Path(care.__file__).resolve().parent)
    _read_proc(monkeypatch, tmp_path)
    _fake_proc(tmp_path, 1, 0, ["/bin/sh", "-c", "agent-browser open x"])
    assert care.look().busy() is False
    wrapper_entry = _fake_proc(
        tmp_path,
        2,
        1,
        ["python3", "-I", f"{runtime}/../{Path(runtime).name}", "agent-browser", "open", "x"],
        exe="/usr/bin/python3",
    )
    assert care.look().busy() is True
    shutil.rmtree(wrapper_entry)
    # The engine's daemon alone starts nothing: it can outlive a Chrome the OOM killer took.
    daemon_entry = _fake_proc(tmp_path, 3, 1, [protocol.ENGINE_BIN], exe=protocol.ENGINE_BIN)
    assert care.look().busy() is False
    shutil.rmtree(daemon_entry)
    _fake_proc(tmp_path, 4, 1, [protocol.ENGINE_BIN, "open", "x"], exe=protocol.ENGINE_BIN)
    assert care.look().busy() is True


@pytest.mark.parametrize(
    ("args", "named"),
    [
        (("--json", "state", "load", "saved.json"), "state"),
        (("--json", "auth", "login", "site"), "auth"),
        (("--json", "true", "auth", "login", "site"), "auth"),
        (("--download-path", "/tmp", "state", "load", "x"), "state"),
        (("--max-output", "10", "connect", "9222"), "connect"),
        (("cookies", "set", "sid", "1"), "cookies set"),
        (("--json", "cookies", "set", "--curl", "c.txt"), "cookies set"),
        (("set", "credentials", "u", "p"), "set credentials"),
        (("set", "auth", "u", "p"), "set auth"),
        (("set", "headers", '{"Authorization": "x"}'), "set headers"),
        (("open", "https://example.com", "--headers", "{}"), "--headers"),
        (("batch", "open https://example.com", "state load x"), "state"),
        (("batch", "--bail", "snapshot", "'cookies' set a b"), "cookies set"),
    ],
)
def test_a_login_is_refused_wherever_the_command_stands(engine, capsys, args, named):
    assert _run(*args) == protocol.REFUSED
    assert capsys.readouterr().err == protocol.LOGGED_OUT_MESSAGE.format(named) + "\n"
    assert engine == {}


def test_a_batch_read_from_stdin_is_checked_and_handed_on(engine, monkeypatch, capsys):
    monkeypatch.setattr(wrapper.sys, "stdin", io.TextIOWrapper(io.BytesIO(b'[["state", "load", "x"]]')))
    assert _run("batch") == protocol.REFUSED
    assert capsys.readouterr().err == protocol.LOGGED_OUT_MESSAGE.format("state") + "\n"

    handed = []
    monkeypatch.setattr(wrapper, "_restore_stdin", handed.append)
    uses = []
    monkeypatch.setattr(gate, "check", lambda use, user_agent: (uses.append(use) or _STANDARD, lambda: None))
    monkeypatch.setattr(wrapper.sys, "stdin", io.TextIOWrapper(io.BytesIO(b'[["click", "@e1"]]')))
    assert _run("--json", "batch", "--bail") is None
    assert handed == [b'[["click", "@e1"]]']
    assert uses == ["act"]

    # Read past an option the wrapper does not know, as the engine reads it.
    monkeypatch.setattr(wrapper.sys, "stdin", io.TextIOWrapper(io.BytesIO(b'[["auth", "login"]]')))
    assert _run("--allow-unknown", "batch") == protocol.REFUSED
    assert capsys.readouterr().err == protocol.LOGGED_OUT_MESSAGE.format("auth") + "\n"


def test_a_state_after_a_wait_is_still_a_saved_login(engine, capsys):
    """The engine reads ``--state`` as a saved login to load wherever it stands,
    after ``wait`` too: it fails there on a file named ``visible``."""
    assert _run("wait", "#spinner", "--state", "hidden") == protocol.REFUSED
    assert capsys.readouterr().err == protocol.LOGGED_OUT_MESSAGE.format("--state") + "\n"
    assert _run("--state", "saved.json", "wait", "@e1") == protocol.REFUSED


@pytest.mark.parametrize(
    ("args", "use"),
    [(("window", "new"), "grow"), (("window", "close"), "act"), (("quit",), "close"), (("exit",), "close")],
)
def test_a_new_window_opens_like_a_tab_and_quit_closes(args: tuple[str, ...], use: str):
    """A window holds memory as a tab does; ``quit`` and ``exit`` are the
    engine's names for ``close``."""
    assert _use(*args) == use


def test_no_config_file_on_the_computer_reaches_the_engine(engine, tmp_path, monkeypatch):
    """A config file in the folder a command runs from, or in the home
    directory, could launch the browser another way, or differently from the
    person's commands, which run from elsewhere."""
    monkeypatch.setenv("AGENT_BROWSER_CONFIG", str(tmp_path / "theirs.json"))
    assert _run("snapshot") is None
    named = Path(engine["env"]["AGENT_BROWSER_CONFIG"])
    assert named.parent == Path(wrapper.__file__).parent
    assert json.loads(named.read_text()) == {}


@pytest.mark.parametrize(
    ("args", "use"),
    [
        (("--download-path", "snapshot", "click", "@e1"), "act"),
        (("--json", "--download-path", "/tmp", "snapshot"), "look"),
        (("--hide-scrollbars", "false", "snapshot"), "look"),
        (("wait", "--fn", "window.done"), "grow"),
        (("wait", "--text", "Done"), "look"),
        (("diff", "url", "https://a.example", "https://b.example"), "act"),
        (("diff", "snapshot"), "look"),
        (("batch", "snapshot", "get url"), "look"),
        (("batch", "snapshot", "click @e1"), "act"),
        (("batch", "tab new", "click @e1"), "grow"),
        (("batch", "snapshot", "close"), "closes"),
        (("batch", "--bail", "snapshot", "batch 'tab new'"), "grow"),
    ],
)
def test_the_use_is_the_command_the_engine_runs(args: tuple[str, ...], use: str):
    assert _use(*args) == use


def test_every_request_goes_through_the_proxy_loopback_included(engine, monkeypatch):
    monkeypatch.setenv("AGENT_BROWSER_PROXY", "http://elsewhere:1")
    monkeypatch.setenv("AGENT_BROWSER_PROXY_BYPASS", "localhost")
    monkeypatch.setenv("AGENT_BROWSER_ARGS", "--proxy-bypass-list=*")
    assert _run("open", "http://localhost:3000") is None
    env = engine["env"]
    assert env["AGENT_BROWSER_PROXY"] == f"http://127.0.0.1:{protocol.PROXY_PORT}"
    assert env["AGENT_BROWSER_PROXY_BYPASS"] == "<-loopback>"
    assert env["AGENT_BROWSER_ARGS"].splitlines() == [
        "--proxy-bypass-list=<-loopback>",
        # The page 1280 by 800 under headless Chrome's undrawn toolbar, which the
        # engine's own 1280 by 720 window left a strip 577 tall.
        "--window-size=1280,943",
        "--disk-cache-size=67108864",
    ]
    assert (env["AGENT_BROWSER_STREAM_MAX_WIDTH"], env["AGENT_BROWSER_STREAM_MAX_HEIGHT"]) == ("1280", "800")
    assert env["AGENT_BROWSER_STREAM_QUALITY"] == "70"


def test_retina_renders_at_twice_the_page_and_streams_it_whole(engine, monkeypatch):
    monkeypatch.setattr(gate, "check", _admits(Launch("", "retina")))
    assert _run("open", "https://example.com") is None
    env = engine["env"]
    # Emulating the scale for the page leaves the screencast at one pixel a page pixel.
    assert "--force-device-scale-factor=2" in env["AGENT_BROWSER_ARGS"].splitlines()
    assert "--window-size=1280,943" in env["AGENT_BROWSER_ARGS"].splitlines()
    assert (env["AGENT_BROWSER_STREAM_MAX_WIDTH"], env["AGENT_BROWSER_STREAM_MAX_HEIGHT"]) == ("2560", "1600")


def test_a_gate_that_cannot_settle_runs_the_launch_on_record(engine, monkeypatch):
    """A broken gate costs the view, never a restart of the open browser."""

    def broken(use, user_agent):
        raise OSError("no state directory")

    monkeypatch.setattr(gate, "check", broken)
    monkeypatch.setattr(launch, "current", lambda: Launch("Open a@example.com", "retina"))
    assert _run("snapshot") is None
    assert engine["env"]["AGENT_BROWSER_USER_AGENT"] == "Open a@example.com"
    assert engine["env"]["AGENT_BROWSER_STREAM_MAX_WIDTH"] == "2560"


@pytest.mark.parametrize(
    ("cpu_max", "chosen", "launched"),
    [
        ("max 100000", None, "standard"),
        ("100000 100000", None, "standard"),
        ("200000 100000", None, "standard"),
        ("400000 100000", None, "retina"),
        ("100000 100000", "light", "light"),
        ("200000 100000", "retina", "retina"),
        # Retina's frames would take the only CPU from the computer's work.
        ("100000 100000", "retina", "sharp"),
    ],
)
def test_a_browser_starts_with_the_chosen_quality_or_one_its_size_can_keep_up_with(
    state, cpu_max, chosen, launched
):
    (system._CGROUP / "cpu.max").write_text(cpu_max)
    if chosen:
        _choose(quality=chosen)
    assert _check("act") == Launch("", launched)
    assert launch.current() == Launch("", launched)


def test_choices_found_garbled_are_none(state):
    (system._CGROUP / "cpu.max").write_text("400000 100000")
    _choose(quality="light")
    stored = json.loads(protocol.state_file(choices._FILE).read_text())
    protocol.write_state(choices._FILE, json.dumps({**stored, "quality": "garbage"}))
    assert choices.held() is None
    assert quality.chosen() == "retina"


def test_an_open_browser_keeps_its_quality_until_it_closes(state, monkeypatch):
    """A launch option that changes between commands restarts the browser and
    loses its pages."""
    _choose(quality="light")
    assert _check("act").quality == "light"
    monkeypatch.setattr(daemon, "listening", _listening(True))
    _choose(quality="sharp")
    assert _check("look").quality == "light"
    assert _check("close").quality == "light"
    monkeypatch.setattr(daemon, "listening", _listening(False))
    assert _check("act").quality == "sharp"


def test_a_machine_with_no_cpu_limit_counts_its_cpus(state, monkeypatch):
    monkeypatch.setattr(quality.os, "cpu_count", lambda: 8)
    assert quality.cpus() == 8
    (system._CGROUP / "cpu.max").write_text("150000 100000")
    assert quality.cpus() == 1.5


@pytest.mark.parametrize(("quota", "cpus"), [("100000", 1.0), ("-1", 8.0)])
def test_a_cgroup_v1_cpu_quota_is_the_computer_s_limit(state, monkeypatch, quota, cpus):
    monkeypatch.setattr(quality.os, "cpu_count", lambda: 8)
    (system._CGROUP / "cpu").mkdir()
    (system._CGROUP / "cpu" / "cpu.cfs_quota_us").write_text(f"{quota}\n")
    (system._CGROUP / "cpu" / "cpu.cfs_period_us").write_text("100000\n")
    assert quality.cpus() == cpus


def test_no_hold_is_taken_or_kept_on_a_browser_that_has_closed(state, capsys, monkeypatch):
    """It would only keep the agent from opening the next browser until it lapsed."""
    holder = "11111111-1111-4111-8111-111111111111"
    up = [False]
    monkeypatch.setattr(care.Look, "browser_up", lambda self: up[0])
    assert machine.main(["control", holder, "take"]) == 0
    assert json.loads(capsys.readouterr().out) == {"held": False}
    assert _check("act") == _STANDARD
    up[0] = True
    assert machine.main(["control", holder, "take"]) == 0
    assert json.loads(capsys.readouterr().out) == {"held": True}
    up[0] = False
    assert machine.main(["control", holder, "renew"]) == 0
    assert json.loads(capsys.readouterr().out) == {"held": False}
    assert hold.held() is False


@pytest.mark.parametrize(
    "argv", [["nothing"], [], ["quality", "sharp"], ["status", "1", "2"], ["start", "soon"], ["choices"], ["install", "x"]]
)
def test_the_usage_names_every_command(state, capsys, argv):
    assert machine.main(argv) == 2
    usage = capsys.readouterr().err
    for word in ("status", "start", "control", "navigate", "reload", "choices", "signins_revision", "serve", "proxy"):
        assert word in usage


def test_where_the_browser_must_not_go_is_one_list(state):
    """Chrome's policy names each place, and the proxy refuses each address."""
    blocked = SANDBOX_CHROME_POLICY["URLBlocklist"]
    for name in (*protocol.METADATA_HOSTS, *protocol.DOCKER_HOSTS):
        assert (f"[{name}]" if ":" in name else name) in blocked
        try:
            address = ipaddress.ip_address(name)
        except ValueError:
            continue
        assert proxy.refused(address, 80, proxy.Places())


#: Saves of the computer's choices, in the order the server made them.
_EARLIER, _LATER, _LATEST = "0000000000000000000a", "0000000000000000000b", "0000000000000000000c"


def test_a_browser_starts_on_the_kept_profile_and_keeps_its_profile_until_it_closes(state, monkeypatch):
    """Changing the profile under an open browser would restart it and lose its pages."""
    _choose(signins=True)
    assert _check("act").kept is True
    monkeypatch.setattr(daemon, "listening", _listening(True))
    _choose(signins=False)
    assert _check("look").kept is True
    monkeypatch.setattr(daemon, "listening", _listening(False))
    monkeypatch.setattr(care.Look, "engine_gone", lambda self: True)
    assert _check("act").kept is False


def test_a_command_joining_a_browser_still_starting_keeps_the_launch_it_starts_with(state, monkeypatch):
    """Before its stream listens, a browser another command is opening is not up,
    and a save made meanwhile would otherwise restart it."""
    _choose(quality="light")
    assert _check("act").quality == "light"
    _choose(quality="retina")
    assert _check("look").quality == "light"


def test_a_read_joining_a_browser_still_starting_is_not_refused_for_memory(state, monkeypatch):
    _check("act")
    monkeypatch.setattr(care, "memory_full", lambda: True)
    _check("look")
    with pytest.raises(gate.Refused):
        _check("grow")


@pytest.mark.parametrize("full", [True, False])
def test_a_start_that_ended_without_a_browser_is_not_joined(state, monkeypatch, full):
    """The engine's command the gate's first reading showed may have ended
    since, so the gate reads again before joining what it was starting."""
    ended = []
    monkeypatch.setattr(care.Look, "reread", lambda self: ended.append(True) or self)
    monkeypatch.setattr(care.Look, "engine_gone", lambda self: bool(ended))
    _choose(quality="light")
    assert _check("act").quality == "light"
    _choose(quality="sharp")
    monkeypatch.setattr(care, "memory_full", lambda: full)
    if full:
        with pytest.raises(gate.Refused):
            _check("look")
    else:
        assert _check("look").quality == "sharp"
    assert ended


def test_asking_for_chrome_behind_a_setup_under_way_queues_the_download(monkeypatch):
    """The setup under way may be one an acquire began, which downloads nothing."""
    detached = []
    monkeypatch.setattr(daemon, "detach", lambda *argv, log=None: detached.append(argv))
    monkeypatch.setattr(protocol, "installing", lambda: True)
    install.get_chrome()
    assert detached == [("install", "chrome", "--wait")]


@pytest.mark.parametrize("wait", ["nan", "inf", "-inf"])
def test_a_status_wait_that_would_never_end_is_refused(state, capsys, wait):
    assert machine.main(["status", wait]) == 2


@pytest.mark.parametrize("kept", [True, False])
def test_only_a_kept_launch_runs_the_engine_on_the_kept_profile(state, engine, monkeypatch, kept):
    monkeypatch.setenv("AGENT_BROWSER_PROFILE", "/home/someone/.config/google-chrome")
    monkeypatch.setattr(gate, "check", _admits(Launch("", "standard", kept=kept)))
    assert _run("open", "https://example.com") is None
    assert engine["env"].get("AGENT_BROWSER_PROFILE") == (signin.profile() if kept else None)


def test_a_chrome_on_the_kept_profile_is_the_engine_s_and_its_profile_outlives_a_reap(state, monkeypatch):
    """Up while its daemon runs; reaped once that died, the sign-ins staying for the next."""
    profile = Path(signin.profile())
    profile.mkdir()
    processes = state / "processes"
    _read_proc(monkeypatch, processes)
    monkeypatch.setattr(care, "_GONE_S", 0)
    _fake_proc(processes, 1, 0, ["/sbin/init"], exe="/sbin/init")
    _fake_proc(processes, 2, 1, [protocol.ENGINE_BIN], exe=protocol.ENGINE_BIN)
    _fake_proc(processes, 20, 2, [protocol.CHROME_BIN, f"--user-data-dir={profile}"])
    assert care.look().browser_up() is True
    shutil.rmtree(processes / "2")
    killed = []
    monkeypatch.setattr(care.os, "kill", lambda pid, sig: killed.append(pid))
    care.tend(False)
    assert killed == [20]
    assert profile.exists()


def test_ending_the_kept_profile_s_browser_ends_all_of_its_processes_first(state, monkeypatch):
    """Chrome's own processes show their arguments as one string, so only the
    browser names the profile; its network service, which keeps the cookies,
    would otherwise write them back after the profile is erased."""
    profile = Path(signin.profile())
    processes = state / "processes"
    _read_proc(monkeypatch, processes)
    _fake_proc(processes, 1, 0, ["/sbin/init"], exe="/sbin/init")
    _fake_proc(processes, 2, 1, [protocol.ENGINE_BIN], exe=protocol.ENGINE_BIN)
    _fake_proc(processes, 20, 2, [protocol.CHROME_BIN, f"--user-data-dir={profile}"])
    joined = f"{protocol.CHROME_BIN} --type=zygote --user-data-dir={profile}"
    _fake_proc(processes, 21, 20, [joined])
    _fake_proc(processes, 22, 21, [joined.replace("zygote", "utility")])
    _fake_proc(processes, 30, 1, [protocol.CHROME_BIN, "--user-data-dir=/home/someone/.config/chromium"])
    killed = []

    def kill(pid, sig):
        killed.append(pid)
        shutil.rmtree(processes / str(pid))

    monkeypatch.setattr(care.os, "kill", kill)
    with care.admitted() as seen:
        seen.end_on_profile()
    assert sorted(killed) == [2, 20, 21, 22]


def test_erasing_the_kept_profile_ends_a_browser_a_command_was_starting_on_it(state, monkeypatch):
    """Admitted before the erase, its Chrome shows only once the engine has
    started it, so the engine goes first, the daemon with the Chrome it runs,
    and the processes are read again."""
    _choose(signins=True)
    profile = Path(signin.profile())
    processes = state / "processes"
    _read_proc(monkeypatch, processes)
    _fake_proc(processes, 1, 0, ["/sbin/init"], exe="/sbin/init")
    _fake_proc(processes, 2, 1, [protocol.ENGINE_BIN], exe=protocol.ENGINE_BIN)
    _fake_proc(processes, 3, 1, [protocol.ENGINE_BIN, "open", "x"], exe=protocol.ENGINE_BIN)
    _fake_proc(processes, 21, 2, _ENGINE_CHROME)
    _fake_proc(processes, 22, 21, ["/chrome/renderer"])
    killed = []

    def kill(pid, sig):
        killed.append(pid)
        shutil.rmtree(processes / str(pid))
        if pid == 2:
            # The daemon had started it as it went.
            _fake_proc(processes, 20, 1, [protocol.CHROME_BIN, f"--user-data-dir={profile}"])

    monkeypatch.setattr(care.os, "kill", kill)
    with care.admitted() as seen:
        seen.end_on_profile()
    assert sorted(killed) == [2, 3, 20, 21, 22]


@pytest.mark.parametrize("recorded", [Launch("", "standard"), None], ids=["throwaway launch", "none, not kept"])
def test_a_command_starting_a_browser_on_a_profile_of_its_own_outlives_a_sign_out(
    state, monkeypatch, recorded, capsys
):
    """It cannot be starting one on the kept profile, which is all a sign-out erases."""
    if recorded:
        launch.write(recorded)
        _choose(signins=True)
    processes = state / "processes"
    _read_proc(monkeypatch, processes)
    _fake_proc(processes, 1, 0, ["/sbin/init"], exe="/sbin/init")
    _fake_proc(processes, 2, 1, [protocol.ENGINE_BIN], exe=protocol.ENGINE_BIN)
    _fake_proc(processes, 3, 1, [protocol.ENGINE_BIN, "open", "x"], exe=protocol.ENGINE_BIN)
    _fake_proc(processes, 20, 2, _ENGINE_CHROME)
    killed = []
    monkeypatch.setattr(care.os, "kill", lambda pid, sig: killed.append(pid))
    assert _hand(capsys, signins_revision=_LATER) is True
    assert choices.held().signins_revision == _LATER
    assert killed == []
    assert launch.current() == recorded


def test_the_lock_a_killed_chrome_left_on_the_kept_profile_is_cleared_only_with_none_on_it(state, monkeypatch):
    """Chrome takes over its own stale lock only when it names this host, and a
    computer restored or moved comes back under another name."""
    profile = Path(signin.profile())
    profile.mkdir()
    (profile / "SingletonLock").symlink_to("otherhost-99")
    processes = state / "processes"
    _read_proc(monkeypatch, processes)
    _fake_proc(processes, 1, 0, ["/sbin/init"], exe="/sbin/init")
    _fake_proc(processes, 20, 1, [protocol.CHROME_BIN, f"--user-data-dir={profile}"])
    with care.admitted() as seen:
        seen.ready_profile()
    assert (profile / "SingletonLock").is_symlink()
    shutil.rmtree(processes / "20")
    _choose(signins=True)
    _check("act")
    assert not (profile / "SingletonLock").is_symlink()


def test_the_kept_profile_is_left_to_a_browser_a_command_is_starting(state, monkeypatch):
    """Its Chrome may not show yet, and would find its lock and its tabs gone."""
    profile = Path(signin.profile())
    (profile / "Default" / "Sessions").mkdir(parents=True)
    (profile / "SingletonLock").symlink_to("thishost-99")
    processes = state / "processes"
    _read_proc(monkeypatch, processes)
    _fake_proc(processes, 3, 1, [protocol.ENGINE_BIN, "open", "x"], exe=protocol.ENGINE_BIN)
    with care.admitted() as seen:
        seen.ready_profile()
    assert (profile / "SingletonLock").is_symlink()
    assert (profile / "Default" / "Sessions").exists()


def test_a_command_reads_the_machine_s_processes_once(state, monkeypatch):
    """A reading costs tens of milliseconds on a machine running a thousand."""
    reads = []
    real = system.processes
    monkeypatch.setattr(system, "processes", lambda wanted=None: reads.append(wanted) or real(wanted))
    _choose(signins=True)
    _check("act")
    assert len(reads) == 1


def test_a_window_on_the_screen_is_drawn_at_the_screens_own_scale():
    """Retina's forced scale would blur a window the person looks at; the
    stream's frames keep their cap either way."""
    retina = Launch("", "retina")
    shown = dataclasses.replace(retina, window=True)._browser_env()
    assert shown["AGENT_BROWSER_HEADED"] == "1"
    assert not any(a.startswith("--force-device-scale-factor") for a in shown["AGENT_BROWSER_ARGS"].split("\n"))
    assert shown["AGENT_BROWSER_STREAM_MAX_WIDTH"] == str(protocol.STREAM_QUALITIES["retina"].width)
    hidden = retina._browser_env()
    assert "AGENT_BROWSER_HEADED" not in hidden
    assert "--force-device-scale-factor=2" in hidden["AGENT_BROWSER_ARGS"].split("\n")


def test_a_window_on_the_screen_stays_until_the_person_closes_it(state, monkeypatch):
    """The engine cannot see the person using a window, and keeps a headed one
    open unless an idle timeout is given; a hidden browser closes when unused."""
    monkeypatch.setenv("AGENT_BROWSER_IDLE_TIMEOUT_MS", "5")
    h = host.current()
    shown = Launch("", "standard", window=True).engine_env(os.environ, h)
    hidden = Launch("", "standard").engine_env(os.environ, h)
    assert "AGENT_BROWSER_IDLE_TIMEOUT_MS" not in shown
    assert hidden["AGENT_BROWSER_IDLE_TIMEOUT_MS"] == "1200000"


@pytest.mark.parametrize(
    ("platform", "own", "desktop", "shows"),
    [
        ("darwin", True, "Aqua", True),
        # A login over the Mac's own SSH runs outside the person's desktop.
        ("darwin", True, "Background", False),
        ("darwin", False, "Aqua", False),
        ("linux", True, "Aqua", False),
    ],
)
def test_only_a_persons_own_mac_in_its_desktop_shows_a_window(state, monkeypatch, platform, own, desktop, shows):
    monkeypatch.setattr(window.sys, "platform", platform)
    monkeypatch.setattr(window.host, "current", lambda: Host(own=own))
    monkeypatch.setattr(
        window.subprocess, "run", lambda *a, **k: subprocess.CompletedProcess(a, 0, stdout=f"{desktop}\n")
    )
    assert not window.chosen()
    _choose(window=True)
    assert window.chosen() is shows
    _choose(window=False)
    assert not window.chosen()


def test_an_open_browser_keeps_whether_it_shows_a_window(state, monkeypatch):
    """Showing one or not restarts the engine's browser, so a change waits for
    the next browser, as the quality does."""
    monkeypatch.setattr(window, "chosen", lambda: True)
    assert _check("act").window
    monkeypatch.setattr(window, "chosen", lambda: False)
    monkeypatch.setattr(gate.daemon, "listening", lambda port: True)
    assert _check("act").window
    assert launch.current().window


def test_the_kept_profile_continues_where_it_left_off_but_on_no_earlier_tabs(state):
    """Chrome keeps a session's cookies only when it continues where it left off,
    and the tabs that would reopen are another thread's."""
    kept = dataclasses.replace(_STANDARD, kept=True)._browser_env()
    assert "--restore-last-session" in kept["AGENT_BROWSER_ARGS"].split("\n")
    assert "--restore-last-session" not in _STANDARD._browser_env()["AGENT_BROWSER_ARGS"]
    default = Path(signin.profile()) / "Default"
    (default / "Sessions").mkdir(parents=True)
    (default / "Sessions" / "Tabs_1").write_text("last session")
    (default / "Preferences").write_text("{}")
    with care.admitted() as seen:
        seen.ready_profile()
    assert not (default / "Sessions").exists()
    assert (default / "Preferences").read_text() == "{}"


def _choices(**changed: object) -> str:
    """The computer's choices as the server hands them over, with *changed*."""
    chosen: dict[str, object] = {
        "quality": "sharp",
        "window": False,
        "signins": True,
        "signins_revision": _LATER,
        "revision": _LATER,
        "computer": "c-1",
    }
    chosen.update(changed)
    return json.dumps(chosen)


def _hand(capsys, **changed: object) -> bool:
    """Hand the machine the choices with *changed*; whether it answers done."""
    assert machine.main(["choices", _choices(**changed)]) == 0
    return json.loads(capsys.readouterr().out)["done"]


def test_the_server_hands_the_machine_every_choice_at_once(state, capsys, monkeypatch):
    monkeypatch.setattr(window, "can_show", lambda: True)
    assert _hand(capsys, window=True) is True
    assert choices.held() == protocol.Choices("sharp", True, True, _LATER, _LATER, "c-1")
    assert (quality.chosen(), window.chosen(), signin.kept()) == ("sharp", True, True)
    # No quality follows the computer's size.
    assert _hand(capsys, quality=None) is True
    assert (choices.held().quality, quality.chosen(), window.chosen()) == (None, "standard", False)
    # One record, written whole.
    assert sorted(path.name for path in state.iterdir() if not path.name.endswith(".lock")) == [choices._FILE]


def test_the_choices_wait_for_a_command_being_admitted(state, monkeypatch):
    """A browser starts on all of one save's choices, never on some of them."""
    reached = threading.Event()
    take = care.lock

    def observed(name, *, wait=True):
        if name == care.ADMISSION_LOCK:
            reached.set()
        return take(name, wait=wait)

    monkeypatch.setattr(care, "lock", observed)
    saving = threading.Thread(target=machine.main, args=(["choices", _choices()],), daemon=True)
    with protocol.locked(care.ADMISSION_LOCK):
        saving.start()
        assert reached.wait(5)
        saving.join(0.2)
        assert saving.is_alive()
        assert choices.held() is None
    saving.join(5)
    assert not saving.is_alive()
    assert choices.held().quality == "sharp"


@pytest.mark.parametrize(
    "given",
    [
        "not json",
        "[]",
        json.dumps({"quality": "sharp", "window": False, "signins": True, "signins_revision": _LATER}),
        _choices(quality="ultra"),
        _choices(quality=["sharp"]),
        _choices(window="on"),
        _choices(window=1),
        _choices(signins=None),
        _choices(signins_revision="not-a-revision"),
        _choices(signins_revision=10),
        _choices(revision=_LATER.upper()),
        _choices(extra=1),
    ],
)
def test_choices_the_machine_cannot_read_change_nothing(state, capsys, monkeypatch, given):
    """Read whole before any is kept, so a malformed one leaves none half kept."""
    ended = []
    monkeypatch.setattr(care.Look, "end_on_profile", lambda self: ended.append(True) or self)
    _choose(quality="light")
    held = choices.held()
    assert machine.main(["choices", given]) == 0
    assert json.loads(capsys.readouterr().out) == {"done": False}
    assert (choices.held(), ended) == (held, [])


def test_a_save_older_than_the_one_held_changes_nothing(state, capsys, monkeypatch):
    """However late it arrives, it never erases a sign-in made since, nor takes
    back what a later save chose."""
    ended = []
    monkeypatch.setattr(care.Look, "end_on_profile", lambda self: ended.append(True) or self)
    profile = Path(signin.profile())
    profile.mkdir()
    assert _hand(capsys) is True
    assert (profile.exists(), ended) == (False, [True])
    held = choices.held()

    profile.mkdir()
    assert _hand(capsys, quality="light", signins=False, signins_revision=_EARLIER, revision=_EARLIER) is True
    assert (choices.held(), profile.exists(), ended) == (held, True, [True])


def test_a_save_at_the_held_revision_is_taken_again_and_signs_nothing_out(state, capsys, monkeypatch):
    """The server hands the same save again whenever it cannot tell it landed."""
    ended = []
    monkeypatch.setattr(care.Look, "end_on_profile", lambda self: ended.append(True) or self)
    assert _hand(capsys) is True
    profile = Path(signin.profile())
    profile.mkdir()
    assert _hand(capsys, quality="light") is True
    assert (choices.held().quality, profile.exists(), ended) == ("light", True, [True])


def test_only_a_newer_sign_ins_save_erases_the_kept_profile(state, capsys, monkeypatch):
    """A save of the quality or the window keeps the sign-ins; one of the
    sign-ins, even to the same choice, is how the person signs out."""
    ended = []
    monkeypatch.setattr(care.Look, "end_on_profile", lambda self: ended.append(True) or self)
    assert _hand(capsys, signins_revision=_EARLIER, revision=_EARLIER) is True
    profile = Path(signin.profile())
    profile.mkdir()
    assert _hand(capsys, quality="light", signins_revision=_EARLIER, revision=_LATER) is True
    assert (profile.exists(), len(ended)) == (True, 1)
    assert _hand(capsys, signins_revision=_LATEST, revision=_LATEST) is True
    assert (profile.exists(), len(ended)) == (False, 2)


def test_a_sign_out_that_cannot_erase_keeps_nothing_and_says_so(state, capsys, monkeypatch):
    """The server hands the save again, and the machine erases it then."""
    assert _hand(capsys, signins_revision=_EARLIER, revision=_EARLIER) is True
    held = choices.held()
    profile = Path(signin.profile())
    (profile / "Default").mkdir(parents=True)

    real, failing = shutil.rmtree, [True]

    def rmtree(path, *args, **kwargs):
        if failing:
            raise PermissionError(13, "Permission denied", path)
        return real(path, *args, **kwargs)

    monkeypatch.setattr(signin.shutil, "rmtree", rmtree)
    assert _hand(capsys) is False
    assert choices.held() == held
    failing.clear()
    assert _hand(capsys) is True
    assert not profile.exists()
    assert choices.held() == protocol.Choices("sharp", False, True, _LATER, _LATER, "c-1")


def test_a_folder_taken_over_from_another_computer_starts_from_this_one_s_choices(state, capsys, monkeypatch):
    """Revisions order one computer's saves only. The kept profile the other
    computer left, its sign-ins, goes with its choices, however new they are."""
    ended = []
    monkeypatch.setattr(care.Look, "end_on_profile", lambda self: ended.append(True) or self)
    assert _hand(capsys, computer="c-old", quality="light") is True
    profile = Path(signin.profile())
    profile.mkdir()
    assert _hand(capsys, computer="c-new", signins_revision=_EARLIER, revision=_EARLIER) is True
    assert (profile.exists(), len(ended)) == (False, 2)
    assert choices.held() == protocol.Choices("sharp", False, True, _EARLIER, _EARLIER, "c-new")


def test_an_engine_upgrade_ends_what_the_old_engine_runs_before_removing_it(state, monkeypatch):
    """On Linux a process whose binary is gone reads as that path marked
    deleted, which no reading would know as the engine's to end."""
    engines = protocol.state_file("engine")
    engines.mkdir()
    old, new = engines / "agent-browser-0.1.0", engines / f"agent-browser-{protocol.ENGINE_VERSION}"
    for binary in (old, new):
        binary.write_text("#!/bin/sh\n")
        binary.chmod(0o755)
    chrome = state / "chrome-bin"
    _use_host(monkeypatch, own=True, engine=str(old), chrome=str(chrome), executable=str(chrome))
    monkeypatch.setattr(install, "platform_name", lambda: "linux")
    monkeypatch.setattr(install, "find_chrome", lambda: str(chrome))
    monkeypatch.setattr(install, "_executable", lambda found: found)
    monkeypatch.setattr(host, "record", lambda recorded: None)
    monkeypatch.setattr(daemon, "listening", lambda port, wait_s=0.0: False)
    ended, navigating = [], []

    @contextlib.contextmanager
    def held_navigation():
        navigating.append("in")
        yield
        navigating.append("out")

    # A person's navigation runs the engine outside admission, so the swap waits for it.
    monkeypatch.setattr(hold, "navigating", held_navigation)
    monkeypatch.setattr(care.Look, "end_all", lambda self: ended.append((old.exists(), list(navigating))) or self)
    assert install.install()["done"] is True
    assert ended == [(True, ["in"])]
    assert navigating == ["in", "out"]
    assert sorted(path.name for path in engines.iterdir()) == [new.name]


class _Seen:
    """A reading of what runs, for the port choice: *running* is a browser's."""

    def __init__(self, running: bool) -> None:
        self.running, self.closed = running, []

    def daemons(self):
        return ["daemon"] if self.running else []

    def browsers(self):
        return []

    def busy(self):
        return False

    def services(self):
        return ["proxy"]

    def close(self, ending):
        self.closed.extend(ending)
        return self


@pytest.mark.parametrize(("running", "taken", "kept"), [(True, True, True), (False, False, True), (False, True, False)])
def test_a_person_s_machine_keeps_its_ports_until_another_program_takes_one(monkeypatch, running, taken, kept):
    """Whatever listens on a recorded port passes for the browser's own, so
    one taken while the browser is closed is given up for three free ones."""
    before = Host(own=True, engine="e", chrome="c", executable="c", stream_port=41001, bridge_port=41002, proxy_port=41003)
    monkeypatch.setattr(daemon, "listening", lambda port, wait_s=0.0: taken and port == 41003)
    monkeypatch.setattr(install, "_free_ports", lambda: (42001, 42002, 42003))
    seen = _Seen(running)
    chosen = install._ports(seen, before)
    assert chosen == ((41001, 41002, 41003) if kept else (42001, 42002, 42003))
    # The runtime's own idle proxy and bridge are ended before the ports are checked.
    assert seen.closed == ([] if running else ["proxy"])


def test_a_runtime_before_the_record_starts_from_the_first_choices_handed(state, capsys):
    """A browser started meanwhile follows the computer's size and keeps no sign-ins."""
    assert (choices.held(), quality.chosen(), signin.kept()) == (None, "standard", False)
    assert _hand(capsys) is True
    assert (quality.chosen(), signin.kept()) == ("sharp", True)


def test_signing_out_ends_the_kept_browser_and_what_lasts_as_long_as_it(state, capsys, monkeypatch):
    """Erased under a browser still open, the profile would be written back as it closed."""
    _choose(signins=True)
    profile = Path(signin.profile())
    profile.mkdir()
    processes = state / "processes"
    _read_proc(monkeypatch, processes)
    _fake_proc(processes, 1, 0, ["/sbin/init"], exe="/sbin/init")
    _fake_proc(processes, 2, 1, [protocol.ENGINE_BIN], exe=protocol.ENGINE_BIN)
    _fake_proc(processes, 20, 2, [protocol.CHROME_BIN, f"--user-data-dir={profile}"])
    killed = []

    def kill(pid, sig):
        killed.append(pid)
        shutil.rmtree(processes / str(pid))

    monkeypatch.setattr(care.os, "kill", kill)
    real, erased_after = shutil.rmtree, []

    def rmtree(path, *args, **kwargs):
        if Path(path) == profile:
            erased_after.append(sorted(killed))
        return real(path, *args, **kwargs)

    monkeypatch.setattr(signin.shutil, "rmtree", rmtree)
    hold.control("view-1", "take")
    launch.write(Launch("", "sharp", kept=True))
    assert _hand(capsys) is True
    assert erased_after == [[2, 20]]
    assert (sorted(killed), profile.exists(), hold.held(), launch.current()) == ([2, 20], False, False, None)


def test_a_record_lacking_a_key_takes_its_default_and_one_of_another_kind_is_none(state):
    """A runtime that wrote fewer fields still reads; a value of the wrong kind,
    a bool for an int or a quality not on the list, is no record."""
    protocol.write_state("launch", json.dumps({"user_agent": "ua", "quality": "light"}))
    assert launch.current() == Launch("ua", "light")
    for stored in (
        {"quality": "light"},
        {"user_agent": "ua", "quality": "ultra"},
        {"user_agent": 1, "quality": "light"},
        {"user_agent": "ua", "quality": "light", "kept": 1},
        ["ua", "light"],
    ):
        protocol.write_state("launch", json.dumps(stored))
        assert launch.current() is None
    protocol.write_state("launch", "not json")
    assert launch.current() is None
    launch.write(Launch("ua", "retina", closing=True, window=True))
    assert launch.current() == Launch("ua", "retina", closing=True, window=True)


def test_a_host_record_short_of_any_field_is_the_image_s(state):
    """Read with the image's defaults, it would mix a person's machine with the
    image's engine or ports."""
    recorded = Host(own=True, engine="/e", chrome="/c", executable="/c", stream_port=20001, bridge_port=20002, proxy_port=20003)
    host.record(recorded)
    assert host.current() == recorded
    stored = json.loads((state / "host").read_text())
    short = {name: value for name, value in stored.items() if name != "proxy_port"}
    for record in (short, {**stored, "proxy_port": True}, {**stored, "extra": 1}):
        protocol.write_state("host", json.dumps(record))
        assert host.current() == Host()


def test_a_lock_not_waited_for_is_not_taken_while_held_elsewhere(state):
    with protocol.locked("test.lock") as taken:
        assert taken is True
        with protocol.locked("test.lock", wait=False) as again:
            assert again is False
    with protocol.locked("test.lock", wait=False) as free:
        assert free is True


def test_the_status_says_whether_the_open_browser_keeps_its_sign_ins(state, monkeypatch):
    monkeypatch.setattr(machine, "_browser_up", lambda deadline: True)
    protocol.write_state(protocol.TOKEN_FILE, "t" * 43)
    launch.write(Launch("", "standard", kept=True))
    assert machine.status().kept is True
    assert protocol.Status.read(machine.status().to_wire()).kept is True
    launch.write(Launch("", "standard"))
    assert machine.status().kept is False
