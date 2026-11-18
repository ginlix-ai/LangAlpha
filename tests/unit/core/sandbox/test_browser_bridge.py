"""The bridge is what stands in for the Origin check it removes, so what it lets
through is the contract; the wrapper and the gate decide what reaches the engine."""

from __future__ import annotations

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
    daemon,
    gate,
    hold,
    launch,
    machine,
    protocol,
    proxy,
    quality,
    relay,
    wrapper,
)
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


class _Ran(Exception):
    """Stands in for exec, which would replace the test process."""


_STANDARD = Launch("", "standard")
#: The real one: the ``state`` fixture stands a no-op in for it.
_TEND = care.tend
_REAL_CHECK = gate.check
_REAL_ENGINE_GONE = care.engine_gone
_REAL_CURRENT = launch.current


@pytest.fixture
def engine(tmp_path, monkeypatch):
    """What ``wrapper.main`` would exec: the argv and environment, or nothing."""
    binary = tmp_path / "engine"
    binary.write_text("#!/bin/sh\n")
    binary.chmod(0o755)
    monkeypatch.setattr(wrapper, "ENGINE_BIN", str(binary))
    monkeypatch.setattr(wrapper.os, "getuid", lambda: 1000)
    monkeypatch.setattr(wrapper, "_OOM_SCORE", tmp_path / "oom_score_adj")
    monkeypatch.setattr(gate, "check", lambda use, user_agent: _STANDARD)
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
    assert engine["argv"] == [wrapper.ENGINE_BIN, "--help"]
    assert _run("snapshot") == protocol.GATE_REFUSED


def test_a_computer_built_before_the_browser_says_so(engine, monkeypatch, capsys):
    monkeypatch.setattr(wrapper, "ENGINE_BIN", "/nowhere/agent-browser")
    assert _run("get", "url") == protocol.NOT_INSTALLED
    assert capsys.readouterr().err == protocol.NOT_INSTALLED_MESSAGE + "\n"


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


def test_the_wrapper_runs_a_command_with_the_user_agent_the_gate_holds(engine, monkeypatch):
    monkeypatch.setattr(gate, "check", lambda use, user_agent: Launch("Held a@example.com", "standard"))
    monkeypatch.setenv("AGENT_BROWSER_PROFILE", "Default")
    assert _run("get", "url") is None
    assert engine["argv"] == [wrapper.ENGINE_BIN, "get", "url"]
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
    expected = [wrapper.ENGINE_BIN, "get", "url"]
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
    monkeypatch.setattr(quality, "_CGROUP", tmp_path)
    monkeypatch.setattr(quality.os, "cpu_count", lambda: 1)
    monkeypatch.setattr(relay, "ensure", lambda wait_s=0.0: True)
    monkeypatch.setattr(proxy, "ensure", lambda: True)
    monkeypatch.setattr(care, "engine_gone", lambda admitting=False: False)
    monkeypatch.setattr(daemon, "listening", _listening(False))
    monkeypatch.setattr(care, "_CGROUP", tmp_path / "no-cgroup")
    monkeypatch.setattr(care, "_MEMINFO", tmp_path / "no-meminfo")
    monkeypatch.setattr(care, "tend", lambda up, admitting=False: None)
    monkeypatch.setattr(gate, "_admission", None)
    # No process of the host's: the gate ends a browserless daemon it finds.
    monkeypatch.setattr(care, "_PROC", tmp_path / "no-proc")
    return directory


def test_reading_runs_and_acting_is_refused_while_the_person_holds_the_page(state):
    assert hold.control("a", "take") is True
    assert gate.check("look") == _STANDARD
    with pytest.raises(gate.Refused, match=re.escape(protocol.HELD_MESSAGE)):
        gate.check("act")


@pytest.mark.parametrize("failing", ["proxy", "bridge"])
def test_a_start_that_cannot_spawn_still_leaves_the_hold_to_decide(state, monkeypatch, failing):
    """Short of memory a fork fails, which is when the person's hold matters most."""

    def no_fork(*_args, **_kwargs):
        raise OSError(12, "Cannot allocate memory")

    monkeypatch.setattr(proxy if failing == "proxy" else relay, "ensure", no_fork)
    monkeypatch.setattr(daemon, "listening", _listening(True))
    hold.control("a", "take")
    assert gate.check("look") == _STANDARD
    with pytest.raises(gate.Refused, match=re.escape(protocol.HELD_MESSAGE)):
        gate.check("act")


def test_a_hold_that_stopped_renewing_lapses_by_itself(state, monkeypatch):
    hold.control("a", "take")
    later = time.time() + hold.CONTROL_LEASE_S + 1
    monkeypatch.setattr(hold.time, "time", lambda: later)
    assert hold.held() is False
    assert hold.control("a", "renew") is False


def test_acting_after_a_hold_is_refused_once_until_the_page_is_read(state):
    assert gate.check("act") == _STANDARD
    hold.control("a", "take")
    hold.control("a", "release")
    with pytest.raises(gate.Refused, match=re.escape(protocol.CHANGED_MESSAGE)):
        gate.check("act")
    assert gate.check("act") == _STANDARD

    hold.control("a", "take")
    hold.control("a", "release")
    assert gate.check("look") == _STANDARD
    assert gate.check("act") == _STANDARD


def test_a_read_during_a_hold_does_not_clear_the_refusal_after_it(state):
    hold.control("a", "take")
    assert gate.check("look") == _STANDARD
    hold.control("a", "release")
    with pytest.raises(gate.Refused, match=re.escape(protocol.CHANGED_MESSAGE)):
        gate.check("act")


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
        gate.check("act")


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
    assert gate.check("act", "One a@example.com") == one
    up[0] = True
    assert gate.check("look") == one
    assert gate.check("act", "One a@example.com") == one

    with pytest.raises(gate.Refused, match=re.escape(protocol.USER_AGENT_MESSAGE)):
        gate.check("look", "Two a@example.com")
    assert gate.check("close") == dataclasses.replace(one, closing=True)
    # A close that never finished: the browser is up on what it was launched with.
    assert gate.check("look") == one
    assert gate.check("close") == dataclasses.replace(one, closing=True)
    with pytest.raises(gate.Refused):
        gate.check("look", "Two a@example.com")
    up[0] = False
    assert gate.check("act", "Two a@example.com") == Launch("Two a@example.com", "standard")


def test_a_user_agent_set_on_a_running_browser_is_refused(state, monkeypatch):
    monkeypatch.setattr(daemon, "listening", _listening(True))
    with pytest.raises(gate.Refused, match=re.escape(protocol.USER_AGENT_MESSAGE)):
        gate.check("act", "Late a@example.com")


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
    daemons = [care._Process(pid=7, ppid=1, uid=0, argv=[])] if engine == "daemon" else []
    monkeypatch.setattr(care, "engine_gone", _REAL_ENGINE_GONE)
    monkeypatch.setattr(care, "end_browserless", lambda admitting=False: False)
    monkeypatch.setattr(care, "_engine_daemons", lambda processes: daemons)
    monkeypatch.setattr(
        care, "_engine_busy", lambda processes, exclude=None, wrappers=True: engine == "command"
    )
    monkeypatch.setattr(daemon, "listening", _listening(stream_up))
    gate.check("look")
    assert hold.held() is kept
    assert (launch.current().user_agent == "Kept a@example.com") is kept


def test_a_restarted_computer_keeps_nothing_of_the_last_browser(state, monkeypatch):
    """Its state files are left and every process is gone."""
    hold.control("a", "take")
    launch.write(Launch("Before the restart", "light"))
    monkeypatch.setattr(care, "engine_gone", lambda admitting=False: True)
    assert gate.check("act") == _STANDARD
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
    monkeypatch.setattr(care, "_CGROUP", state)
    monkeypatch.setattr(daemon, "listening", _listening(up))
    if refused:
        with pytest.raises(gate.Refused, match=re.escape(protocol.MEMORY_MESSAGE)):
            gate.check(use)
    else:
        assert dataclasses.replace(gate.check(use), closing=False) == _STANDARD


def test_a_batch_that_ends_closed_runs_on_its_browser_and_ends_the_user_agent(state, monkeypatch):
    """What runs before its close works on the page as it is, so the launch stays,
    since a changed one restarts the browser; the user agent ends with the
    browser, and stays if the close never ran. What runs first acts on the page,
    so a person's hold refuses it."""
    up = [True]
    monkeypatch.setattr(daemon, "listening", lambda port, wait_s=0.0: up[0])
    launch.write(Launch("Old", "light"))
    assert gate.check("closes") == Launch("Old", "light", closing=True)
    assert launch.current() == Launch("Old", "light", closing=True)
    # The batch stopped before its close: the browser and what it runs with stay.
    assert gate.check("look") == Launch("Old", "light")
    assert gate.check("closes") == Launch("Old", "light", closing=True)
    up[0] = False
    assert gate.check("act").user_agent == ""
    up[0] = True
    hold.control("a", "take")
    with pytest.raises(gate.Refused, match=re.escape(protocol.HELD_MESSAGE)):
        gate.check("closes")


def test_a_batch_that_closes_and_goes_on_starts_its_browser_afresh(state, monkeypatch):
    """Its close ends the browser and the launch on record with it, so the user
    agent and quality are this command's, as for any browser that starts."""
    monkeypatch.setattr(daemon, "listening", _listening(True))
    launch.write(Launch("Old", "light"))
    assert gate.check("reopen", "New") == Launch("New", "standard")
    assert gate.check("reopen") == Launch("", "standard")


def test_a_browser_ended_while_tending_is_one_this_command_must_open(state, monkeypatch):
    """Its Chrome died after the gate looked, so tending ended its daemon and
    stream: acting now opens a browser, which memory must have room for."""
    (state / "memory.max").write_text("1000\n")
    (state / "memory.stat").write_text("anon 590\nshmem 20\nfile 300\n")
    monkeypatch.setattr(care, "_CGROUP", state)
    stream = {"up": True}
    monkeypatch.setattr(daemon, "listening", lambda port, wait_s=0.0: stream["up"])
    monkeypatch.setattr(care, "tend", lambda up, admitting=False: stream.update(up=False))
    with pytest.raises(gate.Refused, match=re.escape(protocol.MEMORY_MESSAGE)):
        gate.check("act")


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
    monkeypatch.setattr(care, "_CGROUP", state)
    monkeypatch.setattr(care, "_MEMINFO", meminfo)
    assert care.memory_full() is full


@pytest.mark.parametrize(("rss", "full"), [(500, False), (590, True)])
def test_a_cgroup_v1_limit_is_the_computer_s_share(state, monkeypatch, rss, full):
    (state / "memory").mkdir()
    (state / "memory" / "memory.limit_in_bytes").write_text("1000\n")
    (state / "memory" / "memory.stat").write_text(f"rss 1\nshmem 1\ntotal_rss {rss}\ntotal_shmem 20\ntotal_cache 300\n")
    monkeypatch.setattr(care, "_CGROUP", state)
    assert care.memory_full() is full


def test_a_cgroup_v1_with_no_limit_is_measured_against_all_memory(state, monkeypatch, tmp_path):
    (state / "memory").mkdir()
    (state / "memory" / "memory.limit_in_bytes").write_text("9223372036854771712\n")
    (state / "memory" / "memory.stat").write_text("total_rss 590\ntotal_shmem 20\n")
    meminfo = tmp_path / "meminfo"
    meminfo.write_text("MemTotal:  1000 kB\nAnonPages:  590 kB\nShmem:  20 kB\n")
    monkeypatch.setattr(care, "_CGROUP", state)
    monkeypatch.setattr(care, "_MEMINFO", meminfo)
    assert care.memory_full() is True


def test_a_machine_that_says_neither_is_never_full(state, monkeypatch, tmp_path):
    (state / "memory.max").write_text("max\n")
    monkeypatch.setattr(care, "_CGROUP", state)
    monkeypatch.setattr(care, "_MEMINFO", tmp_path / "absent")
    assert care.memory_full() is False


#: How the engine starts each Chrome: by its name for it, on a throwaway profile.
_ENGINE_CHROME = [protocol.CHROME_BIN, f"--user-data-dir=/tmp/{protocol.ENGINE_PROFILE_PREFIX}x"]


def _fake_proc(root: Path, pid: int, ppid: int, argv: list[str], exe: str = "/bin/sh", cwd: str = "/") -> Path:
    """A process entry as ``care`` reads one: its stat, command line, exe and cwd."""
    entry = root / str(pid)
    entry.mkdir(parents=True)
    (entry / "stat").write_text(f"{pid} (a name) with spaces) S {ppid} 1 1 0")
    (entry / "cmdline").write_bytes("\0".join(argv).encode() + b"\0")
    (entry / "exe").symlink_to(exe)
    (entry / "cwd").symlink_to(cwd)
    (entry / "oom_score_adj").write_text("0")
    return entry


def test_one_reading_of_the_processes_carries_each_parent_owner_and_command(tmp_path, monkeypatch):
    monkeypatch.setattr(care, "_PROC", tmp_path)
    _fake_proc(tmp_path, 7, 3, ["python3", "-I", "x"])
    (tmp_path / "self").mkdir()
    assert care._processes() == [care._Process(7, 3, os.getuid(), ["python3", "-I", "x", ""])]


def test_only_a_browser_whose_daemon_died_is_reaped(tmp_path, monkeypatch):
    monkeypatch.setattr(protocol, "_STATE_ROOT", tmp_path)
    monkeypatch.setattr(care, "_BROWSER_GONE_S", 0)
    profile = tmp_path / f"{protocol.ENGINE_PROFILE_PREFIX}1"
    profile.mkdir()
    processes = tmp_path / "processes"
    monkeypatch.setattr(care, "_PROC", processes)
    _fake_proc(processes, 1, 0, ["/sbin/init"], exe="/sbin/init")
    _fake_proc(processes, 2, 1, [protocol.ENGINE_BIN], exe=protocol.ENGINE_BIN)
    _fake_proc(processes, 10, 1, [protocol.CHROME_BIN, f"--user-data-dir={profile}"])
    _fake_proc(processes, 20, 2, _ENGINE_CHROME)
    killed = []
    monkeypatch.setattr(care.os, "kill", lambda pid, sig: killed.append(pid))
    care.tend(False)
    assert killed == [10]
    assert not profile.exists()


def test_a_reaped_browser_takes_its_hold_and_launch_along(state, tmp_path, monkeypatch):
    """The next browser starts neither held nor on the dead one's user agent,
    though the bridge, still up, keeps the gate from forgetting them itself."""
    processes = tmp_path / "processes"
    monkeypatch.setattr(care, "_PROC", processes)
    _fake_proc(processes, 1, 0, ["/sbin/init"], exe="/sbin/init")
    _fake_proc(processes, 10, 1, _ENGINE_CHROME)
    monkeypatch.setattr(care.os, "kill", lambda pid, sig: shutil.rmtree(processes / str(pid)))
    hold.control("a", "take")
    launch.write(Launch("Old", "light"))
    _TEND(False)
    assert not (processes / "10").exists()
    assert (hold.held(), launch.current()) == (False, None)
    assert gate.check("act") == _STANDARD


def test_the_bridge_reaps_nothing_while_a_command_is_admitted(state, tmp_path, monkeypatch):
    """What the command records may be for the browser it is about to start;
    its own gate reaps, under the lock, before it records anything."""
    processes = tmp_path / "processes"
    monkeypatch.setattr(care, "_PROC", processes)
    _fake_proc(processes, 1, 0, ["/sbin/init"], exe="/sbin/init")
    _fake_proc(processes, 10, 1, _ENGINE_CHROME)
    killed = []
    monkeypatch.setattr(care.os, "kill", lambda pid, sig: killed.append(pid))
    launch.write(Launch("New", "light"))
    with open(state / care.ADMISSION_LOCK, "a") as admitting:
        fcntl.flock(admitting, fcntl.LOCK_EX)
        _TEND(False)
    assert (killed, launch.current()) == ([], Launch("New", "light"))


def test_a_chrome_the_agent_started_on_its_own_profile_is_never_reaped(tmp_path, monkeypatch):
    """Started from the same path, its launcher gone, but on a profile the
    engine did not make: neither it nor its profile is the runtime's."""
    monkeypatch.setattr(protocol, "_STATE_ROOT", tmp_path)
    profile = tmp_path / "my-profile"
    profile.mkdir()
    processes = tmp_path / "processes"
    monkeypatch.setattr(care, "_PROC", processes)
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
    monkeypatch.setattr(care, "_PROC", tmp_path)
    _fake_proc(tmp_path, 1, 0, ["/sbin/init"], exe="/sbin/init")
    _fake_proc(tmp_path, 10, 1, _ENGINE_CHROME)
    assert care.browser_up() is False
    _fake_proc(tmp_path, 2, 1, [protocol.ENGINE_BIN], exe=protocol.ENGINE_BIN)
    _fake_proc(tmp_path, 20, 2, _ENGINE_CHROME)
    assert care.browser_up() is True


def test_a_daemon_whose_chrome_died_is_ended_with_its_hold_and_launch(state, monkeypatch):
    """The OOM killer can take Chrome and leave the daemon's stream on the last
    frame; the closed browser's hold would then refuse the start that brings one back."""
    processes = state / "processes"
    monkeypatch.setattr(care, "_PROC", processes)
    monkeypatch.setattr(daemon, "listening", _listening(False))
    killed = []
    monkeypatch.setattr(care.os, "kill", lambda pid, sig: killed.append(pid))
    _fake_proc(processes, 1, 0, ["/sbin/init"], exe="/sbin/init")
    _fake_proc(processes, 2, 1, [protocol.ENGINE_BIN], exe=protocol.ENGINE_BIN)
    hold.control("view-1", "take")
    launch.write(Launch("", "standard"))
    assert care.end_browserless() is True
    assert killed == [2]
    assert hold.refusal("act") is None
    assert launch.current() is None


def test_a_daemon_with_its_chrome_or_one_still_starting_is_left(state, monkeypatch):
    processes = state / "processes"
    monkeypatch.setattr(care, "_PROC", processes)
    killed = []
    monkeypatch.setattr(care.os, "kill", lambda pid, sig: killed.append(pid))
    _fake_proc(processes, 2, 1, [protocol.ENGINE_BIN], exe=protocol.ENGINE_BIN)
    _fake_proc(processes, 20, 2, _ENGINE_CHROME)
    assert care.end_browserless() is False
    shutil.rmtree(processes / "20")
    # A command that may be launching the daemon's Chrome; the caller's own does not count.
    _fake_proc(processes, 3, 1, [protocol.ENGINE_BIN, "open", "x"], exe=protocol.ENGINE_BIN)
    assert care.end_browserless() is False
    shutil.rmtree(processes / "3")
    runtime = str(Path(care.__file__).resolve().parent)
    me = os.getpid()
    _fake_proc(processes, me, 1, ["python3", "-I", runtime, "agent-browser", "open", "x"], exe="/usr/bin/python3")
    monkeypatch.setattr(daemon, "listening", _listening(False))
    assert care.end_browserless() is True
    assert killed == [2]


def test_commands_arriving_together_end_a_browserless_daemon_one_at_a_time(state, monkeypatch):
    """Each would see the other as a command that may be launching Chrome and
    leave the dead browser's stream up; the one admitted ends it, and the next
    waits for the gate."""
    processes = state / "processes"
    monkeypatch.setattr(care, "_PROC", processes)
    killed = []
    monkeypatch.setattr(care.os, "kill", lambda pid, sig: killed.append(pid))
    _fake_proc(processes, 2, 1, [protocol.ENGINE_BIN], exe=protocol.ENGINE_BIN)
    runtime = str(Path(care.__file__).resolve().parent)
    _fake_proc(processes, 7, 1, ["python3", "-I", runtime, "agent-browser", "open", "x"], exe="/usr/bin/python3")
    assert care.end_browserless() is False
    assert care.end_browserless(admitting=True) is True
    assert killed == [2]
    gate.check("act")
    with open(state / "admitting.lock") as waiting, pytest.raises(BlockingIOError):
        fcntl.flock(waiting, fcntl.LOCK_EX | fcntl.LOCK_NB)


def test_the_whole_browser_goes_first_and_nothing_beside_it(tmp_path, monkeypatch):
    monkeypatch.setattr(care, "_PROC", tmp_path)
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
    monkeypatch.setattr(care, "browser_up", lambda: True)
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
    monkeypatch.setattr(care, "browser_up", lambda: True)
    # A choice made while the browser is open reaches the next one, not this one.
    assert quality.choose("light") is True
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
    monkeypatch.setattr(care, "browser_up", lambda: False)
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
    monkeypatch.setattr(care, "browser_up", lambda: up[0])

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
    env = wrapper.engine_env(
        {"AGENT_BROWSER_SOCKET_DIR": "/tmp/other", "HOME": "/tmp/elsewhere"}, _STANDARD
    )
    assert env["AGENT_BROWSER_SOCKET_DIR"] == os.path.join(
        wrapper.pwd.getpwuid(os.getuid()).pw_dir, ".agent-browser"
    )
    monkeypatch.setattr(wrapper.os, "getuid", lambda: 2**31 - 7)
    assert wrapper.engine_env({}, _STANDARD)["AGENT_BROWSER_SOCKET_DIR"].endswith("/agent-browser")


def test_a_page_the_engine_could_not_reach_is_a_failure_and_a_slow_one_is_not(engine_runs, monkeypatch):
    hold.control("a", "take")
    monkeypatch.setattr(machine.subprocess, "run", lambda argv, **_: subprocess.CompletedProcess(argv, 1))
    assert machine.navigate("a", "open", "example.com") == {"done": False, "reason": "failed"}

    def slow(argv, **_):
        raise subprocess.TimeoutExpired(argv, 30)

    monkeypatch.setattr(machine.subprocess, "run", slow)
    assert machine.navigate("a", "open", "example.com") == {"done": True}


def test_starting_leaves_a_browser_already_up_on_its_page(state, monkeypatch):
    monkeypatch.setattr(machine.os, "access", lambda *_: True)
    monkeypatch.setattr(daemon, "listening", _listening(True))
    monkeypatch.setattr(care, "browser_up", lambda: True)
    (state / protocol.TOKEN_FILE).write_text("tok")
    spawned = []
    monkeypatch.setattr(daemon.subprocess, "Popen", lambda *a, **k: spawned.append(a))
    assert machine.start(0) == protocol.Status(installed=True, running=True, token="tok")
    assert spawned == []


def test_a_stream_whose_chrome_is_gone_is_not_up_and_a_start_brings_one_back(state, monkeypatch):
    """The engine's daemon keeps its stream listening after the OOM killer takes
    Chrome, and the next command it runs launches another."""
    monkeypatch.setattr(machine.os, "access", lambda *_: True)
    monkeypatch.setattr(daemon, "listening", _listening(True))
    monkeypatch.setattr(care, "browser_up", lambda: False)
    monkeypatch.setattr(care, "engine_busy", lambda: False)
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
    monkeypatch.setattr(machine.os, "access", lambda *_: True)
    monkeypatch.setattr(care, "engine_busy", lambda: False)
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
    monkeypatch.setattr(care, "browser_up", lambda: True)
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


def test_only_a_machine_of_our_own_is_sent_the_browser():
    names = {str(rel) for _, rel in _internal_package_files(_SANDBOX_SRC, provider_class("docker"))}
    without = {str(rel) for _, rel in _internal_package_files(_SANDBOX_SRC, provider_class("ssh"))}
    added = names - without
    assert any(name.endswith("agent-browser") for name in added)
    assert f"{protocol.PACKAGE_NAME}/protocol.py" in added
    # The engine fails every command when the config it is pointed at is missing.
    config = Path(wrapper._ENGINE_ENV["AGENT_BROWSER_CONFIG"]).name
    assert f"{protocol.PACKAGE_NAME}/{config}" in added


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
        gate.check("act")
    assert hold.control("b", "renew") is True
    assert hold.control("b", "release") is False
    with pytest.raises(gate.Refused, match=re.escape(protocol.CHANGED_MESSAGE)):
        gate.check("act")
    assert gate.check("act") == _STANDARD


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
        monkeypatch.setattr(care, "browser_up", lambda: False)
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
    monkeypatch.setattr(machine.os, "access", lambda *_: True)
    monkeypatch.setattr(care, "browser_up", lambda: False)
    monkeypatch.setattr(care, "engine_busy", lambda: False)
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
    monkeypatch.setattr(machine.os, "access", lambda *_: True)
    spawned = []
    monkeypatch.setattr(daemon.subprocess, "Popen", lambda argv, **k: spawned.append(argv[3:]))
    monkeypatch.setattr(care, "engine_busy", lambda: True)
    machine.start(0)
    assert spawned == []
    monkeypatch.setattr(care, "engine_busy", lambda: False)
    machine.start(0)
    assert spawned == [["agent-browser", *machine._ENGINE_START_ARGS]]


def test_a_command_of_ours_or_the_engine_is_seen_starting(tmp_path, monkeypatch):
    runtime = str(Path(care.__file__).resolve().parent)
    monkeypatch.setattr(care, "_PROC", tmp_path)
    _fake_proc(tmp_path, 1, 0, ["/bin/sh", "-c", "agent-browser open x"])
    assert care.engine_busy() is False
    wrapper_entry = _fake_proc(
        tmp_path,
        2,
        1,
        ["python3", "-I", f"{runtime}/../{Path(runtime).name}", "agent-browser", "open", "x"],
        exe="/usr/bin/python3",
    )
    assert care.engine_busy() is True
    shutil.rmtree(wrapper_entry)
    # The engine's daemon alone starts nothing: it can outlive a Chrome the OOM killer took.
    daemon_entry = _fake_proc(tmp_path, 3, 1, [protocol.ENGINE_BIN], exe=protocol.ENGINE_BIN)
    assert care.engine_busy() is False
    shutil.rmtree(daemon_entry)
    _fake_proc(tmp_path, 4, 1, [protocol.ENGINE_BIN, "open", "x"], exe=protocol.ENGINE_BIN)
    assert care.engine_busy() is True


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
    monkeypatch.setattr(gate, "check", lambda use, user_agent: uses.append(use) or _STANDARD)
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
    ]
    assert (env["AGENT_BROWSER_STREAM_MAX_WIDTH"], env["AGENT_BROWSER_STREAM_MAX_HEIGHT"]) == ("1280", "800")
    assert env["AGENT_BROWSER_STREAM_QUALITY"] == "70"


def test_retina_renders_at_twice_the_page_and_streams_it_whole(engine, monkeypatch):
    monkeypatch.setattr(gate, "check", lambda use, user_agent: Launch("", "retina"))
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
    (quality._CGROUP / "cpu.max").write_text(cpu_max)
    if chosen:
        assert quality.choose(chosen) is True
    assert gate.check("act") == Launch("", launched)
    assert launch.current() == Launch("", launched)


def test_a_choice_that_is_not_a_quality_is_refused_and_one_found_garbled_is_ignored(state):
    (quality._CGROUP / "cpu.max").write_text("400000 100000")
    assert quality.choose("garbage") is False
    assert quality.chosen() == "retina"
    protocol.write_state(quality._FILE, "garbage")
    assert quality.chosen() == "retina"


def test_an_open_browser_keeps_its_quality_until_it_closes(state, monkeypatch):
    """A launch option that changes between commands restarts the browser and
    loses its pages."""
    quality.choose("light")
    assert gate.check("act").quality == "light"
    monkeypatch.setattr(daemon, "listening", _listening(True))
    quality.choose("sharp")
    assert gate.check("look").quality == "light"
    assert gate.check("close").quality == "light"
    monkeypatch.setattr(daemon, "listening", _listening(False))
    assert gate.check("act").quality == "sharp"


def test_a_machine_with_no_cpu_limit_counts_its_cpus(state, monkeypatch):
    monkeypatch.setattr(quality.os, "cpu_count", lambda: 8)
    assert quality.cpus() == 8
    (quality._CGROUP / "cpu.max").write_text("150000 100000")
    assert quality.cpus() == 1.5


@pytest.mark.parametrize(("quota", "cpus"), [("100000", 1.0), ("-1", 8.0)])
def test_a_cgroup_v1_cpu_quota_is_the_computer_s_limit(state, monkeypatch, quota, cpus):
    monkeypatch.setattr(quality.os, "cpu_count", lambda: 8)
    (quality._CGROUP / "cpu").mkdir()
    (quality._CGROUP / "cpu" / "cpu.cfs_quota_us").write_text(f"{quota}\n")
    (quality._CGROUP / "cpu" / "cpu.cfs_period_us").write_text("100000\n")
    assert quality.cpus() == cpus


@pytest.mark.parametrize(
    ("argv", "answer", "kept"),
    [
        (["quality", "sharp"], {"done": True}, "sharp"),
        (["quality", "default"], {"done": True}, None),
        (["quality", "ultra"], {"done": False}, "light"),
    ],
)
def test_the_server_hands_the_machine_its_choice_of_quality(state, capsys, argv, answer, kept):
    quality.choose("light")
    assert machine.main(argv) == 0
    assert json.loads(capsys.readouterr().out) == answer
    assert quality._choice() == kept


def test_no_hold_is_taken_or_kept_on_a_browser_that_has_closed(state, capsys, monkeypatch):
    """It would only keep the agent from opening the next browser until it lapsed."""
    holder = "11111111-1111-4111-8111-111111111111"
    up = [False]
    monkeypatch.setattr(care, "browser_up", lambda: up[0])
    assert machine.main(["control", holder, "take"]) == 0
    assert json.loads(capsys.readouterr().out) == {"held": False}
    assert gate.check("act") == _STANDARD
    up[0] = True
    assert machine.main(["control", holder, "take"]) == 0
    assert json.loads(capsys.readouterr().out) == {"held": True}
    up[0] = False
    assert machine.main(["control", holder, "renew"]) == 0
    assert json.loads(capsys.readouterr().out) == {"held": False}
    assert hold.held() is False


def test_the_usage_names_every_command(state, capsys):
    assert machine.main(["nothing"]) == 2
    usage = capsys.readouterr().err
    for word in ("status", "start", "control", "navigate", "reload", "quality", "default", "serve", "proxy"):
        assert word in usage


def test_where_the_browser_must_not_go_is_one_list(state):
    """Chrome's policy names each place, and the proxy refuses each address."""
    blocked = SANDBOX_CHROME_POLICY["URLBlocklist"]
    for host in (*protocol.METADATA_HOSTS, *protocol.DOCKER_HOSTS):
        assert (f"[{host}]" if ":" in host else host) in blocked
        try:
            address = ipaddress.ip_address(host)
        except ValueError:
            continue
        assert proxy.refused(address, 80, proxy.Places())
