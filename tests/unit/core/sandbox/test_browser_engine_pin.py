"""A tool update swaps the engine under an open browser, which keeps the version
it started on until it closes, since a command on another would restart it."""

from __future__ import annotations

import json
import os
import re
import subprocess
import threading
from pathlib import Path

import pytest

from ptc_agent.core.sandbox.browser_runtime import (
    care,
    daemon,
    engine,
    gate,
    hold,
    host,
    launch,
    machine,
    protocol,
    proxy,
    quality,
    relay,
    system,
    wrapper,
)
from ptc_agent.core.sandbox.browser_runtime.host import Host
from ptc_agent.core.sandbox.browser_runtime.launch import Launch

_OUR_PROXY = f"--proxy-server=http://127.0.0.1:{protocol.PROXY_PORT}"


class _Toolset:
    """The engine as a tool update lays it out: each version in its own folder,
    ``current`` pointing at one, and the host's engine a link through it."""

    def __init__(self, root: Path) -> None:
        self.root = root / "toolset"
        self.unit = self.root / "agent-browser"
        self.unit.mkdir(parents=True)
        self.link = root / "lib" / "agent-browser"
        self.link.parent.mkdir()
        self.link.symlink_to(self.unit / "current" / "agent-browser")

    def install(self, version: str) -> str:
        """Put *version* beside the others and point ``current`` at it, as a
        converge does; the version's real path."""
        folder = self.unit / version
        folder.mkdir(exist_ok=True)
        engine = folder / "agent-browser"
        engine.write_text("#!/bin/sh\n")
        engine.chmod(0o755)
        swap = self.unit / ".current"
        swap.symlink_to(version)
        os.replace(swap, self.unit / "current")
        return os.path.realpath(engine)

    def drop(self, version: str) -> None:
        engine = self.unit / version / "agent-browser"
        engine.unlink()
        engine.parent.rmdir()


@pytest.fixture(autouse=True)
def _managed_policy(tmp_path_factory, monkeypatch):
    """The managed policy a cloud computer's image carries, without which its
    host is not ready; beside the test's own folder, which some read whole."""
    policy = tmp_path_factory.mktemp("policy") / "langalpha.json"
    policy.write_text("{}")
    monkeypatch.setattr(host, "CHROME_POLICY_FILE", str(policy))


@pytest.fixture
def toolset(tmp_path, monkeypatch) -> _Toolset:
    """A cloud computer whose engine is a link a converge moves, with the
    runtime's state in *tmp_path*, no browser up and no processes."""
    laid = _Toolset(tmp_path)
    monkeypatch.setattr(protocol, "_STATE_ROOT", tmp_path)
    (tmp_path / protocol.STATE_DIR).mkdir()
    monkeypatch.setattr(system, "_CGROUP", tmp_path)
    monkeypatch.setattr(system, "_MEMINFO", tmp_path / "no-meminfo")
    processes = tmp_path / "proc"
    processes.mkdir()
    monkeypatch.setattr(system, "_PROC", processes)
    monkeypatch.setattr(system, "processes", lambda wanted=None: system._linux_processes())
    monkeypatch.setattr(quality.os, "cpu_count", lambda: 1)
    monkeypatch.setattr(relay, "ensure", lambda wait_s=0.0: True)
    monkeypatch.setattr(proxy, "ensure", lambda: True)
    monkeypatch.setattr(care, "_GONE_S", 0)
    chosen = Host(engine=str(laid.link))
    # The image's Chrome, which a host is ready only with, is not on this machine.
    monkeypatch.setattr(Host, "ready", lambda self: os.access(self.engine, os.X_OK))
    monkeypatch.setattr(host, "current", lambda: chosen)
    return laid


@pytest.fixture
def browser(monkeypatch):
    """Whether the browser is up: its stream listening and its engine running."""
    up = {"up": False}
    monkeypatch.setattr(daemon, "listening", lambda port, wait_s=0.0: up["up"])
    monkeypatch.setattr(care.Look, "engine_gone", lambda self: not up["up"])
    return up


def _tabs(shown: str, *others: str) -> str:
    """What ``--json tab list`` prints for a browser showing *shown*, with
    *others* open beside it."""
    tabs = [{"tabId": "t1", "url": shown, "active": True, "type": "page"}]
    tabs += [{"tabId": f"t{i + 2}", "url": url, "active": False, "type": "page"} for i, url in enumerate(others)]
    return json.dumps({"success": True, "data": {"tabs": tabs}})


def _check(use: str, user_agent: str = "") -> Launch:
    launched, release = gate.check(use, user_agent)
    release()
    return launched


def test_an_open_browser_keeps_its_engine_across_an_update_and_takes_the_new_one_when_it_next_starts(
    toolset, browser
):
    v1 = toolset.install("0.41.1")
    started = _check("act")
    assert started.engine == v1
    browser["up"] = True
    v2 = toolset.install("0.42.0")
    on_open = _check("look")
    assert (on_open.engine, engine.for_launch(on_open, host.current())) == (v1, v1)
    assert _check("close").engine == v1
    # Closed, on its idle timeout or the close: the next start is on the new one.
    browser["up"] = False
    reopened = _check("act")
    assert (reopened.engine, engine.for_launch(reopened, host.current())) == (v2, v2)


def test_a_command_that_reopens_the_browser_starts_it_on_the_new_engine(toolset, browser):
    toolset.install("0.41.1")
    _check("act")
    browser["up"] = True
    v2 = toolset.install("0.42.0")
    assert _check("reopen").engine == v2


def test_a_command_joining_a_browser_another_is_starting_runs_its_engine(toolset, browser, monkeypatch):
    v1 = toolset.install("0.41.1")
    _check("act")
    # The first command's engine runs, its browser not yet listening.
    monkeypatch.setattr(care.Look, "engine_gone", lambda self: False)
    toolset.install("0.42.0")
    assert _check("act").engine == v1



@pytest.fixture
def engine_steps(browser, monkeypatch):
    """What the gate asks the engine itself, by the engine it ran: the open
    browser shows a page, and a close takes it down."""
    asked: list[tuple[str, tuple[str, ...]]] = []

    def run(self, h, *args, timeout):
        asked.append((engine.for_launch(self, h), args))
        if args == ("close",):
            browser["up"] = False
        return _tabs("https://example.com/report") if args == gate._TABS else ""

    monkeypatch.setattr(Launch, "run", run)
    monkeypatch.setattr(care.Look, "browser_up", lambda self: browser["up"])
    return asked


def _quiet_for(seconds: float) -> None:
    noted = protocol.state_file(care._LAST_USE)
    then = noted.stat().st_mtime - seconds
    os.utime(noted, (then, then))


def test_a_browser_quiet_past_the_pause_moves_to_the_new_engine_with_its_user_agent_and_page(
    toolset, browser, engine_steps
):
    v1 = toolset.install("0.41.1")
    _check("act", "Agent/1")
    browser["up"] = True
    v2 = toolset.install("0.42.0")
    _quiet_for(gate.ENGINE_MOVE_AFTER_S + 1)
    moved = _check("look")
    assert (moved.engine, moved.user_agent) == (v2, "Agent/1")
    assert engine_steps == [
        (v1, gate._TABS),
        (v1, ("close",)),
        (v2, ("open", "https://example.com/report")),
    ]


def test_a_browser_on_a_release_pinned_anew_under_its_version_moves_to_the_new_one(toolset, browser, engine_steps):
    """A converge names each folder by the unit's pin, so the same version
    pinned anew lands beside the one the browser started on."""
    old = toolset.install("0.41.1@aaaaaaaaaaaa")
    _check("act")
    browser["up"] = True
    new = toolset.install("0.41.1@bbbbbbbbbbbb")
    _quiet_for(gate.ENGINE_MOVE_AFTER_S + 1)

    assert _check("look").engine == new
    assert engine_steps[:2] == [(old, gate._TABS), (old, ("close",))]


def test_a_browser_in_use_held_or_being_closed_keeps_its_engine(toolset, browser, engine_steps, monkeypatch):
    v1 = toolset.install("0.41.1")
    _check("act")
    browser["up"] = True
    toolset.install("0.42.0")
    # Commands in the last few minutes: the agent may be mid-task on the page.
    _quiet_for(gate.ENGINE_MOVE_AFTER_S - 30)
    assert _check("look").engine == v1
    # The person holds the page.
    monkeypatch.setattr(hold, "held", lambda: True)
    _quiet_for(gate.ENGINE_MOVE_AFTER_S + 1)
    assert gate._move_engine(care.look(), launch.current(), "look", gate.ENGINE_MOVE_AFTER_S + 1)[1] == gate._Moved()
    monkeypatch.setattr(hold, "held", lambda: False)
    # A close ends it anyway, on the engine it runs.
    assert _check("close").engine == v1
    assert engine_steps == []


def test_a_browser_keeps_its_engine_and_page_while_memory_would_refuse_the_one_opened_for_it(
    toolset, browser, engine_steps, monkeypatch
):
    v1 = toolset.install("0.41.1")
    _check("act")
    browser["up"] = True
    toolset.install("0.42.0")
    _quiet_for(gate.ENGINE_MOVE_AFTER_S + 1)
    monkeypatch.setattr(care, "memory_full", lambda: True)
    # A read on the open browser is admitted however full memory is.
    assert _check("look").engine == v1
    assert engine_steps == []
    assert browser["up"]


def test_a_browser_whose_page_cannot_be_read_keeps_its_engine(toolset, browser, engine_steps, monkeypatch):
    """Closed, it would come back blank, and the command would read that."""
    v1 = toolset.install("0.41.1")
    _check("act")
    browser["up"] = True
    toolset.install("0.42.0")
    _quiet_for(gate.ENGINE_MOVE_AFTER_S + 1)
    monkeypatch.setattr(Launch, "run", lambda self, h, *args, timeout: engine_steps.append(args))

    assert _check("look").engine == v1
    assert engine_steps == [gate._TABS]
    assert browser["up"]


@pytest.mark.parametrize(
    "shown",
    ["blob:https://example.com/0f3c", "file:///home/workspace/report.html", "data:text/html,Report", "chrome://version"],
)
def test_a_browser_on_a_page_it_could_not_open_again_keeps_its_engine(toolset, browser, engine_steps, monkeypatch, shown):
    v1 = toolset.install("0.41.1")
    _check("act")
    browser["up"] = True
    toolset.install("0.42.0")
    _quiet_for(gate.ENGINE_MOVE_AFTER_S + 1)
    monkeypatch.setattr(Launch, "run", lambda self, h, *args, timeout: engine_steps.append(args) or _tabs(shown))

    assert _check("look").engine == v1
    assert engine_steps == [gate._TABS]


def test_a_person_who_takes_the_page_while_its_address_is_read_keeps_it_open(
    toolset, browser, engine_steps, monkeypatch
):
    v1 = toolset.install("0.41.1")
    _check("act")
    browser["up"] = True
    toolset.install("0.42.0")
    _quiet_for(gate.ENGINE_MOVE_AFTER_S + 1)
    taken = {"now": False}
    monkeypatch.setattr(hold, "held", lambda: taken["now"])

    def taken_meanwhile(self, h, *args, timeout):
        engine_steps.append(args)
        taken["now"] = True
        return _tabs("https://example.com/report")

    monkeypatch.setattr(Launch, "run", taken_meanwhile)

    assert gate._move_engine(care.look(), launch.current(), "look", gate.ENGINE_MOVE_AFTER_S + 1)[1] == gate._Moved()
    assert engine_steps == [gate._TABS]
    assert browser["up"] and launch.current().engine == v1


def test_a_take_while_the_browser_is_closed_to_move_waits_and_finds_it_closed(
    toolset, browser, engine_steps, monkeypatch
):
    toolset.install("0.41.1")
    _check("act")
    browser["up"] = True
    toolset.install("0.42.0")
    _quiet_for(gate.ENGINE_MOVE_AFTER_S + 1)
    took: dict = {}

    def take() -> None:
        took["holds"] = machine.control("a", "take")

    def closing(self, h, *args, timeout):
        engine_steps.append(args)
        if args == ("close",):
            person = threading.Thread(target=take)
            person.start()
            person.join(0.3)
            took["waited"] = person.is_alive()
            took["thread"] = person
            browser["up"] = False
        return _tabs("https://example.com/report") if args == gate._TABS else ""

    monkeypatch.setattr(Launch, "run", closing)

    moved = gate._move_engine(care.look(), launch.current(), "look", gate.ENGINE_MOVE_AFTER_S + 1)[1]
    took["thread"].join(5)

    assert moved.address == "https://example.com/report"
    assert took["waited"]
    # Told the truth: the page it would have held is gone, and no lease is left
    # to keep the agent from opening the next one.
    assert took["holds"] is False
    assert not hold.held()


def test_a_blank_page_moves_and_opens_nothing_again(toolset, browser, engine_steps, monkeypatch):
    v1 = toolset.install("0.41.1")
    _check("act")
    browser["up"] = True
    v2 = toolset.install("0.42.0")
    _quiet_for(gate.ENGINE_MOVE_AFTER_S + 1)
    asked = engine_steps

    def blank(self, h, *args, timeout):
        asked.append((engine.for_launch(self, h), args))
        if args == ("close",):
            browser["up"] = False
        return _tabs("about:blank") if args == gate._TABS else ""

    monkeypatch.setattr(Launch, "run", blank)

    assert _check("look").engine == v2
    assert asked == [(v1, gate._TABS), (v1, ("close",))]


@pytest.mark.parametrize("meanwhile", ["used", "memory"])
def test_a_browser_used_or_short_of_memory_while_its_address_is_read_keeps_it_open(
    toolset, browser, engine_steps, monkeypatch, meanwhile
):
    """A person may take and hand back the page while it is read, which notes
    a use; memory that fills would refuse the browser opened for it."""
    v1 = toolset.install("0.41.1")
    _check("act")
    browser["up"] = True
    toolset.install("0.42.0")
    _quiet_for(gate.ENGINE_MOVE_AFTER_S + 1)
    full = {"now": False}
    monkeypatch.setattr(care, "memory_full", lambda: full["now"])

    def changed_meanwhile(self, h, *args, timeout):
        engine_steps.append(args)
        if meanwhile == "used":
            noted = protocol.state_file(care._LAST_USE)
            later = noted.stat().st_mtime + 1
            os.utime(noted, (later, later))
        else:
            full["now"] = True
        return _tabs("https://example.com/report")

    monkeypatch.setattr(Launch, "run", changed_meanwhile)

    assert _check("look").engine == v1
    assert engine_steps == [gate._TABS]
    assert browser["up"]


def test_a_browser_whose_close_fails_still_opens_its_page_on_the_new_engine(toolset, browser, engine_steps, monkeypatch):
    v1 = toolset.install("0.41.1")
    _check("act")
    browser["up"] = True
    v2 = toolset.install("0.42.0")
    _quiet_for(gate.ENGINE_MOVE_AFTER_S + 1)
    shown = Launch.run

    def close_times_out(self, h, *args, timeout):
        if args == ("close",):
            # What is left is ended after, as a close that never answers leaves it.
            browser["up"] = False
            raise subprocess.TimeoutExpired("agent-browser", timeout)
        return shown(self, h, *args, timeout=timeout)

    monkeypatch.setattr(Launch, "run", close_times_out)

    assert _check("look").engine == v2
    assert engine_steps == [(v1, gate._TABS), (v2, ("open", "https://example.com/report"))]


@pytest.mark.parametrize("fails", ["answers", "raises"])
def test_a_moved_browser_that_cannot_open_its_page_again_refuses_the_command(
    toolset, browser, engine_steps, monkeypatch, fails
):
    toolset.install("0.41.1")
    _check("act")
    browser["up"] = True
    v2 = toolset.install("0.42.0")
    _quiet_for(gate.ENGINE_MOVE_AFTER_S + 1)
    shown = Launch.run

    def open_fails(self, h, *args, timeout):
        if args[:1] == ("open",):
            engine_steps.append((engine.for_launch(self, h), args))
            if fails == "raises":
                raise OSError("cannot spawn")
            return None
        return shown(self, h, *args, timeout=timeout)

    monkeypatch.setattr(Launch, "run", open_fails)

    with pytest.raises(gate.Refused, match=re.escape(protocol.REOPEN_MESSAGE.format("https://example.com/report"))):
        _check("look")
    assert engine_steps[-1] == (v2, ("open", "https://example.com/report"))
    # The browser is on the new engine now, so the command repeated runs.
    browser["up"] = True
    assert _check("look").engine == v2


def test_a_moved_browser_tells_the_agent_which_other_tabs_it_closed(toolset, browser, engine_steps, monkeypatch, capsys):
    toolset.install("0.41.1")
    _check("act")
    browser["up"] = True
    v2 = toolset.install("0.42.0")
    _quiet_for(gate.ENGINE_MOVE_AFTER_S + 1)
    shown = Launch.run

    def three_tabs(self, h, *args, timeout):
        if args == gate._TABS:
            engine_steps.append((engine.for_launch(self, h), args))
            return _tabs("https://example.com/report", "https://example.org/data", "about:blank")
        return shown(self, h, *args, timeout=timeout)

    monkeypatch.setattr(Launch, "run", three_tabs)

    assert _check("look").engine == v2
    assert engine_steps[-1] == (v2, ("open", "https://example.com/report"))
    # A blank tab held nothing, so only the page with an address is named.
    assert protocol.TABS_CLOSED_MESSAGE.format("https://example.org/data") in capsys.readouterr().err


def test_a_moved_browser_with_one_tab_says_nothing(toolset, browser, engine_steps, capsys):
    toolset.install("0.41.1")
    _check("act")
    browser["up"] = True
    toolset.install("0.42.0")
    _quiet_for(gate.ENGINE_MOVE_AFTER_S + 1)

    _check("look")

    assert capsys.readouterr().err == ""


def test_a_moved_browser_goes_straight_to_the_address_a_command_opens(toolset, browser, engine_steps):
    v1 = toolset.install("0.41.1")
    _check("act")
    browser["up"] = True
    v2 = toolset.install("0.42.0")
    _quiet_for(gate.ENGINE_MOVE_AFTER_S + 1)
    launched, release = gate.check("act", opens_page=True)
    release()
    assert launched.engine == v2
    assert engine_steps == [(v1, gate._TABS), (v1, ("close",))]


def test_a_browser_two_updates_behind_runs_the_host_s_engine(toolset):
    v1 = toolset.install("0.41.1")
    launched = Launch("", "standard", engine=v1)
    toolset.install("0.42.0")
    toolset.install("0.43.0")
    toolset.drop("0.41.1")
    assert engine.for_launch(launched, host.current()) == str(toolset.link)


def test_a_browser_keeps_an_engine_a_later_version_renamed(toolset):
    """A version keeps its own file's name, which the newest may not share."""
    v1 = toolset.install("0.41.1")
    launched = Launch("", "standard", engine=v1)
    renamed = toolset.unit / "0.42.0" / "agent-browser-2"
    renamed.parent.mkdir()
    renamed.write_text("#!/bin/sh\n")
    renamed.chmod(0o755)
    swap = toolset.unit / ".current"
    swap.symlink_to("0.42.0")
    os.replace(swap, toolset.unit / "current")
    toolset.link.unlink()
    toolset.link.symlink_to(toolset.unit / "current" / "agent-browser-2")

    assert engine.for_launch(launched, host.current()) == v1


def test_a_page_the_person_left_unread_keeps_the_browser_on_its_engine(toolset, browser, engine_steps):
    """Closing it would forget that the agent has not read the page the person
    handed back, and its next action would run on a page it has not seen."""
    v1 = toolset.install("0.41.1")
    _check("act")
    browser["up"] = True
    toolset.install("0.42.0")
    hold.moved()
    _quiet_for(gate.ENGINE_MOVE_AFTER_S + 1)

    with pytest.raises(gate.Refused, match=protocol.CHANGED_MESSAGE[:40]):
        _check("act")
    assert engine_steps == []
    assert launch.current().engine == v1


def test_an_engine_that_is_no_link_is_not_pinned(tmp_path):
    binary = tmp_path / "agent-browser"
    binary.write_text("#!/bin/sh\n")
    binary.chmod(0o755)
    real = os.path.realpath(binary)
    assert engine.pinned(Host(engine=real)) == ""


def test_a_launch_recorded_before_engines_were_pinned_runs_the_host_s_engine(toolset, browser):
    """Its browser started on whatever the host's engine was, and keeps it as before."""
    toolset.install("0.41.1")
    protocol.state_file("launch").write_text(json.dumps({"user_agent": "Old", "quality": "light"}))
    recorded = launch.current()
    assert recorded == Launch("Old", "light")
    assert engine.for_launch(recorded, host.current()) == str(toolset.link)
    browser["up"] = True
    toolset.install("0.42.0")
    assert _check("look") == Launch("Old", "light")


@pytest.fixture
def exec_engine(monkeypatch):
    """What ``wrapper.main`` would exec."""
    ran: dict = {}

    class _Ran(Exception):
        pass

    def execute(path, argv, env):
        ran.update(path=path, argv=argv)
        raise _Ran

    monkeypatch.setattr(wrapper.os, "getuid", lambda: 1000)
    monkeypatch.setattr(wrapper.os, "execvpe", execute)

    def run(*args: str) -> dict:
        with pytest.raises(_Ran):
            wrapper.main(list(args))
        return ran

    return run


def test_the_agent_s_command_on_an_open_browser_runs_the_engine_it_started_on(
    toolset, browser, exec_engine, tmp_path, monkeypatch
):
    monkeypatch.setattr(wrapper, "_OOM_SCORE", tmp_path / "oom_score_adj")
    v1 = toolset.install("0.41.1")
    assert exec_engine("snapshot")["argv"][0] == v1
    browser["up"] = True
    v2 = toolset.install("0.42.0")
    assert exec_engine("snapshot")["argv"][0] == v1
    browser["up"] = False
    assert exec_engine("snapshot")["argv"][0] == v2


def test_a_person_s_navigation_runs_the_open_browser_s_engine(toolset, monkeypatch):
    v1 = toolset.install("0.41.1")
    launch.write(Launch("", "standard", engine=v1))
    toolset.install("0.42.0")
    monkeypatch.setattr(care.Look, "live", lambda self: True)
    runs: list[list[str]] = []

    def run(argv, **kwargs):
        runs.append(argv)
        return subprocess.CompletedProcess(argv, 0)

    monkeypatch.setattr(machine.subprocess, "run", run)
    hold.control("a", "take")
    assert machine.navigate("a", "reload") == {"done": True}
    assert [argv[0] for argv in runs] == [v1, v1]


def _proc(root: Path, pid: int, ppid: int, argv: list[str], exe: str) -> None:
    """A process as Linux shows it, its file by the kernel's real path."""
    entry = root / str(pid)
    entry.mkdir()
    (entry / "stat").write_text(f"{pid} (name) S {ppid} 1 1 0")
    (entry / "cmdline").write_bytes("\0".join(argv).encode() + b"\0")
    (entry / "exe").symlink_to(exe)


def _chrome(profile: Path) -> list[str]:
    return [protocol.CHROME_BIN, _OUR_PROXY, f"--user-data-dir={profile}"]


@pytest.fixture
def killed(monkeypatch) -> list[int]:
    ended: list[int] = []
    monkeypatch.setattr(care.os, "kill", lambda pid, sig: ended.append(pid))
    return ended


def test_a_live_browser_on_a_linked_engine_is_its_daemon_s_and_one_left_behind_is_reaped(
    toolset, tmp_path, killed
):
    v1 = toolset.install("0.41.1")
    launch.write(Launch("", "standard", engine=v1))
    toolset.install("0.42.0")
    proc = tmp_path / "proc"
    _proc(proc, 1, 0, ["/sbin/init"], "/sbin/init")
    _proc(proc, 2, 1, [str(toolset.link)], v1)
    live = tmp_path / f"{protocol.ENGINE_PROFILE_PREFIX}live"
    left = tmp_path / f"{protocol.ENGINE_PROFILE_PREFIX}left"
    live.mkdir()
    left.mkdir()
    _proc(proc, 20, 2, _chrome(live), protocol.CHROME_BIN)
    _proc(proc, 30, 1, _chrome(left), protocol.CHROME_BIN)
    seen = care.look()
    assert [p.pid for p in seen.daemons()] == [2]
    assert seen.browser_up()
    care.tend(False)
    assert killed == [30]
    assert live.exists() and not left.exists()


def test_an_engine_command_on_any_installed_version_counts_as_busy(toolset, tmp_path):
    """A command the agent started before the update, whose launch the gate has
    not recorded yet, may be starting a browser."""
    v1 = toolset.install("0.41.1")
    toolset.install("0.42.0")
    _proc(tmp_path / "proc", 5, 1, [str(toolset.link), "open", "https://example.com"], v1)
    assert care.look().busy()


def test_a_daemon_whose_version_an_update_removed_is_still_the_engine_s(toolset, tmp_path, killed):
    v1 = toolset.install("0.41.1")
    launch.write(Launch("", "standard", engine=v1))
    toolset.install("0.42.0")
    toolset.install("0.43.0")
    toolset.drop("0.41.1")
    _proc(tmp_path / "proc", 2, 1, [str(toolset.link)], v1 + " (deleted)")
    seen = care.look()
    assert [p.pid for p in seen.daemons()] == [2]
    # It runs no Chrome, so it is ended rather than left to idle out.
    seen.end_browserless()
    assert killed == [2]


def test_another_engine_on_the_machine_is_not_this_runtime_s(toolset, tmp_path):
    toolset.install("0.41.1")
    _proc(tmp_path / "proc", 3, 1, ["/elsewhere/agent-browser"], "/elsewhere/agent-browser")
    assert care.look().daemons() == []


def test_a_person_s_own_machine_knows_its_engine_by_its_host_record(tmp_path, monkeypatch, killed):
    monkeypatch.setattr(protocol, "_STATE_ROOT", tmp_path)
    (tmp_path / protocol.STATE_DIR).mkdir()
    proc = tmp_path / "proc"
    proc.mkdir()
    monkeypatch.setattr(system, "_PROC", proc)
    monkeypatch.setattr(system, "processes", lambda wanted=None: system._linux_processes())
    binary = tmp_path / "engine" / "agent-browser-0.41.1"
    binary.parent.mkdir()
    binary.write_text("#!/bin/sh\n")
    real = os.path.realpath(binary)
    elsewhere = tmp_path / "elsewhere" / "agent-browser"
    elsewhere.parent.mkdir()
    elsewhere.write_text("#!/bin/sh\n")
    chosen = Host(own=True, engine=real)
    monkeypatch.setattr(host, "current", lambda: chosen)
    _proc(proc, 2, 1, [real], real)
    _proc(proc, 3, 1, [str(elsewhere)], os.path.realpath(elsewhere))
    assert [p.pid for p in care.look().daemons()] == [2]


class _PlainEngine:
    """A computer built before the toolset: the host's engine a plain file the
    toolset adopted, under a browser whose launch pinned nothing, its daemon
    and Chrome running."""

    def __init__(self, toolset: _Toolset, proc: Path, tmp_path: Path) -> None:
        self.toolset = toolset
        self.proc = proc
        toolset.link.unlink()
        toolset.link.write_text("#!/bin/sh\n# 0.40.0\n")
        toolset.link.chmod(0o755)
        self.gone = Path(f"{toolset.link} (deleted)")
        _proc(proc, 2, 1, [str(toolset.link)], str(toolset.link))
        profile = tmp_path / f"{protocol.ENGINE_PROFILE_PREFIX}open"
        profile.mkdir()
        _proc(proc, 20, 2, _chrome(profile), protocol.CHROME_BIN)

    @property
    def daemon_file(self) -> str:
        return system.process_file(2)

    def release(self, version: str) -> str:
        """The first release, which swaps the plain file for a link to its
        version. The daemon still runs the file it started from, which Linux
        names by its old path marked deleted."""
        os.rename(self.toolset.link, self.gone)
        self.toolset.link.symlink_to(self.toolset.unit / "current" / "agent-browser")
        exe = self.proc / "2" / "exe"
        exe.unlink()
        exe.symlink_to(self.gone)
        return self.toolset.install(version)


@pytest.fixture
def plain(toolset, browser, killed, tmp_path) -> _PlainEngine:
    laid = _PlainEngine(toolset, tmp_path / "proc", tmp_path)
    assert _check("act").engine == ""
    browser["up"] = True
    return laid


def test_a_browser_pinned_to_nothing_keeps_its_daemon_s_engine_once_a_release_replaces_the_plain_file(
    plain, engine_steps, killed, tmp_path, monkeypatch
):
    # Until a release, the daemon runs the host's own engine: nothing to pin.
    assert _check("look").engine == ""
    plain.release("0.41.1")
    kept = _check("look")
    assert kept.engine == plain.daemon_file
    assert engine.for_launch(kept, host.current()) == plain.daemon_file
    # The agent's command, as the wrapper would exec it; under this user, whose
    # processes the daemon and Chrome are.
    monkeypatch.setattr(wrapper, "_OOM_SCORE", tmp_path / "oom_score_adj")
    monkeypatch.setattr(wrapper, "_can_drop_nice", lambda: False)
    ran: list[str] = []

    def execute(path, argv, env):
        ran.extend(argv)
        raise SystemExit(0)

    monkeypatch.setattr(wrapper.os, "execvpe", execute)
    with pytest.raises(SystemExit):
        wrapper.main(["snapshot"])
    assert ran[:1] == [plain.daemon_file]
    # The browser stays open, and its daemon and commands are still the engine's.
    seen = care.look()
    assert [p.pid for p in seen.daemons()] == [2]
    assert seen.browser_up()
    assert (killed, engine_steps) == ([], [])


def test_a_browser_pinned_to_its_daemon_s_engine_moves_to_the_new_one_after_the_pause(plain, engine_steps):
    v2 = plain.release("0.41.1")
    _check("look")
    _quiet_for(gate.ENGINE_MOVE_AFTER_S + 1)
    moved = _check("look")
    assert moved.engine == v2
    assert engine_steps == [
        (plain.daemon_file, gate._TABS),
        (plain.daemon_file, ("close",)),
        (v2, ("open", "https://example.com/report")),
    ]


def test_the_person_s_navigation_and_hand_back_run_the_daemon_s_engine_before_any_agent_command(
    plain, monkeypatch
):
    """A release replacing the plain file under a browser no agent command has
    reached since: the viewer's own engine commands still run the file its
    daemon runs, and leave the record for the gate to pin."""
    plain.release("0.41.1")
    monkeypatch.setattr(care.Look, "live", lambda self: True)
    runs: list[str] = []

    def run(argv, **kwargs):
        runs.append(argv[0])
        return subprocess.CompletedProcess(argv, 0)

    monkeypatch.setattr(machine.subprocess, "run", run)
    hold.control("a", "take")
    assert machine.navigate("a", "reload") == {"done": True}
    assert machine.control("a", "release", [["keyboard", "up", "Shift"]]) is False
    assert runs == [plain.daemon_file] * 3
    assert launch.current().engine == ""


def test_the_pin_to_a_daemon_s_engine_goes_with_the_daemon(plain, browser):
    v2 = plain.release("0.41.1")
    kept = _check("look")
    daemon = plain.proc / "2"
    for name in ("stat", "cmdline", "exe"):
        (daemon / name).unlink()
    daemon.rmdir()
    assert engine.for_launch(kept, host.current()) == str(plain.toolset.link)
    # Its number taken by another process: that is not the engine.
    _proc(plain.proc, 2, 1, ["/usr/bin/python3"], "/bin/sh")
    assert engine.for_launch(kept, host.current()) == str(plain.toolset.link)
    browser["up"] = False
    assert _check("act").engine == v2


def test_a_person_s_own_machine_pins_no_engine_for_its_browser(tmp_path, monkeypatch, browser, killed):
    monkeypatch.setattr(protocol, "_STATE_ROOT", tmp_path)
    (tmp_path / protocol.STATE_DIR).mkdir()
    proc = tmp_path / "proc"
    proc.mkdir()
    monkeypatch.setattr(system, "_PROC", proc)
    monkeypatch.setattr(system, "processes", lambda wanted=None: system._linux_processes())
    monkeypatch.setattr(relay, "ensure", lambda wait_s=0.0: True)
    monkeypatch.setattr(proxy, "ensure", lambda: True)
    binary = tmp_path / "engine" / "agent-browser-0.41.1"
    binary.parent.mkdir()
    binary.write_text("#!/bin/sh\n")
    binary.chmod(0o755)
    real = os.path.realpath(binary)
    chosen = Host(own=True, engine=real)
    monkeypatch.setattr(host, "current", lambda: chosen)
    monkeypatch.setattr(care, "memory_full", lambda: False)
    _proc(proc, 2, 1, [real], real)
    profile = tmp_path / f"{protocol.ENGINE_PROFILE_PREFIX}open"
    profile.mkdir()
    _proc(proc, 20, 2, _chrome(profile), protocol.CHROME_BIN)
    launch.write(Launch("", "standard"))
    browser["up"] = True
    assert _check("look") == Launch("", "standard")
    assert engine.for_launch(launch.current(), chosen) == real
    # Even a daemon on a file that is no longer the host's is left as it is.
    (proc / "2" / "exe").unlink()
    other = tmp_path / "engine" / "agent-browser-0.40.0"
    other.write_text("#!/bin/sh\n")
    other.chmod(0o755)
    (proc / "2" / "exe").symlink_to(other)
    gate._pin_running_engine(care.look(), launch.current())
    assert launch.current() == Launch("", "standard")
    assert killed == []


def test_the_person_s_use_of_the_page_starts_the_pause_again(toolset, browser, engine_steps):
    """A person who held the page past the pause and handed it back is not
    followed by a move: their hold and hand-back count as use, as a command does."""
    v1 = toolset.install("0.41.1")
    _check("act")
    browser["up"] = True
    toolset.install("0.42.0")
    _quiet_for(gate.ENGINE_MOVE_AFTER_S + 1)
    assert machine.control("a", "take") is True
    assert machine.control("a", "release") is False
    assert _check("look").engine == v1
    assert engine_steps == []
