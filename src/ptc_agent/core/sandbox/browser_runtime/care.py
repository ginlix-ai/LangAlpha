"""How the browser gives way to the computer's other work: it goes first when
memory runs out, opens no more of itself when memory is short, and leaves
nothing running once its daemon is gone."""

from __future__ import annotations

import dataclasses
import os
import shutil
import signal
import time
from collections.abc import Iterable, Iterator
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import IO

from . import daemon, hold, host, launch, signin, system
from .protocol import ENGINE_PROFILE_PREFIX, lock, locked
from .system import Process

#: Share of the computer's memory in use past which no more browser is opened: a tab
#: of a heavy page holds several hundred MB, and the rest is for the computer's work.
MEMORY_FULL_SHARE = 0.6

#: What a killed process takes to leave the process table, its sockets closed
#: with it, a daemon's stream included.
_GONE_S = 1.0
#: Held by the gate while it admits a command (``admit``), which may be the one
#: to start the next browser.
ADMISSION_LOCK = "admitting.lock"


def tend(up: bool) -> None:
    """The bridge's tick. It admits no command, so it tends only while none is
    being admitted, held to the end, so the hold and launch it forgets with a
    browser it ends are never those a command is recording meanwhile. A tick
    that finds a command being admitted leaves it to that command's gate, and
    one that cannot take the lock to tell leaves it to the next tick."""
    with ExitStack() as stack:
        try:
            clear = stack.enter_context(locked(ADMISSION_LOCK, wait=False))
        except OSError:
            return
        if clear:
            look().tend(up)


class Admission:
    """The gate's lock, held, so no other command is admitted until it is let
    go. A command that becomes the engine's lets it go as it does: the file is
    not inherited, so the exec closes it, and the engine's command then counts
    as busy (``Look.busy``)."""

    def __init__(self, held: IO[str]) -> None:
        self._held = held

    def look(self) -> Look:
        """The machine's processes as they are now, read by the lock's holder."""
        h = host.current()
        return Look(h, _read(h), admitting=True)

    def release(self) -> None:
        self._held.close()


def admit() -> Admission:
    """Wait for the gate's lock, and hold it."""
    held = lock(ADMISSION_LOCK)
    assert held is not None  # waited for, so taken
    return Admission(held)


@contextmanager
def admitted() -> Iterator[Look]:
    """Hold the gate's lock for the block, so no command is admitted until it
    ends, with the machine's processes as they are once it is held."""
    admission = admit()
    try:
        yield admission.look()
    finally:
        admission.release()


def look() -> Look:
    """The machine's processes as they are now, with the arguments of those
    that may be the engine's, its Chrome's or the wrapper's."""
    h = host.current()
    return Look(h, _read(h))


def forget_browser() -> None:
    """Drop what lasts as long as a browser, once it is gone: the person's hold
    on its page and what it was launched with. Kept, the next browser would
    start held, or on the closed one's user agent."""
    hold.forget()
    launch.forget()


def _read(h: host.Host) -> tuple[Process, ...]:
    mine = {h.engine, h.chrome}
    return tuple(
        system.processes(lambda exe: exe in mine or os.path.basename(exe).lower().startswith("python"))
    )


def memory_full() -> bool:
    """Whether what cannot be reclaimed fills the computer's share of memory.

    Measured against the computer's limit, so it holds on every resource tier. A
    machine with no limit of its own, such as a microVM, is measured against all its
    memory. One that says neither never counts as full. A person's own machine
    is full when it says it is short of memory: their own apps hold much of it.
    """
    if host.current().own:
        return system.memory_pressed()
    measured = system.memory_held()
    if measured is None:
        return False
    limit, used = measured
    return used >= limit * MEMORY_FULL_SHARE


@dataclass(frozen=True)
class Look:
    """One reading of the machine's processes, which a command's checks share:
    on a machine running a thousand, each reading costs it tens of milliseconds.

    Under the gate's lock (``Admission.look``), a browser starts meanwhile only
    through an engine command already running, which the reading shows as busy,
    so the checks that hold off for one stay right however long ago it was
    taken. What ends processes returns the reading without them, so the next
    check sees the machine as it left it.
    """

    host: host.Host
    processes: tuple[Process, ...]
    admitting: bool = False

    def tend(self, up: bool) -> Look:
        """While the browser is up it is first for the OOM killer, and a daemon
        whose Chrome died is ended, with its stream; once it is not up, a Chrome
        its dead daemon left behind is reaped. Reaping is only under the gate's
        lock, so what it forgets is never what a command is recording."""
        if not up:
            return self._reap_left_behind()
        seen = self.end_browserless()
        seen._browser_dies_first()
        return seen

    def close(self, ending: Iterable[Process]) -> Look:
        """End each of *ending* and every process under it, so a daemon takes
        the Chrome it runs, wait until they are gone, and return the reading
        without them.

        All of a browser's tree, not only its top: the network service, which
        keeps the profile's cookies, otherwise closes in its own time once the
        browser is gone and writes them back to a profile just erased. What
        this ends is a browser or the daemon behind one, so what lasts as long
        as the browser goes with it (``forget_browser``).
        """
        tree = self._tree(ending)
        if not tree:
            return self
        ended = False
        for p in tree:
            try:
                os.kill(p.pid, signal.SIGKILL)
                ended = True
            except OSError:
                continue
        deadline = time.monotonic() + _GONE_S
        while time.monotonic() < deadline and _any_left(tree):
            time.sleep(0.05)
        if ended:
            forget_browser()
        gone = {p.pid for p in tree}
        return dataclasses.replace(self, processes=tuple(p for p in self.processes if p.pid not in gone))

    def reread(self) -> Look:
        return dataclasses.replace(self, processes=_read(self.host))

    def end_browserless(self) -> Look:
        """End the engine's daemon whose Chrome is gone, as when the OOM killer
        took it: the daemon would keep its stream listening on the last frame,
        and the closed browser's hold and launch would carry over to the next
        one. Not while a browser command runs (``busy``), which may be
        launching its Chrome.
        """
        if self.busy():
            return self
        running = {p.ppid for p in self.browsers()}
        return self.close(p for p in self.daemons() if p.pid not in running)

    def end_on_profile(self) -> Look:
        """End every Chrome of the engine's on the kept profile, and the daemon
        that ran it. For a profile about to be erased, which a Chrome closing in
        its own time would write back.

        A command already admitted may be starting one there that no reading
        shows yet, so when it may (``_kept_may_start``) the engine goes first,
        its commands and its daemons with what they run, and the processes are
        read again for a Chrome it left. A command starting a browser on a
        profile of its own is left to it.
        """
        seen = self
        if seen.busy() and _kept_may_start():
            seen = seen.close(seen.commands() + seen.daemons()).reread()
        return seen.close(seen.browsers(kept_only=True)).end_browserless()

    def end_all(self) -> Look:
        """End the engine's daemons, its commands and every Chrome of this
        runtime's, before the engine, browser or ports they were started with
        change: nothing would know them by the new ones. A command is ended
        too, since one running may be starting a daemon on the old record, and
        the daemon it talks to is ended anyway."""
        return self.close(self.daemons() + self.commands() + self.browsers())

    def browsers(self, *, kept_only: bool = False) -> list[Process]:
        """Each Chrome this runtime's engine started: by the engine's name for
        it, on the kept profile (``signin``) or a throwaway one through this
        runtime's proxy. Both count, so a Chrome the agent's own code runs, even
        from the same path, is left alone, as is another engine's on the same
        machine, whose throwaway profiles are named the same. With *kept_only*,
        only those on the kept one."""
        uid = os.getuid()
        kept = signin.profile()
        ours = f"--proxy-server={self.host.proxy_url}"
        return [
            p
            for p in self.processes
            if p.argv[:1] == [self.host.chrome]
            and p.uid == uid
            and (on := _profile(p)) is not None
            and (on == kept or (_throwaway(on) and ours in p.argv and not kept_only))
        ]

    def daemons(self) -> list[Process]:
        """The engine's daemons: its binary run with no arguments."""
        uid = os.getuid()
        return [
            p for p in self.processes if p.uid == uid and not any(p.argv[1:]) and p.exe == self.host.engine
        ]

    def commands(self) -> list[Process]:
        """The engine's commands: its binary run with arguments."""
        uid = os.getuid()
        return [p for p in self.processes if p.uid == uid and any(p.argv[1:]) and p.exe == self.host.engine]

    def services(self) -> list[Process]:
        """This runtime's proxy and bridge, which outlive the stream by a grace."""
        package = str(Path(__file__).resolve().parent)
        uid = os.getuid()
        return [
            p
            for p in self.processes
            if p.uid == uid
            and p.argv[1:2] == ["-I"]
            and p.argv[3:4] in (["proxy"], ["serve"])
            and os.path.realpath(p.argv[2]) == package
        ]

    def busy(self) -> bool:
        """Whether a browser command other than the caller's runs, ours or the
        engine's own: either may be starting a browser that is not yet listening.
        Under the gate's lock (*admitting*) one of ours is not: it waits for the
        lock or has become the engine's.

        The engine's daemon is the same binary with no arguments, and it outlives
        a Chrome the OOM killer took by up to its idle timeout, so counting it
        would keep a person's start from opening anything for that long.
        """
        wrapper = str(Path(__file__).resolve().parent)
        uid, me = os.getuid(), os.getpid()
        for p in self.processes:
            if p.uid != uid or p.pid == me:
                continue
            # The wrapper on PATH names this package by its own folder.
            if not self.admitting and p.argv[1:2] == ["-I"] and p.argv[3:4] == ["agent-browser"] and (
                os.path.realpath(p.argv[2]) == wrapper
            ):
                return True
            if any(p.argv[1:]) and p.exe == self.host.engine:
                return True
        return False

    def engine_gone(self) -> bool:
        """Whether nothing of the engine runs: no daemon, so no browser, and no
        command that may be starting one."""
        return not self.daemons() and not self.busy()

    def browser_up(self) -> bool:
        """Whether a Chrome the engine's daemon runs is up. One its dead daemon left
        behind is not: no command reaches it, and the next command reaps it."""
        return any(self._run_by_engine(p) for p in self.browsers())

    def live(self) -> bool:
        """Whether Chrome is up behind the engine's stream.

        The stream is the engine's daemon, which outlives a Chrome the OOM
        killer took, so a listening port alone would show a person a frozen
        page, and a command sent to it would launch another Chrome outside the
        wrapper's terms.
        """
        return self.browser_up() and daemon.listening(self.host.stream_port)

    def ready_profile(self) -> None:
        """Ready the kept profile for a browser to start on, with none running on
        it and no command that may be starting one.

        The lock a Chrome killed on it left goes: Chrome takes over its own stale
        lock only when the lock names this host, and a computer restored or moved
        comes back under another name. And the last browser's tabs go (``signin``).
        """
        if self.busy() or self.browsers(kept_only=True):
            return
        profile = signin.profile()
        for name in ("SingletonLock", "SingletonSocket", "SingletonCookie"):
            try:
                os.unlink(os.path.join(profile, name))
            except OSError:
                continue
        signin.forget_last_tabs(profile)

    def _run_by_engine(self, browser: Process) -> bool:
        return any(p.pid == browser.ppid and p.exe == self.host.engine for p in self.processes)

    def _reap_left_behind(self) -> Look:
        """A daemon killed outright leaves its Chrome running, holding memory,
        and neither the next command nor ``close`` can reach it. Its hold and
        launch go only once it is gone, so the next look at the processes cannot
        find it again and forget what a command records for the browser after it.
        """
        reaped = [p for p in self.browsers() if not self._run_by_engine(p)]
        seen = self.close(reaped)
        for p in reaped:
            # The kept profile stays, sign-ins and all, for the next browser.
            profile = _profile(p)
            if profile and _throwaway(profile):
                shutil.rmtree(profile, ignore_errors=True)
        return seen

    def _tree(self, tops: Iterable[Process]) -> list[Process]:
        """Each of *tops* and every process under it, each once. Chrome's own
        processes rewrite their arguments into one string, so only the browser
        shows its profile, and they are found as its descendants instead."""
        children: dict[int, list[Process]] = {}
        for p in self.processes:
            children.setdefault(p.ppid, []).append(p)
        tree: list[Process] = []
        found: set[int] = set()
        pending = list(tops)
        while pending:
            p = pending.pop()
            if p.pid in found:
                continue
            found.add(p.pid)
            tree.append(p)
            pending.extend(children.get(p.pid, ()))
        return tree

    def _browser_dies_first(self) -> None:
        """Put the whole browser ahead of the computer's other work for the OOM killer.

        Chrome lowers its renderers from what they inherit, and a large job outranks
        them once the top of the tree is gone but before their memory is back, so the
        job is killed anyway. The next command brings a browser back; analysis cannot be.
        Only Linux has the OOM killer's score, which it keeps in ``/proc``.
        """
        if not system.has_oom_killer():
            return
        for p in self._tree(self.browsers()):
            system.set_oom_score(p.pid, 1000)


def _kept_may_start() -> bool:
    """Whether a command running now may be starting a browser on the kept
    profile: the launch on record, which the gate wrote for the last command it
    admitted, runs there, or none is on record and the computer keeps sign-ins."""
    launched = launch.current()
    return launched.kept if launched else signin.kept()


def _profile(browser: Process) -> str | None:
    for arg in browser.argv:
        if arg.startswith("--user-data-dir="):
            return arg.split("=", 1)[1].rstrip("/")
    return None


def _throwaway(profile: str) -> bool:
    """Whether *profile* is one the engine made for a single browser."""
    return os.path.basename(profile).startswith(ENGINE_PROFILE_PREFIX)


def _any_left(ending: list[Process]) -> bool:
    # Only which are left, so on a Mac no arguments are read.
    left = {p.pid for p in system.processes(lambda exe: False)}
    return any(p.pid in left for p in ending)
