"""Set the browser up on a person's own machine, which no image prepared.

The engine is downloaded for the machine's platform and checked against the
release's digest. Chrome is the person's own Chrome-family browser where they
have one, started on a profile of the runtime's, so nothing of their everyday
browsing is touched. Where they have none, Chrome for Testing is downloaded
only when they ask (``install chrome``), being several hundred MB. The
runtime's ports are picked once, so its daemons and the browser always agree on
them, from a range no system hands out on its own.
"""

from __future__ import annotations

import glob
import hashlib
import os
import platform
import random
import shlex
import socket
import subprocess
import sys
import tempfile
import urllib.request
from pathlib import Path

from . import care, daemon, hold, host
from .browsers import BROWSERS
from .host import Host
from .protocol import (
    ENGINE_SHA256,
    ENGINE_VERSION,
    INSTALL_LOCK,
    InstallRefusal,
    locked,
    state_file,
    write_state,
)

_RELEASE = "https://github.com/vercel-labs/agent-browser/releases/download/v{version}/agent-browser-{platform}"
_ENGINE_DIR = "engine"
_DOWNLOAD_TIMEOUT_S = 120
_CHROME_DOWNLOAD_TIMEOUT_S = 900
#: Below every system's own range for outgoing connections (Linux from 32768,
#: a Mac from 49152), so none is handed out to a connection meanwhile.
_PORT_RANGE = (20000, 29999)
_PORT_TRIES = 200
#: What the Mac launcher is called, in the machine's state.
_LAUNCHER = "chrome"
_LOG = "install.log"

# Where the engine's own ``install`` puts Chrome for Testing: the archive's
# top folder dropped, under ``chrome-<version>``, or kept, as older engines did.
_TESTING_BROWSERS = (
    "chrome-*/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing",
    "chrome-*/chrome",
    "*/chrome-mac-arm64/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing",
    "*/chrome-mac-x64/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing",
    "*/chrome-linux64/chrome",
)


def install(*, chrome: bool = False) -> dict[str, object]:
    """Set up the engine, find a browser and pick ports, then record them.
    With *chrome*, download Chrome for Testing first where none is found.

    Answers whether the browser is ready, and the browser it uses or None.
    Nothing is recorded until the engine is in place, so a failed download
    leaves the machine as it was. One install at a time: another waits.
    """
    with locked(INSTALL_LOCK):
        return _install(chrome)


def get_chrome() -> None:
    """Start downloading Chrome for Testing, on its own, since it takes
    minutes; ``installing`` says when it is done.

    Always started, since an install under way may be the setup an acquire
    began, which downloads nothing: this one waits for it, and finds Chrome
    already there after another that downloaded it.
    """
    daemon.detach("install", "chrome", "--wait", log=_LOG)


def _install(chrome: bool) -> dict[str, object]:
    name = platform_name()
    if name is None:
        return _answer(None, "unsupported")
    try:
        engine = _engine(name)
    except (OSError, ValueError):
        return _answer(None, "engine")
    found = find_chrome()
    if found is None and chrome:
        _download_chrome(engine)
        found = find_chrome()
    with care.admitted() as seen:
        before = host.current()
        try:
            ports = _ports(seen, before)
        except OSError:
            return _answer(found, "ports")
        after = Host(
            own=True,
            engine=engine,
            chrome=found or "",
            executable=_executable(found) if found else "",
            stream_port=ports[0],
            bridge_port=ports[1],
            proxy_port=ports[2],
        )
        if after != before and before.own:
            # What runs was started on the old record, which nothing would know
            # it by once it changes. A person's navigation runs the engine
            # outside admission, so it is waited for, in the gate's own order,
            # and what runs read again after the wait.
            with hold.navigating():
                seen.reread().end_all()
                host.record(after)
        else:
            host.record(after)
        _drop_older_engines(engine)
    return _answer(found, None if found else "no_chrome")


def _answer(chrome: str | None, refused: InstallRefusal | None) -> dict[str, object]:
    return {"done": refused is None, "reason": refused, "chrome": chrome}


def platform_name() -> str | None:
    """This machine's platform, as the engine's release names it."""
    system = {"darwin": "darwin", "linux": "linux"}.get(sys.platform)
    machine = {"x86_64": "x64", "amd64": "x64", "arm64": "arm64", "aarch64": "arm64"}.get(
        platform.machine().lower()
    )
    name = f"{system}-{machine}" if system and machine else None
    return name if name in ENGINE_SHA256 else None


def _engine(name: str) -> str:
    """The engine for *name* at the pinned version, downloaded when it is not
    already here."""
    folder = state_file(_ENGINE_DIR)
    target = folder / f"agent-browser-{ENGINE_VERSION}"
    if os.access(target, os.X_OK):
        return os.path.realpath(target)
    folder.mkdir(parents=True, exist_ok=True)
    url = _RELEASE.format(version=ENGINE_VERSION, platform=name)
    fd, partial = tempfile.mkstemp(dir=folder, prefix=".download-")
    try:
        digest = hashlib.sha256()
        with os.fdopen(fd, "wb") as out, urllib.request.urlopen(url, timeout=_DOWNLOAD_TIMEOUT_S) as response:
            while chunk := response.read(1 << 20):
                digest.update(chunk)
                out.write(chunk)
        if digest.hexdigest() != ENGINE_SHA256[name]:
            raise ValueError(f"the engine downloaded from {url} is not the release's")
        os.chmod(partial, 0o755)
        os.replace(partial, target)
    finally:
        Path(partial).unlink(missing_ok=True)
    return os.path.realpath(target)


def _drop_older_engines(engine: str) -> None:
    """Remove every engine but *engine*. Only once what an older one runs has
    ended: on Linux a process whose binary is gone reads as that path marked
    deleted, which nothing would know as the engine's to end."""
    for older in state_file(_ENGINE_DIR).glob("agent-browser-*"):
        if os.path.realpath(older) != engine:
            older.unlink(missing_ok=True)


def find_chrome() -> str | None:
    """The Chrome-family browser this machine has, its own before Chrome for
    Testing, by the path its process runs from."""
    if sys.platform == "darwin":
        homes = ("/Applications", os.path.expanduser("~/Applications"))
        apps = [b.mac_app for b in BROWSERS if b.mac_app]
        candidates = [os.path.join(home, app) for app in apps for home in homes]
    else:
        candidates = [binary for b in BROWSERS for binary in b.linux_binaries]
    browsers = os.path.expanduser("~/.agent-browser/browsers")
    for pattern in _TESTING_BROWSERS:
        candidates.extend(sorted(glob.glob(os.path.join(glob.escape(browsers), pattern)), reverse=True))
    for candidate in candidates:
        if os.access(candidate, os.X_OK) and not os.path.realpath(candidate).startswith("/snap/"):
            # A snap's browser cannot open a profile in a hidden folder.
            return os.path.realpath(candidate)
    return None


def _executable(chrome: str) -> str:
    """What the engine is told to start for *chrome*.

    The engine starts any profile of its own with Chrome's mock keychain, under
    which a Mac's Chrome keeps sign-ins with a key every copy of Chrome shares.
    A launcher drops that, so they are kept as the person's own browser keeps
    theirs. Elsewhere the browser runs as it is.
    """
    if sys.platform != "darwin":
        return chrome
    write_state(
        _LAUNCHER,
        "#!/bin/sh\n"
        "# Starts the browser for the engine without its mock keychain (install.py).\n"
        "for arg; do\n"
        "  shift\n"
        '  [ "$arg" = --use-mock-keychain ] || set -- "$@" "$arg"\n'
        "done\n"
        f'exec {shlex.quote(chrome)} "$@"\n',
        mode=0o700,
    )
    return str(state_file(_LAUNCHER))


def _download_chrome(engine: str) -> None:
    """Chrome for Testing, by the engine's own ``install``, which puts it where
    ``find_chrome`` looks."""
    try:
        subprocess.run(
            [engine, "install"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            cwd="/",
            timeout=_CHROME_DOWNLOAD_TIMEOUT_S,
        )
    except (OSError, subprocess.SubprocessError):
        pass


def _ports(seen: care.Look, before: Host) -> tuple[int, int, int]:
    """The recorded ports, or three free ones once another program took one.

    A person's machine runs servers of its own, and one may take a port while
    the browser is closed; whatever listens there would pass for the stream,
    the bridge or the proxy, and get the browser's traffic. While a browser
    runs or starts the ports are its own, and are kept.
    """
    if not before.own:
        return _free_ports()
    recorded = (before.stream_port, before.bridge_port, before.proxy_port)
    if seen.daemons() or seen.browsers() or seen.busy():
        return recorded
    # Idle without a stream, and they would hold the ports being checked.
    seen.close(seen.services())
    # What a start takes for its own is what answers on loopback; a port its
    # last listener left in TIME_WAIT binds for the runtime's servers as before.
    return recorded if not any(daemon.listening(port) for port in recorded) else _free_ports()


def _free_ports() -> tuple[int, int, int]:
    """Three ports nothing on this machine listens on now."""
    chosen: list[int] = []
    for _ in range(_PORT_TRIES):
        port = random.randint(*_PORT_RANGE)
        if port not in chosen and _free(port):
            chosen.append(port)
            if len(chosen) == 3:
                return chosen[0], chosen[1], chosen[2]
    raise OSError("no free ports for the browser")


def _free(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        try:
            probe.bind(("127.0.0.1", port))
        except OSError:
            return False
    return True
