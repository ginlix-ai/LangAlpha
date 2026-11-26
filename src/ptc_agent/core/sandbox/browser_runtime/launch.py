"""What the open browser was launched with, which every command must repeat.

The engine takes a user agent, the stream's settings and the profile, and Chrome
its scale, only when the browser starts, and restarts it, losing its pages, when a command
asks for anything else. So both are settled together when a browser starts and
kept in one record until it closes, and every command, the agent's or one run
for the person, runs with that record (``engine_env``).
"""

from __future__ import annotations

import hashlib
import math
import os
import pwd
import stat
import subprocess
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from . import signin
from .engine import for_launch
from .host import Host
from .protocol import STREAM_QUALITIES, StreamQuality, StreamQualityName, read_record, state_file, write_record

_FILE = "launch"
# What would log the browser in or launch it some other way, from the environment,
# and the daemon's own settings, any of which differing restarts it and closes
# the browser (``daemon_config_fingerprint``).
_UNSET = (
    "AGENT_BROWSER_DEBUG", "AGENT_BROWSER_ACTION_POLICY", "AGENT_BROWSER_CONFIRM_ACTIONS",
    "AGENT_BROWSER_DEFAULT_TIMEOUT", "AGENT_BROWSER_NO_AUTO_DIALOG",
    "AGENT_BROWSER_PROFILE", "AGENT_BROWSER_STATE", "AGENT_BROWSER_RESTORE",
    "AGENT_BROWSER_SESSION_NAME", "AGENT_BROWSER_SESSION", "AGENT_BROWSER_NAMESPACE",
    "AGENT_BROWSER_PROVIDER", "AGENT_BROWSER_CONFIG", "AGENT_BROWSER_CDP",
    "AGENT_BROWSER_AUTO_CONNECT", "AGENT_BROWSER_EXTENSIONS", "AGENT_BROWSER_PLUGINS",
    "AGENT_BROWSER_USER_AGENT", "AGENT_BROWSER_HEADED", "AGENT_BROWSER_ARGS",
    "AGENT_BROWSER_PROXY", "AGENT_BROWSER_PROXY_BYPASS", "AGENT_BROWSER_ALLOW_FILE_ACCESS",
    "AGENT_BROWSER_ALLOWED_DOMAINS", "AGENT_BROWSER_ENABLE", "AGENT_BROWSER_INIT_SCRIPTS",
    "AGENT_BROWSER_ENGINE", "AGENT_BROWSER_WEBGPU", "AGENT_BROWSER_HIDE_SCROLLBARS",
    "AGENT_BROWSER_IDLE_TIMEOUT_MS",
    "AGENT_BROWSER_PROXY_USERNAME", "AGENT_BROWSER_PROXY_PASSWORD", "AGENT_BROWSER_CA_CERT",
    "AGENT_BROWSER_CLEAR_CA_CERT", "AGENT_BROWSER_USE_SYSTEM_CA", "AGENT_BROWSER_NO_XVFB",
)
# The page's size: a laptop's 16:10, which every stream quality's frame scales
# evenly. The engine's own window is 1280 by 720, and Chrome keeps part of a
# window for its toolbar, drawn or not: 143 pixels in the image's Chrome for
# Testing, which left the page a strip 577 tall that cut off most forms, and 87
# in Chrome on a Mac and Debian's Chromium. Sized for the most, a page is never
# shorter than the viewport, only taller. A window size given here replaces the
# engine's.
VIEWPORT = (1280, 800)
#: How long an open Chrome is left unused before it closes, so the memory it
#: holds goes back to the computer's other work.
IDLE_CLOSE_S = 20 * 60
_TOOLBAR_PX = 143
_DISK_CACHE_BYTES = 64 * 1024 * 1024
# The longest folder whose engine socket (``<folder>/default.sock``) a Mac allows.
_SOCKET_DIR_MAX = 90
# Where a folder too long for that gets a short one: the same on every login.
_SHORT_SOCKET_ROOT = "/tmp"
# The same on every command, or the engine would restart the browser.
_ENGINE_ENV = {
    "AGENT_BROWSER_PROXY_BYPASS": "<-loopback>",
    "AGENT_BROWSER_CONTENT_BOUNDARIES": "1",
    "AGENT_BROWSER_NO_WEBMCP": "1",
    # A config file named here replaces the engine's search for one in the
    # folder a command runs from and in the home directory, either of which
    # could launch the browser some other way, or differently for the person's
    # commands, which run from elsewhere, and so restart it.
    "AGENT_BROWSER_CONFIG": str(Path(__file__).with_name("engine.json")),
}


@dataclass(frozen=True)
class Launch:
    """The user agent, empty for the engine's own, the live view's quality,
    whether the browser runs on the kept profile (``signin``), and whether it
    shows as a window on the machine's screen (``window``).

    ``closing`` marks the launch of a browser a command means to close, after
    working on it or not: the command runs on the launch as it is, since a
    changed one would restart the browser under it, and the user agent goes
    only once the next command finds the browser gone. One that finds it up,
    since the close never ran or never finished, keeps it.

    ``engine`` is the version of the engine the browser started on, where the
    host's engine is a link a tool update moves (``engine.pinned``), and empty
    where it is the file itself. A command on another version would restart
    the browser under it, so a new one reaches the browser when it next starts.
    A browser that started with none, whose engine an update replaced since, is
    pinned to the file its daemon runs (``engine.held_by``).

    ``page_width`` and ``page_height`` are the largest page a viewer's panel
    fits it to (``fit``), zero for a browser from before fitting, which keeps
    the agent's own size. They set how large its frames may be, which the
    engine would restart the browser to change, so they are the ones it
    started with. ``agent_sized`` is a page the agent sized itself
    (``set viewport``, ``set device``), which no viewer's panel resizes.
    """

    user_agent: str
    quality: StreamQualityName
    closing: bool = False
    kept: bool = False
    window: bool = False
    engine: str = ""
    page_width: int = 0
    page_height: int = 0
    agent_sized: bool = False

    @property
    def stream(self) -> StreamQuality:
        return STREAM_QUALITIES[self.quality]

    @property
    def page_max(self) -> tuple[int, int]:
        return (self.page_width, self.page_height) if self.page_width and self.page_height else VIEWPORT

    def engine_env(self, base: Mapping[str, str], h: Host) -> dict[str, str]:
        """What the engine runs with on *h*: *base* without what would launch
        the browser another way, and this launch, so a command never restarts
        the browser it opened."""
        env = dict(base)
        for name in _UNSET:
            env.pop(name, None)
        env.update(_ENGINE_ENV)
        env.update(
            {
                # Every request goes through the proxy, loopback too, which
                # Chrome would otherwise send direct (``proxy.py``).
                "AGENT_BROWSER_PROXY": h.proxy_url,
                "AGENT_BROWSER_EXECUTABLE_PATH": h.executable,
                "AGENT_BROWSER_STREAM_PORT": str(h.stream_port),
                "AGENT_BROWSER_SOCKET_DIR": daemon_dir(h),
            }
        )
        env.update(self._browser_env())
        user_agent = self.user_agent.rstrip("\n")
        if user_agent:
            env["AGENT_BROWSER_USER_AGENT"] = user_agent
        return env

    def run(self, h: Host, *args: str, timeout: float) -> str | None:
        """What the engine printed for *args*, run on this launch, or None
        when it failed."""
        env = self.engine_env(os.environ, h)
        # Read as text by the runtime's own callers, whatever the agent's shell set.
        env.pop("AGENT_BROWSER_JSON", None)
        result = subprocess.run(
            [for_launch(self, h), *args],
            env=env,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            errors="replace",
            cwd="/",
            timeout=timeout,
        )
        return (result.stdout or "").strip() if result.returncode == 0 else None

    def _browser_env(self) -> dict[str, str]:
        """What the engine starts this launch's browser with, and so must get on
        every command while that browser is open."""
        quality = self.stream
        # A page fitted to the person's view (``fit``) can be larger than the
        # agent's, and is drawn at the quality's own share of a page pixel.
        largest = self.page_max
        max_width = math.ceil(quality.width * largest[0] / VIEWPORT[0])
        max_height = math.ceil(quality.height * largest[1] / VIEWPORT[1])
        args = [
            "--proxy-bypass-list=<-loopback>",
            f"--window-size={VIEWPORT[0]},{VIEWPORT[1] + _TOOLBAR_PX}",
            # The cache sits in the profile, which on the kept one stays on the
            # computer's disk beside the person's work.
            f"--disk-cache-size={_DISK_CACHE_BYTES}",
        ]
        # A window on the person's screen is drawn at the screen's own scale, which
        # any other would blur for them.
        if quality.scale != 1 and not self.window:
            # Rendered at this scale, the screencast's frames are too. Emulating it
            # for the page instead leaves the frames at one pixel a page pixel.
            args.append(f"--force-device-scale-factor={quality.scale}")
        if self.kept:
            # Keeps a site's session cookies across the browser's closes, as the
            # person's own browser does by continuing where it left off. The flag,
            # not the profile's setting, which Chrome on a Mac resets when anything
            # but itself writes it. The tabs it would reopen are gone (``signin``).
            args.append("--restore-last-session")
        env = {
            # One argument a line: the window size holds a comma.
            "AGENT_BROWSER_ARGS": "\n".join(args),
            "AGENT_BROWSER_STREAM_QUALITY": str(quality.jpeg),
            "AGENT_BROWSER_STREAM_MAX_WIDTH": str(max_width),
            "AGENT_BROWSER_STREAM_MAX_HEIGHT": str(max_height),
        }
        if self.kept:
            env["AGENT_BROWSER_PROFILE"] = signin.profile()
        if self.window:
            env["AGENT_BROWSER_HEADED"] = "1"
        else:
            # An open Chrome holds memory the computer's other work needs, so one
            # left unused ends well before the engine's default hour. A window is
            # left to the engine, which keeps a headed one open: it cannot see the
            # person using it, and they close it themselves.
            env["AGENT_BROWSER_IDLE_TIMEOUT_MS"] = str(IDLE_CLOSE_S * 1000)
        return env


def current() -> Launch | None:
    """The record of the browser last launched, or None when there is none."""
    return read_record(_FILE, Launch)


def write(launch: Launch) -> None:
    write_record(_FILE, launch)


def forget() -> None:
    state_file(_FILE).unlink(missing_ok=True)


def daemon_dir(h: Host) -> str:
    """Where the engine finds its daemon, the same for every command: another
    folder would mean a second daemon and Chrome that the gate, the hold and
    the viewer never see. On a cloud computer the engine's own default, from
    the account rather than HOME, which a command can set; its last resort with
    no account. On a person's own machine one of the runtime's, since the
    default is where their own use of the engine finds its daemon."""
    if h.own:
        return _own_daemon_dir()
    try:
        return os.path.join(pwd.getpwuid(os.getuid()).pw_dir, ".agent-browser")
    except KeyError:
        return os.path.join(tempfile.gettempdir(), "agent-browser")


def _own_daemon_dir() -> str:
    """The runtime's folder for the daemon, or where the machine allows no
    socket path that long (a Mac allows 103 bytes, and a person can root the
    computer anywhere), a short one of this user's alone, named for it."""
    state = state_file("daemon")
    # In bytes, as the socket path's limit is: a character may take several.
    if len(os.fsencode(state)) <= _SOCKET_DIR_MAX:
        return str(state)
    name = hashlib.sha256(str(state).encode()).hexdigest()[:12]
    # Not ``$TMPDIR``, which differs between a login and the desktop's session
    # on a Mac: a command that found another folder would find no daemon there
    # and start a second Chrome on the profile the first holds.
    short = Path(_SHORT_SOCKET_ROOT, f"langalpha-browser-{name}")
    try:
        short.mkdir(mode=0o700)
    except FileExistsError:
        pass
    # In a temporary folder others write to, only one that is this user's,
    # and a folder, is used.
    found = os.lstat(short)
    if not stat.S_ISDIR(found.st_mode) or found.st_uid != os.getuid() or found.st_mode & 0o077:
        raise OSError(f"{short} is not this user's own folder")
    return str(short)
