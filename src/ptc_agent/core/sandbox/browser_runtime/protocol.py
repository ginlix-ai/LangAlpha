"""Names the server and the computer's browser runtime both use.

Stdlib only: it ships into the computer with the runtime, and the server, the
image recipe and Chrome's policy import it on the host. Everything particular
to the engine that the server needs to know sits here.
"""

from __future__ import annotations

import os
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Literal, get_args
from urllib.parse import urlsplit

#: The package directory under ``_internal/src/``, run as ``python3 -I <it> <command>``:
#: ``agent-browser`` with the CLI's own arguments is the wrapper on the agent's
#: PATH, and the commands ``machine.main`` reads answer the server.
PACKAGE_NAME = "browser_runtime"

#: Where the image installs the real CLI. ``agent-browser`` on a command's PATH
#: is the wrapper, which runs this one.
ENGINE_BIN = "/usr/local/lib/agent-browser/agent-browser"
#: A stable name for the Chrome the image's Playwright pin bakes in, whose own
#: path carries a revision. The engine starts Chrome by it, which is how one the
#: agent's code starts itself is told apart.
CHROME_BIN = "/usr/local/bin/chrome-for-testing"
#: How the engine names the throwaway profile it starts each Chrome on, in the
#: temporary directory. A Chrome the agent starts from the same path on a
#: profile of its own is not the engine's, and neither it nor its profile is
#: ever the runtime's to end.
ENGINE_PROFILE_PREFIX = "agent-browser-chrome-"

#: Where the engine's stream listens, on loopback only.
STREAM_PORT = 9223
#: Where the bridge in front of it listens, on a preview port.
BRIDGE_PORT = 9224
#: Where the filtering proxy every request of the browser goes through
#: listens, on loopback only.
PROXY_PORT = 9225
#: What a viewer's upgrade must ask for: ``<prefix><token>``.
PATH_PREFIX = "/b/"
#: What a bridge's token looks like (``secrets.token_urlsafe(32)``), so nothing
#: else a machine prints reaches a URL's path.
TOKEN_PATTERN = re.compile(r"[A-Za-z0-9_-]{43}")
#: The ports on the machine itself the browser must not reach, by any address
#: that is the machine: the stream, its bridge and the proxy, Daytona's toolbox,
#: computer use and terminal, E2B's agent, and Docker's daemon.
SERVICE_PORTS = (STREAM_PORT, BRIDGE_PORT, PROXY_PORT, 2280, 22220, 22222, 49983, 2375, 2376)
#: Cloud metadata services, which the browser must not reach at any port: the
#: proxy refuses each address here, and Chrome's policy each by name.
METADATA_HOSTS = (
    "169.254.169.254",
    "169.254.170.2",
    "fd00:ec2::254",
    "100.100.100.200",
    "metadata.google.internal",
    "metadata.goog",
)
#: The names a Docker container reaches its host by, refused at any port.
DOCKER_HOSTS = ("host.docker.internal", "gateway.docker.internal")


@dataclass(frozen=True)
class StreamQuality:
    """How the stream draws the page: frames at most ``width`` by ``height``, JPEG
    at ``jpeg``, and Chrome rendering at ``scale`` device pixels a page pixel."""

    width: int
    height: int
    jpeg: int
    scale: int = 1


#: What a person can choose for a computer's live view. Measured on one CPU at
#: eight frames a second on a page that redraws constantly: light about 3 Mbps,
#: standard 7, sharp 10, and retina 18 with the whole CPU to itself. A page at
#: rest sends a frame only when it changes.
StreamQualityName = Literal["light", "standard", "sharp", "retina"]
STREAM_QUALITIES: dict[StreamQualityName, StreamQuality] = {
    "light": StreamQuality(960, 600, 50),
    "standard": StreamQuality(1280, 800, 70),
    "sharp": StreamQuality(1280, 800, 85),
    "retina": StreamQuality(2560, 1600, 70, scale=2),
}
#: Retina's frames take a whole CPU to draw, so a computer with fewer than this
#: many gets ``sharp`` in its place, and one with ``RETINA_BY_DEFAULT`` or more
#: gets retina when no one has chosen.
RETINA_MIN_CPUS = 2
RETINA_BY_DEFAULT = 4

#: Who holds the page, as a viewer names itself: a short opaque id.
HOLDER_PATTERN = r"^[A-Za-z0-9_-]{1,64}$"
#: What a viewer does with its hold: start or extend it, extend only one it
#: still has, or hand it back.
HoldAction = Literal["take", "renew", "release"]
HOLD_ACTIONS: tuple[HoldAction, ...] = get_args(HoldAction)
#: A hand-back carries what the person still had down that their stream could
#: not deliver, a ``{"key"}`` or a ``{"button", "x", "y"}`` each. The page keeps
#: a press until it is told, so the machine lets go of these before the hold ends
#: and the agent never inherits a key or button held down.
MouseButton = Literal["left", "middle", "right"]
MOUSE_BUTTONS: tuple[MouseButton, ...] = get_args(MouseButton)
MAX_RELEASES = 16
_MAX_KEY_LENGTH = 32
_MAX_COORDINATE = 100_000


def release_commands(releases: object) -> list[list[str]] | None:
    """The engine commands that let go of *releases*, or None if any is not one."""
    if not isinstance(releases, list) or len(releases) > MAX_RELEASES:
        return None
    commands: list[list[str]] = []
    for release in releases:
        if not isinstance(release, dict):
            return None
        if release.keys() == {"key"} and _engine_key(release["key"]):
            commands.append(["keyup", release["key"]])
        elif release.keys() == {"button", "x", "y"} and release["button"] in MOUSE_BUTTONS:
            x, y = release["x"], release["y"]
            if not all(_coordinate(v) for v in (x, y)):
                return None
            # The engine releases where its own pointer last was, which is not
            # where the person's was.
            commands.append(["mouse", "move", str(round(x)), str(round(y))])
            commands.append(["mouse", "up", release["button"]])
        else:
            return None
    return commands


def _engine_key(key: object) -> bool:
    # A dash-led word is read as one of the CLI's flags, and any ``-h`` as a
    # call for its help, whatever command it follows.
    return (
        isinstance(key, str)
        and 0 < len(key) <= _MAX_KEY_LENGTH
        and key.isprintable()
        and (key == "-" or not key.startswith("-"))
    )


def _coordinate(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and 0 <= value <= _MAX_COORDINATE

#: Where the person holding the page can take it from the panel: to an address,
#: back or forward through the tab's history, or to itself again.
NavigateAction = Literal["open", "back", "forward", "reload"]
NAVIGATE_ACTIONS: tuple[NavigateAction, ...] = get_args(NavigateAction)
#: Why a navigation did nothing: the viewer no longer holds the page, no
#: browser is open, the address is not a web page's, or the engine failed.
NavigateRefusal = Literal["not_held", "not_running", "bad_address", "failed"]
NAVIGATE_REFUSALS: tuple[NavigateRefusal, ...] = get_args(NavigateRefusal)
MAX_ADDRESS_LENGTH = 2048
# Typed without a scheme, these are the person's own apps, which serve plain HTTP,
# the wildcard addresses included, which a dev server prints as where it listens.
_LOCAL_HOSTS = re.compile(
    r"^(localhost|127(\.\d{1,3}){3}|\[::1\]|0\.0\.0\.0|\[::\])(:\d+)?([/?#]|$)", re.IGNORECASE
)


def page_address(text: str) -> str | None:
    """The web page *text* names, read as a browser's address bar reads one
    typed without a scheme, or None for anything but an http or https page:
    another scheme reaches Chrome's own pages or the machine's files."""
    text = text.strip()
    if not text or len(text) > MAX_ADDRESS_LENGTH or any(c.isspace() or not c.isprintable() for c in text):
        return None
    if "://" not in text:
        text = ("http://" if _LOCAL_HOSTS.match(text) else "https://") + text
    try:
        parts = urlsplit(text)
    except ValueError:
        return None
    if parts.scheme.lower() not in ("http", "https") or not parts.hostname:
        return None
    return text


#: The machine's state, in one directory beside the ``src`` this package ships into.
STATE_DIR = ".browser"
#: The bridge's token, which opens its stream.
TOKEN_FILE = "token"
# <computer root>/_internal/src/browser_runtime/protocol.py
_STATE_ROOT = Path(__file__).resolve().parents[2]


def state_file(name: str) -> Path:
    return _STATE_ROOT / STATE_DIR / name


def write_state(name: str, text: str, mode: int = 0o600) -> None:
    """Replace the state file *name* whole, so a reader never sees half of it."""
    target = state_file(name)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{name}.{os.getpid()}")
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)
    with os.fdopen(fd, "w") as f:
        f.write(text)
    os.replace(temporary, target)


#: What the wrapper exits with when it refuses an argument.
REFUSED = 2
#: What it exits with when the gate refuses the command.
GATE_REFUSED = 3
#: What it exits with on a computer whose image has no engine.
NOT_INSTALLED = 127

LOGGED_OUT_MESSAGE = (
    "agent-browser: {} is off here: this browser is logged out and is the one the live view shows."
)
RESTART_MESSAGE = "agent-browser: {} is off here: it would restart this browser and lose its pages."
NOT_INSTALLED_MESSAGE = (
    "agent-browser: this computer was built before the browser. A new computer has it."
)
HELD_MESSAGE = (
    "browser: the person has taken control of this browser. Nothing was done. "
    "Reading the page still works; anything else is refused until they hand it back."
)
CHANGED_MESSAGE = (
    "browser: the person used this browser since you last read it. Nothing was done. "
    "Read the page again, then repeat the command if it still makes sense."
)
MEMORY_MESSAGE = (
    "browser: this computer is short of memory. Nothing was done. Close a tab with "
    "`agent-browser tab close`, or the browser with `agent-browser close`, and try again."
)
USER_AGENT_MESSAGE = (
    "browser: a user agent is set when this browser starts, and changing it now would "
    "restart it and lose its pages. Nothing was done. Run `agent-browser close`, then "
    "open with the one you want. It holds until the next close."
)


@dataclass(frozen=True)
class Status:
    """What ``status`` and ``start`` print: whether the image has the engine,
    whether a browser is up behind a bridge, and the token its stream opens on.
    ``low_memory`` is a start that opened nothing because memory is short."""

    installed: bool
    running: bool = False
    token: str | None = None
    low_memory: bool = False

    def to_wire(self) -> dict[str, object]:
        return asdict(self)

    @classmethod
    def read(cls, answer: object) -> Status:
        """The status an answer holds. Anything else comes from a machine with
        no runtime to answer, and so no browser either. A token not of the
        bridge's shape opens nothing, and is not put in a URL."""
        if not isinstance(answer, dict):
            return cls(installed=False)
        token = answer.get("token")
        token = token if isinstance(token, str) and TOKEN_PATTERN.fullmatch(token) else None
        running = bool(answer.get("running")) and token is not None
        return cls(
            bool(answer.get("installed")),
            running,
            token,
            low_memory=not running and answer.get("low_memory") is True,
        )
