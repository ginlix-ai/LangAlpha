"""Names the server and the computer's browser runtime both use.

Stdlib only: it ships into the computer with the runtime, and the server, the
image recipe and Chrome's policy import it on the host. Everything particular
to the engine that the server needs to know sits here.
"""

from __future__ import annotations

import fcntl
import importlib
import json
import os
import re
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import MISSING, asdict, dataclass, fields
from pathlib import Path
from typing import IO, Literal, Optional, TypeVar, Union, get_args, get_origin, get_type_hints
from urllib.parse import quote_plus, urlsplit

#: The package directory under ``_internal/src/``, run as ``python3 -I <it> <command>``:
#: ``agent-browser`` with the CLI's own arguments is the wrapper on the agent's
#: PATH, and the commands ``machine.main`` reads answer the server.
PACKAGE_NAME = "browser_runtime"

#: The engine's unit in the toolset's table (``toolset_runtime``), which pins
#: its release once for every computer.
ENGINE_UNIT = "agent-browser"


@dataclass(frozen=True)
class EngineRelease:
    """The engine's release for one platform, checked against its digest."""

    version: str
    url: str
    sha256: str


def engine_release(platform: str) -> EngineRelease | None:
    """The engine's release for *platform*, as the release names it, or None
    where it has none. A computer's toolset installs the Linux one; a person's
    own machine downloads the one for its platform (``install``).

    Read when asked, not at import, as every wrapper command imports this
    module. A table that cannot be read raises rather than answering None,
    which an install would report as an unsupported platform.
    """
    unit = next((u for u in _toolset_runtime("units").load().units if u["name"] == ENGINE_UNIT), None)
    if unit is None:
        raise LookupError(f"the toolset's table has no {ENGINE_UNIT} unit")
    release = unit["platforms"].get(platform)
    return EngineRelease(unit["version"], release["url"], release["sha256"]) if release else None


def toolset_seen() -> Toolset | None:
    """What the computer's toolset holds, read from its state file and the
    marker a converge leaves, as the computer's user, so the server learns it
    from the browser's own ``status`` rather than asking root a second time.
    None where the runtime that reads them did not ship beside this one.
    """
    try:
        units, converge = _toolset_runtime("units"), _toolset_runtime("converge")
    except ImportError:
        return None
    root = units.DEFAULT_ROOT
    return Toolset(converge.read_state(root)["version"], converge.converging(root))


def _toolset_runtime(module: str):
    """The toolset runtime's *module*, which ships beside this package:
    top-level on the machine, whose path starts at ``_internal/src``, and in
    the same parent package on the host."""
    parent = (__package__ or "").rpartition(".")[0]
    name = f"toolset_runtime.{module}"
    return importlib.import_module(f"{parent}.{name}" if parent else name)


#: Where the image installs the real CLI. ``agent-browser`` on a command's PATH
#: is the wrapper, which runs this one.
ENGINE_BIN = "/usr/local/lib/agent-browser/agent-browser"
#: A stable name for the Chrome the image's Playwright pin bakes in, whose own
#: path carries a revision. The engine starts Chrome by it, which is how one the
#: agent's code starts itself is told apart.
CHROME_BIN = "/usr/local/bin/chrome-for-testing"
#: Where the image's Chrome reads the managed policy that keeps it off cloud
#: metadata hosts and the computer's service ports (``chrome_policy``).
CHROME_POLICY_FILE = "/etc/opt/chrome_for_testing/policies/managed/langalpha.json"
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


def chrome_policy() -> dict:
    """Chrome's managed policy on a cloud computer, which the toolset's
    ``chrome-policy`` unit carries (``scripts/gen_toolset_pins.py`` writes it
    there). Playwright's Chromium is Chrome for Testing, which reads it only
    from /etc/opt/chrome_for_testing.

    The proxy every request goes through enforces what the browser may reach,
    by the address a name resolves to (``proxy.py``). The policy asks the same
    of Chrome where it can: the list stops a navigation to the metadata
    services and the agents beside it on loopback by name, local network
    access stays gated (on by default in the pinned Chrome, whose only switch
    for it is this opt-out), and WebRTC sends nothing the proxy does not carry.
    The rest of loopback stays open, where the agent's own apps run. Nothing
    asks to be remembered.
    """
    return {
        "URLBlocklist": [
            # An IPv6 address is matched as a URL writes it, in brackets.
            *(f"[{host}]" if ":" in host else host for host in METADATA_HOSTS),
            *DOCKER_HOSTS,
            # localhost and the address are one place reached by two names, so
            # each port is listed for both.
            *(f"{host}:{port}" for host in ("localhost", "127.0.0.1", "[::1]") for port in SERVICE_PORTS),
        ],
        "LocalNetworkAccessRestrictionsTemporaryOptOut": False,
        "WebRtcIPHandling": "disable_non_proxied_udp",
        "PasswordManagerEnabled": False,
        "AutofillAddressEnabled": False,
        "AutofillCreditCardEnabled": False,
        "BrowserSignin": 0,
        "SyncDisabled": True,
    }


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
#: The sizes, in page pixels, a page takes to fit the view it is watched in
#: (``fit``): anything outside is brought to the nearest edge. A cloud
#: computer's largest is smaller, since every pixel is drawn and encoded by the
#: CPU its agent works on; a person's own machine's CPU is theirs.
FIT_MIN = (320, 240)
FIT_MAX_OWN = (1920, 1600)
FIT_MAX_CLOUD = (1600, 1000)


def fit_max(own: bool) -> tuple[int, int]:
    """The largest page a view is fitted to on a machine of the person's own,
    or *own* False, on a cloud computer."""
    return FIT_MAX_OWN if own else FIT_MAX_CLOUD
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
#: Why a person's close left the browser open: someone else holds its page, or
#: it did not end.
CloseRefusal = Literal["held", "failed"]
CLOSE_REFUSALS: tuple[CloseRefusal, ...] = get_args(CloseRefusal)
#: Why bringing in an everyday browser's sign-ins did nothing, as the machine
#: answers: no such profile here, sign-ins are not kept, the browser's key could
#: not be read, memory is short, or anything else.
BringRefusal = Literal["not_found", "not_kept", "keychain", "low_memory", "failed"]
BRING_REFUSALS: tuple[BringRefusal, ...] = get_args(BringRefusal)
#: Why a machine of the person's own could not set its browser up: a platform the
#: engine has no build for, the engine's download, no free ports, or no
#: Chrome-family browser to drive.
InstallRefusal = Literal["unsupported", "engine", "ports", "no_chrome"]
INSTALL_REFUSALS: tuple[InstallRefusal, ...] = get_args(InstallRefusal)
#: The most a person may type into the address field.
MAX_ADDRESS_LENGTH = 2048
#: The longest page address opened: what a search for the longest text typed
#: becomes, every character of it taking up to twelve once percent-encoded.
MAX_URL_LENGTH = 32 * 1024
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
    if not text or len(text) > MAX_URL_LENGTH or any(c.isspace() or not c.isprintable() for c in text):
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


#: Where text that names no site is searched for. DuckDuckGo and Google put a
#: robot check in front of a browser on a cloud computer's address.
SEARCH_URL = "https://www.bing.com/search?q="
#: Schemes a person may type that open no web page; refused rather than searched.
_OTHER_SCHEMES = re.compile(r"^(file|javascript|data|chrome|about|view-source|ftp|blob|mailto|tel):", re.IGNORECASE)
_SCHEME = re.compile(r"^[a-z][a-z0-9+.-]*://", re.IGNORECASE)
_HOST = re.compile(r"^(?P<host>\[[0-9a-f:.]+\]|[^\s/:?#@\[\]]+)(?::(?P<port>\d{1,5}))?(?P<rest>[/?#].*)?$", re.IGNORECASE)
_IPV4 = re.compile(r"^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$")
_LABEL = re.compile(r"^(?!-)[a-z0-9-]{1,63}(?<!-)$", re.IGNORECASE)
#: Top-level names kept for local use, which no TLD list carries: multicast
#: DNS's ``local``, private-use ``internal`` and RFC 6761's ``test`` and
#: ``example``. Chrome reads a name under one as a site.
_LOCAL_TLDS = frozenset({"local", "internal", "test", "example"})


def typed_address(text: str) -> str | None:
    """Where text typed into a browser's address bar goes, as Chrome's reads it:
    an http or https address, a search for what names no site, or None for
    another scheme, which reaches Chrome's own pages or the machine's files.

    A site is named with a scheme at the start, or as ``localhost``, an IP
    address, or a name whose last label is a top-level domain (``tlds``) or one
    kept for local use such as ``local``, in any script, with or without a port
    and a path; a bare name with a port or a path names one too, as an intranet
    host does. Anything else, spaces included, is searched for, and a leading
    ``?`` searches whatever follows it.
    """
    text = text.strip()
    if not text or len(text) > MAX_ADDRESS_LENGTH or not text.isprintable():
        return None
    if text.startswith("?"):
        return _search(text[1:])
    if _OTHER_SCHEMES.match(text):
        return None
    if _SCHEME.match(text):
        return page_address(text.replace(" ", "%20"))
    if any(c.isspace() for c in text):
        return _search(text)
    named = _HOST.match(text)
    if named is None:
        return _search(text)
    host = named["host"].lower().rstrip(".")
    scheme = _site_scheme(host, bare_ok=bool(named["port"] or named["rest"]))
    if scheme is None:
        return _search(text)
    # The scheme given here, so a ``://`` later in the text is not read as one.
    return page_address(f"{scheme}://{text}")


def _site_scheme(host: str, *, bare_ok: bool) -> str | None:
    """The scheme a site named *host* is opened on, or None for no site. As
    Chrome does, a public name is opened on https, and a local name, an IP
    address or a bare intranet name, which serve plain http, on http."""
    if host == "localhost" or host.endswith(".localhost") or host.startswith("["):
        return "http"
    ip = _IPV4.match(host)
    if ip:
        return "http" if all(int(part) <= 255 for part in ip.groups()) else None
    try:
        # A name in another script is checked as the ASCII it is looked up by.
        host = host.encode("idna").decode("ascii")
    except UnicodeError:
        return None
    labels = host.split(".")
    if not all(_LABEL.match(label) for label in labels):
        return None
    if len(labels) == 1:
        return "http" if bare_ok else None
    from .tlds import TLDS

    tld = labels[-1]
    if tld in _LOCAL_TLDS:
        return "http"
    return "https" if tld in TLDS else None


def _search(text: str) -> str | None:
    text = text.strip()
    return SEARCH_URL + quote_plus(text) if text else None


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


R = TypeVar("R")


def read_record(name: str, cls: type[R], *, exact: bool = False) -> R | None:
    """The frozen dataclass *cls* kept in the state file *name*, or None when
    there is none or it holds something else. Fields are str, int, bool or a
    ``Literal`` of those, any of them ``Optional``, and a value of any other
    type is something else, a bool for an int included. A field whose key is
    absent takes its default, as from a runtime that wrote fewer of them;
    *exact* asks for every field and no other key."""
    try:
        stored = json.loads(state_file(name).read_text())
    except (OSError, ValueError):
        return None
    if not isinstance(stored, dict):
        return None
    kinds = get_type_hints(cls)
    each = fields(cls)  # type: ignore[arg-type]
    if exact and stored.keys() != {f.name for f in each}:
        return None
    values = {}
    for f in each:
        if f.name not in stored:
            if f.default is MISSING:
                return None
            continue
        if not _holds(kinds[f.name], stored[f.name]):
            return None
        values[f.name] = stored[f.name]
    return cls(**values)


def _holds(kind: object, value: object) -> bool:
    if get_origin(kind) is Union:
        return any(_holds(option, value) for option in get_args(kind))
    if kind is type(None):
        return value is None
    if get_origin(kind) is Literal:
        return any(type(value) is type(option) and value == option for option in get_args(kind))
    return type(value) is kind


def write_record(name: str, record: object) -> None:
    """Keep the dataclass *record* in the state file *name*, for ``read_record``."""
    write_state(name, json.dumps(asdict(record)))  # type: ignore[call-overload]


def lock(name: str, *, wait: bool = True) -> IO[str] | None:
    """Take the lock file *name*: the open file that holds it until it is
    closed, or None when, without *wait*, it is held elsewhere. The file is not
    inherited, so a process that execs another lets the lock go as it does."""
    path = state_file(name)
    path.parent.mkdir(parents=True, exist_ok=True)
    held = open(path, "a")
    try:
        fcntl.flock(held, fcntl.LOCK_EX if wait else fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        held.close()
        return None
    except BaseException:
        held.close()
        raise
    return held


#: Held for as long as an install runs (``install``), so another waits for it
#: and a status, or a command that finds no browser, can say one is under way.
INSTALL_LOCK = "installing.lock"


def installing() -> bool:
    """Whether an install is under way."""
    try:
        with locked(INSTALL_LOCK, wait=False) as taken:
            return not taken
    except OSError:
        return False


@contextmanager
def locked(name: str, *, wait: bool = True) -> Iterator[bool]:
    """Hold the lock file *name* for the block; whether it was taken. Without
    *wait*, one held elsewhere is not waited for, and the block runs untaken."""
    held = lock(name, wait=wait)
    if held is None:
        yield False
        return
    with held:
        yield True


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
SIGNINS_MESSAGE = (
    "agent-browser: {} is off here: this browser keeps the person's sign-ins, and what it "
    "prints would carry them. To see whether a site is signed in, or read what it shows, use "
    "`snapshot` or `get text` on the page."
)
NOT_INSTALLED_MESSAGE = (
    "agent-browser: the browser is not set up on this computer. Use `web-scraping` instead, "
    "and tell the person the Browser tab sets it up or says why it cannot."
)
SETTING_UP_MESSAGE = (
    "agent-browser: the browser is still being set up on this computer. Try again in a few "
    "minutes; use `web-scraping` meanwhile."
)
NO_CHROME_MESSAGE = (
    "agent-browser: this computer has no Chrome, Edge, Brave, Arc or Chromium for the browser. "
    "The person can get Chrome from the Browser tab; use `web-scraping` meanwhile."
)
HELD_MESSAGE = (
    "browser: the person has taken control of this browser. Nothing was done. "
    "Reading the page still works; anything else is refused until they hand it back."
)
CHANGED_MESSAGE = (
    "browser: the person used this browser since you last read it. Nothing was done. "
    "Read the page again, then repeat the command if it still makes sense."
)
REOPEN_MESSAGE = (
    "browser: the browser restarted for a tool update and could not open {} again. "
    "Nothing was done. Open that address with `agent-browser open`, then repeat the command."
)
#: Set on a command the runtime runs for a person (opening the browser from the
#: Browser tab), which is not the agent's and is told nothing meant for it. Its
#: value is the count of closes the start saw (``gate.person_closes``).
FOR_PERSON_ENV = "LANGALPHA_BROWSER_FOR_PERSON"
PERSON_CLOSED_MESSAGE = (
    "browser: the user closed this computer's browser from the Browser tab, so the tabs it "
    "had open are gone and this command runs in a new browser. Open again any page you still need."
)
TABS_CLOSED_MESSAGE = (
    "browser: the browser restarted for a tool update and kept only the tab that was "
    "showing. It closed these tabs: {}. Open any you still need with `agent-browser tab new <url>`."
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
class Toolset:
    """The toolset as ``status`` reports it: the set version its state file
    records (None until every unit is current) and whether a converge holds
    its lock."""

    version: Optional[str] = None
    converging: bool = False


@dataclass(frozen=True)
class Status:
    """What ``status`` and ``start`` print: whether the image has the engine,
    whether a browser is up behind a bridge, and the token its stream opens on.
    ``low_memory`` is a start that opened nothing because memory is short;
    ``kept``, a running browser on the kept profile, which keeps its sign-ins;
    ``port``, where the bridge listens; ``installing``, a machine with no
    browser yet that is setting one up (``install``); ``toolset``, on a cloud
    computer with no browser, which its toolset may be bringing, what that
    toolset holds, and None from a runtime that does not report it."""

    installed: bool
    running: bool = False
    token: str | None = None
    low_memory: bool = False
    kept: bool = False
    port: int = BRIDGE_PORT
    installing: bool = False
    toolset: Optional[Toolset] = None

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
        port = answer.get("port")
        return cls(
            bool(answer.get("installed")),
            running,
            token,
            low_memory=not running and answer.get("low_memory") is True,
            kept=running and answer.get("kept") is True,
            # A runtime from before a machine's ports were its own says none.
            port=port if type(port) is int and 0 < port < 65536 else BRIDGE_PORT,
            installing=answer.get("installing") is True,
            toolset=_read_toolset(answer.get("toolset")),
        )


def _read_toolset(answer: object) -> Toolset | None:
    if not isinstance(answer, dict):
        return None
    version = answer.get("version")
    return Toolset(version if isinstance(version, str) else None, answer.get("converging") is True)


#: A revision: fixed-width lowercase hex nanoseconds, so text order is save order.
REVISION_PATTERN = re.compile(r"[0-9a-f]{20}")
NO_REVISION = "0" * 20


@dataclass(frozen=True)
class Choices:
    """What a computer's owner chose for its browser, as the server hands it to
    the machine in one ``choices`` command and the machine keeps it.

    ``quality`` is the live view's, None to follow the computer's size;
    ``window``, whether it also shows on a Mac's screen; ``signins``, whether a
    browser starts on the kept profile. ``revision`` orders every save of any of
    them, made by the database under the row's lock, so a machine drops a
    bundle older than the one it holds, however late it arrives.
    ``signins_revision`` is the revision of the last save of the sign-ins
    choice: a machine erases the kept profile whenever that one advances, which
    is how a save of the same choice signs the browser out. ``computer`` is
    whose choices these are: revisions order one computer's saves only, so a
    bundle from another computer than the one held, as on a folder one took
    over from another, is taken as the first, erasing the kept profile the
    other left, however their revisions compare.

    ``Optional`` rather than ``X | None``: the runtime reads this on a Mac's
    own Python 3.9, where ``get_type_hints`` cannot evaluate the latter.
    """

    quality: Optional[StreamQualityName] = None
    window: bool = False
    signins: bool = True
    signins_revision: str = NO_REVISION
    revision: str = NO_REVISION
    computer: str = ""

    def to_wire(self) -> str:
        return json.dumps(asdict(self), separators=(",", ":"))

    @classmethod
    def from_wire(cls, text: str) -> Optional[Choices]:
        """The choices *text* holds, or None unless it holds every one, each of
        its kind."""
        try:
            given = json.loads(text)
        except ValueError:
            return None
        names = {f.name for f in fields(cls)}
        if not isinstance(given, dict) or given.keys() != names:
            return None
        kinds = get_type_hints(cls)
        if not all(_holds(kinds[name], given[name]) for name in names):
            return None
        if not (REVISION_PATTERN.fullmatch(given["revision"]) and REVISION_PATTERN.fullmatch(given["signins_revision"])):
            return None
        return cls(**given)
