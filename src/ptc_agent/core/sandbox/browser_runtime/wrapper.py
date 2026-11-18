"""``agent-browser`` on a computer, ahead of the real CLI on every command's PATH.

It gives the browser the one way it runs here, streaming at a rate a viewer can
afford and on the port the view watches, with every request through the proxy
that keeps it off the machine's own services, and keeps the bridge in front of
that stream up. The arguments are read once, as the engine reads them, and every
check judges that one reading, so an option placed before the command, or a
value that looks like one, is no way around it. This browser is logged out, so
what would save or bring in a login is refused, and so is what would start a
browser the view cannot see. A launch option given on one command and not the
next restarts the browser and loses its pages, so those are refused too, except
a user agent, which sites may need and which is held from the first command that
sets it until the browser closes. While the person holds the page, only what
reads it runs.
"""

from __future__ import annotations

import json
import os
import pwd
import subprocess
import sys
import tempfile
import traceback
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from . import launch
from .launch import Launch
from .protocol import (
    CHROME_BIN,
    ENGINE_BIN,
    GATE_REFUSED,
    LOGGED_OUT_MESSAGE,
    NOT_INSTALLED,
    NOT_INSTALLED_MESSAGE,
    PROXY_PORT,
    REFUSED,
    RESTART_MESSAGE,
    STREAM_PORT,
    StreamQuality,
)

Takes = Literal["value", "flag", "optional"]
Policy = Literal["allow", "login", "restart"]


def _options(takes: Takes, policy: Policy, *names: str) -> dict[str, tuple[Takes, Policy]]:
    return dict.fromkeys(names, (takes, policy))


# The engine's global options (``clean_args`` in its flags.rs), which it reads
# wherever they stand: one that takes a value takes the next argument, and a
# flag takes a following ``true`` or ``false``. ``--restore`` takes one only
# when what follows reads as a value, and is read here as taking none, since it
# is refused whatever follows. Each is allowed, refused as a login, or refused
# as a restart of the open browser.
_OPTIONS: dict[str, tuple[Takes, Policy]] = {
    **_options(
        "value", "login",
        "--profile", "--state", "--restore-save", "--session-name", "--cdp", "--session",
        "--namespace", "--provider", "--config", "--extension", "--headers", "-p",
    ),
    **_options("optional", "login", "--restore"),
    **_options("flag", "login", "--auto-connect"),
    # The daemon's own settings count too: one that differs from the running
    # daemon's stops it, and the browser with it (``daemon_config_fingerprint``).
    **_options(
        "value", "restart",
        "--executable-path", "--args", "--proxy", "--proxy-bypass", "--ca-cert",
        "--allowed-domains", "--enable", "--init-script", "--engine",
        "--action-policy", "--confirm-actions", "--idle-timeout",
    ),
    **_options(
        "flag", "restart",
        "--headed", "--no-ca-cert", "--use-system-ca", "--allow-file-access",
        "--hide-scrollbars", "--webgpu", "--no-webmcp", "--debug", "--no-auto-dialog",
    ),
    **_options(
        "value", "allow",
        "--restore-check-url", "--restore-check-text", "--restore-check-fn", "--cdp-headers",
        "--user-agent", "--device", "--color-scheme", "--download-path", "--max-output",
        "--input-mode", "--screenshot-dir", "--screenshot-quality", "--screenshot-format",
        "--model",
    ),
    **_options(
        "flag", "allow",
        "--json", "--ignore-https-errors", "--pin-tab", "--no-pin-tab", "--annotate",
        "--content-boundaries", "--confirm-interactive", "-v", "--verbose",
        "-q", "--quiet", "--offline", "--quick", "--fix",
    ),
}
_LOGIN_COMMANDS = frozenset(
    {"auth", "state", "profiles", "connect", "dashboard", "chat", "mcp", "plugin", "plugins", "upgrade", "install", "stream"}
)
# A command whose subcommand brings in a login: ``cookies set``, ``set credentials``.
_LOGIN_SUBCOMMANDS = {"cookies": frozenset({"set"}), "set": frozenset({"credentials", "auth", "headers"})}
_LOOK_COMMANDS = frozenset(
    {"", "skills", "snapshot", "read", "get", "is", "wait", "screenshot", "pdf", "console", "errors", "diff"}
)
_BATCH_OPTIONS = frozenset({"--bail", "--omit-command"})
# What runs script the agent wrote in the page.
_SCRIPT_COMMANDS = frozenset({"eval", "addinitscript"})
# Any of these among a command's words, the engine prints help or its version
# and returns before it reaches a daemon.
_INFO_FLAGS = frozenset({"--help", "-h", "--version", "-V"})
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
    "AGENT_BROWSER_PROXY_USERNAME", "AGENT_BROWSER_PROXY_PASSWORD", "AGENT_BROWSER_CA_CERT",
    "AGENT_BROWSER_CLEAR_CA_CERT", "AGENT_BROWSER_USE_SYSTEM_CA", "AGENT_BROWSER_NO_XVFB",
)
# The page's size: a laptop's 16:10, which every stream quality's frame scales
# evenly. The engine's own window is 1280 by 720, and headless Chrome keeps 143
# pixels of a window for a toolbar it never draws, which left the page a strip
# 577 tall that cut off most forms. A window size given here replaces the engine's.
VIEWPORT = (1280, 800)
_UNDRAWN_TOOLBAR_PX = 143
# Every request goes through the proxy, loopback too, which Chrome would
# otherwise send direct (``proxy.py``). The same on every command, or the
# engine would restart the browser.
_ENGINE_ENV = {
    "AGENT_BROWSER_PROXY": f"http://127.0.0.1:{PROXY_PORT}",
    "AGENT_BROWSER_PROXY_BYPASS": "<-loopback>",
    "AGENT_BROWSER_EXECUTABLE_PATH": CHROME_BIN,
    "AGENT_BROWSER_STREAM_PORT": str(STREAM_PORT),
    "AGENT_BROWSER_CONTENT_BOUNDARIES": "1",
    "AGENT_BROWSER_NO_WEBMCP": "1",
    # An open Chrome holds memory the computer's other work needs, so one left
    # unused ends well before the engine's default hour.
    "AGENT_BROWSER_IDLE_TIMEOUT_MS": "1200000",
    # A config file named here replaces the engine's search for one in the
    # folder a command runs from and in the home directory, either of which
    # could launch the browser some other way, or differently for the person's
    # commands, which run from elsewhere, and so restart it.
    "AGENT_BROWSER_CONFIG": str(Path(__file__).with_name("engine.json")),
}
# Run as root, Chrome raises its own threads above everything else, and on one CPU
# a job beside it takes twice as long. Without the capability it stays level with them.
_DROP_NICE = ("setpriv", "--bounding-set=-sys_nice", "--inh-caps=-sys_nice")
_OOM_SCORE = Path("/proc/self/oom_score_adj")


@dataclass(frozen=True)
class Invocation:
    """One command as the engine reads it: its word, the first word after that,
    every argument after the word, and the global options with their values.
    ``unknown_lead`` is an option no one here knows standing before the word,
    which the engine may read some other way. ``given`` is every word as given,
    for a check that must not trust which of them an option took."""

    name: str = ""
    sub: str = ""
    args: tuple[str, ...] = ()
    options: tuple[tuple[str, str | None], ...] = ()
    unknown_lead: bool = False
    given: tuple[str, ...] = ()


def parse(argv: Sequence[str]) -> Invocation:
    """*argv* read once, as the engine reads it. The command word is the first
    argument that is not an option, after each known option has taken its value."""
    words: list[str] = []
    options: list[tuple[str, str | None]] = []
    name = ""
    unknown_lead = False
    i = 0
    while i < len(argv):
        arg = argv[i]
        flag, equals, given = arg.partition("=")
        known = _OPTIONS.get(flag)
        if equals and known and known[0] != "flag":
            # The engine passes ``--option=value`` on as an argument. Read here as
            # the option all the same: it is refused as one, and a user agent so
            # given is the one the gate holds.
            options.append((flag, given))
        elif known is None or equals:
            if name:
                words.append(arg)
            elif arg and not arg.startswith("-"):
                name = arg
            else:
                unknown_lead = True
        elif known[0] == "value":
            options.append((arg, argv[i + 1] if i + 1 < len(argv) else None))
            i += 1
        else:
            options.append((arg, None))
            if known[0] == "flag" and i + 1 < len(argv) and argv[i + 1] in ("true", "false"):
                i += 1
        i += 1
    sub = next((word for word in words if not word.startswith("-")), "")
    return Invocation(name, sub, tuple(words), tuple(options), unknown_lead, tuple(argv))


def expand(top: Invocation, stdin: list[list[str]] | None = None) -> list[Invocation]:
    """*top* and every command a ``batch`` in it runs, however deep, each read
    as the engine reads it. Only *top* reads its commands from *stdin*."""
    found = [top]
    i = 0
    while i < len(found):
        each = found[i]
        if each.name == "batch":
            given = [arg for arg in each.args if arg not in _BATCH_OPTIONS]
            if given:
                found.extend(parse(_shell_words(arg)) for arg in given)
            elif i == 0 and stdin:
                found.extend(parse(words) for words in stdin)
        i += 1
    return found


def _shell_words(text: str) -> list[str]:
    # The engine's own ``shell_words_split``, which is not quite a shell's.
    words: list[str] = []
    current = ""
    double = single = False
    chars = iter(text)
    for c in chars:
        if c == "\\" and not single:
            current += next(chars, "")
        elif c == '"' and not single:
            double = not double
        elif c == "'" and not double:
            single = not single
        elif c == " " and not double and not single:
            if current:
                words.append(current)
                current = ""
        else:
            current += c
    if current:
        words.append(current)
    return words


def refusal(commands: Sequence[Invocation]) -> str | None:
    """Why the wrapper itself refuses these commands, or None."""
    for each in commands:
        for flag, _ in each.options:
            policy = _OPTIONS[flag][1]
            if policy == "login":
                return LOGGED_OUT_MESSAGE.format(flag)
            if policy == "restart":
                return RESTART_MESSAGE.format(flag)
        if each.name in _LOGIN_COMMANDS:
            return LOGGED_OUT_MESSAGE.format(each.name)
        if each.sub in _LOGIN_SUBCOMMANDS.get(each.name, ()):
            return LOGGED_OUT_MESSAGE.format(f"{each.name} {each.sub}")
        if each.name == "open" and not each.sub:
            # With no address the engine launches the browser again, over the
            # one open.
            return RESTART_MESSAGE.format("`open` without an address")
    return None


def _runs_script(word: str) -> bool:
    return word in ("--fn", "-f") or word.startswith(("--fn=", "-f="))


def _use_of(each: Invocation) -> str:
    name, sub = each.name, each.sub
    if name == "batch":
        # Does nothing itself; the commands it runs are judged each.
        return "look"
    if not name and not each.unknown_lead:
        # No command at all: the engine prints its help or version and starts
        # no browser. An option no one here knows might be read as one.
        return "none"
    if name in _SCRIPT_COMMANDS or (name == "wait" and any(_runs_script(word) for word in each.given)):
        # Runs the page's script, which can open a tab with `window.open`, as
        # the engine itself suggests for one it tracks. `wait` waits by running
        # it: read off every word as given, since an option inside a batch,
        # which the engine does not take there, could otherwise seem to swallow it.
        return "grow"
    if name == "diff" and sub == "url":
        # Opens each page in the browser.
        return "act"
    if name in _LOOK_COMMANDS:
        # An option no one here knows could make the engine read another word
        # as the command, so a command read past one is never only a read.
        return "act" if name and each.unknown_lead else "look"
    if name in ("close", "quit", "exit"):
        # The engine's names for one command.
        return "close"
    if name == "tab":
        return {"": "look", "list": "look", "new": "grow"}.get(sub, "act")
    if name == "window":
        return "grow" if sub == "new" else "act"
    if name == "click" and "--new-tab" in each.args:
        # Opens the link in a tab of its own.
        return "grow"
    if name == "network" and sub == "requests":
        return "look"
    return "act"


def use(commands: Sequence[Invocation]) -> str:
    """What these commands do to the browser, by the most any of them does, in
    the order they run.

    A close followed by more opens another browser, ``reopen``. One that follows
    other commands, ``closes``, is judged as they are, since they run on, and may
    first open, the browser it closes, and still ends what the browser was
    launched with. ``none`` starts no browser at all.
    """
    if commands and _INFO_FLAGS.intersection(commands[0].given):
        return "none"
    each_use = [_use_of(each) for each in commands if each.name != "batch"]
    if "close" in each_use:
        first = each_use.index("close")
        if set(each_use[first:]) - {"close", "none"}:
            return "reopen"
        return "closes" if set(each_use[:first]) - {"none"} else "close"
    uses = set(each_use) - {"none"}
    if not uses:
        return "none" if each_use else "look"
    return next((each for each in ("grow", "act") if each in uses), "look")


def asked_user_agent(top: Invocation, default: str) -> str:
    """The user agent the command asks for: the last one it gives, else *default*."""
    for flag, value in top.options:
        if flag == "--user-agent" and value is not None:
            default = value
    return default


def _stdin_batch(top: Invocation) -> tuple[list[list[str]] | None, bytes | None]:
    """A ``batch`` given no commands reads them from stdin, which is read here
    first so they are checked like any others, then handed on unchanged."""
    if top.name != "batch" or any(arg not in _BATCH_OPTIONS for arg in top.args):
        return None, None
    data = sys.stdin.buffer.read()
    try:
        parsed = json.loads(data)
    except ValueError:
        return None, data
    if not isinstance(parsed, list):
        return None, data
    return [[str(word) for word in each] for each in parsed if isinstance(each, list)], data


def _restore_stdin(data: bytes) -> None:
    with tempfile.TemporaryFile() as f:
        f.write(data)
        f.seek(0)
        os.dup2(f.fileno(), 0)


def _gate(use: str, user_agent: str) -> Launch:
    """The launch the gate settles. Only its own refusal stops a command: a
    bridge that cannot run costs the view, not the browser."""
    try:
        from . import gate
    except Exception:
        traceback.print_exc()
        return _unsettled()
    try:
        return gate.check(use, user_agent)
    except gate.Refused as refused:
        print(refused, file=sys.stderr)
        sys.exit(GATE_REFUSED)
    except Exception:
        traceback.print_exc()
        return _unsettled()


def _unsettled() -> Launch:
    # The launch last recorded, which an open browser started with, else the
    # engine's own user agent at the standard quality.
    return launch.current() or Launch("", "standard")


def _launch_env(quality: StreamQuality) -> dict[str, str]:
    """What the engine starts the browser with for *quality*, and so must get on
    every command while that browser is open."""
    args = ["--proxy-bypass-list=<-loopback>", f"--window-size={VIEWPORT[0]},{VIEWPORT[1] + _UNDRAWN_TOOLBAR_PX}"]
    if quality.scale != 1:
        # Rendered at this scale, the screencast's frames are too. Emulating it
        # for the page instead leaves the frames at one pixel a page pixel.
        args.append(f"--force-device-scale-factor={quality.scale}")
    return {
        # One argument a line: the window size holds a comma.
        "AGENT_BROWSER_ARGS": "\n".join(args),
        "AGENT_BROWSER_STREAM_QUALITY": str(quality.jpeg),
        "AGENT_BROWSER_STREAM_MAX_WIDTH": str(quality.width),
        "AGENT_BROWSER_STREAM_MAX_HEIGHT": str(quality.height),
    }


def engine_env(base: Mapping[str, str], launched: Launch) -> dict[str, str]:
    """What the engine runs with here: *base* without what would launch the
    browser another way, and *launched*, so a command never restarts the
    browser that launch opened."""
    env = dict(base)
    for name in _UNSET:
        env.pop(name, None)
    env.update(_ENGINE_ENV)
    env["AGENT_BROWSER_SOCKET_DIR"] = _daemon_dir()
    env.update(_launch_env(launched.stream))
    user_agent = launched.user_agent.rstrip("\n")
    if user_agent:
        env["AGENT_BROWSER_USER_AGENT"] = user_agent
    return env


def _daemon_dir() -> str:
    """Where the engine finds its daemon, the same for every command: another
    folder would mean a second daemon and Chrome that the gate, the hold and
    the viewer never see. The engine's own default, from the account rather
    than HOME, which a command can set; its last resort with no account."""
    try:
        return os.path.join(pwd.getpwuid(os.getuid()).pw_dir, ".agent-browser")
    except KeyError:
        return os.path.join(tempfile.gettempdir(), "agent-browser")


def _can_drop_nice() -> bool:
    try:
        return subprocess.run([*_DROP_NICE, "true"], stderr=subprocess.DEVNULL).returncode == 0
    except OSError:
        return False


def main(args: list[str]) -> int:
    top = parse(args)
    stdin, data = _stdin_batch(top)
    commands = expand(top, stdin)
    refused = refusal(commands)
    if refused:
        print(refused, file=sys.stderr)
        return REFUSED
    if not os.access(ENGINE_BIN, os.X_OK):
        print(NOT_INSTALLED_MESSAGE, file=sys.stderr)
        return NOT_INSTALLED
    user_agent = asked_user_agent(top, os.environ.get("AGENT_BROWSER_USER_AGENT", ""))
    used = use(commands)
    # Help reaches no browser, so neither memory nor the person's hold stops it.
    env = engine_env(os.environ, _unsettled() if used == "none" else _gate(used, user_agent))
    # The browser, and nothing it is started beside, goes before the person's own
    # work if the computer runs out of memory: the next command brings a browser
    # back, and analysis cannot be. The bridge the gate started keeps its own.
    try:
        _OOM_SCORE.write_text("1000")
    except OSError:
        pass
    if data is not None:
        _restore_stdin(data)
    argv = [ENGINE_BIN, *args]
    if os.getuid() == 0 and _can_drop_nice():
        os.execvpe(_DROP_NICE[0], [*_DROP_NICE, *argv], env)
    os.execve(ENGINE_BIN, argv, env)
