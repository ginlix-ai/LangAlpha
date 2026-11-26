"""``agent-browser`` on a computer, ahead of the real CLI on every command's PATH.

It gives the browser the one way it runs here, streaming at a rate a viewer can
afford and on the port the view watches, with every request through the proxy
that keeps it off the machine's own services, and keeps the bridge in front of
that stream up. The arguments are read once, as the engine reads them, and every
check judges that one reading, so an option placed before the command, or a
value that looks like one, is no way around it. The agent never brings a login
into this browser: one is kept only on the computer's own profile (``signin``),
from a person signing in in the live view, so what would save or bring in a
login is refused, and so is what would start a browser the view cannot see.
What would print the sign-ins a kept profile holds is refused as well, and the
headers that carry them are hidden in what the network commands print, since
the agent's output reaches the model. A launch option given on one command and not the
next restarts the browser and loses its pages, so those are refused too, except
a user agent, which sites may need and which is held from the first command that
sets it until the browser closes. While the person holds the page, only what
reads it runs.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import traceback
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from . import host, launch, signin
from .engine import for_launch
from .launch import Launch
from .protocol import (
    GATE_REFUSED,
    LOGGED_OUT_MESSAGE,
    NOT_INSTALLED,
    NO_CHROME_MESSAGE,
    NOT_INSTALLED_MESSAGE,
    REFUSED,
    RESTART_MESSAGE,
    SETTING_UP_MESSAGE,
    SIGNINS_MESSAGE,
    installing,
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
#: Headers whose values are sign-ins, hidden in what the network commands print.
_SIGNIN_HEADERS = frozenset({"cookie", "set-cookie", "authorization", "proxy-authorization"})
_HIDDEN = "[hidden]"
_LOOK_COMMANDS = frozenset(
    {"", "skills", "snapshot", "read", "get", "is", "wait", "screenshot", "pdf", "console", "errors", "diff"}
)
_BATCH_OPTIONS = frozenset({"--bail", "--omit-command"})
# What runs script the agent wrote in the page.
_SCRIPT_COMMANDS = frozenset({"eval", "addinitscript"})
# The engine's names for ``open``.
_OPEN_COMMANDS = frozenset({"open", "goto", "navigate"})
# Any of these among a command's words, the engine prints help or its version
# and returns before it reaches a daemon.
_INFO_FLAGS = frozenset({"--help", "-h", "--version", "-V"})
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
    as the engine reads it, in the order they run. Only *top* reads its commands
    from *stdin*."""
    found: list[Invocation] = []

    def walk(each: Invocation, reads_stdin: bool) -> None:
        found.append(each)
        if each.name != "batch":
            return
        given = [arg for arg in each.args if arg not in _BATCH_OPTIONS]
        if given:
            inner = [parse(_shell_words(arg)) for arg in given]
        else:
            inner = [parse(words) for words in stdin] if reads_stdin and stdin else []
        for command in inner:
            walk(command, False)

    walk(top, True)
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
    """Why the wrapper refuses these commands for their arguments alone, or None."""
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
        if each.name in _OPEN_COMMANDS and not each.sub:
            # With no address the engine launches the browser again, over the
            # one open.
            return RESTART_MESSAGE.format(f"`{each.name}` without an address")
    return None


#: What leaves a page's cookies and storage unread: setting or clearing them.
#: The engine reads them on any other form, a shorthand ``storage local <key>``
#: or a verb it does not know included, so a read is anything but these, at the
#: place the engine reads the verb from.
_COOKIE_WRITES = frozenset({"set", "clear"})
_STORAGE_WRITES = frozenset({"set", "clear"})
#: What a command would do with the sign-ins a kept browser holds: print them,
#: so it is refused, or print the headers that carry them, which are hidden.
_Shown = Literal["signins", "headers"]


def _shows(each: Invocation) -> tuple[_Shown, str] | None:
    """What *each* would print of the browser's sign-ins, with the command named
    as the agent wrote it, or None. Its cookies, its pages' storage, a HAR,
    which records both, and the DevTools address that reaches all of them are
    sign-ins; a request's headers carry them. Setting or clearing them prints
    nothing."""
    if each.name == "cookies" and not _writes(each, 0, _COOKIE_WRITES):
        return "signins", "`cookies`"
    if each.name == "storage" and not _writes(each, 1, _STORAGE_WRITES):
        return "signins", "`storage` read"
    if each.name == "network" and each.sub == "har":
        return "signins", "`network har`"
    if each.name == "get" and each.sub == "cdp-url":
        return "signins", "`get cdp-url`"
    if each.name == "network" and each.sub in ("requests", "request"):
        return "headers", f"`network {each.sub}`"
    return None


def _writes(each: Invocation, at: int, writes: frozenset[str]) -> bool:
    """Whether the engine reads the argument at *at* after *each*'s word as one
    of *writes*. It keeps an option it does not know among those arguments, so
    one there takes the verb's place, and an ``--option=value`` too, which
    ``args`` leaves out, so with one given the place is not known here."""
    if any(_OPTIONS.get(arg.partition("=")[0], ("flag",))[0] != "flag" for arg in each.given if "=" in arg):
        return False
    return len(each.args) > at and each.args[at] in writes


def signins_shown(commands: Sequence[Invocation]) -> tuple[str | None, bool]:
    """For a browser that keeps sign-ins: why these commands are refused, or
    None, and whether what they print must have the sign-ins' headers hidden."""
    shown = [found for found in map(_shows, commands) if found]
    printed = next((named for what, named in shown if what == "signins"), None)
    return (SIGNINS_MESSAGE.format(printed) if printed else None), any(what == "headers" for what, _ in shown)


def hide_signins(printed: str) -> str:
    """*printed* by the engine with the values of the headers that carry
    sign-ins hidden. Only its JSON carries headers: a document whole, or one
    per line for a batch; any other line passes as printed."""
    try:
        return json.dumps(_hidden(json.loads(printed)), ensure_ascii=False) + "\n"
    except ValueError:
        pass
    lines = []
    for line in printed.splitlines(keepends=True):
        try:
            lines.append(json.dumps(_hidden(json.loads(line)), ensure_ascii=False) + "\n")
        except ValueError:
            lines.append(line)
    return "".join(lines)


def _hidden(value: object) -> object:
    if isinstance(value, dict):
        # A HAR-style entry names its header and gives its value apart.
        if isinstance(value.get("name"), str) and value["name"].lower() in _SIGNIN_HEADERS and "value" in value:
            return {**value, "value": _HIDDEN}
        return {
            key: _HIDDEN if key.lower() in _SIGNIN_HEADERS else _hidden(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_hidden(item) for item in value]
    return value


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


def opens_page(commands: Sequence[Invocation]) -> bool:
    """Whether the first of these commands to reach the page goes to an address
    of its own, so a browser opened for them has no page to show again first."""
    for each in commands:
        if each.name == "batch" or _use_of(each) == "none":
            continue
        return each.name in _OPEN_COMMANDS and bool(each.sub)
    return False


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


def _gate(use: str, user_agent: str, opens_page: bool) -> tuple[Launch, Callable[[], None]]:
    """The launch the gate settles, and what lets the next command in. Only its
    own refusal stops a command: a bridge that cannot run costs the view, not
    the browser."""
    try:
        from . import gate
    except Exception:
        traceback.print_exc()
        return _unsettled(), _admitted_by_none
    try:
        return gate.check(use, user_agent, opens_page=opens_page)
    except gate.Refused as refused:
        print(refused, file=sys.stderr)
        sys.exit(GATE_REFUSED)
    except Exception:
        traceback.print_exc()
        return _unsettled(), _admitted_by_none


def _unsettled() -> Launch:
    # The launch last recorded, which an open browser started with, else the
    # engine's own user agent at the standard quality.
    return launch.current() or Launch("", "standard")


def _not_ready(here: host.Host) -> str:
    """Why there is no browser to run, as the agent tells the person: one being
    set up, a machine of the person's own with no Chrome-family browser, or
    neither, which the Browser tab sets up or explains."""
    if installing():
        return SETTING_UP_MESSAGE
    if here.own and os.access(here.engine, os.X_OK):
        return NO_CHROME_MESSAGE
    return NOT_INSTALLED_MESSAGE


def _admitted_by_none() -> None:
    """The release of a command no gate admitted, which holds nothing."""


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
    here = host.current()
    if not here.ready():
        print(_not_ready(here), file=sys.stderr)
        return NOT_INSTALLED
    # Refused before the gate wherever the browser open or the next keeps
    # sign-ins, so a refused command changes nothing the gate would record.
    opened = launch.current()
    if signin.kept() or (opened and opened.kept):
        refused, _ = signins_shown(commands)
        if refused:
            print(refused, file=sys.stderr)
            return REFUSED
    user_agent = asked_user_agent(top, os.environ.get("AGENT_BROWSER_USER_AGENT", ""))
    used = use(commands)
    # Help reaches no browser, so neither memory nor the person's hold stops it.
    launched, release = (_unsettled(), _admitted_by_none) if used == "none" else _gate(used, user_agent, opens_page(commands))
    # Read again once admitted: a setup holds the same admission while it
    # records another engine, Chrome or ports, which this command must run on.
    here = host.current()
    if not here.ready():
        release()
        print(_not_ready(here), file=sys.stderr)
        return NOT_INSTALLED
    # Judged again on the browser the command will run on, as settled, and on
    # the choice: either can change between the reading above and the gate.
    hidden = False
    if launched.kept or signin.kept():
        refused, hidden = signins_shown(commands)
        if refused:
            release()
            print(refused, file=sys.stderr)
            return REFUSED
    env = launched.engine_env(os.environ, here)
    # The browser, and nothing it is started beside, goes before the person's own
    # work if the computer runs out of memory: the next command brings a browser
    # back, and analysis cannot be. The bridge the gate started keeps its own.
    try:
        _OOM_SCORE.write_text("1000")
    except OSError:
        pass
    if data is not None:
        _restore_stdin(data)
    # The version the open browser started on, so an update that landed since
    # reaches it only when it next starts.
    argv = [for_launch(launched, here), *args]
    if os.getuid() == 0 and _can_drop_nice():
        argv = [*_DROP_NICE, *argv]
    if hidden:
        return _run_hiding_signins(argv, env, release)
    os.execvpe(argv[0], argv, env)
    return 0


def _run_hiding_signins(argv: list[str], env: Mapping[str, str], release: Callable[[], None]) -> int:
    """Run the engine beside the wrapper, which passes on what it prints with
    the sign-ins hidden. The next command is let in once the engine's has
    started, as when the wrapper becomes it, since its command counts as busy."""
    engine = subprocess.Popen(argv, env=dict(env), stdout=subprocess.PIPE)
    release()
    printed, _ = engine.communicate()
    sys.stdout.write(hide_signins(printed.decode(errors="replace")))
    sys.stdout.flush()
    return engine.returncode
