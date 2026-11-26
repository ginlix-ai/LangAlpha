"""Claude Code on a machine: the user's own computer over its SSH connection,
or a cloud computer's sandbox (``sandbox_launcher``), both as a ``ShellLink``.

On the user's own computer the CLI runs as the SSH login, under a supervisor
(``detach``) started on one exec channel of the computer's pooled connection
(``SSHRuntime.link``), so a turn opens no login of its own. The channel relays
the SDK's stream-json to and from the CLI, which keeps the tools, the hooks
and the session mirror in the backend worker, as for ``LocalLauncher``; a
connection lost mid-turn leaves the CLI running for a new channel to attach. Each turn runs in a private config dir under the
computer root's internal dir, laid out over SFTP from the checkpoint and
removed after the turn; a dir a lost connection left behind is swept by a
later turn.

The CLI signs in with a setup token made for LangAlpha, never with the
machine's own Claude Code login (``launcher.py`` says why). The token is the
computer's own file on the machine (``machine_token``), which the backend
never reads.
Where the deployment allows it, a machine without one is relayed a
credential (``TokenStore``) on the channel's stdin, read only once the
machine has said it has none: the user's stored token, or on a cloud
computer (``sandbox_launcher``) the credential its turns run on. Either way
a perl shim on the machine puts it in a socket pair and execs the CLI with
the descriptor variable of its kind naming it: it never reaches an argv, an
environment or a file.
"""

from __future__ import annotations

import asyncio
import codecs
import contextlib
import json
import logging
import posixpath
import re
import unicodedata
from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import KW_ONLY, dataclass, field, replace
from typing import Any, Literal, cast

from claude_agent_sdk import ClaudeAgentOptions, CLIConnectionError, ProcessError
from claude_agent_sdk._errors import CLIJSONDecodeError
from claude_agent_sdk._internal.sessions import _sanitize_path
from claude_agent_sdk._internal.transport import Transport
from claude_agent_sdk._internal.transport.subprocess_cli import (
    _DEFAULT_MAX_BUFFER_SIZE,
    SubprocessCLITransport,
    _LineFramer,
    _parse_stdout_line,
)

from ptc_agent.agent.claude_code import cli_install, detach
from ptc_agent.agent.claude_code.launcher import (
    _INHERITED,
    _THREAD_KEY,
    STDERR_KEPT,
    Prepared,
    TokenStore,
    _no_prompt,
    cli_env,
    device_id,
    exit_error,
    session_jsonl,
    stderr_text,
)
from ptc_agent.agent.claude_code.machine_token import (
    FIND_PERL,
    NO_PERL,
    STREAM_BOOT,
    TOKEN_DIR_SH,
    TOKEN_THERE_SH,
    checked_name,
    shim,
    word,
)
from ptc_agent.core.claude_code_managed import claude_code_managed_sh, parse_managed
from ptc_agent.core.paths import SandboxLayout
from ptc_agent.core.sandbox.providers._ssh_shell import check_env
from ptc_agent.agent.claude_code.plan import PlanLog, SignedIn
from ptc_agent.agent.claude_code.session import CheckpointSessionStore, Reach
from ptc_agent.agent.claude_code.shell_link import ShellLink, ShellProcess
from ptc_agent.core.claude_code_clis import CLIS_MAX
from ptc_agent.core.sandbox.providers._ssh_transport import TRANSPORT_ERRORS, CliHold, Link, Machine, SlotWaitTimeout
from ptc_agent.core.sandbox.providers.ssh import IS_MUSL_SH, MUSL_UNSUPPORTED
from src.llms.claude_code import COMPUTER_LOST, ClaudeTurnFailed, is_token_text

logger = logging.getLogger(__name__)

#: Where a run's token came from: the machine's file, the relay, or nowhere.
TokenOrigin = Literal["file", "relay", "none"]
#: How long the machine may take to reach the CLI's exec, and to end the CLI
#: once its input is closed, and again per signal.
START_TIMEOUT_S = 60
EXIT_GRACE_S = 5
#: How long a turn's CLI waits for one of the link's CLI slots, apart from
#: its start: as long as another turn there may take to finish.
CLI_SLOT_WAIT_S = 600
#: How long a subagent's CLI waits for one: its Task was admitted under the
#: computer's count of its CLIs (``hosted.cli_cap``), so a wait means a
#: count that raced or another worker's link, and the run fails past it.
SUBAGENT_CLI_SLOT_WAIT_S = 30
#: A turn's config dir left this long is one a lost connection orphaned.
_STALE_MINUTES = 2 * 24 * 60
_SCRIPT_S = 30
_PID_MARK = detach.MARK + "pid "
_ANNOUNCE = re.compile(rb"langalpha-token (file|relay|none)")
_NOISE_MAX = 64 * 1024
#: How long a run whose connection dropped waits for its machine to answer
#: again: a machine whose tunnel drops tries again within 2.5 to 7.5
#: seconds, then 5 to 15 more, so a gateway restart that misses its first
#: try still lands inside the wait. A turn's first dial waits as long.
RECONNECT_WAIT_S = 30
#: How long the CLI runs on with no channel attached before its supervisor
#: ends it: past the 45 seconds a dead connection's keepalive may take to be
#: noticed here plus the reattach's wait, so only a run whose worker is gone
#: or that gave up leaves it.
DETACHED_GRACE_S = 120


def slots_full(e: SlotWaitTimeout) -> ClaudeTurnFailed:
    """A CLI that found no slot in its wait, refused as a turn its computer
    is too busy for: the user's to wait out or raise the count for, not this
    service's failure. A wait for one of the computer's own CLIs names it,
    for the setting that runs more at once."""
    if e.which == "command":
        return ClaudeTurnFailed("commands_full", str(e))
    where = {} if e.computer_id is None else {"computer_id": e.computer_id, "settable": e.settable, "max": CLIS_MAX}
    return ClaudeTurnFailed("clis_full", str(e), n=e.n, **where)


def kill_if_cli(pid: int, cli: str, sig: str) -> str:
    """A script sending ``sig`` to ``pid`` only while that pid still runs ``cli``,
    so a pid reused since is left alone. Reads /proc where there is one
    (BusyBox ps takes no -p), else ps (macOS)."""
    return (
        f"if [ -r /proc/{pid}/cmdline ]; then a=$(tr '\\0' ' ' < /proc/{pid}/cmdline); "
        f"else a=$(ps -ww -o args= -p {pid} 2>/dev/null); fi; "
        f'case "$a" in {word(cli)}|{word(cli + " ")}*) kill -{sig} {pid} 2>/dev/null;; esac; true'
    )


def remote_session_path(config_dir: str, cwd: str, session_id: str) -> str:
    """Where the CLI on the machine looks for a session's transcript.

    ``cwd`` is its real path there; the SDK's own helper would resolve it
    against this worker's filesystem instead.
    """
    key = _sanitize_path(unicodedata.normalize("NFC", cwd))
    return posixpath.join(config_dir, "projects", key, f"{session_id}.jsonl")


@dataclass
class SSHLauncher:
    """Runs claude on a machine (``ShellLink``), in a private config dir per turn there.

    ``machine`` reaches the computer: its pooled connection, dialed again
    after one is lost, and its root, read once the connection is resolved,
    since a sandbox's root is final only after the setup resolving it waits
    for. ``identity`` is who the turns run for, the device id's key. The CLI
    is the pinned one, installed on the machine as a run first needs it;
    ``install`` False only looks for it, so a check never starts a download.
    ``token_file`` names the token file the shim reads in the tokens' folder:
    the computer's own, or a staged one a check is about; None reads none,
    so the run's credential is the relayed one or nothing. ``slot_wait_s`` is
    how long the CLI waits for a free CLI slot on the computer's connection;
    a check passes a short one. ``cli_slot(wait_s)`` holds one of the CLIs
    the computer runs at once, from the run's layout to its release.

    ``credential``, given only where the deployment relays one, is read only
    once the machine has said it has no token of its own, and told if
    Anthropic refuses what it gave. ``on_announced(origin)`` is told where
    the run's credential comes from as the shim announces it, and
    ``on_file_refused()`` that Anthropic refused the machine's own token
    file. ``on_blocked(present)`` is told that an organization's
    policy is installed on the computer, before the run is refused,
    ``on_no_perl(present)`` that the computer has no perl, and
    ``on_unsupported(present)`` that it runs musl, each with whether
    anything is at ``token_file``'s path there. ``source`` is where the last
    run's credential came from. One launcher serves one run at a time. ``env`` is for a test endpoint.
    ``dial_wait_s`` is how long a run's first dial waits for a machine that
    does not answer; a check passes 0. ``resumes`` stops a turn whose machine
    does not answer again within ``RECONNECT_WAIT_S`` to be resumed
    (``COMPUTER_LOST``), for a computer whose return the backend sees; else
    it fails as a lost connection.
    ``plan`` keeps what the CLI reports of the Claude plan behind that
    credential: the token file, given only where it is the computer's own,
    or a relayed setup token, the saved one.
    """

    machine: Machine[ShellLink]
    _: KW_ONLY
    identity: str
    token_file: str | None
    install: bool = True
    slot_wait_s: float = CLI_SLOT_WAIT_S
    cli_slot: Callable[[float], contextlib.AbstractAsyncContextManager[None]] | None = None
    credential: TokenStore | None = None
    on_announced: Callable[[TokenOrigin], Awaitable[None]] | None = None
    on_file_refused: Callable[[], Awaitable[None]] | None = None
    on_blocked: Callable[[bool], Awaitable[None]] | None = None
    on_no_perl: Callable[[bool], Awaitable[None]] | None = None
    on_unsupported: Callable[[bool], Awaitable[None]] | None = None
    env: dict[str, str] = field(default_factory=dict, repr=False)
    plan: PlanLog | None = None
    dial_wait_s: float = RECONNECT_WAIT_S
    resumes: bool = False
    source: TokenOrigin | None = field(default=None, init=False)
    #: Whether the last run was relayed the saved setup token.
    relayed: bool = field(default=False, init=False)
    _link: ShellLink | None = field(default=None, init=False, repr=False)
    _cli: str | None = field(default=None, init=False)
    _root: str | None = field(default=None, init=False)
    #: The run's hold on one of the computer's CLIs, from its layout to its release.
    _slot: contextlib.AsyncExitStack | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.token_file is not None:
            checked_name(self.token_file)

    @property
    def signed_in(self) -> SignedIn | None:
        if self.source == "file":
            return "machine"
        return "saved" if self.source == "relay" and self.relayed else None

    @property
    def base(self) -> str:
        if self._root is None:
            raise RuntimeError("the computer root is read once the connection is resolved")
        return SandboxLayout.for_root(self._root).claude_code

    async def ready(self) -> ShellLink:
        """The computer's connection, with the CLI ready there: what a run,
        and a change of the computer's token, needs before it starts.

        The first call reads the root and refuses a machine where the CLI
        could not start (``_refuse_unless_runnable``) before anything is
        installed there.
        """
        self._link = await detach.link_within(self.machine, self.dial_wait_s)
        if self._root is None:
            root = self.machine.root()
            if not root.startswith("/"):
                raise ValueError(f"the computer root {root!r} is not an absolute path")
            self._root = root
        if self._cli is None:
            await self._refuse_unless_runnable(self._link)
            try:
                self._cli = await cli_install.ensure_cli(self._link, self._root, install=self.install)
            except cli_install.CliUnsupported as e:
                raise ClaudeTurnFailed("computer_unsupported", str(e)) from e
        return self._link

    async def _refuse_unless_runnable(self, link: ShellLink) -> None:
        """Refuse the machine where an organization's managed Claude Code
        sources are, telling ``on_blocked``, where it runs musl, which no
        build of the CLI runs on, telling ``on_unsupported``, or where it
        has no perl for the shim, telling ``on_no_perl``; each is told
        whether the token file is there, as the status check would say, and
        the machine is refused as admission refuses it. One round trip, read
        by name only."""
        found = await link.run(
            f'{TOKEN_DIR_SH}{IS_MUSL_SH}{claude_code_managed_sh()}printf "%s\\n" "${{cm# }}"; '
            f'f={self._token_path}; {TOKEN_THERE_SH}'
            'if [ "$(uname -s)" = Linux ] && is_musl; then echo musl; '
            'elif command -v perl >/dev/null; then echo perl; else echo none; fi',
            timeout=_SCRIPT_S,
        )
        managed, _, rest = found.stdout.decode(errors="replace").partition("\n")
        present, _, runs = rest.partition("\n")
        runs = runs.strip()
        if found.exit_code != 0 or present not in ("0", "1") or runs not in ("perl", "none", "musl"):
            detail = found.stderr.decode(errors="replace").strip() or f"exit {found.exit_code}"
            raise RuntimeError(f"Could not read Claude Code's managed settings on {link.host}: {detail}")
        sources = parse_managed(managed)
        if not sources and runs == "perl":
            return
        told = self.on_blocked if sources else self.on_unsupported if runs == "musl" else self.on_no_perl
        if told is not None:
            try:
                await told(present == "1")
            except Exception as e:  # noqa: BLE001 - what the backend keeps of it never changes the refusal
                logger.warning("[ClaudeCode] could not record why Claude Code cannot run on a computer: %s", type(e).__name__)
        if sources:
            raise ClaudeTurnFailed("policy_blocked", f"the computer holds managed Claude Code settings {list(sources)}")
        if runs == "musl":
            raise ClaudeTurnFailed("computer_unsupported", MUSL_UNSUPPORTED)
        raise ClaudeTurnFailed("needs_perl", f"Claude Code did not start on {link.host}: {NO_PERL}")

    async def _lay_out(self, link: ShellLink, thread_key: str | None) -> Prepared:
        """A fresh config dir and, for a thread, its working dir, which is its real path there.

        Sweeps the config dirs a lost connection left behind on the way.
        """
        script = (
            f'umask 077; b={word(self.base)}; mkdir -p "$b/config" "$b/threads" || exit 1; '
            'for d in "$b" "$b/config" "$b/threads"; do '
            'if [ -L "$d" ] || [ ! -d "$d" ] || [ ! -O "$d" ]; then echo "$d is not a folder this login owns" >&2; exit 3; fi; '
            'chmod 700 "$d"; done; '
            f'find "$b/config" -mindepth 1 -maxdepth 1 -type d -mmin +{_STALE_MINUTES} -exec rm -rf {{}} + 2>/dev/null; '
            'c=$(mktemp -d "$b/config/XXXXXXXX") || exit 1; w="$c"; '
        )
        if thread_key is not None:
            script += (
                f'w="$b/threads/{thread_key}"; mkdir -p "$w"; '
                'if [ -L "$w" ] || [ ! -d "$w" ] || [ ! -O "$w" ]; then rm -rf "$c"; '
                'echo "$w is not a folder this login owns" >&2; exit 3; fi; '
            )
        script += 'printf "%s\\n" "$c" "$(cd "$w" && pwd -P)"'
        result = await link.run(script, timeout=_SCRIPT_S)
        lines = result.stdout.decode(errors="replace").splitlines()
        if result.exit_code != 0 or len(lines) != 2 or not lines[0].startswith(self.base + "/config/"):
            detail = result.stderr.decode(errors="replace").strip() or f"exit {result.exit_code}"
            raise RuntimeError(f"Could not lay out a Claude Code turn on {link.host}: {detail}")
        return Prepared(config_dir=lines[0], cwd=lines[1])

    async def _write(self, link: ShellLink, prepared: Prepared, files: list[tuple[str, str]]) -> None:
        """Write the turn's files, or remove the turn's dir."""
        try:
            await link.write_files([(path, text.encode()) for path, text in files])
        except BaseException:
            await asyncio.shield(self.release(prepared))
            raise

    async def prepare(
        self, thread_key: str, session_id: str | None, entries: list[dict[str, Any]], *, identity: str,
        system_prompt: str,
    ) -> Prepared:
        """Lay out a turn: a fresh config dir holding the device id, the system prompt and the session file.

        The working dir is the thread's own under ``threads/``, the same path
        on every turn, so the session it keys never moves.
        """
        if not _THREAD_KEY.fullmatch(thread_key):
            raise ValueError(f"thread key {thread_key!r} is not a plain id")
        async with self._holding():
            link = await self.ready()
            device = device_id(identity)
            prepared = await self._lay_out(link, thread_key)
            files = [
                (posixpath.join(prepared.config_dir, ".claude.json"), json.dumps({"userID": device})),
                (prepared.system_prompt_file, system_prompt),
            ]
            if session_id:
                files.append((remote_session_path(prepared.config_dir, prepared.cwd, session_id), session_jsonl(entries)))
            await self._write(link, prepared, files)
            return prepared

    async def scratch(self) -> Prepared:
        async with self._holding():
            link = await self.ready()
            device = device_id(self.identity)
            prepared = await self._lay_out(link, None)
            await self._write(link, prepared, [(posixpath.join(prepared.config_dir, ".claude.json"), json.dumps({"userID": device}))])
            return prepared

    @contextlib.asynccontextmanager
    async def _holding(self) -> AsyncIterator[None]:
        """Take one of the computer's CLIs (``cli_slot``) before anything
        runs on the machine, and keep it for ``release`` unless the layout
        fails. Taken first, a turn on a full computer waits where its wait
        is told, rather than first queueing its setup scripts for channels
        that the running CLIs and their commands hold."""
        if self.cli_slot is not None and self._slot is None:
            slot = contextlib.AsyncExitStack()
            try:
                await slot.enter_async_context(self.cli_slot(self.slot_wait_s))
            except SlotWaitTimeout as e:
                raise slots_full(e) from None
            self._slot = slot
        try:
            yield
        except BaseException:
            await asyncio.shield(self._free_slot())
            raise

    async def _free_slot(self) -> None:
        slot, self._slot = self._slot, None
        if slot is not None:
            await slot.aclose()

    async def release(self, prepared: Prepared) -> None:
        """Give back the run's CLI and remove the turn's config dir; one that
        cannot be reached now is swept by a later turn."""
        await asyncio.shield(self._free_slot())
        if not prepared.config_dir.startswith(self.base + "/config/"):
            raise ValueError(f"{prepared.config_dir} is not a turn's config dir")
        try:
            link = self._link
            if link is None or not link.usable:
                link = self._link = await self.machine.link()
            result = await link.run(f"rm -rf -- {word(prepared.config_dir)}", timeout=_SCRIPT_S)
            if result.exit_code != 0:
                raise RuntimeError(result.stderr.decode(errors="replace").strip() or f"exit {result.exit_code}")
        except Exception as e:  # noqa: BLE001 - the turn is over either way, and the sweep takes what stays
            logger.warning("[ClaudeCode] could not remove a turn's config dir on the computer: %s", e)

    def transport(self, options: ClaudeAgentOptions) -> Transport:
        if self._link is None or self._cli is None:
            raise RuntimeError("prepare the launcher before starting its CLI")
        return SSHTransport(self, self._link, replace(options, cli_path=self._cli))

    async def refused(self) -> None:
        if self.source == "relay" and self.credential is not None:
            await self.credential.refused()
        elif self.source == "file":
            if self.on_file_refused is not None:
                await self.on_file_refused()
            raise ClaudeTurnFailed("token_refused", "Claude refused the computer's own setup token")

    @property
    def _token_path(self) -> str:
        """The token file's path as a word of the scripts, which set ``$d`` to the tokens' folder."""
        return f'"$d/{self.token_file}"' if self.token_file is not None else "''"

    def command(self, options: ClaudeAgentOptions) -> str:
        """The script the first channel runs: the CLI under the shim, in a clean
        environment, started by its supervisor (``detach.pl``), which keeps its
        output in the turn's config dir, and attached from its start.

        The environment is the allowlist, taken from the machine's own as sshd
        set it, plus the runtime's variables; nothing of the worker's.
        """
        argv = SubprocessCLITransport(prompt=_no_prompt(), options=options)._build_command()
        env = check_env(cli_env(options, self.env))
        assigns = " ".join(word(f"{k}={v}") for k, v in env.items())
        cli = (
            f'env -i "${{a[@]}}" {assigns} "$p" -e {word(shim())} {1 if self.credential is not None else 0} {self._token_path} '
            + " ".join(map(word, argv))
        )
        return (
            f"umask 077; {FIND_PERL}{TOKEN_DIR_SH}cd {word(str(options.cwd))} || exit 1; "
            f'a=(); for k in {" ".join(_INHERITED)}; do [ "${{!k+x}}" ] && a+=("$k=${{!k}}"); done; '
            + detach.start_command(str(options.env["CLAUDE_CONFIG_DIR"]), DETACHED_GRACE_S, cli)
        )

    async def started(self, source: TokenOrigin, relayed: bool = False) -> None:
        """The run's CLI started on a token from ``source``, as the shim
        announced it, ``relayed`` the saved one where it was relayed one."""
        self.source, self.relayed = source, relayed
        if self.on_announced is None:
            return
        try:
            await self.on_announced(source)
        except Exception as e:  # noqa: BLE001 - what the backend keeps of it never stops the turn
            logger.warning("[ClaudeCode] could not record where a turn's token came from: %s", type(e).__name__)


class SSHTransport(Transport):
    """The CLI as a process on the machine, relayed over one channel of its link at a time.

    The command line is the SDK's own (``_build_command``) under the shim,
    started by a supervisor on the machine (``detach``) that holds the CLI
    apart from the channel. A connection lost mid-turn leaves the CLI running;
    the transport dials the machine again within ``RECONNECT_WAIT_S``,
    attaches a new channel at the byte of stdout and the line of stderr it
    had read, and sends again the input the supervisor had not taken, so
    neither the SDK nor the CLI sees the gap. The CLI has ended only once a
    channel says so in its last line; one that closes without it lost its
    connection. A machine that does not answer again, or no longer runs the
    CLI, ends the read as the launcher says (``resumes``), and the supervisor
    ends the CLI once no channel has been attached for ``DETACHED_GRACE_S``.
    While it reattaches the run's waits for what the CLI reports wait with it
    (``Reach``), and a wait that runs out with the channel up first asks
    the machine whether it still answers, since a connection gone silent is
    noticed only by its keepalive. stderr is kept for diagnostics and handed
    to ``options.stderr``. Closing ends the CLI: its input is ended, then it
    is signalled by pid until it goes, attached or not, then the channel is
    closed.

    The CLI runs on the computer's CLI its launcher took for the run, and
    each channel holds a CLI slot on one of the login's connections, where
    it is signalled, and keeps the link in use from its start to the end of
    the transport's shutdown, so commands always find a slot and a link shed
    from the pool closes only once the turn is over.
    """

    def __init__(self, launcher: SSHLauncher, link: ShellLink, options: ClaudeAgentOptions) -> None:
        self._launcher = launcher
        self._link = link
        self._cli_link = link
        self._options = options
        self._dir = str(options.env["CLAUDE_CONFIG_DIR"])
        store = options.session_store
        self._reach = store.reach if isinstance(store, CheckpointSessionStore) else Reach()
        self._process: ShellProcess | None = None
        self._ready = False
        #: Whether the channel in ``_process`` is attached and taking input.
        self._live = False
        #: Whether the transport is closing, which no lost connection is reattached for.
        self._ending = False
        #: How the CLI ended, as a channel said (``detach.EXIT_MARK``); None while it runs.
        self._exit: str | None = None
        #: Whether the machine said it no longer runs the CLI, whose pid is then nobody's to signal.
        self._gone = False
        #: The send of the input a reattach queued, which some channels make only on a drain.
        self._resending: asyncio.Task[None] | None = None
        self._pid: int | None = None
        self._stderr: deque[str] = deque(maxlen=STDERR_KEPT)
        self._stderr_task: asyncio.Task[None] | None = None
        self._closing: asyncio.Task[None] | None = None
        self._holds = contextlib.AsyncExitStack()
        self._write_lock = asyncio.Lock()
        self._max_buffer_size = options.max_buffer_size or _DEFAULT_MAX_BUFFER_SIZE
        #: The bytes of the CLI's stdout read so far, and the whole lines of its stderr.
        self._out_at = 0
        self._err_lines = 0
        #: Whether the channel's stderr is the CLI's yet, past what its login printed.
        self._err_attached = False
        self._input = detach.InputLedger()

    async def connect(self) -> None:
        """Start the CLI and settle its credential: a relayed one is read and
        sent only when the machine asks for it. A refusal reading it ends the
        start as it is (``ClaudeTurnFailed``)."""
        if self._process is not None:
            return
        script = self._launcher.command(self._options)
        try:
            hold = await self._holds.enter_async_context(self._link.cli_hold(self._launcher.slot_wait_s))
        except BaseException as e:
            await self.close()
            if isinstance(e, SlotWaitTimeout):
                raise slots_full(e) from None
            raise
        self._cli_link = hold.link
        relayed = False
        try:
            async with asyncio.timeout(START_TIMEOUT_S):
                if self._launcher.plan is not None:
                    await self._launcher.plan.launching()
                await self._start(hold, script)
                if await self._attached() != 0:
                    raise detach.Broken("the CLI's supervisor had taken input before any was sent")
                self._live = True
                how = await self._announced()
                if how == "relay":
                    store = self._launcher.credential
                    credential = await store.read() if store is not None else None
                    if credential is not None and not is_token_text(credential.secret):
                        raise CLIConnectionError("The stored credential is not one line of text, so it was not sent")
                    line = credential.secret if credential is not None else ""
                    await self._send(line + "\n", fail=True)
                    relayed = credential is not None
                    if relayed and self._launcher.plan is not None:
                        await self._launcher.plan.took(credential.secret)
        except BaseException as e:
            await self.close()
            if isinstance(e, CLIConnectionError | ClaudeTurnFailed | asyncio.CancelledError):
                raise
            raise CLIConnectionError(f"Claude Code did not start on {self._link.host}: {e or type(e).__name__}") from e
        await self._launcher.started("none" if how == "relay" and not relayed else how, relayed)
        self._reach.probe = self._answers
        self._ready = True

    async def _start(self, hold: CliHold, script: str) -> None:
        """Open a channel on ``hold`` and run ``script`` there, its stderr read from the start."""
        self._live = self._err_attached = False
        self._process = await hold.start(STREAM_BOOT)
        self._stderr_task = asyncio.ensure_future(self._read_stderr(self._process))
        self._process.stdin.write(script.encode() + b"\n")
        await self._process.stdin.drain()

    async def _first_line(self, pattern: re.Pattern[bytes], *, output: bool) -> re.Match[bytes]:
        """The first stdout line matching ``pattern``, past what the login
        shell printed first; ``output`` counts what it reads as the CLI's."""
        seen = 0
        while True:
            line = await self._process.stdout.readline()
            if output:
                self._out_at += len(line)
            if not line:
                await self._ended(EXIT_GRACE_S)
                if self._stderr_task is not None:
                    await asyncio.wait({self._stderr_task}, timeout=EXIT_GRACE_S)
                why = self.diagnostics() or "the command ended"
                if self._ready:
                    raise detach.Dropped(f"The channel to {self._link.host} ended before it attached: {why}")
                raise CLIConnectionError(f"Claude Code did not start on {self._link.host}: {why}")
            if found := pattern.fullmatch(line.rstrip(b"\r\n")):
                return found
            seen += len(line)
            if seen > _NOISE_MAX:
                raise detach.Broken(f"Claude Code did not start on {self._link.host}: its shell printed too much")

    async def _attached(self) -> int | None:
        """The input the supervisor has taken, as the channel attached says;
        None once the CLI has ended, its output left to read."""
        said = (await self._first_line(detach.ATTACHED, output=False)).group(1)
        if said == b"gone":
            raise detach.Gone(f"Claude Code is no longer running on {self._link.host}")
        return None if said == b"done" else int(said)

    async def _announced(self) -> TokenOrigin:
        """The token source the shim announces, the first line of the CLI's stdout."""
        return cast(TokenOrigin, (await self._first_line(_ANNOUNCE, output=True)).group(1).decode())

    def diagnostics(self) -> str:
        """The last of what the machine wrote to stderr, scrubbed."""
        return stderr_text(self._stderr)

    def _stderr_line(self, line: str, *, whole: bool = True) -> None:
        """One line of the channel's stderr; ``whole`` False for the start of one
        cut short, which the next channel's count starts before."""
        line = line.rstrip()
        if not whole and line.startswith(detach.MARK):
            return  # a protocol line cut short says nothing
        if line == detach.ATTACH_MARK:
            self._err_attached = True
            return
        if self._err_attached:
            if line.startswith(detach.TAKEN_MARK) and line[len(detach.TAKEN_MARK):].isdigit():
                self._input.ack(int(line[len(detach.TAKEN_MARK):]))
                return
            if line.startswith(detach.EXIT_MARK):
                self._exit = line[len(detach.EXIT_MARK):]
                return
            if whole:
                self._err_lines += 1
        if line.startswith(_PID_MARK) and line[len(_PID_MARK):].isdigit():
            self._pid = int(line[len(_PID_MARK):])
            return
        if not line:
            return
        self._stderr.append(line)
        if self._options.stderr is not None:
            try:
                self._options.stderr(line)
            except Exception:  # noqa: BLE001 - a callback must not stop the reader
                logger.debug("stderr callback raised; continuing", exc_info=True)

    async def _read_stderr(self, process: ShellProcess) -> None:
        decoder, framer = codecs.getincrementaldecoder("utf-8")("replace"), _LineFramer()
        try:
            while chunk := await process.stderr.read(65536):
                for line in framer.push(decoder.decode(chunk)):
                    self._stderr_line(line)
                if framer.pending_len > self._max_buffer_size:
                    self._stderr_line(framer.flush(), whole=False)
        except Exception:  # noqa: BLE001 - diagnostics only
            logger.debug("stderr read failed", exc_info=True)
        finally:
            self._stderr_line(framer.flush(), whole=False)

    async def _send(self, data: str, *, fail: bool = False) -> None:
        """Send ``data`` on the channel attached, keeping it until the
        supervisor has taken it. With no channel attached it waits for the
        next, which sends it; ``fail`` fails the write instead."""
        async with self._write_lock:
            encoded = data.encode()
            self._input.add(encoded)
            if not self._live:
                if fail:
                    raise CLIConnectionError(f"Lost the connection to {self._link.host}")
                return
            try:
                self._process.stdin.write(encoded)
                await self._process.stdin.drain()
            except Exception as e:
                self._live = False
                if fail:
                    raise CLIConnectionError(f"Failed to write to process stdin: {e}") from e

    async def write(self, data: str) -> None:
        if not self._ready or self._process is None:
            raise CLIConnectionError("ProcessTransport is not ready for writing")
        if self._exit is not None:
            raise await self._exit_error()
        await self._send(data)

    async def end_input(self) -> None:
        if self._process is not None:
            await self._send(detach.EOF_LINE)

    def read_messages(self) -> AsyncIterator[dict[str, Any]]:
        return self._read_messages()

    async def _read_messages(self) -> AsyncIterator[dict[str, Any]]:
        """The SDK's framing (``_LineFramer``, ``_parse_stdout_line``) over the CLI's
        stdout, across every channel it is attached on."""
        if self._process is None:
            raise CLIConnectionError("Not connected")
        decoder, framer = codecs.getincrementaldecoder("utf-8")(), _LineFramer()

        def guard(length: int) -> None:
            if length > self._max_buffer_size:
                raise CLIJSONDecodeError(
                    f"JSON message exceeded maximum buffer size of {self._max_buffer_size} bytes",
                    ValueError(f"Buffer size {length} exceeds limit {self._max_buffer_size}"),
                )

        try:
            while True:
                try:
                    chunk = await self._process.stdout.read(65536)
                except TRANSPORT_ERRORS as e:
                    await self._reattach(e)
                    continue
                if not chunk:
                    if self._stderr_task is not None:
                        await asyncio.wait({self._stderr_task}, timeout=EXIT_GRACE_S)
                    if self._exit == detach.GRACE_KILLED:
                        ended = self._gave_up(f"Claude Code on {self._link.host} was ended with its connection lost")
                        self._reach.gone(ended)
                        raise ended
                    if self._exit is not None:
                        break
                    await self._reattach(None)
                    continue
                self._out_at += len(chunk)
                for line in framer.push(decoder.decode(chunk)):
                    guard(len(line))
                    if (data := _parse_stdout_line(line)) is not None:
                        yield data
                guard(framer.pending_len)
        except GeneratorExit:
            return
        with contextlib.suppress(CLIJSONDecodeError):
            if (data := _parse_stdout_line(framer.flush())) is not None:
                yield data
        if self._exit_code() != 0:
            raise await self._exit_error()

    def _gave_up(self, detail: str) -> CLIConnectionError | ClaudeTurnFailed:
        """What ends a run whose CLI the transport cannot reach again: stopped
        to be resumed where the launcher says, else a lost connection."""
        if self._launcher.resumes and not self._ending:
            return ClaudeTurnFailed(COMPUTER_LOST, detail)
        return CLIConnectionError(detail)

    async def _reattach(self, cause: BaseException | None) -> None:
        """Attach a new channel to the CLI once its connection is lost, within
        ``RECONNECT_WAIT_S``; past it, or once the CLI is gone from the
        machine, the run ends (``_gave_up``), and so do the waits that
        paused for it (``Reach.gone``)."""
        try:
            await self._reattach_within(cause)
        except BaseException as e:
            self._reach.gone(e)
            raise

    async def _reattach_within(self, cause: BaseException | None) -> None:
        lost = f"Lost the connection to {self._link.host}: {cause or 'the channel closed'}"
        if self._ending:
            raise CLIConnectionError(lost) from cause
        logger.warning("[ClaudeCode] lost the connection to %s mid-turn (%s); reattaching", self._link.host, lost)
        self._reach.lost()
        loop = asyncio.get_running_loop()
        started = loop.time()
        deadline = started + RECONNECT_WAIT_S
        last: BaseException | None = cause
        while True:
            await self._detach()
            if self._ending:
                raise CLIConnectionError(lost) from cause
            remaining = deadline - loop.time()
            if remaining <= 0:
                if isinstance(last, SlotWaitTimeout):
                    raise slots_full(last) from last
                logger.warning("[ClaudeCode] %s did not answer again within %ss: %s", self._link.host, RECONNECT_WAIT_S, last)
                raise self._gave_up(lost) from last
            try:
                async with asyncio.timeout(remaining):
                    await self._attach(deadline)
            except detach.Gone as e:
                self._gone = True
                raise self._gave_up(str(e)) from e
            except Exception as e:
                if not (isinstance(e, detach.Dropped) or detach.unanswered(e)):
                    raise
                last = e
            else:
                if self._ending:
                    await self._detach()
                    raise CLIConnectionError(lost) from cause
                logger.info("[ClaudeCode] reattached to the CLI on %s after %.1fs", self._link.host, loop.time() - started)
                self._reach.back()
                return
            await asyncio.sleep(min(detach.RETRY_S, max(0.0, deadline - loop.time())))

    async def _detach(self) -> None:
        """Let go of the channel attached and the slot it held."""
        self._live = False
        if self._process is not None:
            with contextlib.suppress(Exception):
                self._process.close()
        if self._stderr_task is not None:
            self._stderr_task.cancel()
            await asyncio.wait({self._stderr_task}, timeout=1)
        holds, self._holds = self._holds, contextlib.AsyncExitStack()
        with contextlib.suppress(Exception):
            await holds.aclose()

    async def _attach(self, deadline: float) -> None:
        """Dial the machine once and attach a channel at what was read, and
        send the input the supervisor has not taken in the background: not
        waited out here, since the channel takes it only as fast as this side
        reads the output it replays first. A slot that does not free a
        moment before ``deadline`` says so (``SlotWaitTimeout``), so a busy
        computer is not taken for a lost one."""
        link = await self._launcher.machine.link()
        wait_s = max(0.1, deadline - asyncio.get_running_loop().time() - 1)
        hold = await self._holds.enter_async_context(link.cli_hold(wait_s))
        self._link, self._cli_link = link, hold.link
        await self._start(hold, detach.attach_command(self._dir, self._out_at, self._err_lines))
        taken = await self._attached()
        async with self._write_lock:
            if taken is not None:
                if not self._input.takes(taken):
                    raise detach.Broken(f"Claude Code on {self._link.host} took input this turn never sent")
                self._input.ack(taken)
                if self._input.unsent:
                    try:
                        self._process.stdin.write(bytes(self._input.unsent))
                    except Exception as e:
                        raise detach.Dropped(f"Failed to write to process stdin: {e}") from e
                    self._resending = asyncio.ensure_future(self._resend(self._process))
            self._live = True

    async def _resend(self, process: ShellProcess) -> None:
        """Wait out the send of what a reattach queued on ``process``; a
        channel that fails it is lost, and its read reattaches."""
        try:
            await process.stdin.drain()
        except Exception:  # noqa: BLE001 - the read sees the channel end
            logger.debug("[ClaudeCode] a reattach's resend failed", exc_info=True)

    async def _answers(self) -> bool:
        """Whether the CLI's channel is attached and the machine still answers
        on its connection, asked by a wait that ran out with the CLI in
        reach. A channel already let go is not answering: its read is about
        to reattach. One whose machine does not answer is out of reach, and
        its connection is closed, so the read notices now and the reattach
        dials a new one rather than this stale one."""
        if self._ending:
            return True
        if not self._live:
            return False
        process, link = self._process, self._cli_link
        try:
            if not isinstance(link, Link):
                if (await link.run("true", timeout=2 * EXIT_GRACE_S, kill=True)).exit_code != 0:
                    raise ConnectionError("the probe did not finish")
                return True
            # Only the answer is timed: the kills holding the connection's
            # kill slot are bounded by their own timeouts, and a wait for them
            # is the connection at work, not silence.
            try:
                await asyncio.wait_for(link.kill_slot.acquire(), RECONNECT_WAIT_S)
            except TimeoutError:
                return True
            try:
                answered = await link.run("true", timeout=2 * EXIT_GRACE_S, slot=asyncio.Semaphore(1))
            finally:
                link.kill_slot.release()
            if answered.exit_code != 0:
                raise ConnectionError("the probe did not finish")
            return True
        except Exception as e:  # noqa: BLE001 - any failure to answer is the answer
            logger.warning("[ClaudeCode] %s did not answer mid-turn (%s); reattaching", self._link.host, e or type(e).__name__)
        if self._process is not process:
            return False
        self._reach.lost()
        with contextlib.suppress(Exception):
            if isinstance(link, Link):
                link.close()
            process.close()
        return False

    async def _exit_error(self) -> ProcessError:
        reader = asyncio.shield(self._stderr_task) if self._stderr_task is not None else None
        return await exit_error(self._exit_code(), self._stderr, reader)

    def _exit_code(self) -> int | None:
        return None if self._exit is None else detach.exit_code(self._exit)

    async def _ended(self, timeout: float) -> bool:
        """Whether the channel has ended, waiting up to ``timeout`` for it to close."""
        with contextlib.suppress(TimeoutError, *TRANSPORT_ERRORS):
            await asyncio.wait_for(asyncio.shield(self._process.wait_closed()), timeout)
        return self._process.exit_status is not None or self._process.exit_signal is not None or self._process.is_closing()

    async def _cli_ended(self, timeout: float) -> bool:
        """Whether the CLI has ended, as its channel says once it closes, waiting
        up to ``timeout``; a channel not attached cannot say, so its wait is only
        the time a signal takes."""
        if not self._live:
            await asyncio.sleep(timeout)
            return False
        await self._ended(timeout)
        if self._stderr_task is not None:
            await asyncio.wait({self._stderr_task}, timeout=1)
        return self._exit is not None

    async def _signal(self, sig: str) -> bool:
        """Signal the CLI by its pid, if that pid still runs it, on the
        channel's connection or, with it lost, a new one; False when it could
        not be sent."""
        for _ in range(20):
            if self._pid is not None or self._stderr_task is None or self._stderr_task.done():
                break
            await asyncio.sleep(0.1)
        if self._pid is None:
            return False
        try:
            link = self._cli_link
            if not link.usable:
                link = self._cli_link = await asyncio.wait_for(self._launcher.machine.link(), EXIT_GRACE_S)
            await link.run(kill_if_cli(self._pid, str(self._options.cli_path), sig), timeout=EXIT_GRACE_S + 10, kill=True)
        except Exception as e:  # noqa: BLE001 - the supervisor's grace ends what this cannot
            logger.warning("[ClaudeCode] could not signal the CLI on %s: %s", self._link.host, e or type(e).__name__)
            return False
        return True

    async def _shutdown(self) -> None:
        try:
            if self._process is not None:
                await self._end_cli()
        finally:
            await self._holds.aclose()

    async def _end_cli(self) -> None:
        """End the CLI's input, then signal it until it is known to have ended."""
        if self._live:
            with contextlib.suppress(Exception):
                self._process.stdin.write(detach.EOF_LINE.encode())
                await asyncio.wait_for(self._process.stdin.drain(), EXIT_GRACE_S)
        if self._exit is None and not self._gone and not (self._live and await self._cli_ended(EXIT_GRACE_S)):
            for sig in ("TERM", "KILL"):
                if not await self._signal(sig) or await self._cli_ended(EXIT_GRACE_S):
                    break
        self._live = False
        self._process.close()
        await self._ended(EXIT_GRACE_S)
        if self._resending is not None:
            self._resending.cancel()
        if self._stderr_task is not None:
            await asyncio.wait({self._stderr_task}, timeout=1)
            self._stderr_task.cancel()

    async def close(self) -> None:
        """End the CLI. The shutdown runs as the link's own task, so a caller cancelled mid-close never leaves the CLI running."""
        self._ready = False
        self._ending = True
        if self._reach.probe == self._answers:
            self._reach.probe = None
        if self._closing is None:
            self._closing = self._link.spawn(self._shutdown())
        await asyncio.shield(self._closing)

    def is_ready(self) -> bool:
        return self._ready
