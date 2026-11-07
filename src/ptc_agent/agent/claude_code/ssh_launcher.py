"""Claude Code on a machine: the user's own computer over its SSH connection,
or a cloud computer's sandbox (``sandbox_launcher``), both as a ``ShellLink``.

On the user's own computer the CLI runs as the SSH login, on one exec channel
of the computer's pooled connection (``SSHRuntime.link``), so a turn opens no
login of its own. The channel's stdin and stdout carry the SDK's stream-json, which
keeps the tools, the hooks and the session mirror in the backend worker, as
for ``LocalLauncher``. Each turn runs in a private config dir under the
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

from ptc_agent.agent.claude_code import cli_install
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
from ptc_agent.agent.claude_code.shell_link import ShellLink, ShellProcess
from ptc_agent.core.sandbox.providers._ssh_transport import _CLI_SLOTS, TRANSPORT_ERRORS, Machine, SlotWaitTimeout
from ptc_agent.core.sandbox.providers.ssh import IS_MUSL_SH, MUSL_UNSUPPORTED
from src.llms.claude_code import ClaudeTurnFailed, is_token_text

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
CLI_SLOTS_FULL = f"Claude Code is already running {_CLI_SLOTS} turns on this computer. Try again when one finishes."
COMMAND_SLOTS_FULL = "This computer is busy running other commands. Try again when one finishes."
#: A turn's config dir left this long is one a lost connection orphaned.
_STALE_MINUTES = 2 * 24 * 60
_SCRIPT_S = 30
_PID_MARK = "__langalpha__ pid "
_ANNOUNCE = re.compile(rb"langalpha-token (file|relay|none)")
_NOISE_MAX = 64 * 1024


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
    a check passes a short one.

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
    """

    machine: Machine[ShellLink]
    _: KW_ONLY
    identity: str
    token_file: str | None
    install: bool = True
    slot_wait_s: float = CLI_SLOT_WAIT_S
    credential: TokenStore | None = None
    on_announced: Callable[[TokenOrigin], Awaitable[None]] | None = None
    on_file_refused: Callable[[], Awaitable[None]] | None = None
    on_blocked: Callable[[bool], Awaitable[None]] | None = None
    on_no_perl: Callable[[bool], Awaitable[None]] | None = None
    on_unsupported: Callable[[bool], Awaitable[None]] | None = None
    env: dict[str, str] = field(default_factory=dict, repr=False)
    source: TokenOrigin | None = field(default=None, init=False)
    _link: ShellLink | None = field(default=None, init=False, repr=False)
    _cli: str | None = field(default=None, init=False)
    _root: str | None = field(default=None, init=False)

    def __post_init__(self) -> None:
        if self.token_file is not None:
            checked_name(self.token_file)

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
        self._link = await self.machine.link()
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
        link = await self.ready()
        device = device_id(self.identity)
        prepared = await self._lay_out(link, None)
        await self._write(link, prepared, [(posixpath.join(prepared.config_dir, ".claude.json"), json.dumps({"userID": device}))])
        return prepared

    async def release(self, prepared: Prepared) -> None:
        """Remove the turn's config dir; one that cannot be reached now is swept by a later turn."""
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
        """The script the channel runs: the CLI under the shim, in a clean environment.

        The environment is the allowlist, taken from the machine's own as sshd
        set it, plus the runtime's variables; nothing of the worker's.
        """
        argv = SubprocessCLITransport(prompt=_no_prompt(), options=options)._build_command()
        env = check_env(cli_env(options, self.env))
        assigns = " ".join(word(f"{k}={v}") for k, v in env.items())
        return (
            f"umask 077; {FIND_PERL}{TOKEN_DIR_SH}cd {word(str(options.cwd))} || exit 1; "
            f'a=(); for k in {" ".join(_INHERITED)}; do [ "${{!k+x}}" ] && a+=("$k=${{!k}}"); done; '
            f'exec env -i "${{a[@]}}" {assigns} "$p" -e {word(shim())} {1 if self.credential is not None else 0} {self._token_path} '
            + " ".join(map(word, argv))
        )

    async def started(self, source: TokenOrigin) -> None:
        """The run's CLI started on a token from ``source``, as the shim announced it."""
        self.source = source
        if self.on_announced is None:
            return
        try:
            await self.on_announced(source)
        except Exception as e:  # noqa: BLE001 - what the backend keeps of it never stops the turn
            logger.warning("[ClaudeCode] could not record where a turn's token came from: %s", type(e).__name__)


class SSHTransport(Transport):
    """The CLI as a process on the machine, on one channel of its link.

    The command line is the SDK's own (``_build_command``) under the shim;
    stderr is kept for diagnostics and handed to ``options.stderr``. Closing
    ends the CLI: its input is closed, then it is signalled by pid until it
    goes, then the channel is closed.

    The CLI holds one of the link's CLI slots and keeps the link in use from
    its start to the end of its shutdown, so commands always find a slot and
    a link shed from the pool closes only once the turn is over.
    """

    def __init__(self, launcher: SSHLauncher, link: ShellLink, options: ClaudeAgentOptions) -> None:
        self._launcher = launcher
        self._link = link
        self._options = options
        self._process: ShellProcess | None = None
        self._ready = False
        self._pid: int | None = None
        self._stderr: deque[str] = deque(maxlen=STDERR_KEPT)
        self._stderr_task: asyncio.Task[None] | None = None
        self._closing: asyncio.Task[None] | None = None
        self._holds = contextlib.AsyncExitStack()
        self._write_lock = asyncio.Lock()
        self._max_buffer_size = options.max_buffer_size or _DEFAULT_MAX_BUFFER_SIZE

    async def connect(self) -> None:
        """Start the CLI and settle its credential: a relayed one is read and
        sent only when the machine asks for it. A refusal reading it ends the
        start as it is (``ClaudeTurnFailed``)."""
        if self._process is not None:
            return
        script = self._launcher.command(self._options)
        try:
            start = await self._holds.enter_async_context(self._link.cli_hold(self._launcher.slot_wait_s))
        except BaseException as e:
            await self.close()
            if isinstance(e, SlotWaitTimeout):
                raise CLIConnectionError(CLI_SLOTS_FULL if e.which == "cli" else COMMAND_SLOTS_FULL) from None
            raise
        relayed = False
        try:
            async with asyncio.timeout(START_TIMEOUT_S):
                self._process = await start(STREAM_BOOT)
                self._stderr_task = asyncio.ensure_future(self._read_stderr())
                self._process.stdin.write(script.encode() + b"\n")
                await self._process.stdin.drain()
                how = await self._announced()
                if how == "relay":
                    store = self._launcher.credential
                    credential = await store.read() if store is not None else None
                    if credential is not None and not is_token_text(credential.secret):
                        raise CLIConnectionError("The stored credential is not one line of text, so it was not sent")
                    line = f"{credential.kind} {credential.secret}" if credential is not None else ""
                    self._process.stdin.write(line.encode() + b"\n")
                    relayed = credential is not None
                    await self._process.stdin.drain()
        except BaseException as e:
            await self.close()
            if isinstance(e, CLIConnectionError | ClaudeTurnFailed | asyncio.CancelledError):
                raise
            raise CLIConnectionError(f"Claude Code did not start on {self._link.host}: {e or type(e).__name__}") from e
        await self._launcher.started("none" if how == "relay" and not relayed else how)
        self._ready = True

    async def _announced(self) -> TokenOrigin:
        """The token source the shim announces, past anything the login shell printed first."""
        seen = 0
        while True:
            line = await self._process.stdout.readline()
            if not line:
                await self._ended(EXIT_GRACE_S)
                if self._stderr_task is not None:
                    await asyncio.wait({self._stderr_task}, timeout=EXIT_GRACE_S)
                raise CLIConnectionError(
                    f"Claude Code did not start on {self._link.host}: {self.diagnostics() or 'the command ended'}"
                )
            found = _ANNOUNCE.fullmatch(line.rstrip(b"\r\n"))
            if found:
                return cast(TokenOrigin, found.group(1).decode())
            seen += len(line)
            if seen > _NOISE_MAX:
                raise CLIConnectionError(f"Claude Code did not start on {self._link.host}: its shell printed too much")

    def diagnostics(self) -> str:
        """The last of what the machine wrote to stderr, scrubbed."""
        return stderr_text(self._stderr)

    def _stderr_line(self, line: str) -> None:
        line = line.rstrip()
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

    async def _read_stderr(self) -> None:
        decoder, framer = codecs.getincrementaldecoder("utf-8")("replace"), _LineFramer()
        try:
            while chunk := await self._process.stderr.read(65536):
                for line in framer.push(decoder.decode(chunk)):
                    self._stderr_line(line)
                if framer.pending_len > self._max_buffer_size:
                    self._stderr_line(framer.flush())
        except Exception:  # noqa: BLE001 - diagnostics only
            logger.debug("stderr read failed", exc_info=True)
        finally:
            self._stderr_line(framer.flush())

    async def write(self, data: str) -> None:
        async with self._write_lock:
            if not self._ready or self._process is None:
                raise CLIConnectionError("ProcessTransport is not ready for writing")
            if self._exited():
                raise await self._exit_error()
            try:
                self._process.stdin.write(data.encode())
                await self._process.stdin.drain()
            except Exception as e:
                self._ready = False
                raise CLIConnectionError(f"Failed to write to process stdin: {e}") from e

    async def end_input(self) -> None:
        async with self._write_lock:
            if self._process is not None:
                with contextlib.suppress(Exception):
                    self._process.stdin.write_eof()

    def read_messages(self) -> AsyncIterator[dict[str, Any]]:
        return self._read_messages()

    async def _read_messages(self) -> AsyncIterator[dict[str, Any]]:
        """The SDK's framing (``_LineFramer``, ``_parse_stdout_line``) over the channel's stdout."""
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
            while chunk := await self._process.stdout.read(65536):
                for line in framer.push(decoder.decode(chunk)):
                    guard(len(line))
                    if (data := _parse_stdout_line(line)) is not None:
                        yield data
                guard(framer.pending_len)
        except GeneratorExit:
            return
        except TRANSPORT_ERRORS as e:
            raise CLIConnectionError(f"Lost the connection to {self._link.host}: {e or type(e).__name__}") from e
        with contextlib.suppress(CLIJSONDecodeError):
            if (data := _parse_stdout_line(framer.flush())) is not None:
                yield data
        await self._ended(EXIT_GRACE_S)
        if self._exit_code() not in (None, 0):
            raise await self._exit_error()

    async def _exit_error(self) -> ProcessError:
        reader = asyncio.shield(self._stderr_task) if self._stderr_task is not None else None
        return await exit_error(self._exit_code(), self._stderr, reader)

    def _exited(self) -> bool:
        return self._process.exit_status is not None or self._process.exit_signal is not None

    def _exit_code(self) -> int | None:
        return -1 if self._process.exit_signal is not None else self._process.exit_status

    async def _ended(self, timeout: float) -> bool:
        """Whether the CLI has ended, waiting up to ``timeout`` for its channel to close."""
        with contextlib.suppress(TimeoutError, *TRANSPORT_ERRORS):
            await asyncio.wait_for(asyncio.shield(self._process.wait_closed()), timeout)
        return self._exited() or self._process.is_closing()

    async def _signal(self, sig: str) -> None:
        """Signal the CLI by its pid, if that pid still runs it; and over the channel, which not every sshd honours."""
        with contextlib.suppress(Exception):
            self._process.send_signal(sig)
        for _ in range(20):
            if self._pid is not None or self._stderr_task is None or self._stderr_task.done():
                break
            await asyncio.sleep(0.1)
        if self._pid is None or not self._link.usable:
            return
        with contextlib.suppress(Exception):
            await self._link.run(kill_if_cli(self._pid, str(self._options.cli_path), sig), timeout=EXIT_GRACE_S + 10, kill=True)

    async def _shutdown(self) -> None:
        try:
            if self._process is not None:
                await self._end_cli()
        finally:
            await self._holds.aclose()

    async def _end_cli(self) -> None:
        with contextlib.suppress(Exception):
            self._process.stdin.write_eof()
        if not await self._ended(EXIT_GRACE_S):
            for sig in ("TERM", "KILL"):
                await self._signal(sig)
                if await self._ended(EXIT_GRACE_S):
                    break
        self._process.close()
        await self._ended(EXIT_GRACE_S)
        if self._stderr_task is not None:
            await asyncio.wait({self._stderr_task}, timeout=1)
            self._stderr_task.cancel()

    async def close(self) -> None:
        """End the CLI. The shutdown runs as the link's own task, so a caller cancelled mid-close never leaves the CLI running."""
        self._ready = False
        if self._closing is None:
            self._closing = self._link.spawn(self._shutdown())
        await asyncio.shield(self._closing)

    def is_ready(self) -> bool:
        return self._ready
