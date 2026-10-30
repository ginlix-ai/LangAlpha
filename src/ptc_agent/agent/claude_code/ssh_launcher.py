"""Claude Code on the user's own computer, over the computer's SSH connection.

The CLI runs on the machine as the SSH login, on one exec channel of the
computer's pooled connection (``SSHRuntime.link``), so a turn opens no login
of its own. The channel's stdin and stdout carry the SDK's stream-json, which
keeps the tools, the hooks and the session mirror in the backend worker, as
for ``LocalLauncher``. Each turn runs in a private config dir under the
computer root's internal dir, laid out over SFTP from the checkpoint and
removed after the turn; a dir a lost connection left behind is swept by a
later turn.

The CLI signs in with a setup token made for LangAlpha, never with the
machine's own Claude Code login (``launcher.py`` says why). The token is the
computer's own file on the machine (``machine_token``), which the backend
never reads.
Where the deployment allows it, a machine without one is relayed the user's
stored token on the channel's stdin, only once the machine has said it has
none. Either way a perl shim on the machine puts the token in a socket pair
and execs the CLI with ``CLAUDE_CODE_OAUTH_TOKEN_FILE_DESCRIPTOR`` naming it:
it never reaches an argv, an environment or a file.
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
from typing import Any, Literal

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
    Prepared,
    _no_prompt,
    cli_env,
    device_id,
    session_jsonl,
)
from ptc_agent.agent.claude_code.machine_token import FIND_PERL, STREAM_BOOT, TOKEN_DIR_SH, checked_name, shim, word
from ptc_agent.core.paths import SandboxLayout
from ptc_agent.core.sandbox.providers._ssh_shell import check_env
from ptc_agent.core.sandbox.providers._ssh_transport import _CLI_SLOTS, TRANSPORT_ERRORS, Link, Machine, write_atomic
from src.llms.claude_code import is_token_text

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
    """Runs claude on the user's own computer, in a private config dir per turn there.

    ``machine`` reaches the computer: its pooled connection, dialed again
    after one is lost, and its root, read once the connection is resolved,
    since a sandbox's root is final only after the setup resolving it waits
    for. ``identity`` is who the turns run for, the device id's key. The CLI
    is the pinned one, installed on the machine as a run first needs it;
    ``install`` False only looks for it, so a check never starts a download.
    ``token_file`` names the token file the shim reads in the tokens' folder:
    the computer's own, or a staged one a check is about. ``slot_wait_s`` is
    how long the CLI waits for a free CLI slot on the computer's connection;
    a check passes a short one.

    ``relay``, given only where the deployment relays a stored token, returns
    that token or None; it is called only once the machine has said it has
    no token of its own. ``on_announced(origin)`` is told where the run's
    token comes from as the shim announces it, and ``on_refused(origin)``
    that Anthropic refused it, by where it came from (``file`` or ``relay``),
    never the token. ``source`` is where the last run's token came from. A relayed
    token is held only until it is written to the channel. One launcher
    serves one run at a time. ``env`` is for a test endpoint.
    """

    machine: Machine
    _: KW_ONLY
    identity: str
    token_file: str
    install: bool = True
    slot_wait_s: float = CLI_SLOT_WAIT_S
    relay: Callable[[], Awaitable[str | None]] | None = None
    on_announced: Callable[[TokenOrigin], Awaitable[None]] | None = None
    on_refused: Callable[[TokenOrigin], Awaitable[None]] | None = None
    env: dict[str, str] = field(default_factory=dict, repr=False)
    source: TokenOrigin | None = field(default=None, init=False)
    _link: Link | None = field(default=None, init=False, repr=False)
    _cli: str | None = field(default=None, init=False)
    _root: str | None = field(default=None, init=False)

    def __post_init__(self) -> None:
        checked_name(self.token_file)

    @property
    def base(self) -> str:
        if self._root is None:
            raise RuntimeError("the computer root is read once the connection is resolved")
        return SandboxLayout.for_root(self._root).claude_code

    async def _connection(self) -> Link:
        self._link = await self.machine.link()
        if self._root is None:
            root = self.machine.root()
            if not root.startswith("/"):
                raise ValueError(f"the computer root {root!r} is not an absolute path")
            self._root = root
        if self._cli is None:
            self._cli = await cli_install.ensure_cli(self._link, self._root, install=self.install)
        return self._link

    async def _lay_out(self, link: Link, thread_key: str | None) -> Prepared:
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
            raise RuntimeError(f"Could not lay out a Claude Code turn on {link.target.host}: {detail}")
        return Prepared(config_dir=lines[0], cwd=lines[1])

    async def _write(self, link: Link, prepared: Prepared, files: list[tuple[str, str]]) -> None:
        """Write the turn's files over SFTP, or remove the turn's dir."""
        try:
            async with link.in_use():
                sftp = await link.sftp()
                try:
                    for path, text in files:
                        await write_atomic(sftp, path, text.encode(), mode=0o600)
                except TRANSPORT_ERRORS:
                    link.drop_sftp(sftp)
                    raise
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
        device = device_id(identity)
        link = await self._connection()
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
        device = device_id(self.identity)
        link = await self._connection()
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
        if self.source in ("file", "relay") and self.on_refused is not None:
            await self.on_refused(self.source)

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
            f'exec env -i "${{a[@]}}" {assigns} "$p" -e {word(shim())} {1 if self.relay else 0} "$d/{self.token_file}" '
            + " ".join(map(word, argv))
        )

    def _started(self, how: str, *, relayed: bool) -> None:
        self.source = "none" if how == "relay" and not relayed else how  # type: ignore[assignment]


class SSHTransport(Transport):
    """The CLI as a process on the user's machine, over one exec channel.

    The command line is the SDK's own (``_build_command``) under the shim;
    stderr is kept for diagnostics and handed to ``options.stderr``. Closing
    ends the CLI: its input is closed, then it is signalled by pid until it
    goes, then the channel is closed.

    The CLI holds one of the link's CLI slots and keeps the link in use from
    its start to the end of its shutdown, so commands always find a slot and
    a link shed from the pool closes only once the turn is over.
    """

    def __init__(self, launcher: SSHLauncher, link: Link, options: ClaudeAgentOptions) -> None:
        self._launcher = launcher
        self._link = link
        self._options = options
        self._process: Any = None
        self._ready = False
        self._pid: int | None = None
        self._stderr: deque[str] = deque(maxlen=50)
        self._stderr_task: asyncio.Task[None] | None = None
        self._closing: asyncio.Task[None] | None = None
        self._holds = contextlib.AsyncExitStack()
        self._write_lock = asyncio.Lock()
        self._max_buffer_size = options.max_buffer_size or _DEFAULT_MAX_BUFFER_SIZE

    async def connect(self) -> None:
        """Start the CLI and settle its token: the stored one goes only when the machine asks for it."""
        if self._process is not None:
            return
        script = self._launcher.command(self._options)
        try:
            await self._hold(self._launcher.slot_wait_s)
        except BaseException as e:
            await self.close()
            if isinstance(e, TimeoutError):
                raise CLIConnectionError(CLI_SLOTS_FULL) from None
            raise
        relayed = False
        try:
            async with asyncio.timeout(START_TIMEOUT_S):
                self._process = await self._link.open_process(STREAM_BOOT)
                self._stderr_task = asyncio.ensure_future(self._read_stderr())
                self._process.stdin.write(script.encode() + b"\n")
                await self._process.stdin.drain()
                how = await self._announced()
                if how == "relay":
                    token = await self._launcher.relay() if self._launcher.relay is not None else None
                    if token and not is_token_text(token):
                        raise CLIConnectionError("The stored token is not one line of text, so it was not sent")
                    self._process.stdin.write((token or "").encode() + b"\n")
                    relayed, token = bool(token), None
                    await self._process.stdin.drain()
        except BaseException as e:
            await self.close()
            if isinstance(e, CLIConnectionError | asyncio.CancelledError):
                raise
            raise CLIConnectionError(f"Claude Code did not start on {self._link.target.host}: {e or type(e).__name__}") from e
        self._launcher._started(how, relayed=relayed)
        if self._launcher.on_announced is not None and self._launcher.source is not None:
            try:
                await self._launcher.on_announced(self._launcher.source)
            except Exception as e:  # noqa: BLE001 - what the backend keeps of it never stops the turn
                logger.warning("[ClaudeCode] could not record where a turn's token came from: %s", type(e).__name__)
        self._ready = True

    async def _hold(self, wait_s: float) -> None:
        """Keep the link in use, then wait up to ``wait_s`` for one of its CLI slots."""
        await self._holds.enter_async_context(self._link.in_use())
        async with asyncio.timeout(wait_s):
            await self._link.cli_slots.acquire()
        self._holds.callback(self._link.cli_slots.release)

    async def _announced(self) -> str:
        """The token source the shim announces, past anything the login shell printed first."""
        seen = 0
        while True:
            line = await self._process.stdout.readline()
            if not line:
                await self._ended(EXIT_GRACE_S)
                if self._stderr_task is not None:
                    await asyncio.wait({self._stderr_task}, timeout=EXIT_GRACE_S)
                raise CLIConnectionError(
                    f"Claude Code did not start on {self._link.target.host}: {self.diagnostics() or 'the command ended'}"
                )
            found = _ANNOUNCE.fullmatch(line.rstrip(b"\r\n"))
            if found:
                return found.group(1).decode()
            seen += len(line)
            if seen > _NOISE_MAX:
                raise CLIConnectionError(f"Claude Code did not start on {self._link.target.host}: its shell printed too much")

    def diagnostics(self) -> str:
        """The last of what the machine wrote to stderr."""
        return "\n".join(self._stderr)

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
                raise CLIConnectionError(f"Cannot write to terminated process (exit code: {self._exit_code()})")
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
            raise CLIConnectionError(f"Lost the connection to {self._link.target.host}: {e or type(e).__name__}") from e
        with contextlib.suppress(CLIJSONDecodeError):
            if (data := _parse_stdout_line(framer.flush())) is not None:
                yield data
        await self._ended(EXIT_GRACE_S)
        code = self._exit_code()
        if code not in (None, 0):
            raise ProcessError(f"Command failed with exit code {code}", exit_code=code, stderr=self.diagnostics())

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
