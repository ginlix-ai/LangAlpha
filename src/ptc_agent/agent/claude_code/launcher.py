"""Where a Claude Code turn's CLI runs, and with what environment.

The SDK client, the in-process MCP server and the session store run in the
backend worker that owns the turn; the CLI reaches them over its stdio
control channel. A launcher prepares the machine the CLI runs on and builds
the transport that starts it there. ``LocalLauncher`` runs it on this worker.

The launcher writes the session file from the checkpoint before the CLI
starts, whatever the machine holds: the checkpoint is the session's truth,
so a crash, a rollback or a new worker never resumes from a stale file.

The child never inherits the worker's environment. The SDK's own transport
merges ``os.environ`` into it, which would hand the CLI every credential the
backend holds and, through an inherited ``ANTHROPIC_API_KEY``, bill a plan
thread to the platform's key. The child gets an allowlist plus what the
runtime sets.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import shutil
import stat
import tempfile
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

import anyio
from anyio.streams.text import TextReceiveStream, TextSendStream
from claude_agent_sdk import ClaudeAgentOptions, CLIConnectionError, CLINotFoundError
from claude_agent_sdk._internal._task_compat import spawn_detached
from claude_agent_sdk._internal.sessions import project_key_for_directory
from claude_agent_sdk._internal.transport import Transport
from claude_agent_sdk._internal.transport.subprocess_cli import (
    _ACTIVE_CHILDREN,
    _SDK_READS_SESSION_STATE_ENV,
    PIPE,
    SubprocessCLITransport,
    __version__,
    logger,
)

from src.server.database.encryption import get_encryption_key

#: Inherited from the worker because a process needs them to start or to
#: reach the API from where the worker sits (its proxy and CA); nothing that
#: authenticates to Anthropic. On macOS the CLI finds its Keychain login by the
#: account name in USER, so without it a logged-in machine runs logged out.
_INHERITED = (
    "PATH", "HOME", "USER", "LOGNAME", "LANG", "LC_ALL", "TMPDIR", "TZ", "SSL_CERT_FILE", "SSL_CERT_DIR",
    "HTTPS_PROXY", "HTTP_PROXY", "NO_PROXY", "https_proxy", "http_proxy", "no_proxy", "NODE_EXTRA_CA_CERTS",
)
_THREAD_KEY = re.compile(r"[A-Za-z0-9_-]{1,128}")
AUTH_STATUS_TIMEOUT_S = 30

#: The sha256 of ``SubprocessCLITransport.connect``'s source that
#: ``ScopedEnvTransport.connect`` was copied from; a test fails when it moves.
SDK_CONNECT_SHA256 = "77a86450f9f8c68d2b3346211c5933e617bf1c7eec0207a87a870ba35b3ad0c3"


def device_id(identity: str) -> str:
    """The device Claude Code reports for a user: the same on every turn and worker.

    The CLI sends its config dir's ``userID`` with every request and mints a
    new one in an empty dir, so without a seeded id every turn of a thread
    would report another device. Keyed with the deployment's
    at-rest key, so the id cannot be recomputed from the user id alone; a
    deployment without the key can still start, and only a Claude Code turn
    fails.
    """
    return hmac.new(get_encryption_key().encode(), f"claude-code-device:{identity}".encode(), hashlib.sha256).hexdigest()


@dataclass(frozen=True)
class Prepared:
    """A machine made ready for one turn's CLI.

    ``config_dir`` is the turn's own and holds the session file and the system
    prompt. ``cwd`` is the thread's, the same path on every turn and worker.
    """

    config_dir: str
    cwd: str

    @property
    def system_prompt_file(self) -> str:
        return os.path.join(self.config_dir, "system-prompt.md")


def _own_dir(path: str) -> None:
    """Make ``path`` a private directory of this user, refusing one another user or a symlink put there."""
    os.makedirs(path, mode=0o700, exist_ok=True)
    st = os.lstat(path)
    if not stat.S_ISDIR(st.st_mode) or st.st_uid != os.getuid():
        raise RuntimeError(f"{path} is not a directory this user owns")
    os.chmod(path, 0o700)


def thread_dir(thread_key: str) -> str:
    """The thread's working directory: the same path on every worker, a private directory on each."""
    if not _THREAD_KEY.fullmatch(thread_key):
        raise ValueError(f"thread key {thread_key!r} is not a plain id")
    # Resolved because the CLI keys the session file by the real path (/tmp is
    # /private/tmp on macOS).
    base = os.path.join(os.path.realpath("/tmp"), "langalpha-claude")
    _own_dir(base)
    cwd = os.path.join(base, thread_key)
    _own_dir(cwd)
    return cwd


def session_file_path(config_dir: str, cwd: str, session_id: str) -> Path:
    """Where the CLI looks for a session's transcript: its own project layout."""
    return Path(config_dir) / "projects" / project_key_for_directory(cwd) / f"{session_id}.jsonl"


def write_session_file(config_dir: str, cwd: str, session_id: str, entries: list[dict[str, Any]]) -> None:
    """Lay the session's records out on this machine, replacing what is there."""
    path = session_file_path(config_dir, cwd, session_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(e, separators=(",", ":")) + "\n" for e in entries), encoding="utf-8")


class ScopedEnvTransport(SubprocessCLITransport):
    """The SDK's subprocess transport with the child's environment built from scratch.

    The transport is the SDK's internal API, which is why the ``claude-code``
    extra pins the SDK exactly.
    """

    def __init__(self, *, options: ClaudeAgentOptions, base_env: dict[str, str]) -> None:
        super().__init__(prompt=_no_prompt(), options=options)
        self._base_env = base_env

    async def connect(self) -> None:
        """The SDK's ``connect`` (``SDK_CONNECT_SHA256``), changed in three places only.

        The environment starts from ``base_env``, not ``os.environ``. The version
        probe is skipped: it would run the CLI with the worker's whole
        environment, and the CLI is the one the pinned SDK bundles. The child
        starts in its own session, so a terminal's Ctrl-C or a job-control
        signal to the worker's process group never stops a CLI mid-turn; the
        run's close ends it.
        """
        if self._process:
            return

        if self._cli_path is None:
            self._cli_path = await anyio.to_thread.run_sync(self._find_cli)

        self._reject_windows_batch_cli(self._cli_path)

        cmd = self._build_command()
        try:
            process_env = {
                **self._base_env,
                "CLAUDE_CODE_ENTRYPOINT": "sdk-py",
                **self._options.env,
                "CLAUDE_AGENT_SDK_VERSION": __version__,
            }

            try:
                from opentelemetry import propagate

                carrier: dict[str, str] = {}
                propagate.inject(carrier)
                if "traceparent" in carrier:
                    for key in ("TRACEPARENT", "TRACESTATE"):
                        if key not in self._options.env:
                            process_env.pop(key, None)
                    for k, v in carrier.items():
                        key = k.upper()
                        if key not in self._options.env:
                            process_env[key] = v
            except Exception:  # noqa: BLE001 - best-effort tracing must never break connect()
                logger.debug("OTEL trace context injection failed", exc_info=True)

            if not any(key.upper() == _SDK_READS_SESSION_STATE_ENV for key in process_env):
                process_env[_SDK_READS_SESSION_STATE_ENV] = "1"

            if self._options.enable_file_checkpointing:
                process_env["CLAUDE_CODE_ENABLE_SDK_FILE_CHECKPOINTING"] = "true"

            if self._cwd:
                process_env["PWD"] = self._cwd

            stderr_dest = PIPE if self._options.stderr is not None else None

            self._process = await anyio.open_process(
                cmd,
                stdin=PIPE,
                stdout=PIPE,
                stderr=stderr_dest,
                cwd=self._cwd,
                env=process_env,
                user=self._options.user,
                start_new_session=True,
            )
            _ACTIVE_CHILDREN.add(self._process)

            if self._process.stdout:
                self._stdout_stream = TextReceiveStream(self._process.stdout)

            if stderr_dest is PIPE and self._process.stderr:
                self._stderr_stream = TextReceiveStream(self._process.stderr)
                self._stderr_task = spawn_detached(self._handle_stderr())

            if self._process.stdin:
                self._stdin_stream = TextSendStream(self._process.stdin)

            self._ready = True

        except FileNotFoundError as e:
            if self._cwd and not Path(self._cwd).exists():
                error = CLIConnectionError(f"Working directory does not exist: {self._cwd}")
                self._exit_error = error
                raise error from e
            error = CLINotFoundError(f"Claude Code not found at: {self._cli_path}")
            self._exit_error = error
            raise error from e
        except Exception as e:
            error = CLIConnectionError(f"Failed to start Claude Code: {e}")
            self._exit_error = error
            raise error from e


@dataclass(frozen=True)
class LocalLauncher:
    """Runs claude on this worker, in a private config dir per turn.

    The config dir holds the session file the CLI resumes from and appends
    to; it is created for the turn and removed after it. The CLI names its
    credential store after ``CLAUDE_CONFIG_DIR`` whenever that is set; with
    ``machine_login``, ``CLAUDE_SECURESTORAGE_CONFIG_DIR=""`` points only the
    credential lookup back at the machine's own login (``~/.claude``, or the
    macOS Keychain item named for it).

    ``env`` holds what selects the credential (a token, an API key, a test
    endpoint). The machine's own login is used only with ``machine_login``, a
    choice for a deployment one person uses: otherwise the CLI looks for a
    login only in the turn's empty config dir, so a turn given no credential
    fails as logged out rather than run on whoever's login the worker holds.
    """

    env: dict[str, str] = field(default_factory=dict, repr=False)
    cli_path: str | None = None
    machine_login: bool = False

    async def prepare(
        self, thread_key: str, session_id: str | None, entries: list[dict[str, Any]], *, identity: str,
        system_prompt: str,
    ) -> Prepared:
        """Lay out a turn. The CLI's environment block names its working directory, so ``cwd`` never changes under the session.

        The disk work runs off the event loop: the session file is the whole
        transcript, so writing it on the loop would stall every stream on the
        worker for as long as the thread is long.
        """
        device = device_id(identity)

        def lay_out() -> Prepared:
            cwd = thread_dir(thread_key)
            config_dir = tempfile.mkdtemp(prefix="claude-cfg-")
            prepared = Prepared(config_dir=config_dir, cwd=cwd)
            try:
                Path(config_dir, ".claude.json").write_text(json.dumps({"userID": device}))
                Path(prepared.system_prompt_file).write_text(system_prompt, encoding="utf-8")
                if session_id:
                    write_session_file(config_dir, cwd, session_id, entries)
            except BaseException:
                shutil.rmtree(config_dir, ignore_errors=True)
                raise
            return prepared

        return await anyio.to_thread.run_sync(lay_out)

    async def release(self, prepared: Prepared) -> None:
        """Remove the turn's config dir. The thread's cwd stays: it holds nothing, and another run of the thread may be in it."""
        await anyio.to_thread.run_sync(lambda: shutil.rmtree(prepared.config_dir, ignore_errors=True))

    def _base_env(self) -> dict[str, str]:
        return {**{k: os.environ[k] for k in _INHERITED if k in os.environ}, **self.env}

    def _child_options(self, options: ClaudeAgentOptions) -> ClaudeAgentOptions:
        login = {"CLAUDE_SECURESTORAGE_CONFIG_DIR": ""} if self.machine_login else {}
        return replace(options, cli_path=self.cli_path or options.cli_path, env={**options.env, **login})

    def transport(self, options: ClaudeAgentOptions) -> Transport:
        return ScopedEnvTransport(options=self._child_options(options), base_env=self._base_env())

    async def auth_status(self) -> dict[str, Any]:
        """``claude auth status --json`` as a turn's CLI sees it.

        Same binary, same environment and the same config-dir rule as a turn,
        so a machine that is logged in for a person but not for the runtime
        shows up here rather than as a failed turn.
        """
        config_dir = await anyio.to_thread.run_sync(lambda: tempfile.mkdtemp(prefix="claude-cfg-"))
        try:
            options = self._child_options(ClaudeAgentOptions(env={"CLAUDE_CONFIG_DIR": config_dir}))
            cli = options.cli_path or await anyio.to_thread.run_sync(self.transport(options)._find_cli)
            with anyio.fail_after(AUTH_STATUS_TIMEOUT_S):
                done = await anyio.run_process(
                    [str(cli), "auth", "status", "--json"], env={**self._base_env(), **options.env}, input=b"", check=False,
                )
        finally:
            await anyio.to_thread.run_sync(lambda: shutil.rmtree(config_dir, ignore_errors=True))
        try:
            return json.loads(done.stdout.decode() or "{}")
        except json.JSONDecodeError:
            return {"loggedIn": False}


async def _no_prompt():
    """The transport's prompt in streaming mode: the client writes the messages itself."""
    return
    yield {}  # pragma: no cover
