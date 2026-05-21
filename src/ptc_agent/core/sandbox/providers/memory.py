"""Local subprocess sandbox provider — runs sandboxes as local Python subprocesses.

Designed for **OSS / company-laptop scenarios** where Docker can't be installed
and Daytona's free tier blocks custom snapshot creation.

Tradeoffs vs. Daytona / Docker:
  + Zero external dependencies — uses only stdlib + asyncio
  + Reuses host venv (no need to reinstall pandas/numpy/yfinance/...)
  + Fast startup (no container/VM provisioning)
  - **No isolation**: Agent-generated Python runs directly on the host
    machine. Only safe when you trust the prompts you give the agent.
  - No preview URLs (port forwarding to host)
  - No archive (just stop/delete)

Persistence model:
  - Each runtime gets a directory under ``base_dir`` named ``{runtime_id}``.
    Default ``base_dir`` is ``~/.codebuddy/local-sandboxes/`` so runtimes
    survive backend restarts and can be reconnected via ``provider.get(id)``.
  - State (current lifecycle) is tracked in a ``_state.json`` file inside
    each runtime dir.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import re
import shutil
import sys
import uuid
from pathlib import Path
from typing import Any

from ptc_agent.core.sandbox.providers._chart_capture import (
    build_code_wrapper,
    extract_artifacts,
)
from ptc_agent.core.sandbox.runtime import (
    CodeRunResult,
    ExecResult,
    PreviewInfo,
    RuntimeState,
    SandboxGoneError,
    SandboxProvider,
    SandboxRuntime,
    SandboxTransientError,
    SessionCommandResult,
)

# Allow only safe characters in session IDs (mirrors docker provider).
_SESSION_ID_RE = re.compile(r"^[a-zA-Z0-9_\-.]{1,64}$")

# Default location: under the project's .codebuddy directory so sandboxes
# are co-located with the project and visible to the developer.
# Falls back to ~/.codebuddy/local-sandboxes/ if not running from a project root.
_DEFAULT_BASE_DIR = Path.home() / ".codebuddy" / "local-sandboxes"


def _find_project_sandbox_dir() -> Path:
    """Locate the sandbox directory one level above the project root.

    Placed outside the project so uvicorn --reload never monitors it.
    Returns <project_parent>/.langalpha_sandboxes/ if found, else global default.
    """
    current = Path.cwd()
    for parent in [current, *current.parents]:
        if (parent / "pyproject.toml").exists() and (parent / "src").is_dir():
            sandbox_dir = parent.parent / ".langalpha_sandboxes"
            sandbox_dir.mkdir(parents=True, exist_ok=True)
            return sandbox_dir
    return _DEFAULT_BASE_DIR


def _python_executable() -> str:
    """Return the Python interpreter to spawn for code_run.

    Uses the *same* interpreter the backend is running under so that
    subprocesses see the exact set of packages already installed in the
    backend's venv (pandas / numpy / yfinance / mcp / ...).
    """
    return sys.executable or "python3"


class _FileEntry:
    """Mimics the Daytona SDK file entry object with .name and .is_dir attributes."""

    def __init__(self, name: str, is_dir: bool) -> None:
        self.name = name
        self.is_dir = is_dir


class LocalRuntime(SandboxRuntime):
    """Local subprocess-backed sandbox runtime.

    **Path model**: To match agent_config.yaml ``working_directory: /home/workspace``
    semantics (which the agent prompts, path validation, and SkillsMiddleware all
    assume), this runtime presents a *virtual* working_dir of ``/home/workspace``
    while transparently mapping every file operation to a real host directory
    under ``~/.codebuddy/local-sandboxes/{runtime_id}/``.

    Example:
      Agent writes to:    /home/workspace/results/foo.txt
      Real disk path:     ~/.codebuddy/local-sandboxes/local-abc123/results/foo.txt
      Subprocess cwd:     ~/.codebuddy/local-sandboxes/local-abc123/

    Each instance owns a host directory. exec/code_run spawn real subprocesses
    rooted in that directory. Sessions are stored as files under
    ``{host_dir}/.sessions/{session_id}/`` (matches docker provider layout).
    """

    # Virtual working dir reported to the rest of the system. Must match
    # ``filesystem.working_directory`` in agent_config.yaml (default
    # ``/home/workspace``) so path validation accepts agent-side paths.
    _VIRTUAL_WORK_DIR = "/home/workspace"

    def __init__(
        self,
        runtime_id: str,
        host_dir: str,
        env_vars: dict[str, str] | None = None,
        virtual_work_dir: str | None = None,
    ) -> None:
        self._id = runtime_id
        self._host_dir = host_dir  # real directory on disk
        self._virtual_work_dir = virtual_work_dir or self._VIRTUAL_WORK_DIR
        self._env_vars = dict(env_vars or {})
        self._deleted = False
        self._state = RuntimeState.RUNNING

        os.makedirs(self._host_dir, exist_ok=True)
        os.makedirs(os.path.join(self._host_dir, ".sessions"), exist_ok=True)
        self._persist_state()

    # ============================================================
    # Identity & working dir
    # ============================================================

    @property
    def id(self) -> str:
        return self._id

    @property
    def working_dir(self) -> str:
        # Report the virtual path so the rest of langalpha (which assumes
        # /home/workspace as the canonical sandbox root) just works.
        return self._virtual_work_dir

    async def fetch_working_dir(self) -> str:
        return self._virtual_work_dir

    @property
    def host_dir(self) -> str:
        """Real directory on the host filesystem."""
        return self._host_dir

    # ============================================================
    # Lifecycle
    # ============================================================

    async def start(self, timeout: int = 120) -> None:
        if self._deleted:
            raise SandboxGoneError(self._id, "runtime has been deleted")
        self._state = RuntimeState.RUNNING
        self._persist_state()

    async def stop(self, timeout: int = 60) -> None:
        if self._deleted:
            raise SandboxGoneError(self._id, "runtime has been deleted")
        self._state = RuntimeState.STOPPED
        self._persist_state()

    async def delete(self) -> None:
        self._state = RuntimeState.STOPPED
        self._deleted = True
        # Remove the workspace directory entirely.
        with contextlib.suppress(Exception):
            shutil.rmtree(self._host_dir, ignore_errors=True)

    async def archive(self) -> None:
        if self._deleted:
            raise SandboxGoneError(self._id, "runtime has been deleted")
        # No real archive; just mark as such.
        self._state = RuntimeState.ARCHIVED
        self._persist_state()

    async def get_state(self) -> RuntimeState:
        if self._deleted:
            return RuntimeState.ERROR
        return self._state

    # ============================================================
    # Execution
    # ============================================================

    async def exec(self, command: str, timeout: int = 60) -> ExecResult:
        self._check_running()
        try:
            proc = await asyncio.create_subprocess_shell(
                self._rewrite_command(command),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=self._host_dir,
                env={**os.environ, **self._env_vars},
            )
            stdout_b, stderr_b = await asyncio.wait_for(
                proc.communicate(), timeout=timeout
            )
            stdout = (stdout_b or b"").decode("utf-8", errors="replace")
            stderr = (stderr_b or b"").decode("utf-8", errors="replace")
            # Re-virtualize: replace real host paths back to virtual paths in output
            # so upper layers (ptc_sandbox.virtualize_path) can process them correctly.
            stdout = self._revirtualize_output(stdout)
            stderr = self._revirtualize_output(stderr)
            return ExecResult(
                stdout=stdout, stderr=stderr, exit_code=proc.returncode or 0
            )
        except asyncio.TimeoutError:
            return ExecResult(stdout="", stderr="timeout", exit_code=-1)
        except Exception as e:
            return ExecResult(stdout="", stderr=str(e), exit_code=-1)

    async def code_run(
        self,
        code: str,
        env: dict[str, str] | None = None,
        timeout: int = 300,
    ) -> CodeRunResult:
        self._check_running()

        code_file = os.path.join(
            self._host_dir, f"_exec_{uuid.uuid4().hex[:8]}.py"
        )
        wrapper = build_code_wrapper(code)
        with open(code_file, "w") as f:
            f.write(wrapper)

        run_env = {**os.environ, **self._env_vars, **(env or {})}
        try:
            proc = await asyncio.create_subprocess_exec(
                _python_executable(),
                code_file,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=self._host_dir,
                env=run_env,
            )
            stdout_b, stderr_b = await asyncio.wait_for(
                proc.communicate(), timeout=timeout
            )
            stdout = (stdout_b or b"").decode("utf-8", errors="replace")
            stderr = (stderr_b or b"").decode("utf-8", errors="replace")

            artifacts, clean_stdout = extract_artifacts(stdout)
            return CodeRunResult(
                stdout=clean_stdout,
                stderr=stderr,
                exit_code=proc.returncode or 0,
                artifacts=artifacts,
            )
        except asyncio.TimeoutError:
            return CodeRunResult(
                stdout="", stderr="Execution timed out", exit_code=-1, artifacts=[]
            )
        finally:
            with contextlib.suppress(OSError):
                os.unlink(code_file)

    # ============================================================
    # File I/O
    # ============================================================

    async def upload_file(self, content: bytes, dest_path: str) -> None:
        self._check_running()
        full_path = self._resolve(dest_path)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        with open(full_path, "wb") as f:
            f.write(content)

    async def upload_files(
        self, files: list[tuple[bytes | str, str]]
    ) -> None:
        self._check_running()
        for source, dest in files:
            if isinstance(source, str):
                with open(source, "rb") as f:
                    content = f.read()
            else:
                content = source
            await self.upload_file(content, dest)

    async def download_file(self, path: str) -> bytes:
        self._check_running()
        full_path = self._resolve(path)
        if not os.path.exists(full_path):
            raise FileNotFoundError(f"File not found: {path}")
        with open(full_path, "rb") as f:
            return f.read()

    async def list_files(self, directory: str) -> list[dict[str, Any]]:
        self._check_running()
        full_path = self._resolve(directory)
        if not os.path.isdir(full_path):
            return []
        entries = []
        for name in sorted(os.listdir(full_path)):
            entry_path = os.path.join(full_path, name)
            entries.append(
                _FileEntry(name=name, is_dir=os.path.isdir(entry_path))
            )
        return entries

    # ============================================================
    # Sessions (background commands)
    # ============================================================
    # Each session_id maps to a directory under {work_dir}/.sessions/{id}/.
    # Each command is one file: cmd_{cmd_id}.{stdout,stderr,status}.

    async def create_session(self, session_id: str) -> None:
        if not _SESSION_ID_RE.match(session_id):
            raise ValueError(f"Invalid session_id: {session_id!r}")
        sdir = os.path.join(self._host_dir, ".sessions", session_id)
        if os.path.isdir(sdir):
            raise RuntimeError(f"Session already exists: {session_id}")
        os.makedirs(sdir, exist_ok=True)

    async def session_execute(
        self,
        session_id: str,
        command: str,
        *,
        run_async: bool = False,
        timeout: int | None = None,
    ) -> SessionCommandResult:
        if not _SESSION_ID_RE.match(session_id):
            raise ValueError(f"Invalid session_id: {session_id!r}")
        sdir = os.path.join(self._host_dir, ".sessions", session_id)
        if not os.path.isdir(sdir):
            raise RuntimeError(f"Session not found: {session_id}")

        cmd_id = uuid.uuid4().hex[:12]
        stdout_path = os.path.join(sdir, f"cmd_{cmd_id}.stdout")
        stderr_path = os.path.join(sdir, f"cmd_{cmd_id}.stderr")
        status_path = os.path.join(sdir, f"cmd_{cmd_id}.status")

        # Touch files so concurrent reads don't fail
        Path(stdout_path).touch()
        Path(stderr_path).touch()

        async def _runner():
            try:
                with open(stdout_path, "wb") as out_f, open(stderr_path, "wb") as err_f:
                    proc = await asyncio.create_subprocess_shell(
                        self._rewrite_command(command),
                        stdout=out_f,
                        stderr=err_f,
                        cwd=self._host_dir,
                        env={**os.environ, **self._env_vars},
                    )
                    rc = await proc.wait()
            except Exception as e:
                rc = -1
                with open(stderr_path, "ab") as err_f:
                    err_f.write(f"\n[exec error] {e}".encode())
            with open(status_path, "w") as f:
                f.write(str(rc))

        if run_async:
            asyncio.create_task(_runner())
            return SessionCommandResult(
                cmd_id=cmd_id, exit_code=None, stdout="", stderr=""
            )

        try:
            await asyncio.wait_for(_runner(), timeout=timeout)
        except asyncio.TimeoutError:
            with open(status_path, "w") as f:
                f.write("-1")
            return SessionCommandResult(
                cmd_id=cmd_id, exit_code=-1, stdout="", stderr="timeout"
            )

        return await self.session_command_logs(session_id, cmd_id)

    async def session_command_logs(
        self, session_id: str, command_id: str
    ) -> SessionCommandResult:
        if not _SESSION_ID_RE.match(session_id):
            raise ValueError(f"Invalid session_id: {session_id!r}")
        sdir = os.path.join(self._host_dir, ".sessions", session_id)
        stdout_path = os.path.join(sdir, f"cmd_{command_id}.stdout")
        stderr_path = os.path.join(sdir, f"cmd_{command_id}.stderr")
        status_path = os.path.join(sdir, f"cmd_{command_id}.status")

        stdout = (
            Path(stdout_path).read_text(errors="replace")
            if os.path.exists(stdout_path)
            else ""
        )
        stderr = (
            Path(stderr_path).read_text(errors="replace")
            if os.path.exists(stderr_path)
            else ""
        )
        exit_code: int | None = None
        if os.path.exists(status_path):
            try:
                exit_code = int(Path(status_path).read_text().strip())
            except ValueError:
                exit_code = -1
        return SessionCommandResult(
            cmd_id=command_id,
            exit_code=exit_code,
            stdout=stdout,
            stderr=stderr,
        )

    async def delete_session(self, session_id: str) -> None:
        if not _SESSION_ID_RE.match(session_id):
            raise ValueError(f"Invalid session_id: {session_id!r}")
        sdir = os.path.join(self._host_dir, ".sessions", session_id)
        with contextlib.suppress(Exception):
            shutil.rmtree(sdir, ignore_errors=True)

    # ============================================================
    # Preview URLs (NOT supported in local mode)
    # ============================================================

    async def get_preview_url(
        self, port: int, expires_in: int = 3600
    ) -> PreviewInfo:
        raise NotImplementedError(
            "Preview URLs are not supported by the local sandbox provider"
        )

    async def get_preview_link(self, port: int) -> PreviewInfo:
        raise NotImplementedError(
            "Preview URLs are not supported by the local sandbox provider"
        )

    # ============================================================
    # Capabilities & metadata
    # ============================================================

    @property
    def capabilities(self) -> set[str]:
        return {"exec", "code_run", "file_io", "session", "archive"}

    async def get_metadata(self) -> dict[str, Any]:
        return {
            "id": self._id,
            "working_dir": self._host_dir,
            "state": self._state.value,
            "provider": "memory",
        }

    # ============================================================
    # Internal helpers
    # ============================================================

    def _check_running(self) -> None:
        if self._deleted:
            raise SandboxGoneError(self._id, "runtime has been deleted")
        if self._state != RuntimeState.RUNNING:
            raise SandboxTransientError(
                f"Runtime is not running (state={self._state.value})"
            )

    def _rewrite_command(self, command: str) -> str:
        """Rewrite shell command for the local host filesystem.

        PTCSandbox 上层会把 ``cd /home/workspace`` / 绝对虚拟路径写到命令里，
        但本机磁盘上根本没有 ``/home/workspace`` 这个目录。这里把命令字符串里
        所有出现的虚拟根（``/home/workspace`` 及其子路径）替换为真实 host_dir，
        让 subprocess 能正确 cd/读文件。

        Additionally handles base64-encoded Python code (used by aglob_files etc.)
        where the virtual path is embedded inside the encoded payload — plain
        string replacement on the outer command wouldn't work.

        Examples:
          ``cd /home/workspace && pwd`` → ``cd <host_dir> && pwd``
          ``python /home/workspace/foo.py`` → ``python <host_dir>/foo.py``
        """
        if not command:
            return command
        import base64 as _b64

        vwd = self._virtual_work_dir.rstrip("/")

        # Handle base64-encoded exec patterns:
        #   python3 -c "import base64; exec(base64.b64decode('...').decode())"
        # Decode, rewrite paths inside, re-encode.
        if "base64.b64decode(" in command and vwd not in command:
            import re as _re
            def _rewrite_b64(m: "re.Match[str]") -> str:
                encoded = m.group(1)
                try:
                    decoded = _b64.b64decode(encoded).decode("utf-8")
                except Exception:
                    return m.group(0)
                if vwd in decoded:
                    decoded = decoded.replace(vwd + "/", self._host_dir + "/")
                    decoded = decoded.replace(vwd, self._host_dir)
                    new_encoded = _b64.b64encode(decoded.encode("utf-8")).decode()
                    return m.group(0).replace(encoded, new_encoded)
                return m.group(0)

            command = _re.sub(r"b64decode\('([A-Za-z0-9+/=]+)'\)", _rewrite_b64, command)
            command = _re.sub(r'b64decode\("([A-Za-z0-9+/=]+)"\)', _rewrite_b64, command)

        # Standard plain-text replacement.
        if vwd in command:
            command = command.replace(vwd + "/", self._host_dir + "/")
            command = command.replace(vwd, self._host_dir)
        return command

    def _revirtualize_output(self, output: str) -> str:
        """Replace real host paths back to virtual sandbox paths in command output.

        This ensures that paths printed by subprocess commands (e.g. glob results)
        appear as virtual paths (/home/workspace/...) which the upper layer
        (ptc_sandbox.virtualize_path) expects.
        """
        if not output or self._host_dir not in output:
            return output
        vwd = self._virtual_work_dir.rstrip("/")
        output = output.replace(self._host_dir + "/", vwd + "/")
        output = output.replace(self._host_dir, vwd)
        return output

    def _resolve(self, path: str) -> str:
        """Map a sandbox-side path to a real host filesystem path.

        Mapping rules (虚拟 → 真实):
          1. ``/home/workspace`` 或 ``/home/workspace/...`` → ``host_dir/...``
          2. ``/tmp`` 或 ``/tmp/...`` → 透传（subprocess 可以直接用 /tmp）
          3. 已经在 ``host_dir`` 下的绝对路径 → 原样返回
          4. 其他绝对路径 → 把前导 "/" 去掉，作为相对 host_dir 处理
             （兜底，让 agent 即便用了非常规 sandbox 路径也能落到 host_dir）
          5. 相对路径 → 拼到 ``host_dir`` 下
        """
        if not path:
            return self._host_dir

        # 透传 /tmp（subprocess 直接用）
        if path == "/tmp" or path.startswith("/tmp/"):
            return path

        # 虚拟 sandbox 根：/home/workspace 及其子路径 → host_dir
        vwd = self._virtual_work_dir.rstrip("/")
        if path == vwd or path == vwd + "/":
            return self._host_dir
        if path.startswith(vwd + "/"):
            rel = path[len(vwd) + 1:]
            return os.path.join(self._host_dir, rel)

        if os.path.isabs(path):
            try:
                # 已经是真实 host_dir 内部的绝对路径
                if os.path.commonpath(
                    [os.path.abspath(path), self._host_dir]
                ) == self._host_dir:
                    return path
            except ValueError:
                pass
            # 兜底：把前导 / 去掉，相对 host_dir
            return os.path.join(self._host_dir, path.lstrip("/"))

        # 相对路径
        return os.path.join(self._host_dir, path)

    def _persist_state(self) -> None:
        try:
            state_file = os.path.join(self._host_dir, ".runtime_state.json")
            with open(state_file, "w") as f:
                json.dump(
                    {"state": self._state.value, "id": self._id}, f
                )
        except Exception:
            pass

    @classmethod
    def restore(
        cls,
        runtime_id: str,
        host_dir: str,
        env_vars: dict[str, str] | None = None,
    ) -> "LocalRuntime":
        """Reconstruct a runtime from a persisted directory."""
        rt = cls(runtime_id, host_dir, env_vars)
        # Try to read stored state
        try:
            state_file = os.path.join(host_dir, ".runtime_state.json")
            if os.path.exists(state_file):
                data = json.loads(Path(state_file).read_text())
                rt._state = RuntimeState(data.get("state", "running"))
        except Exception:
            pass
        return rt


class LocalProvider(SandboxProvider):
    """Provider that creates and reconnects to LocalRuntime instances.

    Runtimes persist on disk under ``base_dir`` so they survive server
    restarts. Use ``provider.get(runtime_id)`` to reconnect.
    """

    def __init__(self, base_dir: str | None = None) -> None:
        # Priority: explicit arg > env var > project-local > global default
        env_dir = os.getenv("LANGALPHA_LOCAL_SANDBOX_DIR")
        chosen = base_dir or env_dir or str(_find_project_sandbox_dir())
        self._base_dir = chosen
        os.makedirs(self._base_dir, exist_ok=True)
        self._runtimes: dict[str, LocalRuntime] = {}
        self._closed = False

    async def create(
        self,
        *,
        env_vars: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> SandboxRuntime:
        if self._closed:
            raise RuntimeError("Provider is closed")
        runtime_id = f"local-{uuid.uuid4().hex[:12]}"
        host_dir = os.path.join(self._base_dir, runtime_id)
        runtime = LocalRuntime(runtime_id, host_dir, env_vars)
        self._runtimes[runtime_id] = runtime
        return runtime

    async def get(self, sandbox_id: str) -> SandboxRuntime:
        if self._closed:
            raise RuntimeError("Provider is closed")
        # In-memory cache hit
        rt = self._runtimes.get(sandbox_id)
        if rt is not None and not rt._deleted:
            return rt

        # Try to restore from disk (server restart scenario)
        host_dir = os.path.join(self._base_dir, sandbox_id)
        if os.path.isdir(host_dir):
            rt = LocalRuntime.restore(sandbox_id, host_dir)
            self._runtimes[sandbox_id] = rt
            return rt

        raise SandboxGoneError(sandbox_id, "runtime directory not found on disk")

    async def close(self) -> None:
        self._closed = True

    def is_transient_error(self, exc: Exception) -> bool:
        # No network involved → almost no transient errors. Be conservative.
        if isinstance(exc, SandboxTransientError):
            return True
        msg = str(exc).lower()
        return "timeout" in msg or "transient" in msg
