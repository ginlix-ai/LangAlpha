"""Docker sandbox provider — runs sandboxes as local Docker containers.

Uses ``aiodocker`` for all container operations. Two file-I/O modes are
supported:

* **tar mode** (default) -- files are transferred via the Docker
  ``put_archive`` / ``get_archive`` API.
* **bind-mount mode** (``dev_mode=True``) -- a host directory is mounted
  into the container so the host filesystem can be used directly.
"""

from __future__ import annotations

import asyncio
import base64
import io
import os
import re
import shlex
import tarfile
import uuid
from typing import Any

import structlog

from ptc_agent.config.core import DockerConfig
from ptc_agent.core.sandbox.providers._chart_capture import (
    build_code_wrapper,
    extract_artifacts,
)
from ptc_agent.core.sandbox.runtime import (
    CodeRunResult,
    ExecResult,
    PreviewInfo,
    RuntimeState,
    SandboxProvider,
    SandboxRuntime,
    SandboxTransientError,
    SessionCommandResult,
)

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# State mapping
# ---------------------------------------------------------------------------
_DOCKER_STATE_MAP: dict[str, RuntimeState] = {
    "running": RuntimeState.RUNNING,
    "created": RuntimeState.STOPPED,
    "exited": RuntimeState.STOPPED,
    "paused": RuntimeState.STOPPED,
    "dead": RuntimeState.ERROR,
    "restarting": RuntimeState.STARTING,
    "removing": RuntimeState.STOPPING,
}


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_SESSION_ID_RE = re.compile(r"^[a-zA-Z0-9_-]+$")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_proxy_port_range(range_str: str) -> list[int]:
    """Parse a port range string like ``"13000-13009"`` into a list of ints."""
    parts = range_str.strip().split("-")
    if len(parts) != 2:
        raise ValueError(f"Invalid proxy port range: {range_str!r} (expected 'START-END')")
    start, end = int(parts[0]), int(parts[1])
    if start > end:
        raise ValueError(f"Invalid proxy port range: start ({start}) > end ({end})")
    return list(range(start, end + 1))


def _parse_memory(limit_str: str) -> int:
    """Convert a human-friendly memory string (e.g. ``"4g"``) to bytes."""
    limit_str = limit_str.strip().lower()
    match = re.fullmatch(r"(\d+(?:\.\d+)?)\s*([kmgt])?b?", limit_str)
    if not match:
        raise ValueError(f"Cannot parse memory limit: {limit_str!r}")
    value = float(match.group(1))
    suffix = match.group(2) or ""
    multipliers = {"": 1, "k": 1024, "m": 1024**2, "g": 1024**3, "t": 1024**4}
    return int(value * multipliers[suffix])


# ---------------------------------------------------------------------------
# DockerRuntime
# ---------------------------------------------------------------------------


class DockerRuntime(SandboxRuntime):
    """Runtime that wraps a single Docker container."""

    def __init__(
        self,
        container: Any,  # aiodocker.containers.DockerContainer
        *,
        runtime_id: str,
        working_dir: str = "/home/sandbox",
        dev_mode: bool = False,
        host_work_dir: str | None = None,
        proxy_ports: list[int] | None = None,
        preview_base_url: str | None = None,
    ) -> None:
        self._container = container
        self._id = runtime_id
        self._working_dir = working_dir
        self._dev_mode = dev_mode
        self._host_work_dir = host_work_dir
        self._preview_base_url = preview_base_url
        self._proxy_ports: list[int] = proxy_ports or []
        self._port_map: dict[int, int] = {}  # agent port → proxy port
        self._forwarder_pids: dict[int, str] = {}  # proxy port → socat PID

    # -- Properties --

    @property
    def id(self) -> str:
        return self._id

    @property
    def working_dir(self) -> str:
        return self._working_dir

    async def fetch_working_dir(self) -> str:
        return self._working_dir

    # -- Lifecycle --

    async def start(self, timeout: int = 120) -> None:
        await self._container.start()

    async def stop(self, timeout: int = 60) -> None:
        await self._container.stop(t=timeout)

    async def delete(self) -> None:
        try:
            await self._container.stop(t=5)
        except Exception:
            pass
        await self._container.delete(force=True)

    async def get_state(self) -> RuntimeState:
        info = await self._container.show()
        status = info.get("State", {}).get("Status", "unknown")
        return _DOCKER_STATE_MAP.get(status, RuntimeState.ERROR)

    # -- Execution --

    async def exec(self, command: str, timeout: int = 60) -> ExecResult:
        try:
            exec_obj = await self._container.exec(
                cmd=["bash", "-c", command],
                workdir=self._working_dir,
            )
            # Read all output from the multiplexed stream
            stdout_parts: list[str] = []

            async def _read_stream() -> None:
                async with exec_obj.start() as stream:
                    while True:
                        msg = await stream.read_out()
                        if msg is None:
                            break
                        stdout_parts.append(msg.data.decode("utf-8", errors="replace"))

            await asyncio.wait_for(_read_stream(), timeout=timeout)

            combined = "".join(stdout_parts)

            # Get exit code
            inspect = await exec_obj.inspect()
            exit_code = inspect.get("ExitCode", -1)

            return ExecResult(stdout=combined, stderr="", exit_code=exit_code)
        except asyncio.TimeoutError:
            return ExecResult(stdout="", stderr="timeout", exit_code=-1)
        except Exception as e:
            if _is_container_gone(e):
                raise SandboxTransientError(str(e)) from e
            return ExecResult(stdout="", stderr=str(e), exit_code=-1)

    async def code_run(
        self,
        code: str,
        env: dict[str, str] | None = None,
        timeout: int = 300,
    ) -> CodeRunResult:
        wrapper = build_code_wrapper(code)
        encoded = base64.b64encode(wrapper.encode("utf-8")).decode("ascii")

        script_name = f"_exec_{uuid.uuid4().hex[:8]}.py"
        script_path = f"{self._working_dir}/{script_name}"

        # Write script via exec (avoids tar overhead for a single small file)
        write_cmd = (
            f"python3 -c \"import base64,sys; "
            f"sys.stdout.buffer.write(base64.b64decode('{encoded}'))\" "
            f"> {script_path}"
        )
        write_result = await self.exec(write_cmd, timeout=15)
        if write_result.exit_code != 0:
            return CodeRunResult(
                stdout="",
                stderr=f"Failed to write script: {write_result.stderr or write_result.stdout}",
                exit_code=write_result.exit_code,
                artifacts=[],
            )

        # Build environment exports
        env_prefix = ""
        if env:
            exports = " ".join(f"{k}={_shell_escape(v)}" for k, v in env.items())
            env_prefix = f"export {exports} && "

        stderr_path = f"{self._working_dir}/_stderr_{uuid.uuid4().hex[:8]}.txt"
        run_cmd = f"{env_prefix}python3 {script_path} 2>{stderr_path}"

        # Run the code — we parse combined stdout for artifacts
        exec_result = await self.exec(run_cmd, timeout=timeout)

        # Extract chart artifacts from stdout markers
        artifacts, clean_stdout = extract_artifacts(exec_result.stdout)

        # Read stderr from temp file only on failure (avoids extra exec round-trip
        # on the happy path — the consumer only needs stderr for auto-install detection)
        stderr = ""
        if exec_result.exit_code != 0:
            try:
                cat_result = await self.exec(f"cat {stderr_path} 2>/dev/null", timeout=5)
                if cat_result.exit_code == 0:
                    stderr = cat_result.stdout
            except Exception:
                pass

        # Cleanup temp files (best-effort)
        try:
            await self.exec(f"rm -f {script_path} {stderr_path}", timeout=5)
        except Exception:
            pass

        return CodeRunResult(
            stdout=clean_stdout,
            stderr=stderr,
            exit_code=exec_result.exit_code,
            artifacts=artifacts,
        )

    # -- File I/O --

    async def upload_file(self, content: bytes, dest_path: str) -> None:
        if self._dev_mode and self._host_work_dir:
            self._host_write(dest_path, content)
        else:
            await self._tar_upload(dest_path, content)

    async def upload_files(self, files: list[tuple[bytes | str, str]]) -> None:
        if self._dev_mode and self._host_work_dir:
            for source, dest in files:
                data = _read_source(source)
                self._host_write(dest, data)
            return

        # Tar mode: batch all files into a single tar + one put_archive call.
        prepared: list[tuple[bytes, str]] = []
        parent_dirs: set[str] = set()
        for source, dest in files:
            data = _read_source(source)
            prepared.append((data, dest))
            parent = os.path.dirname(dest)
            if parent:
                parent_dirs.add(parent)

        if not prepared:
            return

        # Best-effort mkdir for parent dirs (tar extraction at "/" should
        # create them anyway, but this avoids permission issues).
        if parent_dirs:
            mkdir_cmd = "mkdir -p " + " ".join(
                f"'{d}'" for d in sorted(parent_dirs)
            )
            await self.exec(mkdir_cmd, timeout=30)

        # Build one tar archive with full absolute paths.
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w") as tar:
            for data, dest_path in prepared:
                tar_name = dest_path.lstrip("/")
                info = tarfile.TarInfo(name=tar_name)
                info.size = len(data)
                info.mode = 0o644
                tar.addfile(info, io.BytesIO(data))
        buf.seek(0)

        await self._container.put_archive("/", buf.read())

    async def download_file(self, path: str) -> bytes:
        if self._dev_mode and self._host_work_dir:
            return self._host_read(path)
        return await self._exec_download(path)

    async def list_files(self, directory: str) -> list[dict[str, Any]]:
        resolved = directory if os.path.isabs(directory) else f"{self._working_dir}/{directory}"
        result = await self.exec(
            f"find {shlex.quote(resolved)} -maxdepth 1 -mindepth 1 -printf '%f\\t%y\\n' 2>/dev/null || "
            f"ls -1 {shlex.quote(resolved)} 2>/dev/null",
            timeout=15,
        )
        entries: list[dict[str, Any]] = []
        if result.exit_code != 0 or not result.stdout.strip():
            return entries
        for line in result.stdout.strip().split("\n"):
            if "\t" in line:
                name, ftype = line.split("\t", 1)
                entries.append({"name": name, "is_dir": ftype == "d"})
            else:
                entries.append({"name": line.strip(), "is_dir": False})
        return entries

    # -- Preview URLs --

    async def _allocate_proxy_port(self, target_port: int) -> int:
        """Allocate a proxy port and start socat forwarding to the target port."""
        if target_port in self._port_map:
            return self._port_map[target_port]

        used = set(self._port_map.values())
        free = [p for p in self._proxy_ports if p not in used]
        if not free:
            raise RuntimeError(
                f"No free proxy ports. All {len(self._proxy_ports)} ports in use. "
                f"Increase preview_proxy_ports range in config."
            )
        proxy_port = free[0]

        # Reserve immediately to prevent concurrent coroutines from picking
        # the same proxy port (multiple awaits follow).
        self._port_map[target_port] = proxy_port
        self._forwarder_pids[proxy_port] = ""  # mark in-progress

        try:
            # Write a minimal Python TCP proxy as fallback when socat is unavailable
            proxy_script = (
                f"import socket as S,threading as T\n"
                f"def f(a,b):\n"
                f" try:\n"
                f"  while True:\n"
                f"   d=a.recv(4096)\n"
                f"   if not d:break\n"
                f"   b.sendall(d)\n"
                f" except:pass\n"
                f" finally:a.close();b.close()\n"
                f"s=S.socket();s.setsockopt(S.SOL_SOCKET,S.SO_REUSEADDR,1)\n"
                f"s.bind(('0.0.0.0',{proxy_port}));s.listen(5)\n"
                f"while True:\n"
                f" c,_=s.accept();d=S.socket();d.connect(('127.0.0.1',{target_port}))\n"
                f" T.Thread(target=f,args=(c,d),daemon=True).start()\n"
                f" T.Thread(target=f,args=(d,c),daemon=True).start()\n"
            )
            encoded_script = base64.b64encode(proxy_script.encode()).decode("ascii")
            proxy_path = f"/tmp/.proxy_{proxy_port}.py"
            await self.exec(
                f"echo {encoded_script} | base64 -d > {proxy_path}", timeout=5
            )

            # Kill any stale forwarder on this proxy port (survives Python-side
            # reconnects while the container stays running).
            await self.exec(
                f"fuser -k {proxy_port}/tcp 2>/dev/null || true", timeout=5
            )

            # Start socat forwarder; fall back to Python proxy if socat is not installed
            result = await self.exec(
                f"nohup bash -c 'socat TCP-LISTEN:{proxy_port},fork,reuseaddr "
                f"TCP:localhost:{target_port} 2>/dev/null "
                f"|| python3 {proxy_path}' > /dev/null 2>&1 & echo $!",
                timeout=10,
            )
            pid = result.stdout.strip()

            # Verify the forwarder is actually listening
            check = await self.exec(
                f"sleep 0.2 && fuser {proxy_port}/tcp >/dev/null 2>&1", timeout=5
            )
            if check.exit_code != 0:
                logger.warning(
                    "Proxy forwarder may not have started",
                    proxy_port=proxy_port,
                    target_port=target_port,
                    pid=pid,
                )

            self._forwarder_pids[proxy_port] = pid
        except Exception:
            # Roll back reservation so the port is available for retry
            del self._port_map[target_port]
            del self._forwarder_pids[proxy_port]
            raise

        logger.info(
            "Proxy port allocated",
            target_port=target_port,
            proxy_port=proxy_port,
            pid=pid,
        )
        return proxy_port

    async def get_preview_url(self, port: int, expires_in: int = 3600) -> PreviewInfo:
        proxy_port = await self._allocate_proxy_port(port)
        base = (self._preview_base_url or "http://localhost").rstrip("/")
        return PreviewInfo(url=f"{base}:{proxy_port}", token="")

    async def get_preview_link(self, port: int) -> PreviewInfo:
        proxy_port = await self._allocate_proxy_port(port)
        # Server-side: resolve host for health checks (may differ from browser URL)
        base = (self._preview_base_url or await self._resolve_server_side_host()).rstrip("/")
        return PreviewInfo(url=f"{base}:{proxy_port}", token="")

    _server_side_host: str | None = None  # class-level cache

    @classmethod
    async def _resolve_server_side_host(cls) -> str:
        """Resolve host for server-side requests to published proxy ports.

        When the backend itself runs inside Docker, ``localhost`` points to the
        backend container, not the host where sandbox ports are published.
        Docker Desktop exposes ``host.docker.internal`` for this purpose.

        Result is cached at the class level — it won't change during process
        lifetime and avoids repeated DNS lookups.
        """
        if cls._server_side_host is not None:
            return cls._server_side_host

        import asyncio

        loop = asyncio.get_running_loop()
        try:
            await loop.getaddrinfo("host.docker.internal", None)
            cls._server_side_host = "http://host.docker.internal"
        except OSError:
            cls._server_side_host = "http://localhost"
        return cls._server_side_host

    # -- Sessions --

    async def create_session(self, session_id: str) -> None:
        if not _SESSION_ID_RE.match(session_id):
            raise ValueError(f"Invalid session_id: {session_id!r}")
        check = await self.exec(f"test -d /tmp/.sessions/{session_id}", timeout=5)
        if check.exit_code == 0:
            raise RuntimeError(f"Session already exists: {session_id}")
        await self.exec(f"mkdir -p /tmp/.sessions/{session_id}", timeout=5)

    async def session_execute(
        self,
        session_id: str,
        command: str,
        *,
        run_async: bool = False,
        timeout: int = 60,
    ) -> SessionCommandResult:
        if not _SESSION_ID_RE.match(session_id):
            raise ValueError(f"Invalid session_id: {session_id!r}")
        cmd_id = uuid.uuid4().hex[:8]
        session_dir = f"/tmp/.sessions/{session_id}"

        if run_async:
            encoded = base64.b64encode(command.encode()).decode("ascii")
            wrapped = (
                f"nohup bash -c '"
                f"echo $$ > {session_dir}/{cmd_id}.pid; "
                f"eval \"$(echo {encoded} | base64 -d)\"; "
                f"echo $? > {session_dir}/{cmd_id}.exit"
                f"' > {session_dir}/{cmd_id}.stdout 2> {session_dir}/{cmd_id}.stderr &\n"
                f"echo $!"
            )
            await self.exec(wrapped, timeout=10)
            return SessionCommandResult(cmd_id=cmd_id, exit_code=None, stdout="", stderr="")

        result = await self.exec(command, timeout=timeout)
        return SessionCommandResult(
            cmd_id=cmd_id,
            exit_code=result.exit_code,
            stdout=result.stdout,
            stderr=result.stderr,
        )

    async def session_command_logs(
        self, session_id: str, command_id: str
    ) -> SessionCommandResult:
        if not _SESSION_ID_RE.match(session_id):
            raise ValueError(f"Invalid session_id: {session_id!r}")
        if not _SESSION_ID_RE.match(command_id):
            raise ValueError(f"Invalid command_id: {command_id!r}")
        session_dir = f"/tmp/.sessions/{session_id}"
        # Single exec round-trip: emit all three files separated by a null byte
        _SEP = "\\x00"
        result = await self.exec(
            f"cat {session_dir}/{command_id}.stdout 2>/dev/null; "
            f"printf '{_SEP}'; "
            f"cat {session_dir}/{command_id}.stderr 2>/dev/null; "
            f"printf '{_SEP}'; "
            f"cat {session_dir}/{command_id}.exit 2>/dev/null",
            timeout=5,
        )
        parts = result.stdout.split("\x00", 2)
        stdout = parts[0] if len(parts) > 0 else ""
        stderr = parts[1] if len(parts) > 1 else ""
        exit_str = parts[2].strip() if len(parts) > 2 else ""
        exit_code = int(exit_str) if exit_str.isdigit() else None
        return SessionCommandResult(
            cmd_id=command_id,
            exit_code=exit_code,
            stdout=stdout,
            stderr=stderr,
        )

    async def delete_session(self, session_id: str) -> None:
        if not _SESSION_ID_RE.match(session_id):
            raise ValueError(f"Invalid session_id: {session_id!r}")
        session_dir = f"/tmp/.sessions/{session_id}"
        await self.exec(
            f"for f in {session_dir}/*.pid; do "
            f"  pid=$(cat \"$f\" 2>/dev/null | tr -cd '0-9'); "
            f"  [ -n \"$pid\" ] && kill \"$pid\" 2>/dev/null; "
            f"done; rm -rf {session_dir}",
            timeout=10,
        )

    # -- Capabilities & metadata --

    @property
    def capabilities(self) -> set[str]:
        return {"exec", "code_run", "file_io", "preview_url", "sessions"}

    async def archive(self) -> None:
        raise NotImplementedError("Docker provider does not support archive")

    async def get_metadata(self) -> dict[str, Any]:
        state = await self.get_state()
        return {
            "id": self._id,
            "working_dir": self._working_dir,
            "state": state.value,
            "dev_mode": self._dev_mode,
            "provider": "docker",
        }

    # -- Internal: tar-based file I/O --

    async def _tar_upload(self, dest_path: str, content: bytes) -> None:
        """Upload a single file into the container via a tar archive.

        Uses ``put_archive("/", ...)`` with the full path encoded in the tar
        entry so we don't depend on the parent directory already existing.
        """
        parent = os.path.dirname(dest_path)
        if parent:
            await self.exec(f"mkdir -p {shlex.quote(parent)}", timeout=10)

        buf = io.BytesIO()
        tar_name = dest_path.lstrip("/")
        with tarfile.open(fileobj=buf, mode="w") as tar:
            info = tarfile.TarInfo(name=tar_name)
            info.size = len(content)
            info.mode = 0o644
            tar.addfile(info, io.BytesIO(content))
        buf.seek(0)

        await self._container.put_archive("/", buf.read())

    async def _exec_download(self, path: str) -> bytes:
        """Download a file via exec + base64 (avoids Docker archive API issues)."""
        result = await self.exec(
            f"test -f {shlex.quote(path)} && base64 {shlex.quote(path)}", timeout=60,
        )
        if result.exit_code != 0:
            raise FileNotFoundError(f"File not found or unreadable: {path}")
        return base64.b64decode(result.stdout)

    # -- Internal: bind-mount file I/O --

    def _host_resolve(self, sandbox_path: str) -> str:
        """Map a sandbox path to a host filesystem path."""
        assert self._host_work_dir is not None
        if sandbox_path.startswith(self._working_dir + "/"):
            relative = sandbox_path[len(self._working_dir) + 1 :]
        elif sandbox_path.startswith(self._working_dir):
            relative = sandbox_path[len(self._working_dir) :]
        else:
            relative = sandbox_path.lstrip("/")
        return os.path.join(self._host_work_dir, relative)

    def _host_write(self, sandbox_path: str, content: bytes) -> None:
        host_path = self._host_resolve(sandbox_path)
        os.makedirs(os.path.dirname(host_path), exist_ok=True)
        with open(host_path, "wb") as f:
            f.write(content)

    def _host_read(self, sandbox_path: str) -> bytes:
        host_path = self._host_resolve(sandbox_path)
        if not os.path.exists(host_path):
            raise FileNotFoundError(f"File not found: {sandbox_path}")
        with open(host_path, "rb") as f:
            return f.read()


# ---------------------------------------------------------------------------
# DockerProvider
# ---------------------------------------------------------------------------


class DockerProvider(SandboxProvider):
    """Provider that manages sandboxes as Docker containers."""

    def __init__(self, config: DockerConfig, working_dir: str | None = None) -> None:
        self._config = config
        # filesystem.working_directory is the single source of truth;
        # fall back to DockerConfig.working_dir only if not provided.
        self._working_dir = working_dir or config.working_dir
        self._client: Any | None = None  # aiodocker.Docker (lazy)

    async def _get_client(self) -> Any:
        """Return the aiodocker client, creating it lazily."""
        if self._client is None:
            import aiodocker

            self._client = aiodocker.Docker()
        return self._client

    # -- SandboxProvider interface --

    async def create(
        self,
        *,
        env_vars: dict[str, str] | None = None,
        mcp_packages: list[str] | None = None,
        **kwargs: Any,
    ) -> DockerRuntime:
        client = await self._get_client()
        await self._ensure_image(client)

        runtime_id = f"docker-{uuid.uuid4().hex[:12]}"
        container_name = f"langalpha-sandbox-{runtime_id}"

        # Parse proxy port pool for preview URL support
        proxy_ports = _parse_proxy_port_range(self._config.preview_proxy_ports)

        # Build container config
        host_config: dict[str, Any] = {
            "Memory": _parse_memory(self._config.memory_limit),
            "NanoCpus": int(self._config.cpu_count * 1e9),
            "NetworkMode": self._config.network_mode,
            "AutoRemove": False,  # We manage removal ourselves
            "Init": True,  # tini as PID 1 for zombie reaping
        }

        # Publish proxy ports so preview URLs are reachable from the host
        if proxy_ports:
            host_config["PortBindings"] = {
                f"{p}/tcp": [{"HostPort": str(p)}] for p in proxy_ports
            }

        binds: list[str] = list(self._config.volumes)  # extra user-defined mounts
        host_work_dir: str | None = None

        if self._config.dev_mode and self._config.host_work_dir:
            host_work_dir = self._config.host_work_dir
            os.makedirs(host_work_dir, exist_ok=True)
            binds.append(f"{host_work_dir}:{self._working_dir}")

        if binds:
            host_config["Binds"] = binds

        container_config: dict[str, Any] = {
            "Image": self._config.image,
            "Cmd": ["sleep", "infinity"],
            "WorkingDir": self._working_dir,
            "Hostname": "sandbox",
            "HostConfig": host_config,
        }

        if proxy_ports:
            container_config["ExposedPorts"] = {f"{p}/tcp": {} for p in proxy_ports}

        if env_vars:
            container_config["Env"] = [f"{k}={v}" for k, v in env_vars.items()]

        container_obj = await client.containers.create(
            config=container_config,
            name=container_name,
        )
        await container_obj.start()

        logger.info(
            "Docker container started",
            container_name=container_name,
            runtime_id=runtime_id,
            image=self._config.image,
        )

        runtime = DockerRuntime(
            container_obj,
            runtime_id=runtime_id,
            working_dir=self._working_dir,
            dev_mode=self._config.dev_mode,
            host_work_dir=host_work_dir,
            proxy_ports=proxy_ports,
            preview_base_url=self._config.preview_base_url,
        )

        # Install MCP npm packages if needed (mirrors Daytona snapshot behavior)
        if mcp_packages:
            pkgs = " ".join(mcp_packages)
            logger.info("Installing MCP packages in Docker container", packages=pkgs)
            result = await runtime.exec(f"npm install -g {pkgs}", timeout=120)
            if result.exit_code != 0:
                logger.warning(
                    "Failed to install MCP packages (npx will download on demand)",
                    packages=pkgs,
                    output=result.stdout,
                )

        return runtime

    async def get(self, sandbox_id: str) -> DockerRuntime:
        client = await self._get_client()
        container_name = f"langalpha-sandbox-{sandbox_id}"

        try:
            container_obj = await client.containers.get(container_name)
        except Exception as e:
            raise RuntimeError(
                f"Docker container not found: {container_name}"
            ) from e

        info = await container_obj.show()
        mounts = info.get("Mounts", [])

        # Detect dev_mode from existing bind mounts
        dev_mode = False
        host_work_dir = None
        for mount in mounts:
            if mount.get("Destination") == self._working_dir:
                dev_mode = True
                host_work_dir = mount.get("Source")
                break

        return DockerRuntime(
            container_obj,
            runtime_id=sandbox_id,
            working_dir=self._working_dir,
            dev_mode=dev_mode,
            host_work_dir=host_work_dir,
            proxy_ports=_parse_proxy_port_range(self._config.preview_proxy_ports),
            preview_base_url=self._config.preview_base_url,
        )

    async def close(self) -> None:
        if self._client is not None:
            try:
                await self._client.close()
            except Exception as e:
                logger.debug("Failed to close Docker client", error=str(e))
            finally:
                self._client = None

    def is_transient_error(self, exc: Exception) -> bool:
        msg = str(exc).lower()
        transient_markers = (
            "connection refused",
            "connection reset",
            "connection aborted",
            "broken pipe",
            "timed out",
            "timeout",
        )
        return any(marker in msg for marker in transient_markers)

    # -- Internal --

    async def _ensure_image(self, client: Any) -> None:
        """Ensure the sandbox image exists, auto-building from Dockerfile.sandbox if needed."""
        try:
            await client.images.inspect(self._config.image)
            logger.debug("Docker image found", image=self._config.image)
            return
        except Exception:
            pass  # Image not found -- try to build

        logger.info(
            "Docker image not found, attempting to build",
            image=self._config.image,
        )

        # Look for Dockerfile.sandbox relative to the repo root
        dockerfile_name = "Dockerfile.sandbox"
        # Try common locations
        search_paths = [
            os.path.join(os.getcwd(), dockerfile_name),
        ]
        # Also check relative to this file's location (up to repo root)
        module_dir = os.path.dirname(os.path.abspath(__file__))
        for _ in range(6):  # Walk up to 6 levels
            candidate = os.path.join(module_dir, dockerfile_name)
            search_paths.append(candidate)
            module_dir = os.path.dirname(module_dir)

        dockerfile_path = None
        for path in search_paths:
            if os.path.isfile(path):
                dockerfile_path = path
                break

        if dockerfile_path is None:
            raise RuntimeError(
                f"Docker image {self._config.image!r} not found and "
                f"{dockerfile_name} not found in any search path. "
                f"Build the image manually or place {dockerfile_name} in the repo root."
            )

        build_context = os.path.dirname(dockerfile_path)
        image_tag = self._config.image

        logger.info(
            "Building Docker sandbox image",
            dockerfile=dockerfile_path,
            tag=image_tag,
        )

        # Use aiodocker to build
        try:
            # aiodocker build expects a tar context or path
            async for log_line in client.images.build(
                path=build_context,
                dockerfile=os.path.basename(dockerfile_path),
                tag=image_tag,
                rm=True,
            ):
                if isinstance(log_line, dict) and "stream" in log_line:
                    line = log_line["stream"].strip()
                    if line:
                        logger.debug("Docker build", log=line)
                elif isinstance(log_line, dict) and "error" in log_line:
                    raise RuntimeError(f"Docker build failed: {log_line['error']}")
        except RuntimeError:
            raise
        except Exception as e:
            raise RuntimeError(
                f"Failed to build Docker image {image_tag!r}: {e}"
            ) from e

        logger.info("Docker sandbox image built", image=image_tag)


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


def _read_source(source: bytes | str) -> bytes:
    """Read file content from bytes or a local file path."""
    if isinstance(source, str):
        with open(source, "rb") as f:
            return f.read()
    return source


def _shell_escape(value: str) -> str:
    """Shell-escape a value for use in an export command."""
    return "'" + value.replace("'", "'\\''") + "'"


def _is_container_gone(exc: Exception) -> bool:
    """Check if an exception indicates the container no longer exists."""
    msg = str(exc).lower()
    return "no such container" in msg or "not found" in msg or "409" in msg
