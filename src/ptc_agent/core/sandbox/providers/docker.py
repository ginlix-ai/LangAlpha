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
import shlex
import tarfile
import time
import uuid
from collections.abc import AsyncIterator, Sequence
from typing import Any

import structlog

from ptc_agent.config.core import DockerConfig, PlatformSecretDefinition
from ptc_agent.core.sandbox._defaults import sandbox_thread_env
from ptc_agent.core.sandbox.providers._exec_sessions import ExecSessionRuntime
from ptc_agent.core.sandbox.providers._tiers import (
    TierSizing,
    applied_cpus,
    container_sizing,
    resolve_tier,
)
from ptc_agent.core.sandbox.runtime import (
    ExecResult,
    PreviewInfo,
    RuntimeState,
    SandboxProvider,
    SandboxGoneError,
    SandboxFailureKind,
    SandboxTransientError,
    STREAM_CHUNK_BYTES,
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


# The file mount (livefs) is FUSE, mounted by root through a root-only exec.
# The capabilities reach root processes only; the sandbox's own user runs
# without them. SYS_PTRACE lets that root daemon read a command's environment,
# which names the command a save belongs to; without it every save runs for no
# conversation. Docker's default AppArmor profile refuses every mount, so the
# container runs unconfined, and that applies to its user too.
_FUSE_HOST_CONFIG: dict[str, Any] = {
    "Devices": [
        {
            "PathOnHost": "/dev/fuse",
            "PathInContainer": "/dev/fuse",
            "CgroupPermissions": "rwm",
        }
    ],
    "CapAdd": ["SYS_ADMIN", "SYS_PTRACE"],
    "SecurityOpt": ["apparmor=unconfined"],
}


async def _remove_quietly(container: Any, name: str) -> None:
    try:
        await container.delete(force=True)
    except Exception as cleanup_error:
        logger.warning(
            "Failed to remove container after a failed start",
            container_name=name,
            error=str(cleanup_error),
        )


# ---------------------------------------------------------------------------
# DockerRuntime
# ---------------------------------------------------------------------------


class DockerRuntime(ExecSessionRuntime):
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
        host_port_map: dict[int, int] | None = None,
        preview_base_url: str | None = None,
    ) -> None:
        self._container = container
        self._id = runtime_id
        self._working_dir = working_dir
        self._dev_mode = dev_mode
        self._host_work_dir = host_work_dir
        self._preview_base_url = preview_base_url
        self._proxy_ports: list[int] = proxy_ports or []
        # container port → host port (for dynamic port publishing)
        self._host_port_map: dict[int, int] = host_port_map or {}
        self._port_map: dict[int, int] = {}  # agent port → container proxy port
        self._forwarder_pids: dict[int, str] = {}  # container proxy port → socat PID
        self._owner_id_cache: tuple[int, int] | None = None  # uid/gid inside the container

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

    async def stop(self, timeout: int = 60, *, force: bool = False) -> None:
        await self._container.stop(t=0 if force else timeout)

    async def delete(self) -> None:
        try:
            await self._container.stop(t=5)
        except Exception:
            pass
        await self._container.delete(force=True)

    @staticmethod
    def _state_from_inspect(info: dict[str, Any]) -> RuntimeState:
        """Shared by get_state and get_metadata so the unknown-status fallback stays one rule."""
        status = info.get("State", {}).get("Status", "unknown")
        return _DOCKER_STATE_MAP.get(status, RuntimeState.ERROR)

    async def get_state(self) -> RuntimeState:
        return self._state_from_inspect(await self._container.show())

    # -- Execution --

    async def exec(
        self, command: str, timeout: int = 60, *, env: dict[str, str] | None = None
    ) -> ExecResult:
        return await self._exec(command, timeout, env=env)

    async def exec_as_root(
        self, command: str, timeout: int = 60, env: dict[str, str] | None = None
    ) -> ExecResult:
        return await self._exec(command, timeout, user="root", env=env)

    async def _exec(
        self,
        command: str,
        timeout: int,
        user: str = "",
        env: dict[str, str] | None = None,
    ) -> ExecResult:
        try:
            exec_obj = await self._container.exec(
                cmd=["bash", "-c", command],
                workdir=self._working_dir,
                user=user,
                environment=env,
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
                shlex.quote(d) for d in sorted(parent_dirs)
            )
            await self.exec(mkdir_cmd, timeout=30)

        uid, gid = await self._owner_ids()
        await self._container.put_archive("/", _upload_tar(prepared, uid, gid))

    async def download_file(self, path: str) -> bytes:
        if self._dev_mode and self._host_work_dir:
            return self._host_read(path)
        return await self._exec_download(path)

    async def download_file_stream(self, path: str) -> AsyncIterator[bytes]:
        if not (self._dev_mode and self._host_work_dir):
            async for chunk in super().download_file_stream(path):
                yield chunk
            return
        host_path = self._host_resolve(path)
        with open(host_path, "rb") as f:
            while chunk := await asyncio.to_thread(f.read, STREAM_CHUNK_BYTES):
                yield chunk

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

    def _host_port_for(self, container_port: int) -> int:
        """Return the host-side port for a given container proxy port.

        When Docker publishes with dynamic host ports (``HostPort: ""``),
        the actual host port differs from the container port.  Falls back
        to the container port itself (static publishing or tests).
        """
        return self._host_port_map.get(container_port, container_port)

    async def get_preview_url(self, port: int, expires_in: int = 3600) -> PreviewInfo:
        proxy_port = await self._allocate_proxy_port(port)
        host_port = self._host_port_for(proxy_port)
        base = (self._preview_base_url or "http://localhost").rstrip("/")
        return PreviewInfo(url=f"{base}:{host_port}", token="")

    async def get_preview_link(self, port: int) -> PreviewInfo:
        proxy_port = await self._allocate_proxy_port(port)
        host_port = self._host_port_for(proxy_port)
        # Server-side: resolve host for health checks (may differ from browser URL)
        base = (self._preview_base_url or await self._resolve_server_side_host()).rstrip("/")
        return PreviewInfo(url=f"{base}:{host_port}", token="")

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

    # -- Capabilities & metadata --

    async def archive(self) -> None:
        raise NotImplementedError("Docker provider does not support archive")

    async def get_metadata(self) -> dict[str, Any]:
        """One inspect call serves state, limits and creation time.

        Limits read back as 0 on a container created outside this provider's
        config, so report nothing rather than a bogus zero. No ``disk`` key —
        docker sets no size quota, so there is no sandbox-scoped figure to give.
        """
        info = await self._container.show()
        host_config = info.get("HostConfig") or {}

        meta: dict[str, Any] = {
            "id": self._id,
            "working_dir": self._working_dir,
            "state": self._state_from_inspect(info).value,
            "dev_mode": self._dev_mode,
            "provider": "docker",
        }
        if created := info.get("Created"):
            meta["created_at"] = created
        if nano_cpus := host_config.get("NanoCpus"):
            meta["cpu"] = round(nano_cpus / 1e9, 2)
        if memory := host_config.get("Memory"):
            meta["memory"] = round(memory / 1024**3, 2)
        return meta

    # -- Internal: tar-based file I/O --

    async def _owner_ids(self) -> tuple[int, int]:
        """The uid/gid the container runs as, cached for the runtime's life.

        ``put_archive`` extracts with each tar entry's ownership, and an entry
        built here defaults to root. A file the container's own user does not
        own can be read but never chmod'd or utime'd, which is exactly what
        placing a restored workspace file has to do.
        """
        if self._owner_id_cache is None:
            res = await self.exec("id -u; id -g", timeout=10)
            try:
                uid, gid = (int(x) for x in res.stdout.split()[:2])
            except ValueError:
                logger.warning(
                    "Could not read the container's uid/gid; uploading as root",
                    runtime_id=self._id,
                )
                uid, gid = 0, 0
            self._owner_id_cache = (uid, gid)
        return self._owner_id_cache

    async def _tar_upload(self, dest_path: str, content: bytes) -> None:
        """Upload a single file into the container via a tar archive.

        Uses ``put_archive("/", ...)`` with the full path encoded in the tar
        entry so we don't depend on the parent directory already existing.
        """
        parent = os.path.dirname(dest_path)
        if parent:
            await self.exec(f"mkdir -p {shlex.quote(parent)}", timeout=10)

        uid, gid = await self._owner_ids()
        await self._container.put_archive(
            "/", _upload_tar([(content, dest_path)], uid, gid)
        )

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

    label = "Docker"
    runs_remote = False
    default_computer_name = "Local computer"
    # Commands run as the image's user, but a write goes through the daemon as
    # root, and in bind-mount mode a path outside the root lands in the host
    # folder rather than the container.
    files_as_command_user = False

    @classmethod
    def reports_own_disk(cls, settings: Any) -> bool:
        # Without a storage quota a container's disk is the host's.
        return bool(settings.storage_quota_enabled)

    def __init__(
        self,
        config: DockerConfig,
        working_dir: str | None = None,
        *,
        # A container holds no platform Secrets, so the catalog is not needed.
        platform_secrets: Sequence[PlatformSecretDefinition] = (),
    ) -> None:
        self._config = config
        # filesystem.working_directory is the single source of truth;
        # fall back to DockerConfig.working_dir only if not provided.
        self._working_dir = working_dir or config.working_dir
        self._client: Any | None = None  # aiodocker.Docker (lazy)
        self._storage_quota_notice_shown = False

    async def _get_client(self) -> Any:
        """Return the aiodocker client, creating it lazily."""
        if self._client is None:
            import aiodocker

            self._client = aiodocker.Docker()
        return self._client

    # -- SandboxProvider interface --

    def _resolve_tier(self, tier: str | None) -> TierSizing:
        """Resolve a tier name to this daemon's container limits."""
        return container_sizing(
            resolve_tier(
                tier,
                default_tier=self._config.default_tier,
                resource_tiers=self._config.resource_tiers,
            ),
            floor_cpus=self._config.cpu_count,
            floor_memory_limit=self._config.memory_limit,
        )

    def _apply_disk_quota(
        self, host_config: dict[str, Any], sizing: TierSizing
    ) -> None:
        """Size the container's writable layer when the storage driver allows it."""
        if sizing.disk_gib is None:
            return
        if self._config.storage_quota_enabled:
            host_config["StorageOpt"] = {"size": f"{sizing.disk_gib}G"}
            return
        # Per provider instance, not per process: a module flag is shared by
        # every runtime in the worker, so the second daemon's operator never
        # sees the notice.
        if not self._storage_quota_notice_shown:
            self._storage_quota_notice_shown = True
            logger.info(
                "Tier disk size not applied; enable docker.storage_quota_enabled "
                "on a daemon whose storage driver supports per-container quotas",
                tier=sizing.tier,
                disk_gib=sizing.disk_gib,
            )

    async def _verify_tier_applied(
        self, container_obj: Any, info: dict[str, Any], sizing: TierSizing
    ) -> None:
        """Fail an elevated tier whose limits the daemon dropped.

        Docker answers a limit its kernel cannot enforce with a warning on the
        create response rather than an error, so an unverified container can run
        at host size while the workspace is billed for a tier. Mirrors the Daytona
        provider's refusal to hand back a base-sized sandbox for an elevated tier.
        """
        host_config = info.get("HostConfig") or {}
        applied = applied_cpus(host_config)
        applied_memory = int(host_config.get("Memory") or 0)
        want_cpus = int(sizing.cpus * 1e9) / 1e9

        if (
            applied is not None
            and abs(applied - want_cpus) < 0.01
            and applied_memory == sizing.memory_bytes
        ):
            return

        raise RuntimeError(
            f"Docker did not apply the {sizing.tier!r} tier limits "
            f"(asked {want_cpus} vCPU / {sizing.memory_bytes} bytes, "
            f"got {applied} vCPU / {applied_memory} bytes); "
            "refusing to run an elevated tier at host size"
        )

    async def _start_new(
        self, client: Any, config: dict[str, Any], name: str, sizing: TierSizing
    ) -> Any:
        """Create and start a container; one that fails to start is removed."""
        try:
            container = await client.containers.create(config=config, name=name)
        except Exception as e:
            if sizing.elevated:
                host_config = config["HostConfig"]
                disk_note = (
                    f", {sizing.disk_gib}G disk" if "StorageOpt" in host_config else ""
                )
                raise RuntimeError(
                    f"Docker rejected the {sizing.tier!r} tier limits "
                    f"({sizing.cpus} vCPU, {sizing.memory_bytes} bytes{disk_note}): "
                    f"{e}; refusing to create a base-sized container for an elevated tier"
                ) from e
            raise
        try:
            await container.start()
        except BaseException:
            await _remove_quietly(container, name)
            raise
        return container

    async def create(
        self,
        *,
        env_vars: dict[str, str] | None = None,
        mcp_packages: list[str] | None = None,
        tier: str | None = None,
        **kwargs: Any,
    ) -> DockerRuntime:
        """Create a sandbox container sized for ``tier``.

        Args:
            env_vars: Environment injected at creation time; wins over the thread
                pool defaults this method sets.
            mcp_packages: npm packages installed into the container after start.
            tier: Resource tier name. ``None`` resolves to the configured default.
        """
        client = await self._get_client()
        await self._ensure_image(client)

        runtime_id = f"docker-{uuid.uuid4().hex[:12]}"
        container_name = f"langalpha-sandbox-{runtime_id}"

        # Parse proxy port pool for preview URL support
        proxy_ports = _parse_proxy_port_range(self._config.preview_proxy_ports)

        sizing = self._resolve_tier(tier)

        # Build container config
        host_config: dict[str, Any] = {
            "Memory": sizing.memory_bytes,
            "NanoCpus": int(sizing.cpus * 1e9),
            "NetworkMode": self._config.network_mode,
            "AutoRemove": False,  # We manage removal ourselves
            "Init": True,  # tini as PID 1 for zombie reaping
            **_FUSE_HOST_CONFIG,
        }
        self._apply_disk_quota(host_config, sizing)

        # host.docker.internal resolves natively on Docker Desktop but not on
        # Linux bridge networks; pin it so the egress relay's default base URL
        # (see services/egress/reachability.py) works on both.
        if self._config.network_mode != "none":
            host_config["ExtraHosts"] = ["host.docker.internal:host-gateway"]

        # Publish proxy ports so preview URLs are reachable from the host.
        # Use dynamic host ports (HostPort: "") so multiple containers can
        # coexist without port conflicts.
        if proxy_ports:
            host_config["PortBindings"] = {
                f"{p}/tcp": [{"HostPort": ""}] for p in proxy_ports
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

        env = {
            **sandbox_thread_env(int(sizing.cpus)),
            **(env_vars or {}),
        }
        container_config["Env"] = [f"{k}={v}" for k, v in env.items()]

        try:
            container_obj = await self._start_new(
                client, container_config, container_name, sizing
            )
        except Exception as e:
            if "/dev/fuse" not in str(e):
                raise
            # A host without FUSE still runs sandboxes, without the file
            # mount: the file tools reach the server-held files in process.
            logger.warning(
                "Docker host has no /dev/fuse; sandbox runs without the file mount",
                container_name=container_name,
                error=str(e),
            )
            for key in _FUSE_HOST_CONFIG:
                del host_config[key]
            container_obj = await self._start_new(
                client, container_config, container_name, sizing
            )
        try:
            info = await container_obj.show()
            if sizing.elevated:
                await self._verify_tier_applied(container_obj, info, sizing)

            # Read actual host port mappings (dynamic ports picked by Docker)
            host_port_map: dict[int, int] = {}
            if proxy_ports:
                port_bindings = info.get("NetworkSettings", {}).get("Ports", {})
                for cp in proxy_ports:
                    bindings = port_bindings.get(f"{cp}/tcp", [])
                    if bindings and bindings[0].get("HostPort"):
                        host_port_map[cp] = int(bindings[0]["HostPort"])
        except BaseException:
            # No runtime/provider reference exists yet, so this method is the
            # only owner capable of removing a container that started but could
            # not be inspected or validated.
            await _remove_quietly(container_obj, container_name)
            raise

        logger.info(
            "Docker container started",
            container_name=container_name,
            runtime_id=runtime_id,
            image=self._config.image,
            tier=sizing.tier,
            cpus=sizing.cpus,
            memory_bytes=sizing.memory_bytes,
            host_port_map=host_port_map or None,
        )

        runtime = DockerRuntime(
            container_obj,
            runtime_id=runtime_id,
            working_dir=self._working_dir,
            dev_mode=self._config.dev_mode,
            host_work_dir=host_work_dir,
            proxy_ports=proxy_ports,
            host_port_map=host_port_map,
            preview_base_url=self._config.preview_base_url,
        )

        # Install MCP npm packages if needed (mirrors Daytona snapshot behavior)
        if mcp_packages:
            pkgs = " ".join(shlex.quote(pkg) for pkg in mcp_packages)
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
            from aiodocker.exceptions import DockerError

            if isinstance(e, DockerError) and e.status == 404:
                raise SandboxGoneError(sandbox_id, "Docker container was removed") from e
            raise

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

        # Read actual host port mappings from running container
        proxy_ports = _parse_proxy_port_range(self._config.preview_proxy_ports)
        host_port_map: dict[int, int] = {}
        port_bindings = info.get("NetworkSettings", {}).get("Ports", {})
        for cp in proxy_ports:
            bindings = port_bindings.get(f"{cp}/tcp", [])
            if bindings and bindings[0].get("HostPort"):
                host_port_map[cp] = int(bindings[0]["HostPort"])

        return DockerRuntime(
            container_obj,
            runtime_id=sandbox_id,
            working_dir=self._working_dir,
            dev_mode=dev_mode,
            host_work_dir=host_work_dir,
            proxy_ports=proxy_ports,
            host_port_map=host_port_map,
            preview_base_url=self._config.preview_base_url,
        )

    async def prepare_reconnect(
        self, runtime: DockerRuntime, *, tier: str | None = None
    ) -> None:
        """Raise legacy limits to the selected tier without replacing its disk.

        Unlimited or larger limits stay untouched. Inspect again after updating:
        the daemon may accept an unsupported setting without enforcing it.
        """
        sizing = self._resolve_tier(tier)
        info = await runtime._container.show()
        host = info.get("HostConfig") or {}
        cpus = applied_cpus(host)
        memory = int(host.get("Memory") or 0)
        changes: dict[str, int] = {}
        if cpus is not None and 0 < cpus < sizing.cpus - 0.01:
            if int(host.get("CpuQuota") or 0) > 0:
                period = int(host.get("CpuPeriod") or 100000)
                changes["CpuQuota"] = int(sizing.cpus * period)
            else:
                changes["NanoCpus"] = int(sizing.cpus * 1e9)
        if 0 < memory < sizing.memory_bytes:
            changes["Memory"] = sizing.memory_bytes
            swap = int(host.get("MemorySwap") or 0)
            if 0 < swap < sizing.memory_bytes:
                changes["MemorySwap"] = sizing.memory_bytes
        if not changes:
            return

        client = await self._get_client()
        # aiodocker has no container.update wrapper; use its JSON transport.
        await client._query_json(
            f"containers/{info['Id']}/update", method="POST", data=changes
        )
        repaired = (await runtime._container.show()).get("HostConfig") or {}
        actual_cpus = applied_cpus(repaired)
        actual_memory = int(repaired.get("Memory") or 0)
        cpu_changed = "NanoCpus" in changes or "CpuQuota" in changes
        if (
            cpu_changed and (actual_cpus is None or actual_cpus < sizing.cpus - 0.01)
        ) or ("Memory" in changes and actual_memory < sizing.memory_bytes):
            raise RuntimeError(
                f"Docker did not apply the {sizing.tier!r} CPU/RAM repair; "
                "the existing container and its files have been retained"
            )
        logger.info("Repaired Docker limits on reconnect", runtime_id=runtime.id, tier=sizing.tier)

    async def close(self) -> None:
        if self._client is not None:
            try:
                await self._client.close()
            except Exception as e:
                logger.debug("Failed to close Docker client", error=str(e))
            finally:
                self._client = None

    def classify_error(self, exc: Exception) -> SandboxFailureKind:
        if isinstance(exc, SandboxGoneError):
            return SandboxFailureKind.SANDBOX_GONE
        return super().classify_error(exc)

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

        dockerfile_basename = os.path.basename(dockerfile_path)
        image_tag = self._config.image

        logger.info(
            "Building Docker sandbox image",
            dockerfile=dockerfile_path,
            tag=image_tag,
        )

        # Use aiodocker to build. It expects a tar stream via ``fileobj`` and
        # the Dockerfile path via ``path_dockerfile``.
        try:
            build_tar = io.BytesIO()
            with tarfile.open(fileobj=build_tar, mode="w") as tar:
                tar.add(dockerfile_path, arcname=dockerfile_basename)
            build_tar.seek(0)

            async for log_line in client.images.build(
                fileobj=build_tar,
                path_dockerfile=dockerfile_basename,
                tag=image_tag,
                rm=True,
                stream=True,
                encoding="identity",
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


def _upload_tar(entries: Sequence[tuple[bytes, str]], uid: int, gid: int) -> bytes:
    """A ``put_archive`` tar of ``(content, absolute path)`` stamped with now.

    Extraction applies each entry's mtime, which ``TarInfo`` leaves at 0, so
    every upload landed at the epoch and a same-size rewrite changed neither
    half of the ``(mtime, size)`` stamp the sandbox detects changes by. A float
    mtime is written as a PAX record, which keeps its sub-second part.
    """
    now = time.time()
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w", format=tarfile.PAX_FORMAT) as tar:
        for data, dest_path in entries:
            info = tarfile.TarInfo(name=dest_path.lstrip("/"))
            info.size = len(data)
            info.mode = 0o644
            info.uid, info.gid = uid, gid
            info.mtime = now
            tar.addfile(info, io.BytesIO(data))
    return buf.getvalue()


def _shell_escape(value: str) -> str:
    """Shell-escape a value for use in an export command."""
    return "'" + value.replace("'", "'\\''") + "'"


def _is_container_gone(exc: Exception) -> bool:
    """Check if an exception indicates the container no longer exists."""
    msg = str(exc).lower()
    return "no such container" in msg or "not found" in msg or "409" in msg
