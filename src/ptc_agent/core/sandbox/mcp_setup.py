"""MCP dependency install, schema discovery, and builtin server startup.

The wrapper union and the per-workspace overlay live in ``tool_overlay.py``.

Functions take the owning ``PTCSandbox`` as their explicit first argument;
``PTCSandbox`` exposes same-name delegators, so call sites and patch
semantics are unchanged.
"""

import asyncio
import json
import posixpath
import shlex
import uuid
from typing import TYPE_CHECKING, Any

import structlog

from ptc_agent.core.sandbox._defaults import DEFAULT_DEPENDENCIES
from ptc_agent.core.sandbox.retry import RetryPolicy
from ptc_agent.core.sandbox.vault_helper import workspace_vault_path

from ..paths import SandboxLayout
from ..project_context import current_project
from .supervisor_runtime import protocol as supervisor_protocol

if TYPE_CHECKING:
    from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

logger = structlog.get_logger(__name__)


async def _install_dependencies(sandbox: "PTCSandbox") -> None:
    """Install required Python packages in sandbox (no-snapshot fallback)."""
    logger.info("Installing dependencies (no snapshot)")

    # yfinance pins curl_cffi<0.14 but scrapling[all] requires >=0.14.
    # Override resolves the conflict (tested, yfinance works with 0.14+).
    install_cmd = (
        "echo 'curl_cffi>=0.14' > /tmp/_overrides.txt && "
            f"uv pip install -q --override /tmp/_overrides.txt {' '.join(DEFAULT_DEPENDENCIES)} && "
            "rm -f /tmp/_overrides.txt"
    )

    try:
        assert sandbox.runtime is not None
        result = await sandbox._runtime_call(
            sandbox.runtime.exec,
            install_cmd,
            retry_policy=RetryPolicy.SAFE,
        )
        if result.exit_code != 0:
            logger.warning(
                "Dependency install exited with non-zero code",
                exit_code=result.exit_code,
                output=result.stdout[:500],
            )
        else:
            logger.info("Dependencies installed")
    except OSError as e:
        logger.error(f"Failed to install dependencies: {e}")
        raise

    # Install Scrapling browsers (Camoufox for StealthyFetcher)
    try:
        result = await sandbox._runtime_call(
            sandbox.runtime.exec,
            "scrapling install",
            retry_policy=RetryPolicy.SAFE,
        )
        if result.exit_code != 0:
            logger.warning(
                "Scrapling browser install failed",
                output=result.stdout[:300] if result.stdout else "",
            )
        else:
            logger.info("Scrapling browsers installed")
    except Exception as e:
        logger.warning(f"Scrapling browser install skipped: {e}")


async def _upload_discovery_client(
    sandbox: "PTCSandbox", extra_servers: list[Any] | None = None
) -> str:
    """Upload a config-only discovery client to a UNIQUE path; return it.

        The client depends only on the effective server configs (no schemas),
        so it can be generated before any discovery has run. It is written to
        a per-call ``_internal`` temp path — never ``tools/mcp_client.py`` —
        because concurrent discoveries (a bulk import fires several probes plus
        the background kick at once) would otherwise clobber one another's
        config and report spurious ``unknown server`` errors, and a probe must
        never replace the runtime client the agent's wrappers import.

        ``extra_servers`` carries freshly-resolved configs for an on-demand
        discovery whose session may predate the edit. They are merged over the
        session's enabled set by name (override an edited server, append a new
        one) so the probe sees pending changes.
        """
    assert sandbox.runtime is not None
    work_dir = sandbox._work_dir
    enabled_servers = [
        server for server in sandbox.config.mcp.servers if server.enabled
    ]
    if extra_servers:
        for srv in extra_servers:
            if not getattr(srv, "enabled", True):
                continue
            idx = next(
                (i for i, s in enumerate(enabled_servers) if s.name == srv.name),
                None,
            )
            if idx is None:
                enabled_servers.append(srv)
            else:
                enabled_servers[idx] = srv
    # Pass the sandbox's real work dir (Lane A handoff): the client embeds
    # the vault path + mcp_servers path from it. Defaulting would point the
    # vault/server paths at the wrong directory after a working-dir change.
    # The union is not folded in: it holds the pre-edit config of the very
    # server this probe is meant to see edited. A workspace-local server
    # resolves its secrets from its owner's vault when a turn names one.
    project = current_project()
    vault_file = (
        workspace_vault_path(work_dir, project.claim) if project is not None else None
    )
    mcp_client_code = sandbox.tool_generator.generate_mcp_client_code(
        enabled_servers, working_dir=work_dir, fold_union=False, vault_file=vault_file
    )
    layout = SandboxLayout(work_dir)
    client_path = (
        f"{layout.internal}/.mcp_discover_client_{uuid.uuid4().hex}.py"
    )
    await sandbox._runtime_call(
        sandbox.runtime.exec,
        f"mkdir -p {shlex.quote(layout.internal)}",
        retry_policy=RetryPolicy.SAFE,
    )
    await sandbox._runtime_call(
        sandbox.runtime.upload_file,
        mcp_client_code.encode("utf-8"),
        client_path,
        retry_policy=RetryPolicy.SAFE,
    )
    logger.debug("MCP discovery client installed", path=client_path)
    return client_path





async def discover_user_mcp_schemas(
    sandbox: "PTCSandbox", servers: list[Any]
) -> dict[str, dict[str, Any]]:
    """Discover tool schemas for user MCP servers via the in-sandbox client.

        For each server: run ``mcp_client.py discover <name> <out>`` (file IPC —
        the CLI writes its result JSON to a temp file, never stdout), read the
        file back, delete it. Per-server error isolation; one hung/broken server
        never blocks the others. Returns
        ``{name: {"status","error","tools","server_info"}}``.
        No vault file is needed — the client substitutes inert placeholders.
        """
    await sandbox._wait_ready()
    assert sandbox.runtime is not None
    work_dir = sandbox._work_dir
    layout = SandboxLayout(work_dir)

    # Upload a config-current discovery client FIRST (it depends only on
    # config, not on schemas) so discovery runs against the latest server
    # set. Pass the servers being discovered so an on-demand probe reflects
    # a pending add/edit the live session has not re-resolved yet (≤30s
    # window). The path is unique per call: concurrent discoveries must not
    # read each other's config (spurious "unknown server" otherwise).
    client_path = await sandbox._upload_discovery_client(extra_servers=servers)

    sem = asyncio.Semaphore(sandbox._DISCOVERY_CONCURRENCY)

    async def _discover_one(server: Any) -> tuple[str, dict[str, Any]]:
        name = server.name
        # Unique per invocation: concurrent discoveries of the same server
        # (background kick + on-demand /discover) must not share a file.
        out_path = f"{layout.internal}/.mcp_discover_{uuid.uuid4().hex}.json"
        async with sem:
            try:
                # python3, not python: the no-snapshot fallback image never
                # gets the /usr/bin/python alias the snapshot build adds.
                cmd = (
                    f"cd {shlex.quote(work_dir)} && python3 "
                        f"{shlex.quote(client_path)} discover "
                        f"{shlex.quote(name)} {shlex.quote(out_path)}"
                )
                await sandbox._runtime_call(
                    sandbox.runtime.exec,
                    cmd,
                    timeout=sandbox._DISCOVERY_EXEC_TIMEOUT_S,
                    retry_policy=RetryPolicy.SAFE,
                    total_timeout=float(sandbox._DISCOVERY_EXEC_TIMEOUT_S + 30),
                )
                raw = await sandbox.adownload_file_bytes(out_path)
                if not raw:
                    return name, {
                        "status": "error",
                        "error": "discovery produced no output",
                        "tools": [],
                    }
                parsed = json.loads(
                    raw.decode("utf-8") if isinstance(raw, bytes) else raw
                )
                info = parsed.get("server_info")
                return name, {
                    "status": parsed.get("status", "error"),
                    "error": parsed.get("error", "") or "",
                    "tools": parsed.get("tools") or [],
                    "server_info": info if isinstance(info, dict) else None,
                }
            except Exception as e:  # noqa: BLE001 — isolate one bad server
                logger.warning(
                    "MCP discovery failed for server", server=name, error=str(e)
                )
                return name, {"status": "error", "error": str(e), "tools": []}
            finally:
                # Best-effort temp-file cleanup; never fail discovery on it.
                try:
                    await sandbox._runtime_call(
                        sandbox.runtime.exec,
                        f"rm -f {shlex.quote(out_path)}",
                        retry_policy=RetryPolicy.SAFE,
                    )
                except Exception:
                    pass

    try:
        pairs = await asyncio.gather(*[_discover_one(s) for s in servers])
    finally:
        # Best-effort removal of this call's discovery client; never fail
        # discovery on cleanup.
        try:
            await sandbox._runtime_call(
                sandbox.runtime.exec,
                f"rm -f {shlex.quote(client_path)}",
                retry_policy=RetryPolicy.SAFE,
            )
        except Exception:
            pass
    return dict(pairs)


async def _start_internal_mcp_servers(sandbox: "PTCSandbox") -> None:
    """Start the computer's MCP supervisor, if it is not already listening.

    One daemon per computer owns every sandbox-side MCP server process, so a
    server is handshaken once per idle window instead of once per
    ``execute_code``. Starting it is idempotent: the daemon takes an exclusive
    lock before binding, so a second start exits without touching the socket.

    A failure here is logged, never raised. The generated client falls back to
    spawning servers in the execution's own interpreter, which is what every
    call did before this daemon existed, so a computer whose supervisor cannot
    start is slower rather than broken.
    """
    assert sandbox.runtime is not None
    work_dir = sandbox._work_dir
    layout = SandboxLayout(work_dir)
    socket_path = f"{work_dir}/{supervisor_protocol.SOCKET_REL_PATH}"
    log_path = f"{work_dir}/{supervisor_protocol.LOG_REL_PATH}"
    src_root = layout.internal_src
    quoted_socket = shlex.quote(socket_path)
    command = (
        f"mkdir -p {shlex.quote(posixpath.dirname(socket_path))} && "
        f"cd {shlex.quote(src_root)} && "
        f"{{ PYTHONPATH={shlex.quote(src_root)} "
        f"nohup python3 -m {supervisor_protocol.PACKAGE_NAME} "
        f"{shlex.quote(work_dir)} >> {shlex.quote(log_path)} 2>&1 < /dev/null & }} ; "
        # Poll rather than sleep a fixed amount: a warm start binds in
        # milliseconds and only a genuinely failing one pays the full wait.
        f"for _ in $(seq 30); do [ -S {quoted_socket} ] && break; sleep 0.1; done; "
        f"[ -S {quoted_socket} ]"
    )
    sandbox.mcp_server_sessions = {
        "supervisor": {"socket": socket_path, "log": log_path, "started": False}
    }
    try:
        result = await sandbox._runtime_call(
            sandbox.runtime.exec,
            command,
            retry_policy=RetryPolicy.SAFE,
        )
    except Exception as e:  # noqa: BLE001 - the client's fallback covers this
        logger.warning("MCP supervisor start failed", error=str(e))
        return
    exit_code = getattr(result, "exit_code", 0)
    if exit_code:
        logger.warning(
            "MCP supervisor did not come up; calls fall back to in-process spawn",
            exit_code=exit_code,
            log=log_path,
        )
        return
    sandbox.mcp_server_sessions["supervisor"]["started"] = True
    logger.info("MCP supervisor listening", socket=socket_path)



def _detect_missing_imports(sandbox: "PTCSandbox", stderr: str) -> list[str]:
    """Extract missing module names from the executed script's own traceback.

        Args:
            stderr: Standard error output from code execution

        Returns:
            List of missing package names (base package only, e.g., 'foo' from 'foo.bar')
        """
    import re

    # Whatever comes out of here is handed to `uv pip install` in the sandbox,
    # so the input is trusted as narrowly as possible against two attacks:
    #
    # 1. Shell injection — the capture is a real dotted module path, never
    #    "anything between the quotes". A name with a space or ``;`` fails to
    #    match at all (and could never name an installable package anyway).
    # 2. Dependency confusion — a third-party MCP server spawned during the run
    #    shares this stderr stream and could inject a fake
    #    "ModuleNotFoundError: No module named 'evil'" to make us install an
    #    attacker-registered package (whose sdist build runs code). So only the
    #    executed script's OWN unhandled exception is trusted: the FINAL
    #    traceback block of stderr, and only when it carries a genuine frame
    #    line. A server's mid-stream text, or a preceding block, is ignored.
    marker = "Traceback (most recent call last):"
    start = stderr.rfind(marker)
    if start == -1:
        return []
    final_tb = stderr[start:]
    if '\n  File "' not in final_tb:
        # No real frame → not a Python interpreter traceback; don't trust it.
        return []

    patterns = [
        r"ModuleNotFoundError: No module named ['\"]([A-Za-z_][A-Za-z0-9_.]*)['\"]",
        r"ImportError: No module named ['\"]([A-Za-z_][A-Za-z0-9_.]*)['\"]",
    ]

    matches = []
    for pattern in patterns:
        matches.extend(re.findall(pattern, final_tb))

    # Handle submodule imports (e.g., "foo.bar" -> "foo")
    # Also deduplicate
    base_packages = list({m.split(".")[0] for m in matches})

    if base_packages:
        logger.info(
            "Detected missing imports",
            packages=base_packages,
        )

    return base_packages


async def _install_package(sandbox: "PTCSandbox", package: str) -> bool:
    """Install a Python package in the sandbox.

        Args:
            package: Package name to install

        Returns:
            True if installation succeeded, False otherwise
        """
    try:
        logger.info(f"Auto-installing missing package: {package}")
        assert sandbox.runtime is not None
        result = await sandbox._runtime_call(
            sandbox.runtime.exec,
            # Quoted at the sink as well as filtered at the parser: this is the
            # only place a caller-supplied name becomes shell text, so it should
            # be safe for callers that don't come through _detect_missing_imports.
            f"uv pip install -q {shlex.quote(package)}",
            retry_policy=RetryPolicy.SAFE,
        )
        exit_code = getattr(result, "exit_code", 1)
        if exit_code == 0:
            logger.info(f"Successfully installed package: {package}")
            return True
        logger.warning(
            f"Failed to install package: {package}, exit_code={exit_code}"
        )
        return False
    except OSError as e:
        logger.warning(f"Failed to install {package}: {e}")
        return False
