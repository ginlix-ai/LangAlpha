"""MCP server installation, schema discovery, and dependency setup.

Functions take the owning ``PTCSandbox`` as their explicit first argument;
``PTCSandbox`` exposes same-name delegators, so call sites and patch
semantics are unchanged.
"""

import asyncio
import json
import shlex
import uuid
from typing import Any

import structlog


from ptc_agent.core.sandbox._defaults import DEFAULT_DEPENDENCIES
from ptc_agent.core.sandbox.retry import RetryPolicy

from ..mcp_sanitize import (
    is_untrusted_server,
    sanitize_tool_name,
)
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

logger = structlog.get_logger(__name__)


def _doc_name(tool_name: str, untrusted: bool) -> str:
    """The filename a tool's documentation is written under.

    Shared by the writer and by the sweep that removes what the writer no longer
    produces. Two derivations of one name is exactly how a doc for a tool the
    agent may not call survives a pass meant to delete it.

    Untrusted names could carry ``..`` or ``/`` and traverse out of the docs
    directory, so there the sanitized identifier is the filename; a builtin's
    name is already a valid identifier.
    """
    if untrusted:
        return sanitize_tool_name(tool_name) or "_invalid_tool"
    return tool_name


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
    mcp_client_code = sandbox.tool_generator.generate_mcp_client_code(
        enabled_servers, working_dir=work_dir
    )
    client_path = (
        f"{work_dir}/_internal/.mcp_discover_client_{uuid.uuid4().hex}.py"
    )
    await sandbox._runtime_call(
        sandbox.runtime.exec,
        f"mkdir -p {shlex.quote(f'{work_dir}/_internal')}",
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


async def _install_tool_modules(sandbox: "PTCSandbox") -> None:
    """Generate and install tool modules + the MCP client from MCP servers."""
    logger.debug("Installing tool modules")

    # Get work directory (set by _setup_workspace)
    work_dir = sandbox._work_dir

    # Collect all files to upload (content generation is CPU-bound, fast)
    uploads: list[tuple[bytes, str, tuple[str, dict[str, str]] | None]] = []

    # 1. MCP client module — config-only, regenerated with the real work dir
    #    so user-server vault/path references resolve correctly.
    enabled_servers = [
        server for server in sandbox.config.mcp.servers if server.enabled
    ]
    mcp_client_code = sandbox.tool_generator.generate_mcp_client_code(
        enabled_servers, working_dir=work_dir
    )
    mcp_client_path = f"{work_dir}/tools/mcp_client.py"
    uploads.append(
        (
            mcp_client_code.encode("utf-8"),
            mcp_client_path,
            ("MCP client module installed", {"path": mcp_client_path}),
        )
    )

    # Trust is computed ONCE here, per server, and passed across the codegen
    # boundary as a bool — codegen never re-derives it from the raw source
    # string (that duplication is how user-level servers once slipped through
    # the workspace-only gates).
    untrusted_by_name = {
        s.name: is_untrusted_server(s) for s in sandbox.config.mcp.servers
    }

    # 2. Tool modules and documentation
    assert sandbox.mcp_registry is not None
    tools_by_server = sandbox.mcp_registry.get_all_tools()

    assert sandbox.runtime is not None

    # Prune stale doc dirs AND stale wrapper modules for servers no longer in
    # the effective set (a disabled built-in, a deleted/edited user server).
    # The diff-prune runs every sync, so the one-shot _disabled_modules_pruned
    # guard is moot here; this removes ``tools/{name}.py`` + ``tools/docs/{name}``.
    docs_root = f"{work_dir}/tools/docs"
    tools_root = f"{work_dir}/tools"
    stale_paths: list[str] = []
    # Not wrapped: ``als_directory`` returns [] for a directory that is empty or
    # absent, and raises only when the sandbox itself failed. Swallowing that
    # raise reads a broken sandbox as "nothing stale here", and the sync then
    # stamps the manifest current -- so the doc for a tool the user declined
    # survives every later sync too, which is the failure this sweep exists to
    # prevent. A fresh sandbox costs nothing here; a broken one should fail.
    existing_docs = await sandbox.als_directory(docs_root)
    stale_paths.extend(
        entry["path"]
        for entry in existing_docs
        if entry.get("is_dir") and entry.get("name") not in tools_by_server
    )

    # The sweep inside the servers that DID survive. Removing a whole directory
    # when its server leaves the set was enough while a server's tool list only
    # changed when the vendor changed it. Capability consent changes it on a user
    # toggle, so a declined tool now routinely leaves its doc behind: the wrapper
    # module is one file rewritten whole and correctly loses the function, while
    # ``tools/docs/<server>/`` goes on describing it.
    #
    # That is not clutter. The tool guide sends the agent to this directory to
    # find out what a server can do, and it is the surface the agent trusts, so a
    # doc left behind is the agent reading that it can place live orders and
    # telling the user so -- on a connection whose owner declined exactly that.
    # The call is refused at the relay either way; what leaks here is the claim,
    # which on this feature is the part that matters.
    #
    # Only directories the listing above already found are opened, so a fresh
    # sandbox costs nothing and a warm one costs one listing per server present.
    for entry in existing_docs:
        server_name = entry.get("name")
        if not entry.get("is_dir") or server_name not in tools_by_server:
            continue
        expected = {
            f"{_doc_name(tool.name, untrusted_by_name.get(server_name, True))}.md"
            for tool in tools_by_server[server_name]
        }
        existing_tool_docs = await sandbox.als_directory(entry["path"])
        stale_paths.extend(
            doc["path"]
            for doc in existing_tool_docs
            if not doc.get("is_dir")
            and doc.get("name", "").endswith(".md")
            and doc.get("name") not in expected
        )
    existing_tools = await sandbox.als_directory(tools_root)
    if existing_tools:
        expected_wrappers = {f"{name}.py" for name in tools_by_server}
        stale_paths.extend(
            entry["path"]
            for entry in existing_tools
            if not entry.get("is_dir")
            and entry.get("name", "").endswith(".py")
            and entry.get("name") not in expected_wrappers
            and entry.get("name") not in ("mcp_client.py", "__init__.py")
        )
    if stale_paths:
        rm_cmd = "rm -rf " + " ".join(shlex.quote(p) for p in stale_paths)
        result = await sandbox._runtime_call(
            sandbox.runtime.exec,
            rm_cmd,
            retry_policy=RetryPolicy.SAFE,
        )
        # A delete that silently failed is the same outcome as never sweeping:
        # the manifest is stamped current and the stale doc is never revisited.
        exit_code = getattr(result, "exit_code", 0)
        if exit_code:
            raise RuntimeError(
                f"failed to remove stale tool files (exit {exit_code}): "
                f"{getattr(result, 'stderr', '')}"
            )

    for server_name, tools in tools_by_server.items():
        # Fail closed: a server present in the registry but missing from the
        # trust map (config drift mid-sync) is treated as untrusted — a wrong
        # guess here costs sanitization, not a docstring breakout.
        untrusted = untrusted_by_name.get(server_name, True)
        # Generate Python module
        module_code = sandbox.tool_generator.generate_tool_module(
            server_name, tools, untrusted=untrusted
        )
        module_path = f"{work_dir}/tools/{server_name}.py"
        uploads.append(
            (
                module_code.encode("utf-8"),
                module_path,
                (
                    "Tool module installed",
                    {
                        "server": server_name,
                        "path": module_path,
                        "tool_count": str(len(tools)),
                    },
                ),
            )
        )

        # Generate documentation for each tool
        for tool in tools:
            doc = sandbox.tool_generator.generate_tool_documentation(
                tool, untrusted=untrusted
            )
            doc_name = _doc_name(tool.name, untrusted)
            doc_path = f"{work_dir}/tools/docs/{server_name}/{doc_name}.md"
            upload_item: tuple[bytes, str, tuple[str, dict[str, str]] | None] = (
                doc.encode("utf-8"),
                doc_path,
                None,
            )
            uploads.append(upload_item)

    # 3. __init__.py for tools package
    init_content = '"""Auto-generated tool modules from MCP servers."""\n'
    init_path = f"{work_dir}/tools/__init__.py"
    init_item: tuple[bytes, str, tuple[str, dict[str, str]] | None] = (
        init_content.encode("utf-8"),
        init_path,
        None,
    )
    uploads.append(init_item)

    # Batch mkdir — all dirs in one command
    all_dirs = [f"{work_dir}/tools"] + [
        f"{work_dir}/tools/docs/{name}" for name in tools_by_server
    ]
    mkdir_cmd = "mkdir -p " + " ".join(shlex.quote(d) for d in all_dirs)
    await sandbox._runtime_call(
        sandbox.runtime.exec,
        mkdir_cmd,
        retry_policy=RetryPolicy.SAFE,
    )

    # Batch upload — single HTTP request for all generated content
    batch = [
        (content, path) for content, path, _ in uploads
    ]
    await sandbox._runtime_call(
        sandbox.runtime.upload_files,
        batch,
        retry_policy=RetryPolicy.SAFE,
    )
    # Log after batch
    for _, _, log_info in uploads:
        if log_info:
            msg, kwargs = log_info
            logger.debug(msg, **kwargs)

    server_count = len(tools_by_server)
    tool_count = sum(len(t) for t in tools_by_server.values())
    logger.info(
        "Tool modules installed",
        servers=server_count,
        tools=tool_count,
    )


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
        out_path = f"{work_dir}/_internal/.mcp_discover_{uuid.uuid4().hex}.json"
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
    """Start MCP servers as background processes inside sandbox."""
    logger.debug("Starting internal MCP servers")

    # Track server sessions for lifecycle management
    sandbox.mcp_server_sessions = {}

    # Built-ins only: user stdio servers are spawned at call time by the
    # in-sandbox mcp_client (npx/uvx fetch then), never pre-started here.
    for server in sandbox._builtin_servers():
        if not server.enabled:
            continue
        if server.transport != "stdio":
            logger.warning(
                f"Skipping non-stdio server {server.name}",
                transport=server.transport,
            )
            continue

        try:
            # Build the command to start the MCP server
            if server.command == "npx":
                # npx -y package-name [args...]
                cmd_parts = [server.command, *server.args]
                cmd = " ".join(cmd_parts)
            else:
                # Custom command
                cmd = f"{server.command} {' '.join(server.args)}"

            # Add environment variables if specified
            env_vars = []
            if hasattr(server, "env") and server.env:
                for key, value in server.env.items():
                    # Environment variables might have ${VAR} syntax, resolve them
                    # For now, we'll pass them as-is and they'll need to be set in sandbox
                    env_vars.append(f"{key}={value}")

            # Create PTY session for the MCP server
            session_name = f"mcp-{server.name}"

            logger.debug(
                "Creating MCP server session",
                server=server.name,
                session=session_name,
                command=cmd,
            )

            # Create session (but don't start the server yet, we'll do that when needed)
            # For now, just track that this server should be available
            sandbox.mcp_server_sessions[server.name] = {
                "session_name": session_name,
                "command": cmd,
                "env": env_vars,
                "started": False,
            }

            logger.debug(
                "MCP server session configured",
                server=server.name,
                session=session_name,
            )

        except OSError as e:
            logger.error(
                "Failed to configure MCP server session",
                server=server.name,
                error=str(e),
            )

    logger.debug(
        "Internal MCP server configuration complete",
        servers=list(sandbox.mcp_server_sessions.keys()),
    )


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
