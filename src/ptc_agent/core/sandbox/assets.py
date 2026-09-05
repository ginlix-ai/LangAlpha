"""Sandbox asset & skills sync — manifests, hashing, uploads, pruning.

Functions take the owning ``PTCSandbox`` as their explicit first argument;
``PTCSandbox`` exposes same-name delegators, so call sites and patch
semantics are unchanged.
"""

import asyncio
import hashlib
import json
import shlex
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import structlog

from src.observability import (
    safe_record,
    sandbox_asset_sync_phase_duration_ms,
    sandbox_asset_sync_total_ms,
)

from ptc_agent.core.sandbox.migration import CURRENT_LAYOUT_VERSION, run_layout_migrations
from ptc_agent.core.sandbox.retry import RetryPolicy

from ..mcp_sanitize import (
    discovery_affecting_payload,
)
from ..tool_generator import MCP_CLIENT_CODEGEN_VERSION
from ptc_agent.core.sandbox._shared import (
    _LOCK_VOLATILE_KEYS,
    _MCP_SHARED_RUNTIME_FILES,
    SyncResult,
    _sha256_file,
    _hash_dict,
    _internal_package_files,
    _resolve_local_path,
    _get_sandbox_eligible_skills,
)
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

logger = structlog.get_logger(__name__)


def _compute_tool_schema_hash(sandbox: "PTCSandbox") -> str:
    """Hash the current MCP tool schemas from the live registry.

        Captures tool names + input schemas so that adding/removing/modifying
        a tool on a running MCP server is detected even if the .py file is unchanged.
        """
    if not sandbox.mcp_registry:
        return ""
    all_tools = sandbox.mcp_registry.get_all_tools()
    parts: list[str] = []
    for server_name in sorted(all_tools):
        for tool in sorted(all_tools[server_name], key=lambda t: t.name):
            parts.append(
                f"{server_name}:{tool.name}:{json.dumps(tool.input_schema, sort_keys=True)}"
            )
    return hashlib.sha256("\n".join(parts).encode()).hexdigest()


def _compute_user_mcp_config_hash(sandbox: "PTCSandbox") -> str:
    """Hash untrusted (``source`` 'workspace'/'user') server CONFIG — never secrets.

        Captures transport/command/args/url, the full env/header maps (literal
        values AND ``${vault:NAME}`` ref strings — the stored values are never
        resolved secrets), the effective secret-less-discovery decision, and
        whether the server is relay-bound, so a config-only edit — a literal
        ``MODE=prod`` -> ``staging`` change, a new authenticated header, a
        vault-ref retarget under the same key, or a first OAuth connect — always
        re-uploads the regenerated ``mcp_client.py``. Builds on
        :func:`discovery_affecting_payload` (the per-server discovery-cache key)
        and adds exactly one field it must never carry — see below. Returns ""
        when there are no user servers so builtin-only workspaces are untouched.
        """
    user_servers = sandbox._user_servers()
    if not user_servers:
        return ""

    parts: list[str] = []
    for server in sorted(user_servers, key=lambda s: s.name):
        payload = discovery_affecting_payload(server, include_identity=True)
        # Deliberate asymmetry with the discovery fingerprint: the two hashes
        # serve different invalidation domains. Codegen branches on the binding
        # — a bound server is emitted as a relay entry with url and headers
        # dropped — so this hash must move when it flips. The vendor's
        # tools/list answer does NOT depend on it, and binding state entering
        # discovery_affecting_payload would leave every OAuth server's snapshot
        # stale forever (pinned by test_ignores_the_resolve_time_oauth_binding
        # in tests/unit/server/services/test_mcp_discovery.py).
        #
        # The BOOL, not the id: codegen only tests is-not-None, while the id
        # rotates on every reconnect and would force uploads that change
        # nothing. Written only when bound, so an unbound workspace's hash stays
        # byte-identical to a pre-binding sandbox and never re-uploads.
        # Always absent for 'workspace' servers: only catalog rows can carry a
        # connection, and a stored workspace blob has the field stripped.
        if getattr(server, "oauth_connection_id", None):
            payload["oauth_bound"] = True
        parts.append(json.dumps(payload, sort_keys=True))
    return hashlib.sha256("\n".join(parts).encode()).hexdigest()


async def _compute_skills_module(
    sandbox: "PTCSandbox",
    skill_roots: list[str],
    *,
    managed_root: str | None = None,
    disabled: frozenset[str] = frozenset(),
) -> dict[str, Any]:
    """Compute a skills module manifest with content-based SHA-256 hashing.

        Unlike the legacy ``_compute_skills_manifest`` (size+mtime), this hashes
        actual file contents so the manifest is deterministic and portable.

        ``managed_root`` marks which root holds the server-managed user tier —
        its skills get ``owner:"user"`` / ``sourceType:MANAGED_SOURCE_TYPE``
        lock entries so the sync may replace/prune them while the agent's own
        installs stay protected. ``disabled`` names are excluded entirely, so
        a disabled builtin leaves the local set (and hence the sandbox).
        """

    skills_base = f"{sandbox._work_dir}/.agents/skills"

    def build() -> dict[str, Any]:
        from ptc_agent.agent.middleware.skills.discovery import (
            parse_skill_metadata,
        )

        sandbox_skill_names, all_registry_names = _get_sandbox_eligible_skills()

        files: dict[str, str] = {}  # rel_path → sha256
        skills_metadata: dict[str, dict[str, Any]] = {}
        seen_skill_names: set[str] = set()

        for root_str in skill_roots:
            root = Path(root_str).expanduser()
            if not root.exists():
                continue
            is_managed_root = managed_root is not None and root_str == managed_root

            for skill_dir in root.iterdir():
                if not skill_dir.is_dir():
                    continue
                if not (skill_dir / "SKILL.md").exists():
                    continue

                skill_name = skill_dir.name
                if skill_name in disabled:
                    continue
                # Skip flash-only skills (not needed in sandbox)
                if (
                    skill_name not in sandbox_skill_names
                    and skill_name in all_registry_names
                ):
                    continue

                # Later sources override earlier ones
                if skill_name in seen_skill_names:
                    prefix = f"{skill_name}/"
                    files = {
                        k: v for k, v in files.items() if not k.startswith(prefix)
                    }
                seen_skill_names.add(skill_name)

                for fp in skill_dir.rglob("*"):
                    if not fp.is_file():
                        continue
                    if "__pycache__" in fp.parts or fp.name == "LICENSE.txt":
                        continue
                    rel = f"{skill_name}/{fp.relative_to(skill_dir)}"
                    files[rel] = _sha256_file(fp)

                # Parse SKILL.md frontmatter
                try:
                    content = (skill_dir / "SKILL.md").read_text(encoding="utf-8")
                except (OSError, UnicodeDecodeError):
                    continue
                sandbox_path = f"{skills_base}/{skill_name}/SKILL.md"
                meta = parse_skill_metadata(content, sandbox_path, skill_name)
                skills_metadata[skill_name] = dict(meta)

                # Build lock entry: platform, or server-managed user tier
                from ptc_agent.agent.middleware.skills.lock import (
                    MANAGED_SOURCE_TYPE,
                    build_lock_entry,
                )

                content_hash = f"sha256:{_sha256_file(skill_dir / 'SKILL.md')}"
                if is_managed_root:
                    lock_entry = build_lock_entry(
                        meta,
                        owner="user",
                        source=f"user:{skill_name}",
                        source_type=MANAGED_SOURCE_TYPE,
                        content_hash=content_hash,
                    )
                else:
                    lock_entry = build_lock_entry(
                        meta,
                        owner="platform",
                        source="platform",
                        source_type="platform",
                        content_hash=content_hash,
                    )
                skills_metadata[skill_name]["lock_entry"] = dict(lock_entry)

        version = _hash_dict(files)

        # Include lock entries in version hash so manifest detects ownership changes.
        # Exclude volatile timestamp fields (installedAt, updatedAt) — they change
        # on every manifest computation and would force a full skills re-upload
        # on every workspace restart even when no skill files changed.
        lock_hash_parts = []
        for name in sorted(skills_metadata):
            entry = skills_metadata[name].get("lock_entry")
            if entry:
                stable = {k: v for k, v in entry.items() if k not in _LOCK_VOLATILE_KEYS}
                lock_hash_parts.append(f"{name}:{json.dumps(stable, sort_keys=True)}")
        if lock_hash_parts:
            lock_payload = "\n".join(lock_hash_parts)
            combined = f"{version}\n{lock_payload}"
            version = hashlib.sha256(combined.encode()).hexdigest()

        return {"version": version, "files": files, "skills": skills_metadata}

    return await asyncio.to_thread(build)


async def _compute_sandbox_manifest(
    sandbox: "PTCSandbox",
    *,
    skill_roots: list[str] | None = None,
    managed_skill_root: str | None = None,
    disabled_skills: frozenset[str] = frozenset(),
    tokens: dict | None = None,
    user_id: str | None = None,
    workspace_id: str | None = None,
) -> dict[str, Any]:
    """Compute the unified local manifest for all sandbox asset modules."""
    modules: dict[str, Any] = {}
    config_dir = getattr(sandbox.config, "config_file_dir", None)

    # ── Module: mcp_servers ──
    # Built-ins only: this module ships local ``uv run python`` server files
    # from the host repo. User servers never ship host-local .py files, so
    # the mcp_servers hash stays byte-identical for builtin-only workspaces.
    mcp_files: dict[str, str] = {}  # filename → sha256
    for server in sandbox._builtin_servers():
        if not server.enabled:
            continue
        if server.transport != "stdio" or server.command != "uv":
            continue
        if (
            len(server.args) < 3
            or server.args[0] != "run"
            or server.args[1] != "python"
        ):
            continue
        resolved = _resolve_local_path(server.args[2], config_dir)
        if resolved:
            mcp_files[Path(resolved).name] = _sha256_file(Path(resolved))
    if mcp_files:
        for shared_name in _MCP_SHARED_RUNTIME_FILES:
            shared = _resolve_local_path(f"mcp_servers/{shared_name}", config_dir)
            if shared:
                mcp_files[shared_name] = _sha256_file(Path(shared))
    mcp_version = _hash_dict(mcp_files)
    modules["mcp_servers"] = {"version": mcp_version, "files": mcp_files}

    # ── Module: internal_packages (src/data_client, src/market_protocol) ──
    # One module for the whole set: the upload is all-or-nothing, so a single
    # version is the honest re-upload gate. Hashes the exact file set the
    # upload ships (same collection helper), so nothing can drift or drop.
    repo_root = config_dir or Path.cwd()
    src_dir = (repo_root / "src").resolve()
    internal_files = {
        str(rel): _sha256_file(local)
        for local, rel in _internal_package_files(src_dir)
    }
    modules["internal_packages"] = {
        "version": _hash_dict(internal_files),
        "files": internal_files,
    }

    # ── Module: tool_modules (derived) ──
    tool_schema_hash = sandbox._compute_tool_schema_hash()
    source_versions = {
        "mcp_servers": mcp_version,
        "tool_schemas": tool_schema_hash,
        # Generated-client output version. Folded in unconditionally so a
        # codegen bump (e.g. new _trace_mcp_call template) changes tm_version
        # for EVERY workspace and re-uploads the regenerated mcp_client.py on
        # the next sync — the manifest otherwise hashes only generation inputs.
        "client_codegen": MCP_CLIENT_CODEGEN_VERSION,
    }
    # User-server config hash — GATED on the presence of user servers so a
    # builtin-only workspace's source_versions dict (and thus tool_modules
    # version) is byte-identical to pre-change. A config-only edit (transport
    # /command/args/url, or any stored env/header value — refs and literals,
    # never resolved secrets) changes this hash and so re-uploads the
    # regenerated mcp_client.py via the tool_modules diff.
    user_mcp_hash = sandbox._compute_user_mcp_config_hash()
    if user_mcp_hash:
        source_versions["user_mcp_config"] = user_mcp_hash
    tm_version = _hash_dict(source_versions)
    modules["tool_modules"] = {
        "version": tm_version,
        "source_versions": source_versions,
    }

    # ── Module: skills ──
    if skill_roots:
        modules["skills"] = await sandbox._compute_skills_module(
            skill_roots, managed_root=managed_skill_root, disabled=disabled_skills
        )

    # ── Module: tokens ──
    if tokens:
        # Version captures the config identity; freshness is checked via minted_at.
        token_config_parts = {
            "user_id": user_id or "",
            "workspace_id": workspace_id or "",
            "client_id": tokens.get("client_id", ""),
        }
        modules["tokens"] = {
            "version": _hash_dict(token_config_parts),
            "minted_at": time.time(),
            "user_id": user_id or "",
            "workspace_id": workspace_id or "",
        }

    return {
        "schema_version": 1,
        "layout_version": CURRENT_LAYOUT_VERSION,
        "modules": modules,
    }


async def _read_unified_manifest(sandbox: "PTCSandbox") -> dict[str, Any] | None:
    """Read the unified manifest from the sandbox.

        Bypasses path validation for ``_internal/``.
        Returns None if missing, corrupt, or wrong ``schema_version``
        (triggers full refresh in the caller).
        """
    assert sandbox.runtime is not None
    try:
        raw = await sandbox._runtime_call(
            sandbox.runtime.download_file,
            sandbox._unified_manifest_path,
            retry_policy=RetryPolicy.SAFE,
        )
        if raw:
            text = raw.decode("utf-8") if isinstance(raw, bytes) else raw
            parsed = json.loads(text)
            if isinstance(parsed, dict) and parsed.get("schema_version") == 1:
                return parsed
    except Exception:
        pass  # Missing file, decode error, or JSON error → full refresh
    return None


async def _write_unified_manifest(sandbox: "PTCSandbox", manifest: dict[str, Any]) -> None:
    """Write the unified manifest to the sandbox.

        Bypasses path validation since ``_internal/`` is a protected directory
        that the agent cannot access, but the system needs to write to.
        """
    assert sandbox.runtime is not None
    await sandbox._runtime_call(
        sandbox.runtime.upload_file,
        json.dumps(manifest, sort_keys=True).encode("utf-8"),
        sandbox._unified_manifest_path,
        retry_policy=RetryPolicy.SAFE,
    )


async def _cleanup_legacy_manifests(sandbox: "PTCSandbox") -> None:
    """Remove old per-module manifest files after migration to unified manifest."""
    work_dir = sandbox._work_dir
    legacy_paths = [
        f"{work_dir}/mcp_servers/.mcp_manifest.json",
        f"{work_dir}/skills/.skills_manifest.json",
        f"{work_dir}/.agents/skills/.skills_manifest.json",
    ]
    assert sandbox.runtime is not None
    try:
        rm_cmd = "rm -f " + " ".join(shlex.quote(p) for p in legacy_paths)
        await sandbox._runtime_call(
            sandbox.runtime.exec,
            rm_cmd,
            retry_policy=RetryPolicy.SAFE,
        )
    except Exception:
        pass  # Best-effort cleanup


async def _upload_mcp_server_files_impl(sandbox: "PTCSandbox") -> None:
    """Upload MCP server .py files to sandbox (pure upload, no manifest check)."""
    work_dir = sandbox._work_dir
    mcp_servers_dir = f"{work_dir}/mcp_servers"
    config_dir = getattr(sandbox.config, "config_file_dir", None)

    files_to_upload: list[tuple[str, str, str]] = []
    expected_files: set[str] = set()
    #: sandbox filename -> the host file already staged under it.
    claimed: dict[str, str] = {}

    # Built-ins only: only built-in servers ship host-local ``uv run python``
    # files. User servers run via npx/uvx/http and have nothing to upload here.
    for server in sandbox._builtin_servers():
        if not server.enabled:
            continue
        if server.transport == "stdio" and server.command == "uv":
            if (
                len(server.args) >= 3
                and server.args[0] == "run"
                and server.args[1] == "python"
            ):
                resolved = _resolve_local_path(server.args[2], config_dir)
                if resolved:
                    filename = Path(resolved).name
                    sandbox_path = f"{mcp_servers_dir}/{filename}"
                    # The sandbox is one flat directory, so the file a server
                    # came from is gone by the time it lands: two servers that
                    # name their entry point the same thing stage to the same
                    # destination. Bundled servers never get this far -- a
                    # collision between them is dropped at composition, in
                    # bundled_mcp_servers -- so what reaches here is a server
                    # an operator added in YAML. Skipping the upload keeps the
                    # first server correct; the second still launches against
                    # the surviving file, which the error names so the fix is
                    # to rename, not to guess.
                    if (clash := claimed.get(filename)) and clash != resolved:
                        logger.error(
                            "MCP server entry points collide in the sandbox: "
                            f"{clash} and {resolved} both stage as {filename}",
                            server=server.name,
                        )
                        continue
                    claimed[filename] = resolved
                    expected_files.add(filename)
                    files_to_upload.append((server.name, resolved, sandbox_path))
                else:
                    searched = [server.args[2]]
                    if config_dir:
                        searched.append(str(config_dir / server.args[2]))
                    logger.warning(
                        f"MCP server file not found: {server.args[2]}",
                        server=server.name,
                        searched_paths=searched,
                    )

    # Shared runtime siblings (imported by the server files) ship alongside
    # them; adding them to expected_files also shields them from the prune.
    if files_to_upload:
        for shared_name in _MCP_SHARED_RUNTIME_FILES:
            shared = _resolve_local_path(f"mcp_servers/{shared_name}", config_dir)
            if shared:
                expected_files.add(shared_name)
                files_to_upload.append(
                    ("_shared", shared, f"{mcp_servers_dir}/{shared_name}")
                )

    assert sandbox.runtime is not None
    runtime = sandbox.runtime

    await sandbox._runtime_call(
        runtime.exec,
        f"mkdir -p {mcp_servers_dir}",
        retry_policy=RetryPolicy.SAFE,
    )

    # Prune stale files — single rm command instead of N
    existing_entries = await sandbox.als_directory(mcp_servers_dir)
    if existing_entries:
        files_to_remove = [
            entry["path"]
            for entry in existing_entries
            if not entry.get("is_dir", False)
            and entry.get("name") not in expected_files
            and entry.get("name")
            not in (".mcp_manifest.json", ".sandbox_manifest.json")
        ]
        if files_to_remove:
            rm_cmd = "rm -f " + " ".join(shlex.quote(p) for p in files_to_remove)
            await sandbox._runtime_call(
                runtime.exec,
                rm_cmd,
                retry_policy=RetryPolicy.SAFE,
            )
            logger.info(
                "Pruned MCP server files",
                removed=len(files_to_remove),
                sandbox_root=mcp_servers_dir,
            )

    # Batch upload — single HTTP request via upload_files
    if files_to_upload:
        batch = [
            (local, remote)
            for _, local, remote in files_to_upload
        ]
        await sandbox._runtime_call(
            runtime.upload_files,
            batch,
            retry_policy=RetryPolicy.SAFE,
        )
        for name, local, remote in files_to_upload:
            logger.info(
                "Uploaded MCP server file",
                server=name,
                local_path=local,
                sandbox_path=remote,
            )


async def sync_sandbox_assets(
    sandbox: "PTCSandbox",
    *,
    skill_dirs: list[tuple[str, str]] | None = None,
    user_skill_dir: tuple[str, str] | None = None,
    disabled_skills: frozenset[str] = frozenset(),
    reusing_sandbox: bool = False,
    force_refresh: bool = False,
    tokens: dict | None = None,
    user_id: str | None = None,
    workspace_id: str | None = None,
    on_progress: Callable[[str], None] | None = None,
) -> SyncResult:
    """Sync all sandbox assets using a single unified manifest.

        Replaces the previous ``sync_tools()`` and ``sync_skills()`` methods
        with a single entry point that tracks MCP servers, data client, tool
        modules, skills, and tokens in one manifest file.

        Args:
            skill_dirs: Ordered list of (local_path, sandbox_path) for skills.
            user_skill_dir: (host cache view, sandbox base) for the user's
                server-managed skill tier. A separate param, not another
                ``skill_dirs`` entry, because the manifest computation must
                know which root is managed to stamp the right lock ownership.
            disabled_skills: Builtin skill names this user disabled — excluded
                from the local set, so the prune removes them from the sandbox.
            reusing_sandbox: Whether reconnecting to an existing sandbox.
            force_refresh: Force re-upload of all modules regardless of manifest.
            tokens: Pre-minted OAuth tokens (from workspace_manager).
            user_id: User ID for token tracking.
            workspace_id: Workspace ID for token tracking.
            on_progress: Optional callback for reporting progress.

        Returns:
            SyncResult with list of refreshed module names.
        """
    await sandbox._wait_ready()

    # Fold the managed user tier into the source list (last, so it can never
    # be overridden); which root is managed travels separately.
    managed_root = user_skill_dir[0] if user_skill_dir else None
    if user_skill_dir:
        skill_dirs = list(skill_dirs or []) + [user_skill_dir]

    async with sandbox._tool_refresh_lock:
        await sandbox.ensure_sandbox_ready()

        _t0 = time.time()
        _sync_phases: dict[str, float] = {}

        def _mark_sync(name: str) -> None:
            nonlocal _t0
            now = time.time()
            _sync_phases[name] = (now - _t0) * 1000
            _t0 = now

        # Steps 0+1+2: all three are independent — parallelize
        # _prune_disabled_tool_modules → sandbox rm (disjoint from manifest paths)
        # _compute_sandbox_manifest → local CPU/disk only
        # _read_unified_manifest → sandbox HTTP GET
        skill_roots = [d for d, _ in skill_dirs] if skill_dirs else None

        _, local_manifest, remote_manifest = await asyncio.gather(
            sandbox._prune_disabled_tool_modules(),
            sandbox._compute_sandbox_manifest(
                skill_roots=skill_roots,
                managed_skill_root=managed_root,
                disabled_skills=disabled_skills,
                tokens=tokens,
                user_id=user_id,
                workspace_id=workspace_id,
            ),
            sandbox._read_unified_manifest(),
        )
        _mark_sync("manifest")

        # 2b. Run layout migrations if needed (zero cost when current)
        remote_layout = (remote_manifest or {}).get("layout_version", 1)
        await run_layout_migrations(
            sandbox.runtime, sandbox._work_dir, remote_layout
        )

        # 3. Determine which modules changed (pure CPU)
        if force_refresh or remote_manifest is None or not reusing_sandbox:
            changed_modules = set(local_manifest["modules"].keys())
        else:
            changed_modules: set[str] = set()
            for mod_name, mod_data in local_manifest["modules"].items():
                remote_mod = remote_manifest.get("modules", {}).get(mod_name)
                if mod_name == "tokens":
                    if sandbox._token_needs_refresh(
                        remote_mod, tokens, user_id, workspace_id
                    ):
                        changed_modules.add("tokens")
                elif (
                    remote_mod is None
                    or remote_mod.get("version") != mod_data["version"]
                ):
                    changed_modules.add(mod_name)

        if not changed_modules:
            # `is None` guard (like the slow path): the cached manifest may
            # already hold the merged local+agent-installed view — overwriting
            # it with the local-only view would drop agent-installed skills
            # from known_skills on every warm reuse.
            if sandbox._skills_manifest is None and "skills" in local_manifest["modules"]:
                skills_mod = local_manifest["modules"]["skills"]
                if skill_dirs:
                    # Cold process / reconnect: one lock read folds the
                    # agent-installed entries in without a filesystem re-scan.
                    sandbox_base = skill_dirs[-1][1].rstrip("/")
                    existing_lock = await sandbox._download_skills_lock(sandbox_base)
                    if existing_lock:
                        sandbox._build_complete_skills_cache(
                            skills_mod, {"skills": existing_lock}, sandbox_base
                        )
                if sandbox._skills_manifest is None:
                    sandbox._skills_manifest = skills_mod
            return SyncResult(refreshed_modules=[], forced=False)

        refreshed: list[str] = []
        skill_collisions: set[str] = set()

        # 4. Upload changed modules
        # Intent-based ordering: tool_modules after mcp_servers (derived from
        # MCP definitions). All other modules write to disjoint sandbox paths
        # and are safe to run in parallel.

        async def _do_skills_upload() -> None:
            """Skills sub-chain: collect → prune → upload (internally sequential)."""
            local_skill_names = await _collect_local_skill_names(
                [d for d, _ in skill_dirs],
                disabled=disabled_skills,
            )
            sandbox_base = skill_dirs[-1][1].rstrip("/")  # type: ignore[index]

            # Download existing lock file once (shared by prune + upload)
            existing_lock = await sandbox._download_skills_lock(sandbox_base)

            await sandbox._prune_remote_skills(
                sandbox_base, local_skill_names, existing_lock=existing_lock
            )
            skills_mod = local_manifest["modules"].get("skills", {})
            if skills_mod.get("files"):
                merged_lock, collisions = await sandbox._upload_skills(
                    skill_dirs,
                    manifest=skills_mod,  # type: ignore[arg-type]
                    existing_lock=existing_lock,
                    disabled=disabled_skills,
                )
                skill_collisions.update(collisions)
                # Build complete skills cache from merged lock data
                if merged_lock:
                    sandbox._build_complete_skills_cache(
                        skills_mod, merged_lock, sandbox_base
                    )

        # Group 1: independent uploads in parallel
        parallel_uploads: list[tuple[str, Any]] = []
        if "mcp_servers" in changed_modules:
            if on_progress:
                on_progress("Syncing MCP server files...")
            parallel_uploads.append(
                ("mcp_servers", sandbox._upload_mcp_server_files_impl())
            )
        if "internal_packages" in changed_modules:
            if on_progress:
                on_progress("Syncing internal packages...")
            parallel_uploads.append(
                ("internal_packages", sandbox._upload_internal_packages())
            )
        if "skills" in changed_modules and skill_dirs:
            if on_progress:
                on_progress("Syncing skills...")
            parallel_uploads.append(("skills", _do_skills_upload()))
        if "tokens" in changed_modules and tokens:
            if on_progress:
                on_progress("Uploading tokens...")
            parallel_uploads.append(("tokens", sandbox.upload_token_file(tokens)))

        if parallel_uploads:
            await asyncio.gather(*[coro for _, coro in parallel_uploads])
            refreshed.extend(name for name, _ in parallel_uploads)
        _mark_sync("uploads")

        # Group 2: tool_modules AFTER mcp_servers (intent: derived from MCP definitions)
        if "tool_modules" in changed_modules:
            if on_progress:
                on_progress("Regenerating tool modules...")
            await sandbox._install_tool_modules()
            refreshed.append("tool_modules")
            _mark_sync("tool_modules")
            try:
                await sandbox._start_internal_mcp_servers()
            except Exception as e:
                logger.warning("Failed to refresh MCP servers", error=str(e))
            _mark_sync("mcp_start")

        # Cache skills metadata (only if not already set by _build_complete_skills_cache,
        # which includes user-installed skills from the lock file)
        if sandbox._skills_manifest is None and "skills" in local_manifest["modules"]:
            sandbox._skills_manifest = local_manifest["modules"]["skills"]

        # A collision-skipped upload must not read as done: fold the skipped
        # names into the written version so the next sync's freshly computed
        # version (which never carries `collisions`) differs and retries.
        # Converges once the colliding agent-installed skill is removed.
        if skill_collisions:
            mod = local_manifest["modules"]["skills"]
            collision_key = ",".join(sorted(skill_collisions))
            mod["collisions"] = sorted(skill_collisions)
            mod["version"] = hashlib.sha256(
                f"{mod['version']}|collisions:{collision_key}".encode()
            ).hexdigest()

        # Steps 5+6: independent — parallelize
        await asyncio.gather(
            sandbox._write_unified_manifest(local_manifest),
            sandbox._cleanup_legacy_manifests(),
        )
        _mark_sync("finalize")

        total = sum(_sync_phases.values())
        phases = " ".join(f"{k}={v:.0f}ms" for k, v in _sync_phases.items())
        logger.info(
            f"[ASSET_SYNC] total={total:.0f}ms ({phases}) "
                f"changed={','.join(sorted(refreshed)) or 'none'}"
        )
        # Mirror the [ASSET_SYNC] log into OTel: one phase histogram sample
        # per bucket + a total, labeled by whether any module changed (so
        # dashboards can split fast no-op syncs from expensive ones).
        _reuse_label = "reuse" if reusing_sandbox else "fresh"
        safe_record(
            sandbox_asset_sync_total_ms,
            total,
            {"changed": "yes" if refreshed else "no", "sandbox": _reuse_label},
        )
        for _phase, _ms in _sync_phases.items():
            safe_record(
                sandbox_asset_sync_phase_duration_ms,
                _ms,
                {"phase": _phase, "sandbox": _reuse_label},
            )
        return SyncResult(refreshed_modules=refreshed, forced=force_refresh)


async def _prune_disabled_tool_modules(sandbox: "PTCSandbox") -> None:
    if not sandbox.runtime or sandbox._disabled_modules_pruned:
        return

    runtime = sandbox.runtime
    disabled = [
        server.name for server in sandbox.config.mcp.servers if not server.enabled
    ]
    if not disabled:
        sandbox._disabled_modules_pruned = True
        return

    work_dir = sandbox._work_dir
    paths: list[str] = []
    for name in disabled:
        paths.append(f"{work_dir}/tools/{name}.py")
        paths.append(f"{work_dir}/tools/docs/{name}")

    async def remove_one(path: str) -> None:
        await sandbox._runtime_call(
            runtime.exec,
            f"rm -rf {shlex.quote(path)}",
            retry_policy=RetryPolicy.SAFE,
        )

    await asyncio.gather(*[remove_one(path) for path in paths])
    sandbox._disabled_modules_pruned = True
    logger.debug("Pruned disabled tool modules", removed=len(paths))


async def _collect_local_skill_names(
    local_skill_roots: list[str],
    *,
    disabled: frozenset[str] = frozenset(),
) -> set[str]:
    def build() -> set[str]:
        sandbox_skill_names, all_registry_names = _get_sandbox_eligible_skills()

        names: set[str] = set()
        for root_str in local_skill_roots:
            root = Path(root_str).expanduser()
            if not root.exists():
                continue
            for skill_dir in root.iterdir():
                if not skill_dir.is_dir():
                    continue
                if not (skill_dir / "SKILL.md").exists():
                    continue
                skill_name = skill_dir.name
                # Skip disabled + flash-only skills so they get pruned from sandbox
                if skill_name in disabled:
                    continue
                if (
                    skill_name not in sandbox_skill_names
                    and skill_name in all_registry_names
                ):
                    continue
                names.add(skill_name)
        return names

    return await asyncio.to_thread(build)


async def _download_skills_lock(
    sandbox: "PTCSandbox", sandbox_skills_base: str
) -> dict[str, Any] | None:
    """Download and parse the existing skills-lock.json from sandbox.

        Returns parsed skill entries dict, or None if missing/corrupt.
        """
    from ptc_agent.agent.middleware.skills.lock import LOCK_FILENAME, parse_skills_lock

    lock_path = f"{sandbox_skills_base}/{LOCK_FILENAME}"
    assert sandbox.runtime is not None
    try:
        raw = await sandbox._runtime_call(
            sandbox.runtime.download_file,
            lock_path,
            retry_policy=RetryPolicy.SAFE,
        )
        if raw:
            text = raw.decode("utf-8") if isinstance(raw, bytes) else raw
            return parse_skills_lock(text)
    except Exception:
        logger.debug("No existing skills-lock.json (fresh sandbox or error)")
    return None


def _build_complete_skills_cache(
    sandbox: "PTCSandbox",
    skills_mod: dict[str, Any],
    merged_lock: dict[str, Any],
    sandbox_skills_base: str,
) -> None:
    """Merge user-installed skills from lock file into the skills manifest cache.

        This ensures known_skills in agent.py includes both platform and
        user-installed skills, eliminating per-message downloads.
        """
    from ptc_agent.agent.middleware.skills.lock import lock_entry_to_skill_metadata

    all_skills = dict(skills_mod.get("skills", {}))

    lock_skills = merged_lock.get("skills", {})
    for name, entry in lock_skills.items():
        if entry.get("owner") == "user" and name not in all_skills:
            skill_path = f"{sandbox_skills_base}/{name}/SKILL.md"
            meta = lock_entry_to_skill_metadata(entry, skill_path)
            all_skills[name] = dict(meta)

    sandbox._skills_manifest = {**skills_mod, "skills": all_skills}


async def _prune_remote_skills(
    sandbox: "PTCSandbox",
    sandbox_base: str,
    local_skill_names: set[str],
    *,
    existing_lock: dict[str, Any] | None = None,
) -> None:
    """Prune stale server-authoritative skills, protecting agent-installed ones.

        Safe default: if lock is unavailable or a skill has no lock entry,
        it is preserved to prevent data loss on transient failures.
        """
    from ptc_agent.agent.middleware.skills.lock import is_agent_installed, is_linked
    assert sandbox.runtime is not None
    runtime = sandbox.runtime
    entries = await sandbox.als_directory(sandbox_base)
    if not entries:
        return

    paths_to_remove: list[str] = []
    for entry in entries:
        name = entry.get("name")
        if not name:
            continue
        if not entry.get("is_dir", False):
            continue
        if name in local_skill_names:
            continue  # Current platform skill — will be re-uploaded

        # Unknown skill — check lock for ownership
        if existing_lock is None:
            # Lock unavailable — safe default: preserve everything
            continue
        lock_entry = existing_lock.get(name)
        if lock_entry is None:
            # Not in lock — unknown origin, preserve (safe default)
            continue
        if is_agent_installed(lock_entry):
            # Agent-installed — no server-side source of truth, never prune
            logger.debug("Preserving agent-installed skill", skill=name)
            continue
        if is_linked(lock_entry):
            # Two-way synced workspace skill — the reconciler is its sole
            # writer/pruner; a disabled linked row keeps its files on purpose.
            logger.debug("Preserving linked workspace skill", skill=name)
            continue
        # Platform or server-managed skill no longer in local set — stale
        # (deleted or disabled server-side), prune it
        paths_to_remove.append(entry["path"])

    if not paths_to_remove:
        return

    async def remove_one(path: str) -> None:
        await sandbox._runtime_call(
            runtime.exec,
            f"rm -rf {shlex.quote(path)}",
            retry_policy=RetryPolicy.SAFE,
        )

    await asyncio.gather(*[remove_one(path) for path in paths_to_remove])
    logger.info(
        "Pruned stale platform skills from sandbox",
        removed=len(paths_to_remove),
        sandbox_root=sandbox_base,
    )


async def _upload_skills(
    sandbox: "PTCSandbox",
    local_skills_dirs: list[tuple[str, str]],
    *,
    manifest: dict[str, Any] | None = None,
    existing_lock: dict[str, Any] | None = None,
    disabled: frozenset[str] = frozenset(),
) -> tuple[dict[str, Any] | None, set[str]]:
    """Upload skill files from local filesystem to sandbox.

        Uses a two-pass approach to fix override precedence:
        - Pass 1 (local I/O only): Walk all sources, later sources overwrite earlier
          ones for the same skill_name — each skill appears exactly once.
        - Pass 2 (sandbox I/O): Single rm, single mkdir, parallel per-skill batch uploads.

        Args:
            local_skills_dirs: List of (local_path, sandbox_path) tuples.
                Example: [("~/.ptc-agent/skills", "{working_directory}/skills")]
            manifest: Pre-computed skills manifest. If None, computed from local_skills_dirs.
            existing_lock: Previously downloaded lock entries, or None for fresh sandbox.

        Returns:
            ``(merged_lock_or_None, collisions)`` — the merged lock file dict if
            lock entries were written, plus the names skipped because
            agent-installed or two-way-synced content occupies them (the caller
            folds these into the module version so the skipped upload retries).
        """
    from ptc_agent.agent.middleware.skills.lock import is_agent_installed, is_linked

    assert sandbox.runtime is not None
    runtime = sandbox.runtime

    if manifest is None:
        local_roots = [local_dir for local_dir, _ in local_skills_dirs]
        manifest = await sandbox._compute_skills_module(local_roots, disabled=disabled)

    if not manifest.get("files"):
        logger.debug("No skills found; skipping upload")
        return None, set()

    # Skills eligible for sandbox upload (exposure "ptc" or "both")
    sandbox_skill_names, all_registry_names = _get_sandbox_eligible_skills()

    # ── Pass 1: Planning (local I/O only) ──
    # For each skill, collect files from the *last* source that provides it.
    # Key: skill_name → (sandbox_skill_dir, list of (local_file, sandbox_dest))
    @dataclass
    class _SkillPlan:
        sandbox_dir: str
        files: list[tuple[Path, str]] = field(default_factory=list)
        subdirs: set[str] = field(default_factory=set)

    final_skills: dict[str, _SkillPlan] = {}

    def _list_skill_dirs(local_root: Path) -> list[Path]:
        dirs: list[Path] = []
        for entry in local_root.iterdir():
            if not entry.is_dir():
                continue
            if not (entry / "SKILL.md").exists():
                continue
            dirs.append(entry)
        return dirs

    def _list_skill_files(skill_dir: Path) -> list[Path]:
        return [
            p
            for p in skill_dir.rglob("*")
            if p.is_file()
            and "__pycache__" not in p.parts
            and p.name != "LICENSE.txt"
        ]

    def _plan_all() -> None:
        for local_dir, sandbox_dir in local_skills_dirs:
            local_path = Path(local_dir).expanduser()
            if not local_path.exists():
                continue

            for skill_dir in _list_skill_dirs(local_path):
                skill_name = skill_dir.name
                if skill_name in ("", ".", ".."):
                    continue
                if skill_name in disabled:
                    continue
                if (
                    skill_name not in sandbox_skill_names
                    and skill_name in all_registry_names
                ):
                    continue

                sandbox_skill_dir = f"{sandbox_dir.rstrip('/')}/{skill_name}"
                plan = _SkillPlan(sandbox_dir=sandbox_skill_dir)

                for fp in _list_skill_files(skill_dir):
                    rel = fp.relative_to(skill_dir)
                    dest = f"{sandbox_skill_dir}/{rel}"
                    plan.files.append((fp, dest))
                    if len(rel.parts) > 1:
                        plan.subdirs.add(f"{sandbox_skill_dir}/{rel.parent}")

                # Later source overwrites earlier for same skill_name
                final_skills[skill_name] = plan

    await asyncio.to_thread(_plan_all)

    # Never write over content this path does not own: a planned skill whose
    # name an agent-installed or reconciler-linked skill already occupies is
    # skipped this cycle (the caller version-stamps the collision so it
    # retries once the name frees up). Same predicate pair as the prune side.
    if existing_lock:
        collisions = {
            name
            for name in final_skills
            if name in existing_lock
            and (
                is_agent_installed(existing_lock[name])
                or is_linked(existing_lock[name])
            )
        }
    else:
        # No lock means either a fresh sandbox or a failed read, and the two
        # are indistinguishable here. Ownership is unknowable, so every name
        # already on disk is treated as owned by someone else. A fresh sandbox
        # has no dirs and uploads everything; a failed read defers to the next
        # pass rather than writing over an agent's files. Prune takes the same
        # posture on the same signal.
        #
        # The trade is deliberate: a sandbox whose lock was deleted outright
        # stops refreshing its platform skills until it is recreated, because
        # every name it holds now reads as foreign. Stale skills that log a
        # warning every pass beat silently overwriting an agent's own files on
        # a transient download failure.
        sandbox_base = local_skills_dirs[-1][1].rstrip("/")
        existing_dirs = {
            e.get("name")
            for e in (await sandbox.als_directory(sandbox_base) or [])
            if e.get("is_dir")
        }
        collisions = {name for name in final_skills if name in existing_dirs}
    if collisions:
        for name in collisions:
            del final_skills[name]
        # One line, not one per name: an unreadable lock makes every skill on
        # disk collide at once, and thirty identical warnings bury the flag
        # that says which of the two branches produced them.
        logger.warning(
            "Skill upload skipped: the names are occupied by content this path "
            "cannot prove it owns",
            skills=sorted(collisions),
            count=len(collisions),
            lock_read=bool(existing_lock),
        )

    if not final_skills:
        logger.debug("No skills to upload after planning")
        return None, collisions

    # ── Pass 2: Execute (minimal sandbox I/O) ──
    # 1. Single rm for clean slate (all skill dirs that will be uploaded).
    # Every surviving name is either lock-verified as ours or absent from the
    # sandbox, so the rm only ever clears a dir this path is about to rewrite.
    rm_targets = [plan.sandbox_dir for plan in final_skills.values()]
    if rm_targets:
        rm_cmd = "rm -rf " + " ".join(shlex.quote(d) for d in rm_targets)
        await sandbox._runtime_call(
            runtime.exec,
            rm_cmd,
            retry_policy=RetryPolicy.SAFE,
        )

    # 2. Single mkdir for all skill dirs + subdirs
    mkdir_targets: set[str] = set()
    for plan in final_skills.values():
        mkdir_targets.add(plan.sandbox_dir)
        mkdir_targets.update(plan.subdirs)
    if mkdir_targets:
        mkdir_cmd = "mkdir -p " + " ".join(
            shlex.quote(d) for d in sorted(mkdir_targets)
        )
        await sandbox._runtime_call(
            runtime.exec,
            mkdir_cmd,
            retry_policy=RetryPolicy.SAFE,
        )

    # 3. Parallel per-skill batch uploads — no race since planning collapsed duplicates
    upload_coros = []
    for plan in final_skills.values():
        if plan.files:
            batch = [
                (str(fp), dest)
                for fp, dest in plan.files
            ]
            upload_coros.append(
                sandbox._runtime_call(
                    runtime.upload_files,
                    batch,
                    retry_policy=RetryPolicy.SAFE,
                )
            )
    if upload_coros:
        await asyncio.gather(*upload_coros)

    logger.debug(
        "Uploaded skills to sandbox",
        skill_count=len(final_skills),
        file_count=len(manifest.get("files", {})),
    )

    # --- Lock file merge + write ---
    # Build authoritative lock entries (platform + managed) from the manifest.
    # Collision-skipped names stay OUT: their files were not written, so the
    # lock must keep claiming the agent-installed entry, not the server one.
    platform_entries = {}
    skills_metadata = manifest.get("skills", {})
    for skill_name, skill_meta in skills_metadata.items():
        if skill_name in collisions:
            continue
        lock_entry = skill_meta.get("lock_entry")
        if lock_entry:
            platform_entries[skill_name] = lock_entry

    if platform_entries or existing_lock:
        # The write is a fresh read-merge-write inside the sandbox, under the
        # same .skills-sync.flock the reconciler holds for its passes — a
        # host-side merge over `existing_lock` would be a snapshot taken
        # before the multi-second prune/upload above, and writing it blind
        # would silently drop anything the reconciler committed in between
        # (whose next prune pass could then delete content the lock should
        # have protected).
        from .skill_sync import merge_authoritative_entries

        merged, lock_skipped = await merge_authoritative_entries(
            sandbox, platform_entries
        )
        if lock_skipped:
            # A name this pass uploaded was claimed agent/linked since the
            # collision check; the claim keeps the lock entry, and the
            # reconciler's tree-hash check arbitrates the bytes next pass.
            logger.warning(
                "Skill lock entries ceded to concurrent agent/linked claims",
                skills=sorted(lock_skipped),
            )
        logger.debug(
            "Skills lock file merged",
            platform_count=len(platform_entries),
            user_count=sum(
                1
                for e in merged["skills"].values()
                if e.get("owner") == "user"
            ),
        )
        return dict(merged), collisions

    return None, collisions
