"""Sandbox asset & skills sync — manifests, hashing, uploads, pruning.

Functions take the owning ``PTCSandbox`` as their explicit first argument;
``PTCSandbox`` exposes same-name delegators, so call sites and patch
semantics are unchanged.
"""

import asyncio
import hashlib
import json
import shlex
import textwrap
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
    """Hash user (``source='workspace'``) server CONFIG — never secret values.

        Captures transport/command/args/url, the full env/header maps (literal
        values AND ``${vault:NAME}`` ref strings — the stored values are never
        resolved secrets), and the effective secret-less-discovery decision, so a
        config-only edit — a literal ``MODE=prod`` -> ``staging`` change, a new
        authenticated header, or a vault-ref retarget under the same key — always
        re-uploads the regenerated ``mcp_client.py``. Shares
        :func:`discovery_affecting_payload` with the per-server discovery-cache
        key so the upload hash and the cache key can never disagree. Returns ""
        when there are no user servers so builtin-only workspaces are untouched.
        """
    user_servers = sandbox._user_servers()
    if not user_servers:
        return ""

    parts: list[str] = []
    for server in sorted(user_servers, key=lambda s: s.name):
        payload = discovery_affecting_payload(server, include_identity=True)
        parts.append(json.dumps(payload, sort_keys=True))
    return hashlib.sha256("\n".join(parts).encode()).hexdigest()


async def _compute_skills_module(sandbox: "PTCSandbox", skill_roots: list[str]) -> dict[str, Any]:
    """Compute a skills module manifest with content-based SHA-256 hashing.

        Unlike the legacy ``_compute_skills_manifest`` (size+mtime), this hashes
        actual file contents so the manifest is deterministic and portable.
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

            for skill_dir in root.iterdir():
                if not skill_dir.is_dir():
                    continue
                if not (skill_dir / "SKILL.md").exists():
                    continue

                skill_name = skill_dir.name
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

                # Build lock entry for platform skill
                from ptc_agent.agent.middleware.skills.lock import build_lock_entry

                content_hash = f"sha256:{_sha256_file(skill_dir / 'SKILL.md')}"
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
    # /command/args/url/header-NAMES — never values) changes this hash and
    # so re-uploads the regenerated mcp_client.py via the tool_modules diff.
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
        modules["skills"] = await sandbox._compute_skills_module(skill_roots)

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
            if "skills" in local_manifest["modules"]:
                sandbox._skills_manifest = local_manifest["modules"]["skills"]
            return SyncResult(refreshed_modules=[], forced=False)

        refreshed: list[str] = []

        # 4. Upload changed modules
        # Intent-based ordering: tool_modules after mcp_servers (derived from
        # MCP definitions). All other modules write to disjoint sandbox paths
        # and are safe to run in parallel.

        async def _do_skills_upload() -> None:
            """Skills sub-chain: collect → prune → upload (internally sequential)."""
            local_skill_names = await sandbox._collect_local_skill_names(
                [d for d, _ in skill_dirs]  # type: ignore[union-attr]
            )
            sandbox_base = skill_dirs[-1][1].rstrip("/")  # type: ignore[index]

            # Download existing lock file once (shared by prune + upload)
            existing_lock = await sandbox._download_skills_lock(sandbox_base)

            await sandbox._prune_remote_skills(
                sandbox_base, local_skill_names, existing_lock=existing_lock
            )
            skills_mod = local_manifest["modules"].get("skills", {})
            if skills_mod.get("files"):
                merged_lock = await sandbox._upload_skills(
                    skill_dirs,
                    manifest=skills_mod,  # type: ignore[arg-type]
                    existing_lock=existing_lock,
                )
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
    sandbox: "PTCSandbox", local_skill_roots: list[str]
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
                # Skip flash-only skills so they get pruned from sandbox
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


async def sync_skills_lock(sandbox: "PTCSandbox") -> None:
    """Reconcile skills-lock.json with the actual filesystem state.

        Bidirectional sync in a single sandbox exec (1 API call):
        - **Remove** lock entries whose skill directories no longer exist
        - **Add** lock entries for skill directories not yet in the lock
          (parses SKILL.md frontmatter to populate name/description/metadata)

        Fast path: if no lock file exists and no skill directories exist,
        exits immediately.  If lock is perfectly in sync, no write occurs.

        Intended to be called post-completion alongside file backup.
        Self-healing in discovery.py serves as a fallback if this fails.
        """
    if not sandbox.runtime:
        return
    skills_base = f"{sandbox._work_dir}/.agents/skills"
    lock_path = f"{skills_base}/skills-lock.json"

    # Single inline Python script that runs entirely in the sandbox.
    # Reads dirs + lock file, diffs, parses SKILL.md for new entries,
    # writes updated lock — all in one exec round trip.
    # Uses json.dumps() for path interpolation (not shlex.quote) because
    # values appear as Python string literals inside python3 -c.
    script = textwrap.dedent(f"""\
            python3 -c '
import json, os, re, hashlib, sys
from datetime import datetime, timezone

SKILLS_BASE = {json.dumps(skills_base)}
LOCK_PATH = {json.dumps(lock_path)}

# 1. List skill dirs (only dirs containing SKILL.md)
dirs = set()
if os.path.isdir(SKILLS_BASE):
    for name in os.listdir(SKILLS_BASE):
        p = os.path.join(SKILLS_BASE, name)
        if os.path.isdir(p) and os.path.isfile(os.path.join(p, "SKILL.md")):
            dirs.add(name)

# 2. Read existing lock
lock_data = {{"version": 1, "skills": {{}}}}
if os.path.isfile(LOCK_PATH):
    try:
        with open(LOCK_PATH) as f:
            lock_data = json.load(f)
    except (json.JSONDecodeError, OSError):
        pass
skills = lock_data.get("skills", {{}})

# 3. Compute diff
locked_names = set(skills.keys())
to_remove = locked_names - dirs
to_add = dirs - locked_names

if not to_remove and not to_add:
    print(json.dumps({{"status": "noop", "removed": 0, "added": 0}}))
    sys.exit(0)

# 4. Remove stale entries
for name in to_remove:
    del skills[name]

# 5. Add new entries by parsing SKILL.md frontmatter
now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
for name in sorted(to_add):
    skill_md = os.path.join(SKILLS_BASE, name, "SKILL.md")
    desc = ""
    confirmed = False
    meta = {{}}
    license_val = None
    allowed_tools = []
    try:
        with open(skill_md, errors="replace") as f:
            content = f.read(1048576)  # 1MB cap
        content = content.replace("\\r\\n", "\\n")
        m = re.match(r"^---\\s*\\n(.*?)\\n---\\s*(?:\\n|$)", content, re.DOTALL)
        if m:
            # Minimal YAML-like parser for simple key: value frontmatter
            # Avoids PyYAML dependency in sandbox
            for line in m.group(1).splitlines():
                line = line.strip()
                if ":" in line:
                    k, _, v = line.partition(":")
                    k, v = k.strip(), v.strip()
                    if k == "description":
                        desc = v.strip("\\"\\x27")
                        confirmed = True
                    elif k == "license":
                        license_val = v.strip("\\"\\x27") or None
            confirmed = confirmed and bool(name)
        content_hash = "sha256:" + hashlib.sha256(content.encode()).hexdigest()
    except Exception:
        content_hash = ""

    skills[name] = {{
        "name": name,
        "description": desc,
        "owner": "user",
        "source": "local",
        "sourceType": "local",
        "computedHash": content_hash,
        "confirmed": confirmed,
        "license": license_val,
        "metadata": meta,
        "allowed_tools": allowed_tools,
        "installedAt": now,
        "updatedAt": now,
    }}

# 6. Write updated lock atomically
lock_data["skills"] = skills
os.makedirs(os.path.dirname(LOCK_PATH), exist_ok=True)
tmp = LOCK_PATH + ".tmp"
try:
    with open(tmp, "w") as f:
        json.dump(lock_data, f, sort_keys=True, indent=2, ensure_ascii=False)
        f.write("\\n")
    os.replace(tmp, LOCK_PATH)
    print(json.dumps({{"status": "ok", "removed": len(to_remove), "added": len(to_add)}}))
except OSError as e:
    try:
        os.unlink(tmp)
    except OSError:
        pass
    print(json.dumps({{"status": "error", "error": str(e)}}))
    sys.exit(1)
'
        """)

    try:
        result = await sandbox._runtime_call(
            sandbox.runtime.exec,
            script.strip(),
            retry_policy=RetryPolicy.SAFE,
        )
        stdout = (getattr(result, "stdout", "") or "").strip()
        if stdout:
            try:
                info = json.loads(stdout)
                if info.get("status") == "ok":
                    logger.info(
                        "Skills lock synced",
                        removed=info.get("removed", 0),
                        added=info.get("added", 0),
                        skills_base=skills_base,
                    )
                elif info.get("status") == "noop":
                    logger.debug("Skills lock already in sync")
            except json.JSONDecodeError:
                pass
    except Exception as e:
        logger.debug("Skills lock sync failed (non-critical)", error=str(e))


async def _prune_remote_skills(
    sandbox: "PTCSandbox",
    sandbox_base: str,
    local_skill_names: set[str],
    *,
    existing_lock: dict[str, Any] | None = None,
) -> None:
    """Prune stale platform skills from sandbox, protecting user-installed ones.

        Safe default: if lock is unavailable or a skill has no lock entry,
        it is preserved to prevent data loss on transient failures.
        """
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
        if lock_entry.get("owner") == "user":
            # User-installed — never prune
            logger.debug("Preserving user-installed skill", skill=name)
            continue
        # Platform skill no longer in local set — stale, prune it
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
) -> dict[str, Any] | None:
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
            Merged lock file dict if lock entries were written, else None.
        """
    assert sandbox.runtime is not None
    runtime = sandbox.runtime

    if manifest is None:
        local_roots = [local_dir for local_dir, _ in local_skills_dirs]
        manifest = await sandbox._compute_skills_module(local_roots)

    if not manifest.get("files"):
        logger.debug("No skills found; skipping upload")
        return

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

    if not final_skills:
        logger.debug("No skills to upload after planning")
        return

    # ── Pass 2: Execute (minimal sandbox I/O) ──
    # 1. Single rm for clean slate (all skill dirs that will be uploaded)
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
    # Build platform lock entries from the manifest
    platform_entries = {}
    skills_metadata = manifest.get("skills", {})
    for skill_name, skill_meta in skills_metadata.items():
        lock_entry = skill_meta.get("lock_entry")
        if lock_entry:
            platform_entries[skill_name] = lock_entry

    if platform_entries or existing_lock:
        from ptc_agent.agent.middleware.skills.lock import (
            LOCK_FILENAME,
            merge_lock_files,
            serialize_skills_lock,
        )

        merged = merge_lock_files(platform_entries, existing_lock)
        lock_content = serialize_skills_lock(merged)

        # Write lock file to sandbox
        sandbox_base = local_skills_dirs[-1][1].rstrip("/")
        lock_path = f"{sandbox_base}/{LOCK_FILENAME}"
        await sandbox._runtime_call(
            runtime.upload_file,
            lock_content.encode("utf-8"),
            lock_path,
            retry_policy=RetryPolicy.SAFE,
        )
        logger.debug(
            "Skills lock file written",
            path=lock_path,
            platform_count=len(platform_entries),
            user_count=sum(
                1
                for e in merged["skills"].values()
                if e.get("owner") == "user"
            ),
        )
        return dict(merged)

    return None
