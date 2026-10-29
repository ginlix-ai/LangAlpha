"""Workspace Sandbox API Router.

Provides sandbox resource stats, disk usage, installed packages,
and package installation for a workspace's Daytona sandbox.

Endpoints:
- GET    /api/v1/workspaces/{workspace_id}/sandbox/stats
- POST   /api/v1/workspaces/{workspace_id}/sandbox/packages
"""

from __future__ import annotations

import asyncio
import json
import logging
import posixpath
import re
import shlex
import time
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import httpx

from fastapi import APIRouter, HTTPException, Path as PathParam, Response
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, Field

from src.server.database.share_codes import share_url
from src.server.database.share_links import ensure_app_link
from src.server.utils.api import CurrentUserId
from src.server.database.workspace import (
    get_preview_command,
    get_workspace_dir_name as db_get_workspace_dir_name,
)
from src.server.database.workspace_folders import is_top_level, workspace_folder_in_use
from src.server.app.workspace_files._shared import work_dir_for
from src.server.services.workspace_files import (
    held_workspace_row,
    owned_workspace_on_computer,
)
from src.server.services.workspace_layout import WorkspaceLayoutUnavailable
from src.server.utils.error_sanitization import sandbox_failure_http
from src.server.services.workspace_manager import WorkspaceManager
from ptc_agent.core.paths import DEFAULT_SANDBOX_ROOT, WorkspaceLayout
from ptc_agent.core.sandbox import PTCSandbox
from ptc_agent.core.sandbox.preview_probe import head_preview
from ptc_agent.core.sandbox.runtime import (
    RuntimeState,
    SandboxTransientError,
)
from src.server.services.preview_capabilities import (
    PreviewProxyNotConfigured,
    PreviewProxyUnavailable,
)
from src.server.services.preview_links import (
    SIGNED_URL_LIFETIME,
    UNSET,
    delete_cached_signed_url,
    delete_preview_owner,
    get_preview_owner,
    preview_launch_lease,
    resolve_preview,
    set_preview_owner,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/workspaces", tags=["Workspace Sandbox"])

# Provider-native synonyms for "up and usable", canonicalized for the wire.
# get_metadata() is provider-specific by contract, so daytona reports "started"
# where docker reports "running". Daytona's own _STATE_MAP maps the same synonym
# one layer down for RuntimeState — renaming a state there needs a change here too.
_DISPLAY_STATE_SYNONYMS = {"started": "running"}


def _configured_provider() -> str | None:
    """Which provider this deployment runs, read from config rather than metadata.

    Deliberately not ``meta["provider"]``: only the docker provider sets that key,
    and the metadata read can fail — either would silently report "not docker" and
    defeat the disk-quota branch this value exists to drive.

    Reads ``config.sandbox`` directly rather than via ``to_core_config()``, which
    deep-copies every section to hand each workspace its own. That is wasted work
    for a single field that reads the same in the copy and in the original.
    """
    try:
        config = WorkspaceManager.get_instance().config
        provider = getattr(getattr(config, "sandbox", None), "provider", None)
        return provider if isinstance(provider, str) else None
    except Exception as e:
        logger.debug(f"Could not resolve the configured sandbox provider: {e}")
        return None


async def _provider_kind(workspace_id: str) -> str | None:
    """Which provider this workspace's machine runs on.

    The computer row carries the kind it was created with, so one deployment can
    serve more than one backend and the reported provider has to come from the
    machine. A workspace with no computer row reports the deployment's.
    """
    try:
        manager = WorkspaceManager.get_instance()
        kind = await manager.provider_kind_for_workspace(workspace_id)
        if kind:
            return kind
    except Exception as e:
        logger.debug(f"Could not resolve the machine's sandbox provider: {e}")
    return _configured_provider()


def _display_state(state: Any) -> str | None:
    """Canonicalize a provider state for the wire; see _DISPLAY_STATE_SYNONYMS.

    Unwraps enums defensively: ``get_metadata`` is provider-specific by contract,
    and ``str()`` on a str-mixin enum yields ``"RuntimeState.RUNNING"`` rather
    than its value. Both current providers already stringify.
    """
    if not state:
        return None
    raw = str(getattr(state, "value", state))
    return _DISPLAY_STATE_SYNONYMS.get(raw, raw)


def _offline_display_state(state: Any, row_status: str | None) -> str | None:
    """Like _display_state, but never reports "running" — the offline path can't back it.

    That path collects no disk usage, packages or skills, while the client reads
    "running" as proof they are present and as licence to offer Stop. When the
    provider says the sandbox is up but the row still says starting/stopping, the
    row is the honest answer: it is what the action endpoints validate against,
    and the full path takes over the moment it says running.
    """
    display = _display_state(state)
    if display is None or display == "running":
        return row_status
    return display


# Regex for validating package names (allows version specifiers)
_PACKAGE_NAME_RE = re.compile(r"^[a-zA-Z0-9._-]+([<>=!~]+.*)?$")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _get_sandbox(workspace_id: str, user_id: str) -> Any:
    """Validate the workspace and return its sandbox, starting the computer if stopped."""
    await owned_workspace_on_computer(workspace_id, user_id)

    manager = WorkspaceManager.get_instance()
    try:
        session = await manager.get_session_for_workspace(workspace_id, user_id=user_id)
    except Exception as e:
        # The raw text quotes provider URLs and sandbox ids; it stays in the log.
        logger.warning(f"Sandbox for workspace {workspace_id} not ready: {e}")
        raise sandbox_failure_http(e) from None

    sandbox = getattr(session, "sandbox", None)
    if sandbox is None:
        raise HTTPException(status_code=503, detail="Sandbox not available")

    return session, sandbox


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class SandboxResources(BaseModel):
    cpu: float | None = None
    memory: float | None = None  # GiB
    disk: float | None = None  # GiB
    gpu: float | None = None


class DiskOverview(BaseModel):
    total: str  # e.g. "20G"
    used: str  # e.g. "3.2G"
    available: str  # e.g. "16.8G"
    use_percent: str  # e.g. "16%"


class DirectorySize(BaseModel):
    path: str  # e.g. "work/"
    size: str  # e.g. "1.2G"


class InstalledPackage(BaseModel):
    name: str
    version: str


class SkillInfo(BaseModel):
    name: str
    description: str | None = None


class SandboxStatsResponse(BaseModel):
    """Sandbox stats for the workspace settings panel.

    ``state`` is a display vocabulary, not ``RuntimeState``: "running" is canonical
    across providers, anything else is a provider or workspace value passed through
    as a label.
    """

    workspace_id: str
    sandbox_id: str | None = None
    state: str | None = None
    # Which provider answered. The UI needs it because disk_usage is a df(1) read
    # inside the sandbox, and docker sets no disk quota, so its totals are the host's.
    provider: str | None = None
    created_at: str | None = None
    auto_stop_interval: int | None = None
    resources: SandboxResources
    disk_usage: DiskOverview | None = None
    directory_breakdown: list[DirectorySize] = Field(default_factory=list)
    packages: list[InstalledPackage] = Field(default_factory=list)
    mcp_servers: list[str] = Field(default_factory=list)
    skills: list[SkillInfo] = Field(default_factory=list)
    default_packages: list[str] = Field(default_factory=list)


class PackageInstallRequest(BaseModel):
    packages: list[str] = Field(..., min_length=1, max_length=50)


class PackageInstallResponse(BaseModel):
    success: bool
    installed: list[str]
    output: str
    error: str | None = None


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------


def _parse_df_output(stdout: str) -> DiskOverview | None:
    """Parse `df -h /home/workspace` output into a DiskOverview."""
    lines = stdout.strip().splitlines()
    if len(lines) < 2:
        return None
    # Header: Filesystem  Size  Used  Avail  Use%  Mounted on
    parts = lines[1].split()
    if len(parts) < 5:
        return None
    return DiskOverview(
        total=parts[1],
        used=parts[2],
        available=parts[3],
        use_percent=parts[4],
    )


def _parse_du_output(
    stdout: str, work_dir: str = DEFAULT_SANDBOX_ROOT
) -> list[DirectorySize]:
    """Parse `du -sh <work_dir>/*/` output into directory sizes."""
    work_dir_prefix = work_dir.rstrip("/") + "/"
    results: list[DirectorySize] = []
    for line in stdout.strip().splitlines():
        parts = line.split(None, 1)
        if len(parts) != 2:
            continue
        size, path = parts
        # Convert absolute path to relative display name
        stripped = path.rstrip("/")
        if stripped.startswith(work_dir_prefix):
            name = stripped[len(work_dir_prefix):]
        else:
            name = stripped.split(work_dir_prefix)[-1]
        if name:
            results.append(DirectorySize(path=name, size=size))
    return results


def _parse_pip_list(stdout: str) -> list[InstalledPackage]:
    """Parse `uv pip list --format json` output."""
    try:
        data = json.loads(stdout)
        return [InstalledPackage(name=p["name"], version=p["version"]) for p in data]
    except (json.JSONDecodeError, KeyError):
        return []


def _parse_skills_frontmatter(stdout: str) -> list[SkillInfo]:
    """Parse concatenated SKILL.md frontmatter blocks.

    Expected input format (one block per skill):
        === skill_dir_name ===
        ---
        name: foo
        description: bar
        ---
    """
    skills: list[SkillInfo] = []
    current_name: str | None = None
    current_desc: str | None = None

    for line in stdout.splitlines():
        line = line.strip()
        if line.startswith("=== ") and line.endswith(" ==="):
            # Flush previous skill
            if current_name:
                skills.append(SkillInfo(name=current_name, description=current_desc))
            current_name = line[4:-4].strip()
            current_desc = None
        elif line.startswith("name:"):
            current_name = line[5:].strip()
        elif line.startswith("description:"):
            current_desc = line[12:].strip()

    # Flush last
    if current_name:
        skills.append(SkillInfo(name=current_name, description=current_desc))

    return skills


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("/{workspace_id}/sandbox/stats")
async def get_sandbox_stats(
    workspace_id: str,
    x_user_id: CurrentUserId,
) -> SandboxStatsResponse:
    """Get sandbox resource stats, disk usage, installed packages, and MCP servers.

    For running workspaces: returns full stats including disk, packages, MCP, skills.
    For stopped/archived workspaces: returns metadata only (state, resources, intervals)
    from Daytona API without starting the sandbox.
    """
    workspace = await owned_workspace_on_computer(workspace_id, x_user_id)

    if workspace.get("status") == "running":
        return await _get_full_sandbox_stats(workspace_id, x_user_id, workspace)
    else:
        return await _get_offline_sandbox_stats(workspace_id, workspace)


async def _get_offline_sandbox_stats(
    workspace_id: str,
    workspace: dict[str, Any],
) -> SandboxStatsResponse:
    """Get sandbox metadata for stopped/archived workspaces via Daytona API (no start)."""
    sandbox_id = workspace.get("sandbox_id")
    provider_kind = await _provider_kind(workspace_id)
    if not sandbox_id:
        return SandboxStatsResponse(
            workspace_id=workspace_id,
            state=workspace.get("status", "unknown"),
            provider=provider_kind,
            created_at=str(workspace.get("created_at", "")),
            resources=SandboxResources(),
        )

    manager = WorkspaceManager.get_instance()
    provider = None
    try:
        provider = await manager.provider_for_workspace(workspace_id)
        runtime = await provider.get(sandbox_id)
        meta = await runtime.get_metadata()
        return SandboxStatsResponse(
            workspace_id=workspace_id,
            sandbox_id=sandbox_id,
            state=_offline_display_state(meta.get("state"), workspace.get("status")),
            provider=provider_kind,
            created_at=str(meta["created_at"]) if meta.get("created_at") else None,
            auto_stop_interval=meta.get("auto_stop_interval"),
            resources=SandboxResources(
                cpu=meta.get("cpu"),
                memory=meta.get("memory"),
                disk=meta.get("disk"),
                gpu=meta.get("gpu"),
            ),
        )
    except Exception as e:
        logger.warning(f"Failed to query sandbox provider for {sandbox_id}: {e}")
        return SandboxStatsResponse(
            workspace_id=workspace_id,
            sandbox_id=sandbox_id,
            state=workspace.get("status", "unknown"),
            provider=provider_kind,
            created_at=str(workspace.get("created_at", "")),
            resources=SandboxResources(),
        )
    finally:
        if provider is not None:
            await provider.close()


async def _get_full_sandbox_stats(
    workspace_id: str,
    x_user_id: str,
    workspace: dict[str, Any],
) -> SandboxStatsResponse:
    """Get full sandbox stats for running workspaces (disk, packages, MCP, skills)."""
    session, sandbox = await _get_sandbox(workspace_id, x_user_id)
    view = WorkspaceManager.get_instance().tool_view(session, workspace_id)
    mcp_servers = list(view.mcp_registry.connectors) if view.mcp_registry else []
    provider_kind = await _provider_kind(workspace_id)

    # --- 1. Static properties from the runtime metadata ---
    resources = SandboxResources()
    # Seeded from the workspace status this path already gated on, so a missing
    # or unreadable provider state can't downgrade a live sandbox to "offline".
    state = workspace.get("status")
    created_at = None
    auto_stop_interval = None
    sandbox_id = getattr(sandbox, "sandbox_id", None)

    runtime = getattr(sandbox, "runtime", None)
    if runtime is not None:
        try:
            meta = await runtime.get_metadata()
            resources = SandboxResources(
                cpu=meta.get("cpu"),
                memory=meta.get("memory"),
                disk=meta.get("disk"),
                gpu=meta.get("gpu"),
            )
            state = _display_state(meta.get("state")) or state
            raw_created = meta.get("created_at")
            if raw_created is not None:
                created_at = str(raw_created)
            auto_stop_interval = meta.get("auto_stop_interval")
        except Exception as e:
            logger.warning(f"Failed to read sandbox metadata for {sandbox_id}: {e}")

    # --- 2. Concurrent bash commands for disk & packages ---
    work_dir = sandbox.working_dir

    async def _get_disk_usage():
        try:
            result = await sandbox.execute_bash_command(
                f"df -h {work_dir}", timeout=10
            )
            if result.get("success"):
                return _parse_df_output(result.get("stdout", ""))
        except Exception as e:
            logger.debug(f"df command failed: {e}")
        return None

    async def _get_directory_breakdown():
        try:
            result = await sandbox.execute_bash_command(
                f"du -sh {work_dir}/*/ 2>/dev/null || true", timeout=15
            )
            if result.get("success"):
                return _parse_du_output(result.get("stdout", ""), work_dir)
        except Exception as e:
            logger.debug(f"du command failed: {e}")
        return []

    async def _get_packages():
        try:
            result = await sandbox.execute_bash_command(
                "uv pip list --format json 2>/dev/null || pip list --format json 2>/dev/null || echo '[]'",
                timeout=15,
            )
            if result.get("success"):
                return _parse_pip_list(result.get("stdout", ""))
        except Exception as e:
            logger.debug(f"pip list failed: {e}")
        return []

    async def _get_skills():
        try:
            # A settle on any worker moves only a folder it can hold, so the
            # folder read under the hold is the one listed; mid-move, none is.
            async with workspace_folder_in_use(workspace_id):
                dir_name = await db_get_workspace_dir_name(workspace_id)
                if dir_name and not is_top_level(dir_name):
                    return []
                # Read SKILL.md frontmatter from each skill directory
                cmd = (
                    f"for d in {shlex.quote(WorkspaceLayout(work_dir, dir_name).skills)}/*/; do "
                    '  [ -f "$d/SKILL.md" ] && echo "=== $(basename "$d") ===" && head -5 "$d/SKILL.md"; '
                    "done 2>/dev/null || true"
                )
                result = await sandbox.execute_bash_command(cmd, timeout=10)
            if result.get("success"):
                return _parse_skills_frontmatter(result.get("stdout", ""))
        except Exception as e:
            logger.debug(f"skills listing failed: {e}")
        return []

    disk_usage, directory_breakdown, packages, skills = await asyncio.gather(
        _get_disk_usage(),
        _get_directory_breakdown(),
        _get_packages(),
        _get_skills(),
    )

    default_packages = list(PTCSandbox.DEFAULT_DEPENDENCIES)

    return SandboxStatsResponse(
        workspace_id=workspace_id,
        sandbox_id=sandbox_id,
        state=state,
        provider=provider_kind,
        created_at=created_at,
        auto_stop_interval=auto_stop_interval,
        resources=resources,
        disk_usage=disk_usage,
        directory_breakdown=directory_breakdown,
        packages=packages,
        mcp_servers=mcp_servers,
        skills=skills,
        default_packages=default_packages,
    )


@router.post("/{workspace_id}/sandbox/packages")
async def install_sandbox_packages(
    workspace_id: str,
    x_user_id: CurrentUserId,
    body: PackageInstallRequest,
) -> PackageInstallResponse:
    """Install pip packages in the workspace sandbox."""

    # Validate package names before touching the sandbox
    for pkg in body.packages:
        if not _PACKAGE_NAME_RE.match(pkg):
            raise HTTPException(
                status_code=400,
                detail=f"Invalid package name: {pkg}",
            )

    _session, sandbox = await _get_sandbox(workspace_id, x_user_id)

    quoted = " ".join(shlex.quote(p) for p in body.packages)
    cmd = f"uv pip install {quoted}"

    try:
        result = await sandbox.execute_bash_command(cmd, timeout=120)
        success = result.get("success", False)

        return PackageInstallResponse(
            success=success,
            installed=body.packages if success else [],
            output=result.get("stdout", ""),
            error=result.get("stderr", "") if not success else None,
        )
    except Exception as e:
        logger.exception("Package install failed for workspace %s", workspace_id)
        return PackageInstallResponse(
            success=False,
            installed=[],
            output="",
            error=str(e),
        )


class PreviewUrlRequest(BaseModel):
    port: int = Field(..., ge=3000, le=9999)
    command: str | None = None
    force: bool = False
    expires_in: int = Field(default=SIGNED_URL_LIFETIME, ge=60, le=86400)


class PreviewUrlResponse(BaseModel):
    url: str
    port: int
    expires_in: int


def _preview_work_dir(workspace: dict[str, Any]) -> str:
    try:
        return work_dir_for(workspace)
    except WorkspaceLayoutUnavailable as e:
        raise HTTPException(
            status_code=503, detail="Workspace files are not available"
        ) from e


async def owner_preview_url(
    workspace_id: str,
    user_id: str,
    port: int,
    *,
    command: str | None | object = UNSET,
    force: bool = False,
    expires_in: int = SIGNED_URL_LIFETIME,
) -> str:
    """A signed URL for the owner's own preview, as an HTTP answer.

    The one acquisition path for an authenticated caller: the preview panel
    and the ``/a/`` page both come through here, so a stopped sandbox is
    started for the owner and for nobody else. Ownership, the unbound-Home
    refusal and every failure status live here rather than in each route.
    """
    _session, sandbox = await _get_sandbox(workspace_id, user_id)
    try:
        # The server starts in the folder, which a settle on any worker would
        # otherwise move between the read and the launch.
        async with held_workspace_row(workspace_id) as workspace:
            return await resolve_preview(
                sandbox, workspace_id, port,
                command=command, force=force, expires_in=expires_in,
                work_dir=_preview_work_dir(workspace),
            )
    except HTTPException:
        raise
    except NotImplementedError:
        raise HTTPException(
            status_code=501,
            detail="Preview URLs are not supported by the current sandbox provider",
        ) from None
    except PreviewProxyNotConfigured:
        raise HTTPException(
            status_code=501,
            detail=(
                "Previews on this computer need the preview proxy, which this "
                "deployment has not configured."
            ),
        ) from None
    except PreviewProxyUnavailable:
        logger.warning(
            "Preview proxy unavailable for workspace %s port %d", workspace_id, port
        )
        raise HTTPException(
            status_code=503, detail="Previews are unavailable. Try again shortly."
        ) from None
    except Exception:
        logger.exception(
            "Failed to get preview URL for workspace %s port %d", workspace_id, port
        )
        raise HTTPException(status_code=500, detail="Failed to get preview URL") from None


def with_preview_path(signed_url: str, path: str | None) -> str:
    """The signed URL opened at a page inside the preview rather than its index.

    A page's own query is appended after the signed one, never spliced ahead
    of it: the provider's parameters are what admit the request. The web's
    ``appendPathSuffix`` composes the owner's panel the same way.
    """
    if not path:
        return signed_url
    rest, _, fragment = path.partition("#")
    pathname, _, query = rest.partition("?")
    normalized = posixpath.normpath("/" + pathname.lstrip("/"))
    if ".." in normalized.split("/"):
        return signed_url
    parts = urlsplit(signed_url)
    if normalized != "/":
        parts = parts._replace(path=parts.path.rstrip("/") + normalized)
    if query:
        page_query = urlencode(parse_qsl(query, keep_blank_values=True))
        parts = parts._replace(query="&".join(filter(None, (parts.query, page_query))))
    if fragment:
        parts = parts._replace(fragment=fragment)
    return urlunsplit(parts)


@router.post("/{workspace_id}/sandbox/preview-url")
async def get_sandbox_preview_url(
    workspace_id: str,
    x_user_id: CurrentUserId,
    body: PreviewUrlRequest,
) -> PreviewUrlResponse:
    """Get a signed preview URL for a service running in the workspace sandbox.

    If command is provided, starts the server process in background before generating the URL.
    """
    url = await owner_preview_url(
        workspace_id, x_user_id, body.port,
        command=body.command if body.command else UNSET,
        force=body.force, expires_in=body.expires_in,
    )
    return PreviewUrlResponse(url=url, port=body.port, expires_in=body.expires_in)


class PreviewHealthRequest(BaseModel):
    port: int = Field(..., ge=3000, le=9999)


class PreviewHealthResponse(BaseModel):
    reachable: bool
    checked_at: int


@router.post("/{workspace_id}/sandbox/preview-health")
async def check_preview_health(
    workspace_id: str,
    x_user_id: CurrentUserId,
    body: PreviewHealthRequest,
) -> PreviewHealthResponse:
    """Check if a preview service is still reachable on the given port.

    Never starts the computer: an open card polls every 2 minutes, so a check
    that started it would undo every stop. A stopped computer answers 503,
    which the card shows as stopped. Uses the sandbox's standard preview link
    (cached) to avoid repeated provider API calls on the polling interval.
    """
    await owned_workspace_on_computer(workspace_id, x_user_id)
    stopped = HTTPException(status_code=503, detail="Computer is stopped")
    manager = WorkspaceManager.get_instance()
    try:
        session = await manager.get_session_if_running(workspace_id, user_id=x_user_id)
    except Exception as e:
        logger.warning(f"Preview health could not reach workspace {workspace_id}: {e}")
        raise sandbox_failure_http(e) from None
    sandbox = getattr(session, "sandbox", None)
    if sandbox is None or sandbox.runtime is None:
        raise stopped

    checked_at = int(time.time())
    reachable = False
    try:
        preview_link = await sandbox.get_preview_link(body.port, wake=False)
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await head_preview(
                client, preview_link.url, preview_link.auth_headers
            )
            reachable = 200 <= resp.status_code < 400
    except NotImplementedError:
        raise HTTPException(
            status_code=501, detail="Preview health checks not supported"
        ) from None
    except Exception:
        pass

    # Invalidate cached signed URL when server is down so next resolve gets a fresh one
    if not reachable:
        await delete_cached_signed_url(sandbox.sandbox_id, body.port)

    # On the runtime itself: the sandbox's own call path reconnects, which
    # would start a computer that stopped on its idle timer. A live preview
    # is activity on both clocks, the same path proxied traffic takes.
    runtime = sandbox.runtime
    try:
        if reachable:
            if not await manager.keep_sandbox_alive(
                workspace_id, sandbox.sandbox_id, runtime=runtime
            ):
                raise stopped
        elif await runtime.refresh_state() is not RuntimeState.RUNNING:
            raise stopped
    except HTTPException:
        raise
    except SandboxTransientError:
        raise stopped from None
    except Exception:
        # A state read that fails leaves the reachability answer as it is.
        logger.debug("Preview health state read failed", exc_info=True)

    return PreviewHealthResponse(reachable=reachable, checked_at=checked_at)



class PreviewRestartRequest(BaseModel):
    port: int = Field(..., ge=3000, le=9999)
    command: str


class PreviewRestartResponse(BaseModel):
    success: bool


@router.post("/{workspace_id}/sandbox/preview-restart")
async def restart_preview_server(
    workspace_id: str,
    x_user_id: CurrentUserId,
    body: PreviewRestartRequest,
) -> PreviewRestartResponse:
    """Restart a preview server process in the workspace sandbox."""
    _session, sandbox = await _get_sandbox(workspace_id, x_user_id)
    # The server starts in the folder, which a settle on any worker would
    # otherwise move between the read and the launch.
    async with held_workspace_row(workspace_id) as workspace:
        work_dir = _preview_work_dir(workspace)
        try:
            async with preview_launch_lease(sandbox.sandbox_id, body.port):
                owner = await get_preview_owner(sandbox.sandbox_id, body.port)
                if owner not in (None, workspace_id):
                    if await sandbox._is_preview_reachable(body.port):
                        raise RuntimeError(
                            f"Port {body.port} is already in use on this computer. "
                            "Choose another port."
                        )
                    await delete_preview_owner(sandbox.sandbox_id, body.port)
                await set_preview_owner(sandbox.sandbox_id, body.port, workspace_id)
                try:
                    await sandbox.start_preview_server(
                        f"cd {shlex.quote(work_dir)} && {body.command}",
                        body.port,
                        owner=workspace_id,
                    )
                except BaseException:
                    await delete_preview_owner(sandbox.sandbox_id, body.port)
                    raise
            return PreviewRestartResponse(success=True)
        except Exception:
            logger.exception(
                "Failed to restart preview server for workspace %s", workspace_id,
            )
            raise HTTPException(status_code=500, detail="Failed to restart preview server") from None


# ---------------------------------------------------------------------------
# Legacy preview redirect
# ---------------------------------------------------------------------------

preview_redirect_router = APIRouter(prefix="/api/v1", tags=["Preview Redirect"])


@preview_redirect_router.get("/preview/{workspace_id}/{port}")
@preview_redirect_router.get("/preview/{workspace_id}/{port}/{path:path}")
async def preview_redirect(
    workspace_id: str,
    port: int = PathParam(ge=3000, le=9999),
    path: str = "",
) -> Response:
    """Send an old preview URL, path suffix or not, on to the app's ``/a/`` link.

    Nothing here touches a session or the sandbox. The old route resolved the
    signed URL itself, which made a bare workspace UUID a credential and,
    on a stale ``running`` row, woke a sandbox that had auto-stopped (#378).
    The ``/a/`` page does the resolving, for the signed-in owner only. An old
    URL's suffix rides along as ``?path=`` rather than onto the link: two old
    URLs for one port can name different pages, and anyone holding the UUID
    can make this request. No server on that port answers the same 404 as an
    unknown workspace, so the route confirms neither.
    """
    if not await get_preview_command(workspace_id, port):
        raise HTTPException(status_code=404, detail="Preview not available")
    link = await ensure_app_link(workspace_id, port)
    entry = posixpath.normpath("/" + path).lstrip("/") if path else None
    response = RedirectResponse(url=share_url(link.code, entry), status_code=302)
    response.headers["Cache-Control"] = "no-store"
    return response
