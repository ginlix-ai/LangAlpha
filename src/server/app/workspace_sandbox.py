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
import re
import shlex
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from src.server.utils.api import CurrentUserId
from src.server.database.workspace import get_workspace as db_get_workspace
from src.server.services.workspace_manager import WorkspaceManager
from src.ptc_agent.core.sandbox import PTCSandbox

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/workspaces", tags=["Workspace Sandbox"])

# Regex for validating package names (allows version specifiers)
_PACKAGE_NAME_RE = re.compile(r"^[a-zA-Z0-9._-]+([<>=!~]+.*)?$")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _require_workspace_owner(
    workspace: dict[str, Any] | None, *, user_id: str, workspace_id: str
) -> None:
    if not workspace:
        raise HTTPException(status_code=404, detail="Workspace not found")
    if workspace.get("user_id") != user_id:
        raise HTTPException(status_code=403, detail="Forbidden")
    if workspace.get("status") == "deleted":
        raise HTTPException(status_code=404, detail="Workspace not found")


async def _get_sandbox(workspace_id: str, user_id: str) -> Any:
    """Validate workspace ownership, reject flash workspaces, and return the sandbox."""
    workspace = await db_get_workspace(workspace_id)
    _require_workspace_owner(workspace, user_id=user_id, workspace_id=workspace_id)

    if workspace.get("status") == "flash":
        raise HTTPException(status_code=400, detail="Flash workspaces do not have a sandbox")

    manager = WorkspaceManager.get_instance()
    session = await manager.get_session_for_workspace(workspace_id, user_id=user_id)

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
    path: str  # e.g. "results/"
    size: str  # e.g. "1.2G"


class InstalledPackage(BaseModel):
    name: str
    version: str


class SkillInfo(BaseModel):
    name: str
    description: str | None = None


class SandboxStatsResponse(BaseModel):
    workspace_id: str
    sandbox_id: str | None = None
    state: str | None = None
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
    """Parse `df -h /home/daytona` output into a DiskOverview."""
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


def _parse_du_output(stdout: str) -> list[DirectorySize]:
    """Parse `du -sh /home/daytona/*/` output into directory sizes."""
    results: list[DirectorySize] = []
    for line in stdout.strip().splitlines():
        parts = line.split(None, 1)
        if len(parts) != 2:
            continue
        size, path = parts
        # Convert absolute path to relative display name
        name = path.rstrip("/").split("/home/daytona/")[-1]
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
    """Get sandbox resource stats, disk usage, installed packages, and MCP servers."""
    session, sandbox = await _get_sandbox(workspace_id, x_user_id)

    # --- 1. Static properties from the Daytona sandbox object ---
    daytona_sandbox = getattr(sandbox, "sandbox", None)
    resources = SandboxResources()
    state = None
    created_at = None
    auto_stop_interval = None
    sandbox_id = getattr(sandbox, "sandbox_id", None)

    if daytona_sandbox is not None:
        try:
            resources = SandboxResources(
                cpu=getattr(daytona_sandbox, "cpu", None),
                memory=getattr(daytona_sandbox, "memory", None),
                disk=getattr(daytona_sandbox, "disk", None),
                gpu=getattr(daytona_sandbox, "gpu", None),
            )
        except Exception:
            pass

        raw_state = getattr(daytona_sandbox, "state", None)
        if raw_state is not None:
            state = raw_state.value if hasattr(raw_state, "value") else str(raw_state)

        raw_created = getattr(daytona_sandbox, "created_at", None)
        if raw_created is not None:
            created_at = str(raw_created)

        auto_stop_interval = getattr(daytona_sandbox, "auto_stop_interval", None)

    # --- 2. Concurrent bash commands for disk & packages ---
    async def _get_disk_usage():
        try:
            result = await sandbox.execute_bash_command("df -h /home/daytona", timeout=10)
            if result.get("success"):
                return _parse_df_output(result.get("stdout", ""))
        except Exception as e:
            logger.debug(f"df command failed: {e}")
        return None

    async def _get_directory_breakdown():
        try:
            result = await sandbox.execute_bash_command(
                "du -sh /home/daytona/*/ 2>/dev/null || true", timeout=15
            )
            if result.get("success"):
                return _parse_du_output(result.get("stdout", ""))
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
            # Read SKILL.md frontmatter from each skill directory
            cmd = (
                'for d in /home/daytona/skills/*/; do '
                '  [ -f "$d/SKILL.md" ] && echo "=== $(basename "$d") ===" && head -5 "$d/SKILL.md"; '
                'done 2>/dev/null || true'
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

    # --- 3. MCP servers ---
    mcp_servers: list[str] = []
    try:
        registry = getattr(session, "mcp_registry", None)
        if registry is not None:
            mcp_servers = list(registry.connectors.keys())
    except Exception:
        pass

    default_packages = list(PTCSandbox.DEFAULT_DEPENDENCIES)

    return SandboxStatsResponse(
        workspace_id=workspace_id,
        sandbox_id=sandbox_id,
        state=state,
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
