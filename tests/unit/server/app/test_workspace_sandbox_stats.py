"""Tests for the sandbox stats state vocabulary (issue #333).

Daytona reports "started" where docker reports "running"; the endpoint
canonicalizes that one synonym and passes everything else through.
"""

import uuid
from contextlib import asynccontextmanager, nullcontext
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from ptc_agent.core.paths import WorkspaceLayout
from tests.conftest import create_test_app

NOW = datetime.now(timezone.utc)


@pytest.fixture(autouse=True)
def _folder_hold_is_free():
    """The skills listing holds the workspace's folder against a settle, an
    advisory lock these database-free tests cannot take."""
    with (
        patch(
            "src.server.app.workspace_sandbox.workspace_folder_in_use",
            lambda _workspace_id: nullcontext(),
        ),
        patch(
            "src.server.app.workspace_sandbox.db_get_workspace_dir_name",
            AsyncMock(return_value="Research"),
        ),
    ):
        yield


@pytest.mark.asyncio
@pytest.mark.parametrize("folder", ["Macro", "_internal/moving/ws"])
async def test_skills_are_listed_from_the_folder_read_under_the_hold(folder):
    """A settle on another worker can move the folder after the acquisition.
    Listed at the old path, the skills read as none, or as those of a sibling
    that took it; mid-move there is no folder to list."""
    from src.server.app.workspace_sandbox import _get_full_sandbox_stats

    workspace = _ws()
    session, sandbox = _sandbox_with_metadata({"state": "running"})
    manager = MagicMock()
    manager.tool_view.return_value = SimpleNamespace(mcp_registry=None)
    events = []

    @asynccontextmanager
    async def hold(_workspace_id):
        events.append("hold")
        try:
            yield
        finally:
            events.append("release")

    async def read(_workspace_id):
        events.append("read")
        return folder

    async def bash(cmd, **_kw):
        if "SKILL.md" in cmd:
            events.append(f"list {cmd.split()[3]}")
        return {"success": False}

    sandbox.execute_bash_command.side_effect = bash
    with (
        patch("src.server.app.workspace_sandbox._get_sandbox", AsyncMock(return_value=(session, sandbox))),
        patch("src.server.app.workspace_sandbox._provider_kind", AsyncMock(return_value="docker")),
        patch("src.server.app.workspace_sandbox.WorkspaceManager.get_instance", return_value=manager),
        patch("src.server.app.workspace_sandbox.workspace_folder_in_use", hold),
        patch("src.server.app.workspace_sandbox.db_get_workspace_dir_name", read),
    ):
        stats = await _get_full_sandbox_stats(workspace["workspace_id"], "test-user-123", workspace)

    listed = [f"list {WorkspaceLayout('/home/workspace', folder).skills}/*/;"]
    assert events == ["hold", "read", *(listed if folder == "Macro" else []), "release"]
    assert stats.skills == []


@pytest.mark.asyncio
async def test_mcp_stats_keep_the_requested_view_when_a_sibling_syncs():
    from src.server.app.workspace_sandbox import _get_full_sandbox_stats

    workspace = _ws()
    session, sandbox = _sandbox_with_metadata({"state": "running"})
    own_registry = SimpleNamespace(connectors={"own-server": object()})
    session.mcp_registry = own_registry
    manager = MagicMock()
    manager.tool_view.return_value = SimpleNamespace(mcp_registry=own_registry)

    async def sibling_sync(*args, **kwargs):
        session.mcp_registry = SimpleNamespace(connectors={"sibling-server": object()})
        return {"success": False}

    sandbox.execute_bash_command.side_effect = sibling_sync
    with (
        patch("src.server.app.workspace_sandbox._get_sandbox", AsyncMock(return_value=(session, sandbox))),
        patch("src.server.app.workspace_sandbox._provider_kind", AsyncMock(return_value="docker")),
        patch("src.server.app.workspace_sandbox.WorkspaceManager.get_instance", return_value=manager),
    ):
        stats = await _get_full_sandbox_stats(workspace["workspace_id"], "test-user-123", workspace)

    assert stats.mcp_servers == ["own-server"]
    manager.tool_view.assert_called_once_with(session, workspace["workspace_id"])


@pytest.mark.asyncio
async def test_refresh_response_keeps_the_requested_workspace_tool_view():
    from src.server.app import workspaces as workspaces_module

    workspace = _ws()
    session, _ = _sandbox_with_metadata({"state": "running"})
    own_registry = SimpleNamespace(connectors={"own-server": object()})
    manager = MagicMock()
    manager.get_session_for_workspace = AsyncMock(return_value=session)

    async def refresh(*args, **kwargs):
        session.mcp_registry = SimpleNamespace(
            connectors={"sibling-server": object()}
        )
        return SimpleNamespace(refreshed_modules=["mcp_servers"])

    manager.refresh_project_assets = AsyncMock(side_effect=refresh)
    manager.tool_view.return_value = SimpleNamespace(mcp_registry=own_registry)
    with (
        patch.object(
            workspaces_module.WorkspaceManager,
            "get_instance",
            return_value=manager,
        ),
        patch.object(
            workspaces_module,
            "db_get_workspace",
            AsyncMock(return_value=workspace),
        ),
        patch.object(workspaces_module, "require_workspace_owner"),
    ):
        response = await workspaces_module.refresh_workspace(
            workspace["workspace_id"], workspace["user_id"]
        )

    assert response.servers == ["own-server"]
    manager.tool_view.assert_called_once_with(session, workspace["workspace_id"])


def _ws(status="running", sandbox_id="sandbox-abc"):
    return {
        "workspace_id": str(uuid.uuid4()),
        "user_id": "test-user-123",
        "name": "Test Workspace",
        "sandbox_id": sandbox_id,
        "status": status,
        "created_at": NOW,
        "updated_at": NOW,
    }


@pytest_asyncio.fixture
async def client():
    from src.server.app.workspace_sandbox import router

    app = create_test_app(router)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c


def _sandbox_with_metadata(meta, *, side_effect=None):
    """A PTCSandbox stand-in whose runtime returns (or raises on) get_metadata."""
    runtime = MagicMock()
    runtime.get_metadata = AsyncMock(return_value=meta, side_effect=side_effect)

    sandbox = MagicMock()
    sandbox.runtime = runtime
    sandbox.sandbox_id = "sandbox-abc"
    sandbox.working_dir = "/home/workspace"
    # Every shell probe is best-effort; report failure so the test isolates state.
    sandbox.execute_bash_command = AsyncMock(return_value={"success": False})

    session = MagicMock()
    session.mcp_registry = None
    return session, sandbox


def _config_with_provider(name, provider=None):
    """A WorkspaceManager stand-in whose config resolves a real provider string.

    A bare MagicMock would hand ``_configured_provider`` a mock attribute, which
    then fails response validation instead of behaving like a config; only
    ``sandbox`` needs to be real. ``provider_kind_for_workspace`` answers None
    because these workspaces have no computer row, which is what sends the
    reported kind back to the deployment config.
    """
    from src.server.services.workspace_manager import WorkspaceManager

    config = MagicMock()
    config.sandbox = SimpleNamespace(provider=name)
    manager = MagicMock(spec=WorkspaceManager)
    manager.config = config
    manager.provider_kind_for_workspace = AsyncMock(return_value=None)
    manager.provider_for_workspace = AsyncMock(return_value=provider)
    return MagicMock(get_instance=MagicMock(return_value=manager))


async def _get_stats(
    client, workspace, meta, *, side_effect=None, provider_name="daytona"
):
    session, sandbox = _sandbox_with_metadata(meta, side_effect=side_effect)
    with (
        patch(
            "src.server.services.workspace_files.db_get_workspace",
            AsyncMock(return_value=workspace),
        ),
        patch(
            "src.server.app.workspace_sandbox._get_sandbox",
            AsyncMock(return_value=(session, sandbox)),
        ),
        patch(
            "src.server.app.workspace_sandbox.WorkspaceManager",
            _config_with_provider(provider_name),
        ),
    ):
        return await client.get(
            f"/api/v1/workspaces/{workspace['workspace_id']}/sandbox/stats"
        )


# ---------------------------------------------------------------------------
# Cross-provider canonicalization
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_daytona_started_serializes_as_running(client):
    """Daytona's native 'started' is the synonym that must be canonicalized."""
    resp = await _get_stats(client, _ws(), {"state": "started"})

    assert resp.status_code == 200
    assert resp.json()["state"] == "running"


@pytest.mark.asyncio
async def test_docker_running_passes_through_as_running(client):
    """Non-discriminating by construction, and kept anyway as a smoke of the full
    path: that path is gated on ``status == "running"``, so its seed is always
    literally "running" and no fixture can make it differ from the expected value.
    Pass-through is really pinned by ``test_non_running_provider_states_pass_through_verbatim``
    and ``test_offline_path_keeps_stopped``, where the row and the provider disagree."""
    resp = await _get_stats(client, _ws(), {"state": "running"})

    assert resp.status_code == 200
    assert resp.json()["state"] == "running"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_state",
    ["archiving", "stopping", "starting", "restoring", "resizing"],
)
async def test_non_running_provider_states_pass_through_verbatim(
    client, provider_state
):
    """Only the one synonym is rewritten. ``archiving``/``stopping``/``starting``
    are the values the panel keys its spinner off; the rest reach it as labels."""
    resp = await _get_stats(client, _ws(), {"state": provider_state})

    assert resp.status_code == 200
    assert resp.json()["state"] == provider_state


@pytest.mark.asyncio
async def test_provider_reaches_the_wire(client):
    """The panel keys its disk display off this: docker sets no size quota, so its
    df(1) totals describe the host, not the sandbox."""
    resp = await _get_stats(client, _ws(), {"state": "running"}, provider_name="docker")

    assert resp.status_code == 200
    assert resp.json()["provider"] == "docker"


@pytest.mark.asyncio
async def test_provider_ignores_metadata_and_survives_its_failure(client):
    """Sourced from config, not ``meta["provider"]``: daytona never sets that key, and
    the read can raise — either would report "not docker" and hand a self-hosted user
    the host's disk totals labelled as their sandbox's."""
    resp = await _get_stats(
        client,
        _ws(),
        None,
        side_effect=RuntimeError("daemon unreachable"),
        provider_name="docker",
    )

    assert resp.status_code == 200
    assert resp.json()["provider"] == "docker"


@pytest.mark.asyncio
async def test_offline_path_reports_provider(client):
    resp = await _get_offline_stats(client, _ws(status="stopped"), {"state": "stopped"})

    assert resp.status_code == 200
    assert resp.json()["provider"] == "daytona"


@pytest.mark.asyncio
async def test_offline_provider_failure_still_reports_provider(client):
    """Every response carries provider, including the ones that learned nothing from
    the provider — so no consumer has to special-case a subset of the branches."""
    resp = await _get_offline_stats(
        client,
        _ws(status="stopping"),
        None,
        side_effect=RuntimeError("provider unreachable"),
        provider_name="docker",
    )

    assert resp.status_code == 200
    assert resp.json()["provider"] == "docker"


@pytest.mark.asyncio
async def test_workspace_without_a_sandbox_still_reports_provider(client):
    workspace = _ws(status="creating", sandbox_id=None)

    with (
        patch(
            "src.server.services.workspace_files.db_get_workspace",
            AsyncMock(return_value=workspace),
        ),
        patch(
            "src.server.app.workspace_sandbox.WorkspaceManager",
            _config_with_provider("docker"),
        ),
    ):
        resp = await client.get(
            f"/api/v1/workspaces/{workspace['workspace_id']}/sandbox/stats"
        )

    assert resp.status_code == 200
    assert resp.json()["provider"] == "docker"


# ---------------------------------------------------------------------------
# Fallbacks — a live sandbox must never report as offline
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_metadata_failure_falls_back_to_workspace_status(client):
    resp = await _get_stats(
        client, _ws(), None, side_effect=RuntimeError("provider unreachable")
    )

    assert resp.status_code == 200
    assert resp.json()["state"] == "running"


@pytest.mark.asyncio
async def test_metadata_without_a_state_key_falls_back(client):
    """Daytona omits 'state' entirely when the SDK object hasn't populated it."""
    resp = await _get_stats(client, _ws(), {"cpu": 2})

    assert resp.status_code == 200
    assert resp.json()["state"] == "running"


# ---------------------------------------------------------------------------
# Offline path
# ---------------------------------------------------------------------------


async def _get_offline_stats(
    client, workspace, meta, *, side_effect=None, provider_name="daytona"
):
    """Drive the offline path: a real provider client, no sandbox session."""
    runtime = MagicMock()
    runtime.get_metadata = AsyncMock(return_value=meta, side_effect=side_effect)
    provider = MagicMock()
    provider.get = AsyncMock(return_value=runtime)
    provider.close = AsyncMock()

    with (
        patch(
            "src.server.services.workspace_files.db_get_workspace",
            AsyncMock(return_value=workspace),
        ),
        patch(
            "src.server.app.workspace_sandbox.WorkspaceManager",
            _config_with_provider(provider_name, provider),
        ),
    ):
        return await client.get(
            f"/api/v1/workspaces/{workspace['workspace_id']}/sandbox/stats"
        )


@pytest.mark.asyncio
async def test_offline_path_keeps_stopped(client):
    """The provider wins over the row: status is ``stopping`` but the sandbox has
    finished stopping, so a fixture that merely echoed the row would read
    ``stopping``. Nothing is started to find out."""
    resp = await _get_offline_stats(
        client, _ws(status="stopping"), {"state": "stopped", "cpu": 2}
    )

    assert resp.status_code == 200
    assert resp.json()["state"] == "stopped"


@pytest.mark.asyncio
@pytest.mark.parametrize("provider_state", ["running", "started"])
async def test_offline_path_never_reports_running(client, provider_state):
    """Both providers' "up" vocabularies, clamped to the row.

    This path collects no disk usage, packages or skills, but the client reads
    "running" as proof they are present and enables Stop on it — which the action
    endpoint then rejects for any row that isn't running. The row wins here; the
    full path takes over the moment it says running.
    """
    resp = await _get_offline_stats(
        client, _ws(status="starting"), {"state": provider_state, "cpu": 2}
    )

    assert resp.status_code == 200
    assert resp.json()["state"] == "starting"


@pytest.mark.asyncio
async def test_offline_path_clamp_is_not_a_blanket_row_echo(client):
    """The clamp is confined to "running" — every other provider state still wins
    over the row, which is the whole point of asking the provider at all."""
    resp = await _get_offline_stats(
        client, _ws(status="stopping"), {"state": "archiving", "cpu": 2}
    )

    assert resp.status_code == 200
    assert resp.json()["state"] == "archiving"


@pytest.mark.asyncio
async def test_offline_path_without_a_state_key_falls_back(client):
    """Daytona omits "state" entirely when the SDK object hasn't populated it, so
    the workspace row is all that's left. ``error`` is distinct from every other
    status in this file, so the fallback can't be satisfied by accident."""
    resp = await _get_offline_stats(client, _ws(status="error"), {"cpu": 2})

    assert resp.status_code == 200
    assert resp.json()["state"] == "error"


@pytest.mark.asyncio
async def test_offline_path_provider_failure_falls_back(client):
    """Docker's get_metadata does a live daemon call, so it really can raise."""
    resp = await _get_offline_stats(
        client,
        _ws(status="stopping"),
        None,
        side_effect=RuntimeError("provider unreachable"),
    )

    assert resp.status_code == 200
    assert resp.json()["state"] == "stopping"


@pytest.mark.asyncio
async def test_no_sandbox_id_reports_workspace_status(client):
    workspace = _ws(status="creating", sandbox_id=None)

    with patch(
        "src.server.services.workspace_files.db_get_workspace",
        AsyncMock(return_value=workspace),
    ):
        resp = await client.get(
            f"/api/v1/workspaces/{workspace['workspace_id']}/sandbox/stats"
        )

    assert resp.status_code == 200
    assert resp.json()["state"] == "creating"


# ---------------------------------------------------------------------------
# Ownership — the offline path has only the endpoint-level check to rely on
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["running", "stopped"])
async def test_another_users_workspace_is_forbidden(client, status):
    """Only ``stopped`` pins the endpoint-level check — the running path has a
    second ``require_workspace_owner`` inside ``_get_sandbox``. WorkspaceManager is
    patched so dropping that check fails as 200-vs-403, not as a config error."""
    workspace = {**_ws(status=status), "user_id": "someone-else"}

    with (
        patch(
            "src.server.services.workspace_files.db_get_workspace",
            AsyncMock(return_value=workspace),
        ),
        patch(
            "src.server.app.workspace_sandbox.WorkspaceManager",
            _config_with_provider("daytona"),
        ),
    ):
        resp = await client.get(
            f"/api/v1/workspaces/{workspace['workspace_id']}/sandbox/stats"
        )

    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_missing_workspace_is_not_found(client):
    with patch(
        "src.server.services.workspace_files.db_get_workspace",
        AsyncMock(return_value=None),
    ):
        resp = await client.get(f"/api/v1/workspaces/{uuid.uuid4()}/sandbox/stats")

    assert resp.status_code == 404
