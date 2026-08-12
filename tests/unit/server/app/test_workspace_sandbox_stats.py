"""Tests for the sandbox stats state vocabulary (issue #333).

Daytona reports "started" where docker reports "running"; the endpoint
canonicalizes that one synonym and passes everything else through.
"""

import uuid
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from tests.conftest import create_test_app

NOW = datetime.now(timezone.utc)


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


def _config_with_provider(name):
    """A WorkspaceManager stand-in whose config resolves a real provider string.

    A bare MagicMock would hand ``_configured_provider`` a mock attribute, which
    then fails response validation instead of behaving like a config. ``config``
    stays a MagicMock so ``to_core_config()`` still answers for the offline path's
    ``create_provider`` call; only ``sandbox`` needs to be real.
    """
    config = MagicMock()
    config.sandbox = SimpleNamespace(provider=name)
    manager = MagicMock()
    manager.config = config
    return MagicMock(get_instance=MagicMock(return_value=manager))


async def _get_stats(
    client, workspace, meta, *, side_effect=None, provider_name="daytona"
):
    session, sandbox = _sandbox_with_metadata(meta, side_effect=side_effect)
    with (
        patch(
            "src.server.app.workspace_sandbox.db_get_workspace",
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
            "src.server.app.workspace_sandbox.db_get_workspace",
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
            "src.server.app.workspace_sandbox.db_get_workspace",
            AsyncMock(return_value=workspace),
        ),
        patch(
            "ptc_agent.core.sandbox.providers.create_provider",
            MagicMock(return_value=provider),
        ),
        patch(
            "src.server.app.workspace_sandbox.WorkspaceManager",
            _config_with_provider(provider_name),
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
        "src.server.app.workspace_sandbox.db_get_workspace",
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
            "src.server.app.workspace_sandbox.db_get_workspace",
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
        "src.server.app.workspace_sandbox.db_get_workspace",
        AsyncMock(return_value=None),
    ):
        resp = await client.get(f"/api/v1/workspaces/{uuid.uuid4()}/sandbox/stats")

    assert resp.status_code == 404
