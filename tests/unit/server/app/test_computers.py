"""Tests for the Computers API router (src/server/app/computers.py).

Two surfaces in one file, because they are one implementation: the
``/api/v1/computers`` routes, and the ``/api/v1/workspaces/{id}/...`` lifecycle
aliases that resolve workspace to computer and call the same action functions.
The alias arms are what guarantee the sandbox settings panel keeps working
while both paths are live.
"""

from unittest.mock import ANY, AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from tests.conftest import create_test_app

USER = "test-user-123"
OTHER_USER = "someone-else"
COMPUTER_ID = "11111111-1111-4111-8111-111111111111"
WORKSPACE_ID = "22222222-2222-4222-8222-222222222222"


@pytest.fixture(autouse=True)
def workspace_alias_quota_gate_off():
    with (
        patch(
            "src.server.dependencies.usage_limits.platform_gating_active",
            return_value=False,
        ),
        patch(
            "src.server.app.computers.platform_gating_active", return_value=False
        ),
    ):
        yield


@pytest_asyncio.fixture
async def client():
    from src.server.app.computers import router

    app = create_test_app(router)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c


@pytest_asyncio.fixture
async def ws_client():
    """A client on the workspaces router, for the alias arms."""
    from src.server.app.workspaces import router

    app = create_test_app(router)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c


@pytest.fixture
def computer(sample_computer_dict):
    return sample_computer_dict(computer_id=COMPUTER_ID)


def _ws(**overrides):
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    data = {
        "workspace_id": WORKSPACE_ID,
        "user_id": USER,
        "name": "Test Workspace",
        "description": None,
        "sandbox_id": "sandbox-abc",
        "computer_id": COMPUTER_ID,
        "status": "running",
        "is_pinned": False,
        "sort_order": 0,
        "created_at": now,
        "updated_at": now,
        "last_activity_at": None,
        "stopped_at": None,
        "config": None,
        "resource_tier": "standard",
        "is_always_on": False,
    }
    data.update(overrides)
    return data


# ---------------------------------------------------------------------------
# Create: the one route that allocates a machine, so the one that is metered
# ---------------------------------------------------------------------------


@pytest.fixture
def entitled():
    """The tier gate dials the platform; these tests are about the route."""
    with patch(
        "src.server.app.computers.assert_spec_allowed", AsyncMock(return_value=None)
    ) as gate:
        yield gate


@pytest.mark.asyncio
async def test_create_computer_returns_the_new_machine(client, computer, entitled):
    manager = MagicMock()
    manager.create_computer_for_user = AsyncMock(
        return_value={**computer, "status": "stopped", "provider_ref": None}
    )
    with patch("src.server.app.computers._computer_manager", return_value=manager):
        resp = await client.post(
            "/api/v1/computers",
            json={"name": "Research box", "resource_tier": "performance"},
        )

    assert resp.status_code == 201
    body = resp.json()
    assert body["computer_id"] == COMPUTER_ID
    # Nothing is provisioned here, so the machine costs nothing until it runs.
    assert body["status"] == "stopped"
    assert body["provider_ref"] is None
    kwargs = manager.create_computer_for_user.call_args.kwargs
    assert kwargs["name"] == "Research box"
    assert kwargs["resource_tier"] == "performance"


@pytest.mark.asyncio
async def test_the_first_computer_is_primary_and_the_second_is_not(
    client, computer, entitled
):
    """Without this the first machine a user names is never their primary, and
    their first project then mints a second "My computer" beside it. The route
    only asks; the insert's NOT EXISTS is what decides, so asking is race-safe.
    """
    primaries: set[str] = set()

    async def _create(user_id, *, name=None, resource_tier=None, is_primary=False):
        # Mirrors the insert's `%s AND NOT EXISTS (... is_primary ...)`, which
        # test_computer_db locks as SQL.
        won = bool(is_primary) and user_id not in primaries
        if won:
            primaries.add(user_id)
        return {**computer, "is_primary": won}

    manager = MagicMock()
    manager.create_computer_for_user = AsyncMock(side_effect=_create)
    with patch("src.server.app.computers._computer_manager", return_value=manager):
        first = await client.post("/api/v1/computers", json={"name": "Research box"})
        second = await client.post("/api/v1/computers", json={"name": "Second box"})

    assert first.json()["is_primary"] is True
    assert second.json()["is_primary"] is False
    assert manager.create_computer_for_user.call_args.kwargs["is_primary"] is True


@pytest.mark.asyncio
async def test_create_computer_defaults_to_the_standard_tier(
    client, computer, entitled
):
    manager = MagicMock()
    manager.create_computer_for_user = AsyncMock(return_value=computer)
    with patch("src.server.app.computers._computer_manager", return_value=manager):
        resp = await client.post("/api/v1/computers", json={})

    assert resp.status_code == 201
    kwargs = manager.create_computer_for_user.call_args.kwargs
    assert kwargs["resource_tier"] == "standard"
    assert kwargs["name"] is None


@pytest.mark.asyncio
async def test_create_serializes_the_tier_check_with_the_insert(
    client, computer
):
    events = []

    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def quota_lock(user_id):
        events.append(("lock", user_id))
        yield
        events.append(("unlock", user_id))

    manager = MagicMock()
    manager.create_computer_for_user = AsyncMock(
        side_effect=lambda *_args, **_kwargs: (
            events.append(("insert", USER)) or computer
        )
    )
    gate = AsyncMock(side_effect=lambda *_args, **_kwargs: events.append(("gate", USER)))
    with (
        patch("src.server.app.computers._computer_manager", return_value=manager),
        patch("src.server.app.computers.assert_spec_allowed", gate),
        patch("src.server.app.computers.platform_gating_active", return_value=True),
        patch("src.server.app.computers.computer_capacity_lock", quota_lock),
    ):
        response = await client.post(
            "/api/v1/computers", json={"resource_tier": "performance"}
        )

    assert response.status_code == 201
    assert events == [
        ("lock", USER),
        ("gate", USER),
        ("insert", USER),
        ("unlock", USER),
    ]


@pytest.mark.asyncio
async def test_create_computer_defers_active_capacity_to_start():
    from src.server.app.computers import router

    app = create_test_app(router)
    post = next(
        r
        for r in app.routes
        if getattr(r, "path", None) == "/api/v1/computers" and "POST" in r.methods
    )
    assert "enforce_computer_limit" not in {
        d.call.__name__ for d in post.dependant.dependencies
    }


@pytest.mark.asyncio
async def test_a_capped_user_can_prepare_a_stopped_computer(computer):
    from fastapi import HTTPException

    from src.server.app.computers import router
    from src.server.dependencies.usage_limits import enforce_computer_limit

    app = create_test_app(router)

    def _capped():
        raise HTTPException(status_code=429, detail={"error": "limit_exceeded"})

    app.dependency_overrides[enforce_computer_limit] = _capped

    manager = MagicMock()
    manager.create_computer_for_user = AsyncMock(
        return_value={**computer, "status": "stopped", "provider_ref": None}
    )
    with patch("src.server.app.computers._computer_manager", return_value=manager):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as capped_client:
            resp = await capped_client.post("/api/v1/computers", json={})

    assert resp.status_code == 201
    manager.create_computer_for_user.assert_awaited_once()


@pytest.mark.asyncio
async def test_an_unentitled_tier_is_refused_before_the_row_is_written(client):
    """The cap and the tier are separate entitlements, so clearing one does not
    buy the other; refusing after the insert would leave a machine to clean up.
    """
    from fastapi import HTTPException

    manager = MagicMock()
    manager.create_computer_for_user = AsyncMock()
    with (
        patch("src.server.app.computers._computer_manager", return_value=manager),
        patch(
            "src.server.app.computers.assert_spec_allowed",
            AsyncMock(side_effect=HTTPException(403, detail="Requires scope")),
        ),
    ):
        resp = await client.post("/api/v1/computers", json={"resource_tier": "max"})

    assert resp.status_code == 403
    manager.create_computer_for_user.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_unknown_tier_is_rejected_by_the_model(client):
    resp = await client.post("/api/v1/computers", json={"resource_tier": "huge"})
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Reads: list, get, ownership
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_computers_returns_the_callers_own(client, computer):
    with (
        patch(
            "src.server.app.computers.get_computers_for_user",
            AsyncMock(return_value=[computer]),
        ) as get_all,
        patch(
            "src.server.app.computers.count_live_workspaces_by_computer",
            AsyncMock(return_value={COMPUTER_ID: 3}),
        ) as counted,
    ):
        resp = await client.get("/api/v1/computers")

    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 1
    assert body["computers"][0]["computer_id"] == COMPUTER_ID
    assert body["computers"][0]["provider_ref"] == "sandbox-abc"
    # Stopping a machine takes every project on it down together, so the card
    # says how many before asking. One query for the whole list.
    assert body["computers"][0]["workspace_count"] == 3
    counted.assert_awaited_once_with([COMPUTER_ID])
    get_all.assert_awaited_once_with(USER)


@pytest.mark.asyncio
async def test_a_machine_with_no_live_project_counts_zero(client, computer):
    """A machine absent from the count is zero, not missing."""
    with (
        patch(
            "src.server.app.computers.get_computers_for_user",
            AsyncMock(return_value=[computer]),
        ),
        patch(
            "src.server.app.computers.count_live_workspaces_by_computer",
            AsyncMock(return_value={}),
        ),
    ):
        resp = await client.get("/api/v1/computers")

    assert resp.status_code == 200
    assert resp.json()["computers"][0]["workspace_count"] == 0


@pytest.mark.asyncio
async def test_get_computer_returns_the_row(client, computer):
    with (
        patch(
            "src.server.app.computers.get_computer", AsyncMock(return_value=computer)
        ),
        patch(
            "src.server.app.computers.count_live_workspaces_by_computer",
            AsyncMock(return_value={COMPUTER_ID: 2}),
        ),
    ):
        resp = await client.get(f"/api/v1/computers/{COMPUTER_ID}")

    assert resp.status_code == 200
    body = resp.json()
    assert body["kind"] == "daytona"
    assert body["root_dir"] == "/home/workspace"
    assert body["is_primary"] is True
    assert body["workspace_count"] == 2


@pytest.mark.asyncio
async def test_get_computer_not_found(client):
    with patch("src.server.app.computers.get_computer", AsyncMock(return_value=None)):
        resp = await client.get(f"/api/v1/computers/{COMPUTER_ID}")

    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_get_computer_forbidden_for_another_user(client, sample_computer_dict):
    foreign = sample_computer_dict(computer_id=COMPUTER_ID, user_id=OTHER_USER)
    with patch(
        "src.server.app.computers.get_computer", AsyncMock(return_value=foreign)
    ):
        resp = await client.get(f"/api/v1/computers/{COMPUTER_ID}")

    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_provider_ref_is_withheld_from_a_non_owner(sample_computer_dict):
    """The vendor id addresses a real billed machine, so it never leaves the row
    for a caller who does not own it. Asserted on the serializer because every
    route 403s first: this is the guard for a future shared-computer reader."""
    from src.server.app.computers import computer_to_response

    row = sample_computer_dict(provider_ref="sandbox-secret")
    assert computer_to_response(row, owner=True).provider_ref == "sandbox-secret"
    assert computer_to_response(row, owner=False).provider_ref is None


@pytest.mark.asyncio
async def test_an_unobserved_layout_is_null_not_version_zero(sample_computer_dict):
    """046 backfills the column at 0 and only an asset sync replaces it, so a
    reader that renders 0 as a version is reporting a layout nothing observed."""
    from src.server.app.computers import computer_to_response

    assert computer_to_response(sample_computer_dict()).layout_version == 3
    assert (
        computer_to_response(sample_computer_dict(layout_version=0)).layout_version
        is None
    )


# ---------------------------------------------------------------------------
# Session info
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_session_reports_the_row_without_resolving_when_not_running(
    client, sample_computer_dict, mock_computer_manager
):
    """A GET must never provision a machine, so a stopped computer is answered
    from Postgres alone."""
    stopped = sample_computer_dict(computer_id=COMPUTER_ID, status="stopped")
    with (
        patch("src.server.app.computers.get_computer", AsyncMock(return_value=stopped)),
        patch(
            "src.server.app.computers._computer_manager",
            return_value=mock_computer_manager,
        ),
    ):
        resp = await client.get(f"/api/v1/computers/{COMPUTER_ID}/session")

    assert resp.status_code == 200
    assert resp.json() == {
        "computer_id": COMPUTER_ID,
        "status": "stopped",
        "provider_ref": "sandbox-abc",
        "ready": False,
        "session_provider_ref": None,
    }
    mock_computer_manager.get_session_for_computer.assert_not_awaited()


@pytest.mark.asyncio
async def test_session_reports_what_this_worker_holds(
    client, computer, mock_computer_manager
):
    session = MagicMock()
    session.sandbox.sandbox_id = "sandbox-live"
    session.sandbox.runtime = MagicMock()
    mock_computer_manager.cached_session_for_computer = MagicMock(return_value=session)

    with (
        patch(
            "src.server.app.computers.get_computer", AsyncMock(return_value=computer)
        ),
        patch(
            "src.server.app.computers._computer_manager",
            return_value=mock_computer_manager,
        ),
    ):
        resp = await client.get(f"/api/v1/computers/{COMPUTER_ID}/session")

    assert resp.status_code == 200
    body = resp.json()
    assert body["ready"] is True
    assert body["session_provider_ref"] == "sandbox-live"
    mock_computer_manager.cached_session_for_computer.assert_called_once_with(computer)
    mock_computer_manager.get_session_for_computer.assert_not_awaited()


@pytest.mark.asyncio
async def test_session_degrades_when_this_worker_has_no_cached_session(
    client, computer, mock_computer_manager
):
    mock_computer_manager.cached_session_for_computer = MagicMock(return_value=None)
    with (
        patch(
            "src.server.app.computers.get_computer", AsyncMock(return_value=computer)
        ),
        patch(
            "src.server.app.computers._computer_manager",
            return_value=mock_computer_manager,
        ),
    ):
        resp = await client.get(f"/api/v1/computers/{COMPUTER_ID}/session")

    assert resp.status_code == 200
    assert resp.json()["ready"] is False
    assert resp.json()["status"] == "running"
    mock_computer_manager.get_session_for_computer.assert_not_awaited()


# ---------------------------------------------------------------------------
# Status events (SSE)
# ---------------------------------------------------------------------------


def _parse_sse(raw: str) -> list[tuple[str, str]]:
    """(event, data) pairs from one SSE chunk; comments and blanks ignored."""
    name, data = "", ""
    for line in raw.split("\n"):
        if line.startswith("event:"):
            name = line[6:].strip()
        elif line.startswith("data:"):
            data = line[5:].strip()
    return [(name, data)] if name else []


async def _collect_sse_events(client, url, *, want_events: int, timeout: float = 2.0):
    """Open an SSE stream and collect up to `want_events` events, then close."""
    import asyncio
    import json as _json

    events: list[tuple[str, dict]] = []
    async with client.stream("GET", url) as resp:
        assert resp.status_code == 200
        buffer = ""

        async def _read_loop():
            nonlocal buffer
            async for chunk in resp.aiter_text():
                buffer += chunk
                while "\n\n" in buffer:
                    raw, _, buffer = buffer.partition("\n\n")
                    for name, data in _parse_sse(raw + "\n\n"):
                        if name == "status" and data:
                            events.append((name, _json.loads(data)))
                        elif name == "timeout":
                            events.append((name, {}))
                    if len(events) >= want_events:
                        return

        try:
            await asyncio.wait_for(_read_loop(), timeout=timeout)
        except asyncio.TimeoutError:
            pass
    return events


@pytest.mark.asyncio
async def test_computer_events_streams_the_transition(client, sample_computer_dict):
    """The frozen frame: {computer_id, status}. The subject is the machine, so a
    client watching one no longer has to pick a project to watch it through."""
    from contextlib import asynccontextmanager

    starting = sample_computer_dict(computer_id=COMPUTER_ID, status="starting")
    running = {**starting, "status": "running"}
    rows = iter([starting, starting, running])
    subscribed: list[str] = []

    async def fake_get(computer_id):
        return next(rows, running)

    @asynccontextmanager
    async def fake_subscribe(computer_id):
        subscribed.append(computer_id)
        sent = False

        async def wait(timeout):
            nonlocal sent
            if not sent:
                sent = True
                return ("message", {"computer_id": computer_id, "status": "running"})
            return ("timeout", None)

        yield wait

    with (
        patch(
            "src.server.app.computers.get_computer", new=AsyncMock(side_effect=fake_get)
        ),
        patch(
            "src.server.app.computers.subscribe_to_computer_status",
            new=fake_subscribe,
        ),
    ):
        events = await _collect_sse_events(
            client, f"/api/v1/computers/{COMPUTER_ID}/events", want_events=2
        )

    assert subscribed == [COMPUTER_ID]
    payloads = [e[1] for e in events if e[0] == "status"]
    assert payloads == [
        {"computer_id": COMPUTER_ID, "status": "starting"},
        {"computer_id": COMPUTER_ID, "status": "running"},
    ]


@pytest.mark.asyncio
async def test_computer_events_forwards_a_failed_start_with_its_reason(
    client, sample_computer_dict
):
    """The one hint the stream does not confirm against the row. A failed start
    hands its claim back, so the row reads 'stopped' again and a confirm finds
    nothing to report; the reason riding on the publish is both what marks the
    message as a failure report and the only place that reason exists."""
    from contextlib import asynccontextmanager

    stopped = sample_computer_dict(computer_id=COMPUTER_ID, status="stopped")

    @asynccontextmanager
    async def fake_subscribe(computer_id):
        sent = False

        async def wait(timeout):
            nonlocal sent
            if not sent:
                sent = True
                return (
                    "message",
                    {
                        "computer_id": computer_id,
                        "status": "error",
                        "error": "Unknown sandbox provider: 'nope'",
                    },
                )
            return ("timeout", None)

        yield wait

    with (
        patch(
            "src.server.app.computers.get_computer",
            new=AsyncMock(return_value=stopped),
        ),
        patch(
            "src.server.app.computers.subscribe_to_computer_status",
            new=fake_subscribe,
        ),
    ):
        events = await _collect_sse_events(
            client, f"/api/v1/computers/{COMPUTER_ID}/events", want_events=2
        )

    payloads = [e[1] for e in events if e[0] == "status"]
    assert payloads == [
        {"computer_id": COMPUTER_ID, "status": "stopped"},
        {
            "computer_id": COMPUTER_ID,
            "status": "error",
            "error": "Unknown sandbox provider: 'nope'",
        },
    ]


@pytest.mark.asyncio
async def test_computer_events_forwards_the_archived_refinement(
    client, sample_computer_dict
):
    """A slow-restore hint is emitted without a DB re-read and carries the
    payload's own status, the same contract the workspace stream has."""
    from contextlib import asynccontextmanager

    starting = sample_computer_dict(computer_id=COMPUTER_ID, status="starting")
    running = {**starting, "status": "running"}
    # initial read, post-subscribe read; the archived refinement does NOT
    # re-read; the running transition re-reads.
    rows = iter([starting, starting, running])

    @asynccontextmanager
    async def fake_subscribe(computer_id):
        msgs = iter(
            [
                {
                    "computer_id": computer_id,
                    "status": "starting",
                    "sandbox_state": "archived",
                },
                {"computer_id": computer_id, "status": "running"},
            ]
        )

        async def wait(timeout):
            payload = next(msgs, None)
            return ("message", payload) if payload else ("timeout", None)

        yield wait

    with (
        patch(
            "src.server.app.computers.get_computer",
            new=AsyncMock(side_effect=lambda _cid: next(rows, running)),
        ),
        patch(
            "src.server.app.computers.subscribe_to_computer_status",
            new=fake_subscribe,
        ),
    ):
        events = await _collect_sse_events(
            client, f"/api/v1/computers/{COMPUTER_ID}/events", want_events=3
        )

    assert events[1][1] == {
        "computer_id": COMPUTER_ID,
        "status": "starting",
        "sandbox_state": "archived",
    }
    assert events[2][1] == {"computer_id": COMPUTER_ID, "status": "running"}


@pytest.mark.asyncio
async def test_computer_events_closes_on_a_terminal_initial_status(
    client, sample_computer_dict
):
    running = sample_computer_dict(computer_id=COMPUTER_ID, status="running")
    with patch(
        "src.server.app.computers.get_computer", new=AsyncMock(return_value=running)
    ):
        events = await _collect_sse_events(
            client,
            f"/api/v1/computers/{COMPUTER_ID}/events",
            want_events=1,
            timeout=1.0,
        )

    assert events == [("status", {"computer_id": COMPUTER_ID, "status": "running"})]


@pytest.mark.asyncio
async def test_computer_events_polls_when_pubsub_is_unavailable(
    client, sample_computer_dict
):
    """Redis down degrades to a DB poll, never to a dead stream: the route must
    still report the transition."""
    from contextlib import asynccontextmanager

    import src.server.app.status_stream as status_stream

    stopped = sample_computer_dict(computer_id=COMPUTER_ID, status="stopped")
    running = {**stopped, "status": "running"}
    rows = iter([stopped, running])

    @asynccontextmanager
    async def no_subscription(computer_id):
        yield None

    with (
        patch(
            "src.server.app.computers.get_computer",
            new=AsyncMock(side_effect=lambda _cid: next(rows, running)),
        ),
        patch(
            "src.server.app.computers.subscribe_to_computer_status",
            new=no_subscription,
        ),
        patch.object(status_stream, "EVENTS_KEEPALIVE_S", 0.01),
    ):
        events = await _collect_sse_events(
            client, f"/api/v1/computers/{COMPUTER_ID}/events", want_events=2
        )

    assert [e[1]["status"] for e in events if e[0] == "status"] == [
        "stopped",
        "running",
    ]


@pytest.mark.asyncio
async def test_computer_events_forbidden_for_another_user(client, sample_computer_dict):
    foreign = sample_computer_dict(computer_id=COMPUTER_ID, user_id=OTHER_USER)
    with patch(
        "src.server.app.computers.get_computer", new=AsyncMock(return_value=foreign)
    ):
        resp = await client.get(f"/api/v1/computers/{COMPUTER_ID}/events")

    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_computer_events_not_found(client):
    with patch(
        "src.server.app.computers.get_computer", new=AsyncMock(return_value=None)
    ):
        resp = await client.get(f"/api/v1/computers/{COMPUTER_ID}/events")

    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_start_from_stopped(client, sample_computer_dict, mock_computer_manager):
    stopped = sample_computer_dict(computer_id=COMPUTER_ID, status="stopped")
    running = sample_computer_dict(computer_id=COMPUTER_ID, status="running")
    mock_computer_manager.start_computer.return_value = running
    with (
        patch(
            "src.server.app.computers.get_computer",
            AsyncMock(return_value=stopped),
        ),
        patch(
            "src.server.app.computers._computer_manager",
            return_value=mock_computer_manager,
        ),
    ):
        resp = await client.post(f"/api/v1/computers/{COMPUTER_ID}/start")

    assert resp.status_code == 200
    assert resp.json()["status"] == "running"
    mock_computer_manager.start_computer.assert_awaited_once_with(COMPUTER_ID)


@pytest.mark.asyncio
async def test_start_is_a_no_op_when_already_running(
    client, computer, mock_computer_manager
):
    with (
        patch(
            "src.server.app.computers.get_computer", AsyncMock(return_value=computer)
        ),
        patch(
            "src.server.app.computers._computer_manager",
            return_value=mock_computer_manager,
        ),
    ):
        resp = await client.post(f"/api/v1/computers/{COMPUTER_ID}/start")

    assert resp.status_code == 200
    assert resp.json()["status"] == "running"
    mock_computer_manager.start_computer.assert_not_awaited()


@pytest.mark.asyncio
async def test_blocking_start_joins_an_existing_start(
    client, sample_computer_dict, mock_computer_manager
):
    starting = sample_computer_dict(computer_id=COMPUTER_ID, status="starting")
    running = sample_computer_dict(computer_id=COMPUTER_ID, status="running")
    mock_computer_manager.start_computer.return_value = running
    with (
        patch(
            "src.server.app.computers.get_computer",
            AsyncMock(return_value=starting),
        ),
        patch(
            "src.server.app.computers._computer_manager",
            return_value=mock_computer_manager,
        ),
    ):
        resp = await client.post(f"/api/v1/computers/{COMPUTER_ID}/start")

    assert resp.status_code == 200
    assert resp.json()["status"] == "running"
    mock_computer_manager.start_computer.assert_awaited_once_with(COMPUTER_ID)


@pytest.mark.asyncio
async def test_start_rejects_a_state_it_cannot_start_from(
    client, sample_computer_dict, mock_computer_manager
):
    stopping = sample_computer_dict(computer_id=COMPUTER_ID, status="stopping")
    with (
        patch(
            "src.server.app.computers.get_computer", AsyncMock(return_value=stopping)
        ),
        patch(
            "src.server.app.computers._computer_manager",
            return_value=mock_computer_manager,
        ),
    ):
        resp = await client.post(f"/api/v1/computers/{COMPUTER_ID}/start")

    assert resp.status_code == 400
    mock_computer_manager.start_computer.assert_not_awaited()


@pytest.mark.asyncio
async def test_start_repairs_an_abandoned_stopping_computer(
    client, sample_computer_dict, mock_computer_manager
):
    stopping = sample_computer_dict(computer_id=COMPUTER_ID, status="stopping")
    stopped = {**stopping, "status": "stopped"}
    running = {**stopping, "status": "running"}
    mock_computer_manager.reconcile_stopping_computer.return_value = stopped
    mock_computer_manager.start_computer.return_value = running
    with (
        patch(
            "src.server.app.computers.get_computer",
            AsyncMock(side_effect=[stopping, stopped]),
        ),
        patch(
            "src.server.app.computers._computer_manager",
            return_value=mock_computer_manager,
        ),
    ):
        response = await client.post(f"/api/v1/computers/{COMPUTER_ID}/start")

    assert response.status_code == 200
    mock_computer_manager.reconcile_stopping_computer.assert_awaited_once_with(
        COMPUTER_ID
    )
    mock_computer_manager.start_computer.assert_awaited_once_with(COMPUTER_ID)


@pytest.mark.asyncio
async def test_lazy_start_returns_202_and_schedules(
    client, sample_computer_dict, mock_computer_manager
):
    import asyncio

    stopped = sample_computer_dict(computer_id=COMPUTER_ID, status="stopped")
    async def start(_id, *, on_start_claimed):
        stopped['status'] = 'starting'
        on_start_claimed()
    mock_computer_manager.start_computer.side_effect = start
    with (
        patch("src.server.app.computers.get_computer", AsyncMock(return_value=stopped)),
        patch(
            "src.server.app.computers._computer_manager",
            return_value=mock_computer_manager,
        ),
    ):
        resp = await client.post(f"/api/v1/computers/{COMPUTER_ID}/start?lazy=true")
        assert resp.status_code == 202
        assert resp.json()["status"] == "starting"
        # The background task holds a strong ref until it finishes; let it run.
        await asyncio.sleep(0)

    mock_computer_manager.start_computer.assert_awaited_once_with(COMPUTER_ID, on_start_claimed=ANY)


@pytest.mark.asyncio
async def test_deduplicated_lazy_waiters_acknowledge_before_provisioning_finishes(
    client, sample_computer_dict, mock_computer_manager
):
    import asyncio

    from src.server.app import computers

    row = sample_computer_dict(computer_id=COMPUTER_ID, status="creating")
    before_claim = asyncio.Event()
    release_claim = asyncio.Event()
    release_provision = asyncio.Event()
    scheduled_twice = asyncio.Event()
    schedules = 0
    schedule = computers.schedule_computer_start

    def tracked_schedule(*args, **kwargs):
        nonlocal schedules
        schedules += 1
        if schedules == 2:
            scheduled_twice.set()
        return schedule(*args, **kwargs)

    async def start(_id, *, on_start_claimed):
        before_claim.set()
        await release_claim.wait()
        row["status"] = "starting"
        on_start_claimed()
        await release_provision.wait()

    mock_computer_manager.start_computer.side_effect = start
    with (
        patch.object(computers, "get_computer", AsyncMock(return_value=row)),
        patch.object(computers, "_computer_manager", return_value=mock_computer_manager),
        patch.object(computers, "schedule_computer_start", tracked_schedule),
    ):
        requests = []
        try:
            requests.append(asyncio.create_task(client.post(
                f"/api/v1/computers/{COMPUTER_ID}/start?lazy=true"
            )))
            await asyncio.wait_for(before_claim.wait(), 1)
            requests.append(asyncio.create_task(client.post(
                f"/api/v1/computers/{COMPUTER_ID}/start?lazy=true"
            )))
            await asyncio.wait_for(scheduled_twice.wait(), 1)
            release_claim.set()
            responses = await asyncio.wait_for(asyncio.gather(*requests), 1)
            assert [r.status_code for r in responses] == [202, 202]
            assert all(r.json()["status"] == "starting" for r in responses)
            assert not release_provision.is_set()
            mock_computer_manager.start_computer.assert_awaited_once()
        finally:
            release_claim.set()
            release_provision.set()
            await asyncio.gather(*requests, return_exceptions=True)
            await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_stop(client, computer, sample_computer_dict, mock_computer_manager):
    stopped = sample_computer_dict(computer_id=COMPUTER_ID, status="stopped")
    mock_computer_manager.stop_computer.return_value = stopped
    with (
        patch(
            "src.server.app.computers.get_computer", AsyncMock(return_value=computer)
        ) as read,
        patch(
            "src.server.app.computers._computer_manager",
            return_value=mock_computer_manager,
        ),
    ):
        resp = await client.post(f"/api/v1/computers/{COMPUTER_ID}/stop")

    assert resp.status_code == 200
    assert resp.json()["status"] == "stopped"
    mock_computer_manager.stop_computer.assert_awaited_once_with(COMPUTER_ID)
    # One read for ownership; the settled row comes from the transition itself.
    assert read.await_count == 1


@pytest.mark.asyncio
async def test_archive(client, sample_computer_dict, mock_computer_manager):
    stopped = sample_computer_dict(computer_id=COMPUTER_ID, status="stopped")
    mock_computer_manager.archive_computer.return_value = stopped
    with (
        patch("src.server.app.computers.get_computer", AsyncMock(return_value=stopped)),
        patch(
            "src.server.app.computers._computer_manager",
            return_value=mock_computer_manager,
        ),
    ):
        resp = await client.post(f"/api/v1/computers/{COMPUTER_ID}/archive")

    assert resp.status_code == 200
    mock_computer_manager.archive_computer.assert_awaited_once_with(COMPUTER_ID)


@pytest.mark.asyncio
async def test_stop_forbidden_for_another_user(
    client, sample_computer_dict, mock_computer_manager
):
    foreign = sample_computer_dict(computer_id=COMPUTER_ID, user_id=OTHER_USER)
    with (
        patch("src.server.app.computers.get_computer", AsyncMock(return_value=foreign)),
        patch(
            "src.server.app.computers._computer_manager",
            return_value=mock_computer_manager,
        ),
    ):
        resp = await client.post(f"/api/v1/computers/{COMPUTER_ID}/stop")

    assert resp.status_code == 403
    mock_computer_manager.stop_computer.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_missing_runtime_is_503_not_a_silent_success(
    client, sample_computer_dict
):
    """The read routes stay up without the computer runtime; an action must not
    claim a transition it could not make."""
    from src.server.app.computers import ComputerRuntimeUnavailable

    stopped = sample_computer_dict(computer_id=COMPUTER_ID, status="stopped")
    with (
        patch("src.server.app.computers.get_computer", AsyncMock(return_value=stopped)),
        patch(
            "src.server.app.computers._computer_manager",
            side_effect=ComputerRuntimeUnavailable("no module"),
        ),
    ):
        resp = await client.post(f"/api/v1/computers/{COMPUTER_ID}/stop")

    assert resp.status_code == 503


@pytest.mark.asyncio
async def test_storage_reuses_a_breakdown_younger_than_a_minute(
    client, computer, mock_computer_manager
):
    """The panel asks on every open; a du over the whole root on each one is
    what the reuse window exists to avoid, so the route has to ask for it."""
    from src.server.services.computer_disk import STORAGE_BREAKDOWN_MAX_AGE_SECONDS

    mock_computer_manager.measures_disk = MagicMock(return_value=True)
    mock_computer_manager.refresh_computer_disk = AsyncMock(
        return_value=(computer, {"alpha": 300})
    )
    with (
        patch("src.server.app.computers.get_computer", AsyncMock(return_value=computer)),
        patch("src.server.app.computers._computer_manager", return_value=mock_computer_manager),
        patch(
            "src.server.app.computers.get_live_workspace_folders_for_computer",
            AsyncMock(return_value=[]),
        ),
    ):
        resp = await client.get(f"/api/v1/computers/{COMPUTER_ID}/storage")

    assert resp.status_code == 200
    assert resp.json()["live"] is True
    assert mock_computer_manager.refresh_computer_disk.await_args.kwargs == {
        "breakdown": True,
        "breakdown_max_age_s": STORAGE_BREAKDOWN_MAX_AGE_SECONDS,
    }


# ---------------------------------------------------------------------------
# Spec tier and always-on
# ---------------------------------------------------------------------------


def _spec_claim(computer, **change):
    from datetime import datetime, timezone

    return {
        **computer,
        "spec_change": {
            "target_tier": "performance",
            "from_tier": "standard",
            "state": "in_progress",
            "error": None,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "finished_at": None,
            "claim_id": "claim-1",
            **change,
        },
        "spec_change_stale": False,
    }


def _spec_client_patches(computer, manager):
    return (
        patch(
            "src.server.app.computers.get_computer",
            AsyncMock(return_value=computer),
        ),
        patch(
            "src.server.app.computers._computer_manager",
            return_value=manager,
        ),
    )


@pytest.mark.asyncio
async def test_set_spec_accepts_then_runs_the_claim_in_the_background(
    client, computer, mock_computer_manager
):
    """A recreate outlives an edge-proxied request, so the route answers 202 and
    hands the claim to a background run whose outcome is read back from the row."""
    import inspect

    from src.server.services.computer_manager import ComputerManager

    mock_computer_manager.precheck_computer_spec = AsyncMock(return_value=False)
    mock_computer_manager.claim_computer_spec = AsyncMock(
        return_value=_spec_claim(computer)
    )
    mock_computer_manager.run_accepted_spec_change = AsyncMock()
    get_row, get_manager = _spec_client_patches(computer, mock_computer_manager)
    with (
        get_row,
        get_manager,
        patch(
            "src.server.app.computers.count_live_workspaces_by_computer",
            AsyncMock(return_value={COMPUTER_ID: 2}),
        ),
    ):
        resp = await client.post(
            f"/api/v1/computers/{COMPUTER_ID}/spec", json={"tier": "performance"}
        )
        await _drain_start_tasks()

    assert resp.status_code == 202
    body = resp.json()
    assert body["spec_change"]["state"] == "in_progress"
    assert body["spec_change"]["claim_id"] == "claim-1"
    assert body["workspace_count"] == 2
    mock_computer_manager.claim_computer_spec.assert_awaited_once_with(
        COMPUTER_ID, "performance"
    )
    mock_computer_manager.run_accepted_spec_change.assert_awaited_once_with(
        COMPUTER_ID, "performance", user_id=USER, claim_id="claim-1"
    )
    # The manager is a bare mock: hold every call the route made to the real
    # signatures, so a renamed method or keyword fails here, not in production.
    for name in (
        "precheck_computer_spec",
        "claim_computer_spec",
        "run_accepted_spec_change",
    ):
        call = getattr(mock_computer_manager, name).await_args
        inspect.signature(getattr(ComputerManager, name)).bind(
            None, *call.args, **call.kwargs
        )


def _assert_refusal(resp, code):
    assert resp.status_code == 409
    detail = resp.json()["detail"]
    assert detail["code"] == code
    assert isinstance(detail["message"], str) and detail["message"]
    assert detail["files"] == []


@pytest.mark.asyncio
async def test_set_spec_refuses_a_second_change_while_one_runs(
    client, computer, mock_computer_manager
):
    mock_computer_manager.precheck_computer_spec = AsyncMock(return_value=False)
    mock_computer_manager.claim_computer_spec = AsyncMock(return_value=None)
    mock_computer_manager.run_accepted_spec_change = AsyncMock()
    get_row, get_manager = _spec_client_patches(computer, mock_computer_manager)
    with get_row, get_manager:
        resp = await client.post(
            f"/api/v1/computers/{COMPUTER_ID}/spec", json={"tier": "performance"}
        )

    _assert_refusal(resp, "spec_in_progress")
    mock_computer_manager.run_accepted_spec_change.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_repeat_of_a_running_change_is_refused_not_read_as_done(
    client, computer, mock_computer_manager
):
    """A running change persists its target tier early, so the same-tier
    shortcut would answer a repeat 200 as if the change had finished."""
    row = _spec_claim({**computer, "resource_tier": "performance"})
    mock_computer_manager.precheck_computer_spec = AsyncMock(return_value=True)
    get_row, get_manager = _spec_client_patches(row, mock_computer_manager)
    with get_row, get_manager:
        resp = await client.post(
            f"/api/v1/computers/{COMPUTER_ID}/spec", json={"tier": "performance"}
        )

    _assert_refusal(resp, "spec_in_progress")


@pytest.mark.asyncio
async def test_a_stale_change_does_not_block_a_new_one(
    client, computer, mock_computer_manager
):
    """The database says the old claim is stale, so the route reads it as
    interrupted and lets the claim take the row over."""
    row = _spec_claim(computer)
    row["spec_change_stale"] = True
    mock_computer_manager.precheck_computer_spec = AsyncMock(return_value=False)
    mock_computer_manager.claim_computer_spec = AsyncMock(
        return_value=_spec_claim(computer, claim_id="claim-2")
    )
    mock_computer_manager.run_accepted_spec_change = AsyncMock()
    get_row, get_manager = _spec_client_patches(row, mock_computer_manager)
    with (
        get_row,
        get_manager,
        patch(
            "src.server.app.computers.count_live_workspaces_by_computer",
            AsyncMock(return_value={}),
        ),
    ):
        resp = await client.post(
            f"/api/v1/computers/{COMPUTER_ID}/spec", json={"tier": "performance"}
        )

    assert resp.status_code == 202
    assert resp.json()["spec_change"]["claim_id"] == "claim-2"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("raised", "code"),
    [("turn", "turn_active"), ("busy", "busy")],
)
async def test_set_spec_refuses_before_accepting_in_the_outcome_shape(
    client, computer, mock_computer_manager, raised, code
):
    from src.server.services.computer_errors import (
        ComputerBusyError,
        MachineBusyError,
    )

    exc = {
        "turn": MachineBusyError("turn running"),
        "busy": ComputerBusyError("mid-operation"),
    }[raised]
    mock_computer_manager.precheck_computer_spec = AsyncMock(side_effect=exc)
    mock_computer_manager.claim_computer_spec = AsyncMock()
    get_row, get_manager = _spec_client_patches(computer, mock_computer_manager)
    with get_row, get_manager:
        resp = await client.post(
            f"/api/v1/computers/{COMPUTER_ID}/spec", json={"tier": "performance"}
        )

    _assert_refusal(resp, code)
    mock_computer_manager.claim_computer_spec.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_incomplete_backup_names_its_files_on_the_spec_route_only():
    from fastapi import HTTPException

    from src.server.app.computers import _computer_action_errors
    from src.server.services.persistence.sync_result import (
        BackupIncomplete,
        UnsavedFile,
    )

    exc = BackupIncomplete("ws-1 unsaved", [UnsavedFile("a.bin", "too_large", 9)])
    with pytest.raises(HTTPException) as spec:
        async with _computer_action_errors("set spec for", "c1", spec=True):
            raise exc
    assert spec.value.status_code == 409
    assert spec.value.detail["code"] == "backup_incomplete"
    assert spec.value.detail["files"] == [
        {"path": "a.bin", "reason": "too_large", "size": 9}
    ]
    assert "ws-1" not in spec.value.detail["message"]

    with pytest.raises(HTTPException) as other:
        async with _computer_action_errors("stop", "c1"):
            raise exc
    assert other.value.status_code == 400
    assert other.value.detail == exc.user_message


@pytest.mark.asyncio
async def test_set_spec_rejects_an_unknown_tier(client, computer):
    with patch(
        "src.server.app.computers.get_computer", AsyncMock(return_value=computer)
    ):
        resp = await client.post(
            f"/api/v1/computers/{COMPUTER_ID}/spec", json={"tier": "enormous"}
        )

    assert resp.status_code == 422


@pytest.mark.asyncio
@pytest.mark.parametrize("initial_status", ["stopped", "creating"])
async def test_enabling_always_on_on_a_stopped_computer_starts_it(
    client, sample_computer_dict, mock_computer_manager, initial_status
):
    import asyncio

    stopped = sample_computer_dict(computer_id=COMPUTER_ID, status=initial_status)
    enabled = sample_computer_dict(
        computer_id=COMPUTER_ID, status=initial_status, is_always_on=True
    )
    events = []

    async def set_always_on(*args, **kwargs):
        events.append(("write", args[0]))
        return enabled
    mock_computer_manager.set_computer_always_on.side_effect = set_always_on
    async def start(_id, *, on_start_claimed):
        stopped['status'] = 'starting'
        on_start_claimed()
    mock_computer_manager.start_computer.side_effect = start
    with (
        patch(
            "src.server.app.computers.get_computer",
            AsyncMock(return_value=stopped),
        ),
        patch(
            "src.server.app.computers.count_live_workspaces_by_computer",
            AsyncMock(return_value={}),
        ),
        patch(
            "src.server.app.computers._computer_manager",
            return_value=mock_computer_manager,
        ),
    ):
        resp = await client.post(
            f"/api/v1/computers/{COMPUTER_ID}/always-on", json={"enabled": True}
        )
        await asyncio.sleep(0)

    assert resp.status_code == 200
    # Always-on means up now, so the response reports the start it just began.
    assert resp.json()["status"] == "starting"
    assert resp.json()["is_always_on"] is True
    assert events == [("write", COMPUTER_ID)]
    mock_computer_manager.set_computer_always_on.assert_awaited_once_with(
        COMPUTER_ID, True, user_id=USER
    )
    mock_computer_manager.start_computer.assert_awaited_once_with(COMPUTER_ID, on_start_claimed=ANY)


@pytest.mark.asyncio
async def test_disabling_always_on_is_never_gated_and_starts_nothing(
    client, sample_computer_dict, mock_computer_manager
):
    on = sample_computer_dict(
        computer_id=COMPUTER_ID, status="stopped", is_always_on=True
    )
    off = sample_computer_dict(
        computer_id=COMPUTER_ID, status="stopped", is_always_on=False
    )
    mock_computer_manager.set_computer_always_on.return_value = off
    with (
        patch("src.server.app.computers.get_computer", AsyncMock(return_value=on)),
        patch(
            "src.server.app.computers.count_live_workspaces_by_computer",
            AsyncMock(return_value={}),
        ),
        patch(
            "src.server.app.computers._computer_manager",
            return_value=mock_computer_manager,
        ),
    ):
        resp = await client.post(
            f"/api/v1/computers/{COMPUTER_ID}/always-on", json={"enabled": False}
        )

    assert resp.status_code == 200
    assert resp.json()["status"] == "stopped"
    mock_computer_manager.set_computer_always_on.assert_awaited_once_with(
        COMPUTER_ID, False, user_id=USER
    )
    mock_computer_manager.start_computer.assert_not_awaited()


@pytest.mark.asyncio
async def test_re_enabling_an_always_on_computer_skips_the_gate(
    client, sample_computer_dict, mock_computer_manager
):
    """An idempotent retry at the quota limit must not 429: no new slot is
    consumed by re-asserting a flag that is already set."""
    on = sample_computer_dict(
        computer_id=COMPUTER_ID, status="running", is_always_on=True
    )
    mock_computer_manager.set_computer_always_on.return_value = on
    with (
        patch("src.server.app.computers.get_computer", AsyncMock(return_value=on)),
        patch(
            "src.server.app.computers.count_live_workspaces_by_computer",
            AsyncMock(return_value={}),
        ),
        patch(
            "src.server.app.computers._computer_manager",
            return_value=mock_computer_manager,
        ),
    ):
        resp = await client.post(
            f"/api/v1/computers/{COMPUTER_ID}/always-on", json={"enabled": True}
        )

    assert resp.status_code == 200
    mock_computer_manager.set_computer_always_on.assert_awaited_once_with(
        COMPUTER_ID, True, user_id=USER
    )


# ---------------------------------------------------------------------------
# Aliases: /api/v1/workspaces/{id}/... resolves to the computer
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "action,method_name",
    [
        ("archive", "archive_workspace"),
    ],
)
async def test_the_workspace_alias_runs_one_transition(
    ws_client, mock_computer_manager, action, method_name
):
    """The sandbox settings panel posts /workspaces/{id}/{action} with the action
    interpolated, so these paths must keep answering while both surfaces are
    live. The project-addressed manager resolves the machine and runs the one
    transition, so the route must not reach the computer surface as well."""
    with (
        patch(
            "src.server.app.workspaces.db_get_workspace",
            AsyncMock(return_value=_ws(status="running")),
        ),
        patch("src.server.app.workspaces.WorkspaceManager") as MockWM,
        patch(
            "src.server.app.computers._computer_manager",
            return_value=mock_computer_manager,
        ),
    ):
        manager = AsyncMock()
        getattr(manager, method_name).return_value = _ws(status="stopped")
        MockWM.get_instance.return_value = manager
        resp = await ws_client.post(f"/api/v1/workspaces/{WORKSPACE_ID}/{action}")

    assert resp.status_code == 200
    getattr(manager, method_name).assert_awaited_once_with(WORKSPACE_ID)
    mock_computer_manager.stop_computer.assert_not_awaited()
    mock_computer_manager.archive_computer.assert_not_awaited()


@pytest.mark.asyncio
async def test_the_spec_alias_answers_with_the_workspace(ws_client):
    upgraded = _ws(resource_tier="performance")
    with (
        patch(
            "src.server.app.workspaces.db_get_workspace",
            AsyncMock(return_value=_ws()),
        ),
        patch("src.server.app.workspaces.WorkspaceManager") as MockWM,
    ):
        manager = AsyncMock()
        manager.set_workspace_spec.return_value = upgraded
        MockWM.get_instance.return_value = manager
        resp = await ws_client.post(
            f"/api/v1/workspaces/{WORKSPACE_ID}/spec", json={"tier": "performance"}
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["resource_tier"] == "performance"
    # WorkspaceResponse keeps its whole shape and gains the binding.
    assert body["workspace_id"] == WORKSPACE_ID
    assert body["computer_id"] == COMPUTER_ID
    assert body["sandbox_id"] == "sandbox-abc"
    manager.set_workspace_spec.assert_awaited_once_with(
        WORKSPACE_ID, "performance", user_id=USER
    )


@pytest.mark.asyncio
async def test_the_spec_alias_delegates_the_quota_decision_to_the_manager(ws_client):
    events = []

    manager = AsyncMock()
    manager.set_workspace_spec.side_effect = lambda *_args, **_kwargs: (
        events.append(("write", WORKSPACE_ID)) or _ws(resource_tier="performance")
    )
    with (
        patch("src.server.app.workspaces.db_get_workspace", AsyncMock(return_value=_ws())),
        patch("src.server.app.workspaces.WorkspaceManager") as manager_cls,
    ):
        manager_cls.get_instance.return_value = manager
        response = await ws_client.post(
            f"/api/v1/workspaces/{WORKSPACE_ID}/spec",
            json={"tier": "performance"},
        )

    assert response.status_code == 200
    assert events == [("write", WORKSPACE_ID)]


@pytest.mark.asyncio
async def test_the_always_on_alias_answers_with_the_workspace(ws_client):
    enabled = _ws(is_always_on=True, status="running")
    with (
        patch(
            "src.server.app.workspaces.db_get_workspace",
            AsyncMock(return_value=_ws()),
        ),
        patch("src.server.app.workspaces.WorkspaceManager") as MockWM,
    ):
        manager = AsyncMock()
        manager.set_workspace_always_on.return_value = enabled
        MockWM.get_instance.return_value = manager
        resp = await ws_client.post(
            f"/api/v1/workspaces/{WORKSPACE_ID}/always-on", json={"enabled": True}
        )

    assert resp.status_code == 200
    assert resp.json()["is_always_on"] is True
    manager.set_workspace_always_on.assert_awaited_once_with(
        WORKSPACE_ID, True, user_id=USER
    )


@pytest.mark.asyncio
async def test_the_always_on_alias_delegates_the_quota_decision_to_the_manager(ws_client):
    events = []

    manager = AsyncMock()
    manager.set_workspace_always_on.side_effect = lambda *_args, **_kwargs: (
        events.append(("write", WORKSPACE_ID))
        or _ws(is_always_on=True, status="running")
    )
    with (
        patch("src.server.app.workspaces.db_get_workspace", AsyncMock(return_value=_ws())),
        patch("src.server.app.workspaces.WorkspaceManager") as manager_cls,
    ):
        manager_cls.get_instance.return_value = manager
        response = await ws_client.post(
            f"/api/v1/workspaces/{WORKSPACE_ID}/always-on",
            json={"enabled": True},
        )

    assert response.status_code == 200
    assert events == [("write", WORKSPACE_ID)]


@pytest.mark.asyncio
@pytest.mark.parametrize("query", ["", "?lazy=true"])
async def test_the_start_alias_addresses_this_project_not_the_machine(
    ws_client, mock_computer_manager, query
):
    """Start is addressed at the project for the same reason every alias is.

    Starting the machine alone attaches whichever live sibling it finds, which
    prepares that sibling's folder and tool overlay and leaves this project
    with neither on a machine its own row now calls running.
    """
    import asyncio

    with (
        patch(
            "src.server.app.workspaces.db_get_workspace",
            AsyncMock(return_value=_ws(status="stopped")),
        ),
        patch("src.server.app.workspaces.WorkspaceManager") as MockWM,
        patch(
            "src.server.app.computers._computer_manager",
            return_value=mock_computer_manager,
        ),
    ):
        manager = AsyncMock()
        MockWM.get_instance.return_value = manager
        resp = await ws_client.post(f"/api/v1/workspaces/{WORKSPACE_ID}/start{query}")
        await asyncio.sleep(0)

    assert resp.status_code == (202 if query else 200)
    mock_computer_manager.start_computer.assert_not_awaited()
    manager.get_session_for_workspace.assert_awaited_once_with(
        WORKSPACE_ID, user_id=ANY
    )


# ---------------------------------------------------------------------------
# A background start that fails
# ---------------------------------------------------------------------------


async def _drain_start_tasks():
    """Await whatever schedule_computer_start left running."""
    import asyncio

    from src.server.app import background_starts

    await asyncio.gather(
        *list(background_starts._start_tasks.values()), return_exceptions=True
    )


@pytest.mark.asyncio
async def test_a_failed_lazy_start_settles_the_row_and_publishes_the_reason(
    client, sample_computer_dict, mock_computer_manager
):
    """The 202 is a promise that something will be published. A start that
    raises is the one transition the row cannot describe on its own, so the
    failure goes out on the machine's channel with its reason, and the claim is
    handed back first so a client re-reading on 'error' sees 'stopped'."""
    stopped = sample_computer_dict(computer_id=COMPUTER_ID, status="stopped")
    starting = sample_computer_dict(computer_id=COMPUTER_ID, status="starting")
    mock_computer_manager.start_computer = AsyncMock(
        side_effect=ValueError("provider https://user:synthetic-password@private.invalid/sandbox/private-id")
    )
    publish = AsyncMock()
    status_write = AsyncMock()

    with (
        patch(
            "src.server.app.computers.get_computer",
            AsyncMock(side_effect=[stopped, starting]),
        ),
        patch(
            "src.server.app.computers._computer_manager",
            return_value=mock_computer_manager,
        ),
        patch("src.server.app.computers.update_computer_status", status_write),
        patch("src.server.app.computers.publish_computer_status_change", publish),
    ):
        resp = await client.post(f"/api/v1/computers/{COMPUTER_ID}/start?lazy=true")
        assert resp.status_code == 503
        assert resp.json()["detail"] == "Sandbox is not reachable: please retry in a moment"
        await _drain_start_tasks()

    status_write.assert_awaited_once_with(COMPUTER_ID, "stopped", expected="starting")
    publish.assert_awaited_once_with(
        COMPUTER_ID,
        "error",
        extra={"error": "Sandbox is not reachable: please retry in a moment"},
    )


@pytest.mark.asyncio
async def test_a_claim_already_handed_back_is_not_written_again(
    sample_computer_dict, mock_computer_manager
):
    """The claim's own revert usually wins the race, and a row that has moved
    on belongs to whoever moved it."""
    from src.server.app import computers

    stopped = sample_computer_dict(computer_id=COMPUTER_ID, status="stopped")
    mock_computer_manager.start_computer = AsyncMock(side_effect=RuntimeError("boom"))
    publish = AsyncMock()
    status_write = AsyncMock()

    with (
        patch("src.server.app.computers.get_computer", AsyncMock(return_value=stopped)),
        patch.object(
            computers, "_computer_manager", return_value=mock_computer_manager
        ),
        patch.object(computers, "update_computer_status", status_write),
        patch.object(computers, "publish_computer_status_change", publish),
    ):
        computers.schedule_computer_start(COMPUTER_ID)
        await _drain_start_tasks()

    status_write.assert_not_awaited()
    publish.assert_awaited_once_with(COMPUTER_ID, "error", extra={"error": "Sandbox is not reachable: please retry in a moment"})


@pytest.mark.asyncio
async def test_a_drained_start_publishes_nothing(mock_computer_manager):
    """Cancellation is the start being taken away, not failing: the machine is
    coming down with the worker, so its subscribers are told nothing."""
    import asyncio

    from src.server.app import background_starts, computers

    started = asyncio.Event()

    async def _never_finishes(_computer_id):
        started.set()
        await asyncio.sleep(3600)

    mock_computer_manager.start_computer = AsyncMock(side_effect=_never_finishes)
    publish = AsyncMock()

    with (
        patch.object(
            computers, "_computer_manager", return_value=mock_computer_manager
        ),
        patch.object(computers, "publish_computer_status_change", publish),
    ):
        computers.schedule_computer_start(COMPUTER_ID)
        await started.wait()
        await background_starts.drain_start_tasks()

    publish.assert_not_awaited()


# ---------------------------------------------------------------------------
# Shutdown drain
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_drain_cancels_in_flight_starts(mock_computer_manager):
    """A start cancelled mid-bringup reverts its own claim; torn down abruptly
    it can leave the row wedged in 'starting'."""
    import asyncio

    from src.server.app import background_starts, computers

    started = asyncio.Event()

    async def _never_finishes(_computer_id):
        started.set()
        await asyncio.sleep(3600)

    mock_computer_manager.start_computer = AsyncMock(side_effect=_never_finishes)
    with patch.object(
        computers, "_computer_manager", return_value=mock_computer_manager
    ):
        computers.schedule_computer_start(COMPUTER_ID)
        await started.wait()
        assert f"computer:{COMPUTER_ID}" in background_starts._start_tasks
        await background_starts.drain_start_tasks()

    assert background_starts._start_tasks == {}


@pytest.mark.asyncio
async def test_a_second_lazy_start_joins_the_one_in_flight(mock_computer_manager):
    """Two 202s for the same machine must not bring it up twice. The second
    schedule finds the first task under the same key and leaves it alone; only
    once it has settled does a new schedule start the machine again."""
    import asyncio

    from src.server.app import background_starts, computers

    started = asyncio.Event()
    release = asyncio.Event()

    async def _held(_computer_id):
        started.set()
        await release.wait()

    mock_computer_manager.start_computer = AsyncMock(side_effect=_held)
    with patch.object(
        computers, "_computer_manager", return_value=mock_computer_manager
    ):
        computers.schedule_computer_start(COMPUTER_ID)
        await started.wait()
        computers.schedule_computer_start(COMPUTER_ID)
        await asyncio.sleep(0)
        assert mock_computer_manager.start_computer.await_count == 1

        release.set()
        await _drain_start_tasks()
        assert f"computer:{COMPUTER_ID}" not in background_starts._start_tasks

        computers.schedule_computer_start(COMPUTER_ID)
        await _drain_start_tasks()

    assert mock_computer_manager.start_computer.await_count == 2


@pytest.mark.asyncio
async def test_lazy_start_preserves_capacity_denial(client, sample_computer_dict, mock_computer_manager):
    from fastapi import HTTPException

    stopped = sample_computer_dict(computer_id=COMPUTER_ID, status="stopped")
    mock_computer_manager.start_computer = AsyncMock(side_effect=HTTPException(429, "capacity exhausted"))
    with (
        patch("src.server.app.computers.get_computer", AsyncMock(return_value=stopped)),
        patch("src.server.app.computers._computer_manager", return_value=mock_computer_manager),
        patch("src.server.app.computers.publish_computer_status_change", AsyncMock()),
        patch("src.server.app.computers.update_computer_status", AsyncMock()) as update,
    ):
        response = await client.post(f"/api/v1/computers/{COMPUTER_ID}/start?lazy=true")
        await _drain_start_tasks()
    assert response.status_code == 429
    assert response.json()["detail"] == "capacity exhausted"
    update.assert_not_awaited()
