"""The activity export: only a service caller with no user reads it, oldest
first, resuming from the cursor it was last handed."""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch
from uuid import UUID

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.server.app import activity as route
from src.server.app.activity import router
from src.server.database import activity_events

TOKEN = "service-secret"
T0 = datetime(2026, 11, 1, tzinfo=timezone.utc)


def _row(i: int) -> dict:
    return {
        "event_id": UUID(int=i),
        "occurred_at": T0 + timedelta(seconds=i),
        "org_id": "org-1",
        "actor_id": "u1",
        "auth": "session",
        "client_id": None,
        "action": "sharing.thread_link_on",
        "outcome": "done",
        "resource_kind": "thread",
        "resource_id": "t1",
        "workspace_id": None,
        "thread_id": UUID(int=99),
        "detail": {"allow_files": True},
        "trace_id": "trace-1",
    }


@pytest.fixture
def client():
    app = FastAPI()
    app.include_router(router)
    with patch("src.server.utils.api._SERVICE_TOKEN", TOKEN):
        yield TestClient(app)


@pytest.fixture
def listed(monkeypatch) -> AsyncMock:
    mock = AsyncMock(return_value=([_row(1), _row(2)], "next", True))
    monkeypatch.setattr(route, "list_for_org", mock)
    return mock


@pytest.mark.parametrize(
    "headers",
    [
        {},
        {"Authorization": "Bearer a-session"},
        {"X-Service-Token": TOKEN, "X-User-Id": "u1"},
    ],
    ids=["nobody", "a-user", "service-for-a-user"],
)
def test_only_a_service_caller_with_no_user_is_answered(client, listed, headers):
    response = client.get("/api/v1/activity?org_id=org-1", headers=headers)
    assert response.status_code == 401
    listed.assert_not_awaited()


def test_a_wrong_service_token_is_refused(client, listed):
    response = client.get("/api/v1/activity?org_id=org-1", headers={"X-Service-Token": "no"})
    assert response.status_code == 401
    listed.assert_not_awaited()


def test_a_page_is_answered_with_its_cursor(client, listed):
    response = client.get(
        "/api/v1/activity?org_id=org-1&cursor=abc&limit=2",
        headers={"X-Service-Token": TOKEN},
    )
    assert response.status_code == 200
    body = response.json()
    assert [i["event_id"] for i in body["items"]] == [str(UUID(int=1)), str(UUID(int=2))]
    assert body["items"][0]["thread_id"] == str(UUID(int=99))
    assert body["items"][0]["detail"] == {"allow_files": True}
    assert (body["next_cursor"], body["has_more"]) == ("next", True)
    listed.assert_awaited_once_with("org-1", cursor="abc", limit=2)


def test_org_id_is_required(client, listed):
    response = client.get("/api/v1/activity", headers={"X-Service-Token": TOKEN})
    assert response.status_code == 422


def test_a_bad_cursor_is_400(client):
    response = client.get(
        "/api/v1/activity?org_id=org-1&cursor=not-a-cursor",
        headers={"X-Service-Token": TOKEN},
    )
    assert response.status_code == 400


def test_the_export_does_not_ask_the_teams_flag(client, listed):
    """The rows are the organization's record: turning the flag off stops new
    ones, never the export of the old."""
    with patch(
        "src.server.services.membership.is_feature_enabled_system", lambda key: False
    ):
        response = client.get("/api/v1/activity?org_id=org-1", headers={"X-Service-Token": TOKEN})
    assert response.status_code == 200


# --- the ledger's paging ------------------------------------------------------


class _Db:
    """The ledger's query, run over a list: the settle horizon against
    ``now``, the keyset, the order and the limit."""

    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows
        self.now = T0 + timedelta(days=1)
        self.sql = ""
        self.params: list = []

    @asynccontextmanager
    async def cursor(self, **_):
        yield self

    async def execute(self, sql, params):
        self.sql, self.params = " ".join(sql.split()), params

    async def fetchall(self):
        settle = self.params[1]
        keyset = self.params[2:4] if len(self.params) == 5 else None
        rows = sorted(self.rows, key=lambda r: (r["occurred_at"], r["event_id"]))
        rows = [r for r in rows if r["occurred_at"] < self.now - timedelta(seconds=settle)]
        if keyset:
            rows = [r for r in rows if (r["occurred_at"], str(r["event_id"])) > tuple(keyset)]
        return rows[: self.params[-1]]


@pytest.fixture
def db(monkeypatch):
    found = _Db([_row(i) for i in (3, 1, 5, 2, 4)])

    @asynccontextmanager
    async def _connection():
        yield found

    monkeypatch.setattr(activity_events, "get_db_connection", _connection)
    return found


@pytest.mark.asyncio
async def test_pages_run_oldest_first_and_resume_from_the_cursor(db):
    rows, cursor, more = await activity_events.list_for_org("org-1", limit=2)
    assert [r["event_id"].int for r in rows] == [1, 2] and more
    assert "ORDER BY occurred_at, event_id" in db.sql
    rows, cursor, more = await activity_events.list_for_org("org-1", cursor=cursor, limit=2)
    assert "(occurred_at, event_id) > (%s, %s)" in db.sql
    assert [r["event_id"].int for r in rows] == [3, 4] and more
    rows, cursor, more = await activity_events.list_for_org("org-1", cursor=cursor, limit=2)
    assert [r["event_id"].int for r in rows] == [5] and not more
    # At the end for now: the cursor is kept so the next pull finds what follows.
    rows, again, more = await activity_events.list_for_org("org-1", cursor=cursor, limit=2)
    assert rows == [] and again == cursor and not more


@pytest.mark.asyncio
async def test_rows_stamped_at_one_instant_page_by_event_id(db):
    db.rows = [{**_row(i), "occurred_at": T0} for i in (3, 1, 2)]
    seen: list[int] = []
    cursor = None
    while True:
        rows, cursor, more = await activity_events.list_for_org("org-1", cursor=cursor, limit=1)
        seen += [r["event_id"].int for r in rows]
        if not more:
            break
    assert seen == [1, 2, 3]


@pytest.mark.asyncio
async def test_a_row_waits_out_the_settle_horizon_so_one_committed_late_is_not_skipped(db):
    """A write stamped earlier but committed later than one already visible
    lands behind it; a cursor handed out before both settled never passes it."""
    db.now = T0 + timedelta(seconds=10)
    early = {**_row(1), "occurred_at": T0}
    later = {**_row(2), "occurred_at": T0 + timedelta(seconds=1)}
    db.rows = [later]  # the later write committed first
    rows, cursor, _ = await activity_events.list_for_org("org-1")
    assert rows == [] and cursor is None
    assert "occurred_at < now() - make_interval(secs => %s)" in db.sql
    assert db.params[1] == activity_events.SETTLE_S
    assert activity_events.SETTLE_S > activity_events.RECORD_BOUND_S
    db.rows = [early, later]  # the earlier one commits within its bound
    db.now = T0 + timedelta(seconds=activity_events.SETTLE_S + 2)
    rows, cursor, _ = await activity_events.list_for_org("org-1", cursor=cursor)
    assert [r["event_id"].int for r in rows] == [1, 2]


@pytest.mark.parametrize(
    "cursor", ["%%%", "bm90LWEtY3Vyc29y", activity_events.encode_cursor(
        {"occurred_at": datetime(2026, 11, 1), "event_id": UUID(int=1)}
    )],
    ids=["not-base64", "no-separator", "naive-time"],
)
def test_a_malformed_cursor_raises(cursor):
    with pytest.raises(ValueError):
        activity_events.decode_cursor(cursor)
