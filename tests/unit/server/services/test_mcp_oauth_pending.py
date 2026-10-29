"""A sign-in that saves its server: the pending payload and its callback.

The contract pinned here is the order a pending callback runs in and what it
leaves behind: the payload is spent with the state whatever happens; the save
is refused before the code is spent and again after; the server is checked
with the new token before anything is written; the row is written around the
connection and taken back when the connection does not land.

The connect seams come from the two-phase connect suite, so a pending flow is
driven through the same fakes an ordinary connect is.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException, Response
from pydantic import BaseModel

from src.server.app import mcp_oauth as routes
from src.server.database import mcp_oauth_pending as db_pending
from src.server.database.mcp_oauth_pending import PendingRow, state_digest
from src.server.models.mcp_server import McpServerInput
from src.server.services import mcp_saves as S
from src.server.services.mcp_oauth import connect
from src.server.services.mcp_oauth import finish as F
from src.server.services.mcp_oauth import redirects
from src.server.services.mcp_oauth.connect import (
    STATE_TTL_SECONDS,
    McpOAuthError,
    StartedConnect,
    complete_callback,
    start_pending_connect,
)
from src.server.services.mcp_oauth.redirects import DEFAULT_RETURN_TO, CallbackError
from src.server.services.mcp_dial import dial_of
from src.server.services.mcp_validation import CheckFailure, Passed
from tests.unit.server.services import test_mcp_oauth_connect as connect_suite
from tests.unit.server.services.test_mcp_oauth_connect import (
    SERVER_NAME,
    SERVER_NAME_Q,
    SERVER_URL,
    USER_ID,
    FakeRedis,
)

# The two-phase suite's seams, as this module's fixtures.
redis = connect_suite.redis
phase1 = connect_suite.phase1
phase2 = connect_suite.phase2


class _Body(BaseModel):
    """A parked save's body; what it holds is the saves' business."""

    note: str = "parked"


def _prepared(**overrides) -> F.PreparedSignIn:
    fields = dict(
        kind=S.PendingKind.SERVER,
        server=SERVER_NAME,
        url=SERVER_URL,
        body=_Body(),
    )
    fields.update(overrides)
    return F.PreparedSignIn(**fields)


def _query(target: str) -> dict[str, str]:
    from urllib.parse import parse_qsl, urlsplit

    return dict(parse_qsl(urlsplit(target).query))


@pytest.fixture
def parked(monkeypatch, redis):
    """The Postgres payload store, in memory, keyed by state like the table."""
    env = SimpleNamespace(rows={}, ttl=None, dropped=[], redis_at_store=None)

    async def _store(state, *, user_id, kind, server_name, payload, ttl_seconds):
        env.redis_at_store = dict(redis.states())
        env.rows[state] = PendingRow(user_id, kind, server_name, payload)
        env.ttl = ttl_seconds

    async def _claim(state, user_id):
        row = env.rows.pop(state, None)
        return row if row is not None and row.user_id == user_id else None

    async def _drop(state):
        env.dropped.append(state)
        env.rows.pop(state, None)

    monkeypatch.setattr(F, "store_pending", _store)
    monkeypatch.setattr(F, "claim_pending", _claim)
    monkeypatch.setattr(F, "drop_pending", _drop)
    return env


class FakeFinish:
    """A parked save that records the order the callback drives it in."""

    def __init__(self, events: list[str], phase2, *, plugin: str | None = None):
        self.events = events
        self.phase2 = phase2
        self.plugin = plugin
        self.before_error: F.CallbackFailed | None = None
        self.after_error: F.CallbackFailed | None = None
        self.upserts_inside_write = None

    async def before_exchange(self):
        self.events.append("before_exchange")
        if self.before_error:
            raise self.before_error

    async def after_exchange(self, bearer):
        self.events.append(f"after_exchange:{bearer}")
        if self.after_error:
            raise self.after_error

    async def write(self, store):
        self.events.append("write")
        await store()
        self.upserts_inside_write = len(self.phase2.upserts)
        self.events.append("written")

    async def after_connect(self):
        self.events.append("after_connect")


@pytest.fixture
def finish(monkeypatch, phase2):
    events: list[str] = []
    fake = FakeFinish(events, phase2)
    opened: list[F.Parked] = []

    async def _claim(state, user_id, server_name):
        opened.append(await F.claim_parked(state, user_id, server_name))
        return fake

    async def _on_request(request):
        events.append("exchange")

    phase2.on_request = _on_request
    monkeypatch.setattr(S, "claim", _claim)
    fake.opened = opened
    return fake


# ---------------------------------------------------------------------------
# Start
# ---------------------------------------------------------------------------


class TestStart:
    @pytest.mark.asyncio
    async def test_payload_is_parked_before_the_state_record(self, redis, phase1, parked):
        started = await start_pending_connect(USER_ID, _prepared())

        # Nothing a callback could claim existed while the payload was missing.
        assert parked.redis_at_store == {}
        assert set(parked.rows) == {started.state}
        row = parked.rows[started.state]
        assert (row.kind, row.server_name) == ("server", SERVER_NAME)
        # One envelope, whatever the kind parks.
        assert row.payload["kind"] == "server"
        assert row.payload["version"] == F.PARKED_VERSION
        assert row.payload["body"] == {"note": "parked"}
        # The payload outlives the state, so a last-second callback finds it.
        assert parked.ttl > STATE_TTL_SECONDS
        record = redis.only_record()
        assert record["pending"] is True
        assert (record["server_name"], record["server_url"]) == (SERVER_NAME, SERVER_URL)

    @pytest.mark.asyncio
    async def test_a_state_collision_takes_its_payload_back(self, monkeypatch, phase1, parked):
        fake = FakeRedis(nx_always_loses=True)
        monkeypatch.setattr(
            "src.utils.cache.redis_cache.get_cache_client",
            lambda: SimpleNamespace(enabled=True, client=fake),
        )

        with pytest.raises(McpOAuthError):
            await start_pending_connect(USER_ID, _prepared())

        assert parked.rows == {}

    @pytest.mark.asyncio
    async def test_a_newer_sign_in_retires_the_older_payload(self, redis, phase1, parked):
        first = await start_pending_connect(USER_ID, _prepared())
        second = await start_pending_connect(USER_ID, _prepared())

        assert set(parked.rows) == {second.state}
        assert parked.dropped == [first.state]

    @pytest.mark.asyncio
    async def test_retiring_a_reconnect_touches_no_payload(self, redis, phase1, parked):
        await connect.start_connect(USER_ID, SERVER_NAME)
        await start_pending_connect(USER_ID, _prepared())

        assert parked.dropped == []


# ---------------------------------------------------------------------------
# Callback
# ---------------------------------------------------------------------------


class TestCallback:
    @pytest.mark.asyncio
    async def test_order_before_exchange_after_write(
        self, redis, phase1, phase2, parked, finish
    ):
        finish.plugin = "demo-plugin"
        started = await start_pending_connect(USER_ID, _prepared())

        target = await complete_callback(
            state=started.state, code="code-1", browser_nonce=started.browser_nonce
        )

        assert finish.events == [
            "before_exchange",
            "exchange",
            "after_exchange:access-fresh",
            "write",
            "written",
            "after_connect",
        ]
        # The connection is stored by the save, inside its write.
        assert finish.upserts_inside_write == 1
        assert phase2.bumps == [USER_ID]
        assert phase2.discoveries == [(USER_ID, SERVER_NAME)]
        assert target == (
            f"{DEFAULT_RETURN_TO}?mcp_connected={SERVER_NAME_Q}&plugin=demo-plugin"
        )
        assert parked.rows == {}

    @pytest.mark.asyncio
    async def test_a_refusal_before_the_exchange_spends_no_code(
        self, redis, phase1, phase2, parked, finish
    ):
        finish.before_error = F.CallbackFailed(
            CallbackError.INSTALL_FAILED, "the name was taken"
        )
        started = await start_pending_connect(USER_ID, _prepared())

        target = await complete_callback(
            state=started.state, code="code-1", browser_nonce=started.browser_nonce
        )

        assert phase2.requests == []
        assert phase2.upserts == []
        assert _query(target) == {
            "mcp_error": "install_failed",
            "server": SERVER_NAME,
            "mcp_detail": "the name was taken",
        }

    @pytest.mark.asyncio
    async def test_a_token_the_server_refuses_writes_nothing(
        self, redis, phase1, phase2, parked, finish
    ):
        finish.after_error = F.CallbackFailed(
            CallbackError.INSTALL_FAILED, "the server did not accept the sign-in"
        )
        started = await start_pending_connect(USER_ID, _prepared())

        target = await complete_callback(
            state=started.state, code="code-1", browser_nonce=started.browser_nonce
        )

        assert "write" not in finish.events
        assert phase2.upserts == []
        assert phase2.bumps == []
        assert _query(target)["mcp_error"] == "install_failed"

    @pytest.mark.asyncio
    async def test_a_missing_payload_reads_as_expired(
        self, redis, phase1, phase2, parked, finish
    ):
        started = await start_pending_connect(USER_ID, _prepared())
        parked.rows.clear()

        target = await complete_callback(
            state=started.state, code="code-1", browser_nonce=started.browser_nonce
        )

        assert target == (
            f"{DEFAULT_RETURN_TO}?mcp_error=invalid_state&server={SERVER_NAME_Q}"
        )
        assert phase2.requests == []

    @pytest.mark.asyncio
    async def test_a_payload_for_another_server_is_not_used(
        self, redis, phase1, phase2, parked, finish
    ):
        started = await start_pending_connect(USER_ID, _prepared())
        row = parked.rows[started.state]
        parked.rows[started.state] = PendingRow(row.user_id, row.kind, "other", row.payload)

        target = await complete_callback(
            state=started.state, code="code-1", browser_nonce=started.browser_nonce
        )

        assert _query(target)["mcp_error"] == "invalid_state"
        assert finish.opened == []

    @pytest.mark.asyncio
    async def test_the_payload_is_spent_even_by_a_forged_callback(
        self, monkeypatch, redis, phase1, phase2, parked, finish
    ):
        # A non-loopback deployment, where the start mints a browser nonce.
        monkeypatch.setattr(redirects, "SERVER_BASE_URL", "https://app.example.com")
        started = await start_pending_connect(USER_ID, _prepared())
        assert started.browser_nonce

        target = await complete_callback(
            state=started.state, code="code-1", browser_nonce="not-the-cookie"
        )

        assert _query(target)["mcp_error"] == "state_mismatch"
        assert parked.rows == {}
        assert phase2.requests == []

    @pytest.mark.asyncio
    async def test_a_second_callback_finds_nothing(
        self, redis, phase1, phase2, parked, finish
    ):
        started = await start_pending_connect(USER_ID, _prepared())
        await complete_callback(
            state=started.state, code="code-1", browser_nonce=started.browser_nonce
        )

        replay = await complete_callback(
            state=started.state, code="code-1", browser_nonce=started.browser_nonce
        )

        assert replay == f"{DEFAULT_RETURN_TO}?mcp_error=invalid_state"
        assert len(phase2.upserts) == 1

    @pytest.mark.asyncio
    async def test_an_expired_state_drops_its_payload(self, redis, phase1, parked):
        started = await start_pending_connect(USER_ID, _prepared())
        redis.store.pop(f"mcp:oauth:state:{started.state}")

        target = await complete_callback(state=started.state, code="code-1")

        assert target == f"{DEFAULT_RETURN_TO}?mcp_error=invalid_state"
        assert parked.rows == {}

    @pytest.mark.asyncio
    async def test_the_detail_is_bounded(self, redis, phase1, phase2, parked, finish):
        finish.before_error = F.CallbackFailed(CallbackError.INSTALL_FAILED, "x " * 400)
        started = await start_pending_connect(USER_ID, _prepared())

        target = await complete_callback(
            state=started.state, code="code-1", browser_nonce=started.browser_nonce
        )

        assert len(_query(target)["mcp_detail"]) <= 200


class TestAfterTheSave:
    """Once the save landed, a step after it failing reports a success: a
    retry would meet its own row as a name already taken."""

    @staticmethod
    async def _boom(*a, **kw):
        raise RuntimeError("down")

    @pytest.mark.asyncio
    @pytest.mark.parametrize("step", ["after_connect", "bump", "discovery"])
    async def test_a_pending_save_reads_as_saved(
        self, monkeypatch, redis, phase1, phase2, parked, finish, step
    ):
        if step == "after_connect":
            monkeypatch.setattr(finish, "after_connect", self._boom)
        elif step == "bump":
            monkeypatch.setattr(connect, "bump_user_workspaces_mcp_version", self._boom)
        else:
            phase2.discovery_error = RuntimeError("down")
        started = await start_pending_connect(USER_ID, _prepared())

        target = await complete_callback(
            state=started.state, code="code-1", browser_nonce=started.browser_nonce
        )

        assert target == f"{DEFAULT_RETURN_TO}?mcp_connected={SERVER_NAME_Q}"
        # None of the steps after it was skipped.
        assert phase2.discoveries == [(USER_ID, SERVER_NAME)]
        assert phase2.applies == [("ws-warm-1", USER_ID)]

    @pytest.mark.asyncio
    async def test_a_reconnect_reads_as_connected(
        self, monkeypatch, redis, phase1, phase2
    ):
        monkeypatch.setattr(connect, "bump_user_workspaces_mcp_version", self._boom)
        started = await connect.start_connect(USER_ID, SERVER_NAME)

        target = await complete_callback(
            state=started.state, code="code-1", browser_nonce=started.browser_nonce
        )

        assert target == f"{DEFAULT_RETURN_TO}?mcp_connected={SERVER_NAME_Q}"
        assert phase2.applies == [("ws-warm-1", USER_ID)]


class TestReconnectSwitchesOn:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(("enabled", "switched"), [(False, True), (True, False)])
    async def test_an_off_row_comes_back_on(
        self, monkeypatch, redis, phase1, phase2, enabled, switched
    ):
        phase1.catalog_row = {**phase1.catalog_row, "enabled": enabled}
        calls = []

        async def _switch_on(user_id, name):
            calls.append((user_id, name))

        monkeypatch.setattr(S, "switch_on", _switch_on)
        started = await connect.start_connect(USER_ID, SERVER_NAME)

        target = await complete_callback(
            state=started.state, code="code-1", browser_nonce=started.browser_nonce
        )

        assert target == f"{DEFAULT_RETURN_TO}?mcp_connected={SERVER_NAME_Q}"
        assert calls == ([(USER_ID, SERVER_NAME)] if switched else [])


# ---------------------------------------------------------------------------
# The saves
# ---------------------------------------------------------------------------


@pytest.fixture
def writes(monkeypatch):
    env = SimpleNamespace(calls=[], store_error=None)

    async def _store():
        env.calls.append("store")
        if env.store_error:
            raise env.store_error

    env.store = _store

    async def _create(user_id, name, **fields):
        env.calls.append(("create", name, fields.get("enabled", "default")))
        return {"name": name, "user_mcp_server_id": ROW_ID}

    async def _delete(user_id, name, **kw):
        env.calls.append(("delete", name, kw.get("row_id")))
        return True

    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def _fence(user_id, names):
        env.calls.append(("fence", tuple(names)))
        yield

    monkeypatch.setattr(S, "create_catalog_server", _create)
    monkeypatch.setattr(S, "delete_catalog_server", _delete)
    monkeypatch.setattr(S, "oauth_fence", _fence)
    return env


ROW_ID = "8d0c3f4e-1b2a-4c5d-9e6f-7a8b9c0d1e2f"


def _server(**overrides) -> McpServerInput:
    fields = {"name": "zz_server", "transport": "http", "url": SERVER_URL}
    fields.update(overrides)
    return McpServerInput(**fields)


class TestSaves:
    @pytest.mark.asyncio
    async def test_a_server_lands_as_the_add_route_lands_it_and_goes_without_its_connection(
        self, writes
    ):
        save = S._AddFinish(S.CatalogAdd(USER_ID, _server()))
        await save.write(writes.store)
        # No ``enabled``: the insert's default, which ``POST /servers`` takes too.
        assert writes.calls == [("create", "zz_server", "default"), "store"]

        writes.calls.clear()
        writes.store_error = RuntimeError("db down")
        with pytest.raises(F.CallbackFailed) as failed:
            await save.write(writes.store)
        assert failed.value.reason is CallbackError.INSTALL_FAILED
        assert writes.calls == [
            ("create", "zz_server", "default"),
            "store",
            ("fence", ("zz_server",)),
            # The row it created, not whatever holds the name by now.
            ("delete", "zz_server", ROW_ID),
        ]

    @pytest.mark.asyncio
    async def test_a_workspace_add_lands_as_the_workspace_add_does(
        self, monkeypatch, writes
    ):
        created, applies = [], []

        async def _create(user_id, workspace_id, name, **fields):
            created.append((workspace_id, name, "enabled" in fields))
            writes.calls.append("create")
            return {"name": name, "user_mcp_server_id": ROW_ID}

        monkeypatch.setattr(S, "create_workspace_catalog_server", _create)
        monkeypatch.setattr(
            S,
            "schedule_proactive_apply",
            lambda workspace_id, user_id: applies.append(workspace_id),
        )
        save = S._AddFinish(S.WorkspaceAdd(USER_ID, _server(), "ws-1"))

        await save.write(writes.store)
        await save.after_connect()

        # The add route's own writer (on here, tombstoned elsewhere), and the
        # apply it schedules for the workspace it was added from.
        assert created == [("ws-1", "zz_server", False)]
        assert writes.calls == ["create", "store"]
        assert applies == ["ws-1"]

    @pytest.mark.asyncio
    async def test_a_brokerage_lands_on(self, monkeypatch, writes):
        created = []

        async def _create(user_id, name, *, enabled):
            created.append((name, enabled))
            return {"name": name, "user_mcp_server_id": ROW_ID}

        monkeypatch.setattr(S, "create_brokerage_row", _create)
        monkeypatch.setattr(
            S, "brokerage_server", lambda name: _server(name=name)
        )
        await S._AddFinish(S.BrokerageAdd(USER_ID, "webull")).write(writes.store)
        assert created == [("webull", True)]
        assert writes.calls == ["store"]

    @pytest.mark.asyncio
    async def test_a_name_taken_at_the_write_is_reported(self, monkeypatch, writes):
        async def _taken(*a, **kw):
            raise ValueError("MCP catalog server 'zz_server' already exists")

        monkeypatch.setattr(S, "create_catalog_server", _taken)
        with pytest.raises(F.CallbackFailed) as failed:
            await S._AddFinish(S.CatalogAdd(USER_ID, _server())).write(writes.store)
        assert "already exists" in failed.value.detail
        assert writes.calls == []

    @pytest.mark.asyncio
    async def test_an_edit_whose_row_moved_is_server_changed(self, monkeypatch):
        prior = dial_of({"transport": "http", "url": "https://old.example/mcp"})

        async def _row(user_id, name, **kw):
            return {"transport": "http", "url": "https://moved.example/mcp"}

        monkeypatch.setattr(S, "get_catalog_server", _row)
        edit = S.ServerEdit(USER_ID, "zz_server", _server())
        with pytest.raises(F.CallbackFailed) as failed:
            await S._EditFinish(edit, prior).before_exchange()
        assert failed.value.reason is CallbackError.SERVER_CHANGED

    @pytest.mark.asyncio
    async def test_a_plugin_is_uninstalled_when_its_connection_does_not_land(
        self, monkeypatch, writes
    ):
        committed = SimpleNamespace(progress=SimpleNamespace(plugin={"name": "zz"}))
        undone = []

        async def _commit(user_id, pending):
            writes.calls.append("commit")
            return committed

        async def _undo(user_id, progress):
            undone.append(progress)

        monkeypatch.setattr(S, "commit_pending_plugin_install", _commit)
        monkeypatch.setattr(S, "undo_pending_plugin_install", _undo)
        pending = SimpleNamespace(
            plugin_name="zz-plugin",
            sign_in=SimpleNamespace(server="zz_server", url=SERVER_URL),
        )
        save = S._PluginFinish(USER_ID, pending)
        writes.store_error = RuntimeError("db down")

        with pytest.raises(F.CallbackFailed):
            await save.write(writes.store)
        assert writes.calls == ["commit", "store"]
        assert undone == [committed.progress]
        assert save.plugin == "zz-plugin"

    @pytest.mark.asyncio
    async def test_a_refused_install_is_reported_with_nothing_to_undo(
        self, monkeypatch, writes
    ):
        async def _commit(user_id, pending):
            raise ValueError("Plugin 'zz-plugin' is already installed; use update")

        monkeypatch.setattr(S, "commit_pending_plugin_install", _commit)
        pending = SimpleNamespace(
            plugin_name="zz-plugin",
            sign_in=SimpleNamespace(server="zz_server", url=SERVER_URL),
        )
        with pytest.raises(F.CallbackFailed) as failed:
            await S._PluginFinish(USER_ID, pending).write(writes.store)
        assert "already installed" in failed.value.detail
        assert writes.calls == []

    @pytest.mark.asyncio
    async def test_the_check_after_the_exchange_sends_the_token_and_nothing_else(
        self, monkeypatch
    ):
        seen = []

        async def _check(user_id, server, *, bearer=None, **kw):
            seen.append((server.headers, bearer))
            return Passed(server.name)

        monkeypatch.setattr(S, "check_server", _check)
        monkeypatch.setattr(S, "refuse_unplaceable", AsyncMock())
        save = S._AddFinish(
            S.CatalogAdd(USER_ID, _server(headers={"X-Api-Key": "${vault:KEY}"}))
        )
        await save.after_exchange("tok")
        assert seen == [({}, "tok")]

        async def _refused(user_id, server, **kw):
            return CheckFailure(
                server=server.name, verdict="credential_rejected", reason="nope",
            )

        monkeypatch.setattr(S, "check_server", _refused)
        with pytest.raises(F.CallbackFailed) as failed:
            await save.after_exchange("tok")
        assert failed.value.detail == "nope"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("kind", "payload"),
        [
            ("server", {"version": 1}),
            ("nope", {"kind": "nope", "version": F.PARKED_VERSION, "body": {}}),
            ("server", {"kind": "server", "version": 99, "body": {}}),
            ("server", {"kind": "server", "version": F.PARKED_VERSION, "body": {}}),
            (
                "brokerage",
                {
                    "kind": "brokerage",
                    "version": F.PARKED_VERSION,
                    "body": {"name": "not-a-broker", "url": SERVER_URL},
                },
            ),
            (
                "plugin",
                {
                    "kind": "plugin",
                    "version": F.PARKED_VERSION,
                    "body": {"plugin_name": "zz-plugin"},
                },
            ),
        ],
    )
    async def test_a_payload_this_build_cannot_read_is_started_again(
        self, monkeypatch, kind, payload
    ):
        async def _claim(state, user_id):
            return PendingRow(user_id, kind, SERVER_NAME, payload)

        monkeypatch.setattr(F, "claim_pending", _claim)
        with pytest.raises(F.CallbackFailed) as failed:
            await S.claim("the-state", USER_ID, SERVER_NAME)
        assert failed.value.reason is CallbackError.INSTALL_FAILED
        assert "start it again" in failed.value.detail


    def test_a_brokerage_moved_since_its_sign_in_started_is_started_again(
        self, monkeypatch
    ):
        # The token is for the address the sign-in started at; a deploy in
        # between that moved the brokerage would save the row somewhere else.
        monkeypatch.setattr(S, "brokerage_server", lambda name: _server(name=name))
        started = S.BrokerageAdd(USER_ID, "webull").prepared()
        parked = S._ParkedBrokerage.model_validate(started.body.model_dump())
        assert isinstance(parked.finish(USER_ID), S._AddFinish)

        monkeypatch.setattr(
            S, "brokerage_server", lambda name: _server(name=name, url=OLD_URL)
        )
        with pytest.raises(F.CallbackFailed) as failed:
            parked.finish(USER_ID)
        assert "start it again" in failed.value.detail


OLD_URL = "https://old.example/mcp"
_AS_BEFORE = object()


@pytest.fixture
def edit(monkeypatch, writes):
    """An edit's seams: the catalog edit, the row read before it and back
    after it, the connection the store overwrites, the switch, and the
    take-back."""
    env = SimpleNamespace(
        result=SimpleNamespace(),
        error=None,
        row={"transport": "http", "url": OLD_URL},
        readback=_AS_BEFORE,
        read_error=None,
        connection=None,
        switched=[],
    )

    async def _edit(user_id, name, fields, *, detach_plugin, **kw):
        writes.calls.append(("edit", fields["url"], detach_plugin))
        if env.error:
            raise env.error
        return env.result

    async def _row(user_id, name, **kw):
        if not any(c[0] == "edit" for c in writes.calls if isinstance(c, tuple)):
            return env.row
        if env.read_error:
            raise env.read_error
        return env.row if env.readback is _AS_BEFORE else env.readback

    async def _connection(user_id, name):
        return env.connection

    async def _enabled(user_id, name, enabled):
        writes.calls.append(("switch", enabled))
        return {"name": name}, None

    async def _disconnect(user_id, name):
        writes.calls.append(("disconnect", name))

    monkeypatch.setattr(S, "apply_catalog_edit", _edit)
    monkeypatch.setattr(S, "get_catalog_server", _row)
    monkeypatch.setattr(S, "get_connection", _connection)
    monkeypatch.setattr(S, "apply_catalog_enabled", _enabled)
    monkeypatch.setattr(S, "disconnect_server", _disconnect)
    edit = S.ServerEdit(USER_ID, "zz_server", _server())
    env.save = S._EditFinish(edit, dial_of(env.row))
    return env


def _connected_at(url: str, status=None) -> SimpleNamespace:
    from src.server.database.mcp_oauth import ConnectionStatus

    return SimpleNamespace(
        server_url=url, status=status or ConnectionStatus.CONNECTED
    )


class TestEditFinish:
    @pytest.mark.asyncio
    async def test_an_edit_lands_after_its_connection_and_keeps_its_switch(
        self, writes, edit
    ):
        await edit.save.write(writes.store)
        await edit.save.after_connect()

        # Off stays off and on stays on: only the edit decides, as without a
        # sign-in (it turns a row detached from a switched-off plugin off).
        assert writes.calls == ["store", ("edit", SERVER_URL, True)]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("connection", "switched_off"),
        [
            # The row stood on a sign-in the store overwrote: on without one,
            # every call fails, so it goes off before the connection is revoked.
            (_connected_at(OLD_URL), True),
            (_connected_at(OLD_URL, status="needs_reauth"), True),
            # It never had one here, so it still answers as it did: left on.
            (None, False),
            (_connected_at(OLD_URL, status="revoked"), False),
            (_connected_at("https://elsewhere.example/mcp"), False),
        ],
    )
    async def test_an_edit_that_fails_before_it_commits_takes_its_connection_back(
        self, writes, edit, connection, switched_off
    ):
        edit.connection = connection
        edit.error = RuntimeError("db down")

        with pytest.raises(F.CallbackFailed) as failed:
            await edit.save.write(writes.store)

        assert failed.value.reason is CallbackError.INSTALL_FAILED
        assert writes.calls == [
            "store",
            ("edit", SERVER_URL, True),
            *([("switch", False)] if switched_off else []),
            ("disconnect", "zz_server"),
        ]

    @pytest.mark.asyncio
    async def test_an_edit_that_fails_after_it_commits_is_a_save(self, writes, edit):
        edit.connection = _connected_at(OLD_URL)
        edit.error = RuntimeError("rediscovery could not be scheduled")
        # The update committed before the raise: the row is at the new address.
        edit.readback = {"transport": "http", "url": SERVER_URL}

        await edit.save.write(writes.store)

        assert writes.calls == ["store", ("edit", SERVER_URL, True)]

    @pytest.mark.asyncio
    async def test_an_edit_whose_row_is_gone_takes_its_connection_back(
        self, writes, edit
    ):
        edit.result = None
        edit.readback = None

        with pytest.raises(F.CallbackFailed):
            await edit.save.write(writes.store)

        assert writes.calls[-1] == ("disconnect", "zz_server")

    @pytest.mark.asyncio
    async def test_a_row_that_cannot_be_read_back_goes_off(self, writes, edit):
        edit.error = RuntimeError("db down")
        edit.read_error = RuntimeError("still down")

        with pytest.raises(F.CallbackFailed):
            await edit.save.write(writes.store)

        assert writes.calls[-2:] == [("switch", False), ("disconnect", "zz_server")]

    @pytest.mark.asyncio
    async def test_an_edit_that_landed_while_the_token_was_checked_is_kept(
        self, writes, edit
    ):
        edit.row = {"transport": "http", "url": "https://moved.example/mcp"}

        with pytest.raises(F.CallbackFailed) as failed:
            await edit.save.write(writes.store)

        assert failed.value.reason is CallbackError.SERVER_CHANGED
        assert writes.calls == []

    @pytest.mark.asyncio
    async def test_a_connection_that_does_not_store_leaves_the_row(
        self, writes, edit
    ):
        writes.store_error = RuntimeError("db down")

        with pytest.raises(F.CallbackFailed):
            await edit.save.write(writes.store)

        assert writes.calls == ["store"]


# ---------------------------------------------------------------------------
# Start-side refusals
# ---------------------------------------------------------------------------


class TestRequireSignIn:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("outcome", "code"),
        [
            (Passed("zz_server"), "mcp_sign_in_not_needed"),
            (CheckFailure(server="zz_server", verdict="unreachable", reason="down"),
             "mcp_check_failed"),
        ],
    )
    async def test_only_a_server_that_asks_for_one_signs_in(
        self, monkeypatch, outcome, code
    ):
        async def _check(*a, **kw):
            return outcome

        monkeypatch.setattr(S, "check_server", _check)
        with pytest.raises(HTTPException) as refused:
            await S._require_sign_in(USER_ID, _server())
        assert refused.value.status_code == 422
        assert refused.value.detail["code"] == code

    @pytest.mark.asyncio
    async def test_a_command_server_is_never_dialled(self, monkeypatch):
        async def _check(*a, **kw):
            raise AssertionError("a command server must not be started for this")

        monkeypatch.setattr(S, "check_server", _check)
        with pytest.raises(HTTPException) as refused:
            await S._require_sign_in(
                USER_ID, McpServerInput(name="zz_cmd", transport="stdio", command="npx")
            )
        assert refused.value.detail["code"] == "mcp_sign_in_not_needed"


# ---------------------------------------------------------------------------
# Route
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_start_route_names_the_server_the_callback_will_report(monkeypatch):
    async def _prepare(self, user_id):
        return _prepared(kind=S.PendingKind.PLUGIN, server="zz_server", plugin="zz-plugin")

    async def _start(user_id, prepared, **kw):
        return StartedConnect(
            authorize_url="https://as.test/authorize?state=s1",
            state="s1",
            browser_nonce="n1",
            redirect_uri="https://api.example.com/api/v1/mcp/oauth/callback",
        )

    monkeypatch.setattr(routes.PluginStart, "prepare", _prepare)
    monkeypatch.setattr(routes, "start_pending_connect", _start)
    response = Response()
    body = routes.PluginStart(kind="plugin", source_url="https://example.com/p")

    answer = await routes.oauth_start_pending(
        "user-1", SimpleNamespace(headers={}), response, body
    )

    assert answer == {
        "authorize_url": "https://as.test/authorize?state=s1",
        "state": "s1",
        "redirect_uri": "https://api.example.com/api/v1/mcp/oauth/callback",
        "expires_in": STATE_TTL_SECONDS,
        "server": "zz_server",
        "kind": "plugin",
        "plugin": "zz-plugin",
    }
    cookies = [v.decode() for k, v in response.raw_headers if k == b"set-cookie"]
    assert cookies and cookies[0].startswith("mcp_oauth_cb_s1=n1")
    # The nonce stays cookie-only.
    assert "n1" not in json.dumps(answer)


def test_the_table_is_keyed_by_a_digest_of_the_state():
    digest = state_digest("the-state")
    assert digest != b"the-state" and len(digest) == 32


@pytest.mark.asyncio
async def test_sweep_once_drops_expired_pending_and_survives_its_failure(monkeypatch):
    from src.server.database import mcp_oauth as db_oauth
    from src.server.services.mcp_oauth.sweep import McpOAuthRefreshSweeper

    calls = {"delete": 0, "list": 0}

    async def _list_due(**_kw):
        calls["list"] += 1
        return []

    monkeypatch.setattr(db_oauth, "list_due_refresh", _list_due)

    async def _delete_ok():
        calls["delete"] += 1
        return 2

    monkeypatch.setattr(db_pending, "delete_expired_pending", _delete_ok)
    assert await McpOAuthRefreshSweeper().sweep_once() == 0

    async def _delete_boom():
        calls["delete"] += 1
        raise RuntimeError("db down")

    monkeypatch.setattr(db_pending, "delete_expired_pending", _delete_boom)
    assert await McpOAuthRefreshSweeper().sweep_once() == 0

    assert calls == {"delete": 2, "list": 2}
