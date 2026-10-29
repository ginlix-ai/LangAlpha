"""The egress session binding under multi-worker truth rules.

Three properties matter beyond the happy path. Removal must converge on a worker
that never bound anything: ``Session`` state is process-local, so the decision
to tear down the sandbox credential file comes from the ``sandbox_egress_grants``
table, not from ``session.egress_binding``. The credential file is the ONLY
channel carrying grant ids: resolved server configs are inputs and are never
written back to, so a retired grant cannot linger in a second place. And
grant replacement, union read, and upload stay under one cross-worker lock:
atomic replacement prevents a torn file, while the lock prevents an older
complete union from landing after a newer one.

A fourth property arrived with the computer/workspace split. The credential
file is the MACHINE's: one file per computer, read by every project on it. So
the grant map a turn on one project writes is the machine's whole active set,
read back from the table (``active_relay_grants_for_computer``), and the JWT's
``computer_id`` claim is the caller's argument, never the session's own label,
which names whichever project happened to build the session.
"""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from ptc_agent.config.core import MCPServerConfig
from ptc_agent.core.session import EgressBinding
from src.server.database.egress_grants import (
    GRANT_KIND_OAUTH_MCP,
    GrantRef,
    GrantSync,
)
from src.server.services.egress.session_binding import (
    maybe_remint_egress_jwt,
    RelayBind,
    sync_egress_relay,
)

WS = "33333333-3333-4333-8333-333333333333"
COMPUTER = "44444444-4444-4444-8444-444444444444"
# The machine label a session carries is whichever project built it. It is a
# decoy here on purpose: nothing minted for WS may ever read it.
SESSION_COMPUTER = "55555555-5555-4555-8555-555555555555"
USER = "user-1"
SECRET = "test-relay-secret-0123456789abcdef0123456789abcdef"
VERSION = 7

_SB = "src.server.services.egress.session_binding"
_LOCK_CONN = object()


def _server(name: str, connection_id: str | None):
    # A real config model, so a resolution output written back onto it would
    # raise rather than quietly re-open the second grant channel.
    return MCPServerConfig(
        name=name,
        transport="http",
        url=f"https://vendor.example.test/{name}",
        source="user",
        oauth_connection_id=connection_id,
    )


def _session(binding: EgressBinding | None = None):
    sandbox = SimpleNamespace(
        sandbox_id="sb-1",
        # Confirmed-publication contract: True = the sandbox got the file.
        upload_egress_relay_credentials=AsyncMock(return_value=True),
    )
    return SimpleNamespace(
        sandbox=sandbox,
        computer_id=SESSION_COMPUTER,
        egress_binding=binding,
        config=SimpleNamespace(sandbox=SimpleNamespace(provider="docker")),
    )


def _resolved(*servers, version: int = VERSION):
    # A real ResolvedMCP, because ``grant_scope`` reads the labelled entries
    # rather than the effective server list.
    from src.server.services.mcp_config import Origin, ResolvedMCP, ResolvedServer, State

    return ResolvedMCP(
        entries=tuple(
            ResolvedServer(config=c, origin=Origin.USER, state=State.ACTIVE)
            for c in servers
        ),
        version=version,
    )


def _ref(connection_id: str, server_name: str) -> GrantRef:
    return GrantRef(
        kind=GRANT_KIND_OAUTH_MCP,
        server_name=server_name,
        connection_id=connection_id,
    )


def _synced(grants: dict[str, str] | None = None, retired: int = 0):
    # Keyed the way the sync answers: (kind, subject), OAuth here throughout.
    return AsyncMock(
        return_value=GrantSync(
            grants={
                (GRANT_KIND_OAUTH_MCP, subject): grant_id
                for subject, grant_id in (grants or {}).items()
            },
            retired=retired,
        )
    )


def _table(grants: dict[str, str] | None = None):
    # The machine's active map as the table answers it: server name -> grant
    # id, for every project on the computer, not only the one syncing.
    return AsyncMock(return_value=dict(grants or {}))


@pytest.fixture(autouse=True)
def publication_lock():
    @asynccontextmanager
    async def _locked(user_id: str):
        assert user_id == USER
        yield _LOCK_CONN

    with patch(f"{_SB}.user_egress_state_lock", new=_locked):
        yield


@contextmanager
def _db(sync, table=None):
    """Both table touches the resolve path makes, so a test that forgets the
    read-back cannot reach a real pool."""
    with (
        patch(f"{_SB}.sync_egress_grants", sync),
        patch(f"{_SB}.active_relay_grants_for_computer", table or _table()),
    ):
        yield


@pytest.fixture
def secret():
    with patch(f"{_SB}.EGRESS_RELAY_SECRET", SECRET):
        yield


@pytest.fixture
def relay_base():
    with (
        patch(
            "src.server.services.egress.reachability.effective_relay_base_url",
            return_value="https://relay.example.test/",
        ),
        patch(
            "src.server.services.egress.reachability.relay_reachability_warning",
            return_value=None,
        ),
    ):
        yield


# ---------------------------------------------------------------------------
# Removal converges from the table, not from process-local session state.
# ---------------------------------------------------------------------------


class TestTeardown:
    @pytest.mark.asyncio
    async def test_fresh_worker_still_tears_down_when_the_table_has_grants(self):
        # Worker B: brand-new session (binding None), but the table says this
        # workspace had active grants; the credential file must still go.
        session = _session(binding=None)
        sync = _synced(retired=2)
        with _db(sync):
            await sync_egress_relay(WS, COMPUTER, USER, session, _resolved())

        sync.assert_awaited_once_with(
            user_id=USER, workspace_id=WS, refs=[],
            config_version=VERSION,
            conn=_LOCK_CONN,
        )
        session.sandbox.upload_egress_relay_credentials.assert_awaited_once_with(None)
        assert session.egress_binding is None

    @pytest.mark.asyncio
    async def test_no_grants_anywhere_uploads_nothing(self):
        # The common case (workspace never had OAuth servers): one indexed
        # no-op UPDATE, zero sandbox I/O.
        session = _session(binding=None)
        with _db(_synced()):
            await sync_egress_relay(WS, COMPUTER, USER, session, _resolved())

        session.sandbox.upload_egress_relay_credentials.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_local_binding_is_cleared_even_when_the_table_is_already_clean(self):
        # Worker A raced: another worker retired the rows first, and this
        # process's file copy and binding still converge.
        binding = EgressBinding(grants={"srv": "g1"}, jwt_exp=9e9, user_id=USER)
        session = _session(binding=binding)
        with _db(_synced()):
            await sync_egress_relay(WS, COMPUTER, USER, session, _resolved())

        session.sandbox.upload_egress_relay_credentials.assert_awaited_once_with(None)
        assert session.egress_binding is None

    @pytest.mark.asyncio
    async def test_a_removal_the_sandbox_refused_keeps_the_binding(self):
        # Same confirmed-publication rule as the add path: a file that is still
        # there must not be recorded as gone, or nothing ever retries it.
        binding = EgressBinding(grants={"srv": "g1"}, jwt_exp=9e9, user_id=USER)
        session = _session(binding=binding)
        session.sandbox.upload_egress_relay_credentials = AsyncMock(return_value=False)
        with _db(_synced()):
            ok = await sync_egress_relay(WS, COMPUTER, USER, session, _resolved())

        assert ok is RelayBind.REFUSED
        assert session.egress_binding is binding

    @pytest.mark.asyncio
    async def test_this_projects_last_grant_leaving_keeps_a_siblings_file(
        self, secret, relay_base
    ):
        """One file per machine: this project resolving to nothing retires its
        own share, but a sibling project on the same computer still holds a
        grant, so the file is rewritten with the sibling's map, not deleted."""
        session = _session(binding=None)
        table = _table({"sibling_srv": "grant-sib"})
        with _db(_synced(retired=1), table):
            ok = await sync_egress_relay(WS, COMPUTER, USER, session, _resolved())

        assert ok is RelayBind.APPLIED
        table.assert_awaited_once_with(COMPUTER, user_id=USER, conn=_LOCK_CONN)
        payload = session.sandbox.upload_egress_relay_credentials.await_args.args[0]
        assert payload is not None
        assert payload["grants"] == {"sibling_srv": "grant-sib"}
        assert session.egress_binding.grants == {"sibling_srv": "grant-sib"}


# ---------------------------------------------------------------------------
# Unknown owner: the whole-set replacement must not run without an identity.
# ---------------------------------------------------------------------------


class TestUnknownOwner:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("owner", [None, ""])
    async def test_an_ownerless_resolve_never_reaches_the_grant_table(self, owner):
        """A caller that lost the workspace owner resolves zero OAuth servers,
        indistinguishable on the wire from a genuine removal, and the sync
        retires the whole active set. It must stop before the table."""
        session = _session(binding=None)
        sync, table = _synced(retired=2), _table()
        with _db(sync, table):
            await sync_egress_relay(WS, COMPUTER, owner, session, _resolved())

        sync.assert_not_awaited()
        table.assert_not_awaited()
        session.sandbox.upload_egress_relay_credentials.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_an_ownerless_resolve_with_oauth_servers_binds_nothing(
        self, secret, relay_base
    ):
        session = _session(binding=None)
        sync = _synced({"conn-1": "g-1"})
        with _db(sync, _table({"srv": "g-1"})):
            await sync_egress_relay(
                WS, COMPUTER, None, session, _resolved(_server("srv", "conn-1"))
            )

        sync.assert_not_awaited()
        session.sandbox.upload_egress_relay_credentials.assert_not_awaited()


# ---------------------------------------------------------------------------
# Binding
# ---------------------------------------------------------------------------


class TestBind:
    @pytest.mark.asyncio
    async def test_sibling_publications_cannot_stale_overwrite_the_machine_union(
        self, secret, relay_base
    ):
        gate = asyncio.Lock()
        first_upload_started = asyncio.Event()
        release_first_upload = asyncio.Event()
        grants: dict[str, str] = {}
        published: list[dict[str, str]] = []

        @asynccontextmanager
        async def serialized(_user_id: str):
            async with gate:
                yield _LOCK_CONN

        async def sync_side_effect(*, workspace_id: str, **_kwargs):
            grants[workspace_id] = f"grant-{workspace_id}"
            return GrantSync(grants={}, retired=0)

        async def table_side_effect(*_args, **_kwargs):
            return dict(grants)

        async def upload(payload):
            published.append(dict(payload["grants"]))
            if len(published) == 1:
                first_upload_started.set()
                await release_first_upload.wait()
            return True

        session_a = _session()
        session_b = _session()
        shared_sandbox = session_a.sandbox
        shared_sandbox.upload_egress_relay_credentials = AsyncMock(side_effect=upload)
        session_b.sandbox = shared_sandbox
        sync = AsyncMock(side_effect=sync_side_effect)
        table = AsyncMock(side_effect=table_side_effect)

        with patch(f"{_SB}.user_egress_state_lock", new=serialized), _db(sync, table):
            first = asyncio.create_task(
                sync_egress_relay("workspace-a", COMPUTER, USER, session_a, _resolved())
            )
            await first_upload_started.wait()
            second = asyncio.create_task(
                sync_egress_relay("workspace-b", COMPUTER, USER, session_b, _resolved())
            )
            await asyncio.sleep(0)
            assert sync.await_count == 1
            release_first_upload.set()
            await asyncio.gather(first, second)

        assert published == [
            {"workspace-a": "grant-workspace-a"},
            {
                "workspace-a": "grant-workspace-a",
                "workspace-b": "grant-workspace-b",
            },
        ]

    @pytest.mark.asyncio
    async def test_binds_grants_in_one_transaction_and_records_the_minted_expiry(
        self, secret, relay_base
    ):
        a, b = _server("srv_a", "conn-a"), _server("srv_b", "conn-b")
        session = _session()
        sync = _synced({"conn-a": "grant-a", "conn-b": "grant-b"})
        with _db(sync, _table({"srv_a": "grant-a", "srv_b": "grant-b"})):
            await sync_egress_relay(WS, COMPUTER, USER, session, _resolved(a, b))

        # Upsert AND retirement are one call: a workspace is never left with a
        # committed grant set that the retirement half hasn't caught up to.
        sync.assert_awaited_once_with(
            user_id=USER, workspace_id=WS,
            refs=[_ref("conn-a", "srv_a"), _ref("conn-b", "srv_b")],
            config_version=VERSION,
            conn=_LOCK_CONN,
        )

        payload = session.sandbox.upload_egress_relay_credentials.await_args.args[0]
        assert payload["grants"] == {"srv_a": "grant-a", "srv_b": "grant-b"}
        assert payload["relay_base_url"] == "https://relay.example.test"
        assert payload["token"]

        binding = session.egress_binding
        assert binding.grants == {"srv_a": "grant-a", "srv_b": "grant-b"}
        assert binding.user_id == USER
        # The expiry comes from the mint itself, not a recompute.
        from src.server.services.egress.relay_jwt import validate_relay_jwt

        assert binding.jwt_exp == validate_relay_jwt(SECRET, payload["token"]).expires_at

    @pytest.mark.asyncio
    async def test_the_file_carries_the_machines_union_not_this_projects_share(
        self, secret, relay_base
    ):
        """A sibling project on the same computer holds ``sibling_srv``. This
        project's sync answers only its own share, yet the file it writes is
        the one every project on the machine reads, so the sibling's grant must
        appear in it or the sibling's server goes unbound mid-turn."""
        session = _session()
        sync = _synced({"conn-a": "grant-a"})
        table = _table({"srv_a": "grant-a", "sibling_srv": "grant-sib"})
        with _db(sync, table):
            await sync_egress_relay(
                WS, COMPUTER, USER, session, _resolved(_server("srv_a", "conn-a"))
            )

        # Read back for THIS machine, not derived from the sync's answer.
        table.assert_awaited_once_with(COMPUTER, user_id=USER, conn=_LOCK_CONN)
        payload = session.sandbox.upload_egress_relay_credentials.await_args.args[0]
        assert payload["grants"] == {"srv_a": "grant-a", "sibling_srv": "grant-sib"}
        assert "sibling_srv" in payload["grants"]
        assert session.egress_binding.grants == payload["grants"]

    @pytest.mark.asyncio
    async def test_the_jwt_names_the_callers_machine_not_the_sessions_label(
        self, secret, relay_base
    ):
        """One session serves every project on its machine, and its own
        ``computer_id`` is whichever project built it. The relay JWT's machine
        claim therefore comes from the caller's binding, never the session."""
        from src.server.services.egress.relay_jwt import validate_relay_jwt

        session = _session()
        assert session.computer_id != COMPUTER  # the decoy must differ to bite
        with _db(_synced({"conn-1": "g-1"}), _table({"srv": "g-1"})):
            await sync_egress_relay(
                WS, COMPUTER, USER, session, _resolved(_server("srv", "conn-1"))
            )

        payload = session.sandbox.upload_egress_relay_credentials.await_args.args[0]
        claims = validate_relay_jwt(SECRET, payload["token"])
        assert claims.computer_id == COMPUTER
        assert claims.computer_id != SESSION_COMPUTER
        assert claims.workspace_id == WS
        assert claims.user_id == USER
        assert claims.sandbox_id == "sb-1"

    @pytest.mark.asyncio
    async def test_the_resolved_configs_are_never_annotated(self, secret, relay_base):
        """The credential file is the only channel: nothing about the grant is
        written back onto the resolution output the codegen path reads."""
        srv = _server("srv", "conn-1")
        before = srv.model_dump()
        session = _session()
        with _db(_synced({"conn-1": "g-1"}), _table({"srv": "g-1"})):
            await sync_egress_relay(WS, COMPUTER, USER, session, _resolved(srv))

        assert srv.model_dump() == before

    @pytest.mark.asyncio
    async def test_vanished_connection_leaves_only_that_server_unbound(
        self, secret, relay_base
    ):
        # The sync could not grant ``gone`` (its connection vanished), so the
        # table never held a row for it; the read-back carries only ``alive``.
        gone, alive = _server("gone", "conn-gone"), _server("alive", "conn-ok")
        session = _session()
        with _db(_synced({"conn-ok": "grant-ok"}), _table({"alive": "grant-ok"})):
            await sync_egress_relay(WS, COMPUTER, USER, session, _resolved(gone, alive))

        payload = session.sandbox.upload_egress_relay_credentials.await_args.args[0]
        assert payload["grants"] == {"alive": "grant-ok"}

    @pytest.mark.asyncio
    async def test_push_uses_the_same_locked_connection_for_sync_and_union(
        self, secret, relay_base
    ):
        srv = _server("srv", "conn-1")
        session = _session()
        sync = _synced({"conn-1": "g-1"})
        table = _table({"srv": "g-1"})
        with _db(sync, table):
            ok = await sync_egress_relay(WS, COMPUTER, USER, session, _resolved(srv))

        assert ok is RelayBind.APPLIED
        assert sync.await_args.kwargs["conn"] is _LOCK_CONN
        assert table.await_args.kwargs["conn"] is _LOCK_CONN
        session.sandbox.upload_egress_relay_credentials.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_failed_publish_leaves_the_binding_unbound(
        self, secret, relay_base
    ):
        # A publish that the sandbox didn't confirm must NOT advance the
        # binding, otherwise the process trusts a token the sandbox never got
        # and won't remint until that phantom token nears expiry.
        srv = _server("srv", "conn-1")
        session = _session()
        session.sandbox.upload_egress_relay_credentials = AsyncMock(return_value=False)
        with _db(_synced({"conn-1": "g-1"}), _table({"srv": "g-1"})):
            ok = await sync_egress_relay(WS, COMPUTER, USER, session, _resolved(srv))

        assert ok is RelayBind.REFUSED  # the caller must keep a retry signal
        assert session.egress_binding is None

    @pytest.mark.asyncio
    async def test_an_unconfigured_relay_touches_neither_db_nor_sandbox(
        self, relay_base
    ):
        srv = _server("srv", "conn-1")
        session = _session()
        sync, table = _synced({"conn-1": "g-1"}), _table({"srv": "g-1"})
        with (
            patch(f"{_SB}.EGRESS_RELAY_SECRET", ""),
            _db(sync, table),
        ):
            ok = await sync_egress_relay(WS, COMPUTER, USER, session, _resolved(srv))

        # Settled non-push: re-running would decide the same, so no retry signal.
        assert ok is RelayBind.APPLIED
        sync.assert_not_awaited()
        table.assert_not_awaited()
        session.sandbox.upload_egress_relay_credentials.assert_not_awaited()


# ---------------------------------------------------------------------------
# Superseded resolve (the DB layer's version CAS refused the replacement)
# ---------------------------------------------------------------------------


class TestStaleResolver:
    @pytest.mark.asyncio
    async def test_a_refused_replacement_pushes_no_credentials(
        self, secret, relay_base
    ):
        """None means a newer sync owns the grant set. Pushing anyway would
        overwrite the sandbox's credential file with this worker's stale grant
        map; the file is last-write-wins, so it would stick."""
        srv = _server("srv", "conn-1")
        session = _session()
        table = _table({"srv": "g-1"})
        with _db(AsyncMock(return_value=None), table):
            ok = await sync_egress_relay(WS, COMPUTER, USER, session, _resolved(srv))

        assert ok is RelayBind.SUPERSEDED
        # Even the read-back is skipped: a stale resolve publishes nothing.
        table.assert_not_awaited()
        session.sandbox.upload_egress_relay_credentials.assert_not_awaited()
        assert session.egress_binding is None

    @pytest.mark.asyncio
    async def test_a_refused_teardown_leaves_the_existing_binding_alone(
        self, secret, relay_base
    ):
        """The teardown path is the dangerous one: `retired`/binding state would
        otherwise drive a delete of a file the winning worker just wrote."""
        binding = EgressBinding(grants={"srv": "g1"}, jwt_exp=9e9, user_id=USER)
        session = _session(binding=binding)
        with _db(AsyncMock(return_value=None)):
            ok = await sync_egress_relay(WS, COMPUTER, USER, session, _resolved())

        assert ok is RelayBind.SUPERSEDED
        session.sandbox.upload_egress_relay_credentials.assert_not_awaited()
        assert session.egress_binding is binding

    @pytest.mark.asyncio
    async def test_the_resolved_version_is_what_gets_compared(self, secret, relay_base):
        """Not the session's applied version or a re-read: the CAS is only
        meaningful if it carries the version this exact set was resolved at."""
        srv = _server("srv", "conn-1")
        session = _session()
        sync = _synced({"conn-1": "g-1"})
        with _db(sync, _table({"srv": "g-1"})):
            await sync_egress_relay(
                WS, COMPUTER, USER, session, _resolved(srv, version=42)
            )

        assert sync.await_args.kwargs["config_version"] == 42


# ---------------------------------------------------------------------------
# Remint (warm fast path)
# ---------------------------------------------------------------------------


class TestRemint:
    @pytest.mark.asyncio
    async def test_noop_without_a_binding(self, secret):
        session = _session(binding=None)
        await maybe_remint_egress_jwt(WS, COMPUTER, session)
        session.sandbox.upload_egress_relay_credentials.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_noop_while_the_jwt_is_fresh(self, secret):
        binding = EgressBinding(grants={"s": "g"}, jwt_exp=9e9, user_id=USER)
        session = _session(binding=binding)
        await maybe_remint_egress_jwt(WS, COMPUTER, session)
        session.sandbox.upload_egress_relay_credentials.assert_not_awaited()
        assert session.egress_binding is binding

    @pytest.mark.asyncio
    async def test_near_expiry_repushes_with_the_bound_identity(
        self, secret, relay_base
    ):
        from src.server.services.egress.relay_jwt import validate_relay_jwt

        binding = EgressBinding(grants={"s": "g"}, jwt_exp=1.0, user_id=USER)
        session = _session(binding=binding)
        with patch(f"{_SB}.active_relay_grants_for_computer", _table({"s": "g"})):
            await maybe_remint_egress_jwt(WS, COMPUTER, session)

        payload = session.sandbox.upload_egress_relay_credentials.await_args.args[0]
        assert payload["grants"] == {"s": "g"}
        refreshed = session.egress_binding
        assert refreshed is not binding
        assert refreshed.user_id == USER
        assert refreshed.jwt_exp > 1.0
        # The warm path takes no resolve, so the machine can only come from
        # the caller here too; the session's decoy label must not leak in.
        claims = validate_relay_jwt(SECRET, payload["token"])
        assert claims.computer_id == COMPUTER
        assert claims.workspace_id == WS


@pytest.mark.asyncio
async def test_remint_does_not_republish_revoked_cached_grants(secret, relay_base):
    session = _session(binding=EgressBinding(
        grants={'deleted-workspace': 'old'}, jwt_exp=1.0, user_id=USER,
    ))
    with patch(f'{_SB}.active_relay_grants_for_computer', _table({'sibling': 'live'})) as table:
        await maybe_remint_egress_jwt(WS, COMPUTER, session)
    table.assert_awaited_once_with(COMPUTER, user_id=USER, conn=_LOCK_CONN)
    assert session.sandbox.upload_egress_relay_credentials.await_args.args[0]['grants'] == {'sibling': 'live'}
