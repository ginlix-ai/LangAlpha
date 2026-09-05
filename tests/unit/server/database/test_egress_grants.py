"""Ownership and atomicity of a workspace's egress grant set.

A grant is what lets a sandbox spend someone's OAuth credential, so three
contracts matter at this layer. ``connection_id`` is *selected* under the owner
predicate rather than trusted from the caller: another user's connection must
produce no grant at all, indistinguishably from one that does not exist. The
upserts and the retirement of everything else commit together — a grant set
that committed without its retirement half is an authorization overhang the
sandbox can still spend. And because the write is a whole-set *replacement*, it
is fenced by a workspace advisory lock plus a ``mcp_config_version`` CAS: two
workers cannot be merged by row locks (their sets need not overlap), so a
resolver carrying a superseded version must replace nothing at all.
"""

from __future__ import annotations

import json
import re
from contextlib import asynccontextmanager
from unittest.mock import patch

import pytest

from src.server.database.egress_grants import (
    GRANT_KIND_OAUTH_MCP,
    apply_consent_to_active_grants,
    sync_oauth_grants,
)
from src.server.services.writer_guard import advisory_key

OWNER = "user-owner"
INTRUDER = "user-intruder"
CONNECTION_ID = "11111111-1111-4111-8111-111111111111"
OTHER_CONNECTION_ID = "44444444-4444-4444-8444-444444444444"
UNKNOWN_CONNECTION_ID = "33333333-3333-4333-8333-333333333333"
WORKSPACE_ID = "22222222-2222-4222-8222-222222222222"
OTHER_WORKSPACE_ID = "55555555-5555-4555-8555-555555555555"
VERSION = 7


class _Cursor:
    """Models the five things this SQL's correctness rests on: the INSERT rows
    come from a SELECT over the connections table (not from the parameters),
    that SELECT filters on connection status as well as owner, each grant's
    tool policy is joined on from the connection's own stored consent, the
    retirement sweeps every active grant outside the keep list, and the live
    ``mcp_config_version`` is re-read (under the lock) rather than trusted from
    the caller."""

    def __init__(self, connections: dict[str, str]) -> None:
        self._connections = connections  # connection_id -> owning user_id
        # connection_id -> status; absent means the default, connected.
        self.statuses: dict[str, str] = {}
        # connection_id -> the address it points at, which is what decides
        # whether we curate a policy for it; absent means a server we do not.
        # The ADDRESS and not the name: a row is named by its user and may be
        # renamed or repointed, so the vendor's own host is the only identity
        # a policy may be derived from.
        self.server_urls: dict[str, str] = {}
        # connection_id -> stored granted_capabilities; absent means NULL.
        self.capabilities: dict[str, list[str] | None] = {}
        self.grants: dict[tuple, dict] = {}  # (workspace, kind, conn) -> row
        self._rows: list[dict] = []
        self._row: dict | None = None
        self.rowcount = 0
        self.depth = 0  # transaction nesting at the time of the last execute
        self.statements: list[tuple[str, tuple, int]] = []
        self.lock_keys: list[int] = []
        # The workspace's live config version; None models a deleted workspace.
        self.version: int | None = VERSION

    async def execute(self, sql: str, params: tuple) -> None:
        self.statements.append((sql, params, self.depth))
        if "pg_advisory_xact_lock" in sql:
            self.lock_keys.append(params[0])
        elif "mcp_config_version" in sql:
            self._row = (
                None if self.version is None else {"mcp_config_version": self.version}
            )
        elif "granted_capabilities" in sql:
            # The policy read, keyed on connection_id under the same owner
            # predicate the INSERT uses.
            wanted, owner = params
            self._rows = [
                {
                    "connection_id": connection_id,
                    "server_url": self.server_urls.get(
                        connection_id, "https://mcp.example.com/own"
                    ),
                    "granted_capabilities": self.capabilities.get(connection_id),
                }
                for connection_id in wanted
                if self._connections.get(connection_id) == owner
            ]
        elif sql.lstrip().startswith("INSERT"):
            # The servable list is unpacked as optional on purpose: the fake
            # filters on status only while the statement actually binds one, so
            # dropping the predicate shows up as a wrong grant set rather than
            # as an unpacking error here.
            (
                _user_id, workspace_id, kind,
                policy_ids, denylists, allowlists, policy_required,
                connection_ids, owner, *rest,
            ) = params
            servable = rest[0] if rest else None
            policy = dict(
                zip(
                    policy_ids,
                    zip(denylists, allowlists, policy_required, strict=True),
                )
            )
            self._rows = []
            for connection_id in connection_ids:
                # The source SELECT: no matching row ⇒ nothing is inserted for
                # that id and ON CONFLICT never fires, so it never RETURNs.
                if self._connections.get(connection_id) != owner:
                    continue
                if (
                    servable is not None
                    and self.statuses.get(connection_id, "connected") not in servable
                ):
                    continue
                row = self.grants.setdefault(
                    (workspace_id, kind, connection_id),
                    {"grant_id": f"grant-for-{connection_id}", "status": "revoked"},
                )
                row["status"] = "active"
                # LEFT JOIN: a connection the policy read did not answer for
                # lands the column defaults, not a skipped row.
                (
                    row["tool_denylist"],
                    row["tool_allowlist"],
                    row["policy_required"],
                ) = policy.get(connection_id, (None, None, False))
                self._rows.append(
                    {"connection_id": connection_id, "grant_id": row["grant_id"]}
                )
        else:
            workspace_id, kind, keep = params
            self._rows = []
            stale = [
                row
                for (ws, k, _c), row in self.grants.items()
                if ws == workspace_id
                and k == kind
                and row["status"] == "active"
                and row["grant_id"] not in keep
            ]
            for row in stale:
                row["status"] = "revoked"
            self.rowcount = len(stale)

    async def fetchall(self) -> list[dict]:
        return self._rows

    async def fetchone(self) -> dict | None:
        return self._row

    def active_grant_ids(self) -> set[str]:
        return {r["grant_id"] for r in self.grants.values() if r["status"] == "active"}

    def grant_writes(self) -> list[tuple[str, tuple, int]]:
        """Only the statements that touch grant rows — the prep reads excluded.

        The policy read is one of those: it reads the connections table to
        decide what each grant may permit, and writes no grant row itself.
        """
        return [
            s
            for s in self.statements
            if "pg_advisory_xact_lock" not in s[0]
            and "mcp_config_version" not in s[0]
            and "granted_capabilities" not in s[0]
        ]


@pytest.fixture
def db():
    """A connections table with two connections, both owned by OWNER."""
    cursor = _Cursor({CONNECTION_ID: OWNER, OTHER_CONNECTION_ID: OWNER})

    @asynccontextmanager
    async def _cursor_cm(**kwargs):
        yield cursor

    @asynccontextmanager
    async def _transaction():
        cursor.depth += 1
        try:
            yield
        finally:
            cursor.depth -= 1

    class _Conn:
        cursor = staticmethod(_cursor_cm)
        transaction = staticmethod(_transaction)

    @asynccontextmanager
    async def _fake_db(conn=None):
        yield conn if conn is not None else _Conn()

    with patch("src.server.database.egress_grants.get_db_connection", new=_fake_db):
        yield cursor


async def _sync(user_id: str, *connection_ids: str, config_version: int = VERSION):
    return await sync_oauth_grants(
        user_id=user_id,
        workspace_id=WORKSPACE_ID,
        connection_ids=list(connection_ids),
        config_version=config_version,
    )


class TestOwnership:
    @pytest.mark.asyncio
    async def test_the_owner_gets_a_grant(self, db):
        synced = await _sync(OWNER, CONNECTION_ID)
        assert synced.grants == {CONNECTION_ID: f"grant-for-{CONNECTION_ID}"}

    @pytest.mark.asyncio
    async def test_another_users_connection_yields_no_grant(self, db):
        """The id is real, but not theirs — it must bind into no workspace."""
        synced = await _sync(INTRUDER, CONNECTION_ID)
        assert synced.grants == {}

    @pytest.mark.asyncio
    async def test_an_unknown_connection_fails_the_same_way(self, db):
        """Same empty answer as the wrong-owner case: guessing ids teaches nothing."""
        synced = await _sync(OWNER, UNKNOWN_CONNECTION_ID)
        assert synced.grants == {}

    @pytest.mark.asyncio
    async def test_one_bad_id_does_not_cost_the_others_their_grants(self, db):
        synced = await _sync(OWNER, UNKNOWN_CONNECTION_ID, CONNECTION_ID)
        assert synced.grants == {CONNECTION_ID: f"grant-for-{CONNECTION_ID}"}

    @pytest.mark.asyncio
    async def test_the_predicate_is_in_the_sql_not_the_caller(self, db):
        """Pinned structurally too — the fake can only model what the SQL says.

        Were the ownership filter to move out of the statement, every arm above
        would still pass against a differently-shaped fake.
        """
        await _sync(OWNER, CONNECTION_ID)
        sql, params, _depth = db.grant_writes()[0]
        flat = re.sub(r"\s+", " ", sql)
        assert "FROM user_mcp_oauth_connections c" in flat
        assert "WHERE c.connection_id = ANY(%s::uuid[]) AND c.user_id = %s" in flat
        # The inserted connection_id AND destination_url both come from the
        # connection row (c.connection_id, c.server_url), never a parameter —
        # a caller can never steer the grant at a host the token wasn't issued
        # for. No destination_url parameter exists to pass.
        assert "SELECT %s, %s::uuid, %s, c.connection_id, c.server_url" in flat
        # Read from the tail: the predicate's parameters are the statement's
        # last three whatever the policy join binds ahead of them.
        assert params[-3:-1] == ([CONNECTION_ID], OWNER)
        assert params[2] == GRANT_KIND_OAUTH_MCP


class TestConnectionStatusFilter:
    """A grant is spendable authority, so it may only be bound to a connection
    whose credential is still servable.

    The upsert's ``DO UPDATE SET status = 'active'`` makes this load-bearing in
    both directions: without the predicate, the next resolve of a workspace
    that still names a revoked connection would *reactivate* the grant the
    disconnect just retired, and a needs_reauth connection would carry an
    active grant the frontend already renders as broken.
    """

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", ["revoked", "needs_reauth"])
    async def test_a_non_servable_connection_gets_no_grant(self, db, status):
        db.statuses[CONNECTION_ID] = status

        synced = await _sync(OWNER, CONNECTION_ID)

        assert synced.grants == {}

    @pytest.mark.asyncio
    async def test_an_ambiguous_connection_still_gets_one(self, db):
        # refresh_ambiguous is servable: the old access token keeps working
        # until it expires, so cutting the grant would break a live sandbox.
        db.statuses[CONNECTION_ID] = "refresh_ambiguous"

        synced = await _sync(OWNER, CONNECTION_ID)

        assert synced.grants == {CONNECTION_ID: f"grant-for-{CONNECTION_ID}"}

    @pytest.mark.asyncio
    async def test_a_revoked_connections_grant_is_retired_not_reactivated(self, db):
        await _sync(OWNER, CONNECTION_ID, OTHER_CONNECTION_ID)
        db.statuses[CONNECTION_ID] = "revoked"

        # The stale catalog still names it, so the resolver still asks for it.
        synced = await _sync(OWNER, CONNECTION_ID, OTHER_CONNECTION_ID)

        assert synced.grants == {OTHER_CONNECTION_ID: f"grant-for-{OTHER_CONNECTION_ID}"}
        assert synced.retired == 1
        assert db.active_grant_ids() == {f"grant-for-{OTHER_CONNECTION_ID}"}

    @pytest.mark.asyncio
    async def test_the_status_predicate_is_in_the_sql_with_the_owner_one(self, db):
        """Structural, like the ownership pin: both predicates guard the same
        SELECT, and the servable vocabulary rides in as a parameter."""
        await _sync(OWNER, CONNECTION_ID)

        sql, params, _depth = db.grant_writes()[0]
        assert "AND c.status = ANY(%s)" in re.sub(r"\s+", " ", sql)
        assert params[-1] == ["connected", "refresh_ambiguous"]


class TestIdempotence:
    @pytest.mark.asyncio
    async def test_re_syncing_returns_the_same_grant(self, db):
        first = await _sync(OWNER, CONNECTION_ID)
        second = await _sync(OWNER, CONNECTION_ID)
        assert first.grants == second.grants
        assert second.retired == 0


class TestRetirement:
    """The retire predicate is what closes the authorization overhang: an
    active grant the resolved set no longer contains must stop being
    spendable, and the upserted set is the only thing that protects a grant."""

    @pytest.mark.asyncio
    async def test_a_dropped_server_loses_its_grant(self, db):
        await _sync(OWNER, CONNECTION_ID, OTHER_CONNECTION_ID)
        synced = await _sync(OWNER, CONNECTION_ID)

        assert synced.retired == 1
        assert db.active_grant_ids() == {f"grant-for-{CONNECTION_ID}"}

    @pytest.mark.asyncio
    async def test_an_empty_set_retires_everything_active(self, db):
        await _sync(OWNER, CONNECTION_ID, OTHER_CONNECTION_ID)
        synced = await _sync(OWNER)

        assert synced.grants == {}
        assert synced.retired == 2
        assert db.active_grant_ids() == set()
        # Nothing to upsert ⇒ only the retirement statement is issued.
        assert len(db.grant_writes()) == 3

    @pytest.mark.asyncio
    async def test_a_connection_that_vanished_loses_its_grant_too(self, db):
        """Its id is still resolved, but it no longer selects a row — the keep
        list is built from what was upserted, never from what was asked for."""
        await _sync(OWNER, CONNECTION_ID)
        db._connections.pop(CONNECTION_ID)
        synced = await _sync(OWNER, CONNECTION_ID)

        assert synced.grants == {}
        assert synced.retired == 1

    @pytest.mark.asyncio
    async def test_upsert_and_retirement_commit_together(self, db):
        await _sync(OWNER, CONNECTION_ID)
        assert [depth for _sql, _params, depth in db.grant_writes()] == [1, 1]

    @pytest.mark.asyncio
    async def test_the_fence_shares_that_transaction(self, db):
        """``pg_advisory_xact_lock`` outside the transaction would release at
        once, and a version read outside it could be overtaken before the
        writes land — both must sit in the same txn as the grant writes."""
        await _sync(OWNER, CONNECTION_ID)
        assert [depth for _sql, _params, depth in db.statements] == [1] * 6


class TestConfigVersionCAS:
    """A whole-set replacement cannot be merged with a concurrent one: two
    workers' sets need not overlap, so Postgres row locks protect nothing. The
    stale worker must therefore be told to stand down entirely — otherwise its
    upsert reactivates a grant the fresh worker just revoked (``ON CONFLICT DO
    UPDATE SET status='active'``) and its retirement revokes the fresh set."""

    @pytest.mark.asyncio
    async def test_a_stale_version_replaces_nothing(self, db):
        await _sync(OWNER, CONNECTION_ID)
        before = db.active_grant_ids()
        db.statements.clear()

        # A fresh worker bumped the config and synced; this resolver still
        # carries the old version.
        db.version = VERSION + 1
        synced = await _sync(OWNER, OTHER_CONNECTION_ID, config_version=VERSION)

        assert synced is None
        # Not "wrote something harmless" — issued no grant statement at all.
        assert db.grant_writes() == []
        assert db.active_grant_ids() == before

    @pytest.mark.asyncio
    async def test_a_matching_version_proceeds(self, db):
        synced = await _sync(OWNER, CONNECTION_ID, config_version=VERSION)
        assert synced is not None
        assert synced.grants == {CONNECTION_ID: f"grant-for-{CONNECTION_ID}"}

    @pytest.mark.asyncio
    async def test_a_deleted_workspace_replaces_nothing(self, db):
        """No row ⇒ no version ⇒ no match, rather than a NULL that compares
        equal to something."""
        db.version = None
        assert await _sync(OWNER, CONNECTION_ID) is None
        assert db.grant_writes() == []

    @pytest.mark.asyncio
    async def test_the_version_is_re_read_under_the_lock(self, db):
        """Order is the whole fix: locking after the read would let a newer
        sync commit in between, and never re-reading would trust the caller's
        stale copy."""
        await _sync(OWNER, CONNECTION_ID)
        kinds = [
            "lock"
            if "pg_advisory_xact_lock" in sql
            else "version"
            if "mcp_config_version" in sql
            else "policy"
            if "granted_capabilities" in sql
            else "write"
            for sql, _params, _depth in db.statements
        ]
        # The policy read sits inside the fence too: reading consent before the
        # lock would let a newer sync's consent land under this one's writes.
        assert kinds == ["lock", "lock", "version", "policy", "write", "write"]

    @pytest.mark.asyncio
    async def test_the_lock_is_workspace_scoped_and_domain_separated(self, db):
        await _sync(OWNER, CONNECTION_ID)
        # The owner's lock first, then the workspace's. A narrowing consent
        # holds the first one over every workspace of the user at once,
        # including the ones being created while it runs; the second is what
        # orders two workers replacing the same workspace's set.
        assert db.lock_keys == [
            advisory_key("EGU", OWNER),
            advisory_key("EG", WORKSPACE_ID),
        ]
        # A different workspace converges concurrently rather than queueing.
        assert advisory_key("EG", WORKSPACE_ID) != advisory_key("EG", CONNECTION_ID)
        # And the tag keeps it off the writer guard's thread/namespace keys,
        # and the two grant domains off each other.
        assert advisory_key("EG", WORKSPACE_ID) != advisory_key("T", WORKSPACE_ID)
        assert advisory_key("EGU", OWNER) != advisory_key("EG", OWNER)


class TestToolPolicy:
    """What each grant is allowed to permit, and where that answer comes from.

    The counterpart of the resolver's identity tests: both halves derive the
    denial from the connection's consented ``server_url``, and if they ever
    disagree a tool is either hidden from the prompt and still callable, or
    offered to the agent and refused at the relay.
    """

    def _policy(self, db, connection_id: str = CONNECTION_ID):
        row = db.grants[(WORKSPACE_ID, GRANT_KIND_OAUTH_MCP, connection_id)]
        denylist = row["tool_denylist"]
        return (
            None if denylist is None else set(json.loads(denylist)),
            row["policy_required"],
        )

    @pytest.mark.asyncio
    async def test_a_server_we_curate_nothing_for_carries_no_policy(self, db):
        """NULL, not empty -- the relay reads it as "no policy", as before."""
        db.server_urls[CONNECTION_ID] = "https://mcp.example.com/own"
        await _sync(OWNER, CONNECTION_ID)
        assert self._policy(db) == (None, False)

    @pytest.mark.asyncio
    async def test_the_vendor_comes_from_the_address_not_the_row_name(self, db):
        """A row named anything, at a broker's host, still carries its policy.

        The name is the user's to choose and to edit, so it decides nothing
        here. The address is what the token was issued for and what the relay
        dials, and it is the only identity the denial is derived from.
        """
        db.server_urls[CONNECTION_ID] = "https://mcp.moomoo.com/mcp"
        db.capabilities[CONNECTION_ID] = ["market_data"]
        await _sync(OWNER, CONNECTION_ID)
        denied, required = self._policy(db)
        assert required is True
        assert "trading_order_place" in denied
        assert "quote_stock_quote" not in denied

    @pytest.mark.asyncio
    async def test_an_address_we_do_not_ship_gets_no_vendors_denial(self, db):
        """The mirror: a reserved name pointed elsewhere is not that vendor.

        Deriving from the name gave this row moomoo's denial -- a list of tool
        names this server does not publish, which refuses nothing and passes
        everything it actually does publish.
        """
        db.server_urls[CONNECTION_ID] = "https://not-moomoo.example.test/mcp"
        db.capabilities[CONNECTION_ID] = ["market_data"]
        await _sync(OWNER, CONNECTION_ID)
        assert self._policy(db) == (None, False)

    @pytest.mark.asyncio
    async def test_both_policy_columns_are_written(self, db):
        """The old column too, because the old code is still serving.

        A blue/green cutover runs both colours at once, and the draining one
        authorizes off ``tool_allowlist``. Writing only the denial left it
        enforcing whatever the last deploy froze there -- so a user who
        declined trading here was still refused by this colour and permitted by
        the other, for as long as the old one had a connection open.
        """
        db.server_urls[CONNECTION_ID] = "https://mcp.moomoo.com/mcp"
        db.capabilities[CONNECTION_ID] = ["market_data"]
        await _sync(OWNER, CONNECTION_ID)
        row = db.grants[(WORKSPACE_ID, GRANT_KIND_OAUTH_MCP, CONNECTION_ID)]
        permitted = set(json.loads(row["tool_allowlist"]))
        assert "quote_stock_quote" in permitted
        assert "trading_order_place" not in permitted

    @pytest.mark.asyncio
    async def test_a_brokerage_with_no_recorded_consent_denies_its_curation(self, db):
        """The one state that must fail closed rather than permissive."""
        db.server_urls[CONNECTION_ID] = "https://mcp.moomoo.com/mcp"
        db.capabilities[CONNECTION_ID] = None
        await _sync(OWNER, CONNECTION_ID)
        denied, required = self._policy(db)
        assert required is True
        assert {"trading_order_place", "quote_stock_quote"} <= denied


class TestApplyConsentToActiveGrants:
    """Narrowing consent has to bite when the user confirms it.

    A reconnect writes the new keys onto the connection, but the relay reads
    the grant. Leaving the grants to a later resolve meant a user who
    reconnected specifically to switch trading *off* kept an agent that could
    place orders until something happened to re-sync -- and indefinitely if
    that failed, since the failure is a log line.
    """

    @staticmethod
    def _db(server_url: str | None, capabilities: list[str] | None):
        statements: list[tuple[str, tuple, int]] = []
        depth = [0]

        class _C:
            rowcount = 1

            def __init__(self):
                self._rows: list[dict] = []
                self._last = ""

            async def execute(self, sql, params=None):
                statements.append((sql, params, depth[0]))
                self._last = sql

            async def fetchall(self):
                return self._rows

            async def fetchone(self):
                # The owner is read first, only to name the lock; the consent is
                # re-read afterwards, under it.
                if "SELECT user_id" in getattr(self, "_last", ""):
                    return {"user_id": OWNER}
                return {
                    "server_url": server_url,
                    "granted_capabilities": capabilities,
                }

        @asynccontextmanager
        async def _cursor_cm(**kwargs):
            yield _C()

        @asynccontextmanager
        async def _transaction():
            depth[0] += 1
            try:
                yield
            finally:
                depth[0] -= 1

        class _Conn:
            cursor = staticmethod(_cursor_cm)
            transaction = staticmethod(_transaction)

        @asynccontextmanager
        async def _fake_db(conn=None):
            yield _Conn()

        return _fake_db, statements

    async def _run(self, server_url, capabilities, **kwargs):
        fake, statements = self._db(server_url, capabilities, **kwargs)
        with patch("src.server.database.egress_grants.get_db_connection", new=fake):
            await apply_consent_to_active_grants(CONNECTION_ID)
        return statements

    async def _apply(self, server_url, capabilities):
        statements = await self._run(server_url, capabilities)
        update = next(s for s in statements if s[0].lstrip().startswith("UPDATE"))
        denylist, allowlist, required, connection_id = update[1]
        assert connection_id == CONNECTION_ID
        assert "status = \'active\'" in update[0]
        # Both columns, for the reason the sync writes both: the other blue/green
        # colour authorizes off the allowlist, and a narrowing that never
        # reached it is a narrowing that half the fleet ignores.
        permitted = None if allowlist is None else set(json.loads(allowlist))
        denied = None if denylist is None else set(json.loads(denylist))
        assert (permitted is None) == (denied is None)
        return denied, required, permitted

    @pytest.mark.asyncio
    async def test_it_bounds_how_long_it_will_wait_for_the_lock(self):
        """This runs in the OAuth callback, behind a lock its own syncs take.

        Unbounded, a wedged grant sync holds the user's connect open for as long
        as it stays wedged. Bounded, Postgres raises inside the transaction,
        which rolls back and releases whatever was taken, and the callback reads
        that like any other failure to settle consent: it revokes the grants
        rather than leaving them carrying a policy nobody confirmed.
        """
        statements = await self._run("https://mcp.moomoo.com/mcp", ["market_data"])
        bound = next(s for s in statements if "lock_timeout" in s[0])
        locks = [s for s in statements if "pg_advisory_xact_lock" in s[0]]
        # Inside the transaction (SET LOCAL is scoped to it) and before the wait
        # it is meant to bound.
        assert bound[2] > 0
        assert statements.index(bound) < statements.index(locks[0])

    @pytest.mark.asyncio
    async def test_it_fences_the_workspaces_that_do_not_exist_yet(self):
        """The sync it races may be in a workspace no query here can name.

        Enumerating the owner's workspaces and locking each fenced only the rows
        that existed when the SELECT ran. A workspace being created alongside
        this connect holds no grant, appears in no enumeration, and its first
        sync is precisely the writer that has read the old consent and not yet
        committed -- so it was free to write the wider policy after the
        narrowing landed, with no version bump left to correct it. One lock over
        the user covers the workspaces and the ones still arriving alike.
        """
        statements = await self._run("https://mcp.moomoo.com/mcp", ["market_data"])
        assert not any("FROM workspaces" in s[0] for s in statements)
        locks = [s for s in statements if "pg_advisory_xact_lock" in s[0]]
        assert [s[1][0] for s in locks] == [advisory_key("EGU", OWNER)]
        # Re-read under the lock, never carried in from the query that named it.
        consent = next(
            s for s in statements if "granted_capabilities" in s[0] and "SELECT" in s[0]
        )
        assert statements.index(consent) > statements.index(locks[-1])

    @pytest.mark.asyncio
    async def test_it_writes_the_consent_the_connection_now_records(self):
        denied, required, permitted = await self._apply(
            "https://mcp.moomoo.com/mcp", ["market_data"]
        )
        assert required is True
        assert "trading_order_place" in denied
        assert "quote_stock_quote" not in denied
        assert "trading_order_place" not in permitted

    @pytest.mark.asyncio
    async def test_widening_clears_what_the_previous_consent_denied(self):
        """Idempotent in both directions, which is what makes it unconditional.

        A grant carrying the old narrower denial would keep refusing the group
        the user just granted, so the same write has to be able to shrink the
        list as well as grow it.
        """
        denied, required, permitted = await self._apply(
            "https://mcp.moomoo.com/mcp",
            ["market_data", "watchlists", "account", "rehearsal", "trading"],
        )
        assert required is True
        assert "trading_order_place" not in denied
        assert "trading_order_place" in permitted

    @pytest.mark.asyncio
    async def test_a_server_we_curate_nothing_for_is_left_with_no_policy(self):
        assert await self._apply("https://mcp.example.com/own", None) == (
            None, False, None,
        )

    @pytest.mark.asyncio
    async def test_it_takes_the_syncs_own_lock_before_it_reads(self):
        """Otherwise the sync it races writes the pre-narrowing policy back.

        ``sync_oauth_grants`` reads the connection's consent inside its
        transaction and writes the grant from it. Interleaved, this update lands
        between those two and the sync's upsert overwrites it -- and no version
        bump can save it, because the sync CASed successfully before any of this
        started. The lock is what makes the two orders the only two possible.
        """
        statements = await self._run("https://mcp.moomoo.com/mcp", ["market_data"])
        locks = [s for s in statements if "pg_advisory_xact_lock" in s[0]]
        assert [s[1][0] for s in locks] == [advisory_key("EGU", OWNER)]
        # Held, not merely taken: a lock released before the write fences
        # nothing, and transaction-scoped is the only way to hold one here.
        update = next(s for s in statements if s[0].lstrip().startswith("UPDATE"))
        assert all(s[2] > 0 for s in locks)
        assert update[2] > 0
        assert statements.index(update) > statements.index(locks[-1])
