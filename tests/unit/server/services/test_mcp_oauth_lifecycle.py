"""Unit tests for the MCP OAuth token lifecycle (refresh single-flight).

The lifecycle's whole job is to hand back a usable access token without ever
letting two workers burn the same one-time refresh token. Four properties
carry that, and each gets its own coverage here:

- the **hot path takes no lock** — with >10 minutes of validity the call is a
  single read, so the common case never touches Postgres' lock manager;
- exactly one **winner** refreshes per cluster (``pg_try_advisory_lock``), and
  it commits under a ``token_generation`` compare-and-swap;
- **losers never block**: a comfortably valid old token is served instantly,
  and only a near-expiry loser briefly polls for the winner's commit;
- an **ambiguous** refresh timeout is terminal for retries — the refresh token
  may already be consumed server-side, so the connection flips to
  ``refresh_ambiguous`` and rides the old access token to expiry.

``disconnect_server`` is the module's other write path, and it gets the same
treatment at the end of the file: its three revocation writes commit as one.

Redis, Postgres and the network are all faked at the module's seams: the
advisory-lock cursor, the connection-row store, and the token endpoint.
"""

from __future__ import annotations

import asyncio
import dataclasses
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import httpx2
import pytest

from src.server.database import mcp_oauth as mcp_oauth_db
from src.server.database.mcp_oauth import (
    SERVABLE,
    BearerBundle,
    ConnectionStatus,
    ConnectionSummary,
    RefreshBundle,
    Secrets,
)
from src.server.services.mcp_oauth import http, lifecycle, tokens
from src.server.services.mcp_oauth.http import OAuthHopBlocked
from src.server.services.mcp_oauth.lifecycle import (
    AccessToken,
    TokenUnavailable,
    ensure_fresh_access_token,
)
from src.server.services.writer_guard import advisory_key
from src.server.utils.egress_guard import EgressBlockedError, PinnedTarget

CONNECTION_ID = "11111111-2222-3333-4444-555555555555"
USER_ID = "user-lifecycle-1"
SERVER_NAME = "demo notes"
SERVER_URL = "https://mcp.demo.test/mcp"
ISSUER = "https://auth.demo.test"
LOCK_KEY = advisory_key("mcp_oauth_refresh", CONNECTION_ID)


def _row(
    *,
    expires_in: float | None = 3600,
    generation: int = 3,
    status: str = "connected",
    access_token: str = "access-old",
    refresh_token: str | None = "refresh-old",
    **overrides,
) -> RefreshBundle:
    """A connection as :func:`get_connection_by_id` hands it over, fully read."""
    now = datetime.now(timezone.utc)
    fields = {
        "connection_id": CONNECTION_ID,
        "user_id": USER_ID,
        "server_name": SERVER_NAME,
        "server_url": SERVER_URL,
        "status": ConnectionStatus(status),
        "token_type": "Bearer",
        "scope": "notes.read offline_access",
        "granted_capabilities": None,
        "expires_at": (
            None if expires_in is None else now + timedelta(seconds=expires_in)
        ),
        "token_generation": generation,
        "client_info": {"client_id": "client-abc123"},
        "as_metadata": {"issuer": ISSUER, "token_endpoint": f"{ISSUER}/token"},
        "resource_metadata": None,
        "has_refresh_token": refresh_token is not None,
        "last_refresh_at": None,
        "created_at": now,
        "updated_at": now,
        "access_token": access_token,
        "refresh_token": refresh_token,
        "client_secret": None,
    }
    fields.update(overrides)
    return RefreshBundle(**fields)


def _project(row: RefreshBundle, secrets: Secrets) -> ConnectionSummary:
    """Mirror the real read: a mode carries only the columns it decrypted.

    Code that reaches for the refresh token or client secret while asking for
    BEARER fails here exactly as it would against Postgres.
    """
    fields = {f.name: getattr(row, f.name) for f in dataclasses.fields(row)}
    if secrets is Secrets.FULL:
        return RefreshBundle(**fields)
    for name in ("refresh_token", "client_secret"):
        fields.pop(name)
    if secrets is Secrets.BEARER:
        return BearerBundle(**fields)
    fields.pop("access_token")
    return ConnectionSummary(**fields)


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class FakeStore:
    """The connection row, plus the two writes the lifecycle may perform.

    ``script`` swaps in a different row on the Nth read, which is how a
    competing worker's commit is made to land mid-flight.
    """

    def __init__(self, row: RefreshBundle | None):
        self.row = row
        self.script: dict[int, RefreshBundle | None] = {}
        self.read_count = 0
        self.marks: list[str] = []
        # Status writes the generation fence refused, and the fences each write
        # carried — a refresh outcome may only land on the bundle it describes.
        self.refused_marks: list[str] = []
        self.mark_generations: list[int] = []
        self.mark_conns: list = []
        # Writes that went through the UNFENCED writer. disconnect_server is its
        # only legitimate caller; a refresh outcome here is the regression.
        self.unfenced_marks: list[str] = []
        # Fires just before a fenced write evaluates its guard — the window a
        # reconnect's upsert lands in.
        self.on_mark = None
        self.commits: list[dict] = []
        self.reads: list[Secrets] = []
        # The connection each write/read was handed — None means it acquired
        # its own from the pool. The refresh winner must thread the held,
        # advisory-locked connection through so it never nests a second acquire.
        self.read_conns: list = []
        self.commit_conns: list = []

    async def get_connection_by_id(self, connection_id, *, secrets=Secrets.NONE, conn=None):
        assert connection_id == CONNECTION_ID
        # The lifecycle always needs at least the bearer; a summary read is a bug.
        assert secrets is not Secrets.NONE
        self.reads.append(secrets)
        self.read_conns.append(conn)
        self.read_count += 1
        if self.read_count in self.script:
            self.row = self.script[self.read_count]
        if self.row is None:
            return None
        return _project(self.row, secrets)

    async def mark_status(self, connection_id, status, *, conn=None):
        self.unfenced_marks.append(status)
        self._apply(status)
        return True

    async def mark_status_if_generation(
        self, connection_id, status, *, expected_generation, conn=None
    ):
        if self.on_mark is not None:
            self.on_mark()
        self.mark_generations.append(expected_generation)
        self.mark_conns.append(conn)
        row = self.row
        if (
            row is None
            or row.token_generation != expected_generation
            or (row.status is ConnectionStatus.REVOKED and status != "revoked")
        ):
            # Both halves of the real UPDATE's WHERE: the generation fence, and
            # mark_status' terminal guard.
            self.refused_marks.append(status)
            return False
        self._apply(status)
        return True

    def _apply(self, status) -> None:
        self.marks.append(status)
        if self.row is not None:
            self.row = dataclasses.replace(self.row, status=ConnectionStatus(status))

    async def commit_refresh(
        self,
        connection_id,
        *,
        expected_generation,
        access_token,
        refresh_token,
        expires_at,
        scope=None,
        conn=None,
    ):
        self.commit_conns.append(conn)
        self.commits.append(
            {
                "connection_id": connection_id,
                "expected_generation": expected_generation,
                "access_token": access_token,
                "refresh_token": refresh_token,
                "expires_at": expires_at,
                "scope": scope,
            }
        )
        if (
            self.row is None
            or self.row.token_generation != expected_generation
            or self.row.status not in SERVABLE
        ):
            # A newer bundle already landed, or the row left the servable set —
            # both halves of the real UPDATE's WHERE.
            return False
        surviving = refresh_token or self.row.refresh_token
        self.row = dataclasses.replace(
            self.row,
            access_token=access_token,
            refresh_token=surviving,
            has_refresh_token=surviving is not None,
            expires_at=expires_at,
            scope=scope or self.row.scope,
            token_generation=expected_generation + 1,
            status=ConnectionStatus.CONNECTED,
        )
        return True


class _FakeCursor:
    def __init__(self, db: "FakeLockDb"):
        self._db = db

    async def __aenter__(self) -> "_FakeCursor":
        return self

    async def __aexit__(self, *exc) -> bool:
        return False

    async def execute(self, sql, params=None):
        self._db.statements.append((" ".join(sql.split()), params))

    async def fetchone(self):
        sql, _ = self._db.statements[-1]
        assert "pg_try_advisory_lock" in sql
        return (self._db.acquired,)


class FakeLockDb:
    """Stands in for the pooled connection the try-lock is taken on."""

    def __init__(self, *, acquired: bool = True):
        self.acquired = acquired
        self.statements: list[tuple[str, tuple | None]] = []
        self.opened = 0
        self.last_conn = None

    @asynccontextmanager
    async def connection(self):
        self.opened += 1
        self.last_conn = SimpleNamespace(cursor=lambda *a, **k: _FakeCursor(self))
        yield self.last_conn

    def _keys(self, fn: str) -> list[int]:
        return [
            params[0]
            for sql, params in self.statements
            if fn in sql and params is not None
        ]

    @property
    def lock_attempts(self) -> list[int]:
        return self._keys("pg_try_advisory_lock")

    @property
    def unlocks(self) -> list[int]:
        return self._keys("pg_advisory_unlock")


class FakeTokenEndpoint:
    def __init__(self):
        self.calls: list[dict] = []
        self.status_code = 200
        self.payload: dict = {
            "access_token": "access-new",
            "token_type": "Bearer",
            "expires_in": 3600,
            "refresh_token": "refresh-new",
            "scope": "notes.read offline_access",
        }
        self.raises: Exception | None = None
        # Fires while the refresh is in flight — the window a rival worker's
        # commit would land in.
        self.on_call = None

    async def request(
        self, client, method, url, *, headers=None, data=None, content=None
    ):
        self.calls.append(
            {"method": method, "url": url, "headers": headers, "data": data}
        )
        if self.on_call is not None:
            self.on_call()
        if self.raises is not None:
            raise self.raises
        return httpx2.Response(self.status_code, json=self.payload)


@asynccontextmanager
async def _fake_http_client():
    # `timeout` is assigned on the client, so it must be a settable attribute.
    yield SimpleNamespace(name="fake-oauth-client", timeout=None)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def store(monkeypatch) -> FakeStore:
    fake = FakeStore(_row())
    monkeypatch.setattr(lifecycle, "get_connection_by_id", fake.get_connection_by_id)
    monkeypatch.setattr(lifecycle, "mark_status", fake.mark_status)
    monkeypatch.setattr(
        lifecycle, "mark_status_if_generation", fake.mark_status_if_generation
    )
    monkeypatch.setattr(lifecycle, "commit_refresh", fake.commit_refresh)
    return fake


@pytest.fixture
def db(monkeypatch) -> FakeLockDb:
    fake = FakeLockDb()
    monkeypatch.setattr(
        "src.server.database.pool.get_db_connection", fake.connection
    )
    return fake


@pytest.fixture
def token_endpoint(monkeypatch) -> FakeTokenEndpoint:
    fake = FakeTokenEndpoint()
    # The token POST lives in mcp_oauth.tokens — the one place both the refresh
    # and the connect-time code exchange go through.
    monkeypatch.setattr(tokens, "pinned_request", fake.request)
    monkeypatch.setattr(tokens, "oauth_http_client", _fake_http_client)
    return fake


@pytest.fixture
def short_poll(monkeypatch):
    """One poll iteration instead of eight — the loop shape, not the wall clock."""
    monkeypatch.setattr(lifecycle, "LOSER_POLL_SECONDS", 0.05)


# ---------------------------------------------------------------------------
# Hot path — no lock, no HTTP
# ---------------------------------------------------------------------------


class TestHotPath:
    @pytest.mark.asyncio
    async def test_comfortable_validity_takes_no_lock(self, store, db, token_endpoint):
        store.row = _row(expires_in=3600)

        token = await ensure_fresh_access_token(CONNECTION_ID)

        assert token == AccessToken(
            access_token="access-old", token_type="Bearer", generation=3
        )
        # The whole point of the margin: one read, and the lock manager is
        # never consulted.
        assert store.read_count == 1
        assert db.opened == 0
        assert db.lock_attempts == []
        assert token_endpoint.calls == []

    @pytest.mark.asyncio
    async def test_the_relayed_call_path_decrypts_the_bearer_only(
        self, store, db, token_endpoint
    ):
        # Every relayed tool call lands here, and each decrypted column re-runs
        # OpenPGP S2K on the DB. The refresh token and client secret are not
        # needed to serve a valid bearer, and the refresh path re-reads the full
        # bundle under the lock anyway — so widening this read back to FULL is a
        # pure regression, and this is the only place that would notice.
        store.row = _row(expires_in=3600)

        await ensure_fresh_access_token(CONNECTION_ID)

        assert store.reads == [Secrets.BEARER]

    @pytest.mark.asyncio
    async def test_no_refresh_token_is_decided_without_decrypting_one(
        self, store, db, token_endpoint
    ):
        # The "can this connection refresh?" question is answered by the
        # column's NOT NULL-ness, so it survives a bearer-only read.
        store.row = _row(expires_in=120, refresh_token=None)

        token = await ensure_fresh_access_token(CONNECTION_ID)

        # Inside the refresh margin, but with nothing to refresh with: ride the
        # old token to expiry rather than attempting a doomed refresh.
        assert token.access_token == "access-old"
        assert store.reads == [Secrets.BEARER]
        assert token_endpoint.calls == []

    @pytest.mark.asyncio
    async def test_a_missing_token_type_is_defaulted_here_not_at_the_caller(
        self, store, db, token_endpoint
    ):
        """Vendors may omit token_type; every holder must still get a header.

        The default belongs on this side of the boundary — an AccessToken that
        can be constructed without a scheme is one every consumer has to
        re-defend against.
        """
        store.row = _row(token_type=None)

        token = await ensure_fresh_access_token(CONNECTION_ID)

        assert token.token_type == "Bearer"
        assert token.header() == "Bearer access-old"

    @pytest.mark.asyncio
    async def test_non_expiring_token_is_never_refreshed(
        self, store, db, token_endpoint
    ):
        store.row = _row(expires_in=None)

        token = await ensure_fresh_access_token(CONNECTION_ID)

        assert token.access_token == "access-old"
        assert db.lock_attempts == []
        assert token_endpoint.calls == []

    @pytest.mark.asyncio
    async def test_a_naive_expiry_is_read_as_utc(self, store, db, token_endpoint):
        # Postgres can hand back a naive timestamp; reading it as local time
        # would misjudge the margin by the UTC offset — enough, in most of the
        # world, to refresh an hour early or serve an expired token.
        naive = (datetime.now(timezone.utc) + timedelta(hours=1)).replace(tzinfo=None)
        store.row = _row(expires_at=naive)

        token = await ensure_fresh_access_token(CONNECTION_ID)

        assert token.access_token == "access-old"
        assert db.lock_attempts == []

    @pytest.mark.asyncio
    async def test_just_inside_the_margin_does_take_the_lock(
        self, store, db, token_endpoint
    ):
        store.row = _row(expires_in=lifecycle.REFRESH_MARGIN_SECONDS - 30)

        await ensure_fresh_access_token(CONNECTION_ID)

        assert db.lock_attempts == [LOCK_KEY]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("status", "reason"), [("revoked", "revoked"), ("needs_reauth", "needs_reauth")]
    )
    async def test_dead_statuses_short_circuit(
        self, store, db, token_endpoint, status, reason
    ):
        store.row = _row(expires_in=30, status=status)

        with pytest.raises(TokenUnavailable) as excinfo:
            await ensure_fresh_access_token(CONNECTION_ID)

        assert excinfo.value.reason == reason
        assert db.lock_attempts == []
        assert token_endpoint.calls == []

    @pytest.mark.asyncio
    async def test_unknown_connection(self, store, db, token_endpoint):
        store.row = None

        with pytest.raises(TokenUnavailable) as excinfo:
            await ensure_fresh_access_token(CONNECTION_ID)

        assert excinfo.value.reason == "unknown_connection"
        assert db.lock_attempts == []

    @pytest.mark.asyncio
    async def test_no_refresh_token_rides_the_access_token_to_expiry(
        self, store, db, token_endpoint
    ):
        store.row = _row(expires_in=120, refresh_token=None)

        token = await ensure_fresh_access_token(CONNECTION_ID)

        assert token.access_token == "access-old"
        assert db.lock_attempts == []
        assert token_endpoint.calls == []
        assert store.marks == []

    @pytest.mark.asyncio
    async def test_no_refresh_token_and_expired_needs_reauth(
        self, store, db, token_endpoint
    ):
        store.row = _row(expires_in=-5, refresh_token=None)

        with pytest.raises(TokenUnavailable) as excinfo:
            await ensure_fresh_access_token(CONNECTION_ID)

        assert excinfo.value.reason == "needs_reauth"
        assert store.marks == ["needs_reauth"]
        assert token_endpoint.calls == []


# ---------------------------------------------------------------------------
# current_access_token — the relay's 401-retry read
# ---------------------------------------------------------------------------


class TestCurrentAccessToken:
    @pytest.mark.asyncio
    async def test_a_revoked_row_hands_out_no_bearer(self, store, db, token_endpoint):
        # The race this pins: entry gate passed → vendor 401 → a concurrent
        # refresh rotated the bundle → the user disconnected → the retry asks
        # for the rotated bearer. Handing it out would send one
        # post-revocation request with a still-vendor-valid token.
        store.row = _row(status="revoked", generation=4)

        assert await lifecycle.current_access_token(CONNECTION_ID) is None
        assert token_endpoint.calls == []

    @pytest.mark.asyncio
    async def test_a_needs_reauth_row_hands_out_no_bearer(
        self, store, db, token_endpoint
    ):
        store.row = _row(status="needs_reauth", generation=4)

        assert await lifecycle.current_access_token(CONNECTION_ID) is None

    @pytest.mark.asyncio
    async def test_an_ambiguous_row_serves_its_bearer_even_expired(
        self, store, db, token_endpoint
    ):
        # Servable and no freshness gate: the caller already holds a vendor
        # 401 in hand — whether THIS bearer is any better is the vendor's
        # call, not a clock check here.
        store.row = _row(status="refresh_ambiguous", expires_in=-5, generation=4)

        token = await lifecycle.current_access_token(CONNECTION_ID)

        assert token is not None
        assert token.access_token == "access-old"
        assert token.generation == 4
        assert token_endpoint.calls == []

    @pytest.mark.asyncio
    async def test_a_missing_bearer_is_none(self, store, db, token_endpoint):
        store.row = _row(access_token="")

        assert await lifecycle.current_access_token(CONNECTION_ID) is None


# ---------------------------------------------------------------------------
# Winner — one refresh, generation-CAS commit, lock always released
# ---------------------------------------------------------------------------


class TestWinner:
    @pytest.mark.asyncio
    async def test_winner_refreshes_once_and_commits_the_next_generation(
        self, store, db, token_endpoint
    ):
        store.row = _row(expires_in=120, generation=3)

        token = await ensure_fresh_access_token(CONNECTION_ID)

        # generation 4: the CAS committed exactly one bump over the row we read.
        assert token == AccessToken(
            access_token="access-new", token_type="Bearer", generation=4
        )
        [call] = token_endpoint.calls
        assert call["method"] == "POST"
        assert call["url"] == f"{ISSUER}/token"
        assert call["data"] == {
            "grant_type": "refresh_token",
            "refresh_token": "refresh-old",
            "client_id": "client-abc123",
        }
        [commit] = store.commits
        assert commit["expected_generation"] == 3
        assert commit["access_token"] == "access-new"
        assert commit["refresh_token"] == "refresh-new"
        expected = datetime.now(timezone.utc) + timedelta(seconds=3600)
        assert abs((commit["expires_at"] - expected).total_seconds()) < 30
        # The stored bundle advanced exactly one generation.
        assert store.row.token_generation == 4

    @pytest.mark.asyncio
    async def test_the_under_lock_re_read_is_the_one_that_takes_the_full_bundle(
        self, store, db, token_endpoint
    ):
        # The counterpart to the bearer-only hot path: the refresh actually
        # spends the refresh token and client secret, so its re-read — and only
        # its re-read — pays for the full decrypt.
        store.row = _row(expires_in=120)

        await ensure_fresh_access_token(CONNECTION_ID)

        assert store.reads == [Secrets.BEARER, Secrets.FULL]

    @pytest.mark.asyncio
    async def test_under_lock_work_reuses_the_held_connection(
        self, store, db, token_endpoint
    ):
        # The refresh winner holds one advisory-locked pool connection and must
        # run its FULL re-read and commit on THAT connection — never nest a
        # second pool acquire inside the first (which stalls every winner on
        # pool timeout under a many-connection refresh storm). The hot-path
        # bearer read, by contrast, acquires its own (conn is None).
        store.row = _row(expires_in=120)

        await ensure_fresh_access_token(CONNECTION_ID)

        assert store.read_conns[0] is None  # hot-path bearer read
        assert store.read_conns[1] is db.last_conn  # under-lock FULL re-read
        assert store.commit_conns == [db.last_conn]

    @pytest.mark.asyncio
    async def test_an_unrotated_refresh_token_is_kept(
        self, store, db, token_endpoint
    ):
        # An AS that omits refresh_token means "keep the one you have";
        # committing that absence as NULL would blank the only copy.
        store.row = _row(expires_in=120)
        token_endpoint.payload = {
            "access_token": "access-new",
            "token_type": "Bearer",
            "expires_in": 3600,
        }

        token = await ensure_fresh_access_token(CONNECTION_ID)

        assert token.access_token == "access-new"
        assert store.commits[0]["refresh_token"] is None
        assert store.row.refresh_token == "refresh-old"

    @pytest.mark.asyncio
    async def test_expires_in_zero_is_stored_as_an_expiry_not_as_forever(
        self, store, db, token_endpoint
    ):
        """0 is an AS saying "already expired", not "no expiry".

        The two are opposite instructions that a falsy test collapses into one:
        stored as NULL, the bearer reads as non-expiring, so it is served past
        its death and never refreshed again.
        """
        store.row = _row(expires_in=120)
        token_endpoint.payload = {
            "access_token": "access-new",
            "token_type": "Bearer",
            "expires_in": 0,
        }

        await ensure_fresh_access_token(CONNECTION_ID)

        [commit] = store.commits
        assert commit["expires_at"] is not None
        elapsed = (commit["expires_at"] - datetime.now(timezone.utc)).total_seconds()
        assert abs(elapsed) < 30

        # The consequence, end to end: the next call finds a dead bearer and
        # refreshes it instead of handing it out forever.
        token_endpoint.payload = {
            "access_token": "access-newer",
            "token_type": "Bearer",
            "expires_in": 3600,
        }
        again = await ensure_fresh_access_token(CONNECTION_ID)

        assert again.access_token == "access-newer"
        assert len(token_endpoint.calls) == 2

    @pytest.mark.asyncio
    async def test_an_omitted_expires_in_is_still_stored_as_no_expiry(
        self, store, db, token_endpoint
    ):
        # The other side of the same test: absent really does mean "no expiry",
        # and must not become an instantly-dead token.
        store.row = _row(expires_in=120)
        token_endpoint.payload = {"access_token": "access-new", "token_type": "Bearer"}

        await ensure_fresh_access_token(CONNECTION_ID)

        assert store.commits[0]["expires_at"] is None

    @pytest.mark.asyncio
    async def test_confidential_client_and_resource_are_sent(
        self, store, db, token_endpoint
    ):
        # RFC 8707: a PRM-scoped connection re-asserts its resource on refresh,
        # and a DCR-issued secret is presented in the body.
        store.row = _row(
            expires_in=120,
            client_secret="client-secret-xyz",
            resource_metadata={"resource": SERVER_URL},
        )

        await ensure_fresh_access_token(CONNECTION_ID)

        [call] = token_endpoint.calls
        assert call["data"]["resource"] == SERVER_URL
        assert call["data"]["client_secret"] == "client-secret-xyz"
        assert call["headers"]["Content-Type"] == "application/x-www-form-urlencoded"

    @pytest.mark.asyncio
    async def test_lock_is_taken_and_released_around_the_refresh(
        self, store, db, token_endpoint
    ):
        store.row = _row(expires_in=120)

        await ensure_fresh_access_token(CONNECTION_ID)

        assert db.lock_attempts == [LOCK_KEY]
        assert db.unlocks == [LOCK_KEY]
        assert "pg_try_advisory_lock" in db.statements[0][0]
        assert "pg_advisory_unlock" in db.statements[-1][0]

    @pytest.mark.asyncio
    async def test_lock_is_released_even_when_the_refresh_fails(
        self, store, db, token_endpoint
    ):
        store.row = _row(expires_in=-5)
        token_endpoint.status_code = 400
        token_endpoint.payload = {"error": "invalid_grant"}

        with pytest.raises(TokenUnavailable):
            await ensure_fresh_access_token(CONNECTION_ID)

        assert db.unlocks == [LOCK_KEY]

    @pytest.mark.asyncio
    async def test_re_read_under_the_lock_skips_a_redundant_refresh(
        self, store, db, token_endpoint
    ):
        """The previous winner committed between our read and our lock."""
        store.row = _row(expires_in=120, generation=3)
        store.script[2] = _row(
            expires_in=3600, generation=4, access_token="access-newer"
        )

        token = await ensure_fresh_access_token(CONNECTION_ID)

        assert token.access_token == "access-newer"
        assert token_endpoint.calls == []
        assert store.commits == []
        assert db.unlocks == [LOCK_KEY]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status_code", [400, 401])
    async def test_definitive_rejection_needs_reauth(
        self, store, db, token_endpoint, status_code
    ):
        store.row = _row(expires_in=120)
        token_endpoint.status_code = status_code
        token_endpoint.payload = {"error": "invalid_grant"}

        with pytest.raises(TokenUnavailable) as excinfo:
            await ensure_fresh_access_token(CONNECTION_ID)

        assert excinfo.value.reason == "needs_reauth"
        assert store.marks == ["needs_reauth"]
        assert store.commits == []

    @pytest.mark.asyncio
    async def test_server_error_keeps_serving_the_old_token(
        self, store, db, token_endpoint
    ):
        store.row = _row(expires_in=120)
        token_endpoint.status_code = 503
        token_endpoint.payload = {"error": "temporarily_unavailable"}

        token = await ensure_fresh_access_token(CONNECTION_ID)

        assert token.access_token == "access-old"
        # A 5xx is transient: the connection stays connected and retryable.
        assert store.marks == []
        assert store.commits == []

    @pytest.mark.asyncio
    async def test_lost_cas_falls_back_to_whatever_is_current(
        self, store, db, token_endpoint
    ):
        store.row = _row(expires_in=120, generation=3)

        def _rival_commits_first():
            store.row = _row(
                expires_in=3600, generation=9, access_token="access-rival"
            )

        # A competing bundle lands while our refresh is in flight, so our own
        # commit is a generation behind by the time it runs.
        token_endpoint.on_call = _rival_commits_first

        token = await ensure_fresh_access_token(CONNECTION_ID)

        assert [c["expected_generation"] for c in store.commits] == [3]
        assert token.access_token == "access-rival"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", ["revoked", "needs_reauth"])
    async def test_a_status_change_under_the_lock_stops_the_refresh(
        self, store, db, token_endpoint, status
    ):
        """The entry gate ran before the lock; the row is re-read after it.

        Without re-checking the status there, a disconnect landing in that
        window still spends the refresh token against a grant the user has
        surrendered — and the outcome then lands back on the revoked row.
        """
        store.row = _row(expires_in=120)
        store.script[2] = _row(expires_in=120, status=status)

        with pytest.raises(TokenUnavailable) as excinfo:
            await ensure_fresh_access_token(CONNECTION_ID)

        assert excinfo.value.reason == status
        assert token_endpoint.calls == []
        assert store.commits == []
        assert store.marks == []
        assert db.unlocks == [LOCK_KEY]

    @pytest.mark.asyncio
    async def test_an_ambiguous_flip_under_the_lock_is_never_retried(
        self, store, db, token_endpoint
    ):
        """Ambiguous is servable but not retryable — the two must not blur.

        The previous winner failed ambiguously and released the lock; our
        entry-gate read predates its mark. Re-spending the same refresh token
        here is exactly the replay the ambiguous state exists to prevent, so
        the under-lock re-read must ride the old bearer instead.
        """
        store.row = _row(expires_in=120)
        store.script[2] = _row(expires_in=120, status="refresh_ambiguous")

        token = await ensure_fresh_access_token(CONNECTION_ID)

        assert token.access_token == "access-old"
        assert token_endpoint.calls == []
        assert store.commits == []
        assert store.marks == []
        assert db.unlocks == [LOCK_KEY]

    @pytest.mark.asyncio
    async def test_an_expired_ambiguous_flip_under_the_lock_needs_reauth(
        self, store, db, token_endpoint
    ):
        # Same race, but the bearer is already dead: nothing may be served and
        # nothing may be retried, so the row settles as needs_reauth.
        store.row = _row(expires_in=120)
        store.script[2] = _row(expires_in=-10, status="refresh_ambiguous")

        with pytest.raises(TokenUnavailable) as excinfo:
            await ensure_fresh_access_token(CONNECTION_ID)

        assert excinfo.value.reason == "needs_reauth"
        assert token_endpoint.calls == []
        assert store.marks == ["needs_reauth"]
        assert db.unlocks == [LOCK_KEY]

    @pytest.mark.asyncio
    async def test_a_refresh_token_nulled_under_the_lock_serves_the_bearer(
        self, store, db, token_endpoint
    ):
        # A reconnect's upsert can null the stored refresh token mid-window;
        # the winner must serve what remains, not POST a None grant.
        store.row = _row(expires_in=120)
        store.script[2] = _row(expires_in=120, refresh_token=None)

        token = await ensure_fresh_access_token(CONNECTION_ID)

        assert token.access_token == "access-old"
        assert token_endpoint.calls == []
        assert db.unlocks == [LOCK_KEY]

    @pytest.mark.asyncio
    async def test_a_cas_lost_to_a_revoke_reports_the_revoke(
        self, store, db, token_endpoint
    ):
        """Under the lock the generation cannot move, so a failed CAS means the
        commit's other guard fired: the status left the servable set."""
        store.row = _row(expires_in=120, generation=3)

        def _revoked_mid_flight():
            store.row = _row(
                expires_in=3600, generation=3, status="revoked",
                access_token="access-surrendered",
            )

        token_endpoint.on_call = _revoked_mid_flight

        with pytest.raises(TokenUnavailable) as excinfo:
            await ensure_fresh_access_token(CONNECTION_ID)

        # Not the still-unexpired bearer of a connection the user gave up.
        assert excinfo.value.reason == "revoked"
        assert db.unlocks == [LOCK_KEY]

    @pytest.mark.asyncio
    async def test_missing_token_endpoint_needs_reauth(
        self, store, db, token_endpoint
    ):
        store.row = _row(expires_in=120, as_metadata={"issuer": ISSUER})

        with pytest.raises(TokenUnavailable) as excinfo:
            await ensure_fresh_access_token(CONNECTION_ID)

        assert excinfo.value.reason == "needs_reauth"
        assert store.marks == ["needs_reauth"]
        assert token_endpoint.calls == []


# ---------------------------------------------------------------------------
# Losers — never block on the winner
# ---------------------------------------------------------------------------


class TestLoser:
    @pytest.fixture
    def db(self, monkeypatch) -> FakeLockDb:
        fake = FakeLockDb(acquired=False)
        monkeypatch.setattr(
            "src.server.database.pool.get_db_connection", fake.connection
        )
        return fake

    @pytest.mark.asyncio
    async def test_still_valid_old_token_is_served_immediately(
        self, store, db, token_endpoint
    ):
        store.row = _row(expires_in=300)  # > the 60s floor

        token = await ensure_fresh_access_token(CONNECTION_ID)

        assert token.access_token == "access-old"
        assert db.lock_attempts == [LOCK_KEY]
        assert db.unlocks == []  # a loser holds nothing to release
        assert token_endpoint.calls == []
        # No polling: the read count is the single up-front read.
        assert store.read_count == 1

    @pytest.mark.asyncio
    async def test_near_expiry_loser_polls_and_picks_up_the_winners_commit(
        self, store, db, token_endpoint, short_poll
    ):
        store.row = _row(expires_in=10, generation=3)  # under the 60s floor
        store.script[2] = _row(
            expires_in=3600, generation=4, access_token="access-new"
        )

        token = await ensure_fresh_access_token(CONNECTION_ID)

        assert token.access_token == "access-new"
        assert store.read_count == 2
        assert token_endpoint.calls == []

    @pytest.mark.asyncio
    async def test_near_expiry_loser_falls_back_to_the_old_token(
        self, store, db, token_endpoint, short_poll
    ):
        """No commit arrives, but the old token still has seconds left."""
        store.row = _row(expires_in=10, generation=3)

        token = await ensure_fresh_access_token(CONNECTION_ID)

        assert token.access_token == "access-old"
        assert token_endpoint.calls == []

    @pytest.mark.asyncio
    async def test_expired_loser_reports_refresh_in_progress(
        self, store, db, token_endpoint, short_poll
    ):
        store.row = _row(expires_in=-5, generation=3)

        with pytest.raises(TokenUnavailable) as excinfo:
            await ensure_fresh_access_token(CONNECTION_ID)

        assert excinfo.value.reason == "refresh_in_progress"
        assert token_endpoint.calls == []
        assert store.commits == []

    @pytest.mark.asyncio
    async def test_a_stale_generation_bump_is_not_mistaken_for_a_refresh(
        self, store, db, token_endpoint, short_poll
    ):
        """A newer generation that is itself already expired is not usable."""
        store.row = _row(expires_in=-5, generation=3)
        store.script[2] = _row(expires_in=-1, generation=4, access_token="access-dud")

        with pytest.raises(TokenUnavailable) as excinfo:
            await ensure_fresh_access_token(CONNECTION_ID)

        assert excinfo.value.reason == "refresh_in_progress"

    @pytest.mark.asyncio
    async def test_a_newer_generation_on_a_revoked_row_is_not_usable_either(
        self, store, db, token_endpoint, short_poll
    ):
        """Usability is status AND clock: the winner's rotation can be followed
        by a disconnect, leaving a fresh, unexpired, unspendable bearer."""
        store.row = _row(expires_in=-5, generation=3)
        store.script[2] = _row(
            expires_in=3600, generation=4, status="revoked",
            access_token="access-surrendered",
        )

        with pytest.raises(TokenUnavailable) as excinfo:
            await ensure_fresh_access_token(CONNECTION_ID)

        assert excinfo.value.reason == "refresh_in_progress"


# ---------------------------------------------------------------------------
# Ambiguous refresh — never retried
# ---------------------------------------------------------------------------


class TestAmbiguousRefresh:
    """The classification is by *what was on the wire*, not by exception family.

    A refresh token is one-time under rotation, so the only safe question is
    whether the request could have reached the AS. Anything that failed after
    the send — a lost response, a mid-stream reset — must be assumed to have
    spent it, because replaying it is what trips an AS's replay detection and
    commonly costs the whole grant.
    """

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "failure",
        [
            httpx2.ReadTimeout("token endpoint timed out"),
            httpx2.ReadError("connection reset waiting for the response"),
            httpx2.WriteError("connection reset mid-request"),
            httpx2.RemoteProtocolError("server disconnected without a response"),
            OAuthHopBlocked("token endpoint hop blocked mid-flight"),
        ],
    )
    async def test_a_post_send_failure_flips_to_ambiguous_and_keeps_the_old_token(
        self, store, db, token_endpoint, failure
    ):
        store.row = _row(expires_in=120)
        token_endpoint.raises = failure

        token = await ensure_fresh_access_token(CONNECTION_ID)

        assert token.access_token == "access-old"
        assert store.marks == ["refresh_ambiguous"]
        assert store.commits == []
        assert len(token_endpoint.calls) == 1

    @pytest.mark.asyncio
    async def test_a_malformed_200_body_flips_to_ambiguous(
        self, store, db, token_endpoint
    ):
        # A 200 the AS honored but whose body we cannot parse: the grant is
        # spent under rotation, so it must classify AMBIGUOUS — escaping as a
        # raw parse error would leave the row connected and the next refresh
        # would replay the consumed token.
        store.row = _row(expires_in=120)
        token_endpoint.payload = {"weird": "body"}

        token = await ensure_fresh_access_token(CONNECTION_ID)

        assert token.access_token == "access-old"
        assert store.marks == ["refresh_ambiguous"]
        assert store.commits == []

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("status_code", "marks"),
        [
            (502, ["refresh_ambiguous"]),
            (504, ["refresh_ambiguous"]),
            (429, []),
            (500, []),
            (503, []),
        ],
    )
    async def test_a_5xx_is_split_by_who_answered(
        self, store, db, token_endpoint, status_code, marks
    ):
        # A gateway 502/504 means the AS's own answer was lost behind it, so
        # the grant may already be spent; a 429/500/503 IS the AS answering
        # about this request, which it could not do having rotated the token.
        store.row = _row(expires_in=120)
        token_endpoint.status_code = status_code
        token_endpoint.payload = {"error": "upstream"}

        token = await ensure_fresh_access_token(CONNECTION_ID)

        assert token.access_token == "access-old"
        assert store.marks == marks
        assert store.commits == []

    @pytest.mark.asyncio
    async def test_ambiguous_refresh_is_never_retried(
        self, store, db, token_endpoint
    ):
        store.row = _row(expires_in=120)
        token_endpoint.raises = httpx2.ReadTimeout("token endpoint timed out")

        first = await ensure_fresh_access_token(CONNECTION_ID)
        token_endpoint.raises = None  # the endpoint recovers; we still must not ask
        second = await ensure_fresh_access_token(CONNECTION_ID)
        third = await ensure_fresh_access_token(CONNECTION_ID)

        assert first.access_token == "access-old"
        assert second.access_token == "access-old"
        assert third.access_token == "access-old"
        # The refresh token may already be consumed server-side: one attempt,
        # ever. Later calls do not even reach for the lock.
        assert len(token_endpoint.calls) == 1
        assert db.lock_attempts == [LOCK_KEY]
        assert store.marks == ["refresh_ambiguous"]

    @pytest.mark.asyncio
    async def test_ambiguous_connection_needs_reauth_once_the_token_expires(
        self, store, db, token_endpoint
    ):
        store.row = _row(expires_in=-1, status="refresh_ambiguous")

        with pytest.raises(TokenUnavailable) as excinfo:
            await ensure_fresh_access_token(CONNECTION_ID)

        assert excinfo.value.reason == "needs_reauth"
        assert store.marks == ["needs_reauth"]
        assert token_endpoint.calls == []
        assert db.lock_attempts == []

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "failure",
        [
            httpx2.ConnectError("connection refused"),
            httpx2.ConnectTimeout("token endpoint connect timed out"),
            httpx2.PoolTimeout("no connection slot free"),
        ],
    )
    async def test_a_pre_send_failure_is_not_ambiguous(
        self, store, db, token_endpoint, failure
    ):
        """No connection was ever established, so nothing could be consumed.

        The timeouts belong here for the same reason ConnectError does — a
        connect that never completed and a pool slot that never came free both
        put zero bytes on the wire. Classifying them as ambiguous would let one
        slow AS walk the sweeper's whole batch into ``refresh_ambiguous``, a
        state nothing ever re-attempts.
        """
        store.row = _row(expires_in=120)
        token_endpoint.raises = failure

        token = await ensure_fresh_access_token(CONNECTION_ID)

        assert token.access_token == "access-old"
        assert store.marks == []  # status untouched — the next call may retry

        token_endpoint.raises = None
        again = await ensure_fresh_access_token(CONNECTION_ID)

        assert again.access_token == "access-new"
        assert len(token_endpoint.calls) == 2

    @pytest.mark.asyncio
    async def test_a_pre_send_hop_block_is_retried_not_burned(
        self, store, db, token_endpoint
    ):
        """The pin runs before the POST, so a blocked hop consumed nothing.

        ``pin_public_url`` raises the same error for a permanent policy
        rejection and for a resolver hiccup ("DNS resolution failed"). Reading
        either as ambiguous lets one second of DNS trouble put the connection
        into ``refresh_ambiguous`` — a state nothing ever retries, which then
        forces a re-auth the moment the access token expires.
        """
        store.row = _row(expires_in=120)
        token_endpoint.raises = OAuthHopBlocked(
            "egress to 'auth.demo.test' is blocked: DNS resolution failed",
            request_sent=False,
        )

        token = await ensure_fresh_access_token(CONNECTION_ID)

        assert token.access_token == "access-old"
        assert store.marks == []  # status untouched — the next call may retry

        token_endpoint.raises = None
        again = await ensure_fresh_access_token(CONNECTION_ID)

        assert again.access_token == "access-new"
        assert len(token_endpoint.calls) == 2

    @pytest.mark.asyncio
    async def test_a_refused_redirect_stays_ambiguous(
        self, store, db, token_endpoint
    ):
        # The hop block's other raise site, and the reason the tag defaults to
        # "sent": the response came back, so the AS saw the grant and may have
        # rotated it behind the redirect.
        store.row = _row(expires_in=120)
        token_endpoint.raises = OAuthHopBlocked(
            "POST https://auth.demo.test/token answered a redirect (302); "
            "redirects are refused on OAuth hops"
        )

        token = await ensure_fresh_access_token(CONNECTION_ID)

        assert token.access_token == "access-old"
        assert store.marks == ["refresh_ambiguous"]

    @pytest.mark.asyncio
    async def test_transport_error_on_an_expired_token_reports_expired(
        self, store, db, token_endpoint
    ):
        store.row = _row(expires_in=-5)
        token_endpoint.raises = httpx2.ConnectError("connection refused")

        with pytest.raises(TokenUnavailable) as excinfo:
            await ensure_fresh_access_token(CONNECTION_ID)

        assert excinfo.value.reason == "expired"
        assert store.marks == []


# ---------------------------------------------------------------------------
# Failure statuses — fenced on the generation they describe
# ---------------------------------------------------------------------------


class TestFailureWritesAreFenced:
    """A refresh outcome describes ONE bundle, so it may only land on that one.

    ``upsert_connection`` takes no advisory lock, so a user reconnecting while a
    refresh is in flight writes a fresh bundle and bumps the generation
    underneath it. An unfenced failure write then flips the just-repaired
    connection to ``needs_reauth`` / ``refresh_ambiguous`` — the user reconnects,
    watches it work, and sees it break again seconds later.
    """

    @staticmethod
    def _reconnect(store):
        """The user's reconnect landing in the window before the write."""

        def _land():
            store.row = _row(
                expires_in=3600, generation=9, access_token="access-reconnected"
            )

        return _land

    @pytest.mark.asyncio
    async def test_a_rejected_refresh_does_not_flip_a_reconnected_bundle(
        self, store, db, token_endpoint
    ):
        store.row = _row(expires_in=120, generation=3)
        token_endpoint.status_code = 400
        token_endpoint.payload = {"error": "invalid_grant"}
        store.on_mark = self._reconnect(store)

        token = await ensure_fresh_access_token(CONNECTION_ID)

        # The stale needs_reauth was refused, and the caller answers with the
        # bundle that actually exists rather than raising over it.
        assert store.refused_marks == ["needs_reauth"]
        assert store.marks == []
        assert store.row.status is ConnectionStatus.CONNECTED
        assert token.access_token == "access-reconnected"

    @pytest.mark.asyncio
    async def test_an_ambiguous_refresh_does_not_flip_a_reconnected_bundle(
        self, store, db, token_endpoint
    ):
        store.row = _row(expires_in=120, generation=3)
        token_endpoint.raises = httpx2.ReadTimeout("token endpoint timed out")
        store.on_mark = self._reconnect(store)

        token = await ensure_fresh_access_token(CONNECTION_ID)

        assert store.refused_marks == ["refresh_ambiguous"]
        assert store.marks == []
        assert store.row.status is ConnectionStatus.CONNECTED
        # Not "access-old": our ambiguity is about a refresh token the reconnect
        # has already replaced, so the new bearer is the honest answer.
        assert token.access_token == "access-reconnected"

    @pytest.mark.asyncio
    async def test_a_missing_token_endpoint_does_not_flip_a_reconnected_bundle(
        self, store, db, token_endpoint
    ):
        # This one never reaches the network, so the reconnect is the only thing
        # that moves — and the fence is all that stands between it and a
        # needs_reauth written over a healthy connection.
        store.row = _row(expires_in=120, generation=3, as_metadata={"issuer": ISSUER})
        store.on_mark = self._reconnect(store)

        token = await ensure_fresh_access_token(CONNECTION_ID)

        assert store.refused_marks == ["needs_reauth"]
        assert store.marks == []
        assert token.access_token == "access-reconnected"
        assert token_endpoint.calls == []

    @pytest.mark.asyncio
    async def test_an_unrefreshable_expired_token_does_not_flip_a_reconnect(
        self, store, db, token_endpoint
    ):
        # The pre-lock verdict: expired, nothing to refresh with. It is read
        # off a row that a reconnect can replace just as easily.
        store.row = _row(expires_in=-5, generation=3, refresh_token=None)
        store.on_mark = self._reconnect(store)

        token = await ensure_fresh_access_token(CONNECTION_ID)

        assert store.refused_marks == ["needs_reauth"]
        assert store.marks == []
        assert token.access_token == "access-reconnected"

    @pytest.mark.asyncio
    async def test_a_refused_write_still_raises_when_nothing_usable_replaced_it(
        self, store, db, token_endpoint
    ):
        """A moved generation is not by itself good news.

        The replacement can be a revoked or already-expired row, and then the
        original failure is still the truth — the fence must not turn "the
        bundle moved" into "everything is fine".
        """
        store.row = _row(expires_in=-5, generation=3)
        token_endpoint.status_code = 400
        token_endpoint.payload = {"error": "invalid_grant"}
        store.on_mark = lambda: setattr(
            store, "row", _row(expires_in=3600, generation=9, status="revoked")
        )

        with pytest.raises(TokenUnavailable) as excinfo:
            await ensure_fresh_access_token(CONNECTION_ID)

        assert excinfo.value.reason == "needs_reauth"
        assert store.refused_marks == ["needs_reauth"]

    @pytest.mark.asyncio
    async def test_an_unmoved_generation_writes_through_the_fence(
        self, store, db, token_endpoint
    ):
        # The transitions themselves are pinned by the sections above, which
        # pass unchanged; what is new here is what the write carries.
        store.row = _row(expires_in=120, generation=3)
        token_endpoint.status_code = 400
        token_endpoint.payload = {"error": "invalid_grant"}

        with pytest.raises(TokenUnavailable):
            await ensure_fresh_access_token(CONNECTION_ID)

        assert store.marks == ["needs_reauth"]
        assert store.refused_marks == []
        # The generation the outcome was computed from...
        assert store.mark_generations == [3]
        # ...and the connection already holding the refresh lock, like every
        # other write on this path — never a second pool acquire.
        assert store.mark_conns == [db.last_conn]

    @pytest.mark.asyncio
    async def test_the_refresh_path_never_uses_the_unfenced_writer(
        self, store, db, token_endpoint
    ):
        # mark_status has one legitimate caller left — disconnect_server, which
        # is writing the terminal state rather than a refresh outcome.
        store.row = _row(expires_in=120)
        token_endpoint.status_code = 401
        token_endpoint.payload = {"error": "invalid_grant"}

        with pytest.raises(TokenUnavailable):
            await ensure_fresh_access_token(CONNECTION_ID)

        assert store.unfenced_marks == []


# ---------------------------------------------------------------------------
# The hop-block raise sites — where the block happened IS the retry decision
# ---------------------------------------------------------------------------


class _FakeUpstream:
    """The streamed side of a hop response: status, headers, chunked body."""

    def __init__(self, *, status=200, headers=None, chunks=(), stall_after=False):
        self.status_code = status
        self.headers = httpx2.Headers(
            headers or {"content-type": "application/json"}
        )
        self.request = httpx2.Request("POST", "https://auth.demo.test/token")
        self._chunks = list(chunks)
        self._stall_after = stall_after
        self.served = 0

    async def aiter_bytes(self):
        for chunk in self._chunks:
            self.served += len(chunk)
            yield chunk
        if self._stall_after:
            await asyncio.sleep(3600)


def _hop_client(upstream: _FakeUpstream):
    @asynccontextmanager
    async def _stream(method, url, **kwargs):
        yield upstream

    return SimpleNamespace(stream=_stream)


class TestHopBlockTagging:
    """``pinned_request`` refuses hops on both sides of the send.

    The classification above can only be as good as this tag, so it is pinned at
    the source: everything downstream reads ``request_sent``, not the message.
    """

    PINNED = PinnedTarget(
        url="https://203.0.113.10/token",
        host="auth.demo.test",
        ip="203.0.113.10",
        authority="auth.demo.test",
    )

    @pytest.mark.asyncio
    async def test_a_failed_pin_is_tagged_as_never_sent(self, monkeypatch):
        async def _blocked(url, **kwargs):
            raise EgressBlockedError(
                "egress to 'auth.demo.test' is blocked: DNS resolution failed"
            )

        monkeypatch.setattr(http, "pin_public_url", _blocked)

        with pytest.raises(OAuthHopBlocked) as excinfo:
            # No client is passed: reaching one would already be the bug.
            await http.pinned_request(None, "POST", "https://auth.demo.test/token")

        assert excinfo.value.request_sent is False

    @pytest.mark.asyncio
    async def test_a_refused_redirect_is_tagged_as_sent(self, monkeypatch):
        async def _pinned(url, **kwargs):
            return self.PINNED

        monkeypatch.setattr(http, "pin_public_url", _pinned)

        with pytest.raises(OAuthHopBlocked) as excinfo:
            await http.pinned_request(
                _hop_client(
                    _FakeUpstream(
                        status=302,
                        headers={"Location": "https://elsewhere.test/token"},
                    )
                ),
                "POST",
                "https://auth.demo.test/token",
            )

        assert excinfo.value.request_sent is True

    def test_an_unlabelled_block_is_assumed_sent(self):
        # The pessimistic default is what keeps a new raise site from silently
        # licensing a replay of a one-time grant.
        assert OAuthHopBlocked("some new hop refusal").request_sent is True

    @pytest.mark.asyncio
    async def test_an_oversized_hop_body_is_refused_mid_stream(self, monkeypatch):
        # A hop answer is KB-scale JSON; buffering whatever the server sends
        # would let one malicious AS spend a worker's memory.
        async def _pinned(url, **kwargs):
            return self.PINNED

        monkeypatch.setattr(http, "pin_public_url", _pinned)
        flood = _FakeUpstream(chunks=[b"x" * 262_144] * 5)

        with pytest.raises(OAuthHopBlocked) as excinfo:
            await http.pinned_request(
                _hop_client(flood), "GET", "https://auth.demo.test/metadata"
            )

        assert "bytes" in str(excinfo.value)
        assert excinfo.value.request_sent is True
        # The cap fired mid-stream: the flood was not drained to completion.
        assert flood.served <= http.HOP_MAX_BYTES + 262_144

    @pytest.mark.asyncio
    async def test_a_stalled_hop_stream_hits_the_wall_clock(self, monkeypatch):
        # The read timeout is an idle timeout — every byte resets it, so only
        # the hop-wide deadline bounds a server that trickles forever.
        async def _pinned(url, **kwargs):
            return self.PINNED

        monkeypatch.setattr(http, "pin_public_url", _pinned)
        monkeypatch.setattr(http, "HOP_DEADLINE_SECONDS", 0.05)

        with pytest.raises(OAuthHopBlocked) as excinfo:
            # The outer wait_for is the test's own guard: a regression that
            # loses the hop deadline fails here in seconds instead of hanging.
            await asyncio.wait_for(
                http.pinned_request(
                    _hop_client(_FakeUpstream(chunks=[b"{"], stall_after=True)),
                    "GET",
                    "https://auth.demo.test/metadata",
                ),
                timeout=5,
            )

        assert "deadline" in str(excinfo.value)
        assert excinfo.value.request_sent is True

    @pytest.mark.asyncio
    async def test_a_bounded_body_round_trips_with_wire_framing_stripped(
        self, monkeypatch
    ):
        # The rebuilt response carries decoded content, so the wire-framing
        # fields (content-encoding et al.) must not survive to confuse a
        # reader into decoding twice.
        async def _pinned(url, **kwargs):
            return self.PINNED

        monkeypatch.setattr(http, "pin_public_url", _pinned)

        response = await http.pinned_request(
            _hop_client(
                _FakeUpstream(
                    headers={
                        "content-type": "application/json",
                        "content-encoding": "gzip",
                        "content-length": "9999",
                    },
                    chunks=[b'{"ok":', b" true}"],
                )
            ),
            "POST",
            "https://auth.demo.test/token",
        )

        assert response.json() == {"ok": True}
        assert response.headers["content-type"] == "application/json"
        assert "content-encoding" not in response.headers
        # httpx restates content-length for the rebuilt body; the wire's
        # value (9999) must not be the one that survived.
        assert response.headers["content-length"] == "12"


# ---------------------------------------------------------------------------
# The commit itself — generation compare-and-swap
# ---------------------------------------------------------------------------


class _CasCursor:
    """Mimics the UPDATE's WHERE clause: rowcount 1 only on a generation hit."""

    def __init__(self, state: dict, log: list[str], params_log: list[tuple]):
        self._state = state
        self._log = log
        self._params_log = params_log
        self.rowcount = 0

    async def __aenter__(self) -> "_CasCursor":
        return self

    async def __aexit__(self, *exc) -> bool:
        return False

    async def execute(self, sql, params=None):
        self._log.append(" ".join(sql.split()))
        self._params_log.append(params)
        access_token = params[0]
        # Trailing params, in SQL order: the status to set, then the three the
        # WHERE clause reads.
        new_status, connection_id, expected_generation, servable = params[-4:]
        state = self._state
        if (
            connection_id == state["connection_id"]
            and expected_generation == state["token_generation"]
            and state["status"] in servable
        ):
            state["token_generation"] += 1
            state["access_token"] = access_token
            state["status"] = new_status
            self.rowcount = 1
        else:
            self.rowcount = 0


class TestGenerationCas:
    @pytest.fixture
    def cas(self, monkeypatch):
        monkeypatch.setenv("BYOK_ENCRYPTION_KEY", "unit-test-key")
        state = {
            "connection_id": CONNECTION_ID,
            "token_generation": 7,
            "access_token": "access-old",
            "status": "connected",
        }
        log: list[str] = []
        params_log: list[tuple] = []

        @asynccontextmanager
        async def _conn(conn=None):
            yield SimpleNamespace(
                cursor=lambda *a, **k: _CasCursor(state, log, params_log)
            )

        monkeypatch.setattr(mcp_oauth_db, "get_db_connection", _conn)
        return SimpleNamespace(state=state, sql=log, params=params_log)

    async def _commit(self, generation: int, access_token: str) -> bool:
        return await mcp_oauth_db.commit_refresh(
            CONNECTION_ID,
            expected_generation=generation,
            access_token=access_token,
            refresh_token="refresh-new",
            expires_at=datetime.now(timezone.utc) + timedelta(seconds=3600),
            scope="notes.read offline_access",
        )

    @pytest.mark.asyncio
    async def test_second_winner_with_a_stale_generation_does_not_land(self, cas):
        first = await self._commit(7, "access-winner")
        second = await self._commit(7, "access-loser")

        assert first is True
        assert second is False
        # The loser's rotation must not overwrite the surviving bundle.
        assert cas.state["access_token"] == "access-winner"
        assert cas.state["token_generation"] == 8

    @pytest.mark.asyncio
    async def test_the_loser_can_commit_once_it_re_reads(self, cas):
        await self._commit(7, "access-winner")

        assert await self._commit(8, "access-second") is True
        assert cas.state["token_generation"] == 9

    @pytest.mark.asyncio
    async def test_a_revoked_connection_rejects_the_commit(self, cas):
        cas.state["status"] = "revoked"

        assert await self._commit(7, "access-winner") is False
        assert cas.state["access_token"] == "access-old"

    @pytest.mark.asyncio
    async def test_an_ambiguous_connection_can_still_be_repaired(self, cas):
        cas.state["status"] = "refresh_ambiguous"

        assert await self._commit(7, "access-repaired") is True
        assert cas.state["status"] == "connected"

    @pytest.mark.asyncio
    async def test_the_update_is_a_compare_and_swap_on_the_generation(self, cas):
        await self._commit(7, "access-winner")

        [sql] = cas.sql
        assert "token_generation = token_generation + 1" in sql
        assert "AND token_generation = %s" in sql
        # The servable set rides in as a parameter, not as inlined literals.
        assert "AND status = ANY(%s)" in sql
        assert cas.params[-1][-1] == ["connected", "refresh_ambiguous"]

    @pytest.mark.asyncio
    async def test_a_null_refresh_token_keeps_the_stored_one(self, cas):
        # The UPDATE must branch on NULL rather than encrypt it, or an AS that
        # skips rotation would cost us the refresh token.
        landed = await mcp_oauth_db.commit_refresh(
            CONNECTION_ID,
            expected_generation=7,
            access_token="access-new",
            refresh_token=None,
            expires_at=None,
        )

        assert landed is True
        [sql] = cas.sql
        assert "refresh_token = CASE WHEN %s::text IS NULL THEN refresh_token" in sql


# ---------------------------------------------------------------------------
# mark_status — revoked is terminal
# ---------------------------------------------------------------------------


class _StatusCursor:
    """Mimics the mark_status UPDATE's WHERE clause."""

    def __init__(self, state: dict, log: list[str]):
        self._state = state
        self._log = log
        self.rowcount = 0

    async def __aenter__(self) -> "_StatusCursor":
        return self

    async def __aexit__(self, *exc) -> bool:
        return False

    async def execute(self, sql, params=None):
        self._log.append(" ".join(sql.split()))
        # The guard parameter is unpacked as optional so that dropping it (and
        # its WHERE clause) reads here as "no guard" — a resurrected row — not
        # as an unpacking error.
        new_status, connection_id, *guard = params
        state = self._state
        blocked = bool(guard) and state["status"] == "revoked" and guard[0] != "revoked"
        if connection_id == state["connection_id"] and not blocked:
            state["status"] = new_status
            self.rowcount = 1
        else:
            self.rowcount = 0


class TestMarkStatusTerminal:
    """``revoked`` is the one status no write may move a row out of.

    Every other status write here is racing a disconnect: a refresh that was
    already in flight resolves afterwards and marks its outcome, and both
    ``needs_reauth`` and ``refresh_ambiguous`` would put the row back somewhere
    a later resolve treats as live — ``refresh_ambiguous`` is servable outright,
    and a re-auth on ``needs_reauth`` revives the grant the user surrendered.
    """

    @pytest.fixture
    def status_db(self, monkeypatch):
        state = {"connection_id": CONNECTION_ID, "status": "connected"}
        log: list[str] = []

        @asynccontextmanager
        async def _conn(conn=None):
            yield SimpleNamespace(cursor=lambda *a, **k: _StatusCursor(state, log))

        monkeypatch.setattr(mcp_oauth_db, "get_db_connection", _conn)
        return SimpleNamespace(state=state, sql=log)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "status",
        [
            ConnectionStatus.NEEDS_REAUTH,
            ConnectionStatus.REFRESH_AMBIGUOUS,
            ConnectionStatus.REVOKED,
        ],
    )
    async def test_a_live_row_takes_any_transition(self, status_db, status):
        assert await mcp_oauth_db.mark_status(CONNECTION_ID, status) is True
        assert status_db.state["status"] == status.value

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "status",
        [ConnectionStatus.NEEDS_REAUTH, ConnectionStatus.REFRESH_AMBIGUOUS],
    )
    async def test_a_revoked_row_is_never_resurrected(self, status_db, status):
        status_db.state["status"] = "revoked"

        assert await mcp_oauth_db.mark_status(CONNECTION_ID, status) is False
        assert status_db.state["status"] == "revoked"

    @pytest.mark.asyncio
    async def test_revoking_a_revoked_row_still_succeeds(self, status_db):
        # Disconnect must stay idempotent — the guard is about leaving the
        # terminal state, not about writing it again.
        status_db.state["status"] = "revoked"

        assert (
            await mcp_oauth_db.mark_status(CONNECTION_ID, ConnectionStatus.REVOKED)
            is True
        )

    @pytest.mark.asyncio
    async def test_the_guard_rides_in_the_statement(self, status_db):
        """A read-then-write would reopen the window this exists to close."""
        await mcp_oauth_db.mark_status(CONNECTION_ID, ConnectionStatus.NEEDS_REAUTH)

        [sql] = status_db.sql
        assert "AND (status <> 'revoked' OR %s::text = 'revoked')" in sql


class _FencedStatusCursor:
    """Mimics the mark_status_if_generation UPDATE's WHERE clause."""

    def __init__(self, state: dict, log: list[str]):
        self._state = state
        self._log = log
        self.rowcount = 0

    async def __aenter__(self) -> "_FencedStatusCursor":
        return self

    async def __aexit__(self, *exc) -> bool:
        return False

    async def execute(self, sql, params=None):
        flat = " ".join(sql.split())
        self._log.append(flat)
        # The trailing params are bound to whichever guards the statement
        # actually carries, so dropping one reads here as "unguarded" — the way
        # Postgres would run it — instead of shifting the remaining values into
        # the wrong comparison.
        new_status, connection_id, *rest = params
        generation = rest.pop(0) if "AND token_generation = %s" in flat else None
        terminal_guard = rest.pop(0) if "%s::text = 'revoked'" in flat else None
        state = self._state
        stale = generation is not None and generation != state["token_generation"]
        blocked = (
            terminal_guard is not None
            and state["status"] == "revoked"
            and terminal_guard != "revoked"
        )
        if connection_id == state["connection_id"] and not stale and not blocked:
            state["status"] = new_status
            self.rowcount = 1
        else:
            self.rowcount = 0


class TestFencedStatusCas:
    """The status write the refresh path uses: generation fence + terminal guard.

    It is deliberately NOT ``mark_needs_reauth``: that one requires the row to be
    exactly ``connected``, because a vendor 401 must never downgrade the strictly
    more informative ``refresh_ambiguous``. A refresh outcome, holding the row
    under the lock, may re-mark one.
    """

    @pytest.fixture
    def cas(self, monkeypatch):
        state = {
            "connection_id": CONNECTION_ID,
            "token_generation": 7,
            "status": "connected",
        }
        log: list[str] = []

        @asynccontextmanager
        async def _conn(conn=None):
            yield SimpleNamespace(
                cursor=lambda *a, **k: _FencedStatusCursor(state, log)
            )

        monkeypatch.setattr(mcp_oauth_db, "get_db_connection", _conn)
        return SimpleNamespace(state=state, sql=log)

    @pytest.mark.asyncio
    async def test_the_generation_it_read_takes_the_write(self, cas):
        applied = await mcp_oauth_db.mark_status_if_generation(
            CONNECTION_ID,
            ConnectionStatus.REFRESH_AMBIGUOUS,
            expected_generation=7,
        )

        assert applied is True
        assert cas.state["status"] == "refresh_ambiguous"

    @pytest.mark.asyncio
    async def test_a_bundle_that_moved_refuses_the_write(self, cas):
        # A reconnect landed while the refresh was failing: generation 8 is a
        # bundle this outcome says nothing about.
        cas.state["token_generation"] = 8

        applied = await mcp_oauth_db.mark_status_if_generation(
            CONNECTION_ID, ConnectionStatus.NEEDS_REAUTH, expected_generation=7
        )

        assert applied is False
        assert cas.state["status"] == "connected"

    @pytest.mark.asyncio
    async def test_a_revoked_row_is_still_never_resurrected(self, cas):
        cas.state["status"] = "revoked"

        applied = await mcp_oauth_db.mark_status_if_generation(
            CONNECTION_ID, ConnectionStatus.REFRESH_AMBIGUOUS, expected_generation=7
        )

        assert applied is False
        assert cas.state["status"] == "revoked"

    @pytest.mark.asyncio
    async def test_both_guards_ride_in_one_statement(self, cas):
        """A read-then-write would reopen the window this exists to close."""
        await mcp_oauth_db.mark_status_if_generation(
            CONNECTION_ID, ConnectionStatus.NEEDS_REAUTH, expected_generation=7
        )

        [sql] = cas.sql
        assert "AND token_generation = %s" in sql
        assert "AND (status <> 'revoked' OR %s::text = 'revoked')" in sql


# ---------------------------------------------------------------------------
# Reporting a vendor 401 — the other compare-and-swap
# ---------------------------------------------------------------------------


class _ReauthCursor:
    """Mimics the needs_reauth UPDATE's WHERE clause."""

    def __init__(self, state: dict, log: list[str]):
        self._state = state
        self._log = log
        self.rowcount = 0

    async def __aenter__(self) -> "_ReauthCursor":
        return self

    async def __aexit__(self, *exc) -> bool:
        return False

    async def execute(self, sql, params=None):
        self._log.append(" ".join(sql.split()))
        new_status, connection_id, expected_generation, required_status = params
        state = self._state
        if (
            connection_id == state["connection_id"]
            and expected_generation == state["token_generation"]
            and state["status"] == required_status
        ):
            state["status"] = new_status
            self.rowcount = 1
        else:
            self.rowcount = 0


class TestNeedsReauthCas:
    """The relay reports which bundle a vendor rejected; this decides if it lands.

    Moving the decision here is the point: the relay observed a 401 at some
    instant, and by the time the write runs that observation may already be
    stale — only the row itself can adjudicate that.
    """

    @pytest.fixture
    def cas(self, monkeypatch):
        state = {
            "connection_id": CONNECTION_ID,
            "token_generation": 7,
            "status": "connected",
        }
        log: list[str] = []

        @asynccontextmanager
        async def _conn(conn=None):
            yield SimpleNamespace(cursor=lambda *a, **k: _ReauthCursor(state, log))

        monkeypatch.setattr(mcp_oauth_db, "get_db_connection", _conn)
        return SimpleNamespace(state=state, sql=log)

    @pytest.mark.asyncio
    async def test_the_rejected_generation_flips_the_connection(self, cas):
        assert await lifecycle.mark_connection_needs_reauth(
            CONNECTION_ID, seen_token_generation=7
        ) is True
        assert cas.state["status"] == "needs_reauth"

    @pytest.mark.asyncio
    async def test_a_rotation_since_the_401_makes_the_report_moot(self, cas):
        """Another worker refreshed after the vendor said no: the stored bundle
        is not the one that was rejected, so it must survive."""
        cas.state["token_generation"] = 8

        assert await lifecycle.mark_connection_needs_reauth(
            CONNECTION_ID, seen_token_generation=7
        ) is False
        assert cas.state["status"] == "connected"

    @pytest.mark.asyncio
    async def test_a_terminal_status_is_not_overwritten(self, cas):
        # refresh_ambiguous carries strictly more information (never retry the
        # refresh token) than needs_reauth; downgrading it would lose that.
        cas.state["status"] = "refresh_ambiguous"

        assert await lifecycle.mark_connection_needs_reauth(
            CONNECTION_ID, seen_token_generation=7
        ) is False
        assert cas.state["status"] == "refresh_ambiguous"

    @pytest.mark.asyncio
    async def test_a_second_report_of_the_same_generation_is_a_no_op(self, cas):
        first = await lifecycle.mark_connection_needs_reauth(
            CONNECTION_ID, seen_token_generation=7
        )
        second = await lifecycle.mark_connection_needs_reauth(
            CONNECTION_ID, seen_token_generation=7
        )

        assert (first, second) == (True, False)
        assert cas.state["status"] == "needs_reauth"

    @pytest.mark.asyncio
    async def test_both_guards_ride_in_one_statement(self, cas):
        """A read-then-write would reopen the window this exists to close."""
        await lifecycle.mark_connection_needs_reauth(
            CONNECTION_ID, seen_token_generation=7
        )

        [sql] = cas.sql
        assert "AND token_generation = %s" in sql
        assert "AND status = %s" in sql


# ---------------------------------------------------------------------------
# The DB-layer → lifecycle row contract
# ---------------------------------------------------------------------------


class _RowCursor:
    """Answers the single-row SELECT with a psycopg-shaped row."""

    def __init__(self, row: dict):
        self._row = row

    async def __aenter__(self) -> "_RowCursor":
        return self

    async def __aexit__(self, *exc) -> bool:
        return False

    async def execute(self, sql, params=None):
        self._row.setdefault("_executed", True)

    async def fetchone(self):
        return self._row


class TestRowShapeContract:
    """``get_connection_by_id`` feeds ``_expiry_seconds`` directly, so what it
    returns has to be native types — not the UI's serialized view.

    Regression lock: the decrypted read once went through ``_row_summary``,
    which ISO-serializes ``expires_at``; every refresh-due call then died on
    ``'str' object has no attribute 'tzinfo'``. These drive the real DB helper
    against a faked cursor, so they fail if that routing ever comes back.
    """

    EXPIRES_IN = 900

    @pytest.fixture
    def db_row(self, monkeypatch) -> dict:
        monkeypatch.setenv("BYOK_ENCRYPTION_KEY", "unit-test-key")
        now = datetime.now(timezone.utc)
        row = {
            # psycopg hands back a UUID object, not a string.
            "connection_id": uuid.UUID(CONNECTION_ID),
            "user_id": USER_ID,
            "server_name": SERVER_NAME,
            "server_url": SERVER_URL,
            "status": "connected",
            "token_type": "Bearer",
            "scope": "notes.read offline_access",
            "granted_capabilities": None,
            "expires_at": now + timedelta(seconds=self.EXPIRES_IN),
            "token_generation": 3,
            "client_info": {"client_id": "client-abc123"},
            "as_metadata": {"issuer": ISSUER},
            "resource_metadata": None,
            "has_refresh_token": True,
            "last_refresh_at": None,
            "created_at": now,
            "updated_at": now,
            "access_token_plain": "access-old",
            "refresh_token_plain": "refresh-old",
            "client_secret_plain": None,
        }

        @asynccontextmanager
        async def _conn(conn=None):
            yield SimpleNamespace(cursor=lambda *a, **k: _RowCursor(row))

        monkeypatch.setattr(mcp_oauth_db, "get_db_connection", _conn)
        return row

    @pytest.mark.asyncio
    async def test_expires_at_survives_as_a_datetime(self, db_row):
        out = await mcp_oauth_db.get_connection_by_id(CONNECTION_ID, secrets=Secrets.FULL)

        assert isinstance(out.expires_at, datetime)
        assert out.expires_at == db_row["expires_at"]
        # The consumer that broke: expiry math straight off the returned row.
        remaining = lifecycle._expiry_seconds(out)
        assert isinstance(remaining, float)
        assert abs(remaining - self.EXPIRES_IN) < 30

    @pytest.mark.asyncio
    async def test_a_db_layer_row_drives_the_hot_path_end_to_end(
        self, db_row, store, db, token_endpoint
    ):
        # The two layers joined: the row the DB helper really produces, handed
        # to the lifecycle unmodified. This is the exact call that used to
        # raise AttributeError before the row shape was fixed.
        store.row = await mcp_oauth_db.get_connection_by_id(CONNECTION_ID, secrets=Secrets.FULL)

        token = await ensure_fresh_access_token(CONNECTION_ID)

        assert token.access_token == "access-old"
        # The generation rides across the layer boundary as an int — it is what
        # a later rotation check compares against.
        assert token.generation == db_row["token_generation"]
        # 900s left is outside the 600s margin, so this is the no-lock path.
        assert db.lock_attempts == []
        assert token_endpoint.calls == []

    @pytest.mark.asyncio
    async def test_decrypted_plaintext_is_mapped_onto_the_full_record(self, db_row):
        out = await mcp_oauth_db.get_connection_by_id(CONNECTION_ID, secrets=Secrets.FULL)

        assert isinstance(out, RefreshBundle)
        assert out.access_token == "access-old"
        assert out.refresh_token == "refresh-old"
        assert out.client_secret is None
        # The ciphertext-column aliases must not ride along.
        assert not [name for name in dir(out) if name.endswith("_plain")]

    @pytest.mark.asyncio
    async def test_a_summary_read_cannot_reach_token_plaintext(self, db_row):
        # The record IS the mode: a read that paid for no decrypt has nowhere
        # to put a token, so "did this reader ask for the bearer?" stops being
        # a convention and becomes a type.
        out = await mcp_oauth_db.get_connection_by_id(CONNECTION_ID)

        assert type(out) is ConnectionSummary
        assert not hasattr(out, "access_token")
        assert not hasattr(out, "refresh_token")
        # ...while still answering whether a refresh is possible at all.
        assert out.has_refresh_token is True

    @pytest.mark.asyncio
    async def test_connection_id_is_stringified(self, db_row):
        out = await mcp_oauth_db.get_connection_by_id(CONNECTION_ID, secrets=Secrets.FULL)

        # Callers interpolate it into advisory-lock keys and log lines.
        assert out.connection_id == CONNECTION_ID
        assert isinstance(out.connection_id, str)

    def test_the_ui_view_still_serializes_timestamps(self, db_row):
        # The other half of the split the fix established: list_connections is
        # a JSON view, so _row_summary keeps ISO strings. Don't unify these.
        summary = mcp_oauth_db._row_summary(db_row)

        assert isinstance(summary["expires_at"], str)
        assert summary["expires_at"] == db_row["expires_at"].isoformat()


# ---------------------------------------------------------------------------
# disconnect_server — the revocation writes commit together
# ---------------------------------------------------------------------------


class FakeDisconnectDb:
    """Records, per write, which connection it ran on and the open-transaction
    depth at the time — the two things atomicity here consists of."""

    def __init__(self) -> None:
        self.conn = SimpleNamespace(transaction=self._transaction)
        self.depth = 0
        self.writes: list[tuple[str, object, int]] = []
        self.args: dict[str, tuple] = {}

    @asynccontextmanager
    async def _transaction(self):
        self.depth += 1
        try:
            yield
        finally:
            self.depth -= 1

    @asynccontextmanager
    async def connection(self):
        yield self.conn

    def write(self, name: str):
        async def _recorded(*args, conn=None, **kwargs):
            self.writes.append((name, conn, self.depth))
            self.args[name] = args
            return 1

        return _recorded

    @property
    def trace(self) -> list[tuple[str, bool, int]]:
        return [(name, c is self.conn, depth) for name, c, depth in self.writes]


@pytest.fixture
def disconnect_db(monkeypatch) -> FakeDisconnectDb:
    fake = FakeDisconnectDb()
    monkeypatch.setattr(
        "src.server.database.pool.get_db_connection", fake.connection
    )
    monkeypatch.setattr(lifecycle, "mark_status", fake.write("mark_status"))
    monkeypatch.setattr(
        lifecycle, "revoke_grants_for_connection", fake.write("revoke_grants")
    )
    monkeypatch.setattr(
        "src.server.database.mcp_tool_schemas."
        "delete_user_and_workspace_tool_schemas_and_bump",
        fake.write("purge_both_tiers"),
    )
    # Patched only so a regression to either narrower write shows up in the
    # trace rather than as a stray database call.
    monkeypatch.setattr(
        "src.server.database.mcp_tool_schemas.delete_user_tool_schemas",
        fake.write("purge_user_tier_only"),
    )
    monkeypatch.setattr(
        "src.server.database.mcp_servers.bump_user_workspaces_mcp_version",
        fake.write("bump_versions"),
    )
    return fake


def _connected(row=None):
    async def _get_connection(user_id, server_name, **kwargs):
        return row

    return _get_connection


@pytest.mark.asyncio
class TestDisconnectAtomicity:
    """A half-applied disconnect disagrees with itself — grants revoked while
    the row still reads connected leaves the sweeper renewing a credential the
    user gave up. All three writes therefore share one transaction."""

    async def test_the_three_revocation_writes_share_one_transaction(
        self, disconnect_db, monkeypatch
    ):
        monkeypatch.setattr(
            "src.server.database.mcp_oauth.get_connection",
            _connected(_project(_row(), Secrets.NONE)),
        )

        assert await lifecycle.disconnect_server(USER_ID, SERVER_NAME) is True

        # Same connection, transaction open, for each — the purge carries the
        # fan-out bump, so nothing is left to commit on its own afterwards.
        assert disconnect_db.trace == [
            ("mark_status", True, 1),
            ("revoke_grants", True, 1),
            ("purge_both_tiers", True, 1),
        ]

    async def test_the_purge_spans_both_snapshot_tiers(
        self, disconnect_db, monkeypatch
    ):
        """The user-tier delete alone is not enough.

        The per-workspace snapshot's fingerprint is OAuth-blind, so it survives
        a disconnect unchanged; the resolved config meanwhile drops the
        connection, and the surviving snapshot's tools would be generated
        against the vendor directly, with no relay in front of them.
        """
        monkeypatch.setattr(
            "src.server.database.mcp_oauth.get_connection",
            _connected(_project(_row(), Secrets.NONE)),
        )

        await lifecycle.disconnect_server(USER_ID, SERVER_NAME)

        assert disconnect_db.args["purge_both_tiers"] == (USER_ID, [SERVER_NAME])
        assert "purge_user_tier_only" not in disconnect_db.args

    async def test_no_connection_writes_nothing(self, disconnect_db, monkeypatch):
        monkeypatch.setattr(
            "src.server.database.mcp_oauth.get_connection", _connected(None)
        )

        assert await lifecycle.disconnect_server(USER_ID, SERVER_NAME) is False
        assert disconnect_db.writes == []


# ---------------------------------------------------------------------------
# upsert_connection — refresh-token retention is scoped to the same grant
# ---------------------------------------------------------------------------


class _UpsertCursor:
    """Captures the upsert SQL; answers with a fixed connection row."""

    def __init__(self, log: list[str]) -> None:
        self._log = log

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def execute(self, sql, params=None):
        self._log.append(sql)

    async def fetchone(self):
        return {"connection_id": CONNECTION_ID}


class TestUpsertRetention:
    @pytest.fixture
    def upsert_sql(self, monkeypatch):
        monkeypatch.setenv("BYOK_ENCRYPTION_KEY", "unit-test-key")
        log: list[str] = []

        @asynccontextmanager
        async def _conn(conn=None):
            yield SimpleNamespace(cursor=lambda *a, **k: _UpsertCursor(log))

        monkeypatch.setattr(mcp_oauth_db, "get_db_connection", _conn)
        return log

    @pytest.mark.asyncio
    async def test_retention_requires_the_same_grant_identity(self, upsert_sql):
        # Keeping a refresh token across a re-registration would pair the old
        # client's token with the new client's credentials, which the AS
        # answers with invalid_grant — the retention arm must require issuer,
        # URL, and client identity to all be unchanged.
        await mcp_oauth_db.upsert_connection(
            "user-1",
            "srv",
            server_url="https://mcp.example.com/mcp",
            access_token="at",
            refresh_token=None,
        )
        [sql] = upsert_sql
        retention = sql.split("refresh_token = CASE", 1)[1].split("END,", 1)[0]
        assert "as_metadata->>'issuer'" in retention
        assert "server_url" in retention
        assert "client_info->>'client_id'" in retention

    @pytest.mark.asyncio
    async def test_retention_refuses_a_refresh_ambiguous_row(self, upsert_sql):
        # An ambiguous row's stored token may already be consumed at the AS,
        # and this write resets the row to connected — retaining would hand
        # the next refresh a replay that trips reuse detection. Nulling costs
        # only a re-auth at expiry, the documented ambiguous outcome.
        await mcp_oauth_db.upsert_connection(
            "user-1",
            "srv",
            server_url="https://mcp.example.com/mcp",
            access_token="at",
            refresh_token=None,
        )
        [sql] = upsert_sql
        retention = sql.split("refresh_token = CASE", 1)[1].split("END,", 1)[0]
        assert "<> 'refresh_ambiguous'" in retention
