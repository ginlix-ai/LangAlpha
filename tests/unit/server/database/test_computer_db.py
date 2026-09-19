"""The computer layer's settled contract: the fences, and who may move a binding.

These are the parts a refactor breaks silently. The write-through statements are
what keep ``workspaces`` truthful for the colour still serving and for the
platform's capacity accounting, so every one of them has to stay a single
statement with ``comp`` ahead of ``shadow`` -- that ordering is what stops two
writers racing on the same pair from deadlocking. The tombstone fence has to
stay on every status move and every bind, because nothing clears
``provider_ref``: a deleted computer still names a real machine, and handing it
back out bills until someone notices. And ``workspaces.sandbox_id`` has exactly
one writer, which is the point of the whole shadow.

Behaviour against a real Postgres is verified end to end elsewhere; what these
lock is the SQL, since a mock cannot tell you a CAS holds.
"""

from __future__ import annotations

import re
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from src.server.database import computer as C

COMPUTER_ID = "11111111-1111-4111-8111-111111111111"
WORKSPACE_ID = "22222222-2222-4222-8222-222222222222"
USER_ID = "user-1"


def _computer_row(**overrides):
    row = {
        "computer_id": COMPUTER_ID,
        "user_id": USER_ID,
        "kind": "daytona",
        "provider_ref": "sbx-1",
        "name": "My computer",
        "is_primary": True,
        "status": "running",
        "resource_tier": "standard",
        "is_always_on": False,
        "platform_secret_version": 0,
        "mcp_config_version": 0,
        "root_dir": "/home/workspace",
        "layout_version": 0,
        "provider_config": {},
        "artifacts": {},
        "last_activity_at": None,
        "stopped_at": None,
        "created_at": None,
        "updated_at": None,
        "config": {},
        "shadowed_workspace_ids": [WORKSPACE_ID],
    }
    row.update(overrides)
    return row


@pytest.fixture
def cursor():
    cur = AsyncMock()
    cur.execute = AsyncMock()
    cur.fetchone = AsyncMock(return_value=_computer_row())
    cur.fetchall = AsyncMock(return_value=[])
    cur.rowcount = 0
    return cur


@pytest.fixture
def db(cursor):
    """Patch the pool at computer.py's import site and mute the pub/sub fan-out."""
    conn = AsyncMock()

    @asynccontextmanager
    async def _cursor_cm(**kwargs):
        yield cursor

    conn.cursor = _cursor_cm
    conn.execute = AsyncMock()

    @asynccontextmanager
    async def _fake(passed=None):
        yield passed if passed is not None else conn

    with (
        patch("src.server.database.computer.get_db_connection", new=_fake),
        patch(
            "src.server.services.workspace_status_pubsub.publish_status_change",
            new=AsyncMock(),
        ) as publish,
    ):
        yield publish


def _sql(cursor) -> str:
    return re.sub(r"\s+", " ", cursor.execute.call_args[0][0])


def _params(cursor):
    return cursor.execute.call_args[0][1]


class TestWriteThroughShape:
    """One statement, computers before workspaces, both rows or neither."""

    @pytest.mark.parametrize(
        "call",
        [
            pytest.param(
                lambda: C.update_computer_status(COMPUTER_ID, "stopped"),
                id="status",
            ),
            pytest.param(
                lambda: C.try_claim_computer_for_start(COMPUTER_ID),
                id="claim",
            ),
            pytest.param(
                lambda: C.try_bind_computer_provider_ref(
                    COMPUTER_ID,
                    provider_ref="sbx-2",
                    expected_previous_provider_ref="sbx-1",
                    platform_secret_version=3,
                ),
                id="bind",
            ),
            pytest.param(
                lambda: C.set_computer_resource_tier(COMPUTER_ID, "max"),
                id="tier",
            ),
            pytest.param(
                lambda: C.set_computer_always_on(COMPUTER_ID, True),
                id="always_on",
            ),
            pytest.param(
                lambda: C.stamp_computer_platform_secret_version(
                    7, computer_id=COMPUTER_ID, expected_provider_ref="sbx-1"
                ),
                id="platform_secret_version",
            ),
        ],
    )
    @pytest.mark.asyncio
    async def test_it_is_one_statement_with_comp_ahead_of_shadow(
        self, db, cursor, call
    ):
        cursor.fetchone.return_value = _computer_row(stamped=1)
        await call()

        assert cursor.execute.await_count == 1, "two statements can half-commit"
        sql = _sql(cursor)
        assert "UPDATE computers" in sql and "UPDATE workspaces" in sql
        assert sql.index("WITH comp AS") < sql.index("shadow AS")
        # The shadow reads comp's output, which is what forces the row locks to
        # be taken in that order in every statement here.
        assert "FROM comp" in sql


class TestTombstoneFence:
    """Nothing clears ``provider_ref``, so a deleted row still names a machine."""

    @pytest.mark.asyncio
    async def test_a_status_move_refuses_a_deleted_computer(self, db, cursor):
        await C.update_computer_status(COMPUTER_ID, "running")
        assert "c.status <> 'deleted'" in _sql(cursor)

    @pytest.mark.asyncio
    async def test_a_bind_refuses_the_three_states_it_must_not_cross(self, db, cursor):
        await C.try_bind_computer_provider_ref(
            COMPUTER_ID,
            provider_ref="sbx-2",
            expected_previous_provider_ref="sbx-1",
            platform_secret_version=1,
        )
        sql = _sql(cursor)
        assert sql.count("status NOT IN ('deleted', 'stopping', 'stopped')") == 2
        assert "c.provider_ref IS NOT DISTINCT FROM %(expected_previous)s" in sql
        assert "w.sandbox_id IS NOT DISTINCT FROM %(expected_previous)s" in sql

    @pytest.mark.asyncio
    async def test_a_scalar_set_refuses_a_deleted_computer(self, db, cursor):
        await C.set_computer_resource_tier(COMPUTER_ID, "max")
        assert "c.status <> 'deleted'" in _sql(cursor)

    @pytest.mark.asyncio
    async def test_an_activity_stamp_refuses_a_deleted_computer(self, db, cursor):
        cursor.rowcount = 1
        await C.update_computer_activity(COMPUTER_ID)
        assert "status <> 'deleted'" in _sql(cursor)

    @pytest.mark.asyncio
    async def test_a_deleted_workspace_is_never_revived_by_its_machine(
        self, db, cursor
    ):
        """Its computer moving must not resurrect a soft-deleted project, and a
        flash workspace has no machine to follow."""
        await C.update_computer_status(COMPUTER_ID, "running")
        assert "w.status NOT IN ('deleted', 'flash')" in _sql(cursor)


class TestFences:
    @pytest.mark.asyncio
    async def test_a_status_move_never_touches_the_binding(self, db, cursor):
        await C.update_computer_status(COMPUTER_ID, "running")
        sql = _sql(cursor)
        assert "provider_ref =" not in sql
        assert "sandbox_id =" not in sql

    @pytest.mark.asyncio
    async def test_the_expected_status_cas_is_in_the_sql(self, db, cursor):
        await C.update_computer_status(COMPUTER_ID, "running", expected="stopped")
        assert "c.status = ANY(%(expected)s)" in _sql(cursor)
        assert _params(cursor)["expected"] == ["stopped"]

    @pytest.mark.asyncio
    async def test_without_expected_there_is_no_status_cas(self, db, cursor):
        await C.update_computer_status(COMPUTER_ID, "running")
        assert "ANY(%(expected)s)" not in _sql(cursor)

    @pytest.mark.asyncio
    async def test_a_tombstone_surrenders_the_primary_flag(self, db, cursor):
        """Every primary probe fences on the tombstone while the index does the
        same, so a deleted row keeping the flag holds a slot nobody can read and
        the user's next primary insert fails forever."""
        await C.update_computer_status(COMPUTER_ID, "deleted")
        assert "is_primary = FALSE" in _sql(cursor)

    @pytest.mark.asyncio
    async def test_no_other_transition_touches_the_primary_flag(self, db, cursor):
        """Stopping a machine is not giving it up as the user's default."""
        for status in ("stopping", "stopped", "running", "error"):
            await C.update_computer_status(COMPUTER_ID, status)
            assert "is_primary =" not in _sql(cursor), status

    @pytest.mark.asyncio
    async def test_a_status_outside_the_check_set_is_refused_before_the_write(
        self, db, cursor
    ):
        """'flash' is a workspace state; sending it would fail 046's CHECK at the
        end of a statement that has already moved workspaces rows."""
        with pytest.raises(ValueError, match="flash"):
            await C.update_computer_status(COMPUTER_ID, "flash")
        cursor.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_the_provider_ref_guard_is_opt_in_because_none_is_a_real_value(
        self, db, cursor
    ):
        await C.try_claim_computer_for_start(COMPUTER_ID)
        assert "provider_ref IS NOT DISTINCT FROM" not in _sql(cursor)

        await C.try_claim_computer_for_start(
            COMPUTER_ID, expected_provider_ref=None, require_provider_ref=True
        )
        assert (
            "c.provider_ref IS NOT DISTINCT FROM %(expected_ref)s" in _sql(cursor)
        )

    @pytest.mark.asyncio
    async def test_the_platform_secret_stamp_needs_exactly_one_authority(self, db):
        with pytest.raises(ValueError, match="exactly one"):
            await C.stamp_computer_platform_secret_version(
                1, expected_provider_ref=None
            )
        with pytest.raises(ValueError, match="exactly one"):
            await C.stamp_computer_platform_secret_version(
                1,
                computer_id=COMPUTER_ID,
                workspace_id=WORKSPACE_ID,
                expected_provider_ref=None,
            )

    @pytest.mark.asyncio
    async def test_the_activity_cooldown_is_the_predicate_not_the_caller(
        self, db, cursor
    ):
        """A caller-side check is per process; this has to hold across workers."""
        await C.update_computer_activity(COMPUTER_ID)
        assert "INTERVAL '60 seconds'" in _sql(cursor)

    @pytest.mark.asyncio
    async def test_an_unlisted_scalar_column_is_refused(self, db):
        with pytest.raises(ValueError, match="not settable"):
            await C._set_computer_scalar(COMPUTER_ID, "provider_ref", "sbx-evil")


class TestReads:
    @pytest.mark.asyncio
    async def test_the_resolver_hides_a_deleted_machine(self, db, cursor):
        await C.get_computer_for_workspace(WORKSPACE_ID)
        sql = _sql(cursor)
        assert "JOIN workspaces w ON w.computer_id = c.computer_id" in sql
        assert "c.status <> 'deleted'" in sql

    @pytest.mark.asyncio
    async def test_the_resolver_qualifies_the_ambiguous_column(self, db, cursor):
        """``computer_id`` is on both tables; an unqualified list is an error."""
        await C.get_computer_for_workspace(WORKSPACE_ID)
        assert "SELECT c.computer_id, c.user_id" in _sql(cursor)

    @pytest.mark.asyncio
    async def test_a_non_uuid_workspace_id_is_not_found_rather_than_a_500(self, db):
        assert await C.get_computer_for_workspace("memory/notes.md") is None
        assert await C.get_computer("memory/notes.md") is None

    @pytest.mark.asyncio
    async def test_the_primary_read_is_fenced_on_the_flag_and_the_tombstone(
        self, db, cursor
    ):
        await C.get_primary_computer(USER_ID)
        assert "WHERE user_id = %s AND is_primary AND status <> 'deleted'" in _sql(
            cursor
        )

    @pytest.mark.asyncio
    async def test_the_cleanup_scan_takes_the_stalest_first(self, db, cursor):
        await C.get_computers_by_status("running", 50)
        sql = _sql(cursor)
        assert "ORDER BY last_activity_at ASC NULLS FIRST" in sql
        assert _params(cursor) == ("running", 50)


class TestCreate:
    @pytest.mark.asyncio
    async def test_a_second_primary_is_squashed_in_the_statement(self, db, cursor):
        await C.create_computer(USER_ID, is_primary=True)
        sql = _sql(cursor)
        assert "NOT EXISTS ( SELECT 1 FROM computers WHERE user_id = %s" in sql
        assert "is_primary AND status <> 'deleted'" in sql

    @pytest.mark.asyncio
    async def test_losing_the_primary_race_retries_as_non_primary(self, db, cursor):
        """The collision means the user already has a primary, which is the
        outcome the caller wanted; raising would fail a legitimate provision."""
        from psycopg.errors import UniqueViolation

        cursor.execute.side_effect = [UniqueViolation(), None]
        cursor.fetchone.return_value = _computer_row(is_primary=False)

        row = await C.create_computer(USER_ID, is_primary=True)

        assert row["is_primary"] is False
        assert cursor.execute.await_count == 2
        assert cursor.execute.await_args_list[-1][0][1][4] is False

    @pytest.mark.asyncio
    async def test_a_unique_violation_on_a_non_primary_insert_still_raises(
        self, db, cursor
    ):
        from psycopg.errors import UniqueViolation

        cursor.execute.side_effect = UniqueViolation()
        with pytest.raises(UniqueViolation):
            await C.create_computer(USER_ID)


class TestPublishFanOut:
    """The pub/sub channel is still keyed by workspace, so a computer-entry
    writer has to wake every workspace it moved or a start waiter hangs until
    its poll."""

    @pytest.mark.asyncio
    async def test_a_status_move_wakes_every_shadowed_workspace(self, db, cursor):
        cursor.fetchone.return_value = _computer_row(
            status="stopped", shadowed_workspace_ids=[WORKSPACE_ID, "ws-2"]
        )
        await C.update_computer_status(COMPUTER_ID, "stopped")
        assert [c.args for c in db.await_args_list] == [
            (WORKSPACE_ID, "stopped"),
            ("ws-2", "stopped"),
        ]

    @pytest.mark.asyncio
    async def test_a_refused_move_wakes_nobody(self, db, cursor):
        cursor.fetchone.return_value = None
        assert await C.update_computer_status(COMPUTER_ID, "stopped") is None
        db.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_bind_publishes_running(self, db, cursor):
        await C.try_bind_computer_provider_ref(
            COMPUTER_ID,
            provider_ref="sbx-2",
            expected_previous_provider_ref="sbx-1",
            platform_secret_version=1,
        )
        assert [c.args for c in db.await_args_list] == [(WORKSPACE_ID, "running")]


class TestAdvisoryKey:
    def test_it_is_domain_separated_not_a_bare_hash(self):
        """The older workspace locks hash an unprefixed id into a 32-bit space
        four domains share; a computer key colliding with a workspace key would
        make two unrelated writers wait on each other."""
        from src.server.services.writer_guard import advisory_key

        assert C.computer_advisory_key(COMPUTER_ID) == advisory_key("C", COMPUTER_ID)
        keys = {
            C.computer_advisory_key(COMPUTER_ID),
            advisory_key("EG", COMPUTER_ID),
            advisory_key("PS", COMPUTER_ID),
            advisory_key("T", COMPUTER_ID),
        }
        assert len(keys) == 4


class TestOneWriterOfTheBinding:
    """``workspaces.sandbox_id`` is the shadow of ``computers.provider_ref``.

    A second writer anywhere makes the two rows able to disagree, and the
    disagreement is invisible until a start hands out a machine another worker
    owns. The bind statement is built in one place so the entry points cannot
    drift; this walks the tree for anyone who wrote their own.
    """

    _ALLOWED = {
        "src/server/database/computer.py",  # builds the one bind statement
        "src/server/database/workspace.py",  # the workspace-entry wrapper
    }
    # Deliberately newline-insensitive: these statements are multi-line
    # f-strings, so a line-based search walks straight past
    # "SET status = 'running',\n sandbox_id = %(provider_ref)s".
    _WRITES = (
        re.compile(r"update\s+workspaces\b.{0,600}?\bsandbox_id\s*=", re.I | re.S),
        re.compile(r"insert\s+into\s+workspaces\s*\([^)]*\bsandbox_id\b", re.I | re.S),
    )

    def test_nothing_else_writes_workspaces_sandbox_id(self):
        root = Path(__file__).resolve().parents[4]
        offenders = []
        for path in sorted((root / "src").rglob("*.py")):
            rel = path.relative_to(root).as_posix()
            if rel in self._ALLOWED:
                continue
            text = path.read_text(encoding="utf-8")
            if "sandbox_id" not in text:
                continue
            for pattern in self._WRITES:
                match = pattern.search(text)
                if match:
                    offenders.append(f"{rel}: {' '.join(match.group(0).split())}")
                    break
        assert not offenders, (
            "workspaces.sandbox_id has exactly one writer, the bind statement "
            "in database/computer.py reached through workspace.py's wrapper. "
            "Route the write there:\n  " + "\n  ".join(offenders)
        )

    def test_the_two_entry_points_share_one_statement_builder(self):
        """Same fences, one source, so neither can be relaxed on its own."""
        ws = C.bind_provider_ref_statement(authority="workspace", returning="1")
        comp = C.bind_provider_ref_statement(authority="computer", returning="1")
        for sql in (ws, comp):
            # Both sides of the shadow carry both fences, from the one constant.
            assert f"c.{C.FENCE_BINDABLE}" in sql
            assert f"w.{C.FENCE_BINDABLE}" in sql
            assert "c.provider_ref IS NOT DISTINCT FROM %(expected_previous)s" in sql
            assert "w.sandbox_id IS NOT DISTINCT FROM %(expected_previous)s" in sql
        with pytest.raises(ValueError, match="authority"):
            C.bind_provider_ref_statement(authority="nonsense", returning="1")

    def test_a_workspace_with_no_computer_keeps_the_pre_computer_predicate(self):
        """The whole backward-compatibility story: flash workspaces and the
        backfill's shared-sandbox losers still bind through this one statement."""
        sql = C.bind_provider_ref_statement(authority="workspace", returning="1")
        assert "(w.computer_id IS NULL OR m.mirrored > 0)" in re.sub(
            r"\s+", " ", sql
        )


class TestLayoutVersionStamp:
    """The host's record of what the sandbox filesystem looks like.

    The column exists so the consolidation job and the reaper can read a
    machine's layout without booting it, which only holds if the value is
    written from an observation and never defaulted.
    """

    @pytest.mark.asyncio
    async def test_it_writes_only_when_the_value_moves(self, db, cursor):
        cursor.fetchone.return_value = {"computer_id": COMPUTER_ID}
        assert await C.stamp_computer_layout_version(COMPUTER_ID, 4) is True

        sql = _sql(cursor)
        assert "UPDATE computers SET layout_version = %(version)s" in sql
        # No write when the row already says 4: the sync path runs this on
        # every acquisition, so an unconditional UPDATE would churn the row.
        assert "layout_version IS DISTINCT FROM %(version)s" in sql
        assert _params(cursor)["version"] == 4

    @pytest.mark.asyncio
    async def test_a_tombstoned_computer_is_never_stamped(self, db, cursor):
        await C.stamp_computer_layout_version(COMPUTER_ID, 4)
        assert "status <> 'deleted'" in _sql(cursor)

    @pytest.mark.asyncio
    async def test_no_matching_row_reads_as_not_written(self, db, cursor):
        cursor.fetchone.return_value = None
        assert await C.stamp_computer_layout_version(COMPUTER_ID, 4) is False

    @pytest.mark.asyncio
    async def test_a_malformed_id_does_not_reach_the_database(self, db, cursor):
        assert await C.stamp_computer_layout_version("not-a-uuid", 4) is False
        cursor.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_the_workspaces_shadow_has_no_column_to_follow(self, db, cursor):
        # Unlike the scalar setters, this one is computer-only on purpose:
        # layout is a property of the machine's filesystem, not of a project.
        await C.stamp_computer_layout_version(COMPUTER_ID, 4)
        assert "UPDATE workspaces" not in _sql(cursor)

    def test_zero_reads_as_unobserved_rather_than_as_version_zero(self):
        """046 backfills every machine at 0 because ``workspaces`` never
        recorded a layout, so a reader that treats 0 as a real version compares
        a machine at v3 against a claim that it is at v0."""
        assert C.observed_layout_version(_computer_row(layout_version=0)) is None
        assert C.observed_layout_version(_computer_row(layout_version=None)) is None
        assert C.observed_layout_version(_computer_row(layout_version=3)) == 3


class TestMcpConfigVersionStamp:
    """The machine's record of which config generation its wrapper union was built from.

    An observation, like the layout stamp beside it: it is written after an
    asset sync rebuilds ``_internal/tools/``, so it trails every config write
    and nothing may gate on it. The egress grant CAS did, and a sole-workspace
    machine then refused every resolve forever, because the refusal is what
    skips the asset sync that would have advanced the column.
    """

    @pytest.mark.asyncio
    async def test_a_sole_workspace_lends_its_own_version(self, db, cursor):
        cursor.fetchone.return_value = {"mcp_config_version": 4}
        assert await C.stamp_computer_mcp_config_version(COMPUTER_ID) == 4

        sql = _sql(cursor)
        assert "WHEN live.n = 1 THEN COALESCE(live.version, 0)" in sql
        assert "min(w.mcp_config_version) AS version" in sql

    @pytest.mark.asyncio
    async def test_a_shared_machine_records_zero(self, db, cursor):
        # Two projects bumping their own counters reach no shared generation,
        # so 0 says "no machine-wide answer" rather than "generation zero".
        cursor.fetchone.return_value = {"mcp_config_version": 0}
        assert await C.stamp_computer_mcp_config_version(COMPUTER_ID) == 0
        assert "ELSE 0" in _sql(cursor)

    @pytest.mark.asyncio
    async def test_only_live_workspaces_are_counted(self, db, cursor):
        # A tombstoned project left in the count would hold a machine at 0
        # after its last real sibling made it sole.
        await C.stamp_computer_mcp_config_version(COMPUTER_ID)
        sql = _sql(cursor)
        assert "w.status <> 'deleted'" in sql
        assert "c.status <> 'deleted'" in sql

    @pytest.mark.asyncio
    async def test_it_writes_only_when_the_value_moves(self, db, cursor):
        await C.stamp_computer_mcp_config_version(COMPUTER_ID)
        assert "mcp_config_version IS DISTINCT FROM CASE" in _sql(cursor)

    @pytest.mark.asyncio
    async def test_no_matching_row_reads_as_nothing_stamped(self, db, cursor):
        cursor.fetchone.return_value = None
        assert await C.stamp_computer_mcp_config_version(COMPUTER_ID) is None

    @pytest.mark.asyncio
    async def test_a_malformed_id_does_not_reach_the_database(self, db, cursor):
        assert await C.stamp_computer_mcp_config_version("not-a-uuid") is None
        cursor.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_the_workspace_counter_is_never_written_back(self, db, cursor):
        # One direction only: ``workspaces.mcp_config_version`` is what the
        # config writers advance and what the grant CAS gates on, so a stamp
        # that touched it would overwrite a version no writer moved.
        await C.stamp_computer_mcp_config_version(COMPUTER_ID)
        sql = _sql(cursor)
        assert "UPDATE computers" in sql
        assert "UPDATE workspaces" not in sql
