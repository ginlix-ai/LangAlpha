"""The spec change record's SQL: one claim, one heartbeat, one settle, one clock.

A mock cannot show a conditional UPDATE holds, so these lock the statements:
the claim and every read agree on staleness because both use the same SQL
predicate, a takeover and a failed settle each revert the tier in the one
statement that writes the record and keeps the workspace shadows in step, and
a heartbeat lands only on the claim that is still the row's.
"""

from __future__ import annotations

import pytest
from psycopg.types.json import Json

from src.server.database import computer_spec_change as S
from src.server.database.sql_fences import (
    COMPUTER_COLS,
    FENCE_NOT_DELETED,
    SPEC_CHANGE_HEARTBEAT_SECONDS,
    SPEC_CHANGE_STALE_SECONDS,
    computer_cols,
    spec_change_stale,
)
from src.server.models.computer import ComputerSpecChange
from tests.unit.server.database import test_computer_db as base
from tests.unit.server.database.test_computer_db import COMPUTER_ID, _params, _sql

# The same patched pool the computer layer's own contract tests use.
cursor = base.cursor
db = base.db


class TestStaleness:
    def test_every_computer_read_projects_the_stale_flag(self):
        assert COMPUTER_COLS.rstrip().endswith(
            f"{spec_change_stale()} AS spec_change_stale"
        )
        assert computer_cols("c").rstrip().endswith(
            f"{spec_change_stale('c.')} AS spec_change_stale"
        )

    def test_stale_is_decided_on_the_database_clock(self):
        predicate = spec_change_stale()
        assert "NOW()" in predicate
        assert f"secs => {SPEC_CHANGE_STALE_SECONDS}" in predicate
        assert "spec_change->>'state' = 'in_progress'" in predicate
        # A row without a change reads false, never NULL.
        assert predicate.startswith("COALESCE(")

    def test_stale_is_aged_from_the_last_heartbeat(self):
        """A change is stale a few missed heartbeats after its last one, not a
        fixed time after it started: a long backup and restore stays owned for
        as long as its worker reports in. A record from before heartbeats
        ages from its start."""
        predicate = spec_change_stale()
        assert (
            "COALESCE(spec_change->>'heartbeat_at', spec_change->>'started_at')"
            "::timestamptz" in predicate
        )
        assert SPEC_CHANGE_STALE_SECONDS <= 6 * SPEC_CHANGE_HEARTBEAT_SECONDS


class TestRecordKeys:
    def test_the_records_and_the_sql_name_only_model_fields(self):
        fields = set(ComputerSpecChange.model_fields)
        claim = S.claim_record("performance", "c-1")
        outcome = S.outcome_record("failed", None)
        assert set(claim) <= fields
        assert set(outcome) <= fields
        # The rest are written in SQL; together they fill the model.
        assert (
            set(claim)
            | set(outcome)
            | {"from_tier", "started_at", "heartbeat_at", "took_over"}
            == fields
        )


@pytest.mark.asyncio
class TestClaim:
    async def test_takes_over_only_a_finished_or_stale_change(self, cursor, db):
        await S.claim_computer_spec_change(COMPUTER_ID, target_tier="performance")
        sql = _sql(cursor)
        comp, _, shadow = sql.partition("shadow AS")
        assert "c.spec_change->>'state' IS DISTINCT FROM 'in_progress'" in comp
        assert spec_change_stale("c.") in comp
        assert f"c.{FENCE_NOT_DELETED}" in comp
        # started_at and the first heartbeat come from the row's clock.
        assert "'started_at', to_jsonb(NOW()), 'heartbeat_at', to_jsonb(NOW())" in comp
        assert shadow

    async def test_a_takeover_reverts_the_stale_changes_tier_in_the_claim(
        self, cursor, db
    ):
        """The dead worker persisted its target tier before it died, so the row
        reads a size the machine may never have reached. The claim puts the
        tier back where that change started, in the same statement, and names
        that tier as its from_tier, so the change it becomes is a real one."""
        await S.claim_computer_spec_change(COMPUTER_ID, target_tier="performance")
        sql = _sql(cursor)
        comp, _, shadow = sql.partition("shadow AS")
        revert = (
            f"CASE WHEN {spec_change_stale('c.')} THEN"
            " CASE WHEN c.resource_tier = c.spec_change->>'target_tier'"
            " THEN COALESCE(c.spec_change->>'from_tier', c.resource_tier)"
            " ELSE c.resource_tier END"
            " ELSE c.resource_tier END"
        )
        assert f"resource_tier = {revert}" in comp
        assert f"'from_tier', COALESCE({revert}, 'standard')" in comp
        # A fresh claim over a finished change leaves the tier as it is: the
        # CASE only fires on a stale record still reading its target.
        assert "ELSE c.resource_tier END" in comp
        # The workspace shadows follow the tier the claim lands on.
        assert "resource_tier = comp.resource_tier" in shadow

    async def test_each_claim_gets_its_own_id(self, cursor, db):
        await S.claim_computer_spec_change(COMPUTER_ID, target_tier="performance")
        first = _params(cursor)["record"]
        await S.claim_computer_spec_change(COMPUTER_ID, target_tier="performance")
        second = _params(cursor)["record"]
        assert isinstance(first, Json)
        assert first.obj["claim_id"] and first.obj["claim_id"] != second.obj["claim_id"]
        assert first.obj["state"] == "in_progress"

    async def test_a_held_row_answers_none(self, cursor, db):
        cursor.fetchone.return_value = None
        assert (
            await S.claim_computer_spec_change(COMPUTER_ID, target_tier="max") is None
        )


@pytest.mark.asyncio
class TestHeartbeat:
    async def test_lands_only_on_the_claim_that_still_holds_the_row(self, cursor, db):
        cursor.rowcount = 1
        assert await S.heartbeat_computer_spec_change(COMPUTER_ID, claim_id="c-1")
        sql = _sql(cursor)
        assert sql.lstrip().startswith("UPDATE computers SET spec_change = spec_change ||")
        assert "jsonb_build_object('heartbeat_at', to_jsonb(NOW()))" in sql
        assert "spec_change->>'claim_id' = %(claim_id)s" in sql
        assert "spec_change->>'state' = 'in_progress'" in sql
        # An observation, not an edit: the row's updated_at is left alone.
        assert "updated_at" not in sql
        assert _params(cursor)["claim_id"] == "c-1"

    async def test_a_taken_over_claim_learns_it_from_the_miss(self, cursor, db):
        cursor.rowcount = 0
        assert not await S.heartbeat_computer_spec_change(COMPUTER_ID, claim_id="old")


@pytest.mark.asyncio
class TestSettle:
    async def test_success_is_a_plain_update_on_its_own_claim(self, cursor, db):
        await S.settle_computer_spec_change(COMPUTER_ID, claim_id="c-1")
        sql = _sql(cursor)
        assert sql.lstrip().startswith("UPDATE computers c SET spec_change")
        assert "resource_tier" not in sql.split("RETURNING")[0]
        assert "c.spec_change->>'claim_id' = %(claim_id)s" in sql
        assert "c.spec_change->>'state' = 'in_progress'" in sql
        params = _params(cursor)
        assert params["claim_id"] == "c-1"
        assert params["outcome"].obj == {"state": "succeeded", "error": None}

    async def test_failure_writes_outcome_and_revert_in_one_statement(
        self, cursor, db
    ):
        error = {"code": "unknown", "message": "m", "files": []}
        await S.settle_computer_spec_change(COMPUTER_ID, claim_id="c-1", error=error)
        assert cursor.execute.await_count == 1
        sql = _sql(cursor)
        comp, _, shadow = sql.partition("shadow AS")
        # The outcome and the revert are the same UPDATE of computers.
        assert "spec_change = c.spec_change ||" in comp
        assert (
            "resource_tier = CASE WHEN c.resource_tier = c.spec_change->>'target_tier'"
            " THEN COALESCE(c.spec_change->>'from_tier', c.resource_tier)"
            " ELSE c.resource_tier END"
        ) in comp
        assert "c.spec_change->>'claim_id' = %(claim_id)s" in comp
        # ...and the workspace shadows follow the tier it lands on.
        assert "resource_tier = comp.resource_tier" in shadow
        assert _params(cursor)["outcome"].obj == {"state": "failed", "error": error}

    async def test_a_taken_over_claim_settles_nothing(self, cursor, db):
        cursor.fetchone.return_value = None
        assert (
            await S.settle_computer_spec_change(COMPUTER_ID, claim_id="old") is None
        )
