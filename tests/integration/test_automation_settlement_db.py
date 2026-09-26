"""Integration tests for settling automation firings against real PostgreSQL:
what ending a firing writes in its one transaction, why it failed, and the
sweep for firings whose process went away."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def _automation(user_id, **overrides):
    from src.server.database.automation import create_automation

    fields = dict(
        user_id=user_id,
        name="Settle Test",
        trigger_type="cron",
        instruction="run me",
        cron_expression="0 9 * * *",
    )
    fields.update(overrides)
    auto = await create_automation(**fields)
    return str(auto["automation_id"])


async def _firing(db, automation_id, *, status="running", heartbeat="NOW()", age="0"):
    """An execution row as a process left it: its status, its heartbeat
    (an SQL expression, or NULL), and how many seconds ago it was created."""
    from uuid import uuid4

    execution_id = str(uuid4())
    async with db() as conn, conn.cursor() as cur:
        await cur.execute(
            f"""
            INSERT INTO automation_executions (
                automation_execution_id, automation_id, status, scheduled_at,
                server_id, created_at, heartbeat_at
            )
            VALUES (%s, %s, %s, NOW(), 'server-1',
                    NOW() - make_interval(secs => %s), {heartbeat})
            """,
            (execution_id, automation_id, status, int(age)),
        )
    return execution_id


async def _status(db, execution_id):
    async with db() as conn, conn.cursor() as cur:
        await cur.execute(
            "SELECT status, skip_reason, error_message FROM automation_executions "
            "WHERE automation_execution_id = %s",
            (execution_id,),
        )
        row = await cur.fetchone()
        return (row["status"], row["skip_reason"], row["error_message"])


async def _sql(db, query, params):
    async with db() as conn, conn.cursor() as cur:
        await cur.execute(query, params)


_STALE = "NOW() - INTERVAL '10 minutes'"


class TestSweepBoundaries:
    """Which rows the sweep takes, and which it leaves."""

    async def test_only_quiet_unsettled_firings_are_abandoned(
        self, seed_user, patched_get_db_connection
    ):
        db = patched_get_db_connection
        from src.server.database.automation import list_abandoned_executions

        aid = await _automation(seed_user["user_id"])
        stale_running = await _firing(db, aid, heartbeat=_STALE)
        stale_waiting = await _firing(db, aid, status="waiting", heartbeat=_STALE)
        await _firing(db, aid)  # fresh
        await _firing(db, aid, heartbeat="NULL")  # young, no heartbeat
        await _firing(db, aid, heartbeat="NULL", age=2 * 86400)  # old, no heartbeat
        await _firing(db, aid, status="completed", heartbeat=_STALE)

        rows = await list_abandoned_executions(300)

        assert {str(r["automation_execution_id"]) for r in rows} == {
            stale_running,
            stale_waiting,
        }
        assert all(r["user_id"] == seed_user["user_id"] for r in rows)

    async def test_legacy_rows_close_after_a_day_without_touching_the_automation(
        self, seed_user, patched_get_db_connection
    ):
        db = patched_get_db_connection
        from src.server.database.automation import (
            get_automation,
            settle_legacy_executions,
        )

        aid = await _automation(seed_user["user_id"])
        young = await _firing(db, aid, heartbeat="NULL")
        old_running = await _firing(db, aid, heartbeat="NULL", age=2 * 86400)
        stale = await _firing(db, aid, heartbeat=_STALE, age=2 * 86400)

        assert await settle_legacy_executions("restarted") == 1

        assert await _status(db, old_running) == ("failed", None, "restarted")
        assert (await _status(db, young))[0] == "running"
        assert (await _status(db, stale))[0] == "running"
        auto = await get_automation(aid, seed_user["user_id"])
        assert auto["failure_count"] == 0
        assert auto["status"] == "active"

    async def test_a_heartbeat_that_came_back_is_not_swept(
        self, seed_user, patched_get_db_connection
    ):
        db = patched_get_db_connection
        from src.server.database.automation import settle_execution

        aid = await _automation(seed_user["user_id"])
        eid = await _firing(db, aid)

        assert await settle_execution(
            eid, automation_id=aid, from_statuses=("running",), to="failed",
            quiet_for=300,
        ) is None
        assert (await _status(db, eid))[0] == "running"

    async def test_concurrent_sweeps_settle_a_firing_once(
        self, seed_user, patched_get_db_connection
    ):
        db = patched_get_db_connection
        import asyncio

        from src.server.database.automation import get_automation, settle_execution

        aid = await _automation(seed_user["user_id"], max_failures=5)
        eid = await _firing(db, aid, heartbeat=_STALE)

        results = await asyncio.gather(*(
            settle_execution(
                eid, automation_id=aid, from_statuses=("pending", "running"),
                to="failed", strike="count", quiet_for=300,
                error_message="restarted",
            )
            for _ in range(4)
        ))

        assert sum(r is not None for r in results) == 1
        auto = await get_automation(aid, seed_user["user_id"])
        assert auto["failure_count"] == 1


class TestSettleExecution:
    """What ending a firing writes, in one transaction."""

    async def test_strikes_count_up_to_the_limit_and_say_why(
        self, seed_user, patched_get_db_connection
    ):
        from src.server.database.automation import get_automation, settle_execution

        auto_id = await _automation(seed_user["user_id"], max_failures=2)

        # The second failure reaches max_failures=2 and disables it.
        for count, status in ((1, "active"), (2, "disabled")):
            eid = await _firing(patched_get_db_connection, auto_id)
            await settle_execution(
                eid, automation_id=auto_id, from_statuses=("running",),
                to="failed", strike="count",
            )
            auto = await get_automation(auto_id, seed_user["user_id"])
            assert (auto["failure_count"], auto["status"]) == (count, status)

        assert auto["next_run_at"] is None
        assert auto["disable_reason"] == "max_failures"

    async def test_strike_disables_at_the_limit(
        self, seed_user, patched_get_db_connection
    ):
        db = patched_get_db_connection
        from src.server.database.automation import get_automation, settle_execution

        aid = await _automation(seed_user["user_id"], max_failures=1)
        eid = await _firing(db, aid)

        row = await settle_execution(
            eid, automation_id=aid, from_statuses=("running",), to="failed",
            strike="count", error_message="boom",
        )

        assert row["settled_from"] == "running"
        auto = await get_automation(aid, seed_user["user_id"])
        assert (auto["status"], auto["failure_count"], auto["next_run_at"]) == (
            "disabled", 1, None,
        )

    async def test_a_claimed_once_firing_closes_its_automation_and_a_manual_one_does_not(
        self, seed_user, patched_get_db_connection
    ):
        from src.server.database.automation import (
            claim_due_automations,
            create_execution,
            get_automation,
            settle_execution,
            transition_execution,
        )

        past = datetime(2020, 1, 1, tzinfo=timezone.utc)
        aid = await _automation(
            seed_user["user_id"], trigger_type="once", cron_expression=None,
            next_run_at=past,
        )
        manual = await create_execution(aid, datetime.now(timezone.utc), "server-1")
        await transition_execution(manual, from_statuses=("pending",), to="running")
        await settle_execution(
            manual, automation_id=aid, from_statuses=("running",), to="completed",
            strike="reset", schedule="close_once",
        )
        assert (await get_automation(aid, seed_user["user_id"]))["status"] == "active"

        [claimed] = await claim_due_automations(datetime.now(timezone.utc), "server-1")
        eid = claimed["_execution_id"]
        await transition_execution(eid, from_statuses=("pending",), to="running")
        await settle_execution(
            eid, automation_id=aid, from_statuses=("running",), to="completed",
            strike="reset", schedule="close_once",
        )
        auto = await get_automation(aid, seed_user["user_id"])
        assert auto["status"] == "completed"

    async def test_a_price_close_waits_for_its_executing_status(
        self, seed_user, patched_get_db_connection
    ):
        db = patched_get_db_connection
        from src.server.database.automation import (
            get_automation,
            settle_execution,
            update_automation,
        )

        aid = await _automation(
            seed_user["user_id"], trigger_type="price", cron_expression=None,
            trigger_config={"symbol": "AAPL", "conditions": [{"type": "price_above", "value": 1}]},
        )
        await update_automation(aid, seed_user["user_id"], status="executing")
        # Paused while the alert's firing ran: the pause wins.
        await update_automation(aid, seed_user["user_id"], status="paused")
        eid = await _firing(db, aid)
        await settle_execution(
            eid, automation_id=aid, from_statuses=("running",), to="completed",
            schedule="close_price",
        )
        assert (await get_automation(aid, seed_user["user_id"]))["status"] == "paused"

    async def test_a_manual_run_leaves_a_price_firing_its_claim(
        self, seed_user, patched_get_db_connection
    ):
        from src.server.database.automation import (
            claim_price_firing,
            create_execution,
            get_automation,
            settle_execution,
            transition_execution,
            update_automation,
        )

        uid = seed_user["user_id"]
        aid = await _automation(
            uid, trigger_type="price", cron_expression=None,
            trigger_config={"symbol": "AAPL", "conditions": [{"type": "price_above", "value": 1}]},
        )
        fired = await claim_price_firing(aid, "server-1")
        manual = await create_execution(aid, datetime.now(timezone.utc), "server-1")
        for eid in (fired, manual):
            await transition_execution(eid, from_statuses=("pending",), to="running")

        # The manual run ends first; the alert's own firing is still running.
        await settle_execution(
            manual, automation_id=aid, from_statuses=("running",), to="completed",
            schedule="rearm_price",
        )
        assert (await get_automation(aid, uid))["status"] == "executing"
        await settle_execution(
            fired, automation_id=aid, from_statuses=("running",), to="completed",
            schedule="rearm_price",
        )
        assert (await get_automation(aid, uid))["status"] == "active"

        # Paused after the monitor loaded it: the claim refuses, the pause holds.
        await update_automation(aid, uid, status="paused")
        assert await claim_price_firing(aid, "server-1") is None
        assert (await get_automation(aid, uid))["status"] == "paused"

    async def test_owner_scoping(
        self, seed_user, patched_get_db_connection
    ):
        db = patched_get_db_connection
        from src.server.database.automation import (
            get_execution_status,
            list_executions,
            settle_execution,
            transition_execution,
        )

        aid = await _automation(seed_user["user_id"])
        other = await _automation(seed_user["user_id"], name="Other")
        eid = await _firing(db, aid)

        executions, total = await list_executions("different-user-id")
        assert (executions, total) == ([], 0)
        executions, total = await list_executions(
            "different-user-id", automation_id=aid
        )
        assert (executions, total) == ([], 0)
        assert await transition_execution(
            eid, from_statuses=("running",), to="waiting", automation_id=other,
        ) is None
        assert await settle_execution(
            eid, automation_id=other, from_statuses=("running",), to="skipped",
        ) is None
        assert await get_execution_status(eid, automation_id=other) is None
        assert await get_execution_status(eid, automation_id=aid) == "running"
        assert (await _status(db, eid))[0] == "running"

    async def test_of_two_firings_waiting_together_one_gives_way(
        self, seed_user, patched_get_db_connection
    ):
        db = patched_get_db_connection
        from src.server.database.automation import has_earlier_waiting_execution

        aid = await _automation(seed_user["user_id"])
        first = await _firing(db, aid, status="waiting")
        second = await _firing(db, aid, status="waiting")
        # The same creation time, as two firings of one claim would have.
        await _sql(
            db,
            "UPDATE automation_executions SET created_at = NOW() "
            "WHERE automation_id = %s",
            (aid,),
        )

        gives_way = [
            await has_earlier_waiting_execution(aid, first),
            await has_earlier_waiting_execution(aid, second),
        ]
        assert sorted(gives_way) == [False, True]

    async def test_an_automation_leads_with_its_firing_in_flight(
        self, seed_user, patched_get_db_connection
    ):
        db = patched_get_db_connection
        from src.server.database.automation import (
            get_automation,
            list_automations,
            update_automation,
        )

        aid = await _automation(seed_user["user_id"])
        running = await _firing(db, aid, age=60)
        await _firing(db, aid, status="skipped")

        fetched = await get_automation(aid, seed_user["user_id"])
        listed, _ = await list_automations(seed_user["user_id"])
        updated = await update_automation(aid, seed_user["user_id"], name="Renamed")
        for auto in (fetched, listed[0], updated):
            assert auto["last_execution"]["automation_execution_id"] == running

        await _sql(
            db,
            "UPDATE automation_executions SET status = 'completed' "
            "WHERE automation_execution_id = %s",
            (running,),
        )
        fetched = await get_automation(aid, seed_user["user_id"])
        assert fetched["last_execution"]["status"] == "skipped"


class TestWhyAFiringFailed:
    """The failure a user acts on, and why an automation switched itself off."""

    async def _settle_failed(self, aid, db, **fields):
        from src.server.database.automation import settle_execution

        eid = await _firing(db, aid)
        row = await settle_execution(
            eid, automation_id=aid, from_statuses=("running",), to="failed",
            error_message="boom", **fields,
        )
        return eid, row

    async def test_a_rejected_key_disables_at_once_and_says_why(
        self, seed_user, patched_get_db_connection
    ):
        db = patched_get_db_connection
        from src.server.database.automation import get_automation, list_executions

        uid = seed_user["user_id"]
        aid = await _automation(uid, max_failures=5)
        eid, _ = await self._settle_failed(
            aid, db, strike="fuse", failure_reason="provider_auth"
        )

        auto = await get_automation(aid, uid)
        assert (auto["status"], auto["failure_count"], auto["next_run_at"]) == (
            "disabled", 1, None,
        )
        assert auto["disable_reason"] == "provider_auth"
        [execution], _ = await list_executions(uid, automation_id=aid)
        assert str(execution["automation_execution_id"]) == eid
        assert execution["failure_reason"] == "provider_auth"
        assert auto["last_execution"]["failure_reason"] == "provider_auth"

    async def test_the_last_strike_says_why(
        self, seed_user, patched_get_db_connection
    ):
        db = patched_get_db_connection
        from src.server.database.automation import get_automation

        uid = seed_user["user_id"]
        aid = await _automation(uid, max_failures=2)
        await self._settle_failed(aid, db, strike="count")
        auto = await get_automation(aid, uid)
        assert auto["status"] == "active"
        assert auto["disable_reason"] is None

        await self._settle_failed(aid, db, strike="count")
        auto = await get_automation(aid, uid)
        assert (auto["status"], auto["failure_count"]) == ("disabled", 2)
        assert auto["disable_reason"] == "max_failures"

    async def test_the_reason_leaves_the_metadata_alone(
        self, seed_user, patched_get_db_connection
    ):
        """The client writes metadata whole, so a reason kept there would be
        lost to the next edit that raced the disable."""
        db = patched_get_db_connection
        from src.server.database.automation import get_automation

        uid = seed_user["user_id"]
        aid = await _automation(uid, metadata={"source": "chat"})
        await self._settle_failed(aid, db, strike="fuse", failure_reason="provider_auth")

        auto = await get_automation(aid, uid)
        assert (auto["disable_reason"], auto["metadata"]) == (
            "provider_auth", {"source": "chat"},
        )

    async def test_a_success_and_a_resume_clear_the_reason(
        self, seed_user, patched_get_db_connection
    ):
        db = patched_get_db_connection
        from src.server.database.automation import (
            get_automation,
            settle_execution,
            update_automation,
        )

        uid = seed_user["user_id"]
        aid = await _automation(uid, max_failures=5)
        await _sql(
            db,
            "UPDATE automations SET disable_reason = 'max_failures', failure_count = 2 "
            "WHERE automation_id = %s",
            (aid,),
        )

        eid = await _firing(db, aid)
        await settle_execution(
            eid, automation_id=aid, from_statuses=("running",), to="completed",
            strike="reset",
        )
        auto = await get_automation(aid, uid)
        assert (auto["failure_count"], auto["disable_reason"]) == (0, None)

        # A run that ends after the automation switched off leaves the reason
        # standing: the automation is still off for it.
        await _sql(
            db,
            "UPDATE automations SET disable_reason = 'max_failures', status = 'disabled' "
            "WHERE automation_id = %s",
            (aid,),
        )
        eid = await _firing(db, aid)
        await settle_execution(
            eid, automation_id=aid, from_statuses=("running",), to="completed",
            strike="reset",
        )
        auto = await get_automation(aid, uid)
        assert auto["disable_reason"] == "max_failures"

        # The resume the handler writes: status and reason in one statement.
        await update_automation(
            aid, uid, status="active", failure_count=0, disable_reason=None
        )
        auto = await get_automation(aid, uid)
        assert (auto["status"], auto["disable_reason"]) == ("active", None)

    async def test_a_settle_reads_the_reason_before_it(
        self, seed_user, patched_get_db_connection
    ):
        db = patched_get_db_connection

        aid = await _automation(seed_user["user_id"], max_failures=5)
        _, first = await self._settle_failed(aid, db, failure_reason="usage_limit")
        _, second = await self._settle_failed(aid, db, failure_reason="usage_limit")
        _, third = await self._settle_failed(aid, db)

        assert first["previous_failure_reason"] is None
        assert second["previous_failure_reason"] == "usage_limit"
        assert third["previous_failure_reason"] == "usage_limit"

    async def test_a_repeat_reads_past_a_server_skip_and_a_success_ends_it(
        self, seed_user, patched_get_db_connection
    ):
        db = patched_get_db_connection
        from src.server.database.automation import settle_execution

        aid = await _automation(seed_user["user_id"], max_failures=5)
        await self._settle_failed(aid, db, failure_reason="usage_limit")
        busy = await _firing(db, aid, status="skipped")
        await _sql(
            db,
            "UPDATE automation_executions SET skip_reason = 'thread_busy' "
            "WHERE automation_execution_id = %s",
            (busy,),
        )
        _, repeat = await self._settle_failed(aid, db, failure_reason="usage_limit")
        assert repeat["previous_failure_reason"] == "usage_limit"

        eid = await _firing(db, aid)
        await settle_execution(
            eid, automation_id=aid, from_statuses=("running",), to="completed",
            strike="reset",
        )
        _, after = await self._settle_failed(aid, db, failure_reason="usage_limit")
        assert after["previous_failure_reason"] is None

    @pytest.mark.parametrize("status", ["paused", "completed", "disabled"])
    async def test_a_strike_leaves_an_automation_that_is_not_live_as_it_is(
        self, status, seed_user, patched_get_db_connection
    ):
        db = patched_get_db_connection
        from src.server.database.automation import get_automation

        uid = seed_user["user_id"]
        aid = await _automation(uid, max_failures=1, metadata={"source": "chat"})
        await _sql(
            db, "UPDATE automations SET status = %s WHERE automation_id = %s",
            (status, aid),
        )
        await self._settle_failed(aid, db, strike="fuse", failure_reason="provider_auth")

        auto = await get_automation(aid, uid)
        assert (auto["status"], auto["failure_count"]) == (status, 1)
        assert (auto["disable_reason"], auto["metadata"]) == (None, {"source": "chat"})

    async def test_the_last_execution_passes_over_a_skip_the_server_made(
        self, seed_user, patched_get_db_connection
    ):
        db = patched_get_db_connection
        from src.server.database.automation import get_automation

        uid = seed_user["user_id"]
        aid = await _automation(uid)
        failed = await _firing(db, aid, status="failed", age=120)
        busy = await _firing(db, aid, status="skipped", age=60)
        stopped = await _firing(db, aid, status="skipped")
        await _sql(
            db,
            "UPDATE automation_executions SET skip_reason = 'thread_busy' "
            "WHERE automation_execution_id = %s",
            (busy,),
        )
        await _sql(
            db,
            "UPDATE automation_executions SET skip_reason = 'interrupted' "
            "WHERE automation_execution_id = %s",
            (stopped,),
        )

        last = (await get_automation(aid, uid))["last_execution"]
        assert str(last["automation_execution_id"]) == failed

        # A skip the user made is theirs to see.
        await _sql(
            db,
            "UPDATE automation_executions SET skip_reason = 'user' "
            "WHERE automation_execution_id = %s",
            (busy,),
        )
        last = (await get_automation(aid, uid))["last_execution"]
        assert str(last["automation_execution_id"]) == busy

        # Nothing but skips the server made: the newest stands.
        await _sql(
            db,
            "UPDATE automation_executions SET status = 'skipped', "
            "skip_reason = 'thread_busy' WHERE automation_execution_id IN (%s, %s)",
            (failed, busy),
        )
        last = (await get_automation(aid, uid))["last_execution"]
        assert str(last["automation_execution_id"]) == stopped
