"""Integration tests for automation CRUD and execution history against real PostgreSQL."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


class TestCreateAutomation:
    """Test automation creation."""

    async def test_create_cron_automation(
        self, seed_user, patched_get_db_connection
    ):
        from src.server.database.automation import create_automation

        auto = await create_automation(
            user_id=seed_user["user_id"],
            name="Daily Market Briefing",
            trigger_type="cron",
            instruction="Summarize today's market highlights",
            cron_expression="0 9 * * 1-5",
            timezone="America/New_York",
            agent_mode="flash",
        )

        assert auto["name"] == "Daily Market Briefing"
        assert auto["trigger_type"] == "cron"
        assert auto["cron_expression"] == "0 9 * * 1-5"
        assert auto["timezone"] == "America/New_York"
        assert auto["agent_mode"] == "flash"
        assert auto["status"] == "active"
        assert auto["failure_count"] == 0
        assert auto["max_failures"] == 3

    async def test_create_once_automation(
        self, seed_user, patched_get_db_connection
    ):
        from src.server.database.automation import create_automation

        run_at = datetime(2025, 6, 15, 14, 0, tzinfo=timezone.utc)
        auto = await create_automation(
            user_id=seed_user["user_id"],
            name="One-time Report",
            trigger_type="once",
            instruction="Generate Q2 earnings report",
            next_run_at=run_at,
        )

        assert auto["trigger_type"] == "once"
        assert auto["next_run_at"] == run_at


class TestGetAndListAutomations:
    """Test automation retrieval and listing."""

    async def test_get_automation(
        self, seed_user, patched_get_db_connection
    ):
        from src.server.database.automation import create_automation, get_automation

        created = await create_automation(
            user_id=seed_user["user_id"],
            name="Test Get",
            trigger_type="cron",
            instruction="test",
            cron_expression="0 * * * *",
        )

        fetched = await get_automation(
            str(created["automation_id"]), seed_user["user_id"]
        )

        assert fetched is not None
        assert fetched["name"] == "Test Get"

    async def test_get_automation_wrong_user(
        self, seed_user, patched_get_db_connection
    ):
        from src.server.database.automation import create_automation, get_automation

        created = await create_automation(
            user_id=seed_user["user_id"],
            name="Wrong User",
            trigger_type="cron",
            instruction="test",
        )

        result = await get_automation(
            str(created["automation_id"]), "different-user-id"
        )
        assert result is None

    async def test_list_automations(
        self, seed_user, patched_get_db_connection
    ):
        from src.server.database.automation import create_automation, list_automations

        for i in range(3):
            await create_automation(
                user_id=seed_user["user_id"],
                name=f"Auto {i}",
                trigger_type="cron",
                instruction=f"task {i}",
            )

        items, total = await list_automations(seed_user["user_id"])
        assert total == 3
        assert len(items) == 3

    async def test_list_automations_with_status_filter(
        self, seed_user, patched_get_db_connection
    ):
        from src.server.database.automation import (
            create_automation,
            list_automations,
            update_automation,
        )

        a1 = await create_automation(
            user_id=seed_user["user_id"],
            name="Active One",
            trigger_type="cron",
            instruction="active",
        )
        a2 = await create_automation(
            user_id=seed_user["user_id"],
            name="Paused One",
            trigger_type="cron",
            instruction="paused",
        )

        await update_automation(
            str(a2["automation_id"]),
            seed_user["user_id"],
            status="paused",
        )

        active_items, active_total = await list_automations(
            seed_user["user_id"], status="active"
        )
        assert active_total == 1
        assert active_items[0]["name"] == "Active One"

        paused_items, paused_total = await list_automations(
            seed_user["user_id"], status="paused"
        )
        assert paused_total == 1
        assert paused_items[0]["name"] == "Paused One"


class TestUpdateAutomation:
    """Test automation updates."""

    async def test_update_automation_fields(
        self, seed_user, patched_get_db_connection
    ):
        from src.server.database.automation import create_automation, update_automation

        auto = await create_automation(
            user_id=seed_user["user_id"],
            name="Original",
            trigger_type="cron",
            instruction="original instruction",
        )

        updated = await update_automation(
            str(auto["automation_id"]),
            seed_user["user_id"],
            name="Updated",
            instruction="updated instruction",
            cron_expression="30 8 * * *",
        )

        assert updated is not None
        assert updated["name"] == "Updated"
        assert updated["instruction"] == "updated instruction"
        assert updated["cron_expression"] == "30 8 * * *"

    async def test_update_automation_status(
        self, seed_user, patched_get_db_connection
    ):
        from src.server.database.automation import create_automation, update_automation

        auto = await create_automation(
            user_id=seed_user["user_id"],
            name="Status Test",
            trigger_type="cron",
            instruction="test",
        )

        updated = await update_automation(
            str(auto["automation_id"]),
            seed_user["user_id"],
            status="paused",
        )

        assert updated["status"] == "paused"


class TestDeleteAutomation:
    """Test automation deletion."""

    async def test_delete_automation(
        self, seed_user, patched_get_db_connection
    ):
        from src.server.database.automation import (
            create_automation,
            delete_automation,
            get_automation,
        )

        auto = await create_automation(
            user_id=seed_user["user_id"],
            name="To Delete",
            trigger_type="cron",
            instruction="delete me",
        )

        deleted = await delete_automation(
            str(auto["automation_id"]), seed_user["user_id"]
        )
        assert deleted is True

        result = await get_automation(
            str(auto["automation_id"]), seed_user["user_id"]
        )
        assert result is None

    async def test_delete_nonexistent(
        self, seed_user, patched_get_db_connection
    ):
        import uuid

        from src.server.database.automation import delete_automation

        deleted = await delete_automation(
            str(uuid.uuid4()), seed_user["user_id"]
        )
        assert deleted is False


class TestPriceTriggerAutomation:
    """Test price-triggered automation CRUD and query."""

    async def test_create_price_automation(
        self, seed_user, patched_get_db_connection
    ):
        from src.server.database.automation import create_automation

        trigger_config = {
            "symbol": "AAPL",
            "conditions": [
                {"type": "price_below", "value": 150.0, "reference": "previous_close"}
            ],
            "retrigger": {"mode": "one_shot", "cooldown_seconds": 3600},
        }

        auto = await create_automation(
            user_id=seed_user["user_id"],
            name="AAPL Price Alert",
            trigger_type="price",
            instruction="Analyze why AAPL dropped below $150",
            trigger_config=trigger_config,
            agent_mode="flash",
        )

        assert auto["name"] == "AAPL Price Alert"
        assert auto["trigger_type"] == "price"
        assert auto["trigger_config"]["symbol"] == "AAPL"
        assert auto["trigger_config"]["conditions"][0]["type"] == "price_below"
        assert auto["trigger_config"]["conditions"][0]["value"] == 150.0
        assert auto["cron_expression"] is None
        assert auto["next_run_at"] is None
        assert auto["status"] == "active"

    async def test_get_active_price_automations(
        self, seed_user, patched_get_db_connection
    ):
        from src.server.database.automation import (
            create_automation,
            get_active_price_automations,
        )

        # Create two price automations and one cron automation
        await create_automation(
            user_id=seed_user["user_id"],
            name="AAPL Alert",
            trigger_type="price",
            instruction="aapl task",
            trigger_config={
                "symbol": "AAPL",
                "conditions": [{"type": "price_below", "value": 150.0}],
                "retrigger": {"mode": "one_shot"},
            },
        )
        await create_automation(
            user_id=seed_user["user_id"],
            name="TSLA Alert",
            trigger_type="price",
            instruction="tsla task",
            trigger_config={
                "symbol": "TSLA",
                "conditions": [{"type": "price_above", "value": 300.0}],
                "retrigger": {"mode": "recurring"},
            },
        )
        await create_automation(
            user_id=seed_user["user_id"],
            name="Daily Cron",
            trigger_type="cron",
            instruction="cron task",
            cron_expression="0 9 * * *",
        )

        price_autos = await get_active_price_automations()
        assert len(price_autos) == 2
        names = {a["name"] for a in price_autos}
        assert names == {"AAPL Alert", "TSLA Alert"}

    async def test_get_active_price_automations_excludes_paused(
        self, seed_user, patched_get_db_connection
    ):
        from src.server.database.automation import (
            create_automation,
            get_active_price_automations,
            update_automation,
        )

        auto = await create_automation(
            user_id=seed_user["user_id"],
            name="Paused Alert",
            trigger_type="price",
            instruction="paused task",
            trigger_config={
                "symbol": "MSFT",
                "conditions": [{"type": "price_above", "value": 400.0}],
                "retrigger": {"mode": "one_shot"},
            },
        )

        await update_automation(
            str(auto["automation_id"]),
            seed_user["user_id"],
            status="paused",
        )

        price_autos = await get_active_price_automations()
        assert len(price_autos) == 0

    async def test_update_price_automation_trigger_config(
        self, seed_user, patched_get_db_connection
    ):
        from src.server.database.automation import (
            create_automation,
            get_automation,
            update_automation,
        )

        auto = await create_automation(
            user_id=seed_user["user_id"],
            name="Update Config",
            trigger_type="price",
            instruction="test update",
            trigger_config={
                "symbol": "AAPL",
                "conditions": [{"type": "price_below", "value": 150.0}],
                "retrigger": {"mode": "one_shot"},
            },
        )

        updated = await update_automation(
            str(auto["automation_id"]),
            seed_user["user_id"],
            trigger_config={
                "symbol": "AAPL",
                "conditions": [{"type": "price_above", "value": 200.0}],
                "retrigger": {"mode": "cooldown", "cooldown_seconds": 1800},
            },
        )

        assert updated["trigger_config"]["conditions"][0]["type"] == "price_above"
        assert updated["trigger_config"]["conditions"][0]["value"] == 200.0
        assert updated["trigger_config"]["retrigger"]["mode"] == "cooldown"


class TestExecutionHistory:
    """Test automation execution record operations."""

    async def test_create_and_list_executions(
        self, seed_user, patched_get_db_connection
    ):
        from src.server.database.automation import (
            create_automation,
            create_execution,
            list_executions,
        )

        auto = await create_automation(
            user_id=seed_user["user_id"],
            name="Exec Test",
            trigger_type="cron",
            instruction="run me",
        )

        now = datetime.now(timezone.utc)
        exec_id = await create_execution(
            automation_id=str(auto["automation_id"]),
            scheduled_at=now,
            server_id="server-1",
        )

        assert exec_id is not None

        executions, total = await list_executions(
            str(auto["automation_id"]), seed_user["user_id"]
        )
        assert total == 1
        assert str(executions[0]["automation_execution_id"]) == exec_id
        assert executions[0]["status"] == "pending"
        assert executions[0]["server_id"] == "server-1"

    async def test_update_execution_status(
        self, seed_user, patched_get_db_connection
    ):
        from src.server.database.automation import (
            create_automation,
            create_execution,
            list_executions,
            update_execution_status,
        )

        auto = await create_automation(
            user_id=seed_user["user_id"],
            name="Exec Status",
            trigger_type="cron",
            instruction="status test",
        )

        now = datetime.now(timezone.utc)
        exec_id = await create_execution(
            automation_id=str(auto["automation_id"]),
            scheduled_at=now,
            server_id="server-1",
        )

        await update_execution_status(
            execution_id=exec_id,
            status="completed",
            started_at=now,
            completed_at=datetime.now(timezone.utc),
        )

        executions, _ = await list_executions(
            str(auto["automation_id"]), seed_user["user_id"]
        )
        assert executions[0]["status"] == "completed"
        assert executions[0]["started_at"] is not None
        assert executions[0]["completed_at"] is not None

    async def test_failure_count_and_auto_disable(
        self, seed_user, patched_get_db_connection
    ):
        from src.server.database.automation import (
            create_automation,
            get_automation,
            increment_failure_count,
        )

        auto = await create_automation(
            user_id=seed_user["user_id"],
            name="Failure Test",
            trigger_type="cron",
            instruction="will fail",
            max_failures=2,
        )

        auto_id = str(auto["automation_id"])

        count1 = await increment_failure_count(auto_id)
        assert count1 == 1

        # Second failure should trigger auto-disable (max_failures=2)
        count2 = await increment_failure_count(auto_id)
        assert count2 == 2

        disabled = await get_automation(auto_id, seed_user["user_id"])
        assert disabled["status"] == "disabled"
        assert disabled["next_run_at"] is None
