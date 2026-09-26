"""Tests for automation Pydantic models.

Covers request/response models in src/server/models/automation.py including
field constraints, enum literals, and defaults.
"""

import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import get_args

import pytest
from pydantic import ValidationError

from src.server.models.automation import (
    AutomationCreate,
    AutomationExecutionResponse,
    AutomationResponse,
    AutomationRunsListResponse,
    AutomationsListResponse,
    AutomationUpdate,
    ExecutionStatus,
)


NOW = datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# AutomationCreate
# ---------------------------------------------------------------------------


class TestAutomationCreate:
    """AutomationCreate construction and validation."""

    def test_valid_minimal_cron(self):
        a = AutomationCreate(
            name="Daily Report",
            trigger_type="cron",
            cron_expression="0 9 * * 1-5",
            instruction="Summarise market news",
        )
        assert a.trigger_type == "cron"
        assert a.agent_mode == "flash"
        assert a.thread_strategy == "new"
        assert a.max_failures == 3
        assert a.timezone == "UTC"

    def test_valid_once(self):
        a = AutomationCreate(
            name="One-shot",
            trigger_type="once",
            instruction="Run analysis",
            next_run_at=NOW,
        )
        assert a.trigger_type == "once"
        assert a.next_run_at == NOW

    def test_invalid_trigger_type(self):
        with pytest.raises(ValidationError):
            AutomationCreate(
                name="Bad",
                trigger_type="webhook",
                instruction="x",
            )

    def test_invalid_agent_mode(self):
        with pytest.raises(ValidationError):
            AutomationCreate(
                name="Bad",
                trigger_type="cron",
                cron_expression="0 9 * * *",
                instruction="x",
                agent_mode="turbo",
            )

    def test_max_failures_bounds(self):
        with pytest.raises(ValidationError):
            AutomationCreate(
                name="Bad",
                trigger_type="cron",
                cron_expression="0 9 * * *",
                instruction="x",
                max_failures=0,
            )
        with pytest.raises(ValidationError):
            AutomationCreate(
                name="Bad",
                trigger_type="cron",
                cron_expression="0 9 * * *",
                instruction="x",
                max_failures=101,
            )

    def test_name_too_long(self):
        with pytest.raises(ValidationError):
            AutomationCreate(
                name="x" * 256,
                trigger_type="cron",
                cron_expression="0 9 * * *",
                instruction="x",
            )

    def test_thread_strategy_values(self):
        for strategy in ("new", "continue"):
            a = AutomationCreate(
                name="A",
                trigger_type="cron",
                cron_expression="0 9 * * *",
                instruction="x",
                thread_strategy=strategy,
            )
            assert a.thread_strategy == strategy


# ---------------------------------------------------------------------------
# Trigger rules, which create and update share
# ---------------------------------------------------------------------------

_SCHEDULE = {
    "cron": {"cron_expression": "0 9 * * *"},
    "once": {"next_run_at": NOW},
    "price": {
        "trigger_config": {
            "symbol": "AAPL", "conditions": [{"type": "price_above", "value": 200}],
        },
    },
}


def _create(kind, **fields):
    return AutomationCreate(
        name="A", instruction="x", trigger_type=kind, **{**_SCHEDULE[kind], **fields}
    )


class TestTriggerRules:
    @pytest.mark.parametrize("kind", ["cron", "once", "price"])
    def test_each_kind_needs_its_schedule_field(self, kind):
        [(field, _)] = _SCHEDULE[kind].items()
        with pytest.raises(ValidationError, match=f"{field} is required"):
            _create(kind, **{field: None})

    @pytest.mark.parametrize(
        ("kind", "other"), [("cron", "once"), ("once", "price"), ("price", "cron")]
    )
    def test_a_field_of_another_kind_is_refused_on_create(self, kind, other):
        [(field, _)] = _SCHEDULE[other].items()
        with pytest.raises(ValidationError, match=f"{field} doesn't apply to a '{kind}'"):
            _create(kind, **_SCHEDULE[other])

    def test_a_time_without_an_offset_is_utc_on_create_and_update(self):
        naive = datetime(2026, 10, 28, 14, 15)
        expected = naive.replace(tzinfo=timezone.utc)
        assert _create("once", next_run_at=naive).next_run_at == expected
        assert AutomationUpdate(next_run_at=naive).next_run_at == expected

    def test_an_invalid_cron_is_refused_on_create_and_update(self):
        with pytest.raises(ValidationError, match="Invalid cron expression"):
            _create("cron", cron_expression="every morning")
        with pytest.raises(ValidationError, match="Invalid cron expression"):
            AutomationUpdate(cron_expression="every morning")

    @pytest.mark.parametrize(
        "config",
        [
            {"conditions": [{"type": "price_above", "value": 100.0}]},
            {"symbol": "AAPL"},
            {"symbol": "AAPL", "conditions": []},
            {"symbol": "AAPL", "conditions": [{"type": "invalid_type", "value": 100.0}]},
            {"symbol": "AAPL", "conditions": [{"type": "price_above", "value": 0}]},
            {"symbol": "^SPX", "conditions": [{"type": "price_above", "value": 1.0}]},
        ],
        ids=["no-symbol", "no-conditions", "empty-conditions", "bad-type", "zero-value", "caret"],
    )
    def test_a_price_config_the_monitor_would_skip_is_refused(self, config):
        """The monitor skips a config it cannot parse without a word."""
        with pytest.raises(ValidationError, match="Invalid price trigger config"):
            _create("price", trigger_config=config)

    def test_a_price_config_is_stored_as_sent(self):
        config = {"symbol": "gspc", "conditions": [{"type": "price_above", "value": 1.0}]}
        assert _create("price", trigger_config=config).trigger_config == config


# ---------------------------------------------------------------------------
# AutomationUpdate
# ---------------------------------------------------------------------------


class TestAutomationUpdate:
    """AutomationUpdate partial update model."""

    def test_empty_update(self):
        u = AutomationUpdate()
        assert u.name is None
        assert u.instruction is None
        assert u.max_failures is None

    def test_partial_update(self):
        u = AutomationUpdate(name="Renamed", max_failures=5)
        assert u.name == "Renamed"
        assert u.max_failures == 5

    def test_max_failures_bounds(self):
        with pytest.raises(ValidationError):
            AutomationUpdate(max_failures=0)
        with pytest.raises(ValidationError):
            AutomationUpdate(max_failures=101)


# ---------------------------------------------------------------------------
# AutomationResponse
# ---------------------------------------------------------------------------


class TestAutomationResponse:
    """AutomationResponse model construction."""

    def test_valid_construction(self):
        uid = uuid.uuid4()
        resp = AutomationResponse(
            automation_id=uid,
            user_id="user-1",
            name="My Auto",
            trigger_type="cron",
            timezone="UTC",
            agent_mode="flash",
            instruction="Do something",
            thread_strategy="new",
            status="active",
            max_failures=3,
            failure_count=0,
            created_at=NOW,
            updated_at=NOW,
        )
        assert resp.automation_id == uid
        assert resp.status == "active"
        assert resp.failure_count == 0


# ---------------------------------------------------------------------------
# AutomationExecutionResponse
# ---------------------------------------------------------------------------


class TestAutomationExecutionResponse:
    """Execution response model."""

    def test_valid_construction(self):
        exec_id = uuid.uuid4()
        auto_id = uuid.uuid4()
        resp = AutomationExecutionResponse(
            automation_execution_id=exec_id,
            automation_id=auto_id,
            status="completed",
            scheduled_at=NOW,
            created_at=NOW,
        )
        assert resp.status == "completed"
        assert resp.error_message is None
        assert resp.started_at is None
        assert resp.completed_at is None

    def test_a_value_a_newer_build_wrote_still_reads(self):
        """The row rides on every automation in the list, so a status or reason
        this build does not know must not fail the whole response."""
        resp = AutomationExecutionResponse(
            automation_execution_id=uuid.uuid4(),
            automation_id=uuid.uuid4(),
            status="archived",
            skip_reason="quota",
            failure_reason="sandbox",
            scheduled_at=NOW,
            created_at=NOW,
        )
        assert (resp.status, resp.skip_reason, resp.failure_reason) == (
            "archived", "quota", "sandbox"
        )

    def test_status_vocabulary_is_the_schema_check(self):
        """Every status the column's CHECK admits validates on the way out,
        and the model admits none the column cannot hold."""
        migration = (
            Path(__file__).parents[4]
            / "migrations/versions/052_automation_execution_report.py"
        ).read_text()
        upgrade = migration.split("def downgrade")[0]
        check = re.search(r"status IN \(([^)]*)\)", upgrade).group(1)
        assert set(re.findall(r"'(\w+)'", check)) == set(get_args(ExecutionStatus))


# ---------------------------------------------------------------------------
# List responses
# ---------------------------------------------------------------------------


class TestAutomationsListResponse:
    """Automation list response."""

    def test_empty(self):
        resp = AutomationsListResponse(automations=[], total=0)
        assert resp.total == 0


class TestAutomationRunsListResponse:
    """A page of runs."""

    def test_empty(self):
        resp = AutomationRunsListResponse(executions=[], has_more=False)
        assert resp.has_more is False
