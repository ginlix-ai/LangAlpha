"""Tests for automation Pydantic models.

Covers request/response models in src/server/models/automation.py including
field constraints, enum literals, and defaults.
"""

import re
import uuid
from datetime import datetime, timedelta, timezone
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
    future_run,
    on_clock,
    parse_delivery,
    region_zone,
    run_time,
    thread_fields,
)


NOW = datetime.now(timezone.utc)
# A valid one-time run: NOW has passed by the time a model checks it.
LATER = (NOW + timedelta(days=30)).replace(microsecond=0)


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
            next_run_at=LATER,
        )
        assert a.trigger_type == "once"
        assert a.next_run_at == LATER

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
    "once": {"next_run_at": LATER},
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
        naive = LATER.replace(tzinfo=None)
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

    def test_a_price_config_is_stored_as_sent_but_its_symbol_as_validated(self):
        config = {"symbol": "gspc", "conditions": [{"type": "price_above", "value": 1.0}]}
        stored = _create("price", trigger_config=config).trigger_config
        assert stored == {**config, "symbol": "SPX"}

    def test_a_shanghai_symbol_is_stored_in_its_display_spelling(self):
        config = {"symbol": "600519.SS", "conditions": [{"type": "price_above", "value": 1.0}]}
        stored = _create("price", trigger_config=config).trigger_config
        assert stored == {**config, "symbol": "600519.SH"}


# ---------------------------------------------------------------------------
# Zone and run time: what every surface refuses, and the agent's own guards
# ---------------------------------------------------------------------------


PAST = datetime(2020, 1, 1, 0, 0, tzinfo=timezone.utc)


class TestZone:
    def test_an_unknown_zone_is_refused(self):
        with pytest.raises(ValidationError, match="unknown IANA timezone 'Mars/Olympus'"):
            _create("cron", timezone="Mars/Olympus")
        with pytest.raises(ValidationError, match="unknown IANA timezone 'Mars/Olympus'"):
            AutomationUpdate(timezone="Mars/Olympus")

    def test_a_region_zone_is_kept(self):
        assert _create("cron", timezone="America/New_York").timezone == "America/New_York"
        assert AutomationUpdate(timezone="Asia/Tokyo").timezone == "Asia/Tokyo"

    def test_a_fixed_offset_zone_is_the_users_to_choose(self):
        """The page may send one; only the agent is held to region zones."""
        assert _create("cron", timezone="EST").timezone == "EST"
        assert AutomationUpdate(timezone="MST").timezone == "MST"

    def test_an_update_that_names_no_zone_leaves_it_unset(self):
        assert AutomationUpdate(name="Renamed").timezone is None

    @pytest.mark.parametrize(
        ("zone", "hint"),
        [("EST", "America/New_York"), ("MST", "America/Denver (or America/Phoenix")],
    )
    def test_the_agents_fixed_offset_zone_is_refused_with_its_region(self, zone, hint):
        """A 9:00 EST automation would run at 10:00 New York time all summer."""
        with pytest.raises(ValueError, match="fixed offset that ignores daylight saving") as exc:
            region_zone(zone)
        assert hint in str(exc.value)
        assert region_zone("America/New_York") == "America/New_York"


class TestRunTime:
    def test_the_page_may_send_a_date_or_a_past_time(self):
        """The agent's surfaces guard against both; the page's times are the
        user's own, and a naive one reads as UTC."""
        assert _create("once", next_run_at="2030-01-01").next_run_at == datetime(2030, 1, 1, tzinfo=timezone.utc)
        assert AutomationUpdate(next_run_at=PAST.replace(tzinfo=None)).next_run_at == PAST

    @pytest.mark.parametrize("text", ["2030-01-01", "20300101", " 2030-01-01 "])
    def test_the_agents_date_without_a_time_of_day_is_refused(self, text):
        """It would run at midnight, which nobody asking for a date means."""
        with pytest.raises(ValueError, match="has no time of day, which would run at midnight"):
            run_time(text)

    def test_the_agents_past_time_is_named_on_the_automations_clock(self):
        with pytest.raises(ValueError) as exc:
            future_run(PAST, "Asia/Tokyo")
        assert str(exc.value).startswith("2020-01-01T09:00:00+09:00 has already passed (it is ")
        assert str(exc.value).endswith("+09:00 on this automation's clock); set a future time")

    def test_a_past_time_with_no_known_zone_is_named_in_utc(self):
        with pytest.raises(ValueError) as exc:
            future_run(PAST.replace(tzinfo=None), None)
        assert str(exc.value).startswith("2020-01-01T00:00:00+00:00 has already passed")
        assert str(exc.value).endswith("+00:00 in UTC); set a future time")

    def test_a_future_time_is_kept_as_given(self):
        when = datetime(2030, 1, 1, 9, 0, tzinfo=timezone(timedelta(hours=9)))
        assert future_run(when, "Asia/Tokyo") is when

    def test_run_time_keeps_what_the_text_says(self):
        assert run_time("2030-01-01T09:30:00") == datetime(2030, 1, 1, 9, 30)
        assert run_time(" 2030-01-01T09:30:00+09:00 ").utcoffset() == timedelta(hours=9)

    @pytest.mark.parametrize(
        ("text", "refusal"),
        [
            ("2030-01-01", "'2030-01-01' has no time of day"),
            ("tomorrow at nine", "not an ISO datetime: 'tomorrow at nine'"),
        ],
    )
    def test_run_time_refuses_what_is_not_a_time(self, text, refusal):
        with pytest.raises(ValueError, match=refusal):
            run_time(text)

    def test_on_clock_names_the_time_on_the_zone_to_the_second(self):
        when = datetime(2030, 7, 1, 13, 0, 5, 999, tzinfo=timezone.utc)
        assert on_clock(when, "America/New_York") == "2030-07-01T09:00:05-04:00"
        assert on_clock(when.replace(tzinfo=None), None) == "2030-07-01T13:00:05+00:00"
        assert on_clock(when, "Mars/Olympus") == "2030-07-01T13:00:05+00:00"


class TestDelivery:
    @pytest.mark.parametrize(
        ("written", "methods"),
        [
            (["slack"], ["slack"]),
            ([" slack ", "email"], ["slack", "email"]),
            ([], []),
            ("slack, email", ["slack", "email"]),
            ("slack,", ["slack"]),
            (["slack", ""], ["slack"]),
            ([" "], []),
        ],
    )
    def test_a_list_or_a_comma_string_is_the_methods(self, written, methods):
        assert parse_delivery(written) == {"methods": methods}

    @pytest.mark.parametrize("written", [None, 3, ["slack", 1], {"methods": ["slack"]}])
    def test_anything_else_is_refused_with_the_shape(self, written):
        with pytest.raises(ValueError, match=r'must be a list of delivery methods, e\.g\. \["slack"\]'):
            parse_delivery(written)


class TestThreadFields:
    THREAD = str(uuid.uuid4())

    def test_new_clears_any_pin(self):
        assert thread_fields("new", self.THREAD) == {
            "thread_strategy": "new", "conversation_thread_id": None,
        }

    def test_persistent_is_the_automations_own_thread(self):
        """Its first run creates it, so a pinned conversation is cleared."""
        assert thread_fields("persistent", self.THREAD) == {
            "thread_strategy": "continue", "conversation_thread_id": None,
        }

    def test_current_pins_the_conversation_writing(self):
        assert thread_fields("current", self.THREAD) == {
            "thread_strategy": "continue", "conversation_thread_id": self.THREAD,
        }

    def test_current_without_a_conversation_is_refused(self):
        with pytest.raises(ValueError, match='"current" needs a conversation thread'):
            thread_fields("current", None)

    def test_a_thread_id_pins_that_thread(self):
        other = uuid.uuid4()
        assert thread_fields(str(other).upper(), None) == {
            "thread_strategy": "continue", "conversation_thread_id": str(other),
        }

    @pytest.mark.parametrize("value", ["latest", "", None, 7])
    def test_anything_else_is_refused(self, value):
        with pytest.raises(ValueError, match='must be "new", "persistent", "current" or a thread id'):
            thread_fields(value, self.THREAD)


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
