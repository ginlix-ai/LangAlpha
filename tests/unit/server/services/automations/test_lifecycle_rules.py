"""The lifecycle's own refusals: the model a run would reject, the status a
pause or resume needs, and the field each refusal names for the file and REST."""

import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest
from pydantic import ValidationError

from src.server.models.automation import AutomationCreate
from src.server.services.automations import lifecycle
from src.server.services.automations.lifecycle import (
    AutomationRefusal,
    create_automation,
    pause_automation,
    resume_automation,
    update_automation,
)

OWNER = "user-owner"
AUTOMATION_ID = str(uuid.uuid4())
PAST = datetime(2020, 1, 1, 0, 0, tzinfo=timezone.utc)

_LIFECYCLE = "src.server.services.automations.lifecycle"


def _create_data(**overrides):
    data = {
        "name": "Daily",
        "instruction": "Summarize the market",
        "trigger_type": "cron",
        "cron_expression": "0 9 * * 1-5",
        "timezone": "UTC",
    }
    data.update(overrides)
    return AutomationCreate(**data)


def _row(**overrides):
    row = {
        "automation_id": AUTOMATION_ID,
        "user_id": OWNER,
        "trigger_type": "cron",
        "cron_expression": "0 9 * * 1-5",
        "timezone": "UTC",
        "status": "active",
        "agent_mode": "flash",
        "workspace_id": None,
        "llm_model": None,
    }
    row.update(overrides)
    return row


@pytest.fixture
def models():
    """A user with one custom model and one custom provider, on a deployment
    that configures two models; classify_model answers unknown until a test
    says otherwise."""
    pref = {
        "custom_models": [{"name": "my-custom"}],
        "custom_providers": [{"name": "my-provider"}],
    }
    with (
        patch(f"{_LIFECYCLE}.user_models.get_model_preference", AsyncMock(return_value=pref)),
        patch(
            f"{_LIFECYCLE}.user_models.classify_model",
            AsyncMock(return_value=("unknown", {})),
        ) as classify,
        patch(
            f"{_LIFECYCLE}.get_configured_llm_models",
            return_value={"vendor-b": ["model-b"], "vendor-a": ["model-a"]},
        ),
    ):
        yield classify


# ---------------------------------------------------------------------------
# llm_model
# ---------------------------------------------------------------------------


class TestModelPreferences:
    @pytest.mark.asyncio
    @patch(f"{_LIFECYCLE}.auto_db")
    async def test_preferences_the_caller_read_are_not_read_again(self, mock_auto_db, models):
        """The automations file reads them before its transaction takes its
        locks, so the check borrows no second connection under them."""
        models.return_value = ("custom", {"name": "my-custom"})
        mock_auto_db.create_automation = AsyncMock(return_value=_row(llm_model="my-custom"))
        mock_auto_db.get_automation = AsyncMock(return_value=_row())
        mock_auto_db.update_automation = AsyncMock(return_value=_row(llm_model="my-custom"))
        pref = {"custom_models": [{"name": "my-custom"}]}

        with patch(f"{_LIFECYCLE}.user_models.get_model_preference", AsyncMock()) as read:
            await create_automation(OWNER, _create_data(llm_model="my-custom"), model_pref=pref)
            await update_automation(AUTOMATION_ID, OWNER, {"llm_model": "my-custom"}, model_pref=pref)

        read.assert_not_awaited()
        assert [c.kwargs["_pref_cache"] for c in models.await_args_list] == [pref, pref]


class TestModelOnCreate:
    @pytest.mark.asyncio
    @patch(f"{_LIFECYCLE}.auto_db")
    async def test_an_unknown_model_is_refused_with_the_names_to_use(self, mock_auto_db, models):
        with pytest.raises(AutomationRefusal) as exc:
            await create_automation(OWNER, _create_data(llm_model="no-such-model"))

        assert exc.value.field == "llm_model"
        assert str(exc.value) == (
            "unknown model 'no-such-model'; use null for the user's default, "
            "or one of: my-custom, model-a, model-b"
        )
        mock_auto_db.create_automation.assert_not_called()

    @pytest.mark.asyncio
    @patch(f"{_LIFECYCLE}.auto_db")
    async def test_a_model_the_user_can_run_is_created(self, mock_auto_db, models):
        models.return_value = ("system", {"model_id": "model-a"})
        mock_auto_db.create_automation = AsyncMock(return_value=_row(llm_model="model-a"))

        await create_automation(OWNER, _create_data(llm_model="model-a"))

        assert mock_auto_db.create_automation.call_args.kwargs["llm_model"] == "model-a"

    @pytest.mark.asyncio
    @patch(f"{_LIFECYCLE}.auto_db")
    async def test_a_custom_provider_named_as_the_model_is_created(self, mock_auto_db, models):
        mock_auto_db.create_automation = AsyncMock(return_value=_row(llm_model="my-provider"))

        await create_automation(OWNER, _create_data(llm_model="my-provider"))

        mock_auto_db.create_automation.assert_awaited_once()

    @pytest.mark.asyncio
    @patch(f"{_LIFECYCLE}.auto_db")
    async def test_no_model_skips_the_check(self, mock_auto_db, models):
        mock_auto_db.create_automation = AsyncMock(return_value=_row())

        await create_automation(OWNER, _create_data())

        models.assert_not_awaited()


class TestModelOnUpdate:
    @pytest.mark.asyncio
    @patch(f"{_LIFECYCLE}.auto_db")
    async def test_a_new_unknown_name_is_refused(self, mock_auto_db, models):
        mock_auto_db.get_automation = AsyncMock(return_value=_row(llm_model="model-a"))

        with pytest.raises(AutomationRefusal) as exc:
            await update_automation(AUTOMATION_ID, OWNER, {"llm_model": "no-such-model"})

        assert exc.value.field == "llm_model"
        mock_auto_db.update_automation.assert_not_called()

    @pytest.mark.asyncio
    @patch(f"{_LIFECYCLE}.auto_db")
    async def test_restating_the_stored_name_is_not_checked(self, mock_auto_db, models):
        """A model the user can no longer run must not block an edit to anything else."""
        mock_auto_db.get_automation = AsyncMock(return_value=_row(llm_model="retired-model"))
        mock_auto_db.update_automation = AsyncMock(return_value=_row(name="Renamed"))

        await update_automation(
            AUTOMATION_ID, OWNER, {"name": "Renamed", "llm_model": "retired-model"}
        )

        models.assert_not_awaited()
        mock_auto_db.update_automation.assert_awaited_once()


# ---------------------------------------------------------------------------
# The field each refusal names
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@patch(f"{_LIFECYCLE}.auto_db")
async def test_ptc_without_a_workspace_names_the_workspace(mock_auto_db):
    with pytest.raises(AutomationRefusal) as exc:
        await create_automation(OWNER, _create_data(agent_mode="ptc"))

    assert exc.value.field == "workspace_id"


@pytest.mark.parametrize(
    ("action", "status", "message"),
    [
        (pause_automation, "paused", "Cannot pause automation in 'paused' status (must be 'active')"),
        (
            resume_automation, "active",
            "Cannot resume automation in 'active' status (must be 'paused' or 'disabled')",
        ),
    ],
)
@pytest.mark.asyncio
@patch(f"{_LIFECYCLE}.auto_db")
async def test_a_status_refusal_names_the_status(mock_auto_db, action, status, message):
    mock_auto_db.get_automation = AsyncMock(return_value=_row(status=status))

    with pytest.raises(AutomationRefusal) as exc:
        await action(AUTOMATION_ID, OWNER)

    assert (str(exc.value), exc.value.field) == (message, "status")
    mock_auto_db.update_automation.assert_not_called()


@pytest.mark.asyncio
@patch(f"{_LIFECYCLE}.auto_db")
async def test_a_passed_one_time_run_is_named_on_the_automations_clock(mock_auto_db):
    mock_auto_db.get_automation = AsyncMock(
        return_value=_row(
            trigger_type="once", cron_expression=None, status="paused",
            timezone="Asia/Tokyo", next_run_at=PAST,
        )
    )

    with pytest.raises(AutomationRefusal) as exc:
        await resume_automation(AUTOMATION_ID, OWNER)

    assert exc.value.field == "status"
    assert str(exc.value) == (
        "Cannot resume a one-time automation whose scheduled time has passed "
        "(2020-01-01T09:00:00+09:00); set next_run_at to a future time first"
    )


# ---------------------------------------------------------------------------
# What the model refuses on the way in
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@patch(f"{_LIFECYCLE}.auto_db")
async def test_an_update_to_an_unknown_zone_writes_nothing(mock_auto_db):
    mock_auto_db.get_automation = AsyncMock(return_value=_row())

    with pytest.raises(ValidationError) as exc:
        await update_automation(AUTOMATION_ID, OWNER, {"timezone": "Mars/Olympus"})

    [error] = exc.value.errors()
    assert error["loc"] == ("timezone",)
    assert "unknown IANA timezone 'Mars/Olympus'" in error["msg"]
    mock_auto_db.update_automation.assert_not_called()


@pytest.mark.asyncio
@patch(f"{_LIFECYCLE}.auto_db")
async def test_the_users_own_zone_and_time_are_stored_as_sent(mock_auto_db):
    """A fixed-offset zone and a past time are an agent's slips, refused on
    the agent's surfaces; what the page sends is the user's own choice."""
    mock_auto_db.get_automation = AsyncMock(
        return_value=_row(trigger_type="once", cron_expression=None, status="paused")
    )
    mock_auto_db.update_automation = AsyncMock(return_value=_row())

    await update_automation(AUTOMATION_ID, OWNER, {"timezone": "EST", "next_run_at": PAST})

    kwargs = mock_auto_db.update_automation.await_args.kwargs
    assert (kwargs["timezone"], kwargs["next_run_at"]) == ("EST", PAST)


# ---------------------------------------------------------------------------
# delivery_warning
# ---------------------------------------------------------------------------


def test_delivery_without_a_webhook_is_warned_about(monkeypatch):
    monkeypatch.setattr(lifecycle.settings, "AUTOMATION_WEBHOOK_URL", "")

    assert lifecycle.delivery_warning(["slack"]) == (
        "Delivery was saved, but AUTOMATION_WEBHOOK_URL is not configured, so runs "
        "post only in the app until it is set."
    )


def test_no_warning_with_a_messaging_service(monkeypatch):
    """A run's agent sends the results itself, with no webhook."""
    monkeypatch.setattr(lifecycle.settings, "AUTOMATION_WEBHOOK_URL", "")
    monkeypatch.setattr(lifecycle.messaging.env, "CHANNEL_GATEWAY_URL", "http://gw.test")
    monkeypatch.setenv("INTERNAL_SERVICE_TOKEN", "svc-token")

    assert lifecycle.delivery_warning(["slack"]) is None


@pytest.mark.parametrize(
    ("webhook", "methods"),
    [("https://example.com/hook", ["slack"]), ("", []), ("", None)],
)
def test_no_warning_with_a_webhook_or_without_delivery(monkeypatch, webhook, methods):
    monkeypatch.setattr(lifecycle.settings, "AUTOMATION_WEBHOOK_URL", webhook)

    assert lifecycle.delivery_warning(methods) is None
