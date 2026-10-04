"""An automation's delivery may name one chat, which the channel gateway
checks on every save that newly names it, through every surface. With a
messaging service, a newly named app is checked too, and kept as written."""

from unittest.mock import AsyncMock, patch

import httpx
import pytest

from src.server.models.automation import AutomationCreate
from src.server.services.automations import lifecycle
from src.server.services.automations.lifecycle import (
    DeliveryRefused,
    check_delivery,
    create_automation,
    update_automation,
)
from tests.unit.server.services.automations._check_target import (  # noqa: F401 - fixtures
    CHAT,
    CHAT_SPELLED,
    DISCORD_DM,
    REFUSAL,
    REFUSED_CHAT,
    SLACK_DM,
    UNLINKED,
    UNLINKED_APP,
)

OWNER = "user-owner"
AUTOMATION_ID = "00000000-0000-4000-8000-000000000001"
_LIFECYCLE = "src.server.services.automations.lifecycle"


def _create_data(methods):
    return AutomationCreate(
        name="Daily",
        instruction="Summarize the market",
        trigger_type="cron",
        cron_expression="0 9 * * 1-5",
        timezone="UTC",
        delivery_config={"methods": methods},
    )


def _row(methods=None):
    return {
        "automation_id": AUTOMATION_ID,
        "user_id": OWNER,
        "trigger_type": "cron",
        "cron_expression": "0 9 * * 1-5",
        "timezone": "UTC",
        "status": "active",
        "agent_mode": "flash",
        "workspace_id": None,
        "llm_model": None,
        "delivery_config": {"methods": methods} if methods is not None else None,
    }


class TestCheckDelivery:
    @pytest.mark.asyncio
    async def test_a_chat_is_stored_as_the_gateway_files_it(self, check_target):
        assert await check_delivery(OWNER, ["slack", CHAT_SPELLED]) == ["slack", CHAT]
        assert check_target.asked == ["slack", CHAT_SPELLED]

    @pytest.mark.asyncio
    async def test_two_spellings_of_one_chat_are_one_entry(self, check_target):
        assert await check_delivery(OWNER, [CHAT, CHAT_SPELLED]) == [CHAT]

    @pytest.mark.asyncio
    async def test_every_refused_chat_is_named_with_the_gateways_reason(
        self, check_target
    ):
        with pytest.raises(DeliveryRefused) as exc:
            await check_delivery(OWNER, [REFUSED_CHAT, CHAT, "slack:T1/CNOPE2"])

        assert exc.value.field == "delivery_config"
        assert exc.value.problems == [
            f"{REFUSED_CHAT!r}: {REFUSAL}",
            f"'slack:T1/CNOPE2': {REFUSAL}",
        ]
        assert exc.value.refusals == [
            (REFUSED_CHAT, REFUSAL),
            ("slack:T1/CNOPE2", REFUSAL),
        ]
        assert str(exc.value) == "; ".join(exc.value.problems)

    @pytest.mark.asyncio
    async def test_a_gateway_that_does_not_answer_refuses_the_chat(self, check_target):
        check_target.error = httpx.ConnectError("down")

        with pytest.raises(DeliveryRefused) as exc:
            await check_delivery(OWNER, [CHAT])

        assert exc.value.problems == [
            f"{CHAT!r}: couldn't be checked: The messaging service could not be reached. Try again shortly."
        ]

    @pytest.mark.asyncio
    async def test_a_stored_entry_is_not_checked_again(self, check_target):
        assert await check_delivery(
            OWNER, [REFUSED_CHAT, UNLINKED_APP], stored=[REFUSED_CHAT, UNLINKED_APP]
        ) == [
            REFUSED_CHAT,
            UNLINKED_APP,
        ]
        assert check_target.asked == []

    @pytest.mark.asyncio
    async def test_a_dm_address_is_checked_and_kept_apart_from_its_app(
        self, check_target
    ):
        """An app name follows the app's chain and a DM address pins the DM,
        so neither stands in for the other."""
        methods = ["discord", DISCORD_DM, "slack", SLACK_DM]

        assert await check_delivery(OWNER, methods) == methods
        assert check_target.asked == methods

    @pytest.mark.asyncio
    async def test_a_new_app_name_is_checked_once_and_kept_as_written(
        self, check_target
    ):
        """It follows the app's chain at each run, so the chat the messaging
        service answers for it today is never stored in its place."""
        assert await check_delivery(OWNER, ["slack", "slack", "email"]) == [
            "slack",
            "email",
        ]
        assert check_target.asked == ["slack", "email"]

    @pytest.mark.asyncio
    async def test_an_app_the_user_has_not_linked_is_refused(self, check_target):
        with pytest.raises(DeliveryRefused) as exc:
            await check_delivery(OWNER, ["slack", UNLINKED_APP])

        assert exc.value.refusals == [(UNLINKED_APP, UNLINKED)]

    @pytest.mark.asyncio
    async def test_without_a_gateway_a_chat_is_refused_and_an_app_is_not(
        self, no_gateway
    ):
        assert await check_delivery(OWNER, ["slack"]) == ["slack"]

        with pytest.raises(DeliveryRefused) as exc:
            await check_delivery(OWNER, ["slack", CHAT])

        assert exc.value.problems == [
            f"{CHAT!r}: naming a chat needs a connected messaging service, and this server has "
            'none; name an app such as "slack" instead'
        ]


class TestTheLimits:
    """The messaging service takes at most 20 entries when a run hands it the
    delivery, refusing the whole run past that, and checks a chat of at most
    256 characters, so a save refuses past either first."""

    @pytest.mark.asyncio
    async def test_twenty_entries_save(self, no_gateway):
        methods = [f"app{i}" for i in range(20)]

        assert await check_delivery(OWNER, methods) == methods

    @pytest.mark.asyncio
    async def test_each_entry_past_twenty_is_refused(self, check_target):
        methods = [f"app{i}" for i in range(22)]

        with pytest.raises(DeliveryRefused) as exc:
            await check_delivery(OWNER, methods)

        assert exc.value.refusals == [
            ("app20", "past the limit of 20 entries"),
            ("app21", "past the limit of 20 entries"),
        ]
        # Refused before the messaging service is asked anything.
        assert check_target.asked == []

    @pytest.mark.asyncio
    async def test_an_entry_longer_than_256_characters_is_refused(self, check_target):
        longest = "slack:" + "C" * 250
        too_long = longest + "C"

        assert await check_delivery(OWNER, [longest], stored=[longest]) == [longest]
        with pytest.raises(DeliveryRefused) as exc:
            await check_delivery(OWNER, ["slack", too_long])

        assert exc.value.refusals == [(too_long, "longer than 256 characters")]
        # Refused here, not read back as a check that failed.
        assert check_target.asked == []

    @pytest.mark.asyncio
    async def test_stored_entries_count_too(self, check_target):
        """The run hands the service the whole list, however much of it was
        saved before."""
        stored = [f"app{i}" for i in range(20)]

        with pytest.raises(DeliveryRefused):
            await check_delivery(OWNER, [*stored, "slack"], stored=stored)

    @pytest.mark.asyncio
    @patch(f"{_LIFECYCLE}.auto_db")
    async def test_a_create_past_the_limit_writes_nothing(self, mock_auto_db, no_gateway):
        with pytest.raises(DeliveryRefused):
            await create_automation(OWNER, _create_data([f"app{i}" for i in range(21)]))

        mock_auto_db.create_automation.assert_not_called()

    @pytest.mark.asyncio
    @patch(f"{_LIFECYCLE}.auto_db")
    async def test_an_update_past_the_limit_writes_nothing(self, mock_auto_db, no_gateway):
        mock_auto_db.get_automation = AsyncMock(return_value=_row(["slack"]))

        with pytest.raises(DeliveryRefused):
            await update_automation(
                AUTOMATION_ID,
                OWNER,
                {"delivery_config": {"methods": [f"app{i}" for i in range(21)]}},
            )

        mock_auto_db.update_automation.assert_not_called()


class TestThroughTheLifecycle:
    @pytest.mark.asyncio
    @patch(f"{_LIFECYCLE}.auto_db")
    async def test_a_create_stores_the_canonical_chat(self, mock_auto_db, check_target):
        mock_auto_db.create_automation = AsyncMock(return_value=_row([CHAT]))

        await create_automation(OWNER, _create_data(["slack", CHAT_SPELLED]))

        stored = mock_auto_db.create_automation.call_args.kwargs["delivery_config"]
        assert stored == {"methods": ["slack", CHAT]}

    @pytest.mark.asyncio
    @patch(f"{_LIFECYCLE}.auto_db")
    async def test_a_create_naming_a_refused_chat_writes_nothing(
        self, mock_auto_db, check_target
    ):
        with pytest.raises(DeliveryRefused) as exc:
            await create_automation(OWNER, _create_data([REFUSED_CHAT]))

        assert str(exc.value) == f"{REFUSED_CHAT!r}: {REFUSAL}"
        mock_auto_db.create_automation.assert_not_called()

    @pytest.mark.asyncio
    @patch(f"{_LIFECYCLE}.auto_db")
    async def test_a_caller_that_checked_is_not_checked_again(
        self, mock_auto_db, check_target
    ):
        mock_auto_db.create_automation = AsyncMock(return_value=_row([CHAT]))

        await create_automation(OWNER, _create_data([CHAT]), delivery_checked=True)

        assert check_target.asked == []

    @pytest.mark.asyncio
    @patch(f"{_LIFECYCLE}.auto_db")
    async def test_an_update_checks_only_a_chat_not_already_saved(
        self, mock_auto_db, check_target
    ):
        mock_auto_db.get_automation = AsyncMock(return_value=_row([REFUSED_CHAT]))
        mock_auto_db.update_automation = AsyncMock(
            return_value=_row([REFUSED_CHAT, CHAT])
        )

        await update_automation(
            AUTOMATION_ID,
            OWNER,
            {"delivery_config": {"methods": [REFUSED_CHAT, CHAT_SPELLED]}},
        )

        assert check_target.asked == [CHAT_SPELLED]
        stored = mock_auto_db.update_automation.call_args.kwargs["delivery_config"]
        assert stored == {"methods": [REFUSED_CHAT, CHAT]}

    @pytest.mark.asyncio
    @patch(f"{_LIFECYCLE}.auto_db")
    async def test_an_update_without_a_gateway_refuses_a_new_chat(
        self, mock_auto_db, no_gateway
    ):
        mock_auto_db.get_automation = AsyncMock(return_value=_row(["slack"]))

        with pytest.raises(DeliveryRefused):
            await update_automation(
                AUTOMATION_ID, OWNER, {"delivery_config": {"methods": [CHAT]}}
            )

        mock_auto_db.update_automation.assert_not_called()

    @pytest.mark.asyncio
    @patch(f"{_LIFECYCLE}.auto_db")
    async def test_clearing_delivery_checks_nothing(self, mock_auto_db, check_target):
        mock_auto_db.get_automation = AsyncMock(return_value=_row([CHAT]))
        mock_auto_db.update_automation = AsyncMock(return_value=_row([]))

        await update_automation(
            AUTOMATION_ID, OWNER, {"delivery_config": {"methods": []}}
        )

        assert check_target.asked == []
        assert mock_auto_db.update_automation.call_args.kwargs["delivery_config"] == {
            "methods": []
        }


def test_a_refusal_is_the_lifecycles_own_kind():
    """REST answers it as a conflict, the tools as an error, the file as a
    problem on its field."""
    assert issubclass(DeliveryRefused, lifecycle.AutomationRefusal)
