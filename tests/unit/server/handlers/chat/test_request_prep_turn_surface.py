"""Which conversation a turn acts in, as its tools read it.

A tool that messages the user reads the turn's run and surface off
``configurable``. The surface is the request's own when it names one; a turn
nobody sent (a notification) and a resumed interrupt arrive without one, and
continue work the thread already had in hand, so they take the surface the
thread is bound to. A plain web turn names none, and must not pick up a
channel's surface just because the thread was born there.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.handlers.chat.request_prep import (
    PriorThread,
    build_graph_config,
    build_turn_context,
    is_notification_turn,
    retried_on_its_surface,
    surface_stamp,
    turn_surface,
)
from src.server.models.chat import ChatRequest, ThreadOrigin

PREP = "src.server.handlers.chat.request_prep"

SLACK_THREAD = PriorThread(platform="slack")
RESUME = {"iid-1": {"decisions": [{"type": "approve"}]}}


class TestIsNotificationTurn:
    def test_a_system_query_with_no_surface_is_one(self):
        assert is_notification_turn(ChatRequest(query_type="system"))

    def test_a_system_query_that_names_a_surface_is_not(self):
        assert not is_notification_turn(
            ChatRequest(query_type="system", platform="slack")
        )

    @pytest.mark.parametrize(
        "request_",
        [
            ChatRequest(),
            ChatRequest(platform="slack"),
            ChatRequest(hitl_response=RESUME),
            ChatRequest(origin=ThreadOrigin(type="automation", id="a-1")),
            ChatRequest(origin=ThreadOrigin(type="agent", id="flash-t")),
        ],
    )
    def test_anything_a_client_or_a_dispatch_sends_is_not(self, request_):
        assert not is_notification_turn(request_)


class TestTurnSurface:
    def test_a_channel_turn_names_its_own_surface(self):
        assert (
            turn_surface(ChatRequest(platform="telegram"), SLACK_THREAD) == "telegram"
        )

    def test_a_symbol_scoped_surface_is_named_without_the_symbol(self):
        assert turn_surface(
            ChatRequest(platform="market_view:AAPL"), PriorThread()
        ) == ("market_view")

    def test_a_web_turn_on_a_channel_thread_names_no_surface(self):
        """The user continued a channel thread in the web app: that turn is
        not in the channel conversation."""
        assert turn_surface(ChatRequest(), SLACK_THREAD) is None

    def test_a_notification_takes_the_surface_the_thread_is_bound_to(self):
        assert turn_surface(ChatRequest(query_type="system"), SLACK_THREAD) == "slack"

    def test_a_resumed_interrupt_takes_the_surface_the_thread_is_bound_to(self):
        """A gateway's resume carries only the decision."""
        assert turn_surface(ChatRequest(hitl_response=RESUME), SLACK_THREAD) == "slack"

    def test_a_resume_that_names_a_surface_keeps_it(self):
        request = ChatRequest(hitl_response=RESUME, platform="discord")
        assert turn_surface(request, SLACK_THREAD) == "discord"

    def test_a_dispatched_run_has_no_surface(self):
        """A background run another agent dispatched is in no conversation;
        its thread was created by that dispatch and carries no surface."""
        request = ChatRequest(origin=ThreadOrigin(type="agent", id="flash-t"))
        assert turn_surface(request, PriorThread()) is None

    def test_a_notification_on_an_unbound_thread_has_no_surface(self):
        assert turn_surface(ChatRequest(query_type="system"), PriorThread()) is None


class TestARetryTakesItsAttemptsSurface:
    """``/retry`` names no surface; the attempt it retries recorded one."""

    def test_the_stamp_keeps_the_surface_as_the_request_spelled_it(self):
        """A chart's symbol is part of its rules."""
        request = ChatRequest(platform="market_view:AAPL")
        assert surface_stamp(request, PriorThread(), inherits_rules=False) == {
            "surface": {"platform": "market_view:AAPL"}
        }

    def test_a_notification_records_the_threads_surface(self):
        request = ChatRequest(query_type="system")
        assert surface_stamp(request, SLACK_THREAD, inherits_rules=True) == {
            "surface": {"platform": "slack", "inherits_rules": True}
        }

    def test_a_web_turn_records_nothing(self):
        assert surface_stamp(ChatRequest(), SLACK_THREAD, inherits_rules=False) == {}

    @pytest.mark.asyncio
    async def test_the_tools_of_a_retried_channel_turn_name_the_channel(self):
        failed = {"metadata": {"surface": {"platform": "slack", "rules": "R"}}}
        retry = ChatRequest(messages=[], checkpoint_id="cp-1", retry_of_run_id="run-0")
        with patch(
            f"{PREP}.tl_db.get_run", new_callable=AsyncMock, return_value=failed
        ):
            restored, inherits = await retried_on_its_surface(retry)

        assert turn_surface(restored, PriorThread()) == "slack"
        assert restored.surface_rules == "R"
        assert inherits is False

    @pytest.mark.asyncio
    async def test_a_turn_that_is_no_retry_reads_nothing(self):
        request = ChatRequest()
        with patch(f"{PREP}.tl_db.get_run", new_callable=AsyncMock) as get_run:
            assert await retried_on_its_surface(request) == (request, False)
        get_run.assert_not_awaited()


class TestTheThreadBindingIsReadWithThePriorTurn:
    @pytest.mark.asyncio
    async def test_the_prior_read_carries_the_stored_surface(self):
        from src.server.handlers.chat.request_prep import ensure_thread

        request = MagicMock(external_thread_id=None, platform=None, origin=None)
        request.fork_from_turn = None
        request.checkpoint_id = None
        with (
            patch(
                f"{PREP}.qr_db.get_thread_by_id",
                new_callable=AsyncMock,
                return_value={"platform": "imessage"},
            ),
            patch(f"{PREP}.qr_db.ensure_thread_exists", new_callable=AsyncMock),
            patch(
                f"{PREP}.tl_db.get_latest_attempt",
                new_callable=AsyncMock,
                return_value=None,
            ),
        ):
            prior = await ensure_thread(request, "t-1", "ws-1", "u-1", msg_type="flash")

        assert prior.platform == "imessage"

    @pytest.mark.asyncio
    async def test_an_unbound_thread_reads_as_none(self):
        from src.server.handlers.chat.request_prep import ensure_thread

        request = MagicMock(external_thread_id=None, platform=None, origin=None)
        request.fork_from_turn = None
        request.checkpoint_id = None
        with (
            patch(
                f"{PREP}.qr_db.get_thread_by_id",
                new_callable=AsyncMock,
                return_value={"platform": None},
            ),
            patch(f"{PREP}.qr_db.ensure_thread_exists", new_callable=AsyncMock),
            patch(
                f"{PREP}.tl_db.get_latest_attempt",
                new_callable=AsyncMock,
                return_value=None,
            ),
        ):
            prior = await ensure_thread(request, "t-1", "ws-1", "u-1", msg_type="flash")

        assert prior.platform is None


class TestTheTurnContextFlagsANotification:
    def test_a_notification_inherits_the_rules(self):
        ctx = build_turn_context(
            ChatRequest(query_type="system"), SLACK_THREAD, user_profile=None
        )
        assert ctx.inherits_rules is True
        # The row's own surface stays the request's: the report names none.
        assert ctx.platform is None

    @pytest.mark.parametrize(
        "request_",
        [
            ChatRequest(),
            ChatRequest(platform="slack"),
            ChatRequest(hitl_response=RESUME),
        ],
    )
    def test_any_other_turn_does_not(self, request_):
        assert (
            build_turn_context(request_, SLACK_THREAD, user_profile=None).inherits_rules
            is False
        )


class TestTheGraphConfigCarriesRunAndSurface:
    def _configurable(self, **kwargs):
        with (
            patch(f"{PREP}.get_langsmith_tags", return_value=[]),
            patch(f"{PREP}.get_langsmith_metadata", return_value={}),
        ):
            config = build_graph_config(
                thread_id="t-1",
                user_id="u-1",
                workspace_id="ws-1",
                mode="flash",
                timezone_str="UTC",
                token_callback=None,
                request=ChatRequest(),
                effective_model=None,
                recursion_limit=100,
                **kwargs,
            )
        return config["configurable"]

    def test_run_id_and_platform_are_in_configurable(self):
        configurable = self._configurable(run_id="run-1", surface="slack")
        assert configurable["run_id"] == "run-1"
        assert configurable["platform"] == "slack"

    def test_both_keys_are_present_when_unknown(self):
        configurable = self._configurable()
        assert configurable["run_id"] is None
        assert configurable["platform"] is None
