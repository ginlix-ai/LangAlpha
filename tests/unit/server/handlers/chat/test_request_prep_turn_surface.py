"""Which conversation a turn acts in, as its tools read it.

A tool that messages the user reads the turn's run and surface off
``configurable``. The surface is the request's own when it names one; a turn
nobody sent (a notification) arrives without one and reports where the user
last was, so it takes the latest attempt's surface, as a resumed interrupt
does. A plain web turn names none, and must not pick up a channel's surface just
because the thread was born there.
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

SLACK_THREAD = PriorThread(attempt_platform="slack")
RESUME = {"iid-1": {"decisions": [{"type": "approve"}]}}


class TestIsNotificationTurn:
    def test_a_system_query_with_no_surface_is_one(self):
        assert is_notification_turn(ChatRequest(query_type="system"))

    def test_a_market_targets_wake_is_one(self):
        assert is_notification_turn(ChatRequest(query_type="market_target"))

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
            ChatRequest(origin=ThreadOrigin(type="agent", id="home-t")),
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

    def test_a_notification_takes_the_latest_attempts_surface(self):
        assert turn_surface(ChatRequest(query_type="system"), SLACK_THREAD) == "slack"

    def test_a_market_targets_wake_takes_the_latest_attempts_surface(self):
        """The user was last on the channel, so the alert's turn answers there."""
        assert (
            turn_surface(ChatRequest(query_type="market_target"), SLACK_THREAD)
            == "slack"
        )

    def test_a_resumed_interrupt_runs_where_the_attempt_it_answers_ran(self):
        """A resume that carries only the decision continues its attempt."""
        prior = PriorThread(attempt_platform="slack")
        assert turn_surface(ChatRequest(hitl_response=RESUME), prior) == "slack"

    def test_answering_a_web_turns_question_on_a_channel_thread_stays_on_the_web(
        self,
    ):
        """The web app sends no surface on a resume. The question was asked
        on a web turn, so the answer is not in the channel conversation the
        thread was born in, and records the web for its own retry."""
        prior = PriorThread(attempt_platform="web")
        request = ChatRequest(hitl_response=RESUME)

        assert turn_surface(request, prior) == "web"
        assert surface_stamp(request, prior, inherits_rules=False) == {
            "surface": {"platform": "web"}
        }

    def test_a_resume_of_an_attempt_that_recorded_no_surface_has_none(self):
        request = ChatRequest(hitl_response=RESUME)
        assert turn_surface(request, PriorThread()) is None
        assert surface_stamp(request, PriorThread(), inherits_rules=False) == {}

    def test_a_notification_after_the_user_moved_to_the_web_speaks_on_the_web(self):
        """A thread born on a channel, then a person on the web: a wakeup
        reports where the person last was, as the rules it inherits say."""
        prior = PriorThread(attempt_platform="web")
        assert turn_surface(ChatRequest(query_type="wakeup"), prior) == "web"

    def test_a_notification_after_an_app_turn_names_no_surface(self):
        """The app records no surface, so the report is in no chat conversation."""
        assert turn_surface(ChatRequest(query_type="system"), PriorThread()) is None

    def test_a_chain_of_notifications_keeps_the_last_persons_surface(self):
        first = ChatRequest(query_type="wakeup")
        stamp = surface_stamp(first, SLACK_THREAD, inherits_rules=True)
        after = PriorThread(attempt_platform=stamp["surface"]["platform"])
        assert turn_surface(ChatRequest(query_type="system"), after) == "slack"

    def test_a_resume_that_names_a_surface_keeps_it(self):
        request = ChatRequest(hitl_response=RESUME, platform="discord")
        assert turn_surface(request, SLACK_THREAD) == "discord"

    def test_a_dispatched_run_has_no_surface(self):
        """A background run another agent dispatched is in no conversation;
        its thread was created by that dispatch and carries no surface."""
        request = ChatRequest(origin=ThreadOrigin(type="agent", id="home-t"))
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

    def test_a_notification_records_the_surface_it_took(self):
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


class TestThePriorReadCarriesTheLatestAttemptsSurface:
    @pytest.mark.asyncio
    async def test_the_surface_a_thread_was_born_on_is_not_carried(self):
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
            prior = await ensure_thread(request, "t-1", "ws-1", "u-1")

        assert prior.attempt_platform is None

    @pytest.mark.asyncio
    async def test_the_prior_read_carries_the_latest_attempts_surface(self):
        from src.server.handlers.chat.request_prep import ensure_thread

        request = MagicMock(external_thread_id=None, platform=None, origin=None)
        request.fork_from_turn = None
        request.checkpoint_id = None
        attempt = {"metadata": {"surface": {"platform": "web"}}}
        with (
            patch(
                f"{PREP}.qr_db.get_thread_by_id",
                new_callable=AsyncMock,
                return_value={"platform": "slack"},
            ),
            patch(f"{PREP}.qr_db.ensure_thread_exists", new_callable=AsyncMock),
            patch(
                f"{PREP}.tl_db.get_latest_attempt",
                new_callable=AsyncMock,
                return_value=attempt,
            ),
        ):
            prior = await ensure_thread(request, "t-1", "ws-1", "u-1")

        assert prior.attempt_platform == "web"

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
            prior = await ensure_thread(request, "t-1", "ws-1", "u-1")

        assert prior.attempt_platform is None


class TestANotificationTakesTheSurfaceOfTheRulesItInherits:
    """An automation's run and an attempt that never reached the graph state
    no surface's rules, so a notification after either takes the surface of
    the attempt before it, which stated the rules the notification inherits."""

    @staticmethod
    async def _prior(
        latest: dict, earlier: dict, *, query_type="system", fork_from_turn=None
    ):
        from src.server.handlers.chat.request_prep import ensure_thread

        request = MagicMock(
            external_thread_id=None, platform=None, origin=None, hitl_response=None
        )
        request.query_type = query_type
        request.fork_from_turn = fork_from_turn
        request.checkpoint_id = "cp-1" if fork_from_turn is not None else None

        async def latest_attempt(thread_id, *, before_turn=None, without_key=None):
            if without_key and before_turn != fork_from_turn:
                return {"metadata": {"surface": {"platform": "discarded"}}}
            return earlier if without_key else latest

        with (
            patch(
                f"{PREP}.qr_db.get_thread_by_id",
                new_callable=AsyncMock,
                return_value={"platform": None},
            ),
            patch(f"{PREP}.qr_db.ensure_thread_exists", new_callable=AsyncMock),
            patch(f"{PREP}.tl_db.get_latest_attempt", side_effect=latest_attempt),
            patch(
                "src.server.database.runs.lifecycle.get_latest_attempt",
                side_effect=latest_attempt,
            ),
        ):
            return await ensure_thread(request, "t-1", "ws-1", "u-1")

    SLACK_ATTEMPT = {"metadata": {"surface": {"platform": "slack"}}}

    @pytest.mark.asyncio
    async def test_a_notification_after_an_automation_takes_the_persons_surface(self):
        automation = {"metadata": {"automation_execution_id": "e-1"}}
        prior = await self._prior(automation, self.SLACK_ATTEMPT)

        assert prior.attempt_platform == "slack"

    @pytest.mark.asyncio
    async def test_a_notification_passes_over_an_attempt_that_never_ran(self):
        unreached = {"metadata": {"surface": {"platform": "web"}, "pending_input": {}}}
        prior = await self._prior(unreached, self.SLACK_ATTEMPT)

        assert prior.attempt_platform == "slack"

    @pytest.mark.asyncio
    async def test_a_regenerated_report_back_reads_before_the_fork(self):
        """An unstarted report-back's regenerate keeps its ``system`` type and
        forks: the attempt it reads past to is one the fork keeps."""
        automation = {"metadata": {"automation_execution_id": "e-1"}}
        prior = await self._prior(automation, self.SLACK_ATTEMPT, fork_from_turn=3)

        assert prior.attempt_platform == "slack"

    @pytest.mark.asyncio
    async def test_a_persons_turn_after_an_automation_reads_the_latest_attempt(self):
        """Only a notification looks past the automation: a resume continues
        the attempt it answers."""
        automation = {"metadata": {"automation_execution_id": "e-1"}}
        prior = await self._prior(automation, self.SLACK_ATTEMPT, query_type=None)

        assert prior.attempt_platform is None


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
                timezone_str="UTC",
                token_callback=None,
                request=ChatRequest(),
                effective_model=None,
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
