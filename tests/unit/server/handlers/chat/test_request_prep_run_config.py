"""
Tests for src/server/handlers/chat/request_prep.py: what a turn's run is wired
with.

Covers:
- init_tracking: returns (TokenTrackingManager, ToolUsageTracker)
- build_graph_config: mode parameterization, optional fields
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.agent.limits import UNBOUNDED_RECURSION_LIMIT

PREP = "src.server.handlers.chat.request_prep"


# ---------------------------------------------------------------------------
# init_tracking
# ---------------------------------------------------------------------------


class TestInitTracking:
    def test_returns_tuple(self):
        from src.server.handlers.chat.request_prep import init_tracking

        with (
            patch(
                f"{PREP}.TokenTrackingManager.initialize_tracking",
                return_value=MagicMock(),
            ) as mock_token,
            patch(
                f"{PREP}.ToolUsageTracker",
                return_value=MagicMock(),
            ) as mock_tool,
        ):
            token_cb, tool_tr = init_tracking("thread-1")

        mock_token.assert_called_once_with(thread_id="thread-1", track_tokens=True)
        mock_tool.assert_called_once_with(thread_id="thread-1")
        assert token_cb is not None
        assert tool_tr is not None


# ---------------------------------------------------------------------------
# turn_start_checkpoint_id
# ---------------------------------------------------------------------------


class TestTurnStartCheckpointId:
    """Only a new message that names no checkpoint reads the thread's base: a
    resume answers the pause on the newest checkpoint, and a fork or replay
    already names its own."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "fields",
        [
            {"checkpoint_id": "cp-fork", "hitl_response": None},
            {"checkpoint_id": None, "hitl_response": {"intr-1": {}}},
        ],
    )
    async def test_named_or_resumed_turns_keep_their_checkpoint(self, fields):
        from src.server.handlers.chat.request_prep import turn_start_checkpoint_id

        with patch(
            "src.server.utils.checkpoint_helpers.turn_base_checkpoint_id",
            new=AsyncMock(return_value="cp-base"),
        ) as base:
            got = await turn_start_checkpoint_id("t-1", MagicMock(**fields), MagicMock())

        assert got is None
        base.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_send_starts_from_the_base(self):
        from src.server.handlers.chat.request_prep import turn_start_checkpoint_id

        saver = MagicMock()
        with patch(
            "src.server.utils.checkpoint_helpers.turn_base_checkpoint_id",
            new=AsyncMock(return_value="cp-base"),
        ) as base:
            got = await turn_start_checkpoint_id(
                "t-1", MagicMock(checkpoint_id=None, hitl_response=None), saver
            )

        assert got == "cp-base"
        base.assert_awaited_once_with(saver, "t-1")


# ---------------------------------------------------------------------------
# build_graph_config
# ---------------------------------------------------------------------------


class TestBuildGraphConfig:
    def _build(self, **kwargs):
        from src.server.handlers.chat.request_prep import build_graph_config

        defaults = dict(
            thread_id="t-1",
            user_id="u-1",
            workspace_id="ws-1",
            timezone_str="America/New_York",
            token_callback=MagicMock(),
            request=MagicMock(
                locale="en-US",
                checkpoint_id=None,
                reasoning_effort=None,
                fast_mode=None,
                platform=None,
            ),
            effective_model="gpt-4o",
        )
        defaults.update(kwargs)
        with (
            patch(f"{PREP}.get_langsmith_tags", return_value=["tag1"]),
            patch(f"{PREP}.get_langsmith_metadata", return_value={"k": "v"}),
        ):
            return build_graph_config(**defaults)

    def test_ptc_config(self):
        config = self._build()
        assert "agent_mode" not in config["configurable"]
        assert config["configurable"]["thread_id"] == "t-1"
        assert config["recursion_limit"] == UNBOUNDED_RECURSION_LIMIT

    def test_a_turn_is_tagged_as_chat_and_by_its_locale(self):
        from src.server.handlers.chat.request_prep import build_graph_config

        config = build_graph_config(
            thread_id="t-1",
            user_id="u-1",
            workspace_id="ws-1",
            timezone_str="UTC",
            token_callback=None,
            request=MagicMock(locale="zh-CN", checkpoint_id=None, platform=None),
            effective_model="gpt-4o",
        )
        assert config["tags"] == ["workflow:chat", "locale:zh-CN"]

    def test_checkpoint_id_added(self):
        request = MagicMock(
            locale="en-US",
            checkpoint_id="cp-123",
            reasoning_effort=None,
            fast_mode=None,
            platform=None,
        )
        config = self._build(request=request)
        assert config["configurable"]["checkpoint_id"] == "cp-123"

    def test_no_checkpoint_id(self):
        config = self._build()
        assert "checkpoint_id" not in config["configurable"]

    def test_base_checkpoint_id_when_the_request_names_none(self):
        config = self._build(base_checkpoint_id="cp-base")
        assert config["configurable"]["checkpoint_id"] == "cp-base"

    def test_request_checkpoint_id_over_the_base(self):
        request = MagicMock(
            locale="en-US",
            checkpoint_id="cp-123",
            reasoning_effort=None,
            fast_mode=None,
            platform=None,
        )
        config = self._build(request=request, base_checkpoint_id="cp-base")
        assert config["configurable"]["checkpoint_id"] == "cp-123"

    def test_token_callback_in_callbacks(self):
        cb = MagicMock()
        config = self._build(token_callback=cb)
        assert config["callbacks"] == [cb]

    def test_no_callbacks_when_none(self):
        config = self._build(token_callback=None)
        assert "callbacks" not in config

    @pytest.mark.parametrize("token_callback", [MagicMock(), None])
    def test_traced_turn_names_a_tracer_that_drops_its_tree(self, token_callback):
        """A turn has no step cap, so the tracer langchain_core would build
        holds a tree that grows for as long as the turn runs."""
        from langsmith.run_helpers import tracing_context

        from src.observability.langsmith_tracer import NonRetainingTracer

        with tracing_context(enabled=True, client=MagicMock()):
            config = self._build(token_callback=token_callback)

        *rest, tracer = config["callbacks"]
        assert rest == ([token_callback] if token_callback else [])
        assert type(tracer) is NonRetainingTracer

    def test_untraced_turn_names_no_tracer(self):
        from langsmith.run_helpers import tracing_context

        cb = MagicMock()
        with tracing_context(enabled=False):
            config = self._build(token_callback=cb)
        assert config["callbacks"] == [cb]

    def test_extra_configurable_merged(self):
        config = self._build(extra_configurable={"custom_flag": True})
        assert config["configurable"]["custom_flag"] is True

    def test_a_command_report_back_names_its_command(self):
        """The call that reads the notice brings the command's data sources,
        read off its row by this id; the trace itself never rides the body."""
        from ptc_agent.agent.middleware.provenance.command_trace import (
            REPORT_COMMAND_KEY,
        )
        from src.server.models.chat import CommandReport, TaskReport

        command = CommandReport(
            kind="command_report",
            command_id="bg-0123456789ab",
            tool="ExecuteCode",
            status="completed",
        )
        task = TaskReport(kind="task", task_id="t-1", display_id="Task-abc123")

        def _config(report):
            return self._build(
                request=MagicMock(
                    locale="en-US", checkpoint_id=None, platform=None,
                    report_source=report,
                )
            )["configurable"]

        assert _config(command)[REPORT_COMMAND_KEY] == "bg-0123456789ab"
        assert REPORT_COMMAND_KEY not in _config(task)
        assert REPORT_COMMAND_KEY not in _config(None)

    def test_graph_metadata_stays_turn_identity(self):
        """A subagent inherits this dict wholesale while running its own model at
        its own effort, so anything the LLM owns is asserted for calls it was
        never true of. Those keys live on the client (see ``LLM.get_llm``)."""
        from src.config.settings import get_langsmith_metadata
        from src.server.handlers.chat.request_prep import build_graph_config

        with (
            patch(f"{PREP}.get_langsmith_tags", return_value=[]),
            patch(f"{PREP}.get_langsmith_metadata", side_effect=get_langsmith_metadata),
        ):
            config = build_graph_config(
                thread_id="t-1",
                user_id="u-1",
                workspace_id="ws-1",
                timezone_str="UTC",
                token_callback=None,
                request=MagicMock(
                    locale=None, checkpoint_id=None, reasoning_effort="high",
                    fast_mode=True, platform=None,
                ),
                effective_model="claude-sonnet-5",
            )

        metadata = config["metadata"]
        assert metadata["user_id"] == "u-1"
        # The turn's own selection, which is a different question from what any
        # one call hit; cost and latency charts group on it.
        assert metadata["llm_model"] == "claude-sonnet-5"
        for llm_owned in (
            "reasoning_effort",
            "prompt_guidance",
            "compaction_profile",
            "fast_mode",
            "is_byok",
        ):
            assert llm_owned not in metadata

    @pytest.mark.parametrize(
        ("origin", "expected"),
        [
            (None, {"origin": "user"}),
            (
                {"type": "automation", "id": "auto-1", "name": "Morning brief"},
                {"origin": "automation", "automation_id": "auto-1"},
            ),
            ({"type": "agent", "id": "t-0", "agent": "chief_of_staff"}, {"origin": "agent"}),
            ({"type": "system"}, {"origin": "system"}),
        ],
    )
    def test_root_run_says_what_drove_the_turn(self, origin, expected):
        from src.server.models.chat import ThreadOrigin

        request = MagicMock(
            locale=None,
            checkpoint_id=None,
            platform=None,
            origin=ThreadOrigin(**origin) if origin else None,
            query_type=None,
        )
        metadata = self._build(request=request, from_server=True)["metadata"]
        assert {k: v for k, v in metadata.items() if k != "k"} == expected

    @pytest.mark.parametrize(
        "origin",
        [{"type": "automation", "id": "auto-1"}, {"type": "agent", "agent": "chief_of_staff"}],
    )
    def test_an_origin_a_client_names_is_not_believed(self, origin):
        """Any client may send one; only the server's own labels the trace."""
        from src.server.models.chat import ThreadOrigin

        request = MagicMock(
            locale=None,
            checkpoint_id=None,
            platform=None,
            origin=ThreadOrigin(**origin),
            query_type=None,
        )
        metadata = self._build(request=request)["metadata"]
        assert {k: v for k, v in metadata.items() if k != "k"} == {"origin": "user"}

    @pytest.mark.parametrize("query_type", ["system", "market_target", "wakeup"])
    def test_a_turn_the_server_posted_is_not_the_users(self, query_type):
        request = MagicMock(
            locale=None, checkpoint_id=None, platform=None, origin=None, query_type=query_type
        )
        metadata = self._build(request=request)["metadata"]
        assert {k: v for k, v in metadata.items() if k != "k"} == {
            "origin": "system",
            "query_type": query_type,
        }

    def test_timezone_in_configurable(self):
        config = self._build(timezone_str="UTC")
        assert config["configurable"]["timezone"] == "UTC"

    def test_skill_contexts_and_dirs_threaded_to_configurable(self):
        """The server→middleware handoff: both skill_contexts and skill_dirs must
        land in ``configurable`` so SkillsMiddleware can inject + locate bodies."""
        config = self._build(
            skill_contexts=[{"name": "chart-annotation", "instruction": "AAPL:1d"}],
            skill_dirs=["/skills"],
        )
        assert config["configurable"]["skill_contexts"] == [
            {"name": "chart-annotation", "instruction": "AAPL:1d"}
        ]
        assert config["configurable"]["skill_dirs"] == ["/skills"]

    def test_skill_contexts_without_dirs_omits_dirs(self):
        """skill_dirs is nested under the skill_contexts gate; contexts may be
        present while dirs default (middleware falls back to project_root/skills)."""
        config = self._build(
            skill_contexts=[{"name": "research"}], skill_dirs=None
        )
        assert config["configurable"]["skill_contexts"] == [{"name": "research"}]
        assert "skill_dirs" not in config["configurable"]

    def test_skill_dirs_without_contexts_is_dropped(self):
        """No skills requested → neither key is set, even if skill_dirs is passed
        (the ``if skill_contexts`` gate guards the whole block)."""
        config = self._build(skill_contexts=None, skill_dirs=["/skills"])
        assert "skill_contexts" not in config["configurable"]
        assert "skill_dirs" not in config["configurable"]
