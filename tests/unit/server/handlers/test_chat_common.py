"""
Tests for src/server/handlers/chat/_common.py shared helpers.

Covers:
- classify_error: recoverable vs non-recoverable error classification
- process_hitl_response: 4-tuple return, various HITL scenarios
- normalize_request_messages: dict conversion, multimodal, empty
- init_tracking: returns (TokenTrackingManager, ToolUsageTracker)
- apply_fetch_override: sets context vars
- ensure_thread: correct DB call with kwargs
- persist_or_skip_replay: skip for replay, persist otherwise
- inject_skills: skill injection for flash and ptc modes
- build_graph_config: mode parameterization, optional fields
- wait_or_steer: ready, steered, and 409 cases
- serialize_context_metadata: context serialization + slash fallback
- setup_steering_tracking: wires callback on handler
"""

from unittest.mock import AsyncMock, MagicMock, patch

import psycopg
import pytest


COMMON = "src.server.handlers.chat._common"


# ---------------------------------------------------------------------------
# classify_error
# ---------------------------------------------------------------------------


class TestClassifyError:
    def _classify(self, e):
        from src.server.handlers.chat._common import classify_error

        return classify_error(e)

    def test_non_recoverable_attribute_error(self):
        result = self._classify(AttributeError("no attribute 'x'"))
        assert result["is_non_recoverable"] is True
        assert result["is_recoverable"] is False
        assert result["error_type"] is None

    def test_non_recoverable_type_error(self):
        result = self._classify(TypeError("expected int"))
        assert result["is_non_recoverable"] is True
        assert result["is_recoverable"] is False

    def test_non_recoverable_key_error(self):
        result = self._classify(KeyError("missing"))
        assert result["is_non_recoverable"] is True

    def test_non_recoverable_name_error(self):
        result = self._classify(NameError("undefined"))
        assert result["is_non_recoverable"] is True

    def test_non_recoverable_syntax_error(self):
        result = self._classify(SyntaxError("bad syntax"))
        assert result["is_non_recoverable"] is True

    def test_non_recoverable_import_error(self):
        result = self._classify(ImportError("no module"))
        assert result["is_non_recoverable"] is True

    def test_recoverable_timeout_error(self):
        result = self._classify(TimeoutError("timed out"))
        assert result["is_recoverable"] is True
        assert result["error_type"] == "timeout_error"

    def test_recoverable_connection_error(self):
        result = self._classify(ConnectionError("refused"))
        assert result["is_recoverable"] is True
        assert result["error_type"] == "connection_error"

    def test_recoverable_timeout_in_message(self):
        result = self._classify(RuntimeError("request timed out"))
        assert result["is_recoverable"] is True
        assert result["error_type"] == "timeout_error"

    def test_recoverable_connection_in_message(self):
        result = self._classify(RuntimeError("connection refused"))
        assert result["is_recoverable"] is True
        assert result["error_type"] == "connection_error"

    def test_recoverable_api_error_500(self):
        result = self._classify(RuntimeError("error code: 500"))
        assert result["is_recoverable"] is True
        assert result["error_type"] == "api_error"

    def test_recoverable_rate_limit(self):
        result = self._classify(RuntimeError("rate limit exceeded"))
        assert result["is_recoverable"] is True
        assert result["error_type"] == "api_error"

    def test_recoverable_service_unavailable(self):
        result = self._classify(RuntimeError("service unavailable"))
        assert result["is_recoverable"] is True
        assert result["error_type"] == "api_error"

    def test_recoverable_postgres_operational_error(self):
        err = psycopg.OperationalError("server closed the connection unexpectedly")
        result = self._classify(err)
        assert result["is_recoverable"] is True
        assert result["error_type"] == "connection_error"

    def test_generic_runtime_error_not_recoverable(self):
        result = self._classify(RuntimeError("something went wrong"))
        assert result["is_recoverable"] is False
        assert result["error_type"] is None

    def test_generic_value_error_not_recoverable(self):
        result = self._classify(ValueError("invalid input"))
        assert result["is_recoverable"] is False
        assert result["is_non_recoverable"] is False
        assert result["error_type"] is None

    def test_api_class_name_match(self):
        """Exception classes with 'api' in the name are considered API errors."""

        class CustomAPIError(Exception):
            pass

        result = self._classify(CustomAPIError("something"))
        assert result["is_recoverable"] is True
        assert result["error_type"] == "api_error"

    def test_non_recoverable_takes_precedence(self):
        """Even if message matches recoverable patterns, non-recoverable type wins."""
        result = self._classify(TypeError("connection timeout"))
        assert result["is_non_recoverable"] is True
        assert result["is_recoverable"] is False


# ---------------------------------------------------------------------------
# process_hitl_response
# ---------------------------------------------------------------------------


class TestProcessHitlResponse:
    def _make_request(self, hitl_response):
        req = MagicMock()
        req.hitl_response = hitl_response
        return req

    def test_approve_with_message(self):
        from src.server.handlers.chat._common import process_hitl_response

        response = MagicMock()
        response.decisions = [MagicMock(type="approve", message="yes please")]
        req = self._make_request({"int-1": response})

        with patch(
            f"{COMMON}.summarize_hitl_response_map",
            return_value={
                "feedback_action": "QUESTION_ANSWERED",
                "content": "approved: yes please",
                "interrupt_ids": ["int-1"],
            },
        ):
            action, content, answers, ids = process_hitl_response(req)

        assert action == "QUESTION_ANSWERED"
        assert ids == ["int-1"]
        assert answers["int-1"] == "yes please"

    def test_reject_without_message(self):
        from src.server.handlers.chat._common import process_hitl_response

        response = MagicMock()
        response.decisions = [MagicMock(type="reject", message="")]
        req = self._make_request({"int-1": response})

        with patch(
            f"{COMMON}.summarize_hitl_response_map",
            return_value={
                "feedback_action": "QUESTION_SKIPPED",
                "content": "rejected",
                "interrupt_ids": ["int-1"],
            },
        ):
            action, content, answers, ids = process_hitl_response(req)

        assert action == "QUESTION_SKIPPED"
        assert answers["int-1"] is None

    def test_dict_style_response(self):
        """HITL response as plain dict (not Pydantic model)."""
        from src.server.handlers.chat._common import process_hitl_response

        response = {"decisions": [{"type": "approve", "message": "ok"}]}
        req = self._make_request({"int-1": response})

        with patch(
            f"{COMMON}.summarize_hitl_response_map",
            return_value={
                "feedback_action": "QUESTION_ANSWERED",
                "content": "ok",
                "interrupt_ids": ["int-1"],
            },
        ):
            action, content, answers, ids = process_hitl_response(req)

        assert answers["int-1"] == "ok"

    def test_multiple_interrupts(self):
        from src.server.handlers.chat._common import process_hitl_response

        r1 = MagicMock()
        r1.decisions = [MagicMock(type="approve", message="answer 1")]
        r2 = MagicMock()
        r2.decisions = [MagicMock(type="reject", message="")]
        req = self._make_request({"int-1": r1, "int-2": r2})

        with patch(
            f"{COMMON}.summarize_hitl_response_map",
            return_value={
                "feedback_action": "QUESTION_ANSWERED",
                "content": "mixed",
                "interrupt_ids": ["int-1", "int-2"],
            },
        ):
            action, content, answers, ids = process_hitl_response(req)

        assert action == "QUESTION_ANSWERED"
        assert answers["int-1"] == "answer 1"
        assert answers["int-2"] is None

    def test_empty_decisions(self):
        from src.server.handlers.chat._common import process_hitl_response

        response = MagicMock()
        response.decisions = []
        req = self._make_request({"int-1": response})

        with patch(
            f"{COMMON}.summarize_hitl_response_map",
            return_value={
                "feedback_action": "QUESTION_SKIPPED",
                "content": "",
                "interrupt_ids": ["int-1"],
            },
        ):
            action, content, answers, ids = process_hitl_response(req)

        assert answers == {}
        assert action == "QUESTION_SKIPPED"


# ---------------------------------------------------------------------------
# normalize_request_messages
# ---------------------------------------------------------------------------


class TestNormalizeRequestMessages:
    def _make_request(self, messages):
        req = MagicMock()
        req.messages = messages
        return req

    def test_string_content(self):
        from src.server.handlers.chat._common import normalize_request_messages

        msg = MagicMock()
        msg.role = "user"
        msg.content = "hello"
        result = normalize_request_messages(self._make_request([msg]))
        assert result == [{"role": "user", "content": "hello"}]

    def test_list_content_text(self):
        from src.server.handlers.chat._common import normalize_request_messages

        item = MagicMock()
        item.type = "text"
        item.text = "hello"
        msg = MagicMock()
        msg.role = "user"
        msg.content = [item]
        result = normalize_request_messages(self._make_request([msg]))
        assert result[0]["content"] == [{"type": "text", "text": "hello"}]

    def test_list_content_image(self):
        from src.server.handlers.chat._common import normalize_request_messages

        item = MagicMock()
        item.type = "image"
        item.text = None
        item.image_url = "https://example.com/img.png"
        msg = MagicMock()
        msg.role = "user"
        msg.content = [item]
        result = normalize_request_messages(self._make_request([msg]))
        assert result[0]["content"] == [
            {"type": "image_url", "image_url": {"url": "https://example.com/img.png"}}
        ]

    def test_empty_messages(self):
        from src.server.handlers.chat._common import normalize_request_messages

        result = normalize_request_messages(self._make_request([]))
        assert result == []

    def test_multiple_messages(self):
        from src.server.handlers.chat._common import normalize_request_messages

        m1 = MagicMock(role="user", content="hello")
        m2 = MagicMock(role="assistant", content="hi there")
        m3 = MagicMock(role="user", content="thanks")
        result = normalize_request_messages(self._make_request([m1, m2, m3]))
        assert len(result) == 3
        assert result[0]["role"] == "user"
        assert result[1]["role"] == "assistant"
        assert result[2]["content"] == "thanks"


# ---------------------------------------------------------------------------
# init_tracking
# ---------------------------------------------------------------------------


class TestInitTracking:
    def test_returns_tuple(self):
        from src.server.handlers.chat._common import init_tracking

        with (
            patch(
                f"{COMMON}.TokenTrackingManager.initialize_tracking",
                return_value=MagicMock(),
            ) as mock_token,
            patch(
                f"{COMMON}.ToolUsageTracker",
                return_value=MagicMock(),
            ) as mock_tool,
        ):
            token_cb, tool_tr = init_tracking("thread-1")

        mock_token.assert_called_once_with(thread_id="thread-1", track_tokens=True)
        mock_tool.assert_called_once_with(thread_id="thread-1")
        assert token_cb is not None
        assert tool_tr is not None


# ---------------------------------------------------------------------------
# apply_fetch_override
# ---------------------------------------------------------------------------


class TestApplyFetchOverride:
    def test_sets_context_vars(self):
        from src.server.handlers.chat._common import apply_fetch_override

        config = MagicMock()
        config.llm.fetch = "gpt-4o-mini"
        config.subsidiary_llm_clients = {"fetch": MagicMock()}

        with (
            patch(f"{COMMON}.fetch_model_override") as mock_model_var,
            patch(f"{COMMON}.fetch_llm_client_override") as mock_client_var,
        ):
            apply_fetch_override(config)

        mock_model_var.set.assert_called_once_with("gpt-4o-mini")
        mock_client_var.set.assert_called_once_with(
            config.subsidiary_llm_clients["fetch"]
        )

    def test_skips_when_no_fetch(self):
        from src.server.handlers.chat._common import apply_fetch_override

        config = MagicMock()
        config.llm.fetch = None

        with (
            patch(f"{COMMON}.fetch_model_override") as mock_model_var,
            patch(f"{COMMON}.fetch_llm_client_override") as mock_client_var,
        ):
            apply_fetch_override(config)

        mock_model_var.set.assert_not_called()
        mock_client_var.set.assert_not_called()

    def test_skips_client_when_not_in_subsidiary(self):
        from src.server.handlers.chat._common import apply_fetch_override

        config = MagicMock()
        config.llm.fetch = "gpt-4o-mini"
        config.subsidiary_llm_clients = {}

        with (
            patch(f"{COMMON}.fetch_model_override") as mock_model_var,
            patch(f"{COMMON}.fetch_llm_client_override") as mock_client_var,
        ):
            apply_fetch_override(config)

        mock_model_var.set.assert_called_once()
        mock_client_var.set.assert_not_called()


# ---------------------------------------------------------------------------
# ensure_thread
# ---------------------------------------------------------------------------


class TestEnsureThread:
    @pytest.mark.asyncio
    async def test_basic_call(self):
        from src.server.handlers.chat._common import ensure_thread

        request = MagicMock()
        request.external_thread_id = None
        request.platform = None

        with patch(f"{COMMON}.qr_db.ensure_thread_exists", new_callable=AsyncMock) as mock_db:
            await ensure_thread(
                request, "t-1", "ws-1", "u-1", msg_type="flash", initial_query="hello"
            )

        mock_db.assert_called_once_with(
            workspace_id="ws-1",
            conversation_thread_id="t-1",
            user_id="u-1",
            initial_query="hello",
            initial_status="in_progress",
            msg_type="flash",
        )

    @pytest.mark.asyncio
    async def test_with_external_thread(self):
        from src.server.handlers.chat._common import ensure_thread

        request = MagicMock()
        request.external_thread_id = "ext-123"
        request.platform = "slack"

        with patch(f"{COMMON}.qr_db.ensure_thread_exists", new_callable=AsyncMock) as mock_db:
            await ensure_thread(
                request, "t-1", "ws-1", "u-1", msg_type="ptc", initial_query=""
            )

        call_kwargs = mock_db.call_args.kwargs
        assert call_kwargs["external_id"] == "ext-123"
        assert call_kwargs["platform"] == "slack"

    @pytest.mark.asyncio
    async def test_default_initial_query(self):
        from src.server.handlers.chat._common import ensure_thread

        request = MagicMock()
        request.external_thread_id = None
        request.platform = None

        with patch(f"{COMMON}.qr_db.ensure_thread_exists", new_callable=AsyncMock) as mock_db:
            await ensure_thread(request, "t-1", "ws-1", "u-1", msg_type="flash")

        call_kwargs = mock_db.call_args.kwargs
        assert call_kwargs["initial_query"] == ""


# ---------------------------------------------------------------------------
# persist_or_skip_replay
# ---------------------------------------------------------------------------


class TestPersistOrSkipReplay:
    @pytest.mark.asyncio
    async def test_checkpoint_replay_marks_persisted(self):
        from src.server.handlers.chat._common import persist_or_skip_replay

        persistence = MagicMock()
        persistence.mark_query_persisted = MagicMock()
        request = MagicMock()
        request.fork_from_turn = 3

        await persist_or_skip_replay(
            persistence_service=persistence,
            is_checkpoint_replay=True,
            request=request,
            query_content="",
            query_type="regenerate",
            feedback_action=None,
            query_metadata={},
            thread_id="t-1",
            log_prefix="TEST",
        )

        persistence.mark_query_persisted.assert_called_once_with(3)

    @pytest.mark.asyncio
    async def test_checkpoint_replay_no_fork_calculates_turn(self):
        from src.server.handlers.chat._common import persist_or_skip_replay

        persistence = MagicMock()
        persistence.get_or_calculate_turn_index = AsyncMock(return_value=5)
        persistence.mark_query_persisted = MagicMock()
        request = MagicMock()
        request.fork_from_turn = None

        await persist_or_skip_replay(
            persistence_service=persistence,
            is_checkpoint_replay=True,
            request=request,
            query_content="",
            query_type="regenerate",
            feedback_action=None,
            query_metadata={},
            thread_id="t-1",
            log_prefix="TEST",
        )

        persistence.mark_query_persisted.assert_called_once_with(5)

    @pytest.mark.asyncio
    async def test_normal_query_persists(self):
        from src.server.handlers.chat._common import persist_or_skip_replay

        persistence = MagicMock()
        persistence.persist_query_start = AsyncMock()
        request = MagicMock()

        await persist_or_skip_replay(
            persistence_service=persistence,
            is_checkpoint_replay=False,
            request=request,
            query_content="hello",
            query_type="initial",
            feedback_action=None,
            query_metadata={"msg_type": "flash"},
            thread_id="t-1",
            log_prefix="TEST",
        )

        persistence.persist_query_start.assert_called_once_with(
            content="hello",
            query_type="initial",
            feedback_action=None,
            metadata={"msg_type": "flash"},
        )


# ---------------------------------------------------------------------------
# build_graph_config
# ---------------------------------------------------------------------------


class TestBuildGraphConfig:
    def _build(self, **kwargs):
        from src.server.handlers.chat._common import build_graph_config

        defaults = dict(
            thread_id="t-1",
            user_id="u-1",
            workspace_id="ws-1",
            mode="flash",
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
            is_byok=False,
            recursion_limit=100,
        )
        defaults.update(kwargs)
        with (
            patch(f"{COMMON}.get_langsmith_tags", return_value=["tag1"]),
            patch(f"{COMMON}.get_langsmith_metadata", return_value={"k": "v"}),
        ):
            return build_graph_config(**defaults)

    def test_basic_flash_config(self):
        config = self._build(mode="flash", recursion_limit=500)
        assert config["configurable"]["agent_mode"] == "flash"
        assert config["configurable"]["thread_id"] == "t-1"
        assert config["recursion_limit"] == 500

    def test_ptc_config_with_plan_mode(self):
        config = self._build(mode="ptc", plan_mode=True, recursion_limit=2000)
        assert config["configurable"]["agent_mode"] == "ptc"
        assert config["recursion_limit"] == 2000

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

    def test_token_callback_in_callbacks(self):
        cb = MagicMock()
        config = self._build(token_callback=cb)
        assert config["callbacks"] == [cb]

    def test_no_callbacks_when_none(self):
        config = self._build(token_callback=None)
        assert "callbacks" not in config

    def test_extra_configurable_merged(self):
        config = self._build(extra_configurable={"plan_mode": True})
        assert config["configurable"]["plan_mode"] is True

    def test_timezone_in_configurable(self):
        config = self._build(timezone_str="UTC")
        assert config["configurable"]["timezone"] == "UTC"


# ---------------------------------------------------------------------------
# wait_or_steer
# ---------------------------------------------------------------------------


class TestWaitOrSteer:
    @pytest.mark.asyncio
    async def test_ready_returns_true(self):
        from src.server.handlers.chat._common import wait_or_steer

        manager = AsyncMock()
        manager.wait_for_soft_interrupted = AsyncMock(return_value=True)

        ready, event = await wait_or_steer(manager, "t-1", "hello", "u-1")
        assert ready is True
        assert event is None

    @pytest.mark.asyncio
    async def test_steered_returns_false_with_event(self):
        from src.server.handlers.chat._common import wait_or_steer

        manager = AsyncMock()
        manager.wait_for_soft_interrupted = AsyncMock(return_value=False)

        with patch(
            "src.server.handlers.chat.steering.steer_thread",
            new_callable=AsyncMock,
            return_value={"position": 1},
        ):
            ready, event = await wait_or_steer(
                manager, "t-1", "hello", "u-1"
            )

        assert ready is False
        assert event is not None
        assert "steering_accepted" in event
        assert '"position": 1' in event

    @pytest.mark.asyncio
    async def test_raises_409_when_queue_fails(self):
        from fastapi import HTTPException

        from src.server.handlers.chat._common import wait_or_steer

        manager = AsyncMock()
        manager.wait_for_soft_interrupted = AsyncMock(return_value=False)

        with (
            patch(
                "src.server.handlers.chat.steering.steer_thread",
                new_callable=AsyncMock,
                return_value=None,
            ),
            pytest.raises(HTTPException) as exc_info,
        ):
            await wait_or_steer(manager, "t-1", "hello", "u-1")

        assert exc_info.value.status_code == 409


# ---------------------------------------------------------------------------
# serialize_context_metadata
# ---------------------------------------------------------------------------


class TestSerializeContextMetadata:
    def _make_request(self, additional_context=None, hitl_response=None):
        req = MagicMock()
        req.additional_context = additional_context
        req.hitl_response = hitl_response
        return req

    def test_serializes_skills_and_directives(self):
        from src.server.handlers.chat._common import serialize_context_metadata

        skill_ctx = MagicMock(type="skills")
        skill_ctx.name = "research"
        directive_ctx = MagicMock(type="directive", content="be concise")
        other_ctx = MagicMock(type="multimodal")

        req = self._make_request(
            additional_context=[skill_ctx, directive_ctx, other_ctx]
        )
        metadata = {}
        serialize_context_metadata(req, metadata, "hello", mode="flash")

        assert metadata["additional_context"] == [
            {"type": "skills", "name": "research"},
            {"type": "directive", "content": "be concise"},
        ]

    def test_slash_command_fallback(self):
        from src.server.handlers.chat._common import serialize_context_metadata

        req = self._make_request(additional_context=None)
        metadata = {}

        mock_skill = MagicMock(name="research")
        with patch(
            f"{COMMON}.detect_slash_commands",
            return_value=("hello", [mock_skill]),
        ):
            serialize_context_metadata(req, metadata, "hello", mode="flash")

        assert "additional_context" in metadata
        assert metadata["additional_context"][0]["type"] == "skills"

    def test_no_slash_commands_found(self):
        from src.server.handlers.chat._common import serialize_context_metadata

        req = self._make_request(additional_context=None)
        metadata = {}

        with patch(
            f"{COMMON}.detect_slash_commands",
            return_value=("hello", []),
        ):
            serialize_context_metadata(req, metadata, "hello", mode="flash")

        assert "additional_context" not in metadata

    def test_hitl_response_skips_slash_commands(self):
        from src.server.handlers.chat._common import serialize_context_metadata

        req = self._make_request(additional_context=None, hitl_response={"int-1": {}})
        metadata = {}

        with patch(f"{COMMON}.detect_slash_commands") as mock_detect:
            serialize_context_metadata(req, metadata, "hello", mode="flash")

        mock_detect.assert_not_called()

    def test_existing_additional_context_prevents_fallback(self):
        from src.server.handlers.chat._common import serialize_context_metadata

        skill_ctx = MagicMock(type="skills")
        skill_ctx.name = "research"
        req = self._make_request(additional_context=[skill_ctx])
        metadata = {}

        with patch(f"{COMMON}.detect_slash_commands") as mock_detect:
            serialize_context_metadata(req, metadata, "hello", mode="ptc")

        # Should not call detect_slash_commands since additional_context was serialized
        mock_detect.assert_not_called()


# ---------------------------------------------------------------------------
# setup_steering_tracking
# ---------------------------------------------------------------------------


class TestSetupSteeringTracking:
    @pytest.mark.asyncio
    async def test_wires_callback(self):
        from src.server.handlers.chat._common import setup_steering_tracking

        handler = MagicMock()
        handler.injected_steerings = []
        handler.on_steering_delivered = None

        setup_steering_tracking(handler)

        assert handler.on_steering_delivered is not None

    @pytest.mark.asyncio
    async def test_callback_filters_empty_content(self):
        from src.server.handlers.chat._common import setup_steering_tracking

        handler = MagicMock()
        handler.injected_steerings = []

        setup_steering_tracking(handler)

        # Call the wired callback
        callback = handler.on_steering_delivered
        await callback([
            {"content": "hello", "role": "user"},
            {"content": "", "role": "user"},
            {"role": "user"},  # no content key
            {"content": "world", "role": "user"},
        ])

        assert len(handler.injected_steerings) == 2
        assert handler.injected_steerings[0]["content"] == "hello"
        assert handler.injected_steerings[1]["content"] == "world"


# ---------------------------------------------------------------------------
# inject_skills
# ---------------------------------------------------------------------------


class TestInjectSkills:
    def test_no_skills_returns_empty(self):
        from src.server.handlers.chat._common import inject_skills

        request = MagicMock()
        request.additional_context = None
        request.hitl_response = None
        messages = [{"role": "user", "content": "hello"}]
        config = MagicMock()

        with patch(f"{COMMON}.parse_skill_contexts", return_value=[]):
            result = inject_skills(messages, request, config, mode="flash")

        assert result == []

    def test_skill_from_additional_context(self):
        from src.server.handlers.chat._common import inject_skills

        request = MagicMock()
        request.additional_context = [MagicMock(type="skills")]
        request.hitl_response = None
        messages = [{"role": "user", "content": "hello"}]
        config = MagicMock()
        config.skills.local_skill_dirs_with_sandbox.return_value = [
            ("/skills/dir", "/sandbox/dir")
        ]

        skill_result = MagicMock()
        skill_result.content = "skill content"
        skill_result.loaded_skill_names = ["research"]

        with (
            patch(f"{COMMON}.parse_skill_contexts", return_value=["skill_ctx"]),
            patch(f"{COMMON}.build_skill_content", return_value=skill_result),
        ):
            result = inject_skills(messages, request, config, mode="flash")

        assert result == ["research"]
        assert "skill content" in messages[0]["content"]

    def test_slash_command_detection_fallback(self):
        from src.server.handlers.chat._common import inject_skills

        request = MagicMock()
        request.additional_context = None
        request.hitl_response = None
        messages = [{"role": "user", "content": "/research market analysis"}]
        config = MagicMock()
        config.skills.local_skill_dirs_with_sandbox.return_value = []

        detected_skill = MagicMock()
        skill_result = MagicMock()
        skill_result.content = "research skill loaded"
        skill_result.loaded_skill_names = ["research"]

        with (
            patch(f"{COMMON}.parse_skill_contexts", return_value=[]),
            patch(
                f"{COMMON}.detect_slash_commands",
                return_value=("market analysis", [detected_skill]),
            ),
            patch(f"{COMMON}.build_skill_content", return_value=skill_result),
        ):
            result = inject_skills(messages, request, config, mode="flash")

        assert result == ["research"]
        # The message text should be cleaned (slash command stripped) then
        # skill content appended
        assert messages[0]["content"].startswith("market analysis")
        assert "research skill loaded" in messages[0]["content"]

    def test_hitl_response_skips_slash_detection(self):
        from src.server.handlers.chat._common import inject_skills

        request = MagicMock()
        request.additional_context = None
        request.hitl_response = {"int-1": {}}
        messages = [{"role": "user", "content": "/research something"}]
        config = MagicMock()

        with (
            patch(f"{COMMON}.parse_skill_contexts", return_value=[]),
            patch(f"{COMMON}.detect_slash_commands") as mock_detect,
        ):
            result = inject_skills(messages, request, config, mode="flash")

        mock_detect.assert_not_called()
        assert result == []


# ---------------------------------------------------------------------------
# _resolve_timezone
# ---------------------------------------------------------------------------


class TestResolveTimezone:
    def test_valid_timezone(self):
        from src.server.handlers.chat._common import _resolve_timezone

        result = _resolve_timezone("America/New_York", "en-US")
        assert result == "America/New_York"

    def test_invalid_timezone_falls_back(self):
        from src.server.handlers.chat._common import _resolve_timezone

        with patch(
            f"{COMMON}.get_locale_config",
            return_value={"timezone": "Asia/Shanghai"},
        ):
            result = _resolve_timezone("Invalid/Zone", "zh-CN")

        assert result == "Asia/Shanghai"

    def test_none_timezone_falls_back(self):
        from src.server.handlers.chat._common import _resolve_timezone

        with patch(
            f"{COMMON}.get_locale_config",
            return_value={"timezone": "UTC"},
        ):
            result = _resolve_timezone(None, "en-US")

        assert result == "UTC"

    def test_none_locale_uses_default(self):
        from src.server.handlers.chat._common import _resolve_timezone

        with patch(
            f"{COMMON}.get_locale_config",
            return_value={"timezone": "UTC"},
        ) as mock_locale:
            result = _resolve_timezone(None, None)

        mock_locale.assert_called_once_with("en-US", "en")
        assert result == "UTC"
