"""
Tests for src/server/handlers/chat/_common.py shared helpers.

Covers:
- classify_error: recoverable vs non-recoverable error classification
- process_hitl_response: 4-tuple return, various HITL scenarios
- normalize_request_messages: dict conversion, multimodal, empty
- init_tracking: returns (TokenTrackingManager, ToolUsageTracker)
- apply_fetch_override: sets context vars
- ensure_thread: correct DB call with kwargs
- inject_skills: skill injection for flash and ptc modes
- build_graph_config: mode parameterization, optional fields
- wait_or_steer: ready, steered, and 409 cases
- serialize_context_metadata: context serialization + slash fallback
- setup_steering_tracking: wires callback on handler
"""

from unittest.mock import AsyncMock, MagicMock, patch

import psycopg
import pytest

from ptc_agent.core.sandbox.runtime import SandboxGoneError, SandboxTransientError
from src.server.models.additional_context import SkillContext

COMMON = "src.server.handlers.chat._common"


# ---------------------------------------------------------------------------
# classify_error
# ---------------------------------------------------------------------------


class TestClassifyError:
    def _classify(self, e):
        from src.server.handlers.chat.error_handling import classify_error

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

    def test_recoverable_sandbox_transient_error(self):
        result = self._classify(
            SandboxTransientError(
                "Transient sandbox transport error; operation failed after retries"
            )
        )
        assert result["is_recoverable"] is True
        assert result["error_type"] == "transient_error"

    def test_recoverable_sandbox_gone_error(self):
        result = self._classify(SandboxGoneError("sandbox-placeholder", "not found"))
        assert result["is_recoverable"] is True
        assert result["error_type"] == "transient_error"

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
# _classify_non_recoverable_error_type
# ---------------------------------------------------------------------------


class TestClassifyNonRecoverableErrorType:
    """Maps workspace-lifecycle errors to structured ``error_type`` labels
    so channel gateways can render user-actionable messages instead of
    raw traceback strings."""

    def _classify(self, e):
        from src.server.handlers.chat.error_handling import (
            _classify_non_recoverable_error_type,
        )
        return _classify_non_recoverable_error_type(e)

    def test_workspace_not_found_value_error(self):
        result = self._classify(ValueError("Workspace abc123 not found"))
        assert result == "workspace_not_found"

    def test_workspace_deleted_runtime_error(self):
        result = self._classify(
            RuntimeError("Workspace abc123 has been deleted"),
        )
        assert result == "workspace_deleted"

    def test_workspace_error_state_runtime_error(self):
        result = self._classify(
            RuntimeError(
                "Workspace abc123 is in error state. Please delete and recreate."
            ),
        )
        assert result == "workspace_error_state"

    def test_workspace_generic_runtime_error_falls_to_unavailable(self):
        """Unknown workspace error wording still gets the workspace bucket
        so consumers can show a workspace-shaped notice."""
        result = self._classify(RuntimeError("Workspace abc123 went sideways"))
        assert result == "workspace_unavailable"

    def test_non_workspace_error_defaults_to_workflow_error(self):
        result = self._classify(RuntimeError("LLM provider quota exceeded"))
        assert result == "workflow_error"

    def test_unrelated_value_error_defaults_to_workflow_error(self):
        result = self._classify(ValueError("invalid agent_mode"))
        assert result == "workflow_error"


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
# apply_fetch_override — real context-var contract (regression lock)
#
# These tests assert the actual ContextVar state transitions rather than mock
# call counts. Each case is isolated in its own copy_context().run() so no
# override leaks into other tests or the module-global vars.
# ---------------------------------------------------------------------------


class TestApplyFetchOverrideContextVars:
    """Contract tests using real ContextVars (no mocking of the vars themselves).

    Isolation: every case runs inside ``contextvars.copy_context().run(...)``
    so the process-global vars are untouched outside each sub-run.
    """

    def _run_and_capture(self, config):
        """Run apply_fetch_override in an isolated context; return snapshot."""
        import contextvars
        from src.server.handlers.chat._common import apply_fetch_override
        from src.tools.web.fetch import fetch_model_override, fetch_llm_client_override

        results = {}

        def _inner():
            apply_fetch_override(config)
            results["model"] = fetch_model_override.get()
            results["client"] = fetch_llm_client_override.get()

        contextvars.copy_context().run(_inner)
        return results

    def test_credentialed_user_sets_both_vars(self):
        """Credentialed (BYOK/OAuth) path: subsidiary_llm_clients['fetch'] is
        pre-populated by resolve_llm_config — apply_fetch_override must forward
        it verbatim into fetch_llm_client_override (fetch.py copies at use time).
        """
        fake_client = MagicMock(name="byok-fetch-client")
        config = MagicMock()
        config.llm.fetch = "claude-haiku-4-5"
        config.subsidiary_llm_clients = {"fetch": fake_client}

        snap = self._run_and_capture(config)

        assert snap["model"] == "claude-haiku-4-5"
        assert snap["client"] is fake_client

    def test_platform_user_leaves_client_var_unset(self):
        """Platform/system path: no entry in subsidiary_llm_clients → the
        client context var must remain None so fetch.py uses LLM(model).get_llm().
        """
        config = MagicMock()
        config.llm.fetch = "claude-haiku-4-5"
        config.subsidiary_llm_clients = {}  # platform user — nothing materialized

        snap = self._run_and_capture(config)

        assert snap["model"] == "claude-haiku-4-5"
        assert snap["client"] is None  # default — fetch.py takes the platform path

    def test_no_fetch_model_leaves_both_vars_unset(self):
        """When config.llm.fetch is falsy neither context var should be set."""
        config = MagicMock()
        config.llm.fetch = None
        config.subsidiary_llm_clients = {"fetch": MagicMock()}  # should be ignored

        snap = self._run_and_capture(config)

        assert snap["model"] is None   # ContextVar default
        assert snap["client"] is None  # ContextVar default

    def test_stored_client_is_not_copied_by_apply_fetch_override(self):
        """apply_fetch_override must store the raw shared instance (not a copy).
        fetch.py performs the .model_copy() at consumption time — this test
        guards against a double-copy regression.
        """
        fake_client = MagicMock(name="shared-client")
        config = MagicMock()
        config.llm.fetch = "claude-haiku-4-5"
        config.subsidiary_llm_clients = {"fetch": fake_client}

        snap = self._run_and_capture(config)

        # Must be the exact same object — no copy performed here.
        assert snap["client"] is fake_client
        fake_client.model_copy.assert_not_called()

    def test_context_isolation_across_cases(self):
        """Override set in one isolated run must not bleed into the next run."""
        import contextvars
        from src.server.handlers.chat._common import apply_fetch_override
        from src.tools.web.fetch import fetch_llm_client_override

        leak_sentinel = MagicMock(name="leaked-client")

        config_with = MagicMock()
        config_with.llm.fetch = "some-model"
        config_with.subsidiary_llm_clients = {"fetch": leak_sentinel}

        # First run sets the client in its own context copy.
        def _first():
            apply_fetch_override(config_with)
            assert fetch_llm_client_override.get() is leak_sentinel

        contextvars.copy_context().run(_first)

        # Process-global var must still be None.
        assert fetch_llm_client_override.get() is None


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


# ---------------------------------------------------------------------------
# wait_or_steer
# ---------------------------------------------------------------------------


_LIVE_ROW = {"conversation_response_id": "run-live", "cancel_requested_at": None}
_STOPPING_ROW = {
    "conversation_response_id": "run-live",
    "cancel_requested_at": "2026-07-14T00:00:00+00:00",
}


class TestWaitOrSteer:
    @pytest.fixture(autouse=True)
    def ledger_slot(self):
        """The ledger slot read behind wait_or_steer's accept-after-exit
        reclaim (v4 2.4c — worker-agnostic, replaces the local task check).
        Defaults to a live row (no reclaim); exit-race tests set it None."""
        with patch(
            "src.server.database.turn_lifecycle.get_active_run",
            new_callable=AsyncMock,
            return_value=_LIVE_ROW,
        ) as slot:
            yield slot

    @pytest.mark.asyncio
    async def test_fresh_returns_true(self):
        from src.server.handlers.chat.admission import wait_or_steer

        manager = AsyncMock()
        manager.wait_for_admission = AsyncMock(return_value=("fresh", None))

        ready, event = await wait_or_steer(manager, "t-1", "hello", "u-1")
        assert ready is True
        assert event is None

    @pytest.mark.asyncio
    async def test_steer_only_fresh_raises_409_not_running(self):
        """A steer_only probe must never be admitted as a fresh turn — the
        probe's SSE reader ignores turn events, so a turn admitted on that
        connection streams its output (interrupts included) into a void.
        409 'not_running' routes the gateway to its resubmit path instead."""
        from fastapi import HTTPException

        from src.server.handlers.chat.admission import wait_or_steer

        manager = AsyncMock()
        manager.wait_for_admission = AsyncMock(return_value=("fresh", None))

        with pytest.raises(HTTPException) as exc_info:
            await wait_or_steer(
                manager, "t-1", "hello", "u-1", steer_only=True
            )

        assert exc_info.value.status_code == 409
        detail = exc_info.value.detail
        assert isinstance(detail, dict)
        assert detail["code"] == "not_running"
        assert "t-1" not in detail["message"]

    @pytest.mark.asyncio
    async def test_steer_only_still_steers_running(self):
        """steer_only forbids only the fresh-admission fallback; a
        genuinely-running turn steers exactly as before."""
        from src.server.handlers.chat.admission import wait_or_steer

        manager = AsyncMock()
        manager.wait_for_admission = AsyncMock(return_value=("running", _LIVE_ROW))
        # Live slot (fixture default) → no accept-after-exit reclaim.

        with patch(
            "src.server.handlers.chat.steering.steer_thread",
            new_callable=AsyncMock,
            return_value={"position": 2, "payload": "QUEUED_JSON"},
        ):
            ready, event = await wait_or_steer(
                manager, "t-1", "hello", "u-1", steer_only=True
            )

        assert ready is False
        assert "steering_accepted" in event

    @pytest.mark.asyncio
    async def test_steer_accept_racing_exit_reclaims_and_admits_fresh(self, ledger_slot):
        """The admission snapshot said "running" but the workflow exited before
        the Redis push landed: the message must be reclaimed and the POST
        routed as a fresh turn — never a false steering_accepted for a
        message nothing will consume."""
        from src.server.handlers.chat.admission import wait_or_steer

        manager = AsyncMock()
        manager.wait_for_admission = AsyncMock(return_value=("running", _LIVE_ROW))
        ledger_slot.return_value = None  # slot emptied between snapshot and push

        with (
            patch(
                "src.server.handlers.chat.steering.steer_thread",
                new_callable=AsyncMock,
                return_value={"position": 1, "payload": "QUEUED_JSON"},
            ),
            patch(
                "src.server.handlers.chat.steering.unsteer_thread",
                new_callable=AsyncMock,
                return_value=True,
            ) as mock_unsteer,
        ):
            ready, event = await wait_or_steer(manager, "t-1", "hello", "u-1")

        assert ready is True
        assert event is None
        mock_unsteer.assert_awaited_once_with("t-1", "QUEUED_JSON")

    @pytest.mark.asyncio
    async def test_steer_accept_racing_exit_steer_only_raises_not_running(
        self, ledger_slot
    ):
        """Same race under steer_only: a reclaimed accept surfaces as
        not_running so the gateway resubmits — not as a fresh admission."""
        from fastapi import HTTPException

        from src.server.handlers.chat.admission import wait_or_steer

        manager = AsyncMock()
        manager.wait_for_admission = AsyncMock(return_value=("running", _LIVE_ROW))
        ledger_slot.return_value = None

        with (
            patch(
                "src.server.handlers.chat.steering.steer_thread",
                new_callable=AsyncMock,
                return_value={"position": 1, "payload": "QUEUED_JSON"},
            ),
            patch(
                "src.server.handlers.chat.steering.unsteer_thread",
                new_callable=AsyncMock,
                return_value=True,
            ),
        ):
            with pytest.raises(HTTPException) as exc_info:
                await wait_or_steer(
                    manager, "t-1", "hello", "u-1", steer_only=True
                )

        assert exc_info.value.status_code == 409
        assert exc_info.value.detail["code"] == "not_running"

    @pytest.mark.asyncio
    async def test_steer_accept_drained_by_exit_still_reports_accepted(
        self, ledger_slot
    ):
        """If the reclaim misses, the exiting workflow's final drain got the
        message first and steering_returned already carried it back on the
        turn stream — the POST keeps the queue contract and reports
        accepted."""
        from src.server.handlers.chat.admission import wait_or_steer

        manager = AsyncMock()
        manager.wait_for_admission = AsyncMock(return_value=("running", _LIVE_ROW))
        ledger_slot.return_value = None

        with (
            patch(
                "src.server.handlers.chat.steering.steer_thread",
                new_callable=AsyncMock,
                return_value={"position": 1, "payload": "QUEUED_JSON"},
            ),
            patch(
                "src.server.handlers.chat.steering.unsteer_thread",
                new_callable=AsyncMock,
                return_value=False,
            ),
        ):
            ready, event = await wait_or_steer(manager, "t-1", "hello", "u-1")

        assert ready is False
        assert "steering_accepted" in event

    @pytest.mark.asyncio
    async def test_running_steers_immediately_with_no_wait(self):
        """CRITICAL regression: a genuinely-running turn is steered immediately
        — admission returns "running" and steer_thread is called without any
        wait on the running task."""
        from src.server.handlers.chat.admission import wait_or_steer

        manager = AsyncMock()
        manager.wait_for_admission = AsyncMock(return_value=("running", _LIVE_ROW))
        # Live slot (fixture default) → no accept-after-exit reclaim.

        with patch(
            "src.server.handlers.chat.steering.steer_thread",
            new_callable=AsyncMock,
            return_value={"position": 1, "payload": "QUEUED_JSON"},
        ) as mock_steer:
            ready, event = await wait_or_steer(
                manager, "t-1", "hello", "u-1"
            )

        assert ready is False
        assert event is not None
        assert "steering_accepted" in event
        assert '"position": 1' in event
        # Stamped with the live run so the middleware's own-run filter
        # delivers it (v4 2.4c).
        mock_steer.assert_awaited_once_with("t-1", "hello", "u-1", run_id="run-live")
        # No wait helper exists anymore — admission decided immediately.
        manager.wait_for_admission.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_stopping_raises_409_without_steering(self):
        """An explicitly-cancelled turn still winding down → 409 'stopping',
        never steered (a second checkpoint writer would corrupt state)."""
        from fastapi import HTTPException

        from src.server.handlers.chat.admission import wait_or_steer

        manager = AsyncMock()
        manager.wait_for_admission = AsyncMock(return_value=("stopping", _STOPPING_ROW))

        with (
            patch(
                "src.server.handlers.chat.steering.steer_thread",
                new_callable=AsyncMock,
            ) as mock_steer,
            pytest.raises(HTTPException) as exc_info,
        ):
            await wait_or_steer(manager, "t-1", "hello", "u-1")

        assert exc_info.value.status_code == 409
        # Structured detail so handle_workflow_error can tag the SSE error code.
        detail = exc_info.value.detail
        assert isinstance(detail, dict)
        assert detail["code"] == "stopping"
        assert "t-1" not in detail["message"]
        mock_steer.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_raises_409_when_steering_fails(self):
        from fastapi import HTTPException

        from src.server.handlers.chat.admission import wait_or_steer

        manager = AsyncMock()
        manager.wait_for_admission = AsyncMock(return_value=("running", _LIVE_ROW))

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
        detail = exc_info.value.detail
        assert isinstance(detail, dict)
        assert detail["code"] == "running"
        assert "t-1" not in detail["message"]

    @pytest.mark.asyncio
    async def test_compacting_raises_409_without_steering(self):
        """A thread mid-compaction whose wait timed out → 409 'compacting',
        never steered (steering mid-summarize corrupts the context rewrite)."""
        from fastapi import HTTPException

        from src.server.handlers.chat.admission import wait_or_steer

        manager = AsyncMock()
        manager.wait_for_admission = AsyncMock(return_value=("compacting", None))

        with (
            patch(
                "src.server.handlers.chat.steering.steer_thread",
                new_callable=AsyncMock,
            ) as mock_steer,
            pytest.raises(HTTPException) as exc_info,
        ):
            await wait_or_steer(manager, "t-1", "hello", "u-1")

        assert exc_info.value.status_code == 409
        # Structured detail so the client can recognize the transient state and
        # re-queue; no thread_id leaked into the user-facing message.
        detail = exc_info.value.detail
        assert isinstance(detail, dict)
        assert detail["code"] == "compacting"
        assert "t-1" not in detail["message"]
        mock_steer.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_cannot_steer_running_is_a_conflict(self):
        """Dispatched flows (can_steer=False) never steer: a running peer is a
        409, not a steering_accepted — a second astream on a thread-keyed
        checkpointer would collide."""
        from fastapi import HTTPException

        from src.server.handlers.chat.admission import wait_or_steer

        manager = AsyncMock()
        manager.wait_for_admission = AsyncMock(return_value=("running", _LIVE_ROW))

        with (
            patch(
                "src.server.handlers.chat.steering.steer_thread",
                new_callable=AsyncMock,
            ) as mock_steer,
            pytest.raises(HTTPException) as exc_info,
        ):
            await wait_or_steer(manager, "t-1", "hi", "u-1", can_steer=False)

        assert exc_info.value.status_code == 409
        assert exc_info.value.detail["code"] == "running"
        mock_steer.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_exclude_run_id_reaches_admission_scan(self):
        """A dispatched flow passes its own pre-registered run_id so the
        admission scan ignores its placeholder and only sees OTHER runs."""
        from src.server.handlers.chat.admission import wait_or_steer

        manager = AsyncMock()
        manager.wait_for_admission = AsyncMock(return_value=("fresh", None))

        ready, event = await wait_or_steer(
            manager, "t-1", "hi", "u-1", exclude_run_id="r-9"
        )

        assert ready is True
        assert event is None
        manager.wait_for_admission.assert_awaited_once_with(
            "t-1", exclude_run_id="r-9"
        )


# ---------------------------------------------------------------------------
# admission_conflict_detail (single 409 wording source)
# ---------------------------------------------------------------------------


class TestAdmissionConflictDetail:
    """The single wording source for every in-generator admission 409 — the
    ``stopping``/``compacting``/``running`` conflicts and the ``steer_only``
    probe's ``not_running`` refusal. Same mapping for PTC and Flash, foreground
    and dispatched."""

    def test_compacting_state_names_compaction(self):
        from src.server.handlers.chat.admission import admission_conflict_detail

        detail = admission_conflict_detail("compacting")
        # Structured detail (code + message) so handle_workflow_error can tag the
        # SSE error with a code and the client can recognize a transient state.
        assert isinstance(detail, dict)
        assert detail["code"] == "compacting"
        assert "compacting" in detail["message"].lower()
        assert "retry" in detail["message"].lower() or "resend" in detail["message"].lower()

    def test_stopping_state_names_stopping(self):
        from src.server.handlers.chat.admission import admission_conflict_detail

        detail = admission_conflict_detail("stopping")
        assert isinstance(detail, dict)
        assert detail["code"] == "stopping"
        assert "stopping" in detail["message"].lower()

    def test_not_running_names_the_steer_refusal(self):
        from src.server.handlers.chat.admission import admission_conflict_detail

        # The steer_only probe against an idle thread: not a wait_for_admission
        # state, but routed through the same mapper so the wording lives here too.
        detail = admission_conflict_detail("not_running")
        assert isinstance(detail, dict)
        assert detail["code"] == "not_running"
        assert "running" in detail["message"].lower()

    def test_running_state_is_the_generic_fallback(self):
        from src.server.handlers.chat.admission import admission_conflict_detail

        # Any other non-fresh state (e.g. "running") falls through to the
        # "still running" code.
        detail = admission_conflict_detail("running")
        assert isinstance(detail, dict)
        assert detail["code"] == "running"
        assert "still running" in detail["message"].lower()
        # The actionable hint (reconnect / cancel) is part of the contract —
        # the web UI renders this message verbatim, so keep it pinned.
        assert "reconnect" in detail["message"].lower()
        assert "cancel" in detail["message"].lower()

    def test_unknown_state_falls_back_to_generic(self):
        from src.server.handlers.chat.admission import admission_conflict_detail

        detail = admission_conflict_detail("wedged")
        assert detail["code"] == "running"
        assert "still running" in detail["message"].lower()


# ---------------------------------------------------------------------------
# handle_workflow_error — HTTPException (admission conflict) handling
# ---------------------------------------------------------------------------


class TestHandleWorkflowErrorHTTPException:
    """An HTTPException reaching the workflow error handler is a deliberate
    protocol response (e.g. a 409 admission conflict raised in-generator by
    wait_or_steer / the dispatched gate), not an execution failure. It must
    surface to the client as an SSE error but never finalize the run as an
    error — the admission path runs with run_handle=None here, and failing
    the thread's open run would clobber a concurrently-running peer turn.

    v4: the durable terminal write moved off ``ConversationPersistenceService``
    onto ``TurnCoordinator.finalize_turn`` (driven by the STARTed ``run_handle``),
    so these tests observe the coordinator rather than a persistence service.
    """

    TC = "src.server.services.turn_lifecycle.TurnCoordinator"

    @staticmethod
    def _handler():
        handler = MagicMock()
        handler.get_tool_usage = MagicMock(return_value=None)
        handler.get_sse_events = MagicMock(return_value=None)
        handler._format_sse_event = MagicMock(
            side_effect=lambda ev, data: f"event: {ev}\ndata: {data}\n\n"
        )
        return handler

    @staticmethod
    def _request():
        request = MagicMock()
        request.workspace_id = None
        request.locale = None
        request.timezone = None
        return request

    @pytest.mark.asyncio
    async def test_http_exception_surfaces_error_but_skips_finalize(self):
        from fastapi import HTTPException

        from src.server.handlers.chat.error_handling import handle_workflow_error

        exc = HTTPException(
            status_code=409,
            detail={"code": "compacting", "message": "compacting; retry shortly"},
        )

        coordinator = AsyncMock()

        with (
            patch(
                "src.server.handlers.chat.error_handling.release_burst_slot",
                new_callable=AsyncMock,
            ),
            patch(self.TC) as mock_coord_cls,
        ):
            mock_coord_cls.get_instance.return_value = coordinator
            events = [
                ev
                async for ev in handle_workflow_error(
                    exc,
                    thread_id="t-1",
                    user_id="u-1",
                    workspace_id="w-1",
                    handler=self._handler(),
                    token_callback=None,
                    run_handle=None,
                    start_time=0.0,
                    request=self._request(),
                    is_byok=False,
                    msg_type="ptc",
                    log_prefix="PTC_TEST",
                )
            ]

        # The client still sees a clean error event...
        assert any("event: error" in ev for ev in events)
        assert any("compacting" in ev for ev in events)
        # ...but the conflict is never finalized as a turn failure — the
        # (possibly peer-owned) open run is untouched.
        coordinator.finalize_turn.assert_not_awaited()
        coordinator.fail_open_run.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_not_running_conflict_skips_finalize(self):
        """not_running (steer_only probe on an idle thread) is an admission
        outcome: it must reach the client as an SSE error but never be
        finalized as an error on a (possibly peer-owned) turn. Pins the
        ADMISSION_CONFLICT_CODES membership added for steer_only."""
        from fastapi import HTTPException

        from src.server.handlers.chat.error_handling import handle_workflow_error

        exc = HTTPException(
            status_code=409,
            detail={"code": "not_running", "message": "no workflow to steer"},
        )

        coordinator = AsyncMock()

        with (
            patch(
                "src.server.handlers.chat.error_handling.release_burst_slot",
                new_callable=AsyncMock,
            ),
            patch(self.TC) as mock_coord_cls,
        ):
            mock_coord_cls.get_instance.return_value = coordinator
            events = [
                ev
                async for ev in handle_workflow_error(
                    exc,
                    thread_id="t-1",
                    user_id="u-1",
                    workspace_id="w-1",
                    handler=self._handler(),
                    token_callback=None,
                    run_handle=None,
                    start_time=0.0,
                    request=self._request(),
                    is_byok=False,
                    msg_type="ptc",
                    log_prefix="PTC_TEST",
                )
            ]

        assert any("event: error" in ev for ev in events)
        assert any("not_running" in ev for ev in events)
        coordinator.finalize_turn.assert_not_awaited()
        coordinator.fail_open_run.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_non_409_http_exception_is_finalized_as_error(self):
        """The skip path is scoped to the 409 admission/cancellation contract.
        A non-409 HTTPException (e.g. a 503 raised because the agent isn't
        initialized) is a genuine failure: it must flow through the normal
        failure path — finalize the run as error and be labeled as a real
        workflow error, NOT mislabeled as an admission conflict."""
        from fastapi import HTTPException

        from src.server.handlers.chat.error_handling import handle_workflow_error

        exc = HTTPException(status_code=503, detail="backend not ready")

        coordinator = AsyncMock()
        run_handle = MagicMock(
            finalized=False, run_id="r-1", workspace_id="w-1", user_id="u-1"
        )

        with (
            patch(
                "src.server.handlers.chat.error_handling.release_burst_slot",
                new_callable=AsyncMock,
            ),
            patch(self.TC) as mock_coord_cls,
        ):
            mock_coord_cls.get_instance.return_value = coordinator
            events = [
                ev
                async for ev in handle_workflow_error(
                    exc,
                    thread_id="t-1",
                    user_id="u-1",
                    workspace_id="w-1",
                    handler=self._handler(),
                    token_callback=None,
                    run_handle=run_handle,
                    start_time=0.0,
                    request=self._request(),
                    is_byok=False,
                    msg_type="ptc",
                    log_prefix="PTC_TEST",
                )
            ]

        # A genuine 503 is finalized as error (real failure path)...
        coordinator.finalize_turn.assert_awaited()
        # ...and is never mislabeled as a transient admission conflict.
        assert not any("admission_conflict" in ev for ev in events)

    @pytest.mark.asyncio
    async def test_409_without_admission_code_is_finalized_as_error(self):
        """The skip path is keyed on the admission-conflict CODE, not the bare
        409 status. A 409 raised elsewhere in the generator (no structured
        admission code) is a genuine failure: it must finalize as error, not
        silently skip it — otherwise a future non-admission 409 would clobber a
        peer turn the same way the original bug #2 did."""
        from fastapi import HTTPException

        from src.server.handlers.chat.error_handling import handle_workflow_error

        # A plain-string 409 (no {"code": ...}) — e.g. a future conflict raised
        # by middleware — must NOT be treated as an admission conflict.
        exc = HTTPException(status_code=409, detail="some unrelated conflict")

        coordinator = AsyncMock()
        run_handle = MagicMock(
            finalized=False, run_id="r-1", workspace_id="w-1", user_id="u-1"
        )

        with (
            patch(
                "src.server.handlers.chat.error_handling.release_burst_slot",
                new_callable=AsyncMock,
            ),
            patch(self.TC) as mock_coord_cls,
        ):
            mock_coord_cls.get_instance.return_value = coordinator
            events = [
                ev
                async for ev in handle_workflow_error(
                    exc,
                    thread_id="t-1",
                    user_id="u-1",
                    workspace_id="w-1",
                    handler=self._handler(),
                    token_callback=None,
                    run_handle=run_handle,
                    start_time=0.0,
                    request=self._request(),
                    is_byok=False,
                    msg_type="ptc",
                    log_prefix="PTC_TEST",
                )
            ]

        coordinator.finalize_turn.assert_awaited()
        assert not any("admission_conflict" in ev for ev in events)


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
# prepare_skill_contexts
# ---------------------------------------------------------------------------


class TestPrepareSkillContexts:
    def test_no_skills_returns_empty(self):
        from src.server.handlers.chat._common import prepare_skill_contexts

        request = MagicMock()
        request.additional_context = None
        request.hitl_response = None
        messages = [{"role": "user", "content": "hello"}]

        with patch(f"{COMMON}.parse_skill_contexts", return_value=[]):
            result = prepare_skill_contexts(messages, request, mode="flash")

        assert result == []

    def test_skill_from_additional_context(self):
        from src.server.handlers.chat._common import prepare_skill_contexts

        request = MagicMock()
        request.additional_context = [MagicMock(type="skills")]
        request.hitl_response = None
        messages = [{"role": "user", "content": "hello"}]

        ctx = SkillContext(type="skills", name="research", instruction="find news")
        with patch(f"{COMMON}.parse_skill_contexts", return_value=[ctx]):
            result = prepare_skill_contexts(messages, request, mode="flash")

        # Returns plain dicts to thread through config; no body injected here.
        assert result == [{"name": "research", "instruction": "find news"}]
        assert messages[0]["content"] == "hello"

    def test_slash_command_detection_fallback_strips_prefix(self):
        from src.server.handlers.chat._common import prepare_skill_contexts

        request = MagicMock()
        request.additional_context = None
        request.hitl_response = None
        messages = [{"role": "user", "content": "/research market analysis"}]

        ctx = SkillContext(type="skills", name="research")
        with (
            patch(f"{COMMON}.parse_skill_contexts", return_value=[]),
            patch(
                f"{COMMON}.detect_slash_commands",
                return_value=("market analysis", [ctx]),
            ),
        ):
            result = prepare_skill_contexts(messages, request, mode="flash")

        assert result == [{"name": "research", "instruction": None}]
        # The /command prefix is stripped in place; no skill body appended.
        assert messages[0]["content"] == "market analysis"

    def test_hitl_response_skips_slash_detection(self):
        from src.server.handlers.chat._common import prepare_skill_contexts

        request = MagicMock()
        request.additional_context = None
        request.hitl_response = {"int-1": {}}
        messages = [{"role": "user", "content": "/research something"}]

        with (
            patch(f"{COMMON}.parse_skill_contexts", return_value=[]),
            patch(f"{COMMON}.detect_slash_commands") as mock_detect,
        ):
            result = prepare_skill_contexts(messages, request, mode="flash")

        mock_detect.assert_not_called()
        assert result == []

    def test_slash_detection_on_multimodal_list_content(self):
        """A supported attachment rewrites content into a block list; the slash
        command must still activate, be stripped in its own text block, and leave
        the attachment blocks intact."""
        from src.server.handlers.chat._common import prepare_skill_contexts

        request = MagicMock()
        request.additional_context = None
        request.hitl_response = None
        image_block = {"type": "image_url", "image_url": {"url": "data:image/png;x"}}
        text_block = {"type": "text", "text": "/research market analysis"}
        messages = [{"role": "user", "content": [image_block, text_block]}]

        ctx = SkillContext(type="skills", name="research")
        with (
            patch(f"{COMMON}.parse_skill_contexts", return_value=[]),
            patch(
                f"{COMMON}.detect_slash_commands",
                return_value=("market analysis", [ctx]),
            ),
        ):
            result = prepare_skill_contexts(messages, request, mode="flash")

        assert result == [{"name": "research", "instruction": None}]
        # Prefix stripped in the text block; the image block survives untouched.
        assert messages[0]["content"][0] is image_block
        assert messages[0]["content"][1]["text"] == "market analysis"

    def test_list_content_without_slash_block_skips_detection(self):
        """List content whose text blocks don't lead with `/` activates nothing."""
        from src.server.handlers.chat._common import prepare_skill_contexts

        request = MagicMock()
        request.additional_context = None
        request.hitl_response = None
        messages = [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "[Attached image: chart.png]"},
                    {"type": "image_url", "image_url": {"url": "data:image/png;x"}},
                    {"type": "text", "text": "what do you see"},
                ],
            }
        ]

        with (
            patch(f"{COMMON}.parse_skill_contexts", return_value=[]),
            patch(f"{COMMON}.detect_slash_commands") as mock_detect,
        ):
            result = prepare_skill_contexts(messages, request, mode="flash")

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


class TestInjectInlineReminders:
    def _inject(self, messages, reminders):
        from src.server.handlers.chat._common import inject_inline_reminders

        return inject_inline_reminders(messages, reminders)

    def test_none_messages_is_noop(self):
        # HITL-resume / checkpoint-replay path: no target list, must not raise.
        self._inject(None, ["\nreminder"])

    def test_empty_messages_is_noop(self):
        msgs = []
        self._inject(msgs, ["\nreminder"])
        assert msgs == []

    def test_appends_present_reminders_in_order(self):
        msgs = [{"role": "user", "content": "hi"}]
        self._inject(msgs, ["\nA", "\nB"])
        assert msgs[-1]["content"] == "hi\nA\nB"

    def test_skips_absent_reminders(self):
        msgs = [{"role": "user", "content": "hi"}]
        self._inject(msgs, [None, "", "\nX"])
        assert msgs[-1]["content"] == "hi\nX"

    def test_does_not_append_to_non_user_tail(self):
        msgs = [{"role": "assistant", "content": "x"}]
        self._inject(msgs, ["\nA"])
        assert msgs[-1]["content"] == "x"
