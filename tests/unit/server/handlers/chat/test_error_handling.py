"""
Tests for src/server/handlers/chat/error_handling.py.

Covers:
- classify_error: recoverable vs non-recoverable error classification
- _classify_non_recoverable_error_type: workspace-lifecycle error labels
- handle_workflow_error: an admission-conflict HTTPException surfaces as an SSE
  error but never finalizes the run
"""

from unittest.mock import AsyncMock, MagicMock, patch

import psycopg
import pytest

from ptc_agent.core.sandbox.runtime import SandboxGoneError, SandboxTransientError


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
    onto ``RunCoordinator.finalize_run`` (driven by the STARTed ``run_handle``),
    so these tests observe the coordinator rather than a persistence service.
    """

    TC = "src.server.services.runs.coordinator.RunCoordinator"

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

    @staticmethod
    def _scope(run_handle=None):
        from src.server.services.runs.admission import RunScope

        scope = RunScope(user_id="u-1", burst_slot_id=None)
        if run_handle is not None:
            scope.attach_run(run_handle)
        return scope

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
                "src.server.dependencies.usage_limits.release_burst_slot",
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
                    scope=self._scope(),
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
        coordinator.finalize_run.assert_not_awaited()
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
                "src.server.dependencies.usage_limits.release_burst_slot",
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
                    scope=self._scope(),
                    start_time=0.0,
                    request=self._request(),
                    is_byok=False,
                    msg_type="ptc",
                    log_prefix="PTC_TEST",
                )
            ]

        assert any("event: error" in ev for ev in events)
        assert any("not_running" in ev for ev in events)
        coordinator.finalize_run.assert_not_awaited()
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
                "src.server.dependencies.usage_limits.release_burst_slot",
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
                    scope=self._scope(run_handle),
                    start_time=0.0,
                    request=self._request(),
                    is_byok=False,
                    msg_type="ptc",
                    log_prefix="PTC_TEST",
                )
            ]

        # A genuine 503 is finalized as error (real failure path)...
        coordinator.finalize_run.assert_awaited()
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
                "src.server.dependencies.usage_limits.release_burst_slot",
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
                    scope=self._scope(run_handle),
                    start_time=0.0,
                    request=self._request(),
                    is_byok=False,
                    msg_type="ptc",
                    log_prefix="PTC_TEST",
                )
            ]

        coordinator.finalize_run.assert_awaited()
        assert not any("admission_conflict" in ev for ev in events)
