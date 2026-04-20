"""
Tests for trigger_compaction() — the manual /compact endpoint handler.

Regression coverage for the bug where the manual /compact path bypassed
resolve_llm_config and therefore always used the base YAML compaction model
instead of the user's compaction_model preference.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.config.agent import AgentConfig, LLMConfig
from ptc_agent.config.core import (
    DaytonaConfig,
    FilesystemConfig,
    LoggingConfig,
    MCPConfig,
    SandboxConfig,
    SecurityConfig,
)


HANDLER = "src.server.handlers.workflow_handler"
LLM_HANDLER = "src.server.handlers.chat.llm_config"


def _make_agent_config(compaction_model: str | None = "system-compaction") -> AgentConfig:
    return AgentConfig(
        llm=LLMConfig(
            name="system-default-model",
            flash="system-flash-model",
            compaction=compaction_model,
        ),
        security=SecurityConfig(),
        logging=LoggingConfig(),
        sandbox=SandboxConfig(daytona=DaytonaConfig(api_key="test-key")),
        mcp=MCPConfig(),
        filesystem=FilesystemConfig(),
    )


def _mock_model_config(system_models=None):
    if system_models is None:
        system_models = {
            "system-default-model",
            "system-flash-model",
            "system-compaction",
            "user-compaction-model",
        }
    mc = MagicMock()
    mc.get_model_config.side_effect = (
        lambda name: {"provider": "openai"} if name in system_models else None
    )
    mc.get_provider_info.return_value = {}
    mc.get_parent_provider.return_value = "openai"
    return mc


@pytest.fixture
def base_config():
    return _make_agent_config()


def _stub_resolve_graph_and_state():
    """Return a coroutine factory producing the 5-tuple _resolve_graph_and_state yields."""

    graph = MagicMock()
    graph.aupdate_state = AsyncMock(return_value=None)
    state = MagicMock()
    state.values = {"_summarization_event": None}
    messages = [MagicMock(id="m1"), MagicMock(id="m2")]
    backend = None
    lg_config = {"configurable": {"thread_id": "thread-1"}}

    async def _stub(thread_id, verb, config=None):
        _stub.captured_config = config
        return graph, lg_config, state, messages, backend

    _stub.captured_config = None
    return _stub


async def _noop_persist(*args, **kwargs):
    return None


@pytest.mark.asyncio
async def test_manual_compact_uses_user_compaction_model(base_config):
    """When user_id is passed and pref sets compaction_model, that model is used."""
    from src.server.handlers.workflow_handler import trigger_compaction

    stub_resolve = _stub_resolve_graph_and_state()

    compact_mock = AsyncMock(
        return_value={
            "event": {"summary_text": "ok"},
            "summary_text": "ok",
            "original_count": 2,
            "preserved_count": 1,
            "offloaded_arg_ids": set(),
            "offloaded_read_ids": set(),
        }
    )

    mock_mc = _mock_model_config()

    with (
        patch("src.server.app.setup.agent_config", base_config),
        patch(f"{HANDLER}._resolve_graph_and_state", new=stub_resolve),
        patch(f"{HANDLER}._persist_context_window_event", new=_noop_persist),
        patch(
            "ptc_agent.agent.middleware.compaction.compact_messages",
            new=compact_mock,
        ),
        patch(
            "src.server.database.api_keys.is_byok_active",
            new_callable=AsyncMock,
            return_value=False,
        ),
        patch(
            f"{LLM_HANDLER}.get_model_preference",
            new_callable=AsyncMock,
            return_value={"compaction_model": "user-compaction-model"},
        ),
        patch(
            f"{LLM_HANDLER}.resolve_oauth_llm_client",
            new_callable=AsyncMock,
            return_value=None,
        ),
        patch("src.llms.llm.LLM.get_model_config", return_value=mock_mc),
    ):
        await trigger_compaction("thread-1", keep_messages=5, user_id="user-1")

    assert compact_mock.await_count == 1
    kwargs = compact_mock.await_args.kwargs
    assert kwargs["model_name"] == "user-compaction-model", (
        "Manual /compact must honor the user's compaction_model preference, "
        f"got {kwargs['model_name']!r}"
    )

    # _resolve_graph_and_state should receive the resolved (user-overridden) config,
    # not the untouched base config.
    resolved = stub_resolve.captured_config
    assert resolved is not None
    assert resolved.llm.compaction == "user-compaction-model"


@pytest.mark.asyncio
async def test_manual_compact_without_user_id_uses_base_config(base_config):
    """No user_id → no resolve_llm_config call; base YAML compaction model is used."""
    from src.server.handlers.workflow_handler import trigger_compaction

    stub_resolve = _stub_resolve_graph_and_state()

    compact_mock = AsyncMock(
        return_value={
            "event": {"summary_text": "ok"},
            "summary_text": "ok",
            "original_count": 2,
            "preserved_count": 1,
            "offloaded_arg_ids": set(),
            "offloaded_read_ids": set(),
        }
    )

    # Guard: if resolve_llm_config is called we want the test to fail loudly.
    resolve_spy = AsyncMock(side_effect=AssertionError("resolve_llm_config called without user_id"))

    with (
        patch("src.server.app.setup.agent_config", base_config),
        patch(f"{HANDLER}._resolve_graph_and_state", new=stub_resolve),
        patch(f"{HANDLER}._persist_context_window_event", new=_noop_persist),
        patch(
            "ptc_agent.agent.middleware.compaction.compact_messages",
            new=compact_mock,
        ),
        patch(f"{LLM_HANDLER}.resolve_llm_config", new=resolve_spy),
    ):
        await trigger_compaction("thread-1", keep_messages=5)

    assert compact_mock.await_count == 1
    kwargs = compact_mock.await_args.kwargs
    assert kwargs["model_name"] == "system-compaction"
    assert resolve_spy.await_count == 0


@pytest.mark.asyncio
async def test_resolve_failure_falls_back_to_base_config(base_config):
    """If resolve_llm_config raises, manual /compact logs and falls back cleanly."""
    from src.server.handlers.workflow_handler import trigger_compaction

    stub_resolve = _stub_resolve_graph_and_state()

    compact_mock = AsyncMock(
        return_value={
            "event": {"summary_text": "ok"},
            "summary_text": "ok",
            "original_count": 2,
            "preserved_count": 1,
            "offloaded_arg_ids": set(),
            "offloaded_read_ids": set(),
        }
    )

    failing_resolve = AsyncMock(side_effect=RuntimeError("db down"))

    with (
        patch("src.server.app.setup.agent_config", base_config),
        patch(f"{HANDLER}._resolve_graph_and_state", new=stub_resolve),
        patch(f"{HANDLER}._persist_context_window_event", new=_noop_persist),
        patch(
            "ptc_agent.agent.middleware.compaction.compact_messages",
            new=compact_mock,
        ),
        patch(
            "src.server.database.api_keys.is_byok_active",
            new_callable=AsyncMock,
            return_value=False,
        ),
        patch(f"{LLM_HANDLER}.resolve_llm_config", new=failing_resolve),
    ):
        await trigger_compaction("thread-1", keep_messages=5, user_id="user-1")

    # Fell back to base YAML compaction model; did not raise.
    kwargs = compact_mock.await_args.kwargs
    assert kwargs["model_name"] == "system-compaction"


@pytest.mark.asyncio
async def test_manual_compact_forwards_subsidiary_oauth_client(base_config):
    """When the user has an OAuth-resolved subsidiary compaction client (the
    same client the auto path uses), manual /compact must hand it to
    compact_messages rather than re-resolving via the system LLM factory.
    Otherwise users on Codex/Claude OAuth or BYOK get billed wrong or 4xx."""
    from src.server.handlers.workflow_handler import trigger_compaction

    stub_resolve = _stub_resolve_graph_and_state()

    compact_mock = AsyncMock(
        return_value={
            "event": {"summary_text": "ok"},
            "summary_text": "ok",
            "original_count": 2,
            "preserved_count": 1,
            "offloaded_arg_ids": set(),
            "offloaded_read_ids": set(),
        }
    )

    oauth_client = MagicMock(name="oauth-codex-client")

    async def _resolve_stub(base_cfg, user_id, request_model, is_byok, mode="ptc"):
        cfg = base_cfg.model_copy(deep=True)
        cfg.llm.compaction = "user-compaction-model"
        cfg.subsidiary_llm_clients["compaction"] = oauth_client
        return cfg

    with (
        patch("src.server.app.setup.agent_config", base_config),
        patch(f"{HANDLER}._resolve_graph_and_state", new=stub_resolve),
        patch(f"{HANDLER}._persist_context_window_event", new=_noop_persist),
        patch(
            "ptc_agent.agent.middleware.compaction.compact_messages",
            new=compact_mock,
        ),
        patch(
            "src.server.database.api_keys.is_byok_active",
            new_callable=AsyncMock,
            return_value=False,
        ),
        patch(f"{LLM_HANDLER}.resolve_llm_config", new=_resolve_stub),
    ):
        await trigger_compaction("thread-1", keep_messages=5, user_id="user-1")

    kwargs = compact_mock.await_args.kwargs
    assert kwargs["model_name"] == "user-compaction-model"
    # Forwarded as a deep copy so _maybe_disable_streaming in compact_messages
    # can't mutate streaming=False on the shared subsidiary client.
    oauth_client.model_copy.assert_called_once_with()
    assert kwargs["llm_client"] is oauth_client.model_copy.return_value, (
        "Manual /compact must forward a copy of the OAuth/BYOK subsidiary "
        "compaction client so compact_messages doesn't rebuild a bare "
        "system-auth client."
    )


@pytest.mark.asyncio
async def test_manual_compact_falls_back_to_main_llm_client(base_config):
    """When no compaction-specific subsidiary client is present but the main
    agent has a BYOK/OAuth llm_client, forward that — mirrors the middleware's
    priority order in PTCAgent.create_agent."""
    from src.server.handlers.workflow_handler import trigger_compaction

    stub_resolve = _stub_resolve_graph_and_state()

    compact_mock = AsyncMock(
        return_value={
            "event": {"summary_text": "ok"},
            "summary_text": "ok",
            "original_count": 2,
            "preserved_count": 1,
            "offloaded_arg_ids": set(),
            "offloaded_read_ids": set(),
        }
    )

    main_client = MagicMock(name="main-byok-client")

    async def _resolve_stub(base_cfg, user_id, request_model, is_byok, mode="ptc"):
        cfg = base_cfg.model_copy(deep=True)
        cfg.llm_client = main_client
        # No subsidiary compaction client — user picked default compaction model.
        cfg.subsidiary_llm_clients.pop("compaction", None)
        return cfg

    with (
        patch("src.server.app.setup.agent_config", base_config),
        patch(f"{HANDLER}._resolve_graph_and_state", new=stub_resolve),
        patch(f"{HANDLER}._persist_context_window_event", new=_noop_persist),
        patch(
            "ptc_agent.agent.middleware.compaction.compact_messages",
            new=compact_mock,
        ),
        patch(
            "src.server.database.api_keys.is_byok_active",
            new_callable=AsyncMock,
            return_value=True,
        ),
        patch(f"{LLM_HANDLER}.resolve_llm_config", new=_resolve_stub),
    ):
        await trigger_compaction("thread-1", keep_messages=5, user_id="user-1")

    kwargs = compact_mock.await_args.kwargs
    # Forwarded as a deep copy — _maybe_disable_streaming would otherwise
    # permanently set streaming=False on the main agent's shared llm_client.
    main_client.model_copy.assert_called_once_with()
    assert kwargs["llm_client"] is main_client.model_copy.return_value


@pytest.mark.asyncio
async def test_manual_compact_copies_llm_client_before_forwarding(base_config):
    """Regression: the llm_client passed to compact_messages MUST be a copy.

    ``compact_messages`` calls ``_maybe_disable_streaming`` which sets
    ``streaming = False`` in place on the client. If we hand over the shared
    ``agent_cfg.llm_client`` directly, the main agent's model is permanently
    mutated and all subsequent chat workflows lose SSE token streaming.
    Mirrors the ``.model_copy()`` pattern in ``PTCAgent.create_agent``.
    """
    from src.server.handlers.workflow_handler import trigger_compaction

    stub_resolve = _stub_resolve_graph_and_state()

    compact_mock = AsyncMock(
        return_value={
            "event": {"summary_text": "ok"},
            "summary_text": "ok",
            "original_count": 2,
            "preserved_count": 1,
            "offloaded_arg_ids": set(),
            "offloaded_read_ids": set(),
        }
    )

    shared_client = MagicMock(name="shared-main-client")
    base_config.llm_client = shared_client
    base_config.subsidiary_llm_clients.pop("compaction", None)

    with (
        patch("src.server.app.setup.agent_config", base_config),
        patch(f"{HANDLER}._resolve_graph_and_state", new=stub_resolve),
        patch(f"{HANDLER}._persist_context_window_event", new=_noop_persist),
        patch(
            "ptc_agent.agent.middleware.compaction.compact_messages",
            new=compact_mock,
        ),
    ):
        await trigger_compaction("thread-1", keep_messages=5)

    kwargs = compact_mock.await_args.kwargs
    shared_client.model_copy.assert_called_once_with()
    assert kwargs["llm_client"] is not shared_client
    assert kwargs["llm_client"] is shared_client.model_copy.return_value


# ---------------------------------------------------------------------------
# Gate: reject manual /compact + /offload while a workflow is streaming
# ---------------------------------------------------------------------------


class TestActiveWorkflowGate:
    """The gate prevents manual /compact and /offload from racing a live chat
    workflow on the same thread. Two concrete races it closes:

    1. _persist_context_window_event's read-modify-write of
       ``conversation_responses.sse_events`` can clobber events the
       streaming handler is concurrently appending.
    2. trigger_compaction / trigger_offload call ``graph.aupdate_state`` on
       ``_summarization_event`` / ``_offloaded_tool_call_ids`` while the
       running middleware is writing those same fields via ``Command``.

    Both endpoints return 409 with a structured detail
    ``{code: "workflow_active", verb, message}`` so the frontend can surface
    a specific "wait until streaming finishes" banner.
    """

    def _tracker(self, status_value):
        tracker = MagicMock()
        tracker.get_status = AsyncMock(
            return_value={"status": status_value} if status_value else None
        )
        return tracker

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", ["active", "disconnected", "interrupted"])
    async def test_compact_rejected_when_workflow_is_running(
        self, base_config, status
    ):
        """Manual /compact must 409 for every non-terminal workflow state so
        the in-flight workflow can finish its writes cleanly."""
        from fastapi import HTTPException

        from src.server.handlers.workflow_handler import trigger_compaction

        compact_mock = AsyncMock()  # should NEVER be called
        stub_resolve = _stub_resolve_graph_and_state()
        tracker_instance = self._tracker(status)

        with (
            patch("src.server.app.setup.agent_config", base_config),
            patch(f"{HANDLER}._resolve_graph_and_state", new=stub_resolve),
            patch(
                "ptc_agent.agent.middleware.compaction.compact_messages",
                new=compact_mock,
            ),
            patch(
                "src.server.services.workflow_tracker.WorkflowTracker.get_instance",
                return_value=tracker_instance,
            ),
        ):
            with pytest.raises(HTTPException) as exc_info:
                await trigger_compaction("thread-1", keep_messages=5)

        assert exc_info.value.status_code == 409
        detail = exc_info.value.detail
        assert isinstance(detail, dict)
        assert detail["code"] == "workflow_active"
        assert detail["verb"] == "compact"
        assert "streaming" in detail["message"].lower()
        # Crucially: we never reached the graph state read/write or the LLM.
        assert compact_mock.await_count == 0
        assert stub_resolve.captured_config is None

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "status", ["completed", "cancelled", None]
    )
    async def test_compact_allowed_when_workflow_is_terminal_or_absent(
        self, base_config, status
    ):
        """completed/cancelled statuses and no-status-in-Redis must NOT block
        manual /compact — otherwise users could never compact a finished
        conversation."""
        from src.server.handlers.workflow_handler import trigger_compaction

        stub_resolve = _stub_resolve_graph_and_state()
        compact_mock = AsyncMock(
            return_value={
                "event": {"summary_text": "ok"},
                "summary_text": "ok",
                "original_count": 2,
                "preserved_count": 1,
                "offloaded_arg_ids": set(),
                "offloaded_read_ids": set(),
            }
        )

        with (
            patch("src.server.app.setup.agent_config", base_config),
            patch(f"{HANDLER}._resolve_graph_and_state", new=stub_resolve),
            patch(f"{HANDLER}._persist_context_window_event", new=_noop_persist),
            patch(
                "ptc_agent.agent.middleware.compaction.compact_messages",
                new=compact_mock,
            ),
            patch(
                "src.server.services.workflow_tracker.WorkflowTracker.get_instance",
                return_value=self._tracker(status),
            ),
        ):
            await trigger_compaction("thread-1", keep_messages=5)

        assert compact_mock.await_count == 1

    @pytest.mark.asyncio
    async def test_offload_rejected_when_workflow_is_running(self, base_config):
        """Same gate applies to /offload — it also writes checkpoint state."""
        from fastapi import HTTPException

        from src.server.handlers.workflow_handler import trigger_offload

        offload_mock = AsyncMock()  # should NEVER be called
        stub_resolve = _stub_resolve_graph_and_state()

        with (
            patch("src.server.app.setup.agent_config", base_config),
            patch(f"{HANDLER}._resolve_graph_and_state", new=stub_resolve),
            patch(
                "ptc_agent.agent.middleware.compaction.offload_tool_args",
                new=offload_mock,
            ),
            patch(
                "src.server.services.workflow_tracker.WorkflowTracker.get_instance",
                return_value=self._tracker("active"),
            ),
        ):
            with pytest.raises(HTTPException) as exc_info:
                await trigger_offload("thread-1")

        assert exc_info.value.status_code == 409
        detail = exc_info.value.detail
        assert isinstance(detail, dict)
        assert detail["code"] == "workflow_active"
        assert detail["verb"] == "offload"
        assert offload_mock.await_count == 0

    @pytest.mark.asyncio
    async def test_compact_fails_open_when_tracker_disabled(self, base_config):
        """Redis outage → tracker.enabled=False → gate bypasses so admin
        actions stay usable while chat workflows are already degraded."""
        from src.server.handlers.workflow_handler import trigger_compaction

        stub_resolve = _stub_resolve_graph_and_state()
        compact_mock = AsyncMock(
            return_value={
                "event": {"summary_text": "ok"},
                "summary_text": "ok",
                "original_count": 2,
                "preserved_count": 1,
                "offloaded_arg_ids": set(),
                "offloaded_read_ids": set(),
            }
        )

        tracker = MagicMock()
        tracker.enabled = False
        # If the gate accidentally reads status, this would raise and we'd
        # want the test to fail loudly rather than pass for the wrong reason.
        tracker.get_status = AsyncMock(
            side_effect=AssertionError("get_status must not be called when disabled")
        )

        with (
            patch("src.server.app.setup.agent_config", base_config),
            patch(f"{HANDLER}._resolve_graph_and_state", new=stub_resolve),
            patch(f"{HANDLER}._persist_context_window_event", new=_noop_persist),
            patch(
                "ptc_agent.agent.middleware.compaction.compact_messages",
                new=compact_mock,
            ),
            patch(
                "src.server.services.workflow_tracker.WorkflowTracker.get_instance",
                return_value=tracker,
            ),
        ):
            await trigger_compaction("thread-1", keep_messages=5)

        assert compact_mock.await_count == 1

    @pytest.mark.asyncio
    async def test_compact_fails_open_on_transient_tracker_error(self, base_config):
        """Redis blip mid-request → get_status raises → gate fails open with
        a warning instead of surfacing HTTP 500 via the broad except."""
        from src.server.handlers.workflow_handler import trigger_compaction

        stub_resolve = _stub_resolve_graph_and_state()
        compact_mock = AsyncMock(
            return_value={
                "event": {"summary_text": "ok"},
                "summary_text": "ok",
                "original_count": 2,
                "preserved_count": 1,
                "offloaded_arg_ids": set(),
                "offloaded_read_ids": set(),
            }
        )

        tracker = MagicMock()
        tracker.enabled = True
        tracker.get_status = AsyncMock(
            side_effect=RuntimeError("redis: connection reset")
        )

        with (
            patch("src.server.app.setup.agent_config", base_config),
            patch(f"{HANDLER}._resolve_graph_and_state", new=stub_resolve),
            patch(f"{HANDLER}._persist_context_window_event", new=_noop_persist),
            patch(
                "ptc_agent.agent.middleware.compaction.compact_messages",
                new=compact_mock,
            ),
            patch(
                "src.server.services.workflow_tracker.WorkflowTracker.get_instance",
                return_value=tracker,
            ),
        ):
            await trigger_compaction("thread-1", keep_messages=5)

        assert compact_mock.await_count == 1
