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

    async def _stub(thread_id, verb, config=None, checkpointer=None):
        _stub.captured_config = config
        _stub.captured_checkpointer = checkpointer
        return graph, lg_config, state, messages, backend

    _stub.captured_config = None
    _stub.captured_checkpointer = None
    return _stub


async def _noop_persist(*args, **kwargs):
    return None


RUNNER_GET_INSTANCE = (
    "src.server.services.thread_mutation.ThreadMutationRunner.get_instance"
)


def _fake_runner(refusal: Exception | None = None, saver=None):
    """A ThreadMutationRunner stand-in: ``exclusive`` yields an unfenced
    session (or the given saver), or raises the given refusal. ``held`` /
    ``released`` record the fence lifecycle."""
    from contextlib import asynccontextmanager

    from src.server.services.thread_mutation import MutationSession

    runner = MagicMock()
    runner.held = []
    runner.released = []

    @asynccontextmanager
    async def _exclusive(thread_id, verb):
        if refusal is not None:
            raise refusal
        runner.held.append((thread_id, verb))
        try:
            yield MutationSession(op_id="test-op", conn=None, saver=saver)
        finally:
            runner.released.append((thread_id, verb))

    runner.exclusive = _exclusive
    return runner


@pytest.fixture(autouse=True)
def mutation_runner():
    """Handler tests exercise the compact/offload logic, not the fence: stub
    the runner with an unfenced pass-through session. Fence tests re-patch
    ``get_instance`` inside their own with-blocks (the inner patch wins)."""
    runner = _fake_runner()
    with patch(RUNNER_GET_INSTANCE, return_value=runner):
        yield runner


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
    resolve_kwargs: dict = {}

    async def _resolve_stub(base_cfg, user_id, request_model, is_byok, mode="ptc", **kwargs):
        resolve_kwargs.update(kwargs)
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
    # thread_id must reach resolve_llm_config so prompt_cache_key binds to the
    # session shard when running on an OpenAI-family compaction model.
    assert resolve_kwargs.get("thread_id") == "thread-1"


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
    resolve_kwargs: dict = {}

    async def _resolve_stub(base_cfg, user_id, request_model, is_byok, mode="ptc", **kwargs):
        resolve_kwargs.update(kwargs)
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
    assert resolve_kwargs.get("thread_id") == "thread-1"


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


class TestMutationFence:
    """trigger_compaction/trigger_offload hold the ThreadMutationRunner
    exclusive fence (v4 2.4): the runner's ledger gate + exclusive T(thread)
    lock replaced the old tracker gate and in-memory compaction guard. The
    handler's job — pinned here — is mapping the runner's refusals onto the
    HTTP contract the frontend branches on (409 ``workflow_active`` /
    ``compaction_in_progress`` / ``thread_busy``, 503 on budget exhaustion),
    holding the fence across the critical section (released even on error or
    a user Stop), and threading the fence-bound saver into graph building so
    checkpoint writes die with the lock session."""

    def _compact_result(self):
        return {
            "event": {"summary_text": "ok"},
            "summary_text": "ok",
            "original_count": 2,
            "preserved_count": 1,
            "offloaded_arg_ids": set(),
            "offloaded_read_ids": set(),
        }

    def _conflict(self, code: str, verb: str):
        from src.server.services.thread_mutation import MutationConflict

        return MutationConflict(code, verb, f"refused: {code}")

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "code", ["workflow_active", "compaction_in_progress", "thread_busy"]
    )
    async def test_compact_maps_runner_refusal_to_409(self, base_config, code):
        """Every MutationConflict — live run on any worker, a rival mutation,
        or tail writers still holding shared T — surfaces as 409 with the
        runner's structured detail, before any graph read or LLM call."""
        from fastapi import HTTPException

        from src.server.handlers.workflow_handler import trigger_compaction

        compact_mock = AsyncMock()  # must NEVER run
        stub_resolve = _stub_resolve_graph_and_state()

        with (
            patch("src.server.app.setup.agent_config", base_config),
            patch(f"{HANDLER}._resolve_graph_and_state", new=stub_resolve),
            patch(
                "ptc_agent.agent.middleware.compaction.compact_messages",
                new=compact_mock,
            ),
            patch(
                RUNNER_GET_INSTANCE,
                return_value=_fake_runner(refusal=self._conflict(code, "compact")),
            ),
        ):
            with pytest.raises(HTTPException) as exc_info:
                await trigger_compaction("thread-1", keep_messages=5)

        assert exc_info.value.status_code == 409
        detail = exc_info.value.detail
        assert isinstance(detail, dict)
        assert detail["code"] == code
        assert detail["verb"] == "compact"
        assert compact_mock.await_count == 0
        assert stub_resolve.captured_config is None

    @pytest.mark.asyncio
    async def test_offload_maps_runner_refusal_to_409(self, base_config):
        from fastapi import HTTPException

        from src.server.handlers.workflow_handler import trigger_offload

        offload_mock = AsyncMock()  # must NEVER run
        stub_resolve = _stub_resolve_graph_and_state()

        with (
            patch("src.server.app.setup.agent_config", base_config),
            patch(f"{HANDLER}._resolve_graph_and_state", new=stub_resolve),
            patch(
                "ptc_agent.agent.middleware.compaction.offload_tool_args",
                new=offload_mock,
            ),
            patch(
                RUNNER_GET_INSTANCE,
                return_value=_fake_runner(
                    refusal=self._conflict("workflow_active", "offload")
                ),
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
    async def test_compact_maps_budget_exhaustion_to_503(self, base_config):
        """MutationUnavailable (pinned-session budget) is a bounded retryable
        503, mirroring WriterGuardUnavailable at the chat boundary."""
        from fastapi import HTTPException

        from src.server.handlers.workflow_handler import trigger_compaction
        from src.server.services.thread_mutation import MutationUnavailable

        compact_mock = AsyncMock()  # must NEVER run

        with (
            patch("src.server.app.setup.agent_config", base_config),
            patch(
                f"{HANDLER}._resolve_graph_and_state",
                new=_stub_resolve_graph_and_state(),
            ),
            patch(
                "ptc_agent.agent.middleware.compaction.compact_messages",
                new=compact_mock,
            ),
            patch(
                RUNNER_GET_INSTANCE,
                return_value=_fake_runner(
                    refusal=MutationUnavailable("budget exhausted")
                ),
            ),
        ):
            with pytest.raises(HTTPException) as exc_info:
                await trigger_compaction("thread-1", keep_messages=5)

        assert exc_info.value.status_code == 503
        assert compact_mock.await_count == 0

    @pytest.mark.asyncio
    async def test_compact_holds_fence_and_threads_fence_saver(self, base_config):
        """The critical section runs inside the fence (held+released exactly
        once) and graph building receives the fence-bound saver, not the
        global pooled one."""
        from src.server.handlers.workflow_handler import trigger_compaction

        fence_saver = object()
        runner = _fake_runner(saver=fence_saver)
        stub_resolve = _stub_resolve_graph_and_state()
        compact_mock = AsyncMock(return_value=self._compact_result())

        with (
            patch("src.server.app.setup.agent_config", base_config),
            patch(f"{HANDLER}._resolve_graph_and_state", new=stub_resolve),
            patch(f"{HANDLER}._persist_context_window_event", new=_noop_persist),
            patch(
                "ptc_agent.agent.middleware.compaction.compact_messages",
                new=compact_mock,
            ),
            patch(RUNNER_GET_INSTANCE, return_value=runner),
        ):
            await trigger_compaction("thread-1", keep_messages=5)

        assert runner.held == [("thread-1", "compact")]
        assert runner.released == [("thread-1", "compact")]
        assert stub_resolve.captured_checkpointer is fence_saver

    @pytest.mark.asyncio
    async def test_offload_threads_fence_saver(self, base_config):
        from src.server.handlers.workflow_handler import trigger_offload

        fence_saver = object()
        runner = _fake_runner(saver=fence_saver)
        stub_resolve = _stub_resolve_graph_and_state()
        offload_mock = AsyncMock(
            return_value={
                "offloaded_args": 0,
                "offloaded_reads": 0,
                "messages": [],
                "original_count": 2,
            }
        )

        with (
            patch("src.server.app.setup.agent_config", base_config),
            patch(f"{HANDLER}._resolve_graph_and_state", new=stub_resolve),
            patch(f"{HANDLER}._persist_context_window_event", new=_noop_persist),
            patch(
                "ptc_agent.agent.middleware.compaction.offload_tool_args",
                new=offload_mock,
            ),
            patch(RUNNER_GET_INSTANCE, return_value=runner),
        ):
            await trigger_offload("thread-1")

        assert runner.held == [("thread-1", "offload")]
        assert runner.released == [("thread-1", "offload")]
        assert stub_resolve.captured_checkpointer is fence_saver

    @pytest.mark.asyncio
    async def test_compact_releases_fence_on_error(self, base_config):
        """A failure inside the critical section still releases the fence, so
        a queued POST is not blocked past the runner's own cleanup."""
        from fastapi import HTTPException

        from src.server.handlers.workflow_handler import trigger_compaction

        runner = _fake_runner()
        compact_mock = AsyncMock(side_effect=RuntimeError("boom"))

        with (
            patch("src.server.app.setup.agent_config", base_config),
            patch(
                f"{HANDLER}._resolve_graph_and_state",
                new=_stub_resolve_graph_and_state(),
            ),
            patch(f"{HANDLER}._persist_context_window_event", new=_noop_persist),
            patch(
                "ptc_agent.agent.middleware.compaction.compact_messages",
                new=compact_mock,
            ),
            patch(RUNNER_GET_INSTANCE, return_value=runner),
        ):
            with pytest.raises(HTTPException) as exc_info:
                await trigger_compaction("thread-1", keep_messages=5)

        assert exc_info.value.status_code == 500
        assert runner.released == [("thread-1", "compact")]

    @pytest.mark.asyncio
    async def test_compact_cancelled_surfaces_clean_http(self, base_config):
        """A user Stop (/cancel → runner.request_stop) cancels this request
        task, often mid summarize-LLM call. ``CancelledError`` is a
        BaseException, so without handling it bubbles to ASGI as a raw 500 —
        the shared ``cancellation_as_http`` wrapper converts it to a clean
        409 ``request_cancelled``; the fence must still be released."""
        import asyncio

        from fastapi import HTTPException

        from src.server.handlers.workflow_handler import trigger_compaction

        runner = _fake_runner()
        compact_mock = AsyncMock(side_effect=asyncio.CancelledError())

        with (
            patch("src.server.app.setup.agent_config", base_config),
            patch(
                f"{HANDLER}._resolve_graph_and_state",
                new=_stub_resolve_graph_and_state(),
            ),
            patch(f"{HANDLER}._persist_context_window_event", new=_noop_persist),
            patch(
                "ptc_agent.agent.middleware.compaction.compact_messages",
                new=compact_mock,
            ),
            patch(RUNNER_GET_INSTANCE, return_value=runner),
        ):
            with pytest.raises(HTTPException) as exc_info:
                await trigger_compaction("thread-1", keep_messages=5)

        assert exc_info.value.status_code == 409
        detail = exc_info.value.detail
        assert isinstance(detail, dict)
        assert detail["code"] == "request_cancelled"
        assert detail["verb"] == "compact"
        assert runner.released == [("thread-1", "compact")]

    @pytest.mark.asyncio
    async def test_offload_cancelled_surfaces_clean_http(self, base_config):
        """Same as the compact case for /offload."""
        import asyncio

        from fastapi import HTTPException

        from src.server.handlers.workflow_handler import trigger_offload

        runner = _fake_runner()
        offload_mock = AsyncMock(side_effect=asyncio.CancelledError())

        with (
            patch("src.server.app.setup.agent_config", base_config),
            patch(
                f"{HANDLER}._resolve_graph_and_state",
                new=_stub_resolve_graph_and_state(),
            ),
            patch(f"{HANDLER}._persist_context_window_event", new=_noop_persist),
            patch(
                "ptc_agent.agent.middleware.compaction.offload_tool_args",
                new=offload_mock,
            ),
            patch(RUNNER_GET_INSTANCE, return_value=runner),
        ):
            with pytest.raises(HTTPException) as exc_info:
                await trigger_offload("thread-1")

        assert exc_info.value.status_code == 409
        detail = exc_info.value.detail
        assert isinstance(detail, dict)
        assert detail["code"] == "request_cancelled"
        assert detail["verb"] == "offload"
        assert runner.released == [("thread-1", "offload")]
