"""
Tests for trigger_compaction() — the manual /compact endpoint handler.

Regression coverage for the bug where the manual /compact path bypassed
resolve_llm_config and therefore always used the base YAML compaction model
instead of the user's compaction_model preference.
"""

from contextlib import AsyncExitStack, ExitStack, asynccontextmanager, contextmanager
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
from fastapi import HTTPException

from src.server.database.workspace_folders import WorkspaceFolderMoving


HANDLER = "src.server.handlers.thread_maintenance"
USER_MODELS = "src.server.services.llm.user_models"
CLIENTS = "src.server.services.llm.clients"
LLM_HANDLER = "src.server.services.llm.config"
COMPACT = "ptc_agent.agent.middleware.compaction.compact"


def _make_agent_config(
    compaction_model: str | None = "system-compaction",
    flash_model: str | None = "system-flash-model",
) -> AgentConfig:
    return AgentConfig(
        llm=LLMConfig(
            name="system-default-model",
            flash=flash_model,
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


def _compaction(preserved=None):
    """A compaction as ``compact_messages`` returns it."""
    from langchain_core.messages import HumanMessage

    from ptc_agent.agent.middleware.compaction import Compaction
    from ptc_agent.agent.middleware.compaction.summarize import Summary

    event = {"cutoff_index": 1, "summary_message": HumanMessage("ok", id="s"), "file_path": None}
    return Compaction(event, Summary("ok", "model", []), 2, preserved or [])


def _summary_model(cfg):
    """The model ``Summarizer.for_agent`` runs for ``cfg``. One resolved by
    name comes back as ``"by-name:<name>"``."""
    from ptc_agent.agent.middleware.compaction.compact import Summarizer

    with patch(f"{COMPACT}.get_llm_by_type", side_effect=lambda name: f"by-name:{name}"):
        return Summarizer.for_agent(cfg).model


def _stub_resolve_graph_and_state():
    """Return a coroutine factory producing the tuple _resolve_graph_and_state yields."""

    graph = MagicMock()
    graph.aupdate_state = AsyncMock(return_value=None)
    state = MagicMock()
    state.values = {"_summarization_event": None}
    messages = [MagicMock(id="m1"), MagicMock(id="m2")]
    backend = None
    lg_config = {"configurable": {"thread_id": "thread-1"}}

    async def _stub(thread_id, verb, config=None, checkpointer=None, user_id=None, held=None):
        _stub.captured_config = config
        _stub.captured_checkpointer = checkpointer
        _stub.captured_user_id = user_id
        return graph, lg_config, state, messages, "ws-1", backend

    _stub.captured_config = None
    _stub.captured_checkpointer = None
    _stub.captured_user_id = None
    _stub.graph = graph
    _stub.state = state
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
            yield MutationSession(conn=None, saver=saver)
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
    from src.server.handlers.thread_maintenance import trigger_compaction

    stub_resolve = _stub_resolve_graph_and_state()

    compact_mock = AsyncMock(return_value=_compaction())

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
            f"{USER_MODELS}.get_model_preference",
            new_callable=AsyncMock,
            return_value={"compaction_model": "user-compaction-model"},
        ),
        patch(
            f"{CLIENTS}.resolve_oauth_llm_client",
            new_callable=AsyncMock,
            return_value=None,
        ),
        patch("src.llms.llm.LLM.get_model_config", return_value=mock_mc),
    ):
        await trigger_compaction("thread-1", keep_messages=5, user_id="user-1")

    assert compact_mock.await_count == 1
    # Graph building and the summary both run on the resolved
    # (user-overridden) config, not the untouched base config.
    resolved = stub_resolve.captured_config
    assert resolved is not None
    assert resolved.llm.compaction == "user-compaction-model"
    assert compact_mock.await_args.args[2] is resolved
    model = _summary_model(resolved)
    assert model == "by-name:user-compaction-model", (
        "Manual /compact must honor the user's compaction_model preference, "
        f"got {model!r}"
    )
    # The summary points at the transcript only once this folder can read it.
    assert compact_mock.await_args.kwargs["workspace_id"] == "ws-1"

    # The session acquire behind it resolves MCP/OAuth per owner, so the caller
    # identity must not be dropped on the way down.
    assert stub_resolve.captured_user_id == "user-1"


THREAD_MODEL = "src.server.services.llm.thread_model"


async def _compact_on_thread_model(base_config, resolve_stub, msg_type="ptc"):
    """Run a manual /compact on a thread holding ``m-thread``; returns the
    ``(model, mode)`` pairs ``resolve_llm_config`` was asked for, in order."""
    from src.server.handlers.thread_maintenance import trigger_compaction

    asked: list = []

    async def _resolve(base_cfg, user_id, request_model, is_byok, mode="ptc", **kwargs):
        asked.append((request_model, mode))
        return await resolve_stub(base_cfg, request_model)

    compact_mock = AsyncMock(return_value=_compaction())
    with (
        patch("src.server.app.setup.agent_config", base_config),
        patch(f"{HANDLER}._resolve_graph_and_state", new=_stub_resolve_graph_and_state()),
        patch(f"{HANDLER}._persist_context_window_event", new=_noop_persist),
        patch("ptc_agent.agent.middleware.compaction.compact_messages", new=compact_mock),
        patch("src.server.database.api_keys.is_byok_active", new_callable=AsyncMock, return_value=False),
        patch(
            "src.server.database.conversation.get_thread_auth_meta",
            new_callable=AsyncMock,
            return_value={"llm_model": "m-thread", "msg_type": msg_type},
        ),
        patch(f"{THREAD_MODEL}.turn_model", new_callable=AsyncMock, side_effect=lambda *a, held, **k: held),
        patch(f"{LLM_HANDLER}.resolve_llm_config", new=_resolve),
    ):
        await trigger_compaction("thread-1", keep_messages=5, user_id="user-1")
    compact_mock.assert_awaited_once()
    return asked


@pytest.mark.asyncio
async def test_manual_compact_runs_the_threads_own_model(base_config):
    """A thread that holds a model compacts on it, as its turns run, not on the
    account default: another model would bring another credential and preset."""

    async def _resolve(base_cfg, model):
        return base_cfg.model_copy(deep=True)

    assert await _compact_on_thread_model(base_config, _resolve) == [("m-thread", "ptc")]


@pytest.mark.asyncio
async def test_manual_compact_resolves_a_flash_thread_in_flash_mode(base_config):
    """A flash thread's model sits in the flash slot, which the compaction model
    falls back to; PTC mode would summarize on the account's flash default
    while automatic compaction ran on the thread's model."""

    async def _resolve(base_cfg, model):
        return base_cfg.model_copy(deep=True)

    asked = await _compact_on_thread_model(base_config, _resolve, msg_type="flash")
    assert asked == [("m-thread", "flash")]


@pytest.mark.asyncio
async def test_manual_compact_runs_the_default_when_the_threads_model_cannot_run(base_config):
    """A lapsed connection behind the thread's model does not block a compaction
    the user asked for; a summary changes no answer, so it runs on the default."""

    async def _resolve(base_cfg, model):
        if model == "m-thread":
            raise HTTPException(status_code=400, detail={"type": "oauth_required"})
        return base_cfg.model_copy(deep=True)

    assert await _compact_on_thread_model(base_config, _resolve) == [
        ("m-thread", "ptc"), (None, "ptc"),
    ]


@pytest.mark.asyncio
async def test_manual_compact_without_user_id_uses_base_config(base_config):
    """No user_id → no resolve_llm_config call; base YAML compaction model is used."""
    from src.server.handlers.thread_maintenance import trigger_compaction

    stub_resolve = _stub_resolve_graph_and_state()

    compact_mock = AsyncMock(return_value=_compaction())

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
    cfg = compact_mock.await_args.args[2]
    assert cfg is base_config
    assert _summary_model(cfg) == "by-name:system-compaction"
    assert resolve_spy.await_count == 0


@pytest.mark.asyncio
async def test_resolve_failure_falls_back_to_base_config(base_config):
    """If resolve_llm_config raises, manual /compact logs and falls back cleanly."""
    from src.server.handlers.thread_maintenance import trigger_compaction

    stub_resolve = _stub_resolve_graph_and_state()

    compact_mock = AsyncMock(return_value=_compaction())

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
    cfg = compact_mock.await_args.args[2]
    assert cfg is base_config
    assert _summary_model(cfg) == "by-name:system-compaction"


@pytest.mark.asyncio
async def test_manual_compact_summarizes_with_the_subsidiary_oauth_client(base_config):
    """When the user has an OAuth-resolved subsidiary compaction client (the
    same client the auto path uses), manual /compact must summarize with it
    rather than re-resolving via the system LLM factory. Otherwise users on
    Codex/Claude OAuth or BYOK get billed wrong or 4xx."""
    from src.server.handlers.thread_maintenance import trigger_compaction

    stub_resolve = _stub_resolve_graph_and_state()

    compact_mock = AsyncMock(return_value=_compaction())

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

    cfg = compact_mock.await_args.args[2]
    assert cfg.llm.compaction == "user-compaction-model"
    model = _summary_model(cfg)
    # A copy, so maybe_disable_streaming in Summarizer.for_agent can't
    # mutate streaming=False on the shared subsidiary client.
    oauth_client.model_copy.assert_called_once_with()
    assert model is oauth_client.model_copy.return_value, (
        "Manual /compact must summarize with a copy of the OAuth/BYOK "
        "subsidiary compaction client, not rebuild a bare system-auth client."
    )
    # thread_id must reach resolve_llm_config so prompt_cache_key binds to the
    # session shard when running on an OpenAI-family compaction model.
    assert resolve_kwargs.get("thread_id") == "thread-1"


@pytest.mark.asyncio
async def test_manual_compact_falls_back_to_main_llm_client():
    """With no compaction model at all (blank compaction, no flash), summarize
    with a copy of the main client, as the automatic path does."""
    from src.server.handlers.thread_maintenance import trigger_compaction

    base_config = _make_agent_config(compaction_model=None, flash_model=None)

    stub_resolve = _stub_resolve_graph_and_state()

    compact_mock = AsyncMock(return_value=_compaction())

    main_client = MagicMock(name="main-byok-client")
    resolve_kwargs: dict = {}

    async def _resolve_stub(base_cfg, user_id, request_model, is_byok, mode="ptc", **kwargs):
        resolve_kwargs.update(kwargs)
        cfg = base_cfg.model_copy(deep=True)
        cfg.llm_client = main_client
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

    model = _summary_model(compact_mock.await_args.args[2])
    # A copy: maybe_disable_streaming would otherwise permanently set
    # streaming=False on the main agent's shared llm_client.
    main_client.model_copy.assert_called_once_with()
    assert model is main_client.model_copy.return_value
    assert resolve_kwargs.get("thread_id") == "thread-1"


@pytest.mark.asyncio
async def test_manual_compact_platform_user_resolves_by_name():
    """A platform user has a main client but no role client. Manual /compact
    must still run the compaction model by name, the same model automatic
    compaction uses, not a copy of the main client. A blank compaction means
    flash."""
    from src.server.handlers.thread_maintenance import trigger_compaction

    base_config = _make_agent_config(compaction_model=None)
    base_config.llm_client = MagicMock(name="platform-main-client")
    base_config.subsidiary_llm_clients.pop("compaction", None)

    compact_mock = AsyncMock(return_value=_compaction())

    with (
        patch("src.server.app.setup.agent_config", base_config),
        patch(f"{HANDLER}._resolve_graph_and_state", new=_stub_resolve_graph_and_state()),
        patch(f"{HANDLER}._persist_context_window_event", new=_noop_persist),
        patch(
            "ptc_agent.agent.middleware.compaction.compact_messages",
            new=compact_mock,
        ),
    ):
        await trigger_compaction("thread-1", keep_messages=5)

    cfg = compact_mock.await_args.args[2]
    assert _summary_model(cfg) == "by-name:system-flash-model"
    base_config.llm_client.model_copy.assert_not_called()


@pytest.mark.asyncio
async def test_manual_compact_summarizes_on_a_copy_of_the_main_client():
    """Regression: the summary model built from the main client MUST be a copy.

    ``Summarizer.for_agent`` calls ``maybe_disable_streaming``, which sets
    ``streaming = False`` in place on the client. On the shared
    ``agent_cfg.llm_client`` itself, the main agent's model is permanently
    mutated and all subsequent chat workflows lose SSE token streaming.
    Mirrors the ``.model_copy()`` pattern in ``PTCAgent.create_agent``.
    """
    from src.server.handlers.thread_maintenance import trigger_compaction

    stub_resolve = _stub_resolve_graph_and_state()

    compact_mock = AsyncMock(return_value=_compaction())

    base_config = _make_agent_config(compaction_model=None, flash_model=None)
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

    model = _summary_model(compact_mock.await_args.args[2])
    shared_client.model_copy.assert_called_once_with()
    assert model is not shared_client
    assert model is shared_client.model_copy.return_value


@pytest.mark.asyncio
async def test_manual_compact_resets_the_token_cache_and_prunes_dead_offloads(base_config):
    """The cached counts measured the view before the summary: left in place,
    the next model call reads them as over the threshold and compacts again.
    Offload ids of calls the summary replaced name nothing the model sees."""
    from langchain_core.messages import AIMessage

    from src.server.handlers.thread_maintenance import trigger_compaction

    stub_resolve = _stub_resolve_graph_and_state()
    stub_resolve.state.values = {
        "_summarization_event": None,
        "_cached_input_tokens": 150_000,
        "_cached_output_tokens": 2_000,
        "_offloaded_tool_call_ids": {"w-summarized", "w-kept"},
        "_offloaded_read_result_ids": {"r-summarized"},
    }
    kept = AIMessage("", id="a-kept", tool_calls=[{"name": "Write", "id": "w-kept", "args": {}}])
    compact_mock = AsyncMock(return_value=_compaction(preserved=[kept]))

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

    update = stub_resolve.graph.aupdate_state.await_args.args[1]
    assert update["_summarization_event"] is compact_mock.return_value.event
    assert update["_cached_input_tokens"] == update["_cached_output_tokens"] == 0
    assert update["_offloaded_tool_call_ids"] == {"w-kept"}
    assert update["_offloaded_read_result_ids"] == set()


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

        from src.server.handlers.thread_maintenance import trigger_compaction

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

        from src.server.handlers.thread_maintenance import trigger_offload

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

        from src.server.handlers.thread_maintenance import trigger_compaction
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
        from src.server.handlers.thread_maintenance import trigger_compaction

        fence_saver = object()
        runner = _fake_runner(saver=fence_saver)
        stub_resolve = _stub_resolve_graph_and_state()
        compact_mock = AsyncMock(return_value=_compaction())

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
        from src.server.handlers.thread_maintenance import trigger_offload

        fence_saver = object()
        runner = _fake_runner(saver=fence_saver)
        stub_resolve = _stub_resolve_graph_and_state()
        offload_mock = AsyncMock(return_value=(set(), set()))

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
        assert offload_mock.await_args.kwargs["workspace_id"] == "ws-1"

    @pytest.mark.asyncio
    async def test_offload_threads_the_caller_identity(self, base_config):
        # Same contract as compaction: the route's x_user_id must reach the
        # session acquire, whose MCP resolve is owner-scoped.
        from src.server.handlers.thread_maintenance import trigger_offload

        runner = _fake_runner()
        stub_resolve = _stub_resolve_graph_and_state()
        offload_mock = AsyncMock(return_value=(set(), set()))

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
            await trigger_offload("thread-1", user_id="user-1")

        assert stub_resolve.captured_user_id == "user-1"

    @pytest.mark.asyncio
    async def test_compact_releases_fence_on_error(self, base_config):
        """A failure inside the critical section still releases the fence, so
        a queued POST is not blocked past the runner's own cleanup."""
        from fastapi import HTTPException

        from src.server.handlers.thread_maintenance import trigger_compaction

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

        from src.server.handlers.thread_maintenance import trigger_compaction

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

        from src.server.handlers.thread_maintenance import trigger_offload

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


# ---------------------------------------------------------------------------
# Session acquire — the workspace manager resolves MCP/OAuth per owner
# ---------------------------------------------------------------------------


class TestSessionAcquireIdentity:
    @pytest.mark.asyncio
    async def test_the_caller_identity_reaches_the_session_acquire(self, base_config):
        """An acquire with no user_id resolves the owner's server set empty, and
        a recovery taken from that path would carry the gap into provisioning."""
        from src.server.handlers.thread_maintenance import _resolve_graph_and_state

        from src.server.services.workspace_manager import WorkspaceManager

        manager = MagicMock(spec=WorkspaceManager)
        manager.get_session_for_workspace = AsyncMock(
            return_value=MagicMock(sandbox=None)
        )
        graph = MagicMock()
        graph.aget_state = AsyncMock(
            return_value=MagicMock(values={"messages": [MagicMock(id="m1")]})
        )

        with (
            patch(
                "src.server.database.conversation.get_thread_with_summary",
                new_callable=AsyncMock,
                return_value={"workspace_id": "ws-1"},
            ),
            patch(
                "src.server.services.workspace_manager.WorkspaceManager.get_instance",
                return_value=manager,
            ),
            patch(
                "ptc_agent.agent.graph.build_ptc_graph_with_session",
                new_callable=AsyncMock,
                return_value=graph,
            ),
        ):
            await _resolve_graph_and_state(
                "thread-1",
                "compact",
                config=base_config,
                checkpointer=MagicMock(),
                user_id="user-1",
                held=AsyncExitStack(),
            )

        assert manager.get_session_for_workspace.await_args.kwargs["user_id"] == "user-1"


class TestTheBackendsFolder:
    """/compact and /offload have no run a settle counts as busy, and what they
    offload is the only copy once the checkpoint is truncated."""

    @staticmethod
    @contextmanager
    def _resolving(events, *, dir_name="Macro", held_by_a_settle=False):
        from src.server.services.workspace_manager import WorkspaceManager

        manager = MagicMock(spec=WorkspaceManager)
        manager.get_session_for_workspace = AsyncMock(
            return_value=MagicMock(sandbox=MagicMock(working_dir="/home/workspace"))
        )
        graph = MagicMock()
        graph.aget_state = AsyncMock(
            return_value=MagicMock(values={"messages": [MagicMock(id="m1")]})
        )

        @asynccontextmanager
        async def hold(workspace_id):
            events.append(f"hold {workspace_id}")
            if held_by_a_settle:
                raise WorkspaceFolderMoving(workspace_id)
            try:
                yield
            finally:
                events.append("release")

        async def read_row(_workspace_id):
            events.append("read")
            return {"dir_name": dir_name}

        with ExitStack() as stack:
            for target, new in (
                ("src.server.database.conversation.get_thread_with_summary",
                 AsyncMock(return_value={"workspace_id": "ws-1"})),
                ("src.server.services.workspace_manager.WorkspaceManager.get_instance",
                 MagicMock(return_value=manager)),
                ("ptc_agent.agent.graph.build_ptc_graph_with_session", AsyncMock(return_value=graph)),
                ("src.server.database.workspace_folders.workspace_folder_in_use", hold),
                ("src.server.database.workspace.get_workspace", read_row),
            ):
                stack.enter_context(patch(target, new))
            yield

    @pytest.mark.asyncio
    async def test_the_backend_writes_to_the_folder_read_under_the_hold(self, base_config):
        from src.server.handlers.thread_maintenance import _resolve_graph_and_state

        events = []
        with self._resolving(events):
            async with AsyncExitStack() as held:
                *_, backend = await _resolve_graph_and_state(
                    "thread-1", "offload", config=base_config, checkpointer=MagicMock(), held=held
                )
                events.append("offload")

        assert events == ["hold ws-1", "read", "offload", "release"]
        assert backend.root_dir == "/home/workspace/Macro"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("moving", ["staged", "held by a settle"])
    async def test_a_folder_a_settle_is_moving_is_refused_for_a_retry(self, base_config, moving):
        from src.server.handlers.thread_maintenance import _resolve_graph_and_state

        events = []
        resolving = self._resolving(
            events,
            dir_name="_internal/moving/ws-1" if moving == "staged" else "Macro",
            held_by_a_settle=moving == "held by a settle",
        )
        with resolving, pytest.raises(HTTPException) as refused:
            async with AsyncExitStack() as held:
                await _resolve_graph_and_state(
                    "thread-1", "offload", config=base_config, checkpointer=MagicMock(), held=held
                )

        assert refused.value.status_code == 503
        assert events == (
            ["hold ws-1", "read", "release"] if moving == "staged" else ["hold ws-1"]
        )
