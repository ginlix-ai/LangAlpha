"""
Tests for trigger_compaction() — the manual /compact endpoint handler.

Manual /compact is sent as the agent's own model call between turns
(``ask_between_turns``), on an agent built and resolved as the thread's next
turn would be: the user's compaction_model, the thread's own model, effort,
fast mode, workspace and role. An agent that compacts nothing falls back to
the summarizer alone (``compact_messages``) on the same config.
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
from ptc_agent.agent.middleware.compaction import NoCompaction
from ptc_agent.core.project_context import ProjectContext
from fastapi import HTTPException

from src.server.database.workspace_folders import WorkspaceFolderMoving


HANDLER = "src.server.handlers.thread_maintenance"
USER_MODELS = "src.server.services.llm.user_models"
CLIENTS = "src.server.services.llm.clients"
LLM_HANDLER = "src.server.services.llm.config"
COMPACT = "ptc_agent.agent.middleware.compaction.compact"
ASK = "ptc_agent.agent.middleware.compaction.ask_between_turns"
COMPACT_MESSAGES = "ptc_agent.agent.middleware.compaction.compact_messages"
THREAD_META = "src.server.database.conversation.get_thread_auth_meta"
WORKSPACE_ROW = "src.server.database.workspace.get_workspace"


def _make_agent_config(
    compaction_model: str | None = "system-compaction",
    background_model: str | None = "system-background-model",
) -> AgentConfig:
    return AgentConfig(
        llm=LLMConfig(
            name="system-default-model",
            background=background_model,
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
            "system-background-model",
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
    """A compaction as the agent's model chain (or ``compact_messages``)
    answers it."""
    from langchain_core.messages import HumanMessage

    from ptc_agent.agent.middleware.compaction import Compaction
    from ptc_agent.agent.middleware.compaction.summarize import Summary

    event = {"cutoff_index": 1, "summary_message": HumanMessage("ok", id="s"), "file_path": None}
    return Compaction(event, Summary("ok", "model", []), 2, preserved or [])


def _summary_model(cfg):
    """The model ``Summarizer.for_agent`` runs for ``cfg``, which the graph
    built on ``cfg`` summarizes with. One resolved by name comes back as
    ``"by-name:<name>"``."""
    from ptc_agent.agent.middleware.compaction.compact import Summarizer

    with patch(f"{COMPACT}.create_llm", side_effect=lambda name, **_: f"by-name:{name}"):
        return Summarizer.for_agent(cfg).model


PROJECT = ProjectContext("ws-1", "Macro")


def _stub_thread_agent():
    """A coroutine standing in for ``_thread_agent``, yielding its ThreadAgent."""
    from src.server.handlers.thread_maintenance import ThreadAgent

    graph = MagicMock()
    graph.aupdate_state = AsyncMock(return_value=None)
    state = MagicMock()
    state.values = {"_summarization_event": None}
    messages = [MagicMock(id="m1"), MagicMock(id="m2")]
    backend = None

    async def _stub(
        thread_id, verb, config=None, checkpointer=None, user_id=None, held=None, role="analyst"
    ):
        _stub.captured_config = config
        _stub.captured_checkpointer = checkpointer
        _stub.captured_user_id = user_id
        _stub.captured_role = role
        return ThreadAgent(thread_id, graph, state, messages, "ws-1", backend, PROJECT)

    _stub.captured_config = None
    _stub.captured_checkpointer = None
    _stub.captured_user_id = None
    _stub.captured_role = None
    _stub.graph = graph
    _stub.state = state
    _stub.messages = messages
    return _stub


def _asked(answer=None, *, error=None):
    """An ``ask_between_turns`` stand-in answering ``answer`` (a fresh
    compaction by default), or raising ``error``; ``.projects`` records the
    project bound around each call."""
    from ptc_agent.core.project_context import current_project

    projects: list = []

    async def _ask(agent, state, ask, **kwargs):
        projects.append(current_project())
        if error is not None:
            raise error
        return answer if answer is not None else _compaction()

    mock = AsyncMock(side_effect=_ask)
    mock.projects = projects
    return mock


@pytest.fixture(autouse=True)
def thread_row():
    """The thread's row and its workspace, as ``thread_route`` reads them: an
    analyst's workspace, no model, effort or fast mode of its own. Tests
    change the dict in place, or patch the reads themselves."""
    meta = {
        "workspace_id": "ws-1",
        "llm_model": None,
        "reasoning_effort": None,
        "fast_mode": None,
        "msg_type": "ptc",
    }
    workspace = {"workspace_id": "ws-1", "user_id": "user-1", "status": "running"}
    with (
        patch(THREAD_META, new=AsyncMock(side_effect=lambda _t: meta)),
        patch(WORKSPACE_ROW, new=AsyncMock(side_effect=lambda _w: workspace)),
    ):
        yield {"meta": meta, "workspace": workspace}


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
    """When user_id is passed and pref sets compaction_model, the agent the
    compaction is asked of is built on it."""
    from ptc_agent.agent.middleware.compaction import Compact
    from src.server.app import setup
    from src.server.handlers.thread_maintenance import trigger_compaction

    stub_resolve = _stub_thread_agent()
    ask = _asked()
    compact_mock = AsyncMock(side_effect=AssertionError("the summarizer alone ran"))

    mock_mc = _mock_model_config()

    with (
        patch("src.server.app.setup.agent_config", base_config),
        patch(f"{HANDLER}._thread_agent", new=stub_resolve),
        patch(ASK, new=ask),
        patch(COMPACT_MESSAGES, new=compact_mock),
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
        await trigger_compaction("thread-1", user_id="user-1")

    # Asked of the built agent, on the state the next turn starts from, with
    # the turn's project bound as a run binds it.
    ask.assert_awaited_once_with(
        stub_resolve.graph.agent,
        stub_resolve.state.values,
        Compact(),
        thread_id="thread-1",
        store=setup.store,
    )
    assert ask.projects == [PROJECT]
    compact_mock.assert_not_awaited()
    # The graph, and the summary its compaction writes, run on the resolved
    # (user-overridden) config, not the untouched base config.
    resolved = stub_resolve.captured_config
    assert resolved is not None
    assert resolved.llm.compaction == "user-compaction-model"
    model = _summary_model(resolved)
    assert model == "by-name:user-compaction-model", (
        "Manual /compact must honor the user's compaction_model preference, "
        f"got {model!r}"
    )

    # The session acquire behind it resolves MCP/OAuth per owner, so the caller
    # identity must not be dropped on the way down.
    assert stub_resolve.captured_user_id == "user-1"
    assert stub_resolve.captured_role == "analyst"


@pytest.mark.asyncio
async def test_an_agent_that_compacts_nothing_falls_back_to_the_summarizer(base_config):
    """Compaction off in the agent's config leaves no middleware to answer:
    the summarizer alone writes the summary, on the same resolved config, and
    the compact installs it as it would the agent's."""
    from src.server.handlers.thread_maintenance import trigger_compaction

    stub_resolve = _stub_thread_agent()
    ask = _asked(error=NoCompaction("the agent has no compaction to ask"))
    compaction = _compaction()
    compact_mock = AsyncMock(return_value=compaction)

    with (
        patch("src.server.app.setup.agent_config", base_config),
        patch(f"{HANDLER}._thread_agent", new=stub_resolve),
        patch(ASK, new=ask),
        patch(COMPACT_MESSAGES, new=compact_mock),
    ):
        result = await trigger_compaction("thread-1")

    ask.assert_awaited_once()
    compact_mock.assert_awaited_once()
    args, kwargs = compact_mock.await_args.args, compact_mock.await_args.kwargs
    assert args == (stub_resolve.messages, stub_resolve.state.values, base_config)
    assert kwargs["thread_id"] == "thread-1"
    # The summary points at the transcript only once this folder can read it.
    assert kwargs["workspace_id"] == "ws-1"
    assert kwargs["backend"] is None
    assert kwargs["notes_dir"] is None
    update = stub_resolve.graph.aupdate_state.await_args.args[1]
    assert update["_summarization_event"] is compaction.event
    assert result["summary_text"] == "ok"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "message",
    ["Not enough messages to compact. Have 2, need more than 5 to preserve.",
     "Cannot determine valid cutoff point for compaction"],
)
async def test_a_view_with_nothing_to_compact_is_a_400(base_config, message):
    from src.server.handlers.thread_maintenance import trigger_compaction

    stub_resolve = _stub_thread_agent()
    with (
        patch("src.server.app.setup.agent_config", base_config),
        patch(f"{HANDLER}._thread_agent", new=stub_resolve),
        patch(ASK, new=_asked(error=ValueError(message))),
        pytest.raises(HTTPException) as refused,
    ):
        await trigger_compaction("thread-1")

    assert refused.value.status_code == 400
    assert refused.value.detail == message
    stub_resolve.graph.aupdate_state.assert_not_awaited()


THREAD_MODEL = "src.server.services.llm.thread_model"


async def _compact_on_thread_model(base_config, resolve_stub):
    """Run a manual /compact on a thread holding ``m-thread``; returns the
    ``(model, slot)`` pairs ``resolve_llm_config`` was asked for, in order."""
    from src.server.handlers.thread_maintenance import trigger_compaction

    asked: list = []

    async def _resolve(base_cfg, user_id, request_model, is_byok, slot="primary", **kwargs):
        asked.append((request_model, slot))
        return await resolve_stub(base_cfg, request_model)

    ask = _asked()
    with (
        patch("src.server.app.setup.agent_config", base_config),
        patch(f"{HANDLER}._thread_agent", new=_stub_thread_agent()),
        patch(ASK, new=ask),
        patch("src.server.database.api_keys.is_byok_active", new_callable=AsyncMock, return_value=False),
        patch(
            THREAD_META,
            new_callable=AsyncMock,
            return_value={"llm_model": "m-thread", "msg_type": "ptc", "workspace_id": "ws-1"},
        ),
        patch(f"{THREAD_MODEL}.turn_model", new_callable=AsyncMock, side_effect=lambda *a, held, **k: held),
        patch(f"{LLM_HANDLER}.resolve_llm_config", new=_resolve),
    ):
        await trigger_compaction("thread-1", user_id="user-1")
    ask.assert_awaited_once()
    return asked


@pytest.mark.asyncio
async def test_manual_compact_runs_the_threads_own_model(base_config):
    """A thread that holds a model compacts on it, as its turns run, not on the
    account default: another model would bring another credential and preset."""

    async def _resolve(base_cfg, model):
        return base_cfg.model_copy(deep=True)

    assert await _compact_on_thread_model(base_config, _resolve) == [("m-thread", "primary")]


@pytest.mark.asyncio
async def test_manual_compact_runs_the_default_when_the_threads_model_cannot_run(base_config):
    """A lapsed connection behind the thread's model does not block a compaction
    the user asked for; a summary changes no answer, so it runs on the default."""

    async def _resolve(base_cfg, model):
        if model == "m-thread":
            raise HTTPException(status_code=400, detail={"type": "oauth_required"})
        return base_cfg.model_copy(deep=True)

    assert await _compact_on_thread_model(base_config, _resolve) == [
        ("m-thread", "primary"), (None, "primary"),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("home", [False, True], ids=["analyst", "chief_of_staff"])
async def test_the_compact_resolves_as_the_threads_next_turn(base_config, thread_row, home):
    """The thread's own effort and fast mode, in the workspace and role its
    next turn runs in, reach the resolver, and the agent is built in that
    role: a call between turns sends that turn's prompt and tools, or the
    provider's cache misses."""
    from src.server.database.home_workspace import get_home_workspace_id
    from src.server.handlers.thread_maintenance import trigger_compaction

    workspace_id = get_home_workspace_id("user-1") if home else "ws-1"
    thread_row["meta"].update(
        workspace_id=workspace_id, reasoning_effort="high", fast_mode=True
    )
    thread_row["workspace"]["workspace_id"] = workspace_id
    stub_resolve = _stub_thread_agent()
    resolve = AsyncMock(side_effect=lambda base_cfg, *a, **k: base_cfg.model_copy(deep=True))

    with (
        patch("src.server.app.setup.agent_config", base_config),
        patch(f"{HANDLER}._thread_agent", new=stub_resolve),
        patch(ASK, new=_asked()),
        patch("src.server.database.api_keys.is_byok_active", new_callable=AsyncMock, return_value=True),
        patch(f"{THREAD_MODEL}.turn_model", new_callable=AsyncMock, return_value=None),
        patch(f"{LLM_HANDLER}.resolve_llm_config", new=resolve),
    ):
        await trigger_compaction("thread-1", user_id="user-1")

    role = "chief_of_staff" if home else "analyst"
    resolve.assert_awaited_once_with(
        base_config,
        "user-1",
        None,
        True,
        reasoning_effort="high",
        fast_mode=True,
        thread_id="thread-1",
        enabled_subagents=None,
        workspace_id=workspace_id,
        role=role,
    )
    assert stub_resolve.captured_role == role


@pytest.mark.asyncio
async def test_manual_compact_without_user_id_uses_base_config(base_config):
    """No user_id → no resolve_llm_config call; base YAML compaction model is used."""
    from src.server.handlers.thread_maintenance import trigger_compaction

    stub_resolve = _stub_thread_agent()
    ask = _asked()

    # Guard: if resolve_llm_config is called we want the test to fail loudly.
    resolve_spy = AsyncMock(side_effect=AssertionError("resolve_llm_config called without user_id"))
    route_spy = AsyncMock(side_effect=AssertionError("thread route read without user_id"))

    with (
        patch("src.server.app.setup.agent_config", base_config),
        patch(f"{HANDLER}._thread_agent", new=stub_resolve),
        patch(ASK, new=ask),
        patch(f"{LLM_HANDLER}.resolve_llm_config", new=resolve_spy),
        patch(f"{HANDLER}.thread_route", new=route_spy),
    ):
        await trigger_compaction("thread-1")

    ask.assert_awaited_once()
    cfg = stub_resolve.captured_config
    assert cfg is base_config
    assert _summary_model(cfg) == "by-name:system-compaction"
    assert stub_resolve.captured_role == "analyst"
    assert resolve_spy.await_count == 0
    assert route_spy.await_count == 0


@pytest.mark.asyncio
async def test_resolve_failure_falls_back_to_base_config(base_config):
    """If resolve_llm_config raises, manual /compact logs and falls back cleanly."""
    from src.server.handlers.thread_maintenance import trigger_compaction

    stub_resolve = _stub_thread_agent()
    ask = _asked()

    failing_resolve = AsyncMock(side_effect=RuntimeError("db down"))

    with (
        patch("src.server.app.setup.agent_config", base_config),
        patch(f"{HANDLER}._thread_agent", new=stub_resolve),
        patch(ASK, new=ask),
        patch(
            "src.server.database.api_keys.is_byok_active",
            new_callable=AsyncMock,
            return_value=False,
        ),
        patch(f"{LLM_HANDLER}.resolve_llm_config", new=failing_resolve),
    ):
        await trigger_compaction("thread-1", user_id="user-1")

    # Fell back to base YAML compaction model; did not raise.
    ask.assert_awaited_once()
    cfg = stub_resolve.captured_config
    assert cfg is base_config
    assert _summary_model(cfg) == "by-name:system-compaction"


@pytest.mark.asyncio
async def test_manual_compact_summarizes_with_the_subsidiary_oauth_client(base_config):
    """When the user has an OAuth-resolved subsidiary compaction client (the
    same client the auto path uses), manual /compact must summarize with it
    rather than re-resolving via the system LLM factory. Otherwise users on
    Codex/Claude OAuth or BYOK get billed wrong or 4xx."""
    from src.server.handlers.thread_maintenance import trigger_compaction

    stub_resolve = _stub_thread_agent()

    oauth_client = MagicMock(name="oauth-codex-client")
    resolve_kwargs: dict = {}

    async def _resolve_stub(base_cfg, user_id, request_model, is_byok, slot="primary", **kwargs):
        resolve_kwargs.update(kwargs)
        cfg = base_cfg.model_copy(deep=True)
        cfg.llm.compaction = "user-compaction-model"
        cfg.subsidiary_llm_clients["compaction"] = oauth_client
        return cfg

    with (
        patch("src.server.app.setup.agent_config", base_config),
        patch(f"{HANDLER}._thread_agent", new=stub_resolve),
        patch(ASK, new=_asked()),
        patch(
            "src.server.database.api_keys.is_byok_active",
            new_callable=AsyncMock,
            return_value=False,
        ),
        patch(f"{LLM_HANDLER}.resolve_llm_config", new=_resolve_stub),
    ):
        await trigger_compaction("thread-1", user_id="user-1")

    cfg = stub_resolve.captured_config
    assert cfg.llm.compaction == "user-compaction-model"
    model = _summary_model(cfg)
    # A copy, as every role gets, so nothing done to it reaches the shared
    # subsidiary client.
    oauth_client.model_copy.assert_called_once_with()
    assert model is oauth_client.model_copy.return_value, (
        "Manual /compact must summarize with a copy of the OAuth/BYOK "
        "subsidiary compaction client, not rebuild a bare system-auth client."
    )
    # thread_id must reach resolve_llm_config so prompt_cache_key binds to the
    # session shard when running on an OpenAI-family compaction model.
    assert resolve_kwargs.get("thread_id") == "thread-1"


@pytest.mark.asyncio
async def test_manual_compact_without_a_role_client_runs_the_turns_model():
    """With no role client, manual /compact builds the compaction model by
    name at the summary level, a blank compaction meaning the model the turn
    runs, as automatic compaction does. The server's own config holds no main
    client to borrow: only ``AgentConfig.create`` hands one in."""
    from ptc_agent.agent.middleware.compaction.compact import Summarizer
    from src.server.handlers.thread_maintenance import trigger_compaction

    base_config = _make_agent_config(compaction_model=None)
    base_config.llm_client = None
    base_config.subsidiary_llm_clients.pop("compaction", None)
    stub_resolve = _stub_thread_agent()

    with (
        patch("src.server.app.setup.agent_config", base_config),
        patch(f"{HANDLER}._thread_agent", new=stub_resolve),
        patch(ASK, new=_asked()),
    ):
        await trigger_compaction("thread-1")

    cfg = stub_resolve.captured_config
    model_config = MagicMock()
    model_config.get_reasoning_efforts.return_value = ["low", "medium", "high", "max"]
    with (
        patch(f"{COMPACT}.LLM.get_model_config", return_value=model_config),
        patch(f"{COMPACT}.create_llm", return_value=MagicMock(name="summary")) as build,
    ):
        Summarizer.for_agent(cfg)

    build.assert_called_once_with("system-default-model", reasoning_effort="medium")


@pytest.mark.asyncio
async def test_manual_compact_resets_the_token_cache_and_prunes_dead_offloads(base_config):
    """The cached counts measured the view before the summary: left in place,
    the next model call reads them as over the threshold and compacts again.
    Offload ids of calls the summary replaced name nothing the model sees."""
    from langchain_core.messages import AIMessage

    from src.server.handlers.thread_maintenance import trigger_compaction

    stub_resolve = _stub_thread_agent()
    stub_resolve.state.values = {
        "_summarization_event": None,
        "_cached_input_tokens": 150_000,
        "_cached_output_tokens": 2_000,
        "_offloaded_tool_call_ids": {"w-summarized", "w-kept"},
        "_offloaded_read_result_ids": {"r-summarized"},
    }
    kept = AIMessage("", id="a-kept", tool_calls=[{"name": "Write", "id": "w-kept", "args": {}}])
    compaction = _compaction(preserved=[kept])

    with (
        patch("src.server.app.setup.agent_config", base_config),
        patch(f"{HANDLER}._thread_agent", new=stub_resolve),
        patch(ASK, new=_asked(compaction)),
    ):
        await trigger_compaction("thread-1")

    update = stub_resolve.graph.aupdate_state.await_args.args[1]
    assert update["_summarization_event"] is compaction.event
    assert update["_cached_input_tokens"] == update["_cached_output_tokens"] == 0
    assert update["_offloaded_tool_call_ids"] == {"w-kept"}
    assert update["_offloaded_read_result_ids"] == set()


@pytest.mark.asyncio
async def test_manual_compact_writes_its_marker_naming_the_summary(base_config):
    """Replay finds the marker in the same checkpoint write that installed the
    summary, naming that summary so the turn does not also show it as a
    compaction of its own, and reads its text from it."""
    from src.server.handlers.thread_maintenance import trigger_compaction

    stub_resolve = _stub_thread_agent()

    with (
        patch("src.server.app.setup.agent_config", base_config),
        patch(f"{HANDLER}._thread_agent", new=stub_resolve),
        patch(ASK, new=_asked()),
    ):
        result = await trigger_compaction("thread-1")

    assert stub_resolve.graph.aupdate_state.await_count == 1
    update = stub_resolve.graph.aupdate_state.await_args.args[1]
    (marker,) = update["ui"]
    assert marker["name"] == "context_window"
    assert marker["id"].startswith("ui-")
    assert marker["metadata"] == {"summary_message_id": "s"}
    assert marker["props"] == {
        "action": "summarize",
        "signal": "complete",
        "original_message_count": 2,
        "new_message_count": result["new_message_count"],
        "summary_length": 2,
        "source": "model",
    }


READER_GET_INSTANCE = (
    "src.server.services.history.reader.CheckpointHistoryReader.get_instance"
)


def _turn_reader(summary_before=None, *, error=None):
    """The latest turn starts at ``cp-in``, holding ``summary_before``."""
    from types import SimpleNamespace

    from src.server.services.history.reader import TurnAnchor

    reader = MagicMock()
    reader.aget_turn_anchors = AsyncMock(
        side_effect=error,
        return_value=(
            [
                TurnAnchor(
                    turn_ordinal=0, input_checkpoint_id="cp-in", tail_checkpoint_id="cp-tail"
                )
            ],
            "cp-tail",
        ),
    )
    reader.aget_state = AsyncMock(
        return_value=SimpleNamespace(
            values={
                "_summarization_event": (
                    {"summary_message": summary_before, "cutoff_index": 1}
                    if summary_before is not None
                    else None
                )
            }
        )
    )
    return reader


async def _compact_after(base_config, values, reader):
    """Compact a thread whose commit pointer holds ``values``; the update."""
    from src.server.handlers.thread_maintenance import trigger_compaction

    stub_resolve = _stub_thread_agent()
    stub_resolve.state.values = values
    stub_resolve.state.config = {"configurable": {"checkpoint_id": "cp-tail"}}
    with (
        patch("src.server.app.setup.agent_config", base_config),
        patch(f"{HANDLER}._thread_agent", new=stub_resolve),
        patch(ASK, new=_asked()),
        patch(READER_GET_INSTANCE, return_value=reader),
    ):
        await trigger_compaction("thread-1")
    return stub_resolve.graph.aupdate_state.await_args.args[1]


def _agent_summary():
    from ptc_agent.agent.middleware.compaction.utils import build_summary_message

    return build_summary_message("the turn's own", None, original_message_count=30)


@pytest.mark.asyncio
async def test_a_compact_right_after_a_turn_keeps_the_summary_the_turn_wrote(base_config):
    """The compact's write becomes the end of the turn it follows, so the
    summary the agent wrote during that turn rides on the compact's marker."""
    own = _agent_summary()
    reader = _turn_reader(summary_before=None)

    update = await _compact_after(
        base_config,
        {"_summarization_event": {"summary_message": own, "cutoff_index": 7}},
        reader,
    )

    (marker,) = update["ui"]
    assert marker["metadata"]["summary_message_id"] == "s"
    assert marker["metadata"]["replaced_summary"] == {
        "id": own.id,
        "action": "summarize",
        "signal": "complete",
        "summary_length": len("the turn's own"),
        "summary_text": "the turn's own",
        "original_message_count": 30,
        "source": "model",
    }
    reader.aget_turn_anchors.assert_awaited_once_with("thread-1", "cp-tail")
    reader.aget_state.assert_awaited_once_with("thread-1", "cp-in")


@pytest.mark.asyncio
async def test_a_second_compact_keeps_the_summary_the_first_installed(base_config):
    """Two compacts after the same turn: the second's write ends the turn's
    span, so the first's summary rides on the second's marker."""
    from src.server.services.history.projector import context_window_record

    first = _agent_summary()
    values = {
        "_summarization_event": {"summary_message": first, "cutoff_index": 7},
        "ui": [context_window_record({"action": "summarize"}, summary_message_id=first.id)],
    }

    update = await _compact_after(base_config, values, _turn_reader(summary_before=None))

    (marker,) = update["ui"]
    assert marker["metadata"]["replaced_summary"]["id"] == first.id
    assert marker["metadata"]["replaced_summary"]["summary_text"] == "the turn's own"


@pytest.mark.asyncio
@pytest.mark.parametrize("written_by", ["an_earlier_turn", "unreadable"])
async def test_a_summary_from_before_the_turn_is_not_copied(base_config, written_by):
    summary = _agent_summary()
    values = {"_summarization_event": {"summary_message": summary, "cutoff_index": 7}}
    if written_by == "an_earlier_turn":
        reader = _turn_reader(summary_before=summary)
    else:
        reader = _turn_reader(error=RuntimeError("pool closed"))

    update = await _compact_after(base_config, values, reader)

    (marker,) = update["ui"]
    assert marker["metadata"] == {"summary_message_id": "s"}


@pytest.mark.asyncio
async def test_manual_offload_writes_its_marker_with_the_ids(base_config):
    from src.server.handlers.thread_maintenance import trigger_offload

    stub_resolve = _stub_thread_agent()
    stub_resolve.state.values = {}
    offload_mock = AsyncMock(return_value=({"w1", "w2"}, {"r1"}))

    with (
        patch("src.server.app.setup.agent_config", base_config),
        patch(f"{HANDLER}._thread_agent", new=stub_resolve),
        patch(
            "ptc_agent.agent.middleware.compaction.offload_tool_args",
            new=offload_mock,
        ),
    ):
        await trigger_offload("thread-1")

    assert stub_resolve.graph.aupdate_state.await_count == 1
    update = stub_resolve.graph.aupdate_state.await_args.args[1]
    assert update["_offloaded_tool_call_ids"] == {"w1", "w2"}
    (marker,) = update["ui"]
    assert marker["name"] == "context_window"
    assert marker["metadata"] == {}
    assert marker["props"] == {
        "action": "offload",
        "signal": "complete",
        "offloaded_args": 2,
        "offloaded_reads": 1,
    }

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

        ask_mock = AsyncMock()  # must NEVER run
        stub_resolve = _stub_thread_agent()

        with (
            patch("src.server.app.setup.agent_config", base_config),
            patch(f"{HANDLER}._thread_agent", new=stub_resolve),
            patch(ASK, new=ask_mock),
            patch(
                RUNNER_GET_INSTANCE,
                return_value=_fake_runner(refusal=self._conflict(code, "compact")),
            ),
        ):
            with pytest.raises(HTTPException) as exc_info:
                await trigger_compaction("thread-1")

        assert exc_info.value.status_code == 409
        detail = exc_info.value.detail
        assert isinstance(detail, dict)
        assert detail["code"] == code
        assert detail["verb"] == "compact"
        assert ask_mock.await_count == 0
        assert stub_resolve.captured_config is None

    @pytest.mark.asyncio
    async def test_offload_maps_runner_refusal_to_409(self, base_config):
        from fastapi import HTTPException

        from src.server.handlers.thread_maintenance import trigger_offload

        offload_mock = AsyncMock()  # must NEVER run
        stub_resolve = _stub_thread_agent()

        with (
            patch("src.server.app.setup.agent_config", base_config),
            patch(f"{HANDLER}._thread_agent", new=stub_resolve),
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

        ask_mock = AsyncMock()  # must NEVER run

        with (
            patch("src.server.app.setup.agent_config", base_config),
            patch(
                f"{HANDLER}._thread_agent",
                new=_stub_thread_agent(),
            ),
            patch(ASK, new=ask_mock),
            patch(
                RUNNER_GET_INSTANCE,
                return_value=_fake_runner(
                    refusal=MutationUnavailable("budget exhausted")
                ),
            ),
        ):
            with pytest.raises(HTTPException) as exc_info:
                await trigger_compaction("thread-1")

        assert exc_info.value.status_code == 503
        assert ask_mock.await_count == 0

    @pytest.mark.asyncio
    async def test_compact_holds_fence_and_threads_fence_saver(self, base_config):
        """The critical section runs inside the fence (held+released exactly
        once) and graph building receives the fence-bound saver, not the
        global pooled one."""
        from src.server.handlers.thread_maintenance import trigger_compaction

        fence_saver = object()
        runner = _fake_runner(saver=fence_saver)
        stub_resolve = _stub_thread_agent()
        ask_mock = AsyncMock(return_value=_compaction())

        with (
            patch("src.server.app.setup.agent_config", base_config),
            patch(f"{HANDLER}._thread_agent", new=stub_resolve),
            patch(ASK, new=ask_mock),
            patch(RUNNER_GET_INSTANCE, return_value=runner),
        ):
            await trigger_compaction("thread-1")

        assert runner.held == [("thread-1", "compact")]
        assert runner.released == [("thread-1", "compact")]
        assert stub_resolve.captured_checkpointer is fence_saver

    @pytest.mark.asyncio
    async def test_offload_threads_fence_saver(self, base_config):
        from src.server.handlers.thread_maintenance import trigger_offload

        fence_saver = object()
        runner = _fake_runner(saver=fence_saver)
        stub_resolve = _stub_thread_agent()
        offload_mock = AsyncMock(return_value=(set(), set()))

        with (
            patch("src.server.app.setup.agent_config", base_config),
            patch(f"{HANDLER}._thread_agent", new=stub_resolve),
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
        stub_resolve = _stub_thread_agent()
        offload_mock = AsyncMock(return_value=(set(), set()))

        with (
            patch("src.server.app.setup.agent_config", base_config),
            patch(f"{HANDLER}._thread_agent", new=stub_resolve),
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
        ask_mock = AsyncMock(side_effect=ConnectionError("boom"))

        with (
            patch("src.server.app.setup.agent_config", base_config),
            patch(
                f"{HANDLER}._thread_agent",
                new=_stub_thread_agent(),
            ),
            patch(ASK, new=ask_mock),
            patch(RUNNER_GET_INSTANCE, return_value=runner),
        ):
            with pytest.raises(HTTPException) as exc_info:
                await trigger_compaction("thread-1")

        assert exc_info.value.status_code == 500
        assert runner.released == [("thread-1", "compact")]

    @pytest.mark.asyncio
    async def test_compact_cancelled_surfaces_clean_http(self, base_config):
        """A user Stop (/cancel → runner.request_stop) cancels this request
        task, often mid model call. ``CancelledError`` is a
        BaseException, so without handling it bubbles to ASGI as a raw 500 —
        the shared ``cancellation_as_http`` wrapper converts it to a clean
        409 ``request_cancelled``; the fence must still be released."""
        import asyncio

        from fastapi import HTTPException

        from src.server.handlers.thread_maintenance import trigger_compaction

        runner = _fake_runner()
        ask_mock = AsyncMock(side_effect=asyncio.CancelledError())

        with (
            patch("src.server.app.setup.agent_config", base_config),
            patch(
                f"{HANDLER}._thread_agent",
                new=_stub_thread_agent(),
            ),
            patch(ASK, new=ask_mock),
            patch(RUNNER_GET_INSTANCE, return_value=runner),
        ):
            with pytest.raises(HTTPException) as exc_info:
                await trigger_compaction("thread-1")

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
                f"{HANDLER}._thread_agent",
                new=_stub_thread_agent(),
            ),
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
        from src.server.handlers.thread_maintenance import _thread_agent

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
            patch(
                "src.server.handlers.thread_maintenance.turn_base_checkpoint_id",
                new=AsyncMock(return_value=None),
            ),
        ):
            await _thread_agent(
                "thread-1",
                "compact",
                config=base_config,
                checkpointer=MagicMock(),
                user_id="user-1",
                held=AsyncExitStack(),
            )

        assert manager.get_session_for_workspace.await_args.kwargs["user_id"] == "user-1"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("binds", [True, False], ids=["bound", "unbound"])
    async def test_the_agent_is_built_as_the_threads_next_turn(self, base_config, binds):
        """A model call between turns sends the turn's prompt and tools, or
        the provider's cache misses: the caller's profile with its trading
        rule, the direct tools (none when they cannot be bound, as a turn
        runs without them), the store, the project and the role."""
        from src.server.app import setup
        from src.server.handlers.thread_maintenance import _thread_agent
        from src.server.services.egress.direct_tools import DirectMCPBinding
        from src.server.services.workspace_manager import WorkspaceManager

        session = MagicMock(sandbox=None)
        manager = MagicMock(spec=WorkspaceManager)
        manager.get_session_for_workspace = AsyncMock(return_value=session)
        view = manager.tool_view.return_value
        graph = MagicMock()
        graph.aget_state = AsyncMock(
            return_value=MagicMock(values={"messages": [MagicMock(id="m1")]})
        )
        direct = [MagicMock(name="direct-tool")]
        bind = AsyncMock(
            return_value=direct if binds else None,
            side_effect=None if binds else RuntimeError("relay down"),
        )

        with (
            patch(
                "src.server.database.conversation.get_thread_with_summary",
                new=AsyncMock(return_value={"workspace_id": "ws-1"}),
            ),
            patch(
                "src.server.services.workspace_manager.WorkspaceManager.get_instance",
                return_value=manager,
            ),
            patch(
                "ptc_agent.agent.graph.build_ptc_graph_with_session",
                new=AsyncMock(return_value=graph),
            ) as build,
            patch(f"{HANDLER}.turn_base_checkpoint_id", new=AsyncMock(return_value=None)),
            patch(
                "src.server.database.user.get_user_profile_for_prompt",
                new=AsyncMock(return_value={"name": "profile"}),
            ),
            patch(
                "src.server.services.trading_rule.with_trading_rule",
                side_effect=lambda profile, rule: {**profile, "rule": rule},
            ),
            patch("src.server.services.egress.direct_tools.bind_direct_mcp_tools", new=bind),
        ):
            agent = await _thread_agent(
                "thread-1",
                "compact",
                config=base_config,
                checkpointer=MagicMock(),
                user_id="user-1",
                held=AsyncExitStack(),
                role="chief_of_staff",
            )

        built = build.await_args.kwargs
        assert built["tool_view"] is view
        assert built["config"] is base_config
        assert built["user_id"] == "user-1"
        assert built["user_profile"] == {"name": "profile", "rule": view.trading_rule}
        assert built["thread_id"] == "thread-1"
        assert built["store"] is setup.store
        # A relay that is down costs the turn its direct tools, not the turn.
        assert built["direct_mcp"] == (direct if binds else DirectMCPBinding(user_id="user-1"))
        assert built["role"] == "chief_of_staff"
        # A session with no sandbox has no folder to bind.
        assert built["project"] is None and agent.project is None
        bind.assert_awaited_once_with(session, user_id="user-1", workspace_id="ws-1", view=view)


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
                ("src.server.handlers.thread_maintenance.turn_base_checkpoint_id",
                 AsyncMock(return_value=None)),
                ("src.server.database.workspace_folders.workspace_folder_in_use", hold),
                ("src.server.database.workspace.get_workspace", read_row),
                # The turn's project is its own read (see resolve_project_context).
                ("src.server.services.workspace_layout.resolve_project_context",
                 AsyncMock(return_value=PROJECT)),
            ):
                stack.enter_context(patch(target, new))
            yield

    @pytest.mark.asyncio
    async def test_the_backend_writes_to_the_folder_read_under_the_hold(self, base_config):
        from src.server.handlers.thread_maintenance import _thread_agent

        events = []
        with self._resolving(events):
            async with AsyncExitStack() as held:
                agent = await _thread_agent(
                    "thread-1", "offload", config=base_config, checkpointer=MagicMock(), held=held
                )
                events.append("offload")

        assert events == ["hold ws-1", "read", "offload", "release"]
        assert agent.backend.root_dir == "/home/workspace/Macro"
        assert agent.project is PROJECT

    @pytest.mark.asyncio
    @pytest.mark.parametrize("moving", ["staged", "held by a settle"])
    async def test_a_folder_a_settle_is_moving_is_refused_for_a_retry(self, base_config, moving):
        from src.server.handlers.thread_maintenance import _thread_agent

        events = []
        resolving = self._resolving(
            events,
            dir_name="_internal/moving/ws-1" if moving == "staged" else "Macro",
            held_by_a_settle=moving == "held by a settle",
        )
        with resolving, pytest.raises(HTTPException) as refused:
            async with AsyncExitStack() as held:
                await _thread_agent(
                    "thread-1", "offload", config=base_config, checkpointer=MagicMock(), held=held
                )

        assert refused.value.status_code == 503
        assert events == (
            ["hold ws-1", "read", "release"] if moving == "staged" else ["hold ws-1"]
        )


@pytest.mark.asyncio
async def test_the_session_is_acquired_with_the_workspace_id_as_a_str(base_config):
    """A cold-start /compact failed in asset sync, which encodes the id."""
    import uuid

    from src.server.handlers.thread_maintenance import _thread_agent

    workspace_id = uuid.uuid4()
    received = []

    async def get_session_for_workspace(ws, user_id=None):
        received.append(ws)
        raise ValueError("stop")

    manager = MagicMock()
    manager.get_session_for_workspace = get_session_for_workspace

    with (
        patch(
            "src.server.database.conversation.get_thread_with_summary",
            new=AsyncMock(return_value={"workspace_id": workspace_id}),
        ),
        patch(
            "src.server.services.workspace_manager.WorkspaceManager.get_instance",
            return_value=manager,
        ),
        pytest.raises(HTTPException),
    ):
        async with AsyncExitStack() as held:
            await _thread_agent(
                "thread-1", "compact", config=base_config, held=held
            )

    assert received == [str(workspace_id)]
    assert isinstance(received[0], str)

def _claude_code_compaction():
    """What the CLI compacting a session of two answers, one in two records,
    writes: a summary, keeping the last tool result and answer."""
    from ptc_agent.agent.claude_code.compaction import SessionCompaction

    entries = [
        {"type": "user", "uuid": "u1"},
        {"type": "assistant", "uuid": "a1", "message": {"id": "m1"}},
        {"type": "assistant", "uuid": "a2", "message": {"id": "m1"}},
        {"type": "user", "uuid": "u2"},
        {"type": "user", "uuid": "u3", "isMeta": True},
        {"type": "assistant", "uuid": "a3", "message": {"id": "m2"}},
    ]
    boundary = {"type": "system", "subtype": "compact_boundary", "uuid": "b",
                "compactMetadata": {"preservedMessages": {"uuids": ["u2", "a3"]}}}
    summary = {"type": "user", "uuid": "s", "isCompactSummary": True, "message": {"content": "the summary"}}
    return SessionCompaction(entries, [{"session_id": "sess", "entries": [boundary, summary]}])


def _claude_code_config(base_config):
    from src.llms.llm import create_llm

    config = base_config.model_copy(deep=True)
    config.llm_client = create_llm("claude-sonnet-5-5-code")
    return config


async def _compact_claude_code_thread(base_config, held_model, *, compaction=None, resolve=None, claude_replied=True,
                                     waiting=False):
    """Compact a thread holding ``held_model`` that resolves to a Claude
    Code row; the stub agent, the ask, the resolve and the outcome. A
    ``waiting`` thread holds a question Claude Code deferred, unanswered."""
    from src.server.handlers.thread_maintenance import trigger_compaction

    from langchain_core.messages import AIMessage, HumanMessage

    stub = _stub_thread_agent()
    calls = [{"name": "AskUserQuestion", "args": {}, "id": "t1", "type": "tool_call"}] if waiting else []
    stub.messages[:] = [HumanMessage("hi"), AIMessage("hello", tool_calls=calls,
                                                      response_metadata={"claude_message_id": "msg_1"} if claude_replied else {})]
    deferral = {"uuid": "d1", "type": "attachment", "attachment": {"type": "hook_deferred_tool", "toolUseID": "t1"}}
    stub.state.values["claude_session"] = {"session_id": "sess", "entries": [deferral]}
    stub.graph.turn.compact_between_turns = (
        AsyncMock(side_effect=compaction) if isinstance(compaction, BaseException) or callable(compaction)
        else AsyncMock(return_value=compaction)
    )
    ask = _asked()
    resolve = resolve or AsyncMock(return_value=_claude_code_config(base_config))
    with (
        patch("src.server.app.setup.agent_config", base_config),
        patch(f"{HANDLER}._thread_agent", new=stub),
        patch(ASK, new=ask),
        patch(THREAD_META, new_callable=AsyncMock, return_value={"llm_model": held_model, "msg_type": "ptc", "workspace_id": "ws-1"}),
        patch(f"{HANDLER}._resolver", new_callable=AsyncMock),
        patch(f"{THREAD_MODEL}.resolve_turn_config", new=resolve),
    ):
        try:
            outcome = await trigger_compaction("thread-1", user_id="user-1")
        except HTTPException as e:
            outcome = e
    return stub, ask, resolve, outcome


@pytest.mark.asyncio
@pytest.mark.parametrize("held_model", ["claude-sonnet-5-5-code", None])
async def test_a_claude_code_thread_is_compacted_by_claude_code(base_config, held_model):
    """The CLI compacts its own session, which the write installs beside the
    thread's messages, and the marker shows the compact in replay. A thread
    holding no model runs the account default, which may be a Claude Code row."""
    stub, ask, resolve, outcome = await _compact_claude_code_thread(
        base_config, held_model, compaction=_claude_code_compaction()
    )
    ask.assert_not_awaited()
    assert resolve.await_args.kwargs["fall_back"] is (held_model is None)
    assert {k: outcome[k] for k in ("original_message_count", "new_message_count", "summary_text", "source")} == {
        "original_message_count": 4, "new_message_count": 3, "summary_text": "the summary", "source": "provider",
    }
    written = stub.graph.aupdate_state.await_args.args[1]
    assert written["claude_session"] == _claude_code_compaction().writes
    assert [r["props"]["original_message_count"] for r in written["ui"]] == [4]
    assert "messages" not in written


@pytest.mark.asyncio
async def test_a_claude_code_compactions_summary_request_lands_in_the_usage_records(base_config, usage_rows):
    """On the compact's row, as an API thread's summary calls do."""
    from claude_agent_sdk import ResultMessage

    from ptc_agent.agent.claude_code import compaction as compaction_module
    from ptc_agent.agent.claude_code.cli import RuntimeSpec

    async def compacted(state):
        result = ResultMessage(subtype="success", duration_ms=1, duration_api_ms=1, is_error=False, num_turns=0,
                               session_id="s", model_usage={"claude-test": {"inputTokens": 200, "outputTokens": 300,
                                                                            "cacheReadInputTokens": 1000,
                                                                            "cacheCreationInputTokens": 0}})
        spec = RuntimeSpec(system_prompt="s", model="claude-test", tools=[], billing={"billing_type": "oauth"})
        await compaction_module._metered(spec, result)
        return _claude_code_compaction()

    with patch("src.server.database.api_keys.is_byok_active", new=AsyncMock(return_value=False)):
        _, _, _, outcome = await _compact_claude_code_thread(base_config, "claude-sonnet-5-5-code", compaction=compacted)
    assert outcome["summary_text"] == "the summary"
    (row,) = usage_rows
    assert (row["status"], row["is_byok"], row["token_usage"]["between_turns"]) == ("completed", True, "compact")
    tokens = row["token_usage"]["by_model"]["claude-test"]
    assert (tokens["input_tokens"], tokens["cached_tokens"], tokens["output_tokens"]) == (1200, 1000, 300)


@pytest.mark.asyncio
async def test_a_claude_code_session_with_nothing_to_summarize_is_left_as_it_is(base_config):
    from ptc_agent.agent.middleware.compaction.compact import NOTHING_TO_COMPACT

    stub, _, _, outcome = await _compact_claude_code_thread(base_config, "claude-sonnet-5-5-code")
    assert outcome.status_code == 400 and outcome.detail == NOTHING_TO_COMPACT
    stub.graph.aupdate_state.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_claude_code_thread_whose_model_does_not_resolve_is_not_compacted_by_another(base_config):
    """Only Claude Code compacts a Claude Code session: a failed resolve does
    not fall back to the base config, whose model would rewrite the record."""
    resolve = AsyncMock(side_effect=RuntimeError("db blip"))
    stub, ask, _, outcome = await _compact_claude_code_thread(base_config, "claude-sonnet-5-5-code", resolve=resolve)
    assert outcome.status_code == 500
    ask.assert_not_awaited()
    stub.graph.turn.compact_between_turns.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(("kind", "status"), [("plan_exhausted", 429), ("api_error", 502), ("startup_failed", 502)])
async def test_a_compaction_the_cli_fails_answers_with_the_status_its_failure_carried(base_config, kind, status):
    """Not "nothing to compact", and not this server's own failure: the
    status a turn that failed the same way streams."""
    from src.llms.claude_code import ClaudeTurnFailed

    failure = ClaudeTurnFailed(kind, "API Error: 429 rate limited")
    _, _, _, outcome = await _compact_claude_code_thread(base_config, "claude-sonnet-5-5-code", compaction=failure)
    assert isinstance(outcome, HTTPException)
    assert outcome.status_code == status and outcome.detail == f"Failed to trigger compaction: {failure}"


@pytest.mark.asyncio
async def test_a_compaction_failure_is_answered_without_the_credentials_its_text_carried(base_config):
    from src.llms.claude_code import ClaudeTurnFailed

    failure = ClaudeTurnFailed("api_error", "the provider said Authorization: Bearer abcdefgh12345 for sk-abcdefgh12345")
    _, _, _, outcome = await _compact_claude_code_thread(base_config, "claude-sonnet-5-5-code", compaction=failure)
    assert outcome.status_code == 502 and outcome.detail.startswith("Failed to trigger compaction: api_error: the provider said")
    assert "abcdefgh12345" not in outcome.detail and "[REDACTED]" in outcome.detail


@pytest.mark.asyncio
async def test_a_compaction_whose_login_anthropic_refused_is_refused_as_admission_refuses_the_turn(base_config):
    from src.llms.claude_code import ClaudeTurnFailed

    _, _, _, outcome = await _compact_claude_code_thread(
        base_config, "claude-sonnet-5-5-code", compaction=ClaudeTurnFailed("auth_failed", "401 invalid token")
    )
    assert outcome.status_code == 400
    assert {k: outcome.detail[k] for k in ("type", "reason")} == {"type": "claude_code_off", "reason": "signed_out"}


@pytest.mark.asyncio
async def test_a_thread_another_model_last_replied_on_is_not_compacted_by_claude_code(base_config):
    """A held model that cannot run falls back to the account default, which
    may be Claude Code: its session, if any, is not the one the thread runs on."""
    stub, ask, _, outcome = await _compact_claude_code_thread(
        base_config, "an-api-model", compaction=_claude_code_compaction(), claude_replied=False
    )
    assert outcome.detail["type"] == "claude_code_compacts_itself"
    ask.assert_not_awaited()
    stub.graph.turn.compact_between_turns.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_thread_claude_code_last_replied_on_is_not_summarized_by_another_model(base_config):
    """A thread following a Claude Code default holds no model of its own: a
    failed resolve falls back to the base config, whose summary would be
    written through a graph that drops Claude's session."""
    from langchain_core.messages import AIMessage, HumanMessage

    from src.server.handlers.thread_maintenance import trigger_compaction

    stub = _stub_thread_agent()
    stub.messages[:] = [HumanMessage("hi"), AIMessage("hello", response_metadata={"claude_message_id": "msg_1"})]
    ask = _asked()
    with (
        patch("src.server.app.setup.agent_config", base_config),
        patch(f"{HANDLER}._thread_agent", new=stub),
        patch(ASK, new=ask),
        patch(THREAD_META, new_callable=AsyncMock, return_value={"llm_model": None, "msg_type": "ptc", "workspace_id": "ws-1"}),
        patch(f"{HANDLER}._resolver", new_callable=AsyncMock),
        patch(f"{THREAD_MODEL}.resolve_turn_config", new=AsyncMock(side_effect=RuntimeError("db blip"))),
        pytest.raises(HTTPException) as refused,
    ):
        await trigger_compaction("thread-1", user_id="user-1")
    assert refused.value.detail["type"] == "claude_code_compacts_itself"
    ask.assert_not_awaited()
    stub.graph.aupdate_state.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_claude_code_thread_is_not_offloaded(base_config):
    from src.server.handlers.thread_maintenance import trigger_offload

    stub = _stub_thread_agent()
    with (
        patch("src.server.app.setup.agent_config", base_config),
        patch(f"{HANDLER}._thread_agent", new=stub),
        patch(THREAD_META, new_callable=AsyncMock, return_value={"llm_model": "claude-haiku-5-5-code"}),
        pytest.raises(HTTPException) as refused,
    ):
        await trigger_offload("thread-1")
    assert refused.value.detail["type"] == "claude_code_compacts_itself"
    stub.graph.aupdate_state.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_thread_claude_code_last_replied_on_is_not_offloaded(base_config):
    """A thread following a Claude Code default holds no model of its own, so
    Claude's last reply is what marks it."""
    from langchain_core.messages import AIMessage, HumanMessage

    from src.server.handlers.thread_maintenance import trigger_offload

    stub = _stub_thread_agent()
    stub.messages[:] = [HumanMessage("hi"), AIMessage("hello", response_metadata={"claude_message_id": "msg_1"})]
    stub.state.values = {}
    with (
        patch("src.server.app.setup.agent_config", base_config),
        patch(f"{HANDLER}._thread_agent", new=stub),
        patch(THREAD_META, new_callable=AsyncMock, return_value={"llm_model": None}),
        pytest.raises(HTTPException) as refused,
    ):
        await trigger_offload("thread-1")
    assert refused.value.detail["type"] == "claude_code_compacts_itself"
    stub.graph.aupdate_state.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_compaction_between_turns_reads_when_the_previous_run_settled():
    """A run cancelled mid-call refreshed the provider's cache without a stamp
    in the checkpoint, so the build's cache clock reads the ledger, as a
    turn's does; a failed read leaves it unknown."""
    from datetime import UTC, datetime

    from src.server.handlers.thread_maintenance import _previous_run

    settled = datetime(2026, 3, 1, 14, 31, tzinfo=UTC)
    latest = "src.server.database.runs.lifecycle.get_latest_attempt"
    with patch(latest, new=AsyncMock(return_value={"status": "cancelled", "usage_settled_at": settled})):
        assert (await _previous_run("t-1")).previous_run_settled_at == settled
    with patch(latest, new=AsyncMock(side_effect=RuntimeError("pool down"))):
        assert (await _previous_run("t-1")).previous_run_settled_at is None


async def _compact_by_the_user(base_config, ask, summarizer=None):
    """Manual /compact by user-1 on a thread resolving to ``base_config``,
    ``ask`` and ``summarizer`` standing in for the agent's model chain and
    the summarizer alone."""
    from src.server.handlers.thread_maintenance import trigger_compaction

    with (
        patch("src.server.app.setup.agent_config", base_config),
        patch(f"{HANDLER}._thread_agent", new=_stub_thread_agent()),
        patch(ASK, new=ask),
        patch(
            COMPACT_MESSAGES,
            new=summarizer or AsyncMock(side_effect=AssertionError("the summarizer alone ran")),
        ),
        patch("src.server.database.api_keys.is_byok_active", new=AsyncMock(return_value=False)),
        patch(f"{THREAD_MODEL}.resolve_turn_config", new=AsyncMock(return_value=base_config)),
    ):
        return await trigger_compaction("thread-1", user_id="user-1")


def _spending(spend, answer=None, *, billing_type="platform", error=None):
    """A stand-in for the agent's model chain or the summarizer that calls the
    model once, then answers ``answer`` (a fresh compaction by default) or
    raises ``error``."""

    async def _call(*_args, **_kwargs):
        await spend(billing_type)
        if error is not None:
            raise error
        return answer if answer is not None else _compaction()

    return AsyncMock(side_effect=_call)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("billing_type", "is_byok"), [("platform", False), ("byok", True), ("oauth", True)]
)
async def test_the_summary_calls_land_in_the_usage_records_as_a_turns_do(
    base_config, usage_rows, spend, billing_type, is_byok
):
    """On a row of their own: no run is open between turns to hold them. It is
    the thread's, in its workspace, under the caller, split by whose key ran
    the calls as a turn's row is, and says it is the compact's."""
    from uuid import UUID

    await _compact_by_the_user(base_config, _spending(spend, billing_type=billing_type))

    (row,) = usage_rows
    assert (row["user_id"], row["conversation_thread_id"], row["workspace_id"]) == (
        "user-1",
        "thread-1",
        "ws-1",
    )
    assert UUID(row["conversation_response_id"])
    assert (row["msg_type"], row["status"], row["is_byok"]) == ("ptc", "completed", is_byok)
    usage = row["token_usage"]
    assert usage["between_turns"] == "compact"
    tokens = usage["by_model"]["claude-test"]
    assert (tokens["input_tokens"], tokens["cached_tokens"], tokens["output_tokens"]) == (
        1200,
        1000,
        300,
    )
    (call,) = usage["per_call_costs"]
    assert call["billing_type"] == billing_type


@pytest.mark.asyncio
async def test_the_callers_own_callbacks_still_see_the_summary_calls(base_config, usage_rows, spend):
    """The tracker joins the callbacks of the runnable config around it, so a
    caller tracing the operation still sees its calls."""
    from langchain_core.callbacks import AsyncCallbackHandler
    from langchain_core.runnables.config import var_child_runnable_config

    class _Observer(AsyncCallbackHandler):
        ended = 0

        async def on_llm_end(self, *_args, **_kwargs) -> None:
            self.ended += 1

    observer = _Observer()
    token = var_child_runnable_config.set({"callbacks": [observer]})
    try:
        await _compact_by_the_user(base_config, _spending(spend))
    finally:
        var_child_runnable_config.reset(token)

    assert observer.ended == 1
    (row,) = usage_rows
    assert row["token_usage"]["by_model"]["claude-test"]["output_tokens"] == 300


@pytest.mark.asyncio
async def test_a_row_that_fails_to_write_is_reported_lost(base_config, usage_rows, spend, caplog):
    """The writer reports a failed insert by returning False, and the calls
    have no other place to land, so the loss is logged rather than passed
    over as a success."""
    from src.server.services.persistence.usage import UsagePersistenceService

    with patch.object(UsagePersistenceService, "persist_usage", AsyncMock(return_value=False)):
        await _compact_by_the_user(base_config, _spending(spend))

    assert "compact row not written for thread_id=thread-1; 1 record(s) are unrecoverable" in caplog.text


@pytest.mark.asyncio
async def test_the_summarizer_alone_is_recorded_too(base_config, usage_rows, spend):
    """An agent that compacts nothing leaves the summary to the summarizer,
    whose calls are the compact's as much as the agent's would be."""
    await _compact_by_the_user(
        base_config,
        _asked(error=NoCompaction("the agent has no compaction to ask")),
        summarizer=_spending(spend),
    )

    (row,) = usage_rows
    assert row["token_usage"]["between_turns"] == "compact"
    assert row["token_usage"]["by_model"]["claude-test"]["output_tokens"] == 300


@pytest.mark.asyncio
async def test_a_compact_that_fails_after_calling_the_model_records_the_calls(
    base_config, usage_rows, spend
):
    with pytest.raises(HTTPException) as caught:
        await _compact_by_the_user(
            base_config, _spending(spend, error=RuntimeError("provider down"))
        )

    assert caught.value.status_code == 500
    (row,) = usage_rows
    assert row["status"] == "error"
    assert row["token_usage"]["by_model"]["claude-test"]["input_tokens"] == 1200


@pytest.mark.asyncio
async def test_a_summary_that_fails_to_install_is_recorded_as_an_error(
    base_config, usage_rows, spend
):
    """The row is written once the compact has ended, so its status is the
    compact's, not only its model call's."""
    with (
        patch(f"{HANDLER}._update_graph_state", new=AsyncMock(side_effect=RuntimeError("lock lost"))),
        pytest.raises(HTTPException) as caught,
    ):
        await _compact_by_the_user(base_config, _spending(spend))

    assert caught.value.status_code == 500
    (row,) = usage_rows
    assert row["status"] == "error"


@pytest.mark.asyncio
async def test_a_row_that_fails_to_price_keeps_the_calls_billing_type(
    base_config, usage_rows, spend
):
    with patch(
        "src.utils.tracking.core.calculate_cost_from_per_call_records",
        side_effect=RuntimeError("no rate card"),
    ):
        await _compact_by_the_user(base_config, _spending(spend, billing_type="byok"))

    (row,) = usage_rows
    assert row["is_byok"] is True


@pytest.mark.asyncio
async def test_a_stop_as_the_summary_arrives_records_the_call(base_config, usage_rows, spend):
    """The call's end is reported in a shielded callback the Stop does not
    wait for, so the record has to land before the row is written, however
    long the tracker takes over it."""
    import time

    from src.utils.tracking.per_call_token_tracker import PerCallTokenTracker

    on_llm_end = PerCallTokenTracker.on_llm_end

    def _slow_end(self, *args, **kwargs):
        time.sleep(0.2)
        return on_llm_end(self, *args, **kwargs)

    async def _stopped(*_args, **_kwargs):
        await spend(stopped=True)

    with (
        patch.object(PerCallTokenTracker, "on_llm_end", _slow_end),
        pytest.raises(HTTPException) as caught,
    ):
        await _compact_by_the_user(base_config, AsyncMock(side_effect=_stopped))

    assert caught.value.status_code == 409
    (row,) = usage_rows
    assert row["status"] == "cancelled"
    assert row["token_usage"]["by_model"]["claude-test"]["output_tokens"] == 300


@pytest.mark.asyncio
async def test_a_compact_that_calls_no_model_records_nothing(base_config, usage_rows):
    with pytest.raises(HTTPException) as caught:
        await _compact_by_the_user(
            base_config, _asked(error=ValueError("Cannot determine valid cutoff point"))
        )

    assert caught.value.status_code == 400
    assert usage_rows == []


@pytest.mark.asyncio
async def test_a_claude_code_thread_waiting_on_the_user_is_refused_before_the_cli_compacts(base_config):
    """The compaction's CLI would load the session with the call pending and
    nothing to run it, and run until the compaction budget."""
    stub, ask, _, outcome = await _compact_claude_code_thread(
        base_config, "claude-sonnet-5-5-code", compaction=_claude_code_compaction(), waiting=True
    )
    assert outcome.status_code == 409
    assert {k: outcome.detail[k] for k in ("code", "verb")} == {"code": "awaiting_input", "verb": "compact"}
    stub.graph.turn.compact_between_turns.assert_not_awaited()
    stub.graph.aupdate_state.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_api_thread_waiting_on_the_user_is_compacted_as_before(base_config):
    from langchain_core.messages import AIMessage, HumanMessage

    from src.server.handlers.thread_maintenance import trigger_compaction

    stub = _stub_thread_agent()
    stub.messages[:] = [HumanMessage("hi"), AIMessage("", tool_calls=[{"name": "ask_user", "args": {}, "id": "t1", "type": "tool_call"}])]
    ask = _asked()
    with (
        patch("src.server.app.setup.agent_config", base_config),
        patch(f"{HANDLER}._thread_agent", new=stub),
        patch(ASK, new=ask),
    ):
        result = await trigger_compaction("thread-1")
    ask.assert_awaited_once()
    assert result["summary_text"] == "ok"
