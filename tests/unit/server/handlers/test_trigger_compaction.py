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
