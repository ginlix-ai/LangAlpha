"""A deterministic thread run through the main agent's context middleware, with
the window trimming its checkpoint or not, for the suites that compare the two.

The graph is langchain's ``create_agent`` over the middleware that read or
write the history the model is sent, in the main stack's order: the subagent
switch, compaction (summaries, Tier 1 cuts, the window), tool-call patching,
the turn and baseline rows, the notes reminders and the tail envelope. The
model and the summary model are fakes that record every request; the
transcript store is ``build_directory`` kept in memory, saved live by
compaction and again at each turn end from the checkpoint, as the server's
export does. Everything an id or a clock could vary is fixed or normalized,
so two runs of the same scenario compare as strings.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

from deepagents.middleware.patch_tool_calls import PatchToolCallsMiddleware
from langchain.agents import create_agent
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import AIMessage, AnyMessage, HumanMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langchain_core.tools import tool
from langgraph.checkpoint.serde.types import _DeltaSnapshot
from langgraph.types import Overwrite
from langsmith import tracing_context
from pydantic import Field

from ptc_agent.agent.context_stack import build_context_middleware
from ptc_agent.agent.main_state import MainAgentState
from ptc_agent.agent.middleware.runtime_context import durable
from ptc_agent.agent.middleware.compaction import notes as notes_module
from ptc_agent.agent.middleware.compaction.compact import Summarizer
from ptc_agent.agent.middleware.compaction.middleware import CompactionMiddleware
from ptc_agent.agent.middleware.compaction.notes import (
    NotesDueMiddleware,
    ThreadScratchpad,
    notes_window_carry,
    thread_notes_subdir,
)
from ptc_agent.agent.middleware.compaction.types import OffloadSettings
from ptc_agent.agent.middleware.subagent_switch import (
    SubagentSwitchMiddleware,
    subagents_window_carry,
)
from ptc_agent.agent.transcript import TranscriptTarget, Window, build_directory, load_manifest
from ptc_agent.agent.transcript.render import split_runs

THREAD = "abcd1234-5678-4000-8000-00000000c0de"
WORKSPACE = "/home/workspace"
NOTES_SUBDIR = thread_notes_subdir(THREAD)
NOTES_DIR = f"{WORKSPACE}/{NOTES_SUBDIR}"

#: Messages in view, the system prompt counted, at which a summary runs.
THRESHOLD = 26
KEEP = 6
#: Tool calls since the notes were last written at which a check-in is due.
CHECK_IN_CALLS = 4

_START = datetime(2026, 9, 1, 9, 0, tzinfo=UTC)


class _Clock(datetime):
    """The wall clock rows are stamped from, stopped at the turn's start."""

    at = _START

    @classmethod
    def now(cls, tz=None):
        return cls.at


@dataclass(frozen=True)
class Turn:
    """One turn of the scenario."""

    #: Tool rounds before the answer.
    rounds: int = 1
    #: The subagent switch as the turn reads it.
    subagents: bool = True
    #: Whether the build reads the switch at all; a notification turn's
    #: does not.
    switch: bool = True
    #: Whether the first round writes the notes.
    notes: bool = False
    #: Tool rounds an orchestrator re-entry runs after the turn, 0 for none.
    reentry: int = 0


#: Long enough for several summaries and trims, with a flip of the subagent
#: switch and a notes write the trims drop, and a long turn that compacts
#: mid-turn before a background task's report re-enters it.
SCENARIO: tuple[Turn, ...] = (
    Turn(),
    Turn(notes=True),
    Turn(subagents=False),
    Turn(subagents=False),
    Turn(),
    Turn(rounds=2),
    Turn(),
    Turn(rounds=6, reentry=2),
    Turn(),
    Turn(rounds=2),
    Turn(),
    Turn(),
)

#: The scenario with the two turns whose trims drop the subagent notices
#: built without the switch.
NOTIFIED: tuple[Turn, ...] = tuple(
    replace(turn, switch=False) if number in (7, 8) else turn
    for number, turn in enumerate(SCENARIO, start=1)
)


class RecordingModel(BaseChatModel):
    """Answers from a script and records every request it is sent."""

    script: list[AIMessage] = Field(default_factory=list)
    requests: list[list[AnyMessage]] = Field(default_factory=list)

    @property
    def _llm_type(self) -> str:
        return "recording"

    def bind_tools(self, tools: Any, **kwargs: Any) -> RecordingModel:
        return self

    def _generate(self, messages, stop=None, run_manager=None, **kwargs) -> ChatResult:
        self.requests.append(list(messages))
        if not self.script:
            raise AssertionError("the model was called more often than scripted")
        return ChatResult(generations=[ChatGeneration(message=self.script.pop(0))])


class SummaryModel(BaseChatModel):
    """Writes numbered summaries and records what it is asked."""

    requests: list[list[AnyMessage]] = Field(default_factory=list)

    @property
    def _llm_type(self) -> str:
        return "summary"

    def _generate(self, messages, stop=None, run_manager=None, **kwargs) -> ChatResult:
        self.requests.append(list(messages))
        text = f"Summary {len(self.requests)}: the user asked for quotes and files."
        return ChatResult(generations=[ChatGeneration(message=AIMessage(text))])


class TranscriptStore:
    """The thread's stored transcript, as the server's store keeps it: each
    save renders against the manifest the last one left."""

    def __init__(self) -> None:
        self.manifest: str | None = None
        self.files: dict[str, bytes] = {}
        self.saves: list[str] = []

    async def save_transcript(self, target, messages, *, window: Window) -> bool:
        directory = build_directory(
            list(messages),
            unit=target.unit,
            previous=load_manifest(self.manifest),
            window=window,
        )
        self.manifest = directory.manifest
        self.files.update(directory.rendered)
        self.saves.append(directory.manifest)
        return True


def _backend(store: TranscriptStore) -> Any:
    async def settled_livefs(workspace_id=None):
        return store

    async def aglob(pattern, path=None):
        return SimpleNamespace(error=None, matches=[{"path": f"{NOTES_DIR}/plan.md"}])

    return SimpleNamespace(
        livefs=store,
        settled_livefs=settled_livefs,
        aglob=aglob,
        normalize_path=lambda path: path,
    )


@tool("Write")
def write_file(file_path: str, content: str) -> str:
    """Write a file."""
    return f"wrote {file_path}"


@tool("fetch_quote")
def fetch_quote(symbol: str) -> str:
    """A quote."""
    return f"{symbol} last 101.25, volume 2.1M, range 99.80-102.40"


def _round(turn: int, round_: str, *, notes: bool) -> list[AIMessage]:
    """A file write, then a quote: one call per message, so the tool results
    land in one order whichever tool finishes first."""
    tag = f"t{turn}r{round_}"
    path = f"{NOTES_DIR}/plan.md" if notes else f"{WORKSPACE}/work/{tag}.py"
    write = {
        "name": "Write",
        "id": f"call-w-{tag}",
        "args": {"file_path": path, "content": f"# {tag}\n" + "x = 1\n" * 120},
    }
    quote = {"name": "fetch_quote", "id": f"call-q-{tag}", "args": {"symbol": f"S{tag}"}}
    return [
        AIMessage("", id=f"ai-{tag}-w", tool_calls=[write]),
        AIMessage(f"Wrote it; quoting {tag}.", id=f"ai-{tag}-q", tool_calls=[quote]),
    ]


def _script(turn: int, rounds: int, *, notes: bool = False, label: str = "") -> list[AIMessage]:
    out = [
        message
        for j in range(rounds)
        for message in _round(turn, f"{label}{j}", notes=notes and j == 0)
    ]
    out.append(AIMessage(f"Answer to turn {turn}{label}.", id=f"ai-t{turn}{label}-end"))
    return out


Coverage = Callable[[Window], Any]


@dataclass
class ThreadRun:
    """What one run of the scenario left behind."""

    graph: Any = None
    model: RecordingModel = field(default_factory=RecordingModel)
    summary_model: SummaryModel = field(default_factory=SummaryModel)
    store: TranscriptStore = field(default_factory=TranscriptStore)
    #: Per turn, after it (and its re-entry): messages held and window base.
    held: list[tuple[int, int]] = field(default_factory=list)
    #: Each trim's question to the coverage: the runs it would leave trimmed,
    #: whether its head is exactly the start of the whole thread, and how
    #: many summaries had been written.
    asked: list[tuple[int, bool, int]] = field(default_factory=list)
    #: The messages of the turn the re-entry ran in, before and after it.
    reentry: tuple[list[str], list[str]] | None = None
    #: The window base before and after the re-entry.
    reentry_base: tuple[int, int] | None = None
    #: Per turn end: the messages exported, their base and the manifest saved.
    exports: list[tuple[list[AnyMessage], int, str]] = field(default_factory=list)
    #: Per turn, the checkpoint its end left.
    tails: list[str] = field(default_factory=list)


def build_graph(
    run: ThreadRun,
    saver: Any,
    *,
    now: datetime,
    turn: Turn,
    window: bool,
    covered: bool = True,
) -> Any:
    """The main agent's context stack for ``turn`` around ``run``'s fakes, as of ``now``,
    the server answering every coverage check with ``covered``."""

    async def switch() -> bool:
        return turn.subagents

    compaction = CompactionMiddleware(
        Summarizer(run.summary_model, limit=10**9, counter=len),
        token_threshold=THRESHOLD,
        keep_messages=KEEP,
        offload=OffloadSettings(keep_messages=KEEP, max_length=200, idle_seconds=0.0),
        backend=_backend(run.store),
        workspace_id="ws-1",
    ).with_scratchpad_notes(NOTES_DIR)
    scratchpad = ThreadScratchpad(
        folder=f"{WORKSPACE}/.agents/scratchpad/", notes_dir=NOTES_DIR, notes_subdir=NOTES_SUBDIR
    )
    notes_rows = NotesDueMiddleware(scratchpad, compaction)
    if window:

        async def coverage(head: Window) -> bool:
            # Not checked against the head, so a test that breaks the window
            # still sees it trim; the answer it should have had is kept.
            whole = whole_threads(run)[-1] if run.exports else []
            run.asked.append(
                (
                    head.runs,
                    head.holds(whole[: head.messages]),
                    len(run.summary_model.requests),
                )
            )
            return covered

        compaction = compaction.with_window(
            coverage, [notes_window_carry(THREAD), subagents_window_carry]
        )
    context = build_context_middleware(
        now=now, guidance=None, model_name=None, sandbox_enabled=False
    )
    return create_agent(
        run.model,
        system_prompt="You are the test analyst.",
        tools=[write_file, fetch_quote],
        middleware=[
            *([SubagentSwitchMiddleware(switch)] if turn.switch else []),
            compaction,
            PatchToolCallsMiddleware(),
            context.turn,
            context.baseline,
            notes_rows,
            context.tail,
        ],
        checkpointer=saver,
        state_schema=MainAgentState,
    )


def config(checkpoint_id: str | None = None, *, turn: int | None = None) -> dict[str, Any]:
    """The thread's config; with ``turn``, stamped as the server stamps a
    turn's input, which pairs it with its query row."""
    configurable: dict[str, Any] = {"thread_id": THREAD}
    if checkpoint_id:
        configurable["checkpoint_ns"] = ""
        configurable["checkpoint_id"] = checkpoint_id
    out: dict[str, Any] = {"configurable": configurable}
    if turn is not None:
        out["metadata"] = {"turn_index": turn - 1, "run_id": f"run-{turn}"}
    return out


async def export_turn_end(run: ThreadRun, graph: Any) -> None:
    """The server's turn-end export: the thread's state, rendered over the
    stored copy from the window's base."""
    values = (await graph.aget_state(config())).values
    messages, window = list(values["messages"]), Window.of(values)
    await run.store.save_transcript(TranscriptTarget(THREAD), messages, window=window)
    run.exports.append((messages, window.runs, run.store.manifest or ""))


async def run_turn(
    run: ThreadRun, saver: Any, number: int, turn: Turn, *, window: bool, text: str | None = None,
    checkpoint_id: str | None = None, covered: bool = True,
) -> Any:
    _Clock.at = _START + timedelta(hours=number)
    graph = build_graph(
        run,
        saver,
        now=_Clock.at,
        turn=turn,
        window=window,
        covered=covered,
    )
    run.model.script = _script(number, turn.rounds, notes=turn.notes)
    question = HumanMessage(text or f"Question {number}: quote and file it.", id=f"h-{number}")
    await graph.ainvoke({"messages": [question]}, config(checkpoint_id, turn=number))
    if turn.reentry:
        values = (await graph.aget_state(config())).values
        before = [m.id for m in _turn_messages(values, number)]
        base_before = Window.of(values).runs
        notice = HumanMessage(
            "Background task finished; collect its result.",
            id=f"orch-{number}",
            name="orchestrator",
            additional_kwargs={"lc_source": "orchestrator"},
        )
        await graph.aupdate_state(config(), {"messages": [notice]}, as_node="__start__")
        run.model.script = _script(number, turn.reentry, label="b")
        await graph.ainvoke(None, config())
        values = (await graph.aget_state(config())).values
        run.reentry = (before, [m.id for m in _turn_messages(values, number)])
        run.reentry_base = (base_before, Window.of(values).runs)
    await export_turn_end(run, graph)
    snapshot = await graph.aget_state(config())
    run.held.append((len(snapshot.values["messages"]), Window.of(snapshot.values).runs))
    run.tails.append(snapshot.config["configurable"]["checkpoint_id"])
    run.graph = graph
    return graph


def _turn_messages(values: Mapping[str, Any], number: int) -> list[AnyMessage]:
    messages = values["messages"]
    start = next(i for i, m in enumerate(messages) if m.id == f"h-{number}")
    return messages[start:]


async def run_scenario(
    saver: Any, *, window: bool, turns=SCENARIO, covered: bool = True
) -> ThreadRun:
    run = ThreadRun()
    with frozen():
        for number, turn in enumerate(turns, start=1):
            await run_turn(run, saver, number, turn, window=window, covered=covered)
    return run


async def run_edit(
    run: ThreadRun, saver: Any, number: int, turns=SCENARIO, *, window: bool
) -> None:
    """An edit of turn ``number``: the thread forked from the end of the turn
    before it, then the scenario's turns from there run again, the edited
    one asked differently."""
    fork = run.tails[number - 2]
    with frozen():
        for offset, turn in enumerate(turns[number - 1 :]):
            at = number + offset
            await run_turn(
                run,
                saver,
                at,
                turn,
                window=window,
                text=f"Question {at}, edited: quote it again." if offset == 0 else None,
                checkpoint_id=fork if offset == 0 else None,
            )


def whole_threads(run: ThreadRun) -> list[list[AnyMessage]]:
    """Each turn end's export with the runs trimmed before it put back, from
    the exports before: what the transcript renders when nothing is trimmed."""
    whole: list[AnyMessage] = []
    out = []
    for messages, base, _ in run.exports:
        whole = [m for trimmed in split_runs(whole)[:base] for m in trimmed] + messages
        out.append(whole)
    return out


@contextmanager
def frozen():
    """The row clock stopped and the check-in brought within the scenario.
    Tracing is off: ``load_dotenv`` may have found a checkout's ``.env``."""
    saved = notes_module.NOTES_CHECK_IN_CALLS, durable.datetime
    notes_module.NOTES_CHECK_IN_CALLS, durable.datetime = CHECK_IN_CALLS, _Clock
    try:
        with tracing_context(enabled=False):
            yield
    finally:
        notes_module.NOTES_CHECK_IN_CALLS, durable.datetime = saved


# ---------------------------------------------------------------- comparing

_ID = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|lc_run--[0-9a-f-]+"
)


class Normalizer:
    """Ids minted at run time, renamed in order of first appearance, so two
    runs that mint them in the same places read the same."""

    def __init__(self) -> None:
        self.names: dict[str, str] = {}

    def __call__(self, text: str) -> str:
        def name(match: re.Match[str]) -> str:
            value = match.group(0)
            if value == THREAD:
                return value
            return self.names.setdefault(value, f"<id{len(self.names)}>")

        return _ID.sub(name, text)


def dump(messages: list[AnyMessage]) -> str:
    """A request as text, every field the message carries."""
    return json.dumps(
        [message.model_dump() for message in messages], sort_keys=True, default=str
    )


def digests(requests: list[list[AnyMessage]]) -> list[str]:
    normalize = Normalizer()
    return [normalize(dump(request)) for request in requests]


def transcript(store: TranscriptStore) -> tuple[list[str], dict[str, str]]:
    """The saved manifests and the files, ids normalized. A manifest's
    digests hash the minted ids, so they are left out: the files they digest
    are compared instead, and a run checks its digests against a whole render
    (``whole_threads``)."""
    normalize = Normalizer()
    return (
        [normalize(_without_digests(save)) for save in store.saves],
        {name: normalize(data.decode()) for name, data in sorted(store.files.items())},
    )


def _without_digests(manifest: str) -> str:
    data = json.loads(manifest)
    for entry in data.get("segments") or ():
        entry.pop("sha256", None)
        entry.pop("shape", None)
        entry.pop("through", None)
    return json.dumps(data, sort_keys=True)


async def decoded_messages(saver: Any, graph: Any) -> int:
    """How many messages loading the thread's latest checkpoint decodes: the
    seed it starts from plus every write replayed onto it."""
    tip = (await graph.aget_state(config())).config
    history = (await saver.aget_delta_channel_history(config=tip, channels=["messages"]))[
        "messages"
    ]
    seed = history.get("seed")
    count = len(seed.value if isinstance(seed, _DeltaSnapshot) else seed or [])
    for _, _, value in history["writes"]:
        value = value.value if isinstance(value, Overwrite) else value
        count += len(value) if isinstance(value, list) else 1
    return count
