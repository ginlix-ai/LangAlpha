"""The scratchpad notes and the main agent's compaction.

With the scratchpad feature on, the agent keeps a checkpoint note per task in
its thread's scratchpad: each finished step, finding, decision and correction,
written as it happens. A summary paraphrases, so the notes are what keeps those
exact. Two runtime-context rows ask for them: one shortly before each summary,
to checkpoint what the notes lack while the history they draw on is still in
view, and a check-in once a run of tool calls goes by with no write to them,
so they keep pace with the work rather than wait for the summary. A summary
mid-turn that keeps no turn, the one that rebuilds a stale system prompt, is
preceded by a step of its own on the notes (see ``rebuild_notes_ask``). After a
summary, it names the note files and sends the agent back to read them; their
text is not copied in, so the summary stays a summary and the notes stay the
one place each entry lives. Only the main agent gets any of this: a subagent
shares the thread but compacts its own run, and its stack is never handed a
notes folder.
"""

from __future__ import annotations

import asyncio
import json
import logging
import posixpath
import re
from collections.abc import Awaitable, Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Annotated, Any, NotRequired

from langchain.agents.middleware.types import (
    AgentMiddleware,
    AgentState,
    ModelRequest,
    ModelResponse,
    PrivateStateAttr,
    ToolCallRequest,
)
from langchain_core.messages import AIMessage, AnyMessage, ToolMessage
from langgraph.types import Command

from ptc_agent.agent.middleware._utils import run_ids
from ptc_agent.agent.middleware.compaction.measure import context_tokens
from ptc_agent.agent.middleware.compaction.utils import read_ahead
from ptc_agent.agent.middleware.runtime_context.changes import UPDATE_SCHEMA_VERSION
from ptc_agent.agent.middleware.runtime_context.durable import (
    DurableUpdate,
    build_update_message,
    written_row,
    last_stated,
    messages_in_view,
    runtime_update_from_message,
)
from ptc_agent.agent.middleware.runtime_context.frozen_prompt import pinned_static
from ptc_agent.agent.middleware.runtime_context.turn import (
    NOTES_CHECK_IN_ROW_KIND,
    NOTES_DUE_ROW_KIND,
    REBUILD_NOTES_ROW_KIND,
)
from ptc_agent.agent.transcript.classify import is_summary_message
from ptc_agent.core.paths import WorkspaceLayout

if TYPE_CHECKING:
    from ptc_agent.agent.middleware.compaction.middleware import CompactionMiddleware
    from ptc_agent.config.agent import AgentConfig

logger = logging.getLogger(__name__)

#: The room left under the summary trigger when the agent is asked to
#: checkpoint its notes: this many tokens, or this share of the trigger where
#: that is more. A step (the model's reply and the results it gets back) that
#: carries the context from under the mark to past the summary skips the ask,
#: so the room should exceed one step, and a step's size is in tokens rather
#: than a share of the trigger: live steps ran 22k to 31k, and one tool result
#: alone can reach about 40k (``LargeResultEvictionMiddleware``'s limit, Read's
#: ``MAX_READ_CHARS``). A reply that writes a long file, or several large
#: results in one step, can still skip it.
NOTES_DUE_ROOM_TOKENS = 40_000
NOTES_DUE_ROOM_SHARE = 0.15
#: The lowest mark: under about an 80k trigger the prompt and the kept tail
#: fill so much of it that a lower one would fire on the first call after each
#: summary.
NOTES_DUE_LOWEST = 0.5

#: Tool calls without a write to the notes before the agent is checked in on.
NOTES_CHECK_IN_CALLS = 30
# The tools that name the notes folder without writing to it.
_READ_ONLY_TOOLS = frozenset({"Read", "Glob", "Grep"})
_NOTES_ROW_KINDS = frozenset({NOTES_DUE_ROW_KIND, NOTES_CHECK_IN_ROW_KIND, REBUILD_NOTES_ROW_KIND})
# The static prompt's scratchpad section, which says what the notes are for.
_PROMPT_SECTION = "<thread_scratchpad>"

# The files a summary names; older ones are counted, not named.
_MAX_NAMED = 20
# A name the summary quotes: path segments of word characters, dots and
# hyphens, short enough to read as a file name.
_NAME_SEGMENT = re.compile(r"[\w.-]+")
_MAX_NAME = 120
# Caps a sandbox that stopped answering. The listing runs beside the summary,
# so the compaction's remaining budget bounds it too.
_LIST_TIMEOUT = 30.0
# What state and last_stated answer when nothing names a summary, which no
# summary id (None before the first summary) equals.
_UNNAMED = object()

_POINTER = (
    "\n\nYour checkpoint notes for this thread are in `{directory}/`: {files}. "
    "Read the note of each task still under way and pick up from there: this "
    "summary paraphrases, and the notes "
    "keep what you recorded word for word."
)
_UNLISTED = (
    "\n\nYour checkpoint notes for this thread, if you kept any, are in "
    "`{directory}/`. Read them before you continue."
)


def notes_due_at(threshold: int) -> float:
    """The fill at which the reminder comes due under a ``threshold``-token trigger."""
    room = max(NOTES_DUE_ROOM_TOKENS / threshold, NOTES_DUE_ROOM_SHARE)
    return max(1 - room, NOTES_DUE_LOWEST)


@dataclass(frozen=True)
class ThreadScratchpad:
    """A thread's scratchpad, in each spelling a reader needs, from one place:
    the prompt names ``folder``, the reminders and the summary pointer name
    ``notes_dir``, and a tool call's arguments are searched for
    ``notes_subdir``.

    Absolute paths, because the agent writes them from code as well as the
    file tools, and code may run from any directory.
    """

    folder: str
    notes_dir: str
    notes_subdir: str

    @classmethod
    def resolve(
        cls, config: AgentConfig, workspace: str, thread_id: str | None
    ) -> ThreadScratchpad | None:
        """``thread_id``'s scratchpad under ``workspace``, or None when the
        user's flag is off or there is no thread to name the folder after."""
        if not thread_id or not config.feature_enabled("scratchpad"):
            return None
        notes_subdir = thread_notes_subdir(thread_id)
        return cls(
            folder=posixpath.join(
                workspace, WorkspaceLayout.scratchpad_subdir(thread_id[:8])
            )
            + "/",
            notes_dir=posixpath.join(workspace, notes_subdir),
            notes_subdir=notes_subdir,
        )


def thread_notes_subdir(thread_id: str) -> str:
    """The thread's notes folder, workspace-relative, whether or not the
    scratchpad is on."""
    return WorkspaceLayout.scratchpad_subdir(thread_id[:8], WorkspaceLayout.SCRATCHPAD_NOTE_DIR)


def _notes_pattern(notes_subdir: str) -> re.Pattern[str]:
    return re.compile(re.escape(notes_subdir) + r"(?![\w.-])")


def notes_window_carry(thread_id: str) -> Callable[[Sequence[Any], Mapping[str, Any]], dict[str, Any]]:
    """The window's carry (see ``compaction.window``) for the check-in's
    count, which runs back to the last write to the notes however far back
    that was. Kept with the scratchpad off too: it can be turned on later."""
    folder = _notes_pattern(thread_notes_subdir(thread_id))

    def carry(trimmed: Sequence[Any], state: Mapping[str, Any]) -> dict[str, Any]:
        held = state.get("_notes_calls_trimmed") or 0
        total = _calls_since_write(trimmed, folder, held)
        return {"_notes_calls_trimmed": total} if total != held else {}

    return carry


async def anotes_pointer(
    backend: Any | None, notes_dir: str | None, budget: float = _LIST_TIMEOUT
) -> str:
    """The text a summary appends for ``notes_dir``: its files, newest first,
    or nothing when it holds none, listed within ``budget`` seconds. Never
    raises: a listing that fails or runs out of time still names the folder."""
    if backend is None or not notes_dir:
        return ""
    try:
        names = await asyncio.wait_for(
            _alist(backend, notes_dir), timeout=min(budget, _LIST_TIMEOUT)
        )
    except Exception as e:
        logger.warning("[Compaction] scratchpad notes listing for %s failed: %r", notes_dir, e)
        return _UNLISTED.format(directory=notes_dir)
    return render_pointer(notes_dir, names)


async def _alist(backend: Any, notes_dir: str) -> list[str]:
    """The folder's files relative to it, newest first as the glob lists them."""
    result = await backend.aglob("**/*", path=notes_dir)
    if result.error:
        raise RuntimeError(result.error)
    root = backend.normalize_path(notes_dir)
    return [
        posixpath.relpath(m["path"], root) for m in result.matches or () if m.get("path")
    ]


def render_pointer(notes_dir: str, names: list[str]) -> str:
    """The pointer naming ``names``, the folder's files newest first.

    The names come from the sandbox and the summary is trusted text, so only
    one that reads as a plain file name is quoted: a backtick or a newline
    would close the quote and let the name write instructions. The rest are
    counted with the files past the twentieth.
    """
    if not names:
        return ""
    quoted = [name for name in names if _quotable(name)][:_MAX_NAMED]
    if not quoted:
        return _UNLISTED.format(directory=notes_dir)
    files = ", ".join(f"`{name}`" for name in quoted)
    if len(names) > len(quoted):
        files += f" and {len(names) - len(quoted)} older file(s)"
    return _POINTER.format(directory=notes_dir, files=files)


def _quotable(name: str) -> bool:
    return len(name) <= _MAX_NAME and all(
        _NAME_SEGMENT.fullmatch(part) and part not in (".", "..") for part in name.split("/")
    )


class NotesDueState(AgentState):
    """``_notes_below_mark`` names the summary under which a call last
    measured the context below the reminder's mark, None before the first
    summary: the reminder is asked only after one has.
    ``_notes_calls_trimmed`` is the tool calls since the last write to the
    notes in the runs the window trimmed, for the check-in's count."""

    _notes_below_mark: Annotated[NotRequired[str | None], PrivateStateAttr]
    _notes_calls_trimmed: Annotated[NotRequired[int], PrivateStateAttr]


class NotesDueMiddleware(AgentMiddleware):
    """Asks the main agent to checkpoint its notes once before each summary,
    and checks in when its tool calls run on with the notes unwritten.

    Rows are durable rather than per call: written once where they came due,
    they stay in view until the summary takes them, so an ask is neither
    repeated on every call nor gone before the agent acts on it. Each names
    the summary the context grew from: the reminder is asked once per summary,
    so one the last summary kept in its tail does not count, while the
    check-in counts calls from the latest write or ask in view. A row naming
    an older summary that the agent had not answered when that summary was
    written is not sent either: past its summary the ask is stale, and read
    after the pointer it would have the notes rewritten from the summary's
    paraphrase. One it answered in the turns the summary kept is sent with
    them, as they were.

    For the same reason the reminder waits for the context to grow past its
    mark under the current summary, from a call measured below it: a summary
    and the tail it keeps can start past the mark, and an ask on the first
    call after it would draw only on what that summary paraphrased.

    A Claude thread pinned to a prompt from before the scratchpad was on is
    sent no section that explains the notes, so until a rebuild takes the
    new prompt it is treated as ``NotesOffMiddleware`` treats a thread.
    """

    state_schema = NotesDueState

    def __init__(
        self, scratchpad: ThreadScratchpad, compaction: CompactionMiddleware, model: Any
    ) -> None:
        super().__init__()
        self._scratchpad = scratchpad
        # The fill is a share of the size the main agent's compaction
        # summarizes at, measured as it measures for the turn's ``model``, so
        # the two cannot drift.
        self._token_threshold = compaction._token_threshold
        self._estimate = compaction._estimate(model)
        self._view = compaction.view
        self._model = model
        self._notes_folder = _notes_pattern(scratchpad.notes_subdir)

    def wrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], ModelResponse],
    ) -> ModelResponse:
        return handler(_notes_rows_sent(request))

    async def awrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
    ) -> ModelResponse:
        return await handler(_notes_rows_sent(request))

    async def awrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], Awaitable[ToolMessage | Command]],
    ) -> ToolMessage | Command:
        """Log each successful call on the notes by the check-in's own rule
        (``_writes_notes``, which counts a Bash ``cat`` of a note too), with
        the row it follows, so the rows asked and the note calls after them can
        be counted from the logs."""
        result = await handler(request)
        if _writes_notes(request.tool_call, self._notes_folder) and _succeeded(result):
            thread_id, agent = run_ids()
            logger.info(
                "[Notes] call tool=%s after=%s thread_id=%s agent=%s",
                request.tool_call.get("name"),
                _answered_row(request.state or {}, self._notes_folder, request.tool_call.get("id")),
                thread_id,
                agent,
            )
        return result

    async def abefore_model(self, state: Any, runtime: Any = None) -> dict[str, Any] | None:
        view = await self._view(state.get("messages") or [], state)
        fill = await self._fill(state, view)
        # At 1.0 the summary runs on this very call, too late to write anything.
        if fill >= 1.0:
            return None
        update: dict[str, Any] = {}
        since = summary_id(state)
        if fill < notes_due_at(self._token_threshold):
            if state.get("_notes_below_mark", _UNNAMED) != since:
                update["_notes_below_mark"] = since
        # The mark is kept either way: a model switch takes the new prompt
        # without a summary, and the stretch it is in still started below.
        if not explains_notes(state, self._model, view):
            return update or None
        row = self._notes_due(state, fill, since) or self._check_in(state)
        if row is not None:
            thread_id, agent = run_ids()
            logger.info(
                "[Notes] asked kind=%s fill=%.2f thread_id=%s agent=%s", row.kind, fill, thread_id, agent
            )
            update["messages"] = [build_update_message(row)]
        return update or None

    def before_model(self, state: Any, runtime: Any = None) -> dict[str, Any] | None:
        # Sync fallback: the async agent won't call this but the protocol requires it.
        return None

    async def _fill(self, state: Any, view: list[Any]) -> float:
        """The context's share of the threshold as the summary trigger reads
        it (``context_tokens``), over ``view``, the view compaction sends,
        without the attachments it removed or the results it hid.

        The count runs short of the trigger's by the system prompt, which
        state does not hold, so where no usage is reported the reminder comes
        late by that much rather than never.
        """
        await read_ahead(view)
        reading = await context_tokens(state, view, self._estimate)
        return reading.tokens / self._token_threshold

    def _notes_due(self, state: Any, fill: float, since: str | None) -> DurableUpdate | None:
        if fill < notes_due_at(self._token_threshold):
            return None
        if state.get("_notes_below_mark", _UNNAMED) != since:
            return None
        # A reminder names the summary it was asked under, and any naming the
        # current one came after every older one, so the last in view says.
        if last_stated(state, NOTES_DUE_ROW_KIND, "summary", _UNNAMED) == since:
            return None
        return DurableUpdate(
            kind=NOTES_DUE_ROW_KIND,
            schema_version=UPDATE_SCHEMA_VERSION,
            text=(
                f"The context is at {fill:.0%} of the size at which it is summarized. "
                f"Your checkpoint notes are in `{self._scratchpad.notes_dir}/`."
            ),
            provenance={"source": "harness", "summary": since},
        )

    def _check_in(self, state: Any) -> DurableUpdate | None:
        folder = self._notes_folder
        since_asked = _calls_since_notes(messages_in_view(state), folder, rows_end=True)
        if since_asked < NOTES_CHECK_IN_CALLS:
            return None
        count = _calls_since_write(
            state.get("messages") or (), folder, state.get("_notes_calls_trimmed") or 0
        )
        return DurableUpdate(
            kind=NOTES_CHECK_IN_ROW_KIND,
            schema_version=UPDATE_SCHEMA_VERSION,
            text=(
                f"{count} tool calls have run since the last write to your "
                f"checkpoint notes in `{self._scratchpad.notes_dir}/`."
            ),
            provenance={"source": "harness", "summary": summary_id(state)},
        )


def rebuild_notes_ask(view: Sequence[Any], notes_dir: str, state: Any) -> AnyMessage | None:
    """The row asking the agent to bring its notes up to date on the step
    before a summary that keeps no turn mid-turn, or None when an ask to write
    them the agent has not acted on is already in ``view``: NotesDue judges
    the fill without the system prompt, so it can write one on this very call.
    A check-in does not count, since it lets the agent skip writing, and this
    ask is the one that says the summary keeps no turn.

    It names the summary it was asked under, as the reminder does, so a call
    after a later summary goes without it (``_without_answered``).
    """
    last = max((i for i, m in enumerate(view) if isinstance(m, AIMessage)), default=-1)
    if any((row := _notes_row(m)) is not None and row.kind != NOTES_CHECK_IN_ROW_KIND for m in view[last + 1 :]):
        return None
    return written_row(
        DurableUpdate(
            kind=REBUILD_NOTES_ROW_KIND,
            schema_version=UPDATE_SCHEMA_VERSION,
            text="",
            provenance={"source": "harness", "summary": summary_id(state), "notes_dir": notes_dir},
        )
    )


def last_step_wrote_notes(view: Sequence[Any], thread_id: str | None) -> bool:
    """Whether the newest answer in ``view`` wrote the thread's notes: a call
    on them (``_writes_notes``) whose result reports success, so they need no
    step of their own before a summary."""
    at = next((i for i in range(len(view) - 1, -1, -1) if isinstance(view[i], AIMessage)), None)
    if at is None or not thread_id:
        return False
    results = {m.tool_call_id: m for m in view[at + 1 :] if isinstance(m, ToolMessage)}
    folder = _notes_pattern(thread_notes_subdir(thread_id))
    return any(
        _writes_notes(call, folder) and call["id"] in results and _succeeded(results[call["id"]])
        for call in view[at].tool_calls
    )


def summary_id(state: Any) -> str | None:
    """The id of the summary the current context starts from, None before any."""
    event = state.get("_summarization_event")
    return getattr(event["summary_message"], "id", None) if event else None


class NotesOffMiddleware(AgentMiddleware):
    """Keeps a thread's notes rows from the model once the user turns the
    scratchpad off: each asks for notes the agent no longer keeps. The
    summary's pointer stays, since the notes it names stay on disk while the
    thread is open, and the next summary drops it."""

    def wrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], ModelResponse],
    ) -> ModelResponse:
        return handler(_without_notes_rows(request))

    async def awrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
    ) -> ModelResponse:
        return await handler(_without_notes_rows(request))


def _succeeded(result: Any) -> bool:
    """Whether a tool's result reports success. Tools report a refusal in their
    result rather than as an error: the file tools as text that starts with
    ``ERROR``, the JSON tools as ``"success": false``, either inside a
    ``Command`` that carries the result."""
    if isinstance(result, Command):
        update = result.update if isinstance(result.update, dict) else {}
        return all(_succeeded(m) for m in update.get("messages") or () if isinstance(m, ToolMessage))
    if not isinstance(result, ToolMessage):
        return True
    text = str(result.content).lstrip()
    if result.status == "error" or text.startswith("ERROR"):
        return False
    try:
        reported = json.loads(text) if text.startswith("{") else None
    except (ValueError, RecursionError):
        return True
    return not (isinstance(reported, dict) and reported.get("success") is False)


def _answered_row(state: Mapping[str, Any], folder: re.Pattern[str], call_id: str | None) -> str:
    """The kind of the notes row the note call ``call_id`` in the newest answer
    follows, or ``none`` when an earlier successful note call came after it.

    Only rows the model's requests still carry count: a row a summary has
    answered is kept out of them (``_without_answered``), so a call after it
    is not an answer to it. The note calls of one answer run together and see
    the same state, so only the first of them follows the row.
    """
    messages = state.get("messages") or []
    asking = max((i for i, m in enumerate(messages) if isinstance(m, AIMessage)), default=len(messages))
    if asking < len(messages):
        batch = [call.get("id") for call in messages[asking].tool_calls if _writes_notes(call, folder)]
        if batch and batch[0] != call_id:
            return "none"
    current = summary_id(state)
    gone = unanswered_notes_rows(state)
    legacy = current is not None and "_unanswered_notes_rows" not in state
    failed: set[str] = set()
    for message in reversed(messages[:asking]):
        if isinstance(message, ToolMessage) and not _succeeded(message):
            failed.add(message.tool_call_id)
        elif (row := _notes_row(message)) is not None:
            # ``_without_answered``'s rule for a call that answers from the
            # current summary: an older summary's row stays unless that
            # summary answered it, or the thread predates the record.
            if row.provenance.get("summary") == current or not (legacy or message.id in gone):
                return row.kind
        elif isinstance(message, AIMessage) and any(
            _writes_notes(call, folder) and call.get("id") not in failed for call in message.tool_calls
        ):
            return "none"
    return "none"


def _notes_row(message: Any) -> DurableUpdate | None:
    update = runtime_update_from_message(message)
    return update if update is not None and update.kind in _NOTES_ROW_KINDS else None


def _calls_since_notes(messages: Sequence[Any], folder: re.Pattern[str], *, rows_end: bool) -> int:
    """The tool calls after the latest write to the notes, or with ``rows_end``
    after the latest write or notes row.

    Rows end the count that times a check-in, so the next one is due a full run
    of calls after the last ask rather than on the call after it. The count a
    check-in reports runs from the last write alone, since that is what its
    text claims.
    """
    return _count_back(messages, folder, rows_end=rows_end)[0]


def _calls_since_write(messages: Sequence[Any], folder: re.Pattern[str], carried: int) -> int:
    """The tool calls after the latest write to the notes, ``carried`` being
    the count the window kept for the messages it trimmed before these."""
    count, wrote = _count_back(messages, folder, rows_end=False)
    return count if wrote else count + carried


def _count_back(
    messages: Sequence[Any], folder: re.Pattern[str], *, rows_end: bool
) -> tuple[int, bool]:
    """``_calls_since_notes``, and whether a write (or row) ended the count
    rather than the start of ``messages``."""
    count = 0
    for message in reversed(messages):
        if rows_end and _notes_row(message) is not None:
            return count, True
        if isinstance(message, AIMessage):
            if any(_writes_notes(call, folder) for call in message.tool_calls):
                return count, True
            count += len(message.tool_calls)
    return count, False


def _writes_notes(call: Mapping[str, Any], folder: re.Pattern[str]) -> bool:
    """Whether ``call`` names the notes ``folder`` from a tool that can write.

    The folder is looked for in every argument, since code and commands name
    it as freely as the file tools do, and joined to a file name as often as
    written out with one. It must end its path component there, so a
    scratchpad file whose name starts with the folder's, such as
    ``notes_lang.txt``, is not taken for a note. A Bash ``cat`` of a note
    counts as a write too, which is accepted. The strings are searched as
    they are: a check-in counts back through the whole thread, and
    serializing every call's arguments would copy all of them on each one.
    """
    if call.get("name") in _READ_ONLY_TOOLS:
        return False
    return any(folder.search(text) for text in _strings(call.get("args")))


def _strings(value: Any) -> Iterator[str]:
    if isinstance(value, str):
        yield value
    elif isinstance(value, Mapping):
        for item in value.values():
            yield from _strings(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            yield from _strings(item)


def unanswered_rows(messages: Sequence[Any]) -> set[str]:
    """The ids of the notes rows after the last answer in ``messages``, the
    tail a summary keeps: written before it and not yet acted on, so after it
    they would ask for notes from what it paraphrased."""
    last = max((i for i, m in enumerate(messages) if isinstance(m, AIMessage)), default=-1)
    return {m.id for m in messages[last + 1 :] if m.id and _notes_row(m) is not None}


def unanswered_notes_rows(state: Mapping[str, Any]) -> set[str]:
    return set(state.get("_unanswered_notes_rows") or ())


def suppressed_notes_rows(state: Mapping[str, Any], kept: Sequence[Any]) -> set[str]:
    """The notes rows of ``kept`` that the summary ``state`` starts from kept
    out of its calls, which the next summary must keep out too: the thinking
    in the turns both keep was produced without them."""
    # TODO(2026-11-25): delete 30 days after this ships, once the threads written before it have aged out.
    if (
        "_unanswered_notes_rows" not in state
        and (summary := summary_id(state)) is not None
    ):
        return {
            m.id
            for m in kept
            if m.id
            and (row := _notes_row(m)) is not None
            and row.provenance.get("summary") != summary
        }
    return unanswered_notes_rows(state) & {m.id for m in kept}


def _without_answered(request: ModelRequest) -> ModelRequest:
    """``request`` without the notes rows a summary has answered since.

    Runs inside compaction, so the summary this call is sent with, the one it
    just wrote included, leads the messages; rows that name another were
    written before it. Only the ones its tail kept unanswered go
    (``unanswered_rows``), recorded with it, and found here on the call
    that answers from it, which runs before the record is written. A row the
    agent answered in the turns a summary keeps stays: those turns are sent
    as they were, since a model with preserved thinking checks the thinking
    in them against everything before it.
    """
    messages = request.messages or []
    state = request.state or {}
    current = next((m.id for m in messages if is_summary_message(m)), None)
    gone = unanswered_notes_rows(state)
    if current != summary_id(state):
        answered = max((i for i, m in enumerate(messages) if isinstance(m, AIMessage)), default=-1)
    # TODO(2026-11-25): delete 30 days after this ships, once the threads written before it have aged out.
    elif current is not None and "_unanswered_notes_rows" not in state:
        answered = 0
    else:
        answered = len(messages)
    return _dropping(
        request,
        {
            i
            for i, m in enumerate(messages)
            if (row := _notes_row(m)) is not None
            and row.provenance.get("summary") != current
            and (i >= answered or m.id in gone)
        },
    )


def explains_notes(state: Any, model: Any, view: Sequence[Any]) -> bool:
    """Whether the static prompt a call to ``model`` sending ``view`` goes
    out with has the scratchpad section: the build's always does where the
    notes rows run, so only a pinned one (``pinned_static``) can lack it."""
    pinned = pinned_static(state, model, view)
    return pinned is None or _PROMPT_SECTION in pinned


def _notes_rows_sent(request: ModelRequest) -> ModelRequest:
    """``request`` with the notes rows its static prompt can explain."""
    explained = explains_notes(
        getattr(request, "state", None), getattr(request, "model", None), request.messages or []
    )
    return _without_answered(request) if explained else _without_notes_rows(request)


def _without_notes_rows(request: ModelRequest) -> ModelRequest:
    """``request`` without its notes rows."""
    messages = request.messages or []
    return _dropping(request, {i for i, m in enumerate(messages) if _notes_row(m) is not None})


def _dropping(request: ModelRequest, drop: set[int]) -> ModelRequest:
    """``request`` without the messages at the positions in ``drop``."""
    if not drop:
        return request
    return request.override(messages=[m for i, m in enumerate(request.messages) if i not in drop])
