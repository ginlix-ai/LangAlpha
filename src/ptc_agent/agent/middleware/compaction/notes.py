"""The scratchpad notes and the main agent's compaction.

With the scratchpad feature on, the agent keeps a checkpoint note per task in
its thread's scratchpad: each finished step, finding, decision and correction,
written as it happens. A summary paraphrases, so the notes are what keeps those
exact. Two runtime-context rows ask for them: one shortly before each summary,
to checkpoint what the notes lack while the history they draw on is still in
view, and a check-in once a run of tool calls goes by with no write to them,
so they keep pace with the work rather than wait for the summary. After a
summary, it names the note files and sends the agent back to read them; their
text is not copied in, so the summary stays a summary and the notes stay the
one place each entry lives. Only the main agent gets any of this: a subagent
shares the thread but compacts its own run, and its stack is never handed a
notes folder.
"""

from __future__ import annotations

import asyncio
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
)
from langchain_core.messages import AIMessage

from ptc_agent.agent.middleware.compaction.utils import get_effective_messages, measured_tokens
from ptc_agent.agent.middleware.runtime_context.changes import UPDATE_SCHEMA_VERSION
from ptc_agent.agent.middleware.runtime_context.durable import (
    DurableUpdate,
    build_update_message,
    last_stated,
    messages_in_view,
    runtime_update_from_message,
)
from ptc_agent.agent.middleware.runtime_context.turn import (
    NOTES_CHECK_IN_ROW_KIND,
    NOTES_DUE_ROW_KIND,
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
_NOTES_ROW_KINDS = frozenset({NOTES_DUE_ROW_KIND, NOTES_CHECK_IN_ROW_KIND})

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
        short_id = thread_id[:8]
        notes_subdir = WorkspaceLayout.scratchpad_subdir(
            short_id, WorkspaceLayout.SCRATCHPAD_NOTE_DIR
        )
        return cls(
            folder=posixpath.join(workspace, WorkspaceLayout.scratchpad_subdir(short_id)) + "/",
            notes_dir=posixpath.join(workspace, notes_subdir),
            notes_subdir=notes_subdir,
        )


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
    summary: the reminder is asked only after one has."""

    _notes_below_mark: Annotated[NotRequired[str | None], PrivateStateAttr]


class NotesDueMiddleware(AgentMiddleware):
    """Asks the main agent to checkpoint its notes once before each summary,
    and checks in when its tool calls run on with the notes unwritten.

    Rows are durable rather than per call: written once where they came due,
    they stay in view until the summary takes them, so an ask is neither
    repeated on every call nor gone before the agent acts on it. Each names
    the summary the context grew from: the reminder is asked once per summary,
    so one the last summary kept in its tail does not count, while the
    check-in counts calls from the latest write or ask in view. A row naming
    an older summary is not sent either: past its summary the ask is stale,
    and read after the pointer it would have the notes rewritten from the
    summary's paraphrase.

    For the same reason the reminder waits for the context to grow past its
    mark under the current summary, from a call measured below it: a summary
    and the tail it keeps can start past the mark, and an ask on the first
    call after it would draw only on what that summary paraphrased.
    """

    state_schema = NotesDueState

    def __init__(self, scratchpad: ThreadScratchpad, compaction: CompactionMiddleware) -> None:
        super().__init__()
        self._scratchpad = scratchpad
        # The fill is a share of the size the main agent's compaction
        # summarizes at, measured as it measures, so the two cannot drift.
        self._token_threshold = compaction._token_threshold
        self._counter = compaction._summarizer.counter
        self._notes_folder = re.compile(re.escape(scratchpad.notes_subdir) + r"(?![\w.-])")

    def wrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], ModelResponse],
    ) -> ModelResponse:
        return handler(_without_answered(request))

    async def awrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
    ) -> ModelResponse:
        return await handler(_without_answered(request))

    async def abefore_model(self, state: Any, runtime: Any = None) -> dict[str, Any] | None:
        fill = self._fill(state)
        # At 1.0 the summary runs on this very call, too late to write anything.
        if fill >= 1.0:
            return None
        update: dict[str, Any] = {}
        since = _summary_id(state)
        if fill < notes_due_at(self._token_threshold):
            if state.get("_notes_below_mark", _UNNAMED) != since:
                update["_notes_below_mark"] = since
        row = self._notes_due(state, fill, since) or self._check_in(state)
        if row is not None:
            update["messages"] = [build_update_message(row)]
        return update or None

    def before_model(self, state: Any, runtime: Any = None) -> dict[str, Any] | None:
        # Sync fallback: the async agent won't call this but the protocol requires it.
        return None

    def _fill(self, state: Any) -> float:
        """The context's share of the threshold as the summary trigger reads
        it: what the last call measured, else a count of the view.

        The count runs short of the trigger's by the system prompt, which
        state does not hold, so where no usage is reported the reminder comes
        late by that much rather than never.
        """
        used = measured_tokens(state)
        if used is None:
            event = state.get("_summarization_event")
            used = self._counter(get_effective_messages(state.get("messages") or [], event))
        return used / self._token_threshold

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
        count = _calls_since_notes(state.get("messages") or (), folder, rows_end=False)
        return DurableUpdate(
            kind=NOTES_CHECK_IN_ROW_KIND,
            schema_version=UPDATE_SCHEMA_VERSION,
            text=(
                f"{count} tool calls have run since the last write to your "
                f"checkpoint notes in `{self._scratchpad.notes_dir}/`."
            ),
            provenance={"source": "harness", "summary": _summary_id(state)},
        )


def _summary_id(state: Any) -> str | None:
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
    count = 0
    for message in reversed(messages):
        if rows_end and _notes_row(message) is not None:
            break
        if isinstance(message, AIMessage):
            if any(_writes_notes(call, folder) for call in message.tool_calls):
                break
            count += len(message.tool_calls)
    return count


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


def _without_answered(request: ModelRequest) -> ModelRequest:
    """``request`` without the notes rows a summary has answered since.

    Runs inside compaction, so the summary this call is sent with, the one it
    just wrote included, leads the messages; rows that name another were
    written before it.
    """
    current = next((m.id for m in request.messages or [] if is_summary_message(m)), None)
    return _without_notes_rows(
        request, keep=lambda row: row.provenance.get("summary") == current
    )


def _without_notes_rows(
    request: ModelRequest, keep: Callable[[DurableUpdate], bool] = lambda _row: False
) -> ModelRequest:
    """``request`` without the notes rows ``keep`` refuses, by default every one."""
    messages = request.messages or []
    kept = [m for m in messages if (row := _notes_row(m)) is None or keep(row)]
    return request if len(kept) == len(messages) else request.override(messages=kept)
