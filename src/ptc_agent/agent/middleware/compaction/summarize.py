"""Writing a compaction summary, which always ends in one.

The text a compaction stores replaces the history the agent sees, so it is
never a provider's error. The summary model is tried first, then the model
running the turn when that is another model, and when both fail the server
lists each request and the reply to it. A failure that survives two models
is lasting (quota, both providers down, an input both refuse), so skipping
the compaction to try again next turn would only pay the same failures again
while the next turn waits on admission.

Shared by the automatic path and manual /compact, so both end the same way.

A model's reply is streamed rather than awaited whole, so a model still
writing is not cut off at a fixed share of the budget, and a reader watching
the compaction sees a count of what it has written. The call is tagged
``nostream``, which keeps its chunks out of the turn's own message stream,
where they would read as the answer, while the callbacks that bill and trace
it still see it.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Mapping, Sequence
from contextlib import aclosing
from dataclasses import dataclass
from itertools import groupby
from typing import Any, Literal

from langchain_core.messages import AIMessage, AnyMessage, HumanMessage
from langchain_core.runnables.base import RunnableBindingBase
from langgraph.constants import TAG_NOSTREAM

from src.llms.content_utils import format_llm_content
from src.llms.provider_compaction import is_opaque

from ptc_agent.agent.middleware.compaction.model import max_input_tokens
from ptc_agent.agent.middleware.compaction.summary_request import trim_for_summary
from ptc_agent.agent.middleware.compaction.types import _DEFAULT_FALLBACK_MESSAGE_COUNT
from ptc_agent.agent.middleware.compaction.utils import (
    handed_off,
    parse_summary_message,
    summary_source,
)
from ptc_agent.agent.transcript.classify import (
    human_kind,
    is_run_boundary_message,
    is_summary_message,
)
from ptc_agent.agent.transcript import Window
from ptc_agent.agent.transcript.pointer import TranscriptTurns
from ptc_agent.agent.transcript.render import message_turns, visible_text
from ptc_agent.agent.transcript.steer_text import report_text, sender_name, sent_steers

logger = logging.getLogger(__name__)

#: What wrote a summary: the agent's provider compacting the conversation
#: (``provider``), the agent's model writing it from its own request
#: (``self``), the summary model or its fallback reading the transcript, or
#: the server listing each request and reply.
SummarySource = Literal["provider", "self", "model", "fallback", "server"]

#: The summary model's share of the budget when another model waits behind
#: it, so a hung summary model still leaves the fallback a real call. A
#: model still writing at the end of its share goes on: what it wrote would
#: be lost, and the fallback would start over in what is left.
FIRST_SHARE = 2 / 3
#: How long a model that has begun writing may go without a chunk past its
#: share before it is taken for hung. Mid-summary gaps measure seconds.
_STALL_S = 60.0
#: How often the running count of what the summary has written is reported.
_PROGRESS_EVERY_S = 2.0
#: Room kept in a model's window for the instructions, the transcript index
#: and the summary it writes back.
_WINDOW_RESERVE = 16_000
#: The most of a model's window the history may fill: tiktoken counts low
#: against other vendors' tokenizers.
_WINDOW_SHARE = 0.7

# A server summary is a few thousand tokens: what the next turn needs to
# carry on, not a copy of the history the transcript already keeps.
_SERVER_CHARS = 24_000
_EARLIER_CHARS = 12_000
_REQUEST_CHARS = 1_500
_REPLY_CHARS = 4_000
_TODO_CHARS = 4_000

_LEAD = (
    "The summary model could not be reached, so this summary was put "
    "together without it: each request is followed by the last reply to "
    "it, and the tool calls in between are left out."
)
_LEAD_TRANSCRIPT = " The transcript described at the end keeps them in full."
#: The lead of a digest written beside a provider's own compaction, which only
#: that provider can read, for every other reader.
PROVIDER_LEAD = (
    "The earlier conversation was compacted into an encrypted summary that "
    "only the model that wrote it can read. What follows is a plain record of "
    "it instead: each request followed by the last reply to it, with the tool "
    "calls in between left out."
)


@dataclass(frozen=True)
class Summary:
    text: str
    source: SummarySource
    #: What the text stands in for in full, which the pointer's span and gap
    #: are read from: the trimmed stretch a model was sent, or the turns the
    #: server copied without a cut.
    covered: list[AnyMessage]
    #: What a provider's own compaction answered with (see
    #: ``src.llms.provider_compaction.native_answer``), and the route that
    #: wrote it. Claude's block holds ``text``; OpenAI's items hold nothing
    #: readable, so ``text`` is a digest of the same messages.
    native: dict[str, Any] | None = None
    origin: str | None = None


#: Given the most tokens a model's window leaves for the history (None when
#: it does not say), the messages it is sent and the request built from them.
Prepare = Callable[[int | None], Awaitable[tuple[list[AnyMessage], list[AnyMessage]]]]
#: Told about how many tokens the summary has written so far.
Progress = Callable[[int], None]


def model_label(model: Any) -> str | None:
    target = model.bound if isinstance(model, RunnableBindingBase) else model
    for attr in ("model_name", "model", "model_id", "deployment_name"):
        value = getattr(target, attr, None)
        if isinstance(value, str) and value:
            return value
    return None


def is_other_model(model: Any, other: Any) -> bool:
    """Whether ``other`` is worth a try after ``model`` failed: the same
    model would most likely fail the same way."""
    if other is None or other is model:
        return False
    name, other_name = model_label(model), model_label(other)
    return name is None or other_name is None or name != other_name


def preparer(
    to_summarize: list[AnyMessage],
    *,
    limit: int | None,
    counter: Callable[[list[AnyMessage]], int],
    render: Callable[[list[AnyMessage]], Awaitable[list[AnyMessage]]],
) -> Prepare:
    """Trims ``to_summarize`` for a model and renders its request, once per
    distinct limit: the fallback usually reads what the summary model was
    sent, and rendering offloads attachments to the sandbox. Raises when
    nothing fits, so the chain moves on: a summary of none of it would stand
    in for all of it, unread."""
    made: dict[int | None, tuple[list[AnyMessage], list[AnyMessage]]] = {}

    async def prepare(room: int | None) -> tuple[list[AnyMessage], list[AnyMessage]]:
        key = _limit(limit, room)
        if key not in made:
            trimmed = _trim(to_summarize, key, counter)
            if to_summarize and not trimmed:
                raise ValueError(f"no history fits {key} summary tokens")
            made[key] = (trimmed, await render(trimmed))
        return made[key]

    return prepare


async def awrite_summary(
    *,
    model: Any,
    fallback: Any | None,
    prepare: Prepare,
    server: Callable[[], Summary],
    budget: float,
    progress: Progress | None = None,
) -> Summary:
    """The summary model's summary, else ``fallback``'s, else the server's.

    The whole chain fits ``budget``, about as long as admission holds the
    next turn, including preparing each request, which uploads attachments
    to the sandbox. ``progress`` hears every few seconds about how many
    tokens have been written. Only a cancellation escapes; every other
    failure moves on.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + budget
    candidates = [("model", model)]
    if is_other_model(model, fallback):
        candidates.append(("fallback", fallback))
    for source, candidate in candidates:
        remaining = deadline - loop.time()
        if remaining <= 0:
            logger.warning("[Compaction] no time left for the %s summary", source)
            break
        share = remaining * FIRST_SHARE if len(candidates) > 1 and source == "model" else remaining
        try:
            # Each attempt counts from zero: the count is the summary's size.
            covered, response = await _attempt(
                prepare,
                candidate,
                quiet_until=loop.time() + share,
                deadline=deadline,
                written=_Written(progress),
            )
            return Summary(_checked(response), source, covered)
        except Exception as e:
            _log_failure(source, candidate, e)
    return _server(server)


async def _attempt(
    prepare: Prepare, model: Any, *, quiet_until: float, deadline: float, written: _Written
) -> tuple[list[AnyMessage], Any]:
    """``model``'s reply, held to ``quiet_until`` until it begins writing,
    then past it to ``deadline`` for as long as its chunks keep coming."""
    loop = asyncio.get_running_loop()
    response = None
    writing = False
    async with asyncio.timeout_at(quiet_until) as limit:
        covered, request = await prepare(_room(model))
        stream = model.astream(request, config={"tags": [TAG_NOSTREAM]})
        async with aclosing(stream) as chunks:
            async for chunk in chunks:
                response = chunk if response is None else response + chunk
                writing = written.add(chunk) or writing
                if writing:
                    limit.reschedule(max(quiet_until, min(deadline, loop.time() + _STALL_S)))
    if response is None:
        raise ValueError("no reply")
    return covered, response


class _Written:
    """About how many tokens a summary has written, reported to ``progress``
    every few seconds rather than per chunk, which is several a second."""

    def __init__(self, progress: Progress | None) -> None:
        self.tokens = 0
        self._progress = progress
        self._reported: float | None = None

    def add(self, chunk: Any) -> bool:
        """Count ``chunk``, and say whether it holds text or reasoning the
        model wrote, rather than the opening role or the closing usage."""
        parts = format_llm_content(chunk.content, getattr(chunk, "additional_kwargs", None))
        chars = len(parts.get("text") or "") + len(parts.get("reasoning") or "")
        if not (chars or chunk.content):
            return False
        # A chunk is about a token, in any script and for reasoning a
        # provider hides; one carrying more counts by its length.
        self.tokens += max(1, chars // 4)
        now = asyncio.get_running_loop().time()
        # The first report waits a few seconds into the writing, so the count
        # opens on a size rather than on one token.
        if self._reported is None:
            self._reported = now
        elif self._progress is not None and now - self._reported >= _PROGRESS_EVERY_S:
            self._reported = now
            self._progress(self.tokens)
        return chars > 0


def summary_text(response: Any) -> str:
    """The response's text, without any reasoning the provider sent along."""
    content = response.content if hasattr(response, "content") else response
    formatted = format_llm_content(content, getattr(response, "additional_kwargs", None))
    return formatted.get("text", "").strip()


def server_summary(
    to_summarize: Sequence[AnyMessage],
    preserved: Sequence[AnyMessage],
    *,
    raw_messages: Sequence[AnyMessage],
    turns: TranscriptTurns | None,
    window: Window,
    lead: str = _LEAD,
) -> Summary:
    """A summary built without a model: each request and the reply to it,
    the newest first to stay when they do not all fit, plus an earlier
    summary and the todo list. ``turns`` is set when the transcript is
    readable, and names the file each turn is in. ``window`` is what was
    trimmed from the head of ``raw_messages``. ``lead`` says why."""
    head = list(to_summarize[:1]) if to_summarize and is_summary_message(to_summarize[0]) else []
    numbers = (
        turns.turns if turns is not None else message_turns(raw_messages, base=window.runs)
    )
    covered: set[str] = set()
    sections: list[str] = []
    budget = _SERVER_CHARS

    if head:
        # An earlier server summary already said the model failed; its own
        # lead would only say it twice. Its newest turns and todo list come
        # last, so it keeps its end where a model's summary, which opens on
        # the goal, keeps its start.
        text = parse_summary_message(head[0])
        by_server = summary_source(head[0]) == "server" or (
            is_opaque(head[0]) and not handed_off(head[0])
        )
        body = text.partition("\n\n")[2] if by_server else text
        cut = (_cut_start if by_server else _cut)(body, _EARLIER_CHARS)
        if cut == body and head[0].id:
            covered.add(head[0].id)
        if cut:
            sections.append(f"## Earlier summary\n{cut}")
            budget -= len(cut)

    # By turn number, not by request: after an earlier summary the messages
    # can open partway through a turn whose request that summary holds, and
    # they end that turn rather than lead into the next request.
    runs = [
        list(run)
        for _, run in groupby(to_summarize[len(head) :], key=lambda m: numbers.get(m.id or ""))
    ]
    blocks: list[str] = []
    for run in reversed(runs):
        block, whole = _turn_block(run, numbers, turns)
        if blocks and len(block) > budget:
            break
        blocks.append(block)
        budget -= len(block)
        if whole:
            covered.update(m.id for m in run if m.id)
    if len(blocks) < len(runs):
        left_out = len(runs) - len(blocks)
        blocks.append(
            f"({left_out} earlier request{'s are' if left_out > 1 else ' is'} not listed here.)"
        )
    sections += reversed(blocks)

    todos = _todo_list(to_summarize, preserved)
    if todos:
        sections.append(f"## Todo list\n{_cut(todos, _TODO_CHARS)}")

    lead += _LEAD_TRANSCRIPT if turns is not None else ""
    return Summary(
        "\n\n".join([lead, *sections]),
        "server",
        [m for m in to_summarize if m.id in covered],
    )


def _room(model: Any) -> int | None:
    window = max_input_tokens(model)
    if window is None or window <= _WINDOW_RESERVE:
        return None
    return min(window - _WINDOW_RESERVE, int(window * _WINDOW_SHARE))


def _limit(limit: int | None, room: int | None) -> int | None:
    if room is None:
        return limit
    return room if limit is None else min(limit, room)


def _trim(
    messages: list[AnyMessage], limit: int | None, counter: Callable[[list[AnyMessage]], int]
) -> list[AnyMessage]:
    if limit is None or not messages:
        return messages
    try:
        return trim_for_summary(messages, limit, counter)
    except Exception as e:
        logger.warning("[Compaction] trimming the summary input failed (%s)", type(e).__name__)
        return messages[-_DEFAULT_FALLBACK_MESSAGE_COUNT:]


def _checked(response: Any) -> str:
    text = summary_text(response)
    if not text:
        raise ValueError("empty summary")
    return text


def _log_failure(source: str, model: Any, error: BaseException) -> None:
    logger.warning(
        "[Compaction] %s summary failed with %s (%s)",
        source,
        model_label(model) or type(model).__name__,
        type(error).__name__,
    )


def _server(server: Callable[[], Summary]) -> Summary:
    try:
        return server()
    except Exception:
        # The last step of the chain must not be the one that fails the turn.
        logger.exception("[Compaction] building the server summary failed")
        return Summary(
            "The summary model could not be reached, and the earlier "
            "conversation could not be listed here.",
            "server",
            [],
        )


def _turn_block(
    run: list[AnyMessage],
    numbers: Mapping[str, int],
    turns: TranscriptTurns | None,
) -> tuple[str, bool]:
    """One turn as its request, what was added while it ran, and its last
    reply, and whether none of them was cut. What was added can take back
    what the request asked for, so it is never left out."""
    from ptc_agent.agent.middleware.skills.content import skill_blocks_as_names

    opener = next((m for m in run if is_run_boundary_message(m)), None)
    replies = (
        visible_text(m.content, attachments=False).strip()
        for m in reversed(run)
        if isinstance(m, AIMessage)
    )
    reply = next((text for text in replies if text), "")
    anchor = opener or run[0]
    name = turns.file(anchor.id) if turns is not None else None
    number = numbers.get(anchor.id or "")
    lines = [f"## `{name}`" if name else f"## Turn {number}" if number else "## Turn"]
    whole = True
    if opener is not None:
        request = skill_blocks_as_names(visible_text(opener.content)).strip()
        lines.append(f"Request: {_cut(request, _REQUEST_CHARS)}")
        whole = len(request) <= _REQUEST_CHARS
    for message in run:
        if isinstance(message, HumanMessage) and human_kind(message) == "steering":
            # A report it carried is what a task found, kept as written.
            reports = report_text(message)
            if reports:
                lines.append(f"Then: {_cut(reports, _REQUEST_CHARS)}")
                whole = whole and len(reports) <= _REQUEST_CHARS
            # Each sender's own words: the line that asked the model to answer
            # them was for the run that read them.
            for steer in sent_steers(message):
                added = steer.text.strip()
                if not added:
                    continue
                by = "" if steer.sender == "user" else f" (from {sender_name(steer.sender)})"
                lines.append(f"Then{by}: {_cut(added, _REQUEST_CHARS)}")
                whole = whole and len(added) <= _REQUEST_CHARS
    lines.append(f"Last reply: {_cut(reply, _REPLY_CHARS) or '(none)'}")
    return "\n".join(lines), whole and len(reply) <= _REPLY_CHARS


def _cut(text: str, limit: int) -> str:
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "…"


def _cut_start(text: str, limit: int) -> str:
    return text if len(text) <= limit else "…" + text[len(text) - limit + 1 :].lstrip()


def _todo_list(to_summarize: Sequence[AnyMessage], preserved: Sequence[AnyMessage]) -> str:
    """The newest todo list among the summarized messages, unless a kept
    message has a newer one the agent still sees."""
    if _latest_todos(preserved) is not None:
        return ""
    return "\n".join(
        f"- [{item.get('status', 'pending')}] {item.get('content', '')}"
        for item in _latest_todos(to_summarize) or ()
        if isinstance(item, Mapping)
    )


def _latest_todos(messages: Sequence[AnyMessage]) -> list[Any] | None:
    for message in reversed(messages):
        if not isinstance(message, AIMessage):
            continue
        for call in reversed(message.tool_calls or []):
            if call.get("name") == "TodoWrite":
                todos = (call.get("args") or {}).get("todos")
                return todos if isinstance(todos, list) else None
    return None
