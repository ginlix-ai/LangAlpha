"""One compaction, whichever path runs it.

The middleware compacts when the context nears its limit, manual /compact
when the user asks. Both summarize the view the model is sent, through the
same chain, into the same event and state update, so a thread reads the same
after either. Manual /offload chooses Tier 1 cuts the way the middleware's
idle pass does.
"""

from __future__ import annotations

import asyncio
import functools
import logging
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, NamedTuple
from uuid import uuid4

from langchain_core.messages import AnyMessage

from langchain.chat_models import BaseChatModel

from src.config.settings import get_compaction_timeout
from src.llms import LLM, create_llm
from src.llms.model_spec import summary_reasoning_effort
from src.llms.provider_compaction import deferred_text, readable_text

from ptc_agent.agent.state import ensure_message_ids
from ptc_agent.agent.middleware.runtime_context.durable import build_update_message
from ptc_agent.agent.middleware.runtime_context.restate import summary_note_row
from ptc_agent.agent.middleware.compaction.types import (
    CompactionEvent,
    OffloadSettings,
    TokenCounter,
)
from ptc_agent.agent.middleware.compaction.summary_request import (
    build_summary_request,
    keeps_turns,
    summary_prompt,
)
from ptc_agent.agent.middleware.compaction.summarize import (
    FIRST_SHARE,
    PROVIDER_LEAD,
    Progress,
    Summary,
    awrite_summary,
    is_other_model,
    preparer,
    server_summary,
)
from ptc_agent.agent.middleware.compaction.measure import (
    estimate_for,
    keep_cutoff,
    keep_tokens_for,
)
from ptc_agent.agent.middleware.compaction.utils import (
    build_compaction_event,
    build_summary_message,
    compacted_skills,
    count_tokens_tiktoken,
    get_effective_messages,
    partition_at_cutoff,
    read_ahead,
    summarized_span,
    target_cost,
)
from ptc_agent.agent.middleware.file_operations.multimodal_strip import (
    MultimodalStripMiddleware,
)
from ptc_agent.agent.middleware.compaction.model import resolve_compaction_client
from ptc_agent.agent.middleware.compaction.notes import (
    anotes_pointer,
    suppressed_notes_rows,
    unanswered_rows,
)
from ptc_agent.agent.middleware.compaction.offloading import (
    Offloads,
    aoffload_base64_content,
    offload_view,
    recorded_offloads,
    removed_attachments,
    select_offloads,
    tool_call_ids,
)
from ptc_agent.agent.middleware.thinking_binding import (
    thinking_after,
    without_thinking,
)
from ptc_agent.agent.transcript import TranscriptTarget, Window
from ptc_agent.config.agent import CredentialSource
from ptc_agent.agent.transcript.pointer import (
    TranscriptTurns,
    aexport_transcript,
    shell_reads_transcripts,
    summary_span,
    transcript_target,
)

if TYPE_CHECKING:
    from ptc_agent.agent.backends.sandbox import SandboxBackend
    from ptc_agent.config.agent import AgentConfig

logger = logging.getLogger(__name__)

#: How far past the threshold the summary model may read, so the history
#: that crossed it, however large its last tool result, usually reaches it
#: whole.
_SUMMARY_HEADROOM = 50_000

#: The refusal of a /compact whose view is no longer than what a summary keeps.
NOTHING_TO_COMPACT = "Nothing to compact yet: the conversation fits in what compaction keeps."


class NativeAnswer(NamedTuple):
    """What a provider's own compaction answered with (see
    ``src.llms.provider_compaction.native_answer``), and the route that
    wrote it."""

    answer: dict[str, Any]
    origin: str | None


#: The agent's own model summarizing the messages it is given, from its own
#: request: its provider's compaction, or the summary it writes as text.
#: Raises when it writes no summary.
Own = Callable[[list[AnyMessage], bool], Awaitable[NativeAnswer | str]]


@dataclass(frozen=True)
class Compaction:
    """A summary in place of the start of the view."""

    event: CompactionEvent
    summary: Summary
    #: How many messages the view held before the summary replaced its start.
    original_count: int
    #: The messages kept after the summary.
    preserved: list[AnyMessage]

    @property
    def messages(self) -> list[AnyMessage]:
        """The view after it."""
        return [self.event["summary_message"], *without_thinking(self.preserved, self.unsent)]

    def note_row(self) -> AnyMessage | None:
        """The row taking the summary's note to the call that answers from
        it, after the turns it kept; None when the summary sends its note
        itself or has none (see ``deferred_text``)."""
        summary = self.event["summary_message"]
        note = deferred_text(summary)
        if note is None:
            return None
        row = build_update_message(summary_note_row(note, summary.id, datetime.now(tz=UTC)))
        row.id = str(uuid4())
        return row

    @property
    def unsent(self) -> set[str]:
        """The kept answers whose thinking it invalidates: all of them, but
        under the compaction block Claude wrote. A summary written on the
        client is a new first message, which changes the prefix every kept
        thinking block is checked against, so they go, as Anthropic's recipe
        for a compaction done on the client says."""
        if isinstance((self.summary.native or {}).get("block"), dict):
            return set()
        summary = self.event["summary_message"]
        return thinking_after([summary, *self.preserved], {summary.id})

    def update(self, state: Mapping[str, Any]) -> dict[str, Any]:
        """The state update that installs it over ``state``.

        Summarized calls never reach the model again, so their recorded
        offload ids are dropped, and so are the summarized messages whose
        attachments or thinking were taken out. The notes rows the tail keeps
        after its last answer are recorded, since no later call is sent them
        (``unanswered_rows``). The cached token counts measured the view
        before it: left in place, the next call would read them as still over
        the threshold and compact again at once.
        """
        live = tool_call_ids(self.preserved)
        args, reads = recorded_offloads(state)
        kept = {msg.id for msg in self.preserved}
        return {
            "_summarization_event": self.event,
            "_offloaded_tool_call_ids": args & live,
            "_offloaded_read_result_ids": reads & live,
            "_removed_attachment_ids": removed_attachments(state) & kept,
            "_unsent_thinking_ids": self.unsent,
            "_unanswered_notes_rows": suppressed_notes_rows(state, self.preserved)
            | unanswered_rows(self.preserved),
            "_cached_input_tokens": 0,
            "_cached_output_tokens": 0,
        }


@dataclass(frozen=True)
class Summarizer:
    """The model that writes an agent's summaries, and how much it reads."""

    model: BaseChatModel
    #: The most tokens of history the summary model is sent.
    limit: int
    #: How tokens are counted, for the threshold and for trimming.
    counter: TokenCounter = count_tokens_tiktoken
    #: The model a summary falls back to, at the summary level: the turn's
    #: model when one picked for compaction writes it, else the model the
    #: turn falls back to.
    fallback: BaseChatModel | None = None
    #: The user's own skill names, which the reload note may send the agent
    #: back to even under a retired built-in's name.
    user_skills: frozenset[str] = frozenset()

    @classmethod
    def for_agent(cls, config: AgentConfig) -> Summarizer:
        """The client ``resolve_compaction_client`` picks, else the named
        compaction model at the summary level."""
        model = resolve_compaction_client(config)
        if model is None:
            name = (config.llm.compaction_name if config.llm else None) or ""
            model = _at_summary_level(name)
        return cls(
            model,
            config.compaction.token_threshold + _SUMMARY_HEADROOM,
            fallback=_summary_fallback(config),
            user_skills=frozenset(spec.name for spec in config.user_skills),
        )

    async def compact(
        self,
        messages: list[AnyMessage],
        view: list[AnyMessage],
        cutoff: int,
        *,
        backend: SandboxBackend | None,
        workspace_id: str | None,
        transcript: TranscriptTarget | None,
        fallback: Any | None,
        thread_id: str | None = None,
        notes_dir: str | None = None,
        window: Window,
        progress: Progress | None = None,
        own: Own | None = None,
    ) -> Compaction:
        """Summarize ``view`` before ``cutoff``, ``messages`` being the
        agent's checkpoint list (what ``window`` trimmed of it aside), by
        ``own`` when given (the agent's own model, from its own request),
        else from this model, then the next one (see ``_next``), then the
        server (see ``summarize``).

        The transcript is saved first: the summary cites its turn files and
        ends pointing at it only if that save lands and ``workspace_id``'s
        folder can read it. Admission holds the next turn for about the
        compaction timeout, so the save counts against it too. The notes in
        ``notes_dir``, the main agent's scratchpad, are listed while the
        summary is written, and the summary names them after its pointer.
        ``progress`` hears how much of the summary has been written.
        """
        to_summarize, preserved = partition_at_cutoff(view, cutoff)
        kept = keeps_turns(preserved)
        started = time.monotonic()
        budget = get_compaction_timeout()
        exported = await aexport_transcript(
            backend,
            transcript,
            messages,
            workspace_id=workspace_id,
            budget=budget,
            window=window,
        )
        turns = TranscriptTurns.of(exported, messages, window=window) if exported else None

        async def render(trimmed: list[AnyMessage]) -> list[AnyMessage]:
            trimmed = await aoffload_base64_content(backend, trimmed, thread_id=thread_id)
            return build_summary_request(summary_prompt("transcript", kept, notes=bool(notes_dir)), trimmed, turns)

        remaining = budget - (time.monotonic() - started)
        # The notes are listed beside the summary and within its budget, so
        # the listing never holds the compaction past it, and it is dropped
        # with a summary that fails.
        listing = asyncio.create_task(anotes_pointer(backend, notes_dir, remaining))
        server = functools.partial(
            server_summary,
            to_summarize,
            preserved,
            raw_messages=messages,
            turns=turns,
            window=window,
        )
        try:
            # The agent's model gets the summary model's share, leaving the
            # summarizer a real call when it writes none.
            summary = await _own_summary(own, to_summarize, kept, server, remaining * FIRST_SHARE)
            remaining = budget - (time.monotonic() - started)
            summary = summary or await awrite_summary(
                model=self.model,
                fallback=self._next(fallback),
                prepare=preparer(
                    to_summarize, limit=self.limit, counter=self.counter, render=render
                ),
                server=server,
                budget=remaining,
                progress=progress,
            )
            notes = await listing
        finally:
            listing.cancel()
        event = build_summary_event(
            summary,
            turns,
            raw_messages=messages,
            to_summarize=to_summarize,
            preserved=preserved,
            original_count=len(view),
            notes=notes,
            user_skills=self.user_skills,
        )
        return Compaction(event, summary, len(view), preserved)

    def _next(self, agent_model: Any) -> Any | None:
        """The model tried after this one: the agent's own when it is another
        model, as this summarizer's fallback when that is the same model at
        the summary level; else this summarizer's fallback, since the same
        model would most likely fail again."""
        if not is_other_model(self.model, agent_model):
            return self.fallback
        if self.fallback is not None and not is_other_model(agent_model, self.fallback):
            return self.fallback
        return agent_model


async def _own_summary(
    own: Own | None,
    to_summarize: list[AnyMessage],
    kept: bool,
    server: Callable[..., Summary],
    budget: float,
) -> Summary | None:
    """``own``'s summary within ``budget``, or None for the summarizer to
    write one: only a cancellation escapes, as in ``awrite_summary``. ``kept``
    says whether the summary leaves turns after it (``keeps_turns``). An
    answer only its provider can read takes ``server``'s digest as its text."""
    if own is None:
        return None
    try:
        async with asyncio.timeout(budget):
            written = await own(to_summarize, kept)
    except Exception as e:
        logger.warning("[Compaction] own model's summary failed (%s: %s)", type(e).__name__, e)
        return None
    if isinstance(written, str):
        return Summary(written, "self", to_summarize)
    text = readable_text(written.answer)
    if text is not None:
        covered = to_summarize
    else:
        digest = server(lead=PROVIDER_LEAD)
        text, covered = digest.text, digest.covered
    return Summary(
        text, "provider", covered, native=written.answer, origin=written.origin
    )


def build_summary_event(
    summary: Summary,
    turns: TranscriptTurns | None,
    *,
    raw_messages: list[AnyMessage],
    to_summarize: Sequence[AnyMessage],
    preserved: list[AnyMessage],
    original_count: int,
    notes: str = "",
    user_skills: frozenset[str] = frozenset(),
) -> CompactionEvent:
    """The event putting ``summary`` in place of ``to_summarize``, pointing
    at the transcript ``turns`` numbers when there is one.

    The skills whose instructions go with ``to_summarize`` are listed from the
    messages, not from the summary: a summarizer may drop a name, and the
    agent mid-procedure needs every one to reload.
    """
    # A summary of an earlier summary alone stands in for the same turns.
    earlier = summarized_span(to_summarize[0]) if to_summarize else None
    span = (
        summary_span(turns.turns, to_summarize, summary.covered, earlier) or earlier
        if turns is not None
        else None
    )
    message = build_summary_message(
        summary.text,
        turns.target if turns is not None else None,
        original_count,
        span=span,
        index=turns.index(span.last) if turns is not None and span is not None else (),
        skills=compacted_skills(to_summarize, preserved, user_skills),
        source=summary.source,
        notes=notes,
        native=summary.native,
        origin=summary.origin,
    )
    return build_compaction_event(
        raw_messages=raw_messages,
        preserved_messages=preserved,
        summary_message=message,
        file_path=turns.target.directory if turns is not None else None,
    )


async def compact_messages(
    messages: list[AnyMessage],
    state: Mapping[str, Any],
    config: AgentConfig,
    *,
    thread_id: str,
    backend: SandboxBackend | None = None,
    workspace_id: str | None = None,
    notes_dir: str | None = None,
) -> Compaction:
    """Manual /compact: summarize all but the tail automatic compaction
    keeps of the view, ``messages`` being the thread's checkpoint list and ``state`` its
    values. The user's main model is tried when the summary model fails.
    ``notes_dir`` is the thread's scratchpad notes folder when the
    scratchpad feature is on.

    Tier 1 has no part here: what it would trim is in the stretch the summary
    replaces.

    Raises:
        ValueError: If the view leaves nothing to summarize.
    """
    if not messages:
        raise ValueError("No messages to compact")
    ensure_message_ids(messages)
    transcript = transcript_target(backend, thread_id)
    view = await offload_view(
        messages,
        state,
        max_length=config.compaction.truncate_args_max_length,
        transcript=transcript,
        shell=shell_reads_transcripts(backend),
    )
    summarizer = Summarizer.for_agent(config)
    main = _main_client(config)
    settings = config.compaction
    # The cutoff counts attachments, on the server's loop, as the strip the
    # agent runs judges them for its model, as the automatic path does.
    await read_ahead(view)
    strip = MultimodalStripMiddleware.for_config(config)
    cost = target_cost(strip.receives(main), strip.pdf_page_tokens(main))
    cutoff = keep_cutoff(
        view,
        tokens=keep_tokens_for(
            main, settings.keep_tokens, settings.claude_keep_tokens, settings.token_threshold
        ),
        estimate=estimate_for(main, summarizer.counter, cost),
    )
    if cutoff <= 0:
        raise ValueError(NOTHING_TO_COMPACT)

    return await summarizer.compact(
        messages,
        view,
        cutoff,
        backend=backend,
        workspace_id=workspace_id,
        transcript=transcript,
        fallback=main,
        thread_id=thread_id,
        notes_dir=notes_dir,
        window=Window.of(state),
    )


def _at_summary_level(name: str) -> BaseChatModel:
    efforts = LLM.get_model_config().get_reasoning_efforts(name)
    return create_llm(name, reasoning_effort=summary_reasoning_effort(efforts))


def _summary_fallback(config: AgentConfig) -> BaseChatModel | None:
    """The role's client, else the turn's first fallback model built by name,
    except for a user on their own key or subscription, whose summary never
    moves to a platform key."""
    client = config.client_for_role("compaction_fallback")
    names = config.llm.fallbacks_for(config.llm.compaction_name) if config.llm else []
    if client is not None or not names or config.credential_source in (
        CredentialSource.OAUTH,
        CredentialSource.BYOK,
    ):
        return client
    try:
        return _at_summary_level(names[0])
    except Exception as e:
        logger.warning("[Compaction] no fallback model for summaries (%s)", type(e).__name__)
        return None


def _main_client(config: AgentConfig) -> BaseChatModel | None:
    try:
        return config.get_llm_client()
    except Exception as e:
        logger.warning("[Compaction] no fallback model for /compact (%s)", type(e).__name__)
        return None


async def offload_tool_args(
    messages: list[AnyMessage],
    state: Mapping[str, Any],
    settings: OffloadSettings,
    *,
    thread_id: str,
    backend: SandboxBackend | None = None,
    workspace_id: str | None = None,
) -> Offloads:
    """Manual /offload: the new (arg ids, read ids) to hide, chosen as the
    middleware's idle pass chooses them, without waiting for a pause.

    An argument is hidden only once the transcript holding it is saved and
    ``workspace_id``'s folder can read it, since that is then its only copy
    the agent can read.

    Raises:
        ValueError: If nothing new can be offloaded.
        RuntimeError: If only args were due and their transcript did not save.
    """
    if not messages:
        raise ValueError("No messages to offload")
    ensure_message_ids(messages)
    effective = get_effective_messages(messages, state.get("_summarization_event"))
    if len(effective) <= settings.keep_messages:
        raise ValueError(
            f"Not enough messages to offload. Have {len(effective)}, "
            f"need more than {settings.keep_messages} to have any candidates."
        )

    arg_ids, read_ids = select_offloads(effective, settings, recorded_offloads(state))
    if arg_ids:
        transcript = transcript_target(backend, thread_id)
        if transcript is None:
            arg_ids = set()
        elif await aexport_transcript(
            backend,
            transcript,
            messages,
            workspace_id=workspace_id,
            window=Window.of(state),
        ) is None:
            if not read_ids:
                # A failure to retry, not "nothing to offload": the caller
                # turns this into a 500 and records nothing.
                raise RuntimeError("Could not save the transcript the arguments are kept in")
            arg_ids = set()
    if not arg_ids and not read_ids:
        raise ValueError("Nothing to offload at the current threshold")
    return arg_ids, read_ids
