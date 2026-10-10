"""One compaction, whichever path runs it.

The middleware compacts when the context nears its limit, manual /compact
when the user asks. Both summarize the view the model is sent, through the
same chain, into the same event and state update, so a thread reads the same
after either. Manual /offload chooses Tier 1 cuts the way the middleware's
idle pass does.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from langchain_core.messages import AnyMessage

from langchain.chat_models import BaseChatModel

from src.config.settings import get_compaction_timeout
from src.llms import get_llm_by_type, maybe_disable_streaming

from ptc_agent.agent.state import ensure_message_ids
from ptc_agent.agent.middleware.compaction.types import (
    CompactionEvent,
    OffloadSettings,
    TokenCounter,
)
from ptc_agent.agent.middleware.compaction.summary_request import (
    DEFAULT_SUMMARY_PROMPT,
    build_summary_request,
)
from ptc_agent.agent.middleware.compaction.summarize import (
    Summary,
    awrite_summary,
    preparer,
    server_summary,
)
from ptc_agent.agent.middleware.compaction.utils import (
    build_compaction_event,
    build_summary_message,
    compacted_skills,
    count_tokens_tiktoken,
    find_group_safe_cutoff,
    get_effective_messages,
    partition_at_cutoff,
    summarized_span,
)
from ptc_agent.agent.middleware.compaction.model import resolve_compaction_client
from ptc_agent.agent.middleware.compaction.notes import anotes_pointer
from ptc_agent.agent.middleware.compaction.offloading import (
    Offloads,
    aoffload_base64_content,
    offload_view,
    recorded_offloads,
    select_offloads,
    tool_call_ids,
)
from ptc_agent.agent.transcript import TranscriptTarget, Window
from ptc_agent.agent.transcript.pointer import (
    TranscriptTurns,
    aexport_transcript,
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
        return [self.event["summary_message"], *self.preserved]

    def update(self, state: Mapping[str, Any]) -> dict[str, Any]:
        """The state update that installs it over ``state``.

        Summarized calls never reach the model again, so their recorded
        offload ids are dropped. The cached token counts measured the view
        before it: left in place, the next call would read them as still over
        the threshold and compact again at once.
        """
        live = tool_call_ids(self.preserved)
        args, reads = recorded_offloads(state)
        return {
            "_summarization_event": self.event,
            "_offloaded_tool_call_ids": args & live,
            "_offloaded_read_result_ids": reads & live,
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

    @classmethod
    def for_agent(cls, config: AgentConfig) -> Summarizer:
        """The client ``resolve_compaction_client`` picks, else the named
        compaction model, with streaming off so its chunks never stream as
        the answer."""
        model = resolve_compaction_client(config) or get_llm_by_type(
            (config.llm.compaction_name if config.llm else None) or ""
        )
        maybe_disable_streaming(model)
        return cls(model, config.compaction.token_threshold + _SUMMARY_HEADROOM)

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
    ) -> Compaction:
        """Summarize ``view`` before ``cutoff``, ``messages`` being the
        agent's checkpoint list (what ``window`` trimmed of it aside), from
        this model, ``fallback`` or the server (see ``summarize``).

        The transcript is saved first: the summary cites its turn files and
        ends pointing at it only if that save lands and ``workspace_id``'s
        folder can read it. Admission holds the next turn for about the
        compaction timeout, so the save counts against it too. The notes in
        ``notes_dir``, the main agent's scratchpad, are listed while the
        summary is written, and the summary names them after its pointer.
        """
        to_summarize, preserved = partition_at_cutoff(view, cutoff)
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
            return build_summary_request(DEFAULT_SUMMARY_PROMPT, trimmed, turns)

        remaining = budget - (time.monotonic() - started)
        # The notes are listed beside the summary and within its budget, so
        # the listing never holds the compaction past it, and it is dropped
        # with a summary that fails.
        listing = asyncio.create_task(anotes_pointer(backend, notes_dir, remaining))
        try:
            summary = await awrite_summary(
                model=self.model,
                fallback=fallback,
                prepare=preparer(
                    to_summarize, limit=self.limit, counter=self.counter, render=render
                ),
                server=lambda: server_summary(
                    to_summarize,
                    preserved,
                    raw_messages=messages,
                    turns=turns,
                    window=window,
                ),
                budget=remaining,
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
            skill_files=backend is not None,
            notes=notes,
        )
        return Compaction(event, summary, len(view), preserved)


def build_summary_event(
    summary: Summary,
    turns: TranscriptTurns | None,
    *,
    raw_messages: list[AnyMessage],
    to_summarize: Sequence[AnyMessage],
    preserved: list[AnyMessage],
    original_count: int,
    skill_files: bool = False,
    notes: str = "",
) -> CompactionEvent:
    """The event putting ``summary`` in place of ``to_summarize``, pointing
    at the transcript ``turns`` numbers when there is one.

    The skills whose instructions go with ``to_summarize`` are listed from the
    messages, not from the summary: a summarizer may drop a name, and the
    agent mid-procedure needs every one to reload. ``skill_files`` says how
    the agent reloads one (see ``skill_reload_note``).
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
        skills=compacted_skills(to_summarize, preserved),
        skill_files=skill_files,
        source=summary.source,
        notes=notes,
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
    keep_messages: int = 5,
    backend: SandboxBackend | None = None,
    workspace_id: str | None = None,
    notes_dir: str | None = None,
) -> Compaction:
    """Manual /compact: summarize all but the last ``keep_messages`` of the
    view, ``messages`` being the thread's checkpoint list and ``state`` its
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
    view = offload_view(
        messages,
        state,
        max_length=config.compaction.truncate_args_max_length,
        transcript=transcript,
    )
    if len(view) <= keep_messages:
        raise ValueError(
            f"Not enough messages to compact. Have {len(view)}, "
            f"need more than {keep_messages} to preserve."
        )
    cutoff = find_group_safe_cutoff(view, len(view) - keep_messages)
    if cutoff <= 0:
        raise ValueError("Cannot determine valid cutoff point for compaction")

    return await Summarizer.for_agent(config).compact(
        messages,
        view,
        cutoff,
        backend=backend,
        workspace_id=workspace_id,
        transcript=transcript,
        fallback=_main_client(config),
        thread_id=thread_id,
        notes_dir=notes_dir,
        window=Window.of(state),
    )


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
