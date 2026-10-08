"""What the summarizer is sent: its instructions and the history to condense.

Shared by the automatic path (the middleware) and manual /compact, so both
send the same request: the instructions in the system channel, the rendered
history in one user message, and, where the agent has a transcript, an index
of its turns and each turn headed by the transcript file that holds it.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from typing import cast

from langchain_core.messages import (
    AIMessage,
    AnyMessage,
    HumanMessage,
    SystemMessage,
    get_buffer_string,
)
from langchain_core.messages.utils import trim_messages

from ptc_agent.agent.middleware.compaction.utils import (
    parse_summary_message,
    summarized_span,
)
from ptc_agent.agent.middleware.runtime_context.durable import (
    runtime_update_from_message,
)
from ptc_agent.agent.middleware.runtime_context.turn import NON_CHANGE_ROW_KINDS
from ptc_agent.agent.transcript.classify import (
    STEERING_TRIGGER,
    human_kind,
    is_run_boundary_message,
    is_summary_message,
)
from ptc_agent.agent.transcript.pointer import TranscriptTurns
from ptc_agent.agent.transcript.store import segment_file
from src.llms.reasoning_payload import strip_reasoning

logger = logging.getLogger(__name__)

_COMPACTION_USER_NUDGE = "Generate the summary now."

# Summarization prompt. Instructions only: the conversation
# history is delivered in a separate HumanMessage so the system channel stays
# bounded and cacheable, and so BaseChatModel.format() doesn't try to interpret
# message content as further format placeholders.
DEFAULT_SUMMARY_PROMPT = """<role>
Conversation Context Summarizer
</role>

<context>
You're nearing your input token limit. The conversation history in the user
message will be replaced with the context you extract. This is critical -
ensure you capture all important information so you can continue the work
without losing progress.

The most recent messages are not in this history: they stay in context, word
for word, right after your summary, so the work may have moved on from where
this history ends. Describe unfinished work as where it stood at that point,
not as still to do.
</context>

<objective>
Extract the most important context to preserve continuity and prevent
repeating completed work. Think deeply about what information is essential to
achieving the user's overall goal.
</objective>

<instructions>
Create a natural, readable summary that captures everything needed to continue the work.
Write in the SAME LANGUAGE as the user's queries.
Use your judgment on structure - the categories below are guidelines, not rigid templates.

Key information to capture:

1. **Current Query**: What is the user asking? Include the verbatim question, relevant tickers/entities, and scope.

2. **Progress**: What has been done and what remains? List completed steps with outcomes, current work, and pending tasks.

3. **Key Findings**: All critical discoveries with their sources:
   - Data points with exact values: prices, ratios, growth rates (always include source)
   - Observations and patterns identified
   - Conclusions reached from analysis
   - URLs crawled, APIs used, files created

4. **Decisions and Instructions**: Methodology choices that affect ongoing work, and every instruction the user gave beyond the task itself, such as how to work, what to deliver or how to present it. Keep each in the user's words, whichever turn it came from, unless a later message replaced it.

5. **Skills and Procedures**: If the work follows a skill (its instructions arrived in a `<loaded-skill name="...">` block, a LoadSkill result, or a SKILL.md that was read) or another multi-step procedure, name it exactly as it was loaded, the stage reached, and the next step. Do not list or paraphrase its steps, not even in short: the skill is loaded again to read them.

6. **Query History** (for multi-turn sessions only): Previous queries in chronological order with their outcomes.

Guidelines:
- Preserve ALL numerical data exactly as discovered
- Include source/citation for each data point
- Do not copy tool-output markers, notices that an argument or result was cut, or commands for reading files (a jq command, a Read call); state what they led to instead
- Omit categories that have no content
- Be concise but comprehensive
- Use natural prose or bullet points as appropriate
</instructions>

<output_format>
Respond ONLY with the extracted context. Do not include preamble or commentary.

Begin with a Brief 1-2 sentence overview of the session and current goal.
Make sure you maintain the user original query and goal.

Then organize naturally using markdown headers.
Write as if briefing a colleague who needs to continue your work without repeating what's done.
</output_format>"""

# Appended only when the history carries transcript markers: without a
# transcript there is no file to cite, and an instruction to cite one would
# invite made-up names.
_CITATION_INSTRUCTIONS = """

<transcript_citations>
The history is divided by lines such as `[transcript: {example}]`. Each names
the transcript file that keeps the full record of the messages after it,
including tool arguments and results this summary cannot hold. Next to each
finding, data point and decision, cite the file it came from in parentheses,
for example ({example}), so the detail can be read back from exactly that file.
The files keep the full record, but they are read only to recover a specific
detail, so write the summary complete enough that this is rarely necessary.
A block headed `[earlier summary ...]` stands in for the history before it;
the files it cites still exist, so keep its citations as they are. The
transcript index lists each {unit}'s file with the request that opened it,
including {unit}s the history no longer shows in full. Cite only file names
that appear in those lines, the earlier summary or the index.
</transcript_citations>"""

# An earlier summary's own note (the pointer, the index, the skills to
# reload) is left out of what the summarizer reads: the new summary gets a
# fresh one, and a copied old one would contradict it.
_EARLIER_SUMMARY = "[earlier summary]"
_EARLIER_SUMMARY_OF = "[earlier summary of {files}]"


def trim_for_summary(
    messages: list[AnyMessage],
    max_tokens: int,
    token_counter: Callable[[list[AnyMessage]], int],
) -> list[AnyMessage]:
    """The newest of ``messages`` that fit ``max_tokens``, for the summarizer.

    An earlier summary at the head is kept whole and the rest trimmed to what
    is left: it is the stretch's one record of everything before it, while
    each turn after it is also in the transcript. One too large to keep along
    with a turn after it is trimmed like any message. Messages are counted
    without their reasoning, which the rendered history never carries: counted
    in, a thinking model's turns are trimmed away while they still fit.

    Empty when not even the newest message fits cut down, as one line longer
    than ``max_tokens`` cannot be: a request over the limit is never sent.
    """

    def count(batch: list[AnyMessage]) -> int:
        return token_counter(_as_rendered(batch))

    if count(messages) <= max_tokens:
        return messages
    if is_summary_message(messages[0]):
        head = messages[:1]
        trimmed = _newest_with_request(messages[1:], max_tokens - count(head), count)
        if trimmed:
            return [*head, *trimmed]
    trimmed = _newest_with_request(messages, max_tokens, count)
    if not trimmed:
        logger.warning(
            "[Compaction] not even the newest message fits %d summary tokens", max_tokens
        )
    return trimmed


def _as_rendered(messages: list[AnyMessage]) -> list[AnyMessage]:
    """``messages`` without the reasoning ``get_buffer_string`` leaves out."""
    rendered: list[AnyMessage] = []
    for message in messages:
        kept = strip_reasoning(message) if isinstance(message, AIMessage) else message
        if kept is not None:
            rendered.append(kept)
    return rendered


def _newest_with_request(
    messages: list[AnyMessage],
    max_tokens: int,
    token_counter: Callable[[list[AnyMessage]], int],
) -> list[AnyMessage]:
    """The newest messages that fit, led by the request of the newest turn
    when that turn does not fit whole: the steps between are in the
    transcript, but the summary is the request's one record once it replaces
    the history."""
    if max_tokens <= 0:
        return []
    trimmed = _newest(messages, max_tokens, token_counter)
    at = next(
        (i for i in range(len(messages) - 1, -1, -1) if is_run_boundary_message(messages[i])),
        None,
    )
    if not trimmed or at is None or any(m is messages[at] for m in trimmed):
        return trimmed
    request = messages[at]
    room = max_tokens - token_counter([request])
    steps = _newest(messages[at + 1 :], room, token_counter) if room > 0 else []
    return [request, *steps] if steps else trimmed


def _newest(
    messages: list[AnyMessage],
    max_tokens: int,
    token_counter: Callable[[list[AnyMessage]], int],
) -> list[AnyMessage]:
    """The newest messages that fit, opening on a human message where one
    fits. One long turn has none after its request, and its newest steps
    still say where the work stood."""
    for start_on in ("human", None):
        trimmed = cast(
            "list[AnyMessage]",
            trim_messages(
                messages,
                max_tokens=max_tokens,
                token_counter=token_counter,
                start_on=start_on,
                strategy="last",
                allow_partial=True,
                include_system=True,
            ),
        )
        if trimmed:
            return trimmed
    return []


def build_summary_request(
    summary_prompt: str,
    messages: list[AnyMessage],
    turns: TranscriptTurns | None = None,
) -> list[AnyMessage]:
    """System = instructions, Human = nudge + rendered history.

    Splitting the two channels keeps the system prompt small and cacheable,
    lets Codex OAuth populate its ``instructions`` field cleanly, and avoids
    Python repr inflation by rendering messages via ``get_buffer_string``.
    """
    from src.llms.api_call import create_messages

    history = _render_history(messages, turns)
    user_prompt = f"{_COMPACTION_USER_NUDGE}\n\n<messages>\n{history}\n</messages>"
    if turns is not None:
        last = turns.last(messages)
        if last is not None:
            index = "\n".join(turns.index(last))
            user_prompt = (
                f"{_COMPACTION_USER_NUDGE}\n\n<transcript_index>\n{index}\n"
                f"</transcript_index>\n\n<messages>\n{history}\n</messages>"
            )
        unit = turns.target.unit
        summary_prompt += _CITATION_INSTRUCTIONS.format(
            example=segment_file(unit, 7), unit=unit
        )
    return create_messages(system_prompt=summary_prompt, user_prompt=user_prompt)


def _render_history(messages: Sequence[AnyMessage], turns: TranscriptTurns | None) -> str:
    """The history as text, each turn headed by its transcript file.

    An earlier summary is a block of its own holding its summary text alone.
    Any other message the transcript does not number stays under the heading
    before it, or above the first one.
    """
    blocks: list[str] = []
    heading: str | None = None
    group: list[AnyMessage] = []

    def close() -> None:
        text = get_buffer_string(_summarizable(group))
        if text:
            blocks.append(f"{heading}\n{text}" if heading else text)
        group.clear()

    for message in messages:
        if is_summary_message(message):
            close()
            heading = None
            blocks.append(f"{_earlier_heading(message, turns)}\n{parse_summary_message(message)}")
            continue
        name = turns.file(message.id) if turns is not None else None
        if name is not None and f"[transcript: {name}]" != heading:
            close()
            heading = f"[transcript: {name}]"
        group.append(message)
    close()
    return "\n".join(blocks)


def _earlier_heading(message: AnyMessage, turns: TranscriptTurns | None) -> str:
    span = summarized_span(message) if turns is not None else None
    if turns is None or span is None:
        return _EARLIER_SUMMARY
    first, last = (segment_file(turns.target.unit, n) for n in (span.first, span.last))
    return _EARLIER_SUMMARY_OF.format(files=first if first == last else f"{first} to {last}")


def _summarizable(messages: list[AnyMessage]) -> list[AnyMessage]:
    """History as the summarizer should read it: harness rows are not the user.

    A runtime-context row is persisted as a ``HumanMessage`` because that is
    the one role every provider accepts anywhere, but ``get_buffer_string``
    would render it as ``Human:`` and the summarizer would take a time stamp
    or a file diff for a request. Turn anchors are dropped, since the block
    they annotate is rebuilt at compaction, and so are subagent-switch
    notices, which restate themselves after a compaction while the switch is
    off and would otherwise leave a summary saying it is off once it is back
    on. Change rows are relabelled as ``System:`` so what they say survives
    without being attributed to anyone. An orchestrator's notice of a
    background task is relabelled the same way. Its steering trigger is
    dropped: it says only that the user wrote more, which the steering
    message after it shows, and the summarizer took it, under either label,
    for a request still to be answered.
    """
    out: list[AnyMessage] = []
    for message in messages:
        if isinstance(message, HumanMessage) and human_kind(message) == "orchestrator":
            if message.content != STEERING_TRIGGER:
                out.append(SystemMessage(content=message.content))
            continue
        update = runtime_update_from_message(message)
        if update is None:
            out.append(message)
        elif update.kind not in NON_CHANGE_ROW_KINDS:
            out.append(SystemMessage(content=update.text))
    return out
