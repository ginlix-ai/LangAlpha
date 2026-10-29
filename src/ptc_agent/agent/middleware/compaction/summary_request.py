"""What the summarizer is sent: its instructions and the history to condense.

Shared by the automatic path (the middleware) and manual /compact, so both
send the same request: the instructions in the system channel, the rendered
history in one user message, and, where the agent has a transcript, an index
of its turns and each turn headed by the transcript file that holds it.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from typing import Literal, cast

from langchain_core.messages import (
    AIMessage,
    AnyMessage,
    HumanMessage,
    SystemMessage,
    ToolMessage,
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
    human_kind,
    is_run_boundary_message,
    is_steering_trigger,
    is_summary_message,
)
from ptc_agent.agent.transcript.pointer import TranscriptTurns
from ptc_agent.agent.transcript.store import segment_file
from src.llms.reasoning_payload import strip_reasoning

logger = logging.getLogger(__name__)

_COMPACTION_USER_NUDGE = "Generate the summary now."

# What a summary holds, whoever writes it.
_SUMMARY_BODY = """<objective>
Extract the most important context to preserve continuity and prevent
repeating completed work.
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
- Record a reading from a live feed, such as a price or a market status, with the time it was taken, as its value at that time rather than the current one
</instructions>

<output_format>
Respond ONLY with the extracted context. Do not include preamble or commentary.

Begin with a Brief 1-2 sentence overview of the session and current goal.
Make sure you maintain the user original query and goal.

Then organize naturally using markdown headers.
Write as if briefing a colleague who needs to continue your work without repeating what's done.
</output_format>"""

def keeps_turns(preserved: Sequence[AnyMessage]) -> bool:
    """Whether a summary leaves a turn word for word after it: an answer or a
    tool result, not only the user's newest message, which a summary that
    keeps nothing hands on as it is."""
    return any(isinstance(m, (AIMessage, ToolMessage)) for m in preserved)


# The transcript summarizer's prompt. Instructions only: the conversation
# history is delivered in a separate HumanMessage so the system channel stays
# bounded and cacheable, and so BaseChatModel.format() doesn't try to interpret
# message content as further format placeholders.
def _transcript_prompt(after: str) -> str:
    return (
        f"""<role>
Conversation Context Summarizer
</role>

<context>
You're nearing your input token limit. The conversation history in the user
message will be replaced with the context you extract. This is critical -
ensure you capture all important information so you can continue the work
without losing progress.

{after}
</context>

"""
        + _SUMMARY_BODY
    )


# The prompt for the model that holds the conversation, which summarizes it in
# place (a provider's own compaction): no role of its own, since it is still
# the agent, and the history is the conversation above, not a user message.
def _native_prompt(after: str) -> str:
    return (
        f"""<context>
You're nearing your context limit. Everything above will be replaced by the
summary you write now, so capture all you need to continue the work without
losing progress.

{after}
</context>

"""
        + _SUMMARY_BODY
    )


# The prompt for the model whose provider compacted the conversation into a
# summary only that provider can read, when the user moves the thread to a
# model of another provider. Sent as the last user message after that summary,
# so the provider reads the summary itself, from its cache.
def _handoff_prompt(after: str) -> str:
    return (
        f"""<context>
This conversation is moving to a model from another provider, which cannot
read the compacted summary above. Write it out in full for that model: what
you write replaces the compacted summary for it, so capture all it needs to
continue the work without losing progress. This request comes from the
system, not the user, and is not part of the work: leave it out of what you
write.

{after}
</context>

"""
        + _SUMMARY_BODY
    )


_NO_TAIL_STATE = """What you write is the only record of where the work stands, so record its
current state exactly: the last step completed, what was in progress and how
far it got, any tool result or decision still pending, any question to the
user still open, and the user's latest request in their own words."""


_NO_TAIL_NOTES = """Your checkpoint notes are the one exception: where a note already records one
of these details, cite it by the note's file name, and its section if the note
has sections, instead of restating it. Then name the details to read back from
each note before continuing."""


def _family(prompt: Callable[[str], str], kept: str, no_tail: str) -> tuple[str, str, str]:
    """A family's prompt for a summary that keeps turns after it, for one that
    keeps none (``keeps_turns``), and for one that keeps none in a thread
    with checkpoint notes, which keep some of that state already."""
    state = f"{no_tail}\n{_NO_TAIL_STATE}"
    return prompt(kept), prompt(state), prompt(f"{state}\n{_NO_TAIL_NOTES}")


_SUMMARY_OPEN, _SUMMARY_END = "<summary>", "</summary>"

# The rules added for the model that holds the conversation writing the
# summary as text, sent as the last user message of its own request. Reading
# its own reasoning, it takes steps it only planned for done; with its tools
# bound it may call one instead; and some providers stop mid-word under a
# normal stop reason, which the closing line is there to show.
_SELF_SUMMARY_RULES = (
    "\n\nCount as done only what a tool result above shows was done: a step you "
    "planned in your reasoning that no tool carried out is not done, and a file "
    "no tool wrote does not exist. Write the summary as text; do not call a tool."
    f"\n\nEnd the summary with the line `{_SUMMARY_END}` on its own."
)

SummaryKind = Literal["transcript", "native", "self", "handoff"]

_NATIVE = _family(
    _native_prompt,
    """The most recent messages are not above: they stay in context, word for word,
right after your summary, so the work may have moved on from where this
conversation ends. Describe unfinished work as where it stood at that point,
not as still to do.""",
    """Nothing above stays in context: the conversation goes on from your summary,
so describe the work as it stands where this conversation ends.""",
)

_PROMPTS: dict[SummaryKind, tuple[str, str, str]] = {
    "transcript": _family(
        _transcript_prompt,
        """The most recent messages are not in this history: they stay in context, word
for word, right after your summary, so the work may have moved on from where
this history ends. Describe unfinished work as where it stood at that point,
not as still to do.""",
        """Nothing after this history stays in context: the conversation goes on from
your summary, so describe the work as it stands where this history ends.""",
    ),
    "native": _NATIVE,
    "self": tuple(prompt + _SELF_SUMMARY_RULES for prompt in _NATIVE),
    "handoff": _family(
        _handoff_prompt,
        """The messages that came after the compacted summary stay in context, word for
word, right after yours, so the work may have moved on from where it ends.
Describe unfinished work as where it stood at that point, not as still to do.""",
        """Nothing came after the compacted summary, so the conversation goes on from
what you write: describe the work as it stands where the summary ends.""",
    ),
}


def summary_prompt(kind: SummaryKind, keeps: bool, notes: bool = False) -> str:
    """The instructions for a summary written by ``kind``: the transcript
    summarizer, the agent's provider compacting in place, the agent's model
    writing it as text, or that model writing out a summary only its provider
    can read. ``keeps`` says whether turns stay after it (``keeps_turns``), and
    ``notes`` whether the thread keeps checkpoint notes (the scratchpad)."""
    kept, no_tail, with_notes = _PROMPTS[kind]
    if keeps:
        return kept
    return with_notes if notes else no_tail


DEFAULT_SUMMARY_PROMPT = summary_prompt("transcript", True)


def finished_summary(text: str) -> str | None:
    """The summary a model wrote to the self-summary prompt, without its tags
    or anything it said around them, or None when it did not reach the
    closing line."""
    body, closed, after = text.rpartition(_SUMMARY_END)
    # A tag mid-line with text after it is quoted, by a summary cut off later.
    if not closed or (after.strip() and body.rsplit("\n", 1)[-1].strip()):
        return None
    _, opened, inside = body.partition(_SUMMARY_OPEN)
    return (inside if opened else body).strip() or None


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
    on, and notes reminders, which the summary itself answers. Change rows
    are relabelled as ``System:`` so what they say survives without being
    attributed to anyone. An orchestrator's notice of a
    background task is relabelled the same way. Its steering trigger is
    dropped: it says only that someone wrote more, which the steering
    message after it shows, and the summarizer took it, under either label,
    for a request still to be answered.
    """
    out: list[AnyMessage] = []
    for message in messages:
        if isinstance(message, HumanMessage) and human_kind(message) == "orchestrator":
            if not is_steering_trigger(message.content):
                out.append(SystemMessage(content=message.content))
            continue
        update = runtime_update_from_message(message)
        if update is None:
            out.append(message)
        elif update.kind not in NON_CHANGE_ROW_KINDS:
            out.append(SystemMessage(content=update.text))
    return out
