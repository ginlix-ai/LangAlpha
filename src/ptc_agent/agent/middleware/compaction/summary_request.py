"""What the summarizer is sent: its instructions and the history to condense.

Shared by the automatic path (the middleware) and manual /compact, so both
send the same request: the instructions in the system channel, the rendered
history in one user message, and, where the agent has a transcript, each turn
headed by the transcript file that holds it.
"""

from __future__ import annotations

from langchain_core.messages import AnyMessage, SystemMessage, get_buffer_string

from ptc_agent.agent.middleware.runtime_context.durable import (
    runtime_update_from_message,
)
from ptc_agent.agent.middleware.runtime_context.turn import NON_CHANGE_ROW_KINDS
from ptc_agent.agent.transcript.pointer import TranscriptTurns
from ptc_agent.agent.transcript.store import segment_file

_COMPACTION_USER_NUDGE = "Generate the summary now."

# Financial research summarization prompt. Instructions only: the conversation
# history is delivered in a separate HumanMessage so the system channel stays
# bounded and cacheable, and so BaseChatModel.format() doesn't try to interpret
# message content as further format placeholders.
DEFAULT_SUMMARY_PROMPT = """<role>
Financial Research Context Summarizer
</role>

<context>
You're nearing your input token limit. The conversation history in the user
message will be replaced with the context you extract. This is critical -
ensure you capture all important information so you can continue the research
without losing progress.
</context>

<objective>
Extract the most important context to preserve research continuity and prevent
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

4. **Decisions**: Any methodology choices or user preferences that affect ongoing work.

5. **Skills and Procedures**: If the work follows a skill (its instructions arrived in a `<loaded-skill name="...">` block, a LoadSkill result, or a SKILL.md that was read) or another multi-step procedure, name it exactly as it was loaded, the stage reached, and the next step.

6. **Query History** (for multi-turn sessions only): Previous queries in chronological order with their outcomes.

Guidelines:
- Preserve ALL numerical data exactly as discovered
- Include source/citation for each data point
- Omit categories that have no content
- Be concise but comprehensive
- Use natural prose or bullet points as appropriate
</instructions>

<output_format>
Respond ONLY with the extracted context. Do not include preamble or commentary.

Begin with a Brief 1-2 sentence overview of the research session and current goal.
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
Cite only file names that appear in those lines.
</transcript_citations>"""


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
        example = segment_file(turns.target.unit, 7)
        summary_prompt += _CITATION_INSTRUCTIONS.format(example=example)
    return create_messages(system_prompt=summary_prompt, user_prompt=user_prompt)


def _render_history(messages: list[AnyMessage], turns: TranscriptTurns | None) -> str:
    """The history as text, each turn headed by its transcript file.

    A message the transcript does not number (the previous summary) stays
    under the heading before it, or above the first one.
    """
    if turns is None:
        return get_buffer_string(_summarizable(messages))
    groups: list[tuple[str | None, list[AnyMessage]]] = [(None, [])]
    for message in messages:
        name = turns.file(message.id)
        if name is not None and name != groups[-1][0]:
            groups.append((name, []))
        groups[-1][1].append(message)
    blocks: list[str] = []
    for name, group in groups:
        text = get_buffer_string(_summarizable(group))
        if not text:
            continue
        blocks.append(f"[transcript: {name}]\n{text}" if name else text)
    return "\n".join(blocks)


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
    without being attributed to anyone.
    """
    out: list[AnyMessage] = []
    for message in messages:
        update = runtime_update_from_message(message)
        if update is None:
            out.append(message)
        elif update.kind not in NON_CHANGE_ROW_KINDS:
            out.append(SystemMessage(content=update.text))
    return out
