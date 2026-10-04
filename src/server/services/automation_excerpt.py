"""An automation run's answer, and its head flattened to one line.

The automation list and the run feed lead with the head, so the answer is
read from the checkpoint once, as the run completes, and the head kept on the
execution row. A run that delivers through the messaging service hands the
chats its agent didn't reach the result it sent the others, else the answer.
"""

import html
import logging
import re
from typing import Any, NamedTuple, Optional

from langchain_core.messages import AIMessage, ToolMessage

logger = logging.getLogger(__name__)

# Markup is stripped from this raw head, so it leaves room for links and
# tables to collapse and still fill the excerpt, and a long answer never
# reaches the patterns whole.
_EXCERPT_RAW_CHARS = 800
_EXCERPT_CHARS = 320

# Line-anchored rules match within a line, [ \t] rather than \s, so a rule
# never reaches across a line break into the next line's markup.
_MD_RULES: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"^[ \t]*(```|~~~).*?(?:^[ \t]*\1[^\n]*$|\Z)", re.M | re.S), " "),
    (re.compile(r"</?[A-Za-z][^>]*>"), " "),
    (re.compile(r"!\[([^\]]*)\]\([^)]*\)"), r"\1"),
    (re.compile(r"\[([^\]]+)\](?:\([^)]*\)|\[[^\]]*\])"), r"\1"),
    (
        re.compile(
            r"^[ \t]*\|?[ \t]*:?-{3,}:?[ \t]*(?:\|[ \t]*:?-{3,}:?[ \t]*)*\|?[ \t]*$",
            re.M,
        ),
        " ",
    ),
    (re.compile(r"^[ \t]*([-*_])(?:[ \t]*\1){2,}[ \t]*$", re.M), " "),
    (re.compile(r"^[ \t]{0,3}#{1,6}[ \t]+|[ \t]+#+[ \t]*$", re.M), ""),
    (re.compile(r"^[ \t]*>[> \t]*", re.M), ""),
    (re.compile(r"^[ \t]*[-*+][ \t]+", re.M), ""),
    (re.compile(r"`([^`]*)`"), r"\1"),
    (re.compile(r"(\*\*|__|~~)(?=\S)(.+?)(?<=\S)\1"), r"\2"),
    (re.compile(r"(?<!\w)([*_])(?=\S)(.+?)(?<=\S)\1(?!\w)"), r"\2"),
    (re.compile(r"[ \t]*\|[ \t]*"), " "),
    (re.compile(r"\s+"), " "),
]


def plain_excerpt(answer: str) -> str:
    """Flatten an answer's markdown head to one line for a list card.

    Fenced code is dropped rather than flattened: a card reads the prose, and
    a code body collapsed onto one line is noise.
    """
    truncated = len(answer) > _EXCERPT_RAW_CHARS
    # Unescaped first, so an escaped tag is stripped like a literal one
    # rather than turning into markup after the stripping.
    text = html.unescape(answer[:_EXCERPT_RAW_CHARS])
    for pattern, repl in _MD_RULES:
        text = pattern.sub(repl, text)
    text = text.strip()
    if len(text) <= _EXCERPT_CHARS and not truncated:
        return text
    cut = text[:_EXCERPT_CHARS]
    space = cut.rfind(" ")
    if space > _EXCERPT_CHARS // 2:
        cut = cut[:space]
    return cut.rstrip(" ,;:-") + "…" if cut else ""


class RunAnswer(NamedTuple):
    """What a completed run said: ``text``, the last text its agent wrote,
    and ``sent``, the text of its last ``send_message`` that went out."""

    text: Optional[str] = None
    sent: Optional[str] = None


# A send the messaging service took: all of it, or its text without some of
# its files.
_LANDED = frozenset({"sent", "partial"})


def _landed(message: ToolMessage) -> bool:
    """Whether a ``send_message`` result says the message went out. The
    status is the delivery artifact's, else the first line of the text the
    model read; a tool that raised sent nothing."""
    if getattr(message, "status", None) == "error":
        return False
    artifact = getattr(message, "artifact", None)
    if isinstance(artifact, dict) and isinstance(artifact.get("status"), str):
        return artifact["status"] in _LANDED
    content = message.content if isinstance(message.content, str) else ""
    label, _, status = content.split("\n", 1)[0].partition(":")
    return label.strip() == "status" and status.strip() in _LANDED


def _last_sent_text(messages: list[Any]) -> Optional[str]:
    """The text of the last ``send_message`` call among ``messages`` whose
    result says it went out and that had text to send; a call that sent only
    files is passed over."""
    landed = {
        m.tool_call_id for m in messages if isinstance(m, ToolMessage) and _landed(m)
    }
    for message in reversed(messages):
        if not isinstance(message, AIMessage):
            continue
        for call in reversed(message.tool_calls or []):
            if call.get("name") != "send_message" or call.get("id") not in landed:
                continue
            args = call.get("args")
            text = args.get("text") if isinstance(args, dict) else None
            if isinstance(text, str) and text.strip():
                return text
    return None


async def read_run_answer(thread_id: str, run_id: str) -> RunAnswer:
    """What the run said: the last text its agent wrote, and the text of its
    last send that went out. Both are None when it has none.

    A run that sends its result writes something else last, such as a line
    saying where it sent it, so the send is the result a chat it didn't
    reach is handed.

    The run's own turn is read, found by its run: the thread can take another
    turn before the settle runs, and the newest turn is then that one's, not
    the run's. Only that turn is materialized. Background subagents
    checkpoint under their own ``task:`` namespaces, so these messages are
    the agent's own. The answer only decorates a list and stands in for a
    delivery the agent didn't make: a failed read leaves none rather than
    failing the execution.
    """
    from src.server.services.history.projector import split_content_blocks
    from src.server.services.history.reader import CheckpointHistoryReader

    try:
        turn = await CheckpointHistoryReader.get_instance().aget_run_turn(
            thread_id, run_id
        )
    except Exception as e:
        logger.warning(
            f"[AUTOMATION] Excerpt read failed: thread_id={thread_id} "
            f"run_id={run_id}: {e}"
        )
        return RunAnswer()
    if turn is None:
        return RunAnswer()
    text = None
    for message in reversed(turn.messages):
        if not isinstance(message, AIMessage):
            continue
        written, _, _ = split_content_blocks(message.content)
        if written and written.strip():
            text = written
            break
    return RunAnswer(text=text, sent=_last_sent_text(turn.messages))
