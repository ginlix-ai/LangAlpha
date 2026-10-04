"""An automation run's answer, and its head flattened to one line.

The automation list and the run feed lead with the head, so the answer is
read from the checkpoint once, as the run completes, and the head kept on the
execution row; the whole answer goes to the chats a delivering run's agent
didn't reach.
"""

import html
import logging
import re
from typing import Optional

from langchain_core.messages import AIMessage

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


async def read_run_answer(thread_id: str, run_id: str) -> Optional[str]:
    """The run's final answer, the last text its agent wrote, or None when it
    has none.

    Called as the run completes, while its turn is still the thread's newest,
    so only that turn is materialized; a later read that finds a newer turn
    leaves none. Background subagents checkpoint under their own ``task:``
    namespaces, so these messages are the agent's own. The answer only
    decorates a list and stands in for a delivery the agent didn't make: a
    failed read leaves none rather than failing the execution.
    """
    from src.server.services.history.projector import split_content_blocks
    from src.server.services.history.reader import CheckpointHistoryReader

    try:
        reader = CheckpointHistoryReader.get_instance()
        anchors, _ = await reader.aget_turn_anchors(thread_id)
        if not anchors or anchors[-1].run_id != run_id:
            return None
        (turn,) = await reader.aget_turn_slices(thread_id, anchors[-1:])
    except Exception as e:
        logger.warning(
            f"[AUTOMATION] Excerpt read failed: thread_id={thread_id} "
            f"run_id={run_id}: {e}"
        )
        return None
    for message in reversed(turn.messages):
        if not isinstance(message, AIMessage):
            continue
        text, _, _ = split_content_blocks(message.content)
        if text and text.strip():
            return text
    return None
