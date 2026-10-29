"""An automation run's answer, and its head flattened to one line.

The automation list and the run feed lead with the head, so the answer is
read from the checkpoint once, as the run completes, and the head kept on the
execution row. A run that delivers through the messaging service hands the
chats its agent didn't reach the result it ended on sending, else the answer.
"""

import html
import re
from typing import Any, NamedTuple, Optional

from langchain_core.messages import AIMessage

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
    and ``sent``, the text of the ``send_message`` it ended on."""

    text: Optional[str] = None
    sent: Optional[str] = None


# What a run may call after sending its result: looking up where to send,
# and sending again. Any other call after a send makes that send an update
# along the way.
_MESSAGING_TOOLS = frozenset({"send_message", "list_message_targets"})


def _result_sent(messages: list[Any]) -> Optional[str]:
    """The text of the send a run ended on: its last ``send_message`` call
    with text, whether it went out or not, since a chat it didn't reach is
    owed the result the agent wrote for it. A call that sent only files is
    passed over. None when the run did other work after its last send: that
    send was an update along the way, and the answer is the result."""
    for message in reversed(messages):
        if not isinstance(message, AIMessage):
            continue
        for call in reversed(message.tool_calls or []):
            name = call.get("name")
            if name not in _MESSAGING_TOOLS:
                return None
            if name != "send_message":
                continue
            args = call.get("args")
            text = args.get("text") if isinstance(args, dict) else None
            if isinstance(text, str) and text.strip():
                return text
    return None


async def read_run_answer(thread_id: str, run_id: str) -> RunAnswer:
    """What the run said: the last text its agent wrote, and the text of the
    send it ended on. Both are None when it has none.

    A run that sends its result writes something else last, such as a line
    saying where it sent it, so the send is the result a chat it didn't
    reach is handed.

    The run's own turn is read, found by its run: the thread can take another
    turn before the settle runs, and the newest turn is then that one's, not
    the run's. Only that turn is materialized. Background subagents
    checkpoint under their own ``task:`` namespaces, so these messages are
    the agent's own. A failed read raises: the caller knows whether the
    answer only heads a list or is what the chats its agent didn't reach are
    handed.
    """
    from src.server.services.history.projector import split_content_blocks
    from src.server.services.history.reader import CheckpointHistoryReader

    turn = await CheckpointHistoryReader.get_instance().aget_run_turn(thread_id, run_id)
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
    return RunAnswer(text=text, sent=_result_sent(turn.messages))
