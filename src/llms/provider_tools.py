"""Tools a provider runs on its own server inside a reply, read by another one.

The GLM Coding Plan endpoint, for one, opens a link itself and answers with an
``mcp_tool_use`` and an ``mcp_tool_result`` block. Those types exist only on
the server that ran the tool: DeepSeek, Anthropic and OpenAI-format servers
refuse them as unknown content, so a thread that holds one fails on every
other model. What the tool returned is often what the rest of the thread is
about, a page the user shared, so it goes to another provider as text.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from typing import Any

from langchain_core.messages import AIMessage

from src.llms.reasoning_payload import drop_if_empty

CALL_TYPES = frozenset({"server_tool_use", "mcp_tool_use"})

# Results are typed after their tool (``mcp_tool_result``,
# ``web_search_tool_result``), so a new server tool is covered on its first
# day. A plain ``tool_result`` answers one of our calls and only ever sits in
# a user turn.
_RESULT_SUFFIX = "_tool_result"


def _type(block: Any) -> str:
    btype = block.get("type") if isinstance(block, dict) else None
    return btype if isinstance(btype, str) else ""


def _is_result(btype: str) -> bool:
    return btype.endswith(_RESULT_SUFFIX) and btype != "tool_result"


def _call_input(call: dict) -> Any:
    """The call's arguments, which a streamed call keeps in ``partial_json``.

    langchain-anthropic opens a streamed block with ``input: {}`` and gathers
    the arguments as text, the shape its own formatter falls back to.
    """
    if call.get("input") or not isinstance(call.get("partial_json"), str):
        return call.get("input")
    try:
        return json.loads(call["partial_json"])
    except ValueError:
        return None


def _texts(node: Any) -> Iterator[str]:
    """The text a result holds, however deep its server nests it.

    Anthropic's fetch wraps the page in a document's ``source``, and code it
    ran answers with ``stdout`` and ``stderr``. Only text carries over: a
    search result's ``encrypted_content`` and an output's ``file_id`` are for
    their own server alone.
    """
    if isinstance(node, str):
        yield node
    elif isinstance(node, list):
        for item in node:
            yield from _texts(item)
    elif isinstance(node, dict):
        if _type(node) == "text":
            yield from _texts(node.get("text"))
            return
        source = node.get("source")
        if isinstance(source, dict) and source.get("type") == "text":
            yield from _texts(source.get("data"))
        for key in ("stdout", "stderr", "content"):
            yield from _texts(node.get(key))


def _as_text(result: dict, call: dict | None) -> dict | None:
    """The result as a text block, or ``None`` when it holds no text."""
    texts = [t for t in _texts(result.get("content")) if t]
    if not texts:
        return None
    name = call.get("name") if call else None
    tool = f"{name}, a tool only the earlier model had," if name else "A tool only the earlier model had"
    args = _call_input(call) if call else None
    on = f" for {json.dumps(args, ensure_ascii=False, sort_keys=True)}" if args else ""
    verb = "failed" if result.get("is_error") else "returned"
    return {"type": "text", "text": f"{tool} {verb}{on}:\n\n" + "\n\n".join(texts)}


def inline_provider_tools(msg: AIMessage) -> AIMessage | None:
    """``msg`` with its provider's tools as text; ``None`` means drop it.

    The reply went on writing after its server's tool returned, so a call of
    ours can sit before more text, which DeepSeek refuses. Ours go last, where
    any other reply has them.
    """
    content = msg.content
    if not isinstance(content, list) or not any(
        _type(b) in CALL_TYPES or _is_result(_type(b)) for b in content
    ):
        return msg
    calls = {b.get("id"): b for b in content if _type(b) in CALL_TYPES}
    kept: list[Any] = []
    ours: list[Any] = []
    for block in content:
        btype = _type(block)
        if btype in CALL_TYPES:
            continue
        if _is_result(btype):
            text = _as_text(block, calls.get(block.get("tool_use_id")))
            if text:
                kept.append(text)
        elif btype == "tool_use":
            ours.append(block)
        else:
            kept.append(block)
    return drop_if_empty(msg.model_copy(update={"content": kept + ours}))
