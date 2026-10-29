"""What an Anthropic request carries in front of each earlier thinking block.

A thinking block from Claude Fable 5.1 or a 5.5 model stays valid only while
the top-level ``system``, the set of ``tools`` and every message before it are
byte-identical to what was sent when the block was produced. With server-side
compaction the message part of that prefix starts at the newest compaction
block. ``cache_control`` markers are outside the check.

These helpers run a real ``create_agent`` graph against the project's own
Anthropic client with the network call replaced: each model call builds the
payload langchain-anthropic would send, records it, and answers with a scripted
reply that carries a signed thinking block. A scenario then compares
consecutive payloads with :func:'prefix_breaks'.
"""

from __future__ import annotations

import base64
import json
from collections.abc import Callable, Iterable
from typing import Any

import anthropic
import httpx
from langchain_anthropic import ChatAnthropic
from langchain_core.messages import AIMessage
from langchain_core.outputs import ChatGeneration, ChatResult

THINKING_TYPES = ("thinking", "redacted_thinking")


def _without_markers(value: Any) -> Any:
    """``value`` without cache markers, a string body read as its one text block.

    A marker can only be put on a block, so placing one promotes a string body
    (a message's, or a tool result's) to a block list; both are the same
    content to the check.
    """
    if isinstance(value, dict):
        out = {k: _without_markers(v) for k, v in value.items() if k != "cache_control"}
        if isinstance(out.get("content"), str) and ("role" in out or out.get("type") == "tool_result"):
            out["content"] = [{"type": "text", "text": out["content"]}]
        return out
    if isinstance(value, list):
        return [_without_markers(v) for v in value]
    return value


def _dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


def _message_start(messages: list[dict[str, Any]]) -> tuple[int, int]:
    """Where the checked message prefix starts: the newest compaction block."""
    for i in range(len(messages) - 1, -1, -1):
        content = messages[i].get("content")
        if not isinstance(content, list):
            continue
        for j in range(len(content) - 1, -1, -1):
            block = content[j]
            if isinstance(block, dict) and block.get("type") == "compaction":
                return i, j
    return 0, 0


def _parts(payload: dict[str, Any], upto: tuple[int, int] | None) -> list[tuple[str, str]]:
    """The labelled prefix in front of message ``upto[0]``, block ``upto[1]``.

    ``None`` means the end of the request: the prefix of a block in the answer
    to it, which the next call sends as a new assistant message.
    """
    clean = _without_markers(payload)
    messages = clean.get("messages") or []
    start_msg, start_block = _message_start(messages)
    end_msg, end_block = upto if upto is not None else (len(messages), 0)
    tools = _checked_tools(clean.get("tools"), messages[start_msg:end_msg])
    parts = [("system", _dump(clean.get("system"))), ("tools", _dump(tools))]
    for k in range(start_msg, end_msg):
        body = messages[k]
        if k == start_msg and start_block:
            body = {**body, "content": body["content"][start_block:]}
        parts.append((f"messages[{k}]", _dump(body)))
    if upto is None:
        parts.append((f"messages[{end_msg}][:0]", _dump({"role": "assistant", "content": []})))
    else:
        first = start_block if end_msg == start_msg else 0
        own = messages[end_msg]["content"][first:end_block]
        parts.append((f"messages[{end_msg}][:{end_block}]", _dump({"role": messages[end_msg]["role"], "content": own})))
    return parts


def _checked_tools(tools: Any, messages: list[dict[str, Any]]) -> Any:
    """The ``tools`` the check reads in front of a block.

    The check ignores an entry declared with ``defer_loading`` until a
    ``tool_addition`` block references it, so a deferred entry counts only once
    a message before the block names it, or a compaction block before it
    records the addition in its ``tool_changes``.
    """
    if not isinstance(tools, list):
        return tools
    blocks = [
        block
        for message in messages
        if isinstance(message.get("content"), list)
        for block in message["content"]
        if isinstance(block, dict)
    ]
    changes = [
        *blocks,
        *(
            change
            for block in blocks
            if block.get("type") == "compaction"
            for change in block.get("tool_changes") or ()
            if isinstance(change, dict)
        ),
    ]
    referenced = {
        change["tool"].get("name")
        for change in changes
        if change.get("type") == "tool_addition" and isinstance(change.get("tool"), dict)
    }
    return [
        tool
        for tool in tools
        if not (isinstance(tool, dict) and tool.get("defer_loading"))
        or tool.get("name") in referenced
    ]


def _thinking_positions(payload: dict[str, Any]) -> list[tuple[str, int, int]]:
    """Each thinking block a request sends back in order, by signature, with
    its message and block index."""
    messages = _without_markers(payload).get("messages") or []
    start_msg, start_block = _message_start(messages)
    out: list[tuple[str, int, int]] = []
    for i in range(start_msg, len(messages)):
        content = messages[i].get("content")
        if not isinstance(content, list):
            continue
        for j in range(start_block if i == start_msg else 0, len(content)):
            block = content[j]
            if isinstance(block, dict) and block.get("type") in THINKING_TYPES:
                out.append((str(block.get("signature") or block.get("data")), i, j))
    return out


def thinking_prefixes(payload: dict[str, Any]) -> dict[str, list[tuple[str, str]]]:
    """Each thinking block a request sends back, with the prefix it now has."""
    return {sig: _parts(payload, (i, j)) for sig, i, j in _thinking_positions(payload)}


def produced_prefix(payload: dict[str, Any]) -> list[tuple[str, str]]:
    """The prefix of a thinking block that leads the answer to ``payload``."""
    return _parts(payload, None)


def _first_difference(was: list[tuple[str, str]], now: list[tuple[str, str]]) -> str | None:
    for (label, a), (label_now, b) in zip(was, now, strict=False):
        if label != label_now or a != b:
            return f"{label} changed"
    if len(was) != len(now):
        return "prefix length changed"
    return None


def all_breaks(wire: Wire) -> list[str]:
    """Every later call that invalidates a thinking block an earlier call produced.

    A block is checked against the request it answered, on every call after
    it, and a block no longer sent counts too: dropping one from the middle of
    the history invalidates the blocks behind it.
    """
    out: list[str] = []
    for n, signature in wire.produced:
        reference = produced_prefix(wire.payloads[n])
        for m in range(n + 1, len(wire.payloads)):
            current = thinking_prefixes(wire.payloads[m]).get(signature)
            why = "block no longer sent" if current is None else _first_difference(reference, current)
            if why:
                out.append(f"{signature} (call {n + 1}) on call {m + 1}: {why}")
                break
    return out


def is_compaction_request(payload: dict[str, Any]) -> bool:
    """Whether ``payload`` asks for a summary rather than an answer."""
    return "compaction" in (payload.get("extra_body") or {})


def _compaction_block(payload: dict[str, Any]) -> str | None:
    messages = _without_markers(payload).get("messages") or []
    start_msg, start_block = _message_start(messages)
    content = messages[start_msg].get("content") if messages else None
    if not isinstance(content, list) or not content:
        return None
    block = content[start_block]
    return _dump(block) if isinstance(block, dict) and block.get("type") == "compaction" else None


def _kept_difference(
    was: list[tuple[str, str]], now: list[tuple[str, str]], swapped: bool
) -> str | None:
    """How ``now`` differs from ``was``, read across a compaction block when
    ``swapped``: one ``was`` did not start at, which replaced the messages it
    summarized. The turns it kept must then follow it as they were, which
    makes them the end of ``was``'s messages, with nothing between: content
    after the block in its own message would sit between the summarized
    messages and the first kept one.
    """
    if not swapped:
        return _first_difference(was, now)
    for (label, a), (_, b) in zip(was[:2], now[:2], strict=True):
        if a != b:
            return f"{label} changed"
    label, opening = now[2]
    if len(json.loads(opening)["content"]) > 1:
        return f"{label} holds more than the compaction block"
    kept = now[3:-1]
    if len(kept) > len(was) - 3:
        return "prefix length changed"
    tail = was[len(was) - 1 - len(kept) : -1]
    for (_, a), (label, b) in zip(tail, kept, strict=True):
        if a != b:
            return f"{label} changed"
    if was[-1][1] != now[-1][1]:
        return f"{now[-1][0]} changed"
    return None


def kept_breaks(wire: Wire) -> list[str]:
    """Every later call that sends an earlier thinking block invalid, across
    compactions.

    Unlike :func:`all_breaks`, a call that leaves a block out breaks nothing
    by itself: thinking may be left out from the end of the history, or with
    the messages a compaction block replaced, and leaving one out of the
    middle shows as a changed prefix on the blocks after it. Sending a block
    again once a call left it out is a break. A compaction request is checked
    for the blocks it sends; leaving out the rest is what it is for.
    """
    out: list[str] = []
    for n, signature in wire.produced:
        reference = produced_prefix(wire.payloads[n])
        block = _compaction_block(wire.payloads[n])
        gone = False
        for m in range(n + 1, len(wire.payloads)):
            payload = wire.payloads[m]
            current = thinking_prefixes(payload).get(signature)
            if current is None:
                gone = gone or not is_compaction_request(payload)
                continue
            newest = _compaction_block(payload)
            why = (
                "sent again after it was left out"
                if gone
                else _kept_difference(reference, current, newest is not None and newest != block)
            )
            if why:
                out.append(f"{signature} (call {n + 1}) on call {m + 1}: {why}")
                break
    return out


def left_out(wire: Wire) -> dict[str, int]:
    """The first call, numbered from 1, each thinking block is no longer sent
    on, by signature: a compaction request sends only what it summarizes, so
    it does not count."""
    out: dict[str, int] = {}
    for n, signature in wire.produced:
        for m in range(n + 1, len(wire.payloads)):
            payload = wire.payloads[m]
            if not is_compaction_request(payload) and signature not in thinking_prefixes(payload):
                out[signature] = m + 1
                break
    return out


def thinking_count(payload: dict[str, Any]) -> int:
    return len(thinking_prefixes(payload))


def reply(
    n: int,
    *,
    tool: tuple[str, dict[str, Any]] | None = None,
    text: str = "",
    model_name: str = "claude-opus-5-5",
) -> AIMessage:
    """A scripted Anthropic answer: a signed thinking block, then text or one tool call."""
    content: list[dict[str, Any]] = [{"type": "thinking", "thinking": "", "signature": f"sig-{n}"}]
    if text:
        content.append({"type": "text", "text": text})
    tool_calls = []
    if tool is not None:
        name, args = tool
        call_id = f"toolu_{n:02d}"
        content.append({"type": "tool_use", "id": call_id, "name": name, "input": args})
        tool_calls.append({"name": name, "args": args, "id": call_id, "type": "tool_call"})
    return AIMessage(
        content=content,
        tool_calls=tool_calls,
        id=f"msg_{n:02d}",
        response_metadata={"model_name": model_name, "stop_reason": "tool_use" if tool else "end_turn"},
        usage_metadata={"input_tokens": 1000, "output_tokens": 50, "total_tokens": 1050},
    )


#: The beta the API lists ``input_transformations`` under and takes
#: ``thinking.block_binding`` with.
BINDING_BETA = "thinking-binding-controls-2026-08-01"

INPUT_TRANSFORMATIONS = "input_transformations"

_MESSAGES_URL = "https://api.anthropic.com/v1/messages"


def refusal(message: str, status: int = 400) -> anthropic.APIStatusError:
    """The error the API answers a request with, as the SDK raises it; a
    scripted reply may be one."""
    body = {"type": "error", "error": {"type": "invalid_request_error", "message": message}}
    response = httpx.Response(status, json=body, request=httpx.Request("POST", _MESSAGES_URL))
    kind = anthropic.BadRequestError if status == 400 else anthropic.APIStatusError
    return kind(message, response=response, body=body)


def _betas(model: Any, payload: dict[str, Any]) -> list[str]:
    """The betas a request goes out with: its own replace the client's header."""
    if "betas" in payload:
        return list(payload["betas"] or [])
    header = (getattr(model, "default_headers", None) or {}).get("anthropic-beta") or ""
    return [b.strip() for b in header.split(",") if b.strip()]


def _count(entries: Iterable[dict[str, Any]]) -> dict[str, int]:
    """Entries by reason, a block let through by its type."""
    total: dict[str, int] = {}
    for entry in entries:
        key = entry.get("reason") if entry.get("type") == "thinking_dropped" else entry.get("type")
        total[key] = total.get(key, 0) + 1
    return total


def api_drops(wire: Wire) -> dict[str, int]:
    """What the API listed across every call, by reason."""
    return _count(entry for entries in wire.transformations for entry in entries or ())


def drops(messages: Iterable[Any]) -> dict[str, int]:
    """What the API listed across ``messages``' answers, by reason, as their
    ``response_metadata`` reports it."""
    return _count(
        entry
        for message in messages
        if isinstance(message, AIMessage)
        for entry in (message.response_metadata or {}).get(INPUT_TRANSFORMATIONS) or ()
    )


def listed(messages: Iterable[Any]) -> list[list[dict[str, Any]] | None]:
    """Each answer's ``input_transformations`` as its ``response_metadata`` holds it."""
    return [
        (message.response_metadata or {}).get(INPUT_TRANSFORMATIONS)
        for message in messages
        if isinstance(message, AIMessage)
    ]


def _api_content(message: AIMessage) -> list[dict[str, Any]]:
    content = message.content
    if isinstance(content, str):
        return [{"type": "text", "text": content}] if content else []
    return [{k: v for k, v in block.items() if k != "index"} for block in content if isinstance(block, dict)]


def _api_message(message: AIMessage, payload: dict[str, Any], transformations: list | None) -> dict[str, Any]:
    """The Messages API body that ``message`` is langchain-anthropic's reading of."""
    metadata = message.response_metadata or {}
    usage = message.usage_metadata or {}
    body: dict[str, Any] = {
        "id": message.id or "msg_wire",
        "type": "message",
        "role": "assistant",
        "model": metadata.get("model_name") or payload.get("model"),
        "content": _api_content(message),
        "stop_reason": metadata.get("stop_reason") or "end_turn",
        "stop_sequence": None,
        "usage": {
            "input_tokens": usage.get("input_tokens", 0),
            "output_tokens": usage.get("output_tokens", 0),
        },
    }
    if transformations is not None:
        body["input_transformations"] = transformations
    return body


def _sse(body: dict[str, Any]) -> bytes:
    """``body`` as the event stream the API sends for it."""
    events: list[dict[str, Any]] = [
        {
            "type": "message_start",
            "message": {
                **body,
                "content": [],
                "stop_reason": None,
                "usage": {"input_tokens": body["usage"]["input_tokens"], "output_tokens": 1},
            },
        }
    ]
    for index, block in enumerate(body["content"]):
        kind = block.get("type")
        if kind == "thinking":
            events.append({"type": "content_block_start", "index": index,
                           "content_block": {"type": "thinking", "thinking": "", "signature": ""}})
            if block.get("thinking"):
                events.append({"type": "content_block_delta", "index": index,
                               "delta": {"type": "thinking_delta", "thinking": block["thinking"]}})
            events.append({"type": "content_block_delta", "index": index,
                           "delta": {"type": "signature_delta", "signature": block.get("signature", "")}})
        elif kind == "text":
            events.append({"type": "content_block_start", "index": index,
                           "content_block": {"type": "text", "text": ""}})
            events.append({"type": "content_block_delta", "index": index,
                           "delta": {"type": "text_delta", "text": block.get("text", "")}})
        elif kind == "tool_use":
            events.append({"type": "content_block_start", "index": index,
                           "content_block": {**block, "input": {}}})
            events.append({"type": "content_block_delta", "index": index,
                           "delta": {"type": "input_json_delta", "partial_json": json.dumps(block.get("input") or {})}})
        else:
            events.append({"type": "content_block_start", "index": index, "content_block": block})
        events.append({"type": "content_block_stop", "index": index})
    events.append({
        "type": "message_delta",
        "delta": {"stop_reason": body["stop_reason"], "stop_sequence": None},
        "usage": body["usage"],
    })
    events.append({"type": "message_stop"})
    return b"".join(f"event: {e['type']}\ndata: {json.dumps(e)}\n\n".encode() for e in events)


class Wire:
    """Stands in for the Anthropic API: records each payload, answers from a script.

    It checks every earlier thinking block a request sends back the way the
    API does, against the request the block answered: one whose prefix
    changed fails. Under ``thinking.block_binding.prefix_mismatch_behavior``
    ``"drop_block"`` it and every thinking block after it are listed as
    dropped, ``"error"`` refuses the request with the API's 400, and with the
    field unset failing blocks are listed as let through. The list rides the
    answer's ``input_transformations`` when the request carries the beta, as
    the API's does. A scripted reply may be an error (:func:`refusal`), which the
    call raises.
    """

    def __init__(
        self,
        replies: Iterable[AIMessage | BaseException | Callable[[dict[str, Any]], AIMessage]] = (),
        *,
        served: Callable[[str], bytes] | None = None,
    ):
        #: What a URL source serves when the API fetches it on a call; the
        #: recorded payload carries the bytes beside the URL, since the check
        #: reads the image the API fetched rather than the address.
        self.served = served
        self.payloads: list[dict[str, Any]] = []
        #: (payload index, signature) of each thinking block an answer carried.
        self.produced: list[tuple[int, str]] = []
        #: What the API listed in each call's ``input_transformations``, by
        #: payload index; None for a call answered without the list or refused.
        self.transformations: list[list[dict[str, Any]] | None] = []
        #: The ``anthropic-beta`` header each call went out with, on the
        #: seam that sends one.
        self.headers: list[str | None] = []
        #: The JSON body each call went out with over HTTP, on the seam that
        #: sends one: what the SDK serialized, not what langchain built.
        self.bodies: list[dict[str, Any]] = []
        self._replies = list(replies)

    def _record(self, payload: dict[str, Any]) -> None:
        self.payloads.append(self._as_read(json.loads(_dump(payload))))

    def _as_read(self, value: Any) -> Any:
        if self.served is None:
            return value
        if isinstance(value, dict):
            out = {k: self._as_read(v) for k, v in value.items()}
            source = out.get("source")
            if out.get("type") in ("image", "document") and isinstance(source, dict) and source.get("type") == "url":
                served = base64.b64encode(self.served(source["url"])).decode("ascii")
                out["source"] = {**source, "served": served}
            return out
        if isinstance(value, list):
            return [self._as_read(v) for v in value]
        return value

    def push(self, *replies: AIMessage | BaseException | Callable[[dict[str, Any]], AIMessage]) -> None:
        self._replies.extend(replies)

    def check(self, payload: dict[str, Any], betas: list[str]) -> list[dict[str, Any]] | None:
        """What the API answers ``payload`` with in ``input_transformations``, the
        last recorded call; None when it would not list them. Raises the
        API's 400 where it refuses the request."""
        if is_compaction_request(payload) and "context_management" in payload:
            raise refusal("compaction and context_management cannot be used in the same request")
        thinking = payload.get("thinking") or {}
        binding = thinking.get("block_binding") if isinstance(thinking, dict) else None
        if binding is not None and BINDING_BETA not in betas:
            raise refusal("thinking.block_binding: Extra inputs are not permitted")
        behavior = (binding or {}).get("prefix_mismatch_behavior")
        produced: dict[str, int] = {}
        for n, signature in self.produced:
            produced.setdefault(signature, n)
        entries: list[dict[str, Any]] = []
        failed = False
        for signature, i, j in _thinking_positions(payload):
            n = produced.get(signature)
            fails = n is not None and self._fails(n, payload, signature)
            path = f"messages.{i}.content.{j}"
            if behavior is not None and (failed or fails):
                if behavior == "error":
                    raise refusal(
                        f"{path}: Invalid `signature` in `thinking` block. The block is bound to a "
                        "different conversation. Remove the block, or set "
                        "`thinking.block_binding.prefix_mismatch_behavior` to \"drop_block\"."
                    )
                failed = True
                entries.append({"type": "thinking_dropped", "path": path, "reason": "prefix_binding_mismatch"})
            elif behavior is None and fails:
                entries.append({"type": "thinking_mismatch_allowed", "path": path, "reason": "prefix_binding_mismatch"})
        return entries if BINDING_BETA in betas else None

    def _fails(self, n: int, payload: dict[str, Any], signature: str) -> bool:
        current = thinking_prefixes(payload)[signature]
        newest = _compaction_block(payload)
        swapped = newest is not None and newest != _compaction_block(self.payloads[n])
        return _kept_difference(produced_prefix(self.payloads[n]), current, swapped) is not None

    def _answer(self, payload: dict[str, Any], betas: list[str]) -> tuple[AIMessage, list | None]:
        """The next scripted reply to the recorded ``payload``, with what the API lists."""
        try:
            transformations = self.check(payload, betas)
        except anthropic.APIStatusError:
            self.transformations.append(None)
            raise
        nxt = self._replies.pop(0)
        message = nxt(payload) if callable(nxt) and not isinstance(nxt, BaseException) else nxt
        if isinstance(message, BaseException):
            self.transformations.append(None)
            raise message
        self.transformations.append(transformations)
        for block in message.content if isinstance(message.content, list) else []:
            if isinstance(block, dict) and block.get("type") in THINKING_TYPES:
                self.produced.append((len(self.payloads) - 1, str(block.get("signature"))))
        return message, transformations

    def install(self, monkeypatch: Any, *, stream: bool | None = None) -> Wire:
        """Answer every Anthropic call in the test.

        By default the scripted message is the answer as it is, with what the
        API lists on its ``response_metadata``. With ``stream`` set, the call goes
        through langchain-anthropic and the SDK as it does live, streamed or
        not, and the Messages API's own body or event stream is what comes
        back over HTTP, so their parsing (and the project's patches of it) is
        what produces the message.
        """
        wire = self
        if stream is None:

            async def _agenerate(model, messages, stop=None, run_manager=None, **kwargs):
                payload = model._get_request_payload(messages, stop=stop, **kwargs)
                wire._record(payload)
                wire.headers.append(None)
                message, transformations = wire._answer(wire.payloads[-1], _betas(model, payload))
                if transformations is not None:
                    message = message.model_copy(
                        update={"response_metadata": {**message.response_metadata, INPUT_TRANSFORMATIONS: transformations}}
                    )
                return ChatResult(generations=[ChatGeneration(message=message)])

            monkeypatch.setattr(ChatAnthropic, "_agenerate", _agenerate)
            monkeypatch.setattr(ChatAnthropic, "_should_stream", lambda self, **_: False)
            return self

        acreate = ChatAnthropic._acreate
        send = httpx.AsyncClient.send

        async def _acreate(model, payload):
            wire._record(payload)
            return await acreate(model, payload)

        async def _send(client, request, **kwargs):
            if not request.url.path.endswith("/v1/messages"):
                return await send(client, request, **kwargs)
            header = request.headers.get("anthropic-beta") or ""
            wire.headers.append(header or None)
            wire.bodies.append(json.loads(request.content))
            betas = [b.strip() for b in header.split(",") if b.strip()]
            try:
                message, transformations = wire._answer(wire.payloads[-1], betas)
            except anthropic.APIStatusError as e:
                return httpx.Response(e.status_code, json=e.body, request=request)
            body = _api_message(message, wire.payloads[-1], transformations)
            if json.loads(request.content).get("stream"):
                return httpx.Response(
                    200, headers={"content-type": "text/event-stream"}, content=_sse(body), request=request
                )
            return httpx.Response(200, json=body, request=request)

        monkeypatch.setattr(ChatAnthropic, "_acreate", _acreate)
        monkeypatch.setattr(httpx.AsyncClient, "send", _send)
        monkeypatch.setattr(ChatAnthropic, "_should_stream", lambda self, **_: stream)
        return self


def anthropic_model(model_name: str = "claude-opus-5-5") -> Any:
    """The client the agent gets for this catalog model, with a placeholder key."""
    from src.llms.llm import create_llm

    return create_llm(model_name, api_key="sk-ant-test-placeholder")
