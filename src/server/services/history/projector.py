"""Project checkpointed messages into replay SSE events. Pure functions, no I/O.

The intermediate ``HistoryEvent`` model is modeled on the v3 stream channels
(content-block-centric messages, tools channel) but owned by this module — it
is never exposed on the wire, so upstream protocol changes land in a future
adapter instead of rippling through replay.

Artifact derivation mirrors the live emitters: ``file_operation``/``todo_update``
payloads are rebuilt from tool-call args (the same source the middleware reads),
``html_widget``/``preview_url``/``chart_annotation`` from the checkpointed
ToolMessage artifact, and ``task`` from ``additional_kwargs.task_artifact``.
Non-derivable payloads (image URL rewrites, widget ``data``) are merged by the
replay endpoint from persisted sources, not synthesized here.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

from langchain_core.messages import AIMessage, AnyMessage, HumanMessage, ToolMessage

from ptc_agent.agent.middleware.compaction.utils import parse_summary_message
from ptc_agent.agent.middleware.tool.argument_parsing import parse_tool_args
from src.llms.content_utils import extract_content_with_type
from src.llms.token_counter import extract_token_usage
from src.server.utils.content_normalizer import normalize_text_content

MAIN_AGENT = "main"

HistoryEventKind = Literal[
    "reasoning-signal",
    "reasoning",
    "text",
    "tool-call",
    "tool-result",
    "artifact",
    "steering-delivered",
    "context-window",
]

_STEERING_MARKERS = (
    "[Steering from User]\n",
    "[Follow-up Instructions from Orchestrator]\n",
)

_FILE_OPERATION_TOOLS = {"Write", "Edit"}
_ARTIFACT_FROM_TOOL_MESSAGE = {
    "ShowWidget": "html_widget",
    "GetPreviewUrl": "preview_url",
    "draw_chart_annotation": "chart_annotation",
    "manage_chart_annotations": "chart_annotation",
}


@dataclass
class HistoryEvent:
    """One projected event; internal stage-2 seam, never a wire format."""

    kind: HistoryEventKind
    agent: str
    message_id: str | None = None
    data: dict[str, Any] = field(default_factory=dict)


def messages_to_history_events(
    messages: list[AnyMessage], *, agent: str = MAIN_AGENT
) -> list[HistoryEvent]:
    """Project a turn slice (or subagent transcript) into ordered events.

    Plain HumanMessages are skipped (the turn's user input is emitted
    separately by the replay endpoint), but marked mid-turn injections —
    steering and compaction summaries — project to their own events.
    """
    events: list[HistoryEvent] = []
    tool_call_index: dict[str, dict[str, Any]] = {}

    for message in messages:
        if isinstance(message, HumanMessage):
            events.extend(_project_human_message(message, agent))
            continue
        if isinstance(message, ToolMessage):
            events.extend(_project_tool_message(message, tool_call_index, agent))
            continue
        if isinstance(message, AIMessage):
            events.extend(_project_ai_message(message, tool_call_index, agent))
    return events


def history_events_to_sse(
    events: list[HistoryEvent],
    *,
    thread_id: str,
) -> list[dict[str, Any]]:
    """Render history events as ``{"event": type, "data": dict}`` items.

    Output items match the persisted ``sse_events`` shape, so the replay
    endpoint emits checkpoint-sourced and stored events identically.
    """
    items: list[dict[str, Any]] = []
    for event in events:
        if event.kind == "reasoning-signal":
            items.append(
                _sse(
                    "message_chunk",
                    {
                        "thread_id": thread_id,
                        "agent": event.agent,
                        "id": event.message_id,
                        "role": "assistant",
                        "content": event.data["signal"],
                        "content_type": "reasoning_signal",
                    },
                )
            )
        elif event.kind in ("reasoning", "text"):
            data = {
                "thread_id": thread_id,
                "agent": event.agent,
                "id": event.message_id,
                "role": "assistant",
                "content": event.data["content"],
                "content_type": "reasoning" if event.kind == "reasoning" else "text",
            }
            if event.data.get("finish_reason"):
                data["finish_reason"] = event.data["finish_reason"]
            items.append(_sse("message_chunk", data))
        elif event.kind == "tool-call":
            items.append(
                _sse(
                    "tool_calls",
                    {
                        "thread_id": thread_id,
                        "agent": event.agent,
                        "id": event.message_id,
                        "role": "assistant",
                        "tool_calls": event.data["tool_calls"],
                        "finish_reason": "tool_calls",
                    },
                )
            )
        elif event.kind == "tool-result":
            data = {
                "thread_id": thread_id,
                "agent": event.agent,
                "id": event.message_id,
                "role": "assistant",
                "tool_call_id": event.data["tool_call_id"],
            }
            if event.data.get("content") is not None:
                data["content"] = event.data["content"]
                data["content_type"] = event.data.get("content_type") or "text"
            if event.data.get("artifact"):
                data["artifact"] = event.data["artifact"]
            items.append(_sse("tool_call_result", data))
        elif event.kind == "artifact":
            items.append(
                _sse(
                    "artifact",
                    {
                        "artifact_type": event.data["artifact_type"],
                        "artifact_id": event.data.get("artifact_id"),
                        "agent": event.agent,
                        "thread_id": thread_id,
                        "timestamp": event.data.get("timestamp"),
                        "status": event.data.get("status"),
                        "payload": event.data.get("payload", {}),
                        **(
                            {"tool_call_id": event.data["tool_call_id"]}
                            if event.data.get("tool_call_id")
                            else {}
                        ),
                    },
                )
            )
        elif event.kind == "steering-delivered":
            items.append(
                _sse(
                    "steering_delivered",
                    {"thread_id": thread_id, "agent": event.agent, **event.data},
                )
            )
        elif event.kind == "context-window":
            items.append(
                _sse(
                    "context_window",
                    {"thread_id": thread_id, "agent": event.agent, **event.data},
                )
            )
    return items


def _sse(event_type: str, data: dict[str, Any]) -> dict[str, Any]:
    if data.get("content") == "":
        data.pop("content")
    return {"event": event_type, "data": data}


def _project_human_message(message: HumanMessage, agent: str) -> list[HistoryEvent]:
    """Project marked mid-turn HumanMessage injections; skip plain user input.

    Payloads come from ``additional_kwargs`` stamped at emit time; unstamped
    historical messages fall back to parsing the marker content.
    """
    kwargs = message.additional_kwargs or {}
    source = kwargs.get("lc_source")
    content = message.content if isinstance(message.content, str) else ""

    if source == "steering" or content.startswith(_STEERING_MARKERS):
        payload = kwargs.get("steering_delivered")
        if not isinstance(payload, dict):
            text = content.split("\n", 1)[1] if "\n" in content else content
            if content.startswith(_STEERING_MARKERS[1]):
                payload = {"content": text, "count": 1}
            else:
                payload = {"count": 1, "messages": [{"content": text}]}
        return [HistoryEvent("steering-delivered", agent, message.id, dict(payload))]

    if source == "summarization":
        stamped = kwargs.get("summarize_complete") or {}
        summary_text = parse_summary_message(message)
        return [
            HistoryEvent(
                "context-window",
                agent,
                message.id,
                {
                    "action": "summarize",
                    "signal": "complete",
                    "summary_length": stamped.get("summary_length", len(summary_text)),
                    "summary_text": summary_text,
                    "original_message_count": stamped.get("original_message_count", 0),
                },
            )
        ]

    return []


def _project_ai_message(
    message: AIMessage,
    tool_call_index: dict[str, dict[str, Any]],
    agent: str,
) -> list[HistoryEvent]:
    events: list[HistoryEvent] = []
    message_id = message.id or "unknown"
    text, reasoning = _split_content_blocks(message.content)
    tool_calls = _filter_tool_calls(message.tool_calls or [])

    if reasoning:
        events.append(
            HistoryEvent("reasoning-signal", agent, message_id, {"signal": "start"})
        )
        events.append(
            HistoryEvent("reasoning", agent, message_id, {"content": reasoning})
        )
        events.append(
            HistoryEvent("reasoning-signal", agent, message_id, {"signal": "complete"})
        )

    if text:
        events.append(
            HistoryEvent(
                "text",
                agent,
                message_id,
                {
                    "content": text,
                    "finish_reason": None if tool_calls else "stop",
                },
            )
        )

    if tool_calls:
        for tool_call in tool_calls:
            if tool_call.get("id"):
                tool_call_index[tool_call["id"]] = tool_call
        events.append(
            HistoryEvent("tool-call", agent, message_id, {"tool_calls": tool_calls})
        )

    # Mirror the live token_usage emitter (compaction middleware): same
    # extractor, same >0 gate, same total — parity by construction.
    usage = extract_token_usage(message)
    input_tokens = usage.get("input_tokens", 0)
    if input_tokens > 0:
        output_tokens = usage.get("output_tokens", 0)
        events.append(
            HistoryEvent(
                "context-window",
                agent,
                message_id,
                {
                    "action": "token_usage",
                    "signal": "complete",
                    "input_tokens": input_tokens,
                    "output_tokens": output_tokens,
                    "total_tokens": input_tokens + output_tokens,
                },
            )
        )
    return events


def _project_tool_message(
    message: ToolMessage,
    tool_call_index: dict[str, dict[str, Any]],
    agent: str,
) -> list[HistoryEvent]:
    events: list[HistoryEvent] = []
    message_id = message.id or "unknown"
    tool_call = tool_call_index.get(message.tool_call_id or "", {})
    status = "failed" if getattr(message, "status", None) == "error" else "completed"

    # Live ordering: middleware/tool writer events fire during tool execution,
    # before the ToolMessage reaches the messages stream.
    derived = _derive_artifact(message, tool_call, status)
    if derived is not None:
        events.append(HistoryEvent("artifact", agent, message_id, derived))

    task_artifact = (message.additional_kwargs or {}).get("task_artifact")
    if task_artifact:
        events.append(
            HistoryEvent(
                "artifact",
                MAIN_AGENT,
                message_id,
                {
                    "artifact_type": "task",
                    "artifact_id": f"task:{task_artifact['task_id']}",
                    "status": "completed",
                    "payload": task_artifact,
                    "tool_call_id": message.tool_call_id,
                },
            )
        )

    content, content_type = normalize_text_content(message.content)
    artifact = getattr(message, "artifact", None)
    events.append(
        HistoryEvent(
            "tool-result",
            agent,
            message_id,
            {
                "tool_call_id": message.tool_call_id,
                "content": content,
                "content_type": content_type,
                "artifact": artifact if artifact else None,
            },
        )
    )
    return events


def _derive_artifact(
    message: ToolMessage,
    tool_call: dict[str, Any],
    status: str,
) -> dict[str, Any] | None:
    """Rebuild the middleware/tool artifact event for a completed tool call."""
    tool_name = message.name or tool_call.get("name")
    # Checkpoints store the raw model args; the live file-op middleware reads
    # them AFTER ToolArgumentParsingMiddleware coerces dict/list JSON payloads to
    # strings. Apply the same canonical coercion so a Write/Edit whose content
    # was emitted as a dict (e.g. a .json file) matches live instead of tripping
    # the string-typed payload derivation below.
    args = parse_tool_args(tool_call.get("args") or {}, tool_name)
    tool_call_id = message.tool_call_id

    if tool_name in _FILE_OPERATION_TOOLS:
        payload: dict[str, Any] = {
            "operation": tool_name,
            "file_path": args.get("file_path", ""),
        }
        if tool_name == "Write":
            content = args.get("content", "")
            payload["line_count"] = _count_lines(content)
            payload["content"] = content
        else:
            new_string = args.get("new_string", "")
            payload["line_count"] = _count_lines(new_string)
            payload["old_string"] = args.get("old_string", "")
            payload["new_string"] = new_string
        return {
            "artifact_type": "file_operation",
            "artifact_id": tool_call_id,
            "status": status,
            "payload": payload,
        }

    if tool_name == "TodoWrite":
        todos = _normalize_todos(args.get("todos"))
        counts = {"completed": 0, "in_progress": 0, "pending": 0}
        for todo in todos:
            todo_status = todo.get("status", "pending")
            if todo_status in counts:
                counts[todo_status] += 1
        return {
            "artifact_type": "todo_update",
            "artifact_id": tool_call_id,
            "status": status,
            "payload": {"todos": todos, "total": len(todos), **counts},
        }

    artifact_type = _ARTIFACT_FROM_TOOL_MESSAGE.get(tool_name or "")
    if artifact_type:
        tool_artifact = getattr(message, "artifact", None)
        if not isinstance(tool_artifact, dict) or not tool_artifact:
            return None
        artifact_id = tool_call_id
        if artifact_type == "preview_url" and tool_artifact.get("port") is not None:
            artifact_id = f"preview_{tool_artifact['port']}"
        return {
            "artifact_type": artifact_type,
            "artifact_id": artifact_id,
            "status": status if artifact_type == "html_widget" else None,
            "payload": tool_artifact,
        }

    return None


def _split_content_blocks(content: Any) -> tuple[str | None, str | None]:
    """Split final-message content into (text, reasoning) parts.

    Unlike ``extract_content_with_type`` on a whole list (which merges every
    block into one string), replay needs reasoning and text separated so they
    render in their own channels.
    """
    if content is None:
        return None, None
    if isinstance(content, str):
        return (content or None), None
    blocks = content if isinstance(content, list) else [content]
    texts: list[str] = []
    reasonings: list[str] = []
    for block in blocks:
        if isinstance(block, str):
            if block:
                texts.append(block)
            continue
        extracted, content_type = extract_content_with_type(block)
        if not extracted:
            continue
        if content_type == "reasoning":
            reasonings.append(extracted)
        else:
            texts.append(extracted)
    return ("".join(texts) or None), ("\n\n".join(reasonings) or None)


def _filter_tool_calls(tool_calls: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [tc for tc in tool_calls if (tc.get("name") or "").strip()]


def _normalize_todos(todos: Any) -> list[dict[str, Any]]:
    if not isinstance(todos, list):
        return []
    normalized = []
    for todo in todos:
        if isinstance(todo, dict):
            normalized.append(
                {
                    "content": todo.get("content", ""),
                    "activeForm": todo.get("activeForm", todo.get("content", "")),
                    "status": todo.get("status", "pending"),
                }
            )
    return normalized


def _count_lines(text: str) -> int:
    return len(text.splitlines()) if text else 0
