"""Golden tests for the checkpoint→SSE projector (pure, no I/O)."""

from __future__ import annotations

from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from src.server.services.history.projector import (
    HistoryEvent,
    history_events_to_sse,
    messages_to_history_events,
)

THREAD_ID = "thread-test"


def _sse(messages, agent="main"):
    return history_events_to_sse(
        messages_to_history_events(messages, agent=agent), thread_id=THREAD_ID
    )


def test_plain_text_message():
    items = _sse([AIMessage(content="hello world", id="ai-1")])
    assert items == [
        {
            "event": "message_chunk",
            "data": {
                "thread_id": THREAD_ID,
                "agent": "main",
                "id": "ai-1",
                "role": "assistant",
                "content": "hello world",
                "content_type": "text",
                "finish_reason": "stop",
            },
        }
    ]


def test_human_messages_skipped():
    items = _sse(
        [HumanMessage(content="question", id="h-1"), AIMessage(content="answer", id="ai-1")]
    )
    assert len(items) == 1
    assert items[0]["data"]["content"] == "answer"


def test_reasoning_blocks_split_from_text():
    msg = AIMessage(
        content=[
            {"type": "thinking", "thinking": "let me think"},
            {"type": "text", "text": "the answer"},
        ],
        id="ai-1",
    )
    items = _sse([msg])
    kinds = [(i["data"].get("content_type"), i["data"].get("content")) for i in items]
    assert kinds == [
        ("reasoning_signal", "start"),
        ("reasoning", "let me think"),
        ("reasoning_signal", "complete"),
        ("text", "the answer"),
    ]
    assert items[-1]["data"]["finish_reason"] == "stop"


def test_tool_call_suppresses_stop_finish():
    msg = AIMessage(
        content="running a tool",
        id="ai-1",
        tool_calls=[{"name": "web_search", "args": {"query": "x"}, "id": "tc-1"}],
    )
    items = _sse([msg])
    assert items[0]["data"]["content"] == "running a tool"
    assert "finish_reason" not in items[0]["data"]
    assert items[1]["event"] == "tool_calls"
    data = items[1]["data"]
    assert data["finish_reason"] == "tool_calls"
    assert data["role"] == "assistant"
    assert data["id"] == "ai-1"
    [tc] = data["tool_calls"]
    assert (tc["name"], tc["args"], tc["id"]) == ("web_search", {"query": "x"}, "tc-1")


def test_nameless_tool_calls_filtered():
    msg = AIMessage(
        content="",
        id="ai-1",
        tool_calls=[{"name": "", "args": {}, "id": "tc-0"}],
    )
    assert _sse([msg]) == []


def test_tool_result_event():
    msgs = [
        AIMessage(
            content="",
            id="ai-1",
            tool_calls=[{"name": "web_search", "args": {}, "id": "tc-1"}],
        ),
        ToolMessage(content="result body", tool_call_id="tc-1", name="web_search", id="tm-1"),
    ]
    items = _sse(msgs)
    result = items[-1]
    assert result["event"] == "tool_call_result"
    assert result["data"]["tool_call_id"] == "tc-1"
    assert result["data"]["content"] == "result body"
    assert result["data"]["content_type"] == "text"


def test_write_tool_derives_file_operation_artifact():
    content = "line one\nline two\n"
    msgs = [
        AIMessage(
            content="",
            id="ai-1",
            tool_calls=[
                {
                    "name": "Write",
                    "args": {"file_path": "work/a.py", "content": content},
                    "id": "tc-w",
                }
            ],
        ),
        ToolMessage(content="ok", tool_call_id="tc-w", name="Write", id="tm-1"),
    ]
    items = _sse(msgs)
    # Live ordering: tool_calls → artifact → tool_call_result.
    assert [i["event"] for i in items] == ["tool_calls", "artifact", "tool_call_result"]
    artifact = items[1]["data"]
    assert artifact["artifact_type"] == "file_operation"
    assert artifact["artifact_id"] == "tc-w"
    assert artifact["status"] == "completed"
    assert artifact["payload"] == {
        "operation": "Write",
        "file_path": "work/a.py",
        "line_count": 2,
        "content": content,
    }


def test_write_tool_with_dict_content_coerced_to_json():
    # A model writing a .json file often passes `content` as a dict, which the
    # checkpoint stores raw. The projector must apply the same JSON coercion the
    # live ToolArgumentParsingMiddleware does, not crash on the dict.
    msgs = [
        AIMessage(
            content="",
            id="ai-1",
            tool_calls=[
                {
                    "name": "Write",
                    "args": {"file_path": "data/config.json", "content": {"k": [1, 2]}},
                    "id": "tc-w",
                }
            ],
        ),
        ToolMessage(content="ok", tool_call_id="tc-w", name="Write", id="tm-1"),
    ]
    artifact = next(i for i in _sse(msgs) if i["event"] == "artifact")["data"]
    assert artifact["payload"] == {
        "operation": "Write",
        "file_path": "data/config.json",
        "line_count": 1,
        "content": '{"k": [1, 2]}',
    }


def test_edit_tool_with_dict_strings_coerced_to_json():
    msgs = [
        AIMessage(
            content="",
            id="ai-1",
            tool_calls=[
                {
                    "name": "Edit",
                    "args": {
                        "file_path": "data/config.json",
                        "old_string": {"v": 1},
                        "new_string": {"v": 2},
                    },
                    "id": "tc-e",
                }
            ],
        ),
        ToolMessage(content="ok", tool_call_id="tc-e", name="Edit", id="tm-1"),
    ]
    artifact = next(i for i in _sse(msgs) if i["event"] == "artifact")["data"]
    assert artifact["payload"] == {
        "operation": "Edit",
        "file_path": "data/config.json",
        "line_count": 1,
        "old_string": '{"v": 1}',
        "new_string": '{"v": 2}',
    }


def test_edit_tool_derives_file_operation_artifact():
    msgs = [
        AIMessage(
            content="",
            id="ai-1",
            tool_calls=[
                {
                    "name": "Edit",
                    "args": {
                        "file_path": "work/a.py",
                        "old_string": "before",
                        "new_string": "after line",
                    },
                    "id": "tc-e",
                }
            ],
        ),
        ToolMessage(content="ok", tool_call_id="tc-e", name="Edit", id="tm-1"),
    ]
    artifact = next(i for i in _sse(msgs) if i["event"] == "artifact")["data"]
    assert artifact["payload"] == {
        "operation": "Edit",
        "file_path": "work/a.py",
        "line_count": 1,
        "old_string": "before",
        "new_string": "after line",
    }


def test_failed_tool_marks_artifact_failed():
    msgs = [
        AIMessage(
            content="",
            id="ai-1",
            tool_calls=[
                {"name": "Write", "args": {"file_path": "a", "content": "x"}, "id": "tc-w"}
            ],
        ),
        ToolMessage(
            content="boom", tool_call_id="tc-w", name="Write", id="tm-1", status="error"
        ),
    ]
    artifact = next(i for i in _sse(msgs) if i["event"] == "artifact")["data"]
    assert artifact["status"] == "failed"


def test_todo_write_derives_todo_update():
    todos = [
        {"content": "a", "activeForm": "doing a", "status": "completed"},
        {"content": "b", "status": "in_progress"},
        {"content": "c", "status": "pending"},
    ]
    msgs = [
        AIMessage(
            content="",
            id="ai-1",
            tool_calls=[{"name": "TodoWrite", "args": {"todos": todos}, "id": "tc-t"}],
        ),
        ToolMessage(content="ok", tool_call_id="tc-t", name="TodoWrite", id="tm-1"),
    ]
    artifact = next(i for i in _sse(msgs) if i["event"] == "artifact")["data"]
    assert artifact["artifact_type"] == "todo_update"
    assert artifact["payload"]["total"] == 3
    assert artifact["payload"]["completed"] == 1
    assert artifact["payload"]["in_progress"] == 1
    assert artifact["payload"]["pending"] == 1
    # activeForm defaults to content when absent.
    assert artifact["payload"]["todos"][1]["activeForm"] == "b"


def test_show_widget_artifact_from_tool_message():
    msgs = [
        AIMessage(
            content="",
            id="ai-1",
            tool_calls=[{"name": "ShowWidget", "args": {}, "id": "tc-s"}],
        ),
        ToolMessage(
            content="widget shown",
            tool_call_id="tc-s",
            name="ShowWidget",
            id="tm-1",
            artifact={"html": "<div/>", "title": "My Widget"},
        ),
    ]
    items = _sse(msgs)
    artifact = next(i for i in _sse(msgs) if i["event"] == "artifact")["data"]
    assert artifact["artifact_type"] == "html_widget"
    assert artifact["artifact_id"] == "tc-s"
    assert artifact["payload"] == {"html": "<div/>", "title": "My Widget"}
    # ToolMessage.artifact also rides the tool_call_result, matching live SSE.
    result = next(i for i in items if i["event"] == "tool_call_result")["data"]
    assert result["artifact"] == {"html": "<div/>", "title": "My Widget"}


def test_preview_url_artifact_id_from_port():
    msgs = [
        AIMessage(
            content="",
            id="ai-1",
            tool_calls=[{"name": "GetPreviewUrl", "args": {"port": 3000}, "id": "tc-p"}],
        ),
        ToolMessage(
            content="url",
            tool_call_id="tc-p",
            name="GetPreviewUrl",
            id="tm-1",
            artifact={"port": 3000, "title": "app", "command": None, "path": "/"},
        ),
    ]
    artifact = next(i for i in _sse(msgs) if i["event"] == "artifact")["data"]
    assert artifact["artifact_type"] == "preview_url"
    assert artifact["artifact_id"] == "preview_3000"


def test_widget_tool_without_artifact_derives_nothing():
    msgs = [
        AIMessage(
            content="",
            id="ai-1",
            tool_calls=[{"name": "ShowWidget", "args": {}, "id": "tc-s"}],
        ),
        ToolMessage(content="failed", tool_call_id="tc-s", name="ShowWidget", id="tm-1"),
    ]
    assert [i["event"] for i in _sse(msgs)] == ["tool_calls", "tool_call_result"]


def test_task_artifact_attributed_to_main():
    task_artifact = {
        "task_id": "abc123",
        "action": "init",
        "description": "research task",
        "prompt": "go research",
    }
    msgs = [
        AIMessage(
            content="",
            id="ai-1",
            tool_calls=[{"name": "Task", "args": {}, "id": "tc-task"}],
        ),
        ToolMessage(
            content="dispatched",
            tool_call_id="tc-task",
            name="Task",
            id="tm-1",
            additional_kwargs={"task_artifact": task_artifact},
        ),
    ]
    artifact = next(i for i in _sse(msgs, agent="other") if i["event"] == "artifact")["data"]
    assert artifact["artifact_type"] == "task"
    assert artifact["artifact_id"] == "task:abc123"
    # Task cards always render under the main transcript, whatever projected the slice.
    assert artifact["agent"] == "main"
    assert artifact["payload"] == task_artifact
    assert artifact["tool_call_id"] == "tc-task"


def test_subagent_agent_label_propagates():
    items = _sse([AIMessage(content="sub says hi", id="ai-1")], agent="task:abc123")
    assert items[0]["data"]["agent"] == "task:abc123"


def test_empty_content_key_dropped():
    items = history_events_to_sse(
        [HistoryEvent("text", "main", "ai-1", {"content": "", "finish_reason": "stop"})],
        thread_id=THREAD_ID,
    )
    assert "content" not in items[0]["data"]


def test_stamped_steering_message_projects_delivered():
    delivered = {
        "count": 1,
        "messages": [{"content": "focus on Q2", "user_id": "u-1", "timestamp": 1.0}],
        "timestamp": 2.0,
    }
    items = _sse(
        [
            HumanMessage(
                content="[Steering from User]\nfocus on Q2",
                id="h-s",
                additional_kwargs={
                    "lc_source": "steering",
                    "steering_delivered": delivered,
                },
            ),
            AIMessage(content="ok", id="ai-1"),
        ]
    )
    assert [i["event"] for i in items] == ["steering_delivered", "message_chunk"]
    data = items[0]["data"]
    assert data["thread_id"] == THREAD_ID
    assert data["messages"] == delivered["messages"]
    assert data["count"] == 1
    # The raw steering text never leaks as assistant content.
    assert items[1]["data"]["content"] == "ok"


def test_unstamped_steering_marker_falls_back_to_content():
    items = _sse(
        [HumanMessage(content="[Steering from User]\nuse eurodollars", id="h-s")]
    )
    assert [i["event"] for i in items] == ["steering_delivered"]
    assert items[0]["data"]["messages"] == [{"content": "use eurodollars"}]
    assert items[0]["data"]["count"] == 1


def test_unstamped_subagent_followup_falls_back_to_content():
    items = _sse(
        [
            HumanMessage(
                content="[Follow-up Instructions from Orchestrator]\nadd sources",
                id="h-f",
            )
        ]
    )
    assert [i["event"] for i in items] == ["steering_delivered"]
    assert items[0]["data"]["content"] == "add sources"
    assert items[0]["data"]["count"] == 1


def test_summary_message_projects_summarize_complete():
    from ptc_agent.agent.middleware.compaction.utils import build_summary_message

    message = build_summary_message(
        "we discussed rates", "/work/history.md", original_message_count=40
    )
    items = _sse([message])
    assert [i["event"] for i in items] == ["context_window"]
    data = items[0]["data"]
    assert data["action"] == "summarize"
    assert data["signal"] == "complete"
    assert data["summary_text"] == "we discussed rates"
    assert data["summary_length"] == len("we discussed rates")
    assert data["original_message_count"] == 40


def test_token_usage_projected_after_message_content():
    items = _sse(
        [
            AIMessage(
                content="answer",
                id="ai-1",
                usage_metadata={
                    "input_tokens": 900,
                    "output_tokens": 100,
                    "total_tokens": 1000,
                },
            )
        ]
    )
    assert [i["event"] for i in items] == ["message_chunk", "context_window"]
    data = items[1]["data"]
    assert data["action"] == "token_usage"
    assert data["signal"] == "complete"
    assert data["input_tokens"] == 900
    assert data["output_tokens"] == 100
    assert data["total_tokens"] == 1000
