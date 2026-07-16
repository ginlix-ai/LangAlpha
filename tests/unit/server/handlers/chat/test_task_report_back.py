"""Locks the task report-back notification-style contract.

``style`` is chosen at enqueue, carried in the durable payload, and honored
at dispatch: inline embeds the result and gates subagent tooling off;
pointer announces and keeps TaskOutput available. Unknown styles must fall
back to inline — a durable job may outlive the code that enqueued it.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.handlers.chat.task_report_back import (
    TASK_RB_RESULT_CAP,
    TASK_RB_STYLE,
    _build_notification_message,
    enqueue_task_report_backs,
    execute_task_report_back,
)


# ---------------------------------------------------------------------------
# _build_notification_message
# ---------------------------------------------------------------------------


def _payload(**over) -> dict:
    base = {
        "task_id": "abc123",
        "display_id": "Task-abc123",
        "subagent_type": "research",
        "description": "look things up",
        "result_text": "the findings",
        "result_truncated": False,
        "result_total_chars": 12,
        "style": "inline",
    }
    base.update(over)
    return base


class TestBuildNotificationMessage:
    def test_inline_embeds_result(self):
        msg = _build_notification_message(_payload())
        assert '<task_result id="Task-abc123" subagent="research">' in msg
        assert "the findings" in msg
        assert "Review the output below" in msg
        assert "TaskOutput" not in msg

    def test_inline_truncation_note(self):
        msg = _build_notification_message(
            _payload(result_truncated=True, result_total_chars=99999)
        )
        assert f"[truncated: showing {TASK_RB_RESULT_CAP} of 99999 chars]" in msg

    def test_inline_missing_result_never_points_at_taskoutput(self):
        # Inline turns have no TaskOutput tool; the recovery hint must not
        # reference it.
        msg = _build_notification_message(_payload(result_text=None))
        assert "workspace files" in msg
        assert "TaskOutput" not in msg

    def test_pointer_directs_to_taskoutput(self):
        msg = _build_notification_message(_payload(style="pointer"))
        assert 'TaskOutput(task_id="abc123")' in msg
        assert "<task_result" not in msg
        assert "Retrieve it" in msg

    def test_unknown_style_renders_inline(self):
        msg = _build_notification_message(_payload(style="carrier-pigeon"))
        assert "<task_result" in msg
        assert "the findings" in msg


# ---------------------------------------------------------------------------
# enqueue: style stamped into the durable payload
# ---------------------------------------------------------------------------


def _task(task_id: str = "abc123") -> SimpleNamespace:
    return SimpleNamespace(
        task_id=task_id,
        display_id=f"Task-{task_id}",
        subagent_type="research",
        description="look things up",
        result={"success": True, "result": "the findings"},
    )


@pytest.fixture
def enqueue_env():
    registry = MagicMock()
    registry.claim_report_back = AsyncMock(return_value=True)
    store = MagicMock()
    store.get_registry = AsyncMock(return_value=registry)
    enqueued: list[dict] = []

    async def _capture(**kwargs):
        enqueued.append(kwargs)

    with (
        patch(
            "src.server.services.background_registry_store."
            "BackgroundRegistryStore.get_instance",
            return_value=store,
        ),
        patch(
            "src.server.database.hook_outbox.enqueue_compensation_job",
            new=AsyncMock(side_effect=_capture),
        ),
        patch(
            "src.server.database.turn_lifecycle.get_run",
            new=AsyncMock(return_value={"status": "completed"}),
        ),
        patch("src.server.services.hook_outbox.HookOutboxDrainer.get_instance"),
    ):
        yield enqueued


class TestEnqueueStyle:
    @pytest.mark.asyncio
    async def test_default_style_follows_module_constant(self, enqueue_env):
        n = await enqueue_task_report_backs(
            thread_id="t1",
            response_id="r1",
            tasks=[_task()],
            workspace_id="w1",
            user_id="u1",
            all_settled=True,
        )
        assert n == 1
        assert enqueue_env[0]["payload"]["style"] == TASK_RB_STYLE

    @pytest.mark.asyncio
    async def test_inline_style_carries_result_text(self, enqueue_env):
        await enqueue_task_report_backs(
            thread_id="t1",
            response_id="r1",
            tasks=[_task()],
            workspace_id="w1",
            user_id="u1",
            all_settled=True,
            style="inline",
        )
        payload = enqueue_env[0]["payload"]
        assert payload["style"] == "inline"
        assert payload["result_text"] == "the findings"

    @pytest.mark.asyncio
    async def test_pointer_style_omits_result_text(self, enqueue_env):
        await enqueue_task_report_backs(
            thread_id="t1",
            response_id="r1",
            tasks=[_task()],
            workspace_id="w1",
            user_id="u1",
            all_settled=True,
            style="pointer",
        )
        payload = enqueue_env[0]["payload"]
        assert payload["style"] == "pointer"
        assert "result_text" not in payload
        assert "result_truncated" not in payload


# ---------------------------------------------------------------------------
# dispatch: the recursion gate follows the style
# ---------------------------------------------------------------------------


def _job(style: str) -> dict:
    payload = _payload(style=style)
    payload.update({"workspace_id": "w1", "user_id": "u1"})
    if style == "pointer":
        payload.pop("result_text")
    return {
        "hook_outbox_id": "0f1e2d3c-0000-0000-0000-000000000001",
        "conversation_thread_id": "t1",
        "attempts": 0,
        "payload": payload,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "style,expected_gate", [("inline", True), ("pointer", False)]
)
async def test_dispatch_gates_subagents_by_style(style, expected_gate):
    posted: list[dict] = []

    async def _post(*, thread_id, body, **kwargs):
        posted.append(body)
        return "dispatched", "rb-run-1"

    with (
        patch(
            "src.server.database.turn_lifecycle.get_latest_attempt",
            new=AsyncMock(return_value={"status": "completed"}),
        ),
        patch(
            "src.server.handlers.chat.notify_turn.post_notification_turn",
            new=AsyncMock(side_effect=_post),
        ),
        patch(
            "src.server.handlers.chat.notify_turn.await_run_terminal",
            new=AsyncMock(return_value="done"),
        ),
        patch(
            "src.server.database.hook_outbox.merge_job_payload",
            new=AsyncMock(return_value=True),
        ),
        patch(
            "src.server.handlers.chat.report_back.publish_wake",
            new=AsyncMock(),
        ),
        patch("src.utils.cache.redis_cache.get_cache_client"),
    ):
        await execute_task_report_back(_job(style))

    assert len(posted) == 1
    assert posted[0]["disable_subagents"] is expected_gate
    content = posted[0]["messages"][0]["content"]
    if style == "pointer":
        assert 'TaskOutput(task_id="abc123")' in content
    else:
        assert "<task_result" in content
