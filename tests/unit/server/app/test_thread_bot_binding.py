"""A thread response names the bot it answers as.

The chat header resolves the name/avatar from that id; a legacy row the
resolver has not stamped yet keeps a null, which is the pre-bot header.
"""

from __future__ import annotations

from src.server.app.threads.crud import _thread_list_item

ROW = {
    "conversation_thread_id": "11111111-1111-1111-1111-111111111111",
    "workspace_id": "22222222-2222-2222-2222-222222222222",
    "thread_index": 1,
    "current_status": "completed",
    "msg_type": "user",
    "title": "T",
    "platform": "web",
    "metadata": {},
    "is_shared": False,
    "is_pinned": False,
    "archived_at": None,
    "llm_model": None,
    "bot_id": "44444444-4444-4444-4444-444444444444",
    "subagents_allowed": True,
    "reasoning_effort": None,
    "fast_mode": None,
    "created_at": __import__("datetime").datetime(2026, 11, 8),
    "updated_at": __import__("datetime").datetime(2026, 11, 8),
}


def test_a_stamped_thread_names_its_bot():
    assert _thread_list_item(ROW).bot_id == "44444444-4444-4444-4444-444444444444"


def test_an_unstamped_thread_keeps_a_null_bot():
    assert _thread_list_item({**ROW, "bot_id": None}).bot_id is None
