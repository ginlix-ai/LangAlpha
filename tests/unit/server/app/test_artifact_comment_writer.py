"""Who a comment message names as its writer.

In a shared workspace a file's comments are the holder's, so each person's
message names the member who wrote it. A personal workspace's answer keeps
its shape: no writer keys at all.
"""

from __future__ import annotations

from datetime import datetime, timezone

from src.server.app.artifacts.models import CommentMessageResponse
from src.server.database.artifacts.comments import _writer

HOLDER = "shared-00000000-0000-4000-8000-0000000000aa"


def _message(**writer) -> dict:
    return CommentMessageResponse(
        message_id="m-1", seq=1, author="user", body="check this", sent_at=None,
        request_key=None, thread_id=None, created_at=datetime(2026, 11, 15, tzinfo=timezone.utc),
        **writer,
    ).model_dump(mode="json")


def test_a_shared_workspace_names_the_member_and_a_personal_one_no_one():
    assert _writer(HOLDER, "lee") == "lee"
    assert _writer("alice", "alice") is None


def test_the_writer_keys_appear_only_when_a_member_is_named():
    named = _message(written_by="lee", written_by_name="Lee Chen")
    assert (named["written_by"], named["written_by_name"]) == ("lee", "Lee Chen")
    plain = _message()
    assert "written_by" not in plain and "written_by_name" not in plain
    # A member with no name set still reads as a member, not as the viewer.
    assert _message(written_by="gone")["written_by_name"] is None
