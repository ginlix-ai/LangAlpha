"""The route that takes a file off what a publish carries, down to its statement."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from src.server.app.artifacts import records
from src.server.app.artifacts.models import KeptRequest, LeftOutRequest
from src.server.database.artifacts import records as records_db
from src.server.database.artifacts.records import Artifact

_NOW = datetime(2026, 10, 31, tzinfo=timezone.utc)
_ID = "00000000-0000-0000-0000-00000000000a"


def _artifact(**over) -> Artifact:
    fields = dict(
        artifact_id=_ID,
        code="c0de",
        workspace_id="00000000-0000-0000-0000-00000000000f",
        path="reports/q3.html",
        created_thread_id=None,
        bound_thread_id=None,
        keep_updated=False,
        latest_seq=0,
        shared_at=None,
        files=None,
        revision=1,
        created_at=_NOW,
    )
    fields.update(over)
    return Artifact(**fields)


@pytest.fixture
def statement(monkeypatch):
    monkeypatch.setattr(records, "_changed", AsyncMock(return_value="answer"))
    written = AsyncMock(return_value=_artifact())
    monkeypatch.setattr(records_db, "set_record", written)
    return written


@pytest.mark.asyncio
@pytest.mark.parametrize("left_out", [True, False])
async def test_one_file_changes_in_one_statement_and_the_answer_is_the_record(
    monkeypatch, statement, left_out
):
    monkeypatch.setattr(records, "owned", AsyncMock(return_value=_artifact()))
    statement.return_value = _artifact(
        left_out_files=("reports/other.html",) if left_out else ()
    )

    answer = await records.set_file_left_out(
        _ID, LeftOutRequest(path="reports/other.html", left_out=left_out), "u"
    )

    assert answer == "answer"
    kwargs = statement.await_args.kwargs
    assert kwargs["user_id"] == "u" and kwargs["artifact_id"] == _ID
    assert kwargs["params"] == {
        "path": "reports/other.html",
        "left_out": left_out,
        "cap": records._MAX_LEFT_OUT,
    }


@pytest.mark.asyncio
async def test_a_path_the_full_list_did_not_take_is_refused(monkeypatch, statement):
    """The statement holds the cap, so two at once cannot both pass it; the
    answer says whether this one got in."""
    full = tuple(f"p{i}.html" for i in range(records._MAX_LEFT_OUT))
    monkeypatch.setattr(records, "owned", AsyncMock(return_value=_artifact()))
    statement.return_value = _artifact(left_out_files=full)

    with pytest.raises(HTTPException) as refused:
        await records.set_file_left_out(
            _ID, LeftOutRequest(path="more.html", left_out=True), "u"
        )
    assert refused.value.status_code == 422

    assert await records.set_file_left_out(
        _ID, LeftOutRequest(path="p0.html", left_out=True), "u"
    ) == "answer"
    assert await records.set_file_left_out(
        _ID, LeftOutRequest(path="more.html", left_out=False), "u"
    ) == "answer"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "refusal, status",
    [
        (records.snapshot.SnapshotRefused("file_missing"), 404),
        (records.snapshot.SnapshotRefused("needs_computer"), 409),
        (records.snapshot.SnapshotUnavailable("down"), 503),
        (records.artifacts_db.SharingChanged(), 409),
    ],
)
async def test_a_refused_keep_leaves_the_record_as_it_was(
    monkeypatch, statement, refusal, status
):
    """Keeping cuts its first version in the write that keeps the record, so a
    read that fails writes nothing: no record kept with no version, nor one
    that starts following turns for a page on no dashboard."""
    monkeypatch.setattr(records, "owned", AsyncMock(return_value=_artifact()))
    monkeypatch.setattr(records.snapshot, "keep", AsyncMock(side_effect=refusal))

    with pytest.raises(HTTPException) as refused:
        await records.set_artifact_kept(_ID, KeptRequest(kept=True), "u")

    assert refused.value.status_code == status
    statement.assert_not_awaited()
