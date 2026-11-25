"""A symbol's chart records, and what deleting a comment leaves for its
computer to drop."""

from __future__ import annotations

import pytest

from src.server.database.artifacts import comments as comments_db
from src.server.database.artifacts import records as records_db

_A = "00000000-0000-0000-0000-00000000000a"
_C = "00000000-0000-0000-0000-00000000000c"
_T = "00000000-0000-0000-0000-00000000000d"
_U = "00000000-0000-0000-0000-00000000000e"
_W = "00000000-0000-0000-0000-00000000000f"


@pytest.fixture(autouse=True)
def _db(artifacts_db):
    return artifacts_db


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("symbol", "pattern"),
    [("NVDA", "NVDA:%"), ("BRK_B", "BRK\\_B:%"), ("A%B", "A\\%B:%")],
)
async def test_a_symbols_charts_match_that_symbol_alone(mock_cursor, symbol, pattern):
    """The prefix ends at the colon, so NVDA never matches NVDAX, and a
    symbol's own ``_`` or ``%`` matches only itself."""
    assert await records_db.list_chart_records(_W, symbol) == []
    sql, params = mock_cursor.execute.await_args.args
    assert "l.workspace_id = %s AND l.kind = 'chart'" in sql
    assert "l.path LIKE %s ESCAPE '\\'" in sql and "ORDER BY l.path" in sql
    assert params == (_W, pattern)


_FILE_ANCHOR = {
    "kind": "region",
    "boxes": [
        {"x": 0.1, "y": 0.1, "w": 0.2, "h": 0.2, "picture": ".agents/comments/a-1.png"},
        {"x": 0.5, "y": 0.5, "w": 0, "h": 0},
        {"x": 0.6, "y": 0.1, "w": 0.2, "h": 0.2, "picture": ".agents/comments/a-2.png"},
    ],
}
_CHART_ANCHOR = {
    "kind": "chart",
    "marks": [
        {"type": "region", "start": "2026-01-02", "end": "2026-01-09", "low": 1,
         "high": 2, "picture": ".agents/comments/NVDA-1day.png"},
        {"type": "point", "time": "2026-01-02", "price": 1},
    ],
}


def test_an_anchor_names_its_boxes_and_its_chart_regions_pictures():
    assert comments_db.anchor_pictures(_FILE_ANCHOR) == [
        ".agents/comments/a-1.png",
        ".agents/comments/a-2.png",
    ]
    assert comments_db.anchor_pictures(_CHART_ANCHOR) == [".agents/comments/NVDA-1day.png"]
    assert comments_db.anchor_pictures({"kind": "text", "quote": "x"}) == []


def test_a_picture_outside_the_comments_folder_is_never_named():
    """Its pictures are deleted with the comment, so a stored path that is not
    one the app writes names nothing, whoever stored it."""
    anchor = {
        "kind": "region",
        "boxes": [
            {"picture": "notes.md"},
            {"picture": ".agents/comments/../../notes.png"},
            {"picture": ".agents/skills/x.png"},
            {"picture": ".agents/comments/ok.png"},
        ],
    }
    assert comments_db.anchor_pictures(anchor) == [".agents/comments/ok.png"]


@pytest.mark.asyncio
async def test_a_deleted_comment_hands_over_every_picture_it_named(mock_cursor):
    """Which are still named is asked after the delete commits, where a
    concurrent delete of another comment naming them is seen."""
    mock_cursor.fetchone.return_value = {
        "thread_id": _T, "workspace_id": _W, "anchor": _FILE_ANCHOR,
    }

    deleted = await comments_db.delete_comment(user_id=_U, artifact_id=_A, comment_id=_C)

    assert deleted == comments_db.DeletedComment(
        thread_id=_T,
        workspace_id=_W,
        pictures=(".agents/comments/a-1.png", ".agents/comments/a-2.png"),
    )
    ((delete_sql, delete_params),) = [c.args for c in mock_cursor.execute.await_args_list]
    assert "DELETE FROM artifact_comments" in delete_sql
    assert delete_params == (_C, _A, _U)


@pytest.mark.asyncio
async def test_pictures_still_named_in_the_workspace_are_left_out(mock_cursor):
    # Another comment in the workspace names a-2.
    mock_cursor.fetchall.return_value = [{"picture": ".agents/comments/a-1.png"}]
    pictures = [".agents/comments/a-1.png", ".agents/comments/a-2.png"]

    assert await comments_db.unnamed_pictures(_W, pictures) == [".agents/comments/a-1.png"]
    sql, params = mock_cursor.execute.await_args.args
    assert "l.workspace_id = %(workspace_id)s" in sql
    assert params == {"pictures": pictures, "workspace_id": _W}


@pytest.mark.asyncio
async def test_a_comment_without_pictures_hands_over_none(mock_cursor):
    mock_cursor.fetchone.return_value = {
        "thread_id": None, "workspace_id": _W, "anchor": {"kind": "text", "quote": "x"},
    }
    deleted = await comments_db.delete_comment(user_id=_U, artifact_id=_A, comment_id=_C)
    assert deleted == comments_db.DeletedComment(thread_id=None, workspace_id=_W, pictures=())


@pytest.mark.asyncio
async def test_a_comment_not_there_is_none(mock_cursor):
    assert await comments_db.delete_comment(user_id=_U, artifact_id=_A, comment_id=_C) is None
