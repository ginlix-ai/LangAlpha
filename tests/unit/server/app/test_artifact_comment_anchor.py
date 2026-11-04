"""The anchor an artifact comment is stored with: an area's fields and their
limits. The comment's own path names the file, so the anchor never does."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from src.server.app.artifacts.models import CommentAnchor, CreateCommentRequest

_PARTS = [
    {
        "path": "#grid > div:nth-of-type(1)",
        "label": "Revenue",
        "quote": "Revenue $46.7B",
    },
    {"path": "#grid > div:nth-of-type(2)", "label": "Gross margin"},
]
_AREA = {
    "kind": "area",
    "label": "Revenue, Gross margin",
    "elements": _PARTS,
    "count": 2,
    "point": {"x": 1.0, "y": 1.0},
    "rect": {"x": 0.03, "y": 0.05, "w": 0.9, "h": 0.04},
}


def test_an_area_keeps_its_parts_count_and_rect():
    anchor = CommentAnchor.model_validate(_AREA)
    assert anchor.model_dump(exclude_none=True) == _AREA


def test_an_area_may_count_more_than_it_lists_and_leave_out_its_rect():
    anchor = CommentAnchor.model_validate({**_AREA, "count": 40, "rect": None})
    assert anchor.count == 40
    assert anchor.rect is None


@pytest.mark.parametrize(
    "change",
    [
        {"elements": _PARTS[:1]},
        {"elements": []},
        {"elements": None},
        {"elements": _PARTS * 5},
        {"count": None},
        {"count": 1},
        {"count": 501},
        {"elements": [*_PARTS, {"label": "Only a label"}]},
        {"elements": [*_PARTS, {"path": "#x", "onclick": "y"}]},
        {"elements": [*_PARTS, {"quote": "x" * 2001}]},
        {"rect": {"x": 1.5, "y": 0, "w": 0.1, "h": 0.1}},
        {"rect": {"x": 0, "y": 0, "w": -0.1, "h": 0.1}},
        {"rect": {"x": 0, "y": 0, "w": 0.1}},
    ],
)
def test_an_area_out_of_bounds_is_refused(change):
    with pytest.raises(ValidationError):
        CommentAnchor.model_validate({**_AREA, **change})


@pytest.mark.parametrize(
    "extra",
    [
        {"elements": _PARTS},
        {"count": 2},
        {"rect": {"x": 0, "y": 0, "w": 0.1, "h": 0.1}},
    ],
)
def test_area_fields_belong_to_an_area(extra):
    with pytest.raises(ValidationError):
        CommentAnchor.model_validate({"kind": "element", "path": "#a", **extra})


def test_other_kinds_are_unchanged():
    anchor = CommentAnchor.model_validate(
        {"kind": "element", "path": "#a", "point": {"x": 0.5, "y": 0.5}}
    )
    assert anchor.model_dump(exclude_none=True) == {
        "kind": "element",
        "path": "#a",
        "point": {"x": 0.5, "y": 0.5},
    }


def test_an_anchor_does_not_name_its_file():
    with pytest.raises(ValidationError):
        CommentAnchor.model_validate(
            {"kind": "element", "path": "#a", "page_path": "index.html"}
        )


def test_a_comment_response_prints_the_anchor_as_stored():
    from datetime import UTC, datetime

    from src.server.app.artifacts.models import CommentResponse

    stored = {"kind": "area", **{k: v for k, v in _AREA.items() if k != "kind"}}
    now = datetime(2026, 1, 1, tzinfo=UTC)
    row = {
        "comment_id": "c",
        "artifact_id": "a",
        "number": 1,
        "path": "r.html",
        "file_sha256": None,
        "title": None,
        "anchor": CommentAnchor.model_validate(stored).model_dump(exclude_none=True),
        "status": "open",
        "delivery": "note",
        "thread_id": None,
        "thread_title": None,
        "thread_archived": False,
        "created_at": now,
        "updated_at": now,
        "resolved_at": None,
        "messages": [],
        "read_through": 0,
        "unread": 0,
    }
    out = CommentResponse.model_validate(row).model_dump(mode="json")
    assert out["anchor"] == row["anchor"]
    assert "prefix" not in out["anchor"]


def test_half_a_surrogate_pair_cannot_reach_the_database():
    """A cut in a page's words leaves one and the JSON parser lets it through:
    the comment would fail on every retry of that spot."""
    request = CreateCommentRequest(
        anchor={
            "kind": "area",
            "elements": [{"path": "#a", "quote": "x\ud83d"}, {"path": "#b"}],
            "count": 2,
            "rect": {"x": 0, "y": 0, "w": 0.5, "h": 0.5},
            "label": "\ud83d\ude00",
            "prefix": "\ude00ab",
        },
        body="words\ud83d",
        title="title\ud83d",
    )
    assert request.anchor.elements[0].quote == "x\ufffd"
    assert request.anchor.label == "\U0001f600"
    assert request.anchor.prefix == "\ufffdab"
    assert request.body == "words\ufffd"
    assert request.title == "title\ufffd"
    request.model_dump_json().encode("utf-8")
