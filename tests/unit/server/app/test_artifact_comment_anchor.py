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
        "kind": "file",
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


_BLOCK = {
    "kind": "block",
    "lines": "14-15",
    "label": "Margin outlook",
    "quote": "We model",
}
_CELLS = {
    "kind": "cells",
    "sheet": "Model",
    "ranges": [{"range": "D3:D6", "values": "338, 231"}, {"range": "D10"}],
}


@pytest.mark.parametrize(
    "anchor",
    [
        _BLOCK,
        {"kind": "block", "lines": "7"},
        _CELLS,
        {"kind": "cells", "ranges": [{"range": "B2", "values": "41%"}]},
        {"kind": "cells", "ranges": [{"range": "XFD1048576"}]},
        {"kind": "text", "quote": "61.5%", "lines": "7-9", "prefix": "We"},
        {"kind": "text", "quote": "net revenue", "page": 3},
        {
            "kind": "area",
            "label": "Revenue",
            "elements": [{"lines": "11", "quote": "Revenue"}, {"lines": "12"}],
            "count": 2,
        },
    ],
)
def test_the_text_viewers_anchors_are_kept_as_sent(anchor):
    assert CommentAnchor.model_validate(anchor).model_dump(exclude_none=True) == anchor


@pytest.mark.parametrize(
    "anchor",
    [
        {"kind": "block"},
        {"kind": "block", "lines": "14 to 15"},
        {"kind": "block", "lines": "1" * 9},
        {"kind": "block", "lines": "0"},
        {"kind": "block", "lines": "0-3"},
        {"kind": "text", "quote": "a", "lines": "4-0"},
        {"kind": "block", "lines": "14-7"},
        {"kind": "area", "elements": [{"lines": "9-8"}, {"lines": "11"}], "count": 2},
        {**_BLOCK, "path": "#a"},
        {**_BLOCK, "prefix": "a"},
        {**_BLOCK, "page": 2},
        {"kind": "element", "path": "#a", "lines": "3"},
        {"kind": "text", "quote": "a", "page": 0},
        {"kind": "element", "path": "#a", "page": 1},
        {"kind": "cells"},
        {"kind": "cells", "ranges": []},
        {"kind": "cells", "ranges": [{"range": "D3:D6"}] * 65},
        {"kind": "cells", "ranges": [{"range": "d3"}]},
        {"kind": "cells", "ranges": [{"range": "A0"}]},
        {"kind": "cells", "ranges": [{"range": "XFE1"}]},
        {"kind": "cells", "ranges": [{"range": "A1:A1048577"}]},
        {"kind": "cells", "ranges": [{"range": "D3", "values": "x" * 2001}]},
        {**_CELLS, "label": "Model"},
        {**_CELLS, "quote": "338"},
        {"kind": "text", "quote": "a", "sheet": "Model"},
        {"kind": "text", "quote": "a", "ranges": [{"range": "A1"}]},
        {
            "kind": "area",
            "elements": [{"lines": "1-2-3"}, {"lines": "4"}],
            "count": 2,
        },
    ],
)
def test_a_text_viewers_anchor_out_of_bounds_is_refused(anchor):
    with pytest.raises(ValidationError):
        CommentAnchor.model_validate(anchor)


_PICTURE = ".agents/comments/q3-report-p3-20261118-141205.png"


@pytest.mark.parametrize(
    "anchor",
    [
        {"kind": "region", "boxes": [{"x": 0.42, "y": 0.17, "w": 0, "h": 0}]},
        {
            "kind": "region",
            "boxes": [
                {
                    "page": 3,
                    "x": 0.12,
                    "y": 0.34,
                    "w": 0.4,
                    "h": 0.22,
                    "picture": _PICTURE,
                },
                {"page": 5, "x": 0.1, "y": 0.1, "w": 0.2, "h": 0.2},
            ],
        },
        {"kind": "region", "boxes": [{"x": 0.5, "y": 0.5, "w": 0.5, "h": 0.5}]},
    ],
)
def test_boxes_on_a_picture_are_kept_as_sent(anchor):
    assert CommentAnchor.model_validate(anchor).model_dump(exclude_none=True) == anchor


@pytest.mark.parametrize(
    "anchor",
    [
        {"kind": "region"},
        {"kind": "region", "boxes": []},
        {"kind": "region", "boxes": [{"x": 0.1, "y": 0.1, "w": 0.1, "h": 0.1}] * 65},
        # Past the edge it is drawn on.
        {"kind": "region", "boxes": [{"x": 0.9, "y": 0, "w": 0.2, "h": 0.1}]},
        # A box has both sides; a point has neither, and no picture.
        {"kind": "region", "boxes": [{"x": 0, "y": 0, "w": 0.1, "h": 0}]},
        {
            "kind": "region",
            "boxes": [{"x": 0, "y": 0, "w": 0, "h": 0, "picture": _PICTURE}],
        },
        # A picture is only ever one the app wrote to the comments folder.
        {
            "kind": "region",
            "boxes": [{"x": 0, "y": 0, "w": 0.1, "h": 0.1, "picture": "../secret.png"}],
        },
        {
            "kind": "region",
            "boxes": [{"x": 0, "y": 0, "w": 0.1, "h": 0.1, "picture": "notes.png"}],
        },
        # On pages, or none on a page.
        {
            "kind": "region",
            "boxes": [
                {"page": 1, "x": 0, "y": 0, "w": 0.1, "h": 0.1},
                {"x": 0, "y": 0, "w": 0.1, "h": 0.1},
            ],
        },
        {"kind": "region", "boxes": [{"x": 0, "y": 0, "w": 0.1, "h": 0.1}], "page": 1},
        {"kind": "text", "quote": "a", "boxes": [{"x": 0, "y": 0, "w": 0, "h": 0}]},
    ],
)
def test_a_region_the_api_could_not_show_is_refused(anchor):
    with pytest.raises(ValidationError):
        CommentAnchor.model_validate(anchor)


_REGION_MARK = {
    "type": "region",
    "start": "2026-09-02",
    "end": "2026-10-14",
    "low": 108.2,
    "high": 131.5,
    "picture": ".agents/comments/NVDA-1day-20261124-141205.png",
}
_POINT_MARK = {"type": "point", "time": "2026-10-28T09:30", "price": 118.4}
_ELEMENT_MARK = {
    "type": "element",
    "layer": "nvda-breakout",
    "layer_title": "Breakout setup",
    "element_kind": "drawing",
    "element_type": "segment",
    "element_id": "neck",
    "element_name": "Neckline",
    "index": 2,
    "time": "2026-10-03T15:59:59",
    "price": 118.4,
}


@pytest.mark.parametrize(
    "marks",
    [
        [_REGION_MARK, _POINT_MARK, _ELEMENT_MARK],
        [{k: v for k, v in _REGION_MARK.items() if k != "picture"}],
        # One instant, one price: a region of no size is still a region.
        [{**_REGION_MARK, "end": "2026-09-02", "high": 108.2}],
        [{**_REGION_MARK, "start": "2026-09-02", "end": "2026-09-02T10:00"}],
        [
            {
                "type": "element",
                "layer": "sma",
                "layer_title": "Moving averages",
                "element_kind": "series",
                "element_id": "sma50",
                "index": 0,
                "time": "2026-10-03",
                "price": 1.5,
            }
        ],
        [{**_ELEMENT_MARK, "element_kind": "level", "element_type": None}],
        [_POINT_MARK] * 64,
    ],
)
def test_chart_marks_are_kept_as_sent(marks):
    anchor = {"kind": "chart", "marks": marks}
    kept = CommentAnchor.model_validate(anchor).model_dump(exclude_none=True)
    assert kept == {
        "kind": "chart",
        "marks": [{k: v for k, v in m.items() if v is not None} for m in marks],
    }


@pytest.mark.parametrize(
    "marks",
    [
        [],
        [_POINT_MARK] * 65,
        [{"type": "box", "time": "2026-10-03", "price": 1}],
        [{**_POINT_MARK, "zone": "UTC"}],
        # Layer times: exchange-local, no zone or offset, a real date.
        [{**_POINT_MARK, "time": "2026-10-28T09:30Z"}],
        [{**_POINT_MARK, "time": "2026-10-28T09:30:00+04:00"}],
        [{**_POINT_MARK, "time": "2026-02-30"}],
        [{**_POINT_MARK, "time": "1730107800"}],
        [{**_POINT_MARK, "price": float("nan")}],
        [{**_POINT_MARK, "price": float("inf")}],
        # A price past any chart's would print as hundreds of digits.
        [{**_POINT_MARK, "price": 1e308}],
        [{**_ELEMENT_MARK, "price": -1e13}],
        [{**_REGION_MARK, "start": "2026-10-15"}],
        [{**_REGION_MARK, "low": 131.6}],
        [{**_REGION_MARK, "high": float("-inf")}],
        [{**_REGION_MARK, "high": 1e13}],
        [{**_REGION_MARK, "picture": "../secret.png"}],
        [{**_REGION_MARK, "picture": ".agents/comments/chart.svg"}],
        [{**_ELEMENT_MARK, "layer": "Not A Layer"}],
        [{**_ELEMENT_MARK, "layer_title": ""}],
        [{**_ELEMENT_MARK, "element_kind": "fill"}],
        [{**_ELEMENT_MARK, "element_type": "doodle"}],
        [{**_ELEMENT_MARK, "element_type": None}],
        [{**_ELEMENT_MARK, "element_kind": "series"}],
        [{**_ELEMENT_MARK, "element_id": "has space"}],
        [{**_ELEMENT_MARK, "element_name": "x" * 121}],
        [{**_ELEMENT_MARK, "index": -1}],
        [{k: v for k, v in _ELEMENT_MARK.items() if k != "time"}],
    ],
)
def test_a_chart_mark_the_agent_could_not_find_is_refused(marks):
    with pytest.raises(ValidationError):
        CommentAnchor.model_validate({"kind": "chart", "marks": marks})


@pytest.mark.parametrize(
    "anchor",
    [
        {"kind": "chart"},
        {"kind": "chart", "marks": [_POINT_MARK], "point": {"x": 0.5, "y": 0.5}},
        {"kind": "chart", "marks": [_POINT_MARK], "quote": "NVDA"},
        {"kind": "region", "marks": [_POINT_MARK]},
    ],
)
def test_a_chart_anchor_holds_its_marks_and_nothing_else(anchor):
    with pytest.raises(ValidationError):
        CommentAnchor.model_validate(anchor)


@pytest.mark.parametrize(
    ("anchor", "kind", "refused"),
    [
        ({"kind": "chart", "marks": [_POINT_MARK]}, "chart", None),
        ({"kind": "chart", "marks": [_POINT_MARK]}, "file", "a file takes no chart anchor"),
        ({"kind": "text", "quote": "Revenue"}, "chart", "a chart takes a chart anchor"),
    ],
)
def test_a_comment_keeps_to_its_records_kind(anchor, kind, refused):
    """A chart has no file, so its comment keeps no hash or title whatever
    the client sent. The wrong kind is refused as the body's validation
    refuses, so a client reads one shape of 422."""
    from fastapi.exceptions import RequestValidationError

    request = CreateCommentRequest(
        anchor=anchor, body="why here?", file_sha256="0" * 64, title="NVDA"
    )
    if refused:
        with pytest.raises(RequestValidationError) as e:
            request.check_against(kind)
        assert list(e.value.errors()) == [
            {
                "type": "value_error",
                "loc": ("body", "anchor", "kind"),
                "msg": f"Value error, {refused}",
                "input": anchor["kind"],
            }
        ]
    else:
        request.check_against(kind)
    assert request.file_fields("chart") == (None, None)
    assert request.file_fields("file") == ("0" * 64, "NVDA")


_NON_FINITE = [float("nan"), float("inf"), float("-inf")]


@pytest.mark.parametrize("bad", _NON_FINITE)
@pytest.mark.parametrize(
    "anchor",
    [
        lambda bad: {"kind": "element", "path": "#a", "point": {"x": bad, "y": 0.5}},
        lambda bad: {**_AREA, "rect": {**_AREA["rect"], "h": bad}},
        lambda bad: {"kind": "region", "boxes": [{"x": bad, "y": 0, "w": 0, "h": 0}]},
        lambda bad: {"kind": "chart", "marks": [{**_REGION_MARK, "low": bad}]},
        lambda bad: {"kind": "chart", "marks": [{**_ELEMENT_MARK, "price": bad}]},
    ],
)
def test_every_number_in_an_anchor_is_finite(anchor, bad):
    with pytest.raises(ValidationError) as e:
        CommentAnchor.model_validate(anchor(bad))
    assert [err["type"] for err in e.value.errors()] == ["finite_number"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("raw", "loc"),
    [
        (b'{"kind":"element","path":"#a","point":{"x":NaN,"y":0.5}}', ["point", "x"]),
        (b'{"kind":"region","boxes":[{"x":0.1,"y":0.1,"w":Infinity,"h":0.1}]}', ["boxes", 0, "w"]),
        (
            b'{"kind":"chart","marks":[{"type":"point","time":"2026-10-03","price":-Infinity}]}',
            ["marks", 0, "point", "price"],
        ),
    ],
)
async def test_a_raw_non_finite_number_is_a_422_not_a_500(raw, loc):
    """Python's JSON reads a raw ``NaN`` or ``Infinity``; the 422 quoting it
    back must still be JSON."""
    from fastapi.exceptions import RequestValidationError
    from httpx import ASGITransport, AsyncClient

    from src.server.app.artifacts import router
    from src.server.app.setup import _request_validation_handler
    from tests.conftest import create_test_app

    app = create_test_app(router)
    app.add_exception_handler(RequestValidationError, _request_validation_handler)
    body = b'{"body":"why here?","anchor":' + raw + b"}"
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        resp = await c.post(
            "/api/v1/artifacts/00000000-0000-0000-0000-00000000000a/comments",
            content=body,
            headers={"content-type": "application/json"},
        )
    assert resp.status_code == 422
    assert resp.json()["detail"] == [
        {
            "type": "finite_number",
            "loc": ["body", "anchor", *loc],
            "msg": "Input should be a finite number",
            "input": None,
        }
    ]
