"""What the agent reads for comments on files and charts, and what the chat
shows."""

from __future__ import annotations

from src.server.database.artifacts import ClaimedComment
from src.server.services.artifacts.comments import (
    FilePlace,
    _render_anchor,
    render_message,
    shown_send,
    stamp_send,
)


def _anchor(anchor: dict) -> list[str]:
    return _render_anchor(anchor, None, set())


def _text(note: str, claimed: list, **kwargs) -> str:
    return render_message(note, claimed, **kwargs).text


def _parts(claimed: list, places: dict | None = None) -> list[str]:
    return render_message("", claimed, places=places).parts


def _area(count: int, parts: int = 3) -> dict:
    names = ["REVENUE (Q2)", "GROSS MARGIN", "OPERATING INCOME"]
    return {
        "kind": "area",
        "label": "REVENUE (Q2), GROSS MARGIN, OPERATING INCOME",
        "elements": [
            {
                "path": f"#app > div:nth-of-type(3) > div:nth-of-type({i + 1})",
                "label": names[i % 3],
                "quote": f"{names[i % 3]} <b>",
            }
            for i in range(parts)
        ],
        "count": count,
        "rect": {"x": 0.03, "y": 0.05, "w": 0.9, "h": 0.04},
    }


def _comment(**overrides) -> ClaimedComment:
    fields = {
        "comment_id": "c1",
        "artifact_id": "a1",
        "workspace_id": "w1",
        "kind": "file",
        "path": "reports/q3/index.html",
        "title": "Q3 margins",
        "number": 3,
        "anchor": {"kind": "element", "path": "#margin", "quote": "41%"},
        "delivered": ["Show the quarter before too."],
    }
    return ClaimedComment(**{**fields, **overrides})


def test_an_area_renders_its_parts_one_element_each():
    assert _anchor(_area(3)) == [
        '<anchor kind="area">',
        "<label>REVENUE (Q2), GROSS MARGIN, OPERATING INCOME</label>",
        "<element><path>#app > div:nth-of-type(3) > div:nth-of-type(1)</path>"
        "<label>REVENUE (Q2)</label><quote>REVENUE (Q2) &lt;b></quote></element>",
        "<element><path>#app > div:nth-of-type(3) > div:nth-of-type(2)</path>"
        "<label>GROSS MARGIN</label><quote>GROSS MARGIN &lt;b></quote></element>",
        "<element><path>#app > div:nth-of-type(3) > div:nth-of-type(3)</path>"
        "<label>OPERATING INCOME</label><quote>OPERATING INCOME &lt;b></quote></element>",
        "</anchor>",
    ]


def test_an_area_says_how_many_more_it_held_than_it_lists():
    lines = _anchor(_area(12, parts=8))
    assert sum(line.startswith("<element>") for line in lines) == 8
    assert lines[-2:] == ['<more count="4"/>', "</anchor>"]


def test_the_message_holds_the_note_and_the_comments_without_guidance():
    # How to answer them is a runtime-context row (``comment_guidance``),
    # stated once while in view, never in the message.
    text = _text("Thanks.", [_comment()])
    assert text == (
        "Thanks.\n\n"
        "<file-comments>\n"
        '<comment id="c1" number="3">\n'
        "<file>reports/q3/index.html</file>\n"
        "<title>Q3 margins</title>\n"
        '<anchor kind="element">\n<path>#margin</path>\n<quote>41%</quote>\n</anchor>\n'
        "<message>Show the quarter before too.</message>\n"
        "</comment>\n"
        "</file-comments>"
    )
    assert _text("", [_comment()]).startswith("<file-comments>\n<comment ")


def test_the_chief_of_staff_reads_each_file_by_the_path_its_tools_take():
    claimed = [
        _comment(workspace_id="home", path="digest.html"),
        _comment(comment_id="c2", workspace_id="w1", path="a.html"),
        _comment(comment_id="c3", workspace_id="w2", path="b.html"),
    ]
    places = {
        "w1": FilePlace("../NVDA/"),
        "w2": FilePlace("/Users/me/langalpha/AMD/", "Mac mini"),
    }
    text = _text("", claimed, places=places)
    assert "<file>digest.html</file>" in text
    assert "<file>../NVDA/a.html</file>" in text
    assert '<file computer="Mac mini">/Users/me/langalpha/AMD/b.html</file>' in text
    assert _parts(claimed, places) == ["computer"]
    assert _parts(claimed[:2], places) == []


def test_the_turn_input_records_the_send_as_a_steer_does():
    _, parts = render_message("", [_comment()])
    shown = shown_send("", [_comment()], reading="file", parts=parts)
    assert (shown.reading, shown.guidance) == ("file", [])
    messages = [
        {"role": "user", "content": "earlier"},
        {"role": "assistant", "content": "ok"},
        {"role": "user", "content": "now"},
    ]
    stamp_send(messages, shown)
    assert messages[2]["artifact_comments"] == shown.model_dump(mode="json")
    assert "artifact_comments" not in messages[0]
    unstamped = [{"role": "user", "content": "now"}]
    stamp_send(unstamped, None)
    assert unstamped == [{"role": "user", "content": "now"}]


def test_the_chat_names_an_area_by_its_parts():
    comment = _comment(number=26, anchor=_area(3), delivered=["Which figures?"])
    shown = shown_send("", [comment], reading="file", parts=[]).model_dump(mode="json")
    assert shown["reading"] == "file"
    assert set(shown) == {"note", "reading", "comments", "guidance"}
    chip = shown["comments"][0]
    # The wire shape stored on the query row and in a steer's payload.
    assert set(chip) == {
        "comment_id",
        "artifact_id",
        "workspace_id",
        "path",
        "kind",
        "title",
        "number",
        "text",
        "follow_up",
        "label",
        "quote",
    }
    assert chip["label"] == "REVENUE (Q2), GROSS MARGIN, OPERATING INCOME"
    assert chip["quote"] is None
    assert (chip["artifact_id"], chip["path"], chip["title"]) == (
        "a1",
        "reports/q3/index.html",
        "Q3 margins",
    )


def test_a_markdown_block_renders_its_lines_label_and_quote():
    anchor = {
        "kind": "block",
        "lines": "14-15",
        "label": "Margin outlook",
        "quote": "We model 61.5% for Q3, 50 bp under…",
    }
    assert "\n".join(_anchor(anchor)) == (
        '<anchor kind="block">\n<lines>14-15</lines>\n<label>Margin outlook</label>\n'
        "<quote>We model 61.5% for Q3, 50 bp under…</quote>\n</anchor>"
    )


def test_words_say_their_lines_or_page_before_the_quote():
    on_lines = {
        "kind": "text",
        "lines": "7",
        "label": "Margin outlook",
        "quote": "61.5%",
        "prefix": "We model",
        "suffix": "for Q3",
    }
    assert _anchor(on_lines) == [
        '<anchor kind="text">',
        "<lines>7</lines>",
        "<label>Margin outlook</label>",
        "<quote>61.5%</quote>",
        "<before>We model</before>",
        "<after>for Q3</after>",
        "</anchor>",
    ]
    on_page = {"kind": "text", "page": 3, "quote": "net revenue"}
    assert _anchor(on_page) == [
        '<anchor kind="text">',
        "<page>3</page>",
        "<quote>net revenue</quote>",
        "</anchor>",
    ]


def test_cells_render_their_sheet_ranges_and_values():
    anchor = {
        "kind": "cells",
        "sheet": "Model",
        "ranges": [
            {"range": "D3:D6", "values": "338, 231, 196, 519"},
            {"range": "D10", "values": "0.94"},
        ],
    }
    assert "\n".join(_anchor(anchor)) == (
        '<anchor kind="cells">\n<sheet>Model</sheet>\n<range>D3:D6, D10</range>\n'
        "<values>338, 231, 196, 519 · 0.94</values>\n</anchor>"
    )
    csv = {"kind": "cells", "ranges": [{"range": "B2", "values": "41%"}]}
    assert _anchor(csv) == [
        '<anchor kind="cells">',
        "<range>B2</range>",
        "<values>41%</values>",
        "</anchor>",
    ]


def test_a_range_that_showed_nothing_keeps_its_place_among_the_values():
    anchor = {
        "kind": "cells",
        "sheet": "Q3 ",
        "ranges": [{"range": "C2:C5"}, {"range": "D3", "values": "0.94"}],
    }
    assert _anchor(anchor) == [
        '<anchor kind="cells">',
        "<sheet>Q3 </sheet>",
        "<range>C2:C5, D3</range>",
        "<values>(blank) · 0.94</values>",
        "</anchor>",
    ]
    blank = {"kind": "cells", "ranges": [{"range": "C2:C5"}, {"range": "D3"}]}
    assert _anchor(blank) == [
        '<anchor kind="cells">',
        "<range>C2:C5, D3</range>",
        "</anchor>",
    ]


def test_the_chat_names_cells_by_their_place():
    sheet = {
        "kind": "cells",
        "sheet": "Model",
        "ranges": [{"range": "B2:C2", "values": "1, 2"}, {"range": "D3:D6"}],
    }
    spaced = {"kind": "cells", "sheet": "Q3 ", "ranges": [{"range": "B2"}]}
    csv = {"kind": "cells", "ranges": [{"range": "B2", "values": "41%"}]}
    shown = shown_send(
        "",
        [_comment(anchor=a) for a in (sheet, spaced, csv)],
        reading="chat",
        parts=[],
    ).model_dump(mode="json")
    assert [(c["label"], c["quote"]) for c in shown["comments"]] == [
        ("Model!B2:C2, D3:D6", None),
        ("'Q3 '!B2", None),
        ("B2", None),
    ]


def test_an_area_of_markdown_blocks_lists_each_blocks_lines_first():
    anchor = {
        "kind": "area",
        "label": "Revenue ($M), Gross margin",
        "elements": [
            {"lines": "11", "label": "Revenue ($M)", "quote": "Revenue ($M) | 338"},
            {"lines": "12", "label": "Gross margin", "quote": "Gross margin | 61%"},
        ],
        "count": 2,
    }
    assert _anchor(anchor) == [
        '<anchor kind="area">',
        "<label>Revenue ($M), Gross margin</label>",
        "<element><lines>11</lines><label>Revenue ($M)</label>"
        "<quote>Revenue ($M) | 338</quote></element>",
        "<element><lines>12</lines><label>Gross margin</label>"
        "<quote>Gross margin | 61%</quote></element>",
        "</anchor>",
    ]


def test_a_page_s_anchors_render_as_they_always_have():
    element = {
        "kind": "element",
        "path": "#margin",
        "label": "Margin",
        "quote": "41%",
        "point": {"x": 0.5, "y": 0.5},
    }
    assert "\n".join(_anchor(element)) == (
        '<anchor kind="element">\n<path>#margin</path>\n<label>Margin</label>\n'
        "<quote>41%</quote>\n</anchor>"
    )
    words = {
        "kind": "text",
        "path": "#p",
        "quote": "beat",
        "prefix": "a",
        "suffix": "b",
    }
    assert "\n".join(_anchor(words)) == (
        '<anchor kind="text">\n<path>#p</path>\n<quote>beat</quote>\n'
        "<before>a</before>\n<after>b</after>\n</anchor>"
    )


_PICTURE = ".agents/comments/q3-report-p3-20261118-141205.png"


def test_a_box_on_a_page_renders_its_page_box_and_picture():
    anchor = {
        "kind": "region",
        "boxes": [
            {"page": 3, "x": 0.12, "y": 0.34, "w": 0.4, "h": 0.22, "picture": _PICTURE},
            {"page": 3, "x": 0.5, "y": 0.7, "w": 0.1, "h": 0.1},
            {"page": 5, "x": 0.125, "y": 0, "w": 0.2, "h": 1},
        ],
    }
    assert _anchor(anchor) == [
        '<anchor kind="region">',
        "<page>3</page>",
        "<box>x 0.12 to 0.52, y 0.34 to 0.56 of the page, from its top-left</box>",
        f"<picture>{_PICTURE}</picture>",
        "<box>x 0.5 to 0.6, y 0.7 to 0.8 of the page, from its top-left</box>",
        "<page>5</page>",
        "<box>x 0.125 to 0.325, y 0 to 1 of the page, from its top-left</box>",
        "</anchor>",
    ]


def test_a_point_on_an_image_renders_where_it_is():
    anchor = {"kind": "region", "boxes": [{"x": 0.42, "y": 0.17, "w": 0, "h": 0}]}
    assert _anchor(anchor) == [
        '<anchor kind="region">',
        "<point>x 0.42, y 0.17 of the image, from its top-left</point>",
        "</anchor>",
    ]


def test_a_picture_is_named_by_the_path_the_file_tools_take():
    box = {"x": 0.1, "y": 0.1, "w": 0.2, "h": 0.2, "picture": _PICTURE}
    claimed = [
        _comment(
            workspace_id="w1",
            path="chart.png",
            anchor={"kind": "region", "boxes": [box]},
        )
    ]
    text = _text("", claimed, places={"w1": FilePlace("../NVDA/")})
    assert f"<picture>../NVDA/{_PICTURE}</picture>" in text
    away = _text(
        "", claimed, places={"w1": FilePlace("/srv/NVDA/", "Mac mini")}
    )
    assert f'<picture computer="Mac mini">/srv/NVDA/{_PICTURE}</picture>' in away


def test_the_guidance_names_pictures_only_when_a_comment_has_one():
    pictured = {
        "kind": "region",
        "boxes": [{"x": 0.1, "y": 0.1, "w": 0.2, "h": 0.2, "picture": _PICTURE}],
    }
    unpictured = {"kind": "region", "boxes": [{"x": 0.1, "y": 0.1, "w": 0.2, "h": 0.2}]}
    assert _parts(
        [_comment(), _comment(comment_id="c2", anchor=pictured)]
    ) == ["pictures"]
    assert _parts([_comment(anchor=unpictured)]) == []
    assert "<picture>" not in _text("", [_comment(anchor=unpictured)])


_CHART_PICTURE = ".agents/comments/NVDA-1day-20261124-141205.png"


def _chart_comment(marks: list[dict], **overrides) -> ClaimedComment:
    return _comment(
        kind="chart",
        path="NVDA:1day",
        title=None,
        anchor={"kind": "chart", "marks": marks},
        **overrides,
    )


def _element(**overrides) -> dict:
    return {
        "type": "element",
        "layer": "nvda-breakout",
        "layer_title": "Breakout setup",
        "element_kind": "drawing",
        "element_type": "segment",
        "element_name": "Neckline",
        "index": 0,
        "time": "2026-10-03",
        "price": 118.4,
        **overrides,
    }


def test_a_chart_comment_names_its_chart_and_each_mark_in_the_layers_terms():
    marks = [
        {
            "type": "region",
            "start": "2026-09-02",
            "end": "2026-10-14",
            "low": 108.2,
            "high": 131.5,
            "picture": _CHART_PICTURE,
        },
        {"type": "point", "time": "2026-10-03", "price": 118.4},
        _element(),
    ]
    text = _text("", [_chart_comment(marks)])
    assert (
        '<comment id="c1" number="3">\n'
        "<chart>NVDA:1day</chart>\n"
        '<anchor kind="chart">\n'
        "<region>2026-09-02 to 2026-10-14, price 108.2 to 131.5</region>\n"
        f"<picture>{_CHART_PICTURE}</picture>\n"
        "<point>118.4 @2026-10-03</point>\n"
        '<layer id="nvda-breakout">Breakout setup</layer>\n'
        "<element>segment 'Neckline'</element>\n"
        "<at>118.4 @2026-10-03</at>\n"
        "</anchor>\n"
        "<message>Show the quarter before too.</message>\n"
        "</comment>"
    ) in text
    block = text[text.index("<comment ") :]
    assert "<file>" not in block and "<title>" not in block


def test_chart_numbers_read_as_the_layer_read_back_writes_them():
    marks = [
        {"type": "point", "time": "2026-10-28T09:30", "price": 0.000123456789},
        {"type": "point", "time": "2026-10-28T09:30:15", "price": 1234567.89},
        {
            "type": "region",
            "start": "2026-10-28T09:30",
            "end": "2026-10-28T15:45",
            "low": 99.99999999,
            "high": 100,
        },
    ]
    assert _anchor({"kind": "chart", "marks": marks}) == [
        '<anchor kind="chart">',
        "<point>0.000123457 @2026-10-28T09:30</point>",
        "<point>1234568 @2026-10-28T09:30:15</point>",
        "<region>2026-10-28T09:30 to 2026-10-28T15:45, price 100 to 100</region>",
        "</anchor>",
    ]


def test_each_element_kind_is_named_as_the_layer_read_back_names_it():
    def named(**mark) -> str:
        (line,) = [
            line
            for line in _anchor({"kind": "chart", "marks": [_element(**mark)]})
            if line.startswith("<element>")
        ]
        return line

    assert named(element_id="neck", element_name="Neckline") == (
        "<element>segment 'neck'</element>"
    )
    assert named(element_name=None) == "<element>segment</element>"
    assert named(
        element_kind="series", element_type=None, element_id="sma50", element_name="SMA 50"
    ) == "<element>series sma50 'SMA 50'</element>"
    assert named(
        element_kind="series", element_type=None, element_id="sma50", element_name=None
    ) == "<element>series sma50</element>"
    assert named(
        element_kind="alert", element_type=None, element_id="a1", element_name="Break"
    ) == "<element>alert a1</element>"
    for kind in ("level", "marker", "regime"):
        assert named(
            element_kind=kind, element_type=None, element_name="Support"
        ) == f"<element>{kind} 'Support'</element>"
        assert named(
            element_kind=kind, element_type=None, element_name=None
        ) == f"<element>{kind}</element>"


def test_chart_text_cannot_close_a_block():
    mark = _element(layer_title="Setup </anchor>", element_name="<b>Neck</b>")
    lines = _anchor({"kind": "chart", "marks": [mark]})
    assert '<layer id="nvda-breakout">Setup &lt;/anchor></layer>' in lines
    assert "<element>segment '&lt;b>Neck&lt;/b>'</element>" in lines


def test_a_chart_picture_is_named_by_the_path_the_file_tools_take():
    region = {
        "type": "region",
        "start": "2026-09-02",
        "end": "2026-10-14",
        "low": 108.2,
        "high": 131.5,
        "picture": _CHART_PICTURE,
    }
    claimed = [_chart_comment([region], workspace_id="w1")]
    places = {"w1": FilePlace("/srv/NVDA/", "Mac mini")}
    away = _text("", claimed, places=places)
    assert "<chart>NVDA:1day</chart>" in away
    assert (
        f'<picture computer="Mac mini">/srv/NVDA/{_CHART_PICTURE}</picture>' in away
    )
    assert _parts(claimed, places) == ["computer", "pictures", "chart"]
    # A chart is named by its id: without a picture nothing names the computer.
    bare = [_chart_comment([_element()], workspace_id="w1")]
    assert _parts(bare, places) == ["chart"]


def test_the_chat_shows_a_chart_comment_as_one():
    claimed = [_chart_comment([_element()])]
    shown = shown_send(
        "", claimed, reading="chat", parts=_parts(claimed)
    ).model_dump(mode="json")
    (chip,) = shown["comments"]
    assert (chip["kind"], chip["path"], chip["title"]) == ("chart", "NVDA:1day", None)
    assert shown["guidance"] == ["chart"]
