"""Grep content mode prints what plain rg would, except that a long line comes
back as windows around its matches.

A JSONL transcript holds a whole event per line, so one match inside a pasted
document or a large tool result returned the entire line, over a million
characters in one live case. The offsets come from rg itself, so the cut
never re-runs the pattern in Python.
"""

from __future__ import annotations

import json

from ptc_agent.core.sandbox.grep_render import (
    GREP_LINE_CHARS,
    GrepLine,
    render_grep_json,
)


def _event(kind: str, path: str, number: int, text: str, *spans: tuple[int, int]) -> str:
    raw = text.encode()
    return json.dumps(
        {
            "type": kind,
            "data": {
                "path": {"text": path},
                "lines": {"text": text},
                "line_number": number,
                "absolute_offset": 0,
                "submatches": [
                    {"match": {"text": raw[s:e].decode()}, "start": s, "end": e}
                    for s, e in spans
                ],
            },
        }
    )


def _render(*events: str, search: str = "/w", numbers: bool = True, grouped: bool = False):
    summary = json.dumps({"type": "summary", "data": {}})
    return render_grep_json(
        "\n".join([*events, summary]), search, line_numbers=numbers, grouped=grouped
    )


def test_short_lines_print_as_plain_rg_prints_them():
    assert _render(
        _event("match", "/w/a.txt", 2, "beta two\n", (0, 4)),
        _event("match", "/w/b.txt", 1, "beta only\n", (0, 4)),
    ) == ["/w/a.txt:2:beta two", "/w/b.txt:1:beta only"]


def test_context_lines_and_group_breaks_match_plain_rg():
    assert _render(
        _event("context", "/w/a.txt", 1, "alpha\n"),
        _event("match", "/w/a.txt", 2, "beta\n", (0, 4)),
        _event("match", "/w/a.txt", 8, "theta beta\n", (6, 10)),
        _event("match", "/w/b.txt", 1, "beta\n", (0, 4)),
        grouped=True,
    ) == [
        "/w/a.txt-1-alpha",
        "/w/a.txt:2:beta",
        "--",
        "/w/a.txt:8:theta beta",
        "--",
        "/w/b.txt:1:beta",
    ]


def test_a_single_file_search_omits_the_path_and_numbers_are_optional():
    assert _render(
        _event("match", "/w/a.txt", 2, "beta two\n", (0, 4)), search="/w/a.txt"
    ) == ["2:beta two"]
    assert _render(
        _event("match", "/w/a.txt", 2, "beta two\n", (0, 4)), numbers=False
    ) == ["/w/a.txt:beta two"]


def test_a_multiline_match_prints_each_of_its_lines():
    assert _render(
        _event("match", "/w/a.txt", 1, "alpha one\nbeta two\n", (6, 14))
    ) == ["/w/a.txt:1:alpha one", "/w/a.txt:2:beta two"]


def test_a_long_line_is_cut_to_windows_around_its_first_matches():
    text = "x" * 50_000 + "NEEDLE-1" + "y" * 50_000 + "NEEDLE-2" + "z" * 50_000 + "\n"
    first = 50_000
    second = first + 8 + 50_000
    [line] = _render(
        _event("match", "/w/t.jsonl", 1, text, (first, first + 8), (second, second + 8))
    )
    assert line.startswith("/w/t.jsonl:1:…")
    assert "NEEDLE-1" in line and "NEEDLE-2" in line
    assert line.endswith("[line of 150,016 chars, cut around its 2 matches]")
    assert len(line) < 1_000


def test_a_line_with_many_matches_shows_the_first_few_and_counts_the_rest():
    text = "hit " * 1_000 + "\n"
    spans = [(i * 4, i * 4 + 3) for i in range(1_000)]
    [line] = _render(_event("match", "/w/t.jsonl", 1, text, *spans))
    assert line.endswith("cut around 3 of its 1000 matches]")
    assert len(line) < GREP_LINE_CHARS * 2


def test_a_long_context_line_keeps_its_head():
    [line] = _render(_event("context", "/w/t.jsonl", 3, "q" * 5_000 + "\n"))
    assert line == f"/w/t.jsonl-3-{'q' * GREP_LINE_CHARS}… [line of 5,000 chars, cut]"


def test_a_window_never_splits_a_character():
    text = "é" * 2_000 + "KEY" + "é" * 2_000 + "\n"
    start = len("é".encode()) * 2_000
    [line] = _render(_event("match", "/w/t.txt", 1, text, (start, start + 3)))
    assert "KEY" in line and "�" not in line


# rg's stderr as the exec returns it, folded into the output. Captured from
# rg 15 with ``rg --json '(?=x)' . 2>&1`` and a missing path.
_RG_LOOKAROUND_ERROR = """\
rg: regex parse error:
    (?:(?=x))
       ^^^
error: look-around, including look-ahead and look-behind, is not supported

Consider enabling PCRE2 with the --pcre2 flag, which can handle backreferences
and look-around."""


def test_a_pattern_rg_rejects_returns_its_error_not_an_empty_result():
    assert render_grep_json(
        _RG_LOOKAROUND_ERROR, "/w", line_numbers=True, grouped=False
    ) == _RG_LOOKAROUND_ERROR.splitlines()


def test_rg_errors_follow_the_matches_in_their_order():
    output = "\n".join(
        [
            "rg: /w/nope.txt: No such file or directory (os error 2)",
            _event("match", "/w/a.txt", 2, "beta x\n", (0, 4)),
            "rg: /w/locked: Permission denied (os error 13)",
        ]
    )
    assert render_grep_json(output, "/w", line_numbers=True, grouped=False) == [
        "/w/a.txt:2:beta x",
        "rg: /w/nope.txt: No such file or directory (os error 2)",
        "rg: /w/locked: Permission denied (os error 13)",
    ]


def test_each_line_holds_its_file_whatever_the_name_holds():
    """A name may hold the ``:`` or ``-`` that ends it in the line, so each
    line carries its path apart from the text, and a group's ``--`` and an
    error carry none."""
    lines = _render(
        _event("context", "/w/Q1: a-2-b.md", 1, "alpha\n"),
        _event("match", "/w/Q1: a-2-b.md", 2, "beta\n", (0, 4)),
        _event("match", "/w/n-1-x", 9, "beta\n", (0, 4)),
        grouped=True,
    )

    assert lines == [
        "/w/Q1: a-2-b.md-1-alpha",
        "/w/Q1: a-2-b.md:2:beta",
        "--",
        "/w/n-1-x:9:beta",
    ]
    assert [getattr(line, "path", None) for line in lines] == [
        "/w/Q1: a-2-b.md",
        "/w/Q1: a-2-b.md",
        None,
        "/w/n-1-x",
    ]
    assert lines[1].at("/v/Q1: a-2-b.md") == "/v/Q1: a-2-b.md:2:beta"


def test_a_line_that_names_no_file_keeps_its_text_when_respelled():
    (line,) = _render(_event("match", "/w/a:1:b", 3, "beta\n", (0, 4)), search="/w/a:1:b")

    assert line == "3:beta"
    moved = line.at("/v/a:1:b")
    assert (moved, moved.path) == ("3:beta", "/v/a:1:b")
    assert isinstance(moved, GrepLine)
