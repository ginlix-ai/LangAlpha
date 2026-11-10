"""Conversation search's pure contracts: terms, snippets with their match
ranges (UTF-16 code units, as the palette slices them), and page cursors."""

import time
from datetime import datetime, timezone
from uuid import UUID

import pytest

from src.server.database.search.sql import Keyset
from src.server.services.search.text import (
    MAX_TERMS,
    Cursor,
    clean_markdown,
    decode_cursor,
    encode_cursor,
    match_ranges,
    snippet,
    split_terms,
)


def _marked(text: str, ranges: list[list[int]]) -> list[str]:
    """The text each range covers, read the way JavaScript slices a string."""
    units = text.encode("utf-16-le")
    return [units[2 * s : 2 * e].decode("utf-16-le") for s, e in ranges]


class TestTerms:
    def test_splits_on_any_whitespace_including_ideographic(self):
        assert split_terms("  nvidia\tmargin\u3000毛利率 ") == ["nvidia", "margin", "毛利率"]

    def test_keeps_each_term_once_whatever_its_case(self):
        assert split_terms("Apple apple APPLE pie") == ["Apple", "pie"]

    def test_caps_the_number_of_terms(self):
        assert len(split_terms(" ".join(f"t{i}" for i in range(20)))) == MAX_TERMS

    def test_blank_has_no_terms(self):
        assert split_terms("   ") == []


class TestCleanMarkdown:
    def test_strips_bold_headings_backticks_and_link_targets(self):
        text = "## Summary\n\n**Gross margin** rose, see [the filing](https://sec.gov/x) and `calc()`.\n![chart](a.png)"
        assert clean_markdown(text) == "Summary Gross margin rose, see the filing and calc(). chart"

    def test_keeps_a_hash_that_is_not_a_heading(self):
        assert clean_markdown("ticker #1 and C#") == "ticker #1 and C#"


class TestSnippet:
    def test_short_text_is_whole_with_its_matches(self):
        text, ranges = snippet("Gross **margin** rose", ["margin"])
        assert text == "Gross margin rose"
        assert _marked(text, ranges) == ["margin"]

    def test_long_text_is_cut_around_the_first_match_at_spaces(self):
        words = " ".join(f"word{i}" for i in range(200))
        text, ranges = snippet(words + " needle " + words, ["needle"])
        assert text.startswith("…") and text.endswith("…")
        assert len(text) <= 162
        assert _marked(text, ranges) == ["needle"]
        # Cut at spaces: no partial word at either edge.
        assert text[1:].split(" ")[0].startswith("word")
        assert text[1].isalpha() and text[-2].isdigit()
        inner = text[1:-1].split(" ")
        assert all(w == "needle" or w.startswith("word") for w in inner)

    def test_match_near_the_start_has_no_leading_ellipsis(self):
        text, _ = snippet("needle " + "x " * 200, ["needle"])
        assert text.startswith("needle") and text.endswith("…")

    def test_match_near_the_end_fills_the_window_before_it(self):
        text, ranges = snippet("y " * 200 + "needle", ["needle"])
        assert text.startswith("…") and text.endswith("needle")
        assert len(text) >= 150
        assert _marked(text, ranges) == ["needle"]

    def test_cjk_text_without_spaces_is_cut_by_characters(self):
        body = "公司本季度营收增长" * 30 + "毛利率显著提升" + "现金流稳定" * 30
        text, ranges = snippet(body, ["毛利率"])
        assert text.startswith("…") and text.endswith("…")
        assert _marked(text, ranges) == ["毛利率"]

    def test_ranges_count_utf16_units_past_an_emoji(self):
        text, ranges = snippet("🚀 margin up 🚀🚀 Margin", ["margin"])
        assert ranges == [[3, 9], [18, 24]]
        assert _marked(text, ranges) == ["margin", "Margin"]

    def test_every_term_is_marked_and_overlaps_merge(self):
        text, ranges = snippet("the gross margin and margins", ["margin", "gin", "gross"])
        assert _marked(text, ranges) == ["gross", "margin", "margin"]

    def test_ranges_are_into_the_cleaned_text(self):
        text, ranges = snippet("# Title\n**Bold** then `needle`", ["needle"])
        assert text == "Title Bold then needle"
        assert _marked(text, ranges) == ["needle"]

    def test_a_match_only_the_markup_held_keeps_the_markup(self):
        text, ranges = snippet("See [the filing](https://sec.gov/edgar)", ["edgar"])
        assert "sec.gov/edgar" in text
        assert _marked(text, ranges) == ["edgar"]

    def test_a_match_deep_in_a_long_answer_is_cut_from_around_it(self):
        body = "[link](https://example.com/" + "x" * 50 + ") " + "filler " * 3000
        text, ranges = snippet(body + "the needle here " + body, ["needle"])
        assert text.startswith("…") and text.endswith("…")
        assert _marked(text, ranges) == ["needle"]
        assert "link" in snippet("[link](u) " + "a " * 3000 + "needle", ["link"])[0]

    def test_a_match_the_markup_held_gives_way_to_a_shown_one_far_past_it(self):
        body = "See [the filing](https://sec.gov/edgar) " + "filler " * 400 + "edgar search"
        text, ranges = snippet(body, ["edgar"])
        assert "sec.gov" not in text and "](" not in text
        assert _marked(text, ranges) == ["edgar"]

    @pytest.mark.parametrize(
        "body",
        [
            "[a](" * 8000,
            "![a](" * 6400,
            "[" * 32000,
            "[a" * 16000 + "](x)",
            # The term only ever in link addresses: every window tried.
            ("[x](https://e.com/needle) " + "y " * 500) * 30,
        ],
    )
    def test_markup_built_to_backtrack_stays_fast(self, body):
        # Unclosed links over a whole 32K answer were quadratic to clean. The
        # cleanup now runs in a window around the match, so four times the
        # text costs about the same; quadratic would cost sixteen times. A
        # ratio, not a deadline, so a loaded CI runner can't fail it.
        def cost(text: str) -> float:
            best = float("inf")
            for _ in range(3):
                started = time.perf_counter()
                for terms in (["a"], ["absent"], ["needle"]):
                    snippet(text, terms)
                best = min(best, time.perf_counter() - started)
            return best

        assert cost(body) < 3 * cost(body[: len(body) // 4]) + 0.005

    def test_no_match_is_the_start_of_the_text(self):
        text, ranges = snippet("plain words " * 40, ["absent"])
        assert text.startswith("plain words") and ranges == []


def test_match_ranges_are_case_insensitive():
    assert match_ranges("NVDA nvda Nvda", ["nvda"]) == [[0, 4], [5, 9], [10, 14]]


class TestCursor:
    def test_round_trips_through_the_key_its_read_checks(self):
        at = datetime(2026, 11, 9, 12, 30, 1, 123456, tzinfo=timezone.utc)
        row_key = (at, "6f1c2a3e-0000-4000-8000-000000000001", 7)
        decoded = decode_cursor(encode_cursor(Cursor("turns", row_key)))
        assert decoded.kind == "turns"
        page = Keyset((("a", datetime), ("b", UUID), ("c", int)))
        params: dict = {}
        page.after(decoded.key, params)
        assert tuple(params.values()) == row_key

    def test_cursor_is_url_safe(self):
        at = datetime(2026, 1, 2, tzinfo=timezone.utc)
        value = encode_cursor(
            Cursor("turns", (at, "6f1c2a3e-0000-4000-8000-000000000001", 0))
        )
        assert set(value) <= set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_")

    @pytest.mark.parametrize(
        "value",
        [
            "garbage!",
            "",
            # Valid base64 of things that are not cursors.
            "bnVsbA",  # null
            "eyJrIjoidHVybnMifQ",  # {"k":"turns"}
            "eyJrIjoibm9wZSIsImtleSI6W119",  # {"k":"nope","key":[]}
        ],
    )
    def test_anything_else_is_refused(self, value):
        with pytest.raises(ValueError):
            decode_cursor(value)


class TestShortLatinTerms:
    """A one- or two-letter Latin term is a word's start, as the palette reads it."""

    def test_ranges_only_at_word_starts(self):
        assert match_ranges("conventions on NVDA", ["nv"]) == [[15, 17]]
        assert match_ranges("conventions on NVDA", ["nvd"]) == [[15, 18]]
