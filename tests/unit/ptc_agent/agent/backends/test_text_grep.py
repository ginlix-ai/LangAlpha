"""The whole-text pre-pass and the paged search over server-held text.

The pre-pass rules a file out unread when no line of it can match, so a
slip in it drops matches without a word: what is pinned here is that it
never says no where the line-by-line search says yes.
"""

from __future__ import annotations

import random

import pytest

from ptc_agent.agent.backends.text_grep import (
    PageGrep,
    compile_grep,
    grep_texts,
    may_match,
    whole_text,
)

PATTERNS = [
    "foo",
    "^foo",
    "foo$",
    "^$",
    "^",
    "$",
    "o+",
    "[^a]",
    r"\s",
    r"\S+$",
    "a.b",
    "(?i)FOO",
    "x*",
    ".*",
    r"\bfoo\b",
    "foo|^bar$",
    "(?s)a.b",
    r"\n",
    r"[\s\S]",
    r"^\s*$",
    "é",
    r"\x{e9}",
    r"\pL+$",
    "(?m)^b",
    "(?U)a+?b",
    r"\d{2,}",
    "[[:upper:]]",
]

TEXTS = [
    "",
    "foo",
    "foo\n",
    "bar\nfoo\n",
    "a\nb",
    "a\r\nb\r\n",
    "a\rb",
    "a b",
    "a\x85b",
    "a\x0bb\x0cc",
    "\n\n",
    "   \n\t\n",
    "café\nnaïve\n",
    "FOO bar\n",
    "xx 12 yy\n",
    "Upper\nlower",
    "foo\r\n\r\nbar",
    "ab\n",
    "aab\n",
]


def _line_hit(pattern: str, text: str, ci: bool) -> bool:
    compiled = compile_grep(pattern, ci)
    return any(compiled.search(line) for line in text.splitlines())


@pytest.mark.parametrize("ci", [False, True])
@pytest.mark.parametrize("pattern", PATTERNS)
def test_the_pass_never_rules_out_a_file_a_line_matches(pattern: str, ci: bool) -> None:
    whole = whole_text(pattern, ci)
    for text in TEXTS:
        if whole is not None and _line_hit(pattern, text, ci):
            assert may_match(text.encode(), whole), (pattern, text)


def test_the_pass_holds_over_random_texts_and_patterns() -> None:
    rng = random.Random(7)
    atoms = ["a", "b", "é", " ", ".", "^", "$", r"\s", r"\w", "[ab]", "[^a]", "a*", "b+", "(a|b)"]
    chars = ["a", "b", "é", " ", "\n", "\r", "\r\n", " ", "\t"]
    for _ in range(2000):
        pattern = "".join(rng.choice(atoms) for _ in range(rng.randint(1, 4)))
        text = "".join(rng.choice(chars) for _ in range(rng.randint(0, 12)))
        ci = rng.random() < 0.3
        whole = whole_text(pattern, ci)
        if whole is not None and _line_hit(pattern, text, ci):
            assert may_match(text.encode(), whole), (pattern, text, ci)


@pytest.mark.parametrize("pattern", [r"\Afoo", r"foo\z", "(?-m)^foo", "(?i-m)foo$"])
def test_a_pattern_anchored_to_the_whole_text_gets_no_pass(pattern: str) -> None:
    assert whole_text(pattern, False) is None


def test_the_pass_rules_out_a_file_no_line_matches() -> None:
    whole = whole_text("needle", False)
    assert whole is not None
    assert not may_match(b"hay\nstack\n", whole)
    # A match across a line end is no line's match.
    assert not may_match(b"nee\ndle\n", whole)


async def _pages(items, *, size: int = 500, fail_after: int | None = None):
    for index, start in enumerate(range(0, len(items), size)):
        if fail_after is not None and index == fail_after:
            raise RuntimeError("source stopped answering")
        yield items[start : start + size]


class TestPageGrep:
    @pytest.mark.asyncio
    async def test_it_answers_as_grep_texts_does(self) -> None:
        files = [(f"/f{i}.md", f"line {i}\nZEPH {i}\n".encode()) for i in range(30)]
        search = PageGrep("ZEPH", case_insensitive=False, output_mode="content", show_line_numbers=True)

        await search.search(_pages(files, size=7))

        expected = grep_texts(
            [(p, d.decode()) for p, d in files],
            compile_grep("ZEPH", False),
            "content",
            show_line_numbers=True,
            cut_long_lines=True,
        )
        assert search.found == expected

    @pytest.mark.asyncio
    @pytest.mark.parametrize("read_ahead", [False, True])
    async def test_it_stops_at_the_limit(self, read_ahead: bool) -> None:
        files = [(f"/f{i}.md", b"ZEPH\n") for i in range(10)]
        asked: list[int] = []

        async def pages():
            for start in range(0, len(files), 2):
                asked.append(start)
                yield files[start : start + 2]

        search = PageGrep(
            "ZEPH", case_insensitive=False, output_mode="files_with_matches",
            show_line_numbers=True, limit=3,
        )

        await search.search(pages(), read_ahead=read_ahead)

        assert search.found == ["/f0.md", "/f1.md", "/f2.md"]
        # Without reading ahead, no page past the one that filled it; with
        # it, one page at most, and whether that fetch had begun before the
        # search ended is the event loop's to decide.
        assert asked in ([[0, 2], [0, 2, 4]] if read_ahead else [[0, 2]])

    @pytest.mark.asyncio
    @pytest.mark.parametrize("read_ahead", [False, True])
    async def test_a_source_that_fails_keeps_what_arrived(self, read_ahead: bool) -> None:
        files = [(f"/f{i}.md", b"ZEPH\n") for i in range(5)]
        search = PageGrep(
            "ZEPH", case_insensitive=False, output_mode="files_with_matches", show_line_numbers=True,
        )

        with pytest.raises(RuntimeError):
            await search.search(_pages(files, size=1, fail_after=2), read_ahead=read_ahead)

        assert search.found == ["/f0.md", "/f1.md"]

    @pytest.mark.asyncio
    async def test_a_source_not_held_to_utf8_is_not_ruled_out_on_its_bytes(self) -> None:
        # A replaced byte reads as U+FFFD, which the bytes themselves never match.
        files = [("/bad.jsonl", b"x\xffy\n"), ("/ok.jsonl", b"plain\n")]
        search = PageGrep(
            "�", case_insensitive=False, output_mode="files_with_matches", show_line_numbers=True,
        )

        await search.search(_pages(files))

        assert search.found == ["/bad.jsonl"]
