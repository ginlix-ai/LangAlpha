"""Glob's ``{a,b}`` groups, expanded as a shell expands them.

Models write brace patterns because every glob they know takes them, and
fnmatch alone reads ``{a,b}`` as one literal name, so such a Glob found
nothing. ``find`` runs as the sandbox runs it, over a real tree.
"""

import os

import pytest

from ptc_agent.core.sandbox.glob_runtime import (
    MAX_BRACE_PATTERNS,
    TooManyPatterns,
    expand_braces,
    find,
)


@pytest.mark.parametrize(
    ("pattern", "expanded"),
    [
        ("docs/{price,macro}/*.md", ["docs/price/*.md", "docs/macro/*.md"]),
        ("*.{py,md}", ["*.py", "*.md"]),
        ("a{b,c}d{e,f}", ["abde", "abdf", "acde", "acdf"]),
        ("{a,b{c,d}}", ["a", "bc", "bd"]),
        ("a{,b}", ["a", "ab"]),
        ("{a,a}", ["a"]),
        # An alternative that leaves nothing is dropped, as a shell drops it.
        ("{a.md,}", ["a.md"]),
        ("{,}", []),
        # A group with no comma of its own is literal, while one inside it
        # still expands.
        ("{a}", ["{a}"]),
        ("{}", ["{}"]),
        ("{a{b,c}}", ["{ab}", "{ac}"]),
        # A `{` nothing closes, or a stray `}`, is a plain character.
        ("x{a,b", ["x{a,b"]),
        ("{a,{b,c}", ["{a,b", "{a,c"]),
        ("{a,b}c}", ["ac}", "bc}"]),
        # A backslash makes a brace or comma literal, and is dropped.
        ("\\{a,b\\}", ["{a,b}"]),
        ("{a\\,b,c}", ["a,b", "c"]),
        # Inside a class, braces and commas are the class's own.
        ("[{]a,b}", ["[{]a,b}"]),
        ("[{a,b}]", ["[{a,b}]"]),
        ("[]{a,b}]x{c,d}", ["[]{a,b}]xc", "[]{a,b}]xd"]),
        ("[{a,b}", ["[a", "[b"]),
    ],
)
def test_braces_expand_as_a_shell_expands_them(pattern, expanded):
    assert expand_braces(pattern) == expanded


def test_a_pattern_expanding_past_the_cap_is_refused():
    assert len(expand_braces("{a,b}" * 6)) == MAX_BRACE_PATTERNS
    with pytest.raises(TooManyPatterns):
        expand_braces("{a,b}" * 7)


def _find(pattern: str, root) -> list[str]:
    found = find(pattern, str(root), [], {}, "/nonexistent-mount")
    return [os.path.relpath(path, root) for path in found]


def test_each_alternative_is_found_newest_first_and_once(tmp_path):
    for age, rel in enumerate(
        ("docs/macro/b.md", "docs/price/a.md", "docs/options/c.md", "a.md")
    ):
        (tmp_path / rel).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / rel).write_text("x")
        os.utime(tmp_path / rel, (1_000_000 - age, 1_000_000 - age))

    # Newest first across the alternatives, not in their order.
    assert _find("docs/{price,macro}/*.md", tmp_path) == [
        "docs/macro/b.md",
        "docs/price/a.md",
    ]
    # Each alternative is a pattern of its own, so a bare name looks at
    # every depth and a file two of them name is listed once.
    assert _find("{a.md,docs/*/a.md}", tmp_path) == ["docs/price/a.md", "a.md"]
    # Nor twice when two alternatives spell it two ways.
    assert _find("{docs/price/a.md,./docs/price/a.md}", tmp_path) == [
        "docs/price/a.md"
    ]


def test_a_file_gone_before_the_sort_leaves_the_rest_sorted(tmp_path, monkeypatch):
    for age, name in enumerate(("new.md", "gone.md", "old.md")):
        (tmp_path / name).write_text("x")
        os.utime(tmp_path / name, (1_000_000 - age, 1_000_000 - age))
    getmtime = os.path.getmtime

    def vanished(path):
        if path.endswith("gone.md"):
            raise FileNotFoundError(path)
        return getmtime(path)

    monkeypatch.setattr(os.path, "getmtime", vanished)
    assert _find("*.md", tmp_path) == ["new.md", "old.md", "gone.md"]


def test_a_literal_brace_in_a_file_name_is_found(tmp_path):
    (tmp_path / "v{1,2}.md").write_text("x")

    assert _find("v[{]1,2}.md", tmp_path) == ["v{1,2}.md"]
    assert _find("v\\{1,2}.md", tmp_path) == ["v{1,2}.md"]
    assert _find("v{1,2}.md", tmp_path) == []
