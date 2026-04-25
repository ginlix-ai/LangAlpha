"""Unit tests for ``slugify_filename``.

Validates unicode stripping, whitespace normalization, extension preservation,
and collision suffixing. The key that comes out must pass
``validate_store_key`` unchanged — that's the contract memo.py relies on.
"""

from __future__ import annotations

import pytest

from ptc_agent.agent.backends.langgraph_store import validate_store_key
from ptc_agent.agent.memo.slug import slugify_filename


class TestBasicSlug:
    def test_spaces_become_dashes_and_lowercase(self):
        assert slugify_filename("Q1 2026 Thesis.md") == "q1-2026-thesis.md"

    def test_parens_and_punctuation_stripped(self):
        assert slugify_filename("Q1 (Final) Draft.pdf") == "q1-final-draft.pdf"

    def test_extension_preserved(self):
        assert slugify_filename("notes.TXT") == "notes.txt"

    def test_unicode_accents_folded(self):
        assert slugify_filename("Résumé.md") == "resume.md"

    def test_runs_of_dashes_collapse(self):
        assert slugify_filename("a   -- b.md") == "a-b.md"

    def test_leading_trailing_dashes_trimmed(self):
        assert slugify_filename("--a.md--") == "a.md"

    def test_empty_filename_falls_back(self):
        assert slugify_filename("") == "memo"

    def test_pure_unicode_falls_back(self):
        # CJK-only name with no extension — our slugifier strips everything,
        # but must still return a valid placeholder key.
        assert slugify_filename("备忘录") == "memo"

    def test_keeps_keys_path_safe(self):
        slug = slugify_filename("NVDA Earnings Q4 (FY26).pdf")
        # Must not raise — this is the invariant memo.py depends on.
        validate_store_key(slug)

    def test_long_name_truncates(self):
        name = "a" * 500 + ".md"
        slug = slugify_filename(name)
        # Base cap is 120 chars; extension then appended.
        assert slug.endswith(".md")
        base = slug[: -len(".md")]
        assert len(base) <= 120


class TestCollisions:
    def test_returns_base_when_unused(self):
        assert slugify_filename("notes.md", existing_keys=[]) == "notes.md"

    def test_appends_2_on_first_collision(self):
        assert (
            slugify_filename("notes.md", existing_keys=["notes.md"])
            == "notes-2.md"
        )

    def test_appends_3_when_2_also_taken(self):
        assert (
            slugify_filename("notes.md", existing_keys=["notes.md", "notes-2.md"])
            == "notes-3.md"
        )

    def test_collisions_respect_slug_normalization(self):
        # User uploads "Notes.md" when "notes.md" already exists — collision
        # is detected AFTER slugification.
        assert (
            slugify_filename("Notes.md", existing_keys=["notes.md"])
            == "notes-2.md"
        )

    def test_unrelated_keys_do_not_conflict(self):
        assert (
            slugify_filename("notes.md", existing_keys=["other.md", "thoughts.md"])
            == "notes.md"
        )


class TestEdgeCases:
    @pytest.mark.parametrize(
        "raw",
        [
            "my/memo.md",   # slash in basename
            "foo\\bar.md",  # backslash
            "memo?.md",
            "<script>.md",
        ],
    )
    def test_dangerous_chars_are_stripped_to_valid_key(self, raw):
        slug = slugify_filename(raw)
        # Must be accepted by the store's own validator.
        validate_store_key(slug)
