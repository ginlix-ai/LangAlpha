"""What search's SQL is handed: terms as escaped LIKE patterns or word-start
regexes, and a page's key checked before it reaches a cast."""

from datetime import datetime, timezone
from uuid import UUID

import pytest

from src.server.database.search.sql import BadKey, Keyset, Terms, like_pattern


class TestLikePattern:
    def test_wraps_the_term_for_a_substring_match(self):
        assert like_pattern("margin") == "%margin%"

    @pytest.mark.parametrize(
        ("term", "pattern"),
        [
            ("100%", "%100\\%%"),
            ("a_b", "%a\\_b%"),
            ("c:\\dir", "%c:\\\\dir%"),
            ("\\%_", "%\\\\\\%\\_%"),
        ],
    )
    def test_escapes_like_metacharacters(self, term, pattern):
        assert like_pattern(term) == pattern


class TestTerms:
    def test_a_short_latin_term_matches_at_a_word_start(self):
        terms = Terms(["nv", "nvd", "毛利"])
        assert terms.params == {"term0": r"\mnv", "term1": "%nvd%", "term2": "%毛利%"}
        assert terms.on("c") == (
            "c ~* %(term0)s AND c ILIKE %(term1)s ESCAPE '\\' "
            "AND c ILIKE %(term2)s ESCAPE '\\'"
        )

    def test_the_phrase_is_the_words_as_typed_when_there_are_several(self):
        assert Terms(["gross", "100%"]).phrase == "%gross 100\\%%"
        assert Terms(["gross"]).phrase is None

    def test_no_terms_match_everything(self):
        assert Terms([]).on("c") == "TRUE"


class TestKeyset:
    PAGE = Keyset((("a", bool), ("b", datetime), ("c", UUID), ("d", int)))
    AT = datetime(2026, 11, 9, 12, 0, tzinfo=timezone.utc)
    ID = "6f1c2a3e-0000-4000-8000-000000000001"

    def test_the_first_page_has_no_condition(self):
        assert self.PAGE.after(None, {}) == ""

    def test_a_key_starts_the_page_after_it(self):
        params: dict = {}
        sql = self.PAGE.after([True, self.AT.isoformat(), self.ID.upper(), 3], params)
        assert params == {"after0": True, "after1": self.AT, "after2": self.ID, "after3": 3}
        assert sql == (
            "AND a <= %(after0)s::boolean AND (a, b, c, d) < "
            "(%(after0)s::boolean, %(after1)s::timestamptz, %(after2)s::uuid, %(after3)s::int)"
        )

    def test_orders_every_column_descending(self):
        assert self.PAGE.order_by == "a DESC, b DESC, c DESC, d DESC"

    @pytest.mark.parametrize(
        "key",
        [
            [True, AT.isoformat(), ID],  # short
            [1, AT.isoformat(), ID, 3],  # not a bool
            [True, "2026-11-09T12:00:00", ID, 3],  # no zone
            [True, "yesterday", ID, 3],
            [True, AT.isoformat(), "not-a-uuid", 3],
            [True, AT.isoformat(), ID, True],  # a bool is not an int
            [True, AT.isoformat(), ID, "3"],
            [True, AT.isoformat(), ID, 2**31],  # past what the int cast takes
            [True, AT.isoformat(), ID, -(2**31) - 1],
        ],
    )
    def test_anything_else_is_refused(self, key):
        with pytest.raises(BadKey):
            self.PAGE.after(key, {})
