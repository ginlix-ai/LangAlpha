"""A turn's text is written as a TEXT column takes it, so one bad turn never
fails the statement that writes its whole thread."""

from src.server.services.search.index import MAX_CHARS, _clean


def test_half_a_surrogate_pair_and_nul_are_replaced_and_pairs_kept():
    assert _clean("a\x00b\ud83dc 🚀") == "ab�c 🚀"
    assert _clean("🚀").encode("utf-8") == "🚀".encode("utf-8")


def test_capped_in_characters():
    assert len(_clean("x" * (MAX_CHARS + 5))) == MAX_CHARS
    assert _clean(None) == ""
