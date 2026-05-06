"""Tests for src/server/utils/http_headers.py."""

from __future__ import annotations

from urllib.parse import quote

from src.server.utils.http_headers import content_disposition


def _is_latin1_safe(value: str) -> bool:
    """Mirror what Starlette does to header values before sending them on the wire."""
    try:
        value.encode("latin-1")
    except UnicodeEncodeError:
        return False
    return True


def test_ascii_filename_preserved():
    header = content_disposition("report.pdf")
    assert header == 'attachment; filename="report.pdf"; filename*=UTF-8\'\'report.pdf'
    assert _is_latin1_safe(header)


def test_cjk_filename_does_not_break_latin1_encoder():
    # Regression: a CJK filename used to 500 the download endpoint because
    # Starlette can't latin-1 encode multi-byte UTF-8 in header values.
    name = "测试文件.md"
    header = content_disposition(name)
    assert _is_latin1_safe(header)
    # ASCII fallback exists and the extension survives.
    assert 'filename="' in header
    assert ".md" in header
    # Original bytes survive in the percent-encoded RFC 5987 parameter.
    assert "filename*=UTF-8''" in header
    assert quote(name, safe="") in header


def test_emoji_filename_safe():
    header = content_disposition("party-🎉.png")
    assert _is_latin1_safe(header)
    assert "filename*=UTF-8''" in header


def test_quotes_and_control_chars_stripped():
    header = content_disposition('hi"there\r\n.txt')
    assert _is_latin1_safe(header)
    # The ASCII fallback must not contain the unescaped quote that would break
    # the filename token.
    fallback_segment = header.split(";")[1]
    assert '"' in fallback_segment  # the wrapping quotes are still there
    assert fallback_segment.count('"') == 2  # only the wrapping quotes
    # Control chars must be gone from both ASCII and percent-encoded forms.
    assert "\r" not in header
    assert "\n" not in header


def test_inline_disposition():
    header = content_disposition("a.png", disposition="inline")
    assert header.startswith("inline; ")


def test_empty_filename_falls_back():
    header = content_disposition("", fallback="download")
    assert 'filename="download"' in header
    assert _is_latin1_safe(header)


def test_pure_cjk_filename_ascii_token_is_underscores():
    # Each CJK char is replaced by ``_``, giving e.g. ``"__"``. That's neither
    # empty nor whitespace, so the ``fallback`` argument is NOT used here —
    # see ``test_space_only_falls_back`` for the fallback-path case. The
    # original bytes still survive in the percent-encoded ``filename*``.
    name = "测试"
    header = content_disposition(name, fallback="memo")
    assert 'filename="__"' in header
    assert _is_latin1_safe(header)
    assert quote(name, safe="") in header


def test_space_only_falls_back():
    header = content_disposition("   ", fallback="download")
    assert 'filename="download"' in header


def test_backslash_in_filename_does_not_escape_token():
    # A path-traversal-style upload (`..\evil.exe`) must not survive into the
    # ASCII filename token where backslash escaping is allowed and could
    # confuse downstream parsers. The percent-encoded form preserves the byte
    # for clients that consult filename*.
    name = "..\\evil.exe"
    header = content_disposition(name)
    first_q = header.index('"')
    second_q = header.index('"', first_q + 1)
    ascii_token = header[first_q + 1 : second_q]
    assert "\\" not in ascii_token
    assert quote(name, safe="") in header
