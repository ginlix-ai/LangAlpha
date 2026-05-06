"""Tests for the charset-detecting text decode in workspace_files.py.

Agent-generated reports in non-UTF-8 locales (mainland China GBK, Taiwan/HK
Big5, Japan Shift-JIS, Korea EUC-KR) routinely land on disk in the system's
default codec, and a strict UTF-8-only read returned 415 with a misleading
"binary file" message. The fallback uses charset-normalizer with a chaos
threshold + min-bytes floor so genuinely binary content still surfaces as
None for the caller to 415 on.
"""

from __future__ import annotations

from src.server.app.workspace_files import _decode_file_text


def test_utf8_text_decodes_unchanged():
    raw = "hello world\n# 标题\n正文".encode("utf-8")
    assert _decode_file_text(raw) == "hello world\n# 标题\n正文"


def test_pure_ascii_decodes_via_utf8():
    raw = b"plain ascii content\n"
    assert _decode_file_text(raw) == "plain ascii content\n"


def test_empty_bytes_decodes_to_empty_string():
    # UTF-8 fast path handles empty without invoking the detector.
    assert _decode_file_text(b"") == ""


def _is_non_utf8(raw: bytes) -> bool:
    try:
        raw.decode("utf-8")
    except UnicodeDecodeError:
        return True
    return False


def test_gbk_simplified_chinese_decodes():
    # Mainland China — the most common non-UTF-8 encoding shape for this regression.
    original = "# 财务报告\n营业收入持续增长。利润率保持稳定。\n这是测试文本。"
    raw = original.encode("gbk")
    assert _is_non_utf8(raw), "fixture must be non-UTF-8 to exercise the fallback"
    assert _decode_file_text(raw) == original


def test_big5_traditional_chinese_decodes():
    # Taiwan / Hong Kong / Macau — what UTF-8→GB18030 alone would silently
    # corrupt because GB18030 happens to accept Big5's byte ranges.
    original = "# 財務報告\n營業收入持續增長。利潤率保持穩定。\n這是測試文本。"
    raw = original.encode("big5")
    assert _is_non_utf8(raw), "fixture must be non-UTF-8 to exercise the fallback"
    assert _decode_file_text(raw) == original


def test_shift_jis_japanese_decodes():
    # Japan — Shift-JIS / cp932 is still the default in many Japanese-locale
    # tools. charset-normalizer should detect either as compatible.
    original = "こんにちは世界。これは長めのテスト用文章です。日本語の文書をデコードできるか確認します。"
    raw = original.encode("shift-jis")
    assert _is_non_utf8(raw), "fixture must be non-UTF-8 to exercise the fallback"
    assert _decode_file_text(raw) == original


def test_png_header_returns_none():
    # Truly binary content with structured byte patterns must still surface
    # as None so the caller can 415, instead of decoding as Cyrillic garbage.
    raw = b"\x89PNG\r\n\x1a\n\xff\xfe\xfd\xfc\xfb\xfa\xf9\xf8\xf7\xf6"
    assert _decode_file_text(raw) is None


def test_random_binary_returns_none():
    raw = bytes(range(256))
    assert _decode_file_text(raw) is None


def test_short_non_utf8_returns_none():
    # Tiny non-UTF-8 inputs (< 8 bytes) bypass the detector entirely — the
    # library would otherwise match them to obscure codepages with bogus
    # confidence. Real text files clear this floor easily.
    raw = b"\xff\xfe\xfd"
    assert _decode_file_text(raw) is None


def test_utf16_bom_only_returns_none():
    # Two-byte BOM with no payload is not meaningful text content. The
    # min-bytes guardrail catches this before the detector can mis-claim it.
    raw = b"\xff\xfe"
    assert _decode_file_text(raw) is None
