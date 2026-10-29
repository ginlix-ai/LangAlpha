"""The scrollbar the SEC proxy splices into a filing."""

from src.server.app.sec_proxy import _with_filing_scrollbars

_INSET_TRACK = b"::-webkit-scrollbar-track{background:transparent;margin:4px}"


def test_splices_after_head_and_never_before_the_doctype():
    body = (
        b'<?xml version="1.0"?><!DOCTYPE html><html>'
        b'<head lang="en"><title>10-K</title></head><body></body></html>'
    )
    out = _with_filing_scrollbars(body)
    assert out.startswith(
        b'<?xml version="1.0"?><!DOCTYPE html><html><head lang="en"><style>'
    )
    assert _INSET_TRACK in out


def test_leaves_a_filing_without_a_head_untouched():
    body = b"<html><body>no head</body></html>"
    assert _with_filing_scrollbars(body) is body


def test_leaves_a_filing_with_its_own_webkit_scrollbar_untouched():
    body = b"<html><head><style>::-webkit-scrollbar{width:6px}</style></head><body></body></html>"
    assert _with_filing_scrollbars(body) is body
