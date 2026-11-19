"""The filer category a filing states for itself: dei:EntityFilerCategory in its XBRL.

Since 2019 the cover page of a 10-K, 10-Q, 20-F or 40-F is tagged in inline XBRL, so the category is in the main
document as an ix:nonNumeric element; before that it is in the XBRL instance document filed with it (from 2009 for
the largest filers, 2011 for all). It is the category the filer held when it filed, which sets that report's due
date. A 10-K states the category for the coming year's 10-Qs too; the category can change only at a fiscal year end,
measured by the public float at the end of the second quarter, so a 10-K can state a new one.
"""
from __future__ import annotations

import html
import re

from .rules import FilerCategory, normalize_category

__all__ = ["filer_category_from_document"]

_INLINE = re.compile(r"<ix:nonNumeric\b[^>]*\bname=[\"']dei:EntityFilerCategory[\"'][^>]*>(.*?)</ix:nonNumeric>", re.IGNORECASE | re.DOTALL)
_INSTANCE = re.compile(r"<dei:EntityFilerCategory\b[^>]*>(.*?)</dei:EntityFilerCategory>", re.IGNORECASE | re.DOTALL)
_TAG = re.compile(r"<[^>]+>")


def filer_category_from_document(text: str) -> FilerCategory | None:
    """The category stated by an inline XBRL document or an XBRL instance (or a full submission holding either);
    None when neither tags one."""
    for pattern in (_INLINE, _INSTANCE):
        for match in pattern.finditer(text):
            category = normalize_category(html.unescape(_TAG.sub("", match.group(1))))
            if category:
                return category
    return None
