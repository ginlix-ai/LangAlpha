"""HTML → markdown/text extraction, shared by the scrape MCP server and the
in-process crawler.

Calibrated against a live sample (see ``to_markdown``); both callers must stay
on the same thresholds or the same page yields different content depending on
which path fetched it.

Sandbox-runnable: stdlib only at import time, with trafilatura and
html_to_markdown imported inside the functions that use them — the same rule
the MCP server files follow. Logging is the caller's job; pass ``trace`` to
observe the decision.
"""

from __future__ import annotations

import re
from typing import Callable, Optional

#: Sink for one-line decision notes. The MCP servers run over stdio and have
#: no logger; the crawler passes ``logger.debug``.
Trace = Callable[[str], None]


def _silent(_message: str) -> None:
    pass


# Tuning for the trafilatura-vs-full-page decision, calibrated on a live 10-page
# sample (financial news, IR releases, explainers, government statements). Well
# extracted pages retain 88-100% of figures and stay above ~10% of the full-page
# size; pages where trafilatura silently drops the article body retain <=28% of
# figures (CNBC card/liveblog layouts) or collapse to <2% of the page (index/
# listing stubs). Thresholds sit inside those gaps, biased toward preserving
# content — for a research agent a noisier full page beats silent data loss.
_STUB_SIZE_RATIO = 0.10       # extraction below this fraction of the full page...
_STUB_MIN_FULL_LEN = 5000     # ...on a non-trivial page => listing/index stub
_FIGURE_MIN_SAMPLE = 8        # only trust the figure ratio above this many figures
_FIGURE_KEEP_RATIO = 0.65     # retaining fewer than this fraction => body dropped

# Context-safety ceiling on the full-page fallback. The fallback returns the
# entire noisy page, which on liveblog/hub layouts runs to hundreds of KB; cap
# it to ~100K tokens (~4 chars/token) so a single crawl can't swamp the agent's
# context. The clean trafilatura extraction is small and never hits this.
_MAX_FULL_PAGE_CHARS = 400_000

_PCT_RE = re.compile(r"\d+(?:\.\d+)?\s?%")
_DOLLAR_RE = re.compile(
    r"\$\s?\d[\d,]*(?:\.\d+)?\s?(?:billion|million|trillion|bn|b|m)?", re.IGNORECASE
)
_BIGNUM_RE = re.compile(r"\b\d{1,3}(?:,\d{3})+(?:\.\d+)?\b")


def _financial_figures(text: str) -> set[str]:
    """Normalized set of $/% /comma-grouped numbers — the detail an analyst needs."""
    figs: set[str] = set()
    for rx in (_PCT_RE, _DOLLAR_RE, _BIGNUM_RE):
        for match in rx.findall(text):
            figs.add(re.sub(r"\s+", "", match.lower()))
    return figs


def _try_trafilatura(html: str, trace: Trace) -> Optional[str]:
    """Extract the main article as markdown with a YAML metadata frontmatter.

    `with_metadata=True` keeps trafilatura's title/source/date/author/description
    block — without it a lone `<h1>` page extracts to body-only and loses the
    heading. The frontmatter is already clean (junk fields come back empty and are
    omitted); pass it through and let the agent decide which fields it cares about.
    """
    import trafilatura

    try:
        return trafilatura.extract(
            html,
            favor_recall=True,
            output_format="markdown",
            include_links=True,
            include_images=True,
            include_formatting=True,
            include_tables=True,
            with_metadata=True,
        )
    except Exception as e:
        trace(f"trafilatura extraction failed: {e}")
        return None


def _try_full_page(html: str, trace: Trace) -> Optional[str]:
    """Convert the entire page to markdown via html-to-markdown's Rust core.

    `extract_metadata=False` suppresses html-to-markdown's default <head> dump (a
    `meta-og:*` / gtm-dataLayer frontmatter block) that is pure noise on the hub,
    listing and error pages this full-page fallback handles.
    """
    import html_to_markdown

    try:
        return html_to_markdown.convert(
            html, html_to_markdown.ConversionOptions(extract_metadata=False)
        ).content
    except Exception as e:
        trace(f"html-to-markdown conversion failed: {e}")
        return None


def _cap_full_page(text: str, trace: Trace) -> str:
    """Truncate an oversized full-page fallback to the context-safety ceiling."""
    if len(text) <= _MAX_FULL_PAGE_CHARS:
        return text
    trace(
        f"full-page fallback {len(text)} chars exceeds cap — truncating to "
        f"{_MAX_FULL_PAGE_CHARS}"
    )
    return text[:_MAX_FULL_PAGE_CHARS] + "\n\n[... truncated: page exceeded ~100K tokens ...]"


def to_markdown(html: str, *, trace: Trace = _silent) -> str:
    """Convert fetched HTML to markdown for the LLM.

    trafilatura extracts the main article and strips nav/ads/boilerplate (cleaner,
    3-7x cheaper input), but silently under-extracts on two page shapes. We compare
    it against a faithful full-page conversion (html-to-markdown's Rust core — the
    cheaper of the two and immune to recursion limits) and prefer the full page when
    trafilatura returns an index/listing stub or drops most of the page's financial
    figures. A stdlib text extractor is the last resort.
    """
    extracted = _try_trafilatura(html, trace)
    full = _try_full_page(html, trace)

    # trafilatura found no main content (e.g. legacy table-only filings).
    if not (extracted and extracted.strip()):
        return _cap_full_page(full, trace) if (full and full.strip()) else _plain_text(html)

    # No full-page baseline to compare against — trust trafilatura.
    if not (full and full.strip()):
        return extracted

    # Index/listing stub: trafilatura kept an intro blurb and dropped the link
    # list (SEC/Fed newsrooms). The full page preserves the headlines.
    if len(extracted) < _STUB_SIZE_RATIO * len(full) and len(full) > _STUB_MIN_FULL_LEN:
        trace(
            f"trafilatura output {len(extracted)} chars vs full {len(full)} — "
            "treating as listing stub, using full page"
        )
        return _cap_full_page(full, trace)

    # Card/liveblog layout: trafilatura kept the lead card and dropped the body's
    # figures (CNBC). Compare $/% figure sets; prefer the full page on heavy loss.
    full_figs = _financial_figures(full)
    if len(full_figs) >= _FIGURE_MIN_SAMPLE:
        kept = len(_financial_figures(extracted) & full_figs) / len(full_figs)
        if kept < _FIGURE_KEEP_RATIO:
            trace(
                f"trafilatura retained {kept:.0%} of {len(full_figs)} figures — "
                "treating as dropped body, using full page"
            )
            return _cap_full_page(full, trace)

    return extracted


def to_text(html: str) -> str:
    """Plain-text extraction of the main article (no markdown formatting)."""
    import trafilatura

    return trafilatura.extract(html, output_format="txt") or ""


def _plain_text(html: str) -> str:
    """Last-resort plain-text extraction using the stdlib parser (never recurses)."""
    from html.parser import HTMLParser

    class _Extractor(HTMLParser):
        def __init__(self) -> None:
            super().__init__()
            self.parts: list[str] = []
            self._skip = 0

        def handle_starttag(self, tag, attrs):
            if tag in ("script", "style"):
                self._skip += 1

        def handle_endtag(self, tag):
            if tag in ("script", "style") and self._skip:
                self._skip -= 1

        def handle_data(self, data):
            if not self._skip and data.strip():
                self.parts.append(data.strip())

    try:
        p = _Extractor()
        p.feed(html)
        return " ".join(p.parts)
    except Exception:
        return html
