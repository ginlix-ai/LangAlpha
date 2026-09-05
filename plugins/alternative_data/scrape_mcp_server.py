#!/usr/bin/env python3
"""Web scraping MCP server — scrapling as a library, era-proof.

Replaces the third-party ``scrapling mcp`` entrypoint, which imports the
mcp 1.x-only ``mcp.server.fastmcp`` and dies in any 2.x environment. Importing
scrapling's fetchers directly decouples the server from the SDK era the
scrapling CLI was built against; ``_bootstrap`` handles our own era split.

Fetching and extraction live in ``_browser``/``_extract``, shared with the
in-process crawler; this file owns the tool contract — argument validation,
per-URL error envelopes, the concurrency gates and the call budget every
argument combination has to fit.

Tools: scrape_page, scrape_pages.
"""

from __future__ import annotations

import asyncio
import math
import re
import sys
import time
from typing import Any, List, Optional

try:
    from _bootstrap import MCPServer  # script launch: mcp_servers/ is sys.path[0]
except ModuleNotFoundError:  # imported as a package module (tests)
    from mcp_servers._bootstrap import MCPServer

from mcp_servers._browser import (
    fetch_fast,
    fetch_with_session,
    make_session,
    url_block_reason,
)
from mcp_servers._extract import to_markdown, to_text
from mcp_servers._schemas import (
    ERROR_PROPS,
    INT,
    STR,
    described,
    output_model,
    union_schema,
)

mcp = MCPServer("ScrapeMCP")

_MODES = ("fast", "browser", "stealth")
_EXTRACTIONS = ("markdown", "html", "text")
_MAX_TIMEOUT_S = 60.0
_DEFAULT_TIMEOUT_S = 30.0
_MAX_BULK_URLS = 10
# Browser sessions are ~400MB each; the third-party server serialized too.
_BROWSER_CONCURRENCY = 2
_FAST_CONCURRENCY = 8
# The client kills the whole server process once a call passes its own timeout
# (mcp_client_runtime._CALL_TIMEOUT, 120s), losing every result in the batch
# and orphaning any live browser, so no argument combination may reach it.
_CALL_BUDGET_S = 110.0
# Session start and teardown sit outside the page-load timeout scrapling
# honors; fast mode only has to absorb redirect and retry overshoot.
_BROWSER_GRACE_S = 15.0
_FAST_GRACE_S = 5.0
_MAX_CONTENT_CHARS = 400_000
_MAX_DETAIL_CHARS = 300

# Per interpreter, not per call — and not process-wide across executions. A
# semaphore built inside the handler would bound only its own batch, but each
# execute_code spawns a fresh interpreter with its own server process, so K
# parallel executions on one sandbox still reach K x _BROWSER_CONCURRENCY
# browsers; the memory ceiling that needs is not enforceable from here. Safe to
# build at import: asyncio binds the loop on first contended acquire, and the
# stdio server runs a single loop for the life of the process.
_BROWSER_SEM = asyncio.Semaphore(_BROWSER_CONCURRENCY)
_FAST_SEM = asyncio.Semaphore(_FAST_CONCURRENCY)

_TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)

# Wording drift from the shared error contract, pinned so the published bytes
# stay stable; unify with ERROR_PROPS the next time schemas may move.
_ERROR_PROPS = {
    **ERROR_PROPS,
    "detail": described(STR, "Human-readable cause (error responses only)."),
}

_PAGE_FIELDS = {
    "url": STR,
    "status": described(INT, "HTTP status of the final response."),
    "title": STR,
    "content": described(STR, "Extracted content, truncated to 400k chars."),
    "extraction": {**STR, "enum": list(_EXTRACTIONS)},
    "mode": {**STR, "enum": list(_MODES)},
}
# Bulk rows carry a per-URL error in place of the page fields.
_PAGE_ROW = {**_PAGE_FIELDS, **_ERROR_PROPS}

_OUT_PAGE = output_model(
    "ScrapePageOut",
    union_schema(_PAGE_FIELDS, ("url", "status", "content"), error_props=_ERROR_PROPS),
)

_OUT_PAGES = output_model(
    "ScrapePagesOut",
    union_schema(
        {
            "results": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": True,
                    "properties": _PAGE_ROW,
                },
                "description": "One entry per input URL, in input order.",
            },
            "count": INT,
        },
        ("results", "count"),
        error_props=_ERROR_PROPS,
    ),
)


def _clean_detail(detail: str) -> str:
    """Collapse to one capped line — upstream exception text can carry a whole
    response body or driver dump, and the agent only needs the cause."""
    collapsed = re.sub(r"\s+", " ", detail).strip()
    if len(collapsed) <= _MAX_DETAIL_CHARS:
        return collapsed
    return collapsed[:_MAX_DETAIL_CHARS] + "..."


def _error(code: str, detail: str, **echo: Any) -> dict:
    return {"error": code, "detail": _clean_detail(detail), **echo}


def _as_entry(url: str, result: Any) -> dict:
    """Per-URL row for one gather slot.

    ``return_exceptions=True`` hands back the exception object rather than
    failing the batch, so anything that still escaped _scrape_one owes the
    caller a row — not an MCP isError over the other nine URLs. Cancellation
    is not ours to convert into a result.
    """
    if isinstance(result, BaseException):
        if not isinstance(result, Exception):
            raise result
        return _error("scrape_failed", f"{type(result).__name__}: {result}", url=url)
    return result


def _extract_title(html: str) -> str:
    match = _TITLE_RE.search(html[:50_000])
    return re.sub(r"\s+", " ", match.group(1)).strip() if match else ""


def _slots(mode: str) -> int:
    return _FAST_CONCURRENCY if mode == "fast" else _BROWSER_CONCURRENCY


def _grace_s(mode: str) -> float:
    return _FAST_GRACE_S if mode == "fast" else _BROWSER_GRACE_S


def _waves(mode: str, url_count: int) -> int:
    """Sequential rounds a batch needs — the gate admits _slots(mode) at a time."""
    return math.ceil(url_count / _slots(mode))


def _fetch_bound_s(mode: str, timeout_s: float) -> float:
    """Wall time one fetch may take: the page-load timeout scrapling honors,
    plus the session start and teardown that sit outside it. The admission
    check and the runtime bound both read it here so they cannot disagree."""
    return timeout_s + _grace_s(mode)


def _report_close_error(exc: BaseException, post_cancel: bool) -> None:
    """Browser teardown failed — a leaked Chromium survives it, and the sandbox
    image has no reaper. stderr is the only sink here: the client tails server
    stderr when a call fails, and this server configures no logging."""
    when = "after cancellation" if post_cancel else "during teardown"
    print(
        f"ERROR: scrape browser close failed {when}: {type(exc).__name__}: {exc}",
        file=sys.stderr,
        flush=True,
    )


async def _fetch_html(
    url: str,
    mode: str,
    timeout_s: float,
    solve_cloudflare: bool,
    close_deadline: float | None = None,
) -> tuple[str, int]:
    if mode == "fast":
        _, html, status = await fetch_fast(url, timeout_s=timeout_s)
        return html, status

    session = make_session(mode, timeout_ms=timeout_s * 1000)
    fetch_kwargs = {"solve_cloudflare": solve_cloudflare} if mode == "stealth" else {}
    _, html, status = await fetch_with_session(
        session,
        url,
        on_close_error=_report_close_error,
        close_deadline=close_deadline,
        **fetch_kwargs,
    )
    return html, status


async def _scrape_one(
    url: str,
    mode: str,
    extraction: str,
    timeout_s: float,
    solve_cloudflare: bool,
    close_deadline: float | None = None,
) -> dict:
    blocked = url_block_reason(url)
    if blocked:
        return _error("invalid_url", f"{blocked}: {url[:200]}", url=url)
    fetch_bound_s = _fetch_bound_s(mode, timeout_s)
    # Gate held across the fetch only: the extraction below is thread work
    # holding no browser, so releasing early lets the next URL start sooner.
    async with (_FAST_SEM if mode == "fast" else _BROWSER_SEM):
        try:
            html, status = await asyncio.wait_for(
                _fetch_html(url, mode, timeout_s, solve_cloudflare, close_deadline),
                timeout=fetch_bound_s,
            )
        # Nothing under us bounds a browser fetch: start() takes no timeout and
        # the Cloudflare solver loops without a cap. _browser shields close()
        # precisely so this cancellation tears the session down cleanly.
        except asyncio.TimeoutError:
            return _error(
                "fetch_failed", f"timed out after {fetch_bound_s:.0f}s", url=url
            )
        except Exception as e:  # noqa: BLE001 - per-URL failures become error dicts
            return _error("fetch_failed", f"{type(e).__name__}: {e}", url=url)

    # Extraction is as failure-prone as the fetch — trafilatura and
    # html_to_markdown exhaust the recursion limit on deeply nested or
    # malformed markup — and an unguarded raise here sinks the whole batch.
    try:
        if extraction == "html":
            content = html
        elif extraction == "text":
            content = await asyncio.to_thread(to_text, html)
        else:
            content = await asyncio.to_thread(to_markdown, html)
    except Exception as e:  # noqa: BLE001 - same per-URL envelope as a fetch failure
        return _error(
            "extract_failed", f"{type(e).__name__}: {e}", url=url, status=status
        )

    return {
        "url": url,
        "status": status,
        "title": _extract_title(html),
        "content": content[:_MAX_CONTENT_CHARS],
        "extraction": extraction,
        "mode": mode,
    }


def _validate_args(mode: str, extraction: str, timeout: float) -> Optional[dict]:
    if mode not in _MODES:
        return _error("invalid_mode", f"mode must be one of {_MODES}, got: {mode!r}")
    if extraction not in _EXTRACTIONS:
        return _error(
            "invalid_extraction",
            f"extraction must be one of {_EXTRACTIONS}, got: {extraction!r}",
        )
    if not 1.0 <= timeout <= _MAX_TIMEOUT_S:
        return _error(
            "invalid_timeout", f"timeout must be 1-{_MAX_TIMEOUT_S:.0f}s, got: {timeout}"
        )
    return None


def _check_budget(mode: str, timeout: float, url_count: int) -> Optional[dict]:
    """Refuse argument combinations whose worst case outlives _CALL_BUDGET_S.

    URLs run in waves of _slots(mode), each wave bounded by timeout + grace, so
    it is the batch — not the per-URL cap — that can push a call into the
    client's process kill. Rejecting is the honest answer: quietly shrinking
    the timeout would return a batch of fetch_failed rows with no cause.
    """
    waves = _waves(mode, url_count)
    if waves * _fetch_bound_s(mode, timeout) <= _CALL_BUDGET_S:
        return None
    allowed = _CALL_BUDGET_S / waves - _grace_s(mode)
    fits = int(_CALL_BUDGET_S // _fetch_bound_s(mode, timeout)) * _slots(mode)
    return _error(
        "invalid_timeout",
        f"{url_count} URLs in {mode} mode run {waves} rounds of {_slots(mode)}, "
        f"which fits timeout<={allowed:.0f}s, got: {timeout}. Send at most "
        f"{min(fits, _MAX_BULK_URLS)} URLs at this timeout, or lower the timeout.",
    )


@mcp.tool()
async def scrape_page(
    url: str,
    mode: str = "fast",
    extraction: str = "markdown",
    timeout: float = _DEFAULT_TIMEOUT_S,
    solve_cloudflare: bool = False,
) -> _OUT_PAGE:
    """Scrape one web page and extract its content.

    Start with mode='fast' (plain HTTP). Use 'browser' when the page needs
    JavaScript rendering, 'stealth' for bot-protected sites; add
    solve_cloudflare=true only when 'stealth' still returns a challenge page.

    Args:
        url: Full http(s) URL. Localhost and private addresses are rejected.
        mode: fast|browser|stealth.
        extraction: markdown (default) | html (raw) | text (plain).
        timeout: Per-fetch seconds, 1-60.
        solve_cloudflare: Solve Cloudflare challenges (stealth mode only).

    Returns:
        dict: {url, status, title, content, extraction, mode}. content is
        truncated to 400k chars.
        On error: {error, detail} — invalid_url|invalid_mode|
        invalid_extraction|invalid_timeout|fetch_failed|extract_failed.
    """
    bad = _validate_args(mode, extraction, timeout) or _check_budget(mode, timeout, 1)
    if bad:
        return bad
    # Teardown is charged to the same clock admission budgeted: a cancelled
    # fetch's close wait may only spend what is left of the call budget.
    close_deadline = time.monotonic() + _CALL_BUDGET_S
    return await _scrape_one(
        url, mode, extraction, timeout, solve_cloudflare, close_deadline
    )


@mcp.tool()
async def scrape_pages(
    urls: List[str],
    mode: str = "fast",
    extraction: str = "markdown",
    timeout: float = _DEFAULT_TIMEOUT_S,
    solve_cloudflare: bool = False,
) -> _OUT_PAGES:
    """Scrape up to 10 web pages concurrently.

    Same modes as scrape_page; per-URL failures come back as {error, detail}
    entries in results instead of failing the batch. browser and stealth fetch
    2 URLs at a time, so a batch that could not finish in time is refused with
    invalid_timeout — at the default timeout they take 4 URLs per call.

    Args:
        urls: Full http(s) URLs, max 10 per call (4 in browser/stealth).
        mode: fast|browser|stealth.
        extraction: markdown (default) | html (raw) | text (plain).
        timeout: Per-fetch seconds, 1-60.
        solve_cloudflare: Solve Cloudflare challenges (stealth mode only).

    Returns:
        dict: {results, count}. results holds one entry per input URL in
        input order — {url, status, title, content, extraction, mode} or a
        per-URL {error, detail, url}.
        On error: {error, detail} — invalid_urls|invalid_mode|
        invalid_extraction|invalid_timeout.
    """
    bad = _validate_args(mode, extraction, timeout)
    if bad:
        return bad
    if not urls:
        return _error("invalid_urls", "urls must be a non-empty list")
    if len(urls) > _MAX_BULK_URLS:
        return _error(
            "invalid_urls", f"max {_MAX_BULK_URLS} URLs per call, got {len(urls)}"
        )
    over = _check_budget(mode, timeout, len(urls))
    if over:
        return over

    # _scrape_one holds the concurrency gate itself, so the batch just fans out.
    # One shared deadline: teardown in any wave draws down the same budget the
    # admission check sized, so a wedged close cannot push the batch past the
    # client's process kill.
    close_deadline = time.monotonic() + _CALL_BUDGET_S
    results = await asyncio.gather(
        *(
            _scrape_one(u, mode, extraction, timeout, solve_cloudflare, close_deadline)
            for u in urls
        ),
        return_exceptions=True,
    )
    entries = [_as_entry(u, r) for u, r in zip(urls, results)]
    return {"results": entries, "count": len(entries)}


if __name__ == "__main__":
    mcp.run(transport="stdio")
