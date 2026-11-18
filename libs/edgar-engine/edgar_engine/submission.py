"""A filing's full-text submission (the accession's .txt): reading its SEC header, fetching it with one request so
a truncated download is never kept, and parsing it once more when the first download does not parse."""
from __future__ import annotations

import logging
import re
from collections.abc import Callable
from typing import TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    from .cache import SubmissionCache

__all__ = ["TruncatedSubmission", "decode", "fetch_submission", "header", "is_complete", "parse_submission",
           "submission_url"]

logger = logging.getLogger(__name__)
T = TypeVar("T")

_HEADER_FIELD = {
    "form": re.compile(r"^\s*CONFORMED SUBMISSION TYPE:\s*(\S+)", re.M),
    "filing_date": re.compile(r"^\s*FILED AS OF DATE:\s*(\d{8})", re.M),
    "accession": re.compile(r"^\s*ACCESSION NUMBER:\s*([\d-]+)", re.M),
    "period": re.compile(r"^\s*CONFORMED PERIOD OF REPORT:\s*(\d{8})", re.M),
}
_TRAILER = re.compile(rb"</SEC-DOCUMENT>\s*$")


class TruncatedSubmission(ValueError):
    """A filing's full-text submission is cut short: it ends before its </SEC-DOCUMENT> trailer, or it did not parse
    twice, most often because EDGAR served it cut short."""

    def __init__(self, accession: str):
        super().__init__(f"the submission of {accession} is truncated")
        self.accession = accession


def decode(raw: bytes | str) -> str:
    if isinstance(raw, str):
        return raw
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("cp1252", errors="replace")


def header(submission: str) -> dict:
    """Form, filing date, accession and period from the SEC header (dates as YYYY-MM-DD)."""
    head = submission[:20000]
    out = {}
    for key, rx in _HEADER_FIELD.items():
        m = rx.search(head)
        if not m:
            continue
        v = m.group(1)
        out[key] = f"{v[:4]}-{v[4:6]}-{v[6:]}" if key in ("filing_date", "period") else v
    return out


def is_complete(raw: bytes) -> bool:
    """True when the submission ends with its </SEC-DOCUMENT> trailer."""
    return bool(_TRAILER.search(raw[-200:]))


def submission_url(cik: int | str, accession: str) -> str:
    return f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession}.txt"


def fetch_submission(cik: int | str, accession: str, cache: SubmissionCache | None = None) -> bytes:
    """The .txt submission of one filing (any CIK it is listed under serves it), through edgartools' identity, rate
    limit and retries, and cached by accession when cache is given.

    A body without the </SEC-DOCUMENT> trailer is a truncated download: it is fetched once more past edgartools'
    HTTP cache (which keeps Archives responses forever), raises TruncatedSubmission if still short and is never
    cached, and a truncated entry already in the cache is fetched again.
    """
    from edgar.httprequests import get_with_retry

    if cache is not None and (cached := cache.get(accession)) is not None and is_complete(cached):
        return cached
    url = submission_url(cik, accession)
    r = get_with_retry(url)
    r.raise_for_status()
    body = r.content
    if not is_complete(body):
        body = _fetch_uncached(url)
    if body is None or not is_complete(body):
        raise TruncatedSubmission(accession)
    if cache is not None:
        cache.put(accession, body)
    return body


def _fetch_uncached(url: str) -> bytes | None:
    try:
        from edgar.httpclient import uncached_http_client
    except ImportError:
        return _fetch_uncached_5x(url)
    with uncached_http_client() as client:
        r = client.get(url)
    r.raise_for_status()
    return r.content


def _fetch_uncached_5x(url: str) -> bytes:
    """edgartools 5.x has no uncached client: a copy of its HTTP manager with the cache off keeps the identity, proxy,
    SSL, timeout and the shared rate limiter."""
    from dataclasses import replace

    from edgar.httpclient import HTTP_MGR

    mgr = replace(HTTP_MGR, cache_mode="Disabled", cache_dir=None, _client=None, httpx_params=dict(HTTP_MGR.httpx_params))
    try:
        with mgr.http_client() as client:
            r = client.get(url)
        r.raise_for_status()
        return r.content
    finally:
        mgr.close()


def parse_submission(filing, parse: Callable[[str], T]) -> T:
    """parse over an edgartools Filing's full-text submission. A submission that does not parse (ValueError) is most
    often a truncated download, which edgartools' HTTP cache keeps and serves again, so its copy is dropped and the
    submission fetched and parsed once more before TruncatedSubmission is raised."""
    for attempt in (1, 2):
        try:
            return parse(filing.full_text_submission())
        except ValueError as e:
            if attempt == 2:
                raise TruncatedSubmission(filing.accession_no) from e
            logger.warning("edgar_engine: %s did not read to its end; fetching it again", filing.accession_no)
            _drop_cached(filing)
    raise AssertionError("unreachable")


def _drop_cached(filing) -> None:
    """Remove edgartools' cached copy of the filing's submission. The cache layout is httpxthrottlecache's, so a
    layout this cannot read leaves the copy in place and the second read fails the same way."""
    try:
        from edgar.httpclient import get_cache_directory
        from httpx import URL
        from httpxthrottlecache.filecache.transport import FileCache

        url = filing.text_url
        u = URL(url)
        path = FileCache(get_cache_directory(), locking=False).to_path(host=u.host, path=u.path, query=u.query.decode())
    except Exception:
        logger.warning("edgar_engine: could not locate the cached copy of %s", filing.accession_no, exc_info=True)
        return
    for f in (path, path.with_suffix(path.suffix + ".meta")):
        f.unlink(missing_ok=True)
