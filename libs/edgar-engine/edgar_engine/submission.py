"""Reading a filing's full-text submission, once more when the first download does not parse."""
from __future__ import annotations

import logging
from collections.abc import Callable
from typing import TypeVar

__all__ = ["TruncatedSubmission", "parse_submission"]

logger = logging.getLogger(__name__)
T = TypeVar("T")


class TruncatedSubmission(ValueError):
    """A filing's full-text submission did not parse twice, most often because EDGAR served it cut short."""

    def __init__(self, accession: str):
        super().__init__(f"the submission of {accession} is truncated")
        self.accession = accession


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
