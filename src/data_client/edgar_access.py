"""What every EDGAR read in data_client shares: the name SEC asks each client to send, whether edgar_engine can
run here, and whether a failure means EDGAR has no such company.
"""

from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)

DEFAULT_IDENTITY = "OpenSource user@example.com"


def edgar_identity() -> str:
    """EDGAR_IDENTITY, the name and email SEC asks every client to send. Unset or empty, it is set to the
    open-source default with a warning, so edgartools and every direct request send the same one."""
    identity = os.environ.get("EDGAR_IDENTITY")
    if not identity:
        logger.warning("EDGAR_IDENTITY is not set: SEC asks every client to name itself with a name and an email "
                       "(\"Jane Doe jane@example.com\"), and may block requests without one")
        identity = os.environ["EDGAR_IDENTITY"] = DEFAULT_IDENTITY
    return identity


def engine_available() -> bool:
    """Whether edgar_engine and the edgartools it reads EDGAR with are installed here (a sandbox image built
    before edgartools joined its packages ships the engine without it)."""
    try:
        import edgar  # noqa: F401
        import edgar_engine  # noqa: F401
    except ImportError:
        return False
    return True


def unknown_company(exc: BaseException) -> bool:
    """Whether *exc* says EDGAR has no such company: edgartools' NotFoundError for a ticker it cannot map to a
    CIK, or EDGAR's 404 for the submissions of a CIK it does not know."""
    from edgar.exceptions import NotFoundError

    if isinstance(exc, NotFoundError):
        return True
    response, request = getattr(exc, "response", None), getattr(exc, "request", None)
    return getattr(response, "status_code", None) == 404 and "/submissions/" in str(getattr(request, "url", ""))


def filing_url(cik: int | str, accession: str) -> str:
    """The sec.gov index page of a filing: the link a model cites for what the filing states."""
    return f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession.replace('-', '')}/{accession}-index.htm"
