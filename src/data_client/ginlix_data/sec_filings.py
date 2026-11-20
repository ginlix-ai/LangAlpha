"""ginlix-data's SEC filings routes, read by get_sec_filing.

Blocking on purpose: get_sec_filing reads EDGAR on a thread pool of its own, and edgar_engine reads its caches through
a synchronous Store, so these calls run on those threads. Every answer that is not a usable 200 (a miss, a list cut
short, a 503) comes back as None, so the caller reads EDGAR instead. A 200 is parsed into the types below here, so an
answer of another shape raises rather than reaching the caller half read.
"""

from __future__ import annotations

import functools
import gzip
import json
import logging
import os
import threading
from dataclasses import dataclass
from datetime import date
from typing import Any, Mapping, Sequence

import httpx
from edgar_engine.cache import kind_of

from .client import SERVICE_USER_ID
from .v2_routes import path_segment

logger = logging.getLogger(__name__)

#: The most rows one /filings answer holds; a filer with more is read from EDGAR.
MAX_FILINGS = 5000


@dataclass(frozen=True)
class IndexedCompany:
    cik: int
    name: str | None
    tickers: tuple[str, ...]
    fiscal_year_end: str | None


@dataclass(frozen=True)
class IndexedFilings:
    """A filer's filings, newest first, as rows in edgartools' filings-table shape (form, accession_number,
    filing_date, reportDate, acceptanceDateTime, items), the shape edgar_engine.filings.refs_for_form selects from."""
    cik: int
    rows: list[dict[str, Any]]


@dataclass(frozen=True)
class FiscalLabels:
    """(accession, form) -> (fiscal year, fiscal period), as edgar_engine's labels_from_companyfacts computes them,
    and the labels engine ginlix-data computed them with."""
    cik: int
    engine: str | None
    labels: Mapping[tuple[str, str], tuple[int, str]]


@dataclass(frozen=True)
class OwedReport:
    """The periodic report a filer owes next. notice: the accession of its notice of late filing, if it filed one."""
    form: str
    fiscal_period: str
    period_end: str
    window: tuple[str, str]
    deadline: str
    latest_deadline: str
    notice: str | None


@dataclass(frozen=True)
class FilingCalendar:
    """Whether a filer's latest periodic report (latest, an accession) is current, and the report it owes next.
    termination: the accession of the Form 15 it stopped reporting with."""
    cik: int
    verdict: str
    latest: str | None
    termination: str | None
    next: OwedReport | None


def _text(value: Any) -> str:
    if not isinstance(value, str):
        raise TypeError(f"expected a string, not {type(value).__name__}")
    return value


def _maybe_text(value: Any) -> str | None:
    return None if value is None else _text(value)


def _list(value: Any) -> list[Any]:
    if not isinstance(value, list):
        raise TypeError(f"expected a list, not {type(value).__name__}")
    return value


def _texts(value: Any) -> tuple[str, ...]:
    return () if value is None else tuple(_text(v) for v in _list(value))


def _year(value: Any) -> int:
    if type(value) is not int:
        raise TypeError(f"expected an integer year, not {value!r}")
    return value


def _date(value: Any) -> str:
    return date.fromisoformat(_text(value)).isoformat()


def _fiscal_year_end(value: Any) -> str | None:
    """None, or a month and day as MMDD (raises for anything else, 0229 included)."""
    if value is not None:
        if not (isinstance(value, str) and len(value) == 4 and value.isdigit()):
            raise ValueError(f"not a fiscal year end: {value!r}")
        date(2000, int(value[:2]), int(value[2:]))
    return value


def _calendar(body: dict[str, Any]) -> FilingCalendar:
    nxt = body["next"]
    owed = None if nxt is None else OwedReport(
        _text(nxt["form"]), _text(nxt["fiscal_period"]), _date(nxt["period_end"]),
        (_date(nxt["window"][0]), _date(nxt["window"][1])), _date(nxt["deadline"]), _date(nxt["latest_deadline"]),
        _maybe_text(nxt.get("notice")))
    return FilingCalendar(int(body["cik"]), _text(body["verdict"]),
                          _maybe_text((body.get("latest") or {}).get("accession")), _maybe_text(body.get("termination")),
                          owed)


class SecFilingsClient:
    def __init__(self, base_url: str, service_token: str = ""):
        headers = {"X-User-Id": SERVICE_USER_ID}
        if service_token:
            headers["X-Service-Token"] = service_token
        self.http = httpx.Client(base_url=f"{base_url.rstrip('/')}/api/v2/data/sec", headers=headers,
                                 timeout=httpx.Timeout(5.0, connect=2.0))

    def _json(self, path: str, params: dict[str, Any] | None = None) -> Any:
        r = self.http.get(path, params=params)
        if r.status_code in (404, 503):
            return None
        r.raise_for_status()
        return r.json()

    def company(self, ticker_or_cik: str) -> IndexedCompany | None:
        body = self._json(f"/companies/{path_segment(ticker_or_cik)}")
        if body is None:
            return None
        return IndexedCompany(int(body["cik"]), _maybe_text(body.get("name")),
                              _texts(body.get("tickers")),
                              _fiscal_year_end(body.get("fiscal_year_end")))

    def filings(self, ticker_or_cik: str, forms: Sequence[str]) -> IndexedFilings | None:
        """The filer's filings of these forms, or None when there are more than one answer holds."""
        body = self._json(f"/filings/{path_segment(ticker_or_cik)}", {"forms": ",".join(forms), "limit": MAX_FILINGS})
        if body is None or body.get("truncated"):
            return None
        return IndexedFilings(int(body["cik"]), [
            {"accession_number": _text(r["accession_number"]), "form": _text(r["form"]),
             "filing_date": _date(r["filing_date"]), "reportDate": _date(r["report_date"]) if r.get("report_date") else "",
             "acceptanceDateTime": _maybe_text(r.get("accepted")), "items": _maybe_text(r.get("items"))}
            for r in _list(body["filings"])])

    def fiscal_labels(self, ticker_or_cik: str) -> FiscalLabels | None:
        body = self._json(f"/fiscal-labels/{path_segment(ticker_or_cik)}")
        if body is None:
            return None
        return FiscalLabels(int(body["cik"]), _maybe_text(body.get("engine")),
                            {(_text(a), _text(f)): (_year(y), _text(p)) for a, f, y, p in _list(body["labels"])})

    def calendar(self, ticker_or_cik: str) -> FilingCalendar | None:
        body = self._json(f"/calendar/{path_segment(ticker_or_cik)}")
        return None if body is None else _calendar(body)

    def companyfacts(self, cik: int) -> dict[str, Any] | None:
        body = self._json(f"/companyfacts/{cik}")
        if body is not None and not isinstance(body, dict):
            raise TypeError(f"expected an object, not {type(body).__name__}")
        return body

    def parsed(self, key: str, cik: int) -> bytes | None:
        """The stored blob under an edgar_engine cache key, gzipped as the engine's caches write it. The filer's cik
        goes with the key so ginlix-data can take up a filing it does not hold yet."""
        with self.http.stream("GET", f"/parsed/{key}", params={"cik": cik}, headers={"Accept-Encoding": "gzip"}) as r:
            if r.status_code == 404:
                _note_parser_skew(key, r)
            if r.status_code in (404, 503):
                return None
            r.raise_for_status()
            data = b"".join(r.iter_raw())
            return data if r.headers.get("content-encoding") == "gzip" else gzip.compress(data, mtime=0)


def _note_parser_skew(key: str, miss: httpx.Response) -> None:
    try:
        body = json.loads(miss.read())
    except ValueError:
        return
    if body.get("reason") == "other_version":
        note_skew(kind_of(key), str(body.get("versions")), key.rsplit("/", 1)[0])


@functools.cache
def note_skew(what: str, theirs: str | None, ours: str) -> None:
    """Warns once per skew when ginlix-data holds what under another engine or edgartools version than this process,
    which turns every read of it into a miss."""
    logger.warning("ginlix-data holds %s under %s, not %s: its engine differs from this process's, so these reads "
                   "miss and are computed here", what, theirs, ours)


_client: SecFilingsClient | None = None
_lock = threading.Lock()


def sec_filings_client() -> SecFilingsClient | None:
    """The process's client, or None when no ginlix-data is configured or its SEC reads are switched off."""
    global _client
    from src.config.settings import GINLIX_DATA_SEC_ENABLED, GINLIX_DATA_URL

    if not (GINLIX_DATA_URL and GINLIX_DATA_SEC_ENABLED):
        return None
    with _lock:
        if _client is None:
            _client = SecFilingsClient(GINLIX_DATA_URL, os.getenv("INTERNAL_SERVICE_TOKEN", ""))
        return _client
