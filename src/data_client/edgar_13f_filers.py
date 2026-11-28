"""13F managers by name, matched against EDGAR's full index (the build without LangAlpha Data).

A manager's name is matched against every name a 13F-HR filer used in EDGAR's full index for this quarter and the three
before, and, for an older period, the quarter its reports were due in. Each quarter's list is kept on disk and read
again daily until a day after the quarter ends; older quarters' lists are kept by last use.
"""

from __future__ import annotations

import asyncio
import contextlib
import gzip
import json
import os
import re
import tempfile
import threading
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import httpx

from data_client.edgar_access import edgar_identity

_FULL_INDEX = "https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/form.gz"
# A form.idx line: form type, company name (cut at 60 characters), CIK, date filed, file name.
_FILER = re.compile(r"^13F-HR(?:/A)?\s+(?P<name>.*?)\s+(?P<cik>\d+)\s+(?P<filed>\d{4}-\d{2}-\d{2})\s+\S+\s*$")
QUARTERS = 4
_NAME_CUT = 60  # EDGAR's full index cuts a company name at 60 characters
HISTORY = 4  # older quarters' lists kept, the most recently used
_QUARTER_FILE = re.compile(r"13f-hr-(\d{4})q([1-4])\.json")
_CURRENT_TTL_S = 86400.0
_REQUEST_GAP_S = 0.5  # 2 requests a second at most
_indexing = threading.Lock()
_NEW_YORK = ZoneInfo("America/New_York")


def _index_dir() -> Path:
    return Path.home() / ".cache" / "langalpha" / "edgar-13f-filers"


def _quarters(today: date) -> list[tuple[int, int]]:
    """This quarter and the three before it, newest first: a year of 13F-HR filings, so a manager that filed for
    any of the latest four periods is named."""
    year, quarter = today.year, (today.month - 1) // 3 + 1
    out = []
    for _ in range(QUARTERS):
        out.append((year, quarter))
        year, quarter = (year - 1, 4) if quarter == 1 else (year, quarter - 1)
    return out


def _fetch_filers(year: int, quarter: int, *, current: bool) -> list[list[str]]:
    """Every name a 13F-HR filer used in EDGAR's full index for the quarter, as [CIK (as digits), name] pairs, the
    latest filed first; [] for the current quarter before EDGAR lists its index."""
    url = _FULL_INDEX.format(year=year, quarter=quarter)
    with httpx.Client(timeout=120, headers={"User-Agent": edgar_identity()}, follow_redirects=True) as http:
        resp = http.get(url)
    if current and resp.status_code == 404:
        return []
    resp.raise_for_status()
    filed: dict[tuple[str, str], str] = {}
    for line in gzip.decompress(resp.content).decode("latin-1").splitlines():
        if m := _FILER.match(line):
            filed[(m["cik"], m["name"])] = max(filed.get((m["cik"], m["name"]), ""), m["filed"])
    return [list(pair) for pair in sorted(filed, key=filed.__getitem__, reverse=True)]


def _filing_quarter(period: str) -> tuple[int, int]:
    """The quarter a period's 13F-HR is due in: the one after the quarter the period ends."""
    day = date.fromisoformat(period)
    quarter = (day.month - 1) // 3 + 1
    return (day.year + 1, 1) if quarter == 4 else (day.year, quarter + 1)


def _closed(year: int, quarter: int) -> float:
    """When EDGAR's full index for the quarter is complete: a day after the quarter's last day ends, in New York."""
    after = date(year + 1, 1, 1) if quarter == 4 else date(year, 3 * quarter + 1, 1)
    return datetime.combine(after + timedelta(days=1), datetime.min.time(), _NEW_YORK).timestamp()


def _cached(path: Path, year: int, quarter: int) -> list[list[str]] | None:
    """The quarter's filers kept at *path*, or None when there are none, another process just deleted them, the file
    is not a list of [CIK, name] pairs, or they were read before the quarter's index was complete and more than a day
    ago. A read marks the file used (its access time), which decides the older quarters kept."""
    try:
        mtime = path.stat().st_mtime
        if mtime < _closed(year, quarter) and time.time() - mtime > _CURRENT_TTL_S:
            return None
        part = json.loads(path.read_text())
        if not isinstance(part, list) or not all(
                isinstance(p, list) and len(p) == 2 and all(isinstance(x, str) for x in p) for p in part):
            return None
        os.utime(path, (time.time(), mtime))
    except (FileNotFoundError, ValueError):
        return None
    return part


def _keep(folder: Path, path: Path, part: list[list[str]]) -> None:
    """Writes the quarter's filers to *path* in one step, so another process never reads half of them."""
    fd, tmp = tempfile.mkstemp(dir=folder, prefix=f".{path.name}.")
    try:
        with os.fdopen(fd, "w") as out:
            out.write(json.dumps(part))
        os.replace(tmp, path)
        os.utime(path, (time.time(), time.time()))  # the kernel's own stamp is coarser
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


def _evict(folder: Path, wanted: list[tuple[int, int]]) -> None:
    """Deletes the lists of quarters outside *wanted* beyond the HISTORY most recently used; a list another process
    deleted first is passed over."""
    older = []
    for path in folder.glob("13f-hr-*.json"):
        if (named := _QUARTER_FILE.fullmatch(path.name)) and (int(named[1]), int(named[2])) not in wanted:
            with contextlib.suppress(FileNotFoundError):
                older.append((path.stat().st_atime, path))
    for _, path in sorted(older, reverse=True)[HISTORY:]:
        path.unlink(missing_ok=True)


def _filers(today: date, period: str | None = None) -> list[tuple[str, str]]:
    """The 13F-HR filers of _quarters(today), and of *period*'s filing quarter when it is older, as (CIK, name)
    pairs, newest quarter first, a CIK once per name it filed under. Each quarter's list is read from the disk cache
    where it is fresh: a list read once its quarter's index was complete never changes, and any other is read again
    after a day."""
    folder = _index_dir()
    folder.mkdir(parents=True, exist_ok=True)
    recent = _quarters(today)
    wanted = recent + ([due] if period is not None and (due := _filing_quarter(period)) < recent[-1] else [])
    parts: dict[tuple[int, int], list[list[str]]] = {}
    with _indexing:
        fetched = False
        for year, quarter in reversed(wanted):
            path = folder / f"13f-hr-{year}q{quarter}.json"
            part = _cached(path, year, quarter)
            if part is None:
                if fetched:
                    time.sleep(_REQUEST_GAP_S)
                part = _fetch_filers(year, quarter, current=(year, quarter) == wanted[0])
                fetched = True
                _keep(folder, path, part)
            parts[(year, quarter)] = part
        _evict(folder, wanted)
    return list(dict.fromkeys((cik, name) for q in wanted for cik, name in parts[q]))


def _search(name: str, period: str | None) -> dict[str, Any]:
    from edgar_engine.thirteenf import name_matches, name_tokens

    want = name_tokens(name)

    def compared(listed: str) -> str:
        """A name EDGAR cut at _NAME_CUT, as *name* when name continues it, so a name typed in full matches."""
        have = name_tokens(listed) if len(listed) >= _NAME_CUT else ()
        cut = len(have) - 1
        continues = have and want[:cut] == have[:cut] and len(want) > cut and want[cut].startswith(have[cut])
        return name if continues else listed

    filers = _filers(datetime.now(_NEW_YORK).date(), period)
    rows: dict[str, dict[str, str]] = {}
    for i in name_matches(name, [compared(n) for _, n in filers]):
        cik, filer_name = filers[i]
        rows.setdefault(cik, {"filer_cik": f"{int(cik):010d}", "filer_name": filer_name})
    return {"rows": list(rows.values()), "total": len(rows)}


async def find_managers(name: str, period: str | None = None) -> dict[str, Any]:
    """The 13F filers a manager's name means (``rows`` of ``filer_cik`` and ``filer_name``, and ``total``):
    edgar_engine.thirteenf.name_matches over every name a 13F-HR filer used in EDGAR's full index for this quarter and
    the three before, and for *period*'s filing quarter when it is older, one row per CIK. A failed index read raises,
    so a missing list never reads as no filer."""
    return await asyncio.to_thread(_search, name, period)
