"""Filings, sections and a workspace for get_sec_filing's tests, read without the network."""

from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace

from edgar_engine import FilingSections, Section, Target
from edgar_engine.filings import FilingRef

ACC, AMEND = "0000000001-26-000001", "0000000001-26-000002"
REF = FilingRef(1, ACC, "10-K", "2026-02-01", "2025-12-31", 2025, "FY")
FOLDER = f"data/sec/ACME/10-K_FY2025_{ACC}"
INCOME = ("income statement",)


def section(item, text, status="present", start=0, target=None, source=None):
    return Section(item=item, status=status, text=text, start=start, end=start + len(text), target=target, source=source)


def filing(*sections, accession=ACC, form="10-K", filed="2026-02-01", scope=None):
    """Sections as the engine reads them, each naming its filing as its source."""
    text = "\n".join(s.text for s in sections)
    items = {s.item: replace(s, source=s.source or accession) for s in sections}
    return FilingSections(accession=accession, form=form, filing_date=filed, items=items, text=text,
                          engine_version="t", scope=scope)


ORIGINAL = filing(section("1", "Item 1. Business\nWe make anvils."),
                  section("7", "Item 7. MD&A\nSales rose on anvil demand.", start=30),
                  section("11", "Item 11. Executive Compensation\nSee the proxy statement.", "stub", start=70,
                          target=Target(kind="proxy")))


class Backend:
    """A workspace whose uploads fail for the paths fail names (every path when fail is a string)."""

    def __init__(self, fail=None):
        self.files, self.batches, self.fail = {}, [], fail
        self.filesystem_config = SimpleNamespace(enable_path_validation=False)

    def error(self, path):
        if isinstance(self.fail, str):
            return self.fail
        return "write_failed: gone" if self.fail and path in self.fail else None

    async def aread_text(self, path):
        return self.files.get(path)

    async def aupload_files(self, files):
        self.batches.append([p for p, _ in files])
        self.files.update({p: c.decode() for p, c in files if not self.error(p)})
        return [SimpleNamespace(path=p, error=self.error(p)) for p, _ in files]
