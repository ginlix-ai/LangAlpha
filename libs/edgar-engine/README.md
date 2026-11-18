# langalpha-edgar-engine

Structured data from SEC EDGAR filings, built on [edgartools](https://github.com/dgunning/edgartools). It uses
edgartools for company lookup, filing lists and downloads, and does its own parsing, so it works on edgartools
5.62 and 6.0 alike (it never touches the legacy `edgar.files` parser or `ChunkedDocument`).

## Sections

`edgar_engine.sections` splits a filing into its items: 10-K and 10-K405 (Items 1 to 16), 10-Q (Part I and II
items, keyed `I-2`, `II-1A`), 8-K (`2.02` since August 2004, the old `1` to `12` before), 20-F (1 to 19,
4A, 16A to 16K) and 40-F (titled sections: `AIF`, `MDA`, `DCP`, `ACFE`, `CODE`, `FEES`, ...). It covers every
era of EDGAR, from 1993 plain-text filings to inline XBRL.

```python
from edgar import set_identity
from edgar_engine.cache import SectionsCache
from edgar_engine import get_sections

set_identity("Your Name you@example.com")
sections = get_sections("AAPL", "10-K", fiscal_year=2019, cache=SectionsCache())
mdna = sections["7"]
mdna.status       # "present", "stub" (points elsewhere) or "xref" (mapped by a cross-reference index)
mdna.text         # the item's text, lossless
mdna.target       # for a stub: where its content is (kind, description, and the text when it is in this filing)
```

`extract_sections(submission)` does the same for a full-text submission you already hold (the accession's
`.txt` file), and `sections_for_filing(filing)` for an edgartools `Filing`. `submission_documents(submission, keep=None)` returns
every non-binary document of a submission (type, filename, description, rendered text) in sequence order, for the
exhibits the items do not cover; `keep(type, filename)` skips a document before it is rendered, and a truncated
submission raises `ValueError`.

**How items are found.** The engine renders the main document (and, for annual reports, every Exhibit 13) into
lines, one per paragraph, heading or table row, without deleting anything: page numbers, running headers and
number-only table cells stay. An item heading is "Item" and the item's number at the start of a line. For
each item in the form's order, the longest stretch from one of its headings to the nearest later item that has
a heading wins, so a table-of-contents entry, which only reaches the next contents line, loses to the body. A
stretch also ends at a PART heading of a later part, SIGNATURES or the signature paragraph, the financial
statement pages (for the last item), an 8-K section divider, or the end of the document. Tables of contents
are recognised as dense runs of headings with page numbers, so an item printed only there is not taken from
them. 10-Q filings are split into Part I and Part II first. 40-F sections are found by their titles.

**Stubs and cross-references.** An item whose own text only points elsewhere ("incorporated by reference to the
proxy statement", "see Note 7", "is set forth under 'Market Risk' on pages 20 to 23 of the Annual Report") is a
stub. The engine resolves its target only inside the filing: another item, a note to the statements, a
captioned section, the exhibit index, the financial statement pages or a printed page range, in the main
document or an Exhibit 13. When the pointer names nothing it can find, the stub keeps the target's kind and
description and no text, never a guess. Items with no heading that a cross-reference index maps to pages are
returned as `xref` with the mapped pages.

## Amendments

10-K/A, 10-Q/A, 20-F/A and 40-F/A are read like any filing: an amendment has the items it prints and no others,
so one that carries only Part III, an exhibit list or a cover page simply has fewer items. Every result says
which filing it is: `filing_status` is `"original"` or `"amendment"`, and an amendment's `amends` is the
accession of the original it amends (`None` when that cannot be found).

```python
original = get_sections("AAPL", "10-K", fiscal_year=2023)                          # as filed (the default)
amended = get_sections("AAPL", "10-K", fiscal_year=2023, view="as_amended")      # with every amendment merged in
first = get_sections("AAPL", "10-K", fiscal_year=2023, amendment=1)              # the first amendment, as filed
through = get_sections("AAPL", "10-K", fiscal_year=2023, view="as_amended", amendment=1)
amended["10"].source   # the accession the item's text comes from
amended.amendments     # the amendments merged, in filing order
first.scope            # "complete", "partial", "exhibits" or "cover"
```

`amendment=n` counts in filing order from 1, or from the end with -1 for the latest. `filed_on_or_before`
leaves out later filings, so an as-amended view can be taken as of a day.

**Which filing an amendment amends.** EDGAR does not record it. The engine takes the latest original of the same
form (10-K405 counts as 10-K) with the same period of report, filed on or before the amendment; this holds for
amendments filed years later and for several amendments to one original. Some filings carry a wrong period of
report, so an amendment is also read: the period its cover page states, or else the date its explanatory note
gives for the original filing, places it, and wins over the period of report when it does.

**What an amendment restates.** An amendment that prints an item from every part of the form and at least half
its items (every principal document for a 40-F), the exhibit list aside, is `complete` and replaces the whole
item set. Otherwise it is `partial` (or `exhibits` when it prints only its exhibit list) and replaces only the
items it prints, its exhibit list included; the original's full list stays in the as-filed view. An amendment
that prints no item at all, or that furnishes an employee plan's Form 11-K, is `cover` and changes no item. The as-amended view applies the amendments in filing order. Its `text` is the original's text
followed by each amendment's, so every offset still indexes it. It is computed from the per-accession cache
entries and is not cached itself.

## Filings and cache

`edgar_engine.filings.select_filing(company, form, fiscal_year=..., fiscal_period=..., period_end=...)` picks an
original filing for any period, not only the latest; its amendments are read with `get_sections`. Fiscal
labels come from the filer's own XBRL facts where they exist, and otherwise from the report date and fiscal
year end, moved to the filer's XBRL convention (a January year end is the prior fiscal year for some filers and
the same one for others). `edgar_engine.cache` keys parsed sections and raw
submissions by accession over a small `Store` interface; `LocalDirStore` (a directory, `EDGAR_ENGINE_CACHE` or
`~/.cache/edgar-engine`) is the default, and object storage plugs in by implementing `get`, `put` and `delete`.
`S3Store` does so over any S3-compatible client with boto3's call shape, which you pass in, so the library
depends on no S3 package: `SectionsCache(S3Store(boto3.client("s3", endpoint_url=...), "bucket", prefix="edgar/"))`.
A missing object reads as absent; any other client error propagates.
Cached sections are keyed by `SECTIONS_VERSION`, so an engine change never serves stale results. Pass
`submissions=SubmissionCache()` to `get_sections` or `sections_for_filing` to keep raw submissions too, so a
re-parse reads exactly what EDGAR served; a submission that does not parse (a truncated download) is not kept.

## Tests

```bash
cd libs/edgar-engine && uv run pytest          # unit tests, no network
uv run pytest -m network                       # also read a few public filings from EDGAR
```
