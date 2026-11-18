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

## Filing calendar

`edgar_engine.calendar` answers, for any filer of 10-K, 10-Q, 20-F or 40-F reports: is its latest periodic report
the latest there should be, and when is the next one expected and due? It is pure functions over the filer's
filing list (the rows of EDGAR's submissions JSON or edgartools' filings table); it makes no request.

```python
from datetime import datetime
from zoneinfo import ZoneInfo
from edgar_engine.calendar import filings_from_rows, report_currency

filings = filings_from_rows(rows)        # the filer's whole list: periodic reports, NT notices, 8-Ks, 10-Ds, Forms 15
status = report_currency(filings, as_of=datetime.now(ZoneInfo("America/New_York")).date(), category="Large accelerated filer", fiscal_year_end="0927")
status.verdict             # "current", "expected", "extended", "overdue", "deregistered" or "unknown"
status.latest              # the latest original periodic report on file
nxt = status.next          # ExpectedFiling: form, fiscal_period, period_end, expected, window, deadline, latest_deadline
status.owed                # every report owed since the latest one, then the next period's
```

`as_of` is a day in EDGAR's time zone (US Eastern), where filing dates are kept. `fiscal_year_end` is the one EDGAR
stated on `as_of`: for a past `as_of` before a change of fiscal year end, pass the year end then in force, since
today's disagrees with the reports filed then and gives `unknown`. The verdict is `unknown`, with a
`reason`, when the list holds no periodic report, when the fiscal calendar cannot be read (only quarterly reports and
no `fiscal_year_end`), or when the stated fiscal year end disagrees with the one the reports follow, as during a
change of fiscal year before its transition report (10-KT, 10-QT) is on file. A Form 15 or 15F filed after the latest
report's period end (a filer going dark often files it before that report) is `termination`; since one can retire a
single class of notes while the filer keeps reporting, the verdict turns `deregistered` only once the next report is
overdue. A filer with a Form 15 on file and no report for a year and an annual report's latest due date past its
latest period is `deregistered` even when its calendar would read `unknown`.

**Due dates.** `deadline(form, period_end, category)` applies the rules in force for the form, the filer category and
the fiscal year (`DEADLINE_RULES`, one row per rule with its source): 10-K 60, 75 or 90 days and 10-Q 40 or 45 days
by category, with the 2003 to 2006 phase-in; 20-F six months, four for fiscal years ending on or after 2011-12-15;
40-F with the Canadian annual filing, 140 days and from 2004 90 days (120 for a venture issuer). A due date on a weekend, a federal holiday or a day the government was closed by executive order
(`CLOSURES`, kept by hand: a closure missing from it moves a due date a day early)
rolls to the next business day (Rule 0-3). `extended_deadline` adds Rule 12b-25's 15 days (annual) or 5 days
(quarterly) from that rolled date, for a notice filed by the business day after it; a 40-F has no such extension.
The filing list does not say whether a notice claims the extension in its Part II, which nearly every timely notice
does, so every timely notice is read as claiming it. A transition report (10-KT, 10-QT) has no `deadline`: Rules
13a-10 and 15d-10 count it from the later of the transition period's end and the date the change was decided, which
the filing list does not hold.
With no category known, the non-accelerated deadline applies, the latest any domestic filer may use. Every function
takes the category in any spelling `normalize_category` reads. `latest_deadline` is the latest a report can still be
on time given what EDGAR does not say in advance: an annual report's category is set at the year end it covers and
can drop to non-accelerated, and a 40-F filer may be a venture issuer; with `extended=True` it adds the Rule 12b-25
extension. `ExpectedFiling.latest_deadline` also allows a projected 52/53-week quarter to be a 14-week one. A report
is overdue only after it, or after its extension when a timely notice is on file.

**Filer category.** `normalize_category` reads any way EDGAR states it: XBRL `dei:EntityFilerCategory`, the
Financial Statement Data Sets' code, or the category line of the submissions JSON. `filer_category_from_document`
reads it from a filing's inline XBRL cover page or XBRL instance. A filer's category changes only at a fiscal year
end, so a 10-K can state a new one: about 7% of 10-Ks do, against under 1% of 10-Qs.

**Fiscal calendar.** The filer's year end and quarter ends are read from the period ends it has reported: the last
day of a month, or for a 52/53-week year the last given weekday of a month or the one nearest its end, or a fixed
date. A 52/53-week quarter ends 13 weeks after the period before it. A projected period end is a forecast: a report
whose period ends within 10 days of it covers it, so a 14-week quarter does not leave a phantom one owed. A 10-K
filer owes 10-Qs unless it is an asset-backed issuer (a 10-D and no 10-Q in two years: a sponsor co-files its
securitization's 10-Ds and keeps filing 10-Qs); never having filed one is no exemption (Rules 13a-13 and 15d-13
exempt classes of issuer).

**Expected date.** The filer's own lag a year earlier for the same fiscal period, held to the due date when that
lag was on time. With no history, the due date less the median slack of filers of its category. An earnings release
(an 8-K with Item 2.02) after the period end moves the expected date to the release when the lag put it earlier; a
notice of late filing moves it to the extended due date. The window around it holds the middle 80% of outcomes for
that basis. All population figures live in `CALIBRATION`.

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
