"""Every string get_sec_filing shows the model: the inline result, the files' headers and the errors.

The wording is prompt surface; change it only with an approved before and after.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Sequence

from edgar_engine import FilingSections, Section, SubmissionDocument
from edgar_engine.filings import FilingRef
from edgar_engine.forms import item_title

if TYPE_CHECKING:
    from .edgar import Calendar, EightKRef, Financials

SCOPE_PHRASES = {"complete": "restates the whole report", "exhibits": "adds exhibits only",
                 "cover": "changes the cover page only"}
TARGET_PHRASES = {"exhibit": "points to an exhibit", "proxy": "points to the proxy statement",
                  "annual_report": "points to the annual report to shareholders",
                  "other_filing": "points to another filing", "none_stated": "points elsewhere without saying where"}
STATUS_LEGEND = ("Status: present is the item's own text; stub means the item points elsewhere, and its file holds the "
                 "pointer and, when the target is in this filing, that text; xref means the item has no heading and its "
                 "text is the pages the filing's cross-reference index lists for it.")
STATUS_LEGEND_NO_FILES = ("Status: present is the item's own text; stub means the item points elsewhere; xref means "
                          "the item has no heading and its text is the pages the filing's cross-reference index lists "
                          "for it.")
CUT_ITEM = "[Cut here: this result is limited to 40k tokens. The rest of this item and the earnings call are not included.]"
CUT_CALL = "[Cut here: this result is limited to 40k tokens. The rest of the transcript is not included.]"
CITE = "Cite this source URL inline for every claim you take from the filing."
CITE_AMENDED = ("Cite this source URL inline for every claim you take from the filing, or, for text or figures an "
                "amendment supplies, the source URL the item file or Key metrics names.")
CUT_DOCUMENTS = ("[Cut here: this result is limited to 40k tokens. The rest of this document and the documents after "
                 "it are not included.]")
NO_XBRL = "This filing predates XBRL, so it has no structured statements."
NO_STATEMENTS = "This filing has no structured statements."
STATEMENTS_FAILED = "The statements could not be read this time; call again later for them."
XBRL_FROM = "2009-06-15"


def tokens(chars: int) -> str:
    return "under 1k tokens" if chars < 4000 else f"{round(chars / 4000)}k tokens"


def period_label(ref: FilingRef, short: bool = False) -> str:
    fy, fp = ref.fiscal_year, ref.fiscal_period
    if fy is None:
        return ref.report_date or ref.filing_date
    if fp in (None, "FY"):
        return f"FY{fy}" if short else f"fiscal year {fy}"
    return f"{fp} FY{fy}" if short else f"{fp} of fiscal year {fy}"


def header_period(ref: FilingRef) -> str:
    if ref.fiscal_year is None:
        return ""
    return f", fiscal year {ref.fiscal_year}" + (f" {ref.fiscal_period}" if ref.fiscal_period not in (None, "FY") else "")


def item_file_name(item: str) -> str:
    if "-" in item and item.split("-", 1)[0] in ("I", "II"):
        part, rest = item.split("-", 1)
        return f"item_{part}-{rest.lower()}.md"
    return f"item_{item.lower()}.md"


def _heading_title(section: Section) -> str:
    h = " ".join((section.heading or "").split())
    return re.sub(r"^(?:PART\s+[IV]+\W*)?ITEMS?\s+[\w.]+\s*[.:\-\u2014\u2013]*\s*", "", h, flags=re.I).strip(" .") or h


def title(fs: FilingSections, section: Section) -> str:
    """The form's title for the item in the filing's era, else the heading the filer printed."""
    return item_title(fs.form, section.item, fs.filing_date) or _heading_title(section) or section.item


def target_text(fs: FilingSections, section: Section) -> str | None:
    t = section.target
    if not t or t.kind != "in_document":
        return None
    if t.text:
        return t.text
    return "".join(fs.text[s.start:s.end] for s in t.spans) or None


def _where(fs: FilingSections, section: Section) -> str:
    spans = section.target.spans if section.target else []
    if spans:
        start = spans[0].start
        for other in fs.items.values():
            if other is not section and other.status == "present" and other.start <= start < other.end:
                return f"Item {other.item}"
    return "text"


def status_cell(fs: FilingSections, section: Section) -> str:
    if section.status != "stub":
        return section.status
    t = section.target
    if t is None:
        return "stub"
    if t.kind == "in_document":
        if target_text(fs, section):
            return f"stub, points to this filing's {_where(fs, section)}"
        return "stub, points to a part of this filing it could not locate"
    return f"stub, {TARGET_PHRASES.get(t.kind, 'points elsewhere without saying where')}"


def item_body(fs: FilingSections, section: Section) -> str:
    body = section.text
    if section.status == "stub" and (tt := target_text(fs, section)):
        body = f"{body}\n\n{tt}"
    return body


def from_accession(accession: str, url: str) -> str:
    return f"From accession {accession}. Source: {url}\n"


def item_file(symbol: str, form: str, period: str, section: Section, fs: FilingSections, url: str, accession: str,
              filed: str) -> str:
    status = status_cell(fs, section)
    return (f"{symbol} {form} {period}, Item {section.item}: {title(fs, section)} ({status})\n"
            f"From accession {accession}, filed {filed}. Source: {url}\n\n{item_body(fs, section)}\n")


@dataclass
class AmendmentNote:
    accession: str
    filing_date: str
    scope: str | None
    items: Sequence[str] = ()


def _scope_phrase(a: AmendmentNote) -> str:
    if a.scope == "partial":
        return f"restates items {', '.join(a.items)}" if a.items else "restates some items"
    return SCOPE_PHRASES.get(a.scope or "", "scope not read")


def view_line(view: str, amendments: Sequence[AmendmentNote], unread: Sequence[AmendmentNote] = ()) -> str:
    """unread: amendments the filing list places on this filing that could not be read, so none is merged."""
    not_merged = "; ".join(f"{a.accession} (filed {a.filing_date})" for a in unread)
    tail = f" Not merged, because it could not be read: {not_merged}." if unread else ""
    if view == "as_amended" and amendments:
        last = amendments[-1]
        return (f"As amended through {last.accession} (filed {last.filing_date}). Each item file names the filing its "
                f"text comes from.{tail}")
    if not amendments and unread:
        return f"As filed. Amended by {not_merged}, not merged because it could not be read."
    if not amendments:
        return "As filed. No amendments on file."
    listed = "; ".join(f"{a.accession} (filed {a.filing_date}, {_scope_phrase(a)})" for a in amendments)
    return f'As filed. Amended by {listed}. Pass view="as_amended" to read the filing with its amendments merged in.{tail}'


def metrics_section(fin: Financials, filing_date: str, source: tuple[str, str] | None = None) -> list[str]:
    """source: the accession and URL of the amendment the figures come from, when not the filing's own."""
    lines = ["## Key metrics"] + ([from_accession(*source).rstrip()] if source else [])
    if fin.failed:
        return lines + [STATEMENTS_FAILED]
    if not (fin.metrics or fin.statements):
        return lines + [NO_XBRL if filing_date < XBRL_FROM else NO_STATEMENTS]
    metrics, unit = fin.metrics, {"USD": "$", None: ""}.get(fin.currency, f"{fin.currency} ")
    rows = []
    if metrics.get("revenue") is not None:
        rows.append(f"| Revenue | {unit}{metrics['revenue'] / 1e9:.2f}B |")
    if metrics.get("net_income") is not None:
        rows.append(f"| Net Income | {unit}{metrics['net_income'] / 1e9:.2f}B |")
    if metrics.get("current_ratio") is not None:
        rows.append(f"| Current Ratio | {metrics['current_ratio']:.2f} |")
    unread = [statements_unread(fin.unread)] if fin.unread else []
    return lines + (["| Metric | Value |", "|--------|-------|", *rows] if rows else []) + unread


def _and(names: Sequence[str]) -> str:
    return ", ".join(names[:-1]) + " and " + names[-1] if len(names) > 1 else "".join(names)


def statements_unread(parts: Sequence[str]) -> str:
    return f"The {_and(parts)} could not be read this time; call again later for {'them' if len(parts) > 1 else 'it'}."


@dataclass
class CallNote:
    status: str
    quarter: int | None = None
    fiscal_year: int | None = None
    call_date: str | None = None
    period_end: str | None = None
    chars: int = 0


def call_line(c: CallNote | None, in_file: bool = True) -> str | None:
    """in_file: the transcript is in earnings_call.md, else it has not been written anywhere."""
    if c is None:
        return None
    if c.status == "included":
        call = (f"the Q{c.quarter} FY{c.fiscal_year} earnings call held {c.call_date}, which reports the period ended "
                f"{c.period_end} ({tokens(c.chars)})")
        return f"earnings_call.md: {call}." if in_file else f"Earnings call matched: {call}."
    reason = {
        "no_match": f"no earnings call transcript could be matched to the period ending {c.period_end}.",
        "unpublished": f"the earnings call held {c.call_date} for the period ending {c.period_end} has no published transcript yet.",
        "failed": "the transcript service did not answer, so call again later for it.",
        "unsupported": "earnings calls are matched to 10-K and 10-Q filings only.",
    }[c.status]
    return f"No earnings call file: {reason}"


def call_file(symbol: str, form: str, accession: str, c: CallNote, transcript: str) -> str:
    return (f"{symbol} earnings call, Q{c.quarter} FY{c.fiscal_year}, held {c.call_date}\n"
            f"It reports the period ended {c.period_end}, the same fiscal period as {form} accession {accession}.\n\n"
            f"{transcript}\n")


_PERIOD_ORDER = {"Q1": 1, "Q2": 2, "Q3": 3, "FY": 4}


def _next_period(returned: FilingRef, fiscal_period: str) -> str:
    """The report after the returned one as " for <period>" in period_label's words; empty when the returned
    report's fiscal period is not known."""
    if returned.fiscal_year is None or returned.fiscal_period not in _PERIOD_ORDER or fiscal_period not in _PERIOD_ORDER:
        return ""
    fy = returned.fiscal_year + (_PERIOD_ORDER[fiscal_period] <= _PERIOD_ORDER[returned.fiscal_period])
    return f" for fiscal year {fy}" if fiscal_period == "FY" else f" for {fiscal_period} of fiscal year {fy}"


def calendar_line(calendar: Calendar | None, returned: FilingRef) -> str | None:
    """The filing calendar's line for a returned report that is the latest periodic report on file; None keeps the
    plain line (no calendar, another latest report, an unknown verdict, or a filing the filings table does not hold).
    A served calendar can be older than the day it is read, so the dates that decide a line are compared with that
    day. A window that runs past the due date is shown ending at the due date; the late lines say when a report is
    late."""
    nxt = calendar and calendar.served.next
    if nxt is None or calendar.served.latest != returned.accession:
        return None
    report = f"{nxt.form}{_next_period(returned, nxt.fiscal_period)} (period end {nxt.period_end})"
    start, end = nxt.window
    if start <= nxt.deadline < end:
        end = nxt.deadline
    window = f"expected between {start} and {end} and due by {nxt.deadline}"
    latest = "This is the latest periodic report on file"
    verdict = calendar.served.verdict
    if verdict == "current" and calendar.today >= start:
        verdict = "expected"
    if verdict in ("expected", "extended") and calendar.today > nxt.latest_deadline:
        verdict = "deregistered" if calendar.served.termination else "overdue"
    match verdict:
        case "current":
            return f"{latest}. The next is the {report}, {window}."
        case "expected" if calendar.today <= nxt.deadline:
            return f"{latest}, and the next may be filed any day: the {report}, {window}."
        case "expected":
            return (f"{latest}. The next, the {report}, was due by {nxt.deadline} and is not on file yet; it can still "
                    f"be on time until {nxt.latest_deadline}.")
        case "extended" if (notice := calendar.filed.get(nxt.notice or "")):
            return (f"{latest}. The {report} was due by {nxt.deadline}; the company filed a notice of late filing on "
                    f"{notice[1]}, which extends the due date to {nxt.latest_deadline}.")
        case "overdue":
            return (f"{latest}. The {report} is late: it was due by {nxt.deadline} and is still not on file after "
                    f"{nxt.latest_deadline}, the latest date it could have been on time.")
        case "deregistered" if (ended := calendar.filed.get(calendar.served.termination or "")):
            return (f"{latest}, and no newer one is expected: the company filed Form {ended[0]} on {ended[1]} to stop "
                    f"reporting, and the {report} due by {nxt.latest_deadline} was not filed.")
    return None


def recent_reports_section(reports: Sequence[FilingRef], returned: FilingRef, *, foreign: bool,
                           calendar: Calendar | None) -> list[str]:
    newer = [r for r in reports if (r.report_date or "", r.filing_date) > (returned.report_date or "", returned.filing_date)]
    lines = ["## Recent periodic reports"]
    if not newer:
        lines.append(calendar_line(calendar, returned) or "This is the latest periodic report on file.")
    else:
        latest = newer[0]
        what = f"the {latest.form} for {period_label(latest)}, period ended {latest.report_date}, filed {latest.filing_date}"
        lines.append(f"A newer report is on file: {what}. Fetch it for later figures." if len(newer) == 1
                     else f"{len(newer)} newer reports are on file; the latest is {what}. Fetch it for later figures.")
    lines += ["", "| Form | Period | Period end | Filed | |", "|---|---|---|---|---|"]
    top = list(reports[:4])
    for r in top:
        lines.append(f"| {r.form} | {period_label(r, short=True)} | {r.report_date or ''} | {r.filing_date} | "
                     f"{'returned' if r.accession == returned.accession else ''} |")
    if all(r.accession != returned.accession for r in top):
        lines.append("| ... | | | | |")
        lines.append(f"| {returned.form} | {period_label(returned, short=True)} | {returned.report_date or ''} | "
                     f"{returned.filing_date} | returned |")
    if foreign:
        lines += ["", "Interim results of a foreign issuer come in 6-K reports, which this tool does not fetch."]
    return lines


def _amended(form: str) -> str:
    return f" ({form})" if form.upper().endswith("/A") else ""


def recent_8k_section(eight_ks: Sequence[EightKRef]) -> list[str]:
    if not eight_ks:
        return []
    lines = ["## Recent 8-K filings (last 90 days)",
             f'{len(eight_ks)} filed since this report. They may carry material events, results or guidance; fetch them with filing_type="8-K".']
    for k in eight_ks:
        lines.append(f"- {k.ref.filing_date}{_amended(k.ref.form)}: {items_phrase(k.items, k.ref.filing_date)}. {k.url}")
    return lines


def items_phrase(items: Sequence[str], filed: str) -> str:
    return "; ".join(f"{i} {item_title('8-K', i, filed) or ''}".strip() for i in items) or "no items listed"


@dataclass
class ItemRow:
    item: str
    title: str
    status: str
    chars: int
    file: str


def periodic_inline(*, name: str, symbol: str, ref: FilingRef, url: str, view_text: str, metrics: list[str],
                    folder: str | None, rows: Sequence[ItemRow], missing: Sequence[str], full_chars: int,
                    statements: tuple[str, int, Sequence[str]] | None, call: str | None, recent: list[str],
                    recent_8k: list[str], amended: bool = False) -> str:
    """statements: the statements file, its size and the statements it holds, when the folder holds one. amended:
    the index is of the amended view, whose full.md is still the filing as first filed."""
    lines = [f"# {name} ({symbol}) {ref.form}{header_period(ref)}, period ended {ref.report_date or 'not stated'}",
             f"Accession {ref.accession}, filed {ref.filing_date}, CIK {ref.cik}. Source: {url}",
             CITE_AMENDED if amended else CITE, "", view_text, "", *metrics, "",
             "## Items"]
    if folder:
        lines.append(f"Each item's full text is in {folder}/. Read only the items you need.")
        lines += [STATUS_LEGEND, "", "| Item | Title | Status | Size | File |", "|---|---|---|---|---|"]
        lines += [f"| {r.item} | {r.title} | {r.status} | {tokens(r.chars)} | {r.file} |" for r in rows]
    else:
        lines += [STATUS_LEGEND_NO_FILES, "", "| Item | Title | Status | Size |", "|---|---|---|---|"]
        lines += [f"| {r.item} | {r.title} | {r.status} | {tokens(r.chars)} |" for r in rows]
    if missing:
        lines.append(f"Not in this filing: {', '.join(missing)}.")
    if folder:
        also = [f"full.md (the whole filing{' as first filed' if amended else ''}, {tokens(full_chars)})"]
        if statements:
            also.append(f"{statements[0]} ({_and(statements[2])}, {tokens(statements[1])})")
        lines += ["", f"Also in the folder: {'; '.join(also)}."]
    if call:
        lines.append(call)
    lines += ["", *recent]
    if recent_8k:
        lines += ["", *recent_8k]
    return "\n".join(lines).rstrip() + "\n"


def slug(text: str, limit: int = 44, keep: str = "") -> str:
    """text as lowercase words joined by hyphens, cut at the last whole word within limit."""
    s = re.sub(rf"[^a-z0-9{re.escape(keep)}]+", "-", text.lower()).strip("-")
    if len(s) > limit:
        s = s[:limit + 1].rsplit("-", 1)[0] if "-" in s[:limit + 1] else s[:limit]
    return s or "document"


def document_label(doc: SubmissionDocument, exhibit_list: dict[str, str]) -> str:
    desc = " ".join(doc.description.split())
    if desc and desc.upper() != doc.type.upper():
        label = desc
    elif (number := doc.type.upper().removeprefix("EX-")) in exhibit_list:
        label = exhibit_list[number]
    else:
        label = next((ln.strip() for ln in doc.text.splitlines() if ln.strip()), doc.filename)
    label = " ".join(label.split())
    return label if len(label) <= 120 else label[:119].rstrip() + "\u2026"


def document_file_name(doc: SubmissionDocument, label: str) -> str:
    return f"{slug(doc.type, 20, keep='.')}_{slug(label)}.md"


def document_file(symbol: str, filed: str, doc: SubmissionDocument, label: str, accession: str, url: str) -> str:
    return f"{symbol} 8-K {filed}, {doc.type}: {label}\nFrom accession {accession}. Source: {url}\n\n{doc.text}\n"


@dataclass
class EightKEntry:
    filed: str
    form: str
    accession: str
    url: str
    items: str
    folder: str | None = None
    item_files: Sequence[str] = ()
    item_chars: int = 0
    documents: Sequence[tuple[str, str, str, int]] = ()
    unreadable: bool = False


def eight_k_inline(name: str, symbol: str, entries: Sequence[EightKEntry], folders: bool = True) -> str:
    """folders: the filings' text is in their folders; without them it follows inline."""
    lines = [f"# {name} ({symbol}) 8-K filings, last 90 days: {len(entries)}",
             "Cite each filing's source URL inline for every claim you take from it."
             + (" Each filing's text is in its folder below." if folders else "")]
    for e in entries:
        lines += ["", f"## {e.filed}, accession {e.accession}{_amended(e.form)}", f"Source: {e.url}", f"Items: {e.items}"]
        if e.unreadable:
            lines.append(EIGHT_K_UNREADABLE)
        if e.folder:
            lines.append(f"Folder: {e.folder}/")
            if e.item_files:
                lines.append(f"- {', '.join(e.item_files)} ({tokens(e.item_chars)} in all)")
            lines += [f"- {fname}: {dtype}, {label} ({tokens(chars)})" for fname, dtype, label, chars in e.documents]
    return "\n".join(lines) + "\n"


EIGHT_K_UNREADABLE = "Its text could not be read this time; call again later for it."


def no_8ks(symbol: str, days: int) -> str:
    return f"No 8-K filings found for {symbol} in the last {days} days."


def bad_form(x: str) -> str:
    return f'filing_type must be "10-K", "10-Q", "20-F", "40-F" or "8-K", not "{x}".'


def bad_view(x: str) -> str:
    return f'view must be "as_filed" or "as_amended", not "{x}".'


def no_filing(symbol: str, form: str, fiscal_year: int | None, fiscal_period: str | None, on_file: Sequence[str]) -> str:
    which = " for" + (f" fiscal year {fiscal_year}" if fiscal_year else "") + (f" {fiscal_period}" if fiscal_period else "")
    which = "" if which == " for" else which
    tail = f" Its {form}s on file cover: {', '.join(on_file)}." if on_file else ""
    return f"{symbol} has no {form}{which} on EDGAR.{tail}"


def not_a_filer(symbol: str, form: str) -> str:
    return f"{symbol} is not an SEC registrant, so it files no {form}."


def period_on_annual(form: str) -> str:
    return f"fiscal_period applies to a 10-Q; a {form} covers the whole fiscal year."


def eight_k_period() -> str:
    return "An 8-K request returns every 8-K of the last 90 days; omit fiscal_year and fiscal_period."


def eight_k_amended() -> str:
    return ('view="as_amended" applies to 10-K, 10-Q, 20-F and 40-F; an 8-K request lists each 8-K/A on its own, '
            "as filed.")


def bad_period(period: str) -> str:
    return f'fiscal_period must be "Q1", "Q2" or "Q3", not "{period}"; a fiscal year\'s fourth quarter is reported in its 10-K.'


def files_not_written(folder: str, reason: str) -> str:
    return (f"The filing was read, but its files could not be written to {folder}: {reason}. "
            "The index above is complete; call again to write the files.")


def unreadable(accession: str, url: str) -> str:
    return f"EDGAR's copy of {accession} could not be read to its end twice; try again later. Source: {url}"
