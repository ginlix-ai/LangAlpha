"""40-F boundaries: the general-instruction wrapper, its printed titles, and the documents it carries."""
import re
from dataclasses import dataclass

from .boundaries import Heading, ItemSpan, Layout, toc_lines
from .rendering import Rendering
from .text import CONTENTS, EXHIBIT_LIST, SENTENCE, SIGNATURE_PARAGRAPH, SIGNATURES, normalize
from .vocab import spec_for

LABEL = r"(?:(?:[A-Z]|\d{1,2}|[IVX]{1,4})\.\s*|\([a-z0-9]{1,3}\)\s*)?"
COMMITTEE = r"AUDIT(?: AND [A-Z][A-Z ]{0,40}?)? COMMITTEE"
DOCS = spec_for("40-F").principal_documents
MDA_RX = r"MANAGEMENT'?S?'? DISCUSSION (?:AND|&) ANALYSIS"
AFS_RX = r"(?:AUDITED )?(?:ANNUAL )?(?:AUDITED )?(?:CONSOLIDATED )?FINANCIAL STATEMENTS"
TITLES = [
    (("AFS", "MDA"), rf".*FINANCIAL STATEMENTS AND {MDA_RX}"),
    (("MDA", "AFS"), rf"{MDA_RX} AND {AFS_RX}"),
    (("AC", "ACFE"), rf"{COMMITTEE} IDENTIFICATION AND FINANCIAL EXPERTS?"),
    (("AC", "FEES"), rf"{COMMITTEE} INFORMATION (?:AND|&) (?:PRINCIPAL )?(?:ACCOUNTANT|AUDITOR)'?S?'? FEES(?: AND SERVICES)?"),
    (("OBS", "CONTRACTUAL"), r"OFF[- ]?BALANCE[- ]SHEET (?:ARRANGEMENTS|TRANSACTIONS) AND (?:TABULAR DISCLOSURE OF )?CONTRACTUAL OBLIGATIONS?"),
    (("AIF",), r"(?:(?:19|20)\d\d )?(?:RENEWAL )?ANNUAL INFORMATION FORMS?"),
    (("AFS",), AFS_RX),
    (("MDA",), rf"{MDA_RX}(?:\s+OF\b.*|\s*\(.*\))?"),
    (("DCP",), r"(?:CERTIFICATIONS AND )?(?:DISCLOSURE REGARDING )?(?:INTERNAL CONTROLS? OVER FINANCIAL REPORTING AND )?"
               r"(?:EVALUATION OF )?(?:DISCLOSURE )?CONTROLS? AND PROCEDURES(?:\s*[;,]\s*INTERNAL CONTROL.*| AND INTERNAL CONTROL.*)?"),
    (("ACFE",), rf"{COMMITTEE} FINANCIAL EXPERTS?"),
    (("CODE",), r"CODES? OF (?:BUSINESS )?(?:CONDUCT AND )?(?:BUSINESS )?(?:ETHICS|CONDUCT)"),
    (("FEES",), r"(?:PRINCIPAL|INDEPENDENT|EXTERNAL) (?:ACCOUNTANTS?|ACCOUNTING|AUDITORS?)'? ?S?'? (?:FEES(?: AND SERVICES)?|SERVICE FEES)"),
    (("OBS",), r"OFF[- ]?BALANCE[- ]SHEET (?:ARRANGEMENTS|TRANSACTIONS)"),
    (("CONTRACTUAL",), r"(?:TABULAR )?(?:DISCLOSURE OF )?CONTRACTUAL OBLIGATIONS?"),
    (("AC",), rf"(?:IDENTIFICATION OF (?:THE )?)?{COMMITTEE}"),
    (("MINE",), r"MINE SAFETY(?: AND HEALTH ADMINISTRATION)?(?: DISCLOSURES?)?"),
    (("NYSE_GOV",), r"(?=.*\b(?:NYSE|NASDAQ|NEW YORK STOCK EXCHANGE)\b)(?=.*\b(?:GOVERNANCE|LISTING STANDARDS|LISTED COMPANY MANUAL|REQUIREMENTS|DISCLOSURE)\b).{0,110}"),
    (("RECOVERY",), r"(?:RECOVERY OF ERRONEOUSLY AWARDED COMPENSATION|COMPENSATION RECOVERY|CLAWBACK)\b.*"),
    (("INSIDER",), r"INSIDER TRADING (?:POLICIES|POLICY|ARRANGEMENTS)\b.*"),
    (("CYBER",), r"CYBERSECURITY"),
    (("UNDERTAKING", "CONSENT"), r"UNDERTAKINGS? AND CONSENT TO SERVICE OF PROCESS"),
    (("UNDERTAKING",), r"UNDERTAKINGS?"),
    (("CONSENT",), r"CONSENT TO SERVICE OF PROCESS"),
]
STANDALONE = [(ids, re.compile(rf"{LABEL}(?:{rx})\s*[.:]?", re.I)) for ids, rx in TITLES]
SUFFIXED = [(ids, re.compile(rf"{LABEL}(?:{rx})\s*(?:[-;,]|\bAND\b)\s+\S.*", re.I)) for ids, rx in TITLES if ids not in (("AFS", "MDA"),) and "NYSE" not in rx]
RUNIN = [(ids, re.compile(rf"{LABEL}(?:{rx})\s*[.:]\s+(?=[A-Z\"\u201c(\[])", re.I)) for ids, rx in TITLES if ".*" not in rx[:3] and "NYSE" not in rx]
PACKAGE = re.compile(r"(?:MANAGEMENT'?S (?:REPORT|RESPONSIBILITY)(?: (?:FOR|TO) .*)?|(?:INDEPENDENT )?AUDITORS?'? ?S?'? REPORT(?: TO .*)?|"
                     r"REPORT OF INDEPENDENT (?:REGISTERED PUBLIC ACCOUNTING FIRM|AUDITORS?|CHARTERED ACCOUNTANTS))\s*[.:]?", re.I)
UNLISTED = re.compile(LABEL + r"(?:NOTICES? PURSUANT TO REGULATION BTR|NOTICE OF PENSION FUND BLACKOUT PERIOD|(?:REGULATION )?BTR NOTICES?|"
                      r"(?:CAUTION(?:ARY)? (?:NOTE |STATEMENT )?(?:CONCERNING|REGARDING|ON) )?FORWARD[- ]LOOKING (?:STATEMENTS|INFORMATION)|"
                      r"NOTE REGARDING FORWARD[- ]LOOKING.*|NON-GAAP (?:AND OTHER )?FINANCIAL MEASURES|INTERACTIVE DATA FILES?|"
                      r"DISCLOSURE (?:PURSUANT TO|REQUIRED BY) (?:SECTION 13\(r\)|IRAN).*|DISCLOSURE REGARDING FOREIGN JURISDICTIONS.*|"
                      r"CRITICAL ACCOUNTING (?:POLICIES|ESTIMATES).*|TAX(?:ATION)? MATTERS|CORPORATE GOVERNANCE|BOARD DIVERSITY.*|"
                      r"(?:MINERAL )?(?:RESERVE AND RESOURCE|RESOURCE AND RESERVE) ESTIMATES|WEBSITE INFORMATION|DOCUMENTS FILED AS PART OF.*|"
                      r"ADDITIONAL DISCLOSURES?|ADDITIONAL INFORMATION|EXPLANATORY NOTE|CURRENCY|(?:NOTE TO UNITED STATES READERS ?-? ?)?DIFFERENCES IN UNITED STATES AND CANADIAN REPORTING PRACTICES|"
                      r"PRINCIPAL (?:CANADIAN )?DOCUMENTS|DOCUMENTS INCORPORATED BY REFERENCE|INCORPORATION BY REFERENCE|CERTIFICATIONS?)\s*[.:]?", re.I)
PRINCIPAL = re.compile(r"(?:PRINCIPAL (?:CANADIAN )?DOCUMENTS|DOCUMENTS INCORPORATED BY REFERENCE|INCORPORATION BY REFERENCE|"
                       r"MATERIAL INCLUDED IN THIS FORM.*|DOCUMENTS FILED UNDER COVER.*)\s*[.:]?", re.I)
DOC_LABEL = re.compile(r"DOCUMENT (?:NO\.? ?)?(?:\d+|[A-Z])", re.I)
CERT_DOC = re.compile(r"CERTIFICATION OF ANNUAL FILINGS|EXHIBIT \d+(?:\.\d+)?", re.I)
PERIOD_LINE = re.compile(r"(?:for the (?:fiscal )?(?:year|period|nine|six|three|twelve)\b.*|as (?:at|of) [A-Z][a-z]+ \d{1,2}, \d{4}|"
                         r"[A-Z][a-z]+ \d{1,2}, ?\d{4}|\(.*(?:dollars|u\.s\.).*\))", re.I)
CONNECT = re.compile(r"\b(?:AND|OF|ON|FOR|TO|THE|REGARDING|IN|BY|WITH|CORPORATE)\s*$", re.I)
CONT = {
    "DCP": re.compile(LABEL + r"(?:MANAGEMENT'?S? (?:ANNUAL )?REPORT|INTERNAL CONTROLS?|(?:AUDITOR'?S? )?ATTESTATION|AUDIT REPORT OF|CHANGES IN INTERNAL|"
                      r"CERTIFICATIONS?\b|EVALUATION OF DISCLOSURE|LIMITATIONS?\b|DISCLOSURE CONTROLS|CHANGES? IN INTERNAL)", re.I),
    "FEES": re.compile(LABEL + r"(?:(?:AUDIT COMMITTEE(?:'S)? |AUDIT AND [A-Z ]{1,40}? COMMITTEE )?PRE-?APPROVAL|AUDIT(?:-RELATED)? FEES|TAX FEES|"
                       r"(?:ALL )?OTHER FEES|HOURS EXPENDED|AUDIT SERVICES)", re.I),
    "AC": re.compile(r"(?:INDEPENDENCE|MEMBERS|COMPOSITION)", re.I),
}
EXCHANGE = re.compile(r"\b(?:NYSE|NASDAQ|New York Stock Exchange)\b", re.I)
LISTING_TEXT = re.compile(r"listing (?:standards|rules|requirements)|corporate governance (?:standards|rules|requirements|practices)|"
                          r"home[- ]country practice|\b303A\b|\bRule [45]\d{3}\b|Marketplace Rules?|"
                          r"(?:NYSE|NASDAQ|New York Stock Exchange)\W+(?:\w+\W+){0,4}governance", re.I)
PREAPPROVAL = re.compile(LABEL + r"(?:(?:AUDIT COMMITTEE(?:'S)? |AUDIT AND [A-Z ]{1,40}? COMMITTEE )?PRE-?APPROVAL\b.{0,100})", re.I)
SMALL_WORDS = {"of", "and", "the", "to", "for", "on", "in", "with", "a", "an", "or", "by"}
PAGEISH = re.compile(r"(?:-?\s*\d{1,3}\s*-?|[ivx]{1,4}|page \d+|.*\bpage \d+|40-F\d+|table of contents)", re.I)
BULLET = "\u2022\u00b7\u25cf\u25aa*"


def strip_label_cell(raw):
    return re.sub(r"^\s*((?:[A-Z]|\d{1,2}|[IVXivx]{1,4})\.|\([A-Za-z0-9]{1,3}\))\s*\|\s*", r"\1 ", raw)


def is_caps(s):
    letters = [c for c in s if c.isalpha()]
    return len(letters) >= 3 and sum(c.isupper() for c in letters) >= 0.9 * len(letters)


def titled(s):
    """A heading's case: all caps, or every significant word capitalized."""
    words = [w for w in re.findall(r"[A-Za-z][\w'’-]*", s) if w.lower() not in SMALL_WORDS]
    return is_caps(s) or bool(words) and all(w[0].isupper() for w in words)


def has_label(s):
    return bool(re.match(r"(?:(?:[A-Z]|\d{1,2}|[IVX]{1,4})\.\s*|\([a-z0-9]{1,3}\)\s*)\S", s))


def match_title(s, prev_raw):
    if not s or len(s) > 4000 or re.sub(r"^(?:[A-Za-z]|\d{1,2}|[IVXivx]{1,4})\.\s*|^\([a-z0-9]{1,3}\)\s*", "", s)[:1].islower():
        return None
    if len(s) <= 150:
        for ids, rx in STANDALONE:
            if rx.fullmatch(s) and (ids != ("NYSE_GOV",) or titled(s)):
                return ids, "standalone"
    if len(s) <= 120 and is_caps(s) and not SENTENCE.search(s[:-1] if s.endswith(".") else s):
        for ids, rx in SUFFIXED:
            if rx.fullmatch(s):
                return ids, "standalone"
    p = (prev_raw or "").rstrip()
    if not p or p[-1] in ".:;!?)\"\u201d" or is_caps(normalize(p)) or len(p) <= 50:
        for ids, rx in RUNIN:
            if rx.match(s[:200]):
                return ids, "runin"
    return None


def named_docs(s) -> list[str]:
    """The principal documents a line names, in the order it names them."""
    s = s[:200]
    hits = []
    for iid, rx in (("AIF", r"annual information form"), ("MDA", r"management'?s?'? discussion (?:and|&) analysis|\bMD&A\b"),
                    ("AFS", r"financial statements")):
        m = re.search(rx, s, re.I)
        if m:
            hits.append((m.start(), iid))
    return [iid for _, iid in sorted(hits)]


def exhibit_documents(lines, line_doc, start):
    """Principal documents a 40-F/A refiles as exhibits: each exhibit whose opening title names the annual
    information form, the statements or the MD&A (or the statements' report package) is that document whole."""
    out = {}
    n = len(lines)
    a = start
    while a < n:
        b = next((j for j in range(a, n) if line_doc[j] != line_doc[a]), n)
        seen = 0
        for j in range(a, b):
            s = normalize(strip_label_cell(lines[j]))
            if not s:
                continue
            seen += 1
            if seen > 25 or " | " in s:
                continue
            m = match_title(s, lines[j - 1] if j else "")
            ids = m[0] if m and m[1] == "standalone" and set(m[0]) <= DOCS else ("AFS",) if PACKAGE.fullmatch(s) else None
            if ids:
                for x in ids:
                    out.setdefault(x, ItemSpan(x, j, b, j, [o for o in ids if o != x]))
                break
        a = b
    return out


@dataclass
class _Lines:
    """The main document's lines as the 40-F rules read them, classified once: each line's kind ("sig", "exindex",
    "doclabel", "certdoc", "quicklinks", "title", "runin", "unlisted"), the ids a title names and its style, and
    the lines a two-line title took into the one above."""
    raw: list[str]
    norm: list[str]
    table: list[bool]
    kind: list[str | None]
    ids_at: dict[int, tuple[str, ...]]
    style: dict[int, str]
    consumed: set[int]

    def next_nonblank(self, i: int, window: int, skip_consumed: bool = False) -> int | None:
        """The first non-blank line among the window - 1 lines after i."""
        return next((j for j in range(i + 1, min(len(self.norm), i + window))
                     if self.norm[j] and not (skip_consumed and j in self.consumed)), None)


def fortyf(rendering: Rendering) -> Layout:
    """The 40-F's sections: the wrapper's titled sections in its main cluster, the principal documents it prints
    or lists, and for a 40-F/A the documents its exhibits refile."""
    main_end = rendering.main_end
    if main_end < len(rendering.lines):
        main = Rendering(rendering.lines[:main_end], rendering.line_doc[:main_end], rendering.documents[:1])
        layout = fortyf(main)
        layout.spans.update(exhibit_documents(rendering.lines, rendering.line_doc, main_end))
        return layout
    L = _classify(rendering.lines)
    heads = [Heading(i, "item", L.ids_at[i]) for i in L.ids_at]
    toc = _toc(L, rendering, heads)
    exrows = _exhibit_rows(L, toc)
    titles = [i for i in sorted(L.ids_at) if i not in toc and i not in exrows and not (L.table[i] and not has_label(L.norm[i]))]
    listed = _principal_list(L)
    group = _group_headings(L, titles)
    starts = [i for i in titles if i not in group]
    wrapper = _wrapper_cluster(L, titles, starts, toc, rendering.offsets)
    if wrapper is None:
        return Layout({}, toc, heads)
    main, wstart = wrapper
    printed = _printed_documents(L, main, toc)
    chosen = _assign(L, main, titles, printed, listed, toc)
    st_list = [L.style[i] for i in main if i in L.style]
    unlisted = _Unlisted(L, toc, main[0], max(main) + 400,
                         max(set(st_list), key=lambda s: (st_list.count(s), s == "caps")) if st_list else "caps")
    if "NYSE_GOV" not in chosen and (j := _governance_by_text(L, unlisted)) is not None:
        chosen["NYSE_GOV"], L.ids_at[j] = j, ("NYSE_GOV",)
    open_starts = sorted(set(chosen.values()))
    out = {}
    for sid, i in chosen.items():
        end = _section_end(L, sid, i, next((x for x in open_starts if x > i), len(L.norm)), rendering.line_doc, toc,
                           exrows, titles, group, main, wstart, printed, unlisted)
        out[sid] = ItemSpan(sid, i, end, i, [o for o in L.ids_at.get(i, (sid,)) if o != sid and o in chosen and chosen[o] == i])
    return Layout(out, toc, heads)


def _classify(lines: list[str]) -> _Lines:
    n = len(lines)
    raw = [strip_label_cell(ln) if len(ln) <= 4000 else "" for ln in lines]
    norm = [normalize(r) for r in raw]
    table = [(" | " in r and not re.match(r"^\s*(?:[A-Z]|\d{1,2})\. ", r)) or r.lstrip()[:1] in BULLET for r in raw]
    L = _Lines(raw, norm, table, [None] * n, {}, {}, set())
    kind = L.kind
    for i, s in enumerate(norm):
        if i in L.consumed or not s:
            continue
        if len(s) <= 40 and SIGNATURES.fullmatch(s) or (SIGNATURE_PARAGRAPH.match(s) and "duly caused" in s[:400].lower()):
            kind[i] = "sig"
            continue
        if len(s) <= 40 and EXHIBIT_LIST.fullmatch(s):
            kind[i] = "exindex"
            continue
        if len(s) <= 20 and DOC_LABEL.fullmatch(s):
            kind[i] = "doclabel"
            continue
        if len(s) <= 60 and CERT_DOC.fullmatch(s):
            kind[i] = "certdoc"
            continue
        if s.lower() == "quicklinks":
            kind[i] = "quicklinks"
            continue
        m = match_title(s, raw[i - 1] if i else "")
        if (m is None or CONNECT.search(s)) and i + 1 < n and is_caps(s) and len(s) <= 90 and is_caps(norm[i + 1]) and len(norm[i + 1]) <= 70:
            mj = match_title(s + " " + norm[i + 1], raw[i - 1] if i else "")
            if mj and mj[1] == "standalone":
                m = mj
                L.consumed.add(i + 1)
        if m and m[1] == "standalone" and len(s) > 100 and s.endswith(".") and set(m[0]) <= DOCS:
            m = None
        if m:
            kind[i] = "title" if m[1] == "standalone" else "runin"
            L.ids_at[i] = m[0]
            L.style[i] = "label" if has_label(s) else ("runin" if m[1] == "runin" else ("caps" if is_caps(s) else ("period" if s.endswith(".") else "title")))
            continue
        if len(s) <= 120 and UNLISTED.fullmatch(s):
            kind[i] = "unlisted"
    return L


def _toc(L: _Lines, rendering: Rendering, heads: list[Heading]) -> set[int]:
    """Tables of contents: the engine's rule over the titles, a window after a contents title, and QuickLinks blocks."""
    norm, n = L.norm, len(L.norm)
    toc = set(toc_lines(heads, rendering.lines, [x for ids, _ in TITLES for x in ids]))
    for i, s in enumerate(norm):
        if s and len(s) < 40 and CONTENTS.fullmatch(s.upper().replace("TABLE OF CONTENTS", "CONTENTS")) or (s and s.upper() == "TABLE OF CONTENTS"):
            seen, win = set(), []
            for j in range(i + 1, min(n, i + 80)):
                t = norm[j]
                if not t:
                    continue
                nx = L.next_nonblank(j, 3)
                if L.kind[j] == "doclabel" or (len(t) > 40 and SENTENCE.search(t)) or len(t) > 90 or t.upper() in seen or \
                        (nx is not None and (len(norm[nx]) > 90 or (len(norm[nx]) > 40 and SENTENCE.search(norm[nx])))):
                    break
                seen.add(t.upper())
                win.append(j)
            if len(win) >= 3:
                toc.update(win)
        if L.kind[i] == "quicklinks":
            line_doc = rendering.line_doc
            toc.update(range(i, next((j for j in range(i + 1, n) if line_doc[j] != line_doc[i]), n)))
    return toc


def _exhibit_rows(L: _Lines, toc: set[int]) -> set[int]:
    """Rows of an exhibit index, which are not headings."""
    exrows = set()
    in_ex = False
    for i in range(len(L.norm)):
        if L.kind[i] == "exindex":
            in_ex = True
            continue
        if L.kind[i] in ("sig", "doclabel"):
            in_ex = False
        if in_ex and L.kind[i] in ("title", "runin", "unlisted"):
            if L.table[i] or re.match(r"\d{1,3}(?:\.\d+)?[A-Za-z()]*\.?\s", L.norm[i]) or i in toc:
                exrows.add(i)
            else:
                in_ex = False
    return exrows


def _principal_list(L: _Lines) -> dict[str, int]:
    """Principal-document lists: each entry naming the AIF, statements or MD&A is a stub heading for it, for the
    document it names first, and for any other it names that no entry names first."""
    norm, kind, n = L.norm, L.kind, len(L.norm)
    listed: dict[str, int] = {}
    for i, s in enumerate(norm):
        if s and (len(s) <= 80 and PRINCIPAL.fullmatch(s) or re.match(r"the following documents\b.*\b(?:filed|incorporated|included)\b", s, re.I) and len(s) <= 300):
            entries = []
            for j in range(i + 1, min(n, i + 12)):
                if kind[j] in ("title", "unlisted", "sig", "exindex") and not (kind[j] == "title" and set(L.ids_at[j]) <= DOCS):
                    break
                if not re.match(r"the following", norm[j], re.I):
                    entries.append((j, named_docs(norm[j])))
            for j, docs in entries:
                if docs:
                    listed.setdefault(docs[0], j)
            for j, docs in entries:
                for d in docs[1:]:
                    listed.setdefault(d, j)
    return listed


def _group_headings(L: _Lines, titles: list[int]) -> set[int]:
    """A title whose next line is a title naming a subset of its ids only ends the previous section."""
    group = set()
    n = len(L.norm)
    for i in titles:
        nxt = next((j for j in range(i + 1, min(n, i + 3)) if L.norm[j] and j not in L.consumed and not PAGEISH.fullmatch(L.norm[j])), None)
        if nxt in L.ids_at and nxt in titles and set(L.ids_at[nxt]) < set(L.ids_at[i]):
            group.add(i)
    return group


def _wrapper_cluster(L: _Lines, titles: list[int], starts: list[int], toc: set[int],
                     offs: list[int]) -> tuple[list[int], int] | None:
    """The wrapper's own section headings: the cluster of starts naming the most wrapper sections, the starts up to
    its signatures, and the document titles just before it; with where the wrapper begins. An annual information
    form printed whole (its title, then its date) holds its own governance headings, which are not the wrapper's
    when the wrapper prints sections outside it."""
    norm, kind, ids_at, n = L.norm, L.kind, L.ids_at, len(L.norm)
    aif_at = next((i for i in titles if ids_at[i] == ("AIF",) and kind[i] == "title" and not L.table[i] and len(norm[i]) <= 150
                   and any(PERIOD_LINE.fullmatch(norm[j]) for j in range(i + 1, min(n, i + 4)) if norm[j])), None)
    wrapper = [x for x in starts if set(ids_at[x]) - DOCS]
    inside = set()
    if aif_at is not None:
        aif_end = next((j for j in range(aif_at + 1, n) if kind[j] in ("doclabel", "quicklinks", "sig", "certdoc")), n)
        if any(not aif_at < x < aif_end for x in wrapper):
            inside = set(range(aif_at + 1, aif_end))
            wrapper = [x for x in wrapper if x not in inside]
    clusters = []
    for i in wrapper:
        if clusters and offs[i] - offs[clusters[-1][-1]] <= 20000:
            clusters[-1].append(i)
        else:
            clusters.append([i])
    if not clusters:
        return None
    main = max(clusters, key=lambda cl: (len({x for i in cl for x in ids_at[i] if x not in DOCS}), len(cl)))
    lo = offs[main[0]] - 3000
    near = [i for i in starts if lo <= offs[i] <= offs[main[-1]] and (i in main or not set(ids_at[i]) - DOCS)]
    sig_after = next((j for j in range(main[-1] + 1, n) if kind[j] == "sig" and j not in toc), n)
    main = sorted(set(main) | {i for i in starts if main[-1] < i < sig_after and set(ids_at[i]) - DOCS and i not in inside})
    wstart = main[0]
    return sorted(set(main) | set(near)), wstart


def _doc_ids(L: _Lines, i: int, toc: set[int]) -> list[str] | None:
    """The principal documents a line titles when it opens one."""
    if i in L.ids_at and L.kind[i] == "title" and (set(L.ids_at[i]) & DOCS) and not L.table[i] and i not in toc and len(L.norm[i]) <= 150:
        return [x for x in L.ids_at[i] if x in DOCS]
    if PACKAGE.fullmatch(L.norm[i] or "") and not L.table[i] and i not in toc:
        return ["AFS"]
    return None


def _opening(L: _Lines, i: int) -> bool:
    """A title followed by the period it covers, as a document printed whole opens."""
    nx = L.next_nonblank(i, 4, skip_consumed=True)
    return nx is not None and bool(PERIOD_LINE.fullmatch(L.norm[nx]))


def _printed_documents(L: _Lines, main: list[int], toc: set[int]) -> dict[str, int]:
    """Principal documents printed whole in the body: the title after each document label, then, outside the
    wrapper's sections, the AIF that opens with its period and the MD&A and statements after it, each document
    kept where it was found first."""
    norm, n = L.norm, len(L.norm)
    printed: dict[str, int] = {}
    for lb in (i for i in range(n) if L.kind[i] == "doclabel"):
        st = next((j for j in range(lb + 1, min(n, lb + 9)) if _doc_ids(L, j, toc)), None)
        for doc_id in (_doc_ids(L, st, toc) if st is not None else None) or ():
            printed.setdefault(doc_id, st)
    aif = printed.get("AIF")
    if aif is None:
        aif = next((i for i in range(n) if _doc_ids(L, i, toc) == ["AIF"] and _opening(L, i)), None)
        if aif is None:
            return printed
        printed["AIF"] = aif
    cur = aif
    while True:
        nxt = None
        for want in ("MDA", "AFS"):
            if want in printed:
                continue
            c = next((i for i in range(cur + 1, n) if (d := _doc_ids(L, i, toc)) and d[0] == want
                      and (is_caps(norm[i]) or _opening(L, i) or (want == "AFS" and PACKAGE.fullmatch(norm[i])))
                      and (main[0] > i or i > main[-1]) and (want != "AFS" or i > printed.get("MDA", -1) or "MDA" in printed)), None)
            if c is not None and (nxt is None or c < nxt[1]):
                nxt = (want, c)
        if not nxt:
            break
        printed[nxt[0]] = nxt[1]
        cur = nxt[1]
    return printed


def _assign(L: _Lines, main: list[int], titles: list[int], printed: dict[str, int], listed: dict[str, int],
            toc: set[int]) -> dict[str, int]:
    """The heading line each section opens at: the wrapper's headings, then the documents printed or listed, a
    combined heading's named ids moved to their own sub-headings, and a pre-approval heading right before the
    fees title opening FEES."""
    norm, kind, ids_at = L.norm, L.kind, L.ids_at
    chosen: dict[str, int] = {}
    for i in main:
        for x in ids_at[i]:
            if x in DOCS and (kind[i] == "title" and len(norm[i]) > 60 and not has_label(norm[i])):
                continue
            chosen.setdefault(x, i)
    for d, i in printed.items():
        w = chosen.get(d)
        if w is None or w == i or kind[w] != "title" or _opening(L, w) or w in listed.values() or i < w:
            chosen[d] = i
    for d, i in listed.items():
        if d not in chosen:
            chosen[d] = i
    for i in sorted(set(chosen.values())):
        own = set(ids_at.get(i, ()))
        if len(own) < 2:
            continue
        for j in (t for t in titles if t > i):
            if not set(ids_at[j]) < own or any(kind[k] in ("sig", "unlisted", "exindex") for k in range(i + 1, j)):
                break
            for x in ids_at[j]:
                if chosen.get(x) == i:
                    chosen[x] = j
    if "FEES" in chosen:
        i = chosen["FEES"]
        k = next((k for k in range(i - 1, max(-1, i - 60), -1) if kind[k] or k in ids_at or PREAPPROVAL.fullmatch(norm[k] or "")), None)
        if k is not None and PREAPPROVAL.fullmatch(norm[k]) and titled(norm[k]) and k > max([x for x in chosen.values() if x < i], default=-1):
            chosen["FEES"] = k
    return chosen


@dataclass(frozen=True)
class _Unlisted:
    """A heading the 40-F's list does not name, in the wrapper's dominant heading style ("caps", "label", "period"
    or "title"), which ends the section before it. Lines a section's own continuation pattern names do not."""
    lines: _Lines
    toc: set[int]
    first: int
    region_end: int
    style: str

    def ends(self, j: int, sid: str) -> bool:
        L, norm = self.lines, self.lines.norm
        s = norm[j]
        if not s or j in self.toc or j in L.consumed or L.table[j] or len(s) > 110 or PAGEISH.fullmatch(s):
            return False
        cont = CONT.get(sid)
        if cont and cont.match(s):
            return False
        if L.kind[j] == "unlisted":
            return True
        if not (self.first <= j <= self.region_end):
            return False
        body = s[:-1] if s.endswith(".") else s
        if SENTENCE.search(body) or s.endswith(":") or len(re.findall(r"[A-Za-z]{2,}", s)) < 1:
            return False
        if j > 0 and is_caps(norm[j - 1]) and CONNECT.search(norm[j - 1]):
            return False
        nx = L.next_nonblank(j, 3)
        prose_next = nx is not None and len(norm[nx].split()) >= 8 and any(c.islower() for c in norm[nx])
        if self.style == "caps":
            return is_caps(s) and not has_label(s)
        if self.style == "label":
            return has_label(s) and len(s) <= 90
        if self.style == "period":
            return s.endswith(".") and len(s) <= 90 and s[0].isupper() and not has_label(s)
        if self.style == "title":
            return prose_next and len(s) <= 70 and not s.endswith(".") and not has_label(s) and titled(s)
        return False


def _governance_by_text(L: _Lines, unlisted: _Unlisted) -> int | None:
    """Exchange governance is decided by its text: when no heading names the exchange, the first unlisted heading
    whose own text is the listing-standards disclosure opens NYSE_GOV."""
    norm, n = L.norm, len(L.norm)
    for j in range(unlisted.first, min(n, unlisted.region_end)):
        if j in L.ids_at or j in unlisted.toc or not unlisted.ends(j, ""):
            continue
        stop = next((k for k in range(j + 1, n) if k in L.ids_at or L.kind[k] or unlisted.ends(k, "")), n)
        body = " ".join(norm[j + 1:stop])[:3000]
        if EXCHANGE.search(body) and LISTING_TEXT.search(body):
            return j
    return None


def _section_end(L: _Lines, sid: str, i: int, nxt: int, line_doc: list[int], toc: set[int], exrows: set[int],
                 titles: list[int], group: set[int], main: list[int], wstart: int, printed: dict[str, int],
                 unlisted: _Unlisted) -> int:
    """Where the section opening at line i ends, before the next section's opening (nxt). A document printed whole
    runs to a structural end or the wrapper; a wrapper section also ends at another title or an unlisted heading."""
    norm, kind, ids_at = L.norm, L.kind, L.ids_at
    is_doc = sid in DOCS and sid in printed and printed[sid] == i
    for j in range(i + 1, nxt):
        if line_doc[j] != line_doc[i]:
            return j
        if j in L.consumed:
            continue
        if is_doc:
            if kind[j] in ("sig", "doclabel", "certdoc", "quicklinks") or j == wstart or (j in group and j < wstart) or \
                    (kind[j] == "unlisted" and re.fullmatch(r"ADDITIONAL DISCLOSURES?\.?", norm[j], re.I)):
                return j
            continue
        if kind[j] in ("sig", "exindex", "doclabel", "certdoc", "quicklinks") and j not in toc:
            return j
        if j in group or (j in titles and j not in toc and j not in exrows and set(ids_at[j]) - {sid}):
            if not (j in titles and set(ids_at[j]) & DOCS and sid in DOCS and j not in main):
                return j
        if sid in ids_at.get(j, ()):
            continue
        if unlisted.ends(j, sid):
            return j
    return nxt
