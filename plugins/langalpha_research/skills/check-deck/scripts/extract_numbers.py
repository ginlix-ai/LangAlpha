#!/usr/bin/env python3
"""Check that a deck tells one story about every number: same metric, same period, same value.

Usage:
    python extract_numbers.py <deck.json | deck.pptx | content.md> [--check]

The input is the JSON the pptx skill's extractor writes, which keeps the slide
structure a prose export throws away:

    python .agents/skills/pptx/scripts/extract.py deck.pptx > deck.json

A .pptx path works directly when that extractor is reachable, and a markdown or
text export is read as a fallback, where a "## Slide 3" or "--- slide 3 ---"
line starts a slide.

Checks, in order of importance:

    value_conflict   one metric and period carrying two different values
    phrase_conflict  the same wording ahead of two different values
    currency_mixing  one metric and period stated in two currencies
    unit_mixing      one metric written at two scales on a single slide
    source_missing   a number heavy slide with no source, footnote or as-of line on it

Every number is keyed by the metric word and the period token nearest to it, and
two numbers are compared only when the key, the unit class (percent, bps,
multiple, currency, count) and the currency mark all match. That is what keeps an
FY25E forecast from reading as a contradiction of the FY24A actual, a CY24 figure
from reading as a contradiction of the FY24 one, an H1 figure from reading as a
contradiction of the H2 one, and $1,200m from reading as a contradiction of
$1.2bn. A scale suffix written with no currency mark takes its class from the
metric beside it, so a revenue slide's 100m still weighs against $120m while 5m
customers stays a count, and a per-share figure written with its decimals is a
price with or without the mark, so an EPS of 5.00 weighs against $6.00. Each
entry in the numbers array carries its key, slide and location, so a report can
list every occurrence behind a finding.

Findings carry a level: "fail" blocks delivery, "warn" is a judgement call.
With --check the exit code is 1 when any fail exists.
"""

from __future__ import annotations

import importlib.util
import json
import re
import sys
from decimal import Decimal
from pathlib import Path

EXAMPLES = 25
DENSE_SLIDE = 5  # numbers on one slide before it owes the reader a source
WINDOW = 90  # characters a metric or period word may sit from its number
TOLERANCE = {"percent": 0.005, "bps": 0.005, "multiple": 0.005}
DEFAULT_TOLERANCE = 0.01

# the plural is how a column header writes the scale, "($ in millions)", so it has
# to reach the same size as the suffix on a figure; while the table held singulars
# only, that header's bare 100 stayed a count and never weighed against $120m
SCALE = {name: (word, size) for word, size, spellings in (
    ("thousand", 1e3, "k thousand thousands"), ("million", 1e6, "m mm mn million millions"),
    ("billion", 1e9, "b bn billion billions"), ("trillion", 1e12, "t tn trillion trillions"),
) for name in spellings.split()}

# a category series writes its points in values, an XY or bubble series on named axes
CHART_POINTS = (("values", ""), ("x_values", " x"), ("y_values", " y"), ("bubble_sizes", " size"))

NUMBER_RE = re.compile(
    r"(?P<pre>(?:[$€£¥(+-]\s?){0,3})"
    r"(?P<num>\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d+(?:\.\d+)?|\.\d+)"
    r"(?P<close>\s?\))?"
    r"\s?(?P<suffix>%|bps|bp|basis\s+points?|mm|mn|bn|tn|thousands?|millions?|billions?|trillions?|[kmbtx])?"
    r"(?![A-Za-z0-9])",
    re.IGNORECASE,
)

METRICS = {
    "revenue": ["revenue", "revenues", "net sales", "sales", "top line", "topline"],
    # a deck that writes both is reconciling two claims about one line, not
    # contradicting itself, so gross and net are keyed apart from each other and
    # from the bare word; "net sales" stays on "revenue" because it is how a
    # filing writes the ordinary top line, not a contrast with a gross figure
    "gross revenue": ["gross revenue", "gross revenues"],
    "net revenue": ["net revenue", "net revenues"],
    "ebitda": ["ebitda", "adjusted ebitda", "adj. ebitda"],
    "ebitda margin": ["ebitda margin"],
    "ebit": ["ebit", "operating income", "operating profit"],
    "gross margin": ["gross margin", "gross profit margin"],
    "net income": ["net income", "net profit", "net earnings"],
    "eps": ["eps", "earnings per share"],
    "fcf": ["fcf", "free cash flow"],
    # "cfo" is left out on purpose: on a team slide it is the officer, and keying
    # a bio's years of experience as cash flow is a blocking conflict about nothing
    "ocf": ["operating cash flow", "cash flow from operations", "cash from operations"],
    "operating margin": ["operating margin", "operating profit margin", "ebit margin"],
    "net margin": ["net margin", "net profit margin"],
    "margin": ["margin", "margins"],
    "growth": ["growth", "yoy", "y/y", "year over year", "year-over-year"],
    "cagr": ["cagr"],
    "irr": ["irr"],
    "gross irr": ["gross irr"],
    "net irr": ["net irr"],
    "moic": ["moic", "money multiple", "cash on cash"],
    "gross moic": ["gross moic"],
    "net moic": ["net moic"],
    "capex": ["capex", "capital expenditure", "capital expenditures"],
    "net debt": ["net debt"],
    "leverage": ["leverage", "net leverage", "debt / ebitda", "debt/ebitda"],
    "ev": ["enterprise value", "ev"],
    "market cap": ["market cap", "market capitalisation", "market capitalization"],
    "share price": ["share price", "stock price"],
    "price target": ["price target", "target price"],
    "wacc": ["wacc", "discount rate", "cost of capital"],
    "ev/ebitda": ["ev/ebitda", "ev / ebitda"],
    "ev/revenue": ["ev/revenue", "ev / revenue", "ev/sales"],
    "p/e": ["p/e", "pe multiple", "price / earnings", "price to earnings"],
    "multiple": ["multiple", "multiples"],
    "share count": ["share count", "shares outstanding", "diluted shares"],
    "dividend": ["dividend", "dividends", "dps"],
    "buyback": ["buyback", "buybacks", "share repurchase", "repurchases"],
    "headcount": ["headcount", "employees", "fte"],
    "customers": ["customers", "clients", "subscribers"],
    "arr": ["arr", "annual recurring revenue"],
    "churn": ["churn"],
    "retention": ["net revenue retention", "nrr", "retention"],
}
METRIC_PHRASE = {p: canon for canon, phrases in METRICS.items() for p in phrases}
# words that name a family of metrics rather than one metric
GENERIC_METRICS = {"margin", "growth", "multiple"}
QUALIFIERS = 3  # words in front of a family word that may say which member it is
# metrics counted in whole units, the only ones whose figure can land inside the year range
COUNT_METRICS = {"headcount", "customers"}
TOUCHING = 3  # characters between a count word and the digits it claims
# metrics the table states as an amount of money, so a lone scale suffix on one of
# them is a sum and not a tally: "revenue 100m" has to weigh against "$120m"
MONETARY_METRICS = {"revenue", "gross revenue", "net revenue", "arr", "ebitda", "ebit",
                    "net income", "fcf", "ocf", "capex", "net debt", "ev", "market cap"}
# metrics quoted per share, whose figure is a price and carries no scale of its own:
# read as a tally, "EPS FY25 5.00" never weighed against the "$6.00" two slides on.
# The decimals are what separate that price from the tallies a deck writes beside the
# same word, "raised for 12 consecutive years", which are counts and not money
PER_SHARE_METRICS = {"eps", "dividend", "share price", "price target"}
METRIC_RE = re.compile(
    r"\b(?:" + "|".join(re.escape(p) for p in sorted(METRIC_PHRASE, key=len, reverse=True)) + r")\b",
    re.IGNORECASE,
)

QUALIFIER_RE = re.compile(r"([A-Za-z][A-Za-z.'/&-]*)\s*$")
# a year as a deck writes it beside a period word: FY 2025, FY'25, Q1 ’25, 1Q FY25
YEAR = r"\s?['’]?\s?(?:\d{4}|\d{2})"
PERIOD_RE = re.compile(
    # the year rides on an apostrophe as often as on a space, and a quarter that
    # stops at its own digit leaves the year behind for the scanner to read as a figure
    r"\b(?:fy|cy)" + YEAR + r"[aeplf]?\b"
    r"|\b(?:19|20)\d{2}[aep]\b"
    r"|\bq[1-4](?:\s?(?:fy)?" + YEAR + r")?[aeplf]?\b"
    r"|\b[1-4]q(?:\s?(?:fy)?" + YEAR + r")?[aeplf]?\b"
    # a half is a window like a quarter, and without it H1 and H2 both fall back to
    # the bare year, which reads one metric as contradicting itself
    r"|\bh[12](?:\s?(?:fy)?" + YEAR + r")?[aeplf]?\b"
    r"|\b[12]h(?:\s?(?:fy)?" + YEAR + r")?[aeplf]?\b"
    r"|\b(?:ltm|ntm|ytd|mrq|cagr)\b"
    r"|\b(?:base|bull|bear|downside|upside|management|street)\s+case\b"
    r"|\b(?:base|bull|bear|downside|upside)\b"
    r"|\b(?:19|20)\d{2}\b",
    re.IGNORECASE,
)
FISCAL_RE = re.compile(
    r"\b(?:fy|cy|fiscal|calendar|year|years|yr|ytd|ltm|ntm|quarter|period|ended|ending|as of|as at)\b",
    re.IGNORECASE,
)
HINT_RE = re.compile(r"\(([^)]{1,14})\)|\bin\s+(millions|billions|thousands)\b", re.IGNORECASE)
SOURCE_RE = re.compile(
    r"\bsources?\b|\bfootnotes?\b|\bnotes?\s*:|\bas (?:of|at)\b|\bper\s+(?:company|management|filings)\b",
    re.IGNORECASE,
)
FOOTER_RE = re.compile(r"footer|slide\s*number|page\s*number|pgnum", re.IGNORECASE)
DATE_RE = re.compile(r"\b\d{1,4}[/-]\d{1,2}[/-]\d{2,4}\b")
CELL_RE = re.compile(r"^(.*) r\d+c\d+$")
RANGE_RE = re.compile(r"\s*(?:[-\u2013]|to|through|and)\s*", re.IGNORECASE)
SYMBOL = {"percent": "%", "bps": "bps", "multiple": "x"}
STOPWORDS = {"of", "the", "in", "at", "to", "and", "for", "a", "an", "is", "was", "by", "on", "with", "from"}


class Finding:
    def __init__(self, check: str, level: str, message: str):
        self.check, self.level, self.message = check, level, message
        self.examples: list[str] = []
        self.count = 0

    def add(self, example: str) -> None:
        self.count += 1
        if len(self.examples) < EXAMPLES:
            self.examples.append(example)

    def as_dict(self) -> dict:
        return {"check": self.check, "level": self.level, "count": self.count,
                "message": self.message, "examples": self.examples}


def squash(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def nearest(pattern: re.Pattern, hay: str, pos: int, avoid: tuple[int, int] | None = None):
    """The match closest to pos, preferring the longer phrase on a tie.

    Proximity is the whole keying rule: the metric a number belongs to is the one
    written beside it, not the first one on the slide. `avoid` is the number's own
    span, so a figure is never read as its own period: the bare year alternative
    matches the digits of "2050 customers", which then keyed as FY2050.
    """
    best, best_gap = None, None
    for m in pattern.finditer(hay):
        if avoid is not None and m.start() < avoid[1] and avoid[0] < m.end():
            continue
        gap = 0 if m.start() <= pos <= m.end() else min(abs(pos - m.end()), abs(m.start() - pos))
        if best_gap is None or gap < best_gap or (gap == best_gap and len(m.group(0)) > len(best.group(0))):
            best, best_gap = m, gap
    return best if best is not None and best_gap <= WINDOW else None


def covering(pattern: re.Pattern, hay: str, span: tuple[int, int]):
    """The longest match the given span sits inside, which is the token the digits belong to."""
    hits = [m for m in pattern.finditer(hay) if m.start() <= span[0] and span[1] <= m.end()]
    return max(hits, key=lambda m: len(m.group(0))) if hits else None


def qualified_metric(hay: str, hit: re.Match) -> str:
    """The metric a family word belongs to, which is what the deck wrote in front of it.

    "margin", "growth" and "multiple" name a family, so on their own they read a
    deck's operating margin as a contradiction of its net margin. A period token, a
    stopword or any punctuation ends the run of qualifiers, which leaves "FY25
    margin" and "the margin" on the bare family word.
    """
    canon = METRIC_PHRASE[squash(hit.group(0)).lower()]
    if canon not in GENERIC_METRICS:
        return canon
    words, head = [], hay[: hit.start()]
    while len(words) < QUALIFIERS:
        found = QUALIFIER_RE.search(head)
        if found is None:
            break
        word = found.group(1).lower().strip(".")
        if len(word) < 2 or word in STOPWORDS or FISCAL_RE.fullmatch(word):
            break
        if covering(PERIOD_RE, hay, found.span(1)):
            break  # "base case margin" and "LTM margin" name no member of the family
        words.append(word)
        head = head[: found.start(1)]
    for size in range(len(words), 0, -1):  # the longest run the deck itself names as a metric
        phrase = " ".join(reversed(words[:size]))
        named = METRIC_PHRASE.get(f"{phrase} {canon}") or METRIC_PHRASE.get(phrase)
        if named and named not in GENERIC_METRICS:
            return named if named.endswith(canon) else f"{named} {canon}"
    return f"{words[0]} {canon}" if words else canon


def touches(hit: re.Match, span: tuple[int, int]) -> bool:
    """A metric word written against the digits rather than anywhere on the line."""
    return min(abs(span[0] - hit.end()), abs(hit.start() - span[1])) <= TOUCHING


def normalise_period(raw: str) -> str:
    token = re.sub(r"[\s.'’]+", "", raw.lower())
    year_form = re.fullmatch(r"(fy|cy)?(\d{2}|\d{4})([aeplf]?)", token)
    if year_form:
        year = int(year_form.group(2))
        if year < 100:
            year += 2000 if year < 70 else 1900
        # a calendar year is the same window as a fiscal one only for a December year end
        return f"{year_form.group(1) or 'fy'}{year}{year_form.group(3)}"
    part = re.fullmatch(
        r"(?:q(?P<q>[1-4])|(?P<q2>[1-4])q|h(?P<h>[12])|(?P<h2>[12])h)"
        r"(?:fy)?(?P<year>\d{2}|\d{4})?(?P<mark>[aeplf]?)", token)
    if part:
        quarter = part.group("q") or part.group("q2")
        stamp = f"q{quarter}" if quarter else f"h{part.group('h') or part.group('h2')}"
        if part.group("year"):
            year = int(part.group("year"))
            stamp += f"fy{year + (2000 if year < 70 else 1900) if year < 100 else year}"
        return stamp + part.group("mark")
    return token


def unit_hint(hay: str, pos: int):
    """A column label like ($m) or (%) that tells a bare table figure what it is.

    The mark on the label is part of the answer: a column headed (€m) is euros, and
    reading it as a bare scale let its cells tie out against dollars elsewhere.
    """
    match = nearest(HINT_RE, hay, pos)
    if match is None:
        return None
    label = (match.group(1) or match.group(2) or "").lower()
    # "us" is bounded because it also sits inside "thousand", and stripping it there
    # left "thoand", which no scale answers to
    token = re.sub(r"[\s$€£¥]|\bus\b|\bin\b", "", label)
    if token in SCALE:
        word, size = SCALE[token]
        return "currency", word, size, next((c for c in label if c in "$€£¥"), "")
    if token in ("%", "percent", "pct"):
        return "percent", "", 1.0, ""
    if token == "x":
        return "multiple", "", 1.0, ""
    if token == "bps":
        return "bps", "", 1.0, ""
    return None


def date_adjacent(text: str, start: int, end: int) -> bool:
    """Part of a date rather than a figure.

    A hyphen alone cannot decide it, since "12.5%-19.6%" is a range, so a hyphen
    only counts inside a whole date while a slash beside a number always counts.
    """
    if (text[start - 1] if start else "") == "/" or text[end : end + 1] == "/":
        return True
    return any(m.start() <= start and end <= m.end() for m in DATE_RE.finditer(text))


def leading_phrase(hay: str, pos: int) -> str:
    words = re.findall(r"[A-Za-z][A-Za-z.'/-]*", hay[:pos])[-3:]
    if not words or not any(w.lower() not in STOPWORDS and len(w) > 2 for w in words):
        return ""
    phrase = " ".join(w.lower().strip(".") for w in words)
    return phrase if len(phrase) >= 6 else ""


def read_number(hay: str, match: re.Match, slide: int, footer: bool) -> dict | None:
    """One number with the unit, metric and period the deck wrote around it."""
    start, end = match.start("num"), match.end()
    raw = hay[match.start() : end].strip()
    pre = match.group("pre") or ""
    lead = hay[start - 1] if start else ""
    before = hay[match.start() - 1] if match.start() else ""
    currency = next((c for c in pre if c in "$€£¥"), "")
    if lead.isalpha() and not currency:  # the 24 inside FY24, the 1 inside 1H25
        return None
    if date_adjacent(hay, match.start(), end):
        return None
    suffix = squash(match.group("suffix") or "").lower()
    value = float(match.group("num").replace(",", ""))
    scale_label, factor = SCALE.get(suffix, ("", 1.0))
    if suffix == "%":
        unit_class = "percent"
    elif suffix.startswith(("bps", "bp", "basis")):
        unit_class = "bps"
    elif suffix == "x":
        unit_class = "multiple"
    elif currency:
        unit_class = "currency"
    elif suffix:
        unit_class = "count"
    else:
        unit_class = ""
    if unit_class:
        hinted = False
    else:  # a bare table cell or chart point, read through the label beside it
        hint = unit_hint(hay, start)
        unit_class, scale_label, factor, currency = hint or ("count", "", 1.0, "")
        hinted = hint is not None
    value *= factor
    integral = value.is_integer()
    is_year = integral and 1900 <= value <= 2100 and not currency and not suffix
    if is_year and not (FISCAL_RE.search(hay) or METRIC_RE.search(hay)):
        return None
    if integral and value <= 200 and squash(hay) == raw and (footer or value == slide):
        return None  # a slide number standing alone in its own box
    metric_hit = nearest(METRIC_RE, hay, start)
    metric = qualified_metric(hay, metric_hit) if metric_hit else ""
    if unit_class == "count" and suffix in SCALE and metric in MONETARY_METRICS:
        unit_class = "currency"  # a deck drops the $ once the column header carries it
    elif unit_class == "count" and not suffix and "." in match.group("num") and metric in PER_SHARE_METRICS:
        unit_class = "currency"  # a share price is money with or without the mark
    digits = match.span("num")
    label = covering(PERIOD_RE, hay, digits)
    if label is not None:
        if label.start() < digits[0] or label.end() > digits[1]:
            return None  # the digits are part of a period token (FY2024, Q1 2024), not a figure
        if is_year and not (metric in COUNT_METRICS and touches(metric_hit, digits)):
            return None  # a year printed as a column header or standing in prose
    period_hit = nearest(PERIOD_RE, hay, start, avoid=digits)
    period = normalise_period(period_hit.group(0)) if period_hit else ""
    if before and (before.isdigit() or before in "%)x"):  # the far end of a range, not a minus
        raw = raw.lstrip("-+ ")
    elif "(" in pre and (match.group("close") or hay[end : end + 2].lstrip()[:1] == ")"):
        value = -value  # accounting parentheses
        raw = raw if match.group("close") else hay[match.start() : hay.index(")", end) + 1]
    elif "-" in pre:
        value = -value
    if metric:
        key = f"{metric}|{period or 'none'}"
    else:
        phrase = leading_phrase(hay, start)
        key = f"~{phrase}|{period or 'none'}" if phrase else "unknown"
    return {"raw": raw, "value": value, "unit_class": unit_class, "scale": scale_label, "hinted": hinted,
            "currency": currency, "metric": metric, "period": period, "key": key,
            "context": squash(hay[max(0, start - 60) : end + 60])}


def mark_ranges(hay: str, found: list[tuple]) -> None:
    """Two figures joined by a dash or a "to" are one range, so neither contradicts the other."""
    for (left, low), (right, high) in zip(found, found[1:]):
        # the slice ends at the second endpoint's digits, so that endpoint's own
        # currency mark rides in front of them and would otherwise hide the dash
        gap = hay[left.end() : right.start("num")].rstrip("$€£¥ ")
        # the second endpoint's sign rides in front of its digits too, and is the same
        # character as the dash that joins them, so "-5% to -10%" leaves "to -" here;
        # only a gap that does not already read as a separator gives up its last sign
        if gap[-1:] in "-+(" and not RANGE_RE.fullmatch(gap):
            gap = gap[:-1].rstrip("$€£¥ ")
        if low["unit_class"] == high["unit_class"] and RANGE_RE.fullmatch(gap):
            low["key"] = high["key"] = "unknown"


def scan(slides: list[dict]) -> list[dict]:
    numbers = []
    for slide in slides:
        for unit in slide["units"]:
            hay = f"{unit['before']} ~ {unit['text']}" if unit["before"] else unit["text"]
            offset = len(hay) - len(unit["text"])
            found = []
            for match in NUMBER_RE.finditer(hay):
                if match.start("num") < offset:
                    continue  # the label, not the figure it labels
                record = read_number(hay, match, slide["index"], unit["footer"])
                if record:
                    found.append((match, record))
            mark_ranges(hay, found)
            for _, record in found:
                if unit.get("axis"):
                    record["key"] = "unknown"  # an XY point has no category to key it to a claim
            numbers += [{"slide": slide["index"], "location": unit["loc"], **r} for _, r in found]
    return numbers


def flatten(shapes: list[dict]):
    """Every shape the slide draws, group members included.

    A shape extract.py marks hidden contributes no line to the slide text, so
    walking it here would spend its character budget on the lines of the shapes
    after it and name the wrong box behind every number that follows.
    """
    for shape in shapes:
        if shape.get("hidden"):  # a hidden group takes its members with it
            continue
        if shape.get("kind") == "group":
            yield from flatten(shape.get("members", []))
        else:
            yield shape


def plain(value: float) -> str:
    """A chart point written out in full, because ":g" turns 1e9 into "1e+09" and the scanner reads that as 9."""
    text = f"{Decimal(f'{value:.12g}'):f}"
    return text.rstrip("0").rstrip(".") if "." in text else text


def slides_from_extract(report: dict) -> list[dict]:
    """Rebuild shape attribution that extract.py's flat text list drops.

    It emits slide text shape by shape in the order it lists the shapes, so
    walking the two in step names the box each line came from. The character
    budget is a heuristic: worst case a line lands on the neighbouring shape and
    only the location label is wrong.
    """
    height = (report.get("slide_size_in") or [0, 7.5])[1] or 7.5
    out = []
    for slide in report.get("slides", []):
        # a slide can draw two tables under one name, a copy of the first or a
        # generator's default, and keying on the name alone kept only the last: every
        # table shape then dropped that many lines and ate the boxes between them
        tables: dict[str, list[dict]] = {}
        for table in slide.get("tables", []):
            tables.setdefault(table["shape"] or "shape", []).append(table)
        lines = list(slide.get("text", []))
        # the blob is what a reader can see: a source line in the speaker notes is
        # not a citation on the slide, and counting it hid unsourced pages
        units, blob = [], list(lines)
        for shape in flatten(slide.get("shapes", [])):
            name = shape.get("name") or "shape"
            if shape.get("kind") == "table":
                queue = tables.get(name) or []
                table = queue.pop(0) if queue else None
                for _ in range(table["rows"] if table else 0):
                    if lines:
                        lines.pop(0)  # table prose is read from the cells instead
                continue
            budget = shape.get("chars") or 0
            footer = FOOTER_RE.search(f"{name} {shape.get('placeholder', '')}") is not None or (
                (shape.get("top") or 0) > height * 0.85
            )
            while lines and budget >= len(lines[0]):
                line = lines.pop(0)
                budget -= len(line) + 1
                units.append({"loc": name, "text": line, "before": "", "footer": footer})
        for index, line in enumerate(lines):  # anything the walk could not place
            units.append({"loc": f"text line {index + 1}", "text": line, "before": "", "footer": False})
        drawn: dict[str, int] = {}
        for table in slide.get("tables", []):
            cells = table.get("cells", [])
            # a repeated name is numbered from its second use, so two tables never
            # report one location, which read their cells as a single sensitivity grid
            drawn[table["shape"]] = drawn.get(table["shape"], 0) + 1
            seen = drawn[table["shape"]]
            shape_name = table["shape"] if seen == 1 else f"{table['shape']} ({seen})"
            for r, row in enumerate(cells):
                for c, cell in enumerate(row):
                    label = cells[r][0] if c else ""
                    header = cells[0][c] if r and cells else ""
                    loc = f"{shape_name} r{r + 1}c{c + 1}"
                    units.append({"loc": loc, "text": cell, "before": squash(f"{label} {header}"), "footer": False})
                    blob.append(cell)
        for chart in slide.get("charts", []):
            categories = chart.get("categories") or []
            for series in chart.get("series", []):
                for field, axis in CHART_POINTS:
                    for index, value in enumerate(series.get(field) or []):
                        if value is None:
                            continue
                        category = categories[index] if index < len(categories) else ""
                        name = series.get("name") or index + 1
                        units.append({"loc": f"{chart['shape']} series {name}{axis} at {category or index + 1}",
                                      "text": plain(value), "before": squash(f"{name} {category}"),
                                      "footer": False, "axis": axis})
        out.append({"index": slide.get("index", len(out) + 1), "units": units,
                    "blob": " ".join(blob), "notes": slide.get("notes", "")})
    return out


def slides_from_text(content: str) -> list[dict]:
    marker = re.compile(r"^\s*(?:#+\s*slide|-{2,}\s*slide)\s*(\d+)", re.IGNORECASE)
    slides, current = [], None
    for line in content.splitlines():
        hit = marker.match(line)
        if hit:
            current = {"index": int(hit.group(1)), "units": [], "blob": "", "notes": ""}
            slides.append(current)
            continue
        if current is None:
            current = {"index": 1, "units": [], "blob": "", "notes": ""}
            slides.append(current)
        text = line.strip().lstrip("#-*| ").strip()
        if text:
            current["units"].append({"loc": "text", "text": text, "before": "", "footer": False})
            current["blob"] += " " + text
    return slides


def load_extractor():
    here = Path(__file__).resolve()
    relative = {4: "langalpha_deliverables/skills/pptx/scripts", 2: "pptx/scripts"}
    roots = [here.parents[up] / tail for up, tail in relative.items() if len(here.parents) > up]
    roots += [Path.cwd() / ".agents/skills/pptx/scripts", Path.cwd() / "skills/pptx/scripts"]
    tried = []
    for root in roots:
        tried.append(str(root))
        target = root / "extract.py"
        if target.exists():
            spec = importlib.util.spec_from_file_location("pptx_extract", target)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            return module
    raise FileNotFoundError(
        "the pptx skill's extract.py was not found in " + ", ".join(tried)
        + "; run it yourself and pass its JSON instead"
    )


def load(path: Path) -> tuple[list[dict], str]:
    if path.suffix.lower() == ".pptx":
        return slides_from_extract(load_extractor().extract(path, None, False)), "pptx"
    if path.suffix.lower() == ".json":
        report = json.loads(path.read_text())
        if not isinstance(report, dict) or "slides" not in report:
            raise ValueError("not an extract.py report: no slides key")
        return slides_from_extract(report), "extract-json"
    return slides_from_text(path.read_text()), "text"


def human(value: float, unit_class: str, mark: str = "") -> str:
    if unit_class == "currency":
        for label, size in (("tn", 1e12), ("bn", 1e9), ("m", 1e6), ("k", 1e3)):
            if abs(value) >= size:
                return f"{mark}{value / size:g}{label}"
    return f"{mark}{value:g}{SYMBOL.get(unit_class, '')}"


def trail_of(members: list[dict], unit_class: str) -> str:
    return "; ".join(f"{human(m['value'], unit_class, m['currency'])} on slide {m['slide']} ({m['location']})"
                     for m in members[:6])


def sensitivity_axis(members: list[dict]) -> bool:
    """Three or more different values in one table are an axis, not a repeated claim.

    A sensitivity grid writes its ladder along the top and down the side, which
    otherwise reads as one metric disagreeing with itself once per column.
    """
    cells = [CELL_RE.match(m["location"]) for m in members]
    return all(cells) and len({c.group(1) for c in cells}) == 1 and len({m["value"] for m in members}) >= 3


def cluster(values: list[float], tolerance: float) -> list[list[float]]:
    groups: list[list[float]] = []
    for value in sorted(values):
        if groups:
            head = groups[-1][0]
            spread = abs(value - head) / max(abs(value), abs(head)) if max(abs(value), abs(head)) else 0.0
            if spread <= tolerance:
                groups[-1].append(value)
                continue
        groups.append([value])
    return groups


def analyse(slides: list[dict], numbers: list[dict]) -> list[dict]:
    findings: dict[str, Finding] = {}

    def note(check: str, level: str, message: str) -> Finding:
        if check not in findings:
            findings[check] = Finding(check, level, message)
        return findings[check]

    groups: dict[tuple[str, str], list[dict]] = {}
    for number in numbers:
        if number["key"] != "unknown":
            groups.setdefault((number["key"], number["unit_class"]), []).append(number)
    for (key, unit_class), members in sorted(groups.items()):
        marks = sorted({m["currency"] for m in members if m["currency"]})
        if len(marks) > 1:
            note("currency_mixing", "warn", "one metric and period stated in two currencies; say which one the deck reports in and translate the other").add(f"{key}: {trail_of(members, unit_class)}")
        # two currencies are two amounts, so they are never weighed against each other;
        # an unmarked figure belongs to whichever one it is written beside, so it stays
        # in both parts rather than standing as a claim of its own
        parts = [[m for m in members if m["currency"] in (mark, "")] for mark in marks] if len(marks) > 1 else [members]
        for part in parts:
            if len(cluster([m["value"] for m in part], TOLERANCE.get(unit_class, DEFAULT_TOLERANCE))) < 2:
                continue
            if sensitivity_axis(part):
                continue
            example = f"{key} [{unit_class}]: {trail_of(part, unit_class)}"
            if key.startswith("~"):
                note("phrase_conflict", "warn", "the same wording introduces two different values; confirm they are different things").add(example)
            else:
                note("value_conflict", "fail", "one metric and period carrying two different values; reconcile to a single figure").add(example)

    per_slide: dict[tuple[int, str], dict[str, set]] = {}
    for number in numbers:
        if not number["metric"]:
            continue
        seen = per_slide.setdefault((number["slide"], number["metric"]), {"scales": set(), "classes": set()})
        # only a scale the deck writes beside the figure counts as a scale it chose
        if number["scale"] and number["unit_class"] == "currency" and not number["hinted"]:
            seen["scales"].add(number["scale"])
        if number["unit_class"] in ("percent", "bps"):
            seen["classes"].add(number["unit_class"])
    for (slide_index, metric), seen in sorted(per_slide.items()):
        mixed = sorted(seen["scales"]) if len(seen["scales"]) > 1 else sorted(seen["classes"])
        if len(mixed) > 1:
            note("unit_mixing", "warn", "one metric written at two scales on a single slide; pick one and restate the other").add(f"slide {slide_index}: {metric} in {' and '.join(mixed)}")

    counted: dict[int, int] = {}
    for number in numbers:
        counted[number["slide"]] = counted.get(number["slide"], 0) + 1
    for slide in slides:
        index = slide["index"]
        if counted.get(index, 0) < DENSE_SLIDE or SOURCE_RE.search(slide["blob"]):
            continue
        note("source_missing", "warn", "a slide of figures with no source, footnote or as-of line; cite where the numbers came from").add(f"slide {index}: {counted[index]} numbers, no source line")

    out = [f.as_dict() for f in findings.values()]
    out.sort(key=lambda d: ({"fail": 0, "warn": 1, "info": 2}[d["level"]], d["check"]))
    return out


USAGE = "usage: extract_numbers.py <deck.json|deck.pptx|content.md> [--check]"
TAKES_VALUE = {"--check": False}


def fail(message: str) -> None:
    print(json.dumps({"status": "error", "message": message}))
    sys.exit(1)


def parse_args(argv: list[str]) -> tuple[list[str], dict[str, str | bool]]:
    """Positional arguments and flags, refusing any token TAKES_VALUE does not name.

    A misspelt flag used to be dropped with the rest of the switches, so `--chek`
    printed a report full of value conflicts and exited 0 on the deck it was gating.
    """
    args: list[str] = []
    flags: dict[str, str | bool] = {}
    i = 0
    while i < len(argv):
        token = argv[i]
        if not token.startswith("--"):
            args.append(token)
            i += 1
        elif token not in TAKES_VALUE:
            fail(f"unknown option: {token}; {USAGE}")
        elif token in flags:
            fail(f"repeated option: {token}; {USAGE}")
        elif not TAKES_VALUE[token]:
            flags[token] = True
            i += 1
        elif i + 1 >= len(argv) or argv[i + 1].startswith("--"):
            fail(f"{token} requires a value; {USAGE}")
        else:
            flags[token] = argv[i + 1]
            i += 2
    return args, flags


def main(argv: list[str]) -> None:
    if "-h" in argv or "--help" in argv:
        print(__doc__.strip())
        sys.exit(0)
    args, flags = parse_args(argv)
    if not args:
        fail(USAGE)
    path = Path(args[0]).expanduser().resolve()
    if not path.exists():
        fail(f"no such file: {path}")
    try:
        slides, source = load(path)
    except Exception as exc:  # a bad path or a deck python-pptx cannot open
        print(json.dumps({"status": "error", "file": str(path), "message": f"{type(exc).__name__}: {exc}"}))
        sys.exit(1)
    numbers = scan(slides)
    findings = analyse(slides, numbers)
    report = {
        "status": "fail" if any(f["level"] == "fail" for f in findings) else "pass",
        "file": str(path),
        "stats": {"slides": len(slides), "numbers": len(numbers),
                  "keys": len({n["key"] for n in numbers if n["key"] != "unknown"}), "read_as": source},
        "numbers": numbers,
        "findings": findings,
    }
    print(json.dumps(report, indent=2, ensure_ascii=False))
    if "--check" in flags and report["status"] == "fail":
        sys.exit(1)


if __name__ == "__main__":
    main(sys.argv[1:])
