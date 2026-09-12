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
    unit_mixing      one metric written at two scales on a single slide
    source_missing   a number heavy slide with no source, footnote or as-of line

Every number is keyed by the metric word and the period token nearest to it, and
two numbers are compared only when both the key and the unit class (percent,
bps, multiple, currency, count) match. That is what keeps an FY25E forecast from
reading as a contradiction of the FY24A actual, and $1,200m from reading as a
contradiction of $1.2bn. Each entry in the numbers array carries its key, slide
and location, so a report can list every occurrence behind a finding.

Findings carry a level: "fail" blocks delivery, "warn" is a judgement call.
With --check the exit code is 1 when any fail exists.
"""

from __future__ import annotations

import importlib.util
import json
import re
import sys
from pathlib import Path

EXAMPLES = 25
DENSE_SLIDE = 5  # numbers on one slide before it owes the reader a source
WINDOW = 90  # characters a metric or period word may sit from its number
TOLERANCE = {"percent": 0.005, "bps": 0.005, "multiple": 0.005}
DEFAULT_TOLERANCE = 0.01

SCALE = {name: (word, size) for word, size, spellings in (
    ("thousand", 1e3, "k thousand"), ("million", 1e6, "m mm mn million"),
    ("billion", 1e9, "b bn billion"), ("trillion", 1e12, "t tn trillion"),
) for name in spellings.split()}

NUMBER_RE = re.compile(
    r"(?P<pre>(?:[$€£¥(+-]\s?){0,3})"
    r"(?P<num>\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d+(?:\.\d+)?|\.\d+)"
    r"(?P<close>\s?\))?"
    r"\s?(?P<suffix>%|bps|bp|basis\s+points?|mm|mn|bn|tn|thousand|million|billion|trillion|[kmbtx])?"
    r"(?![A-Za-z0-9])",
    re.IGNORECASE,
)

METRICS = {
    "revenue": ["revenue", "revenues", "net sales", "sales", "top line", "topline"],
    "ebitda": ["ebitda", "adjusted ebitda", "adj. ebitda"],
    "ebitda margin": ["ebitda margin"],
    "ebit": ["ebit", "operating income", "operating profit"],
    "gross margin": ["gross margin"],
    "net income": ["net income", "net profit", "net earnings"],
    "eps": ["eps", "earnings per share"],
    "fcf": ["fcf", "free cash flow", "cash flow"],
    "margin": ["margin", "margins"],
    "growth": ["growth", "yoy", "y/y", "year over year", "year-over-year"],
    "cagr": ["cagr"],
    "irr": ["irr", "gross irr", "net irr"],
    "moic": ["moic", "money multiple", "cash on cash"],
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
METRIC_RE = re.compile(
    r"\b(?:" + "|".join(re.escape(p) for p in sorted(METRIC_PHRASE, key=len, reverse=True)) + r")\b",
    re.IGNORECASE,
)

PERIOD_RE = re.compile(
    r"\b(?:fy|cy)\s?\d{2,4}[aeplf]?\b"
    r"|\b(?:19|20)\d{2}[aep]\b"
    r"|\bq[1-4]\s?(?:fy)?\s?\d{0,4}\b"
    r"|\b[1-4]q\s?(?:fy)?\s?\d{0,4}\b"
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


def nearest(pattern: re.Pattern, hay: str, pos: int):
    """The match closest to pos, preferring the longer phrase on a tie.

    Proximity is the whole keying rule: the metric a number belongs to is the one
    written beside it, not the first one on the slide.
    """
    best, best_gap = None, None
    for m in pattern.finditer(hay):
        gap = 0 if m.start() <= pos <= m.end() else min(abs(pos - m.end()), abs(m.start() - pos))
        if best_gap is None or gap < best_gap or (gap == best_gap and len(m.group(0)) > len(best.group(0))):
            best, best_gap = m, gap
    return best if best is not None and best_gap <= WINDOW else None


def normalise_period(raw: str) -> str:
    token = re.sub(r"[\s.]+", "", raw.lower())
    year_form = re.fullmatch(r"(?:fy|cy)?(\d{2}|\d{4})([aeplf]?)", token)
    if year_form:
        year = int(year_form.group(1))
        if year < 100:
            year += 2000 if year < 70 else 1900
        return f"fy{year}{year_form.group(2)}"
    quarter = re.fullmatch(r"(?:q([1-4])|([1-4])q)(?:fy)?(\d{2}|\d{4})?", token)
    if quarter:
        stamp = f"q{quarter.group(1) or quarter.group(2)}"
        if quarter.group(3):
            year = int(quarter.group(3))
            return f"{stamp}fy{year + (2000 if year < 70 else 1900) if year < 100 else year}"
        return stamp
    return token


def unit_hint(hay: str, pos: int):
    """A column label like ($m) or (%) that tells a bare table figure what it is."""
    match = nearest(HINT_RE, hay, pos)
    if match is None:
        return None
    token = re.sub(r"[\s$€£¥]|us|\bin\b", "", (match.group(1) or match.group(2) or "").lower())
    if token in SCALE:
        return "currency", SCALE[token][0], SCALE[token][1]
    if token in ("%", "percent", "pct"):
        return "percent", "", 1.0
    if token == "x":
        return "multiple", "", 1.0
    if token == "bps":
        return "bps", "", 1.0
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
        unit_class, scale_label, factor = hint or ("count", "", 1.0)
        hinted = hint is not None
    value *= factor
    integral = value.is_integer()
    is_year = integral and 1900 <= value <= 2100 and not currency and not suffix
    if is_year and not (FISCAL_RE.search(hay) or METRIC_RE.search(hay)):
        return None
    if integral and value <= 200 and squash(hay) == raw and (footer or value == slide):
        return None  # a slide number standing alone in its own box
    metric_hit = nearest(METRIC_RE, hay, start)
    metric = METRIC_PHRASE[squash(metric_hit.group(0)).lower()] if metric_hit else ""
    period_hit = nearest(PERIOD_RE, hay, start)
    period = normalise_period(period_hit.group(0)) if period_hit else ""
    if is_year and period == f"fy{int(value)}":
        return None  # a year printed as its own column header, not a figure
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
            "metric": metric, "period": period, "key": key, "context": squash(hay[max(0, start - 60) : end + 60])}


def mark_ranges(hay: str, found: list[tuple]) -> None:
    """Two figures joined by a dash or a "to" are one range, so neither contradicts the other."""
    for (left, low), (right, high) in zip(found, found[1:]):
        if low["unit_class"] == high["unit_class"] and RANGE_RE.fullmatch(hay[left.end() : right.start("num")]):
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
            numbers += [{"slide": slide["index"], "location": unit["loc"], **r} for _, r in found]
    return numbers


def flatten(shapes: list[dict]):
    for shape in shapes:
        if shape.get("kind") == "group":
            yield from flatten(shape.get("members", []))
        else:
            yield shape


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
        tables = {t["shape"]: t for t in slide.get("tables", [])}
        lines = list(slide.get("text", []))
        units, blob = [], list(lines) + [slide.get("notes", "")]
        for shape in flatten(slide.get("shapes", [])):
            name = shape.get("name") or "shape"
            if shape.get("kind") == "table":
                table = tables.get(name)
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
        for table in slide.get("tables", []):
            cells = table.get("cells", [])
            for r, row in enumerate(cells):
                for c, cell in enumerate(row):
                    label = cells[r][0] if c else ""
                    header = cells[0][c] if r and cells else ""
                    loc = f"{table['shape']} r{r + 1}c{c + 1}"
                    units.append({"loc": loc, "text": cell, "before": squash(f"{label} {header}"), "footer": False})
                    blob.append(cell)
        for chart in slide.get("charts", []):
            categories = chart.get("categories") or []
            for series in chart.get("series", []):
                for index, value in enumerate(series.get("values") or []):
                    if value is None:
                        continue
                    category = categories[index] if index < len(categories) else ""
                    name = series.get("name") or index + 1
                    units.append({"loc": f"{chart['shape']} series {name} at {category or index + 1}",
                                  "text": f"{value:g}", "before": squash(f"{name} {category}"), "footer": False})
        out.append({"index": slide.get("index", len(out) + 1), "units": units, "blob": " ".join(blob)})
    return out


def slides_from_text(content: str) -> list[dict]:
    marker = re.compile(r"^\s*(?:#+\s*slide|-{2,}\s*slide)\s*(\d+)", re.IGNORECASE)
    slides, current = [], None
    for line in content.splitlines():
        hit = marker.match(line)
        if hit:
            current = {"index": int(hit.group(1)), "units": [], "blob": ""}
            slides.append(current)
            continue
        if current is None:
            current = {"index": 1, "units": [], "blob": ""}
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


def human(value: float, unit_class: str) -> str:
    if unit_class == "currency":
        for label, size in (("tn", 1e12), ("bn", 1e9), ("m", 1e6), ("k", 1e3)):
            if abs(value) >= size:
                return f"{value / size:g}{label}"
    return f"{value:g}{SYMBOL.get(unit_class, '')}"


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
        if len(cluster([m["value"] for m in members], TOLERANCE.get(unit_class, DEFAULT_TOLERANCE))) < 2:
            continue
        if sensitivity_axis(members):
            continue
        trail = "; ".join(f"{human(m['value'], unit_class)} on slide {m['slide']} ({m['location']})" for m in members[:6])
        example = f"{key} [{unit_class}]: {trail}"
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


def main(argv: list[str]) -> None:
    if "-h" in argv or "--help" in argv:
        print(__doc__.strip())
        sys.exit(0)
    args = [a for a in argv if not a.startswith("-")]
    if not args:
        print(json.dumps({"status": "error", "message": "usage: extract_numbers.py <deck.json|deck.pptx|content.md> [--check]"}))
        sys.exit(1)
    path = Path(args[0]).expanduser().resolve()
    if not path.exists():
        print(json.dumps({"status": "error", "message": f"no such file: {path}"}))
        sys.exit(1)
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
    if "--check" in argv and report["status"] == "fail":
        sys.exit(1)


if __name__ == "__main__":
    main(sys.argv[1:])
