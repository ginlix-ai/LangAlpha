#!/usr/bin/env python3
"""Recalculate every formula in a workbook (IronCalc, else LibreOffice) and report formula errors.

Usage:
    python recalc.py <file.xlsx> [timeout_seconds] [--check-only] [--force]

openpyxl writes formulas without cached values, so a workbook it saves shows
blanks in any viewer that does not calculate and hides errors until a human
opens it in Excel. A native chart is the same story one level down: it carries
its own copy of the points it plots, and a viewer that does not recalculate
draws that copy rather than the cells. This script calculates a disposable copy,
then copies the computed values back into the original package and rebuilds each
chart's cached points from the cells it plots; every other part of the file
(styles, comments, validation, names, formula text, unknown parts) stays
byte-identical. The original is replaced only after the calculated values were
read back successfully.

Two engines. IronCalc (a Rust spreadsheet engine with Python bindings) runs in
milliseconds and is tried first; headless LibreOffice takes seconds and is the
fallback whenever IronCalc cannot load the file, raises, or answers #NAME? for a
function it does not implement or a bare #ERROR! for a construct it cannot
evaluate. The report says which engine produced the values.

Output is one JSON object on stdout:

    status          "success" (zero errors, every formula valued), "errors_found",
                    "incomplete" (the engine produced no value for some formula cells,
                    listed in unmatched; the source is left untouched unless --force),
                    or "error". A nonzero unmatched is "incomplete" even when some
                    valued cell errored, because the run did not finish; the errors
                    are reported either way
    total_formulas  formula cells in the workbook
    total_errors    cells whose computed value is an Excel error
    error_summary   {"#DIV/0!": n, "#REF!": n, ...}
    errors          [{"sheet", "cell", "error", "formula"}, ...] capped at 100
    truncated       true when the errors list was capped
    values_written  formula cells that received a computed value
    unmatched       formula cells the engine produced no value for
    charts_updated  chart parts whose cached points were rebuilt from the new values
    chart_refs_skipped  chart references left as they were because they do not name
                    plain cells in this workbook: external books, defined names,
                    whole columns, multi-level categories
    engine          "ironcalc" or "libreoffice"

Exit code is 1 only for status "error" (file missing, LibreOffice failed,
timeout, external links without --force). "errors_found" exits 0 so a caller
can parse the report and fix the cells.

--check-only leaves the input untouched and reports from the calculated copy.
--force recalculates even when the workbook links to external files, which
LibreOffice cannot resolve here and will turn into #REF! or #NAME? errors. A
link counts whether the package carries an externalLinks part, a formula names
another book itself (=[rates.xlsx]Curve!B2), or a defined name names it on the
formula's behalf (ExternalRate = '[rates.xlsx]Curve'!$B$2, read from a cell as
=ExternalRate); the last two are the only form a workbook written cell by cell
has. The refusal names the cells in external_cells and the names in
external_names.
"""

from __future__ import annotations

import json
import re
import os
import posixpath
import shutil
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path

from lxml import etree
from openpyxl import load_workbook
from openpyxl.formula.tokenizer import Tokenizer
from openpyxl.utils.cell import column_index_from_string, get_column_letter

EXCEL_ERRORS = ("#DIV/0!", "#REF!", "#NAME?", "#VALUE!", "#N/A", "#NUM!", "#NULL!", "#ERROR!", "#SPILL!", "#CALC!")
# IronCalc answers a construct it cannot evaluate with a bare #ERROR!, which is not an
# Excel error and so was never counted; any #TOKEN! or #TOKEN? string is an error.
ERROR_SHAPE = re.compile(r"^#(?:N/A|[A-Z0-9/]+[!?])$")
MAX_LISTED = 100
# A workbook arrives from outside, and an entity declaration in one of its parts is a
# directive to the parser rather than data: resolving it lets the file decide what the
# tree contains. Every part below is parsed with this, never the default parser.
XML = etree.XMLParser(resolve_entities=False, no_network=True)
SML = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
REL_ID = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"
CH = "{http://schemas.openxmlformats.org/drawingml/2006/chart}"
FLAGS = ("--check-only", "--force")
# A chart reference is "Sheet!$A$1:$B$2", the sheet name quoted when it holds a space and
# '' a literal quote inside those quotes. Anything else is deliberately not matched.
CHART_REF = re.compile(r"^(?:'((?:[^']|'')+)'|([^'!\[\]]+))!\$?([A-Z]{1,3})\$?([0-9]+)(?::\$?([A-Z]{1,3})\$?([0-9]+))?$")
# Excel plots at most 32000 points per series, so a wider range is a reference misread.
MAX_POINTS = 32000

# LibreOffice trusts the cached values in an Excel file unless told otherwise,
# so a workbook whose inputs were edited by a program that did not calculate
# would come back unchanged. Recalc mode 0 is "always recalculate on load".
RECALC_PROFILE = """<?xml version="1.0" encoding="UTF-8"?>
<oor:items xmlns:oor="http://openoffice.org/2001/registry"
 xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
<item oor:path="/org.openoffice.Office.Calc/Formula/Load"><prop oor:name="OOXMLRecalcMode" oor:op="fuse"><value>0</value></prop></item>
<item oor:path="/org.openoffice.Office.Calc/Formula/Load"><prop oor:name="ODFRecalcMode" oor:op="fuse"><value>0</value></prop></item>
</oor:items>
"""


def fail(message: str, **extra) -> None:
    print(json.dumps({"status": "error", "message": message, **extra}, indent=2))
    sys.exit(1)


def soffice_calculate(src: Path, out_dir: Path, timeout: int) -> Path:
    """Convert xlsx to xlsx through LibreOffice with a private profile that forces recalculation."""
    out_dir.mkdir(parents=True, exist_ok=True)
    profile = Path(tempfile.mkdtemp(prefix="lo_profile_"))
    (profile / "user").mkdir()
    (profile / "user" / "registrymodifications.xcu").write_text(RECALC_PROFILE, encoding="utf-8")
    cmd = [
        "soffice",
        f"-env:UserInstallation=file://{profile}",
        "--headless",
        "--norestore",
        "--nologo",
        "--convert-to",
        "xlsx:Calc MS Excel 2007 XML",
        "--outdir",
        str(out_dir),
        str(src),
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        fail(f"LibreOffice timed out after {timeout}s; retry with a longer timeout")
    finally:
        shutil.rmtree(profile, ignore_errors=True)
    produced = out_dir / (src.stem + ".xlsx")
    if proc.returncode != 0 or not produced.exists():
        fail("LibreOffice conversion failed", stderr=proc.stderr.strip()[-2000:], stdout=proc.stdout.strip()[-500:])
    return produced


def collect_formulas(path: Path) -> tuple[dict[tuple[str, str], str], list[tuple[str, str, str]], bool]:
    wb = load_workbook(path, data_only=False)
    formulas: dict[tuple[str, str], str] = {}
    for ws in wb.worksheets:
        for row in ws.iter_rows():
            for cell in row:
                if cell.data_type == "f":
                    formulas[(ws.title, cell.coordinate)] = str(cell.value)
    # openpyxl files workbook-scoped names on the workbook and sheet-scoped ones on the
    # sheet, so a scan that reads only wb.defined_names sees half of them.
    names = [(name, "workbook", str(dn.value or "")) for name, dn in wb.defined_names.items()]
    for ws in wb.worksheets:
        names += [(name, ws.title, str(dn.value or "")) for name, dn in getattr(ws, "defined_names", {}).items()]
    return formulas, names, bool(getattr(wb, "_external_links", None))


def external_operand(text: str) -> str | None:
    """The first operand naming another workbook, as `[Book.xlsx]Sheet!A1`, or None.

    Read through the tokenizer rather than over the text: a `[` inside a string
    literal is prose, and a `Table[Column]` operand names no book at all.
    """
    try:
        tokens = Tokenizer(text if text.startswith("=") else "=" + text).items
    except Exception:
        return None  # an unparseable formula is the recalculation's to report, not this gate's
    for token in tokens:
        if token.type != "OPERAND" or token.subtype != "RANGE" or "!" not in token.value:
            continue
        if "[" in token.value.rsplit("!", 1)[0]:
            return token.value
    return None


def external_references(formulas: dict[tuple[str, str], str]) -> list[dict[str, str]]:
    """Formula cells whose operands name another workbook, as [Book.xlsx]Sheet!A1.

    openpyxl's `_external_links` reports only an xl/externalLinks part, which a
    workbook assembled cell by cell never grows, so on a generated model the
    formulas and the defined names are the only record that the link is there.
    """
    found: list[dict[str, str]] = []
    for (sheet, coord), text in formulas.items():
        reference = external_operand(text)
        if reference:
            found.append({"sheet": sheet, "cell": coord, "reference": reference})
    return found


def external_names(names: list[tuple[str, str, str]]) -> list[dict[str, str]]:
    """Defined names whose expression points into another workbook.

    A cell reading `=ExternalRate` carries no `[` of its own, so the link sits one
    level down in the name and the scan over formula operands walks past it; the
    recalculation then resolves the name to nothing and caches the error.
    """
    found: list[dict[str, str]] = []
    for name, scope, expression in names:
        if external_operand(expression):
            found.append({"name": name, "scope": scope, "expression": expression})
    return found


def sheet_parts(payload: dict[str, bytes]) -> dict[str, str]:
    """Map sheet name to its worksheet part path inside the package."""
    wb_xml = etree.fromstring(payload["xl/workbook.xml"], XML)
    rels = etree.fromstring(payload["xl/_rels/workbook.xml.rels"], XML)
    targets = {r.get("Id"): r.get("Target") for r in rels}
    parts = {}
    for sheet in wb_xml.iter(f"{{{SML}}}sheet"):
        target = targets.get(sheet.get(REL_ID), "")
        part = target.lstrip("/") if target.startswith("/") else posixpath.normpath(posixpath.join("xl", target))
        parts[sheet.get("name")] = part
    return parts


def read_package(path: Path) -> tuple[list[zipfile.ZipInfo], dict[str, bytes]]:
    with zipfile.ZipFile(path) as z:
        infos = z.infolist()
        return infos, {i.filename: z.read(i.filename) for i in infos}


def computed_values(payload: dict[str, bytes]) -> dict[str, dict[str, tuple[str | None, str | None]]]:
    """{sheet name: {A1: (type attr, value text)}} for every formula cell LibreOffice wrote."""
    out: dict[str, dict[str, tuple[str | None, str | None]]] = {}
    for name, part in sheet_parts(payload).items():
        if part not in payload:
            continue
        root = etree.fromstring(payload[part], XML)
        cells = {}
        for c in root.iter(f"{{{SML}}}c"):
            if c.find(f"{{{SML}}}f") is None:
                continue
            v = c.find(f"{{{SML}}}v")
            t = c.get("t")
            if v is None:
                text = None
            else:
                # A genuine empty-string result is an empty <v> under t="str",
                # whose text lxml reports as None just like an absent <v>.
                text = "" if v.text is None and t == "str" else v.text
            cells[c.get("r")] = (t, text)
        out[name] = cells
    return out


def prepare_for_ironcalc(path: Path) -> None:
    """Normalise the disposable copy into the package shape IronCalc's importer accepts.

    Two openpyxl habits trip it: absolute relationship targets (/xl/...), which
    it resolves relative to the owning part and reports as missing, and cell
    comments with empty text, on which it panics. Comments carry no values, so
    the copy simply loses its comment lists; the original is never touched.
    """
    infos, payload = read_package(path)
    changed = False
    for name in list(payload):
        if name.startswith("xl/comments/") and name.endswith(".xml"):
            root = etree.fromstring(payload[name], XML)
            for lst in root.findall(f"{{{SML}}}commentList"):
                if len(lst):
                    lst[:] = []
                    changed = True
            payload[name] = etree.tostring(root, xml_declaration=True, encoding="UTF-8", standalone=True)
            continue
        if not name.endswith(".rels"):
            continue
        root = etree.fromstring(payload[name], XML)
        base = posixpath.dirname(posixpath.dirname(name))  # part dir that owns the rels
        for rel in root:
            target = rel.get("Target", "")
            if target.startswith("/") and rel.get("TargetMode") != "External":
                rel.set("Target", posixpath.relpath(target.lstrip("/"), base or "."))
                changed = True
        payload[name] = etree.tostring(root, xml_declaration=True, encoding="UTF-8", standalone=True)
    if changed:
        tmp = path.with_name(path.name + ".tmp")
        with zipfile.ZipFile(tmp, "w") as zout:
            for info in infos:
                zout.writestr(info, payload[info.filename], compress_type=info.compress_type)
        os.replace(tmp, path)


IRONCALC_WORKER = r"""
import json, sys
from openpyxl.utils.cell import column_index_from_string, coordinate_from_string
import ironcalc
job = json.load(sys.stdin)
model = ironcalc.load_from_xlsx(job["path"], "en", "UTC")
model.evaluate()
index = {p["name"]: i for i, p in enumerate(model.get_worksheets_properties())}
out = {}
for sheet, ref in job["cells"]:
    if sheet not in index:
        raise SystemExit("sheet missing: " + sheet)
    col, row = coordinate_from_string(ref)
    s, r, c = index[sheet], row, column_index_from_string(col)
    kind = str(model.get_cell_type(s, r, c)).rsplit(".", 1)[-1]
    value = model.get_cell_value(s, r, c)
    if kind == "Number":
        cell = [None, str(int(value)) if float(value).is_integer() and abs(value) < 1e15 else repr(float(value))]
    elif kind == "LogicalValue":
        cell = ["b", "1" if value else "0"]
    elif kind == "ErrorValue":
        # #NAME? is a function IronCalc lacks and a bare #ERROR! a construct it cannot
        # evaluate; both are the engine's limit rather than the workbook's, so the cell
        # would otherwise cache an error LibreOffice can calculate.
        if value in ("#NAME?", "#ERROR!"):
            raise SystemExit("unsupported construct")  # let LibreOffice decide
        cell = ["e", str(value)]
    elif kind == "Text":
        cell = ["str", "" if value is None else str(value)]
    else:
        raise SystemExit("unhandled cell type " + kind)  # arrays and compound data
    out.setdefault(sheet, {})[ref] = cell
json.dump(out, sys.stdout)
"""


def ironcalc_values(work: Path, formulas: dict[tuple[str, str], str], timeout: int) -> dict[str, dict[str, tuple[str | None, str | None]]] | None:
    """Compute every formula cell with IronCalc; None when the engine is absent or declines the file.

    Runs in a child process on purpose: IronCalc is a Rust extension and a
    failed importer assertion surfaces as a panic, which is not an Exception
    and would otherwise abort the run before the LibreOffice fallback.
    """
    try:
        import ironcalc  # noqa: F401
    except ImportError:
        return None
    try:
        prepare_for_ironcalc(work)
        job = json.dumps({"path": str(work), "cells": list(formulas)})
        proc = subprocess.run(
            [sys.executable, "-c", IRONCALC_WORKER],
            input=job, capture_output=True, text=True, timeout=min(timeout, 120), check=False,
        )
        if proc.returncode != 0 or not proc.stdout.strip():
            return None
        raw = json.loads(proc.stdout)
        return {sheet: {ref: (t, v) for ref, (t, v) in cells.items()} for sheet, cells in raw.items()}
    except Exception:
        return None


def cell_index(payload: dict[str, bytes]) -> dict[str, dict[str, tuple[str, str]]]:
    """{sheet name: {A1: (kind, text)}} for every valued cell, shared strings resolved.

    A chart plots literal labels alongside calculated points, so this reads whole
    sheets rather than only the formula cells transfer_values just wrote.
    """
    shared: list[str] = []
    if "xl/sharedStrings.xml" in payload:
        sst = etree.fromstring(payload["xl/sharedStrings.xml"], XML)
        shared = ["".join(t.text or "" for t in si.iter(f"{{{SML}}}t")) for si in sst.findall(f"{{{SML}}}si")]
    out: dict[str, dict[str, tuple[str, str]]] = {}
    for name, part in sheet_parts(payload).items():
        if part not in payload:
            continue
        cells: dict[str, tuple[str, str]] = {}
        for c in etree.fromstring(payload[part], XML).iter(f"{{{SML}}}c"):
            t = c.get("t")
            if t == "inlineStr":
                inline = c.find(f"{{{SML}}}is")
                if inline is not None:
                    cells[c.get("r")] = ("s", "".join(x.text or "" for x in inline.iter(f"{{{SML}}}t")))
                continue
            v = c.find(f"{{{SML}}}v")
            if v is None:
                continue
            text = "" if v.text is None and t == "str" else v.text
            if text is None:
                continue
            if t == "s":
                cells[c.get("r")] = ("s", shared[int(text)] if int(text) < len(shared) else "")
            elif t == "str":
                cells[c.get("r")] = ("s", text)
            elif t in ("e", "b"):
                cells[c.get("r")] = (t, text)
            else:
                cells[c.get("r")] = ("n", text)
        out[name] = cells
    return out


def ref_cells(text: str | None, sheets: set[str]) -> tuple[str, list[str]] | None:
    """(sheet, [A1, ...]) in reading order for a chart reference into this workbook, else None."""
    m = CHART_REF.match((text or "").strip())
    if m is None:
        return None
    sheet = m.group(1).replace("''", "'") if m.group(1) else m.group(2)
    if sheet not in sheets:
        return None
    c1, r1 = column_index_from_string(m.group(3)), int(m.group(4))
    c2, r2 = (column_index_from_string(m.group(5)), int(m.group(6))) if m.group(5) else (c1, r1)
    c1, c2, r1, r2 = min(c1, c2), max(c1, c2), min(r1, r2), max(r1, r2)
    if (c2 - c1 + 1) * (r2 - r1 + 1) > MAX_POINTS:
        return None
    return sheet, [f"{get_column_letter(c)}{r}" for r in range(r1, r2 + 1) for c in range(c1, c2 + 1)]


def write_cache(ref, tag: str, total: int, points: list[tuple[int, str]]) -> bool:
    """Replace one cache's points in place; True when the element actually changed."""
    cache = ref.find(tag)
    if cache is None:
        if not points:
            return False  # nothing to say about this reference, so leave it exactly as it was
        cache = etree.Element(tag)
        ref.find(f"{CH}f").addnext(cache)  # order inside the ref: c:f, the cache, c:extLst
    before = etree.tostring(cache)
    for pt in cache.findall(f"{CH}pt"):
        cache.remove(pt)
    count = cache.find(f"{CH}ptCount")
    if count is None:
        count = etree.Element(f"{CH}ptCount")
        fmt = cache.find(f"{CH}formatCode")  # order inside the cache: formatCode?, ptCount?, pt*
        cache.insert(list(cache).index(fmt) + 1 if fmt is not None else 0, count)
    count.set("val", str(total))
    at = list(cache).index(count) + 1
    for offset, (idx, value) in enumerate(points):
        pt = etree.Element(f"{CH}pt")
        pt.set("idx", str(idx))
        etree.SubElement(pt, f"{CH}v").text = value
        cache.insert(at + offset, pt)
    return etree.tostring(cache) != before


def refresh_charts(payload: dict[str, bytes]) -> tuple[int, list[str]]:
    """Rebuild every chart's cached points from the recalculated cells its reference names.

    Without this a reader that does not recalculate draws the pre-edit series beside
    the new numbers, and an empty frame where the cache is missing altogether.
    Returns the parts changed and the references left as they were.
    """
    index = cell_index(payload)
    sheets = set(index)
    updated, skipped = 0, []
    for part in sorted(n for n in payload if n.startswith("xl/charts/") and n.endswith(".xml")):
        root = etree.fromstring(payload[part], XML)
        changed = False
        for ref in root.iter(f"{CH}numRef", f"{CH}strRef", f"{CH}multiLvlStrRef"):
            f = ref.find(f"{CH}f")
            # Multi-level categories cache one list per level; rebuilding that is not this fix.
            target = None if ref.tag == f"{CH}multiLvlStrRef" else ref_cells(f.text if f is not None else None, sheets)
            if target is None:
                if f is not None and f.text:
                    skipped.append(f.text)
                continue
            sheet, refs = target
            numeric = ref.tag == f"{CH}numRef"
            points = []
            for i, coord in enumerate(refs):
                got = index[sheet].get(coord)
                # A blank, an error or a label is not a plotted point; the cache leaves the gap.
                if got is None or (numeric and got[0] != "n"):
                    continue
                points.append((i, got[1]))
            changed |= write_cache(ref, f"{CH}numCache" if numeric else f"{CH}strCache", len(refs), points)
        if changed:
            fresh = etree.tostring(root, xml_declaration=True, encoding="UTF-8", standalone=True)
            if fresh != payload[part]:
                payload[part] = fresh
                updated += 1
    return updated, sorted(set(skipped))


def transfer_values(src: Path, dst: Path, values: dict[str, dict[str, tuple[str | None, str | None]]]) -> tuple[int, int, int, list[str]]:
    """Write LibreOffice's computed values into the original package's formula cells only."""
    infos, payload = read_package(src)
    written = unmatched = 0
    for name, part in sheet_parts(payload).items():
        if part not in payload:
            continue
        root = etree.fromstring(payload[part], XML)
        sheet_values = values.get(name, {})
        changed = False
        for c in root.iter(f"{{{SML}}}c"):
            f = c.find(f"{{{SML}}}f")
            if f is None:
                continue
            got = sheet_values.get(c.get("r"))
            if got is None or got[1] is None:
                unmatched += 1
                continue
            t, text = got
            v = c.find(f"{{{SML}}}v")
            if v is None:
                v = etree.Element(f"{{{SML}}}v")
                f.addnext(v)
            v.text = text
            if t in ("str", "e", "b"):
                c.set("t", t)
            elif "t" in c.attrib:
                del c.attrib["t"]
            written += 1
            changed = True
        if changed:
            payload[part] = etree.tostring(root, xml_declaration=True, encoding="UTF-8", standalone=True)
    charts_updated, chart_refs_skipped = refresh_charts(payload)
    tmp = dst.with_name(dst.name + ".tmp")
    with zipfile.ZipFile(tmp, "w") as zout:
        for info in infos:
            zout.writestr(info, payload[info.filename], compress_type=info.compress_type)
    os.replace(tmp, dst)
    return written, unmatched, charts_updated, chart_refs_skipped


def scan_errors(path: Path, formulas: dict[tuple[str, str], str]) -> list[dict]:
    wb = load_workbook(path, data_only=True)
    found = []
    for ws in wb.worksheets:
        for row in ws.iter_rows():
            for cell in row:
                if (ws.title, cell.coordinate) not in formulas:
                    continue  # a typed "#N/A" is text a provider wrote, not a calculation error
                if isinstance(cell.value, str) and (cell.value in EXCEL_ERRORS or ERROR_SHAPE.match(cell.value)):
                    found.append({"sheet": ws.title, "cell": cell.coordinate, "error": cell.value, "formula": formulas.get((ws.title, cell.coordinate), "")})
    return found


def recalculate(src: Path, timeout: int, flags: set[str]) -> dict:
    formulas, names, linked_part = collect_formulas(src)
    linked_cells = external_references(formulas)
    linked_names = external_names(names)
    has_external = linked_part or bool(linked_cells) or bool(linked_names)
    if has_external and "--force" not in flags:
        if linked_cells:
            where = f"First link: {linked_cells[0]['sheet']}!{linked_cells[0]['cell']} -> {linked_cells[0]['reference']}"
        elif linked_names:
            where = (f"First link: defined name {linked_names[0]['name']} ({linked_names[0]['scope']} scope)"
                     f" -> {linked_names[0]['expression']}")
        else:
            where = "The package carries an externalLinks part"
        fail(
            "workbook links to external files; LibreOffice cannot resolve them here. "
            f"{where}. "
            "Remove the links (paste the values, or reference a sheet in this workbook) "
            "or rerun with --force to accept #REF!/#NAME? in those cells.",
            external_links=True,
            external_cells=linked_cells[:MAX_LISTED],
            external_names=linked_names[:MAX_LISTED],
        )

    with tempfile.TemporaryDirectory(prefix="recalc_") as tmp:
        tmp_dir = Path(tmp)
        work = tmp_dir / src.name
        shutil.copyfile(src, work)
        engine = "ironcalc"
        values = ironcalc_values(work, formulas, timeout)
        if values is None:
            if shutil.which("soffice") is None:
                fail("IronCalc produced no values and soffice (LibreOffice) is not on PATH for the fallback")
            engine = "libreoffice"
            shutil.copyfile(src, work)
            produced = soffice_calculate(work, tmp_dir / "out", timeout)
            _, produced_payload = read_package(produced)
            values = computed_values(produced_payload)
        patched = tmp_dir / ("patched" + src.suffix)
        written, unmatched, charts_updated, chart_refs_skipped = transfer_values(src, patched, values)
        errors = scan_errors(patched, formulas)
        complete = unmatched == 0
        written_back = "--check-only" not in flags and (complete or "--force" in flags)
        if written_back:
            # The source is replaced only by a complete recalculation, and in one step.
            staged = src.with_name(src.name + ".recalc.tmp")
            shutil.copyfile(patched, staged)
            os.replace(staged, src)

    summary: dict[str, int] = {}
    for e in errors:
        summary[e["error"]] = summary.get(e["error"], 0) + 1
    return {
        # Unmatched cells outrank errors: the workbook was not written back, so a caller
        # reading "errors_found" would treat a partial pass as a finished calculation.
        "status": "incomplete" if unmatched else ("errors_found" if errors else "success"),
        "file": str(src),
        "total_formulas": len(formulas),
        "total_errors": len(errors),
        "error_summary": summary,
        "errors": errors[:MAX_LISTED],
        "truncated": len(errors) > MAX_LISTED,
        "values_written": written,
        "unmatched": unmatched,
        "charts_updated": charts_updated,
        "chart_refs_skipped": chart_refs_skipped,
        "engine": engine,
        "external_links": bool(has_external),
        "written": written_back,
    }


def main(argv: list[str]) -> None:
    if "-h" in argv or "--help" in argv:
        print(__doc__.strip())
        sys.exit(0)
    args = [a for a in argv if not a.startswith("--")]
    flags = {a for a in argv if a.startswith("--")}
    unknown = sorted(flags.difference(FLAGS))
    if unknown:
        # A misspelt --check-only used to drop out of the set unread, and the run then
        # replaced the very source the flag was there to protect.
        fail(f"unknown option: {', '.join(unknown)}; usage: recalc.py <file.xlsx> [timeout_seconds] [--check-only] [--force]")
    if not args:
        fail("usage: recalc.py <file.xlsx> [timeout_seconds] [--check-only] [--force]")  # --force also writes back an incomplete recalculation
    src = Path(args[0]).expanduser().resolve()
    try:
        timeout = int(args[1]) if len(args) > 1 else 60
    except ValueError:
        fail(f"timeout must be a whole number of seconds, not {args[1]!r}")
    if not src.exists():
        fail(f"file not found: {src}")
    if src.suffix.lower() not in (".xlsx", ".xlsm"):
        fail("recalc.py handles .xlsx and .xlsm files only")
    try:
        report = recalculate(src, timeout, flags)
    except Exception as exc:  # a workbook this script cannot open is a report, not a traceback
        print(json.dumps({"status": "error", "file": str(src), "message": f"{type(exc).__name__}: {exc}"}))
        sys.exit(1)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main(sys.argv[1:])
