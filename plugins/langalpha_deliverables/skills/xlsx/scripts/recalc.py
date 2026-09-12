#!/usr/bin/env python3
"""Recalculate every formula in a workbook (IronCalc, else LibreOffice) and report formula errors.

Usage:
    python recalc.py <file.xlsx> [timeout_seconds] [--check-only] [--force]

openpyxl writes formulas without cached values, so a workbook it saves shows
blanks in any viewer that does not calculate and hides errors until a human
opens it in Excel. This script calculates a disposable copy, then copies only
the computed values back into the original package: every other part of the
file (styles, charts, comments, validation, names, formula text, unknown parts)
stays byte-identical. The original is replaced only after the calculated values
were read back successfully.

Two engines. IronCalc (a Rust spreadsheet engine with Python bindings) runs in
milliseconds and is tried first; headless LibreOffice takes seconds and is the
fallback whenever IronCalc cannot load the file, raises, or returns #NAME? for
a function it does not implement. The report says which engine produced the
values.

Output is one JSON object on stdout:

    status          "success" (zero errors, every formula valued), "errors_found",
                    "incomplete" (the engine produced no value for some formula cells,
                    listed in unmatched; the source is left untouched unless --force),
                    or "error"
    total_formulas  formula cells in the workbook
    total_errors    cells whose computed value is an Excel error
    error_summary   {"#DIV/0!": n, "#REF!": n, ...}
    errors          [{"sheet", "cell", "error", "formula"}, ...] capped at 100
    truncated       true when the errors list was capped
    values_written  formula cells that received a computed value
    unmatched       formula cells the engine produced no value for
    engine          "ironcalc" or "libreoffice"

Exit code is 1 only for status "error" (file missing, LibreOffice failed,
timeout, external links without --force). "errors_found" exits 0 so a caller
can parse the report and fix the cells.

--check-only leaves the input untouched and reports from the calculated copy.
--force recalculates even when the workbook links to external files, which
LibreOffice cannot resolve here and will turn into #REF! or #NAME? errors.
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

EXCEL_ERRORS = ("#DIV/0!", "#REF!", "#NAME?", "#VALUE!", "#N/A", "#NUM!", "#NULL!", "#ERROR!", "#SPILL!", "#CALC!")
# IronCalc answers a construct it cannot evaluate with a bare #ERROR!, which is not an
# Excel error and so was never counted; any #TOKEN! or #TOKEN? string is an error.
ERROR_SHAPE = re.compile(r"^#(?:N/A|[A-Z0-9/]+[!?])$")
MAX_LISTED = 100
SML = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
REL_ID = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"

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


def collect_formulas(path: Path) -> tuple[dict[tuple[str, str], str], bool]:
    wb = load_workbook(path, data_only=False)
    formulas: dict[tuple[str, str], str] = {}
    for ws in wb.worksheets:
        for row in ws.iter_rows():
            for cell in row:
                if cell.data_type == "f":
                    formulas[(ws.title, cell.coordinate)] = str(cell.value)
    return formulas, bool(getattr(wb, "_external_links", None))


def sheet_parts(payload: dict[str, bytes]) -> dict[str, str]:
    """Map sheet name to its worksheet part path inside the package."""
    wb_xml = etree.fromstring(payload["xl/workbook.xml"])
    rels = etree.fromstring(payload["xl/_rels/workbook.xml.rels"])
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
        root = etree.fromstring(payload[part])
        cells = {}
        for c in root.iter(f"{{{SML}}}c"):
            if c.find(f"{{{SML}}}f") is None:
                continue
            v = c.find(f"{{{SML}}}v")
            cells[c.get("r")] = (c.get("t"), v.text if v is not None else None)
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
            root = etree.fromstring(payload[name])
            for lst in root.findall(f"{{{SML}}}commentList"):
                if len(lst):
                    lst[:] = []
                    changed = True
            payload[name] = etree.tostring(root, xml_declaration=True, encoding="UTF-8", standalone=True)
            continue
        if not name.endswith(".rels"):
            continue
        root = etree.fromstring(payload[name])
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
        if value == "#NAME?":
            raise SystemExit("unsupported function")  # let LibreOffice decide
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


def transfer_values(src: Path, dst: Path, values: dict[str, dict[str, tuple[str | None, str | None]]]) -> tuple[int, int]:
    """Write LibreOffice's computed values into the original package's formula cells only."""
    infos, payload = read_package(src)
    written = unmatched = 0
    for name, part in sheet_parts(payload).items():
        if part not in payload:
            continue
        root = etree.fromstring(payload[part])
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
    tmp = dst.with_name(dst.name + ".tmp")
    with zipfile.ZipFile(tmp, "w") as zout:
        for info in infos:
            zout.writestr(info, payload[info.filename], compress_type=info.compress_type)
    os.replace(tmp, dst)
    return written, unmatched


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


def main(argv: list[str]) -> None:
    if "-h" in argv or "--help" in argv:
        print(__doc__.strip())
        sys.exit(0)
    args = [a for a in argv if not a.startswith("--")]
    flags = {a for a in argv if a.startswith("--")}
    if not args:
        fail("usage: recalc.py <file.xlsx> [timeout_seconds] [--check-only] [--force]")  # --force also writes back an incomplete recalculation
    src = Path(args[0]).expanduser().resolve()
    timeout = int(args[1]) if len(args) > 1 else 60
    if not src.exists():
        fail(f"file not found: {src}")
    if src.suffix.lower() not in (".xlsx", ".xlsm"):
        fail("recalc.py handles .xlsx and .xlsm files only")
    formulas, has_external = collect_formulas(src)
    if has_external and "--force" not in flags:
        fail(
            "workbook links to external files; LibreOffice cannot resolve them here. "
            "Remove the links (paste the values, or reference a sheet in this workbook) "
            "or rerun with --force to accept #REF!/#NAME? in those cells.",
            external_links=True,
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
        written, unmatched = transfer_values(src, patched, values)
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
    print(json.dumps({
        "status": "errors_found" if errors else ("incomplete" if unmatched else "success"),
        "file": str(src),
        "total_formulas": len(formulas),
        "total_errors": len(errors),
        "error_summary": summary,
        "errors": errors[:MAX_LISTED],
        "truncated": len(errors) > MAX_LISTED,
        "values_written": written,
        "unmatched": unmatched,
        "engine": engine,
        "external_links": has_external,
        "written": written_back,
    }, indent=2))


if __name__ == "__main__":
    main(sys.argv[1:])
