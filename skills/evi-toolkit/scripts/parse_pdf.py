#!/usr/bin/env python3
"""evi-toolkit / parse_pdf.py

Convert a single PDF to Markdown for the EVI base layer.

Strategy (in priority order):
  1. pdfplumber  → keeps page boundaries; best for financial PDFs
  2. pypdf       → text-only fallback
  3. fail soft   → write a stub markdown with `status: manual_required`
                   so the agent can decide what to do.

Usage:
    python3 parse_pdf.py --in <pdf> --out <md>
    python3 parse_pdf.py --in <pdf>          # auto: same path with .md suffix

Notes:
  - We do NOT try to extract tables here; financial PDFs vary too much.
    The downstream `extract_indicators.py` (executed by Agent) will do
    targeted extraction.
  - Output is plain Markdown with H2 page headers `## Page {n}`.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path


def _try_pdfplumber(pdf_path: Path) -> str | None:
    try:
        import pdfplumber  # type: ignore
    except Exception:
        return None
    try:
        out: list[str] = []
        with pdfplumber.open(str(pdf_path)) as pdf:
            for i, page in enumerate(pdf.pages, start=1):
                text = page.extract_text() or ""
                out.append(f"## Page {i}\n\n{text.strip()}\n")
        return "\n".join(out)
    except Exception as e:
        print(f"[parse_pdf] pdfplumber failed: {e}", file=sys.stderr)
        return None


def _try_pypdf(pdf_path: Path) -> str | None:
    try:
        from pypdf import PdfReader  # type: ignore
    except Exception:
        try:
            from PyPDF2 import PdfReader  # type: ignore
        except Exception:
            return None
    try:
        reader = PdfReader(str(pdf_path))
        out: list[str] = []
        for i, page in enumerate(reader.pages, start=1):
            text = page.extract_text() or ""
            out.append(f"## Page {i}\n\n{text.strip()}\n")
        return "\n".join(out)
    except Exception as e:
        print(f"[parse_pdf] pypdf failed: {e}", file=sys.stderr)
        return None


def parse_pdf(pdf: Path, out: Path) -> int:
    if not pdf.exists():
        print(f"[parse_pdf] ERROR: not found: {pdf}", file=sys.stderr)
        return 2

    text = _try_pdfplumber(pdf) or _try_pypdf(pdf)
    out.parent.mkdir(parents=True, exist_ok=True)

    if text is None or not text.strip():
        out.write_text(
            f"---\nstatus: manual_required\nsource: {pdf.name}\n---\n\n"
            f"PDF parsing failed (no `pdfplumber`/`pypdf` available, or pdf is scanned).\n"
            f"Agent should fall back to LLM-based document understanding.\n",
            encoding="utf-8",
        )
        print(f"[parse_pdf] WARN: wrote stub (no parser worked) → {out}")
        return 1

    header = (
        f"---\nsource_pdf: {pdf.name}\nparsed_by: evi-toolkit/parse_pdf.py\n---\n\n"
    )
    out.write_text(header + text, encoding="utf-8")
    print(f"[parse_pdf] OK {pdf.name} → {out} ({len(text):,} chars)")
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--in", dest="pdf", required=True, help="Input PDF path")
    p.add_argument("--out", dest="md", help="Output MD path (default: <pdf>.md)")
    args = p.parse_args()

    pdf = Path(args.pdf).resolve()
    out = Path(args.md).resolve() if args.md else pdf.with_suffix(".md")
    return parse_pdf(pdf, out)


if __name__ == "__main__":
    sys.exit(main())
