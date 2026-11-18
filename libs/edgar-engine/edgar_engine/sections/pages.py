"""Printed pages: the page-number chain each rendered document prints, and the lines of a printed page range.

Stubs ("on pages 20 to 23"), cross-reference indexes and contents rows all cite printed pages, so they share
this one map from a page number to its footer line.
"""
from __future__ import annotations

import re

from .rendering import Rendering
from .text import normalize


class Pagination:
    def __init__(self, rendering: Rendering):
        self.rendering = rendering
        self._footers: dict[int, dict[str, int]] = {}
        self._running: dict[int, set[str]] = {}

    def footers(self, d: int) -> dict[str, int]:
        """Printed page number -> its footer line in document d. A footer is a short line that is a page number
        alone or a number beside a running text that repeats on many pages ("IKON OFFICE SOLUTIONS 37", or on
        left-hand pages "38 2025 FORM 10-K", the number before a running text that opens with a year). A page
        number printed twice on a page (a header and a footer) ends the page at the later one."""
        if d in self._footers:
            return self._footers[d]
        lines = self.rendering.lines
        a, b = self.rendering.doc_range(d)
        cands = []
        for ln in range(a, b):
            s = normalize(lines[ln])
            if len(s) > 80:
                continue
            m = re.fullmatch(r"(?P<pre>.*?)[\s|]*(?:page\s+)?-?\s?(?P<p>(?:F-\s?)?\d{1,3})\s?-?", s, re.I) or \
                re.fullmatch(r"-?\s?(?P<p>(?:F-\s?)?\d{1,3})\s?-?[\s|]+(?P<pre>(?=\D|\d{4}\D*[a-z]).*)", s, re.I)
            if m:
                cands.append((m.group("pre").strip(" |-").lower(), m.group("p").replace(" ", "").upper(), ln))
        stems: dict[str, set] = {}
        for stem, p, _ in cands:
            stems.setdefault(stem, set()).add(p)
        out: dict[str, int] = {}
        for prefix in ("", "F-"):
            seq = [(int(p[len(prefix):]), ln) for stem, p, ln in cands
                   if (stem == "" or len(stems[stem]) >= 3) and p.startswith(prefix) and p[len(prefix):].isdigit()]
            chain: list[tuple[int, int]] = []
            for v, ln in seq:
                if not chain or 0 < v - chain[-1][0] <= 3:
                    chain.append((v, ln))
                elif v == chain[-1][0]:
                    chain[-1] = (v, ln)
                elif v == 1 or (len(chain) < 3 and v > chain[-1][0]):
                    chain = [(v, ln)] if len(chain) < 3 else chain
            if len(chain) >= 3:
                for v, ln in chain:
                    out.setdefault(f"{prefix}{v}", ln)
        self._footers[d] = out
        return out

    def page_range(self, d: int, lo: str, hi: str) -> tuple[int, int] | None:
        """Lines of printed pages lo..hi in document d, without the running header that opens the first page."""
        f = self.footers(d)
        if lo not in f or hi not in f or f[hi] < f[lo]:
            return None
        prev = [ln for ln in f.values() if ln < f[lo]]
        a, _ = self.rendering.doc_range(d)
        start = max(prev) + 1 if prev else a
        running = self._running_heads(d)
        while start < f[hi] and normalize(self.rendering.lines[start]) in running:
            start += 1
        return (start, f[hi] + 1)

    def breaks(self) -> list[int]:
        """Every footer line of every document, in order: where printed pages end."""
        return sorted(ln for d in range(len(self.rendering.documents)) for ln in self.footers(d).values())

    def _running_heads(self, d: int) -> set[str]:
        if d not in self._running:
            a, b = self.rendering.doc_range(d)
            counts: dict[str, int] = {}
            for ln in range(a, b):
                k = normalize(self.rendering.lines[ln])
                if len(k) <= 100:
                    counts[k] = counts.get(k, 0) + 1
            self._running[d] = {k for k, n in counts.items() if n >= 5 and k}
        return self._running[d]
