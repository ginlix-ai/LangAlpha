"""Line diffs between two versions' texts, and the run-length form a
version's line attribution is kept in. Pure: no I/O.

A diff is bounded in work, not time, so the same two texts always diff
the same way: the cached attribution is built one diff at a time and must
equal working it out whole.
"""

from __future__ import annotations

from difflib import SequenceMatcher

#: ``SequenceMatcher``'s junk heuristic stays off up to this many lines a
#: side, so counts are exact for any memo a person writes.
AUTOJUNK_LINES = 5000
#: Line comparisons one diff may make before it falls back to matching
#: lines as a multiset.
DIFF_WORK_MAX = 20_000_000


def lines_of(text: str) -> list[str]:
    """Lines as the web shows them: split on newlines, no line after a
    final newline, none at all in an empty text."""
    if not text:
        return []
    out = text.split("\n")
    if len(out) > 1 and out[-1] == "":
        out.pop()
    return out


class _TooCostly(Exception):
    pass


class _Bounded(SequenceMatcher):
    """``SequenceMatcher`` that gives up past ``DIFF_WORK_MAX``: a text of
    many repeated lines (5,000 blank lines, a table of zeros) costs it
    time quadratic in the repeats, in a thread nothing can stop."""

    def __init__(self, a: list[str], b: list[str]) -> None:
        self.left = DIFF_WORK_MAX
        self.weights: list[int] | None = None
        super().__init__(None, a, b, autojunk=max(len(a), len(b)) > AUTOJUNK_LINES)

    def find_longest_match(self, alo=0, ahi=None, blo=0, bhi=None):  # noqa: ANN001, ANN201
        if self.weights is None:
            # Work of a call: for each line of ``a`` in range, the places
            # ``b`` has it.
            self.weights = [0]
            for line in self.a:
                self.weights.append(self.weights[-1] + len(self.b2j.get(line, ())))
        ahi = len(self.a) if ahi is None else ahi
        bhi = len(self.b) if bhi is None else bhi
        self.left -= self.weights[ahi] - self.weights[alo] + (ahi - alo) + 1
        if self.left < 0:
            raise _TooCostly
        return super().find_longest_match(alo, ahi, blo, bhi)


def _multiset(a: list[str], b: list[str], a_by: list[int], seq: int) -> tuple[int, int, list[int]]:
    """Past the work cap: each line of ``b`` takes the first unused equal
    line of ``a``; what is left over is added or removed."""
    unused: dict[str, list[int]] = {}
    for i in range(len(a) - 1, -1, -1):
        unused.setdefault(a[i], []).append(i)
    b_by: list[int] = []
    added = 0
    for line in b:
        found = unused.get(line)
        if found:
            b_by.append(a_by[found.pop()])
        else:
            added += 1
            b_by.append(seq)
    return added, sum(len(v) for v in unused.values()), b_by


def step(a: list[str], b: list[str], a_by: list[int], seq: int) -> tuple[int, int, list[int]]:
    """Version ``seq``'s lines ``b`` against the one before's ``a``, whose
    line ``i`` version ``a_by[i]`` wrote: ``(added, removed, b_by)``, a line
    kept keeping its writer and every other written by ``seq``."""
    try:
        ops = _Bounded(a, b).get_opcodes()
    except _TooCostly:
        return _multiset(a, b, a_by, seq)
    added = removed = 0
    b_by: list[int] = []
    for tag, i1, i2, j1, j2 in ops:
        if tag == "equal":
            b_by.extend(a_by[i1:i2])
        else:
            removed += i2 - i1
            added += j2 - j1
            b_by.extend([seq] * (j2 - j1))
    return added, removed, b_by


def counts(a: list[str], b: list[str]) -> tuple[int, int]:
    """Lines ``b`` adds and removes against ``a``."""
    added, removed, _ = step(a, b, [0] * len(a), 0)
    return added, removed


def packed(by_line: list[int]) -> list[int]:
    """Per-line writers as run-length pairs ``[seq, count, ...]``."""
    out: list[int] = []
    for seq in by_line:
        if out and out[-2] == seq:
            out[-1] += 1
        else:
            out += [seq, 1]
    return out


def unpacked(pairs: list[int]) -> list[int]:
    out: list[int] = []
    for i in range(0, len(pairs), 2):
        out.extend([pairs[i]] * pairs[i + 1])
    return out


def runs_of(pairs: list[int]) -> list[tuple[int, int, int]]:
    """``(start_line, count, seq)``, 1-based, adjacent runs of one writer joined."""
    out: list[tuple[int, int, int]] = []
    line = 1
    for i in range(0, len(pairs), 2):
        seq, count = pairs[i], pairs[i + 1]
        if count <= 0:
            continue
        if out and out[-1][2] == seq:
            start, n, _ = out[-1]
            out[-1] = (start, n + count, seq)
        else:
            out.append((line, count, seq))
        line += count
    return out
