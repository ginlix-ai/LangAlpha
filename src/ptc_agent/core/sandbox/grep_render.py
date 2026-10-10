"""ripgrep ``--json`` output rendered as the lines plain rg prints, long lines cut."""

import base64
import json
from typing import Any


class GrepLine(str):
    """A line Grep prints in content mode, holding the path of the file it is
    from. A path may hold the ``:`` and ``-`` that end it in the line, so the
    path is never read back out of the text.

    ``named`` is whether the line starts with the path, which rg leaves off
    when it searches the one file it was given.
    """

    __slots__ = ("path", "named")

    path: str
    named: bool

    def __new__(cls, text: str, path: str, named: bool = True) -> "GrepLine":
        line = super().__new__(cls, text)
        line.path = path
        line.named = named
        return line

    def __reduce__(self) -> tuple[Any, ...]:
        return (GrepLine, (str(self), self.path, self.named))

    def at(self, path: str) -> "GrepLine":
        """The line with its file spelled ``path``."""
        if path == self.path:
            return self
        text = path + self[len(self.path) :] if self.named else str(self)
        return GrepLine(text, path, self.named)


# A matching line can be a whole JSON event or a minified bundle, and one line
# returned whole can outweigh the model's context. Past this many characters a
# line comes back as windows around its first few matches.
GREP_LINE_CHARS = 500
_GREP_WINDOW_BYTES = 150
_GREP_WINDOWS_PER_LINE = 3


def _rg_data(field: dict[str, Any]) -> bytes:
    if "text" in field:
        return field["text"].encode()
    return base64.b64decode(field.get("bytes", ""))


def _cut_grep_line(line: bytes, spans: list[tuple[int, int]]) -> str:
    """A long line cut to windows around its first matches."""
    text = line.decode(errors="replace")
    if len(text) <= GREP_LINE_CHARS:
        return text
    if not spans:
        return f"{text[:GREP_LINE_CHARS]}… [line of {len(text):,} chars, cut]"
    windows: list[list[int]] = []
    for start, end in spans[:_GREP_WINDOWS_PER_LINE]:
        low = max(0, start - _GREP_WINDOW_BYTES)
        high = min(len(line), end + _GREP_WINDOW_BYTES, start + 2 * _GREP_WINDOW_BYTES)
        if windows and low <= windows[-1][1]:
            windows[-1][1] = max(windows[-1][1], high)
        else:
            windows.append([low, high])
    body = " … ".join(line[low:high].decode(errors="ignore") for low, high in windows)
    if windows[0][0] > 0:
        body = "…" + body
    if windows[-1][1] < len(line):
        body += "…"
    total = len(spans)
    if total == 1:
        which = "its match"
    elif total <= _GREP_WINDOWS_PER_LINE:
        which = f"its {total} matches"
    else:
        which = f"{_GREP_WINDOWS_PER_LINE} of its {total} matches"
    return f"{body} [line of {len(text):,} chars, cut around {which}]"


def render_grep_json(
    output: str, search_path: str, *, line_numbers: bool, grouped: bool
) -> list[str]:
    """rg ``--json`` events as the lines plain rg prints, long lines cut,
    each a ``GrepLine``; a ``--`` between groups and an error are plain.

    rg writes its errors (a pattern it rejects, a missing or unreadable path)
    as plain text on stderr, which the exec folds into the output. They follow
    the rendered lines, so a run that only failed reads as the failure rather
    than as no matches.
    """
    events = []
    errors: list[str] = []
    for raw in output.splitlines():
        try:
            event = json.loads(raw)
        except ValueError:
            event = None
        if not isinstance(event, dict):
            errors.append(_cut_grep_line(raw.encode(), []))
            continue
        if event.get("type") in ("match", "context"):
            events.append((event["type"] == "match", event["data"]))
    # rg names the file only when it searched more than the one file it was given.
    named = any(_rg_data(data["path"]).decode(errors="replace") != search_path for _, data in events)

    lines: list[str] = []
    last: tuple[str, int] | None = None
    for is_match, data in events:
        path = _rg_data(data["path"]).decode(errors="replace")
        first = data.get("line_number") or 0
        block = _rg_data(data["lines"])
        if block.endswith(b"\n"):
            block = block[:-1]
        spans = [(m["start"], m["end"]) for m in data.get("submatches") or []]
        if grouped and last is not None and (path != last[0] or first != last[1] + 1):
            lines.append("--")
        sep = ":" if is_match else "-"
        # A multiline match spans several lines; each prints on its own.
        begin = 0
        for number, line in enumerate(block.split(b"\n"), start=first):
            end = begin + len(line)
            own = [
                (max(s, begin) - begin, min(e, end) - begin)
                for s, e in spans
                if (s < end and e > begin) or (s == e and begin <= s <= end)
            ]
            prefix = f"{path}{sep}" if named else ""
            if line_numbers:
                prefix += f"{number}{sep}"
            lines.append(GrepLine(prefix + _cut_grep_line(line, own), path, named))
            begin = end + 1
            last = (path, number)
    return lines + errors
