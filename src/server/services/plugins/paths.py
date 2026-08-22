"""Archive member-path containment (spec §4.1).

Every member path must resolve inside the package root. This module is pure
string policy — the archive layer applies it before any bytes are read.
"""


def split_member(name: str) -> tuple[str, ...] | None:
    """Normalize a member path to its segments, or None when it escapes.

    Rejects absolute paths, Windows drive/backslash forms, ``..`` segments,
    and NUL; collapses ``.`` and empty segments. A directory member yields its
    segments too (trailing slash tolerated).
    """
    if not isinstance(name, str) or not name or "\x00" in name:
        return None
    if "\\" in name:
        return None
    if name.startswith("/") or (len(name) > 1 and name[1] == ":"):
        return None
    segments = tuple(s for s in name.split("/") if s not in ("", "."))
    if any(s == ".." for s in segments):
        return None
    return segments
