"""What a server says it is, reduced to the one thing a row draws.

The MCP handshake has carried a server's own name, website and icons since
spec revision 2025-11-25, and we completed that handshake and threw all of it
away. A user who adds a server that ships a perfectly good mark still gets a
letter in a tinted square, which is the fallback for servers that told us
nothing rather than an answer.

Nothing here trusts the card. It arrives from a server the user added, not
from us, so every field is optional, every shape is checked, and the worst
case is the monogram that was already going to be drawn.
"""

from __future__ import annotations

from urllib.parse import urlsplit

from src.server.services.brand_icons import MAX_SOURCE_CHARS

#: A `sizes` entry the spec allows in place of dimensions, meaning "scalable".
#: Nothing outranks it, because an SVG is the right answer at every tile size.
_ANY = "any"

#: Longest run of digits worth converting. Past six there is no tile it could
#: describe, and past 4,300 ``int()`` refuses the string on its own -- which
#: would raise out of a reducer whose worst case is meant to be the monogram.
_MAX_DIMENSION_DIGITS = 6


def _largest(sizes: object) -> int:
    """The biggest square edge a ``sizes`` list claims, 0 when it claims none."""
    if not isinstance(sizes, list):
        return 0
    best = 0
    for entry in sizes:
        if not isinstance(entry, str):
            continue
        if entry.strip().lower() == _ANY:
            return 1 << 16
        head, _, tail = entry.lower().partition("x")
        if not (head.isdigit() and tail.isdigit()):
            continue
        if max(len(head), len(tail)) > _MAX_DIMENSION_DIGITS:
            # Claims nothing rather than claiming everything: an unreadable
            # size must not outrank an icon that gave a real one.
            continue
        best = max(best, min(int(head), int(tail)))
    return best


def _usable(icon: object) -> tuple[str, int, int] | None:
    """``(src, untinted, size)`` for a drawable icon entry, else ``None``."""
    if not isinstance(icon, dict):
        return None
    src = icon.get("src")
    if not isinstance(src, str) or not src.strip():
        return None
    mime = icon.get("mimeType")
    if isinstance(mime, str) and mime and not mime.startswith("image/"):
        return None
    # A themed icon is right for half the viewers and wrong for the other half,
    # because the tile it lands in follows the reader's theme and this choice
    # is made once, on the host, for everyone. An untinted mark outranks a
    # larger themed one for that reason alone.
    untinted = 0 if icon.get("theme") else 1
    return src.strip(), untinted, _largest(icon.get("sizes"))


def icon_source(server_info: dict | None) -> str | None:
    """Where to find this server's mark, or ``None`` if it named nowhere.

    A declared icon beats the website, because an icon is the mark the author
    chose and a favicon is whatever their web host happens to serve. The
    website is still worth keeping as the fallback: it costs one field to read
    and it is the only mark most servers will ever have.

    The return value is a source in the sense ``brand_icons.icon_for_source``
    means it, so a `data:` URI, an absolute URL and a bare host all come back
    from here and the caller does not sort them out.

    Total by construction, never raising. Every field here comes from a
    server's own handshake, so for a user-added server it is that operator's
    text, and the answer is persisted and then read again on every listing.
    A reducer for a decorative field that can raise takes the whole catalog
    page down for the one user who can least repair it: the row they would
    have to delete is only reachable through the listing that is failing.
    """
    if not isinstance(server_info, dict):
        return None

    best: tuple[int, int] = (-1, -1)
    chosen: str | None = None
    icons = server_info.get("icons")
    for entry in icons if isinstance(icons, list) else ():
        usable = _usable(entry)
        if usable is None:
            continue
        src, untinted, size = usable
        if (untinted, size) > best:
            best, chosen = (untinted, size), src
    if chosen is not None:
        return chosen

    website = server_info.get("websiteUrl")
    if isinstance(website, str) and website.strip():
        try:
            host = urlsplit(website.strip()).hostname
        except ValueError:
            return None
        if host:
            return host
    return None


#: Enough variants for a server to offer a themed and a scalable mark without
#: letting it park an unbounded list in a row that every listing loads.
_MAX_ICONS = 8
#: A cut for the short descriptive strings. Generous for a real value, and the
#: point is only that no single one of them can be large.
_MAX_FIELD_CHARS = 512
#: Every ``src`` on the card, added up. A per-icon cut alone still lets eight
#: near-maximum sources through, which is megabytes in a row that every
#: listing reads back, so the total has to be bounded too. One resolvable
#: icon's worth is the budget: real variants are URLs or small data URIs and
#: fit many times over, and a server that spends it all on one is the
#: single-icon case the per-icon cut already allows.
_MAX_TOTAL_SOURCE_CHARS = MAX_SOURCE_CHARS


def bounded_identity(server_info: object) -> dict | None:
    """``server_info`` cut down to what a row can afford to carry.

    The card arrives from a server the user configured and is written into
    JSONB that every later listing reads back, so its size is that server's
    choice unless something here makes it ours. A ``src`` too long for the
    icon route to resolve is dropped rather than stored: it could never have
    produced a mark, so keeping it costs every read and buys nothing.
    """
    if not isinstance(server_info, dict):
        return None

    out: dict = {}
    icons: list[dict] = []
    spent = 0
    declared = server_info.get("icons")
    for entry in declared if isinstance(declared, list) else ():
        if len(icons) >= _MAX_ICONS:
            break
        if not isinstance(entry, dict):
            continue
        src = entry.get("src")
        if not isinstance(src, str) or not src.strip() or len(src) > MAX_SOURCE_CHARS:
            continue
        if spent + len(src) > _MAX_TOTAL_SOURCE_CHARS:
            continue
        if not _encodable(src):
            continue
        spent += len(src)
        kept: dict = {"src": src}
        for key in ("mimeType", "theme"):
            value = entry.get(key)
            if isinstance(value, str):
                kept[key] = value[:_MAX_FIELD_CHARS]
        sizes = entry.get("sizes")
        if isinstance(sizes, list):
            kept["sizes"] = [
                s[:_MAX_FIELD_CHARS] for s in sizes if isinstance(s, str)
            ][:_MAX_ICONS]
        icons.append(kept)
    if icons:
        out["icons"] = icons

    for key in ("name", "title", "version", "websiteUrl"):
        value = server_info.get(key)
        if isinstance(value, str) and value.strip() and _encodable(value):
            out[key] = value[:_MAX_FIELD_CHARS]

    return out or None


def _encodable(value: str) -> bool:
    """Whether the string can survive being stored and served.

    JSON carries lone surrogates -- a lone high-surrogate escape parses to one --
    and Python holds them, but no UTF-8 encoder takes them. Keeping one means
    a row that fails on write or a listing that raises where the field is
    decorative, so it is dropped at the same place its length is.
    """
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        return False
    return True
