"""The page sized to the view it is watched in.

The browser opens at the agent's 1280 by 800 (``launch.VIEWPORT``). A viewer's
panel has a shape of its own, and a page of another shape shows inside it with
empty bands, so each viewer asks for its view's size and the page takes it,
whether a person holds the page or the agent drives it: the agent then works
on the page the person sees. Kept to the view's shape, as large as the view up
to the largest the browser was launched with (``Launch.page_max``), shrunk as
a whole when the view is larger, so the page always fills the panel.

Two sizes stand. One the agent set itself (``set viewport``, ``set device``,
``--device``), since it is testing that size, until its browser closes
(``Launch.agent_sized``). And a window on the person's screen, which they
size. While a person holds the page, no other viewer's view sizes it, another
holder's included, so another panel never resizes what they are using.

The size is set on the tab that shows, as the engine sets it, so a tab opened
later opens at the agent's size until a viewer asks again.
"""

from __future__ import annotations

import subprocess
from typing import Literal

from . import care, hold, host, launch
from .launch import Launch
from .protocol import FIT_MIN

#: What the engine is given to resize the page.
_RESIZE_S = 10.0


def size(width: int, height: int, largest: tuple[int, int]) -> tuple[int, int]:
    """A page of *width* by *height*'s shape, as large as that up to *largest*,
    shrunk or grown as a whole so its shape never changes, then kept to
    ``FIT_MIN`` and *largest* where a shape too long for both leaves no other way."""
    shrink = min(1.0, largest[0] / width, largest[1] / height)
    w, h = width * shrink, height * shrink
    grow = max(1.0, FIT_MIN[0] / w, FIT_MIN[1] / h)
    w, h = w * grow, h * grow
    return min(max(round(w), FIT_MIN[0]), largest[0]), min(max(round(h), FIT_MIN[1]), largest[1])


#: The page keeps a size of its own for as long as its browser lasts: the
#: agent's, or a window's on the person's screen. Asking again changes nothing.
KEPT = "kept"


def fit(viewer: str, width: int, height: int) -> tuple[int, int] | Literal["kept"] | None:
    """Size the page to *viewer*'s view of *width* by *height* page pixels; the
    size it took, ``KEPT`` for a page that keeps its own, or None for one that
    cannot be sized now and may be later.

    Run on an open browser with the launch it already has, so this neither
    starts a browser nor restarts one, and under the gate's admission and the
    navigation lock, so it lands between the agent's commands and a person's
    navigations. A command or a navigation still running, or another person
    holding the page, leaves it as it is for now: waiting out a navigation
    under the admission would hold up the agent's commands behind it.
    """
    with care.admitted() as seen, hold.navigating(wait=False) as free:
        if not free:
            return None
        # A second holder too: the page is the first's, under their pointer.
        if hold.held_by_other(viewer):
            return None
        if seen.busy() or not seen.live():
            return None
        launched = seen.pin_engine(launch.current())
        if launched is None:
            return None
        if launched.window or launched.agent_sized:
            return KEPT
        page = size(width, height, launched.page_max)
        return page if _resize(launched, seen.host, *page, launched.stream.scale) else None


def _resize(launched: Launch, h: host.Host, width: int, height: int, scale: int) -> bool:
    try:
        return launched.run(h, "set", "viewport", str(width), str(height), str(scale), timeout=_RESIZE_S) is not None
    except (subprocess.TimeoutExpired, OSError):
        return False
