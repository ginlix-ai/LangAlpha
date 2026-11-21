"""How sharply the live view draws the page in a browser starting now: as chosen
for the computer, which outlives any one browser, or by the computer's size.
The open browser keeps the quality it started with (``launch``)."""

from __future__ import annotations

import os

from . import choices, host, system
from .protocol import RETINA_BY_DEFAULT, RETINA_MIN_CPUS, StreamQualityName


def cpus() -> float:
    """The CPUs the computer may use: its own limit where it has one, as a
    container does, else every CPU the machine has."""
    quota = system.cpu_quota()
    return quota if quota is not None else float(os.cpu_count() or 1)


def chosen() -> StreamQualityName:
    """The quality a browser starting now gets: the one chosen for the computer,
    or by its size when none is, and never one its CPUs cannot keep up with. A
    person's own machine gets standard unless they choose: its CPUs are theirs,
    and the stream crosses their connection."""
    count = cpus()
    default = "retina" if count >= RETINA_BY_DEFAULT and not host.current().own else "standard"
    held = choices.held()
    name = (held and held.quality) or default
    if name == "retina" and count < RETINA_MIN_CPUS:
        return "sharp"
    return name
