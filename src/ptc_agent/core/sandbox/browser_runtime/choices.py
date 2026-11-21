"""The computer's choices for the browsers it starts, as the machine last took
them from the server (``machine.choose``): one record, so a browser never
starts on part of one save and part of another. Until the first reaches it, a
browser follows the computer's size and keeps no sign-ins.
"""

from __future__ import annotations

from typing import Optional

from .protocol import Choices, read_record, write_record

_FILE = "choices"


def held() -> Optional[Choices]:
    """The choices the machine holds, or None before any reached it."""
    return read_record(_FILE, Choices, exact=True)


def keep(chosen: Choices) -> None:
    write_record(_FILE, chosen)
