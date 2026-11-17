"""How many Claude Code CLIs a computer of the user's own runs at once.

Its owner picks the number, 1 to ``CLIS_MAX``; one never set runs
``CLIS_DEFAULT``. The SSH transport carries a login's CLIs on at most two
connections of ``CLI_SLOTS`` each, so the most is two connections' worth and
the default is one's.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, NamedTuple, Protocol

if TYPE_CHECKING:
    from src.llms.claude_code import CliWait

CLIS_DEFAULT = 4
CLIS_MAX = 8


def agents(n: int) -> str:
    """``n`` Claude Code agents, as a refusal names them."""
    return "1 agent" if n == 1 else f"{n} agents"


class CliCount(NamedTuple):
    """What a CLI starting on a computer reads of it: the most CLIs it runs at
    once, whether that is its owner's to set, and its name, read together
    so a CLI that finds them all taken has what its notice says."""

    cap: int
    settable: bool
    name: str | None


class CliQueue(Protocol):
    """Told about a CLI that waits for one of its computer's CLIs: once, as
    it finds them all taken, and once the wait ends, by getting one
    (``admitted``) or not (``gave_up``: it timed out or was cancelled), after
    how long. A CLI that never waited tells it nothing."""

    def waiting(self, wait: CliWait) -> None: ...

    def admitted(self, waited_s: float) -> None: ...

    def gave_up(self, waited_s: float) -> None: ...
