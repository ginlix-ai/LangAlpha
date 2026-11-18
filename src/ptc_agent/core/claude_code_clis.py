"""How many Claude Code CLIs a computer of the user's own runs at once.

Its owner picks the number, 1 to ``CLIS_MAX``; one never set runs what its
memory allows (``default_clis``). The SSH transport opens as many connections
to the machine as its CLIs and their commands need (``_ssh_pool``), so the
count is bounded by the machine, not by one connection's sessions.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, NamedTuple, Protocol

if TYPE_CHECKING:
    from src.llms.claude_code import CliWait

CLIS_DEFAULT = 4
CLIS_MAX = 16
#: The memory each CLI is counted at for a computer whose owner set no
#: count. A CLI takes 150 to 300 MB, so this leaves most of a machine to its
#: own work: 8 on a 16 GB Mac, the most from 32 GB.
_BYTES_PER_CLI = 2 * 1024**3


def default_clis(memory_bytes: object) -> int:
    """The CLIs a computer runs at once when its owner set none, by the
    memory it reported; ``CLIS_DEFAULT`` when it reported none."""
    if type(memory_bytes) is not int:
        return CLIS_DEFAULT
    return max(CLIS_DEFAULT, min(CLIS_MAX, memory_bytes // _BYTES_PER_CLI))


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
