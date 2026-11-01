"""Acquiring a computer's session without starting it or keeping it up.

A reader nobody is waiting on, such as an idle compaction, may use a
computer only while it already runs, and must not count as activity: the
idle sweep would otherwise keep the computer up for a thread its user left.
Inside ``attach_only`` (the sandbox layer's, which its reconnect honours too)
an acquire that would start, wait for or rebuild the computer raises
``ComputerNotRunning``, and an attach stamps only the fence a concurrent stop
checks.
"""

from ptc_agent.core.sandbox.attach_only import (
    SandboxNotRunning,
    attach_only,
    attaching_only,
    for_every_acquire,
)


class ComputerNotRunning(SandboxNotRunning):
    """An acquire under ``attach_only`` met a computer it would have had to
    start, restart, wait for or rebuild."""


def refuse_start(computer_id: str) -> None:
    """Raise ``ComputerNotRunning`` where an acquire under ``attach_only``
    would bring the computer up."""
    if attaching_only():
        raise ComputerNotRunning(f"computer {computer_id} is not running")


__all__ = ["ComputerNotRunning", "attach_only", "attaching_only", "for_every_acquire", "refuse_start"]
