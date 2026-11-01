"""Using a sandbox only while it already runs.

A reader nobody is waiting on, such as an idle compaction, may use a computer
only while it runs: inside ``attach_only`` a reconnect that finds its sandbox in
any other state raises ``SandboxNotRunning`` rather than start, restore or wait
for it, and the server's acquire refuses the same (see
``src.server.services.computer_manager._attach_only``).
"""

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar

_attach_only: ContextVar[bool] = ContextVar("sandbox_attach_only", default=False)


class SandboxNotRunning(Exception):
    """A sandbox used under ``attach_only`` was not running."""


@contextmanager
def attach_only() -> Iterator[None]:
    token = _attach_only.set(True)
    try:
        yield
    finally:
        _attach_only.reset(token)


def attaching_only() -> bool:
    return _attach_only.get()


def for_every_acquire() -> None:
    """Lift ``attach_only`` in the current task, for work shared with every
    acquire that joins it, such as a machine's bring-up, which a task
    started under ``attach_only`` would otherwise inherit for its life."""
    _attach_only.set(False)
