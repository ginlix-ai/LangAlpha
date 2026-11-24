"""What the file operations ask of the threads beside them.

A FUSE handler cannot notify the kernel itself (an invalidation waits on
the directory lock the request being answered may hold), and should not
wait on the server taking notices. So it asks:
``Effects`` is that request, and ``Signals`` holds it for the threads that
carry each out. The defaults do nothing, which is what the operations get
when nothing serves them (a check driving them in process).
"""

from __future__ import annotations

import threading



class Effects:
    def forget(self, path: str) -> None:
        """Have the kernel drop what it holds of ``path`` and its listing."""

    def noticed(self) -> None:
        """A notice for the agent was filed: send it to the server."""


class Signals(Effects):
    """What the file operations ask, held for the threads that carry it out."""

    def __init__(self) -> None:
        self.forgets: set[str] = set()
        self.forget_mu = threading.Lock()
        self.notice = threading.Event()

    def forget(self, path: str) -> None:
        with self.forget_mu:
            self.forgets.add(path)

    def noticed(self) -> None:
        self.notice.set()

    def take_forgets(self) -> set[str]:
        with self.forget_mu:
            paths, self.forgets = self.forgets, set()
        return paths
