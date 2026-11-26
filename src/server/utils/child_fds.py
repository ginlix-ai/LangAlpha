"""What a worker's child processes inherit."""

import contextlib
import os
import sys


def keep_descriptors_from_children() -> None:
    """Leave every descriptor this worker holds to itself, as asyncio would.

    uvicorn hands each worker the server's listening socket still inheritable,
    and uvloop starts a child without closing what it inherits, unlike
    asyncio's ``close_fds``. Every command, CLI and MCP server the worker
    started would hold the port, so one outliving its worker would keep a
    restart from binding it. A descriptor a child should have goes to it by
    ``pass_fds``, which makes it inheritable for that child alone.
    """
    if sys.platform == "win32":
        return
    fd_dir = "/proc/self/fd" if os.path.isdir("/proc/self/fd") else "/dev/fd"
    for name in os.listdir(fd_dir):
        fd = int(name)
        if fd > 2:
            # The listing's own descriptor is closed by the time it is reached.
            with contextlib.suppress(OSError):
                os.set_inheritable(fd, False)
