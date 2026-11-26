"""Which engine, Chrome and ports this machine's browser uses.

A cloud computer's image installs the engine and Chrome at fixed paths, and the
ports the runtime listens on are its alone (``protocol``). A person's own
machine has neither: ``install`` puts the engine under the machine's state,
picks a Chrome-family browser already there, and picks ports, and records them
here. Without that record the machine is a cloud computer.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from .protocol import (
    BRIDGE_PORT,
    CHROME_BIN,
    CHROME_POLICY_FILE,
    ENGINE_BIN,
    PROXY_PORT,
    SERVICE_PORTS,
    STREAM_PORT,
    read_record,
    write_record,
)

_FILE = "host"


@dataclass(frozen=True)
class Host:
    """``chrome`` is what a browser the engine starts runs as, its first
    argument, and ``executable`` what the engine is told to start, which runs
    it: the same file, except on a Mac, where a launcher keeps the keychain."""

    own: bool = False
    engine: str = ENGINE_BIN
    chrome: str = CHROME_BIN
    executable: str = CHROME_BIN
    stream_port: int = STREAM_PORT
    bridge_port: int = BRIDGE_PORT
    proxy_port: int = PROXY_PORT

    def ready(self) -> bool:
        """Whether the engine is here and a browser for it: a tool update can
        install the engine where the Chrome it links is missing. On a cloud
        computer, also the managed policy, which a tool update that failed on
        it alone would leave missing: no browser starts without it."""
        return (
            os.access(self.engine, os.X_OK)
            and os.access(self.chrome, os.X_OK)
            and (self.own or os.path.isfile(CHROME_POLICY_FILE))
        )

    @property
    def bridge_address(self) -> str:
        """Where the bridge listens. A cloud computer's preview reaches it from
        outside the machine. A person's own machine is reached by an SSH login
        or its tunnel, both of which arrive on loopback, so nothing else on
        their network can."""
        return "127.0.0.1" if self.own else "0.0.0.0"

    @property
    def proxy_url(self) -> str:
        """The proxy every browser of this runtime's goes through, which its
        arguments name, telling it from another runtime's on the same machine."""
        return f"http://127.0.0.1:{self.proxy_port}"

    @property
    def service_ports(self) -> frozenset[int]:
        """The ports on this machine the browser must not reach."""
        ours = {self.stream_port, self.bridge_port, self.proxy_port}
        return frozenset(ours if self.own else ours.union(SERVICE_PORTS))


def current() -> Host:
    """The recorded host, or the cloud image's."""
    # Every field or none: a record short of one would mix a person's machine
    # with the image's engine, Chrome or ports.
    return read_record(_FILE, Host, exact=True) or Host()


def record(host: Host) -> None:
    write_record(_FILE, host)
