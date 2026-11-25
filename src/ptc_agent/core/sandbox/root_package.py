"""A sandbox package that root runs from a checked copy of its own.

The package is shipped under the internal sources, which the sandbox's own
user can rewrite, so root never imports it there: the host sends ``boot``'s
text as the command, and ``boot`` runs a copy in root's private directory made
only from files that read as the digests this host ships. A machine whose
shipped files are not this host's answers ``stale_code`` and runs nothing.
"""

from __future__ import annotations

import functools
import hashlib
import json
import shlex
from collections.abc import Callable
from pathlib import Path
from typing import Any, Final

STALE_CODE: Final = "stale_code"
# Making the folder the package is shipped into.
SHIP_TIMEOUT_S: Final = 30


def python_files(folder: Path) -> list[Path]:
    """A package's own modules, sorted: what the file mount's daemon ships."""
    return sorted(p for p in folder.iterdir() if p.is_file() and p.suffix == ".py")


class RootPackage:
    """*package_dir*'s files as *listing* picks them, the same list the asset
    sync ships, shipped as *name* (the folder's own name unless given) and run
    by root from a copy under *private*. With
    *keep_copies*, a new copy drops all but that many copies of other code;
    without, the package's own code owns that, as the file mount's daemon
    does when it replaces itself."""

    def __init__(
        self,
        package_dir: Path,
        private: str,
        *,
        name: str | None = None,
        listing: Callable[[Path], list[Path]] = python_files,
        keep_copies: int | None = None,
    ) -> None:
        self.package_dir = package_dir
        self.private = private
        self.name = name or package_dir.name
        self.listing = listing
        self.keep_copies = keep_copies

    @functools.cached_property
    def files(self) -> tuple[tuple[str, bytes], ...]:
        """What a machine is shipped, by name."""
        return tuple((path.name, path.read_bytes()) for path in self.listing(self.package_dir))

    @functools.cached_property
    def code(self) -> str:
        """The version root's copy is kept under."""
        digest = hashlib.sha256()
        for name, data in self.files:
            digest.update(name.encode() + b"\0" + data)
        return digest.hexdigest()[:16]

    @functools.cached_property
    def manifest(self) -> str:
        """Each file's digest, which ``boot`` checks the shipped files against."""
        return json.dumps(
            {name: hashlib.sha256(data).hexdigest() for name, data in self.files}, sort_keys=True
        )

    @functools.cached_property
    def _boot(self) -> tuple[str, str]:
        from ptc_agent.core.sandbox.livefs_runtime import boot

        return Path(boot.__file__).read_text(), boot.KEEP_FLAG

    def shipped_dir(self, internal_src: str) -> str:
        return f"{internal_src}/{self.name}"

    def command(self, internal_src: str, *args: str, private: str | None = None) -> str:
        """Root's command running *args* in the package; *private* stands in
        for the package's own directory, as the Drive's daemon runs the file
        mount's code from a directory of its own."""
        script, keep_flag = self._boot
        keep = () if self.keep_copies is None else (f"{keep_flag}{self.keep_copies}",)
        argv = (
            script,
            private or self.private,
            self.shipped_dir(internal_src),
            self.code,
            self.manifest,
            *keep,
            *args,
        )
        return "cd / && python3 -I -c " + " ".join(shlex.quote(a) for a in argv)

    async def ship(self, runtime: Any, internal_src: str) -> None:
        """Put the files where the asset sync would, so the next sync finds
        them as it ships them."""
        shipped = self.shipped_dir(internal_src)
        await runtime.exec(f"mkdir -p {shlex.quote(shipped)}", timeout=SHIP_TIMEOUT_S)
        await runtime.upload_files([(data, f"{shipped}/{name}") for name, data in self.files])

    async def run(
        self, runtime: Any, internal_src: str, *args: str, timeout: float
    ) -> dict[str, Any] | None:
        """*args* run as root, the JSON answer printed last, or None. A machine
        whose shipped files are missing or not this host's (a warm one that
        has not synced since a deploy) is shipped them and asked once more.

        On the runtime itself: a session's call path reconnects, and would
        start a computer that stopped."""
        answer = await self._once(runtime, internal_src, args, timeout)
        if _stale(answer):
            await self.ship(runtime, internal_src)
            answer = await self._once(runtime, internal_src, args, timeout)
        return None if _stale(answer) else answer

    async def _once(
        self, runtime: Any, internal_src: str, args: tuple[str, ...], timeout: float
    ) -> dict[str, Any] | None:
        return parse(await runtime.exec_as_root(self.command(internal_src, *args), timeout))


def parse(result: Any) -> dict[str, Any] | None:
    """The JSON answer a command printed last (None: none)."""
    lines = (getattr(result, "stdout", "") or "").strip().splitlines()
    try:
        answer = json.loads(lines[-1]) if lines else None
    except ValueError:
        return None
    return answer if isinstance(answer, dict) else None


def _stale(answer: dict[str, Any] | None) -> bool:
    return answer is not None and answer.get("error") == STALE_CODE
