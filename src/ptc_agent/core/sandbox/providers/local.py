"""Local provider: the agent runs directly on the machine the server runs on.

For the bundled desktop app, where the server is the owner's own and their
machine is the computer. It runs as the same OS user with no isolation, so it
exists only where a build turned it on for one person's own server:
:func:`local_computer_allowed` is false on any hosted one, whatever a computer
row says, and a provider cannot be built there at all.

The machine is the owner's, so like an SSH computer it is never rebuilt and
stopping one ends only its own processes. Unlike one, it lasts as long as the
server and its database do, so Home may live on it.
"""

from __future__ import annotations

import os
import sys
import uuid
from collections.abc import Awaitable, Callable, Collection, Sequence
from typing import Any

from ptc_agent.config.core import LocalConfig, PlatformSecretDefinition
from ptc_agent.core.paths import DEFAULT_SANDBOX_ROOT
from ptc_agent.core.sandbox.providers.local_runtime import (
    LocalRuntime,
    machine_home,
    root_is_free,
)
from ptc_agent.core.sandbox.providers.ssh_errors import connect_failure
from ptc_agent.core.sandbox.runtime import (
    SandboxFailureKind,
    SandboxProvider,
    SandboxTransientError,
)

__all__ = [
    "LocalComputerNotAllowed",
    "NoFreeRootFolder",
    "LocalProvider",
    "LocalRuntime",
    "local_computer_allowed",
]

_ROOT_FOLDER = "LangAlpha"
# Past this many folders of its name, the home is not one a computer picks in.
_ROOT_CANDIDATES = 100


class LocalComputerNotAllowed(ValueError):
    """Asked for a computer on the server's own machine where none may run."""


class NoFreeRootFolder(ValueError):
    """Every folder a new computer here would pick for itself is taken."""


def local_computer_allowed() -> bool:
    """Whether agent code may run as this server's own user: only on an
    open-source server whose build turned it on, and where there is a bash.

    An unset ``HOST_MODE`` is an open-source server, as the server reads it.
    The owner secret is required too, since without it the server takes any
    caller on the machine, a container included, for its owner.
    """
    return (
        os.environ.get("HOST_MODE", "oss") == "oss"
        and os.environ.get("LANGALPHA_LOCAL_COMPUTER") == "1"
        and bool(os.environ.get("LANGALPHA_OWNER_SECRET"))
        and sys.platform != "win32"
    )


class LocalProvider(SandboxProvider):
    """Provider for computers on the server's own machine. Construction does no
    I/O beyond reading the environment, and neither does ``get``."""

    label = "This computer"
    runs_remote = False
    holds_always_on = True
    recreatable = False
    metered = False
    converges_env = True
    # The file mount needs root and /dev/fuse, which the owner need not grant.
    mounts_livefs = False
    # No sandbox image put a browser here, so the machine sets its own up, as
    # one brought over SSH does.
    installs_browser = True
    serves_toolset = False
    platform_secret_namespace_env = None
    # Every computer here was laid out at v4 or later, and the older steps
    # move and delete folders by name in a root that holds the owner's files.
    first_layout_version = 4
    default_computer_name = "This computer"

    def __init__(
        self,
        config: LocalConfig,
        working_dir: str | None = None,
        *,
        platform_secrets: Sequence[PlatformSecretDefinition] = (),
    ) -> None:
        # The machine holds no managed Secrets.
        del platform_secrets
        if not local_computer_allowed():
            raise LocalComputerNotAllowed(
                "A computer on the server's own machine needs HOST_MODE=oss, "
                "LANGALPHA_LOCAL_COMPUTER=1 and LANGALPHA_OWNER_SECRET"
            )
        self._config = config
        # A deployment's configured root is a path inside a sandbox image, which
        # means nothing here, and on a Mac cannot exist.
        self._working_dir = (
            working_dir
            if working_dir and working_dir != DEFAULT_SANDBOX_ROOT
            else os.path.join(machine_home(), _ROOT_FOLDER)
        )

    @classmethod
    def reports_own_disk(cls, settings: Any) -> bool:
        return False

    @classmethod
    async def default_root_dir(
        cls, taken: Callable[[], Awaitable[Collection[str]]]
    ) -> str:
        """``~/LangAlpha``, or the next numbered one a computer could take.

        A folder a computer of an earlier install still marks, or one the user
        keeps their own files in, is left as it is rather than refused at the
        first start with no way to pick another. With none free the create is
        refused, since a folder handed out anyway is one a computer already has.
        """
        base = os.path.join(machine_home(), _ROOT_FOLDER)
        roots = await taken()
        for n in range(1, _ROOT_CANDIDATES):
            candidate = base if n == 1 else f"{base}-{n}"
            if candidate not in roots and await root_is_free(candidate):
                return candidate
        raise NoFreeRootFolder(
            f"{base} and its numbered folders up to {base}-{_ROOT_CANDIDATES - 1} "
            "are taken. Give this computer a folder of its own (root_dir)."
        )

    def _runtime(self, sandbox_id: str) -> LocalRuntime:
        return LocalRuntime(
            sandbox_id=sandbox_id,
            working_dir=self._working_dir,
            bootstrap_timeout=self._config.bootstrap_timeout,
        )

    # -- SandboxProvider interface --

    async def create(
        self,
        *,
        env_vars: dict[str, str] | None = None,
        platform_secret_bindings: dict[str, str] | None = None,
        tier: str | None = None,
        auto_stop_minutes: int | None = None,
        **kwargs: Any,
    ) -> LocalRuntime:
        """Lay out a new computer here and install its runtime.

        The machine's size is its own, so ``tier`` is ignored; platform
        secrets and image packages never apply to it.
        """
        runtime = self._runtime(uuid.uuid4().hex)
        await runtime.create(env_vars or {})
        return runtime

    async def get(self, sandbox_id: str) -> LocalRuntime:
        return self._runtime(sandbox_id)

    async def close(self) -> None:
        return None

    def is_transient_error(self, exc: Exception) -> bool:
        failure = connect_failure(exc)
        if failure is not None:
            return not failure.answered
        return isinstance(exc, (SandboxTransientError, TimeoutError))

    def is_host_unavailable(self, exc: Exception) -> bool:
        """Never: the machine answering is what the server is running on."""
        return False

    def classify_error(self, exc: Exception) -> SandboxFailureKind:
        """Never ``SANDBOX_GONE``: the machine is not the server's to replace,
        and a missing computer folder is laid out again by a start."""
        if isinstance(exc, FileNotFoundError):
            return SandboxFailureKind.PATH_ABSENT
        if self.is_transient_error(exc):
            return SandboxFailureKind.TRANSIENT
        return SandboxFailureKind.UNKNOWN
