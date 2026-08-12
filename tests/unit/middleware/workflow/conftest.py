"""Fakes shared by the workflow-orchestration suite.

Plain classes and functions, imported via ``from .conftest import ...`` —
the pattern the mcp_servers suite already uses. What lives here is what more
than one module needs to agree on; fakes that only look similar stay in the
module that owns them.
"""

from __future__ import annotations

from types import SimpleNamespace


def workflow_script(
    body: str, *, name: str = "test", description: str = "test workflow"
) -> str:
    """Wrap a body in the minimal ``meta`` header ``compile_check`` accepts."""
    return (
        f"export const meta = {{ name: '{name}', description: '{description}' }};\n"
        f"{body}"
    )


class FakeBackend:
    """The filesystem seam: scripts are read from ``files``, run artifacts land
    in ``writes``. ``fail_writes`` covers a read-only or full sandbox."""

    def __init__(
        self, files: dict[str, str] | None = None, *, fail_writes: bool = False
    ) -> None:
        self.files = dict(files or {})
        self.fail_writes = fail_writes
        self.writes: dict[str, str] = {}

    async def aread_text(self, path: str) -> str:
        if path not in self.files:
            raise FileNotFoundError(path)
        return self.files[path]

    async def awrite_text(self, path: str, content: str) -> bool:
        # Returns the real backends' success bool, not None: the driver treats
        # a falsy write as "did not land", so a fake that shrugs would make
        # every artifact look lost.
        if self.fail_writes:
            raise OSError("read only")
        self.writes[path] = content
        return True


class FakeSandbox:
    """Path translation for the workflow mounts.

    Shared rather than copied because the copies drifted: ``lstrip('./')`` here
    strips a character *set*, so it ate the leading dot of the real mount
    (``.agents/…`` → ``agents/…``) in both of them at once.
    """

    root_dir = "/home/workspace"
    filesystem_config = SimpleNamespace()

    def normalize_path(self, path: str) -> str:
        if path.startswith("/"):
            return path
        return f"{self.root_dir}/{path.removeprefix('./')}"

    def virtualize_path(self, path: str) -> str:
        return path.removeprefix(f"{self.root_dir}/")

    def validate_path(self, path: str) -> bool:
        return True
