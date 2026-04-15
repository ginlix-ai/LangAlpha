"""Sandbox backend for deepagents FilesystemMiddleware.

This backend bridges deepagents' BackendProtocol and SandboxBackendProtocol to `PTCSandbox`.

Naming convention:
- Async methods are prefixed with `a` (e.g. `aread`, `aglob_info`).

Design choice:
- We intentionally do not support the synchronous protocol methods here. `PTCSandbox`
  is async-native and depends on an active event loop; sync wrappers tend to be
  brittle and encourage accidental blocking.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any, Callable

import structlog
from deepagents.backends.protocol import EditResult, ExecuteResponse, FileDownloadResponse, FileUploadResponse, WriteResult

from ptc_agent.config.core import SecurityConfig, create_default_security_config  # noqa: F401
from ptc_agent.core.sandbox import PTCSandbox

logger = structlog.get_logger(__name__)

# Type alias for operation callback
OperationCallback = Callable[[dict[str, Any]], None]


class SandboxBackend:
    """deepagents backend implementation backed by `PTCSandbox`."""

    def __init__(
        self,
        sandbox: PTCSandbox,
        root_dir: str | None = None,
        *,
        virtual_mode: bool = True,
        operation_callback: OperationCallback | None = None,
    ) -> None:
        """Create a new SandboxBackend.

        Args:
            sandbox: Initialized `PTCSandbox` instance.
            root_dir: Root directory used when resolving virtual paths.
                      Defaults to ``sandbox.config.filesystem.working_directory``.
            virtual_mode: If True, treat non-absolute paths as relative to `root_dir`.
            operation_callback: Optional callback invoked on file operations (write, edit).
                                Receives a dict with operation details for persistence/logging.
        """
        self.sandbox = sandbox
        self.root_dir = (root_dir or sandbox.config.filesystem.working_directory).rstrip("/")
        self.virtual_mode = virtual_mode
        self.operation_callback = operation_callback
        logger.debug("Initialized SandboxBackend", root_dir=self.root_dir, virtual_mode=self.virtual_mode)

    @property
    def id(self) -> str:
        """Return a stable identifier for this backend instance."""
        return self.sandbox.sandbox_id or "unknown"

    def _normalize_path(self, path: str) -> str:
        """Normalize a path into an absolute sandbox path."""
        if not self.virtual_mode:
            return path

        if path in (None, "", ".", "/"):
            return self.root_dir

        path = path.strip()

        # Already absolute in sandbox (working directory or /tmp)
        if path.startswith((self.root_dir, "/tmp")):
            return path

        if path.startswith("/"):
            return f"{self.root_dir}{path}"

        return f"{self.root_dir}/{path}"

    def _format_cat_n(self, lines: list[str], *, start_line_number: int) -> str:
        """Format lines using `cat -n`-style numbering."""
        return "\n".join(f"{i:6}\t{line}" for i, line in enumerate(lines, start=start_line_number))

    def _invoke_operation_callback(self, operation: str, file_path: str, **kwargs: Any) -> None:
        """Invoke the operation callback if configured.

        Args:
            operation: The operation type (e.g., "write_file", "edit_file").
            file_path: The normalized file path.
            **kwargs: Additional operation-specific data.
        """
        if self.operation_callback is None:
            return

        try:
            self.operation_callback({
                "operation": operation,
                "file_path": file_path,
                "timestamp": datetime.now(UTC).isoformat(),
                **kwargs,
            })
        except Exception:
            logger.exception("Failed to invoke operation callback", operation=operation, file_path=file_path)

    # ---------------------------------------------------------------------
    # Sync protocol methods (unsupported)
    # ---------------------------------------------------------------------

    def ls_info(self, path: str = ".") -> list[dict]:  # pragma: no cover
        """List directory contents (sync).

        Raises:
            RuntimeError: Always, because this backend is async-native.
        """
        raise RuntimeError("SandboxBackend is async-native; use als_info()")

    def read(self, file_path: str, offset: int = 0, limit: int = 2000) -> str:  # pragma: no cover
        """Read a file (sync).

        Raises:
            RuntimeError: Always, because this backend is async-native.
        """
        raise RuntimeError("SandboxBackend is async-native; use aread()")

    def write(self, file_path: str, content: str) -> WriteResult:  # pragma: no cover
        """Write a file (sync).

        Raises:
            RuntimeError: Always, because this backend is async-native.
        """
        raise RuntimeError("SandboxBackend is async-native; use awrite()")

    def edit(self, file_path: str, old_string: str, new_string: str, *, replace_all: bool = False) -> EditResult:  # pragma: no cover
        """Edit a file (sync).

        Raises:
            RuntimeError: Always, because this backend is async-native.
        """
        raise RuntimeError("SandboxBackend is async-native; use aedit()")

    def grep_raw(self, pattern: str, path: str | None = None, glob: str | None = None) -> list[dict] | str:  # pragma: no cover
        """Search for pattern matches (sync).

        Raises:
            RuntimeError: Always, because this backend is async-native.
        """
        raise RuntimeError("SandboxBackend is async-native; use agrep_raw()")

    def glob_info(self, pattern: str, path: str = "/") -> list[dict]:  # pragma: no cover
        """Return glob matches (sync).

        Raises:
            RuntimeError: Always, because this backend is async-native.
        """
        raise RuntimeError("SandboxBackend is async-native; use aglob_info()")

    def upload_files(self, files: list[tuple[str, bytes]]) -> list[FileUploadResponse]:  # pragma: no cover
        """Upload files (sync).

        Raises:
            RuntimeError: Always, because this backend is async-native.
        """
        raise RuntimeError("SandboxBackend is async-native; use aupload_files()")

    def download_files(self, paths: list[str]) -> list[FileDownloadResponse]:  # pragma: no cover
        """Download files (sync).

        Raises:
            RuntimeError: Always, because this backend is async-native.
        """
        raise RuntimeError("SandboxBackend is async-native; use adownload_files()")

    def execute(self, command: str) -> ExecuteResponse:  # pragma: no cover
        """Execute a shell command (sync).

        Raises:
            RuntimeError: Always, because this backend is async-native.
        """
        raise RuntimeError("SandboxBackend is async-native; use aexecute()")

    # ---------------------------------------------------------------------
    # Async protocol methods
    # ---------------------------------------------------------------------

    async def als_info(self, path: str = ".") -> list[dict]:
        """Async directory listing."""
        normalized_path = self._normalize_path(path)
        entries = await self.sandbox.als_directory(normalized_path)
        return [{"path": e.get("path", ""), "is_dir": bool(e.get("is_dir", False))} for e in entries]

    async def aread(self, file_path: str, offset: int = 0, limit: int = 2000) -> str:
        """Async file read in cat -n format."""
        normalized_path = self._normalize_path(file_path)
        content = await self.sandbox.aread_file_text(normalized_path)
        if content is None:
            return f"Error: File '{file_path}' not found"

        lines = content.splitlines()
        window = lines[offset : offset + limit]
        return self._format_cat_n(window, start_line_number=offset + 1)

    async def awrite(self, file_path: str, content: str) -> WriteResult:
        """Async file write (overwrite)."""
        normalized_path = self._normalize_path(file_path)
        ok = await self.sandbox.awrite_file_text(normalized_path, content)
        if ok:
            self._invoke_operation_callback(
                "write_file",
                normalized_path,
                line_count=content.count("\n") + 1,
                content=content,
            )
            return WriteResult(path=normalized_path, files_update=None)
        return WriteResult(error=f"Failed to write to '{normalized_path}'")

    async def aedit(self, file_path: str, old_string: str, new_string: str, *, replace_all: bool = False) -> EditResult:
        """Async exact-string edit."""
        normalized_path = self._normalize_path(file_path)
        result = await self.sandbox.aedit_file_text(
            normalized_path,
            old_string,
            new_string,
            replace_all=replace_all,
        )
        if result.get("success"):
            occurrences = int(result.get("occurrences", 1))
            content = None
            if self.operation_callback:
                content = await self.sandbox.aread_file_text(normalized_path)
            self._invoke_operation_callback(
                "edit_file",
                normalized_path,
                occurrences=occurrences,
                replace_all=replace_all,
                old_string=old_string,
                new_string=new_string,
                content=content,
            )
            return EditResult(path=normalized_path, files_update=None, occurrences=occurrences)
        return EditResult(error=str(result.get("error", "Edit failed")))

    def _parse_grep_matches(self, raw: Any) -> list[dict]:
        """Parse sandbox grep output into deepagents GrepMatch dicts."""
        if not raw:
            return []

        matches: list[dict] = []
        raw_items: list[Any]
        if isinstance(raw, str):
            raw_items = [line for line in raw.strip().split("\n") if line]
        elif isinstance(raw, list):
            raw_items = raw
        else:
            return []

        for item in raw_items:
            if not item:
                continue
            if isinstance(item, dict):
                matches.append(
                    {
                        "path": item.get("path", ""),
                        "line": int(item.get("line", 0) or 0),
                        "text": item.get("text", ""),
                    }
                )
                continue

            if isinstance(item, str) and ":" in item:
                parts = item.split(":", 2)
                if len(parts) >= 3:
                    try:
                        line_no = int(parts[1])
                        text = parts[2]
                    except ValueError:
                        line_no = 0
                        text = ":".join(parts[1:])
                    matches.append({"path": parts[0], "line": line_no, "text": text})
        return matches

    async def agrep_raw(self, pattern: str, path: str | None = None, glob: str | None = None) -> list[dict] | str:
        """Async grep using `PTCSandbox.agrep_content` and return structured matches."""
        search_path = self._normalize_path(path) if path else self.root_dir
        raw = await self.sandbox.agrep_content(
            pattern=pattern,
            path=search_path,
            output_mode="content",
            glob=glob,
            show_line_numbers=True,
        )
        return self._parse_grep_matches(raw)

    async def aglob_info(self, pattern: str, path: str = "/") -> list[dict]:
        """Async glob returning FileInfo dicts."""
        normalized_path = self._normalize_path(path)
        file_paths = await self.sandbox.aglob_files(pattern, normalized_path)
        return [{"path": fp} for fp in file_paths]

    async def adownload_files(self, paths: list[str]) -> list[FileDownloadResponse]:
        """Async batch download."""

        async def _download_one(p: str) -> FileDownloadResponse:
            normalized = self._normalize_path(p)
            try:
                content = await self.sandbox.adownload_file_bytes(normalized)
                if content is None:
                    return FileDownloadResponse(path=p, error="file_not_found")
                return FileDownloadResponse(path=p, content=content)
            except Exception:
                logger.exception("Failed to download file", path=p)
                return FileDownloadResponse(path=p, error="file_not_found")

        return await asyncio.gather(*[_download_one(p) for p in paths])

    async def aupload_files(self, files: list[tuple[str, bytes]]) -> list[FileUploadResponse]:
        """Async batch upload."""

        async def _upload_one(path: str, content: bytes) -> FileUploadResponse:
            normalized = self._normalize_path(path)
            try:
                ok = await self.sandbox.aupload_file_bytes(normalized, content)
                if ok:
                    return FileUploadResponse(path=path)
                return FileUploadResponse(path=path, error="permission_denied")
            except Exception:
                logger.exception("Failed to upload file", path=path)
                return FileUploadResponse(path=path, error="permission_denied")

        return await asyncio.gather(*[_upload_one(p, c) for p, c in files])

    async def aexecute(self, command: str) -> ExecuteResponse:
        """Execute a shell command in the sandbox."""
        try:
            res = await self.sandbox.execute_bash_command(command=command, working_dir=self.root_dir, timeout=60)
            output = (res.get("stdout") or "") + (res.get("stderr") or "")
            exit_code = int(res.get("exit_code") or 0)
            return ExecuteResponse(output=output, exit_code=exit_code, truncated=False)
        except Exception as e:
            logger.exception("Failed to execute command")
            return ExecuteResponse(output=str(e), exit_code=1, truncated=False)
