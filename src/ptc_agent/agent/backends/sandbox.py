"""Sandbox backend bridging `PTCSandbox` to deepagents `SandboxBackendProtocol`.

Adapter-extension pattern:
- Conforms to `SandboxBackendProtocol` so deepagents middleware can consume us.
- Extends with langalpha-specific rich methods (added in Phase 3A) that tool
  factories call directly, preserving all PTCSandbox capabilities without
  squeezing through the narrow protocol surface.

Intentional divergences from the protocol (documented once, enforced everywhere):

- `awrite(path, content, *, overwrite=False)` — default matches protocol
  (create-only). Middleware passes `overwrite=True` explicitly at each callsite.
- `agrep(pattern, ...)` returns regex matches (PTCSandbox uses `rg` without
  `-F`). The protocol contract is literal. Langalpha agent prompts assume regex.
- Error classification is lossy. PTCSandbox collapses not-found / decode-failure
  / permission-denied into `None` or `[]`. We use `FileOperationError` literals
  only where PTCSandbox makes the distinction; fall back to `str(exc)` otherwise.

Sync protocol methods are not implemented here — langalpha is async end-to-end,
and `PTCSandbox` requires an event loop. Calls fall through to the protocol
base class defaults, which raise `NotImplementedError`.
"""

from __future__ import annotations

import asyncio
import shlex
from datetime import UTC, datetime
from typing import Any, Callable, cast

import structlog
from deepagents.backends.protocol import (
    EditResult,
    ExecuteResponse,
    FileData,
    FileDownloadResponse,
    FileInfo,
    FileUploadResponse,
    GlobResult,
    GrepMatch,
    GrepResult,
    LsResult,
    ReadResult,
    SandboxBackendProtocol,
    WriteResult,
)

from ptc_agent.core.sandbox import ExecutionResult, PTCSandbox
from ptc_agent.core.sandbox.runtime import PreviewInfo

logger = structlog.get_logger(__name__)

# Type alias for operation callback
OperationCallback = Callable[[dict[str, Any]], None]

# Seconds to wait for the create-only existence pre-check (`test -e`). Generous
# because cold-start sandboxes can take a few seconds to respond on first call.
_FILE_EXISTS_TIMEOUT_SECONDS = 10


class SandboxBackend(SandboxBackendProtocol):
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

    def _invoke_operation_callback(self, operation: str, file_path: str, **kwargs: Any) -> None:
        """Invoke the operation callback if configured."""
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

    async def _areserve_new_file(self, normalized_path: str) -> bool | None:
        """Atomically create an empty file if it does not exist.

        Uses POSIX `set -C` (noclobber) with `>` redirection so the
        create-check and create-empty happen in a single shell roundtrip.
        Concurrent callers racing on the same path both run `set -C; > path`;
        exactly one wins (exit 0) and the others see the winner's empty file
        and fail (exit != 0). This gives us atomic create-only semantics for
        the existence check; the content write that follows is a separate
        roundtrip but any concurrent writer has already failed their own
        reservation and cannot clobber us.

        Returns:
            True if the reservation succeeded (file is now ours to write),
            False if the file already existed (caller should return error),
            None if the bash command failed for any other reason.
        """
        try:
            res = await self.sandbox.execute_bash_command(
                command=f"set -C; > {shlex.quote(normalized_path)}",
                working_dir=self.root_dir,
                timeout=_FILE_EXISTS_TIMEOUT_SECONDS,
            )
        except Exception:
            logger.debug("File reservation failed", path=normalized_path, exc_info=True)
            return None
        exit_code = int(res.get("exit_code") or 0)
        return exit_code == 0 if exit_code in (0, 1) else None

    # ---------------------------------------------------------------------
    # Async protocol methods
    # ---------------------------------------------------------------------

    async def als(self, path: str = ".") -> LsResult:
        """List directory contents as `LsResult`."""
        normalized_path = self._normalize_path(path)
        try:
            entries = await self.sandbox.als_directory(normalized_path)
        except Exception as exc:
            logger.debug("als failed", path=path, error=str(exc))
            return LsResult(error=str(exc))

        file_infos: list[FileInfo] = [
            cast(FileInfo, {
                "path": e.get("path", ""),
                "is_dir": bool(e.get("is_dir", False)),
            })
            for e in entries
        ]
        return LsResult(entries=file_infos)

    async def aread(self, file_path: str, offset: int = 0, limit: int = 2000) -> ReadResult:
        """Read a UTF-8 text file, returning a `ReadResult`.

        Uses `PTCSandbox.aread_file_range` (server-side sed) to avoid
        downloading the full file when only a slice is requested.
        """
        normalized_path = self._normalize_path(file_path)
        try:
            content = await self.sandbox.aread_file_range(normalized_path, offset, limit)
        except Exception as exc:
            logger.debug("aread failed", path=file_path, error=str(exc))
            return ReadResult(error=str(exc))

        if content is None:
            return ReadResult(error="file_not_found")

        file_data: FileData = cast(FileData, {"content": content, "encoding": "utf-8"})
        return ReadResult(file_data=file_data)

    async def awrite(
        self,
        file_path: str,
        content: str,
        *,
        overwrite: bool = False,
    ) -> WriteResult:
        """Write a file.

        The protocol contract is create-only (error if exists). Langalpha
        middleware legitimately needs to rewrite by-id paths (eviction,
        compaction), so we accept `overwrite: bool = False` with a
        protocol-conformant default. Middleware passes `overwrite=True`
        explicitly at each callsite, making the divergence visible.

        When `overwrite=False`, the existence check is atomic via POSIX
        `set -C` noclobber (see `_areserve_new_file`). The subsequent
        content write is a separate sandbox roundtrip, so a concurrent
        write could leave an empty reservation without content — but it
        cannot cause silent clobbering, since any racing writer loses the
        reservation race and returns "already exists".
        """
        normalized_path = self._normalize_path(file_path)

        if not overwrite:
            reserved = await self._areserve_new_file(normalized_path)
            if reserved is False:
                return WriteResult(error=f"File '{file_path}' already exists")
            if reserved is None:
                return WriteResult(error=f"Failed to reserve '{normalized_path}'")

        try:
            ok = await self.sandbox.awrite_file_text(normalized_path, content)
        except Exception as exc:
            # A sandbox failure has to come back as a tool error, like every
            # other method here. The large-result eviction middleware writes
            # through this path from *outside* the tool-error middleware, so an
            # exception escaping here is never caught and kills the whole turn.
            logger.exception("Failed to write file", path=normalized_path)
            return WriteResult(error=f"write_failed: {exc}")
        if not ok:
            return WriteResult(error=f"Failed to write to '{normalized_path}'")

        self._invoke_operation_callback(
            "write_file",
            normalized_path,
            line_count=content.count("\n") + 1,
            content=content,
        )
        return WriteResult(path=normalized_path)

    async def aedit(
        self,
        file_path: str,
        old_string: str,
        new_string: str,
        replace_all: bool = False,
    ) -> EditResult:
        """Edit a file by exact-string replacement."""
        normalized_path = self._normalize_path(file_path)
        try:
            result = await self.sandbox.aedit_file_text(
                normalized_path,
                old_string,
                new_string,
                replace_all=replace_all,
            )
        except Exception as exc:
            logger.exception("Failed to edit file", path=normalized_path)
            return EditResult(error=f"edit_failed: {exc}")
        if not result.get("success"):
            return EditResult(error=str(result.get("error", "Edit failed")))

        occurrences = int(result.get("occurrences", 1))
        content = None
        if self.operation_callback:
            try:
                content = await self.sandbox.aread_file_text(normalized_path)
            except Exception:
                # The edit is already committed. Now that a read raises instead
                # of returning None, letting this escape would fail the whole
                # tool call over a snapshot the callback can do without.
                logger.warning(
                    "Could not read back edited file for the operation callback",
                    path=normalized_path,
                )
        self._invoke_operation_callback(
            "edit_file",
            normalized_path,
            occurrences=occurrences,
            replace_all=replace_all,
            old_string=old_string,
            new_string=new_string,
            content=content,
        )
        return EditResult(path=normalized_path, occurrences=occurrences)

    def _parse_grep_matches(self, raw: Any) -> list[GrepMatch]:
        """Parse sandbox grep output into deepagents `GrepMatch` TypedDicts."""
        if not raw:
            return []

        matches: list[GrepMatch] = []
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
                    cast(GrepMatch, {
                        "path": item.get("path", ""),
                        "line": int(item.get("line", 0) or 0),
                        "text": item.get("text", ""),
                    })
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
                    matches.append(cast(GrepMatch, {"path": parts[0], "line": line_no, "text": text}))
        return matches

    async def agrep(
        self,
        pattern: str,
        path: str | None = None,
        glob: str | None = None,
    ) -> GrepResult:
        """Search file contents.

        **Divergence from protocol**: regex semantics (PTCSandbox runs `rg`
        without `-F`). Langalpha agent prompts rely on regex throughout;
        conforming to literal-only would break existing behavior. Callers that
        need literal matching can regex-escape their pattern.
        """
        search_path = self._normalize_path(path) if path else self.root_dir
        try:
            raw = await self.sandbox.agrep_content(
                pattern=pattern,
                path=search_path,
                output_mode="content",
                glob=glob,
                show_line_numbers=True,
            )
        except Exception as exc:
            logger.debug("agrep failed", pattern=pattern, error=str(exc))
            return GrepResult(error=str(exc))

        return GrepResult(matches=self._parse_grep_matches(raw))

    async def aglob(self, pattern: str, path: str = "/") -> GlobResult:
        """Return files matching `pattern` under `path`."""
        normalized_path = self._normalize_path(path)
        try:
            file_paths = await self.sandbox.aglob_files(pattern, normalized_path)
        except Exception as exc:
            logger.debug("aglob failed", pattern=pattern, error=str(exc))
            return GlobResult(error=str(exc))

        file_infos: list[FileInfo] = [cast(FileInfo, {"path": fp}) for fp in file_paths]
        return GlobResult(matches=file_infos)

    async def adownload_files(self, paths: list[str]) -> list[FileDownloadResponse]:
        """Batch-download files from the sandbox."""

        async def _download_one(p: str) -> FileDownloadResponse:
            normalized = self._normalize_path(p)
            try:
                content = await self.sandbox.adownload_file_bytes(normalized)
                if content is None:
                    return FileDownloadResponse(path=p, error="file_not_found")
                return FileDownloadResponse(path=p, content=content)
            except Exception as exc:
                # Never report a sandbox failure as file_not_found: the agent
                # reads that as "this file does not exist" and moves on, quietly
                # building on missing data instead of retrying.
                logger.exception("Failed to download file", path=p)
                return FileDownloadResponse(path=p, error=f"read_failed: {exc}")

        return await asyncio.gather(*[_download_one(p) for p in paths])

    async def aupload_files(self, files: list[tuple[str, bytes]]) -> list[FileUploadResponse]:
        """Batch-upload files to the sandbox."""

        async def _upload_one(path: str, content: bytes) -> FileUploadResponse:
            normalized = self._normalize_path(path)
            try:
                ok = await self.sandbox.aupload_file_bytes(normalized, content)
                if ok:
                    return FileUploadResponse(path=path)
                return FileUploadResponse(path=path, error="permission_denied")
            except Exception as exc:
                # Mirrors the download path: never report a sandbox failure as a
                # per-path verdict. "permission_denied" reads to the agent as
                # "this path is not writable", so it stops trying instead of
                # retrying something that was only ever temporarily unreachable.
                logger.exception("Failed to upload file", path=path)
                return FileUploadResponse(path=path, error=f"write_failed: {exc}")

        return await asyncio.gather(*[_upload_one(p, c) for p, c in files])

    async def aexecute(self, command: str, *, timeout: int | None = None) -> ExecuteResponse:
        """Execute a shell command in the sandbox.

        `timeout=None` uses PTCSandbox's built-in default (60s); we omit the
        kwarg rather than passing `None` since `execute_bash_command` expects
        `int`, not `int | None`.
        """
        kwargs: dict[str, Any] = {"command": command, "working_dir": self.root_dir}
        if timeout is not None:
            kwargs["timeout"] = timeout

        try:
            res = await self.sandbox.execute_bash_command(**kwargs)
            output = (res.get("stdout") or "") + (res.get("stderr") or "")
            exit_code = int(res.get("exit_code") or 0)
            return ExecuteResponse(output=output, exit_code=exit_code, truncated=False)
        except Exception as e:
            logger.exception("Failed to execute command")
            return ExecuteResponse(output=str(e), exit_code=1, truncated=False)

    # ---------------------------------------------------------------------
    # Langalpha-specific rich methods (not part of SandboxBackendProtocol).
    #
    # Tool factories call these directly to preserve PTCSandbox features the
    # protocol's minimal surface doesn't expose (rich grep options, background
    # bash, native Python code execution, path helpers, preview URL). Middleware
    # only uses the protocol methods above. All rich methods are thin
    # passthroughs — this class is an adapter, not an interface redesign.
    # ---------------------------------------------------------------------

    # --- Path helpers (sync, pure delegation) ---

    def normalize_path(self, path: str) -> str:
        """Convert a virtual/relative path to an absolute sandbox path."""
        return self.sandbox.normalize_path(path)

    def virtualize_path(self, path: str) -> str:
        """Strip the working-directory prefix to produce an agent-visible path."""
        return self.sandbox.virtualize_path(path)

    def validate_path(self, path: str) -> bool:
        """Return True if `path` is within the allowed directories."""
        return self.sandbox.validate_path(path)

    # --- Sandbox properties ---

    @property
    def filesystem_config(self) -> Any:
        """Return the sandbox's filesystem config (allowed dirs, working dir, validation flag)."""
        return self.sandbox.config.filesystem

    @property
    def sandbox_id(self) -> str | None:
        """Return the raw sandbox ID attribute (may be None if not yet initialized)."""
        return self.sandbox.sandbox_id

    @property
    def skills_manifest(self) -> dict[str, Any] | None:
        """Return the sandbox's skills manifest, if loaded."""
        return self.sandbox.skills_manifest

    # --- File ops (rich, native PTCSandbox return types) ---

    async def aread_range(
        self, file_path: str, offset: int = 0, limit: int = 2000
    ) -> str | None:
        """Read a line range server-side via sed. Returns `None` if file missing."""
        normalized = self.normalize_path(file_path)
        return await self.sandbox.aread_file_range(normalized, offset, limit)

    async def aread_text(self, file_path: str) -> str | None:
        """Read a full file as UTF-8 text. Returns `None` on not-found / decode failure."""
        normalized = self.normalize_path(file_path)
        return await self.sandbox.aread_file_text(normalized)

    async def awrite_text(self, file_path: str, content: str) -> bool:
        """Write UTF-8 text to a file (overwrite). Returns True on success."""
        normalized = self.normalize_path(file_path)
        return await self.sandbox.awrite_file_text(normalized, content)

    async def aedit_text(
        self,
        file_path: str,
        old_string: str,
        new_string: str,
        *,
        replace_all: bool = False,
    ) -> dict[str, Any]:
        """Edit by exact-string replacement. Returns PTCSandbox's raw result dict."""
        normalized = self.normalize_path(file_path)
        return await self.sandbox.aedit_file_text(
            normalized, old_string, new_string, replace_all=replace_all
        )

    # --- Search (rich options) ---

    async def agrep_rich(
        self,
        pattern: str,
        path: str = ".",
        output_mode: str = "files_with_matches",
        glob: str | None = None,
        type: str | None = None,  # noqa: A002 — matches ripgrep's --type flag
        *,
        case_insensitive: bool = False,
        show_line_numbers: bool = True,
        lines_after: int | None = None,
        lines_before: int | None = None,
        lines_context: int | None = None,
        multiline: bool = False,
        head_limit: int | None = None,
        offset: int = 0,
    ) -> Any:
        """Search with all PTCSandbox grep options (output_mode, type, context, pagination).

        Used by the Grep tool to preserve its full feature set. The
        protocol-surface `agrep` above is a thin adapter that only exposes
        (pattern, path, glob) per the deepagents contract.
        """
        return await self.sandbox.agrep_content(
            pattern=pattern,
            path=path,
            output_mode=output_mode,
            glob=glob,
            type=type,
            case_insensitive=case_insensitive,
            show_line_numbers=show_line_numbers,
            lines_after=lines_after,
            lines_before=lines_before,
            lines_context=lines_context,
            multiline=multiline,
            head_limit=head_limit,
            offset=offset,
        )

    async def aglob_paths(self, pattern: str, path: str = ".") -> list[str]:
        """Return glob matches as a flat list of paths (no dataclass wrapper)."""
        return await self.sandbox.aglob_files(pattern, path)

    # --- Execution (bash + Python code) ---

    async def aexecute_bash(
        self,
        command: str,
        working_dir: str | None = None,
        timeout: int = 60,
        *,
        background: bool = False,
        thread_id: str | None = None,
    ) -> dict[str, Any]:
        """Run a bash command with full PTCSandbox options (working_dir, background, thread_id).

        Used by the Bash tool. Returns PTCSandbox's raw result dict.
        """
        return await self.sandbox.execute_bash_command(
            command=command,
            working_dir=working_dir,
            timeout=timeout,
            background=background,
            thread_id=thread_id,
        )

    async def astop_background_command(self, command_id: str) -> bool:
        """Stop a previously started background bash command."""
        return await self.sandbox.stop_background_command(command_id)

    async def aget_background_command_status(self, command_id: str) -> dict[str, Any]:
        """Return status + buffered output for a background bash command."""
        return await self.sandbox.get_background_command_status(command_id)

    async def aexecute_code(
        self,
        code: str,
        *,
        thread_id: str | None = None,
    ) -> ExecutionResult:
        """Execute Python code in the sandbox (Jupyter-style).

        Distinct from `aexecute` (shell) — returns rich `ExecutionResult`
        with stdout/charts/mcp_trace.
        """
        return await self.sandbox.execute(code, thread_id=thread_id)

    # --- File transfer (single-file helpers used by ShowWidget) ---

    async def adownload_file_bytes(self, file_path: str) -> bytes | None:
        """Download a single file's bytes. Returns `None` on not-found."""
        normalized = self.normalize_path(file_path)
        return await self.sandbox.adownload_file_bytes(normalized)

    # --- Preview URL (Daytona proxy) ---

    async def astart_preview_url(
        self,
        command: str,
        port: int,
        *,
        expires_in: int = 3600,
        startup_timeout: float = 10.0,
    ) -> PreviewInfo:
        """Start a server command in the sandbox and return a signed preview URL."""
        return await self.sandbox.start_and_get_preview_url(
            command,
            port,
            expires_in=expires_in,
            startup_timeout=startup_timeout,
        )
