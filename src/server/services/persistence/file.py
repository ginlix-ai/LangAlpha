"""
File Persistence Service.

Syncs workspace files between Daytona sandboxes and PostgreSQL.

- Snapshots files to DB on workspace stop/delete
- Restores files to sandbox when sandbox is recreated
- Serves file metadata/content from DB when sandbox is stopped
"""

import asyncio
import hashlib
import logging
import mimetypes
import os
import shlex
from datetime import datetime, timezone
from typing import Any

from ptc_agent.core.paths import BACKUP_EXCLUDE_AGENT_SUBDIRS, BACKUP_EXCLUDE_DIRS, ALWAYS_HIDDEN_DIR_NAMES, HIDDEN_DIR_NAMES
from src.server.database.workspace_file import (
    bulk_update_file_mtimes,
    bulk_upsert_files,
    delete_removed_files,
    get_file as db_get_file,
    get_file_metadata_for_sync,
    get_files_for_workspace,
    get_workspace_total_size,
    update_file_mtime,
)

logger = logging.getLogger(__name__)

# Known binary file extensions (reused from workspace_files.py)
_BINARY_EXTENSIONS = frozenset(
    {
        ".pdf",
        ".png",
        ".jpg",
        ".jpeg",
        ".gif",
        ".webp",
        ".bmp",
        ".ico",
        ".tiff",
        ".zip",
        ".tar",
        ".gz",
        ".bz2",
        ".7z",
        ".rar",
        ".exe",
        ".dll",
        ".so",
        ".dylib",
        ".mp3",
        ".mp4",
        ".wav",
        ".avi",
        ".mov",
        ".mkv",
        ".doc",
        ".docx",
        ".xls",
        ".xlsx",
        ".ppt",
        ".pptx",
        ".sqlite",
        ".db",
        ".pickle",
        ".pkl",
    }
)

def _sync_marker_path(work_dir: str) -> str:
    """Return the sync marker file path for the given working directory."""
    return f"{work_dir}/.file_sync_marker"


def _is_binary_extension(file_path: str) -> bool:
    """Check if file extension indicates binary content."""
    _, ext = os.path.splitext(file_path)
    return ext.lower() in _BINARY_EXTENSIONS


def _detect_is_binary(file_path: str, content: bytes) -> bool:
    """Detect whether file content is binary."""
    if _is_binary_extension(file_path):
        return True
    sample = content[:8192]
    if b"\x00" in sample:
        return True
    try:
        sample.decode("utf-8")
        return False
    except UnicodeDecodeError:
        return True


class FilePersistenceService:
    """Sync workspace files between Daytona sandbox and PostgreSQL."""

    MAX_FILE_SIZE = 100 * 1024 * 1024  # 100MB per file
    MAX_WORKSPACE_SIZE = 1024 * 1024 * 1024  # 1GB total per workspace

    # Directories excluded from sync (relative to /home/workspace/).
    # All patterns match at any depth via find's -not -path '*/{d}/*'.
    EXCLUDE_DIRS = BACKUP_EXCLUDE_DIRS | HIDDEN_DIR_NAMES | ALWAYS_HIDDEN_DIR_NAMES

    # File extensions to exclude
    EXCLUDE_EXTENSIONS = {".pyc", ".pyo", ".so", ".dylib", ".o"}

    # Basenames to exclude
    EXCLUDE_BASENAMES = {".DS_Store", "Thumbs.db", "__init__.py"}

    @classmethod
    async def list_sandbox_files(cls, sandbox: Any) -> dict[str, dict[str, Any]]:
        """List user files in sandbox with metadata.

        Returns:
            Dict mapping virtual_path to {abs_path, file_name, file_size, mtime}.
            Empty dict if no files found.
        """
        work_dir = sandbox.working_dir
        work_dir_prefix = work_dir + "/"
        sync_marker = _sync_marker_path(work_dir)

        exclude_flags = []
        for d in cls.EXCLUDE_DIRS:
            exclude_flags.append(f"-not -path '*/{d}/*'")
        for subdir in BACKUP_EXCLUDE_AGENT_SUBDIRS:
            exclude_flags.append(f"-not -path '*/{subdir}/*'")
        for ext in cls.EXCLUDE_EXTENSIONS:
            exclude_flags.append(f"-not -name '*{ext}'")
        for name in cls.EXCLUDE_BASENAMES:
            exclude_flags.append(f"-not -name '{name}'")

        find_cmd = (
            f"find {work_dir} -type f "
            f"{' '.join(exclude_flags)} "
            f"-printf '%s\\t%T@\\t%p\\n' 2>/dev/null"
        )

        find_result = await sandbox.execute_bash_command(find_cmd, timeout=30)
        if not find_result.get("success"):
            raise RuntimeError(
                f"Failed to list sandbox files: {find_result.get('stderr', 'unknown error')}"
            )
        if not find_result.get("stdout", "").strip():
            return {}

        result: dict[str, dict[str, Any]] = {}
        for line in find_result["stdout"].strip().split("\n"):
            parts = line.split("\t", 2)
            if len(parts) != 3:
                continue

            size_str, mtime_str, abs_path = parts
            try:
                file_size = int(size_str)
            except ValueError:
                continue

            if file_size > cls.MAX_FILE_SIZE:
                continue

            virtual_path = abs_path
            if virtual_path.startswith(work_dir_prefix):
                virtual_path = virtual_path[len(work_dir_prefix):]
            elif virtual_path == work_dir:
                continue

            if abs_path == sync_marker:
                continue

            try:
                mtime = float(mtime_str)
            except ValueError:
                mtime = 0.0

            result[virtual_path] = {
                "abs_path": abs_path,
                "file_name": os.path.basename(abs_path),
                "file_size": file_size,
                "mtime": mtime,
            }

        return result

    @classmethod
    async def _compute_sandbox_hashes(
        cls, sandbox: Any, file_paths: list[str]
    ) -> dict[str, str]:
        """Compute SHA-256 hashes on the sandbox to avoid downloading unchanged files.

        Args:
            sandbox: Sandbox instance
            file_paths: List of absolute paths on the sandbox

        Returns:
            Dict mapping abs_path to hex hash. Missing entries mean the file
            could not be hashed (deleted, permission error, etc.).
        """
        if not file_paths:
            return {}

        batch_size = 200
        batches = [
            file_paths[i : i + batch_size]
            for i in range(0, len(file_paths), batch_size)
        ]

        async def _run_batch(paths: list[str]) -> dict[str, str]:
            quoted = " ".join(shlex.quote(p) for p in paths)
            cmd = f"sha256sum {quoted} 2>/dev/null"
            res = await sandbox.execute_bash_command(cmd, timeout=30)
            result: dict[str, str] = {}
            if not res.get("success") or not res.get("stdout", "").strip():
                return result
            for line in res["stdout"].strip().split("\n"):
                parts = line.split(None, 1)
                if len(parts) == 2:
                    hex_hash, path = parts
                    result[path] = hex_hash
            return result

        try:
            batch_results = await asyncio.gather(
                *[_run_batch(b) for b in batches], return_exceptions=True
            )
            merged: dict[str, str] = {}
            for br in batch_results:
                if isinstance(br, dict):
                    merged.update(br)
            return merged
        except Exception as e:
            logger.warning(f"Sandbox hash computation failed: {e}")
            return {}

    @classmethod
    async def sync_to_db(cls, workspace_id: str, sandbox: Any) -> dict[str, Any]:
        """
        Snapshot workspace files from sandbox to PostgreSQL.

        Args:
            workspace_id: Workspace UUID
            sandbox: Sandbox instance with file access methods

        Returns:
            Sync result summary
        """
        result = {"synced": 0, "skipped": 0, "deleted": 0, "errors": 0, "total_size": 0}

        try:
            # 1. List files in sandbox
            sandbox_meta = await cls.list_sandbox_files(sandbox)

            if not sandbox_meta:
                logger.info(f"No files found for workspace {workspace_id}")
                deleted = await delete_removed_files(workspace_id, set())
                result["deleted"] = deleted
                return result

            sandbox_files = [
                {"virtual_path": vp, **info} for vp, info in sandbox_meta.items()
            ]
            total_size = sum(f["file_size"] for f in sandbox_files)

            # Check total workspace size limit
            if total_size > cls.MAX_WORKSPACE_SIZE:
                logger.warning(
                    f"Workspace {workspace_id} total size ({total_size}) exceeds limit "
                    f"({cls.MAX_WORKSPACE_SIZE}). Syncing anyway but this may be slow."
                )

            # 3. Get existing metadata from DB for incremental sync
            existing_meta = await get_file_metadata_for_sync(workspace_id)
            active_paths: set[str] = set()

            # 4. Pre-filter: skip files where size + mtime are unchanged
            changed_files: list[dict[str, Any]] = []
            for file_info in sandbox_files:
                virtual_path = file_info["virtual_path"]
                active_paths.add(virtual_path)

                db_meta = existing_meta.get(virtual_path)
                if db_meta is not None:
                    # Compare size and mtime — if both match, skip download
                    size_match = db_meta["file_size"] == file_info["file_size"]
                    mtime_match = (
                        db_meta["mtime_epoch"] is not None
                        and file_info["mtime"] > 0
                        and abs(db_meta["mtime_epoch"] - file_info["mtime"]) < 1.0
                    )
                    if size_match and mtime_match:
                        result["skipped"] += 1
                        continue

                changed_files.append(file_info)

            # 5. Compute hashes on sandbox to avoid unnecessary downloads
            sandbox_hashes = await cls._compute_sandbox_hashes(
                sandbox, [f["abs_path"] for f in changed_files]
            )

            # 6. Split into mtime-only updates vs files needing download
            mtime_updates: list[tuple[str, datetime]] = []
            files_to_download: list[dict[str, Any]] = []

            for file_info in changed_files:
                virtual_path = file_info["virtual_path"]
                sandbox_hash = sandbox_hashes.get(file_info["abs_path"])
                db_meta = existing_meta.get(virtual_path)

                if (
                    sandbox_hash
                    and db_meta is not None
                    and db_meta["content_hash"] == sandbox_hash
                ):
                    # Content unchanged — just update mtime in DB
                    if file_info["mtime"] > 0:
                        mtime_updates.append(
                            (
                                virtual_path,
                                datetime.fromtimestamp(
                                    file_info["mtime"], tz=timezone.utc
                                ),
                            )
                        )
                    result["skipped"] += 1
                else:
                    files_to_download.append(file_info)

            # 7. Bulk update mtimes for unchanged files
            if mtime_updates:
                await bulk_update_file_mtimes(workspace_id, mtime_updates)

            # 8. Download files that actually changed, in parallel batches
            upsert_payloads: list[dict[str, Any]] = []
            download_batch_size = 10

            async def _download_file(file_info: dict[str, Any]) -> dict[str, Any] | None:
                virtual_path = file_info["virtual_path"]
                try:
                    content = await sandbox.adownload_file_bytes(file_info["abs_path"])
                    if content is None:
                        return None

                    # Recompute hash from actual bytes (race safety)
                    content_hash = hashlib.sha256(content).hexdigest()

                    is_binary = _detect_is_binary(virtual_path, content)
                    content_text = None
                    content_binary = None
                    if is_binary:
                        content_binary = content
                    else:
                        try:
                            content_text = content.decode("utf-8")
                        except UnicodeDecodeError:
                            is_binary = True
                            content_binary = content

                    mime, _ = mimetypes.guess_type(virtual_path)

                    sandbox_modified_at = None
                    if file_info["mtime"] > 0:
                        sandbox_modified_at = datetime.fromtimestamp(
                            file_info["mtime"], tz=timezone.utc
                        )

                    return {
                        "file_path": virtual_path,
                        "file_name": file_info["file_name"],
                        "file_size": file_info["file_size"],
                        "content_hash": content_hash,
                        "content_text": content_text,
                        "content_binary": content_binary,
                        "mime_type": mime,
                        "is_binary": is_binary,
                        "permissions": None,
                        "sandbox_modified_at": sandbox_modified_at,
                    }
                except Exception as e:
                    logger.warning(
                        f"Error downloading file {virtual_path} "
                        f"for workspace {workspace_id}: {e}"
                    )
                    return None

            for i in range(0, len(files_to_download), download_batch_size):
                batch = files_to_download[i : i + download_batch_size]
                batch_results = await asyncio.gather(
                    *[_download_file(f) for f in batch],
                    return_exceptions=False,
                )
                for payload in batch_results:
                    if payload is not None:
                        upsert_payloads.append(payload)
                    else:
                        result["errors"] += 1

            # 9. Bulk upsert all downloaded files
            if upsert_payloads:
                count = await bulk_upsert_files(workspace_id, upsert_payloads)
                result["synced"] = count

            # 10. Delete files from DB that no longer exist in sandbox
            deleted = await delete_removed_files(workspace_id, active_paths)
            result["deleted"] = deleted

            result["total_size"] = await get_workspace_total_size(workspace_id)

            logger.debug(
                f"File sync completed for workspace {workspace_id}: "
                f"synced={result['synced']}, skipped={result['skipped']}, "
                f"deleted={result['deleted']}, errors={result['errors']}"
            )

        except Exception as e:
            logger.error(f"File sync failed for workspace {workspace_id}: {e}")
            raise

        return result

    @classmethod
    async def restore_to_sandbox(
        cls, workspace_id: str, sandbox: Any
    ) -> dict[str, Any]:
        """
        Restore workspace files from DB to sandbox.

        Pre-creates all unique parent directories in one bulk mkdir, then
        uploads files concurrently through a semaphore-bounded worker pool
        so the next upload starts the instant any slot frees up (rather
        than waiting for the slowest file in a fixed-size batch).

        Args:
            workspace_id: Workspace UUID
            sandbox: Sandbox instance

        Returns:
            Restore result summary
        """
        result = {"restored": 0, "errors": 0}

        try:
            files = await get_files_for_workspace(workspace_id, include_content=True)

            if not files:
                logger.info(f"No files to restore for workspace {workspace_id}")
                return result

            logger.info(f"Restoring {len(files)} files for workspace {workspace_id}")

            # Bulk-create unique parent directories in one exec call so
            # we skip ~N redundant round-trips inside the per-file path.
            work_dir = sandbox.working_dir
            unique_dirs = {
                os.path.dirname(f"{work_dir}/{file_record['file_path']}")
                for file_record in files
            }
            unique_dirs.discard("")
            unique_dirs.discard(work_dir)
            if unique_dirs:
                await cls._bulk_create_directories(sandbox, unique_dirs)

            # Worker-pool upload: semaphore bounds concurrency, but each
            # task releases its slot as soon as it finishes — no batch
            # waits for the slowest file.
            sem = asyncio.Semaphore(16)

            async def _upload(file_record: dict) -> tuple[str, bool | Exception]:
                async with sem:
                    try:
                        ok = await cls._restore_file_content_only(sandbox, file_record)
                        return (file_record["file_path"], ok)
                    except Exception as e:
                        return (file_record["file_path"], e)

            outcomes = await asyncio.gather(*(_upload(f) for f in files))
            for path, res in outcomes:
                if isinstance(res, Exception):
                    logger.warning(f"Failed to restore {path}: {res}")
                    result["errors"] += 1
                elif res:
                    result["restored"] += 1
                else:
                    result["errors"] += 1

            # Write sync marker
            try:
                work_dir = sandbox.working_dir
                marker_content = datetime.now(timezone.utc).isoformat().encode("utf-8")
                await sandbox.aupload_file_bytes(_sync_marker_path(work_dir), marker_content)
            except Exception:
                pass  # Non-critical

            # Update DB mtimes to match restored files' new sandbox mtimes
            if result["restored"] > 0:
                try:
                    sandbox_meta = await cls.list_sandbox_files(sandbox)
                    for vpath, info in sandbox_meta.items():
                        if info["mtime"] > 0:
                            await update_file_mtime(
                                workspace_id,
                                vpath,
                                datetime.fromtimestamp(
                                    info["mtime"], tz=timezone.utc
                                ),
                            )
                except Exception as e:
                    logger.warning(
                        f"Mtime sync after restore failed for {workspace_id}: {e}"
                    )

            logger.info(
                f"File restore completed for workspace {workspace_id}: "
                f"restored={result['restored']}, errors={result['errors']}"
            )

        except Exception as e:
            logger.error(f"File restore failed for workspace {workspace_id}: {e}")
            raise

        return result

    @classmethod
    async def _restore_single_file(cls, sandbox: Any, file_record: dict) -> bool:
        """Restore a single file to sandbox, creating its parent directory.

        Kept for callers outside the bulk restore path (e.g. single-file
        writes). Bulk restore uses ``_restore_file_content_only`` after a
        single pre-pass that creates all unique parents at once.
        """
        work_dir = sandbox.working_dir
        abs_path = f"{work_dir}/{file_record['file_path']}"

        parent_dir = os.path.dirname(abs_path)
        if parent_dir and parent_dir != work_dir:
            await sandbox.acreate_directory(parent_dir)

        return await cls._restore_file_content_only(sandbox, file_record)

    @classmethod
    async def _restore_file_content_only(
        cls, sandbox: Any, file_record: dict
    ) -> bool:
        """Upload file content assuming parent directory already exists."""
        work_dir = sandbox.working_dir
        abs_path = f"{work_dir}/{file_record['file_path']}"

        if file_record.get("is_binary") and file_record.get("content_binary"):
            content = file_record["content_binary"]
            if isinstance(content, memoryview):
                content = bytes(content)
        elif file_record.get("content_text") is not None:
            content = file_record["content_text"].encode("utf-8")
        else:
            return False

        return await sandbox.aupload_file_bytes(abs_path, content)

    @classmethod
    async def _bulk_create_directories(
        cls, sandbox: Any, dirs: set[str]
    ) -> None:
        """Pre-create all unique parent directories before bulk upload.

        Uses the sandbox's bulk API (single ``mkdir -p`` exec for all
        paths). Falls back to parallel per-dir creates if the bulk call
        fails (e.g. command-line length exceeded for huge workspaces).
        ``mkdir -p`` is idempotent either way.
        """
        if not sandbox or not dirs:
            return

        ok = await sandbox.acreate_directories(dirs)
        if ok:
            return

        logger.debug(
            f"Bulk mkdir unavailable or failed for {len(dirs)} dirs; "
            "falling back to parallel per-dir creates."
        )
        sem = asyncio.Semaphore(16)

        async def _mkone(d: str) -> None:
            async with sem:
                await sandbox.acreate_directory(d)

        await asyncio.gather(*(_mkone(d) for d in dirs), return_exceptions=True)

    @classmethod
    async def maybe_restore(cls, workspace_id: str, sandbox: Any) -> None:
        """
        Restore files from DB if sandbox was recreated (files lost).

        Checks for sync marker file. If absent, files were lost and need restore.
        """
        try:
            work_dir = sandbox.working_dir
            sync_marker = _sync_marker_path(work_dir)
            marker = await sandbox.adownload_file_bytes(sync_marker)
            if marker is not None:
                # Marker exists — sandbox still has its files
                return

            # Check if we have any files in DB to restore
            files = await get_files_for_workspace(workspace_id, include_content=False)
            if not files:
                # No files saved — write marker and skip
                try:
                    marker_content = (
                        datetime.now(timezone.utc).isoformat().encode("utf-8")
                    )
                    await sandbox.aupload_file_bytes(sync_marker, marker_content)
                except Exception:
                    pass
                return

            logger.info(
                f"Sync marker missing for workspace {workspace_id}. "
                f"Restoring {len(files)} files from DB."
            )
            await cls.restore_to_sandbox(workspace_id, sandbox)

        except Exception as e:
            logger.warning(f"Error in maybe_restore for workspace {workspace_id}: {e}")

    @classmethod
    async def get_file_tree(cls, workspace_id: str) -> list[dict[str, Any]]:
        """
        Get file metadata from DB for offline UI browsing.

        Returns flat list of file metadata (no content).
        """
        files = await get_files_for_workspace(workspace_id, include_content=False)
        return [
            {
                "path": f["file_path"],
                "name": f["file_name"],
                "size": f["file_size"],
                "mime_type": f.get("mime_type"),
                "is_binary": f.get("is_binary", False),
                "modified_at": f.get("sandbox_modified_at"),
            }
            for f in files
        ]

    @classmethod
    async def get_file_content(
        cls, workspace_id: str, file_path: str
    ) -> dict[str, Any] | None:
        """
        Get file content from DB for offline access.

        Returns file record with content, or None if not found.
        """
        return await db_get_file(workspace_id, file_path, include_content=True)
