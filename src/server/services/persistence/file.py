"""
File Persistence Service.

Keeps a workspace's manifest in step with the sandbox that holds the live
files. The manifest is one Postgres row per path, with the bytes in object
storage or, where there is none, in the row itself.

- Snapshots the sandbox into the manifest on workspace stop/delete
- Restores the manifest into a sandbox that was recreated
- Serves file metadata and content from the manifest while the sandbox is down

Each job has its own module: ``backup`` snapshots, ``restore`` restores,
``blobs`` moves bytes into storage, ``resolve`` reads them back, and ``_rows``
holds the row shape they share. This module is the facade over them.
"""

from typing import Any

from src.server.database.blob_keys import MAX_BLOB_BYTES
from src.server.database.workspace import ANY_SANDBOX
from src.server.database.workspace_file import (
    get_file as db_get_file,
    get_files_for_workspace,
)
from src.server.services.persistence import backup, restore
from src.server.services.persistence.restore import (
    RestoreGuardUnavailable as RestoreGuardUnavailable,
    RestoreIdentityLost as RestoreIdentityLost,
)
from src.server.services.persistence.resolve import (
    FileBytesUnavailable as FileBytesUnavailable,
    resolve_file_bytes as resolve_file_bytes,
    resolve_file_bytes_or_none as resolve_file_bytes_or_none,
    resolve_file_text_or_none as resolve_file_text_or_none,
)


async def get_file_tree(workspace_id: str) -> list[dict[str, Any]]:
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


async def get_file_content(
    workspace_id: str, file_path: str
) -> dict[str, Any] | None:
    """
    Get file content from DB for offline access.

    Returns file record with content, or None if not found.
    """
    return await db_get_file(workspace_id, file_path, include_content=True)


class FilePersistenceService:
    """Sync workspace files between Daytona sandbox and PostgreSQL."""

    # Same number as the per-blob storage cap, and derived from it rather than
    # restated: a file this path accepts must be storable.
    MAX_FILE_SIZE = MAX_BLOB_BYTES

    @staticmethod
    async def sync_to_db(workspace_id: str, sandbox: Any) -> dict[str, Any]:
        return await backup.sync_to_db(workspace_id, sandbox)

    @staticmethod
    async def list_sandbox_files(
        sandbox: Any, *, prior: dict[str, tuple[int, int, str]] | None = None
    ) -> dict[str, dict[str, Any]]:
        return await backup.list_sandbox_files(sandbox, prior=prior)

    @staticmethod
    def prior_from_meta(
        existing: dict[str, dict[str, Any]],
    ) -> dict[str, tuple[int, int, str]]:
        return backup.prior_from_meta(existing)

    @staticmethod
    async def restore_to_sandbox(
        workspace_id: str, sandbox: Any, *, expected_sandbox_id: Any = ANY_SANDBOX
    ) -> dict[str, Any]:
        return await restore.restore_to_sandbox(
            workspace_id, sandbox, expected_sandbox_id=expected_sandbox_id
        )

    @staticmethod
    async def maybe_restore(workspace_id: str, sandbox: Any) -> None:
        await restore.maybe_restore(workspace_id, sandbox)

    @staticmethod
    async def get_file_tree(workspace_id: str) -> list[dict[str, Any]]:
        return await get_file_tree(workspace_id)

    @staticmethod
    async def get_file_content(
        workspace_id: str, file_path: str
    ) -> dict[str, Any] | None:
        return await get_file_content(workspace_id, file_path)
