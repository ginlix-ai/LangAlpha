"""Carry a file or folder between two of a user's computers through object
storage, for CopyFile.

The source hashes what it holds and uploads only the digests the store does
not have yet, so a file a backup already stored costs no upload; the
destination fetches every file itself. No byte passes through this process
except a pack chunk the source cannot send itself, as in a backup. Small
files travel in pack chunks, made for the copy; large ones as their own
objects, which is where a copy finds what a backup stored before it.

The objects it uploads are registered as a backup's are and referenced by no
manifest row, so the collector takes them after its grace period unless a
backup comes to name them first.
"""

from __future__ import annotations

import asyncio
import logging
import posixpath
from collections import defaultdict
from typing import Any

from ptc_agent.agent.tools.computer_reach import (
    ComputerUnreachable,
    Shipment,
    Shipped,
    ShipmentTooLarge,
)
from ptc_agent.core.paths import logged_path
from src.server.database.blob_keys import blob_key
from src.server.database.workspace_file_blobs import registered_blobs
from src.server.services.persistence.blobs import _persist_packed, _push_direct
from src.server.services.persistence.transfer import (
    PACK_CUTOFF,
    PACK_MAX_BYTES,
    SCAN_TIMEOUT_S,
    ScanEntry,
    TransferRoots,
    hash_one_file,
    pull_direct,
    run_transfer_op,
    transfer_timeout_s,
)
from src.utils.storage import get_signed_url

logger = logging.getLogger(__name__)


class StoreUnreachable(Exception):
    """A computer could not reach the store, or the store would not sign;
    the copy can still go another way."""


async def ship_via_store(
    user_id: str,
    source: Any,
    source_root: str,
    destination: Any,
    destination_root: str,
    shipment: Shipment,
    *,
    source_name: str,
    destination_name: str,
) -> Shipped:
    """Land ``shipment`` from the ``source`` sandbox in its staging folder on
    ``destination``. The roots are each computer's own, where pack chunks
    stage. Raises ``ComputerUnreachable`` for the agent, ``StoreUnreachable``
    when this route cannot serve."""
    where = f'computer "{source_name}"'
    if shipment.size > shipment.max_bytes:
        raise ShipmentTooLarge(shipment.size)
    roots = TransferRoots(
        shipment.path if shipment.folder else posixpath.dirname(shipment.path),
        source_root,
    )
    entries, root = (
        await _scan(source, roots, shipment, where)
        if shipment.folder
        else await _hash(source, roots, shipment, where)
    )
    files = [e for e in entries if e.kind == "file"]
    size = sum(e.size for e in files)
    if size > shipment.max_bytes:
        raise ShipmentTooLarge(size)

    # Named in the backup's logs as a workspace would be.
    label = f"copy of {logged_path(shipment.path)}"
    loose = [e for e in files if not shipment.folder or e.size > PACK_CUTOFF]
    packed = [e for e in files if shipment.folder and e.size <= PACK_CUTOFF]
    # Small files are packed afresh, though a chunk the store holds already
    # (the same files sent before) is not sent again.
    uploaded = await _upload_loose(user_id, label, source, loose, roots, shipment.path, where)
    chunks: dict[str, list[tuple[ScanEntry, int]]] = defaultdict(list)
    if packed:
        rows, unsaved, _ = await _persist_packed(
            user_id, label, source, packed, {}, layout=roots
        )
        _refuse_unsaved([u.reason for u in unsaved], shipment.path, where)
        by_path = {e.path: e for e in packed}
        for row in rows:
            chunks[row["pack_sha256"]].append((by_path[row["file_path"]], int(row["pack_offset"])))

    items = [root] if root is not None else []
    items += await _items(user_id, entries, chunks, shipment, size)
    results = await pull_direct(
        destination,
        items,
        layout=TransferRoots(shipment.staging, destination_root),
    )
    failed = [(path, r) for path, r in results.items() if r.get("status") != "ok"]
    # The store failing the download leaves the server to carry the copy; a
    # failure on the destination's own disk would meet that route too, so
    # the agent hears it.
    if failed and all(_from_store(r) for _, r in failed):
        raise StoreUnreachable(
            f'computer "{destination_name}" could not fetch {len(failed)} file(s) from the store'
        )
    if failed:
        path, r = next((path, r) for path, r in failed if not _from_store(r))
        raise ComputerUnreachable(
            f'The copy could not be fetched on computer "{destination_name}" '
            f"({len(failed)} failed, {posixpath.basename(path)} among them): "
            f"{r.get('error') or r.get('status')}"
        )
    logger.info(
        f"Shipped {logged_path(shipment.path)}: {len(files)} file(s), {size} bytes; "
        f"{uploaded} bytes of {len(loose)} large file(s) uploaded, {len(packed)} small packed"
    )
    return Shipped(
        size=size,
        files=len(files),
        entries=len(entries) + (1 if shipment.folder else 0),
    )


async def _hash(
    source: Any, roots: TransferRoots, shipment: Shipment, where: str
) -> tuple[list[ScanEntry], None]:
    entry = await hash_one_file(
        source, posixpath.basename(shipment.path), prior=None, layout=roots
    )
    if entry is None:
        raise ComputerUnreachable(f"{shipment.path} is no longer a file on {where}.")
    return [entry], None


async def _scan(
    source: Any, roots: TransferRoots, shipment: Shipment, where: str
) -> tuple[list[ScanEntry], dict[str, Any]]:
    """Every entry under the folder, files hashed, and the pull item for the
    folder itself, which carries its mode. Walked once without reading any
    file first, so a folder over the limit is refused before it is read."""
    spec = {
        "root": shipment.path,
        "tail_base": posixpath.basename(shipment.path),
        "exclude_tails": list(shipment.tails),
        "exclude_name_patterns": list(shipment.names),
        "exclude_same_as": list(shipment.same_as),
        "max_file_bytes": None,
    }
    # A runtime from before a copy's rules would ignore them, so a result
    # without the root's fields means it is replaced first.
    listed = await run_transfer_op(
        source,
        "scan",
        {**spec, "hash": False},
        timeout_s=SCAN_TIMEOUT_S,
        reports=("root_left_out",),
    )
    size = sum(int(e.get("size") or 0) for e in listed.get("entries", []) if e["kind"] == "file")
    if size > shipment.max_bytes:
        raise ShipmentTooLarge(size)
    out = await run_transfer_op(
        source, "scan", spec, timeout_s=SCAN_TIMEOUT_S, reports=("root_left_out",)
    )
    if out.get("root_left_out"):
        raise ComputerUnreachable(
            f"{shipment.path} is one of the computer's own folders on {where}, which no "
            "copy reads."
        )
    if errors := out.get("errors"):
        first = errors[0]
        raise ComputerUnreachable(
            f"{shipment.path} could not be read whole on {where}, so nothing was copied "
            f"({len(errors)} unreadable, {first.get('path')} among them): {first.get('error')}"
        )
    entries = [ScanEntry.from_wire(e) for e in out.get("entries", [])]
    if any(e.kind == "file" and not e.sha256 for e in entries):
        raise ComputerUnreachable(f"{shipment.path} could not be read whole on {where}.")
    root = {
        "path": f"unpacked/{posixpath.basename(shipment.path)}",
        "kind": "dir",
        "mode": out.get("root_mode"),
        "mtime_ns": out.get("root_mtime_ns"),
    }
    return entries, root


async def _upload_loose(
    user_id: str,
    label: str,
    source: Any,
    files: list[ScanEntry],
    roots: TransferRoots,
    path: str,
    where: str,
) -> int:
    """Upload the digests among ``files`` the store lacks, straight from the
    source; the bytes sent."""
    wanted = {e.sha256 for e in files if e.sha256}
    need = wanted - await registered_blobs(user_id, list(wanted))
    if not need:
        return 0
    pushed = await _push_direct(user_id, label, source, files, need, layout=roots)
    if pushed is None or pushed.unreachable:
        raise StoreUnreachable(f"{where} could not upload to the store")
    missing = need - pushed.registered
    _refuse_unsaved(
        ["changed" if sha in pushed.changed else "failed" for sha in missing], path, where
    )
    sizes = {e.sha256: e.size for e in files}
    return sum(sizes[sha] for sha in need)


def _from_store(result: dict[str, Any]) -> bool:
    """A download the store failed: no route to it, an HTTP error from it,
    or bytes that are not the ones asked for."""
    return result.get("status") in ("unreachable", "mismatch") or result.get("http") is not None


def _refuse_unsaved(reasons: list[str], path: str, where: str) -> None:
    """A file that changed as it was read is the agent's to retry; any other
    failed upload is this route's, and the server may still carry it."""
    if "changed" in reasons:
        raise ComputerUnreachable(
            f"{path} changed on {where} while it was copied, so nothing was copied. Try again."
        )
    if reasons:
        raise StoreUnreachable(f"{len(reasons)} file(s) could not be uploaded from {where}")


async def _items(
    user_id: str,
    entries: list[ScanEntry],
    chunks: dict[str, list[tuple[ScanEntry, int]]],
    shipment: Shipment,
    size: int,
) -> list[dict[str, Any]]:
    """The pull items landing ``entries`` in the staging folder: a lone file
    as ``whole``, a folder's under ``unpacked/<its name>``. One signed URL
    per object and per chunk."""
    expires = transfer_timeout_s(size) + 60
    in_chunk = {e.path for members in chunks.values() for e, _ in members}
    under = f"unpacked/{posixpath.basename(shipment.path)}"

    def at(entry: ScanEntry) -> str:
        return f"{under}/{entry.path}" if shipment.folder else "whole"

    shas = sorted(
        {e.sha256 for e in entries if e.kind == "file" and e.path not in in_chunk and e.sha256}
        | set(chunks)
    )
    urls = dict(
        zip(
            shas,
            await asyncio.gather(
                *(
                    asyncio.to_thread(get_signed_url, blob_key(user_id, sha), expires)
                    for sha in shas
                )
            ),
        )
    )
    if any(url is None for url in urls.values()):
        raise StoreUnreachable("the store would not sign a download")

    items: list[dict[str, Any]] = []
    for e in entries:
        stamp = {"mode": e.mode, "mtime_ns": e.mtime_ns}
        if e.kind == "dir":
            items.append({"path": at(e), "kind": "dir", **stamp})
        elif e.kind == "symlink":
            items.append(
                {
                    "path": at(e),
                    "kind": "symlink",
                    "symlink_target": e.symlink_target,
                    "mtime_ns": e.mtime_ns,
                }
            )
        elif e.path not in in_chunk:
            items.append(
                {
                    "path": at(e),
                    "kind": "file",
                    "sha256": e.sha256,
                    "size": e.size,
                    "url": urls[e.sha256],
                    **stamp,
                }
            )
    for sha, members in chunks.items():
        items.append(
            {
                "kind": "pack",
                "sha256": sha,
                # Weighed at the most a chunk holds, as a restore weighs it.
                "size": PACK_MAX_BYTES,
                "url": urls[sha],
                "members": [
                    {
                        "path": at(e),
                        "offset": offset,
                        "size": e.size,
                        "sha256": e.sha256,
                        "mode": e.mode,
                        "mtime_ns": e.mtime_ns,
                    }
                    for e, offset in members
                ],
            }
        )
    items.sort(key=lambda i: int(i.get("size") or 0), reverse=True)
    return items
