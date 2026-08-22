"""Export an installed plugin as a spec-compliant package zip.

Regenerated from the three stored sources: plugin.json and mcp.json come back
from the JSONB manifests as they were stored, and each still-owned skill's
canonical archive is unpacked under ``skills/<dir>/``. No secret value can
ride along, because the stored document is scrubbed of credential-looking
literals at validation time rather than here — a package that embedded one in
violation of the spec exports with the key present and the value empty. Files
the
install didn't model (README, LICENSE, extension dirs) were reported as
``dropped_files`` and are not round-tripped. Detached components are the
user's now — they are not exported.
"""

import asyncio
import io
import json
import logging
import zipfile
from typing import Any

from src.server.database.plugins import list_plugin_skill_names
from src.server.services.user_skills.limits import MAX_CONCURRENT_ARCHIVE_OPS
from src.server.services.user_skills.materialize import fetch_skill_archive
from src.server.services.user_skills.validate import archive_file_pairs

logger = logging.getLogger(__name__)


def _build_zip(
    docs: list[tuple[str, str]], archives: list[tuple[str, str, bytes]]
) -> bytes:
    """Unpack every stored archive and deflate the whole tree in one hop.

    Unpacking and compressing are CPU work on the same bytes, so a thread hop
    per skill would pay the handoff once per skill to reach a thread the
    compress step already needs. A skill whose archive will not open is
    dropped with a warning, matching the isolation the fetch side gives.
    """
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for path, text in docs:
            zf.writestr(path, text)
        for name, directory, raw in archives:
            try:
                # Canonical archives are rooted at the skill name;
                # archive_file_pairs strips that root with containment guards.
                pairs = archive_file_pairs(raw)
            except Exception:
                logger.warning(
                    "[plugins] export: archive unreadable for skill %r",
                    name, exc_info=True,
                )
                continue
            for rel, data in pairs:
                zf.writestr(f"skills/{directory}/{rel}", data)
    return buf.getvalue()


async def export_plugin_zip(user_id: str, plugin: dict[str, Any]) -> bytes:
    """Build the package zip for an installed plugin row.

    Fetch concurrently, compress once. The enumerated rows carry their own
    archive locator, so no skill costs a second read, and the fetches overlap
    under a storage-shaped bound instead of running as long as the package is
    deep. Compression stays off the loop: it is the one unavoidably expensive
    step, and doing it inline blocks every other request this worker serves.
    """
    docs: list[tuple[str, str]] = [
        ("plugin.json", json.dumps(plugin["manifest"], indent=2) + "\n")
    ]
    if plugin.get("mcp_document") is not None:
        docs.append(
            ("mcp.json", json.dumps(plugin["mcp_document"], indent=2) + "\n")
        )

    refs = await list_plugin_skill_names(user_id, plugin["user_plugin_id"])
    gate = asyncio.Semaphore(MAX_CONCURRENT_ARCHIVE_OPS)

    async def _fetch(ref: dict[str, Any]) -> bytes:
        async with gate:
            return await fetch_skill_archive(user_id, ref)

    fetched = await asyncio.gather(
        *(_fetch(ref) for ref in refs), return_exceptions=True
    )
    archives: list[tuple[str, str, bytes]] = []
    for ref, result in zip(refs, fetched, strict=True):
        if isinstance(result, BaseException):
            if not isinstance(result, Exception):
                raise result  # cancellation is not a fetch failure
            logger.warning(
                "[plugins] export: archive fetch failed for skill %r",
                ref["name"], exc_info=result,
            )
            continue
        archives.append(
            (ref["name"], ref.get("plugin_skill_dir") or ref["name"], result)
        )
    return await asyncio.to_thread(_build_zip, docs, archives)
