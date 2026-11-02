"""Published artifact versions, each artifact served from an origin of its own
under the preview base.

An artifact's host is ``artifact-<artifact id>.<base>``, so its scripts run on
an origin that is neither the app's nor another artifact's, and since the id
is stable its storage survives a new grant. The grant rides in the path
(``services/artifacts/grants``). A deployment without a base serves the same
files on the API origin under a CSP sandbox (``app/artifact_frames.py``),
which costs the version its storage but still keeps it out of the app.

A version never changes, so its files come from Postgres and object storage
alone, never a sandbox: a version opens while its computer is stopped.

The owner's grant opens any version, read only. A visitor who came through
the artifact's link holds a grant for one version instead, which every fetch
checks against the link's row, so an unshare or an Update holds on the next
request on every worker. Either way a document carries the page bridge read
only (``page_bridge``): comments sit on the live file, which
``workspace_files/serve.py`` frames with them on.
"""

from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass
from typing import Any

import psycopg

from src.server.database.blob_keys import BlobError
from src.server.database.artifacts import (
    StoredVersionFile,
    get_shared_version_file,
    get_version_file,
)
from src.server.database.workspace_file_blobs import fetch_blob
from src.server.preview_proxy.page_bridge import bridge_for, with_bridge
from src.server.services.artifacts.grants import (
    ArtifactGrantError,
    grant_prefix,
    is_share_grant,
    verify_artifact_grant,
    verify_artifact_share_grant,
)
from src.server.utils.html_cdns import html_document_csp
from src.server.utils.served_origins import served_sources

__all__ = [
    "ArtifactFileUnavailable",
    "ServedArtifactFile",
    "load_artifact_file",
    "serve_artifact",
    "serve_version_file",
    "version_file_bytes",
]

logger = logging.getLogger(__name__)

_ROUTE = re.compile(r"/g/([^/]+)/v/([^/]+)/(.*)", re.DOTALL)
_SEQ = re.compile(r"[1-9][0-9]{0,8}")
# What the version file rows admit (migration 101).
_MAX_PATH_CHARS = 1024

_PERMISSIONS_POLICY = ", ".join(
    f"{feature}=()"
    for feature in (
        "camera",
        "microphone",
        "geolocation",
        "display-capture",
        "usb",
        "hid",
        "serial",
        "bluetooth",
        "midi",
        "payment",
        "idle-detection",
    )
)
# A version never changes and its URL carries a bearer grant, so the browser
# may keep it as long as it likes and nothing shared may keep it at all.
_CACHE_CONTROL = "private, max-age=31536000, immutable"
# A document carries the bridge as well as the version, and the bridge changes
# with the server: held as immutable, a version viewed once would keep speaking
# the old bridge to a new viewer until the grant rotated. So the browser asks
# each time and the ETag, which names both, answers most asks with a 304.
_DOCUMENT_CACHE_CONTROL = "private, no-cache"
# A visitor's every file is asked for again, so each one meets the share's
# row: a file the browser kept would still open after the link was stopped.
# The ETag keeps the asking cheap.
_SHARED_CACHE_CONTROL = "private, no-cache"
# A service worker would outlive the grant and answer for the origin;
# registering one with any scope a document can reach fails against this.
_SERVICE_WORKER_ALLOWED = "/__no_sw__/"

_TEXT_TYPES = frozenset(
    {
        "application/javascript",
        "application/json",
        "application/xml",
        "application/yaml",
        "image/svg+xml",
    }
)

_ERROR_HEADERS = [
    (b"content-type", b"text/html; charset=utf-8"),
    (b"cache-control", b"no-store"),
    # So the app's read of a file sees the 404 that ends a grant, and asks
    # for a fresh one, rather than a CORS failure it cannot tell apart.
    (b"access-control-allow-origin", b"*"),
    (b"x-content-type-options", b"nosniff"),
    (b"referrer-policy", b"no-referrer"),
    (b"x-robots-tag", b"noindex"),
]
_ERROR_CSP = "default-src 'none'; style-src 'unsafe-inline'"
_ERRORS = {
    404: (
        "File not found",
        "This link has expired or was never valid. Open the file again from "
        "the app to get a fresh one.",
    ),
    405: ("Method not allowed", "A file does not accept this kind of request."),
    503: (
        "File unavailable",
        "This file can't be loaded right now. Try again shortly.",
    ),
}


class ArtifactFileUnavailable(Exception):
    """The file exists, or may, but cannot be read right now."""


@dataclass(frozen=True)
class ServedArtifactFile:
    body: bytes
    headers: list[tuple[str, str]]
    status: int = 200


def _valid_path(path: str) -> bool:
    """A file path the URL may name: relative, with no step outside or in place.

    The rows hold only such paths, so this is not containment; it keeps a
    malformed name from reaching the query. A directory is not a file.
    """
    if not path or len(path) > _MAX_PATH_CHARS or "\\" in path or "\x00" in path:
        return False
    return all(segment not in ("", ".", "..") for segment in path.split("/"))


def _content_type(stored: str) -> str:
    """The stored type, naming UTF-8 for a text type that names no charset.

    UTF-8 is what publishing reads text as, and a document without the
    parameter would leave the browser guessing.
    """
    media = stored.split(";", 1)[0].strip().lower()
    if "charset=" in stored.lower():
        return stored
    if (
        media.startswith("text/")
        or media in _TEXT_TYPES
        or media.endswith(("+json", "+xml"))
    ):
        return f"{stored}; charset=utf-8"
    return stored


def _is_html(content_type: str) -> bool:
    return content_type.split(";", 1)[0].strip().lower() == "text/html"


def _charset_is_utf8(content_type: str) -> bool:
    for parameter in content_type.split(";")[1:]:
        name, _, value = parameter.partition("=")
        if name.strip().lower() == "charset":
            return value.strip().strip('"').lower() in ("utf-8", "utf8")
    return True


def _headers(
    content_type: str,
    etag: str,
    *,
    cache_control: str,
    sandboxed: bool,
    frame_ancestors: str | None,
    prefix: str,
    host: str | None,
) -> list[tuple[str, str]]:
    # On its own host a version's origin is the artifact's, so `'self'` is
    # its own files. On the API origin the sandbox gives it an opaque origin
    # instead, and `'self'` would be every API route, so it names the route it
    # was asked through on the configured origins (``utils/served_origins``);
    # Host only picks among them, naming the PDF renderer's for a request
    # addressed to it.
    own = served_sources(prefix, host) if sandboxed else ["'self'"]
    csp = html_document_csp(
        own, published=True, sandboxed=sandboxed, frame_ancestors=frame_ancestors
    )
    headers = [
        ("content-type", content_type),
        ("cache-control", cache_control),
        ("etag", f'"{etag}"'),
        ("content-security-policy", csp),
        ("permissions-policy", _PERMISSIONS_POLICY),
        ("x-content-type-options", "nosniff"),
        ("referrer-policy", "no-referrer"),
        ("x-robots-tag", "noindex"),
        ("service-worker-allowed", _SERVICE_WORKER_ALLOWED),
        # A sandboxed document's origin is opaque, so its fetch() and module
        # scripts reach its own files cross-origin, as Origin: null; and the
        # app reads a version it cannot frame (a PDF, a CSV) from the
        # artifact's host, another origin. The grant in the URL is the whole
        # capability and no credential rides along, so opening the read to
        # any origin grants nothing new.
        ("access-control-allow-origin", "*"),
    ]
    return headers


async def _owner_file(
    grant: str, seq: int, path: str, host_artifact_id: str | None
) -> StoredVersionFile | None:
    """The file under an owner's grant, which opens any version."""
    try:
        artifact_id = await verify_artifact_grant(grant)
    except ArtifactGrantError as e:
        logger.info("Refusing artifact request: %s", e)
        return None
    except psycopg.Error as e:
        raise ArtifactFileUnavailable(f"artifact grant key unreadable: {e}") from e
    if host_artifact_id is not None and artifact_id != host_artifact_id:
        logger.info("Refusing artifact request: grant is for another artifact")
        return None
    try:
        return await get_version_file(artifact_id, seq, path)
    except psycopg.Error as e:
        raise ArtifactFileUnavailable(f"artifact file lookup failed: {e}") from e


async def _visitor_file(
    grant: str, seq: int, path: str, host_artifact_id: str | None
) -> StoredVersionFile | None:
    """The file under a visitor's grant: only the version it names, and only
    while the link's row still opens that version to anyone."""
    try:
        shared = await verify_artifact_share_grant(grant)
    except ArtifactGrantError as e:
        logger.info("Refusing shared artifact request: %s", e)
        return None
    except psycopg.Error as e:
        raise ArtifactFileUnavailable(
            f"artifact share grant key unreadable: {e}"
        ) from e
    if host_artifact_id is not None and shared.artifact_id != host_artifact_id:
        logger.info("Refusing shared artifact request: grant is for another artifact")
        return None
    if seq != shared.seq:
        logger.info("Refusing shared artifact request: grant is for another version")
        return None
    try:
        return await get_shared_version_file(
            code=shared.code,
            artifact_id=shared.artifact_id,
            revision=shared.revision,
            seq=shared.seq,
            path=path,
        )
    except psycopg.Error as e:
        raise ArtifactFileUnavailable(
            f"shared artifact file lookup failed: {e}"
        ) from e


async def load_artifact_file(
    grant: str,
    seq: str,
    path: str,
    *,
    host_artifact_id: str | None = None,
    sandboxed: bool = False,
    frame_ancestors: str | None = None,
    if_none_match: str | None = None,
    host: str | None = None,
) -> ServedArtifactFile | None:
    """The file ``v/<seq>/<path>`` names under a grant, or None for the 404.

    On an artifact's own host the grant must be for that artifact, or one
    artifact's grant would run another's files on this origin, with its
    storage. On the API origin the grant alone names the artifact. A visitor's
    grant is checked against its link as well (``_visitor_file``). A bad
    grant, another artifact's, an unshared link, a version no longer the
    latest, a missing version and a missing file all answer None, so the
    response never says which. Raises ``ArtifactFileUnavailable`` when a read
    fails, which a caller answers with a 503. A file the browser already
    holds answers 304 before its bytes are read, and only once the grant has
    passed every check.
    """
    if _SEQ.fullmatch(seq) is None or not _valid_path(path):
        return None
    visitor = is_share_grant(grant)
    load = _visitor_file if visitor else _owner_file
    stored = await load(grant, int(seq), path, host_artifact_id)
    if stored is None:
        return None
    return await serve_version_file(
        stored,
        visitor=visitor,
        prefix=grant_prefix(grant),
        sandboxed=sandboxed,
        frame_ancestors=frame_ancestors,
        if_none_match=if_none_match,
        host=host,
    )


async def serve_version_file(
    stored: StoredVersionFile,
    *,
    visitor: bool,
    prefix: str,
    sandboxed: bool,
    frame_ancestors: str | None = None,
    if_none_match: str | None = None,
    host: str | None = None,
) -> ServedArtifactFile:
    """A version file the caller has already authorized, as a response.

    ``prefix`` is the API route the file was asked through, the one a
    sandboxed document may fetch its own files from. A visitor's file is
    asked for again on every view, so each fetch meets the link's row.
    """
    content_type = _content_type(stored.content_type)
    bridged = _is_html(content_type) and _charset_is_utf8(content_type)
    bridge = bridge_for("view")
    etag = f"{stored.sha256}-{bridge.digest}" if bridged else stored.sha256
    if visitor:
        cache_control = _SHARED_CACHE_CONTROL
    else:
        cache_control = _DOCUMENT_CACHE_CONTROL if bridged else _CACHE_CONTROL
    headers = _headers(
        content_type,
        etag,
        cache_control=cache_control,
        sandboxed=sandboxed,
        frame_ancestors=frame_ancestors,
        prefix=prefix,
        host=host,
    )
    if if_none_match is not None and _etag_matches(if_none_match, etag):
        return ServedArtifactFile(body=b"", headers=headers, status=304)

    body = await version_file_bytes(stored)
    if bridged:
        body = await asyncio.to_thread(with_bridge, body, bridge)
    return ServedArtifactFile(body=body, headers=headers)


async def version_file_bytes(stored: StoredVersionFile) -> bytes:
    """A stored version file's bytes, from its row or its blob.

    ``ArtifactFileUnavailable`` when the blob cannot be read.
    """
    if stored.content is not None:
        return stored.content
    try:
        return await fetch_blob(stored.user_id, stored.sha256)
    except BlobError as e:
        raise ArtifactFileUnavailable(str(e)) from e


def _etag_matches(if_none_match: str, etag: str) -> bool:
    """Whether an If-None-Match names ``etag``, by the weak comparison it calls for."""
    for candidate in if_none_match.split(","):
        candidate = candidate.strip()
        if candidate == "*":
            return True
        if candidate.removeprefix("W/") == f'"{etag}"':
            return True
    return False


async def _error(send: Any, status: int, frame_ancestors: str | None) -> None:
    title, text = _ERRORS[status]
    body = (
        '<!doctype html><html><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width,initial-scale=1">'
        f"<title>{title}</title></head>"
        '<body style="font-family:system-ui,sans-serif;max-width:32rem;'
        'margin:4rem auto;padding:0 1rem;color:#333">'
        f'<h1 style="font-size:1.25rem">{title}</h1><p>{text}</p></body></html>'
    ).encode()
    csp = _ERROR_CSP
    if frame_ancestors:
        csp = f"{csp}; frame-ancestors {frame_ancestors}"
    headers = [
        *_ERROR_HEADERS,
        (b"content-security-policy", csp.encode("latin-1")),
        (b"content-length", str(len(body)).encode()),
    ]
    if status == 405:
        headers.append((b"allow", b"GET, HEAD"))
    await send({"type": "http.response.start", "status": status, "headers": headers})
    await send({"type": "http.response.body", "body": body})


async def serve_artifact(
    scope: dict[str, Any],
    receive: Any,
    send: Any,
    *,
    artifact_id: str,
    frame_ancestors: str | None,
) -> None:
    """Answer an http request to ``artifact-<artifact_id>.<base>``."""
    method = scope["method"]
    if method not in ("GET", "HEAD"):
        await _error(send, 405, frame_ancestors)
        return
    route = _ROUTE.fullmatch(scope.get("path", ""))
    if route is None:
        await _error(send, 404, frame_ancestors)
        return
    grant, seq, path = route.groups()
    if_none_match = next(
        (
            v.decode("latin-1")
            for k, v in scope.get("headers", [])
            if k == b"if-none-match"
        ),
        None,
    )
    try:
        served = await load_artifact_file(
            grant,
            seq,
            path,
            host_artifact_id=artifact_id,
            frame_ancestors=frame_ancestors,
            if_none_match=if_none_match,
        )
    except ArtifactFileUnavailable as e:
        logger.warning("Artifact %s could not be served: %s", artifact_id, e)
        await _error(send, 503, frame_ancestors)
        return
    if served is None:
        await _error(send, 404, frame_ancestors)
        return
    headers = [(k.encode("latin-1"), v.encode("latin-1")) for k, v in served.headers]
    if served.status != 304:
        headers.append((b"content-length", str(len(served.body)).encode()))
    await send(
        {"type": "http.response.start", "status": served.status, "headers": headers}
    )
    await send(
        {"type": "http.response.body", "body": b"" if method == "HEAD" else served.body}
    )
