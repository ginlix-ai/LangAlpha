"""Shared helpers for multipart upload handling."""

from fastapi import HTTPException, UploadFile

_UPLOAD_READ_CHUNK: int = 64 * 1024


async def read_capped(file: UploadFile, max_bytes: int) -> bytes:
    """Read an uploaded file in chunks, raising 413 once the cap is exceeded.

    FastAPI parses the multipart form before the endpoint runs, so by the time we
    hold an ``UploadFile`` the body is already spooled to a SpooledTemporaryFile
    (1 MB threshold, then disk); no check here can prevent that. What it does
    prevent is a bare ``await file.read()`` pulling all of a 100 MB adversarial
    upload into memory, so an oversized body gets a deterministic 413 instead of
    an OOM or a silently truncated read.
    """
    chunks: list[bytes] = []
    total = 0
    while True:
        chunk = await file.read(_UPLOAD_READ_CHUNK)
        if not chunk:
            break
        total += len(chunk)
        if total > max_bytes:
            raise HTTPException(
                status_code=413,
                detail=f"File too large (>{max_bytes} bytes).",
            )
        chunks.append(chunk)
    return b"".join(chunks)
