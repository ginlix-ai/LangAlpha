"""Shared helpers for multipart upload handling."""

from typing import TypeVar

from fastapi import HTTPException, UploadFile
from pydantic import TypeAdapter, ValidationError

T = TypeVar("T")

_UPLOAD_READ_CHUNK: int = 64 * 1024


def _too_large(max_bytes: int, actual: int | None = None) -> str:
    limit = f"Maximum upload size is {max_bytes // (1024 * 1024)} MB."
    if actual is None:
        return f"File is too large. {limit}"
    return f"File is too large ({actual // (1024 * 1024)} MB). {limit}"


async def read_capped(file: UploadFile, max_bytes: int) -> bytes:
    """Read an uploaded file, raising 413 rather than materializing past the cap.

    FastAPI parses the multipart form before the endpoint runs, so by the time
    we hold an ``UploadFile`` the body is already spooled to a
    SpooledTemporaryFile (1 MB threshold, then disk); no check here can
    prevent that. What it does prevent is a bare ``await file.read()`` copying
    all of an adversarial upload into memory. The parser has already counted
    the bytes, so a declared size over the cap is refused without reading any.
    """
    if file.size is not None and file.size > max_bytes:
        raise HTTPException(status_code=413, detail=_too_large(max_bytes, file.size))
    if file.size is not None:
        # One allocation of the known size; joining chunks holds the body
        # twice at the join. The extra byte catches a spool that outgrew its count.
        data = await file.read(file.size + 1)
        if len(data) > max_bytes:
            raise HTTPException(status_code=413, detail=_too_large(max_bytes))
        return data
    chunks: list[bytes] = []
    total = 0
    while True:
        chunk = await file.read(_UPLOAD_READ_CHUNK)
        if not chunk:
            break
        total += len(chunk)
        if total > max_bytes:
            raise HTTPException(status_code=413, detail=_too_large(max_bytes))
        chunks.append(chunk)
    return b"".join(chunks)


def json_form_field(
    raw: str | None, adapter: TypeAdapter[T], refusal: str
) -> T | None:
    """A multipart field that carries JSON, as the JSON body would carry it.

    None when the field is absent or blank. Anything ``adapter`` refuses,
    unparseable JSON included, is answered 422 with ``refusal``: one sentence
    naming the shape the field takes.
    """
    if raw is None or not raw.strip():
        return None
    try:
        return adapter.validate_json(raw)
    except ValidationError:
        raise HTTPException(status_code=422, detail=refusal) from None
