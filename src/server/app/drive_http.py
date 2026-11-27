"""The Drive over HTTP, for both its surfaces (the REST API and the mount's
actions): entity tags, byte ranges, and receiving a save's body."""

from __future__ import annotations

import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import Request

from src.server.services import drive
from src.server.services.drive import DriveError, Limits
from src.server.utils.uploads import read_body

_RANGE = re.compile(r"bytes=(\d*)-(\d*)")


def etag_of(header: str | None) -> str | None:
    """An entity tag as sent, unquoted, without ``W/``; ``*`` stays ``*``."""
    if header is None:
        return None
    return header.strip().removeprefix("W/").strip('"').lower()


def byte_range(header: str | None, size: int) -> tuple[int, int] | None:
    """``(start, end)``, end exclusive, of the one range ``header`` asks
    for; None for the whole file. Several ranges, or a malformed one, are
    answered whole, which a server may always do.

    Raises:
        DriveError: ``invalid_range``, for a range wholly past the end.
    """
    match = _RANGE.fullmatch((header or "").strip())
    if match is None or not (match[1] or match[2]):
        return None
    if not match[1]:
        start, end = max(0, size - int(match[2])), size
    else:
        start = int(match[1])
        end = min(size, int(match[2]) + 1) if match[2] else size
        if match[2] and int(match[2]) < start:
            return None
    if start >= size or start >= end:
        raise DriveError("invalid_range", "Range not satisfiable")
    return start, end


@asynccontextmanager
async def receive(request: Request, bounds: Limits) -> AsyncIterator[bytes]:
    """A save's body, read once room for it is held (``drive.holding``) and
    refused as ``too_large`` past what one request carries: before it is
    read when its declared length already is."""
    cap = bounds.max_request_bytes
    declared = request.headers.get("content-length")
    size = int(declared) if declared and declared.isdigit() else cap
    bounds.too_large(size, cap)
    async with drive.holding(size):
        data = await read_body(request, cap)
        bounds.too_large(len(data), cap)
        yield data
