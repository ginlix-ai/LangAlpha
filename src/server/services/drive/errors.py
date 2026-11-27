"""The HTTP status each ``DriveError`` code answers with: for the API's
answers and the mount's, and for a report that is not an HTTP answer (an
upload's failure) yet carries one."""

from __future__ import annotations

HTTP_STATUS: dict[str, int] = {
    "not_found": 404,
    "exists": 409,
    "is_folder": 409,
    "not_folder": 409,
    "not_empty": 409,
    "changed": 412,
    "too_large": 413,
    "invalid_range": 416,
    "invalid_path": 422,
    "invalid": 422,
    "over_quota": 507,
    "too_many_entries": 507,
    "forbidden": 403,
    "unavailable": 503,
    # Not 429, which a client reads as throttling to retry after a pause:
    # this lasts until one of the person's own uploads ends.
    "too_many_uploads": 409,
    "upload_ended": 409,
    "parts_missing": 409,
    "bad_part": 422,
    "bad_digest": 422,
    "checkout_required": 409,
    "not_memo": 409,
    "not_text": 409,
    "mismatch": 422,
}


def http_status(code: str) -> int:
    return HTTP_STATUS.get(code, 400)
