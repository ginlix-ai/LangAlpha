"""The key a browser's upload is assembled under.

Its part URLs name this key, so the user segment is what keeps one user's
browser from writing under another's prefix.
"""

from __future__ import annotations

import pytest

from src.server.database.blob_keys import UPLOAD_KEY_PREFIX, BlobError, upload_key

USER = "user-1"
UPLOAD_ID = "0000000a-0000-4000-8000-00000000000b"


def test_upload_key_is_per_user():
    assert upload_key(USER, UPLOAD_ID) == f"{UPLOAD_KEY_PREFIX}{USER}/{UPLOAD_ID}"
    assert UPLOAD_KEY_PREFIX == "uploads/"


@pytest.mark.parametrize("bad", ["", None, "../other", "a/b", "user a", "-x", "x" * 256])
def test_upload_key_refuses_malformed_user_ids(bad):
    with pytest.raises(BlobError):
        upload_key(bad, UPLOAD_ID)


@pytest.mark.parametrize(
    "bad",
    [
        "",
        None,
        "not-a-uuid",
        UPLOAD_ID.upper(),
        UPLOAD_ID.replace("-", ""),
        f"{UPLOAD_ID}/../other",
        f"../{UPLOAD_ID}",
    ],
)
def test_upload_key_refuses_anything_but_a_uuid(bad):
    with pytest.raises(BlobError):
        upload_key(USER, bad)
