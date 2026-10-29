"""The store primitives a browser's upload is built on, and the switch that offers it.

A part URL binds its length, so a browser holding it writes that many bytes to
that part or nothing. Assembling reads the parts back from the store rather
than from the browser, so a listing has to be complete or absent, never short.
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

import src.utils.storage as storage
from src.utils.storage import s3_compatible

BUCKET = "test-bucket"
KEY = "uploads/user-1/00000000-0000-4000-8000-000000000001"
STORE_ID = "store-upload-1"
MiB = 1024 * 1024


@pytest.fixture(autouse=True)
def _bucket():
    with patch.object(s3_compatible.StorageConfig, "BUCKET_NAME", BUCKET):
        yield


def _client_error(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": code}}, "op")


def _clients():
    server, browser = MagicMock(), MagicMock()
    server.generate_presigned_url.return_value = "https://store.invalid/server?sig"
    browser.generate_presigned_url.return_value = "https://store.invalid/browser?sig"
    return (
        server,
        browser,
        patch.object(s3_compatible, "_get_client", return_value=server),
        patch.object(s3_compatible, "_get_browser_client", return_value=browser),
    )


@pytest.mark.parametrize("for_browser", [False, True])
def test_a_part_url_signs_its_length_for_the_client_that_follows_it(for_browser):
    server, browser, server_patch, browser_patch = _clients()
    with server_patch, browser_patch:
        url = s3_compatible.sign_upload_part(
            KEY, STORE_ID, part_number=3, size=5 * MiB, expires_in=600, for_browser=for_browser
        )

    signer, other = (browser, server) if for_browser else (server, browser)
    assert url == signer.generate_presigned_url.return_value
    other.generate_presigned_url.assert_not_called()
    assert signer.generate_presigned_url.call_args.args == ("upload_part",)
    assert signer.generate_presigned_url.call_args.kwargs == {
        "Params": {
            "Bucket": BUCKET,
            "Key": KEY,
            "UploadId": STORE_ID,
            "PartNumber": 3,
            "ContentLength": 5 * MiB,
        },
        "ExpiresIn": 600,
    }


def test_a_part_that_cannot_be_signed_is_none():
    with patch.object(s3_compatible, "_get_client", side_effect=RuntimeError("no creds")):
        assert s3_compatible.sign_upload_part(KEY, STORE_ID, part_number=1, size=1) is None


def _part(number: int, size: int = 10) -> dict:
    return {"PartNumber": number, "Size": size, "ETag": f'"e{number}"'}


def test_the_parts_listing_follows_every_page_and_comes_back_in_order():
    client = MagicMock()
    client.list_parts.side_effect = [
        {"Parts": [_part(2), _part(1)], "IsTruncated": True, "NextPartNumberMarker": 2},
        {"Parts": [_part(3, size=5)], "IsTruncated": False},
    ]
    with patch.object(s3_compatible, "_get_client", return_value=client):
        parts = s3_compatible.list_uploaded_parts(KEY, STORE_ID)

    assert parts == [
        {"part_number": 1, "size": 10, "etag": '"e1"'},
        {"part_number": 2, "size": 10, "etag": '"e2"'},
        {"part_number": 3, "size": 5, "etag": '"e3"'},
    ]
    assert [c.kwargs["PartNumberMarker"] for c in client.list_parts.call_args_list] == [0, 2]
    assert {c.kwargs["UploadId"] for c in client.list_parts.call_args_list} == {STORE_ID}


@pytest.mark.parametrize(
    "pages",
    [
        [_client_error("NoSuchUpload")],
        # A failure past the first page: a short listing would read as parts missing.
        [{"Parts": [_part(1)], "IsTruncated": True, "NextPartNumberMarker": 1}, _client_error("InternalError")],
    ],
    ids=["upload-gone", "later-page-fails"],
)
def test_a_listing_the_store_cannot_finish_is_none(pages):
    client = MagicMock()
    client.list_parts.side_effect = pages
    with patch.object(s3_compatible, "_get_client", return_value=client):
        assert s3_compatible.list_uploaded_parts(KEY, STORE_ID) is None


def test_object_size_reads_the_stored_length():
    client = MagicMock()
    client.head_object.return_value = {"ContentLength": 42}
    with patch.object(s3_compatible, "_get_client", return_value=client):
        assert s3_compatible.object_size(KEY) == 42
    client.head_object.assert_called_once_with(Bucket=BUCKET, Key=KEY)


@pytest.mark.parametrize("code", ["404", "NoSuchKey"])
def test_no_object_is_none_and_not_an_error(code, caplog):
    client = MagicMock()
    client.head_object.side_effect = _client_error(code)
    with patch.object(s3_compatible, "_get_client", return_value=client):
        with caplog.at_level(logging.ERROR, logger=s3_compatible.logger.name):
            assert s3_compatible.object_size(KEY) is None
    assert not caplog.records


def test_a_store_that_will_not_say_is_none_and_logged(caplog):
    client = MagicMock()
    client.head_object.side_effect = _client_error("403")
    with patch.object(s3_compatible, "_get_client", return_value=client):
        with caplog.at_level(logging.ERROR, logger=s3_compatible.logger.name):
            assert s3_compatible.object_size(KEY) is None
    assert caplog.records


def test_signed_multipart_lays_out_every_part_by_length():
    client = MagicMock()
    client.create_multipart_upload.return_value = {"UploadId": STORE_ID}
    client.generate_presigned_url.side_effect = lambda _op, *, Params, ExpiresIn: f"u{Params['PartNumber']}"
    part = 64 * MiB
    with patch.object(s3_compatible, "_get_client", return_value=client):
        out = s3_compatible.create_signed_multipart_upload(
            KEY, content_length=2 * part + 7, content_type="application/octet-stream"
        )

    assert out == (
        STORE_ID,
        [
            {"part_number": 1, "offset": 0, "size": part, "url": "u1"},
            {"part_number": 2, "offset": part, "size": part, "url": "u2"},
            {"part_number": 3, "offset": 2 * part, "size": 7, "url": "u3"},
        ],
    )
    client.abort_multipart_upload.assert_not_called()


def test_signed_multipart_aborts_the_upload_when_a_part_fails_to_sign():
    """Nothing was sent, but an open upload would sit unseen until a lifecycle rule."""
    client = MagicMock()
    client.create_multipart_upload.return_value = {"UploadId": STORE_ID}
    client.generate_presigned_url.side_effect = ["u1", RuntimeError("signing failed")]
    with patch.object(s3_compatible, "_get_client", return_value=client):
        out = s3_compatible.create_signed_multipart_upload(
            KEY, content_length=3 * 64 * MiB, content_type="application/octet-stream"
        )

    assert out is None
    client.abort_multipart_upload.assert_called_once_with(Bucket=BUCKET, Key=KEY, UploadId=STORE_ID)


# --- the switch ----------------------------------------------------------------


@pytest.mark.parametrize(
    "flag, provider, enabled",
    [
        (True, "s3", True),
        (False, "s3", False),
        # OSS signs no part URLs, and with no store there is nowhere to send them.
        (True, "oss", False),
        (True, "none", False),
    ],
)
def test_browser_uploads_need_the_flag_and_a_store_that_signs_parts(monkeypatch, flag, provider, enabled):
    monkeypatch.setattr(storage, "BROWSER_UPLOADS", flag)
    monkeypatch.setattr(storage, "STORAGE_PROVIDER", provider)
    assert storage.browser_uploads_enabled() is enabled


@pytest.mark.parametrize(
    "error, agreed",
    [(None, True), (_client_error("NoSuchUpload"), True), (_client_error("AccessDenied"), False)],
    ids=["aborted", "already-gone", "refused"],
)
def test_an_abort_counts_an_upload_the_store_no_longer_knows_as_done(error, agreed):
    # Aborted before, or assembled: either way it holds no parts.
    server, _browser, server_patch, browser_patch = _clients()
    server.abort_multipart_upload.side_effect = error
    with server_patch, browser_patch:
        assert s3_compatible.abort_multipart_upload("uploads/u/1", "store-1") is agreed


def test_a_setting_turned_off_in_the_config_overrides_the_environment(monkeypatch):
    # The off switch has to work from agent_config whatever the environment says.
    monkeypatch.setenv("STORAGE_BROWSER_UPLOADS", "true")
    monkeypatch.setattr(storage, "load_agent_config", lambda: {"storage": {"browser_uploads": False}})
    assert storage._load_storage_setting("browser_uploads", "STORAGE_BROWSER_UPLOADS", "false") == "false"
    monkeypatch.setattr(storage, "load_agent_config", lambda: {"storage": {}})
    assert storage._load_storage_setting("browser_uploads", "STORAGE_BROWSER_UPLOADS", "false") == "true"


# The bucket has to accept the browser's PUTs (CORS), which only the operator
# can know, so the flag is read at import in a fresh interpreter with an empty
# agent_config, as test_provider_requires_bucket does.
_PROBE = (
    "import yaml; yaml.safe_load = lambda *a, **k: {}; "
    "from src.utils import storage; "
    "print(storage.BROWSER_UPLOADS, storage.browser_uploads_enabled())"
)


def _probe(**env: str) -> str:
    clean = {k: v for k, v in os.environ.items() if k != "STORAGE_BROWSER_UPLOADS"}
    clean.update(STORAGE_PROVIDER="s3", STORAGE_BUCKET_NAME=BUCKET, **env)
    out = subprocess.run(
        [sys.executable, "-c", _PROBE], env=clean, capture_output=True, text=True, check=True
    )
    return out.stdout.strip()


def test_browser_uploads_are_off_until_the_operator_sets_them():
    assert _probe() == "False False"
    assert _probe(STORAGE_BROWSER_UPLOADS="true") == "True True"
