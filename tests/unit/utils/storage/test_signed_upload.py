"""Contracts for the content-bound presigned PUT and the transfer-mode switch.

The presign binds digest, length and type into the signature; that binding is
what makes a sandbox-held URL safe to use against a shared content-addressed
prefix. Verified live against an S3-compatible store (a mismatched body is
rejected with BadDigest) before these were written; they pin the parameters.
"""

from __future__ import annotations

import base64
from unittest.mock import MagicMock, patch

import src.utils.storage as storage
from src.utils.storage import s3_compatible

SHA = "a" * 64


def test_presign_binds_digest_length_and_type():
    client = MagicMock()
    client.generate_presigned_url.return_value = "https://store/blobs/x?sig"
    with patch.object(s3_compatible, "_get_client", return_value=client):
        out = s3_compatible.get_signed_upload_url(
            f"blobs/{SHA}", sha256_hex=SHA, content_length=12, content_type="application/octet-stream", expires_in=600
        )
    assert out is not None
    url, headers = out
    assert url == "https://store/blobs/x?sig"
    kwargs = client.generate_presigned_url.call_args.kwargs
    assert client.generate_presigned_url.call_args.args == ("put_object",)
    params = kwargs["Params"]
    expected_b64 = base64.b64encode(bytes.fromhex(SHA)).decode()
    assert params["Key"] == f"blobs/{SHA}"
    assert params["ContentLength"] == 12
    assert params["ContentType"] == "application/octet-stream"
    assert params["ChecksumSHA256"] == expected_b64
    assert kwargs["ExpiresIn"] == 600
    # The uploader must send exactly what was signed.
    assert headers == {"Content-Type": "application/octet-stream", "x-amz-checksum-sha256": expected_b64}


def test_presign_failure_returns_none_not_raise():
    with patch.object(s3_compatible, "_get_client", side_effect=RuntimeError("no creds")):
        assert s3_compatible.get_signed_upload_url("k", sha256_hex=SHA, content_length=1, content_type="x") is None


def test_transfer_mode_auto_is_direct_only_for_daytona_on_s3(monkeypatch):
    monkeypatch.setattr(storage, "BLOB_TRANSFER_MODE", "auto")
    monkeypatch.setattr(storage, "STORAGE_PROVIDER", "s3")
    assert storage.get_blob_transfer_mode("daytona") == "direct"
    assert storage.get_blob_transfer_mode("docker") == "relay"
    assert storage.get_blob_transfer_mode(None) == "relay"


def test_transfer_mode_relay_for_stores_that_cannot_bind_content(monkeypatch):
    monkeypatch.setattr(storage, "BLOB_TRANSFER_MODE", "auto")
    for provider in ("oss", "none"):
        monkeypatch.setattr(storage, "STORAGE_PROVIDER", provider)
        assert storage.get_blob_transfer_mode("daytona") == "relay"


def test_transfer_mode_explicit_overrides_auto(monkeypatch):
    monkeypatch.setattr(storage, "STORAGE_PROVIDER", "s3")
    monkeypatch.setattr(storage, "BLOB_TRANSFER_MODE", "relay")
    assert storage.get_blob_transfer_mode("daytona") == "relay"
    monkeypatch.setattr(storage, "BLOB_TRANSFER_MODE", "direct")
    assert storage.get_blob_transfer_mode("docker") == "direct"
    monkeypatch.setattr(storage, "BLOB_TRANSFER_MODE", "bogus")
    assert storage.get_blob_transfer_mode("daytona") == "direct"
