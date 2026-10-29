"""Contracts for STORAGE_KEY_PREFIX: one namespace per deployment in a shared bucket.

Verified live first (a prefixed server backed up, served a signed link and
restored a duplicate entirely under its prefix, with nothing at the top
level). These pin that every S3 operation, presigned or not, carries the
prefix, and that an unset prefix changes nothing.
"""

from __future__ import annotations

from unittest.mock import MagicMock
from urllib.parse import urlsplit

import pytest

from src.utils.storage import key_prefix, oss_uploader, s3_compatible

KEY = "blobs/u1/" + "a" * 64


@pytest.fixture
def s3(monkeypatch):
    cfg = s3_compatible.StorageConfig
    monkeypatch.setattr(cfg, "ACCESS_KEY_ID", "test")
    monkeypatch.setattr(cfg, "SECRET_ACCESS_KEY", "test")
    monkeypatch.setattr(cfg, "BUCKET_NAME", "bucket")
    monkeypatch.setattr(cfg, "REGION", "us-east-1")
    monkeypatch.setattr(cfg, "ENDPOINT_URL", "https://store.example.com")
    monkeypatch.setattr(cfg, "ADDRESSING_STYLE", "path")
    monkeypatch.setattr(cfg, "PUBLIC_URL_BASE", "https://assets.example.com")
    s3_compatible._reset_client_for_test()
    yield
    s3_compatible._reset_client_for_test()


def _set_prefix(monkeypatch, value: str) -> None:
    monkeypatch.setattr(key_prefix, "KEY_PREFIX", value)
    monkeypatch.setattr(s3_compatible, "KEY_PREFIX", value)


@pytest.mark.parametrize(
    ("raw", "want"),
    [("", ""), ("  ", ""), ("staging", "staging/"), ("staging/", "staging/"), ("a/b", "a/b/")],
)
def test_prefix_is_normalized_to_one_trailing_slash(monkeypatch, raw, want):
    monkeypatch.setenv("STORAGE_KEY_PREFIX", raw)
    assert key_prefix._load_key_prefix() == want


@pytest.mark.parametrize("raw", ["/staging", "../staging", "a/../b"])
def test_prefix_that_could_escape_the_namespace_is_refused(monkeypatch, raw):
    monkeypatch.setenv("STORAGE_KEY_PREFIX", raw)
    with pytest.raises(ValueError):
        key_prefix._load_key_prefix()


def _sent_path(client, call):
    """Run one client call without a network and return its key path (the bucket joins later)."""
    seen: dict = {}

    def short_circuit(model, params, **_):
        seen["path"] = params["url_path"].split("?")[0]
        return MagicMock(status_code=200, headers={}), {}

    client.meta.events.register("before-call.s3", short_circuit)
    try:
        call(client)
    finally:
        client.meta.events.unregister("before-call.s3", short_circuit)
    return seen["path"]


def test_every_call_and_presigned_link_lands_under_the_prefix(monkeypatch, s3):
    _set_prefix(monkeypatch, "staging/")
    client = s3_compatible._get_client()

    for call in (
        lambda c: c.head_object(Bucket="bucket", Key=KEY),
        lambda c: c.delete_object(Bucket="bucket", Key=KEY),
        lambda c: c.create_multipart_upload(Bucket="bucket", Key=KEY),
    ):
        assert _sent_path(client, call) == f"/staging/{KEY}"

    for op in ("get_object", "put_object"):
        url = client.generate_presigned_url(op, Params={"Bucket": "bucket", "Key": KEY})
        assert urlsplit(url).path == f"/bucket/staging/{KEY}"
    assert s3_compatible.get_public_url(KEY) == f"https://assets.example.com/staging/{KEY}"


def test_unset_prefix_leaves_keys_untouched(monkeypatch, s3):
    _set_prefix(monkeypatch, "")
    client = s3_compatible._get_client()

    assert _sent_path(client, lambda c: c.head_object(Bucket="bucket", Key=KEY)) == f"/{KEY}"
    url = client.generate_presigned_url("get_object", Params={"Bucket": "bucket", "Key": KEY})
    assert urlsplit(url).path == f"/bucket/{KEY}"
    assert s3_compatible.get_public_url(KEY) == f"https://assets.example.com/{KEY}"


def test_oss_public_url_carries_the_prefix(monkeypatch):
    monkeypatch.setattr(oss_uploader.OSSConfig, "BUCKET_NAME", "bucket")
    monkeypatch.setattr(oss_uploader.OSSConfig, "ENDPOINT", "oss.example.com")
    _set_prefix(monkeypatch, "staging/")
    assert oss_uploader.get_public_url(KEY) == f"https://bucket.oss.example.com/staging/{KEY}"
