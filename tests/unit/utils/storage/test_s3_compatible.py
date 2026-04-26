"""Tests for src/utils/storage/s3_compatible.py timeout config."""

from __future__ import annotations

import importlib
from unittest.mock import patch

import pytest


def _reload_module():
    """Reimport the module so env-var-derived class attributes refresh."""
    import src.utils.storage.s3_compatible as mod
    importlib.reload(mod)
    return mod


def test_default_timeouts_applied_to_boto3_config():
    mod = _reload_module()
    mod._reset_client_for_test()
    captured: dict = {}

    def fake_client(*_args, **kwargs):
        captured["config"] = kwargs.get("config")
        return object()

    with patch.object(mod.boto3, "client", side_effect=fake_client):
        mod._get_client()

    cfg = captured["config"]
    assert cfg.connect_timeout == 5
    assert cfg.read_timeout == 30


def test_env_overrides_timeouts(monkeypatch):
    monkeypatch.setenv("STORAGE_CONNECT_TIMEOUT_S", "2")
    monkeypatch.setenv("STORAGE_READ_TIMEOUT_S", "11")
    mod = _reload_module()
    mod._reset_client_for_test()
    captured: dict = {}

    def fake_client(*_args, **kwargs):
        captured["config"] = kwargs.get("config")
        return object()

    with patch.object(mod.boto3, "client", side_effect=fake_client):
        mod._get_client()

    cfg = captured["config"]
    assert cfg.connect_timeout == 2
    assert cfg.read_timeout == 11


# --- E2E timeout → upload error mapping ----------------------------------
# The Config(...) wiring above is necessary but not sufficient. These tests
# go through the real upload_bytes() path so we verify both halves:
# 1. boto's ConnectTimeoutError / ReadTimeoutError get caught (they're
#    botocore Exception subclasses, so the bare `except Exception:` in
#    upload_bytes returns False instead of letting the exception escape).
# 2. False from upload_bytes maps to MemoBinaryUploadError at the
#    memo_binary_storage boundary, which the route then turns into 502.


def _patch_put_object_to_raise(mod, exc):
    """Wire boto3.client().put_object to raise the given exception."""
    from unittest.mock import MagicMock

    fake_client = MagicMock()
    fake_client.put_object.side_effect = exc
    mod._reset_client_for_test()
    return patch.object(mod.boto3, "client", return_value=fake_client)


def test_upload_bytes_returns_false_on_connect_timeout():
    """Boto ConnectTimeoutError is caught inside upload_bytes (returns False)."""
    from botocore.exceptions import ConnectTimeoutError

    mod = _reload_module()
    err = ConnectTimeoutError(endpoint_url="https://r2.example/")
    with _patch_put_object_to_raise(mod, err):
        # Force a configured state so upload_bytes doesn't short-circuit.
        with patch.object(mod.StorageConfig, "BUCKET_NAME", "test-bucket"):
            assert mod.upload_bytes("memo/u1/x.pdf", b"x", "application/pdf") is False


def test_upload_bytes_returns_false_on_read_timeout():
    """Boto ReadTimeoutError is caught inside upload_bytes (returns False)."""
    from botocore.exceptions import ReadTimeoutError

    mod = _reload_module()
    err = ReadTimeoutError(endpoint_url="https://r2.example/")
    with _patch_put_object_to_raise(mod, err):
        with patch.object(mod.StorageConfig, "BUCKET_NAME", "test-bucket"):
            assert mod.upload_bytes("memo/u1/x.pdf", b"x", "application/pdf") is False


@pytest.mark.asyncio
async def test_store_binary_raises_upload_error_when_put_times_out(monkeypatch):
    """A timing-out PUT goes upload_bytes→False→MemoBinaryUploadError."""
    from src.server.services import memo_binary_storage

    def fake_upload(*_a, **_k):  # mimics upload_bytes's catch-and-return-False
        return False

    monkeypatch.setattr(memo_binary_storage, "is_configured", lambda: True)
    monkeypatch.setattr(memo_binary_storage, "_storage_upload_bytes", fake_upload)
    with pytest.raises(memo_binary_storage.MemoBinaryUploadError):
        await memo_binary_storage.store_binary(
            user_id="u1", content=b"%PDF-1.4 x", content_type="application/pdf",
        )
