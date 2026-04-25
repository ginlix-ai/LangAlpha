"""Unified cloud storage upload module.

Supports any S3-compatible service (AWS S3, Cloudflare R2, Tencent COS, MinIO, etc.)
and Alibaba Cloud OSS via a single interface.

Configuration priority:
    1. agent_config.yaml (storage.provider)
    2. STORAGE_PROVIDER environment variable
    3. Default: "none" (disabled)

Provider options:
    provider: "s3"    # Any S3-compatible service (S3, R2, COS, MinIO)
    provider: "oss"   # Alibaba Cloud OSS
    provider: "none"  # Disable uploads

Usage:
    from src.utils.storage import upload_bytes, get_public_url, is_storage_enabled

    if is_storage_enabled():
        success = upload_bytes("images/photo.png", data)
        url = get_public_url("images/photo.png")
"""

import logging
import os
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)


def _load_storage_provider() -> str:
    """Load storage provider from agent_config.yaml, with env var fallback."""
    config_path = Path(__file__).parent.parent.parent.parent / "agent_config.yaml"
    if config_path.exists():
        try:
            with config_path.open() as f:
                config = yaml.safe_load(f)
            provider = config.get("storage", {}).get("provider")
            if provider:
                return provider.lower()
        except (OSError, yaml.YAMLError) as e:
            logger.warning(f"Failed to load agent_config.yaml: {e}")

    return os.getenv("STORAGE_PROVIDER", "none").lower()


STORAGE_PROVIDER = _load_storage_provider()


def is_storage_enabled() -> bool:
    """Check if cloud storage uploads are enabled."""
    return STORAGE_PROVIDER != "none"


def get_provider_name() -> str:
    """Get the display name of the configured storage provider."""
    return _PROVIDER_NAME


def get_provider_id() -> str:
    """Get the ID of the configured storage provider."""
    return STORAGE_PROVIDER


# Import the appropriate backend based on provider
if STORAGE_PROVIDER == "none":
    _PROVIDER_NAME = "Disabled"

    def upload_file(key: str, file_path: str, content_type: str | None = None) -> bool:
        return False

    def upload_base64(key: str, image_data: str, content_type: str | None = None) -> bool:
        return False

    def upload_bytes(key: str, data: bytes, content_type: str | None = None) -> bool:
        return False

    def get_bytes(key: str) -> bytes | None:
        return None

    def does_object_exist(key: str) -> bool:
        return False

    def delete_object(key: str) -> bool:
        return False

    def get_public_url(key: str) -> str:
        return ""

    def get_signed_url(key: str, expires_in: int = 3600) -> str | None:
        return None

    def upload_image(file_path: str, prefix: str | None = None, custom_name: str | None = None) -> str | None:
        return None

    def upload_chart(file_path: str, custom_name: str | None = None) -> str | None:
        return None

    def sanitize_storage_key(name: str, data_url: str | None = None) -> str:
        lines = (name or "").splitlines()
        safe = (lines[0].strip()[:120] if lines else "") or "file"
        safe = safe.replace("/", "_")
        ext = ""
        if data_url:
            if data_url.startswith("data:application/pdf"):
                ext = ".pdf"
            elif data_url.startswith("data:image/"):
                mime = data_url.split(";")[0].split("/")[-1]
                ext = f".{mime}" if mime and mime.isalnum() else ".png"
        if ext and not safe.lower().endswith(ext):
            safe = f"{safe}{ext}"
        return safe

    def verify_connection() -> bool:
        logger.info("Storage is disabled (STORAGE_PROVIDER=none)")
        return True

elif STORAGE_PROVIDER == "oss":
    from src.utils.storage.oss_uploader import (
        delete_object,
        does_object_exist,
        get_bytes,
        get_public_url,
        get_signed_url,
        sanitize_storage_key,
        upload_base64,
        upload_bytes,
        upload_chart,
        upload_file,
        upload_image,
        verify_connection,
    )

    _PROVIDER_NAME = "Alibaba Cloud OSS"

else:
    # All S3-compatible providers: "s3", "r2", "cos", or any custom value
    from src.utils.storage.s3_compatible import (
        delete_object,
        does_object_exist,
        get_bytes,
        get_public_url,
        get_signed_url,
        sanitize_storage_key,
        upload_base64,
        upload_bytes,
        upload_chart,
        upload_file,
        upload_image,
        verify_connection,
    )
    _PROVIDER_NAME = f"S3-compatible ({STORAGE_PROVIDER})"


__all__ = [
    "delete_object",
    "does_object_exist",
    "get_bytes",
    "get_provider_id",
    "get_provider_name",
    "get_public_url",
    "get_signed_url",
    "is_storage_enabled",
    "sanitize_storage_key",
    "upload_base64",
    "upload_bytes",
    "upload_chart",
    "upload_file",
    "upload_image",
    "verify_connection",
]
