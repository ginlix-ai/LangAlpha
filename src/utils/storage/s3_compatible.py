"""S3-compatible cloud storage uploader.

A single module for all S3-compatible services: AWS S3, Cloudflare R2,
Tencent COS, MinIO, and any other service supporting the S3 API.

The only difference between providers is configuration (endpoint, credentials).
All use boto3 under the hood.

Dependencies:
    pip install boto3

Environment Variables:
    STORAGE_ACCESS_KEY_ID     - Access key (falls back to AWS_ACCESS_KEY_ID)
    STORAGE_SECRET_ACCESS_KEY - Secret key (falls back to AWS_SECRET_ACCESS_KEY)
    STORAGE_BUCKET_NAME       - Bucket name (falls back to S3_BUCKET_NAME)
    STORAGE_REGION            - Region (falls back to S3_REGION, default: us-east-1)
    STORAGE_ENDPOINT_URL      - Custom endpoint for non-AWS services (falls back to S3_ENDPOINT_URL)
    STORAGE_PUBLIC_URL_BASE   - Public URL base (falls back to S3_PUBLIC_URL_BASE)
    STORAGE_MAX_UPLOAD_SIZE   - Max upload size in bytes (default: 10MB)

Provider-specific examples:
    AWS S3:      No endpoint needed, just credentials + bucket + region
    Cloudflare R2: STORAGE_ENDPOINT_URL=https://{account_id}.r2.cloudflarestorage.com
                   STORAGE_REGION=auto
    Tencent COS:   STORAGE_ENDPOINT_URL=https://cos.{region}.myqcloud.com
    MinIO:         STORAGE_ENDPOINT_URL=http://localhost:9000
"""

import base64
import logging
import mimetypes
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

IMAGE_MIME_TYPES = {
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".gif": "image/gif",
    ".webp": "image/webp",
    ".svg": "image/svg+xml",
    ".ico": "image/x-icon",
    ".bmp": "image/bmp",
    ".tiff": "image/tiff",
    ".tif": "image/tiff",
}


def _get_content_type(key: str) -> str | None:
    """Get MIME content type for a file based on its extension."""
    ext = Path(key).suffix.lower()
    if ext in IMAGE_MIME_TYPES:
        return IMAGE_MIME_TYPES[ext]
    mime_type, _ = mimetypes.guess_type(key)
    return mime_type


class StorageConfig:
    """Storage configuration loaded from environment variables.

    Uses STORAGE_* env vars with fallback to legacy S3_*/AWS_* vars
    for backward compatibility.
    """

    ACCESS_KEY_ID = os.getenv("STORAGE_ACCESS_KEY_ID") or os.getenv("AWS_ACCESS_KEY_ID")
    SECRET_ACCESS_KEY = os.getenv("STORAGE_SECRET_ACCESS_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY")
    BUCKET_NAME = os.getenv("STORAGE_BUCKET_NAME") or os.getenv("S3_BUCKET_NAME")
    REGION = os.getenv("STORAGE_REGION") or os.getenv("S3_REGION", "us-east-1")
    ENDPOINT_URL = os.getenv("STORAGE_ENDPOINT_URL") or os.getenv("S3_ENDPOINT_URL")
    PUBLIC_URL_BASE = os.getenv("STORAGE_PUBLIC_URL_BASE") or os.getenv("S3_PUBLIC_URL_BASE")
    MAX_UPLOAD_SIZE = int(os.getenv("STORAGE_MAX_UPLOAD_SIZE", str(10 * 1024 * 1024)))

    DEFAULT_IMAGE_PREFIX = os.getenv("STORAGE_DEFAULT_IMAGE_PREFIX", "images/")
    DEFAULT_CHART_PREFIX = os.getenv("STORAGE_DEFAULT_CHART_PREFIX", "charts/")

    @classmethod
    def get_public_url_base(cls) -> str:
        """Get the public URL base for the bucket."""
        if cls.PUBLIC_URL_BASE:
            return cls.PUBLIC_URL_BASE.rstrip("/")
        return f"https://{cls.BUCKET_NAME}.s3.{cls.REGION}.amazonaws.com"


# Module-level cache. boto3 clients are thread-safe and the underlying
# botocore connection pool reuses TLS sessions, so a single shared client
# eliminates the per-op handshake that previously dominated upload/download
# latency under load. Lazy so import-time failures (missing creds in tests)
# don't blow up modules that never actually use object storage.
_CLIENT: Any | None = None


def _get_client() -> Any:
    """Return the lazily-constructed shared S3-compatible client."""
    global _CLIENT
    if _CLIENT is not None:
        return _CLIENT
    kwargs: dict[str, Any] = {
        "aws_access_key_id": StorageConfig.ACCESS_KEY_ID,
        "aws_secret_access_key": StorageConfig.SECRET_ACCESS_KEY,
        "region_name": StorageConfig.REGION,
        "config": Config(
            signature_version="s3v4",
            retries={"max_attempts": 3, "mode": "standard"},
        ),
    }
    if StorageConfig.ENDPOINT_URL:
        kwargs["endpoint_url"] = StorageConfig.ENDPOINT_URL
    _CLIENT = boto3.client("s3", **kwargs)
    return _CLIENT


def _reset_client_for_test() -> None:
    """Drop the cached client. Test-only — production never re-initializes."""
    global _CLIENT
    _CLIENT = None


def upload_file(key: str, file_path: str, content_type: str | None = None) -> bool:
    """Upload a local file."""
    path_obj = Path(file_path)
    if not path_obj.exists():
        logger.error(f"File not found: {path_obj}")
        return False

    file_size = path_obj.stat().st_size
    if file_size > StorageConfig.MAX_UPLOAD_SIZE:
        logger.error(f"File too large: {file_size} bytes > {StorageConfig.MAX_UPLOAD_SIZE} bytes")
        return False

    if content_type is None:
        content_type = _get_content_type(key) or _get_content_type(file_path)

    try:
        client = _get_client()
        put_args: dict[str, Any] = {"Bucket": StorageConfig.BUCKET_NAME, "Key": key}
        if content_type:
            put_args["ContentType"] = content_type
        with path_obj.open("rb") as f:
            put_args["Body"] = f
            client.put_object(**put_args)
        logger.debug(f"Uploaded {path_obj} as {key}")
        return True
    except ClientError:
        logger.exception(f"Upload failed for {key}")
        return False
    except Exception:
        logger.exception(f"Unexpected error uploading {key}")
        return False


def upload_base64(key: str, image_data: str, content_type: str | None = None) -> bool:
    """Upload base64-encoded data."""
    try:
        if "," in image_data:
            prefix, image_data = image_data.split(",", 1)
            if content_type is None and prefix.startswith("data:"):
                mime_part = prefix[5:]
                if ";" in mime_part:
                    content_type = mime_part.split(";")[0]
        image_bytes = base64.b64decode(image_data)
        return upload_bytes(key, image_bytes, content_type=content_type)
    except Exception as e:
        logger.error(f"Failed to decode base64 data for {key}: {e}")
        return False


def upload_bytes(key: str, data: bytes, content_type: str | None = None) -> bool:
    """Upload raw bytes."""
    if len(data) > StorageConfig.MAX_UPLOAD_SIZE:
        logger.error(f"Data too large: {len(data)} bytes > {StorageConfig.MAX_UPLOAD_SIZE} bytes")
        return False

    if content_type is None:
        content_type = _get_content_type(key)

    try:
        client = _get_client()
        put_args: dict[str, Any] = {"Bucket": StorageConfig.BUCKET_NAME, "Key": key, "Body": data}
        if content_type:
            put_args["ContentType"] = content_type
        client.put_object(**put_args)
        logger.debug(f"Uploaded bytes as {key}")
        return True
    except ClientError:
        logger.exception(f"Upload failed for {key}")
        return False
    except Exception:
        logger.exception(f"Unexpected error uploading {key}")
        return False


def get_bytes(key: str) -> bytes | None:
    """Download an object's raw bytes. Returns None on failure or missing object."""
    try:
        client = _get_client()
        response = client.get_object(Bucket=StorageConfig.BUCKET_NAME, Key=key)
        body = response.get("Body")
        if body is None:
            return None
        data = body.read()
        return data if isinstance(data, bytes) else bytes(data)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in {"NoSuchKey", "404"}:
            logger.debug(f"Object not found: {key}")
            return None
        logger.exception(f"Download failed for {key}")
        return None
    except Exception:
        logger.exception(f"Unexpected error downloading {key}")
        return None


def does_object_exist(key: str) -> bool:
    """Check if an object exists in the bucket."""
    try:
        client = _get_client()
        client.head_object(Bucket=StorageConfig.BUCKET_NAME, Key=key)
        return True
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "404":
            return False
        logger.error(f"Error checking existence for {key}: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error checking {key}: {e}")
        return False


def delete_object(key: str) -> bool:
    """Delete an object from the bucket."""
    try:
        client = _get_client()
        client.delete_object(Bucket=StorageConfig.BUCKET_NAME, Key=key)
        logger.debug(f"Deleted {key}")
        return True
    except ClientError as e:
        logger.error(f"Deletion failed for {key}: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error deleting {key}: {e}")
        return False


def get_public_url(key: str) -> str:
    """Get the public URL for an uploaded object."""
    return f"{StorageConfig.get_public_url_base()}/{key}"


def get_signed_url(key: str, expires_in: int = 3600) -> str | None:
    """Generate a pre-signed URL for temporary access."""
    try:
        client = _get_client()
        return client.generate_presigned_url(
            "get_object",
            Params={"Bucket": StorageConfig.BUCKET_NAME, "Key": key},
            ExpiresIn=expires_in,
        )
    except ClientError as e:
        logger.error(f"Failed to generate signed URL for {key}: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error generating signed URL for {key}: {e}")
        return None


def upload_image(
    file_path: str, prefix: str | None = None, custom_name: str | None = None,
) -> str | None:
    """Upload an image file and return the public URL."""
    if prefix is None:
        prefix = StorageConfig.DEFAULT_IMAGE_PREFIX

    path_obj = Path(file_path)
    if custom_name:
        filename = custom_name
    else:
        timestamp = datetime.now(tz=UTC).strftime("%Y%m%d_%H%M%S")
        filename = f"{path_obj.stem}_{timestamp}{path_obj.suffix}"

    key = f"{prefix.rstrip('/')}/{filename}"
    if upload_file(key, str(file_path)):
        return get_public_url(key)
    return None


def upload_chart(file_path: str, custom_name: str | None = None) -> str | None:
    """Upload a chart image and return the public URL."""
    return upload_image(file_path, prefix=StorageConfig.DEFAULT_CHART_PREFIX, custom_name=custom_name)


def sanitize_storage_key(name: str, data_url: str | None = None) -> str:
    """Derive a safe S3 key segment from a display name.

    Takes the first line, truncates to 120 chars, strips path-unsafe
    characters, and appends a MIME-derived extension when possible.
    """
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
    """Verify connection and credentials."""
    try:
        client = _get_client()
        client.list_objects_v2(Bucket=StorageConfig.BUCKET_NAME, MaxKeys=1)
        logger.info(f"Connected to bucket: {StorageConfig.BUCKET_NAME}")
        return True
    except ClientError as e:
        logger.error(f"Connection verification failed: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error during verification: {e}")
        return False
