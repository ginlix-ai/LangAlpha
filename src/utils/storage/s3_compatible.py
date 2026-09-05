"""S3-compatible cloud storage uploader.

A single module for all S3-compatible services: AWS S3, Cloudflare R2,
MinIO, and any other service supporting the S3 API.

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
    STORAGE_CONNECT_TIMEOUT_S - boto3 connect timeout in seconds (default: 5)
    STORAGE_READ_TIMEOUT_S    - boto3 read timeout in seconds (default: 30)
    STORAGE_MAX_POOL_CONNECTIONS - boto3 connection pool size (default: 32)
    STORAGE_ADDRESSING_STYLE  - Bucket addressing: virtual (default) | path | auto

Endpoint / addressing:
    AWS S3:        No endpoint needed, just credentials + bucket + region
    Cloudflare R2: STORAGE_ENDPOINT_URL=https://{account_id}.r2.cloudflarestorage.com
                   STORAGE_REGION=auto
    MinIO:         STORAGE_ENDPOINT_URL=http://localhost:9000  + STORAGE_ADDRESSING_STYLE=path
    Other S3-compatible: STORAGE_ENDPOINT_URL=https://{bare-host}  (no bucket subdomain)

Most cloud S3-compatible services use virtual-hosted addressing (the default);
self-hosted / path-only backends (e.g. MinIO) require STORAGE_ADDRESSING_STYLE=path.
For a custom endpoint, give the bare host with no bucket subdomain — embedding the
bucket double-prefixes object keys under virtual addressing.
"""

import base64
import logging
import os
import threading
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from src.utils.mime import resolve_content_type

logger = logging.getLogger(__name__)


_VALID_ADDRESSING_STYLES = {"virtual", "path", "auto"}


def _resolve_addressing_style() -> str:
    """Read STORAGE_ADDRESSING_STYLE, trimmed and validated; default 'virtual'.

    Warns on an unrecognized value rather than silently coercing it, so a typo
    or stray whitespace on a path-only backend surfaces instead of reintroducing
    key doubling.
    """
    raw = (os.getenv("STORAGE_ADDRESSING_STYLE") or "virtual").strip().lower()
    if raw not in _VALID_ADDRESSING_STYLES:
        logger.warning(
            "Invalid STORAGE_ADDRESSING_STYLE=%r; falling back to 'virtual' (valid: virtual, path, auto)",
            raw,
        )
        return "virtual"
    return raw


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

    CONNECT_TIMEOUT_S = int(os.getenv("STORAGE_CONNECT_TIMEOUT_S", "5"))
    READ_TIMEOUT_S = int(os.getenv("STORAGE_READ_TIMEOUT_S", "30"))

    # botocore defaults this to 10, which workspace-file restore overruns: it
    # fetches blobs under a semaphore of 16, so six of every sixteen requests
    # got their connection discarded and paid a fresh TLS handshake. Sized
    # above that concurrency with headroom for the concurrent sync path.
    MAX_POOL_CONNECTIONS = int(os.getenv("STORAGE_MAX_POOL_CONNECTIONS", "32"))

    # boto3 bucket addressing. Defaults to "virtual" because most cloud
    # S3-compatible services require/prefer virtual-hosted addressing, whereas
    # boto3's own default ("path" for custom endpoints) double-prefixes keys
    # against an endpoint that already embeds the bucket. Self-hosted / path-only
    # backends (e.g. MinIO) should set STORAGE_ADDRESSING_STYLE=path.
    ADDRESSING_STYLE = _resolve_addressing_style()

    @classmethod
    def get_public_url_base(cls) -> str:
        """Get the public URL base for the bucket."""
        if cls.PUBLIC_URL_BASE:
            return cls.PUBLIC_URL_BASE.rstrip("/")
        return f"https://{cls.BUCKET_NAME}.s3.{cls.REGION}.amazonaws.com"


# Module-level cache. Using a boto3 client from many threads is safe and the
# underlying connection pool reuses TLS sessions, so a single shared client
# eliminates the per-op handshake that previously dominated upload/download
# latency under load. Lazy so import-time failures (missing creds in tests)
# don't blow up modules that never actually use object storage.
#
# *Constructing* one is a different matter: botocore's session and loader are
# not thread-safe, and every caller here arrives through asyncio.to_thread.
# Workspace file restore fetches blobs under a semaphore of 16, so on a cold
# process all 16 threads reach this at once. The lock makes construction
# happen exactly once; the unlocked fast path keeps the steady state free.
_CLIENT: Any | None = None
_RANGE_CLIENT: Any | None = None
_CLIENT_MU = threading.Lock()


def _get_client() -> Any:
    """Return the lazily-constructed shared S3-compatible client."""
    global _CLIENT
    if _CLIENT is not None:
        return _CLIENT
    with _CLIENT_MU:
        if _CLIENT is not None:
            return _CLIENT
        _CLIENT = _build_client()
    return _CLIENT


def _get_range_client() -> Any:
    """The client for ranged GETs, which must not validate response checksums.

    An object uploaded with a full-object checksum comes back with that
    checksum header on a ranged response too, and the SDK then checks the
    partial body against it and fails every time. Callers reading a range
    verify the bytes they get by their own means.
    """
    global _RANGE_CLIENT
    if _RANGE_CLIENT is not None:
        return _RANGE_CLIENT
    with _CLIENT_MU:
        if _RANGE_CLIENT is not None:
            return _RANGE_CLIENT
        _RANGE_CLIENT = _build_client(response_checksum_validation="when_required")
    return _RANGE_CLIENT


def _build_client(**config_overrides: Any) -> Any:
    kwargs: dict[str, Any] = {
        "aws_access_key_id": StorageConfig.ACCESS_KEY_ID,
        "aws_secret_access_key": StorageConfig.SECRET_ACCESS_KEY,
        "region_name": StorageConfig.REGION,
        "config": Config(
            signature_version="s3v4",
            s3={"addressing_style": StorageConfig.ADDRESSING_STYLE},
            retries={"max_attempts": 3, "mode": "standard"},
            connect_timeout=StorageConfig.CONNECT_TIMEOUT_S,
            read_timeout=StorageConfig.READ_TIMEOUT_S,
            max_pool_connections=StorageConfig.MAX_POOL_CONNECTIONS,
            **config_overrides,
        ),
    }
    if StorageConfig.ENDPOINT_URL:
        kwargs["endpoint_url"] = StorageConfig.ENDPOINT_URL
    return boto3.client("s3", **kwargs)


def _reset_client_for_test() -> None:
    """Drop the cached clients. Test-only — production never re-initializes."""
    global _CLIENT, _RANGE_CLIENT
    with _CLIENT_MU:
        _CLIENT = None
        _RANGE_CLIENT = None


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

    if not content_type:
        content_type = resolve_content_type(key, default=resolve_content_type(file_path))

    try:
        client = _get_client()
        put_args: dict[str, Any] = {
            "Bucket": StorageConfig.BUCKET_NAME,
            "Key": key,
            "ContentType": content_type,
        }
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


def upload_bytes(
    key: str,
    data: bytes,
    content_type: str | None = None,
    max_size: int | None = None,
) -> bool:
    """Upload raw bytes.

    ``max_size`` overrides ``STORAGE_MAX_UPLOAD_SIZE`` for this call only.
    Callers whose own domain cap is larger than the shared default (workspace
    file blobs, at 100MB) pass it rather than lifting the guard globally for
    avatars, charts, and memo binaries.
    """
    cap = StorageConfig.MAX_UPLOAD_SIZE if max_size is None else max_size
    if len(data) > cap:
        logger.error(f"Data too large: {len(data)} bytes > {cap} bytes")
        return False

    if not content_type:
        content_type = resolve_content_type(key)

    try:
        client = _get_client()
        put_args: dict[str, Any] = {
            "Bucket": StorageConfig.BUCKET_NAME,
            "Key": key,
            "Body": data,
            "ContentType": content_type,
        }
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


def get_bytes_range(key: str, start: int, length: int) -> bytes | None:
    """Download ``length`` bytes of an object from offset ``start``.

    ``None`` on failure or a missing object, like :func:`get_bytes`. A zero
    length returns ``b""`` without a request: an empty HTTP range is invalid.
    """
    if length <= 0:
        return b""
    try:
        client = _get_range_client()
        response = client.get_object(
            Bucket=StorageConfig.BUCKET_NAME,
            Key=key,
            Range=f"bytes={start}-{start + length - 1}",
        )
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
        logger.exception(f"Ranged download failed for {key}")
        return None
    except Exception:
        logger.exception(f"Unexpected error downloading a range of {key}")
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


def get_signed_upload_url(
    key: str,
    *,
    sha256_hex: str,
    content_length: int,
    content_type: str,
    expires_in: int = 900,
) -> tuple[str, dict[str, str]] | None:
    """Presign a PUT bound to the object's content, not just its key.

    The digest, length and type are signed into the URL, so the store rejects
    any body that does not hash to ``sha256_hex`` or exceed ``content_length``
    (BadDigest / SignatureDoesNotMatch). That binding is what lets an
    untrusted uploader write into a shared content-addressed prefix. The
    returned headers must be sent verbatim; Content-Length is set by the
    uploader from the body it streams.
    """
    checksum_b64 = base64.b64encode(bytes.fromhex(sha256_hex)).decode("ascii")
    try:
        client = _get_client()
        url = client.generate_presigned_url(
            "put_object",
            Params={
                "Bucket": StorageConfig.BUCKET_NAME,
                "Key": key,
                "ContentLength": content_length,
                "ContentType": content_type,
                "ChecksumSHA256": checksum_b64,
            },
            ExpiresIn=expires_in,
        )
    except Exception as e:
        logger.error(f"Failed to presign upload for {key}: {e}")
        return None
    return url, {
        "Content-Type": content_type,
        "x-amz-checksum-sha256": checksum_b64,
    }


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
