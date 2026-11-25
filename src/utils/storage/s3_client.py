"""Configuration and clients for an S3-compatible store.

Kept apart from the operations (:mod:`s3_compatible`) because every one of
them reaches the store through these clients, which are built once per
process and replaced as they age.

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
    STORAGE_RANGE_READ_TIMEOUT_S - read timeout for small ranged reads, which
                                   fail fast so a dead pooled connection is
                                   retried quickly (default: 5)
    STORAGE_MAX_POOL_CONNECTIONS - boto3 connection pool size (default: 32)
    STORAGE_ADDRESSING_STYLE  - Bucket addressing: virtual (default) | path | auto
    STORAGE_BROWSER_ENDPOINT_URL - Endpoint browsers reach for signed download
                                   links, when it differs from STORAGE_ENDPOINT_URL
    STORAGE_KEY_PREFIX        - Namespace for every key this process stores, for
                                environments that share one bucket (default: none)

Endpoint / addressing:
    AWS S3:        No endpoint needed, just credentials + bucket + region
    Cloudflare R2: STORAGE_ENDPOINT_URL=https://{account_id}.r2.cloudflarestorage.com
                   STORAGE_REGION=auto
    MinIO:         STORAGE_ENDPOINT_URL=http://localhost:9000  + STORAGE_ADDRESSING_STYLE=path
    Other S3-compatible: STORAGE_ENDPOINT_URL=https://{bare-host}  (no bucket subdomain)

Most cloud S3-compatible services use virtual-hosted addressing (the default);
self-hosted / path-only backends (e.g. MinIO) require STORAGE_ADDRESSING_STYLE=path.
For a custom endpoint, give the bare host with no bucket subdomain: embedding the
bucket double-prefixes object keys under virtual addressing.
"""

import logging
import os
import threading
import time
from collections.abc import Callable
from typing import Any

import boto3
from botocore.config import Config

from src.utils.storage.key_prefix import KEY_PREFIX, full_key

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
    # The endpoint a browser reaches the store at, when it differs from the
    # one this process uses (a MinIO addressed by its compose service name).
    # Links handed to a browser are signed for this host instead.
    BROWSER_ENDPOINT_URL = os.getenv("STORAGE_BROWSER_ENDPOINT_URL")
    MAX_UPLOAD_SIZE = int(os.getenv("STORAGE_MAX_UPLOAD_SIZE", str(10 * 1024 * 1024)))

    DEFAULT_IMAGE_PREFIX = os.getenv("STORAGE_DEFAULT_IMAGE_PREFIX", "images/")
    DEFAULT_CHART_PREFIX = os.getenv("STORAGE_DEFAULT_CHART_PREFIX", "charts/")

    CONNECT_TIMEOUT_S = int(os.getenv("STORAGE_CONNECT_TIMEOUT_S", "5"))
    READ_TIMEOUT_S = int(os.getenv("STORAGE_READ_TIMEOUT_S", "30"))
    RANGE_READ_TIMEOUT_S = int(os.getenv("STORAGE_RANGE_READ_TIMEOUT_S", "5"))

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
_BROWSER_CLIENT: Any | None = None
_CLIENT_MU = threading.Lock()


# A client older than this is rebuilt. Connections only live inside a client,
# so none sits idle anywhere near the store's close interval, however traffic
# is spread across the pool.
_CLIENT_MAX_AGE_S = 120


class _ExpiringClient:
    """A lazily built client that is replaced once it is old.

    The store closes an idle connection after a fixed interval and the close
    can be lost on the way, so a pooled connection can look alive after it is
    dead and the next call waits out the read timeout before the SDK retries.
    Bounding the client's age bounds every connection's idle time, and costs
    one handshake per connection each interval. The replaced client is closed
    shortly after, so a call still running on it finishes.
    """

    def __init__(self, factory: Callable[[], Any]) -> None:
        self._factory = factory
        self._client: Any | None = None
        self._born = 0.0

    def get(self) -> Any:
        client = self._client
        if client is not None and time.monotonic() - self._born <= _CLIENT_MAX_AGE_S:
            return client
        with _CLIENT_MU:
            if (
                self._client is None
                or time.monotonic() - self._born > _CLIENT_MAX_AGE_S
            ):
                old, self._client = self._client, self._factory()
                self._born = time.monotonic()
                _retire(old)
            return self._client

    def reset(self) -> None:
        with _CLIENT_MU:
            old, self._client = self._client, None
        _retire(old)


# A thread can take a client just before it is replaced and reach its pool just
# after, and a request on a closed pool fails outright. The replaced client is
# closed once that window has passed; a request already running is unaffected.
_RETIRED_CLOSE_DELAY_S = 30


def _close_quietly(client: Any | None) -> None:
    if client is None:
        return
    try:
        client.close()
    except Exception:
        logger.debug("Closing a replaced storage client failed", exc_info=True)


def _retire(client: Any | None) -> None:
    if client is None:
        return
    timer = threading.Timer(_RETIRED_CLOSE_DELAY_S, _close_quietly, args=(client,))
    timer.daemon = True
    timer.start()


_SHARED = _ExpiringClient(lambda: _build_client())
# Ranged GETs must not validate response checksums: an object uploaded with a
# full-object checksum returns that header on a ranged response too, and the
# SDK then checks the partial body against it and fails every time. Callers
# reading a range verify the bytes they get by their own means.
_RANGED = _ExpiringClient(
    lambda: _build_client(response_checksum_validation="when_required")
)
# A small slice that has not started replying within a few seconds is a dead
# pooled connection, not a slow transfer. Failing fast lets the SDK's retry
# open a fresh connection instead of waiting out the full read timeout.
_SLICE = _ExpiringClient(
    lambda: _build_client(
        response_checksum_validation="when_required",
        read_timeout=min(
            StorageConfig.RANGE_READ_TIMEOUT_S, StorageConfig.READ_TIMEOUT_S
        ),
    )
)


def _get_client() -> Any:
    """Return the shared S3-compatible client."""
    return _SHARED.get()


def _get_range_client() -> Any:
    """Return the client for streamed reads that skip checksum validation."""
    return _RANGED.get()


def _get_slice_client() -> Any:
    """Return the fail-fast client for small ranged reads."""
    return _SLICE.get()


def _get_browser_client() -> Any:
    """The client that signs links a browser follows; signing makes no request."""
    if not StorageConfig.BROWSER_ENDPOINT_URL:
        return _get_client()
    global _BROWSER_CLIENT
    if _BROWSER_CLIENT is not None:
        return _BROWSER_CLIENT
    with _CLIENT_MU:
        if _BROWSER_CLIENT is None:
            _BROWSER_CLIENT = _build_client(
                endpoint_url=StorageConfig.BROWSER_ENDPOINT_URL
            )
    return _BROWSER_CLIENT


def _build_client(*, endpoint_url: str | None = None, **config_overrides: Any) -> Any:
    config_args: dict[str, Any] = {
        "read_timeout": StorageConfig.READ_TIMEOUT_S,
        **config_overrides,
    }
    kwargs: dict[str, Any] = {
        "aws_access_key_id": StorageConfig.ACCESS_KEY_ID,
        "aws_secret_access_key": StorageConfig.SECRET_ACCESS_KEY,
        "region_name": StorageConfig.REGION,
        "config": Config(
            signature_version="s3v4",
            s3={"addressing_style": StorageConfig.ADDRESSING_STYLE},
            retries={"max_attempts": 3, "mode": "standard"},
            connect_timeout=StorageConfig.CONNECT_TIMEOUT_S,
            max_pool_connections=StorageConfig.MAX_POOL_CONNECTIONS,
            **config_args,
        ),
    }
    if endpoint_url or StorageConfig.ENDPOINT_URL:
        kwargs["endpoint_url"] = endpoint_url or StorageConfig.ENDPOINT_URL
    client = boto3.client("s3", **kwargs)
    if KEY_PREFIX:
        # On the client rather than at each call, so no operation, presigned
        # or not, can reach the bucket outside the namespace.
        client.meta.events.register("before-parameter-build.s3", _prefix_object_key)
    return client


def _prefix_object_key(params: dict[str, Any], **_: Any) -> None:
    if "Key" in params:
        params["Key"] = full_key(params["Key"])


def _reset_client_for_test() -> None:
    """Drop the cached clients. Test-only — production never re-initializes."""
    global _BROWSER_CLIENT
    _SHARED.reset()
    _RANGED.reset()
    _SLICE.reset()
    with _CLIENT_MU:
        _BROWSER_CLIENT = None
