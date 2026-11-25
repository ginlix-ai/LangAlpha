"""S3-compatible cloud storage uploader.

A single module for all S3-compatible services: AWS S3, Cloudflare R2,
MinIO, and any other service supporting the S3 API.

The only difference between providers is configuration (endpoint, credentials).
All use boto3 under the hood.

Dependencies:
    pip install boto3

Configuration and the clients live in :mod:`s3_client`.

Multipart uploads:
    An upload opened and never completed keeps its parts as storage that no
    listing shows. This module aborts on every failure it can see, but that
    is a cost optimisation, not the guarantee: a process that dies mid-upload
    aborts nothing. The bucket's own AbortIncompleteMultipartUpload lifecycle
    rule is what actually reaps them, and it is required configuration. R2
    applies one after 7 days by default; S3 has none until you add it.
"""

import base64
import hashlib
import logging
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from botocore.exceptions import (
    ClientError,
    HTTPClientError,
    IncompleteReadError,
)
from botocore.exceptions import ConnectionError as BotocoreConnectionError

from src.utils.mime import resolve_content_type
from src.utils.storage.key_prefix import full_key
from src.utils.storage.s3_client import (
    StorageConfig,
    _get_browser_client,
    _get_client,
    _get_range_client,
    _get_slice_client,
)

logger = logging.getLogger(__name__)


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


def _warn_if_retried(response: dict[str, Any], key: str) -> None:
    """The SDK retries a dead connection silently; this makes the stall visible."""
    retries = response.get("ResponseMetadata", {}).get("RetryAttempts", 0)
    if retries:
        logger.warning(f"Read of {key} needed {retries} retries")


def get_bytes(key: str) -> bytes | None:
    """Download an object's raw bytes. Returns None on failure or missing object."""
    try:
        client = _get_client()
        response = client.get_object(Bucket=StorageConfig.BUCKET_NAME, Key=key)
        _warn_if_retried(response, key)
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


def _read_range(client: Any, key: str, start: int, length: int) -> bytes | None:
    response = client.get_object(
        Bucket=StorageConfig.BUCKET_NAME,
        Key=key,
        Range=f"bytes={start}-{start + length - 1}",
    )
    _warn_if_retried(response, key)
    body = response.get("Body")
    if body is None:
        return None
    try:
        data = body.read()
    finally:
        body.close()
    return data if isinstance(data, bytes) else bytes(data)


def get_bytes_range(key: str, start: int, length: int) -> bytes | None:
    """Download ``length`` bytes of an object from offset ``start``.

    ``None`` on failure or a missing object, like :func:`get_bytes`. A zero
    length returns ``b""`` without a request: an empty HTTP range is invalid.

    The fail-fast client covers a reply that never starts, which the SDK
    retries. A body that stalls past its short timeout is outside the SDK's
    retry, so a transport failure is tried once more on the full-timeout client.
    """
    if length <= 0:
        return b""
    try:
        try:
            return _read_range(_get_slice_client(), key, start, length)
        except (HTTPClientError, BotocoreConnectionError, IncompleteReadError):
            logger.warning(f"Ranged read of {key} failed, retrying with the full timeout")
            return _read_range(_get_range_client(), key, start, length)
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
    return f"{StorageConfig.get_public_url_base()}/{full_key(key)}"


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


# The store's own multipart limits, and the part size chosen against them.
# Every part but the last must be at least _MIN_PART_BYTES and they must all
# be the same size for stores that require uniform parts, so one size is
# picked here and the last part is whatever remains. _TARGET_PART_BYTES is
# what a retry costs and what a stalled request has to move before its socket
# timeout; it only grows for an object too large to fit _MAX_PARTS at that
# size, which is the one case where the store's ceiling binds first.
_MIN_PART_BYTES = 5 * 1024 * 1024
_MAX_PARTS = 10_000
_TARGET_PART_BYTES = 64 * 1024 * 1024


def create_signed_multipart_upload(
    key: str,
    *,
    content_length: int,
    content_type: str,
    expires_in: int = 900,
) -> tuple[str, list[dict[str, Any]]] | None:
    """Open a multipart upload and presign one PUT per part.

    Returns ``(upload_id, parts)``, each part carrying its number, URL and
    byte length, or ``None`` when the object is not worth splitting or the
    store refused; the caller's single-PUT signature covers both. Unlike
    :func:`get_signed_upload_url`, nothing here binds the object to its
    digest: multipart offers only a checksum-of-checksums for SHA-256, and
    the per-part digests are not known at signing time. Each part's length is
    signed, so an uploader cannot write more than it was granted, and the
    digest a reader checks against the key is verified when the bytes are
    read back.
    """
    part_size = max(_MIN_PART_BYTES, _TARGET_PART_BYTES, -(-content_length // _MAX_PARTS))
    if content_length <= part_size:
        # One part is a PUT with extra steps, and a zero-length one is
        # rejected outright. The caller's single-PUT signature covers this.
        return None
    part_count = -(-content_length // part_size)
    upload_id = open_multipart_upload(key, content_type=content_type)
    if upload_id is None:
        return None

    parts: list[dict[str, Any]] = []
    for number in range(1, part_count + 1):
        offset = (number - 1) * part_size
        size = min(part_size, content_length - offset)
        url = sign_upload_part(
            key, upload_id, part_number=number, size=size, expires_in=expires_in
        )
        if url is None:
            # Nothing has been uploaded yet, but the open upload would sit as
            # an invisible partial object until a lifecycle rule reaped it.
            abort_multipart_upload(key, upload_id)
            return None
        parts.append({"part_number": number, "offset": offset, "size": size, "url": url})
    return upload_id, parts


def open_multipart_upload(key: str, *, content_type: str) -> str | None:
    """Start a multipart upload under ``key``; its upload id, or ``None`` if refused."""
    try:
        created = _get_client().create_multipart_upload(
            Bucket=StorageConfig.BUCKET_NAME, Key=key, ContentType=content_type
        )
        return created["UploadId"]
    except Exception as e:
        logger.error(f"Failed to open a multipart upload for {key}: {e}")
        return None


def sign_upload_part(
    key: str,
    upload_id: str,
    *,
    part_number: int,
    size: int,
    expires_in: int = 900,
    for_browser: bool = False,
    content_md5: str | None = None,
) -> str | None:
    """Presign one part's PUT with its length signed in; ``None`` if signing failed.

    The length binds the body, so whoever holds the URL writes exactly that
    many bytes to that part or nothing. ``content_md5`` (base64) is signed in
    too when given, and the store then refuses a body with other bytes; the
    uploader sends it as ``Content-MD5``. MD5 because a part's SHA-256 is not
    something every S3-compatible store checks. ``for_browser`` signs for
    ``STORAGE_BROWSER_ENDPOINT_URL`` when one is set, as a download link does.
    """
    params: dict[str, Any] = {
        "Bucket": StorageConfig.BUCKET_NAME,
        "Key": key,
        "UploadId": upload_id,
        "PartNumber": part_number,
        "ContentLength": size,
    }
    if content_md5:
        params["ContentMD5"] = content_md5
    try:
        client = _get_browser_client() if for_browser else _get_client()
        return client.generate_presigned_url("upload_part", Params=params, ExpiresIn=expires_in)
    except Exception as e:
        logger.error(f"Failed to presign part {part_number} of {key}: {e}")
        return None


def upload_part(
    key: str, upload_id: str, *, part_number: int, data: bytes, content_md5: str
) -> str | None:
    """Send one part of an open upload from this process; its ETag, or ``None``
    when the store refused it or did not answer."""
    try:
        sent = _get_client().upload_part(
            Bucket=StorageConfig.BUCKET_NAME,
            Key=key,
            UploadId=upload_id,
            PartNumber=part_number,
            Body=data,
            ContentMD5=content_md5,
        )
        return sent["ETag"]
    except Exception as e:
        logger.error(f"Failed to send part {part_number} of {key}: {e}")
        return None


# A copy is made in ranges of this size, side by side. One CopyObject of a
# large object outlasts the client's read timeout: the store answers only once
# the whole copy is made (measured on R2, 2 GiB: one call timed out, 8 ranges
# of 256 MiB took 12 s). Ranges grow past it only for an object that would
# not fit _MAX_PARTS at that size; at the store's 5 TiB object limit that is
# about 525 MiB, well inside the 5 GiB a copied part may be.
_COPY_PART_BYTES = 256 * 1024 * 1024
_COPY_WORKERS = 8


def copy_object(src: str, dst: str, size: int) -> bool:
    """Copy ``src`` to ``dst`` inside the store, no bytes passing through here.

    The source is named in full: the key-prefix hook rewrites only ``Key``,
    and the SDK's managed copy reads its source through that hook too, so it
    would look under the prefix twice. Blocking; callers run it in a thread.
    """
    client = _get_client()
    bucket = StorageConfig.BUCKET_NAME
    source = f"{bucket}/{full_key(src)}"
    if size <= _COPY_PART_BYTES:
        try:
            client.copy_object(Bucket=bucket, Key=dst, CopySource=source)
            return True
        except Exception as e:
            logger.error(f"Failed to copy {src} to {dst}: {e}")
            return False
    upload_id = open_multipart_upload(dst, content_type="application/octet-stream")
    if upload_id is None:
        return False
    part = max(_COPY_PART_BYTES, -(-size // _MAX_PARTS))

    def copy_range(number: int) -> tuple[int, str]:
        start = (number - 1) * part
        end = min(size, start + part) - 1
        copied = client.upload_part_copy(
            Bucket=bucket,
            Key=dst,
            UploadId=upload_id,
            PartNumber=number,
            CopySource=source,
            CopySourceRange=f"bytes={start}-{end}",
        )
        return number, copied["CopyPartResult"]["ETag"]

    try:
        with ThreadPoolExecutor(_COPY_WORKERS) as pool:
            parts = list(pool.map(copy_range, range(1, -(-size // part) + 1)))
    except Exception as e:
        logger.error(f"Failed to copy {src} to {dst} in parts: {e}")
        abort_multipart_upload(dst, upload_id)
        return False
    if complete_multipart_upload(dst, upload_id, parts):
        return True
    abort_multipart_upload(dst, upload_id)
    return False

def list_uploaded_parts(key: str, upload_id: str) -> list[dict[str, Any]] | None:
    """The parts the store holds for an open upload, in order, as it counts them.

    Each is ``{"part_number", "size", "etag"}``. ``None`` when the store cannot
    say, including for an upload that no longer exists. Read from the store
    rather than taken from the uploader, so assembling never trusts a client's
    word for which parts arrived or how large they were.
    """
    parts: list[dict[str, Any]] = []
    marker = 0
    try:
        client = _get_client()
        while True:
            page = client.list_parts(
                Bucket=StorageConfig.BUCKET_NAME,
                Key=key,
                UploadId=upload_id,
                PartNumberMarker=marker,
            )
            for part in page.get("Parts") or []:
                parts.append(
                    {
                        "part_number": int(part["PartNumber"]),
                        "size": int(part["Size"]),
                        "etag": part["ETag"],
                    }
                )
            if not page.get("IsTruncated"):
                break
            marker = int(page["NextPartNumberMarker"])
    except Exception as e:
        logger.error(f"Failed to list the parts of {key}: {e}")
        return None
    parts.sort(key=lambda p: p["part_number"])
    return parts


def object_size(key: str) -> int | None:
    """The stored object's length, or ``None`` when there is none or the store did not say."""
    try:
        head = _get_client().head_object(Bucket=StorageConfig.BUCKET_NAME, Key=key)
        return int(head["ContentLength"])
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") not in ("404", "NoSuchKey"):
            logger.error(f"Failed to read the size of {key}: {e}")
        return None
    except Exception as e:
        logger.error(f"Failed to read the size of {key}: {e}")
        return None


def complete_multipart_upload(
    key: str, upload_id: str, parts: list[tuple[int, str]]
) -> bool:
    """Assemble an uploaded multipart into its object. ``parts`` is (number, etag)."""
    try:
        client = _get_client()
        client.complete_multipart_upload(
            Bucket=StorageConfig.BUCKET_NAME,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={
                "Parts": [
                    {"PartNumber": number, "ETag": etag}
                    for number, etag in sorted(parts)
                ]
            },
        )
        return True
    except Exception as e:
        logger.error(f"Failed to complete multipart upload for {key}: {e}")
        return False


def sha256_object(
    key: str,
    deadline_for: Callable[[int], float] | None = None,
    stop: threading.Event | None = None,
) -> str | None:
    """Hash the bytes the store holds under ``key``, streamed. ``None`` if unreadable.

    The store checks a multipart part for its length only, so reading the
    assembled object back is the one way to learn what it actually holds.
    The SDK's own checksum check is off, as for a range: this is that check.
    ``deadline_for`` maps the object's size to seconds for the whole read:
    the socket timeout only catches a store gone silent, not one that
    trickles, and this runs in a thread nothing can cancel; ``stop`` set
    ends it at the next chunk, answering ``None``.
    """
    body = None
    try:
        response = _get_range_client().get_object(
            Bucket=StorageConfig.BUCKET_NAME, Key=key
        )
        body = response["Body"]
        ends = None
        if deadline_for is not None:
            ends = time.monotonic() + deadline_for(int(response["ContentLength"]))
        digest = hashlib.sha256()
        for chunk in body.iter_chunks(chunk_size=64 * 1024):
            if ends is not None and time.monotonic() > ends:
                raise TimeoutError("read-back passed its deadline")
            if stop is not None and stop.is_set():
                return None
            digest.update(chunk)
        return digest.hexdigest()
    except Exception as e:
        logger.error(f"Failed to read back {key}: {e}")
        return None
    finally:
        if body is not None:
            body.close()


def abort_multipart_upload(key: str, upload_id: str) -> bool:
    """Discard an unfinished multipart upload and the parts already stored.

    Returns whether the store agreed, which an upload it no longer knows
    counts as: aborted before, or assembled, it holds no parts. An upload left
    open holds its parts as an object nothing lists and no registry row
    references, so a caller that cannot reach this, or gets ``False`` back,
    has leaked storage that only the bucket's lifecycle rule will reap.
    """
    try:
        client = _get_client()
        client.abort_multipart_upload(
            Bucket=StorageConfig.BUCKET_NAME, Key=key, UploadId=upload_id
        )
        return True
    except ClientError as e:
        if (
            e.response.get("Error", {}).get("Code") == "NoSuchUpload"
            or e.response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 404
        ):
            return True
        logger.error(f"Failed to abort multipart upload for {key}: {e}")
        return False
    except Exception as e:
        logger.error(f"Failed to abort multipart upload for {key}: {e}")
        return False


def get_signed_url(
    key: str,
    expires_in: int = 3600,
    *,
    content_disposition: str | None = None,
    content_type: str | None = None,
    for_browser: bool = False,
) -> str | None:
    """Generate a pre-signed URL for temporary access.

    ``content_disposition`` and ``content_type`` override the stored object's
    headers on the response, so a content-addressed object can be handed to a
    browser under its user-facing name and type. ``for_browser`` signs for
    ``STORAGE_BROWSER_ENDPOINT_URL`` when one is set.
    """
    params: dict[str, Any] = {"Bucket": StorageConfig.BUCKET_NAME, "Key": key}
    if content_disposition:
        params["ResponseContentDisposition"] = content_disposition
    if content_type:
        params["ResponseContentType"] = content_type
    try:
        client = _get_browser_client() if for_browser else _get_client()
        return client.generate_presigned_url(
            "get_object",
            Params=params,
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


def has_multipart_cleanup_rule(
    *, min_days: int = 0, prefix: str | None = None
) -> bool | None:
    """Whether the bucket expires multipart uploads nobody finished.

    ``None`` when the store would not say (no permission, or an API that does
    not serve lifecycle config). An upload whose abort never ran, because the
    process died between opening it and settling it, holds its parts as bytes
    nothing lists; only this rule ever reclaims them. With ``prefix``, only
    the rules over keys under it count, and the soonest of them must wait
    ``min_days``: an upload kept open longer than that is aborted under it.
    """
    try:
        config = _get_client().get_bucket_lifecycle_configuration(
            Bucket=StorageConfig.BUCKET_NAME
        )
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "NoSuchLifecycleConfiguration":
            return False
        return None
    except Exception:
        return None
    days = [
        int(rule["AbortIncompleteMultipartUpload"].get("DaysAfterInitiation") or 0)
        for rule in config.get("Rules", [])
        if rule.get("Status") == "Enabled"
        and "AbortIncompleteMultipartUpload" in rule
        and (prefix is None or full_key(prefix).startswith(_rule_prefix(rule)))
    ]
    return bool(days) and min(days) >= min_days


def _rule_prefix(rule: dict[str, Any]) -> str:
    """The key prefix a lifecycle rule applies to, in any of its spellings."""
    found = rule.get("Filter") or {}
    return (
        found.get("Prefix")
        or (found.get("And") or {}).get("Prefix")
        or rule.get("Prefix")
        or ""
    )


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
