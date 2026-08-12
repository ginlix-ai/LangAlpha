"""
Validation utilities for tools.

Provides common validation functions for URLs, data formats, and other inputs.
"""

import asyncio
import io
import logging
import re
from datetime import datetime
from typing import Optional

import httpx
from PIL import Image

logger = logging.getLogger(__name__)


def validate_date_format(date_str: Optional[str]) -> Optional[str]:
    """Validate a YYYY-MM-DD date string, returning it unchanged (None passes)."""
    if date_str is None:
        return None
    if not re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
        raise ValueError(f"Date must be in YYYY-MM-DD format, got: {date_str}")
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError as e:
        raise ValueError(f"Invalid date: {date_str}. {str(e)}")
    return date_str

# OpenAI Vision API supported image formats
# Reference: https://platform.openai.com/docs/guides/vision
OPENAI_SUPPORTED_FORMATS = {
    'image/jpeg',
    'image/png',
    'image/gif',
    'image/webp'
}


async def _check_url_accessible_quick(url: str, timeout: int) -> bool:
    """
    Lenient validation: Quick HEAD request to check if image exists.

    Uses HEAD request (doesn't download) for fast accessibility check.
    Suitable for images that will only be displayed in frontend, not sent to Vision APIs.

    Args:
        url: URL to check
        timeout: Request timeout in seconds

    Returns:
        True if accessible with image content type, False otherwise
    """
    try:
        async with httpx.AsyncClient(http2=True, timeout=float(timeout)) as client:
            response = await client.head(url, follow_redirects=True)
            if response.status_code == 200:
                content_type = response.headers.get('Content-Type', '').lower()
                # For lenient mode, accept any image/* content type
                return content_type.startswith('image/')
            else:
                logger.debug(f"HEAD request failed with status {response.status_code} for {url}")
            return False
    except httpx.TimeoutException:
        logger.debug(f"Timeout during HEAD request for {url}")
        return False
    except Exception as e:
        logger.debug(f"HEAD request error for {url}: {type(e).__name__}: {e}")
        return False


async def _check_url_downloadable(url: str, timeout: int, retry: bool = True) -> bool:
    """
    Strict validation: Full GET request with image decoding validation.

    Uses GET request to download and decode the image, matching what OpenAI's Vision API
    will do. This catches:
    - Slow-downloading images that would timeout on OpenAI
    - Corrupted images that can't be decoded
    - Unsupported image formats

    Note: Content-Type headers are NOT trusted - many CDNs/S3 buckets return incorrect
    headers (e.g., binary/octet-stream). We use PIL's format detection as authoritative.

    Args:
        url: URL to check
        timeout: Request timeout in seconds
        retry: Whether to retry once on timeout/connection errors (default: True)

    Returns:
        True if accessible, downloadable, decodable and has supported format, False otherwise
    """
    # Map PIL format names to MIME types for validation
    PIL_FORMAT_TO_MIME = {
        'JPEG': 'image/jpeg',
        'PNG': 'image/png',
        'GIF': 'image/gif',
        'WEBP': 'image/webp',
    }

    async def _attempt_validation() -> bool:
        """Single validation attempt."""
        try:
            async with httpx.AsyncClient(http2=True, timeout=float(timeout)) as client:
                response = await client.get(url, follow_redirects=True)

                if response.status_code != 200:
                    logger.debug(f"GET request failed with status {response.status_code} for {url}")
                    return False

                # Warn about missing Content-Length header (required by some LLM providers like Qwen)
                # Don't reject - rely on retry logic to filter failing images at runtime
                content_length = response.headers.get('Content-Length')
                if not content_length:
                    logger.warning(
                        f"Missing Content-Length header for {url}. "
                        f"Some multimodal APIs (e.g., Qwen) may reject this image."
                    )

                # Get the image data
                image_data = response.content

                if not image_data:
                    logger.debug(f"Empty image data received from {url}")
                    return False

                # Verify the image is decodable with PIL and check actual format
                # (don't trust Content-Type headers - S3/CDNs often return wrong types)
                try:
                    img = Image.open(io.BytesIO(image_data))
                    # Verify the image by attempting to load it
                    img.verify()

                    # Check if PIL-detected format is supported by OpenAI
                    pil_format = img.format
                    mime_type = PIL_FORMAT_TO_MIME.get(pil_format)

                    if mime_type not in OPENAI_SUPPORTED_FORMATS:
                        logger.debug(
                            f"Unsupported image format '{pil_format}' for {url}. "
                            f"OpenAI only supports: JPEG, PNG, GIF, WEBP"
                        )
                        return False

                    logger.debug(
                        f"Image validation success: {url} "
                        f"(format={pil_format}, size={img.size}, {len(image_data)} bytes)"
                    )
                    return True
                except Exception as e:
                    logger.debug(
                        f"Image decoding failed for {url}: {type(e).__name__}: {e}"
                    )
                    return False

        except httpx.TimeoutException:
            logger.debug(f"Timeout ({timeout}s) downloading image from {url}")
            return False
        except httpx.RequestError as e:
            logger.debug(f"Network error downloading {url}: {type(e).__name__}: {e}")
            return False
        except Exception as e:
            logger.debug(f"Unexpected error validating {url}: {type(e).__name__}: {e}")
            return False

    # First attempt
    result = await _attempt_validation()

    # Retry once on failure if enabled (helps with intermittent CDN issues)
    if not result and retry:
        logger.debug(f"Retrying validation for {url}")
        await asyncio.sleep(0.5)  # Brief delay before retry
        result = await _attempt_validation()
        if result:
            logger.debug(f"Image validation succeeded on retry: {url}")

    return result


async def validate_image_url(url: str, timeout: Optional[int] = None, strict: bool = True) -> Optional[str]:
    """
    Validate image URL and auto-upgrade HTTP to HTTPS when possible.

    Supports two validation modes:
    - Strict mode (default): Uses GET request with full image download and decoding validation
      → For images sent to OpenAI Vision API or other services with strict requirements
      → Validates Content-Type is one of OpenAI's supported formats
      → Actually decodes the image with PIL to ensure it's valid
      → Default 2s timeout to match OpenAI's behavior
      → Includes retry logic for intermittent CDN failures

    - Lenient mode: Uses HEAD request for quick accessibility check
      → For images only displayed in frontend (not sent to Vision APIs)
      → Faster validation, doesn't test download speed or decode image
      → Default 10s timeout for slower CDNs

    For HTTP URLs, automatically attempts to upgrade to HTTPS for compatibility.

    Args:
        url: Image URL to validate
        timeout: Request timeout in seconds (default: 2 for strict, 10 for lenient)
        strict: If True, use GET with full validation; if False, use HEAD (default: True)

    Returns:
        The validated URL (potentially upgraded to HTTPS) if valid, None otherwise

    Examples:
        >>> # Strict mode (for OpenAI Vision API) - 2s timeout, full validation
        >>> validated_url = await validate_image_url("http://example.com/image.jpg")
        >>>
        >>> # Lenient mode (for frontend display) - 10s timeout, quick check
        >>> validated_url = await validate_image_url("http://example.com/image.jpg",
        ...                                           strict=False)
        >>>
        >>> # Custom timeout
        >>> validated_url = await validate_image_url("http://example.com/image.jpg",
        ...                                           timeout=5, strict=True)
    """
    # Set default timeouts based on mode
    if timeout is None:
        timeout = 2 if strict else 10
    # Select appropriate validation function based on mode
    check_func = _check_url_downloadable if strict else _check_url_accessible_quick

    # Try to upgrade HTTP to HTTPS for better compatibility
    if url.startswith("http://"):
        https_url = url.replace("http://", "https://", 1)

        # Prefer HTTPS version
        if await check_func(https_url, timeout):
            logger.debug(f"Upgraded HTTP to HTTPS: {url} -> {https_url}")
            return https_url

        # HTTPS not available
        if strict:
            # Reject HTTP-only in strict mode (LLM providers like Anthropic require HTTPS)
            logger.warning(f"HTTP URL rejected (HTTPS upgrade failed): {url}")
            return None
        else:
            # In lenient mode, can fall back to HTTP if needed
            logger.debug(f"HTTPS upgrade failed, trying HTTP in lenient mode: {url}")
            if await check_func(url, timeout):
                return url
            return None

    # Already HTTPS or other protocol - validate directly
    if await check_func(url, timeout):
        return url

    logger.debug(f"Image URL validation failed for {url}")
    return None
