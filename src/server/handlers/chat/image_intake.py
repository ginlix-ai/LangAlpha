"""Images a chat request gives by URL, fetched once at intake.

A provider fetches a URL source again on every request, so a later request can
read other bytes than the ones an earlier answer's thinking was produced
behind, and a URL that expires fails every request after it. The bytes fetched
here are what the thread keeps, as an upload's are.
"""

from __future__ import annotations

import asyncio
import base64
import logging
from collections import Counter
from typing import TYPE_CHECKING

import httpx
from fastapi import HTTPException

from ptc_agent.agent.middleware.file_operations.image_fit import (
    UnreadableImage,
    identify_image,
)
from ptc_agent.agent.middleware.file_operations.multimodal import MAX_CONTENT_BYTES
from src.tools.web.inhouse.guard import (
    BlockedAddressError,
    OverCap,
    UnexpectedEncoding,
    UnexpectedStatus,
    UnresolvableHost,
    fetch_capped,
    fetch_host,
)

if TYPE_CHECKING:
    from src.server.models.chat import ChatRequest

# Hard-coded logger name for backward-compat with existing log routing.
logger = logging.getLogger("src.server.handlers.chat_handler")

#: The most images one request may give by URL, a repeated URL counted each
#: time: each becomes its own copy of the bytes in the request and the thread.
MAX_IMAGE_URLS = 20

#: The most bytes those images may come to, a repeated URL counted each time:
#: about what one request to a provider carries once they are base64.
MAX_IMAGE_URL_BYTES = 20 * 1024 * 1024

#: How many of them are fetched at once.
_CONCURRENT_FETCHES = 4


def _refusal(code: str, message: str) -> HTTPException:
    return HTTPException(status_code=422, detail={"code": code, "message": message})


async def _fetch_image(url: str) -> tuple[str, int]:
    """The image at ``url`` as a data URL and its size, fetched and checked
    the way Read fetches one; a 422 naming the URL when it cannot be."""

    def refuse(why: str) -> HTTPException:
        return _refusal("image_unavailable", f"Could not load the image at {url}: {why}")

    try:
        body = await fetch_capped(url, lambda _body: MAX_CONTENT_BYTES)
    except UnexpectedStatus as exc:
        raise refuse(f"HTTP {exc.status_code}") from None
    except UnexpectedEncoding as exc:
        raise refuse(f"it was sent {exc.encoding}-compressed, and only uncompressed images are read") from None
    except OverCap as exc:
        raise refuse(f"larger than {exc.limit // (1024 * 1024)} MB") from None
    except UnresolvableHost:
        raise refuse("its host could not be found") from None
    except BlockedAddressError as exc:
        logger.warning("[CHAT] Blocked image address for host %s: %s", fetch_host(url), exc)
        raise refuse("this address is not allowed") from None
    except httpx.TimeoutException:
        raise refuse("timed out") from None
    except httpx.RequestError as exc:
        raise refuse(f"network error ({type(exc).__name__})") from None
    except (httpx.InvalidURL, UnicodeError):
        raise refuse("this is not a valid URL") from None
    try:
        mime_type = identify_image(body)
    except UnreadableImage:
        raise refuse("not a readable image") from None
    return f"data:{mime_type};base64,{base64.b64encode(body).decode('ascii')}", len(body)


def _is_web_url(value: object) -> bool:
    """Whether ``value`` names an http(s) URL, read off its scheme alone so a
    URL that does not parse is refused rather than passed on."""
    return isinstance(value, str) and value.partition(":")[0].strip().lower() in ("http", "https")


async def _fetch_all(given: list[str]) -> dict[str, str]:
    """Each URL in ``given`` fetched once, a few at a time; the first refusal
    cancels the rest, since the request is refused whatever they bring."""
    gate = asyncio.Semaphore(_CONCURRENT_FETCHES)
    copies = Counter(given)
    urls = list(copies)
    total = 0

    async def one(url: str) -> str:
        nonlocal total
        async with gate:
            data_url, size = await _fetch_image(url)
        total += size * copies[url]
        if total > MAX_IMAGE_URL_BYTES:
            raise _refusal(
                "image_urls_too_large",
                f"The images this request gives by URL come to more than {MAX_IMAGE_URL_BYTES // (1024 * 1024)} MB.",
            )
        return data_url

    try:
        async with asyncio.TaskGroup() as group:
            tasks = [group.create_task(one(url)) for url in urls]
    except* HTTPException as refused:
        raise refused.exceptions[0] from None
    return {url: task.result() for url, task in zip(urls, tasks, strict=True)}


async def inline_image_urls(request: ChatRequest) -> ChatRequest:
    """The request with each image it gives by an http(s) URL fetched once
    and carried as bytes."""
    given = [
        item.image_url
        for message in request.messages or ()
        if isinstance(message.content, list)
        for item in message.content
        if item.type == "image" and _is_web_url(item.image_url)
    ]
    if not given:
        return request
    if len(given) > MAX_IMAGE_URLS:
        raise _refusal(
            "too_many_image_urls",
            f"A request can give at most {MAX_IMAGE_URLS} images by URL; this one gives {len(given)}.",
        )
    fetched = await _fetch_all(given)
    messages = [
        message.model_copy(
            update={
                "content": [
                    item.model_copy(update={"image_url": fetched[item.image_url]})
                    if item.type == "image" and item.image_url in fetched
                    else item
                    for item in message.content
                ]
            }
        )
        if isinstance(message.content, list)
        else message
        for message in request.messages
    ]
    return request.model_copy(update={"messages": messages})
