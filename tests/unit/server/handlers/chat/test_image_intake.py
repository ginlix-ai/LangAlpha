"""Tests for src/server/handlers/chat/image_intake.py: an image a request gives
by URL is fetched once and carried as bytes, or the request is refused."""

import asyncio
import base64
import io

import httpx
import pytest
from fastapi import HTTPException
from PIL import Image

from src.server.handlers.chat import image_intake
from src.server.handlers.chat.image_intake import MAX_IMAGE_URLS, inline_image_urls
from src.server.models.chat import ChatMessage, ChatRequest, ContentItem
from src.tools.web.inhouse import guard
from src.tools.web.inhouse.guard import BlockedAddressError


def _png(shade: int = 10) -> bytes:
    buffer = io.BytesIO()
    Image.new("RGB", (4, 4), (shade, 20, 30)).save(buffer, "PNG")
    return buffer.getvalue()


def _image_request(*urls: str) -> ChatRequest:
    content = [ContentItem(type="text", text="what is this?")]
    content += [ContentItem(type="image", image_url=url) for url in urls]
    return ChatRequest(messages=[ChatMessage(role="user", content=content)])


def _serving(monkeypatch, respond):
    fetched: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        fetched.append(str(request.url))
        return respond(request)

    monkeypatch.setattr(guard, "GuardedAsyncTransport", lambda: httpx.MockTransport(handler))
    return fetched


class TestInlineImageUrls:
    @pytest.mark.asyncio
    async def test_a_url_image_is_fetched_once_and_carried_as_bytes(self, monkeypatch):
        png = _png()
        fetched = _serving(monkeypatch, lambda _: httpx.Response(200, content=png))
        data_url = "data:image/png;base64," + base64.b64encode(png).decode()
        url = "https://images.example.com/a.png"

        out = await inline_image_urls(_image_request(url, data_url, url))

        assert fetched == [url]
        assert [item.image_url for item in out.messages[0].content] == [None, data_url, data_url, data_url]

    @pytest.mark.asyncio
    async def test_each_url_carries_its_own_bytes(self, monkeypatch):
        """The fetches run together; each answer still lands on its own URL."""
        served = {f"https://images.example.com/{shade}.png": _png(shade) for shade in (10, 100, 200)}
        _serving(monkeypatch, lambda request: httpx.Response(200, content=served[str(request.url)]))

        out = await inline_image_urls(_image_request(*served))

        assert [base64.b64decode(item.image_url.split(",", 1)[1]) for item in out.messages[0].content[1:]] == list(
            served.values()
        )

    @pytest.mark.asyncio
    async def test_a_request_with_no_url_image_is_left_alone(self, monkeypatch):
        fetched = _serving(monkeypatch, lambda _: None)
        request = _image_request("data:image/png;base64,AAAA")

        assert await inline_image_urls(request) is request
        assert fetched == []

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("respond", "why"),
        [
            ("blocked", "this address is not allowed"),
            (lambda _: httpx.Response(404), "HTTP 404"),
            (lambda _: httpx.Response(200, content=b"<html></html>"), "not a readable image"),
            (lambda _: httpx.Response(200, content=b"\x89PNG" + b"0" * (5 * 1024 * 1024)), "larger than 5 MB"),
        ],
        ids=["blocked", "not-found", "not-an-image", "too-large"],
    )
    async def test_an_image_that_cannot_be_loaded_refuses_the_request(self, monkeypatch, respond, why):
        def blocked(request):
            raise BlockedAddressError("Blocked private/reserved address 10.0.0.5", request=request)

        _serving(monkeypatch, blocked if respond == "blocked" else respond)

        with pytest.raises(HTTPException) as caught:
            await inline_image_urls(_image_request("https://images.example.com/a.png"))

        assert caught.value.status_code == 422
        assert caught.value.detail["code"] == "image_unavailable"
        assert why in caught.value.detail["message"]
        assert "10.0.0.5" not in caught.value.detail["message"]

    @pytest.mark.asyncio
    async def test_a_compressed_answer_refuses_the_request(self, monkeypatch):
        """Inflated bytes pass no cap until they are decoded, so a compressed
        answer is refused rather than read."""
        _serving(monkeypatch, lambda _: httpx.Response(200, content=b"x", headers={"Content-Encoding": "gzip"}))

        with pytest.raises(HTTPException) as caught:
            await inline_image_urls(_image_request("https://images.example.com/a.png"))

        assert "gzip-compressed" in caught.value.detail["message"]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("url", ["https://[::1/a.png", "https://xn--/a.png"], ids=["bad-port", "bad-idna"])
    async def test_a_url_that_does_not_parse_refuses_the_request(self, monkeypatch, url):
        _serving(monkeypatch, lambda _: httpx.Response(200, content=_png()))

        with pytest.raises(HTTPException) as caught:
            await inline_image_urls(_image_request(url))

        assert caught.value.status_code == 422
        assert "not a valid URL" in caught.value.detail["message"]

    @pytest.mark.asyncio
    async def test_a_scheme_in_capitals_is_still_fetched(self, monkeypatch):
        fetched = _serving(monkeypatch, lambda _: httpx.Response(200, content=_png()))

        out = await inline_image_urls(_image_request("HTTPS://images.example.com/a.png"))

        assert fetched == ["https://images.example.com/a.png"]
        assert out.messages[0].content[1].image_url.startswith("data:image/png;base64,")

    @pytest.mark.asyncio
    async def test_too_many_urls_refuse_the_request_before_any_fetch(self, monkeypatch):
        fetched = _serving(monkeypatch, lambda _: httpx.Response(200, content=_png()))
        urls = [f"https://images.example.com/{n}.png" for n in range(MAX_IMAGE_URLS + 1)]

        with pytest.raises(HTTPException) as caught:
            await inline_image_urls(_image_request(*urls))

        assert caught.value.detail["code"] == "too_many_image_urls"
        assert fetched == []

    @pytest.mark.asyncio
    async def test_one_url_repeated_past_the_cap_refuses_the_request(self, monkeypatch):
        """Each repeat becomes its own copy of the bytes, so repeats count."""
        fetched = _serving(monkeypatch, lambda _: httpx.Response(200, content=_png()))

        with pytest.raises(HTTPException) as caught:
            await inline_image_urls(_image_request(*["https://images.example.com/a.png"] * (MAX_IMAGE_URLS + 1)))

        assert caught.value.detail["code"] == "too_many_image_urls"
        assert fetched == []

    @pytest.mark.asyncio
    async def test_images_coming_to_more_than_the_byte_cap_refuse_the_request(self, monkeypatch):
        """Each repeat becomes its own copy of the bytes, so it counts toward the
        total once per time it is given."""
        png = _png()
        fetched = _serving(monkeypatch, lambda _: httpx.Response(200, content=png))
        monkeypatch.setattr(image_intake, "MAX_IMAGE_URL_BYTES", 2 * len(png))
        url = "https://images.example.com/a.png"

        assert len((await inline_image_urls(_image_request(url, url))).messages[0].content) == 3
        with pytest.raises(HTTPException) as caught:
            await inline_image_urls(_image_request(url, url, url))

        assert caught.value.detail["code"] == "image_urls_too_large"
        assert fetched == [url, url]

    @pytest.mark.asyncio
    async def test_the_first_refusal_stops_the_other_fetches(self, monkeypatch):
        """The request is refused whatever the others bring, so they stop
        rather than hold their bytes until each one ends."""
        started: list[str] = []
        stopped: list[str] = []

        async def fetch(url, _cap):
            started.append(url)
            if url.endswith("bad.png"):
                raise guard.UnexpectedStatus(404)
            try:
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                stopped.append(url)
                raise

        monkeypatch.setattr(image_intake, "fetch_capped", fetch)
        slow = [f"https://images.example.com/{n}.png" for n in range(3)]

        with pytest.raises(HTTPException) as caught:
            await asyncio.wait_for(inline_image_urls(_image_request(*slow, "https://images.example.com/bad.png")), 5)

        assert "HTTP 404" in caught.value.detail["message"]
        assert sorted(stopped) == sorted(slow)
