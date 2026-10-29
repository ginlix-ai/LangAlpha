"""Fitting each image in an outgoing request to its target's types and limit.

The limit is passed in as a function of the image count, so most of these use
small images and a small limit; the real per-route numbers and types are pinned
in ``tests/unit/llms/test_max_image_side.py``.
"""

import base64
import io
import types

import pytest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from PIL import Image, ImageDraw

from ptc_agent.agent.middleware.file_operations import image_fit
from ptc_agent.agent.middleware.file_operations.image_fit import (
    fit_images,
    identify_image,
)
from ptc_agent.agent.middleware.file_operations.multimodal import (
    attach_to_tool_result,
    build_content_blocks,
)
from ptc_agent.agent.middleware.file_operations.multimodal_strip import (
    PLACEHOLDER_GUIDANCE,
    MultimodalStripMiddleware,
)
from src.llms.llm import COMMON_IMAGE_TYPES, get_image_types
from src.server.models.additional_context import MultimodalContext
from src.server.utils.multimodal_context import inject_multimodal_context


def _encode(size, fmt="PNG", **options) -> bytes:
    img = Image.new("RGB", size, "white")
    draw = ImageDraw.Draw(img)
    for y in range(0, size[1], 7):
        draw.line([0, y, size[0], y + 3], fill=(30, 90, 200))
    if fmt == "GIF":
        img = img.quantize(16)
    buffer = io.BytesIO()
    img.save(buffer, fmt, **options)
    return buffer.getvalue()


def _image_url(raw: bytes, mime="image/png") -> dict:
    url = f"data:{mime};base64,{base64.b64encode(raw).decode()}"
    return {"type": "image_url", "image_url": {"url": url}}


def _decoded(block: dict) -> Image.Image:
    data = block["image_url"]["url"].split(",", 1)[1]
    return Image.open(io.BytesIO(base64.b64decode(data)))


def _media_type(block: dict) -> str:
    return block["image_url"]["url"].split(";", 1)[0].removeprefix("data:")


# An ISO-BMFF header with a HEIC brand: what an iPhone photo starts with, and
# all the sniffing reads. Pillow has no decoder for it here.
_HEIC = b"\0\0\0\x18ftypheic\0\0\0\0mif1heic" + b"\0" * 64


async def _fit(messages, limit=100, accepted=COMMON_IMAGE_TYPES):
    result, _ = await fit_images(messages, accepted, lambda count: limit)
    return result


@pytest.fixture(autouse=True)
def _fresh_memo(monkeypatch):
    monkeypatch.setattr(image_fit, "_MEMO", image_fit._Memo(budget=1 << 20))


class TestFitImages:
    @pytest.mark.asyncio
    async def test_an_oversized_image_is_downscaled_in_the_copy_only(self):
        image = _image_url(_encode((40, 300)))
        history = [HumanMessage(content=[{"type": "text", "text": "look"}, image])]

        result = await _fit(history)

        assert result is not history
        label, fitted, note = result[0].content
        assert label == {"type": "text", "text": "look"}
        assert _decoded(fitted).size == (13, 100)
        assert note == {
            "type": "text",
            "text": "[Image downscaled from 40x300 to 13x100 pixels to fit this model's limit.]",
        }
        # The checkpoint's copy keeps the original, so a later target with a
        # higher limit gets every pixel it can take.
        assert history[0].content == [{"type": "text", "text": "look"}, image]

    @pytest.mark.asyncio
    async def test_an_image_within_the_limit_costs_no_copy(self):
        history = [HumanMessage(content=[_image_url(_encode((100, 60)))])]
        assert await _fit(history) is history

    @pytest.mark.asyncio
    async def test_no_limit_means_no_fit(self):
        history = [HumanMessage(content=[_image_url(_encode((40, 300)))])]
        assert await _fit(history, limit=None) is history

    @pytest.mark.asyncio
    async def test_the_output_is_byte_identical_across_calls(self, monkeypatch):
        """The prompt cache keys on the bytes: a fit that differed per call would
        rewrite everything after the image on every call of the thread."""
        history = [HumanMessage(content=[_image_url(_encode((300, 40), "JPEG"), "image/jpeg")])]
        first = await _fit(history)
        monkeypatch.setattr(image_fit, "_MEMO", image_fit._Memo(budget=1 << 20))
        second = await _fit(history)
        assert first[0].content == second[0].content

    @pytest.mark.parametrize(
        ("fmt", "mime", "expected"),
        [
            ("PNG", "image/png", "image/png"),
            ("JPEG", "image/jpeg", "image/jpeg"),
            ("WEBP", "image/webp", "image/webp"),
            ("GIF", "image/gif", "image/png"),
        ],
    )
    @pytest.mark.asyncio
    async def test_each_format_keeps_a_sensible_container(self, fmt, mime, expected):
        history = [HumanMessage(content=[_image_url(_encode((40, 300), fmt), mime)])]
        fitted = (await _fit(history))[0].content[0]
        assert fitted["image_url"]["url"].startswith(f"data:{expected};base64,")
        assert max(_decoded(fitted).size) == 100

    @pytest.mark.asyncio
    async def test_a_header_past_the_prefix_is_still_measured(self):
        """A JPEG states its size only after its metadata segments, which can
        outrun the header read; such an image is measured whole, not skipped."""
        raw = _encode((40, 300), "JPEG", icc_profile=b"\0" * (300 * 1024))
        history = [HumanMessage(content=[_image_url(raw, "image/jpeg")])]
        fitted = (await _fit(history))[0].content[0]
        assert _decoded(fitted).size == (13, 100)

    @pytest.mark.asyncio
    async def test_anthropic_and_langchain_shapes_are_rebuilt_in_kind(self):
        raw = base64.b64encode(_encode((40, 300), "GIF")).decode()
        native = {
            "type": "image",
            "source": {"type": "base64", "media_type": "image/gif", "data": raw},
        }
        v1 = {"type": "image", "base64": raw, "mime_type": "image/gif"}
        history = [HumanMessage(content=[native, v1])]

        native_out, _, v1_out, _ = (await _fit(history))[0].content

        assert native_out["source"]["media_type"] == "image/png"
        assert native_out["source"]["data"] != raw
        assert v1_out["mime_type"] == "image/png"
        assert v1_out["base64"] != raw

    @pytest.mark.asyncio
    async def test_the_limit_is_asked_for_the_request_image_count(self):
        """URL images count toward the provider's threshold too, though there are
        no bytes here to fit; an image replaced by a placeholder no longer does."""
        counts = []
        history = [
            HumanMessage(content=[
                _image_url(_encode((10, 10))),
                {"type": "image_url", "image_url": {"url": "https://example.com/a.png"}},
                _image_url(b"not an image"),
            ]),
        ]
        await fit_images(history, COMMON_IMAGE_TYPES, lambda count: counts.append(count))
        assert counts == [2]

    @pytest.mark.asyncio
    async def test_an_image_that_does_not_decode_is_left_for_the_caller_to_replace(self):
        """The strip writes every placeholder of the call in one pass, so the
        guidance lands on the first whatever kind it is."""
        history = [
            HumanMessage(content=[_image_url(b"not an image"), _image_url(b"nor this")]),
        ]
        result, unreadable = await fit_images(history, COMMON_IMAGE_TYPES, lambda count: 100)
        assert result is history
        assert unreadable == {(0, 0), (0, 1)}

    @pytest.mark.asyncio
    async def test_an_unreadable_image_is_reported_where_it_lands_past_a_note(self):
        history = [
            HumanMessage(content=[_image_url(_encode((40, 300))), _image_url(b"not an image")]),
        ]
        result, unreadable = await fit_images(history, COMMON_IMAGE_TYPES, lambda count: 100)
        assert result[0].content[1]["text"].startswith("[Image downscaled")
        assert unreadable == {(0, 2)}
        assert result[0].content[2] == history[0].content[1]


class TestFormats:
    @pytest.mark.asyncio
    async def test_an_mpo_is_a_jpeg_to_every_provider(self):
        """What many phones write: a JPEG with a second frame after the first,
        which Pillow reports as its own format."""
        buffer = io.BytesIO()
        frame = Image.new("RGB", (40, 300), "white")
        frame.save(buffer, "MPO", save_all=True, append_images=[Image.new("RGB", (8, 8))])
        raw = buffer.getvalue()
        assert identify_image(raw) == "image/jpeg"

        history = [HumanMessage(content=[_image_url(raw, "image/jpeg")])]
        fitted = (await _fit(history))[0].content[0]
        assert _media_type(fitted) == "image/jpeg"
        assert _decoded(fitted).format == "JPEG"
        assert _decoded(fitted).size == (13, 100)

    @pytest.mark.asyncio
    async def test_a_format_the_route_does_not_take_is_converted_at_full_size(self):
        history = [HumanMessage(content=[_image_url(_encode((40, 30), "BMP"), "image/bmp")])]
        result = await _fit(history)
        [converted] = result[0].content
        assert _media_type(converted) == "image/png"
        assert _decoded(converted).size == (40, 30)

    @pytest.mark.asyncio
    async def test_a_format_the_route_takes_is_left_alone(self):
        history = [HumanMessage(content=[_image_url(_encode((40, 30), "BMP"), "image/bmp")])]
        assert await _fit(history, accepted=get_image_types("dashscope")) is history

    @pytest.mark.asyncio
    async def test_a_mislabelled_image_is_relabelled_from_its_bytes(self):
        """Anthropic refuses a WebP declared as JPEG, though it takes both."""
        raw = _encode((40, 30), "WEBP")
        url = _image_url(raw, "image/jpeg")
        native = {
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": "image/jpeg",
                "data": base64.b64encode(raw).decode(),
            },
        }
        history = [HumanMessage(content=[url, native])]

        relabelled, native_out = (await _fit(history))[0].content

        assert relabelled["image_url"]["url"] == url["image_url"]["url"].replace(
            "image/jpeg", "image/webp", 1
        )
        assert native_out["source"] == {**native["source"], "media_type": "image/webp"}

    @pytest.mark.asyncio
    async def test_heic_goes_where_it_is_taken_and_is_unreadable_elsewhere(self):
        history = [HumanMessage(content=[_image_url(_HEIC, "image/heic")])]
        for route in ("gemini", "dashscope"):
            result, unreadable = await fit_images(
                history, get_image_types(route), lambda count: None
            )
            assert result is history and not unreadable
        result, unreadable = await fit_images(history, COMMON_IMAGE_TYPES, lambda count: None)
        assert unreadable == {(0, 0)}

    @pytest.mark.asyncio
    async def test_svg_is_unreadable_on_every_route(self):
        svg = b'<svg xmlns="http://www.w3.org/2000/svg" width="10" height="10"/>'
        history = [HumanMessage(content=[_image_url(svg, "image/svg+xml")])]
        for route in ("anthropic", "gemini", "dashscope"):
            _, unreadable = await fit_images(history, get_image_types(route), lambda count: None)
            assert unreadable == {(0, 0)}


class TestPixels:
    @pytest.mark.asyncio
    async def test_a_16_bit_image_keeps_its_tones(self):
        """A plain conversion clips everything over 255, which turns it white."""
        buffer = io.BytesIO()
        Image.new("I;16", (40, 300), 30000).save(buffer, "PNG")
        history = [HumanMessage(content=[_image_url(buffer.getvalue())])]
        out = _decoded((await _fit(history))[0].content[0])
        assert out.getpixel((5, 5)) == 30000 * 255 // 65535

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("fmt", "options"),
        [
            ("JPEG", {"exif": b""}),
            # Pillow turns a TIFF upright as it decodes, both through libtiff
            # and through its own decoder, so the fit must not turn it again.
            ("TIFF", {"compression": "tiff_lzw", "tiffinfo": {0x0112: 6}}),
            ("TIFF", {"compression": None, "tiffinfo": {0x0112: 6}}),
        ],
        ids=["jpeg", "tiff-lzw", "tiff-uncompressed"],
    )
    async def test_a_rotated_photo_is_turned_upright_once(self, fmt, options):
        """The orientation tag does not survive the re-encode, so the pixels
        have to carry it, and the note names the size the user sees."""
        img = Image.new("RGB", (300, 40), "white")
        img.paste((220, 20, 20), (0, 0, 30, 40))
        if "exif" in options:
            exif = Image.Exif()
            exif[0x0112] = 6
            options = {"exif": exif.tobytes()}
        buffer = io.BytesIO()
        img.save(buffer, fmt, **options)
        mime = f"image/{fmt.lower()}"
        history = [HumanMessage(content=[_image_url(buffer.getvalue(), mime)])]

        fitted, note = (await _fit(history))[0].content

        out = _decoded(fitted).convert("RGB")
        assert out.size == (13, 100)
        top, bottom = out.getpixel((6, 2)), out.getpixel((6, 97))
        assert top[0] > 180 and top[1] < 80  # the bar, on the raw left edge
        assert bottom[1] > 180
        assert note["text"].startswith("[Image downscaled from 40x300 to 13x100")

    @pytest.mark.asyncio
    async def test_an_image_over_the_decode_ceiling_is_never_decoded(self, monkeypatch):
        monkeypatch.setattr(image_fit, "_MAX_DECODE_PIXELS", 5000)
        monkeypatch.setattr(image_fit, "_render", None)
        history = [HumanMessage(content=[_image_url(_encode((40, 300)))])]
        result, unreadable = await fit_images(history, COMMON_IMAGE_TYPES, lambda count: 100)
        assert result is history
        assert unreadable == {(0, 0)}

    @pytest.mark.asyncio
    async def test_a_jpeg_over_it_is_decoded_at_a_scale_under_it(self, monkeypatch):
        """libjpeg decodes at a fraction of full size, which is all the fit
        then holds, at the cost of landing under the limit."""
        monkeypatch.setattr(image_fit, "_MAX_DECODE_PIXELS", 5000)
        raw = _encode((40, 300), "JPEG")
        fitted = (await _fit([HumanMessage(content=[_image_url(raw, "image/jpeg")])], limit=200))
        assert _decoded(fitted[0].content[0]).size == (20, 150)


def _read(path: str, raw: bytes):
    ack = ToolMessage(content=f"Read {path}", tool_call_id="call_1", name="Read")
    return [
        AIMessage(content="", tool_calls=[
            {"name": "Read", "args": {"file_path": path}, "id": "call_1", "type": "tool_call"},
        ]),
        attach_to_tool_result(
            ack, build_content_blocks(base64.b64encode(raw).decode(), path, "image/png")
        ),
    ]


def _upload(raw: bytes, saved_to: str | None):
    """An attachment as the turn injects it, labelled with its sandbox path if any."""
    data = f"data:image/png;base64,{base64.b64encode(raw).decode()}"
    ctx = MultimodalContext(type="image", data=data, description="tall (1).png")
    [message] = inject_multimodal_context(
        [{"role": "user", "content": "look"}], [ctx], file_paths=[saved_to]
    )
    return [HumanMessage(content=message["content"])]


_IN_SANDBOX = (
    "[Image downscaled from 40x300 to 13x100 pixels to fit this model's "
    "limit. For fine detail, crop the original in the sandbox.]"
)
_NOWHERE = "[Image downscaled from 40x300 to 13x100 pixels to fit this model's limit.]"


class TestTheNoteNamesTheOriginal:
    @pytest.mark.asyncio
    async def test_a_read_of_a_workspace_path_points_at_the_sandbox(self):
        result = await _fit(_read("work/charts/tall.png", _encode((40, 300))))
        assert result[1].content[-1]["text"] == _IN_SANDBOX

    @pytest.mark.asyncio
    async def test_a_read_of_a_url_has_no_original_there(self):
        result = await _fit(_read("https://example.com/tall.png", _encode((40, 300))))
        assert result[1].content[-1]["text"] == _NOWHERE

    @pytest.mark.asyncio
    async def test_an_upload_saved_to_the_sandbox_points_at_it(self):
        history = _upload(_encode((40, 300)), "work/uploads/tall__1_.png_1a2b3c4d.png")
        label, _, note, ask = (await _fit(history))[0].content
        assert label["text"].startswith("[Attached image: tall (1).png (saved to ")
        assert note["text"] == _IN_SANDBOX
        assert ask == {"type": "text", "text": "look"}

    @pytest.mark.asyncio
    async def test_an_upload_with_no_sandbox_copy_has_no_original_there(self):
        result = await _fit(_upload(_encode((40, 300)), None))
        assert result[0].content[2]["text"] == _NOWHERE


class _Request:
    def __init__(self, manifest_model, messages):
        self.model = types.SimpleNamespace(metadata={"manifest_model": manifest_model})
        self.messages = messages

    def override(self, **kwargs):
        return _Request(self.model.metadata["manifest_model"], kwargs["messages"])


async def _forwarded(model, messages):
    seen = {}

    async def handler(request):
        seen["messages"] = request.messages

    await MultimodalStripMiddleware().awrap_model_call(_Request(model, messages), handler)
    return seen["messages"]


_SVG = _image_url(
    b'<svg xmlns="http://www.w3.org/2000/svg" width="10" height="10"/>', "image/svg+xml"
)


class TestThroughTheStripMiddleware:
    @pytest.mark.asyncio
    async def test_a_thread_stuck_on_an_oversized_image_recovers_on_its_next_call(self):
        """DeepSeek refuses 8193 px on a side and accepts 8192."""
        history = [HumanMessage(content=[_image_url(_encode((10, 9000)))])]
        forwarded = await _forwarded("deepseek-flash", history)
        assert _decoded(forwarded[0].content[0]).size == (9, 8192)
        assert forwarded[0].content[1]["text"].startswith("[Image downscaled from 10x9000")

    @pytest.mark.asyncio
    async def test_a_text_only_target_still_gets_the_strip_not_a_fit(self):
        history = [HumanMessage(content=[_image_url(_encode((10, 9000)))])]
        forwarded = await _forwarded("glm-5.2", history)
        assert len(forwarded[0].content) == 1
        assert "not visible to the current model" in forwarded[0].content[0]["text"]

    @pytest.mark.asyncio
    async def test_an_undecodable_image_on_a_vision_target_gets_the_guided_placeholder(self):
        history = [HumanMessage(content=[_image_url(b"not an image")])]
        forwarded = await _forwarded("claude-sonnet-5-5", history)
        text = forwarded[0].content[0]["text"]
        assert text.startswith("[Image attached in a prior turn, not visible to the current model.")
        assert PLACEHOLDER_GUIDANCE in text

    @pytest.mark.asyncio
    async def test_the_guidance_stays_on_the_first_placeholder_as_the_thread_grows(self):
        """A later placeholder of another kind must not take it: the earlier
        message would change and rewrite the cached history behind it."""
        before = [HumanMessage(content=[_SVG]), AIMessage(content="ok")]
        pdf = {
            "type": "file",
            "mime_type": "application/pdf",
            "base64": base64.b64encode(b"%PDF-1.4").decode(),
            "pages": 5000,
        }
        after = [*before, HumanMessage(content=[pdf])]

        first = await _forwarded("claude-sonnet-5-5", before)
        second = await _forwarded("claude-sonnet-5-5", after)

        assert PLACEHOLDER_GUIDANCE in first[0].content[0]["text"]
        assert second[0].content == first[0].content
        assert PLACEHOLDER_GUIDANCE not in second[2].content[0]["text"]
