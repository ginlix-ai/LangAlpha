"""An attached image the turn's route can't be given reaches the agent as a
sandbox file, never as an image block, so it cannot fail the call or the thread
after it. One the fit can convert, or the route takes as is, stays an image."""

import base64
import io
import types

import pytest
from PIL import Image

from src.server.handlers.chat.attachments import attach_request_files
from src.server.models.chat import ChatRequest


def _data_url(mime: str, raw: bytes) -> str:
    return f"data:{mime};base64,{base64.b64encode(raw).decode()}"


def _image(fmt: str) -> bytes:
    buffer = io.BytesIO()
    Image.new("RGB", (8, 8), "white").save(buffer, fmt)
    return buffer.getvalue()


def _request(data: str) -> ChatRequest:
    return ChatRequest.model_validate({
        "messages": [{"role": "user", "content": "what is this?"}],
        "additional_context": [{"type": "image", "data": data, "description": "chart.x"}],
    })


class _Sandbox:
    def __init__(self):
        self.files: dict[str, bytes] = {}

    def normalize_path(self, path, project=None):
        return f"/home/workspace/{path}"

    async def aupload_file_bytes(self, path, data):
        self.files[path] = data
        return True


async def _attach(data: str, model: str):
    sandbox = _Sandbox()
    [message] = await attach_request_files(
        [{"role": "user", "content": "what is this?"}],
        _request(data),
        types.SimpleNamespace(sandbox=sandbox),
        model,
        types.SimpleNamespace(input_modalities=None),
    )
    return message["content"], sandbox


UNREADABLE = {
    "svg": _data_url(
        "image/svg+xml", b'<svg xmlns="http://www.w3.org/2000/svg" width="1" height="1"/>'
    ),
    "truncated png": _data_url("image/png", _image("PNG")[:40]),
}

# An ISO-BMFF header with a HEIC brand, all the check reads; no decoder here.
_HEIC = _data_url("image/heic", b"\0\0\0\x18ftypheic\0\0\0\0mif1heic" + b"\0" * 64)


@pytest.mark.parametrize("model", ["deepseek-flash", "glm-5.2"])
@pytest.mark.parametrize("data", UNREADABLE.values(), ids=UNREADABLE.keys())
@pytest.mark.asyncio
async def test_an_image_no_provider_decodes_goes_to_the_sandbox_as_a_file(data, model):
    """On a vision model and a text-only one alike: the agent is told where the
    file is, and is not told to switch models, which would not help."""
    content, sandbox = await _attach(data, model)

    assert isinstance(content, str)
    [saved] = sandbox.files
    path = saved.removeprefix("/home/workspace/")
    mime = data.split(";", 1)[0].removeprefix("data:")
    assert content == (
        "what is this?\n\n<system-reminder>\n"
        f"The user attached a file (chart.x, {mime}). It has been saved to {path}. "
        "Use Python to process it.\n</system-reminder>"
    )


@pytest.mark.asyncio
async def test_one_with_no_payload_is_still_named_without_a_block():
    content, sandbox = await _attach("data:image/png;base64", "deepseek-flash")
    assert not sandbox.files
    assert content == (
        "what is this?\n\n<system-reminder>\n"
        "The user attached a file (chart.x, image/png).\n</system-reminder>"
    )


def _as_block(sandbox, data):
    """The content of an upload that went as an image, labelled with its copy."""
    [saved] = sandbox.files
    path = saved.removeprefix("/home/workspace/")
    return [
        {"type": "text", "text": f"[Attached image: chart.x (saved to {path})]"},
        {"type": "image_url", "image_url": {"url": data}},
        {"type": "text", "text": "what is this?"},
    ]


@pytest.mark.parametrize("fmt", ["PNG", "JPEG", "GIF", "WEBP", "BMP", "TIFF"])
@pytest.mark.asyncio
async def test_an_image_pillow_decodes_is_a_block_for_a_vision_model(fmt):
    """The four common types go as they are; the rest the fit converts."""
    data = _data_url(f"image/{fmt.lower()}", _image(fmt))
    content, sandbox = await _attach(data, "deepseek-flash")
    assert content == _as_block(sandbox, data)


@pytest.mark.asyncio
async def test_heic_is_a_block_where_the_route_takes_it_and_a_file_elsewhere():
    content, sandbox = await _attach(_HEIC, "gemini-3.8-flash")
    assert content == _as_block(sandbox, _HEIC)

    content, _ = await _attach(_HEIC, "deepseek-flash")
    assert isinstance(content, str)
    assert "The user attached a file (chart.x, image/heic)" in content


@pytest.mark.asyncio
async def test_the_route_is_read_off_the_client_being_called():
    """A user-defined model has no manifest row; its client's stamp names the
    provider, and so whether HEIC goes as an image."""
    [message] = await attach_request_files(
        [{"role": "user", "content": "what is this?"}],
        _request(_HEIC),
        types.SimpleNamespace(sandbox=_Sandbox()),
        "My Gemini",
        types.SimpleNamespace(
            input_modalities=["text", "image"],
            llm_client=types.SimpleNamespace(
                metadata={"manifest_model": "My Gemini", "pricing_provider": "gemini"}
            ),
        ),
    )
    assert message["content"][1] == {"type": "image_url", "image_url": {"url": _HEIC}}

