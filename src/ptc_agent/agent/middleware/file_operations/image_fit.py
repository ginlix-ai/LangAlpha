"""Fitting each image in an outgoing request to the types and pixel limit of its target.

An image block is written into history once and replayed on every later call,
so an image past a provider's limit is not one failed call: the provider
refuses every call after it on the thread, until the user switches models. The
limit belongs to the model actually called (a fallback, a subagent, a later
switch), and two providers tighten it with the number of images in the request,
so it can only be judged per call. The fit is made on the request copy: history
keeps the original, a thread already stuck recovers on its next call, and each
model gets the most detail it can take.

The same goes for the format. Each route takes its own set of image types,
so one it does not take is converted to PNG on the way out, and its declared
media type is corrected from its bytes, which a provider checks. Also home to
the format checks the ways an image comes in share, since bytes no provider
decodes strand a thread the same way.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import io
import re
import struct
import threading
from collections import OrderedDict
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any

from PIL import Image, JpegImagePlugin

from src.llms.attachment_payload import block_type, is_image_block
from src.llms.llm import COMMON_IMAGE_TYPES

# Format names to MIME, for the formats some route takes as is. MPO is Pillow's
# name for the multi-picture JPEG many cameras and phones write: its first
# picture is a baseline JPEG, which is what a provider decodes. HEIC and HEIF
# are named by the header read alone, since Pillow has no decoder for them.
_FORMAT_TO_MIME = {
    "PNG": "image/png",
    "JPEG": "image/jpeg",
    "MPO": "image/jpeg",
    "GIF": "image/gif",
    "WEBP": "image/webp",
    "BMP": "image/bmp",
    "TIFF": "image/tiff",
    "HEIC": "image/heic",
    "HEIF": "image/heif",
}
_NO_DECODER = frozenset({"HEIC", "HEIF"})


class UnreadableImage(ValueError):
    """Bytes that do not open as an image at all."""


class UnsupportedImageFormat(UnreadableImage):
    """An image, in a format no provider accepts."""

    def __init__(self, image_format: str | None) -> None:
        self.image_format = image_format or "an unknown"
        super().__init__(f"in {self.image_format} format, which no provider accepts")


def _decodable_format(content: bytes) -> str:
    """Pillow's name for the format of bytes it decodes, else raise ``UnreadableImage``.

    Structural rather than a magic-byte peek: what passes is checkpointed, and
    a provider that cannot decode it refuses every later call on the thread.
    """
    try:
        img = Image.open(io.BytesIO(content))
        img.verify()
    except Exception:
        raise UnreadableImage("not a readable image") from None
    return img.format or ""


def identify_image(content: bytes) -> str:
    """MIME type of image bytes in one of the four common formats, else raise
    ``UnreadableImage``. What the Read tool attaches."""
    image_format = _decodable_format(content)
    mime_type = _FORMAT_TO_MIME.get(image_format)
    if mime_type not in COMMON_IMAGE_TYPES:
        raise UnsupportedImageFormat(image_format)
    return mime_type


def usable_image(content: bytes, accepted: frozenset[str]) -> bool:
    """Whether an attachment can reach a route taking ``accepted`` as an image.

    Anything Pillow decodes can, since the fit converts what the route does not
    take. Of the rest, only a HEIC or HEIF the route takes as is: Pillow has no
    decoder for those, so their header is all there is to judge them by.
    """
    try:
        _decodable_format(content)
        return True
    except UnreadableImage:
        pass
    try:
        probe = _sniff(lambda offset, size: content[offset : offset + size])
    except Exception:
        return False
    return (
        probe is not None
        and probe[0] in _NO_DECODER
        and _FORMAT_TO_MIME[probe[0]] in accepted
    )


_SANDBOX_TAIL = " For fine detail, crop the original in the sandbox."

# The label an upload saved to the sandbox carries (``inject_multimodal_context``
# writes it just before the image), the one place its sandbox copy is stated.
_SAVED_UPLOAD_LABEL = re.compile(r"\[Attached image: .* \(saved to [^()]+\)\]", re.DOTALL)
# The same label with no path: the image block is the upload's only copy.
_UPLOAD_LABEL = re.compile(r"\[Attached image: (.*)\]", re.DOTALL)


@dataclass(frozen=True)
class _Fitted:
    data: str
    mime: str
    original: tuple[int, int]
    size: tuple[int, int]


class _Memo:
    """Fits already made, so a thread pays for each image's resize once per
    worker rather than on every model call of every turn.

    Safe as process state because it holds no truth: the fit is a pure function
    of the bytes and the limit, so every worker computes identical output, and
    a miss only costs the resize again.
    """

    def __init__(self, budget: int) -> None:
        self._items: OrderedDict[tuple[bytes, int], _Fitted] = OrderedDict()
        self._held = 0
        self._budget = budget
        self._lock = threading.Lock()

    def get(self, key: tuple[bytes, int]) -> _Fitted | None:
        with self._lock:
            hit = self._items.get(key)
            if hit is not None:
                self._items.move_to_end(key)
            return hit

    def put(self, key: tuple[bytes, int], fitted: _Fitted) -> None:
        with self._lock:
            if key in self._items:
                return
            self._items[key] = fitted
            self._held += len(fitted.data)
            while self._held > self._budget and self._items:
                _, dropped = self._items.popitem(last=False)
                self._held -= len(dropped.data)


_MEMO = _Memo(budget=64 * 1024 * 1024)

# A render holds the decoded source, a premultiplied copy when it has alpha,
# and the resize's passes at once: measured at 9 to 15 bytes per source pixel.
# An image over 48 million pixels (8000 x 6000) is not decoded, so one render
# peaks around 720 MiB, and renders run one at a time per process so that bound
# is the process's too. A JPEG is held to it at the scale libjpeg decodes it.
_MAX_DECODE_PIXELS = 8000 * 6000
_RENDER_POOL = ThreadPoolExecutor(max_workers=1, thread_name_prefix="image-fit")
_JPEG_SCALES = (1, 2, 4, 8)


def _least_scale(width: int, height: int) -> int | None:
    """The smallest JPEG decode scale that keeps the image under the ceiling."""
    for scale in _JPEG_SCALES:
        if -(-width // scale) * -(-height // scale) <= _MAX_DECODE_PIXELS:
            return scale
    return None


def _decodable(image_format: str, size: tuple[int, int] | None) -> bool:
    if size is None:
        return True
    if image_format in ("JPEG", "MPO"):
        return _least_scale(*size) is not None
    return size[0] * size[1] <= _MAX_DECODE_PIXELS


def _data_url(url: Any) -> tuple[str, str] | None:
    """Payload and media type of a base64 data URL."""
    if isinstance(url, str) and url.startswith("data:") and ";base64," in url:
        header, data = url.split(";base64,", 1)
        return data, header[5:]
    return None


def _inline(block: dict) -> tuple[str, Any, Callable[[str, str], dict]] | None:
    """An image block's base64 payload, its declared media type, and how to
    rebuild it with another payload and type.

    Covers the shapes an image arrives in: OpenAI's ``image_url`` data URL,
    Anthropic's ``source``, and langchain's ``base64`` or data ``url``. One that
    points at a URL has no bytes here to fit and is left to the provider.
    """
    kind = block_type(block)
    if kind == "image_url":
        envelope = block.get("image_url")
        parsed = _data_url(envelope.get("url")) if isinstance(envelope, dict) else None
        if parsed is None:
            return None
        return *parsed, lambda d, m: {
            **block,
            "image_url": {**envelope, "url": f"data:{m};base64,{d}"},
        }
    if kind != "image":
        return None
    source = block.get("source")
    if isinstance(source, dict) and source.get("type") == "base64":
        data = source.get("data")
        if isinstance(data, str):
            return data, source.get("media_type"), lambda d, m: {
                **block,
                "source": {**source, "media_type": m, "data": d},
            }
    if isinstance(block.get("base64"), str):
        return block["base64"], block.get("mime_type"), lambda d, m: {
            **block,
            "base64": d,
            "mime_type": m,
        }
    parsed = _data_url(block.get("url"))
    if parsed is not None:
        return *parsed, lambda d, m: {**block, "url": f"data:{m};base64,{d}"}
    return None


def _is_image(block: Any) -> bool:
    """Counts toward the request's image total, inline or not."""
    if is_image_block(block):
        return True
    return block_type(block) == "image" and isinstance(block.get("url"), str)


# What a header read settles: the format (Pillow's name for it, or HEIC/HEIF,
# which Pillow has no decoder for) and the size, None where the header that
# names the format does not state one.
_Probe = tuple[str, tuple[int, int] | None]

# A payload with line breaks puts bytes at other offsets than the arithmetic
# assumes; MIME wraps at 76 characters, so a look at its start finds them.
_NOT_BASE64 = re.compile(r"[^A-Za-z0-9+/=]")
_JPEG_SOF = frozenset({0xC0, 0xC1, 0xC2, 0xC3, 0xC5, 0xC6, 0xC7, 0xC9, 0xCA, 0xCB, 0xCD, 0xCE, 0xCF})
_HEIC_BRANDS = frozenset({b"heic", b"heix", b"hevc", b"hevx", b"heim", b"heis"})
_HEIF_BRANDS = frozenset({b"mif1", b"msf1", b"heif"})


def _window(data: str) -> Callable[[int, int], bytes]:
    """Reads of the decoded bytes that decode only the base64 quanta they
    cover, so a header costs a few dozen bytes whatever the image's size."""

    def read(offset: int, size: int) -> bytes:
        start = offset // 3 * 4
        end = -(-(offset + size) // 3) * 4
        chunk = base64.b64decode(data[start:end], validate=True)
        return chunk[offset % 3 : offset % 3 + size]

    return read


def _jpeg_size(read: Callable[[int, int], bytes]) -> tuple[int, int] | None:
    """Walks the segment headers to the frame header, skipping the metadata
    (EXIF, ICC profiles) that can put it hundreds of kilobytes in."""
    pos = 2
    for _ in range(256):
        seg = read(pos, 9)
        if len(seg) < 4 or seg[0] != 0xFF:
            return None
        marker = seg[1]
        if marker == 0xFF:
            pos += 1
        elif marker in _JPEG_SOF:
            height, width = struct.unpack(">HH", seg[5:9])
            return (width, height) if width and height else None
        elif marker == 0x01 or 0xD0 <= marker <= 0xD8:
            pos += 2
        elif marker in (0xD9, 0xDA):
            return None
        else:
            pos += 2 + struct.unpack(">H", seg[2:4])[0]
    return None


def _sniff(read: Callable[[int, int], bytes]) -> _Probe | None:
    """Format and size from the header alone, None for a format it does not know.

    Not Pillow: opening a WebP there builds the animation decoder just to
    report a size. Bytes it cannot place go to Pillow off the event loop.
    """
    head = read(0, 32)
    if head.startswith(b"\x89PNG\r\n\x1a\n") and head[12:16] == b"IHDR":
        return "PNG", struct.unpack(">II", head[16:24])
    if head[:6] in (b"GIF87a", b"GIF89a"):
        return "GIF", struct.unpack("<HH", head[6:10])
    if head[:4] == b"RIFF" and head[8:12] == b"WEBP":
        chunk = head[12:16]
        if chunk == b"VP8X":
            width = int.from_bytes(head[24:27], "little") + 1
            return "WEBP", (width, int.from_bytes(head[27:30], "little") + 1)
        if chunk == b"VP8L" and head[20] == 0x2F:
            bits = int.from_bytes(head[21:25], "little")
            return "WEBP", ((bits & 0x3FFF) + 1, ((bits >> 14) & 0x3FFF) + 1)
        if chunk == b"VP8 " and head[23:26] == b"\x9d\x01\x2a":
            width, height = struct.unpack("<HH", head[26:30])
            return "WEBP", (width & 0x3FFF, height & 0x3FFF)
        return None
    if head[:2] == b"BM":
        if int.from_bytes(head[14:18], "little") == 12:
            return "BMP", struct.unpack("<HH", head[18:22])
        width, height = struct.unpack("<ii", head[18:26])
        return "BMP", (abs(width), abs(height))
    if head[:3] == b"\xff\xd8\xff":
        size = _jpeg_size(read)
        return ("JPEG", size) if size else None
    if head[4:8] == b"ftyp":
        box = read(0, min(int.from_bytes(head[:4], "big"), 256))
        brands = {box[k : k + 4] for k in range(8, len(box) - 3, 4)} - {box[12:16]}
        if brands & _HEIC_BRANDS:
            return "HEIC", None
        if brands & _HEIF_BRANDS and b"avif" not in brands:
            return "HEIF", None
    return None


def _probe_header(data: str) -> _Probe | None | bool:
    """Read on the event loop. False when only the whole payload can tell."""
    if _NOT_BASE64.search(data, 0, 4096):
        return False
    try:
        return _sniff(_window(data)) or False
    except Exception:
        return False


def _probe_whole(data: str) -> _Probe | None:
    """Off the event loop: the header read could not settle it. None when the
    bytes are not an image anything here can place."""
    try:
        raw = base64.b64decode(data)
    except Exception:
        return None
    try:
        if probe := _sniff(lambda offset, size: raw[offset : offset + size]):
            return probe
    except Exception:
        pass
    try:
        with Image.open(io.BytesIO(raw)) as img:
            return img.format, img.size
    except Exception:
        return None


# EXIF orientation to the transpose that undoes it, as ``ImageOps.exif_transpose``
# applies it. Applied to the resized image rather than the source so that a
# rotated photo does not cost a second full-size copy.
_UPRIGHT = {
    2: Image.Transpose.FLIP_LEFT_RIGHT,
    3: Image.Transpose.ROTATE_180,
    4: Image.Transpose.FLIP_TOP_BOTTOM,
    5: Image.Transpose.TRANSPOSE,
    6: Image.Transpose.ROTATE_270,
    7: Image.Transpose.TRANSVERSE,
    8: Image.Transpose.ROTATE_90,
}


_SWAPS_SIDES = frozenset(
    {
        Image.Transpose.TRANSPOSE,
        Image.Transpose.ROTATE_270,
        Image.Transpose.TRANSVERSE,
        Image.Transpose.ROTATE_90,
    }
)


def _orientation(img: Image.Image) -> int:
    """The EXIF orientation, 1 (upright) when absent or unreadable: a broken
    tag should cost the rotation, not the image."""
    try:
        return int(img.getexif().get(0x0112, 1))
    except Exception:
        return 1


_PNG_MODES = frozenset({"1", "L", "LA", "I", "I;16", "P", "RGB", "RGBA"})


def _to_8_bit(img: Image.Image) -> Image.Image:
    """Greyscale at more than 8 bits, scaled down to 8.

    A plain conversion clips every value above 255, which turns a 16-bit image
    white. A 16-bit mode is scaled by its full range; a 32-bit or float one by
    the range its values use, unless they already sit in 8 bits (or in 0 to 1,
    for float).
    """
    if img.mode.startswith("I;16"):
        low, high, span = 0, 65535, 65535
    else:
        low, high = img.getextrema()
        if img.mode == "F" and 0 <= low and high <= 1:
            low, span = 0, 1
        elif 0 <= low and high <= 255:
            return img.convert("L")
        elif img.mode == "I" and 0 <= low and high <= 65535:
            low, span = 0, 65535
        else:
            span = (high - low) or 1
    source = img if img.mode == "F" else img.convert("I")
    scale = 255 / span
    return source.point(lambda v: (v - low) * scale).convert("L")


def _resample(img: Image.Image, size: tuple[int, int]) -> Image.Image:
    """Resize in a mode LANCZOS can blend, returning to a palette where the
    source had one so a flat chart does not come back many times its bytes."""
    if img.mode.startswith("I") or img.mode == "F":
        img = _to_8_bit(img)
    if size == img.size and img.mode in ("1", "L", "LA", "P", "RGB", "RGBA"):
        # A conversion alone: nothing to blend, and a palette stays exact.
        img.load()
        return img
    if img.mode in ("P", "PA"):
        rgba = img.mode == "PA" or "transparency" in img.info
        out = img.convert("RGBA" if rgba else "RGB").resize(
            size, Image.Resampling.LANCZOS, reducing_gap=3.0
        )
        method = Image.Quantize.FASTOCTREE if rgba else Image.Quantize.MEDIANCUT
        return out.quantize(colors=256, method=method, dither=Image.Dither.NONE)
    if img.mode == "1":
        img = img.convert("L")
    elif img.mode not in ("L", "LA", "RGB", "RGBA", "CMYK"):
        has_alpha = "A" in img.getbands() or "transparency" in img.info
        img = img.convert("RGBA" if has_alpha else "RGB")
    return img.resize(size, Image.Resampling.LANCZOS, reducing_gap=3.0)


def _render(data: str, limit: int | None, convert: bool) -> _Fitted | None:
    """Re-encode an image so neither side exceeds ``limit`` (None keeps its
    size), as PNG when ``convert`` says the route does not take its format.
    None if it does not decode.

    The output is turned upright by the source's EXIF orientation, since the
    tag is not carried over and a phone photo would otherwise reach the model
    on its side. Byte-identical for the same input and arguments, which the
    prompt cache depends on: the encoders are deterministic and nothing time-
    or host-dependent is written. JPEG keeps the source's quantization tables,
    so the output's bytes follow its pixel count down instead of being
    re-encoded at a guessed quality.
    """
    key = (hashlib.blake2b(data.encode(), digest_size=16).digest(), limit or 0, convert)
    if (hit := _MEMO.get(key)) is not None:
        return hit
    try:
        with Image.open(io.BytesIO(base64.b64decode(data))) as img:
            fmt = img.format
            width, height = img.size
            size = (width, height)
            if limit is not None and max(width, height) > limit:
                scale = limit / max(width, height)
                size = (
                    max(1, min(limit, round(width * scale))),
                    max(1, min(limit, round(height * scale))),
                )
            icc = img.info.get("icc_profile")
            options: dict[str, Any] = {"icc_profile": icc} if icc else {}
            if fmt in ("JPEG", "MPO"):
                # libjpeg decodes at 1/2, 1/4 or 1/8 scale: the smallest the
                # target allows, and no larger than the ceiling allows, which
                # can leave the result under the limit rather than at it.
                least = _least_scale(width, height) or 8
                request = (min(size[0], width // least), min(size[1], height // least))
                img.draft(img.mode, request)
                size = (min(size[0], img.size[0]), min(size[1], img.size[1]))
            if img.size[0] * img.size[1] > _MAX_DECODE_PIXELS:
                return None
            jpeg = fmt in ("JPEG", "MPO") and not convert
            if jpeg:
                if img.quantization:
                    options["qtables"] = img.quantization
                sampling = JpegImagePlugin.get_sampling(img)
                if sampling != -1:
                    options["subsampling"] = sampling
            out = _resample(img, size)
            # Read once decoded, not before: Pillow's TIFF loader turns the
            # image upright itself and drops the tag (its size is upright from
            # the open), and a second turn would undo the first.
            turn = _UPRIGHT.get(_orientation(img))
            if turn is not None:
                out = out.transpose(turn)
                if turn in _SWAPS_SIDES:
                    width, height = height, width
            buffer = io.BytesIO()
            if jpeg:
                out.save(buffer, "JPEG", **options)
                mime = "image/jpeg"
            elif fmt == "WEBP" and not convert:
                out.save(buffer, "WEBP", quality=90, **options)
                mime = "image/webp"
            else:
                # GIF too: only its first frame was ever read, and PNG holds a
                # still frame without the 256-colour cap.
                if out.mode not in _PNG_MODES:
                    out = out.convert("RGBA" if "A" in out.getbands() else "RGB")
                out.save(buffer, "PNG", **options)
                mime = "image/png"
    except Exception:
        return None
    fitted = _Fitted(
        data=base64.b64encode(buffer.getvalue()).decode("ascii"),
        mime=mime,
        original=(width, height),
        size=out.size,
    )
    _MEMO.put(key, fitted)
    return fitted


def _note(fitted: _Fitted, in_sandbox: bool) -> dict[str, str]:
    (ow, oh), (w, h) = fitted.original, fitted.size
    tail = _SANDBOX_TAIL if in_sandbox else ""
    return {
        "type": "text",
        "text": (
            f"[Image downscaled from {ow}x{oh} to {w}x{h} pixels "
            f"to fit this model's limit.{tail}]"
        ),
    }


def _saved_upload(label: Any) -> bool:
    text = label.get("text") if block_type(label) == "text" else None
    return isinstance(text, str) and _SAVED_UPLOAD_LABEL.fullmatch(text) is not None


def unsaved_upload_name(label: Any) -> str | None:
    """The name an upload's label gives when no copy of it was saved, or None
    when ``label`` is no such label."""
    text = label.get("text") if block_type(label) == "text" else None
    if not isinstance(text, str) or _saved_upload(label):
        return None
    match = _UPLOAD_LABEL.fullmatch(text)
    return match.group(1) if match else None


def _path_read_calls(messages: list) -> set[str]:
    """Ids of tool calls that read a file by path, not by URL: their image has
    an original in the sandbox the model can go back to for detail."""
    ids: set[str] = set()
    for msg in messages:
        for call in getattr(msg, "tool_calls", None) or []:
            path = (call.get("args") or {}).get("file_path")
            call_id = call.get("id")
            if (
                call_id
                and isinstance(path, str)
                and path
                and not path.startswith(("http://", "https://"))
            ):
                ids.add(call_id)
    return ids


async def fit_images(
    messages: list,
    accepted: frozenset[str],
    max_side: Callable[[int], int | None],
) -> tuple[list, frozenset[tuple[int, int]]]:
    """Fit every image block in ``messages`` to the target's types and side limit.

    ``accepted`` is the MIME types the target takes as is; an image in another
    format Pillow decodes is converted to PNG, and every block's media type is
    set from its bytes. ``max_side`` maps the request's image count to the
    limit. A downscaled image is followed by a text block saying so, which
    points at the original when the sandbox holds one: an upload saved there,
    or a Read by path.

    Returns the messages, the original list when nothing changed, and the
    positions (message, block) in them of the images the target can't be
    given. Those are left in place for the caller to replace in its own pass,
    so that the guidance on the first placeholder lands on the first in the
    history whatever kind it is. Never mutates a message: these are checkpoint
    objects shared with the rest of the graph.
    """
    located: list[tuple[int, int, str, Any, Callable[[str, str], dict]]] = []
    total = 0
    for i, msg in enumerate(messages):
        content = getattr(msg, "content", None)
        if not isinstance(content, list):
            continue
        for j, block in enumerate(content):
            if not _is_image(block):
                continue
            total += 1
            payload = _inline(block)
            if payload is not None:
                located.append((i, j, *payload))
    if not located:
        return messages, frozenset()

    probes: dict[tuple[int, int], _Probe | None] = {}
    deferred = []
    for i, j, data, _, _ in located:
        probe = _probe_header(data)
        if probe is False:
            deferred.append((i, j, data))
        else:
            probes[(i, j)] = probe
    if deferred:
        probed = await asyncio.gather(
            *(asyncio.to_thread(_probe_whole, data) for _, _, data in deferred)
        )
        probes.update(((i, j), probe) for (i, j, _), probe in zip(deferred, probed))
    # Unknown bytes, and a format the target does not take that Pillow cannot
    # convert, can only be dropped. They are out of the count the limit reads.
    dropped = {
        key
        for key, probe in probes.items()
        if probe is None
        or (probe[0] in _NO_DECODER and _FORMAT_TO_MIME[probe[0]] not in accepted)
    }
    limit = max_side(total - len(dropped))

    renders = []
    relabels: dict[tuple[int, int], str] = {}
    for i, j, data, declared, _ in located:
        if (i, j) in dropped:
            continue
        image_format, size = probes[(i, j)]
        mime = _FORMAT_TO_MIME.get(image_format) or Image.MIME.get(image_format)
        over = limit is not None and size is not None and max(size) > limit
        if mime in accepted and not over:
            if mime != declared:
                relabels[(i, j)] = mime
        elif not _decodable(image_format, size):
            dropped.add((i, j))
        else:
            renders.append((i, j, data, limit if over else None, mime not in accepted))
    fitted: dict[tuple[int, int], _Fitted] = {}
    if renders:
        loop = asyncio.get_running_loop()
        results = await asyncio.gather(
            *(
                loop.run_in_executor(_RENDER_POOL, _render, data, cap, convert)
                for _, _, data, cap, convert in renders
            )
        )
        for (i, j, *_), result in zip(renders, results):
            if result is None:
                dropped.add((i, j))
            else:
                fitted[(i, j)] = result
    if not fitted and not relabels:
        return messages, frozenset(dropped)

    payloads = {(i, j): (data, rebuild) for i, j, data, _, rebuild in located}
    path_reads = _path_read_calls(messages) if fitted else set()
    unreadable: set[tuple[int, int]] = set()
    result = list(messages)
    for i in sorted({i for i, _ in dropped | fitted.keys() | relabels.keys()}):
        msg = messages[i]
        read_by_path = getattr(msg, "tool_call_id", None) in path_reads
        blocks: list = []
        for j, block in enumerate(msg.content):
            if (i, j) in dropped:
                # Where it sits in the returned message, past any notes added.
                unreadable.add((i, len(blocks)))
                blocks.append(block)
            elif (fit := fitted.get((i, j))) is not None:
                blocks.append(payloads[(i, j)][1](fit.data, fit.mime))
                if fit.size != fit.original:
                    in_sandbox = read_by_path or (j > 0 and _saved_upload(msg.content[j - 1]))
                    blocks.append(_note(fit, in_sandbox))
            elif (mime := relabels.get((i, j))) is not None:
                data, rebuild = payloads[(i, j)]
                blocks.append(rebuild(data, mime))
            else:
                blocks.append(block)
        if len(blocks) != len(msg.content) or any(
            new is not old for new, old in zip(blocks, msg.content)
        ):
            result[i] = msg.model_copy(update={"content": blocks})
    return result, frozenset(unreadable)


__all__ = [
    "UnreadableImage",
    "UnsupportedImageFormat",
    "fit_images",
    "identify_image",
    "usable_image",
]
