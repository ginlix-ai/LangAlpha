"""Attachment content-block predicates over a message payload.

Pure predicates with no middleware or graph dependency, beside
``reasoning_payload`` for the same reason it gives: the layer that owns provider
routing also owns the shape of what routing has to carry.

One home because the enumeration had already drifted. Seven places named these
block types by hand — the token counter, the base64 stripper, the offloader, the
read-side strip, the tool-result normalizer, eviction, and the vendored GLM
client — and three of them still did not know Anthropic's ``document``.
"""

from __future__ import annotations

from typing import Any

#: Blocks carrying an image. Covers the Anthropic-native ``source`` shape and
#: the langchain v1 ``base64`` one, matched on the type rather than a payload key
#: so a shape that drifts on a library bump is still recognised.
IMAGE_BLOCK_TYPES = frozenset({"image", "image_url"})

#: Blocks carrying a document. ``file`` is what we write; ``document`` is
#: Anthropic's own name for the same thing, so a block echoed back by a provider
#: or built by hand arrives under it.
FILE_BLOCK_TYPES = frozenset({"file", "document"})

#: What a model has to support natively to accept the block at all.
ATTACHMENT_BLOCK_TYPES = IMAGE_BLOCK_TYPES | FILE_BLOCK_TYPES

#: Legal beside an attachment and readable by every route, so it never needs
#: stripping and never on its own makes a list an attachment payload.
COMPANION_BLOCK_TYPES = frozenset({"text"})


def block_type(block: Any) -> str | None:
    """The ``type`` of a content block, or ``None`` if this is not a block."""
    if not isinstance(block, dict):
        return None
    value = block.get("type")
    return value if isinstance(value, str) else None


def is_text_block(block: Any) -> bool:
    return block_type(block) == "text"


#: Keys under which a block carries the bytes themselves: ``source`` is
#: Anthropic's envelope, ``base64`` langchain v1's, ``file_data`` OpenAI's file
#: input. A block with none of them names an attachment without being one.
PAYLOAD_KEYS = frozenset({"source", "base64", "file_data"})


def _carries_payload(block: dict) -> bool:
    if any(block.get(key) for key in PAYLOAD_KEYS):
        return True
    # OpenAI's file input nests the bytes one level down, under ``file``.
    nested = block.get("file")
    return isinstance(nested, dict) and any(nested.get(key) for key in PAYLOAD_KEYS)


def is_image_block(block: Any) -> bool:
    """True for a block that is an image, not one that merely mentions one.

    The payload check is what separates an attachment from a web-search hit. A
    verbose Tavily or Bocha search answers with ``{"type": "image",
    "image_url": "https://...", "image_description": ...}``, and on a query that
    returns pictures and no pages that is the whole tool result. Matching the
    type alone would call it an attachment, and the normalizer would then hand
    the provider a list of blocks no route can read instead of the search JSON
    the agent asked for.
    """
    kind = block_type(block)
    if kind == "image_url":
        # OpenAI's envelope is an object holding the URL, and the URL is the
        # payload: a bare string here is the search-result shape above, and an
        # envelope without one names an image the provider cannot fetch.
        envelope = block.get("image_url")
        if not isinstance(envelope, dict):
            return False
        url = envelope.get("url")
        return isinstance(url, str) and bool(url)
    return kind == "image" and _carries_payload(block)


def is_file_block(block: Any) -> bool:
    return block_type(block) in FILE_BLOCK_TYPES and _carries_payload(block)


def is_attachment_block(block: Any) -> bool:
    return is_image_block(block) or is_file_block(block)


def has_attachment(content: Any) -> bool:
    """True if block-form ``content`` carries an image or a document."""
    return isinstance(content, list) and any(is_attachment_block(b) for b in content)


def is_attachment_payload(content: Any) -> bool:
    """True if ``content`` is a block list whose point is an attachment.

    Both halves are load-bearing. Every element has to be a block we recognise,
    or an arbitrary list of typed dicts would reach the provider as content. And
    at least one has to be an attachment carrying its bytes, because a list of
    plain text blocks reads identically as a string, and a list of search hits
    that merely name pictures is tool output rather than a payload. Treating
    either as one would change the shape of a tool result the model needs.
    """
    if not isinstance(content, list) or not content:
        return False
    known = ATTACHMENT_BLOCK_TYPES | COMPANION_BLOCK_TYPES
    return all(block_type(b) in known for b in content) and has_attachment(content)
