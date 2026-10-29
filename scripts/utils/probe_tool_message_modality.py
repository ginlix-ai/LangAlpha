#!/usr/bin/env python
"""Probe whether a model reads an image or PDF carried inside a ToolMessage.

Mid-turn attachments ride on the tool result, so "does this route accept an
attachment there" is a per-provider fact the manifest only claims. This asks the
provider.

A secret word is rendered *into* the asset, so a pass means the model read the
pixels or the page rather than merely tolerating the request. That distinction is
the whole point: several routes accept a PDF in a tool result, return 200, and
answer from the filename.

    uv run python scripts/utils/probe_tool_message_modality.py -m gpt-6.1-sol
    uv run python scripts/utils/probe_tool_message_modality.py -m glm-5.3-flash -m qwen3.8-flash-intl --kind pdf
    uv run python scripts/utils/probe_tool_message_modality.py -m gemini-3-pro --json

Verdicts:
    read     the secret word came back; the attachment was understood
    blind    200, no secret word; accepted and ignored (a manifest overclaim)
    refused  the provider rejected the request; the error text is shown
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import io
import json
import os
import random
import sys
from dataclasses import dataclass, field
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "src"))

# A probe is one model call, not an agent turn worth a trace, and the exporter's
# retries print over the table this exists to show. Set it explicitly to keep it.
os.environ.setdefault("LANGSMITH_TRACING", "false")

from langchain_core.messages import AIMessage, HumanMessage, ToolMessage  # noqa: E402
from langchain_core.tools import tool  # noqa: E402
from PIL import Image, ImageDraw, ImageFont  # noqa: E402

from src.llms.llm import create_llm, get_input_modalities  # noqa: E402
from ptc_agent.agent.middleware.file_operations.multimodal import (  # noqa: E402
    attach_to_tool_result,
    build_content_blocks,
)

# Short, unambiguous, and visually distinct once rendered. Drawn from at random
# per run so a model cannot pass by recalling an earlier probe.
_WORDS = [
    "MANGO", "BANJO", "COMET", "FJORD", "GLYPH", "HAVEN", "INGOT", "JOUST",
    "KRILL", "LUMEN", "MIRTH", "NOMAD", "OXIDE", "PRISM", "QUILT", "RIVET",
    "SYRUP", "TONIC", "USHER", "VIXEN", "WHARF", "XENON", "YACHT", "ZEBRA",
]

_TOOL_CALL_ID = "call_probe_toolmsg_0"

_INSTRUCTION = (
    "You called Read on a file. Its content is attached to the tool result.\n"
    "A single English word is written inside it in large letters.\n"
    "Reply with that word and nothing else. Do not call any tool. "
    "If you cannot see the attachment, reply exactly: CANNOT_SEE"
)


@tool("Read")
def _read(file_path: str) -> str:
    """Read a file. Present only so the assistant's tool call is well formed."""
    return ""


def _png_with_word(word: str) -> bytes:
    """A plain white PNG with ``word`` drawn large and centred."""
    img = Image.new("RGB", (640, 240), "white")
    draw = ImageDraw.Draw(img)
    font = None
    for candidate in (
        "/System/Library/Fonts/Supplemental/Arial Bold.ttf",
        "/System/Library/Fonts/Helvetica.ttc",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    ):
        if os.path.exists(candidate):
            try:
                font = ImageFont.truetype(candidate, 96)
                break
            except OSError:
                continue
    if font is None:
        # The default bitmap font is tiny; scale the canvas down around it so
        # the word still fills a usable fraction of the image.
        font = ImageFont.load_default()
        img = Image.new("RGB", (200, 60), "white")
        draw = ImageDraw.Draw(img)
    box = draw.textbbox((0, 0), word, font=font)
    draw.text(
        ((img.width - box[2] + box[0]) / 2, (img.height - box[3] + box[1]) / 2),
        word,
        fill="black",
        font=font,
    )
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def _pdf_with_word(word: str) -> bytes:
    """A one-page PDF with ``word`` as real Helvetica text.

    Hand-built rather than pulled from a rendering library: the harness has to
    run anywhere the backend runs, and a probe that fails because a dev box
    lacks reportlab reports the wrong thing.
    """
    stream = f"BT /F1 72 Tf 60 90 Td ({word}) Tj ET".encode()
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 480 240] "
        b"/Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
        b"<< /Length %d >>\nstream\n%s\nendstream" % (len(stream), stream),
    ]

    out = bytearray(b"%PDF-1.4\n")
    offsets = []
    for number, body in enumerate(objects, start=1):
        offsets.append(len(out))
        out += b"%d 0 obj\n" % number + body + b"\nendobj\n"

    xref_at = len(out)
    out += b"xref\n0 %d\n" % (len(objects) + 1)
    out += b"0000000000 65535 f \n"
    for offset in offsets:
        out += b"%010d 00000 n \n" % offset
    out += b"trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF\n" % (
        len(objects) + 1,
        xref_at,
    )
    return bytes(out)


@dataclass
class Probe:
    model: str
    kind: str
    verdict: str = "?"
    detail: str = ""
    prompt_tokens: int | None = None
    reply: str = ""
    declared: list[str] = field(default_factory=list)


def _messages(kind: str, word: str) -> list:
    """The synthetic history, assembled by the middleware's own two functions.

    Built rather than hand-written so the probed message is the one that ships:
    ``build_content_blocks`` decides the block shapes and ``attach_to_tool_result``
    merges the Read acknowledgment into the first of them. A route can accept a
    bare block list and still reject that merge.
    """
    if kind == "image":
        payload, name, mime = _png_with_word(word), "chart.png", "image/png"
        pages, label = None, "image"
    else:
        payload, name, mime = _pdf_with_word(word), "report.pdf", "application/pdf"
        pages, label = 1, "PDF"

    blocks = build_content_blocks(
        base64.b64encode(payload).decode(), name, mime, pages
    )
    ack = ToolMessage(
        content=f"Loading {label}: {name}", tool_call_id=_TOOL_CALL_ID
    )
    return [
        HumanMessage(content=_INSTRUCTION),
        AIMessage(
            content="",
            tool_calls=[
                {"name": "Read", "args": {"file_path": name}, "id": _TOOL_CALL_ID}
            ],
        ),
        attach_to_tool_result(ack, blocks),
    ]


async def _run_one(model: str, kind: str, word: str, timeout: float) -> Probe:
    probe = Probe(model=model, kind=kind)
    try:
        probe.declared = get_input_modalities(model)
    except Exception:  # noqa: BLE001 - a custom model has no manifest row
        probe.declared = []

    try:
        llm = create_llm(model, max_retries=1)
        reply = await asyncio.wait_for(
            llm.bind_tools([_read]).ainvoke(_messages(kind, word)), timeout=timeout
        )
    except TimeoutError:
        probe.verdict, probe.detail = "refused", f"timed out after {timeout:.0f}s"
        return probe
    except Exception as exc:  # noqa: BLE001 - any provider error is a result
        probe.verdict = "refused"
        probe.detail = " ".join(str(exc).split())[:220]
        return probe

    text = reply.text() if callable(getattr(reply, "text", None)) else str(reply.content)
    probe.reply = " ".join(text.split())[:120]
    usage = getattr(reply, "usage_metadata", None) or {}
    probe.prompt_tokens = usage.get("input_tokens")
    probe.verdict = "read" if word.lower() in text.lower() else "blind"
    return probe


async def _main() -> int:
    parser = argparse.ArgumentParser(
        description="Ask a provider whether it reads an attachment in a tool result."
    )
    parser.add_argument(
        "-m", "--model", action="append", default=[], metavar="NAME",
        help="manifest model name; repeat for several",
    )
    parser.add_argument(
        "--kind", choices=("image", "pdf", "both"), default="both",
        help="which attachment to probe (default: both)",
    )
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--word", help="fix the secret word instead of choosing one")
    parser.add_argument("--json", action="store_true", help="emit JSON, not a table")
    args = parser.parse_args()

    if not args.model:
        parser.error("give at least one -m/--model")

    word = args.word or random.choice(_WORDS)
    kinds = ("image", "pdf") if args.kind == "both" else (args.kind,)

    results = await asyncio.gather(
        *(_run_one(m, k, word, args.timeout) for m in args.model for k in kinds)
    )

    if args.json:
        print(json.dumps([vars(r) for r in results], indent=2))
        return 0

    print(f"\nsecret word: {word}\n")
    width = max(len(r.model) for r in results)
    print(f"{'model'.ljust(width)}  kind   verdict  in-tok  declared")
    print("-" * (width + 46))
    for r in results:
        declared = ",".join(r.declared) or "-"
        tokens = str(r.prompt_tokens if r.prompt_tokens is not None else "-")
        print(
            f"{r.model.ljust(width)}  {r.kind.ljust(5)}  "
            f"{r.verdict.ljust(7)}  {tokens.rjust(6)}  {declared}"
        )
        if r.detail:
            print(f"{' ' * (width + 2)}  ! {r.detail}")
        elif r.verdict == "blind":
            print(f"{' ' * (width + 2)}  replied: {r.reply!r}")

    overclaim = [
        r for r in results
        if r.verdict != "read" and (r.kind in r.declared or (r.kind == "pdf" and "pdf" in r.declared))
    ]
    if overclaim:
        print("\nmanifest overclaims (declared but not read):")
        for r in overclaim:
            print(f"  {r.model} {r.kind}: {r.verdict}")
    return 1 if overclaim else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
