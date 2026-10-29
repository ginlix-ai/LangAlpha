"""Read-side half of multimodal support: what the target model can still see.

An image or PDF block is written into graph state once, by the tool call that
produced it, and then replayed on every later turn. The model reading it is not
the one that wrote it — a user can switch mid-thread, a resilience fallback can
land on a text-only candidate, and a subagent runs its own model entirely — so
whether a block is legal is a question only this side can answer, on the call
where the target is actually known.

Split from ``multimodal`` because the two halves share no state and not every
agent wires both: an agent with no filesystem tool has nothing to intercept,
but still inherits history full of blocks it may not be able to read.
"""

from collections.abc import Callable, Collection
from typing import Annotated, Any, Literal

from langchain.agents.middleware import AgentMiddleware, AgentState
from langchain.agents.middleware.types import ExtendedModelResponse, PrivateStateAttr
from langchain_core.messages import AIMessage, ToolMessage
from langgraph.types import Command
from typing_extensions import NotRequired

from ptc_agent.agent.middleware._message_utils import order_tool_results_first
from ptc_agent.agent.middleware.file_operations.image_fit import fit_images, unsaved_upload_name
from ptc_agent.agent.middleware.thinking_binding import checked_thinking_end
from src.llms.attachment_payload import FILE_BLOCK_TYPES, IMAGE_BLOCK_TYPES, block_type
from src.llms.llm import (
    get_image_types,
    get_input_modalities,
    get_max_image_side,
    get_max_image_side_by_count,
    get_max_pdf_pages,
    get_pdf_page_tokens,
    image_route,
)


def _placeholder_for(
    block: Any,
    has_image: bool,
    has_pdf: bool,
    max_pdf_pages: int | None,
    guidance: str = "",
) -> dict[str, str] | None:
    """The text block replacing one this target can't read, or None to keep it.

    ``guidance`` is what to do about it, and rides inside the placeholder so it
    sits next to the block it explains, on the message the model is reading;
    the caller passes it for the first placeholder of a call and nothing after.
    Appending it to the system prompt instead moved the cached prefix on every
    call where a strip fired, which rewrote the whole history behind it.
    """
    kind = block_type(block)
    tail = f" {guidance}" if guidance else ""

    # Matched on the type alone: this is the only thing standing between a
    # mid-thread switch and a 400, and failing closed costs one placeholder
    # where guessing from the payload keys costs the turn.
    if kind in IMAGE_BLOCK_TYPES and not has_image:
        return {
            "type": "text",
            "text": f"[Image attached in a prior turn, not visible to the current model.{tail}]",
        }

    if kind not in FILE_BLOCK_TYPES:
        return None

    # Same reason as "image" above: `pdf` is the only file-modality flag the
    # manifest carries, so a file block we can't classify — mime absent, null,
    # or non-PDF — is one a model without it has no way to accept.
    if not has_pdf:
        return {
            "type": "text",
            "text": f"[PDF attached in a prior turn, not visible to the current model.{tail}]",
        }

    # Over the target's page ceiling. Unstamped blocks pass: they predate the
    # stamp, and re-deriving the count would mean decoding every PDF in history
    # on every call. They keep the old failure mode; nothing that already worked
    # starts failing here.
    pages = block.get("pages")
    if max_pdf_pages is not None and isinstance(pages, int) and pages > max_pdf_pages:
        if _unsaved(block):
            name = block.get("filename") or "file"
            size = f"{pages} page{'' if pages == 1 else 's'}"
            return {
                "type": "text",
                "text": (
                    f"[PDF {name} ({size}) is over the current model's "
                    f"{max_pdf_pages}-page limit and was not sent.{tail}]"
                ),
            }
        return {
            "type": "text",
            "text": (
                f"[PDF attached in a prior turn, {pages} pages, over the current "
                f"model's {max_pdf_pages}-page limit.{tail}]"
            ),
        }
    return None


def read_sources(messages: list) -> dict[str, str]:
    """The path or URL each Read call in ``messages`` was given, by call id.

    A Read result is the only attachment that knows where its bytes came
    from, and only through its call: the image block that carries them goes
    to the provider as it is, so nothing can be stamped on it.
    """
    return {
        call["id"]: call.get("args", {}).get("file_path") or ""
        for msg in messages
        if isinstance(msg, AIMessage)
        for call in msg.tool_calls or ()
        if call.get("name") == "Read" and call.get("id")
    }


def _unsaved(block: Any) -> bool:
    """A composer PDF inlined because it could not be saved anywhere, so the
    agent has no copy to extract from and no path to give."""
    return isinstance(block, dict) and block.get("unsaved") is True


def _source(msg: Any, sources: dict[str, str]) -> str | None:
    return sources.get(msg.tool_call_id) or None if isinstance(msg, ToolMessage) else None


def strip_unsupported_content_blocks(
    messages: list,
    has_image: bool,
    has_pdf: bool,
    max_pdf_pages: int | None = None,
    guidance: str | Callable[[str | None], str] = "",
    unreadable: Collection[tuple[int, int]] = (),
) -> list:
    """Replace image/file blocks this target can't read with text placeholders.

    ``unreadable`` is the positions (message, block) of images this target
    can't be given though it reads images, as ``fit_images`` reports them.

    ``max_pdf_pages`` is the target's own page ceiling (None = unbounded). A PDF
    over it is dropped here rather than refused at injection, because the ceiling
    is a property of the model that reads the block, not of the tool call that
    created it — the same document is fine on a 1M-context route and rejected on
    a 200K one, and only this side knows which one is being called.

    ``guidance`` is one text for every block, or the text for a block given the
    path or URL its Read was given (None for any other attachment).

    Returns the original list when nothing changed, and never mutates a message:
    these are checkpoint objects shared with the rest of the graph.
    """
    result: list = []
    modified = False
    sources = read_sources(messages) if callable(guidance) else {}
    # Each guidance rides on the first placeholder it applies to only: the
    # advice is the same for every block from the same place, and the first
    # is the one whose text never moves when a later turn adds another
    # attachment, so the prefix behind it stays cached.
    said: set[str] = set()

    for i, msg in enumerate(messages):
        content = getattr(msg, "content", None)
        if not isinstance(content, list):
            result.append(msg)
            continue

        advice = guidance(_source(msg, sources)) if callable(guidance) else guidance
        new_blocks = []
        changed = False
        for j, block in enumerate(content):
            # Extraction advice would send the model after a copy that does
            # not exist, so an unsaved block keeps only what to tell the user.
            tail = _IF_NOT if advice and _unsaved(block) else advice
            placeholder = _placeholder_for(
                block,
                has_image and (i, j) not in unreadable,
                has_pdf,
                max_pdf_pages,
                "" if tail in said else tail,
            )
            if placeholder is not None:
                changed = True
                said.add(tail)
            new_blocks.append(block if placeholder is None else placeholder)

        if not changed:
            result.append(msg)
            continue
        modified = True
        result.append(msg.model_copy(update={"content": new_blocks}))

    return result if modified else messages


# What the model should do about a block it cannot see. Rides inside the
# placeholder rather than in the system prompt, so the advice is decided on the
# call where the target is known (a fallback or a subagent is judged on its own
# model) without the cached prefix moving.
_IF_NOT = (
    "If that cannot answer the question, tell the user: an unsupported input type "
    "means switching to a model that accepts it, a page-limit rejection means "
    "splitting the document or a longer-context model. Answer in best effort otherwise."
)
PLACEHOLDER_GUIDANCE = (
    "The file is still in the workspace: extract it yourself with Python in the "
    "sandbox (a PDF's text or page renders, OCR or image analysis) before giving up. "
    + _IF_NOT
)


def placeholder_guidance(source: str | None) -> str:
    """The guidance for a block read from ``source``.

    A URL's bytes are fetched by the server and never written to the
    workspace, so telling the model the file is there sends it looking for
    something that does not exist.
    """
    if source and source.startswith(("http://", "https://")):
        return (
            f"The file is at {source}: download it in the sandbox and extract it "
            "yourself with Python (a PDF's text or page renders, OCR or image "
            "analysis) before giving up. " + _IF_NOT
        )
    return PLACEHOLDER_GUIDANCE


def _removed_placeholder(
    block: Any, name: str | None, pages: int | None, unsaved_image: str | None = None
) -> dict[str, str] | None:
    """The text standing in for an attachment taken out to fit the context
    window, or None for a block that is not one. ``unsaved_image`` names an
    uploaded image whose label says no copy of it was saved."""
    kind = block_type(block)
    if kind in IMAGE_BLOCK_TYPES:
        if unsaved_image is not None:
            # Nothing to Read: the block was its only copy.
            return {
                "type": "text",
                "text": (
                    f"[Image {unsaved_image} was removed from the conversation to fit the "
                    "context window, and no copy was saved. Ask the user to attach it again "
                    "if you need it.]"
                ),
            }
        image = f"Image {name}" if name else "Image"
        return {
            "type": "text",
            "text": f"[{image} was removed from the conversation to fit the context window.]",
        }
    if kind not in FILE_BLOCK_TYPES:
        return None
    name = name or block.get("filename") or "PDF"
    size = f" ({pages} page{'' if pages == 1 else 's'})" if pages else ""
    if _unsaved(block):
        # Nothing to Read: the block was its only copy.
        tail = ", and no copy was saved. Ask the user to attach it again if you need it.]"
    else:
        tail = ". Read it again by page range if you need it.]"
    return {
        "type": "text",
        "text": f"[{name}{size} was removed from the conversation to fit the context window{tail}",
    }


def remove_attachments(
    messages: list,
    ids: Collection[str],
    *,
    pages: Callable[[dict], int | None] = lambda block: block.get("pages"),
) -> list:
    """Replace every attachment in the messages ``ids`` names with the
    placeholder saying it was removed to fit the context window.

    A Read result names its path or URL, which is what reading it again
    takes; another attachment names the file it carries, if any. ``pages``
    gives a PDF block's page count, None when it cannot be read.

    Returns the original list when nothing changed, and never mutates a
    message.
    """
    if not ids:
        return messages
    sources = read_sources(messages)
    result: list = []
    modified = False
    for msg in messages:
        content = getattr(msg, "content", None)
        if getattr(msg, "id", None) not in ids or not isinstance(content, list):
            result.append(msg)
            continue
        name = _source(msg, sources)
        new_blocks = []
        for j, block in enumerate(content):
            placeholder = None
            if isinstance(block, dict):
                count = pages(block) if block_type(block) in FILE_BLOCK_TYPES else None
                # An image block reaches the provider as it is, so whether an
                # upload was saved is read off the label written just before it.
                unsaved = unsaved_upload_name(content[j - 1]) if j else None
                placeholder = _removed_placeholder(block, name, count, unsaved)
            new_blocks.append(block if placeholder is None else placeholder)
        if new_blocks == content:
            result.append(msg)
            continue
        modified = True
        result.append(msg.model_copy(update={"content": new_blocks}))
    return result if modified else messages


#: How a thread's images are fitted to Claude: ``"fixed"``, to the side every
#: image count allows, or ``"count"``, to the side the request's image count
#: allows, as before the fixed side shipped.
IMAGE_SIDE_KEY = "image_side_rule"

# TODO(2026-11-25): delete 30 days after this ships, once the threads written before it have aged out. "count" goes with it.
ImageSideRule = Literal["fixed", "count"]


class ImageSideState(AgentState):
    image_side_rule: NotRequired[Annotated[ImageSideRule, PrivateStateAttr]]


def _image_side_rule(request: Any, route: str) -> tuple[ImageSideRule, ImageSideRule | None]:
    """The thread's rule, and the one to record when it has none yet.

    Chosen once, on the thread's first call to Claude: a thread whose first
    such call already has an image ahead of thinking was sent it by count, and
    fitting it anew would invalidate that thinking, so it keeps the count.
    """
    stored = (getattr(request, "state", None) or {}).get(IMAGE_SIDE_KEY)
    if stored or route != "anthropic":
        return stored or "fixed", None
    messages = request.messages or []
    behind = any(
        isinstance(block, dict) and block_type(block) in IMAGE_BLOCK_TYPES
        for message in messages[: checked_thinking_end(messages)]
        if isinstance(message.content, list)
        for block in message.content
    )
    rule: ImageSideRule = "count" if behind else "fixed"
    return rule, rule


class MultimodalStripMiddleware(AgentMiddleware):
    """Remove attachment blocks the model about to be called cannot read.

    Wired inside the resilience stack so it judges the post-fallback client, and
    shared across the main and subagent stacks: the target is read off each
    request, so one instance serves every model in the graph.
    """

    state_schema = ImageSideState

    def __init__(
        self,
        *,
        model_name: str | None = None,
        custom_modalities: list[str] | None = None,
    ) -> None:
        """
        Args:
            model_name: Configured manifest model, used only to decide whether
                ``custom_modalities`` applies to the target in hand.
            custom_modalities: Per-model modality override from user preferences.
        """
        super().__init__()
        self.model_name = model_name
        self.custom_modalities = custom_modalities

    @classmethod
    def for_config(cls, config: Any) -> "MultimodalStripMiddleware":
        """The strip an agent built from ``config`` runs, which manual
        compaction also judges attachments by when that agent has none."""
        return cls(
            model_name=config.llm.name if config.llm else None,
            custom_modalities=config.input_modalities,
        )

    def _resolve_target(self, request: Any) -> tuple[str | None, list[str]]:
        """Name and modalities of the model this request will actually reach.

        Read off ``request.model`` rather than the configured name so a
        resilience fallback — or a subagent running its own model — is judged on
        the client in hand. The name comes back too because the PDF page ceiling
        is per-model and has to be looked up against the same target.
        """
        return self._target(request.model)

    def _target(self, model: Any) -> tuple[str | None, list[str]]:
        metadata = getattr(model, "metadata", None)
        stamped = metadata.get("manifest_model") if isinstance(metadata, dict) else None

        if not isinstance(stamped, str) or not stamped:
            # Unattributable: the client skipped LLM.get_llm(), which today means
            # a subagent whose config names a bare model string for deepagents to
            # resolve via init_chat_model. Falling back to the configured name
            # would lend a vision parent's modalities to a text-only target and
            # replay exactly the blocks this strip exists to remove, so an
            # unstamped client is judged text-only — the same fail-closed reading
            # LLM.get_llm() documents when it writes the stamp.
            return None, ["text"]
        # ``custom_modalities`` is a per-model override from user preferences, so
        # it describes only the model it was configured for — a fallback is a
        # different model and has to be judged on its own manifest entry.
        overrides = self.custom_modalities if stamped == self.model_name else None
        return stamped, get_input_modalities(stamped, custom_modalities=overrides)

    def receives(self, model: Any) -> Callable[[Any], bool]:
        """Whether a call to ``model`` is sent a block as it is, rather than
        the placeholder this strip writes in its place.

        Compaction, outside the resilience stack, counts only those toward its
        trigger and its fit, judged as here so the two cannot drift. An image
        the call-time fit cannot decode still counts, as the small overcount
        it is.
        """
        stamped, modalities = self._target(model)
        has_image, has_pdf = "image" in modalities, "pdf" in modalities
        max_pdf_pages = get_max_pdf_pages(stamped) if stamped and has_pdf else None
        return lambda block: (
            _placeholder_for(block, has_image, has_pdf, max_pdf_pages, "") is None
        )

    def pdf_page_tokens(self, model: Any) -> int | None:
        """What a call to ``model`` is charged for a PDF page, where its vendor's
        charge was measured; None leaves compaction its default estimate.

        Read off the same client as ``receives``, so the vendor is the one the
        call reaches, a fallback's or a subagent's included.
        """
        return get_pdf_page_tokens(image_route(getattr(model, "metadata", None)))

    async def awrap_model_call(self, request, handler):
        # Runs for every target, not just modality-limited ones: the interleaving
        # only arises where injection succeeded, which is a model that can see it.
        messages = order_tool_results_first(request.messages)

        stamped, modalities = self._resolve_target(request)
        has_image = "image" in modalities
        has_pdf = "pdf" in modalities
        # Looked up even for a fully multimodal target: a model that accepts PDFs
        # can still be handed one past its page ceiling, which is the judgement
        # the tool side deliberately no longer attempts.
        max_pdf_pages = get_max_pdf_pages(stamped) if stamped and has_pdf else None

        unreadable: frozenset[tuple[int, int]] = frozenset()
        route = image_route(getattr(request.model, "metadata", None))
        rule, update = _image_side_rule(request, route)
        if has_image:
            side = get_max_image_side_by_count if rule == "count" else get_max_image_side
            messages, unreadable = await fit_images(
                messages, get_image_types(route), lambda count: side(route, count)
            )
        # Decided on the call where the target is known: a tool-time note judged
        # the configured model and froze that verdict into the transcript, which
        # a subagent on its own model then inherited wrongly. The system prompt
        # is left alone so the cached prefix does not move with the strip, and
        # every placeholder, an image the fit could not use among them, is
        # written in this one pass so each guidance stays on the first it
        # applies to.
        messages = strip_unsupported_content_blocks(
            messages,
            has_image,
            has_pdf,
            max_pdf_pages,
            placeholder_guidance,
            unreadable=unreadable,
        )
        if messages is not request.messages:
            request = request.override(messages=messages)
        response = await handler(request)
        if update is None:
            return response
        return ExtendedModelResponse(model_response=response, command=Command(update={IMAGE_SIDE_KEY: update}))


__all__ = [
    "MultimodalStripMiddleware",
    "placeholder_guidance",
    "read_sources",
    "remove_attachments",
    "strip_unsupported_content_blocks",
]
