"""Running a ShowWidget widget in headless Chromium, so the agent learns what it did.

The widget runs in the render page (``web/widget-render.html``, built by
``pnpm build:widget-render``), which frames it exactly as the chat does: the
same srcDoc builder, sandbox tokens and permissions, in the light theme at the
widest chat column. The page hands over what the widget's frame reports
(errors, CSP refusals, failed loads) as it arrives, waits for it to settle,
asks it what its document holds, asks again at a phone's width (where the
same widget is also shown) and puts it back, and this module screenshots the
frame.

The browser and the request gate are ``headless_browser``'s: one warm
Chromium per worker process, a context per render. The page itself is served
from disk under an origin nothing resolves. Relative URLs in the widget
resolve to its workspace folder through a file grant minted for the render
alone, for the user the turn runs as, and lasting minutes.
"""

from __future__ import annotations

import asyncio
import io
import logging
import math
import os
import re
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Annotated, Any, NamedTuple
from urllib.parse import unquote, urlsplit

import httpx
from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)

from src.server.services.headless_browser import (
    BrowserError,
    BrowserUnavailable,
    Budget,
    RequestGate,
    gated_context,
    settle_later,
    use_browser,
    within,
)

logger = logging.getLogger(__name__)

__all__ = [
    "Inspection",
    "WidgetRenderError",
    "WidgetRenderReport",
    "WidgetRenderUnavailable",
    "render_widget",
]

# The browser's own failures, under the names the tool catches; the page's
# own failures to build or load are these too.
WidgetRenderError = BrowserError
WidgetRenderUnavailable = BrowserUnavailable


# The page's build: beside this module, from `pnpm build:widget-render` or the
# image's node stage. The dev stack mounts src/ over that copy, so its image
# keeps a second one at WIDGET_RENDER_DIR, used when the checkout has no build
# of its own (a host build wins, as chartkit's render page does).
_DEFAULT_RENDER_DIR = Path(__file__).with_name("widget_render_page")
_RENDER_DIR_ENV = "WIDGET_RENDER_DIR"
_ENTRY = "widget-render.html"
#: Never resolved: every request to it is answered from disk before it could leave.
_HOST = "widget-render.invalid"
_ORIGIN = f"https://{_HOST}"
#: The widget's workspace files, as the widget sees them. Never resolved
#: either: the render fetches each from the loopback serve route itself. A
#: sandboxed widget has an opaque origin, which Chromium's Local Network Access
#: counts as public, so the frame itself may not reach the server's loopback.
_FILES_ORIGIN = "https://workspace.widget-render.invalid"
#: The most one workspace file may bring into a render; all of them together
#: count against the render's ``Budget`` with what it fetched from the internet.
_MAX_FILE_BYTES = 16 * 1024 * 1024

#: The widest column a widget is shown in (the desktop chat); the screenshot's width.
_VIEWPORT_WIDTH = 768
#: The narrowest (a phone's chat, the market view panel): measured, not shot.
_NARROW_WIDTH = 360
#: What the screenshot covers at most; a taller widget is shown from the top.
_SCREENSHOT_MAX_HEIGHT = 1400
_SCREENSHOT_MIN_HEIGHT = 40
#: The tallest the frame grows, whatever the widget reports (one sized in vh
#: would otherwise grow with every report).
_FRAME_MAX_HEIGHT = 8000
#: A PNG past this is tried as a JPEG, and the smaller kept: the image is
#: base64 in a checkpointed tool result, sent again on every later call.
_PNG_SOFT_CAP = 300_000
_JPEG_QUALITY = 80
#: What the screenshot never passes: lower JPEG qualities first, then smaller.
_SCREENSHOT_HARD_CAP = 400_000
_JPEG_FALLBACK_QUALITIES = (60, 40)
_DOWNSCALES = (0.75, 0.5, 0.35)

#: As widgetPdf.ts waits: quiet after the last report, with a cap.
_SETTLE_MS = 700
_MAX_WAIT_MS = 8000
#: The least the page's wait is given, however little time is left.
_MIN_WAIT_S = 0.3
#: Kept back from the wait for the inspect answer and the screenshot.
_SCREENSHOT_RESERVE_S = 2.5
_SCREENSHOT_TIMEOUT_S = 3.0
#: What a page that never answered gets for its screenshot, and what the run
#: keeps back for it: a hung page seldom paints, so it is not given long.
_HUNG_SCREENSHOT_S = 1.0
#: Past the page's own cap and its inspect wait, the page is hung.
_HUNG_MARGIN_S = 2.0
#: Long enough for one render; the grant can be read out of the page.
_GRANT_TTL_S = 300

_MAX_ITEMS = 20
_FETCH_TYPES = frozenset({"fetch", "xhr", "eventsource"})
_MAX_ITEM_CHARS = 300

# One render at a time per slot: each holds a context and a page. Apart from
# the PDF renderer's slots, so a long export never holds up an agent's turn.
_RENDER_SEMAPHORE = asyncio.Semaphore(2)
#: For the browser's launch and a free slot together, before the render's own
#: clock starts: a render given its slot late would otherwise have no time to
#: run and report a working widget as broken.
_SETUP_TIMEOUT_S = 10.0


# --- The page's answer -------------------------------------------------------


def _count(value: Any) -> int:
    """A count the page measured, or 0 for anything that is not one."""
    if isinstance(value, (int, float)) and math.isfinite(value) and value >= 0:
        return int(value)
    return 0


Count = Annotated[int, BeforeValidator(_count)]


class _Images(BaseModel):
    model_config = ConfigDict(frozen=True)

    total: Count = 0
    broken: Count = 0


def _serve_base_of(info: ValidationInfo) -> str | None:
    return (info.context or {}).get("serve_base")


class Inspection(BaseModel):
    """The frame's answer to ``widget:inspect`` (``WidgetInspection`` in
    ``web/src/widgetRender/protocol.ts``). The widget's frame authors it, so
    a field that is not of its shape reads as nothing rather than failing the
    answer. Validated with ``context={"serve_base": ...}``, the visible text
    loses the grant as every other line the model reads does."""

    model_config = ConfigDict(frozen=True, populate_by_name=True, extra="ignore")

    height: Count = 0
    width: Count = 0
    scroll_width: Count = Field(0, alias="scrollWidth")
    text_chars: Count = Field(0, alias="textChars")
    elements: Count = 0
    images: _Images = _Images()
    canvases: Count = 0
    svgs: Count = 0
    fixed_elements: Count = Field(0, alias="fixedElements")
    data_keys: list[str] = Field(default_factory=list, alias="dataKeys")
    visible_text: str = Field("", alias="visibleText")

    @field_validator("images", mode="before")
    @classmethod
    def _images(cls, value: Any) -> Any:
        return value if isinstance(value, dict) else {}

    @field_validator("data_keys", mode="before")
    @classmethod
    def _data_keys(cls, value: Any) -> list[str]:
        if not isinstance(value, list):
            return []
        return [key for key in value if isinstance(key, str)][:50]

    @field_validator("visible_text", mode="before")
    @classmethod
    def _visible_text(cls, value: Any, info: ValidationInfo) -> str:
        if not isinstance(value, str):
            return ""
        return redact(value, _serve_base_of(info))[:300]


def _inspection_or_none(value: Any) -> Any:
    return value if isinstance(value, dict) else None


def _positive_height(value: Any) -> int | None:
    if isinstance(value, (int, float)) and math.isfinite(value) and value > 0:
        return int(value)
    return None


class RenderResult(BaseModel):
    """What the page's run resolves (``WidgetRenderResult``), read as
    ``Inspection`` is. Its diagnostics are not read here: each one reached
    the render live as it arrived (``_Observer``)."""

    model_config = ConfigDict(frozen=True, populate_by_name=True, extra="ignore")

    settled: bool = False
    #: The last height the widget reported, or None if it never did.
    height: Annotated[int | None, BeforeValidator(_positive_height)] = None
    frame_height: Annotated[int | None, BeforeValidator(_positive_height)] = Field(
        None, alias="frameHeight"
    )
    inspection: Annotated[Inspection | None, BeforeValidator(_inspection_or_none)] = None
    #: Only beside a main answer: the narrow pass measures the same document.
    narrow: Annotated[Inspection | None, BeforeValidator(_inspection_or_none)] = None

    @field_validator("settled", mode="before")
    @classmethod
    def _settled(cls, value: Any) -> bool:
        return bool(value)

    @model_validator(mode="before")
    @classmethod
    def _narrow_needs_a_main_answer(cls, data: Any) -> Any:
        if isinstance(data, dict) and not isinstance(data.get("inspection"), dict):
            return {**data, "narrow": None}
        return data

    @classmethod
    def read(cls, raw: Any, serve_base: str | None) -> RenderResult | None:
        """The page's answer, or None when it resolved to anything but an object."""
        if not isinstance(raw, dict):
            return None
        return cls.model_validate(raw, context={"serve_base": serve_base})


# --- The report ----------------------------------------------------------------


class Overflow(NamedTuple):
    frame: int
    content: int


def _overflow(inspection: Inspection | None) -> Overflow | None:
    if inspection is None:
        return None
    frame, content = inspection.width, inspection.scroll_width
    return Overflow(frame, content) if frame and content and content > frame + 2 else None


_NOTHING = Inspection()


@dataclass(frozen=True)
class WidgetRenderReport:
    """What the widget did. Lines are workspace-relative where they point into
    the workspace and never carry a grant."""

    #: The frame's answer at the main width; None when it never answered.
    inspection: Inspection | None
    #: The same document at ``_NARROW_WIDTH``; None when that pass was not run
    #: (no time left, no main answer) or the frame did not answer it.
    narrow: Inspection | None
    errors: list[str] = field(default_factory=list)
    blocked: list[str] = field(default_factory=list)
    load_failed: list[str] = field(default_factory=list)
    #: What the render did not load for its own size caps; it loads for the user.
    too_large: list[str] = field(default_factory=list)
    #: The page went quiet before its cap.
    settled: bool = False
    #: The last height the widget reported, shown when the frame never answered.
    reported_height: int | None = None
    screenshot: bytes | None = None
    #: ``image/png``, or ``image/jpeg`` when that was much smaller.
    media_type: str = "image/png"
    elapsed_ms: int = 0

    @property
    def inspected(self) -> bool:
        return self.inspection is not None

    @property
    def seen(self) -> Inspection:
        """The answer, or one of zeros when the frame gave none."""
        return self.inspection or _NOTHING

    @property
    def width(self) -> int | None:
        return self.inspection.width if self.inspection else None

    @property
    def height(self) -> int | None:
        return self.inspection.height if self.inspection else self.reported_height

    @property
    def blank(self) -> bool:
        seen = self.seen
        return not (seen.text_chars or seen.images.total or seen.canvases or seen.svgs)

    @property
    def overflow(self) -> Overflow | None:
        """The content wider than the main frame, so it scrolls sideways."""
        return _overflow(self.inspection)

    @property
    def narrow_overflow(self) -> Overflow | None:
        """The content wider than a phone's column."""
        return _overflow(self.narrow)

    @property
    def hung(self) -> bool:
        """The frame never answered: a script stuck in a loop or blocking the page."""
        return not self.inspected

    @property
    def unsettled(self) -> bool:
        """It answered but was still changing when the wait hit its cap."""
        return self.inspected and not self.settled


# --- Pure parts -------------------------------------------------------------


def render_dir() -> Path | None:
    """The render page's build, or None when none is installed."""
    configured = os.environ.get(_RENDER_DIR_ENV)
    for candidate in (_DEFAULT_RENDER_DIR, Path(configured) if configured else None):
        if candidate is not None and (candidate / _ENTRY).is_file():
            return candidate
    return None


def bundle_file(root: Path, url_path: str) -> Path | None:
    """The file of the build a request path names, never one outside it."""
    relative = unquote(url_path).lstrip("/")
    if not relative:
        return None
    resolved = (root / relative).resolve()
    if resolved.is_file() and resolved.is_relative_to(root.resolve()):
        return resolved
    return None


_GRANT_PATH = re.compile(r"(?:https?://[^/\s\"']+)?/api/v1/wsfiles/g/[^/\s\"']+/")


def redact(text: str, serve_base: str | None) -> str:
    """``text`` with the grant taken out of every URL into the workspace, and
    the render page's own origin, so a path reads as the widget wrote it
    (``memes/01.jpg``) and no grant reaches the model."""
    if serve_base:
        text = text.replace(serve_base, "")
    text = _GRANT_PATH.sub("", text)
    return text.replace(f"{_ORIGIN}/", "")


def _clip(text: str) -> str:
    return text if len(text) <= _MAX_ITEM_CHARS else text[: _MAX_ITEM_CHARS - 1] + "…"


def describe_diag(
    diag: dict[str, Any], serve_base: str | None
) -> tuple[str, str] | None:
    """A diagnostic as ``(list, line)`` for the report, or None for one that
    is not of the contract's shape (the widget's frame authors these)."""
    if not isinstance(diag, dict):
        return None
    kind = diag.get("kind")
    if kind == "error":
        message = diag.get("message")
        if not isinstance(message, str) or not message:
            return None
        source, line = diag.get("source"), diag.get("line")
        where = ""
        if source == "widget html" and isinstance(line, int):
            where = f" (line {line} of the widget html)"
        elif isinstance(source, str) and source and source != "about:srcdoc":
            where = f" ({source}{f':{line}' if isinstance(line, int) else ''})"
        return "errors", _clip(redact(message + where, serve_base))
    if kind == "blocked":
        directive, url = diag.get("directive"), diag.get("url")
        if not isinstance(directive, str) or not isinstance(url, str):
            return None
        return "blocked", _clip(redact(f"{directive}: {url or '(inline)'}", serve_base))
    if kind == "load-failed":
        tag, url = diag.get("tag"), diag.get("url")
        if not isinstance(tag, str) or not isinstance(url, str):
            return None
        status, failure = diag.get("status"), diag.get("failure")
        outcome = (
            f" (HTTP {status})"
            if isinstance(status, int)
            else f" ({failure})"
            if isinstance(failure, str) and failure
            else ""
        )
        return "load_failed", _clip(redact(f"<{tag}> {url}{outcome}", serve_base))
    return None


class Diagnostics:
    """Every diagnostic seen, as report lines: deduplicated in arrival order
    and capped per list.

    A load the render cut off for size (``too_large``, the render's
    ``Budget.too_large``) is not a failure: the reader's browser has no such
    cap, so it is reported on its own and its failed load is dropped."""

    def __init__(self, serve_base: str | None, too_large: list[str] | None = None) -> None:
        self._serve_base = serve_base
        self._seen: set[tuple[str, str]] = set()
        self._too_large = too_large if too_large is not None else []
        self.lists: dict[str, list[str]] = {"errors": [], "blocked": [], "load_failed": []}

    def too_large(self) -> list[str]:
        lines = dict.fromkeys(_clip(redact(url, self._serve_base)) for url in self._too_large)
        return list(lines)[:_MAX_ITEMS]

    def add(self, diag: Any) -> None:
        if (
            isinstance(diag, dict)
            and diag.get("kind") == "load-failed"
            and isinstance(diag.get("url"), str)
            and diag["url"].split("#", 1)[0] in self._too_large
        ):
            return
        described = describe_diag(diag, self._serve_base)
        if described is None or described in self._seen:
            return
        name, line = described
        if len(self.lists[name]) < _MAX_ITEMS:
            self._seen.add(described)
            self.lists[name].append(line)


def build_report(
    result: RenderResult | None,
    diagnostics: Diagnostics,
    *,
    screenshot: bytes | None = None,
    media_type: str = "image/png",
    elapsed_ms: int = 0,
) -> WidgetRenderReport:
    """The report from the page's answer (None when it never resolved) and
    every diagnostic seen."""
    return WidgetRenderReport(
        inspection=result.inspection if result else None,
        narrow=result.narrow if result else None,
        errors=diagnostics.lists["errors"],
        blocked=diagnostics.lists["blocked"],
        load_failed=diagnostics.lists["load_failed"],
        too_large=diagnostics.too_large(),
        settled=bool(result and result.settled),
        reported_height=result.height if result else None,
        screenshot=screenshot,
        media_type=media_type,
        elapsed_ms=elapsed_ms,
    )


def screenshot_height(result: RenderResult | None) -> int:
    """How much of the frame the screenshot covers, from the top."""
    height = (result.frame_height if result else None) or 150
    return int(min(_SCREENSHOT_MAX_HEIGHT, max(_SCREENSHOT_MIN_HEIGHT, height)))


# --- The grant ----------------------------------------------------------------


async def _serve_base(workspace_id: str | None, user_id: str | None) -> str | None:
    """The absolute URL the widget's relative URLs resolve under: the grant
    route on ``_FILES_ORIGIN``, for the grant ``user_id`` would get in their
    own viewer, lasting only the render. None with no workspace, one that is
    gone, or a shared one with no user to bind the grant to."""
    if not workspace_id:
        return None
    from src.server.database.workspace import get_workspace_placement
    from src.server.services.file_grants import grant_prefix, viewer_file_grant

    placement = await get_workspace_placement(workspace_id)
    if placement is None:
        return None
    grant = await viewer_file_grant(
        str(placement["workspace_id"]), placement.get("user_id"), user_id, ttl=_GRANT_TTL_S
    )
    return f"{_FILES_ORIGIN}{grant_prefix(grant)}" if grant else None


def internal_url(url: str, serve_base: str) -> str | None:
    """The loopback serve route's address for a workspace file the widget
    asked for, or None for anything outside its grant's prefix."""
    from src.config.env import PDF_RENDER_INTERNAL_BASE

    if not url.startswith(serve_base) or not serve_base.startswith(_FILES_ORIGIN):
        return None
    return PDF_RENDER_INTERNAL_BASE.rstrip("/") + url[len(_FILES_ORIGIN):]


# --- What the render answers itself ---------------------------------------------

# What a file answer keeps of the serve route's headers; the body arrives
# decoded and whole, so its encoding and length are the fulfil's own.
_FILE_HEADERS = frozenset({
    "content-type", "access-control-allow-origin", "content-range",
    "accept-ranges", "cache-control", "content-disposition",
})


class _OwnRequests:
    """The requests no gate sees: the render page's own files, from its build
    on disk, and the widget's workspace files, from the loopback serve route.
    Both are answered whole, so the workspace's count against ``budget``."""

    def __init__(
        self, root: Path, serve_base: str | None, client: httpx.AsyncClient, budget: Budget
    ) -> None:
        self._root = root
        self._serve_base = serve_base
        self._client = client
        self._budget = budget

    async def answer(self, route, request) -> bool:
        if self._serve_base and request.url.startswith(self._serve_base):
            await self._answer_file(route, request, self._serve_base)
            return True
        parts = urlsplit(request.url)
        if parts.hostname != _HOST:
            return False
        file = bundle_file(self._root, parts.path)
        if file is not None:
            await route.fulfill(path=str(file))
        else:
            await route.fulfill(status=404, body="")
        return True

    async def _answer_file(self, route, request, serve_base: str) -> None:
        target = internal_url(request.url.split("#", 1)[0], serve_base)
        if target is None:
            await route.abort()
            return
        headers = {k: v for k, v in request.headers.items() if k.lower() == "range"}
        try:
            async with self._client.stream("GET", target, headers=headers) as response:
                body = bytearray()
                async for chunk in response.aiter_bytes():
                    body += chunk
                    if not self._budget.spend(len(chunk)) or len(body) > _MAX_FILE_BYTES:
                        self._budget.cut_off(request.url)
                        await route.abort()
                        return
                kept = {
                    k: v for k, v in response.headers.items() if k.lower() in _FILE_HEADERS
                }
                await route.fulfill(
                    status=response.status_code, headers=kept, body=bytes(body)
                )
        except Exception:
            logger.debug("widget render workspace file not answered", exc_info=True)
            await route.abort()


# --- What the render watches ---------------------------------------------------


class _Observer:
    """What the render sees besides the page's answer: each diagnostic the
    page hands over as it arrives, the fetches that failed, and how many
    requests are still in flight, which the page asks before it calls the
    widget settled."""

    def __init__(self, serve_base: str | None, too_large: list[str] | None = None) -> None:
        self.diagnostics = Diagnostics(serve_base, too_large)
        self.in_flight = 0
        self._main_frame: Any = None

    async def attach(self, page) -> None:
        self._main_frame = page.main_frame
        page.on("request", self._started)
        page.on("requestfinished", self._ended)
        page.on("requestfailed", self._ended)
        page.on("response", self._responded)
        page.on("requestfailed", self._failed)
        # Bindings, not functions: Playwright installs both in every frame,
        # the widget's own included, and only the render page may call them,
        # or a widget could write its own report.
        await page.expose_binding("__laWidgetDiag", self._on_diag)
        await page.expose_binding("__laWidgetNetBusy", self._busy)

    def _from_page(self, source: dict[str, Any]) -> bool:
        return source.get("frame") is self._main_frame

    def _on_diag(self, source: dict[str, Any], diag: Any = None) -> None:
        if self._from_page(source):
            self.diagnostics.add(diag)

    def _busy(self, source: dict[str, Any]) -> int:
        return self.in_flight if self._from_page(source) else 0

    def _started(self, _request) -> None:
        self.in_flight += 1

    def _ended(self, _request) -> None:
        self.in_flight = max(0, self.in_flight - 1)

    # A failed fetch reaches the widget's code, not an element, so the frame
    # reports it only if the widget does; the network sees it.
    def _fetch_failed(self, request, **outcome: Any) -> None:
        if request.resource_type in _FETCH_TYPES and request.frame is not self._main_frame:
            self.diagnostics.add(
                {"kind": "load-failed", "tag": "fetch", "url": request.url, **outcome}
            )

    def _responded(self, response) -> None:
        if response.status >= 400:
            self._fetch_failed(response.request, status=response.status)

    def _failed(self, request) -> None:
        self._fetch_failed(request, failure=request.failure or "failed")


def _close_other_windows(context, page) -> None:
    """Close any window the widget opens: nothing reads it, and its requests
    are gated as the page's are."""
    closing: set[asyncio.Task] = set()

    def close(other) -> None:
        if other is page:
            return
        task = asyncio.ensure_future(other.close())
        closing.add(task)
        task.add_done_callback(closing.discard)
        settle_later(task)

    context.on("page", close)


# --- The render ---------------------------------------------------------------

_RUN_JS = "(payload) => window.__LA_WIDGET_RENDER_RUN__(payload)"
_CONTEXT_OPTIONS: dict[str, Any] = {
    "viewport": {"width": _VIEWPORT_WIDTH, "height": 900},
    "device_scale_factor": 1,
    "color_scheme": "light",
    "service_workers": "block",
    "accept_downloads": False,
}


class _Deadline:
    def __init__(self, seconds: float) -> None:
        self.started = time.monotonic()
        self._end = self.started + seconds

    def left(self, reserve: float = 0.0, floor: float = 0.0) -> float:
        """Seconds left with ``reserve`` kept back for what comes after, never
        under ``floor``."""
        return max(self._end - time.monotonic() - reserve, floor)

    def elapsed_ms(self) -> int:
        return int((time.monotonic() - self.started) * 1000)


@asynccontextmanager
async def _render_slot(deadline: _Deadline) -> AsyncIterator[None]:
    try:
        async with asyncio.timeout(deadline.left(floor=0.01)):
            await _RENDER_SEMAPHORE.acquire()
    except TimeoutError:
        raise WidgetRenderError("no render slot came free in time") from None
    try:
        yield
    finally:
        _RENDER_SEMAPHORE.release()


async def render_widget(
    *,
    html: str,
    data: dict[str, str] | None,
    workspace_id: str | None,
    user_id: str | None,
    timeout_s: float = 12.0,
) -> WidgetRenderReport:
    """Run the widget and report what it did, with a screenshot of its frame.

    Past ``timeout_s`` it reports what it has rather than raising; that clock
    starts once the browser is up and a slot is free. Raises
    ``WidgetRenderUnavailable`` when no renderer is installed, and
    ``WidgetRenderError`` when the browser did not start or no slot came free
    within ``_SETUP_TIMEOUT_S``, the render page itself could not be loaded in
    time, or the browser failed: a widget never checked is never reported
    broken.
    """
    try:
        from playwright.async_api import Error as PlaywrightError
    except ImportError:
        raise WidgetRenderUnavailable("playwright is not installed") from None

    root = render_dir()
    if root is None:
        raise WidgetRenderUnavailable(
            "the widget render page is not built (run: cd web && pnpm build:widget-render)"
        )
    setup = _Deadline(_SETUP_TIMEOUT_S)
    async with (
        use_browser(launch_timeout_s=setup.left(floor=0.01)) as browser,
        _render_slot(setup),
    ):
        deadline = _Deadline(timeout_s)
        try:
            async with asyncio.timeout(deadline.left(floor=0.01)):
                serve_base = await _serve_base(workspace_id, user_id)
        except TimeoutError:
            raise WidgetRenderError("no file grant came in time") from None
        budget = Budget()
        observer = _Observer(serve_base, budget.too_large)
        # Loopback only, to the serve route under the grant (``internal_url``);
        # never the guarded client, which refuses loopback by design.
        async with httpx.AsyncClient(timeout=10.0, follow_redirects=False) as files:
            own = _OwnRequests(root, serve_base, files, budget)
            try:
                async with gated_context(
                    browser, RequestGate(None, budget), own=own.answer, **_CONTEXT_OPTIONS
                ) as context:
                    page = await context.new_page()
                    _close_other_windows(context, page)
                    await observer.attach(page)
                    await page.goto(
                        f"{_ORIGIN}/{_ENTRY}",
                        wait_until="load",
                        timeout=deadline.left(reserve=_SCREENSHOT_RESERVE_S, floor=0.5)
                        * 1000,
                    )
                    result = await _run(page, html, data, serve_base, deadline)
                    shot, media_type = await _screenshot(
                        page,
                        screenshot_height(result),
                        min(
                            _SCREENSHOT_TIMEOUT_S if result else _HUNG_SCREENSHOT_S,
                            deadline.left(floor=0.5),
                        ),
                    )
                    return build_report(
                        result,
                        observer.diagnostics,
                        screenshot=shot,
                        media_type=media_type,
                        elapsed_ms=deadline.elapsed_ms(),
                    )
            except PlaywrightError as exc:
                raise WidgetRenderError(f"the render page did not load: {exc}") from exc


async def _run(
    page,
    html: str,
    data: dict[str, str] | None,
    serve_base: str | None,
    deadline: _Deadline,
) -> RenderResult | None:
    """The page's answer for the widget, or None when it gave none in time."""
    from playwright.async_api import Error as PlaywrightError

    wait_s = min(_MAX_WAIT_MS / 1000, deadline.left(_SCREENSHOT_RESERVE_S, _MIN_WAIT_S))
    payload = {
        "html": html,
        "data": data or None,
        "base": serve_base,
        "width": _VIEWPORT_WIDTH,
        "narrowWidth": _NARROW_WIDTH,
        "settleMs": _SETTLE_MS,
        "maxWaitMs": int(wait_s * 1000),
        "maxHeight": _FRAME_MAX_HEIGHT,
    }
    try:
        # The page answers by its own cap, the inspect wait included; past
        # that and a margin, the widget has hung the page (a loop in a frame
        # that shares its process), which paints nothing either.
        raw = await within(
            page.evaluate(_RUN_JS, payload),
            min(deadline.left(reserve=_HUNG_SCREENSHOT_S), wait_s + _HUNG_MARGIN_S),
        )
    except (TimeoutError, PlaywrightError):
        logger.info("widget render did not finish")
        return None
    return RenderResult.read(raw, serve_base)


def _smaller(image: bytes) -> tuple[bytes, str] | None:
    """``image`` scaled down as a JPEG until it is under the hard cap, or
    None when even the smallest is not."""
    from PIL import Image

    with Image.open(io.BytesIO(image)) as opened:
        rgb = opened.convert("RGB")
    for scale in _DOWNSCALES:
        size = (max(1, round(rgb.width * scale)), max(1, round(rgb.height * scale)))
        out = io.BytesIO()
        rgb.resize(size, Image.Resampling.LANCZOS).save(
            out, "JPEG", quality=_JPEG_FALLBACK_QUALITIES[0]
        )
        if out.tell() <= _SCREENSHOT_HARD_CAP:
            return out.getvalue(), "image/jpeg"
    return None


async def _screenshot(page, height: int, timeout_s: float) -> tuple[bytes | None, str]:
    """The frame from the top, as a PNG, or the smaller JPEG past
    ``_PNG_SOFT_CAP``, never past ``_SCREENSHOT_HARD_CAP``; None when the page
    cannot paint in time (a widget that hangs it)."""
    clip = {"x": 0, "y": 0, "width": _VIEWPORT_WIDTH, "height": height}

    async def shoot() -> tuple[bytes | None, str]:
        await page.set_viewport_size({"width": _VIEWPORT_WIDTH, "height": height})
        png = await page.screenshot(type="png", clip=clip)
        if len(png) <= _PNG_SOFT_CAP:
            return png, "image/png"
        jpeg = await page.screenshot(type="jpeg", quality=_JPEG_QUALITY, clip=clip)
        best = min((png, "image/png"), (jpeg, "image/jpeg"), key=lambda shot: len(shot[0]))
        for quality in _JPEG_FALLBACK_QUALITIES:
            if len(best[0]) <= _SCREENSHOT_HARD_CAP:
                return best
            best = (await page.screenshot(type="jpeg", quality=quality, clip=clip), "image/jpeg")
        if len(best[0]) <= _SCREENSHOT_HARD_CAP:
            return best
        smaller = await asyncio.to_thread(_smaller, png)
        return smaller if smaller is not None else (None, "image/png")

    try:
        return await within(shoot(), timeout_s)
    except Exception:
        logger.info("widget render screenshot not taken", exc_info=True)
        return None, "image/png"
