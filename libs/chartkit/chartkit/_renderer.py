"""The render page in headless Chromium: one worker thread, the routed API, framing, the screenshot.

The render page (``render/chart-render.html``, built from ``web/`` by ``pnpm build:chart-render``)
ships with this package, so the page and this code always match. It loads under an origin nothing
resolves, and every request it makes is answered here through Playwright routing: its files from
disk, its bar requests from ``ChartBars``, its chart-annotation route from the draft layer.

Playwright's sync API cannot run on a thread with a running asyncio loop, and a browser is worth
keeping for a second snapshot in the same run, so every render happens on one worker thread of
this module's own, which also closes the browser at exit (a Chromium left behind would hold the
job open).
"""

from __future__ import annotations

import atexit
import json
import queue
import re
import threading
import time
from concurrent.futures import Future
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit

from ._bars_source import ChartBars
from ._util import first_line
from ._view import RANGE_PRESETS, Frame, UserView, preset_window

RENDER_DIR = Path(__file__).with_name("render")
ENTRY = "chart-render.html"
#: Never resolved: every request is routed before it could leave the browser.
HOST = "chart-snapshot.invalid"
ORIGIN = f"https://{HOST}"
WORKSPACE_ID = "snapshot"
TIMEOUT_S = 45.0
#: The chart asks for more bars a beat after a view change (its scroll-load is debounced 300 ms).
_QUIET_S = 0.7

# Ready when the page mounted, the bars are drawn (no loading veil, a canvas), and, with a layer,
# the layer is painted: the renderer's report names it.
_READY_JS = """(layerId) => {
  const s = window.__LA_CHART_RENDER_STATUS__;
  if (s && s.error) return { error: s.error };
  if (!s || !s.mounted) return false;
  const err = document.querySelector('.chart-error');
  if (err) return { error: err.innerText.replace(/\\s+/g, ' ').trim() };
  if (document.querySelector('.chart-loading')) return false;
  if (!document.querySelector('.chart-wrapper canvas')) return false;
  if (!layerId) return { ok: true };
  const r = window.__LA_CHART_REPORT__;
  return r && r.frame >= 1 && (r.layers || []).some((l) => String(l.id) === layerId) ? { ok: true } : false;
}"""

# Settled once nothing repaints for three frames (the report's paint counter, or just three frames
# when no layer is on the chart, which publishes no report), up to a second.
_SETTLE_JS = """() => new Promise((resolve) => {
  const frame = () => (window.__LA_CHART_REPORT__ ? window.__LA_CHART_REPORT__.frame : 0);
  let last = frame(), still = 0;
  const t0 = performance.now();
  const tick = () => {
    const f = frame();
    still = f === last ? still + 1 : 0;
    last = f;
    if (still >= 3 || performance.now() - t0 > 1000) resolve(window.__LA_CHART_REPORT__ || null);
    else requestAnimationFrame(tick);
  };
  requestAnimationFrame(tick);
})"""

_FRAME_JS = "(f) => window.__LA_CHART_RENDER_FRAME__(f)"
_VIEW_JS = "() => window.__LA_CHART_RENDER_VIEW__()"


class SnapshotUnavailable(Exception):
    """This computer cannot take a snapshot, or this one failed: reported, never raised to the agent."""


@dataclass(frozen=True)
class SnapshotJob:
    """One snapshot to render: the chart, its draft layer, the look and the frame."""

    symbol: str
    timeframe: str
    tz: str
    #: The draft layer as ``Chart.to_dict()`` builds it, None for the plain chart.
    layer: dict | None
    path: str
    theme: str
    width: int
    height: int
    bars: ChartBars
    frame: Frame
    user: UserView | None = None
    #: The reader's language; the page draws English for one it has no words in.
    locale: str = "en-US"
    #: The DataFrame's currency, when the tool names none.
    currency: str | None = None
    #: What the report compares the chart's bars with: the agent's DataFrame and its local bar times.
    df: Any = field(default=None, compare=False, repr=False)
    idx: Any = field(default=None, compare=False, repr=False)
    #: The report's lines before the layer's: where the look came from, why a layer was not drawn.
    look_line: str = ""
    notes: tuple[str, ...] = ()
    #: The layer's element counts, as ``Chart._counts`` words them.
    counts: str = ""


@dataclass(frozen=True)
class Rendered:
    """What the page showed once settled."""

    #: ``window.__LA_CHART_REPORT__``: published only while a layer is on the chart.
    report: dict | None
    #: The visible window, ``{from, to}``.
    view: dict | None
    #: ``ok`` (framed as asked), ``default`` (nothing asked) or ``pending`` (the times are not on the chart).
    framed: str
    size: tuple[int, int]
    unanswered: list[str]
    page_errors: list[str]


def _playwright():
    try:
        from playwright import sync_api
    except ImportError:
        raise SnapshotUnavailable("it needs Playwright with Chromium, which this computer does not have.") from None
    return sync_api


# --------------------------------------------------------------------------- the routed API


def api(job: SnapshotJob, path: str, query: dict, unanswered: list[str]) -> tuple[int, dict]:
    """The answer the backend would give: the chart's bars and the draft layer."""
    parts = [unquote(p) for p in path.strip("/").split("/")]
    q = {k: v[0] for k, v in query.items()}
    if parts[:3] == ["api", "v1", "market-data"] and len(parts) >= 4:
        kind = parts[3]
        if kind in ("daily", "intraday"):
            daily = kind == "daily"
            if daily != (job.timeframe == "1day") or (not daily and q.get("interval") != job.timeframe):
                unanswered.append(f"bars at another timeframe ({path}?interval={q.get('interval')})")
                return 404, {"detail": "Not Found"}
            bars = job.bars
            data = bars.answer(q.get("from"), q.get("to"))
            body = {
                "symbol": job.symbol.upper(), "count": len(data), "data": data,
                "cache": {"cached": True, "complete": True, "market_phase": bars.phase(), "truncated": False,
                          "revision": 0, "watermark": data[-1]["time"] if data else None},
            }
            if not daily:
                body["interval"] = job.timeframe
            currency = bars.currency or job.currency
            if currency:
                body["currency"] = currency
            return 200, body
        if kind == "bars":
            # The live chart's delta poll: absent here, so it falls back to the route above.
            return 404, {"detail": "Not Found"}
        if kind == "snapshots":
            # No live quote: a snapshot is one frame of the bars.
            return 200, {"snapshots": [], "count": 0}
    if parts[:3] == ["api", "v1", "workspaces"] and len(parts) == 5 and parts[4] == "chart-annotations":
        sym = q.get("symbol", job.symbol)
        chart_id = f"{sym}:{job.timeframe}"
        layers = []
        if job.layer is not None:
            layer = dict(job.layer, symbol=sym)
            layers.append({"workspace_id": parts[3], "chart_id": chart_id, "symbol": sym,
                           "timeframe": job.timeframe, "layer_id": layer["id"], "layer": layer})
        charts = [{"chart_id": chart_id, "symbol": sym, "timeframe": job.timeframe,
                   "layers": layers}] if layers else []
        return 200, {"workspace_id": parts[3], "charts": charts}
    unanswered.append(path)
    return 404, {"detail": "Not Found"}


# --------------------------------------------------------------------------- one render


def _render(browser, job: SnapshotJob) -> Rendered:
    unanswered: list[str] = []
    page_errors: list[str] = []
    last_api = [time.monotonic()]
    root = RENDER_DIR.resolve()

    def handle(route, request):
        url = urlsplit(request.url)
        if url.hostname != HOST:
            unanswered.append(f"{request.method} {url.scheme}://{url.netloc}{url.path}")
            route.abort()
            return
        if url.path.startswith("/api/"):
            status, body = api(job, url.path, parse_qs(url.query), unanswered)
            last_api[0] = time.monotonic()
            route.fulfill(status=status, content_type="application/json", body=json.dumps(body))
            return
        file = (root / url.path.lstrip("/")).resolve()
        if file.is_file() and file.is_relative_to(root):
            route.fulfill(path=str(file))
        else:
            unanswered.append(f"{request.method} {url.path}")
            route.fulfill(status=404, body="")

    def quiet(limit: float):
        # Until the chart has asked for nothing for a beat: its background
        # backfill, or the older bars a new window scrolls into.
        stop = time.monotonic() + limit
        while time.monotonic() < stop:
            page.wait_for_timeout(100)
            if time.monotonic() - last_api[0] >= _QUIET_S and not page.locator(".chart-loading").count():
                return

    payload = {"symbol": job.symbol, "timeframe": job.timeframe, "theme": job.theme,
               "locale": job.locale, "workspaceId": WORKSPACE_ID, "width": job.width, "height": job.height}
    context = browser.new_context(
        viewport={"width": job.width, "height": job.height + 200},
        device_scale_factor=1,
        color_scheme=job.theme,
        service_workers="block",
    )
    try:
        page = context.new_page()
        page.on("pageerror", lambda e: page_errors.append(first_line(e)))
        page.route("**/*", handle)
        page.add_init_script(f"window.__LA_CHART_RENDER__ = {json.dumps(payload)};")
        deadline = time.monotonic() + TIMEOUT_S
        page.goto(f"{ORIGIN}/{ENTRY}", wait_until="load", timeout=TIMEOUT_S * 1000)
        layer_id = job.layer["id"] if job.layer else None
        try:
            state = page.wait_for_function(
                _READY_JS, arg=layer_id, polling="raf",
                timeout=max(deadline - time.monotonic(), 1.0) * 1000,
            ).json_value()
        except _playwright().TimeoutError:
            why = f"; page error: {page_errors[0]}" if page_errors else ""
            why += f"; unanswered: {', '.join(unanswered[:3])}" if unanswered else ""
            raise SnapshotUnavailable(f"the chart did not finish drawing within {TIMEOUT_S:.0f}s{why}") from None
        bars = job.bars
        if bars.failure and not bars.served:
            gap = f", and {bars.df_gap}" if bars.df_gap else ""
            raise SnapshotUnavailable(f"no bars to draw: the market-data tool failed: {bars.failure}{gap}")
        if state.get("error"):
            raise SnapshotUnavailable(f"the chart could not draw: {state['error']}")
        quiet(4.0)
        framed = _apply_frame(page, job)
        if framed == "ok":
            quiet(6.0)
        report = page.evaluate(_SETTLE_JS)
        view = page.evaluate(_VIEW_JS)
        region = page.locator(".charts-container").first
        box = region.bounding_box() or {}
        region.screenshot(path=job.path, animations="disabled", caret="hide")
        return Rendered(
            report=report if isinstance(report, dict) else None,
            view=view if isinstance(view, dict) else None,
            framed=framed,
            size=(round(box.get("width", 0)), round(box.get("height", 0))),
            unanswered=unanswered,
            page_errors=page_errors,
        )
    finally:
        context.close()


def _apply_frame(page, job: SnapshotJob) -> str:
    """Frame the drawn chart as asked: ``ok``, ``default`` (nothing asked) or ``pending``."""
    frame = job.frame
    if frame.kind == "default":
        return "default"
    if frame.kind == "layer":
        target = {"kind": "layer", "layerId": frame.layer_id}
    elif frame.kind == "preset":
        key = frame.preset
        if RANGE_PRESETS[key] == job.timeframe:
            # The user's one click, through the chart's own preset logic.
            button = page.locator(".chart-range-selector .range-btn", has_text=re.compile(rf"^{re.escape(key)}$"))
            if button.count():
                button.first.click()
                return "ok"
        window = preset_window(key, job.bars.times().index, job.timeframe)
        if window is None:
            return "pending"
        target = {"kind": "range", "from": window[0], "to": window[1]}
    else:
        target = {"kind": "range", "from": frame.start, "to": frame.end}
    stop = time.monotonic() + 3.0
    while True:
        status = page.evaluate(_FRAME_JS, target)
        if status != "pending" or time.monotonic() > stop:
            return status
        page.wait_for_timeout(50)


# --------------------------------------------------------------------------- the worker


class Renderer:
    """One worker thread owning Playwright and its browser for the life of the process."""

    def __init__(self):
        self._jobs: queue.Queue = queue.Queue()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()

    def run(self, job: SnapshotJob) -> Rendered:
        if not (RENDER_DIR / ENTRY).is_file():
            raise SnapshotUnavailable("this computer has no chart renderer installed. Publish without a snapshot.")
        playwright_error = _playwright().Error
        with self._lock:
            if self._thread is None or not self._thread.is_alive():
                self._thread = threading.Thread(target=self._loop, name="chartkit-snapshot", daemon=True)
                self._thread.start()
        fut: Future = Future()
        self._jobs.put((job, fut))
        try:
            return fut.result(timeout=TIMEOUT_S + 60)
        except TimeoutError:
            raise SnapshotUnavailable("the renderer did not answer in time") from None
        except playwright_error as e:
            raise SnapshotUnavailable(f"the renderer failed ({type(e).__name__}: {first_line(e)})") from None

    def _loop(self):
        pw = browser = None
        try:
            while True:
                item = self._jobs.get()
                if item is None:
                    return
                job, fut = item
                try:
                    if browser is None or not browser.is_connected():
                        pw, browser = _launch(pw)
                    fut.set_result(_render(browser, job))
                except BaseException as e:  # noqa: BLE001 - handed to the caller
                    fut.set_exception(e)
        finally:
            for closer in (getattr(browser, "close", None), getattr(pw, "stop", None)):
                try:
                    if closer:
                        closer()
                except Exception:  # noqa: BLE001 - best effort at exit
                    pass

    def close(self):
        if self._thread is not None and self._thread.is_alive():
            self._jobs.put(None)
            self._thread.join(timeout=10)


def _launch(pw):
    """Start Chromium, and the Playwright driver first when there is none. A
    driver started here is stopped again if Chromium fails, since the caller
    never receives it: each later snapshot would otherwise start one more."""
    sync_api = _playwright()
    started = pw is None
    if started:
        pw = sync_api.sync_playwright().start()
    try:
        browser = pw.chromium.launch(args=["--disable-dev-shm-usage", "--hide-scrollbars"])
    except BaseException as e:
        if started:
            try:
                pw.stop()
            except Exception:  # noqa: BLE001 - the launch error is the one to report
                pass
        if isinstance(e, sync_api.Error):
            raise SnapshotUnavailable(f"Chromium did not start ({first_line(e)})") from None
        raise
    return pw, browser


RENDERER = Renderer()
atexit.register(RENDERER.close)
