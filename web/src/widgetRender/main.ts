/**
 * Entry of the widget render page (`widget-render.html`, built apart from the
 * app by `vite.widget-render.config.js`). The backend loads it in headless
 * Chromium after a ShowWidget call and runs one widget in it, framed as the
 * chat frames it: the same srcDoc builder, sandbox tokens and permissions, in
 * the light theme, at the widest chat column. It reports what the widget did
 * (errors, refused and failed loads, what the document holds) so the agent
 * learns about a broken widget before the user does, and measures it again at
 * a phone's width, since the same widget is shown that narrow too.
 */
import { buildWidgetSrcDoc } from '@/pages/ChatAgent/components/viewers/html/buildHtmlSrcDoc';
import { LIGHT_PALETTE } from '@/pages/ChatAgent/components/viewers/html/frameTheme';
import { createWidgetFrame } from '@/pages/ChatAgent/components/viewers/html/sandbox';
import {
  postToWidget,
  readWidgetMessage,
  type WidgetDiag,
  type WidgetInspection,
} from '@/pages/ChatAgent/components/viewers/html/widgetProtocol';

import type { WidgetRenderPayload, WidgetRenderResult } from './protocol';

/** How long the frame has to answer `widget:inspect`. */
const INSPECT_TIMEOUT_MS = 1000;
/** After a change of the frame's width, quiet this long after the last resize report counts as laid out... */
const RELAYOUT_QUIET_MS = 200;
/** ...or this long in all, whatever the widget keeps reporting. */
const RELAYOUT_MAX_MS = 600;
/**
 * The narrow pass runs only with this much of the page's cap left (two
 * relayouts and an answer), so it never pushes a render past the cap.
 */
const NARROW_MIN_LEFT_MS = 2 * RELAYOUT_MAX_MS + 300;
/** The chat's cold guess before the first report (InlineWidget). */
const INITIAL_HEIGHT_PX = 150;
/** Diagnostics handed to the driver; the frame caps itself, this guards against one that does not. */
const MAX_DIAGS = 60;

// The chat's light page behind the frame, whose own background is transparent.
const HOST_CSS = `
html, body { margin: 0; padding: 0; overflow: hidden; color-scheme: light; background: ${LIGHT_PALETTE.vars['--color-bg-page']}; }
#root { width: max-content; }
iframe { display: block; border: 0; background: transparent; }
`;

interface FrameEvents {
  /** The frame was given this height (already bounded). */
  resize: number;
  diag: WidgetDiag;
  load: undefined;
  inspected: WidgetInspection | null;
}

type Listener<K extends keyof FrameEvents> = (value: FrameEvents[K]) => void;

interface FrameChannel {
  readonly frame: HTMLIFrameElement;
  /** The frame's document finished loading. */
  readonly loaded: boolean;
  on<K extends keyof FrameEvents>(event: K, listener: Listener<K>): () => void;
  close(): void;
}

/**
 * The one listener on what the frame says, for as long as the render runs.
 * Whatever phase is under way, a resize report sizes the frame (as the chat
 * does) and a diagnostic goes to the driver as it arrives; each phase listens
 * for what it waits on.
 */
function frameChannel(frame: HTMLIFrameElement, maxHeight: number, htmlLine: number): FrameChannel {
  const listeners: { [K in keyof FrameEvents]: Set<Listener<K>> } = {
    resize: new Set(),
    diag: new Set(),
    load: new Set(),
    inspected: new Set(),
  };
  const emit = <K extends keyof FrameEvents>(event: K, value: FrameEvents[K]) => {
    for (const listener of listeners[event]) listener(value);
  };
  let loaded = false;
  let sent = 0;

  // An inline script's error names a line of the whole srcDoc; the widget's
  // author knows the lines of its own html.
  const ownLine = (diag: WidgetDiag): WidgetDiag => {
    if (diag.kind !== 'error' || diag.source !== 'about:srcdoc') return diag;
    const { source: _source, line, ...rest } = diag;
    return line && line >= htmlLine ? { ...rest, source: 'widget html', line: line - htmlLine + 1 } : rest;
  };

  const onMessage = (event: MessageEvent) => {
    if (event.source !== frame.contentWindow) return;
    const msg = readWidgetMessage(event.data);
    if (!msg) return;
    switch (msg.type) {
      case 'widget:resize': {
        const next = Math.min(maxHeight, Math.ceil(msg.height));
        frame.style.height = `${next}px`;
        emit('resize', next);
        return;
      }
      case 'widget:diag': {
        const diag = ownLine(msg.diag);
        if (sent < MAX_DIAGS) {
          sent++;
          window.__laWidgetDiag?.(diag).catch(() => {});
        }
        emit('diag', diag);
        return;
      }
      case 'widget:inspected':
        emit('inspected', msg.report);
        return;
      default:
        return;
    }
  };
  const onLoad = () => {
    loaded = true;
    emit('load', undefined);
  };
  window.addEventListener('message', onMessage);
  frame.addEventListener('load', onLoad);

  return {
    frame,
    get loaded() {
      return loaded;
    },
    on(event, listener) {
      listeners[event].add(listener);
      return () => listeners[event].delete(listener);
    },
    close() {
      window.removeEventListener('message', onMessage);
      frame.removeEventListener('load', onLoad);
    },
  };
}

async function netBusy(): Promise<number> {
  try {
    return (await window.__laWidgetNetBusy?.()) ?? 0;
  } catch {
    return 0;
  }
}

/**
 * Wait for the widget to settle: quiet for `settleMs` after the last resize,
 * diagnostic or load, with its document loaded and no request in flight, or
 * `maxWaitMs` in all (as widgetPdf.ts waits, plus the load and the network,
 * since a widget that fetches its data after the first report would otherwise
 * be caught empty). Answers with the last height the widget reported.
 */
function settle(
  channel: FrameChannel,
  { settleMs, maxWaitMs }: Pick<WidgetRenderPayload, 'settleMs' | 'maxWaitMs'>,
): Promise<{ settled: boolean; height: number | null }> {
  return new Promise((resolve) => {
    let height: number | null = null;
    let over = false;
    let quiet: ReturnType<typeof setTimeout> | undefined;
    const arm = () => {
      clearTimeout(quiet);
      quiet = setTimeout(() => void check(), settleMs);
    };
    const check = async () => {
      if (over) return;
      if (!channel.loaded || (await netBusy()) > 0) arm();
      else end(true);
    };
    const offs = [
      channel.on('resize', (h) => {
        height = h;
        arm();
      }),
      channel.on('diag', arm),
      channel.on('load', arm),
    ];
    const cap = setTimeout(() => end(false), maxWaitMs);
    function end(settled: boolean) {
      if (over) return;
      over = true;
      clearTimeout(quiet);
      clearTimeout(cap);
      offs.forEach((off) => off());
      resolve({ settled, height });
    }
  });
}

/** Give the frame `width` (and `frameHeight`, when given) and wait for it to lay out again. */
function relayout(channel: FrameChannel, width: number, frameHeight: number | null): Promise<void> {
  return new Promise((resolve) => {
    let still: ReturnType<typeof setTimeout> | undefined;
    const restart = () => {
      clearTimeout(still);
      still = setTimeout(end, RELAYOUT_QUIET_MS);
    };
    const off = channel.on('resize', restart);
    const cap = setTimeout(end, RELAYOUT_MAX_MS);
    function end() {
      clearTimeout(still);
      clearTimeout(cap);
      off();
      resolve();
    }
    channel.frame.style.width = `${width}px`;
    if (frameHeight !== null) channel.frame.style.height = `${frameHeight}px`;
    restart();
  });
}

/** The frame's answer to `widget:inspect`, or null when it gives none in time. */
function inspect(channel: FrameChannel, timeoutMs = INSPECT_TIMEOUT_MS): Promise<WidgetInspection | null> {
  return new Promise((resolve) => {
    const off = channel.on('inspected', (report) => finish(report));
    const timer = setTimeout(() => finish(null), timeoutMs);
    function finish(result: WidgetInspection | null) {
      clearTimeout(timer);
      off();
      resolve(result);
    }
    postToWidget(channel.frame.contentWindow, { type: 'widget:inspect' });
  });
}

/**
 * Mount the widget, wait for it to settle and inspect it. Then, time allowing,
 * inspect it again at `narrowWidth` and put it back at `width` for the
 * screenshot.
 */
async function run(payload: WidgetRenderPayload): Promise<WidgetRenderResult> {
  const { doc, htmlLine } = buildWidgetSrcDoc('widget-inline', {
    html: payload.html,
    data: payload.data ?? undefined,
    base: payload.base ?? undefined,
    // Fixed rather than read: this page carries none of the app's stylesheet.
    theme: LIGHT_PALETTE,
  });

  const frame = createWidgetFrame();
  frame.title = 'widget';
  frame.style.width = `${payload.width}px`;
  frame.style.height = `${INITIAL_HEIGHT_PX}px`;

  const deadline = performance.now() + payload.maxWaitMs;
  // Listening before the frame loads anything, so its first words are heard.
  const channel = frameChannel(frame, payload.maxHeight, htmlLine);
  const settling = settle(channel, payload);
  frame.srcdoc = doc;
  document.getElementById('root')!.appendChild(frame);

  const { settled, height } = await settling;
  const inspection = await inspect(channel);
  let narrow: WidgetInspection | null = null;
  if (inspection && payload.narrowWidth && deadline - performance.now() >= NARROW_MIN_LEFT_MS) {
    const mainHeight = frame.getBoundingClientRect().height;
    await relayout(channel, payload.narrowWidth, null);
    const left = deadline - performance.now() - RELAYOUT_MAX_MS;
    narrow = await inspect(channel, Math.max(100, Math.min(INSPECT_TIMEOUT_MS, left)));
    // Back to the main layout, which the screenshot shows.
    await relayout(channel, payload.width, mainHeight);
  }
  channel.close();
  return {
    settled,
    loaded: channel.loaded,
    height,
    frameHeight: frame.getBoundingClientRect().height,
    inspection,
    narrow,
  };
}

const style = document.createElement('style');
style.textContent = HOST_CSS;
document.head.appendChild(style);
window.__LA_WIDGET_RENDER_RUN__ = run;
