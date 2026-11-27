/**
 * What a widget frame and the page around it say to each other, and the one
 * reader of what the frame says. The frame runs agent-written code, so every
 * message from it is read here, field by field, before any host acts on it:
 * the chat's frames (useHtmlSandbox), the print copy (widgetPdf), the headless
 * render (src/widgetRender) and the shell a copy opens in (widgetExport).
 */

/** The widget's three ways to speak as the user (frameScripts/bridge.js). */
export type WidgetPromptCall = 'sendPrompt' | 'draftPrompt' | 'startThread';

/** `data` is what the widget passed beside `text`, already bounded; undefined when it passed none or too much. */
export type WidgetPromptHandler = (text: string, data?: unknown) => void;

/** A host's answer to each prompt call; a call it leaves out goes unanswered. */
export type FramePrompts = Partial<Record<WidgetPromptCall, WidgetPromptHandler>>;

/** A localStorage write from the frame's stand-in (frameScripts/early.js). */
export type WidgetStorageWrite = { key: string; value: string | null } | { clear: true };

/** A diagnostic the frame posted (`widget:diag`), as the render contract shapes it. */
export type WidgetDiag =
  | { kind: 'error'; message: string; source?: string; line?: number }
  | { kind: 'blocked'; directive: string; url: string }
  | { kind: 'load-failed'; tag: string; url: string };

/** The frame's answer to `widget:inspect`. */
export interface WidgetInspection {
  height: number;
  width: number;
  scrollWidth: number;
  textChars: number;
  elements: number;
  images: { total: number; broken: number };
  canvases: number;
  svgs: number;
  fixedElements: number;
  dataKeys: string[];
  visibleText: string;
}

export type WidgetFrameMessage =
  /** Finite and above zero; each host rounds and bounds it for its own box. */
  | { type: 'widget:resize'; height: number }
  | { type: `widget:${WidgetPromptCall}`; call: WidgetPromptCall; text: string; data?: unknown }
  | { type: 'widget:storage'; write: WidgetStorageWrite }
  /** An http(s) or data:image URL, the only kind the app's lightbox may load. */
  | { type: 'widget:zoom'; src: string; alt?: string }
  | { type: 'widget:diag'; diag: WidgetDiag }
  /** Null when the answer was not a report at all. */
  | { type: 'widget:inspected'; report: WidgetInspection | null };

export type WidgetHostMessage =
  /** `paletteCss` declarations for the document's `:root`. */
  | { type: 'widget:themeUpdate'; css: string }
  | { type: 'widget:inspect' }
  /** A write another frame of the same widget made, for this frame's localStorage stand-in. */
  | { type: 'widget:storageSync'; write: WidgetStorageWrite }
  /** Everything the preview has to draw so far (frameScripts/stream.js). */
  | { type: 'widget:streamHtml'; html: string };

/**
 * Over this, a prompt call's `data` is dropped, counted as UTF-8 bytes of its
 * JSON. The frame checks first, to warn the widget's author; the parent checks
 * again, since the widget can replace the frame's check.
 */
export const SEND_PROMPT_DATA_MAX_BYTES = 32 * 1024;

/**
 * How long a copy waits for the widget to stop changing height, and in all.
 * The frame reports on DOMContentLoaded and again at 100ms, 300ms, 800ms, 2s
 * and 5s, because a widget that pulls a chart library off a CDN is not done
 * when the document is. Settling on quiet rather than on a fixed delay keeps a
 * simple widget fast and still gives a slow one its last report.
 */
export const WIDGET_SETTLE_MS = 700;
export const WIDGET_MAX_WAIT_MS = 8000;

/**
 * Roughly forty A4 pages at the chat's width, and the point past which a number
 * is not a widget that got long. The height is authored by the widget, so a
 * copy bounds it rather than trusting it: an unbounded box is one Chromium's
 * print service has to lay out and paginate.
 */
export const WIDGET_MAX_HEIGHT_PX = 40_000;

/**
 * Slack on a height measured for paper.
 *
 * The box is the printed area (Chromium paginates what is inside an iframe but
 * not past the bottom of the box), and the print pass lays the content out
 * again, landing a hair taller than the measurement: different rasterization,
 * and a page content box that is a length in millimetres rather than whole
 * pixels. Measured without this, a widget lost its last element with no other
 * symptom. Too tall only costs trailing white space; too short deletes content
 * silently.
 */
export const PRINT_HEIGHT_SLACK = 1.02;
export const PRINT_HEIGHT_SLACK_PX = 32;

/**
 * The printable width of A4 and of Letter at the browser's default margins,
 * near enough, in CSS pixels. A copy for paper is laid out and measured at
 * this width: measured wider, it reflows taller on the page than the box it
 * was given, and what runs past the box is not printed.
 */
export const PRINT_WIDTH_PX = 718;

/** Any URL but an image's own data: a lightbox source has no reason to be longer. */
const ZOOM_URL_MAX = 8192;
/** An inline image is its bytes, so it gets room for an ordinary photo or chart. */
const ZOOM_DATA_IMAGE_MAX = 8 * 1024 * 1024;

const PROMPT_CALLS: Record<string, WidgetPromptCall> = {
  'widget:sendPrompt': 'sendPrompt',
  'widget:draftPrompt': 'draftPrompt',
  'widget:startThread': 'startThread',
};

const encoder = new TextEncoder();

function boundedData(data: unknown, call: WidgetPromptCall): unknown {
  if (data === undefined) return undefined;
  let serialized: string | undefined;
  try {
    serialized = JSON.stringify(data);
  } catch {
    serialized = undefined;
  }
  if (serialized === undefined || encoder.encode(serialized).length > SEND_PROMPT_DATA_MAX_BYTES) {
    console.warn(`[widget] ${call} data dropped: not JSON or over 32 KB serialized`);
    return undefined;
  }
  return data;
}

function str(value: unknown, max = 500): string {
  return typeof value === 'string' ? value.slice(0, max) : '';
}

function num(value: unknown): number {
  return typeof value === 'number' && Number.isFinite(value) ? value : 0;
}

function readDiag(raw: Record<string, unknown>): WidgetDiag | null {
  switch (raw.kind) {
    case 'error': {
      const message = str(raw.message);
      if (!message) return null;
      const diag: WidgetDiag = { kind: 'error', message };
      if (typeof raw.source === 'string' && raw.source) diag.source = str(raw.source);
      if (typeof raw.line === 'number' && Number.isFinite(raw.line)) diag.line = raw.line;
      return diag;
    }
    case 'blocked':
      return { kind: 'blocked', directive: str(raw.directive, 100), url: str(raw.url) };
    case 'load-failed':
      return { kind: 'load-failed', tag: str(raw.tag, 20), url: str(raw.url) };
    default:
      return null;
  }
}

function readInspection(raw: unknown): WidgetInspection | null {
  if (!raw || typeof raw !== 'object') return null;
  const r = raw as Record<string, unknown>;
  const images = (r.images && typeof r.images === 'object' ? r.images : {}) as Record<string, unknown>;
  return {
    height: num(r.height),
    width: num(r.width),
    scrollWidth: num(r.scrollWidth),
    textChars: num(r.textChars),
    elements: num(r.elements),
    images: { total: num(images.total), broken: num(images.broken) },
    canvases: num(r.canvases),
    svgs: num(r.svgs),
    fixedElements: num(r.fixedElements),
    dataKeys: Array.isArray(r.dataKeys) ? r.dataKeys.filter((k): k is string => typeof k === 'string').slice(0, 50) : [],
    visibleText: str(r.visibleText, 300),
  };
}

/**
 * A message from a widget frame, rebuilt from its typed fields, or null for
 * anything else. Who sent it is the caller's to check (`event.source`), since
 * every widget on the page posts the same shapes.
 */
export function readWidgetMessage(data: unknown): WidgetFrameMessage | null {
  if (!data || typeof data !== 'object') return null;
  const msg = data as Record<string, unknown>;
  const { type } = msg;
  if (typeof type !== 'string') return null;
  switch (type) {
    case 'widget:resize': {
      // Bounded, not merely typed: `NaN` is a number and would poison a
      // `Math.max` over later reports, and `Infinity` reaches a style as
      // `Infinitypx`, which the parser drops. Both clip content silently.
      const h = msg.height;
      return typeof h === 'number' && Number.isFinite(h) && h > 0 ? { type, height: h } : null;
    }
    case 'widget:sendPrompt':
    case 'widget:draftPrompt':
    case 'widget:startThread': {
      const call = PROMPT_CALLS[type];
      const text = typeof msg.text === 'string' ? msg.text.trim() : '';
      if (!text) return null;
      return { type, call, text, data: boundedData(msg.data, call) };
    }
    case 'widget:storage':
      if (msg.clear === true) return { type, write: { clear: true } };
      if (typeof msg.key === 'string' && (typeof msg.value === 'string' || msg.value === null)) {
        return { type, write: { key: msg.key, value: msg.value } };
      }
      return null;
    case 'widget:zoom': {
      const { src } = msg;
      if (typeof src !== 'string') return null;
      const dataImage = /^data:image\//i.test(src);
      if (!dataImage && !/^https?:/i.test(src)) return null;
      if (src.length > (dataImage ? ZOOM_DATA_IMAGE_MAX : ZOOM_URL_MAX)) return null;
      return typeof msg.alt === 'string' ? { type, src, alt: msg.alt.slice(0, 500) } : { type, src };
    }
    case 'widget:diag': {
      const diag = readDiag(msg);
      return diag && { type, diag };
    }
    case 'widget:inspected':
      return { type, report: readInspection(msg.report) };
    default:
      return null;
  }
}

/**
 * Post to a widget frame. To '*': sandboxed without `allow-same-origin`, the
 * frame's document has an opaque origin, which no targetOrigin can name.
 */
export function postToWidget(win: Window | null | undefined, message: WidgetHostMessage): void {
  win?.postMessage(message, '*');
}
