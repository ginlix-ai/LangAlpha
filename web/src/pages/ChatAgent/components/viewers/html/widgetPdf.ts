import { renderToPdf } from '@/lib/shellPdf';
import type { SavePdfResult } from '@/lib/desktop';
import { resolveThemeVars } from './buildHtmlSrcDoc';

/**
 * How long to keep waiting for the widget to stop changing height.
 *
 * The srcDoc reports its height on DOMContentLoaded and then again at 100ms,
 * 300ms, 800ms, 2s and 5s, because a widget that pulls a chart library off a CDN
 * is not done when the document is. Settling on quiet rather than on a fixed
 * delay keeps a simple widget fast and still gives a slow one its last report.
 */
const SETTLE_MS = 700;
const MAX_WAIT_MS = 8000;
/** Enough to be a legible page if the widget never reports at all. */
const FALLBACK_HEIGHT_PX = 1000;

/**
 * Slack on the measured height.
 *
 * The box is the printed area — Chromium paginates what is inside an iframe but
 * not past the bottom of the box — and the print pass lays the content out
 * again, landing a hair taller than the measurement: different rasterization,
 * and a page content box that is 180mm rather than exactly 680px. Measured
 * without this, a widget lost its last element with no other symptom. Too tall
 * only costs trailing white space; too short deletes content silently.
 */
const HEIGHT_SLACK = 1.02;
const HEIGHT_SLACK_PX = 32;

/**
 * Roughly forty A4 pages at this width, and the point past which a number is
 * not a widget that got long. The height is authored by the widget, so it is
 * bounded here rather than trusted: an unbounded box is one Chromium's print
 * service has to lay out and paginate.
 */
const MAX_HEIGHT_PX = 40_000;

/**
 * The light palette, read off a probe rather than off the live document.
 *
 * A srcDoc bakes its theme in as literal values when it is built, and the shell
 * prints on white paper. So neither the baked theme nor the one the app happens
 * to be wearing is the right answer: a widget authored while the app was dark
 * otherwise lands as dark cards on a white sheet.
 *
 * `[data-theme="light"]` in styles/tokens.css is a bare attribute selector, so
 * it applies to any element, not only the root. It has to be in the document to
 * have a computed style at all, and `color-scheme` is inherited rather than
 * declared per theme, so the probe states its own.
 */
function lightThemeCss(): string {
  const probe = document.createElement('div');
  probe.dataset.theme = 'light';
  probe.style.cssText = 'position:fixed;left:-10000px;top:0;color-scheme:light';
  document.body.appendChild(probe);
  try {
    return resolveThemeVars(probe);
  } finally {
    probe.remove();
  }
}

/**
 * Wait for the widget to settle, and answer with the height it needs.
 *
 * The iframe is sandboxed without `allow-same-origin`, so its document is an
 * opaque origin this side cannot read: the height can only come from the
 * `widget:resize` message the srcDoc already posts. Matched on `event.source`
 * rather than the payload, because every widget on the page posts the same
 * shape and the inline ones are still running.
 */
function measureHeight(frame: HTMLIFrameElement): Promise<number> {
  return new Promise((resolve) => {
    let height = 0;
    let quiet: ReturnType<typeof setTimeout> | undefined;
    let cap: ReturnType<typeof setTimeout> | undefined;

    const finish = () => {
      clearTimeout(quiet);
      clearTimeout(cap);
      window.removeEventListener('message', onMessage);
      resolve(height || FALLBACK_HEIGHT_PX);
    };

    const onMessage = (event: MessageEvent) => {
      if (event.source !== frame.contentWindow) return;
      const data = event.data as { type?: string; height?: number } | null;
      if (!data || data.type !== 'widget:resize') return;
      // Bounded, not merely typed. The widget authors this number: `NaN` is a
      // number and would poison `Math.max` for every later report, leaving the
      // fallback height as the only outcome, and `Infinity` reaches the style
      // as `Infinitypx`, which the parser drops so the box keeps whatever it
      // had. Both of those clip content silently, which is the one failure
      // mode this measurement exists to avoid.
      const reported = data.height;
      if (typeof reported !== 'number' || !Number.isFinite(reported) || reported <= 0) return;
      height = Math.min(MAX_HEIGHT_PX, Math.max(height, reported));
      clearTimeout(quiet);
      quiet = setTimeout(finish, SETTLE_MS);
    };

    cap = setTimeout(finish, MAX_WAIT_MS);
    window.addEventListener('message', onMessage);
  });
}

/**
 * Render a widget to a PDF through the desktop shell.
 *
 * A widget lives in a sandboxed iframe and the shell renders the window it is
 * asked from, so the export is a second iframe carrying the same srcDoc. Its own
 * iframe rather than the one on screen because the print reflow unmounts the
 * modal around it, and its box has to be as tall as the content: Chromium
 * paginates what is inside an iframe, but not past the bottom of the box.
 *
 * Takes the inline srcDoc, never the fullscreen one. The fullscreen variant is
 * sized and magnified for the dialog it is displayed in (HtmlFullscreenModal.css);
 * on paper that box is 680px wide and neither number means anything.
 */
export function saveWidgetPdf(srcDoc: string, fileName: string): Promise<SavePdfResult | null> {
  return renderToPdf(async (root) => {
    const frame = document.createElement('iframe');
    // No popup tokens: nothing in a print copy is clickable, and the srcDoc's
    // link handler would otherwise be able to open tabs from an offscreen node.
    frame.setAttribute('sandbox', 'allow-scripts');
    frame.style.cssText = 'display:block;width:100%;border:0';
    frame.style.height = `${FALLBACK_HEIGHT_PX}px`;
    // Before the append, so the listener is in place for the load it causes.
    // The push is the same channel useHtmlSandbox uses on the frames that are
    // on screen, and '*' for the same reason: sandboxed without
    // allow-same-origin, this document has no origin that can be named.
    frame.addEventListener('load', () => {
      frame.contentWindow?.postMessage(
        { type: 'widget:themeUpdate', css: lightThemeCss() },
        '*',
      );
    });
    frame.srcdoc = srcDoc;
    root.appendChild(frame);

    const measured = await measureHeight(frame);
    frame.style.height = `${Math.ceil(measured * HEIGHT_SLACK) + HEIGHT_SLACK_PX}px`;
  }, fileName);
}
