import { renderToPdf } from '@/lib/shellPdf';
import type { SavePdfResult } from '@/lib/desktop';
import { LIGHT_PALETTE, paletteCss } from './frameTheme';
import { createWidgetFrame } from './sandbox';
import {
  postToWidget,
  PRINT_HEIGHT_SLACK,
  PRINT_HEIGHT_SLACK_PX,
  readWidgetMessage,
  WIDGET_MAX_HEIGHT_PX,
  WIDGET_MAX_WAIT_MS,
  WIDGET_SETTLE_MS,
} from './widgetProtocol';

/** Enough to be a legible page if the widget never reports at all. */
const FALLBACK_HEIGHT_PX = 1000;

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
      const msg = readWidgetMessage(event.data);
      if (msg?.type !== 'widget:resize') return;
      height = Math.min(WIDGET_MAX_HEIGHT_PX, Math.max(height, msg.height));
      clearTimeout(quiet);
      quiet = setTimeout(finish, WIDGET_SETTLE_MS);
    };

    cap = setTimeout(finish, WIDGET_MAX_WAIT_MS);
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
    // The chat frame's tokens, so the copy runs as the widget did on screen.
    // Its popups need a click, which an offscreen node never gets.
    const frame = createWidgetFrame();
    frame.style.cssText = 'display:block;width:100%;border:0';
    frame.style.height = `${FALLBACK_HEIGHT_PX}px`;
    // Before the append, so the listener is in place for the load it causes.
    // The push is the same channel useHtmlSandbox uses on the frames that are
    // on screen. The light palette whatever the app wears: a srcDoc bakes its
    // theme in when it is built, and the paper is white, so a widget built
    // while the app was dark would otherwise land as dark cards on a white sheet.
    frame.addEventListener('load', () => {
      postToWidget(frame.contentWindow, { type: 'widget:themeUpdate', css: paletteCss(LIGHT_PALETTE) });
    });
    frame.srcdoc = srcDoc;
    root.appendChild(frame);

    const measured = await measureHeight(frame);
    frame.style.height = `${Math.ceil(measured * PRINT_HEIGHT_SLACK) + PRINT_HEIGHT_SLACK_PX}px`;
  }, fileName);
}
