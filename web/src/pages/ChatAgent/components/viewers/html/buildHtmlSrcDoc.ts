/**
 * srcDoc builder for sandboxed HTML widget iframes: the chat's inline and
 * fullscreen frames, the print copy, the exported copies, the preview drawn
 * while a call streams, and the headless render page the server runs after a
 * ShowWidget call (`src/widgetRender/`). That page is built apart from the
 * app, so this module and what it imports stay pure string building: no
 * React, no app singletons, and the DOM is read only when no `theme` is
 * passed. Locked against captured fixtures (__tests__/buildHtmlSrcDoc.test.ts).
 * File documents do NOT use this; they load via the served wsfiles URL.
 *
 * The parts live beside it: the theme (frameTheme.ts), the stylesheet
 * (frameStyles.ts), the programs the document runs (frameScripts/), and what
 * the frame and its parent say to each other (widgetProtocol.ts).
 */
import { escapeAttr, scriptJson } from './docUtils';
import { BRIDGE_SCRIPT, EARLY_SCRIPT, STREAM_RECEIVER_SCRIPT } from './frameScripts';
import { frameStyles, ownsScrollbar, type FrameLayout } from './frameStyles';
import { paletteCss, readThemePalette, type ThemePalette } from './frameTheme';
import { SEND_PROMPT_DATA_MAX_BYTES } from './widgetProtocol';

export { STREAM_RECEIVER_SCRIPT } from './frameScripts';

export type HtmlSrcDocVariant = 'widget-inline' | 'widget-fullscreen';

export interface BuildHtmlSrcDocOptions {
  html: string;
  /** Inline data file contents, injected as `window.__WIDGET_DATA__`. */
  data?: Record<string, string>;
  /**
   * Absolute URL ending in `/`, emitted as `<base href>`: the workspace folder
   * under the viewer's file grant, so `<img src="memes/01.jpg">` and
   * `fetch("data/bars.json")` load the workspace's files. Omitted, relative
   * URLs resolve against the embedding page, which serves none of them.
   */
  base?: string;
  /**
   * What the localStorage stand-in starts with on an opaque origin, where the
   * browser refuses real storage. Its writes go to the parent as
   * `widget:storage`, which keeps them for the next render.
   */
  storageSeed?: Record<string, string>;
  /** A fixed palette. Omitted, the live document's theme is read (`readThemePalette`). */
  theme?: ThemePalette;
}

export interface BuildStreamSrcDocOptions {
  base?: string;
  theme?: ThemePalette;
}

/**
 * The load policy of a served HTML report (`src/server/utils/html_policy.py`,
 * `_LOADS`): it limits nothing a document loads, since a widget that cannot
 * reach its chart library, an image or a public API just looks broken. What a
 * widget can read is settled by the opaque origin (`WIDGET_SANDBOX`) and the
 * grant its `<base>` carries. Keep the two in step.
 */
export const WIDGET_CSP = "default-src * data: blob: 'unsafe-inline' 'unsafe-eval'";

const LAYOUT: Record<HtmlSrcDocVariant, FrameLayout> = {
  'widget-inline': 'inline',
  'widget-fullscreen': 'fullscreen',
};

/**
 * The theme's own element. A `widget:themeUpdate` push rewrites it whole
 * (frameScripts/bridge.js writes this same text), then fires `themechange` on
 * the window, so a widget can repaint what it drew from the variables (a
 * canvas keeps the colors it was painted with). The first theme fires
 * nothing: it is parsed before any widget script runs, and the template never
 * applies it later, so there is nothing a listener could miss. A frame that
 * needs another theme after load (the fullscreen dialog, the print copy) gets
 * it as a push, which does fire.
 */
function themeStyle(css: string): string {
  return `<style id="widget-theme">\n:root {\n  ${css}\n}\n</style>`;
}

interface FrameHeadOptions {
  base?: string;
  theme?: ThemePalette;
  csp: string;
  layout: FrameLayout;
  scrollbars: boolean;
}

/** The start of either document, up to where their scripts differ. */
function frameHead({ base, theme, csp, layout, scrollbars }: FrameHeadOptions): string {
  const themeCss = paletteCss(theme ?? readThemePalette());
  // First in <head>: a <base> applies only to URLs resolved after it.
  const baseTag = base ? `<base href="${escapeAttr(base)}">\n` : '';
  return `<!DOCTYPE html><html><head>
${baseTag}<meta http-equiv="Content-Security-Policy" content="${csp}">
${themeStyle(themeCss)}
<style>
${frameStyles(layout, scrollbars)}
</style>
`;
}

/**
 * The widget's document, and the line of it on which the widget's own html
 * starts: an inline script's error names a line of the whole document, and
 * the widget's author knows the lines of its own html.
 */
export function buildWidgetSrcDoc(
  variant: HtmlSrcDocVariant,
  { html, data: widgetData, base, storageSeed, theme }: BuildHtmlSrcDocOptions,
): { doc: string; htmlLine: number } {
  const dataScript = widgetData && Object.keys(widgetData).length > 0
    ? `<script>window.__WIDGET_DATA__ = ${scriptJson(widgetData)};</script>\n`
    : '';
  const head = `${frameHead({ base, theme, csp: WIDGET_CSP, layout: LAYOUT[variant], scrollbars: !ownsScrollbar(html) })}<script>
${EARLY_SCRIPT}(${scriptJson(storageSeed ?? {})},${scriptJson(base ?? '')});
</script>
${dataScript}<script>
${BRIDGE_SCRIPT}(${SEND_PROMPT_DATA_MAX_BYTES});
</script>
</head><body>`;
  return { doc: `${head}${html}</body></html>`, htmlLine: head.split('\n').length };
}

/** A fresh nonce for the preview's own scripts, so none in the markup can claim it. */
function scriptNonce(): string {
  const bytes = new Uint8Array(16);
  globalThis.crypto.getRandomValues(bytes);
  return btoa(String.fromCharCode(...bytes));
}

/**
 * The preview drawn while a ShowWidget call's arguments stream: the widget's
 * policy, theme, base, inline layout and height reports, with the receiver
 * (`STREAM_RECEIVER_SCRIPT`) in place of the widget's code and an empty body,
 * which the parent fills by posting `widget:streamHtml`. Laid out as the inline
 * widget that replaces it, so the hand-over moves nothing. Only scripts
 * carrying its nonce run, so markup drawn into it never does.
 */
export function buildStreamSrcDoc({ base, theme }: BuildStreamSrcDocOptions = {}): string {
  const nonce = scriptNonce();
  return `${frameHead({ base, theme, csp: `${WIDGET_CSP}; script-src 'nonce-${nonce}'`, layout: 'inline', scrollbars: true })}<script nonce="${nonce}">
${BRIDGE_SCRIPT}(${SEND_PROMPT_DATA_MAX_BYTES});
</script>
<script nonce="${nonce}">
${STREAM_RECEIVER_SCRIPT}();
</script>
</head><body></body></html>`;
}

export function buildHtmlSrcDoc(variant: HtmlSrcDocVariant, options: BuildHtmlSrcDocOptions): string {
  return buildWidgetSrcDoc(variant, options).doc;
}
