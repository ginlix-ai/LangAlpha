/**
 * The stylesheet a widget document carries beside its theme: the base look of
 * what the widget leaves unstyled, and the layout of the box it is shown in.
 */

/**
 * The widget's outer element: the first child of body that draws. The tool asks
 * for <style> ahead of the content, so the widget streams in styled, and a
 * plain `:first-child` would land on that <style> instead.
 */
const OUTER_ELEMENT = 'body > :nth-child(1 of :not(style, script, link, meta, template, noscript))';

/**
 * CSS safety net: the widget's outer wrapper sits seamless on the chat, whatever
 * the agent wrote. Only a wrapper: a button, field, picture or table that
 * happens to come first keeps its own box.
 */
const SEAMLESS_OVERRIDE = `
${OUTER_ELEMENT}:not(button, input, select, textarea, a, img, svg, canvas, video, picture, table) {
  background: transparent !important;
  border: none !important;
  border-radius: 0 !important;
  box-shadow: none !important;
  margin: 0 auto !important;
}`;

/**
 * The widget is a separate document, so it inherits none of the app's chrome and
 * draws the platform's default scrollbar: a bright slab against a dark surface.
 * Applies to both layouts — an inline widget with its own scrolling table shows
 * the same one. Matches `styles/tokens.css`; keep them in step. A widget with
 * WebKit scrollbar rules of its own gets none of it: the two would merge rule by
 * rule, and a 6px scrollbar less this 3px inset on each side paints no thumb at
 * all. `scrollbar-width` and `scrollbar-color` cannot merge that way, so a
 * widget that sets only those still gets it, and a browser without them shows it.
 */
const SCROLLBARS = `
::-webkit-scrollbar { width: 12px; height: 12px; }
::-webkit-scrollbar-track { background: transparent; margin: 4px; }
::-webkit-scrollbar-thumb { background-color: var(--color-border-elevated); background-clip: padding-box; border: 3px solid transparent; border-radius: 6px; }
::-webkit-scrollbar-thumb:hover { background-color: var(--color-text-tertiary); }`;

/** The widget styles its own WebKit scrollbar, and the inset one would break it (SCROLLBARS). */
export function ownsScrollbar(html: string): boolean {
  return /-webkit-scrollbar/i.test(html);
}

/**
 * What an element the widget leaves unstyled looks like: the app's type, fields,
 * buttons and tables rather than the browser's, in either theme. Every selector
 * sits in `:where()`, so it weighs nothing and any rule the widget writes wins,
 * a bare element selector or an inline style included. Media are held to the
 * column, which is 350px on a phone. No SVG fill or stroke: CSS beats a
 * presentation attribute, so a default there would repaint every D3 chart.
 *
 * A button's hover mixes the ink into its own fill rather than painting
 * `--color-bg-hover`, a translucent wash meant for a row on the page: laid over
 * the transparent body it would replace the button's fill, which in dark goes
 * darker than the button at rest. Input-shaped buttons are drawn as buttons.
 */
const BASE_STYLES = `
:where(h1, h2, h3, h4) { margin: 0 0 .5em; font-weight: 500; line-height: 1.3; }
:where(h1) { font-size: 22px; }
:where(h2) { font-size: 18px; }
:where(h3) { font-size: 16px; }
:where(h4) { font-size: 14px; }
:where(p) { margin: 0 0 .75em; }
:where(small) { color: var(--color-text-secondary); }
:where(a) { color: var(--color-accent-primary); text-underline-offset: 2px; }
:where(img, video) { max-width: 100%; height: auto; }
:where(svg, canvas) { max-width: 100%; }
:where(table) { width: 100%; border-collapse: collapse; font-size: 13px; font-variant-numeric: tabular-nums; }
:where(th, td) { padding: 8px 10px; text-align: left; border-bottom: 1px solid var(--color-border-subtle); }
:where(th) { font-weight: 500; font-size: 12px; color: var(--color-text-secondary); }
:where(button, input, select, textarea) { font: inherit; color: inherit; }
:where(input:not([type=checkbox], [type=radio], [type=range], [type=color], [type=button], [type=submit], [type=reset]), select, textarea) { background: var(--color-bg-input); border: 1px solid var(--color-border-default); border-radius: 6px; padding: 6px 10px; }
:where(button, input[type=button], input[type=submit], input[type=reset]) { background: var(--color-bg-card); border: 1px solid var(--color-border-default); border-radius: 6px; padding: 6px 12px; cursor: pointer; }
:where(button, input[type=button], input[type=submit], input[type=reset]):where(:hover:not(:disabled)) { background: color-mix(in srgb, var(--color-bg-card), var(--color-text-primary) 8%); }
:where(button, input[type=button], input[type=submit], input[type=reset]):where(:disabled) { opacity: .5; cursor: default; }
:where(input[type=checkbox], input[type=radio], input[type=range], progress) { accent-color: var(--color-accent-primary); }
:where(:focus-visible) { outline: 2px solid var(--color-accent-primary); outline-offset: 2px; }
:where(code, pre, kbd) { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: .92em; }
:where(pre) { background: var(--color-bg-surface); padding: 10px 12px; border-radius: 6px; overflow-x: auto; }
:where(hr) { border: 0; border-top: 1px solid var(--color-border-default); margin: 16px 0; }`;

/** The body rule every layout shares; the layout adds its overflow. */
const BODY_BASE = "margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; font-size: 14px; line-height: 1.5; overflow-wrap: break-word; color: var(--color-text-primary); background: transparent;";

/**
 * Fullscreen caps at the chat column the widget was authored for and centres in
 * whatever room is left, rather than stretching: a widget built for a 768px
 * column has no layout to give at 1366px, so the extra width becomes dead space.
 *
 * The magnification that fills the dialog is NOT here. It is a `zoom` on the
 * host iframe (HtmlFullscreenModal.css), which is the element that knows how
 * much room there is; Chromium divides the child's layout viewport by it, so
 * this stays plain layout with no measurement in it. Keeping it out of the
 * document is also what stops it reaching the other boxes the same markup is
 * rendered in, notably the 680px print root.
 */
const FULLSCREEN_LAYOUT = `
body {
  max-width: 768px;
  margin-left: auto;
  margin-right: auto;
}
${OUTER_ELEMENT} {
  margin-left: auto !important;
  margin-right: auto !important;
}`;

/** The box a widget document is laid out for. The preview takes `inline`, as the widget that replaces it. */
export type FrameLayout = 'inline' | 'fullscreen';

/**
 * What each layout asks of the document, so the template carries no branches.
 *
 * A widget is authored for the chat column, so neither layout stretches to fill
 * a wider box and the inline one never scrolls vertically. Fullscreen still caps
 * at that column width, but it no longer clips what overflows it. `html` here
 * carries no overflow of its own, so the body's value propagates to the
 * viewport, and `overflow-x: hidden` there put a widget wider than the cap out
 * of reach entirely rather than merely making it untidy. A widget that does not
 * fit the column is a widget we did not author; a sideways scrollbar is the
 * better way to be wrong about one.
 */
const LAYOUTS: Record<FrameLayout, { bodyOverflow: string; layout: string }> = {
  inline: {
    // Seamless in the chat flow: the host sizes the iframe from the widget's
    // own reported height, so the document never scrolls down. It can scroll
    // across: a widget wider than the column would otherwise be out of reach, and
    // the height it reports counts what that scrollbar takes from the viewport,
    // unless it is sized in vh and already fills the frame: each report would grow
    // the frame, and the widget with it, without end.
    bodyOverflow: 'overflow-x: auto; overflow-y: hidden;',
    layout: SEAMLESS_OVERRIDE,
  },
  fullscreen: {
    bodyOverflow: 'overflow: auto; height: 100%;',
    layout: FULLSCREEN_LAYOUT,
  },
};

/** The document's stylesheet for `layout`; `scrollbars` false where the widget draws its own. */
export function frameStyles(layout: FrameLayout, scrollbars: boolean): string {
  const { bodyOverflow, layout: box } = LAYOUTS[layout];
  return `*, *::before, *::after { box-sizing: border-box; }
body { ${BODY_BASE} ${bodyOverflow} }
${BASE_STYLES}
${box}\n${scrollbars ? SCROLLBARS : ''}`;
}
