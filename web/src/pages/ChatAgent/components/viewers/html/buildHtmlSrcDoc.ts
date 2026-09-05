/**
 * srcDoc builder for sandboxed HTML widget iframes.
 *
 * Extracted verbatim from InlineWidget so widget rendering stays byte-identical
 * (see __tests__/buildHtmlSrcDoc.test.ts, locked against captured fixtures).
 * File documents do NOT use this — they load via the served wsfiles URL.
 */

export type HtmlSrcDocVariant = 'widget-inline' | 'widget-fullscreen';

export interface BuildHtmlSrcDocOptions {
  html: string;
  /** Inline data file contents — injected directly as __WIDGET_DATA__. */
  data?: Record<string, string>;
}

/** CSS variables to inject into the widget iframe for theme matching. */
export const THEME_VARS = [
  '--color-bg-page',
  '--color-bg-card',
  '--color-bg-elevated',
  '--color-bg-input',
  '--color-bg-surface',
  '--color-bg-hover',
  '--color-bg-subtle',
  '--color-border-muted',
  '--color-border-default',
  '--color-border-elevated',
  '--color-border-subtle',
  '--color-text-primary',
  '--color-text-secondary',
  '--color-text-tertiary',
  '--color-text-quaternary',
  '--color-text-muted',
  '--color-accent-primary',
  '--color-accent-soft',
  '--color-profit',
  '--color-profit-soft',
  '--color-loss',
  '--color-loss-soft',
  '--color-warning',
  '--color-info',
  '--color-success',
];

/** CSS safety net: force outermost element to be seamless regardless of agent HTML. */
const SEAMLESS_OVERRIDE = `
body > :first-child {
  background: transparent !important;
  border: none !important;
  border-radius: 0 !important;
  box-shadow: none !important;
  margin: 0 auto !important;
}`;

/**
 * The widget is a separate document, so it inherits none of the app's chrome and
 * draws the platform's default scrollbar: a bright slab against a dark surface.
 * Applies to both variants — an inline widget with its own scrolling table shows
 * the same one. Matches `styles/tokens.css`; keep them in step.
 */
const SCROLLBARS = `
::-webkit-scrollbar { width: 8px; height: 8px; }
::-webkit-scrollbar-track { background: transparent; }
::-webkit-scrollbar-thumb { background: var(--color-border-elevated); border-radius: 4px; }
::-webkit-scrollbar-thumb:hover { background: var(--color-text-tertiary); }`;

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
body > :first-child {
  margin-left: auto !important;
  margin-right: auto !important;
}`;

/**
 * `from` exists for the print path, which needs the light palette rather than
 * the one the document is currently wearing (see widgetPdf.ts). Every other
 * caller wants the live theme and passes nothing.
 */
export function resolveThemeVars(from: Element = document.documentElement): string {
  const style = getComputedStyle(from);
  const vars = THEME_VARS.map((v) => {
    const val = style.getPropertyValue(v).trim();
    return val ? `${v}: ${val};` : '';
  })
    .filter(Boolean)
    .join('\n  ');
  // The host <html> declares color-scheme (index.html); an embedded document
  // whose used color-scheme differs from its embedder's loses transparent
  // compositing — the browser paints the iframe canvas opaque white, which in
  // dark mode puts a white sheet behind the widget's transparent body. Mirror
  // the host's scheme into the injected :root block (themeUpdate pushes ride
  // along, since the iframe splices this string into that same block).
  const scheme = style.colorScheme;
  return scheme && scheme !== 'normal' ? `color-scheme: ${scheme};\n  ${vars}` : vars;
}

/**
 * What each variant asks of the document, so the template carries no branches.
 *
 * A widget is authored for the chat column, so neither variant stretches to fill
 * a wider box and the inline one never scrolls at all. Fullscreen still caps at
 * that column width, but it no longer clips what overflows it. `html` here
 * carries no overflow of its own, so the body's value propagates to the
 * viewport, and `overflow-x: hidden` there put a widget wider than the cap out
 * of reach entirely rather than merely making it untidy. A widget that does not
 * fit the column is a widget we did not author; a sideways scrollbar is the
 * better way to be wrong about one.
 */
const VARIANTS: Record<HtmlSrcDocVariant, { bodyOverflow: string; layout: string }> = {
  'widget-inline': {
    // Seamless in the chat flow: the host sizes the iframe from the widget's
    // own reported height, so the document never scrolls itself.
    bodyOverflow: 'overflow: hidden;',
    layout: SEAMLESS_OVERRIDE,
  },
  'widget-fullscreen': {
    bodyOverflow: 'overflow: auto; height: 100%;',
    layout: FULLSCREEN_LAYOUT,
  },
};

export function buildHtmlSrcDoc(
  variant: HtmlSrcDocVariant,
  { html, data: widgetData }: BuildHtmlSrcDocOptions,
): string {
  const themeCSS = resolveThemeVars();
  const dataScript = widgetData && Object.keys(widgetData).length > 0
    ? `<script>window.__WIDGET_DATA__ = ${JSON.stringify(widgetData).replace(/<\//g, '<\\/')};</script>\n`
    : '';

  // Injected before any widget code runs:
  // 1. Patch JSON.parse to handle NaN/Infinity from Python's json.dumps (not valid JSON).
  // 2. Catch uncaught errors and unhandled rejections, display an inline error overlay.
  // 3. Route link clicks to window.open(..., 'noopener'): a plain <a href>
  //    navigates the IFRAME itself, rendering the target inside the sandbox
  //    where it has no cookie access (real sites' bot checks break).
  const earlyScripts = `<script>
(function(){
  var _p=JSON.parse;
  JSON.parse=function(t,r){
    if(typeof t==='string')t=t.replace(/\\bNaN\\b/g,'null').replace(/(?<![A-Za-z_])-?Infinity\\b/g,'null');
    return _p.call(this,t,r);
  };
  var shown={},count=0,rendering=false;
  function showError(msg){
    if(rendering||shown[msg])return;
    shown[msg]=1;
    if(++count>5)return;
    rendering=true;
    msg=String(msg).slice(0,500);
    var root=document.body||document.documentElement;
    var d=document.createElement('div');
    d.style.cssText='margin:12px;padding:14px 16px;background:var(--color-bg-card);border:0.5px solid var(--color-border-muted);border-radius:10px;display:flex;align-items:center;gap:12px;';
    var dot=document.createElement('span');
    dot.style.cssText='flex-shrink:0;width:6px;height:6px;border-radius:50%;background:var(--color-loss);';
    var mid=document.createElement('div');
    mid.style.cssText='flex:1;min-width:0;';
    var detail=document.createElement('div');
    detail.style.cssText='font:13px/1.4 ui-monospace,SFMono-Regular,monospace;color:var(--color-text-muted);white-space:pre-wrap;word-break:break-word;';
    detail.textContent=msg;
    mid.appendChild(detail);
    var b=document.createElement('button');
    b.textContent='Fix';
    b.style.cssText='flex-shrink:0;padding:6px 14px;border-radius:8px;border:0.5px solid var(--color-border-default);background:var(--color-bg-elevated);color:var(--color-text-primary);font:500 13px/1 -apple-system,sans-serif;cursor:pointer;transition:background 0.15s;';
    b.onmouseover=function(){b.style.background='var(--color-bg-hover)';};
    b.onmouseout=function(){b.style.background='var(--color-bg-elevated)';};
    b.onclick=function(){window.sendPrompt('The widget threw an error: '+msg+'. Please fix the widget code and call ShowWidget again.');b.textContent='Sent';b.disabled=true;b.style.opacity='0.4';b.style.cursor='default';};
    d.appendChild(dot);d.appendChild(mid);d.appendChild(b);
    root.appendChild(d);
    parent.postMessage({type:'widget:resize',height:root.scrollHeight},'*');
    rendering=false;
  }
  window.onerror=function(_,__,___,____,e){showError(e&&e.message||String(_));};
  window.addEventListener('unhandledrejection',function(e){showError(e.reason&&e.reason.message||String(e.reason));});
  document.addEventListener('click',function(e){
    if(e.defaultPrevented)return;
    var a=e.target&&e.target.closest?e.target.closest('a[href]'):null;
    if(!a)return;
    var href=a.getAttribute('href')||'';
    if(href.charAt(0)==='#')return;
    if(!/^https?:/i.test(a.href)){e.preventDefault();return;}
    if(a.host===location.host)return;
    e.preventDefault();
    window.open(a.href,'_blank','noopener,noreferrer');
  });
})();
</script>\n`;

  return `<!DOCTYPE html><html><head>
<meta http-equiv="Content-Security-Policy" content="default-src 'none'; script-src 'unsafe-inline' cdnjs.cloudflare.com cdn.jsdelivr.net unpkg.com esm.sh; style-src 'unsafe-inline'; img-src data: blob:; font-src cdnjs.cloudflare.com cdn.jsdelivr.net; connect-src cdnjs.cloudflare.com cdn.jsdelivr.net unpkg.com esm.sh;">
<style>
:root {
  ${themeCSS}
}
*, *::before, *::after { box-sizing: border-box; }
body { margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; color: var(--color-text-primary); background: transparent; ${VARIANTS[variant].bodyOverflow} }
${VARIANTS[variant].layout}\n${SCROLLBARS}
</style>
${earlyScripts}${dataScript}<script>
window.sendPrompt = function(text) {
  parent.postMessage({ type: 'widget:sendPrompt', text: String(text) }, '*');
};
(function() {
  var lastH = 0;
  var pending = 0;
  function reportHeight() {
    if (!document.body) return;
    var h = document.body.scrollHeight;
    if (h > 0 && Math.abs(h - lastH) > 2) {
      lastH = h;
      parent.postMessage({ type: 'widget:resize', height: h }, '*');
    }
  }
  function debouncedReport() {
    if (pending) return;
    pending = requestAnimationFrame(function() {
      pending = 0;
      reportHeight();
    });
  }
  document.addEventListener('DOMContentLoaded', function() {
    var mo = new MutationObserver(debouncedReport);
    mo.observe(document.body, { childList: true, subtree: true });
    if (typeof ResizeObserver === 'function') {
      new ResizeObserver(debouncedReport).observe(document.body);
    }
    reportHeight();
  });
  var checks = [100, 300, 800, 2000, 5000];
  checks.forEach(function(ms) { setTimeout(reportHeight, ms); });
  // A container width change is a pure reflow — no DOM mutation — so the
  // observers above never see it; track the viewport and body box directly.
  window.addEventListener('resize', debouncedReport);
  window.addEventListener('message', function(e) {
    if (e.data && e.data.type === 'widget:themeUpdate' && e.data.css) {
      var style = document.querySelector('style');
      if (style) {
        style.textContent = style.textContent.replace(
          /:root\\s*\\{[^}]*\\}/,
          ':root {\\n  ' + e.data.css + '\\n}'
        );
      }
    }
  });
})();
</script>
</head><body>${html}</body></html>`;
}
