import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import './InlineWidget.css';

interface InlineWidgetProps {
  html: string;
  title?: string;
  onSendPrompt?: (text: string) => void;
  /** Inline data file contents — injected directly as __WIDGET_DATA__. */
  data?: Record<string, string>;
}

/** CSS variables to inject into the widget iframe for theme matching. */
const THEME_VARS = [
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
  margin: 0 !important;
}`;

function resolveThemeVars(): string {
  const style = getComputedStyle(document.documentElement);
  return THEME_VARS.map((v) => {
    const val = style.getPropertyValue(v).trim();
    return val ? `${v}: ${val};` : '';
  })
    .filter(Boolean)
    .join('\n  ');
}

function buildSrcDoc(html: string, widgetData?: Record<string, string>): string {
  const themeCSS = resolveThemeVars();
  const dataScript = widgetData && Object.keys(widgetData).length > 0
    ? `<script>window.__WIDGET_DATA__ = ${JSON.stringify(widgetData).replace(/<\//g, '<\\/')};</script>\n`
    : '';

  // Injected before any widget code runs:
  // 1. Patch JSON.parse to handle NaN/Infinity from Python's json.dumps (not valid JSON).
  // 2. Catch uncaught errors and unhandled rejections, display an inline error overlay.
  const earlyScripts = `<script>
(function(){
  var _p=JSON.parse;
  JSON.parse=function(t,r){
    if(typeof t==='string')t=t.replace(/\\bNaN\\b/g,'null').replace(/\\b-?Infinity\\b/g,'null');
    return _p.call(this,t,r);
  };
  var shown={};
  function showError(msg){
    if(shown[msg])return;
    shown[msg]=1;
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
  }
  window.onerror=function(_,__,___,____,e){showError(e&&e.message||String(_));};
  window.addEventListener('unhandledrejection',function(e){showError(e.reason&&e.reason.message||String(e.reason));});
})();
</script>\n`;

  return `<!DOCTYPE html><html><head>
${earlyScripts}<meta http-equiv="Content-Security-Policy" content="default-src 'none'; script-src 'unsafe-inline' cdnjs.cloudflare.com cdn.jsdelivr.net unpkg.com esm.sh; style-src 'unsafe-inline'; img-src data: blob:; font-src cdnjs.cloudflare.com cdn.jsdelivr.net; connect-src cdnjs.cloudflare.com cdn.jsdelivr.net unpkg.com esm.sh;">
<style>
:root {
  ${themeCSS}
}
*, *::before, *::after { box-sizing: border-box; }
body { margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; color: var(--color-text-primary); background: transparent; overflow: hidden; }
${SEAMLESS_OVERRIDE}
</style>
${dataScript}<script>
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
    reportHeight();
  });
  var checks = [100, 300, 800, 2000, 5000];
  checks.forEach(function(ms) { setTimeout(reportHeight, ms); });
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

export default function InlineWidget({ html, title, onSendPrompt, data }: InlineWidgetProps) {
  const iframeRef = useRef<HTMLIFrameElement>(null);
  const [height, setHeight] = useState<number | null>(null);

  const srcDoc = useMemo(() => buildSrcDoc(html, data), [html, data]);

  const handleMessage = useCallback(
    (e: MessageEvent) => {
      if (!iframeRef.current || e.source !== iframeRef.current.contentWindow) return;

      const { type, height: h, text } = e.data || {};
      if (type === 'widget:resize' && typeof h === 'number' && h > 0) {
        setHeight((prev) => {
          const next = Math.ceil(h);
          return prev === next ? prev : next;
        });
      } else if (type === 'widget:sendPrompt' && typeof text === 'string' && text.trim()) {
        onSendPrompt?.(text.trim());
      }
    },
    [onSendPrompt],
  );

  useEffect(() => {
    window.addEventListener('message', handleMessage);
    return () => window.removeEventListener('message', handleMessage);
  }, [handleMessage]);

  useEffect(() => {
    const observer = new MutationObserver(() => {
      if (!iframeRef.current?.contentWindow) return;
      const themeCSS = resolveThemeVars();
      iframeRef.current.contentWindow.postMessage(
        { type: 'widget:themeUpdate', css: themeCSS },
        '*',
      );
    });
    observer.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ['data-theme'],
    });
    return () => observer.disconnect();
  }, []);

  // No max-height cap — widgets span naturally to fit content (charts, tables,
  // dashboards). The agent controls HTML output and the skill doc guides it to
  // keep widgets reasonable. A cap would add scroll-in-scroll UX that's worse
  // than a tall widget pushing chat down.
  return (
    <div className="inline-widget-container">
      <iframe
        ref={iframeRef}
        srcDoc={srcDoc}
        sandbox="allow-scripts"
        title={title || 'Widget'}
        className="inline-widget-frame"
        style={{
          height: height != null ? `${height}px` : '150px',
          opacity: height != null ? 1 : 0,
        }}
      />
    </div>
  );
}
