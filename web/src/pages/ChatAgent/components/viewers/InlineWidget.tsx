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

  return `<!DOCTYPE html><html><head>
<meta http-equiv="Content-Security-Policy" content="default-src 'none'; script-src 'unsafe-inline' cdnjs.cloudflare.com cdn.jsdelivr.net unpkg.com esm.sh; style-src 'unsafe-inline'; img-src data: blob:; font-src cdnjs.cloudflare.com cdn.jsdelivr.net; connect-src cdnjs.cloudflare.com cdn.jsdelivr.net unpkg.com esm.sh;">
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

export default function InlineWidget({ html, onSendPrompt, data }: InlineWidgetProps) {
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

  return (
    <div className="inline-widget-container">
      <iframe
        ref={iframeRef}
        srcDoc={srcDoc}
        sandbox="allow-scripts"
        title="Widget"
        className="inline-widget-frame"
        style={{
          height: height != null ? `${height}px` : '150px',
          opacity: height != null ? 1 : 0,
        }}
      />
    </div>
  );
}
