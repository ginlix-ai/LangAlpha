import { useCallback, useEffect, useRef, useState } from 'react';
import { resolveThemeVars } from './buildHtmlSrcDoc';

interface UseHtmlSandboxOptions {
  iframeRef: React.RefObject<HTMLIFrameElement | null>;
  /** When true, listen for widget:resize and expose the reported height. */
  autoHeight: boolean;
  /** Called when the iframe posts a widget:sendPrompt message. */
  onSendPrompt?: (text: string) => void;
}

interface UseHtmlSandboxResult {
  /** Reported iframe height (null until first resize), only set when autoHeight. */
  height: number | null;
  /** Push current theme vars into the iframe (call after a served iframe loads). */
  pushTheme: () => void;
}

/**
 * Owns the parent-side postMessage bridge for a sandboxed HTML iframe:
 * resize → height (when autoHeight), sendPrompt → callback, and a theme
 * MutationObserver that re-pushes widget:themeUpdate on data-theme changes.
 */
export function useHtmlSandbox({
  iframeRef,
  autoHeight,
  onSendPrompt,
}: UseHtmlSandboxOptions): UseHtmlSandboxResult {
  const [height, setHeight] = useState<number | null>(null);

  const pushTheme = useCallback(() => {
    const win = iframeRef.current?.contentWindow;
    if (!win) return;
    // Target '*' is intentional: `sandbox allow-scripts` (no allow-same-origin)
    // gives the iframe an opaque origin, which can't be named as a targetOrigin.
    win.postMessage({ type: 'widget:themeUpdate', css: resolveThemeVars() }, '*');
  }, [iframeRef]);

  const handleMessage = useCallback(
    (e: MessageEvent) => {
      if (!iframeRef.current || e.source !== iframeRef.current.contentWindow) return;

      const { type, height: h, text } = e.data || {};
      if (autoHeight && type === 'widget:resize' && typeof h === 'number' && h > 0) {
        setHeight((prev) => {
          const next = Math.ceil(h);
          return prev === next ? prev : next;
        });
      } else if (type === 'widget:sendPrompt' && typeof text === 'string' && text.trim()) {
        onSendPrompt?.(text.trim());
      }
    },
    [iframeRef, autoHeight, onSendPrompt],
  );

  useEffect(() => {
    window.addEventListener('message', handleMessage);
    return () => window.removeEventListener('message', handleMessage);
  }, [handleMessage]);

  // Re-push theme vars whenever the app toggles data-theme.
  const pushThemeRef = useRef(pushTheme);
  pushThemeRef.current = pushTheme;
  useEffect(() => {
    const observer = new MutationObserver(() => pushThemeRef.current());
    observer.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ['data-theme'],
    });
    return () => observer.disconnect();
  }, []);

  return { height, pushTheme };
}
