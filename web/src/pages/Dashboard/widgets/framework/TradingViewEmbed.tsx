import { useCallback, useEffect, useRef, useState } from 'react';
import { useTheme } from '@/contexts/ThemeContext';
import { TV_COMMON_CONFIG, resolveScriptSrc } from './tvConfig';
import { EmbedFallback } from './EmbedFallback';

interface Props {
  /** Either a known short key (`ticker-tape`) or a full embed-widget URL. */
  scriptKey: string;
  /** Per-widget config merged on top of TV_COMMON_CONFIG + theme. */
  config: Record<string, unknown>;
  /** Class applied to the outer container. */
  className?: string;
  /**
   * When true, wrap the embed in the dashboard's `dashboard-glass-card` shell
   * so TV widgets visually match our native widgets (background, border,
   * rounded corners, padding). Off by default so MarketView's bare usage is
   * unaffected.
   */
  card?: boolean;
  /**
   * Fixed pixel height for the iframe area. When set, the host stops using
   * `h-full` and sizes to content, so the widget can pair with
   * `fitToContent: true` and the cell tracks chrome (header + body padding)
   * between edit/view modes instead of clipping the iframe. Use for embeds
   * with a well-defined natural height (e.g. TickerTape ~76px).
   */
  contentHeight?: number;
}

/**
 * Shared host for TradingView embed widgets.
 *
 * Lifecycle:
 *   1. Mount creates the container DIV; effect appends a fresh <script>
 *      with the JSON config inline. TV's embed script reads its own DOM
 *      sibling for config, builds the iframe.
 *   2. On config / theme change, the effect destroys the previous iframe
 *      and rebuilds with the new payload.
 *   3. A 10s timeout watches for the iframe child to appear; if missing
 *      (ad-blocker, CSP, network), the card flips to the error state with
 *      a Retry button.
 *
 * The drag-cancel behavior in WidgetFrame.css keeps the iframe inert
 * during edit mode so RGL drag works without per-widget plumbing.
 */
// First rebuild fires immediately so the user sees the iframe right away.
// Subsequent dep changes (config edits in the settings dialog, theme toggles
// affecting all 17 trader-tv tiles at once) coalesce to a single rebuild
// after this much idle time. Keeps typing in a SymbolField from rebuilding
// the iframe on every keystroke and prevents a theme-toggle storm against
// the TradingView CDN.
const REBUILD_DEBOUNCE_MS = 200;

export function TradingViewEmbed({ scriptKey, config, className, card = false, contentHeight }: Props) {
  const { theme } = useTheme();
  const containerRef = useRef<HTMLDivElement>(null);
  const [status, setStatus] = useState<'loading' | 'ready' | 'error'>('loading');
  const [retryToken, setRetryToken] = useState(0);
  const builtOnceRef = useRef(false);
  // Mirror of `status` for use inside the rebuild effect without adding it
  // to the dep array — the effect would otherwise re-run on every status
  // flip (including loading→ready), causing a rebuild loop.
  const statusRef = useRef(status);
  useEffect(() => {
    statusRef.current = status;
  }, [status]);

  // Stable JSON of config. Effect dependency uses the string so reference
  // changes from inline object literals don't trigger spurious rebuilds.
  const configKey = JSON.stringify(config);

  const retry = useCallback(() => {
    setStatus('loading');
    setRetryToken((n) => n + 1);
  }, []);

  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;
    // Clear a stuck error overlay when a real config/theme change triggers
    // a rebuild. Skip when healthy so theme toggles across 17 tiles don't
    // flash the loading skeleton.
    if (statusRef.current === 'error') setStatus('loading');

    let timeoutId: number | null = null;
    let buildTimer: number | null = null;
    let observer: MutationObserver | null = null;
    let cancelled = false;

    const build = () => {
      if (cancelled) return;
      builtOnceRef.current = true;

      container.innerHTML = '';

      const widgetDiv = document.createElement('div');
      widgetDiv.className = 'tradingview-widget-container__widget';
      widgetDiv.style.height = '100%';
      widgetDiv.style.width = '100%';
      container.appendChild(widgetDiv);

      const script = document.createElement('script');
      let src: string;
      try {
        src = resolveScriptSrc(scriptKey);
      } catch (e) {
        console.error('[TradingViewEmbed]', e);
        setStatus('error');
        return;
      }
      script.src = src;
      script.type = 'text/javascript';
      script.async = true;
      // Escape `<` to `<` so a user-controlled symbol like
      // `</script><script>…` can't break out of the embedded JSON and execute.
      // JSON.stringify allows raw `<` because it's valid JSON, but the browser
      // parses script bodies with HTML rules and would close the script tag.
      script.text = JSON.stringify({
        ...TV_COMMON_CONFIG,
        ...config,
        colorTheme: theme,
        theme: theme,
      }).replace(/</g, '\\u003c');
      script.onerror = () => {
        if (cancelled) return;
        setStatus('error');
      };
      container.appendChild(script);

      // TV embeds inject an <iframe> child after the script executes. Watch
      // for it; if none appears within 10s, flip to error (ad-blocker most
      // commonly causes this). Observer stays alive past the timeout — slow
      // CDN/mobile loads where the iframe arrives at 10.5s shouldn't leave
      // the user stuck on the fallback. Late arrival flips status back to ready.
      observer = new MutationObserver(() => {
        if (cancelled) return;
        if (container.querySelector('iframe')) {
          setStatus('ready');
          if (timeoutId !== null) {
            window.clearTimeout(timeoutId);
            timeoutId = null;
          }
        }
      });
      observer.observe(container, { childList: true, subtree: true });

      timeoutId = window.setTimeout(() => {
        if (cancelled) return;
        if (!container.querySelector('iframe')) {
          setStatus('error');
          // Do NOT disconnect the observer — keep watching so a late iframe
          // (slow CDN, mobile, network blip) can recover the widget instead
          // of leaving the user staring at a permanent fallback.
        }
      }, 10_000);
    };

    if (builtOnceRef.current) {
      // Subsequent dep change: defer so rapid keystrokes / theme storms
      // coalesce into one rebuild. Cleanup cancels in-flight schedules.
      buildTimer = window.setTimeout(build, REBUILD_DEBOUNCE_MS);
    } else {
      // First mount: build synchronously so the iframe is up by the next
      // paint and tests can assert against the script tag without advancing
      // timers.
      build();
    }

    return () => {
      cancelled = true;
      if (buildTimer !== null) window.clearTimeout(buildTimer);
      if (timeoutId !== null) window.clearTimeout(timeoutId);
      observer?.disconnect();
      if (container) container.innerHTML = '';
    };
  // Intentionally NOT including `config` in deps — `configKey` (the stable
  // JSON string) already tracks content changes. Including the raw object
  // would re-trigger the effect on every parent render (each widget passes
  // an inline `config={{...}}`), rebuilding the iframe and flashing the
  // embed on theme toggle / edit-mode flip / neighbor widget edits.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [scriptKey, configKey, theme, retryToken]);

  const cardClass = card ? 'dashboard-glass-card p-3 overflow-hidden' : '';
  const isFixed = typeof contentHeight === 'number';
  return (
    <div className={`tv-embed-host flex flex-col h-full ${cardClass} ${className ?? ''}`}>
      <div
        className={
          isFixed
            ? 'relative flex-1 min-h-0 flex items-center justify-center'
            : 'relative flex-1 min-h-0'
        }
      >
        <div
          ref={containerRef}
          className="tv-embed-container tradingview-widget-container w-full"
          style={
            isFixed
              ? { height: contentHeight, flex: 'none' }
              : { minHeight: 80, height: '100%' }
          }
        />
        {status === 'error' && (
          <EmbedFallback onRetry={retry} />
        )}
      </div>
    </div>
  );
}

