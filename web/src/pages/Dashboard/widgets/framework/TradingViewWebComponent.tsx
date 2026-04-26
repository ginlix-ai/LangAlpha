import { useEffect, useRef, useState, useCallback } from 'react';
import { useTranslation } from 'react-i18next';
import { useTheme } from '@/contexts/ThemeContext';
import { EmbedFallback } from './EmbedFallback';
import { mapLocaleForTV } from './tvConfig';

/**
 * Per-element+locale module loader cache. Each TradingView web-component lives
 * at a per-name + per-locale module URL (e.g.
 * `widgets.tradingview-widget.com/w/zh_CN/tv-economic-map.js`). The bundle
 * embeds chrome strings for that locale, so loading `/w/en/` once locks the
 * element class to English regardless of any later `locale` attribute —
 * `customElements.define()` is one-shot per element name per page. We key
 * the cache by `${locale}:${elementName}` so a session that starts in zh-CN
 * fetches the right bundle on first paint. Switching locale mid-session
 * cannot re-register the element class on the same page (browser limitation);
 * a full reload is required for chrome strings to swap. We still update the
 * `locale` attribute on the element in case TV's runtime honors it for any
 * dynamic copy.
 *
 * IMPORTANT — host: it's `widgets.tradingview-widget.com`, NOT
 * `www.tradingview-widget.com`. The www host returns 403 for these paths.
 * The widgets subdomain serves them from S3 with proper CORS.
 */
const loaderPromises = new Map<string, Promise<void>>();

const TV_WC_HOST = 'https://widgets.tradingview-widget.com/w/';

/**
 * Once-per-page setup: load the `<tv-custom-settings>` element and stamp it
 * into the document with a `symbol-url` template that routes every symbol
 * click inside any TV widget to our `/market?symbol=…` route. This is the
 * crown-jewel of the web-components migration — keeps users in our funnel
 * instead of bouncing to tradingview.com when they click a heatmap cell or
 * a ticker.
 *
 * `{tvsymbol}` is TV's documented placeholder token. The element only needs
 * to exist once anywhere in the document; subsequent TV widgets pick up the
 * setting on registration.
 */
let customSettingsInstalled = false;
// `locale` only takes effect on the FIRST call. Once `<tv-custom-settings>` is
// registered, the singleton flag short-circuits subsequent invocations — so a
// mid-session locale switch can't re-register it in the new bundle. This
// matches the broader one-shot `customElements.define()` browser limitation
// documented at the top of this file: full reload is required to pick up a
// different locale for already-registered TV elements.
function ensureCustomSettings(locale: string): void {
  if (customSettingsInstalled || typeof window === 'undefined') return;
  customSettingsInstalled = true;
  ensureLoaded('tv-custom-settings', locale).then(
    () => {
      if (document.querySelector('tv-custom-settings')) return;
      const el = document.createElement('tv-custom-settings');
      el.setAttribute('symbol-url', `${window.location.origin}/market?symbol={tvsymbol}`);
      document.body.appendChild(el);
    },
    () => {
      // ad-blocker or CDN failure — accept silently; no-op.
    },
  );
}

function ensureLoaded(elementName: string, locale: string): Promise<void> {
  const cacheKey = `${locale}:${elementName}`;
  const cached = loaderPromises.get(cacheKey);
  if (cached) return cached;

  const src = `${TV_WC_HOST}${locale}/${elementName}.js`;

  const promise = (async () => {
    if (typeof window === 'undefined') {
      throw new Error('[TradingViewWebComponent] no window');
    }
    if (window.customElements?.get(elementName)) {
      return;
    }

    try {
      // Use dynamic `import()` — the pattern the TV docs document in their
      // "dynamic import" example. Browsers handle CORS, MIME-type checks,
      // and module-cache dedup for us. `@vite-ignore` keeps Vite from
      // trying to pre-bundle this external URL at build time.
      await import(/* @vite-ignore */ src);
    } catch (e) {
      console.error(`[TradingViewWebComponent] import failed for ${src}`, e);
      loaderPromises.delete(cacheKey);
      throw e;
    }

    // After import resolves, the module has executed — but TV's modules may
    // call `customElements.define` asynchronously inside that execution.
    // `whenDefined` resolves as soon as the registration happens.
    await window.customElements.whenDefined(elementName);
  })();

  loaderPromises.set(cacheKey, promise);
  return promise;
}

/** camelCase → kebab-case AND snake_case → kebab-case. Iframe-era TV configs
 * mix both naming styles (`displayMode`, `support_host`, `hide_top_toolbar`),
 * and HTML attributes only accept hyphens. */
function toAttr(key: string): string {
  return key.replace(/([A-Z])/g, '-$1').replace(/_/g, '-').toLowerCase();
}

function toAttrValue(v: unknown): string {
  if (v == null) return '';
  if (typeof v === 'string') return v;
  if (typeof v === 'number') return String(v);
  // Booleans use HTML's presence semantics — `true` = empty string (attribute
  // is present), `false` is filtered upstream so we don't render it. Stringifying
  // `"false"` here would set the attribute, which TV reads as truthy.
  if (typeof v === 'boolean') return '';
  return JSON.stringify(v);
}

interface Props {
  /** Web-component element name (e.g. `tv-ticker-tape`). */
  element: string;
  /** Per-widget config; keys converted to kebab-case attributes. */
  config: Record<string, unknown>;
  className?: string;
  card?: boolean;
}

/**
 * Shared host for TradingView web-component widgets.
 *
 * Lifecycle:
 *   1. Mount: triggers loader for the element's module script (deduped).
 *   2. On `ready`, the inner element is rendered with the config as
 *      kebab-case attributes plus a reactive `theme` attribute.
 *   3. Config or theme changes update attributes IN PLACE — TV web
 *      components react to attribute changes without remount, so theme
 *      flips and config edits don't rebuild the iframe inside.
 *   4. 15s timeout watches for `customElements.get(element)` to register;
 *      if missing (ad-blocker, CDN failure), flips to error with Retry.
 */
export function TradingViewWebComponent({
  element,
  config,
  className,
  card = false,
}: Props) {
  const { theme } = useTheme();
  const { i18n } = useTranslation();
  const hostRef = useRef<HTMLDivElement>(null);
  const elRef = useRef<HTMLElement | null>(null);
  const [status, setStatus] = useState<'loading' | 'ready' | 'error'>('loading');
  const [retryToken, setRetryToken] = useState(0);

  // Stable JSON-encoded config. Widget definitions pass inline object
  // literals (`config={{ ... }}`), so the raw reference changes on every
  // parent render even when content is identical. Using the serialized
  // form as the effect key skips redundant setAttribute loops when
  // neighbor widgets edit, the grid layout shifts, or edit-mode toggles.
  const configKey = JSON.stringify(config);

  const tvLocale = mapLocaleForTV(i18n.language);

  const retry = useCallback(() => {
    loaderPromises.delete(`${tvLocale}:${element}`);
    setStatus('loading');
    setRetryToken((n) => n + 1);
  }, [element, tvLocale]);

  // Loader: drives status transitions. Re-runs on element change, locale
  // change, or retry. Note: mid-session locale switch triggers a fresh fetch
  // for the locale-keyed bundle, but `customElements.define()` is one-shot
  // per element name — the element class registered by the first-loaded
  // bundle stays bound to that locale's chrome strings. A page reload is
  // required for chrome to fully re-localize.
  useEffect(() => {
    // Reset to 'loading' when the element prop changes mid-mount. Without
    // this, status stays 'ready' from the prior element, the mount effect
    // races ahead and calls `document.createElement(newElement)` against a
    // tag whose module hasn't loaded yet — producing an HTMLUnknownElement
    // with leaked attributes.
    setStatus('loading');
    ensureCustomSettings(tvLocale);
    let cancelled = false;
    let timeoutId: number | null = window.setTimeout(() => {
      if (cancelled) return;
      if (!window.customElements?.get(element)) {
        setStatus('error');
      }
    }, 15_000);

    ensureLoaded(element, tvLocale).then(
      () => {
        if (cancelled) return;
        if (timeoutId !== null) {
          window.clearTimeout(timeoutId);
          timeoutId = null;
        }
        setStatus('ready');
      },
      (err) => {
        if (cancelled) return;
        console.error('[TradingViewWebComponent]', err);
        if (timeoutId !== null) {
          window.clearTimeout(timeoutId);
          timeoutId = null;
        }
        setStatus('error');
      },
    );

    return () => {
      cancelled = true;
      if (timeoutId !== null) window.clearTimeout(timeoutId);
    };
  }, [element, tvLocale, retryToken]);

  // Mount: when ready, create the custom element imperatively and append to
  // the host ref. Imperative because React's JSX doesn't ship attributes
  // that need exact kebab-case — JSX would camelCase or hyphenate
  // inconsistently depending on the prop name. Imperative `setAttribute` is
  // unambiguous.
  useEffect(() => {
    if (status !== 'ready') return;
    const host = hostRef.current;
    if (!host) return;

    if (!elRef.current) {
      const el = document.createElement(element);
      host.appendChild(el);
      elRef.current = el;
    }
    const el = elRef.current;

    const wantedAttrs = new Set<string>();
    for (const [k, v] of Object.entries(config)) {
      // Skip `false` booleans entirely. HTML boolean attrs use presence
      // semantics (`hide-chart` present = true). Setting `hide-chart="false"`
      // is still "present" → TV reads it as truthy. Caller passes `true` to
      // enable, omits or passes `false` to disable.
      // Also skip null/undefined — toAttrValue would emit '' which TV reads
      // as truthy too, so an `{ hideLegend: undefined }` spread accidentally
      // enables hide-legend. Same presence trap as the false branch.
      if (v === false || v == null) continue;
      const name = toAttr(k);
      wantedAttrs.add(name);
      el.setAttribute(name, toAttrValue(v));
    }
    // Theme is reactive and lives outside `config` so dashboard widgets
    // don't need to thread it through every call site.
    el.setAttribute('theme', theme);
    wantedAttrs.add('theme');
    el.setAttribute('color-theme', theme);
    wantedAttrs.add('color-theme');
    el.setAttribute('locale', tvLocale);
    wantedAttrs.add('locale');

    // Strip stale attributes from prior config so attribute state mirrors
    // the current config object exactly.
    for (const attr of Array.from(el.attributes)) {
      if (!wantedAttrs.has(attr.name)) {
        el.removeAttribute(attr.name);
      }
    }
    // `config` is read from closure but intentionally not in deps —
    // `configKey` (the stable JSON string) tracks content changes without
    // tripping on reference churn from inline object literals.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [status, element, configKey, theme, tvLocale]);

  // Teardown on unmount or element change: detach the custom element so it
  // doesn't leak its iframe into the next mount.
  useEffect(() => {
    return () => {
      if (elRef.current) {
        elRef.current.remove();
        elRef.current = null;
      }
    };
  }, [element]);

  const cardClass = card ? 'dashboard-glass-card p-3 overflow-hidden' : '';
  return (
    <div className={`tv-embed-host flex flex-col h-full ${cardClass} ${className ?? ''}`}>
      <div className="relative flex-1 min-h-0">
        <div
          ref={hostRef}
          className="tv-wc-container w-full h-full"
          style={{ minHeight: 80 }}
        />
        {status === 'error' && <EmbedFallback onRetry={retry} />}
      </div>
    </div>
  );
}

