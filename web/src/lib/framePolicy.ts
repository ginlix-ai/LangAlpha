// Where the app's frames may load, as CSP `frame-src`. A frame's navigations,
// its own included, are checked against its parent's frame-src, so this is what
// keeps a tampered report from sending itself to another site with what it read.
//
// Dependency-free: scripts/vite-plugins/framePolicy.ts builds index.html's
// policy from it, and the app reads the same answer when it decides how to
// frame a URL.

/**
 * Documents the app frames from third parties. TradingView's embed scripts
 * build their iframe on the widget host and fall back to the other when the
 * first is refused.
 */
export const EMBED_FRAME_ORIGINS = [
  'https://www.tradingview-widget.com',
  'https://s.tradingview.com',
] as const;

/**
 * Links a frame hands to the device's mail or phone app. Following one loads
 * no document and sends nothing until the person acts in that app, so they
 * stay open; without them a page's contact link ends the frame in an error
 * page.
 */
export const HANDOFF_SCHEMES = ['mailto:', 'tel:'] as const;

// One origin, or a `*.` host wildcard, never a bare web scheme: `https:` would
// let a frame go anywhere.
const SOURCE = /^https?:\/\/(\*\.)?[a-z0-9-]+(\.[a-z0-9-]+)*(:(\d{1,5}|\*))?$/;
const HANDOFF: ReadonlySet<string> = new Set(HANDOFF_SCHEMES);

/** `scheme://host[:port]` of an http(s) URL, or null for anything else. */
export function originOf(url: string | null | undefined, base?: string): string | null {
  if (!url) return null;
  let parsed: URL;
  try {
    parsed = base ? new URL(url, base) : new URL(url);
  } catch {
    return null;
  }
  if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') return null;
  return SOURCE.test(parsed.origin) ? parsed.origin : null;
}

function directive(sources: readonly string[]): string {
  for (const source of sources) {
    if (source !== "'self'" && !HANDOFF.has(source) && !SOURCE.test(source)) {
      throw new Error(`frame-src source must be an origin, got ${JSON.stringify(source)}`);
    }
  }
  return `frame-src ${[...new Set(sources)].join(' ')}`;
}

/**
 * The app document's policy. `'self'` is the frame host and the API routes on
 * the app's own origin; an absolute API base adds that origin. Nothing known
 * only at runtime belongs here: a meta policy can be added to but never
 * widened, so a frame on such an origin goes through the frame host instead.
 */
export function appFrameSrc(apiBase?: string | null): string {
  const api = apiBase && /^https?:\/\//i.test(apiBase) ? originOf(apiBase) : null;
  return directive(["'self'", ...(api ? [api] : []), ...EMBED_FRAME_ORIGINS, ...HANDOFF_SCHEMES]);
}

/** A frame host's policy: the one origin it was set up to frame. */
export function hostedFrameSrc(src: string): string | null {
  const origin = originOf(src);
  return origin ? directive([origin, ...HANDOFF_SCHEMES]) : null;
}
