// Starts the active locale's catalog download from index.html, alongside the
// entry instead of after it.
//
// Each locale is its own chunk (src/i18n.ts) and nothing renders until the
// active one is in, so left to the entry the request would wait a full
// waterfall step. The build writes the hashed names into an inline script that
// picks the locale with the same resolveLocale() the app boots with and appends
// a preload. It replaces index.html's `<!-- locale-preload -->` marker: after
// the stale-build script, whose listener must exist before this preload can fail
// (a 404 then reloads like any dead asset), and before any stylesheet, which
// would hold an inline script until it loaded.
//
// Unlike the stale-build script this one may use modern syntax: the entry is an
// ES module, so a browser that cannot parse it never runs the app either.

import fs from 'node:fs';
import path from 'node:path';
import { minifySync, type Plugin } from 'vite';
import { DEFAULT_LOCALE, resolveLocale } from '../src/lib/resolveLocale.ts';

const MARKER = /<!--\s*locale-preload\b[^>]*-->/;

/** The locales that have a catalog; the order is the one the resolver sees. */
export function catalogLocales(dir: string): string[] {
  return fs
    .readdirSync(dir)
    .filter((f) => f.endsWith('.json'))
    .map((f) => path.basename(f, '.json'))
    .sort();
}

/**
 * How the inline script spells a locale's chunk, one capture group each for the
 * locale and the file. scripts/check-critical-path.mjs reads the catalogs back
 * out of index.html with it.
 */
export const CATALOG_URL = /"([A-Za-z]{2,3}-[A-Za-z0-9]+)":"[^"]*\/assets\/([^"]+\.js)"/g;

// Stands in for the JSON map while the script is minified, so the map keeps
// the double-quoted shape CATALOG_URL reads.
const URLS = '__LOCALE_URLS__';

/** `urls` maps each locale to its chunk, in catalogLocales() order. */
export function localePreloadScript(urls: Record<string, string>): string {
  // Vite's own preload shape, fallback included: rel="preload" where
  // modulepreload is unsupported, as="script" either way. Both are shapes the
  // recovery listener classifies as a build asset.
  const { code } = minifySync(
    'locale-preload.js',
    '(function(){' +
      `var u=${URLS},k=document.createElement("link");` +
      'try{k.rel=k.relList.supports("modulepreload")?"modulepreload":"preload"}catch(e){k.rel="preload"}' +
      'k.as="script";k.crossOrigin="";' +
      `k.href=u[(${resolveLocale})(document.cookie,navigator.language||"",Object.keys(u),${JSON.stringify(DEFAULT_LOCALE)})];` +
      'document.head.appendChild(k)' +
      '})();',
  );
  return code.trim().replace(URLS, () => JSON.stringify(urls));
}

/** Build-only: the dev server has no hashed chunks to name. */
export function localePreload(localesDir: string): Plugin {
  let base = '/';
  return {
    name: 'la-locale-preload',
    apply: 'build',
    configResolved(config) {
      base = config.base;
    },
    transformIndexHtml: {
      order: 'post',
      handler(html, ctx) {
        const dir = fs.realpathSync(localesDir);
        const found = new Map<string, string>();
        for (const out of Object.values(ctx.bundle ?? {})) {
          const id = out.type === 'chunk' ? out.facadeModuleId : null;
          if (id && path.dirname(id) === dir && id.endsWith('.json')) {
            found.set(path.basename(id, '.json'), base + out.fileName);
          }
        }
        // A catalog without a chunk of its own was imported statically
        // somewhere, which puts it back in the entry.
        const locales = catalogLocales(dir);
        const missing = locales.filter((l) => !found.has(l));
        if (missing.length) {
          throw new Error(`locale-preload: no chunk of its own for ${missing.join(', ')}; is it imported statically?`);
        }
        if (!found.has(DEFAULT_LOCALE)) {
          throw new Error(`locale-preload: no catalog for the default locale ${DEFAULT_LOCALE}`);
        }
        if (!MARKER.test(html)) {
          throw new Error('locale-preload: index.html has no <!-- locale-preload --> marker');
        }
        const urls = Object.fromEntries(locales.map((l) => [l, found.get(l)!]));
        return html.replace(MARKER, () => `<script>${localePreloadScript(urls)}</script>`);
      },
    },
  };
}
