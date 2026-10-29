import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { createRequire } from 'node:module';
import { afterEach, describe, expect, it } from 'vitest';
import { catalogLocales, localePreloadScript } from '../../../scripts/locale-preload';
import { SUPPORTED_LOCALES, detectLocale } from '../locale';

// jsdom ships no type declarations and @types/jsdom is not a dependency here;
// see staleBuildPreBoot.test.ts.
const { JSDOM } = createRequire(import.meta.url)('jsdom') as {
  JSDOM: new (
    html: string,
    options: { url: string; runScripts: 'outside-only' },
  ) => { window: unknown };
};

// The build serialises resolveLocale() into an inline script in index.html that
// preloads the catalog before the bundle runs. The script is a string that never
// reaches tsc, so it is run here, as the browser gets it, against detectLocale().
// A disagreement costs a zh-CN visitor a wasted download plus a waterfall on the
// catalog it actually needs.

type Win = Window & typeof globalThis & { eval: (s: string) => void };

const ORIGIN = window.location.origin;
const html = readFileSync(resolve(__dirname, '../../../index.html'), 'utf8');
const recovery = [...html.matchAll(/<script>([\s\S]*?)<\/script>/g)]
  .map((m) => m[1])
  .find((s) => s.includes('__LA_BOOTED__'));

const locales = catalogLocales(resolve(__dirname, '../../locales'));
const urls = Object.fromEntries(locales.map((l) => [l, `/assets/${l}-1a2b3c4d.js`]));
const script = localePreloadScript(urls);

function preload(cookies: string[], language: string, withRecovery = false) {
  const dom = new JSDOM('<!doctype html><html><head></head><body><div id="root"></div></body></html>', {
    url: `${ORIGIN}/dashboard`,
    runScripts: 'outside-only',
  });
  const win = dom.window as Win;
  for (const c of cookies) win.document.cookie = c;
  Object.defineProperty(win.navigator, 'language', { value: language, configurable: true });
  let reloads = 0;
  (win as unknown as { setTimeout: unknown }).setTimeout = () => ++reloads;
  if (withRecovery) win.eval(recovery!);
  win.eval(script);
  const links = win.document.head.querySelectorAll('link');
  expect(links).toHaveLength(1);
  return { win, link: links[0], reloads: () => reloads };
}

function detectInApp(cookies: string[], language: string): string {
  for (const c of cookies) document.cookie = c;
  Object.defineProperty(navigator, 'language', { value: language, configurable: true });
  return detectLocale();
}

afterEach(() => {
  for (const c of document.cookie.split(';')) {
    const name = c.split('=')[0].trim();
    if (name) document.cookie = `${name}=; expires=Thu, 01 Jan 1970 00:00:00 GMT`;
  }
  delete (navigator as { language?: string }).language;
});

it('has a catalog for exactly the supported locales', () => {
  expect(locales).toEqual([...SUPPORTED_LOCALES].sort());
});

describe('it preloads the catalog detectLocale() will ask for', () => {
  const languageOnly = [...new Set(SUPPORTED_LOCALES.map((l) => l.split('-')[0]))];
  const cases: Array<[string[], string]> = [
    ...SUPPORTED_LOCALES.map((l): [string[], string] => [[], l]),
    ...languageOnly.map((l): [string[], string] => [[], l]),
    [[], 'zh-TW'],
    [[], 'zh-Hans-CN'],
    [[], 'en-GB'],
    [[], 'en-us'],
    [[], 'fr-FR'],
    [[], ''],
    ...SUPPORTED_LOCALES.map((l): [string[], string] => [[`locale=${l}`], 'fr-FR']),
    [['locale=zh-CN'], 'en-US'],
    [['locale=en-US'], 'zh-CN'],
    [['other=1', 'locale=zh-CN'], 'en-US'],
    [['xlocale=zh-CN'], 'en-US'],
    [['locale=zh%2DCN'], 'en-US'],
    [['locale=fr-FR'], 'zh-CN'],
    [['locale=%E0%A4%A'], 'zh-CN'],
    [['locale=constructor'], 'zh-CN'],
    [['locale=__proto__'], 'en-US'],
  ];

  it.each(cases)('cookies %j, browser %j', (cookies, language) => {
    const { link } = preload(cookies, language);
    expect(link.getAttribute('href')).toBe(urls[detectInApp(cookies, language)]);
  });
});

describe('a catalog that fails to preload is a dead build asset', () => {
  // The link is how every browser reports the failure before boot. Safari's
  // import error names no URL, so this is the only signal it gives.
  it('the recovery script schedules its reload', () => {
    const { win, link, reloads } = preload([], 'zh-CN', true);
    link.dispatchEvent(new win.Event('error'));
    expect(reloads()).toBe(1);
  });

  it('sits after the recovery script, whose listener has to exist first, and before any stylesheet', () => {
    const marker = html.search(/<!--\s*locale-preload\b/);
    expect(marker).toBeGreaterThan(html.indexOf('__LA_BOOTED__'));
    expect(marker).toBeLessThan(html.search(/<link\b[^>]*rel="stylesheet"/));
  });
});
