import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { createRequire } from 'node:module';
import { describe, it, expect } from 'vitest';
import { isStaleBuildError, __resetStaleBuildForTests } from '../staleBuild';

// jsdom ships no type declarations and @types/jsdom is not a dependency here.
// The surface this file uses is one constructor wide, so declare that rather
// than add a types package for a single test.
const { JSDOM } = createRequire(import.meta.url)('jsdom') as {
  JSDOM: new (
    html: string,
    options: { url: string; runScripts: 'outside-only' },
  ) => { window: unknown };
};

// The pre-boot half of stale-build recovery, exercised as the browser actually
// gets it: the literal <script> text out of index.html, evaluated in its own
// document. It is inline and un-importable by construction — the bundle a test
// would import from is the thing that failed — so lifting the source out of the
// HTML is the only way to reach it at all.
//
// It carries the two guards with the worst failure modes in the feature. The
// attempt bound is all that stands between a persistent /assets/ 404 and every
// open tab reloading against a broken origin forever, and the __LA_BOOTED__
// gate is all that stops a reload from discarding an agent turn that has been
// streaming for minutes. Neither is reachable from the module tests.

// The two halves are compared against the same messages below, and both apply a
// same-origin test — so the document the IIFE runs in has to share an origin
// with the one vitest gives the module, or the comparison is meaningless.
const ORIGIN = window.location.origin;
const KEY = '__la_asset_recovery__';

const source = readFileSync(resolve(__dirname, '../../../index.html'), 'utf8');
const iife = [...source.matchAll(/<script>([\s\S]*?)<\/script>/g)]
  .map((m) => m[1])
  .find((s) => s.includes('__LA_BOOTED__'));

interface Harness {
  win: Window & typeof globalThis & { __LA_STALE_BUILD__?: string; __LA_BOOTED__?: boolean };
  reloadsScheduled: () => number;
}

function boot({
  booted = false,
  stamp,
  rawStamp,
}: { booted?: boolean; stamp?: unknown; rawStamp?: string } = {}): Harness {
  const dom = new JSDOM('<!doctype html><html><body><div id="root"></div></body></html>', {
    url: `${ORIGIN}/dashboard`,
    runScripts: 'outside-only',
  });
  const win = dom.window as unknown as Harness['win'];
  let scheduled = 0;
  // The reload is jittered through setTimeout, so counting the scheduling is
  // equivalent and sidesteps jsdom's non-configurable location.reload.
  (win as unknown as { setTimeout: unknown }).setTimeout = () => {
    scheduled++;
    return 0;
  };
  // rawStamp writes the string as stored, which JSON.stringify cannot express:
  // it turns Infinity into null, so the value that actually reaches the parser
  // in the wild (`1e999`) is unreachable through `stamp`.
  if (rawStamp !== undefined) win.sessionStorage.setItem(KEY, rawStamp);
  else if (stamp !== undefined) win.sessionStorage.setItem(KEY, JSON.stringify(stamp));
  if (booted) win.__LA_BOOTED__ = true;
  (win as unknown as { eval: (s: string) => void }).eval(iife!);
  return { win, reloadsScheduled: () => scheduled };
}

function firePreloadError(win: Harness['win'], message: string): void {
  const e = new win.Event('vite:preloadError') as Event & { payload?: Error };
  e.payload = new win.Error(message) as Error;
  win.dispatchEvent(e);
}

/**
 * A resource load error, dispatched the way the browser delivers one: at the
 * element, not bubbling. The capture phase still walks Window down to the
 * target, which is the only reason a listener on window sees it at all.
 */
function fireResourceError(win: Harness['win'], el: Element): void {
  win.document.head.appendChild(el);
  el.dispatchEvent(new win.Event('error'));
}

function buildScript(win: Harness['win'], src: string): Element {
  const el = win.document.createElement('script');
  el.setAttribute('src', src);
  return el;
}

/**
 * The two shapes Vite writes a JS preload in. `runtime` is what __vitePreload
 * appends for a lazy chunk (verified against the shipped helper: `u ||
 * (h.as = "script")`), and `document` is what the build bakes into index.html
 * for the entry's vendor chunks — which carries no `as` at all.
 */
function preloadLink(win: Harness['win'], href: string, shape: 'runtime' | 'document'): Element {
  const el = win.document.createElement('link');
  el.setAttribute('rel', 'modulepreload');
  if (shape === 'runtime') el.setAttribute('as', 'script');
  el.setAttribute('href', href);
  return el;
}

const deadChunk = `Failed to fetch dynamically imported module: ${ORIGIN}/assets/a-11111111.js`;

it('is present in index.html at all', () => {
  expect(iife, 'recovery IIFE not found — it must stay inline in <head>').toBeTruthy();
});

describe('it classifies exactly what the module half classifies', () => {
  // Two implementations of one rule, in two languages, that can never import
  // each other. This is the only place they are run against the same inputs.
  const cases: Array<[string, boolean]> = [
    [deadChunk, true],
    ['Unable to preload CSS for /assets/Chat-a1b2.css', true],
    ['Unable to preload CSS for dist/assets/x.css', false],
    ['Unable to preload CSS for https://cdn.example.com/assets/x.css', false],
  ];

  it.each(cases)('%s', (message, expected) => {
    const { win } = boot({ booted: true });
    firePreloadError(win, message);
    expect(!!win.__LA_STALE_BUILD__).toBe(expected);

    __resetStaleBuildForTests();
    expect(isStaleBuildError(new Error(message))).toBe(expected);
  });

  it('ignores a preloadError carrying no message', () => {
    const { win } = boot({ booted: true });
    firePreloadError(win, '');
    expect(win.__LA_STALE_BUILD__).toBeUndefined();
  });
});

describe('it recognises a dead build asset by the element that failed', () => {
  // The other half of classification, and the half no message ever reaches:
  // a resource error carries an element, not a string.
  it('classifies a dead module script', () => {
    const { win } = boot({ booted: true });
    fireResourceError(win, buildScript(win, `${ORIGIN}/assets/index-1111.js`));
    expect(win.__LA_STALE_BUILD__).toBe('resource');
  });

  it('classifies the runtime preload link Vite appends for a lazy chunk', () => {
    // Safari's whole story. Its import error names no URL, so both message
    // classifiers decline it, and the chunk never becomes a <script> — this
    // link is the only evidence that exists. Without it the boundary rethrows
    // and the pane goes blank, which is the failure the feature exists to fix.
    const { win } = boot({ booted: true });
    fireResourceError(win, preloadLink(win, `${ORIGIN}/assets/MarketView-1111.js`, 'runtime'));
    expect(win.__LA_STALE_BUILD__).toBe('resource');
  });

  it("classifies the document's own baked-in vendor preload, which has no as", () => {
    // Same rel, different shape. Matching only on as="script" reads this one as
    // somebody else's asset.
    const { win } = boot({ booted: true });
    fireResourceError(win, preloadLink(win, `${ORIGIN}/assets/vendor-react-1111.js`, 'document'));
    expect(win.__LA_STALE_BUILD__).toBe('resource');
  });

  it('ignores build assets that are not scripts', () => {
    // /assets/ also holds CSS, fonts and images. One of those blipping would
    // otherwise reload the page under a user whose build is perfectly current.
    const { win } = boot({ booted: true });

    const img = win.document.createElement('img');
    img.setAttribute('src', `${ORIGIN}/assets/logo-1111.png`);
    fireResourceError(win, img);

    const css = win.document.createElement('link');
    css.setAttribute('rel', 'stylesheet');
    css.setAttribute('href', `${ORIGIN}/assets/index-1111.css`);
    fireResourceError(win, css);

    // A preload that is not a script: same rel Vite falls back to, other `as`.
    const font = win.document.createElement('link');
    font.setAttribute('rel', 'preload');
    font.setAttribute('as', 'font');
    font.setAttribute('href', `${ORIGIN}/assets/Geist-1111.woff2`);
    fireResourceError(win, font);

    expect(win.__LA_STALE_BUILD__).toBeUndefined();
  });

  it('ignores a script from another origin', () => {
    const { win } = boot({ booted: true });
    fireResourceError(win, buildScript(win, 'https://cdn.example.com/assets/index-1111.js'));
    expect(win.__LA_STALE_BUILD__).toBeUndefined();
  });
});

describe('once the app has booted it never reloads', () => {
  it('publishes the reason instead of reloading', () => {
    const { win, reloadsScheduled } = boot({ booted: true });
    let detail: string | null = null;
    win.addEventListener('la:stale-build', ((e: CustomEvent<string>) => {
      detail = e.detail;
    }) as EventListener);

    firePreloadError(win, deadChunk);

    expect(detail).toBe('preload');
    expect(reloadsScheduled()).toBe(0);
  });
});

describe('before boot it reloads, but a bounded number of times', () => {
  it('records the first attempt and schedules one reload', () => {
    const { win, reloadsScheduled } = boot();
    firePreloadError(win, deadChunk);

    expect(JSON.parse(win.sessionStorage.getItem(KEY)!).n).toBe(1);
    expect(reloadsScheduled()).toBe(1);
  });

  it('acts once however many assets die in one document', () => {
    // A deploy kills the whole manifest at once, so a document that fails
    // usually fails several times over. One decision per document.
    const { win, reloadsScheduled } = boot();
    firePreloadError(win, deadChunk);
    const first = win.sessionStorage.getItem(KEY);
    firePreloadError(win, deadChunk);
    fireResourceError(win, buildScript(win, `${ORIGIN}/assets/index-2222.js`));

    expect(win.sessionStorage.getItem(KEY)).toBe(first);
    expect(reloadsScheduled()).toBe(1);
  });

  it('gives the reloaded document its second attempt, then the control', () => {
    // Each boot() is the next document the tab loads, carrying the counter its
    // predecessor left behind. This used to stop dead after the first reload:
    // a timestamp window stood in for the per-document latch above, and since
    // the reload lands within seconds it read the new document as a repeat of
    // the old one — swallowing the second attempt and the dead end behind it,
    // for a blank page with no control on it.
    let stamp: unknown;
    for (const attempt of [1, 2]) {
      const { win, reloadsScheduled } = boot({ stamp });
      firePreloadError(win, deadChunk);

      expect(reloadsScheduled()).toBe(1);
      stamp = JSON.parse(win.sessionStorage.getItem(KEY)!);
      expect(stamp).toMatchObject({ n: attempt });
    }

    // Third document, still dead. Leaving it blank here is the exact failure
    // this feature exists to remove.
    const { win, reloadsScheduled } = boot({ stamp });
    firePreloadError(win, deadChunk);

    expect(reloadsScheduled()).toBe(0);
    const root = win.document.getElementById('root')!;
    expect(root.querySelector('[role=alert]')).toBeTruthy();
    expect(root.querySelector('button')).toBeTruthy();
  });

  it('treats a corrupt attempt count as no attempt yet', () => {
    // Storage this code does not own. Reading a bogus value as "already at the
    // cap" would switch recovery off for the session and say nothing.
    const { win, reloadsScheduled } = boot({ stamp: { n: 'lots' } });
    firePreloadError(win, deadChunk);

    expect(reloadsScheduled()).toBe(1);
    expect(JSON.parse(win.sessionStorage.getItem(KEY)!).n).toBe(1);
  });

  it.each([
    ['an infinite count', '{"n":1e999}'],
    ['a fractional count', '{"n":0.5}'],
  ])('treats %s as no attempt yet', (_label, rawStamp) => {
    // Both are numbers greater than zero, so a bare `> 0` test let them
    // through: Infinity read as already past the cap, and a fraction counted
    // up in steps below one, reaching the cap a document later than it should.
    const { win, reloadsScheduled } = boot({ rawStamp });
    firePreloadError(win, deadChunk);

    expect(reloadsScheduled()).toBe(1);
    expect(JSON.parse(win.sessionStorage.getItem(KEY)!).n).toBe(1);
  });

  it('offers the control when session storage will not hold the count', () => {
    // Private mode or an exhausted quota. Reloading has to stay off — there is
    // no way to bound it — but the tab is still blank, which is the thing this
    // file exists to prevent, whatever the reason for it.
    const { win, reloadsScheduled } = boot();
    // Replaces the accessor, not the method: jsdom's Storage is proxy-backed,
    // so assigning setItem on the instance does not shadow the real one and
    // the write quietly succeeds.
    Object.defineProperty(win, 'sessionStorage', {
      configurable: true,
      value: {
        getItem: () => null,
        removeItem: () => {},
        setItem: () => {
          throw new Error('QuotaExceededError');
        },
      },
    });
    firePreloadError(win, deadChunk);

    expect(reloadsScheduled()).toBe(0);
    const root = win.document.getElementById('root')!;
    expect(root.querySelector('[role=alert]')).toBeTruthy();
    expect(root.querySelector('button')).toBeTruthy();
  });
});
