import React from 'react';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import {
  isStaleBuildError,
  checkForNewBuild,
  reportStaleBuild,
  watchStaleBuild,
  markBooted,
  STALE_BUILD_EVENT,
  __resetStaleBuildForTests,
} from '../staleBuild';
import { toast } from '@/components/ui/use-toast';
import { StaleBuildBoundary } from '@/components/StaleBuildBoundary';

vi.mock('@/components/ui/use-toast', () => ({ toast: vi.fn() }));

// The contract these lock is the one that is easy to get wrong in both
// directions: too loose and every crash becomes a "reload" prompt that hides a
// real bug; too tight and the blank page it exists to fix comes back.

const CHUNK_MSG = 'Failed to fetch dynamically imported module: ';

function setEntryScript(name: string) {
  document.head.querySelectorAll('script[type="module"]').forEach((n) => n.remove());
  const s = document.createElement('script');
  s.type = 'module';
  s.src = `${window.location.origin}/assets/${name}`;
  document.head.appendChild(s);
}

beforeEach(() => {
  __resetStaleBuildForTests();
  vi.mocked(toast).mockClear();
  sessionStorage.clear();
  delete window.__LA_BOOTED__;
  vi.spyOn(console, 'error').mockImplementation(() => {});
  vi.spyOn(console, 'warn').mockImplementation(() => {});
});

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

describe('isStaleBuildError', () => {
  it('matches a chunk failure naming a same-origin build asset', () => {
    const err = new Error(`${CHUNK_MSG}${window.location.origin}/assets/Dashboard-a1b2c3d4.js`);
    expect(isStaleBuildError(err)).toBe(true);
  });

  it('rejects the same message pointing at another origin', () => {
    // A third-party script failing to import is not our deploy.
    const err = new Error(`${CHUNK_MSG}https://cdn.example.com/assets/x.js`);
    expect(isStaleBuildError(err)).toBe(false);
  });

  it('rejects the same message with no URL at all', () => {
    // Bare pattern matching also fires on a plain network outage, which needs a
    // different remedy than reloading.
    expect(isStaleBuildError(new Error('error loading dynamically imported module'))).toBe(false);
  });

  it("matches Vite's CSS preload failure, which names the dep as a bare path", () => {
    // Only reachable once the edge answers a miss with a real 404: the SPA
    // fallback's 200 text/html makes the <link> fire `load`, so it never threw
    // before. The message carries no URL, only a path.
    const err = new Error('Unable to preload CSS for /assets/ChatAgent-Cx0714-u.css');
    expect(isStaleBuildError(err)).toBe(true);
  });

  it('rejects a CSS preload failure pointing at another origin', () => {
    const err = new Error('Unable to preload CSS for https://cdn.example.com/assets/x.css');
    expect(isStaleBuildError(err)).toBe(false);
  });

  it('rejects an ordinary render error', () => {
    expect(isStaleBuildError(new Error('Cannot read properties of undefined'))).toBe(false);
    expect(isStaleBuildError(new TypeError('x is not a function'))).toBe(false);
    expect(isStaleBuildError(null)).toBe(false);
  });

  it('accepts a URL-less chunk message when index.html flagged the build', () => {
    // Safari's message carries no URL, so the flag is the only signal there.
    window.__LA_STALE_BUILD__ = 'preload';
    expect(isStaleBuildError(new Error('Importing a module script failed.'))).toBe(true);
  });

  it('does not let the flag reclassify an unrelated error', () => {
    // The flag is sticky and set from a resource error. If it were checked
    // before the message, one flagged resource would turn every render bug for
    // the rest of the session into a "stale build" the boundary swallows.
    window.__LA_STALE_BUILD__ = 'resource';
    expect(isStaleBuildError(new TypeError('x is not a function'))).toBe(false);
    expect(isStaleBuildError(new Error('Cannot read properties of undefined'))).toBe(false);
  });
});

describe('checkForNewBuild', () => {
  beforeEach(() => setEntryScript('index-CURRENT.js'));

  const respond = (body: unknown, init: { ok?: boolean; type?: string } = {}) =>
    vi.fn().mockResolvedValue({
      ok: init.ok ?? true,
      headers: new Headers({ 'content-type': init.type ?? 'application/json' }),
      json: async () => body,
    });

  it('reports when the server serves a different build', async () => {
    vi.stubGlobal('fetch', respond({ build: 'index-NEWER.js' }));
    await checkForNewBuild();
    expect(console.error).toHaveBeenCalledWith(expect.stringContaining('staleBuild'));
  });

  it('stays quiet when the build matches', async () => {
    vi.stubGlobal('fetch', respond({ build: 'index-CURRENT.js' }));
    await checkForNewBuild();
    expect(console.error).not.toHaveBeenCalled();
  });

  it('stays quiet on an HTML answer even when its body parses as JSON', async () => {
    // A miss that fell through to the SPA shell answers 200 text/html. The
    // body deliberately parses here: a version.json that only checked the
    // parsed shape would be satisfied by any proxy, error page or dev server
    // that happens to return JSON-ish HTML, and would then tell every user they
    // are behind. Only the content-type check rejects this.
    vi.stubGlobal('fetch', respond({ build: 'index-NEWER.js' }, { type: 'text/html' }));
    await checkForNewBuild();
    expect(console.error).not.toHaveBeenCalled();
  });

  it('stays quiet when the body is not JSON at all', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue({
        ok: true,
        headers: new Headers({ 'content-type': 'application/json' }),
        json: async () => {
          throw new SyntaxError('Unexpected token <');
        },
      }),
    );
    await checkForNewBuild();
    expect(console.error).not.toHaveBeenCalled();
  });

  it('stays quiet on a non-OK response or a network failure', async () => {
    vi.stubGlobal('fetch', respond({ build: 'index-NEWER.js' }, { ok: false }));
    await checkForNewBuild();
    __resetStaleBuildForTests();
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new Error('offline')));
    await checkForNewBuild();
    expect(console.error).not.toHaveBeenCalled();
  });
});

describe('the pre-boot half and this one stay in step', () => {
  // index.html is plain HTML: it never reaches tsc and no unit test can import
  // it, so the only thing standing between the two classifiers and a silent
  // disagreement is an assertion on the source text itself.
  const html = readFileSync(resolve(__dirname, '../../../index.html'), 'utf8');
  const lib = readFileSync(resolve(__dirname, '../staleBuild.tsx'), 'utf8');

  it('names the same build prefix on both sides', () => {
    const inline = html.match(/BUILD_PREFIXES\s*=\s*\[([^\]]+)\]/)?.[1];
    const mod = lib.match(/const BUILD_PREFIX = '([^']+)'/)?.[1];
    expect(mod).toBeTruthy();
    expect(inline).toContain(`'${mod}'`);
  });

  it('anchors a bare path the same way on both sides', () => {
    // The ES5 copy used to match the prefix anywhere in the string, so
    // `dist/assets/x.css` was stale to it and not stale here. Both now require
    // the prefix to start a path.
    expect(html).toContain(`new RegExp('(^|[\\\\s\\'"(])' + BUILD_PREFIXES[j])`);
    expect(lib).toContain('new RegExp(`(^|[\\\\s\'"(])${BUILD_PREFIX}`)');
  });

  it('agrees on the event name that joins them', () => {
    expect(html).toContain(`'${STALE_BUILD_EVENT}'`);
  });
});

describe('reporting is once per session', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    setEntryScript('index-CURRENT.js');
  });

  it('surfaces once no matter how many signals arrive', async () => {
    // Four callers converge on this latch (boundary catch, the index.html
    // event, the mount-time flag replay, the version poll) and the toast is
    // duration:Infinity — so losing the latch stacks permanent toasts the user
    // has to dismiss one at a time.
    reportStaleBuild('chunk');
    reportStaleBuild('resource');
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue({
        ok: true,
        headers: new Headers({ 'content-type': 'application/json' }),
        json: async () => ({ build: 'index-NEWER.js' }),
      }),
    );
    await checkForNewBuild();
    vi.runAllTimers();

    expect(toast).toHaveBeenCalledTimes(1);
    expect(console.error).toHaveBeenCalledTimes(1);
  });

  it('takes the latch but skips the toast when the caller renders its own card', () => {
    reportStaleBuild('chunk', { silent: true });
    vi.runAllTimers();
    expect(toast).not.toHaveBeenCalled();
    // Still logged: a real /assets/* 404 regression must not vanish behind a
    // friendly card.
    expect(console.error).toHaveBeenCalledTimes(1);

    reportStaleBuild('version');
    vi.runAllTimers();
    expect(toast).not.toHaveBeenCalled();
  });

  it('lets a boundary claim a failure something else already reported', () => {
    // The ordering is not a race we can win by reordering callers: index.html
    // dispatches its event synchronously inside vite:preloadError, before Vite
    // rethrows, so the ambient toast is always in flight before React can
    // mount the card. Cancelling it is the only way one event makes one
    // notice, and this is what makes the boundary's `silent` mean anything.
    reportStaleBuild('preload');
    reportStaleBuild('chunk', { silent: true });
    vi.runAllTimers();

    expect(toast).not.toHaveBeenCalled();
  });

  it('dismisses a toast a boundary claims only after it was raised', () => {
    // The deferral is not a bound on when the claim arrives. App.tsx warms the
    // route chunk on mount while the setup gate is still resolving, so the
    // import can reject long before Main — and the boundary in it — exists at
    // all. By then there is no timer left to cancel, only a toast to retract.
    const dismiss = vi.fn();
    vi.mocked(toast).mockReturnValue({ id: '1', dismiss, update: vi.fn() });

    reportStaleBuild('preload');
    vi.runAllTimers();
    expect(toast).toHaveBeenCalledTimes(1);

    reportStaleBuild('chunk', { silent: true });
    expect(dismiss).toHaveBeenCalledTimes(1);
  });

  it('still raises the toast when no boundary claims the failure', () => {
    // The other half of the deferral. Main.tsx prefetches a route chunk and
    // swallows the rejection, so plenty of dead chunks never reach a boundary
    // at all — waiting must not turn into never telling the user.
    reportStaleBuild('preload');
    expect(toast).not.toHaveBeenCalled();

    vi.runAllTimers();
    expect(toast).toHaveBeenCalledTimes(1);
    // Ordinary notices evict the oldest toast, and this one is the oldest by
    // the time any arrive — without the exemption the only Reload control in
    // the app disappears and the latch above stops it coming back.
    expect(vi.mocked(toast).mock.calls[0][0]).toMatchObject({ pinned: true });
  });
});

describe('checkForNewBuild rate and reporting', () => {
  beforeEach(() => setEntryScript('index-CURRENT.js'));

  it('fetches at most once per throttle window', async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      headers: new Headers({ 'content-type': 'application/json' }),
      json: async () => ({ build: 'index-CURRENT.js' }),
    });
    vi.stubGlobal('fetch', fetchMock);

    // Two live drivers now call this (tab resume, and every settled turn), so
    // an alt-tabbing user on a long stream would otherwise poll continuously.
    await checkForNewBuild();
    await checkForNewBuild();
    await checkForNewBuild();
    expect(fetchMock).toHaveBeenCalledTimes(1);

    vi.spyOn(Date, 'now').mockReturnValue(Date.now() + 61_000);
    await checkForNewBuild();
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it('warns once when the manifest answers but cannot be read', async () => {
    // The silent-on-ambiguity rule is right for users and wrong for operators:
    // an unreadable manifest is indistinguishable from "up to date" forever.
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue({
        ok: true,
        headers: new Headers({ 'content-type': 'text/html' }),
        json: async () => ({ build: 'index-NEWER.js' }),
      }),
    );
    await checkForNewBuild();
    expect(console.warn).toHaveBeenCalledTimes(1);
    expect(console.error).not.toHaveBeenCalled();
  });

  it('warns when the manifest is JSON that does not parse', async () => {
    // A truncated or half-written manifest answers with the right header and
    // an unreadable body. Left inside the network catch it read as "offline"
    // and the version layer went quiet with nothing said about it.
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue({
        ok: true,
        headers: new Headers({ 'content-type': 'application/json' }),
        json: async () => {
          throw new SyntaxError('Unexpected end of JSON input');
        },
      }),
    );
    await checkForNewBuild();
    expect(console.warn).toHaveBeenCalledTimes(1);
  });

  it('stays quiet when the request never lands', async () => {
    // The genuinely ambiguous case, and the reason the warn is scoped rather
    // than applied to every failure: an offline tab is not evidence of a deploy.
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new TypeError('Failed to fetch')));
    await checkForNewBuild();
    expect(console.warn).not.toHaveBeenCalled();
    expect(console.error).not.toHaveBeenCalled();
  });
});

describe('watchStaleBuild wiring', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    setEntryScript('index-CURRENT.js');
  });

  it('replays a flag index.html set before React mounted', () => {
    // The likeliest real timeline: an asset dies during boot, React mounts a
    // moment later, and the listener attaches too late to have seen the event.
    // This replay is the entire reason the flag is a sticky global.
    window.__LA_STALE_BUILD__ = 'resource';
    const stop = watchStaleBuild();
    vi.runAllTimers();
    expect(toast).toHaveBeenCalledTimes(1);
    stop();
  });

  it('reports an event dispatched after mount, and stops after cleanup', () => {
    const stop = watchStaleBuild();
    window.dispatchEvent(new CustomEvent(STALE_BUILD_EVENT, { detail: 'preload' }));
    vi.runAllTimers();
    expect(toast).toHaveBeenCalledTimes(1);

    stop();
    __resetStaleBuildForTests();
    vi.mocked(toast).mockClear();
    window.dispatchEvent(new CustomEvent(STALE_BUILD_EVENT, { detail: 'preload' }));
    vi.runAllTimers();
    expect(toast).not.toHaveBeenCalled();
  });
});

describe('markBooted', () => {
  it('flips the gate index.html reads before it will reload', () => {
    // This flag is the switch between "reload silently" and "ask the user". A
    // refactor that drops or defers it reintroduces a reload that discards an
    // agent turn mid-stream.
    expect(window.__LA_BOOTED__).toBeFalsy();
    markBooted();
    expect(window.__LA_BOOTED__).toBe(true);
  });

  it('clears the pre-boot attempt counter, since booting is what proves it worked', () => {
    sessionStorage.setItem('__la_asset_recovery__', JSON.stringify({ n: 2 }));
    markBooted();
    expect(sessionStorage.getItem('__la_asset_recovery__')).toBeNull();
  });
});

describe('StaleBuildBoundary', () => {
  const Boom = ({ error }: { error: Error }) => {
    throw error;
  };

  it('renders the recovery fallback for a chunk failure', () => {
    const err = new Error(`${CHUNK_MSG}${window.location.origin}/assets/x-1234abcd.js`);
    render(
      <StaleBuildBoundary>
        <Boom error={err} />
      </StaleBuildBoundary>,
    );
    expect(screen.getByRole('alert')).toBeInTheDocument();
  });

  it('rethrows an ordinary error instead of swallowing it', () => {
    // The whole point of the classification: a boundary that catches everything
    // turns deterministic bugs into "stale build" reports.
    class Catcher extends React.Component<{ children: React.ReactNode }, { hit: boolean }> {
      state = { hit: false };
      static getDerivedStateFromError() {
        return { hit: true };
      }
      render() {
        return this.state.hit ? <div data-testid="outer" /> : this.props.children;
      }
    }

    render(
      <Catcher>
        <StaleBuildBoundary>
          <Boom error={new Error('ordinary bug')} />
        </StaleBuildBoundary>
      </Catcher>,
    );
    expect(screen.getByTestId('outer')).toBeInTheDocument();
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
  });

  it('propagates a falsy thrown value instead of re-rendering the children', async () => {
    // `throw null` is legal, and a custom lazy loader can reject with it. Read
    // through the thrown value's truthiness, that state looks like "nothing
    // was thrown", so the boundary renders the children that just threw and
    // they throw again — a loop where a rethrow was intended.
    class Catcher extends React.Component<{ children: React.ReactNode }, { hit: boolean }> {
      state = { hit: false };
      static getDerivedStateFromError() {
        return { hit: true };
      }
      render() {
        return this.state.hit ? <div data-testid="outer" /> : this.props.children;
      }
    }
    const Lazy = React.lazy(() => Promise.reject(null));

    render(
      <Catcher>
        <React.Suspense fallback={<div data-testid="spinner" />}>
          <StaleBuildBoundary variant="pane">
            <Lazy />
          </StaleBuildBoundary>
        </React.Suspense>
      </Catcher>,
    );

    expect(await screen.findByTestId('outer')).toBeInTheDocument();
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
  });

  it('catches a rejected React.lazy from inside the Suspense it sits in', async () => {
    // Main.tsx puts this boundary INSIDE <Suspense> rather than around it. The
    // placement is not cosmetic: the boundary is keyed by route so navigating
    // away from a dead chunk clears the error, and a key on the outside remounts
    // the Suspense boundary too, which then has to show its fallback and flashes
    // the pane spinner on every first navigation. This pins the property that
    // made the move safe — a rejected lazy still reaches it from in there.
    const Lazy = React.lazy(() =>
      Promise.reject(new Error(`${CHUNK_MSG}${window.location.origin}/assets/route-9f8e7d6c.js`)),
    );

    render(
      <React.Suspense fallback={<div data-testid="spinner" />}>
        <StaleBuildBoundary variant="pane">
          <Lazy />
        </StaleBuildBoundary>
      </React.Suspense>,
    );

    expect(await screen.findByRole('alert')).toBeInTheDocument();
  });
});
