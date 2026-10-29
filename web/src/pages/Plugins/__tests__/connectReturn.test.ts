/**
 * What is owed to someone whose connect left and came back, with an answer or
 * without one.
 *
 * Pinned because it fails quietly: the return path must survive a bfcache
 * restore, which is how a user actually comes back from a refused connect and
 * the one path a mount effect misses.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { render, waitFor } from '@testing-library/react';
import React from 'react';
import { MemoryRouter } from 'react-router';
import type { McpServerInput } from '@/pages/ChatAgent/utils/api';

const landing = vi.hoisted(() => ({ landConnectReturn: vi.fn() }));
vi.mock('../connectLanding', () => landing);

const { markConnectLeft, rememberConnect, settleConnectReturn } = await import('../connectReturn');
const { ConnectLandingHost } = await import('../components/ConnectLandingHost');

const at = (search: string) => window.history.replaceState({}, '', `/plugins${search}`);
/** A workspace page, where its settings panel can start a sign-in too. */
const WORKSPACE = '/chat/ws-1';
const onWorkspace = () => window.history.replaceState({}, '', WORKSPACE);
const records = () => JSON.parse(sessionStorage.getItem('mcp:connect-started') ?? '[]');
const restore = (persisted: boolean) =>
  window.dispatchEvent(Object.assign(new Event('pageshow'), { persisted }));

/** A connect started from the page the test is on. */
const start = (server: string) =>
  rememberConnect({
    server,
    label: server,
    retry: { kind: 'other' },
    desktopOnly: false,
    returnTo: '/plugins?tab=mcp',
  });
/** A connect started here that went to the vendor. */
const startedAndLeft = (server: string) => {
  start(server);
  markConnectLeft(server);
};
const settled = (search: string) => settleConnectReturn(search);
const abandoned = (search = '') => {
  const landed = settled(search);
  return landed?.kind === 'abandoned' ? landed.record.server : null;
};

beforeEach(() => {
  sessionStorage.clear();
  at('');
});

afterEach(() => {
  vi.resetAllMocks();
  vi.restoreAllMocks();
});

describe('settleConnectReturn', () => {
  it('reports the server whose connect never came back, once', () => {
    startedAndLeft('robinhood');
    expect(abandoned()).toBe('robinhood');
    expect(abandoned()).toBeNull();
  });

  it('says nothing when no connect was started', () => {
    expect(settled('')).toBeNull();
  });

  // A landing that carries the callback's own params means the flow ended and
  // the page below has something to say about it. The record still has to go:
  // left behind, it would have some later ordinary visit announce a failure
  // that resolved long ago.
  it.each([
    ['?mcp_connected=robinhood'],
    ['?mcp_error=invalid_state&server=robinhood'],
  ])('answers %s, and spends the record', (search) => {
    startedAndLeft('robinhood');
    at(search);
    expect(settled(search)).toMatchObject({ kind: 'answered', record: { server: 'robinhood' } });

    at('');
    expect(settled('')).toBeNull();
  });

  // Inside the shell the authorize URL opens in the system browser, so the page
  // never leaves and a second broker can be started while the first is out. One
  // slot meant the second overwrote the first, and then whichever answered first
  // spent a record belonging to the other.
  it('spends only the record the landing is about', () => {
    startedAndLeft('robinhood');
    startedAndLeft('ibkr');
    expect(settled('?mcp_connected=robinhood')).toMatchObject({ record: { server: 'robinhood' } });

    // ibkr never answered, and its record survived the other's landing.
    expect(abandoned()).toBe('ibkr');
  });

  // Nothing in a bare landing names which flow it belongs to. Announcing one
  // would be a guess, and the wrong guess calls a live broker abandoned.
  it('names nobody while more than one connect is out', () => {
    startedAndLeft('robinhood');
    startedAndLeft('ibkr');
    expect(settled('')).toBeNull();
  });

  // A sign-in that installs a plugin is reported under its server's name, which
  // is the name the start answered with and the record was written under.
  it('spends the record of a sign-in that installed a plugin', () => {
    startedAndLeft('alpha_vantage');
    expect(settled('?mcp_connected=alpha_vantage&plugin=alpha-vantage')).toMatchObject({
      record: { server: 'alpha_vantage' },
    });
    expect(settled('')).toBeNull();
  });

  // The shell keeps the page alive while the vendor's page is open in the
  // system browser, so the same row can be started twice. The callback names a
  // server and nothing of the flow, so one record stands for both.
  it('keeps one record for a row started twice', () => {
    start('robinhood');
    start('robinhood');
    expect(records()).toHaveLength(1);
  });

  // Private windows and blocked site data. This guidance is a nicety, and it
  // must never be the thing that takes the page down.
  it('survives a storage that throws on both sides', () => {
    vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => {
      throw new Error('blocked');
    });
    vi.spyOn(Storage.prototype, 'getItem').mockImplementation(() => {
      throw new Error('blocked');
    });
    expect(() => startedAndLeft('robinhood')).not.toThrow();
    expect(settled('')).toBeNull();
  });

  // Back returns the user to the page a sign-in left from. One that left from a
  // workspace page and never came back used to be announced on the next visit
  // to another page, about a page the user was no longer on.
  it('leaves a sign-in that left from another page to that page', () => {
    onWorkspace();
    startedAndLeft('wind');
    at('');
    expect(settled('')).toBeNull();

    onWorkspace();
    expect(abandoned()).toBe('wind');
  });

  // The other half of the same bug: another page's record made two out, and
  // with two out this page's own abandonment went unreported.
  it("reports this page's sign-in while another page's is still out", () => {
    onWorkspace();
    startedAndLeft('wind');
    at('');
    startedAndLeft('robinhood');

    expect(abandoned()).toBe('robinhood');
    expect(records()).toMatchObject([{ server: 'wind', from: WORKSPACE }]);
  });

  // The callback lands on one page for every surface, so its answer retires a
  // record whichever page it left from.
  it('spends the record of a sign-in started elsewhere when its callback lands', () => {
    onWorkspace();
    startedAndLeft('wind');
    at('?mcp_connected=wind');
    settled('?mcp_connected=wind');

    expect(records()).toEqual([]);
  });

  // Inside the desktop app the vendor's page opens in the system browser and
  // this page stays; a record read as backed out of at the start was called
  // abandoned on the next mount while the sign-in was still live there.
  it('calls nothing abandoned until the page actually leaves', () => {
    start('robinhood');
    expect(settled('')).toBeNull();
    expect(records()).toMatchObject([{ server: 'robinhood', from: '/plugins', left: false }]);

    markConnectLeft('robinhood');
    expect(abandoned()).toBe('robinhood');
  });
});

describe('who an answer belongs to', () => {
  it('vouches only for a connect this tab started', () => {
    start('moomoo');
    expect(settled('?mcp_connected=moomoo')).toMatchObject({ record: { server: 'moomoo' } });
    // The callback's params alone, as a link would carry them.
    expect(settled('?mcp_connected=robinhood')).toMatchObject({ kind: 'answered', record: null });
  });

  it('takes an answer naming no server for the one connect out, and only then', () => {
    const nameless = '?mcp_error=access_denied';
    expect(settled(nameless)).toMatchObject({ record: null });
    start('moomoo');
    expect(settled(nameless)).toMatchObject({ record: { server: 'moomoo' } });
    start('moomoo');
    start('webull');
    expect(settled(nameless)).toMatchObject({ record: null });
  });
});

// A sign-in that saves a server keeps its body until it comes back. A value
// with a ref in it was kept whole, so a key typed beside the ref went into
// session storage, which any script on the page can read.
it('keeps no key typed beside a stored one', () => {
  const server: McpServerInput = {
    name: 'acme',
    transport: 'http',
    url: 'https://acme.example.com/mcp',
    headers: {
      Authorization: 'Bearer ${vault:ACME}',
      'X-Key': 'sk-live ${vault:ACME}',
      'X-Raw': 'sk-live',
    },
  };
  rememberConnect({
    server: 'acme',
    label: 'acme',
    retry: { kind: 'server', server },
    desktopOnly: false,
    returnTo: '/plugins?tab=mcp',
  });

  expect(records()[0].retry.server.headers).toEqual({
    Authorization: 'Bearer ${vault:ACME}',
    'X-Key': '',
    'X-Raw': '',
  });
});

describe('ConnectLandingHost', () => {
  const host = () => render(React.createElement(MemoryRouter, null, React.createElement(ConnectLandingHost)));

  it('reads the landing a page load brings', async () => {
    startedAndLeft('robinhood');
    host();
    await waitFor(() => expect(landing.landConnectReturn).toHaveBeenCalledTimes(1));
  });

  it('loads nothing when no connect is out and no answer came', async () => {
    host();
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(landing.landConnectReturn).not.toHaveBeenCalled();
  });

  // The real sequence: the page was already mounted when the user pressed
  // Connect, so nothing was pending at mount. Coming back from the vendor is a
  // bfcache restore, which returns the tab without remounting anything.
  it('reads one that left after mount and came back through bfcache', async () => {
    host();
    startedAndLeft('robinhood');
    restore(true);
    await waitFor(() => expect(landing.landConnectReturn).toHaveBeenCalledTimes(1));
  });

  it('ignores a pageshow that is not a restore', async () => {
    host();
    startedAndLeft('robinhood');
    restore(false);
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(landing.landConnectReturn).not.toHaveBeenCalled();
  });

  it('stops listening once unmounted', async () => {
    host().unmount();
    startedAndLeft('robinhood');
    restore(true);
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(landing.landConnectReturn).not.toHaveBeenCalled();
  });
});
