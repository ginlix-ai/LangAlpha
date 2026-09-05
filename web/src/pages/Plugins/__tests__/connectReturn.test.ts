/**
 * What is owed to someone whose connect left and never came back.
 *
 * Pinned because it fails quietly: the return path must survive a bfcache
 * restore, which is how a user actually comes back from a refused connect and
 * the one path a mount effect misses.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { renderHook } from '@testing-library/react';

const { markConnectStarted, useConnectReturn } = await import('../connectReturn');

const at = (search: string) => window.history.replaceState({}, '', `/plugins${search}`);
const restore = (persisted: boolean) =>
  window.dispatchEvent(Object.assign(new Event('pageshow'), { persisted }));
/** Both handlers, so a test can assert on one without naming the other. */
const handlers = () => ({ onAbandoned: vi.fn(), onStandDown: vi.fn() });

beforeEach(() => {
  sessionStorage.clear();
  at('');
});

afterEach(() => {
  vi.restoreAllMocks();
});

describe('useConnectReturn', () => {
  it('reports the server whose connect never came back', () => {
    markConnectStarted('robinhood', false);
    const h = handlers();

    renderHook(() => useConnectReturn(h));

    expect(h.onAbandoned).toHaveBeenCalledExactlyOnceWith('robinhood');
  });

  it('says nothing when no connect was started', () => {
    const h = handlers();
    renderHook(() => useConnectReturn(h));
    expect(h.onAbandoned).not.toHaveBeenCalled();
  });

  // A landing that carries the callback's own params means the flow ended and
  // the page below already has something to say about it. The marker still has
  // to go: left behind, it would have some later ordinary visit announce a
  // failure that resolved long ago.
  it.each([
    ['?mcp_connected=robinhood'],
    ['?mcp_error=invalid_state&server=robinhood'],
  ])('stays quiet on %s, and spends the marker', (search) => {
    markConnectStarted('robinhood', false);
    at(search);
    const h = handlers();

    renderHook(() => useConnectReturn(h));
    expect(h.onAbandoned).not.toHaveBeenCalled();

    at('');
    renderHook(() => useConnectReturn(h));
    expect(h.onAbandoned).not.toHaveBeenCalled();
  });

  // Inside the shell the authorize URL opens in the system browser, so the page
  // never leaves and a second broker can be started while the first is out. One
  // slot meant the second overwrote the first, and then whichever answered first
  // spent a marker belonging to the other.
  it('spends only the marker the landing is about', () => {
    markConnectStarted('robinhood', false);
    markConnectStarted('ibkr', false);
    at('?mcp_connected=robinhood');
    const h = handlers();

    renderHook(() => useConnectReturn(h));
    expect(h.onAbandoned).not.toHaveBeenCalled();

    // ibkr never answered, and its marker survived the other's landing.
    at('');
    renderHook(() => useConnectReturn(h));
    expect(h.onAbandoned).toHaveBeenCalledExactlyOnceWith('ibkr');
  });

  // Nothing in a bare landing names which flow it belongs to. Announcing one
  // would be a guess, and the wrong guess calls a live broker abandoned.
  it('names nobody while more than one connect is out', () => {
    markConnectStarted('robinhood', false);
    markConnectStarted('ibkr', false);
    at('');
    const h = handlers();

    renderHook(() => useConnectReturn(h));
    expect(h.onAbandoned).not.toHaveBeenCalled();
  });

  // The real sequence: the page was already mounted when the user pressed
  // Connect, so nothing was pending at mount. Coming back from the vendor is a
  // bfcache restore, which returns the tab without remounting anything.
  it('reports one that left after mount and came back through bfcache', () => {
    const h = handlers();
    renderHook(() => useConnectReturn(h));
    expect(h.onAbandoned).not.toHaveBeenCalled();

    markConnectStarted('robinhood', false);
    restore(true);

    expect(h.onAbandoned).toHaveBeenCalledExactlyOnceWith('robinhood');
  });

  it('ignores a pageshow that is not a restore', () => {
    const h = handlers();
    renderHook(() => useConnectReturn(h));

    markConnectStarted('robinhood', false);
    restore(false);

    expect(h.onAbandoned).not.toHaveBeenCalled();
  });

  it('stops listening once unmounted', () => {
    const h = handlers();
    const { unmount } = renderHook(() => useConnectReturn(h));
    unmount();

    markConnectStarted('robinhood', false);
    restore(true);

    expect(h.onAbandoned).not.toHaveBeenCalled();
  });

  // The lifecycle's own rollback runs from a catch that sits before the jump to
  // the vendor, and everything the vendor can refuse happens after it. Left
  // alone, a row this connect switched on stays on with nothing behind it, and
  // a brokerage switched on is inherited by every workspace.
  it('stands down a row this connect brought live when the vendor refused', () => {
    markConnectStarted('robinhood', true);
    at('?mcp_error=denied&server=robinhood');
    const h = handlers();

    renderHook(() => useConnectReturn(h));

    expect(h.onStandDown).toHaveBeenCalledExactlyOnceWith('robinhood');
  });

  // The row was the user's, and already on, before any of this started.
  it('leaves a row alone that was live before the connect', () => {
    markConnectStarted('robinhood', false);
    at('?mcp_error=denied&server=robinhood');
    const h = handlers();

    renderHook(() => useConnectReturn(h));

    expect(h.onStandDown).not.toHaveBeenCalled();
  });

  it('leaves the row up when the connect succeeded', () => {
    markConnectStarted('robinhood', true);
    at('?mcp_connected=robinhood');
    const h = handlers();

    renderHook(() => useConnectReturn(h));

    expect(h.onStandDown).not.toHaveBeenCalled();
  });

  // A bare landing could as easily be a connect that finished in another tab.
  // Switching off a live broker on a guess is worse than leaving one on.
  it('stands nothing down on a landing with nothing to say', () => {
    markConnectStarted('robinhood', true);
    at('');
    const h = handlers();

    renderHook(() => useConnectReturn(h));

    expect(h.onAbandoned).toHaveBeenCalledExactlyOnceWith('robinhood');
    expect(h.onStandDown).not.toHaveBeenCalled();
  });

  // Two out at once, which is only reachable in the shell. The refusal names
  // one of them, and the other is still at its consent screen.
  it('stands down only the row the refusal names', () => {
    markConnectStarted('robinhood', true);
    markConnectStarted('ibkr', true);
    at('?mcp_error=denied&server=ibkr');
    const h = handlers();

    renderHook(() => useConnectReturn(h));

    expect(h.onStandDown).toHaveBeenCalledExactlyOnceWith('ibkr');
  });

  // Two connects for the SAME row, which the shell also allows: it keeps the
  // page alive, and starting another broker frees the first one's button. The
  // return names a server and nothing of the flow, so it cannot say which of
  // the two it settled -- and standing the row down on the wrong guess takes
  // away what the live attempt has just brought up. The undo is given up
  // instead, leaving a row the user can switch off themselves.
  it('gives up the undo once a second connect joins the same row', () => {
    markConnectStarted('robinhood', true);
    markConnectStarted('robinhood', true);
    expect(JSON.parse(sessionStorage.getItem('mcp:connect-started') ?? '[]')).toHaveLength(1);

    at('?mcp_error=invalid_state&server=robinhood');
    const h = handlers();

    renderHook(() => useConnectReturn(h));

    expect(h.onStandDown).not.toHaveBeenCalled();
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
    const h = handlers();

    expect(() => markConnectStarted('robinhood', false)).not.toThrow();
    expect(() => renderHook(() => useConnectReturn(h))).not.toThrow();
    expect(h.onAbandoned).not.toHaveBeenCalled();
  });
});
