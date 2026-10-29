import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { act, renderHook } from '@testing-library/react';
import { useLastHeight, useLastShape } from '../lastShape';
import { setStorageUser, userLocalStorage } from '../userStorage';

// A platform build, where shapes are kept per signed-in user.
vi.mock('@/config/hostMode', async (importActual) => ({
  ...(await importActual<typeof import('@/config/hostMode')>()),
  isPlatformMode: true,
}));
vi.mock('@/config/localUser', async (importActual) => ({
  ...(await importActual<typeof import('@/config/localUser')>()),
  hasAuthEnv: true,
}));

describe('useLastShape', () => {
  beforeEach(() => localStorage.clear());

  it('is null on a device that never loaded the region', () => {
    const { result } = renderHook(() => useLastShape('rows', null));
    expect(result.current).toBeNull();
  });

  it('keeps the shape read at mount for the whole load, and the loaded one for next time', () => {
    userLocalStorage.setItem('lastShape.rows', '3');
    const { result, rerender } = renderHook(({ current }) => useLastShape('rows', current), {
      initialProps: { current: null as number | null },
    });
    expect(result.current).toBe(3);
    rerender({ current: 7 });
    // The skeleton this mount drew stays its shape; the next mount gets 7.
    expect(result.current).toBe(3);
    expect(userLocalStorage.getItem('lastShape.rows')).toBe('7');
  });

  it('reads the new key when the region changes, rather than carrying the old one', () => {
    userLocalStorage.setItem('lastShape.tab.a', '100');
    userLocalStorage.setItem('lastShape.tab.b', '250');
    const { result, rerender } = renderHook(({ k }) => useLastShape(k, null), { initialProps: { k: 'tab.a' } });
    expect(result.current).toBe(100);
    rerender({ k: 'tab.b' });
    expect(result.current).toBe(250);
  });

  it.each(['oops', '-3', '2.5', '1e8', ''])('ignores a stored %s, which this module never writes', (stored) => {
    userLocalStorage.setItem('lastShape.rows', stored);
    const { result } = renderHook(() => useLastShape('rows', null));
    expect(result.current).toBeNull();
  });
});

describe('accounts', () => {
  beforeEach(() => localStorage.clear());

  it("never reads another account's shapes", () => {
    localStorage.setItem('user:someone-else:lastShape.gallery.workspaces', '8');
    const { result } = renderHook(() => useLastShape('gallery.workspaces', null));
    expect(result.current).toBeNull();
  });

  it('reads again when the account changes under a mounted region', () => {
    setStorageUser('a');
    userLocalStorage.setItem('lastShape.gallery.workspaces', '8');
    setStorageUser('b');
    userLocalStorage.setItem('lastShape.gallery.workspaces', '2');
    setStorageUser('a');
    const { result } = renderHook(() => useLastShape('gallery.workspaces', null));
    expect(result.current).toBe(8);
    act(() => setStorageUser('b'));
    expect(result.current).toBe(2);
  });
});

describe('useLastHeight', () => {
  beforeEach(() => localStorage.clear());
  afterEach(() => vi.unstubAllGlobals());

  it('keeps no height taken while not recording, even as recording resumes', () => {
    const observers: Array<() => void> = [];
    vi.stubGlobal('ResizeObserver', class {
      constructor(private report: () => void) {}
      observe() { observers.push(this.report); }
      disconnect() { observers.splice(observers.indexOf(this.report), 1); }
    });
    const el = document.createElement('div');
    let offsetHeight = 500;
    Object.defineProperty(el, 'offsetHeight', { get: () => offsetHeight });
    const { result, rerender } = renderHook(({ record }) => useLastHeight('brief', record), {
      initialProps: { record: false },
    });
    act(() => result.current.ref(el));
    // The drawer open: measured, not kept.
    act(() => observers.forEach((report) => report()));
    rerender({ record: true });
    expect(userLocalStorage.getItem('lastShape.brief')).toBeNull();
    // The drawer closed: the layout it returns to is the one kept.
    offsetHeight = 300;
    act(() => observers.forEach((report) => report()));
    expect(userLocalStorage.getItem('lastShape.brief')).toBe('300');
  });
});
