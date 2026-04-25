import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { useNetworkStatus } from '../useNetworkStatus';

describe('useNetworkStatus', () => {
  let originalOnLine: PropertyDescriptor | undefined;

  beforeEach(() => {
    originalOnLine = Object.getOwnPropertyDescriptor(globalThis.navigator, 'onLine');
    Object.defineProperty(globalThis.navigator, 'onLine', { configurable: true, get: () => true });
  });
  afterEach(() => {
    if (originalOnLine) Object.defineProperty(globalThis.navigator, 'onLine', originalOnLine);
  });

  it('reports navigator.onLine on first render', () => {
    Object.defineProperty(globalThis.navigator, 'onLine', { configurable: true, get: () => true });
    const { result } = renderHook(() => useNetworkStatus());
    expect(result.current.online).toBe(true);
  });

  it('flips to false on offline event', () => {
    const { result } = renderHook(() => useNetworkStatus());
    act(() => {
      window.dispatchEvent(new Event('offline'));
    });
    expect(result.current.online).toBe(false);
  });

  it('flips back to true on online event', () => {
    Object.defineProperty(globalThis.navigator, 'onLine', { configurable: true, get: () => false });
    const { result } = renderHook(() => useNetworkStatus());
    expect(result.current.online).toBe(false);
    act(() => {
      window.dispatchEvent(new Event('online'));
    });
    expect(result.current.online).toBe(true);
  });

  it('removes listeners on unmount', () => {
    const removeSpy = vi.spyOn(window, 'removeEventListener');
    const { unmount } = renderHook(() => useNetworkStatus());
    unmount();
    const events = removeSpy.mock.calls.map((c) => c[0]);
    expect(events).toContain('online');
    expect(events).toContain('offline');
  });

  it('returns true when navigator is undefined (SSR)', () => {
    // Stub typeof navigator === 'undefined' branch by deleting it temporarily.
    const realNav = globalThis.navigator;
    // @ts-expect-error — deliberately remove for SSR path test
    delete globalThis.navigator;
    try {
      const { result } = renderHook(() => useNetworkStatus());
      expect(result.current.online).toBe(true);
    } finally {
      Object.defineProperty(globalThis, 'navigator', { configurable: true, value: realNav });
    }
  });
});
