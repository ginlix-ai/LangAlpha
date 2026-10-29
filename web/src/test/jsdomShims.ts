/// <reference types="vitest/globals" />
import '@testing-library/jest-dom/vitest';
import { webcrypto } from 'node:crypto';

export function installJsdomShims(): void {
  // Under the vm pool the global is jsdom's window, whose crypto has no `subtle`;
  // a browser on a secure origin has it, and so does the forks pool. Without it
  // auth-js quietly signs in with plain PKCE instead of S256, a branch production
  // never takes, and tests of that flow still pass.
  if (!globalThis.crypto.subtle) {
    Object.defineProperty(globalThis.crypto, 'subtle', { configurable: true, value: webcrypto.subtle });
  }

  // Mock window.matchMedia for framer-motion. Configurable so a test can still
  // vi.stubGlobal it: under the vm pool `window` is the global itself.
  Object.defineProperty(window, 'matchMedia', {
    configurable: true,
    writable: true,
    value: (query: string) => ({
      matches: false,
      media: query,
      onchange: null,
      addListener: () => {},
      removeListener: () => {},
      addEventListener: () => {},
      removeEventListener: () => {},
      dispatchEvent: () => false,
    }),
  });

  // Mock IntersectionObserver
  class IntersectionObserverMock {
    observe = vi.fn();
    unobserve = vi.fn();
    disconnect = vi.fn();
  }
  window.IntersectionObserver = IntersectionObserverMock as unknown as typeof IntersectionObserver;

  // Mock ResizeObserver
  class ResizeObserverMock {
    observe = vi.fn();
    unobserve = vi.fn();
    disconnect = vi.fn();
  }
  window.ResizeObserver = ResizeObserverMock as unknown as typeof ResizeObserver;

  // jsdom has no canvas: getContext already answers null, which painters guard,
  // but logs "Not implemented" on every call. Same answer, without the noise.
  HTMLCanvasElement.prototype.getContext = (() => null) as typeof HTMLCanvasElement.prototype.getContext;

  // Tests run past the auth gate, as the app does. A platform build keeps no
  // workspace state for nobody (lib/userStorage.ts), so without a user every
  // tab and thread persistence test would find nothing stored. Imported per test
  // so a file that mocks the host mode gets the instance it mocked.
  beforeEach(async () => {
    (await import('@/lib/userStorage')).setStorageUser('test-user');
  });
}
