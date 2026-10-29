// @vitest-environment node
import { describe, it, expect, afterEach, vi } from 'vitest';

import { randomUUID } from '../randomUUID';

describe('randomUUID', () => {
  const originalCrypto = globalThis.crypto;

  afterEach(() => {
    vi.stubGlobal('crypto', originalCrypto);
  });

  it('uses crypto.randomUUID when available', () => {
    const value = randomUUID();
    expect(value).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/);
  });

  it('falls back to a real v4 UUID built from getRandomValues when crypto.randomUUID is missing', () => {
    // Mirrors a browser insecure context (plain HTTP on a non-localhost host):
    // crypto.randomUUID is undefined there, but getRandomValues is not.
    vi.stubGlobal('crypto', {
      getRandomValues: originalCrypto.getRandomValues.bind(originalCrypto),
    });
    const value = randomUUID();
    expect(value).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
  });

  it('still returns a UUID-shaped, unique string when neither crypto API is available', () => {
    vi.stubGlobal('crypto', undefined);
    const a = randomUUID();
    const b = randomUUID();
    expect(a).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
    expect(a).not.toBe(b);
  });
});
