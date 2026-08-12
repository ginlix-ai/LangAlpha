import { describe, it, expect, beforeEach } from 'vitest';
import { getLocaleCookie, setLocaleCookie, detectLocale } from '../lib/locale';

// localStorage-based locale + the cross-tab `storage` listener were replaced by a
// single shared `locale` cookie (readable server-side, unlike localStorage).
// These cover the cookie helpers + cookie-first detection.
function clearLocaleCookie() {
  document.cookie = 'locale=; path=/; expires=Thu, 01 Jan 1970 00:00:00 GMT';
}

describe('locale cookie helpers', () => {
  beforeEach(() => clearLocaleCookie());

  it('returns null when no locale cookie is set', () => {
    expect(getLocaleCookie()).toBeNull();
  });

  it('round-trips a supported locale', () => {
    setLocaleCookie('zh-CN');
    expect(getLocaleCookie()).toBe('zh-CN');
  });

  it('ignores unsupported values on write', () => {
    setLocaleCookie('fr-FR');
    expect(getLocaleCookie()).toBeNull();
  });

  it('treats an unsupported cookie value as absent on read', () => {
    document.cookie = 'locale=zh-TW; path=/'; // valid BCP-47, not supported
    expect(getLocaleCookie()).toBeNull();
  });

  it('treats a malformed percent-encoded cookie as absent instead of throwing', () => {
    // getLocaleCookie runs at module load via detectLocale → a throw here
    // would white-screen the app on every load until cookies are cleared.
    document.cookie = 'locale=%E0%A4%A; path=/';
    expect(getLocaleCookie()).toBeNull();
    expect(() => detectLocale()).not.toThrow();
  });

  it('detectLocale prefers a valid cookie', () => {
    setLocaleCookie('zh-CN');
    expect(detectLocale()).toBe('zh-CN');
  });
});
