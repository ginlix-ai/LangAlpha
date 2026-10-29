import { afterAll, afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  SETUP_RETURN_KEY,
  allowedTarget,
  captureSetupReturn,
  clearSetupReturn,
  readSetupReturn,
  returnFromSetup,
  withoutSetupReturnParam,
} from '../setupReturn';

const ORIGIN = 'https://app.example.com';
const BASE = '/account';
const GOOD = `${ORIGIN}/account/onboarding/models`;
const HOUR = 60 * 60 * 1000;
const NOW = 1_000_000;
const search = (url: string) => `?${new URLSearchParams({ return: url })}`;

vi.mock('@/config/supportLinks', () => ({ PLATFORM_URL: '/account' }));

/**
 * This page's origin and the way out of it. `assign` is an own non-configurable
 * property of Location, so the whole object is swapped rather than the one
 * method spied.
 */
const assign = vi.fn();
const realLocation = window.location;
Object.defineProperty(window, 'location', {
  configurable: true,
  value: { href: `${ORIGIN}/setup/method`, origin: ORIGIN, assign },
});
afterAll(() => {
  Object.defineProperty(window, 'location', { configurable: true, value: realLocation });
});

beforeEach(() => {
  vi.useFakeTimers({ toFake: ['Date'] });
  vi.setSystemTime(NOW);
});

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
  assign.mockClear();
  sessionStorage.clear();
});

describe('allowedTarget', () => {
  const allowed = (url: string, base: string | null = BASE) => allowedTarget(url, base, ORIGIN) !== null;

  it('accepts a page under the account onboarding path on this origin', () => {
    expect(allowed(GOOD)).toBe(true);
    expect(allowed(`${GOOD}?step=2#top`)).toBe(true);
  });

  it('accepts the account portal on its own origin when the base is absolute', () => {
    const base = 'https://account.example.com/account';
    expect(allowed('https://account.example.com/account/onboarding/models', base)).toBe(true);
    expect(allowed(GOOD, base)).toBe(false);
  });

  it('accepts a base with no path', () => {
    const base = 'https://account.example.com';
    expect(allowed('https://account.example.com/onboarding/models', base)).toBe(true);
  });

  it.each([
    ['another origin', 'https://evil.example/account/onboarding/models'],
    ['another scheme', 'http://app.example.com/account/onboarding/models'],
    ['another port', 'https://app.example.com:8443/account/onboarding/models'],
    ['another path', `${ORIGIN}/account/settings`],
    ['the onboarding root without a slash', `${ORIGIN}/account/onboarding`],
    ['a sibling of the base', `${ORIGIN}/accounts/onboarding/models`],
    ['onboarding outside the base', `${ORIGIN}/onboarding/models`],
    ['protocol-relative', '//app.example.com/account/onboarding/models'],
    ['a relative path', '/account/onboarding/models'],
    ['script scheme', 'javascript:alert(1)'],
    ['data scheme', 'data:text/html,<p>hi</p>'],
    ['dot segments out of onboarding', `${ORIGIN}/account/onboarding/../x`],
    ['encoded dot segments', `${ORIGIN}/account/onboarding/%2e%2e/x`],
    ['user and password', 'https://user:pass@app.example.com/account/onboarding/models'],
    ['user only', 'https://user@app.example.com/account/onboarding/models'],
    ['this host as a user name', 'https://app.example.com@evil.example/account/onboarding/models'],
    ['empty', ''],
  ])('rejects %s', (_label, url) => {
    expect(allowed(url)).toBe(false);
  });

  it('rejects everything when no account portal is configured', () => {
    expect(allowed(GOOD, null)).toBe(false);
  });
});

describe('capture and read', () => {
  it('keeps an allowed return and reads it back', () => {
    expect(captureSetupReturn(search(GOOD))).toBe(GOOD);
    expect(readSetupReturn()).toBe(GOOD);
    expect(JSON.parse(sessionStorage.getItem(SETUP_RETURN_KEY) ?? '{}')).toEqual({ url: GOOD, at: NOW });
  });

  it('checks the param against the configured portal', () => {
    expect(captureSetupReturn(search('https://evil.example/account/onboarding/models'))).toBeNull();
    expect(captureSetupReturn(search(`${ORIGIN}/onboarding/models`))).toBeNull();
    expect(readSetupReturn()).toBeNull();
  });

  it('ignores a search without the param', () => {
    expect(captureSetupReturn('?foo=1')).toBeNull();
    expect(readSetupReturn()).toBeNull();
  });

  it('gives the return already kept when the search has none', () => {
    captureSetupReturn(search(GOOD));
    expect(captureSetupReturn('?foo=1')).toBe(GOOD);
  });

  it('ignores an invalid value without dropping the one already kept', () => {
    captureSetupReturn(search(GOOD));
    expect(captureSetupReturn(search('https://evil.example/account/onboarding/x'))).toBe(GOOD);
    expect(readSetupReturn()).toBe(GOOD);
  });

  it('expires an hour after it was kept', () => {
    vi.setSystemTime(0);
    captureSetupReturn(search(GOOD));
    vi.setSystemTime(HOUR - 1);
    expect(readSetupReturn()).toBe(GOOD);
    vi.setSystemTime(HOUR);
    expect(readSetupReturn()).toBeNull();
    expect(sessionStorage.getItem(SETUP_RETURN_KEY)).toBeNull();
  });

  it('checks a stored value again on the way out', () => {
    sessionStorage.setItem(SETUP_RETURN_KEY, JSON.stringify({ url: 'https://evil.example/x', at: NOW }));
    expect(readSetupReturn()).toBeNull();
    expect(sessionStorage.getItem(SETUP_RETURN_KEY)).toBeNull();
  });

  it.each([
    ['not JSON', '{'],
    ['no timestamp', JSON.stringify({ url: GOOD })],
    ['a string timestamp', JSON.stringify({ url: GOOD, at: '1' })],
    ['null', 'null'],
  ])('drops a stored value that is %s', (_label, raw) => {
    sessionStorage.setItem(SETUP_RETURN_KEY, raw);
    expect(readSetupReturn()).toBeNull();
    expect(sessionStorage.getItem(SETUP_RETURN_KEY)).toBeNull();
  });

  it('stays quiet when storage throws', () => {
    const fail = () => {
      throw new DOMException('blocked', 'SecurityError');
    };
    vi.spyOn(Storage.prototype, 'getItem').mockImplementation(fail);
    vi.spyOn(Storage.prototype, 'setItem').mockImplementation(fail);
    vi.spyOn(Storage.prototype, 'removeItem').mockImplementation(fail);

    expect(captureSetupReturn(search(GOOD))).toBeNull();
    expect(readSetupReturn()).toBeNull();
    expect(() => clearSetupReturn()).not.toThrow();
    expect(returnFromSetup('anthropic')).toBe(false);
    expect(assign).not.toHaveBeenCalled();
  });

  it('stays quiet when sessionStorage itself is out of reach', () => {
    vi.spyOn(window, 'sessionStorage', 'get').mockImplementation(() => {
      throw new DOMException('blocked', 'SecurityError');
    });

    expect(captureSetupReturn(search(GOOD))).toBeNull();
    expect(readSetupReturn()).toBeNull();
    expect(() => clearSetupReturn()).not.toThrow();
    expect(returnFromSetup('anthropic')).toBe(false);
    expect(assign).not.toHaveBeenCalled();
  });
});

describe('returnFromSetup', () => {
  /** Keeps `url`, goes back naming `connected`, and gives where it went. */
  const goBack = (url: string, connected?: string | null) => {
    captureSetupReturn(search(url));
    expect(returnFromSetup(connected)).toBe(true);
    expect(assign).toHaveBeenCalledTimes(1);
    const [target] = assign.mock.calls[0];
    assign.mockClear();
    return target;
  };

  it('goes to the kept return with the provider and clears it', () => {
    expect(goBack(`${GOOD}?step=2`, 'deepseek')).toBe(`${GOOD}?step=2&connected=deepseek`);
    expect(sessionStorage.getItem(SETUP_RETURN_KEY)).toBeNull();
  });

  it('follows the return once', () => {
    captureSetupReturn(search(GOOD));
    expect(returnFromSetup('anthropic')).toBe(true);
    expect(returnFromSetup('anthropic')).toBe(false);
    expect(assign).toHaveBeenCalledTimes(1);
  });

  it('keeps the existing query and fragment', () => {
    expect(goBack(`${GOOD}?step=2&from=setup#top`, 'claude-oauth')).toBe(
      `${GOOD}?step=2&from=setup&connected=claude-oauth#top`,
    );
  });

  it('replaces a connected param already there', () => {
    expect(goBack(`${GOOD}?connected=openai`, 'deepseek')).toBe(`${GOOD}?connected=deepseek`);
  });

  it('goes back as it was when nothing was connected', () => {
    expect(goBack(`${GOOD}?step=2`)).toBe(`${GOOD}?step=2`);
    expect(goBack(`${GOOD}?step=2`, null)).toBe(`${GOOD}?step=2`);
  });

  it('does nothing without a kept return', () => {
    expect(returnFromSetup('anthropic')).toBe(false);
    expect(assign).not.toHaveBeenCalled();
  });

  it('does nothing once the kept return has expired', () => {
    captureSetupReturn(search(GOOD));
    vi.setSystemTime(NOW + HOUR);
    expect(returnFromSetup('anthropic')).toBe(false);
    expect(assign).not.toHaveBeenCalled();
  });
});

describe('withoutSetupReturnParam', () => {
  it('drops the param and keeps the others', () => {
    expect(withoutSetupReturnParam(`${search(GOOD)}&foo=1`)).toBe('?foo=1');
    expect(withoutSetupReturnParam(search(GOOD))).toBe('');
  });

  it('is null when there is no param to drop', () => {
    expect(withoutSetupReturnParam('?foo=1')).toBeNull();
    expect(withoutSetupReturnParam('')).toBeNull();
  });
});
