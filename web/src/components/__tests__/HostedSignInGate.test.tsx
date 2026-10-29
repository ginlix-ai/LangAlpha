import { afterAll, afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act, render, screen } from '@testing-library/react';

/** Which shell the page runs in: one with a portal, one without, or none (a browser). */
const env = vi.hoisted(() => ({
  shell: 'saas' as 'saas' | 'oss' | null,
  platformUrl: '/account' as string | null,
}));
vi.mock('@/lib/desktop', () => ({
  get desktop() {
    if (!env.shell) return undefined;
    return { capabilities: env.shell === 'saas' ? ['integration-login', 'integration-install', 'console-return'] : [] };
  },
}));
vi.mock('@/config/supportLinks', () => ({
  get PLATFORM_URL() {
    return env.platformUrl;
  },
}));

import { HostedSignInGate } from '../HostedSignInGate';
import { forgetHostedSignIn, hostedSignInReturn, landAfterSignIn } from '@/lib/hostedSignIn';

/** `replace` is an own non-configurable property of Location, so the object is swapped whole. */
const replace = vi.fn();
const realLocation = window.location;
Object.defineProperty(window, 'location', {
  configurable: true,
  value: { href: 'https://app.example.com/', origin: 'https://app.example.com', replace },
});
afterAll(() => {
  Object.defineProperty(window, 'location', { configurable: true, value: realLocation });
});

const APP_PAGE = "this app's sign-in";
const renderEntry = () =>
  render(
    <HostedSignInGate>
      <p>{APP_PAGE}</p>
    </HostedSignInGate>,
  );

beforeEach(() => {
  env.shell = 'saas';
  env.platformUrl = '/account';
  sessionStorage.clear();
  vi.useFakeTimers();
  vi.setSystemTime(1_000_000);
  vi.spyOn(console, 'warn').mockImplementation(() => {});
});

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
  replace.mockReset();
});

describe('HostedSignInGate', () => {
  it("sends a signed-out desktop window to the portal's sign-in instead of this app's", () => {
    renderEntry();

    expect(replace).toHaveBeenCalledWith('/account/login');
    expect(screen.queryByText(APP_PAGE)).toBeNull();
  });

  it('uses an absolute portal address as given', () => {
    env.platformUrl = 'https://platform.example.com';
    renderEntry();

    expect(replace).toHaveBeenCalledWith('https://platform.example.com/login');
  });

  it.each([
    ['a browser', null, '/account'],
    ['the self-hosted edition, even with the portal on its own server', 'oss', '/account'],
    ['the self-hosted edition with the portal elsewhere', 'oss', 'https://platform.example.com'],
  ] as const)("keeps this app's page in %s", (_, shell, platformUrl) => {
    env.shell = shell;
    env.platformUrl = platformUrl;
    renderEntry();

    expect(screen.getByText(APP_PAGE)).toBeTruthy();
    expect(replace).not.toHaveBeenCalled();
  });

  it("keeps this app's page in a build with no portal", () => {
    env.platformUrl = null;
    renderEntry();

    expect(screen.getByText(APP_PAGE)).toBeTruthy();
    expect(replace).not.toHaveBeenCalled();
  });

  it('signs in here when the portal sends the window straight back signed out', () => {
    renderEntry().unmount();
    replace.mockClear();

    vi.setSystemTime(1_000_000 + 5_000);
    renderEntry();

    expect(screen.getByText(APP_PAGE)).toBeTruthy();
    expect(replace).not.toHaveBeenCalled();
  });

  it('goes to the portal again when the user comes back later, by Back or Home', () => {
    renderEntry().unmount();
    replace.mockClear();

    vi.setSystemTime(1_000_000 + 60_000);
    renderEntry();

    expect(replace).toHaveBeenCalledWith('/account/login');
  });

  it('goes to the portal again for a sign-out right after the portal signed the window in', () => {
    renderEntry().unmount();
    replace.mockClear();
    // Back from the portal with a session, which the app drops the note for.
    forgetHostedSignIn();

    vi.setSystemTime(1_000_000 + 5_000);
    renderEntry();

    expect(replace).toHaveBeenCalledWith('/account/login');
  });

  it("keeps this app's page when storage cannot be read", () => {
    vi.spyOn(Storage.prototype, 'getItem').mockImplementation(() => {
      throw new Error('denied');
    });
    renderEntry();

    expect(screen.getByText(APP_PAGE)).toBeTruthy();
    expect(replace).not.toHaveBeenCalled();
  });

  it('stays put when the hand-off cannot be recorded, then shows its own page', () => {
    vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => {
      throw new Error('full');
    });
    renderEntry();

    expect(replace).not.toHaveBeenCalled();
    act(() => vi.advanceTimersByTime(10_000));
    expect(screen.getByText(APP_PAGE)).toBeTruthy();
  });

  it('shows its own page when the shell holds the navigation', () => {
    renderEntry();
    expect(replace).toHaveBeenCalledOnce();
    expect(screen.queryByText(APP_PAGE)).toBeNull();

    // Still here: nothing unloaded the page.
    act(() => vi.advanceTimersByTime(10_000));
    expect(screen.getByText(APP_PAGE)).toBeTruthy();
  });
});

describe('the page a sign-in was sent for', () => {
  const askFor = (search: string | undefined) => {
    (window.location as { search?: string }).search = search;
  };
  afterEach(() => askFor(undefined));

  it('keeps it across the portal trip until the window is signed in', () => {
    askFor('?redirect=%2Fs%2Fshare-1');
    renderEntry();

    expect(replace).toHaveBeenCalledWith('/account/login');
    expect(hostedSignInReturn()).toBe('/s/share-1');

    // Back with a session, which the app clears the notes for.
    forgetHostedSignIn();
    expect(hostedSignInReturn()).toBeNull();
  });

  it('drops an earlier one when the next trip is for no page', () => {
    askFor('?redirect=%2Fs%2Fshare-1');
    renderEntry().unmount();
    askFor(undefined);

    vi.setSystemTime(1_000_000 + 60_000);
    renderEntry();

    expect(replace).toHaveBeenCalledTimes(2);
    expect(hostedSignInReturn()).toBeNull();
  });
});

describe('landAfterSignIn', () => {
  it("continues a desktop sign-in on the portal's sign-in, which routes the account", () => {
    const navigate = vi.fn();
    landAfterSignIn(navigate);

    expect(replace).toHaveBeenCalledWith('/account/login');
    expect(navigate).not.toHaveBeenCalled();
  });

  it('goes to the dashboard in a browser', () => {
    env.shell = null;
    const navigate = vi.fn();
    landAfterSignIn(navigate);

    expect(navigate).toHaveBeenCalledWith('/dashboard', { replace: true });
    expect(replace).not.toHaveBeenCalled();
  });

  it('goes to the dashboard when the hand-off cannot be recorded', () => {
    vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => {
      throw new Error('full');
    });
    const navigate = vi.fn();
    landAfterSignIn(navigate);

    expect(navigate).toHaveBeenCalledWith('/dashboard', { replace: true });
    expect(replace).not.toHaveBeenCalled();
  });
});
