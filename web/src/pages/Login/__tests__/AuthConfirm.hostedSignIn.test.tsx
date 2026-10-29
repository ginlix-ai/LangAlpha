import { afterAll, afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act, render } from '@testing-library/react';
import { MemoryRouter, Route, Routes } from 'react-router';
import AuthConfirm from '@/pages/Login/AuthConfirm';

vi.mock('@/pages/Login/EdgeGrain', () => ({ default: () => null }));
vi.mock('@/config/hostMode', () => ({ isPlatformMode: true, APP_ENTRY_PATH: '/' }));
vi.mock('@/config/supportLinks', () => ({ PLATFORM_URL: 'https://platform.example.com' }));
// A shell with a portal to open: the edition a hosted signup comes from.
vi.mock('@/lib/desktop', () => ({
  desktop: { capabilities: ['integration-login', 'integration-install', 'console-return'] },
}));

const auth = { verifyEmailOtp: vi.fn(), isLoggedIn: false };
vi.mock('@/contexts/AuthContext', () => ({ useAuth: () => auth }));

/** `replace` is an own non-configurable property of Location, so the object is swapped whole. */
const replace = vi.fn();
const realLocation = window.location;
afterAll(() => {
  Object.defineProperty(window, 'location', { configurable: true, value: realLocation });
});

beforeEach(() => {
  vi.useFakeTimers();
  sessionStorage.clear();
  auth.verifyEmailOtp.mockResolvedValue({ data: {}, error: null });
  // The link the shell delivers when it starts on one: `langalpha://callback`
  // resolved onto this app, since the window had not loaded the portal yet.
  Object.defineProperty(window, 'location', {
    configurable: true,
    value: {
      href: 'https://app.example.com/auth/confirm?token_hash=abc&type=email',
      origin: 'https://app.example.com',
      pathname: '/auth/confirm',
      search: '?token_hash=abc&type=email',
      hash: '',
      replace,
    },
  });
});

afterEach(() => {
  vi.useRealTimers();
  replace.mockReset();
  auth.isLoggedIn = false;
});

describe('AuthConfirm in the desktop app', () => {
  it("hands a confirmed signup to the portal's sign-in rather than the dashboard", async () => {
    const { getByTestId } = render(
      <MemoryRouter initialEntries={['/auth/confirm?token_hash=abc&type=email']}>
        <Routes>
          <Route path="/auth/confirm" element={<AuthConfirm />} />
          <Route path="/dashboard" element={<p data-testid="dashboard" />} />
        </Routes>
      </MemoryRouter>,
    );
    await act(async () => {});
    expect(auth.verifyEmailOtp).toHaveBeenCalledWith('abc', 'email');

    await act(async () => {
      vi.advanceTimersByTime(1500);
    });

    expect(replace).toHaveBeenCalledWith('https://platform.example.com/login');
    expect(() => getByTestId('dashboard')).toThrow();
  });

  // The shell hands an email link in once per click, so clicking it again
  // mid-onboarding loads a token the first click spent into a signed-in window.
  it('carries a signed-in window on when its link was already spent', async () => {
    auth.isLoggedIn = true;
    auth.verifyEmailOtp.mockResolvedValue({
      data: {},
      error: { code: 'otp_expired', message: 'Email link is invalid or has expired' },
    });
    const { queryByText } = render(
      <MemoryRouter initialEntries={['/auth/confirm?token_hash=abc&type=email']}>
        <Routes>
          <Route path="/auth/confirm" element={<AuthConfirm />} />
        </Routes>
      </MemoryRouter>,
    );
    await act(async () => {});
    expect(queryByText('This link is invalid or has expired.')).toBeNull();

    await act(async () => {
      vi.advanceTimersByTime(1500);
    });

    expect(replace).toHaveBeenCalledWith('https://platform.example.com/login');
  });
});
