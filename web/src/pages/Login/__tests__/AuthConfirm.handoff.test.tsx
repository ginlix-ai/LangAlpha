import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, act } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter } from 'react-router-dom';
import AuthConfirm from '@/pages/Login/AuthConfirm';

vi.mock('@/pages/Login/WavesBackground', () => ({ default: () => null }));

vi.mock('@/config/hostMode', () => ({
  isPlatformMode: true,
  APP_ENTRY_PATH: '/',
}));

const auth = {
  verifyEmailOtp: vi.fn(),
  isLoggedIn: false,
};

vi.mock('@/contexts/AuthContext', () => ({
  useAuth: () => auth,
}));

const renderPage = () =>
  render(
    <MemoryRouter>
      <AuthConfirm />
    </MemoryRouter>
  );

/**
 * Catch the scheme navigation, which jsdom will not perform. `assign` is an own
 * non-configurable property of Location, so the whole object is swapped rather
 * than the one method spied.
 */
let navigations: string[] = [];
const realLocation = window.location;

function landOn(pathname: string, search: string) {
  navigations = [];
  Object.defineProperty(window, 'location', {
    configurable: true,
    value: {
      href: `http://localhost${pathname}${search}`,
      origin: 'http://localhost',
      pathname,
      search,
      hash: '',
      assign: (url: string) => { navigations.push(url); },
    },
  });
}

beforeEach(() => {
  vi.clearAllMocks();
  auth.isLoggedIn = false;
  auth.verifyEmailOtp.mockResolvedValue({ data: {}, error: null });
});

afterEach(() => {
  Object.defineProperty(window, 'location', { configurable: true, value: realLocation });
});

// A shell-marked link is redeemed by the app, not by the browser that opened
// the mail. Only one side can: verifyOtp consumes the token, so a browser that
// verifies "just to be safe" is a browser that breaks the handoff it started.
describe('AuthConfirm shell handoff', () => {
  it('passes a marked link to the app without consuming the token', async () => {
    landOn('/auth/confirm/desktop', '?token_hash=abc123&type=email');
    await act(async () => { renderPage(); });

    expect(auth.verifyEmailOtp).not.toHaveBeenCalled();
    expect(navigations).toEqual(['langalpha://callback?token_hash=abc123&type=email']);
    expect(screen.getByText('Opening LangAlpha…')).toBeInTheDocument();
  });

  it('redeems it here when the user says nothing opened', async () => {
    const user = userEvent.setup();
    landOn('/auth/confirm/desktop', '?token_hash=abc123&type=email');
    await act(async () => { renderPage(); });

    await act(async () => {
      await user.click(screen.getByRole('button', { name: 'Continue in this browser' }));
    });

    expect(auth.verifyEmailOtp).toHaveBeenCalledWith('abc123', 'email');
  });

  // The browser path is the common one and must be untouched by any of this.
  it('verifies an unmarked link immediately, as every web signup does', async () => {
    landOn('/auth/confirm', '?token_hash=abc123&type=email');
    await act(async () => { renderPage(); });

    expect(auth.verifyEmailOtp).toHaveBeenCalledWith('abc123', 'email');
    expect(navigations).toEqual([]);
  });
});
