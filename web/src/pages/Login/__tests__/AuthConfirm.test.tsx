import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, act } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import AuthConfirm from '@/pages/Login/AuthConfirm';

// The canvas pane needs a real 2d context and is irrelevant here.
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

beforeEach(() => {
  vi.clearAllMocks();
  auth.isLoggedIn = false;
  window.history.replaceState(null, '', '/auth/confirm');
});

afterEach(() => {
  vi.useRealTimers();
});

// The supabase client auto-exchanges a PKCE `?code=` on load and strips it
// from the URL — when that wins the race against this component's effect, the
// landing looks bare even though the sign-in succeeded. A bare landing must
// wait for the session, not instantly claim the link is invalid.
describe('AuthConfirm no-token landings', () => {
  it('confirms a bare landing when the session is already established', async () => {
    auth.isLoggedIn = true;
    renderPage();

    expect(await screen.findByText('Email confirmed — signing you in…')).toBeInTheDocument();
    expect(screen.queryByText('This link is invalid or has expired.')).not.toBeInTheDocument();
    expect(auth.verifyEmailOtp).not.toHaveBeenCalled();
  });

  it('confirms a bare landing once the in-flight exchange lands', async () => {
    const { rerender } = renderPage();
    expect(screen.getByText('Confirming your email…')).toBeInTheDocument();

    auth.isLoggedIn = true;
    rerender(
      <MemoryRouter>
        <AuthConfirm />
      </MemoryRouter>
    );

    expect(await screen.findByText('Email confirmed — signing you in…')).toBeInTheDocument();
  });

  it('fails fast when the verify endpoint redirected with error params', () => {
    window.history.replaceState(
      null,
      '',
      '/auth/confirm?error=access_denied&error_code=otp_expired#error=access_denied&error_code=otp_expired'
    );
    renderPage();

    expect(screen.getByText('This link is invalid or has expired.')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Request a new link' })).toBeInTheDocument();
    expect(auth.verifyEmailOtp).not.toHaveBeenCalled();
  });

  it('still errors a bare landing after the bounded wait when no session arrives', () => {
    vi.useFakeTimers();
    renderPage();
    expect(screen.getByText('Confirming your email…')).toBeInTheDocument();

    act(() => {
      vi.advanceTimersByTime(8000);
    });

    expect(screen.getByText('This link is invalid or has expired.')).toBeInTheDocument();
  });

  it('fails fast on a truncated link carrying half a token pair', () => {
    window.history.replaceState(null, '', '/auth/confirm?token_hash=abc123');
    renderPage();

    expect(screen.getByText('This link is invalid or has expired.')).toBeInTheDocument();
    expect(auth.verifyEmailOtp).not.toHaveBeenCalled();
  });
});
