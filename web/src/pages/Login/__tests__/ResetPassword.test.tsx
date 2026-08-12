import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import ResetPassword from '@/pages/Login/ResetPassword';

// The canvas pane needs a real 2d context and is irrelevant here.
vi.mock('@/pages/Login/WavesBackground', () => ({ default: () => null }));

vi.mock('@/config/hostMode', () => ({
  isPlatformMode: true,
  APP_ENTRY_PATH: '/',
}));

const auth = {
  isInitialized: true,
  isLoggedIn: false,
  updatePassword: vi.fn(),
  verifyEmailOtp: vi.fn(),
};

vi.mock('@/contexts/AuthContext', () => ({
  useAuth: () => auth,
}));

const renderPage = () =>
  render(
    <MemoryRouter>
      <ResetPassword />
    </MemoryRouter>
  );

beforeEach(() => {
  vi.clearAllMocks();
  auth.isInitialized = true;
  auth.isLoggedIn = false;
  window.history.replaceState(null, '', '/reset-password');
});

// Recovery email links land here directly: custom templates carry
// `?token_hash=&type=recovery` (verified by the page), default templates a
// PKCE `?code=` the client already exchanged into a session (or never will).
describe('ResetPassword landings', () => {
  it('verifies a token landing and shows the form once the session lands', async () => {
    window.history.replaceState(null, '', '/reset-password?token_hash=abc123&type=recovery');
    auth.verifyEmailOtp.mockResolvedValue({ error: null });
    const { rerender } = renderPage();

    expect(auth.verifyEmailOtp).toHaveBeenCalledWith('abc123', 'recovery');
    // Verified but the session announcement is still in flight — keep
    // loading, never flash the expired card.
    expect(await screen.findByText('Loading...')).toBeInTheDocument();
    expect(screen.queryByText('This reset link is invalid or has expired.')).not.toBeInTheDocument();

    auth.isLoggedIn = true;
    rerender(
      <MemoryRouter>
        <ResetPassword />
      </MemoryRouter>
    );

    expect(await screen.findByText('Set a new password')).toBeInTheDocument();
  });

  it('shows the expired card when the token is rejected', async () => {
    window.history.replaceState(null, '', '/reset-password?token_hash=abc123&type=recovery');
    auth.verifyEmailOtp.mockResolvedValue({ error: new Error('otp_expired') });
    renderPage();

    expect(await screen.findByText('This reset link is invalid or has expired.')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Request a new link' })).toBeInTheDocument();
  });

  it('shows the expired card on a bare landing with no session', async () => {
    renderPage();

    expect(await screen.findByText('This reset link is invalid or has expired.')).toBeInTheDocument();
    expect(auth.verifyEmailOtp).not.toHaveBeenCalled();
  });

  it('shows the form on a bare landing with a session (auto-exchanged code)', async () => {
    auth.isLoggedIn = true;
    renderPage();

    expect(await screen.findByText('Set a new password')).toBeInTheDocument();
  });
});
