import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, act } from '@testing-library/react';
import { MemoryRouter, Route, Routes } from 'react-router';
import AuthConfirm from '@/pages/Login/AuthConfirm';

// The canvas pane needs a real 2d context and is irrelevant here.
vi.mock('@/pages/Login/EdgeGrain', () => ({ default: () => null }));

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

describe('AuthConfirm with a session already here', () => {
  it('takes a spent link to the app rather than to an error', async () => {
    vi.useFakeTimers();
    auth.isLoggedIn = true;
    auth.verifyEmailOtp.mockResolvedValue({
      data: {},
      error: { code: 'otp_expired', message: 'Email link is invalid or has expired' },
    });
    window.history.replaceState(null, '', '/auth/confirm?token_hash=abc123&type=signup');
    render(
      <MemoryRouter initialEntries={['/auth/confirm']}>
        <Routes>
          <Route path="/auth/confirm" element={<AuthConfirm />} />
          <Route path="/dashboard" element={<p>dashboard</p>} />
        </Routes>
      </MemoryRouter>
    );
    await act(async () => {});

    expect(auth.verifyEmailOtp).toHaveBeenCalledWith('abc123', 'signup');
    expect(screen.queryByText('This link is invalid or has expired.')).not.toBeInTheDocument();
    expect(screen.getByText('Email confirmed — signing you in…')).toBeInTheDocument();

    await act(async () => {
      vi.advanceTimersByTime(1500);
    });
    expect(screen.getByText('dashboard')).toBeInTheDocument();
  });

  describe('loaded on top of another page', () => {
    // The Navigation API, which jsdom lacks: the page Back leads to.
    const behind = (path: string) => {
      const here = { url: window.location.href, index: 1 };
      Object.defineProperty(window, 'navigation', {
        configurable: true,
        value: { currentEntry: here, entries: () => [{ url: new URL(path, window.location.href).href, index: 0 }, here] },
      });
    };
    let back: ReturnType<typeof vi.spyOn>;

    beforeEach(() => {
      back = vi.spyOn(window.history, 'back').mockImplementation(() => {});
    });
    afterEach(() => {
      back.mockRestore();
      delete (window as { navigation?: unknown }).navigation;
    });

    const spentLink = async () => {
      vi.useFakeTimers();
      auth.isLoggedIn = true;
      auth.verifyEmailOtp.mockResolvedValue({
        data: {},
        error: { code: 'otp_expired', message: 'Email link is invalid or has expired' },
      });
      window.history.replaceState(null, '', '/auth/confirm?token_hash=abc123&type=signup');
      render(
        <MemoryRouter initialEntries={['/auth/confirm']}>
          <Routes>
            <Route path="/auth/confirm" element={<AuthConfirm />} />
            <Route path="/dashboard" element={<p>dashboard</p>} />
          </Routes>
        </MemoryRouter>
      );
      await act(async () => {});
      await act(async () => {
        vi.advanceTimersByTime(1500);
      });
    };

    it('goes back to the page a spent link interrupted', async () => {
      behind('/chat/t/thread-1');
      await spentLink();

      expect(back).toHaveBeenCalledOnce();
      expect(screen.queryByText('dashboard')).not.toBeInTheDocument();
    });

    it.each([
      '/auth/confirm?token_hash=old&type=signup',
      '/callback?token_hash=old&type=signup',
      '/reset-password?token_hash=old&type=recovery',
    ])(
      'goes on to the app rather than back to the email link at %s',
      async (path) => {
        behind(path);
        await spentLink();

        expect(back).not.toHaveBeenCalled();
        expect(screen.getByText('dashboard')).toBeInTheDocument();
      }
    );
  });
});
