import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, act } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter } from 'react-router-dom';
import LoginPage from '@/pages/Login/LoginPage';

// The canvas panes need a real 2d context and are irrelevant to the view
// state machine under test.
vi.mock('@/pages/Login/MarketScanlines', () => ({ default: () => null }));
vi.mock('@/pages/Login/EdgeGrain', () => ({ default: () => null }));

const auth = {
  loginWithEmail: vi.fn(),
  signupWithEmail: vi.fn(),
  loginWithProvider: vi.fn(),
  sendMagicLink: vi.fn(),
  sendPasswordReset: vi.fn(),
  resendConfirmation: vi.fn(),
};

vi.mock('@/contexts/AuthContext', () => ({
  useAuth: () => auth,
}));

/** A promise whose settlement the test controls — the in-flight request. */
function deferred<T>() {
  let resolve!: (v: T) => void;
  let reject!: (e: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

const renderPage = () =>
  render(
    <MemoryRouter>
      <LoginPage />
    </MemoryRouter>
  );

beforeEach(() => {
  vi.clearAllMocks();
});

// The stale-response guard: a submit belongs to the view it started on. If the
// user navigates away while the request is in flight, the late response must
// neither navigate them to check-inbox nor paint its error on the new view.
describe('LoginPage cross-view race guard', () => {
  it('drops a magic-link success that lands after leaving the view', async () => {
    const user = userEvent.setup();
    const request = deferred<{ error: null }>();
    auth.sendMagicLink.mockReturnValue(request.promise);
    renderPage();

    await user.click(screen.getByRole('button', { name: 'Email me a sign-in link' }));
    await user.type(screen.getByPlaceholderText('you@example.com'), 'trader@example.com');
    await user.click(screen.getByRole('button', { name: 'Send sign-in link' }));

    // Leave for the method picker while the request is still in flight.
    await user.click(screen.getByRole('button', { name: '← Other sign-in options' }));
    expect(screen.getByRole('button', { name: 'Continue with email' })).toBeInTheDocument();

    await act(async () => {
      request.resolve({ error: null });
      await request.promise;
    });

    // Still on the method picker — the late success didn't yank the view.
    expect(screen.queryByText('Check your inbox')).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Continue with email' })).toBeInTheDocument();
  });

  it('drops a login failure that lands after switching to signup', async () => {
    const user = userEvent.setup();
    const request = deferred<never>();
    auth.loginWithEmail.mockReturnValue(request.promise);
    renderPage();

    await user.click(screen.getByRole('button', { name: 'Continue with email' }));
    await user.type(screen.getByPlaceholderText('you@example.com'), 'trader@example.com');
    await user.type(screen.getByPlaceholderText('Your password'), 'hunter2!!');
    await user.click(screen.getByRole('button', { name: 'Sign in' }));

    // Switch to signup while the login request is in flight.
    await user.click(screen.getByRole('button', { name: 'Sign up' }));
    expect(screen.getByText('Create your account')).toBeInTheDocument();

    await act(async () => {
      request.reject({ code: 'invalid_credentials', message: 'Invalid login credentials' });
      await request.promise.catch(() => {});
    });

    // The stale failure stays off the signup view.
    expect(screen.queryByText('Incorrect email or password.')).not.toBeInTheDocument();
    expect(screen.getByText('Create your account')).toBeInTheDocument();
  });

  it('still routes to check-inbox when the view is unchanged', async () => {
    const user = userEvent.setup();
    const request = deferred<{ error: null }>();
    auth.sendMagicLink.mockReturnValue(request.promise);
    renderPage();

    await user.click(screen.getByRole('button', { name: 'Email me a sign-in link' }));
    await user.type(screen.getByPlaceholderText('you@example.com'), 'trader@example.com');
    await user.click(screen.getByRole('button', { name: 'Send sign-in link' }));

    await act(async () => {
      request.resolve({ error: null });
      await request.promise;
    });

    expect(screen.getByText('Check your inbox')).toBeInTheDocument();
    expect(screen.getByText(/trader@example\.com/)).toBeInTheDocument();
  });
});

// The magic-link view's back button returns to the method picker, so its label
// must say so; forgot-password returns to the login form and keeps the default.
describe('EmailOnlyView back labels', () => {
  it('labels the magic-link back button with the method picker', async () => {
    const user = userEvent.setup();
    renderPage();

    await user.click(screen.getByRole('button', { name: 'Email me a sign-in link' }));
    expect(screen.getByRole('button', { name: '← Other sign-in options' })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: '← Back to sign in' })).not.toBeInTheDocument();
  });

  it('keeps the login-form label on the forgot-password back button', async () => {
    const user = userEvent.setup();
    renderPage();

    await user.click(screen.getByRole('button', { name: 'Continue with email' }));
    await user.click(screen.getByRole('button', { name: 'Forgot password?' }));
    expect(screen.getByRole('button', { name: '← Back to sign in' })).toBeInTheDocument();
  });
});
