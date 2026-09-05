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

const renderPage = () =>
  render(
    <MemoryRouter>
      <LoginPage />
    </MemoryRouter>
  );

/** Walks the method picker to a filled-in signup form and submits it. */
async function submitSignup(user: ReturnType<typeof userEvent.setup>) {
  await user.click(screen.getByRole('button', { name: 'Sign up' }));
  await user.type(screen.getByPlaceholderText('Your name'), 'Trader');
  await user.type(screen.getByPlaceholderText('you@example.com'), 'trader@example.com');
  await user.type(screen.getByPlaceholderText('Choose a password'), 'Probe12345');
  await user.type(screen.getByPlaceholderText('Confirm password'), 'Probe12345');
  await act(async () => {
    await user.click(screen.getByRole('button', { name: 'Sign up' }));
  });
}

beforeEach(() => {
  vi.clearAllMocks();
});

// A signup that resolves without an error means the confirmation mail is sent,
// so the handler has to land somewhere for every response shape. It used to
// require `data.user`, which auth-js 2.106.1 answered as null for exactly this
// call, leaving the form on screen saying nothing while the account existed.
describe('LoginPage signup outcomes', () => {
  it('routes to check-inbox when the client loses the user object', async () => {
    const user = userEvent.setup();
    auth.signupWithEmail.mockResolvedValue({
      data: { user: null, session: null },
      error: null,
    });
    renderPage();

    await submitSignup(user);

    expect(screen.getByText('Check your inbox')).toBeInTheDocument();
    expect(screen.getByText(/trader@example\.com/)).toBeInTheDocument();
  });

  it('routes to check-inbox on the ordinary confirmation-required response', async () => {
    const user = userEvent.setup();
    auth.signupWithEmail.mockResolvedValue({
      data: {
        user: { id: 'u_1', email: 'trader@example.com', identities: [{ provider: 'email' }] },
        session: null,
      },
      error: null,
    });
    renderPage();

    await submitSignup(user);

    expect(screen.getByText('Check your inbox')).toBeInTheDocument();
  });

  it('reports an existing account instead of check-inbox on empty identities', async () => {
    const user = userEvent.setup();
    auth.signupWithEmail.mockResolvedValue({
      data: { user: { id: 'u_1', email: 'trader@example.com', identities: [] }, session: null },
      error: null,
    });
    renderPage();

    await submitSignup(user);

    expect(screen.getByText('An account with this email already exists.')).toBeInTheDocument();
    expect(screen.queryByText('Check your inbox')).not.toBeInTheDocument();
  });

  it('stays put when the signup came back already signed in', async () => {
    const user = userEvent.setup();
    auth.signupWithEmail.mockResolvedValue({
      data: {
        user: { id: 'u_1', email: 'trader@example.com', identities: [{ provider: 'email' }] },
        session: { access_token: 'tok' },
      },
      error: null,
    });
    renderPage();

    await submitSignup(user);

    expect(screen.queryByText('Check your inbox')).not.toBeInTheDocument();
  });
});
