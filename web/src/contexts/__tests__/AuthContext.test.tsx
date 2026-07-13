import React, { type ReactElement } from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor, act } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

// Enable platform auth code path (AuthProvider checks VITE_HOST_MODE)
// Must be set before the dynamic import below.
vi.stubEnv('VITE_HOST_MODE', 'platform');
vi.stubEnv('VITE_SUPABASE_URL', 'https://test.supabase.co');

// Mock supabase with a functional mock auth object
const mockGetSession = vi.fn().mockResolvedValue({ data: { session: null } });
const mockOnAuthStateChange = vi.fn().mockReturnValue({
  data: { subscription: { unsubscribe: vi.fn() } },
});

const mockSignUp = vi.fn().mockResolvedValue({ data: { user: null, session: null }, error: null });
const mockSignInWithOtp = vi.fn().mockResolvedValue({ data: { user: null, session: null }, error: null });
const mockResetPasswordForEmail = vi.fn().mockResolvedValue({ data: {}, error: null });
const mockResend = vi.fn().mockResolvedValue({ data: { user: null, session: null }, error: null });
const mockVerifyOtp = vi.fn().mockResolvedValue({ data: { user: null, session: null }, error: null });
const mockUpdateUser = vi.fn().mockResolvedValue({ data: { user: null }, error: null });

vi.mock('../../lib/supabase', () => ({
  supabase: {
    auth: {
      getSession: (...args: unknown[]) => mockGetSession(...args),
      onAuthStateChange: (...args: unknown[]) => mockOnAuthStateChange(...args),
      signInWithPassword: vi.fn(),
      signUp: (...args: unknown[]) => mockSignUp(...args),
      signInWithOAuth: vi.fn(),
      signOut: vi.fn(),
      signInWithOtp: (...args: unknown[]) => mockSignInWithOtp(...args),
      resetPasswordForEmail: (...args: unknown[]) => mockResetPasswordForEmail(...args),
      resend: (...args: unknown[]) => mockResend(...args),
      verifyOtp: (...args: unknown[]) => mockVerifyOtp(...args),
      updateUser: (...args: unknown[]) => mockUpdateUser(...args),
    },
  },
}));

vi.mock('../../api/client', () => ({
  setTokenGetter: vi.fn(),
  setTokenRefresher: vi.fn(),
}));

// Spy on the module-level nav stores so we can assert sign-out resets them.
const mockResetNavPanelExpansion = vi.fn();
const mockResetStableNavOrder = vi.fn();
const mockResetSharedWorkspaceThreads = vi.fn();

vi.mock('@/pages/ChatAgent/components/navExpansionStore', () => ({
  resetNavPanelExpansion: () => mockResetNavPanelExpansion(),
}));
vi.mock('@/pages/ChatAgent/hooks/useNavigationData', () => ({
  resetStableNavOrder: () => mockResetStableNavOrder(),
  resetSharedWorkspaceThreads: () => mockResetSharedWorkspaceThreads(),
}));

// Dynamic import so mocks and env stubs are applied first
const { AuthProvider, useAuth } = await import('../AuthContext');
type AuthContextValue = ReturnType<typeof useAuth>;

function TestConsumer() {
  const auth = useAuth();
  return (
    <div>
      <span data-testid="userId">{auth.userId ?? 'none'}</span>
      <span data-testid="isLoggedIn">{String(auth.isLoggedIn)}</span>
      <span data-testid="isInitialized">{String(auth.isInitialized)}</span>
    </div>
  );
}

function renderWithQueryClient(ui: ReactElement) {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return render(
    <QueryClientProvider client={queryClient}>{ui}</QueryClientProvider>
  );
}

describe('AuthContext', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockGetSession.mockResolvedValue({ data: { session: null } });
    mockOnAuthStateChange.mockReturnValue({
      data: { subscription: { unsubscribe: vi.fn() } },
    });
  });

  describe('when no session exists', () => {
    it('shows isInitialized true and isLoggedIn false after bootstrap', async () => {
      renderWithQueryClient(
        <AuthProvider>
          <TestConsumer />
        </AuthProvider>
      );

      await waitFor(() =>
        expect(screen.getByTestId('isInitialized').textContent).toBe('true')
      );
      expect(screen.getByTestId('isLoggedIn').textContent).toBe('false');
      expect(screen.getByTestId('userId').textContent).toBe('none');
    });
  });

  describe('when a session exists', () => {
    it('shows isLoggedIn true and exposes userId', async () => {
      mockGetSession.mockResolvedValue({
        data: {
          session: {
            user: { id: 'user-abc' },
            access_token: 'tok-123',
          },
        },
      });

      renderWithQueryClient(
        <AuthProvider>
          <TestConsumer />
        </AuthProvider>
      );

      await waitFor(() =>
        expect(screen.getByTestId('isLoggedIn').textContent).toBe('true')
      );
      expect(screen.getByTestId('userId').textContent).toBe('user-abc');
    });
  });

  describe('useAuth', () => {
    it('throws when used outside AuthProvider', () => {
      function BadConsumer() {
        useAuth();
        return null;
      }

      const spy = vi.spyOn(console, 'error').mockImplementation(() => {});
      expect(() => render(<BadConsumer />)).toThrow(
        'useAuth must be used within AuthProvider'
      );
      spy.mockRestore();
    });
  });

  describe('AuthProvider renders children', () => {
    it('renders child components', async () => {
      renderWithQueryClient(
        <AuthProvider>
          <div data-testid="child">Hello</div>
        </AuthProvider>
      );

      await waitFor(() =>
        expect(screen.getByTestId('child').textContent).toBe('Hello')
      );
    });
  });

  describe('email-flow methods', () => {
    // Capture the context value so the wrappers can be called directly.
    let auth: AuthContextValue;
    function Capture() {
      auth = useAuth();
      return null;
    }

    const CONFIRM_URL = window.location.origin + '/auth/confirm';
    const RESET_URL = window.location.origin + '/reset-password';

    beforeEach(async () => {
      renderWithQueryClient(
        <AuthProvider>
          <Capture />
        </AuthProvider>
      );
      await waitFor(() => expect(auth?.isInitialized).toBe(true));
    });

    it('signupWithEmail passes emailRedirectTo to the confirm route', async () => {
      await auth.signupWithEmail('a@b.co', 'secret123', 'Alice');
      expect(mockSignUp).toHaveBeenCalledWith({
        email: 'a@b.co',
        password: 'secret123',
        options: { data: { name: 'Alice' }, emailRedirectTo: CONFIRM_URL },
      });
    });

    it('sendMagicLink creates accounts and redirects to the confirm route', async () => {
      await auth.sendMagicLink('a@b.co');
      expect(mockSignInWithOtp).toHaveBeenCalledWith({
        email: 'a@b.co',
        options: { shouldCreateUser: true, emailRedirectTo: CONFIRM_URL },
      });
    });

    it('sendPasswordReset lands recovery links on the reset form', async () => {
      await auth.sendPasswordReset('a@b.co');
      expect(mockResetPasswordForEmail).toHaveBeenCalledWith('a@b.co', {
        redirectTo: RESET_URL,
      });
    });

    it('resendConfirmation re-sends the signup email', async () => {
      await auth.resendConfirmation('a@b.co');
      expect(mockResend).toHaveBeenCalledWith({
        type: 'signup',
        email: 'a@b.co',
        options: { emailRedirectTo: CONFIRM_URL },
      });
    });

    it('verifyEmailOtp forwards the token hash and type', async () => {
      await auth.verifyEmailOtp('hash-123', 'recovery');
      expect(mockVerifyOtp).toHaveBeenCalledWith({ token_hash: 'hash-123', type: 'recovery' });
    });

    it('updatePassword forwards the new password', async () => {
      await auth.updatePassword('newpass123');
      expect(mockUpdateUser).toHaveBeenCalledWith({ password: 'newpass123' });
    });
  });

  describe('onAuthStateChange subscription', () => {
    it('subscribes to auth state changes on mount', async () => {
      renderWithQueryClient(
        <AuthProvider>
          <TestConsumer />
        </AuthProvider>
      );

      await waitFor(() =>
        expect(screen.getByTestId('isInitialized').textContent).toBe('true')
      );
      expect(mockOnAuthStateChange).toHaveBeenCalled();
    });

    // Regression: the module-level nav stores live on globalThis and survive
    // logout (no page reload), so they must be reset on sign-out or one user's
    // folders/thread lists leak into the next user's session on a shared tab.
    it('resets the module-level nav stores on sign-out', async () => {
      renderWithQueryClient(
        <AuthProvider>
          <TestConsumer />
        </AuthProvider>
      );

      await waitFor(() => expect(mockOnAuthStateChange).toHaveBeenCalled());

      const handler = mockOnAuthStateChange.mock.calls[0][0] as (
        event: string,
        session: unknown,
      ) => void;
      // Wrap in act(): the handler drives AuthProvider state updates, which
      // React 19 warns about if flushed outside an act() boundary.
      await act(async () => {
        handler('SIGNED_OUT', null);
      });

      expect(mockResetNavPanelExpansion).toHaveBeenCalledTimes(1);
      expect(mockResetStableNavOrder).toHaveBeenCalledTimes(1);
      expect(mockResetSharedWorkspaceThreads).toHaveBeenCalledTimes(1);
    });
  });
});
