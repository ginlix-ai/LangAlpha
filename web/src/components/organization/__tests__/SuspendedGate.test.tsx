import { afterEach, describe, it, expect, vi } from 'vitest';
import { act, render, screen } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type { User } from '@/types/api';
import { noteIfSuspended, resetAccountSuspended } from '@/lib/accountSuspension';
import { SuspendedGate } from '../SuspendedAccount';

const me: { user: Partial<User> | null } = { user: null };
vi.mock('@/hooks/useUser', () => ({ useUser: () => ({ user: me.user }) }));
vi.mock('@/contexts/AuthContext', () => ({ useAuth: () => ({ logout: vi.fn() }) }));

function renderGate() {
  return render(
    <QueryClientProvider client={new QueryClient()}>
      <SuspendedGate><p>the app</p></SuspendedGate>
    </QueryClientProvider>,
  );
}

afterEach(() => {
  resetAccountSuspended();
  me.user = null;
});

describe('SuspendedGate', () => {
  it('renders the app for an account with no organization', () => {
    me.user = { user_id: 'u' };
    renderGate();
    expect(screen.getByText('the app')).toBeInTheDocument();
  });

  it('renders the app while the rules cannot be read', () => {
    me.user = { user_id: 'u', organization: null, policy: null };
    renderGate();
    expect(screen.getByText('the app')).toBeInTheDocument();
  });

  it('replaces the app once /users/me says the account is suspended', () => {
    me.user = { user_id: 'u', organization: { id: 'o', name: 'Northfield', suspended: true }, policy: null };
    renderGate();
    expect(screen.queryByText('the app')).not.toBeInTheDocument();
    expect(screen.getAllByText(/Northfield/).length).toBeGreaterThan(0);
  });

  it('replaces the app as soon as any refusal reveals the suspension', () => {
    me.user = { user_id: 'u' };
    renderGate();
    expect(screen.getByText('the app')).toBeInTheDocument();
    act(() => { noteIfSuspended({ type: 'account_suspended', organization: 'Northfield' }); });
    expect(screen.queryByText('the app')).not.toBeInTheDocument();
    expect(screen.getAllByText(/Northfield/).length).toBeGreaterThan(0);
  });
});
