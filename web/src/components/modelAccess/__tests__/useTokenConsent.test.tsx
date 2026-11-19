/**
 * The agreement a sheet records before it asks for a token goes through the
 * real custody choice, whose answer for an account that has since changed is
 * refused: no token is asked for on the agreement of the account before.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { QueryClient } from '@tanstack/react-query';
import { renderHookWithProviders } from '@/test/utils';
import { queryKeys } from '@/lib/queryKeys';
import { runAuthResets } from '@/lib/authResets';
import type { ClaudeCodeCustody, ClaudeCodeStatus } from '@/api/model';
import type { TokenRoutes } from '../tokenTarget';

vi.mock('@/api/client', () => ({ api: { get: vi.fn(), put: vi.fn() } }));
const account = vi.hoisted(() => ({ userId: 'u1' }));
vi.mock('@/hooks/useUser', () => ({ useUser: () => ({ user: { user_id: account.userId }, isLoading: false, isError: false }) }));
import { api } from '@/api/client';
import { useTokenConsent } from '../useTokenConsent';

const STATUS: ClaudeCodeStatus = { enabled: true, state: 'off', email: null, expires_at: null, saved: false };
const LOCAL: ClaudeCodeCustody = { mode: 'local', offered: ['stored'], blocked: false, runs_on: null, refusal: null };

function run() {
  (api.get as Mock).mockImplementation(async (url: string) => {
    if (url === '/api/v1/claude-code/status') return { data: STATUS };
    if (url === '/api/v1/claude-code/custody') return { data: LOCAL };
    throw new Error(url);
  });
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  queryClient.setQueryData(queryKeys.features.list(), [{ key: 'claude_code_runtime', enabled: true }]);
  const routes = { start: vi.fn(async () => ({ authorize_url: 'https://claude.ai/oauth', expires_at: '2099-01-01T00:00:00Z' })) } as unknown as TokenRoutes;
  return { routes, ...renderHookWithProviders(() => useTokenConsent(routes, true), { queryClient }) };
}

describe('useTokenConsent', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    account.userId = 'u1';
  });

  it('clears the box for another account signing in here', async () => {
    const { result, rerender } = run();
    act(() => result.current.form?.onAgree(true));
    expect(result.current.form?.agreed).toBe(true);
    account.userId = 'u2';
    rerender();
    expect(result.current.form?.agreed).toBe(false);
    expect(result.current.waiting).toBe(true);
  });

  it('asks for no token where the account changed while the agreement was recorded', async () => {
    let answer!: (v: unknown) => void;
    (api.put as Mock).mockImplementation(() => new Promise((resolve) => { answer = resolve; }));
    const { routes, result } = run();
    await waitFor(() => expect(api.get).toHaveBeenCalledWith('/api/v1/claude-code/custody'));
    act(() => result.current.form?.onAgree(true));
    let outcome: Promise<unknown> = Promise.resolve();
    act(() => { outcome = result.current.routes.start().then(() => 'sent', () => 'held'); });
    await waitFor(() => expect(api.put).toHaveBeenCalled());
    runAuthResets();
    await act(async () => { answer({ data: { ...LOCAL, mode: 'stored' } }); });
    expect(await outcome).toBe('held');
    expect(routes.start).not.toHaveBeenCalled();
  });
});
