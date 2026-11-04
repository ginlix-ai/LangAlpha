/**
 * How Claude Code runs on cloud computers, where the server lets the user
 * choose: a server that offers no choice answers 404, which reads as none,
 * and a choice made is the answer at once.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { QueryClient } from '@tanstack/react-query';
import { renderHookWithProviders } from '@/test/utils';
import { queryKeys } from '@/lib/queryKeys';
import type { ClaudeCodeCustody, ClaudeCodeStatus } from '@/api/model';

vi.mock('@/api/client', () => ({ api: { get: vi.fn(), put: vi.fn() } }));
import { api } from '@/api/client';
import { useClaudeCodeCustody } from '../useClaudeCodeCustody';

const OWN_ONLY: ClaudeCodeStatus = { enabled: true, state: 'off', email: null, expires_at: null, saved: false };
const LOCAL: ClaudeCodeCustody = { mode: 'local', api_key_saved: false, runs_on: null, refusal: null };
const notFound = () => Object.assign(new Error('Request failed with status code 404'), { response: { status: 404, data: {} } });

/** The server's answers by route; \`custody\` a thunk so a 404 can be thrown. */
function serve(status: ClaudeCodeStatus, custody: () => Promise<ClaudeCodeCustody>) {
  (api.get as Mock).mockImplementation(async (url: string) => {
    if (url === '/api/v1/claude-code/status') return { data: status };
    if (url === '/api/v1/claude-code/custody') return { data: await custody() };
    throw new Error(url);
  });
}

function run() {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false, retryDelay: 0 } } });
  queryClient.setQueryData(queryKeys.features.list(), [{ key: 'claude_code_runtime', enabled: true }]);
  return renderHookWithProviders(() => useClaudeCodeCustody(), { queryClient });
}

const calls = (url: string) => (api.get as Mock).mock.calls.filter(([u]) => u === url).length;

describe('useClaudeCodeCustody', () => {
  beforeEach(() => { vi.clearAllMocks(); });

  it('reads the choice where the server offers one', async () => {
    serve(OWN_ONLY, async () => LOCAL);
    const { result } = run();
    await waitFor(() => expect(result.current.custody).toEqual(LOCAL));
    expect(result.current.unread).toBe(false);
  });

  it('reads a 404 as no choice offered, not as a failed read', async () => {
    serve(OWN_ONLY, async () => { throw notFound(); });
    const { result } = run();
    await waitFor(() => expect(calls('/api/v1/claude-code/custody')).toBe(1));
    await waitFor(() => expect(result.current.isLoading).toBe(false));
    expect(result.current).toMatchObject({ custody: null, unread: false });
  });

  it.each([
    ['Claude Code is off for the user', { ...OWN_ONLY, enabled: false }],
  ] as const)('reads nothing while %s', async (_case, status) => {
    serve(status, async () => LOCAL);
    const { result } = run();
    await waitFor(() => expect(calls('/api/v1/claude-code/status')).toBe(1));
    await new Promise((r) => setTimeout(r, 50));
    expect(calls('/api/v1/claude-code/custody')).toBe(0);
    expect(result.current.custody).toBeNull();
  });

  it('takes a choice made as the answer, and reads the status again', async () => {
    serve(OWN_ONLY, async () => LOCAL);
    (api.put as Mock).mockResolvedValue({ data: { mode: 'api_key', api_key_saved: true, runs_on: null, refusal: null } });
    const { result } = run();
    await waitFor(() => expect(result.current.custody).toEqual(LOCAL));
    await act(async () => { await result.current.choose('api_key'); });
    expect(api.put).toHaveBeenCalledWith('/api/v1/claude-code/custody', { mode: 'api_key' });
    await waitFor(() => expect(result.current.custody).toEqual({ mode: 'api_key', api_key_saved: true, runs_on: null, refusal: null }));
    expect(calls('/api/v1/claude-code/custody')).toBe(1);
    await waitFor(() => expect(calls('/api/v1/claude-code/status')).toBe(2));
  });

  it('reads the choice again once a key is saved or removed', async () => {
    let saved = false;
    serve(OWN_ONLY, async () => ({ mode: 'api_key', api_key_saved: saved, runs_on: null, refusal: null }));
    const { result, queryClient } = run();
    await waitFor(() => expect(result.current.custody).toEqual({ mode: 'api_key', api_key_saved: false, runs_on: null, refusal: null }));
    saved = true;
    await act(async () => { await queryClient.invalidateQueries({ queryKey: queryKeys.user.apiKeys() }); });
    await waitFor(() => expect(result.current.custody).toEqual({ mode: 'api_key', api_key_saved: true, runs_on: null, refusal: null }));
  });
});
