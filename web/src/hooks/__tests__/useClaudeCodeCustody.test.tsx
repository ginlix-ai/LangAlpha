/**
 * How Claude Code runs on cloud computers, where the server lets the user
 * choose: a server that offers no choice answers 404, which reads as none,
 * and a choice made is the answer at once. It is read wherever Claude Code
 * is on, since a stored token runs on the server as a self-hosted one does.
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
const LOCAL: ClaudeCodeCustody = { mode: 'local', api_key_saved: false, offered: ['api_key', 'stored'], blocked: false, runs_on: null, refusal: null, disclosure: '2026-11-v1' };
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

  it('reads the choice where this server runs turns on the saved token, which may be the stored one', async () => {
    const stored: ClaudeCodeCustody = { ...LOCAL, mode: 'stored' };
    serve({ ...OWN_ONLY, state: 'signed_in', saved: true }, async () => stored);
    const { result } = run();
    await waitFor(() => expect(result.current.custody).toEqual(stored));
  });

  it('reads nothing while Claude Code is off for the user', async () => {
    serve({ ...OWN_ONLY, enabled: false }, async () => LOCAL);
    const { result } = run();
    await waitFor(() => expect(calls('/api/v1/claude-code/status')).toBe(1));
    await new Promise((r) => setTimeout(r, 50));
    expect(calls('/api/v1/claude-code/custody')).toBe(0);
    expect(result.current.custody).toBeNull();
  });

  it('takes a choice made as the answer, and reads the status again', async () => {
    serve(OWN_ONLY, async () => LOCAL);
    (api.put as Mock).mockResolvedValue({ data: { ...LOCAL, mode: 'api_key', api_key_saved: true } });
    const { result } = run();
    await waitFor(() => expect(result.current.custody).toEqual(LOCAL));
    await act(async () => { await result.current.choose({ mode: 'api_key' }); });
    expect(api.put).toHaveBeenCalledWith('/api/v1/claude-code/custody', { mode: 'api_key' });
    await waitFor(() => expect(result.current.custody).toEqual({ ...LOCAL, mode: 'api_key', api_key_saved: true }));
    expect(calls('/api/v1/claude-code/custody')).toBe(1);
    await waitFor(() => expect(calls('/api/v1/claude-code/status')).toBe(2));
  });

  it('sends the consent accepted with a choice to store the token', async () => {
    serve(OWN_ONLY, async () => LOCAL);
    (api.put as Mock).mockResolvedValue({ data: { ...LOCAL, mode: 'stored' } });
    const { result } = run();
    await waitFor(() => expect(result.current.custody).toEqual(LOCAL));
    await act(async () => { await result.current.choose({ mode: 'stored', disclosure: '2026-11-v1' }); });
    expect(api.put).toHaveBeenCalledWith('/api/v1/claude-code/custody', { mode: 'stored', disclosure: '2026-11-v1' });
  });

  it('reads the choice again when the server refuses one, since what it offers may have changed', async () => {
    let custody = LOCAL;
    serve(OWN_ONLY, async () => custody);
    (api.put as Mock).mockRejectedValue(Object.assign(new Error('409'), { response: { status: 409, data: { error: { code: 'unavailable' } } } }));
    const { result } = run();
    await waitFor(() => expect(result.current.custody).toEqual(LOCAL));
    custody = { ...LOCAL, offered: ['api_key'] };
    await act(async () => { await result.current.choose({ mode: 'stored', disclosure: '2026-11-v1' }).catch(() => {}); });
    await waitFor(() => expect(result.current.custody).toEqual(custody));
  });

  it('reads the status again where leaving the stored token deleted it but a turn on it did not stop', async () => {
    serve(OWN_ONLY, async () => ({ ...LOCAL, mode: 'stored' }));
    (api.put as Mock).mockRejectedValue(Object.assign(new Error('500'), { response: { status: 500, data: { error: { code: 'turns_running' } } } }));
    const { result } = run();
    await waitFor(() => expect(result.current.custody?.mode).toBe('stored'));
    await waitFor(() => expect(calls('/api/v1/claude-code/status')).toBe(1));
    await act(async () => { await result.current.choose({ mode: 'local' }).catch(() => {}); });
    await waitFor(() => expect(calls('/api/v1/claude-code/status')).toBe(2));
  });

  it('reads the choice again once a key is saved or removed', async () => {
    let saved = false;
    serve(OWN_ONLY, async () => ({ ...LOCAL, mode: 'api_key', api_key_saved: saved }));
    const { result, queryClient } = run();
    await waitFor(() => expect(result.current.custody).toMatchObject({ mode: 'api_key', api_key_saved: false }));
    saved = true;
    await act(async () => { await queryClient.invalidateQueries({ queryKey: queryKeys.user.apiKeys() }); });
    await waitFor(() => expect(result.current.custody).toMatchObject({ mode: 'api_key', api_key_saved: true }));
  });
});
