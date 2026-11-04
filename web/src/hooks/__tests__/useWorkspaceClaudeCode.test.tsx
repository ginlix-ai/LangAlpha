/**
 * Whose Claude Code token a workspace's turn runs on: the computer's own
 * entry on one of the user's own computers; elsewhere the saved token where
 * this server runs turns on one, else as the user chose for cloud computers
 * where the server offers that choice, and nowhere otherwise.
 */
import { describe, it, expect, vi } from 'vitest';
import type { Mock } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { QueryClient } from '@tanstack/react-query';
import { renderHookWithProviders } from '@/test/utils';
import { queryKeys } from '@/lib/queryKeys';
import type { ClaudeCodeComputer, ClaudeCodeCustody, ClaudeCodeStatus } from '@/api/model';

vi.mock('@/api/model', () => ({ getClaudeCodeStatus: vi.fn(), getClaudeCodeComputers: vi.fn(), getClaudeCodeCustody: vi.fn() }));
vi.mock('@/pages/ChatAgent/utils/api', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/pages/ChatAgent/utils/api')>()),
  getWorkspace: vi.fn(),
}));
import { getClaudeCodeComputers, getClaudeCodeCustody, getClaudeCodeStatus } from '@/api/model';
import { getWorkspace } from '@/pages/ChatAgent/utils/api';
import { useWorkspaceClaudeCode, type WorkspaceClaudeCode } from '../useWorkspaceClaudeCode';

const MAC: ClaudeCodeComputer = {
  computer_id: 'cmp-mac', name: 'Studio Mac', kind: 'helper', state: 'no_token', source: null, email: null, expires_at: null,
};
/** A self-hosted server running turns on a saved token the check refused. */
const RUNS_HERE: ClaudeCodeStatus = { enabled: true, state: 'rejected', email: null, expires_at: null, saved: true };
/** A server that runs Claude Code only on the user's own computers. */
const OWN_ONLY: ClaudeCodeStatus = { enabled: true, state: 'off', email: null, expires_at: null, saved: false };

/** `computerId` null: the workspace is not read yet, and its read is `getWorkspace`'s. */
function run(
  computerId: string | null,
  status: ClaudeCodeStatus,
  list: () => Promise<unknown> = () => Promise.resolve({ computers: [MAC] }),
  custody: () => Promise<ClaudeCodeCustody | null> = () => Promise.resolve(null),
) {
  (getClaudeCodeStatus as Mock).mockResolvedValue(status);
  (getClaudeCodeCustody as Mock).mockReset().mockImplementation(custody);
  (getClaudeCodeComputers as Mock).mockReset().mockImplementation(list);
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false, retryDelay: 0 } } });
  queryClient.setQueryData(queryKeys.features.list(), [{ key: 'claude_code_runtime', enabled: true }]);
  if (computerId) queryClient.setQueryData(queryKeys.workspaces.detail('ws-1'), desk(computerId));
  return renderHookWithProviders(() => useWorkspaceClaudeCode('ws-1'), { queryClient });
}
const desk = (computerId: string | null) => ({ workspace_id: 'ws-1', name: 'Desk', is_home: false, computer_id: computerId });
const failed = (code: number) => Object.assign(new Error(`Request failed with status code ${code}`), { response: { status: code, data: {} } });

describe('useWorkspaceClaudeCode', () => {
  it.each([RUNS_HERE, OWN_ONLY])("follows the computer's own entry on one of the user's own computers", async (status) => {
    const { result } = run('cmp-mac', status);
    await waitFor(() => expect(result.current).toEqual({ state: 'no_token', computerId: 'cmp-mac', known: true }));
  });

  it("follows the saved token on a hosted computer where this server runs turns on it", async () => {
    const { result } = run('cmp-daytona', RUNS_HERE);
    await waitFor(() => expect(result.current).toEqual({ state: 'rejected', computerId: null, known: true }));
  });

  it("needs one of the user's own computers on a hosted computer where no saved token runs", async () => {
    const { result } = run('cmp-daytona', OWN_ONLY);
    await waitFor(() => expect(result.current).toEqual({ state: 'needs_own_computer', computerId: null, known: true }));
  });

  // On a server that runs the saved token too, an own computer must not
  // read as the saved token while its entry is on the way.
  it('waits as on a check until the list is read', async () => {
    const { result } = run('cmp-mac', RUNS_HERE, () => new Promise(() => {}));
    await waitFor(() => expect(getClaudeCodeStatus).toHaveBeenCalled());
    await waitFor(() => expect(result.current).toEqual({ state: 'checking', computerId: null, known: false }));
  });

  // Which computers are the user's own is unknown then, so a hosted one must
  // not read as needing one, nor an own one as the saved token.
  it.each([RUNS_HERE, OWN_ONLY])('waits as on a check when the list cannot be read, after one more try', async (status) => {
    const { result } = run('cmp-daytona', status, () => Promise.reject(Object.assign(new Error('unavailable'), { response: { status: 503 } })));
    await waitFor(() => expect(getClaudeCodeComputers).toHaveBeenCalledTimes(2), { timeout: 3000 });
    await new Promise((r) => setTimeout(r, 50));
    expect(result.current).toEqual({ state: 'checking', computerId: null, known: false });
  });

  it('keeps the list it had when reading it again fails', async () => {
    const { result, queryClient } = run('cmp-mac', OWN_ONLY);
    await waitFor(() => expect(result.current).toEqual({ state: 'no_token', computerId: 'cmp-mac', known: true }));
    (getClaudeCodeComputers as Mock).mockImplementation(() => Promise.reject(Object.assign(new Error('unavailable'), { response: { status: 503 } })));
    await queryClient.invalidateQueries({ queryKey: queryKeys.features.claudeCodeComputers() }).catch(() => {});
    await waitFor(() => expect(getClaudeCodeComputers).toHaveBeenCalledTimes(3), { timeout: 3000 });
    await waitFor(() => expect(queryClient.getQueryState(queryKeys.features.claudeCodeComputers())?.status).toBe('error'));
    expect(result.current).toEqual({ state: 'no_token', computerId: 'cmp-mac', known: true });
  });

  // An own computer must not read as the saved token while its workspace's
  // computer is unknown.
  it('waits as on a check when the workspace cannot be read', async () => {
    (getWorkspace as Mock).mockReset().mockRejectedValue(failed(500));
    const { result, queryClient } = run(null, RUNS_HERE);
    await waitFor(() => expect(queryClient.getQueryState(queryKeys.workspaces.detail('ws-1'))?.status).toBe('error'));
    await waitFor(() => expect(getClaudeCodeComputers).toHaveBeenCalled());
    await new Promise((r) => setTimeout(r, 50));
    expect(result.current).toEqual({ state: 'checking', computerId: null, known: false });
  });

  it.each([403, 404])('follows the saved token for a workspace the server answers %i for', async (code) => {
    (getWorkspace as Mock).mockReset().mockRejectedValue(failed(code));
    const { result } = run(null, RUNS_HERE);
    await waitFor(() => expect(result.current).toEqual({ state: 'rejected', computerId: null, known: true }));
  });

  // Home's placeholder is as of its first read, maybe before a start put it on a computer.
  it("waits as on a check while only Home's placeholder row is held", async () => {
    let answer!: (w: unknown) => void;
    (getWorkspace as Mock).mockReset().mockReturnValue(new Promise((r) => { answer = r; }));
    const { result, queryClient } = run(null, RUNS_HERE);
    queryClient.setQueryData(queryKeys.workspaces.home(), { ...desk(null), is_home: true });
    await waitFor(() => expect(getClaudeCodeComputers).toHaveBeenCalled());
    await new Promise((r) => setTimeout(r, 50));
    expect(result.current).toEqual({ state: 'checking', computerId: null, known: false });
    await act(async () => { answer({ ...desk('cmp-mac'), is_home: true }); });
    await waitFor(() => expect(result.current).toEqual({ state: 'no_token', computerId: 'cmp-mac', known: true }));
  });

  it.each<[WorkspaceClaudeCode['state'], Partial<ClaudeCodeCustody>]>([
    ['hosted_off', { mode: 'local', api_key_saved: true, refusal: 'hosted_off' }],
    ['needs_api_key', { mode: 'api_key', api_key_saved: false, refusal: 'needs_api_key' }],
    ['api_key_endpoint', { mode: 'api_key', api_key_saved: true, refusal: 'api_key_endpoint' }],
    ['api_key', { mode: 'api_key', api_key_saved: true, runs_on: 'api_key' }],
    ['hosted_paused', { mode: 'api_key', api_key_saved: true, offered: ['stored'], refusal: 'hosted_paused' }],
    ['hosted_paused', { mode: 'stored', offered: ['api_key'], refusal: 'hosted_paused' }],
    ['needs_own_computer', { blocked: true, offered: [], refusal: 'stored_blocked' }],
    ['hosted_off', { blocked: true, offered: ['api_key'], refusal: 'hosted_off' }],
    ['checking', { mode: 'stored', offered: ['stored'], runs_on: 'stored_token' }],
  ])('on a hosted computer reads %s off the verdict for the choice for cloud computers', async (state, choice) => {
    const custody: ClaudeCodeCustody = {
      mode: 'local', api_key_saved: false, offered: ['api_key', 'stored'], blocked: false, runs_on: null, refusal: null, disclosure: null, ...choice,
    };
    const { result } = run('cmp-daytona', OWN_ONLY, undefined, () => Promise.resolve(custody));
    await waitFor(() => expect(result.current).toEqual({ state, computerId: null, known: true }));
  });

  // The status says what the stored token is doing once it reads the choice.
  it('waits as on a check while the token is stored but the status does not run it yet', async () => {
    const { result } = run('cmp-daytona', OWN_ONLY, undefined, () => Promise.resolve({ mode: 'stored', api_key_saved: false, offered: ['stored'], blocked: false, runs_on: 'stored_token', refusal: null, disclosure: null }));
    await waitFor(() => expect(getClaudeCodeCustody).toHaveBeenCalled());
    await new Promise((r) => setTimeout(r, 50));
    expect(result.current).toEqual({ state: 'checking', computerId: null, known: true });
  });

  it('waits as on a check until the choice for cloud computers is read, and when it cannot be', async () => {
    const pending = run('cmp-daytona', OWN_ONLY, undefined, () => new Promise(() => {}));
    await waitFor(() => expect(getClaudeCodeCustody).toHaveBeenCalled());
    expect(pending.result.current).toEqual({ state: 'checking', computerId: null, known: true });
    pending.unmount();
    const { result } = run('cmp-daytona', OWN_ONLY, undefined, () => Promise.reject(Object.assign(new Error('unavailable'), { response: { status: 503 } })));
    await waitFor(() => expect(getClaudeCodeCustody).toHaveBeenCalledTimes(2), { timeout: 3000 });
    await new Promise((r) => setTimeout(r, 50));
    expect(result.current).toEqual({ state: 'checking', computerId: null, known: true });
  });

  // A stored token is one this server runs turns on, as a self-hosted one's is.
  it('follows the saved token where this server runs turns on it, whatever the choice reads', async () => {
    const { result } = run('cmp-daytona', RUNS_HERE, undefined, () => Promise.resolve({ mode: 'stored', api_key_saved: false, offered: ['stored'], blocked: false, runs_on: 'stored_token', refusal: null, disclosure: null }));
    await waitFor(() => expect(getClaudeCodeCustody).toHaveBeenCalled());
    await waitFor(() => expect(result.current).toEqual({ state: 'rejected', computerId: null, known: true }));
  });

  it("follows an own computer's entry whatever the choice for cloud computers", async () => {
    const { result } = run('cmp-mac', OWN_ONLY, undefined, () => Promise.resolve({ mode: 'local', api_key_saved: false, offered: ['api_key'], blocked: false, runs_on: null, refusal: 'hosted_off', disclosure: null }));
    await waitFor(() => expect(getClaudeCodeCustody).toHaveBeenCalled());
    await waitFor(() => expect(result.current).toEqual({ state: 'no_token', computerId: 'cmp-mac', known: true }));
  });

  // As the saved token expires the server reads it as refused, and the picker
  // offers its models no longer, on a hosted computer and on one relaying it.
  describe('as the saved token expires', () => {
    const NOW = new Date('2026-11-03T12:00:00Z');
    const SIGNED_IN: ClaudeCodeStatus = { enabled: true, state: 'signed_in', email: null, expires_at: '2026-11-03T12:00:30Z', saved: true };
    const RELAYING: ClaudeCodeComputer = { ...MAC, state: 'signed_in', source: 'saved' };

    it.each([
      ['a hosted computer', 'cmp-daytona', null],
      ['a computer relaying it', 'cmp-mac', 'cmp-mac'],
    ])('reads %s as refused', async (_, computerId, expected) => {
      vi.useFakeTimers({ toFake: ['Date', 'setInterval', 'clearInterval'] });
      vi.setSystemTime(NOW);
      try {
        const { result } = run(computerId, SIGNED_IN, () => Promise.resolve({ computers: [RELAYING] }));
        await act(() => vi.waitFor(() => expect(result.current).toEqual({ state: 'signed_in', computerId: expected, known: true })));
        (getClaudeCodeStatus as Mock).mockResolvedValue({ ...SIGNED_IN, state: 'rejected' });
        (getClaudeCodeComputers as Mock).mockImplementation(() => Promise.resolve({ computers: [{ ...RELAYING, state: 'rejected' }] }));
        await act(() => vi.advanceTimersByTimeAsync(31_000));
        await act(() => vi.waitFor(() => expect(result.current).toEqual({ state: 'rejected', computerId: expected, known: true })));
      } finally {
        vi.useRealTimers();
      }
    });

    it('reads a computer relaying it as refused where the status is read again first', async () => {
      (getClaudeCodeStatus as Mock).mockResolvedValue(SIGNED_IN);
      (getClaudeCodeComputers as Mock).mockReset().mockResolvedValue({ computers: [RELAYING] });
      const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false, retryDelay: 0 } } });
      queryClient.setQueryData(queryKeys.features.list(), [{ key: 'claude_code_runtime', enabled: true }]);
      queryClient.setQueryData(queryKeys.workspaces.detail('ws-1'), desk('cmp-mac'));
      const { result } = renderHookWithProviders(() => useWorkspaceClaudeCode('ws-1'), { queryClient });
      await waitFor(() => expect(result.current).toEqual({ state: 'signed_in', computerId: 'cmp-mac', known: true }));
      (getClaudeCodeStatus as Mock).mockResolvedValue({ ...SIGNED_IN, state: 'rejected' });
      (getClaudeCodeComputers as Mock).mockResolvedValue({ computers: [{ ...RELAYING, state: 'rejected' }] });
      await act(() => queryClient.invalidateQueries({ queryKey: queryKeys.features.claudeCodeStatus() }));
      await waitFor(() => expect(result.current).toEqual({ state: 'rejected', computerId: 'cmp-mac', known: true }));
    });
  });

  it('is off while Claude Code is', async () => {
    const { result } = run('cmp-mac', { ...OWN_ONLY, enabled: false });
    await waitFor(() => expect(result.current).toEqual({ state: 'off', computerId: null, known: true }));
  });
});
