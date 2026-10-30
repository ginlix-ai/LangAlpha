/**
 * Whose Claude Code token a workspace's turn runs on: the computer's own
 * entry on one of the user's own computers; elsewhere the saved token where
 * this server runs turns on one, and nowhere otherwise.
 */
import { describe, it, expect, vi } from 'vitest';
import type { Mock } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { QueryClient } from '@tanstack/react-query';
import { renderHookWithProviders } from '@/test/utils';
import { queryKeys } from '@/lib/queryKeys';
import type { ClaudeCodeComputer, ClaudeCodeStatus } from '@/api/model';

vi.mock('@/api/model', () => ({ getClaudeCodeStatus: vi.fn(), getClaudeCodeComputers: vi.fn() }));
vi.mock('@/pages/ChatAgent/utils/api', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/pages/ChatAgent/utils/api')>()),
  getWorkspace: vi.fn(),
}));
import { getClaudeCodeComputers, getClaudeCodeStatus } from '@/api/model';
import { getWorkspace } from '@/pages/ChatAgent/utils/api';
import { useWorkspaceClaudeCode } from '../useWorkspaceClaudeCode';

const MAC: ClaudeCodeComputer = {
  computer_id: 'cmp-mac', name: 'Studio Mac', kind: 'helper', state: 'no_token', source: null, email: null, expires_at: null,
};
/** A self-hosted server running turns on a saved token the check refused. */
const RUNS_HERE: ClaudeCodeStatus = { enabled: true, state: 'rejected', email: null, expires_at: null, saved: true };
/** A server that runs Claude Code only on the user's own computers. */
const OWN_ONLY: ClaudeCodeStatus = { enabled: true, state: 'off', email: null, expires_at: null, saved: false };

/** `computerId` null: the workspace is not read yet, and its read is `getWorkspace`'s. */
function run(computerId: string | null, status: ClaudeCodeStatus, list: () => Promise<unknown> = () => Promise.resolve({ computers: [MAC] })) {
  (getClaudeCodeStatus as Mock).mockResolvedValue(status);
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

  it('is off while Claude Code is', async () => {
    const { result } = run('cmp-mac', { ...OWN_ONLY, enabled: false });
    await waitFor(() => expect(result.current).toEqual({ state: 'off', computerId: null, known: true }));
  });
});
