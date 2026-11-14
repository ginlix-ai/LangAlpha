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
import type { ClaudeCodeChiefOfStaff, ClaudeCodeComputer, ClaudeCodeCustody, ClaudeCodeStatus } from '@/api/model';

vi.mock('@/api/model', () => ({
  getClaudeCodeStatus: vi.fn(), getClaudeCodeComputers: vi.fn(), getClaudeCodeCustody: vi.fn(), getClaudeCodeSetup: vi.fn(),
}));
vi.mock('@/pages/ChatAgent/utils/api', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/pages/ChatAgent/utils/api')>()),
  getWorkspace: vi.fn(),
}));
import { getClaudeCodeComputers, getClaudeCodeCustody, getClaudeCodeSetup, getClaudeCodeStatus } from '@/api/model';
import { getWorkspace } from '@/pages/ChatAgent/utils/api';
import { useChiefOfStaffClaudeCode, useWorkspaceClaudeCode, type WorkspaceClaudeCode } from '../useWorkspaceClaudeCode';

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
/** The picker's meter on a token that works, which turns run on through the saved one. */
const savedMeter = (name: string) => ({ name, plan: undefined, credential: { kind: 'saved' }, checkAfter: null });
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

  it("reads the plan of the token the turn runs on: the computer's own, or the saved one", async () => {
    const plan = (credits: 'on' | 'off') => ({ five_hour: null, seven_day: null, limits: [], credits, observed_at: '2026-11-06T19:00:00Z' });
    const own = run('cmp-mac', { ...RUNS_HERE, plan: plan('off') }, () => Promise.resolve({ computers: [{ ...MAC, plan: plan('on') }] }));
    await waitFor(() => expect(own.result.current.plan).toEqual(plan('on')));
    const saved = run('cmp-daytona', { ...RUNS_HERE, plan: plan('off') });
    await waitFor(() => expect(saved.result.current.plan).toEqual(plan('off')));
  });

  it("holds Check now as long as the server says, counted from when its read arrived, never from a time on the server's clock", async () => {
    vi.useFakeTimers({ toFake: ['Date'] });
    const arrived = Date.parse('2026-11-07T15:00:00Z');
    vi.setSystemTime(arrived);
    // The server's clock runs an hour ahead of this browser's.
    const plan = { five_hour: null, seven_day: null, limits: [], credits: null, observed_at: '2026-11-07T16:00:00Z' };
    try {
      const own = run('cmp-mac', OWN_ONLY, () => Promise.resolve({ computers: [{ ...MAC, state: 'signed_in', source: 'machine', plan, check_in: 60 }] }));
      await waitFor(() => expect(own.result.current.meter?.checkAfter).toBe(arrived + 60_000));
      const saved = run('cmp-daytona', { ...RUNS_HERE, state: 'signed_in', plan, check_in: 120 });
      await waitFor(() => expect(saved.result.current.meter?.checkAfter).toBe(arrived + 120_000));
    } finally {
      vi.useRealTimers();
    }
  });

  it('offers no Check now in the meter where a check would be billed to usage credits', async () => {
    const plan = { five_hour: null, seven_day: null, limits: [], credits: null, observed_at: '2026-11-07T16:00:00Z' };
    const own = run('cmp-mac', OWN_ONLY, () => Promise.resolve({ computers: [{ ...MAC, state: 'signed_in', source: 'machine', plan, check_bills: true }] }));
    await waitFor(() => expect(own.result.current.meter).toMatchObject({ name: 'Studio Mac', credential: null }));
    const saved = run('cmp-daytona', { ...RUNS_HERE, state: 'signed_in', plan, check_bills: true });
    await waitFor(() => expect(saved.result.current.meter).toBeDefined());
    expect(saved.result.current.meter?.credential).toBeNull();
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
    ['hosted_off', { mode: 'local', refusal: 'hosted_off' }],
    ['needs_own_computer', { blocked: true, offered: [], refusal: 'stored_blocked' }],
    ['needs_own_computer', { mode: 'stored', refusal: 'needs_own_computer' }],
    ['checking', { mode: 'stored', offered: ['stored'], runs_on: 'stored_token' }],
  ])('on a hosted computer reads %s off the verdict for the choice for cloud computers', async (state, choice) => {
    const custody: ClaudeCodeCustody = {
      mode: 'local', offered: ['stored'], blocked: false, runs_on: null, refusal: null, ...choice,
    };
    const { result } = run('cmp-daytona', OWN_ONLY, undefined, () => Promise.resolve(custody));
    await waitFor(() => expect(result.current).toEqual({ state, computerId: null, known: true }));
  });

  const SMALL = () => Promise.resolve({ computers: [MAC], cloud_refusals: { 'cmp-daytona': 'needs_bigger_computer' } });

  it('on a cloud computer the server names too small reads the refusal, the credential first', async () => {
    const custody: ClaudeCodeCustody = {
      mode: 'local', offered: ['stored'], blocked: false, runs_on: null, refusal: 'hosted_off',
    };
    const { result } = run('cmp-daytona', OWN_ONLY, SMALL, () => Promise.resolve(custody));
    await waitFor(() => expect(result.current).toEqual({ state: 'hosted_off', computerId: null, known: true }));
  });

  // A self-hosted server offers no choice, and names the computer on the same list.
  it.each<[ClaudeCodeStatus['state'], WorkspaceClaudeCode['state']]>([
    ['signed_in', 'needs_bigger_computer'], ['unverified', 'needs_bigger_computer'], ['rejected', 'rejected'], ['no_token', 'no_token'],
  ])('with the saved token %s on a cloud computer the server names too small, reads %s', async (state, expected) => {
    const { result } = run('cmp-daytona', { ...RUNS_HERE, state }, SMALL);
    const small = expected === 'needs_bigger_computer' ? { smallComputerId: 'cmp-daytona' } : {};
    await waitFor(() => expect(result.current).toEqual({ state: expected, computerId: null, known: true, ...small }));
  });

  it('runs on a cloud computer the server does not name', async () => {
    const { result } = run('cmp-other', { ...RUNS_HERE, state: 'signed_in' }, SMALL);
    await waitFor(() => expect(result.current).toEqual({ state: 'signed_in', computerId: null, known: true, meter: savedMeter('your setup token') }));
  });

  // The status says what the stored token is doing once it reads the choice.
  it('waits as on a check while the token is stored but the status does not run it yet', async () => {
    const { result } = run('cmp-daytona', OWN_ONLY, undefined, () => Promise.resolve({ mode: 'stored', offered: ['stored'], blocked: false, runs_on: 'stored_token', refusal: null }));
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
    const { result } = run('cmp-daytona', RUNS_HERE, undefined, () => Promise.resolve({ mode: 'stored', offered: ['stored'], blocked: false, runs_on: 'stored_token', refusal: null }));
    await waitFor(() => expect(getClaudeCodeCustody).toHaveBeenCalled());
    await waitFor(() => expect(result.current).toEqual({ state: 'rejected', computerId: null, known: true }));
  });

  it('needs one of the user\'s own computers where no cloud computer runs it, whatever the stored token reads', async () => {
    const { result } = run('cmp-daytona', { ...RUNS_HERE, state: 'signed_in' }, undefined, () => Promise.resolve({ mode: 'stored', offered: ['stored'], blocked: false, runs_on: null, refusal: 'needs_own_computer' }));
    await waitFor(() => expect(getClaudeCodeCustody).toHaveBeenCalled());
    await waitFor(() => expect(result.current).toEqual({ state: 'needs_own_computer', computerId: null, known: true }));
  });

  it("follows an own computer's entry whatever the choice for cloud computers", async () => {
    const { result } = run('cmp-mac', OWN_ONLY, undefined, () => Promise.resolve({ mode: 'local', offered: ['stored'], blocked: false, runs_on: null, refusal: 'hosted_off' }));
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
      ['a hosted computer', 'cmp-daytona', null, 'your setup token'],
      ['a computer relaying it', 'cmp-mac', 'cmp-mac', 'Studio Mac'],
    ])('reads %s as refused', async (_, computerId, expected, name) => {
      vi.useFakeTimers({ toFake: ['Date', 'setInterval', 'clearInterval'] });
      vi.setSystemTime(NOW);
      try {
        const { result } = run(computerId, SIGNED_IN, () => Promise.resolve({ computers: [RELAYING] }));
        await act(() => vi.waitFor(() => expect(result.current).toEqual({ state: 'signed_in', computerId: expected, known: true, meter: savedMeter(name) })));
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
      await waitFor(() => expect(result.current).toEqual({ state: 'signed_in', computerId: 'cmp-mac', known: true, meter: savedMeter('Studio Mac') }));
      (getClaudeCodeStatus as Mock).mockResolvedValue({ ...SIGNED_IN, state: 'rejected' });
      (getClaudeCodeComputers as Mock).mockResolvedValue({ computers: [{ ...RELAYING, state: 'rejected' }] });
      await act(() => queryClient.invalidateQueries({ queryKey: queryKeys.features.claudeCodeStatus() }));
      await waitFor(() => expect(result.current).toEqual({ state: 'rejected', computerId: 'cmp-mac', known: true }));
    });
  });

  // Where the user's cloud computers run no CLI themselves, a cloud
  // workspace's runs on the own computer the server picked.
  describe('with the CLI on an own computer the server picks', () => {
    const LOCAL: ClaudeCodeCustody = { mode: 'local', offered: ['stored'], blocked: false, runs_on: null, refusal: 'hosted_off' };
    const plan = { five_hour: null, seven_day: null, limits: [], credits: null, observed_at: '2026-11-07T16:00:00Z' };
    const picked = { ...MAC, state: 'signed_in' as const, source: 'machine' as const, plan };
    const pick = (cli: object | null) => () => Promise.resolve({ computers: [picked], cli_computer: cli });

    it("reads a cloud workspace as on that computer: its token, plan and meter", async () => {
      const { result } = run('cmp-daytona', OWN_ONLY, pick({ kind: 'picked', computer_id: 'cmp-mac', name: 'Studio Mac', refusal: null }), () => Promise.resolve(LOCAL));
      await waitFor(() => expect(result.current).toEqual({
        state: 'signed_in', computerId: 'cmp-mac', known: true, plan,
        meter: { name: 'Studio Mac', plan, credential: { kind: 'computer', computerId: 'cmp-mac' }, checkAfter: null },
      }));
    });

    it.each([
      [{ kind: 'picked', computer_id: null, name: null, refusal: 'no_own_computer' }, { state: 'no_own_computer' }],
      [{ kind: 'picked', computer_id: null, name: 'Studio Mac', refusal: 'cli_computer_offline' }, { state: 'cli_computer_offline', cliComputerName: 'Studio Mac' }],
    ])('reads why none can run it, naming the computer that is offline', async (cli, expected) => {
      const { result } = run('cmp-daytona', OWN_ONLY, pick(cli), () => Promise.resolve(LOCAL));
      await waitFor(() => expect(result.current).toEqual({ ...expected, computerId: null, known: true }));
    });

    it('keeps the verdict for cloud computers where they run the CLI themselves', async () => {
      const { result } = run('cmp-daytona', OWN_ONLY, pick(null), () => Promise.resolve(LOCAL));
      await waitFor(() => expect(result.current).toEqual({ state: 'hosted_off', computerId: null, known: true }));
    });

    it("follows the workspace's own computer over the pick", async () => {
      const own = { ...MAC, computer_id: 'cmp-ssh', name: 'Lab box' };
      const list = () => Promise.resolve({ computers: [picked, own], cli_computer: { kind: 'picked', computer_id: 'cmp-mac', name: 'Studio Mac', refusal: null } });
      const { result } = run('cmp-ssh', OWN_ONLY, list, () => Promise.resolve(LOCAL));
      await waitFor(() => expect(result.current).toEqual({ state: 'no_token', computerId: 'cmp-ssh', known: true }));
    });
  });

  // Admission refuses an own computer's token past its expiry, whatever its
  // last check said, so the picker reads it as refused rather than offering it.
  it.each(['signed_in', 'unverified'] as const)("reads an own computer's %s token past its expiry as refused", async (state) => {
    const expired = { ...MAC, state, source: 'machine' as const, expires_at: '2000-01-01T00:00:00Z' };
    const { result } = run('cmp-mac', OWN_ONLY, () => Promise.resolve({ computers: [expired] }));
    await waitFor(() => expect(result.current).toEqual({ state: 'rejected', computerId: 'cmp-mac', known: true }));
  });

  // The composer reads no minute clock, so the list is read again as the
  // token expires, and the turn is judged at that read.
  it("reads an own computer's token as refused once it expires, the composer left open", async () => {
    vi.useFakeTimers({ toFake: ['Date', 'setTimeout', 'clearTimeout', 'setInterval', 'clearInterval'], shouldAdvanceTime: true });
    try {
      const expiring = { ...MAC, state: 'signed_in' as const, source: 'machine' as const, expires_at: new Date(Date.now() + 60_000).toISOString() };
      const { result } = run('cmp-mac', OWN_ONLY, () => Promise.resolve({ computers: [expiring] }));
      await waitFor(() => expect(result.current.state).toBe('signed_in'));
      const reads = (getClaudeCodeComputers as Mock).mock.calls.length;
      await act(async () => { await vi.advanceTimersByTimeAsync(62_000); });
      await waitFor(() => expect(result.current.state).toBe('rejected'));
      expect((getClaudeCodeComputers as Mock).mock.calls.length).toBeGreaterThan(reads);
    } finally {
      vi.useRealTimers();
    }
  });

  // A read again at the expiry that fails leaves the read from before it,
  // which still finds the token working, so the list keeps being read.
  it("keeps reading the list after an own computer's token expires until a read lands, and then stops", async () => {
    vi.useFakeTimers({ toFake: ['Date', 'setTimeout', 'clearTimeout', 'setInterval', 'clearInterval'], shouldAdvanceTime: true });
    try {
      const expiring = { ...MAC, state: 'signed_in' as const, source: 'machine' as const, expires_at: new Date(Date.now() + 60_000).toISOString() };
      let reads = 0;
      const list = () => {
        reads += 1;
        // The read at the expiry and its retry fail.
        return reads === 2 || reads === 3 ? Promise.reject(new Error('offline')) : Promise.resolve({ computers: [expiring] });
      };
      const { result } = run('cmp-mac', OWN_ONLY, list);
      await waitFor(() => expect(result.current.state).toBe('signed_in'));
      await act(async () => { await vi.advanceTimersByTimeAsync(62_000); });
      expect(reads).toBe(3);
      expect(result.current.state).toBe('signed_in');
      await act(async () => { await vi.advanceTimersByTimeAsync(31_000); });
      await waitFor(() => expect(result.current.state).toBe('rejected'));
      const landed = reads;
      await act(async () => { await vi.advanceTimersByTimeAsync(120_000); });
      expect(reads).toBe(landed);
    } finally {
      vi.useRealTimers();
    }
  });

  it("reads an own computer's token before its expiry as working", async () => {
    const live = { ...MAC, state: 'signed_in' as const, source: 'machine' as const, expires_at: '2999-01-01T00:00:00Z' };
    const { result } = run('cmp-mac', OWN_ONLY, () => Promise.resolve({ computers: [live] }));
    await waitFor(() => expect(result.current).toMatchObject({ state: 'signed_in', computerId: 'cmp-mac', known: true }));
  });

  it('is off while Claude Code is', async () => {
    const { result } = run('cmp-mac', { ...OWN_ONLY, enabled: false });
    await waitFor(() => expect(result.current).toEqual({ state: 'off', computerId: null, known: true }));
  });
});

/**
 * A Chief of Staff turn reads where the server places its CLI as a
 * workspace's turn reads that placement: the reason it runs nowhere, the own
 * computer that runs it, or Home's cloud computer or this server by the
 * credential.
 */
describe('useChiefOfStaffClaudeCode', () => {
  const placed = (over: Partial<ClaudeCodeChiefOfStaff>): ClaudeCodeChiefOfStaff => ({
    kind: 'server', computer_id: null, name: null, refusal: null, default_runs: true, ...over,
  });
  function runHome(
    cos: ClaudeCodeChiefOfStaff | null,
    status: ClaudeCodeStatus = OWN_ONLY,
    list: () => Promise<unknown> = () => Promise.resolve({ computers: [MAC] }),
  ) {
    (getClaudeCodeStatus as Mock).mockResolvedValue(status);
    (getClaudeCodeCustody as Mock).mockReset().mockResolvedValue(null);
    (getClaudeCodeComputers as Mock).mockReset().mockImplementation(list);
    (getClaudeCodeSetup as Mock).mockReset().mockResolvedValue({ state: 'connected', on: true, computers: [], saved_token_runs: true, cloud: true, chief_of_staff: cos });
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false, retryDelay: 0 } } });
    queryClient.setQueryData(queryKeys.features.list(), [{ key: 'claude_code_runtime', enabled: true }]);
    return renderHookWithProviders(() => useChiefOfStaffClaudeCode(true), { queryClient });
  }

  it.each([
    [placed({ kind: 'picked', refusal: 'cli_computer_offline', name: 'Studio Mac' }), { state: 'cli_computer_offline', cliComputerName: 'Studio Mac' }],
    [placed({ kind: 'picked', refusal: 'no_own_computer' }), { state: 'no_own_computer' }],
    [
      placed({ kind: 'home_cloud', refusal: 'needs_bigger_computer', computer_id: 'cmp-home', name: 'Home desk' }),
      { state: 'needs_bigger_computer', smallComputerId: 'cmp-home' },
    ],
    [placed({ kind: 'home_cloud', refusal: 'signed_out' }), { state: 'no_token' }],
    [placed({ kind: 'home_cloud', refusal: 'token_refused' }), { state: 'rejected' }],
    [placed({ kind: 'own', computer_id: 'cmp-mac', refusal: 'policy_blocked' }), { state: 'policy_blocked' }],
  ])('reads a refusal as the state the rows word it in', async (cos, expected) => {
    const { result } = runHome(cos);
    await waitFor(() => expect(result.current).toEqual({ ...expected, computerId: null, known: true }));
  });

  it.each(['own', 'picked'] as const)('reads %s placement as on that computer: its token, plan and meter', async (kind) => {
    const plan = { five_hour: null, seven_day: null, limits: [], credits: null, observed_at: '2026-11-07T16:00:00Z' };
    const mac = { ...MAC, state: 'signed_in' as const, source: 'machine' as const, plan };
    const { result } = runHome(placed({ kind, computer_id: 'cmp-mac', name: 'Studio Mac' }), OWN_ONLY, () => Promise.resolve({ computers: [mac] }));
    await waitFor(() => expect(result.current).toEqual({
      state: 'signed_in', computerId: 'cmp-mac', known: true, plan,
      meter: { name: 'Studio Mac', plan, credential: { kind: 'computer', computerId: 'cmp-mac' }, checkAfter: null },
    }));
  });

  it.each(['own', 'picked'] as const)("reads %s placement on a computer whose token is past its expiry as refused", async (kind) => {
    const mac = { ...MAC, state: 'signed_in' as const, source: 'machine' as const, expires_at: '2000-01-01T00:00:00Z' };
    const { result } = runHome(placed({ kind, computer_id: 'cmp-mac', name: 'Studio Mac' }), OWN_ONLY, () => Promise.resolve({ computers: [mac] }));
    await waitFor(() => expect(result.current).toEqual({ state: 'rejected', computerId: 'cmp-mac', known: true }));
  });

  it.each(['home_cloud', 'server'] as const)('reads %s placement by the credential, as a cloud computer is', async (kind) => {
    const { result } = runHome(placed({ kind, computer_id: kind === 'home_cloud' ? 'cmp-home' : null }), { ...RUNS_HERE, state: 'signed_in' });
    await waitFor(() => expect(result.current).toEqual({ state: 'signed_in', computerId: null, known: true, meter: savedMeter('your setup token') }));
  });

  it("tells Home's cloud computer too small once the credential would run", async () => {
    const list = () => Promise.resolve({ computers: [], cloud_refusals: { 'cmp-home': 'needs_bigger_computer' } });
    const { result } = runHome(placed({ kind: 'home_cloud', computer_id: 'cmp-home' }), { ...RUNS_HERE, state: 'signed_in' }, list);
    await waitFor(() => expect(result.current.state).toBe('needs_bigger_computer'));
  });

  it('is off while the server places it nowhere', async () => {
    const { result } = runHome(null);
    await waitFor(() => expect(result.current).toEqual({ state: 'off', computerId: null, known: true }));
  });
});
