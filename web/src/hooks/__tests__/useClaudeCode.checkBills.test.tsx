/**
 * Check now hidden because a check would be billed (`check_bills`) comes
 * back as the limit behind it ends: the status and the computers are read
 * again then, since only the server knows which limits bind the model a
 * check asks for.
 */
import type { ReactNode } from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { renderHook, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type { ClaudeCodeComputer, ClaudeCodePlan, ClaudeCodeStatus } from '@/api/model';

const h = vi.hoisted(() => ({ status: vi.fn(), computers: vi.fn() }));

vi.mock('@/api/model', () => ({ getClaudeCodeStatus: h.status, getClaudeCodeComputers: h.computers, getClaudeCodeSetup: vi.fn() }));
vi.mock('@/hooks/useFeatures', () => ({ useFeatures: () => ({ data: {}, isLoading: false }), isFeatureEnabled: () => true }));

import { useClaudeCodeComputers, useClaudeCodeStatus } from '../useClaudeCode';

const NOW = Date.parse('2026-11-07T15:00:00Z');
const plan = (until: number): ClaudeCodePlan => ({
  five_hour: null, seven_day: null, credits: null, observed_at: new Date(NOW).toISOString(),
  limits: [{ window: 'five_hour', until: new Date(until).toISOString(), family: null, reset_known: true }],
});
const status = (billed: boolean): ClaudeCodeStatus => ({
  enabled: true, state: 'signed_in', email: null, expires_at: null, saved: true, plan: plan(NOW + 60_000), check_bills: billed,
});
const computer = (billed: boolean): ClaudeCodeComputer => ({
  computer_id: 'cmp-1', name: 'Lab box', kind: 'ssh', state: 'signed_in', source: 'machine', email: null, expires_at: null,
  plan: plan(NOW + 60_000), check_bills: billed,
} as ClaudeCodeComputer);

function wrapper({ children }: { children: ReactNode }) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return <QueryClientProvider client={client}>{children}</QueryClientProvider>;
}

describe('Check now hidden while a check would be billed', () => {
  beforeEach(() => {
    vi.useFakeTimers({ toFake: ['Date', 'setTimeout', 'clearTimeout', 'setInterval', 'clearInterval'], shouldAdvanceTime: true });
    vi.setSystemTime(NOW);
    h.status.mockReset().mockResolvedValueOnce(status(true)).mockResolvedValue(status(false));
    h.computers.mockReset().mockResolvedValueOnce({ computers: [computer(true)] }).mockResolvedValue({ computers: [computer(false)] });
  });
  afterEach(() => vi.useRealTimers());

  it('comes back on the status as the limit behind it ends, the tab left open', async () => {
    const { result } = renderHook(() => useClaudeCodeStatus(), { wrapper });
    await waitFor(() => expect(result.current.checkBills).toBe(true));
    await vi.advanceTimersByTimeAsync(62_000);
    await waitFor(() => expect(result.current.checkBills).toBe(false));
  });

  it('comes back on a computer as the limit behind it ends, the tab left open', async () => {
    const { result } = renderHook(() => useClaudeCodeComputers(), { wrapper });
    await waitFor(() => expect(result.current.computers[0]?.check_bills).toBe(true));
    await vi.advanceTimersByTimeAsync(62_000);
    await waitFor(() => expect(result.current.computers[0]?.check_bills).toBe(false));
  });
});
