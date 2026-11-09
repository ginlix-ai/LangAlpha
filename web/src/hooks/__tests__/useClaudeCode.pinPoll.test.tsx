/**
 * A pin is held while an own computer checks its token, even where the
 * setup is connected by another token, so the setup is read again until
 * that check lands and the picker frees the pin with no remount.
 */
import type { ReactNode } from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { renderHook, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type { ClaudeCodeSetup, ClaudeCodeStatus } from '@/api/model';

const h = vi.hoisted(() => ({ status: vi.fn(), setup: vi.fn() }));

vi.mock('@/api/model', () => ({ getClaudeCodeStatus: h.status, getClaudeCodeComputers: vi.fn(), getClaudeCodeSetup: h.setup }));
vi.mock('@/hooks/useFeatures', () => ({ useFeatures: () => ({ data: {}, isLoading: false }), isFeatureEnabled: () => true }));

import { useClaudeCodeSetup } from '../useClaudeCode';

const status: ClaudeCodeStatus = {
  enabled: true, state: 'signed_in', email: null, expires_at: null, saved: true, plan: null, check_bills: false,
} as ClaudeCodeStatus;
const setup = (pin: string): ClaudeCodeSetup => ({ state: 'connected', pin_state: pin, computers: [] } as unknown as ClaudeCodeSetup);

function wrapper({ children }: { children: ReactNode }) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return <QueryClientProvider client={client}>{children}</QueryClientProvider>;
}

describe('the setup read while a pin waits on a check', () => {
  beforeEach(() => {
    vi.useFakeTimers({ toFake: ['Date', 'setTimeout', 'clearTimeout', 'setInterval', 'clearInterval'], shouldAdvanceTime: true });
    h.status.mockReset().mockResolvedValue(status);
    h.setup.mockReset().mockResolvedValueOnce(setup('checking')).mockResolvedValue(setup('connected'));
  });
  afterEach(() => vi.useRealTimers());

  it('is read again until the check lands, the picker left open', async () => {
    const { result } = renderHook(() => useClaudeCodeSetup(), { wrapper });
    await waitFor(() => expect(result.current.setup?.pin_state).toBe('checking'));
    await vi.advanceTimersByTimeAsync(6_000);
    await waitFor(() => expect(result.current.setup?.pin_state).toBe('connected'));
  });

  // A composer outside Home reads no setup (`enabled: false`); the pins
  // surface beside it still polls.
  it('is read again beside a reader that reads no setup', async () => {
    const { result } = renderHook(() => ({ off: useClaudeCodeSetup({ enabled: false }), pins: useClaudeCodeSetup() }), { wrapper });
    await waitFor(() => expect(result.current.pins.setup?.pin_state).toBe('checking'));
    await vi.advanceTimersByTimeAsync(6_000);
    await waitFor(() => expect(result.current.pins.setup?.pin_state).toBe('connected'));
  });
});
