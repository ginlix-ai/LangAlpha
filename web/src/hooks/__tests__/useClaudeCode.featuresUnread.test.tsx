/**
 * Claude Code's reads wait on the user's flags. Flags never read leave
 * whether Claude Code is on unknown, so its token reads are unread, not off,
 * and trying again reads the flags.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { QueryClient } from '@tanstack/react-query';
import { renderHookWithProviders } from '@/test/utils';

vi.mock('@/api/client', () => ({ api: { get: vi.fn(), put: vi.fn() } }));
import { api } from '@/api/client';
import { useClaudeCodeComputers, useClaudeCodeStatus } from '../useClaudeCode';

const calls = (url: string) => (api.get as Mock).mock.calls.filter(([u]) => u === url).length;

function run() {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return renderHookWithProviders(
    () => ({ status: useClaudeCodeStatus(), computers: useClaudeCodeComputers() }),
    { queryClient },
  );
}

describe('Claude Code with the flags unread', () => {
  beforeEach(() => {
    (api.get as Mock).mockReset();
    (api.get as Mock).mockRejectedValue(new Error('offline'));
  });

  it('reads its token as unread, not off, and reads the flags again on retry', async () => {
    const { result } = run();
    await waitFor(() => expect(result.current.status.unread).toBe(true));
    expect(result.current.computers.unread).toBe(true);
    const before = calls('/api/v1/features');
    act(() => result.current.status.retry());
    await waitFor(() => expect(calls('/api/v1/features')).toBe(before + 1));
  });

  it('reads nothing as unread for a caller that asked for nothing', async () => {
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const { result } = renderHookWithProviders(() => useClaudeCodeStatus({ enabled: false }), { queryClient });
    await waitFor(() => expect(calls('/api/v1/features')).toBeGreaterThan(0));
    await waitFor(() => expect(result.current.isLoading).toBe(false));
    expect(result.current.unread).toBe(false);
  });
});
