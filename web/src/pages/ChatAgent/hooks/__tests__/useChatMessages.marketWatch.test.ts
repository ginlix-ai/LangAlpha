/**
 * Tests for the useChatMessages market-watch chip state.
 *
 * The hook seeds `marketWatch` from GET /market-watch on thread load and must
 * clear it EAGERLY on an in-place thread switch — otherwise thread B briefly
 * shows thread A's chip symbols until B's fetch resolves (cross-thread flash).
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';

// ---------------------------------------------------------------------------
// Mocks (mirrors useChatMessages.workspaceStatus.test.ts scaffolding)
// ---------------------------------------------------------------------------

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (k: string) => k }),
}));

vi.mock('@/lib/supabase', () => ({ supabase: null }));

vi.mock('../utils/threadStorage', () => ({
  getStoredThreadId: vi.fn().mockReturnValue(null),
  setStoredThreadId: vi.fn(),
  removeStoredThreadId: vi.fn(),
}));

vi.mock('../../utils/api', () => ({
  fetchMarketWatch: vi.fn().mockResolvedValue({ thread_id: 't', symbols: [] }),
  sendChatMessageStream: vi.fn(),
  sendHitlResponse: vi.fn(),
  replayThreadHistory: vi.fn().mockResolvedValue(undefined),
  getWorkflowStatus: vi.fn().mockResolvedValue({ can_reconnect: false, status: 'completed' }),
  reconnectToWorkflowStream: vi.fn(),
  streamSubagentTaskEvents: vi.fn(),
  fetchThreadTurns: vi.fn().mockResolvedValue({ turns: [], retry_checkpoint_id: null }),
  submitFeedback: vi.fn(),
  removeFeedback: vi.fn(),
  getThreadFeedback: vi.fn().mockResolvedValue([]),
  watchThread: vi.fn(),
}));

import { fetchMarketWatch } from '../../utils/api';
import { useChatMessages } from '../useChatMessages';

const mockFetchMarketWatch = fetchMarketWatch as Mock;

describe('useChatMessages – market watch chip state', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('seeds marketWatch from the GET endpoint on thread load', async () => {
    mockFetchMarketWatch.mockResolvedValue({ thread_id: 'thread-a', symbols: ['NVDA', 'TSLA'] });

    const { result } = renderHookWithProviders(() => useChatMessages('ws-test', 'thread-a'));

    await waitFor(() => {
      expect(result.current.marketWatch).toEqual({ symbols: ['NVDA', 'TSLA'] });
    });
    expect(mockFetchMarketWatch).toHaveBeenCalledWith('thread-a');
  });

  it('stores null (no chip) when the thread has an empty watch list', async () => {
    mockFetchMarketWatch.mockResolvedValue({ thread_id: 'thread-a', symbols: [] });

    const { result } = renderHookWithProviders(() => useChatMessages('ws-test', 'thread-a'));

    await waitFor(() => expect(mockFetchMarketWatch).toHaveBeenCalled());
    expect(result.current.marketWatch).toBeNull();
  });

  it('clears marketWatch EAGERLY on thread switch — no stale flash of the old thread symbols', async () => {
    // Thread A resolves immediately with symbols.
    mockFetchMarketWatch.mockResolvedValue({ thread_id: 'thread-a', symbols: ['NVDA'] });

    let tid = 'thread-a';
    const { result, rerender } = renderHookWithProviders(() => useChatMessages('ws-test', tid));

    await waitFor(() => {
      expect(result.current.marketWatch).toEqual({ symbols: ['NVDA'] });
    });

    // Thread B's fetch hangs — the mid-flight window where the flash happened.
    let resolveB: (v: { thread_id: string; symbols: string[] }) => void = () => {};
    mockFetchMarketWatch.mockImplementation(
      () => new Promise((r) => { resolveB = r; }),
    );

    tid = 'thread-b';
    await act(async () => {
      rerender();
    });

    // While B's fetch is pending, thread A's symbols must NOT linger.
    expect(result.current.marketWatch).toBeNull();

    // When B's list arrives, the chip seeds with the new thread's symbols.
    await act(async () => {
      resolveB({ thread_id: 'thread-b', symbols: ['AAPL'] });
    });
    await waitFor(() => {
      expect(result.current.marketWatch).toEqual({ symbols: ['AAPL'] });
    });
  });
});
