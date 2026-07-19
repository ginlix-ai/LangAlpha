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

// Market watch is feature-gated; force the flag on so useMarketWatch seeds/refetches.
vi.mock('@/hooks/useFeatures', () => ({
  useFeatureEnabled: () => true,
}));

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
  openThreadMuxStream: vi.fn(() => new Promise<void>(() => {})),
  fetchThreadTurns: vi.fn().mockResolvedValue({ turns: [], retry_checkpoint_id: null }),
  submitFeedback: vi.fn(),
  removeFeedback: vi.fn(),
  getThreadFeedback: vi.fn().mockResolvedValue([]),
  watchThread: vi.fn(),
}));

import { fetchMarketWatch } from '../../utils/api';
import { useChatMessages } from '../useChatMessages';
import { useMarketWatch } from '../useMarketWatch';

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

// The turn-completion refetch (c) only knows the symbol list; it must MERGE onto
// the live snapshot, not replace it — otherwise the `content`/`timestamp` a
// mid-turn `market_watch_update` (b) just streamed is destroyed and the Status
// panel blanks at turn end.
describe('useMarketWatch – live stamp survives the turn-completion refetch (merge, not replace)', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('preserves content/timestamp when the completion refetch returns the same symbols', async () => {
    mockFetchMarketWatch.mockResolvedValue({ thread_id: 'thread-a', symbols: ['NVDA'] });
    const threadIdRef = { current: 'thread-a' };
    let loading = true;
    const { result, rerender } = renderHookWithProviders(
      () => useMarketWatch('thread-a', loading, threadIdRef),
    );

    // (a) seed from the GET endpoint.
    await waitFor(() => {
      expect(result.current.marketWatch).toEqual({ symbols: ['NVDA'] });
    });

    // (b) a live market_watch_update stamps content + timestamp mid-turn.
    act(() => {
      result.current.setMarketWatch({ symbols: ['NVDA'], content: 'As of 14:30 ET', timestamp: 1_700_000_000 });
    });
    expect(result.current.marketWatch).toEqual({
      symbols: ['NVDA'], content: 'As of 14:30 ET', timestamp: 1_700_000_000,
    });

    // (c) turn completes (isLoading true → false); the refetch returns only the
    // symbol list, and the merge keeps the live stamp.
    mockFetchMarketWatch.mockResolvedValue({ thread_id: 'thread-a', symbols: ['NVDA'] });
    loading = false;
    await act(async () => {
      rerender();
    });
    await waitFor(() => {
      expect(result.current.marketWatch).toEqual({
        symbols: ['NVDA'], content: 'As of 14:30 ET', timestamp: 1_700_000_000,
      });
    });
  });

  it('nulls the state when the completion refetch returns an empty watch list (unwatch)', async () => {
    mockFetchMarketWatch.mockResolvedValue({ thread_id: 'thread-a', symbols: ['NVDA'] });
    const threadIdRef = { current: 'thread-a' };
    let loading = true;
    const { result, rerender } = renderHookWithProviders(
      () => useMarketWatch('thread-a', loading, threadIdRef),
    );

    await waitFor(() => {
      expect(result.current.marketWatch).toEqual({ symbols: ['NVDA'] });
    });
    act(() => {
      result.current.setMarketWatch({ symbols: ['NVDA'], content: 'live', timestamp: 1 });
    });

    // The unwatch turn's completion refetch returns an empty list → chip off.
    mockFetchMarketWatch.mockResolvedValue({ thread_id: 'thread-a', symbols: [] });
    loading = false;
    await act(async () => {
      rerender();
    });
    await waitFor(() => {
      expect(result.current.marketWatch).toBeNull();
    });
  });
});
