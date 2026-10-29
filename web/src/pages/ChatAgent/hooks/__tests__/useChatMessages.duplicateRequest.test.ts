/**
 * Tests for v4 request_key wiring in useChatMessages.
 *
 * A logical send carries one request_key, reused verbatim when the same send
 * is retransmitted after a failure whose response never arrived. The server
 * dedups on it with HTTP 409 `duplicate_request` (+ the accepted run's
 * identity for the owner); the hook adopts that run — latch ids, reconnect —
 * instead of surfacing an error banner for a turn that actually exists.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (k: string) => k }),
}));

vi.mock('@/lib/supabase', () => ({ supabase: null }));

vi.mock('../utils/threadStorage', () => ({
  getStoredThreadId: vi.fn().mockReturnValue(null),
  setStoredThreadId: vi.fn(),
  removeStoredThreadId: vi.fn(),
}));

vi.mock('../../utils/api', async () => (await import('./chatHookHarness')).apiMockModule());

import { sendChatMessageStream, getWorkflowStatus, replayThreadHistory } from '../../utils/api';
import { getStoredThreadId } from '../utils/threadStorage';
import { useChatMessages } from '../useChatMessages';

const mockSendStream = sendChatMessageStream as Mock;
const mockStatus = getWorkflowStatus as Mock;
const mockReplay = replayThreadHistory as Mock;

// onRunIdResolved and requestKey live in the options object (4th arg).
const latchOf = (args: unknown[]) => (args[3] as { onRunIdResolved: (rid: string, tid: string) => void }).onRunIdResolved;
const requestKeyOf = (args: unknown[]) => (args[3] as { requestKey: string | null }).requestKey;

function duplicateError(extra: Record<string, unknown> = {}) {
  return Object.assign(new Error('This request was already accepted'), {
    status: 409,
    errorInfo: {
      code: 'duplicate_request',
      message: 'This request was already accepted; reconnect to the existing run instead of resending.',
      ...extra,
    },
  });
}

describe('useChatMessages — request_key dedup (409 duplicate_request)', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockStatus.mockResolvedValue({ can_reconnect: false, status: 'completed' });
  });

  it('adopts the accepted run: latches its thread id, reconnects, no error banner', async () => {
    mockSendStream.mockRejectedValue(
      duplicateError({ thread_id: 'th-dup', run_id: 'run-dup', run_status: 'in_progress' }),
    );

    const { result } = renderHookWithProviders(() => useChatMessages('ws-test'));

    await act(async () => {
      await result.current.handleSendMessage('hello');
    });

    // Reconnect probes the ADOPTED thread (proof the ids were latched from
    // the 409 detail), and no error banner appears for a turn that exists.
    await waitFor(() => {
      expect(mockStatus).toHaveBeenCalledWith('th-dup');
    });
    expect(result.current.messageError).toBe(null);
  });

  it('falls through to the plain error surface when run identity is not disclosed', async () => {
    // Fail-closed backend answer for a foreign key collision: bare conflict,
    // no thread/run ids. Not adoptable — surface the error.
    mockSendStream.mockRejectedValue(duplicateError());

    const { result } = renderHookWithProviders(() => useChatMessages('ws-test'));

    await act(async () => {
      await result.current.handleSendMessage('hello');
    });

    await waitFor(() => {
      expect(result.current.messageError).toBeTruthy();
    });
    expect(mockStatus).not.toHaveBeenCalledWith('th-dup');
  });

  it('reuses the request_key when the identical send is retransmitted after a lost response', async () => {
    // First copy dies before response headers (latch never fires).
    mockSendStream.mockRejectedValueOnce(new TypeError('Load failed'));
    mockSendStream.mockResolvedValue({ disconnected: false, aborted: false, contentLocation: null });

    const { result } = renderHookWithProviders(() => useChatMessages('ws-test'));

    await act(async () => {
      await result.current.handleSendMessage('hello');
    });
    await act(async () => {
      await result.current.handleSendMessage('hello');
    });

    const first = requestKeyOf(mockSendStream.mock.calls[0]);
    const second = requestKeyOf(mockSendStream.mock.calls[1]);
    expect(first).toBeTruthy();
    expect(second).toBe(first);
  });

  it('mints a fresh key once headers proved acceptance (a later identical send is a new logical send)', async () => {
    mockSendStream.mockImplementation(async (...args: unknown[]) => {
      latchOf(args)('run-ok', 'th-ok'); // headers arrived — key consumed
      return { disconnected: false, aborted: false, contentLocation: null };
    });

    const { result } = renderHookWithProviders(() => useChatMessages('ws-test'));

    await act(async () => {
      await result.current.handleSendMessage('hello');
    });
    await act(async () => {
      await result.current.handleSendMessage('hello');
    });

    const first = requestKeyOf(mockSendStream.mock.calls[0]);
    const second = requestKeyOf(mockSendStream.mock.calls[1]);
    expect(first).toBeTruthy();
    expect(second).toBeTruthy();
    expect(second).not.toBe(first);
  });

  it('takes the thread a new chat\'s first message landed in, when its Retry finds the copy already answered', async () => {
    // A new chat whose thread pre-create failed (this mock has none), so its
    // first send went out with no thread. The server made one and accepted the
    // send, but the response was lost, so the view never learned that thread.
    (getStoredThreadId as Mock).mockReturnValue('__default__');
    try {
      mockSendStream.mockRejectedValueOnce(new TypeError('Failed to fetch'));
      const { result } = renderHookWithProviders(() => useChatMessages('ws-test'));
      await act(async () => {
        await result.current.handleSendMessage('hello');
      });
      expect(result.current.threadId).toBe('__default__');

      mockSendStream.mockRejectedValueOnce(
        duplicateError({ thread_id: 'th-dup', run_id: 'run-dup', run_status: 'completed' }),
      );
      await act(async () => {
        await result.current.handleRetry();
      });

      // The view moves to that thread and loads the turn the copy answered.
      await waitFor(() => expect(result.current.threadId).toBe('th-dup'));
      await waitFor(() => expect(mockReplay.mock.calls.some(([tid]) => tid === 'th-dup')).toBe(true));
      expect(result.current.messageError).toBe(null);
    } finally {
      (getStoredThreadId as Mock).mockReturnValue(null);
    }
  });
});
