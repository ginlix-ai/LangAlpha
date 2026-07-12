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

vi.mock('../../utils/api', () => ({
  sendChatMessageStream: vi.fn(),
  sendHitlResponse: vi.fn(),
  replayThreadHistory: vi.fn().mockResolvedValue(undefined),
  getWorkflowStatus: vi.fn().mockResolvedValue({ can_reconnect: false, status: 'completed' }),
  reconnectToWorkflowStream: vi.fn().mockResolvedValue({ disconnected: false, aborted: false }),
  streamSubagentTaskEvents: vi.fn(),
  fetchThreadTurns: vi.fn().mockResolvedValue({ turns: [], retry_checkpoint_id: null }),
  submitFeedback: vi.fn(),
  removeFeedback: vi.fn(),
  getThreadFeedback: vi.fn().mockResolvedValue([]),
  watchThread: vi.fn(),
}));

import { sendChatMessageStream, getWorkflowStatus } from '../../utils/api';
import { useChatMessages } from '../useChatMessages';

const mockSendStream = sendChatMessageStream as Mock;
const mockStatus = getWorkflowStatus as Mock;

// Positional tail of sendChatMessageStream: (..., onRunIdResolved, signal, requestKey).
const latchOf = (args: unknown[]) =>
  args[args.length - 3] as (rid: string, tid: string) => void;
const requestKeyOf = (args: unknown[]) => args[args.length - 1] as string | null;

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
      await result.current.handleSendMessage('hello', false);
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
      await result.current.handleSendMessage('hello', false);
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
      await result.current.handleSendMessage('hello', false);
    });
    await act(async () => {
      await result.current.handleSendMessage('hello', false);
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
      await result.current.handleSendMessage('hello', false);
    });
    await act(async () => {
      await result.current.handleSendMessage('hello', false);
    });

    const first = requestKeyOf(mockSendStream.mock.calls[0]);
    const second = requestKeyOf(mockSendStream.mock.calls[1]);
    expect(first).toBeTruthy();
    expect(second).toBeTruthy();
    expect(second).not.toBe(first);
  });
});
