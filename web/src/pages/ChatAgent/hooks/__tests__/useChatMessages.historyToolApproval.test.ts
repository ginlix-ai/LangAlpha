/**
 * A stored, unanswered order approval arms the composer's pending slot, so the
 * card a reload replays is one the user can still answer. An approval naming no
 * attempt is left inert instead: order governance reads its decisions by
 * attempt id, so nothing on the server would read that card's verdict.
 *
 * The slot is what routes the approve/reject click back to the interrupt it
 * belongs to, so leaving it null on the paused branch is what stranded a thread
 * that stopped on an order: the card rendered with controls that answered
 * nothing. Both entries into the slot are pinned here, because they are
 * different code paths -- the paused branch reads stored history, and an active
 * run strips the replayed card and re-arms from the reconnect stream instead. A
 * credit pause on the same branch must keep arming either way.
 *
 * Drives the REAL hook (api module mocked), the way the dedup suite does.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { waitFor } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';
import { settleMountEffect } from './chatHookHarness';

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

import { getWorkflowStatus, replayThreadHistory, reconnectToWorkflowStream } from '../../utils/api';
import { useChatMessages } from '../useChatMessages';
import type { AssistantMessage, ContentSegment } from '@/types/chat';

const mockStatus = getWorkflowStatus as Mock;
const mockReplay = replayThreadHistory as Mock;
const mockReconnect = reconnectToWorkflowStream as Mock;

/** The stop as a live-order approval persisted it: one direct MCP call, named
 *  by the ledger row whose verdict answers it. */
const ORDER_REQUEST = [{
  name: 'mcp__moomoo__place_order', args: { symbol: 'AAPL', qty: 1 }, attempt_id: 'attempt-1',
}];
/** The same stop with no ledger id, which no middleware here can answer. */
const STAMPLESS_REQUEST = [{ name: 'mcp__moomoo__place_order', args: { symbol: 'AAPL', qty: 1 } }];
const PAUSE_REQUEST = [{ type: 'credit_pause', message: 'Out of credits.' }];

function replayStoppedOn(actionRequests: unknown[]) {
  return (_tid: string, onEvent: (e: Record<string, unknown>) => void) => {
    onEvent({ event: 'user_message', turn_index: 0, content: 'buy one share', role: 'user' });
    onEvent({ event: 'interrupt', turn_index: 0, interrupt_id: 'int-1', action_requests: actionRequests });
    return Promise.resolve();
  };
}

function segmentsOf(messages: readonly unknown[], type: string): ContentSegment[] {
  return messages
    .filter((m): m is AssistantMessage => (m as AssistantMessage).role === 'assistant')
    .flatMap((m) => ((m.contentSegments as ContentSegment[] | undefined) || []).filter((s) => s.type === type));
}

describe('useChatMessages: unanswered interrupts from history on a paused thread', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockReplay.mockReset();
    mockStatus.mockReset();
    // Paused: the run is over, so the reconnect branch is not the one taken.
    mockStatus.mockResolvedValue({ can_reconnect: false, status: 'completed', active_tasks: [] });
    mockReconnect.mockReset();
    mockReconnect.mockResolvedValue({ disconnected: false, aborted: false });
  });

  it('renders a stored tool approval and arms it for an answer', async () => {
    mockReplay.mockImplementation(replayStoppedOn(ORDER_REQUEST));

    const { result } = renderHookWithProviders(() => useChatMessages('ws-x', 'th-x'));
    await waitFor(() => expect(mockReplay).toHaveBeenCalled());
    await settleMountEffect();

    await waitFor(() => expect(segmentsOf(result.current.messages, 'tool_approval')).toHaveLength(1));
    // Armed: `pendingInterrupt` is what a click on the card resumes against.
    await waitFor(() => expect(result.current.pendingInterrupt?.type).toBe('tool_approval'));
    expect(result.current.pendingInterrupt?.interruptId).toBe('int-1');
  });

  // Arming this one would be worse than leaving it alone: the click resumes the
  // graph, no middleware reads a verdict it cannot key, and the call runs with
  // the rejection dropped. The card still replays as the record of the stop.
  it('leaves an approval naming no attempt inert', async () => {
    mockReplay.mockImplementation(replayStoppedOn(STAMPLESS_REQUEST));

    const { result } = renderHookWithProviders(() => useChatMessages('ws-x', 'th-x'));
    await waitFor(() => expect(mockReplay).toHaveBeenCalled());
    await settleMountEffect();

    await waitFor(() => expect(segmentsOf(result.current.messages, 'tool_approval')).toHaveLength(1));
    expect(result.current.pendingInterrupt?.type).not.toBe('tool_approval');
  });

  // The active-run branch strips the replayed card and lets the reconnect
  // stream redeliver the interrupt through the LIVE projection, so this arms
  // from `projectLiveInterrupt` rather than from the stored history entry.
  describe('when the run is still active and the reconnect stream redelivers the stop', () => {
    function redeliver(actionRequests: unknown[]) {
      mockStatus.mockResolvedValue({
        can_reconnect: true, status: 'running', run_id: 'run-1', active_tasks: [], pending_report_back: false,
      });
      mockReplay.mockImplementation(replayStoppedOn(actionRequests));
      mockReconnect.mockImplementation(async (...args: unknown[]) => {
        const onEvent = args[3] as (e: Record<string, unknown>) => void;
        onEvent({ event: 'interrupt', interrupt_id: 'int-1', action_requests: actionRequests });
        return { disconnected: false, aborted: false };
      });
    }

    it('arms the redelivered tool approval for an answer', async () => {
      redeliver(ORDER_REQUEST);

      const { result } = renderHookWithProviders(() => useChatMessages('ws-x', 'th-x'));
      await waitFor(() => expect(mockReconnect).toHaveBeenCalled());
      await settleMountEffect();

      await waitFor(() => expect(segmentsOf(result.current.messages, 'tool_approval')).toHaveLength(1));
      await waitFor(() => expect(result.current.pendingInterrupt?.type).toBe('tool_approval'));
      expect(result.current.pendingInterrupt?.interruptId).toBe('int-1');
    });

    it('still arms a redelivered credit pause', async () => {
      redeliver(PAUSE_REQUEST);

      const { result } = renderHookWithProviders(() => useChatMessages('ws-x', 'th-x'));
      await waitFor(() => expect(mockReconnect).toHaveBeenCalled());
      await settleMountEffect();

      await waitFor(() => expect(result.current.pendingInterrupt?.type).toBe('credit_pause'));
      expect(result.current.pendingInterrupt?.interruptId).toBe('int-1');
    });
  });

  it('still re-arms a stored credit pause, which Resume can answer', async () => {
    mockReplay.mockImplementation(replayStoppedOn(PAUSE_REQUEST));

    const { result } = renderHookWithProviders(() => useChatMessages('ws-x', 'th-x'));
    await waitFor(() => expect(mockReplay).toHaveBeenCalled());
    await settleMountEffect();

    await waitFor(() => expect(result.current.pendingInterrupt?.type).toBe('credit_pause'));
    expect(result.current.pendingInterrupt?.interruptId).toBe('int-1');
  });
});
