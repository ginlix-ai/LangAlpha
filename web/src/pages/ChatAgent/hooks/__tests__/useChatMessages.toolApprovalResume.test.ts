/**
 * A refused resume must hand the tool-approval card back.
 *
 * The click settles the card the moment it is made, because that is the only
 * optimistic state the user gets. Admission can still refuse the resume (429
 * or 503), and then no run opened and the backend is still holding the stopped
 * call, so a card left reading "Approved" with its controls gone is a dead
 * end: nothing on screen can answer the interrupt, and nothing says the
 * decision never went out.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, waitFor } from '@testing-library/react';
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

import {
  getWorkflowStatus,
  replayThreadHistory,
  fetchThreadTurns,
  sendChatMessageStream,
  sendHitlResponse,
} from '../../utils/api';
import { useChatMessages } from '../useChatMessages';
import type { AssistantMessage } from '@/types/chat';

const mockStatus = getWorkflowStatus as Mock;
const mockReplay = replayThreadHistory as Mock;
const mockSend = sendChatMessageStream as Mock;
const mockSendHitl = sendHitlResponse as Mock;
const mockTurns = fetchThreadTurns as Mock;

const INTERRUPT_ID = 'int-order-1';

const toolInterrupt = {
  event: 'interrupt',
  interrupt_id: INTERRUPT_ID,
  action_requests: [
    {
      name: 'mcp__moomoo__trading_order_place',
      args: { acc_id: '12345678', code: 'US.AAPL', side: 'BUY', qty: '1', price: '100' },
    },
  ],
};

const BATCH_ID = 'int-order-batch';

/** Two stopped calls under one interrupt, which the resume must answer together. */
const batchedInterrupt = {
  event: 'interrupt',
  interrupt_id: BATCH_ID,
  action_requests: [
    { name: 'mcp__moomoo__trading_order_place', args: { code: 'US.AAPL', side: 'BUY', qty: '1' } },
    { name: 'mcp__moomoo__trading_order_place', args: { code: 'US.MSFT', side: 'SELL', qty: '2' } },
  ],
};

/** The card for the stopped call, wherever it landed. */
function approvalCard(messages: readonly unknown[]) {
  return messages
    .filter((m): m is AssistantMessage => (m as AssistantMessage).role === 'assistant')
    .map((m) => m.toolApprovals?.[INTERRUPT_ID])
    .find(Boolean);
}

describe('useChatMessages: a refused resume of a tool approval', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockReplay.mockReset();
    mockReplay.mockResolvedValue(undefined);
    mockSend.mockReset();
    mockSendHitl.mockReset();
    mockTurns.mockReset();
    mockTurns.mockResolvedValue({ turns: [], retry_checkpoint_id: null });
    mockStatus.mockReset();
    mockStatus.mockResolvedValue({ can_reconnect: false, status: 'completed' });
    mockSend.mockImplementation(async (...args: unknown[]) => {
      const onEvent = args[5] as (e: Record<string, unknown>) => void;
      onEvent(toolInterrupt);
      return { disconnected: false };
    });
  });

  async function raiseApproval() {
    const { result } = renderHookWithProviders(() => useChatMessages('ws-x', 'th-x'));
    await waitFor(() => expect(mockReplay).toHaveBeenCalled());
    await settleMountEffect();
    await act(async () => {
      await result.current.handleSendMessage('buy one AAPL', false);
    });
    await waitFor(() => expect(approvalCard(result.current.messages)?.status).toBe('pending'));
    return result;
  }

  it('puts the card back to pending so the stopped call can still be answered', async () => {
    const result = await raiseApproval();
    mockSendHitl.mockRejectedValue({ status: 429, rateLimitInfo: { message: 'Out of credits' } });

    await act(async () => {
      result.current.handleApproveToolCall(INTERRUPT_ID, INTERRUPT_ID, { index: 0, count: 1 });
      await new Promise((r) => setTimeout(r, 0));
    });
    await waitFor(() => expect(mockSendHitl).toHaveBeenCalledTimes(1));
    await settleMountEffect();

    expect(approvalCard(result.current.messages)?.status).toBe('pending');
  });

  it('accepts the retried decision instead of half-filling the batch', async () => {
    const result = await raiseApproval();
    mockSendHitl.mockRejectedValueOnce({ status: 503, errorInfo: { message: 'Service unavailable' } });
    mockSendHitl.mockResolvedValue({ disconnected: false, aborted: false });

    await act(async () => {
      result.current.handleApproveToolCall(INTERRUPT_ID, INTERRUPT_ID, { index: 0, count: 1 });
      await new Promise((r) => setTimeout(r, 0));
    });
    await waitFor(() => expect(mockSendHitl).toHaveBeenCalledTimes(1));
    await settleMountEffect();

    // The retry has to reach the wire with the full decision set: a slot left
    // filled from the refused attempt would keep the batch gate from firing,
    // and a card left settled would offer nothing to click in the first place.
    expect(approvalCard(result.current.messages)?.status).toBe('pending');
    await act(async () => {
      result.current.handleApproveToolCall(INTERRUPT_ID, INTERRUPT_ID, { index: 0, count: 1 });
      await new Promise((r) => setTimeout(r, 0));
    });
    await waitFor(() => expect(mockSendHitl).toHaveBeenCalledTimes(2));
    expect(mockSendHitl.mock.calls[1][2]).toEqual({
      [INTERRUPT_ID]: { decisions: [{ type: 'approve' }] },
    });
  });
});

describe('useChatMessages: an interrupt that stopped two calls', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockReplay.mockReset();
    mockReplay.mockResolvedValue(undefined);
    mockSend.mockReset();
    mockSendHitl.mockReset();
    mockSendHitl.mockResolvedValue({ disconnected: false, aborted: false });
    mockTurns.mockReset();
    mockTurns.mockResolvedValue({ turns: [], retry_checkpoint_id: null });
    mockStatus.mockReset();
    mockStatus.mockResolvedValue({ can_reconnect: false, status: 'completed' });
    mockSend.mockImplementation(async (...args: unknown[]) => {
      const onEvent = args[5] as (e: Record<string, unknown>) => void;
      onEvent(batchedInterrupt);
      return { disconnected: false };
    });
  });

  // The server errors unless the resume carries one decision per stopped call,
  // in the order the interrupt raised them. The slot each answer fills comes
  // from the card that was clicked, so a deferred render cannot file the
  // second answer into the first one's slot.
  it('sends both decisions, in order, only once both cards are answered', async () => {
    const { result } = renderHookWithProviders(() => useChatMessages('ws-x', 'th-x'));
    await waitFor(() => expect(mockReplay).toHaveBeenCalled());
    await settleMountEffect();
    await act(async () => {
      await result.current.handleSendMessage('place both orders', false);
    });

    const cards = () => result.current.messages
      .filter((m): m is AssistantMessage => (m as AssistantMessage).role === 'assistant')
      .flatMap((m) => Object.entries(m.toolApprovals || {}));
    await waitFor(() => expect(cards()).toHaveLength(2));
    const [first, second] = cards().sort((a, b) => a[1].actionIndex - b[1].actionIndex);

    await act(async () => {
      result.current.handleRejectToolCall(
        second[0], BATCH_ID, { index: second[1].actionIndex, count: second[1].actionCount }, 'not this one',
      );
      await new Promise((r) => setTimeout(r, 0));
    });
    // One card still unanswered, so nothing may go out yet.
    expect(mockSendHitl).not.toHaveBeenCalled();

    await act(async () => {
      result.current.handleApproveToolCall(
        first[0], BATCH_ID, { index: first[1].actionIndex, count: first[1].actionCount },
      );
      await new Promise((r) => setTimeout(r, 0));
    });
    await waitFor(() => expect(mockSendHitl).toHaveBeenCalledTimes(1));
    expect(mockSendHitl.mock.calls[0][2]).toEqual({
      [BATCH_ID]: {
        decisions: [{ type: 'approve' }, { type: 'reject', message: 'not this one' }],
      },
    });
  });
});
