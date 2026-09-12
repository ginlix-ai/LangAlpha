/**
 * A resume that answers an order names the attempt it answers.
 *
 * The server refuses a keyed attempt it was handed no verdict for, so absence
 * is a refusal and never an approval: every keyed request the interrupt raised
 * has to reach `order_decisions`, whatever the batch around it looks like. The
 * positional list still goes out beside it, so an interrupt that mixes a keyed
 * order with an ordinary call is unambiguous on both readings.
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

const INTERRUPT_ID = 'int-order-keyed';

const ORDER = { action: 'place', mode: 'live', vendor: 'moomoo', symbol: 'US.AAPL' };

/** One keyed order and one ordinary direct call, stopped together. */
const mixedInterrupt = {
  event: 'interrupt',
  interrupt_id: INTERRUPT_ID,
  kind: 'order_approval',
  action_requests: [
    {
      name: 'mcp__moomoo__trading_order_place',
      args: { code: 'US.AAPL', side: 'BUY', qty: '1' },
      tool_call_id: 'call-aapl',
      attempt_id: 'attempt-aapl',
      order: ORDER,
    },
    { name: 'mcp__moomoo__quote_stock_quote', args: { code: 'US.MSFT' } },
  ],
};

function cardsOf(messages: readonly unknown[]) {
  return messages
    .filter((m): m is AssistantMessage => (m as AssistantMessage).role === 'assistant')
    .flatMap((m) => Object.entries(m.toolApprovals || {}));
}

describe('useChatMessages: answering an interrupt that keyed an order', () => {
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
      onEvent(mixedInterrupt);
      return { disconnected: false };
    });
  });

  it('sends the keyed verdict by attempt id and the whole batch positionally', async () => {
    const { result } = renderHookWithProviders(() => useChatMessages('ws-x', 'th-x'));
    await waitFor(() => expect(mockReplay).toHaveBeenCalled());
    await settleMountEffect();
    await act(async () => {
      await result.current.handleSendMessage('buy one AAPL and quote MSFT', false);
    });

    await waitFor(() => expect(cardsOf(result.current.messages)).toHaveLength(2));
    const [order, quote] = cardsOf(result.current.messages)
      .sort((a, b) => a[1].actionIndex - b[1].actionIndex);

    // The card is what carries the attempt to the click, which is the whole
    // chain this locks: interrupt to card to resume.
    expect(order[1].attemptId).toBe('attempt-aapl');
    expect(order[1].order?.mode).toBe('live');
    expect(quote[1].attemptId).toBeUndefined();

    await act(async () => {
      result.current.handleRejectToolCall(
        quote[0], INTERRUPT_ID,
        { index: quote[1].actionIndex, count: quote[1].actionCount },
        'not now', quote[1].attemptId,
      );
      await new Promise((r) => setTimeout(r, 0));
    });
    expect(mockSendHitl).not.toHaveBeenCalled();

    await act(async () => {
      result.current.handleApproveToolCall(
        order[0], INTERRUPT_ID,
        { index: order[1].actionIndex, count: order[1].actionCount },
        order[1].attemptId,
      );
      await new Promise((r) => setTimeout(r, 0));
    });

    await waitFor(() => expect(mockSendHitl).toHaveBeenCalledTimes(1));
    expect(mockSendHitl.mock.calls[0][2]).toEqual({
      [INTERRUPT_ID]: {
        decisions: [{ type: 'approve' }, { type: 'reject', message: 'not now' }],
        order_decisions: { 'attempt-aapl': { type: 'approve' } },
      },
    });
  });
});
