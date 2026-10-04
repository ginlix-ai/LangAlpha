/**
 * The mobile market composer keys its thread's model on the thread this hook
 * sends to, so the id the first send's stream names has to reach the host,
 * not only the ref the next send reads.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';

const sendFlashChatMessage = vi.fn();
vi.mock('../../utils/api', () => ({
  sendFlashChatMessage: (...args: unknown[]) => sendFlashChatMessage(...args),
}));

import { useMarketChat } from '../useMarketChat';

type Emit = (event: Record<string, unknown>) => void;

/** The arg positions `useMarketChat` passes the thread id and event callback in. */
const THREAD_ID = 1;
const ON_EVENT = 2;

beforeEach(() => {
  sendFlashChatMessage.mockReset();
});

describe('useMarketChat thread id', () => {
  it('exposes the thread the first send created and sends the next one there', async () => {
    sendFlashChatMessage.mockImplementationOnce(async (...args: unknown[]) => {
      const emit = args[ON_EVENT] as Emit;
      emit({ event: 'metadata', thread_id: 't-1', run_id: 'run-1' });
      emit({ event: 'message_chunk', content_type: 'text', content: 'It is up 1.2%.' });
    });
    sendFlashChatMessage.mockImplementationOnce(async () => {});
    const { result } = renderHookWithProviders(() => useMarketChat());
    expect(result.current.threadId).toBe('__default__');

    await act(async () => {
      await result.current.handleSendMessage('What is AAPL doing?');
    });
    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(sendFlashChatMessage.mock.calls[0][THREAD_ID]).toBe('__default__');
    expect(result.current.threadId).toBe('t-1');

    await act(async () => {
      await result.current.handleSendMessage('And MSFT?');
    });
    expect(sendFlashChatMessage.mock.calls[1][THREAD_ID]).toBe('t-1');
    expect(result.current.threadId).toBe('t-1');
  });
});
