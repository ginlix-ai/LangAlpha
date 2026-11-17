/**
 * A Home turn that ran on the cloud stand-in says so under its message, the
 * same whether the live stream's `turn_placement` frame or the replay's
 * query metadata named it.
 */
import { describe, expect, it, vi } from 'vitest';
import { screen } from '@testing-library/react';

import { renderWithProviders } from '@/test/utils';
import type { ChatMessage } from '@/types/chat';
import type { HomeFallback } from '@/types/sse';

const api = vi.hoisted(() => ({
  replayThreadHistory: vi.fn(),
  getHomeHandbacks: vi.fn(async () => ({ stand_in_workspace_id: 'ws-stand-in', handbacks: [] })),
}));
vi.mock('@/pages/ChatAgent/utils/api', async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  replayThreadHistory: api.replayThreadHistory,
}));
vi.mock('@/pages/ChatAgent/utils/api/workspaces', async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  getHomeHandbacks: api.getHomeHandbacks,
}));
vi.mock('@/hooks/useUser', () => ({ useUser: () => ({ user: null }) }));

import MessageList from '../../MessageList';
import { MessageActionsProvider } from '../MessageActionsContext';
import type { MessageRecord } from '../types';
import { createStreamEventProcessor } from '../../../session/stream/processStreamEvent';
import { loadConversationHistory } from '../../../session/history/replayHistory';
import { buildRuntime, makeDeps, replayOf } from '../../../session/history/__tests__/replayHarness';
import type { SSEEvent } from '../../../session/types';

type Processor = Parameters<typeof createStreamEventProcessor>;

const FALLBACK: HomeFallback = {
  computer_id: 'c-mac',
  computer_name: 'Office Mac',
  cloud_computer_id: 'c-cloud',
  cloud_name: 'Cloud One',
  at: '2026-11-15T09:00:00Z',
  reason: 'unreachable',
};

function draw(messages: MessageRecord[]) {
  renderWithProviders(
    <MessageActionsProvider actions={{}}>
      <MessageList messages={messages} />
    </MessageActionsProvider>,
  );
}

/** The live stream as far as the frame: a sent message and its reply bubble. */
function liveTurn(...placements: Array<typeof FALLBACK | null>): MessageRecord[] {
  let messages: ChatMessage[] = [
    { id: 'u1', role: 'user', content: 'Summarize my notes', contentType: 'text', timestamp: new Date(), isStreaming: false, turnIndex: 0 },
    { id: 'a1', role: 'assistant', content: 'Done.', isStreaming: false, contentSegments: [], reasoningProcesses: {}, toolCallProcesses: {}, turnIndex: 0 },
  ] as unknown as ChatMessage[];
  const setMessages = (updater: (prev: ChatMessage[]) => ChatMessage[]) => {
    messages = updater(messages);
  };
  const rt = {
    setMessages,
    queueMessages: setMessages,
    flushMessages: vi.fn(),
    lastEventIdRef: { current: null },
    threadIdRef: { current: 'thread-1' },
    threadId: 'thread-1',
    setMessageError: vi.fn(),
    t: (k: string) => k,
  };
  const process = createStreamEventProcessor(
    rt as unknown as Processor[0],
    { readMessage: (id: string) => messages.find((m) => m.id === id) } as unknown as Processor[1],
    'a1',
    { contentOrderCounterRef: { current: 0 }, subagentStateRefs: {}, isReconnect: false } as unknown as Processor[3],
    () => null,
  );
  for (const placement of placements.length ? placements : [FALLBACK]) {
    process({ event: 'turn_placement', home_fallback: placement, workspace_id: 'home-1' } as unknown as SSEEvent);
  }
  return messages as unknown as MessageRecord[];
}

describe('the turn badge of a Home turn on the cloud', () => {
  it('comes from the live turn_placement frame', () => {
    const messages = liveTurn();
    expect(messages[0].homeFallback).toEqual(FALLBACK);
    expect(messages[1].homeFallback).toBeUndefined();
    draw(messages);
    expect(screen.getByTestId('home-turn-on-cloud')).toHaveTextContent('Ran on Cloud One');
  });

  it('leaves when a retry of the turn runs back on Home', () => {
    const messages = liveTurn(FALLBACK, null);
    expect(messages[0].homeFallback).toBeUndefined();
    draw(messages);
    expect(screen.queryByTestId('home-turn-on-cloud')).toBeNull();
  });

  it('comes from the turn metadata on replay', async () => {
    const { rt, read } = buildRuntime();
    api.replayThreadHistory.mockImplementation(replayOf([
      {
        event: 'user_message',
        data: { thread_id: 'thread-1', turn_index: 0, content: 'Summarize my notes', metadata: { home_fallback: FALLBACK } },
      },
      {
        event: 'message_chunk',
        data: { thread_id: 'thread-1', turn_index: 0, role: 'assistant', agent: 'main', content_type: 'text', content: 'Done.' },
      },
    ]));
    await loadConversationHistory(rt, makeDeps());
    const messages = read() as unknown as MessageRecord[];
    expect(messages.find((m) => m.role === 'user')?.homeFallback).toEqual(FALLBACK);
    draw(messages);
    expect(screen.getByTestId('home-turn-on-cloud')).toHaveTextContent('Ran on Cloud One');
  });

  it('draws nothing on a turn that ran where Home lives', () => {
    draw([{ id: 'u1', role: 'user', content: 'Hi', contentType: 'text', timestamp: new Date(), isStreaming: false, turnIndex: 0 }]);
    expect(screen.queryByTestId('home-turn-on-cloud')).toBeNull();
  });
});
