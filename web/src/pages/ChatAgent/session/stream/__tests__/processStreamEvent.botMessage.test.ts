// @vitest-environment node
/**
 * A bot-to-bot message attached live: the run's first frame names its sender,
 * and a synthetic turn comes with no user message, so the row is drawn from
 * the frame. A reconnect replays the same frame and must not draw it twice;
 * history takes the row over by its run id.
 */
import { describe, expect, it, vi } from 'vitest';
import type { ChatMessage } from '@/types/chat';
import type { SSEEvent } from '../../types';
import { createStreamEventProcessor } from '../processStreamEvent';

type Processor = Parameters<typeof createStreamEventProcessor>;

const SOURCE = {
  kind: 'bot_message',
  message_id: 'msg-1',
  from_bot_id: 'bot-2',
  from_name: 'Scout',
  message_kind: 'request',
};

function setup() {
  const messages: ChatMessage[] = [];
  const rt = {
    setMessages: vi.fn((updater: (prev: ChatMessage[]) => ChatMessage[]) => {
      const next = updater(messages.slice());
      messages.length = 0;
      messages.push(...next);
    }),
    queueMessages: vi.fn(),
    flushMessages: vi.fn(),
    updateSubagentCard: vi.fn(),
    lastEventIdRef: { current: null },
    currentRunIdRef: { current: null },
    latestRunIdRef: { current: null },
    latestRunEpochRef: { current: 0 },
    currentMessageRef: { current: 'a1' },
    threadIdRef: { current: 'thread-1' },
    pendingPTCBackfillRef: { current: new Map() },
  };
  const deps = {
    clearModelStatus: () => {}, armReportBack: vi.fn(), refreshWorkspaces: vi.fn(), refreshWakeups: vi.fn(),
    refreshWorkspaceFolder: null, refreshCommands: vi.fn(), onAdmittedTurn: vi.fn(),
  };
  const refs = {
    contentOrderCounterRef: { current: 0 },
    currentReasoningIdRef: { current: null },
    currentToolCallIdRef: { current: null },
    subagentStateRefs: {},
    steeringAtOrderRef: { current: null },
    isReconnect: false,
  };
  const process = createStreamEventProcessor(
    rt as unknown as Processor[0],
    deps as unknown as Processor[1],
    'a1',
    refs as unknown as Processor[3],
    () => null,
  );
  const metadata = (extra: Record<string, unknown> = {}) => process({
    event: 'metadata',
    run_id: 'run-9',
    thread_id: 'thread-1',
    turn_index: 3,
    ...extra,
  } as unknown as SSEEvent);
  return { messages, metadata };
}

describe('a bot-to-bot message attached live', () => {
  it('draws its row from the run\'s first frame', () => {
    const { messages, metadata } = setup();
    metadata({ bot_message_source: SOURCE });

    expect(messages).toHaveLength(1);
    expect(messages[0]).toMatchObject({
      id: 'bot-message-run-9',
      role: 'user',
      runId: 'run-9',
      turnIndex: 3,
      content: '',
      provenance: {
        kind: 'bot_message',
        fromBotId: 'bot-2',
        fromName: 'Scout',
        messageKind: 'request',
        messageId: 'msg-1',
      },
    });
  });

  it('draws it once when a reconnect replays the frame', () => {
    const { messages, metadata } = setup();
    metadata({ bot_message_source: SOURCE });
    metadata({ bot_message_source: SOURCE });

    expect(messages.filter((m) => m.role === 'user')).toHaveLength(1);
  });

  it('draws no row for a turn that carries no bot-message source', () => {
    const { messages, metadata } = setup();
    metadata();

    expect(messages).toEqual([]);
  });
});
