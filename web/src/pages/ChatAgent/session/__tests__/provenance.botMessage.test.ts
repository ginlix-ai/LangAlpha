// @vitest-environment node
/**
 * A delivered bot-to-bot message: the turn's own message is the framed text
 * the sender wrote for the model. The session layer turns it into a row that
 * names its sender, strips the frame, and draws the same row whether it came
 * from history replay or was attached live from the stream's first frame.
 */
import { describe, it, expect } from 'vitest';
import {
  botMessageRow,
  botMessageSourceFromMetadata,
  parseSenderFrame,
  stripSenderFrame,
  withLiveBotMessageRow,
} from '../provenance';
import { handleHistoryUserMessage } from '../history/historyHandlers';
import type { BotMessageSource } from '@/types/api';
import type { ChatMessage } from '@/types/chat';
import type { MessageRecord } from '../../hooks/utils/types';

const SOURCE: BotMessageSource = {
  kind: 'bot_message',
  message_id: 'msg-1',
  from_bot_id: 'bot-2',
  from_name: 'Scout',
  message_kind: 'request',
};

const tailed = (kind: BotMessageSource['message_kind']) => ({
  ...SOURCE,
  message_kind: kind,
});

function replay(event: Record<string, unknown>, live: MessageRecord[] = []): MessageRecord[] {
  const messages: MessageRecord[] = [...live];
  handleHistoryUserMessage({
    event,
    pairIndex: 4,
    assistantMessagesByPair: new Map(),
    pairStateByPair: new Map(),
    refs: {
      recentlySentTracker: { isRecentlySent: () => false },
      currentMessageRef: { current: null },
      newMessagesStartIndexRef: { current: 0 },
    },
    setMessages: (updater) => {
      const next = updater(messages.slice());
      messages.length = 0;
      messages.push(...next);
    },
  });
  return messages.filter((m) => m.role === 'user');
}

describe('botMessageSourceFromMetadata', () => {
  it('reads the source the delivery stamps on the turn', () => {
    expect(botMessageSourceFromMetadata({
      kind: 'bot_message',
      message_id: 'msg-1',
      from_bot_id: 'bot-2',
      from_name: 'Scout',
      message_kind: 'request',
    })).toEqual(SOURCE);
  });

  it('keeps a sender with no bot id, as the user\'s and the system\'s are', () => {
    expect(botMessageSourceFromMetadata({
      kind: 'bot_message', from_bot_id: null, from_name: 'the user', message_kind: 'message',
    })).toEqual({
      kind: 'bot_message', from_bot_id: null, from_name: 'the user', message_kind: 'message',
    });
  });

  it('names no sender when the value names none', () => {
    expect(botMessageSourceFromMetadata(null)).toBeNull();
    expect(botMessageSourceFromMetadata('Scout')).toBeNull();
    expect(botMessageSourceFromMetadata({ kind: 'report_back', from_name: 'Scout' })).toBeNull();
    expect(botMessageSourceFromMetadata({ kind: 'bot_message', from_name: '   ' })).toBeNull();
    // An unknown kind still draws a message from a named sender.
    expect(botMessageSourceFromMetadata({
      kind: 'bot_message', from_name: 'Scout', message_kind: 'shout',
    })).toMatchObject({ message_kind: 'message' });
  });
});

describe('stripSenderFrame', () => {
  it('keeps the sender\'s words out of the frame for kinds whose frame has none', () => {
    expect(stripSenderFrame('[Message from Scout]\nNVDA moved 3%.', tailed('message'))).toBe('NVDA moved 3%.');
    expect(stripSenderFrame('[Notice from Scout]\nHeads up.', tailed('notice'))).toBe('Heads up.');
    expect(stripSenderFrame('[Message from Scout]\nline one\nline two', tailed('message'))).toBe('line one\nline two');
  });

  it('drops the server\'s instruction line for the kinds that carry one', () => {
    expect(stripSenderFrame(
      '[Request from Scout]\nThey are asking you to do this; reply when you have an answer.\nPull the filings.',
      tailed('request'),
    )).toBe('Pull the filings.');
    expect(stripSenderFrame(
      '[Reply from Scout]\nThis answers something you asked for.\nDone.\nNext.',
      tailed('reply'),
    )).toBe('Done.\nNext.');
    expect(stripSenderFrame('[Report from Scout]\nThis is the result of work you asked for.', tailed('report'))).toBe('');
  });

  it('leaves text that is not framed as it is', () => {
    expect(stripSenderFrame('plain words', SOURCE)).toBe('plain words');
  });});

describe('parseSenderFrame', () => {
  it('names the sender and kind from an older row\'s first line', () => {
    expect(parseSenderFrame('[Reply from Scout]\nThis answers something you asked for.\nDone.')).toEqual({
      source: { kind: 'bot_message', from_bot_id: null, from_name: 'Scout', message_kind: 'reply' },
      body: 'Done.',
    });
  });

  it('reads no frame from ordinary text', () => {
    expect(parseSenderFrame('What moved NVDA?')).toBeNull();
  });
});

describe('botMessageRow', () => {
  it('carries the sender as provenance and the body without the frame', () => {
    const row = botMessageRow(SOURCE, { id: 'row-1', timestamp: new Date(0), turnIndex: 4 },
      `[Request from Scout]\nThey are asking you to do this; reply when you have an answer.\nDeliver the brief.`);
    expect(row).toMatchObject({
      id: 'row-1',
      role: 'user',
      content: 'Deliver the brief.',
      turnIndex: 4,
      provenance: {
        kind: 'bot_message',
        fromBotId: 'bot-2',
        fromName: 'Scout',
        messageKind: 'request',
        messageId: 'msg-1',
      },
    });
  });
});

describe('withLiveBotMessageRow', () => {
  const messages: ChatMessage[] = [
    { id: 'a1', role: 'assistant', content: '', contentType: 'text', timestamp: new Date(0), isStreaming: true,
      contentSegments: [], reasoningProcesses: {}, toolCallProcesses: {} },
  ];

  it('places the row ahead of the bubble the attach opened, keyed on its run', () => {
    const next = withLiveBotMessageRow(messages, 'run-9', SOURCE, 'a1', 3);
    expect(next.map((m) => m.id)).toEqual(['bot-message-run-9', 'a1']);
    expect(next[0]).toMatchObject({
      role: 'user', runId: 'run-9', turnIndex: 3,
      provenance: { kind: 'bot_message', fromName: 'Scout', messageKind: 'request' },
    });
  });

  it('draws one row however many times the frame arrives', () => {
    const once = withLiveBotMessageRow(messages, 'run-9', SOURCE, 'a1');
    const twice = withLiveBotMessageRow(once, 'run-9', SOURCE, 'a1');
    expect(twice).toBe(once);
    expect(twice.filter((m) => m.role === 'user')).toHaveLength(1);
  });
});

describe('history replay of a delivered bot message', () => {
  it('draws the sender row, with the frame stripped, and the run its turn opened', () => {
    const rows = replay({
      content: '[Request from Scout]\nThey are asking you to do this; reply when you have an answer.\nDeliver the brief.',
      query_type: 'system',
      report_run_id: 'run-4',
      timestamp: 1_700_000_000,
      metadata: {
        bot_message_source: {
          kind: 'bot_message', message_id: 'msg-1', from_bot_id: 'bot-2',
          from_name: 'Scout', message_kind: 'request',
        },
      },
    });
    expect(rows).toEqual([expect.objectContaining({
      id: 'history-user-4',
      content: 'Deliver the brief.',
      turnIndex: 4,
      runId: 'run-4',
      provenance: {
        kind: 'bot_message', fromBotId: 'bot-2', fromName: 'Scout',
        messageKind: 'request', messageId: 'msg-1',
      },
    })]);
  });

  it('draws an older row from its frame when no source was stamped', () => {
    const rows = replay({
      content: '[Message from Scout]\nNVDA moved 3%.',
      query_type: 'system',
      timestamp: 1_700_000_000,
      metadata: {},
    });
    expect(rows).toEqual([expect.objectContaining({
      content: 'NVDA moved 3%.',
      provenance: { kind: 'bot_message', fromBotId: null, fromName: 'Scout', messageKind: 'message' },
    })]);
  });

  it('takes over the row a live attach drew before the reload', () => {
    const live: MessageRecord[] = [
      { id: 'bot-message-run-4', role: 'user', content: '', runId: 'run-4', provenance: { kind: 'bot_message' } },
      { id: 'a-live', role: 'assistant', content: '' },
    ];
    const rows = replay({
      content: '[Message from Scout]\nNVDA moved 3%.',
      query_type: 'system',
      report_run_id: 'run-4',
      metadata: { bot_message_source: { kind: 'bot_message', from_name: 'Scout', message_kind: 'message' } },
    }, live);
    expect(rows.map((m) => m.id)).toEqual(['history-user-4']);
  });

  it('draws a replayed bot message with no report_run_id and no run to take over', () => {
    const rows = replay({
      content: '[Message from Scout]\nNVDA moved 3%.',
      query_type: 'system',
      timestamp: 1_700_000_000,
      metadata: {
        bot_message_source: {
          kind: 'bot_message', message_id: 'msg-1', from_bot_id: 'bot-2',
          from_name: 'Scout', message_kind: 'message',
        },
      },
    });
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      content: 'NVDA moved 3%.',
      provenance: { kind: 'bot_message', fromName: 'Scout', messageKind: 'message' },
    });
    // No run was reported for the row, so nothing claims to be its turn.
    expect(rows[0].runId).toBeUndefined();
  });

  it('keeps a system turn with no sender and no frame hidden, as it always was', () => {
    expect(replay({ content: '<system>done</system>', query_type: 'system', metadata: {} })).toEqual([]);
  });
});
