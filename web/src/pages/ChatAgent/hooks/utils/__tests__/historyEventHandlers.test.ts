/**
 * Pin the deterministic-bubble-id contract for history replay handlers.
 *
 * Earlier code keyed assistant + user history bubbles on
 * `${role}-${pairIndex}-${Date.now()}` — making the same logical bubble
 * look new on every replay. Combined with a non-deduping setMessages
 * reducer, a second `loadConversationHistory` invocation (e.g. via the
 * failed-reconnect setReloadTrigger increment) would silently double the
 * visible history.
 *
 * The deterministic key is `history-{role}-{pairIndex}` — pairIndex is the
 * server-side `turn_index` and is unique per turn. These tests pin that
 * the handler uses ONLY pairIndex as the disambiguator, so a re-replay
 * produces stable ids that the reset-before-replay step in
 * `loadConversationHistory` can clean up cleanly.
 */
import { describe, it, expect } from 'vitest';
import { handleHistoryUserMessage } from '../historyEventHandlers';
import type { MessageRecord } from '../types';

function makeRefs() {
  return {
    recentlySentTracker: { isRecentlySent: () => false },
    currentMessageRef: { current: null as string | null },
    newMessagesStartIndexRef: { current: 0 },
    historyMessagesRef: { current: new Set<string>() },
  };
}

describe('historyEventHandlers — bubble id determinism', () => {
  it('user-message bubble id depends only on pairIndex', () => {
    const inserted: MessageRecord[] = [];
    const setMessages = (updater: ((prev: MessageRecord[]) => MessageRecord[]) | MessageRecord[]) => {
      const next = typeof updater === 'function' ? updater(inserted.slice()) : updater;
      inserted.length = 0;
      inserted.push(...next);
    };
    const refs = makeRefs();
    const assistantMessagesByPair = new Map<number, string>();
    const pairStateByPair = new Map();

    handleHistoryUserMessage({
      event: { event: 'user_message', role: 'user', content: 'hello', turn_index: 3 },
      pairIndex: 3,
      assistantMessagesByPair,
      pairStateByPair,
      refs,
      messages: [],
      setMessages,
    });

    const userBubble = inserted.find((m) => m.role === 'user');
    expect(userBubble).toBeDefined();
    expect(userBubble!.id).toBe('history-user-3');
  });

  it('assistant placeholder id depends only on pairIndex', () => {
    const inserted: MessageRecord[] = [];
    const setMessages = (updater: ((prev: MessageRecord[]) => MessageRecord[]) | MessageRecord[]) => {
      const next = typeof updater === 'function' ? updater(inserted.slice()) : updater;
      inserted.length = 0;
      inserted.push(...next);
    };
    const refs = makeRefs();
    const assistantMessagesByPair = new Map<number, string>();
    const pairStateByPair = new Map();

    handleHistoryUserMessage({
      event: { event: 'user_message', role: 'user', content: 'hello', turn_index: 7 },
      pairIndex: 7,
      assistantMessagesByPair,
      pairStateByPair,
      refs,
      messages: [],
      setMessages,
    });

    expect(assistantMessagesByPair.get(7)).toBe('history-assistant-7');
    const assistantBubble = inserted.find((m) => m.role === 'assistant');
    expect(assistantBubble).toBeDefined();
    expect(assistantBubble!.id).toBe('history-assistant-7');
  });

  it('two replays of the same turn produce the same bubble ids', () => {
    // The crux of the fix: deterministic ids let a re-replay's bubbles
    // collide with the first replay's, which combined with the reset in
    // `loadConversationHistory` (filter `isHistory: true` from messages
    // before re-running) keeps the rendered list stable.
    const idsRun1: string[] = [];
    const idsRun2: string[] = [];

    for (const collector of [idsRun1, idsRun2]) {
      const inserted: MessageRecord[] = [];
      const setMessages = (updater: ((prev: MessageRecord[]) => MessageRecord[]) | MessageRecord[]) => {
        const next = typeof updater === 'function' ? updater(inserted.slice()) : updater;
        inserted.length = 0;
        inserted.push(...next);
      };
      const refs = makeRefs();
      const assistantMessagesByPair = new Map<number, string>();
      const pairStateByPair = new Map();
      handleHistoryUserMessage({
        event: { event: 'user_message', role: 'user', content: 'hi', turn_index: 0 },
        pairIndex: 0,
        assistantMessagesByPair,
        pairStateByPair,
        refs,
        messages: [],
        setMessages,
      });
      collector.push(...inserted.map((m) => m.id as string));
    }

    expect(idsRun1).toEqual(idsRun2);
  });
});
