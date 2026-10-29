/**
 * Shared fixtures for the history-replay suites: a HistoryRuntime whose
 * setMessages is a real reducer over a local array, so a test reads the
 * messages the replay actually built rather than a mock's call log.
 */
import { vi } from 'vitest';

import type { HistoryRuntime } from '../../runtime';
import type { HistoryInterruptInfo } from '../../types';
import { createSubagentHistoryStore } from '../../subagents/historyStore';
import type { ChatMessage } from '@/types/chat';

type Ref<T> = { current: T };
const ref = <T,>(current: T): Ref<T> => ({ current });

export function buildRuntime() {
  let messages: ChatMessage[] = [];
  const rt = {
    workspaceId: 'ws-1',
    threadId: 'thread-1',
    t: (key: string) => key,
    updateTodoListCard: null,
    setMessages: ((updater: (prev: ChatMessage[]) => ChatMessage[]) => {
      messages = updater(messages);
    }) as HistoryRuntime['setMessages'],
    setIsLoadingHistory: vi.fn(),
    setHistoryLoadFailed: vi.fn(),
    setIsCompacting: vi.fn(),
    setMessageError: vi.fn(),
    setFallbackSuggestion: vi.fn(),
    setThreadModels: vi.fn(),
    setTokenUsage: vi.fn(),
    setReloadTrigger: vi.fn(),
    setThreadId: vi.fn(),
    historyLoadingRef: ref(false),
    replayedRunIdsRef: ref([] as string[]),
    historyLoadedKeyRef: ref<string | null>(null),
    historyHasUnresolvedInterruptRef: ref(false),
    unresolvedHistoryInterruptRef: ref([] as HistoryInterruptInfo[]),
    lastRenderedTurnIndexRef: ref<number | null>(null),
    newMessagesStartIndexRef: ref(0),
    historyPendingTaskToolCallIdsRef: ref([] as string[]),
    currentMessageRef: ref<string | null>(null),
    latestRunIdRef: ref<string | null>(null),
    latestRunEpochRef: ref(0),
    lastEventIdRef: ref<number | string | null>(null),
    renderedInterruptIdsRef: ref(new Set<string>()),
    subagentHistory: createSubagentHistoryStore(),
    recentlySentTrackerRef: ref({ isRecentlySent: () => false }),
    requestKeyRef: ref({ settle: () => {} }),
    offloadBatchRef: ref(null),
  } as unknown as HistoryRuntime;
  return { rt, read: () => messages };
}

/**
 * Fresh per call: `vi.clearAllMocks()` clears calls but not implementations, so
 * a shared object would leak one test's `mockImplementation` into the rest.
 */
export function makeDeps() {
  return {
    applyFallbackSuggestion: vi.fn(),
    loadFeedback: vi.fn().mockResolvedValue(undefined),
    projectSubagentHistory: vi.fn(),
  };
}

/** Turns a flat `{ event, data }` list into a replayThreadHistory implementation. */
export function replayOf(items: Array<Record<string, unknown>>) {
  return async (_threadId: string, onEvent: (e: Record<string, unknown>) => void) => {
    for (const item of items) {
      onEvent({ event: item.event, ...(item.data as Record<string, unknown>) });
    }
  };
}
