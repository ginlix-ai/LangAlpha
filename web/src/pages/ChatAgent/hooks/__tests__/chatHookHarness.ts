/**
 * Shared harness for the useChatMessages hook suites (report-back, reconnect
 * guards, reconnect-on-reactivate). Each test file still declares its own
 * `vi.mock('../../utils/api', ...)` (vitest hoists mocks per file) but delegates
 * the module shape to {@link apiMockModule} so the scaffold lives once.
 */
import { vi, type Mock } from 'vitest';
import { act } from '@testing-library/react';

/**
 * Mock module shape for `../../utils/api` used by all useChatMessages suites.
 * `overrides` replaces individual members for suites that need a different
 * resolved value or a driving implementation — the rest keep the idle defaults.
 */
export function apiMockModule<T extends Record<string, unknown>>(overrides = {} as T) {
  return {
    sendChatMessageStream: vi.fn(),
    sendRetryStream: vi.fn(),
    sendHitlResponse: vi.fn(),
    cancelWorkflow: vi.fn().mockResolvedValue({ success: true }),
    replayThreadHistory: vi.fn().mockResolvedValue(undefined),
    getWorkflowStatus: vi.fn().mockResolvedValue({ can_reconnect: false, status: 'completed' }),
    getReportBackStatus: vi.fn().mockResolvedValue({ pending_report_back: false, report_back_run_id: null }),
    reconnectToWorkflowStream: vi.fn().mockResolvedValue({ disconnected: false, aborted: false }),
    // Default: a mux socket that connects and stays silent (never resolves),
    // like a live stream with no frames. Tests that feed subagent frames
    // drive the REAL v2 client through captureMuxConnections below.
    openThreadMuxStream: vi.fn(() => new Promise<void>(() => {})),
    fetchThreadTurns: vi.fn().mockResolvedValue({ turns: [] }),
    submitFeedback: vi.fn(),
    removeFeedback: vi.fn(),
    getThreadFeedback: vi.fn().mockResolvedValue([]),
    watchThread: vi.fn().mockImplementation(() => ({ abort: new AbortController() })),
    fetchMarketWatch: vi.fn().mockResolvedValue({ thread_id: 't', symbols: [] }),
    ...overrides,
  };
}

/**
 * importOriginal-partial mocks for the stream-handler leaves. Mock the LEAF
 * module, not the `../utils/streamEventHandlers` barrel: the barrel re-exports
 * the mocked leaf, so both import routes stay intercepted, while the session
 * modules (which import leaves directly) are covered too. Stubbed names are
 * replaced; every other export keeps its real implementation, so a factory
 * cannot go stale as its module gains or loses exports.
 */
export function mainHandlersMockModule(
  original: unknown,
  overrides: Record<string, unknown> = {},
) {
  return {
    ...(original as Record<string, unknown>),
    handleReasoningSignal: vi.fn(),
    handleReasoningContent: vi.fn(),
    handleTextContent: vi.fn(),
    handleToolCalls: vi.fn(),
    handleToolCallResult: vi.fn(),
    handleToolCallChunks: vi.fn(),
    handleTodoUpdate: vi.fn(),
    ...overrides,
  };
}

/** Leaf mock for `session/subagents/liveEventHandlers` (same partial rule). */
export function subagentHandlersMockModule(
  original: unknown,
  overrides: Record<string, unknown> = {},
) {
  return {
    ...(original as Record<string, unknown>),
    isSubagentEvent: vi.fn().mockReturnValue(false),
    handleSubagentMessageChunk: vi.fn(),
    handleSubagentToolCallChunks: vi.fn(),
    handleSubagentToolCalls: vi.fn(),
    handleSubagentToolCallResult: vi.fn(),
    handleTaskSteeringAccepted: vi.fn(),
    ...overrides,
  };
}

/** Leaf mock for `session/streamRefs` (same partial rule). */
export function streamRefsMockModule(
  original: unknown,
  overrides: Record<string, unknown> = {},
) {
  return {
    ...(original as Record<string, unknown>),
    getOrCreateTaskRefs: vi.fn().mockReturnValue({
      contentOrderCounterRef: { current: 0 },
      currentReasoningIdRef: { current: null },
      currentToolCallIdRef: { current: null },
    }),
    ...overrides,
  };
}

/** importOriginal-partial mock for `../utils/historyEventHandlers` (same rule). */
export function historyHandlersMockModule(
  original: unknown,
  overrides: Record<string, unknown> = {},
) {
  return {
    ...(original as Record<string, unknown>),
    handleHistoryUserMessage: vi.fn(),
    handleHistoryReasoningSignal: vi.fn(),
    handleHistoryReasoningContent: vi.fn(),
    handleHistoryTextContent: vi.fn(),
    handleHistoryToolCalls: vi.fn(),
    handleHistoryToolCallResult: vi.fn(),
    handleHistoryTodoUpdate: vi.fn(),
    handleHistorySteeringDelivered: vi.fn(),
    ...overrides,
  };
}

/**
 * Flush the mount effect's status-fetch → history-load → branch decision.
 * Every awaited call in that chain is a resolved mock, so flushing micro +
 * macro tasks settles it deterministically.
 */
export async function settleMountEffect() {
  for (let i = 0; i < 2; i++) {
    await act(async () => {
      await new Promise((r) => setTimeout(r, 0));
    });
  }
}

/** A `/status` response with report-back-relevant defaults, overridable per test. */
export function threadStatus(over: Record<string, unknown> = {}) {
  return {
    can_reconnect: false,
    status: 'completed',
    pending_report_back: false,
    active_tasks: [],
    ...over,
  };
}

/** One captured mux socket: cursors sent on connect, and a line feeder. */
export interface MuxConnection {
  threadId: string;
  cursors: string | null;
  /** Feed raw SSE text (one or more frames, blank-line terminated). */
  push: (sse: string) => void;
  /** Server-side close: resolves the transport promise (client reconnects). */
  close: () => void;
  signal: AbortSignal;
}

/** Capture every openThreadMuxStream connection so tests can drive the real
 * v2 mux client with wire-shaped frames (chan_open / task frames / chan_close). */
export function captureMuxConnections(mockOpen: Mock): MuxConnection[] {
  const conns: MuxConnection[] = [];
  mockOpen.mockImplementation(
    (
      threadId: string,
      cursors: string | null,
      onLine: (line: string) => void,
      signal: AbortSignal,
    ) =>
      new Promise<void>((resolve) => {
        conns.push({
          threadId,
          cursors,
          push: (sse: string) => {
            for (const line of sse.split('\n')) onLine(line);
          },
          close: resolve,
          signal,
        });
      }),
  );
  return conns;
}

/** One captured watchThread subscription: thread, callbacks, abort controller. */
export interface WatchCall {
  tid: string;
  cb: (p?: {
    run_id?: string | null;
    needs_input?: string | null;
    cleared?: boolean;
  }) => void | Promise<void>;
  onClosed?: () => void;
  onResubscribed?: () => void;
  /** State-on-attach frame (the backend's /watch snapshot). */
  onSnapshot?: (status: Record<string, unknown>) => void | Promise<void>;
  controller: AbortController;
}

/** Capture every watchThread subscription (callbacks + per-watch controller). */
export function captureWatchCalls(mockWatch: Mock): WatchCall[] {
  const calls: WatchCall[] = [];
  mockWatch.mockImplementation(
    (
      tid: string,
      cb: WatchCall['cb'],
      onClosed?: () => void,
      onResubscribed?: () => void,
      onSnapshot?: WatchCall['onSnapshot'],
    ) => {
      const controller = new AbortController();
      calls.push({ tid, cb, onClosed, onResubscribed, onSnapshot, controller });
      return { abort: controller };
    },
  );
  return calls;
}
