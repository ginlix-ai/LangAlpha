/**
 * Shared harness for the useChatMessages hook suites (report-back, reconnect
 * guards, reconnect-on-reactivate). Each test file still declares its own
 * `vi.mock('../../utils/api', ...)` (vitest hoists mocks per file) but delegates
 * the module shape to {@link apiMockModule} so the scaffold lives once.
 */
import { vi, type Mock } from 'vitest';
import { act } from '@testing-library/react';

/** Mock module shape for `../../utils/api` used by all useChatMessages suites. */
export function apiMockModule() {
  return {
    sendChatMessageStream: vi.fn(),
    sendHitlResponse: vi.fn(),
    cancelWorkflow: vi.fn().mockResolvedValue({ success: true }),
    replayThreadHistory: vi.fn().mockResolvedValue(undefined),
    getWorkflowStatus: vi.fn().mockResolvedValue({ can_reconnect: false, status: 'completed' }),
    getReportBackStatus: vi.fn().mockResolvedValue({ pending_report_back: false, report_back_run_id: null }),
    reconnectToWorkflowStream: vi.fn().mockResolvedValue({ disconnected: false, aborted: false }),
    streamSubagentTaskEvents: vi.fn().mockResolvedValue(undefined),
    fetchThreadTurns: vi.fn().mockResolvedValue({ turns: [], retry_checkpoint_id: null }),
    submitFeedback: vi.fn(),
    removeFeedback: vi.fn(),
    getThreadFeedback: vi.fn().mockResolvedValue([]),
    watchThread: vi.fn().mockImplementation(() => ({ abort: new AbortController() })),
    fetchMarketWatch: vi.fn().mockResolvedValue({ thread_id: 't', symbols: [] }),
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
