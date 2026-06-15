/**
 * Pins the frontend data-provenance pipeline across BOTH dispatch paths.
 *
 * `useChatMessages` has two independent event dispatchers — the live stream
 * (`handleSendMessage` → `createStreamEventProcessor`) and history replay
 * (`replayThreadHistory`). Replay does NOT reuse the live processor, so a
 * `provenance` event must be wired into both or accessed-data sources vanish
 * on reload. These tests exercise the REAL handlers (streamEventHandlers /
 * historyEventHandlers are not mocked) so they assert the actual
 * `message.provenanceRecords` state, including the keying scheme that keeps
 * multiple web_search URLs sharing one tool_call_id distinct.
 *
 * Neutral placeholder URLs/paths only — no production data.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';

// ---------------------------------------------------------------------------
// Mocks — only i18n / supabase / threadStorage / api are mocked. The real
// stream + history event handlers run so we observe `provenanceRecords`.
// ---------------------------------------------------------------------------

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (k: string) => k }),
}));

vi.mock('@/lib/supabase', () => ({ supabase: null }));

vi.mock('../utils/threadStorage', () => ({
  // Default: no stored thread (live-send test). The history test overrides
  // this to a real thread so the mount load-history effect fires.
  getStoredThreadId: vi.fn().mockReturnValue(null),
  setStoredThreadId: vi.fn(),
  removeStoredThreadId: vi.fn(),
}));

vi.mock('../../utils/api', () => ({
  sendChatMessageStream: vi.fn(),
  sendHitlResponse: vi.fn(),
  replayThreadHistory: vi.fn().mockResolvedValue(undefined),
  getWorkflowStatus: vi.fn().mockResolvedValue({ can_reconnect: false, status: 'completed' }),
  reconnectToWorkflowStream: vi.fn(),
  streamSubagentTaskEvents: vi.fn(),
  fetchThreadTurns: vi.fn().mockResolvedValue({ turns: [], retry_checkpoint_id: null }),
  submitFeedback: vi.fn(),
  removeFeedback: vi.fn(),
  getThreadFeedback: vi.fn().mockResolvedValue([]),
  watchThread: vi.fn(),
}));

import { sendChatMessageStream, replayThreadHistory } from '../../utils/api';
import { getStoredThreadId } from '../utils/threadStorage';
import { useChatMessages } from '../useChatMessages';
import type { AssistantMessage } from '@/types/chat';

const mockSendStream = sendChatMessageStream as Mock;
const mockReplay = replayThreadHistory as Mock;
const mockGetStoredThreadId = getStoredThreadId as Mock;

type OnEvent = (e: Record<string, unknown>) => void;

// Two web_search records share one tool_call_id (the backend emits one record
// per result URL). The keying scheme must keep both distinct.
const SEARCH_TOOL_CALL_ID = 'tc-search-1';
const SEARCH_URL_A = 'https://example.com/alpha';
const SEARCH_URL_B = 'https://example.com/beta';

function provenanceEvent(over: Record<string, unknown>): Record<string, unknown> {
  return {
    event: 'provenance',
    record_id: 'rec-default',
    timestamp: '2024-01-01T00:00:00Z',
    source_type: 'web_search',
    identifier: SEARCH_URL_A,
    title: 'Alpha',
    detail: 'company_overview',
    provider: 'tavily',
    tool_call_id: SEARCH_TOOL_CALL_ID,
    result_sha256: 'a'.repeat(64),
    result_size: 123,
    result_snippet: 'snippet alpha',
    args: { symbol: 'AAPL', period: '1y', api_key: '[redacted]' },
    ...over,
  };
}

describe('useChatMessages — provenance dispatch (live + history)', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockReplay.mockResolvedValue(undefined);
    mockGetStoredThreadId.mockReturnValue(null);
  });

  // -------------------------------------------------------------------------
  // (a) LIVE path: handleSendMessage / processEvent populates provenanceRecords
  // -------------------------------------------------------------------------
  it('live stream provenance events populate message.provenanceRecords', async () => {
    mockSendStream.mockImplementation(
      async (
        _msg: string,
        _ws: string,
        _tid: string | null,
        _hist: unknown[],
        _plan: boolean,
        onEvent: OnEvent,
      ) => {
        onEvent({ event: 'thread_id', thread_id: 'thread-prov-live' });
        // Two web_search records sharing one tool_call_id (distinct URLs).
        onEvent(provenanceEvent({ record_id: 'rec-a', identifier: SEARCH_URL_A, title: 'Alpha' }));
        onEvent(provenanceEvent({ record_id: 'rec-b', identifier: SEARCH_URL_B, title: 'Beta' }));
        // A file_read record with no tool_call_id (keyed by source_type:identifier).
        onEvent(provenanceEvent({
          record_id: 'rec-file',
          source_type: 'file_read',
          identifier: '.agents/user/notes/example.md',
          provider: undefined,
          tool_call_id: undefined,
          title: 'example.md',
        }));
        // A subagent-emitted record (agent="task:..") must still attach to the
        // main turn's assistant message.
        onEvent(provenanceEvent({
          record_id: 'rec-sub',
          agent: 'task:abc123',
          source_type: 'mcp_tool',
          identifier: 'financial-data:get_quote',
          provider: 'mcp:financial-data',
          tool_call_id: 'tc-mcp-1',
          title: undefined,
        }));
        return { disconnected: false };
      },
    );

    const { result } = renderHookWithProviders(() => useChatMessages('ws-prov-live'));

    await act(async () => {
      await result.current.handleSendMessage('research example', false);
    });

    await waitFor(() => {
      const assistants = result.current.messages.filter(
        (m): m is AssistantMessage => m.role === 'assistant',
      );
      expect(assistants.length).toBeGreaterThan(0);
      const records = assistants[0].provenanceRecords;
      expect(records).toBeDefined();
      // All four records survive — no key collision dropped the second URL.
      expect(Object.keys(records!).length).toBe(4);
    });

    const assistant = result.current.messages.find(
      (m): m is AssistantMessage => m.role === 'assistant',
    )!;
    const records = assistant.provenanceRecords!;
    const identifiers = Object.values(records).map((r) => r.identifier).sort();

    // Both web_search URLs present despite the shared tool_call_id.
    expect(identifiers).toContain(SEARCH_URL_A);
    expect(identifiers).toContain(SEARCH_URL_B);

    // Subagent record kept its agent attribution.
    const subRecord = Object.values(records).find((r) => r.record_id === 'rec-sub');
    expect(subRecord).toBeDefined();
    expect(subRecord!.agent).toBe('task:abc123');

    // Fingerprint fields round-tripped flat off the event.
    const alpha = Object.values(records).find((r) => r.record_id === 'rec-a')!;
    expect(alpha.result_sha256).toBe('a'.repeat(64));
    expect(alpha.result_size).toBe(123);
    expect(alpha.result_snippet).toBe('snippet alpha');
    // detail (data-kind slug) round-trips through the shared event→record mapper.
    expect(alpha.detail).toBe('company_overview');
    // Captured tool-call args (secrets pre-redacted) survive the live path.
    expect(alpha.args).toEqual({ symbol: 'AAPL', period: '1y', api_key: '[redacted]' });
  });

  // -------------------------------------------------------------------------
  // (b) HISTORY path: replayThreadHistory re-attaches persisted provenance
  //     events to the correct turn's assistant message.
  // -------------------------------------------------------------------------
  it('replay provenance events re-attach records to the right turn', async () => {
    // A stored thread makes the mount load-history effect fire replayThreadHistory.
    mockGetStoredThreadId.mockReturnValue('thread-prov-history');
    mockReplay.mockImplementation(async (_threadId: string, onEvent: OnEvent) => {
      // Turn 0: a user message creates the assistant placeholder for the pair,
      // then provenance events with the same turn_index re-attach.
      onEvent({
        event: 'user_message',
        role: 'user',
        content: 'historical question',
        turn_index: 0,
        timestamp: '2024-01-01T00:00:00Z',
      });
      onEvent(provenanceEvent({
        event: 'provenance',
        record_id: 'rec-h-a',
        identifier: SEARCH_URL_A,
        title: 'Alpha',
        turn_index: 0,
        response_id: 'resp-0',
      }));
      onEvent(provenanceEvent({
        event: 'provenance',
        record_id: 'rec-h-b',
        identifier: SEARCH_URL_B,
        title: 'Beta',
        turn_index: 0,
        response_id: 'resp-0',
      }));
    });

    const { result } = renderHookWithProviders(() => useChatMessages('ws-prov-history'));

    await waitFor(() => {
      expect(mockReplay).toHaveBeenCalled();
      const assistant = result.current.messages.find(
        (m): m is AssistantMessage =>
          m.role === 'assistant' && m.id === 'history-assistant-0',
      );
      expect(assistant).toBeDefined();
      expect(assistant!.provenanceRecords).toBeDefined();
      // Both web_search URLs re-attached despite the shared tool_call_id.
      expect(Object.keys(assistant!.provenanceRecords!).length).toBe(2);
    });

    const assistant = result.current.messages.find(
      (m): m is AssistantMessage =>
        m.role === 'assistant' && m.id === 'history-assistant-0',
    )!;
    const identifiers = Object.values(assistant.provenanceRecords!)
      .map((r) => r.identifier)
      .sort();
    expect(identifiers).toEqual([SEARCH_URL_A, SEARCH_URL_B]);

    // Captured tool-call args (secrets pre-redacted) survive the replay path.
    const alpha = Object.values(assistant.provenanceRecords!).find((r) => r.record_id === 'rec-h-a')!;
    expect(alpha.args).toEqual({ symbol: 'AAPL', period: '1y', api_key: '[redacted]' });
  });
});
