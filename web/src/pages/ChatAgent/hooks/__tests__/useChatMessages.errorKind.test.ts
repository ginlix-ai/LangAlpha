/**
 * Tests for useChatMessages SSE `error` event routing by `error_kind`.
 *
 * Backend emits two kinds of stream errors:
 *   - `upstream` — LLM provider failed. Render inline on the failed turn so
 *     the user sees it next to the turn they sent. Banner cleared.
 *   - `internal` — our pipeline failed. Surface the error in a banner and
 *     keep the assistant bubble in state (edit/regenerate count bubbles to
 *     find turn_index): failed with its content if it streamed any, else
 *     settled without `error` so the render-time orphan skip hides it.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (k: string) => k }),
}));

vi.mock('@/lib/supabase', () => ({ supabase: null }));

vi.mock('../utils/threadStorage', () => ({
  getStoredThreadId: vi.fn().mockReturnValue(null),
  setStoredThreadId: vi.fn(),
  removeStoredThreadId: vi.fn(),
}));

vi.mock('../../session/stream/mainEventHandlers', async (importOriginal) =>
  (await import('./chatHookHarness')).mainHandlersMockModule(await importOriginal()));

vi.mock('../../session/subagents/liveEventHandlers', async (importOriginal) =>
  (await import('./chatHookHarness')).subagentHandlersMockModule(await importOriginal()));

vi.mock('../../session/streamRefs', async (importOriginal) =>
  (await import('./chatHookHarness')).streamRefsMockModule(await importOriginal()));

vi.mock('../../session/history/historyHandlers', async (importOriginal) =>
  (await import('./chatHookHarness')).historyHandlersMockModule(await importOriginal()));

vi.mock('../../utils/api', async () => (await import('./chatHookHarness')).apiMockModule());

import { sendChatMessageStream } from '../../utils/api';
import { handleTextContent } from '../../session/stream/mainEventHandlers';
import { useChatMessages } from '../useChatMessages';
import { isOrphanAssistantMessage } from '../../components/messageList/contentProjection';

const mockSendStream = sendChatMessageStream as Mock;

function mockStreamWithEvents(events: Array<Record<string, unknown>>) {
  // Intentionally skip the `thread_id` event: it triggers a history-load
  // useEffect that resets messageError to null, which races the test
  // assertions. The error-routing logic under test runs identically
  // regardless of whether we received a thread_id first.
  mockSendStream.mockImplementation(
    async (
      _msg: string,
      _ws: string,
      _tid: string | null,
      { onEvent }: { onEvent: (e: Record<string, unknown>) => void },
    ) => {
      for (const e of events) onEvent(e);
      return { disconnected: false };
    },
  );
}

describe('useChatMessages — SSE error routing by error_kind', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('internal error on an empty bubble keeps it, unflagged and orphan-skipped, with the banner', async () => {
    mockStreamWithEvents([
      {
        event: 'error',
        error: 'DB pool closed',
        error_kind: 'internal',
      },
    ]);

    const { result } = renderHookWithProviders(() => useChatMessages('ws-test'));

    await act(async () => {
      await result.current.handleSendMessage('hello');
    });

    await waitFor(() => {
      const assistants = result.current.messages.filter((m) => m.role === 'assistant');
      expect(assistants).toHaveLength(1);
      const bubble = assistants[0] as unknown as Parameters<typeof isOrphanAssistantMessage>[0];
      expect(bubble.isStreaming).toBe(false);
      expect(bubble.error).toBeFalsy();
      expect(isOrphanAssistantMessage(bubble)).toBe(true);
      expect(result.current.messages.some((m) => m.role === 'user')).toBe(true);
    });

    // The headline alone, as a reload shows it: never the frame's exception text.
    expect(result.current.messageError).toEqual({ message: '', kind: 'internal' });
  });

  it('internal error after streamed content keeps the bubble and its content, marked failed', async () => {
    let seen: string | undefined;
    mockSendStream.mockImplementation(
      async (
        _msg: string,
        _ws: string,
        _tid: string | null,
        { onEvent }: { onEvent: (e: Record<string, unknown>) => void },
      ) => {
        // Seed streamed content on the live bubble, as the (stubbed) text
        // handler would, then fail the turn the way a stream quota stop does.
        const handler = handleTextContent as Mock;
        handler.mockImplementation(({ assistantMessageId, setMessages }: {
          assistantMessageId: string;
          setMessages: (u: (prev: Record<string, unknown>[]) => Record<string, unknown>[]) => void;
        }) => {
          seen = assistantMessageId;
          setMessages((prev) => prev.map((m) => (m.id === assistantMessageId
            ? { ...m, content: 'partial analysis', contentSegments: [{ type: 'text', content: 'partial analysis', order: 1 }] }
            : m)));
        });
        onEvent({ event: 'message_chunk', content: 'partial analysis', content_type: 'text', role: 'assistant' });
        onEvent({ event: 'error', error: 'transport_lost: stream quota exceeded', error_kind: 'internal' });
        return { disconnected: false };
      },
    );

    const { result } = renderHookWithProviders(() => useChatMessages('ws-test'));

    await act(async () => {
      await result.current.handleSendMessage('hello');
    });

    await waitFor(() => {
      const assistants = result.current.messages.filter((m) => m.role === 'assistant') as unknown as
        Array<{ id: string; content?: string; error?: boolean; isStreaming?: boolean }>;
      expect(assistants).toHaveLength(1);
      expect(assistants[0].id).toBe(seen);
      expect(assistants[0].content).toBe('partial analysis');
      expect(assistants[0].error).toBe(true);
      expect(assistants[0].isStreaming).toBe(false);
      // The banner reports it, so the bubble carries no card of its own.
      expect((assistants[0] as { structuredError?: unknown }).structuredError).toBeUndefined();
    });

    const err = result.current.messageError;
    expect(typeof err === 'object' && err && 'kind' in err ? err.kind : null).toBe('internal');
  });

  it('a run that ends failed without an error frame settles as an internal failure', async () => {
    mockSendStream.mockImplementation(
      async (
        _msg: string,
        _ws: string,
        _tid: string | null,
        { onEvent }: { onEvent: (e: Record<string, unknown>) => void },
      ) => {
        (handleTextContent as Mock).mockImplementation(({ assistantMessageId, setMessages }: {
          assistantMessageId: string;
          setMessages: (u: (prev: Record<string, unknown>[]) => Record<string, unknown>[]) => void;
        }) => {
          setMessages((prev) => prev.map((m) => (m.id === assistantMessageId
            ? { ...m, content: 'partial analysis', contentSegments: [{ type: 'text', content: 'partial analysis', order: 1 }] }
            : m)));
        });
        onEvent({ event: 'message_chunk', content: 'partial analysis', content_type: 'text', role: 'assistant' });
        // A stream quota stop: the server cannot write the failure into the
        // stream it ran out of, so the outcome is all the client hears.
        onEvent({ event: 'run_end', run_id: 'run-1', outcome: 'error' });
        return { disconnected: false };
      },
    );

    const { result } = renderHookWithProviders(() => useChatMessages('ws-test'));

    await act(async () => {
      await result.current.handleSendMessage('hello');
    });

    await waitFor(() => {
      const assistants = result.current.messages.filter((m) => m.role === 'assistant') as unknown as
        Array<{ content?: string; error?: boolean; isStreaming?: boolean; structuredError?: unknown }>;
      expect(assistants).toHaveLength(1);
      expect(assistants[0]).toMatchObject({ content: 'partial analysis', error: true, isStreaming: false });
      expect(assistants[0].structuredError).toBeUndefined();
    });
    expect(result.current.messageError).toEqual({ message: '', kind: 'internal' });
  });

  it('upstream error keeps the assistant bubble with structuredError + clears banner', async () => {
    mockStreamWithEvents([
      {
        event: 'error',
        error: '401 invalid api key',
        error_kind: 'upstream',
        status_code: 401,
        hints: ['api_key', 'model_access', 'not_a_real_hint'],
      },
    ]);

    const { result } = renderHookWithProviders(() => useChatMessages('ws-test'));

    await act(async () => {
      await result.current.handleSendMessage('hello');
    });

    await waitFor(() => {
      const assistant = result.current.messages.find((m) => m.role === 'assistant') as
        | { error?: boolean; structuredError?: { kind?: string; statusCode?: number; hints?: string[] } }
        | undefined;
      expect(assistant).toBeTruthy();
      expect(assistant?.error).toBe(true);
      expect(assistant?.structuredError?.kind).toBe('upstream');
      expect(assistant?.structuredError?.statusCode).toBe(401);
      // Unknown hint values are filtered out by isUpstreamHint.
      expect(assistant?.structuredError?.hints).toEqual(['api_key', 'model_access']);
    });

    expect(result.current.messageError).toBe(null);
  });

  it('legacy error (no error_kind) falls through to the unclassified inline path', async () => {
    mockStreamWithEvents([
      {
        event: 'error',
        error: 'Something broke',
      },
    ]);

    const { result } = renderHookWithProviders(() => useChatMessages('ws-test'));

    await act(async () => {
      await result.current.handleSendMessage('hello');
    });

    await waitFor(() => {
      const assistant = result.current.messages.find((m) => m.role === 'assistant') as
        | { error?: boolean; structuredError?: unknown }
        | undefined;
      expect(assistant?.error).toBe(true);
      // Unclassified: the failure rides apart from the reply, with no kind.
      expect(assistant?.structuredError).toEqual({ message: 'Something broke' });
    });
  });
});
