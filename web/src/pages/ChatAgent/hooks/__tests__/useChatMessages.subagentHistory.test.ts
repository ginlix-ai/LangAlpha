/**
 * The subagent history getters are state, not views over refs: they keep their
 * identity while the history is unchanged, so a memoized consumer stays quiet
 * through a streamed turn, and take a new one on every history change, so the
 * same consumer never misses one.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { useMemo } from 'react';
import { act } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';
import { settleMountEffect } from './chatHookHarness';

// One `t` for the whole run, as i18next hands out: a fresh one per call would
// churn every callback that reads it.
vi.mock('react-i18next', () => {
  const t = (k: string) => k;
  return { useTranslation: () => ({ t }) };
});

vi.mock('@/lib/supabase', () => ({ supabase: null }));

vi.mock('../utils/threadStorage', () => ({
  getStoredThreadId: vi.fn().mockReturnValue(null),
  setStoredThreadId: vi.fn(),
  removeStoredThreadId: vi.fn(),
}));

vi.mock('../../utils/api', async () => (await import('./chatHookHarness')).apiMockModule({
  getSubagentTaskStatus: vi.fn(),
  getSubagentTaskHistory: vi.fn(),
}));

import { getSubagentTaskHistory, sendChatMessageStream } from '../../utils/api';
import { useChatMessages } from '../useChatMessages';

const mockTaskHistory = getSubagentTaskHistory as Mock;
const mockSendStream = sendChatMessageStream as Mock;

/** The hook plus a consumer memoized on each getter, as a compiled component holds them. */
function renderWithConsumers(threadId?: string) {
  return renderHookWithProviders(() => {
    const chat = useChatMessages('ws-test', threadId);
    const { getSubagentHistory, resolveSubagentIdToAgentId } = chat;
    const seenHistory = useMemo(() => getSubagentHistory('task:abc'), [getSubagentHistory]);
    const seenSpawn = useMemo(() => resolveSubagentIdToAgentId('toolu_spawn'), [resolveSubagentIdToAgentId]);
    const seenResult = useMemo(() => resolveSubagentIdToAgentId('toolu_result'), [resolveSubagentIdToAgentId]);
    return { chat, seenHistory, seenSpawn, seenResult };
  });
}

describe('useChatMessages: subagent history getters', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('keeps the getters stable across a render that leaves the history alone', async () => {
    const { result, rerender } = renderWithConsumers('thread-1');
    await settleMountEffect();

    const { getSubagentHistory, resolveSubagentIdToAgentId, hydrateTaskTranscript } = result.current.chat;
    rerender();

    expect(result.current.chat.getSubagentHistory).toBe(getSubagentHistory);
    expect(result.current.chat.resolveSubagentIdToAgentId).toBe(resolveSubagentIdToAgentId);
    expect(result.current.chat.hydrateTaskTranscript).toBe(hydrateTaskTranscript);
  });

  it('re-renders a memoized consumer when a hydrated transcript is all that changed', async () => {
    mockTaskHistory.mockResolvedValue({
      items: [
        { event: 'message_chunk', data: { agent: 'task:abc', role: 'assistant', content_type: 'text', content: 'brief' } },
      ],
    });
    const { result } = renderWithConsumers('thread-1');
    await settleMountEffect();
    expect(result.current.seenHistory).toBeNull();
    const before = result.current.chat.getSubagentHistory;

    let landed: Awaited<ReturnType<typeof before>> | undefined;
    await act(async () => {
      landed = await result.current.chat.hydrateTaskTranscript('task:abc', { status: 'completed' });
    });

    expect(result.current.seenHistory).toMatchObject({ agentId: 'task:abc', status: 'completed' });
    expect(result.current.seenHistory?.messages.length).toBeGreaterThan(0);
    expect(result.current.chat.getSubagentHistory).not.toBe(before);
    // Resolves with the entry itself, for callers still holding the old getter.
    expect(before('task:abc')).toBeNull();
    expect(landed).toMatchObject({ agentId: 'task:abc', status: 'completed' });
  });

  it('carries the stream processor\'s tool-call mappings to resolveSubagentIdToAgentId', async () => {
    mockSendStream.mockImplementation(
      async (_msg: string, _ws: string, _tid: string | null, { onEvent }: { onEvent: (e: Record<string, unknown>) => void }) => {
        onEvent({ event: 'tool_calls', agent: 'main', tool_calls: [{ name: 'Task', id: 'toolu_spawn', args: { description: 'screen' } }] });
        // A spawn artifact maps its call to the task it opened...
        onEvent({ event: 'artifact', artifact_type: 'task', agent: 'main', tool_call_id: 'toolu_spawn', payload: { task_id: 'abc', action: 'spawned' } });
        // ...and so does a tool result that carries the task in its artifact.
        onEvent({ event: 'tool_call_result', agent: 'tools', tool_call_id: 'toolu_result', content: 'resumed', artifact: { task_id: 'def' } });
        return { disconnected: false };
      },
    );
    const { result } = renderWithConsumers();
    expect(result.current.seenSpawn).toBe('toolu_spawn');

    await act(async () => {
      await result.current.chat.handleSendMessage('screen AI names');
    });

    expect(result.current.chat.resolveSubagentIdToAgentId('toolu_spawn')).toBe('task:abc');
    expect(result.current.seenSpawn).toBe('task:abc');
    expect(result.current.seenResult).toBe('task:def');
  });
});
