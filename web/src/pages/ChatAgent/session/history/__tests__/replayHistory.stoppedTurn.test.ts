// @vitest-environment node
/**
 * A hard-stopped turn must replay with its Stopped chip.
 *
 * The backend closes a stopped turn with a synthetic `finish_reason:
 * "stopped"` chunk that carries no content and no content_type. The handler
 * stamps `stopped` for it, but it only does so if the dispatcher routes a
 * content-less close there; the handler's own suite calls it directly and
 * cannot see a dispatcher that closes the message some other way.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { AssistantMessage } from '@/types/chat';

const api = vi.hoisted(() => ({ replayThreadHistory: vi.fn() }));

vi.mock('../../../utils/api', () => ({
  replayThreadHistory: api.replayThreadHistory,
}));

import { loadConversationHistory } from '../replayHistory';
import { buildRuntime, makeDeps, replayOf } from './replayHarness';

const CHUNK = { thread_id: 'thread-1', turn_index: 0, role: 'assistant', id: 'lc_run--x', agent: 'model:abc' };

/** A reloaded turn, stopped mid-answer, ending on the close given. */
function stoppedTurn(finishReason: string) {
  return [
    { event: 'user_message', data: { thread_id: 'thread-1', turn_index: 0, content: 'Summarise the filing' } },
    { event: 'message_chunk', data: { ...CHUNK, content_type: 'reasoning_signal', content: 'start' } },
    { event: 'message_chunk', data: { ...CHUNK, content_type: 'reasoning', content: 'Reading the risk factors' } },
    { event: 'message_chunk', data: { ...CHUNK, content_type: 'reasoning_signal', content: 'complete' } },
    { event: 'message_chunk', data: { ...CHUNK, content_type: 'text', content: 'The filing flags three' } },
    { event: 'message_chunk', data: { ...CHUNK, finish_reason: finishReason } },
  ];
}

async function assistantAfter(items: Array<Record<string, unknown>>) {
  const { rt, read } = buildRuntime();
  api.replayThreadHistory.mockImplementation(replayOf(items));
  await loadConversationHistory(rt, makeDeps());
  return read().find((m) => m.id === 'history-assistant-0') as AssistantMessage;
}

beforeEach(() => vi.clearAllMocks());

describe('history replay: a stopped turn', () => {
  it('keeps the partial answer and stamps the message stopped', async () => {
    const assistant = await assistantAfter(stoppedTurn('stopped'));

    expect(assistant.content).toBe('The filing flags three');
    expect(assistant.stopped).toBe(true);
    expect(assistant.isStreaming).toBe(false);
  });

  it('does not stamp a turn that finished normally', async () => {
    const assistant = await assistantAfter(stoppedTurn('stop'));

    expect(assistant.content).toBe('The filing flags three');
    expect(assistant.stopped).toBeUndefined();
    expect(assistant.isStreaming).toBe(false);
  });
});
