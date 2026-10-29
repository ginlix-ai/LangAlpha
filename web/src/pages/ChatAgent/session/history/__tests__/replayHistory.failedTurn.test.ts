// @vitest-environment node
/**
 * A failed turn replays with its reply marked failed, as the live stream left
 * it, which is what puts Retry on the reply. The server closes the turn with
 * its persisted `error` event; a pre-graph failure replays as the question
 * and that event alone.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { AssistantMessage } from '@/types/chat';

const api = vi.hoisted(() => ({ replayThreadHistory: vi.fn() }));

vi.mock('../../../utils/api', () => ({
  replayThreadHistory: api.replayThreadHistory,
}));

import { loadConversationHistory } from '../replayHistory';
import { buildRuntime, makeDeps, replayOf } from './replayHarness';

const CHUNK = { thread_id: 'thread-1', role: 'assistant', id: 'lc_run--x', agent: 'model:abc' };
const answered = [
  { event: 'user_message', data: { thread_id: 'thread-1', turn_index: 0, content: 'Hi' } },
  { event: 'message_chunk', data: { ...CHUNK, turn_index: 0, content_type: 'text', content: 'Hello' } },
  { event: 'message_chunk', data: { ...CHUNK, turn_index: 0, finish_reason: 'stop' } },
];
const failure = (error: string, extra: Record<string, unknown> = {}) => ({
  event: 'error',
  data: { thread_id: 'thread-1', error, type: 'workflow_error', turn_index: 1, response_id: 'resp-1', ...extra },
});

async function replyAfter(items: Array<Record<string, unknown>>, id: string) {
  const { rt, read } = buildRuntime();
  api.replayThreadHistory.mockImplementation(replayOf(items));
  await loadConversationHistory(rt, makeDeps());
  return read().find((m) => m.id === id) as AssistantMessage;
}

beforeEach(() => vi.clearAllMocks());

describe('history replay: a failed turn', () => {
  it('marks the reply of a turn that failed before the graph, worded by its error', async () => {
    const reply = await replyAfter([
      ...answered,
      { event: 'user_message', data: { thread_id: 'thread-1', turn_index: 1, content: 'Reply BRAVO' } },
      failure('The provider key is not set'),
    ], 'history-assistant-1');

    expect(reply.error).toBe(true);
    expect(reply.isStreaming).toBe(false);
    expect(reply.content).toBe('');
    expect(reply.structuredError).toEqual({ message: 'The provider key is not set' });
  });

  it('keeps the text a turn wrote before it failed', async () => {
    const reply = await replyAfter([
      ...answered,
      { event: 'user_message', data: { thread_id: 'thread-1', turn_index: 1, content: 'Write a story' } },
      { event: 'message_chunk', data: { ...CHUNK, turn_index: 1, content_type: 'text', content: 'Once upon' } },
      failure('Provider timed out'),
    ], 'history-assistant-1');

    expect(reply.error).toBe(true);
    expect(reply.content).toBe('Once upon');
    expect(reply.structuredError).toEqual({ message: 'Provider timed out' });
  });

  it('words a failure the server stored no text for', async () => {
    const reply = await replyAfter([
      ...answered,
      { event: 'user_message', data: { thread_id: 'thread-1', turn_index: 1, content: 'Reply BRAVO' } },
      failure(''),
    ], 'history-assistant-1');

    expect(reply.error).toBe(true);
    expect(reply.structuredError).toEqual({ message: 'An error occurred while processing your request.' });
  });

  it('replays a failure the server classified as its own with no words of its own', async () => {
    const reply = await replyAfter([
      ...answered,
      { event: 'user_message', data: { thread_id: 'thread-1', turn_index: 1, content: 'Write a story' } },
      { event: 'message_chunk', data: { ...CHUNK, turn_index: 1, content_type: 'text', content: 'Once upon' } },
      failure('', { error_kind: 'internal' }),
    ], 'history-assistant-1');

    expect(reply.error).toBe(true);
    expect(reply.content).toBe('Once upon');
    expect(reply.structuredError).toMatchObject({ message: '', kind: 'internal' });
  });

  it('replays a provider failure with its status', async () => {
    const reply = await replyAfter([
      ...answered,
      { event: 'user_message', data: { thread_id: 'thread-1', turn_index: 1, content: 'Reply BRAVO' } },
      failure('Error code: 401 - invalid key', { error_kind: 'upstream', status_code: 401 }),
    ], 'history-assistant-1');

    expect(reply.structuredError).toMatchObject({ message: 'Error code: 401 - invalid key', kind: 'upstream', statusCode: 401 });
  });

  it('marks a reply the server could not load, which no Retry reruns', async () => {
    const reply = await replyAfter([
      ...answered,
      { event: 'user_message', data: { thread_id: 'thread-1', turn_index: 1, content: 'Reply BRAVO' } },
      failure('This response could not be loaded.', { type: 'replay_unavailable', error_kind: 'internal' }),
    ], 'history-assistant-1');

    expect(reply.error).toBe(true);
    expect(reply.unloaded).toBe(true);
    expect(reply.structuredError).toMatchObject({ message: 'This response could not be loaded.', kind: 'internal' });
  });

  it('leaves a failed run open to its Retry', async () => {
    const reply = await replyAfter([
      ...answered,
      { event: 'user_message', data: { thread_id: 'thread-1', turn_index: 1, content: 'Reply BRAVO' } },
      failure('boom'),
    ], 'history-assistant-1');

    expect(reply.unloaded).toBeUndefined();
  });

  it('leaves the turns that did not fail alone', async () => {
    const reply = await replyAfter([
      ...answered,
      { event: 'user_message', data: { thread_id: 'thread-1', turn_index: 1, content: 'Reply BRAVO' } },
      failure('boom'),
    ], 'history-assistant-0');

    expect(reply.error).toBeUndefined();
    expect(reply.content).toBe('Hello');
  });
});
