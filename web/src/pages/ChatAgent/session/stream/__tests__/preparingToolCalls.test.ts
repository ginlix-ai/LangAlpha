// @vitest-environment node
/**
 * Parallel calls stream their arguments side by side, and each keeps its
 * preparing row until it lands. The producer sends a model call's tool_calls,
 * one event per call, ahead of its finish, so a call still preparing at the
 * finish is one it dropped: the finish takes its row away, as the turn's end
 * does, for the main agent and a subagent alike.
 */
import { describe, expect, it, vi } from 'vitest';
import type { PendingToolCallChunk } from '@/types/chat';
import type { MessageRecord } from '../../../hooks/utils/types';
import { preparingCallsOf } from '../../../components/messageList/pendingToolCalls';
import { handleTextContent, handleToolCallChunks, handleToolCalls } from '../mainEventHandlers';
import { failAssistantMessage } from '../finalizeMessage';
import {
  handleSubagentMessageChunk, handleSubagentToolCallChunks, handleSubagentToolCalls,
} from '../../subagents/liveEventHandlers';
import type { StreamRefs } from '../../streamRefs';

const chunk = (index: number, id: string) => ({ index, id, name: 'bash', args: '{"command": "ls' });
const call = (id: string) => ({ id, name: 'bash', args: { command: 'ls' } });

const preparing = (msg: MessageRecord | undefined) =>
  preparingCallsOf(
    msg?.pendingToolCallChunks as Record<string, PendingToolCallChunk> | undefined,
    (msg?.toolCallProcesses ?? {}) as Record<string, unknown>,
    new Set(),
  )?.calls.map((c) => c.toolCallId) ?? [];

function mainStream() {
  let messages: MessageRecord[] = [{ id: 'a1', role: 'assistant', contentSegments: [], toolCallProcesses: {}, isStreaming: true }];
  const setMessages = (update: (prev: MessageRecord[]) => MessageRecord[]) => {
    messages = update(messages);
  };
  const refs = {
    contentOrderCounterRef: { current: 0 },
    currentReasoningIdRef: { current: null },
    currentToolCallIdRef: { current: null },
  } as StreamRefs;
  return {
    chunks: (...chunks: ReturnType<typeof chunk>[]) => handleToolCallChunks({ assistantMessageId: 'a1', chunks, setMessages }),
    land: (id: string) => handleToolCalls({ assistantMessageId: 'a1', toolCalls: [call(id)], finishReason: 'tool_calls', refs, setMessages }),
    finish: (finishReason: string) => handleTextContent({ assistantMessageId: 'a1', content: '', finishReason, refs, setMessages }),
    end: (settle: (msg: MessageRecord) => MessageRecord) => setMessages((prev) => prev.map(settle)),
    list: () => messages,
    bubble: () => messages[0],
  };
}

describe('preparing tool calls on the main stream', () => {
  it('keeps each parallel call preparing until it lands, and drops a dropped one at the finish', () => {
    const stream = mainStream();
    stream.chunks(chunk(0, 'call-a'), chunk(1, 'call-b'), chunk(2, 'call-c'));
    expect(preparing(stream.bubble())).toEqual(['call-a', 'call-b', 'call-c']);

    stream.land('call-a');
    expect(preparing(stream.bubble())).toEqual(['call-b', 'call-c']);
    stream.land('call-b');
    // call-c never lands: the producer dropped it.
    expect(preparing(stream.bubble())).toEqual(['call-c']);

    expect(stream.finish('tool_calls')).toBe(false);
    expect(preparing(stream.bubble())).toEqual([]);
  });

  it('leaves the transcript as it was at a finish with nothing preparing', () => {
    const stream = mainStream();
    stream.chunks(chunk(0, 'call-a'));
    stream.land('call-a');
    const before = stream.list();
    stream.finish('tool_calls');
    expect(stream.list()).toBe(before);
  });

  it('drops what is still preparing when the model call stops, and when the turn fails', () => {
    const stopped = mainStream();
    stopped.chunks(chunk(0, 'call-a'));
    stopped.finish('stop');
    expect(preparing(stopped.bubble())).toEqual([]);

    const failed = mainStream();
    failed.chunks(chunk(0, 'call-a'));
    failed.end((msg) => failAssistantMessage(msg));
    expect(preparing(failed.bubble())).toEqual([]);
  });
});

describe('preparing tool calls on a subagent stream', () => {
  it('drops a call the producer dropped at the finish of its model call', () => {
    const refs = {
      contentOrderCounterRef: { current: 0 },
      currentReasoningIdRef: { current: null },
      currentToolCallIdRef: { current: null },
      subagentStateRefs: {},
    } as StreamRefs;
    const updateSubagentCard = vi.fn();
    const at = { taskId: 'task:k7Xm2p', assistantMessageId: 's1', refs, updateSubagentCard };
    const bubble = () => refs.subagentStateRefs!['task:k7Xm2p'].messages.find((m) => m.id === 's1');

    handleSubagentToolCallChunks({ ...at, chunks: [chunk(0, 'call-a'), chunk(1, 'call-b')] });
    handleSubagentToolCalls({ ...at, toolCalls: [call('call-a')] });
    expect(preparing(bubble())).toEqual(['call-b']);

    expect(handleSubagentMessageChunk({ ...at, contentType: 'text', content: '', finishReason: 'tool_calls' })).toBe(false);
    expect(preparing(bubble())).toEqual([]);
  });
});
