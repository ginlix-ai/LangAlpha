/**
 * The projection cache has to answer for the message as it is now.
 *
 * Message identity alone does not carry that: the subagent tool-call and
 * tool-call-result handlers assign `contentSegments` and `toolCallProcesses`
 * onto the existing record and hand React a new array around the same object.
 * A cache keyed on the record answered the first tool call with the text-only
 * projection that preceded it, and that projection carries `nextExpiry: null`,
 * so nothing would ever retire it.
 */
import { describe, it, expect } from 'vitest';
import { isOrphanAssistantMessage, projectMessageContent } from '../contentProjection';
import type { MessageRecord } from '../types';
import type { ActivityRenderBlock, TextRenderBlock } from '../buildRenderBlocks';

const textOnly = (): MessageRecord => ({
  id: 'a0',
  role: 'assistant',
  content: 'Looking into it.',
  isStreaming: true,
  contentSegments: [{ type: 'text', content: 'Looking into it.', order: 0 }],
  reasoningProcesses: {},
  toolCallProcesses: {},
});

describe('projectMessageContent cache', () => {
  it('re-projects when a handler assigns new segments onto the same record', () => {
    const message = textOnly();
    const first = projectMessageContent(message, true);
    expect(first.blocks.map((b) => b.type)).toEqual(['text']);
    // Permanent by itself: nothing about a settled projection expires.
    expect(first.nextExpiry).toBeNull();

    // Verbatim shape of what handleSubagentToolCalls writes.
    message.contentSegments = [
      { type: 'text', content: 'Looking into it.', order: 0 },
      { type: 'tool_call', toolCallId: 'tc1', order: 1 },
    ];
    message.toolCallProcesses = {
      tc1: {
        toolName: 'Read', toolCall: { args: {} },
        isInProgress: true, isComplete: false, order: 1, _createdAt: Date.now(),
      },
    };

    const second = projectMessageContent(message, true);
    expect(second).not.toBe(first);
    expect(second.blocks.some((b) => b.type === 'activity')).toBe(true);
  });

  it('still returns the cached projection when nothing about the record moved', () => {
    const message = textOnly();
    expect(projectMessageContent(message, true)).toBe(projectMessageContent(message, true));
  });

  it('re-projects when the record stops streaming', () => {
    // A call that just finished keeps its live exposure while the turn streams,
    // and settles the moment the stream stops.
    const message: MessageRecord = {
      ...textOnly(),
      id: 'a-stop',
      contentSegments: [{ type: 'tool_call', toolCallId: 'tc1', order: 0 }],
      toolCallProcesses: {
        tc1: { toolName: 'bash', toolCall: { args: {} }, isInProgress: false, isComplete: true, order: 0, _createdAt: Date.now() },
      },
    };
    const liveState = (p: ReturnType<typeof projectMessageContent>) => (p.blocks[0] as ActivityRenderBlock).items[0]._liveState;
    expect(liveState(projectMessageContent(message, true))).toBe('completing');
    message.isStreaming = false;
    expect(liveState(projectMessageContent(message, true))).toBe('completed');
  });

  it('judges an orphan on the root the bubble projects with', () => {
    // The only call reads the profile README under a computer's own root, which
    // the bubble hides; a check that projected without the root saw a row,
    // painted an empty bubble, and flipped the cache entry on every render.
    const root = '/home/someone/langalpha';
    const message: MessageRecord = {
      id: 'a-root',
      role: 'assistant',
      isStreaming: false,
      contentSegments: [{ type: 'tool_call', toolCallId: 'tc1', order: 0 }],
      reasoningProcesses: {},
      toolCallProcesses: {
        tc1: {
          toolName: 'Read', toolCall: { args: { file_path: `${root}/.agents/user/profile/README.md` } },
          isInProgress: false, isComplete: true, order: 0, _createdAt: Date.now(),
        },
      },
    };
    expect(isOrphanAssistantMessage(message, false, root)).toBe(true);
    const checked = projectMessageContent(message, false, root);
    expect(isOrphanAssistantMessage(message, false, root)).toBe(true);
    expect(projectMessageContent(message, false, root)).toBe(checked);
  });
});

describe('projectMessageContent across streamed chunks', () => {
  // Every chunk arrives as a new record, so this is the path a stream takes.
  const tool = { toolName: 'bash', toolCall: { args: {} }, toolCallResult: { output: 'ok' }, isInProgress: false, isComplete: true, order: 0 };
  const chunk = (text: string, tc1: Record<string, unknown> = tool): MessageRecord => ({
    id: 'a-chunks',
    role: 'assistant',
    isStreaming: true,
    contentSegments: [{ type: 'tool_call', toolCallId: 'tc1', order: 0 }, { type: 'text', content: text, order: 1 }],
    reasoningProcesses: {},
    toolCallProcesses: { tc1 },
  });

  it('reuses the blocks a chunk did not change, so their memoized rows skip it', () => {
    const first = projectMessageContent(chunk('Hel'));
    const second = projectMessageContent(chunk('Hello'));
    expect(second.blocks[0]).toBe(first.blocks[0]);
    expect(second.blocks[1]).not.toBe(first.blocks[1]);
    expect((second.blocks[1] as TextRenderBlock).segment.content).toBe('Hello');
  });

  it('never reuses a tool call whose record changed', () => {
    const first = projectMessageContent(chunk('Hello'));
    const second = projectMessageContent(chunk('Hello', { ...tool, toolCallResult: { output: 'changed' } }));
    const item = (p: ReturnType<typeof projectMessageContent>) => (p.blocks[0] as ActivityRenderBlock).items[0];
    expect(item(second)).not.toBe(item(first));
    const changed = item(second);
    expect(changed.type === 'tool_call' && changed.toolCallResult).toEqual({ output: 'changed' });
  });
});
