/**
 * A ShowWidget call draws its widget while the model writes it: the stream
 * keeps that one call's arguments, the transcript draws a preview block where
 * the widget will sit, and the artifact takes the block over under the same
 * key. A call that ends with no widget, or a turn that ends first, takes the
 * preview away. Only the main agent draws widgets.
 */
import { describe, expect, it } from 'vitest';
import type { MessageRecord } from '../../../hooks/utils/types';
import type { HtmlWidgetData } from '../../../hooks/utils/types';
import { projectContent } from '../../../components/messageList/contentProjection';
import type { HtmlWidgetRenderBlock } from '../../../components/messageList/buildRenderBlocks';
import type { ContentSegmentRecord, ToolCallProcessRecord } from '../../../components/messageList/types';
import type { PendingToolCallChunk, WidgetDraft } from '@/types/chat';
import { handleHtmlWidget, handleTextContent, handleToolCallChunks, handleToolCallResult, handleToolCalls } from '../mainEventHandlers';
import { finalizeAssistantMessage } from '../finalizeMessage';
import { handleSubagentToolCallChunks } from '../../subagents/liveEventHandlers';
import type { StreamRefs } from '../../streamRefs';

const HTML = '<style>td{padding:4px}</style><table><tr><td>Q3</td></tr></table><script>draw()</script>';
const ARGS = JSON.stringify({ title: 'Revenue', html: HTML });

function stream() {
  let messages: MessageRecord[] = [{
    id: 'a1', role: 'assistant', contentSegments: [{ type: 'text', content: 'Here it is.', order: 1 }],
    toolCallProcesses: {}, isStreaming: true,
  }];
  const setMessages = (update: (prev: MessageRecord[]) => MessageRecord[]) => {
    messages = update(messages);
  };
  const refs = {
    contentOrderCounterRef: { current: 10 },
    currentReasoningIdRef: { current: null },
    currentToolCallIdRef: { current: null },
  } as StreamRefs;
  return {
    chunk: (args: string, extra: Record<string, unknown> = {}) =>
      handleToolCallChunks({ assistantMessageId: 'a1', chunks: [{ index: 0, args, ...extra }], setMessages }),
    land: (id: string, name = 'ShowWidget') => handleToolCalls({
      assistantMessageId: 'a1', toolCalls: [{ id, name, args: JSON.parse(ARGS) }], finishReason: 'tool_calls', refs, setMessages,
    }),
    artifact: (id: string) => handleHtmlWidget({
      assistantMessageId: 'a1', artifactType: 'html_widget', artifactId: id,
      payload: { type: 'html_widget', html: HTML, title: 'Revenue' } as unknown as HtmlWidgetData, refs, setMessages,
    }),
    result: (id: string, content: string) => handleToolCallResult({
      assistantMessageId: 'a1', toolCallId: id, result: { content, tool_call_id: id }, refs, setMessages,
    }),
    finish: (reason: string) => handleTextContent({ assistantMessageId: 'a1', content: '', finishReason: reason, refs, setMessages }),
    settle: () => setMessages((prev) => prev.map((m) => finalizeAssistantMessage(m, 'completed'))),
    bubble: () => messages[0],
  };
}

/** The widget blocks the bubble draws, as the transcript builds them. */
function widgets(msg: MessageRecord) {
  return projectContent({
    segments: msg.contentSegments as ContentSegmentRecord[],
    reasoningProcesses: {},
    toolCallProcesses: (msg.toolCallProcesses ?? {}) as Record<string, ToolCallProcessRecord>,
    pendingToolCallChunks: msg.pendingToolCallChunks as Record<string, PendingToolCallChunk> | undefined,
    widgetDrafts: msg.widgetDrafts as Record<string, WidgetDraft> | undefined,
    isStreaming: msg.isStreaming === true,
  }).blocks.filter((b): b is HtmlWidgetRenderBlock => b.type === 'html_widget');
}

describe('a ShowWidget call drawn as it streams', () => {
  it('draws the markup so far, under the key the widget takes over', () => {
    const s = stream();
    s.chunk(ARGS.slice(0, 12), { id: 'call-w', name: 'ShowWidget' });
    // Only the title so far: a block with nothing to draw, which shows the preparing line.
    expect(widgets(s.bubble())).toEqual([expect.objectContaining({
      key: 'widget-call-w', preview: expect.objectContaining({ html: null, argsLength: 12 }),
    })]);

    // The generic path names the call on its first chunk only.
    s.chunk(ARGS.slice(12, 60));
    s.chunk(ARGS.slice(60));
    const [preview] = widgets(s.bubble());
    expect(preview.preview!.html).toBe(HTML);

    // Landed: drawn from the complete arguments where the call sits, while the tool runs.
    s.land('call-w');
    s.finish('tool_calls');
    const [landed] = widgets(s.bubble());
    expect(landed.key).toBe('widget-call-w');
    expect(landed.preview).toEqual({ html: HTML, title: 'Revenue', argsLength: HTML.length });

    // The artifact takes the block over: same key, no preview.
    s.artifact('call-w');
    const [final] = widgets(s.bubble());
    expect(final.key).toBe('widget-call-w');
    expect(final.preview).toBeUndefined();
    expect(s.bubble().widgetDrafts).toEqual({});

    // Its result changes nothing.
    s.result('call-w', 'Rendered 720x300.');
    expect(widgets(s.bubble())).toEqual([final]);
  });

  it('keeps no arguments for any other tool', () => {
    const s = stream();
    s.chunk('{"command": "ls -la"}', { id: 'call-b', name: 'Bash' });
    expect(s.bubble().widgetDrafts ?? {}).toEqual({});
    expect(widgets(s.bubble())).toEqual([]);
  });

  it('takes the preview away when the call shows no widget', () => {
    const s = stream();
    s.chunk(ARGS, { id: 'call-w', name: 'ShowWidget' });
    s.land('call-w');
    expect(widgets(s.bubble())).toHaveLength(1);
    s.result('call-w', 'Widget not shown. Some data_files cannot be delivered.');
    expect(widgets(s.bubble())).toEqual([]);
  });

  it('takes the preview away when the turn ends before the widget lands', () => {
    const s = stream();
    s.chunk(ARGS, { id: 'call-w', name: 'ShowWidget' });
    s.land('call-w');
    s.settle();
    expect(widgets(s.bubble())).toEqual([]);

    const t = stream();
    t.chunk(ARGS.slice(0, 40), { id: 'call-x', name: 'ShowWidget' });
    t.finish('stopped');
    expect(widgets(t.bubble())).toEqual([]);
  });

  it('draws the widget where its call sits, ahead of a parallel call that landed before the artifact', () => {
    const s = stream();
    s.land('call-w');
    s.land('call-b', 'Bash');
    s.artifact('call-w');
    const kinds = projectContent({
      segments: s.bubble().contentSegments as ContentSegmentRecord[],
      reasoningProcesses: {},
      toolCallProcesses: s.bubble().toolCallProcesses as Record<string, ToolCallProcessRecord>,
      isStreaming: true,
    }).blocks.map((b) => b.type);
    expect(kinds).toEqual(['text', 'html_widget', 'activity']);
  });

  it('keeps the preview of a call streamed before its id, under the id once it comes', () => {
    const s = stream();
    s.chunk(ARGS.slice(0, 30), { name: 'ShowWidget' });
    const [early] = widgets(s.bubble());
    expect(early.preview!.title).toBe('Revenue');
    s.chunk(ARGS.slice(30, 60), { id: 'call-w' });
    s.chunk(ARGS.slice(60));
    const [named] = widgets(s.bubble());
    expect(named.key).toBe('widget-call-w');
    expect(named.preview!.html).toBe(HTML);
    expect(Object.keys(s.bubble().widgetDrafts as object)).toEqual(['call-w']);
  });

  it('hands a call that never gets an id its landing, matched by order', () => {
    const s = stream();
    s.chunk(ARGS, { name: 'ShowWidget' });
    s.land('call-w');
    expect(Object.keys(s.bubble().widgetDrafts as object)).toEqual(['call-w']);
    expect(widgets(s.bubble())).toEqual([expect.objectContaining({ key: 'widget-call-w' })]);
  });

  it('takes away a preview whose call is dropped at the end of its model call', () => {
    const s = stream();
    s.chunk(ARGS.slice(0, 40), { id: 'call-w', name: 'ShowWidget' });
    s.finish('tool_calls');
    expect(widgets(s.bubble())).toEqual([]);
    expect(s.bubble().widgetDrafts).toEqual({});
  });

  it('draws nothing for a subagent writing one', () => {
    let messages: MessageRecord[] = [];
    const refs = { subagentStateRefs: {} } as unknown as StreamRefs;
    handleSubagentToolCallChunks({
      taskId: 't1', assistantMessageId: 's1', chunks: [{ index: 0, id: 'call-s', name: 'ShowWidget', args: ARGS }],
      refs, updateSubagentCard: (_id: string, patch: Record<string, unknown>) => { messages = (patch.messages as MessageRecord[]) ?? messages; },
    } as unknown as Parameters<typeof handleSubagentToolCallChunks>[0]);
    expect(messages).toHaveLength(1);
    for (const msg of messages) expect(msg.widgetDrafts).toBeUndefined();
  });
});
