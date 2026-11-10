/**
 * The retired annotation tools (`draw_chart_annotation`, `manage_chart_annotations`)
 * still sit in older threads, their results carrying the old `chart_annotation`
 * artifact. Nothing draws that artifact any more: each call is an ordinary
 * completed row in the timeline, with no card and nothing a collapsed turn keeps.
 */
import { describe, expect, it } from 'vitest';

import { buildRenderBlocks } from '../buildRenderBlocks';
import { projectMessageContent } from '../contentProjection';
import type { MessageRecord, ToolCallProcessRecord } from '../types';

/** A `draw_chart_annotation` result as the server stored it. */
const drawArtifact = (annotationId: string) => ({
  type: 'chart_annotation',
  op: 'add',
  chart_id: 'NVDA:1day',
  symbol: 'NVDA',
  timeframe: '1day',
  workspace_id: 'ws1',
  annotation_id: annotationId,
  annotations: [{ annotation_id: annotationId, type: 'price_line', symbol: 'NVDA', price: 180 }],
});

const proc = (name: string, artifact: Record<string, unknown>): ToolCallProcessRecord => ({
  toolName: name,
  toolCall: { id: 'x', name, args: { symbol: 'NVDA', timeframe: '1day' } },
  isInProgress: false,
  isComplete: true,
  isFailed: false,
  toolCallResult: { content: 'Drew 1 annotation.', artifact },
} as unknown as ToolCallProcessRecord);

const segs = (ids: string[]) => ids.map((id, order) => ({ type: 'tool_call' as const, toolCallId: id, order }));

const processes = {
  d1: proc('draw_chart_annotation', drawArtifact('ann_1')),
  d2: proc('draw_chart_annotation', drawArtifact('ann_2')),
  m1: proc('manage_chart_annotations', { type: 'chart_annotation', op: 'clear', chart_id: 'NVDA:1day', workspace_id: 'ws1' }),
};

describe('an old thread with chart annotation calls', () => {
  it('renders each call as a completed row in one activity block, with no card', () => {
    const { blocks } = buildRenderBlocks(segs(['d1', 'd2', 'm1']), {
      reasoningProcesses: {},
      toolCallProcesses: processes,
      isStreaming: false,
    });
    expect(blocks.map((b) => b.type)).toEqual(['activity']);
    const block = blocks[0];
    const items = block.type === 'activity' ? block.items : [];
    expect(items.map((i) => [i.id, i.type === 'tool_call' && i.toolName, i.type === 'tool_call' && i._liveState]))
      .toEqual([
        ['d1', 'draw_chart_annotation', 'completed'],
        ['d2', 'draw_chart_annotation', 'completed'],
        ['m1', 'manage_chart_annotations', 'completed'],
      ]);
  });

  it('folds the calls away in a collapsed turn, keeping only the answer', () => {
    const message = {
      id: 'a0', role: 'assistant', content: 'Marked.', contentType: 'text',
      contentSegments: [...segs(['d1', 'd2']), { type: 'text', content: 'Marked.', order: 5 }],
      reasoningProcesses: {},
      toolCallProcesses: { d1: processes.d1, d2: processes.d2 },
    } as unknown as MessageRecord;
    const projection = projectMessageContent(message);
    expect(projection.blocks.map((b) => [b.type, projection.roles.get(b.key)])).toEqual([
      ['activity', 'process'],
      ['text', 'text'],
    ]);
    expect(projection.hasRetained).toBe(false);
  });
});
