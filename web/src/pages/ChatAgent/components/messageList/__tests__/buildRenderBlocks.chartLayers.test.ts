/**
 * A layer is published from inside an ExecuteCode or Bash call, so its row
 * stays in the timeline and the chart's card follows it. The card is the one
 * thing of that turn a collapsed fold keeps.
 */
import { describe, expect, it } from 'vitest';

import { buildRenderBlocks, type ChartLayersRenderBlock } from '../buildRenderBlocks';
import { projectContent, projectMessageContent } from '../contentProjection';
import type { MessageRecord, ToolCallProcessRecord } from '../types';

const published = (layerId: string, title: string, over: Record<string, unknown> = {}) => ({
  chart_id: 'NVDA:1day', symbol: 'NVDA', timeframe: '1day', workspace_id: 'ws1',
  layer_id: layerId, title, summary: 'why', as_of: '2026-10-28', ...over,
});

const proc = (name: string, layers?: Record<string, unknown>[]): ToolCallProcessRecord => ({
  toolName: name,
  toolCall: { id: 'x', name, args: {} },
  isInProgress: false,
  isComplete: true,
  isFailed: false,
  toolCallResult: { content: 'SUCCESS', artifact: layers ? { type: 'execute_code', chart_layers: layers } : { type: 'execute_code' } },
} as unknown as ToolCallProcessRecord);

const segs = (ids: string[]) => ids.map((id, order) => ({ type: 'tool_call' as const, toolCallId: id, order }));

describe('buildRenderBlocks, published chart layers', () => {
  it('keeps the ExecuteCode row and pins the chart card right after it', () => {
    const { blocks } = buildRenderBlocks(segs(['t1', 't2']), {
      reasoningProcesses: {},
      toolCallProcesses: { t1: proc('ExecuteCode', [published('a', 'A')]), t2: proc('Bash') },
      isStreaming: false,
    });
    expect(blocks.map((b) => b.type)).toEqual(['activity', 'chart_layers', 'activity']);
    const row = blocks[0];
    expect(row.type === 'activity' && row.items.map((i) => i.id)).toEqual(['t1']);
    const card = blocks[1];
    expect(card.type === 'chart_layers' && card.card).toMatchObject({ symbol: 'NVDA', timeframe: '1day', workspaceId: 'ws1' });
  });

  it('feeds one card for a chart the turn published on twice, and keeps both rows', () => {
    const { blocks } = buildRenderBlocks(segs(['t1', 't2']), {
      reasoningProcesses: {},
      toolCallProcesses: {
        t1: proc('ExecuteCode', [published('a', 'A')]),
        t2: proc('Bash', [published('b', 'B')]),
      },
      isStreaming: false,
    });
    const cards = blocks.flatMap((b) => (b.type === 'chart_layers' ? [b] : []));
    expect(cards).toHaveLength(1);
    expect(cards[0].card.layers.map((l) => l.title)).toEqual(['A', 'B']);
    const rows = blocks.flatMap((b) => (b.type === 'activity' ? b.items.map((i) => i.id) : []));
    expect(rows).toEqual(['t1', 't2']);
  });

  it('draws no card beside a call still running', () => {
    const running = { ...proc('ExecuteCode', [published('a', 'A')]), isInProgress: true, _createdAt: 1_000 };
    const { blocks } = buildRenderBlocks(segs(['t1']), {
      reasoningProcesses: {}, toolCallProcesses: { t1: running }, isStreaming: true, now: 2_000,
    });
    expect(blocks.map((b) => b.type)).toEqual(['activity']);
  });

  it('draws nothing extra for a call with no layers', () => {
    const { blocks } = buildRenderBlocks(segs(['t1']), {
      reasoningProcesses: {}, toolCallProcesses: { t1: proc('ExecuteCode') }, isStreaming: false,
    });
    expect(blocks.map((b) => b.type)).toEqual(['activity']);
  });
});

describe('a chart two calls published on in parallel', () => {
  const t1 = proc('ExecuteCode', [published('a', 'A')]);
  const project = (t2: ToolCallProcessRecord, prev?: ReturnType<typeof projectContent>) => projectContent({
    segments: segs(['t1', 't2']), reasoningProcesses: {}, toolCallProcesses: { t1, t2 }, isStreaming: false,
  }, prev);
  const cardOf = (projection: ReturnType<typeof projectContent>) =>
    projection.blocks.find((b): b is ChartLayersRenderBlock => b.type === 'chart_layers')!;

  it('lists the second call’s layer once its result lands after the card was drawn', () => {
    const before = project({ ...proc('Bash'), toolCallResult: null } as ToolCallProcessRecord);
    expect(cardOf(before).card.layers.map((l) => l.title)).toEqual(['A']);
    // The card sits beside the first call, whose record did not change.
    const after = project(proc('Bash', [published('b', 'B')]), before);
    expect(cardOf(after).key).toBe(cardOf(before).key);
    expect(cardOf(after)).not.toBe(cardOf(before));
    expect(cardOf(after).card.layers.map((l) => l.title)).toEqual(['A', 'B']);
  });

  it('keeps the drawn card while nothing it lists changed', () => {
    const t2 = proc('Bash', [published('b', 'B')]);
    const before = project(t2);
    expect(cardOf(project({ ...t2 }, before))).toBe(cardOf(before));
  });
});

describe('a collapsed turn', () => {
  it('keeps the layer card and folds the rows that made it', () => {
    const message = {
      id: 'a0', role: 'assistant', content: 'Done.', contentType: 'text',
      contentSegments: [...segs(['t1']), { type: 'text', content: 'Done.', order: 5 }],
      reasoningProcesses: {},
      toolCallProcesses: { t1: proc('ExecuteCode', [published('a', 'A')]) },
    } as unknown as MessageRecord;
    const projection = projectMessageContent(message);
    const roles = Object.fromEntries(projection.blocks.map((b) => [b.type, projection.roles.get(b.key)]));
    expect(roles).toMatchObject({ activity: 'process', chart_layers: 'retain', text: 'text' });
    expect(projection.hasRetained).toBe(true);
  });
});
