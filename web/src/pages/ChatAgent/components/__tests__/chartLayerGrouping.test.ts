// @vitest-environment node
import { describe, expect, it } from 'vitest';

import type { PublishedLayer } from '@/pages/MarketView/types/chartLayer';
import { chartKeyFor } from '@/pages/MarketView/utils/chartInstanceKey';
import { planChartLayerCards, planReportChartCards } from '../chartLayerGrouping';
import { projectTurns } from '../messageList/turnProjection';
import type { MessageRecord, ToolCallProcessRecord } from '../messageList/types';

const pub = (over: Partial<PublishedLayer> = {}): PublishedLayer => ({
  chart_id: 'NVDA:1day',
  symbol: 'NVDA',
  timeframe: '1day',
  workspace_id: 'ws1',
  layer_id: 'a',
  title: 'A',
  summary: 'why',
  as_of: '2026-10-28',
  ...over,
});

const call = (...layers: PublishedLayer[]): ToolCallProcessRecord => ({
  toolCallResult: { artifact: { type: 'execute_code', chart_layers: layers } },
});
const seg = (id: string, order = 0) => ({ type: 'tool_call', toolCallId: id, order });
const NVDA = chartKeyFor('ws1', 'NVDA', '1day')!;

describe('planChartLayerCards', () => {
  it('makes no card for a call that published nothing', () => {
    const plan = planChartLayerCards([seg('t1'), seg('t2')], {
      t1: { toolCallResult: { artifact: { type: 'execute_code' } } },
      t2: { toolCallResult: null },
    });
    expect(plan.size).toBe(0);
  });

  it('pins one card per chart at the first publishing call, listing its layers', () => {
    const plan = planChartLayerCards([seg('t1')], { t1: call(pub(), pub({ layer_id: 'b', title: 'B' })) });
    const [card] = plan.get('t1')!;
    expect(plan.size).toBe(1);
    expect(card).toMatchObject({ id: NVDA, key: NVDA, workspaceId: 'ws1', symbol: 'NVDA', timeframe: '1day' });
    expect(card.layers.map((l) => l.title)).toEqual(['A', 'B']);
    expect(card.layers[0]).toEqual(pub());
  });

  it('folds later publishes on the same chart into the first call’s card', () => {
    const plan = planChartLayerCards([seg('t1'), seg('t2')], {
      t1: call(pub()),
      t2: call(pub({ layer_id: 'b', title: 'B' })),
    });
    expect([...plan.keys()]).toEqual(['t1']);
    expect(plan.get('t1')![0].layers.map((l) => l.title)).toEqual(['A', 'B']);
  });

  it('lets a republished layer replace its entry and keep its place', () => {
    const plan = planChartLayerCards([seg('t1'), seg('t2')], {
      t1: call(pub(), pub({ layer_id: 'b', title: 'B' })),
      t2: call(pub({ title: 'A, revised', summary: null })),
    });
    const { layers } = plan.get('t1')![0];
    expect(layers.map((l) => l.title)).toEqual(['A, revised', 'B']);
    expect(layers[0].summary).toBeNull();
  });

  it('keeps charts apart by instance, timeframe and workspace', () => {
    const plan = planChartLayerCards([seg('t1'), seg('t2')], {
      t1: call(pub(), pub({ chart_id: 'NVDA:1hour', timeframe: '1hour', layer_id: 'h' })),
      t2: call(pub({ workspace_id: 'ws2', layer_id: 'w' }), pub({ chart_id: 'AAPL:1day', symbol: 'AAPL', layer_id: 'x' })),
    });
    expect(plan.get('t1')!.map((c) => c.key)).toEqual([NVDA, chartKeyFor('ws1', 'NVDA', '1hour')]);
    expect(plan.get('t2')!.map((c) => c.key)).toEqual([chartKeyFor('ws2', 'NVDA', '1day'), chartKeyFor('ws1', 'AAPL', '1day')]);
  });

  it('reads one chart in either spelling of its ticker as one card', () => {
    const plan = planChartLayerCards([seg('t1'), seg('t2')], {
      t1: call(pub({ chart_id: '600519.SS:1day', symbol: '600519.SS', layer_id: 'a' })),
      t2: call(pub({ chart_id: '600519.SH:1day', symbol: '600519.SH', layer_id: 'b' })),
    });
    expect([...plan.keys()]).toEqual(['t1']);
    const [card] = plan.get('t1')!;
    expect(card.symbol).toBe('600519.SH');
    expect(card.layers.map((l) => l.layer_id)).toEqual(['a', 'b']);
  });

  it('still cards an entry that names no workspace, with no chart to show it on', () => {
    const plan = planChartLayerCards([seg('t1')], { t1: call(pub({ workspace_id: '' })) });
    expect(plan.get('t1')![0]).toMatchObject({ key: null, workspaceId: null, symbol: 'NVDA' });
  });

  it('leaves out a chart carded elsewhere in the turn', () => {
    const plan = planChartLayerCards([seg('t1')], {
      t1: call(pub(), pub({ chart_id: 'AAPL:1day', symbol: 'AAPL', layer_id: 'x' })),
    }, new Set([NVDA]));
    expect(plan.get('t1')!.map((c) => c.symbol)).toEqual(['AAPL']);
  });

  it('ignores segments that are not tool calls', () => {
    const plan = planChartLayerCards([{ type: 'text', order: 0 }, seg('missing')], {});
    expect(plan.size).toBe(0);
  });
});

describe('planReportChartCards', () => {
  const report = (id: string, turnIndex: number, ...chartLayers: PublishedLayer[]): MessageRecord => ({
    id, role: 'user', content: '', turnIndex,
    provenance: { kind: 'command_report', tool: 'Bash', description: null, status: 'completed', failureReason: null, toolCallId: null, chartLayers },
  });
  const reply = (id: string, turnIndex: number, calls: Record<string, ToolCallProcessRecord>): MessageRecord => ({
    id, role: 'assistant', content: '', turnIndex,
    contentSegments: Object.keys(calls).map((callId, order) => seg(callId, order)),
    toolCallProcesses: calls,
  });

  it('cards the report row and folds the turn’s republish into it, which its message leaves out', () => {
    // Thread 71f122b9: the report-back turn republished the layer its command had.
    const first = pub({ layer_id: 'nvda-resistance-210', title: 'NVDA resistance 210' });
    const again = pub({ layer_id: 'nvda-resistance-210', title: 'NVDA resistance 210, revised' });
    const other = pub({ chart_id: 'AAPL:1day', symbol: 'AAPL', layer_id: 'x' });
    const answer = reply('a1', 3, { t1: call(again, other) });
    const messages = [report('r1', 3, first), answer];
    const { byReport, omitByMessage } = planReportChartCards(projectTurns(messages));
    const [card] = byReport.get('r1')!;
    expect(byReport.get('r1')).toHaveLength(1);
    expect(card.layers).toEqual([again]);
    expect(omitByMessage.get(answer)).toEqual(new Set([NVDA]));
    // Its own projection then draws only the chart the report did not card.
    const own = planChartLayerCards(answer.contentSegments as never, answer.toolCallProcesses as never, omitByMessage.get(answer));
    expect([...own.values()].flat().map((c) => c.symbol)).toEqual(['AAPL']);
  });

  it('puts a chart two reports opening one turn both published on under the first', () => {
    const messages = [
      report('r1', 2, pub()),
      report('r2', 2, pub({ layer_id: 'b' }), pub({ chart_id: 'AAPL:1day', symbol: 'AAPL', layer_id: 'x' })),
    ];
    const { byReport } = planReportChartCards(projectTurns(messages));
    expect(byReport.get('r1')![0].layers.map((l) => l.layer_id)).toEqual(['a', 'b']);
    expect(byReport.get('r2')!.map((c) => c.symbol)).toEqual(['AAPL']);
  });

  it('leaves a message its card on a chart only a later report in the turn cards', () => {
    // r1 cards NVDA; the reply publishes on AAPL; r2 then cards AAPL. The reply's AAPL
    // layers were never folded into r2's card, so the reply still draws them.
    const aapl = (id: string) => pub({ chart_id: 'AAPL:1day', symbol: 'AAPL', layer_id: id });
    const answer = reply('a1', 3, { t1: call(aapl('mine')) });
    const messages = [report('r1', 3, pub()), answer, report('r2', 3, aapl('theirs'))];
    const { byReport, omitByMessage } = planReportChartCards(projectTurns(messages));
    expect(byReport.get('r2')!.map((c) => c.symbol)).toEqual(['AAPL']);
    expect(omitByMessage.get(answer)).toEqual(new Set([NVDA]));
  });

  it('leaves another turn’s message its own card', () => {
    const later = reply('a2', 4, { t1: call(pub({ layer_id: 'b' })) });
    const messages = [report('r1', 3, pub()), reply('a1', 3, {}), later];
    const { byReport, omitByMessage } = planReportChartCards(projectTurns(messages));
    expect(byReport.get('r1')![0].layers.map((l) => l.layer_id)).toEqual(['a']);
    expect(omitByMessage.has(later)).toBe(false);
  });

  it('plans nothing for a transcript with no report that published', () => {
    const plan = planReportChartCards(projectTurns([report('r1', 1), reply('a1', 1, { t1: call(pub()) })]));
    expect(plan.byReport.size).toBe(0);
    expect(plan.omitByMessage.size).toBe(0);
  });
});
