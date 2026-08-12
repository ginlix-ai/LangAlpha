import { describe, expect, it } from 'vitest';

import { chartInstanceKey, planChartAnnotationCards } from '../chartAnnotationGrouping';

const drawProc = (artifact: Record<string, unknown> | undefined) => ({
  toolName: 'draw_chart_annotation',
  toolCallResult: artifact ? { artifact } : undefined,
});

const chartAnnotation = (over: Record<string, unknown> = {}) => ({
  type: 'chart_annotation',
  workspace_id: 'ws1',
  chart_id: 'NVDA:1day',
  ...over,
});

describe('chartInstanceKey', () => {
  it('uses workspace_id + chart_id when present', () => {
    expect(chartInstanceKey(chartAnnotation())).toBe('ws1|NVDA:1day');
  });

  it('derives SYMBOL:timeframe when chart_id is absent, defaulting timeframe to 1day', () => {
    expect(chartInstanceKey({ workspace_id: 'ws1', symbol: 'nvda' })).toBe('ws1|NVDA:1day');
    expect(chartInstanceKey({ workspace_id: 'ws1', symbol: 'nvda', timeframe: '1hour' })).toBe(
      'ws1|NVDA:1hour',
    );
  });

  it('uses an empty workspace segment when workspace_id is missing', () => {
    expect(chartInstanceKey({ chart_id: 'NVDA:1day' })).toBe('|NVDA:1day');
  });

  it('falls through empty-string chart_id/timeframe instead of collapsing charts onto one key', () => {
    // `||` (not `??`): an empty chart_id must derive SYMBOL:timeframe, otherwise
    // two different charts would both key to `ws1|` and share a single card.
    expect(chartInstanceKey({ workspace_id: 'ws1', chart_id: '', symbol: 'nvda' })).toBe(
      'ws1|NVDA:1day',
    );
    expect(
      chartInstanceKey({ workspace_id: 'ws1', symbol: 'nvda', timeframe: '' }),
    ).toBe('ws1|NVDA:1day');
  });
});

describe('planChartAnnotationCards', () => {
  it('anchors the card at the first draw and tracks the latest per chart instance', () => {
    const segments = [
      { type: 'tool_call', toolCallId: 'a' },
      { type: 'tool_call', toolCallId: 'b' },
      { type: 'tool_call', toolCallId: 'c' },
    ];
    const procs = {
      a: drawProc(chartAnnotation()),
      b: drawProc(chartAnnotation()),
      c: drawProc(chartAnnotation()),
    };
    const plan = planChartAnnotationCards(segments, procs);
    expect(plan.get('ws1|NVDA:1day')).toEqual({ anchorCallId: 'a', latestCallId: 'c' });
  });

  it('plans one card per distinct chart (symbol/timeframe)', () => {
    const segments = [
      { type: 'tool_call', toolCallId: 'a' },
      { type: 'tool_call', toolCallId: 'b' },
      { type: 'tool_call', toolCallId: 'c' },
    ];
    const procs = {
      a: drawProc(chartAnnotation({ chart_id: 'NVDA:1day' })),
      b: drawProc(chartAnnotation({ chart_id: 'NVDA:1hour' })),
      c: drawProc(chartAnnotation({ chart_id: 'NVDA:1day' })),
    };
    const plan = planChartAnnotationCards(segments, procs);
    expect(plan.get('ws1|NVDA:1day')).toEqual({ anchorCallId: 'a', latestCallId: 'c' });
    expect(plan.get('ws1|NVDA:1hour')).toEqual({ anchorCallId: 'b', latestCallId: 'b' });
  });

  it('ignores in-progress draws with no artifact yet (latest stays the newest completed)', () => {
    const segments = [
      { type: 'tool_call', toolCallId: 'a' },
      { type: 'tool_call', toolCallId: 'b' }, // still streaming, no artifact
    ];
    const procs = {
      a: drawProc(chartAnnotation()),
      b: drawProc(undefined),
    };
    const plan = planChartAnnotationCards(segments, procs);
    expect(plan.get('ws1|NVDA:1day')).toEqual({ anchorCallId: 'a', latestCallId: 'a' });
  });

  it('ignores non-chart_annotation artifacts and non-tool_call segments', () => {
    const segments = [
      { type: 'reasoning', toolCallId: undefined },
      { type: 'tool_call', toolCallId: 'a' },
      { type: 'tool_call', toolCallId: 'b' },
    ];
    const procs = {
      a: drawProc({ type: 'stock_prices' }),
      b: drawProc(chartAnnotation()),
    };
    const plan = planChartAnnotationCards(segments, procs);
    expect(plan.size).toBe(1);
    expect(plan.get('ws1|NVDA:1day')).toEqual({ anchorCallId: 'b', latestCallId: 'b' });
  });
});
