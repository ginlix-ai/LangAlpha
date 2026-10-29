// @vitest-environment node
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

/** A draw of `id` whose read returned `ids`, optionally stamped with when. */
const snap = (id: string, ids: string[], readAtUs?: number) =>
  drawProc(
    chartAnnotation({
      annotation_id: id,
      annotations: ids.map((annotation_id) => ({ annotation_id })),
      ...(readAtUs === undefined ? {} : { read_at_us: readAtUs }),
    }),
  );

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
  it('anchors the card at the first draw and tracks the freshest per chart instance', () => {
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
    expect(plan.get('ws1|NVDA:1day')).toEqual({ anchorCallId: 'a', freshestCallId: 'c' });
  });

  it('keeps the freshest snapshot when parallel draws finish out of call order', () => {
    // Called a, b, c, d; committed d, a, c, b. The last call holds the oldest set.
    const segments = ['a', 'b', 'c', 'd'].map((toolCallId) => ({ type: 'tool_call', toolCallId }));
    const procs = {
      a: snap('a', ['d', 'a']),
      b: snap('b', ['d', 'a', 'c', 'b']),
      c: snap('c', ['d', 'a', 'c']),
      d: snap('d', ['d']),
    };
    const plan = planChartAnnotationCards(segments, procs);
    expect(plan.get('ws1|NVDA:1day')).toEqual({ anchorCallId: 'a', freshestCallId: 'b' });
  });

  it('follows a later round that shrank the set after a removal', () => {
    const segments = ['a', 'b', 'c'].map((toolCallId) => ({ type: 'tool_call', toolCallId }));
    const procs = {
      a: snap('a', ['a']),
      b: snap('b', ['a', 'b']),
      c: snap('c', ['c']),
    };
    const plan = planChartAnnotationCards(segments, procs);
    expect(plan.get('ws1|NVDA:1day')).toEqual({ anchorCallId: 'a', freshestCallId: 'c' });
  });

  it('orders by read stamp when a removal lands between parallel reads', () => {
    // a reads {a, b}; a is removed; b reads {b}. Membership alone would keep
    // a's set, since it holds b, though b read later.
    const segments = ['a', 'b'].map((toolCallId) => ({ type: 'tool_call', toolCallId }));
    const procs = { a: snap('a', ['a', 'b'], 2), b: snap('b', ['b'], 3) };
    const plan = planChartAnnotationCards(segments, procs);
    expect(plan.get('ws1|NVDA:1day')).toEqual({ anchorCallId: 'a', freshestCallId: 'b' });
  });

  it('never lets a draw whose read-back failed displace a real read', () => {
    // b's read failed: its artifact holds only itself and no stamp.
    const segments = ['a', 'b', 'c'].map((toolCallId) => ({ type: 'tool_call', toolCallId }));
    const procs = { a: snap('a', ['a'], 2), b: snap('b', ['b']), c: snap('c', ['a', 'b', 'c'], 4) };
    const twoCalls = planChartAnnotationCards(segments.slice(0, 2), procs);
    expect(twoCalls.get('ws1|NVDA:1day')).toEqual({ anchorCallId: 'a', freshestCallId: 'a' });
    const plan = planChartAnnotationCards(segments, procs);
    expect(plan.get('ws1|NVDA:1day')).toEqual({ anchorCallId: 'a', freshestCallId: 'c' });
  });

  it('keeps an earlier call whose read stamp is later', () => {
    const segments = ['a', 'b'].map((toolCallId) => ({ type: 'tool_call', toolCallId }));
    const procs = { a: snap('a', ['b', 'a'], 5), b: snap('b', ['b'], 3) };
    const plan = planChartAnnotationCards(segments, procs);
    expect(plan.get('ws1|NVDA:1day')).toEqual({ anchorCallId: 'a', freshestCallId: 'a' });
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
    expect(plan.get('ws1|NVDA:1day')).toEqual({ anchorCallId: 'a', freshestCallId: 'c' });
    expect(plan.get('ws1|NVDA:1hour')).toEqual({ anchorCallId: 'b', freshestCallId: 'b' });
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
    expect(plan.get('ws1|NVDA:1day')).toEqual({ anchorCallId: 'a', freshestCallId: 'a' });
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
    expect(plan.get('ws1|NVDA:1day')).toEqual({ anchorCallId: 'b', freshestCallId: 'b' });
  });
});
