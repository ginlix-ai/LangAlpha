// @vitest-environment node
import { describe, expect, it } from 'vitest';

import { layerAskContext } from '../layerAsk';

const REQ = { symbol: 'NVDA', timeframe: '1day', layerId: 'a', layerTitle: 'Rising channel', summary: 'Rides the lower rail.' };

describe('layerAsk', () => {
  it('writes a chart context that names the layer id for the agent to look up', () => {
    const ctx = layerAskContext(REQ);
    expect(ctx).toMatchObject({ source: 'chart', label: 'NVDA · 1day · Rising channel' });
    expect(ctx.snippet).toContain('(id a)');
    expect(ctx.snippet).toContain('Rides the lower rail.');
  });

  it('adds the element, its note and where it sits when one was picked', () => {
    const ctx = layerAskContext({
      ...REQ,
      element: {
        layerId: 'a', layerTitle: 'Rising channel', kind: 'level', label: 'Support', note: 'Held 3 times',
        anchors: [{ time: '2026-10-20', price: 195 }],
      },
    });
    expect(ctx.label).toBe('NVDA · 1day · Rising channel · level "Support"');
    expect(ctx.snippet).toContain('Your note on that element: Held 3 times');
    expect(ctx.snippet).toContain('2026-10-20 @ 195');
  });
});
