import type { ChartLayer, ChartLayerArtifactPayload, StoredChartLayer } from '../../types/chartLayer';

/** A minimal valid layer; `over` replaces any field. */
export function makeLayer(over: Partial<ChartLayer> = {}): ChartLayer {
  return {
    version: 1,
    id: 'nvda-channel',
    title: 'NVDA rising channel',
    summary: 'Price rides the lower rail of a rising channel; a close under it voids the setup.',
    symbol: 'NVDA',
    timeframe: '1day',
    as_of: '2026-10-28',
    source: { kind: 'python' },
    panes: [],
    series: [
      {
        id: 'ema', title: 'EMA 20', pane: 'price', type: 'line', color: '#e9954a',
        data: [{ time: '2026-10-27', value: 201 }, { time: '2026-10-28', value: 203 }],
      },
    ],
    fills: [],
    levels: [{ price: 195, pane: 'price', title: 'Support' }],
    markers: [],
    regimes: [],
    drawings: [],
    alerts: [],
    ...over,
  };
}

export function makeStored(over: Partial<ChartLayer> = {}, ws = 'ws-1'): StoredChartLayer {
  const layer = makeLayer(over);
  return {
    workspace_id: ws,
    layer_id: layer.id,
    symbol: layer.symbol,
    timeframe: layer.timeframe,
    chart_id: `${layer.symbol}:${layer.timeframe}`,
    layer,
  };
}

export function addPayload(ws: string, over: Partial<ChartLayer> = {}): Extract<ChartLayerArtifactPayload, { op: 'add' }> {
  return { op: 'add', ...makeStored(over, ws) };
}
