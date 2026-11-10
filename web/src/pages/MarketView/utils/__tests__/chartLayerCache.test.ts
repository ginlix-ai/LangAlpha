import { QueryClient, QueryObserver } from '@tanstack/react-query';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('../api', () => ({ fetchChartLayers: vi.fn(), deleteChartLayer: vi.fn() }));


import { fetchChartLayers } from '../api';
import { applyChartLayerEvent, applyLayerOp, invalidateReportedLayers, rowsOfChart } from '../chartLayerCache';
import { chartLayersQuery } from '../../hooks/useChartLayerQuery';
import { chartKeyFor } from '../chartInstanceKey';
import {
  getHiddenLayers,
  getShowLayersRequest,
  resetLayerViewForTesting,
  setLayerHidden,
} from '../../stores/layerViewStore';
import { addPayload, makeLayer, makeStored } from '../../stores/__tests__/layerFixture';
import type { StoredChartLayer } from '../../types/chartLayer';

const WS = 'ws-1';
const KEY = chartKeyFor(WS, 'NVDA', '1day')!;
const ref = { workspace_id: WS, chart_id: 'NVDA:1day', symbol: 'NVDA', timeframe: '1day' };
const ids = (rows: StoredChartLayer[] | undefined) => rows?.map((r) => r.layer_id);
const fetchLayers = vi.mocked(fetchChartLayers);

describe('applyLayerOp', () => {
  const a = makeStored({ id: 'a' });
  const b = makeStored({ id: 'b' });
  const hourly = makeStored({ id: 'a', timeframe: '1hour' });

  it('appends a new layer and replaces a republished one in place', () => {
    const rows = applyLayerOp(addPayload(WS, { id: 'c' }))([a, b]);
    expect(ids(rows)).toEqual(['a', 'b', 'c']);
    const again = applyLayerOp(addPayload(WS, { id: 'a', title: 'Again' }))(rows);
    expect(ids(again)).toEqual(['a', 'b', 'c']);
    expect(again![0].layer.title).toBe('Again');
  });

  it('removes by id and clears only the chart it names', () => {
    expect(ids(applyLayerOp({ op: 'remove', ...ref, ids: ['a'] })([a, b, hourly]))).toEqual(['b', 'a']);
    expect(applyLayerOp({ op: 'clear', ...ref })([a, b, hourly])).toEqual([hourly]);
  });

  it('leaves nothing cached as it is, for the next read to fetch', () => {
    expect(applyLayerOp(addPayload(WS))(undefined)).toBeUndefined();
  });

  it('leaves the cache alone for an add whose body the server dropped', () => {
    const { layer: _layer, ...bodyless } = addPayload(WS, { id: 'c' });
    expect(ids(applyLayerOp(bodyless)([a, b]))).toEqual(['a', 'b']);
  });

  it('reads a Shanghai `.SS` event as the `.SH` chart the server keys', () => {
    const sh = makeStored({ id: 'm', symbol: '600519.SH' });
    const ss = { op: 'remove' as const, ...ref, chart_id: '600519.SS:1day', symbol: '600519.SS', ids: ['m'] };
    expect(applyLayerOp(ss)([sh])).toEqual([]);
  });
});

describe('rowsOfChart', () => {
  it("keeps the chart's drawable rows and leaves out other timeframes and unknown versions", () => {
    const rows = [
      makeStored({ id: 'a' }),
      makeStored({ id: 'h', timeframe: '1hour' }),
      { ...makeStored({ id: 'v2' }), layer: { ...makeLayer({ id: 'v2' }), version: 2 as unknown as 1 } },
    ];
    expect(ids(rowsOfChart(rows, KEY))).toEqual(['a']);
  });
});

describe('applyChartLayerEvent', () => {
  let client: QueryClient;
  let stop: () => void;
  const { queryKey } = chartLayersQuery(KEY);
  const cached = () => client.getQueryData<StoredChartLayer[]>(queryKey);
  /** A chart on screen reading the query, so an invalidation reads the server again. */
  const watch = () => {
    stop = new QueryObserver(client, chartLayersQuery(KEY)).subscribe(() => {});
  };

  beforeEach(() => {
    client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    stop = () => {};
    resetLayerViewForTesting();
    fetchLayers.mockReset();
  });
  afterEach(() => {
    stop();
    client.clear();
  });

  it('writes a live add to the cache and asks to show it, un-hidden', async () => {
    client.setQueryData(queryKey, [makeStored({ id: 'a' })]);
    setLayerHidden(KEY, 'b', true);
    await applyChartLayerEvent(client, addPayload(WS, { id: 'b' }));
    expect(ids(cached())).toEqual(['a', 'b']);
    expect(getShowLayersRequest()).toMatchObject({ key: KEY, layerIds: ['b'] });
    expect(getHiddenLayers(KEY).has('b')).toBe(false);
  });

  it('applies a live remove without asking to show anything', async () => {
    client.setQueryData(queryKey, [makeStored({ id: 'a' })]);
    await applyChartLayerEvent(client, { op: 'remove', ...ref, ids: ['a'] });
    expect(cached()).toEqual([]);
    expect(getShowLayersRequest()).toBeNull();
  });

  it('does not bring back a layer removed through REST when a reconnect replays its add', async () => {
    // The viewer removed `a`, so neither the cache nor the server has it.
    fetchLayers.mockResolvedValue([makeStored({ id: 'b' })]);
    watch();
    await vi.waitFor(() => expect(ids(cached())).toEqual(['b']));
    await applyChartLayerEvent(client, addPayload(WS, { id: 'a' }), { replay: true });
    expect(ids(cached())).toEqual(['b']);
    expect(fetchLayers).toHaveBeenCalledTimes(2);
    expect(getShowLayersRequest()).toBeNull();
  });

  it('takes what a replay carried from the server instead', async () => {
    fetchLayers.mockResolvedValueOnce([makeStored({ id: 'a' })]);
    watch();
    await vi.waitFor(() => expect(ids(cached())).toEqual(['a']));
    fetchLayers.mockResolvedValueOnce([makeStored({ id: 'a' }), makeStored({ id: 'c' })]);
    await applyChartLayerEvent(client, addPayload(WS, { id: 'c' }), { replay: true });
    expect(ids(cached())).toEqual(['a', 'c']);
  });

  it('reads the chart again when the event cancels a read in flight', async () => {
    let land!: (rows: StoredChartLayer[]) => void;
    fetchLayers.mockImplementationOnce(() => new Promise((resolve) => { land = resolve; }));
    fetchLayers.mockResolvedValueOnce([makeStored({ id: 'a' }), makeStored({ id: 'h', timeframe: '1hour' })]);
    watch();
    expect(client.getQueryState(queryKey)?.fetchStatus).toBe('fetching');
    await applyChartLayerEvent(client, addPayload(WS, { id: 'a' }));
    land([]);
    await vi.waitFor(() => expect(ids(cached())).toEqual(['a', 'h']));
    expect(fetchLayers).toHaveBeenCalledTimes(2);
  });

  it('reads the server’s list for an add that came without its layer, writing nothing', async () => {
    fetchLayers.mockResolvedValueOnce([makeStored({ id: 'a' })]);
    watch();
    await vi.waitFor(() => expect(ids(cached())).toEqual(['a']));
    fetchLayers.mockResolvedValueOnce([makeStored({ id: 'a' }), makeStored({ id: 'big' })]);
    const { layer: _layer, ...bodyless } = addPayload(WS, { id: 'big' });
    await applyChartLayerEvent(client, bodyless);
    expect(ids(cached())).toEqual(['a', 'big']);
    expect(fetchLayers).toHaveBeenCalledTimes(2);
    // Live, it is still brought into view, as a layer with its body is.
    expect(getShowLayersRequest()).toMatchObject({ key: KEY, layerIds: ['big'] });
  });

  it('shows nothing for a replayed add', async () => {
    fetchLayers.mockResolvedValue([makeStored({ id: 'a' })]);
    watch();
    await applyChartLayerEvent(client, addPayload(WS, { id: 'a' }), { replay: true });
    expect(getShowLayersRequest()).toBeNull();
  });
});

describe('invalidateReportedLayers', () => {
  const published = (symbol: string, timeframe = '1day', workspace_id = WS) =>
    ({ workspace_id, chart_id: `${symbol}:${timeframe}`, symbol, timeframe, layer_id: 'x', title: 't', summary: null, as_of: null });
  let client: QueryClient;
  beforeEach(() => {
    client = new QueryClient();
  });
  afterEach(() => client.clear());

  it('marks each reported (workspace, symbol) entry stale, once, and no other', () => {
    const nvda = chartKeyFor(WS, 'NVDA', '1day')!;
    const aapl = chartKeyFor(WS, 'AAPL', '1day')!;
    const other = chartKeyFor('ws-2', 'NVDA', '1day')!;
    for (const k of [nvda, aapl, other]) client.setQueryData(chartLayersQuery(k).queryKey, []);
    const spy = vi.spyOn(client, 'invalidateQueries');
    invalidateReportedLayers(client, [published('NVDA'), published('NVDA', '1hour'), published('AAPL')]);
    expect(spy).toHaveBeenCalledTimes(2);
    const stale = (k: typeof nvda) => client.getQueryState(chartLayersQuery(k).queryKey)?.isInvalidated;
    expect([stale(nvda), stale(aapl), stale(other)]).toEqual([true, true, false]);
  });

  it('ignores a report with no layers', () => {
    const spy = vi.spyOn(client, 'invalidateQueries');
    invalidateReportedLayers(client, undefined);
    invalidateReportedLayers(client, null);
    invalidateReportedLayers(client, []);
    expect(spy).not.toHaveBeenCalled();
  });
});
