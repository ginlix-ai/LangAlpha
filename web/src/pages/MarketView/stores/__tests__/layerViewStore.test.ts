// @vitest-environment node
import { afterEach, describe, expect, it, vi } from 'vitest';

import {
  clearShowRequest,
  getHiddenLayers,
  getShowLayersRequest,
  requestShowLayers,
  resetLayerViewForTesting,
  setLayerHidden,
  showRequestExpired,
  SHOW_REQUEST_TTL_MS,
} from '../layerViewStore';
import { chartKeyFor } from '../../utils/chartInstanceKey';

const KEY = chartKeyFor('ws-1', 'NVDA', '1day')!;
const OTHER = chartKeyFor('ws-1', 'NVDA', '1hour')!;

afterEach(() => {
  resetLayerViewForTesting();
  vi.useRealTimers();
});

describe('layerViewStore', () => {
  it('hides and shows a layer per chart', () => {
    setLayerHidden(KEY, 'a');
    expect(getHiddenLayers(KEY).has('a')).toBe(true);
    expect(getHiddenLayers(OTHER).has('a')).toBe(false);
    setLayerHidden(KEY, 'a');
    expect(getHiddenLayers(KEY).has('a')).toBe(false);
  });

  it("keeps the hidden set's identity across a write that changes nothing", () => {
    setLayerHidden(KEY, 'a', true);
    const before = getHiddenLayers(KEY);
    setLayerHidden(KEY, 'a', true);
    setLayerHidden(OTHER, 'b', true);
    expect(getHiddenLayers(KEY)).toBe(before);
  });

  it('un-hides the layers a show request names, and keeps the rest hidden', () => {
    setLayerHidden(KEY, 'a', true);
    setLayerHidden(KEY, 'b', true);
    requestShowLayers(KEY, ['a']);
    expect([...getHiddenLayers(KEY)]).toEqual(['b']);
    expect(getShowLayersRequest()).toMatchObject({ key: KEY, layerIds: ['a'] });
  });

  it('lets a newer request replace an older one, which then cannot clear it', () => {
    requestShowLayers(KEY, ['a']);
    const first = getShowLayersRequest()!;
    requestShowLayers(OTHER, ['b']);
    clearShowRequest(first.seq);
    expect(getShowLayersRequest()).toMatchObject({ key: OTHER });
    clearShowRequest(getShowLayersRequest()!.seq);
    expect(getShowLayersRequest()).toBeNull();
  });

  it('ignores a request naming no layer', () => {
    requestShowLayers(KEY, ['']);
    expect(getShowLayersRequest()).toBeNull();
  });

  it('expires a request no chart took in time', () => {
    vi.useFakeTimers();
    requestShowLayers(KEY, ['a']);
    const req = getShowLayersRequest()!;
    expect(showRequestExpired(req)).toBe(false);
    vi.advanceTimersByTime(SHOW_REQUEST_TTL_MS + 1);
    expect(showRequestExpired(req)).toBe(true);
  });
});
