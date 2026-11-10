import { beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('@/api/client', () => ({ api: { get: vi.fn(), defaults: { baseURL: '' } } }));

import { api } from '@/api/client';
import { makeStored } from '../../stores/__tests__/layerFixture';
import { fetchChartLayers } from '../api';

const get = vi.mocked(api.get);

describe('fetchChartLayers', () => {
  beforeEach(() => get.mockReset());

  it('flattens every chart’s layers', async () => {
    get.mockResolvedValue({ data: { charts: [{ layers: [makeStored({ id: 'a' })] }, { layers: [makeStored({ id: 'b' })] }] } } as never);
    expect((await fetchChartLayers('ws-1', 'NVDA')).map((r) => r.layer_id)).toEqual(['a', 'b']);
  });

  it('reads a chart an older server sent without a `layers` key as having none', async () => {
    get.mockResolvedValue({ data: { charts: [{}, { layers: [makeStored({ id: 'a' })] }] } } as never);
    expect((await fetchChartLayers('ws-1', 'NVDA')).map((r) => r.layer_id)).toEqual(['a']);
  });
});
