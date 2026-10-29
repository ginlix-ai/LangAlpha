// @vitest-environment node
import { describe, it, expect, vi, beforeEach } from 'vitest';

const getTodayInsights = vi.fn(async (): Promise<Record<string, unknown>[]> => []);
const getInsightDetail = vi.fn(async (_id: string): Promise<Record<string, unknown>> => ({}));
vi.mock('@/pages/Dashboard/utils/api', () => ({
  getTodayInsights: () => getTodayInsights(),
  getInsightDetail: (id: string) => getInsightDetail(id),
}));

import { MAX_BRIEF_CHARS } from '../welcome/kickoff';
import { fetchMarketBrief } from '../welcome/marketBrief';

const detail = {
  market_insight_id: 'm1',
  type: 'market_update',
  headline: 'Stocks slip as yields climb',
  summary: 'Treasury yields rose for a third day.',
  completed_at: '2026-10-19T14:00:00Z',
  topics: [{ text: 'Rates', trend: 'up' }],
  content: [{ title: 'Yields climb', body: 'The ten-year touched 4.6%.', url: 'https://example.com/a' }],
  sources: [{ url: 'https://example.com/a', title: 'Example' }],
};

describe('fetchMarketBrief', () => {
  beforeEach(() => {
    getTodayInsights.mockReset().mockResolvedValue([]);
    getInsightDetail.mockReset().mockResolvedValue(detail);
  });

  it('reads the newest market-wide brief in full, as markdown', async () => {
    getTodayInsights.mockResolvedValue([
      { market_insight_id: 'p1', type: 'personalized', headline: 'Your portfolio today' },
      { market_insight_id: 'm1', type: 'market_update', headline: 'Stocks slip as yields climb' },
      { market_insight_id: 'm0', type: 'pre_market', headline: 'Earlier' },
    ]);
    const brief = await fetchMarketBrief();
    // One written for the user is passed over: the turn wants what moved.
    expect(getInsightDetail).toHaveBeenCalledWith('m1');
    expect(brief).toContain('## Stocks slip as yields climb');
    expect(brief).toContain('Treasury yields rose for a third day.');
    expect(brief).toContain('**Topics:** Rates (up)');
    expect(brief).toContain('1. **Yields climb** — https://example.com/a');
  });

  it('is none when there is no brief, or only one written for the user', async () => {
    expect(await fetchMarketBrief()).toBeNull();
    getTodayInsights.mockResolvedValue([{ market_insight_id: 'p1', type: 'personalized' }]);
    expect(await fetchMarketBrief()).toBeNull();
    expect(getInsightDetail).not.toHaveBeenCalled();
  });

  it('is none when the detail fails or comes back without a headline', async () => {
    getTodayInsights.mockResolvedValue([{ market_insight_id: 'm1', type: 'market_update' }]);
    getInsightDetail.mockRejectedValueOnce(new Error('404'));
    expect(await fetchMarketBrief()).toBeNull();
    getInsightDetail.mockResolvedValueOnce({ market_insight_id: 'm1', type: 'market_update' });
    expect(await fetchMarketBrief()).toBeNull();
  });

  it('cuts a runaway brief to what the opening turn carries', async () => {
    getTodayInsights.mockResolvedValue([{ market_insight_id: 'm1', type: 'market_update' }]);
    getInsightDetail.mockResolvedValue({ ...detail, summary: 'x'.repeat(MAX_BRIEF_CHARS * 2) });
    expect(await fetchMarketBrief()).toHaveLength(MAX_BRIEF_CHARS);
  });
});
