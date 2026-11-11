import { QueryClient } from '@tanstack/react-query';
import { afterEach, describe, expect, it, vi } from 'vitest';
import i18n from '@/i18n';
import { methodEntry, toQuote } from '../methods';

const fetchTodayInsights = vi.fn();
const fetchEarningsCalendar = vi.fn();
const getCompanyProfiles = vi.fn();
const getNews = vi.fn();

vi.mock('@/pages/Dashboard/utils/api', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/pages/Dashboard/utils/api')>()),
  fetchTodayInsights: () => fetchTodayInsights(),
  fetchEarningsCalendar: (args: unknown) => fetchEarningsCalendar(args),
  getCompanyProfiles: (symbols: string[]) => getCompanyProfiles(symbols),
  getNews: (args: unknown) => getNews(args),
}));

function run(name: string, params: Record<string, unknown> = {}) {
  const entry = methodEntry(name);
  if (entry?.kind !== 'read') throw new Error(`${name} is not a read`);
  return entry.run(params, new QueryClient());
}

afterEach(() => { vi.useRealTimers(); });

describe('methodEntry', () => {
  it('names only the registry\'s own methods', () => {
    expect(methodEntry('news.list')).toMatchObject({ scope: 'news', kind: 'read' });
    expect(methodEntry('quotes.stream')).toMatchObject({ scope: 'quotes', kind: 'stream' });
    expect(methodEntry('toString')).toBeNull();
    expect(methodEntry('constructor')).toBeNull();
    expect(methodEntry('__proto__')).toBeNull();
  });
});

describe('reads that fail', () => {
  it('reject rather than answer an empty brief', async () => {
    fetchTodayInsights.mockRejectedValue(new Error('down'));
    await expect(run('brief.today')).rejects.toThrow('down');
  });
});

describe('toQuote', () => {
  it('prices a quote before the open at the latest minute, which its change is measured to', () => {
    const early = { symbol: 'NVDA', price: null, last_minute_close: 180.7, change: 2.03, change_percent: 1.136, previous_close: 178.67 };
    expect(toQuote('NVDA', early)).toMatchObject({ price: 180.7, change: 2.03, changePercent: 1.136, previousClose: 178.67 });
    expect(toQuote('NVDA', { ...early, price: 179 })).toMatchObject({ price: 179 });
  });
});

describe('brief.today', () => {
  it('leaves out the personalized brief, which is written from the holdings', async () => {
    fetchTodayInsights.mockResolvedValue([
      { market_insight_id: 'a', type: 'pre_market', headline: 'Futures flat', summary: null, completed_at: null },
      { market_insight_id: 'b', type: 'personalized', headline: 'Your NVDA position', summary: null, completed_at: null },
    ]);
    await expect(run('brief.today')).resolves.toEqual([
      { id: 'a', title: 'Futures flat', summary: null, createdAt: null },
    ]);
  });
});

describe('news.list', () => {
  it('reads the feed again in a new language, which picks the feed', async () => {
    const queryClient = new QueryClient();
    const entry = methodEntry('news.list');
    if (entry?.kind !== 'read') throw new Error('news.list is not a read');
    getNews.mockReset();
    getNews.mockResolvedValue({ results: [], count: 0, next_cursor: null });
    const was = i18n.language;
    try {
      await entry.run({}, queryClient);
      await entry.run({}, queryClient);
      expect(getNews).toHaveBeenCalledTimes(1);
      i18n.language = was === 'zh-CN' ? 'en-US' : 'zh-CN';
      await entry.run({}, queryClient);
      expect(getNews).toHaveBeenCalledTimes(2);
    } finally {
      i18n.language = was;
    }
  });
});

describe('profiles.get', () => {
  const nvda = { symbol: 'NVDA', name: 'NVIDIA', sector: 'Technology' };
  const profiles = (queryClient: QueryClient) => {
    const entry = methodEntry('profiles.get');
    if (entry?.kind !== 'read') throw new Error('profiles.get is not a read');
    return entry.run({ symbols: ['NVDA', 'AMD'] }, queryClient);
  };

  it('asks again after an answer a symbol failed in, and keeps a whole one', async () => {
    const queryClient = new QueryClient();
    getCompanyProfiles.mockReset();
    getCompanyProfiles.mockResolvedValueOnce({ results: { NVDA: nvda }, errors: { AMD: 'vendor timeout' } });
    await expect(profiles(queryClient)).resolves.toEqual([nvda]);
    // The vendor answers again: the failed symbol is not remembered as having no profile.
    const amd = { symbol: 'AMD', name: 'AMD', sector: 'Technology' };
    getCompanyProfiles.mockResolvedValue({ results: { NVDA: nvda, AMD: amd }, errors: {} });
    await expect(profiles(queryClient)).resolves.toEqual([nvda, amd]);
    await profiles(queryClient);
    expect(getCompanyProfiles).toHaveBeenCalledTimes(2);
  });
});

describe('earnings.calendar', () => {
  it('takes its default window from the user\'s own day, not UTC\'s', async () => {
    vi.useFakeTimers({ toFake: ['Date'] });
    // Late evening of the 3rd somewhere west of UTC is already the 4th in UTC.
    const local = new Date(2026, 5, 3, 23, 30);
    vi.setSystemTime(local);
    fetchEarningsCalendar.mockResolvedValue({ data: [], count: 0 });
    await run('earnings.calendar');
    expect(fetchEarningsCalendar).toHaveBeenCalledWith({ from: '2026-06-03', to: '2026-06-10' });
  });
});
