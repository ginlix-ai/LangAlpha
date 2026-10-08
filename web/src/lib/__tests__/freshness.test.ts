// @vitest-environment node
import { describe, expect, it } from 'vitest';

import {
  asQuoteTier,
  chartFreshnessParts,
  venueTime,
  isLiveRow,
  lagMinutes,
  lastBarLabel,
  resolveChartFreshness,
  resolveFreshnessBadge,
  type FreshnessFields,
} from '../freshness';
import type { Freshness } from '@/types/market';

const f = (partial: Partial<Freshness>): Freshness =>
  ({ label: 'unknown', measured: false, ...partial }) as Freshness;

describe('resolveFreshnessBadge', () => {
  const cases: Array<[string, FreshnessFields, string | null, number | undefined]> = [
    ['no fields at all', {}, null, undefined],
    ['measured live reads realtime', { freshness: f({ label: 'live', measured: true }) }, 'marketView.header.statusRealtime', undefined],
    [
      'measured live wins over a delayed declaration',
      { tier: 'delayed_15m', freshness: f({ label: 'live', measured: true }) },
      'marketView.header.statusRealtime',
      undefined,
    ],
    ['declared realtime, nothing measured', { tier: 'realtime' }, 'marketView.header.statusRealtime', undefined],
    [
      'a daily series one session behind reads last close, not 1440 minutes',
      { freshness: f({ label: 'delayed', measured: true, lag_s: 86400, interval: '1day' }) },
      'marketView.header.statusLastClose',
      undefined,
    ],
    [
      'a declared delay the bars were too wide to measure, no tier on the row',
      { freshness: f({ label: 'delayed', measured: false, interval: '4hour' }) },
      'marketView.header.statusDelayed15m',
      undefined,
    ],
    ['declared 15-min delay, nothing measured', { tier: 'delayed_15m' }, 'marketView.header.statusDelayed15m', undefined],
    [
      'declared 15-min delay with an unmeasured freshness block',
      { tier: 'delayed_15m', freshness: f({ label: 'unknown', measured: false }) },
      'marketView.header.statusDelayed15m',
      undefined,
    ],
    [
      'measured delay rounds to whole minutes',
      { tier: 'delayed_15m', freshness: f({ label: 'delayed', measured: true, lag_s: 754 }) },
      'marketView.header.statusDelayedMin',
      13,
    ],
    [
      'a sub-minute measured delay floors at one minute',
      { freshness: f({ label: 'delayed', measured: true, lag_s: 12 }) },
      'marketView.header.statusDelayedMin',
      1,
    ],
    ['stale', { freshness: f({ label: 'stale', measured: true, lag_s: 90_000 }) }, 'marketView.header.statusStale', undefined],
    ['end-of-day tier', { tier: 'eod' }, 'marketView.header.statusLastClose', undefined],
    [
      'end-of-day reads "last close", never "stale"',
      { tier: 'eod', freshness: f({ label: 'stale', measured: true, lag_s: 90_000 }) },
      'marketView.header.statusLastClose',
      undefined,
    ],
    ['an unknown measurement with no tier says nothing', { freshness: f({ label: 'unknown', measured: true }) }, null, undefined],
    [
      'a current quote on a closed venue is the last close, not realtime',
      { tier: 'realtime', freshness: f({ label: 'live', measured: true, closed: true }) },
      'marketView.header.statusLastClose',
      undefined,
    ],
    [
      'a realtime declaration on a closed venue is the last close too',
      { tier: 'realtime', freshness: f({ label: 'live', measured: false, closed: true }) },
      'marketView.header.statusLastClose',
      undefined,
    ],
    [
      'a declared delay on a closed venue is the last close, not "delayed 15 min"',
      { tier: 'delayed_15m', freshness: f({ label: 'delayed', measured: false, closed: true }) },
      'marketView.header.statusLastClose',
      undefined,
    ],
    [
      'a stale print on a closed venue still reads stale',
      { tier: 'realtime', freshness: f({ label: 'stale', measured: true, closed: true }) },
      'marketView.header.statusStale',
      undefined,
    ],
  ];

  it.each(cases)('%s', (_name, fields, key, minutes) => {
    const spec = resolveFreshnessBadge(fields);
    expect(spec?.key ?? null).toBe(key);
    expect(spec?.minutes).toBe(minutes);
  });
});

describe('lagMinutes', () => {
  it('floors at one minute and rounds to nearest otherwise', () => {
    expect(lagMinutes(null)).toBe(1);
    expect(lagMinutes(0)).toBe(1);
    expect(lagMinutes(29)).toBe(1);
    expect(lagMinutes(90)).toBe(2);
    expect(lagMinutes(900)).toBe(15);
  });
});

describe('isLiveRow', () => {
  it('is true for measured live and for an unmeasured realtime declaration only', () => {
    expect(isLiveRow({ freshness: f({ label: 'live', measured: true }) })).toBe(true);
    expect(isLiveRow({ tier: 'realtime' })).toBe(true);
    expect(isLiveRow({ tier: 'delayed_15m' })).toBe(false);
    expect(isLiveRow({})).toBe(false);
  });

  it('is false on a closed venue, however current the row is', () => {
    expect(isLiveRow({ freshness: f({ label: 'live', measured: true, closed: true }) })).toBe(false);
    expect(isLiveRow({ tier: 'realtime', freshness: f({ label: 'live', closed: true }) })).toBe(false);
  });
});

describe('cases the header and the badge used to disagree on', () => {
  it('an eod row measured a session behind on daily bars reads last close', () => {
    const spec = resolveFreshnessBadge({
      tier: 'eod',
      freshness: f({ label: 'delayed', measured: true, lag_s: 86_400, interval: '1day' }),
    });
    expect(spec?.key).toBe('marketView.header.statusLastClose');
  });

  it.each(['1day', 'daily', '1d'])('treats %s as a daily interval', (interval) => {
    const spec = resolveFreshnessBadge({ freshness: f({ label: 'delayed', measured: true, lag_s: 86_400, interval }) });
    expect(spec?.key).toBe('marketView.header.statusLastClose');
  });

  it('an unmeasured delay reads the declared 15-minute delay', () => {
    expect(resolveFreshnessBadge({ freshness: f({ label: 'delayed', measured: false }) })?.key)
      .toBe('marketView.header.statusDelayed15m');
  });
});

describe('resolveChartFreshness', () => {
  it('phrases each state for the chart line', () => {
    expect(resolveChartFreshness(f({ label: 'live', measured: true }))?.key).toBe('marketView.header.chartLive');
    expect(resolveChartFreshness(f({ label: 'delayed', measured: true, lag_s: 600, interval: '1min' })))
      .toEqual({ key: 'marketView.header.chartDelayedMin', minutes: 10, state: 'delayed' });
    expect(resolveChartFreshness(f({ label: 'incomplete', measured: true, lag_s: 180 })))
      .toEqual({ key: 'marketView.header.chartShortOfClose', minutes: 3, state: 'incomplete' });
    expect(resolveChartFreshness(f({ label: 'stale', measured: true }))?.key).toBe('marketView.header.statusStale');
  });

  it('a complete series on a closed venue reads last close, never live', () => {
    for (const interval of ['5min', '1day']) {
      const spec = resolveChartFreshness(f({ label: 'live', measured: true, closed: true, interval }));
      expect(spec).toEqual({ key: 'marketView.header.statusLastClose', state: 'eod' });
    }
  });

  it('a daily chart a session behind reads last close, as the badge does', () => {
    expect(resolveChartFreshness(f({ label: 'delayed', measured: true, lag_s: 86_400, interval: 'daily' }))?.key)
      .toBe('marketView.header.statusLastClose');
  });

  it('says nothing when nothing was measured or declared', () => {
    expect(resolveChartFreshness(null)).toBeNull();
    expect(resolveChartFreshness(f({ label: 'unknown', measured: true }))).toBeNull();
  });
});

describe('lastBarLabel', () => {
  const ms = Date.UTC(2026, 8, 9, 6, 56);

  it('prints a clock time in venue time for intraday bars', () => {
    expect(lastBarLabel(f({ interval: '1min', actual_latest: ms }), '600519.SS', 'en-US', ms)).toBe('14:56');
  });

  it('dates an intraday bar from an earlier venue day', () => {
    const sunday = Date.UTC(2026, 8, 13, 2, 0);
    const label = lastBarLabel(f({ interval: '1min', actual_latest: ms }), '600519.SS', 'en-US', sunday);
    expect(label).toMatch(/14:56/);
    expect(label).not.toBe('14:56');
  });

  it('prints in the locale it is given, not the browser\'s', () => {
    // Both sides, so a host whose own locale is zh cannot pass it by default.
    expect(lastBarLabel(f({ interval: '1day', actual_latest: ms }), '600519.SS', 'zh-CN', ms)).toMatch(/9月/);
    expect(lastBarLabel(f({ interval: '1day', actual_latest: ms }), '600519.SS', 'en-US', ms)).toBe('Sep 9');
  });

  it('prints a date for every spelling of a daily interval', () => {
    for (const interval of ['1day', 'daily', '1d']) {
      expect(lastBarLabel(f({ interval, actual_latest: ms }), '600519.SS', 'en-US', ms)).not.toMatch(/\d{2}:\d{2}/);
    }
  });

  it('is null without a stamp', () => {
    expect(lastBarLabel(f({ interval: '1min' }), 'AMD', 'en-US', ms)).toBeNull();
  });
});

describe('asQuoteTier', () => {
  it('keeps the declared tiers and drops anything else', () => {
    expect(asQuoteTier('eod')).toBe('eod');
    expect(asQuoteTier('delayed_15m')).toBe('delayed_15m');
    expect(asQuoteTier('bogus')).toBeNull();
    expect(asQuoteTier(undefined)).toBeNull();
  });
});

describe('chartFreshnessParts', () => {
  // Echo the key and its interpolation so the test pins composition, not copy.
  const t = ((k: string, o?: Record<string, unknown>) => (o ? `${k}:${JSON.stringify(o)}` : k)) as never;
  const NOW = Date.UTC(2026, 6, 14, 20, 0);

  it('returns source, state and last bar separately, null where absent', () => {
    const parts = chartFreshnessParts(
      t,
      f({ label: 'live', measured: true, source: 'tushare', interval: '1min', actual_latest: Date.UTC(2026, 6, 14, 19, 45) }),
      'ACME', 'en-US', NOW,
    );
    expect(parts.source).toBe('Tushare');
    expect(parts.state).toBe('marketView.header.chartLive');
    expect(parts.lastBar).toBe('marketView.header.chartLastBar:{"time":"15:45"}');
    expect(chartFreshnessParts(t, null, 'ACME', 'en-US', NOW)).toEqual({ source: null, state: null, lastBar: null });
  });

  it('names a daily series by key, not a provider', () => {
    expect(chartFreshnessParts(t, f({ source: 'daily' }), 'ACME', 'en-US', NOW).source).toBe('marketView.header.sourceDaily');
  });
});

describe('venueTime same-day', () => {
  it('compares calendar days on the venue clock, not the reader\'s', () => {
    // 03:00 UTC Jul 14 is still Jul 13 in New York; 05:00 UTC is 01:00 on the 14th.
    const before = Date.UTC(2026, 6, 14, 3, 0);
    const now = Date.UTC(2026, 6, 14, 5, 0);
    expect(venueTime(now, 'ACME', 'en-US', now)).toBe('01:00');
    expect(venueTime(before, 'ACME', 'en-US', now)).toMatch(/Jul 13/);
  });
});
