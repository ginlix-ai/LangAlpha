import { describe, it, expect } from 'vitest';
import { deriveMarketSession, getExtendedHoursType } from '../marketSession';
import type { MarketSession, MarketSessionInputs } from '../marketSession';

/** Venue wall-clock encoded as fake-UTC chart seconds (the chart's encoding). */
const chartSec = (iso: string) => Date.parse(`${iso}Z`) / 1000;

// One reference instant per venue evening/morning, expressed as REAL instants
// (with offset) so the daily settled-head date comparison exercises real tz math.
const US_MORNING = new Date('2026-07-06T08:00:00-04:00'); // pre-market ET
const US_MIDDAY = new Date('2026-07-06T12:00:00-04:00');
const US_EVENING = new Date('2026-07-06T18:55:00-04:00'); // after-hours ET
const US_NIGHT = new Date('2026-07-06T22:00:00-04:00'); // past 20:00 ET
const HK_EVENING = new Date('2026-07-06T22:00:00+08:00'); // past HK close

type Row = {
  name: string;
  in: MarketSessionInputs;
  out: Partial<MarketSession>;
};

const TABLE: Row[] = [
  // --- US equity, extended-hours intervals: label follows the HEAD BAR's window ---
  {
    name: 'US 1min regular hours → default styling',
    in: { symbol: 'AMD', interval: '1min', phase: 'open', headBarTime: chartSec('2026-07-06T11:59:00'), now: US_MIDDAY },
    out: { priceMark: null, showRegularCloseLine: false },
  },
  {
    name: 'US 1min after-hours → ext-post + official-close reference line',
    in: { symbol: 'AMD', interval: '1min', phase: 'post', headBarTime: chartSec('2026-07-06T18:54:00'), now: US_EVENING },
    out: { priceMark: 'ext-post', showRegularCloseLine: true },
  },
  {
    name: 'US 1min pre-market → ext-pre, no close line',
    in: { symbol: 'AMD', interval: '1min', phase: 'pre', headBarTime: chartSec('2026-07-06T08:00:00'), now: US_MORNING },
    out: { priceMark: 'ext-pre', showRegularCloseLine: false },
  },
  {
    name: 'US 1min at 16:01 with a lagging open phase → bar time wins (ext-post now)',
    in: { symbol: 'AMD', interval: '1min', phase: 'open', headBarTime: chartSec('2026-07-06T16:01:00'), now: US_EVENING },
    out: { priceMark: 'ext-post' },
  },
  {
    // The head bar is the last AH print, not the official close — the settled
    // label must say so, and the official close keeps its reference line.
    name: 'US 1min overnight (venue closed, AH head bar) → settled AH close + official-close line',
    in: { symbol: 'AMD', interval: '1min', phase: 'closed', headBarTime: chartSec('2026-07-06T19:59:00'), now: US_NIGHT },
    out: { priceMark: 'settled-ext-post', showRegularCloseLine: true },
  },
  {
    name: 'US 1min closed with a pre-market head bar → settled PM close, no close line',
    in: { symbol: 'AMD', interval: '1min', phase: 'closed', headBarTime: chartSec('2026-07-06T09:15:00'), now: US_NIGHT },
    out: { priceMark: 'settled-ext-pre', showRegularCloseLine: false },
  },
  {
    name: 'US 1min closed with a regular-session head bar → settled official close',
    in: { symbol: 'AMD', interval: '1min', phase: 'closed', headBarTime: chartSec('2026-07-06T15:59:00'), now: US_NIGHT },
    out: { priceMark: 'settled-close', showRegularCloseLine: false },
  },
  {
    name: 'US 4hour is not an ext interval: post phase → default styling',
    in: { symbol: 'AMD', interval: '4hour', phase: 'post', headBarTime: chartSec('2026-07-06T16:00:00'), now: US_EVENING },
    out: { priceMark: null, showRegularCloseLine: false },
  },
  {
    name: 'US 4hour closed → settled-close',
    in: { symbol: 'AMD', interval: '4hour', phase: 'closed', headBarTime: chartSec('2026-07-06T16:00:00'), now: US_NIGHT },
    out: { priceMark: 'settled-close' },
  },

  // --- Non-US / index intraday: no ext labels, phase drives the settle ---
  {
    name: 'HK 1min during lunch (legacy phase "open") → default styling',
    in: { symbol: '0700.HK', interval: '1min', phase: 'open', headBarTime: chartSec('2026-07-06T11:59:00'), now: US_MIDDAY },
    out: { priceMark: null },
  },
  {
    name: 'HK 1min evening (closed) → settled-close',
    in: { symbol: '0700.HK', interval: '1min', phase: 'closed', headBarTime: chartSec('2026-07-06T15:59:00'), now: HK_EVENING },
    out: { priceMark: 'settled-close' },
  },
  {
    name: 'HK 1min evening with a legacy null phase → no settle (pre-CMDP fallback)',
    in: { symbol: '0700.HK', interval: '1min', phase: null, headBarTime: chartSec('2026-07-06T15:59:00'), now: HK_EVENING },
    out: { priceMark: null },
  },
  {
    name: 'US index 1min closed → settled-close (no ext label for indexes)',
    in: { symbol: '^GSPC', interval: '1min', phase: 'closed', headBarTime: chartSec('2026-07-06T19:59:00'), now: US_NIGHT },
    out: { priceMark: 'settled-close' },
  },

  // --- 1day: fold gate + settled head ---
  {
    name: 'US 1day mid-session → live fold on, default styling',
    in: { symbol: 'AMD', interval: '1day', phase: 'open', headBarTime: chartSec('2026-07-06T00:00:00'), now: US_MIDDAY },
    out: { priceMark: null, foldDailyQuote: true },
  },
  {
    name: 'US 1day after-hours → fold OFF (AH tape must not enter the daily candle)',
    in: { symbol: 'AMD', interval: '1day', phase: 'post', headBarTime: chartSec('2026-07-06T00:00:00'), now: US_EVENING },
    out: { priceMark: null, foldDailyQuote: false },
  },
  {
    name: 'US 1day overnight closed, head bar still venue-today → settled-close, fold off',
    in: { symbol: 'AMD', interval: '1day', phase: 'closed', headBarTime: chartSec('2026-07-06T00:00:00'), now: US_NIGHT },
    out: { priceMark: 'settled-close', foldDailyQuote: false },
  },
  {
    name: 'US 1day pre-market with yesterday head bar → settled-close via date rule',
    in: { symbol: 'AMD', interval: '1day', phase: 'pre', headBarTime: chartSec('2026-07-02T00:00:00'), now: US_MORNING },
    out: { priceMark: 'settled-close', foldDailyQuote: false },
  },
  {
    name: 'US 1day null phase, head bar today → fold on (pre-CMDP date-only gate)',
    in: { symbol: 'AMD', interval: '1day', phase: null, headBarTime: chartSec('2026-07-06T00:00:00'), now: US_MIDDAY },
    out: { priceMark: null, foldDailyQuote: true },
  },
  {
    name: 'US 1day null phase, head bar yesterday → settled, fold off',
    in: { symbol: 'AMD', interval: '1day', phase: null, headBarTime: chartSec('2026-07-02T00:00:00'), now: US_MORNING },
    out: { priceMark: 'settled-close', foldDailyQuote: false },
  },
  {
    name: 'HK 1day evening (closed, head bar venue-today) → settled-close, fold off',
    in: { symbol: '0700.HK', interval: '1day', phase: 'closed', headBarTime: chartSec('2026-07-06T00:00:00'), now: HK_EVENING },
    out: { priceMark: 'settled-close', foldDailyQuote: false },
  },

  // --- Empty series ---
  {
    name: 'empty series → nothing to mark, no fold, no close line',
    in: { symbol: 'AMD', interval: '1min', phase: 'closed', headBarTime: null, now: US_NIGHT },
    out: { priceMark: null, foldDailyQuote: false, showRegularCloseLine: false },
  },
];

describe('deriveMarketSession', () => {
  it.each(TABLE)('$name', ({ in: inputs, out }) => {
    const session = deriveMarketSession(inputs);
    for (const [key, value] of Object.entries(out)) {
      expect(session[key as keyof MarketSession], key).toBe(value);
    }
  });

  describe('badge', () => {
    it('WS delivering wins over a stale closed phase', () => {
      const s = deriveMarketSession({
        symbol: 'AMD', interval: '1min', phase: 'closed',
        headBarTime: chartSec('2026-07-06T15:59:00'), wsLive: true, now: US_MIDDAY,
      });
      expect(s.badge).toBe('live');
    });

    it('closed phase without WS → closed', () => {
      const s = deriveMarketSession({
        symbol: '0700.HK', interval: '1min', phase: 'closed',
        headBarTime: chartSec('2026-07-06T15:59:00'), now: HK_EVENING,
      });
      expect(s.badge).toBe('closed');
    });

    it('open phase without WS → delayed; unknown phase → delayed', () => {
      const base = {
        symbol: 'AMD', interval: '1min',
        headBarTime: chartSec('2026-07-06T11:59:00'), now: US_MIDDAY,
      };
      expect(deriveMarketSession({ ...base, phase: 'open' }).badge).toBe('delayed');
      expect(deriveMarketSession({ ...base, phase: null }).badge).toBe('delayed');
    });
  });
});

describe('getExtendedHoursType', () => {
  it('classifies against the 09:30–16:00 ET regular session', () => {
    expect(getExtendedHoursType(chartSec('2026-07-06T09:29:00'))).toBe('pre');
    expect(getExtendedHoursType(chartSec('2026-07-06T09:30:00'))).toBeNull();
    expect(getExtendedHoursType(chartSec('2026-07-06T15:59:00'))).toBeNull();
    expect(getExtendedHoursType(chartSec('2026-07-06T16:00:00'))).toBe('post');
  });
});
