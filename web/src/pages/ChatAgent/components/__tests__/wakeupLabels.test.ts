import { describe, it, expect } from 'vitest';
import type { TFunction } from 'i18next';
import type { Wakeup } from '@/types/wakeups';
import { chipLabel, nextFireAt, orderOutcomeLabel, triggerText, wakeupsAt, zoneCity } from '../wakeupLabels';

function wakeup(over: Partial<Wakeup>): Wakeup {
  return {
    wakeup_id: 'w-1', short_id: 'w_3f2a', label: null, kind: 'price', state: 'armed',
    timezone: null, cron: null, at: null, until: null, next_fire_at: null,
    fires_done: 0, fires_total: 1, ends_at: null, expires_at: null, fired_at: null,
    price: null, order: null, cancellable: true, legacy: false, created_at: '2030-01-01T00:00:00Z',
    ...over,
  };
}

describe('zoneCity', () => {
  it('reads the IANA city, and names the exchange cities in Chinese', () => {
    expect(zoneCity('America/New_York', 'en-US')).toBe('New York');
    expect(zoneCity('America/New_York', 'zh-CN')).toBe('纽约');
    expect(zoneCity('Asia/Hong_Kong', 'zh-CN')).toBe('香港');
  });

  it('falls back to the English city for any other zone', () => {
    expect(zoneCity('America/Sao_Paulo', 'zh-CN')).toBe('Sao Paulo');
  });
});

describe('nextFireAt', () => {
  it('is the soonest timed fire, or null when none is timed', () => {
    const a = wakeup({ next_fire_at: '2030-01-03T00:00:00Z' });
    const b = wakeup({ next_fire_at: '2030-01-02T00:00:00Z' });
    expect(nextFireAt([a, b, wakeup({})])).toBe('2030-01-02T00:00:00Z');
    expect(nextFireAt([wakeup({})])).toBeNull();
  });
});

describe('wakeupsAt', () => {
  const now = Date.parse('2030-01-02T00:00:00Z');

  it('reads an armed price or order wakeup past its window as lapsed or expired', () => {
    const price = wakeup({ expires_at: '2030-01-01T00:00:00Z' });
    const order = wakeup({ wakeup_id: 'o', kind: 'order', expires_at: '2030-01-01T00:00:00Z' });
    const [p, o] = wakeupsAt([price, order], now);
    expect(p).toMatchObject({ state: 'lapsed', cancellable: false });
    expect(o).toMatchObject({ state: 'expired', cancellable: false });
  });

  it('returns the same list when none has lapsed', () => {
    const list = [wakeup({ expires_at: '2030-02-01T00:00:00Z' }), wakeup({ kind: 'cron', expires_at: '2020-01-01T00:00:00Z' })];
    expect(wakeupsAt(list, now)).toBe(list);
  });
});

describe('chipLabel time', () => {
  // Echoes the time argument so the test reads what the chip would show.
  const t = ((_key: string, o?: { time?: string }) => o?.time ?? '') as unknown as TFunction;
  const now = Date.parse('2030-03-06T12:00:00Z');

  it('shows the clock alone for a time today, in the zone it is read in', () => {
    const w = wakeup({ state: 'armed', kind: 'at', next_fire_at: '2030-03-06T18:15:00Z' });
    expect(chipLabel([w], t, 'en-US', now, 'UTC')).toBe('6:15 PM');
  });

  it('adds the weekday for another day', () => {
    const w = wakeup({ state: 'armed', kind: 'at', next_fire_at: '2030-03-07T18:15:00Z' });
    expect(chipLabel([w], t, 'en-US', now, 'UTC')).toBe('Thu 6:15 PM');
  });

  it('judges today in the reader zone, not the machine zone', () => {
    const w = wakeup({ state: 'armed', kind: 'at', next_fire_at: '2030-03-07T01:15:00Z' });
    expect(chipLabel([w], t, 'en-US', now, 'America/New_York')).toBe('8:15 PM');
  });
});

describe('orderOutcomeLabel price', () => {
  const t = ((_key: string, o?: { price?: string }) => o?.price ?? '') as unknown as TFunction;
  const filled = { vendor_order_id: 'o1', summary: 'Buy 1 ABC', outcome: 'filled' as const, filled_qty: 1, qty: 1, avg_fill_price: 1234.5 };

  it('prices a fill in the order currency', () => {
    expect(orderOutcomeLabel({ ...filled, currency: 'USD' }, t, 'en-US')).toBe('$1,234.50');
    expect(orderOutcomeLabel({ ...filled, currency: 'HKD' }, t, 'en-US')).toBe('HK$1,234.50');
  });

  it('falls back to a plain number without a usable currency', () => {
    expect(orderOutcomeLabel(filled, t, 'en-US')).toBe('1,234.50');
    expect(orderOutcomeLabel({ ...filled, currency: 'nope' }, t, 'en-US')).toBe('1,234.50');
  });
});

describe('orderOutcomeLabel staged ends', () => {
  const t = ((key: string) => key) as unknown as TFunction;
  const base = { vendor_order_id: 'o1', summary: 'Buy 1 ABC', filled_qty: null, qty: 1, avg_fill_price: null };

  // A lapse, a confirmation and a silent hand-off are three different ends,
  // and none of them is the failure every unnamed end used to read as.
  it.each([
    ['partial_expired', 'chat.wakeups.orderPartialExpired'],
    ['expired', 'chat.wakeups.orderExpired'],
    ['confirmed', 'chat.wakeups.orderConfirmed'],
    ['handed_off', 'chat.wakeups.orderHandedOff'],
  ] as const)('names %s in its own words', (outcome, key) => {
    expect(orderOutcomeLabel({ ...base, outcome }, t, 'en-US')).toBe(key);
  });
});

describe('triggerText zone', () => {
  // Echoes the arguments so the test reads which zone the line names.
  const t = ((key: string, o?: { time?: string; schedule?: string; zone?: string }) =>
    [key.split('.').pop(), o?.time, o?.zone].filter(Boolean).join(' ')) as unknown as TFunction;
  const now = Date.parse('2030-03-06T12:00:00Z');
  const fields = { kind: 'cron' as const, cron: '0 9 * * 1-5', at: null, price: null };

  it('reads an at time in the user zone, not the zone it was set in', () => {
    const at = { ...fields, kind: 'at' as const, cron: null, at: '2030-03-06T18:15:00Z', timezone: 'Asia/Tokyo' };
    expect(triggerText(at, t, 'en-US', now, 'America/New_York')).toBe('labelAt 1:15 PM');
  });

  it('names a repeat zone only when it is not the user zone, a retired name matching the current one', () => {
    expect(triggerText({ ...fields, timezone: 'Asia/Tokyo' }, t, 'en-US', now, 'America/New_York'))
      .toBe('labelRepeatZone Tokyo');
    expect(triggerText({ ...fields, timezone: 'America/Indianapolis' }, t, 'en-US', now, 'America/Indiana/Indianapolis'))
      .toBe('labelRepeat');
  });
});
