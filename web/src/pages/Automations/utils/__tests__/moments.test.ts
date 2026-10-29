// @vitest-environment node
import { describe, it, expect } from 'vitest';
import { CalendarDate } from '@internationalized/date';
import { instantAt, nextMarketDayAt } from '../moments';

describe('market moments', () => {
  it('resolves New York wall time on both sides of a DST change', () => {
    expect(instantAt(new CalendarDate(2026, 10, 30), { hour: 9, minute: 0 }, 'America/New_York').toISOString()).toBe('2026-10-30T13:00:00.000Z');
    expect(instantAt(new CalendarDate(2026, 11, 2), { hour: 9, minute: 0 }, 'America/New_York').toISOString()).toBe('2026-11-02T14:00:00.000Z');
  });

  it('skips the weekend to the next weekday', () => {
    // Friday 2026-09-25, 17:00 New York: the next 9:00 there is Monday's.
    const fridayEvening = new Date('2026-09-25T21:00:00Z');
    expect(nextMarketDayAt(fridayEvening, { hour: 9, minute: 0 }).toISOString()).toBe('2026-09-28T13:00:00.000Z');
  });
});

describe('instantAt across a clock change', () => {
  // Resolved as the server's zoneinfo does (fold=0): a time the clock skips
  // lands after the jump, a time it passes twice takes the first pass.
  it.each([
    ['New York spring gap', 'America/New_York', 2027, 3, 14, 2, '2027-03-14T07:30:00.000Z'],
    ['New York fall overlap', 'America/New_York', 2027, 11, 7, 1, '2027-11-07T05:30:00.000Z'],
    ['Sydney spring gap', 'Australia/Sydney', 2026, 10, 4, 2, '2026-10-03T16:30:00.000Z'],
    ['Sydney autumn overlap', 'Australia/Sydney', 2027, 4, 4, 2, '2027-04-03T15:30:00.000Z'],
    ['Santiago midnight gap', 'America/Santiago', 2026, 9, 6, 0, '2026-09-06T04:30:00.000Z'],
    ['Santiago overlap before midnight', 'America/Santiago', 2027, 4, 3, 23, '2027-04-04T02:30:00.000Z'],
    ['Havana midnight gap', 'America/Havana', 2027, 3, 14, 0, '2027-03-14T05:30:00.000Z'],
    ['Havana midnight overlap', 'America/Havana', 2026, 11, 1, 0, '2026-11-01T04:30:00.000Z'],
  ])('%s', (_name, tz, year, month, day, hour, expected) => {
    expect(instantAt(new CalendarDate(year, month, day), { hour, minute: 30 }, tz).toISOString()).toBe(expected);
  });
});
