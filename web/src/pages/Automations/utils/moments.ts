import {
  type CalendarDate,
  type Disambiguation,
  fromAbsolute,
  isWeekend,
  Time,
  toCalendarDate,
  toCalendarDateTime,
  toZoned,
} from '@internationalized/date';
import { US_MARKET_TZ } from '@/lib/bars/exchanges';
import { MORNING, SLOT_MINUTES, type TimeOfDay } from './timeOfDay';

/** The first quarter hour at least `leadMinutes` from now. */
export function nextSlot(now: Date, leadMinutes = SLOT_MINUTES): Date {
  const step = SLOT_MINUTES * 60_000;
  return new Date(Math.ceil((now.getTime() + leadMinutes * 60_000) / step) * step);
}

/** The calendar day a clock in `tz` shows at `at`. */
export const dayIn = (at: Date, tz: string): CalendarDate => toCalendarDate(fromAbsolute(at.getTime(), tz));

export function timeIn(at: Date, tz: string): TimeOfDay {
  const { hour, minute } = fromAbsolute(at.getTime(), tz);
  return { hour, minute };
}

/**
 * The instant a clock in `tz` reads `t` on `day`. The library's default
 * ('compatible') resolves a wall time the way the server's zoneinfo does
 * (fold=0): a time the clock reads twice, as it falls back, is its first
 * occurrence, and a time it skips, as it springs forward (in some zones at
 * midnight), lands as far past the jump as it was meant to be past the
 * skipped hour: 2:30 on a New York spring-forward day is 3:30.
 */
export const instantAt = (day: CalendarDate, t: TimeOfDay, tz: string, disambiguation?: Disambiguation): Date =>
  toZoned(toCalendarDateTime(day, new Time(t.hour, t.minute)), tz, disambiguation).toDate();

/** The first instant after `now` at which a clock in `tz` reads `t` on
 *  `day`, or undefined once it has gone. As the clock falls back, a repeated
 *  time whose first pass is gone still comes round a second time that day. */
export const nextOnDay = (day: CalendarDate, t: TimeOfDay, tz: string, now: Date): Date | undefined =>
  [instantAt(day, t, tz), instantAt(day, t, tz, 'later')].find((at) => at > now);

/** The same wall-clock reading in another zone: 2:15 PM in London becomes
 *  2:15 PM in Tokyo, the way a calendar treats an event's changed zone. */
export function rezone(at: Date, from: string, to: string): Date {
  return instantAt(dayIn(at, from), timeIn(at, from), to);
}

/** The next weekday on which the US market's clock reaches `t` after `now`.
 *  Exchange holidays are not known here; the pick shows its date, so a
 *  closed day is visible before it is chosen. */
export function nextMarketDayAt(now: Date, t: TimeOfDay, tz = US_MARKET_TZ): Date {
  for (let day = dayIn(now, tz); ; day = day.add({ days: 1 })) {
    const at = instantAt(day, t, tz);
    // The US market's weekend, not the reader's: `isWeekend` reads it off
    // the locale's region, and in some regions it starts on Friday.
    if (at > now && !isWeekend(day, 'en-US')) return at;
  }
}

export type QuickPickId = 'inAnHour' | 'tomorrowMorning' | 'beforeOpen' | 'afterClose';

/** Half an hour before the 9:30 open, and a quarter hour after the 4:00
 *  close, once closing prints have settled. */
const BEFORE_OPEN: TimeOfDay = { hour: 9, minute: 0 };
const AFTER_CLOSE: TimeOfDay = { hour: 16, minute: 15 };

/** The one-click moments a one-time run is most often set for. A generic
 *  pick that lands on the same instant as a market one (a reader in New York
 *  asking for tomorrow at nine) gives way to it. */
export function quickPicks(now: Date, tz: string): { id: QuickPickId; at: Date }[] {
  const picks: { id: QuickPickId; at: Date }[] = [
    { id: 'inAnHour', at: nextSlot(now, 60) },
    { id: 'tomorrowMorning', at: instantAt(dayIn(now, tz).add({ days: 1 }), MORNING, tz) },
    { id: 'beforeOpen', at: nextMarketDayAt(now, BEFORE_OPEN) },
    { id: 'afterClose', at: nextMarketDayAt(now, AFTER_CLOSE) },
  ];
  return picks.filter(
    (p, i) => !picks.some((q, j) => j > i && q.at.getTime() === p.at.getTime()),
  );
}
