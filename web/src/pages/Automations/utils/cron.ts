import type { TFunction } from 'i18next';
import { MORNING } from './timeOfDay';
import { formatTimeOfDay, weekdayNames } from './time';

const EVERY_DAY = [true, true, true, true, true, true, true];
const NO_DAY = [false, false, false, false, false, false, false];
const MON_TO_FRI = [true, true, true, true, true, false, false];
const WEEKEND = [false, false, false, false, false, true, true];

const sameDays = (a: boolean[], b: boolean[]) => a.every((on, i) => on === b[i]);

/**
 * What a cron expression says, in the shapes this page can word, draw and
 * build. `days` is Monday first. Anything else is `custom`, kept as written;
 * `everyDay` says its day fields are all `*`, so it still runs every day.
 */
export type Schedule =
  | { kind: 'minutes'; interval: number }
  | { kind: 'hours'; interval: number }
  | { kind: 'hourly'; minute: number }
  | { kind: 'days'; hour: number; minute: number; days: boolean[] }
  | { kind: 'monthly'; hour: number; minute: number; dayOfMonth: number | 'L' }
  | { kind: 'custom'; raw: string; everyDay: boolean };

function whole(field: string, min: number, max: number): number | null {
  const n = Number(field);
  return /^\d+$/.test(field) && n >= min && n <= max ? n : null;
}

// A step that does not divide its field's cycle is valid cron but restarts
// every hour or day (`*/45` fires at :00 and :45, then :00 again), so only a
// divisor is a steady interval.
function step(field: string, cycle: number): number | null {
  const n = field.startsWith('*/') ? whole(field.slice(2), 1, cycle - 1) : null;
  return n !== null && cycle % n === 0 ? n : null;
}

/** The every-N-minutes intervals that run evenly, which the builder offers. */
export const MINUTE_INTERVALS = Array.from({ length: 30 }, (_, i) => i + 1).filter((n) => 60 % n === 0);

/** A day-of-week field of numbers and ranges as Monday-first flags, or null
 *  for any other syntax. */
function dayFlags(dayField: string): boolean[] | null {
  const on = [...NO_DAY];
  for (const item of dayField.split(',')) {
    const m = /^(\d)(?:-(\d))?$/.exec(item);
    if (!m) return null;
    const from = Number(m[1]);
    const to = m[2] != null ? Number(m[2]) : from;
    if (from > 7 || to > 7 || to < from) return null;
    // Cron counts from Sunday = 0 (and accepts 7 for Sunday too).
    for (let d = from; d <= to; d += 1) on[(d + 6) % 7] = true;
  }
  return on;
}

/** The one reading of an expression: the readout, the week drawn beside it
 *  and the builder's controls all start here, so they cannot disagree. */
export function parseSchedule(expression: string): Schedule {
  const raw = expression.trim();
  const fields = raw.split(/\s+/);
  const [min, hr, dom, mon, dow] = fields;
  const everyDay = fields.length === 5 && dom === '*' && mon === '*' && dow === '*';
  const custom: Schedule = { kind: 'custom', raw, everyDay };
  if (fields.length !== 5 || mon !== '*') return custom;

  const minute = whole(min, 0, 59);
  if (everyDay) {
    const everyMinutes = step(min, 60);
    if (everyMinutes !== null && hr === '*') return { kind: 'minutes', interval: everyMinutes };
    const everyHours = step(hr, 24);
    if (minute === 0 && everyHours !== null) return { kind: 'hours', interval: everyHours };
    if (minute !== null && hr === '*') return { kind: 'hourly', minute };
  }

  const hour = whole(hr, 0, 23);
  if (minute === null || hour === null) return custom;
  if (dom === '*') {
    const days = dow === '*' ? [...EVERY_DAY] : dayFlags(dow);
    return days ? { kind: 'days', hour, minute, days } : custom;
  }
  const dayOfMonth = dom === 'L' ? 'L' : whole(dom, 1, 31);
  return dayOfMonth !== null && dow === '*' ? { kind: 'monthly', hour, minute, dayOfMonth } : custom;
}

function dayList(days: boolean[], t: TFunction, locale: string): string {
  if (sameDays(days, MON_TO_FRI)) return t('automation.cron.weekdays');
  if (sameDays(days, WEEKEND)) return t('automation.cron.weekends');
  return weekdayNames(locale)
    .filter((_, i) => days[i])
    .join(t('automation.cron.daySeparator'));
}

/** A schedule in words, in the reader's language and clock. */
export function describeSchedule(s: Schedule, t: TFunction, locale: string): string {
  switch (s.kind) {
    case 'minutes':
      return s.interval === 1
        ? t('automation.cron.everyMinute')
        : t('automation.cron.everyMinutes', { n: s.interval });
    case 'hours':
      return s.interval === 1
        ? t('automation.cron.everyHour')
        : t('automation.cron.everyHours', { n: s.interval });
    case 'hourly':
      return s.minute === 0
        ? t('automation.cron.everyHour')
        : t('automation.cron.hourlyAt', { minute: String(s.minute).padStart(2, '0'), m: s.minute });
    case 'days': {
      const time = formatTimeOfDay(s.hour, s.minute, locale);
      return s.days.every(Boolean)
        ? t('automation.cron.daily', { time })
        : t('automation.cron.weekly', { time, days: dayList(s.days, t, locale) });
    }
    // The same "At time, when" shape as a weekly schedule, short enough for a list line.
    case 'monthly': {
      const time = formatTimeOfDay(s.hour, s.minute, locale);
      return s.dayOfMonth === 'L'
        ? t('automation.cron.monthEnd', { time })
        : t('automation.cron.monthly', { time, count: s.dayOfMonth, ordinal: true });
    }
    case 'custom':
      return s.raw;
  }
}

export function cronToHuman(expression: string, t: TFunction, locale: string): string {
  return describeSchedule(parseSchedule(expression), t, locale);
}

/** The days to light for a schedule: a sub-daily one runs every day, and a
 *  day-of-month one has no weekday, so none is lit. */
export function scheduleDays(s: Schedule): boolean[] {
  switch (s.kind) {
    case 'days':
      return s.days;
    case 'monthly':
      return NO_DAY;
    case 'custom':
      return s.everyDay ? EVERY_DAY : NO_DAY;
    default:
      return EVERY_DAY;
  }
}

// ── The schedule builder's controls ─────────────────────────────────────

/** The kinds the builder has controls for. Every few hours has none, so it
 *  opens as custom. */
export type Frequency = 'minutes' | 'hourly' | 'days' | 'monthly' | 'custom';

/** Every control's value, not only the chosen kind's, so switching kind and
 *  back keeps what was set. */
export interface ScheduleDraft {
  kind: Frequency;
  interval: number;
  minute: number;
  hour: number;
  /** Monday first. Every day, weekdays and any hand-picked set are all one
   *  shape, so the picker covers what the daily/weekdays/weekly modes did. */
  days: boolean[];
  /** 1-31, or 'L' for the month's last day, whatever its date. */
  dayOfMonth: number | 'L';
  raw: string;
}

const DRAFT_DEFAULTS: ScheduleDraft = {
  kind: 'days',
  interval: 30,
  ...MORNING,
  days: EVERY_DAY,
  dayOfMonth: 1,
  raw: '',
};

/** An expression as the builder's controls; one they cannot show is
 *  `custom`, kept verbatim in `raw`. */
export function scheduleDraft(expression: string): ScheduleDraft {
  const raw = expression.trim();
  if (!raw) return { ...DRAFT_DEFAULTS };
  const s = parseSchedule(raw);
  if (s.kind === 'hours' || s.kind === 'custom') return { ...DRAFT_DEFAULTS, kind: 'custom', raw };
  return { ...DRAFT_DEFAULTS, ...s, raw };
}

/** Monday-first flags as a day-of-week field: `*` for every day, `1-5` for
 *  the working week, otherwise the days listed Monday first (Sunday is 0). */
function dowField(days: boolean[]): string {
  if (days.every(Boolean)) return '*';
  if (sameDays(days, MON_TO_FRI)) return '1-5';
  return days.flatMap((on, i) => (on ? [String((i + 1) % 7)] : [])).join(',');
}

export function buildCron(s: ScheduleDraft): string {
  switch (s.kind) {
    case 'minutes': return `*/${s.interval} * * * *`;
    case 'hourly': return `${s.minute} * * * *`;
    case 'days': return `${s.minute} ${s.hour} * * ${dowField(s.days)}`;
    case 'monthly': return `${s.minute} ${s.hour} ${s.dayOfMonth} * *`;
    case 'custom': return s.raw;
  }
}

/** The schedule a new automation starts on: every day, in the morning. */
export const DEFAULT_CRON = buildCron(DRAFT_DEFAULTS);
