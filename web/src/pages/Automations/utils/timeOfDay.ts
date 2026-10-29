// Apart from ./moments, which carries the date library: ./cron reads MORNING
// and the chat's automation cards import ./cron, so whatever this module
// imports loads with the chat as well.

/** A wall-clock time with no day attached. */
export interface TimeOfDay {
  hour: number;
  minute: number;
}

export const SLOT_MINUTES = 15;

/** The time a new schedule, an empty day or an empty time list starts on. */
export const MORNING: TimeOfDay = { hour: 9, minute: 0 };

/** Every quarter hour of a day, the steps the time lists offer. */
export const DAY_SLOTS: TimeOfDay[] = Array.from({ length: (24 * 60) / SLOT_MINUTES }, (_, i) => ({
  hour: Math.floor((i * SLOT_MINUTES) / 60),
  minute: (i * SLOT_MINUTES) % 60,
}));

const MERIDIEM_PM = /(p\.?m?\.?|下午|晚上|中午)/i;
const MERIDIEM_AM = /(a\.?m?\.?|上午|早上|凌晨)/i;

/**
 * Reads a time the way people type one: "14:15", "2:15 pm", "2:15p", "215p",
 * "2p", "9.30", "0930", "下午2:15". Null when it is not a time, so the field
 * can put back what it had rather than guess.
 */
export function parseTime(text: string): TimeOfDay | null {
  const s = text.trim().toLowerCase();
  if (!s) return null;
  const pm = MERIDIEM_PM.test(s);
  const am = !pm && MERIDIEM_AM.test(s);
  const digits = s.replace(/[^\d:.点时分]/g, '');
  let hour: number;
  let minute = 0;
  const split = digits.split(/[:.点时]/).filter(Boolean);
  if (split.length >= 2) {
    hour = parseInt(split[0], 10);
    minute = parseInt(split[1], 10);
  } else {
    const run = digits.replace(/\D/g, '');
    if (!run || run.length > 4) return null;
    if (run.length <= 2) hour = parseInt(run, 10);
    else {
      hour = parseInt(run.slice(0, -2), 10);
      minute = parseInt(run.slice(-2), 10);
    }
  }
  if (Number.isNaN(hour) || Number.isNaN(minute) || minute > 59) return null;
  if (pm || am) {
    if (hour < 1 || hour > 12) return null;
    if (pm && hour < 12) hour += 12;
    if (am && hour === 12) hour = 0;
  }
  if (hour > 23) return null;
  return { hour, minute };
}

export const sameTime = (a: TimeOfDay | null, b: TimeOfDay | null) =>
  !!a && !!b && a.hour === b.hour && a.minute === b.minute;

/** The time `delta` quarter hours away, snapped onto the quarter-hour grid
 *  first, wrapping round midnight. */
export function stepTime(t: TimeOfDay, delta: number): TimeOfDay {
  const mins = t.hour * 60 + t.minute;
  const snapped = delta > 0 ? Math.floor(mins / SLOT_MINUTES) : Math.ceil(mins / SLOT_MINUTES);
  const next = (((snapped + delta) * SLOT_MINUTES) % 1440 + 1440) % 1440;
  return { hour: Math.floor(next / 60), minute: next % 60 };
}
