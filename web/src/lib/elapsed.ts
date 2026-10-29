/**
 * Elapsed-time copy: the chat's turn and thought headers, how long an
 * automation run took, and how long a subagent or an analyst's run took, so
 * each reads a duration in the same words.
 *
 * Import-free on purpose, like `liveZoneTiming`: the header's duration is the
 * kind of thing a Node-side spec wants to assert, and the render-block module
 * graph (i18n JSON, framer) cannot load there. The translator arrives as an
 * argument for the same reason, the way `activitySummary` takes it.
 */

/** Translation function signature compatible with i18next's t(). */
export type TFn = (key: string, opts?: Record<string, unknown>) => string;

/**
 * Each scale is one key rather than an interpolated unit, because a language
 * decides both the unit and how it sits beside the number: `12m 58s` is
 * `12 分 58 秒`, and an hour is 小时 rather than a single letter.
 */
const SCALE_KEYS = {
  subSecond: 'chat.duration.subSecond',
  seconds: 'chat.duration.seconds',
  minutes: 'chat.duration.minutesSeconds',
  hours: 'chat.duration.hoursMinutes',
  wholeMinutes: 'chat.duration.minutes',
  wholeHours: 'chat.duration.hours',
} as const;

/**
 * `4.2s`, `47s`, `12m 58s`, `1h 3m` — one step of precision at every scale, so
 * the row stops changing width a few seconds into a turn and the reader's eye
 * is not pulled back to a ticking number for the rest of a long run.
 */
export function formatWorkedFor(ms: number, t: TFn): string {
  const elapsed = Number.isFinite(ms) && ms > 0 ? ms : 0;
  // Under ten seconds the tenth is the whole signal; truncate rather than
  // round so a timer never reads a moment that has not happened yet.
  if (elapsed < 10_000) {
    return t(SCALE_KEYS.seconds, { value: (Math.floor(elapsed / 100) / 10).toFixed(1) });
  }
  const totalSeconds = Math.floor(elapsed / 1000);
  if (totalSeconds < 60) return t(SCALE_KEYS.seconds, { value: totalSeconds });
  const totalMinutes = Math.floor(totalSeconds / 60);
  if (totalMinutes < 60) {
    return t(SCALE_KEYS.minutes, { minutes: totalMinutes, seconds: totalSeconds % 60 });
  }
  return t(SCALE_KEYS.hours, { hours: Math.floor(totalMinutes / 60), minutes: totalMinutes % 60 });
}

/**
 * `4s`, `47s`, `12m 58s`, `1h 3m` for how long the model thought. Whole
 * seconds from the first tick: the header ticks once a second while a thought
 * runs, and tenths there would read as jitter.
 */
export function formatThoughtFor(ms: number, t: TFn): string {
  const elapsed = Number.isFinite(ms) && ms > 0 ? ms : 0;
  const totalSeconds = Math.floor(elapsed / 1000);
  if (totalSeconds < 1) return t(SCALE_KEYS.subSecond);
  if (totalSeconds < 60) return t(SCALE_KEYS.seconds, { value: totalSeconds });
  return formatWorkedFor(elapsed, t);
}

/**
 * How long something that has finished took: `formatThoughtFor`, without a
 * zero tail (`15m`, not `15m 0s`). The live headers keep the tail so their
 * width holds while they tick; a settled figure never ticks.
 */
export function formatTook(ms: number, t: TFn): string {
  const elapsed = Number.isFinite(ms) && ms > 0 ? ms : 0;
  const totalMinutes = Math.floor(elapsed / 60_000);
  if (totalMinutes >= 60 && totalMinutes % 60 === 0) {
    return t(SCALE_KEYS.wholeHours, { hours: totalMinutes / 60 });
  }
  if (totalMinutes >= 1 && totalMinutes < 60 && Math.floor(elapsed / 1000) % 60 === 0) {
    return t(SCALE_KEYS.wholeMinutes, { minutes: totalMinutes });
  }
  return formatThoughtFor(elapsed, t);
}

/**
 * How long a finished run took, to the minute once it ran a minute: a report
 * is read as "18m", and the seconds past it are noise beside a run that long.
 */
export function formatRunLength(ms: number, t: TFn): string {
  return formatTook(ms >= 60_000 ? Math.floor(ms / 60_000) * 60_000 : ms, t);
}

/** A wire timestamp as epoch ms; null when absent or unparseable. */
export function epochMs(iso: string | null | undefined): number | null {
  if (!iso) return null;
  const ms = Date.parse(iso);
  return Number.isFinite(ms) ? ms : null;
}

/** A settled run's length, when the ledger gave both ends of it. */
export function runLengthMs(startedAt: number | undefined, finishedAt: number | undefined): number | undefined {
  if (startedAt === undefined || finishedAt === undefined || finishedAt < startedAt) return undefined;
  return finishedAt - startedAt;
}

/**
 * `0:04`, `3:12`, `1:02:09`: elapsed time as a clock, for a column of runs
 * read side by side. Digits and colons only, so it needs no translation, and
 * at a fixed width in tabular figures the column stays aligned as it ticks.
 * Truncated like `formatWorkedFor`, never rounded up.
 */
export function formatClock(ms: number): string {
  const totalSeconds = Math.floor(Number.isFinite(ms) && ms > 0 ? ms / 1000 : 0);
  const seconds = String(totalSeconds % 60).padStart(2, '0');
  const totalMinutes = Math.floor(totalSeconds / 60);
  if (totalMinutes < 60) return `${totalMinutes}:${seconds}`;
  return `${Math.floor(totalMinutes / 60)}:${String(totalMinutes % 60).padStart(2, '0')}:${seconds}`;
}
