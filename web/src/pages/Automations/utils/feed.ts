import type { AutomationRun } from '@/types/automation';
import { localDayKey } from './time';

/** The moment a feed entry shows: when the run started, or when it was due
 *  while it has not. */
export function runTime(run: AutomationRun): string {
  return run.started_at ?? run.scheduled_at ?? run.created_at;
}

export interface FeedDay {
  key: string;
  date: Date;
  runs: AutomationRun[];
}

const at = (run: AutomationRun) => Date.parse(runTime(run));
const midnight = (d: Date) => new Date(d.getFullYear(), d.getMonth(), d.getDate()).getTime();

/**
 * The runs under the local day of the time each one shows, newest day first
 * and newest run first within it. The server orders by creation, and a run
 * that waited can start well after runs created later, so a day gathers its
 * runs wherever they sit in the list; grouping only neighbours would print
 * the same day twice.
 */
export function groupRunsByDay(runs: AutomationRun[]): FeedDay[] {
  const days = new Map<string, FeedDay>();
  for (const run of runs) {
    const key = localDayKey(runTime(run));
    const day = days.get(key);
    if (day) day.runs.push(run);
    else days.set(key, { key, date: new Date(runTime(run)), runs: [run] });
  }
  const out = [...days.values()].sort((a, b) => midnight(b.date) - midnight(a.date));
  for (const day of out) day.runs.sort((a, b) => at(b) - at(a));
  return out;
}

/** Today's and yesterday's keys by the calendar, not by subtracting a day's
 *  worth of milliseconds, which misses by an hour across a DST change. */
export function nearDayKeys(now: Date): { today: string; yesterday: string } {
  return {
    today: localDayKey(now),
    yesterday: localDayKey(new Date(now.getFullYear(), now.getMonth(), now.getDate() - 1)),
  };
}
