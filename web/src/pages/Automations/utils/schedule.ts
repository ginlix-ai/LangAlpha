import type { TFunction } from 'i18next';
import { formatTimezoneName, relativeTime } from '@/lib/format';
import { currentTimezoneName } from '@/lib/deviceTimezone';
import type { Automation } from '@/types/automation';
import { cronToHuman } from './cron';
import { formatPriceTrigger, formatRetriggerMode } from './price';
import { formatDateTimeShort, formatUpcoming } from './time';

/** When a one-time automation runs, or ran: its pending slot while it has
 *  one, otherwise the slot its last execution was scheduled for. */
function onceAt(a: Automation): string | null {
  return a.next_run_at ?? a.last_execution?.scheduled_at ?? null;
}

/** A zone's name for a compact line, or null when it is the reader's own:
 *  there every line would repeat it, and the line set elsewhere is the one
 *  worth reading. Sentences and the form always name the zone. */
export function awayZoneName(tz: string | null | undefined, homeZone: string, locale: string): string | null {
  return tz && currentTimezoneName(tz) !== homeZone ? formatTimezoneName(tz, locale) : null;
}

/** The compact line under a name in a list: what makes this automation run. */
export function scheduleLine(a: Automation, t: TFunction, homeZone: string, locale: string): string {
  if (a.trigger_type === 'price') return formatPriceTrigger(a.trigger_config, t);
  let line: string;
  if (a.trigger_type === 'once') {
    const at = onceAt(a);
    if (!at) return t('automation.once');
    line = t('automation.onceAt', { when: formatDateTimeShort(at, locale, a.timezone) });
  } else {
    line = cronToHuman(a.cron_expression ?? '', t, locale);
  }
  return [line, awayZoneName(a.timezone, homeZone, locale)].filter(Boolean).join(' · ');
}

/** The inspector's sentence: the schedule, then the next occurrence. */
export function scheduleSentence(a: Automation, t: TFunction, locale: string, now: number): string {
  if (a.trigger_type === 'price') {
    return t('automation.sentenceWatching', {
      condition: formatPriceTrigger(a.trigger_config, t),
      retrigger: formatRetriggerMode(a.trigger_config, t),
    });
  }
  if (a.trigger_type === 'once') {
    const at = onceAt(a);
    if (!at) return t('automation.once');
    const key = a.status === 'completed' || !a.next_run_at ? 'automation.sentenceRanOnce' : 'automation.sentenceOnce';
    return t(key, { when: formatDateTimeShort(at, locale, a.timezone), zone: formatTimezoneName(a.timezone, locale) });
  }
  const schedule = t('automation.sentenceRecurring', {
    schedule: cronToHuman(a.cron_expression ?? '', t, locale),
    zone: formatTimezoneName(a.timezone, locale),
  });
  // Only a live schedule has a next run to name: a row switched off under an
  // earlier build can still hold a slot that will never fire.
  if (!a.next_run_at || a.status === 'paused' || a.status === 'disabled') return schedule;
  return t('automation.sentenceThen', {
    first: schedule,
    then: t('automation.sentenceNextRun', {
      when: formatUpcoming(a.next_run_at, locale, now, a.timezone),
      relative: relativeTime(a.next_run_at, locale, now),
    }),
  });
}
