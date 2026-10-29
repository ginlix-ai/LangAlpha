import React from 'react';
import { useTranslation } from 'react-i18next';
import { useHomeTimezone } from '@/hooks/useHomeTimezone';
import { useLocale } from '@/hooks/useLocale';
import { cn } from '@/lib/utils';
import { describeSchedule, parseSchedule, scheduleDays } from '../utils/cron';
import { awayZoneName } from '../utils/schedule';
import type { AutomationTemplate } from '../utils/templates';
import { formatTimeOfDay, weekdayInitials } from '../utils/time';
// MeterMark and OnceMark are drawn on the price meter's track.
import './PriceMeter.css';
import './CadenceMarks.css';

/*
 * The small marks that say how often something runs, one per kind of
 * trigger, all drawn at the same width so a column of them lines up: the days
 * of the week a schedule lands on, the meter a price watch shows, one point
 * for a one-time run, and an unset track for nothing chosen yet.
 */

/** Monday-first flags; a day that is off is drawn faded, not left out. */
export function WeekStrip({ days }: { days: boolean[] }) {
  const locale = useLocale();
  const initials = weekdayInitials(locale);
  return (
    <span className="automation-mono automations-week" aria-hidden="true">
      {initials.map((initial, i) => (
        <span key={i} className={cn(days[i] && 'is-on')}>
          {initial}
        </span>
      ))}
    </span>
  );
}

export function MeterMark() {
  return (
    <span className="automation-meter-track automations-cadence-track" aria-hidden="true">
      <span className="automation-meter-gap" style={{ left: '30%', width: '42%' }} />
      <span className="automation-meter-threshold" style={{ left: '72%' }} />
      <span className="automation-meter-dot" style={{ left: '30%' }} />
    </span>
  );
}

export function OnceMark() {
  return (
    <span className="automation-meter-track automations-cadence-track" aria-hidden="true">
      <span className="automation-meter-dot" style={{ left: '62%' }} />
    </span>
  );
}

export function UnsetMark() {
  return <span className="automations-cadence-track automations-cadence-unset" aria-hidden="true" />;
}

/** A template's cadence: its mark, with the time and place (or the kind of
 *  trigger) written underneath. */
export function Cadence({ template: tpl }: { template: AutomationTemplate }) {
  const { t } = useTranslation();
  const locale = useLocale();
  const homeZone = useHomeTimezone();
  const d = tpl.defaults;

  if (d.trigger_type === 'cron' && d.cron_expression) {
    const schedule = parseSchedule(d.cron_expression);
    const zone = awayZoneName(d.timezone, homeZone, locale);
    return (
      <>
        <span className="sr-only">{[describeSchedule(schedule, t, locale), zone].filter(Boolean).join(' · ')}</span>
        <WeekStrip days={scheduleDays(schedule)} />
        <span className="automation-mono automations-cadence-sub" aria-hidden="true">
          {['hour' in schedule && formatTimeOfDay(schedule.hour, schedule.minute, locale), zone].filter(Boolean).join(' · ')}
        </span>
      </>
    );
  }

  if (d.trigger_type === 'price') {
    return (
      <>
        <MeterMark />
        <span className="automation-mono automations-cadence-sub">{t('automation.cadencePrice')}</span>
      </>
    );
  }

  if (d.trigger_type === 'once') {
    return (
      <>
        <OnceMark />
        <span className="automation-mono automations-cadence-sub">{t('automation.cadenceOnce')}</span>
      </>
    );
  }

  return (
    <>
      <UnsetMark />
      <span className="automation-mono automations-cadence-sub">{t('automation.cadenceBlank')}</span>
    </>
  );
}
