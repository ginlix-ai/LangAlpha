import React, { useId, useState, useEffect } from 'react';
import { useTranslation } from 'react-i18next';
import { Input } from '@/components/ui/input';
import { useLocale } from '@/hooks/useLocale';
import {
  Select,
  SelectItem,
  SelectListBox,
  SelectPopover,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/aria-select';
import { buildCron, MINUTE_INTERVALS, scheduleDraft, type Frequency, type ScheduleDraft } from '../utils/cron';
import { weekdayInitials } from '../utils/time';
import OptionSelect, { type Option } from './OptionSelect';
import TimeField from './TimeField';

const FREQUENCIES: Option<Frequency>[] = [
  { value: 'days', labelKey: 'automation.freqDays' },
  { value: 'minutes', labelKey: 'automation.freqMinutes' },
  { value: 'hourly', labelKey: 'automation.freqHourly' },
  { value: 'monthly', labelKey: 'automation.freqMonthly' },
  { value: 'custom', labelKey: 'automation.freqCustom' },
];

const MINUTE_STEPS = [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55];
const MONTH_DAYS = Array.from({ length: 31 }, (_, i) => i + 1);

// ── Day picker ────────────────────────────────────────────

const DAY_NAME_KEYS = [
  'automation.dayMon',
  'automation.dayTue',
  'automation.dayWed',
  'automation.dayThu',
  'automation.dayFri',
  'automation.daySat',
  'automation.daySun',
];

/** Any set of weekdays, one press each. The last day left on stays on: a
 *  schedule with no days would never run. */
function DayPicker({ days, onChange }: { days: boolean[]; onChange: (days: boolean[]) => void }) {
  const { t } = useTranslation();
  const locale = useLocale();
  const initials = weekdayInitials(locale);
  const count = days.filter(Boolean).length;
  return (
    <div className="automation-day-picker" role="group" aria-label={t('automation.freqDays')}>
      {initials.map((initial, i) => (
        <button
          key={i}
          type="button"
          className="automation-mono automation-day"
          aria-pressed={days[i]}
          aria-label={t(DAY_NAME_KEYS[i])}
          onClick={() => {
            if (days[i] && count === 1) return;
            onChange(days.map((on, j) => (j === i ? !on : on)));
          }}
        >
          {initial}
        </button>
      ))}
    </div>
  );
}

// ── Component ─────────────────────────────────────────────

interface CronScheduleBuilderProps {
  value: string;
  onChange: (cron: string) => void;
  /** The zone control, set beside the time. Only a schedule every few
   *  minutes reads the same in any zone; an hourly minute does not, since
   *  some zones sit a half or quarter hour off the rest. */
  zone?: React.ReactNode;
  /** The row label, which names the frequency choice. */
  labelledBy?: string;
}

export default function CronScheduleBuilder({ value, onChange, zone, labelledBy }: CronScheduleBuilderProps) {
  const { t } = useTranslation();
  const helpId = useId();
  // The form mounts the builder afresh for each automation it opens, and
  // nothing else writes the schedule, so the value is read only here.
  const [state, setState] = useState<ScheduleDraft>(() => scheduleDraft(value));

  // An empty schedule opens on the default, so the form holds what it shows.
  useEffect(() => {
    if (!value.trim()) onChange(buildCron(state));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const update = (patch: Partial<ScheduleDraft>) => {
    const next = { ...state, ...patch };
    setState(next);
    onChange(buildCron(next));
  };

  // Custom starts from the schedule as built so far, not from the expression
  // the form opened with, which would undo every change made since.
  const changeKind = (kind: Frequency) =>
    update(kind === 'custom' && state.kind !== 'custom' ? { kind, raw: buildCron(state) } : { kind });

  // A schedule written elsewhere may run off the five-minute grid; offer its
  // minute too, so the control shows what the schedule says.
  const minuteSteps = MINUTE_STEPS.includes(state.minute)
    ? MINUTE_STEPS
    : [...MINUTE_STEPS, state.minute].sort((a, b) => a - b);
  const needsTime = state.kind === 'days' || state.kind === 'monthly';
  const needsZone = state.kind !== 'minutes';

  return (
    <div className="flex flex-col gap-2">
      <div className="flex flex-wrap items-center gap-x-3 gap-y-2">
        <OptionSelect
          aria-labelledby={labelledBy}
          value={state.kind}
          options={FREQUENCIES}
          onChange={changeKind}
          className="min-w-40"
        />

        {/* Every N minutes */}
        {state.kind === 'minutes' && (
          <div className="flex items-center gap-2">
            <span className="automation-form-joiner">
              {t('automation.every')}
            </span>
            <Select
              aria-label={t('automation.intervalMinutes')}
              selectedKey={state.interval}
              onSelectionChange={(k) => {
                if (typeof k === 'number') update({ interval: k });
              }}
              className="w-20"
            >
              <SelectTrigger><SelectValue /></SelectTrigger>
              <SelectPopover>
                <SelectListBox>
                  {MINUTE_INTERVALS.map((n) => (
                    <SelectItem key={n} id={n}>{String(n)}</SelectItem>
                  ))}
                </SelectListBox>
              </SelectPopover>
            </Select>
            <span className="automation-form-joiner">
              {t('automation.minutes')}
            </span>
          </div>
        )}

        {/* Hourly at minute */}
        {state.kind === 'hourly' && (
          <div className="flex items-center gap-2">
            <span className="automation-form-joiner">
              {t('automation.atMinute')}
            </span>
            <Select
              aria-label={t('automation.atMinute')}
              selectedKey={state.minute}
              onSelectionChange={(k) => {
                if (typeof k === 'number') update({ minute: k });
              }}
              className="w-24"
            >
              <SelectTrigger><SelectValue /></SelectTrigger>
              <SelectPopover>
                <SelectListBox>
                  {minuteSteps.map((m) => (
                    <SelectItem key={m} id={m}>{`:${String(m).padStart(2, '0')}`}</SelectItem>
                  ))}
                </SelectListBox>
              </SelectPopover>
            </Select>
          </div>
        )}

        {/* Day of month */}
        {state.kind === 'monthly' && (
          <div className="flex items-center gap-2">
            <span className="automation-form-joiner">
              {t('automation.onDay')}
            </span>
            <Select
              aria-label={t('automation.onDay')}
              selectedKey={state.dayOfMonth}
              onSelectionChange={(k) => {
                if (k === 'L' || typeof k === 'number') update({ dayOfMonth: k });
              }}
              className="min-w-24"
            >
              <SelectTrigger><SelectValue /></SelectTrigger>
              <SelectPopover>
                <SelectListBox>
                  {/* A day of the month is a phrase, not a bare number: Chinese writes "1 日". */}
                  {MONTH_DAYS.map((d) => (
                    <SelectItem key={d} id={d}>{t('automation.monthDay', { day: d })}</SelectItem>
                  ))}
                  <SelectItem id="L">{t('automation.lastDay')}</SelectItem>
                </SelectListBox>
              </SelectPopover>
            </Select>
          </div>
        )}

        {/* Time */}
        {needsTime && (
          <div className="flex items-center gap-2">
            <span className="automation-form-joiner">
              {t('automation.atTime')}
            </span>
            <TimeField
              value={{ hour: state.hour, minute: state.minute }}
              onChange={(time) => update(time)}
              aria-label={t('automation.time')}
              className="w-32"
            />
          </div>
        )}

        {zone && needsZone && (
          <div className="flex items-center gap-2">
            <span className="automation-form-joiner">{t('automation.inZone')}</span>
            {zone}
          </div>
        )}
      </div>

      {state.kind === 'days' && <DayPicker days={state.days} onChange={(days) => update({ days })} />}

      {/* Cron has no "or the last day if shorter": a 31st skips the months
          that lack one, so say so and point at the option that does not. */}
      {state.kind === 'monthly' && typeof state.dayOfMonth === 'number' && state.dayOfMonth > 28 && (
        <span className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
          {t('automation.skipsShortMonths', { day: state.dayOfMonth })}
        </span>
      )}

      {/* Custom cron fallback */}
      {state.kind === 'custom' && (
        <div className="flex flex-col gap-1.5">
          <Input
            value={state.raw}
            onChange={(e) => update({ raw: e.target.value })}
            placeholder="*/30 * * * *"
            required
            aria-label={t('automation.freqCustom')}
            aria-describedby={helpId}
            className="font-mono"
          />
          <span id={helpId} className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
            {t('automation.cronHelp')}
          </span>
        </div>
      )}
    </div>
  );
}
