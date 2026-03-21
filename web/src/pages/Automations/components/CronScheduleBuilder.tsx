import React, { useState, useEffect, useRef } from 'react';
import { useTranslation } from 'react-i18next';
import { Input } from '@/components/ui/input';
import { Select } from '@/components/ui/select';

// ── Types ─────────────────────────────────────────────────

type Frequency = 'minutes' | 'hourly' | 'daily' | 'weekdays' | 'weekly' | 'monthly' | 'custom';

interface ScheduleState {
  frequency: Frequency;
  interval: number;
  minute: number;
  hour: number;
  dayOfWeek: number;
  dayOfMonth: number;
  raw: string;
}

// ── Parse / Build ─────────────────────────────────────────

const DEFAULTS: ScheduleState = {
  frequency: 'daily',
  interval: 30,
  minute: 0,
  hour: 9,
  dayOfWeek: 1,
  dayOfMonth: 1,
  raw: '',
};

export function parseCron(expr: string): ScheduleState {
  const raw = expr.trim();
  if (!raw) return { ...DEFAULTS };

  const parts = raw.split(/\s+/);
  if (parts.length !== 5) return { ...DEFAULTS, frequency: 'custom', raw };

  const [min, hr, dom, mon, dow] = parts;

  // */N * * * *
  if (min.startsWith('*/') && hr === '*' && dom === '*' && mon === '*' && dow === '*') {
    const n = parseInt(min.slice(2), 10);
    if (n > 0 && n <= 59) return { ...DEFAULTS, frequency: 'minutes', interval: n, raw };
  }

  // M * * * *
  if (/^\d+$/.test(min) && hr === '*' && dom === '*' && mon === '*' && dow === '*') {
    return { ...DEFAULTS, frequency: 'hourly', minute: parseInt(min, 10), raw };
  }

  if (!/^\d+$/.test(min) || !/^\d+$/.test(hr)) return { ...DEFAULTS, frequency: 'custom', raw };

  const m = parseInt(min, 10);
  const h = parseInt(hr, 10);

  if (dom === '*' && mon === '*' && dow === '1-5') return { ...DEFAULTS, frequency: 'weekdays', minute: m, hour: h, raw };
  if (dom === '*' && mon === '*' && /^[0-6]$/.test(dow)) return { ...DEFAULTS, frequency: 'weekly', minute: m, hour: h, dayOfWeek: parseInt(dow, 10), raw };
  if (dom === '*' && mon === '*' && dow === '*') return { ...DEFAULTS, frequency: 'daily', minute: m, hour: h, raw };
  if (/^\d+$/.test(dom) && mon === '*' && dow === '*') return { ...DEFAULTS, frequency: 'monthly', minute: m, hour: h, dayOfMonth: parseInt(dom, 10), raw };

  return { ...DEFAULTS, frequency: 'custom', raw };
}

export function buildCron(s: ScheduleState): string {
  switch (s.frequency) {
    case 'minutes': return `*/${s.interval} * * * *`;
    case 'hourly': return `${s.minute} * * * *`;
    case 'daily': return `${s.minute} ${s.hour} * * *`;
    case 'weekdays': return `${s.minute} ${s.hour} * * 1-5`;
    case 'weekly': return `${s.minute} ${s.hour} * * ${s.dayOfWeek}`;
    case 'monthly': return `${s.minute} ${s.hour} ${s.dayOfMonth} * *`;
    case 'custom': return s.raw;
  }
}

// ── Day-of-week options ───────────────────────────────────

const DOW_KEYS = [
  { v: 0, k: 'automation.daySun' },
  { v: 1, k: 'automation.dayMon' },
  { v: 2, k: 'automation.dayTue' },
  { v: 3, k: 'automation.dayWed' },
  { v: 4, k: 'automation.dayThu' },
  { v: 5, k: 'automation.dayFri' },
  { v: 6, k: 'automation.daySat' },
];

// ── Component ─────────────────────────────────────────────

interface CronScheduleBuilderProps {
  value: string;
  onChange: (cron: string) => void;
}

export default function CronScheduleBuilder({ value, onChange }: CronScheduleBuilderProps) {
  const { t } = useTranslation();
  const lastEmitted = useRef(value);
  const [state, setState] = useState<ScheduleState>(() => parseCron(value));

  // Sync when parent value changes externally (e.g., template switch)
  useEffect(() => {
    if (value !== lastEmitted.current) {
      setState(parseCron(value));
      lastEmitted.current = value;
    }
  }, [value]);

  // Emit default cron on mount when starting from empty
  useEffect(() => {
    if (!value.trim() && state.frequency !== 'custom') {
      const cron = buildCron(state);
      lastEmitted.current = cron;
      onChange(cron);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const update = (patch: Partial<ScheduleState>) => {
    const next = { ...state, ...patch };
    const cron = buildCron(next);
    lastEmitted.current = cron;
    setState(next);
    onChange(cron);
  };

  const needsTime = ['daily', 'weekdays', 'weekly', 'monthly'].includes(state.frequency);
  const timeStr = `${String(state.hour).padStart(2, '0')}:${String(state.minute).padStart(2, '0')}`;

  const inputBg = {
    backgroundColor: 'var(--color-bg-card)',
    borderColor: 'var(--color-border-default)',
  };

  return (
    <div className="flex flex-col gap-3">
      <div className="flex flex-wrap items-end gap-3">
        {/* Frequency */}
        <Select
          className="w-40"
          value={state.frequency}
          onChange={(e) => update({ frequency: e.target.value as Frequency })}
        >
          <option value="minutes">{t('automation.freqMinutes')}</option>
          <option value="hourly">{t('automation.freqHourly')}</option>
          <option value="daily">{t('automation.freqDaily')}</option>
          <option value="weekdays">{t('automation.freqWeekdays')}</option>
          <option value="weekly">{t('automation.freqWeekly')}</option>
          <option value="monthly">{t('automation.freqMonthly')}</option>
          <option value="custom">{t('automation.freqCustom')}</option>
        </Select>

        {/* Every N minutes */}
        {state.frequency === 'minutes' && (
          <div className="flex items-center gap-2">
            <span className="text-sm" style={{ color: 'var(--color-text-secondary)' }}>
              {t('automation.every')}
            </span>
            <Input
              type="number"
              min={1}
              max={59}
              value={state.interval}
              onChange={(e) => update({ interval: parseInt(e.target.value, 10) || 1 })}
              className="w-20 border"
              style={inputBg}
            />
            <span className="text-sm" style={{ color: 'var(--color-text-secondary)' }}>
              {t('automation.minutes')}
            </span>
          </div>
        )}

        {/* Hourly at minute */}
        {state.frequency === 'hourly' && (
          <div className="flex items-center gap-2">
            <span className="text-sm" style={{ color: 'var(--color-text-secondary)' }}>
              {t('automation.atMinute')}
            </span>
            <Select
              className="w-24"
              value={state.minute}
              onChange={(e) => update({ minute: parseInt(e.target.value, 10) })}
            >
              {[0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55].map((m) => (
                <option key={m} value={m}>:{String(m).padStart(2, '0')}</option>
              ))}
            </Select>
          </div>
        )}

        {/* Day of week */}
        {state.frequency === 'weekly' && (
          <div className="flex items-center gap-2">
            <span className="text-sm" style={{ color: 'var(--color-text-secondary)' }}>
              {t('automation.onDay')}
            </span>
            <Select
              className="w-32"
              value={state.dayOfWeek}
              onChange={(e) => update({ dayOfWeek: parseInt(e.target.value, 10) })}
            >
              {DOW_KEYS.map((d) => (
                <option key={d.v} value={d.v}>{t(d.k)}</option>
              ))}
            </Select>
          </div>
        )}

        {/* Day of month */}
        {state.frequency === 'monthly' && (
          <div className="flex items-center gap-2">
            <span className="text-sm" style={{ color: 'var(--color-text-secondary)' }}>
              {t('automation.onDay')}
            </span>
            <Select
              className="w-24"
              value={state.dayOfMonth}
              onChange={(e) => update({ dayOfMonth: parseInt(e.target.value, 10) })}
            >
              {Array.from({ length: 28 }, (_, i) => i + 1).map((d) => (
                <option key={d} value={d}>{d}</option>
              ))}
            </Select>
          </div>
        )}

        {/* Time */}
        {needsTime && (
          <div className="flex items-center gap-2">
            <span className="text-sm" style={{ color: 'var(--color-text-secondary)' }}>
              {t('automation.atTime')}
            </span>
            <Input
              type="time"
              value={timeStr}
              onChange={(e) => {
                const [h, m] = (e.target.value || '00:00').split(':').map(Number);
                update({ hour: h || 0, minute: m || 0 });
              }}
              className="w-32 border"
              style={inputBg}
            />
          </div>
        )}
      </div>

      {/* Custom cron fallback */}
      {state.frequency === 'custom' && (
        <div className="flex flex-col gap-1.5">
          <Input
            value={state.raw}
            onChange={(e) => {
              const raw = e.target.value;
              setState((prev) => ({ ...prev, raw }));
              lastEmitted.current = raw;
              onChange(raw);
            }}
            placeholder="*/30 * * * *"
            required
            className="font-mono border placeholder:text-gray-500"
            style={inputBg}
          />
          <span className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
            {t('automation.cronHelp')}
          </span>
        </div>
      )}
    </div>
  );
}
