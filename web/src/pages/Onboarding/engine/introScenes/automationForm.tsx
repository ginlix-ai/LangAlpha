import type { CSSProperties, ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import { ChevronDown, ChevronRight } from 'lucide-react';
import { useLocale } from '@/hooks/useLocale';
import { cn } from '@/lib/utils';
import { weekdayInitials } from '@/pages/Automations/utils/time';
import { Track } from './automationParts';

/* The new-automation form at scene scale, each part a small copy of the
   component named on it: the hairline rows and their fields, the trigger
   choice with its cadence marks, the readout and the actions, the folded
   options and the day picker. automations.tsx lays the form out; the page
   around it is in automationParts.tsx. */
/* eslint-disable react-refresh/only-export-components -- form parts: small
   components plus the weekday list and a hook */

/** Monday to Friday, Monday first, as the week strip and day picker read it. */
export const WEEKDAYS = [true, true, true, true, true, false, false] as const;

/** The chosen chip or day: a darker edge, a washed fill and full ink. */
const ON: CSSProperties = {
  borderColor: 'var(--color-text-secondary)',
  background: 'var(--color-bg-hover)',
  color: 'var(--color-text-primary)',
};

/* ── the trigger's cadence marks ───────────────────────────────────────── */

/** The days a schedule lands on (`WeekStrip`): weekday initials, the days it
    runs in full ink and the rest faded. */
function WeekStrip({ days }: { days: readonly boolean[] }) {
  const initials = weekdayInitials(useLocale());
  return (
    <span
      className="grid grid-cols-[repeat(7,6px)] font-mono text-[5.5px] leading-[6px]"
      style={{ color: 'var(--color-text-tertiary)' }}
      aria-hidden
    >
      {initials.map((initial, i) => (
        <span
          key={i}
          className="text-center"
          style={days[i] ? { color: 'var(--color-text-primary)' } : { opacity: 0.45 }}
        >
          {initial}
        </span>
      ))}
    </span>
  );
}

/** A price watch's mark (`MeterMark`), at the week strip's width. */
function MeterMark({ wash }: { wash?: string }) {
  return <Track width={42} cur={30} thr={72} gap={[30, 42]} wash={wash} />;
}

/** A one-time run's mark (`OnceMark`): one point on the track. */
function OnceMark({ wash }: { wash?: string }) {
  return <Track width={42} cur={62} wash={wash} />;
}

/* ── the form ──────────────────────────────────────────────────────────── */

/** One setting (`FormRow`), its name above its field the way the manage
    pane sets them; every row after the first opens on a hairline. */
export function FormRow({
  label,
  first = false,
  className,
  children,
}: {
  label: ReactNode;
  first?: boolean;
  className?: string;
  children?: ReactNode;
}) {
  return (
    <div
      className={cn('flex flex-col gap-[4px] py-[5px]', !first && 'border-t', className)}
      style={{ borderColor: 'var(--color-border-muted)' }}
    >
      <div
        className="text-[7px] font-medium leading-[9px]"
        style={{ color: 'var(--color-text-primary)' }}
      >
        {label}
      </div>
      {children}
    </div>
  );
}

/** A field on the form: the input fill on the page, the hairline edge, the
    accent edge while focused; `chevron` makes it a select. */
export function Field({
  focus = false,
  chevron = false,
  className,
  children,
}: {
  focus?: boolean;
  chevron?: boolean;
  className?: string;
  children?: ReactNode;
}) {
  return (
    <div
      className={cn(
        'flex h-4 min-w-0 items-center gap-[3px] rounded-[4px] border px-[5px]',
        'text-[7px] leading-none',
        className
      )}
      style={{
        background: 'var(--color-bg-input)',
        borderColor: focus ? 'var(--color-accent-primary)' : 'var(--color-border-muted)',
        color: 'var(--color-text-primary)',
        transition: 'border-color var(--motion-quick) var(--ease-out)',
      }}
    >
      {/* a taller line than the field's, so the clip keeps descenders */}
      <span className="min-w-0 flex-1 truncate leading-[1.3]">{children}</span>
      {chevron && <ChevronDown className="h-[7px] w-[7px] shrink-0 opacity-60" />}
    </div>
  );
}

/** The kinds of trigger (`TriggerChoice`), each drawn with its cadence mark:
    the chosen one pressed, the others' marks faded. `days` is what the
    schedule's strip shows. */
export function TriggerChips({
  value,
  days,
}: {
  value: 'cron' | 'once' | 'price';
  days: readonly boolean[];
}) {
  const { t } = useTranslation();
  const kinds = [
    { id: 'cron', label: t('automation.triggerSchedule') },
    { id: 'once', label: t('automation.once') },
    { id: 'price', label: t('automation.triggerPrice') },
  ] as const;
  return (
    <div className="flex gap-1">
      {kinds.map((kind) => {
        const on = kind.id === value;
        const wash = on ? 'var(--color-bg-hover)' : undefined;
        return (
          <div
            key={kind.id}
            className={cn(
              'flex min-w-max max-w-[92px] flex-1 flex-col items-start gap-[4px] rounded-[4px]',
              'border px-1.5 py-[4px]'
            )}
            style={
              on
                ? ON
                : { borderColor: 'var(--color-border-muted)', color: 'var(--color-text-secondary)' }
            }
          >
            <span style={{ opacity: on ? 1 : 0.6 }}>
              {kind.id === 'cron' ? (
                <WeekStrip days={days} />
              ) : kind.id === 'once' ? (
                <OnceMark wash={wash} />
              ) : (
                <MeterMark wash={wash} />
              )}
            </span>
            <span className="text-[7px] font-medium leading-none">{kind.label}</span>
          </div>
        );
      })}
    </div>
  );
}

/** What the trigger as set adds up to, read back under it. */
export function Readout({ className, children }: { className?: string; children: ReactNode }) {
  return (
    <div
      className={cn('text-[7px] leading-[1.3]', className)}
      style={{ color: 'var(--color-text-tertiary)' }}
    >
      {children}
    </div>
  );
}

/** The form's Create (or Save changes) and Cancel. */
export function FormActions({ label, pressed }: { label: string; pressed?: string }) {
  const { t } = useTranslation();
  return (
    <div className="flex items-center gap-1 pt-[6px]">
      <span
        className={cn(
          'inline-flex h-[15px] items-center rounded-[4px] px-2 text-[7px] font-medium',
          'leading-none transition-transform duration-press'
        )}
        style={{
          background: 'var(--color-btn-primary-bg)',
          color: 'var(--color-btn-primary-text)',
          transform: pressed,
        }}
      >
        {label}
      </span>
      <span
        className="inline-flex h-[15px] items-center px-2 text-[7px] font-medium leading-none"
        style={{ color: 'var(--color-text-primary)' }}
      >
        {t('common.cancel')}
      </span>
    </div>
  );
}

/** The form's title, one step under the page's. */
export function FormTitle({ children }: { children: ReactNode }) {
  return (
    <div
      className="text-[9px] font-semibold leading-none"
      style={{ color: 'var(--color-text-primary)' }}
    >
      {children}
    </div>
  );
}

/** The instruction box: the input fill, the text cut to two lines. */
export function InstructionBox({ text }: { text: string }) {
  return (
    <div
      className="rounded-[4px] border px-[5px] py-[3px] text-[6.5px] leading-[1.35]"
      style={{
        background: 'var(--color-bg-input)',
        borderColor: 'var(--color-border-muted)',
        color: 'var(--color-text-primary)',
      }}
    >
      {/* clamped inside the padding, so a third line cannot show through it */}
      <p className="line-clamp-2">{text}</p>
    </div>
  );
}

/** The folded options' row (`MoreOptions`): its name and chevron, and while
    folded what they are set to, under it as the narrow pane sets it. */
export function MoreToggle({ open, summary }: { open: boolean; summary: string }) {
  const { t } = useTranslation();
  return (
    <div
      className="flex flex-col gap-[3px] border-t pb-[5px] pt-[5px]"
      style={{ borderColor: 'var(--color-border-muted)' }}
    >
      <span
        className="inline-flex items-center gap-[2px] text-[7px] font-medium leading-[9px]"
        style={{ color: open ? 'var(--color-text-primary)' : 'var(--color-text-secondary)' }}
      >
        {t('automation.moreOptions')}
        <ChevronRight
          className="h-[7px] w-[7px] transition-transform duration-fold"
          style={{ transform: open ? 'rotate(90deg)' : undefined }}
        />
      </span>
      {!open && (
        <span
          className="truncate text-[6.5px] leading-[9px]"
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          {summary}
        </span>
      )}
    </div>
  );
}

/** What the folded options say, as `MoreOptions` joins it. */
export function useSummary(): string {
  const { t } = useTranslation();
  return [
    t('agents.chiefOfStaff'),
    t('automation.continueExisting'),
    t('automation.noDelivery'),
    t('automation.stopsAfter', { count: 3 }),
  ].join(' · ');
}

/** The schedule's days (`DayPicker`): seven squares, Monday to Friday pressed. */
export function WeekPicker({ className }: { className: string }) {
  const initials = weekdayInitials(useLocale());
  return (
    <div className="flex gap-[2px]">
      {initials.map((initial, i) => (
        <span
          key={i}
          className={className}
          style={
            WEEKDAYS[i]
              ? ON
              : { borderColor: 'var(--color-border-muted)', color: 'var(--color-text-tertiary)' }
          }
        >
          {initial}
        </span>
      ))}
    </div>
  );
}
