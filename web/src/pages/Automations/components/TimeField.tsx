import React, { useLayoutEffect, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Popover } from '@/components/ui/aria-popover';
import { Input } from '@/components/ui/input';
import { useLocale } from '@/hooks/useLocale';
import { cn } from '@/lib/utils';
import { useScrollReveal } from '../hooks/useScrollReveal';
import { DAY_SLOTS, MORNING, parseTime, sameTime, stepTime, type TimeOfDay } from '../utils/timeOfDay';
import { formatTimeOfDay } from '../utils/time';
import './TimeField.css';

const minutes = (t: TimeOfDay) => t.hour * 60 + t.minute;

interface TimeSlotListProps {
  value: TimeOfDay | null;
  /** The earliest slot that can be picked; the ones before it stay visible
   *  so the list keeps its shape, but read as passed. */
  min?: TimeOfDay | null;
  onPick: (t: TimeOfDay) => void;
  className?: string;
}

/** The quarter hours of a day, for the pointer. The keyboard path is the
 *  typed field beside it, so the options stay out of the tab order. */
export function TimeSlotList({ value, min, onPick, className }: TimeSlotListProps) {
  const { t } = useTranslation();
  const locale = useLocale();
  const ref = useRef<HTMLDivElement>(null);
  const revealScroll = useScrollReveal();
  const floor = min ? minutes(min) : 0;
  const rest = value ?? (minutes(MORNING) >= floor ? MORNING : min!);

  // Rest on the chosen time (or nine in the morning), centered, by moving the
  // list itself: scrollIntoView would also scroll the page under a popover.
  const restKey = minutes(rest);
  useLayoutEffect(() => {
    const list = ref.current;
    if (!list) return;
    const i = DAY_SLOTS.findIndex((s) => minutes(s) >= restKey);
    const el = list.children[Math.max(i, 0)] as HTMLElement | undefined;
    if (el) list.scrollTop = el.offsetTop - list.clientHeight / 2 + el.offsetHeight / 2;
  }, [restKey]);

  return (
    <div
      ref={ref}
      role="listbox"
      aria-label={t('automation.time')}
      className={cn('automation-time-list automations-scroller', className)}
      onScroll={revealScroll}
    >
      {DAY_SLOTS.map((s) => (
        <button
          key={minutes(s)}
          type="button"
          role="option"
          tabIndex={-1}
          aria-selected={sameTime(s, value)}
          disabled={minutes(s) < floor}
          className="automation-mono automation-time-slot"
          onClick={() => onPick(s)}
        >
          {formatTimeOfDay(s.hour, s.minute, locale)}
        </button>
      ))}
    </div>
  );
}

interface TimeInputProps extends Omit<React.ComponentProps<'input'>, 'value' | 'onChange'> {
  value: TimeOfDay | null;
  onCommit: (t: TimeOfDay) => void;
  /** Enter was pressed and whatever was typed is now committed. */
  onEnter?: () => void;
}

/**
 * A time typed as it is said ("2:40p", "14:40") and read back in the reader's
 * clock format once it is committed, on Enter or on leaving the field. ↑/↓
 * step a quarter hour, in the direction the list beside it runs.
 */
export function TimeInput({ value, onCommit, onEnter, onKeyDown, onBlur, className, ...rest }: TimeInputProps) {
  const locale = useLocale();
  // A draft belongs to the time it was typed over. A slot picked from the
  // list moves the value while focus stays here, and the draft left behind
  // must not be committed over the pick when the field is left.
  const valueKey = value ? minutes(value) : -1;
  const [typed, setTyped] = useState<{ text: string; over: number } | null>(null);
  const draft = typed && typed.over === valueKey ? typed.text : null;
  const setDraft = (text: string | null) => setTyped(text === null ? null : { text, over: valueKey });
  const commit = () => {
    if (draft === null) return;
    const parsed = parseTime(draft);
    setDraft(null);
    if (parsed && !sameTime(parsed, value)) onCommit(parsed);
  };
  return (
    <Input
      {...rest}
      inputMode="text"
      autoComplete="off"
      spellCheck={false}
      value={draft ?? (value ? formatTimeOfDay(value.hour, value.minute, locale) : '')}
      onChange={(e) => setDraft(e.target.value)}
      onBlur={(e) => {
        commit();
        onBlur?.(e);
      }}
      onKeyDown={(e) => {
        onKeyDown?.(e);
        if (e.defaultPrevented) return;
        if (e.key === 'Enter') {
          // Inside the form, Enter would otherwise submit it.
          e.preventDefault();
          commit();
          onEnter?.();
        } else if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
          e.preventDefault();
          setDraft(null);
          onCommit(stepTime(value ?? MORNING, e.key === 'ArrowDown' ? 1 : -1));
        }
      }}
      className={cn('automation-mono automation-time-input', className)}
    />
  );
}

interface TimeFieldProps {
  value: TimeOfDay;
  onChange: (t: TimeOfDay) => void;
  'aria-label'?: string;
  className?: string;
}

/** A standalone time of day: the typed field, with the quarter hours
 *  dropping beneath it while it is being set. Focus never leaves the field,
 *  so the field is what closes the list: on leaving it, on Escape, on Enter. */
export default function TimeField({ value, onChange, className, ...aria }: TimeFieldProps) {
  const [open, setOpen] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);
  return (
    <>
      <TimeInput
        ref={inputRef}
        {...aria}
        value={value}
        onCommit={onChange}
        onClick={() => setOpen(true)}
        onEnter={() => setOpen(false)}
        onBlur={() => setOpen(false)}
        onKeyDown={(e) => {
          if (e.key !== 'Escape' || !open) return;
          e.preventDefault();
          e.stopPropagation();
          setOpen(false);
        }}
        aria-haspopup="listbox"
        aria-expanded={open}
        className={className}
      />
      <Popover
        triggerRef={inputRef}
        isOpen={open}
        onOpenChange={setOpen}
        isNonModal
        placement="bottom start"
        offset={6}
        className="automation-time-popover"
      >
        {/* A press on a slot must not take focus from the field, which would
            close the list before the press lands. */}
        <div onMouseDown={(e) => e.preventDefault()}>
          <TimeSlotList
            value={value}
            onPick={(t) => {
              onChange(t);
              setOpen(false);
            }}
          />
        </div>
      </Popover>
    </>
  );
}
