import { useId, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { isSameDay, type CalendarDate } from '@internationalized/date';
import { CalendarDays } from 'lucide-react';
import { Button } from 'react-aria-components';
import {
  Calendar,
  CalendarCell,
  CalendarGrid,
  CalendarGridBody,
  CalendarGridHeader,
  CalendarHeaderCell,
  CalendarHeading,
} from '@/components/ui/aria-calendar';
import { Popover, PopoverDialog, PopoverTrigger } from '@/components/ui/aria-popover';
import { useLocale } from '@/hooks/useLocale';
import { formatTimezoneName } from '@/lib/format';
import { dayIn, instantAt, nextOnDay, nextSlot, quickPicks, timeIn, type QuickPickId } from '../utils/moments';
import { MORNING, type TimeOfDay } from '../utils/timeOfDay';
import { formatMoment, formatUpcoming, toDate } from '../utils/time';
import { TimeInput, TimeSlotList } from './TimeField';
import './DateTimePicker.css';

const PICK_LABEL_KEY: Record<QuickPickId, string> = {
  inAnHour: 'automation.quickInAnHour',
  tomorrowMorning: 'automation.quickTomorrowMorning',
  beforeOpen: 'automation.quickBeforeOpen',
  afterClose: 'automation.quickAfterClose',
};

interface DateTimePickerProps {
  /** An ISO instant, or '' while nothing is chosen. */
  value: string;
  onChange: (iso: string) => void;
  /** The IANA zone whose clock the days and times are read on. */
  timeZone: string;
  labelledBy?: string;
}

/**
 * One moment in the future, on the clock of the zone the automation is set
 * in: a few common moments to take in one click, a month to pick the day
 * from, and the quarter hours (or any typed time) for the time. It holds an
 * instant rather than a wall-clock string, so what is shown is what gets
 * saved.
 *
 * Choosing never closes the panel; Done, Enter in the time field, Escape or
 * a click outside do. It stays to show what the choice set, in the typed
 * field above the list most of all: closing on a quarter hour would hide
 * that any time can be typed there.
 */
export default function DateTimePicker({ value, onChange, timeZone, labelledBy }: DateTimePickerProps) {
  const { t } = useTranslation();
  const locale = useLocale();
  const valueId = useId();
  const at = toDate(value);
  const [open, setOpen] = useState(false);
  const [now, setNow] = useState(() => new Date());
  // Days and times as the zone's clock reads them; instants only at the edges.
  const today = dayIn(now, timeZone);
  const atDay = at ? dayIn(at, timeZone) : null;
  const atTime = at ? timeIn(at, timeZone) : null;
  // The day the calendar's cursor is on; the page shown is its month.
  const [focused, setFocused] = useState(atDay ?? today);

  const onOpenChange = (next: boolean) => {
    if (next) {
      const fresh = new Date();
      setNow(fresh);
      setFocused(atDay ?? dayIn(fresh, timeZone));
    }
    setOpen(next);
  };

  const commit = (d: Date) => {
    setFocused(dayIn(d, timeZone));
    onChange(d.toISOString());
  };

  // A day keeps the time already set (nine in the morning before there is
  // one); today with that hour gone takes the next quarter hour instead.
  const pickDay = (day: CalendarDate) => {
    commit(nextOnDay(day, atTime ?? MORNING, timeZone, now) ?? nextSlot(now));
  };

  // A time keeps the day already set (today before there is one); a time
  // that has passed on that day means the same time the next day.
  const pickTime = (time: TimeOfDay) => {
    commit(nextOnDay(atDay ?? today, time, timeZone, now) ?? instantAt(today.add({ days: 1 }), time, timeZone));
  };

  const firstOpen = nextSlot(now, 1);
  const slotFloor: TimeOfDay | null =
    atDay && isSameDay(atDay, today)
      ? isSameDay(dayIn(firstOpen, timeZone), today) ? timeIn(firstOpen, timeZone) : { hour: 24, minute: 0 }
      : null;

  return (
    <PopoverTrigger isOpen={open} onOpenChange={onOpenChange}>
      <Button
        className="automation-moment-field flex h-9 w-full max-w-64 items-center gap-2 rounded-md border px-3 text-sm"
        data-empty={at ? undefined : ''}
        aria-labelledby={labelledBy ? `${labelledBy} ${valueId}` : undefined}
      >
        <CalendarDays aria-hidden="true" />
        <span id={valueId}>{at ? formatMoment(at, locale, timeZone) : t('automation.pickMoment')}</span>
      </Button>
      <Popover placement="bottom start" offset={6} containerPadding={16} className="automation-moment-popover">
        <PopoverDialog aria-label={t('automation.pickMoment')} className="p-0">
          <div className="automation-moment-picks">
            {quickPicks(now, timeZone).map((p) => (
              <button
                key={p.id}
                type="button"
                className="automation-moment-pick"
                aria-pressed={at?.getTime() === p.at.getTime()}
                onClick={() => commit(p.at)}
              >
                <span className="automation-moment-pick-name">{t(PICK_LABEL_KEY[p.id])}</span>
                <span className="automation-mono automation-moment-pick-when">{formatUpcoming(p.at, locale, now.getTime(), timeZone)}</span>
              </button>
            ))}
          </div>
          <div className="automation-moment-body">
            {/* Monday first, like the schedule's weekday strip. Days before
                today stay in place but cannot be chosen. */}
            <Calendar
              value={atDay}
              onChange={pickDay}
              minValue={today}
              today={today}
              focusedValue={focused}
              onFocusChange={setFocused}
              firstDayOfWeek="mon"
              autoFocus
              className="automation-calendar"
            >
              <CalendarHeading previousLabel={t('automation.prevMonth')} nextLabel={t('automation.nextMonth')} />
              <CalendarGrid>
                <CalendarGridHeader>{(day) => <CalendarHeaderCell>{day}</CalendarHeaderCell>}</CalendarGridHeader>
                <CalendarGridBody>{(date) => <CalendarCell date={date} />}</CalendarGridBody>
              </CalendarGrid>
            </Calendar>
            <div className="automation-moment-times">
              <TimeInput
                value={atTime}
                onCommit={pickTime}
                onEnter={() => setOpen(false)}
                placeholder={t('automation.time')}
                aria-label={t('automation.time')}
              />
              <TimeSlotList value={atTime} min={slotFloor} onPick={pickTime} />
            </div>
          </div>
          <div className="automation-moment-foot">
            <span className="automation-mono automation-moment-zone">{formatTimezoneName(timeZone, locale)}</span>
            <button type="button" className="automation-moment-done" onClick={() => setOpen(false)}>
              {t('common.done')}
            </button>
          </div>
        </PopoverDialog>
      </Popover>
    </PopoverTrigger>
  );
}
