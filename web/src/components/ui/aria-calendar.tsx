"use client"

import * as React from "react"
import {
  getLocalTimeZone,
  today as todayIn,
  type CalendarDate,
} from "@internationalized/date"
import { ChevronLeft, ChevronRight } from "lucide-react"
import {
  Button as AriaButton,
  Calendar as AriaCalendar,
  CalendarCell as AriaCalendarCell,
  CalendarCellProps as AriaCalendarCellProps,
  CalendarGrid as AriaCalendarGrid,
  CalendarGridBody as AriaCalendarGridBody,
  CalendarGridHeader as AriaCalendarGridHeader,
  CalendarGridProps as AriaCalendarGridProps,
  CalendarHeaderCell as AriaCalendarHeaderCell,
  CalendarHeaderCellProps as AriaCalendarHeaderCellProps,
  CalendarProps as AriaCalendarProps,
  DateValue as AriaDateValue,
  Heading as AriaHeading,
  composeRenderProps,
} from "react-aria-components"

import { cn } from "@/lib/utils"

// RAC's own `data-today` follows this device's clock. A calendar of another
// zone's days has to mark that zone's today, so the cells carry
// `data-zone-today` from the calendar's `today` and the styling reads that.
const TodayContext = React.createContext<CalendarDate | null>(null)

interface CalendarProps<T extends AriaDateValue> extends AriaCalendarProps<T> {
  /** Today as the clock the days are read on shows it; this device's by
   *  default. */
  today?: CalendarDate
}

function Calendar<T extends AriaDateValue>({
  className,
  today,
  ...props
}: CalendarProps<T>) {
  return (
    <TodayContext.Provider value={today ?? todayIn(getLocalTimeZone())}>
      <AriaCalendar
        className={composeRenderProps(className, (className) =>
          cn("w-fit", className)
        )}
        {...props}
      />
    </TodayContext.Provider>
  )
}

const NAV_BUTTON =
  "inline-flex size-7 items-center justify-center rounded-md text-[color:var(--color-text-secondary)] data-[hovered]:bg-[color:var(--color-bg-hover)] data-[hovered]:text-[color:var(--color-text-primary)] data-[disabled]:cursor-default data-[disabled]:opacity-35"

/** The month and year, with the page turners set to its right. */
function CalendarHeading({
  className,
  previousLabel,
  nextLabel,
}: {
  className?: string
  /** Replaces RAC's own "Previous" and "Next". */
  previousLabel?: string
  nextLabel?: string
}) {
  return (
    <header
      className={cn(
        "flex items-center justify-between pb-1.5 pl-1.5 pr-0.5",
        className
      )}
    >
      <AriaHeading className="text-[0.8125rem] font-semibold text-[color:var(--color-text-primary)]" />
      <div className="flex gap-0.5">
        <AriaButton slot="previous" aria-label={previousLabel} className={NAV_BUTTON}>
          <ChevronLeft aria-hidden="true" className="size-[15px]" />
        </AriaButton>
        <AriaButton slot="next" aria-label={nextLabel} className={NAV_BUTTON}>
          <ChevronRight aria-hidden="true" className="size-[15px]" />
        </AriaButton>
      </div>
    </header>
  )
}

const CalendarGrid = ({ className, ...props }: AriaCalendarGridProps) => (
  <AriaCalendarGrid
    className={cn(
      "border-separate border-spacing-x-0 border-spacing-y-0.5 [&_td]:p-0",
      className
    )}
    {...props}
  />
)

const CalendarGridHeader = AriaCalendarGridHeader

const CalendarHeaderCell = ({
  className,
  ...props
}: AriaCalendarHeaderCellProps) => (
  <AriaCalendarHeaderCell
    className={cn(
      "py-1 text-center font-mono text-[0.6875rem] font-normal text-[color:var(--color-text-tertiary)]",
      className
    )}
    {...props}
  />
)

const CalendarGridBody = AriaCalendarGridBody

/** Days outside the month keep their place but show nothing. Today is an
 *  annotation, a dot of amber under its number; the chosen day takes the
 *  primary button's fill. */
function CalendarCell({ className, date, ...props }: AriaCalendarCellProps) {
  const today = React.useContext(TodayContext)
  return (
    <AriaCalendarCell
      date={date}
      data-zone-today={today && date.compare(today) === 0 ? "" : undefined}
      className={composeRenderProps(className, (className) =>
        cn(
          "relative flex h-8 w-[34px] cursor-default items-center justify-center rounded-md text-[0.8125rem] tabular-nums text-[color:var(--color-text-primary)] transition-colors",
          "[&[data-hovered]:not([data-selected])]:bg-[color:var(--color-bg-hover)]",
          "data-[disabled]:text-[color:var(--color-text-quaternary)]",
          "data-[outside-month]:invisible",
          "data-[selected]:bg-[color:var(--color-btn-primary-bg)] data-[selected]:font-semibold data-[selected]:text-[color:var(--color-btn-primary-text)]",
          "data-[zone-today]:after:absolute data-[zone-today]:after:bottom-1 data-[zone-today]:after:left-1/2 data-[zone-today]:after:size-[3px] data-[zone-today]:after:-translate-x-1/2 data-[zone-today]:after:rounded-full data-[zone-today]:after:bg-[color:var(--color-accent-primary)]",
          className
        )
      )}
      {...props}
    />
  )
}

export {
  Calendar,
  CalendarHeading,
  CalendarGrid,
  CalendarGridHeader,
  CalendarHeaderCell,
  CalendarGridBody,
  CalendarCell,
}
export type { CalendarProps }
