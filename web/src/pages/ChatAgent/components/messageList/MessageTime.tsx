import React from 'react';
import { useLocale } from '@/hooks/useLocale';
import { useNow } from '@/hooks/useNow';
import { chatTime, fullDateTime } from '@/lib/format';
import { messageTimeMs } from './turnProjection';

/** When a message was sent or a reply finished, at the end of its actions
 *  row. A time that is missing or unreadable draws nothing: `new Date(null)`
 *  is the 1970 epoch, a valid date, so it must not reach the formatter. */
export function MessageTime({ value }: { value: unknown }): React.ReactElement | null {
  const locale = useLocale();
  // The clock ticks on UTC boundaries, and every zone's offset is a multiple of
  // 15 minutes, so a quarter-hour clock turns at local midnight wherever the
  // reader is: "8:43 PM" becomes a weekday when the day does.
  const now = useNow(15 * 60_000);
  const ms = messageTimeMs(value);
  if (ms === undefined) return null;
  return (
    <time
      dateTime={new Date(ms).toISOString()}
      title={fullDateTime(ms, locale)}
      className="self-center whitespace-nowrap px-1 text-xs tabular-nums"
      style={{ color: 'var(--color-text-tertiary)' }}
    >
      {chatTime(ms, locale, now)}
    </time>
  );
}
