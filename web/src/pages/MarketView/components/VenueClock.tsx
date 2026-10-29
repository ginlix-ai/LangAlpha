/**
 * TradingView-style venue clock for the chart bottom bar: the symbol's
 * market-local time ticking each second, with its UTC offset ("08:09:36 UTC+8").
 * Pairs with the chart's market-local time axis — the clock and the axis always
 * agree on whose wall clock is shown.
 */
import { memo } from 'react';
import { useNow } from '@/hooks/useNow';
import { formatUtcOffset, utcOffsetMinutes } from '@/lib/timezones';

// Memoized: ticks itself once a second; parent chart re-renders shouldn't add
// extra toLocaleTimeString/Intl work on top.
export default memo(function VenueClock({ tz }: { tz: string }) {
  // The shared second clock: it turns on the wall clock's second, not a second
  // after mount, and catches up at once when a hidden tab comes back.
  const nowMs = useNow(1000);
  const now = new Date(nowMs);

  // en-GB pins the 24h HH:MM:SS form regardless of the user's locale.
  const time = now.toLocaleTimeString('en-GB', { timeZone: tz, hour12: false });

  return (
    <span className="venue-clock" title={tz}>
      <span className="venue-clock-time">{time}</span>
      <span className="venue-clock-offset">{formatUtcOffset(utcOffsetMinutes(tz, now))}</span>
    </span>
  );
});
