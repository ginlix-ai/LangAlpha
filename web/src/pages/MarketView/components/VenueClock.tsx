/**
 * TradingView-style venue clock for the chart bottom bar: the symbol's
 * market-local time ticking each second, with its UTC offset ("08:09:36 UTC+8").
 * Pairs with the chart's market-local time axis — the clock and the axis always
 * agree on whose wall clock is shown.
 */
import { useEffect, useState } from 'react';
import { utcOffsetLabel } from '@/lib/utils';

export default function VenueClock({ tz }: { tz: string }) {
  const [now, setNow] = useState(() => new Date());

  useEffect(() => {
    const timer = setInterval(() => setNow(new Date()), 1000);
    return () => clearInterval(timer);
  }, []);

  // en-GB pins the 24h HH:MM:SS form regardless of the user's locale.
  const time = now.toLocaleTimeString('en-GB', { timeZone: tz, hour12: false });

  return (
    <span className="venue-clock" title={tz}>
      <span className="venue-clock-time">{time}</span>
      <span className="venue-clock-offset">{utcOffsetLabel(tz, now)}</span>
    </span>
  );
}
