// Apart from `lib/timezones` and the picker's zone list because the entry
// reads the device's zone (the sign-in sync, every thread request), and a
// module the entry imports ships in it whole, with every export a lazy chunk
// uses: the picker's zone list and search rode first paint that way.

/** Names the IANA database has retired but ICU still reports, Chrome's own
 *  zone included for a reader in India or Vietnam. Each is kept under the
 *  name that replaced it, so the browser's list and the common one agree
 *  on a single entry per zone. */
export const CURRENT_NAME: Record<string, string> = {
  'Africa/Asmera': 'Africa/Asmara',
  'America/Buenos_Aires': 'America/Argentina/Buenos_Aires',
  'America/Catamarca': 'America/Argentina/Catamarca',
  'America/Cordoba': 'America/Argentina/Cordoba',
  'America/Godthab': 'America/Nuuk',
  'America/Indianapolis': 'America/Indiana/Indianapolis',
  'America/Jujuy': 'America/Argentina/Jujuy',
  'America/Louisville': 'America/Kentucky/Louisville',
  'America/Mendoza': 'America/Argentina/Mendoza',
  'Asia/Calcutta': 'Asia/Kolkata',
  'Asia/Katmandu': 'Asia/Kathmandu',
  'Asia/Rangoon': 'Asia/Yangon',
  'Asia/Saigon': 'Asia/Ho_Chi_Minh',
  'Atlantic/Faeroe': 'Atlantic/Faroe',
  'Europe/Kiev': 'Europe/Kyiv',
  'Pacific/Enderbury': 'Pacific/Kanton',
  'Pacific/Ponape': 'Pacific/Pohnpei',
  'Pacific/Truk': 'Pacific/Chuuk',
};

const clocks = new Map<string, Intl.DateTimeFormat>();

/** A zone's wall clock to the second, cached per zone since a clock ticking
 *  every second reads through it. Numeric fields only: the named offset
 *  styles (`longOffset`, `shortOffset`) throw a RangeError before Chrome 95
 *  and Safari 15.4. Throws for a zone this browser does not know. */
export function zoneClock(tz: string): Intl.DateTimeFormat {
  let fmt = clocks.get(tz);
  if (!fmt) {
    fmt = new Intl.DateTimeFormat('en-US', {
      timeZone: tz,
      hourCycle: 'h23',
      year: 'numeric',
      month: 'numeric',
      day: 'numeric',
      hour: 'numeric',
      minute: 'numeric',
      second: 'numeric',
    });
    clocks.set(tz, fmt);
  }
  return fmt;
}

export function isKnownTimezone(tz: string): boolean {
  try {
    zoneClock(tz);
    return true;
  } catch {
    return false;
  }
}

/** A zone by the name the IANA database uses now, when this browser knows
 *  that name too. */
export function currentTimezoneName(tz: string): string {
  const current = CURRENT_NAME[tz];
  return current && isKnownTimezone(current) ? current : tz;
}

/** The zone this device's clock is in, or null where the browser cannot say,
 *  for a caller whose fallback is to send nothing. A name the engine reports
 *  but cannot format with (a stripped ICU, an `Etc/Unknown`) counts as not
 *  saying: every clock drawn in it would throw. */
export function detectedTimezone(): string | null {
  try {
    const tz = Intl.DateTimeFormat().resolvedOptions().timeZone;
    if (tz && isKnownTimezone(tz)) return currentTimezoneName(tz);
  } catch {
    // Intl not available
  }
  return null;
}

/** The zone this device's clock is in. */
export function deviceTimezone(): string {
  return detectedTimezone() ?? 'America/New_York';
}
