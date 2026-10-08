/**
 * Locale-aware number/date formatters.
 *
 * Every formatter takes the locale as an argument (`useLocale()` in a
 * component), and `relativeTime` takes the time (`useNow()`), instead of
 * reading `i18n.language` or the clock itself. The React Compiler caches a call
 * on its arguments, so an input read behind its back pins the output at the
 * first value it saw: the old language after a switch, or a clock that never
 * moves. Intl instances are cached per locale, so a hot render path pays for
 * one construction per locale, not per call.
 */

// Defensive: if the locale is briefly empty or invalid (transient
// changeLanguage state, a broken cookie), the Intl constructor throws, and
// every formatted widget on the dashboard would crash at once. The fallback
// uses the host default locale, cached under the bad key so the construction
// is not retried on every call.
function perLocale<T>(make: (locale: string | undefined) => T): (locale: string) => T {
  const made = new Map<string, T>();
  return (locale) => {
    let value = made.get(locale);
    if (value === undefined) {
      try {
        value = make(locale);
      } catch {
        value = make(undefined);
      }
      made.set(locale, value);
    }
    return value;
  };
}

export function createFormatter(opts: Intl.NumberFormatOptions): (n: number, locale: string) => string {
  const format = perLocale((locale) => new Intl.NumberFormat(locale, opts));
  return (n, locale) => format(locale).format(n);
}

export function createDateFormatter(
  opts: Intl.DateTimeFormatOptions,
): (d: Date | number, locale: string) => string {
  const format = perLocale((locale) => new Intl.DateTimeFormat(locale, opts));
  return (d, locale) => format(locale).format(d);
}

// Option sets shared by several components.
export const shortMonthDay = createDateFormatter({ month: 'short', day: 'numeric' });
export const clockTime = createDateFormatter({ hour: 'numeric', minute: '2-digit' });
export const paddedClockTime = createDateFormatter({ hour: '2-digit', minute: '2-digit' });
export const mediumDate = createDateFormatter({ dateStyle: 'medium' });
export const mediumTime = createDateFormatter({ timeStyle: 'medium' });
export const shortWeekday = createDateFormatter({ weekday: 'short' });
export const weekdayMonthDay = createDateFormatter({ weekday: 'long', month: 'short', day: 'numeric' });

// The fields Date#toLocaleString() prints by default.
const dateTime = createDateFormatter({
  year: 'numeric', month: 'numeric', day: 'numeric', hour: 'numeric', minute: 'numeric', second: 'numeric',
});

/** A timestamp string as a date and time in `locale`. An unparseable one comes
 *  back as written, since Intl throws a RangeError on an invalid Date. */
export function formatTimestamp(ts: string | null | undefined, locale: string): string {
  if (!ts) return '';
  const d = new Date(ts);
  if (Number.isNaN(d.getTime())) return ts;
  return dateTime(d, locale);
}

const listFormat = perLocale((locale) => new Intl.ListFormat(locale, { type: 'conjunction' }));

/** Names joined as the locale says "A, B and C". A Chinese conjunction is a
 *  word and is spaced from the names beside it (`moomoo 和 Webull`), since the
 *  names are usually Latin; the enumeration comma needs no space. */
export function formatList(items: readonly string[], locale: string): string {
  return listFormat(locale)
    .formatToParts(items)
    .map((part) => (part.type === 'literal' && /^\p{Script=Han}+$/u.test(part.value) ? ` ${part.value} ` : part.value))
    .join('');
}

const zoneNames = new Map<string, string>();

/** A zone's name in `locale` ("Eastern Time", "中国标准时间"). UTC stays
 *  "UTC", which Intl would call "GMT". The short style names the country where
 *  a zone is a whole one ("Germany Time"). */
export function formatTimezoneName(
  tz: string,
  locale: string,
  style: 'longGeneric' | 'shortGeneric' = 'longGeneric',
): string {
  if (tz === 'UTC' || tz === 'Etc/UTC') return 'UTC';
  const key = `${locale}|${style}|${tz}`;
  let name = zoneNames.get(key);
  if (name === undefined) {
    try {
      const opts: Intl.DateTimeFormatOptions = { timeZone: tz, timeZoneName: style };
      let zone: Intl.DateTimeFormat;
      try {
        zone = new Intl.DateTimeFormat(locale, opts);
      } catch {
        zone = new Intl.DateTimeFormat(undefined, opts);
      }
      name = zone.formatToParts(new Date()).find((p) => p.type === 'timeZoneName')?.value ?? tz;
    } catch {
      name = tz;
    }
    zoneNames.set(key, name);
  }
  return name;
}

// Compact short-form integer formatter — `1234 → "1.2K"`, `5_142 → "5.1K"`,
// `1_500_000 → "1.5M"`. Locale-aware via Intl. Numbers under 1000 render in
// full; suffix style follows the active locale (en `K`, zh `万`, etc).
export const compactNumber = createFormatter({ notation: 'compact', maximumFractionDigits: 1 });

// Grouped, for a figure read in the locale's own style (`1,234.50`). `grouped`
// is `toLocaleString()`'s default digits; `integer` is a count. Not `fixed2`,
// which turns grouping off.
export const grouped = createFormatter({ maximumFractionDigits: 3 });
export const grouped2 = createFormatter({ minimumFractionDigits: 2, maximumFractionDigits: 2 });
export const integer = createFormatter({ maximumFractionDigits: 0 });

// The quote-strip variants. Two fixed decimals so a column of figures keeps
// its width from one tick to the next; grouping off because a stock price
// reads as one number (`1234.50`, not `1,234.50`). Null is the reader's
// concern: these take a number, the caller decides what an absent one shows.
export const fixed2 = createFormatter({ minimumFractionDigits: 2, maximumFractionDigits: 2, useGrouping: false });
// `exceptZero` rather than `always`: a flat or sub-cent move prints `0.00`, not `-0.00`.
export const signedFixed2 = createFormatter({ minimumFractionDigits: 2, maximumFractionDigits: 2, useGrouping: false, signDisplay: 'exceptZero' });
export const compactNumberFixed2 = createFormatter({ notation: 'compact', minimumFractionDigits: 2, maximumFractionDigits: 2 });

const byteAmount = createFormatter({ maximumFractionDigits: 1 });
const BYTE_UNITS = ['B', 'KB', 'MB', 'GB', 'TB'] as const;

// A byte count in the largest binary unit it reaches, one decimal under ten
// (`40960 → "40 KB"`, `1536 → "1.5 KB"`). The number follows the locale; the
// unit symbols are the same everywhere, spaced so zh-CN reads `256 MB` too.
export function formatBytes(bytes: number, locale: string): string {
  if (!Number.isFinite(bytes) || bytes < 0) return `${byteAmount(0, locale)} B`;
  let value = bytes;
  let unit = 0;
  while (value >= 1024 && unit < BYTE_UNITS.length - 1) {
    value /= 1024;
    unit += 1;
  }
  // 1023.7 KB would print as "1,024 KB"; carry it into the next unit instead.
  if (unit > 0 && unit < BYTE_UNITS.length - 1 && Math.round(value) >= 1024) {
    value /= 1024;
    unit += 1;
  }
  const shown = unit === 0 || value >= 10 ? Math.round(value) : Math.round(value * 10) / 10;
  return `${byteAmount(shown, locale)} ${BYTE_UNITS[unit]}`;
}

const _RELATIVE_STEPS: Array<[Intl.RelativeTimeFormatUnit, number]> = [
  ['year', 31536000],
  ['month', 2592000],
  ['week', 604800],
  ['day', 86400],
  ['hour', 3600],
  ['minute', 60],
];

const relativeFormats = perLocale((locale) => ({
  counts: new Intl.RelativeTimeFormat(locale, { numeric: 'auto', style: 'narrow' }),
  phrases: new Intl.RelativeTimeFormat(locale, { numeric: 'auto', style: 'long' }),
}));

/**
 * Locale-aware relative time from `now` — `"5m ago"`, `"yesterday"`,
 * `"in 3d"`, `"next month"`, `"昨天"`. Signed, so future timestamps read as
 * future; sub-minute deltas collapse to the locale's "now" phrasing.
 *
 * A count is compact (`"in 3mo"`), but a phrase is spelled out: `numeric:
 * 'auto'` words a step of one as a phrase, and the narrow style would clip
 * its words to `"next mo."`.
 *
 * Missing and unparseable inputs return `''` rather than a plausible-looking
 * "now" — every call site renders this straight into the DOM, so a bad
 * timestamp has to read as absent, not as fresh.
 */
export function relativeTime(
  d: Date | number | string | null | undefined,
  locale: string,
  now: number,
): string {
  if (d === null || d === undefined || d === '') return '';
  const ms = new Date(d).getTime();
  if (Number.isNaN(ms)) return '';
  const { counts, phrases } = relativeFormats(locale);
  const seconds = (ms - now) / 1000;
  const abs = Math.abs(seconds);
  for (const [unit, unitSeconds] of _RELATIVE_STEPS) {
    if (abs < unitSeconds) continue;
    const n = Math.round(seconds / unitSeconds);
    const isCount = counts.formatToParts(n, unit).some((p) => p.type === 'integer');
    return (isCount ? counts : phrases).format(n, unit);
  }
  return counts.format(0, 'second');
}
