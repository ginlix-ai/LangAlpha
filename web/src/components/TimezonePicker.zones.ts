import { CURRENT_NAME, currentTimezoneName } from '@/lib/deviceTimezone';
import { formatTimezoneName } from '@/lib/format';
import { utcOffsetMinutes } from '@/lib/timezones';

/** The zones shown before any search: the major markets, and at least one
 *  zone for every offset where many people live. */
export const COMMON_TIMEZONES: readonly string[] = [
  'Pacific/Honolulu',
  'America/Anchorage',
  'America/Los_Angeles',
  'America/Phoenix',
  'America/Denver',
  'America/Chicago',
  'America/Mexico_City',
  'America/New_York',
  'America/Toronto',
  'America/Sao_Paulo',
  'UTC',
  'Europe/London',
  'Europe/Paris',
  'Europe/Berlin',
  'Africa/Johannesburg',
  'Europe/Istanbul',
  'Europe/Moscow',
  'Asia/Dubai',
  'Asia/Karachi',
  'Asia/Kolkata',
  'Asia/Bangkok',
  'Asia/Singapore',
  'Asia/Hong_Kong',
  'Asia/Shanghai',
  'Asia/Taipei',
  'Asia/Seoul',
  'Asia/Tokyo',
  'Australia/Sydney',
  'Pacific/Auckland',
];

const formerNames = (id: string) => Object.keys(CURRENT_NAME).filter((old) => CURRENT_NAME[old] === id);

let everyZone: string[] | null = null;

/** Every zone this browser knows, by current names, UTC included. */
export function allTimezones(): string[] {
  if (!everyZone) {
    let ids: string[];
    try {
      ids = Intl.supportedValuesOf('timeZone');
    } catch {
      ids = [...COMMON_TIMEZONES];
    }
    everyZone = [...new Set(['UTC', ...COMMON_TIMEZONES, ...ids.map(currentTimezoneName)])];
  }
  return everyZone;
}

/** "America/Argentina/Buenos_Aires" → "Buenos Aires". */
function timezoneCity(tz: string): string {
  if (tz === 'UTC') return 'UTC';
  return (tz.split('/').pop() ?? tz).replace(/_/g, ' ');
}

export interface Zone {
  id: string;
  name: string;
  city: string;
  offset: number;
  /** Everything a search may match, folded: the long and short names
   *  (the short one often the country) in the reader's language and in
   *  English, the city, and the IANA id, old names too, since people still
   *  type Kiev or Calcutta. */
  terms: string;
}

export const foldZoneText = (s: string) => s.normalize('NFD').replace(/[̀-ͯ]/g, '').toLowerCase();

/** A zone as the picker lists it, named in `locale`. */
export function describeZone(id: string, now: Date, locale: string): Zone {
  const name = formatTimezoneName(id, locale);
  const city = timezoneCity(id);
  return {
    id,
    name,
    city,
    offset: utcOffsetMinutes(id, now),
    terms: foldZoneText(
      [
        name,
        formatTimezoneName(id, 'en-US'),
        formatTimezoneName(id, locale, 'shortGeneric'),
        formatTimezoneName(id, 'en-US', 'shortGeneric'),
        city,
        id,
        ...formerNames(id),
      ]
        .join(' ')
        .replace(/[/_]/g, ' '),
    ),
  };
}

const warmed = new Set<string>();

/** Describes every zone in small slices while the browser is idle, so the
 *  name and offset lookups a search reads are cached before the picker
 *  first opens; cold, they cost a few hundred ms at once. Returns a cancel. */
export function warmZoneSearch(locale: string): () => void {
  if (warmed.has(locale)) return () => {};
  const ids = allTimezones();
  const now = new Date();
  const idle = typeof window.requestIdleCallback === 'function';
  let i = 0;
  let handle = 0;
  const step = () => {
    const until = performance.now() + 8;
    while (i < ids.length && performance.now() < until) describeZone(ids[i++], now, locale);
    if (i < ids.length) handle = idle ? window.requestIdleCallback(step) : window.setTimeout(step, 16);
    else warmed.add(locale);
  };
  handle = idle ? window.requestIdleCallback(step) : window.setTimeout(step, 16);
  return () => (idle ? window.cancelIdleCallback(handle) : window.clearTimeout(handle));
}

export const byOffset = (a: Zone, b: Zone) =>
  a.offset - b.offset || a.name.localeCompare(b.name) || a.city.localeCompare(b.city);

/** "+8", "utc-4", "GMT+5:30" as minutes from UTC; null for anything else. */
function offsetQuery(q: string): number | null {
  const m = /^(?:utc|gmt)?([+\-−])(\d{1,2})(?::?(\d{2}))?$/.exec(q.replace(/\s+/g, ''));
  if (!m) return null;
  return (m[1] === '+' ? 1 : -1) * (Number(m[2]) * 60 + Number(m[3] ?? 0));
}

/** Zones matching a typed name, city or offset. Those where a word starts
 *  with what was typed come first, then the zones people usually mean
 *  (`preferred`, the home and common ones), then west to east. */
export function searchZones(zones: Zone[], query: string, preferred: ReadonlySet<string> = new Set()): Zone[] {
  const q = foldZoneText(query.trim());
  if (!q) return [];
  const offset = offsetQuery(q);
  const words = q.split(/\s+/);
  const leads = (z: Zone) => offset !== null || z.terms.split(/[\s-]+/).some((w) => w.startsWith(words[0]));
  const rank = (z: Zone) => (leads(z) ? 0 : 2) + (preferred.has(z.id) ? 0 : 1);
  return zones
    .filter((z) => (offset !== null ? z.offset === offset : words.every((w) => z.terms.includes(w))))
    .sort((a, b) => rank(a) - rank(b) || byOffset(a, b));
}
