/**
 * How a wakeup reads, over the fields a listed wakeup and a notice of it
 * share: what sets it off, its price condition, and its times, all in the
 * user's zone. The tray chip and the Status panel compose their lines here;
 * the transcript's rows and the quiet fold compose theirs beside their
 * components from the same parts, so the two say one wakeup the same way.
 * Pure: the caller hands in `t`, the locale, the time and the zone.
 */
import type { TFunction } from 'i18next';
import { createDateFormatter, createFormatter, relativeTime } from '@/lib/format';
import { currentTimezoneName, deviceTimezone } from '@/lib/deviceTimezone';
import { epochMs } from '@/lib/elapsed';
import { cronToHuman } from '../../Automations/utils/cron';
import { formatDateTimeShort, formatUpcoming } from '../../Automations/utils/time';
import type { Wakeup, WakeupKind, WakeupOrder, WakeupPrice, WakeupState } from '@/types/wakeups';

// A level as the agent set it (205, 190.5), a traded price with its cents
// (205.12), and a move with its sign (+3%, -2.5%).
const levelFormat = createFormatter({ maximumFractionDigits: 4 });
export const priceFormat = createFormatter({ minimumFractionDigits: 2, maximumFractionDigits: 4 });
const moveFormat = createFormatter({ style: 'percent', signDisplay: 'always', maximumFractionDigits: 2 });
const qtyFormat = createFormatter({ maximumFractionDigits: 6 });

const currencyFormats = new Map<string, (n: number, locale: string) => string>();
/** A price in the order's own currency, plain when none is named or the code
 *  is not one Intl knows. */
function orderPrice(n: number, currency: string | null | undefined, locale: string): string {
  if (!currency) return priceFormat(n, locale);
  let format = currencyFormats.get(currency);
  if (!format) {
    try {
      format = createFormatter({ style: 'currency', currency, minimumFractionDigits: 2, maximumFractionDigits: 4 });
      format(1, 'en-US');
    } catch {
      format = priceFormat;
    }
    currencyFormats.set(currency, format);
  }
  return format(n, locale);
}

/** zh names for the exchange cities; any other zone reads as its English city. */
const ZH_CITIES: Record<string, string> = {
  New_York: '纽约', Chicago: '芝加哥', Toronto: '多伦多', London: '伦敦', Berlin: '柏林',
  Frankfurt: '法兰克福', Paris: '巴黎', Zurich: '苏黎世', Amsterdam: '阿姆斯特丹',
  Madrid: '马德里', Rome: '罗马', Hong_Kong: '香港', Shanghai: '上海', Tokyo: '东京',
  Seoul: '首尔', Taipei: '台北', Singapore: '新加坡', Kolkata: '加尔各答', Sydney: '悉尼',
};

/** An IANA zone's city: `America/New_York` is New York, or 纽约 in zh. */
export function zoneCity(zone: string, locale: string): string {
  const segment = zone.split('/').at(-1) ?? zone;
  if (locale.toLowerCase().startsWith('zh') && ZH_CITIES[segment]) return ZH_CITIES[segment];
  return segment.replaceAll('_', ' ');
}

/** The zone as Intl takes it, or undefined for one it does not know. An offset
 *  the agent set an `at` in arrives as `UTC-04:00`. */
function intlZone(zone: string | null | undefined): string | undefined {
  if (!zone) return undefined;
  for (const candidate of [zone, zone.replace(/^UTC(?=[+-])/, '')]) {
    try {
      new Intl.DateTimeFormat('en-US', { timeZone: candidate });
      return candidate;
    } catch { /* try the next spelling */ }
  }
  return undefined;
}

const clockByZone = new Map<string, (d: Date, locale: string) => string>();
const dayByZone = new Map<string, Intl.DateTimeFormat>();

/** The clock in `zone`, or on this device's clock without one. */
export function zonedClock(d: Date, locale: string, zone: string | undefined): string {
  const key = zone ?? '';
  let format = clockByZone.get(key);
  if (!format) {
    format = createDateFormatter(zone ? { hour: 'numeric', minute: '2-digit', timeZone: zone } : { hour: 'numeric', minute: '2-digit' });
    clockByZone.set(key, format);
  }
  return format(d, locale);
}

/** Whether two moments fall on one day in `zone`, or on this device's
 *  calendar without one: the one "today reads as the clock alone" rule. */
export function sameDay(a: Date, b: Date, zone: string | undefined): boolean {
  const key = zone ?? '';
  let format = dayByZone.get(key);
  if (!format) {
    format = new Intl.DateTimeFormat('en-CA', zone ? { timeZone: zone } : undefined);
    dayByZone.set(key, format);
  }
  return format.format(a) === format.format(b);
}

const WEEK_MS = 7 * 86_400_000;

/**
 * A moment in `zone`: the clock alone today, the weekday with it within the
 * week ahead, else the date. A moment more than a week gone reads by its date
 * too, since a weekday would name the coming one. No zone reads this device's.
 */
export function whenLabel(iso: string | null | undefined, zone: string | null | undefined, locale: string, now: number): string {
  const ms = epochMs(iso);
  if (ms === null) return '';
  const inZone = intlZone(zone);
  const d = new Date(ms);
  if (sameDay(d, new Date(now), inZone)) return zonedClock(d, locale, inZone);
  if (ms < now - WEEK_MS) return formatDateTimeShort(d, locale, inZone);
  return formatUpcoming(d, locale, now, inZone);
}

/** Whether `zone` is the user's own, a retired name matching the one that
 *  replaced it. No user zone is this device's, as their times read. */
function isHomeZone(zone: string, userTimezone: string | null): boolean {
  return currentTimezoneName(zone) === currentTimezoneName(userTimezone ?? deviceTimezone());
}

/** The compact condition: `≥ 205`, `≤ 190.5`, `+3%`, `-3%`. */
export function conditionLabel(kind: WakeupPrice['kind'], value: number, locale: string): string {
  switch (kind) {
    case 'above':
      return `≥ ${levelFormat(value, locale)}`;
    case 'below':
      return `≤ ${levelFormat(value, locale)}`;
    case 'pct_up':
      return moveFormat(value / 100, locale);
    case 'pct_down':
      return moveFormat(-value / 100, locale);
    default:
      return kind satisfies never;
  }
}

/** A price condition in full: a move says what it is measured from. */
export function priceConditionLabel(
  price: Pick<WakeupPrice, 'kind' | 'value' | 'pct_from' | 'armed_price'>,
  t: TFunction,
  locale: string,
): string {
  const condition = conditionLabel(price.kind, price.value, locale);
  if (price.kind === 'above' || price.kind === 'below') return condition;
  if (price.pct_from === 'now') {
    return price.armed_price !== null
      ? t('chat.marketWatch.pctFromPrice', { move: condition, price: priceFormat(price.armed_price, locale) })
      : t('chat.marketWatch.pctFromSet', { move: condition });
  }
  return t('chat.marketWatch.pctFromClose', { move: condition });
}

/** The fields a listed wakeup and a notice of it share that say what sets it
 *  off. `at` is the moment a one-time wakeup is for; a notice of one names
 *  the moment it fired, which is the same. */
export interface TriggerFields {
  kind: WakeupKind;
  at?: string | null;
  cron?: string | null;
  timezone?: string | null;
  price?: WakeupPrice | null;
}

/**
 * What sets a timed or a price wakeup off, in words, with its times in the
 * user's zone. A schedule kept in another zone says which. '' for an order,
 * whose words each surface composes with what it knows of the order, and for
 * a trigger missing the field it needs.
 */
export function triggerText(w: TriggerFields, t: TFunction, locale: string, now: number, userTimezone: string | null): string {
  switch (w.kind) {
    case 'at':
      return w.at ? t('chat.wakeups.labelAt', { time: whenLabel(w.at, userTimezone, locale, now) }) : '';
    case 'cron': {
      if (!w.cron) return '';
      const schedule = cronToHuman(w.cron, t, locale);
      return w.timezone && !isHomeZone(w.timezone, userTimezone)
        ? t('chat.wakeups.labelRepeatZone', { schedule, zone: zoneCity(w.timezone, locale) })
        : t('chat.wakeups.labelRepeat', { schedule });
    }
    case 'price':
      return w.price ? `${w.price.symbol} ${priceConditionLabel(w.price, t, locale)}` : '';
    case 'order':
      return '';
    default:
      return w.kind satisfies never;
  }
}

/** Wakeups that can still fire, or whose fire has not reached the agent. */
export function isPending(w: Wakeup): boolean {
  return w.state === 'armed' || w.state === 'held' || w.state === 'held_usage';
}

/** The pending wakeups the chip counts, in listing order. */
export function pendingWakeups(wakeups: readonly Wakeup[] | null | undefined): Wakeup[] {
  return (wakeups ?? []).filter(isPending);
}

/** The soonest next fire among `wakeups`, or null when none is timed. */
export function nextFireAt(wakeups: readonly Wakeup[]): string | null {
  let best: string | null = null;
  let bestMs = Infinity;
  for (const w of wakeups) {
    const ms = epochMs(w.next_fire_at);
    if (ms !== null && ms < bestMs) {
      best = w.next_fire_at;
      bestMs = ms;
    }
  }
  return best;
}

function lapsesAt(w: Wakeup): number | null {
  if (w.state !== 'armed' || (w.kind !== 'price' && w.kind !== 'order')) return null;
  return epochMs(w.expires_at);
}

/**
 * The wakeups as they stand at `now`. The server settles a price or order
 * wakeup past its window on its next sweep, and the view re-reads the list
 * only on a change it is told of, so one past its window would go on reading
 * armed, with a live Cancel. It reads lapsed here first. The same list comes
 * back when none has lapsed.
 */
export function wakeupsAt(wakeups: Wakeup[], now: number): Wakeup[] {
  const lapsed = (w: Wakeup) => { const ms = lapsesAt(w); return ms !== null && ms <= now; };
  if (!wakeups.some(lapsed)) return wakeups;
  return wakeups.map((w) => (lapsed(w)
    ? { ...w, state: (w.kind === 'price' ? 'lapsed' : 'expired') as WakeupState, cancellable: false, next_fire_at: null }
    : w));
}

/** When the next armed price or order wakeup lapses after `now`, or null. */
export function nextWakeupLapse(wakeups: readonly Wakeup[] | null | undefined, now: number): number | null {
  let next: number | null = null;
  for (const w of wakeups ?? []) {
    const ms = lapsesAt(w);
    if (ms !== null && ms > now && (next === null || ms < next)) next = ms;
  }
  return next;
}

/** The chip's words: how many are pending, and the next time when one is timed. */
export function chipLabel(
  pending: readonly Wakeup[],
  t: TFunction,
  locale: string,
  now: number,
  userTimezone: string | null,
): string {
  const count = pending.length;
  const next = nextFireAt(pending);
  if (!next) return count === 1 ? t('chat.wakeups.chip_one_untimed') : t('chat.wakeups.chip_other_untimed', { count });
  const time = whenLabel(next, userTimezone, locale, now);
  return count === 1 ? t('chat.wakeups.chip_one', { time }) : t('chat.wakeups.chip_other', { count, time });
}

/** What sets the wakeup off, for the line under its name. */
export function triggerLabel(w: Wakeup, t: TFunction, locale: string, now: number, userTimezone: string | null): string {
  return w.kind === 'order'
    ? t('chat.wakeups.labelOrder', { order: w.order?.summary ?? '' })
    : triggerText(w, t, locale, now, userTimezone);
}

/** The row's name: the agent's label, else what sets it off. */
export function wakeupTitle(w: Wakeup, t: TFunction, locale: string, now: number, userTimezone: string | null): string {
  return w.label ? t('chat.wakeups.rowTitle', { label: w.label }) : triggerLabel(w, t, locale, now, userTimezone);
}

/** An order's ending in words, for a row or a chat line. */
export function orderOutcomeLabel(order: WakeupOrder, t: TFunction, locale: string): string | null {
  const name = order.summary;
  const filled = order.filled_qty ?? 0;
  const qty = order.qty ?? 0;
  const quantities = { order: name, filled: qtyFormat(filled, locale), qty: qtyFormat(qty, locale) };
  switch (order.outcome) {
    case 'filled':
      return order.avg_fill_price != null
        ? t('chat.wakeups.orderFilled', { order: name, price: orderPrice(order.avg_fill_price, order.currency, locale) })
        : null;
    case 'partial':
      return t('chat.wakeups.orderPartial', quantities);
    case 'partial_cancelled':
      return t('chat.wakeups.orderPartialCancelled', quantities);
    case 'cancelled':
      return t('chat.wakeups.orderCancelled', { order: name });
    case 'rejected':
      return t('chat.wakeups.orderRejected', { order: name });
    case 'failed':
      return t('chat.wakeups.orderFailed', { order: name });
    case 'partial_expired':
      return t('chat.wakeups.orderPartialExpired', quantities);
    case 'expired':
      return t('chat.wakeups.orderExpired', { order: name });
    case 'confirmed':
      return t('chat.wakeups.orderConfirmed', { order: name });
    case 'handed_off':
      return t('chat.wakeups.orderHandedOff', { order: name });
    default:
      return null;
  }
}

function firedLabel(w: Wakeup, t: TFunction, locale: string, now: number, userTimezone: string | null): string {
  const base = t('chat.wakeups.stateFired', { time: whenLabel(w.fired_at, userTimezone, locale, now) });
  if (w.price?.fired_price != null) return `${base} · ${priceFormat(w.price.fired_price, locale)}`;
  const outcome = w.order ? orderOutcomeLabel(w.order, t, locale) : null;
  return outcome ? `${base} · ${outcome}` : base;
}

/** Where the wakeup stands, for the line under its name. */
export function stateLabel(w: Wakeup, t: TFunction, locale: string, now: number, userTimezone: string | null): string {
  switch (w.state) {
    case 'armed': {
      if (w.kind === 'price' || w.kind === 'order') {
        const when = relativeTime(w.expires_at, locale, now) || whenLabel(w.expires_at, userTimezone, locale, now);
        return t('chat.wakeups.stateLapses', { when });
      }
      const time = whenLabel(w.next_fire_at, userTimezone, locale, now);
      return w.fires_total > 1
        ? t('chat.wakeups.stateProgress', { done: w.fires_done, total: w.fires_total, time })
        : t('chat.wakeups.stateNext', { time });
    }
    case 'held':
      return t('chat.wakeups.stateHeld');
    case 'held_usage':
      return t('chat.wakeups.stateHeldUsage');
    case 'fired':
      return firedLabel(w, t, locale, now, userTimezone);
    case 'finished':
      return t('chat.wakeups.stateFinished');
    case 'lapsed':
      return t('chat.wakeups.stateLapsed');
    case 'expired':
      return t('chat.wakeups.stateExpired');
    case 'cancelled':
      return t('chat.wakeups.stateCancelled');
    default:
      return w.state satisfies never;
  }
}
