/**
 * How a wakeup reads in the transcript: the row a wake turn opens with, the
 * row a notice delivered into a running turn draws, and the quiet checks'
 * fold. Composed from the trigger words the Status panel reads
 * (`wakeupLabels.ts`), in the same zone. Pure: the caller hands in `t`, the
 * locale, the time and the zone.
 */
import type { TFunction } from 'i18next';
import { epochMs } from '@/lib/elapsed';
import { clockTime, formatList } from '@/lib/format';
import { formatDateTimeShort } from '../../../Automations/utils/time';
import type { WakeupArrival } from '@/types/chat';
import type { WakeNotice } from '@/types/wakeups';
import {
  conditionLabel, orderOutcomeLabel, priceFormat, sameDay, triggerText, zonedClock,
} from '../wakeupLabels';
import type { QuietCheck } from './quietChecks';

/** What happened to an order, else the order itself, else the notice's own
 *  name: a notice may arrive without its order. */
function orderText(notice: WakeNotice, t: TFunction, locale: string): string {
  const order = notice.order;
  if (!order) return notice.label ?? notice.short_id;
  return orderOutcomeLabel(order, t, locale) ?? t('chat.wakeups.labelOrder', { order: order.summary });
}

/** What set a notice off, in words, for one the agent gave no label. */
function noticeTrigger(notice: WakeNotice, t: TFunction, locale: string, now: number, zone: string): string {
  if (notice.kind === 'order') return notice.order ? orderText(notice, t, locale) : '';
  return triggerText({ ...notice, at: notice.fired_at || null }, t, locale, now, zone);
}

/** One notice's name: the agent's label, else what set it off, and which
 *  check of a series it is. */
export function noticeName(notice: WakeNotice, t: TFunction, locale: string, now: number, zone: string): string {
  const name = notice.label?.trim() || noticeTrigger(notice, t, locale, now, zone) || notice.short_id;
  if (notice.total <= 1) return name;
  const check = t('chat.wakeups.check', { n: notice.seq, total: notice.total });
  return name ? `${name} · ${check}` : check;
}

/** `205.12 at 1:05 PM`, or whichever half is known. */
function pricedAt(price: number | null, at: string | null, t: TFunction, locale: string): string {
  const ms = epochMs(at);
  const priced = price !== null ? priceFormat(price, locale) : null;
  if (priced && ms !== null) return t('chat.marketAlert.pricedAt', { price: priced, time: clockTime(ms, locale) });
  if (ms !== null) return t('chat.marketAlert.at', { time: clockTime(ms, locale) });
  return priced ?? '';
}

/** A price notice as a sentence, `NVDA crossed above 205.00 · 205.12 at
 *  1:05 PM`, or '' for one too old to name its condition. */
function priceSentence(notice: WakeNotice, t: TFunction, locale: string): string {
  const price = notice.price;
  if (!price) return '';
  const { symbol, kind, value } = price;
  if (notice.notice === 'lapse') {
    const condition = conditionLabel(kind, value, locale);
    const ms = epochMs(price.expires_at);
    return ms !== null
      ? t('chat.marketAlert.expiring', { symbol, condition, time: clockTime(ms, locale) })
      : t('chat.marketAlert.expiringSoon', { symbol, condition });
  }
  let what: string;
  if (kind === 'above' || kind === 'below') {
    const level = priceFormat(value, locale);
    what = kind === 'above'
      ? t('chat.marketAlert.crossedAbove', { symbol, level })
      : t('chat.marketAlert.crossedBelow', { symbol, level });
  } else {
    const move = conditionLabel(kind, value, locale);
    what = price.pct_from === 'now'
      ? t('chat.marketAlert.movedFromSet', { symbol, move })
      : t('chat.marketAlert.movedFromClose', { symbol, move });
  }
  const detail = pricedAt(price.fired_price, notice.fired_at || null, t, locale);
  return detail ? `${what} · ${detail}` : what;
}

/**
 * Whether the row reads as a price alert, as one always has: every notice a
 * price's that names its condition, or a target's notice from before
 * wakeups, which reads as the generic alert even when it names nothing.
 */
export function isPriceArrival(arrival: WakeupArrival): boolean {
  if (arrival.legacy) return true;
  return arrival.notices.length > 0 && arrival.notices.every((n) => n.kind === 'price' && n.price);
}

/** A price alert's row text. A notice that names no condition says nothing
 *  specific, so the row falls back to the generic label rather than to the
 *  model's text. */
export function priceAlertText(arrival: WakeupArrival, t: TFunction, locale: string): string {
  const text = arrival.notices.map((n) => priceSentence(n, t, locale)).filter(Boolean).join('; ');
  if (arrival.source === 'wake') {
    return text ? t('chat.marketAlert.woke', { alert: text }) : t('chat.marketAlert.wokeGeneric');
  }
  return text || t('chat.marketAlert.generic');
}

/**
 * A wakeup row's text, when it is not a price alert's: a wake names every
 * notice that started the turn, and an order's update says what happened to
 * it; a delivered row names its one notice.
 */
export function wakeupRowText(arrival: WakeupArrival, t: TFunction, locale: string, now: number, zone: string): string {
  const { notices } = arrival;
  if (notices.length === 0) return t('toolArtifact.tool.scheduleWakeup');
  const names = notices.map((n) => (n.kind === 'price' && n.price
    ? priceSentence(n, t, locale)
    : n.kind === 'order' ? orderText(n, t, locale) : noticeName(n, t, locale, now, zone)));
  const label = formatList(names, locale);
  if (arrival.source === 'delivered') return t('chat.wakeups.arrived', { label });
  return notices.every((n) => n.kind === 'order')
    ? t('chat.wakeups.wokeOrder', { label })
    : t('chat.wakeups.woke', { labels: label });
}

/** A check's time, on this device's clock as the rest of the transcript's
 *  are: the clock today, the date with it before. */
export function checkTime(ms: number | undefined, locale: string, now: number): string {
  if (ms === undefined) return '';
  const d = new Date(ms);
  return sameDay(d, new Date(now), undefined) ? zonedClock(d, locale, undefined) : formatDateTimeShort(d, locale);
}

/** When a check ran: its turn's start, else when its first notice fired. */
export function checkAt(check: QuietCheck): number | undefined {
  return check.startedAt ?? epochMs(check.notices[0]?.fired_at) ?? undefined;
}

/** One quiet check, as its fold lists it. */
export function quietEntryText(check: QuietCheck, t: TFunction, locale: string, now: number, zone: string): string {
  return t('chat.wakeups.quietEntry', {
    time: checkTime(checkAt(check), locale, now),
    labels: formatList(check.notices.map((n) => noticeName(n, t, locale, now, zone)), locale),
    status: check.status,
  });
}

/** The fold's header: how many checks found nothing, and the last one's time. */
export function quietHeaderText(checks: readonly QuietCheck[], t: TFunction, locale: string, now: number): string {
  const last = checks.at(-1);
  const time = last ? checkTime(checkAt(last), locale, now) : '';
  return checks.length === 1
    ? t('chat.wakeups.quiet_one', { time })
    : t('chat.wakeups.quiet_other', { count: checks.length, time });
}
