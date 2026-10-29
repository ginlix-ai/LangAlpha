import { useTranslation } from 'react-i18next';
import { useLocale } from '@/hooks/useLocale';
import { formatTook } from '@/lib/elapsed';
import { clockTime, fixed2, formatTimezoneName, relativeTime, signedFixed2 } from '@/lib/format';
import {
  cronToHuman,
  DEFAULT_CRON,
  parseSchedule,
  scheduleDays,
} from '@/pages/Automations/utils/cron';
import {
  distanceLabel,
  formatPriceTrigger,
  formatRetriggerMode,
  meterReading,
} from '@/pages/Automations/utils/price';
import { formatDateTimeShort, formatUpcoming } from '@/pages/Automations/utils/time';
import type { PriceTriggerConfig } from '@/types/automation';

/* The account the automations scenes show, and the lines they print about
   it from the Automations page's own formatters: a weekday 8:00 brief and a
   Friday review; a price alert on NVDA, which last traded at 182.42 (+1.61%
   on a 179.53 close, as the dashboard scenes quote it); an alert at $180
   that fired this morning; and a one-time AVGO earnings preview that ran
   at 6:30 and is done. The reader's clock reads 10:14 on a Wednesday, and
   8:06 the next morning once a scene moves on a day. */
const NOW = new Date(2026, 9, 21, 10, 14).getTime();
export const NEXT_MORNING = new Date(2026, 9, 22, 8, 6).getTime();
export const on = (day: number, hour: number, minute: number) =>
  new Date(2026, 9, day, hour, minute);
const BRIEF_CRON = '0 8 * * 1-5';
const REVIEW_CRON = '0 22 * * 5';
const MARKET_ZONE = 'America/New_York';
export const NVDA = { price: 182.42, previous_close: 179.53 };
/** The level the price alert fires at. */
export const ALERT_LEVEL = 190;
const ALERT: PriceTriggerConfig = {
  symbol: 'NVDA',
  conditions: [{ type: 'price_above', value: ALERT_LEVEL }],
  retrigger: { mode: 'one_shot' },
};
/** The schedule chip's strip on a form not set to a schedule: the
    builder's own starting days. */
export const DEFAULT_DAYS = scheduleDays(parseSchedule(DEFAULT_CRON));

export const BRIEF_INSTRUCTION =
  'Provide a pre-market briefing covering: overnight market moves, key economic data releases today, notable earnings reports, and sector trends to watch.';
export const ALERT_INSTRUCTION =
  'Analyze recent news and market sentiment for {symbol} to explain the price movement. Include key catalysts, analyst reactions, and short-term outlook.';
export const BRIEF_EXCERPT =
  'Chipmakers lead a broad rally as NVDA guides above estimates. Futures point higher, with AVGO and TSM up premarket, while Treasury yields slip ahead of Friday’s jobs report.';
export const LAST_EXCERPT =
  'Futures are flat ahead of a quiet data day. Semis stay bid, with AVGO and NVDA near their highs, while oil slips for a third session.';
export const NEXT_EXCERPT =
  'Semis extend gains, SOXX up 1.2% premarket as NVDA holds above $182. Jobless claims land at 8:30, and TSM reports monthly sales after the close.';
export const FIRED = {
  name: 'NVDA crosses $180',
  excerpt:
    'NVDA crossed $180.00 at 10:12, up 1.6% on the day after guiding data-center revenue above estimates. Two brokers raised their targets, and AVGO and TSM followed it higher.',
};
export const PREVIEW = {
  name: 'AVGO earnings preview',
  place: 'Semis research',
  excerpt:
    'AVGO reports Thursday, Dec 10, after the close. The Street expects AI revenue to beat again, and options price a 7% move. The custom-accelerator backlog is the number to watch.',
};

/** The lines the scenes print, from the page's own formatters, as they
    read at `now`. */
export function useBook(now = NOW) {
  const { t } = useTranslation();
  const locale = useLocale();
  const reading = meterReading(ALERT, NVDA);
  const briefLine = cronToHuman(BRIEF_CRON, t, locale);
  const zone = formatTimezoneName(MARKET_ZONE, locale);
  const nextBrief = on(new Date(now).getDate() + 1, 8, 0);
  const nextReview = on(23, 22, 0);
  return {
    brief: t('automation.templateMorningBriefing'),
    alert: t('automation.templatePriceAlert'),
    briefLine,
    briefReadout: `${briefLine} · ${zone}`,
    briefSentence: t('automation.sentenceThen', {
      first: t('automation.sentenceRecurring', { schedule: briefLine, zone }),
      then: t('automation.sentenceNextRun', {
        when: formatUpcoming(nextBrief, locale, now),
        relative: relativeTime(nextBrief, locale, now),
      }),
    }),
    nextBrief: relativeTime(nextBrief, locale, now),
    review: t('automation.templateWeeklyReview'),
    reviewLine: cronToHuman(REVIEW_CRON, t, locale),
    nextReview: relativeTime(nextReview, locale, now),
    /** What runs next, soonest first. */
    upNext: [
      [nextBrief, t('automation.templateMorningBriefing')],
      [nextReview, t('automation.templateWeeklyReview')],
    ] as [Date, string][],
    zone,
    alertLine: formatPriceTrigger(ALERT, t),
    alertSentence: t('automation.sentenceWatching', {
      condition: formatPriceTrigger(ALERT, t),
      retrigger: formatRetriggerMode(ALERT, t),
    }),
    /** The page header's census line; the watching part waits for a saved alert. */
    census: (watching = true) =>
      [
        t('automation.censusScheduled', { count: 2 }),
        ...(watching ? [t('automation.censusWatching', { count: 1 })] : []),
      ].join(' · '),
    away: distanceLabel(reading, t, locale),
    price: fixed2(NVDA.price, locale),
    move: signedFixed2(((NVDA.price - NVDA.previous_close) / NVDA.previous_close) * 100, locale),
    time: (hour: number, minute: number) => clockTime(on(21, hour, minute), locale),
    ago: (d: Date) => relativeTime(d, locale, now),
    upcoming: (d: Date) => formatUpcoming(d, locale, now),
    moment: (d: Date) => formatDateTimeShort(d, locale),
    took: (ms: number) => formatTook(ms, (key, opts) => t(key, opts)),
    deliverTo: t('automation.deliverToDm', { app: t('automation.deliverToSlack') }),
  };
}
