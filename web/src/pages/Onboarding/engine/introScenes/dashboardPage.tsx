import type { ReactNode } from 'react';
import { Paperclip } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { useLocale } from '@/hooks/useLocale';
import { formatMoney } from '@/lib/bars/currencyDisplay';
import { clockTime, grouped2, shortWeekday } from '@/lib/format';
import { cn } from '@/lib/utils';
import { FaviconImg } from '@/pages/ChatAgent/components/charts/FaviconImg';
import { TermCard, TermChange, TermTag } from '@/pages/ChatAgent/components/charts/TermCard';
import { PAST, ease, useSceneTime } from './clock';
import { Appear, rv, signColor } from './kit';
import {
  changeAt,
  Chips,
  INDICES,
  Real,
  SESSION,
  TODAY,
  WATCH,
  Spark,
  Widget,
} from './dashboardParts';

/* The dashboard page in its default arrangement (the Morning brief preset),
   drawn in miniature: the index cards across the top, the brief and news on
   the left, the watchlist and earnings on the right, each widget the card
   the dashboard draws, with its body's own classes. Shared by the scenes
   that show the page. Quotes, headlines and symbols are invented. */
/* eslint-disable react-refresh/only-export-components -- shared parts:
   scene components plus data and style helpers */

/** The live page's script (ms), which the dashboard intro's first scene
    plays: the index lines trace in and keep taking new points, the brief
    comes in part by part, quotes tick (their times are in INDICES and WATCH,
    set to land while the camera is on them). */
export const GRID = {
  draw: 300,
  drawDur: 1100,
  brief: 700,
  briefPer: 240,
  shifts: [1900, 3000, 4100],
  shiftDur: 520,
  length: 7600,
};

/** `mins` minutes before the page's time. */
const minutesAgo = (mins: number) => new Date(TODAY.getTime() - mins * 60_000);

/* ── the index cards ───────────────────────────────────────────────────── */

/** The index card's line, at the height the dashboard draws it. */
const SPARK_H = 32;

/**
 * One index as its small card (`IndexMovementCard`): the symbol in the
 * edge with the name beside it, the price, the day's move with the session
 * at the line's end, and the day's line in the move's color. The real card
 * also prints the move in points; at the drawing's width it would wrap the
 * line. `reach`/`slid` drive the line on the live page; `fill` stretches
 * the line to a cell of fixed height, as in the edit frame.
 */
function IndexTile({
  index,
  live,
  reach,
  slid,
  fill = false,
}: {
  index: (typeof INDICES)[number];
  live: boolean;
  reach?: number;
  slid?: number;
  fill?: boolean;
}) {
  const locale = useLocale();
  const t = useSceneTime();
  const v = changeAt(index.ticks, t, live);
  return (
    <TermCard title={index.sym} sub={index.name} className={cn(fill && 'flex flex-col')}>
      <div className="term-line">
        <span className="term-big">{grouped2(index.prev * (1 + v / 100), locale)}</span>
      </div>
      <div className="term-line">
        <TermChange value={v} />
        <span className="term-push term-t3">{SESSION}</span>
      </div>
      <div className={cn('dash-spark', fill && 'min-h-0 flex-1')} style={fill ? undefined : { height: SPARK_H }}>
        <Spark
          series={index.spark}
          color={signColor(v)}
          reach={reach}
          slid={slid}
          stretch
          className="block h-full w-full"
        />
      </div>
    </TermCard>
  );
}

/** The index cards side by side, a page-wide row. On the page they stand
    alone (the markets widget draws no frame of its own); in the edit frame
    they fill its body. */
export function IndexRow({
  live = false,
  reach,
  slid,
  fill = false,
}: {
  live?: boolean;
  reach?: (i: number) => number;
  slid?: number;
  fill?: boolean;
}) {
  return (
    <div className={cn('grid grid-cols-5 gap-3', fill && 'h-full')}>
      {INDICES.map((index, i) => (
        <IndexTile
          key={index.sym}
          index={index}
          live={live}
          reach={reach?.(i)}
          slid={slid}
          fill={fill}
        />
      ))}
    </div>
  );
}

/* ── the AI brief ──────────────────────────────────────────────────────── */

/** The insight the brief card leads with, as the insight service writes one:
    a headline, a summary and its topics, each moving up or down. It agrees
    with the quotes (INDICES, WATCH) and the lead headline in NEWS. */
export const INSIGHT = {
  headline: 'Chipmakers lead a broad rally as NVDA guides above estimates',
  summary:
    'The Nasdaq 100 rose 0.7% and the S&P 500 0.4%. NVDA gained 1.6% after guiding ' +
    'data-center revenue above estimates, lifting AVGO and TSM, while Treasury yields ' +
    "slipped ahead of Friday's jobs report.",
  topics: [
    { text: 'Semiconductors', up: true },
    { text: 'AI Capex', up: true },
    { text: 'Bond Yields', up: false },
  ],
  written: minutesAgo(23),
};

/** The brief widget's card: its kind beside the title, when it was written
    at the edge's end. */
export function BriefWidget({
  edit,
  delay,
  className,
  children,
}: {
  edit?: boolean;
  delay?: number;
  className?: string;
  children: ReactNode;
}) {
  const { t } = useTranslation();
  const locale = useLocale();
  return (
    <Widget
      title={t('dashboard.widgets.insightBrief.cardTitle')}
      sub={t('dashboard.brief.typeLabel.marketUpdate')}
      meta={clockTime(INSIGHT.written, locale)}
      edit={edit}
      delay={delay}
      className={className}
    >
      <div className="widget-term">{children}</div>
    </Widget>
  );
}

/** The brief's headline and summary (`AIDailyBriefCard`), in the text face.
    `clamp` holds the summary to two lines, for a cell shorter than the
    card. */
export function BriefLede({ clamp = false }: { clamp?: boolean }) {
  return (
    <>
      <p className="dash-brief-lede">{INSIGHT.headline}</p>
      <p className={cn('dash-brief-summary', clamp && 'line-clamp-2')}>{INSIGHT.summary}</p>
    </>
  );
}

/** The brief's topics, each with the way it moved drawn beside it. */
function BriefTopics() {
  return (
    <div className="term-chips">
      {INSIGHT.topics.map((topic) => (
        <span key={topic.text} className="term-chip dash-topic">
          <TermChange value={topic.up ? 1 : -1}>{''}</TermChange>
          {topic.text}
        </span>
      ))}
    </div>
  );
}

/**
 * The brief at rest: the headline, the summary, its topics, then the
 * brief's two ways on at the foot. On the live page each part enters in
 * turn from `at`, as the card's parts do, into room the card holds for it,
 * so the news under the card stays put.
 */
function BriefCard({ at }: { at: number }) {
  const { t } = useTranslation();
  return (
    <BriefWidget delay={0.22} className="shrink-0">
      <Appear at={at} reserve>
        <p className="dash-brief-lede">{INSIGHT.headline}</p>
      </Appear>
      <Appear at={at + GRID.briefPer} reserve>
        <p className="dash-brief-summary">{INSIGHT.summary}</p>
      </Appear>
      <Appear at={at + 2 * GRID.briefPer} reserve>
        <BriefTopics />
      </Appear>
      <Appear at={at + 3 * GRID.briefPer} reserve>
        <div className="term-line dash-brief-foot">
          <span className="dash-link term-push">{t('dashboard.brief.generatePersonalized')}</span>
          <span className="dash-link term-ink">
            {t('dashboard.brief.readFullBrief')} <span aria-hidden>›</span>
          </span>
        </div>
      </Appear>
    </BriefWidget>
  );
}

/* ── news ──────────────────────────────────────────────────────────────── */

/** The market feed, newest first. `mins` is how long before the page's time
    the story ran. */
export const NEWS = [
  {
    title: 'Chip stocks rally on NVDA beat',
    source: 'Reuters',
    mins: 12,
    tickers: ['NVDA', 'AVGO', 'TSM'],
  },
  { title: 'Yields slip ahead of jobs data', source: 'Bloomberg', mins: 38, tickers: [] },
  { title: 'Oil steadies on supply relief', source: 'WSJ', mins: 64, tickers: [] },
  { title: 'Small caps lag on rate bets', source: 'CNBC', mins: 95, tickers: ['IWM'] },
  { title: 'Dollar firms before Fed minutes', source: 'FT', mins: 140, tickers: [] },
];

/** The news widget's card, the Market tab picked; its rows are the
    caller's. */
export function NewsWidget({
  edit,
  delay,
  className,
  children,
}: {
  edit?: boolean;
  delay?: number;
  className?: string;
  children: ReactNode;
}) {
  const { t } = useTranslation();
  const tabs = (['tab_top', 'tab_market', 'tab_portfolio', 'tab_watchlist'] as const).map((key) =>
    t(`dashboard.widgets.newsFeed.${key}`)
  );
  return (
    <Widget
      title={t('dashboard.widgets.newsFeed.cardTitle')}
      meta="24"
      edit={edit}
      delay={delay}
      className={className}
    >
      <div className="widget-term dash-fill dash-news">
        {/* the tabs alone: the feed's filters run on past them, and at the
            drawing's width they would wrap onto a second line */}
        <Chips labels={tabs} picked={1} />
        <table className="term-table dash-table">
          <tbody>{children}</tbody>
        </table>
      </div>
    </Widget>
  );
}

/**
 * A story on one line (`NewsFeedWidget`'s row): when it ran, the headline,
 * who ran it, and the first symbol it names. `hovered` fills the row as the
 * pointer over it does; `clip` is the row's paperclip, which the scene
 * shows itself, at the end of the headline's cell, as the real row does on
 * hover.
 */
export function NewsRow({
  item,
  hovered = false,
  clip,
}: {
  item: (typeof NEWS)[number];
  hovered?: boolean;
  clip?: ReactNode;
}) {
  const locale = useLocale();
  // The row's fill, cell by cell, as the dashboard's hover draws it.
  const cell = (edge?: 'first' | 'last') => ({
    background: hovered ? 'var(--color-bg-hover)' : 'transparent',
    borderRadius: edge === 'first' ? '4px 0 0 4px' : edge === 'last' ? '0 4px 4px 0' : undefined,
  });
  const fill = 'transition-colors duration-quick';
  return (
    <tr>
      <td className={cn('term-t3', fill)} style={cell('first')}>
        {clockTime(minutesAgo(item.mins), locale)}
      </td>
      <td className={cn('term-fill dash-attach-cell', fill)} style={cell()}>
        <span>{item.title}</span>
        {clip}
      </td>
      <td className={cn('term-t3', fill)} style={cell()}>
        <span className="dash-source">
          <FaviconImg domain="" name={item.source} size={12} />
          <span className="dash-source-name">{item.source}</span>
        </span>
      </td>
      <td className={cn('term-code', fill)} style={cell('last')}>
        {item.tickers[0] ?? ''}
        {item.tickers.length > 1 && <span className="term-t3"> +{item.tickers.length - 1}</span>}
      </td>
    </tr>
  );
}

/** A row's paperclip (`RowAttachButton`): shown while `shown`, in the
    accent while the pointer is on it. */
export function RowClip({ shown, on }: { shown: boolean; on: boolean }) {
  return (
    <span
      className="row-attach"
      style={{
        opacity: shown ? 1 : 0,
        background: on ? 'var(--color-accent-soft)' : 'transparent',
        color: on ? 'var(--color-accent-primary)' : 'var(--color-text-tertiary)',
      }}
    >
      <Paperclip className="h-3 w-3" />
    </span>
  );
}

/* ── the watchlist ─────────────────────────────────────────────────────── */

/** The portfolio widget's card on its Watchlist tab (`PortfolioWatchlistWidget`):
    the tabs, a table of the rows given, and the way to add one. */
export function WatchWidget({
  rows = WATCH.length,
  live = false,
  edit,
  delay,
  className,
}: {
  rows?: number;
  live?: boolean;
  edit?: boolean;
  delay?: number;
  className?: string;
}) {
  const { t } = useTranslation();
  return (
    <Widget
      title={t('dashboard.widgets.portfolioWatchlist.cardTitle')}
      meta={String(WATCH.length)}
      edit={edit}
      delay={delay}
      className={className}
    >
      <div className="widget-term">
        <Chips
          labels={[
            t('dashboard.widgets.portfolioWatchlist.headerWatchlist'),
            t('dashboard.widgets.portfolioWatchlist.headerHoldings'),
          ]}
        />
        <table className="term-table dash-table">
          <thead>
            <tr>
              <th>{t('dashboard.widgets.holdings.colSymbol')}</th>
              <th className="term-num">{t('dashboard.widgets.holdings.colLast')}</th>
              <th className="term-num">{t('dashboard.widgets.holdings.colChange')}</th>
            </tr>
          </thead>
          <tbody>
            {WATCH.slice(0, rows).map((row) => (
              <WatchRow key={row.sym} row={row} live={live} />
            ))}
          </tbody>
        </table>
        <div className="term-more">+ {t('dashboard.widgets.portfolioWatchlist.addSymbol')}</div>
      </div>
    </Widget>
  );
}

/** One watchlist row (`WatchlistRowItem`): the symbol, the last price, and
    the day's move. */
function WatchRow({ row, live }: { row: (typeof WATCH)[number]; live: boolean }) {
  const locale = useLocale();
  const t = useSceneTime();
  const v = changeAt(row.ticks, t, live);
  return (
    <tr>
      <td className="term-fill term-code term-strong">{row.sym}</td>
      <td className="term-num">{formatMoney(row.prev * (1 + v / 100), 'USD', locale)}</td>
      <td className="term-num">
        <TermChange value={v} />
      </td>
    </tr>
  );
}

/* ── earnings ──────────────────────────────────────────────────────────── */

/** Who reports next, from the watchlist, all after the close. */
export const EARNINGS = [
  { sym: 'MSFT', date: new Date(2026, 9, 28), eps: 3.66 },
  { sym: 'AAPL', date: new Date(2026, 9, 29), eps: 1.76 },
  { sym: 'AMZN', date: new Date(2026, 9, 29), eps: 1.57 },
];

/** The earnings widget's look-ahead window, at its default two weeks. */
const EARNINGS_DAYS = 14;

/**
 * The earnings widget's card (`EarningsCalendarWidget`): the count and the
 * window at the edge's end, then a row per report, the day, the symbol, when
 * in the session, and the estimate. The real row also names the company
 * after the session; the drawing's column has no room left for it.
 */
export function EarningsWidget({
  edit,
  delay,
  className,
}: {
  edit?: boolean;
  delay?: number;
  className?: string;
}) {
  const { t } = useTranslation();
  const locale = useLocale();
  return (
    <Widget
      title={t('dashboard.widgets.earningsCalendar.header')}
      meta={
        <>
          <span>{EARNINGS.length}</span>
          <span>{t('dashboard.widgets.earningsCalendar.windowLabel', { days: EARNINGS_DAYS })}</span>
        </>
      }
      edit={edit}
      delay={delay}
      className={className}
    >
      <div className="widget-term">
        <table className="term-table dash-table">
          <tbody>
            {EARNINGS.map((row) => (
              <tr key={row.sym}>
                <td className="term-t3">{shortWeekday(row.date, locale)}</td>
                <td className="term-code term-strong">{row.sym}</td>
                <td className="term-fill">
                  <TermTag>AMC</TermTag>
                </td>
                <td className="term-num term-t2">
                  {t('dashboard.widgets.earningsCalendar.estEps', {
                    value: formatMoney(row.eps, 'USD', locale),
                  })}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </Widget>
  );
}

/* ── the page ──────────────────────────────────────────────────────────── */

/**
 * The page under the header, as the default layout lays it out (Morning
 * brief preset): the index cards across the top, then the brief over news in
 * eight of the twelve columns and the watchlist over earnings in the other
 * four, the lower widget of each column running on to the page's foot when
 * the page has a height. `live` plays the script (the brief comes in part
 * by part, the index lines trace in and keep taking points, quotes tick);
 * otherwise it sits on the finished frame.
 */
export function PageBody({ live = false }: { live?: boolean }) {
  const now = useSceneTime();
  const t = live ? now : Number.POSITIVE_INFINITY;
  const slid = GRID.shifts.reduce((s, at) => s + ease(t, at, GRID.shiftDur), 0);
  return (
    <div className="flex min-h-0 flex-auto flex-col gap-1.5">
      <div className="intro-rv shrink-0" style={rv(0.05)}>
        <Real>
          <IndexRow
            live={live}
            reach={(i) => 100 * ease(t, GRID.draw + i * 90, GRID.drawDur)}
            slid={slid}
          />
        </Real>
      </div>
      <div className="grid min-h-0 flex-auto grid-cols-[2fr_1fr] gap-1.5">
        {/* the brief, news under it */}
        <div className="flex min-w-0 flex-col gap-1.5">
          {/* the AI brief; on the live page its parts enter in turn */}
          <BriefCard at={live ? GRID.brief : PAST} />
          <NewsWidget delay={0.3} className="flex-auto">
            {NEWS.slice(0, 4).map((item) => (
              <NewsRow key={item.title} item={item} />
            ))}
          </NewsWidget>
        </div>
        {/* the watchlist, earnings under it */}
        <div className="flex min-w-0 flex-col gap-1.5">
          <WatchWidget live={live} delay={0.32} className="shrink-0" />
          <EarningsWidget delay={0.4} className="flex-auto" />
        </div>
      </div>
    </div>
  );
}
