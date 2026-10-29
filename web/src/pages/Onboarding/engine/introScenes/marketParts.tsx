import type { ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import {
  BarChart3,
  ChevronDown,
  ChevronLeft,
  Info,
  Menu,
  MessageSquare,
  RefreshCw,
  Settings2,
  SlidersHorizontal,
} from 'lucide-react';
import { useLocale } from '@/hooks/useLocale';
import { fixed2, signedFixed2 } from '@/lib/format';
import { cn } from '@/lib/utils';
import { ClearButton, Intervals, Segmented, SelectionTools, ToolBtn } from './chartTools';
import { InputDock, signColor } from './kit';
import { NVDA_LAST, NVDA_QUOTE } from './nvdaSeries';
import { Transcript } from './transcriptParts';

/* MarketView as the market intro's camera sees it, after pages/MarketView:
   the stock header and its metrics over the chart and its toolbar, the
   folded sidebar's strip, and the chat panel with its empty state,
   selection chips and composer. The page is drawn whole at its own size and
   the scenes frame parts of it, so the scripted pointer lands where the
   parts are. */
/* eslint-disable react-refresh/only-export-components -- shared parts:
   page components plus layout constants */

/* ── layout, in the page's px ──────────────────────────────────────────── */

const HEAD_H = 28;
const METRICS_H = 40;
const TOOLS_H = 20;
const CHART_W = 260;
const STRIP_W = 15;
const CHAT_W = 170;
export const PAGE = { width: CHART_W + STRIP_W + CHAT_W, height: 318 };
/** The chart, under the header, the metrics and the toolbar. */
export const CHART = {
  x: 0,
  y: HEAD_H + METRICS_H + TOOLS_H,
  w: CHART_W,
  h: PAGE.height - HEAD_H - METRICS_H - TOOLS_H,
};
/** The chat panel's left edge. */
export const CHAT_X = CHART_W + STRIP_W;

/** The toolbar's right group, laid from the right: the mode switch, the menu,
    the tools, then Price level and Region. */
const MODE_W = 48;
const BTN = 11;
const GAP = 2;
const PAD_X = 5;
/** The Region button's center. */
export const REGION_BTN = {
  x: CHART_W - PAD_X - MODE_W - 3 * (BTN + GAP) - GAP - BTN / 2,
  y: HEAD_H + METRICS_H + TOOLS_H / 2 - 0.5,
};

/** The chat composer's send button. */
export const SEND = { x: PAGE.width - 21, y: PAGE.height - 21 };

/* ── the header band: stock header, the strip, chat header ──────────── */

/** The stock header's top (`StockHeader`): the symbol switcher, the names,
    the exchange and the live dot over the Company Overview pill; the price
    and the day's change on the right. */
function StockTop() {
  const { t } = useTranslation();
  const locale = useLocale();
  const q = NVDA_QUOTE;
  const change = NVDA_LAST - q.prev;
  return (
    <div className="flex min-w-0 flex-1 items-center gap-2 px-2">
      <div className="flex min-w-0 flex-1 flex-col gap-[3px]">
        <div className="flex min-w-0 items-center gap-[5px] leading-none">
          <span
            className="flex shrink-0 items-center gap-[1px] rounded-[2px] px-[1px] text-[9px] font-semibold"
            style={{ color: 'var(--color-text-primary)' }}
          >
            {q.sym}
            <ChevronDown className="h-[6px] w-[6px]" style={{ color: 'var(--color-icon-muted)' }} />
          </span>
          <span
            className="min-w-0 truncate text-[7px]"
            style={{ color: 'var(--color-text-secondary)' }}
          >
            {q.name}
          </span>
          <span
            className="shrink-0 rounded-[2px] px-[3px] py-[1.5px] text-[5px]"
            style={{ background: 'var(--color-bg-tag)', color: 'var(--color-text-secondary)' }}
          >
            NASDAQ
          </span>
          <span
            className="flex shrink-0 items-center gap-[2px] text-[5px]"
            style={{ color: 'var(--color-text-secondary)' }}
          >
            <span
              className="h-[3px] w-[3px] rounded-full"
              style={{ background: 'var(--color-profit)' }}
            />
            {t('marketView.quote.live')}
          </span>
        </div>
        <span
          className="flex w-fit items-center gap-[2px] rounded-[3px] border px-[4px] py-[1.5px] text-[5px] leading-none"
          style={{
            borderColor: 'var(--color-border-default)',
            color: 'var(--color-text-secondary)',
          }}
        >
          <Info className="h-[5px] w-[5px]" />
          {t('marketView.companyOverview')}
        </span>
      </div>
      <div className="flex shrink-0 flex-col items-end gap-[2px] leading-none tabular-nums">
        <span className="text-[11px] font-semibold" style={{ color: signColor(change) }}>
          {fixed2(NVDA_LAST, locale)}
          <span
            className="ml-[2px] text-[5px] font-normal"
            style={{ color: 'var(--color-text-secondary)' }}
          >
            USD
          </span>
        </span>
        <span className="text-[6px] font-medium" style={{ color: signColor(change) }}>
          {signedFixed2(change, locale)} {signedFixed2((change / q.prev) * 100, locale)}%
        </span>
      </div>
    </div>
  );
}

/** The chat panel's header: the thread's title (the first message, or New
    chat) with its dot and chevron, and Open in Chat once there is a thread. */
function ChatHeader({ width, title }: { width: number; title?: string }) {
  const { t } = useTranslation();
  return (
    <div
      className="flex shrink-0 items-center gap-1 border-l px-1.5"
      style={{ width, borderColor: 'var(--color-border-muted)' }}
    >
      <span className="flex min-w-0 items-center gap-[3px] leading-none">
        <span
          className="h-[3px] w-[3px] shrink-0 rounded-full"
          style={{
            background: title ? 'var(--color-accent-primary)' : 'var(--color-text-tertiary)',
            opacity: title ? 1 : 0.35,
          }}
        />
        <span
          className="truncate text-[7px] font-medium"
          style={{ color: 'var(--color-text-primary)' }}
        >
          {title ?? t('marketView.chatHistory.newChat')}
        </span>
        <ChevronDown
          className="h-[6px] w-[6px] shrink-0"
          style={{ color: 'var(--color-text-tertiary)' }}
        />
      </span>
      {title && (
        <span
          className={cn(
            'intro-pop ml-auto flex shrink-0 items-center gap-[2px] rounded-[3px] border',
            'px-[4px] py-[2px] text-[6px] font-medium leading-none'
          )}
          style={{
            borderColor: 'var(--color-border-default)',
            color: 'var(--color-text-secondary)',
          }}
        >
          <MessageSquare className="h-[6px] w-[6px]" />
          {t('marketView.chatPanel.openInChat')}
        </span>
      )}
    </div>
  );
}

/* ── the stock header's metrics and the chart toolbar ──────────────────── */

/** The header's metrics grid: two rows of five, label over value. */
function Metrics() {
  const { t } = useTranslation();
  const locale = useLocale();
  const q = NVDA_QUOTE;
  const last = NVDA_LAST;
  const pct = ((last - q.prev) / q.prev) * 100;
  const cells: [string, string, string?][] = [
    [t('marketView.header.prevClose'), fixed2(q.prev, locale)],
    [t('marketView.header.open'), fixed2(q.open, locale)],
    [t('marketView.header.low'), fixed2(Math.min(q.low, last), locale)],
    [t('marketView.header.high'), fixed2(Math.max(q.high, last), locale)],
    [t('marketView.header.high52w'), fixed2(q.hi52, locale)],
    [t('marketView.header.low52w'), fixed2(q.lo52, locale)],
    [t('marketView.header.avgVolume3m'), q.avgVol],
    [t('marketView.header.volume'), q.vol],
    [
      t('marketView.header.dayRange'),
      `${fixed2(Math.min(q.low, last), locale)} – ${fixed2(Math.max(q.high, last), locale)}`,
    ],
    [t('marketView.header.changePct'), `${signedFixed2(pct, locale)}%`, signColor(pct)],
  ];
  return (
    <div
      // columns take their widest value, so the day's range never runs into
      // its neighbour in a column this narrow
      className="grid shrink-0 grid-cols-[repeat(5,auto)] content-center justify-between gap-x-[3px] gap-y-[4px] border-b px-2"
      style={{
        height: METRICS_H,
        borderColor: 'var(--color-border-default)',
        background: 'var(--color-bg-card)',
      }}
    >
      {cells.map(([label, value, color]) => (
        <span key={label} className="flex min-w-0 flex-col gap-[2px] leading-none">
          <span
            className="whitespace-nowrap text-[4.5px] uppercase"
            style={{ color: 'var(--color-text-secondary)' }}
          >
            {label}
          </span>
          <span
            className="whitespace-nowrap text-[5.5px] font-medium tabular-nums"
            style={{ color: color ?? 'var(--color-text-primary)' }}
          >
            {value}
          </span>
        </span>
      ))}
    </div>
  );
}

/** The chart's toolbar (`.chart-tools`) as a page-width chart lays it out:
    intervals and indicators on the left; Clear while the agent's drawing is
    up, the selection tools, the tools, the menu (the view buttons fold into
    it at this width) and the Light / Advanced switch on the right. */
function ChartTools({ armed = false, clear = false }: { armed?: boolean; clear?: boolean }) {
  const { t } = useTranslation();
  return (
    <div
      className="flex shrink-0 items-center gap-1 border-b"
      style={{
        height: TOOLS_H,
        paddingInline: PAD_X,
        borderColor: 'var(--color-border-default)',
        background: 'var(--color-bg-card)',
      }}
    >
      <Intervals />
      <ToolBtn icon={SlidersHorizontal} />
      <span className="ml-auto flex items-center" style={{ gap: GAP }}>
        {clear && (
          <span className="intro-pop">
            <ClearButton />
          </span>
        )}
        <SelectionTools armed={armed} />
        <ToolBtn icon={Settings2} />
        <ToolBtn icon={Menu} />
        <Segmented
          active={0}
          className="w-[48px]"
          items={[t('marketView.chart.light'), t('marketView.chart.advanced')]}
        />
      </span>
    </div>
  );
}

/* ── the sidebar, folded ──────────────────────────────────────────────── */

/** The folded sidebar: a strip with the chart glyph and the unfold chevron. */
function FoldedSidebar() {
  return (
    <div
      className="flex shrink-0 flex-col items-center gap-[3px] border-l pt-[7px]"
      style={{
        width: STRIP_W,
        borderColor: 'var(--color-border-muted)',
        background: 'var(--color-bg-card)',
        color: 'var(--color-text-secondary)',
      }}
    >
      <BarChart3 className="h-[7px] w-[7px]" />
      <ChevronLeft className="h-[6px] w-[6px]" />
    </div>
  );
}

/* ── the chat panel ────────────────────────────────────────────────────── */

/** The app's mark, as the empty chat draws it (`LogoLoading`), at rest. */
function Mark() {
  return (
    <svg viewBox="0 0 60 60" className="h-5 w-5" fill="none" aria-hidden>
      {[
        'M40.03 29.6L49.99 25.41C50.53 25.18 50.76 24.53 50.48 24.01L45.64 15.05C45.35 14.52 44.66 14.36 44.17 14.72L34.65 21.61M40.03 29.6L33.93 32.17C31.79 33.08 31.45 35.98 33.32 37.36L44.17 45.32C44.66 45.68 45.35 45.52 45.64 44.99L50.51 35.98C50.78 35.48 50.58 34.86 50.07 34.61L40.03 29.6ZM34.65 21.61L30.59 24.56C28.75 25.89 26.16 24.78 25.85 22.54L24.16 10.14C24.07 9.53 24.54 9 25.15 9H34.72C35.3 9 35.77 9.51 35.71 10.09L34.65 21.61Z',
        'M35.28 47L35.66 50.02C35.73 50.62 35.26 51.15 34.66 51.14L25.17 51.03C24.58 51.03 24.13 50.52 24.19 49.93L25.14 40.67C25.23 39.81 24.27 39.25 23.57 39.75L15.85 45.23C15.36 45.58 14.67 45.41 14.39 44.88L9.69 35.91C9.42 35.39 9.65 34.75 10.2 34.52L18.11 31.28C18.9 30.95 18.95 29.85 18.19 29.46L10.21 25.36C9.71 25.1 9.52 24.48 9.79 23.99L14.72 15.07C15.02 14.54 15.71 14.39 16.2 14.76L18.5 16.5',
      ].map((d) => (
        <path
          key={d.length}
          d={d}
          stroke="var(--color-accent-overlay)"
          strokeWidth={3}
          strokeLinecap="round"
        />
      ))}
    </svg>
  );
}

/** A fresh chat: the mark and the prompt to start, then two suggestions
    for the symbol on the chart and the shuffle. */
function EmptyChat() {
  const { t } = useTranslation();
  return (
    <>
      <div className="flex flex-1 flex-col items-center justify-center gap-1.5 px-1 text-center">
        <Mark />
        <span className="text-[6px] leading-[1.4]" style={{ color: 'var(--color-text-secondary)' }}>
          {t('marketView.chatPanel.startConversation')}
        </span>
      </div>
      <div className="flex flex-col gap-1 pb-1">
        {(['q2', 'q3'] as const).map((q) => (
          <span
            key={q}
            className="rounded-[5px] border px-[5px] py-[4px] text-[6px] leading-[1.4]"
            style={{
              background: 'var(--color-bg-card)',
              borderColor: 'var(--color-border-default)',
              color: 'var(--color-text-primary)',
            }}
          >
            {t(`marketView.quickQuery.${q}`, { symbol: NVDA_QUOTE.sym })}
          </span>
        ))}
        <span
          className="flex h-[13px] w-[13px] items-center justify-center self-center rounded-full border"
          style={{
            borderColor: 'var(--color-border-default)',
            color: 'var(--color-text-secondary)',
          }}
        >
          <RefreshCw className="h-[6px] w-[6px]" />
        </span>
      </div>
    </>
  );
}

/** The chat panel (`MarketChatPanel`): the transcript, or the empty state
    until there is one, then any selection chip, then the composer under the
    market placeholder, its send filled while a selection is `ready` to go. */
function ChatPanel({
  transcript,
  chip,
  ready,
  sendAt,
}: {
  transcript?: ReactNode;
  chip?: ReactNode;
  ready?: boolean;
  sendAt?: number[];
}) {
  const { t } = useTranslation();
  return (
    <div
      className="flex shrink-0 flex-col border-l px-[6px] pb-[6px]"
      style={{
        width: CHAT_W,
        borderColor: 'var(--color-border-muted)',
        background: 'var(--color-bg-page)',
      }}
    >
      {transcript ? <Transcript className="px-[2px] pt-2">{transcript}</Transcript> : <EmptyChat />}
      {chip && <div className="flex px-[2px] pb-[4px]">{chip}</div>}
      <InputDock
        placeholder={t('marketView.chatPanel.defaultPlaceholder')}
        ready={ready}
        sendAt={sendAt}
      />
    </div>
  );
}

/* ── the page ──────────────────────────────────────────────────────────── */

/**
 * MarketView with the sidebar folded, drawn at `PAGE` size: the header band
 * over the chart column (metrics, toolbar, then `chart`, `CHART` sized), the
 * sidebar's strip, and the chat panel. `title` names the thread once there
 * is one; `ready` and `sendAt` drive the composer's send, as `InputDock`
 * reads them.
 */
export function MarketPage({
  armed,
  clear,
  chart,
  title,
  transcript,
  chip,
  ready,
  sendAt,
}: {
  armed?: boolean;
  clear?: boolean;
  chart: ReactNode;
  title?: string;
  transcript?: ReactNode;
  chip?: ReactNode;
  ready?: boolean;
  sendAt?: number[];
}) {
  const card = { borderColor: 'var(--color-border-muted)', background: 'var(--color-bg-card)' };
  return (
    <div className="flex flex-col" style={PAGE}>
      <div
        className="flex shrink-0 border-b"
        style={{ height: HEAD_H, borderColor: 'var(--color-border-muted)' }}
      >
        <div className="flex min-w-0" style={{ width: CHART_W, ...card }}>
          <StockTop />
        </div>
        <div className="shrink-0 border-l" style={{ width: STRIP_W, ...card }} />
        <ChatHeader width={CHAT_W} title={title} />
      </div>
      <div className="flex min-h-0 flex-1">
        <div className="flex min-w-0 flex-col" style={{ width: CHART_W }}>
          <Metrics />
          <ChartTools armed={armed} clear={clear} />
          <div className="relative flex-1" style={{ background: 'var(--color-bg-card)' }}>
            {chart}
          </div>
        </div>
        <FoldedSidebar />
        <ChatPanel transcript={transcript} chip={chip} ready={ready} sendAt={sendAt} />
      </div>
    </div>
  );
}
