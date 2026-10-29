import { useTranslation } from 'react-i18next';
import { CandlestickChart, ExternalLink, Menu, TextSelect, X } from 'lucide-react';
import { useLocale } from '@/hooks/useLocale';
import { fixed2, signedFixed2 } from '@/lib/format';
import type { IntroVisualId } from '../../registry';
import { CLOSE, Camera, REST, type Shot, VIEW, shot } from './camera';
import { Candles, EventBadge, badgeAnchor } from './candles';
import {
  AnnotateRow,
  CommentBox,
  FoldedIntervals,
  Pin,
  Selection,
  SelectionChip,
  SelectionTools,
  SentSelection,
  ToolBtn,
  addButton,
  besideRegion,
  dragBox,
} from './chartTools';
import { StockPriceCard } from './charts';
import { FileCard } from './chatParts';
import { PAST, useSceneTime } from './clock';
import { Appear, Loader, Typed, UserBubble, pointerScript, signColor } from './kit';
import { NVDA, NVDA_EVENTS, NVDA_LAST, NVDA_PREV, NVDA_QUOTE, chartScale } from './nvdaSeries';
import { PanelTab } from './panelParts';
import { PANEL_W, ThreadPage } from './threadParts';
import { HEADER_H } from './transcriptParts';
import { Reply, Transcript } from './transcriptParts';
import type { IntroScene } from './types';

/* The thread intro's chart step: a chart tab in the file panel
   (`MarketChartSurface` in its compact form, as `ChartTab` hosts it). The
   user takes a region of it, notes it and sends it from the thread's
   composer; the agent pins the news behind the move to the same chart and
   answers with the chart's card in the transcript. A camera follows the
   work across the page: the chart, the composer, the reply, then the chart
   again, where a badge opens on its story. */
/* eslint-disable react-refresh/only-export-components -- scene module:
   private scene components, one record export */

/* ── layout, in the page's px ──────────────────────────────────────────── */

/** The panel's left edge, past the chat column. */
const PANEL_X = VIEW.width - PANEL_W;
/** The chart card sits on the panel's ground, inset 4px top and 5px around. */
const CARD = {
  x: PANEL_X + 1 + 5,
  y: HEADER_H + 4,
  w: PANEL_W - 1 - 10,
  h: VIEW.height - HEADER_H - 4 - 5,
};
const TOOLBAR_H = 18;
const SUBROW_H = 19;
/** The chart inside the card, under its toolbar and the stats row. */
const CHART = {
  x: CARD.x + 1,
  y: CARD.y + 1 + TOOLBAR_H + SUBROW_H,
  w: CARD.w - 2,
  h: CARD.h - 2 - TOOLBAR_H - SUBROW_H,
};
const SCALE = chartScale(NVDA, CHART.w, CHART.h);
const toPage = (x: number, y: number) => ({ x: CHART.x + x, y: CHART.y + y });

/** The toolbar from its right end: the trail's two buttons, then the menu,
    Price level and Region. */
const REGION_BTN = {
  x: CARD.x + CARD.w - 1 - 5 - (11 + 2 + 11) - 4 - (11 + 2) - (11 + 2) - 5.5,
  y: CARD.y + 1 + TOOLBAR_H / 2,
};
/** The thread composer's send button. */
const SEND = { x: PANEL_X - 10 - 7 - 8, y: VIEW.height - 10 - 7 - 8 };

const { earnings: EARNINGS, exportRule: EXPORT_RULE } = NVDA_EVENTS;

/** The earnings gap and the slide that filled it: Aug 27 to Sep 11. */
const REGION = {
  x0: SCALE.x(EARNINGS.i) - SCALE.step / 2,
  y0: SCALE.y(187.6),
  x1: SCALE.x(20) + SCALE.step / 2,
  y1: SCALE.y(169.9),
};
const BOX_W = 88;
const BOX = besideRegion(SCALE, REGION.x0, REGION.x1, REGION.y0, BOX_W);
const PIN = besideRegion(SCALE, REGION.x0, REGION.x1, REGION.y0, 10);
const ADD = addButton(BOX, BOX_W);
/** The export rule's badge, centered over its anchor. */
const BADGE = toPage(...badgeAnchor(SCALE, EXPORT_RULE));

const CH = {
  arm: 850,
  press: 1350,
  release: 2050,
  note: 2250,
  add: 3050,
  added: 3200,
  ask: 4200,
  send: 5300,
  think: 5500,
  draw: 5800,
  drawn: 6600,
  answer: 6800,
  open: 8900,
  length: 9900,
};

const NOTE = 'Earnings gap';
const ASK = 'Why did this fade?';

/** The camera: on the panel whole, the chart card from its toolbar to its
    volume and an edge of the chat beside it, as the region is taken and
    noted; across to the chat as the chip lands in the composer, held there
    as the ask is typed and the reply comes in above it; then in on the
    chart as its badges land and one opens on its story. */
const SHOTS: [Shot, ...Shot[]] = [
  shot(0, 240, 132, REST),
  shot(4100, 160, 198, REST, 850),
  shot(8500, 267, 142, CLOSE, 900),
];

/** Region armed; the drag from the region's corner, pressed over the
    dwell, to its far corner and off it; Add on the comment box; send in
    the composer; and the export rule's badge, clicked open. */
const POINTER = pointerScript(
  { at: 150, x: 352, y: 168 },
  {
    region: { ...REGION_BTN, arrive: 650, click: CH.arm, leave: 1000 },
    anchor: { ...toPage(REGION.x0, REGION.y0), arrive: 1300, leave: 1400 },
    release: { ...toPage(REGION.x1, REGION.y1), arrive: 2050 },
    clear: { ...toPage(REGION.x1 + 4, REGION.y1 - 10), arrive: 2350 },
    add: { ...toPage(ADD.x, ADD.y), arrive: 2900, click: CH.add, leave: 3300 },
    send: { ...SEND, arrive: 5100, click: CH.send, leave: 5600 },
    badge: { ...BADGE, arrive: 8800, click: CH.open - 60 },
  }
);

/* ── the chart tab ─────────────────────────────────────────────────────── */

/** The ticker legend that leads the compact toolbar (`LegendLead`): symbol,
    price and currency, the change, and the feed's status. Cut short with an
    ellipsis when the row runs out, as the lead is the one thing that gives. */
function LegendLead() {
  const { t } = useTranslation();
  const locale = useLocale();
  const change = NVDA_LAST - NVDA_PREV;
  const tint = { color: signColor(change) };
  return (
    <span
      className="min-w-0 flex-1 overflow-hidden text-ellipsis whitespace-nowrap text-[5.5px] leading-none tabular-nums"
      style={{ color: 'var(--color-text-secondary)' }}
    >
      <span className="text-[6px] font-semibold" style={{ color: 'var(--color-text-primary)' }}>
        NVDA
      </span>{' '}
      <span className="font-mono font-semibold" style={tint}>
        {fixed2(NVDA_LAST, locale)}
      </span>{' '}
      <span className="text-[4.8px]">USD</span>{' '}
      <span className="font-mono" style={tint}>
        {signedFixed2(change, locale)} ({signedFixed2((change / NVDA_PREV) * 100, locale)}%)
      </span>{' '}
      <span
        className="inline-block h-[3px] w-[3px] rounded-full align-middle"
        style={{ background: 'var(--color-profit)' }}
      />{' '}
      <span className="text-[5px]">{t('marketView.quote.live')}</span>
    </span>
  );
}

/** The day's figures under the toolbar (`LegendStats`), key then value. */
function LegendStats() {
  const { t } = useTranslation();
  const locale = useLocale();
  const q = NVDA_QUOTE;
  const pct = ((NVDA_LAST - q.prev) / q.prev) * 100;
  const stats: [string, string, string?][] = [
    [t('marketView.quote.open'), fixed2(q.open, locale)],
    [t('marketView.quote.high'), fixed2(q.high, locale)],
    [t('marketView.quote.low'), fixed2(q.low, locale)],
    [t('marketView.quote.close'), fixed2(NVDA_LAST, locale)],
    [t('marketView.quote.volume'), q.vol],
    [t('marketView.quote.prevClose'), fixed2(q.prev, locale)],
    [t('marketView.quote.range52w'), `${fixed2(q.lo52, locale)}–${fixed2(q.hi52, locale)}`],
    [t('marketView.quote.change'), `${signedFixed2(pct, locale)}%`, signColor(pct)],
  ];
  return (
    <div
      className="flex shrink-0 flex-wrap content-center gap-x-[5px] gap-y-[2px] border-b px-[5px] font-mono text-[5px] leading-none tabular-nums"
      style={{ height: SUBROW_H, borderColor: 'var(--color-border-default)' }}
    >
      {stats.map(([key, value, color]) => (
        <span key={key} className="whitespace-nowrap">
          <span style={{ color: 'var(--color-text-tertiary)' }}>{key}</span>{' '}
          <span style={{ color: color ?? 'var(--color-text-primary)' }}>{value}</span>
        </span>
      ))}
    </div>
  );
}

/** The compact chart in the panel: the legend and the folded interval
    picker, Clear (icon only at this width) while the agent's drawing is up,
    the selection tools and the menu, then the tab's own actions, add to
    context and open in MarketView. */
function PanelChart({ armed, clear }: { armed: boolean; clear: boolean }) {
  const t = useSceneTime();
  const sent = t >= CH.send;
  const drawing = t >= CH.press && !sent;
  return (
    <div
      className="flex flex-1 flex-col px-[5px] pb-[5px] pt-1"
      style={{ background: 'var(--color-bg-canvas)' }}
    >
      <div
        className="flex flex-1 flex-col overflow-hidden rounded-[4px] border"
        style={{ borderColor: 'var(--color-border-default)', background: 'var(--color-bg-card)' }}
      >
        <div
          className="flex shrink-0 items-center gap-1 border-b px-[5px]"
          style={{ height: TOOLBAR_H, borderColor: 'var(--color-border-default)' }}
        >
          <LegendLead />
          <FoldedIntervals />
          <span className="flex items-center gap-[2px]">
            {clear && (
              <span className="intro-pop">
                <ToolBtn icon={X} />
              </span>
            )}
            <SelectionTools armed={armed} />
            <ToolBtn icon={Menu} />
          </span>
          <span className="flex items-center gap-[2px]">
            <ToolBtn icon={TextSelect} />
            <ToolBtn icon={ExternalLink} />
          </span>
        </div>
        <LegendStats />
        <Candles series={NVDA} scale={SCALE}>
          {drawing && t < CH.added && (
            <Selection {...dragBox(REGION, POINTER.path, CHART, t, CH)} />
          )}
          {drawing && t >= CH.added && <Selection {...REGION} active={false} />}
          {t >= CH.release && t < CH.added && (
            <CommentBox
              left={BOX.left}
              top={BOX.top}
              width={BOX_W}
              pressed={t >= CH.add}
              text={t >= CH.note ? <Typed text={NOTE} at={CH.note} cps={18} /> : undefined}
            />
          )}
          {t >= CH.added && !sent && <Pin n={1} left={PIN.left} top={PIN.top} />}
          <EventBadge scale={SCALE} ev={EARNINGS} at={CH.drawn} />
          <EventBadge
            scale={SCALE}
            ev={EXPORT_RULE}
            at={CH.drawn + 150}
            hover={t >= POINTER.beats.badge.arrive}
            open={t >= CH.open}
          />
        </Candles>
      </div>
    </div>
  );
}

/* ── the transcript ────────────────────────────────────────────────────── */

/** The chart's card in a thread (`InlineChartAnnotationCard` with no live
    chart beside it): the candle glyph over the interval, the chart's name,
    how many annotations and which, and Open. */
function ChartFileCard() {
  const { t } = useTranslation();
  const labels = [EARNINGS.title, EXPORT_RULE.title];
  return (
    <Appear at={CH.drawn} pop>
      <FileCard
        name={t('chat.chartAnnotationCard.name', { symbol: 'NVDA', interval: '1D' })}
        meta={
          <>
            {t('chat.chartAnnotationCard.metaCount', { count: labels.length })} ·{' '}
            {labels.join(t('chat.chartAnnotationCard.labelSeparator'))}
          </>
        }
        icon={CandlestickChart}
        label="1D"
        solo
      />
    </Appear>
  );
}

/** The chat column: a thread with history, then the selection's chip above
    the composer while it waits to go out. */
function Chat() {
  const t = useSceneTime();
  return (
    <>
      {/* a thread with history, scrolled to its newest lines, the oldest
          running off the top as the turn comes in */}
      <Transcript settled>
        <UserBubble at={PAST}>When did NVDA last report?</UserBubble>
        <Reply text="**Aug 26**, after the close. Data-center revenue beat, and guidance came in above estimates." />
        <UserBubble at={PAST}>Chart NVDA since earnings</UserBubble>
        <StockPriceCard
          sym="NVDA"
          price="$182.42"
          change={7.18}
          period="2026-08-13, 2026-10-21"
          series={NVDA.bars.map((b) => b.c)}
          high="$186.90"
          low="$170.60"
          vol="181.5M"
          at={PAST}
        />
        <Reply text="It gapped to **186.90** on Aug 27, then gave most of it back by mid-September." />
        <SentSelection at={CH.send} text={ASK} note={NOTE} />
        {t >= CH.think && t < CH.draw && (
          <Loader color="var(--color-icon-muted)" className="text-[10px]" />
        )}
        {t < CH.drawn ? <AnnotateRow at={CH.draw} done={CH.drawn} /> : <ChartFileCard />}
        <Reply
          text="It faded on news: a **new export rule** on Sep 8 took 3% off in a day, and the slide ran through the gap to **170.60**. Both events are on the chart."
          at={CH.answer}
          cps={110}
        />
      </Transcript>
      {t >= CH.added && t < CH.send && <SelectionChip note={NOTE} at={CH.added} />}
    </>
  );
}

function ChartPanel() {
  const t = useSceneTime();
  return (
    <Camera shots={SHOTS} pointer={POINTER}>
      <ThreadPage
        title="NVDA earnings"
        tabs={<PanelTab icon={CandlestickChart} label="NVDA" active />}
        chat={<Chat />}
        panel={<PanelChart armed={t >= CH.arm && t < CH.release} clear={t >= CH.drawn} />}
        ask={{ text: ASK, at: CH.ask, send: CH.send, cps: 22 }}
      />
    </Camera>
  );
}

export const threadChartScenes = {
  chartPanel: { Scene: ChartPanel, length: CH.length, anchor: 'right' },
} satisfies Partial<Record<IntroVisualId, IntroScene>>;
