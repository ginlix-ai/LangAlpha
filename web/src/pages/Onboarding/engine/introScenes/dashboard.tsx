import { Layers, Plus, RotateCcw, X } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { useLocale } from '@/hooks/useLocale';
import { formatMoney } from '@/lib/bars/currencyDisplay';
import { grouped2 } from '@/lib/format';
import { cn } from '@/lib/utils';
import type { IntroVisualId } from '../../registry';
import { CLOSE, Camera, REST, VIEW, shot, type Shot } from './camera';
import { ease, useSceneTime } from './clock';
import { Appear, pointAt, pointerScript, pressAt, signColor } from './kit';
import type { IntroScene } from './types';
import { attachScene } from './dashboardAttach';
import {
  WATCH,
  Spark,
  DashTopbar,
  DashPage,
  PAGE_W,
  PAD,
  GAP,
  Floating,
  Composer,
  Widget,
} from './dashboardParts';
import {
  BriefLede,
  BriefWidget,
  EarningsWidget,
  IndexRow,
  NEWS,
  NewsRow,
  NewsWidget,
  PageBody,
  WatchWidget,
  GRID,
} from './dashboardPage';

/* Scenes for the dashboard intro: the live page, and customizing it, and
   from its own file (dashboardAttach.tsx) clipping a row into the chat. Each
   widget is its real counterpart under pages/Dashboard, drawn with the
   dashboard's own card classes and scaled down, with the app's own strings
   for its labels. A camera shows the
   page a part at a time, close enough to read, with an edge of the next
   part so the viewer can tell where on the page it is. */
/* eslint-disable react-refresh/only-export-components -- scene module:
   private scene components, one record export */

/* ── dashboard · step 1: the page, live ────────────────────────────────── */

/** The page's size in page px, as the default card style lays it out (the
    page takes its content's height; Quiet's titles inside the cards make it
    taller), and the middle of the brief's column. */
const GRID_PAGE = { width: PAGE_W, height: 304 };
const BRIEF_X = 135;
/** Between the watchlist's symbols and its prices: a view from the page's
    left edge that ends here sits a touch closer than REST and cuts none of
    the watchlist's figures. */
const WATCH_NAMES_END = 306;

/** A slow pan across the page: the index lines with the brief arriving
    under them, a short push in on the brief, out to the watchlist while its
    quotes tick, the index cards over it, then out to the page whole. */
const GRID_SHOTS: [Shot, ...Shot[]] = [
  // the index cards and the brief, out to the watchlist's symbols
  shot(0, 0, 0, VIEW.width / WATCH_NAMES_END),
  // the brief whole, the view's top on the index cards' so their figures
  // stay in over it
  shot(2600, BRIEF_X, PAD + VIEW.height / 2 / CLOSE, CLOSE, 1000),
  // the watchlist whole, the index cards over it
  shot(3800, PAGE_W, 0, REST, 1000),
  // the page whole, as wide as the view
  shot(
    6400,
    PAGE_W / 2,
    GRID_PAGE.height / 2,
    Math.min(VIEW.width / PAGE_W, VIEW.height / GRID_PAGE.height),
    1000
  ),
];

function DashboardGrid() {
  return (
    <Camera shots={GRID_SHOTS} page={GRID_PAGE}>
      <DashPage>
        <PageBody live />
      </DashPage>
    </Camera>
  );
}

/** The Mini Chart Grid the toolbar adds: a tile per watchlist symbol, its
    move and line over thirty days. */
const MINI: { sym: string; v: number; series: number[] }[] = [
  { sym: 'NVDA', v: 8.42, series: [21, 19, 20, 15, 13, 11, 7] },
  { sym: 'AAPL', v: -1.36, series: [14, 15, 13, 15, 14, 16, 17] },
  { sym: 'TSLA', v: 11.05, series: [23, 21, 17, 19, 14, 10, 8] },
  { sym: 'MSFT', v: 2.34, series: [17, 16, 17, 15, 14, 15, 13] },
  { sym: 'AMZN', v: 4.81, series: [22, 20, 21, 16, 17, 12, 9] },
];

/** The days a mini chart tile looks back. */
const MINI_DAYS = 30;

/** The mini chart grid's tiles (`MiniChartGridWidget`), three to a row as
    the dashboard sets them: the symbol over its move, then its last price
    and its line. Each enters in turn once the widget is added. */
function MiniChartBody() {
  const locale = useLocale();
  return (
    <div className="grid grid-cols-3 gap-2">
      {MINI.map(({ sym, v, series }, i) => {
        const color = signColor(v);
        const quote = WATCH.find((w) => w.sym === sym);
        // the watchlist's last price, as the grid's live quote overlays it
        const last = quote ? quote.prev * (1 + quote.ticks[quote.ticks.length - 1].v / 100) : 0;
        return (
          <Appear
            key={sym}
            at={ADDED + 120 + i * 40}
            reserve
            className="rounded-lg border p-2"
            style={{ background: 'var(--color-bg-card)', borderColor: 'var(--color-border-muted)' }}
          >
            <span className="flex items-center justify-between">
              <span className="text-xs font-bold" style={{ color: 'var(--color-text-primary)' }}>
                {sym}
              </span>
              <span className="font-mono text-[11px] tabular-nums" style={{ color }}>
                {v >= 0 ? '+' : ''}
                {grouped2(v, locale)}%
              </span>
            </span>
            <span className="mt-1 flex items-end justify-between">
              <span
                className="font-mono text-[11px] tabular-nums"
                style={{ color: 'var(--color-text-secondary)' }}
              >
                {formatMoney(last, 'USD', locale)}
              </span>
              <Spark series={series} color={color} className="h-[33px] w-[110px]" />
            </span>
          </Appear>
        );
      })}
    </div>
  );
}

/* ── dashboard · step 2: customize (edit mode, drag and add widgets) ──── */

/** Customize, played out: press the edit-layout pencil, carry the earnings widget up the
    right column (the watchlist slides down to make room), then add a widget
    from the edit toolbar. */
const CUSTOM = {
  edit: 1250,
  grab: 2700,
  carry: 2800,
  drop: 3700,
  add: 4700,
  length: 6600,
};
/** The page's height (a window's, so the toolbar floats at its foot), its
    header's, and the grid's room above the floating toolbar. */
const CUSTOM_H = 360;
const HEADER_H = 28;
const GRID_TOP = HEADER_H + PAD;
const FLOAT_ROOM = 50;
/** The markets overview's row, its frame around the index cards, then two
    rows of the same height, as the default layout's columns fill the rest
    of the window. */
const MARKETS_ROW = 86;
const ROW = (CUSTOM_H - GRID_TOP - PAD - FLOAT_ROOM - MARKETS_ROW - 2 * GAP) / 2;
const ROW_STEP = ROW + GAP;
/** The right column's left edge: the left one takes eight of twelve. */
const RIGHT_X = PAD + ((PAGE_W - 2 * PAD - GAP) * 2) / 3 + GAP;
const ROW2_TOP = GRID_TOP + MARKETS_ROW + GAP;
const ROW3_TOP = ROW2_TOP + ROW_STEP;
/** Pointer targets in page px, read off the layout: the header's
    edit-layout pencil, the earnings widget's title in its edge, and the
    toolbar's add button. */
const EDIT = { x: 367, y: 13 };
const GRAB = { x: RIGHT_X + 34, y: ROW3_TOP + 8 };
const DROP = { x: GRAB.x - 4, y: GRAB.y - ROW_STEP + 5 };
const ADD = { x: 132, y: CUSTOM_H - PAD - 11 };
/** The added widget's row, as tall as its two rows of tiles. The real grid
    puts a new widget at the foot of the layout, full width, so the page
    grows by a row and the toolbar, which floats at the window's foot, rides
    down with it as the window follows. */
const MINI_ROW = 104;
const GROW = GAP + MINI_ROW;
const ADDED = CUSTOM.add + 120;
const GROW_DUR = 500;
const grown = (t: number) => (t >= ADDED ? GROW * ease(t, ADDED, GROW_DUR) : 0);
/** The page once it has grown by the new row. */
const GROWN = { width: PAGE_W, height: CUSTOM_H + GROW };
/** Past halfway, the watchlist gives up its slot. */
const SWAP = (CUSTOM.carry + CUSTOM.drop) / 2;
/** The pencil, the earnings widget's header grabbed and carried a row
    up, the toolbar's add button; then down with the toolbar as the page
    grows under it. */
const CUSTOM_POINTER = pointerScript(
  { at: 300, x: 300, y: 110 },
  {
    edit: { ...EDIT, arrive: 1100, click: CUSTOM.edit, leave: 1450 },
    grab: { ...GRAB, arrive: 2550, leave: CUSTOM.carry },
    drop: { ...DROP, arrive: CUSTOM.drop, leave: 4000 },
    add: { ...ADD, arrive: 4550, click: CUSTOM.add, leave: ADDED },
    ride: { x: ADD.x, y: ADD.y + GROW, arrive: ADDED + GROW_DUR },
  }
);
/** The page's top right under the header's pencil; a short push in on it,
    and back out down the right column as the earnings widget is
    carried up it; down to the toolbar's add button, and out to the page's
    full width as the page grows by the new widget's row, from the second
    row's top to the page's foot, so the widget is whole with the toolbar
    under it. */
const CUSTOM_SHOTS: [Shot, ...Shot[]] = [
  shot(0, PAGE_W, 0, REST),
  shot(1200, PAGE_W, 0, CLOSE, 800),
  // the view's top in the gap over the brief and the watchlist
  shot(2500, PAGE_W, ROW2_TOP - GAP / 2 + VIEW.height / 2 / REST, REST, 1100),
  // the view's foot on the window's, the toolbar in
  shot(4600, PAGE_W, CUSTOM_H - VIEW.height / 2 / REST, REST, 600),
  // from the second row's top to the page's foot, no closer than the
  // page's full width
  shot(
    ADDED + GROW_DUR + 130,
    PAGE_W / 2,
    GROWN.height,
    Math.min(VIEW.width / PAGE_W, VIEW.height / (GROWN.height - ROW2_TOP)),
    700
  ),
];

/** The toolbar edit mode floats at the foot of the page. */
function EditToolbar() {
  const { t } = useTranslation();
  const btn = 'flex items-center gap-[3px] rounded-full px-1.5 py-[3px] text-[7px] leading-none';
  return (
    <Appear
      at={CUSTOM.edit}
      className="flex items-center gap-0.5 rounded-full border px-1 py-[3px]"
      style={{
        background: 'var(--color-bg-elevated)',
        borderColor: 'var(--color-border-elevated)',
        boxShadow: '0 4px 12px -4px rgba(0, 0, 0, 0.25)',
      }}
    >
      <span
        className={cn(btn, 'font-medium')}
        style={{
          background: 'var(--color-btn-primary-bg)',
          color: 'var(--color-btn-primary-text)',
        }}
      >
        <Plus className="h-2 w-2" />
        {t('dashboard.widgets.shell.addWidget')}
      </span>
      <span className={cn(btn, 'font-medium')} style={{ color: 'var(--color-text-primary)' }}>
        <Layers className="h-2 w-2" />
        {t('dashboard.widgets.shell.presets')}
      </span>
      <span className={btn} style={{ color: 'var(--color-text-secondary)' }}>
        <RotateCcw className="h-2 w-2" />
        {t('dashboard.widgets.shell.reset')}
      </span>
      <span className="mx-0.5 h-2.5 w-px" style={{ background: 'var(--color-border-muted)' }} />
      <span className={cn(btn, 'font-medium')} style={{ color: 'var(--color-text-secondary)' }}>
        <X className="h-2 w-2" />
        {t('dashboard.widgets.shell.done')}
      </span>
    </Appear>
  );
}

/** The default layout in edit mode, each widget in its frame: the markets
    overview across the top, its frame now drawn around the index cards, the
    brief over news on the left, the watchlist over earnings on the right.
    The cells are shorter than the widgets, so each shows its top. */
function EditGrid() {
  const { t: tr } = useTranslation();
  const t = useSceneTime();
  const held = t >= CUSTOM.grab;
  const dropped = t >= CUSTOM.drop;
  const swapped = t >= SWAP;
  // the earnings widget rides with the pointer while held, then settles into
  // its new slot from wherever the pointer let go; held, it is picked up the
  // way the grid picks a widget up, a touch larger, with no shadow, since a
  // card the style leaves unfilled would cast one around nothing. It carries
  // the page's ground, so the rows it passes over do not read through it.
  const lift = ease(t, CUSTOM.grab, 160) * (1 - ease(t, CUSTOM.drop, 260));
  const settle = 1 - ease(t, CUSTOM.drop, 260);
  const p = pointAt(CUSTOM_POINTER.path, t);
  const dx = dropped ? (DROP.x - GRAB.x) * settle : held ? p.x - GRAB.x : 0;
  const dy = dropped ? (DROP.y - GRAB.y + ROW_STEP) * settle : held ? p.y - GRAB.y : 0;
  return (
    <Appear at={CUSTOM.edit + 80} className="flex flex-col">
      <div
        className="grid grid-cols-[2fr_1fr] gap-1.5"
        style={{ gridTemplateRows: `${MARKETS_ROW}px ${ROW}px ${ROW}px` }}
      >
        <div style={{ gridColumn: '1 / 3', gridRow: 1 }}>
          <Widget title={tr('dashboard.widgets.marketsOverview.title')} edit className="h-full">
            <IndexRow fill />
          </Widget>
        </div>
        <div style={{ gridColumn: 1, gridRow: 2 }}>
          <BriefWidget edit className="h-full">
            <BriefLede clamp />
          </BriefWidget>
        </div>
        <div style={{ gridColumn: 1, gridRow: 3 }}>
          <NewsWidget edit className="h-full">
            {NEWS.slice(0, 3).map((item) => (
              <NewsRow key={item.title} item={item} />
            ))}
          </NewsWidget>
        </div>
        {/* where the held widget will land, as the grid marks it */}
        {held && t < CUSTOM.drop + 260 && (
          <div
            className="rounded-lg"
            style={{
              gridColumn: 2,
              gridRow: swapped ? 2 : 3,
              background: 'var(--color-accent-soft)',
            }}
          />
        )}
        {/* watchlist: slides one row down once the earnings pass halfway */}
        <div
          style={{
            gridColumn: 2,
            gridRow: swapped ? 3 : 2,
            transform: swapped
              ? `translateY(${-ROW_STEP * (1 - ease(t, SWAP, 300))}px)`
              : undefined,
          }}
        >
          <WatchWidget rows={3} edit className="h-full" />
        </div>
        {/* the earnings, carried */}
        <div
          className="relative rounded-lg"
          style={{
            gridColumn: 2,
            gridRow: dropped ? 2 : 3,
            zIndex: held ? 2 : undefined,
            background: lift > 0 ? 'var(--color-bg-page)' : undefined,
            transform: `translate(${dx}px, ${dy}px) scale(${1 + 0.015 * lift})`,
          }}
        >
          <EarningsWidget edit className="h-full" />
        </div>
      </div>
      {/* the widget the toolbar adds, at the layout's foot, full width */}
      {t >= ADDED && (
        <div className="shrink-0 overflow-hidden" style={{ height: grown(t) }}>
          <Appear at={ADDED} pop className="flex" style={{ paddingTop: GAP, height: GROW }}>
            <Widget
              title={tr('dashboard.widgets.miniChartGrid.header')}
              meta={tr('dashboard.widgets.miniChartGrid.summary', {
                count: MINI.length,
                days: MINI_DAYS,
              })}
              edit
              className="flex-1"
            >
              <MiniChartBody />
            </Widget>
          </Appear>
        </div>
      )}
    </Appear>
  );
}

/** The page as step 1 left it, until the pencil puts every widget in its
    frame. */
function DashboardCustomize() {
  const t = useSceneTime();
  const edit = t >= CUSTOM.edit;
  return (
    <Camera shots={CUSTOM_SHOTS} page={GROWN} pointer={CUSTOM_POINTER}>
      <DashPage
        height={CUSTOM_H + grown(t)}
        header={<DashTopbar edit={edit} pressed={pressAt(t, CUSTOM.edit)} />}
      >
        {t < CUSTOM.edit + 80 ? (
          <PageBody />
        ) : (
          /* the grid stops short of the floating toolbar, as its rows do */
          <div className="flex min-h-0 flex-1 flex-col" style={{ paddingBottom: FLOAT_ROOM }}>
            <EditGrid />
          </div>
        )}
        {/* the floating chat, which edit mode swaps for its toolbar */}
        <Floating>{edit ? <EditToolbar /> : <Composer delay={0.46} />}</Floating>
      </DashPage>
    </Camera>
  );
}

export const dashboardScenes = {
  dashboardGrid: { Scene: DashboardGrid, length: GRID.length, anchor: 'left' },
  dashboardCustomize: { Scene: DashboardCustomize, length: CUSTOM.length, anchor: 'right' },
  dashboardAttach: attachScene,
} satisfies Partial<Record<IntroVisualId, IntroScene>>;
