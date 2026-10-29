import type { CSSProperties } from 'react';
import { Newspaper, X } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { useLocale } from '@/hooks/useLocale';
import { relativeTime } from '@/lib/format';
import { CLOSE, Camera, VIEW, corner, shot, type Shot } from './camera';
import { easeInOut, useSceneTime } from './clock';
import { Typed, pointerScript } from './kit';
import type { IntroScene } from './types';
import { DashPage, PAD, PAGE_W, Floating, Composer, useRowShift } from './dashboardParts';
import { NEWS, NewsRow, NewsWidget, RowClip, WatchWidget } from './dashboardPage';

/* The dashboard intro's last scene: clipping a row into the chat's context.
   Drawn after its real counterpart under pages/Dashboard (row paperclip,
   context deck), in miniature. It plays from the dashboard intro's record,
   so it downloads with that intro's other scenes. */
/* eslint-disable react-refresh/only-export-components -- scene module: a
   private scene component and its record entry */

/* ── dashboard · step 3: clip a row into chat context ──────────────────── */

/** Clipping a row, played out: the pointer finds the news row's paperclip
    (it shows on hover), clicks, the row flies into the composer and lands as
    a context card, then a question about it is typed. */
const ATTACH = {
  hover: 900,
  click: 1350,
  leave: 1900,
  fly: 1450,
  flyDur: 1000,
  focus: 2750,
  type: 2900,
  ask: 'Is this already priced in?',
  length: 5000,
};
/** The page's size: a window's height, so the chat floats at its foot. */
const ATTACH_PAGE = { width: PAGE_W, height: 300 };
/** Page px, read off the layout in the default card style: the first row
    and its paperclip, at the end of the headline's cell, the card in the
    composer's open deck, and the line its text goes on. A row sits lower
    or higher in the other styles (`useRowShift`). */
const ROW = { x: 17, y: 37, w: 236 };
const CLIP = { x: 169, y: 43 };
const CARD = { x: 89, y: 216, w: 222 };
const TEXT = { x: 198, y: 257 };
/** Where the crops' edges fall, in page px: between the watchlist's
    symbols and its prices; over the cards' edges; the foot of the composer,
    which floats the page's padding above the page's foot; and the news
    card's left padding, which puts the right edge of a view at the last
    shot's zoom between the watchlist's prices and their changes. */
const WATCH_NAMES_END = 306;
const OVER_CARDS = PAD / 2;
const COMPOSER_FOOT = ATTACH_PAGE.height - PAD;
const WIDGET_PAD = 12;
/** The flying card's box: over the clipped row, then its slot in the deck.
    It eases in and out over the camera's move, so it rides along in view. */
const CARD_FROM = { x: ROW.x, y: ROW.y - 6, w: ROW.w };
/** Onto the row's paperclip and a click on it, then down to the
    composer's line, clicked to type. `shift` is where the card style puts
    the row. */
const attachPointer = (shift: number) =>
  pointerScript(
    { at: 300, x: 150, y: 140 },
    {
      clip: { ...CLIP, y: CLIP.y + shift, arrive: 1150, click: ATTACH.click, leave: ATTACH.leave },
      text: { ...TEXT, arrive: 2650, click: ATTACH.focus },
    }
  );
/** The feed with its header, out to the watchlist's symbols, a touch
    closer than REST; a short push in on the feed as the paperclip is
    clicked, the watchlist's edge still in; then out with the card as it
    flies down to the composer, wide enough that the feed's header and the
    composer's foot are in: the card it lands as is whole, and the question
    under it reads. */
const ATTACH_SHOTS: [Shot, ...Shot[]] = [
  shot(0, 0, 0, VIEW.width / WATCH_NAMES_END),
  corner(1150, OVER_CARDS, OVER_CARDS, CLOSE, 900),
  corner(
    ATTACH.fly + ATTACH.flyDur,
    WIDGET_PAD,
    OVER_CARDS,
    VIEW.height / (COMPOSER_FOOT - OVER_CARDS),
    ATTACH.flyDur
  ),
];

/** The context card a clipped row becomes in the composer: the story's
    title, then its source and when it ran, as the row's snapshot labels
    it. */
function ContextCard({ style }: { style?: CSSProperties }) {
  const locale = useLocale();
  return (
    <div
      className="flex items-center gap-1.5 rounded-md border p-1"
      style={{
        background: 'var(--color-bg-card)',
        borderColor: 'var(--color-border-muted)',
        ...style,
      }}
    >
      <span
        className="flex h-4 w-4 shrink-0 items-center justify-center rounded-[4px]"
        style={{ background: 'var(--color-accent-soft)', color: 'var(--color-accent-primary)' }}
      >
        <Newspaper className="h-2 w-2" />
      </span>
      <span className="flex min-w-0 flex-1 flex-col gap-[3px]">
        <span
          className="truncate text-[7px] font-medium leading-none"
          style={{ color: 'var(--color-text-primary)' }}
        >
          News: {NEWS[0].title}
        </span>
        <span
          className="truncate text-[5px] leading-none"
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          {NEWS[0].source} · {relativeTime(-NEWS[0].mins * 60_000, locale, 0)}
        </span>
      </span>
      <X className="h-2 w-2 shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
    </div>
  );
}

/** The deck over the composer's text: the count, then the card on top. */
function ContextDeck({ shown }: { shown: boolean }) {
  const { t } = useTranslation();
  return (
    <div
      className="flex flex-col gap-1 border-b border-dashed pb-1"
      style={{ borderColor: 'var(--color-border-muted)', visibility: shown ? 'visible' : 'hidden' }}
    >
      <span
        className={
          'flex items-center gap-1 px-0.5 font-mono text-[5px] uppercase leading-none ' +
          'tracking-[0.18em]'
        }
        style={{ color: 'var(--color-accent-primary)' }}
      >
        <span
          className="h-[3px] w-[3px] rounded-full"
          style={{ background: 'var(--color-accent-primary)' }}
        />
        {t('chat.widgetContext.inContext', { count: 1 })}
      </span>
      <ContextCard />
    </div>
  );
}

function DashboardAttach() {
  const t = useSceneTime();
  const shift = useRowShift();
  const pointer = attachPointer(shift);
  const hovered = t >= ATTACH.hover && t < ATTACH.leave + 150;
  const { clip } = pointer.beats;
  const onClip = t >= clip.arrive && t < clip.leave;
  const landed = t >= ATTACH.fly + ATTACH.flyDur;
  // eased as the camera's move is, so the card keeps pace with it
  const fly = easeInOut(t, ATTACH.fly, ATTACH.flyDur);
  const from = { ...CARD_FROM, y: CARD_FROM.y + shift };
  return (
    <Camera shots={ATTACH_SHOTS} page={ATTACH_PAGE} pointer={pointer}>
      <DashPage height={ATTACH_PAGE.height}>
        <div className="grid min-h-0 flex-1 grid-cols-[2fr_1fr] gap-1.5">
          {/* news widget: the paperclip shows on the hovered row */}
          <NewsWidget delay={0.12}>
            {NEWS.map((item, i) => (
              <NewsRow
                key={item.title}
                item={item}
                hovered={i === 0 && hovered}
                clip={i === 0 ? <RowClip shown={hovered} on={onClip} /> : undefined}
              />
            ))}
          </NewsWidget>
          <WatchWidget delay={0.24} />
        </div>
        {/* the floating chat: takes the clipped row, then the question */}
        <Floating>
          <Composer
            focused={t >= ATTACH.focus}
            delay={0.36}
            chip={
              /* opens while the card is on its way, so it lands in place */
              <div
                className="grid"
                style={{ gridTemplateRows: `${fly}fr`, marginBottom: (fly - 1) * 6 }}
              >
                <div className="min-h-0 overflow-hidden">
                  <ContextDeck shown={landed} />
                </div>
              </div>
            }
            text={t >= ATTACH.type ? <Typed text={ATTACH.ask} at={ATTACH.type} /> : undefined}
          />
        </Floating>
        {/* the row on its way into the composer */}
        {t >= ATTACH.fly && !landed && (
          <ContextCard
            style={{
              position: 'absolute',
              pointerEvents: 'none',
              left: from.x + (CARD.x - from.x) * fly,
              top: from.y + (CARD.y - from.y) * fly,
              width: from.w + (CARD.w - from.w) * fly,
              // lifted off the row: solid at once, with the shadow a
              // carried card takes
              opacity: Math.min(fly * 12, 1),
              boxShadow: '0 6px 14px -6px rgba(0, 0, 0, 0.3)',
            }}
          />
        )}
      </DashPage>
    </Camera>
  );
}

/** The scene's entry in the dashboard record (dashboard.tsx). */
export const attachScene: IntroScene = {
  Scene: DashboardAttach,
  length: ATTACH.length,
  anchor: 'right',
};
