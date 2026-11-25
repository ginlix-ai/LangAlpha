import type { ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import { Check } from 'lucide-react';
import type { IntroVisualId } from '../../registry';
import { Camera, REST, VIEW, shot, type Shot } from './camera';
import { Candles, LayerLabel } from './candles';
import {
  CommentBox,
  CommentCard,
  CommentPin,
  CommentReplyLine,
  DragArea,
  ModeHint,
  PublishRow,
  QueuedDeck,
  SentComment,
  SpotOutline,
  besidePin,
  dragBox,
  queueButton,
  useDeckPlaceholder,
  useRegionLabel,
} from './chartTools';
import { PAST, useSceneTime } from './clock';
import { Loader, Typed, pointerScript } from './kit';
import { CHART, CHAT_X, COMMENT_BTN, MarketPage, PAGE, SEND } from './marketParts';
import { NVDA, NVDA_EVENTS, chartScale } from './nvdaSeries';
import { Reply } from './transcriptParts';
import type { IntroScene } from './types';

/* The market intro, on MarketView itself, framed by a camera that shows
   one side of the page at a time with an edge of the other. marketAsk: on
   the chart under the stock's header, Comment turns comment mode on, a
   drag takes the September drop and the box beside it takes the question,
   added to the queue; across to the chat panel, where it waits in the
   composer and goes out. The agent starts on it. marketDraw: on the chat
   panel as the agent publishes a layer labelling the news behind each move
   and answers under the comment; then across to the chart, where each move
   carries its label and the comment's pin opens on the answer. */
/* eslint-disable react-refresh/only-export-components -- scene module:
   private scene components, one record export */

const SCALE = chartScale(NVDA, CHART.w, CHART.h);
/** A point of the chart, in page px. */
const onPage = (x: number, y: number) => ({ x: CHART.x + x, y: CHART.y + y });

const NOTE = 'What drove this drop?';
const LEAD = 'Two pieces of news drove it. Marking them on your chart.';
const ANSWER =
  'A **new export rule** on Sep 8 took 3% off in a day, and the slide ran to **170.60**. The **Fed cut** on Sep 16 ended it.';

/** Where the metrics' third column (Low, Volume) ends, in page px. A view
    of the chat side from just past it to the page's right edge sits a
    touch wider than REST and cuts none of the metrics' figures. */
const METRICS_GAP = 137;
const CHAT_REST = VIEW.width / (PAGE.width - METRICS_GAP);

/** The September drop: from above the post-earnings high (Aug 28) to under
    the low it slid to (Sep 11), dragged from its top left. Its pin's tail is
    where the drag was let go. */
const REGION = {
  x0: SCALE.x(11) - SCALE.step / 2,
  y0: SCALE.y(187.6),
  x1: SCALE.x(20) + SCALE.step / 2,
  y1: SCALE.y(169),
};
const REGION_DATES = ['2026-08-28', '2026-09-11'] as const;
const BOX = besidePin(SCALE, REGION.x1, REGION.y1, 130);
const QUEUE = queueButton(BOX);
/** On the pin, in page px. */
const ON_PIN = onPage(REGION.x1 + 5, REGION.y1 - 5);

/** A native tooltip (a button's `title`), under the pointer. */
function Tip({ x, y, children }: { x: number; y: number; children: ReactNode }) {
  return (
    <span
      className="intro-pop absolute z-10 whitespace-nowrap rounded-[2px] border px-[3px] py-[2px] text-[5px] leading-none"
      style={{
        left: x,
        top: y,
        background: 'var(--color-bg-elevated)',
        borderColor: 'var(--color-border-default)',
        color: 'var(--color-text-primary)',
        boxShadow: '0 1px 3px rgba(0, 0, 0, 0.12)',
      }}
    >
      {children}
    </span>
  );
}

/* ── marketAsk ─────────────────────────────────────────────────────────── */

const ASK = {
  tip: 850,
  arm: 1200,
  press: 2100,
  release: 2800,
  type: 3000,
  queue: 4250,
  /** The box closes on Add to queue, leaving the pin and the queue's row. */
  added: 4400,
  send: 5750,
  think: 5950,
  lead: 6550,
  length: 8000,
};

/** At rest on the chart while comment mode goes on and the drop is taken and
    noted; across to the chat panel's foot as the comment waits in the
    composer, then up the panel to the message as it lands. */
const ASK_SHOTS: [Shot, ...Shot[]] = [
  // the chart column whole under the stock's header, the toolbar's tools
  // on it, the chat panel's edge beside it
  shot(0, 0, 0, REST),
  // the panel from the toolbar's line down to the composer, the chart's
  // right half beside it
  shot(5300, PAGE.width, PAGE.height, CHAT_REST, 800),
  // the panel from its header down, the message on top
  shot(6300, PAGE.width, 0, CHAT_REST, 750),
];

/** On the Comment button, clear of the pointer its own icon draws. */
const ON_COMMENT = { x: COMMENT_BTN.x + 2.5, y: COMMENT_BTN.y + 2.5 };

/** Comment, its tooltip up first; the drag from the region's corner,
    pressed at the end of the dwell, to its far corner and off it; Add to
    queue in the box; then send in the chat's composer. */
const ASK_POINTER = pointerScript(
  { at: 200, x: 238, y: 142 },
  {
    comment: { ...ON_COMMENT, arrive: 800, click: ASK.arm, leave: 1400 },
    anchor: { ...onPage(REGION.x0, REGION.y0), arrive: 2000, leave: 2100 },
    release: { ...onPage(REGION.x1, REGION.y1), arrive: 2800 },
    clear: { ...onPage(REGION.x1 - 8, REGION.y1 + 12), arrive: 3100 },
    queue: { ...onPage(QUEUE.x, QUEUE.y), arrive: 4100, click: ASK.queue, leave: 4500 },
    send: { ...SEND, arrive: 5550, click: ASK.send },
  }
);

function MarketAsk() {
  const { t: tr } = useTranslation();
  const t = useSceneTime();
  const place = useRegionLabel(...REGION_DATES);
  const placeholder = useDeckPlaceholder();
  const sent = t >= ASK.send;
  const drafting = t >= ASK.release && t < ASK.added;
  const queued = t >= ASK.added && !sent;
  return (
    <Camera shots={ASK_SHOTS} page={PAGE} pointer={ASK_POINTER}>
      <MarketPage
        comments={{ on: t >= ASK.arm, open: t >= ASK.added ? 1 : 0 }}
        chart={
          <Candles series={NVDA} scale={SCALE}>
            {t >= ASK.arm && <ModeHint at={ASK.arm} />}
            {t >= ASK.press && t < ASK.release && (
              <DragArea {...dragBox(REGION, ASK_POINTER.path, CHART, t, ASK)} />
            )}
            {drafting && (
              <>
                <SpotOutline picked {...REGION} />
                <CommentPin n={1} state="draft" left={REGION.x1} top={REGION.y1} />
                <CommentBox
                  left={BOX.left}
                  top={BOX.top}
                  head={place}
                  pressed={ASK.queue}
                  text={t >= ASK.type ? <Typed text={NOTE} at={ASK.type} cps={20} /> : undefined}
                />
              </>
            )}
            {t >= ASK.added && (
              <CommentPin
                n={1}
                state={t < ASK.send ? 'queued' : t < ASK.think ? 'sent' : 'working'}
                left={REGION.x1}
                top={REGION.y1}
              />
            )}
          </Candles>
        }
        title={sent ? NOTE : undefined}
        deck={
          queued ? { node: <QueuedDeck at={ASK.added} words={NOTE} />, placeholder } : undefined
        }
        transcript={
          sent ? (
            <>
              <SentComment at={ASK.send} words={NOTE} place={place} />
              {t >= ASK.think && t < ASK.lead && (
                <Loader color="var(--color-icon-muted)" className="text-[10px]" />
              )}
              <Reply text={LEAD} at={ASK.lead} cps={60} />
            </>
          ) : undefined
        }
        sendAt={[ASK.send]}
      />
      {t >= ASK.tip && t < ASK.arm && (
        <Tip x={ON_COMMENT.x} y={COMMENT_BTN.y + 9}>
          {tr('pageViewer:comments.modeTitleChart')}
        </Tip>
      )}
    </Camera>
  );
}

/* ── marketDraw ────────────────────────────────────────────────────────── */

/** The layer's chip in the panel (`InlineChartLayerCard` beside the live
    chart), right after the run that published it: the chart it is on, and
    how many layers the turn put there. */
function LayerChip() {
  const { t } = useTranslation();
  return (
    <span
      className="intro-pop flex w-fit items-center gap-[4px] rounded-full border px-[5px] py-[3px] text-[6.5px] leading-none"
      style={{
        background: 'var(--color-bg-tool-card)',
        borderColor: 'var(--color-border-muted)',
        color: 'var(--color-text-tertiary)',
      }}
    >
      <Check className="h-[7px] w-[7px] shrink-0" style={{ color: 'var(--color-profit)' }} />
      <span className="whitespace-nowrap">
        <span className="font-semibold" style={{ color: 'var(--color-text-primary)' }}>
          NVDA
        </span>
        {' · 1D · '}
        {t('chat.chartLayerCard.chipShownCount', { count: 1 })}
      </span>
    </span>
  );
}

/** One ExecuteCode run builds the layer and publishes it: its row settles,
    the chip lands under it and every label lands on the chart at once; then
    the answer goes under the comment, and its line lands in the turn. */
const DRAW = {
  run: 250,
  published: 1500,
  answer: 1900,
  click: 5600,
  length: 8600,
};

const EVENTS = [
  NVDA_EVENTS.earnings,
  NVDA_EVENTS.exportRule,
  NVDA_EVENTS.fedCut,
  NVDA_EVENTS.upgrade,
];

const CARD_W = 124;
const CARD = besidePin(SCALE, REGION.x1, REGION.y1, CARD_W);

/** On the chat panel while the turn comes in, its message on top and the
    chart's right half beside it; then across to rest on the chart, where the
    layer labels each move and the comment's pin opens on the answer. */
const DRAW_SHOTS: [Shot, ...Shot[]] = [
  shot(0, PAGE.width, 0, CHAT_REST),
  // the chart from its toolbar down, so the card under the pin shows whole
  shot(4900, 0, PAGE.height, REST, 700),
];

/** Across from the chat to the comment's pin, clicked to open its card. */
const DRAW_POINTER = pointerScript(
  { at: 3700, x: CHAT_X + 120, y: 238 },
  {
    pin: { ...ON_PIN, arrive: 5400, click: DRAW.click },
  }
);

function MarketDraw() {
  const t = useSceneTime();
  const place = useRegionLabel(...REGION_DATES);
  const answered = t >= DRAW.answer;
  const open = t >= DRAW.click + 60;
  return (
    <Camera shots={DRAW_SHOTS} page={PAGE} pointer={DRAW_POINTER}>
      <MarketPage
        title={NOTE}
        comments={{ open: 1, unread: answered && !open ? 1 : 0 }}
        chart={
          <Candles series={NVDA} scale={SCALE}>
            {EVENTS.map((ev) => (
              <LayerLabel key={ev.title} scale={SCALE} el={ev} at={DRAW.published} />
            ))}
            {open && <SpotOutline {...REGION} />}
            <CommentPin
              n={1}
              state={answered ? 'sent' : 'working'}
              unread={answered && !open}
              open={open}
              left={REGION.x1}
              top={REGION.y1}
            />
            {open && (
              <CommentCard
                left={CARD.left}
                top={CARD.top}
                width={CARD_W}
                n={1}
                words={NOTE}
                reply={ANSWER}
              />
            )}
          </Candles>
        }
        transcript={
          <>
            <SentComment at={PAST} words={NOTE} place={place} />
            <Reply text={LEAD} />
            <PublishRow at={DRAW.run} done={DRAW.published} />
            {t >= DRAW.published && <LayerChip />}
            <CommentReplyLine at={DRAW.answer} read={DRAW.click + 60} text={ANSWER} />
          </>
        }
      />
    </Camera>
  );
}

export const marketScenes = {
  marketAsk: { Scene: MarketAsk, length: ASK.length, anchor: 'center' },
  marketDraw: { Scene: MarketDraw, length: DRAW.length, anchor: 'center' },
} satisfies Partial<Record<IntroVisualId, IntroScene>>;
