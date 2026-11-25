import type { CSSProperties, ReactNode } from 'react';
import { Trans, useTranslation } from 'react-i18next';
import {
  Check,
  ChevronDown,
  MessageSquare,
  MessageSquarePlus,
  Plus,
  SquareChevronRight,
  X,
  type LucideIcon,
} from 'lucide-react';
import { LangAlphaMark } from '@/components/brand/LangAlphaMark';
import { useLocale } from '@/hooks/useLocale';
import { chartTitle } from '@/lib/artifactTarget';
import { relativeTime } from '@/lib/format';
import { flattenMarkdown } from '@/lib/plainLine';
import { personLabel } from '@/lib/sharedThreads';
import { cn } from '@/lib/utils';
import { markLabel } from '@/pages/ChatAgent/components/artifactComments/chartMarkLabels';
import type { ChartRegionMark } from '@/types/artifacts';
import { useSceneTime } from './clock';
import { Appear, Loader, type PointerScript, pointAt, pressAt } from './kit';
import type { ChartScale } from './nvdaSeries';
import { ToolRow } from './transcriptParts';

/* What goes over and around the chart (candles.tsx) when the user comments
   on it, after the comment layer every viewer has (`ChartComments`): the
   Comment toggle among the chart's tools, the mode's hint, the area a drag
   draws, the comment box, the numbered pin and the card it opens; in the
   chat, the queue in the composer, the comment a send carries, and the line
   the agent's answer leaves; and the chart toolbar's parts. The market page
   and the thread's chart tab both draw these, so they sit apart from either. */
/* eslint-disable react-refresh/only-export-components -- shared parts:
   the overlays plus the sizes and placement helpers the scenes lay them
   out by */

/** The chart the scenes comment on, as its comments are kept. */
const CHART_ID = 'NVDA:1day';

/** "NVDA · 1D", as every comment line names the chart. */
export const ON_CHART = chartTitle(CHART_ID);

/** A region's dates as the comment UI names them ("Aug 28 to Sep 11"). */
export function useRegionLabel(start: string, end: string): string {
  const { t } = useTranslation();
  const mark: ChartRegionMark = { type: 'region', start, end, low: 0, high: 0 };
  return markLabel(t, mark);
}

/* ── on the chart ──────────────────────────────────────────────────────── */

/** A region's corners, in chart px. */
interface Region {
  x0: number;
  y0: number;
  x1: number;
  y1: number;
}

const boxOf = ({ x0, y0, x1, y1 }: Region): CSSProperties => ({
  left: Math.min(x0, x1),
  top: Math.min(y0, y1),
  width: Math.abs(x1 - x0),
  height: Math.abs(y1 - y0),
});

/** The area being dragged out (`.page-comments-area`): dashed amber, the
    annotation colour, with a faint fill. Corners in chart px. */
export function DragArea(region: Region) {
  return (
    <span
      className="absolute rounded-[2px] border border-dashed"
      style={{
        ...boxOf(region),
        borderColor: 'var(--color-accent-primary)',
        background: 'color-mix(in srgb, var(--color-accent-primary) 7%, transparent)',
      }}
    />
  );
}

/** The region a comment is on, outlined in the pick blue
    (`.page-comments-outline`): `picked` while its box is open, a stronger
    fill further out; plain while its card is. */
export function SpotOutline({ picked = false, ...region }: Region & { picked?: boolean }) {
  return (
    <span
      className="intro-pop absolute rounded-[1.5px]"
      style={{
        ...boxOf(region),
        outline: '0.75px solid var(--color-info)',
        outlineOffset: picked ? 1 : 0.5,
        background: `color-mix(in srgb, var(--color-info) ${picked ? 16 : 10}%, transparent)`,
      }}
    />
  );
}

/** How a pin reads: the draft's plus, waiting in the queue, with the agent,
    or the agent at work on it. */
type PinState = 'draft' | 'queued' | 'sent' | 'working';

/** Its size in chart px: the app's 22px pin at the chart's scale. */
const PIN = 10;

/**
 * A comment's mark (`.page-comment-pin`, PinBadge.css): a ring around a
 * disc holding its number. On the chart (`left`, `top` set) it is a speech
 * bubble whose bottom-left corner is the spot, so it sits up and to the
 * right of it; beside text it is a round badge a size down. `unread` is the
 * amber dot an answer not yet opened wears; `open` the ring of the pin whose
 * card is showing.
 */
export function CommentPin({
  n,
  state,
  unread = false,
  open = false,
  left,
  top,
  size = PIN,
}: {
  n: number;
  state: PinState;
  unread?: boolean;
  open?: boolean;
  left?: number;
  top?: number;
  size?: number;
}) {
  const onChart = left != null;
  const r = size / 2;
  const queued = state === 'queued';
  const keyline = 'var(--color-bg-page)';
  const ring = open ? `, 0 0 0 1.25px var(--color-text-primary), 0 0 0 1.9px ${keyline}` : '';
  return (
    <span
      className={cn('flex shrink-0 box-border', onChart && 'intro-pop absolute')}
      style={{
        left,
        top: onChart ? top! - size : undefined,
        minWidth: size,
        height: size,
        padding: queued ? size * 0.07 : size * 0.14,
        borderRadius: onChart ? `${r}px ${r}px ${r}px 0` : r,
        border: queued ? `${size * 0.07}px dashed var(--color-text-secondary)` : undefined,
        background:
          state === 'draft'
            ? 'var(--color-text-primary)'
            : queued
              ? onChart
                ? keyline
                : 'transparent'
              : 'var(--color-accent-primary)',
        boxShadow: onChart ? `0 0 0 0.6px ${keyline}${ring}, var(--shadow-card)` : undefined,
        transformOrigin: '0 100%',
        position: onChart ? 'absolute' : 'relative',
      }}
    >
      <span
        className="grid flex-1 place-items-center overflow-hidden font-semibold leading-none tabular-nums"
        style={{
          borderRadius: r,
          fontSize: size * 0.46,
          background:
            state === 'draft' || queued
              ? onChart
                ? keyline
                : 'transparent'
              : 'var(--color-bg-elevated)',
          color: state === 'working' ? 'var(--color-accent-primary)' : 'var(--color-text-primary)',
        }}
      >
        {state === 'draft' ? (
          <Plus style={{ width: size * 0.5, height: size * 0.5 }} strokeWidth={2.5} />
        ) : state === 'working' ? (
          <Loader />
        ) : (
          n
        )}
      </span>
      {unread && (
        <span
          className="absolute rounded-full"
          style={{
            top: -size * 0.09,
            right: -size * 0.09,
            width: size * 0.36,
            height: size * 0.36,
            background: 'var(--color-accent-primary)',
            boxShadow: `0 0 0 ${size * 0.07}px var(--pin-ground, ${keyline})`,
          }}
        />
      )}
    </span>
  );
}

/** The mode's hint at the top of the chart (`.page-comments-hint`): what a
    drag, a click and a click on a shape do, and its own way out. */
export function ModeHint({ at }: { at: number }) {
  const { t } = useTranslation();
  return (
    <Appear at={at} pop className="absolute inset-x-[6px] top-[4px] z-[5] flex justify-center">
      <span
        className="flex min-w-0 items-center gap-[2px] rounded-full border py-[1px] pl-[5px] pr-[1px] text-[5px] leading-none"
        style={{
          color: 'var(--color-text-secondary)',
          background: 'var(--color-bg-elevated)',
          borderColor: 'var(--color-border-muted)',
          boxShadow: 'var(--shadow-card)',
        }}
      >
        <span className="min-w-0 truncate">{t('pageViewer:comments.modeHintChart')}</span>
        <span
          className="flex h-[10px] shrink-0 items-center gap-[2px] rounded-full pl-[3px] pr-[2px] font-medium"
          style={{ color: 'var(--color-text-primary)' }}
        >
          {t('pageViewer:comments.exitMode')}
          <kbd
            className="rounded-[2px] border px-[1.5px] font-sans text-[4.5px] leading-[6px]"
            style={{
              borderColor: 'var(--color-border-muted)',
              color: 'var(--color-text-tertiary)',
            }}
          >
            Esc
          </kbd>
        </span>
      </span>
    </Appear>
  );
}

/** The popover's look, which the comment box and the card share. */
const POPOVER: CSSProperties = {
  background: 'var(--color-bg-popover)',
  borderColor: 'var(--color-border-default)',
  boxShadow: '0 4px 10px rgba(0, 0, 0, 0.14)',
};

/** The comment box's width on the market page, in chart px; a narrow chart
    passes its own. */
const BOX_W = 130;
/** Narrower than this, the footer's buttons go together to a line of their
    own, right-aligned, as the composer's footer wraps in a narrow panel. */
const STACK_BELOW = 115;

/**
 * The comment box beside a fresh spot (`CommentComposer` in its popover):
 * what the spot is and the ×, the words (the placeholder until they are
 * typed), where they go, and the footer, For the agent ticked, Send now
 * quiet and Add to queue filled. `pressed` is when Add to queue is pressed.
 */
export function CommentBox({
  left,
  top,
  width = BOX_W,
  head,
  text,
  pressed,
}: {
  left: number;
  top: number;
  width?: number;
  head: string;
  text?: ReactNode;
  pressed: number;
}) {
  const { t } = useTranslation();
  const now = useSceneTime();
  return (
    <span
      className="intro-pop absolute z-10 flex flex-col gap-[3px] rounded-[5px] border px-[5px] pb-[4px] pt-[4px] text-left"
      style={{ left, top, width, ...POPOVER }}
    >
      <span className="flex min-w-0 items-center gap-[3px]">
        <span
          className="min-w-0 flex-1 truncate text-[5.5px] leading-none"
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          {head}
        </span>
        <X className="h-[6px] w-[6px] shrink-0" style={{ color: 'var(--color-text-secondary)' }} />
      </span>
      <span
        className="block min-h-[15px] text-[6.5px] leading-[1.45]"
        style={{
          color: text == null ? 'var(--color-text-quaternary)' : 'var(--color-text-primary)',
        }}
      >
        {text ?? t('pageViewer:comments.placeholder')}
      </span>
      <span
        className="flex min-w-0 items-center gap-[2px] text-[5.5px] leading-none"
        style={{ color: 'var(--color-text-tertiary)' }}
      >
        {t('pageViewer:comments.to')}
        <span className="truncate font-medium" style={{ color: 'var(--color-text-secondary)' }}>
          {t('pageViewer:comments.thisThread')}
        </span>
        <ChevronDown className="h-[5px] w-[5px] shrink-0" />
      </span>
      <span
        className={cn(
          'flex items-center gap-[2px] text-[5.5px] leading-none',
          width < STACK_BELOW && 'flex-wrap gap-y-[3px]'
        )}
      >
        <span
          className="mr-auto flex shrink-0 items-center gap-[3px] whitespace-nowrap"
          style={{ color: 'var(--color-text-primary)' }}
        >
          <span
            className="flex h-[6px] w-[6px] items-center justify-center rounded-[1.5px]"
            style={{
              background: 'var(--color-btn-primary-bg)',
              color: 'var(--color-btn-primary-text)',
            }}
          >
            <Check className="h-[5px] w-[5px]" strokeWidth={3} />
          </span>
          {t('pageViewer:comments.forAgent')}
        </span>
        <span className="ml-auto flex shrink-0 items-center gap-[2px]">
          <span
            className="flex h-[11px] shrink-0 items-center rounded-[3px] px-[3px]"
            style={{ color: 'var(--color-text-secondary)' }}
          >
            {t('pageViewer:comments.sendNow')}
          </span>
          <span
            className="flex h-[11px] shrink-0 items-center gap-[2px] rounded-[3px] px-[4px] font-medium transition-transform duration-press"
            style={{
              background: 'var(--color-btn-primary-bg)',
              color: 'var(--color-btn-primary-text)',
              transform: pressAt(now, pressed),
            }}
          >
            {t('pageViewer:comments.addToQueue')}
            <span className="opacity-60">↵</span>
          </span>
        </span>
      </span>
    </span>
  );
}

/** Where Add to queue sits in a comment box at (`left`, `top`) `width` wide. */
export const queueButton = (box: { left: number; top: number }, width = BOX_W) => ({
  x: box.left + width - 18,
  y: box.top + (width < STACK_BELOW ? 55 : 42),
});

/**
 * Where a box `w` wide opens for a pin whose spot is (`x`, `y`): beside the
 * pin, level with its top, or to the left of the spot where the chart runs
 * out, as the popover flips.
 */
export function besidePin(
  k: ChartScale,
  x: number,
  y: number,
  w: number
): { left: number; top: number } {
  const gap = 3;
  const right = x + PIN + gap;
  return { left: right + w > k.width - 2 ? Math.max(2, x - gap - w) : right, top: y - PIN };
}

/**
 * A comment's card over its pin (`CommentCard` in the popover): its badge,
 * who wrote it and when, and the mark that it reached the agent; the words;
 * the agent's answer under them; then the reply field.
 */
export function CommentCard({
  left,
  top,
  width = BOX_W,
  n,
  words,
  reply,
}: {
  left: number;
  top: number;
  width?: number;
  n: number;
  words: string;
  /** The agent's answer, in markdown with `**bold**` its only mark. */
  reply: string;
}) {
  const { t } = useTranslation();
  const locale = useLocale();
  const muted = { color: 'var(--color-text-tertiary)' };
  return (
    <span
      className="intro-pop absolute z-10 flex flex-col rounded-[5px] border px-[5px] pb-[5px] pt-[4px] text-left"
      style={
        { left, top, width, ...POPOVER, '--pin-ground': 'var(--color-bg-popover)' } as CSSProperties
      }
    >
      <span className="flex min-w-0 items-center gap-[3px] text-[5.5px] leading-none">
        <CommentPin n={n} state="sent" size={8.5} />
        <span className="font-medium" style={{ color: 'var(--color-text-primary)' }}>
          {personLabel(t, { name: null, mine: true })}
        </span>
        <span className="truncate" style={muted}>
          {relativeTime(0, locale, 180_000)}
        </span>
        <span className="flex shrink-0" style={{ color: 'var(--color-accent-primary)' }}>
          <LangAlphaMark className="h-[6px] w-[6px] [stroke-width:4.5]" />
        </span>
      </span>
      <span
        className="pt-[3px] text-[6px] leading-[1.45]"
        style={{ color: 'var(--color-text-primary)' }}
      >
        {words}
      </span>
      <span className="flex flex-col gap-[1.5px] pl-[11.5px] pt-[4px]">
        <span className="flex items-center gap-[3px] text-[5.5px] leading-none">
          <span
            className="grid h-[8px] w-[8px] shrink-0 place-items-center rounded-full border"
            style={{
              borderColor: 'var(--color-border-default)',
              background: 'var(--color-bg-page)',
              color: 'var(--color-text-primary)',
            }}
          >
            <LangAlphaMark className="h-[5.5px] w-[5.5px] [stroke-width:4.5]" />
          </span>
          <span className="font-medium" style={{ color: 'var(--color-text-primary)' }}>
            {t('pageViewer:comments.agent')}
          </span>
          <span style={muted}>{relativeTime(0, locale, 60_000)}</span>
        </span>
        <span className="text-[6px] leading-[1.45]" style={{ color: 'var(--color-text-primary)' }}>
          {reply.split(/\*\*([^*]+)\*\*/).map((part, i) =>
            i % 2 ? (
              <b key={i} className="font-semibold">
                {part}
              </b>
            ) : (
              part
            )
          )}
        </span>
      </span>
      <span
        className="mt-[5px] rounded-[3px] border px-[4px] py-[3px] text-[6px] leading-none"
        style={{
          background: 'var(--color-bg-input)',
          borderColor: 'var(--color-border-input)',
          color: 'var(--color-text-quaternary)',
        }}
      >
        {t('pageViewer:comments.replyPlaceholder')}
      </span>
    </span>
  );
}

/** The region while it is dragged out: its anchor corner held, its far
    corner on the pointer (`path`, in page px, less the chart's `origin`)
    from `press`, and the whole of it from `release`. */
export function dragBox(
  region: Region,
  path: PointerScript['path'],
  origin: { x: number; y: number },
  t: number,
  { press, release }: { press: number; release: number }
): Region {
  if (t >= release) return region;
  const p = pointAt(path, Math.max(t, press));
  return { ...region, x1: p.x - origin.x, y1: p.y - origin.y };
}

/* ── in the chat ───────────────────────────────────────────────────────── */

/**
 * The queue at the top of the composer (`QueuedCommentDeck`), ruled off
 * from the text box: the rail naming how many and on which chart, with
 * Keep as notes; then one row per comment, its badge, its words, and where
 * on the chart it is while the row has room for that.
 */
export function QueuedDeck({ at, words, place }: { at: number; words: string; place?: string }) {
  const { t } = useTranslation();
  const muted = { color: 'var(--color-text-tertiary)' };
  const lead = 'grid grid-cols-[10px_minmax(0,1fr)_auto] items-center gap-[3px]';
  return (
    <Appear
      at={at}
      className="grid min-w-0 gap-[1px] border-b border-dashed pb-[4px]"
      style={{ borderColor: 'var(--color-border-muted)' }}
    >
      <span className={cn(lead, 'min-h-[11px] px-[1px] text-[5.5px] leading-none')}>
        <span
          className="h-[6px] w-[6px] justify-self-center rounded-full border border-dashed"
          style={{ borderColor: 'var(--color-text-secondary)' }}
        />
        <span className="flex min-w-0 items-baseline gap-[3px]">
          <span className="shrink-0 font-medium" style={{ color: 'var(--color-text-primary)' }}>
            {t('chat.artifactComments.deck.queuedCount', { count: 1 })}
          </span>
          <span className="truncate" style={muted}>
            {t('chat.artifactComments.deck.onFile', { title: ON_CHART })}
          </span>
        </span>
        <span className="whitespace-nowrap" style={muted}>
          {t('chat.artifactComments.deck.keepAsNotes')}
        </span>
      </span>
      <span className={cn(lead, 'h-[14px] min-w-0 pl-[1px] pr-[2px]')}>
        <span className="justify-self-center">
          <CommentPin n={1} state="queued" size={9} />
        </span>
        <span
          className="truncate text-[7px] leading-none"
          style={{ color: 'var(--color-text-primary)' }}
        >
          {words}
        </span>
        {place && (
          <span className="max-w-[60px] truncate text-[6.5px] leading-none" style={muted}>
            {place}
          </span>
        )}
      </span>
    </Appear>
  );
}

/** The placeholder the composer takes while a comment waits in its queue. */
export function useDeckPlaceholder(): string {
  const { t } = useTranslation();
  return t('chat.artifactComments.deck.placeholder', { count: 1 });
}

/**
 * The user's message that sent the queued comment (`CommentSendBody`, with
 * no note of its own): the line naming the chart, then the comment, its badge
 * in the sent state, where on the chart it is, and the words.
 */
export function SentComment({ at, words, place }: { at: number; words: string; place: string }) {
  const { t } = useTranslation();
  return (
    <Appear at={at} className="flex justify-end">
      <span
        className="grid max-w-[85%] gap-[3px] rounded-md px-1.5 py-[4px] text-left"
        style={{ background: 'var(--color-bg-elevated)' }}
      >
        <span
          className="text-[6.5px] leading-none"
          style={{ color: 'var(--color-text-secondary)' }}
        >
          {t('chat.artifactComments.message.head', { count: 1, title: ON_CHART })}
        </span>
        <span className="grid grid-cols-[auto_minmax(0,1fr)] gap-[4px]">
          <CommentPin n={1} state="sent" size={9} />
          <span className="grid min-w-0 gap-[2px] leading-none">
            <span
              className="truncate text-[6.5px]"
              style={{ color: 'var(--color-text-secondary)' }}
            >
              {place}
            </span>
            <span className="text-[7.5px]" style={{ color: 'var(--color-text-primary)' }}>
              {words}
            </span>
          </span>
        </span>
      </span>
    </Appear>
  );
}

/**
 * The line the agent's answer under a comment leaves in its turn
 * (`ArtifactCommentReplyLines`): "Replied to comment 1 on NVDA · 1D", the
 * number a link wearing the unread dot until the answer is opened, then the
 * answer's first two lines, its markdown flattened. Lands at `at`; `read` is when its card opens.
 */
export function CommentReplyLine({ at, read, text }: { at: number; read: number; text: string }) {
  const unread = useSceneTime() < read;
  const excerpt = flattenMarkdown(text);
  return (
    <Appear at={at} className="grid gap-[2px]">
      <span
        className="text-[7.5px] leading-[1.5]"
        style={{ color: unread ? 'var(--color-text-primary)' : 'var(--color-text-secondary)' }}
      >
        <Trans
          i18nKey="chat.artifactComments.reply.replied"
          count={1}
          values={{ title: ON_CHART }}
          components={{
            nums: (
              <span
                className="underline underline-offset-[1.5px]"
                style={{
                  color: 'var(--color-text-primary)',
                  textDecorationColor: 'var(--color-text-tertiary)',
                }}
              >
                1
                {unread && (
                  <span
                    className="mx-[0.5px] inline-block h-[3.5px] w-[3.5px] rounded-full align-super"
                    style={{ background: 'var(--color-accent-primary)' }}
                  />
                )}
              </span>
            ),
          }}
        />
      </span>
      <span
        className="line-clamp-2 text-[7.5px] leading-[1.5]"
        style={{ color: 'var(--color-text-secondary)' }}
      >
        {excerpt}
      </span>
    </Appear>
  );
}

/** What the agent says its run does: the call's `description`. */
const PUBLISH = 'Publish the news layer on NVDA';

/** The ExecuteCode call that builds the agent's layer with chartkit and
    publishes it (`ToolCallRow`): its description while it runs, then the
    settled row with the description as its pill. */
export function PublishRow({ at, done }: { at: number; done: number }) {
  const { t } = useTranslation();
  return (
    <ToolRow
      icon={SquareChevronRight}
      at={at}
      done={done}
      running={{ label: `${PUBLISH}...` }}
      settled={{ label: t('toolArtifact:tool.executeCode'), pill: PUBLISH }}
    />
  );
}

/* ── the toolbar ───────────────────────────────────────────────────────── */

/** A toolbar button (`.chart-tool-btn`): `on` is a tool that is held on. */
export function ToolBtn({
  icon: Icon,
  on = false,
  children,
}: {
  icon: LucideIcon;
  on?: boolean;
  children?: ReactNode;
}) {
  return (
    <span
      className={cn(
        'flex h-[11px] shrink-0 items-center justify-center gap-[2px] rounded-[2px] border',
        'text-[5px] leading-none',
        children ? 'px-[2px]' : 'w-[11px]'
      )}
      style={{
        background: on ? 'var(--color-bg-hover)' : 'var(--color-bg-surface)',
        borderColor: on ? 'var(--color-accent-primary)' : 'var(--color-border-elevated)',
        color: on ? 'var(--color-text-primary)' : 'var(--color-text-secondary)',
      }}
    >
      <Icon className="h-[6px] w-[6px] shrink-0" />
      {children}
    </span>
  );
}

/**
 * The chart's comment tools (`CommentTools`, its `chart` variant): Comment,
 * which holds the mode on, then the list, with how many comments are open
 * and, in the unread mark's amber, how many have an answer not yet read.
 */
export function ChartCommentTools({
  on = false,
  open = 0,
  unread = 0,
}: {
  on?: boolean;
  open?: number;
  unread?: number;
}) {
  const { t } = useTranslation();
  return (
    <span className="flex gap-[2px]">
      <ToolBtn icon={MessageSquarePlus} on={on} />
      <ToolBtn icon={MessageSquare}>
        {open > 0 && <span className="tabular-nums">{open}</span>}
        {unread > 0 && (
          <span
            className="whitespace-nowrap rounded-full px-[2px] py-[0.5px] font-medium"
            style={{
              background: 'var(--color-accent-primary)',
              color: 'var(--color-text-on-accent)',
            }}
          >
            {t('pageViewer:comments.unreadBadge', { count: unread })}
          </span>
        )}
      </ToolBtn>
    </span>
  );
}

/** A segmented control (`.interval-selector`), the picked one raised. */
export function Segmented({
  items,
  active,
  mono = false,
  className,
}: {
  items: ReactNode[];
  active: number;
  mono?: boolean;
  className?: string;
}) {
  return (
    <span
      className={cn('flex shrink-0 gap-[1px] rounded-[3px] p-[1px]', className)}
      style={{ background: 'var(--color-bg-surface)' }}
    >
      {items.map((item, i) => (
        <span
          key={i}
          className={cn(
            'flex flex-auto items-center justify-center gap-[1px] whitespace-nowrap rounded-[2px]',
            'px-[2.5px] py-[2px] text-[5px] leading-none',
            mono && 'font-mono',
            i === active ? 'font-semibold' : 'font-medium'
          )}
          style={
            i === active
              ? { background: 'var(--color-bg-card)', color: 'var(--color-text-primary)' }
              : { color: 'var(--color-text-secondary)' }
          }
        >
          {item}
        </span>
      ))}
    </span>
  );
}

/** The interval picker: 1m, 1D (picked), More. */
export function Intervals() {
  const { t } = useTranslation();
  return (
    <Segmented
      mono
      active={1}
      items={[
        '1m',
        '1D',
        <span key="more" className="flex items-center gap-[1px] font-sans">
          {t('marketView:chart.more')}
          <ChevronDown className="h-[5px] w-[5px]" />
        </span>,
      ]}
    />
  );
}

/** Beside a ticker legend the intervals are text (`chart--with-lead`), set
    off by a divider; with the legend taking most of a narrow row the primary
    intervals fold into their dropdown, so the picker is the one button. */
export function FoldedIntervals() {
  return (
    <span
      className="flex shrink-0 items-center gap-[1px] border-l pl-[4px] font-mono text-[5.5px] font-semibold leading-none"
      style={{ borderColor: 'var(--color-border-default)', color: 'var(--color-text-primary)' }}
    >
      1D
      <ChevronDown className="h-[5px] w-[5px]" style={{ color: 'var(--color-text-secondary)' }} />
    </span>
  );
}
