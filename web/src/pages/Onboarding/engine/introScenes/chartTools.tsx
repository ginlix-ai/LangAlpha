import type { ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import {
  Check,
  ChevronDown,
  PenLine,
  Plus,
  Ruler,
  SquareDashedMousePointer,
  X,
  type LucideIcon,
} from 'lucide-react';
import { cn } from '@/lib/utils';
import { Appear, type PointerScript, UserBubble, pointAt } from './kit';
import type { ChartScale } from './nvdaSeries';
import { ToolRow } from './transcriptParts';

/* What goes over and around the chart (candles.tsx) when the user picks out
   part of it: the selection, its pin and comment box, the chip it waits as
   above a composer and the card it is sent as, and the agent's annotate
   call; and the chart toolbar's parts. The market page and the thread's
   chart tab both draw these, so they sit apart from either. */
/* eslint-disable react-refresh/only-export-components -- shared parts:
   the overlays plus the sizes and placement helpers the scenes lay them
   out by */

/* ── the user's selection ──────────────────────────────────────────────── */

/** A region's corners, in chart px. */
interface Region {
  x0: number;
  y0: number;
  x1: number;
  y1: number;
}

/** A region being drawn or held (`SelectionPrimitive`): a teal box, dashed,
    bolder while it is the one being edited. Corners in chart px. */
export function Selection({
  x0,
  y0,
  x1,
  y1,
  active = true,
}: {
  x0: number;
  y0: number;
  x1: number;
  y1: number;
  active?: boolean;
}) {
  return (
    <span
      className="absolute border-dashed"
      style={{
        left: Math.min(x0, x1),
        top: Math.min(y0, y1),
        width: Math.abs(x1 - x0),
        height: Math.abs(y1 - y0),
        borderWidth: active ? 1.25 : 1,
        borderColor: `rgba(31, 184, 201, ${active ? 0.98 : 0.6})`,
        background: 'rgba(31, 184, 201, 0.12)',
      }}
    />
  );
}

/** The numbered marker a confirmed selection keeps (`.selection-pin`): amber,
    its bottom left corner sharp, a comment's tail. */
export function Pin({ n, left, top }: { n: number; left?: number; top?: number }) {
  return (
    <span
      className={cn(
        'flex h-2.5 min-w-2.5 shrink-0 items-center justify-center px-[3px]',
        'text-[6px] font-bold leading-none',
        left != null && 'intro-pop absolute'
      )}
      style={{
        left,
        top,
        borderRadius: '5px 5px 5px 1.5px',
        background: 'var(--color-accent-primary)',
        color: 'var(--color-text-on-accent)',
      }}
    >
      {n}
    </span>
  );
}

/** The comment box beside a fresh selection (`SelectionCommentOverlay`): its
    pin, the note's pill with Add (Plus) while it is new, and the discard X.
    Its width, for placing it. */
const COMMENT_W = 100;

export function CommentBox({
  left,
  top,
  width = COMMENT_W,
  text,
  pressed = false,
}: {
  left: number;
  top: number;
  width?: number;
  /** What has been typed; the placeholder until then. */
  text?: ReactNode;
  pressed?: boolean;
}) {
  const { t } = useTranslation();
  return (
    <span className="intro-pop absolute flex items-center gap-[3px]" style={{ left, top, width }}>
      <Pin n={1} />
      <span
        className="flex h-4 min-w-0 flex-1 items-center gap-[2px] rounded-full border pl-[5px] pr-[2px]"
        style={{ background: 'var(--color-bg-elevated)', borderColor: 'var(--color-border-muted)' }}
      >
        <span
          className="min-w-0 flex-1 truncate text-[6px] leading-none"
          style={{
            color: text == null ? 'var(--color-text-tertiary)' : 'var(--color-text-primary)',
          }}
        >
          {text ?? t('marketView.selection.commentPlaceholder')}
        </span>
        <span
          className="flex h-3 w-3 shrink-0 items-center justify-center rounded-full transition-transform duration-press"
          style={{
            background: 'var(--color-btn-primary-bg)',
            color: 'var(--color-btn-primary-text)',
            transform: pressed ? 'scale(0.85)' : undefined,
          }}
        >
          {pressed ? <Check className="h-[6px] w-[6px]" /> : <Plus className="h-[7px] w-[7px]" />}
        </span>
      </span>
      <span
        className="flex h-[11px] w-[11px] shrink-0 items-center justify-center rounded-full border"
        style={{
          background: 'var(--color-bg-elevated)',
          borderColor: 'var(--color-border-muted)',
          color: 'var(--color-text-secondary)',
        }}
      >
        <X className="h-[6px] w-[6px]" />
      </span>
    </span>
  );
}

/** The comment box's Add button, from the box's top left. */
export const addButton = (box: { left: number; top: number }, w = COMMENT_W) => ({
  x: box.left + w - 23,
  y: box.top + 8,
});

/** Where the comment box sits for a region spanning chart x `left` to
    `right` with its top at `top`: past the right edge, or flipped left of the
    region when the pane runs out, as the overlay places it. Same for a pin
    `w` wide. */
export function besideRegion(
  k: ChartScale,
  left: number,
  right: number,
  top: number,
  w = COMMENT_W
): { left: number; top: number } {
  const gap = 3;
  const x = right + gap + w + 2 > k.paneW ? left - gap - w : right + gap;
  return { left: Math.max(x, 2), top };
}

/** A chart selection waiting above the composer (`SelectionChips`): what
    it is, the symbol and timeframe, the note, and its X. Pops in at `at`,
    as the comment box's Add hands it over. */
export function SelectionChip({ note, at }: { note: string; at: number }) {
  const { t } = useTranslation();
  return (
    <Appear at={at} pop className="flex min-w-0">
      <span
        className={cn(
          'flex w-fit max-w-full items-center gap-[3px] rounded-[3px] border py-[2.5px] pl-[5px] pr-[3px]',
          'text-[6px] leading-none'
        )}
        style={{
          backgroundColor: 'var(--color-bg-page)',
          backgroundImage: 'linear-gradient(var(--color-bg-surface), var(--color-bg-surface))',
          borderColor: 'var(--color-border-muted)',
          color: 'var(--color-text-secondary)',
        }}
      >
        <span className="min-w-0 truncate">
          {t('marketView.selection.chipRegion', { symbol: 'NVDA', timeframe: '1day' })} · "{note}"
        </span>
        <X className="h-[6px] w-[6px] shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
      </span>
    </Appear>
  );
}

/** A sent selection under the user's message (`InlineSelectionCards`): the
    region glyph on its tile, what was selected on which chart, the note. */
export function SelectionCard({ at, note }: { at: number; note: string }) {
  const { t } = useTranslation();
  return (
    <Appear at={at} className="flex justify-end">
      <span
        className="flex w-[128px] max-w-full items-center gap-[5px] rounded-[5px] border px-[5px] py-[4px]"
        style={{ background: 'var(--color-bg-elevated)', borderColor: 'var(--color-border-muted)' }}
      >
        <span
          className="flex h-[14px] w-[14px] shrink-0 items-center justify-center rounded-[3px]"
          style={{ background: 'var(--color-accent-soft)', color: 'var(--color-accent-light)' }}
        >
          <SquareDashedMousePointer className="h-[7px] w-[7px]" />
        </span>
        <span className="flex min-w-0 flex-col gap-[3px] leading-none">
          <span
            className="truncate text-[6px] font-semibold"
            style={{ color: 'var(--color-text-primary)' }}
          >
            {t('marketView.selection.cardRegionTitle')}
            <span className="font-normal" style={{ color: 'var(--color-text-tertiary)' }}>
              {' · '}NVDA 1day
            </span>
          </span>
          <span className="truncate text-[6px]" style={{ color: 'var(--color-text-secondary)' }}>
            “{note}”
          </span>
        </span>
      </span>
    </Appear>
  );
}

/** The user's message as it goes out with the selection under it. */
export function SentSelection({ at, text, note }: { at: number; text: string; note: string }) {
  return (
    <>
      <UserBubble at={at}>{text}</UserBubble>
      <SelectionCard at={at + 60} note={note} />
    </>
  );
}

/** The region while it is dragged out: its anchor corner held, its far
    corner on the pointer (`path`, in page px, less the chart's `origin`)
    from `press`, and the whole of it from `release`. The overlay that
    draws it stays inline in each scene: as a shared component its
    constants became props, and it compiled to more than the copies it
    replaced. */
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

/** One `draw_chart_annotation` call (`ToolCallRow`): annotating while it
    runs, then the settled row naming the chart it drew on. */
export function AnnotateRow({ at, done }: { at: number; done: number }) {
  const { t } = useTranslation();
  return (
    <ToolRow
      icon={PenLine}
      at={at}
      done={done}
      running={{ label: t('toolArtifact.inProgress.annotatingChart') }}
      settled={{ label: t('toolArtifact.tool.annotateChart'), pill: 'NVDA · 1D' }}
    />
  );
}

/* ── the toolbar ───────────────────────────────────────────────────────── */

/** A toolbar button (`.chart-tool-btn`): `on` is the tool that is armed. */
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

/** Region, then Price level: the hand-off to the agent, never in a menu. */
export function SelectionTools({ armed = false }: { armed?: boolean }) {
  return (
    <span className="flex gap-[2px]">
      <ToolBtn icon={SquareDashedMousePointer} on={armed} />
      <ToolBtn icon={Ruler} />
    </span>
  );
}

/** Clear, shown while the agent's drawing is on the chart. */
export function ClearButton() {
  const { t } = useTranslation();
  return <ToolBtn icon={X}>{t('marketView.chart.clearAnnotations')}</ToolBtn>;
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
          {t('marketView.chart.more')}
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
