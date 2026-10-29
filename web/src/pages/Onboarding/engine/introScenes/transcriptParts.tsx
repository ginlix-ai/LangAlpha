import { Fragment, type ReactNode } from 'react';
import { ArrowLeft, PanelRight, type LucideIcon } from 'lucide-react';
import { cn } from '@/lib/utils';
import { PAST, useSceneTime } from './clock';
import { Appear, Tag, typedCount } from './kit';

/* A thread's transcript as every scene that opens one draws it: the thread's
   header and title, the column its turns stack in, the agent's prose, and
   the tool rows on their timeline. The chat, thread, market and automations
   scenes all show the same chat page, so its parts live here once. Each is
   a small copy of the component the app draws it with (named on each).

   The chat and automations scenes draw their tool rows `dense`, bare with
   no icon column, where the thread scenes keep the app's icon column. */

/** The thread header's height, and the file panel's tab strip's beside it:
    the app's 53px top bar at the scenes' scale. Every scene draws it the
    same, so whatever sits under it (a popover, a chip the pointer clicks, a
    shot's top edge) can be placed from it. */
export const HEADER_H = 28;

/**
 * A thread page's own header, `ChatView`'s top bar, `HEADER_H` tall: back
 * and the thread's `title`, the `agent` it runs with, and `children` (the
 * header's buttons) at its right. The bar's padding and gaps are the app's
 * at the scenes' scale, 9/16 as the panel's chrome is drawn: 16px of padding
 * is 9px, the back button's 8px is 4.5px around the arrow, and the 16px gap
 * after it is 9px. The app's bar names no agent; the scenes add the tag to
 * say whose thread it is, and set it where the bar sets what follows the
 * title, in the same group a gap after it, so the title is what truncates.
 * `className` places the header in its page.
 */
export function ThreadHeader({
  title,
  agent,
  className,
  children,
}: {
  title: ReactNode;
  /** Left out where the title already names the agent, as Home's does. */
  agent?: ReactNode;
  className?: string;
  children?: ReactNode;
}) {
  return (
    <div
      className={cn('flex shrink-0 items-center gap-[9px] border-b px-[9px]', className)}
      style={{ height: HEADER_H, borderColor: 'var(--color-border-muted)' }}
    >
      <span className="flex shrink-0 p-[4.5px]">
        <ArrowLeft className="h-2.5 w-2.5" style={{ color: 'var(--color-text-tertiary)' }} />
      </span>
      <span
        className="min-w-0 truncate text-[8px] font-medium leading-none"
        style={{ color: 'var(--color-text-primary)' }}
      >
        {title}
      </span>
      {agent && <Tag>{agent}</Tag>}
      {children && <span className="ml-auto flex shrink-0">{children}</span>}
    </div>
  );
}

/** The chat header's panel toggle while the panel is shut: in the header's
    ink, and pulled past the header's padding by its own, as the app pulls
    it (`-mr-2`), so its glyph sits where the open panel's toggle does. */
export function PanelToggle() {
  return (
    <span
      className="-mr-[4.5px] flex h-4 w-4 items-center justify-center"
      style={{ color: 'var(--color-text-primary)' }}
    >
      <PanelRight className="h-[9px] w-[9px]" />
    </span>
  );
}

/**
 * A chat column's transcript, `ChatView`'s message area: from the top while
 * it fits, then held on its newest lines with the oldest running off the
 * top, as a thread scrolls with its replies. A `settled` transcript has
 * history above it, so it sits on the composer from the first frame.
 */
export function Transcript({
  settled = false,
  className,
  children,
}: {
  settled?: boolean;
  className?: string;
  children: ReactNode;
}) {
  return (
    <div className={cn('flex min-h-0 flex-1 flex-col justify-end overflow-hidden', className)}>
      <div className={cn('flex shrink-0 flex-col gap-1.5', !settled && 'min-h-full')}>
        {children}
      </div>
    </div>
  );
}

/**
 * The agent's prose (`TextMessageContent`, the chat variant of `Markdown`):
 * body text, `**bold**` lead-ins, and a workspace path in backticks drawn as
 * the link the app makes of it, the lines 1.5 apart as the chat variant's
 * `leading-normal` sets them. Streams in from `at`, `cps` characters a
 * second, and takes no room until its first character lands; with no `at`
 * it was written before the scene began, and outside a clock it is whole.
 */
export function Reply({ text, at = PAST, cps = 150 }: { text: string; at?: number; cps?: number }) {
  let left = Math.max(typedCount(useSceneTime(), at, cps), 0);
  if (left === 0) return null;
  const out: ReactNode[] = [];
  text
    .split(/(\*\*[^*]+\*\*|`[^`]+`)/)
    .filter(Boolean)
    .forEach((part, i) => {
      if (left <= 0) return;
      const bold = part.startsWith('**');
      const link = part.startsWith('`');
      const body = bold ? part.slice(2, -2) : link ? part.slice(1, -1) : part;
      const piece = body.slice(0, left);
      left -= body.length;
      out.push(
        bold ? (
          <b key={i} className="font-semibold">
            {piece}
          </b>
        ) : link ? (
          <span
            key={i}
            className="underline decoration-from-font underline-offset-[1.5px]"
            style={{ color: 'var(--color-accent-primary)' }}
          >
            {piece}
          </span>
        ) : (
          <Fragment key={i}>{piece}</Fragment>
        )
      );
    });
  return (
    <p className="text-[8px] leading-[1.5]" style={{ color: 'var(--color-text-primary)' }}>
      {out}
    </p>
  );
}

/** What a tool row says at one stage: its label, and the mono pill naming
    what it acted on when there is one. */
interface RowLine {
  label: string;
  pill?: string;
}

/**
 * One tool call (`ToolCallRow`, or `ReasoningRow` for a think): its icon,
 * then what it says. While it runs the label shimmers, then it settles. A
 * file call keeps one pill throughout ("Writing" then "Write", beside
 * `notes.md`); a memory call runs as one phrase and settles with its topic
 * as the pill. `after` follows the settled line where a pill would, as a
 * think's chevron does. The app draws no spinner and no check on these
 * rows. A row with no `running` is a call the thread made before the scene
 * began, so it is settled from the start.
 *
 * As the app's row line does, the label and pill share a baseline, and a
 * settled label stays whole so the pill is what truncates; a running label
 * may truncate. A full row keeps the app's icon column and a little
 * padding; a `dense` row is bare, its icon its whole height.
 */
export function ToolRow({
  icon: Icon,
  running,
  settled,
  after,
  at = PAST,
  done = PAST,
  dense = false,
}: {
  icon: LucideIcon;
  running?: RowLine;
  settled: RowLine;
  after?: ReactNode;
  at?: number;
  done?: number;
  dense?: boolean;
}) {
  const now = useSceneTime();
  const live = running != null && now < done;
  const { label, pill } = live ? running : settled;
  const muted = { color: 'var(--Labels-Secondary)' };
  return (
    <Appear at={at} className={cn('flex min-w-0 items-center gap-1.5', !dense && 'py-[3px]')}>
      {dense ? (
        <Icon className="h-2 w-2 shrink-0" style={muted} />
      ) : (
        <span className="flex h-[11px] w-3.5 shrink-0 items-center justify-center">
          <Icon className="h-2 w-2" style={muted} />
        </span>
      )}
      <span className="flex min-w-0 items-baseline gap-1.5">
        <span
          className={cn(
            live ? 'truncate' : 'shrink-0 whitespace-nowrap',
            'text-[8px] leading-none',
            live && 'intro-shimmer'
          )}
          style={live ? undefined : { color: 'var(--color-text-primary)' }}
        >
          {label}
        </span>
        {pill && (
          <span className="truncate font-mono text-[7px] leading-none" style={muted}>
            {pill}
          </span>
        )}
        {!live && after}
      </span>
    </Appear>
  );
}

/**
 * A turn's tool calls threaded on the timeline's rule (`ActivityBlock`). As
 * the app draws it, the rule is a short segment from one row's icon to the
 * next, clear of each icon, and ends at the first and last icons; kit.css
 * draws it (`.intro-tl`). Under `dense` rows, which are bare, the rows sit
 * 10px apart so the segment has room between them.
 */
export function Timeline({ dense = false, children }: { dense?: boolean; children: ReactNode }) {
  return (
    <div className={cn('intro-tl flex flex-col', dense && 'intro-tl--dense gap-[10px]')}>
      {children}
    </div>
  );
}
