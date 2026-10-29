import React, { useEffect, useId, useRef, useState } from 'react';
import { AlertCircle, ChevronRight, StopCircle, type LucideIcon } from 'lucide-react';
import { Loader } from '@/components/ui/loader';
import { useNow } from '@/hooks/useNow';
import { formatClock } from '@/lib/elapsed';
import { cn } from '@/lib/utils';
import { FoldPanel } from './FoldPanel';

/**
 * The line subagents and analyst hand-offs share in a reply, so the two read
 * as one family: who does the work on a round avatar, what it was asked, and
 * under that where it stands, then what it is doing, what it returned or who
 * works on it, with the time and the way in on the right. The status leads
 * the second line rather than following the title, so every row is two lines
 * whatever its run has to say. No card: the line sits in the reply like
 * the turn's other rows. A row that opens something fills on hover, as a
 * rounded box held a little inside the reply on either side, since the reply
 * clips whatever passes its edge and a fill that met it would look cut; the
 * line's content sits that box's padding in. A group adds a one-line header
 * whose icon sits in the avatars' column.
 *
 * Both draw where a run stands with `RunGlyph` and `RunStatus`, fold what
 * opens under a line with `RunDisclosure`, and head a group with `RunGroup`,
 * so a glyph's size or a tally's order cannot drift between them.
 */

/** A row: avatar | the two lines | the right. Sized by hand, since a button
 *  does not stretch to its container. */
export const RUN_ROW =
  'relative mx-1 grid w-[calc(100%-0.5rem)] min-w-0 grid-cols-[28px_minmax(0,1fr)_auto] items-center gap-x-3 rounded-lg px-2 py-1.5 text-left';

/** A row or header that opens or folds something: a fill on hover, and a
 *  header's chevron lit. */
export const RUN_HOVER = 'group/run cursor-pointer transition-colors hover:bg-(--color-bg-hover)';

/** The keyboard ring, inside the row's own rounded box: a group's rows sit in
 *  a fold that clips whatever spills past it. */
export const RUN_FOCUS =
  'outline-hidden focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-(--color-focus-ring)';

/** The first line: what it was asked. */
export const RUN_TITLE =
  'min-w-0 truncate text-[0.84375rem] font-medium leading-[1.3] text-(--color-text-primary)';

/** Where it stands, leading the second line in that line's size, in quieter
 *  ink than what follows: a glyph where one says more, and a word. */
const RUN_STATUS = 'inline-flex shrink-0 items-center gap-1 whitespace-nowrap text-(--color-text-tertiary)';

/** The second line: where it stands, then `RunPart`s saying what it is doing,
 *  what it returned, or who works on it and where. */
export const RUN_DETAIL =
  'mt-[3px] flex min-w-0 items-center gap-1.5 truncate text-[0.78125rem] leading-[1.3] text-(--color-text-secondary)';

/** A part of the second line after its status, behind a dot. Laid out as the
 *  line's own items, so a `className` that hides it takes its dot too. */
export function RunPart({ className, children }: { className?: string; children: React.ReactNode }): React.ReactElement {
  return (
    <span className={cn('contents', className)}>
      <span aria-hidden className="shrink-0 text-(--color-text-tertiary)">·</span>
      {children}
    </span>
  );
}

/** A mono clock on the right. */
export const RUN_CLOCK = 'font-mono text-[0.71875rem] tabular-nums text-(--color-text-tertiary)';

/**
 * A working run's clock, in mono so its digits hold still as they tick: from
 * its start once the server says when that was, from when this view first saw
 * it running until then. Hidden from a screen reader, which would otherwise
 * hear it every second.
 */
export function ElapsedClock({ startedAt, className }: { startedAt?: number; className?: string }): React.ReactElement {
  const [seenAt] = useState(Date.now);
  const now = useNow(1000);
  return <span aria-hidden className={cn(RUN_CLOCK, className)}>{formatClock(now - (startedAt ?? seenAt))}</span>;
}

/** A group's one-line header: its icon in the avatars' column, how many, how
 *  they stand, and the fold's chevron right after the words. As wide as its
 *  words, so its fill ends where they do. */
export const RUN_GROUP_HEAD = cn(
  'ml-1 flex h-8 w-fit max-w-[calc(100%-0.5rem)] min-w-0 items-center gap-3 rounded-lg px-2 text-left',
  RUN_HOVER,
  RUN_FOCUS,
);

/** Who or how many, in a header. */
export const RUN_HEAD_TITLE = 'shrink-0 text-[0.84375rem] font-medium text-(--color-text-primary)';

/** The tally beside a header's title. */
export const RUN_HEAD_SUMMARY =
  'flex min-w-0 items-center gap-1.5 overflow-hidden whitespace-nowrap text-[0.78125rem] text-(--color-text-secondary)';

/**
 * Who does the work, as a glyph in a hairline ring, unfilled so it sits on
 * whatever the row is drawn on. Decorative: the row says the same in words.
 * `badge` sits on its lower right.
 */
export function RunAvatar({
  children,
  badge,
  quiet = false,
  className,
}: {
  children: React.ReactNode;
  badge?: React.ReactNode;
  /** For a run that never happened. */
  quiet?: boolean;
  className?: string;
}): React.ReactElement {
  return (
    <span
      aria-hidden
      className={cn(
        'relative flex h-7 w-7 shrink-0 items-center justify-center rounded-full border border-(--color-border-default)',
        quiet ? 'text-(--color-text-tertiary)' : 'text-(--color-text-secondary)',
        className,
      )}
    >
      {children}
      {badge}
    </span>
  );
}

/** A group header's icon, bare, centered over the avatars under it. */
export function RunGroupIcon({ children }: { children: React.ReactNode }): React.ReactElement {
  return (
    <span aria-hidden className="flex w-7 shrink-0 justify-center text-(--color-text-secondary)">
      {children}
    </span>
  );
}

/** A group header's fold chevron: right while folded, down while open, as the
 *  turn's own fold turns. */
export function RunFoldChevron({ open }: { open: boolean }): React.ReactElement {
  return (
    <ChevronRight
      aria-hidden
      className={cn(
        'h-3.5 w-3.5 shrink-0 text-(--color-text-tertiary) transition-transform duration-fold group-hover/run:text-(--color-text-secondary) motion-reduce:transition-none',
        open && 'rotate-90',
      )}
    />
  );
}

/**
 * Where a run stands, as a glyph. `live` is the sidebar's running-thread
 * spinner, so the amber spinner reads as an agent working on every surface;
 * `ring` waits on the reader, in amber, and `pending` is a run not yet under
 * way. A failure takes the alert, in the danger token rather than the market
 * loss red, and a stopped run the stop. An icon draws a verb, such as an
 * update sent to a task. A finished run has none: the spinner going away is
 * the done state. `done` is where a run's glyph has a place of its own, in a
 * column that an empty place would leave gapped: the circle the others draw,
 * filled, its check cut out. Decorative: the
 * words beside it say the same. `large` is a size up: a group header's lead,
 * beside the header's larger words, and a glyph in a column of its own.
 */
export type RunGlyphKind = 'live' | 'ring' | 'pending' | 'alert' | 'stop' | 'done' | LucideIcon;

export function RunGlyph({ kind, large = false }: { kind: RunGlyphKind; large?: boolean }): React.ReactElement {
  const icon = cn(large ? 'h-3.5 w-3.5' : 'h-3 w-3', 'shrink-0');
  switch (kind) {
    case 'live':
      // The Loader carries its own role="status"; hidden, it is not said twice.
      return large ? (
        <span aria-hidden className="flex w-3 shrink-0 justify-center">
          <Loader size={13} className="text-(--color-accent-primary)" />
        </span>
      ) : (
        <span aria-hidden className="inline-flex shrink-0">
          <Loader size={11} className="h-3 w-3 text-(--color-accent-primary)" />
        </span>
      );
    case 'ring':
    case 'pending':
      return (
        <span aria-hidden className={cn(icon, 'inline-flex items-center justify-center')}>
          <span
            className={cn(
              'block rounded-full border-[1.25px]',
              large ? 'h-2.5 w-2.5' : 'h-2 w-2',
              kind === 'ring' ? 'border-(--color-accent-primary)' : 'border-(--color-text-tertiary)',
            )}
          />
        </span>
      );
    case 'alert':
      return <AlertCircle aria-hidden strokeWidth={2} className={cn(icon, 'text-(--color-icon-danger)')} />;
    case 'stop':
      return <StopCircle aria-hidden strokeWidth={2} className={cn(icon, 'text-(--color-text-tertiary)')} />;
    case 'done':
      return <DoneGlyph className={cn(icon, 'text-(--color-text-tertiary)')} />;
    default: {
      const Icon = kind;
      return <Icon aria-hidden strokeWidth={2} className={cn(icon, 'text-(--color-text-tertiary)')} />;
    }
  }
}

/** The disc is the size of the outlined circles beside it, and the check is
 *  cut through it rather than drawn over it, so it shows whatever the row is
 *  on, hovered or not. */
function DoneGlyph({ className }: { className: string }) {
  const cut = useId();
  return (
    <svg aria-hidden viewBox="0 0 24 24" className={className}>
      <mask id={cut}>
        <rect width="24" height="24" fill="white" />
        <path d="m7.5 12.5 3 3 6-7" fill="none" stroke="black" strokeWidth={2.6} strokeLinecap="round" strokeLinejoin="round" />
      </mask>
      <circle cx="12" cy="12" r="11" fill="currentColor" mask={`url(#${cut})`} />
    </svg>
  );
}

/**
 * Where a run stands, leading the line under its title: the glyph, if one says
 * more than the words, and the words, which take the danger color when the run
 * did not do what it was asked. `children` follow the words: a working run's
 * clock.
 */
export function RunStatus({
  glyph,
  words,
  danger = false,
  id,
  'aria-hidden': hidden,
  'data-testid': testId,
  children,
}: {
  glyph?: RunGlyphKind | null;
  words: React.ReactNode;
  danger?: boolean;
  id?: string;
  /** For a status a screen reader hears from elsewhere. */
  'aria-hidden'?: boolean;
  'data-testid'?: string;
  children?: React.ReactNode;
}): React.ReactElement {
  return (
    <span
      id={id}
      aria-hidden={hidden}
      data-testid={testId}
      className={cn(RUN_STATUS, danger && 'text-(--color-icon-danger)')}
    >
      {glyph && <RunGlyph kind={glyph} />}
      <span>{words}</span>
      {children}
    </span>
  );
}

/**
 * Whether `words` are news: they changed after the first read gave them. What
 * a view says before its first read is the default, and the first read on a
 * page load is history, so neither is spoken. Latched, so a status that moves
 * back keeps being announced.
 */
function useNews(words: string, read: boolean): boolean {
  const [seen, setSeen] = useState<{ words: string; moved: boolean } | null>(null);
  const moved = !!seen && (seen.moved || seen.words !== words);
  if (read && seen === null) setSeen({ words, moved: false });
  else if (seen && !seen.moved && moved) setSeen({ words, moved: true });
  return moved;
}

/**
 * What a status shows, for a screen reader: `id` describes the control it
 * belongs to, and a change after the first read is said once through the live
 * region inside. The words sit in one span or the other, never both, so a
 * reader browsing hears them once. The clock is left out: it would be read
 * every second. With a `label`, only a new label is news, and the region
 * keeps the words it said: the length of a run that settled a moment ago
 * lands a read later, and would otherwise say the same state again.
 */
export function StatusSpeech({ id, words, label = words, read, live }: { id?: string; words: string; label?: string; read: boolean; live: boolean }) {
  const news = useNews(label, read);
  const [said, setSaid] = useState({ label, words });
  if (said.label !== label) setSaid({ label, words });
  const spoken = live && news;
  const shown = spoken ? said.words : words;
  return (
    <span id={id} className="sr-only">
      {read && !spoken ? shown : null}
      {live && <span role="status">{spoken ? shown : ''}</span>}
    </span>
  );
}

/**
 * A line that folds what is under it open and shut. The body stays in the
 * document, so the toggle's aria-controls names something while it is folded
 * away. Folding takes the body away, and with it a keyboard user's place, so
 * focus inside it moves up to the toggle.
 */
export function RunDisclosure({
  head,
  open,
  onToggle,
  className,
  labelledBy,
  describedBy,
  speech,
  children,
}: {
  /** What the toggle shows. */
  head: React.ReactNode;
  open: boolean;
  onToggle: () => void;
  className: string;
  labelledBy?: string;
  describedBy?: string;
  /** Beside the toggle: what its status says to a screen reader. */
  speech?: React.ReactNode;
  children: React.ReactNode;
}): React.ReactElement {
  const bodyId = useId();
  const toggleRef = useRef<HTMLButtonElement>(null);
  const bodyRef = useRef<HTMLDivElement>(null);
  const wasOpen = useRef(open);
  useEffect(() => {
    const body = bodyRef.current;
    if (wasOpen.current && !open && body?.contains(body.ownerDocument.activeElement)) {
      toggleRef.current?.focus();
    }
    wasOpen.current = open;
  }, [open]);
  return (
    <>
      <button
        ref={toggleRef}
        type="button"
        aria-expanded={open}
        aria-controls={bodyId}
        aria-labelledby={labelledBy}
        aria-describedby={describedBy}
        onClick={onToggle}
        className={className}
      >
        {head}
      </button>
      {speech}
      <div id={bodyId} ref={bodyRef}>
        <FoldPanel open={open}>{children}</FoldPanel>
      </div>
    </>
  );
}

/** One word of a group's tally. `keep` holds its full width on a narrow
 *  header, where the rest of the line gives way first: what went wrong. */
export interface RunGroupPart {
  text: string;
  danger?: boolean;
  keep?: boolean;
}

function Sep({ inline = false }: { inline?: boolean }): React.ReactElement {
  return (
    <span aria-hidden className={cn('text-(--color-text-tertiary)', inline && 'mx-1.5')}>·</span>
  );
}

const DANGER_TEXT = 'text-(--color-icon-danger)';

/**
 * Runs launched together, as one: a header line with the group's icon, how
 * many, and how they stand, over a row each. The tally's parts that may give
 * way share one span that truncates; kept parts follow, then the clock, which
 * ticks and so is never read out. With `announce`, a screen reader hears the
 * title as the header's name and the tally as its description, said again as
 * it changes once `read`.
 */
export function RunGroup({
  icon,
  title,
  lead,
  parts,
  clock,
  open,
  onToggle,
  announce,
  children,
}: {
  /** The group's glyph, centered over the avatars under it. */
  icon: React.ReactNode;
  title: string;
  /** The glyph of the state that still moves, if any does. */
  lead?: RunGlyphKind;
  parts: readonly RunGroupPart[];
  clock?: string;
  open: boolean;
  onToggle: () => void;
  announce?: { read: boolean };
  children: React.ReactNode;
}): React.ReactElement {
  const titleId = useId();
  const tallyId = useId();
  const quiet = parts.filter((p) => !p.keep);
  const kept = parts.filter((p) => p.keep);
  return (
    <RunDisclosure
      open={open}
      onToggle={onToggle}
      className={RUN_GROUP_HEAD}
      labelledBy={announce ? titleId : undefined}
      describedBy={announce ? tallyId : undefined}
      speech={announce && (
        <StatusSpeech id={tallyId} words={parts.map((p) => p.text).join(', ')} read={announce.read} live />
      )}
      head={
        <>
          <RunGroupIcon>{icon}</RunGroupIcon>
          <span id={titleId} className={RUN_HEAD_TITLE}>{title}</span>
          <span data-testid="run-group-tally" aria-hidden={announce ? true : undefined} className={RUN_HEAD_SUMMARY}>
            {lead && <RunGlyph kind={lead} large />}
            {quiet.length > 0 && (
              <span className="min-w-0 truncate">
                {quiet.map((p, i) => (
                  <React.Fragment key={i}>
                    {i > 0 && <Sep inline />}
                    {p.danger ? <span className={DANGER_TEXT}>{p.text}</span> : p.text}
                  </React.Fragment>
                ))}
              </span>
            )}
            {kept.map((p, i) => (
              <React.Fragment key={p.text}>
                {(i > 0 || quiet.length > 0) && <Sep />}
                <span className={cn('shrink-0', p.danger && DANGER_TEXT)}>{p.text}</span>
              </React.Fragment>
            ))}
            {clock && (
              <>
                <Sep />
                <span aria-hidden className={cn(RUN_CLOCK, 'shrink-0')}>{clock}</span>
              </>
            )}
          </span>
          <RunFoldChevron open={open} />
        </>
      }
    >
      {children}
    </RunDisclosure>
  );
}
