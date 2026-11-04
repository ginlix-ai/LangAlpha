/**
 * Queued file comments at the top of the composer, ruled off from the text
 * box: what the next send carries, with the box's text as their note. A rail
 * names the queue and offers to keep it all as notes (or, while a turn runs,
 * to add them to it); one row per comment opens the file on it, and its x
 * keeps that one as a note. Past four rows it folds to three, and to one in
 * a phone's dock.
 *
 * Everything that comes and goes here folds, since the composer's height is
 * the transcript's bottom edge: the deck arriving or leaving, a row joining or
 * kept, the rows past the fold, the error line. A cut here jumps the whole
 * transcript.
 */
import React, { useContext, useEffect, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { AlertCircle, ChevronDown, X } from 'lucide-react';
import { AnimatePresence, motion, PresenceContext, usePresence } from '@/lib/framer';
import { DURATION, SPRING_FOLD } from '@/lib/motion';
import { Disclosure } from '@/components/ui/Disclosure';
import { SwapCell } from '@/components/ui/crossfade';
import { cn } from '@/lib/utils';
import type { QueuedArtifactComment } from '@/types/api';
import { commentsByFile, latestOwnerWords } from './artifactCommentSend';
import type { ArtifactCommentRef } from './artifactCommentReply';
import { areaTitle } from './commentModel';
import { PinBadge } from './PinBadge';
import type { QueuedCommentDeck as Deck } from './useQueuedCommentDeck';

// Past this many rows the deck folds, so the composer never grows into the
// transcript. A phone's dock sits under the file itself, so it keeps one.
const FOLD_PAST = 4;
const FOLDED_ROWS = 3;
const COMPACT_ROWS = 1;

// The rail's glyph and each row's number share this column, so the queue's
// words and every comment start on one edge.
const LEAD = 'grid-cols-[24px_minmax(0,1fr)_auto] gap-2';

// A tall deck leaving at the exit beat reads as a cut.
const deckBeat = (height: number) => (height > 96 ? DURATION.fold : DURATION.exit);

/**
 * A keep from the keyboard takes its button away, which would drop focus on
 * the composer: it moves to the next row's keep, or the one before, or, with the
 * queue empty, to the text box. A pointer's keep leaves focus alone, so a
 * phone's keyboard does not come up for it.
 */
function focusAfterKeep(from: HTMLElement, deck: HTMLElement | null, focusComposer?: () => void): void {
  if (!from.matches(':focus-visible')) return;
  const keeps = deck ? [...deck.querySelectorAll<HTMLElement>('button[data-keep-one]')] : [];
  const at = keeps.indexOf(from);
  const next = at >= 0 ? keeps[at + 1] ?? keeps[at - 1] : undefined;
  if (next) next.focus();
  else focusComposer?.();
}

type Line =
  | { kind: 'file'; key: string; title: string; shown: boolean }
  | { kind: 'comment'; key: string; comment: QueuedArtifactComment; shown: boolean };

/**
 * A line of the deck: unfolds when it joins (not when the deck first draws),
 * folds behind the "more" toggle, and folds out before it is removed.
 */
function DeckFold({ shown, children }: { shown: boolean; children: React.ReactNode }): React.ReactElement {
  const [present, safeToRemove] = usePresence();
  const quietStart = useContext(PresenceContext)?.initial === false;
  // A line already folded away has nothing to close when it leaves.
  useEffect(() => {
    if (!present && !shown) safeToRemove?.();
  }, [present, shown, safeToRemove]);
  return (
    <Disclosure open={present && shown} appear={!quietStart} onClosed={present ? undefined : safeToRemove}>
      {children}
    </Disclosure>
  );
}

function Row({ comment, onOpen, onKeep }: {
  comment: QueuedArtifactComment;
  onOpen?: (comment: ArtifactCommentRef) => void;
  /** `from` is the row's keep button, which goes with the row. */
  onKeep: (from: HTMLElement) => void;
}): React.ReactElement {
  const { t } = useTranslation();
  // An area's parts would starve in this slot; its size says what it is.
  const area = comment.anchor.kind === 'area';
  const label = area ? areaTitle(t, comment.anchor) : comment.anchor.label?.trim();
  const body = (
    <>
      <PinBadge number={comment.number} state="queued" className="justify-self-center" />
      <span className="truncate text-[13px]" style={{ color: 'var(--color-text-primary)' }}>
        {latestOwnerWords(comment)}
      </span>
      {/* Where in the file it sits, until the words would starve for it. */}
      <span
        className="max-w-[120px] truncate text-xs @max-[420px]:hidden"
        style={{ color: 'var(--color-text-tertiary)' }}
        title={area ? comment.anchor.label : undefined}
      >
        {label}
      </span>
    </>
  );
  const main = cn('grid h-7 min-w-0 items-center rounded-md pl-0.5 pr-1 text-left', LEAD);
  return (
    <div
      className={cn(
        'group/row grid grid-cols-[minmax(0,1fr)_22px] items-center rounded-md transition-colors duration-quick',
        'hover:bg-(--color-bg-hover) focus-within:bg-(--color-bg-hover)',
      )}
    >
      {onOpen ? (
        <button
          type="button"
          className={cn(main, 'cursor-pointer outline-hidden focus-visible:ring-2 focus-visible:ring-ring')}
          title={t('chat.artifactComments.deck.showInFile')}
          onClick={() => onOpen({
            artifactId: comment.artifact_id,
            workspaceId: comment.workspace_id,
            path: comment.path,
            commentId: comment.comment_id,
          })}
        >
          {body}
        </button>
      ) : (
        <div className={main}>{body}</div>
      )}
      <button
        type="button"
        className={cn(
          'grid h-5 w-5 cursor-pointer place-items-center rounded-full opacity-0 outline-hidden transition-opacity duration-quick',
          'group-hover/row:opacity-100 group-focus-within/row:opacity-100 [@media(hover:none)]:opacity-100',
          'text-(--color-text-tertiary) hover:bg-(--color-border-default) hover:text-(--color-text-primary) focus-visible:ring-2 focus-visible:ring-ring',
        )}
        title={t('chat.artifactComments.deck.keepOne')}
        aria-label={t('chat.artifactComments.deck.keepOneLabel', { number: comment.number })}
        data-keep-one=""
        onClick={(event) => onKeep(event.currentTarget)}
      >
        <X size={11} strokeWidth={2.2} />
      </button>
    </div>
  );
}

function DeckBody({ deck, running, compact, onOpen, onAddToTurn, focusComposer }: DeckProps): React.ReactElement {
  const { t } = useTranslation();
  const [expanded, setExpanded] = useState(false);
  const rootRef = useRef<HTMLDivElement>(null);
  const { ready, error, keep } = deck;
  const rows = ready;

  const groups = commentsByFile(rows);
  const foldedRows = compact ? COMPACT_ROWS : FOLDED_ROWS;
  const folds = rows.length > (compact ? COMPACT_ROWS : FOLD_PAST);
  const lines: Line[] = [];
  let index = 0;
  for (const group of groups) {
    const groupShown = !folds || expanded || index < foldedRows;
    if (groups.length > 1) {
      lines.push({ kind: 'file', key: `file:${group.artifactId}`, title: group.title, shown: groupShown });
    }
    for (const comment of group.comments) {
      lines.push({ kind: 'comment', key: comment.comment_id, comment, shown: !folds || expanded || index < foldedRows });
      index += 1;
    }
  }
  const rest = Math.max(0, rows.length - foldedRows);

  const muted = { color: 'var(--color-text-tertiary)' };
  const quietButton = 'shrink-0 cursor-pointer whitespace-nowrap rounded-md px-1.5 py-0.5 text-(--color-text-tertiary) outline-hidden transition-colors duration-quick hover:bg-(--color-bg-hover) hover:text-(--color-text-primary) focus-visible:ring-2 focus-visible:ring-ring';
  const where = groups.length > 1
    ? t('chat.artifactComments.deck.onFiles', { files: groups.length })
    : t('chat.artifactComments.deck.onFile', { title: groups[0]?.title ?? '' });

  return (
    // The bottom margin is the column's gap, folding with the deck; the outer
    // box cancels the gap itself (chat-input.css).
    <div
      ref={rootRef}
      className="@container mb-2 grid min-w-0 gap-0.5 border-b border-dashed pb-2"
      style={{ borderColor: 'var(--color-border-muted)' }}
    >
      <div className={cn('grid min-h-6 min-w-0 items-center px-0.5 pb-0.5 text-xs', LEAD)}>
        <span
          aria-hidden="true"
          className="h-3 w-3 justify-self-center rounded-full border-[1.5px] border-dashed"
          style={{ borderColor: 'var(--color-text-secondary)' }}
        />
        <span className="flex min-w-0 items-baseline gap-1.5">
          <SwapCell swapKey={`count:${rows.length}`} className="shrink-0 whitespace-nowrap font-medium" layerClassName="whitespace-nowrap">
            <span style={{ color: 'var(--color-text-primary)' }}>
              {t('chat.artifactComments.deck.queuedCount', { count: rows.length })}
            </span>
          </SwapCell>
          <span className="truncate" style={muted}>{where}</span>
        </span>
        <SwapCell swapKey={running ? 'running' : 'idle'} className="justify-self-end" layerClassName="justify-self-end">
          {running ? (
            <button
              type="button"
              className={cn(quietButton, 'inline-flex items-center gap-1.5')}
              title={t('chat.artifactComments.deck.addToTurnTitle')}
              onClick={onAddToTurn}
            >
              <kbd
                className="rounded border px-[3px] font-sans text-[10px] leading-4 pointer-coarse:hidden"
                style={{ borderColor: 'var(--color-border-default)' }}
              >
                ↵
              </kbd>
              {t('chat.artifactComments.deck.addToTurn')}
            </button>
          ) : (
            <button
              type="button"
              className={quietButton}
              title={t('chat.artifactComments.deck.keepAsNotesTitle')}
              onClick={(event) => {
                focusAfterKeep(event.currentTarget, rootRef.current, focusComposer);
                keep(rows);
              }}
            >
              {t('chat.artifactComments.deck.keepAsNotes')}
            </button>
          )}
        </SwapCell>
      </div>

      <Disclosure open={!!error}>
        <div role="status" className="flex items-start gap-1.5 px-0.5 pb-1 text-xs" style={{ color: 'var(--color-text-secondary)' }}>
          <AlertCircle size={13} className="mt-0.5 shrink-0" style={{ color: 'var(--color-loss)' }} />
          <span className="min-w-0">{error}</span>
        </div>
      </Disclosure>

      <div className={cn('grid min-w-0', folds && 'max-h-[200px] overflow-y-auto')}>
        <AnimatePresence initial={false}>
          {lines.map((line) => (
            <DeckFold key={line.key} shown={line.shown}>
              {line.kind === 'file' ? (
                <div className="truncate pb-0.5 pl-[34px] pr-1 pt-1.5 text-[11px]" style={muted}>
                  {line.title}
                </div>
              ) : (
                <Row
                  comment={line.comment}
                  onOpen={onOpen}
                  onKeep={(from) => {
                    focusAfterKeep(from, rootRef.current, focusComposer);
                    keep([line.comment]);
                  }}
                />
              )}
            </DeckFold>
          ))}
        </AnimatePresence>
      </div>

      <Disclosure open={folds}>
        <button
          type="button"
          aria-expanded={expanded}
          className="ml-7 inline-flex cursor-pointer items-center gap-1 rounded-md px-1.5 py-0.5 text-xs text-(--color-text-secondary) outline-hidden transition-colors duration-quick hover:bg-(--color-bg-hover) hover:text-(--color-text-primary) focus-visible:ring-2 focus-visible:ring-ring"
          onClick={() => setExpanded((open) => !open)}
        >
          <SwapCell swapKey={expanded ? 'less' : `more:${rest}`}>
            {expanded ? t('chat.artifactComments.deck.showLess') : t('chat.artifactComments.deck.more', { rest })}
          </SwapCell>
          <motion.span
            aria-hidden="true"
            className="inline-flex"
            initial={false}
            animate={{ rotate: expanded ? 180 : 0 }}
            transition={SPRING_FOLD}
          >
            <ChevronDown size={12} />
          </motion.span>
        </button>
      </Disclosure>
    </div>
  );
}

interface DeckProps {
  deck: Deck;
  running: boolean;
  /** Folds to one row, for a phone's dock under the file. */
  compact?: boolean;
  onOpen?: (comment: ArtifactCommentRef) => void;
  /** Sends into the running turn, as Enter does. */
  onAddToTurn?: () => void;
  /** Puts focus in the composer's text box, where a keep that empties the queue leaves it. */
  focusComposer?: () => void;
}

export function QueuedCommentDeck(props: DeckProps): React.ReactElement | null {
  const { deck } = props;
  // Drawn once the thread's queue has been read, and anew for each thread, so
  // a queue already there is just there: only a change to it moves.
  if (!deck.loaded) return null;
  return (
    <Disclosure
      key={deck.threadId}
      open={deck.ready.length > 0}
      closeBeat={deckBeat}
    >
      <DeckBody {...props} />
    </Disclosure>
  );
}
