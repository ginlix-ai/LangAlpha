import React, { useEffect, useState } from 'react';
import { ArrowUp, Check, MessageSquare, MessageSquarePlus } from 'lucide-react';
import { useQueryClient } from '@tanstack/react-query';
import type { TFunction } from 'i18next';
import { useTranslation } from 'react-i18next';
import { Button } from '@/components/ui/button';
import { InPlaceSwap, SwapCell } from '@/components/ui/crossfade';
import { artifactPathKey, useArtifactByPath, useArtifactComments } from '@/hooks/useArtifact';
import { useLocale } from '@/hooks/useLocale';
import { ALL, refreshArtifactComments } from '@/lib/artifactCache';
import { unreadComments } from '@/lib/artifactCommentReads';
import { formatList } from '@/lib/format';
import { AnimatePresence, motion } from '@/lib/framer';
import { TWEEN } from '@/lib/motion';
import { cn } from '@/lib/utils';
import type { ArtifactComment } from '@/types/api';
import { useWorkspaceFolders } from '../../../../contexts/WorkspaceContext';
import type { CommentSendStore } from '../../../artifactComments/commentSendStore';
import { useArtifactCommentSender } from '../../../artifactComments/sendContext';
import { oneLine, openComments } from '../../../artifactComments/commentModel';
import { queuedBatches, refusalReason, sendBatches, type BatchOutcome } from '../../../artifactComments/commentSends';
import { SendingGlyph } from './SendingGlyph';
import { targetKey } from '../../../artifactComments/destinations';
import { useCommentDestinations } from '../../../artifactComments/useCommentDestinations';
import type { CommentUi } from './useCommentUi';
import './CommentTools.css';

interface CommentToolsProps {
  tabId: string;
  workspaceId: string;
  /** The HTML file, workspace-relative or as the tab holds it. */
  path: string;
  ui: CommentUi;
  /** `crumb` sits in the file panel's crumb row, `bar` in the standalone view's top bar. */
  variant?: 'crumb' | 'bar';
  /** Comment mode is not offered: a published version is read only. */
  modeDisabled?: boolean;
}

const BAR_BUTTON = 'h-[30px] gap-1.5 px-2 text-xs sm:px-2.5';
const NONE: [] = [];
/** How long Send all's outcome stays in the toolbar before the queue, if any, comes back. */
const OUTCOME_MS = 6000;

/** A new number rises in from below as the old one lifts out. */
const ROLL = { y: 4 };

function Tally({ n }: { n: number }): React.ReactElement {
  return <SwapCell swapKey={String(n)} shift={ROLL} className="tabular-nums">{n}</SwapCell>;
}

/**
 * A count arriving or leaving opens or closes its own width, so the row it
 * sits in eases over rather than jumping. Its room before it is inside that
 * width, where a flex gap would land at once.
 */
function Grow({ space, className, children }: { space: string; className?: string; children: React.ReactNode }): React.ReactElement {
  return (
    <motion.span
      className={cn('inline-flex overflow-hidden', className)}
      initial={{ width: 0, opacity: 0 }}
      animate={{ width: 'auto', opacity: 1, transition: TWEEN.fold }}
      exit={{ width: 0, opacity: 0, transition: TWEEN.exit }}
    >
      <span className={cn('inline-flex shrink-0 items-center whitespace-nowrap', space)}>{children}</span>
    </motion.span>
  );
}

/** `text` either side of its count, so the number alone rolls; null where the locale writes it otherwise. */
function aroundCount(text: string, count: number): [string, string] | null {
  const n = String(count);
  const at = text.indexOf(n);
  return at < 0 ? null : [text.slice(0, at), text.slice(at + n.length)];
}

/** Classes that fold a control's words away where room is short, and show its number alone there. */
interface Fold {
  words: string;
  number: string;
}

/** In the page's top bar on a phone, or the page's title beside it is left one letter wide. */
const BAR_FOLD: Fold = { words: 'hidden sm:inline', number: 'sm:hidden' };
/** In a narrow panel's crumb row (CommentTools.css), or the file's actions are pushed out of it. */
const CRUMB_FOLD: Fold = { words: 'page-comment-queue-words', number: 'page-comment-queue-number' };

/**
 * A count in words, as a badge: the number rolls alone where the locale writes
 * it inside the words. With `fold` it keeps only its number where room is short.
 */
function CountBadge({ text, count, fold, className, title }: {
  text: string;
  count: number;
  fold?: Fold;
  className: string;
  title: string;
}): React.ReactElement {
  const parts = aroundCount(text, count);
  return (
    <span className={className} title={title}>
      {parts ? (
        <>
          {parts[0] ? <span className={fold?.words}>{parts[0]}</span> : null}
          <Tally n={count} />
          {parts[1] ? <span className={fold?.words}>{parts[1]}</span> : null}
        </>
      ) : (
        <SwapCell swapKey={String(count)} shift={ROLL}>
          {fold ? (
            <>
              <span className={fold.number}>{count}</span>
              <span className={fold.words}>{text}</span>
            </>
          ) : text}
        </SwapCell>
      )}
    </span>
  );
}

/**
 * What the Open list holds and how many comments have a reply the owner has
 * not read. Mounted once the comments are in, so the counts a tab opens with
 * draw settled, and only a change after that moves.
 */
function Counts({ open, unread, bar }: { open: number; unread: number; bar: boolean }): React.ReactElement {
  const { t } = useTranslation();
  const space = bar ? 'pl-1.5' : 'pl-1';
  return (
    <AnimatePresence initial={false}>
      {open > 0 && (
        <Grow key="open" space={space}>
          <Tally n={open} />
        </Grow>
      )}
      {unread > 0 && (
        <Grow key="unread" space={space}>
          <CountBadge
            className="page-comment-unread-badge"
            text={t('pageViewer.comments.unreadBadge', { count: unread })}
            count={unread}
            fold={bar ? BAR_FOLD : undefined}
            title={t('pageViewer.comments.unreadTitle', { count: unread })}
          />
        </Grow>
      )}
    </AnimatePresence>
  );
}

/** How much of each thread's title the outcome keeps when it names several, so each shows. */
const THREAD_CHARS = 24;

/** What Send all did, as the toolbar says it for a moment. */
interface SendAllOutcome {
  /** "Sent 3 to <thread>", or each thread's share; null when nothing went. */
  sent: string | null;
  /** "1 not sent"; each card says why. */
  refused: string | null;
}

function summarize(t: TFunction, locale: string, outcomes: readonly BatchOutcome[], besideThreadId: string | null): SendAllOutcome {
  const parts: string[] = [];
  let refused = 0;
  // A destination's full batches went to one thread, so they read as one share.
  const went = new Map<string, { first: BatchOutcome; count: number }>();
  for (const outcome of outcomes) {
    const size = outcome.batch.comments.length;
    if (outcome.reason !== null) {
      refused += size;
      continue;
    }
    const key = targetKey(outcome.batch.to);
    const held = went.get(key);
    if (held) held.count += size;
    else went.set(key, { first: outcome, count: size });
  }
  for (const { first: { batch, threadId }, count } of went.values()) {
    if (threadId === besideThreadId) {
      parts.push(t('pageViewer.comments.sentAllPartHere', { count }));
    } else if ('newThreadIn' in batch.to) {
      parts.push(t('pageViewer.comments.sentAllPartNew', { count }));
    } else {
      // A comment whose thread was archived names that one, not this.
      const title = batch.comments.find((c) => c.thread_id === threadId)?.thread_title?.trim();
      const thread = title
        ? (went.size > 1 ? oneLine(title, THREAD_CHARS) : title)
        : t('pageViewer.comments.untitledThread');
      parts.push(t('pageViewer.comments.sentAllPart', { count, thread }));
    }
  }
  return {
    sent: parts.length > 0 ? t('pageViewer.comments.sentAll', { parts: formatList(parts, locale) }) : null,
    refused: refused > 0 ? t('pageViewer.comments.notSentCount', { count: refused }) : null,
  };
}

/**
 * The file's queued comments with the way to send them: the count in the
 * dashed edge queued wears on pins and cards, and Send all, which sends them
 * now, one send per thread they wait for. The button shows the sends going,
 * each card shows its own, and the outcome takes the control's place for a
 * moment; a refused comment stays queued with the reason on its card. Gone
 * once nothing waits and the outcome has been said.
 */
function SendAll({ comments, artifactId, workspaceId, docPath, sends, bar }: {
  comments: readonly ArtifactComment[];
  artifactId: string;
  workspaceId: string;
  docPath: string;
  sends: CommentSendStore;
  bar: boolean;
}): React.ReactElement {
  const { t } = useTranslation();
  const locale = useLocale();
  const queryClient = useQueryClient();
  const sender = useArtifactCommentSender();
  const { targetFor } = useCommentDestinations(workspaceId, docPath);
  const queued = comments.filter((c) => c.status === 'open' && c.delivery === 'queued');
  const [busy, setBusy] = useState(false);
  const [outcome, setOutcome] = useState<SendAllOutcome | null>(null);
  useEffect(() => {
    if (!outcome) return;
    const timer = setTimeout(() => setOutcome(null), OUTCOME_MS);
    return () => clearTimeout(timer);
  }, [outcome]);

  const sendAll = () => {
    if (busy) return;
    // A card already sending its comment keeps that send.
    const waiting = queued
      .filter((c) => sends.get(c.comment_id)?.phase !== 'sending')
      .map((c) => ({ ...c, workspace_id: workspaceId }));
    const batches = queuedBatches(waiting, targetFor);
    if (batches.length === 0) return;
    setBusy(true);
    setOutcome(null);
    void sendBatches(batches, { sender, sends, reasonOf: (err) => refusalReason(t, err, true) }).then((outcomes) => {
      setBusy(false);
      setOutcome(summarize(t, locale, outcomes, sender.besideThreadId));
      refreshArtifactComments(queryClient, [{ artifactId, workspaceId }], ALL);
    });
  };

  const said = outcome ? [outcome.sent, outcome.refused].filter(Boolean).join(' · ') : '';
  const fold = bar ? BAR_FOLD : CRUMB_FOLD;
  return (
    <>
      {/* Mounted before anything is said in it, so the outcome is read out. */}
      <span className="sr-only" role="status">{said}</span>
      <AnimatePresence initial={false}>
        {(queued.length > 0 || outcome) && (
          <Grow key="queue" space="" className="shrink-0">
            <SwapCell swapKey={outcome ? 'outcome' : 'queue'} className="shrink-0">
              {outcome ? (
                <span className="page-comment-queue-outcome" data-bar={bar || undefined} title={said} aria-hidden>
                  {outcome.sent && <Check className="h-3.5 w-3.5 shrink-0" style={{ color: 'var(--color-accent-primary)' }} />}
                  {/* Where room is short it keeps one word, or the row's other actions are pushed out. */}
                  {outcome.sent && <span className={cn('min-w-0 truncate', fold.words)}>{outcome.sent}</span>}
                  {outcome.sent && !outcome.refused && <span className={fold.number}>{t('pageViewer.comments.sentShort')}</span>}
                  {outcome.sent && outcome.refused && <span className={cn('shrink-0', fold.words)}>·</span>}
                  {outcome.refused && (
                    <span className="shrink-0" style={{ color: 'var(--color-icon-danger)' }}>{outcome.refused}</span>
                  )}
                </span>
              ) : (
                <span className="page-comment-queue" data-bar={bar || undefined}>
                  <CountBadge
                    className="page-comment-queue-count"
                    text={t('pageViewer.comments.queuedBadge', { count: queued.length })}
                    count={queued.length}
                    fold={fold}
                    title={t('pageViewer.comments.queuedTitle')}
                  />
                  <Button
                    type="button"
                    size="sm"
                    className="page-comment-queue-send gap-1 text-xs"
                    // Held while the sends go, without the faded look of a control that cannot act.
                    aria-disabled={busy || undefined}
                    // Its name where its words fold away.
                    aria-label={busy ? t('pageViewer.comments.sending') : t('pageViewer.comments.sendAll')}
                    title={t('pageViewer.comments.sendAllTitle', { count: queued.length })}
                    onClick={sendAll}
                  >
                    <InPlaceSwap
                      swapped={busy}
                      className="justify-items-center"
                      from={(
                        <>
                          <ArrowUp className="h-3 w-3" aria-hidden />
                          <span className={fold.words}>{t('pageViewer.comments.sendAll')}</span>
                        </>
                      )}
                      to={(
                        <>
                          <SendingGlyph on={busy} />
                          <span className={fold.words}>{t('pageViewer.comments.sending')}</span>
                        </>
                      )}
                    />
                  </Button>
                </span>
              )}
            </SwapCell>
          </Grow>
        )}
      </AnimatePresence>
    </>
  );
}

/**
 * An HTML file's comment controls: the mode that turns clicks into picks
 * (dark while on, and it stays on from one comment to the next), the list,
 * with what is still open and what has a reply not yet read, and what waits
 * for a send, with Send all. They count the file the frame is on, as the
 * layer over it shows: the one the tab opened, or another a link led to.
 */
export function CommentTools({
  tabId, workspaceId, path, ui, variant = 'crumb', modeDisabled = false,
}: CommentToolsProps): React.ReactElement {
  const { t } = useTranslation();
  const rootDir = useWorkspaceFolders()?.rootDir;
  const docPath = ui.docOf(tabId, artifactPathKey(path, rootDir));
  const { data: artifact } = useArtifactByPath(workspaceId, docPath);
  const commentsQuery = useArtifactComments(artifact?.artifact_id);
  const comments = artifact === null ? NONE : commentsQuery.data;
  const open = comments ? openComments(comments).length : 0;
  const unread = comments ? unreadComments(comments) : 0;
  const mode = ui.modeTab === tabId && !modeDisabled;
  const listOpen = ui.listTab === tabId;
  const openLabel = t('pageViewer.comments.listLabel', { count: open });
  const listLabel = unread > 0
    ? t('pageViewer.comments.listLabelUnread', { label: openLabel, unread: t('pageViewer.comments.unreadTitle', { count: unread }) })
    : openLabel;
  const counts = comments && <Counts open={open} unread={unread} bar={variant === 'bar'} />;
  const sendAll = comments && artifact && (
    <SendAll
      comments={comments}
      artifactId={artifact.artifact_id}
      workspaceId={workspaceId}
      docPath={docPath}
      sends={ui.sends}
      bar={variant === 'bar'}
    />
  );

  if (variant === 'bar') {
    // The bar's own buttons, as its other actions draw them; on a phone the
    // mode's label folds away like theirs.
    return (
      <>
        <Button
          type="button"
          variant={mode ? 'default' : 'outline'}
          size="sm"
          onClick={() => ui.setMode(tabId, !mode)}
          disabled={modeDisabled}
          // Its own edge while on, so the button keeps its size as it toggles.
          className={cn(BAR_BUTTON, mode && 'border border-(--color-btn-primary-bg)')}
          aria-pressed={mode}
          aria-keyshortcuts="C"
          aria-label={t('pageViewer.comments.comment')}
          title={t('pageViewer.comments.modeTitle')}
        >
          <MessageSquarePlus className="h-3.5 w-3.5" />
          <span className="hidden sm:inline">{t('pageViewer.comments.comment')}</span>
        </Button>
        <Button
          type="button"
          variant="outline"
          size="sm"
          onClick={() => ui.setList(tabId, !listOpen)}
          // The counts bring their own room, inside the width they open.
          className={cn(BAR_BUTTON, 'gap-0')}
          aria-pressed={listOpen}
          aria-label={listLabel}
          title={listLabel}
        >
          <MessageSquare className="h-3.5 w-3.5" />
          {counts}
        </Button>
        {sendAll}
      </>
    );
  }

  return (
    <>
      <button
        type="button"
        onClick={() => ui.setMode(tabId, !mode)}
        disabled={modeDisabled}
        className={cn('page-comment-mode-btn press shrink-0', mode && 'is-on')}
        aria-pressed={mode}
        aria-keyshortcuts="C"
        title={t('pageViewer.comments.modeTitle')}
      >
        <MessageSquarePlus className="h-3.5 w-3.5" />
        <span>{t('pageViewer.comments.comment')}</span>
      </button>
      <button
        type="button"
        onClick={() => ui.setList(tabId, !listOpen)}
        className="file-panel-icon-btn page-comment-list-btn shrink-0"
        aria-pressed={listOpen}
        aria-label={listLabel}
        title={listLabel}
      >
        <MessageSquare className="h-3.5 w-3.5" />
        {counts}
      </button>
      {sendAll}
    </>
  );
}
