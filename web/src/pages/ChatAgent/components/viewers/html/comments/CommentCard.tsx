import React, { useId, useLayoutEffect, useRef, useState } from 'react';
import {
  ArrowUp, ArrowUpRight, Check, ListPlus, MoreHorizontal, RotateCcw, StickyNote, Trash2, type LucideIcon,
} from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { Link } from 'react-router';
import { LangAlphaMark } from '@/components/brand/LangAlphaMark';
import { Button } from '@/components/ui/button';
import { InPlaceSwap, SwapCell } from '@/components/ui/crossfade';
import { Disclosure, PresenceFold } from '@/components/ui/Disclosure';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import { Loader } from '@/components/ui/loader';
import { TextShimmer } from '@/components/ui/text-shimmer';
import { useLocale } from '@/hooks/useLocale';
import { useNow } from '@/hooks/useNow';
import { relativeTime } from '@/lib/format';
import { AnimatePresence } from '@/lib/framer';
import { DURATION } from '@/lib/motion';
import { flattenMarkdown } from '@/lib/plainLine';
import { cn } from '@/lib/utils';
import type { ArtifactComment, ArtifactCommentMessage } from '@/types/api';
import Markdown from '../../../Markdown';
import { threadLabel } from '../../../artifactComments/destinations';
import { CommentComposer } from './CommentComposer';
import { useCommentLayer, useCommentLook } from './commentLayerContext';
import {
  cardActions, firstWords, laterMessages, newestUnread, oneLine, spotPhrase, type CardAction, type PinState,
} from '../../../artifactComments/commentModel';
import { PinBadge } from '../../../artifactComments/PinBadge';
import { SendingGlyph } from './SendingGlyph';
import { useKeptWhileFading } from './useKeptWhileFading';
import './CommentCard.css';

/** How much of the newest unread reply a shut card shows; the line truncates it to its width. */
const NEWS_LIMIT = 160;
/** How near the foot of the thread still counts as reading at its foot, in px. */
const FOOT_SLACK = 4;
/** No resize for this long: the fold has landed. */
const SETTLED_MS = DURATION.quick * 1000;

const REPLY = { kind: 'reply' } as const;

const ACTION_ICON: Record<CardAction, LucideIcon> = {
  keepNote: StickyNote,
  sendNow: ArrowUp,
  addToQueue: ListPlus,
  reopen: RotateCcw,
  delete: Trash2,
};

const ACTION_LABEL: Record<CardAction, string> = {
  keepNote: 'pageViewer.comments.keepNote',
  sendNow: 'pageViewer.comments.sendNow',
  addToQueue: 'pageViewer.comments.addToQueue',
  reopen: 'pageViewer.comments.reopen',
  delete: 'common.delete',
};

/**
 * What a screen reader hears for the state the badge shows by its ring. A
 * sent comment says so through the agent's mark beside it.
 */
const STATE_LABEL: Partial<Record<PinState, string>> = {
  queued: 'pageViewer.comments.tagQueued',
  note: 'pageViewer.comments.tagNote',
  resolved: 'pageViewer.comments.tagResolved',
};

/**
 * Holds `scroller` at its foot while `content` grows, frame by frame as a fold
 * opens, and lets go once it stops growing.
 */
function holdFoot(scroller: HTMLElement, content: HTMLElement): () => void {
  let timer: ReturnType<typeof setTimeout> | undefined;
  function stop() {
    observer.disconnect();
    clearTimeout(timer);
  }
  const observer = new ResizeObserver(() => {
    scroller.scrollTop = scroller.scrollHeight;
    clearTimeout(timer);
    timer = setTimeout(stop, SETTLED_MS);
  });
  observer.observe(content);
  return stop;
}

/** The mark a comment wears once the agent has had it, and the agent's avatar on its replies. */
function AgentMark({ className }: { className?: string }): React.ReactElement {
  // Stroked heavier than the mark's own weight, which is drawn for a larger size.
  return <LangAlphaMark className={cn('shrink-0 [stroke-width:4.5]', className)} />;
}

/**
 * Where the card sits: in the box over its pin, or in the list, where a press
 * on its head opens and shuts it.
 */
export type CardPlace = { kind: 'over' } | { kind: 'list'; onToggle: () => void };

interface CommentCardProps {
  comment: ArtifactComment;
  /** Its spot was looked for on the file as it is now and is not there. */
  outdated: boolean;
  /** The comment is gone; whatever holds the card closes it. */
  onDeleted: () => void;
  /** Its body shows: always over a pin, and in the list once opened. */
  open: boolean;
  place: CardPlace;
}

/**
 * One comment: its pin's number in its state, who wrote it and when, the
 * agent's mark once it has had it, ✓ and a menu of the moves that fit, then
 * the owner's words. Open, it adds where the comment is, the answers under it
 * and a reply field. The owner's words are drawn as text; the agent's answers
 * go through the chat's sanitising markdown.
 */
export function CommentCard(props: CommentCardProps): React.ReactElement {
  // One card per comment: a holder that hands it another comment gets a
  // fresh one, not this one's reply, delete confirmation and thread.
  return <CardBody key={props.comment.comment_id} {...props} />;
}

function CardBody({ comment, outdated, onDeleted, open, place }: CommentCardProps): React.ReactElement {
  const { t } = useTranslation();
  const locale = useLocale();
  const now = useNow();
  const bodyId = useId();
  const [reply, setReply] = useState('');
  const [busy, setBusy] = useState(false);
  const [failed, setFailed] = useState(false);
  const [confirming, setConfirming] = useState(false);

  const { actions, besideThreadId, destinations } = useCommentLayer();
  const { state, working, reached, canReply, pending, refusal } = useCommentLook(comment);
  // The ⋯ menu, Send now and the reply box go where this comment's words go:
  // the thread it waits for or went to, else the file's default.
  const sendRoute = destinations.targetFor(comment, 'send');
  const queueRoute = destinations.targetFor(comment, 'queue');
  const sendNow = () => {
    if (sendRoute?.kind === 'send') actions.send(comment, sendRoute.to);
  };
  const resolved = comment.status === 'resolved';
  const queued = state === 'queued';
  // Kept while its fold closes, once the comment has gone or been sent again.
  const shownRefusal = useKeptWhileFading(refusal);
  const later = laterMessages(comment);
  const spot = spotPhrase(t, comment.anchor);
  const unread = comment.unread > 0;
  // Shut, an unread card says what is new in a line; open, the reply itself shows.
  const news = unread ? newestUnread(comment) : null;
  // A comment that went to a thread other than the one beside says which,
  // with the way there: the answer comes in that thread.
  const away = state === 'sent' && comment.thread_id && comment.thread_id !== besideThreadId
    ? { id: comment.thread_id, label: threadLabel(t, { thread_id: comment.thread_id, title: comment.thread_title }, besideThreadId) }
    : null;
  const status = queued ? 'queued' : away ? 'sent' : null;
  // What the comment was on, kept for when the file no longer shows it.
  const stored = outdated ? (comment.anchor.quote ?? null) : null;

  // A reader at the foot of the thread stays there as an answer folds in;
  // one who scrolled up to reread is left where they are.
  const threadRef = useRef<HTMLDivElement>(null);
  const contentRef = useRef<HTMLDivElement>(null);
  const arrived = `${later.length}:${working}`;
  const seen = useRef(arrived);
  useLayoutEffect(() => {
    if (seen.current === arrived) return;
    seen.current = arrived;
    const thread = threadRef.current;
    const content = contentRef.current;
    // Read before the fold has grown: it was just shut on its content.
    if (!thread || !content || thread.scrollHeight - thread.scrollTop - thread.clientHeight > FOOT_SLACK) return;
    return holdFoot(thread, content);
  }, [arrived]);

  const run = async (act: () => Promise<void>) => {
    if (busy) return;
    setBusy(true);
    setFailed(false);
    try {
      await act();
    } catch {
      setFailed(true);
    } finally {
      setBusy(false);
    }
  };

  const act = (action: CardAction) => {
    if (action === 'keepNote') void run(() => actions.setDelivery(comment, { kind: 'note' }));
    else if (action === 'addToQueue') {
      if (queueRoute?.kind === 'queue') void run(() => actions.setDelivery(comment, queueRoute));
    } else if (action === 'reopen') void run(() => actions.setStatus(comment, 'open'));
    else if (action === 'sendNow') sendNow();
    else setConfirming(true);
  };

  const who = (
    <>
      <PinBadge number={comment.number} state={state} working={working} unread={unread} />
      <span className="sr-only">{t('pageViewer.comments.pinLabel', { number: comment.number })}</span>
      {unread && <span className="sr-only">{t('pageViewer.comments.unreadLabel')}</span>}
      <span className="shrink-0 text-[13px] font-medium" style={{ color: 'var(--color-text-primary)' }}>
        {t('pageViewer.comments.you')}
      </span>
      <time className="shrink-0 text-xs whitespace-nowrap" style={{ color: 'var(--color-text-tertiary)' }} dateTime={comment.created_at}>
        {relativeTime(comment.created_at, locale, now)}
      </time>
      {reached && (
        <span role="img" aria-label={t('pageViewer.comments.sentToAgent')} title={t('pageViewer.comments.sentToAgent')} className="inline-flex" style={{ color: 'var(--color-accent-primary)' }}>
          <AgentMark className="h-3.5 w-3.5" />
        </span>
      )}
      {STATE_LABEL[state] && <span className="sr-only">{t(STATE_LABEL[state])}</span>}
      {/* Said folded or open: a list read top to bottom should not hide which
          comments the file has moved out from under. */}
      {outdated && <span className="page-comment-outdated shrink-0">{t('pageViewer.comments.outdated')}</span>}
    </>
  );

  return (
    <div className="page-comment-card" data-open={open || undefined} data-resolved={resolved || undefined} data-pin={place.kind === 'over' ? '' : undefined}>
      <div className="flex min-h-[26px] min-w-0 items-center gap-[7px]">
        {place.kind === 'list' ? (
          <button type="button" className="page-comment-card-toggle" aria-expanded={open} aria-controls={bodyId} onClick={place.onToggle}>
            {who}
          </button>
        ) : (
          <span className="flex min-w-0 flex-1 items-center gap-[7px]">{who}</span>
        )}
        <span className="page-comment-card-over flex shrink-0 items-center">
          <button
            type="button"
            className="page-comment-icon-btn"
            disabled={busy}
            aria-label={resolved ? t('pageViewer.comments.reopen') : t('pageViewer.comments.resolve')}
            title={resolved ? t('pageViewer.comments.reopen') : t('pageViewer.comments.resolve')}
            onClick={() => void run(() => actions.setStatus(comment, resolved ? 'open' : 'resolved'))}
          >
            <InPlaceSwap scale swapped={resolved} from={<Check className="h-[15px] w-[15px]" />} to={<RotateCcw className="h-3.5 w-3.5" />} />
          </button>
          <DropdownMenu modal={false}>
            <DropdownMenuTrigger asChild>
              <button
                type="button"
                className="page-comment-icon-btn"
                aria-label={t('pageViewer.comments.more')}
                title={t('pageViewer.comments.more')}
              >
                <MoreHorizontal className="h-[15px] w-[15px]" />
              </button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end" className="min-w-44">
              {cardActions(state, { canSend: sendRoute !== null, canQueue: queueRoute !== null }).map((action) => {
                const Icon = ACTION_ICON[action];
                return (
                  <DropdownMenuItem
                    key={action}
                    variant={action === 'delete' ? 'destructive' : 'default'}
                    disabled={busy && action !== 'delete'}
                    onSelect={() => act(action)}
                  >
                    <Icon className="h-3.5 w-3.5 shrink-0" aria-hidden />
                    {t(ACTION_LABEL[action])}
                  </DropdownMenuItem>
                );
              })}
            </DropdownMenuContent>
          </DropdownMenu>
        </span>
      </div>

      <div ref={threadRef} className="page-comment-thread min-h-0 overflow-y-auto">
        <div ref={contentRef}>
          <p
            className="pt-1 text-[13px] leading-[1.45] break-words whitespace-pre-wrap"
            style={{ color: resolved ? 'var(--color-text-secondary)' : 'var(--color-text-primary)' }}
          >
            {firstWords(comment)}
          </p>
          {/* Where the comment stands: queued, with the way out, or sent to
              another thread. One replaces the other in place as it goes. */}
          <Disclosure open={status !== null}>
            <SwapCell
              as="div"
              sizeToIncoming
              swapKey={status}
              className="page-comment-card-over"
              layerClassName="flex min-w-0 items-center gap-1.5 pt-1.5 text-xs"
            >
              {status === 'queued' ? (
                <QueuedRow
                  comment={comment}
                  besideThreadId={besideThreadId}
                  pending={pending}
                  onSend={sendRoute ? sendNow : null}
                />
              ) : away ? (
                <ThreadLink id={away.id} label={away.label} text={t('pageViewer.comments.sentTo', { thread: away.label })} />
              ) : null}
            </SwapCell>
          </Disclosure>
          <Disclosure open={refusal !== null}>
            <p role="alert" className="page-comment-card-over pt-1 text-xs leading-[1.4]" style={{ color: 'var(--color-icon-danger)' }}>
              {shownRefusal && t('pageViewer.comments.notSent', { reason: shownRefusal })}
            </p>
          </Disclosure>
          <Disclosure open={!open && news !== null}>
            {news && (
              <p className="flex min-w-0 items-center gap-1.5 pt-[5px] text-xs" style={{ color: 'var(--color-text-secondary)' }}>
                <span
                  aria-hidden
                  className="grid h-4 w-4 shrink-0 place-items-center rounded-full border"
                  style={{ borderColor: 'var(--color-border-default)', backgroundColor: 'var(--color-bg-page)', color: 'var(--color-text-primary)' }}
                >
                  <AgentMark className="h-2.5 w-2.5" />
                </span>
                <span className="shrink-0 font-medium whitespace-nowrap" style={{ color: 'var(--color-text-primary)' }}>
                  {resolved ? t('pageViewer.comments.newReplyResolved') : t('pageViewer.comments.newReply')}
                </span>
                <span className="min-w-0 truncate">{oneLine(flattenMarkdown(news.body), NEWS_LIMIT)}</span>
              </p>
            )}
          </Disclosure>
          <Disclosure id={bodyId} open={open}>
            <p className="flex min-w-0 items-center gap-1.5 pt-2 text-[11.5px]" style={{ color: 'var(--color-text-tertiary)' }} title={spot}>
              <span className="truncate">{spot}</span>
            </p>
            {stored && <p className="page-comment-quote mt-1 line-clamp-3 text-xs">{stored}</p>}
            {/* What arrives while the card is open folds in; each fold
                carries its own room above it, so it opens with the row. */}
            <div role="list">
              <AnimatePresence initial={false}>
                {later.map((m) => (
                  <PresenceFold key={m.message_id} appear className="pt-2.5">
                    <Reply message={m} locale={locale} now={now} />
                  </PresenceFold>
                ))}
              </AnimatePresence>
            </div>
            {/* Folds shut as the answer it waited for folds open. */}
            <AnimatePresence initial={false}>
              {working && (
                <PresenceFold key="working" appear className="pt-2.5">
                  <div className="flex items-center gap-2 pl-[27px] text-xs">
                    <Loader size={12} style={{ color: 'var(--color-accent-primary)' }} />
                    <TextShimmer as="span" className="text-xs">{t('pageViewer.comments.working')}</TextShimmer>
                  </div>
                </PresenceFold>
              )}
            </AnimatePresence>
          </Disclosure>
        </div>
      </div>

      <Disclosure open={open && canReply}>
        <div className="pt-2.5">
          <CommentComposer
            mode={REPLY}
            value={reply}
            onChange={setReply}
            placeholder={resolved ? t('pageViewer.comments.replyToReopen') : t('pageViewer.comments.replyPlaceholder')}
            canQueue={queueRoute !== null}
            canSend={sendRoute !== null}
            onSubmit={async (intent) => {
              const route = destinations.targetFor(comment, intent);
              if (!route) return;
              await actions.reply(comment, reply.trim(), route);
              setReply('');
            }}
          />
        </div>
      </Disclosure>

      <Disclosure open={confirming}>
        <div className="page-comment-card-over flex items-center gap-1.5 pt-2">
          <span className="min-w-0 flex-1 text-xs" style={{ color: 'var(--color-text-secondary)' }}>
            {t('pageViewer.comments.deleteConfirm')}
          </span>
          <Button type="button" size="sm" variant="ghost" className="h-7 px-2 text-xs" disabled={busy} onClick={() => setConfirming(false)}>
            {t('common.cancel')}
          </Button>
          <Button
            type="button"
            size="sm"
            variant="destructive"
            className="h-7 px-2.5 text-xs"
            disabled={busy}
            onClick={() => void run(async () => {
              await actions.remove(comment);
              onDeleted();
            })}
          >
            {t('common.delete')}
          </Button>
        </div>
      </Disclosure>
      {failed && (
        <p role="alert" className="page-comment-card-over pt-1.5 text-xs" style={{ color: 'var(--color-icon-danger)' }}>
          {t('pageViewer.comments.saveFailed')}
        </p>
      )}
    </div>
  );
}

/** "for <thread> ↗" after Queued, or "Sent to <thread> ↗": the way to the thread, which opens there. */
function ThreadLink({ id, label, text }: { id: string; label: string; text: string }): React.ReactElement {
  return (
    <Link to={`/chat/t/${id}`} className="page-comment-thread-link inline-flex min-w-0 items-center gap-0.5" title={label}>
      <span className="truncate">{text}</span>
      <ArrowUpRight className="h-3 w-3 shrink-0" aria-hidden />
    </Link>
  );
}

/**
 * A queued comment's way out: the state in the pin's own dashed edge, the
 * thread it waits for, and Send now, which sends it alone to that thread
 * and shows the send until the server has it. The chat beside the file needs
 * no link; a comment whose thread is gone goes where new comments go, and
 * its Send now holds (`onSend` null) until that is known.
 */
function QueuedRow({ comment, besideThreadId, pending, onSend }: {
  comment: ArtifactComment;
  besideThreadId: string | null;
  pending: boolean;
  onSend: (() => void) | null;
}): React.ReactElement {
  const { t } = useTranslation();
  const thread = comment.thread_archived ? null : comment.thread_id;
  const label = thread && thread !== besideThreadId
    ? threadLabel(t, { thread_id: thread, title: comment.thread_title }, besideThreadId)
    : null;
  return (
    <>
      <span className="page-comment-queued-tag shrink-0">{t('pageViewer.comments.tagQueued')}</span>
      {thread && label ? (
        <ThreadLink id={thread} label={label} text={t('pageViewer.comments.forThread', { thread: label })} />
      ) : thread ? (
        <span className="truncate" style={{ color: 'var(--color-text-secondary)' }}>{t('pageViewer.comments.forThisThread')}</span>
      ) : null}
      <Button
        type="button"
        size="sm"
        className="ml-auto h-6 shrink-0 gap-1 rounded-md px-2 text-xs"
        // Held while the send goes, without the faded look of a control that cannot act.
        aria-disabled={pending || !onSend || undefined}
        onClick={() => {
          if (!pending) onSend?.();
        }}
      >
        <InPlaceSwap
          swapped={pending}
          className="justify-items-center"
          from={(
            <>
              <ArrowUp className="h-3 w-3" aria-hidden />
              {t('pageViewer.comments.sendNow')}
            </>
          )}
          to={(
            <>
              <SendingGlyph on={pending} />
              {t('pageViewer.comments.sending')}
            </>
          )}
        />
      </Button>
    </>
  );
}

/** One answer under the comment: the agent's with its avatar, in markdown, or the owner's next words as typed. */
function Reply({ message, locale, now }: { message: ArtifactCommentMessage; locale: string; now: number }): React.ReactElement {
  const { t } = useTranslation();
  const agent = message.author === 'agent';
  return (
    <div role="listitem" className="grid gap-0.5 pl-[27px]">
      <div className="flex min-w-0 items-center gap-1.5 text-xs" style={{ color: 'var(--color-text-secondary)' }}>
        {agent && (
          <span
            aria-hidden
            className="grid h-[18px] w-[18px] shrink-0 place-items-center rounded-full border"
            style={{ borderColor: 'var(--color-border-default)', backgroundColor: 'var(--color-bg-page)', color: 'var(--color-text-primary)' }}
          >
            <AgentMark className="h-3 w-3" />
          </span>
        )}
        <span className="font-medium" style={{ color: 'var(--color-text-primary)' }}>
          {agent ? t('pageViewer.comments.agent') : t('pageViewer.comments.you')}
        </span>
        <time className="whitespace-nowrap" style={{ color: 'var(--color-text-tertiary)' }} dateTime={message.created_at}>
          {relativeTime(message.created_at, locale, now)}
        </time>
      </div>
      {agent ? (
        <Markdown variant="compact" content={message.body} className="text-[13px] leading-[1.45]" />
      ) : (
        <p className="text-[13px] leading-[1.45] break-words whitespace-pre-wrap" style={{ color: 'var(--color-text-primary)' }}>
          {message.body}
        </p>
      )}
    </div>
  );
}
