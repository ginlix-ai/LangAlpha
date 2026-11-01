/**
 * The agent's answers to file comments, as quiet lines in its reply:
 * "Replied to comment 2 on <file>", and "and resolved it" when the reply
 * resolved it. Each number opens the file on that comment and carries the
 * unread dot until the owner has read what the agent posted there. Replies in
 * a row to one file share a line, which names the file once and the resolved
 * ones after it when only some were. Under the line, each answer's first
 * lines, which open in place: the agent answers under the comment, so a chat
 * reader sees it here without the agent saying it twice.
 */
import React, { useLayoutEffect, useRef, useState } from 'react';
import { Trans, useTranslation } from 'react-i18next';
import { SwapCell } from '@/components/ui/crossfade';
import { useArtifactComments, useArtifactUnread } from '@/hooks/useArtifact';
import { useLocale } from '@/hooks/useLocale';
import { AnimatePresence, motion, useReducedMotion } from '@/lib/framer';
import { followHeight } from '@/lib/followHeight';
import { formatList } from '@/lib/format';
import { SPRING_FOLD, TWEEN } from '@/lib/motion';
import { flattenMarkdown } from '@/lib/plainLine';
import { useMotionSafe } from '@/lib/reducedMotion';
import { cn } from '@/lib/utils';
import { useWorkspaceId } from '../../contexts/WorkspaceContext';
import { fileNameOf } from './artifactCommentSend';
import { groupCommentReplies, readArtifactCommentReply, type ArtifactCommentReply } from './artifactCommentReply';
import type { InlineCardProps } from '../charts/cardReaders';
import Markdown from '../Markdown';
import { useMessageActions } from '../messageList/MessageActionsContext';
import './unreadMarks.css';

export interface CommentReplyMember {
  key: string;
  reply: ArtifactCommentReply;
  /** Opens the call's own result, where no file can be opened directly. */
  openCall?: () => void;
}

// Markers a list of numbers is formatted around, so the locale's own list
// ("2, 3 and 4", "2、3 和 4") keeps its words while each number is a link.
const OPEN = '\u0001';
const CLOSE = '\u0002';
const MARKED = new RegExp(`${OPEN}(\\d+)${CLOSE}`);

// The dot's size and the room either side of it, which framer tweens to
// nothing: close on the digit, and clear of a comma set right after it.
const DOT = 6;
const DOT_GAP = 1;

const LINK_CLASS = 'cursor-pointer rounded-sm underline decoration-(--color-text-tertiary) underline-offset-2 outline-hidden hover:decoration-(--color-text-primary) focus-visible:ring-2 focus-visible:ring-ring';

function UnreadDot({ on }: { on: boolean }): React.ReactElement {
  const { t } = useTranslation();
  // framer's reduced motion stills transforms only, and the dot's room moves
  // the words after it, so under it the room comes and goes at once.
  const snap = { duration: 0 };
  const room = useReducedMotion() ? { width: snap, marginLeft: snap, marginRight: snap } : null;
  return (
    <AnimatePresence initial={false}>
      {on && (
        <motion.span
          key="dot"
          className="comment-unread-dot"
          role="img"
          aria-label={t('chat.artifactComments.reply.unread')}
          initial={{ opacity: 0, scale: 0.6, width: 0, marginLeft: 0, marginRight: 0 }}
          animate={{ opacity: 1, scale: 1, width: DOT, marginLeft: DOT_GAP, marginRight: DOT_GAP }}
          exit={{ opacity: 0, scale: 0.6, width: 0, marginLeft: 0, marginRight: 0, transition: { ...TWEEN.exit, ...room } }}
          transition={{ ...TWEEN.quick, ...room }}
        />
      )}
    </AnimatePresence>
  );
}

function NumberLinks({ group, onOpen, unread }: {
  group: readonly CommentReplyMember[];
  onOpen: ((member: CommentReplyMember) => void) | null;
  /** The comments holding something unread; null draws no dots (not the owner, or not read yet). */
  unread: ReadonlySet<string> | null;
}): React.ReactElement {
  const locale = useLocale();
  const parts = formatList(group.map((_, i) => `${OPEN}${i}${CLOSE}`), locale).split(MARKED);
  return (
    <>
      {parts.map((part, i) => {
        // split() with a capture group puts each captured index at odd places.
        if (i % 2 === 0) return part ? <React.Fragment key={i}>{part}</React.Fragment> : null;
        const member = group[Number(part)];
        const number = member.reply.number;
        const dot = unread && <UnreadDot on={unread.has(member.reply.commentId)} />;
        if (!onOpen) return <span key={i} style={{ color: 'var(--color-text-primary)' }}>{number}{dot}</span>;
        // A digit is a few px wide: the padding gives the pointer room, and
        // the equal negative margin keeps the sentence where it was.
        return (
          <button
            key={i}
            type="button"
            className={cn('-mx-1 -my-0.5 px-1 py-0.5', LINK_CLASS)}
            style={{ color: 'var(--color-text-primary)' }}
            onClick={() => onOpen(member)}
          >
            {number}
            {dot}
          </button>
        );
      })}
    </>
  );
}

/**
 * One answer's first two lines, opening in place to the whole of what the
 * chat holds. A clipped answer ends, open, on a way to the comment, where the
 * rest of it is. The box tweens between the two heights it measures.
 */
function ReplyExcerpt({ reply, label, onOpen }: {
  reply: ArtifactCommentReply;
  /** The comment's number, where the line names more than one. */
  label: number | null;
  onOpen: (() => void) | null;
}): React.ReactElement {
  const { t } = useTranslation();
  const motionSafe = useMotionSafe();
  const [open, setOpen] = useState(false);
  const [overflows, setOverflows] = useState(false);
  const boxRef = useRef<HTMLDivElement>(null);
  const innerRef = useRef<HTMLDivElement>(null);
  const clampRef = useRef<HTMLParagraphElement>(null);
  const fromRef = useRef<number | null>(null);
  const preview = flattenMarkdown(reply.text);

  // Whether the clamp hides anything, read again as the column's width moves.
  useLayoutEffect(() => {
    const el = clampRef.current;
    if (!el || open) return;
    const measure = () => setOverflows(el.scrollHeight > el.clientHeight + 1);
    measure();
    const observer = new ResizeObserver(measure);
    observer.observe(el);
    return () => observer.disconnect();
  }, [open, preview]);

  useLayoutEffect(() => {
    const box = boxRef.current;
    const inner = innerRef.current;
    const from = fromRef.current;
    fromRef.current = null;
    if (!box || !inner) return;
    const release = () => {
      box.style.height = '';
      box.style.overflow = '';
    };
    if (from === null) {
      release();
      return;
    }
    box.style.overflow = 'hidden';
    box.style.height = `${from}px`;
    const stop = followHeight(box, inner, { from, leg: () => SPRING_FOLD, onLand: release });
    return () => {
      stop();
      release();
    };
  }, [open]);

  const toggle = () => {
    if (motionSafe && boxRef.current) fromRef.current = boxRef.current.getBoundingClientRect().height;
    setOpen((was) => !was);
  };

  return (
    <div className="flex text-[13px] leading-5" style={{ color: 'var(--color-text-secondary)' }}>
      {label !== null && (
        <span className="mr-1.5 shrink-0 tabular-nums" style={{ color: 'var(--color-text-tertiary)' }}>{label}</span>
      )}
      <div className="min-w-0 flex-1">
        <div ref={boxRef}>
          <div ref={innerRef}>
            {open ? (
              <>
                <Markdown variant="compact" content={reply.text} className="text-[13px] leading-5" />
                {reply.clipped && onOpen && (
                  <button type="button" className={cn('mt-1', LINK_CLASS)} style={{ color: 'var(--color-text-primary)' }} onClick={onOpen}>
                    {t('chat.artifactComments.reply.readRest')}
                  </button>
                )}
              </>
            ) : (
              <p ref={clampRef} className="comment-reply-excerpt is-clamped break-words">{preview}</p>
            )}
          </div>
        </div>
        {(open || overflows) && (
          <button
            type="button"
            className={cn('mt-0.5', LINK_CLASS)}
            style={{ color: 'var(--color-text-tertiary)' }}
            aria-expanded={open}
            onClick={toggle}
          >
            {t(open ? 'chat.artifactComments.reply.showLess' : 'chat.artifactComments.reply.showMore')}
          </button>
        )}
      </div>
    </div>
  );
}

function ReplyLine({ group }: { group: readonly CommentReplyMember[] }): React.ReactElement {
  const { onOpenArtifactComment } = useMessageActions();
  const first = group[0].reply;
  // Only the owner can read the file's comments; a shared chat names the file
  // by its path and marks nothing unread.
  const owner = !!useWorkspaceId();
  const artifactId = owner ? first.artifactId : null;
  const { data: comments } = useArtifactComments(artifactId);
  const { commentIds } = useArtifactUnread(artifactId);
  // Null until the comments are read, so a dot already there when they
  // arrive is drawn rather than played in.
  const unread = comments ? new Set(commentIds) : null;
  const named = comments?.find((c) => c.comment_id === first.commentId)?.title?.trim();
  const title = named || fileNameOf(first.path);
  const openMember = onOpenArtifactComment
    ? (member: CommentReplyMember) => onOpenArtifactComment({
      artifactId: member.reply.artifactId,
      workspaceId: member.reply.workspaceId,
      path: member.reply.path,
      commentId: member.reply.commentId,
    })
    : group.every((member) => member.openCall)
      ? (member: CommentReplyMember) => member.openCall?.()
      : null;
  const resolved = group.filter((member) => member.reply.resolved);
  const sentence = resolved.length === 0 ? 'replied'
    : resolved.length === group.length ? 'repliedResolved'
    : 'repliedPartlyResolved';
  // Full strength while anything in the line is unread; read, it goes quiet.
  const anyUnread = !!unread && group.some((member) => unread.has(member.reply.commentId));
  const answered = group.filter((member) => member.reply.text.trim());
  return (
    <div className="grid gap-1">
      <div
        className="text-[13px] leading-5 transition-colors"
        style={{ color: anyUnread ? 'var(--color-text-primary)' : 'var(--color-text-secondary)' }}
      >
        {/* A reply joining the line says a new sentence where the old one was,
            and so does the file's title arriving after the line was drawn. */}
        <SwapCell as="div" swapKey={`${group.length}:${resolved.length}:${title}`}>
          <Trans
            i18nKey={`chat.artifactComments.reply.${sentence}`}
            count={group.length}
            values={{ title }}
            components={{
              nums: <NumberLinks group={group} onOpen={openMember} unread={unread} />,
              // A number named again among the resolved keeps its dot where it
              // was first named: one comment, one mark.
              resolved: <NumberLinks group={resolved} onOpen={openMember} unread={null} />,
            }}
          />
        </SwapCell>
      </div>
      {answered.map((member) => (
        <ReplyExcerpt
          key={member.key}
          reply={member.reply}
          label={group.length > 1 ? member.reply.number : null}
          onOpen={openMember ? () => openMember(member) : null}
        />
      ))}
    </div>
  );
}

export function ArtifactCommentReplyLines({ members }: { members: readonly CommentReplyMember[] }): React.ReactElement {
  return (
    <div className="grid gap-2">
      {groupCommentReplies(members).map((group) => <ReplyLine key={group[0].key} group={group} />)}
    </div>
  );
}

/** One reply on its own, where a surface draws each card by itself. */
export function ArtifactCommentReplyCard({ artifact, onClick }: InlineCardProps): React.ReactElement | null {
  const reply = readArtifactCommentReply(artifact);
  if (!reply) return null;
  return <ArtifactCommentReplyLines members={[{ key: reply.commentId, reply, openCall: onClick }]} />;
}
