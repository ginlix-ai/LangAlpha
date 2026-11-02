/**
 * The comments a chat message sent, under its note: a line naming how many
 * and on which file, then one line per comment, its pin number, what it is on
 * and the owner's words. History, a steer and the view's own send all draw it
 * from the same `ArtifactCommentsShown`, so a reload shows what the send did.
 */
import React from 'react';
import { useTranslation } from 'react-i18next';
import type { TFunction } from 'i18next';
import { TextShimmer } from '@/components/ui/text-shimmer';
import { cn } from '@/lib/utils';
import type { ArtifactCommentsShown, ShownArtifactComment } from '@/types/api';
import { commentsByFile } from './artifactCommentSend';
import type { ArtifactCommentRef } from './artifactCommentReply';
import { PinBadge } from './PinBadge';
import { useMessageActions } from '../messageList/MessageActionsContext';

/** What a comment is on: the element's label and the text it quotes. A short
 *  element is labelled by its own words, which the line then says once, as
 *  the comment's card does. */
function anchorLine(t: TFunction, label: string | null | undefined, quote: string | null | undefined): string {
  if (label && quote && label !== quote) return t('chat.artifactComments.message.labelQuote', { label, quote });
  if (label) return label;
  if (quote) return t('chat.artifactComments.message.quote', { quote });
  return '';
}

function CommentLine({ comment, refused, onOpen }: {
  comment: ShownArtifactComment;
  refused: boolean;
  onOpen?: (comment: ArtifactCommentRef) => void;
}): React.ReactElement {
  const { t } = useTranslation();
  const on = anchorLine(t, comment.label, comment.quote);
  const body = (
    <>
      {/* A refused send's comments went back to the queue, so they wear its badge. */}
      <PinBadge number={comment.number} state={refused ? 'queued' : 'sent'} className="mt-px" />
      <span className="grid min-w-0">
        {on && (
          <span className="truncate text-xs" style={{ color: 'var(--color-text-secondary)' }}>{on}</span>
        )}
        <span className="line-clamp-4 whitespace-pre-wrap text-[13px] leading-snug decoration-(--color-text-tertiary) underline-offset-2 group-hover:underline">
          {comment.text}
        </span>
      </span>
    </>
  );
  const row = 'group grid w-full grid-cols-[auto_minmax(0,1fr)] gap-2 rounded-md py-0.5 text-left';
  if (!onOpen) return <div className={row}>{body}</div>;
  return (
    <button
      type="button"
      className={cn(row, 'cursor-pointer outline-hidden focus-visible:ring-2 focus-visible:ring-ring')}
      title={t('chat.artifactComments.message.open')}
      aria-label={t('chat.artifactComments.message.openLabel', { number: comment.number })}
      onClick={() => onOpen({
        artifactId: comment.artifact_id,
        workspaceId: comment.workspace_id,
        path: comment.path,
        commentId: comment.comment_id,
      })}
    >
      {body}
    </button>
  );
}

export function ArtifactCommentList({ shown, pending = false, leads = false, refused = false, className }: {
  shown: ArtifactCommentsShown;
  /** The send has not reached the agent yet (a steer still waiting). */
  pending?: boolean;
  /** Nothing sits above the list in its bubble, so its head line is the
   *  message's first, the one that shimmers while the send is pending. */
  leads?: boolean;
  /** The send was refused and its comments are queued again. */
  refused?: boolean;
  className?: string;
}): React.ReactElement {
  const { t } = useTranslation();
  // A shared chat has no panel to open the file in.
  const { onOpenArtifactComment } = useMessageActions();
  const groups = commentsByFile(shown.comments);
  const count = shown.comments.length;
  const head = groups.length > 1
    ? t('chat.artifactComments.message.headFiles', { comments: count, files: groups.length })
    : t('chat.artifactComments.message.head', { count, title: groups[0]?.title ?? '' });
  return (
    <div className={cn('grid gap-1.5 text-left transition-opacity', pending && 'opacity-70', className)}>
      <div className="text-xs" style={{ color: 'var(--color-text-secondary)' }}>
        {pending && leads ? (
          <TextShimmer
            as="span"
            className="text-xs [--base-color:var(--color-text-secondary)] [--base-gradient-color:var(--color-text-primary)]"
            duration={1.5}
          >
            {head}
          </TextShimmer>
        ) : head}
      </div>
      {groups.map((group) => (
        <div key={group.artifactId} className="grid gap-1">
          {groups.length > 1 && (
            <div className="truncate pl-[26px] text-[11px]" style={{ color: 'var(--color-text-tertiary)' }}>
              {group.title}
            </div>
          )}
          {group.comments.map((comment) => (
            <CommentLine key={comment.comment_id} comment={comment} refused={refused} onOpen={onOpenArtifactComment} />
          ))}
        </div>
      ))}
    </div>
  );
}
