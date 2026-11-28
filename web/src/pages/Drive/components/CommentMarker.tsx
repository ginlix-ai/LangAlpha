import { MessageSquare } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { useDriveCommentCount } from '@/hooks/useDriveComments';
import type { DriveEntry } from '@/types/drive';
import { useDriveContext } from '../driveContext';

/**
 * A file's comments, beside its name in a list: how many are still open, and
 * a dot while an answer waits unread. Quiet like the row's other details; the
 * dot is the one mark in the accent, since it asks to be read. A button of
 * its own beside the row's: it opens the file with its comment list, at the
 * first answer not read yet.
 */
export function CommentMarker({ entry }: { entry: Pick<DriveEntry, 'id' | 'path'> }) {
  const { t } = useTranslation();
  const { space, go } = useDriveContext();
  const count = useDriveCommentCount(space, entry.id);
  if (!count) return null;
  const open = count.open > 0 ? t('drive:comments.open', { count: count.open }) : null;
  const unread = count.unread > 0 ? t('drive:comments.unread', { count: count.unread }) : null;
  // Joined as the reader's language joins two clauses.
  const said = open && unread ? t('drive:comments.openAndUnread', { open, unread }) : (open ?? unread ?? '');
  return (
    <button
      type="button"
      className="drive-comment-marker tap-target"
      title={said}
      data-testid={`drive-comment-marker-${entry.id}`}
      onClick={(e) => {
        // Not the row's own open, which it sits in.
        e.stopPropagation();
        go({ file: entry.path, ask: 'list' });
      }}
    >
      <span className="sr-only">{said}</span>
      <span className="drive-comment-marker-glyph" aria-hidden="true">
        <MessageSquare className="h-3 w-3" />
        {count.open > 0 && <span className="drive-comment-marker-count">{count.open}</span>}
        {count.unread > 0 && <span className="drive-comment-marker-dot" />}
      </span>
    </button>
  );
}
