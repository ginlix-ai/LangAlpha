import { useCallback, useEffect, useRef } from 'react';
import { useNavigate } from 'react-router';
import { useTranslation } from 'react-i18next';
import { toast } from '@/components/ui/use-toast';
import { driveErrorType, getEntryById } from '@/api/drive';
import { apiErrorStatus } from '@/api/errors';
import { driveHref, type DriveLocation } from '@/lib/driveLinks';
import { COMMENTS_SPACE } from '@/hooks/useDriveComments';
import type { ArtifactKind } from '@/types/api';
import type { ArtifactCommentOpeners, ArtifactCommentRef } from './artifactCommentReply';

type DriveRef = Extract<ArtifactCommentRef, { kind: 'drive' }>;

/**
 * Where a Drive file's comment opens, or why it cannot: the file is in the
 * Drive's trash, it is no longer in the Drive, or it could not be looked up.
 */
export type DriveCommentOpen = { loc: DriveLocation } | { problem: 'trash' | 'gone' | 'failed' };

/**
 * The place in the Drive a Drive file's comment opens. The file is found by its entry
 * id, so one renamed or moved since the comment still opens. The server says
 * when that id is in the trash or gone; only where it could not be asked (a
 * failed read) does the path the line names open instead, as any Drive link
 * does.
 */
export async function driveCommentOpen(
  ref: Pick<DriveRef, 'path' | 'commentId' | 'entryId'>,
  find: (space: string, entryId: string) => Promise<{ path: string }> = getEntryById,
): Promise<DriveCommentOpen> {
  const at = (file: string): DriveCommentOpen => ({ loc: { file, ask: { comment: ref.commentId } } });
  const byPath = ref.path ? at(ref.path) : null;
  if (!ref.entryId) return byPath ?? { problem: 'gone' };
  try {
    return at((await find(COMMENTS_SPACE, ref.entryId)).path);
  } catch (err) {
    if (driveErrorType(err) === 'in_trash') return { problem: 'trash' };
    // Its path may hold another file by now, so a file that is gone opens nothing.
    if (apiErrorStatus(err) === 404) return { problem: 'gone' };
    return byPath ?? { problem: 'failed' };
  }
}

const PROBLEM_KEY = {
  trash: 'chat.artifactComments.driveInTrash',
  gone: 'chat.artifactComments.driveGone',
  failed: 'chat.artifactComments.driveOpenFailed',
} as const;

/** Clicks on a Drive comment so far, on any opener: each row of a chat holds its own. */
let clicks = 0;

/**
 * Counts a click, and answers whether it is still the last one. A plain
 * function, since a compiled hook reads a module's own `let` as a constant.
 */
function lastClick(): () => boolean {
  clicks += 1;
  const mine = clicks;
  return () => mine === clicks;
}

/**
 * Opens a Drive file's comment, or says why it cannot: through `place` where
 * the view shows the Drive itself (a chat, in its panel's Drive tab), else on
 * the Drive page.
 */
export function useDriveCommentOpener(place?: (loc: DriveLocation) => void): (ref: ArtifactCommentRef) => void {
  const navigate = useNavigate();
  const { t } = useTranslation();
  // Only the last click opens: a slower lookup for an earlier one, on this
  // opener or another row's, or one that answers after the view is gone,
  // opens nothing.
  const gone = useRef(false);
  useEffect(() => {
    gone.current = false;
    return () => { gone.current = true; };
  }, []);
  return useCallback((ref: ArtifactCommentRef) => {
    if (ref.kind !== 'drive') return;
    const last = lastClick();
    void driveCommentOpen(ref).then((open) => {
      if (!last() || gone.current) return;
      if ('problem' in open) toast({ description: t(PROBLEM_KEY[open.problem]) });
      else if (place) place(open.loc);
      else navigate(driveHref(open.loc));
    });
  }, [navigate, t, place]);
}

/**
 * How a comment of `kind` opens from a view's `openers`. A view names the
 * kinds it places itself (a file beside the chat, a chart's tab, a chat's
 * Drive tab); a Drive file's opens on the Drive page from a view that names
 * no place for it, so that is added here rather than by each view. A view
 * with no openers (a shared chat) opens nothing, the Drive included.
 */
export function useCommentOpener(
  openers: ArtifactCommentOpeners | undefined,
  kind: ArtifactKind,
): ((ref: ArtifactCommentRef) => void) | undefined {
  const openDrive = useDriveCommentOpener();
  if (!openers) return undefined;
  return openers[kind] ?? (kind === 'drive' ? openDrive : undefined);
}
