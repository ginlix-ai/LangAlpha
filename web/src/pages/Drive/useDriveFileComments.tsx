import { useEffect, useEffectEvent, useMemo, type ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import { useArtifactComments, useArtifactRecord } from '@/hooks/useArtifact';
import { useHomeWorkspaceId } from '@/hooks/useHomeWorkspace';
import { useStableHandler } from '@/hooks/useStableHandler';
import { COMMENTS_SPACE } from '@/hooks/useDriveComments';
import type { ViewerCommentsTarget } from '@/pages/ChatAgent/components/viewers/comments/ViewerComments';
import { CommentTools } from '@/pages/ChatAgent/components/viewers/html/comments/CommentTools';
import type { CommentSurface } from '@/pages/ChatAgent/components/viewers/html/comments/commentSurface';
import { LazyCommentLayer } from '@/pages/ChatAgent/components/viewers/html/comments/lazyCommentLayer';
import { useCommentUi } from '@/pages/ChatAgent/components/viewers/html/comments/useCommentUi';
import type { DriveCommentAsk } from '@/lib/driveLinks';
import type { ArtifactComment } from '@/types/artifacts';
import type { DriveEntry, DriveVersion } from '@/types/drive';
import { versionOfComment } from './commentVersions';
import type { DriveHost } from './driveContext';
import type { MemoView } from './memoMarks';
import type { ShownDocument } from './useShownDocument';

/** The open file stands where a panel's tab id would: the pane shows one at a time. */
export const DRIVE_FILE_TAB = 'drive-file';

/**
 * What a comment box on the Drive does differently: its "To:" line can search
 * every chat, and a box goes without a picture, there being no workspace
 * folder to keep one. On the page Enter sends at once, as no chat beside it
 * shows a queue; in a chat's Drive tab the chat does, so the owner's
 * preference holds as for a file beside it.
 */
export const DRIVE_COMMENT_SURFACE: CommentSurface = { enter: 'send', pickAnyChat: true, keepsPictures: false };
export const DRIVE_TAB_COMMENT_SURFACE: CommentSurface = { pickAnyChat: true, keepsPictures: false };

export interface DriveFileComments {
  /** What the file's viewer draws its comment layer for, on which bytes; null where the file takes none now. */
  target: ViewerCommentsTarget | null;
  /**
   * How the comments read, for the pane to provide around the header's tools
   * and the body (`CommentSurfaceContext`): a past version's are read only
   * and only those made on it show; an Outdated card on the current file
   * links to the version it was made on.
   */
  surface: CommentSurface;
  /** The comment toggle, the counts and Send all, for the file's header. */
  tools: ReactNode;
}

/**
 * Which comments the pane shows, and the hash of the bytes they are on: the
 * current file's, which take new ones; a past version's, read only; or none.
 */
export type CommentScope =
  | { kind: 'head'; sha: string | null }
  | { kind: 'version'; seq: number; sha: string }
  | null;

/**
 * The current file outside the editor takes comments. A past version of a
 * memo shows, read only, those made on it, once its versions are read (they
 * place a comment on it). Neither a memo's record, its changes nor who wrote
 * what shows any, nor a file in the trash or in a space other than the
 * person's own.
 */
export function commentScope({ space, entry, view, shown, editing }: {
  space: string | null;
  entry: DriveEntry | undefined;
  view: MemoView;
  shown: Pick<ShownDocument, 'docVersion' | 'viewed' | 'sha'>;
  editing: boolean;
}): CommentScope {
  if (space !== COMMENTS_SPACE || !entry || entry.kind !== 'file' || entry.deleted_at || editing || view !== 'document') return null;
  if (shown.docVersion === null) return { kind: 'head', sha: shown.sha };
  return shown.viewed ? { kind: 'version', seq: shown.viewed.seq, sha: shown.viewed.sha256 } : null;
}

/** The comment an ask opens: the one it names, or for the list the first holding an unread answer among those shown. */
export function askedComment(ask: DriveCommentAsk, comments: readonly ArtifactComment[], shows?: (c: ArtifactComment) => boolean): string | null {
  if (ask !== 'list') return ask.comment;
  return [...comments]
    .filter((c) => c.unread > 0 && (!shows || shows(c)))
    .sort((a, b) => a.number - b.number)[0]?.comment_id ?? null;
}

/**
 * The owner's comments on the open Drive file. Its record is keyed by the
 * file's entry id under the owner's Home, so a rename or a move keeps it.
 *
 * `ask` comes from the place the view is at (a chat's link, a list row's
 * marker), on the page's address or a panel tab; it is shown once the
 * comments are read, and `onAsked` lets the place drop it.
 * `onSeeVersion` opens a past version at one of its comments.
 */
export function useDriveFileComments({ entry, scope, versions, ask, onAsked, onSeeVersion, host = 'page' }: {
  entry: DriveEntry | undefined;
  scope: CommentScope;
  /** Where the Drive is shown: a chat's panel has a chat beside the file. */
  host?: DriveHost;
  /** The file's kept versions, where it is a memo: what places a comment on one. */
  versions: readonly DriveVersion[] | undefined;
  ask: DriveCommentAsk | null;
  onAsked: () => void;
  onSeeVersion: (seq: number, commentId: string) => void;
}): DriveFileComments {
  const { t } = useTranslation();
  const ui = useCommentUi();
  const homeId = useHomeWorkspaceId();
  const entryId = entry?.id ?? null;
  const on = scope !== null && !!homeId && !!entryId;
  const pastSeq = scope?.kind === 'version' ? scope.seq : null;
  const sha = scope?.sha ?? null;
  const head = entry?.version ?? null;

  // Leaving what takes comments ends the mode and the list; a past version
  // takes none, so the mode ends there too.
  const { setMode, setList, requestFocus } = ui;
  useEffect(() => {
    if (!on || pastSeq !== null) setMode(DRIVE_FILE_TAB, false);
    if (!on) setList(DRIVE_FILE_TAB, false);
  }, [on, pastSeq, setMode, setList]);

  const shows = useMemo(
    () => (pastSeq !== null && versions ? (c: ArtifactComment) => versionOfComment(c, versions) === pastSeq : undefined),
    [pastSeq, versions],
  );

  // An ask for the list waits for the comments, to open it at the first unread answer.
  const wantsComments = on && ask === 'list';
  const record = useArtifactRecord(homeId, entryId ? { kind: 'drive', path: entryId } : null, { enabled: wantsComments });
  const comments = useArtifactComments(wantsComments ? record.data?.artifact_id : null);
  const settled = ask !== 'list' || record.data === null || record.isError || comments.isSuccess || comments.isError;
  const asked = useEffectEvent(onAsked);
  const answer = useEffectEvent(() => {
    if (!ask || !entryId) return;
    if (ask === 'list') setList(DRIVE_FILE_TAB, true);
    const commentId = askedComment(ask, comments.data ?? [], shows);
    if (commentId) requestFocus({ kind: 'drive', path: entryId, commentId });
    asked();
  });
  useEffect(() => {
    if (on && ask && settled) answer();
  }, [on, ask, settled]);

  // The layer's code is fetched with the file, so it draws with it.
  useEffect(() => {
    if (on) void LazyCommentLayer.preload();
  }, [on]);

  const seeVersion = useStableHandler(onSeeVersion);
  const target = useMemo<ViewerCommentsTarget | null>(
    () => (on && homeId && entryId
      ? { workspaceId: homeId, kind: 'drive', path: entryId, tabId: DRIVE_FILE_TAB, ui, active: true, fileSha: sha }
      : null),
    [on, homeId, entryId, ui, sha],
  );
  const surface = useMemo<CommentSurface>(() => {
    const base = host === 'panel' ? DRIVE_TAB_COMMENT_SURFACE : DRIVE_COMMENT_SURFACE;
    if (pastSeq !== null) return { ...base, readOnly: true, shows };
    return {
      ...base,
      // An Outdated comment is seen as it was made on the version it was made
      // on, while that one is kept and is not the head itself.
      outdatedLink: versions
        ? (comment) => {
            const seq = versionOfComment(comment, versions);
            return seq === null || seq === head
              ? null
              : { label: t('drive:comments.seeOnVersion', { seq }), open: () => seeVersion(seq, comment.comment_id) };
          }
        : undefined,
    };
  }, [host, pastSeq, shows, versions, head, t, seeVersion]);

  const tools = target && (
    <>
      {pastSeq !== null && (
        <span className="drive-comments-scope" title={t('drive:comments.onVersionTitle', { seq: pastSeq })}>
          {t('drive:comments.onVersion', { seq: pastSeq })}
        </span>
      )}
      <CommentTools
        tabId={DRIVE_FILE_TAB}
        workspaceId={target.workspaceId}
        target={{ kind: 'drive', path: target.path }}
        ui={ui}
        variant="bar"
        modeTitle={t('pageViewer:comments.modeTitleFile')}
      />
    </>
  );
  return { target, surface, tools };
}
