import type React from 'react';
import { useState } from 'react';
import type { UseQueryResult } from '@tanstack/react-query';
import { apiErrorStatus } from '@/api/errors';
import { useDriveEntry } from '@/hooks/useDrive';
import type { DriveCommentAsk } from '@/lib/driveLinks';
import { cn } from '@/lib/utils';
import type { FileBody as Body } from '@/pages/ChatAgent/components/filePanel/fileBody';
import type { FileError } from '@/pages/ChatAgent/components/filePanel/fileErrors';
import type { OpenFileHandler } from '@/pages/ChatAgent/utils/fileLocation';
import { CommentSurfaceContext } from '@/pages/ChatAgent/components/viewers/html/comments/commentSurface';
import type { DriveEntry, DriveVersion, DriveVersions } from '@/types/drive';
import { useDriveContext } from '../driveContext';
import { bodyTakesComments } from '../bodyComments';
import { noneCategory, showsExtract } from '../driveBody';
import { nameOf } from '../drivePaths';
import { fileAddress, memoState, resolveFileView, type FileView, type MemoView } from '../memoMarks';
import type { DriveEditor } from '../useDriveEditor';
import { useShownDocument } from '../useShownDocument';
import { commentScope, useDriveFileComments } from '../useDriveFileComments';
import { FileBars } from './FileBars';
import { FileBody, type BodyView } from './FileBody';
import { FileHeader } from './FileHeader';
import { MemoAuthors } from './MemoAuthors';
import { MemoChanges } from './MemoChanges';
import { MemoHeader } from './MemoHeader';
import { MemoRecord } from './MemoRecord';

function bodyError(err: unknown): FileError | null {
  if (!err) return null;
  return apiErrorStatus(err) === 404 ? { category: 'not_found' } : { category: 'unknown' };
}

/**
 * The open file: its header, a bar when something needs saying, then the
 * file. A memo's header carries its status and its views (the document, its
 * record, the changes between versions, who wrote what); an ordinary file
 * whose last save can be undone says so under its header.
 */
export function DriveFilePane({
  path, entry, entryError, onRetry, fileView, versions: versionsQuery, mobile, editor, head, imageLoader, onOpenLink, commentAsk = null, onCommentAsked,
}: {
  path: string;
  entry: DriveEntry | undefined;
  entryError: 'gone' | 'failed' | null;
  onRetry: () => void;
  /** The view, version and comparison the address names, as `resolveFileView` read it for this file. */
  fileView: FileView;
  /** The file's versions, which the address may name; read only for a file with a record. */
  versions: UseQueryResult<DriveVersions>;
  mobile: boolean;
  editor: DriveEditor;
  /** The head's body, read for the viewer and the editor alike. */
  head: UseQueryResult<Body>;
  imageLoader: (path: string) => Promise<string>;
  onOpenLink: OpenFileHandler;
  /** What of the file's comments the address asks to show, once: one comment, or the list. */
  commentAsk?: DriveCommentAsk | null;
  onCommentAsked?: () => void;
}) {
  const { space, actions, go, leaveEditor, ask, host } = useDriveContext();
  // The same read the page holds for this file, so a Word text can ask it again.
  const entryQuery = useDriveEntry(space, path);
  const { session } = editor;
  const [htmlSource, setHtmlSource] = useState(false);
  const [comparing, setComparing] = useState<string | null>(null);
  // The body a viewer failed on (a PDF or workbook it could not parse, a page framed with no bridge), by its bytes.
  const [failedBody, setFailedBody] = useState<string | null>(null);

  const state = memoState(entry);
  const mark = state.kind === 'memo' ? state.mark : null;
  const versions = versionsQuery.data?.versions;
  const kept = versions?.map((v) => v.seq);
  const { view, version, against } = fileView;
  const shownDocument = useShownDocument(space, entry, fileView, versionsQuery.data, head);
  const { docVersion, viewed, shownEntry, body: shown, mode, bodyKey } = shownDocument;
  const isText = !!shownEntry?.is_text && mode !== 'none';
  const isHtml = mode === 'html';

  const comments = useDriveFileComments({
    entry,
    scope: commentScope({ space, entry, view, shown: shownDocument, editing: !!session }),
    versions,
    ask: commentAsk,
    onAsked: () => onCommentAsked?.(),
    onSeeVersion: (seq, commentId) => go({ file: path, version: seq, ask: { comment: commentId } }),
    host,
  });

  // A comparison with theirs belongs to the conflict it was opened on.
  const conflictKey = session ? `${session.sitting}:${session.conflict}` : null;
  const compareTheirs = !!conflictKey && comparing === conflictKey;

  const headSeq = entry?.version ?? null;
  /** Every move between views and versions, written as the address reads it. */
  const goTo = (next: { view?: MemoView; version?: number | null; against?: number | null }) => {
    const nextView = next.view ?? view;
    const to = resolveFileView(entry, { tab: nextView === 'document' ? null : nextView, version: next.version ?? null, against: next.against ?? null }, false, kept);
    const move = () => go({ file: path, ...fileAddress(to) });
    if (session) leaveEditor(move);
    else move();
  };
  const pickVersion = (v: DriveVersion) => {
    // A memo's version opens in Changes when its text can be compared (the
    // server says, per version); any other opens as it was.
    goTo({ view: mark && v.text !== 'none' ? 'changes' : 'document', version: v.seq });
  };

  const restore = async () => {
    if (!entry || !viewed) return;
    if (await actions.restoreVersion(entry, viewed.seq)) goTo({ view: 'document', version: null });
  };

  const name = entry?.name ?? nameOf(path);

  const documentView = ((): BodyView => {
    // An open editor keeps its text on screen even when the file goes from under it.
    if (session && entry) {
      return {
        kind: 'editor',
        name: entry.name,
        draft: session.draft,
        original: editor.theirs?.content ?? session.base.content,
        compare: compareTheirs,
        modelPath: session.modelPath,
        editorRef: editor.editorRef,
        onChange: editor.change,
      };
    }
    if (entryError) return { kind: 'failure', name, gone: entryError === 'gone', onRetry };
    if (!entry || !space) return { kind: 'loading' };
    // A version the URL names that the listing cannot show is never stood in for by the head.
    if (docVersion !== null && !viewed) {
      if (versionsQuery.error) return { kind: 'failure', name, gone: false, onRetry: () => void versionsQuery.refetch() };
      if (versionsQuery.isPending) return { kind: 'loading' };
      return { kind: 'versionGone', onBack: () => goTo({ view: 'document', version: null }) };
    }
    // A Word file has no viewer here: it reads as the text the server takes out
    // of it, whatever its name would otherwise open it as (a renamed report).
    if (shownEntry?.sha256 && showsExtract(entry, viewed) && entry.version) {
      // What names the bytes it reads: the versions for a past one, the entry for the head.
      const source = viewed ? versionsQuery : entryQuery;
      return { kind: 'extract', entry, seq: viewed?.seq ?? entry.version, sha: shownEntry.sha256, source };
    }
    const content = shown.data?.content;
    if (isHtml && content != null) {
      return {
        kind: 'html',
        name: entry.name,
        content,
        source: htmlSource,
        page: shownEntry?.sha256 ? { entryId: entry.id, sha: shownEntry.sha256, seq: viewed?.seq ?? null, at: viewed?.seq ?? entry.version } : null,
        comments: comments.target,
        onDocumentFailed: (failed) => setFailedBody(failed ? bodyKey : null),
      };
    }
    return {
      kind: 'document',
      entry,
      body: shown.data ?? null,
      loading: mode !== 'none' && shown.isPending,
      // Not read at all, or read as none: a download card that says why.
      error: mode === 'none' || shown.data?.mode === 'none'
        ? { category: noneCategory(shownEntry ?? entry) }
        : bodyError(shown.error),
      onRetry: () => void shown.refetch(),
      seq: viewed?.seq ?? null,
      comments: comments.target,
      onDocumentFailed: (failed) => setFailedBody(failed ? bodyKey : null),
    };
  })();

  let main: React.ReactNode;
  let fill = false;
  if (view === 'document' || !entry) {
    main = <FileBody view={documentView} imageLoader={imageLoader} onOpenLink={onOpenLink} />;
    fill = !!session || (isHtml && !htmlSource);
  } else if (view === 'record') {
    main = <MemoRecord entry={entry} onPick={pickVersion} />;
  } else if (view === 'changes') {
    main = <MemoChanges entry={entry} version={version} against={against} onPick={(v, vs) => goTo({ view: 'changes', version: v, against: vs })} />;
    fill = true;
  } else {
    main = <MemoAuthors entry={entry} onPick={(seq) => goTo({ view: 'changes', version: seq })} />;
  }

  const editable = view === 'document' && isText && docVersion === null && !head.isError;
  return (
    <section
      className="drive-file"
      aria-label={name}
      onKeyDown={(e) => {
        if (session && (e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 's') {
          e.preventDefault();
          void editor.save();
        }
      }}
    >
      {/* One surface for the header's comment tools and the body's layer: they read the same comments. */}
      <CommentSurfaceContext value={comments.surface}>
        <FileHeader
          path={path}
          entry={entry}
          mobile={mobile}
          editor={editor}
          edit={!editable ? 'none' : head.data?.version && !head.isPlaceholderData ? 'ready' : 'waiting'}
          html={view === 'document' && isHtml && docVersion === null ? { source: htmlSource, toggle: () => setHtmlSource((s) => !s) } : null}
          // The tools show only over a body the comment layer draws on.
          comments={bodyTakesComments(documentView, failedBody !== null && failedBody === bodyKey) ? comments.tools : null}
        >
          {mark && entry && (
            <MemoHeader
              mark={mark}
              head={headSeq}
              // Who wrote what reads the head's own text, as the server's attribution does.
              isText={versions?.find((v) => v.seq === headSeq)?.text === 'text'}
              view={view}
              onView={(next) => goTo({ view: next, version: next === 'document' || next === 'changes' ? version : null })}
              onEdit={() => ask.memo(entry, 'edit')}
              onViewFinal={() => goTo({ view: 'document', version: mark.final_seq })}
              onMakeFinal={() => void actions.setMemo(entry, { status: 'final', ifMatch: entry.sha256 })}
              busy={actions.pending.setMemo}
            />
          )}
        </FileHeader>
        <FileBars
          entry={entry}
          view={view}
          viewed={viewed}
          editor={editor}
          compare={{ on: compareTheirs, toggle: () => setComparing(compareTheirs ? null : conflictKey) }}
          onRestore={() => void restore()}
          onBackToDocument={() => goTo({ view: 'document', version: null })}
        />
        <div className="drive-file-body">
          <div className={cn('drive-file-main', fill && 'is-fill')}>
            {/* A new view fades in over the one it replaces rather than cutting to it. */}
            <div key={view} className="drive-file-view">{main}</div>
          </div>
        </div>
      </CommentSurfaceContext>
    </section>
  );
}
