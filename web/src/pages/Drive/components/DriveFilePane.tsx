import type React from 'react';
import { useState } from 'react';
import { useTranslation } from 'react-i18next';
import type { UseQueryResult } from '@tanstack/react-query';
import { RotateCcw } from 'lucide-react';
import { apiErrorStatus } from '@/api/errors';
import { HeaderButton } from '@/components/mcp/McpPrimitives';
import { useLocale } from '@/hooks/useLocale';
import { useNow } from '@/hooks/useNow';
import { relativeTime } from '@/lib/format';
import { cn } from '@/lib/utils';
import type { FileBody as Body } from '@/pages/ChatAgent/components/filePanel/fileBody';
import type { FileError } from '@/pages/ChatAgent/components/filePanel/fileErrors';
import type { OpenFileHandler } from '@/pages/ChatAgent/utils/fileLocation';
import type { DriveEntry, DriveVersion, DriveVersions } from '@/types/drive';
import { useDriveContext } from '../driveContext';
import { atVersion, driveBodyMode, tooLargeToPreview, useDriveBody } from '../driveBody';
import { nameOf } from '../drivePaths';
import { fileAddress, memoState, resolveFileView, type FileView, type MemoView } from '../memoMarks';
import type { DriveEditor } from '../useDriveEditor';
import { whoLabel } from '../who';
import { DriveBar } from './DriveBar';
import { FileBody, type BodyView } from './FileBody';
import { FileHeader } from './FileHeader';
import { MemoAuthors } from './MemoAuthors';
import { MemoChanges } from './MemoChanges';
import { MemoHeader } from './MemoHeader';
import { MemoRecord } from './MemoRecord';
import { UndoSave } from './UndoSave';

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
export function DriveFilePane({ path, entry, entryError, onRetry, fileView, versions: versionsQuery, mobile, editor, head, imageLoader, onOpenLink }: {
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
}) {
  const { t } = useTranslation();
  const locale = useLocale();
  const now = useNow();
  const { space, actions, go, leaveEditor, ask } = useDriveContext();
  const { session } = editor;
  const [htmlSource, setHtmlSource] = useState(false);
  const [comparing, setComparing] = useState<string | null>(null);

  const state = memoState(entry);
  const mark = state.kind === 'memo' ? state.mark : null;
  const versions = versionsQuery.data?.versions;
  const kept = versions?.map((v) => v.seq);
  const { view, version, against } = fileView;
  const docVersion = view === 'document' ? version : null;
  const viewed: DriveVersion | null = docVersion !== null ? versions?.find((v) => v.seq === docVersion) ?? null : null;

  // A past version opens by its own size and kind of bytes.
  const shownEntry = entry ? atVersion(entry, viewed) : null;
  const old = useDriveBody(space, entry && viewed ? shownEntry : null, viewed ? { seq: viewed.seq, sha: viewed.sha256 } : null);
  const shown = viewed ? old : head;
  const mode = shownEntry ? driveBodyMode(shownEntry) : 'none';
  const isText = !!shownEntry?.is_text && mode !== 'none';
  const isHtml = mode === 'html';

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

  const theirsBy = whoLabel(t, entry?.modified_by);
  const bars: React.ReactNode[] = [];
  if (session?.conflict && editor.theirs) {
    bars.push(
      <DriveBar
        key="conflict"
        role="alert"
        actions={(
          <>
            <HeaderButton variant="ghost" onClick={() => setComparing(compareTheirs ? null : conflictKey)}>
              {compareTheirs ? t('drive:conflict.hideCompare') : t('drive:conflict.compare')}
            </HeaderButton>
            <HeaderButton onClick={editor.takeTheirs}>{t('drive:conflict.takeTheirs')}</HeaderButton>
            <HeaderButton variant="primary" onClick={editor.keepMine} disabled={session.save !== 'idle'}>{t('drive:conflict.keepMine')}</HeaderButton>
          </>
        )}
      >
        <strong>{t('drive:conflict.title')}</strong>{' '}{t('drive:conflict.body', { who: theirsBy })}
      </DriveBar>,
    );
  } else if (editor.behind && editor.dirty) {
    bars.push(
      <DriveBar key="behind" role="status" actions={<HeaderButton onClick={editor.review}>{t('drive:edit.review')}</HeaderButton>}>
        {t('drive:edit.changedUnder', { who: theirsBy })}
      </DriveBar>,
    );
  }
  if (!session && viewed && view === 'document') {
    bars.push(
      <DriveBar
        key="version"
        actions={(
          <>
            <HeaderButton icon={RotateCcw} onClick={() => void restore()} disabled={actions.pending.restoreVersion} title={t('drive:versions.restoreHint')}>
              {t('drive:actions.restore')}
            </HeaderButton>
            <HeaderButton variant="ghost" onClick={() => goTo({ view: 'document', version: null })}>{t('drive:versions.backToCurrent')}</HeaderButton>
          </>
        )}
      >
        {t('drive:versions.viewing', { seq: viewed.seq, when: relativeTime(viewed.created_at, locale, now), who: whoLabel(t, viewed.actor) })}
      </DriveBar>,
    );
  }
  if (!session && entry && state.kind !== 'memo' && view === 'document') bars.push(<UndoSave key="undo" entry={entry} />);
  if (entry && state.kind === 'former' && view === 'record') {
    bars.push(
      <DriveBar key="was-memo" role="status" actions={<HeaderButton variant="ghost" onClick={() => goTo({ view: 'document' })}>{t('drive:record.backToFile')}</HeaderButton>}>
        {t('drive:record.wasMemo')}
      </DriveBar>,
    );
  }

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
    const content = shown.data?.content;
    if (isHtml && content != null) return { kind: 'html', name: entry.name, content, source: htmlSource };
    return {
      kind: 'document',
      entry,
      body: shown.data ?? null,
      loading: mode !== 'none' && shown.isPending,
      // A body read as none is a PDF past the bound that could not be read by range.
      error: mode === 'none'
        ? { category: tooLargeToPreview(shownEntry ?? entry) ? 'too_large' : 'binary_file' }
        : shown.data?.mode === 'none' ? { category: 'too_large' } : bodyError(shown.error),
      onRetry: () => void shown.refetch(),
      seq: viewed?.seq ?? null,
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
      <FileHeader
        path={path}
        entry={entry}
        mobile={mobile}
        editor={editor}
        canEdit={view === 'document' && isText && docVersion === null && !!head.data?.version}
        html={view === 'document' && isHtml && docVersion === null ? { source: htmlSource, toggle: () => setHtmlSource((s) => !s) } : null}
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
      {bars}
      <div className="drive-file-body">
        <div className={cn('drive-file-main', fill && 'is-fill')}>{main}</div>
      </div>
    </section>
  );
}
