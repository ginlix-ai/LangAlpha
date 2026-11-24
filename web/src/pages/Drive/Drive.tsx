import { useEffect, useState } from 'react';
import { Navigate, useNavigate, useSearchParams } from 'react-router';
import { useTranslation } from 'react-i18next';
import { objectUrlAt } from '@/api/drive';
import { apiErrorStatus } from '@/api/errors';
import { spaceRef, useDriveChanges, useDriveEntry, useDriveList, useDriveSpaces } from '@/hooks/useDrive';
import { useIsMobile } from '@/hooks/useIsMobile';
import { cn } from '@/lib/utils';
import { driveHref, readDriveLocation, type DriveLocation } from '@/lib/driveLinks';
import ConfirmDialog from '@/pages/Dashboard/components/ConfirmDialog';
import { drivePathHref } from '@/pages/ChatAgent/utils/agentArtifactRouting';
import { classifyAgentPath } from '@/pages/ChatAgent/utils/agentPaths';
import type { OpenFileHandler } from '@/pages/ChatAgent/utils/fileLocation';
import type { DriveEntry } from '@/types/drive';
import { DriveFilePane } from './components/DriveFilePane';
import { DriveList } from './components/DriveList';
import { DriveTrash } from './components/DriveTrash';
import { UploadTray } from './components/UploadTray';
import { DriveProvider } from './DriveProvider';
import { useDriveBody } from './driveBody';
import { driveLinkPath, followMove, isWithinKey, linkedFile, parentOf } from './drivePaths';
import { useDriveActions } from './useDriveActions';
import { useDriveEditor } from './useDriveEditor';
import { useDriveUpload } from './useDriveUpload';
import './Drive.css';

function readError(err: unknown): 'gone' | 'failed' | null {
  if (!err) return null;
  return apiErrorStatus(err) === 404 ? 'gone' : 'failed';
}

/**
 * /drive: the person's window onto the files their agent works in. A folder
 * on the left (the whole width until a file opens), the open file on the
 * right with its history beside it; on a phone one at a time, with back. The
 * folder and file in view are the URL, so a link from a chat lands here. The
 * space's change feed keeps it current while it is in view, the agent's
 * writes included.
 */
function Drive() {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const [params] = useSearchParams();
  const mobile = useIsMobile();
  const loc = readDriveLocation(params);
  const file = loc.file;
  const dir = file ? parentOf(file) : loc.dir;

  const spaces = useDriveSpaces();
  const current = spaces.data?.find((s) => (loc.space ? s.id === loc.space || s.kind === loc.space : s.kind === 'personal'))
    ?? (loc.space ? undefined : spaces.data?.[0]);
  const space = current ? spaceRef(current) : null;
  const usage = current?.usage ?? null;
  const hrefFor = (next: DriveLocation) => driveHref({ space: current && current.kind !== 'personal' ? current.id : null, ...next });

  const list = useDriveList(space, dir);
  const entry = useDriveEntry(space, file);
  const fileEntry = entry.data?.kind === 'file' ? entry.data : undefined;
  const head = useDriveBody(space, fileEntry ?? null, null);
  const editor = useDriveEditor(space, file, fileEntry, head, loc.version);

  // Leaving unsaved edits asks first; the answer runs what was asked for.
  const [leaving, setLeaving] = useState<{ then?: () => void } | null>(null);
  const leaveEditor = (then?: () => void) => {
    if (editor.dirty) {
      setLeaving({ then });
      return;
    }
    editor.close();
    then?.();
  };
  const go = (next: DriveLocation, opts: { replace?: boolean } = {}) => {
    const href = hrefFor(next);
    if (editor.session && next.file !== file) leaveEditor(() => navigate(href, opts));
    else navigate(href, opts);
  };

  // A tab closed or reloaded with unsaved edits asks the browser's own question.
  useEffect(() => {
    if (!editor.dirty) return;
    const onBeforeUnload = (e: BeforeUnloadEvent) => e.preventDefault();
    window.addEventListener('beforeunload', onBeforeUnload);
    return () => window.removeEventListener('beforeunload', onBeforeUnload);
  }, [editor.dirty]);

  const onMoved = (from: string, to: string) => {
    // Matched as the server matches names, since the URL may spell them otherwise.
    const moved = followMove(file ?? dir, from, to);
    if (moved === null) return;
    navigate(hrefFor(file ? { file: moved, history: loc.history } : { dir: moved }), { replace: true });
  };
  const actions = useDriveActions(space, usage, onMoved);
  const uploads = useDriveUpload({ space, usage });

  // The open file renamed or moved by the agent: follow it, unless an edit is open.
  // A resync names no entries: the refetch it causes settles the open file,
  // and a file gone with it answers 404, which the pane shows.
  useDriveChanges(space, current?.change_seq ?? null, (changed) => {
    const open = entry.data;
    if (!open || editor.session) return;
    const now = changed.find((e) => e.id === open.id);
    if (now && !now.deleted_at && now.path !== open.path) {
      navigate(hrefFor({ file: now.path, history: loc.history }), { replace: true });
    }
  });

  const trash = async (target: DriveEntry) => {
    if (!(await actions.trash(target))) return;
    // The URL may spell the open file in another case than the server does.
    if (file && isWithinKey(file, target.path)) {
      editor.close();
      navigate(hrefFor({ dir: parentOf(target.path) }), { replace: true });
    }
  };

  const imageLoader = (path: string) => {
    if (!space) return Promise.reject(new Error('no space'));
    const at = linkedFile(space, dir, path);
    return objectUrlAt(at.space, at.path);
  };

  const onOpenLink: OpenFileHandler = (href, _workspaceId, _location, opts) => {
    const info = classifyAgentPath(href);
    if (info.kind === 'drive') {
      navigate(drivePathHref(info));
      return;
    }
    const target = driveLinkPath(dir, href, opts?.rooted);
    go(href.endsWith('/') ? { dir: target } : { file: target });
  };

  // A space the person no longer has: their own Drive, from its root.
  if (loc.space && spaces.data && !current) return <Navigate replace to={driveHref({})} />;
  // A file link that names a folder opens the folder.
  if (file && entry.data?.kind === 'folder') return <Navigate replace to={hrefFor({ dir: file })} />;

  const showFile = !loc.trash && !!file;
  // On a phone the file takes the page only once it can show: until then the
  // folder side shows the spaces loading, or failing to, with its retry.
  const fileTakesPage = mobile ? showFile && !!space : showFile;
  return (
    <DriveProvider
      space={space}
      actions={actions}
      go={go}
      leaveEditor={leaveEditor}
      trash={(target) => void trash(target)}
      open={(target) => go(target.kind === 'folder' ? { dir: target.path } : { file: target.path, history: loc.history })}
    >
      <div className="drive-page">
        <div className="chrome-drag-strip" aria-hidden="true" />
        <div className={cn('drive-shell', fileTakesPage && 'has-file')}>
          {loc.trash ? (
            <DriveTrash
              trashDays={current?.trash_days ?? 14}
              trashBytes={usage?.trash_bytes ?? null}
              onRestored={(path, kind) => go({ dir: kind === 'folder' ? path : parentOf(path) })}
            />
          ) : (!mobile || !fileTakesPage) && (
            <DriveList
              dir={dir}
              entries={list.data}
              loading={spaces.isPending || list.isPending}
              error={spaces.error ? 'failed' : readError(list.error)}
              openPath={file}
              compact={showFile}
              usage={usage}
              upload={(files, into, folders) => void uploads.upload(files, into, folders)}
              onRetry={() => void (spaces.error ? spaces.refetch() : list.refetch())}
              tray={<UploadTray
                  items={uploads.items}
                  usage={usage}
                  onCancel={uploads.cancel}
                  onRetry={uploads.retry}
                  onResume={uploads.resume}
                  onDiscard={uploads.discard}
                  onDismiss={uploads.dismiss}
                />}
            />
          )}
          {showFile && space && (
            <DriveFilePane
              key={file}
              path={file}
              entry={fileEntry}
              entryError={readError(entry.error)}
              onRetry={() => void entry.refetch()}
              version={loc.version}
              showHistory={loc.history}
              mobile={mobile}
              editor={editor}
              head={head}
              imageLoader={imageLoader}
              onOpenLink={onOpenLink}
            />
          )}
        </div>
      </div>
      <ConfirmDialog
        open={leaving !== null}
        title={t('drive:edit.discardConfirm')}
        message={t('drive:edit.leaveBody')}
        confirmLabel={t('drive:edit.discard')}
        surface="page"
        onConfirm={() => {
          const then = leaving?.then;
          editor.close();
          then?.();
        }}
        onOpenChange={(open) => {
          if (!open) setLeaving(null);
        }}
      />
    </DriveProvider>
  );
}

export default Drive;
