import type React from 'react';
import { useEffect, useEffectEvent, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { objectUrlAt } from '@/api/drive';
import { apiErrorStatus } from '@/api/errors';
import { useDriveChanges, useDriveEntryById, useDriveList } from '@/hooks/useDrive';
import { usePageTabLeaveGuard } from '@/components/PageTabs/usePageTabLeaveGuard';
import { cn } from '@/lib/utils';
import { useLatestRef } from '@/hooks/useLatestRef';
import { withFile, type DriveLocation, type ReadLocation } from '@/lib/driveLinks';
import ConfirmDialog from '@/pages/Dashboard/components/ConfirmDialog';
import { LinkActionsContext, useLinkActions, type LinkActions } from '@/pages/ChatAgent/contexts/LinkActionsContext';
import { driveLocationOf, driveRouteLocation } from '@/pages/ChatAgent/utils/agentArtifactRouting';
import { classifyAgentPath } from '@/pages/ChatAgent/utils/agentPaths';
import type { OpenFileHandler } from '@/pages/ChatAgent/utils/fileLocation';
import type { DriveEntry } from '@/types/drive';
import { DriveFilePane } from './components/DriveFilePane';
import { DriveList } from './components/DriveList';
import { MemosView } from './components/MemosView';
import { DriveTrash } from './components/DriveTrash';
import { ListDivider } from './components/ListDivider';
import { UploadTray } from './components/UploadTray';
import { DriveProvider } from './DriveProvider';
import type { DriveHost } from './driveContext';
import { useListWidth } from './listWidth';
import { driveLinkPath, drivePathKey, followMove, isWithinKey, linkedFile, parentOf } from './drivePaths';
import { fileAddress } from './memoMarks';
import { useDriveActions } from './useDriveActions';
import { useDriveSpace, useOpenDriveFile } from './useOpenDriveFile';
import { useEscapeClosesFile } from './useEscapeClosesFile';
import { useDriveUpload } from './useDriveUpload';
import './Drive.css';
// After Drive.css: its memo rules override shared ones of the same weight.
import './Memo.css';

function readError(err: unknown): 'gone' | 'failed' | null {
  if (!err) return null;
  return apiErrorStatus(err) === 404 ? 'gone' : 'failed';
}

export interface DriveViewProps {
  /** Where the view is, as its host keeps it: the page in its address, a panel tab in its state. */
  loc: ReadLocation;
  /**
   * Takes the view to another place, every part of it named (`space` is the
   * address's: a space's id, null for the person's own). `replace` corrects
   * where the view is rather than moving it, and adds no step to go back to.
   */
  navigate: (to: DriveLocation, opts: { replace?: boolean }) => void;
  host: DriveHost;
  /** One pane at a time, the list or the file, as on a phone or in a narrow panel. */
  single: boolean;
  /** An action the host adds to each pane's header, such as leaving the panel for the Drive page. */
  pageAction?: React.ReactNode;
  /** The address reads as the file has it but is spelled otherwise (an old history link): write it anew. */
  stale?: boolean;
  /**
   * The open file's id where the host keeps one, and where it is told the
   * id once the file is read: a file gone from its path is then looked up
   * by id, so a move made while the view was out of sight is followed.
   */
  entryId?: string;
  onTrack?: (entryId: string) => void;
  /** The person started editing the open file. */
  onEditing?: () => void;
}

/**
 * The person's Drive: a folder, the trash or every memo, and the open file
 * beside it, or in place of it when `single`. The page and the chat panel's
 * Drive tab both show it, each keeping the place itself (`loc`) and moving
 * it (`navigate`). The space's change feed keeps it current while it is in
 * view, the agent's writes included.
 */
export function DriveView({ loc, navigate, host, single, pageAction, stale = false, entryId, onTrack, onEditing }: DriveViewProps) {
  const { t } = useTranslation();
  const file = loc.file;
  const dir = file ? parentOf(file) : loc.dir;

  const { spaces, current, space, usage, missing } = useDriveSpace(loc.space);
  /**
   * The place `next` names, filled in: the space this view is in unless it
   * names another, and beside the memo list, a file opened from it, or moved
   * while open, keeps the list in view.
   */
  const placeOf = (next: DriveLocation): DriveLocation => ({
    ...next,
    space: next.space !== undefined ? next.space : current && current.kind !== 'personal' ? current.id : null,
    memos: next.memos ?? (!!next.file && loc.memos),
  });
  const correct = (next: DriveLocation) => navigate(placeOf(next), { replace: true });

  const list = useDriveList(space, dir);
  // The file as its folder's listing has it, matched as the server matches names.
  // Only a listing still fresh: the change feed marks one stale when a file in
  // it changes, and a stale seed would read and offer to edit an old version.
  const listed = file && !list.isStale ? list.data?.find((e) => drivePathKey(e.path) === drivePathKey(file)) : undefined;
  const { entry, fileEntry, versions, shown, head, editor, fileView, settled } = useOpenDriveFile(space, file, loc, listed);

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
  const go = (to: DriveLocation, opts: { replace?: boolean } = {}) => {
    const next = placeOf(to);
    const land = () => navigate(next, opts);
    // The same path in another space is another file.
    if (editor.session && (next.file !== file || next.space !== placeOf({}).space)) leaveEditor(land);
    else land();
  };

  // On the page, a page tab switched or closed asks the same, since a route
  // change fires no beforeunload. In the chat's panel nothing asks: a draft
  // is kept by its file (driveDrafts) and reopens with it, short of one past
  // the store's size bound, which only the window's close asks about.
  usePageTabLeaveGuard(host === 'page' && editor.dirty ? (then) => leaveEditor(then) : null);

  // Working in a file keeps it where the host keeps its places.
  const editing = !!editor.session;
  const startedEditing = useEffectEvent(() => onEditing?.());
  useEffect(() => {
    if (editing) startedEditing();
  }, [editing]);

  // On the page, Escape puts an open file away, as it closes any other panel.
  // Not while editing (the editor keeps its own keys), nor just after an
  // Escape the comment layer took (`useEscapeClosesFile`). In the chat's
  // panel Escape is the chat's.
  const closeFile = () => go(loc.memos ? { memos: true } : { dir });
  const fileOpen = !loc.trash && !!file;
  useEscapeClosesFile(host === 'page' && fileOpen && !editor.session, closeFile);

  // Where the view is now, for what settles after a request: a move or a
  // trash follows the place it changed only while the view still shows it.
  const here = useLatestRef({ file, dir, loc });
  const onMoved = (from: string, to: string) => {
    const now = here.current;
    // Matched as the server matches names, since the address may spell them otherwise.
    const moved = followMove(now.file ?? now.dir, from, to);
    if (moved === null) return;
    // The memo list beside it too, as it is now: `correct` would fill it in
    // from the place the move started on.
    correct(now.file ? { ...withFile(now.loc, moved), memos: now.loc.memos } : { dir: moved });
  };
  const actions = useDriveActions(space, usage, onMoved);
  const uploads = useDriveUpload({ space, usage });
  const listWidth = useListWidth();

  // The open file renamed or moved by the agent: follow it, unless an edit is open.
  // Where the host keeps the file's id, that file, whatever its path holds now.
  // A resync names no entries: the refetch it causes settles the open file,
  // and a file gone with it answers 404, which the pane shows.
  useDriveChanges(space, current?.change_seq ?? null, (changed) => {
    const open = entry.data;
    const id = entryId ?? open?.id;
    if (!file || !id || editor.session) return;
    const now = changed.find((e) => e.id === id);
    const at = open?.id === id ? open.path : file;
    if (now && !now.deleted_at && now.path !== at) correct(withFile(loc, now.path));
  });

  // The feed is read only while the view is on screen, so where the host
  // keeps the file's id, a file moved meanwhile is found again by it: when
  // its path is gone, or holds another file or a folder now. The id moves to
  // the file in view only once the one it names is gone for good.
  const fileId = fileEntry?.id;
  const gone = !!file && apiErrorStatus(entry.error) === 404;
  const pathId = file ? entry.data?.id : undefined;
  const swapped = !!pathId && !!entryId && pathId !== entryId;
  const found = useDriveEntryById(space, (gone || swapped) && entryId ? entryId : null);
  // Only this lookup's own answer moves the view: a cached one from an
  // earlier lookup may name where the file was before it moved again, and a
  // refetch that failed still hands back the answer it was meant to replace.
  const settledFound = found.isSuccess && !found.isFetching ? found.data : undefined;
  const movedTo = settledFound && !settledFound.deleted_at && settledFound.path !== file ? settledFound.path : null;
  // Gone means the server says so, once the lookup has settled; one that
  // failed for another reason is shown as a failure to retry, and keeps the id.
  const lookupDone = found.isError && !found.isFetching;
  const lookupFailed = lookupDone && apiErrorStatus(found.error) !== 404;
  const trackedGone = (lookupDone && !lookupFailed) || !!settledFound?.deleted_at;
  // The lookup can find the file back at the path the read last saw holding
  // another entry; that read is stale, so it is read again, once per answer.
  const backHere = swapped && !!settledFound && !settledFound.deleted_at && !movedTo;
  const rereadPath = useEffectEvent(() => void entry.refetch());
  useEffect(() => {
    if (backHere) rereadPath();
  }, [backHere, found.dataUpdatedAt]);
  const track = useEffectEvent((id: string) => onTrack?.(id));
  useEffect(() => {
    if (fileId && (!entryId || (fileId !== entryId && trackedGone))) track(fileId);
  }, [fileId, entryId, trackedGone]);
  // Not under an open editor, which the feed above does not follow either.
  const follow = useEffectEvent((path: string) => correct(withFile(loc, path)));
  useEffect(() => {
    if (movedTo && !editing) follow(movedTo);
  }, [movedTo, editing]);
  // Gone from its path but still being looked for by id: not gone yet.
  const seeking = gone && !!entryId && (found.isPending || found.isFetching || !!movedTo);
  // Another file at the path is not the tracked one until the lookup says
  // which the tab holds: while it runs, and while it fails, when the pane
  // offers it again. Until then it is not shown, offered for editing, or read
  // to correct the address (its view is not the tracked file's).
  const swapOpen = swapped && !trackedGone;
  const resolvingSwap = swapOpen && !editing;

  const trash = async (target: DriveEntry) => {
    if (!(await actions.trash(target))) return;
    // Only if the view still shows what it showed when asked: a move made
    // while the trash was under way stays where the reader took it. The
    // address may spell the open file in another case than the server does.
    if (file && here.current.file === file && isWithinKey(file, target.path)) {
      editor.close();
      correct({ dir: parentOf(target.path) });
    }
  };

  const imageLoader = (path: string) => {
    if (!space) return Promise.reject(new Error('no space'));
    const at = linkedFile(space, dir, path);
    return objectUrlAt(at.space, at.path);
  };

  // In a panel a Drive page link in an open document moves this tab, as a
  // Drive path does, rather than opening the place in another one or leaving
  // for the page; any other route is the host's to open.
  const outerLinks = useLinkActions();
  const links: LinkActions = host === 'panel'
    ? {
        ...outerLinks,
        openAppRoute: (route) => {
          const to = driveRouteLocation(route);
          if (!to) return outerLinks.openAppRoute?.(route) ?? false;
          go(to);
          return true;
        },
      }
    : outerLinks;

  const onOpenLink: OpenFileHandler = (href, _workspaceId, _location, opts) => {
    const info = classifyAgentPath(href);
    if (info.kind === 'drive') {
      go(driveLocationOf(info));
      return;
    }
    const target = driveLinkPath(dir, href, opts?.rooted);
    go(href.endsWith('/') ? { dir: target } : { file: target });
  };

  // Where the view stands corrected, written as a step that replaces this
  // one: a space the person no longer has is their own Drive from its root,
  // a file link that names a folder opens the folder, and an address the
  // file reads otherwise (or an old history link) is written as it reads,
  // once the file and its versions are as the server has them. A space named
  // otherwise than its address spells it (`?space=personal`) is written as
  // spelled, so one place has one address and one tab.
  // A folder that took the path of the file a tab keeps is that only once the file is gone.
  const isFolder = !!file && entry.data?.kind === 'folder' && !swapOpen;
  const canonical = fileAddress(shown);
  const differs = canonical.tab !== loc.tab || canonical.version !== loc.version || canonical.against !== loc.against;
  const respelled = !!current && placeOf({}).space !== loc.space;
  const correction: DriveLocation | null = missing ? { space: null }
    : isFolder ? { dir: file }
    : file && fileEntry && settled && !swapOpen && (differs || stale) ? { file, ...canonical, ask: loc.ask }
    : respelled ? { ...loc, space: undefined }
    : null;
  const correctionKey = correction ? JSON.stringify(placeOf(correction)) : null;
  const applyCorrection = useEffectEvent(() => {
    if (correction) correct(correction);
  });
  useEffect(() => {
    if (correctionKey) applyCorrection();
  }, [correctionKey]);
  if (missing || isFolder) return null;

  const showFile = !loc.trash && !!file;
  // One pane at a time, the file takes the view only once it can show: until
  // then the folder side shows the spaces loading, or failing to, with its retry.
  const fileTakesPage = single ? showFile && !!space : showFile;
  // A list beside a file, or alone in a narrow view, is narrow: one column, its actions as icons.
  const compact = showFile || single;
  const entryError = seeking ? null : lookupFailed ? 'failed' : entry.error ? readError(entry.error) : null;
  return (
    <LinkActionsContext value={links}>
      <DriveProvider
        space={space}
        actions={actions}
        go={go}
        leaveEditor={leaveEditor}
        backTo={loc.memos ? 'memos' : 'folder'}
        back={closeFile}
        trash={(target) => void trash(target)}
        open={(target) => go(target.kind === 'folder' ? { dir: target.path } : { file: target.path })}
        host={host}
        pageAction={pageAction}
        fileCrumbs={host === 'panel' && single}
      >
        <div className={cn('drive-page', host === 'panel' && 'is-panel')}>
          {host === 'page' && <div className="chrome-drag-strip" aria-hidden="true" />}
          <div
            className={cn('drive-shell', fileTakesPage && 'has-file', single && 'is-single')}
            style={{ '--drive-list-width': `${listWidth.width}px` } as React.CSSProperties}
          >
            {loc.trash ? (
              <DriveTrash
                trashDays={current?.trash_days ?? 14}
                trashBytes={usage?.trash_bytes ?? null}
                onRestored={(path, kind) => go({ dir: kind === 'folder' ? path : parentOf(path) })}
              />
            ) : (!single || !fileTakesPage) && loc.memos ? (
              <MemosView openPath={file} compact={compact} />
            ) : (!single || !fileTakesPage) && (
              <DriveList
                dir={dir}
                entries={list.data}
                loading={spaces.isPending || list.isPending}
                error={spaces.error ? 'failed' : readError(list.error)}
                openPath={file}
                compact={compact}
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
            {fileTakesPage && !single && <ListDivider width={listWidth.width} onChange={listWidth.change} />}
            {showFile && space && (
              <DriveFilePane
                key={file}
                path={file}
                entry={resolvingSwap ? undefined : fileEntry}
                entryError={entryError}
                onRetry={() => void (lookupFailed ? found.refetch() : entry.refetch())}
                fileView={fileView}
                versions={versions}
                mobile={single}
                editor={editor}
                head={head}
                imageLoader={imageLoader}
                onOpenLink={onOpenLink}
                commentAsk={loc.ask}
                // Shown once: the place lets it go, so Back or a reload does not open it again.
                onCommentAsked={() => correct(withFile(loc, file))}
              />
            )}
          </div>
        </div>
        <ConfirmDialog
          open={leaving !== null}
          title={t('drive:edit.discardConfirm')}
          message={t('drive:edit.leaveBody')}
          confirmLabel={t('drive:edit.discard')}
          surface={host === 'page' ? 'page' : undefined}
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
    </LinkActionsContext>
  );
}
