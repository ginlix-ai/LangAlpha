import React, { useCallback, useEffect, useId, useMemo, useRef, useState, Suspense } from 'react';
import { TextSelect, Upload } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import {
  memoMimeForName,
  useAddToMemo,
  useWorkspaceMemoIndex,
  useMemoStaleCheck,
  MemoStaleBanner,
  MemoDiffModal,
} from './FilePanelMemo';
import { useWorkspace } from '@/hooks/useWorkspace';
import { useIsMobile } from '@/hooks/useIsMobile';
import { useNarrowContainer } from '@/hooks/useNarrowContainer';
import { SandboxSettingsContent } from './SandboxSettingsPanel';
import {
  readWorkspaceFile, readWorkspaceFileFull, writeWorkspaceFile, downloadWorkspaceFileAsArrayBuffer,
  triggerFileDownload, resolveWorkspaceFile,
} from '../utils/api';
import { linkCandidates } from '../utils/fileRefResolver';
import type { WriteEvent } from '../utils/fileRefResolver';
import { classifyAgentPath, parseAgentPath } from '../utils/agentPaths';
import { useStableHandler } from '@/hooks/useStableHandler';
import { parseFragment, type FileLocation, type OpenFileHandler } from '../utils/fileLocation';
import FileHeaderActions from './FileHeaderActions';
import './FilePanel.css';

const ExportPreviewModal = React.lazy(() => import('./ExportPreviewModal'));

import type { ApiAdapter, ChartTabSpec, ContextPayload, PanelTarget } from './filePanel/types';
import { EDITABLE_EXTENSIONS, getFileExtension, viewerFor } from './filePanel/fileMeta';
import { useFileUpload } from './filePanel/useFileUpload';
import { useFileEdit } from './filePanel/useFileEdit';
import { useSelectionContext } from './filePanel/useSelectionContext';
import { useFileSelection } from './filePanel/useFileSelection';
import { useFileBackup } from './filePanel/useFileBackup';
import { useFileFocus } from './filePanel/useFileFocus';
import { FocusChip } from './filePanel/FocusChip';
import { useFileTabs, lastChartSymbol } from './filePanel/useFileTabs';
import { useTreeFilter } from './filePanel/useTreeFilter';
import { useTreeInteraction } from './filePanel/useTreeInteraction';
import { useFileRefOpen } from './filePanel/useFileRefOpen';
import { PanelNotices } from './filePanel/PanelNotices';
import { FileContextMenu, type FileMenuAction } from './filePanel/FileContextMenu';
import { useFileDownloads } from './filePanel/useFileDownloads';
import { useFileBodyCache, useFileBody } from './filePanel/useFileBody';
import { useChangedFiles } from './filePanel/useChangedFiles';
import { countLines } from '../utils/fileLocation';
import { TabStrip } from './filePanel/TabStrip';
import { AnimatePresence } from 'framer-motion';
import { TreeColumn } from './filePanel/TreeColumn';
import { FileCrumbs } from './filePanel/FileCrumbs';
import { FileViewer } from './filePanel/FileViewer';
import { EmptyTab } from './filePanel/EmptyTab';
import { PreviewCrumbs } from './filePanel/PreviewCrumbs';
import { PreviewPanes } from './filePanel/PreviewPanes';
import { ChartTab } from './filePanel/ChartTab';
import { usePreviews } from './filePanel/usePreviews';

/** Below this the tree cannot be a column without starving the viewer. */
const TREE_OVERLAY_WIDTH = 720;

interface FilePanelProps {
  workspaceId: string;
  /** Scopes the tab strip; absent for a chat that has not sent its first message, or a share. */
  threadId?: string | null;
  onClose: () => void;
  /** What the chat asked the panel to show. Only `file`, `preview` and `chart`
   *  concern this panel; a `file` target's `dir` stays on as the tree's scope
   *  until `onTargetHandled` clears it. */
  target?: PanelTarget | null;
  onTargetHandled?: () => void;
  /** Leaves the panel for the full MarketView page on this symbol. */
  onOpenInMarketView?: ((spec: ChartTabSpec) => void) | null;
  /** Opens a reference to another workspace (a `__wsref__` link inside a viewed file). */
  onOpenFile?: OpenFileHandler | null;
  /** This thread's Write/Edit paths, newest first, for resolving a reference by name. */
  getRecentWritePaths?: (() => string[]) | null;
  /** Every Write/Edit in the thread, newest first; what marks an open tab changed. */
  getWriteLog?: (() => WriteEvent[]) | null;
  files?: string[];
  filesLoading?: boolean;
  filesError?: string | null;
  onRefreshFiles?: () => void;
  readOnly?: boolean;
  /** Whether this viewer may save a file's bytes. A copy-link share grants
   *  `allow_files` without `allow_download`, and the download endpoint refuses
   *  what `allow_files` alone opened, so an offered save fails after the click. */
  canDownload?: boolean;
  /** Lock to one file: no tree, and closing the last tab closes the panel. */
  singleFileMode?: boolean;
  /** False keeps the strip in memory only: a panel browsing beside a gallery
   *  must not write over the strip the workspace's conversations seed from. */
  persistTabs?: boolean;
  apiAdapter?: ApiAdapter | null;
  onAddContext?: ((ctx: ContextPayload) => void) | null;
  showSystemFiles?: boolean;
  onToggleSystemFiles?: (() => void) | null;
  /** Hide the panel-close affordances when FilePanel is embedded inside a
   * tabbed wrapper that owns the close button. */
  hideClose?: boolean;
  /** Whether any open tab holds an unsaved edit. A wrapper that owns the close
   *  button reads this to ask before it unmounts the panel. */
  onDirtyChange?: ((dirty: boolean) => void) | null;
  onSwitchToMemoTab?: (() => void) | null;
  /** Copy a shareable link to an HTML report (authenticated app only). */
  onCopyShareLink?: ((filePath: string) => void) | null;
}

function FilePanel({
  workspaceId,
  threadId = null,
  onClose,
  target = null,
  onTargetHandled,
  onOpenInMarketView = null,
  onOpenFile = null,
  getRecentWritePaths = null,
  getWriteLog = null,
  files = [],
  filesLoading = false,
  filesError = null,
  onRefreshFiles,
  readOnly = false,
  canDownload = true,
  singleFileMode = false,
  persistTabs = true,
  apiAdapter = null,
  onAddContext = null,
  showSystemFiles = false,
  onToggleSystemFiles = null,
  hideClose = false,
  onDirtyChange = null,
  onSwitchToMemoTab = null,
  onCopyShareLink = null,
}: FilePanelProps): React.ReactElement {
  const { t } = useTranslation();
  const isMobile = useIsMobile();
  const panelRef = useRef<HTMLDivElement>(null);
  const narrow = useNarrowContainer(panelRef, TREE_OVERLAY_WIDTH);

  // A share reads through its own endpoints, which take no workspace id.
  // Memoised as one object: every hook below closes over these, and rebuilding
  // them per render would make each of those callbacks unstable in turn.
  const { readFileFn, readFileFullFn, downloadFileAsArrayBufferFn, triggerDownloadFn, writeFileFn, resolveFileFn } = useMemo(() => {
    const adapter: ApiAdapter = apiAdapter ?? {};
    const { readFile, readFileFull, downloadFileAsArrayBuffer, triggerDownload, writeFile, resolveFile } = adapter;
    return {
      readFileFn: readFile ? (_: string, p: string) => readFile(p) : readWorkspaceFile,
      readFileFullFn: readFileFull ? (_: string, p: string) => readFileFull(p) : readWorkspaceFileFull,
      downloadFileAsArrayBufferFn: downloadFileAsArrayBuffer
        ? (_: string, p: string) => downloadFileAsArrayBuffer(p)
        : downloadWorkspaceFileAsArrayBuffer,
      triggerDownloadFn: triggerDownload ? (_: string, p: string) => triggerDownload(p) : triggerFileDownload,
      writeFileFn: writeFile ? (_: string, p: string, c: string) => writeFile(p, c) : writeWorkspaceFile,
      resolveFileFn: apiAdapter
        ? resolveFile ?? null
        : (candidates: string[], recentWrites: string[]) => resolveWorkspaceFile(workspaceId, candidates, recentWrites),
    };
  }, [apiAdapter, workspaceId]);

  const { data: wsData } = useWorkspace(workspaceId);
  const isFlashWorkspace = wsData?.status === 'flash';
  // A file the tree never got is not a file that is gone, so this flag also
  // decides what a missed reference is told.
  const filesRestoreIncomplete = wsData?.files_restore_incomplete === true;

  // A share has no workspace id of its own, so its bodies are scoped to this
  // mount: two shares open at once must not read each other's bytes.
  const mountId = useId();
  const scope = workspaceId || `adapter:${mountId}`;
  const readers = useMemo(
    () => ({ readFile: readFileFn, readFileFull: readFileFullFn, downloadFileAsArrayBuffer: downloadFileAsArrayBufferFn }),
    [readFileFn, readFileFullFn, downloadFileAsArrayBufferFn],
  );
  const cache = useFileBodyCache({ scope, workspaceId, readers });

  // A peek at one file borrows the PTC workspace's id for its reads; its strip
  // must not become that workspace's seed, so it is kept in memory only, and
  // the last chart symbol is neither read nor left behind. The strip is still
  // this workspace's: pointing the panel at another one starts that
  // workspace's strip rather than leaving these tabs under the new id.
  const persistStrip = !singleFileMode && persistTabs;
  const tabs = useFileTabs(workspaceId, threadId, { persist: persistStrip });
  const activeTab = tabs.activeTab;
  const selectedFile = activeTab.kind === 'file' ? activeTab.path : null;

  const previews = usePreviews(workspaceId);

  const { body, loading: fileLoading, error: readError, readAt, refetch } = useFileBody({
    cache, path: selectedFile, workspaceStatus: wsData?.status,
  });
  const fileContent = body?.content ?? null;
  const fileMime = body?.mime ?? null;

  const changed = useChangedFiles(getWriteLog);
  useEffect(() => {
    if (selectedFile && readAt) changed.markRead(selectedFile);
  }, [selectedFile, readAt]); // eslint-disable-line react-hooks/exhaustive-deps
  // The read marks die with this mount, so the bytes they describe must too:
  // a panel reopened inside the body's fresh window would otherwise show
  // bytes written over while it was away and stamp that write as read. With
  // no observer left, marking stale issues no request; the active tab
  // re-reads on the way back in.
  const dropBodies = useStableHandler(() => cache.invalidate());
  useEffect(() => () => dropBodies(), [dropBodies]);

  const downloads = useFileDownloads({ workspaceId, triggerDownloadFn, workspaceStatus: wsData?.status });
  const fileError = downloads.errorFor(selectedFile) ?? readError;

  const [exportModalOpen, setExportModalOpen] = useState(false);
  const [pageCounts, setPageCounts] = useState<Record<string, number>>({});

  const { uploadProgress, uploadError, setUploadError, fileInputRef, isDragOver, handleFileInputChange, handleDragEnter, handleDragLeave, handleDragOver, handleDrop } =
    useFileUpload({ workspaceId, onRefreshFiles });

  const setFileContent = useCallback((next: React.SetStateAction<string | null>) => {
    if (!selectedFile) return;
    const value = typeof next === 'function' ? next(fileContent) : next;
    cache.patchBody(selectedFile, { content: value, truncated: false });
  }, [cache, selectedFile, fileContent]);

  const edit = useFileEdit({
    tabId: activeTab.id, workspaceId, selectedFile, fileContent, setFileContent, readFileFullFn, writeFileFn,
  });

  // Every draft, parked or on screen, lives in this mount and dies with it. A
  // wrapper that owns the close button therefore has to ask before it unmounts
  // the panel, and can only know to when the panel says so. Reporting clean on
  // the way out keeps it from asking about a panel that is already gone.
  const reportDirty = useStableHandler((dirty: boolean) => onDirtyChange?.(dirty));
  useEffect(() => { reportDirty(edit.hasAnyUnsavedChanges); }, [edit.hasAnyUnsavedChanges, reportDirty]);
  useEffect(() => () => reportDirty(false), [reportDirty]);

  const { selectionTooltip, contentWrapperRef, contextMenu, setContextMenu, handleContentMouseUp, handleEditorTextSelect, handleAddSelectionContext } =
    useSelectionContext({ selectedFile, fileContent, onAddContext });

  const viewer = selectedFile ? viewerFor(selectedFile, fileMime, edit.isEditing) : 'other';
  const focus = useFileFocus({
    selectedFile,
    viewer,
    ready: !fileLoading && !fileError,
    editing: edit.isEditing,
    content: fileContent,
    truncated: !!body?.truncated,
    pageCount: selectedFile ? pageCounts[selectedFile] ?? null : null,
    containerRef: contentWrapperRef,
  });

  // Replay the tab's own location whenever it comes back to the front.
  const tabLocationSeq = activeTab.kind === 'file' ? activeTab.locationSeq : 0;
  useEffect(() => {
    if (activeTab.kind === 'file' && activeTab.location) focus.focusAt(activeTab.path, activeTab.location);
  }, [activeTab.id, tabLocationSeq]); // eslint-disable-line react-hooks/exhaustive-deps

  const handleAddToMemo = useAddToMemo({ workspaceId, downloadFileAsArrayBufferFn, readFileFullFn, onSwitchToMemoTab });
  const memoedMap = useWorkspaceMemoIndex(workspaceId);
  const memoEntry = selectedFile ? memoedMap.get(selectedFile) ?? null : null;
  const { status: memoStaleStatus, sandboxText: memoStaleSandboxText, refresh: refreshMemoStale } = useMemoStaleCheck({
    workspaceId, selectedFile, fileMime, memoSha256: memoEntry?.sha256 ?? null, readFileFullFn,
  });
  const [memoSyncing, setMemoSyncing] = useState(false);
  const [memoDiffOpen, setMemoDiffOpen] = useState(false);

  // The folder a chat link pointed the tree at; it filters until the back
  // button clears it. The panel's own state rather than a reading of the
  // target: the target clears once handled, like every other kind, so a
  // remount does not replay the ask and pop the tree open again.
  const [scopeDir, setScopeDir] = useState<string | null>(() => (target?.kind === 'file' ? target.dir ?? null : null));
  const filter = useTreeFilter({ workspaceId, files, scopeDir, rootRef: panelRef });
  // A deleted file's tab goes with it, draft and cached bytes included, or the
  // strip keeps showing a file the tree no longer lists and a save would write
  // it back. No confirm: the reader just confirmed the delete, and the write
  // log never records a panel-side delete, so the close path's change marker
  // would not drop the body on its own.
  const forgetDeleted = useCallback((paths: string[]) => {
    const gone = new Set(paths);
    for (const tab of tabs.tabs) {
      if (tab.kind !== 'file' || !gone.has(tab.path)) continue;
      edit.forgetTab(tab.id);
      changed.forget(tab.path);
      cache.invalidate(tab.path);
      tabs.closeTab(tab.id);
    }
  }, [tabs, edit, changed, cache]);
  const selection = useFileSelection({ workspaceId, filteredSortedFiles: filter.filteredSortedFiles, targetDirectory: scopeDir, onRefreshFiles, onDeleted: forgetDeleted });
  const backup = useFileBackup({ workspaceId, files, readOnly });

  // `openFileAt` is defined below, so the tree reaches it through a handler
  // that stays the same object across renders.
  const openFromTree = useStableHandler((path: string) => { void openFileAt(path); });
  const tree = useTreeInteraction({
    workspaceId, rootRef: panelRef, selection, narrow, activePath: selectedFile, openFile: openFromTree,
  });
  const { open: treeOpen, setOpen: setTreeOpen } = tree;

  /** A breadcrumb segment points the tree at that directory. */
  const revealInTree = useCallback((dir: string) => {
    setTreeOpen(true);
    filter.revealDir(dir);
  }, [setTreeOpen, filter]);

  /** Nothing the panel is showing over the viewer survives a file landing in it. */
  const onBeforeOpen = useCallback(() => {
    downloads.clearError();
  }, [downloads]);

  /** A reference nothing could settle: the tree, filtered to the name it used. */
  const landOnSearch = useCallback((ref: string, name: string, matches: string[]) => {
    filter.showMatches(ref, name, matches);
    setTreeOpen(true);
    // A folder scope would hide candidates outside it, so it goes.
    setScopeDir(null);
  }, [filter, setTreeOpen]);

  const { openFileAt, openFileRef, retryOpen, cancelPending } = useFileRefOpen({
    tabs,
    cache,
    hasChanged: changed.hasChanged,
    files,
    workspaceStatus: wsData?.status,
    resolveFileFn,
    getRecentWritePaths,
    onBeforeOpen,
    clearSearch: filter.clearSearch,
    onLandOnSearch: landOnSearch,
    refetch,
  });

  // A folder from chat points the tree, which stays on screen beside the open
  // file, so nothing has to be closed to honour it. A parent that leaves the
  // target in place re-runs this on every render of it, so the ask is keyed:
  // a target that only shed its path must not re-open a tree the reader has
  // since folded away, only a new ask does.
  const lastDirAsk = useRef<string | null>(null);
  useEffect(() => {
    if (!target) return;
    switch (target.kind) {
      case 'file': {
        if (target.dir != null) {
          setScopeDir(target.dir);
          const ask = `${target.seq ?? 0}:${target.dir}`;
          if (lastDirAsk.current !== ask) {
            lastDirAsk.current = ask;
            filter.clearSearch();
            setTreeOpen(true);
          }
        }
        if (target.path) void openFileRef(target.path, { location: target.location ?? null, pin: !!target.pin });
        onTargetHandled?.();
        return;
      }
      case 'preview':
        // The agent published an app: its tab, and a URL fresh enough to load.
        cancelPending();
        tabs.openPreview(target);
        previews.open(target);
        onTargetHandled?.();
        return;
      case 'chart':
        cancelPending();
        tabs.openChart(target);
        onTargetHandled?.();
        return;
      default:
        return;
    }
  }, [target]); // eslint-disable-line react-hooks/exhaustive-deps

  const retry = useCallback(() => {
    downloads.clearError();
    retryOpen(selectedFile);
  }, [downloads, retryOpen, selectedFile]);

  // --- file actions ---

  const handleDownloadSelected = canDownload && selectedFile ? () => downloads.download(selectedFile) : undefined;
  const handleDownloadInFallback = canDownload && selectedFile ? () => downloads.downloadQuietly(selectedFile) : undefined;

  const handleContextMenuAction = useCallback((action: FileMenuAction, filePath: string) => {
    setContextMenu(null);
    if (action === 'add-context' && onAddContext) {
      tabs.openFile(filePath, { pin: true });
      onAddContext({ path: filePath });
    } else if (action === 'add-to-memo') {
      handleAddToMemo(filePath);
    } else if (action === 'open') {
      void openFileAt(filePath);
    } else if (action === 'open-new-tab') {
      void openFileAt(filePath, { pin: true });
    } else if (action === 'download') {
      downloads.download(filePath);
    } else if (action === 'download-many') {
      void downloads.downloadMany([...selection.selectedPaths]);
    }
  }, [onAddContext, handleAddToMemo, openFileAt, downloads, selection.selectedPaths, setContextMenu, tabs]);

  /**
   * Coming back to a tab whose file the agent has rewritten re-reads it. The
   * amber dot is the notice; arriving at the tab is the moment the reader
   * wants the new bytes, and re-reading every open tab the instant a write
   * lands would fight whoever is reading one of them.
   */
  const activateTab = useCallback((id: string) => {
    const tab = tabs.tabs.find((x) => x.id === id);
    if (tab?.kind === 'file' && changed.hasChanged(tab.path)) cache.invalidate(tab.path);
    cancelPending();
    tabs.activate(id);
  }, [tabs, changed, cache, cancelPending]);

  const newTab = useCallback(() => {
    cancelPending();
    tabs.newTab();
  }, [tabs, cancelPending]);

  const openSettings = useCallback(() => {
    cancelPending();
    tabs.openSettings();
  }, [tabs, cancelPending]);

  const startEdit = useCallback(() => {
    tabs.pinTab(activeTab.id);
    void edit.handleStartEdit();
  }, [tabs, activeTab.id, edit]);

  // Citing a range is working with the file, so its tab stops being the loaned
  // one, the same rule the selection tooltip and the tree's menu follow.
  const addViewerContext = useCallback((ctx: ContextPayload) => {
    tabs.pinTab(activeTab.id);
    onAddContext?.(ctx);
  }, [tabs, activeTab.id, onAddContext]);

  // A route change fires no beforeunload, so the panel's drafts would go with it.
  const leaveForMarketView = useCallback((spec: ChartTabSpec) => {
    if (edit.hasAnyUnsavedChanges && !window.confirm(t('filePanel.discardUnsaved'))) return;
    onOpenInMarketView?.(spec);
  }, [edit.hasAnyUnsavedChanges, onOpenInMarketView, t]);

  const closeTab = useCallback((id: string) => {
    if (edit.tabHasUnsavedChanges(id) && !window.confirm(t('filePanel.discardUnsaved'))) return;
    edit.forgetTab(id);
    const tab = tabs.tabs.find((x) => x.id === id);
    if (tab?.kind === 'file') {
      // The marker is what forces a re-read on reopen; the cached bytes must
      // not outlive it, or a rewrite lands inside the body's fresh window.
      if (changed.hasChanged(tab.path)) cache.invalidate(tab.path);
      changed.forget(tab.path);
    }
    if (singleFileMode && tabs.tabs.length <= 1) return onClose();
    tabs.closeTab(id);
  }, [edit, tabs, changed, cache, singleFileMode, onClose, t]);

  const handleViewerLink = useStableHandler((path: string, linkWorkspaceId?: string, location?: FileLocation, opts?: { rooted?: boolean; pin?: boolean }) => {
    const rooted = !!opts?.rooted;
    const otherWorkspace = !!linkWorkspaceId && linkWorkspaceId !== workspaceId;
    const kind = classifyAgentPath(path).kind;
    const directory = parseAgentPath(path).directory;
    // A folder inside a viewed file is written relative to it, the same as a
    // file link, but the router takes the path as given, so the join happens here.
    const linkTarget = directory && kind === 'file' && !otherWorkspace
      ? linkCandidates(path, rooted ? null : selectedFile)[0]
      : path;
    if (otherWorkspace || kind !== 'file' || directory) {
      onOpenFile?.(linkTarget, linkWorkspaceId, location);
      return;
    }
    // A rooted reference named where it starts, so the open file's directory is
    // not a reading it invited.
    void openFileRef(path, { fromFile: rooted ? null : selectedFile, location, pin: opts?.pin });
  });

  const handleAnchorLink = useStableHandler((fragment: string) => {
    if (selectedFile) focus.focusAt(selectedFile, parseFragment(fragment));
  });

  const handleSyncMemo = useCallback(async () => {
    if (!selectedFile || memoSyncing) return;
    setMemoSyncing(true);
    try {
      await handleAddToMemo(selectedFile);
      refreshMemoStale();
    } finally {
      setMemoSyncing(false);
    }
  }, [selectedFile, memoSyncing, handleAddToMemo, refreshMemoStale]);

  // A write lands in the sandbox, so the copy the panel holds and the backup
  // verdict beside it are both a version behind until they are re-read.
  const handleSave = useCallback(async () => {
    await edit.handleSave();
    if (selectedFile) cache.invalidate(selectedFile);
    onRefreshFiles?.();
  }, [edit, cache, selectedFile, onRefreshFiles]);

  // Editing needs a text viewer under it: the pdf, excel and html readers are
  // not editors, and an image or a parked tool result is not text. A CSV reads
  // in the grid (`other`) but edits as the text it is; a file already in the
  // editor keeps its verdict rather than reading the editor as `other`.
  const selectedExt = selectedFile ? getFileExtension(selectedFile) : '';
  const canEdit = !!(selectedFile && !readOnly && !fileError
    && EDITABLE_EXTENSIONS.has(selectedExt)
    && (edit.isEditing || viewer === 'code' || viewer === 'markdown' || (viewer === 'other' && selectedExt === 'csv' && fileMime !== 'image')));

  const meta = useMemo(() => {
    if (!selectedFile || !body) return null;
    const pages = pageCounts[selectedFile];
    if (body.mime === 'pdf') return pages ? t('filePanel.metaPages', { count: pages }) : null;
    if (body.content == null) return null;
    const lines = countLines(body.content);
    return body.truncated ? t('filePanel.metaFirstLines', { count: lines }) : t('filePanel.metaLines', { count: lines });
  }, [selectedFile, body, pageCounts, t]);

  /** Opening a running app from the tree: the tab it already has, or a new one. */
  const openPreviewTab = useCallback((port: number) => {
    const entry = previews.byPort.get(port);
    cancelPending();
    tabs.openPreview({ port, title: entry?.title, path: entry?.path, command: entry?.command });
    previews.ensure(port);
  }, [previews, tabs, cancelPending]);

  // Only a folder-like tab takes a drop: a preview is a frame and a chart is a
  // live view, and settings is a form.
  const canDropHere = !readOnly && (activeTab.kind === 'empty' || activeTab.kind === 'file');

  /** What fills the viewer slot. Preview panes are rendered beside this, always mounted. */
  const renderActive = (): React.ReactNode => {
    switch (activeTab.kind) {
      case 'preview':
        return null;
      case 'chart':
        return (
          <ChartTab
            tab={activeTab}
            tabs={tabs}
            workspaceId={workspaceId}
            onAddContext={onAddContext}
            onOpenInMarketView={onOpenInMarketView ? leaveForMarketView : null}
          />
        );
      case 'settings':
        return (
          <div className="file-panel-settings">
            <SandboxSettingsContent workspaceId={workspaceId} />
          </div>
        );
      case 'file': {
        const path = activeTab.path;
        return (
          <FileViewer
            path={path}
            body={body}
            loading={fileLoading}
            error={fileError}
            onRetry={retry}
            onDownload={handleDownloadSelected}
            onDownloadInFallback={handleDownloadInFallback}
            workspaceId={workspaceId}
            focus={focus}
            onPageCount={(pages) => setPageCounts((prev) => (prev[path] === pages ? prev : { ...prev, [path]: pages }))}
            isEditing={edit.isEditing}
            editContent={edit.editContent}
            originalContent={edit.originalContent}
            showDiff={edit.showDiff}
            editorRef={edit.editorRef}
            onEditorChange={edit.handleEditorChange}
            onUndoRedoChange={edit.handleUndoRedoChange}
            onEditorTextSelect={handleEditorTextSelect}
            onAddContext={onAddContext ? addViewerContext : null}
            onContentMouseUp={handleContentMouseUp}
            onViewerLink={handleViewerLink}
            onAnchorLink={handleAnchorLink}
            servedUrl={apiAdapter?.buildServedUrl?.(path, { injectTheme: true })}
            onCopyShareLink={onCopyShareLink}
          />
        );
      }
      case 'empty':
        return (
          <EmptyTab
            canUpload={!readOnly}
            treeOpen={treeOpen}
            onShowTree={singleFileMode ? null : () => setTreeOpen(true)}
            onOpenChart={readOnly || singleFileMode ? null : () => { cancelPending(); tabs.openChart({ symbol: lastChartSymbol(workspaceId, { persist: persistStrip }) }); }}
          />
        );
      default:
        return activeTab satisfies never;
    }
  };

  // The panel's own close button, on a mount that owns one. An unsaved edit
  // takes it away rather than asking about it: the tab's close asks already,
  // and that is the way out that lets the reader save first.
  const panelClose = hideClose || edit.hasAnyUnsavedChanges ? null : onClose;

  return (
    <div className="file-panel" ref={panelRef} onKeyDown={tree.onEscape}>
      <TabStrip
        tabs={tabs.tabs}
        activeId={tabs.activeId}
        onActivate={activateTab}
        onClose={closeTab}
        onPin={tabs.pinTab}
        onNewTab={singleFileMode || readOnly ? null : newTab}
        hasChanged={changed.hasChanged}
        treeOpen={treeOpen}
        onToggleTree={singleFileMode ? null : () => setTreeOpen((v) => !v)}
        onPanelClose={panelClose}
        backArrow={isMobile}
      />

      {activeTab.kind === 'preview' && (
        <PreviewCrumbs
          entry={previews.byPort.get(activeTab.port) ?? { port: activeTab.port, url: '', loading: true, error: false, reloadToken: 0 }}
          onRefresh={() => previews.refresh(activeTab.port)}
        />
      )}

      {selectedFile && (
        <FileCrumbs
          path={selectedFile}
          onOpenDir={revealInTree}
          meta={meta}
          unsaved={edit.hasUnsavedChanges}
          chip={focus.chip && (
            <FocusChip state={focus.chip} onJump={focus.jump} onDismiss={() => { focus.dismiss(); tabs.clearLocation(activeTab.id); }} />
          )}
          actions={(
            <FileHeaderActions
              selectedFile={selectedFile}
              isEditing={edit.isEditing}
              workspaceId={workspaceId}
              fileContent={fileContent}
              fileMime={fileMime}
              canEdit={canEdit}
              onStartEdit={startEdit}
              onOpenExportModal={() => setExportModalOpen(true)}
              triggerDownloadFn={triggerDownloadFn}
              canDownload={canDownload}
              readFileFullFn={readFileFullFn}
              htmlServedUrl={apiAdapter?.buildServedUrl?.(selectedFile)}
              editorRef={edit.editorRef}
              canUndo={edit.canUndo}
              canRedo={edit.canRedo}
              hasUnsavedChanges={edit.hasUnsavedChanges}
              showDiff={edit.showDiff}
              setShowDiff={edit.setShowDiff}
              isSaving={edit.isSaving}
              saveError={edit.saveError}
              onSave={handleSave}
              onCancelEdit={edit.handleCancelEdit}
            />
          )}
        />
      )}

      <PanelNotices
        uploadProgress={uploadProgress}
        error={uploadError || selection.deleteError}
        onDismissError={() => { setUploadError(null); selection.setDeleteError(null); }}
        // The tree shows this itself; the body takes it over while the tree is
        // folded or absent, so a listing that failed is never a silent blank.
        filesError={treeOpen && !singleFileMode ? null : filesError}
        filesRestoreIncomplete={treeOpen && !singleFileMode ? false : filesRestoreIncomplete}
        onRefreshFiles={onRefreshFiles}
        busy={selection.deleteLoading || backup.backingUp}
        backupResult={backup.backupResult}
        onDismissBackupResult={() => backup.setBackupResult(null)}
        editing={edit.isEditing}
      />
      {memoEntry && selectedFile && (
        <MemoStaleBanner
          status={memoStaleStatus}
          syncing={memoSyncing}
          onSwitchToMemoTab={onSwitchToMemoTab}
          onSync={handleSyncMemo}
          onViewDiff={memoStaleSandboxText !== null ? () => setMemoDiffOpen(true) : null}
        />
      )}

        <div className="file-panel-body">
          <div
            className="file-panel-viewer"
            onDragEnter={canDropHere ? handleDragEnter : undefined}
            onDragLeave={canDropHere ? handleDragLeave : undefined}
            onDragOver={canDropHere ? handleDragOver : undefined}
            onDrop={canDropHere ? handleDrop : undefined}
          >
            {canDropHere && isDragOver && (
              <div className="file-panel-drag-overlay">
                <Upload className="h-8 w-8" style={{ color: 'var(--color-accent-primary)' }} />
                <span>{t('filePanel.dropToUpload')}</span>
              </div>
            )}
            {/* font-content only while reading a file: the tree stays on the UI font. */}
            <div className={`file-panel-content${selectedFile ? ' font-content' : ''}`} ref={contentWrapperRef}>
              {selectionTooltip && onAddContext && (
                <button
                  type="button"
                  className="file-panel-selection-tooltip"
                  style={{ left: Math.max(8, selectionTooltip.x - 60), top: Math.max(4, selectionTooltip.y - 32) }}
                  // Acts on mousedown: a click would land after the browser has
                  // already collapsed the selection it is meant to capture.
                  onMouseDown={(e: React.MouseEvent) => {
                    e.preventDefault(); e.stopPropagation();
                    tabs.pinTab(activeTab.id);
                    handleAddSelectionContext();
                  }}
                  onKeyDown={(e: React.KeyboardEvent) => {
                    if (e.key !== 'Enter' && e.key !== ' ') return;
                    e.preventDefault();
                    tabs.pinTab(activeTab.id);
                    handleAddSelectionContext();
                  }}
                >
                  <TextSelect className="h-3.5 w-3.5" style={{ color: 'var(--color-accent-primary)' }} />
                  {selectionTooltip.lineStart != null
                    ? (selectionTooltip.lineEnd !== selectionTooltip.lineStart
                        ? t('context.addLinesToContext', { start: selectionTooltip.lineStart, end: selectionTooltip.lineEnd })
                        : t('context.addLineToContext', { line: selectionTooltip.lineStart }))
                    : t('context.addToContext')}
                </button>
              )}
              <PreviewPanes tabs={tabs.tabs} activeId={activeTab.id} previews={previews} />
              {renderActive()}
            </div>
          </div>

          <AnimatePresence initial={false}>
          {!singleFileMode && treeOpen && (
            <TreeColumn
              key="tree"
              filter={filter}
              selection={selection}
              backup={backup}
              filesLoading={filesLoading}
              filesError={filesError}
              onRefreshFiles={onRefreshFiles}
              showSystemFiles={showSystemFiles}
              onToggleSystemFiles={onToggleSystemFiles}
              scopeDir={scopeDir}
              onClearScope={() => setScopeDir(null)}
              openPaths={tabs.openPaths}
              activePath={selectedFile}
              onFileClick={tree.onOpen}
              onFileDoubleClick={(path) => { tabs.openFile(path, { pin: true }); void cache.fetchBody(path).catch(() => {}); }}
              onOpenFromKeyboard={tree.onOpen}
              onEscape={() => (selection.selectMode ? selection.exitSelectMode() : setTreeOpen(!narrow))}
              memoedMap={memoedMap}
              memoedTitle={t('context.inMemo')}
              onAddContext={onAddContext}
              setContextMenu={setContextMenu}
              activeContextPath={contextMenu?.filePath ?? null}
              readOnly={readOnly}
              uploadDisabled={uploadProgress !== null}
              onUpload={() => fileInputRef.current?.click()}
              previews={previews.previews}
              onOpenPreview={openPreviewTab}
              activePreviewPort={activeTab.kind === 'preview' ? activeTab.port : null}
              onOpenSettings={!readOnly && !isFlashWorkspace ? openSettings : null}
              workspaceName={wsData?.name}
              filesRestoreIncomplete={filesRestoreIncomplete}
              overlay={narrow}
              onDismissOverlay={() => setTreeOpen(false)}
            />
          )}
          </AnimatePresence>
        </div>

      <input ref={fileInputRef} type="file" className="hidden" onChange={handleFileInputChange} />

      {contextMenu && (
        <FileContextMenu
          menu={contextMenu}
          onAction={handleContextMenuAction}
          onClose={() => setContextMenu(null)}
          canAddContext={!!onAddContext}
          memoState={memoMimeForName(contextMenu.filePath)
            ? (memoedMap.has(contextMenu.filePath) ? 'present' : 'absent')
            : null}
          canDownload={canDownload}
          selectedCount={selection.selectedPaths.has(contextMenu.filePath) ? selection.selectedPaths.size : 0}
        />
      )}

      {selectedFile && exportModalOpen && (
        <Suspense fallback={null}>
          <ExportPreviewModal
            open={exportModalOpen}
            onOpenChange={setExportModalOpen}
            content={fileContent ?? ''}
            fileName={selectedFile}
            workspaceId={workspaceId}
            readFileFullFn={readFileFullFn}
          />
        </Suspense>
      )}
      {selectedFile && memoEntry && memoStaleSandboxText !== null && (
        <MemoDiffModal
          open={memoDiffOpen}
          memoKey={memoEntry.key}
          fileName={selectedFile.split('/').pop() || selectedFile}
          sandboxText={memoStaleSandboxText}
          onClose={() => setMemoDiffOpen(false)}
        />
      )}
    </div>
  );
}

export default FilePanel;
export type { ContextPayload, PanelTarget } from './filePanel/types';
export { SYSTEM_DIR_PREFIXES } from './filePanel/fileMeta';
// eslint-disable-next-line react-refresh/only-export-components
export { categorizeFileError } from './filePanel/fileErrors';
export { FileErrorDisplay } from './filePanel/fileErrors';
