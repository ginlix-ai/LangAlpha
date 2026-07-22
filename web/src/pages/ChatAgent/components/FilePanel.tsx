import React, { useState, useEffect, useCallback, useRef, useMemo, Suspense } from 'react';
import { ArrowLeft, X, RefreshCw, Upload, ArrowUpDown, Trash2, CheckSquare, HardDrive, Pencil, TextSelect, FolderOpen, Settings, ScrollText } from 'lucide-react';
import {
  memoMimeForName,
  useAddToMemo,
  useWorkspaceMemoIndex,
  useMemoStaleCheck,
  MemoStaleBanner,
  MemoDiffModal,
} from './FilePanelMemo';
import { useWorkspace } from '@/hooks/useWorkspace';
import { SandboxSettingsContent } from './SandboxSettingsPanel';
import { useIsMobile } from '@/hooks/useIsMobile';
import SyntaxHighlighter, { oneDark, oneLight } from './SyntaxHighlighter';
import { useTranslation } from 'react-i18next';
import { readWorkspaceFile, readWorkspaceFileFull, writeWorkspaceFile, downloadWorkspaceFile, downloadWorkspaceFileAsArrayBuffer, triggerFileDownload } from '../utils/api';
import { stripLineNumbers } from './toolDisplayConfig';
import Markdown from './Markdown';
import ImageLightbox from './ImageLightbox';
import DocumentErrorBoundary from './viewers/DocumentErrorBoundary';
import FileHeaderActions from './FileHeaderActions';
import './FilePanel.css';

const PdfViewer = React.lazy(() => import('./viewers/PdfViewer'));
const ExcelViewer = React.lazy(() => import('./viewers/ExcelViewer'));
const CsvViewer = React.lazy(() => import('./viewers/CsvViewer'));
const HtmlViewer = React.lazy(() => import('./viewers/HtmlViewer'));
const CodeEditor = React.lazy(() => import('./viewers/CodeEditor'));
const ExportPreviewModal = React.lazy(() => import('./ExportPreviewModal'));

import type { ApiAdapter, ContextPayload } from './filePanel/types';
import type { FileError } from './filePanel/fileErrors';
import { categorizeFileError, FileErrorDisplay } from './filePanel/fileErrors';
import { EDITABLE_EXTENSIONS, EXT_TO_LANG, getAvailableTypes, getFileExtension, getFileType, SORT_OPTIONS, sortFiles } from './filePanel/fileMeta';
import { buildFileTree } from './filePanel/fileTree';
import { DirectoryNode } from './filePanel/DirectoryNode';
import { DocumentErrorFallback, DocumentLoadingFallback } from './filePanel/fallbacks';
import { useFileUpload } from './filePanel/useFileUpload';
import { useFileEdit } from './filePanel/useFileEdit';
import { useSelectionContext } from './filePanel/useSelectionContext';
import { useFileSelection } from './filePanel/useFileSelection';
import { useFileBackup } from './filePanel/useFileBackup';

// --- FilePanel ---

interface FilePanelProps {
  workspaceId: string;
  onClose: () => void;
  targetFile?: string | null;
  onTargetFileHandled?: () => void;
  targetDirectory?: string | null;
  onTargetDirHandled?: () => void;
  files?: string[];
  filesLoading?: boolean;
  filesError?: string | null;
  onRefreshFiles?: () => void;
  readOnly?: boolean;
  /** Lock to a single file — back button closes the panel instead of returning to the file tree. */
  singleFileMode?: boolean;
  apiAdapter?: ApiAdapter | null;
  onAddContext?: ((ctx: ContextPayload) => void) | null;
  showSystemFiles?: boolean;
  onToggleSystemFiles?: (() => void) | null;
  /** Hide the panel-close affordances (the mobile back arrow and the trailing X)
   * when FilePanel is embedded inside a tabbed wrapper that owns the close button. */
  hideClose?: boolean;
  onSwitchToMemoTab?: (() => void) | null;
  /** Copy a shareable link to an HTML report (authenticated app only). Enables
   *  sharing if needed, then copies a direct full-tab link to the served file
   *  (`${origin}/api/v1/public/shared/{token}/files/serve/<path>`). */
  onCopyShareLink?: ((filePath: string) => void) | null;
}

function FilePanel({
  workspaceId,
  onClose,
  targetFile,
  onTargetFileHandled,
  targetDirectory,
  onTargetDirHandled,
  // Shared file list from useWorkspaceFiles hook
  files = [],
  filesLoading = false,
  filesError = null,
  onRefreshFiles,
  readOnly = false,
  singleFileMode = false,
  apiAdapter = null,
  onAddContext = null,
  showSystemFiles = false,
  onToggleSystemFiles = null,
  hideClose = false,
  onSwitchToMemoTab = null,
  onCopyShareLink = null,
}: FilePanelProps): React.ReactElement {
  const { t } = useTranslation();
  const isMobile = useIsMobile();
  // Resolve API functions -- use adapter overrides if provided, otherwise fall back to authenticated imports
  const readFileFn = apiAdapter?.readFile
    ? (_: string, path: string) => apiAdapter.readFile!(path)
    : readWorkspaceFile;
  const downloadFileFn = apiAdapter?.downloadFile
    ? (_: string, path: string) => apiAdapter.downloadFile!(path)
    : downloadWorkspaceFile;
  const downloadFileAsArrayBufferFn = apiAdapter?.downloadFileAsArrayBuffer
    ? (_: string, path: string) => apiAdapter.downloadFileAsArrayBuffer!(path)
    : downloadWorkspaceFileAsArrayBuffer;
  const triggerDownloadFn = apiAdapter?.triggerDownload
    ? (_: string, path: string) => apiAdapter.triggerDownload!(path)
    : triggerFileDownload;
  const writeFileFn = apiAdapter?.writeFile
    ? (_: string, path: string, content: string) => apiAdapter.writeFile!(path, content)
    : writeWorkspaceFile;
  const readFileFullFn = apiAdapter?.readFileFull
    ? (_: string, path: string) => apiAdapter.readFileFull!(path)
    : readWorkspaceFileFull;

  // Workspace settings inline view
  const [showSettings, setShowSettings] = useState(false);
  const { data: wsData } = useWorkspace(workspaceId);
  const isFlashWorkspace = wsData?.status === 'flash';
  const workspaceName = wsData?.name;

  // File detail view state
  const [selectedFile, setSelectedFile] = useState<string | null>(null);
  const [fileContent, setFileContent] = useState<string | null>(null);
  const [fileArrayBuffer, setFileArrayBuffer] = useState<ArrayBuffer | null>(null);
  const [fileMime, setFileMime] = useState<string | null>(null);
  const [imageLightboxOpen, setImageLightboxOpen] = useState(false);
  const [fileLoading, setFileLoading] = useState(false);
  const [fileError, setFileError] = useState<FileError | null>(null);

  // Upload + drag-and-drop (filePanel/useFileUpload).
  const {
    uploadProgress,
    uploadError,
    setUploadError,
    fileInputRef,
    isDragOver,
    handleFileInputChange,
    handleDragEnter,
    handleDragLeave,
    handleDragOver,
    handleDrop,
  } = useFileUpload({ workspaceId, onRefreshFiles });

  // Edit mode (filePanel/useFileEdit).
  const {
    isEditing,
    setIsEditing,
    editContent,
    setEditContent,
    isSaving,
    saveError,
    setSaveError,
    showDiff,
    setShowDiff,
    originalContent,
    setOriginalContent,
    editorRef,
    canUndo,
    setCanUndo,
    canRedo,
    setCanRedo,
    handleUndoRedoChange,
    hasUnsavedChanges,
    handleStartEdit,
    handleEditorChange,
    handleSave,
    handleCancelEdit,
  } = useFileEdit({ workspaceId, selectedFile, fileContent, setFileContent, readFileFullFn, writeFileFn });

  // Selection tooltip + right-click context menu (filePanel/useSelectionContext).
  const {
    selectionTooltip,
    contentWrapperRef,
    contextMenu,
    setContextMenu,
    handleContentMouseUp,
    handleEditorTextSelect,
    handleAddSelectionContext,
  } = useSelectionContext({ selectedFile, fileContent, onAddContext });

  const handleAddToMemo = useAddToMemo({
    workspaceId,
    downloadFileAsArrayBufferFn,
    readFileFullFn,
    onSwitchToMemoTab,
  });

  const handleContextMenuAction = useCallback((action: string, filePath: string) => {
    setContextMenu(null);
    if (action === 'add-context' && onAddContext) {
      onAddContext({ path: filePath });
    } else if (action === 'add-to-memo') {
      handleAddToMemo(filePath);
    } else if (action === 'open') {
      handleFileClick(filePath);
    }
  }, [onAddContext, handleAddToMemo]); // eslint-disable-line react-hooks/exhaustive-deps

  // Export modal state
  const [exportModalOpen, setExportModalOpen] = useState(false);

  // Filter and sort state
  const [filterType, setFilterType] = useState('All');
  const [sortBy, setSortBy] = useState('name-asc');
  const [showSortMenu, setShowSortMenu] = useState(false);
  const sortMenuRef = useRef<HTMLDivElement>(null);

  // Memo'd lookup + stale-check verdict — see FilePanelMemo.tsx.
  const memoedMap = useWorkspaceMemoIndex(workspaceId);
  const memoedTitle = t('context.inMemo');
  const memoEntryForSelected = selectedFile ? memoedMap.get(selectedFile) ?? null : null;
  const {
    status: memoStaleStatus,
    sandboxText: memoStaleSandboxText,
    refresh: refreshMemoStale,
  } = useMemoStaleCheck({
    workspaceId,
    selectedFile,
    fileMime,
    memoSha256: memoEntryForSelected?.sha256 ?? null,
    readFileFullFn,
  });
  const [memoSyncing, setMemoSyncing] = useState(false);
  const [memoDiffOpen, setMemoDiffOpen] = useState(false);

  const handleSyncMemo = useCallback(async () => {
    if (!selectedFile || memoSyncing) return;
    setMemoSyncing(true);
    try {
      await handleAddToMemo(selectedFile);
      // Re-run the stale check even if memoListData is still revalidating.
      refreshMemoStale();
    } finally {
      setMemoSyncing(false);
    }
  }, [selectedFile, memoSyncing, handleAddToMemo, refreshMemoStale]);

  const handleViewMemoDiff = useCallback(() => {
    setMemoDiffOpen(true);
  }, []);

  const availableTypes = useMemo(() => getAvailableTypes(files), [files]);

  // Apply directory filter, type filter, sort, then group
  const filteredSortedFiles = useMemo(() => {
    let result = files;
    if (targetDirectory) {
      const prefix = targetDirectory.endsWith('/') ? targetDirectory : targetDirectory + '/';
      result = result.filter((fp) => fp.startsWith(prefix));
    }
    if (filterType !== 'All') {
      result = result.filter((fp) => getFileType(fp) === filterType);
    }
    return sortFiles(result, sortBy);
  }, [files, filterType, sortBy, targetDirectory]);

  // Multi-select + delete (filePanel/useFileSelection).
  const {
    selectMode,
    setSelectMode,
    selectedPaths,
    deleteLoading,
    deleteError,
    setDeleteError,
    deleteConfirm,
    toggleSelect,
    toggleSelectAll,
    toggleDirSelect,
    exitSelectMode,
    handleDelete,
  } = useFileSelection({ workspaceId, filteredSortedFiles, targetDirectory, onRefreshFiles });

  // COS backup status + trigger (filePanel/useFileBackup).
  const {
    backedUpSet,
    modifiedSet,
    backingUp,
    backupResult,
    setBackupResult,
    handleBackup,
  } = useFileBackup({ workspaceId, files, readOnly });

  // Directory expand state
  const storageKey = `filePanel.expandedDirs.${workspaceId}`;
  const [expandedDirs, setExpandedDirs] = useState<Set<string>>(() => {
    try {
      const saved = localStorage.getItem(storageKey);
      return saved ? new Set(JSON.parse(saved) as string[]) : new Set();
    } catch { return new Set(); }
  });
  const fileTree = useMemo(() => buildFileTree(filteredSortedFiles), [filteredSortedFiles]);

  useEffect(() => {
    localStorage.setItem(storageKey, JSON.stringify([...expandedDirs]));
  }, [expandedDirs, storageKey]);

  const toggleDir = useCallback((dir: string) => {
    setExpandedDirs((prev) => {
      const next = new Set(prev);
      if (next.has(dir)) next.delete(dir);
      else next.add(dir);
      return next;
    });
  }, []);

  useEffect(() => {
    if (!showSortMenu) return;
    const handler = (e: MouseEvent) => {
      if (sortMenuRef.current && !sortMenuRef.current.contains(e.target as Node)) {
        setShowSortMenu(false);
      }
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, [showSortMenu]);

  useEffect(() => {
    return () => {
      if (fileMime === 'image' && fileContent) {
        URL.revokeObjectURL(fileContent);
      }
    };
  }, [fileContent, fileMime]);

  useEffect(() => {
    if (targetFile) {
      handleFileClick(targetFile);
      onTargetFileHandled?.();
    }
  }, [targetFile]); // eslint-disable-line react-hooks/exhaustive-deps

  const handleFileClick = async (filePath: string) => {
    const ext = getFileExtension(filePath);
    setFileError(null);

    // Binary files
    if (['pdf', 'png', 'jpg', 'jpeg', 'gif', 'svg', 'webp', 'xlsx', 'xls', 'docx', 'zip'].includes(ext)) {
      if (['png', 'jpg', 'jpeg', 'gif', 'svg', 'webp'].includes(ext)) {
        if (fileMime === 'image' && fileContent) {
          URL.revokeObjectURL(fileContent);
        }
        setSelectedFile(filePath);
        setFileLoading(true);
        setFileMime('image');
        try {
          const blobUrl = await downloadFileFn(workspaceId, filePath);
          setFileContent(blobUrl);
        } catch (err) {
          console.error('[FilePanel] Failed to download image:', err);
          setFileError(categorizeFileError(err, wsData?.status));
          setFileContent(null);
          setFileMime(null);
        } finally {
          setFileLoading(false);
        }
        return;
      }
      if (ext === 'pdf') {
        setSelectedFile(filePath);
        setFileLoading(true);
        setFileMime('pdf');
        try {
          const buf = await downloadFileAsArrayBufferFn(workspaceId, filePath);
          setFileArrayBuffer(buf);
        } catch (err) {
          console.error('[FilePanel] Failed to load PDF:', err);
          setFileError(categorizeFileError(err, wsData?.status));
          setFileMime(null);
        } finally {
          setFileLoading(false);
        }
        return;
      }
      if (ext === 'xlsx' || ext === 'xls') {
        setSelectedFile(filePath);
        setFileLoading(true);
        setFileMime('excel');
        try {
          const buf = await downloadFileAsArrayBufferFn(workspaceId, filePath);
          setFileArrayBuffer(buf);
        } catch (err) {
          console.error('[FilePanel] Failed to load Excel file:', err);
          setFileError(categorizeFileError(err, wsData?.status));
          setFileMime(null);
        } finally {
          setFileLoading(false);
        }
        return;
      }
      try {
        await triggerDownloadFn(workspaceId, filePath);
      } catch (err) {
        console.error('[FilePanel] Failed to download file:', err);
      }
      return;
    }

    // HTML files: read the full source (the viewer renders via the served URL,
    // but the Source tab needs untruncated content — the paginated read caps at 20k lines).
    if (['html', 'htm'].includes(ext)) {
      setSelectedFile(filePath);
      setFileLoading(true);
      try {
        const data = await readFileFullFn(workspaceId, filePath);
        setFileContent(data.content || '');
        setFileMime('text/html');
      } catch (err) {
        console.error('[FilePanel] Failed to read HTML file:', err);
        setFileError(categorizeFileError(err, wsData?.status));
        setFileContent(null);
        setFileMime(null);
      } finally {
        setFileLoading(false);
      }
      return;
    }

    // Text files - read content
    setSelectedFile(filePath);
    setFileLoading(true);
    try {
      const data = await readFileFn(workspaceId, filePath);
      setFileContent(data.content || '');
      setFileMime(data.mime || 'text/plain');
    } catch (err) {
      console.error('[FilePanel] Failed to read file:', err);
      setFileError(categorizeFileError(err, wsData?.status));
      setFileContent(null);
      setFileMime(null);
    } finally {
      setFileLoading(false);
    }
  };

  const selectedExt = selectedFile ? getFileExtension(selectedFile.split('/').pop() || '') : '';
  const canEdit = !!(selectedFile
    && !readOnly
    && !fileError
    && EDITABLE_EXTENSIONS.has(selectedExt)
    && fileMime !== 'image'
    && fileMime !== 'pdf'
    && fileMime !== 'excel'
    && !['html', 'htm'].includes(selectedExt)
    && !selectedFile.startsWith('/large_tool_results/'));

  const handleBack = () => {
    // In single-file mode, back closes the panel instead of returning to file tree
    if (singleFileMode) {
      onClose();
      return;
    }
    if (hasUnsavedChanges) {
      if (!window.confirm(t('filePanel.discardUnsaved'))) return;
    }
    if (fileMime === 'image' && fileContent) {
      URL.revokeObjectURL(fileContent);
    }
    setSelectedFile(null);
    setFileContent(null);
    setFileArrayBuffer(null);
    setFileMime(null);
    setFileError(null);
    setExportModalOpen(false);
    setIsEditing(false);
    setEditContent(null);
    setShowDiff(false);
    setOriginalContent(null);
    editorRef.current = null;
    setCanUndo(false);
    setCanRedo(false);
    setSaveError(null);
  };

  const fileName = selectedFile?.split('/').pop() || '';

  // The JSX return is very large. Due to its size and the fact that it is
  // purely template code with no logic changes, we keep it identical to the
  // original JS version. TypeScript inference handles the JSX elements.
  return (
    <div className="file-panel">
      {/* Header */}
      <div className="file-panel-header">
        <div className="flex items-center gap-2 min-w-0">
          {showSettings ? (
            <button onClick={() => setShowSettings(false)} className="file-panel-icon-btn" title={t('filePanel.backToFileList')}>
              <ArrowLeft className="h-4 w-4" />
            </button>
          ) : selectedFile ? (
            <button onClick={handleBack} className="file-panel-icon-btn" title={t('filePanel.backToFileList')}>
              <ArrowLeft className="h-4 w-4" />
            </button>
          ) : targetDirectory ? (
            <button onClick={() => onTargetDirHandled?.()} className="file-panel-icon-btn" title={t('filePanel.backToAllFiles')}>
              <ArrowLeft className="h-4 w-4" />
            </button>
          ) : isMobile && !hideClose ? (
            <button onClick={onClose} className="file-panel-icon-btn" title={t('filePanel.close')}>
              <ArrowLeft className="h-4 w-4" />
            </button>
          ) : null}
          <span className="text-sm font-semibold truncate" style={{ color: 'var(--color-text-primary)' }}>
            {showSettings ? t('chat.workspaceSettings') : selectedFile ? (<>{fileName}{hasUnsavedChanges && <span style={{ color: 'var(--color-text-tertiary)' }}> *</span>}</>) : targetDirectory ? `${targetDirectory}/` : t('chat.workspaceFiles')}
          </span>
        </div>
        <div className="flex items-center gap-1">
          {!showSettings && !selectedFile && !selectMode && (
            <>
              {!readOnly && files.length > 0 && (
                <button
                  onClick={() => setSelectMode(true)}
                  className="file-panel-icon-btn"
                  title={t('filePanel.selectFiles')}
                >
                  <CheckSquare className="h-4 w-4" />
                </button>
              )}
              {!readOnly && (
                <>
                  <button
                    onClick={() => fileInputRef.current?.click()}
                    className="file-panel-icon-btn"
                    title={t('filePanel.uploadFile')}
                    disabled={uploadProgress !== null}
                  >
                    <Upload className="h-4 w-4" />
                  </button>
                  <input
                    ref={fileInputRef}
                    type="file"
                    className="hidden"
                    onChange={handleFileInputChange}
                  />
                  <button
                    onClick={handleBackup}
                    className="file-panel-icon-btn"
                    title={t('filePanel.backupFiles')}
                    disabled={backingUp}
                  >
                    <HardDrive className={`h-4 w-4 ${backingUp ? 'animate-pulse' : ''}`} />
                  </button>
                </>
              )}
              {!readOnly && (
                <button
                  onClick={onRefreshFiles}
                  className="file-panel-icon-btn"
                  title={t('filePanel.refresh')}
                >
                  <RefreshCw className={`h-4 w-4 ${filesLoading ? 'animate-spin' : ''}`} />
                </button>
              )}
            </>
          )}
          {!readOnly && !selectedFile && selectMode && (
            <>
              <span className="text-xs" style={{ color: 'var(--color-text-tertiary)', whiteSpace: 'nowrap' }}>
                {selectedPaths.size} selected
              </span>
              <button
                onClick={toggleSelectAll}
                className="file-panel-chip"
                style={{ marginLeft: 2, fontSize: 10, padding: '1px 6px' }}
              >
                {selectedPaths.size === filteredSortedFiles.length ? 'Deselect All' : 'Select All'}
              </button>
              {deleteConfirm ? (
                <button
                  onClick={handleDelete}
                  className="file-panel-delete-confirm-btn"
                  disabled={deleteLoading}
                >
                  Delete {selectedPaths.size}?
                </button>
              ) : (
                <button
                  onClick={handleDelete}
                  className="file-panel-icon-btn"
                  title={t('filePanel.deleteSelected')}
                  disabled={selectedPaths.size === 0 || deleteLoading}
                  style={selectedPaths.size > 0 ? { color: 'var(--color-icon-danger)' } : undefined}
                >
                  <Trash2 className="h-4 w-4" />
                </button>
              )}
              <button onClick={exitSelectMode} className="file-panel-icon-btn" title={t('filePanel.cancelSelection')}>
                <X className="h-4 w-4" />
              </button>
            </>
          )}
          <FileHeaderActions
            selectedFile={selectedFile}
            isEditing={isEditing}
            workspaceId={workspaceId}
            fileContent={fileContent}
            fileMime={fileMime}
            canEdit={canEdit}
            onStartEdit={handleStartEdit}
            onOpenExportModal={() => setExportModalOpen(true)}
            triggerDownloadFn={triggerDownloadFn}
            readFileFullFn={readFileFullFn}
            htmlServedUrl={selectedFile ? apiAdapter?.buildServedUrl?.(selectedFile) : undefined}
            editorRef={editorRef}
            canUndo={canUndo}
            canRedo={canRedo}
            hasUnsavedChanges={hasUnsavedChanges}
            showDiff={showDiff}
            setShowDiff={setShowDiff}
            isSaving={isSaving}
            saveError={saveError}
            onSave={handleSave}
            onCancelEdit={handleCancelEdit}
          />
          {!selectMode && !isEditing && !hideClose && (
            <button onClick={onClose} className="file-panel-icon-btn" title={t('filePanel.close')}>
              <X className="h-4 w-4" />
            </button>
          )}
        </div>
      </div>

      {/* Upload progress bar */}
      {uploadProgress !== null && (
        <div className="file-panel-upload-progress">
          <div className="file-panel-upload-progress-bar" style={{ width: `${uploadProgress}%` }} />
        </div>
      )}

      {uploadError && (
        <div className="file-panel-upload-error">
          <span>{uploadError}</span>
          <button onClick={() => setUploadError(null)} className="file-panel-icon-btn">
            <X className="h-3.5 w-3.5" />
          </button>
        </div>
      )}

      {deleteLoading && <div className="file-panel-progress-indeterminate" />}

      {deleteError && (
        <div className="file-panel-upload-error">
          <span>{deleteError}</span>
          <button onClick={() => setDeleteError(null)} className="file-panel-icon-btn">
            <X className="h-3.5 w-3.5" />
          </button>
        </div>
      )}

      {backupResult && (
        <div className={`file-panel-backup-result ${backupResult.error ? 'error' : ''}`}>
          <span>
            {backupResult.error
              ? backupResult.error
              : `Backed up ${backupResult.synced} file${backupResult.synced !== 1 ? 's' : ''}${backupResult.skipped ? `, ${backupResult.skipped} unchanged` : ''}`}
          </span>
          <button onClick={() => setBackupResult(null)} className="file-panel-icon-btn" style={{ padding: 2 }}>
            <X className="h-3 w-3" />
          </button>
        </div>
      )}

      {backingUp && <div className="file-panel-progress-indeterminate" />}

      {isEditing && (
        <div className="file-panel-edit-hint">
          <Pencil className="h-3 w-3" style={{ flexShrink: 0 }} />
          <span>{t('filePanel.editingHint')}</span>
        </div>
      )}


      {/* Filter & Sort toolbar */}
      {!showSettings && !selectedFile && !filesLoading && !filesError && files.length > 0 && (
        <div className="file-panel-toolbar">
          <div className="file-panel-filter-chips">
            <button className={`file-panel-chip ${filterType === 'All' ? 'active' : ''}`} onClick={() => setFilterType('All')}>
              All
            </button>
            {availableTypes.map((tp) => (
              <button
                key={tp}
                className={`file-panel-chip ${filterType === tp ? 'active' : ''}`}
                onClick={() => setFilterType(filterType === tp ? 'All' : tp)}
              >
                {tp}
              </button>
            ))}
          </div>
          {onToggleSystemFiles && (
            <button
              className={`file-panel-chip ${showSystemFiles ? 'active' : ''}`}
              onClick={onToggleSystemFiles}
              title="Show system directories (.agents/, .system/, tools/, etc.)"
            >
              System
            </button>
          )}
          <div className="file-panel-sort-wrapper" ref={sortMenuRef}>
            <button className="file-panel-icon-btn" title={t('filePanel.sortFiles')} onClick={() => setShowSortMenu((v) => !v)}>
              <ArrowUpDown className="h-3.5 w-3.5" />
            </button>
            {showSortMenu && (
              <div className="file-panel-sort-menu">
                {SORT_OPTIONS.map((opt) => (
                  <div
                    key={opt.value}
                    className={`file-panel-sort-item ${sortBy === opt.value ? 'active' : ''}`}
                    onClick={() => { setSortBy(opt.value); setShowSortMenu(false); }}
                  >
                    {opt.label}
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
      )}

      {/* Workspace settings card */}
      {!showSettings && !selectedFile && !readOnly && !isFlashWorkspace && !selectMode && (
        <div
          className="flex items-center justify-between mx-3 mt-2 mb-1 px-3 py-2 rounded-lg cursor-pointer transition-colors hover:opacity-80"
          style={{ backgroundColor: 'var(--color-bg-card)', border: '1px solid var(--color-border-muted)' }}
          onClick={() => setShowSettings(true)}
        >
          <span className="text-xs truncate" style={{ color: 'var(--color-text-secondary)' }}>
            {workspaceName || t('thread.workspace')}
          </span>
          <Settings className="h-3.5 w-3.5 flex-shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
        </div>
      )}

      {/* Inline settings view */}
      {showSettings ? (
        <div style={{ flex: 1, minHeight: 0, overflow: 'hidden', padding: '0 12px 12px' }}>
          <SandboxSettingsContent workspaceId={workspaceId} />
        </div>
      ) : (
      /* Content */
      <div
        className="file-panel-content-wrapper"
        onDragEnter={!readOnly && !selectedFile ? handleDragEnter : undefined}
        onDragLeave={!readOnly && !selectedFile ? handleDragLeave : undefined}
        onDragOver={!readOnly && !selectedFile ? handleDragOver : undefined}
        onDrop={!readOnly && !selectedFile ? handleDrop : undefined}
        style={{ position: 'relative', flex: 1, minHeight: 0, overflow: 'hidden' }}
      >
        {!readOnly && isDragOver && !selectedFile && (
          <div className="file-panel-drag-overlay">
            <Upload className="h-8 w-8" style={{ color: 'var(--color-accent-primary)' }} />
            <span>Drop file to upload</span>
          </div>
        )}

        <div className="file-panel-content" ref={contentWrapperRef}>
          {selectionTooltip && onAddContext && (
            <div
              className="file-panel-selection-tooltip"
              style={{ left: Math.max(8, selectionTooltip.x - 60), top: Math.max(4, selectionTooltip.y - 32) }}
              onMouseDown={(e: React.MouseEvent) => { e.preventDefault(); e.stopPropagation(); handleAddSelectionContext(); }}
            >
              <TextSelect className="h-3.5 w-3.5" style={{ color: 'var(--color-accent-primary)' }} />
              {selectionTooltip.lineStart != null
                ? (selectionTooltip.lineEnd !== selectionTooltip.lineStart
                    ? t('context.addLinesToContext', { start: selectionTooltip.lineStart, end: selectionTooltip.lineEnd })
                    : t('context.addLineToContext', { line: selectionTooltip.lineStart }))
                : t('context.addToContext')}
            </div>
          )}

          {contextMenu && (
            <div
              className="file-panel-context-menu"
              style={{ left: contextMenu.x, top: contextMenu.y }}
              onMouseDown={(e: React.MouseEvent) => e.stopPropagation()}
            >
              {onAddContext && (
                <div className="file-panel-context-menu-item" onClick={() => handleContextMenuAction('add-context', contextMenu.filePath)}>
                  <TextSelect className="h-3.5 w-3.5" style={{ color: 'var(--color-text-tertiary)' }} />
                  {t('context.addToContext')}
                </div>
              )}
              {memoMimeForName(contextMenu.filePath) && (
                <div className="file-panel-context-menu-item" onClick={() => handleContextMenuAction('add-to-memo', contextMenu.filePath)}>
                  {memoedMap.has(contextMenu.filePath) ? (
                    <>
                      <RefreshCw className="h-3.5 w-3.5" style={{ color: 'var(--color-text-tertiary)' }} />
                      {t('context.syncWithMemo')}
                    </>
                  ) : (
                    <>
                      <ScrollText className="h-3.5 w-3.5" style={{ color: 'var(--color-text-tertiary)' }} />
                      {t('context.addToMemo')}
                    </>
                  )}
                </div>
              )}
              <div className="file-panel-context-menu-item" onClick={() => handleContextMenuAction('open', contextMenu.filePath)}>
                <FolderOpen className="h-3.5 w-3.5" style={{ color: 'var(--color-text-tertiary)' }} />
                {t('context.openFile')}
              </div>
            </div>
          )}

          {selectedFile ? (
            <>
              {memoEntryForSelected && (
                <MemoStaleBanner
                  status={memoStaleStatus}
                  syncing={memoSyncing}
                  onSwitchToMemoTab={onSwitchToMemoTab}
                  onSync={handleSyncMemo}
                  onViewDiff={memoStaleSandboxText !== null ? handleViewMemoDiff : null}
                />
              )}
              {fileLoading ? (
              <div className="p-4">
                <div className="flex items-center justify-center py-12">
                  <RefreshCw className="h-5 w-5 animate-spin" style={{ color: 'var(--color-text-tertiary)' }} />
                </div>
              </div>
            ) : fileError ? (
              <FileErrorDisplay
                error={fileError}
                onRetry={() => handleFileClick(selectedFile)}
                onDownload={() => triggerDownloadFn(workspaceId, selectedFile).catch((err: unknown) => console.error('[FilePanel] Download failed:', err))}
              />
            ) : fileMime === 'pdf' ? (
              <Suspense fallback={<DocumentLoadingFallback />}>
                <DocumentErrorBoundary fallback={<DocumentErrorFallback onDownload={() => triggerDownloadFn(workspaceId, selectedFile).catch((err: unknown) => console.error('[FilePanel] Download failed:', err))} />}>
                  <PdfViewer data={fileArrayBuffer!} />
                </DocumentErrorBoundary>
              </Suspense>
            ) : fileMime === 'excel' ? (
              <Suspense fallback={<DocumentLoadingFallback />}>
                <DocumentErrorBoundary fallback={<DocumentErrorFallback onDownload={() => triggerDownloadFn(workspaceId, selectedFile).catch((err: unknown) => console.error('[FilePanel] Download failed:', err))} />}>
                  <ExcelViewer data={fileArrayBuffer!} />
                </DocumentErrorBoundary>
              </Suspense>
            ) : getFileExtension(selectedFile) === 'csv' ? (
              isEditing ? (
                <div className="file-panel-editor-container">
                  <Suspense fallback={<DocumentLoadingFallback />}>
                    <CodeEditor value={editContent ?? undefined} onChange={handleEditorChange} fileName={selectedFile} diffMode={showDiff} originalValue={originalContent ?? undefined} editorRef={editorRef} onUndoRedoChange={handleUndoRedoChange} onTextSelect={onAddContext ? handleEditorTextSelect : undefined} />
                  </Suspense>
                </div>
              ) : (
                <Suspense fallback={<DocumentLoadingFallback />}>
                  <DocumentErrorBoundary fallback={<DocumentErrorFallback onDownload={() => triggerDownloadFn(workspaceId, selectedFile).catch((err: unknown) => console.error('[FilePanel] Download failed:', err))} />}>
                    <CsvViewer content={fileContent ?? ''} />
                  </DocumentErrorBoundary>
                </Suspense>
              )
            ) : ['html', 'htm'].includes(getFileExtension(selectedFile)) ? (
              <Suspense fallback={<DocumentLoadingFallback />}>
                <DocumentErrorBoundary fallback={<DocumentErrorFallback onDownload={() => triggerDownloadFn(workspaceId, selectedFile).catch((err: unknown) => console.error('[FilePanel] Download failed:', err))} />}>
                  <HtmlViewer
                    content={fileContent ?? ''}
                    fileName={fileName}
                    workspaceId={workspaceId}
                    filePath={selectedFile}
                    servedUrlOverride={apiAdapter?.buildServedUrl?.(selectedFile, { injectTheme: true })}
                    onCopyShareLink={onCopyShareLink ?? undefined}
                    onTriggerDownload={() => triggerDownloadFn(workspaceId, selectedFile).catch((err: unknown) => console.error('[FilePanel] Download failed:', err))}
                  />
                </DocumentErrorBoundary>
              </Suspense>
            ) : isEditing ? (
              <div className="file-panel-editor-container">
                <Suspense fallback={<DocumentLoadingFallback />}>
                  <CodeEditor value={editContent ?? undefined} onChange={handleEditorChange} fileName={selectedFile} diffMode={showDiff} originalValue={originalContent ?? undefined} editorRef={editorRef} onUndoRedoChange={handleUndoRedoChange} onTextSelect={onAddContext ? handleEditorTextSelect : undefined} />
                </Suspense>
              </div>
            ) : (
              <div className="p-4" onMouseUp={handleContentMouseUp}>
                {fileMime === 'image' ? (
                  <>
                    <img src={fileContent!} alt={fileName} className="max-w-full rounded cursor-pointer" onClick={() => setImageLightboxOpen(true)} />
                    <ImageLightbox src={fileContent!} alt={fileName} open={imageLightboxOpen} onClose={() => setImageLightboxOpen(false)} />
                  </>
                ) : selectedFile?.startsWith('/large_tool_results/') ? (
                  <div className="markdown-print-content">
                    <Markdown variant="panel" content={stripLineNumbers(fileContent) ?? ''} className="text-sm" />
                  </div>
                ) : fileMime?.includes('markdown') || getFileExtension(selectedFile) === 'md' ? (
                  <div className="markdown-print-content">
                    <Markdown variant="panel" content={fileContent ?? ''} className="text-sm" />
                  </div>
                ) : (
                  <SyntaxHighlighter
                    language={EXT_TO_LANG[getFileExtension(selectedFile)] || 'text'}
                    style={typeof window !== 'undefined' && document.documentElement.getAttribute('data-theme') === 'light' ? oneLight : oneDark}
                    customStyle={{ margin: 0, padding: 0, backgroundColor: 'transparent', fontSize: '12px', lineHeight: '1.6' }}
                    codeTagProps={{ style: { backgroundColor: 'transparent' } }}
                    showLineNumbers
                    lineNumberStyle={{ minWidth: '2.5em', paddingRight: '1em', color: 'var(--color-text-tertiary)', userSelect: 'none', fontSize: '11px', opacity: 0.5 }}
                    wrapLines
                    lineProps={(lineNumber: number) => ({ 'data-line': lineNumber } as React.HTMLProps<HTMLElement>)}
                    wrapLongLines
                  >
                    {fileContent!}
                  </SyntaxHighlighter>
                )}
              </div>
            )}
            </>
          ) : (
            <div className="py-1 file-tree-root">
              {filesLoading ? (
                Array.from({ length: 5 }).map((_, i) => (
                  <div key={i} className="file-panel-item animate-pulse">
                    <div className="h-4 w-4 rounded" style={{ backgroundColor: 'var(--color-border-muted)' }} />
                    <div className="h-4 flex-1 rounded" style={{ backgroundColor: 'var(--color-border-muted)', width: `${50 + i * 10}%` }} />
                  </div>
                ))
              ) : filesError ? (
                <div className="px-4 py-8 text-center">
                  <p className="text-sm" style={{ color: 'var(--color-text-tertiary)' }}>{filesError}</p>
                </div>
              ) : files.length === 0 ? (
                <div className="px-4 py-8 text-center">
                  <p className="text-sm" style={{ color: 'var(--color-text-tertiary)' }}>No files yet</p>
                </div>
              ) : filteredSortedFiles.length === 0 ? (
                <div className="px-4 py-8 text-center">
                  <p className="text-sm" style={{ color: 'var(--color-text-tertiary)' }}>No {filterType.toLowerCase()} files</p>
                </div>
              ) : (
                fileTree.map((node) => (
                  <DirectoryNode
                    key={node.fullPath}
                    node={node}
                    depth={0}
                    showHeader={node.name !== '/'}
                    expandedDirs={expandedDirs}
                    toggleDir={toggleDir}
                    selectMode={selectMode}
                    selectedPaths={selectedPaths}
                    toggleSelect={toggleSelect}
                    toggleDirSelect={toggleDirSelect}
                    handleFileClick={handleFileClick}
                    readOnly={readOnly}
                    backedUpSet={backedUpSet}
                    modifiedSet={modifiedSet}
                    memoedMap={memoedMap}
                    memoedTitle={memoedTitle}
                    onAddContext={onAddContext}
                    setContextMenu={setContextMenu}
                    activeContextPath={contextMenu?.filePath ?? null}
                  />
                ))
              )}
            </div>
          )}
        </div>
      </div>
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
      {selectedFile && memoEntryForSelected && memoStaleSandboxText !== null && (
        <MemoDiffModal
          open={memoDiffOpen}
          memoKey={memoEntryForSelected.key}
          fileName={selectedFile.split('/').pop() || selectedFile}
          sandboxText={memoStaleSandboxText}
          onClose={() => setMemoDiffOpen(false)}
        />
      )}
    </div>
  );
}

export default FilePanel;
export type { ContextPayload } from './filePanel/types';
export { SYSTEM_DIR_PREFIXES } from './filePanel/fileMeta';
// eslint-disable-next-line react-refresh/only-export-components
export { categorizeFileError } from './filePanel/fileErrors';
export { FileErrorDisplay } from './filePanel/fileErrors';
