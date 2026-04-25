import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  AlertTriangle,
  ArrowLeft,
  CheckSquare,
  Download,
  FileText,
  Info,
  Loader2,
  Pencil,
  RefreshCw,
  ScrollText,
  Square,
  Trash2,
  Upload,
  X,
} from 'lucide-react';
import { useTranslation } from 'react-i18next';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogTitle,
} from '@/components/ui/dialog';
import {
  HoverCard,
  HoverCardContent,
  HoverCardTrigger,
} from '@/components/ui/hover-card';
import {
  useDeleteUserMemo,
  useReadUserMemo,
  useRegenerateUserMemo,
  useUploadUserMemo,
  useUserMemoList,
  useWriteUserMemo,
} from '../hooks/useMemo';
import { useWorkspaces } from '../../../hooks/useWorkspaces';
import { useQueryClient } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import {
  deleteUserMemo,
  triggerUserMemoDownload,
  downloadUserMemoBlobUrl,
  type MemoEntry,
  type MemoMetadataStatus,
} from '../utils/api';
import Markdown from './Markdown';
import './FilePanel.css';

// --- Constants -------------------------------------------------------------

const MAX_UPLOAD_BYTES = 5 * 1024 * 1024;

const ACCEPTED_MIME_TYPES = new Set<string>([
  'text/markdown',
  'text/plain',
  'text/csv',
  'application/json',
  'application/pdf',
]);

const ACCEPTED_EXTENSIONS = new Set<string>([
  'md',
  'txt',
  'csv',
  'json',
  'pdf',
]);

const ACCEPT_ATTR = '.md,.txt,.csv,.json,.pdf,text/markdown,text/plain,text/csv,application/json,application/pdf';

// --- Helpers ---------------------------------------------------------------

function formatBytes(n: number): string {
  if (!n) return '0 B';
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / 1024 / 1024).toFixed(2)} MB`;
}

function formatDate(iso: string | null): string {
  if (!iso) return '';
  try {
    const d = new Date(iso);
    const now = new Date();
    const sameYear = d.getFullYear() === now.getFullYear();
    const sameDay = d.toDateString() === now.toDateString();
    if (sameDay) {
      return d.toLocaleTimeString([], {
        hour: '2-digit',
        minute: '2-digit',
      });
    }
    return d.toLocaleDateString(
      [],
      sameYear
        ? { month: 'short', day: 'numeric' }
        : { year: 'numeric', month: 'short', day: 'numeric' },
    );
  } catch {
    return '';
  }
}

function mimeToLabel(mime: string | null | undefined, key: string): string {
  const m = (mime || '').toLowerCase();
  if (m === 'application/pdf') return 'PDF';
  if (m === 'text/markdown') return 'Markdown';
  if (m === 'text/plain') return 'Text';
  if (m === 'text/csv') return 'CSV';
  if (m === 'application/json') return 'JSON';
  const ext = key.split('.').pop()?.toLowerCase() || '';
  if (ext === 'pdf') return 'PDF';
  if (ext === 'md') return 'Markdown';
  if (ext === 'txt') return 'Text';
  if (ext === 'csv') return 'CSV';
  if (ext === 'json') return 'JSON';
  return ext ? ext.toUpperCase() : 'File';
}

function extOf(name: string): string {
  const i = name.lastIndexOf('.');
  return i >= 0 ? name.slice(i + 1).toLowerCase() : '';
}

function isAcceptedFile(file: File): boolean {
  const mime = (file.type || '').toLowerCase();
  if (mime && ACCEPTED_MIME_TYPES.has(mime)) return true;
  // Safari / drag-and-drop sometimes report empty MIME — fall back to ext.
  const ext = extOf(file.name);
  return ACCEPTED_EXTENSIONS.has(ext);
}

function sortByCreatedAsc(entries: MemoEntry[]): MemoEntry[] {
  return [...entries].sort((a, b) => {
    const ta = a.created_at ? new Date(a.created_at).getTime() : 0;
    const tb = b.created_at ? new Date(b.created_at).getTime() : 0;
    if (ta !== tb) return ta - tb;
    return a.key.localeCompare(b.key);
  });
}

function errToString(err: unknown): string {
  if (!err) return '';
  const e = err as {
    response?: { data?: { detail?: string } };
    message?: string;
  };
  return e?.response?.data?.detail || e?.message || String(err);
}

// --- Layout breakpoints (panel-relative, not viewport) -------------------
// The memo panel lives inside RightPanel which the user can resize, so the
// breakpoints are container-relative. Tracked via ResizeObserver.
const BREAK_HIDE_PROVENANCE = 520; // below this, hide the workspace · path subline
const BREAK_HIDE_TYPE = 640;       // below this, hide the Type column
const BREAK_HIDE_SECONDARY = 420;  // below this, hide Size + Uploaded too


function useElementWidth(): {
  ref: React.MutableRefObject<HTMLDivElement | null>;
  width: number;
} {
  const ref = useRef<HTMLDivElement | null>(null);
  const [width, setWidth] = useState(0);
  useEffect(() => {
    const node = ref.current;
    if (!node) return;
    setWidth(node.getBoundingClientRect().width);
    const observer = new ResizeObserver((entries) => {
      const w = entries[0]?.contentRect.width ?? 0;
      setWidth(w);
    });
    observer.observe(node);
    return () => observer.disconnect();
  }, []);
  return { ref, width };
}


// --- Status Badge ---------------------------------------------------------

function StatusBadge({ status }: { status: MemoMetadataStatus | null | undefined }) {
  const { t } = useTranslation();
  if (status === 'ready') {
    return (
      <span
        className="inline-flex items-center rounded-full px-2 py-0.5 text-[10px] font-medium"
        style={{
          backgroundColor: 'var(--color-accent-soft)',
          color: 'var(--color-accent-primary)',
        }}
      >
        {t('memoPanel.status.ready')}
      </span>
    );
  }
  if (status === 'failed') {
    return (
      <span
        className="inline-flex items-center rounded-full px-2 py-0.5 text-[10px] font-medium"
        style={{
          backgroundColor: 'var(--color-loss-soft)',
          color: 'var(--color-loss)',
        }}
      >
        {t('memoPanel.status.failed')}
      </span>
    );
  }
  return (
    <span
      className="inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-[10px] font-medium"
      style={{
        backgroundColor: 'var(--color-border-muted)',
        color: 'var(--color-text-tertiary)',
      }}
    >
      <Loader2 className="h-3 w-3 animate-spin" />
      {t('memoPanel.status.pending')}
    </span>
  );
}

// --- Confirm dialog -------------------------------------------------------

interface ConfirmDialogProps {
  open: boolean;
  title: string;
  body: string;
  confirmLabel: string;
  cancelLabel: string;
  busy?: boolean;
  onConfirm: () => void;
  onCancel: () => void;
}

function ConfirmDialog({
  open,
  title,
  body,
  confirmLabel,
  cancelLabel,
  busy,
  onConfirm,
  onCancel,
}: ConfirmDialogProps) {
  return (
    <Dialog
      open={open}
      onOpenChange={(next) => {
        if (!next) onCancel();
      }}
    >
      <DialogContent
        style={{
          backgroundColor: 'var(--color-bg-page)',
          borderColor: 'var(--color-border-muted)',
        }}
      >
        <div className="flex items-center gap-3">
          <div
            className="w-10 h-10 rounded-full flex items-center justify-center flex-shrink-0"
            style={{ backgroundColor: 'rgba(255, 56, 60, 0.2)' }}
          >
            <AlertTriangle
              className="h-5 w-5"
              style={{ color: 'var(--color-loss)' }}
            />
          </div>
          <DialogTitle
            className="text-lg font-semibold"
            style={{ color: 'var(--color-text-primary)' }}
          >
            {title}
          </DialogTitle>
        </div>
        <DialogDescription asChild>
          <p
            className="text-sm"
            style={{ color: 'var(--color-text-secondary)' }}
          >
            {body}
          </p>
        </DialogDescription>
        <div className="flex gap-3 justify-end pt-2">
          <button
            type="button"
            onClick={onCancel}
            disabled={busy}
            className="px-4 py-2 rounded-md text-sm font-medium transition-colors hover:bg-foreground/10 disabled:opacity-50"
            style={{ color: 'var(--color-text-primary)' }}
          >
            {cancelLabel}
          </button>
          <button
            type="button"
            onClick={onConfirm}
            disabled={busy}
            className="px-4 py-2 rounded-md text-sm font-medium transition-colors disabled:opacity-50"
            style={{
              backgroundColor: busy
                ? 'var(--color-loss-soft)'
                : 'var(--color-loss)',
              color: 'var(--color-text-on-accent)',
            }}
          >
            {confirmLabel}
          </button>
        </div>
      </DialogContent>
    </Dialog>
  );
}

// --- Main component -------------------------------------------------------

export default function MemoPanel() {
  const { t } = useTranslation();

  const list = useUserMemoList(true);
  const uploadMutation = useUploadUserMemo();
  const deleteMutation = useDeleteUserMemo();
  const writeMutation = useWriteUserMemo();
  const regenerateMutation = useRegenerateUserMemo();
  const queryClient = useQueryClient();

  // Resolve workspace_id → name for the provenance subline. Only enable
  // when there's at least one sandbox-sourced memo to avoid the lookup
  // for users who only ever upload from disk.
  const hasSandboxSource = useMemo(
    () =>
      (list.data?.entries ?? []).some(
        (e) => e.source_kind === 'sandbox' && !!e.source_workspace_id,
      ),
    [list.data],
  );
  const { data: wsData } = useWorkspaces({
    limit: 100,
    includeFlash: true,
    enabled: hasSandboxSource,
  });
  const workspaceNameById = useMemo(() => {
    const map = new Map<string, string>();
    const items = (wsData as { workspaces?: { workspace_id: string; name?: string }[] } | undefined)?.workspaces;
    for (const ws of items ?? []) {
      if (ws.workspace_id && ws.name) map.set(ws.workspace_id, ws.name);
    }
    return map;
  }, [wsData]);

  const [selectedKey, setSelectedKey] = useState<string | null>(null);
  const [uploadError, setUploadError] = useState<string | null>(null);
  const [listError, setListError] = useState<string | null>(null);
  const [viewerError, setViewerError] = useState<string | null>(null);
  const [editing, setEditing] = useState(false);
  const [editContent, setEditContent] = useState<string>('');
  const [deleteKey, setDeleteKey] = useState<string | null>(null);
  const [pdfBlobUrl, setPdfBlobUrl] = useState<string | null>(null);

  // Panel width drives the responsive layout below.
  const { ref: bodyRef, width: panelWidth } = useElementWidth();
  const showProvenance = panelWidth === 0 || panelWidth >= BREAK_HIDE_PROVENANCE;
  const showType = panelWidth === 0 || panelWidth >= BREAK_HIDE_TYPE;
  const showSecondary = panelWidth === 0 || panelWidth >= BREAK_HIDE_SECONDARY;

  // Bulk-select state — only relevant in the list view, never in detail.
  const [selectMode, setSelectMode] = useState(false);
  const [selectedKeys, setSelectedKeys] = useState<Set<string>>(new Set());
  const [bulkDeleteOpen, setBulkDeleteOpen] = useState(false);
  const [bulkDeleting, setBulkDeleting] = useState(false);

  const exitSelectMode = useCallback(() => {
    setSelectMode(false);
    setSelectedKeys(new Set());
  }, []);

  const toggleSelectKey = useCallback((key: string) => {
    setSelectedKeys((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  }, []);

  const handleBulkDelete = useCallback(async () => {
    if (selectedKeys.size === 0) return;
    setBulkDeleting(true);
    const keys = Array.from(selectedKeys);
    const failures: string[] = [];
    // Bypass useDeleteUserMemo so we don't fire one list-invalidate (and
    // server-side memo.md rebuild) per key. One invalidate after the loop.
    for (const k of keys) {
      try {
        await deleteUserMemo(k);
      } catch (err) {
        failures.push(`${k}: ${errToString(err)}`);
      }
    }
    queryClient.invalidateQueries({ queryKey: queryKeys.memo.all });
    setBulkDeleting(false);
    setBulkDeleteOpen(false);
    exitSelectMode();
    if (failures.length > 0) {
      setListError(
        t('memoPanel.errors.bulkDeleteFailed', {
          count: failures.length,
          first: failures[0],
        }),
      );
    }
  }, [selectedKeys, queryClient, exitSelectMode, t]);

  // Drag and drop
  const [isDragOver, setIsDragOver] = useState(false);
  const dragCounterRef = useRef(0);
  const fileInputRef = useRef<HTMLInputElement | null>(null);

  const sortedEntries = useMemo(
    () => sortByCreatedAsc(list.data?.entries ?? []),
    [list.data],
  );

  const selectedEntry = useMemo(
    () => sortedEntries.find((e) => e.key === selectedKey) ?? null,
    [sortedEntries, selectedKey],
  );

  const read = useReadUserMemo(selectedKey, !!selectedKey);

  // Surface list errors
  useEffect(() => {
    if (list.error) {
      setListError(errToString(list.error) || t('memoPanel.errors.loadFailed'));
    } else {
      setListError(null);
    }
  }, [list.error, t]);

  // Surface read errors in viewer
  useEffect(() => {
    if (read.error) {
      setViewerError(
        errToString(read.error) || t('memoPanel.errors.readFailed'),
      );
    } else {
      setViewerError(null);
    }
  }, [read.error, t]);

  const isPdf = (selectedEntry?.mime_type || '').toLowerCase() === 'application/pdf';

  // Fetch PDF blob URL when entering viewer for a PDF memo.
  useEffect(() => {
    let cancelled = false;
    let currentUrl: string | null = null;
    if (selectedKey && isPdf) {
      downloadUserMemoBlobUrl(selectedKey)
        .then((url) => {
          if (cancelled) {
            URL.revokeObjectURL(url);
            return;
          }
          currentUrl = url;
          setPdfBlobUrl(url);
        })
        .catch((err) => {
          if (cancelled) return;
          setViewerError(
            errToString(err) || t('memoPanel.errors.readFailed'),
          );
        });
    } else {
      setPdfBlobUrl(null);
    }
    return () => {
      cancelled = true;
      if (currentUrl) URL.revokeObjectURL(currentUrl);
    };
  }, [selectedKey, isPdf, t]);

  // --- Upload handlers -----------------------------------------------------

  const doUpload = useCallback(
    async (file: File) => {
      setUploadError(null);
      if (!isAcceptedFile(file)) {
        setUploadError(t('memoPanel.errors.unsupportedType'));
        return;
      }
      if (file.size > MAX_UPLOAD_BYTES) {
        const mb = (file.size / (1024 * 1024)).toFixed(1);
        setUploadError(t('memoPanel.errors.fileTooLarge', { size: mb }));
        return;
      }
      try {
        await uploadMutation.mutateAsync(file);
      } catch (err) {
        setUploadError(
          errToString(err) || t('memoPanel.errors.uploadFailed'),
        );
      }
    },
    [uploadMutation, t],
  );

  const handleFileInputChange = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      const file = e.target.files?.[0];
      if (file) void doUpload(file);
      if (fileInputRef.current) fileInputRef.current.value = '';
    },
    [doUpload],
  );

  const handleDragEnter = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    dragCounterRef.current++;
    if (e.dataTransfer.types.includes('Files')) {
      setIsDragOver(true);
    }
  }, []);

  const handleDragLeave = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    dragCounterRef.current--;
    if (dragCounterRef.current === 0) setIsDragOver(false);
  }, []);

  const handleDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
  }, []);

  const handleDrop = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault();
      e.stopPropagation();
      dragCounterRef.current = 0;
      setIsDragOver(false);
      const file = e.dataTransfer.files?.[0];
      if (file) void doUpload(file);
    },
    [doUpload],
  );

  // --- Viewer actions ------------------------------------------------------

  const handleOpen = useCallback((key: string) => {
    setSelectedKey(key);
    setViewerError(null);
    setEditing(false);
    setEditContent('');
  }, []);

  const handleBackToList = useCallback(() => {
    setSelectedKey(null);
    setViewerError(null);
    setEditing(false);
    setEditContent('');
  }, []);

  const handleDownload = useCallback(
    async (key: string, filename: string | null) => {
      try {
        await triggerUserMemoDownload(key, filename);
      } catch (err) {
        setViewerError(
          errToString(err) || t('memoPanel.errors.downloadFailed'),
        );
      }
    },
    [t],
  );

  const handleStartEdit = useCallback(() => {
    if (!read.data) return;
    setEditContent(read.data.content || '');
    setEditing(true);
  }, [read.data]);

  const handleCancelEdit = useCallback(() => {
    setEditing(false);
    setEditContent('');
  }, []);

  const handleSaveEdit = useCallback(async () => {
    if (!selectedKey) return;
    try {
      await writeMutation.mutateAsync({
        key: selectedKey,
        content: editContent,
      });
      setEditing(false);
    } catch (err) {
      setViewerError(errToString(err) || t('memoPanel.errors.saveFailed'));
    }
  }, [selectedKey, editContent, writeMutation, t]);

  const handleRegenerate = useCallback(
    async (key: string) => {
      try {
        await regenerateMutation.mutateAsync(key);
      } catch (err) {
        setViewerError(
          errToString(err) || t('memoPanel.errors.regenerateFailed'),
        );
      }
    },
    [regenerateMutation, t],
  );

  const handleConfirmDelete = useCallback(async () => {
    if (!deleteKey) return;
    try {
      await deleteMutation.mutateAsync(deleteKey);
      if (selectedKey === deleteKey) {
        handleBackToList();
      }
      setDeleteKey(null);
    } catch (err) {
      setViewerError(
        errToString(err) || t('memoPanel.errors.deleteFailed'),
      );
    }
  }, [deleteKey, deleteMutation, selectedKey, handleBackToList, t]);

  // --- Viewer render --------------------------------------------------------

  if (selectedEntry) {
    const displayName =
      selectedEntry.original_filename || selectedEntry.key;
    const status = (read.data?.metadata_status ||
      selectedEntry.metadata_status ||
      null) as MemoMetadataStatus | null;
    const content = read.data?.content ?? '';
    const ext = extOf(selectedEntry.key);
    const canEdit = !isPdf;
    const isMarkdown =
      ext === 'md' ||
      (selectedEntry.mime_type || '').toLowerCase() === 'text/markdown';

    return (
      <div
        className="flex flex-col h-full"
        style={{ backgroundColor: 'var(--color-bg-page)' }}
      >
        {/* Header */}
        <div
          className="flex items-center justify-between gap-2 px-3 py-2 border-b"
          style={{ borderColor: 'var(--color-border-muted)' }}
        >
          <div className="flex items-center gap-2 min-w-0">
            <button
              onClick={handleBackToList}
              className="file-panel-icon-btn"
              title={t('memoPanel.backToList')}
            >
              <ArrowLeft className="h-4 w-4" />
            </button>
            <FileText
              className="h-4 w-4 flex-shrink-0"
              style={{ color: 'var(--color-text-tertiary)' }}
            />
            <span
              className="text-sm font-semibold truncate"
              style={{ color: 'var(--color-text-primary)' }}
              title={displayName}
            >
              {displayName}
            </span>
            <StatusBadge status={status} />
          </div>
          <div className="flex items-center gap-1 flex-shrink-0">
            {status === 'failed' && (
              <button
                onClick={() => handleRegenerate(selectedEntry.key)}
                className="file-panel-icon-btn"
                title={t('memoPanel.actions.regenerate')}
                disabled={regenerateMutation.isPending}
              >
                <RefreshCw
                  className={`h-4 w-4 ${
                    regenerateMutation.isPending ? 'animate-spin' : ''
                  }`}
                />
              </button>
            )}
            <button
              onClick={() =>
                handleDownload(
                  selectedEntry.key,
                  selectedEntry.original_filename,
                )
              }
              className="file-panel-icon-btn"
              title={t('memoPanel.actions.download')}
            >
              <Download className="h-4 w-4" />
            </button>
            {canEdit && !editing && (
              <button
                onClick={handleStartEdit}
                className="file-panel-icon-btn"
                title={t('memoPanel.actions.edit')}
                disabled={read.isLoading || !read.data}
              >
                <Pencil className="h-4 w-4" />
              </button>
            )}
            <button
              onClick={() => setDeleteKey(selectedEntry.key)}
              className="file-panel-icon-btn"
              title={t('memoPanel.actions.delete')}
            >
              <Trash2 className="h-4 w-4" />
            </button>
          </div>
        </div>

        {/* Body */}
        <div className="flex-1 overflow-y-auto px-4 py-3">
          {viewerError && (
            <div
              className="mb-3 px-3 py-2 rounded text-xs"
              style={{
                backgroundColor: 'var(--color-loss-soft)',
                color: 'var(--color-loss)',
              }}
            >
              {viewerError}
            </div>
          )}

          {status === 'failed' && read.data?.metadata_error && (
            <div
              className="mb-3 px-3 py-2 rounded text-xs"
              style={{
                backgroundColor: 'var(--color-loss-soft)',
                color: 'var(--color-loss)',
              }}
            >
              {t('memoPanel.metadataFailed')}
            </div>
          )}

          {read.data?.summary && !editing && (
            <div
              className="mb-4 px-3 py-2 rounded text-xs"
              style={{
                backgroundColor: 'var(--color-bg-card)',
                color: 'var(--color-text-secondary)',
                border: '1px solid var(--color-border-muted)',
              }}
            >
              {read.data.summary}
            </div>
          )}

          {isPdf ? (
            pdfBlobUrl ? (
              <object
                data={pdfBlobUrl}
                type="application/pdf"
                className="w-full"
                style={{ height: 'calc(100vh - 220px)', minHeight: 400 }}
              >
                <div
                  className="text-xs"
                  style={{ color: 'var(--color-text-tertiary)' }}
                >
                  {t('memoPanel.pdfFallback')}{' '}
                  <button
                    onClick={() =>
                      handleDownload(
                        selectedEntry.key,
                        selectedEntry.original_filename,
                      )
                    }
                    className="underline"
                    style={{ color: 'var(--color-accent-primary)' }}
                  >
                    {t('memoPanel.actions.download')}
                  </button>
                </div>
              </object>
            ) : (
              <div
                className="flex items-center justify-center py-10"
                style={{ color: 'var(--color-text-tertiary)' }}
              >
                <Loader2 className="h-5 w-5 animate-spin" />
              </div>
            )
          ) : editing ? (
            <div className="flex flex-col gap-2">
              <textarea
                value={editContent}
                onChange={(e) => setEditContent(e.target.value)}
                className="w-full font-mono text-xs rounded-md p-3 resize-y"
                style={{
                  minHeight: 320,
                  backgroundColor: 'var(--color-bg-card)',
                  color: 'var(--color-text-primary)',
                  border: '1px solid var(--color-border-muted)',
                }}
              />
              <div className="flex gap-2 justify-end">
                <button
                  onClick={handleCancelEdit}
                  disabled={writeMutation.isPending}
                  className="px-3 py-1.5 rounded text-xs font-medium"
                  style={{
                    color: 'var(--color-text-primary)',
                    border: '1px solid var(--color-border-muted)',
                  }}
                >
                  {t('memoPanel.actions.cancel')}
                </button>
                <button
                  onClick={handleSaveEdit}
                  disabled={writeMutation.isPending}
                  className="px-3 py-1.5 rounded text-xs font-medium"
                  style={{
                    backgroundColor: 'var(--color-accent-primary)',
                    color: 'var(--color-text-on-accent)',
                  }}
                >
                  {writeMutation.isPending ? (
                    <Loader2 className="h-3 w-3 animate-spin" />
                  ) : (
                    t('memoPanel.actions.save')
                  )}
                </button>
              </div>
            </div>
          ) : read.isLoading ? (
            <div
              className="text-xs"
              style={{ color: 'var(--color-text-tertiary)' }}
            >
              {t('memoPanel.loading')}
            </div>
          ) : isMarkdown ? (
            <Markdown content={content} variant="panel" />
          ) : (
            <pre
              className="whitespace-pre-wrap break-words text-xs font-mono"
              style={{ color: 'var(--color-text-primary)' }}
            >
              {content}
            </pre>
          )}
        </div>

        <ConfirmDialog
          open={!!deleteKey}
          title={t('memoPanel.confirm.deleteTitle')}
          body={t('memoPanel.confirm.deleteBody', {
            name:
              selectedEntry.original_filename || selectedEntry.key,
          })}
          confirmLabel={t('memoPanel.confirm.deleteConfirm')}
          cancelLabel={t('memoPanel.confirm.deleteCancel')}
          busy={deleteMutation.isPending}
          onConfirm={handleConfirmDelete}
          onCancel={() => setDeleteKey(null)}
        />
      </div>
    );
  }

  // --- List render ---------------------------------------------------------

  const isEmpty = !list.isLoading && sortedEntries.length === 0;

  return (
    <div
      className="flex flex-col h-full"
      style={{ backgroundColor: 'var(--color-bg-page)' }}
      onDragEnter={handleDragEnter}
      onDragLeave={handleDragLeave}
      onDragOver={handleDragOver}
      onDrop={handleDrop}
    >
      {/* Header */}
      <div
        className="flex items-center justify-between px-3 py-2 border-b"
        style={{ borderColor: 'var(--color-border-muted)' }}
      >
        <div className="flex items-center gap-1.5 min-w-0">
          <span
            className="text-sm font-semibold"
            style={{ color: 'var(--color-text-primary)' }}
          >
            {t('memoPanel.title')}
          </span>
          <HoverCard openDelay={150} closeDelay={100}>
            <HoverCardTrigger asChild>
              <button
                type="button"
                className="inline-flex flex-shrink-0 cursor-help"
                aria-label={t('memoPanel.uploadHint')}
              >
                <Info
                  className="h-3.5 w-3.5"
                  style={{ color: 'var(--color-text-tertiary)' }}
                />
              </button>
            </HoverCardTrigger>
            <HoverCardContent
              align="start"
              side="bottom"
              className="w-auto max-w-xs px-3 py-2 text-xs"
              style={{
                backgroundColor: 'var(--color-bg-card)',
                borderColor: 'var(--color-border-muted)',
                color: 'var(--color-text-secondary)',
              }}
            >
              {t('memoPanel.uploadHint')}
            </HoverCardContent>
          </HoverCard>
        </div>
        <div className="flex items-center gap-1 flex-shrink-0">
          {selectMode ? (
            <>
              <span
                className="text-xs mr-2"
                style={{ color: 'var(--color-text-secondary)' }}
              >
                {t('memoPanel.selectedCount', { count: selectedKeys.size })}
              </span>
              <button
                onClick={() => setBulkDeleteOpen(true)}
                disabled={selectedKeys.size === 0}
                className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded text-xs font-medium disabled:opacity-50"
                style={{
                  backgroundColor: 'var(--color-loss)',
                  color: 'var(--color-text-on-accent)',
                }}
              >
                <Trash2 className="h-3.5 w-3.5" />
                {t('memoPanel.deleteSelected')}
              </button>
              <button
                onClick={exitSelectMode}
                className="inline-flex items-center px-2.5 py-1 rounded text-xs font-medium"
                style={{ color: 'var(--color-text-primary)' }}
              >
                {t('memoPanel.actions.cancel')}
              </button>
            </>
          ) : (
            <>
              <button
                onClick={() => list.refetch()}
                className="file-panel-icon-btn"
                title={t('memoPanel.refresh')}
                disabled={list.isFetching}
              >
                <RefreshCw
                  className={`h-4 w-4 ${list.isFetching ? 'animate-spin' : ''}`}
                />
              </button>
              <button
                onClick={() => setSelectMode(true)}
                className="file-panel-icon-btn"
                title={t('memoPanel.selectMode')}
                disabled={sortedEntries.length === 0}
              >
                <CheckSquare className="h-4 w-4" />
              </button>
              <button
                onClick={() => fileInputRef.current?.click()}
                className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded text-xs font-medium"
                style={{
                  backgroundColor: 'var(--color-accent-primary)',
                  color: 'var(--color-text-on-accent)',
                }}
                disabled={uploadMutation.isPending}
              >
                {uploadMutation.isPending ? (
                  <Loader2 className="h-3.5 w-3.5 animate-spin" />
                ) : (
                  <Upload className="h-3.5 w-3.5" />
                )}
                {uploadMutation.isPending
                  ? t('memoPanel.uploading')
                  : t('memoPanel.uploadButton')}
              </button>
              <input
                ref={fileInputRef}
                type="file"
                className="hidden"
                accept={ACCEPT_ATTR}
                onChange={handleFileInputChange}
              />
            </>
          )}
        </div>
      </div>

      {uploadError && (
        <div
          className="flex items-start justify-between gap-2 px-3 py-2 text-xs"
          style={{
            backgroundColor: 'var(--color-loss-soft)',
            color: 'var(--color-loss)',
          }}
        >
          <span className="min-w-0">{uploadError}</span>
          <button
            onClick={() => setUploadError(null)}
            className="flex-shrink-0"
            title={t('memoPanel.actions.cancel')}
          >
            <X className="h-3.5 w-3.5" />
          </button>
        </div>
      )}

      {listError && (
        <div
          className="px-3 py-2 text-xs"
          style={{
            backgroundColor: 'var(--color-loss-soft)',
            color: 'var(--color-loss)',
          }}
        >
          {listError}
        </div>
      )}

      {/* Body */}
      <div
        ref={bodyRef}
        className="flex-1 min-h-0 overflow-y-auto relative"
      >
        {isDragOver && (
          <div className="file-panel-drag-overlay">
            <Upload
              className="h-8 w-8"
              style={{ color: 'var(--color-accent-primary)' }}
            />
            <span>{t('memoPanel.dragOverlayText')}</span>
          </div>
        )}

        {list.isLoading && sortedEntries.length === 0 && (
          <div
            className="px-3 py-6 text-xs text-center"
            style={{ color: 'var(--color-text-tertiary)' }}
          >
            {t('memoPanel.loadingList')}
          </div>
        )}

        {isEmpty && !listError && (
          <div
            className="px-4 py-10 flex flex-col items-center gap-3 text-center"
            style={{ color: 'var(--color-text-tertiary)' }}
          >
            <ScrollText className="h-10 w-10 opacity-40" />
            <div
              className="text-sm font-medium"
              style={{ color: 'var(--color-text-primary)' }}
            >
              {t('memoPanel.empty.title')}
            </div>
            <div className="text-xs max-w-[18rem]">
              {t('memoPanel.empty.hint')}
            </div>
          </div>
        )}

        {sortedEntries.length > 0 && (
          <table className="w-full text-xs">
            <thead>
              <tr
                className="text-left"
                style={{ color: 'var(--color-text-tertiary)' }}
              >
                {selectMode && (
                  <th className="font-medium px-2 py-2 w-6" />
                )}
                <th className="font-medium px-3 py-2">
                  {t('memoPanel.columns.name')}
                </th>
                {showType && (
                  <th className="font-medium px-2 py-2 whitespace-nowrap">
                    {t('memoPanel.columns.type')}
                  </th>
                )}
                {showSecondary && (
                  <th className="font-medium px-2 py-2 whitespace-nowrap">
                    {t('memoPanel.columns.size')}
                  </th>
                )}
                <th className="font-medium px-2 py-2 whitespace-nowrap">
                  {t('memoPanel.columns.status')}
                </th>
                <th className="font-medium px-2 py-2">
                  {t('memoPanel.columns.description')}
                </th>
                {showSecondary && (
                  <th className="font-medium px-2 py-2 whitespace-nowrap">
                    {t('memoPanel.columns.uploaded')}
                  </th>
                )}
              </tr>
            </thead>
            <tbody>
              {sortedEntries.map((entry) => {
                const displayName =
                  entry.original_filename || entry.key;
                const isSelected = selectedKeys.has(entry.key);
                const isSandboxSourced =
                  entry.source_kind === 'sandbox' && entry.source_path;
                const workspaceLabel = isSandboxSourced
                  ? (entry.source_workspace_id
                      ? workspaceNameById.get(entry.source_workspace_id)
                      : null) || t('memoPanel.source.sandbox')
                  : null;
                const sourceTitle = isSandboxSourced
                  ? `${workspaceLabel} · ${entry.source_path}`
                  : displayName;
                return (
                  <tr
                    key={entry.key}
                    onClick={() =>
                      selectMode ? toggleSelectKey(entry.key) : handleOpen(entry.key)
                    }
                    className="cursor-pointer transition-colors"
                    style={{
                      color: 'var(--color-text-primary)',
                      backgroundColor:
                        selectMode && isSelected
                          ? 'var(--color-accent-soft)'
                          : 'transparent',
                    }}
                    onMouseEnter={(e) => {
                      if (selectMode && isSelected) return;
                      (e.currentTarget as HTMLTableRowElement).style.backgroundColor =
                        'var(--color-border-muted)';
                    }}
                    onMouseLeave={(e) => {
                      (e.currentTarget as HTMLTableRowElement).style.backgroundColor =
                        selectMode && isSelected
                          ? 'var(--color-accent-soft)'
                          : 'transparent';
                    }}
                  >
                    {selectMode && (
                      <td className="px-2 py-2 w-6">
                        {isSelected ? (
                          <CheckSquare
                            className="h-4 w-4"
                            style={{ color: 'var(--color-accent-primary)' }}
                          />
                        ) : (
                          <Square
                            className="h-4 w-4"
                            style={{ color: 'var(--color-text-tertiary)' }}
                          />
                        )}
                      </td>
                    )}
                    <td className="px-3 py-2">
                      <div className="flex items-center gap-2 min-w-0">
                        <FileText
                          className="h-4 w-4 flex-shrink-0"
                          style={{ color: 'var(--color-text-tertiary)' }}
                        />
                        <div className="min-w-0">
                          <div
                            className="truncate max-w-[14rem]"
                            title={sourceTitle}
                          >
                            {displayName}
                          </div>
                          {isSandboxSourced && showProvenance && (
                            <div
                              className="truncate max-w-[14rem] text-[10px]"
                              style={{ color: 'var(--color-text-tertiary)' }}
                              title={sourceTitle}
                            >
                              {workspaceLabel} · {entry.source_path}
                            </div>
                          )}
                        </div>
                      </div>
                    </td>
                    {showType && (
                      <td
                        className="px-2 py-2 whitespace-nowrap"
                        style={{ color: 'var(--color-text-tertiary)' }}
                      >
                        {mimeToLabel(entry.mime_type, entry.key)}
                      </td>
                    )}
                    {showSecondary && (
                      <td
                        className="px-2 py-2 whitespace-nowrap"
                        style={{ color: 'var(--color-text-tertiary)' }}
                      >
                        {formatBytes(entry.size_bytes)}
                      </td>
                    )}
                    <td className="px-2 py-2 whitespace-nowrap">
                      <StatusBadge status={entry.metadata_status} />
                    </td>
                    <td
                      className="px-2 py-2"
                      style={{ color: 'var(--color-text-secondary)' }}
                    >
                      <div className="line-clamp-2 max-w-[20rem]">
                        {entry.description || ''}
                      </div>
                    </td>
                    {showSecondary && (
                      <td
                        className="px-2 py-2 whitespace-nowrap"
                        style={{ color: 'var(--color-text-tertiary)' }}
                      >
                        {formatDate(entry.created_at)}
                      </td>
                    )}
                  </tr>
                );
              })}
            </tbody>
          </table>
        )}
      </div>

      <ConfirmDialog
        open={bulkDeleteOpen}
        title={t('memoPanel.confirm.bulkDeleteTitle')}
        body={t('memoPanel.confirm.bulkDeleteBody', {
          count: selectedKeys.size,
        })}
        confirmLabel={t('memoPanel.confirm.deleteConfirm')}
        cancelLabel={t('memoPanel.confirm.deleteCancel')}
        busy={bulkDeleting}
        onConfirm={handleBulkDelete}
        onCancel={() => setBulkDeleteOpen(false)}
      />
    </div>
  );
}
