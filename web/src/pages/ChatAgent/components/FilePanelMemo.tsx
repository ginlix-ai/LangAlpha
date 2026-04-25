import React, { useCallback, useEffect, useMemo, useRef, useState, Suspense } from 'react';
import { HardDrive, Loader2, RefreshCw, ScrollText } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { Dialog, DialogContent, DialogTitle, DialogDescription } from '@/components/ui/dialog';
import { useToast } from '@/components/ui/use-toast';
import { ToastAction } from '@/components/ui/toast';
import { useReadUserMemo, useUploadUserMemo, useUserMemoList } from '../hooks/useMemo';
import type { MemoEntry } from '../utils/api';

const CodeEditor = React.lazy(() => import('./viewers/CodeEditor'));

// Mirror of ACCEPTED_MIME_TYPES in src/ptc_agent/agent/memo/schema.py.
const MEMO_EXT_TO_MIME: Record<string, string> = {
  md: 'text/markdown',
  txt: 'text/plain',
  csv: 'text/csv',
  json: 'application/json',
  pdf: 'application/pdf',
};

export function memoMimeForName(name: string): string | null {
  const i = name.lastIndexOf('.');
  if (i < 0) return null;
  return MEMO_EXT_TO_MIME[name.slice(i + 1).toLowerCase()] ?? null;
}

// Toasts are width-constrained (~360px) and a long filename in quotes wraps
// awkwardly. Elide the middle, preserving the extension so the user can still
// recognize the file: "accelerator_landscape.md" → "accelerator_l…pe.md".
function shortenFilename(name: string, max = 22): string {
  if (name.length <= max) return name;
  const dot = name.lastIndexOf('.');
  if (dot > 0 && name.length - dot <= 6) {
    const ext = name.slice(dot);
    const headLen = Math.max(1, max - ext.length - 1);
    return name.slice(0, headLen) + '…' + ext;
  }
  return name.slice(0, max - 1) + '…';
}

// --- useWorkspaceMemoIndex: workspace path → memo entry, for badge + banner lookup ---

export function useWorkspaceMemoIndex(workspaceId: string): Map<string, MemoEntry> {
  const { data: memoListData } = useUserMemoList(true);
  return useMemo(() => {
    const map = new Map<string, MemoEntry>();
    const entries = memoListData?.entries ?? [];
    for (const entry of entries) {
      if (
        entry.source_kind === 'sandbox'
        && entry.source_workspace_id === workspaceId
        && entry.source_path
      ) {
        map.set(entry.source_path, entry);
      }
    }
    return map;
  }, [memoListData, workspaceId]);
}

// --- useAddToMemo: upload a workspace file into the user's memo store ---

interface UseAddToMemoArgs {
  workspaceId: string;
  downloadFileAsArrayBufferFn: (workspaceId: string, path: string) => Promise<ArrayBuffer>;
  readFileFullFn: (workspaceId: string, path: string) => Promise<{ content: string }>;
  onSwitchToMemoTab: (() => void) | null | undefined;
}

export function useAddToMemo({
  workspaceId,
  downloadFileAsArrayBufferFn,
  readFileFullFn,
  onSwitchToMemoTab,
}: UseAddToMemoArgs): (filePath: string) => Promise<void> {
  const { t } = useTranslation();
  const { toast } = useToast();
  const uploadMemoMutation = useUploadUserMemo();

  return useCallback(async (filePath: string) => {
    const fileName = filePath.split('/').pop() || filePath;
    const mime = memoMimeForName(fileName);
    if (!mime) {
      toast({
        variant: 'destructive',
        title: t('context.memoAddError'),
        description: t('context.memoUnsupportedType'),
      });
      return;
    }
    const shortName = shortenFilename(fileName);
    const loading = toast({
      title: t('context.memoAdding', { name: shortName }),
      description: (
        <span className="inline-flex items-center gap-2 text-xs">
          <Loader2 className="h-3.5 w-3.5 animate-spin" />
          {t('context.memoUploading')}
        </span>
      ),
      duration: Infinity,
    });
    try {
      // For text mimes, route through /files/read (unlimited) so the bytes we
      // upload are exactly what the detail view will later display. Going via
      // /files/download would skip vault-secret redaction and preserve a
      // trailing newline that /files/read strips — making future stale checks
      // false-positive even when the file hasn't changed.
      let file: File;
      if (mime === 'application/pdf') {
        const buf = await downloadFileAsArrayBufferFn(workspaceId, filePath);
        file = new globalThis.File([buf], fileName, { type: mime });
      } else {
        const data = await readFileFullFn(workspaceId, filePath);
        const text = (data?.content as string | undefined) ?? '';
        const bytes = new TextEncoder().encode(text);
        file = new globalThis.File([bytes], fileName, { type: mime });
      }
      const result = await uploadMemoMutation.mutateAsync({
        file,
        source: {
          source_kind: 'sandbox',
          source_workspace_id: workspaceId,
          source_path: filePath,
        },
      });
      loading.dismiss();
      toast({
        title: result.replaced
          ? t('context.memoUpdateSuccess', { name: shortName })
          : t('context.memoAddSuccess', { name: shortName }),
        description: (
          <span className="inline-flex items-center gap-2 text-xs">
            <Loader2 className="h-3.5 w-3.5 animate-spin" />
            {t('context.memoGenerating')}
          </span>
        ),
        action: onSwitchToMemoTab ? (
          <ToastAction
            altText={t('context.viewInMemo')}
            onClick={() => onSwitchToMemoTab()}
          >
            {t('context.viewInMemo')}
          </ToastAction>
        ) : undefined,
        duration: 8000,
      });
    } catch (err: unknown) {
      const e = err as { response?: { data?: { detail?: string } }; message?: string };
      loading.dismiss();
      toast({
        variant: 'destructive',
        title: t('context.memoAddError'),
        description: e?.response?.data?.detail || e?.message || String(err),
        duration: 6000,
      });
    }
  }, [workspaceId, downloadFileAsArrayBufferFn, readFileFullFn, uploadMemoMutation, onSwitchToMemoTab, toast, t]);
}

// --- useMemoStaleCheck: cold-path verdict for the currently selected file ---

export type MemoStaleStatus = 'unknown' | 'checking' | 'fresh' | 'stale';

interface UseMemoStaleCheckArgs {
  workspaceId: string;
  selectedFile: string | null;
  fileMime: string | null | undefined;
  memoSha256: string | null;
  readFileFullFn: (workspaceId: string, path: string) => Promise<{ content: string }>;
}

interface UseMemoStaleCheckResult {
  status: MemoStaleStatus;
  sandboxText: string | null;
  refresh: () => void;
}

export function useMemoStaleCheck({
  workspaceId,
  selectedFile,
  fileMime,
  memoSha256,
  readFileFullFn,
}: UseMemoStaleCheckArgs): UseMemoStaleCheckResult {
  const [status, setStatus] = useState<MemoStaleStatus>('unknown');
  const [sandboxText, setSandboxText] = useState<string | null>(null);
  const [nonce, setNonce] = useState(0);
  // Track which file the last verdict was computed for, so silent re-checks
  // (memoListData polling, fileMime arriving after click) don't flicker the
  // banner CTAs in and out via a transient "checking" state.
  const lastVerdictFileRef = useRef<string | null>(null);

  useEffect(() => {
    if (!selectedFile || !memoSha256) {
      setStatus('unknown');
      setSandboxText(null);
      lastVerdictFileRef.current = selectedFile;
      return;
    }
    if (fileMime === 'pdf' || fileMime === 'image' || fileMime === 'excel') {
      setStatus('unknown');
      setSandboxText(null);
      lastVerdictFileRef.current = selectedFile;
      return;
    }
    const isFileChange = lastVerdictFileRef.current !== selectedFile;
    lastVerdictFileRef.current = selectedFile;
    setStatus((prev) =>
      isFileChange || (prev !== 'fresh' && prev !== 'stale') ? 'checking' : prev
    );
    let cancelled = false;
    (async () => {
      try {
        const data = await readFileFullFn(workspaceId, selectedFile);
        const text = (data?.content as string | undefined) ?? '';
        const bytes = new TextEncoder().encode(text);
        const buf = await crypto.subtle.digest('SHA-256', bytes);
        const hex = Array.from(new Uint8Array(buf))
          .map((b) => b.toString(16).padStart(2, '0'))
          .join('');
        if (cancelled) return;
        setSandboxText(text);
        setStatus(hex === memoSha256 ? 'fresh' : 'stale');
      } catch {
        if (!cancelled) {
          setStatus('unknown');
          setSandboxText(null);
        }
      }
    })();
    return () => { cancelled = true; };
  }, [memoSha256, selectedFile, fileMime, readFileFullFn, workspaceId, nonce]);

  const refresh = useCallback(() => { setNonce((n) => n + 1); }, []);
  return { status, sandboxText, refresh };
}

// --- MemoStaleBanner: detail-view banner with sync / view-diff CTAs ---

interface MemoStaleBannerProps {
  status: MemoStaleStatus;
  syncing: boolean;
  onSwitchToMemoTab: (() => void) | null;
  onSync: (() => void) | null;
  onViewDiff: (() => void) | null;
}

export function MemoStaleBanner({
  status,
  syncing,
  onSwitchToMemoTab,
  onSync,
  onViewDiff,
}: MemoStaleBannerProps): React.ReactElement {
  const { t } = useTranslation();
  const tone = status === 'stale' ? 'stale' : status === 'fresh' ? 'fresh' : 'neutral';
  const message =
    status === 'fresh' ? t('filePanel.memoBanner.fresh')
    : status === 'stale' ? t('filePanel.memoBanner.stale')
    : status === 'checking' ? t('filePanel.memoBanner.checking')
    : t('filePanel.memoBanner.unknown');
  return (
    <div className={`file-panel-memo-banner file-panel-memo-banner-${tone}`}>
      <ScrollText className="h-4 w-4 flex-shrink-0" />
      <span className="text-sm flex-1 truncate">{message}</span>
      <div className="file-panel-memo-banner-actions">
        {status === 'stale' && onViewDiff && (
          <button
            type="button"
            className="file-panel-memo-banner-action"
            onClick={onViewDiff}
            disabled={syncing}
          >
            {t('filePanel.memoBanner.viewDiff')}
          </button>
        )}
        {status === 'stale' && onSync && (
          <button
            type="button"
            className="file-panel-memo-banner-action file-panel-memo-banner-action-primary"
            onClick={onSync}
            disabled={syncing}
          >
            {syncing && <Loader2 className="h-3 w-3 animate-spin" />}
            {syncing ? t('filePanel.memoBanner.syncing') : t('filePanel.memoBanner.sync')}
          </button>
        )}
        {onSwitchToMemoTab && (
          <button
            type="button"
            className="file-panel-memo-banner-action"
            onClick={onSwitchToMemoTab}
            disabled={syncing}
          >
            {t('context.viewInMemo')}
          </button>
        )}
      </div>
    </div>
  );
}

// --- MemoDiffModal: side-by-side compare of saved memo vs. workspace file ---

function DiffSpinner(): React.ReactElement {
  return (
    <div className="flex items-center justify-center h-full">
      <RefreshCw className="h-5 w-5 animate-spin" style={{ color: 'var(--color-text-tertiary)' }} />
    </div>
  );
}

interface MemoDiffModalProps {
  open: boolean;
  memoKey: string | null;
  fileName: string;
  sandboxText: string;
  onClose: () => void;
}

export function MemoDiffModal({
  open,
  memoKey,
  fileName,
  sandboxText,
  onClose,
}: MemoDiffModalProps): React.ReactElement {
  const { t } = useTranslation();
  const { data: memoData, isLoading } = useReadUserMemo(memoKey, open && !!memoKey);
  const memoText = memoData?.content ?? '';

  // Monaco's diff editor splits its panes ~50/50 but the actual seam isn't
  // exactly at the wrapper's midpoint (the change-indicator strip lives there
  // and shifts the visible boundary). Measure the modified pane's DOM offset
  // so the header divider lines up with the real seam under any width.
  const editorWrapperRef = useRef<HTMLDivElement | null>(null);
  const [seamLeftPx, setSeamLeftPx] = useState<number | null>(null);
  useEffect(() => {
    if (!open) return undefined;
    const wrapper = editorWrapperRef.current;
    if (!wrapper) return undefined;

    const measure = () => {
      const modified = wrapper.querySelector(
        '.editor.modified, .modified-in-monaco-diff-editor'
      ) as HTMLElement | null;
      if (!modified) return false;
      const wrapperRect = wrapper.getBoundingClientRect();
      const modRect = modified.getBoundingClientRect();
      const offset = modRect.left - wrapperRect.left;
      if (offset > 0 && offset < wrapperRect.width) {
        setSeamLeftPx(offset);
        return true;
      }
      return false;
    };

    let attempts = 0;
    const pollId = window.setInterval(() => {
      attempts++;
      const ok = measure();
      if (ok || attempts > 30) window.clearInterval(pollId);
    }, 100);
    const observer = new ResizeObserver(() => { measure(); });
    observer.observe(wrapper);

    return () => {
      window.clearInterval(pollId);
      observer.disconnect();
    };
  }, [open, memoText]);

  return (
    <Dialog open={open} onOpenChange={(next) => { if (!next) onClose(); }}>
      <DialogContent
        className="max-w-5xl"
        style={{
          width: '90vw',
          height: '80vh',
          padding: 0,
          display: 'flex',
          flexDirection: 'column',
          backgroundColor: 'var(--color-bg-page)',
          borderColor: 'var(--color-border-muted)',
        }}
      >
        <div
          className="px-4 py-3 border-b"
          style={{ borderColor: 'var(--color-border-muted)' }}
        >
          <DialogTitle className="text-sm font-semibold" style={{ color: 'var(--color-text-primary)' }}>
            {t('filePanel.memoBanner.diffTitle', { name: fileName })}
          </DialogTitle>
          <DialogDescription className="sr-only">
            {t('filePanel.memoBanner.diffSubtitle')}
          </DialogDescription>
        </div>
        <div
          className="flex border-b"
          style={{ borderColor: 'var(--color-border-muted)' }}
        >
          <div
            className="flex items-center gap-2 px-4 py-2 border-r overflow-hidden"
            style={{
              width: seamLeftPx !== null ? `${seamLeftPx}px` : '50%',
              borderColor: 'var(--color-border-muted)',
              backgroundColor: 'var(--color-bg-card)',
            }}
          >
            <ScrollText className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-text-secondary)' }} />
            <span className="text-sm font-medium truncate" style={{ color: 'var(--color-text-primary)' }}>
              {t('filePanel.memoBanner.diffLeftLabel')}
            </span>
            <span className="text-xs truncate" style={{ color: 'var(--color-text-tertiary)' }}>
              {t('filePanel.memoBanner.diffLeftHint')}
            </span>
          </div>
          <div
            className="flex-1 flex items-center gap-2 px-4 py-2 overflow-hidden"
            style={{ backgroundColor: 'var(--color-bg-card)' }}
          >
            <HardDrive className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-text-secondary)' }} />
            <span className="text-sm font-medium truncate" style={{ color: 'var(--color-text-primary)' }}>
              {t('filePanel.memoBanner.diffRightLabel')}
            </span>
            <span className="text-xs truncate" style={{ color: 'var(--color-text-tertiary)' }}>
              {t('filePanel.memoBanner.diffRightHint')}
            </span>
          </div>
        </div>
        <div ref={editorWrapperRef} className="flex-1 min-h-0 overflow-hidden">
          {isLoading ? (
            <DiffSpinner />
          ) : (
            <Suspense fallback={<DiffSpinner />}>
              <CodeEditor
                value={sandboxText}
                fileName={fileName}
                readOnly
                diffMode
                originalValue={memoText}
                height="100%"
              />
            </Suspense>
          )}
        </div>
      </DialogContent>
    </Dialog>
  );
}
