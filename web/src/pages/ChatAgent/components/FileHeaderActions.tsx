import React, { useState } from 'react';
import { Download, Pencil, Save, X, Undo2, Redo2, FileDiff, FileText, Check, Clipboard } from 'lucide-react';
import { DropdownMenu, DropdownMenuContent, DropdownMenuTrigger, DropdownMenuItem } from '@/components/ui/dropdown-menu';
import { toast } from '@/components/ui/use-toast';
import { useTranslation } from 'react-i18next';

// --- File type detection helpers ---

export function getFileExtension(fileName: string): string {
  const dot = fileName.lastIndexOf('.');
  return dot >= 0 ? fileName.slice(dot + 1).toLowerCase() : '';
}

export function isMarkdownFile(filePath: string, mime: string | null): boolean {
  return getFileExtension(filePath.split('/').pop() || '') === 'md' || (mime?.includes('markdown') ?? false);
}

export function isTextMime(mime: string | null): boolean {
  if (!mime) return false;
  if (mime.startsWith('text/')) return true;
  if (['application/json', 'application/yaml', 'application/xml', 'application/javascript', 'application/typescript'].some(t => mime.includes(t))) return true;
  if (mime.includes('markdown')) return true;
  return false;
}

// --- Props ---

interface FileHeaderActionsProps {
  selectedFile: string | null;
  isEditing: boolean;
  workspaceId: string;
  fileContent: string | null;
  fileMime: string | null;
  canEdit: boolean;
  onStartEdit: () => void;
  onOpenExportModal: () => void;
  triggerDownloadFn: (workspaceId: string, filePath: string) => Promise<void>;
  readFileFullFn: (workspaceId: string, filePath: string) => Promise<{ content: string }>;
  // Edit mode callbacks
  editorRef: React.RefObject<any>;
  canUndo: boolean;
  canRedo: boolean;
  hasUnsavedChanges: boolean;
  showDiff: boolean;
  setShowDiff: (fn: (d: boolean) => boolean) => void;
  isSaving: boolean;
  saveError: string | null;
  onSave: () => void;
  onCancelEdit: () => void;
}

// --- Component ---

function FileHeaderActions({
  selectedFile,
  isEditing,
  workspaceId,
  fileContent,
  fileMime,
  canEdit,
  onStartEdit,
  onOpenExportModal,
  triggerDownloadFn,
  readFileFullFn,
  editorRef,
  canUndo,
  canRedo,
  hasUnsavedChanges,
  showDiff,
  setShowDiff,
  isSaving,
  saveError,
  onSave,
  onCancelEdit,
}: FileHeaderActionsProps) {
  const { t } = useTranslation();
  const [copied, setCopied] = useState(false);

  const handleCopy = async () => {
    if (!selectedFile) return;
    try {
      // Fetch full content to avoid copying truncated text for large files
      const { content } = await readFileFullFn(workspaceId, selectedFile);
      await navigator.clipboard.writeText(content);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch {
      toast({ description: t('filePanel.copyFailed'), variant: 'destructive' });
    }
  };

  if (!selectedFile) return null;

  // --- Edit mode ---
  if (isEditing) {
    return (
      <>
        {saveError && (
          <span className="text-xs truncate" style={{ color: 'var(--color-icon-danger)', maxWidth: 120 }} title={saveError}>
            {saveError}
          </span>
        )}
        <button
          onClick={() => editorRef.current?.trigger('toolbar', 'undo', null)}
          className="file-panel-icon-btn"
          title={t('filePanel.undo')}
          disabled={!canUndo}
        >
          <Undo2 className="h-4 w-4" />
        </button>
        <button
          onClick={() => editorRef.current?.trigger('toolbar', 'redo', null)}
          className="file-panel-icon-btn"
          title={t('filePanel.redo')}
          disabled={!canRedo}
        >
          <Redo2 className="h-4 w-4" />
        </button>
        {hasUnsavedChanges && (
          <button
            onClick={() => setShowDiff(d => !d)}
            className={`file-panel-icon-btn ${showDiff ? 'file-panel-icon-btn-active' : ''}`}
            title={showDiff ? t('filePanel.hideDiff') : t('filePanel.showDiff')}
          >
            <FileDiff className="h-4 w-4" />
          </button>
        )}
        <button
          onClick={onSave}
          className="file-panel-icon-btn"
          title={t('filePanel.save')}
          disabled={!hasUnsavedChanges || isSaving}
        >
          <Save className={`h-4 w-4 ${isSaving ? 'animate-pulse' : ''}`} />
        </button>
        <button
          onClick={onCancelEdit}
          className="file-panel-icon-btn"
          title={t('filePanel.cancelEditing')}
        >
          <X className="h-4 w-4" />
        </button>
      </>
    );
  }

  // --- View mode ---

  const isMd = isMarkdownFile(selectedFile, fileMime);
  const isText = isTextMime(fileMime);

  const renderDropdownItems = () => {
    if (isMd) {
      // Markdown file: Download as PDF + Download as Markdown
      return (
        <>
          <DropdownMenuItem onSelect={() => onOpenExportModal()}>
            <FileText className="h-3.5 w-3.5" />
            {t('filePanel.downloadAsPdf')}
          </DropdownMenuItem>
          <DropdownMenuItem onSelect={() => triggerDownloadFn(workspaceId, selectedFile).catch((err: unknown) => console.error('[FileHeaderActions] Download failed:', err))}>
            <Download className="h-3.5 w-3.5" />
            {t('filePanel.downloadAsMarkdown')}
          </DropdownMenuItem>
        </>
      );
    }

    if (isText) {
      // Non-markdown text file: Download + Copy to clipboard
      return (
        <>
          <DropdownMenuItem onSelect={() => triggerDownloadFn(workspaceId, selectedFile).catch((err: unknown) => console.error('[FileHeaderActions] Download failed:', err))}>
            <Download className="h-3.5 w-3.5" />
            {t('filePanel.download')}
          </DropdownMenuItem>
          <DropdownMenuItem onSelect={handleCopy}>
            {copied
              ? <Check className="h-3.5 w-3.5" style={{ color: 'var(--color-success)' }} />
              : <Clipboard className="h-3.5 w-3.5" />
            }
            {copied
              ? (t('filePanel.copiedToClipboard') ?? 'Copied!')
              : (t('filePanel.copyToClipboard') ?? 'Copy to clipboard')
            }
          </DropdownMenuItem>
        </>
      );
    }

    // Binary file: Download only
    return (
      <DropdownMenuItem onSelect={() => triggerDownloadFn(workspaceId, selectedFile).catch((err: unknown) => console.error('[FileHeaderActions] Download failed:', err))}>
        <Download className="h-3.5 w-3.5" />
        {t('filePanel.download')}
      </DropdownMenuItem>
    );
  };

  return (
    <>
      <DropdownMenu modal={false}>
        <DropdownMenuTrigger asChild>
          <button
            className="file-panel-icon-btn"
            aria-label={t('filePanel.downloadOptions') ?? 'Download options'}
          >
            <Download className="h-4 w-4" />
          </button>
        </DropdownMenuTrigger>
        <DropdownMenuContent align="end" sideOffset={4}>
          {renderDropdownItems()}
        </DropdownMenuContent>
      </DropdownMenu>

      {canEdit && (
        <button
          onClick={onStartEdit}
          className="file-panel-icon-btn"
          title={t('filePanel.editFile')}
        >
          <Pencil className="h-4 w-4" />
        </button>
      )}
    </>
  );
}

export default FileHeaderActions;
