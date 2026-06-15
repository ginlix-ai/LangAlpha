import React, { useRef, useState } from 'react';
import { Download, FileDown, Pencil, Save, Settings2, X, Undo2, Redo2, FileDiff, FileText, Check, Clipboard } from 'lucide-react';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuTrigger,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuSub,
  DropdownMenuSubTrigger,
  DropdownMenuSubContent,
} from '@/components/ui/dropdown-menu';
import { toast } from '@/components/ui/use-toast';
import { useTranslation } from 'react-i18next';
import { cn } from '@/lib/utils';
import { exportServedPdf } from './viewers/html/useHtmlActions';

const PDF_SCALE_CHOICES = [0.8, 1, 1.25];

// --- File type detection helpers ---

export function getFileExtension(fileName: string): string {
  const dot = fileName.lastIndexOf('.');
  return dot >= 0 ? fileName.slice(dot + 1).toLowerCase() : '';
}

export function isMarkdownFile(filePath: string, mime: string | null): boolean {
  return getFileExtension(filePath.split('/').pop() || '') === 'md' || (mime?.includes('markdown') ?? false);
}

export function isHtmlFile(filePath: string): boolean {
  return ['html', 'htm'].includes(getFileExtension(filePath.split('/').pop() || ''));
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
  /** Byte-faithful served URL for the selected HTML file (e.g. the public
   *  share serve URL). Defaults to the wsfiles route when omitted. */
  htmlServedUrl?: string;
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
  fileContent: _fileContent,
  fileMime,
  canEdit,
  onStartEdit,
  onOpenExportModal,
  triggerDownloadFn,
  readFileFullFn,
  htmlServedUrl,
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
  // Server PDF renders take seconds; ignore re-entry while one is in flight.
  const pdfInFlight = useRef(false);
  const [pdfScale, setPdfScale] = useState(1);
  const [pdfPageNumbers, setPdfPageNumbers] = useState(false);
  const [pdfBranding, setPdfBranding] = useState(true);

  const handleExportHtmlPdf = async () => {
    if (!selectedFile || pdfInFlight.current) return;
    pdfInFlight.current = true;
    try {
      await exportServedPdf({
        workspaceId,
        filePath: selectedFile,
        servedUrl: htmlServedUrl,
        printHint: t('filePanel.pdfPrintHint'),
        generatingHint: t('filePanel.pdfGenerating'),
        scale: pdfScale,
        pageNumbers: pdfPageNumbers,
        branding: pdfBranding,
      });
    } finally {
      pdfInFlight.current = false;
    }
  };

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
  const isHtml = isHtmlFile(selectedFile);
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

    if (isHtml) {
      // HTML file: this menu owns Download + Save-as-PDF; the HtmlViewer
      // toolbar keeps only view actions (link/fullscreen/new tab).
      // PDF options toggle component state via preventDefault so the menu
      // stays open while the user composes the export.
      return (
        <>
          <DropdownMenuItem onSelect={() => triggerDownloadFn(workspaceId, selectedFile).catch((err: unknown) => console.error('[FileHeaderActions] Download failed:', err))}>
            <Download className="h-3.5 w-3.5" />
            {t('filePanel.download')}
          </DropdownMenuItem>
          <DropdownMenuItem onSelect={() => void handleExportHtmlPdf()}>
            <FileDown className="h-3.5 w-3.5" />
            {t('filePanel.saveAsPdf')}
          </DropdownMenuItem>
          <DropdownMenuSub>
            <DropdownMenuSubTrigger>
              <Settings2 className="h-3.5 w-3.5" />
              {t('filePanel.pdfOptions')}
            </DropdownMenuSubTrigger>
            <DropdownMenuSubContent>
              <DropdownMenuItem
                onSelect={(e) => {
                  e.preventDefault();
                  setPdfBranding((v) => !v);
                }}
              >
                <Check className={cn('h-3.5 w-3.5', !pdfBranding && 'invisible')} />
                {t('filePanel.pdfBranding')}
              </DropdownMenuItem>
              <DropdownMenuItem
                onSelect={(e) => {
                  e.preventDefault();
                  setPdfPageNumbers((v) => !v);
                }}
              >
                <Check className={cn('h-3.5 w-3.5', !pdfPageNumbers && 'invisible')} />
                {t('filePanel.pdfPageNumbers')}
              </DropdownMenuItem>
              <DropdownMenuSeparator />
              <DropdownMenuLabel>{t('filePanel.pdfScale')}</DropdownMenuLabel>
              {PDF_SCALE_CHOICES.map((scale) => (
                <DropdownMenuItem
                  key={scale}
                  onSelect={(e) => {
                    e.preventDefault();
                    setPdfScale(scale);
                  }}
                >
                  <Check className={cn('h-3.5 w-3.5', pdfScale !== scale && 'invisible')} />
                  {Math.round(scale * 100)}%
                </DropdownMenuItem>
              ))}
            </DropdownMenuSubContent>
          </DropdownMenuSub>
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
