import React, { useRef, useState } from 'react';
import { Code2, Download, Eye, ExternalLink, FileDown, Link2, Pencil, Settings2, X, Undo2, Redo2, FileDiff, FileText, Check, Clipboard } from 'lucide-react';
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
import { copyText } from '@/lib/clipboard';
import { useTranslation } from 'react-i18next';
import { cn } from '@/lib/utils';
import { fileExtension } from '../utils/filePaths';
import { useDownloadState, workspaceDownloadKey } from '../utils/downloadNotice';
import { exportServedPdf } from './viewers/html/useHtmlActions';
import { useServedHtml } from './viewers/html/useServedHtml';
import ShareLinkDialog from './ShareLinkDialog';

const PDF_SCALE_CHOICES = [0.8, 1, 1.25];

// --- File type detection helpers ---

/** The one extension reader; re-exported so this module's callers keep one import. */
export { fileExtension as getFileExtension };

export function isMarkdownFile(filePath: string, mime: string | null): boolean {
  return fileExtension(filePath) === 'md' || (mime?.includes('markdown') ?? false);
}

export function isHtmlFile(filePath: string): boolean {
  return ['html', 'htm'].includes(fileExtension(filePath));
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
  /** Whether this viewer may save the bytes. A copy-link share grants
   *  `allow_files` without `allow_download`, and every item in the menu below
   *  writes a file to disk, so the whole menu goes rather than leaving a
   *  trigger over an empty list. Copy-to-clipboard goes with it: the menu is
   *  labelled and iconed for downloading, and the viewer can still select the
   *  text it is reading. */
  canDownload?: boolean;
  readFileFullFn: (workspaceId: string, filePath: string) => Promise<{ content: string }>;
  /** The serve prefix a share was handed; omitted, the owner's grant serves HTML. */
  servePrefix?: string;
  /** Offer the selected file's share dialog: the owner's own panel only. */
  canShare?: boolean;
  /** An HTML file's view: its markup rather than its page, and the switch between them. */
  htmlSource?: boolean;
  onToggleHtmlSource?: (() => void) | null;
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
  canDownload = true,
  readFileFullFn,
  servePrefix,
  canShare = false,
  htmlSource = false,
  onToggleHtmlSource = null,
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
  const downloadPending = useDownloadState(
    selectedFile ? workspaceDownloadKey(workspaceId, selectedFile) : null,
  ) !== 'idle';
  const [copied, setCopied] = useState(false);
  // Server PDF renders take seconds; ignore re-entry while one is in flight.
  const pdfInFlight = useRef(false);
  const [pdfScale, setPdfScale] = useState(1);
  const [pdfPageNumbers, setPdfPageNumbers] = useState(false);
  const [pdfBranding, setPdfBranding] = useState(true);
  const [shareOpen, setShareOpen] = useState(false);

  const served = useServedHtml(
    workspaceId,
    selectedFile && isHtmlFile(selectedFile) ? selectedFile : null,
    servePrefix,
  );

  const handleExportHtmlPdf = async () => {
    if (!selectedFile || pdfInFlight.current) return;
    if (!served.plainUrl) {
      toast({ description: t('filePanel.pdfFailed') });
      return;
    }
    pdfInFlight.current = true;
    try {
      await exportServedPdf({
        filePath: selectedFile,
        servedUrl: served.plainUrl,
        openUrl: served.openUrl,
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
      await copyText(content);
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
          <Check className={`h-4 w-4 ${isSaving ? 'animate-pulse' : ''}`} />
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

  // One handler for every save in this menu. Four sites carried this same body
  // before, which is four places a new one could copy without the guard above.
  const download = () => triggerDownloadFn(workspaceId, selectedFile).catch((err: unknown) => {
    console.error('[FileHeaderActions] Download failed:', err);
    // A save is the whole interaction: nothing opens, nothing navigates, and the
    // browser shows no file. Without this the menu item is indistinguishable
    // from a dead one. A toast rather than an inline error, because the document
    // the reader is looking at is still fine and should stay on screen.
    toast({ description: t('filePanel.downloadFailed'), variant: 'destructive' });
  });

  const isMd = isMarkdownFile(selectedFile, fileMime);
  const isHtml = isHtmlFile(selectedFile);
  const isText = isTextMime(fileMime);
  const openUrl = isHtml ? served.openUrl : undefined;

  const renderDropdownItems = () => {
    if (isMd) {
      // Markdown file: Download as PDF + Download as Markdown
      return (
        <>
          <DropdownMenuItem onSelect={() => onOpenExportModal()}>
            <FileText className="h-3.5 w-3.5" />
            {t('filePanel.downloadAsPdf')}
          </DropdownMenuItem>
          <DropdownMenuItem onSelect={download} disabled={downloadPending}>
            <Download className="h-3.5 w-3.5" />
            {t('filePanel.downloadAsMarkdown')}
          </DropdownMenuItem>
        </>
      );
    }

    if (isHtml) {
      // HTML file: this menu owns Download + Save-as-PDF; the view actions
      // (source, open in new tab) sit beside it in the header.
      // PDF options toggle component state via preventDefault so the menu
      // stays open while the user composes the export.
      return (
        <>
          <DropdownMenuItem onSelect={download} disabled={downloadPending}>
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
          <DropdownMenuItem onSelect={download} disabled={downloadPending}>
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
      <DropdownMenuItem onSelect={download} disabled={downloadPending}>
        <Download className="h-3.5 w-3.5" />
        {t('filePanel.download')}
      </DropdownMenuItem>
    );
  };

  return (
    <>
      {/* One switch for the two views, showing where it goes. */}
      {isHtml && onToggleHtmlSource && (
        <button
          onClick={onToggleHtmlSource}
          className="file-panel-icon-btn"
          title={htmlSource ? t('filePanel.viewPreview') : t('filePanel.viewSource')}
          aria-label={htmlSource ? t('filePanel.viewPreview') : t('filePanel.viewSource')}
        >
          {htmlSource ? <Eye className="h-4 w-4" /> : <Code2 className="h-4 w-4" />}
        </button>
      )}
      {canShare && (
        <>
          <button
            onClick={() => setShareOpen(true)}
            className="file-panel-icon-btn"
            title={t('filePanel.copyShareLink')}
            aria-label={t('filePanel.copyShareLink')}
          >
            <Link2 className="h-4 w-4" />
          </button>
          {/* Keyed by file so each file's dialog starts fresh, while staying
              mounted through its own close so the exit animation plays. */}
          <ShareLinkDialog
            key={selectedFile}
            open={shareOpen}
            workspaceId={workspaceId}
            filePath={selectedFile}
            onClose={() => setShareOpen(false)}
          />
        </>
      )}
      {/* The file's own page in a new tab: its /a/ link for the owner, the
          share's served bytes on a share. Never the grant URL, which expires. */}
      {openUrl && (
        <button
          onClick={() => window.open(openUrl, '_blank', 'noopener,noreferrer')}
          className="file-panel-icon-btn"
          title={t('filePanel.openInNewTab')}
          aria-label={t('filePanel.openInNewTab')}
        >
          <ExternalLink className="h-4 w-4" />
        </button>
      )}
      {canDownload && (
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
      )}

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
