import { useTranslation } from 'react-i18next';
import { Maximize2, Minimize2, ExternalLink, Download, FileDown, Code, Link2 } from 'lucide-react';
import { cn } from '@/lib/utils';
import './HtmlActionBar.css';

interface HtmlActionBarProps {
  onOpenInNewTab: () => void;
  onDownload: () => void;
  onExportPdf: () => void;
  onCopySource: () => void;
  /** Copy a shareable link to this report. Omit to hide the link button. */
  onCopyLink?: () => void;
  /** Toggle fullscreen. Omit to hide the expand/exit button. */
  onFullscreen?: () => void;
  isFullscreen?: boolean;
  /** Visual context — 'overlay' for the inline-widget hover bar. */
  variant?: 'toolbar' | 'overlay';
  className?: string;
}

/** Presentational icon-button row for HTML surfaces (widget, file viewer, fullscreen). */
export default function HtmlActionBar({
  onOpenInNewTab,
  onDownload,
  onExportPdf,
  onCopySource,
  onCopyLink,
  onFullscreen,
  isFullscreen = false,
  variant = 'toolbar',
  className,
}: HtmlActionBarProps) {
  const { t } = useTranslation();

  return (
    <div className={cn('html-action-bar', variant === 'overlay' && 'html-action-bar-overlay', className)}>
      {onCopyLink && (
        <button
          type="button"
          className="html-action-btn"
          onClick={onCopyLink}
          title={t('filePanel.copyShareLink')}
          aria-label={t('filePanel.copyShareLink')}
        >
          <Link2 className="h-4 w-4" />
        </button>
      )}
      {onFullscreen && (
        <button
          type="button"
          className="html-action-btn"
          onClick={onFullscreen}
          title={isFullscreen ? t('filePanel.exitFullscreen') : t('filePanel.fullscreen')}
          aria-label={isFullscreen ? t('filePanel.exitFullscreen') : t('filePanel.fullscreen')}
        >
          {isFullscreen ? <Minimize2 className="h-4 w-4" /> : <Maximize2 className="h-4 w-4" />}
        </button>
      )}
      <button
        type="button"
        className="html-action-btn"
        onClick={onCopySource}
        title={t('filePanel.copySource')}
        aria-label={t('filePanel.copySource')}
      >
        <Code className="h-4 w-4" />
      </button>
      <button
        type="button"
        className="html-action-btn"
        onClick={onDownload}
        title={t('filePanel.downloadAsHtml')}
        aria-label={t('filePanel.downloadAsHtml')}
      >
        <Download className="h-4 w-4" />
      </button>
      <button
        type="button"
        className="html-action-btn"
        onClick={onOpenInNewTab}
        title={t('filePanel.openInNewTab')}
        aria-label={t('filePanel.openInNewTab')}
      >
        <ExternalLink className="h-4 w-4" />
      </button>
      <button
        type="button"
        className="html-action-btn"
        onClick={onExportPdf}
        title={t('filePanel.saveAsPdf')}
        aria-label={t('filePanel.saveAsPdf')}
      >
        <FileDown className="h-4 w-4" />
      </button>
    </div>
  );
}
