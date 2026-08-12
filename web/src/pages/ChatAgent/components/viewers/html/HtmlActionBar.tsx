import { useTranslation } from 'react-i18next';
import { Maximize2, Minimize2, ExternalLink, Download, FileDown, Link2, MoreHorizontal } from 'lucide-react';
import { DropdownMenu, DropdownMenuContent, DropdownMenuTrigger, DropdownMenuItem } from '@/components/ui/dropdown-menu';
import { cn } from '@/lib/utils';
import './HtmlActionBar.css';

interface HtmlActionBarProps {
  onOpenInNewTab: () => void;
  /** Download/PDF live in a secondary "more" menu. Omit both on surfaces where
   *  they're owned elsewhere (the file toolbar defers to the panel header menu). */
  onDownload?: () => void;
  onExportPdf?: () => void | Promise<void>;
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
        onClick={onOpenInNewTab}
        title={t('filePanel.openInNewTab')}
        aria-label={t('filePanel.openInNewTab')}
      >
        <ExternalLink className="h-4 w-4" />
      </button>
      {(onDownload || onExportPdf) && (
        <DropdownMenu modal={false}>
          <DropdownMenuTrigger asChild>
            <button
              type="button"
              className="html-action-btn"
              title={t('filePanel.moreActions')}
              aria-label={t('filePanel.moreActions')}
            >
              <MoreHorizontal className="h-4 w-4" />
            </button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end" sideOffset={4}>
            {onDownload && (
              <DropdownMenuItem onSelect={() => onDownload()}>
                <Download className="h-3.5 w-3.5" />
                {t('filePanel.downloadAsHtml')}
              </DropdownMenuItem>
            )}
            {onExportPdf && (
              <DropdownMenuItem onSelect={() => void onExportPdf()}>
                <FileDown className="h-3.5 w-3.5" />
                {t('filePanel.saveAsPdf')}
              </DropdownMenuItem>
            )}
          </DropdownMenuContent>
        </DropdownMenu>
      )}
    </div>
  );
}
