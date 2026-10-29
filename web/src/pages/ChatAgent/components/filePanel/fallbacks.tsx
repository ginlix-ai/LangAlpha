import React from 'react';
import { useTranslation } from 'react-i18next';
import { AlertTriangle } from 'lucide-react';
import { Loader } from '@/components/ui/loader';
import { downloadLabel, type DownloadState } from '../../utils/downloadNotice';

export function DocumentLoadingFallback(): React.ReactElement {
  return (
    <div className="flex items-center justify-center py-12">
      <Loader size={20} className="text-(--color-text-tertiary)" />
    </div>
  );
}

interface DocumentErrorFallbackProps {
  /** Given where the failure may be the network's rather than the file's. */
  onRetry?: () => void;
  /** Omitted where the viewer may not save the bytes, which leaves the
   *  explanation without the offer it cannot honour. */
  onDownload?: () => void;
  downloadState?: DownloadState;
}

const BUTTON_STYLE: React.CSSProperties = {
  background: 'var(--color-bg-elevated)', color: 'var(--color-text-primary)', border: '1px solid var(--color-border-elevated)',
};

export function DocumentErrorFallback({ onRetry, onDownload, downloadState = 'idle' }: DocumentErrorFallbackProps): React.ReactElement {
  const { t } = useTranslation();
  return (
    <div className="flex flex-col items-center justify-center gap-3 py-12">
      <AlertTriangle className="h-6 w-6" style={{ color: 'var(--color-text-tertiary)' }} />
      <p className="text-sm" style={{ color: 'var(--color-text-tertiary)' }}>{t('filePanel.error.previewFailed')}</p>
      {(onRetry || onDownload) && (
        <div className="flex gap-2">
          {onRetry && (
            <button className="text-xs px-3 py-1.5 rounded" style={BUTTON_STYLE} onClick={onRetry}>
              {t('filePanel.error.retry')}
            </button>
          )}
          {onDownload && (
            <button
              className="text-xs px-3 py-1.5 rounded disabled:opacity-60 disabled:cursor-default"
              style={BUTTON_STYLE}
              onClick={onDownload}
              disabled={downloadState !== 'idle'}
            >
              {downloadLabel(downloadState, t('filePanel.error.download'), t)}
            </button>
          )}
        </div>
      )}
    </div>
  );
}
