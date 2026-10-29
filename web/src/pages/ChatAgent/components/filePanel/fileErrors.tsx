import React from 'react';
import { useTranslation } from 'react-i18next';
import { AlertTriangle } from 'lucide-react';
import { downloadLabel, type DownloadState } from '../../utils/downloadNotice';

// --- File error categorization ---

export type FileErrorCategory =
  | 'not_found'
  | 'not_backed_up'
  | 'sandbox_starting'
  | 'sandbox_unavailable'
  | 'binary_file'
  | 'too_large'
  | 'access_denied'
  | 'unknown';

export interface FileError {
  category: FileErrorCategory;
  detail?: string;
  /** The server's own sentence, shown in place of the category's hint. */
  message?: string;
}

const ERROR_I18N_KEY: Record<FileErrorCategory, string> = {
  not_found: 'notFound',
  not_backed_up: 'notBackedUp',
  sandbox_starting: 'sandboxStarting',
  sandbox_unavailable: 'sandboxUnavailable',
  binary_file: 'binaryFile',
  too_large: 'tooLarge',
  access_denied: 'accessDenied',
  unknown: 'unknown',
};

// eslint-disable-next-line react-refresh/only-export-components
export function categorizeFileError(err: unknown, wsStatus?: string): FileError {
  const response = (err as any)?.response;
  const status: number | undefined = response?.status;
  const raw = response?.data?.detail;
  const detail: string = typeof raw === 'string' ? raw : '';

  switch (status) {
    case 404:
      if (wsStatus === 'stopped' || wsStatus === 'stopping') {
        return { category: 'not_backed_up', detail };
      }
      return { category: 'not_found', detail };
    case 413:
      return { category: 'too_large', detail };
    case 415:
      return { category: 'binary_file', detail };
    case 503:
      if (detail.toLowerCase().includes('starting')) {
        return { category: 'sandbox_starting', detail };
      }
      return { category: 'sandbox_unavailable', detail };
    case 400:
      // Home before its first start or turn has no computer; the open that
      // led here is starting one.
      if (detail.toLowerCase().includes('not on a computer')) {
        return { category: 'sandbox_starting', detail };
      }
      return { category: 'unknown', detail };
    case 403:
      return { category: 'access_denied', detail };
    case 422:
      // An owner's SSH machine that answered and refused, as when our key was
      // taken off it: the server's sentence says what to fix there. A
      // validation refusal carries a list instead, and keeps the generic hint.
      return detail ? { category: 'unknown', detail, message: detail } : { category: 'unknown', detail };
    default:
      return { category: 'unknown', detail };
  }
}

interface FileErrorDisplayProps {
  error: FileError;
  onRetry?: () => void;
  onDownload?: () => void;
  /** A save of this file is being prepared; the button waits for it. */
  downloadState?: DownloadState;
}

export function FileErrorDisplay({ error, onRetry, onDownload, downloadState = 'idle' }: FileErrorDisplayProps): React.ReactElement {
  const { t } = useTranslation();
  const key = ERROR_I18N_KEY[error.category];

  const showRetry = error.category === 'sandbox_starting' || error.category === 'sandbox_unavailable' || error.category === 'unknown';
  const downloadable = error.category === 'binary_file' || error.category === 'too_large';
  const showDownload = downloadable && !!onDownload;
  // These hints end "but you can download it", which is a promise only the
  // button keeps. A share that grants `allow_files` without `allow_download`
  // gets the sentence without that clause instead of a claim it cannot honour.
  const hintKey = downloadable && !onDownload
    ? `filePanel.error.${key}NoDownloadHint`
    : `filePanel.error.${key}Hint`;

  return (
    <div className="flex flex-col items-center justify-center gap-3 py-12">
      <AlertTriangle className="h-6 w-6" style={{ color: 'var(--color-text-tertiary)' }} />
      <p className="text-sm font-medium" style={{ color: 'var(--color-text-secondary)' }}>
        {t(`filePanel.error.${key}`)}
      </p>
      <p className="text-xs text-center max-w-xs" style={{ color: 'var(--color-text-tertiary)' }}>
        {error.message ?? t(hintKey)}
      </p>
      <div className="flex gap-2 mt-1">
        {showRetry && onRetry && (
          <button
            className="text-xs px-3 py-1.5 rounded"
            style={{ background: 'var(--color-bg-elevated)', color: 'var(--color-text-primary)', border: '1px solid var(--color-border-elevated)' }}
            onClick={onRetry}
          >
            {t('filePanel.error.retry')}
          </button>
        )}
        {showDownload && onDownload && (
          <button
            className="text-xs px-3 py-1.5 rounded disabled:opacity-60 disabled:cursor-default"
            style={{ background: 'var(--color-bg-elevated)', color: 'var(--color-text-primary)', border: '1px solid var(--color-border-elevated)' }}
            onClick={onDownload}
            disabled={downloadState !== 'idle'}
          >
            {downloadLabel(downloadState, t('filePanel.error.download'), t)}
          </button>
        )}
      </div>
    </div>
  );
}
