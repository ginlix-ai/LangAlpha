import React from 'react';
import { useTranslation } from 'react-i18next';
import { AlertTriangle } from 'lucide-react';

// --- File error categorization ---

export type FileErrorCategory =
  | 'not_found'
  | 'not_backed_up'
  | 'sandbox_starting'
  | 'sandbox_unavailable'
  | 'binary_file'
  | 'no_sandbox'
  | 'access_denied'
  | 'unknown';

export interface FileError {
  category: FileErrorCategory;
  detail?: string;
}

const ERROR_I18N_KEY: Record<FileErrorCategory, string> = {
  not_found: 'notFound',
  not_backed_up: 'notBackedUp',
  sandbox_starting: 'sandboxStarting',
  sandbox_unavailable: 'sandboxUnavailable',
  binary_file: 'binaryFile',
  no_sandbox: 'noSandbox',
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
    case 415:
      return { category: 'binary_file', detail };
    case 503:
      if (detail.toLowerCase().includes('starting')) {
        return { category: 'sandbox_starting', detail };
      }
      return { category: 'sandbox_unavailable', detail };
    case 400:
      if (detail.toLowerCase().includes('flash')) {
        return { category: 'no_sandbox', detail };
      }
      return { category: 'unknown', detail };
    case 403:
      return { category: 'access_denied', detail };
    default:
      return { category: 'unknown', detail };
  }
}

interface FileErrorDisplayProps {
  error: FileError;
  onRetry?: () => void;
  onDownload?: () => void;
}

export function FileErrorDisplay({ error, onRetry, onDownload }: FileErrorDisplayProps): React.ReactElement {
  const { t } = useTranslation();
  const key = ERROR_I18N_KEY[error.category];

  const showRetry = error.category === 'sandbox_starting' || error.category === 'sandbox_unavailable' || error.category === 'unknown';
  const showDownload = error.category === 'binary_file';

  return (
    <div className="flex flex-col items-center justify-center gap-3 py-12">
      <AlertTriangle className="h-6 w-6" style={{ color: 'var(--color-text-tertiary)' }} />
      <p className="text-sm font-medium" style={{ color: 'var(--color-text-secondary)' }}>
        {t(`filePanel.error.${key}`)}
      </p>
      <p className="text-xs text-center max-w-xs" style={{ color: 'var(--color-text-tertiary)' }}>
        {t(`filePanel.error.${key}Hint`)}
      </p>
      <div className="flex gap-2 mt-1">
        {showRetry && onRetry && (
          <button
            className="text-xs px-3 py-1.5 rounded"
            style={{ background: 'var(--color-accent-soft)', color: 'var(--color-accent-primary)', border: '1px solid var(--color-accent-overlay)' }}
            onClick={onRetry}
          >
            {t('filePanel.error.retry')}
          </button>
        )}
        {showDownload && onDownload && (
          <button
            className="text-xs px-3 py-1.5 rounded"
            style={{ background: 'var(--color-accent-soft)', color: 'var(--color-accent-primary)', border: '1px solid var(--color-accent-overlay)' }}
            onClick={onDownload}
          >
            {t('filePanel.error.download')}
          </button>
        )}
      </div>
    </div>
  );
}
