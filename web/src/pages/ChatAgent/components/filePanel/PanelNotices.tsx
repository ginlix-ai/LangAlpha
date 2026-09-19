import React from 'react';
import { Pencil, RefreshCw, X } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import type { BackupResult } from './types';

interface PanelNoticesProps {
  /** 0–100 while a file is going up, null when nothing is. */
  uploadProgress: number | null;
  error: string | null;
  onDismissError: () => void;
  /** The listing failed and the tree that would say so is not on screen. */
  filesError?: string | null;
  /** The restore is short and the tree that would say so is not on screen. */
  filesRestoreIncomplete?: boolean;
  onRefreshFiles?: () => void;
  /** A delete or a backup is running; neither reports a percentage. */
  busy: boolean;
  backupResult: BackupResult | null;
  onDismissBackupResult: () => void;
  editing: boolean;
}

/**
 * The strip of notices between the panel's header rows and its body: uploads,
 * deletes, backups and the edit-mode reminder.
 *
 * They stack rather than replace each other because each belongs to a
 * different actor — an upload the reader started, a delete they confirmed, a
 * backup they asked for — and losing one behind another is losing the only
 * report that action ever makes.
 */
export function PanelNotices({
  uploadProgress, error, onDismissError, filesError = null, filesRestoreIncomplete = false, onRefreshFiles, busy, backupResult, onDismissBackupResult, editing,
}: PanelNoticesProps): React.ReactElement {
  const { t } = useTranslation();
  return (
    <>
      {uploadProgress !== null && (
        <div className="file-panel-upload-progress">
          <div className="file-panel-upload-progress-bar" style={{ width: `${uploadProgress}%` }} />
        </div>
      )}
      {!filesError && filesRestoreIncomplete && (
        <p className="px-3 pt-2 pb-1.5 text-xs" style={{ color: 'var(--color-text-secondary)' }}>
          {t('filePanel.restoreIncomplete')}
        </p>
      )}
      {filesError && (
        <div className="file-panel-upload-error" role="alert">
          <span>{filesError}</span>
          {onRefreshFiles && (
            <button onClick={onRefreshFiles} className="file-panel-icon-btn" title={t('filePanel.refresh')} aria-label={t('filePanel.refresh')}>
              <RefreshCw className="h-3.5 w-3.5" />
            </button>
          )}
        </div>
      )}
      {error && (
        <div className="file-panel-upload-error">
          <span>{error}</span>
          <button onClick={onDismissError} className="file-panel-icon-btn" aria-label={t('common.close')}>
            <X className="h-3.5 w-3.5" />
          </button>
        </div>
      )}
      {busy && <div className="file-panel-progress-indeterminate" />}
      {backupResult && (
        <div className={`file-panel-backup-result ${backupResult.error ? 'error' : ''}`}>
          <span>
            {backupResult.error
              ? backupResult.error
              : t('filePanel.backupSummary', { count: backupResult.synced ?? 0 })
                + (backupResult.skipped ? t('filePanel.backupUnchanged', { count: backupResult.skipped }) : '')}
          </span>
          <button onClick={onDismissBackupResult} className="file-panel-icon-btn" style={{ padding: 2 }} aria-label={t('common.close')}>
            <X className="h-3 w-3" />
          </button>
        </div>
      )}
      {editing && (
        <div className="file-panel-edit-hint">
          <Pencil className="h-3 w-3" style={{ flexShrink: 0 }} />
          <span>{t('filePanel.editingHint')}</span>
        </div>
      )}
    </>
  );
}
