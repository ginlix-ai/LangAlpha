import React from 'react';
import { Pencil, RefreshCw, X } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { useLocale } from '@/hooks/useLocale';
import type { BackupResult, UnsavedFile } from './types';
import { unsavedReasonLabel } from './unsavedReason';

/** How many skipped files the notice names before it summarises the rest. */
const UNSAVED_NAMED = 5;

function UnsavedReasonText({ file }: { file: UnsavedFile }): React.ReactElement {
  const { t } = useTranslation();
  const locale = useLocale();
  return <span className="file-panel-unsaved-reason">{unsavedReasonLabel(t, file, locale)}</span>;
}

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
  /** A delete, a backup, or the last step of an upload is running; none reports a percentage. */
  busy: boolean;
  backupResult: BackupResult | null;
  onDismissBackupResult: () => void;
  editing: boolean;
  /** The offer to restore a folder found missing, first: until it is
   *  answered the listing below is of an empty folder. */
  restoreOffer?: React.ReactNode;
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
  restoreOffer = null,
}: PanelNoticesProps): React.ReactElement {
  const { t } = useTranslation();
  return (
    <>
      {restoreOffer}
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
      {backupResult && !backupResult.error && (backupResult.unsaved_count || backupResult.unsaved?.length) ? (
        <UnsavedBackupNotice result={backupResult} onDismiss={onDismissBackupResult} />
      ) : backupResult && (
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

function UnsavedBackupNotice({ result, onDismiss }: { result: BackupResult; onDismiss: () => void }): React.ReactElement {
  const { t } = useTranslation();
  const listed = result.unsaved ?? [];
  const total = Math.max(result.unsaved_count ?? 0, listed.length);
  const named = listed.slice(0, UNSAVED_NAMED);
  const rest = total - named.length;
  return (
    <div className="file-panel-backup-result unsaved" role="status">
      <div className="file-panel-backup-result-head">
        <span>
          {t('filePanel.backupSummary', { count: result.synced ?? 0 })}
          {' · '}
          <span className="file-panel-unsaved-count">{t('filePanel.backupNotSaved', { count: total })}</span>
        </span>
        <button onClick={onDismiss} className="file-panel-icon-btn" style={{ padding: 2 }} aria-label={t('common.close')}>
          <X className="h-3 w-3" />
        </button>
      </div>
      <ul className="file-panel-unsaved-list">
        {named.map((file) => (
          <li key={file.path}>
            <span className="file-panel-unsaved-path" title={file.path}>{file.path}</span>
            <UnsavedReasonText file={file} />
          </li>
        ))}
      </ul>
      {rest > 0 && <span className="file-panel-unsaved-more">{t('filePanel.backupNotSavedMore', { count: rest })}</span>}
    </div>
  );
}
