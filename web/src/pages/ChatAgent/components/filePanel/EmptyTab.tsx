import React from 'react';
import { FolderOpen } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import './EmptyTab.css';

interface EmptyTabProps {
  /** Read-only panels have nothing to drop onto, so the invitation drops too. */
  canUpload: boolean;
}

/**
 * The empty tab of a panel with no tree. Where there is a tree, the empty tab
 * is the tree, laid across the whole body, and this never shows.
 */
export function EmptyTab({ canUpload }: EmptyTabProps): React.ReactElement {
  const { t } = useTranslation();
  return (
    <div className="file-panel-empty-tab">
      <FolderOpen className="h-8 w-8" style={{ color: 'var(--color-text-tertiary)' }} />
      <span className="file-panel-empty-tab-title">{t('filePanel.openFile')}</span>
      {canUpload && <span className="file-panel-empty-tab-hint">{t('filePanel.dropToUploadHint')}</span>}
    </div>
  );
}
