import React from 'react';
import { CornerUpLeft, FolderOpen } from 'lucide-react';
import { useTranslation } from 'react-i18next';

interface SiblingCrumbsProps {
  /** The workspace whose files the panel shows in place of Home's own. */
  workspaceName: string;
  onReturnHome: () => void;
  /** The view the button returns to; Home when unset. */
  returnLabel?: string | null;
}

/**
 * The row a sibling's files get when a link from Home or an analyst opened
 * them. Its tabs and tree look the same as the view's own, so the row says
 * whose files these are, and it is the way back to the view's own.
 */
export function SiblingCrumbs({ workspaceName, onReturnHome, returnLabel = null }: SiblingCrumbsProps): React.ReactElement {
  const { t } = useTranslation();
  return (
    <div className="file-panel-crumbs">
      <FolderOpen className="h-3.5 w-3.5 shrink-0" style={{ color: 'var(--color-icon-muted)' }} aria-hidden="true" />
      {workspaceName && (
        // The name is the whole point of the row, so it takes the width the
        // crumb class would cap and gives way only to the button.
        <span className="file-panel-crumb is-file" style={{ maxWidth: 'none', minWidth: 0 }} title={workspaceName}>
          {t('agents.inWorkspace', { workspace: workspaceName })}
        </span>
      )}
      <span className="file-panel-crumb-spacer" />
      <button
        type="button"
        className="file-panel-crumb inline-flex shrink-0 items-center gap-1"
        onClick={onReturnHome}
        title={returnLabel ?? t('agents.home')}
      >
        <CornerUpLeft className="h-3 w-3 shrink-0" aria-hidden="true" />
        {/* Ellipsis needs a block of its own: it does not reach a flex item's bare text. */}
        <span className="min-w-0 truncate">{returnLabel ?? t('agents.home')}</span>
      </button>
    </div>
  );
}
