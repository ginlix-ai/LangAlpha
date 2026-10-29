import { useTranslation } from 'react-i18next';
import { useWorkspaceOptions } from '../hooks/useWorkspaceOptions';
import { RetryNote } from './RetryNote';

/**
 * Why the workspace reach is missing when the workspace list failed to load.
 * It stays out rather than count a reach against no list, so without this the
 * section reads as one where nothing has a scope.
 */
export function WorkspacesLoadNote() {
  const { t } = useTranslation();
  const { loadFailed, retry } = useWorkspaceOptions();
  if (!loadFailed) return null;
  return (
    <div className="plugins-load-note">
      <RetryNote text={t('plugins.scope.workspacesLoadFailed')} onRetry={retry} />
    </div>
  );
}
