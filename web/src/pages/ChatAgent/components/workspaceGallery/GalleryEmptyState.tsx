import { MessageSquareText, Plus } from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import { useTranslation } from 'react-i18next';

import { getFlashWorkspace } from '../../utils/api';
import type { WorkspaceRecord } from './types';

interface GalleryEmptyStateProps {
  /** A search that matched nothing says so; it does not re-pitch the product. */
  isFiltered: boolean;
  onNewWorkspace: () => void;
}

export function GalleryEmptyState({ isFiltered, onNewWorkspace }: GalleryEmptyStateProps) {
  const { t } = useTranslation();
  const navigate = useNavigate();

  if (isFiltered) {
    return (
      <p className="text-sm" style={{ color: 'var(--color-text-tertiary)' }}>
        {t('workspace.noWorkspacesFound')}
      </p>
    );
  }

  const startOnboarding = async () => {
    try {
      const flashWsData = await getFlashWorkspace();
      navigate('/chat/t/__default__', {
        state: {
          workspaceId: (flashWsData as WorkspaceRecord).workspace_id,
          isOnboarding: true,
          agentMode: 'flash',
          workspaceStatus: 'flash',
        },
      });
    } catch (err) {
      console.error('Error starting onboarding:', err);
    }
  };

  return (
    <>
      <p className="text-lg font-medium mb-2" style={{ color: 'var(--color-text-primary)' }}>
        {t('workspace.welcomeTitle')}
      </p>
      <p className="text-sm mb-8" style={{ color: 'var(--color-text-tertiary)' }}>
        {t('workspace.welcomeDesc')}
      </p>
      <div className="flex flex-col sm:flex-row items-center gap-3">
        <button
          onClick={() => void startOnboarding()}
          className="flex items-center gap-2 px-6 py-3 rounded-lg transition-all hover:opacity-90 active:scale-[0.985]"
          style={{
            backgroundColor: 'var(--color-btn-primary-bg)',
            color: 'var(--color-btn-primary-text)',
          }}
        >
          <MessageSquareText className="h-5 w-5" />
          <span className="font-medium">{t('settings.startOnboarding')}</span>
        </button>
        <button
          onClick={onNewWorkspace}
          className="flex items-center gap-2 px-6 py-3 rounded-lg border transition-all hover:bg-foreground/5 hover:scale-[1.01] active:scale-[0.985]"
          style={{
            borderColor: 'var(--color-border-muted)',
            color: 'var(--color-text-primary)',
          }}
        >
          <Plus className="h-5 w-5" />
          <span className="font-medium">{t('workspace.createWorkspace')}</span>
        </button>
      </div>
    </>
  );
}

export default GalleryEmptyState;
