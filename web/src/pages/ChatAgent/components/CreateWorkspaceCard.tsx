import { useTranslation } from 'react-i18next';
import { FolderPlus } from 'lucide-react';
import { AUTO_APPROVE, useApproveAlways } from '@/hooks/useAutoApprove';
import { ProposalCard } from './ProposalCard';

interface ProposalData {
  workspace_name: string;
  workspace_description?: string;
  status: 'pending' | 'approved' | 'rejected';
}

interface CreateWorkspaceCardProps {
  proposalData: ProposalData | null;
  onApprove?: () => void;
  onReject?: () => void;
}

/**
 * CreateWorkspaceCard - Inline HITL card for workspace creation approval.
 *
 * Three states:
 *   pending  - workspace name + description, Approve/Reject buttons
 *   approved - collapsed "Workspace created: [name]", expandable
 *   rejected - collapsed "Workspace creation declined"
 */
function CreateWorkspaceCard({ proposalData, onApprove, onReject }: CreateWorkspaceCardProps) {
  const { t } = useTranslation();
  const { approveAlways, saving } = useApproveAlways(AUTO_APPROVE.workspaceCreation);

  if (!proposalData) return null;

  const { workspace_name, workspace_description, status } = proposalData;

  return (
    <ProposalCard
      icon={FolderPlus}
      title={t('chat.createWorkspaceCard.title')}
      status={status}
      settledLabel={
        status === 'approved'
          ? t('chat.createWorkspaceCard.created', { name: workspace_name })
          : t('chat.createWorkspaceCard.declined')
      }
      approveLabel={t('chat.createWorkspaceCard.create')}
      rejectLabel={t('chat.createWorkspaceCard.decline')}
      onApprove={onApprove}
      onReject={onReject}
      approveAlways={{
        label: t('chat.createWorkspaceCard.alwaysApprove'),
        hint: t('chat.createWorkspaceCard.alwaysApproveHint'),
        onClick: () => approveAlways(() => onApprove?.()),
      }}
      busy={saving}
    >
      <div className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
        {workspace_name}
      </div>
      {workspace_description && (
        <div className="text-sm mt-1" style={{ color: 'var(--color-text-tertiary)' }}>
          {workspace_description}
        </div>
      )}
    </ProposalCard>
  );
}

export default CreateWorkspaceCard;
