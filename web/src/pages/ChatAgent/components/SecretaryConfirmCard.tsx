import { Trash2, Square, MessageSquareX, type LucideIcon } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { ProposalCard } from './ProposalCard';

type SecretaryActionType = 'delete_workspace' | 'stop_workspace' | 'delete_thread';

interface ProposalData {
  actionType: SecretaryActionType;
  workspace_id?: string;
  /** Sent with a workspace delete; older interrupts carry only the id. */
  workspace_name?: string | null;
  thread_id?: string;
  status: 'pending' | 'approved' | 'rejected';
}

interface SecretaryConfirmCardProps {
  proposalData: ProposalData | null;
  onApprove?: () => void;
  onReject?: () => void;
}

const ACTION_CONFIG: Record<SecretaryActionType, {
  icon: LucideIcon;
  titleKey: string;
  approvedKey: string;
  rejectedKey: string;
  idLabelKey: string;
  idField: 'workspace_id' | 'thread_id';
}> = {
  delete_workspace: {
    icon: Trash2,
    titleKey: 'chat.secretaryConfirm.deleteWorkspace.title',
    approvedKey: 'chat.secretaryConfirm.deleteWorkspace.approved',
    rejectedKey: 'chat.secretaryConfirm.deleteWorkspace.rejected',
    idLabelKey: 'chat.secretaryConfirm.workspaceLabel',
    idField: 'workspace_id',
  },
  stop_workspace: {
    icon: Square,
    titleKey: 'chat.secretaryConfirm.stopWorkspace.title',
    approvedKey: 'chat.secretaryConfirm.stopWorkspace.approved',
    rejectedKey: 'chat.secretaryConfirm.stopWorkspace.rejected',
    idLabelKey: 'chat.secretaryConfirm.workspaceLabel',
    idField: 'workspace_id',
  },
  delete_thread: {
    icon: MessageSquareX,
    titleKey: 'chat.secretaryConfirm.deleteThread.title',
    approvedKey: 'chat.secretaryConfirm.deleteThread.approved',
    rejectedKey: 'chat.secretaryConfirm.deleteThread.rejected',
    idLabelKey: 'chat.secretaryConfirm.threadLabel',
    idField: 'thread_id',
  },
};

/**
 * SecretaryConfirmCard - Generic HITL confirmation card for secretary actions.
 *
 * Handles delete_workspace, stop_workspace, and delete_thread interrupt types.
 *
 * Three states:
 *   pending  - action description + ID, Approve/Reject buttons
 *   approved - collapsed confirmation, expandable
 *   rejected - collapsed declined message
 */
function SecretaryConfirmCard({ proposalData, onApprove, onReject }: SecretaryConfirmCardProps) {
  const { t } = useTranslation();

  if (!proposalData) return null;

  const { actionType, status } = proposalData;
  const config = ACTION_CONFIG[actionType];
  if (!config) return null;

  const targetId = proposalData[config.idField] || t('chat.secretaryConfirm.unknownId');
  const shortId = targetId.length > 12 ? `${targetId.slice(0, 8)}...` : targetId;
  const targetName = config.idField === 'workspace_id' ? proposalData.workspace_name : null;
  // The card has room only for the start of a bare id; the settled row opens
  // onto all of it.
  const target = (id: string) => (
    <div className="text-sm" style={{ color: 'var(--color-text-tertiary)' }}>
      <span className="font-medium">{t(config.idLabelKey)}</span>{' '}
      {targetName
        ? <span>{targetName}</span>
        : <span className="font-mono text-xs">{id}</span>}
    </div>
  );

  return (
    <ProposalCard
      icon={config.icon}
      title={t(config.titleKey)}
      status={status}
      settledLabel={t(status === 'approved' ? config.approvedKey : config.rejectedKey)}
      approveLabel={t('chat.secretaryConfirm.confirm')}
      rejectLabel={t('chat.secretaryConfirm.decline')}
      onApprove={onApprove}
      onReject={onReject}
      settledDetail={target(targetId)}
    >
      {target(shortId)}
    </ProposalCard>
  );
}

export default SecretaryConfirmCard;
