import { useTranslation } from 'react-i18next';
import { MessageSquareText } from 'lucide-react';
import { ProposalCard } from './ProposalCard';

interface ProposalData {
  question: string;
  status: 'pending' | 'approved' | 'rejected';
}

interface StartQuestionCardProps {
  proposalData: ProposalData | null;
  onApprove?: () => void;
  onReject?: () => void;
}

/**
 * StartQuestionCard - Inline HITL card for starter question approval.
 *
 * Three states:
 *   pending  - question text preview, Approve/Reject buttons
 *   approved - collapsed "Question started", expandable to show question
 *   rejected - collapsed "Question declined"
 */
function StartQuestionCard({ proposalData, onApprove, onReject }: StartQuestionCardProps) {
  const { t } = useTranslation();

  if (!proposalData) return null;

  const { question, status } = proposalData;

  return (
    <ProposalCard
      icon={MessageSquareText}
      title={t('chat.startQuestion')}
      status={status}
      settledLabel={status === 'approved' ? t('chat.questionStarted') : t('chat.questionDeclined')}
      approveLabel={t('chat.letsGo')}
      rejectLabel={t('chat.skip')}
      onApprove={onApprove}
      onReject={onReject}
    >
      <div className="text-sm" style={{ color: 'var(--color-text-tertiary)' }}>
        {question}
      </div>
    </ProposalCard>
  );
}

export default StartQuestionCard;
