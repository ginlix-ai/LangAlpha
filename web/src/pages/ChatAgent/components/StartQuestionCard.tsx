import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { motion, AnimatePresence } from 'framer-motion';
import { MessageSquareText, Check, X, ChevronRight } from 'lucide-react';
import { Loader } from '@/components/ui/loader';

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
  const [collapsed, setCollapsed] = useState(true);

  if (!proposalData) return null;

  const { question, status } = proposalData;
  const isApproved = status === 'approved';
  const isRejected = status === 'rejected';

  // --- Resolved (approved / rejected) ---
  if (isApproved || isRejected) {
    return (
      <div>
        <button
          onClick={() => setCollapsed((v) => !v)}
          className="flex items-center gap-2 py-1 cursor-pointer w-full text-left"
        >
          <motion.div
            animate={{ rotate: collapsed ? 0 : 90 }}
            transition={{ duration: 0.2 }}
          >
            <ChevronRight
              className="h-3.5 w-3.5 flex-shrink-0"
              style={{ color: 'var(--color-icon-muted)' }}
            />
          </motion.div>
          {isApproved ? (
            <Check className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-accent-light)' }} />
          ) : (
            <X className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
          )}
          <span
            className="text-sm"
            style={{ color: 'var(--color-text-tertiary)' }}
          >
            {isApproved ? t('chat.questionStarted') : t('chat.questionDeclined')}
          </span>
        </button>

        <AnimatePresence initial={false}>
          {!collapsed && (
            <motion.div
              initial={{ height: 0, opacity: 0 }}
              animate={{ height: 'auto', opacity: 1 }}
              exit={{ height: 0, opacity: 0 }}
              transition={{ duration: 0.25, ease: [0.22, 1, 0.36, 1] }}
              className="overflow-hidden"
            >
              <div className="pt-2 pb-1 pl-6">
                <div
                  className="rounded-lg px-4 py-3"
                  style={{
                    border: '1px solid var(--color-border-muted)',
                    opacity: isRejected ? 0.6 : 0.8,
                  }}
                >
                  <div className="text-sm" style={{ color: 'var(--color-text-tertiary)' }}>
                    {question}
                  </div>
                </div>
              </div>
            </motion.div>
          )}
        </AnimatePresence>
      </div>
    );
  }

  // --- Pending: interactive ---
  return (
    <motion.div
      initial={{ opacity: 0, y: 8 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.4, ease: [0.22, 1, 0.36, 1] }}
    >
      {/* Header */}
      <div className="flex items-center gap-2 pb-3">
        <MessageSquareText className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-accent-light)' }} />
        <span className="text-[0.9375rem] font-medium" style={{ color: 'var(--color-text-primary)' }}>
          {t('chat.startQuestion')}
        </span>
        <Loader
         
          size={14}
         
          className="ml-auto flex-shrink-0 text-[color:var(--color-icon-muted)]"
        />
      </div>

      {/* Question preview */}
      <div
        className="rounded-lg px-4 py-3"
        style={{ border: '1px solid var(--color-border-muted)' }}
      >
        <div className="text-sm" style={{ color: 'var(--color-text-tertiary)' }}>
          {question}
        </div>
      </div>

      {/* Actions */}
      <div className="pt-3 flex items-center gap-2">
        <motion.button
          onClick={(e: React.MouseEvent) => { e.stopPropagation(); onApprove?.(); }}
          className="flex items-center gap-1.5 text-sm px-4 py-2 rounded-md font-medium transition-colors hover:brightness-110"
          style={{ backgroundColor: 'var(--color-btn-primary-bg)', color: 'var(--color-btn-primary-text)' }}
          whileHover={{ scale: 1.02 }}
          whileTap={{ scale: 0.98 }}
        >
          <Check className="h-3.5 w-3.5 stroke-[2.5]" />
          {t('chat.letsGo')}
        </motion.button>
        <motion.button
          onClick={(e: React.MouseEvent) => { e.stopPropagation(); onReject?.(); }}
          className="flex items-center gap-1.5 text-sm px-4 py-2 rounded-md font-medium transition-colors"
          style={{
            backgroundColor: 'var(--color-border-muted)',
            color: 'var(--color-text-tertiary)',
          }}
          whileHover={{ scale: 1.02 }}
          whileTap={{ scale: 0.98 }}
        >
          <X className="h-3.5 w-3.5" />
          {t('chat.skip')}
        </motion.button>
      </div>
    </motion.div>
  );
}

export default StartQuestionCard;
