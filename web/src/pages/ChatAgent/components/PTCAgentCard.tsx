import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { motion, AnimatePresence } from 'framer-motion';
import { FlaskConical, Loader2, Check, X, ChevronRight, ExternalLink } from 'lucide-react';

interface ProposalData {
  workspace_name?: string;
  question: string;
  status: 'pending' | 'approved' | 'rejected';
  thread_id?: string;
  workspace_id?: string;
  report_back?: boolean;
}

interface FlashContext {
  threadId: string;
  workspaceId: string;
}

interface PTCAgentCardProps {
  proposalData: ProposalData | null;
  onApprove?: (overrides?: { report_back?: boolean }) => void;
  onReject?: () => void;
  flashContext?: FlashContext | null;
}

/**
 * PTCAgentCard - Inline HITL card for dispatching a PTC research agent.
 *
 * Three states:
 *   pending  - workspace name + question preview, Approve/Reject buttons
 *   approved - clickable artifact linking to the dispatched thread
 *   rejected - collapsed "Research declined"
 */
function PTCAgentCard({ proposalData, onApprove, onReject, flashContext }: PTCAgentCardProps) {
  const [collapsed, setCollapsed] = useState(true);
  const [reportBack, setReportBack] = useState(proposalData?.report_back ?? true);
  const navigate = useNavigate();

  if (!proposalData) return null;

  const { workspace_name, question, status, thread_id, workspace_id } = proposalData;
  const isApproved = status === 'approved';
  const isRejected = status === 'rejected';

  // --- Approved: clickable artifact to navigate to thread ---
  if (isApproved && thread_id && workspace_id) {
    return (
      <motion.button
        onClick={() => navigate(`/chat/t/${thread_id}`, { state: {
          workspaceId: workspace_id,
          ...(flashContext ? { fromThreadId: flashContext.threadId, fromWorkspaceId: flashContext.workspaceId } : {}),
        } })}
        className="flex items-center gap-3 w-full text-left rounded-lg px-4 py-3 cursor-pointer group transition-colors"
        style={{
          border: '1px solid var(--color-border-muted)',
          backgroundColor: 'var(--color-bg-secondary)',
        }}
        whileHover={{ scale: 1.005 }}
        whileTap={{ scale: 0.995 }}
      >
        <FlaskConical
          className="h-4 w-4 flex-shrink-0"
          style={{ color: 'var(--color-accent-light)' }}
        />
        <div className="flex-1 min-w-0">
          {workspace_name && (
            <div className="text-sm font-medium truncate" style={{ color: 'var(--color-text-primary)' }}>
              {workspace_name}
            </div>
          )}
          <div className="text-sm truncate" style={{ color: 'var(--color-text-tertiary)' }}>
            {question}
          </div>
        </div>
        <ExternalLink
          className="h-3.5 w-3.5 flex-shrink-0 opacity-0 group-hover:opacity-100 transition-opacity"
          style={{ color: 'var(--color-text-tertiary)' }}
        />
      </motion.button>
    );
  }

  // --- Resolved without thread_id (approved fallback or rejected) ---
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
            {isApproved ? 'Research dispatched' : 'Research declined'}
            {workspace_name && isApproved ? `: ${workspace_name}` : ''}
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
                  {workspace_name && (
                    <div className="text-sm font-medium mb-1" style={{ color: 'var(--color-text-primary)' }}>
                      {workspace_name}
                    </div>
                  )}
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
        <FlaskConical className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-accent-light)' }} />
        <span className="text-[15px] font-medium" style={{ color: 'var(--color-text-primary)' }}>
          Start Research
        </span>
        <Loader2
          className="h-3.5 w-3.5 animate-spin ml-auto flex-shrink-0"
          style={{ color: 'var(--color-icon-muted)' }}
        />
      </div>

      {/* Preview */}
      <div
        className="rounded-lg px-4 py-3"
        style={{ border: '1px solid var(--color-border-muted)' }}
      >
        {workspace_name && (
          <div className="text-sm font-medium mb-1" style={{ color: 'var(--color-text-primary)' }}>
            {workspace_name}
          </div>
        )}
        <div className="text-sm" style={{ color: 'var(--color-text-tertiary)' }}>
          {question}
        </div>
        {/* Report-back toggle */}
        <div
          className="mt-2.5 -mx-4 px-4 pt-2.5"
          style={{ borderTop: '1px solid var(--color-border-muted)' }}
        >
          <button
            type="button"
            className="flex items-center justify-between w-full cursor-pointer"
            onClick={(e: React.MouseEvent) => { e.stopPropagation(); setReportBack((v) => !v); }}
          >
            <span className="text-[13px]" style={{ color: 'var(--color-text-tertiary)' }}>
              Report back with summary
            </span>
            <div
              className="relative w-8 h-[18px] rounded-full transition-colors"
              style={{ background: reportBack ? 'var(--color-accent-light)' : 'rgba(255,255,255,0.12)' }}
            >
              <div
                className="absolute top-[3px] left-[3px] w-3 h-3 rounded-full bg-white transition-transform"
                style={{ transform: reportBack ? 'translateX(14px)' : 'translateX(0)' }}
              />
            </div>
          </button>
        </div>
      </div>

      {/* Actions */}
      <div className="pt-3 flex items-center gap-2">
        <motion.button
          onClick={(e: React.MouseEvent) => { e.stopPropagation(); onApprove?.({ report_back: reportBack }); }}
          className="flex items-center gap-1.5 text-sm px-4 py-2 rounded-md font-medium transition-colors hover:brightness-110"
          style={{ backgroundColor: 'var(--color-btn-primary-bg)', color: 'var(--color-btn-primary-text)' }}
          whileHover={{ scale: 1.02 }}
          whileTap={{ scale: 0.98 }}
        >
          <Check className="h-3.5 w-3.5 stroke-[2.5]" />
          Approve
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
          Decline
        </motion.button>
      </div>
    </motion.div>
  );
}

export default PTCAgentCard;
