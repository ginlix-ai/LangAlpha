import React, { useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { Trash2, Square, MessageSquareX, Check, X, ChevronRight } from 'lucide-react';
import { Loader } from '@/components/ui/loader';

type SecretaryActionType = 'delete_workspace' | 'stop_workspace' | 'delete_thread';

interface ProposalData {
  actionType: SecretaryActionType;
  workspace_id?: string;
  thread_id?: string;
  status: 'pending' | 'approved' | 'rejected';
}

interface SecretaryConfirmCardProps {
  proposalData: ProposalData | null;
  onApprove?: () => void;
  onReject?: () => void;
}

const ACTION_CONFIG: Record<SecretaryActionType, {
  icon: React.ComponentType<{ className?: string; style?: React.CSSProperties }>;
  title: string;
  approvedLabel: string;
  rejectedLabel: string;
  idLabel: string;
  idField: 'workspace_id' | 'thread_id';
}> = {
  delete_workspace: {
    icon: Trash2,
    title: 'Delete Workspace',
    approvedLabel: 'Workspace deleted',
    rejectedLabel: 'Workspace deletion declined',
    idLabel: 'Workspace',
    idField: 'workspace_id',
  },
  stop_workspace: {
    icon: Square,
    title: 'Stop Workspace',
    approvedLabel: 'Workspace stopped',
    rejectedLabel: 'Workspace stop declined',
    idLabel: 'Workspace',
    idField: 'workspace_id',
  },
  delete_thread: {
    icon: MessageSquareX,
    title: 'Delete Thread',
    approvedLabel: 'Thread deleted',
    rejectedLabel: 'Thread deletion declined',
    idLabel: 'Thread',
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
  const [collapsed, setCollapsed] = useState(true);

  if (!proposalData) return null;

  const { actionType, status } = proposalData;
  const config = ACTION_CONFIG[actionType];
  if (!config) return null;

  const Icon = config.icon;
  const targetId = proposalData[config.idField] || 'unknown';
  const shortId = targetId.length > 12 ? `${targetId.slice(0, 8)}...` : targetId;
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
            {isApproved ? config.approvedLabel : config.rejectedLabel}
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
                    <span className="font-medium">{config.idLabel}:</span>{' '}
                    <span className="font-mono text-xs">{targetId}</span>
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
        <Icon className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-accent-light)' }} />
        <span className="text-[0.9375rem] font-medium" style={{ color: 'var(--color-text-primary)' }}>
          {config.title}
        </span>
        <Loader
         
          size={14}
         
          className="ml-auto flex-shrink-0 text-[color:var(--color-icon-muted)]"
        />
      </div>

      {/* Details */}
      <div
        className="rounded-lg px-4 py-3"
        style={{ border: '1px solid var(--color-border-muted)' }}
      >
        <div className="text-sm" style={{ color: 'var(--color-text-tertiary)' }}>
          <span className="font-medium">{config.idLabel}:</span>{' '}
          <span className="font-mono text-xs">{shortId}</span>
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
          Confirm
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

export default SecretaryConfirmCard;
