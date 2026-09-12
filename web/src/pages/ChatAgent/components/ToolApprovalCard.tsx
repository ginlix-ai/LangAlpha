import React, { useState } from 'react';
import { motion } from 'framer-motion';
import { Check, X } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { Loader } from '@/components/ui/loader';
import type { ToolApprovalState } from '@/types/chat';
import { ArgsTable } from './mcp/ArgsTable';
import { DirectToolTileMark } from './mcp/DirectToolMark';
import { OrderApprovalCard } from './mcp/OrderApprovalCard';
import { SettledToolStep } from './mcp/SettledToolStep';
import { useDirectToolVendorLabel } from './mcp/useDirectToolVendor';
import { humanizeKey } from '../utils/structuredResult';

interface ToolApprovalCardProps {
  data: ToolApprovalState | null;
  onApprove?: () => void;
  onReject?: (message?: string) => void;
  /** The approved call has not answered yet. Only meaningful for an order,
   *  whose card waits for its receipt rather than settling on the click. */
  resultPending?: boolean;
  /** The approved call produced no result and no longer can, so no receipt
   *  will ever state the outcome. Only meaningful for an order, whose end
   *  state the ledger still holds. */
  resultLost?: boolean;
}

/**
 * Inline card for a direct MCP tool call that stopped for approval. Pending
 * shows the vendor, the tool and every argument exactly as it will be sent,
 * with Approve and Reject plus an optional reason; a settled card collapses to
 * a status row that still opens to the arguments. Read-only replay passes no
 * handlers, and a pending card there is a record of a stop nobody can answer
 * now, so it names that rather than asking.
 *
 * A call that places an order is a different question and gets its own card:
 * the person is deciding on a trade, not on whether a JSON frame is right, and
 * that card is drawn in the shape of the receipt the order becomes.
 */
function ToolApprovalCard({ data, onApprove, onReject, resultPending, resultLost }: ToolApprovalCardProps): React.ReactElement | null {
  const { t } = useTranslation();
  const [reason, setReason] = useState('');
  const vendorLabel = useDirectToolVendorLabel(data?.server || '');

  if (!data) return null;

  if (data.order) {
    return (
      <OrderApprovalCard
        data={data}
        order={data.order}
        onApprove={onApprove}
        onReject={onReject}
        resultPending={resultPending}
        resultLost={resultLost}
      />
    );
  }

  const { status, server, tool, args } = data;
  const toolLabel = humanizeKey(tool);
  const isApproved = status === 'approved';
  const isRejected = status === 'rejected';

  const detailBlock = (
    <div className="rounded-lg px-4 py-3" style={{ border: '1px solid var(--color-border-muted)' }}>
      <ArgsTable args={args || {}} emptyLabel={t('toolArtifact.directTool.noArguments')} />
    </div>
  );

  if (isApproved || isRejected) {
    return (
      <SettledToolStep
        approved={isApproved}
        reason={isRejected ? data.reason : null}
        label={
          isApproved
            ? t('toolArtifact.directTool.approvedAction', { vendor: vendorLabel, tool: toolLabel })
            : t('toolArtifact.directTool.rejectedAction', { vendor: vendorLabel, tool: toolLabel })
        }
      >
        {detailBlock}
      </SettledToolStep>
    );
  }

  const canAct = !!onApprove || !!onReject;

  return (
    <motion.div
      initial={{ opacity: 0, y: 8 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.4, ease: [0.22, 1, 0.36, 1] }}
      data-testid="tool-approval-card"
    >
      <div className="flex items-center gap-3 pb-3">
        <DirectToolTileMark server={server} />
        <div className="min-w-0 flex-1">
          <div className="text-[0.9375rem] font-medium truncate" style={{ color: 'var(--color-text-primary)' }}>
            {canAct
              ? t('toolArtifact.directTool.approvalTitle')
              : t('toolArtifact.directTool.unansweredTitle')}
          </div>
          <div className="text-xs truncate" style={{ color: 'var(--color-text-tertiary)' }}>
            {vendorLabel} · {toolLabel}
          </div>
        </div>
        {canAct && (
          <Loader size={14} className="ml-auto flex-shrink-0 text-[color:var(--color-icon-muted)]" />
        )}
      </div>

      {detailBlock}

      {canAct && (
        <div className="pt-3 flex flex-wrap items-center gap-2">
          <motion.button
            type="button"
            onClick={(e: React.MouseEvent) => { e.stopPropagation(); onApprove?.(); }}
            disabled={!onApprove}
            className="flex items-center gap-1.5 text-sm px-4 py-2 rounded-md font-medium transition-colors hover:brightness-110 disabled:opacity-50"
            style={{ backgroundColor: 'var(--color-btn-primary-bg)', color: 'var(--color-btn-primary-text)' }}
            whileHover={{ scale: 1.02 }}
            whileTap={{ scale: 0.98 }}
          >
            <Check className="h-3.5 w-3.5 stroke-[2.5]" />
            {t('toolArtifact.directTool.approve')}
          </motion.button>
          <motion.button
            type="button"
            onClick={(e: React.MouseEvent) => { e.stopPropagation(); onReject?.(reason.trim() || undefined); }}
            disabled={!onReject}
            className="flex items-center gap-1.5 text-sm px-4 py-2 rounded-md font-medium transition-colors disabled:opacity-50"
            style={{ backgroundColor: 'var(--color-border-muted)', color: 'var(--color-text-tertiary)' }}
            whileHover={{ scale: 1.02 }}
            whileTap={{ scale: 0.98 }}
          >
            <X className="h-3.5 w-3.5" />
            {t('toolArtifact.directTool.reject')}
          </motion.button>
          <input
            type="text"
            value={reason}
            onChange={(e) => setReason(e.target.value)}
            maxLength={200}
            placeholder={t('toolArtifact.directTool.reasonPlaceholder')}
            aria-label={t('toolArtifact.directTool.reasonPlaceholder')}
            className="flex-1 min-w-[10rem] text-sm px-3 py-2 rounded-md bg-transparent outline-none focus:ring-1"
            style={{ border: '1px solid var(--color-border-muted)', color: 'var(--color-text-primary)' }}
          />
        </div>
      )}
    </motion.div>
  );
}

export default ToolApprovalCard;
