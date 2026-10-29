import React from 'react';
import { Check, X, type LucideIcon } from 'lucide-react';
import { Loader } from '@/components/ui/loader';
import { ResolveSwap } from './decisionCardMotion';
import { SettledStep } from './SettledStep';

interface ProposalCardProps {
  icon: LucideIcon;
  title: string;
  status: 'pending' | 'approved' | 'rejected';
  /** The settled row's words, for whichever answer it was given. */
  settledLabel: string;
  approveLabel: string;
  rejectLabel: string;
  onApprove?: () => void;
  onReject?: () => void;
  /** A second way to approve, between the two: approve this and every one
   *  like it. */
  approveAlways?: { label: string; hint: string; onClick: () => void };
  /** Every button waits while something the card started is saving. */
  busy?: boolean;
  /** What is proposed, drawn in the card's detail box. */
  children: React.ReactNode;
  /** The detail once answered, where it says more than the card had room
   *  for (an id the card shortened). The card's own detail otherwise. */
  settledDetail?: React.ReactNode;
}

function DetailBox({ children }: { children: React.ReactNode }): React.ReactElement {
  return (
    <div className="rounded-lg px-4 py-3" style={{ border: '1px solid var(--color-border-muted)' }}>
      {children}
    </div>
  );
}

/**
 * An action the agent asks leave to take, and the row it leaves once
 * answered: a workspace to create, one to delete or stop, a question to start
 * on. The proposals differ only in what they say, so they share the card: a
 * title under the proposal's glyph, what is proposed in a box, and Approve and
 * Reject, settling into the shared `SettledStep`, which opens onto the same box.
 */
export function ProposalCard({
  icon: Icon,
  title,
  status,
  settledLabel,
  approveLabel,
  rejectLabel,
  onApprove,
  onReject,
  approveAlways,
  busy = false,
  children,
  settledDetail,
}: ProposalCardProps): React.ReactElement {
  return (
    <ResolveSwap
      resolved={status !== 'pending'}
      settled={
        <SettledStep
          verdict={status === 'approved' ? 'approved' : 'rejected'}
          label={settledLabel}
          testId="proposal-settled"
        >
          <DetailBox>{settledDetail ?? children}</DetailBox>
        </SettledStep>
      }
    >
      <div className="flex items-center gap-2 pb-3">
        <Icon className="h-4 w-4 shrink-0" style={{ color: 'var(--color-accent-light)' }} />
        <span className="text-[0.9375rem] font-medium" style={{ color: 'var(--color-text-primary)' }}>
          {title}
        </span>
        <Loader size={14} className="ml-auto shrink-0 text-(--color-icon-muted)" />
      </div>

      <DetailBox>{children}</DetailBox>

      <div className="pt-3 flex items-center gap-2">
        <button
          type="button"
          disabled={busy}
          onClick={(e) => { e.stopPropagation(); onApprove?.(); }}
          className="press flex items-center gap-1.5 text-sm px-4 py-2 rounded-md font-medium hover:brightness-110"
          style={{ backgroundColor: 'var(--color-btn-primary-bg)', color: 'var(--color-btn-primary-text)' }}
        >
          <Check className="h-3.5 w-3.5 stroke-[2.5]" />
          {approveLabel}
        </button>
        {approveAlways && (
          <button
            type="button"
            disabled={busy}
            title={approveAlways.hint}
            onClick={(e) => { e.stopPropagation(); approveAlways.onClick(); }}
            className="press text-sm px-4 py-2 rounded-md font-medium hover:bg-(--color-bg-hover)"
            style={{ border: '1px solid var(--color-border-default)', color: 'var(--color-text-secondary)' }}
          >
            {approveAlways.label}
          </button>
        )}
        <button
          type="button"
          disabled={busy}
          onClick={(e) => { e.stopPropagation(); onReject?.(); }}
          className="press flex items-center gap-1.5 text-sm px-4 py-2 rounded-md font-medium"
          style={{ backgroundColor: 'var(--color-border-muted)', color: 'var(--color-text-tertiary)' }}
        >
          <X className="h-3.5 w-3.5" />
          {rejectLabel}
        </button>
      </div>
    </ResolveSwap>
  );
}
