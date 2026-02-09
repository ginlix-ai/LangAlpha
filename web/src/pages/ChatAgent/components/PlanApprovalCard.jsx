import React from 'react';
import { Zap, Loader2, Check, X, ChevronRight } from 'lucide-react';
import Markdown from './Markdown';

/**
 * PlanApprovalCard - Inline message segment card for HITL plan approval.
 *
 * Three states:
 *   pending  – full card with truncated plan preview + approve/reject buttons
 *   approved – compact green indicator, clickable to view full plan
 *   rejected – compact red indicator with "provide feedback below" hint
 *
 * @param {Object}   props.planData      – { description, interruptId, status }
 * @param {Function} props.onApprove     – called when user clicks Approve
 * @param {Function} props.onReject      – called when user clicks Reject
 * @param {Function} props.onDetailClick – called to open full plan in detail panel
 */
function PlanApprovalCard({ planData, onApprove, onReject, onDetailClick }) {
  if (!planData) return null;

  const { description, status } = planData;
  const isPending = status === 'pending';
  const isApproved = status === 'approved';
  const isRejected = status === 'rejected';

  // --- Compact: approved ---
  if (isApproved) {
    return (
      <div
        className="rounded-lg px-4 py-2.5 flex items-center gap-2 cursor-pointer transition-all hover:brightness-110"
        style={{
          backgroundColor: '#252738',
          border: '1px solid rgba(255, 255, 255, 0.06)',
        }}
        onClick={() => onDetailClick?.()}
      >
        <Check className="h-4 w-4 flex-shrink-0" style={{ color: '#8B83F0' }} />
        <span className="text-sm" style={{ color: 'rgba(255, 255, 255, 0.6)' }}>
          Plan approved
        </span>
        <ChevronRight
          className="h-3.5 w-3.5 ml-auto flex-shrink-0"
          style={{ color: 'rgba(255, 255, 255, 0.2)' }}
        />
      </div>
    );
  }

  // --- Compact: rejected ---
  if (isRejected) {
    return (
      <div
        className="rounded-lg px-4 py-2.5 flex items-center gap-2 cursor-pointer transition-all hover:brightness-110"
        style={{
          backgroundColor: '#252738',
          border: '1px solid rgba(255, 255, 255, 0.06)',
        }}
        onClick={() => onDetailClick?.()}
      >
        <X className="h-4 w-4 flex-shrink-0" style={{ color: 'rgba(255, 255, 255, 0.45)' }} />
        <span className="text-sm" style={{ color: 'rgba(255, 255, 255, 0.6)' }}>
          Plan rejected
        </span>
        <span className="text-xs" style={{ color: 'rgba(255, 255, 255, 0.3)' }}>
          — provide feedback below
        </span>
        <ChevronRight
          className="h-3.5 w-3.5 ml-auto flex-shrink-0"
          style={{ color: 'rgba(255, 255, 255, 0.2)' }}
        />
      </div>
    );
  }

  // --- Full card: pending ---
  return (
    <div
      className="rounded-lg overflow-hidden"
      style={{
        backgroundColor: '#252738',
        border: '1px solid rgba(255, 255, 255, 0.06)',
      }}
    >
      {/* Header */}
      <div
        className="flex items-center gap-2 px-4 py-2.5"
        style={{ borderBottom: '1px solid rgba(255, 255, 255, 0.05)' }}
      >
        <Zap className="h-4 w-4 flex-shrink-0" style={{ color: '#8B83F0' }} />
        <span className="text-sm font-medium" style={{ color: 'rgba(255, 255, 255, 0.85)' }}>
          Plan Approval Required
        </span>
        <Loader2
          className="h-3.5 w-3.5 animate-spin ml-auto flex-shrink-0"
          style={{ color: 'rgba(255, 255, 255, 0.2)' }}
        />
      </div>

      {/* Truncated plan body — click to open full plan in detail panel */}
      <div
        className="relative cursor-pointer"
        onClick={() => onDetailClick?.()}
      >
        <div
          className="px-4 py-3 overflow-hidden"
          style={{ maxHeight: '260px' }}
        >
          <Markdown variant="chat" content={description} className="text-sm" />
        </div>
        {/* Gradient fade indicating more content */}
        <div
          style={{
            position: 'absolute',
            bottom: 0,
            left: 0,
            right: 0,
            height: '64px',
            background: 'linear-gradient(to bottom, transparent, #252738)',
            pointerEvents: 'none',
          }}
        />
      </div>

      {/* Footer — Approve / Reject */}
      <div
        className="px-4 py-2.5 flex items-center"
        style={{ borderTop: '1px solid rgba(255, 255, 255, 0.05)' }}
      >
        <button
          onClick={(e) => { e.stopPropagation(); onReject?.(); }}
          className="flex items-center gap-1.5 text-sm px-3 py-1.5 rounded-md transition-colors"
          style={{
            backgroundColor: 'transparent',
            color: 'rgba(255, 255, 255, 0.45)',
          }}
          onMouseEnter={(e) => { e.currentTarget.style.backgroundColor = 'rgba(255, 255, 255, 0.06)'; e.currentTarget.style.color = 'rgba(255, 255, 255, 0.7)'; }}
          onMouseLeave={(e) => { e.currentTarget.style.backgroundColor = 'transparent'; e.currentTarget.style.color = 'rgba(255, 255, 255, 0.45)'; }}
        >
          <X className="h-3.5 w-3.5" />
          Reject
        </button>
        <button
          onClick={(e) => { e.stopPropagation(); onApprove?.(); }}
          className="flex items-center gap-1.5 text-sm px-3 py-1.5 rounded-md transition-colors hover:brightness-110 ml-auto"
          style={{
            backgroundColor: '#3D3580',
            color: '#C8C3FF',
          }}
        >
          <Check className="h-3.5 w-3.5" />
          Approve & Execute
        </button>
      </div>
    </div>
  );
}

export default PlanApprovalCard;
