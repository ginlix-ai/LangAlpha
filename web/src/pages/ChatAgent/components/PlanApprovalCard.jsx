import React, { useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { ScrollText, Loader2, Check, X, ChevronRight } from 'lucide-react';
import Markdown from './Markdown';

/**
 * PlanApprovalCard - Inline message segment card for HITL plan approval.
 *
 * Three states:
 *   pending  – plan preview with approve/reject buttons
 *   approved – status banner + plan visible (collapsible)
 *   rejected – status banner + plan visible (collapsible) + feedback hint
 *
 * Resolved states default to expanded; user can manually collapse.
 */
function PlanApprovalCard({ planData, onApprove, onReject, onDetailClick }) {
  if (!planData) return null;

  const { description, status } = planData;
  const isApproved = status === 'approved';
  const isRejected = status === 'rejected';

  const [collapsed, setCollapsed] = useState(false);

  // --- Resolved (approved / rejected): expanded by default, manually collapsible ---
  if (isApproved || isRejected) {
    return (
      <div>
        {/* Header row — click to toggle */}
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
              style={{ color: 'rgba(255,255,255,0.25)' }}
            />
          </motion.div>
          {isApproved ? (
            <Check className="h-4 w-4 flex-shrink-0" style={{ color: '#8B83F0' }} />
          ) : (
            <X className="h-4 w-4 flex-shrink-0" style={{ color: 'rgba(255, 255, 255, 0.4)' }} />
          )}
          <span
            className="text-sm"
            style={{ color: isApproved ? 'rgba(255, 255, 255, 0.6)' : 'rgba(255, 255, 255, 0.5)' }}
          >
            {isApproved ? 'Plan Approved' : 'Plan Rejected'}
          </span>
          {isRejected && (
            <span className="text-xs" style={{ color: 'rgba(255, 255, 255, 0.25)' }}>
              — provide feedback below
            </span>
          )}
        </button>

        {/* Plan body — expanded by default */}
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
                  className="relative cursor-pointer rounded-lg overflow-hidden"
                  style={{
                    border: `1px solid rgba(255, 255, 255, ${isApproved ? '0.06' : '0.04'})`,
                    opacity: isRejected ? 0.6 : 0.8,
                  }}
                  onClick={() => onDetailClick?.()}
                >
                  <div className="px-4 py-3 overflow-hidden" style={{ maxHeight: '260px' }}>
                    <Markdown variant="chat" content={description} className="text-sm" />
                  </div>
                  <div
                    style={{
                      position: 'absolute',
                      bottom: 0, left: 0, right: 0, height: '64px',
                      background: 'linear-gradient(to bottom, transparent, #1B1D25)',
                      pointerEvents: 'none',
                    }}
                  />
                </div>
              </div>
            </motion.div>
          )}
        </AnimatePresence>
      </div>
    );
  }

  // --- Pending: full interactive ---
  return (
    <motion.div
      initial={{ opacity: 0, y: 8 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.4, ease: [0.22, 1, 0.36, 1] }}
    >
      {/* Header */}
      <div className="flex items-center gap-2 pb-3">
        <ScrollText className="h-4 w-4 flex-shrink-0" style={{ color: '#8B83F0' }} />
        <span className="text-[15px] font-medium" style={{ color: 'rgba(255, 255, 255, 0.9)' }}>
          Plan Approval Required
        </span>
        <Loader2
          className="h-3.5 w-3.5 animate-spin ml-auto flex-shrink-0"
          style={{ color: 'rgba(255, 255, 255, 0.2)' }}
        />
      </div>

      {/* Plan body */}
      <div
        className="relative cursor-pointer rounded-lg overflow-hidden"
        style={{ border: '1px solid rgba(255, 255, 255, 0.06)' }}
        onClick={() => onDetailClick?.()}
      >
        <div className="px-4 py-3 overflow-hidden" style={{ maxHeight: '260px' }}>
          <Markdown variant="chat" content={description} className="text-sm" />
        </div>
        <div
          style={{
            position: 'absolute',
            bottom: 0, left: 0, right: 0, height: '64px',
            background: 'linear-gradient(to bottom, transparent, #1B1D25)',
            pointerEvents: 'none',
          }}
        />
      </div>

      {/* Actions — matched sizing */}
      <div className="pt-3 flex items-center gap-2">
        <motion.button
          onClick={(e) => { e.stopPropagation(); onApprove?.(); }}
          className="flex items-center gap-1.5 text-sm px-4 py-2 rounded-md font-medium transition-colors hover:brightness-110"
          style={{ backgroundColor: 'white', color: '#1a1b2e' }}
          whileHover={{ scale: 1.02 }}
          whileTap={{ scale: 0.98 }}
        >
          <Check className="h-3.5 w-3.5 stroke-[2.5]" />
          Approve
        </motion.button>
        <motion.button
          onClick={(e) => { e.stopPropagation(); onReject?.(); }}
          className="flex items-center gap-1.5 text-sm px-4 py-2 rounded-md font-medium transition-colors"
          style={{
            backgroundColor: 'rgba(255, 255, 255, 0.06)',
            color: 'rgba(255, 255, 255, 0.45)',
          }}
          onMouseEnter={(e) => {
            e.currentTarget.style.backgroundColor = 'rgba(255, 255, 255, 0.1)';
            e.currentTarget.style.color = 'rgba(255, 255, 255, 0.7)';
          }}
          onMouseLeave={(e) => {
            e.currentTarget.style.backgroundColor = 'rgba(255, 255, 255, 0.06)';
            e.currentTarget.style.color = 'rgba(255, 255, 255, 0.45)';
          }}
          whileHover={{ scale: 1.02 }}
          whileTap={{ scale: 0.98 }}
        >
          <X className="h-3.5 w-3.5" />
          Reject
        </motion.button>
      </div>
    </motion.div>
  );
}

export default PlanApprovalCard;
