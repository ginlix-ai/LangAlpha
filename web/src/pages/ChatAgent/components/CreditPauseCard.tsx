import React from 'react';
import { motion } from 'framer-motion';
import { useTranslation } from 'react-i18next';
import { Check, PauseCircle, Play } from 'lucide-react';
import { Loader } from '@/components/ui/loader';
import { ErrorLink } from '@/components/ui/error-banner';
import type { CreditPauseState } from '@/types/chat';

interface CreditPauseCardProps {
  pauseData: CreditPauseState;
  /** Absent in read-only transcripts — the card then informs without offering resume. */
  onResume?: () => void;
}

// The denial copy is the platform's and arrives unbounded: nothing on the path
// caps its length, and a single long unbroken token would push the card past
// its container. Wrapping handles the token, this handles the length.
const DENIAL_COPY_MAX_CHARS = 600;

function clampDenialCopy(message: string): string {
  return message.length > DENIAL_COPY_MAX_CHARS
    ? `${message.slice(0, DENIAL_COPY_MAX_CHARS).trimEnd()}\u2026`
    : message;
}

/**
 * HITL card for a credit-pause interrupt: the turn is checkpointed, not lost.
 * Three states: pending (platform's denial copy + links + Resume), resuming
 * (same copy, button disabled while admission decides), and resumed (quiet
 * collapsed line). Resume re-runs admission server-side, which refuses with a
 * 429 when the gate still denies — so the card only claims "Resumed" once a run
 * actually opened, and returns to pending otherwise.
 */
function CreditPauseCard({ pauseData, onResume }: CreditPauseCardProps) {
  const { t } = useTranslation();

  const isResuming = pauseData.status === 'resuming';

  // --- Resolved (resumed) ---
  if (pauseData.status === 'resumed') {
    return (
      <div className="flex items-center gap-2 py-1">
        <Check className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-accent-light)' }} />
        <span className="text-sm" style={{ color: 'var(--color-text-tertiary)' }}>
          {t('chat.creditPause.resumedLabel')}
        </span>
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
        <PauseCircle className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-accent-light)' }} />
        <span className="text-[0.9375rem] font-medium" style={{ color: 'var(--color-text-primary)' }}>
          {t('chat.creditPause.title')}
        </span>
      </div>

      {/* Details: the platform's message plus where to act on it */}
      <div
        className="rounded-lg px-4 py-3 space-y-2"
        style={{ border: '1px solid var(--color-border-muted)' }}
      >
        {pauseData.message && (
          <div
            className="text-sm break-words whitespace-pre-wrap"
            style={{ color: 'var(--color-text-secondary)' }}
          >
            {clampDenialCopy(pauseData.message)}
          </div>
        )}
        {pauseData.links && pauseData.links.length > 0 && (
          <div className="flex items-center gap-4 text-sm" style={{ color: 'var(--color-accent-light)' }}>
            {pauseData.links.map((l) => (
              <ErrorLink key={`${l.url}|${l.label}`} {...l} />
            ))}
          </div>
        )}
      </div>

      {/* Actions */}
      {onResume && (
        <div className="pt-3 flex items-center gap-2">
          <motion.button
            onClick={(e: React.MouseEvent) => { e.stopPropagation(); if (!isResuming) onResume(); }}
            disabled={isResuming}
            className="flex items-center gap-1.5 text-sm px-4 py-2 rounded-md font-medium transition-colors hover:brightness-110 disabled:cursor-default disabled:opacity-60 disabled:hover:brightness-100"
            style={{ backgroundColor: 'var(--color-btn-primary-bg)', color: 'var(--color-btn-primary-text)' }}
            whileHover={isResuming ? undefined : { scale: 1.02 }}
            whileTap={isResuming ? undefined : { scale: 0.98 }}
          >
            {isResuming
              ? <Loader size={14} className="flex-shrink-0" style={{ color: 'inherit' }} />
              : <Play className="h-3.5 w-3.5 stroke-[2.5]" />}
            {isResuming ? t('chat.creditPause.resuming') : t('chat.creditPause.resume')}
          </motion.button>
        </div>
      )}
    </motion.div>
  );
}

export default CreditPauseCard;
