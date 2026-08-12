import React, { useEffect, useId, useRef, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { motion, AnimatePresence, type MotionProps } from 'framer-motion';
import { Check, X, ChevronRight, ArrowRight, AlertTriangle, Square } from 'lucide-react';
import { Loader } from '@/components/ui/loader';
import { useDispatchStatus, type PTCDispatchStatus } from '../hooks/usePTCDispatchStatus';

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

// Featured-surface visual language (matches ConversationWidget / AIDailyBriefCard).
// Message-flow card → the chat artifact fill (tinted on the light chat page),
// not --color-bg-card (white; the dashboard-on-canvas pairing).
const PANEL_BG = 'var(--color-bg-tool-card)';
const MONO = 'var(--font-mono)';

/**
 * Single source of truth for the dispatch card's per-status presentation.
 * Every status-driven attribute — glyph, label color, whether the run is still
 * in flight (the amber left rule), and the footer hint/CTA — lives in this one
 * declarative table so adding or renaming a status is a single-row edit.
 * `hintKey`/`ctaKey`/`labelKey` are i18n keys resolved with `t()` at the render
 * site. Status is text + a small glyph, never a filled pill or accent bar: the
 * design system keeps amber annotation-only, and the glyphs match the sidebar's
 * thread rows (ascii spinner = running, hollow ring = needs input) so run state
 * reads the same everywhere.
 */
const STATUS_UI: Record<
  PTCDispatchStatus,
  {
    labelKey: string;
    glyph: React.ReactNode;
    labelColor: string;
    /** Run still in flight — the shell keeps the slightly stronger border. */
    live: boolean;
    hintKey: string | null;
    ctaKey: string;
    /** Paints the CTA amber (the one state asking for the user's action). */
    ctaAccent?: boolean;
  }
> = {
  starting: {
    labelKey: 'chat.ptcCard.statusStarting',
    glyph: <LiveSpinner />,
    labelColor: 'var(--color-text-tertiary)',
    live: true,
    hintKey: 'chat.ptcCard.hintProvisioning',
    ctaKey: 'chat.ptcCard.ctaOpenThread',
  },
  running: {
    labelKey: 'chat.ptcCard.statusWorking',
    glyph: <LiveSpinner />,
    labelColor: 'var(--color-text-tertiary)',
    live: true,
    hintKey: 'chat.ptcCard.hintWorking',
    ctaKey: 'chat.ptcCard.ctaOpenThread',
  },
  needs_input: {
    labelKey: 'chat.ptcCard.statusNeedsInput',
    glyph: (
      <span aria-hidden className="flex h-3 w-3 flex-shrink-0 items-center justify-center">
        <span className="rounded-full" style={{ width: 7, height: 7, border: '1.5px solid var(--color-accent-primary)' }} />
      </span>
    ),
    labelColor: 'var(--color-accent-primary)',
    live: true,
    hintKey: null,
    ctaKey: 'chat.ptcCard.ctaAnswerContinue',
    ctaAccent: true,
  },
  completed: {
    labelKey: 'chat.ptcCard.statusCompleted',
    glyph: <Check aria-hidden className="h-3 w-3 flex-shrink-0 stroke-[2.5]" style={{ color: 'var(--color-success)' }} />,
    labelColor: 'var(--color-text-tertiary)',
    live: false,
    hintKey: null,
    ctaKey: 'chat.ptcCard.ctaOpenThread',
  },
  failed: {
    labelKey: 'chat.ptcCard.statusFailed',
    glyph: <AlertTriangle aria-hidden className="h-3 w-3 flex-shrink-0" style={{ color: 'var(--color-loss)' }} />,
    labelColor: 'var(--color-loss)',
    live: false,
    hintKey: 'chat.ptcCard.hintFailed',
    ctaKey: 'chat.ptcCard.ctaViewThread',
  },
  stopped: {
    labelKey: 'chat.ptcCard.statusStopped',
    glyph: <Square aria-hidden className="h-2.5 w-2.5 flex-shrink-0" style={{ color: 'var(--color-icon-muted)' }} />,
    labelColor: 'var(--color-text-tertiary)',
    live: false,
    hintKey: 'chat.ptcCard.hintStopped',
    ctaKey: 'chat.ptcCard.ctaViewThread',
  },
};

/** The sidebar's running-thread glyph (amber ascii spinner), reused so "amber
 *  spinner = agent working" reads identically on every surface. The Loader
 *  ships its own role="status"; the aria-hidden wrapper drops it from the a11y
 *  tree — the card's persistent live region already announces the label. */
function LiveSpinner() {
  return (
    <span aria-hidden className="flex-shrink-0">
      <Loader size={12} className="text-[color:var(--color-accent-primary)]" />
    </span>
  );
}

function fmtElapsed(secs: number): string {
  const m = Math.floor(secs / 60);
  return `${m}:${String(secs % 60).padStart(2, '0')}`;
}

/** Elapsed seconds since `active` first turned true on this mount (best-effort —
 *  a card mounted mid-run counts from mount, not from the true run start). */
function useElapsedSeconds(active: boolean): number {
  const [secs, setSecs] = useState(0);
  const startRef = useRef<number | null>(null);
  useEffect(() => {
    if (!active) {
      startRef.current = null;
      setSecs(0);
      return;
    }
    if (startRef.current === null) startRef.current = Date.now();
    const tick = () => setSecs(Math.floor((Date.now() - (startRef.current as number)) / 1000));
    tick();
    const id = setInterval(tick, 1000);
    return () => clearInterval(id);
  }, [active]);
  return secs;
}

function StatusIndicator({ status, elapsed }: { status: PTCDispatchStatus; elapsed: string | null }) {
  const { t } = useTranslation();
  const ui = STATUS_UI[status];
  // One persistent role="status" live region; only the inner glyph/label swap
  // per state. Mounting a fresh live region per state can drop the
  // announcement, so transitions stay reliably announced this way.
  return (
    <span
      role="status"
      aria-live="polite"
      className="inline-flex flex-shrink-0 items-center gap-1.5 whitespace-nowrap text-[0.75rem] font-medium"
      style={{ color: ui.labelColor }}
    >
      {ui.glyph}
      {t(ui.labelKey)}
      {status === 'running' && elapsed && (
        <span aria-hidden className="text-[0.6875rem]" style={{ fontFamily: MONO, color: 'var(--color-text-quaternary)' }}>
          {elapsed}
        </span>
      )}
    </span>
  );
}

interface MissionPanelProps {
  /** Small mono kicker naming the workspace the run belongs to. */
  eyebrow: string;
  /** Research question. */
  question: string;
  /** Border color of the shell. */
  border: string;
  /** Right-aligned header content (status indicator or "awaiting approval" label). */
  statusSlot: React.ReactNode;
  /** Shell entrance animation, forwarded to the motion shell. */
  animate: MotionProps['animate'];
  transition: MotionProps['transition'];
  /** Footer content beneath the question (open-thread affordance or approve/decline). */
  children?: React.ReactNode;
}

/**
 * Shared work-order chrome for the pending + approved cards: the motion shell,
 * kicker, and question. Both cards layout-match exactly; only the header status
 * slot, border, and footer differ, so those are slots. Deliberately quiet — the
 * card sits inside a chat transcript, so the question renders at body size and
 * the workspace name is small mono metadata, not a display headline.
 */
function MissionPanel({
  eyebrow,
  question,
  border,
  statusSlot,
  animate,
  transition,
  children,
}: MissionPanelProps) {
  return (
    <motion.div
      initial={{ opacity: 0, y: 6 }}
      animate={animate}
      transition={transition}
      className="relative overflow-hidden rounded-lg"
      style={{
        border: `1px solid ${border}`,
        background: PANEL_BG,
      }}
    >
      <div className="relative px-4 pb-3 pt-3">
        <div className="mb-2 flex items-center justify-between gap-3">
          <span
            className="min-w-0 truncate text-[0.6563rem] font-medium uppercase"
            style={{ fontFamily: MONO, letterSpacing: '0.08em', color: 'var(--color-text-quaternary)' }}
          >
            {eyebrow}
          </span>
          {statusSlot}
        </div>

        <div className="text-[0.875rem] font-medium leading-snug" style={{ color: 'var(--color-text-primary)' }}>
          {question}
        </div>

        {children}
      </div>
    </motion.div>
  );
}

/**
 * PTCAgentCard — inline HITL card for dispatching a background PTC run.
 *
 *   pending  — question + report-back toggle + Approve/Decline
 *   approved — live work-order panel that tracks the dispatched thread's /status
 *              (starting → running → completed/needs-input/failed/stopped)
 *   rejected — quiet collapsed "Research declined" row
 */
function PTCAgentCard({ proposalData, onApprove, onReject, flashContext }: PTCAgentCardProps) {
  const { t } = useTranslation();
  const [collapsed, setCollapsed] = useState(true);
  const [reportBack, setReportBack] = useState(proposalData?.report_back ?? true);
  const navigate = useNavigate();
  const detailId = useId();

  const status = proposalData?.status;
  const isApproved = status === 'approved';
  const threadId = proposalData?.thread_id;

  const { status: dispatchStatus } = useDispatchStatus(threadId, isApproved && !!threadId);
  const elapsedSecs = useElapsedSeconds(isApproved && dispatchStatus === 'running');

  if (!proposalData) return null;

  const { workspace_name, question, workspace_id } = proposalData;
  // The kicker names which workspace the run belongs to — PTC runs aren't only
  // "deep research", so we surface the workspace's real name (resolved by the
  // backend). Empty when unknown rather than a fixed placeholder string.
  const eyebrow = workspace_name?.trim() || '';

  const openThread = () => {
    if (!threadId) return;
    navigate(`/chat/t/${threadId}`, {
      state: {
        ...(workspace_id ? { workspaceId: workspace_id } : {}),
        ...(flashContext ? { fromThreadId: flashContext.threadId, fromWorkspaceId: flashContext.workspaceId } : {}),
      },
    });
  };

  // ---------------- Rejected: quiet collapsible row ----------------
  if (status === 'rejected') {
    return (
      <div>
        <button onClick={() => setCollapsed((v) => !v)} aria-expanded={!collapsed} aria-controls={detailId} className="flex w-full cursor-pointer items-center gap-2 py-1 text-left">
          <motion.div animate={{ rotate: collapsed ? 0 : 90 }} transition={{ duration: 0.2 }}>
            <ChevronRight className="h-3.5 w-3.5 flex-shrink-0" style={{ color: 'var(--color-icon-muted)' }} />
          </motion.div>
          <X className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
          <span className="text-sm" style={{ color: 'var(--color-text-tertiary)' }}>{t('chat.ptcCard.researchDeclined')}</span>
        </button>
        <AnimatePresence initial={false}>
          {!collapsed && (
            <motion.div
              id={detailId}
              initial={{ height: 0, opacity: 0 }}
              animate={{ height: 'auto', opacity: 1 }}
              exit={{ height: 0, opacity: 0 }}
              transition={{ duration: 0.25, ease: [0.22, 1, 0.36, 1] }}
              className="overflow-hidden"
            >
              <div className="pb-1 pl-6 pt-2">
                <div className="rounded-lg px-4 py-3" style={{ border: '1px solid var(--color-border-muted)', opacity: 0.6 }}>
                  {workspace_name && <div className="mb-1 text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>{workspace_name}</div>}
                  <div className="text-sm" style={{ color: 'var(--color-text-tertiary)' }}>{question}</div>
                </div>
              </div>
            </motion.div>
          )}
        </AnimatePresence>
      </div>
    );
  }

  // ---------------- Approved: live work-order panel ----------------
  if (isApproved) {
    const ui = STATUS_UI[dispatchStatus];
    const elapsed = dispatchStatus === 'running' ? fmtElapsed(elapsedSecs) : null;

    return (
      <MissionPanel
        eyebrow={eyebrow}
        question={question}
        border={ui.live ? 'var(--color-border-default)' : 'var(--color-border-muted)'}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.35, ease: [0.22, 1, 0.36, 1] }}
        statusSlot={<StatusIndicator status={dispatchStatus} elapsed={elapsed} />}
      >
        {threadId && (
          <div className="mt-3 flex items-center justify-between gap-3 pt-2.5" style={{ borderTop: '1px solid var(--color-border-muted)' }}>
            <span className="text-[0.75rem]" style={{ color: 'var(--color-text-quaternary)' }}>{ui.hintKey ? t(ui.hintKey) : ''}</span>
            <button
              onClick={openThread}
              className="group inline-flex flex-shrink-0 items-center gap-1 text-[0.7813rem] font-medium transition-opacity hover:opacity-80"
              style={{ color: ui.ctaAccent ? 'var(--color-accent-primary)' : 'var(--color-text-tertiary)' }}
            >
              {t(ui.ctaKey)}
              <ArrowRight aria-hidden className="h-3.5 w-3.5 transition-transform group-hover:translate-x-0.5" />
            </button>
          </div>
        )}
      </MissionPanel>
    );
  }

  // ---------------- Pending: quieter version of the panel ----------------
  return (
    <MissionPanel
      eyebrow={eyebrow}
      question={question}
      border="var(--color-border-muted)"
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.35, ease: [0.22, 1, 0.36, 1] }}
      statusSlot={
        <span className="flex-shrink-0 text-[0.6875rem] font-medium" style={{ color: 'var(--color-text-quaternary)' }}>{t('chat.ptcCard.awaitingApproval')}</span>
      }
    >
      {/* Report-back toggle */}
      <button
        type="button"
        role="switch"
        aria-checked={reportBack}
        aria-label={t('chat.ptcCard.reportBack')}
        className="mt-3 flex w-full cursor-pointer items-center justify-between rounded-md pt-2.5 outline-none focus-visible:ring-2 focus-visible:ring-[var(--color-accent-primary)]"
        style={{ borderTop: '1px solid var(--color-border-muted)' }}
        onClick={(e: React.MouseEvent) => { e.stopPropagation(); setReportBack((v) => !v); }}
      >
        <span className="text-[0.8125rem]" style={{ color: 'var(--color-text-tertiary)' }}>{t('chat.ptcCard.reportBack')}</span>
        <div aria-hidden className="relative h-[18px] w-8 rounded-full transition-colors" style={{ background: reportBack ? 'var(--color-accent-primary)' : 'var(--color-border-muted)' }}>
          {/* Hairline ring + drop shadow keep the knob legible on the very light
              OFF track in light theme (where a flat white knob nearly vanishes)
              without dimming the ON-state look in either theme. */}
          <div
            className="absolute left-[3px] top-[3px] h-3 w-3 rounded-full bg-white transition-transform"
            style={{ transform: reportBack ? 'translateX(14px)' : 'translateX(0)', boxShadow: '0 1px 2px rgba(0,0,0,0.25), 0 0 0 0.5px rgba(0,0,0,0.12)' }}
          />
        </div>
      </button>

      {/* Actions — quiet text buttons, no motion bounce. */}
      <div className="flex items-center gap-2 pt-3">
        <button
          onClick={(e: React.MouseEvent) => { e.stopPropagation(); onApprove?.({ report_back: reportBack }); }}
          className="rounded-md px-3.5 py-1.5 text-[0.8125rem] font-medium transition-[filter] hover:brightness-110"
          style={{ backgroundColor: 'var(--color-btn-primary-bg)', color: 'var(--color-btn-primary-text)' }}
        >
          {t('chat.ptcCard.approve')}
        </button>
        <button
          onClick={(e: React.MouseEvent) => { e.stopPropagation(); onReject?.(); }}
          className="rounded-md px-3.5 py-1.5 text-[0.8125rem] font-medium transition-colors hover:bg-[var(--color-bg-hover)]"
          style={{ border: '1px solid var(--color-border-default)', color: 'var(--color-text-tertiary)' }}
        >
          {t('chat.ptcCard.decline')}
        </button>
      </div>
    </MissionPanel>
  );
}

export default PTCAgentCard;
