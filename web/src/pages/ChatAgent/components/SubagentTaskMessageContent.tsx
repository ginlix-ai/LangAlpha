import React from 'react';
import { useTranslation } from 'react-i18next';
import { ArrowRight, PauseCircle } from 'lucide-react';
import { compactNumber } from '@/lib/format';
import { ErrorLink } from '@/components/ui/error-banner';
import { CREDIT_STOP_ERROR_TYPE } from '@/types/sse';
import { buildRateLimitError } from '@/utils/rateLimitError';
import { type SubagentTokenUsage } from '../utils/tokenUsage';
import { useCreditPausePending } from './CreditPausePendingContext';
import { useSubagentTelemetry } from './SubagentTelemetryContext';
import TaskCardShell, { MONO_STACK } from './TaskCardShell';
import { taskCardStatusKind, type TaskCardStatusKind } from './taskStatusUi';

/**
 * Extract a short one-line summary from a full task description.
 * Takes the first sentence or first line, truncated to maxLen chars.
 */
function summarize(text: string | undefined, maxLen = 100): string {
  if (!text || typeof text !== 'string') return '';
  const firstLine = text.split(/\n/)[0].trim();
  const cleaned = firstLine.replace(/:$/, '');
  if (cleaned.length <= maxLen) return cleaned;
  return cleaned.slice(0, maxLen).replace(/\s+\S*$/, '') + '…';
}

// Neither reason is bounded: the credit denial is the platform's copy and the
// rest are exception text, so one long enough would push a card's own content
// out of shape.
const STOP_REASON_MAX_CHARS = 180;

function clampReason(text: string): string {
  return text.length > STOP_REASON_MAX_CHARS
    ? `${text.slice(0, STOP_REASON_MAX_CHARS).trimEnd()}\u2026`
    : text;
}

function accountLinks(reason: string) {
  // Same builder the pause card uses, so the two surfaces send the user to the
  // same pages with the same wording rather than growing a second answer.
  return buildRateLimitError(
    { message: reason },
    (import.meta.env.VITE_PLATFORM_URL as string | undefined) || '/account',
  ).links;
}

/**
 * A reason the user cannot act on, as a line inside the card: transport lost,
 * a handler that raised. Worth saying, not worth interrupting for.
 */
function TaskStopReason({ reason }: { reason: string }): React.ReactElement {
  return (
    <div
      data-testid="subagent-stop-reason"
      style={{
        marginTop: 8,
        fontSize: '0.6875rem',
        lineHeight: 1.5,
        color: 'var(--color-text-tertiary)',
        minWidth: 0,
        wordBreak: 'break-word',
      }}
    >
      {clampReason(reason)}
    </div>
  );
}

/**
 * A credit stop, as a notice at the foot of the message in the MAIN transcript.
 *
 * A background task can outlive the turn that spawned it, and when the credit
 * gate stops one the turn is already finished: there is no model boundary left
 * to interrupt, so no pause card is ever raised in the thread. The denial then
 * reaches only the task's own transcript, behind a click, while the thread
 * shows a "Stopped" chip indistinguishable from a task the user ended.
 *
 * Deliberately outside the card rather than a line within it. Inside, it reads
 * as a footnote on one task; the thing that actually happened is that the
 * account ran out of money, which is the turn's news and the user's to act on.
 * It borrows the pause card's layout for that reason - the same event should
 * not look like two different kinds of event depending on when it landed. The
 * placement is MessageContentSegments' call, and the reason it is last is
 * written there.
 */
export function SubagentStopNotice({ subagentId }: { subagentId: string | undefined }): React.ReactElement | null {
  const { t } = useTranslation();
  const telemetry = useSubagentTelemetry(subagentId);
  const reason = telemetry?.stopReason;
  if (!reason || telemetry?.stopReasonType !== CREDIT_STOP_ERROR_TYPE) return null;
  const links = accountLinks(reason);
  return (
    <div
      data-testid="subagent-credit-stop-notice"
      className="mt-2 rounded-lg px-4 py-3 space-y-2"
      style={{ border: '1px solid var(--color-border-muted)' }}
    >
      <div className="flex items-center gap-2">
        <PauseCircle className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-accent-light)' }} />
        <span className="text-[0.9375rem] font-medium" style={{ color: 'var(--color-text-primary)' }}>
          {t('chat.creditStop.title')}
        </span>
      </div>
      <div className="text-sm break-words" style={{ color: 'var(--color-text-secondary)' }}>
        {clampReason(reason)}
      </div>
      {links && links.length > 0 && (
        <div className="flex items-center gap-4 text-sm" style={{ color: 'var(--color-accent-light)' }}>
          {links.map((l) => (
            <ErrorLink key={`${l.url}|${l.label}`} {...l} />
          ))}
        </div>
      )}
    </div>
  );
}

export interface ToolCallProcess {
  toolCallResult?: {
    content?: unknown;
    [key: string]: unknown;
  };
  [key: string]: unknown;
}

interface SubagentInfo {
  subagentId: string;
  description: string;
  type: string;
  status: string;
}

interface SubagentTaskMessageContentProps {
  subagentId?: string;
  description?: string;
  type?: string;
  status?: string;
  /** Card verb from the task record. An unrecognized wire spelling is not
   *  coerced — it falls through the ladder below to `unknown`, which shows the
   *  raw status rather than inventing a state. */
  action?: string;
  resumeTargetId?: string;
  onOpen?: (info: SubagentInfo) => void;
  onDetailOpen?: (process: ToolCallProcess) => void;
  toolCallProcess?: ToolCallProcess;
  /** Live tool-call count for this subagent — derived from card state at the call site. */
  toolCalls?: number;
  /** Live cumulative token usage for this subagent — derived from card state at the call site. */
  tokenUsage?: SubagentTokenUsage;
}

/**
 * Inline subagent card. Trading-terminal-style row: agent type on the left of
 * the rule, semantic status on the right, description and live telemetry
 * (tools / tokens) in the body. Adapts to light/dark via design tokens.
 */
function SubagentTaskMessageContent({
  subagentId,
  description,
  type = 'general-purpose',
  status = 'unknown',
  action = 'init',
  resumeTargetId,
  onOpen,
  onDetailOpen,
  toolCallProcess,
  toolCalls: toolCallsProp,
  tokenUsage: tokenUsageProp,
}: SubagentTaskMessageContentProps): React.ReactElement | null {
  // Subscribe to live telemetry at the leaf so token-tick re-renders bypass
  // the memoized MessageBubble / MessageContentSegments above us. Direct
  // props (used by tests and any explicit caller) take precedence over the
  // context lookup so call sites can still override or stub.
  const ctxTelemetry = useSubagentTelemetry(subagentId);
  const toolCalls = toolCallsProp ?? ctxTelemetry?.toolCalls ?? 0;
  const tokenUsage = tokenUsageProp ?? ctxTelemetry?.tokenUsage;
  const stopReason = ctxTelemetry?.stopReason;
  const stopReasonType = ctxTelemetry?.stopReasonType;
  // A still-running task keeps working until its own gate stops it at the next
  // model boundary, so while this turn holds an unanswered credit pause the
  // card says it is finishing rather than promising more.
  const pausing = useCreditPausePending();
  const { t } = useTranslation();

  if (!subagentId && !description) {
    return null;
  }

  const isRunning = status === 'running';
  const isCompleted = status === 'completed';
  // A cancelled subagent is terminal like completed (workflow stopped) — it may
  // still have captured partial output worth viewing.
  const isCancelled = status === 'cancelled';
  // The panel this opens shows the task's instructions and status, never its
  // output — the reply it used to key on is dispatch boilerplate that exists
  // from the moment the task starts, so it promised a result no panel had.
  const hasDetails = (isCompleted || isCancelled) && !!toolCallProcess;
  const hasTelemetry = toolCalls > 0 || (tokenUsage?.total ?? 0) > 0;

  // Status discriminator — drives icon, label, and accent color via STATUS_UI.
  const statusKind: TaskCardStatusKind =
    action === 'update' ? 'updated'
    : action === 'resume' ? 'resumed'
    : action === 'init' ? taskCardStatusKind(status, pausing)
    : 'unknown';

  const handleOpen = (): void => {
    onOpen?.({ subagentId: resumeTargetId || subagentId || '', description: description || '', type, status });
  };

  const handleViewDetails = (e: React.MouseEvent<HTMLButtonElement>): void => {
    e.stopPropagation();
    if (onDetailOpen && toolCallProcess) {
      onDetailOpen(toolCallProcess);
    }
  };

  return (
    <TaskCardShell
      eyebrow={type}
      statusKind={statusKind}
      rawStatus={status}
      title={summarize(description) || t('chat.subagentCard.titleFallback')}
      hint={
        onOpen
          ? t(
              action === 'update' ? 'chat.subagentCard.openUpdated'
              : action === 'resume' ? 'chat.subagentCard.openResumed'
              : isRunning ? 'chat.subagentCard.openRunning'
              : 'chat.subagentCard.openDetails'
            )
          : undefined
      }
      onOpen={onOpen ? handleOpen : undefined}
      affordance={hasDetails && onDetailOpen ? (
        <button
          type="button"
          aria-label={t('chat.subagentCard.viewDetails')}
          onClick={handleViewDetails}
          style={{
            background: 'transparent',
            border: 'none',
            padding: 0,
            display: 'inline-flex',
            alignItems: 'center',
            cursor: 'pointer',
            color: 'var(--color-accent-primary)',
            flexShrink: 0,
          }}
        >
          <ArrowRight style={{ width: 14, height: 14 }} />
        </button>
      ) : undefined}
    >
      {hasTelemetry && (
        <div
          data-testid="subagent-telemetry"
          style={{
            display: 'flex',
            gap: 8,
            marginTop: 8,
            fontSize: '0.6875rem',
            color: 'var(--color-text-tertiary)',
            fontFamily: MONO_STACK,
            letterSpacing: '0.02em',
          }}
        >
          {toolCalls > 0 && (
            <span>
              <strong style={{ color: 'var(--color-text-secondary)', fontWeight: 600 }}>{toolCalls}</strong>
              {' '}
              {t('chat.subagentCard.toolUnit', { count: toolCalls })}
            </span>
          )}
          {(tokenUsage?.total ?? 0) > 0 && (
            <span title={`${tokenUsage!.input} in · ${tokenUsage!.output} out`}>
              {toolCalls > 0 ? '· ' : ''}
              <strong style={{ color: 'var(--color-text-secondary)', fontWeight: 600 }}>{compactNumber(tokenUsage!.total)}</strong>
              {' '}
              {t('chat.subagentCard.tokenUnit')}
            </span>
          )}
        </div>
      )}
      {/* Terminal only: a reason on a card still claiming to run would read as
          a prediction. `running` covers `pausing`, which is a running task.
          A credit stop is excluded here because it gets the notice at the foot
          of the message instead; saying it twice would make the notice look
          optional. */}
      {stopReason
        && stopReasonType !== CREDIT_STOP_ERROR_TYPE
        && statusKind !== 'running'
        && statusKind !== 'pausing' && (
        <TaskStopReason reason={stopReason} />
      )}
    </TaskCardShell>
  );
}

export default SubagentTaskMessageContent;
