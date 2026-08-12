import React from 'react';
import { useTranslation } from 'react-i18next';
import { ArrowRight } from 'lucide-react';
import { compactNumber } from '@/lib/format';
import { type SubagentTokenUsage } from '../utils/tokenUsage';
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
    : action === 'init' ? taskCardStatusKind(status)
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
    </TaskCardShell>
  );
}

export default SubagentTaskMessageContent;
