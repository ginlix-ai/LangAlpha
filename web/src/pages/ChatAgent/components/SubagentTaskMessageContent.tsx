import React from 'react';
import { Check, Loader2, ArrowRight, ChevronRight, RotateCw, RefreshCw } from 'lucide-react';
import { compactNumber } from '@/lib/format';
import { type SubagentTokenUsage } from '../utils/tokenUsage';

const MONO_STACK = 'ui-monospace, SFMono-Regular, Menlo, Consolas, monospace';

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

interface ToolCallProcess {
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
  action?: 'init' | 'update' | 'resume';
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
  toolCalls = 0,
  tokenUsage,
}: SubagentTaskMessageContentProps): React.ReactElement | null {
  if (!subagentId && !description) {
    return null;
  }

  const isRunning = status === 'running';
  const isCompleted = status === 'completed';
  const hasResult = isCompleted && toolCallProcess?.toolCallResult?.content;
  const summary = summarize(description);

  // Status discriminator — drives icon, label, and accent color.
  // Updated/Resumed share the warning-amber treatment with Running because
  // those are all "in-flight or recent change" states; Completed is success.
  const statusKind: 'completed' | 'running' | 'updated' | 'resumed' | 'unknown' =
    action === 'update' ? 'updated'
    : action === 'resume' ? 'resumed'
    : action === 'init' && isRunning ? 'running'
    : action === 'init' && isCompleted ? 'completed'
    : 'unknown';

  const statusColor =
    statusKind === 'completed' ? 'var(--color-success)'
    : statusKind === 'unknown' ? 'var(--color-text-tertiary)'
    : 'var(--color-warning)';

  const statusLabel =
    statusKind === 'completed' ? 'Completed'
    : statusKind === 'running' ? 'Running'
    : statusKind === 'updated' ? 'Updated'
    : statusKind === 'resumed' ? 'Resumed'
    : status;

  const StatusIcon =
    statusKind === 'completed' ? Check
    : statusKind === 'running' ? Loader2
    : statusKind === 'updated' ? RefreshCw
    : statusKind === 'resumed' ? RotateCw
    : null;

  const handleCardClick = (): void => {
    if (onOpen) {
      onOpen({ subagentId: resumeTargetId || subagentId || '', description: description || '', type, status });
    }
  };

  const handleViewOutput = (e: React.MouseEvent): void => {
    e.stopPropagation();
    if (onDetailOpen && toolCallProcess) {
      onDetailOpen(toolCallProcess);
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLDivElement>): void => {
    if (e.key === 'Enter' || e.key === ' ') {
      e.preventDefault();
      handleCardClick();
    }
  };

  return (
    <div
      role="button"
      tabIndex={0}
      style={{
        background: 'var(--color-bg-tool-card)',
        border: '1px solid var(--color-border-muted)',
        borderRadius: 12,
        overflow: 'hidden',
        cursor: 'pointer',
        fontFamily: MONO_STACK,
        transition: 'border-color 0.15s',
      }}
      onClick={handleCardClick}
      onKeyDown={handleKeyDown}
      onMouseEnter={(e: React.MouseEvent<HTMLDivElement>) => (e.currentTarget.style.borderColor = 'var(--color-border-default)')}
      onMouseLeave={(e: React.MouseEvent<HTMLDivElement>) => (e.currentTarget.style.borderColor = 'var(--color-border-muted)')}
      title={
        action === 'update' ? 'Click to view updated subagent'
        : action === 'resume' ? 'Click to view resumed subagent'
        : isRunning ? 'Click to view running subagent'
        : 'Click to view subagent details'
      }
    >
      {/* Rule: agent type · status · affordance */}
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          gap: 12,
          padding: '10px 12px 8px 14px',
          borderBottom: '1px solid var(--color-border-subtle)',
          fontSize: 12,
        }}
      >
        <span
          style={{
            color: 'var(--color-text-secondary)',
            fontWeight: 500,
            textTransform: 'lowercase',
            overflow: 'hidden',
            textOverflow: 'ellipsis',
            whiteSpace: 'nowrap',
            minWidth: 0,
            flex: '0 1 auto',
          }}
        >
          {type}
        </span>
        <span style={{ flex: 1 }} />
        <span
          style={{
            display: 'inline-flex',
            alignItems: 'center',
            gap: 5,
            color: statusColor,
            fontSize: 11,
            letterSpacing: '0.04em',
            fontWeight: 500,
            whiteSpace: 'nowrap',
          }}
        >
          {StatusIcon && (
            <StatusIcon
              style={{
                width: 11,
                height: 11,
                animation: statusKind === 'running' ? 'spin 1s linear infinite' : undefined,
              }}
            />
          )}
          {statusLabel}
        </span>
        {hasResult ? (
          <button
            type="button"
            aria-label="View subagent output"
            onClick={handleViewOutput}
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
        ) : (
          <ChevronRight
            aria-hidden="true"
            style={{
              width: 14,
              height: 14,
              flexShrink: 0,
              color: 'var(--color-text-quaternary)',
            }}
          />
        )}
      </div>

      {/* Body: description + telemetry */}
      <div style={{ padding: '12px 14px 14px' }}>
        <div
          style={{
            fontFamily:
              "'Inter', system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif",
            fontSize: 14,
            fontWeight: 500,
            color: 'var(--color-text-primary)',
            overflow: 'hidden',
            textOverflow: 'ellipsis',
            whiteSpace: 'nowrap',
            marginBottom: (toolCalls > 0 || (tokenUsage?.total ?? 0) > 0) ? 8 : 0,
          }}
        >
          {summary || 'Subagent Task'}
        </div>

        {(toolCalls > 0 || (tokenUsage?.total ?? 0) > 0) && (
          <div
            data-testid="subagent-telemetry"
            style={{
              display: 'flex',
              gap: 8,
              fontSize: 11,
              color: 'var(--color-text-tertiary)',
              fontFamily: MONO_STACK,
              letterSpacing: '0.02em',
            }}
          >
            {toolCalls > 0 && (
              <span>
                <strong style={{ color: 'var(--color-text-secondary)', fontWeight: 600 }}>{toolCalls}</strong>
                {' '}
                {toolCalls === 1 ? 'tool' : 'tools'}
              </span>
            )}
            {(tokenUsage?.total ?? 0) > 0 && (
              <span title={`${tokenUsage!.input} in · ${tokenUsage!.output} out`}>
                {toolCalls > 0 ? '· ' : ''}
                <strong style={{ color: 'var(--color-text-secondary)', fontWeight: 600 }}>{compactNumber(tokenUsage!.total)}</strong>
                {' '}
                tokens
              </span>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

export default SubagentTaskMessageContent;
