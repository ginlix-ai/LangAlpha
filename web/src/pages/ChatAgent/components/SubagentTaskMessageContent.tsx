import React from 'react';
import { Check, Loader2, ArrowRight, RotateCw, RefreshCw } from 'lucide-react';
import iconRobo from '../../../assets/img/icon-robo.png';
import iconRoboSing from '../../../assets/img/icon-robo-sing.png';
import { compactNumber } from '@/lib/format';
import { type SubagentTokenUsage } from '../utils/tokenUsage';
import './NavigationPanel.css';

/**
 * Extract a short one-line summary from a full task description.
 * Takes the first sentence or first line, truncated to maxLen chars.
 */
function summarize(text: string | undefined, maxLen = 100): string {
  if (!text || typeof text !== 'string') return '';
  // Take first line only
  const firstLine = text.split(/\n/)[0].trim();
  // Remove trailing colon (often "Research X comprehensively. Cover:")
  const cleaned = firstLine.replace(/:$/, '');
  if (cleaned.length <= maxLen) return cleaned;
  return cleaned.slice(0, maxLen).replace(/\s+\S*$/, '') + '\u2026';
}

const CARD_BORDER = 'var(--color-border-muted)';

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
 * SubagentTaskMessageContent Component
 *
 * Renders a compact, clickable card in the main chat view to indicate that
 * a background subagent task was launched or resumed (via the `task` tool).
 * Uses the same visual style as inline artifact cards (company overview, etc.).
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

  const handleCardClick = (): void => {
    if (onOpen) {
      // For resume cards, open the original subagent's tab if possible
      onOpen({ subagentId: resumeTargetId || subagentId || '', description: description || '', type, status });
    }
  };

  const handleViewOutput = (e: React.MouseEvent): void => {
    e.stopPropagation();
    if (onDetailOpen && toolCallProcess) {
      onDetailOpen(toolCallProcess);
    }
  };

  return (
    <div
      style={{
        background: 'var(--color-bg-tool-card)',
        border: `1px solid ${CARD_BORDER}`,
        borderRadius: 8,
        padding: '12px 14px',
        cursor: 'pointer',
        transition: 'border-color 0.15s',
      }}
      onClick={handleCardClick}
      onMouseEnter={(e: React.MouseEvent<HTMLDivElement>) => (e.currentTarget.style.borderColor = 'var(--color-border-muted)')}
      onMouseLeave={(e: React.MouseEvent<HTMLDivElement>) => (e.currentTarget.style.borderColor = CARD_BORDER)}
      title={action === 'update' ? 'Click to view updated subagent' : action === 'resume' ? 'Click to view resumed subagent' : isRunning ? 'Click to view running subagent' : 'Click to view subagent details'}
    >
      {/* Top row: icon + summary text */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 8 }}>
        <div style={{ position: 'relative', flexShrink: 0 }}>
          <img
            src={isCompleted ? iconRobo : iconRoboSing}
            alt="Subagent"
            className={isRunning ? 'nav-panel-agent-pulse' : ''}
            style={{ width: 20, height: 20 }}
          />
          {isRunning && (
            <Loader2
              style={{
                width: 10, height: 10,
                position: 'absolute', bottom: -2, right: -2,
                color: 'var(--color-accent-primary)',
                animation: 'spin 1s linear infinite',
              }}
            />
          )}
        </div>
        <span style={{ fontWeight: 600, color: 'var(--color-text-primary)', fontSize: 14, flex: 1, minWidth: 0, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
          {summary || 'Subagent Task'}
        </span>
        {!!hasResult && (
          <ArrowRight
            style={{ width: 14, height: 14, flexShrink: 0, color: 'var(--color-accent-primary)' }}
            onClick={handleViewOutput}
          />
        )}
      </div>

      {/* Bottom row: type badge + status */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
        <span style={{ fontSize: 12, color: 'var(--color-text-tertiary)' }}>
          {type}
        </span>
        <span style={{ display: 'flex', alignItems: 'center', gap: 4, fontSize: 12, color: (action === 'update' || action === 'resume') ? 'var(--color-warning)' : isRunning ? 'var(--color-accent-primary)' : isCompleted ? 'var(--color-accent-primary)' : 'var(--color-text-tertiary)' }}>
          {action === 'update' && <RefreshCw style={{ width: 12, height: 12 }} />}
          {action === 'resume' && <RotateCw style={{ width: 12, height: 12 }} />}
          {action === 'init' && isRunning && <Loader2 style={{ width: 12, height: 12, animation: 'spin 1s linear infinite' }} />}
          {action === 'init' && isCompleted && <Check style={{ width: 12, height: 12 }} />}
          {action === 'update' ? 'Updated' : action === 'resume' ? 'Resumed' : isRunning ? 'Running' : isCompleted ? 'Completed' : status}
        </span>
      </div>
      {/* Telemetry row: live tool-call count + cumulative token total. Hidden
          when both are zero so freshly-spawned cards stay visually quiet. */}
      {(toolCalls > 0 || (tokenUsage?.total ?? 0) > 0) && (
        <div
          data-testid="subagent-telemetry"
          style={{
            marginTop: 6,
            display: 'flex',
            gap: 10,
            fontSize: 11,
            fontFamily: 'ui-monospace, SFMono-Regular, Menlo, Consolas, monospace',
            color: 'var(--color-text-tertiary)',
          }}
        >
          {toolCalls > 0 && <span>{toolCalls} {toolCalls === 1 ? 'tool' : 'tools'}</span>}
          {(tokenUsage?.total ?? 0) > 0 && (
            <span title={`${tokenUsage!.input} in · ${tokenUsage!.output} out`}>
              {toolCalls > 0 ? '· ' : ''}{compactNumber(tokenUsage!.total)} tokens
            </span>
          )}
        </div>
      )}
    </div>
  );
}

export default SubagentTaskMessageContent;
