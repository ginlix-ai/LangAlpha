import type React from 'react';
import { useTranslation } from 'react-i18next';
import { AlertTriangle, CheckCircle2, Circle, Loader2, StopCircle } from 'lucide-react';
import { deriveSubagentStatus } from '../../session/subagents/subagentStatus';
import type { SubagentStatusIndicatorProps } from './types';

export default function SubagentStatusIndicator({ status, currentTool, toolCalls = 0, messages = [] }: SubagentStatusIndicatorProps): React.ReactElement {
  const { t } = useTranslation();
  // Derive the in-flight tool from messages (self-sufficient, no subagent_status dependency)
  const lastAssistant = [...messages].reverse().find(m => m.role === 'assistant');

  // Derive current tool from message state
  const derivedCurrentTool = (() => {
    if (currentTool) return currentTool;
    if (!lastAssistant?.toolCallProcesses) return '';
    const inProgress = Object.values(lastAssistant.toolCallProcesses).find(p => p.isInProgress);
    return (inProgress?.toolName as string) || '';
  })();

  // Shared derivation (same as the nav tree and SubagentStatusBar, so the
  // surfaces can never disagree): terminal card statuses — completed,
  // cancelled, error — are authoritative; everything else displays as
  // running. Never derive 'completed' from message streaming gaps — those
  // are transient, especially after update/resume actions.
  const effectiveStatus = deriveSubagentStatus({ status, messages });

  // A terminal card never shows a live tool spinner — a tool process that was
  // mid-flight when the run settled is history, not activity.
  const isTerminal =
    effectiveStatus === 'completed' ||
    effectiveStatus === 'cancelled' ||
    effectiveStatus === 'error';

  const getIcon = (): React.ReactElement => {
    if (!isTerminal && derivedCurrentTool) {
      return <Loader2 className="h-3.5 w-3.5 animate-spin" style={{ color: 'var(--color-text-tertiary)' }} />;
    }
    if (effectiveStatus === 'active') {
      return <Loader2 className="h-3.5 w-3.5 animate-spin" style={{ color: 'var(--color-text-tertiary)' }} />;
    }
    if (effectiveStatus === 'completed') {
      return <CheckCircle2 className="h-3.5 w-3.5" style={{ color: 'var(--color-text-tertiary)' }} />;
    }
    if (effectiveStatus === 'cancelled') {
      return <StopCircle className="h-3.5 w-3.5" style={{ color: 'var(--color-text-tertiary)' }} />;
    }
    if (effectiveStatus === 'error') {
      return <AlertTriangle className="h-3.5 w-3.5" style={{ color: 'var(--color-text-tertiary)' }} />;
    }
    return <Circle className="h-3.5 w-3.5" style={{ color: 'var(--color-icon-muted)' }} />;
  };

  const getText = (): string => {
    if (!isTerminal && derivedCurrentTool) return t('chat.running', { tool: derivedCurrentTool });
    if (effectiveStatus === 'error') return t('common.failed');
    if (effectiveStatus === 'completed') {
      return toolCalls > 0 ? t('chat.completedWithCalls', { count: toolCalls }) : t('chat.completed');
    }
    if (effectiveStatus === 'cancelled') {
      return t('chat.stoppedChip');
    }
    if (effectiveStatus === 'active') {
      return t('chat.runningStatus');
    }
    return t('chat.initializing');
  };

  return (
    <div className="flex items-center gap-1.5 text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
      {getIcon()}
      <span>{getText()}</span>
    </div>
  );
}
