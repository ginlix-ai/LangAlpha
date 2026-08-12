import React, { useState, useRef, useCallback } from 'react';
import { useTranslation } from 'react-i18next';
import { AlertCircle, MessageSquarePlus, Send, X } from 'lucide-react';
import { Loader } from '@/components/ui/loader';
import { cn } from '../../../lib/utils';
import iconRobo from '../../../assets/img/icon-robo.png';
import iconRoboSing from '../../../assets/img/icon-robo-sing.png';
import Markdown from './Markdown';
import { sendSubagentMessage } from '../utils/api';
import { deriveSubagentStatus } from '../session/subagents/subagentStatus';
import { SubagentStatusIcon } from './taskStatusUi';
import './NavigationPanel.css';

interface AgentMessage {
  role: string;
  isStreaming?: boolean;
  toolCallProcesses?: Record<string, { isInProgress?: boolean; toolName?: string; [key: string]: unknown }>;
  [key: string]: unknown;
}

interface Agent {
  name?: string;
  description?: string;
  type?: string;
  status?: string;
  /** Ledger failure reason for an errored task — surfaced below the header. */
  error?: string;
  currentTool?: string;
  toolCalls?: number;
  messages?: AgentMessage[];
  [key: string]: unknown;
}

interface SubagentStatusBarProps {
  agent: Agent | null;
  threadId: string;
  onInstructionSent?: (text: string) => void;
}

/**
 * SubagentStatusBar Component
 *
 * Replaces the chat input area when viewing a subagent tab.
 * Shows agent avatar, name, description, status, and current tool.
 * Includes an expandable input for sending instructions to running subagents.
 */
function SubagentStatusBar({ agent, threadId, onInstructionSent }: SubagentStatusBarProps): React.ReactElement | null {
  const { t } = useTranslation();
  const [inputOpen, setInputOpen] = useState(false);
  const [inputValue, setInputValue] = useState('');
  const [sending, setSending] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);

  const handleSend = useCallback(async (): Promise<void> => {
    const text = inputValue.trim();
    const tId = agent?.name?.replace('Task-', '') || null;
    // 'completed', 'cancelled' and 'error' are all terminal — no steering a
    // settled task (an optimistic instruction would render before the
    // backend rejects it).
    if (!text || sending || !threadId || !tId || agent?.status === 'completed' || agent?.status === 'cancelled' || agent?.status === 'error') return;

    // Immediately show pending message in the subagent view
    onInstructionSent?.(text);

    setSending(true);
    setInputValue('');
    setInputOpen(false);
    try {
      await sendSubagentMessage(threadId, tId, text);
    } catch (err) {
      console.error('[SubagentStatusBar] Failed to send message:', err);
    } finally {
      setSending(false);
    }
  }, [inputValue, sending, threadId, agent?.name, agent?.status, onInstructionSent]);

  const handleKeyDown = useCallback((e: React.KeyboardEvent<HTMLInputElement>): void => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
    if (e.key === 'Escape') {
      setInputOpen(false);
      setInputValue('');
    }
  }, [handleSend]);

  if (!agent) return null;

  const messages = (agent.messages || []) as AgentMessage[];

  const lastAssistant = [...messages].reverse().find(m => m.role === 'assistant');

  // Derive current tool from message state
  const derivedCurrentTool = ((): string => {
    if (agent.currentTool) return agent.currentTool;
    if (!lastAssistant?.toolCallProcesses) return '';
    const inProgress = Object.values(lastAssistant.toolCallProcesses).find(p => p.isInProgress);
    return inProgress?.toolName || '';
  })();

  // Shared derivation (also used by the nav tree): terminal card statuses are
  // authoritative, everything else displays as running.
  const effectiveStatus = deriveSubagentStatus(agent);

  const isActive = effectiveStatus === 'active';
  const isCompleted = effectiveStatus === 'completed';
  const isCancelled = effectiveStatus === 'cancelled';
  const isError = effectiveStatus === 'error';
  const isTerminal = isCompleted || isCancelled || isError;

  // Extract task ID from display ID (e.g. "Task-k7Xm2p" -> "k7Xm2p")
  const taskId = agent.name?.replace('Task-', '') || null;

  // Can send: subagent is not terminal (running/initializing), with thread + task.
  const canSend = !isTerminal && threadId && taskId != null;

  // Terminal outcome wins over the derived current tool: a task reaped
  // mid-tool-call leaves that call forever "in progress", but the run is
  // done — it must not still spin. Below terminal, a current tool is itself
  // liveness, so it promotes a still-silent task to the running treatment.
  const iconStatus = !isTerminal && derivedCurrentTool ? 'active' : effectiveStatus;

  // Plain terminal/running labels reuse the chat.taskCard.status* vocabulary
  // (same table STATUS_UI reads) so the bar and the inline cards can't drift;
  // only the bar-specific composites live under chat.subagentBar.
  const getStatusText = (): string => {
    // Terminal outcome wins over the derived current tool, as above.
    if (isCompleted) {
      if (agent.toolCalls && agent.toolCalls > 0) {
        return t('chat.subagentBar.completedWithTools', { count: agent.toolCalls });
      }
      return t('chat.taskCard.statusCompleted');
    }
    if (isCancelled) {
      return t('chat.taskCard.statusStopped');
    }
    if (isError) {
      return t('chat.taskCard.statusFailed');
    }
    if (derivedCurrentTool) {
      return t('chat.subagentBar.runningWithTool', { tool: derivedCurrentTool });
    }
    if (isActive) {
      return t('chat.taskCard.statusRunning');
    }
    return t('chat.subagentBar.initializing');
  };

  return (
    <div className="space-y-2">
      <div
        className="flex items-center gap-3 px-4 py-3 rounded-lg"
        style={{
          backgroundColor: 'var(--color-border-muted)',
          border: '1px solid var(--color-border-muted)',
        }}
      >
        {/* Agent avatar */}
        <div
          className={cn(
            "w-10 h-10 rounded-lg flex items-center justify-center flex-shrink-0",
            isActive && !isCompleted && "nav-panel-agent-pulse"
          )}
          style={{
            backgroundColor: isActive && !isCompleted
              ? 'var(--color-accent-soft)'
              : 'var(--color-border-muted)',
          }}
        >
          <img
            src={isTerminal ? iconRobo : iconRoboSing}
            alt={t('chat.subagentBar.avatarAlt')}
            className="h-5 w-5 subagent-avatar-glyph"
          />
        </div>

        {/* Info */}
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2">
            <span className="text-sm font-medium truncate" style={{ color: 'var(--color-text-primary)' }}>
              {agent.name}
            </span>
            <span
              className="text-xs px-1.5 py-0.5 rounded"
              style={{
                backgroundColor: 'var(--color-border-muted)',
                color: 'var(--color-text-tertiary)',
              }}
            >
              {agent.type}
            </span>
          </div>
          {agent.description && (
            <div
              className="mt-0.5"
              style={{
                color: 'var(--color-text-tertiary)',
                display: '-webkit-box',
                WebkitLineClamp: 2,
                WebkitBoxOrient: 'vertical',
                overflow: 'hidden',
              }}
            >
              <Markdown variant="compact" content={agent.description} className="text-xs" />
            </div>
          )}
        </div>

        {/* Right side: status + instruction button stacked */}
        <div className="flex flex-col items-end gap-1.5 flex-shrink-0">
          <div className="flex items-center gap-1.5">
            <SubagentStatusIcon status={iconStatus} surface="statusBar" className="h-4 w-4" />
            <span className="text-xs whitespace-nowrap" style={{ color: isCompleted ? 'var(--color-accent-primary)' : 'var(--color-text-tertiary)' }}>
              {getStatusText()}
            </span>
          </div>
          {canSend && !inputOpen && (
            <button
              onClick={() => {
                setInputOpen(true);
                setTimeout(() => inputRef.current?.focus(), 50);
              }}
              className="flex items-center gap-1.5 px-2.5 py-1.5 rounded-md text-xs transition-colors"
              style={{
                backgroundColor: 'var(--color-bg-elevated)',
                color: 'var(--color-text-tertiary)',
                border: '1px solid var(--color-border-elevated)',
              }}
              onMouseEnter={(e: React.MouseEvent<HTMLButtonElement>) => {
                e.currentTarget.style.color = 'var(--color-text-primary)';
              }}
              onMouseLeave={(e: React.MouseEvent<HTMLButtonElement>) => {
                e.currentTarget.style.color = 'var(--color-text-tertiary)';
              }}
            >
              <MessageSquarePlus className="h-3.5 w-3.5" />
              <span>{t('chat.subagentBar.instruct')}</span>
            </button>
          )}
        </div>
      </div>

      {/* Failure reason — the "clue inside" for a Failed card. The status chip
          alone said Failed with no cause; the ledger's reason lands here. */}
      {isError && (
        <div
          className="flex items-start gap-2 px-4 py-2.5 rounded-lg"
          style={{
            backgroundColor: 'var(--color-loss-soft)',
            border: '1px solid var(--color-border-loss)',
          }}
        >
          <AlertCircle className="h-4 w-4 flex-shrink-0 mt-0.5" style={{ color: 'var(--color-icon-danger)' }} />
          <div className="min-w-0">
            <div className="text-xs font-medium" style={{ color: 'var(--color-icon-danger)' }}>
              {t('chat.subagentBar.errorHeading')}
            </div>
            {agent.error && (
              <div className="text-xs mt-0.5 break-words" style={{ color: 'var(--color-text-tertiary)' }}>
                {agent.error}
              </div>
            )}
          </div>
        </div>
      )}

      {/* Expandable instruction input — canSend gates it so an input left
          open when the task reaches terminal (e.g. errors) disappears. */}
      {inputOpen && canSend && (
        <div
          className="flex items-center gap-2 px-3 py-2 rounded-lg"
          style={{
            backgroundColor: 'var(--color-border-muted)',
            border: '1px solid var(--color-border-elevated)',
          }}
        >
          <input
            ref={inputRef}
            type="text"
            value={inputValue}
            onChange={(e: React.ChangeEvent<HTMLInputElement>) => setInputValue(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder={t('chat.subagentBar.inputPlaceholder')}
            disabled={sending}
            className="flex-1 bg-transparent text-sm placeholder-foreground/30 outline-none"
            style={{ color: 'var(--color-text-primary)' }}
          />
          <div className="flex items-center gap-1">
            <button
              onClick={() => { setInputOpen(false); setInputValue(''); }}
              disabled={sending}
              className="p-1 rounded transition-colors"
              style={{ color: 'var(--color-text-tertiary)' }}
              onMouseEnter={(e: React.MouseEvent<HTMLButtonElement>) => { e.currentTarget.style.color = 'var(--color-text-primary)'; }}
              onMouseLeave={(e: React.MouseEvent<HTMLButtonElement>) => { e.currentTarget.style.color = 'var(--color-text-tertiary)'; }}
            >
              <X className="h-4 w-4" />
            </button>
            <button
              onClick={handleSend}
              disabled={!inputValue.trim() || sending}
              className="p-1 rounded transition-colors"
              style={{
                color: inputValue.trim() && !sending ? 'var(--color-accent-primary)' : 'var(--color-icon-muted)',
              }}
              onMouseEnter={(e: React.MouseEvent<HTMLButtonElement>) => {
                if (inputValue.trim() && !sending) e.currentTarget.style.color = 'var(--color-accent-primary)';
              }}
              onMouseLeave={(e: React.MouseEvent<HTMLButtonElement>) => {
                if (inputValue.trim() && !sending) e.currentTarget.style.color = 'var(--color-accent-primary)';
              }}
            >
              {sending ? <Loader size={16} className="text-current" /> : <Send className="h-4 w-4" />}
            </button>
          </div>
        </div>
      )}
    </div>
  );
}

export default SubagentStatusBar;
