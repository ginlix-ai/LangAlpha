import React, { useState, useRef, useCallback } from 'react';
import { CheckCircle2, Circle, Loader2, MessageSquarePlus, Send, X, StopCircle } from 'lucide-react';
import { cn } from '../../../lib/utils';
import iconRobo from '../../../assets/img/icon-robo.png';
import iconRoboSing from '../../../assets/img/icon-robo-sing.png';
import Markdown from './Markdown';
import { sendSubagentMessage } from '../utils/api';
import { deriveSubagentStatus } from '../utils/subagentStatus';
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
  const [inputOpen, setInputOpen] = useState(false);
  const [inputValue, setInputValue] = useState('');
  const [sending, setSending] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);

  const handleSend = useCallback(async (): Promise<void> => {
    const text = inputValue.trim();
    const tId = agent?.name?.replace('Task-', '') || null;
    // 'completed' and 'cancelled' are both terminal — no steering a settled task.
    if (!text || sending || !threadId || !tId || agent?.status === 'completed' || agent?.status === 'cancelled') return;

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
  const isTerminal = isCompleted || isCancelled;

  // Extract task ID from display ID (e.g. "Task-k7Xm2p" -> "k7Xm2p")
  const taskId = agent.name?.replace('Task-', '') || null;

  // Can send: subagent is not terminal (running/initializing), with thread + task.
  const canSend = !isTerminal && threadId && taskId != null;

  const getStatusIcon = (): React.ReactElement => {
    if (derivedCurrentTool) {
      return <Loader2 className="h-4 w-4 animate-spin" style={{ color: 'var(--color-text-tertiary)' }} />;
    }
    if (isActive) {
      return <Loader2 className="h-4 w-4 animate-spin" style={{ color: 'var(--color-text-tertiary)' }} />;
    }
    if (isCompleted) {
      return <CheckCircle2 className="h-4 w-4" style={{ color: 'var(--color-accent-primary)' }} />;
    }
    if (isCancelled) {
      return <StopCircle className="h-4 w-4" style={{ color: 'var(--color-text-tertiary)' }} />;
    }
    return <Circle className="h-4 w-4" style={{ color: 'var(--color-icon-muted)' }} />;
  };

  const getStatusText = (): string => {
    if (derivedCurrentTool) {
      return `Running: ${derivedCurrentTool}`;
    }
    if (isCompleted) {
      if (agent.toolCalls && agent.toolCalls > 0) {
        return `Completed (${agent.toolCalls} tool calls)`;
      }
      return 'Completed';
    }
    if (isCancelled) {
      return 'Stopped';
    }
    if (isActive) {
      return 'Running';
    }
    return 'Initializing';
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
            alt="Agent"
            className="h-5 w-5"
            style={{ filter: 'brightness(0) saturate(100%) invert(100%)' }}
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
            {getStatusIcon()}
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
                backgroundColor: 'var(--color-accent-soft)',
                color: 'var(--color-text-tertiary)',
                border: '1px solid var(--color-accent-overlay)',
              }}
              onMouseEnter={(e: React.MouseEvent<HTMLButtonElement>) => {
                e.currentTarget.style.backgroundColor = 'var(--color-accent-soft)';
                e.currentTarget.style.color = 'var(--color-text-primary)';
              }}
              onMouseLeave={(e: React.MouseEvent<HTMLButtonElement>) => {
                e.currentTarget.style.backgroundColor = 'var(--color-accent-soft)';
                e.currentTarget.style.color = 'var(--color-text-tertiary)';
              }}
            >
              <MessageSquarePlus className="h-3.5 w-3.5" />
              <span>Instruct</span>
            </button>
          )}
        </div>
      </div>

      {/* Expandable instruction input */}
      {inputOpen && (
        <div
          className="flex items-center gap-2 px-3 py-2 rounded-lg"
          style={{
            backgroundColor: 'var(--color-border-muted)',
            border: '1px solid var(--color-accent-overlay)',
          }}
        >
          <input
            ref={inputRef}
            type="text"
            value={inputValue}
            onChange={(e: React.ChangeEvent<HTMLInputElement>) => setInputValue(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Add instruction for this agent..."
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
              {sending ? <Loader2 className="h-4 w-4 animate-spin" /> : <Send className="h-4 w-4" />}
            </button>
          </div>
        </div>
      )}
    </div>
  );
}

export default SubagentStatusBar;
