import React, { useCallback, useEffect, useRef, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { motion, AnimatePresence } from 'framer-motion';
import { useTranslation } from 'react-i18next';
import { FlaskConical, Loader2, Check, X, ChevronRight, ExternalLink, Activity, AlertCircle, Clock, Wrench } from 'lucide-react';
import { getWorkflowStatus, reconnectToWorkflowStream } from '@/pages/ChatAgent/utils/api';

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

type ProgressPhase = 'idle' | 'waiting' | 'running' | 'paused' | 'completed' | 'failed' | 'disconnected';
type ToolStepStatus = 'running' | 'completed' | 'failed';
type TranslateFn = (key: string, options?: Record<string, unknown>) => string;

interface ToolStep {
  id: string;
  label: string;
  status: ToolStepStatus;
}

interface ProgressState {
  phase: ProgressPhase;
  statusText: string;
  completedSteps: number;
  totalSteps: number;
  activeLabel: string | null;
  latestText: string | null;
  error: string | null;
  runId: string | null;
  tools: ToolStep[];
}

interface WorkflowStatusSnapshot {
  status?: string;
  can_reconnect?: boolean;
  run_id?: string | null;
  active_tasks?: unknown[];
}

const BASE_PROGRESS: ProgressState = {
  phase: 'idle',
  statusText: '',
  completedSteps: 0,
  totalSteps: 0,
  activeLabel: null,
  latestText: null,
  error: null,
  runId: null,
  tools: [],
};

function createInitialProgress(t: TranslateFn): ProgressState {
  return {
    ...BASE_PROGRESS,
    statusText: t('chat.ptcAgent.progress.waitingStart'),
  };
}

const FAILURE_STATUS = new Set(['cancelled', 'failed']);

const TOOL_LABEL_KEYS: Record<string, string> = {
  execute_code: 'executeCode',
  ExecuteCode: 'executeCode',
  bash: 'bash',
  Bash: 'bash',
  Read: 'read',
  Write: 'write',
  Edit: 'edit',
  Glob: 'glob',
  Grep: 'grep',
  web_search: 'webSearch',
  WebSearch: 'webSearch',
  web_fetch: 'webFetch',
  WebFetch: 'webFetch',
  get_stock_daily_prices: 'stockPrices',
  get_company_overview: 'companyData',
  get_sec_filing: 'secFiling',
  screen_stocks: 'stockScreener',
  TodoWrite: 'todoWrite',
};

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function humanizeToolName(name: string | undefined, t: TranslateFn): string {
  if (!name) return t('chat.ptcAgent.toolLabels.fallback');
  const labelKey = TOOL_LABEL_KEYS[name];
  if (labelKey) return t(`chat.ptcAgent.toolLabels.${labelKey}`);
  return name
    .replace(/[_-]+/g, ' ')
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

function compactText(value: unknown, maxLength = 180): string | null {
  if (value == null) return null;
  let text = '';
  if (typeof value === 'string') {
    text = value;
  } else {
    try {
      text = JSON.stringify(value);
    } catch {
      return null;
    }
  }
  const cleaned = text
    .replace(/<[^>]+>/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  if (!cleaned) return null;
  return cleaned.length > maxLength ? `${cleaned.slice(0, maxLength - 1)}...` : cleaned;
}

function isToolFailure(content: unknown): boolean {
  if (typeof content !== 'string') return false;
  try {
    const parsed = JSON.parse(content) as Record<string, unknown>;
    return parsed?.success === false || Boolean(parsed?.error);
  } catch {
    return /\b(error|failed|traceback|exception)\b/i.test(content);
  }
}

function phaseFromStatus(status: string | undefined, canReconnect = false): ProgressPhase | null {
  if (!status) return null;
  if (status === 'active') return 'running';
  if (status === 'completed') return 'completed';
  if (status === 'interrupted' || status === 'soft_interrupted') return canReconnect ? 'running' : 'paused';
  if (FAILURE_STATUS.has(status)) return 'failed';
  return null;
}

function updateFromStatus(prev: ProgressState, snapshot: WorkflowStatusSnapshot, t: TranslateFn): ProgressState {
  const statusPhase = phaseFromStatus(snapshot.status, Boolean(snapshot.can_reconnect));
  const activeTasks = Array.isArray(snapshot.active_tasks) ? snapshot.active_tasks.length : 0;
  const nextPhase = statusPhase ?? (snapshot.can_reconnect ? 'running' : prev.phase);
  const activeLabel = activeTasks > 0
    ? t('chat.ptcAgent.progress.backgroundTasksRunning', { count: activeTasks })
    : prev.activeLabel;

  if (nextPhase === 'completed') {
    return {
      ...prev,
      phase: 'completed',
      statusText: t('chat.ptcAgent.progress.analysisComplete'),
      completedSteps: Math.max(prev.completedSteps, prev.totalSteps),
      activeLabel: null,
      error: null,
      runId: snapshot.run_id || prev.runId,
    };
  }

  if (nextPhase === 'failed') {
    const failureText = snapshot.status === 'cancelled'
      ? t('chat.ptcAgent.progress.analysisCancelled')
      : t('chat.ptcAgent.progress.analysisStopped');
    return {
      ...prev,
      phase: 'failed',
      statusText: failureText,
      activeLabel: null,
      error: failureText,
      runId: snapshot.run_id || prev.runId,
    };
  }

  if (nextPhase === 'running') {
    return {
      ...prev,
      phase: 'running',
      statusText: t('chat.ptcAgent.progress.analysisRunning'),
      activeLabel,
      error: null,
      runId: snapshot.run_id || prev.runId,
    };
  }

  if (nextPhase === 'paused') {
    return {
      ...prev,
      phase: 'paused',
      statusText: t('chat.ptcAgent.progress.analysisPaused'),
      activeLabel: snapshot.status === 'soft_interrupted'
        ? t('chat.ptcAgent.progress.softInterrupted')
        : t('chat.ptcAgent.progress.waitingForInput'),
      error: null,
      runId: snapshot.run_id || prev.runId,
    };
  }

  return {
    ...prev,
    runId: snapshot.run_id || prev.runId,
  };
}

function usePtcProgress(threadId: string | undefined, enabled: boolean, t: TranslateFn): ProgressState {
  const [progress, setProgress] = useState<ProgressState>(() => createInitialProgress(t));
  const lastEventIdRef = useRef<number | null>(null);
  const finalTextRef = useRef('');

  const handleEvent = useCallback((event: Record<string, unknown>) => {
    const eventType = typeof event.event === 'string' ? event.event : 'message_chunk';
    const eventId = event._eventId;
    if (typeof eventId === 'number') {
      lastEventIdRef.current = eventId;
    }

    if (eventType === 'metadata') {
      setProgress((prev) => ({
        ...prev,
        phase: 'running',
        statusText: t('chat.ptcAgent.progress.analysisRunning'),
        runId: typeof event.run_id === 'string' ? event.run_id : prev.runId,
        error: null,
      }));
      return;
    }

    if (eventType === 'workflow_status') {
      setProgress((prev) => updateFromStatus(prev, event as WorkflowStatusSnapshot, t));
      return;
    }

    if (eventType === 'reasoning_signal') {
      const isComplete = event.content === 'complete';
      setProgress((prev) => ({
        ...prev,
        phase: prev.phase === 'idle' || prev.phase === 'waiting' ? 'running' : prev.phase,
        statusText: t('chat.ptcAgent.progress.analysisRunning'),
        activeLabel: isComplete
          ? t('chat.ptcAgent.progress.reasoningComplete')
          : t('chat.ptcAgent.progress.reasoning'),
      }));
      return;
    }

    if (eventType === 'reasoning_content') {
      const latest = compactText(event.content);
      if (!latest) return;
      setProgress((prev) => ({
        ...prev,
        phase: prev.phase === 'idle' || prev.phase === 'waiting' ? 'running' : prev.phase,
        latestText: latest,
        activeLabel: t('chat.ptcAgent.progress.reasoning'),
      }));
      return;
    }

    if (eventType === 'tool_calls') {
      const calls = Array.isArray(event.tool_calls) ? event.tool_calls as Array<Record<string, unknown>> : [];
      if (calls.length === 0) return;
      setProgress((prev) => {
        const existing = new Set(prev.tools.map((tool) => tool.id));
        const additions: ToolStep[] = calls
          .map((call, idx) => ({
            id: String(call.id || `${Date.now()}-${idx}`),
            label: humanizeToolName(typeof call.name === 'string' ? call.name : undefined, t),
            status: 'running' as const,
          }))
          .filter((tool) => !existing.has(tool.id));
        if (additions.length === 0) return prev;
        const tools = [...prev.tools, ...additions].slice(-5);
        return {
          ...prev,
          phase: 'running',
          statusText: t('chat.ptcAgent.progress.analysisRunning'),
          totalSteps: prev.totalSteps + additions.length,
          activeLabel: additions[additions.length - 1]?.label || prev.activeLabel,
          tools,
          error: null,
        };
      });
      return;
    }

    if (eventType === 'tool_call_result') {
      const toolCallId = typeof event.tool_call_id === 'string' ? event.tool_call_id : null;
      const failed = isToolFailure(event.content);
      const latest = compactText(event.content, 140);
      setProgress((prev) => {
        let countedCompletion = false;
        let found = false;
        const tools = prev.tools.map((tool) => {
          if (tool.id !== toolCallId) return tool;
          found = true;
          if (tool.status === 'running') countedCompletion = true;
          return { ...tool, status: failed ? 'failed' as const : 'completed' as const };
        });
        const nextTools = found
          ? tools
          : [
              ...tools,
              {
                id: toolCallId || `result-${Date.now()}`,
                label: t('chat.ptcAgent.progress.completedToolStep'),
                status: failed ? 'failed' as const : 'completed' as const,
              },
            ].slice(-5);
        return {
          ...prev,
          phase: prev.phase === 'completed' || prev.phase === 'failed' ? prev.phase : 'running',
          statusText: failed
            ? t('chat.ptcAgent.progress.toolStepError')
            : t('chat.ptcAgent.progress.analysisRunning'),
          completedSteps: prev.completedSteps + (countedCompletion || !found ? 1 : 0),
          totalSteps: found ? prev.totalSteps : prev.totalSteps + 1,
          activeLabel: failed
            ? t('chat.ptcAgent.progress.recoveringToolError')
            : t('chat.ptcAgent.progress.toolStepCompleted'),
          latestText: latest || prev.latestText,
          error: failed ? latest || t('chat.ptcAgent.progress.toolStepFailed') : null,
          tools: nextTools,
        };
      });
      return;
    }

    if (eventType === 'message_chunk') {
      if (typeof event.content === 'string' && event.content) {
        finalTextRef.current = `${finalTextRef.current}${event.content}`.slice(-600);
        const latest = compactText(finalTextRef.current, 180);
        setProgress((prev) => ({
          ...prev,
          phase: prev.phase === 'idle' || prev.phase === 'waiting' ? 'running' : prev.phase,
          activeLabel: t('chat.ptcAgent.progress.writingFinal'),
          latestText: latest || prev.latestText,
        }));
      }
      if (event.finish_reason === 'stop') {
        setProgress((prev) => ({
          ...prev,
          statusText: t('chat.ptcAgent.progress.finalReady'),
          activeLabel: t('chat.ptcAgent.progress.finalReady'),
        }));
      }
      return;
    }

    if (eventType === 'artifact') {
      setProgress((prev) => ({
        ...prev,
        phase: prev.phase === 'idle' || prev.phase === 'waiting' ? 'running' : prev.phase,
        activeLabel: t('chat.ptcAgent.progress.generatedArtifact'),
      }));
      return;
    }

    if (eventType === 'finish') {
      setProgress((prev) => ({
        ...prev,
        phase: 'completed',
        statusText: t('chat.ptcAgent.progress.analysisComplete'),
        completedSteps: Math.max(prev.completedSteps, prev.totalSteps),
        activeLabel: null,
        error: null,
      }));
      return;
    }

    if (eventType === 'error') {
      const errorText = compactText(event.message || event.content || event.error) || t('chat.ptcAgent.progress.analysisFailed');
      setProgress((prev) => ({
        ...prev,
        phase: 'failed',
        statusText: t('chat.ptcAgent.progress.analysisFailed'),
        activeLabel: null,
        error: errorText,
      }));
    }
  }, [t]);

  useEffect(() => {
    if (!enabled || !threadId) {
      setProgress(createInitialProgress(t));
      lastEventIdRef.current = null;
      finalTextRef.current = '';
      return;
    }

    let disposed = false;
    const abort = new AbortController();

    const run = async () => {
      lastEventIdRef.current = null;
      finalTextRef.current = '';
      setProgress({
        ...createInitialProgress(t),
        phase: 'waiting',
        statusText: t('chat.ptcAgent.progress.startingStream'),
      });

      try {
        let snapshot: WorkflowStatusSnapshot | null = null;
        for (let attempt = 0; attempt < 12; attempt += 1) {
          if (disposed || abort.signal.aborted) return;
          snapshot = await getWorkflowStatus(threadId) as WorkflowStatusSnapshot;
          if (disposed || abort.signal.aborted) return;
          setProgress((prev) => updateFromStatus(prev, snapshot!, t));

          const phase = phaseFromStatus(snapshot.status, Boolean(snapshot.can_reconnect));
          if (snapshot.can_reconnect || phase === 'running') break;
          if (phase === 'completed' || phase === 'failed' || phase === 'paused') return;
          await sleep(attempt < 3 ? 800 : 1500);
        }

        if (disposed || abort.signal.aborted) return;
        let runId = snapshot?.run_id || null;
        for (let attempt = 0; attempt < 4; attempt += 1) {
          try {
            const result = await reconnectToWorkflowStream(
              threadId,
              runId,
              lastEventIdRef.current,
              handleEvent,
              abort.signal,
            );
            if (!result?.disconnected) break;
          } catch (streamError) {
            if (disposed || abort.signal.aborted) return;

            const latestSnapshot = await getWorkflowStatus(threadId) as WorkflowStatusSnapshot;
            if (disposed || abort.signal.aborted) return;
            setProgress((prev) => updateFromStatus(prev, latestSnapshot, t));

            const latestPhase = phaseFromStatus(latestSnapshot.status, Boolean(latestSnapshot.can_reconnect));
            if (latestPhase === 'completed' || latestPhase === 'failed' || latestPhase === 'paused') return;
            if (attempt === 3) throw streamError;

            runId = latestSnapshot.run_id || runId;
            setProgress((prev) => ({
              ...prev,
              phase: 'waiting',
              statusText: t('chat.ptcAgent.progress.waitingStream'),
              activeLabel: t('chat.ptcAgent.progress.connectingProgress'),
            }));
            await sleep(900 + attempt * 600);
            continue;
          }

          if (disposed || abort.signal.aborted) return;

          const latestSnapshot = await getWorkflowStatus(threadId) as WorkflowStatusSnapshot;
          if (disposed || abort.signal.aborted) return;
          setProgress((prev) => updateFromStatus(prev, latestSnapshot, t));

          const latestPhase = phaseFromStatus(latestSnapshot.status, Boolean(latestSnapshot.can_reconnect));
          if (latestPhase === 'completed' || latestPhase === 'failed' || latestPhase === 'paused') return;
          if (attempt === 3) throw new Error(t('chat.ptcAgent.progress.streamDisconnected'));

          runId = latestSnapshot.run_id || runId;
          setProgress((prev) => ({
            ...prev,
            phase: 'waiting',
            statusText: t('chat.ptcAgent.progress.waitingStream'),
            activeLabel: t('chat.ptcAgent.progress.connectingProgress'),
          }));
          await sleep(900 + attempt * 600);
        }
      } catch (err) {
        if (disposed || abort.signal.aborted) return;
        const error = err instanceof Error ? err.message : t('chat.ptcAgent.progress.unableToStream');
        setProgress((prev) => ({
          ...prev,
          phase: 'disconnected',
          statusText: t('chat.ptcAgent.progress.livePaused'),
          activeLabel: null,
          error,
        }));
      } finally {
        if (!disposed && !abort.signal.aborted) {
          try {
            const snapshot = await getWorkflowStatus(threadId) as WorkflowStatusSnapshot;
            if (!disposed && !abort.signal.aborted) {
              setProgress((prev) => {
                const next = updateFromStatus(prev, snapshot, t);
                if (next.phase === 'running') {
                  return {
                    ...next,
                    phase: 'disconnected',
                    statusText: t('chat.ptcAgent.progress.livePaused'),
                    activeLabel: t('chat.ptcAgent.progress.openThreadFullStream'),
                  };
                }
                return next;
              });
            }
          } catch {
            // Keep the latest streamed state when the final status check fails.
          }
        }
      }
    };

    void run();

    return () => {
      disposed = true;
      abort.abort();
    };
  }, [enabled, handleEvent, threadId, t]);

  return progress;
}

/**
 * PTCAgentCard - Inline HITL card for dispatching a PTC research agent.
 *
 * Three states:
 *   pending  - workspace name + question preview, Approve/Reject buttons
 *   approved - clickable artifact linking to the dispatched thread
 *   rejected - collapsed "Research declined"
 */
function PTCAgentCard({ proposalData, onApprove, onReject, flashContext }: PTCAgentCardProps) {
  const { t } = useTranslation();
  const [collapsed, setCollapsed] = useState(true);
  const [reportBack, setReportBack] = useState(proposalData?.report_back ?? true);
  const navigate = useNavigate();
  const progress = usePtcProgress(
    proposalData?.thread_id,
    proposalData?.status === 'approved' && Boolean(proposalData?.thread_id),
    t as TranslateFn,
  );

  if (!proposalData) return null;

  const { workspace_name, question, status, thread_id, workspace_id } = proposalData;
  const isApproved = status === 'approved';
  const isRejected = status === 'rejected';
  const progressPercent = progress.phase === 'completed'
    ? 100
    : progress.totalSteps <= 0
      ? (progress.phase === 'running' ? 18 : 8)
      : Math.max(8, Math.min(95, Math.round((progress.completedSteps / progress.totalSteps) * 100)));
  const statusTone = progress.phase === 'failed'
    ? 'var(--color-icon-danger)'
    : progress.phase === 'completed'
      ? 'var(--color-accent-light)'
      : 'var(--color-text-tertiary)';
  const StatusIcon = progress.phase === 'failed'
    ? AlertCircle
    : progress.phase === 'completed'
      ? Check
      : progress.phase === 'waiting' || progress.phase === 'paused'
        ? Clock
        : Activity;

  // --- Approved: clickable artifact to navigate to thread ---
  if (isApproved && thread_id && workspace_id) {
    return (
      <motion.div
        className="w-full rounded-lg px-4 py-3"
        style={{
          border: '1px solid var(--color-border-muted)',
          backgroundColor: 'var(--color-bg-secondary)',
        }}
        whileHover={{ scale: 1.005 }}
        whileTap={{ scale: 0.995 }}
      >
        <div className="flex items-start gap-3">
          <FlaskConical
            className="h-4 w-4 flex-shrink-0 mt-0.5"
            style={{ color: 'var(--color-accent-light)' }}
          />
          <div className="flex-1 min-w-0">
            <div className="flex items-start gap-2">
              <div className="flex-1 min-w-0">
                {workspace_name && (
                  <div className="text-sm font-medium truncate" style={{ color: 'var(--color-text-primary)' }}>
                    {workspace_name}
                  </div>
                )}
                <div className="text-sm truncate" style={{ color: 'var(--color-text-tertiary)' }}>
                  {question}
                </div>
              </div>
              <button
                type="button"
                onClick={() => navigate(`/chat/t/${thread_id}`, { state: {
                  workspaceId: workspace_id,
                  ...(flashContext ? { fromThreadId: flashContext.threadId, fromWorkspaceId: flashContext.workspaceId } : {}),
                } })}
                className="inline-flex items-center gap-1.5 rounded-md px-2 py-1 text-xs font-medium transition-colors hover:brightness-110"
                style={{
                  border: '1px solid var(--color-border-muted)',
                  color: 'var(--color-text-tertiary)',
                }}
              >
                {t('chat.ptcAgent.card.open')}
                <ExternalLink className="h-3 w-3" />
              </button>
            </div>

            <div className="mt-3">
              <div className="flex items-center gap-2">
                <StatusIcon
                  className={`h-3.5 w-3.5 flex-shrink-0 ${progress.phase === 'running' ? 'animate-pulse' : ''}`}
                  style={{ color: statusTone }}
                />
                <span className="text-xs font-medium" style={{ color: statusTone }}>
                  {progress.statusText}
                </span>
                {progress.totalSteps > 0 && (
                  <span className="text-xs ml-auto" style={{ color: 'var(--color-text-tertiary)' }}>
                    {progress.completedSteps}/{progress.totalSteps} {t('chat.ptcAgent.progress.steps')}
                  </span>
                )}
              </div>
              <div
                className="mt-2 h-1.5 w-full overflow-hidden rounded-full"
                style={{ backgroundColor: 'var(--color-border-muted)' }}
              >
                <motion.div
                  className="h-full rounded-full"
                  style={{ backgroundColor: progress.phase === 'failed' ? 'var(--color-icon-danger)' : 'var(--color-accent-light)' }}
                  animate={{ width: `${progressPercent}%` }}
                  transition={{ duration: 0.35, ease: 'easeOut' }}
                />
              </div>

              {(progress.activeLabel || progress.latestText || progress.error) && (
                <div className="mt-2 space-y-1">
                  {progress.activeLabel && (
                    <div className="flex items-center gap-1.5 text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
                      <Wrench className="h-3 w-3 flex-shrink-0" />
                      <span className="truncate">{progress.activeLabel}</span>
                    </div>
                  )}
                  {progress.latestText && (
                    <div className="text-xs leading-relaxed line-clamp-2" style={{ color: 'var(--color-text-tertiary)' }}>
                      {progress.latestText}
                    </div>
                  )}
                  {progress.error && progress.phase !== 'completed' && (
                    <div className="text-xs leading-relaxed" style={{ color: 'var(--color-icon-danger)' }}>
                      {progress.error}
                    </div>
                  )}
                </div>
              )}

              {progress.tools.length > 0 && (
                <div className="mt-2 flex flex-wrap gap-1.5">
                  {progress.tools.map((tool) => (
                    <span
                      key={tool.id}
                      className="inline-flex max-w-full items-center gap-1 rounded px-1.5 py-0.5 text-[11px]"
                      style={{
                        border: '1px solid var(--color-border-muted)',
                        color: tool.status === 'failed' ? 'var(--color-icon-danger)' : 'var(--color-text-tertiary)',
                      }}
                    >
                      {tool.status === 'running' ? (
                        <Loader2 className="h-2.5 w-2.5 animate-spin flex-shrink-0" />
                      ) : tool.status === 'completed' ? (
                        <Check className="h-2.5 w-2.5 flex-shrink-0" />
                      ) : (
                        <X className="h-2.5 w-2.5 flex-shrink-0" />
                      )}
                      <span className="truncate">{tool.label}</span>
                    </span>
                  ))}
                </div>
              )}
            </div>
          </div>
        </div>
      </motion.div>
    );
  }

  // --- Resolved without thread_id (approved fallback or rejected) ---
  if (isApproved || isRejected) {
    return (
      <div>
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
              style={{ color: 'var(--color-icon-muted)' }}
            />
          </motion.div>
          {isApproved ? (
            <Check className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-accent-light)' }} />
          ) : (
            <X className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
          )}
          <span
            className="text-sm"
            style={{ color: 'var(--color-text-tertiary)' }}
          >
            {isApproved ? t('chat.ptcAgent.card.researchDispatched') : t('chat.ptcAgent.card.researchDeclined')}
            {workspace_name && isApproved ? `: ${workspace_name}` : ''}
          </span>
        </button>

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
                  className="rounded-lg px-4 py-3"
                  style={{
                    border: '1px solid var(--color-border-muted)',
                    opacity: isRejected ? 0.6 : 0.8,
                  }}
                >
                  {workspace_name && (
                    <div className="text-sm font-medium mb-1" style={{ color: 'var(--color-text-primary)' }}>
                      {workspace_name}
                    </div>
                  )}
                  <div className="text-sm" style={{ color: 'var(--color-text-tertiary)' }}>
                    {question}
                  </div>
                </div>
              </div>
            </motion.div>
          )}
        </AnimatePresence>
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
        <FlaskConical className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-accent-light)' }} />
        <span className="text-[15px] font-medium" style={{ color: 'var(--color-text-primary)' }}>
          {t('chat.ptcAgent.card.startResearch')}
        </span>
        <Loader2
          className="h-3.5 w-3.5 animate-spin ml-auto flex-shrink-0"
          style={{ color: 'var(--color-icon-muted)' }}
        />
      </div>

      {/* Preview */}
      <div
        className="rounded-lg px-4 py-3"
        style={{ border: '1px solid var(--color-border-muted)' }}
      >
        {workspace_name && (
          <div className="text-sm font-medium mb-1" style={{ color: 'var(--color-text-primary)' }}>
            {workspace_name}
          </div>
        )}
        <div className="text-sm" style={{ color: 'var(--color-text-tertiary)' }}>
          {question}
        </div>
        {/* Report-back toggle */}
        <div
          className="mt-2.5 -mx-4 px-4 pt-2.5"
          style={{ borderTop: '1px solid var(--color-border-muted)' }}
        >
          <button
            type="button"
            className="flex items-center justify-between w-full cursor-pointer"
            onClick={(e: React.MouseEvent) => { e.stopPropagation(); setReportBack((v) => !v); }}
          >
            <span className="text-[13px]" style={{ color: 'var(--color-text-tertiary)' }}>
              {t('chat.ptcAgent.card.reportBack')}
            </span>
            <div
              className="relative w-8 h-[18px] rounded-full transition-colors"
              style={{ background: reportBack ? 'var(--color-accent-light)' : 'rgba(255,255,255,0.12)' }}
            >
              <div
                className="absolute top-[3px] left-[3px] w-3 h-3 rounded-full bg-white transition-transform"
                style={{ transform: reportBack ? 'translateX(14px)' : 'translateX(0)' }}
              />
            </div>
          </button>
        </div>
      </div>

      {/* Actions */}
      <div className="pt-3 flex items-center gap-2">
        <motion.button
          onClick={(e: React.MouseEvent) => { e.stopPropagation(); onApprove?.({ report_back: reportBack }); }}
          className="flex items-center gap-1.5 text-sm px-4 py-2 rounded-md font-medium transition-colors hover:brightness-110"
          style={{ backgroundColor: 'var(--color-btn-primary-bg)', color: 'var(--color-btn-primary-text)' }}
          whileHover={{ scale: 1.02 }}
          whileTap={{ scale: 0.98 }}
        >
          <Check className="h-3.5 w-3.5 stroke-[2.5]" />
          {t('chat.ptcAgent.card.approve')}
        </motion.button>
        <motion.button
          onClick={(e: React.MouseEvent) => { e.stopPropagation(); onReject?.(); }}
          className="flex items-center gap-1.5 text-sm px-4 py-2 rounded-md font-medium transition-colors"
          style={{
            backgroundColor: 'var(--color-border-muted)',
            color: 'var(--color-text-tertiary)',
          }}
          whileHover={{ scale: 1.02 }}
          whileTap={{ scale: 0.98 }}
        >
          <X className="h-3.5 w-3.5" />
          {t('chat.ptcAgent.card.decline')}
        </motion.button>
      </div>
    </motion.div>
  );
}

export default PTCAgentCard;
