import React, { useCallback, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { StopCircle } from 'lucide-react';
import { Loader } from '@/components/ui/loader';
import { compactNumber } from '@/lib/format';
import {
  WORKFLOW_TASK_TYPE,
  deriveChildIdentity,
  workflowRunCardKind,
  type WorkflowChild,
  type WorkflowRunState,
} from '../session/subagents/workflowRunState';
import type { SubagentTelemetry } from '../session/subagents/resolveSubagentTelemetry';
import { useWorkflowRun } from './WorkflowRunContext';
import { MONO_STACK } from './TaskCardShell';
import { TaskStatusChip, type TaskCardStatusKind } from './taskStatusUi';
import {
  SECTION_LABEL_STYLE,
  WorkflowChildRow,
  summarizeRun,
  workflowChildStatusColor,
} from './workflowRunUi';
import StructuredResultBlock from './messageList/StructuredResultBlock';
// Dependency-free module, not `../utils/api`: the predicate must survive the
// tests that mock the api barrel wholesale.
import { isNoOpCancellation } from '../utils/cancelOutcome';
import { parseResultPreview } from '../utils/structuredResult';
import type { AgentInfo, SubagentInfo } from './chatView/types';

interface WorkflowRunDetailProps {
  agent: AgentInfo;
  onOpenChild?: (info: SubagentInfo) => void;
  /** Live per-child telemetry (tool calls; token fallback) keyed by `task:<id>`. */
  resolveChildTelemetry?: (subagentId: string) => SubagentTelemetry | undefined;
  /** Stop the run (task-targeted cancel); button hidden when absent. */
  onStop?: () => Promise<unknown>;
}

/** Children in dispatch order, cut into runs of the same phase; unphased
 *  children group under `null` and render without a header. */
function groupChildrenByPhase(
  children: WorkflowChild[],
): Array<{ phase: string | null; items: WorkflowChild[] }> {
  const groups: Array<{ phase: string | null; items: WorkflowChild[] }> = [];
  for (const child of children) {
    const last = groups[groups.length - 1];
    if (last && last.phase === (child.phase ?? null)) {
      last.items.push(child);
    } else {
      groups.push({ phase: child.phase ?? null, items: [child] });
    }
  }
  return groups;
}

function Stat({ label, value }: { label: string; value: string }): React.ReactElement {
  return (
    <div style={{ minWidth: 0 }}>
      <div style={{ ...SECTION_LABEL_STYLE, marginBottom: 2 }}>{label}</div>
      <div style={{ fontSize: '0.8125rem', fontWeight: 600, color: 'var(--color-text-primary)' }}>
        {value}
      </div>
    </div>
  );
}

function Section({
  label,
  children,
}: {
  label: string;
  children: React.ReactNode;
}): React.ReactElement {
  return (
    <div style={{ marginTop: 6 }}>
      <div style={{ ...SECTION_LABEL_STYLE, margin: '8px 0 4px' }}>{label}</div>
      {children}
    </div>
  );
}

function RunHeader({
  run,
  agent,
  kind,
  stopping,
  onStop,
}: {
  run: WorkflowRunState | undefined;
  agent: AgentInfo;
  kind: TaskCardStatusKind;
  stopping: boolean;
  /** Absent when the run is settled or the view can't cancel. */
  onStop?: () => void;
}): React.ReactElement {
  const { t } = useTranslation();
  return (
    <div style={{ display: 'flex', alignItems: 'flex-start', gap: 12 }}>
      <div style={{ minWidth: 0, flex: 1 }}>
        <div
          style={{
            fontSize: '0.6875rem',
            color: 'var(--color-text-tertiary)',
            letterSpacing: '0.04em',
            textTransform: 'lowercase',
          }}
        >
          {WORKFLOW_TASK_TYPE}{run?.name ? ` · ${run.name}` : ''}{run?.source ? ` · ${run.source}` : ''}
        </div>
        <div
          style={{
            fontFamily: 'var(--font-ui)',
            fontSize: '1rem',
            fontWeight: 600,
            color: 'var(--color-text-primary)',
            marginTop: 2,
          }}
        >
          {run?.description || agent.description || t('chat.workflowRun.titleFallback')}
        </div>
      </div>
      <TaskStatusChip kind={kind} style={{ marginTop: 3 }} />
      {onStop && (
        <button
          type="button"
          onClick={onStop}
          disabled={stopping}
          data-testid="workflow-detail-stop"
          style={{
            display: 'inline-flex',
            alignItems: 'center',
            gap: 5,
            padding: '3px 9px',
            marginTop: 0,
            fontSize: '0.6875rem',
            fontFamily: MONO_STACK,
            letterSpacing: '0.04em',
            color: 'var(--color-text-secondary)',
            background: 'transparent',
            border: '1px solid var(--color-border-muted)',
            borderRadius: 6,
            cursor: stopping ? 'default' : 'pointer',
            opacity: stopping ? 0.6 : 1,
            whiteSpace: 'nowrap',
          }}
        >
          <StopCircle style={{ width: 11, height: 11 }} />
          {t(stopping ? 'chat.workflowRun.stopping' : 'chat.workflowRun.stop')}
        </button>
      )}
    </div>
  );
}

function PhaseGroup({
  phase,
  items,
  active,
  resolveChildTelemetry,
  onOpenChild,
}: {
  phase: string | null;
  items: WorkflowChild[];
  /** The run is here right now — the header spins and takes the warning token. */
  active: boolean;
  resolveChildTelemetry?: (subagentId: string) => SubagentTelemetry | undefined;
  onOpenChild?: (child: WorkflowChild) => void;
}): React.ReactElement {
  const { t } = useTranslation();
  const doneCount = items.filter((c) => c.status !== 'running').length;
  return (
    <div style={{ marginBottom: 10 }}>
      {phase && (
        <div
          style={{
            ...SECTION_LABEL_STYLE,
            display: 'flex',
            alignItems: 'center',
            gap: 6,
            color: active ? 'var(--color-warning)' : 'var(--color-text-quaternary)',
            margin: '8px 0 4px',
          }}
        >
          {active && (
            // Shared liveness glyph — inherits the phase label's warning amber.
            <Loader size={10} label={t('chat.taskCard.statusRunning')} style={{ color: 'inherit' }} />
          )}
          {phase}
          <span style={{ color: 'var(--color-text-quaternary)', letterSpacing: 0 }}>
            {doneCount}/{items.length}
          </span>
        </div>
      )}
      <div style={{ display: 'flex', flexDirection: 'column' }}>
        {items.map((child) => {
          const telemetry =
            child.childTaskId && resolveChildTelemetry
              ? resolveChildTelemetry(`task:${child.childTaskId}`)
              : undefined;
          const tokens = child.tokensUsed ?? telemetry?.tokenUsage.total ?? 0;
          const toolCalls = telemetry?.toolCalls ?? 0;
          const meta = [
            toolCalls > 0 ? t('chat.workflowRun.toolCalls', { count: toolCalls }) : null,
            tokens > 0 ? t('chat.workflowRun.tokensShort', { value: compactNumber(tokens) }) : null,
          ].filter(Boolean).join(' · ');
          const showError =
            child.status !== 'running' && child.status !== 'ok' && !!child.error;
          return (
            <React.Fragment key={child.seq}>
              <WorkflowChildRow
                child={child}
                surface="detail"
                meta={meta}
                onOpen={
                  onOpenChild && child.childTaskId ? () => onOpenChild(child) : undefined
                }
              />
              {showError && (
                <div
                  data-testid="workflow-detail-child-error"
                  style={{
                    margin: '0 0 4px 22px',
                    fontSize: '0.6875rem',
                    color: workflowChildStatusColor(child.status),
                    whiteSpace: 'pre-wrap',
                    wordBreak: 'break-word',
                    display: '-webkit-box',
                    WebkitLineClamp: 3,
                    WebkitBoxOrient: 'vertical',
                    overflow: 'hidden',
                  }}
                >
                  {child.error}
                </div>
              )}
            </React.Fragment>
          );
        })}
      </div>
    </div>
  );
}

/**
 * Right-panel detail for a workflow run task: header + status, a quiet stat
 * band, the children grouped by phase (each row opens that child's own task
 * view), and the run's log lines. A workflow run has no transcript of its own
 * — this view replaces the generic subagent MessageList entirely.
 */
function WorkflowRunDetail({
  agent,
  onOpenChild,
  resolveChildTelemetry,
  onStop,
}: WorkflowRunDetailProps): React.ReactElement {
  const { t } = useTranslation();
  const ctxRun = useWorkflowRun(agent.id);
  const run = agent.workflowRun ?? ctxRun;

  const kind = workflowRunCardKind(run?.status ?? agent.status);
  const isRunning = kind === 'running';

  const [stopping, setStopping] = useState(false);
  const handleStop = useCallback(() => {
    if (!onStop || stopping) return;
    setStopping(true);
    onStop()
      .then((outcome: unknown) => {
        // Only a cancellation that was actually sent produces the terminal
        // frame this waits for. A no-op resolves the same way, so without
        // this the button reads "Stopping…" for the life of the view.
        if (isNoOpCancellation(outcome)) setStopping(false);
        // Deliberately not reconciling the card here: the stream layer is the
        // only writer of workflowRun.status, and a stale card that no longer
        // has a run settles from its own lane closure.
      })
      .catch((err: unknown) => {
        console.error('[WorkflowRunDetail] stop failed:', err);
        setStopping(false);
      });
  }, [onStop, stopping]);

  const openChild = useCallback(
    (child: WorkflowChild) => {
      if (!onOpenChild || !child.childTaskId) return;
      const identity = deriveChildIdentity(child);
      onOpenChild({
        subagentId: `task:${child.childTaskId}`,
        ...identity,
        // An unsettled child has no terminal status to carry, and the card's
        // no-status fallback is 'completed' — so a running child would open
        // reading "Completed" with an empty transcript. This view knows it is
        // still running; say so.
        status: identity.status ?? 'active',
        // Ownership drives hierarchical back-navigation (child → this view).
        ownerTaskId: agent.id,
      });
    },
    [onOpenChild, agent.id],
  );

  // A script's return value is structured, so render it as fields. The preview
  // is byte-clipped server-side, so a large result may only be partly
  // recoverable — and one that yields nothing at all keeps the raw block below.
  const resultStructured = useMemo(
    () => parseResultPreview(run?.resultPreview),
    [run?.resultPreview]
  );
  // Fallback for an unsalvageable preview: pretty-print when it parses, else raw.
  const resultPretty = useMemo(() => {
    const raw = run?.resultPreview;
    if (!raw) return null;
    try {
      return JSON.stringify(JSON.parse(raw), null, 2);
    } catch {
      return raw;
    }
  }, [run?.resultPreview]);

  const { children, doneCount, agentCount, duration } = summarizeRun(run);
  // The wire carries `error` on cancelled runs too ("Workflow cancelled") — a
  // stop is not a failure, so only a genuinely failed run gets the danger box.
  // Same gate as the inline card's error line.
  const error = kind === 'error' ? (run?.error ?? agent.error) : null;
  const groups = groupChildrenByPhase(children);

  return (
    <div style={{ fontFamily: MONO_STACK }}>
      <RunHeader
        run={run}
        agent={agent}
        kind={kind}
        stopping={stopping}
        onStop={isRunning && onStop ? handleStop : undefined}
      />

      {error && (
        <div
          style={{
            marginTop: 10,
            padding: '8px 10px',
            fontSize: '0.75rem',
            color: 'var(--color-icon-danger)',
            border: '1px solid var(--color-border-muted)',
            borderRadius: 8,
            whiteSpace: 'pre-wrap',
            wordBreak: 'break-word',
          }}
        >
          {error}
        </div>
      )}

      {run && (
        <div
          style={{
            display: 'flex',
            gap: 28,
            marginTop: 14,
            padding: '10px 0',
            borderTop: '1px solid var(--color-border-subtle)',
            borderBottom: '1px solid var(--color-border-subtle)',
          }}
        >
          <Stat
            label={t('chat.workflowRun.statAgents')}
            value={isRunning && agentCount > 0 ? `${doneCount} / ${agentCount}` : String(agentCount)}
          />
          {run.phases.length > 0 && (
            <Stat label={t('chat.workflowRun.statPhases')} value={String(run.phases.length)} />
          )}
          {duration && <Stat label={t('chat.workflowRun.statDuration')} value={duration} />}
          {run.tokensSpent != null && (
            <Stat label={t('chat.workflowRun.statTokens')} value={compactNumber(run.tokensSpent)} />
          )}
        </div>
      )}

      {groups.length > 0 && (
        <div style={{ marginTop: 12 }}>
          {groups.map((group, gi) => (
            <PhaseGroup
              key={`${group.phase ?? 'unphased'}-${gi}`}
              phase={group.phase}
              items={group.items}
              active={isRunning && group.phase != null && group.phase === run?.currentPhase}
              resolveChildTelemetry={resolveChildTelemetry}
              onOpenChild={onOpenChild ? openChild : undefined}
            />
          ))}
        </div>
      )}

      {/* Terminal result — the script's return value, clipped server-side */}
      {(resultStructured || resultPretty) && (
        <Section label={t('chat.workflowRun.sectionResult')}>
          {resultStructured ? (
            <div data-testid="workflow-detail-result">
              <StructuredResultBlock result={resultStructured} collapsedMaxHeight={320} />
            </div>
          ) : (
            <pre
              data-testid="workflow-detail-result"
              style={{
                border: '1px solid var(--color-border-subtle)',
                borderRadius: 8,
                padding: '8px 10px',
                fontSize: '0.6875rem',
                fontFamily: MONO_STACK,
                color: 'var(--color-text-secondary)',
                whiteSpace: 'pre-wrap',
                wordBreak: 'break-word',
                maxHeight: 260,
                overflowY: 'auto',
                margin: 0,
              }}
            >
              {resultPretty}
            </pre>
          )}
        </Section>
      )}

      {(run?.logs.length ?? 0) > 0 && (
        <Section label={t('chat.workflowRun.sectionLog')}>
          <div
            style={{
              border: '1px solid var(--color-border-subtle)',
              borderRadius: 8,
              padding: '8px 10px',
              fontSize: '0.6875rem',
              color: 'var(--color-text-secondary)',
              display: 'flex',
              flexDirection: 'column',
              gap: 3,
              maxHeight: 220,
              overflowY: 'auto',
            }}
          >
            {run!.logs.map((line, i) => (
              <div key={i} style={{ whiteSpace: 'pre-wrap', wordBreak: 'break-word' }}>
                › {line}
              </div>
            ))}
          </div>
        </Section>
      )}

      {!run && (
        <div style={{ marginTop: 14, fontSize: '0.75rem', color: 'var(--color-text-tertiary)' }}>
          {t(isRunning ? 'chat.workflowRun.starting' : 'chat.workflowRun.noProgress')}
        </div>
      )}
    </div>
  );
}

export default WorkflowRunDetail;
