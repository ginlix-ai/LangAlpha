import React from 'react';
import { motion } from 'framer-motion';
import { McpStatusPill } from './McpStatusPill';
import type { McpStatus } from '../../utils/api';

/**
 * The end-to-end lifecycle indicator for one effective MCP server row.
 *
 * It unifies the two independent axes a user actually cares about into one
 * honest signal — "is this added, verified, and will it work on my next turn?":
 *
 *   1. **Verify** — does langalpha know the server's tools? (discovery)
 *        pending → checking → connected / error / needs_secret
 *   2. **Apply**  — has the *running agent* actually loaded it? (sync)
 *        derived from `synced`: the live session's applied config version has
 *        caught up to the saved one (version-accurate, not a 30s guess).
 *
 * Terminal states render as a single pill (`McpStatusPill`); a server that is
 * still progressing renders an animated 3-step track (Saved → Verifying →
 * Ready) so the user sees real movement and a truthful current phase instead
 * of a dead "Pending". A healthy, fully-applied server collapses back to the
 * clean green "Connected" pill — no perpetual stepper noise.
 */

const SPRING = { type: 'spring' as const, stiffness: 200, damping: 22 };

type StepState = 'done' | 'active' | 'todo';

interface McpLifecycleProps {
  status: McpStatus;
  enabled: boolean;
  origin: 'builtin' | 'workspace';
  /** A discovery probe is in flight for this row. */
  checking: boolean;
  /** The running session has applied the saved config (apply axis complete). */
  synced: boolean;
  /** Whether the workspace sandbox is running (discovery/apply can happen). */
  sandboxRunning: boolean;
  /** The sandbox is warming up toward running (a background apply kicked it). */
  sandboxWarming?: boolean;
}

export function McpLifecycle({ status, enabled, origin, checking, synced, sandboxRunning, sandboxWarming = false }: McpLifecycleProps) {
  // Built-ins are process-global: always connected, no per-workspace discovery
  // or apply state to surface. They never show the verify/apply track.
  if (origin === 'builtin') return <McpStatusPill status={status} enabled={enabled} />;
  // Terminal / steady states → a single pill, same as before. A disabled row is
  // always enabled=false (the optimistic toggle writes enabled+status coherently
  // at the source), so this guard alone covers it.
  if (!enabled) return <McpStatusPill status={status} enabled={false} />;
  if (status === 'error') return <McpStatusPill status="error" enabled />;
  if (status === 'needs_secret') return <McpStatusPill status="needs_secret" enabled />;
  if (status === 'unknown') return <McpStatusPill status="unknown" enabled />;
  // Fully done: verified AND loaded into the running agent.
  if (status === 'connected' && synced) return <McpStatusPill status="connected" enabled />;

  // Otherwise the server is still moving through the lifecycle.
  const verifying = checking || (status === 'pending' && sandboxRunning);
  // Pending while the sandbox is coming up: discovery can't run yet, but a warm
  // is in flight, so the verify step is active ("Starting workspace…") rather
  // than a dead "Waiting…".
  const warmingUp = status === 'pending' && !sandboxRunning && sandboxWarming;
  const verified = status === 'connected';

  const verifyState: StepState = verified ? 'done' : verifying || warmingUp ? 'active' : 'todo';
  const readyState: StepState = verified ? (synced ? 'done' : 'active') : 'todo';

  let label: string;
  if (verifying) label = 'Verifying…';
  else if (warmingUp) label = 'Starting workspace…';
  else if (status === 'pending') label = 'Waiting for workspace to start';
  else if (verified && !synced) label = sandboxRunning ? 'Applying to agent…' : 'Applies when workspace starts';
  else label = 'Ready';

  const phase = verifying
    ? 'verifying'
    : warmingUp
      ? 'starting'
      : readyState === 'active'
        ? 'applying'
        : 'waiting';

  return (
    <span
      className="inline-flex items-center gap-1.5"
      data-testid="mcp-lifecycle"
      data-phase={phase}
    >
      <LifecycleTrack steps={[{ key: 'saved', state: 'done' }, { key: 'verify', state: verifyState }, { key: 'ready', state: readyState }]} />
      <span className="text-[0.6875rem]" style={{ color: 'var(--color-text-tertiary)' }}>{label}</span>
    </span>
  );
}

function LifecycleTrack({ steps }: { steps: Array<{ key: string; state: StepState }> }) {
  return (
    <span className="inline-flex items-center" aria-hidden>
      {steps.map((step, i) => (
        <React.Fragment key={step.key}>
          <Node state={step.state} />
          {i < steps.length - 1 && (
            <Connector
              filled={step.state === 'done'}
              shimmer={step.state === 'done' && steps[i + 1].state === 'active'}
            />
          )}
        </React.Fragment>
      ))}
    </span>
  );
}

function Node({ state }: { state: StepState }) {
  const color =
    state === 'done'
      ? 'var(--color-profit)'
      : state === 'active'
        ? 'var(--color-accent-primary)'
        : 'transparent';
  return (
    <motion.span
      className="inline-block rounded-full"
      style={{
        width: 7,
        height: 7,
        background: color,
        border: state === 'todo' ? '1.5px solid var(--color-border-muted)' : undefined,
      }}
      // The active node breathes; done/todo are static.
      animate={state === 'active' ? { scale: [1, 1.35, 1], opacity: [1, 0.6, 1] } : { scale: 1, opacity: 1 }}
      transition={state === 'active' ? { duration: 1.2, repeat: Infinity, ease: 'easeInOut' } : SPRING}
    />
  );
}

function Connector({ filled, shimmer }: { filled: boolean; shimmer: boolean }) {
  return (
    <span
      className="relative inline-block overflow-hidden"
      style={{
        width: 14,
        height: 3,
        margin: '0 2px',
        borderRadius: 1.5,
        background: filled ? 'var(--color-profit)' : 'var(--color-border-muted)',
      }}
    >
      {shimmer && (
        <motion.span
          className="absolute inset-y-0"
          style={{ width: '40%', background: 'rgba(255,255,255,0.45)' }}
          animate={{ left: ['-40%', '100%'] }}
          transition={{ duration: 1.2, repeat: Infinity, ease: 'easeInOut' }}
        />
      )}
    </span>
  );
}
