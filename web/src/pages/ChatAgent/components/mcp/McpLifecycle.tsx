import React from 'react';
import { useTranslation } from 'react-i18next';
import { motion } from 'framer-motion';
import { McpOauthPill, McpStatusPill } from './McpStatusPill';
import { deriveLifecycle, type McpLifecycleInput, type McpLifecycleStep } from './mcpState';

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
 *
 * Which of those to render is decided by `deriveLifecycle` in `mcpState.ts`;
 * this component only paints the result.
 */

const SPRING = { type: 'spring' as const, stiffness: 200, damping: 22 };

export function McpLifecycle(props: McpLifecycleInput) {
  const { t } = useTranslation();
  const view = deriveLifecycle(props);

  if (view.kind === 'status') return <McpStatusPill status={view.status} enabled={view.enabled} />;
  if (view.kind === 'oauth') return <McpOauthPill status={view.status} />;

  return (
    <span
      className="inline-flex items-center gap-1.5"
      data-testid="mcp-lifecycle"
      data-phase={view.phase}
    >
      <LifecycleTrack
        steps={[
          { key: 'saved', state: 'done' },
          { key: 'verify', state: view.verifyState },
          { key: 'ready', state: view.readyState },
        ]}
      />
      <span className="text-[0.6875rem]" style={{ color: 'var(--color-text-tertiary)' }}>{t(view.labelKey)}</span>
    </span>
  );
}

function LifecycleTrack({ steps }: { steps: Array<{ key: string; state: McpLifecycleStep }> }) {
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

function Node({ state }: { state: McpLifecycleStep }) {
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
