import React from 'react';
import { AlertCircle, CheckCircle2, Clock, KeyRound, MinusCircle, HelpCircle } from 'lucide-react';
import type { McpStatus } from '../../utils/api';

/**
 * The terminal status pill for an effective MCP server row.
 *
 * - connected  → green
 * - error      → red (the row also surfaces the error text)
 * - needs_secret → amber (the row offers a "Set up NAME" affordance)
 * - pending    → gray ("waiting for discovery — start the workspace or test connection")
 * - disabled   → muted
 * - unknown    → muted fallback
 *
 * In-flight states (verifying / applying) are not rendered here — `McpLifecycle`
 * owns the progressing track and delegates to this pill only for terminal
 * states.
 */

interface StatusMeta {
  label: string;
  color: string;
  bg: string;
  icon: React.ComponentType<{ className?: string }>;
}

const STATUS_META: Record<McpStatus, StatusMeta> = {
  connected: {
    label: 'Connected',
    color: 'var(--color-profit)',
    bg: 'var(--color-profit-soft)',
    icon: CheckCircle2,
  },
  error: {
    label: 'Error',
    color: 'var(--color-loss)',
    bg: 'var(--color-loss-soft)',
    icon: AlertCircle,
  },
  needs_secret: {
    label: 'Needs secret',
    color: 'var(--color-warning, #d97706)',
    bg: 'var(--color-warning-soft)',
    icon: KeyRound,
  },
  pending: {
    label: 'Pending',
    color: 'var(--color-text-tertiary)',
    bg: 'var(--color-bg-tag)',
    icon: Clock,
  },
  disabled: {
    label: 'Disabled',
    color: 'var(--color-text-tertiary)',
    bg: 'var(--color-bg-tag)',
    icon: MinusCircle,
  },
  unknown: {
    label: 'Unknown',
    color: 'var(--color-text-tertiary)',
    bg: 'var(--color-bg-tag)',
    icon: HelpCircle,
  },
};

const PENDING_HINT = 'Waiting for discovery — start the workspace or test connection';

interface McpStatusPillProps {
  /** The effective status from the backend. A disabled row overrides to `disabled`. */
  status: McpStatus;
  enabled: boolean;
}

export function McpStatusPill({ status, enabled }: McpStatusPillProps) {
  // A disabled row always reads as muted regardless of its last-known status.
  const effective: McpStatus = enabled ? status : 'disabled';
  const meta = STATUS_META[effective] ?? STATUS_META.unknown;
  const Icon = meta.icon;
  return (
    <span
      className="inline-flex items-center gap-1 text-[0.6875rem] px-1.5 py-0.5 rounded font-medium"
      style={{ color: meta.color, backgroundColor: meta.bg }}
      title={effective === 'pending' ? PENDING_HINT : undefined}
      data-testid={`mcp-status-${effective}`}
    >
      <Icon className="h-3 w-3" />
      {meta.label}
    </span>
  );
}
