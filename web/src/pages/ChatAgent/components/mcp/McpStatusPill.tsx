import React from 'react';
import { useTranslation } from 'react-i18next';
import { AlertCircle, CheckCircle2, Clock, KeyRound, MinusCircle, HelpCircle } from 'lucide-react';
import { StatusPill } from './McpPrimitives';
import type { McpOauthStatus, McpStatus } from '../../utils/api';

/**
 * Status pills for MCP server rows — the two vocabularies mapped onto the one
 * shared `StatusPill` shape so the Plugins page and the workspace tab stay
 * pixel-identical:
 *
 * - `McpStatusPill` — per-workspace lifecycle terminal states (connected /
 *   error / needs_secret / pending / disabled / unknown). In-flight states are
 *   not rendered here — `McpLifecycle` owns the progressing track and
 *   delegates to this pill only for terminal states.
 * - `McpOauthPill` — the user-level OAuth connection state of a user-tier
 *   server (connected / needs_reauth / refresh_ambiguous / revoked).
 */

interface PillMeta {
  labelKey: string;
  color: string;
  bg: string;
  icon: React.ComponentType<{ className?: string }>;
}

const STATUS_META: Record<McpStatus, PillMeta> = {
  connected: {
    labelKey: 'mcp.status.connected',
    color: 'var(--color-profit)',
    bg: 'var(--color-profit-soft)',
    icon: CheckCircle2,
  },
  error: {
    labelKey: 'mcp.status.error',
    color: 'var(--color-loss)',
    bg: 'var(--color-loss-soft)',
    icon: AlertCircle,
  },
  needs_secret: {
    labelKey: 'mcp.status.needsSecret',
    color: 'var(--color-warning)',
    bg: 'var(--color-warning-soft)',
    icon: KeyRound,
  },
  pending: {
    labelKey: 'mcp.status.pending',
    color: 'var(--color-text-tertiary)',
    bg: 'var(--color-bg-tag)',
    icon: Clock,
  },
  disabled: {
    labelKey: 'mcp.status.disabled',
    color: 'var(--color-text-tertiary)',
    bg: 'var(--color-bg-tag)',
    icon: MinusCircle,
  },
  unknown: {
    labelKey: 'mcp.status.unknown',
    color: 'var(--color-text-tertiary)',
    bg: 'var(--color-bg-tag)',
    icon: HelpCircle,
  },
};

interface McpStatusPillProps {
  /** The effective status from the backend. A disabled row overrides to `disabled`. */
  status: McpStatus;
  enabled: boolean;
}

export function McpStatusPill({ status, enabled }: McpStatusPillProps) {
  const { t } = useTranslation();
  // A disabled row always reads as muted regardless of its last-known status.
  const effective: McpStatus = enabled ? status : 'disabled';
  const meta = STATUS_META[effective] ?? STATUS_META.unknown;
  return (
    <StatusPill
      icon={meta.icon}
      label={t(meta.labelKey)}
      color={meta.color}
      bg={meta.bg}
      title={effective === 'pending' ? t('mcp.status.pendingHint') : undefined}
      testid={`mcp-status-${effective}`}
    />
  );
}

const OAUTH_META: Record<McpOauthStatus, PillMeta> = {
  connected: {
    labelKey: 'plugins.oauth.connected',
    color: 'var(--color-profit)',
    bg: 'var(--color-profit-soft)',
    icon: CheckCircle2,
  },
  needs_reauth: {
    labelKey: 'plugins.oauth.needsReauth',
    color: 'var(--color-warning)',
    bg: 'var(--color-warning-soft)',
    icon: AlertCircle,
  },
  refresh_ambiguous: {
    labelKey: 'plugins.oauth.refreshAmbiguous',
    color: 'var(--color-warning)',
    bg: 'var(--color-warning-soft)',
    icon: AlertCircle,
  },
  revoked: {
    labelKey: 'plugins.oauth.revoked',
    color: 'var(--color-text-tertiary)',
    bg: 'var(--color-bg-tag)',
    icon: MinusCircle,
  },
};

/**
 * The i18n key naming an OAuth status, or null when there is no connection (or
 * a status this build doesn't know). Reading it off `OAUTH_META` is the point:
 * that record is exhaustive over `McpOauthStatus`, so a new status is a compile
 * error here instead of a surface that quietly labels it nothing.
 */
export function oauthLabelKey(status: McpOauthStatus | null | undefined): string | null {
  return (status && OAUTH_META[status]?.labelKey) || null;
}

export function McpOauthPill({ status }: { status: McpOauthStatus }) {
  const { t } = useTranslation();
  const meta = OAUTH_META[status];
  if (!meta) return null;
  return (
    <StatusPill
      icon={meta.icon}
      label={t(meta.labelKey)}
      color={meta.color}
      bg={meta.bg}
      testid={`oauth-status-${status}`}
    />
  );
}
