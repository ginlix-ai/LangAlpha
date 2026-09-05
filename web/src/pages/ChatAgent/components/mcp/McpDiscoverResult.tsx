import React from 'react';
import { AlertCircle, CheckCircle2, Clock, Wrench } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import type { McpDiscoveryResult } from '../../utils/api';
import { McpStatusPill } from './McpStatusPill';

/**
 * Renders the outcome of a discovery probe: the discovered tool list on a
 * `connected` success, the error text on failure, or a "pending" note when the
 * workspace was stopped and discovery couldn't run yet. Anything else defers to
 * the canonical status pill rather than asserting a connection it never saw.
 */

interface McpDiscoverResultProps {
  result: McpDiscoveryResult;
}

export function McpDiscoverResult({ result }: McpDiscoverResultProps) {
  const { t } = useTranslation();
  const status = result.status;

  if (status === 'error') {
    return (
      <div
        className="flex items-start gap-2 text-xs p-2 rounded"
        style={{ backgroundColor: 'var(--color-bg-card)', color: 'var(--color-loss)' }}
        data-testid="mcp-discover-error"
      >
        <AlertCircle className="h-3.5 w-3.5 flex-shrink-0 mt-0.5" />
        <span className="whitespace-pre-wrap break-words">
          {result.error || t('mcp.discover.failed')}
        </span>
      </div>
    );
  }

  if (status === 'pending') {
    return (
      <div
        className="flex items-center gap-2 text-xs p-2 rounded"
        style={{ backgroundColor: 'var(--color-bg-card)', color: 'var(--color-text-tertiary)' }}
        data-testid="mcp-discover-pending"
      >
        <Clock className="h-3.5 w-3.5 flex-shrink-0" />
        {t('mcp.status.pendingHint')}
      </div>
    );
  }

  if (status !== 'connected') {
    // `needs_secret`, `disabled` and `unknown` used to fall into the branch
    // below and be painted as a green "Connected", which is the probe claiming
    // an outcome it never got. Defer to the pill's exhaustive table so each
    // keeps its real name, and so a status added later is a compile error
    // there rather than a wrong label here.
    return (
      <div className="flex" data-testid="mcp-discover-status">
        <McpStatusPill status={status} enabled />
      </div>
    );
  }

  const tools = result.tools ?? [];
  return (
    <div className="flex flex-col gap-1.5" data-testid="mcp-discover-ok">
      <div className="flex items-center gap-1.5 text-xs font-medium" style={{ color: 'var(--color-profit)' }}>
        <CheckCircle2 className="h-3.5 w-3.5" />
        {tools.length === 0
          ? t('mcp.discover.noTools')
          : t('mcp.discover.toolCount', { count: tools.length })}
      </div>
      {tools.length > 0 && (
        <div className="flex flex-col gap-1 max-h-48 overflow-y-auto">
          {tools.map((t) => (
            <div
              key={t.name}
              className="flex items-start gap-2 py-1.5 px-2 rounded text-xs"
              style={{ backgroundColor: 'var(--color-bg-card)' }}
            >
              <Wrench className="h-3.5 w-3.5 flex-shrink-0 mt-0.5" style={{ color: 'var(--color-accent-primary)' }} />
              <div className="min-w-0">
                <span className="font-mono" style={{ color: 'var(--color-text-primary)' }}>{t.name}</span>
                {t.description && (
                  <p className="text-[0.6875rem] mt-0.5 line-clamp-2" style={{ color: 'var(--color-text-tertiary)' }}>
                    {t.description}
                  </p>
                )}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
