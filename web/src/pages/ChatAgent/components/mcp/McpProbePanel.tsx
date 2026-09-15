import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { AlertCircle, CheckCircle2, KeyRound, ShieldCheck, Wrench, Zap } from 'lucide-react';
import { Loader } from '@/components/ui/loader';
import type { McpProbeResult } from '../../utils/api';

/**
 * The verdict of the host-side check of a remote address, shown inside the
 * add form while the user is still typing. One line says what the wire said
 * (open, credential accepted, credential wanted, OAuth, unreachable) and the
 * tool list sits behind a disclosure so a 20-tool server does not push the
 * Save button off screen.
 */

interface McpProbePanelProps {
  result: McpProbeResult | null;
  probing: boolean;
  /** Whether the address is one the check can be run on right now. */
  canCheck: boolean;
  hasHeaders: boolean;
  onCheck: () => void;
}

export function McpProbePanel({ result, probing, canCheck, hasHeaders, onCheck }: McpProbePanelProps) {
  const { t } = useTranslation();
  const [toolsOpen, setToolsOpen] = useState(false);

  const checkButton = (
    <button
      type="button"
      onClick={onCheck}
      disabled={!canCheck || probing}
      className="inline-flex items-center gap-1 text-[0.6875rem] disabled:opacity-50"
      style={{ color: 'var(--color-accent-primary)' }}
      data-testid="mcp-probe-check"
    >
      <Zap className="h-3 w-3" />
      {t('mcp.probe.check')}
    </button>
  );

  if (probing) {
    return (
      <div
        className="flex items-center gap-2 text-xs p-2 rounded"
        style={{ backgroundColor: 'var(--color-bg-card)', color: 'var(--color-text-tertiary)' }}
        data-testid="mcp-probe-checking"
      >
        <Loader size={14} className="text-current" />
        {t('mcp.probe.checking')}
      </div>
    );
  }

  if (!result) {
    return canCheck ? <div className="flex">{checkButton}</div> : null;
  }

  if (result.status === 'error') {
    const oauth = result.auth === 'oauth';
    const wantsCredential = result.auth === 'credential' && !hasHeaders;
    const missing = result.missing_secrets.length > 0;
    const tone = oauth || wantsCredential || missing ? 'var(--color-text-secondary)' : 'var(--color-loss)';
    const Icon = oauth ? ShieldCheck : wantsCredential || missing ? KeyRound : AlertCircle;
    return (
      <div
        className="flex flex-col gap-1.5 text-xs p-2 rounded"
        style={{ backgroundColor: 'var(--color-bg-card)', color: tone }}
        data-testid="mcp-probe-error"
      >
        <div className="flex items-start gap-2">
          <Icon className="h-3.5 w-3.5 flex-shrink-0 mt-0.5" />
          <span className="whitespace-pre-wrap break-words">
            {missing
              ? t('mcp.probe.missingSecrets', { names: result.missing_secrets.join(', ') })
              : oauth
                ? t('mcp.probe.authOauth')
                : wantsCredential
                  ? t('mcp.probe.authCredentialMissing')
                  : result.error || t('mcp.probe.failed')}
          </span>
        </div>
        {!oauth && !wantsCredential && !missing && result.error && result.http_status != null && (
          <span className="text-[0.625rem] pl-5" style={{ color: 'var(--color-text-tertiary)' }}>
            HTTP {result.http_status}
          </span>
        )}
        <div className="pl-5">{checkButton}</div>
      </div>
    );
  }

  const tools = result.tools ?? [];
  return (
    <div className="flex flex-col gap-1.5 text-xs p-2 rounded" style={{ backgroundColor: 'var(--color-bg-card)' }} data-testid="mcp-probe-ok">
      <div className="flex items-center justify-between gap-2">
        <div className="flex items-center gap-1.5 font-medium" style={{ color: 'var(--color-profit)' }}>
          <CheckCircle2 className="h-3.5 w-3.5" />
          {tools.length === 0 ? t('mcp.probe.okNoTools') : t('mcp.probe.ok', { count: tools.length })}
        </div>
        {checkButton}
      </div>
      <span className="text-[0.6875rem]" style={{ color: 'var(--color-text-tertiary)' }}>
        {result.auth === 'credential' ? t('mcp.probe.authCredential') : t('mcp.probe.authNone')}
        {result.server_info?.name ? ` ${result.server_info.name}${result.server_info.version ? ` ${result.server_info.version}` : ''}.` : ''}
      </span>
      {tools.length > 0 && (
        <button
          type="button"
          onClick={() => setToolsOpen((v) => !v)}
          aria-expanded={toolsOpen}
          className="text-[0.6875rem] self-start"
          style={{ color: 'var(--color-accent-primary)' }}
        >
          {toolsOpen ? t('mcp.probe.hideTools') : t('mcp.probe.showTools')}
        </button>
      )}
      {toolsOpen && tools.length > 0 && (
        <div className="flex flex-col gap-1 max-h-40 overflow-y-auto">
          {tools.map((tool) => (
            <div key={tool.name} className="flex items-start gap-2 py-1 px-1.5 rounded" style={{ backgroundColor: 'var(--color-bg-elevated)' }}>
              <Wrench className="h-3 w-3 flex-shrink-0 mt-0.5" style={{ color: 'var(--color-accent-primary)' }} />
              <div className="min-w-0">
                <span className="font-mono" style={{ color: 'var(--color-text-primary)' }}>{tool.name}</span>
                {tool.description && (
                  <p className="text-[0.625rem] mt-0.5 line-clamp-2" style={{ color: 'var(--color-text-tertiary)' }}>
                    {tool.description}
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
