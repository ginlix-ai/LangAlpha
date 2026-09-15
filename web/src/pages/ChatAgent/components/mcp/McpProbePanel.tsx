import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { AlertCircle, CheckCircle2, KeyRound, ShieldCheck, Wrench, Zap } from 'lucide-react';
import { Disclosure } from '@/components/ui/Disclosure';
import { Loader } from '@/components/ui/loader';
import type { McpProbeResult, ProbeVerdict } from '../../utils/api';

/**
 * The verdict of the host-side check of a remote address, shown inside the
 * add form while the user is still typing. One line says what the wire said,
 * and the tool list sits behind a disclosure so a 20-tool server does not push
 * the Save button off screen.
 *
 * The verdict is computed on the host, which is the only side that knows
 * whether a credential was sent, so nothing here re-derives it: the table
 * below is the whole mapping from word to tone, glyph and sentence.
 */

type ProbeTone = 'good' | 'attention' | 'bad';

interface VerdictUi {
  Icon: React.ComponentType<{ className?: string }>;
  tone: ProbeTone;
  messageKey: string;
  /** The quiet second line: the tools this answer came with, or what the wire
   *  reported. `none` for the verdicts whose sentence is the whole answer. */
  detail: 'tools' | 'wire' | 'none';
}

const VERDICT_UI: Record<ProbeVerdict, VerdictUi> = {
  ok: { Icon: CheckCircle2, tone: 'good', messageKey: 'mcp.probe.authNone', detail: 'tools' },
  ok_authed: {
    Icon: ShieldCheck,
    tone: 'good',
    messageKey: 'mcp.probe.authCredential',
    detail: 'tools',
  },
  needs_credential: {
    Icon: KeyRound,
    tone: 'attention',
    messageKey: 'mcp.probe.authCredentialMissing',
    detail: 'none',
  },
  credential_rejected: {
    Icon: KeyRound,
    tone: 'bad',
    messageKey: 'mcp.probe.credentialRejected',
    detail: 'wire',
  },
  oauth: { Icon: ShieldCheck, tone: 'attention', messageKey: 'mcp.probe.authOauth', detail: 'none' },
  missing_secrets: {
    Icon: KeyRound,
    tone: 'attention',
    messageKey: 'mcp.probe.missingSecrets',
    detail: 'none',
  },
  unreachable: { Icon: AlertCircle, tone: 'bad', messageKey: 'mcp.probe.failed', detail: 'wire' },
};

const TONE_COLOR: Record<ProbeTone, string> = {
  good: 'var(--color-profit)',
  attention: 'var(--color-text-secondary)',
  bad: 'var(--color-loss)',
};

// A word this build has no entry for reads as unreachable rather than
// crashing the form it sits in.
const UNKNOWN_VERDICT = VERDICT_UI.unreachable;

interface McpProbePanelProps {
  result: McpProbeResult | null;
  probing: boolean;
  /** Whether the address is one the check can be run on right now. */
  canCheck: boolean;
  onCheck: () => void;
}

export function McpProbePanel({ result, probing, canCheck, onCheck }: McpProbePanelProps) {
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

  const { Icon, tone, messageKey, detail } = VERDICT_UI[result.verdict] ?? UNKNOWN_VERDICT;
  const tools = result.tools ?? [];
  const identity = result.server_info?.name
    ? ` ${result.server_info.name}${result.server_info.version ? ` ${result.server_info.version}` : ''}.`
    : '';
  const detailLine =
    detail === 'tools'
      ? `${tools.length === 0 ? t('mcp.probe.okNoTools') : t('mcp.probe.ok', { count: tools.length })}${identity}`
      : detail === 'wire'
        ? [result.error, result.http_status != null ? `HTTP ${result.http_status}` : '']
            .filter(Boolean)
            .join(' · ')
        : '';

  return (
    <div
      className="flex flex-col gap-1.5 text-xs p-2 rounded"
      style={{ backgroundColor: 'var(--color-bg-card)' }}
      data-testid="mcp-probe"
      data-verdict={result.verdict}
    >
      <div className="flex items-start justify-between gap-2">
        <div className="flex items-start gap-2 min-w-0" style={{ color: TONE_COLOR[tone] }}>
          <Icon className="h-3.5 w-3.5 flex-shrink-0 mt-0.5" />
          <span className="whitespace-pre-wrap break-words">
            {t(messageKey, { names: (result.missing_secrets ?? []).join(', ') })}
          </span>
        </div>
        <div className="flex-shrink-0">{checkButton}</div>
      </div>

      {detailLine && (
        <span className="text-[0.6875rem] pl-5 break-words" style={{ color: 'var(--color-text-tertiary)' }}>
          {detailLine}
        </span>
      )}

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
      <Disclosure open={toolsOpen && tools.length > 0}>
        <div className="flex flex-col gap-1 max-h-40 overflow-y-auto">
          {tools.map((tool) => (
            <div
              key={tool.name}
              className="flex items-start gap-2 py-1 px-1.5 rounded"
              style={{ backgroundColor: 'var(--color-bg-elevated)' }}
            >
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
      </Disclosure>
    </div>
  );
}
