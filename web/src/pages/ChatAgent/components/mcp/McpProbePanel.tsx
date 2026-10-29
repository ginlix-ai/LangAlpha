import { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { KeyRound, Wrench } from 'lucide-react';
import { Disclosure } from '@/components/ui/Disclosure';
import { Loader } from '@/components/ui/loader';
import { cn } from '@/lib/utils';
import { McpQuietButton } from './McpQuietButton';
import {
  verdictDetail,
  verdictMessageKey,
  verdictUi,
  verdictWire,
  type ProbeTone,
} from './mcpProbeVerdict';
import type { McpProbeResult } from '../../utils/api';

/**
 * The verdict of the host-side check of a remote address, shown inside the
 * add form while the user is still typing. One line says what the wire said,
 * and the tool list sits behind a disclosure so a 20-tool server does not push
 * the Save button off screen. The words for each verdict are in
 * `mcpProbeVerdict.ts`, shared with the connector's own page.
 */

const TONE_COLOR: Record<ProbeTone, string> = {
  good: 'var(--color-profit)',
  attention: 'var(--color-text-secondary)',
  bad: 'var(--color-loss)',
};

/** Why there is no answer on screen for the form as it stands. */
export type ProbePending =
  /** A key went into a form whose address is already settled: check it. */
  | 'keyAdded'
  /** A key is in the form and the address has not been said to be finished,
   *  so the automatic check is holding the key back from it. */
  | 'keyHeld';

interface McpProbePanelProps {
  result: McpProbeResult | null;
  probing: boolean;
  /** Whether the address is one the check can be run on right now. */
  canCheck: boolean;
  onCheck: () => void;
  /** Set when the form has a key with no answer for it yet. */
  pending?: ProbePending | null;
}

export function McpProbePanel({ result, probing, canCheck, onCheck, pending = null }: McpProbePanelProps) {
  const { t } = useTranslation();
  const [toolsOpen, setToolsOpen] = useState(false);

  const checkButton = (label: string, tone: 'neutral' | 'primary' = 'neutral') => (
    <McpQuietButton
      onClick={onCheck}
      disabled={!canCheck || probing}
      tone={tone}
      data-testid="mcp-probe-check"
    >
      {label}
    </McpQuietButton>
  );

  if (probing) {
    return (
      <div
        className="flex items-center gap-2 text-xs p-2 rounded-md"
        style={{ backgroundColor: 'var(--color-bg-card)', color: 'var(--color-text-tertiary)' }}
        data-testid="mcp-probe-checking"
        role="status"
      >
        <Loader size={14} className="text-current" />
        {t('mcp.probe.checking')}
      </div>
    );
  }

  if (!result) {
    if (!pending || !canCheck) return null;
    return (
      <div
        className="flex items-center justify-between gap-2 text-xs p-2 rounded-md"
        style={{ backgroundColor: 'var(--color-bg-card)' }}
        data-testid="mcp-probe-pending"
        data-pending={pending}
      >
        <div className="flex items-start gap-2 min-w-0" style={{ color: 'var(--color-text-secondary)' }}>
          <KeyRound className="h-3.5 w-3.5 shrink-0 mt-0.5" />
          <span className="wrap-break-word">
            {pending === 'keyAdded' ? t('mcp.probe.keyAdded') : t('mcp.probe.keyHeld')}
          </span>
        </div>
        <div className="shrink-0">{checkButton(t('mcp.probe.checkNow'), 'primary')}</div>
      </div>
    );
  }

  const { Icon, tone } = verdictUi(result.verdict);
  const tools = result.tools ?? [];
  const detailLine = verdictDetail(result, t);
  // The server's own words are a technical aside to the sentence above them,
  // and set smaller so they never read as the explanation.
  const wire = !!verdictWire(result);

  return (
    <div
      className="flex flex-col gap-1.5 text-xs p-2 rounded-md"
      style={{ backgroundColor: 'var(--color-bg-card)' }}
      data-testid="mcp-probe"
      data-verdict={result.verdict}
    >
      <div className="flex items-start justify-between gap-2">
        <div className="flex items-start gap-2 min-w-0" style={{ color: TONE_COLOR[tone] }}>
          <Icon className="h-3.5 w-3.5 shrink-0 mt-0.5" />
          <span className="whitespace-pre-wrap wrap-break-word">
            {t(verdictMessageKey(result), { names: (result.missing_secrets ?? []).join(', ') })}
          </span>
        </div>
        <div className="shrink-0">{checkButton(t('mcp.probe.recheck'))}</div>
      </div>

      {detailLine && (
        <span
          className={cn('pl-5 wrap-break-word', wire ? 'text-[0.625rem]' : 'text-[0.6875rem]')}
          style={{ color: 'var(--color-text-tertiary)' }}
          data-testid={wire ? 'mcp-probe-wire' : undefined}
        >
          {detailLine}
        </span>
      )}

      {tools.length > 0 && (
        <McpQuietButton
          onClick={() => setToolsOpen((v) => !v)}
          aria-expanded={toolsOpen}
          className="self-start ml-5"
        >
          {toolsOpen ? t('mcp.probe.hideTools') : t('mcp.probe.showTools')}
        </McpQuietButton>
      )}
      <Disclosure open={toolsOpen && tools.length > 0}>
        <div className="flex flex-col gap-1 max-h-40 overflow-y-auto">
          {tools.map((tool) => (
            <div
              key={tool.name}
              className="flex items-start gap-2 py-1 px-1.5 rounded"
              style={{ backgroundColor: 'var(--color-bg-elevated)' }}
            >
              <Wrench className="h-3 w-3 shrink-0 mt-0.5" style={{ color: 'var(--color-text-tertiary)' }} />
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
