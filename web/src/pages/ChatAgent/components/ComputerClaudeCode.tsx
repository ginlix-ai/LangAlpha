/**
 * Claude Code on this computer, in its details: the same line Settings shows
 * for it (its status, which token it uses, what can be done about it), so
 * a computer's token is set where the computer is managed too. A computer
 * Claude Code does not list (a hosted one) shows nothing.
 */
import { useTranslation } from 'react-i18next';

import { StatusActions } from '@/components/modelAccess/parts';
import { TokenActions } from '@/components/modelAccess/ClaudeCodeToken';
import { computerTokenLine } from '@/components/modelAccess/tokenLine';
import { useClaudeCodeComputers } from '@/hooks/useClaudeCode';
import { useLocale } from '@/hooks/useLocale';
import { CLAUDE_CODE_CONNECTION } from '@/lib/claudeCode';

import type { MachineActions } from './computerParts';

export function ComputerClaudeCode({ computerId, actions }: { computerId: string; actions: MachineActions }) {
  const { t } = useTranslation();
  const locale = useLocale();
  // Read with Claude Code off too, which still offers to remove a token left here.
  const entry = useClaudeCodeComputers({ whenOff: true }).computers.find((c) => c.computer_id === computerId);
  const line = entry && computerTokenLine(t, locale, entry);
  if (!line) return null;
  return (
    <section className="computer-details-group">
      <h3 className="computer-details-title">{CLAUDE_CODE_CONNECTION.display_name}</h3>
      {/* As the Settings row wraps: too narrow for both, the status and the
          actions take a line of their own under the detail. */}
      <div className="flex flex-wrap items-center gap-x-3 gap-y-2">
        {line.detail && (
          <p className="min-w-0 flex-[1_1_12rem] text-xs wrap-break-word" style={{ color: 'var(--color-text-tertiary)' }}>
            {line.detail}
          </p>
        )}
        <StatusActions status={line.status}>
          <TokenActions actions={line.actions} onOpen={actions.claudeCodeToken} onRemove={actions.removeClaudeCodeToken} />
        </StatusActions>
      </div>
    </section>
  );
}
