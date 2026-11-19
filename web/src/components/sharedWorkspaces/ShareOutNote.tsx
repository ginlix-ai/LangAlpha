import { Users } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { cn } from '@/lib/utils';
import type { ShareOutState } from '@/hooks/useSharedWorkspaces';
import { shareOutLine } from '@/lib/shareOut';
import { useAccount } from '@/hooks/useAccount';

/** The workspace's own rule, drawn as the organization's rules are
 *  (`RuleNote`) but with the workspace's people for its glyph, so the two
 *  are told apart. */
export function ShareOutNote({ state, className }: { state: ShareOutState; className?: string }) {
  const { t } = useTranslation();
  const { organizationLabel } = useAccount();
  const line = shareOutLine(t, state, organizationLabel);
  if (!line) return null;
  return <WorkspaceRuleNote className={className}>{line}</WorkspaceRuleNote>;
}

export function WorkspaceRuleNote({ children, className }: { children: React.ReactNode; className?: string }) {
  return (
    <p
      className={cn('flex items-start gap-2 text-[0.8125rem] leading-snug', className)}
      style={{ color: 'var(--color-text-secondary)' }}
    >
      <Users aria-hidden="true" className="h-3.5 w-3.5 shrink-0 mt-0.5" style={{ color: 'var(--color-text-tertiary)' }} />
      <span className="min-w-0 wrap-break-word">{children}</span>
    </p>
  );
}
