import type React from 'react';
import { useTranslation } from 'react-i18next';
import { useAllModels } from '@/hooks/useAllModels';
import { usePlanClock, usePlanOut } from '@/hooks/useClaudeCodePlan';
import { useWorkspaceClaudeCode } from '@/hooks/useWorkspaceClaudeCode';
import { isClaudeCodeModel } from '@/lib/claudeCode';
import { composerPlanLine } from '@/lib/claudeCodePlan';
import { ComposerRow } from './ComposerRow';

/* Above the composer of a thread on a Claude Code model whose Claude plan,
   the one behind the token the workspace's turns run on, is at its limit:
   used up until it resets, or running on usage credits until then. The
   model pill below is where another model is picked, so the row has nothing
   to act on. */
export function ClaudeCodePlanRow({ workspaceId, model }: {
  workspaceId: string | null | undefined;
  model: string | null | undefined;
}): React.ReactElement | null {
  const { t } = useTranslation();
  const clock = usePlanClock();
  const { metadata } = useAllModels();
  const { plan } = useWorkspaceClaudeCode(workspaceId);
  const { planOut } = usePlanOut();
  if (!model || !isClaudeCodeModel(metadata[model])) return null;
  const line = composerPlanLine(t, plan, planOut, model, clock);
  return line ? <ComposerRow text={line} /> : null;
}
