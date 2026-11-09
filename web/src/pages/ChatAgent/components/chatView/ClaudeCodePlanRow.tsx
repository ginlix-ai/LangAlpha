import type React from 'react';
import { useTranslation } from 'react-i18next';
import { useAllModels } from '@/hooks/useAllModels';
import { usePlanClock, usePlanOut } from '@/hooks/useClaudeCodePlan';
import { useTurnClaudeCode } from '@/hooks/useWorkspaceClaudeCode';
import { isClaudeCodeModel } from '@/lib/claudeCode';
import { CheckNowFor } from '@/components/modelAccess/PlanCheck';
import { composerPlanLine, creditsRefused } from '@/lib/claudeCodePlan';
import { ComposerRow } from './ComposerRow';

/* Above the composer of a thread on a Claude Code model whose Claude plan,
   the one behind the token the workspace's turns run on (the Chief of
   Staff's in Home, `chiefOfStaff`), is at its limit:
   used up until it resets, or running on usage credits until then. The
   model pill below is where another model is picked, so the row acts only
   where the user chose usage credits and the account has them off: Check
   now then costs nothing, and reads them once they are on. */
export function ClaudeCodePlanRow({ workspaceId, chiefOfStaff = false, model }: {
  workspaceId: string | null | undefined;
  chiefOfStaff?: boolean;
  model: string | null | undefined;
}): React.ReactElement | null {
  const { t } = useTranslation();
  const clock = usePlanClock();
  const { metadata } = useAllModels();
  const { plan, meter } = useTurnClaudeCode(chiefOfStaff, workspaceId);
  const { planOut } = usePlanOut();
  if (!model || !isClaudeCodeModel(metadata[model])) return null;
  const line = composerPlanLine(t, plan, planOut, model, clock);
  if (!line) return null;
  const credential = meter?.credential;
  return (
    <ComposerRow text={line}>
      {credential && creditsRefused(plan, planOut, model, clock.now) && (
        <CheckNowFor target={{ credential, checkAfter: meter.checkAfter }} />
      )}
    </ComposerRow>
  );
}
