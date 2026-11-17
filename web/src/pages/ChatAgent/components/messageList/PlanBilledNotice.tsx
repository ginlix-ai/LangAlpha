import type React from 'react';
import { useTranslation } from 'react-i18next';
import type { PlanBilled } from '@/api/model';
import { NoticeLine } from '@/components/ui/error-banner';
import { usePlanClock } from '@/hooks/useClaudeCodePlan';
import { planBilledText } from '@/lib/claudeCodePlan';

/** Under a reply whose last request was billed to usage credits, worded in
 *  the reader's zone. */
export function PlanBilledNotice({ billed }: { billed: PlanBilled }): React.ReactElement {
  const { t } = useTranslation();
  return <NoticeLine message={planBilledText(t, billed, usePlanClock())} className="py-1.5" />;
}
