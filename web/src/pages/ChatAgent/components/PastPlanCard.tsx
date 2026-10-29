import React from 'react';
import { useTranslation } from 'react-i18next';
import { OverflowCollapse } from './messageList/OverflowCollapse';
import type { PastPlanOutcome } from './messageList/pastPlan';
import { SettledStep, type SettledVerdict } from './SettledStep';
import Markdown from './Markdown';

interface PastPlanCardProps {
  description: string;
  outcome: PastPlanOutcome;
}

/** Collapsed bound for a long plan; the old card's preview height. */
const PLAN_COLLAPSE_PX = 260;

const VERDICT: Record<PastPlanOutcome, SettledVerdict> = {
  approved: 'approved',
  rejected: 'rejected',
  unreviewed: 'none',
};

/**
 * A plan an older thread submitted for review, shown as a record. The agent no
 * longer submits plans, so nothing here can be answered: there are no controls
 * and no pending state, only the plan and what became of it. Open by default,
 * as the resolved card was, and bounded by a Show all toggle in place of the
 * detail tab it used to open.
 */
function PastPlanCard({ description, outcome }: PastPlanCardProps): React.ReactElement {
  const { t } = useTranslation();
  return (
    <SettledStep
      verdict={VERDICT[outcome]}
      label={t(`chat.pastPlan.${outcome}`)}
      defaultOpen
      testId="past-plan"
    >
      <div className="rounded-lg px-4 py-3" style={{ border: '1px solid var(--color-border-muted)' }}>
        <OverflowCollapse enabled maxHeight={PLAN_COLLAPSE_PX}>
          <Markdown variant="chat" content={description} className="text-sm" />
        </OverflowCollapse>
      </div>
    </SettledStep>
  );
}

export default PastPlanCard;
