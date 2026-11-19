/**
 * Claude Code on a cloud computer, for a user with no computer of their own,
 * as three steps taken in order: a plan whose cloud computer is Medium or
 * larger, Home's computer changed to that size, then a setup token. Read
 * from what the server says about each (the plan's quota, Home's row, the
 * saved token), so a step done anywhere else, or by a plan change, shows
 * done here too.
 *
 * The plan step exists only where a plan page does (platform mode). The
 * token step opens once the change to Medium has started, since the token
 * runs nowhere before, or once the user has agreed to store one; it can be
 * done while the change runs. The steps are setup: once all three are done
 * they give way to the full view, which manages the token.
 *
 * They are offered only where they can be finished: Home's provider offers
 * Medium, and either the plan opens it or a bigger plan would. A read that
 * failed (the quota, Home's provider, the account's rules on /users/me)
 * offers none, rather than a step read as done or a rule read as allowing.
 */
import type { TFunction } from 'i18next';
import { useTranslation } from 'react-i18next';
import { useNavigate } from 'react-router';
import type { Computer, ComputerSpecChange, ResourceTier } from '@/types/api';
import { isPlatformMode } from '@/config/hostMode';
import { useAccount, useRule } from '@/hooks/useAccount';
import { RULE } from '@/lib/accountRules';
import { PLAN_URL } from '@/config/supportLinks';
import { CLAUDE_CODE_CHOICE_URL, CLAUDE_CODE_FEATURE, CLAUDE_CODE_MIN_TIER, CLEARED_BY_STEPS } from '@/lib/claudeCode';
import { providerManagesSpec } from '@/pages/ChatAgent/components/providerUi';
import { activeSpecChange, effectiveTier, smallerThan } from '@/pages/ChatAgent/components/specChangeUi';
import {
  TIER_ORDER, isTierBlocked, needsBiggerPlan, normalizeTier, offeredTiers, quotaForSpecChange, tierCapacity, tierLabel,
} from '@/pages/ChatAgent/components/tierUi';
import { primaryComputer, useComputerProviders, useComputers } from '@/pages/ChatAgent/hooks/useComputers';
import { useTierQuota } from '@/pages/ChatAgent/hooks/useTierQuota';
import { useClaudeCodeSetup, useClaudeCodeStatus } from './useClaudeCode';
import { useClaudeCodeCustody } from './useClaudeCodeCustody';
import { useFeatures } from './useFeatures';
import { useUser } from './useUser';
import { useWorkspace } from './useWorkspace';
import type { WorkspaceClaudeCode } from './useWorkspaceClaudeCode';

/** The step to take now; `resizing` while the change to Medium runs. */
export type NextStep = 'plan' | 'medium' | 'resizing' | 'token';

export interface ClaudeCodeSteps {
  /** `loading` while a read the steps stand on has not answered and the
   *  answers so far leave them possible; `pending` where they apply and a
   *  token is still to add; `none` otherwise. */
  status: 'loading' | 'pending' | 'none';
  next: NextStep;
  home: Computer | null;
  /** Null where there is no plan page, so no plan step. `current` is the
   *  plan's name, null where unknown. */
  plan: { done: boolean; current: string | null } | null;
  /** The size step 2 changes Home's computer to: the largest the plan opens
   *  and the provider builds, and never less than Claude Code's minimum. */
  size: ResourceTier;
  /** The largest size Home's provider builds, which the plan page names its
   *  plans' computers by; null where the provider is not known. */
  largest: ResourceTier | null;
  /** `change` is the change running on Home's computer, either way: one to
   *  the size or larger runs the step, any other holds it `locked` until it
   *  ends, since no other change starts while one runs. */
  medium: { state: 'done' | 'active' | 'locked' | 'running'; change: ComputerSpecChange | null };
  token: { state: 'done' | 'active' | 'locked' };
}

export function useClaudeCodeSteps({ enabled = true }: { enabled?: boolean } = {}): ClaudeCodeSteps {
  const { setup, isLoading: setupLoading } = useClaudeCodeSetup({ enabled });
  const { custody, isLoading: custodyLoading } = useClaudeCodeCustody({ enabled });
  const status = useClaudeCodeStatus({ enabled });
  const feature = useFeatures().data?.find((f) => f.key === CLAUDE_CODE_FEATURE);
  // A rule against the user's own subscriptions keeps Claude Code off: no step
  // turns it on or asks for a token.
  const subscriptions = useRule(RULE.ownSubscriptions);
  const { unknown: rulesUnread } = useAccount();
  const { data: list, isLoading: listLoading } = useComputers({ enabled });
  const home = primaryComputer(list?.computers);
  const cloudHome = !!home && providerManagesSpec(home.kind);
  const { data: quota, isLoading: quotaLoading } = useTierQuota({ enabled: enabled && cloudHome });
  const { data: providers, isLoading: providersLoading } = useComputerProviders({ enabled: enabled && cloudHome });
  const { user, isLoading: userLoading } = useUser({ enabled });
  const hasPlanStep = !!PLAN_URL;
  const slotQuota = home ? quotaForSpecChange(quota, normalizeTier(home.resource_tier)) : null;
  const needsPlan = hasPlanStep && cloudHome && needsBiggerPlan(slotQuota, CLAUDE_CODE_MIN_TIER);

  const change = home ? activeSpecChange(home) : null;
  const medium: ClaudeCodeSteps['medium']['state'] = home && change && !smallerThan(home, CLAUDE_CODE_MIN_TIER) ? 'running'
    : home && !smallerThan(home, CLAUDE_CODE_MIN_TIER) ? 'done'
      : needsPlan || change ? 'locked' : 'active';
  const provider = home ? providers?.providers.find((p) => p.kind === home.kind) : undefined;
  const builds = offeredTiers(provider).tiers;
  const size = [...builds].reverse().find((tier) => {
    if (TIER_ORDER.indexOf(tier) < TIER_ORDER.indexOf(CLAUDE_CODE_MIN_TIER)) return false;
    // Above the minimum only on a size the plan is known to open: a server
    // that reports no quota, or none for that size, says nothing about it.
    const capacity = tierCapacity(slotQuota, tier);
    return tier === CLAUDE_CODE_MIN_TIER || (!!capacity && !isTierBlocked(capacity));
  }) ?? CLAUDE_CODE_MIN_TIER;
  const mediumReachable = medium === 'done' || medium === 'running' || (
    !!home && offeredTiers(provider, effectiveTier(home)).tiers.includes(CLAUDE_CODE_MIN_TIER)
    && (needsPlan || !isTierBlocked(tierCapacity(slotQuota, CLAUDE_CODE_MIN_TIER)))
  );
  const tokenDone = custody?.mode === 'stored' && status.saved;
  const token: ClaudeCodeSteps['token']['state'] = tokenDone ? 'done'
    : medium === 'running' || medium === 'done' || custody?.mode === 'stored' ? 'active' : 'locked';
  const plan = hasPlanStep ? { done: !needsPlan, current: user?.plan_display_name ?? null } : null;
  const next: NextStep = plan && !plan.done ? 'plan'
    : medium === 'running' ? 'resizing' : medium === 'done' ? 'token' : 'medium';

  // While Claude Code is off the choice is not read, since the server answers
  // it only once it is on; `setup.cloud` says storing a token would be offered.
  const cloudOffered = !!setup?.cloud && (!setup.on || (!!custody && (custody.mode === 'stored' || custody.offered.includes('stored'))));
  // An organization's off keeps the gate as it is and refuses the turn-on.
  const canTurnOn = !!setup?.on || (!feature?.set_by_organization && (feature?.gate === 'opt_in' || feature?.gate === 'opt_out'));
  const quotaRead = !isPlatformMode || !cloudHome || !!quota;
  const allowed = !!user && !rulesUnread && subscriptions.allowed;
  const applies = allowed && cloudOffered && canTurnOn && setup.computers.length === 0 && cloudHome && quotaRead && !!provider && mediumReachable;
  const ready = !setupLoading && !custodyLoading && !status.isLoading && !listLoading && !(cloudHome && (quotaLoading || providersLoading || userLoading));
  const possible = !setup ? setupLoading : setup.cloud && setup.computers.length === 0;
  return {
    status: !ready ? (possible ? 'loading' : 'none') : applies && !(tokenDone && medium === 'done') ? 'pending' : 'none',
    next,
    home,
    plan,
    size,
    largest: provider ? builds.at(-1) ?? null : null,
    medium: { state: medium, change },
    token: { state: token },
  };
}

/** What a Claude Code model's picker row says is missing while the steps are
 *  pending: the step to take now, by what it needs. */
export function stepsMissing(t: TFunction, { next, medium }: ClaudeCodeSteps): string {
  const step = next === 'medium' && medium.change ? 'resizing' : next;
  const size = tierLabel(t, step === 'resizing' && medium.change ? medium.change.target_tier : CLAUDE_CODE_MIN_TIER);
  return t(`modelAccess.claudeCode.steps.picker.${step}`, { size });
}

/** The hold a model picker puts on Claude Code's models while the steps are
 *  pending and would clear the turn's refusal: the step to take, opening the
 *  steps. They are Home's, so only a Chief of Staff turn or a turn in a
 *  workspace (`workspaceId`) on Home's computer takes it; a workspace on
 *  another computer keeps its own hold. */
export function useStepsHold(
  claudeCode: WorkspaceClaudeCode, chiefOfStaff: boolean, workspaceId: string | null | undefined, enabled: boolean,
): { unavailable: string; fix: () => void; fixLabel: string } | null {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const steppable = enabled && CLEARED_BY_STEPS.has(claudeCode.state);
  const steps = useClaudeCodeSteps({ enabled: steppable });
  const workspace = useWorkspace(steppable && !chiefOfStaff ? workspaceId : null);
  const turnComputer = claudeCode.smallComputerId ?? workspace.data?.computer_id;
  const onHome = chiefOfStaff || (!!steps.home && turnComputer === steps.home.computer_id);
  if (!steppable || !onHome || steps.status !== 'pending') return null;
  return {
    unavailable: stepsMissing(t, steps),
    fix: () => navigate(CLAUDE_CODE_CHOICE_URL),
    fixLabel: t('modelAccess.claudeCode.steps.setUp'),
  };
}
