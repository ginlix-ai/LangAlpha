/**
 * What gets a cloud computer too small for Claude Code to the size it runs
 * at, as every surface that names one offers it: the change-spec dialog on
 * that size, the plan page where the user's plan keeps it closed, or the
 * change already running. Decided from what the server says (the computer's
 * row, the sizes its provider offers, the plan's quota), so a self-hosted
 * server, which reports no quota, never sends anyone to a plan.
 */
import { useEffect } from 'react';
import { useLocation, useNavigate, useSearchParams } from 'react-router';
import { useTranslation } from 'react-i18next';
import { useQueryClient } from '@tanstack/react-query';

import { PLAN_URL } from '@/config/supportLinks';
import { openConsolePage } from '@/lib/desktop';
import { CLAUDE_CODE_CHOICE_URL, CLAUDE_CODE_MIN_TIER } from '@/lib/claudeCode';
import type { ComputerSpecChange } from '@/types/api';

import { activeSpecChange, effectiveTier } from '../components/specChangeUi';
import { TIER_ORDER, needsBiggerPlan, normalizeTier, offeredTiers, quotaForSpecChange } from '../components/tierUi';
import {
  PLAN_CHANGED_PARAM,
  RESIZE_PARAM,
  computersHostMounted,
  openComputerSpec,
  plansOpenInPlace,
  type SpecRequest,
} from './computerPanelStore';
import { useComputerProviders, useComputers } from './useComputers';
import { readQuotaAfterPlanChange, useTierQuota } from './useTierQuota';

export type MediumAction =
  | { kind: 'resize'; go: () => void }
  | { kind: 'plan'; go: () => void }
  | { kind: 'resizing'; change: ComputerSpecChange; go: () => void };

/** The setup wizard's Claude Code step, which mounts the dialogs too. */
const SETUP_STEP = '/setup/claude-code';

/** Where the plan trip comes back to from `pathname`: the setup step it
 *  left, so onboarding keeps its place, else Claude Code's view. */
const backTo = (pathname: string) => (pathname === SETUP_STEP ? SETUP_STEP : CLAUDE_CODE_CHOICE_URL);

/** `back` (Claude Code's view by default) with the change-spec dialog open
 *  on `computerId`. */
const resizeUrlFor = (computerId: string, back = CLAUDE_CODE_CHOICE_URL) =>
  `${back}${back.includes('?') ? '&' : '?'}${RESIZE_PARAM}=${encodeURIComponent(computerId)}`;

/**
 * The plan page, asked to send the user back to `back` once the plan
 * changes, with the change-spec dialog open on `computerId`. Null where the
 * build names no plan page.
 */
export function planUrlFor(computerId: string, back = CLAUDE_CODE_CHOICE_URL): string | null {
  if (!PLAN_URL) return null;
  return `${PLAN_URL}?return=${encodeURIComponent(`${resizeUrlFor(computerId, back)}&${PLAN_CHANGED_PARAM}=1`)}`;
}

/** Change spec as Claude Code needs it on `computerId`: on Medium, and where
 *  the plan keeps Medium closed, why, with the plan page that comes back to
 *  `back`. */
export const claudeCodeSpecRequest = (computerId: string, back = CLAUDE_CODE_CHOICE_URL): SpecRequest => ({
  tier: CLAUDE_CODE_MIN_TIER,
  planUrl: planUrlFor(computerId, back),
  leadKey: 'modelAccess.claudeCode.medium.rowPlan',
});

/** Takes `RESIZE_PARAM` off the address and opens Change spec as Claude Code
 *  needs it on that computer: how a link that runs no code asks for it (a
 *  refusal kept in a thread, the plan page's way back). Back from the plan
 *  page (`PLAN_CHANGED_PARAM`), the next quota read and the change ask the
 *  server to read the plan again. For the page that mounts the dialogs. */
export function useResizeParam() {
  const queryClient = useQueryClient();
  const { pathname } = useLocation();
  const [searchParams, setSearchParams] = useSearchParams();
  const resize = searchParams.get(RESIZE_PARAM);
  const planChanged = searchParams.has(PLAN_CHANGED_PARAM);
  useEffect(() => {
    if (!resize) return;
    if (planChanged) void readQuotaAfterPlanChange(queryClient);
    openComputerSpec(resize, { ...claudeCodeSpecRequest(resize, backTo(pathname)), planChanged });
    setSearchParams((params) => {
      params.delete(RESIZE_PARAM);
      params.delete(PLAN_CHANGED_PARAM);
      return params;
    }, { replace: true });
  }, [resize, planChanged, pathname, queryClient, setSearchParams]);
}

/**
 * The action for `computerId`, a cloud computer below Claude Code's size;
 * null while it is unknown, while a change takes it to a smaller size, once it is that size (a refusal kept in a thread
 * outlives the change), and where its provider offers no such size, so the
 * surface states the requirement alone. A plan whose slot for a larger
 * computer is taken still opens the dialog, which says so. The plan page
 * opens in this tab where the page on screen says so (`plansOpenInPlace`).
 */
export function useMediumAction(computerId: string | null | undefined): MediumAction | null {
  const navigate = useNavigate();
  const { pathname } = useLocation();
  const { data } = useComputers({ enabled: !!computerId });
  const computer = computerId ? data?.computers.find((c) => c.computer_id === computerId) : undefined;
  const { data: providers } = useComputerProviders({ enabled: !!computer });
  const { data: quota } = useTierQuota({ enabled: !!computer });
  if (!computerId || !computer) return null;
  // Where this page shows no change-spec dialog, Claude Code's view does.
  const openSpec = (request: SpecRequest | null) => () =>
    computersHostMounted() ? openComputerSpec(computerId, request) : navigate(resizeUrlFor(computerId));
  const change = activeSpecChange(computer);
  if (change) {
    // A change to a smaller size is not the way to Claude Code's.
    const toMedium = TIER_ORDER.indexOf(normalizeTier(change.target_tier)) >= TIER_ORDER.indexOf(CLAUDE_CODE_MIN_TIER);
    return toMedium ? { kind: 'resizing', change, go: openSpec(null) } : null;
  }
  const tier = effectiveTier(computer);
  if (TIER_ORDER.indexOf(tier) >= TIER_ORDER.indexOf(CLAUDE_CODE_MIN_TIER)) return null;
  const provider = providers?.providers.find((p) => p.kind === computer.kind);
  if (!offeredTiers(provider, tier).tiers.includes(CLAUDE_CODE_MIN_TIER)) return null;
  const back = backTo(pathname);
  const planUrl = planUrlFor(computerId, back);
  if (planUrl && needsBiggerPlan(quotaForSpecChange(quota, normalizeTier(computer.resource_tier)), CLAUDE_CODE_MIN_TIER)) {
    return { kind: 'plan', go: () => openConsolePage(planUrl, { inPlace: plansOpenInPlace() }) };
  }
  return { kind: 'resize', go: openSpec(claudeCodeSpecRequest(computerId, back)) };
}

/** A Claude Code model's picker row on a computer too small for it, by what
 *  picking it does. */
const MEDIUM_HOLD: Readonly<Record<MediumAction['kind'], string>> = {
  resize: 'chat.modelSelector.claudeCodeNeedsMedium',
  plan: 'chat.modelSelector.claudeCodeNeedsMediumPlan',
  resizing: 'chat.modelSelector.claudeCodeNeedsMediumResizing',
};

/** The hold a picker puts on Claude Code's models while `computerId` is too
 *  small for them, and picking one runs the fix; null where nothing holds. */
export function useMediumHold(computerId: string | null | undefined): { unavailable: string; fix: () => void } | null {
  const { t } = useTranslation();
  const action = useMediumAction(computerId);
  return action && { unavailable: t(MEDIUM_HOLD[action.kind]), fix: action.go };
}
