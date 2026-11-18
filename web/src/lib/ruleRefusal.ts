/**
 * A refusal by rule, wherever it arrives: a route's 4xx (`detail.type ===
 * "policy_denied"`), a turn's admission refusal, or a stream's error frame
 * (`error_type: "policy_denied"`). Every reader turns it into the same
 * neutral row, worded here from the rule and who set it, never from the
 * server's English sentence, which stays the fallback for a rule this client
 * does not know yet.
 */
import type { QueryClient } from '@tanstack/react-query';
import type { RuleSetter } from '@/types/api';
import { RULE, ruleSetterName } from '@/lib/accountRules';
import { queryKeys } from '@/lib/queryKeys';

export const POLICY_DENIED = 'policy_denied';

/** The rule on creating a shared workspace. Not in `RULE`: `/users/me` does
 *  not send it, so it is only ever read off a refusal or the list's
 *  `can_create`. */
export const SHARED_WORKSPACES_CREATE = 'shared_workspaces.create';

export interface RuleRefusal {
  rule: string;
  setBy: RuleSetter;
  /** The model a turn was refused, for a model rule. */
  model?: string;
  /** The server's sentence, shown for a rule this client has no words for. */
  message: string;
}

/** The refusal `detail` (a 4xx body's detail, or a stream frame) carries, or
 *  null when it is no refusal by rule. */
export function readRuleRefusal(detail: unknown): RuleRefusal | null {
  if (!detail || typeof detail !== 'object') return null;
  const d = detail as Record<string, unknown>;
  if (d.type !== POLICY_DENIED && d.error_type !== POLICY_DENIED) return null;
  if (typeof d.rule !== 'string' || !d.rule) return null;
  const message = typeof d.message === 'string' ? d.message : typeof d.error === 'string' ? d.error : '';
  return {
    rule: d.rule,
    setBy: d.set_by === 'organization' ? 'organization' : 'deployment',
    model: typeof d.model === 'string' && d.model ? d.model : undefined,
    message,
  };
}

/** Read the account's rules again after the server refused by one: it
 *  refused by a rule the app did not know of, or knew otherwise, so the
 *  rules changed since they were read, and every surface reads them anew and
 *  the picker stops offering what was just refused. Called where a refusal
 *  arrives, once per refusal, never where one is drawn. */
export function rereadRules(qc: QueryClient): void {
  void qc.invalidateQueries({ queryKey: queryKeys.user.me() });
}

/** What fixes the refusal from where it is shown. */
export type RuleAction = 'choose_model' | null;

export interface RuleRefusalCopy {
  title: string;
  body: string | null;
  action: RuleAction;
}

type Translate = (key: string, options?: Record<string, unknown>) => string;

/**
 * The row's words for `refusal`: who set the rule (the organization by name
 * when the app knows it), what it keeps from happening, and what to do
 * instead. `modelName` gives a model its display name.
 */
export function ruleRefusalCopy(
  t: Translate, refusal: RuleRefusal, organizationName: string | null, modelName: (model: string) => string,
): RuleRefusalCopy {
  const who = ruleSetterName(t, refusal.setBy, organizationName);
  const by = refusal.setBy === 'organization' ? 'organization' : 'deployment';
  const values = { who: who.start, whoMid: who.mid, model: refusal.model ? modelName(refusal.model) : '' };
  switch (refusal.rule) {
    case RULE.modelsAllowed:
      return refusal.model
        ? { title: t('organization.refusal.modelNotAllowed', values), body: t(`organization.refusal.pickAllowed.${by}`), action: 'choose_model' }
        : { title: t('organization.refusal.modelsLimited', values), body: t(`organization.refusal.pickAllowed.${by}`), action: 'choose_model' };
    case RULE.ownApiKeys:
      return refusal.model
        ? { title: t('organization.refusal.ownModels', values), body: t(`organization.refusal.ownModelBody.${by}`, values), action: 'choose_model' }
        : { title: t('organization.refusal.ownApiKeys', values), body: null, action: null };
    case RULE.ownSubscriptions:
      return refusal.model
        ? { title: t('organization.refusal.ownSubscriptions', values), body: t(`organization.refusal.ownSubscriptionBody.${by}`, values), action: 'choose_model' }
        : { title: t('organization.refusal.ownSubscriptions', values), body: null, action: null };
    case RULE.ownMachines:
      return { title: t('organization.refusal.ownMachines', values), body: t('organization.refusal.ownMachinesBody'), action: null };
    case RULE.publicLinks:
      return { title: t('organization.refusal.publicLinks', values), body: t('organization.refusal.publicLinksBody'), action: null };
    case RULE.trading:
      return { title: t('organization.refusal.trading', values), body: t('organization.refusal.tradingBody'), action: null };
    case RULE.userConnectors:
      return { title: t('organization.refusal.userConnectors', values), body: t('organization.refusal.userConnectorsBody'), action: null };
    case SHARED_WORKSPACES_CREATE:
      return { title: t('organization.refusal.sharedWorkspaces', values), body: t('organization.refusal.sharedWorkspacesBody'), action: null };
    default:
      return { title: refusal.message || t('organization.refusal.generic', values), body: null, action: null };
  }
}
