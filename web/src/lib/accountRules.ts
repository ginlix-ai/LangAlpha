/**
 * The rules an account works under, read once from `/users/me`, and the
 * decisions every surface makes from them: which models a turn may run, which
 * connections may be added, who set a rule. Pure, so the picker, Settings, the
 * plugins page and the refusal row agree because they ask the same function.
 *
 * Two layers can set a rule, the account's organization and the deployment;
 * the server merges them and reports, per restricted rule, which one set it.
 * A server that predates rules sends no `policy`, which restricts nothing. A
 * `policy` of null means the rules cannot be read right now: the server then
 * refuses turns, so the app says so instead of acting as if nothing applied.
 */
import type { AccountOrganization, AccountPolicy, AccountTeam, RuleSetter, User } from '@/types/api';
import { isClaudeCodeModel } from '@/lib/claudeCode';

export const RULE = {
  modelsAllowed: 'models.allowed',
  ownApiKeys: 'own_model_access.api_keys',
  ownSubscriptions: 'own_model_access.subscriptions',
  publicLinks: 'public_links',
  trading: 'trading',
  ownMachines: 'own_machines',
  userConnectors: 'user_connectors',
} as const;

export type Rule = (typeof RULE)[keyof typeof RULE];

/** Where the account stands, for every surface that changes with it. */
export interface AccountStanding {
  /** The organization, when the account belongs to one the server named. */
  organization: AccountOrganization | null;
  /** The merged rules; null while they cannot be read (`unknown`) and for a
   *  server that sends none, which restricts nothing. */
  policy: AccountPolicy | null;
  /** The rules cannot be read right now: turns are refused until they can. */
  unknown: boolean;
  /** The account's access through its organization has ended. */
  suspended: boolean;
  /** The account's teams in its organization: none from a server that
   *  sends no teams, and none while suspended. */
  teams: AccountTeam[];
}

export const NO_STANDING: AccountStanding = {
  organization: null, policy: null, unknown: false, suspended: false, teams: [],
};

export function standingOf(user: User | null | undefined): AccountStanding {
  if (!user) return NO_STANDING;
  const organization = user.organization ?? null;
  const suspended = organization?.suspended === true;
  return {
    organization,
    policy: user.policy ?? null,
    // Explicit null only: an older server leaves the field out.
    unknown: user.policy === null && !suspended,
    suspended,
    teams: suspended ? [] : teamsOf(organization),
  };
}

/** The organization's `teams` as far as they can be read: a team missing its
 *  id, name or role is left out rather than shown half known. */
function teamsOf(organization: AccountOrganization | null): AccountTeam[] {
  const raw: unknown = organization?.teams;
  if (!Array.isArray(raw)) return [];
  return raw.filter((t): t is AccountTeam => {
    if (!t || typeof t !== 'object') return false;
    const { id, name, role } = t as Record<string, unknown>;
    return typeof id === 'string' && id !== '' && typeof name === 'string' && (role === 'admin' || role === 'member');
  });
}

/** Whether `rule` allows what it governs. A rule nobody restricts allows it,
 *  and so does a server that sends no rules. `models.allowed` restricts by
 *  model and is asked through `modelRefusedBy`. */
export function allows(policy: AccountPolicy | null, rule: Exclude<Rule, 'models.allowed'>): boolean {
  if (!policy) return true;
  switch (rule) {
    case RULE.ownApiKeys: return policy.own_model_access.api_keys;
    case RULE.ownSubscriptions: return policy.own_model_access.subscriptions;
    case RULE.publicLinks: return policy.public_links;
    case RULE.trading: return policy.trading;
    case RULE.ownMachines: return policy.own_machines;
    case RULE.userConnectors: return policy.user_connectors;
  }
}

/** Who set `rule`; the deployment when the server named nobody. */
export function setterOf(policy: AccountPolicy | null, rule: string): RuleSetter {
  return policy?.set_by?.[rule] ?? 'deployment';
}

/**
 * The rule that keeps a turn off `model`, judged by its catalog entry, or
 * null when none does. Mirrors the server's order: the allow-list first
 * (which names manifest models only, so a model the user added never passes
 * it), then the user's own keys for a model they added, then their own
 * sign-ins for a model that runs on one: a subscription's, or Claude Code's.
 */
export function modelRefusedBy(policy: AccountPolicy | null, metadata: Record<string, ModelMeta>, model: string): Rule | null {
  if (!policy) return null;
  const meta = metadata[model];
  const custom = meta?.is_custom_model === true;
  const allowed = policy.models.allowed;
  if (allowed && (custom || !allowed.includes(model))) return RULE.modelsAllowed;
  if (custom && !policy.own_model_access.api_keys) return RULE.ownApiKeys;
  if ((meta?.access_type === 'oauth' || isClaudeCodeModel(meta)) && !policy.own_model_access.subscriptions) return RULE.ownSubscriptions;
  return null;
}

/** The picker row's words for a model `rule` refuses. A model the user added
 *  never passes a list, whatever it is named. */
export function modelRuleRowKey(rule: Rule, meta: ModelMeta | undefined): string {
  const copy = rule === RULE.modelsAllowed && meta?.is_custom_model === true ? 'custom' : MODEL_RULE_COPY[rule];
  return `organization.picker.row.${copy}`;
}

/** Whether any rule narrows which models the account sees. */
export function restrictsModels(policy: AccountPolicy | null): boolean {
  return !!policy && (
    policy.models.allowed !== null
    || !policy.own_model_access.api_keys
    || !policy.own_model_access.subscriptions
  );
}

type Translate = (key: string, options?: Record<string, unknown>) => string;

/** `key` naming `organization`, or its `Unnamed` variant when the name is
 *  unknown: the wording that reads naturally without a name differs from
 *  the one around it. */
export function orgWords(t: Translate, organization: string | null, key: string, values?: Record<string, unknown>): string {
  return organization ? t(key, { ...values, organization }) : t(`${key}Unnamed`, values);
}

/** The organization's name, or "your organization" where the wording takes
 *  a name either way. */
export function organizationLabel(t: Translate, organization: string | null): string {
  return organization ?? t('organization.yourOrganization');
}

/**
 * Who set a rule, as the start of a sentence and in the middle of one: the
 * organization's name where the server gave it, else "your organization", or
 * "this deployment". Copy reads `{{who}} doesn't allow ...` with one of these.
 */
export function ruleSetterName(
  t: Translate, setter: RuleSetter, organizationName: string | null | undefined,
): { start: string; mid: string } {
  if (setter === 'deployment') {
    return { start: t('organization.who.deploymentStart'), mid: t('organization.who.deploymentMid') };
  }
  if (organizationName) return { start: organizationName, mid: organizationName };
  return { start: t('organization.who.yourOrganizationStart'), mid: t('organization.who.yourOrganizationMid') };
}

type ModelMeta = { access_type?: string; is_custom_model?: boolean; sdk?: string };

/**
 * `models` (provider group to its models) without the ones a rule keeps a
 * turn off, a group with none left dropping out. The same object when nothing
 * restricts models, so a deployment and an account with no rules render
 * exactly as before.
 */
export function withoutRefusedModels<T extends { models?: string[] }>(
  models: Record<string, T>,
  policy: AccountPolicy | null,
  metadata: Record<string, ModelMeta>,
): Record<string, T> {
  if (!restrictsModels(policy)) return models;
  const out: Record<string, T> = {};
  for (const [group, data] of Object.entries(models)) {
    const kept = (data.models ?? []).filter((m) => modelRefusedBy(policy, metadata, m) === null);
    if (kept.length > 0) out[group] = { ...data, models: kept };
  }
  return out;
}

/**
 * The model a turn runs when `saved` is refused by a rule: the first entry of
 * the allow-list the user can reach, as the server picks it, else the
 * deployment's default when allowed. `saved` itself when nothing refuses it.
 */
export function allowedDefault(
  policy: AccountPolicy | null,
  saved: string | null,
  reachable: (model: string) => boolean,
  metadata: Record<string, ModelMeta>,
  deploymentDefault: string | null,
): string | null {
  if (saved === null || !restrictsModels(policy)) return saved;
  const refused = (m: string) => modelRefusedBy(policy, metadata, m) !== null;
  if (!refused(saved)) return saved;
  const listed = policy?.models.allowed?.find((m) => reachable(m) && !refused(m));
  if (listed) return listed;
  return deploymentDefault && !refused(deploymentDefault) ? deploymentDefault : null;
}

/** The rule that narrows the account's models most, which the picker names
 *  under its list; null when none does. */
export function modelsRule(policy: AccountPolicy | null): Rule | null {
  if (!policy) return null;
  if (policy.models.allowed !== null) return RULE.modelsAllowed;
  if (!policy.own_model_access.api_keys) return RULE.ownApiKeys;
  if (!policy.own_model_access.subscriptions) return RULE.ownSubscriptions;
  return null;
}

/** The i18n key part each model rule's copy sits under. */
export const MODEL_RULE_COPY: Record<string, 'allowList' | 'ownApiKeys' | 'ownSubscriptions'> = {
  [RULE.modelsAllowed]: 'allowList',
  [RULE.ownApiKeys]: 'ownApiKeys',
  [RULE.ownSubscriptions]: 'ownSubscriptions',
};

/** Whether a way of connecting a model is open: a subscription's sign-in
 *  under the subscriptions rule, a key or a local server under the keys rule. */
export function methodAllowed(policy: AccountPolicy | null, method: 'oauth' | 'api_key' | 'local' | 'coding_plan'): boolean {
  return method === 'oauth' ? allows(policy, RULE.ownSubscriptions) : allows(policy, RULE.ownApiKeys);
}
