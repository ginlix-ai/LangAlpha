/**
 * Who pays the vendor when a turn runs on a model, the answer every model
 * name carries as its badge.
 *
 * The client's copy of the server's ladder (`resolve_model_client`): a model
 * on a subscription provider runs only on that sign-in; any other model runs
 * on the user's own key when one serves it and their keys are switched on,
 * and otherwise on the deployment, which bills credits on a hosted account
 * and spends its own key on a self-hosted one.
 */
import { isPlatformMode } from '@/config/hostMode';
import { CLAUDE_CODE } from '@/lib/claudeCode';
import type { ConfiguredProvider } from '@/hooks/useConfiguredProviders';
import { SERVER_KEY, isOwnKey } from '@/lib/ownKey';
import type { KeyCandidates } from '@/components/model/types';
import type { PlatformModelsResponse } from '@/types/platform';

/**
 * A subscription carries the provider it signs in to, or whose coding-plan key
 * pays, so its tag can name the plan and two plans stay apart; every key of the user's pays alike, and so
 * does the deployment. Kept a string so payers compare, group and key maps as
 * values.
 */
export type Payer = 'own_key' | 'credits' | 'server_key' | `subscription:${string}`;

const SUBSCRIPTION = 'subscription:';

/** The payer of a model on the sign-in to `provider`, the model's own. */
export const subscriptionPayer = (provider = ''): Payer => `${SUBSCRIPTION}${provider}`;

export const isSubscription = (payer: Payer | undefined): payer is `subscription:${string}` =>
  !!payer?.startsWith(SUBSCRIPTION);

/** The deployment, when it pays: credits on a hosted account, its own key on
 *  a self-hosted one. */
export const deploymentPayer = (): Payer => (isPlatformMode ? 'credits' : 'server_key');

export const paidByDeployment = (payer: Payer | undefined): boolean =>
  payer === 'credits' || payer === 'server_key';

/** The user's own key or sign-in pays. */
export const paidByUser = (payer: Payer | undefined): boolean =>
  payer === 'own_key' || isSubscription(payer);

// The plan a sign-in draws on, named as its maker sells it. A bare
// "Subscription" beside "Credits" reads as the user's LangAlpha plan, which is
// where credits come from.
const SUBSCRIPTION_PLANS: Record<string, string> = {
  'claude-oauth': 'Claude',
  'codex-oauth': 'ChatGPT',
  'grok-oauth': 'Grok',
};

const PAYER_LABEL_KEYS = {
  own_key: 'payer.ownKey',
  credits: 'payer.credits',
  server_key: 'payer.serverKey',
} as const;

/** A payer's badge text. A coding plan is named by `planName`, the catalog's
 *  name for it; one whose name already says plan ("GLM Coding Plan") is
 *  tagged with the name alone. A plan nothing names keeps the generic word.
 *  Claude Code is named alone: it runs on the setup token kept for it on the
 *  server, not on a plan the user connects in Settings. */
export function payerLabel(
  t: (key: string, opts?: Record<string, unknown>) => string,
  payer: Payer,
  planName?: (provider: string) => string | undefined,
): string {
  if (!isSubscription(payer)) return t(PAYER_LABEL_KEYS[payer]);
  const provider = payer.slice(SUBSCRIPTION.length);
  if (provider === CLAUDE_CODE) return t('payer.claudeCode');
  const plan = SUBSCRIPTION_PLANS[provider] ?? planName?.(provider);
  if (!plan) return t('payer.subscription');
  return /\bplan$/i.test(plan) ? plan : t('payer.plan', { plan });
}

/** What a payer reads off a model's manifest entry. */
export interface PayerModel {
  provider?: string;
  access_type?: string;
  tier?: number;
}

export interface OwnKeyContext {
  configured: readonly ConfiguredProvider[];
  byokEnabled: boolean;
  keyCandidates: Record<string, KeyCandidates>;
}

export interface PayerContext extends OwnKeyContext {
  /** The plan answer, already merged with the user's own connections; null
   *  when there is none (self-hosted, or the service did not answer). */
  platform: PlatformModelsResponse | null;
}

/**
 * Whether one of the user's own keys serves a provider's models, as the
 * server's walk decides (`walk_byok_candidates`, `key_fits`): a key under one
 * of the provider's slugs that shares its credential, or any of its slugs when
 * the key was saved with a base URL, which wins over the model's endpoint.
 * `anyKey` takes every slug, for a model the user added, whose client is built
 * against whichever slug holds the key.
 *
 * Read off the keys the user saved, the store that walk reads, and none while
 * their keys are switched off, as the server reads none without `is_byok`.
 * Not the plan answer's `byok_providers`: that is another service's list, and
 * merging it with ours does not drop it when the switch is off.
 */
export function ownKeyRule({ configured, byokEnabled, keyCandidates }: OwnKeyContext) {
  const own = byokEnabled
    ? configured.filter((p) => p.access_type !== 'oauth' && isOwnKey(p))
    : [];
  const keyed = new Set(own.map((p) => p.provider));
  const pointed = new Set(own.filter((p) => p.base_url).map((p) => p.provider));
  return {
    /** Whether a key is saved under this very slug. */
    holds: (slug: string) => keyed.has(slug),
    serves: (provider: string, anyKey = false) => {
      const { slugs, shared } = keyCandidates[provider] ?? { slugs: [provider], shared: [provider] };
      return (anyKey ? slugs : shared).some((s) => keyed.has(s)) || slugs.some((s) => pointed.has(s));
    },
  };
}

/**
 * Builds the payer of each model for one set of connections. A model with no
 * payer is one nothing serves (locked on the plan, a self-hosted model the
 * deployment holds no key for) or a local server, which nobody pays.
 *
 * `custom` is the name of a model the user added, whose provider is
 * `provider` here. Only their own credential runs one, so without a key that
 * serves it (none saved, or their keys switched off) it has no payer, as the
 * server builds no client for it.
 */
export function payerResolver(ctx: PayerContext) {
  const ownKey = ownKeyRule(ctx);
  const served = new Set(
    ctx.configured.filter((p) => p.masked_key === SERVER_KEY).map((p) => p.provider),
  );
  const byAccess = (access: ConfiguredProvider['access_type']) =>
    new Set(ctx.configured.filter((p) => p.access_type === access).map((p) => p.provider));
  const signIns = byAccess('oauth');
  const plans = byAccess('coding_plan');
  const locals = byAccess('local');
  return (meta: PayerModel | undefined, provider: string, custom?: string): Payer | undefined => {
    // A model the user added runs on its own route, whatever built-in it
    // shadows: their sign-in, their local server, or a key of theirs, saved
    // under the model's own name or one its provider's key serves. A key of a
    // coding plan pays as the plan, a subscription bought with a key.
    if (custom !== undefined) {
      if (signIns.has(provider)) return subscriptionPayer(provider);
      if (locals.has(provider)) return undefined;
      if (!ownKey.holds(custom) && !ownKey.serves(provider, true)) return undefined;
      return plans.has(provider) ? subscriptionPayer(provider) : 'own_key';
    }
    if (meta?.access_type === 'oauth') return subscriptionPayer(provider);
    if (meta?.access_type === 'local') return undefined;
    if (ownKey.serves(provider)) return meta?.access_type === 'coding_plan' ? subscriptionPayer(provider) : 'own_key';
    if (isPlatformMode) {
      const { platform } = ctx;
      // No plan answer fails open, as the model filter does.
      if (!platform || (meta?.tier ?? 0) <= platform.model_tier) return 'credits';
      return undefined;
    }
    return served.has(provider) ? 'server_key' : undefined;
  };
}

/** The payer the server reported on the wire (`billing_type`), read with the
 *  model's catalog entry: the wire does not name a sign-in, which the model's
 *  provider names, and it calls a local server's keyless call "platform",
 *  where nobody pays. */
export function payerOfBilling(billing: unknown, model?: PayerModel): Payer | undefined {
  if (model?.access_type === 'local') return undefined;
  if (billing === 'oauth') return subscriptionPayer(model?.provider);
  if (billing === 'byok') return model?.access_type === 'coding_plan' ? subscriptionPayer(model.provider) : 'own_key';
  if (billing === 'platform') return deploymentPayer();
  return undefined;
}
