/**
 * The models the default step offers for the answers, and the setup it
 * recommends from them: a conversation model and a Background one, each with
 * the reason it was picked, read from the manifest's ratings (or, for the
 * deployment's models, the deployment's own picks, and for Claude Code, its
 * newest Sonnet, which suits any Claude plan).
 */
import { CLAUDE_CODE, claudeCodeStartModel } from '@/lib/claudeCode';
import type { Payer } from '@/lib/modelPayer';
import { listedFor } from '@/lib/connectionModels';
import type { ConnectionOf } from '@/lib/providerCatalog';
import type { CustomModelEntry } from '@/components/model/types';
import type { ModelsAnswer } from './setupSteps';

/** How many models the step lists; the rest are in Settings. */
export const DEFAULT_CHOICES_SHOWN = 5;

export type RatedMetadata = Record<string, { sdk?: string; model_id?: string; provider?: string; intelligence?: number; speed?: number } | undefined>;

export interface DefaultChoicesInput {
  models: ModelsAnswer;
  /** The models the user can run, by provider. */
  offered: Record<string, { models?: string[] }>;
  /** The models that can run Background work; a conversation-only one (Claude Code) is not one. */
  slotModels: readonly string[];
  payers: Record<string, Payer>;
  /** Providers connected with the user's own sign-in or key. */
  ownProviders: readonly string[];
  metadata: RatedMetadata;
  customModels: readonly CustomModelEntry[];
  connectionOf: ConnectionOf;
  systemDefault: string | null;
  systemBackground: string | null;
  /** The saved default, which connecting in setup set. */
  preferred: string | null;
}

export type PrimaryWhy = 'strongest' | 'strongestFastest' | 'deployment' | 'claudeCode';
export type BackgroundWhy = 'fastest' | 'only' | 'deployment' | 'claudeCode' | 'claudeCodeCredits';

export interface RecommendedSetup {
  primary: string | null;
  primaryWhy: PrimaryWhy | null;
  /** Null leaves Background unset, which runs the deployment's Background model. */
  background: string | null;
  backgroundWhy: BackgroundWhy | null;
}

export interface DefaultChoices {
  list: string[];
  initial: string | null;
  recommended: RecommendedSetup;
}

const rating = (value: number | undefined) => value ?? -1;

function best(pool: readonly string[], first: (m: string) => number, second: (m: string) => number): { model: string; tied: boolean } | null {
  if (pool.length === 0) return null;
  const top = Math.max(...pool.map(first));
  if (top < 0) return null;
  const tops = pool.filter((m) => first(m) === top);
  const model = tops.reduce((a, b) => (second(b) > second(a) ? b : a));
  // Tied only where the second rating is what told them apart.
  return { model, tied: new Set(tops.map(second)).size > 1 };
}

/** The highest intelligence rating in \`pool\`, the faster on a tie; null where none is rated. */
export function strongestModel(pool: readonly string[], metadata: RatedMetadata): { model: string; tied: boolean } | null {
  return best(pool, (m) => rating(metadata[m]?.intelligence), (m) => rating(metadata[m]?.speed));
}

type PrimaryPick = Pick<RecommendedSetup, 'primary' | 'primaryWhy'>;
type BackgroundPick = Pick<RecommendedSetup, 'background' | 'backgroundWhy'>;

function strongestPick(input: DefaultChoicesInput, pool: string[]): PrimaryPick {
  const strongest = strongestModel(pool, input.metadata);
  return {
    primary: strongest?.model ?? pool[0] ?? null,
    primaryWhy: strongest ? (strongest.tied ? 'strongestFastest' : 'strongest') : null,
  };
}

/** The fastest model Background can run other than the primary, else the
 *  primary itself where it is the only one. */
function fastestOtherPick(input: DefaultChoicesInput, pool: string[], primary: string | null): BackgroundPick {
  const runnable = pool.filter((m) => input.slotModels.includes(m));
  const others = runnable.filter((m) => m !== primary);
  if (others.length === 0) {
    return primary && runnable.includes(primary) ? { background: primary, backgroundWhy: 'only' } : { background: null, backgroundWhy: null };
  }
  const fastest = best(others, (m) => rating(input.metadata[m]?.speed), (m) => rating(input.metadata[m]?.intelligence));
  return { background: fastest?.model ?? others[0], backgroundWhy: fastest ? 'fastest' : null };
}

/** Served by the deployment, as LangAlpha's models are: never a key or plan the user connected. */
const platformServed = (input: DefaultChoicesInput, model: string) => input.payers[model] === 'credits' || input.payers[model] === 'server_key';

/** How each answer picks its primary and its Background. */
const RULES: Record<ModelsAnswer, {
  primary: (input: DefaultChoicesInput, pool: string[]) => PrimaryPick;
  background: (input: DefaultChoicesInput, pool: string[], primary: string | null) => BackgroundPick;
}> = {
  langalpha_models: {
    primary: (input, pool) => (input.systemDefault && pool.includes(input.systemDefault)
      ? { primary: input.systemDefault, primaryWhy: 'deployment' } : strongestPick(input, pool)),
    background: (input, pool, primary) => (input.systemBackground && input.slotModels.includes(input.systemBackground)
      && platformServed(input, input.systemBackground)
      ? { background: input.systemBackground, backgroundWhy: 'deployment' } : fastestOtherPick(input, pool, primary)),
  },
  // Claude Code runs conversations only, so Background stays the deployment's.
  claude_code: {
    primary: (input, pool) => {
      const primary = claudeCodeStartModel(pool, input.metadata, null);
      return { primary, primaryWhy: primary ? 'claudeCode' : null };
    },
    background: (input) => ({
      background: null,
      backgroundWhy: !input.systemBackground ? null
        : input.payers[input.systemBackground] === 'credits' ? 'claudeCodeCredits' : 'claudeCode',
    }),
  },
  subscription_or_key: { primary: strongestPick, background: fastestOtherPick },
};

function recommend(input: DefaultChoicesInput, pool: string[]): RecommendedSetup {
  const rule = RULES[input.models];
  const primary = rule.primary(input, pool);
  return { ...primary, ...rule.background(input, pool, primary.primary) };
}

export function defaultChoices(input: DefaultChoicesInput): DefaultChoices {
  const of = (provider: string) => listedFor(provider, input.offered, input.metadata, input.customModels, input.connectionOf);
  let pool: string[];
  if (input.models === 'claude_code') pool = of(CLAUDE_CODE);
  else if (input.models === 'subscription_or_key') pool = input.ownProviders.flatMap(of);
  else {
    pool = Object.values(input.offered)
      .flatMap((p) => p.models ?? [])
      .filter((m) => platformServed(input, m));
  }
  pool = [...new Set(pool)];

  const recommended = recommend(input, pool);
  // Connecting saved the model it starts on, which is the recommended one; a
  // default saved on an earlier run of setup is kept.
  const saved = input.models !== 'langalpha_models' && input.preferred && pool.includes(input.preferred)
    ? input.preferred : null;
  const initial = saved ?? recommended.primary;
  const rest = pool.filter((m) => m !== initial && m !== recommended.primary);
  const head = [initial, recommended.primary].filter((m, i, all): m is string => !!m && all.indexOf(m) === i);
  return { list: [...head, ...rest].slice(0, DEFAULT_CHOICES_SHOWN), initial, recommended };
}
